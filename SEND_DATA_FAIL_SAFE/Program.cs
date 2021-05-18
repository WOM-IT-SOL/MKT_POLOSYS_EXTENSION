using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using POLO_EXTENSION;

namespace SEND_DATA_FAIL_SAFE
{
    class Program
    {
        private string connString;
        private SqlConnection connection;
        private SqlCommand command;

        public Program(string connString)
        {
            this.connString = connString;
            this.connection = new SqlConnection(connString);
            this.command = new SqlCommand();
            this.command.Connection = this.connection;
        }

        private void logJob(string state, string errMsg = "NULL")
        {
            this.command.CommandText = @"INSERT INTO CONFINS.DBO.LOG_JOB_PROC_WOM(JOB_NAME, PROC_NAME, DATE_PROCESSED, ERR_MESSAGE, ERR_LINE, ERR_NUMBER) 
                VALUES('JOB_POLOSYS_SENDDATA_FAILSAFE', '" + state + " JOB_POLOSYS_SENDDATA_FAILSAFE', GETDATE(), " + errMsg + ", NULL, NULL)";
            this.command.CommandType = CommandType.Text;
            this.command.Connection.Open();
            this.command.ExecuteReader();
            this.command.Connection.Close();
        }

        public async Task startProcess()
        {
            List<string> taskIds = new List<string>();
            this.command.CommandText = @"SELECT TASK_ID FROM T_MKT_POLO_ORDER_IN WHERE SEND_FLAG_WISE='0' OR SEND_FLAG_MSS='0'";
            this.command.CommandType = CommandType.Text;

            this.command.Connection.Open();
            SqlDataReader dr = this.command.ExecuteReader();

            while (dr.Read())
            {
                taskIds.Add(dr.GetString(0));
            }

            this.command.Connection.Close();
            dr.Close();

            /* remark
            SendDataPreparation send = new SendDataPreparation(this.connString, true);
            await send.startProcess(taskId);
            */

            /*add */
            List<string> errors = new List<string>();
            foreach (string taskId in taskIds)
            {
                try
                {
                    SendDataPreparation send = new SendDataPreparation(this.connString, true);
                    await send.startProcess(taskId);
                }
                catch (Exception e)
                {
                    errors.Add(taskId+" : "+e.Message+";");
                    continue;
                }

                if (errors.Count>0)
                {
                    string errMsg = "";
                    foreach (string err in errors)
                    {
                        errMsg += err;
                    }
                    throw new Exception(errMsg);
                }
            }
        }

        static async Task Main(string[] args)
        {
            Program program = new Program(ConfigurationManager.ConnectionStrings[args[0]].ToString());

            string errMsg = "";
            try
            {
                program.logJob("START");
                await program.startProcess();
                Console.WriteLine("Done");
            }
            catch (Exception e)
            {
                errMsg = e.Message;
                Console.WriteLine("Error");
                Console.WriteLine(errMsg);
            }
            finally
            {
                if (errMsg != "") program.logJob("END", "'" + errMsg + "'");
                else program.logJob("END");
            }
        }
    }
}
