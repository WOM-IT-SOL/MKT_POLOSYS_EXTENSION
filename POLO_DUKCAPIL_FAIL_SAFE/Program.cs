using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace POLO_DUKCAPIL_FAIL_SAFE
{
    class Program
    {
        private SqlConnection connection;
        private SqlCommand command;

        public Program(string connString)
        {
            this.connection = new SqlConnection(connString);
            this.command = new SqlCommand();
            this.command.Connection = this.connection;
        }

        private void logJob(string state, string errMsg = "NULL")
        {
            this.command.CommandText = @"INSERT INTO CONFINS.DBO.LOG_JOB_PROC_WOM(JOB_NAME, PROC_NAME, DATE_PROCESSED, ERR_MESSAGE, ERR_LINE, ERR_NUMBER) 
                VALUES('JOB_POLOSYS_DUKCAPIL_FAILSAFE', '" + state + " JOB_POLOSYS_DUKCAPIL_FAILSAFE', GETDATE(), " + errMsg + ", NULL, NULL)";
            this.command.CommandType = CommandType.Text;
            this.command.Connection.Open();
            this.command.ExecuteReader();
            this.command.Connection.Close();
        }

        private async Task startProcess()
        {
            Dictionary<string, string> result = new Dictionary<string, string>();
            DateTime today = DateTime.Today;

            this.command.CommandText = @"DECLARE @successCode VARCHAR(max)
                SELECT @successCode = PARAMETER_VALUE
                FROM M_MKT_POLO_PARAMETER
                WHERE PARAMETER_TYPE = 'RESPONSE_CODE_API_DUKCAPIL_SUCCESS'
                
                SELECT DISTINCT QUEUE_UID
                FROM T_MKT_POLO_DUKCAPIL_CHECK_QUEUE
                WHERE RESPONSE_CODE NOT IN (SELECT * FROM fnMKT_POLO_SPLIT_STRING(@successCode, ','))
                    AND FLAG_PROCESS = 'F'
                    AND CAST(DTM_CRT AS DATE)= '" + today.ToString("yyyy-MM-dd") + "'";

            this.command.CommandType = CommandType.Text;
            this.command.Connection.Open();

            SqlDataReader rd = this.command.ExecuteReader();
            List<string> queueIds = new List<string>();

            while (rd.Read())
            {
                queueIds.Add(rd.GetString(0));
            }

            this.command.Connection.Close();
            rd.Close();

            await consume_polo_dukcapil(queueIds);
        }

        private async Task consume_polo_dukcapil(List<string> queueUids)
        {
            this.command.CommandText = "SELECT PARAMETER_VALUE FROM M_MKT_POLO_PARAMETER WHERE PARAMETER_TYPE='URL_MKT_POLO_API_DUKCAPIL_FROM_POLO'";
            this.command.Connection.Open();
            SqlDataReader dr = this.command.ExecuteReader();
            dr.Read();

            string url = dr.GetString(0);

            dr.Close();
            this.command.Connection.Close();

            HttpClient client = new HttpClient();

            foreach (var id in queueUids)
            {
                var body = JsonConvert.SerializeObject(new { dataSource = "UPLOAD", queueUID = id, isJob = "" }, Formatting.None);
                var content = new StringContent(body, Encoding.UTF8, "application/json");

                var response = await client.PostAsync(new Uri(url), content);
                if (!response.IsSuccessStatusCode)
                {
                    throw new Exception(response.ReasonPhrase);
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
