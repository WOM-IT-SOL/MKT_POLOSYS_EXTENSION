using System;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Collections.Generic;
using System.Net.Http;
using Newtonsoft.Json;
using System.Text;
using Newtonsoft.Json.Linq;
using System.Runtime.InteropServices;

namespace POLO_EXTENSION
{
    public class SendDataPreparation
    {
        private Boolean isJob;
        private SqlConnection connection;
        private SqlCommand command;

        public SendDataPreparation(string connectionString, bool isJob = false)
        {
            this.isJob = isJob;
            this.connection = new SqlConnection(connectionString);
            this.command = new SqlCommand();
            this.command.Connection = this.connection;
        }

        public async Task startProcess(string taskId)
        {
            Dictionary<string, string> result = new Dictionary<string, string>();
            string sendDataFlag;
            string sendDataTo;
            string sendDataName;

            this.command.CommandText = "spMKT_POLO_SENDDATA_VALIDATION";
            this.command.CommandType = CommandType.StoredProcedure;
            this.command.Parameters.Clear();
            this.command.Parameters.AddWithValue("@taskId", taskId);
            this.command.Connection.Open();

            SqlDataReader rd = this.command.ExecuteReader();

            rd.Read();
            sendDataFlag = rd.GetValue(rd.GetOrdinal("SENDDATA_FLAG")).ToString();
            sendDataTo = rd.GetValue(rd.GetOrdinal("SENDDATA_TO")).ToString();

            // the line below will only be used in case of more than 1 data needed to be send to MSS/WISE
            sendDataName = rd.GetValue(rd.GetOrdinal("SENDDATA_NAME")).ToString();

            rd.Close();
            this.command.Connection.Close();

            if (sendDataFlag == "T" && (sendDataTo == "WISE" || sendDataTo == "MSS"))
            {
                await this.fetchData(taskId, sendDataTo);
            }
        }

        private async Task fetchData(string taskId, string sendTo)
        {
            Dictionary<string, string> record = new Dictionary<string, string>();
            string spName = "";

            if (sendTo == "WISE")
            {
                spName = "spMKT_POLO_SENDDATA_GETDATAPREP";
            }
            else if (sendTo == "MSS")
            {
                spName = "spMKT_POLO_SENDDATA_GETDATATASK";
            }

            this.command.CommandText = spName;
            this.command.CommandType = CommandType.StoredProcedure;
            this.command.Parameters.Clear();
            this.command.Parameters.AddWithValue("@taskId", taskId);

            this.command.Connection.Open();

            SqlDataReader rd = this.command.ExecuteReader();
            rd.Read();

            for (int i = 0; i < rd.FieldCount; i++)
            {
                record.Add(rd.GetName(i), rd.GetValue(i).ToString());
            }

            rd.Close();
            this.command.Connection.Close();

            if (sendTo == "WISE")
            {
                await this.consumeSendDataAPI(taskId, "DataPreparation_To_Wise", "URL_API_SEND_DATA_WISE", record);
            }
            else if (sendTo == "MSS")
            {
                await this.consumeSendDataAPI(taskId, "DataTask_To_MSS", "URL_API_SEND_DATA_MSS", record);
            }

        }

        #region log request and response
        private Dictionary<string, string> logRequest(string taskId, string apiName, string bodyJson)
        {
            if (isJob) apiName = "Job_" + apiName;

            Dictionary<string, string> result = new Dictionary<string, string>();
            this.command.CommandText = "spMKT_POLO_API_LOGREQUEST";
            this.command.CommandType = CommandType.StoredProcedure;

            this.command.Parameters.Clear();
            this.command.Parameters.AddWithValue("idName", apiName);
            this.command.Parameters.AddWithValue("parameter", bodyJson);
            this.command.Parameters.AddWithValue("taskId", taskId);

            this.command.Connection.Open();

            SqlDataReader rd = this.command.ExecuteReader();
            rd.Read();
            for (int i = 0; i < rd.FieldCount; i++)
            {
                result.Add(rd.GetName(i), rd.GetValue(i).ToString());
            }

            this.command.Connection.Close();
            rd.Close();

            return result;
        }

        private void logResponse(string taskId, string apiName, string responseId, string responseMsg, string responseCode, string errorDesc)
        {
            if (isJob) apiName = "Job_" + apiName;
            
            this.command.CommandText = "spMKT_POLO_API_LOGRESPONSE";
            this.command.CommandType = CommandType.StoredProcedure;
            this.command.Parameters.Clear();
            this.command.Parameters.AddWithValue("idName", apiName);
            this.command.Parameters.AddWithValue("responseCode", (object)responseCode ?? DBNull.Value);
            this.command.Parameters.AddWithValue("responseMsg", (object)responseMsg ?? DBNull.Value);
            this.command.Parameters.AddWithValue("errorDesc", (object)errorDesc ?? DBNull.Value);
            this.command.Parameters.AddWithValue("responseId", responseId);
            this.command.Parameters.AddWithValue("taskId", taskId);

            this.command.Connection.Open();

            this.command.ExecuteReader();
            this.command.Connection.Close();
        }
        #endregion

        #region after consume API
        private void postConsumeWiseAPI(string taskId, string startedDt, string respCode, string appNo)
        {
            this.command.CommandText = "spMKT_POLO_SENDDATA_UPDATEDATAPREP";
            this.command.CommandType = CommandType.StoredProcedure;
            this.command.Parameters.Clear();
            this.command.Parameters.AddWithValue("taskId", taskId);
            this.command.Parameters.AddWithValue("startedDt", (object)startedDt ?? DBNull.Value);
            this.command.Parameters.AddWithValue("respCode", (object)respCode ?? DBNull.Value);
            this.command.Parameters.AddWithValue("appNo", (object)appNo ?? DBNull.Value);

            this.command.Connection.Open();

            this.command.ExecuteReader();
            this.command.Connection.Close();
        }

        private void postConsumeMSSAPI(string taskId, string respCode, string orderNo)
        {
            this.command.CommandText = "spMKT_POLO_SENDDATA_UPDATEDATATASK";
            this.command.CommandType = CommandType.StoredProcedure;
            this.command.Parameters.Clear();
            this.command.Parameters.AddWithValue("taskId", taskId);
            this.command.Parameters.AddWithValue("respCode", (object)respCode ?? DBNull.Value);
            this.command.Parameters.AddWithValue("orderNo", (object)orderNo ?? DBNull.Value);

            this.command.Connection.Open();

            this.command.ExecuteReader();
            this.command.Connection.Close();
        }
        #endregion

        private async Task consumeSendDataAPI(string taskId, string apiName, string urlParameterId, Dictionary<string, string> record)
        {
            #region fetch api url    

            this.command.CommandText = @"SELECT PARAMETER_VALUE FROM M_MKT_POLO_PARAMETER WHERE PARAMETER_ID='" + urlParameterId + "'";
            this.command.CommandType = CommandType.Text;

            this.command.Connection.Open();

            SqlDataReader rd = this.command.ExecuteReader();
            rd.Read();
            string sendDataApiUrl = rd.GetString(0).ToString();
            rd.Close();

            this.command.Connection.Close();
            #endregion

            string tempStartDt = "";
            if (apiName == "DataPreparation_To_Wise")
            {
                tempStartDt = record["startDt"];
                record["startDt"] = DateTime.Parse(record["startDt"]).ToString("dd/MM/yyyy");
            }
            string bodyJson = JsonConvert.SerializeObject(record, Formatting.None);
            var requestLogResult = this.logRequest(taskId, apiName, bodyJson);

            #region consume send data API
            try
            {
                HttpClient client = new HttpClient();
                client.Timeout = TimeSpan.FromMinutes(3);
                var content = new StringContent(bodyJson, Encoding.UTF8, "application/json");
                var response = await client.PostAsync(new Uri(sendDataApiUrl), content);

                if (response.IsSuccessStatusCode)
                {
                    string resJson = await response.Content.ReadAsStringAsync();
                    JObject resObj = JObject.Parse(resJson);

                    if (apiName == "DataPreparation_To_Wise")
                    {
                        this.logResponse(taskId, apiName, requestLogResult["responseId"], resObj["status"]["message"].ToString(), resObj["status"]["code"].ToString(), null);
                        this.postConsumeWiseAPI(taskId, tempStartDt, resObj["status"]["code"].ToString(), resObj["appNo"].ToString());
                    }
                    else if (apiName == "DataTask_To_MSS")
                    {
                        this.logResponse(taskId, apiName, requestLogResult["responseId"], resObj["message"].ToString(), resObj["code"].ToString(), null);
                        this.postConsumeMSSAPI(taskId, resObj["code"].ToString(), resObj["orderNo"].ToString());
                    }
                }
                else
                {
                    throw new Exception(response.ReasonPhrase);
                }
            }
            catch (Exception e)
            {
                if (apiName == "DataPreparation_To_Wise")
                {
                    this.logResponse(taskId, apiName, requestLogResult["responseId"], null, null, e.Message);
                    this.postConsumeWiseAPI(taskId, null, null, null);
                }
                else if (apiName == "DataTask_To_MSS")
                {
                    this.logResponse(taskId, apiName, requestLogResult["responseId"], null, null, e.Message);
                    this.postConsumeMSSAPI(taskId, null, null);
                }
            }
            #endregion
        }
    }
}

