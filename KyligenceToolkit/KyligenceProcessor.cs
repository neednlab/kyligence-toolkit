using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using Newtonsoft.Json.Linq;

namespace KyligenceToolkit
{
    class KyligenceProcessor
    {
        public static string kapCredential = ConfigurationManager.AppSettings["kapCredential"];
        public static readonly string kapHost = ConfigurationManager.AppSettings["kapHost"];
        public static string kyligenceProjectName;

        // Authorization use Base64 string
        static readonly string cred = Convert.ToBase64String(Encoding.UTF8.GetBytes(kapCredential));

        // REST URLs
        static string kapUrl;
      
        static string listUserUrl;
        static string addUserUrl;

        static string listProjectUserUrl;

        static string getRowAccessUrl;
        static string setRowAccessUrl;

        static string getColumnAccessUrl;
        static string setColumnAccessUrl;

        static string segmentUrl;

        static string getCubeMetadataUrl;

        // Define datetime var
        public static DateTime yesterday = DateTime.Now.AddDays(-1);
        public static int initailYear = 2018;
        public static int currentYear = yesterday.Year;
        public static int currentMonth = yesterday.Month;
        public static int nextYearMonth = yesterday.AddMonths(1).Year * 100 + yesterday.AddMonths(1).Month;
        public static int previousYearMonth = yesterday.AddMonths(-1).Year * 100 + yesterday.AddMonths(-1).Month;
        public static int previous2YearMonth = yesterday.AddMonths(-2).Year * 100 + yesterday.AddMonths(-2).Month;

        static List<int> pastedYearList = new List<int>();

        public KyligenceProcessor(string env, string projectName)
        {
            kyligenceProjectName = projectName;

            if (env == "kc2")
            {
                
                kapUrl = $"https://{kapHost}/kylin/api";

                listUserUrl = $"{kapUrl}/kap/user/users";
                addUserUrl = $"{kapUrl}/kap/user";

                listProjectUserUrl = $"{kapUrl}/access/ProjectInstance/{projectName}";

                getRowAccessUrl = $"{kapUrl}/acl/row/paged/{projectName}";
                setRowAccessUrl = $"{kapUrl}/acl/row/{projectName}";

                getColumnAccessUrl = $"{kapUrl}/acl/column/paged/{projectName}";
                setColumnAccessUrl = $"{kapUrl}/acl/column/{projectName}";

                segmentUrl = $"{kapUrl}/cubes";

                getCubeMetadataUrl = $"{kapUrl}/metastore/export";
            }
            else if (env == "kc3")
            {
                kapUrl = $"http://{kapHost}/kylin/api";

                listUserUrl = $"{kapUrl}/kap/user/users";
                addUserUrl = $"{kapUrl}/kap/user";

                listProjectUserUrl = $"{kapUrl}/access/ProjectInstance/{projectName}";

                getRowAccessUrl = $"{kapUrl}/acl/row/paged/{projectName}";
                setRowAccessUrl = $"{kapUrl}/acl/row/{projectName}";

                getColumnAccessUrl = $"{kapUrl}/acl/column/paged/{projectName}";
                setColumnAccessUrl = $"{kapUrl}/acl/column/{projectName}";

                segmentUrl = $"{kapUrl}/models";

                getCubeMetadataUrl = $"{kapUrl}/metastore/backup/models?project={projectName}";
            }

            for (int year = initailYear; year < currentYear; year++)
            {
                // Do not add last year if JAN
                if (currentMonth == 1 && year == currentYear - 1)
                {
                    continue;
                }
                pastedYearList.Add(year);
            }

        }

        private enum RequestMethod {GET , POST, PUT};
        private enum InfoType {DEBUG, INFO, WARN, ERROR};

        private static void Print(InfoType info, string msg)
        {
            if (info != InfoType.DEBUG )
            {
                Console.WriteLine($"{info}: {msg} at {DateTime.Now}");
            }
        }

        // REST API v4
        private static string KylingenceRest(string requestUrl, RequestMethod method, string body = "", bool isSaveAsFile = false, string outputFilePath = @"C:\Kyligence\backup")
        {
            try
            {
                HttpWebRequest request = WebRequest.Create(requestUrl) as HttpWebRequest;
                request.Method = method.ToString();
                request.ContentType = "application/json;charset=utf-8";
                request.Headers.Add("Authorization", $"Basic {cred}");
                request.Headers.Add("Accept-Language", "en");
                request.Accept = "application/vnd.apache.kylin-v4-public+json";

                // Add BODY
                if (body != "")
                {
                    byte[] byteData = Encoding.UTF8.GetBytes(body);
                    request.ContentLength = byteData.Length;


                    using (Stream postStream = request.GetRequestStream())
                    {
                        postStream.Write(byteData, 0, byteData.Length);
                  
                    }
                }
             
                using (HttpWebResponse response = request.GetResponse() as HttpWebResponse)
                {
                    Stream stream = response.GetResponseStream();
                    StreamReader reader = new StreamReader(stream);

                    if (isSaveAsFile)
                    {
                        int bufferSize = 2048;
                        byte[] bytes = new byte[bufferSize];

                        string today = DateTime.Today.ToString("yyyyMMdd");

                        string backupFilePath = $"{outputFilePath}\\model_{today}.zip";

                        FileStream fs = new FileStream(backupFilePath, FileMode.Create);

                        int length = stream.Read(bytes, 0, bufferSize);

                        while (length > 0)
                        {
                            fs.Write(bytes, 0, length);
                            length = stream.Read(bytes, 0, bufferSize);
                        }
                        stream.Close();
                        fs.Close();
                        response.Close();

                        return "Success";

                    }
                    else
                    {
                        return reader.ReadToEnd();
                    }
                }


            }
            catch (Exception ex)
            {
                Print(InfoType.ERROR, ex.Message);
                return "";
            }


        }


        // KE Access
        //========================================================================================================================
        public bool CheckUserExists(string userName)
        {
            string resUrl = listUserUrl + $"?name={userName}";

            string res = KylingenceRest(resUrl, RequestMethod.GET);

            bool userExists = false;
            if (res.Contains($"\"username\":\"{userName}\""))
            {
                userExists = true;
                Print(InfoType.DEBUG, $"USER [{userName}] already exists!");
            }
            
            return userExists;
        }

        public void AddUser(string userName)
        {
            // Check if user exists
            if (!CheckUserExists(userName)) 
            {
                string resUrl = addUserUrl + $"/{userName}";
                string defaultPassword = "Cqm@g14dss";
                string defaultRole = "ROLE_ANALYST";
                string body = "{" + $"\"password\":\"{defaultPassword}\",\"disabled\":false,\"authorities\":[\"{defaultRole}\"]" + "}";

                KylingenceRest(resUrl, RequestMethod.POST, body);

                Print(InfoType.INFO, $"USER [{userName}] added, default PASSWORD is {defaultPassword}");
            }

        }

        public bool CheckProejctUserExists(string userName)
        {
            string resUrl =  listProjectUserUrl + $"?pageSize=10000";

            string res = KylingenceRest(resUrl, RequestMethod.GET);

            bool userExists = false;
            if (res.Contains($"\"principal\":\"{userName}\""))
            {
                userExists = true;
                Print(InfoType.DEBUG, $"USER [{userName}] already exists in PROJECT [{kyligenceProjectName}]!");
            }

            //Console.WriteLine(res);
            return userExists;
        }

        public void AddProjectUser(string userName)
        {
            if (!CheckProejctUserExists(userName))
            {
                string resUrl = listProjectUserUrl;
                string defaultPermission = "READ";

                // true = user; false = group
                string defaultUserType = "true";
                string body = $"{{\"permission\":\"{defaultPermission}\",\"principal\":{defaultUserType},\"sid\":\"{userName}\"}}";

                KylingenceRest(resUrl, RequestMethod.POST, body);
                Print(InfoType.INFO, $"USER [{userName}] added in PROJECT [{kyligenceProjectName}]");      
            }

        }

        public bool CheckRowLevelAccessExists(string userName, string tableName)
        {
            string resUrl = getRowAccessUrl + $"/{tableName}?pageSize=10000";
            string res = KylingenceRest(resUrl, RequestMethod.GET);

            JObject resJson = JObject.Parse(res);
            bool userExists = false;

            var users = resJson["data"]["user"];
            
            foreach (var user in users)
            {
               if (user[$"{userName}"] != null)
               {
                    userExists = true;
                    Print(InfoType.INFO, $"USER [{userName}] row level access already exists!");
               }
            }

            return userExists;
        }

        public bool CheckColumnLevelAccessExists(string userName, string tableName)
        {
            string resUrl = getColumnAccessUrl + $"/{tableName}?pageSize=10000";
            string res = KylingenceRest(resUrl, RequestMethod.GET);

            JObject resJson = JObject.Parse(res);
            bool userExists = false;

            var users = resJson["data"]["user"];

            foreach (var user in users)
            {
                if (user[$"{userName}"] != null)
                {
                    userExists = true;
                    Print(InfoType.INFO, $"USER [{userName}] column level access already exists!");
                }
            }

            return userExists;
        }

        public void AddRowLevelAccess(string userName, string tableName, string columnName, string accessValue)
        {
            string accessType = "user";
            string resUrl = setRowAccessUrl + $"/{accessType}/{tableName}/{userName}";
            string[] accessValueList = accessValue.Split('|');

            string accessValueTemplate =
            @"
                    {{
                    ""type"":""CLOSED"",
                    ""leftExpr"":""{0}"",
                    ""rightExpr"":""{1}""
                    }}
                ";
            string accessValueJson = "";

            // accessValueList has multi-value like WHERE Col IN (x,y)
            foreach (string value in accessValueList)
            {
                accessValueJson += string.Format(accessValueTemplate, value, value) + ",";
            }
            accessValueJson = accessValueJson.Trim(',');
    

            string body = @"{{
                 ""condsWithColumn"":{{
                    ""{0}"":[{1}]
                    }}
                }}";
            body = string.Format(body, columnName, accessValueJson);

            // PUT : change row level access
            // POST : add row level access
            RequestMethod method;
            string op;

            if (CheckRowLevelAccessExists(userName, tableName))
            {
                method = RequestMethod.PUT;
                op = "updated";
            }
            else
            {
                method = RequestMethod.POST;
                op = "added";
            }
       
            KylingenceRest(resUrl, method, body);
            Print(InfoType.INFO, $"USER [{userName}] {op} row level access : \"{columnName} = {accessValue}\"");

        }

        public void AddColumnLevelAccess(string userName, string tableName, string columns)
        {
            string accessType = "user";
            string resUrl = setColumnAccessUrl + $"/{accessType}/{tableName}/{userName}";
            string[] columnList = columns.Split('|');

            string accessValueTemplate =
            @"[{0}]";
            string restrictColumns = "";

            // accessValueList has multi-value like WHERE Col IN (x,y)
            foreach (string columnName in columnList)
            {
                restrictColumns += "\"" + columnName + "\",";       
            }
            restrictColumns = restrictColumns.Trim(',');
            string body = string.Format(accessValueTemplate, restrictColumns);


            // PUT : change row level access
            // POST : add row level access
            RequestMethod method;
            string op;

            
            if (CheckColumnLevelAccessExists(userName, tableName))
            {
                method = RequestMethod.PUT;
                op = "updated";
            }
            else
            {
                method = RequestMethod.POST;
                op = "added";
            }
            

            KylingenceRest(resUrl, method, body);
            Print(InfoType.INFO, $"USER [{userName}] {op} column level access : \"{columns}\"");

        }

        public void ProcessUserList(string tableName, string userListFilePath = "Resource/user_list.csv")
        {
            var rows = File.ReadLines(userListFilePath, Encoding.UTF8).Skip(1);

            foreach (string row in rows)
            {
                string[] columns = row.Split(',');

                string userName = columns[0];
                string accessLevel = columns[1];
                string accessValue = columns[2];
                string restrictTable = columns[3];
                string restrictColumns = columns[4];

                // Add user into KE (if not exists)
                AddUser(userName);

                // Add user into PROJECT (if not exists)
                AddProjectUser(userName);

                // Add row level access
                AddRowLevelAccess(userName, tableName, accessLevel, accessValue);

                // Add column level restriction (if exists)
                if (!string.IsNullOrEmpty(restrictTable))
                {
                    AddColumnLevelAccess(userName, restrictTable, restrictColumns);
                }
            }

        }
        //========================================================================================================================

        // KE model
        //========================================================================================================================
        // General method
        private static long ConvertGmtTimestamp(int inputDate)
        {
            //TimeZone.CurrentTimeZone.ToLocalTime(new DateTime(1970, 1, 1));
            DateTime baseDateTime = new DateTime(1970, 1, 1);
            int year = inputDate / 10000;
            int month = inputDate / 100 % 100;
            int day = inputDate % 100;

            DateTime inputDateTime = new DateTime(year, month, day);

            long timeStamp = (long)(inputDateTime - baseDateTime).TotalMilliseconds;

            //Console.WriteLine(timeStamp);
            return timeStamp;
        }

        private void BuildSegment(string modelName, int startDate, int endDate, string durationType)
        {
            long startTimeStamp = 0;
            long endTimeStamp = 0;
            if (durationType == "timestamp")
            {
                startTimeStamp = ConvertGmtTimestamp(startDate);
                endTimeStamp = ConvertGmtTimestamp(endDate);
            }
            else if (durationType == "year")
            {
                startTimeStamp = startDate;
                endTimeStamp = endDate;
            }
            else if (durationType == "full")
            {
                startTimeStamp = 0;
                endTimeStamp = 0;
            }

            string resUrl = segmentUrl + $"/{modelName}/segments";
            string body = @"{{
                ""project"":""{0}"",
                ""start"":""{1}"",
                ""end"":""{2}""
            }}";
            body = string.Format(body, kyligenceProjectName, startTimeStamp, endTimeStamp);

            KylingenceRest(resUrl, RequestMethod.POST, body);
            Print(InfoType.INFO, $"Model [{modelName}] bulit a segment from {startDate} to {endDate}");
        }

        private void MergeSegment(string modelName, string segmentArray)
        {
            if (string.IsNullOrEmpty(segmentArray))
            {
                Print(InfoType.WARN, $"Model [{modelName}] has not these segments");
            }
            else
            {
                string resUrl = segmentUrl + $"/{modelName}/segments";

                string body = @"{{
                ""project"":""{0}"",
                ""ids"":[{1}],
                ""type"":""MERGE""
                }}";
                body = string.Format(body, kyligenceProjectName, segmentArray);


                KylingenceRest(resUrl, RequestMethod.PUT, body);

                Print(InfoType.INFO, $"Model [{modelName}] segment [{segmentArray}] started merging");
            }

        }

        private void RefreshSegment(string modelName, string segmentArray)
        {
            if (string.IsNullOrEmpty(segmentArray))
            {
                Print(InfoType.WARN, $"Model [{modelName}] has not these segments");
            }
            else
            {

                string resUrl = segmentUrl + $"/{modelName}/segments";

                string body = @"{{
                ""project"":""{0}"",
                ""ids"":[{1}],
                ""type"":""REFRESH""
                }}";
                body = string.Format(body, kyligenceProjectName, segmentArray);

                KylingenceRest(resUrl, RequestMethod.PUT, body);

                Print(InfoType.INFO, $"Model [{modelName}] segment [{segmentArray}] started refresh");
            }

        }

        private string GetSegmentIdsStringArray(string modelName, DataTable segmentTable)
        {

            // segment ids
            List<string> segmentIds = new List<string>();
            string segmentArray = "";

            string resUrl = segmentUrl + $"/{modelName}/segments?project={kyligenceProjectName}&page_size=500";


            // call GET return a segment JSON
            Print(InfoType.INFO, $"Getting segments of model [{modelName}]");
            string resultJsonString = KylingenceRest(resUrl, RequestMethod.GET);

            if (!string.IsNullOrEmpty(resultJsonString))
            {

                JObject resJson = JObject.Parse(resultJsonString);
                JToken idJson = resJson["data"]["value"];

                foreach (DataRow segment in segmentTable.Rows)
                {
                    foreach (JObject id in idJson)
                    {
                        string segmentId = id["id"].ToString();
                        string segmentDuration = id["name"].ToString();

                        if (((segment[1] + "000000" + "_" + segment[2] + "000000") == segmentDuration) || segmentDuration == "FULL_BUILD")
                        {
                            segmentIds.Add(segmentId);
                            Print(InfoType.INFO, $"Get segment {segmentId} of model [{modelName}]");
                        }
                    }

                }

                // bulid a string of segment array
                foreach (string segmentId in segmentIds)
                {
                    segmentArray += $"\"{segmentId}\",";
                }

                segmentArray = segmentArray.Trim(',');

                return segmentArray;
            }
            else
            {
                return "";
            }
        }


        // Project method
        private string GetRefreshSegmentIdStringArray(string modelName, string partitionMode, string refreshMode)
        {
            
            // Define segment
            DataTable segmentTable = new DataTable("segmentTable");

            DataColumn[] segmentTableHeader =
            {
                new DataColumn("model_name",typeof(string)),
                new DataColumn("start_date",typeof(int)) ,
                new DataColumn("end_date",typeof(int)) ,
                new DataColumn("partition_mode",typeof(string))
            };
            segmentTable.Columns.AddRange(segmentTableHeader);

            // Define duration by partition mode
            // month/year/full
            List<List<int>> duration = new List<List<int>>();

            // month
            if (partitionMode == "year")
            {
                // Morning mode : refresh current 1 year
                if (refreshMode == "morning")
                {
                    duration.Add(new List<int> { currentYear, currentYear + 1 });
                }
                // Night mode: refresh past years
                else if (refreshMode == "night")
                {
                    foreach (int year in pastedYearList)
                    {
                        duration.Add(new List<int> { year, year + 1 });
                    }

                }
            }
            else if (partitionMode == "month")
            {
                // Morning mode : refresh last 2 months
                if (refreshMode == "morning")
                {
                    duration.Add(new List<int> { previousYearMonth * 100 + 1, currentYear * 10000 + currentMonth * 100 + 1 });
                    duration.Add(new List<int> { currentYear * 10000 + currentMonth * 100 + 1, nextYearMonth * 100 + 1 });
                }
                // Night mode: refresh past years + JAN ~last 2 months
                else if (refreshMode == "night")
                {
                    foreach (int year in pastedYearList)
                    {
                        duration.Add(new List<int> { year * 10000 + 101, (year + 1) * 10000 + 101 });
                    }

                    // JAN ~last 2 months
                    duration.Add(new List<int> { currentYear * 10000 + 101, previousYearMonth * 100 + 1 });
                }

            }
            else if (partitionMode == "full")
            {
                duration.Add(new List<int> { 0, 0 });
            }

            // Add rows into segmentDurtion
            foreach (List<int> segmentDurtion in duration)
            {

                DataRow segmentDataRow = segmentTable.NewRow();
                segmentDataRow[0] = modelName;
                segmentDataRow[1] = segmentDurtion[0];
                segmentDataRow[2] = segmentDurtion[1];
                segmentDataRow[3] = partitionMode == "month" ? "timestamp" : partitionMode;

                segmentTable.Rows.Add(segmentDataRow);
            }

            // segment ids
            string segmentArray = GetSegmentIdsStringArray(modelName: modelName, segmentTable: segmentTable);

            return segmentArray;
        }

        private string GetMergeSegmentIdStringArray(string modelName)
        {

            // Define segment
            DataTable segmentTable = new DataTable("segmentTable");

            DataColumn[] segmentTableHeader =
            {
                new DataColumn("model_name",typeof(string)),
                new DataColumn("start_date",typeof(int)) ,
                new DataColumn("end_date",typeof(int)) 
            };
            segmentTable.Columns.AddRange(segmentTableHeader);

            // Define duration by partition mode
            // month/year/full
            List<List<int>> duration = new List<List<int>>();

            // JAN ~ last 2 month
            duration.Add(new List<int> { currentYear * 10000 + 101, previous2YearMonth * 100 + 1 });
            duration.Add(new List<int> { previous2YearMonth * 100 + 1, previousYearMonth * 100 + 1 });
                
            // Add rows into segmentDurtion
            foreach (List<int> segmentDurtion in duration)
            {

                DataRow segmentDataRow = segmentTable.NewRow();
                segmentDataRow[0] = modelName;
                segmentDataRow[1] = segmentDurtion[0];
                segmentDataRow[2] = segmentDurtion[1];

                segmentTable.Rows.Add(segmentDataRow);
            }

            // segment ids
            string segmentArray = GetSegmentIdsStringArray(modelName: modelName, segmentTable: segmentTable);

            return segmentArray;
        }

        private void RefreshSingleModelSegments(string modelName, string partitionMode, string refreshMode)
        {

            string segmentArray = GetRefreshSegmentIdStringArray(modelName: modelName, partitionMode: partitionMode, refreshMode: refreshMode);
            
            RefreshSegment(modelName: modelName, segmentArray: segmentArray);
        }

        private void BuildAndMergeSingleModelSegments(string modelName, string partitionMode)
        {
            int refreshStartDate;
            int refreshEndDate;

            // Add new segment [Current Year] ~ [Next Year] when partitionMode = "year" on Jan 2nd
            // Add new segment [Current Month] ~ [Next Month] when partitionMode = "month" on every 2nd    
            if (currentMonth == 1 && partitionMode == "year")
            {
                refreshStartDate = currentYear * 10000 + 101;
                refreshEndDate = (currentYear + 1) * 10000 + 101;
            }
            else if (currentMonth != 3 && partitionMode == "month")
            {
                refreshStartDate = currentYear * 10000 + currentMonth * 100 + 1;
                refreshEndDate = nextYearMonth * 100 + 1;

            }
            else
            {
                Print(InfoType.INFO, $"Model [{modelName}] does not need build or merge segment");
                return;
            }

            BuildSegment(   modelName: modelName,
                            startDate: refreshStartDate,
                            endDate: refreshEndDate,
                            durationType: partitionMode == "month" ? "timestamp" : partitionMode);

            string segmentArray = GetMergeSegmentIdStringArray(modelName: modelName);
            MergeSegment(modelName: modelName, segmentArray: segmentArray);
                
            

        }


        // Public method
        // Batch 99 is for BuildAndMergeSingleModelSegments test
        public void ProcessModelBatch(int processBatch, string refreshMode, string cubeListFilePath = "Resource/cube_refresh_list.csv")
        {
            // Read CSV
            var rows = File.ReadLines(cubeListFilePath, Encoding.UTF8).Skip(1);

            // Process CSV
            foreach (string row in rows)
            {
                string[] columns = row.Split(',');

                string modelName = columns[0];
                string partitionMode = columns[1];
                int batch = Convert.ToInt32(columns[2]);

                // Process current batch
                if (batch == processBatch)
                {
                    RefreshSingleModelSegments(
                                               modelName: modelName,
                                               partitionMode: partitionMode,
                                               refreshMode: refreshMode);

                    if ((yesterday.Day == 1 && refreshMode  == "morning" )|| processBatch == 99) 
                    {
                        BuildAndMergeSingleModelSegments(modelName: modelName,
                                                         partitionMode: partitionMode);
                    }
                }
            }

        }
        
        public void ExportModelMetadata()
        {
            string resUrl = getCubeMetadataUrl;
            string body = @"{""names"": [
                             ""vip"", 
                             ""vip_point"",
                             ""vip_sales"",
                             ""vip_sales_item"",
                             ""coupon_analysis"", 
                             ""pos_payment"",
                             ""pos_sales"",
                             ""pos_sales_item_production"",
                             ""pos_promotion"",
                             ""pos_budget"",
                             ""pos_target""
                            ]}";
            KylingenceRest(resUrl, RequestMethod.POST, body, true);

            Print(InfoType.INFO, $"Project [{kyligenceProjectName}] backuped");
        }

    }
}
