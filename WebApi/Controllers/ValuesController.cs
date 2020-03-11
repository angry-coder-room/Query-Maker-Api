using Dapper;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Runtime.Serialization;
using System.Web;
using System.Web.Http;
using System.Web.Http.Cors;
using System.Web.Http.Results;
using WebApi.Models;
using System.Web.Script.Serialization;
using System.Web.Mvc;
using RouteAttribute = System.Web.Http.RouteAttribute;
using HttpPostAttribute = System.Web.Http.HttpPostAttribute;

namespace WebApi.Controllers
{
    public class ValuesController : ApiController
    {
        public class CustomBody
        {
            public string Table1 { get; set; }
            public string Table2 { get; set; }
        }

        public class TableColumns
        {
            public string COLUMN_NAME { get; set; }
        }

        public IEnumerable<string> Get()
        {
            return new string[] { "value1", "value2" };
        }

        public class AliasTable
        {
            public string Alias { get; set; }
        }

        [HttpPost]
        [Route("api/PostData")]
        [EnableCors(origins: "*", headers: "*", methods: "*")]
        public string PostData()
        {
            List<AliasTable> AliasTable = new List<AliasTable>();

            string result = "No data returned";

            try
            {
                string table1 = Convert.ToString(HttpContext.Current.Request.Form.GetValues("Table1").FirstOrDefault());
                string table2 = Convert.ToString(HttpContext.Current.Request.Form.GetValues("Table2").FirstOrDefault());
                string tableName1 = Convert.ToString(HttpContext.Current.Request.Form.GetValues("TableName1").FirstOrDefault());
                string tableName2 = Convert.ToString(HttpContext.Current.Request.Form.GetValues("TableName2").FirstOrDefault());
                string joinTable = Convert.ToString(HttpContext.Current.Request.Form.GetValues("JoinTable").FirstOrDefault());
                string joinType = Convert.ToString(HttpContext.Current.Request.Form.GetValues("JoinType").FirstOrDefault());
    
                if (IsValidInput(table1, table2, joinTable, joinType, tableName1, tableName2))
                {
                    string tableAlias1 = BuildAlias(tableName1, AliasTable);
                    string tableAlias2 = BuildAlias(tableName2, AliasTable);

                    DataTable tCol1 = ConvertRowToDataTable(table1);
                    var tbl1Columns = GetColumnNames(tCol1, tableAlias1);

                    DataTable tCol2 = ConvertRowToDataTable(table2);
                    var tbl2Columns = GetColumnNames(tCol2, tableAlias2);

                    string joinRes = "";

                    if (joinType != "CROSS JOIN")
                    {
                        joinRes = GetJoinCondition(joinTable, tableAlias1, tableAlias2);
                    }
                    else
                    {
                        joinRes = "";
                    }

                    string joinQuery = GetJoinType(tableName1 + " " +tableAlias1, tableName2 +" " + tableAlias2, joinType);
                                        
                    result = GetSelectText(tbl1Columns, tbl2Columns, joinQuery, joinRes);

                }
                else
                {
                    return GetError(table1, table2, tableName1, tableName2, joinType, joinTable);
                }

                return result;
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        [HttpPost]
        [Route("api/PostExecute")]
        [EnableCors(origins: "*", headers: "*", methods: "*")]
        public string PostExecute(string sv, string db, string usr, string pwd)
        {
            try
            {
                DataSet ds = new DataSet();
                string sqlQuery = Convert.ToString(HttpContext.Current.Request.Form.GetValues("dataSql").FirstOrDefault()).Replace("\n", "");
                string connectionString = @"Data Source=" + sv + ";Initial Catalog=" + db + ";User ID=" + usr + ";Password=" + pwd;
                using (SqlConnection con = new SqlConnection(connectionString))
                {
                    using (SqlCommand cmd = new SqlCommand())
                    {
                        SqlDataAdapter adapter = new SqlDataAdapter();
                        adapter.SelectCommand = new SqlCommand(sqlQuery, con);
                        adapter.Fill(ds);
                    }
                }
            }

            catch(Exception ex)
            {
                return ex.Message;
            }

            return "Valid";
        }

        private string BuildAlias(string tableName, List<AliasTable> aliasTable)
        {
            string result = tableName.Substring(0,1).ToUpper();
            result = GetAliasName(result, aliasTable);
            return result;
        }

        private string GetAliasName(string tempAlias, List<AliasTable> aliasTable)
        {
            string retAlias = "";
            if (!CheckInAlias(tempAlias, aliasTable))
            {
                retAlias = tempAlias;
                aliasTable.Add(new AliasTable { Alias = tempAlias });
            }
            else
            {
                bool end = true;
                int i = 1;
                while (end)
                {
                    string newAlias = tempAlias;
                    newAlias = newAlias + Convert.ToString(i);
                    if (!CheckInAlias(newAlias, aliasTable))
                    {
                        aliasTable.Add(new AliasTable { Alias = newAlias });
                        end = false;
                        retAlias = newAlias;
                    }
                }
            }
            return retAlias;
        }

        private bool CheckInAlias(string temp, List<AliasTable> aliasTable)
        {
            if (!aliasTable.Any(x => x.Alias == temp))
            {
                return false;
            }
            else
            {
                return true;
            }
        }

        [HttpPost]
        [Route("api/GetColumnNames")]
        [EnableCors(origins: "*", headers: "*", methods: "*")]
        public List<TableColumns> GetColumnNames(string sv, string db, string usr, string pwd, string tblnm)
        {
            string conn = "Data Source="+sv+";Initial Catalog="+db+";User ID="+usr+";Password="+pwd;
            string sql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'"+ tblnm + "'";
            dynamic result;
            using (var connection = new SqlConnection(conn))
            {
                try
                {
                    var affectedRows = connection.Execute(sql);
                    result = connection.Query<TableColumns>(sql);
                    string json = JsonConvert.SerializeObject(result);

                }

                catch (Exception ex)
                {
                    return null;
                }

            }

            return result;
        }


        public bool IsValidInput(string table1, string table2, string joinTable, string joinType, string tableName1, string tableName2)
        {
            if (table1 != "{}" && table2 != "{}" && joinTable != "{}" && joinType != "CROSS JOIN" && joinType != "{}" && tableName1 != "" && tableName2 != "")
            {
                return true;
            }
            if (table1 != "{}" && table2 != "{}" && joinType == "CROSS JOIN"  && tableName1 != "" && tableName2 != "")
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public string GetError(string table1, string table2, string tableName1, string tableName2, string joinType, string joinTable)
        {
            if (table1 == "{}")
            {
                return "ERROR : Columns not provided";
            }
            if (table2 == "{}")
            {
                return "ERROR : Columns not provided";
            }
            if (tableName1 == "")
            {
                return "ERROR : Left Table name missing";
            }
            if (tableName2 == "")
            {
                return "ERROR : Right Table name missing";
            }
            if (joinType == "{}")
            {
                return "ERROR : Select Join";
            }
            if (joinTable == "{}")
            {
                return "ERROR : Join conditions not set";
            }

            return "Some went wrong";
        }

        private string GetJoinType(string tableName1, string tableName2, string joinType)
        {
            return tableName1 + " " + joinType + " " + tableName2;
        }

        private string GetJoinCondition(string joinTable, string tableName1, string tableName2)
        {
            DataTable joinDataTable = ConvertRowToDataTable(joinTable);

            var joinCond = joinDataTable.AsEnumerable().Select(row =>
            new TJoinTable
            {
                TJoinTable1 = row.Field<string>("TJoinTable1"),
                TJoinTable2 = row.Field<string>("TJoinTable2"),
                TJoinCondition = GetCondition(row.Field<string>("TJoinCondition"))
            }).ToList();

            var tempJoin = joinCond.Select(x => tableName1 + "." + x.TJoinTable1 + " " + x.TJoinCondition + " " + tableName2 + "." + x.TJoinTable2).ToList();

            return String.Join(" AND ", tempJoin);

        }

        private string GetCondition(string joinCond)
        {
            if (joinCond == "1")
                return "=";

            if (joinCond == "2")
                return "!=";

            return "=";
        }

        private string GetSelectText(List<QueryTables> tbl1Columns, List<QueryTables> tbl2Columns, string joinType, string joinRes)
        {
            var tColumns1 = tbl1Columns.Where(x => x.TSelectKey == "True").Select(x => x.TTableName + "." + x.TColumnName).ToList();
            var tColumns2 = tbl2Columns.Where(x => x.TSelectKey == "True").Select(x => x.TTableName + "." + x.TColumnName).ToList();

            List<string> mergeList = tColumns1.Concat(tColumns2).ToList();

            string IsOnRequired = "ON";

            if(joinType.Contains("CROSS JOIN"))
            {
                IsOnRequired = "";
            }

            return "\n SELECT " + String.Join(", ", mergeList) + "\n FROM " + joinType + "\n " + IsOnRequired + joinRes;
        }

        private List<QueryTables> GetColumnNames(DataTable tCol, string tableName)
        {
            try
            {
                var result = tCol.AsEnumerable()
                    .Select(x => new QueryTables
                    {
                        TTableName = tableName,
                        TColumnName = x.Field<string>("TColumnName"),
                        TJoinKey = x.Field<string>("TJoinKey"),
                        TSelectKey = x.Field<string>("TSelectKey")
                    }).ToList();

                return result;
            }
            catch(Exception ex)
            {
                return null;
            }
        }

        public DataTable ConvertRowToDataTable(string value)
        {
            JArray ja = new JArray();
            JObject json = string.IsNullOrEmpty(value) ? new JObject() : JObject.Parse(value);
            ja.Insert(0, json);
            DataTable dt = JsonConvert.DeserializeObject<DataTable>(ja.ToString());
            DataTable resultDt = new DataTable();

            List<string> cleanColumnList = new List<string>();
            List<string> colCountList = new List<string>();

            var columnList = (from DataColumn x in dt.Columns select x.ColumnName).ToList();

            foreach (string column in columnList)
            {
                if (column.Contains("_zline"))
                {
                    int index = column.IndexOf("_zline");
                    cleanColumnList.Add(column.Substring(0, index));

                    int colIndex = column.IndexOf("_zline") + 3;
                    colCountList.Add(column.Substring(colIndex, column.Length - colIndex));
                }
            }

            colCountList = colCountList.Distinct().ToList();
            int colStart = 0;
            int colEnd = colCountList.Count;
            cleanColumnList = cleanColumnList.Distinct().ToList();

            foreach (string column in cleanColumnList)
            {
                resultDt.Columns.Add(column, typeof(string));
            }

            foreach (DataRow row in dt.Rows)
            {
                do
                {
                    colStart++;
                    DataRow newRow = resultDt.NewRow();
                    foreach (DataColumn col in dt.Columns)
                    {
                        string dtColumn = col.ToString();
                        if (dtColumn.Contains("_zline" + colStart + "_"))
                        {
                            int index = dtColumn.IndexOf("_zline");
                            string column = dtColumn.Substring(0, index);

                            newRow[column] = row[dtColumn].ToString();
                        }
                    }

                    resultDt.Rows.Add(newRow);
                }
                while (colEnd != colStart);
            }

            DataColumnCollection columns = resultDt.Columns;
            if (!columns.Contains("TJoinKey"))
            {
                resultDt.Columns.Add("TJoinKey", typeof(bool));
            }

            return resultDt;
        }
    }
}
