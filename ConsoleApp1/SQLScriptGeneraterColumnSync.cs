using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;

namespace ConsoleApp1
{
    public class SQLScriptGeneraterColumnSync
    {
        string primaryKey, updateqry, Insertqry, DesTableName = "AbpTenants", TableName = "AbpTenants";
        string updateAdd = "";
        string srcDB = "MSDRiskAnalysisDatabaseTelemedD";
        string targetDB = "MSDRiskAnalysisDatabaseTelemedDB";

        public SQLScriptGeneraterColumnSync()
        {

        }
        //public void Run()
        //{
        //    // ArrayList al = new ArrayList();
        //    string values, IDValues = "", insqry, upqry;
        //    int i = 0;
        //    SqlDataReader myReader;
        //    SqlConnection mySqlConnection = new SqlConnection();
        //    SqlConnection mSqlConnection = new SqlConnection();
        //    SqlCommand mySqlCommand = new SqlCommand();
        //    SqlCommand msqlCommand = new SqlCommand();
        //    string cnnString = "Integrated Security=SSPI;Persist Security Info=False;Initial Catalog=MSDRiskAnalysisDatabaseTelemedD;Data Source=AZHAR-PC\\SQLEXPRESS";
        //    mSqlConnection = new SqlConnection(cnnString);
        //    mySqlConnection = new SqlConnection(cnnString);
        //    mySqlCommand = new SqlCommand("select * from AbpTenants", mySqlConnection);
        //    TableName = "AbpTenants";
        //    primaryKey = GetprimaryKey(TableName);
        //    insqry = "";
        //    mySqlConnection.Open();

        //    myReader = mySqlCommand.ExecuteReader();
        //    if (myReader != null)
        //    {

        //        while (myReader.Read())
        //        {
        //            i = i + 1;
        //            updateAdd = "";
        //            insqry = "";

        //            string celldata = "", coulmenName = "";
        //            for (int j = 0; j < myReader.FieldCount; j++)
        //            {
        //                if (j > 0)
        //                {

        //                    {
        //                        coulmenName += "," + myReader.GetName(j).ToString();
        //                        celldata += ",'" + myReader[j].ToString() + "'";
        //                    }

        //                }
        //                else
        //                {
        //                    coulmenName += myReader.GetName(j).ToString();
        //                    celldata += "'" + myReader[j].ToString() + "'";

        //                }


        //                if (primaryKey == myReader.GetName(j).ToString())
        //                {
        //                    IDValues = myReader[j].ToString();

        //                }
        //                if (IDValues != null)
        //                {
        //                    //Generates the update Query
        //                    upqry = UpdateQuery(coulmenName, celldata, primaryKey, IDValues);
        //                    updateAdd += upqry;
        //                    //Generates the Insert Query
        //                    insqry = InsertQuery(coulmenName, celldata, DesTableName);
        //                }

        //            }

        //            WriteScripts(DesTableName, insqry, updateAdd, IDValues, primaryKey, i);
        //        }
        //        Console.WriteLine("Total number of record in database are=" + i);

        //    }
        //}

        public void AlterTableScripts()
        {

            // ArrayList al = new ArrayList();

            //SqlConnection mySqlConnection = new SqlConnection();
            //SqlConnection mSqlConnection = new SqlConnection();
            //SqlCommand mySqlCommand = new SqlCommand();
            //SqlCommand msqlCommand = new SqlCommand();
            string cnnSourceString = "Integrated Security=SSPI;Persist Security Info=False;Initial Catalog=MSDRiskAnalysisDatabaseTelemedDB;Data Source=AZHAR-PC\\SQLEXPRESS";
            string cnnDestinationString = "Integrated Security=SSPI;Persist Security Info=False;Initial Catalog=MSDRiskAnalysisDatabaseEmmaV4;Data Source=AZHAR-PC\\SQLEXPRESS";
            var sourceTables = GetTableList(cnnSourceString);
            var destinationTables = GetTableList(cnnDestinationString);
            StringBuilder sb = new StringBuilder();
            foreach (var item in sourceTables)
            {
                if (destinationTables.Any(x => x == item))
                {
                    var columnSrcList = GetColumnList(item, cnnSourceString);
                    var columnDestList = GetColumnList(item, cnnDestinationString);
                    var destPrimaryColumn = columnDestList.FirstOrDefault(x => x.PrimaryKey);
                    if (destPrimaryColumn == null)
                        destPrimaryColumn = columnDestList.FirstOrDefault(x => x.IsNullable == false);
                    if (destPrimaryColumn == null)
                        destPrimaryColumn = columnDestList.FirstOrDefault();
                    foreach (var col in columnSrcList)
                    {
                        if (!columnDestList.Any(x => x.ColumnName == col.ColumnName))
                        {
                            var defaultText = "";
                            if (!col.IsNullable)
                            {
                                switch (col.Datatype)
                                {
                                    case "datetime2":
                                        defaultText = $"Default(getdate())";
                                        break;
                                    case "bit":
                                        defaultText = $"Default(0)";
                                        break;
                                    case "int":
                                    case "bigint":
                                        defaultText = $"Default(1)";
                                        break;
                                    default:
                                        break;
                                }
                            }

                            var dataTypeText = col.Datatype;
                            //if (!col.IsNullable)
                            //{
                            switch (col.Datatype)
                            {
                                case "nvarchar":
                                    dataTypeText = col.MaxLength == "-1" ? $"nvarchar(max)" : $"nvarchar({col.MaxLength})";
                                    break;
                                case "decimal":
                                    dataTypeText = $"decimal({col.Precision},{col.Scale})";
                                    break;
                                default:
                                    dataTypeText = col.Datatype;
                                    break;
                            }
                            // }
                            //if(col.Datatype == "" && col)
                            sb.AppendLine($" IF NOT EXISTS ( SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{item}' AND COLUMN_NAME = '{col.ColumnName}') ");
                            sb.AppendLine(" ");
                            sb.AppendLine("BEGIN ");
                            sb.AppendLine($"  ALTER TABLE [{item}] ");
                            sb.AppendLine($"    ADD [{col.ColumnName}] {dataTypeText} {(col.IsNullable ? " null " : " not null ")} {defaultText} ; ");
                            if (col.ColumnName == "Id")
                            {
                                sb.AppendLine(" -- update Id with default serial");
                                sb.AppendLine($" EXEC('   UPDATE x ");
                                sb.AppendLine($"    SET x.Id = x.New_Id ");
                                sb.AppendLine($" FROM( ");
                                sb.AppendLine($"   SELECT Id, ROW_NUMBER() OVER(ORDER BY {destPrimaryColumn.ColumnName}) AS New_Id ");
                                sb.AppendLine($"   FROM [{item}] ");
                                sb.AppendLine($"   ) x '); ");
                                sb.AppendLine(" -- XXXXXXXXX ---------");
                            }
                            sb.AppendLine(" END;  ");
                            sb.AppendLine(" ");
                        }
                    }
                }
            }
            WriteScripts(sb.ToString());
            Console.WriteLine("ALL DONE ####");
        }



        public void TableColumnDataMissmatchScripts()
        {
            string cnnSourceString = "Integrated Security=SSPI;Persist Security Info=False;Initial Catalog=EMMAHostDB_20220809;Data Source=AZHAR-PC\\SQLEXPRESS";
            string cnnDestinationString = "Integrated Security=SSPI;Persist Security Info=False;Initial Catalog=MSDRiskAnalysisDBV3;Data Source=AZHAR-PC\\SQLEXPRESS";
            var sourceTables = GetTableList(cnnSourceString);
            var destinationTables = GetTableList(cnnDestinationString);
            StringBuilder sb = new StringBuilder();
            foreach (var item in sourceTables)
            {
                if (destinationTables.Any(x => x == item))
                {
                    var columnSrcList = GetColumnList(item, cnnSourceString);
                    var columnDestList = GetColumnList(item, cnnDestinationString);
                    
                    sb.AppendLine($"----------- Table {item} ");
                    foreach (var col in columnSrcList)
                    {
                        var destColumn = columnDestList.FirstOrDefault(x => x.ColumnName == col.ColumnName);
                        if (destColumn != null && destColumn.Datatype != col.Datatype)
                        {
                            sb.AppendLine($" Column name: {col.ColumnName} source type : {col.Datatype} nullable : {col.IsNullable}  destination type : {destColumn.Datatype} nullable : {destColumn.IsNullable}") ;
                            sb.AppendLine(" ");
                        }
                    }

                    sb.AppendLine($"--**************************");
                }
            }
            WriteScripts(sb.ToString());
            Console.WriteLine("ALL DONE ####");
        }
        #region this Methods retun ID columan of table which table we pass to it
        public string GetprimaryKey(string tableName)
        {
            string names, ID = "";
            SqlDataReader mReader;
            SqlConnection mSqlConnection = new SqlConnection();
            SqlCommand mSqlCommand = new SqlCommand();
            string cnString = "Integrated Security=SSPI;Persist Security Info=False;Initial Catalog=luna;Data Source=AZHAR-PC\\SQL19";

            mSqlConnection = new SqlConnection(cnString);
            mSqlConnection.Open();
            mSqlCommand = new SqlCommand("sp_pkeys", mSqlConnection);
            mSqlCommand.CommandType = CommandType.StoredProcedure;
            mSqlCommand.Parameters.Add("@table_name", SqlDbType.NVarChar).Value = tableName;
            mReader = mSqlCommand.ExecuteReader();
            while (mReader.Read())
            {
                ID = mReader[3].ToString();

            }

            return ID;
        }
        #endregion
        #region this methods retun  ID values to compaire for insert or Update

        public void WriteScripts(string tableName, string insertqry, string updateqry, string IDvalues, string PrimaryKey, int i)
        {
            string script = "";
            updateqry = "update " + DesTableName + " set " + updateqry + " Where " + PrimaryKey + " = '" + IDvalues + "'";
            int index = updateqry.LastIndexOf(",");
            string updatqry = updateqry.Remove(index, 1);
            if (i == 1)
            {
                script += "DECLARE @updateCount INT;" + Environment.NewLine;
                script += "DECLARE @insertCount INT;" + Environment.NewLine;
                script += "DECLARE @count INT;" + Environment.NewLine;
                script += "SET @updateCount = 0;" + Environment.NewLine;
                script += "SET @insertCount = 0;" + Environment.NewLine;
                script += "SELECT @count = COUNT(*) FROM [" + tableName + "] WHERE [" + PrimaryKey + "] = '" + IDvalues + "'" + Environment.NewLine;
                script += "IF @count = 0" + Environment.NewLine;
                script += "BEGIN ";
                script += insertqry + "" + Environment.NewLine;
                script += "SET @insertCount = @insertCount + 1 " + Environment.NewLine;
                script += "END" + Environment.NewLine;
                script += "ELSE" + Environment.NewLine;
                script += "BEGIN " + Environment.NewLine;
                script += updatqry + "" + Environment.NewLine;
                script += "SET @updateCount = @updateCount + 1 " + Environment.NewLine;
                script += "END" + Environment.NewLine;
                StreamWriter sw = new StreamWriter(@"d:\script1.txt", true, Encoding.UTF8);
                sw.Write(script);
                sw.Close();
            }
            else
            {
                script += "SELECT @count = COUNT(*) FROM [" + tableName + "] WHERE [" + PrimaryKey + "] = '" + IDvalues + "'" + Environment.NewLine;
                script += "IF @count = 0" + Environment.NewLine;
                script += "BEGIN " + Environment.NewLine;
                script += insertqry + "" + Environment.NewLine;
                script += "SET @insertCount = @insertCount + 1 " + Environment.NewLine;
                script += "END" + Environment.NewLine;
                script += "ELSE" + Environment.NewLine;
                script += "BEGIN " + Environment.NewLine;
                script += updatqry + "" + Environment.NewLine;
                script += "SET @updateCount = @updateCount + 1 " + Environment.NewLine;
                script += "END" + Environment.NewLine;
                StreamWriter sw = new StreamWriter(@"d:\script1.txt", true, Encoding.UTF8);
                sw.Write(script);
                sw.Close();
            }
        }

        #endregion
        #region this methods retun insert query and update query

        public string InsertQuery(string coulmenName, string celldata, string TableName)
        {
            return Insertqry = "insert into " + TableName + "(" + coulmenName + ")values(" + celldata + ")";
        }
        public void WriteScripts(string insertqry)
        {

            StreamWriter sw = new StreamWriter(@"d:\script13.txt", false, Encoding.UTF8);
            sw.Write(insertqry);
            sw.Close();

        }
        public string InsertIntoSelectFromQuery(string coulmenName,
             string TableName)
        {
            StringBuilder sb = new StringBuilder();


            sb.AppendLine("");
            sb.AppendLine($"SET IDENTITY_INSERT {TableName} ON ");
            sb.AppendLine("GO");
            sb.AppendLine($"INSERT INTO [{TableName}]  ({ coulmenName })");
            sb.AppendLine($"SELECT {coulmenName} FROM {srcDB}.dbo.[{TableName}]");
            sb.AppendLine("GO");
            sb.AppendLine($"SET IDENTITY_INSERT {TableName} OFF ");
            return sb.ToString();
        }

        public string UpdateQuery(string coulmenName, string celldata, string Name, string Value)
        {
            string IDName, IDValue, Ud = "", name = "", values = "";
            IDName = Name;
            IDValue = Value;

            if (IDName != null)
            {
                int indexcolumn = coulmenName.LastIndexOf(",");
                int indexValues = celldata.LastIndexOf(",");
                if (indexcolumn > 0 && indexValues > 0)
                {
                    coulmenName = coulmenName.Substring(indexcolumn);
                    celldata = celldata.Substring(indexValues);
                    name = coulmenName.Replace(",", "");
                    values = celldata.Replace(",", "");
                    if (name != IDName && values != IDValue)
                    {
                        Ud = name + "=" + values + ",";
                    }

                }
                else
                {
                    name = coulmenName;
                    values = celldata;

                    if (name != IDName && values != IDValue)
                    {
                        Ud = name + "=" + values + ",";
                    }

                }
            }
            return Ud;
        }
        #endregion

        private List<string> GetTableList(string connectionString)
        {
            List<string> tableList = new List<string>();
            using (SqlConnection con = new SqlConnection(connectionString))
            {
                using (SqlCommand cmd = new SqlCommand("SELECT NAME  FROM   SYS.TABLES  WHERE  TYPE = 'U' AND SCHEMA_ID = 1 ", con))
                {
                    cmd.CommandType = CommandType.Text;

                    //cmd.Parameters.Add("@FirstName", SqlDbType.VarChar).Value = txtFirstName.Text;

                    con.Open();
                    var dr = cmd.ExecuteReader();
                    while (dr.Read())
                    {
                        string table = dr["NAME"].ToString();
                        tableList.Add(table);
                    }
                }
            }

            return tableList;
        }

        private List<TableColumn> GetColumnList(string tableName, string connectionString)
        {
            StringBuilder sb = new StringBuilder();

            sb.AppendLine(" SELECT ");
            sb.AppendLine("     c.name 'ColumnName', ");
            sb.AppendLine("     t.Name 'Datatype', ");
            sb.AppendLine("     c.max_length 'MaxLength', ");
            sb.AppendLine("     c.Precision, ");
            sb.AppendLine("     c.Scale, ");
            sb.AppendLine("     c.is_nullable, ");
            sb.AppendLine("     ISNULL(i.is_primary_key, 0) 'PrimaryKey' ");
            sb.AppendLine(" FROM ");
            sb.AppendLine("     sys.columns c ");
            sb.AppendLine(" INNER JOIN ");
            sb.AppendLine("     sys.types t ON c.user_type_id = t.user_type_id ");
            sb.AppendLine(" LEFT OUTER JOIN ");
            sb.AppendLine("     sys.index_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id ");
            sb.AppendLine(" LEFT OUTER JOIN ");
            sb.AppendLine("     sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id ");
            sb.AppendLine(" WHERE ");
            sb.AppendLine($"     c.object_id = OBJECT_ID('{tableName}') ");

            List<TableColumn> tableColumnList = new List<TableColumn>();
            using (SqlConnection con = new SqlConnection(connectionString))
            {
                using (SqlCommand cmd = new SqlCommand(sb.ToString(), con))
                {
                    cmd.CommandType = CommandType.Text;

                    //cmd.Parameters.Add("@FirstName", SqlDbType.VarChar).Value = txtFirstName.Text;

                    con.Open();
                    var dr = cmd.ExecuteReader();
                    while (dr.Read())
                    {
                        var tableColumn = new TableColumn();

                        tableColumn.ColumnName = dr["ColumnName"].ToString();

                        tableColumn.Datatype = dr["Datatype"].ToString();
                        tableColumn.MaxLength = dr["MaxLength"].ToString();
                        tableColumn.Precision = dr["Precision"].ToString();
                        tableColumn.Scale = dr["Scale"].ToString();
                        tableColumn.IsNullable = dr["is_nullable"].ToString() == "True" ? true : false;
                        tableColumn.PrimaryKey = dr["PrimaryKey"].ToString() == "True" ? true : false;
                        //tableColumn.IsNullable = dr["PrimaryKey"].ToString() == "1" ? true : false;
                        tableColumnList.Add(tableColumn);
                    }
                }
            }

            return tableColumnList;
        }

    }

    public class TableColumn
    {
        public string ColumnName { get; set; }
        public string Datatype { get; set; }
        public string MaxLength { get; set; }
        public string Precision { get; set; }
        public string Scale { get; set; }
        public bool IsNullable { get; set; }
        public bool PrimaryKey { get; set; }

    }
}
