using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Text;

namespace ConsoleApp1
{
    public class SQLScriptGenerater
    {
        string primaryKey, updateqry, Insertqry, DesTableName = "AbpTenants", TableName = "AbpTenants";
        string updateAdd = "";
        string srcDB = "MSDRiskAnalysisDatabaseTelemedD";
        string targetDB = "MSDRiskAnalysisDatabaseTelemedDB";

        public SQLScriptGenerater()
        {

        }
        public void Run()
        {
            // ArrayList al = new ArrayList();
            string values, IDValues = "", insqry, upqry;
            int i = 0;
            SqlDataReader myReader;
            SqlConnection mySqlConnection = new SqlConnection();
            SqlConnection mSqlConnection = new SqlConnection();
            SqlCommand mySqlCommand = new SqlCommand();
            SqlCommand msqlCommand = new SqlCommand();
            string cnnString = "Integrated Security=SSPI;Persist Security Info=False;Initial Catalog=MSDRiskAnalysisDatabaseTelemedD;Data Source=AZHAR-PC\\SQLEXPRESS";
            mSqlConnection = new SqlConnection(cnnString);
            mySqlConnection = new SqlConnection(cnnString);
            mySqlCommand = new SqlCommand("select * from AbpTenants", mySqlConnection);
            TableName = "AbpTenants";
            primaryKey = GetprimaryKey(TableName);
            insqry = "";
            mySqlConnection.Open();

            myReader = mySqlCommand.ExecuteReader();
            if (myReader != null)
            {

                while (myReader.Read())
                {
                    i = i + 1;
                    updateAdd = "";
                    insqry = "";

                    string celldata = "", coulmenName = "";
                    for (int j = 0; j < myReader.FieldCount; j++)
                    {
                        if (j > 0)
                        {

                            {
                                coulmenName += "," + myReader.GetName(j).ToString();
                                celldata += ",'" + myReader[j].ToString() + "'";
                            }

                        }
                        else
                        {
                            coulmenName += myReader.GetName(j).ToString();
                            celldata += "'" + myReader[j].ToString() + "'";

                        }


                        if (primaryKey == myReader.GetName(j).ToString())
                        {
                            IDValues = myReader[j].ToString();

                        }
                        if (IDValues != null)
                        {
                            //Generates the update Query
                            upqry = UpdateQuery(coulmenName, celldata, primaryKey, IDValues);
                            updateAdd += upqry;
                            //Generates the Insert Query
                            insqry = InsertQuery(coulmenName, celldata, DesTableName);
                        }

                    }

                    WriteScripts(DesTableName, insqry, updateAdd, IDValues, primaryKey, i);
                }
                Console.WriteLine("Total number of record in database are=" + i);

            }
        }
        string tables_OLD = @"EvalExerciseXSetTbl
,ChiefComplaint
,ExerciseByRegionTbl
,ReportPositiveFindingTbl
,UpperJointPriority
,BodyRegionQuestions
,EvalInterventionTbl
,ReportReferencesTbl
,TestMappingTbl
,ExerciseKeyPointTbl
,BodyRegion
,EvalEvidenceDataTbl
,EvalGoalTbl
,EvalRepeaterItemTbl
,ReportStatusTbl
,TestAnswer
,AspNetUsers
,FysicalScoreRangeXInterventionTbl
,EvalMappingXExerciseTbl
,SuperBillTypeTbl
,EvalExerciseListDataTbl
,IntakeTbl
,EvalMappingXModalityTbl
,SuperBillTbl
,IntakeCounterTbl
,EvalModalityTbl
,SuperBillResponseTbl
,ReportPdfSendListTbl
,Answer
,FysicalScoreRangeXModalityTbl
,SuperBillQuestionsTbl
,AspNetUserRoles
,FcmNotificationTbl
,SubGroupRegionImpact
,EvalReportList
,ExercisePlanTbl
,LogTbl
,AspNetUserLogins
,ExerciseRepeatTbl
,AspNetUserClaims
,AppSubscriptionPayments
,QuestionDialogTbl
,QuestionXVirtualAgentTbl
,ExerciseSyncTbl
,EvalExerciseListBySectionTbl
,EvalMappingProtocolXProviderTbl
,AppInvoices
,ExerciseVoiceTbl
,EvalPlanTbl
,FycialScoresTbl
,LanguageTbl
,FysicalScoreRangeXGoalTbl
,EvalFormListTbl
,FysicalScoreRangeXPlanTbl
,ReportExerciseResponseTbl
,SpineIntegrationGrid
,QuestionVersionTbl
,SpineArea
,RiskAnalysisTbl
,SensoryImpact
,ICD10CodesTbl
,EvalExerciseTrackingDetailsTbl
,ScoreConversion
,AppSubscriptionPaymentsExtensionData
,Question
,NarrativeTbl
,SchedulerEvent
,QuestionGroupHeaderTbl
,SaasEcomUsers
,RuleListTbl
,QuestionBodyLocationTbl
,RomReportModels
,TagTbl
,NotificationServiceTbl
,MindManagerMappingTbl
,AEX
,EvalExerciseTrackingTbl
,NotificationServiceTypeTbl
,MedicalHistory
,DiagnosticsReportTbl
,EvalExercisePhaseListTbl
,MapSubCategoriesTbl
,FysicalScoreRangeXNarrativeTbl
,AppUserDelegations
,ReportExerciseVersionDetailTbl
,MappingResponseTbl
,ReportSpecialTestTbl
,EvalTypeIdTbl
,ReportExerciseVersionListTbl
,MappingResponseAnswersTbl
,GlobalSettingsTbl
,BackgroundJobTbl
,EvalTypeListTbl
,MappingAssessmentAnswerTbl
,MapListTbl
,MapCategoriesTypeTbl
,EvalExerciseCategoryTbl
,ExerciseDataTbl
,MapCategoriesTbl
,LowerJointPriority
,SpecialTestMediaTbl
,PostureGradingTbl
,SpecialTestTbl
,ExamFeatureTbl
,Log4NetLogType
,EvalExerciseXCategoryTbl
,Log4NetLogTbl
,EquipmentTbl
,JournalQuestionnairesTbl
,KinoveaSetting
,MindMapTbl
,EvalModalityMediaTbl
,JournalTbl
,FysicalScoreRangeListTbl
,ReportModalityTbl
,JournalResponesDescTbl
,MapSectionTbl
,AppRecentPasswords
,UserPermissionsTbl
,ExerciseRelatedTbl
,JournalListTbl
,ExerciseTbl
,Joint
,FysicalScoreRangeXExerciseTbl
,IntakeGoal
,SpecialExerciseMediaTbl
,IntakeExercise
,ExerciseKeyPhaseTbl
,AnswerTbl
,EvalMappingXGoalTbl
,ExerciseMeasurementTbl
,EvalMappingXInterventionTbl
,Group
,TemporaryMediaTbl
,EvalMappingProtocolTbl
,EvalMappingXPlanTbl
,PriorityWeightage
,RuleSetTbl
,PreliminaryGroup
,PatientInformation
,NotificationNarrativeTbl
,EvalAssignProtocolTbl
,PageQuestion
,NotificationTypeTbl
,NotificationListTbl
,NotificationRuleListTbl
,QuestionTypeTbl
,ImpactWeightage
,ReferTbl
,ReportGoalTbl
,PageQuestionHeader
,NotificationRuleSetTbl
,AppChatMessages
,ReportPdfTbl
,ReportInterventionTbl
,UserContentTbl
,ReferAPatientTbl
,NotificationTagTbl
,AppFriendships
,RecommendationTbl
,ReportPlanTbl
,AppBinaryObjects
,CareNavigationTbl
,RomReportTbl
,ExerciseAiFailedList
,AspNetRoles
,ExamInformation
,EvalExerciseFrequencyTbl
,ReportQuestionTbl
,ExerciseAiFailedTbl
,AnatomicSubGroup
,MapSectionListTbl
,QuestionRiskTbl
,AllScores
,ActivityHistory
,EvalExerciseMediaTbl
,ReportResponseTbl
,Accounts
,EvalExerciseResponseTbl
,SpecialExerciseTbl
,EvalExerciseTypeListTbl
,EquipmentMediaTbl
,EvalExerciseListTbl
,AnswerSpineWeightage
,EvalExerciseTypeMapTbl
,AnswerRegionWeightage
,AnswerBackupBeforeSystemicChanges
,EvalExerciseXRepsTbl
,FlowBotConditionTbl
,Response
,Final_CC
,EvalExerciseXResistanceTbl
,FlowBotListTbl
,Conversation
,VersionControlTbl
,EvalExerciseXRomCodeTbl
,CompanyDetails
,ReportIcd10Tbl";

        string tables = @"AbpPersistedGrants
,AppSubscriptionPayments
,AppInvoices
,AbpEntityChangeSets
,AbpEntityChanges
,AbpEntityPropertyChanges
,AbpOrganizationUnitRoles
,AppSubscriptionPaymentsExtensionData
,AbpWebhookEvents
,AbpWebhookSubscriptions
,AbpWebhookSendAttempts
,AppUserDelegations
,AbpDynamicProperties
,AbpDynamicEntityProperties
,AbpDynamicPropertyValues
,AbpDynamicEntityPropertyValues
,AbpEditions
,AppRecentPasswords
,AbpAuditLogs
,AnswerMultiLanTbl
,AbpUserAccounts
,AnswerTbl
,AbpUserLoginAttempts
,BodyRegion
,AbpUserOrganizationUnits
,BodyRegionQuestions
,AbpBackgroundJobs
,ChiefComplaint
,AbpLanguages
,EvalExerciseFrequencyTbl
,AbpLanguageTexts
,EvalExerciseListTbl
,AbpNotifications
,EvalExerciseMediaTbl
,AbpNotificationSubscriptions
,EvalExerciseResponseTbl
,AbpTenantNotifications
,EvalExerciseTrackingDetailsTbl
,AbpUserNotifications
,EvalExerciseXRepsTbl
,AbpOrganizationUnits
,EvalExerciseXResistanceTbl
,AbpUsers
,EvalExerciseXSetTbl
,EvalMappingProtocolTbl
,EvalTypeListTbl
,AppChatMessages
,ExamInformation
,AppFriendships
,ExerciseByRegionTbl
,AppBinaryObjects
,ExerciseDataTbl
,AbpFeatures
,ExerciseMeasurementTbl
,AbpUserClaims
,ExerciseRelatedTbl
,ExerciseTbl
,AbpUserLogins
,FycialScoresTbl
,AbpUserRoles
,FysicalScoreRangeXExerciseTbl
,IntakeTbl
,AbpUserTokens
,JournalResponesDescTbl
,AbpSettings
,JournalTbl
,LanguageTbl
,AbpRoles
,NotificationTypeTbl
,Question
,AbpTenants
,QuestionDialogTbl
,ReportPdfTbl
,ReportResponseTbl
,AbpPermissions
,Response
,TestAnswer
,AbpRoleClaims";
        public void InserIntoDestinationSelectFromSource()
        {

            // ArrayList al = new ArrayList();
            string values, IDValues = "", insqry, upqry;
            int i = 0;
            SqlDataReader myReader;
            SqlConnection mySqlConnection = new SqlConnection();
            SqlConnection mSqlConnection = new SqlConnection();
            SqlCommand mySqlCommand = new SqlCommand();
            SqlCommand msqlCommand = new SqlCommand();
            string cnnString = "Integrated Security=SSPI;Persist Security Info=False;Initial Catalog=MSDRiskAnalysisDatabaseTelemedDB;Data Source=AZHAR-PC\\SQLEXPRESS";
            mSqlConnection = new SqlConnection(cnnString);
            mySqlConnection = new SqlConnection(cnnString);
            insqry = "";
            string[] tableList = tables.Split(",");
            foreach (var item in tableList)
            {
                TableName = item.Replace(",", " ").Trim();

                mySqlCommand = new SqlCommand($"SELECT c.name  AS 'ColumnName' FROM sys.columns c JOIN sys.tables t ON c.object_id = t.object_id WHERE t.name ='{TableName}'", mySqlConnection);

                //primaryKey = GetprimaryKey(TableName);

                mySqlConnection.Open();

                myReader = mySqlCommand.ExecuteReader();
                if (myReader != null)
                {
                    StringBuilder sbCoulmenName = new StringBuilder();
                    int j = 0;
                    while (myReader.Read())
                    {
                        i = i + 1;
                        if (j > 0)
                        {
                            {
                                sbCoulmenName.AppendLine($",[{ myReader["ColumnName"].ToString() }]");
                            }

                        }
                        else
                        {
                            sbCoulmenName.AppendLine($"[{myReader["ColumnName"].ToString()}]");
                        }
                        j++;
                    }
                    if (string.IsNullOrEmpty(insqry))
                    {
                        StringBuilder sb = new StringBuilder();
                        sb.AppendLine($"USE {targetDB}");
                        sb.AppendLine($"GO");
                        insqry = sb.ToString();
                    }
                    insqry += InsertIntoSelectFromQuery(sbCoulmenName.ToString(), TableName);

                    Console.WriteLine($"{TableName} ----- Done --");
                }
                mySqlConnection.Close();
            }
            WriteScripts(insqry);
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

            StreamWriter sw = new StreamWriter(@"d:\script1.txt", false, Encoding.UTF8);
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
    }
}
