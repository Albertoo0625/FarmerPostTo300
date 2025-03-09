using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text.Json;
namespace FarmerPostTo300.src
{
    class PostTo300
    {
        SqlConnection conn = null;
        SqlCommand cmd = null;
        SqlDataReader rdr;
        Connection MyConnection = new Connection();

        public int ColumnExists(string table_name, string COLUMN_NAME)
        {
            int TempResult = 0;
            conn = MyConnection.CompanyDbConnect();
            conn.Open();
            string checkQ = "select COLUMN_NAME from information_schema.columns where table_name ='" + table_name + "'  AND COLUMN_NAME = '" + COLUMN_NAME + "'";
            cmd = new SqlCommand(checkQ, conn);
            rdr = cmd.ExecuteReader();
            if (rdr.Read())
            {
                TempResult = 1;
            }
            else
            {
                TempResult = 0;
            }

            return TempResult;
        }

        public void CreateTableColumns()
        {
            if (ColumnExists("UnitLineBatch", "PostTo300Status") == 0)
            {
                conn = MyConnection.CompanyDbConnect();
                conn.Open();
                string Quiz = "ALTER TABLE UnitLineBatch ADD PostTo300Status INT DEFAULT 1";
                cmd = new SqlCommand(Quiz, conn);
                cmd.CommandTimeout = 120;
                cmd.ExecuteReader();

                //conn.Close();
                //conn = MyConnection.CompanyDbConnect();
                //conn.Open();
                //string Quiz1 = "UPDATE UnitLineBatch SET PostTo300Status = 1 ";
                //cmd = new SqlCommand(Quiz1, conn);
                //cmd.ExecuteReader();
            }
        }


        public List<Dictionary<string, object>> GetPayslipEarnLine(string EmployeeCode)
        {
            conn = MyConnection.CompanyDbConnectSage();
            conn.Open();
            string query = "SELECT PayslipEarnLineID FROM Payroll.PayslipEarnLine " +
                "WHERE EmpEarningDefID = (SELECT TOP 1 EmpEarningDefID FROM Payroll.EmpEarningDef" +
                " WHERE PayslipDefID = (SELECT TOP 1 PayslipDefID FROM Payroll.PayslipDef " +
                "WHERE EmployeeRuleID = (SELECT TOP 1 EmployeeRuleID FROM Employee.EmployeeRule " +
                "WHERE EmployeeID = (SELECT TOP 1 EmployeeID FROM Employee.Employee " +
                "WHERE EmployeeCode = @EmployeeCode))))";
            cmd = new SqlCommand(query, conn);
            cmd.Parameters.AddWithValue("@EmployeeCode", EmployeeCode);
            List<Dictionary<string, object>> result = new List<Dictionary<string, object>>();
            using (SqlDataReader reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    Dictionary<string, object> row = new Dictionary<string, object>();
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        row[reader.GetName(i)] = reader.GetValue(i);
                    }
                    result.Add(row);
                }
            }

            return result;
        }




        public List<Dictionary<string, object>> GetUnitLevelDefinitionID(string HeirarchyHeaderCode)
        {

            conn = MyConnection.CompanyDbConnectSage();
            conn.Open();
            string levelQuery = @"
                        SELECT TOP 1 LevelDefinitionID
                        FROM JobCosting.LevelDefinition
                        WHERE HierarchyHeaderID = (
                            SELECT TOP 1 HierarchyHeaderID 
                            FROM Entity.HierarchyHeader 
                            WHERE Code = @HierarchyHeaderCode)";
            cmd = new SqlCommand(levelQuery, conn);
            cmd.Parameters.AddWithValue("@HierarchyHeaderCode", HeirarchyHeaderCode);
            List<Dictionary<string, object>> result = new List<Dictionary<string, object>>();
            using (SqlDataReader reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    Dictionary<string, object> row = new Dictionary<string, object>();
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        row[reader.GetName(i)] = reader.GetValue(i);
                    }
                    result.Add(row);
                }
            }

            return result;
        }



        public void Post300()
        {
            try
            {
                CreateTableColumns();
                DataTable dt = new DataTable();
                conn = MyConnection.CompanyDbConnect();
                conn.Open();

                string saveQ1 = @"  
                              DECLARE @StartDate DATETIME = '2024-09-01';
                              DECLARE @EndDate DATETIME = '2024-09-30';

                              IF NOT EXISTS (
                              SELECT 1 
                              FROM sys.indexes 
                              WHERE name = 'idx_UnitLineBatch_PostTo300Status_FarmerCode' 
                              AND object_id = OBJECT_ID('UnitLineBatch')
                              )
                              BEGIN
                                  PRINT 'Creating index idx_UnitLineBatch_PostTo300Status_FarmerCode...';
                                  CREATE INDEX idx_UnitLineBatch_PostTo300Status_FarmerCode 
                                  ON UnitLineBatch (PostTo300Status, FarmerCode) 
                                  INCLUDE ([Pay Run Definition], Note, Units,ItemDescription,[JobGradeCode],[JobTitleCode] ,[Member Rate], [Date Worked], [End Date], RouteCode);
                                  PRINT 'Index created successfully.';
                              END
                              ELSE
                              BEGIN
                                  PRINT 'Index already exists.';
                              END
                              
                              
                              PRINT 'Executing optimized query...';
                              SELECT TOP 10 
                                  FarmerCode, 
                                  [Pay Run Definition], 
                                  Note, 
                                  Units,
                                  ItemDescription,
                                  [JobGradeCode],
                                  [JobTitleCode],
                                  [Member Rate], 
                                  [Date Worked], 
                                  [End Date], 
                                  RouteCode 
                              FROM UnitLineBatch 
                              WHERE [Date Worked] BETWEEN @StartDate AND @EndDate
                              ORDER BY FarmerCode";
                SqlCommand cmd2 = new SqlCommand(saveQ1, conn);
                cmd2.CommandTimeout = 1200;
                using (SqlDataReader rdr = cmd2.ExecuteReader())
                {
                    dt.Load(rdr);
                }

                List<Dictionary<string, object>> result = new List<Dictionary<string, object>>();
                conn = MyConnection.CompanyDbConnect();
                conn.Open();

                using (SqlDataReader reader = cmd2.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        Dictionary<string, object> row = new Dictionary<string, object>();

                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            row[reader.GetName(i)] = reader.GetValue(i);
                        }

                        // Construct HierarchyList with two entries
                        List<Dictionary<string, string>> hierarchyList = new List<Dictionary<string, string>>
                            {
                                new Dictionary<string, string>
                                {
                                    { "HierarchyHeaderCode", "ACTIVITY" },
                                    { "HierarchyCode", row.ContainsKey("ItemDescription") && row["ItemDescription"]?.ToString() == "GREEN LEAF" ? "GL" : row["ItemDescription"]?.ToString() ?? "" }
                                },
                                new Dictionary<string, string>
                                {
                                    { "HierarchyHeaderCode", "ROUTE" },
                                    { "HierarchyCode", row.ContainsKey("RouteCode") ? row["RouteCode"].ToString() : "" } // RouteCode from database
                                }
                            };

                        // Add the HierarchyList to the row
                        row["HierarchyList"] = hierarchyList;

                        result.Add(row);
                    }
                }

                // Serialize the result as JSON (array format)
                string jsonResult = JsonSerializer.Serialize(result, new JsonSerializerOptions { WriteIndented = true });
                Console.WriteLine(jsonResult);
                // Deserialize the JSON back into a List of Dictionaries
                var deserializedResult = JsonSerializer.Deserialize<List<Dictionary<string, object>>>(jsonResult);

                // Debug Output
                Console.WriteLine(jsonResult);
                conn.Close();
                conn = MyConnection.CompanyDbConnectSage();
                conn.Open();

                string query = @"
                INSERT INTO Tea_Auto.Payroll.PayslipEarnUnit 
                (TransactionCodeID, PayslipEarnLineID, LevelDefinitionIDs, Units, Amount, CalculationAmount, CalculationAmountRuleCCY, 
                 EmployeeRate, DateWorked, DateCaptured, CapturedBy, UserID, EndDate, Note, LastChanged)
                VALUES 
                (@TransactionCodeID, @PayslipEarnLineID, @LevelDefinitionIDs, @Units, @Amount, @CalculationAmount, 
                 @CalculationAmountRuleCCY, @EmployeeRate, @DateWorked, @DateCaptured, @CapturedBy, @UserID, @EndDate, @Note, @LastChanged)";

                using (SqlCommand insertCommand = new SqlCommand(query, conn))
                {
                    foreach (var data in deserializedResult)
                    {

                        foreach (var hierarchyItem in JsonSerializer.Deserialize<List<Dictionary<string, string>>>(data["HierarchyList"].ToString()))
                        {
                            Console.WriteLine($"Processing HierarchyHeaderCode: {hierarchyItem["HierarchyHeaderCode"]}");

                            insertCommand.Parameters.Clear();

                            string hierarchyHeaderCodeString = hierarchyItem["HierarchyHeaderCode"].ToString();
                            Console.WriteLine($"test from insert {hierarchyHeaderCodeString}");
                            List<Dictionary<string, object>> LevelIDCodeString = this.GetUnitLevelDefinitionID(hierarchyHeaderCodeString);
                            string LevelId = LevelIDCodeString[0]["LevelDefinitionID"].ToString();
                            Console.WriteLine($"test from insert {LevelId}");


                            string farmerCode = data["FarmerCode"] is JsonElement jsonFarmer ? jsonFarmer.GetString() ?? "" : data["FarmerCode"].ToString();

                            //TODO: add checks for mismatch in farmer number
                            List<Dictionary<string, object>> FarmerCodeString = this.GetPayslipEarnLine(farmerCode);
                            string payslipId = FarmerCodeString[0]["PayslipEarnLineID"].ToString();

                            // ✅ Ensure correct type conversion

                            string payRunDefinition = data["Pay Run Definition"] is JsonElement jsonPayRun ? jsonPayRun.GetString() ?? "" : data["Pay Run Definition"].ToString();
                            string note = data["Note"] is JsonElement jsonNote ? (jsonNote.ValueKind == JsonValueKind.String ? jsonNote.GetString() ?? "" : JsonSerializer.Serialize(jsonNote)) : data["Note"]?.ToString() ?? "";


                            decimal units = data["Units"] is JsonElement jsonUnits ? jsonUnits.GetDecimal() : Convert.ToDecimal(data["Units"]);
                            decimal memberRate = data["Member Rate"] is JsonElement jsonRate ? jsonRate.ValueKind == JsonValueKind.Number ? jsonRate.GetDecimal() : decimal.TryParse(jsonRate.GetString(), out decimal val) ? val : 0 : Convert.ToDecimal(data["Member Rate"]);

                            DateTime dateWorked = data["Date Worked"] is JsonElement jsonDate ? jsonDate.GetDateTime() : Convert.ToDateTime(data["Date Worked"]);
                            DateTime endDate = DateTime.TryParse(data["End Date"]?.ToString(), out DateTime parsedDate) && parsedDate >= new DateTime(1753, 1, 1) ? parsedDate : new DateTime(1753, 1, 1);


                            insertCommand.Parameters.AddWithValue("@TransactionCodeID", 1);
                            insertCommand.Parameters.AddWithValue("@PayslipEarnLineID", payslipId);
                            insertCommand.Parameters.AddWithValue("@LevelDefinitionIDs", LevelId);
                            insertCommand.Parameters.AddWithValue("@CompanyCode", "TET_TEA");
                            insertCommand.Parameters.AddWithValue("@CompanyRuleCode", "FARM");

                            insertCommand.Parameters.AddWithValue("@PayRunDefinitionCode", payRunDefinition);
                            insertCommand.Parameters.AddWithValue("@DateWorked", dateWorked);
                            insertCommand.Parameters.AddWithValue("@Units", units);
                            insertCommand.Parameters.AddWithValue("@Amount", 2);
                            insertCommand.Parameters.AddWithValue("@CalculationAmount", 20);

                            decimal calculationAmount = Convert.ToDecimal(insertCommand.Parameters["@CalculationAmount"].Value);
                            insertCommand.Parameters.AddWithValue("@CalculationAmountRuleCCY", units * calculationAmount);
                            insertCommand.Parameters.AddWithValue("@EmployeeRate", memberRate);
                            insertCommand.Parameters.AddWithValue("@Note", note);
                            insertCommand.Parameters.AddWithValue("@DateCaptured", dateWorked);
                            insertCommand.Parameters.AddWithValue("@CapturedBy", "iFetch");
                            insertCommand.Parameters.AddWithValue("@UserID", "Admin");
                            insertCommand.Parameters.AddWithValue("@EndDate", endDate);
                            insertCommand.Parameters.AddWithValue("@LastChanged", endDate);

                            Console.WriteLine($"Inserting: FarmerCode={farmerCode}, PayRun={payRunDefinition}, Units={units}, DateWorked={dateWorked}");

                            insertCommand.ExecuteNonQuery();

                            

                            string Levelquery = @"
                                INSERT INTO [Tea_Auto].[Payroll].[PayslipEarnUnitLevel] 
                                ([PayslipEarnUnitID], [LevelDefinitionID], [LevelDefinitionSequence], [LevelSetupID], 
                                [HierarchyID], [JobGradeID], [JobTitleTypeID], [UserID], [LastChanged]) 
                                VALUES 
                                (@PayslipEarnUnitID, @LevelDefinitionID, @LevelDefinitionSequence, @LevelSetupID, 
                                @HierarchyID, @JobGradeID, @JobTitleTypeID, @UserID, @LastChanged);";

                            cmd = new SqlCommand(Levelquery, conn);
                            string hierarchyCodeString = hierarchyItem["HierarchyCode"].ToString()=="GL"? "1": "2";
                            //int JobGradeID = int.Parse(data["JobGradeCode"].ToString());
                            //int JobTitleTypeID = int.Parse(data["JobTitleCode"].ToString());

                            string PayslipEarnUnitIDQuery = @"select PayslipEarnUnitID from [Tea_Auto].[Payroll].[PayslipEarnUnit] where PayslipEarnLineID= @PayslipEarnUnitID";
                            cmd2 = new SqlCommand(PayslipEarnUnitIDQuery, conn);
                            cmd2.Parameters.AddWithValue("@PayslipEarnUnitID", payslipId);

                            object result2 = cmd2.ExecuteScalar();
                            string payslipEarnUnitID = result2 != null ? result2.ToString() : string.Empty; // Handle null case

                            Console.WriteLine($"PayslipEarnUnitID: {payslipEarnUnitID}");

                            string HierarchyIDQuery = @"select HierarchyID from [Tea_Auto].[Entity].[Hierarchy] where HierarchyCode= @HierarchyCode";
                            SqlCommand cmd3 = new SqlCommand(HierarchyIDQuery, conn);
                            cmd3.Parameters.AddWithValue("@HierarchyCode",hierarchyItem["HierarchyCode"]);

                            object result3 = cmd3.ExecuteScalar();
                            string hierarchyID = result3 != null ? result3.ToString() : string.Empty; // Handle null case

                            Console.WriteLine($"HierarchyID: {hierarchyID}");

                            cmd.Parameters.AddWithValue("@PayslipEarnUnitID", payslipEarnUnitID);
                            cmd.Parameters.AddWithValue("@LevelDefinitionID", LevelId);
                            cmd.Parameters.AddWithValue("@LevelDefinitionSequence", LevelId);
                            cmd.Parameters.AddWithValue("@LevelSetupID", hierarchyCodeString);
                            cmd.Parameters.AddWithValue("@HierarchyID", hierarchyID);
                            cmd.Parameters.AddWithValue("@JobGradeID", 1);
                            cmd.Parameters.AddWithValue("@JobTitleTypeID", 1);
                            cmd.Parameters.AddWithValue("@UserID", "Admin");
                            cmd.Parameters.AddWithValue("@LastChanged", endDate);

                            cmd.ExecuteNonQuery();
                        }
                    }

                    Console.Read();
                }
            }
            catch (Exception ex)
            {
                //MessageBox.Show(ex.ToString());
                Console.WriteLine(ex.ToString());
                Console.WriteLine(ex.Message);
                Console.Read();
            }
        }
    }
}




