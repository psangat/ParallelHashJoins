using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParallelHashJoins
{
    class AtireJoin
    {
        private static string binaryFilesDirectory = @"C:\Raw_Data_Source_For_Test\SSBM - DBGEN\BF";
        private string scaleFactor { get; set; }

        public TestResults testResults = new TestResults();
        public AtireJoin(string scaleFactor)
        {
            this.scaleFactor = scaleFactor;
            testResults.totalRAMAvailable = Utils.getAvailableRAM();
        }

        ~AtireJoin()
        {
            saveAndPrintResults();
        }

        #region Private Variables
        private List<int> cCustKey = new List<int>();
        private List<string> cName = new List<string>();
        private List<string> cAddress = new List<string>();
        private List<string> cCity = new List<string>();
        private List<string> cNation = new List<string>();
        private List<string> cRegion = new List<string>();
        private List<string> cPhone = new List<string>();
        private List<string> cMktSegment = new List<string>();

        private List<int> sSuppKey = new List<int>();
        private List<string> sName = new List<string>();
        private List<string> sAddress = new List<string>();
        private List<string> sCity = new List<string>();
        private List<string> sNation = new List<string>();
        private List<string> sRegion = new List<string>();
        private List<string> sPhone = new List<string>();

        private List<int> pSize = new List<int>();
        private List<int> pPartKey = new List<int>();
        private List<string> pName = new List<string>();
        private List<string> pMFGR = new List<string>();
        private List<string> pCategory = new List<string>();
        private List<string> pBrand = new List<string>();
        private List<string> pColor = new List<string>();
        private List<string> pType = new List<string>();
        private List<string> pContainer = new List<string>();

        private List<int> loOrderKey = new List<int>();
        private List<int> loLineNumber = new List<int>();
        private List<int> loCustKey = new List<int>();
        private List<int> loPartKey = new List<int>();
        private List<int> loSuppKey = new List<int>();
        private List<int> loOrderDate = new List<int>();
        private List<char> loShipPriority = new List<char>();
        private List<int> loQuantity = new List<int>();
        private List<Tuple<int, int>> loQuantityWithId = new List<Tuple<int, int>>();

        private List<int> loExtendedPrice = new List<int>();
        private List<int> loOrdTotalPrice = new List<int>();
        private List<int> loDiscount = new List<int>();
        private List<Tuple<int, int>> loDiscountWithId = new List<Tuple<int, int>>();
        private List<int> loRevenue = new List<int>();
        private List<int> loSupplyCost = new List<int>();
        private List<int> loTax = new List<int>();
        private List<int> loCommitDate = new List<int>();
        private List<string> loShipMode = new List<string>();
        private List<string> loOrderPriority = new List<string>();

        private List<int> dDateKey = new List<int>();
        private List<int> dYear = new List<int>();
        private List<int> dYearMonthNum = new List<int>();
        private List<int> dDayNumInWeek = new List<int>();
        private List<int> dDayNumInMonth = new List<int>();
        private List<int> dDayNumInYear = new List<int>();
        private List<int> dMonthNumInYear = new List<int>();
        private List<int> dWeekNumInYear = new List<int>();
        private List<int> dLastDayInWeekFL = new List<int>();
        private List<int> dLastDayInMonthFL = new List<int>();
        private List<int> dHolidayFL = new List<int>();
        private List<int> dWeekDayFL = new List<int>();
        private List<string> dDate = new List<string>();
        private List<string> dDayOfWeek = new List<string>();
        private List<string> dMonth = new List<string>();
        private List<string> dYearMonth = new List<string>();
        private List<string> dSellingSeason = new List<string>();

        private List<Customer> customer = new List<Customer>();
        private List<Supplier> supplier = new List<Supplier>();
        private List<Part> part = new List<Part>();
        private List<LineOrder> lineOrder = new List<LineOrder>();
        private List<Date> date = new List<Date>();


        private string dateFile = binaryFilesDirectory + @"\date\";
        private string customerFile = binaryFilesDirectory + @"\customer\";
        private string supplierFile = binaryFilesDirectory + @"\supplier\";
        private string partFile = binaryFilesDirectory + @"\part\";

        private string dDateKeyFile = binaryFilesDirectory + @"\dDateKey\";
        private string dYearFile = binaryFilesDirectory + @"\dYear\";
        private string dYearMonthNumFile = binaryFilesDirectory + @"\dYearMonthNum\";
        private string dDayNumInWeekFile = binaryFilesDirectory + @"\dDayNumInWeek\";
        private string dDayNumInMonthFile = binaryFilesDirectory + @"\dDayNumInMont\";
        private string dDayNumInYearFile = binaryFilesDirectory + @"\dDayNumInYear\";
        private string dMonthNumInYearFile = binaryFilesDirectory + @"\dMonthNumInYear\";
        private string dWeekNumInYearFile = binaryFilesDirectory + @"\dWeekNumInYear\";
        private string dLastDayInWeekFLFile = binaryFilesDirectory + @"\dLastDayInWeekFL\";
        private string dLastDayInMonthFLFile = binaryFilesDirectory + @"\dLastDayInMonthFL\";
        private string dHolidayFLFile = binaryFilesDirectory + @"\dHolidayFL\";
        private string dWeekDayFLFile = binaryFilesDirectory + @"\dWeekDayFL\";
        private string dDateFile = binaryFilesDirectory + @"\dDate\";
        private string dDayOfWeekFile = binaryFilesDirectory + @"\dDayOfWeek\";
        private string dMonthFile = binaryFilesDirectory + @"\dMonth\";
        private string dYearMonthFile = binaryFilesDirectory + @"\dYearMonth\";
        private string dSellingSeasonFile = binaryFilesDirectory + @"\dSellingSeason\";

        private string loOrderKeyFile = binaryFilesDirectory + @"\loOrderKey\";
        private string loLineNumberFile = binaryFilesDirectory + @"\loLineNumber\";
        private string loCustKeyFile = binaryFilesDirectory + @"\loCustKey\";
        private string loPartKeyFile = binaryFilesDirectory + @"\loPartKey\";
        private string loSuppKeyFile = binaryFilesDirectory + @"\loSuppKey\";
        private string loOrderDateFile = binaryFilesDirectory + @"\loOrderDate\";
        private string loShipPriorityFile = binaryFilesDirectory + @"\loShipPriority\";
        private string loQuantityFile = binaryFilesDirectory + @"\loQuantity\";
        private string loExtendedPriceFile = binaryFilesDirectory + @"\loExtendedPrice\";
        private string loOrdTotalPriceFile = binaryFilesDirectory + @"\loOrdTotalPrice\";
        private string loDiscountFile = binaryFilesDirectory + @"\loDiscount\";
        private string loRevenueFile = binaryFilesDirectory + @"\loRevenue\";
        private string loSupplyCostFile = binaryFilesDirectory + @"\loSupplyCost\";
        private string loTaxFile = binaryFilesDirectory + @"\loTax\";
        private string loCommitDateFile = binaryFilesDirectory + @"\loCommitDate\";
        private string loShipModeFile = binaryFilesDirectory + @"\loShipMode\";
        private string loOrderPriorityFile = binaryFilesDirectory + @"\loOrderPriority\";

        private string cCustKeyFile = binaryFilesDirectory + @"\cCustKey\";
        private string cNameFile = binaryFilesDirectory + @"\cName\";
        private string cAddressFile = binaryFilesDirectory + @"\cAddress\";
        private string cCityFile = binaryFilesDirectory + @"\cCity\";
        private string cNationFile = binaryFilesDirectory + @"\cNation\";
        private string cRegionFile = binaryFilesDirectory + @"\cRegion\";
        private string cPhoneFile = binaryFilesDirectory + @"\cPhone\";
        private string cMktSegmentFile = binaryFilesDirectory + @"\cMktSegment\";

        private string sSuppKeyFile = binaryFilesDirectory + @"\sSuppKey\";
        private string sNameFile = binaryFilesDirectory + @"\sName\";
        private string sAddressFile = binaryFilesDirectory + @"\sAddress\";
        private string sCityFile = binaryFilesDirectory + @"\sCity\";
        private string sNationFile = binaryFilesDirectory + @"\sNation\";
        private string sRegionFile = binaryFilesDirectory + @"\sRegion\";
        private string sPhoneFile = binaryFilesDirectory + @"\sPhone\";

        private string pSizeFile = binaryFilesDirectory + @"\pSize\";
        private string pPartKeyFile = binaryFilesDirectory + @"\pPartKey\";
        private string pNameFile = binaryFilesDirectory + @"\pName\";
        private string pMFGRFile = binaryFilesDirectory + @"\pMFGR\";
        private string pCategoryFile = binaryFilesDirectory + @"\pCategory\";
        private string pBrandFile = binaryFilesDirectory + @"\pBrand\";
        private string pColorFile = binaryFilesDirectory + @"\pColor\";
        private string pTypeFile = binaryFilesDirectory + @"\pType\";
        private string pContainerFile = binaryFilesDirectory + @"\pContainer\";

        #endregion Private Variables

        /// <summary>
        /// Failed Experiment
        /// Intersect based approach
        /// </summary>
        public void Query_3_1_Intersect()
        {
            try
            {
                Stopwatch sw = new Stopwatch();

                List<Customer> customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
                List<Supplier> supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
                List<Date> dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));

                sw.Start();
                #region Phase 1

                var dateDictionary = new Dictionary<int, string>(dateDimension.Count);
                var customerBitMap = new BitArray(customerDimension.Count);
                var supplierBitMap = new BitArray(supplierDimension.Count);


                foreach (var row in dateDimension)
                {
                    if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
                        dateDictionary.Add(row.dDateKey, row.dYear);
                }

                int i = 0;
                foreach (var row in customerDimension)
                {
                    if (row.cRegion.Equals("ASIA"))
                        customerBitMap.Set(i, true);
                    i++;
                }

                i = 0;
                foreach (var row in supplierDimension)
                {
                    if (row.sRegion.Equals("ASIA"))
                        supplierBitMap.Set(i, true);
                    i++;
                }

                sw.Stop();
                long elapsedTimePhase1 = sw.ElapsedMilliseconds;
                Console.WriteLine("[DGJoin] Phase1 Time: " + elapsedTimePhase1);
                #endregion Phase1

                List<int> loCustomerKey = Utils.ReadFromBinaryFiles<int>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                List<int> loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                List<int> loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));

                #region Phase2
                sw.Reset();
                sw.Start();
                var dateGroupDictionary = new Dictionary<string, HashSet<int>>();
                var customerGroupDictionary = new Dictionary<string, HashSet<int>>();
                var supplierGroupDictionary = new Dictionary<string, HashSet<int>>();

                int factRowID = 0;
                foreach (var suppKey in loSupplierKey)
                {
                    int dimRowID = suppKey - 1;
                    if (supplierBitMap[dimRowID])
                    {
                        HashSet<int> groupIDs = null;
                        string sNation = supplierDimension[dimRowID].sNation;

                        if (supplierGroupDictionary.TryGetValue(sNation, out groupIDs))
                        {
                            groupIDs.Add(factRowID);
                            supplierGroupDictionary[sNation] = groupIDs;
                        }
                        else
                        {
                            groupIDs = new HashSet<int>();
                            groupIDs.Add(factRowID);
                            supplierGroupDictionary[sNation] = groupIDs;
                        }
                    }
                    factRowID++;

                }

                factRowID = 0;
                foreach (var orderDate in loOrderDate)
                {
                    string dYear = string.Empty;
                    if (dateDictionary.TryGetValue(orderDate, out dYear))
                    {
                        HashSet<int> groupIDs = null;
                        if (dateGroupDictionary.TryGetValue(dYear, out groupIDs))
                        {
                            groupIDs.Add(factRowID);
                            dateGroupDictionary[dYear] = groupIDs;
                        }
                        else
                        {
                            groupIDs = new HashSet<int>();
                            groupIDs.Add(factRowID);
                            dateGroupDictionary[dYear] = groupIDs;
                        }
                    }
                    factRowID++;
                }

                factRowID = 0;
                foreach (var custKey in loCustomerKey)
                {
                    int dimRowID = custKey - 1;
                    if (customerBitMap[dimRowID])
                    {
                        HashSet<int> groupIDs = null;
                        string cNation = customerDimension[dimRowID].cNation;

                        if (customerGroupDictionary.TryGetValue(cNation, out groupIDs))
                        {
                            groupIDs.Add(factRowID);
                            customerGroupDictionary[cNation] = groupIDs;
                        }
                        else
                        {
                            groupIDs = new HashSet<int>();
                            groupIDs.Add(factRowID);
                            customerGroupDictionary[cNation] = groupIDs;
                        }
                    }
                    factRowID++;
                }

                sw.Stop();
                long elapsedTimePhase2 = sw.ElapsedMilliseconds;
                Console.WriteLine("[DGJoin] Phase2 Time: " + elapsedTimePhase2);
                #endregion Phase2

                List<int> loRevenue = Utils.ReadFromBinaryFiles<int>(loRevenueFile.Replace("BF", "BF" + scaleFactor));

                #region Phase3
                sw.Reset();
                sw.Start();


                List<string> finalTable = new List<string>();

                foreach (var cGItem in customerGroupDictionary)
                {
                    foreach (var sGItem in supplierGroupDictionary)
                    {
                        var cGSize = cGItem.Value.Count;
                        var sGSize = sGItem.Value.Count;

                        IEnumerable<int> colIntersect;
                        if (cGSize <= sGSize)
                        {
                            colIntersect = cGItem.Value.Intersect(sGItem.Value);
                        }
                        else
                        {
                            colIntersect = sGItem.Value.Intersect(cGItem.Value);
                        }

                        var intersectionSize = colIntersect.Count();
                        if (intersectionSize > 0)
                        {
                            foreach (var dGItem in dateGroupDictionary)
                            {
                                var dGSize = dGItem.Value.Count;
                                if (intersectionSize <= dGSize)
                                {
                                    colIntersect = colIntersect.Intersect(dGItem.Value);
                                }
                                else
                                {
                                    colIntersect = dGItem.Value.Intersect(colIntersect);
                                }
                                var collcount = colIntersect.Count();
                                if (colIntersect.Count() > 0)
                                {
                                    int sum = 0;
                                    foreach (var item in colIntersect)
                                    {
                                        sum += loRevenue[item - 1];
                                    }
                                    finalTable.Add(cGItem.Key + ", " + sGItem.Key + ", " + dGItem.Key + ", " + sum);
                                }
                            }
                        }
                    }
                }

                sw.Stop();
                long elapsedTimePhase3 = sw.ElapsedMilliseconds;
                Console.WriteLine("[DGJoin] Phase3 Time: " + elapsedTimePhase3);
                #endregion Phase3
                //testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
                //testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
                //testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
                //// Console.WriteLine("[Nimble Join]: Time taken {0} ms.", testResults.totalExecutionTime);
                //testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
                //testResults.totalNumberOfOutput = joinOutputFinal.Count();
                Console.WriteLine("[DGJoin] Total: " + finalTable.Count);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Hash all the grouping attributes as key. Way better than Intersect Based
        /// Still slower than IMA
        /// </summary>
        public void Query_3_1_Hashed()
        {
            try
            {
                Stopwatch sw = new Stopwatch();

                List<Customer> customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
                List<Supplier> supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
                List<Date> dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                List<int> loCustomerKey = Utils.ReadFromBinaryFiles<int>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                List<int> loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                List<int> loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                List<int> loRevenue = Utils.ReadFromBinaryFiles<int>(loRevenueFile.Replace("BF", "BF" + scaleFactor));


                GroupingAttributes[] tempTable = new GroupingAttributes[loCustomerKey.Count];
                for (int j = 0; j < loCustomerKey.Count; j++)
                {
                    tempTable[j] = new GroupingAttributes();
                }

                var groupTracker = new SortedDictionary<string, HashSet<int>>();


                sw.Start();
                #region Phase 1

                // var dateDictionary = new Dictionary<int, string>(dateDimension.Count);
                var customerBitMap = new BitArray(customerDimension.Count);
                var supplierBitMap = new BitArray(supplierDimension.Count);


                //foreach (var row in dateDimension)
                //{
                //    if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
                //        dateDictionary.Add(row.dDateKey, row.dYear);
                //}

                int i = 0;
                foreach (var row in customerDimension)
                {
                    if (row.cRegion.Equals("ASIA"))
                        customerBitMap.Set(i, true);
                    i++;
                }

                i = 0;
                foreach (var row in supplierDimension)
                {
                    if (row.sRegion.Equals("ASIA"))
                        supplierBitMap.Set(i, true);
                    i++;
                }

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("T0 Time: {0}", t0));
                #endregion Phase1

                sw.Start();
                int factRowID = 0;
                foreach (var suppKey in loSupplierKey)
                {
                    int dimRowID = suppKey - 1;
                    if (supplierBitMap[dimRowID]) // Accesss so many times so use the first idea
                    {
                        string sNation = supplierDimension[dimRowID].sNation;
                        GroupingAttributes grpAttr = tempTable[factRowID];
                        grpAttr.Y = sNation;
                        // No need to check in the serial algorithm
                        //if (grpAttr.isFilled())
                        //{
                        //    HashSet<int> positions = null;
                        //    if (groupTracker.TryGetValue(grpAttr.getKey(), out positions))
                        //    {
                        //        positions.Add(factRowID);
                        //        groupTracker[grpAttr.getKey()] = positions;
                        //    }
                        //    else
                        //    {
                        //        positions = new HashSet<int>();
                        //        positions.Add(factRowID);
                        //        groupTracker[grpAttr.getKey()] = positions;
                        //    }
                        //}
                        //
                    }
                    factRowID++;
                }

                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("T1 Time: {0}", t1));

                sw.Reset();
                sw.Start();
                factRowID = 0;
                foreach (var orderDate in loOrderDate)
                {
                    int dYear = orderDate / 10000; // We dont even need to load the dimension attributes
                    if (dYear >= 1992 && dYear <= 1997)
                    {
                        GroupingAttributes grpAttr = tempTable[factRowID];
                        grpAttr.Z = dYear;
                        // No need to check in the serial algorithm. Check only at the end.
                        //if (grpAttr.isFilled())
                        //{
                        //    HashSet<int> positions = null;
                        //    if (groupTracker.TryGetValue(grpAttr.getKey(), out positions))
                        //    {
                        //        positions.Add(factRowID);
                        //        groupTracker[grpAttr.getKey()] = positions;
                        //    }
                        //    else
                        //    {
                        //        positions = new HashSet<int>();
                        //        positions.Add(factRowID);
                        //        groupTracker[grpAttr.getKey()] = positions;
                        //    }
                        //}
                        //
                    }
                    factRowID++;
                }

                sw.Stop();
                long t2 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("T2 Time: {0}", t2));

                sw.Reset();
                sw.Start();
                factRowID = 0;
                int count = 0;
                foreach (var custKey in loCustomerKey)
                {
                    int dimRowID = custKey - 1;
                    if (customerBitMap[dimRowID])
                    {
                        string cNation = customerDimension[dimRowID].cNation;
                        GroupingAttributes grpAttr = tempTable[factRowID];
                        grpAttr.X = cNation;
                        // Check only at the end in serial algorithm

                        if (grpAttr.isFilled())
                        {
                            HashSet<int> positions = null;
                            if (groupTracker.TryGetValue(grpAttr.getKey(), out positions))
                            {
                                count++;
                                positions.Add(factRowID);
                                groupTracker[grpAttr.getKey()] = positions;
                            }
                            else
                            {
                                positions = new HashSet<int>();
                                positions.Add(factRowID);
                                groupTracker.Add(grpAttr.getKey(), positions);
                            }
                        }

                    }
                    factRowID++;
                }
                sw.Stop();
                long t3 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("Count: {0}", count));
                Console.WriteLine(String.Format("T3 Time: {0}", t3));

                sw.Reset();
                sw.Start();
                List<string> finalResult = new List<string>();
                foreach (var group in groupTracker)
                {
                    int sum = 0;
                    foreach (var position in group.Value)
                    {
                        sum += loRevenue[position];
                    }

                    finalResult.Add(group.Key + ", " + sum);
                }
                sw.Stop();
                long t4 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("T4 Time: {0}", t4));
                Console.WriteLine(String.Format("Total Time: {0}", t1 + t2 + t3 + t4));
                Console.WriteLine();
                //testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
                //testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
                //testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
                //// Console.WriteLine("[Nimble Join]: Time taken {0} ms.", testResults.totalExecutionTime);
                //testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
                //testResults.totalNumberOfOutput = joinOutputFinal.Count();
                // Console.WriteLine("[DGJoin] Total: " + finalTable.Count);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Tire based approach. Better than all.
        /// </summary>
        public void Query_3_1()
        {
            try
            {
                Stopwatch sw = new Stopwatch();

                List<Customer> customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
                List<Supplier> supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
                List<Date> dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                List<int> loCustomerKey = Utils.ReadFromBinaryFiles<int>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                List<int> loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                List<int> loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                List<int> loRevenue = Utils.ReadFromBinaryFiles<int>(loRevenueFile.Replace("BF", "BF" + scaleFactor));


                sw.Start();
                #region Phase 1

                var customerHashTable = new Dictionary<int, string>();
                var supplierHashTable = new Dictionary<int, string>();
                var dateHashTable = new Dictionary<int, string>();

                foreach (var row in dateDimension)
                {
                    if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
                        dateHashTable.Add(row.dDateKey, row.dYear);
                }

                foreach (var row in customerDimension)
                {
                    if (row.cRegion.Equals("ASIA"))
                        customerHashTable.Add(row.cCustKey, row.cNation);
                }

                foreach (var row in supplierDimension)
                {
                    if (row.sRegion.Equals("ASIA"))
                        supplierHashTable.Add(row.sSuppKey, row.sNation);
                }

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[Atire Join] T0 Time: {0}", t0));
                #endregion Phase1

                sw.Start();
                Atire tire = new Atire();
                for (int i = 0; i < loCustomerKey.Count(); i++)
                {
                    int custKey = loCustomerKey[i];
                    int suppKey = loSupplierKey[i];
                    int dateKey = loOrderDate[i];
                    string custNation = string.Empty;
                    string suppNation = string.Empty;
                    string dYear = string.Empty;
                    if (customerHashTable.TryGetValue(custKey, out custNation) && supplierHashTable.TryGetValue(suppKey, out suppNation) && dateHashTable.TryGetValue(dateKey, out dYear))
                    {
                        tire.Insert(tire, new List<string> { custNation, suppNation, dYear }, false, loRevenue[i]);
                    }
                }
                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                // Console.WriteLine(String.Format("Count: {0}", count));
                Console.WriteLine(String.Format("[Atire Join] T1 Time: {0}", t1));

                Console.WriteLine(String.Format("[Atire Join] Total Time: {0}", t0 + t1));
                tire.GetResults(tire);
                var results = tire.results;
                //System.IO.File.WriteAllLines(@"C:\Results\AtireJoin.txt", results);
                //Console.WriteLine(String.Format("[Atire Join] Total Items: {0}", finalTable.Count));
                Console.WriteLine();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void AggregationScalabilityTest1(int numberOfAggregations)
        {
            try
            {
                Stopwatch sw = new Stopwatch();

                List<int> loTax = null;
                List<int> loDiscount = null;
                List<int> loQuantity = null;
                List<int> loSupplyCost = null;
                List<int> loRevenue = null;
                List<int> loOrderTotalPrice = null;
                List<int> loCommitDate = Utils.ReadFromBinaryFiles<int>(loCommitDateFile.Replace("BF", "BF" + scaleFactor));
                switch (numberOfAggregations)
                {
                    case 1:
                        loTax = Utils.ReadFromBinaryFiles<int>(loTaxFile.Replace("BF", "BF" + scaleFactor));
                        break;
                    case 2:
                        loTax = Utils.ReadFromBinaryFiles<int>(loTaxFile.Replace("BF", "BF" + scaleFactor));
                        loDiscount = Utils.ReadFromBinaryFiles<int>(loDiscountFile.Replace("BF", "BF" + scaleFactor));
                        break;
                    case 3:
                        loTax = Utils.ReadFromBinaryFiles<int>(loTaxFile.Replace("BF", "BF" + scaleFactor));
                        loDiscount = Utils.ReadFromBinaryFiles<int>(loDiscountFile.Replace("BF", "BF" + scaleFactor));
                        loQuantity = Utils.ReadFromBinaryFiles<int>(loQuantityFile.Replace("BF", "BF" + scaleFactor));
                        break;
                    case 4:
                        loTax = Utils.ReadFromBinaryFiles<int>(loTaxFile.Replace("BF", "BF" + scaleFactor));
                        loDiscount = Utils.ReadFromBinaryFiles<int>(loDiscountFile.Replace("BF", "BF" + scaleFactor));
                        loQuantity = Utils.ReadFromBinaryFiles<int>(loQuantityFile.Replace("BF", "BF" + scaleFactor));
                        loSupplyCost = Utils.ReadFromBinaryFiles<int>(loSupplyCostFile.Replace("BF", "BF" + scaleFactor));
                        break;
                    case 5:
                        loTax = Utils.ReadFromBinaryFiles<int>(loTaxFile.Replace("BF", "BF" + scaleFactor));
                        loDiscount = Utils.ReadFromBinaryFiles<int>(loDiscountFile.Replace("BF", "BF" + scaleFactor));
                        loQuantity = Utils.ReadFromBinaryFiles<int>(loQuantityFile.Replace("BF", "BF" + scaleFactor));
                        loSupplyCost = Utils.ReadFromBinaryFiles<int>(loSupplyCostFile.Replace("BF", "BF" + scaleFactor));
                        loRevenue = Utils.ReadFromBinaryFiles<int>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
                        break;
                    case 6:
                        loTax = Utils.ReadFromBinaryFiles<int>(loTaxFile.Replace("BF", "BF" + scaleFactor));
                        loDiscount = Utils.ReadFromBinaryFiles<int>(loDiscountFile.Replace("BF", "BF" + scaleFactor));
                        loQuantity = Utils.ReadFromBinaryFiles<int>(loQuantityFile.Replace("BF", "BF" + scaleFactor));
                        loSupplyCost = Utils.ReadFromBinaryFiles<int>(loSupplyCostFile.Replace("BF", "BF" + scaleFactor));
                        loRevenue = Utils.ReadFromBinaryFiles<int>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
                        loOrderTotalPrice = Utils.ReadFromBinaryFiles<int>(loOrdTotalPriceFile.Replace("BF", "BF" + scaleFactor));
                        break;
                }

                #region Value Extraction Phase
                sw.Start();
                Atire atire = new Atire();
                for (int i = 0; i < loCommitDate.Count; i++)
                {
                    Int64[] values = new Int64[numberOfAggregations];
                    switch (numberOfAggregations)
                    {
                        case 1:
                            values[0] = loTax[i];
                            break;
                        case 2:
                            values[0] = loTax[i];
                            values[1] = loDiscount[i];
                            break;
                        case 3:
                            values[0] = loTax[i];
                            values[1] = loDiscount[i];
                            values[2] = loQuantity[i];
                            break;
                        case 4:
                            values[0] = loTax[i];
                            values[1] = loDiscount[i];
                            values[2] = loQuantity[i];
                            values[3] = loSupplyCost[i];
                            break;
                        case 5:
                            values[0] = loTax[i];
                            values[1] = loDiscount[i];
                            values[2] = loQuantity[i];
                            values[3] = loSupplyCost[i];
                            values[4] = loRevenue[i];
                            break;
                        case 6:
                            values[0] = loTax[i];
                            values[1] = loDiscount[i];
                            values[2] = loQuantity[i];
                            values[3] = loSupplyCost[i];
                            values[4] = loRevenue[i];
                            values[5] = loOrderTotalPrice[i];
                            break;
                    }
                    string commitDate = Convert.ToString(loCommitDate[i]);
                    atire.Insert(atire, new List<string> { commitDate }, true, values);
                }

                sw.Stop();
                long t2 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[ATire Join] Total Time: {0}", t2));
                // Console.WriteLine(String.Format("[Invisible Join] Total Time: {0}", t0 + t1 + t2));
                //Console.WriteLine(String.Format("[ATire Join] Total : {0}", joinOutputFinal.Count));
                Console.WriteLine();
                #endregion Value Extraction Phase
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void AggregationScalabilityTest2(int numberOfAggregations)
        {
            try
            {
                Stopwatch sw = new Stopwatch();

                List<int> loTax = null;
                List<int> loDiscount = null;
                List<int> loQuantity = null;
                List<int> loSupplyCost = null;
                List<int> loRevenue = null;
                List<int> loOrderTotalPrice = null;
                List<int> loCommitDate = Utils.ReadFromBinaryFiles<int>(loCommitDateFile.Replace("BF", "BF" + scaleFactor));
                List<int> loCustomerKey = Utils.ReadFromBinaryFiles<int>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                List<int> loPartKey = Utils.ReadFromBinaryFiles<int>(loPartKeyFile.Replace("BF", "BF" + scaleFactor));
                List<Customer> customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
                List<Part> partDimension = Utils.ReadFromBinaryFiles<Part>(partFile.Replace("BF", "BF" + scaleFactor));

                switch (numberOfAggregations)
                {
                    case 1:
                        loTax = Utils.ReadFromBinaryFiles<int>(loTaxFile.Replace("BF", "BF" + scaleFactor));
                        break;
                    case 2:
                        loTax = Utils.ReadFromBinaryFiles<int>(loTaxFile.Replace("BF", "BF" + scaleFactor));
                        loDiscount = Utils.ReadFromBinaryFiles<int>(loDiscountFile.Replace("BF", "BF" + scaleFactor));
                        break;
                    case 3:
                        loTax = Utils.ReadFromBinaryFiles<int>(loTaxFile.Replace("BF", "BF" + scaleFactor));
                        loDiscount = Utils.ReadFromBinaryFiles<int>(loDiscountFile.Replace("BF", "BF" + scaleFactor));
                        loQuantity = Utils.ReadFromBinaryFiles<int>(loQuantityFile.Replace("BF", "BF" + scaleFactor));
                        break;
                    case 4:
                        loTax = Utils.ReadFromBinaryFiles<int>(loTaxFile.Replace("BF", "BF" + scaleFactor));
                        loDiscount = Utils.ReadFromBinaryFiles<int>(loDiscountFile.Replace("BF", "BF" + scaleFactor));
                        loQuantity = Utils.ReadFromBinaryFiles<int>(loQuantityFile.Replace("BF", "BF" + scaleFactor));
                        loSupplyCost = Utils.ReadFromBinaryFiles<int>(loSupplyCostFile.Replace("BF", "BF" + scaleFactor));
                        break;
                    case 5:
                        loTax = Utils.ReadFromBinaryFiles<int>(loTaxFile.Replace("BF", "BF" + scaleFactor));
                        loDiscount = Utils.ReadFromBinaryFiles<int>(loDiscountFile.Replace("BF", "BF" + scaleFactor));
                        loQuantity = Utils.ReadFromBinaryFiles<int>(loQuantityFile.Replace("BF", "BF" + scaleFactor));
                        loSupplyCost = Utils.ReadFromBinaryFiles<int>(loSupplyCostFile.Replace("BF", "BF" + scaleFactor));
                        loRevenue = Utils.ReadFromBinaryFiles<int>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
                        break;
                    case 6:
                        loTax = Utils.ReadFromBinaryFiles<int>(loTaxFile.Replace("BF", "BF" + scaleFactor));
                        loDiscount = Utils.ReadFromBinaryFiles<int>(loDiscountFile.Replace("BF", "BF" + scaleFactor));
                        loQuantity = Utils.ReadFromBinaryFiles<int>(loQuantityFile.Replace("BF", "BF" + scaleFactor));
                        loSupplyCost = Utils.ReadFromBinaryFiles<int>(loSupplyCostFile.Replace("BF", "BF" + scaleFactor));
                        loRevenue = Utils.ReadFromBinaryFiles<int>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
                        loOrderTotalPrice = Utils.ReadFromBinaryFiles<int>(loOrdTotalPriceFile.Replace("BF", "BF" + scaleFactor));
                        break;
                }

                sw.Start();
                #region Key Hashing Phase 

                var customerHashTable = new Dictionary<int, string>();
                var partHashTable = new Dictionary<int, string>();


                foreach (var row in customerDimension)
                {
                    // if (row.cRegion.Equals("ASIA"))
                    customerHashTable.Add(row.cCustKey, row.cRegion);
                }

                foreach (var row in partDimension)
                {
                    //if (row.sRegion.Equals("ASIA"))
                    partHashTable.Add(row.pPartKey, row.pMFGR);
                }

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[ATire Join] AGTest2 T0 Time: {0}", t0));
                #endregion Key Hashing Phase
                Atire tire = new Atire();

                #region Probing Phase
                sw.Reset();
                sw.Start();
                for (int i = 0; i < loCustomerKey.Count; i++)
                {
                    int custKey = loCustomerKey[i];
                    int partKey = loPartKey[i];
                    string cRegion = string.Empty;
                    string pMFGR = string.Empty;
                    if (customerHashTable.TryGetValue(custKey, out cRegion)
                        && partHashTable.TryGetValue(partKey, out pMFGR))
                    {
                        long[] values = new long[numberOfAggregations];
                        switch (numberOfAggregations)
                        {
                            case 1:
                                values[0] = loTax[i];
                                break;
                            case 2:
                                values[0] = loTax[i];
                                values[1] = loDiscount[i];
                                break;
                            case 3:
                                values[0] = loTax[i];
                                values[1] = loDiscount[i];
                                values[2] = loQuantity[i];
                                break;
                            case 4:
                                values[0] = loTax[i];
                                values[1] = loDiscount[i];
                                values[2] = loQuantity[i];
                                values[3] = loSupplyCost[i];
                                break;
                            case 5:
                                values[0] = loTax[i];
                                values[1] = loDiscount[i];
                                values[2] = loQuantity[i];
                                values[3] = loSupplyCost[i];
                                values[4] = loRevenue[i];
                                break;
                            case 6:
                                values[0] = loTax[i];
                                values[1] = loDiscount[i];
                                values[2] = loQuantity[i];
                                values[3] = loSupplyCost[i];
                                values[4] = loRevenue[i];
                                values[5] = loOrderTotalPrice[i];
                                break;
                        }
                        tire.Insert(tire, new List<string> { cRegion, pMFGR }, true, values);
                    }
                }

                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[ATire Join] AGTest2 T1 Time: {0}", t1));
                sw.Reset();

                #endregion Probing Phase

                Console.WriteLine(String.Format("[ATire Join] AGTest2 Total Time: {0}", t0 + t1));
                //Console.WriteLine(String.Format("[Nimble Join] AGTest2 Total : {0}", joinOutputFinal.Count));
                Console.WriteLine();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void JoinScalabilityTest(int numberOfJoins)
        {
            try
            {
                Stopwatch sw = new Stopwatch();
                List<Customer> customerDimension = null;
                List<Supplier> supplierDimension = null;
                List<Date> dateDimension = null;
                List<Part> partDimension = null;
                List<int> loCustomerKey = null;
                List<int> loSupplierKey = null;
                List<int> loOrderDate = null;
                List<int> loPartKey = null;
                switch (numberOfJoins)
                {
                    case 1:
                        customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));

                        loCustomerKey = Utils.ReadFromBinaryFiles<int>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                        break;
                    case 2:
                        customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
                        supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));

                        loCustomerKey = Utils.ReadFromBinaryFiles<int>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                        loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                        break;
                    case 3:
                        customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
                        supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
                        dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));

                        loCustomerKey = Utils.ReadFromBinaryFiles<int>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                        loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                        loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                        break;
                    case 4:
                        customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
                        supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
                        dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                        partDimension = Utils.ReadFromBinaryFiles<Part>(partFile.Replace("BF", "BF" + scaleFactor));

                        loCustomerKey = Utils.ReadFromBinaryFiles<int>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                        loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                        loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                        loPartKey = Utils.ReadFromBinaryFiles<int>(loPartKeyFile.Replace("BF", "BF" + scaleFactor));

                        break;

                }

                List<int> loTax = Utils.ReadFromBinaryFiles<int>(loTaxFile.Replace("BF", "BF" + scaleFactor));

                sw.Start();
                #region Key Hashing Phase 

                var customerHashTable = new Dictionary<int, string>();
                var supplierHashTable = new Dictionary<int, string>();
                var dateHashTable = new Dictionary<int, string>();
                var partHashTable = new Dictionary<int, string>();
                switch (numberOfJoins)
                {

                    case 1:
                        foreach (var row in customerDimension)
                        {
                            //if (row.cRegion.Equals("ASIA"))
                            customerHashTable.Add(row.cCustKey, row.cRegion);
                        }
                        break;
                    case 2:
                        foreach (var row in customerDimension)
                        {
                            //if (row.cRegion.Equals("ASIA"))
                            customerHashTable.Add(row.cCustKey, row.cRegion);
                        }

                        foreach (var row in supplierDimension)
                        {
                            //if (row.sRegion.Equals("ASIA"))
                            supplierHashTable.Add(row.sSuppKey, row.sRegion);
                        }
                        break;
                    case 3:
                        foreach (var row in dateDimension)
                        {
                            //if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
                            dateHashTable.Add(row.dDateKey, row.dYear);
                        }

                        foreach (var row in customerDimension)
                        {
                            //if (row.cRegion.Equals("ASIA"))
                            customerHashTable.Add(row.cCustKey, row.cRegion);
                        }

                        foreach (var row in supplierDimension)
                        {
                            //if (row.sRegion.Equals("ASIA"))
                            supplierHashTable.Add(row.sSuppKey, row.sRegion);
                        }
                        break;
                    case 4:
                        foreach (var row in dateDimension)
                        {
                            //if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
                            dateHashTable.Add(row.dDateKey, row.dYear);
                        }

                        foreach (var row in customerDimension)
                        {
                            //if (row.cRegion.Equals("ASIA"))
                            customerHashTable.Add(row.cCustKey, row.cRegion);
                        }

                        foreach (var row in supplierDimension)
                        {
                            //if (row.sRegion.Equals("ASIA"))
                            supplierHashTable.Add(row.sSuppKey, row.sRegion);
                        }

                        foreach (var row in partDimension)
                        {
                            partHashTable.Add(row.pPartKey, row.pMFGR);
                        }
                        break;
                }

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[ATire Join] JSTest T0 Time: {0}", t0));
                #endregion Key Hashing Phase

                Atire tire = new Atire();
                #region Probing Phase
                sw.Reset();
                sw.Start();

                switch (numberOfJoins)
                {
                    case 1:
                        for (int i = 0; i < loCustomerKey.Count; i++)
                        {
                            int custKey = loCustomerKey[i];
                            string cRegionOut = string.Empty;
                            if (customerHashTable.TryGetValue(custKey, out cRegionOut))
                            {
                                tire.Insert(tire, new List<string> { cRegionOut }, true, loTax[i]);
                            }
                        }
                        break;
                    case 2:
                        for (int i = 0; i < loCustomerKey.Count; i++)
                        {
                            int custKey = loCustomerKey[i];
                            int suppKey = loSupplierKey[i];
                            string cRegionOut = string.Empty;
                            string sRegionOut = string.Empty;
                            if (customerHashTable.TryGetValue(custKey, out cRegionOut)
                                && supplierHashTable.TryGetValue(suppKey, out sRegionOut))
                            {
                                tire.Insert(tire, new List<string> { cRegionOut, sRegionOut }, true, loTax[i]);
                            }
                        }
                        break;
                    case 3:
                        for (int i = 0; i < loCustomerKey.Count; i++)
                        {
                            int custKey = loCustomerKey[i];
                            int suppKey = loSupplierKey[i];
                            int dateKey = loOrderDate[i];
                            string cRegionOut = string.Empty;
                            string sRegionOut = string.Empty;
                            string yearOut = string.Empty;
                            if (customerHashTable.TryGetValue(custKey, out cRegionOut)
                                && supplierHashTable.TryGetValue(suppKey, out sRegionOut)
                                && dateHashTable.TryGetValue(dateKey, out yearOut))
                            {
                                tire.Insert(tire, new List<string> { cRegionOut, sRegionOut, yearOut }, true, loTax[i]);
                            }
                        }
                        break;
                    case 4:
                        for (int i = 0; i < loCustomerKey.Count; i++)
                        {
                            int custKey = loCustomerKey[i];
                            int suppKey = loSupplierKey[i];
                            int dateKey = loOrderDate[i];
                            int partKey = loPartKey[i];
                            string cRegionOut = string.Empty;
                            string sRegionOut = string.Empty;
                            string yearOut = string.Empty;
                            string pMFGROut = string.Empty;
                            if (customerHashTable.TryGetValue(custKey, out cRegionOut)
                                && supplierHashTable.TryGetValue(suppKey, out sRegionOut)
                                && dateHashTable.TryGetValue(dateKey, out yearOut)
                                && partHashTable.TryGetValue(partKey, out pMFGROut))
                            {
                                tire.Insert(tire, new List<string> { cRegionOut, sRegionOut, yearOut, pMFGROut }, true, loTax[i]);
                            }
                        }
                        break;
                }

                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[ATire Join] JSTest T1 Time: {0}", t1));
                sw.Reset();
                #endregion Probing Phase


                Console.WriteLine(String.Format("[ATire Join] JSTest Total Time: {0}", t0 + t1));
                //Console.WriteLine(String.Format("[ATire Join] JSTest Total : {0}", joinOutputFinal.Count));
                Console.WriteLine();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void GroupingAttributeScalabilityTest(int numberOfGroupingAttributes)
        {
            try
            {
                Stopwatch sw = new Stopwatch();

                List<Customer> customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
                List<Supplier> supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
                List<Date> dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                List<int> loCustomerKey = Utils.ReadFromBinaryFiles<int>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                List<int> loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                List<int> loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                List<int> loTax = Utils.ReadFromBinaryFiles<int>(loTaxFile.Replace("BF", "BF" + scaleFactor));


                sw.Start();
                #region Phase 1

                var customerHashTable = new Dictionary<int, Tuple<string, string>>();
                var supplierHashTable = new Dictionary<int, Tuple<string, string>>();
                var dateHashTable = new Dictionary<int, Tuple<string, string>>();

                foreach (var row in dateDimension)
                {
                    if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
                        dateHashTable.Add(row.dDateKey, Tuple.Create(row.dYear, row.dMonth));
                }

                foreach (var row in customerDimension)
                {
                    if (row.cRegion.Equals("ASIA"))
                        customerHashTable.Add(row.cCustKey, Tuple.Create(row.cNation, row.cRegion));
                }

                foreach (var row in supplierDimension)
                {
                    if (row.sRegion.Equals("ASIA"))
                        supplierHashTable.Add(row.sSuppKey, Tuple.Create(row.sNation, row.sRegion));
                }

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[Atire Join] GSTest T0 Time: {0}", t0));
                #endregion Phase1

                sw.Start();
                Atire tire = new Atire();
                List<string> groupingAttributes = new List<string>();
                for (int i = 0; i < loCustomerKey.Count; i++)
                {
                    int custKey = loCustomerKey[i];
                    int suppKey = loSupplierKey[i];
                    int dateKey = loOrderDate[i];
                    Tuple<string, string> cOut = null;
                    Tuple<string, string> sOut = null;
                    Tuple<string, string> dOut = null;
                    if (customerHashTable.TryGetValue(custKey, out cOut)
                        && supplierHashTable.TryGetValue(suppKey, out sOut)
                        && dateHashTable.TryGetValue(dateKey, out dOut))
                    {
                        switch (numberOfGroupingAttributes)
                        {
                            case 1:
                                groupingAttributes.Add(cOut.Item1);
                                break;
                            case 2:
                                groupingAttributes.Add(cOut.Item1);
                                groupingAttributes.Add(cOut.Item2);

                                break;
                            case 3:
                                groupingAttributes.Add(cOut.Item1);
                                groupingAttributes.Add(cOut.Item2);
                                groupingAttributes.Add(sOut.Item1);
                                break;
                            case 4:
                                groupingAttributes.Add(cOut.Item1);
                                groupingAttributes.Add(cOut.Item2);
                                groupingAttributes.Add(sOut.Item1);
                                groupingAttributes.Add(sOut.Item2);
                                break;
                            case 5:
                                groupingAttributes.Add(cOut.Item1);
                                groupingAttributes.Add(cOut.Item2);
                                groupingAttributes.Add(sOut.Item1);
                                groupingAttributes.Add(sOut.Item2);
                                groupingAttributes.Add(dOut.Item1);
                                break;
                            case 6:
                                groupingAttributes.Add(cOut.Item1);
                                groupingAttributes.Add(cOut.Item2);
                                groupingAttributes.Add(sOut.Item1);
                                groupingAttributes.Add(sOut.Item2);
                                groupingAttributes.Add(dOut.Item1);
                                groupingAttributes.Add(dOut.Item2);
                                break;
                        }
                        tire.Insert(tire, groupingAttributes, true, loTax[i]);
                        groupingAttributes.Clear();
                    }
                }
                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                // Console.WriteLine(String.Format("Count: {0}", count));
                Console.WriteLine(String.Format("[Atire Join] GATest T1 Time: {0}", t1));

                Console.WriteLine(String.Format("[Atire Join] GATest Total Time: {0}", t0 + t1));
                tire.GetResults(tire);
                var results = tire.results;
                //System.IO.File.WriteAllLines(@"C:\Results\AtireJoin.txt", results);
                Console.WriteLine(String.Format("[Atire Join] GATest Total Items: {0}", results.Count));
                Console.WriteLine();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public void saveAndPrintResults()
        {
            //TestResultsDatabase.nimbleJoinOutput.Add(testResults.toString());
            //Console.WriteLine("DGJoin: " + testResults.toString());
            //Console.WriteLine();
        }

    }

    class GroupingAttributes
    {
        public string X { get; set; }
        public string Y { get; set; }
        public int Z { get; set; }
        public string Key { get; set; }

        public GroupingAttributes()
        {
            this.X = string.Empty;
            this.Y = string.Empty;
            this.Z = 0;
            this.Key = string.Empty;
        }
        public bool isFilled()
        {
            if (string.IsNullOrEmpty(X) || string.IsNullOrEmpty(Y) || Z == 0)
            {
                return false;
            }
            return true;
        }

        public string getKey()
        {
            if (string.IsNullOrEmpty(Key))
            {
                Key = X + ", " + Y + ", " + Z;
            }
            return Key;
        }
    }
}
