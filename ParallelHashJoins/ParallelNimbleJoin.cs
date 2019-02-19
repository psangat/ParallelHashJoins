﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParallelHashJoins
{
    class ParallelNimbleJoin
    {
        struct Triplets
        {
            public string x;
            public string y;
            public string z;
        };

        private static string binaryFilesDirectory = @"C:\Raw_Data_Source_For_Test\SSBM - DBGEN\BF";
        private string scaleFactor { get; set; }
        private MemoryManagement memoryManagement { get; set; }

        private ParallelOptions parallelOptions = null;

        public ParallelNimbleJoin(string scaleFactor, int degreeOfParallelism = 1)
        {
            this.scaleFactor = scaleFactor;
            testResults.totalRAMAvailable = Utils.getAvailableRAM();
            parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = degreeOfParallelism };
        }
        ~ParallelNimbleJoin()
        {
            // saveAndPrintResults();
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

        private bool isFirst = true;
        private const int NUMBER_OF_RECORDS_OUTPUT = 10000;
        private int outputRecordsCounter = 0;
        #endregion Private Variables


        public TestResults testResults = new TestResults();

        public void Query_1_1()
        {
            try
            {
                long memoryStartPhase1 = GC.GetTotalMemory(true);
                outputRecordsCounter = 0;
                var dateHashTable = new Dictionary<int, string>();
                Stopwatch sw = new Stopwatch();
                Stopwatch swInitialRecorder = new Stopwatch();
                Stopwatch swOutputRecorder = new Stopwatch();

                #region Key Hashing Phase
                List<Date> dateDimension = null;

                swInitialRecorder.Start();
                swOutputRecorder.Start();
                sw.Start();
                dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase11IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                foreach (var row in dateDimension)
                {
                    if (row.dYear.Equals("1993"))
                        dateHashTable.Add(row.dDateKey, row.dYear);
                }
                sw.Stop();
                testResults.phase11HashTime = sw.ElapsedMilliseconds;
                sw.Reset();

                testResults.phase13HashTime = sw.ElapsedMilliseconds;
                testResults.phase1Time = testResults.phase11HashTime + testResults.phase11IOTime +
                    testResults.phase12HashTime + testResults.phase12IOTime +
                    testResults.phase13HashTime + testResults.phase13IOTime;
                sw.Reset();

                dateDimension.Clear();

                long memoryUsedPhase1 = GC.GetTotalMemory(true) - memoryStartPhase1;
                #endregion Key Hashing Phase

                #region Probing Phase
                long memoryStartPhase2 = GC.GetTotalMemory(true);

                sw.Start();
                List<int> loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase22IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var intermediateHashTable = new Dictionary<int, Triplets>();
                var k = 0;
                foreach (var orderDate in loOrderDate)
                {
                    string dYear = string.Empty;
                    Triplets values = new Triplets();
                    if (dateHashTable.TryGetValue(orderDate, out dYear))
                    {
                        intermediateHashTable.Add(k, values);
                    }
                    k++;
                }

                sw.Stop();
                testResults.phase22ProbeTime = sw.ElapsedMilliseconds;

                testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime +
                    testResults.phase22IOTime + testResults.phase22ProbeTime +
                    testResults.phase23IOTime + testResults.phase23ProbeTime;
                sw.Reset();

                loOrderDate.Clear();

                long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
                #endregion Probing Phase

                #region Value Extraction Phase
                long memoryStartPhase3 = GC.GetTotalMemory(true);
                sw.Start();
                List<int> loExtendedPrice = null;
                List<int> loDiscount = null;
                List<int> loQuantity = null;

                Parallel.Invoke(parallelOptions, () => loExtendedPrice = Utils.ReadFromBinaryFiles<int>(loExtendedPriceFile.Replace("BF", "BF" + scaleFactor)),
                    () => loDiscount = Utils.ReadFromBinaryFiles<int>(loDiscountFile.Replace("BF", "BF" + scaleFactor)),
                    () => loQuantity = Utils.ReadFromBinaryFiles<int>(loQuantityFile.Replace("BF", "BF" + scaleFactor)));
                sw.Stop();
                testResults.phase3IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();

                var j = 0;
                int totalRevenue = 0;
                object lockObject = new object();
                Parallel.ForEach(intermediateHashTable, (row) =>
                {
                    string cNation = string.Empty;
                    int key = row.Key;
                    if (loQuantity[key] < 25)
                    {
                        int discount = loDiscount[key];
                        if (discount >= 1 && discount <= 3)
                        {
                            lock (lockObject)
                            {
                                var revenue = (loExtendedPrice[key] * discount);
                                totalRevenue += revenue;
                            }

                            if (isFirst)
                            {
                                swInitialRecorder.Stop();
                                testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
                                isFirst = false;
                            }
                            //Console.WriteLine(k +", "+ values.ToString() + ", " + sNation);
                            outputRecordsCounter++;
                            if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
                            {
                                swOutputRecorder.Stop();
                                testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
                                swOutputRecorder.Start();
                            }
                        }
                        else
                        {
                            // intermediateHashTable.Remove(j);
                        }
                    }
                    else
                    {
                        // intermediateHashTable.Remove(j);
                    }
                    j++;
                });

                sw.Stop();

                long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
                #endregion Value Extraction Phase
                testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
                testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
                testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
                // Console.WriteLine("[Nimble Join]: Time taken {0} ms.", testResults.totalExecutionTime);
                testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
                testResults.totalNumberOfOutput = totalRevenue;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public void Query_1_2()
        {
            try
            {
                long memoryStartPhase1 = GC.GetTotalMemory(true);
                outputRecordsCounter = 0;
                var dateHashTable = new Dictionary<int, string>();
                Stopwatch sw = new Stopwatch();
                Stopwatch swInitialRecorder = new Stopwatch();
                Stopwatch swOutputRecorder = new Stopwatch();

                #region Key Hashing Phase
                List<Date> dateDimension = null;

                swInitialRecorder.Start();
                swOutputRecorder.Start();
                sw.Start();
                dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase11IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                foreach (var row in dateDimension)
                {
                    if (row.dYearMonthNum == 199401)
                        dateHashTable.Add(row.dDateKey, row.dYear);
                }
                sw.Stop();
                testResults.phase11HashTime = sw.ElapsedMilliseconds;
                sw.Reset();

                testResults.phase13HashTime = sw.ElapsedMilliseconds;
                testResults.phase1Time = testResults.phase11HashTime + testResults.phase11IOTime +
                    testResults.phase12HashTime + testResults.phase12IOTime +
                    testResults.phase13HashTime + testResults.phase13IOTime;
                sw.Reset();

                dateDimension.Clear();

                long memoryUsedPhase1 = GC.GetTotalMemory(true) - memoryStartPhase1;
                #endregion Key Hashing Phase

                #region Probing Phase
                long memoryStartPhase2 = GC.GetTotalMemory(true);

                sw.Start();
                List<int> loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase22IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var intermediateHashTable = new Dictionary<int, Triplets>();
                var k = 0;
                foreach (var orderDate in loOrderDate)
                {
                    string dYear = string.Empty;
                    Triplets values = new Triplets();
                    if (dateHashTable.TryGetValue(orderDate, out dYear))
                    {
                        intermediateHashTable.Add(k, values);
                    }
                    k++;
                }

                sw.Stop();
                testResults.phase22ProbeTime = sw.ElapsedMilliseconds;

                testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime +
                    testResults.phase22IOTime + testResults.phase22ProbeTime +
                    testResults.phase23IOTime + testResults.phase23ProbeTime;
                sw.Reset();

                loOrderDate.Clear();

                long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
                #endregion Probing Phase

                #region Value Extraction Phase
                long memoryStartPhase3 = GC.GetTotalMemory(true);
                sw.Start();
                List<int> loExtendedPrice = null;
                List<int> loDiscount = null;
                List<int> loQuantity = null;

                Parallel.Invoke(parallelOptions, () => loExtendedPrice = Utils.ReadFromBinaryFiles<int>(loExtendedPriceFile.Replace("BF", "BF" + scaleFactor)),
                    () => loDiscount = Utils.ReadFromBinaryFiles<int>(loDiscountFile.Replace("BF", "BF" + scaleFactor)),
                    () => loQuantity = Utils.ReadFromBinaryFiles<int>(loQuantityFile.Replace("BF", "BF" + scaleFactor)));
                sw.Stop();
                testResults.phase3IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();

                var j = 0;
                int totalRevenue = 0;
                object lockObject = new object();
                Parallel.ForEach(intermediateHashTable, (row) =>
                {
                    string cNation = string.Empty;
                    int key = row.Key;
                    int quantity = loQuantity[key];
                    if (quantity >= 26 && quantity <= 35)
                    {
                        int discount = loDiscount[key];
                        if (discount >= 4 && discount <= 6)
                        {
                            lock (lockObject)
                            {
                                var revenue = (loExtendedPrice[key] * discount);
                                totalRevenue += revenue;
                            }

                            if (isFirst)
                            {
                                swInitialRecorder.Stop();
                                testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
                                isFirst = false;
                            }
                            //Console.WriteLine(k +", "+ values.ToString() + ", " + sNation);
                            outputRecordsCounter++;
                            if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
                            {
                                swOutputRecorder.Stop();
                                testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
                                swOutputRecorder.Start();
                            }
                        }
                        else
                        {
                            // intermediateHashTable.Remove(j);
                        }
                    }
                    else
                    {
                        // intermediateHashTable.Remove(j);
                    }
                    j++;
                });

                sw.Stop();

                long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
                #endregion Value Extraction Phase
                testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
                testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
                testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
                // Console.WriteLine("[Nimble Join]: Time taken {0} ms.", testResults.totalExecutionTime);
                testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
                testResults.totalNumberOfOutput = totalRevenue;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public void Query_1_3()
        {
            try
            {
                long memoryStartPhase1 = GC.GetTotalMemory(true);
                outputRecordsCounter = 0;
                var dateHashTable = new Dictionary<int, string>();
                Stopwatch sw = new Stopwatch();
                Stopwatch swInitialRecorder = new Stopwatch();
                Stopwatch swOutputRecorder = new Stopwatch();

                #region Key Hashing Phase
                List<Date> dateDimension = null;

                swInitialRecorder.Start();
                swOutputRecorder.Start();
                sw.Start();
                dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase11IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                foreach (var row in dateDimension)
                {
                    if (row.dYear.Equals("1994") && row.dWeekNumInYear == 6)
                        dateHashTable.Add(row.dDateKey, row.dYear);
                }
                sw.Stop();
                testResults.phase11HashTime = sw.ElapsedMilliseconds;
                sw.Reset();

                testResults.phase13HashTime = sw.ElapsedMilliseconds;
                testResults.phase1Time = testResults.phase11HashTime + testResults.phase11IOTime +
                    testResults.phase12HashTime + testResults.phase12IOTime +
                    testResults.phase13HashTime + testResults.phase13IOTime;
                sw.Reset();

                dateDimension.Clear();

                long memoryUsedPhase1 = GC.GetTotalMemory(true) - memoryStartPhase1;
                #endregion Key Hashing Phase

                #region Probing Phase
                long memoryStartPhase2 = GC.GetTotalMemory(true);

                sw.Start();
                List<int> loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase22IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var intermediateHashTable = new Dictionary<int, Triplets>();
                var k = 0;
                foreach (var orderDate in loOrderDate)
                {
                    string dYear = string.Empty;
                    Triplets values = new Triplets();
                    if (dateHashTable.TryGetValue(orderDate, out dYear))
                    {
                        intermediateHashTable.Add(k, values);
                    }
                    k++;
                }

                sw.Stop();
                testResults.phase22ProbeTime = sw.ElapsedMilliseconds;

                testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime +
                    testResults.phase22IOTime + testResults.phase22ProbeTime +
                    testResults.phase23IOTime + testResults.phase23ProbeTime;
                sw.Reset();

                loOrderDate.Clear();

                long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
                #endregion Probing Phase

                #region Value Extraction Phase
                long memoryStartPhase3 = GC.GetTotalMemory(true);
                sw.Start();
                List<int> loExtendedPrice = null;
                List<int> loDiscount = null;
                List<int> loQuantity = null;

                Parallel.Invoke(parallelOptions, () => loExtendedPrice = Utils.ReadFromBinaryFiles<int>(loExtendedPriceFile.Replace("BF", "BF" + scaleFactor)),
                    () => loDiscount = Utils.ReadFromBinaryFiles<int>(loDiscountFile.Replace("BF", "BF" + scaleFactor)),
                    () => loQuantity = Utils.ReadFromBinaryFiles<int>(loQuantityFile.Replace("BF", "BF" + scaleFactor)));
                sw.Stop();
                testResults.phase3IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();

                var j = 0;
                int totalRevenue = 0;
                object lockObject = new object();
                Parallel.ForEach(intermediateHashTable, (row) =>
                {
                    string cNation = string.Empty;
                    int key = row.Key;
                    int quantity = loQuantity[key];
                    if (quantity >= 26 && quantity <= 35)
                    {
                        int discount = loDiscount[key];
                        if (discount >= 5 && discount <= 7)
                        {
                            lock (lockObject)
                            {
                                var revenue = (loExtendedPrice[key] * discount);
                                totalRevenue += revenue;
                            }

                            if (isFirst)
                            {
                                swInitialRecorder.Stop();
                                testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
                                isFirst = false;
                            }
                            //Console.WriteLine(k +", "+ values.ToString() + ", " + sNation);
                            outputRecordsCounter++;
                            if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
                            {
                                swOutputRecorder.Stop();
                                testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
                                swOutputRecorder.Start();
                            }
                        }
                        else
                        {
                            // intermediateHashTable.Remove(j);
                        }
                    }
                    else
                    {
                        // intermediateHashTable.Remove(j);
                    }
                    j++;
                });

                sw.Stop();

                long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
                #endregion Value Extraction Phase
                testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
                testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
                testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
                // Console.WriteLine("[Nimble Join]: Time taken {0} ms.", testResults.totalExecutionTime);
                testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
                testResults.totalNumberOfOutput = totalRevenue;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void Query_2_1()
        {
            try
            {
                long memoryStartPhase1 = GC.GetTotalMemory(true);
                outputRecordsCounter = 0;
                var dateHashTable = new Dictionary<int, string>();
                var partHashTable = new Dictionary<int, string>();
                var supplierHashTable = new Dictionary<int, string>();
                Stopwatch sw = new Stopwatch();
                Stopwatch swInitialRecorder = new Stopwatch();
                Stopwatch swOutputRecorder = new Stopwatch();

                #region Key Hashing Phase
                List<Date> dateDimension = null;
                List<Supplier> supplierDimension = null;
                List<Part> partDimension = null;

                swInitialRecorder.Start();
                swOutputRecorder.Start();
                sw.Start();
                Parallel.Invoke(parallelOptions,
                    () => dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor)),
                    () => partDimension = Utils.ReadFromBinaryFiles<Part>(partFile.Replace("BF", "BF" + scaleFactor)),
                    () => supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor)));
                sw.Stop();
                testResults.phase11IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                Parallel.Invoke(parallelOptions, () =>
                {
                    foreach (var row in dateDimension)
                    {
                        dateHashTable.Add(row.dDateKey, row.dYear);
                    }
                },
                    () =>
                    {
                        foreach (var row in partDimension)
                        {
                            if (row.pCategory.Equals("MFGR#12"))
                                partHashTable.Add(row.pPartKey, row.pBrand);
                        }
                    },
                    () =>
                    {
                        foreach (var row in supplierDimension)
                        {
                            if (row.sRegion.Equals("AMERICA"))
                                supplierHashTable.Add(row.sSuppKey, row.sNation);
                        }
                    }
                    );

                sw.Stop();
                testResults.phase11HashTime = sw.ElapsedMilliseconds;
                testResults.phase1Time = testResults.phase11HashTime + testResults.phase11IOTime;
                sw.Reset();

                partDimension.Clear();
                dateDimension.Clear();
                supplierDimension.Clear();

                long memoryUsedPhase1 = GC.GetTotalMemory(true) - memoryStartPhase1;
                #endregion Key Hashing Phase

                #region Probing Phase
                long memoryStartPhase2 = GC.GetTotalMemory(true);
                sw.Start();
                List<int> loOrderDate = null;
                List<int> loPartKey = null;
                List<int> loSupplierKey = null;
                Parallel.Invoke(parallelOptions,
                    () => loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor)),
                    () => loPartKey = Utils.ReadFromBinaryFiles<int>(loPartKeyFile.Replace("BF", "BF" + scaleFactor)),
                    () => loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor)));
                sw.Stop();
                testResults.phase21IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                // var _maat = new MAAT(loOrderDate.Count);
                var _maat = new Dictionary<int, string>();
                var k = 0;
                foreach (var suppKey in loSupplierKey)
                {
                    string sNationOut = string.Empty;
                    if (supplierHashTable.TryGetValue(suppKey, out sNationOut))
                    {
                        _maat.Add(k, sNationOut);
                    }
                    k++;
                }

                var i = 0;
                foreach (var orderDate in loOrderDate)
                {
                    string dYear = "";
                    if (dateHashTable.TryGetValue(orderDate, out dYear))
                    {
                        string record;
                        if (_maat.TryGetValue(i, out record))
                        {
                            _maat[i] = record + "," + dYear;
                        }
                    }
                    else
                    {
                        _maat.Remove(i);
                    }
                    i++;
                }

                var j = 0;
                foreach (var partKey in loPartKey)
                {
                    string pBrandOut = string.Empty;
                    if (partHashTable.TryGetValue(partKey, out pBrandOut))
                    {
                        string record;
                        if (_maat.TryGetValue(j, out record))
                        {

                            _maat[j] = record + "," + pBrandOut;

                            if (isFirst)
                            {
                                swInitialRecorder.Stop();
                                testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
                                isFirst = false;
                            }
                            //Console.WriteLine(k +", "+ values.ToString() + ", " + sNation);
                            outputRecordsCounter++;
                            if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
                            {
                                swOutputRecorder.Stop();
                                testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
                                swOutputRecorder.Start();
                            }
                            //_maat.positions.Add(j);
                        }
                    }
                    else
                    {
                        _maat.Remove(j);
                    }
                    j++;
                }


                sw.Stop();
                testResults.phase21ProbeTime = sw.ElapsedMilliseconds;

                testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime;
                sw.Reset();

                loOrderDate.Clear();
                dateHashTable.Clear();
                loPartKey.Clear();
                partHashTable.Clear();
                loSupplierKey.Clear();
                supplierHashTable.Clear();

                long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
                #endregion Probing Phase


                #region Value Extraction Phase
                long memoryStartPhase3 = GC.GetTotalMemory(true);
                sw.Start();
                List<int> loRevenue = Utils.ReadFromBinaryFiles<int>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase3IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var joinOutputFinal = new Dictionary<int, string>();
                foreach (var item in _maat)
                {
                    joinOutputFinal.Add(item.Key, item.Value + ", " + loRevenue[item.Key]); // Direct array lookup
                }
                sw.Stop();

                long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
                #endregion Value Extraction Phase
                testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
                testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
                testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
                //Console.WriteLine("[Invisble Join]: Time taken {0} ms.", sw.ElapsedMilliseconds);
                testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
                testResults.totalNumberOfOutput = joinOutputFinal.Count;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public void Query_2_2()
        {
            try
            {
                long memoryStartPhase1 = GC.GetTotalMemory(true);
                outputRecordsCounter = 0;
                var dateHashTable = new Dictionary<int, string>();
                var partHashTable = new Dictionary<int, string>();
                var supplierHashTable = new Dictionary<int, string>();
                Stopwatch sw = new Stopwatch();
                Stopwatch swInitialRecorder = new Stopwatch();
                Stopwatch swOutputRecorder = new Stopwatch();

                #region Key Hashing Phase
                List<Date> dateDimension = null;
                List<Supplier> supplierDimension = null;
                List<Part> partDimension = null;

                swInitialRecorder.Start();
                swOutputRecorder.Start();
                sw.Start();
                Parallel.Invoke(parallelOptions,
                    () => dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor)),
                    () => partDimension = Utils.ReadFromBinaryFiles<Part>(partFile.Replace("BF", "BF" + scaleFactor)),
                    () => supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor)));
                sw.Stop();
                testResults.phase11IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                Parallel.Invoke(parallelOptions, () =>
                {
                    foreach (var row in dateDimension)
                    {
                        dateHashTable.Add(row.dDateKey, row.dYear);
                    }
                },
                    () =>
                    {
                        foreach (var row in partDimension)
                        {
                            if (String.CompareOrdinal(row.pBrand, "MFGR#2221") >= 0 && String.CompareOrdinal(row.pBrand, "MFGR#2228") <= 0)
                                partHashTable.Add(row.pPartKey, row.pBrand);
                        }
                    },
                    () =>
                    {
                        foreach (var row in supplierDimension)
                        {
                            if (row.sRegion.Equals("ASIA"))
                                supplierHashTable.Add(row.sSuppKey, row.sNation);
                        }
                    }
                    );

                sw.Stop();
                testResults.phase11HashTime = sw.ElapsedMilliseconds;
                testResults.phase1Time = testResults.phase11HashTime + testResults.phase11IOTime;
                sw.Reset();

                partDimension.Clear();
                dateDimension.Clear();
                supplierDimension.Clear();

                long memoryUsedPhase1 = GC.GetTotalMemory(true) - memoryStartPhase1;
                #endregion Key Hashing Phase

                #region Probing Phase
                long memoryStartPhase2 = GC.GetTotalMemory(true);
                sw.Start();
                List<int> loOrderDate = null;
                List<int> loPartKey = null;
                List<int> loSupplierKey = null;
                Parallel.Invoke(parallelOptions,
                    () => loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor)),
                    () => loPartKey = Utils.ReadFromBinaryFiles<int>(loPartKeyFile.Replace("BF", "BF" + scaleFactor)),
                    () => loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor)));
                sw.Stop();
                testResults.phase21IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                // var _maat = new MAAT(loOrderDate.Count);
                var _maat = new Dictionary<int, string>();
                var k = 0;
                foreach (var suppKey in loSupplierKey)
                {
                    string sNationOut = string.Empty;
                    if (supplierHashTable.TryGetValue(suppKey, out sNationOut))
                    {
                        _maat.Add(k, sNationOut);
                    }
                    k++;
                }

                var i = 0;
                foreach (var orderDate in loOrderDate)
                {
                    string dYear = "";
                    if (dateHashTable.TryGetValue(orderDate, out dYear))
                    {
                        string record;
                        if (_maat.TryGetValue(i, out record))
                        {
                            _maat[i] = record + "," + dYear;
                        }
                    }
                    else
                    {
                        _maat.Remove(i);
                    }
                    i++;
                }

                var j = 0;
                foreach (var partKey in loPartKey)
                {
                    string pBrandOut = string.Empty;
                    if (partHashTable.TryGetValue(partKey, out pBrandOut))
                    {
                        string record;
                        if (_maat.TryGetValue(j, out record))
                        {

                            _maat[j] = record + "," + pBrandOut;

                            if (isFirst)
                            {
                                swInitialRecorder.Stop();
                                testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
                                isFirst = false;
                            }
                            //Console.WriteLine(k +", "+ values.ToString() + ", " + sNation);
                            outputRecordsCounter++;
                            if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
                            {
                                swOutputRecorder.Stop();
                                testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
                                swOutputRecorder.Start();
                            }
                            //_maat.positions.Add(j);
                        }
                    }
                    else
                    {
                        _maat.Remove(j);
                    }
                    j++;
                }


                sw.Stop();
                testResults.phase21ProbeTime = sw.ElapsedMilliseconds;

                testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime;
                sw.Reset();

                loOrderDate.Clear();
                dateHashTable.Clear();
                loPartKey.Clear();
                partHashTable.Clear();
                loSupplierKey.Clear();
                supplierHashTable.Clear();

                long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
                #endregion Probing Phase


                #region Value Extraction Phase
                long memoryStartPhase3 = GC.GetTotalMemory(true);
                sw.Start();
                List<int> loRevenue = Utils.ReadFromBinaryFiles<int>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase3IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var joinOutputFinal = new Dictionary<int, string>();
                foreach (var item in _maat)
                {
                    joinOutputFinal.Add(item.Key, item.Value + ", " + loRevenue[item.Key]); // Direct array lookup
                }
                sw.Stop();

                long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
                #endregion Value Extraction Phase
                testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
                testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
                testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
                //Console.WriteLine("[Invisble Join]: Time taken {0} ms.", sw.ElapsedMilliseconds);
                testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
                testResults.totalNumberOfOutput = joinOutputFinal.Count;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public void Query_2_3()
        {
            try
            {
                long memoryStartPhase1 = GC.GetTotalMemory(true);
                outputRecordsCounter = 0;
                var dateHashTable = new Dictionary<int, string>();
                var partHashTable = new Dictionary<int, string>();
                var supplierHashTable = new Dictionary<int, string>();
                Stopwatch sw = new Stopwatch();
                Stopwatch swInitialRecorder = new Stopwatch();
                Stopwatch swOutputRecorder = new Stopwatch();

                #region Key Hashing Phase
                List<Date> dateDimension = null;
                List<Supplier> supplierDimension = null;
                List<Part> partDimension = null;

                swInitialRecorder.Start();
                swOutputRecorder.Start();
                sw.Start();
                Parallel.Invoke(parallelOptions,
                    () => dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor)),
                    () => partDimension = Utils.ReadFromBinaryFiles<Part>(partFile.Replace("BF", "BF" + scaleFactor)),
                    () => supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor)));
                sw.Stop();
                testResults.phase11IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                Parallel.Invoke(parallelOptions, () =>
                {
                    foreach (var row in dateDimension)
                    {
                        dateHashTable.Add(row.dDateKey, row.dYear);
                    }
                },
                    () =>
                    {
                        foreach (var row in partDimension)
                        {
                            if (row.pBrand.Equals("MFGR#2221"))
                                partHashTable.Add(row.pPartKey, row.pBrand);
                        }
                    },
                    () =>
                    {
                        foreach (var row in supplierDimension)
                        {
                            if (row.sRegion.Equals("EUROPE"))
                                supplierHashTable.Add(row.sSuppKey, row.sNation);
                        }
                    }
                    );

                sw.Stop();
                testResults.phase11HashTime = sw.ElapsedMilliseconds;
                testResults.phase1Time = testResults.phase11HashTime + testResults.phase11IOTime;
                sw.Reset();

                partDimension.Clear();
                dateDimension.Clear();
                supplierDimension.Clear();

                long memoryUsedPhase1 = GC.GetTotalMemory(true) - memoryStartPhase1;
                #endregion Key Hashing Phase

                #region Probing Phase
                long memoryStartPhase2 = GC.GetTotalMemory(true);
                sw.Start();
                List<int> loOrderDate = null;
                List<int> loPartKey = null;
                List<int> loSupplierKey = null;
                Parallel.Invoke(parallelOptions,
                    () => loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor)),
                    () => loPartKey = Utils.ReadFromBinaryFiles<int>(loPartKeyFile.Replace("BF", "BF" + scaleFactor)),
                    () => loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor)));
                sw.Stop();
                testResults.phase21IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                // var _maat = new MAAT(loOrderDate.Count);
                var _maat = new Dictionary<int, string>();
                var k = 0;
                foreach (var suppKey in loSupplierKey)
                {
                    string sNationOut = string.Empty;
                    if (supplierHashTable.TryGetValue(suppKey, out sNationOut))
                    {
                        _maat.Add(k, sNationOut);
                    }
                    k++;
                }

                var i = 0;
                foreach (var orderDate in loOrderDate)
                {
                    string dYear = "";
                    if (dateHashTable.TryGetValue(orderDate, out dYear))
                    {
                        string record;
                        if (_maat.TryGetValue(i, out record))
                        {
                            _maat[i] = record + "," + dYear;
                        }
                    }
                    else
                    {
                        _maat.Remove(i);
                    }
                    i++;
                }

                var j = 0;
                foreach (var partKey in loPartKey)
                {
                    string pBrandOut = string.Empty;
                    if (partHashTable.TryGetValue(partKey, out pBrandOut))
                    {
                        string record;
                        if (_maat.TryGetValue(j, out record))
                        {

                            _maat[j] = record + "," + pBrandOut;

                            if (isFirst)
                            {
                                swInitialRecorder.Stop();
                                testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
                                isFirst = false;
                            }
                            //Console.WriteLine(k +", "+ values.ToString() + ", " + sNation);
                            outputRecordsCounter++;
                            if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
                            {
                                swOutputRecorder.Stop();
                                testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
                                swOutputRecorder.Start();
                            }
                            //_maat.positions.Add(j);
                        }
                    }
                    else
                    {
                        _maat.Remove(j);
                    }
                    j++;
                }


                sw.Stop();
                testResults.phase21ProbeTime = sw.ElapsedMilliseconds;

                testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime;
                sw.Reset();

                loOrderDate.Clear();
                dateHashTable.Clear();
                loPartKey.Clear();
                partHashTable.Clear();
                loSupplierKey.Clear();
                supplierHashTable.Clear();

                long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
                #endregion Probing Phase


                #region Value Extraction Phase
                long memoryStartPhase3 = GC.GetTotalMemory(true);
                sw.Start();
                List<int> loRevenue = Utils.ReadFromBinaryFiles<int>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase3IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var joinOutputFinal = new Dictionary<int, string>();
                foreach (var item in _maat)
                {
                    joinOutputFinal.Add(item.Key, item.Value + ", " + loRevenue[item.Key]); // Direct array lookup
                }
                sw.Stop();

                long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
                #endregion Value Extraction Phase
                testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
                testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
                testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
                //Console.WriteLine("[Invisble Join]: Time taken {0} ms.", sw.ElapsedMilliseconds);
                testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
                testResults.totalNumberOfOutput = joinOutputFinal.Count;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void Query_4_1()
        {

            try
            {
                long memoryStartPhase1 = GC.GetTotalMemory(true);
                outputRecordsCounter = 0;
                var dateHashTable = new Dictionary<int, string>();
                var partHashTable = new Dictionary<int, string>();
                var supplierHashTable = new Dictionary<int, string>();
                var customerHashTable = new Dictionary<int, string>();
                Stopwatch sw = new Stopwatch();
                Stopwatch swInitialRecorder = new Stopwatch();
                Stopwatch swOutputRecorder = new Stopwatch();

                #region Key Hashing Phase
                List<Date> dateDimension = null;
                List<Supplier> supplierDimension = null;
                List<Part> partDimension = null;
                List<Customer> customerDimension = null;

                swInitialRecorder.Start();
                swOutputRecorder.Start();
                sw.Start();
                Parallel.Invoke(parallelOptions,
                    () => dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor)),
                    () => partDimension = Utils.ReadFromBinaryFiles<Part>(partFile.Replace("BF", "BF" + scaleFactor)),
                    () => supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor)),
                    () => customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor)));
                sw.Stop();
                testResults.phase11IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                Parallel.Invoke(parallelOptions,
                    () =>
                    {
                        foreach (var row in dateDimension)
                        {
                            dateHashTable.Add(row.dDateKey, row.dYear);
                        }
                    },
                    () =>
                    {
                        foreach (var row in partDimension)
                        {
                            if (row.pMFGR.Equals("MFGR#1") || row.pMFGR.Equals("MFGR#2"))
                                partHashTable.Add(row.pPartKey, row.pMFGR);
                        }
                    },
                    () =>
                    {
                        foreach (var row in supplierDimension)
                        {
                            if (row.sRegion.Equals("AMERICA"))
                                supplierHashTable.Add(row.sSuppKey, row.sNation);
                        }
                    },
                    () =>
                    {
                        foreach (var row in customerDimension)
                        {
                            if (row.cRegion.Equals("AMERICA"))
                                customerHashTable.Add(row.cCustKey, row.cNation);
                        }
                    }
                    );

                sw.Stop();
                testResults.phase11HashTime = sw.ElapsedMilliseconds;
                testResults.phase1Time = testResults.phase11HashTime + testResults.phase11IOTime;
                sw.Reset();

                partDimension.Clear();
                dateDimension.Clear();
                supplierDimension.Clear();
                customerDimension.Clear();

                long memoryUsedPhase1 = GC.GetTotalMemory(true) - memoryStartPhase1;
                #endregion Key Hashing Phase

                #region Probing Phase
                long memoryStartPhase2 = GC.GetTotalMemory(true);
                sw.Start();
                List<int> loOrderDate = null;
                List<int> loPartKey = null;
                List<int> loSupplierKey = null;
                List<int> loCustomerKey = null;
                Parallel.Invoke(parallelOptions,
                    () => loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor)),
                    () => loPartKey = Utils.ReadFromBinaryFiles<int>(loPartKeyFile.Replace("BF", "BF" + scaleFactor)),
                    () => loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor)),
                    () => loCustomerKey = Utils.ReadFromBinaryFiles<int>(loCustKeyFile.Replace("BF", "BF" + scaleFactor)));
                sw.Stop();
                testResults.phase21IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                // var _maat = new MAAT(loOrderDate.Count);
                var _maat = new Dictionary<int, string>();
                var k = 0;
                foreach (var suppKey in loSupplierKey)
                {
                    string sNationOut = string.Empty;
                    if (supplierHashTable.TryGetValue(suppKey, out sNationOut))
                    {
                        _maat.Add(k, sNationOut);
                    }
                    k++;
                }

                var i = 0;
                foreach (var orderDate in loOrderDate)
                {
                    string dYear = "";
                    if (dateHashTable.TryGetValue(orderDate, out dYear))
                    {
                        string record;
                        if (_maat.TryGetValue(i, out record))
                        {
                            _maat[i] = record + "," + dYear;
                        }
                    }
                    else
                    {
                        _maat.Remove(i);
                    }
                    i++;
                }

                var j = 0;
                foreach (var custKey in loCustomerKey)
                {
                    string cNationOut = string.Empty;
                    if (customerHashTable.TryGetValue(custKey, out cNationOut))
                    {
                        string record;
                        if (_maat.TryGetValue(j, out record))
                        {
                            _maat[j] = record + "," + cNationOut;
                        }
                    }
                    else
                    {
                        _maat.Remove(j);
                    }
                    j++;
                }

                var l = 0;
                foreach (var partKey in loPartKey)
                {
                    string pBrandOut = string.Empty;
                    if (partHashTable.TryGetValue(partKey, out pBrandOut))
                    {
                        string record;
                        if (_maat.TryGetValue(l, out record))
                        {

                            //_maat[l] = record + "," + pBrandOut;

                            if (isFirst)
                            {
                                swInitialRecorder.Stop();
                                testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
                                isFirst = false;
                            }
                            //Console.WriteLine(k +", "+ values.ToString() + ", " + sNation);
                            outputRecordsCounter++;
                            if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
                            {
                                swOutputRecorder.Stop();
                                testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
                                swOutputRecorder.Start();
                            }
                            //_maat.positions.Add(j);
                        }
                    }
                    else
                    {
                        _maat.Remove(l);
                    }
                    l++;
                }


                sw.Stop();
                testResults.phase21ProbeTime = sw.ElapsedMilliseconds;

                testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime;
                sw.Reset();

                loOrderDate.Clear();
                dateHashTable.Clear();
                loPartKey.Clear();
                partHashTable.Clear();
                loSupplierKey.Clear();
                supplierHashTable.Clear();
                loCustomerKey.Clear();
                customerHashTable.Clear();

                long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
                #endregion Probing Phase


                #region Value Extraction Phase
                long memoryStartPhase3 = GC.GetTotalMemory(true);
                sw.Start();
                List<int> loRevenue = null;
                List<int> loSupplyCost = null;

                Parallel.Invoke(parallelOptions,
                    () => loRevenue = Utils.ReadFromBinaryFiles<int>(loRevenueFile.Replace("BF", "BF" + scaleFactor)),
                    () => loSupplyCost = Utils.ReadFromBinaryFiles<int>(loSupplyCostFile.Replace("BF", "BF" + scaleFactor)));

                sw.Stop();
                testResults.phase3IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var joinOutputFinal = new Dictionary<int, string>();
                foreach (var item in _maat)
                {
                    joinOutputFinal.Add(item.Key, item.Value + ", " + loRevenue[item.Key] + ", " + loSupplyCost[item.Key]); // Direct array lookup
                }
                sw.Stop();

                long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
                #endregion Value Extraction Phase
                testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
                testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
                testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
                //Console.WriteLine("[Invisble Join]: Time taken {0} ms.", sw.ElapsedMilliseconds);
                testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
                testResults.totalNumberOfOutput = joinOutputFinal.Count;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public void Query_4_2()
        {
            try
            {
                long memoryStartPhase1 = GC.GetTotalMemory(true);
                outputRecordsCounter = 0;
                var dateHashTable = new Dictionary<int, string>();
                var partHashTable = new Dictionary<int, string>();
                var supplierHashTable = new Dictionary<int, string>();
                var customerHashTable = new Dictionary<int, string>();
                Stopwatch sw = new Stopwatch();
                Stopwatch swInitialRecorder = new Stopwatch();
                Stopwatch swOutputRecorder = new Stopwatch();

                #region Key Hashing Phase
                List<Date> dateDimension = null;
                List<Supplier> supplierDimension = null;
                List<Part> partDimension = null;
                List<Customer> customerDimension = null;

                swInitialRecorder.Start();
                swOutputRecorder.Start();
                sw.Start();
                Parallel.Invoke(parallelOptions,
                    () => dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor)),
                    () => partDimension = Utils.ReadFromBinaryFiles<Part>(partFile.Replace("BF", "BF" + scaleFactor)),
                    () => supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor)),
                    () => customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor)));
                sw.Stop();
                testResults.phase11IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                Parallel.Invoke(parallelOptions,
                    () =>
                    {
                        foreach (var row in dateDimension)
                        {
                            if (row.dYear.Equals("1997") || row.dYear.Equals("1998"))
                                dateHashTable.Add(row.dDateKey, row.dYear);
                        }
                    },
                    () =>
                    {
                        foreach (var row in partDimension)
                        {
                            if (row.pMFGR.Equals("MFGR#1") || row.pMFGR.Equals("MFGR#2"))
                                partHashTable.Add(row.pPartKey, row.pCategory);
                        }
                    },
                    () =>
                    {
                        foreach (var row in supplierDimension)
                        {
                            if (row.sRegion.Equals("AMERICA"))
                                supplierHashTable.Add(row.sSuppKey, row.sNation);
                        }
                    },
                    () =>
                    {
                        foreach (var row in customerDimension)
                        {
                            if (row.cRegion.Equals("AMERICA"))
                                customerHashTable.Add(row.cCustKey, row.cNation);
                        }
                    }
                    );

                sw.Stop();
                testResults.phase11HashTime = sw.ElapsedMilliseconds;
                testResults.phase1Time = testResults.phase11HashTime + testResults.phase11IOTime;
                sw.Reset();

                partDimension.Clear();
                dateDimension.Clear();
                supplierDimension.Clear();
                customerDimension.Clear();

                long memoryUsedPhase1 = GC.GetTotalMemory(true) - memoryStartPhase1;
                #endregion Key Hashing Phase

                #region Probing Phase
                long memoryStartPhase2 = GC.GetTotalMemory(true);
                sw.Start();
                List<int> loOrderDate = null;
                List<int> loPartKey = null;
                List<int> loSupplierKey = null;
                List<int> loCustomerKey = null;
                Parallel.Invoke(parallelOptions,
                    () => loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor)),
                    () => loPartKey = Utils.ReadFromBinaryFiles<int>(loPartKeyFile.Replace("BF", "BF" + scaleFactor)),
                    () => loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor)),
                    () => loCustomerKey = Utils.ReadFromBinaryFiles<int>(loCustKeyFile.Replace("BF", "BF" + scaleFactor)));
                sw.Stop();
                testResults.phase21IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                // var _maat = new MAAT(loOrderDate.Count);
                var _maat = new Dictionary<int, string>();
                var k = 0;
                foreach (var suppKey in loSupplierKey)
                {
                    string sNationOut = string.Empty;
                    if (supplierHashTable.TryGetValue(suppKey, out sNationOut))
                    {
                        _maat.Add(k, sNationOut);
                    }
                    k++;
                }

                var i = 0;
                foreach (var orderDate in loOrderDate)
                {
                    string dYear = "";
                    if (dateHashTable.TryGetValue(orderDate, out dYear))
                    {
                        string record;
                        if (_maat.TryGetValue(i, out record))
                        {
                            _maat[i] = record + "," + dYear;
                        }
                    }
                    else
                    {
                        _maat.Remove(i);
                    }
                    i++;
                }

                var j = 0;
                foreach (var custKey in loCustomerKey)
                {
                    string cNationOut = string.Empty;
                    if (customerHashTable.TryGetValue(custKey, out cNationOut))
                    {
                        string record;
                        if (_maat.TryGetValue(j, out record))
                        {
                            // _maat[j] = record + "," + cNationOut;
                        }
                    }
                    else
                    {
                        _maat.Remove(j);
                    }
                    j++;
                }

                var l = 0;
                foreach (var partKey in loPartKey)
                {
                    string pCategoryOut = string.Empty;
                    if (partHashTable.TryGetValue(partKey, out pCategoryOut))
                    {
                        string record;
                        if (_maat.TryGetValue(l, out record))
                        {

                            _maat[l] = record + "," + pCategoryOut;

                            if (isFirst)
                            {
                                swInitialRecorder.Stop();
                                testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
                                isFirst = false;
                            }
                            //Console.WriteLine(k +", "+ values.ToString() + ", " + sNation);
                            outputRecordsCounter++;
                            if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
                            {
                                swOutputRecorder.Stop();
                                testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
                                swOutputRecorder.Start();
                            }
                            //_maat.positions.Add(j);
                        }
                    }
                    else
                    {
                        _maat.Remove(l);
                    }
                    l++;
                }


                sw.Stop();
                testResults.phase21ProbeTime = sw.ElapsedMilliseconds;

                testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime;
                sw.Reset();

                loOrderDate.Clear();
                dateHashTable.Clear();
                loPartKey.Clear();
                partHashTable.Clear();
                loSupplierKey.Clear();
                supplierHashTable.Clear();
                loCustomerKey.Clear();
                customerHashTable.Clear();

                long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
                #endregion Probing Phase


                #region Value Extraction Phase
                long memoryStartPhase3 = GC.GetTotalMemory(true);
                sw.Start();
                List<int> loRevenue = null;
                List<int> loSupplyCost = null;

                Parallel.Invoke(parallelOptions,
                    () => loRevenue = Utils.ReadFromBinaryFiles<int>(loRevenueFile.Replace("BF", "BF" + scaleFactor)),
                    () => loSupplyCost = Utils.ReadFromBinaryFiles<int>(loSupplyCostFile.Replace("BF", "BF" + scaleFactor)));

                sw.Stop();
                testResults.phase3IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var joinOutputFinal = new Dictionary<int, string>();
                foreach (var item in _maat)
                {
                    joinOutputFinal.Add(item.Key, item.Value + ", " + loRevenue[item.Key] + ", " + loSupplyCost[item.Key]); // Direct array lookup
                }
                sw.Stop();

                long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
                #endregion Value Extraction Phase
                testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
                testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
                testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
                //Console.WriteLine("[Invisble Join]: Time taken {0} ms.", sw.ElapsedMilliseconds);
                testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
                testResults.totalNumberOfOutput = joinOutputFinal.Count;
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }
        public void Query_4_3()
        {

            try
            {
                long memoryStartPhase1 = GC.GetTotalMemory(true);
                outputRecordsCounter = 0;
                var dateHashTable = new Dictionary<int, string>();
                var partHashTable = new Dictionary<int, string>();
                var supplierHashTable = new Dictionary<int, string>();
                var customerHashTable = new Dictionary<int, string>();
                Stopwatch sw = new Stopwatch();
                Stopwatch swInitialRecorder = new Stopwatch();
                Stopwatch swOutputRecorder = new Stopwatch();

                #region Key Hashing Phase
                List<Date> dateDimension = null;
                List<Supplier> supplierDimension = null;
                List<Part> partDimension = null;
                List<Customer> customerDimension = null;

                swInitialRecorder.Start();
                swOutputRecorder.Start();
                sw.Start();
                Parallel.Invoke(parallelOptions,
                    () => dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor)),
                    () => partDimension = Utils.ReadFromBinaryFiles<Part>(partFile.Replace("BF", "BF" + scaleFactor)),
                    () => supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor)),
                    () => customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor)));
                sw.Stop();
                testResults.phase11IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                Parallel.Invoke(parallelOptions,
                    () =>
                    {
                        foreach (var row in dateDimension)
                        {
                            if (row.dYear.Equals("1997") || row.dYear.Equals("1998"))
                                dateHashTable.Add(row.dDateKey, row.dYear);
                        }
                    },
                    () =>
                    {
                        foreach (var row in partDimension)
                        {
                            if (row.pCategory.Equals("MFGR#14"))
                                partHashTable.Add(row.pPartKey, row.pBrand);
                        }
                    },
                    () =>
                    {
                        foreach (var row in supplierDimension)
                        {
                            if (row.sNation.Equals("UNITED STATES"))
                                supplierHashTable.Add(row.sSuppKey, row.sNation);
                        }
                    },
                    () =>
                    {
                        foreach (var row in customerDimension)
                        {
                            if (row.cRegion.Equals("AMERICA"))
                                customerHashTable.Add(row.cCustKey, row.cNation);
                        }
                    }
                    );

                sw.Stop();
                testResults.phase11HashTime = sw.ElapsedMilliseconds;
                testResults.phase1Time = testResults.phase11HashTime + testResults.phase11IOTime;
                sw.Reset();

                partDimension.Clear();
                dateDimension.Clear();
                supplierDimension.Clear();
                customerDimension.Clear();

                long memoryUsedPhase1 = GC.GetTotalMemory(true) - memoryStartPhase1;
                #endregion Key Hashing Phase

                #region Probing Phase
                long memoryStartPhase2 = GC.GetTotalMemory(true);
                sw.Start();
                List<int> loOrderDate = null;
                List<int> loPartKey = null;
                List<int> loSupplierKey = null;
                List<int> loCustomerKey = null;
                Parallel.Invoke(parallelOptions,
                    () => loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor)),
                    () => loPartKey = Utils.ReadFromBinaryFiles<int>(loPartKeyFile.Replace("BF", "BF" + scaleFactor)),
                    () => loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor)),
                    () => loCustomerKey = Utils.ReadFromBinaryFiles<int>(loCustKeyFile.Replace("BF", "BF" + scaleFactor)));
                sw.Stop();
                testResults.phase21IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                // var _maat = new MAAT(loOrderDate.Count);
                var _maat = new Dictionary<int, string>();
                var k = 0;
                foreach (var suppKey in loSupplierKey)
                {
                    string sNationOut = string.Empty;
                    if (supplierHashTable.TryGetValue(suppKey, out sNationOut))
                    {
                        _maat.Add(k, sNationOut);
                    }
                    k++;
                }

                var i = 0;
                foreach (var orderDate in loOrderDate)
                {
                    string dYear = "";
                    if (dateHashTable.TryGetValue(orderDate, out dYear))
                    {
                        string record;
                        if (_maat.TryGetValue(i, out record))
                        {
                            _maat[i] = record + "," + dYear;
                        }
                    }
                    else
                    {
                        _maat.Remove(i);
                    }
                    i++;
                }

                var j = 0;
                foreach (var custKey in loCustomerKey)
                {
                    string cNationOut = string.Empty;
                    if (customerHashTable.TryGetValue(custKey, out cNationOut))
                    {
                        string record;
                        if (_maat.TryGetValue(j, out record))
                        {
                            // _maat[j] = record + "," + cNationOut;
                        }
                    }
                    else
                    {
                        _maat.Remove(j);
                    }
                    j++;
                }

                var l = 0;
                foreach (var partKey in loPartKey)
                {
                    string pBrandOut = string.Empty;
                    if (partHashTable.TryGetValue(partKey, out pBrandOut))
                    {
                        string record;
                        if (_maat.TryGetValue(l, out record))
                        {

                            _maat[l] = record + "," + pBrandOut;

                            if (isFirst)
                            {
                                swInitialRecorder.Stop();
                                testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
                                isFirst = false;
                            }
                            //Console.WriteLine(k +", "+ values.ToString() + ", " + sNation);
                            outputRecordsCounter++;
                            if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
                            {
                                swOutputRecorder.Stop();
                                testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
                                swOutputRecorder.Start();
                            }
                            //_maat.positions.Add(j);
                        }
                    }
                    else
                    {
                        _maat.Remove(l);
                    }
                    l++;
                }


                sw.Stop();
                testResults.phase21ProbeTime = sw.ElapsedMilliseconds;

                testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime;
                sw.Reset();

                loOrderDate.Clear();
                dateHashTable.Clear();
                loPartKey.Clear();
                partHashTable.Clear();
                loSupplierKey.Clear();
                supplierHashTable.Clear();
                loCustomerKey.Clear();
                customerHashTable.Clear();

                long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
                #endregion Probing Phase


                #region Value Extraction Phase
                long memoryStartPhase3 = GC.GetTotalMemory(true);
                sw.Start();
                List<int> loRevenue = null;
                List<int> loSupplyCost = null;

                Parallel.Invoke(parallelOptions,
                    () => loRevenue = Utils.ReadFromBinaryFiles<int>(loRevenueFile.Replace("BF", "BF" + scaleFactor)),
                    () => loSupplyCost = Utils.ReadFromBinaryFiles<int>(loSupplyCostFile.Replace("BF", "BF" + scaleFactor)));

                sw.Stop();
                testResults.phase3IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var joinOutputFinal = new Dictionary<int, string>();
                foreach (var item in _maat)
                {
                    joinOutputFinal.Add(item.Key, item.Value + ", " + loRevenue[item.Key] + ", " + loSupplyCost[item.Key]); // Direct array lookup
                }
                sw.Stop();

                long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
                #endregion Value Extraction Phase
                testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
                testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
                testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
                //Console.WriteLine("[Invisble Join]: Time taken {0} ms.", sw.ElapsedMilliseconds);
                testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
                testResults.totalNumberOfOutput = joinOutputFinal.Count;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public void Query_3_1()
        {
            try
            {
                long memoryStartPhase1 = GC.GetTotalMemory(true);
                outputRecordsCounter = 0;
                var dateHashTable = new Dictionary<int, string>();
                var customerHashTable = new Dictionary<int, string>();
                var supplierHashTable = new Dictionary<int, string>();
                Stopwatch sw = new Stopwatch();
                Stopwatch swInitialRecorder = new Stopwatch();
                Stopwatch swOutputRecorder = new Stopwatch();

                #region Key Hashing Phase
                List<Date> dateDimension = null;
                List<Supplier> supplierDimension = null;
                List<Customer> customerDimension = null;

                swInitialRecorder.Start();
                swOutputRecorder.Start();
                sw.Start();

                Parallel.Invoke(parallelOptions, () => dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor)),
                () => customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor)),
                () => supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor))
                     );
                sw.Stop();
                testResults.phase11IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                Parallel.Invoke(parallelOptions, () =>
                {
                    foreach (var row in dateDimension)
                    {
                        if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
                            dateHashTable.Add(row.dDateKey, row.dYear);
                    }
                },
                () =>
                {
                    foreach (var row in customerDimension)
                    {
                        if (row.cRegion.Equals("ASIA"))
                            customerHashTable.Add(row.cCustKey, row.cNation);
                    }
                },
                () =>
                {
                    foreach (var row in supplierDimension)
                    {
                        if (row.sRegion.Equals("ASIA"))
                            supplierHashTable.Add(row.sSuppKey, row.sNation);
                    }
                });
                sw.Stop();
                testResults.phase11HashTime = sw.ElapsedMilliseconds;
                testResults.phase1Time = testResults.phase11HashTime + testResults.phase11IOTime;
                sw.Reset();

                customerDimension.Clear();
                dateDimension.Clear();
                supplierDimension.Clear();

                long memoryUsedPhase1 = GC.GetTotalMemory(true) - memoryStartPhase1;
                #endregion Key Hashing Phase

                #region Probing Phase
                long memoryStartPhase2 = GC.GetTotalMemory(true);
                sw.Start();
                List<int> loOrderDate = null;
                List<int> loCustomerKey = null;
                List<int> loSupplierKey = null;
                Parallel.Invoke(parallelOptions, () => loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor)),
                  () => loCustomerKey = Utils.ReadFromBinaryFiles<int>(loCustKeyFile.Replace("BF", "BF" + scaleFactor)),
                  () => loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor)));
                sw.Stop();
                testResults.phase21IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var intermediateHashTable = new Dictionary<int, string>();
                var i = 0;
                foreach (var suppKey in loSupplierKey)
                {
                    string sNation = string.Empty;
                    if (supplierHashTable.TryGetValue(suppKey, out sNation))
                    {
                        intermediateHashTable.Add(i, sNation);
                    }
                    i++;
                }

                var k = 0;
                foreach (var orderDate in loOrderDate)
                {
                    string dYear = string.Empty;
                    if (dateHashTable.TryGetValue(orderDate, out dYear))
                    {
                        string values = string.Empty;
                        if (intermediateHashTable.TryGetValue(k, out values))
                        {
                            intermediateHashTable[k] = values + ", " + dYear;
                        }
                    }
                    else
                    {
                        intermediateHashTable.Remove(k);
                    }
                    k++;
                }

                var j = 0;
                foreach (var custKey in loCustomerKey)
                {
                    string cNation = string.Empty;
                    if (customerHashTable.TryGetValue(custKey, out cNation))
                    {
                        string values = string.Empty;
                        if (intermediateHashTable.TryGetValue(j, out values))
                        {
                            intermediateHashTable[j] = values + ", " + cNation;
                            if (isFirst)
                            {
                                swInitialRecorder.Stop();
                                testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
                                isFirst = false;
                            }
                            //Console.WriteLine(k +", "+ values.ToString() + ", " + sNation);

                            outputRecordsCounter++;
                            if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
                            {
                                swOutputRecorder.Stop();
                                testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
                                swOutputRecorder.Start();
                            }
                        }
                    }
                    else
                    {
                        intermediateHashTable.Remove(j);
                    }
                    j++;
                }

                sw.Stop();
                testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
                testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime;
                sw.Reset();

                loOrderDate.Clear();
                loCustomerKey.Clear();
                loSupplierKey.Clear();

                long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
                #endregion Probing Phase

                #region Value Extraction Phase
                long memoryStartPhase3 = GC.GetTotalMemory(true);
                sw.Start();
                List<int> loRevenue = Utils.ReadFromBinaryFiles<int>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase3IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var joinOutputFinal = new Dictionary<int, string>();
                foreach (var item in intermediateHashTable)
                {
                    joinOutputFinal.Add(item.Key, item.Value + ", " + loRevenue[item.Key]); // Direct array lookup
                }
                sw.Stop();

                long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
                #endregion Value Extraction Phase
                testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
                testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
                testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
                // Console.WriteLine("[Nimble Join]: Time taken {0} ms.", testResults.totalExecutionTime);
                testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
                testResults.totalNumberOfOutput = intermediateHashTable.Count;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }


        public void Query_3_1(string selectivityRatio)
        {
            try
            {
                outputRecordsCounter = 0;
                var dateHashTable = new Dictionary<int, string>();
                var customerHashTable = new Dictionary<int, string>();
                var supplierHashTable = new Dictionary<int, string>();
                Stopwatch sw = new Stopwatch();
                Stopwatch swInitialRecorder = new Stopwatch();
                Stopwatch swOutputRecorder = new Stopwatch();

                #region Key Hashing Phase
                List<Date> dateDimension = null;
                List<Supplier> supplierDimension = null;
                List<Customer> customerDimension = null;
                swInitialRecorder.Start();
                swOutputRecorder.Start();
                switch (selectivityRatio)
                {
                    case "0.007":
                        sw.Start();
                        Parallel.Invoke(parallelOptions, () => dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor)),
                        () => customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor)),
                        () => supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor))
                             );
                        sw.Stop();
                        testResults.phase11IOTime = sw.ElapsedMilliseconds;
                        sw.Reset();

                        sw.Start();
                        Parallel.Invoke(parallelOptions, () =>
                        {
                            foreach (var row in dateDimension)
                            {
                                if (row.dYear.Equals("1992"))
                                    dateHashTable.Add(row.dDateKey, row.dYear);
                            }
                        },
                        () =>
                        {
                            foreach (var row in customerDimension)
                            {
                                if (row.cRegion.Equals("ASIA"))
                                    customerHashTable.Add(row.cCustKey, row.cNation);
                            }
                        },
                        () =>
                        {
                            foreach (var row in supplierDimension)
                            {
                                if (row.sRegion.Equals("ASIA"))
                                    supplierHashTable.Add(row.sSuppKey, row.sNation);
                            }
                        });
                        sw.Stop();
                        break;
                    case "0.07":
                        sw.Start();

                        Parallel.Invoke(parallelOptions, () => dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor)),
                        () => customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor)),
                        () => supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor))
                             );
                        sw.Stop();
                        testResults.phase11IOTime = sw.ElapsedMilliseconds;
                        sw.Reset();

                        sw.Start();
                        Parallel.Invoke(parallelOptions, () =>
                        {
                            foreach (var row in dateDimension)
                            {
                                if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1996") <= 0)
                                    dateHashTable.Add(row.dDateKey, row.dYear);
                            }
                        },
                        () =>
                        {
                            foreach (var row in customerDimension)
                            {
                                if (row.cRegion.Equals("ASIA") || row.cRegion.Equals("AFRICA"))
                                    customerHashTable.Add(row.cCustKey, row.cNation);
                            }
                        },
                        () =>
                        {
                            foreach (var row in supplierDimension)
                            {
                                if (row.sRegion.Equals("ASIA"))
                                    supplierHashTable.Add(row.sSuppKey, row.sNation);
                            }
                        });
                        sw.Stop();
                        break;
                    case "0.7":
                        sw.Start();

                        Parallel.Invoke(parallelOptions, () => dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor)),
                        () => customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor)),
                        () => supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor))
                             );
                        sw.Stop();
                        testResults.phase11IOTime = sw.ElapsedMilliseconds;
                        sw.Reset();

                        sw.Start();
                        Parallel.Invoke(parallelOptions, () =>
                        {
                            foreach (var row in dateDimension)
                            {
                                if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1999") <= 0)
                                    dateHashTable.Add(row.dDateKey, row.dYear);
                            }
                        },
                        () =>
                        {
                            foreach (var row in customerDimension)
                            {
                                if (row.cRegion.Equals("ASIA") || row.cRegion.Equals("AMERICA") || row.cRegion.Equals("EUROPE") || row.cRegion.Equals("AFRICA"))
                                    customerHashTable.Add(row.cCustKey, row.cNation);
                            }
                        },
                        () =>
                        {
                            foreach (var row in supplierDimension)
                            {
                                if (row.sRegion.Equals("ASIA") || row.sRegion.Equals("AMERICA") || row.sRegion.Equals("EUROPE") || row.sRegion.Equals("AFRICA"))
                                    supplierHashTable.Add(row.sSuppKey, row.sNation);
                            }
                        });
                        sw.Stop();
                        break;
                }
                testResults.phase11HashTime = sw.ElapsedMilliseconds;
                testResults.phase1Time = testResults.phase11HashTime + testResults.phase11IOTime;
                sw.Reset();

                customerDimension.Clear();
                dateDimension.Clear();
                supplierDimension.Clear();

                #endregion Key Hashing Phase

                #region Probing Phase

                sw.Start();
                List<int> loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase21IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var intermediateHashTable = new Dictionary<int, string>();
                var i = 0;
                foreach (var orderDate in loOrderDate)
                {
                    string dYear = "";
                    if (dateHashTable.TryGetValue(orderDate, out dYear))
                    {
                        intermediateHashTable.Add(i, dYear);
                    }
                    i++;
                }
                sw.Stop();
                testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<int> loCustomerKey = Utils.ReadFromBinaryFiles<int>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase22IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var j = 0;
                foreach (var custKey in loCustomerKey)
                {
                    string cNation = string.Empty;
                    if (customerHashTable.TryGetValue(custKey, out cNation))
                    {
                        string values = string.Empty;
                        if (intermediateHashTable.TryGetValue(j, out values))
                        {
                            intermediateHashTable[j] = values + ", " + cNation;
                        }
                    }
                    else
                    {
                        intermediateHashTable.Remove(j);
                    }
                    j++;
                }
                sw.Stop();
                testResults.phase22ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<int> loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase23IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var k = 0;
                foreach (var suppKey in loSupplierKey)
                {
                    string sNation = string.Empty;
                    if (supplierHashTable.TryGetValue(suppKey, out sNation))
                    {
                        string values = string.Empty;
                        if (intermediateHashTable.TryGetValue(k, out values))
                        {
                            intermediateHashTable[k] = values + ", " + sNation;
                            if (isFirst)
                            {
                                swInitialRecorder.Stop();
                                testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
                                isFirst = false;
                            }
                            //Console.WriteLine(k +", "+ values.ToString() + ", " + sNation);
                            outputRecordsCounter++;
                            if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
                            {
                                sw.Stop();
                                testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, sw.ElapsedTicks));
                                sw.Start();
                            }
                        }
                    }
                    else
                    {
                        intermediateHashTable.Remove(k);
                    }
                    k++;
                }
                sw.Stop();
                testResults.phase23ProbeTime = sw.ElapsedMilliseconds;
                testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime +
                    testResults.phase22IOTime + testResults.phase22ProbeTime +
                    testResults.phase23IOTime + testResults.phase23ProbeTime;
                sw.Reset();

                loOrderDate.Clear();
                loCustomerKey.Clear();
                loSupplierKey.Clear();
                #endregion Probing Phase

                #region Value Extraction Phase

                sw.Start();
                List<int> loRevenue = Utils.ReadFromBinaryFiles<int>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase3IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var joinOutputFinal = new Dictionary<int, string>();
                foreach (var item in intermediateHashTable)
                {
                    joinOutputFinal.Add(item.Key, item.Value + ", " + loRevenue[item.Key]); // Direct array lookup
                }
                sw.Stop();
                testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
                testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;

                #endregion Value Extraction Phase
                testResults.totalNumberOfOutput = joinOutputFinal.Count;
                testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
                // Console.WriteLine("[Nimble Join]: Time taken {0} ms.", sw.ElapsedMilliseconds);

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
        }
        public void Query_3_2()
        {
            try
            {
                long memoryStartPhase1 = GC.GetTotalMemory(true);
                outputRecordsCounter = 0;
                var dateHashTable = new Dictionary<int, string>();
                var customerHashTable = new Dictionary<int, string>();
                var supplierHashTable = new Dictionary<int, string>();
                Stopwatch sw = new Stopwatch();
                Stopwatch swInitialRecorder = new Stopwatch();
                Stopwatch swOutputRecorder = new Stopwatch();

                #region Key Hashing Phase
                List<Date> dateDimension = null;
                List<Supplier> supplierDimension = null;
                List<Customer> customerDimension = null;

                swInitialRecorder.Start();
                swOutputRecorder.Start();
                sw.Start();

                Parallel.Invoke(parallelOptions, () => dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor)),
                () => customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor)),
                () => supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor))
                     );
                sw.Stop();
                testResults.phase11IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                Parallel.Invoke(parallelOptions, () =>
                {
                    foreach (var row in dateDimension)
                    {
                        if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
                            dateHashTable.Add(row.dDateKey, row.dYear);
                    }
                },
                () =>
                {
                    foreach (var row in customerDimension)
                    {
                        if (row.cNation.Equals("UNITED STATES"))
                            customerHashTable.Add(row.cCustKey, row.cCity);
                    }
                },
                () =>
                {
                    foreach (var row in supplierDimension)
                    {
                        if (row.sNation.Equals("UNITED STATES"))
                            supplierHashTable.Add(row.sSuppKey, row.sCity);
                    }
                });
                sw.Stop();
                testResults.phase11HashTime = sw.ElapsedMilliseconds;
                testResults.phase1Time = testResults.phase11HashTime + testResults.phase11IOTime;
                sw.Reset();

                customerDimension.Clear();
                dateDimension.Clear();
                supplierDimension.Clear();

                long memoryUsedPhase1 = GC.GetTotalMemory(true) - memoryStartPhase1;
                #endregion Key Hashing Phase

                #region Probing Phase
                long memoryStartPhase2 = GC.GetTotalMemory(true);
                sw.Start();
                List<int> loOrderDate = null;
                List<int> loCustomerKey = null;
                List<int> loSupplierKey = null;
                Parallel.Invoke(parallelOptions, () => loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor)),
                  () => loCustomerKey = Utils.ReadFromBinaryFiles<int>(loCustKeyFile.Replace("BF", "BF" + scaleFactor)),
                  () => loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor)));
                sw.Stop();
                testResults.phase21IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var intermediateHashTable = new Dictionary<int, string>();
                var i = 0;
                foreach (var suppKey in loSupplierKey)
                {
                    string sCity = string.Empty;
                    if (supplierHashTable.TryGetValue(suppKey, out sCity))
                    {
                        intermediateHashTable.Add(i, sCity);
                    }
                    i++;
                }

                var k = 0;
                foreach (var orderDate in loOrderDate)
                {
                    string dYear = string.Empty;
                    if (dateHashTable.TryGetValue(orderDate, out dYear))
                    {
                        string values = string.Empty;
                        if (intermediateHashTable.TryGetValue(k, out values))
                        {
                            intermediateHashTable[k] = values + ", " + dYear;
                        }
                    }
                    else
                    {
                        intermediateHashTable.Remove(k);
                    }
                    k++;
                }

                var j = 0;
                foreach (var custKey in loCustomerKey)
                {
                    string cCity = string.Empty;
                    if (customerHashTable.TryGetValue(custKey, out cCity))
                    {
                        string values = string.Empty;
                        if (intermediateHashTable.TryGetValue(j, out values))
                        {
                            intermediateHashTable[j] = values + ", " + cCity;
                            if (isFirst)
                            {
                                swInitialRecorder.Stop();
                                testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
                                isFirst = false;
                            }
                            //Console.WriteLine(k +", "+ values.ToString() + ", " + sNation);

                            outputRecordsCounter++;
                            if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
                            {
                                swOutputRecorder.Stop();
                                testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
                                swOutputRecorder.Start();
                            }
                        }
                    }
                    else
                    {
                        intermediateHashTable.Remove(j);
                    }
                    j++;
                }

                sw.Stop();
                testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
                testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime;
                sw.Reset();

                loOrderDate.Clear();
                loCustomerKey.Clear();
                loSupplierKey.Clear();

                long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
                #endregion Probing Phase

                #region Value Extraction Phase
                long memoryStartPhase3 = GC.GetTotalMemory(true);
                sw.Start();
                List<int> loRevenue = Utils.ReadFromBinaryFiles<int>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase3IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var joinOutputFinal = new Dictionary<int, string>();
                foreach (var item in intermediateHashTable)
                {
                    joinOutputFinal.Add(item.Key, item.Value + ", " + loRevenue[item.Key]); // Direct array lookup
                }
                sw.Stop();

                long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
                #endregion Value Extraction Phase
                testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
                testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
                testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
                // Console.WriteLine("[Nimble Join]: Time taken {0} ms.", testResults.totalExecutionTime);
                testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
                testResults.totalNumberOfOutput = intermediateHashTable.Count;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public void Query_3_3()
        {
            try
            {
                long memoryStartPhase1 = GC.GetTotalMemory(true);
                outputRecordsCounter = 0;
                var dateHashTable = new Dictionary<int, string>();
                var customerHashTable = new Dictionary<int, string>();
                var supplierHashTable = new Dictionary<int, string>();
                Stopwatch sw = new Stopwatch();
                Stopwatch swInitialRecorder = new Stopwatch();
                Stopwatch swOutputRecorder = new Stopwatch();

                #region Key Hashing Phase
                List<Date> dateDimension = null;
                List<Supplier> supplierDimension = null;
                List<Customer> customerDimension = null;

                swInitialRecorder.Start();
                swOutputRecorder.Start();
                sw.Start();

                Parallel.Invoke(parallelOptions, () => dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor)),
                () => customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor)),
                () => supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor))
                     );
                sw.Stop();
                testResults.phase11IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                Parallel.Invoke(parallelOptions, () =>
                {
                    foreach (var row in dateDimension)
                    {
                        if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
                            dateHashTable.Add(row.dDateKey, row.dYear);
                    }
                },
                () =>
                {
                    foreach (var row in customerDimension)
                    {
                        if (row.cCity.Equals("UNITED KI1") || row.cCity.Equals("UNITED KI5"))
                            customerHashTable.Add(row.cCustKey, row.cCity);
                    }
                },
                () =>
                {
                    foreach (var row in supplierDimension)
                    {
                        if (row.sCity.Equals("UNITED KI1") || row.sCity.Equals("UNITED KI5"))
                            supplierHashTable.Add(row.sSuppKey, row.sCity);
                    }
                });
                sw.Stop();
                testResults.phase11HashTime = sw.ElapsedMilliseconds;
                testResults.phase1Time = testResults.phase11HashTime + testResults.phase11IOTime;
                sw.Reset();

                customerDimension.Clear();
                dateDimension.Clear();
                supplierDimension.Clear();

                long memoryUsedPhase1 = GC.GetTotalMemory(true) - memoryStartPhase1;
                #endregion Key Hashing Phase

                #region Probing Phase
                long memoryStartPhase2 = GC.GetTotalMemory(true);
                sw.Start();
                List<int> loOrderDate = null;
                List<int> loCustomerKey = null;
                List<int> loSupplierKey = null;
                Parallel.Invoke(parallelOptions, () => loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor)),
                  () => loCustomerKey = Utils.ReadFromBinaryFiles<int>(loCustKeyFile.Replace("BF", "BF" + scaleFactor)),
                  () => loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor)));
                sw.Stop();
                testResults.phase21IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var intermediateHashTable = new Dictionary<int, string>();
                var i = 0;
                foreach (var suppKey in loSupplierKey)
                {
                    string sCity = string.Empty;
                    if (supplierHashTable.TryGetValue(suppKey, out sCity))
                    {
                        intermediateHashTable.Add(i, sCity);
                    }
                    i++;
                }

                var k = 0;
                foreach (var orderDate in loOrderDate)
                {
                    string dYear = string.Empty;
                    if (dateHashTable.TryGetValue(orderDate, out dYear))
                    {
                        string values = string.Empty;
                        if (intermediateHashTable.TryGetValue(k, out values))
                        {
                            intermediateHashTable[k] = values + ", " + dYear;
                        }
                    }
                    else
                    {
                        intermediateHashTable.Remove(k);
                    }
                    k++;
                }

                var j = 0;
                foreach (var custKey in loCustomerKey)
                {
                    string cCity = string.Empty;
                    if (customerHashTable.TryGetValue(custKey, out cCity))
                    {
                        string values = string.Empty;
                        if (intermediateHashTable.TryGetValue(j, out values))
                        {
                            intermediateHashTable[j] = values + ", " + cCity;
                            if (isFirst)
                            {
                                swInitialRecorder.Stop();
                                testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
                                isFirst = false;
                            }
                            //Console.WriteLine(k +", "+ values.ToString() + ", " + sNation);

                            outputRecordsCounter++;
                            if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
                            {
                                swOutputRecorder.Stop();
                                testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
                                swOutputRecorder.Start();
                            }
                        }
                    }
                    else
                    {
                        intermediateHashTable.Remove(j);
                    }
                    j++;
                }

                sw.Stop();
                testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
                testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime;
                sw.Reset();

                loOrderDate.Clear();
                loCustomerKey.Clear();
                loSupplierKey.Clear();

                long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
                #endregion Probing Phase

                #region Value Extraction Phase
                long memoryStartPhase3 = GC.GetTotalMemory(true);
                sw.Start();
                List<int> loRevenue = Utils.ReadFromBinaryFiles<int>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase3IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var joinOutputFinal = new Dictionary<int, string>();
                foreach (var item in intermediateHashTable)
                {
                    joinOutputFinal.Add(item.Key, item.Value + ", " + loRevenue[item.Key]); // Direct array lookup
                }
                sw.Stop();

                long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
                #endregion Value Extraction Phase
                testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
                testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
                testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
                // Console.WriteLine("[Nimble Join]: Time taken {0} ms.", testResults.totalExecutionTime);
                testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
                testResults.totalNumberOfOutput = intermediateHashTable.Count;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void Query_3_4()
        {
            try
            {
                long memoryStartPhase1 = GC.GetTotalMemory(true);
                outputRecordsCounter = 0;
                var dateHashTable = new Dictionary<int, string>();
                var customerHashTable = new Dictionary<int, string>();
                var supplierHashTable = new Dictionary<int, string>();
                Stopwatch sw = new Stopwatch();
                Stopwatch swInitialRecorder = new Stopwatch();
                Stopwatch swOutputRecorder = new Stopwatch();

                #region Key Hashing Phase
                List<Date> dateDimension = null;
                List<Supplier> supplierDimension = null;
                List<Customer> customerDimension = null;

                swInitialRecorder.Start();
                swOutputRecorder.Start();
                sw.Start();

                Parallel.Invoke(parallelOptions, () => dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor)),
                () => customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor)),
                () => supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor))
                     );
                sw.Stop();
                testResults.phase11IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                Parallel.Invoke(parallelOptions, () =>
                {
                    foreach (var row in dateDimension)
                    {
                        if (row.dYearMonth.Equals("Dec1997"))
                            dateHashTable.Add(row.dDateKey, row.dYear);
                    }
                },
                () =>
                {
                    foreach (var row in customerDimension)
                    {
                        if (row.cCity.Equals("UNITED KI1") || row.cCity.Equals("UNITED KI5"))
                            customerHashTable.Add(row.cCustKey, row.cCity);
                    }
                },
                () =>
                {
                    foreach (var row in supplierDimension)
                    {
                        if (row.sCity.Equals("UNITED KI1") || row.sCity.Equals("UNITED KI5"))
                            supplierHashTable.Add(row.sSuppKey, row.sCity);
                    }
                });
                sw.Stop();
                testResults.phase11HashTime = sw.ElapsedMilliseconds;
                testResults.phase1Time = testResults.phase11HashTime + testResults.phase11IOTime;
                sw.Reset();

                customerDimension.Clear();
                dateDimension.Clear();
                supplierDimension.Clear();

                long memoryUsedPhase1 = GC.GetTotalMemory(true) - memoryStartPhase1;
                #endregion Key Hashing Phase

                #region Probing Phase
                long memoryStartPhase2 = GC.GetTotalMemory(true);
                sw.Start();
                List<int> loOrderDate = null;
                List<int> loCustomerKey = null;
                List<int> loSupplierKey = null;
                Parallel.Invoke(parallelOptions, () => loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor)),
                  () => loCustomerKey = Utils.ReadFromBinaryFiles<int>(loCustKeyFile.Replace("BF", "BF" + scaleFactor)),
                  () => loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor)));
                sw.Stop();
                testResults.phase21IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var intermediateHashTable = new Dictionary<int, string>();
                var i = 0;
                foreach (var suppKey in loSupplierKey)
                {
                    string sCity = string.Empty;
                    if (supplierHashTable.TryGetValue(suppKey, out sCity))
                    {
                        intermediateHashTable.Add(i, sCity);
                    }
                    i++;
                }

                var k = 0;
                foreach (var orderDate in loOrderDate)
                {
                    string dYear = string.Empty;
                    if (dateHashTable.TryGetValue(orderDate, out dYear))
                    {
                        string values = string.Empty;
                        if (intermediateHashTable.TryGetValue(k, out values))
                        {
                            intermediateHashTable[k] = values + ", " + dYear;
                        }
                    }
                    else
                    {
                        intermediateHashTable.Remove(k);
                    }
                    k++;
                }

                var j = 0;
                foreach (var custKey in loCustomerKey)
                {
                    string cCity = string.Empty;
                    if (customerHashTable.TryGetValue(custKey, out cCity))
                    {
                        string values = string.Empty;
                        if (intermediateHashTable.TryGetValue(j, out values))
                        {
                            intermediateHashTable[j] = values + ", " + cCity;
                            if (isFirst)
                            {
                                swInitialRecorder.Stop();
                                testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
                                isFirst = false;
                            }
                            //Console.WriteLine(k +", "+ values.ToString() + ", " + sNation);

                            outputRecordsCounter++;
                            if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
                            {
                                swOutputRecorder.Stop();
                                testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
                                swOutputRecorder.Start();
                            }
                        }
                    }
                    else
                    {
                        intermediateHashTable.Remove(j);
                    }
                    j++;
                }

                sw.Stop();
                testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
                testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime;
                sw.Reset();

                loOrderDate.Clear();
                loCustomerKey.Clear();
                loSupplierKey.Clear();

                long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
                #endregion Probing Phase

                #region Value Extraction Phase
                long memoryStartPhase3 = GC.GetTotalMemory(true);
                sw.Start();
                List<int> loRevenue = Utils.ReadFromBinaryFiles<int>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase3IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var joinOutputFinal = new Dictionary<int, string>();
                foreach (var item in intermediateHashTable)
                {
                    joinOutputFinal.Add(item.Key, item.Value + ", " + loRevenue[item.Key]); // Direct array lookup
                }
                sw.Stop();

                long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
                #endregion Value Extraction Phase
                testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
                testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
                testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
                // Console.WriteLine("[Nimble Join]: Time taken {0} ms.", testResults.totalExecutionTime);
                testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
                testResults.totalNumberOfOutput = intermediateHashTable.Count;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void Query_2_1_IM()
        {
            try
            {
                Stopwatch sw = new Stopwatch();

                List<Part> partDimension = Utils.ReadFromBinaryFiles<Part>(partFile.Replace("BF", "BF" + scaleFactor));
                List<Supplier> supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
                List<Date> dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                List<int> loPartKey = Utils.ReadFromBinaryFiles<int>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                List<int> loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                List<int> loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                List<int> loRevenue = Utils.ReadFromBinaryFiles<int>(loRevenueFile.Replace("BF", "BF" + scaleFactor));

                sw.Start();
                #region Key Hashing Phase 

                var partHashTable = new Dictionary<int, string>();
                var supplierHashTable = new Dictionary<int, string>();
                var dateHashTable = new Dictionary<int, string>();

                Parallel.Invoke(parallelOptions,
                () =>
                {
                    foreach (var row in dateDimension)
                    {
                        dateHashTable.Add(row.dDateKey, row.dYear);
                    }
                },
                () =>
                {
                    foreach (var row in partDimension)
                    {
                        if (row.pCategory.Equals("MFGR#12"))
                            partHashTable.Add(row.pPartKey, row.pBrand);
                    }
                },
                () =>
                {
                    foreach (var row in supplierDimension)
                    {
                        if (row.sRegion.Equals("AMERICA"))
                            supplierHashTable.Add(row.sSuppKey, row.sNation);
                    }
                });

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNimble Join] T0 Time: {0}", t0));
                sw.Reset();

                #endregion Key Hashing Phase

                var _maat = new MAATIM(loSupplierKey.Count);
                var partitionIndexes = Utils.getPartitionIndexes(loSupplierKey.Count, parallelOptions.MaxDegreeOfParallelism);
                #region Probing Phase
                sw.Start();

                List<Task> tasks = new List<Task>();
                foreach (var indexes in partitionIndexes)
                {
                    Task t = Task.Factory.StartNew(() =>
                    {
                        for (int i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            int partKey = loPartKey[i];
                            int dateKey = loOrderDate[i];
                            int suppKey = loSupplierKey[i];
                            string pBrand = string.Empty;
                            string dYear = string.Empty;
                            if (partHashTable.TryGetValue(partKey, out pBrand)
                            && dateHashTable.TryGetValue(dateKey, out dYear)
                             && supplierHashTable.ContainsKey(suppKey))
                            {
                                _maat.AddOrUpdate(i, new List<object> { dYear, pBrand, loRevenue[i] });
                            }
                        }
                    });
                    tasks.Add(t);
                }

                Task.WaitAll(tasks.ToArray());

                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNimble Join] T1 Time: {0}", t1));
                sw.Reset();

                #endregion Probing Phase
                #region Value Extraction Phase
                sw.Start();

                var joinOutputFinal = new Dictionary<string, int>();
                int index = 0;
                foreach (var item in _maat.GetAll())
                {
                    if (item != null)
                    {
                        string key = item[0] + ", " + item[1];
                        int revenue = 0;
                        if (joinOutputFinal.TryGetValue(key, out revenue))
                        {
                            joinOutputFinal[key] = revenue + Convert.ToInt32(item[2]);
                        }
                        else
                        {
                            joinOutputFinal.Add(key, Convert.ToInt32(item[2]));
                        }
                    }
                    index++;
                }

                sw.Stop();
                long t2 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNimble Join] T2 Time: {0}", t2));
                Console.WriteLine(String.Format("[PNimble Join] Total Time: {0}", t0 + t1 + t2));
                Console.WriteLine(String.Format("[PNimble Join] Total Count: {0}", joinOutputFinal.Count()));
                Console.WriteLine();
                #endregion Value Extraction Phase
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void Query_2_2_IM()
        {
            try
            {
                Stopwatch sw = new Stopwatch();

                List<Part> partDimension = Utils.ReadFromBinaryFiles<Part>(partFile.Replace("BF", "BF" + scaleFactor));
                List<Supplier> supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
                List<Date> dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                List<int> loPartKey = Utils.ReadFromBinaryFiles<int>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                List<int> loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                List<int> loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                List<int> loRevenue = Utils.ReadFromBinaryFiles<int>(loRevenueFile.Replace("BF", "BF" + scaleFactor));

                sw.Start();
                #region Key Hashing Phase 

                var partHashTable = new Dictionary<int, string>();
                var supplierHashTable = new Dictionary<int, string>();
                var dateHashTable = new Dictionary<int, string>();

                Parallel.Invoke(parallelOptions,
               () =>
               {
                   foreach (var row in dateDimension)
                   {
                       dateHashTable.Add(row.dDateKey, row.dYear);
                   }
               },
               () =>
               {
                   foreach (var row in partDimension)
                   {
                       if (String.CompareOrdinal(row.pBrand, "MFGR#2221") >= 0 && String.CompareOrdinal(row.pBrand, "MFGR#2228") <= 0)
                           partHashTable.Add(row.pPartKey, row.pBrand);
                   }
               },
               () =>
               {
                   foreach (var row in supplierDimension)
                   {
                       if (row.sRegion.Equals("ASIA"))
                           supplierHashTable.Add(row.sSuppKey, row.sNation);
                   }
               });

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNimble Join] T0 Time: {0}", t0));
                sw.Reset();

                #endregion Key Hashing Phase

                var _maat = new MAATIM(loSupplierKey.Count);
                var partitionIndexes = Utils.getPartitionIndexes(loSupplierKey.Count, parallelOptions.MaxDegreeOfParallelism);
                #region Probing Phase
                sw.Start();

                List<Task> tasks = new List<Task>();
                foreach (var indexes in partitionIndexes)
                {
                    Task t = Task.Factory.StartNew(() =>
                    {
                        for (int i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            int partKey = loPartKey[i];
                            int dateKey = loOrderDate[i];
                            int suppKey = loSupplierKey[i];
                            string pBrand = string.Empty;
                            string dYear = string.Empty;
                            if (partHashTable.TryGetValue(partKey, out pBrand)
                            && dateHashTable.TryGetValue(dateKey, out dYear)
                             && supplierHashTable.ContainsKey(suppKey))
                            {
                                _maat.AddOrUpdate(i, new List<object> { dYear, pBrand, loRevenue[i] });
                            }
                        }
                    });
                    tasks.Add(t);
                }

                Task.WaitAll(tasks.ToArray());

                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNimble Join] T1 Time: {0}", t1));
                sw.Reset();

                #endregion Probing Phase
                #region Value Extraction Phase
                sw.Start();

                var joinOutputFinal = new Dictionary<string, int>();
                int index = 0;
                foreach (var item in _maat.GetAll())
                {
                    if (item != null)
                    {
                        string key = item[0] + ", " + item[1];
                        int revenue = 0;
                        if (joinOutputFinal.TryGetValue(key, out revenue))
                        {
                            joinOutputFinal[key] = revenue + Convert.ToInt32(item[2]);
                        }
                        else
                        {
                            joinOutputFinal.Add(key, Convert.ToInt32(item[2]));
                        }
                    }
                    index++;
                }

                sw.Stop();
                long t2 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNimble Join] T2 Time: {0}", t2));
                Console.WriteLine(String.Format("[PNimble Join] Total Time: {0}", t0 + t1 + t2));
                Console.WriteLine(String.Format("[PNimble Join] Total Count: {0}", joinOutputFinal.Count()));
                Console.WriteLine();
                #endregion Value Extraction Phase
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void Query_2_3_IM()
        {
            try
            {
                Stopwatch sw = new Stopwatch();

                List<Part> partDimension = Utils.ReadFromBinaryFiles<Part>(partFile.Replace("BF", "BF" + scaleFactor));
                List<Supplier> supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
                List<Date> dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                List<int> loPartKey = Utils.ReadFromBinaryFiles<int>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                List<int> loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                List<int> loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                List<int> loRevenue = Utils.ReadFromBinaryFiles<int>(loRevenueFile.Replace("BF", "BF" + scaleFactor));

                sw.Start();
                #region Key Hashing Phase 

                var partHashTable = new Dictionary<int, string>();
                var supplierHashTable = new Dictionary<int, string>();
                var dateHashTable = new Dictionary<int, string>();

                Parallel.Invoke(parallelOptions, () =>
                {
                    foreach (var row in dateDimension)
                    {
                        dateHashTable.Add(row.dDateKey, row.dYear);
                    }
                },
                     () =>
                     {
                         foreach (var row in partDimension)
                         {
                             if (row.pBrand.Equals("MFGR#2221"))
                                 partHashTable.Add(row.pPartKey, row.pBrand);
                         }
                     },
                     () =>
                     {
                         foreach (var row in supplierDimension)
                         {
                             if (row.sRegion.Equals("EUROPE"))
                                 supplierHashTable.Add(row.sSuppKey, row.sNation);
                         }
                     });

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNimble Join] T0 Time: {0}", t0));
                sw.Reset();

                #endregion Key Hashing Phase

                var _maat = new MAATIM(loSupplierKey.Count);
                var partitionIndexes = Utils.getPartitionIndexes(loSupplierKey.Count, parallelOptions.MaxDegreeOfParallelism);
                #region Probing Phase
                sw.Start();

                List<Task> tasks = new List<Task>();
                foreach (var indexes in partitionIndexes)
                {
                    Task t = Task.Factory.StartNew(() =>
                    {
                        for (int i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            int partKey = loPartKey[i];
                            int dateKey = loOrderDate[i];
                            int suppKey = loSupplierKey[i];
                            string pBrand = string.Empty;
                            string dYear = string.Empty;
                            if (partHashTable.TryGetValue(partKey, out pBrand)
                            && dateHashTable.TryGetValue(dateKey, out dYear)
                             && supplierHashTable.ContainsKey(suppKey))
                            {
                                _maat.AddOrUpdate(i, new List<object> { dYear, pBrand, loRevenue[i] });
                            }
                        }
                    });
                    tasks.Add(t);
                }

                Task.WaitAll(tasks.ToArray());

                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNimble Join] T1 Time: {0}", t1));
                sw.Reset();

                #endregion Probing Phase
                #region Value Extraction Phase
                sw.Start();

                var joinOutputFinal = new Dictionary<string, int>();
                int index = 0;
                foreach (var item in _maat.GetAll())
                {
                    if (item != null)
                    {
                        string key = item[0] + ", " + item[1];
                        int revenue = 0;
                        if (joinOutputFinal.TryGetValue(key, out revenue))
                        {
                            joinOutputFinal[key] = revenue + Convert.ToInt32(item[2]);
                        }
                        else
                        {
                            joinOutputFinal.Add(key, Convert.ToInt32(item[2]));
                        }
                    }
                    index++;
                }

                sw.Stop();
                long t2 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNimble Join] T2 Time: {0}", t2));
                Console.WriteLine(String.Format("[PNimble Join] Total Time: {0}", t0 + t1 + t2));
                Console.WriteLine(String.Format("[PNimble Join] Total Count: {0}", joinOutputFinal.Count()));
                Console.WriteLine();
                #endregion Value Extraction Phase
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        /// <summary>
        /// IM refers Inmemory
        /// </summary>
        public void Query_3_1_IM()
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
                #region Key Hashing Phase 

                var customerHashTable = new Dictionary<int, string>();
                var supplierHashTable = new Dictionary<int, string>();
                var dateHashTable = new Dictionary<int, string>();

                Parallel.Invoke(parallelOptions,
                () =>
                {
                    foreach (var row in dateDimension)
                    {
                        if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
                            dateHashTable.Add(row.dDateKey, row.dYear);
                    }
                },
                () =>
                {
                    foreach (var row in customerDimension)
                    {
                        if (row.cRegion.Equals("ASIA"))
                            customerHashTable.Add(row.cCustKey, row.cNation);
                    }
                },
                () =>
                {
                    foreach (var row in supplierDimension)
                    {
                        if (row.sRegion.Equals("ASIA"))
                            supplierHashTable.Add(row.sSuppKey, row.sNation);
                    }
                });

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNimble Join] T0 Time: {0}", t0));
                sw.Reset();
                #endregion Key Hashing Phase

                var _maat = new MAATIM(loSupplierKey.Count);
                var partitionIndexes = Utils.getPartitionIndexes(loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);
                #region Probing Phase
                sw.Start();

                List<Task> tasks = new List<Task>();
                foreach (var indexes in partitionIndexes)
                {
                    Task t = Task.Factory.StartNew(() =>
                    {
                        for (int i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            int custKey = loCustomerKey[i];
                            int suppKey = loSupplierKey[i];
                            int dateKey = loOrderDate[i];
                            string custNation = string.Empty;
                            string suppNation = string.Empty;
                            string dYear = string.Empty;
                            if (customerHashTable.TryGetValue(custKey, out custNation)
                            && supplierHashTable.TryGetValue(suppKey, out suppNation)
                            && dateHashTable.TryGetValue(dateKey, out dYear))
                            {
                                _maat.AddOrUpdate(i, new List<object> { custNation, suppNation, dYear, loRevenue[i] });
                            }
                        }
                    });
                    tasks.Add(t);
                }

                Task.WaitAll(tasks.ToArray());

                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNimble Join] T1 Time: {0}", t1));
                sw.Reset();

                #endregion Probing Phase

                #region Value Extraction Phase
                sw.Start();

                var joinOutputFinal = new Dictionary<string, int>();
                int index = 0;
                foreach (var item in _maat.GetAll())
                {
                    if (item != null)
                    {
                        string key = item[0] + ", " + item[1] + ", " + item[2];
                        int revenue = 0;
                        if (joinOutputFinal.TryGetValue(key, out revenue))
                        {
                            joinOutputFinal[key] = revenue + Convert.ToInt32(item[3]);
                        }
                        else
                        {
                            joinOutputFinal.Add(key, Convert.ToInt32(item[3]));
                        }
                    }
                    index++;
                }

                sw.Stop();
                long t2 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNimble Join] T2 Time: {0}", t2));
                Console.WriteLine(String.Format("[PNimble Join] Total Time: {0}", t0 + t1 + t2));
                Console.WriteLine(String.Format("[PNimble Join] Total Count: {0}", joinOutputFinal.Count()));
                Console.WriteLine();
                #endregion Value Extraction Phase
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// IM refers Inmemory
        /// </summary>
        public void Query_3_2_IM()
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
                #region Key Hashing Phase 

                var customerHashTable = new Dictionary<int, string>();
                var supplierHashTable = new Dictionary<int, string>();
                var dateHashTable = new Dictionary<int, string>();

                Parallel.Invoke(parallelOptions,
                () =>
                {
                    foreach (var row in dateDimension)
                    {
                        if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
                            dateHashTable.Add(row.dDateKey, row.dYear);
                    }
                },
                () =>
                {
                    foreach (var row in customerDimension)
                    {
                        if (row.cNation.Equals("UNITED STATES"))
                            customerHashTable.Add(row.cCustKey, row.cCity);
                    }
                },
                () =>
                {
                    foreach (var row in supplierDimension)
                    {
                        if (row.sNation.Equals("UNITED STATES"))
                            supplierHashTable.Add(row.sSuppKey, row.sCity);
                    }
                });

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNimble Join] T0 Time: {0}", t0));
                sw.Reset();
                #endregion Key Hashing Phase

                var _maat = new MAATIM(loSupplierKey.Count);
                var partitionIndexes = Utils.getPartitionIndexes(loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);
                #region Probing Phase
                sw.Start();

                List<Task> tasks = new List<Task>();
                foreach (var indexes in partitionIndexes)
                {
                    Task t = Task.Factory.StartNew(() =>
                    {
                        for (int i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            int custKey = loCustomerKey[i];
                            int suppKey = loSupplierKey[i];
                            int dateKey = loOrderDate[i];
                            string custCity = string.Empty;
                            string suppCity = string.Empty;
                            string dYear = string.Empty;
                            if (customerHashTable.TryGetValue(custKey, out custCity)
                            && supplierHashTable.TryGetValue(suppKey, out suppCity)
                            && dateHashTable.TryGetValue(dateKey, out dYear))
                            {
                                _maat.AddOrUpdate(i, new List<object> { custCity, suppCity, dYear, loRevenue[i] });
                            }
                        }
                    });
                    tasks.Add(t);
                }

                Task.WaitAll(tasks.ToArray());

                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNimble Join] T1 Time: {0}", t1));
                sw.Reset();

                #endregion Probing Phase

                #region Value Extraction Phase
                sw.Start();

                var joinOutputFinal = new Dictionary<string, int>();
                int index = 0;
                foreach (var item in _maat.GetAll())
                {
                    if (item != null)
                    {
                        string key = item[0] + ", " + item[1] + ", " + item[2];
                        int revenue = 0;
                        if (joinOutputFinal.TryGetValue(key, out revenue))
                        {
                            joinOutputFinal[key] = revenue + Convert.ToInt32(item[3]);
                        }
                        else
                        {
                            joinOutputFinal.Add(key, Convert.ToInt32(item[3]));
                        }
                    }
                    index++;
                }

                sw.Stop();
                long t2 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNimble Join] T2 Time: {0}", t2));
                Console.WriteLine(String.Format("[PNimble Join] Total Time: {0}", t0 + t1 + t2));
                Console.WriteLine(String.Format("[PNimble Join] Total Count: {0}", joinOutputFinal.Count()));
                Console.WriteLine();
                #endregion Value Extraction Phase
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void Query_3_3_IM()
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
                #region Key Hashing Phase 

                var customerHashTable = new Dictionary<int, string>();
                var supplierHashTable = new Dictionary<int, string>();
                var dateHashTable = new Dictionary<int, string>();

                Parallel.Invoke(parallelOptions, () =>
                {
                    foreach (var row in dateDimension)
                    {
                        if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
                            dateHashTable.Add(row.dDateKey, row.dYear);
                    }
                },
                () =>
                {
                    foreach (var row in customerDimension)
                    {
                        if (row.cCity.Equals("UNITED KI1") || row.cCity.Equals("UNITED KI5"))
                            customerHashTable.Add(row.cCustKey, row.cCity);
                    }
                },
                () =>
                {
                    foreach (var row in supplierDimension)
                    {
                        if (row.sCity.Equals("UNITED KI1") || row.sCity.Equals("UNITED KI5"))
                            supplierHashTable.Add(row.sSuppKey, row.sCity);
                    }
                });

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNimble Join] T0 Time: {0}", t0));
                sw.Reset();
                #endregion Key Hashing Phase

                var _maat = new MAATIM(loSupplierKey.Count);
                var partitionIndexes = Utils.getPartitionIndexes(loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);
                #region Probing Phase
                sw.Start();

                List<Task> tasks = new List<Task>();
                foreach (var indexes in partitionIndexes)
                {
                    Task t = Task.Factory.StartNew(() =>
                    {
                        for (int i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            int custKey = loCustomerKey[i];
                            int suppKey = loSupplierKey[i];
                            int dateKey = loOrderDate[i];
                            string custCity = string.Empty;
                            string suppCity = string.Empty;
                            string dYear = string.Empty;
                            if (customerHashTable.TryGetValue(custKey, out custCity)
                            && supplierHashTable.TryGetValue(suppKey, out suppCity)
                            && dateHashTable.TryGetValue(dateKey, out dYear))
                            {
                                _maat.AddOrUpdate(i, new List<object> { custCity, suppCity, dYear, loRevenue[i] });
                            }
                        }
                    });
                    tasks.Add(t);
                }

                Task.WaitAll(tasks.ToArray());

                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNimble Join] T1 Time: {0}", t1));
                sw.Reset();

                #endregion Probing Phase

                #region Value Extraction Phase
                sw.Start();

                var joinOutputFinal = new Dictionary<string, int>();
                int index = 0;
                foreach (var item in _maat.GetAll())
                {
                    if (item != null)
                    {
                        string key = item[0] + ", " + item[1] + ", " + item[2];
                        int revenue = 0;
                        if (joinOutputFinal.TryGetValue(key, out revenue))
                        {
                            joinOutputFinal[key] = revenue + Convert.ToInt32(item[3]);
                        }
                        else
                        {
                            joinOutputFinal.Add(key, Convert.ToInt32(item[3]));
                        }
                    }
                    index++;
                }

                sw.Stop();
                long t2 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNimble Join] T2 Time: {0}", t2));
                Console.WriteLine(String.Format("[PNimble Join] Total Time: {0}", t0 + t1 + t2));
                Console.WriteLine(String.Format("[PNimble Join] Total Count: {0}", joinOutputFinal.Count()));
                Console.WriteLine();
                #endregion Value Extraction Phase
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void Query_3_4_IM()
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
                #region Key Hashing Phase 

                var customerHashTable = new Dictionary<int, string>();
                var supplierHashTable = new Dictionary<int, string>();
                var dateHashTable = new Dictionary<int, string>();

                Parallel.Invoke(parallelOptions, () =>
                {
                    foreach (var row in dateDimension)
                    {
                        if (row.dYearMonth.Equals("Dec1997"))
                            dateHashTable.Add(row.dDateKey, row.dYear);
                    }
                },
                () =>
                {
                    foreach (var row in customerDimension)
                    {
                        if (row.cCity.Equals("UNITED KI1") || row.cCity.Equals("UNITED KI5"))
                            customerHashTable.Add(row.cCustKey, row.cCity);
                    }
                },
                () =>
                {
                    foreach (var row in supplierDimension)
                    {
                        if (row.sCity.Equals("UNITED KI1") || row.sCity.Equals("UNITED KI5"))
                            supplierHashTable.Add(row.sSuppKey, row.sCity);
                    }
                });

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNimble Join] T0 Time: {0}", t0));
                sw.Reset();
                #endregion Key Hashing Phase

                var _maat = new MAATIM(loSupplierKey.Count);
                var partitionIndexes = Utils.getPartitionIndexes(loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);
                #region Probing Phase
                sw.Start();

                List<Task> tasks = new List<Task>();
                foreach (var indexes in partitionIndexes)
                {
                    Task t = Task.Factory.StartNew(() =>
                    {
                        for (int i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            int custKey = loCustomerKey[i];
                            int suppKey = loSupplierKey[i];
                            int dateKey = loOrderDate[i];
                            string custCity = string.Empty;
                            string suppCity = string.Empty;
                            string dYear = string.Empty;
                            if (customerHashTable.TryGetValue(custKey, out custCity)
                            && supplierHashTable.TryGetValue(suppKey, out suppCity)
                            && dateHashTable.TryGetValue(dateKey, out dYear))
                            {
                                _maat.AddOrUpdate(i, new List<object> { custCity, suppCity, dYear, loRevenue[i] });
                            }
                        }
                    });
                    tasks.Add(t);
                }

                Task.WaitAll(tasks.ToArray());

                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNimble Join] T1 Time: {0}", t1));
                sw.Reset();

                #endregion Probing Phase

                #region Value Extraction Phase
                sw.Start();

                var joinOutputFinal = new Dictionary<string, int>();
                int index = 0;
                foreach (var item in _maat.GetAll())
                {
                    if (item != null)
                    {
                        string key = item[0] + ", " + item[1] + ", " + item[2];
                        int revenue = 0;
                        if (joinOutputFinal.TryGetValue(key, out revenue))
                        {
                            joinOutputFinal[key] = revenue + Convert.ToInt32(item[3]);
                        }
                        else
                        {
                            joinOutputFinal.Add(key, Convert.ToInt32(item[3]));
                        }
                    }
                    index++;
                }

                sw.Stop();
                long t2 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNimble Join] T2 Time: {0}", t2));
                Console.WriteLine(String.Format("[PNimble Join] Total Time: {0}", t0 + t1 + t2));
                Console.WriteLine(String.Format("[PNimble Join] Total Count: {0}", joinOutputFinal.Count()));
                Console.WriteLine();
                #endregion Value Extraction Phase
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void Query_4_1_IM()
        {
            try
            {
                Stopwatch sw = new Stopwatch();

                List<Customer> customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
                List<Supplier> supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
                List<Date> dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                List<Part> partDimension = Utils.ReadFromBinaryFiles<Part>(partFile.Replace("BF", "BF" + scaleFactor));

                List<int> loCustomerKey = Utils.ReadFromBinaryFiles<int>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                List<int> loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                List<int> loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                List<int> loPartKey = Utils.ReadFromBinaryFiles<int>(loPartKeyFile.Replace("BF", "BF" + scaleFactor));

                List<int> loRevenue = Utils.ReadFromBinaryFiles<int>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
                List<int> loSupplyCost = Utils.ReadFromBinaryFiles<int>(loSupplyCostFile.Replace("BF", "BF" + scaleFactor));

                sw.Start();
                #region Key Hashing Phase 

                var customerHashTable = new Dictionary<int, string>();
                var supplierHashTable = new Dictionary<int, string>();
                var dateHashTable = new Dictionary<int, string>();
                var partHashTable = new Dictionary<int, string>();

                Parallel.Invoke(parallelOptions,
                () =>
                {
                    foreach (var row in dateDimension)
                    {
                        dateHashTable.Add(row.dDateKey, row.dYear);
                    }
                },
                () =>
                {
                    foreach (var row in customerDimension)
                    {
                        if (row.cRegion.Equals("AMERICA"))
                            customerHashTable.Add(row.cCustKey, row.cNation);
                    }
                },
                () =>
                {
                    foreach (var row in supplierDimension)
                    {
                        if (row.sRegion.Equals("AMERICA"))
                            supplierHashTable.Add(row.sSuppKey, row.sNation);
                    }
                },
                () =>
                {
                    foreach (var row in partDimension)
                    {
                        if (row.pMFGR.Equals("MFGR#1") || row.pMFGR.Equals("MFGR#2"))
                        {
                            partHashTable.Add(row.pPartKey, row.pMFGR);
                        }
                    }
                });

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNimble Join] T0 Time: {0}", t0));
                sw.Reset();
                #endregion Key Hashing Phase

                var _maat = new MAATIM(loSupplierKey.Count);
                var partitionIndexes = Utils.getPartitionIndexes(loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);
                #region Probing Phase
                sw.Start();

                List<Task> tasks = new List<Task>();
                foreach (var indexes in partitionIndexes)
                {
                    Task t = Task.Factory.StartNew(() =>
                    {
                        for (int i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            int custKey = loCustomerKey[i];
                            int dateKey = loOrderDate[i];
                            int suppKey = loSupplierKey[i];
                            int partKey = loPartKey[i];
                            string custNation = string.Empty;
                            string dYear = string.Empty;
                            if (customerHashTable.TryGetValue(custKey, out custNation)
                            && dateHashTable.TryGetValue(dateKey, out dYear)
                            && supplierHashTable.ContainsKey(suppKey)
                            && partHashTable.ContainsKey(partKey))
                            {
                                _maat.AddOrUpdate(i, new List<object> { dYear, custNation, (loRevenue[i] - loSupplyCost[i]) });
                            }
                        }
                    });
                    tasks.Add(t);
                }

                Task.WaitAll(tasks.ToArray());

                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNimble Join] T1 Time: {0}", t1));
                sw.Reset();

                #endregion Probing Phase

                #region Value Extraction Phase
                sw.Start();

                var joinOutputFinal = new Dictionary<string, int>();
                int index = 0;
                foreach (var item in _maat.GetAll())
                {
                    if (item != null)
                    {
                        string key = item[0] + ", " + item[1];
                        int revenue = 0;
                        if (joinOutputFinal.TryGetValue(key, out revenue))
                        {
                            joinOutputFinal[key] = revenue + Convert.ToInt32(item[2]);
                        }
                        else
                        {
                            joinOutputFinal.Add(key, Convert.ToInt32(item[2]));
                        }
                    }
                    index++;
                }

                sw.Stop();
                long t2 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNimble Join] T2 Time: {0}", t2));
                Console.WriteLine(String.Format("[PNimble Join] Total Time: {0}", t0 + t1 + t2));
                Console.WriteLine(String.Format("[PNimble Join] Total Count: {0}", joinOutputFinal.Count()));
                Console.WriteLine();
                #endregion Value Extraction Phase
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void Query_4_2_IM()
        {
            try
            {
                Stopwatch sw = new Stopwatch();

                List<Customer> customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
                List<Supplier> supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
                List<Date> dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                List<Part> partDimension = Utils.ReadFromBinaryFiles<Part>(partFile.Replace("BF", "BF" + scaleFactor));

                List<int> loCustomerKey = Utils.ReadFromBinaryFiles<int>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                List<int> loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                List<int> loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                List<int> loPartKey = Utils.ReadFromBinaryFiles<int>(loPartKeyFile.Replace("BF", "BF" + scaleFactor));

                List<int> loRevenue = Utils.ReadFromBinaryFiles<int>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
                List<int> loSupplyCost = Utils.ReadFromBinaryFiles<int>(loSupplyCostFile.Replace("BF", "BF" + scaleFactor));

                sw.Start();
                #region Key Hashing Phase 

                var customerHashTable = new Dictionary<int, string>();
                var supplierHashTable = new Dictionary<int, string>();
                var dateHashTable = new Dictionary<int, string>();
                var partHashTable = new Dictionary<int, string>();

                Parallel.Invoke(parallelOptions,
               () =>
               {
                   foreach (var row in dateDimension)
                   {
                       if (row.dYear.Equals("1997") || row.dYear.Equals("1998"))
                       {
                           dateHashTable.Add(row.dDateKey, row.dYear);
                       }
                   }
               },
               () =>
               {
                   foreach (var row in customerDimension)
                   {
                       if (row.cRegion.Equals("AMERICA"))
                           customerHashTable.Add(row.cCustKey, row.cNation);
                   }
               },
               () =>
               {
                   foreach (var row in supplierDimension)
                   {
                       if (row.sRegion.Equals("AMERICA"))
                           supplierHashTable.Add(row.sSuppKey, row.sNation);
                   }
               },
               () =>
               {
                   foreach (var row in partDimension)
                   {
                       if (row.pMFGR.Equals("MFGR#1") || row.pMFGR.Equals("MFGR#2"))
                       {
                           partHashTable.Add(row.pPartKey, row.pCategory);
                       }
                   }
               });

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNimble Join] T0 Time: {0}", t0));
                sw.Reset();
                #endregion Key Hashing Phase

                var _maat = new MAATIM(loSupplierKey.Count);
                var partitionIndexes = Utils.getPartitionIndexes(loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);
                #region Probing Phase
                sw.Start();

                List<Task> tasks = new List<Task>();
                foreach (var indexes in partitionIndexes)
                {
                    Task t = Task.Factory.StartNew(() =>
                    {
                        for (int i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            int suppKey = loSupplierKey[i];
                            int dateKey = loOrderDate[i];
                            int partKey = loPartKey[i];
                            int custKey = loCustomerKey[i];
                            string suppNation = string.Empty;
                            string dYear = string.Empty;
                            string pCategory = string.Empty;
                            if (supplierHashTable.TryGetValue(suppKey, out suppNation)
                            && partHashTable.TryGetValue(partKey, out pCategory)
                            && dateHashTable.TryGetValue(dateKey, out dYear)
                            && customerHashTable.ContainsKey(custKey))
                            {
                                _maat.AddOrUpdate(i, new List<object> { dYear, suppNation, pCategory, (loRevenue[i] - loSupplyCost[i]) });
                            }
                        }
                    });
                    tasks.Add(t);
                }

                Task.WaitAll(tasks.ToArray());

                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNimble Join] T1 Time: {0}", t1));
                sw.Reset();

                #endregion Probing Phase

                #region Value Extraction Phase
                sw.Start();

                var joinOutputFinal = new Dictionary<string, int>();
                int index = 0;
                foreach (var item in _maat.GetAll())
                {
                    if (item != null)
                    {
                        string key = item[0] + ", " + item[1] + ", " + item[2];
                        int revenue = 0;
                        if (joinOutputFinal.TryGetValue(key, out revenue))
                        {
                            joinOutputFinal[key] = revenue + Convert.ToInt32(item[3]);
                        }
                        else
                        {
                            joinOutputFinal.Add(key, Convert.ToInt32(item[3]));
                        }
                    }
                    index++;
                }

                sw.Stop();
                long t2 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNimble Join] T2 Time: {0}", t2));
                Console.WriteLine(String.Format("[PNimble Join] Total Time: {0}", t0 + t1 + t2));
                Console.WriteLine(String.Format("[PNimble Join] Total Count: {0}", joinOutputFinal.Count()));
                Console.WriteLine();
                #endregion Value Extraction Phase
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void Query_4_3_IM()
        {
            try
            {
                Stopwatch sw = new Stopwatch();

                List<Customer> customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
                List<Supplier> supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
                List<Date> dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                List<Part> partDimension = Utils.ReadFromBinaryFiles<Part>(partFile.Replace("BF", "BF" + scaleFactor));

                List<int> loCustomerKey = Utils.ReadFromBinaryFiles<int>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                List<int> loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                List<int> loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                List<int> loPartKey = Utils.ReadFromBinaryFiles<int>(loPartKeyFile.Replace("BF", "BF" + scaleFactor));

                List<int> loRevenue = Utils.ReadFromBinaryFiles<int>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
                List<int> loSupplyCost = Utils.ReadFromBinaryFiles<int>(loSupplyCostFile.Replace("BF", "BF" + scaleFactor));

                sw.Start();
                #region Key Hashing Phase 

                var customerHashTable = new Dictionary<int, string>();
                var supplierHashTable = new Dictionary<int, string>();
                var dateHashTable = new Dictionary<int, string>();
                var partHashTable = new Dictionary<int, string>();

                Parallel.Invoke(parallelOptions,
                () =>
                {
                    foreach (var row in dateDimension)
                    {
                        if (row.dYear.Equals("1997") || row.dYear.Equals("1998"))
                            dateHashTable.Add(row.dDateKey, row.dYear);
                    }
                },
                () =>
                {
                    foreach (var row in customerDimension)
                    {
                        if (row.cRegion.Equals("AMERICA"))
                            customerHashTable.Add(row.cCustKey, row.cNation);
                    }
                },
                () =>
                {
                    foreach (var row in supplierDimension)
                    {
                        if (row.sNation.Equals("UNITED STATES"))
                            supplierHashTable.Add(row.sSuppKey, row.sCity);
                    }
                },
                () =>
                {
                    foreach (var row in partDimension)
                    {
                        if (row.pCategory.Equals("MFGR#14"))
                            partHashTable.Add(row.pPartKey, row.pBrand);
                    }
                });

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNimble Join] T0 Time: {0}", t0));
                sw.Reset();
                #endregion Key Hashing Phase

                var _maat = new MAATIM(loSupplierKey.Count);
                var partitionIndexes = Utils.getPartitionIndexes(loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);
                #region Probing Phase
                sw.Start();

                List<Task> tasks = new List<Task>();
                foreach (var indexes in partitionIndexes)
                {
                    Task t = Task.Factory.StartNew(() =>
                    {
                        for (int i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            int suppKey = loSupplierKey[i];
                            int dateKey = loOrderDate[i];
                            int partKey = loPartKey[i];
                            int custKey = loCustomerKey[i];
                            string suppCity = string.Empty;
                            string dYear = string.Empty;
                            string pBrand = string.Empty;
                            if (supplierHashTable.TryGetValue(suppKey, out suppCity)
                            && partHashTable.TryGetValue(partKey, out pBrand)
                            && dateHashTable.TryGetValue(dateKey, out dYear)
                            && customerHashTable.ContainsKey(custKey))
                            {
                                _maat.AddOrUpdate(i, new List<object> { dYear, suppCity, pBrand, (loRevenue[i] - loSupplyCost[i]) });
                            }
                        }
                    });
                    tasks.Add(t);
                }

                Task.WaitAll(tasks.ToArray());

                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNimble Join] T1 Time: {0}", t1));
                sw.Reset();

                #endregion Probing Phase

                #region Value Extraction Phase
                sw.Start();

                var joinOutputFinal = new Dictionary<string, int>();
                int index = 0;
                foreach (var item in _maat.GetAll())
                {
                    if (item != null)
                    {
                        string key = item[0] + ", " + item[1] + ", " + item[2];
                        int revenue = 0;
                        if (joinOutputFinal.TryGetValue(key, out revenue))
                        {
                            joinOutputFinal[key] = revenue + Convert.ToInt32(item[3]);
                        }
                        else
                        {
                            joinOutputFinal.Add(key, Convert.ToInt32(item[3]));
                        }
                    }
                    index++;
                }

                sw.Stop();
                long t2 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNimble Join] T2 Time: {0}", t2));
                Console.WriteLine(String.Format("[PNimble Join] Total Time: {0}", t0 + t1 + t2));
                Console.WriteLine(String.Format("[PNimble Join] Total Count: {0}", joinOutputFinal.Count()));
                Console.WriteLine();
                #endregion Value Extraction Phase
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        //public void Query_3_1_Using_ConcurrentDictionary()
        //{
        //    try
        //    {
        //        long memoryStartPhase1 = GC.GetTotalMemory(true);
        //        outputRecordsCounter = 0;
        //        var dateHashTable = new Dictionary<int, string>();
        //        var customerHashTable = new Dictionary<int, string>();
        //        var supplierHashTable = new Dictionary<int, string>();
        //        Stopwatch sw = new Stopwatch();
        //        Stopwatch swInitialRecorder = new Stopwatch();
        //        Stopwatch swOutputRecorder = new Stopwatch();

        //        #region Key Hashing Phase
        //        List<Date> dateDimension = null;
        //        List<Supplier> supplierDimension = null;
        //        List<Customer> customerDimension = null;

        //        swInitialRecorder.Start();
        //        swOutputRecorder.Start();
        //        sw.Start();

        //        Parallel.Invoke(parallelOptions,
        //            () => dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor)),
        //            () => customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor)),
        //            () => supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor))
        //             );
        //        sw.Stop();
        //        testResults.phase11IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        Parallel.Invoke(parallelOptions, () =>
        //        {
        //            foreach (var row in dateDimension)
        //            {
        //                if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
        //                    dateHashTable.Add(row.dDateKey, row.dYear);
        //            }
        //        },
        //        () =>
        //        {
        //            foreach (var row in customerDimension)
        //            {
        //                if (row.cRegion.Equals("ASIA"))
        //                    customerHashTable.Add(row.cCustKey, row.cNation);
        //            }
        //        },
        //        () =>
        //        {
        //            foreach (var row in supplierDimension)
        //            {
        //                if (row.sRegion.Equals("ASIA"))
        //                    supplierHashTable.Add(row.sSuppKey, row.sNation);
        //            }
        //        });
        //        sw.Stop();
        //        testResults.phase11HashTime = sw.ElapsedMilliseconds;
        //        testResults.phase1Time = testResults.phase11HashTime + testResults.phase11IOTime;
        //        sw.Reset();

        //        customerDimension.Clear();
        //        dateDimension.Clear();
        //        supplierDimension.Clear();

        //        long memoryUsedPhase1 = GC.GetTotalMemory(true) - memoryStartPhase1;
        //        #endregion Key Hashing Phase

        //        #region Probing Phase
        //        long memoryStartPhase2 = GC.GetTotalMemory(true);
        //        sw.Start();
        //        List<int> loOrderDate = null;
        //        List<int> loCustomerKey = null;
        //        List<int> loSupplierKey = null;
        //        Parallel.Invoke(parallelOptions,
        //            () => loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loCustomerKey = Utils.ReadFromBinaryFiles<int>(loCustKeyFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor)));
        //        sw.Stop();
        //        testResults.phase21IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        var intermediateHashTable = new ConcurrentDictionary<int, Triplets>();
        //        var i = 0;
        //        foreach (var suppKey in loSupplierKey)
        //        {
        //            string sNation = string.Empty;
        //            if (supplierHashTable.TryGetValue(suppKey, out sNation))
        //            {
        //                Triplets t = new Triplets();
        //                t.x = sNation;
        //                intermediateHashTable.TryAdd(i, t);
        //            }
        //            i++;
        //        }

        //        Parallel.Invoke(() =>
        //        {
        //            var k = 0;
        //            foreach (var orderDate in loOrderDate)
        //            {
        //                string dYear = string.Empty;
        //                if (dateHashTable.TryGetValue(orderDate, out dYear))
        //                {
        //                    Triplets values;
        //                    if (intermediateHashTable.TryGetValue(k, out values))
        //                    {
        //                        values.y = dYear;
        //                        intermediateHashTable[k] = values;
        //                    }
        //                }
        //                else
        //                {
        //                    Triplets values;
        //                    intermediateHashTable.TryRemove(k, out values);
        //                }
        //                k++;
        //            }
        //        }, () =>
        //        {
        //            var j = 0;
        //            foreach (var custKey in loCustomerKey)
        //            {
        //                string cNation = string.Empty;
        //                if (customerHashTable.TryGetValue(custKey, out cNation))
        //                {
        //                    Triplets values;
        //                    if (intermediateHashTable.TryGetValue(j, out values))
        //                    {
        //                        values.z = cNation;
        //                        intermediateHashTable[j] = values;
        //                    }
        //                }
        //                else
        //                {
        //                    Triplets values;
        //                    intermediateHashTable.TryRemove(j, out values);
        //                }
        //                j++;
        //            }
        //        });

        //        sw.Stop();
        //        testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
        //        testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime;
        //        sw.Reset();

        //        loOrderDate.Clear();
        //        loCustomerKey.Clear();
        //        loSupplierKey.Clear();

        //        long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
        //        #endregion Probing Phase

        //        #region Value Extraction Phase
        //        long memoryStartPhase3 = GC.GetTotalMemory(true);
        //        sw.Start();
        //        List<int> loRevenue = Utils.ReadFromBinaryFiles<int>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
        //        sw.Stop();
        //        testResults.phase3IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        var joinOutputFinal = new Dictionary<int, string>();
        //        foreach (var item in intermediateHashTable)
        //        {
        //            joinOutputFinal.Add(item.Key, item.Value + ", " + loRevenue[item.Key]); // Direct array lookup
        //        }
        //        sw.Stop();

        //        long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
        //        #endregion Value Extraction Phase
        //        testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
        //        testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
        //        testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
        //        // Console.WriteLine("[Nimble Join]: Time taken {0} ms.", testResults.totalExecutionTime);
        //        testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
        //        testResults.totalNumberOfOutput = intermediateHashTable.Count;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        private void saveAndPrintResults()
        {
            TestResultsDatabase.pNimbleJoinOutput.Add(testResults.toString());
            Console.WriteLine("Parallel Nimble: " + testResults.toString());
            Console.WriteLine();
        }
    }
}
