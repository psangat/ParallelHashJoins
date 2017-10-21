using System;
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
        public ParallelNimbleJoin(string scaleFactor, MemoryManagement memoryManagement = MemoryManagement.LAZY)
        {
            this.scaleFactor = scaleFactor;
            this.memoryManagement = memoryManagement;
            testResults.totalRAMAvailable = Utils.getAvailableRAM();
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

        ParallelOptions parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount };
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
        public void Query_3_1_Using_ConcurrentDictionary()
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

                Parallel.Invoke(parallelOptions,
                    () => dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor)),
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
                Parallel.Invoke(parallelOptions,
                    () => loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor)),
                    () => loCustomerKey = Utils.ReadFromBinaryFiles<int>(loCustKeyFile.Replace("BF", "BF" + scaleFactor)),
                    () => loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor)));
                sw.Stop();
                testResults.phase21IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var intermediateHashTable = new ConcurrentDictionary<int, Triplets>();
                var i = 0;
                foreach (var suppKey in loSupplierKey)
                {
                    string sNation = string.Empty;
                    if (supplierHashTable.TryGetValue(suppKey, out sNation))
                    {
                        Triplets t = new Triplets();
                        t.x = sNation;
                        intermediateHashTable.TryAdd(i, t);
                    }
                    i++;
                }

                Parallel.Invoke(() =>
                {
                    var k = 0;
                    foreach (var orderDate in loOrderDate)
                    {
                        string dYear = string.Empty;
                        if (dateHashTable.TryGetValue(orderDate, out dYear))
                        {
                            Triplets values;
                            if (intermediateHashTable.TryGetValue(k, out values))
                            {
                                values.y = dYear;
                                intermediateHashTable[k] = values;
                            }
                        }
                        else
                        {
                            Triplets values;
                            intermediateHashTable.TryRemove(k, out values);
                        }
                        k++;
                    }
                }, () =>
                {
                    var j = 0;
                    foreach (var custKey in loCustomerKey)
                    {
                        string cNation = string.Empty;
                        if (customerHashTable.TryGetValue(custKey, out cNation))
                        {
                            Triplets values;
                            if (intermediateHashTable.TryGetValue(j, out values))
                            {
                                values.z = cNation;
                                intermediateHashTable[j] = values;
                            }
                        }
                        else
                        {
                            Triplets values;
                            intermediateHashTable.TryRemove(j, out values);
                        }
                        j++;
                    }
                });

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
    }
}
