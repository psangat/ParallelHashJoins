using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParallelHashJoins
{
    class ParallelInvisibleJoin
    {
        private static string binaryFilesDirectory = @"C:\Raw_Data_Source_For_Test\SSBM - DBGEN\BF";
        private string scaleFactor { get; set; }
        private MemoryManagement memoryManagement { get; set; }
        public ParallelInvisibleJoin(string scaleFactor, MemoryManagement memoryManagement = MemoryManagement.LAZY)
        {
            this.scaleFactor = scaleFactor;
            this.memoryManagement = memoryManagement;
            testResults.totalRAMAvailable = Utils.getAvailableRAM();
        }

        #region Private Variables
        private Object obj = new object();
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
                List<int> loOrderDate = null;
                List<int> loDiscount = null;
                List<int> loQuantity = null;
                Parallel.Invoke(parallelOptions, () => loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor)),
                    () => loDiscount = Utils.ReadFromBinaryFiles<int>(loDiscountFile.Replace("BF", "BF" + scaleFactor)),
                    () => loQuantity = Utils.ReadFromBinaryFiles<int>(loQuantityFile.Replace("BF", "BF" + scaleFactor)));

                sw.Stop();
                testResults.phase21IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var listOrderDatePositions = new List<int>();
                var listLineOrderDiscountPositions = new List<int>();
                var listLineOrderQuantityPositions = new List<int>();

                Parallel.Invoke(parallelOptions, () =>
                {
                    var i = 0;
                    foreach (var orderDate in loOrderDate)
                    {
                        string dYear = "";
                        if (dateHashTable.TryGetValue(orderDate, out dYear))
                        {
                            listOrderDatePositions.Add(i);
                        }
                        i++;
                    }
                },
                () =>
                {
                    var j = 0;
                    foreach (var _loDiscount in loDiscount)
                    {
                        if (_loDiscount >= 1 && _loDiscount <= 3)
                        {
                            listLineOrderDiscountPositions.Add(j);
                        }
                        j++;
                    }
                },
                () =>
                {
                    var k = 0;
                    foreach (var _loQuantity in loQuantity)
                    {
                        string sNationOut = string.Empty;
                        if (_loQuantity < 25)
                        {
                            listLineOrderQuantityPositions.Add(k);
                        }
                        k++;
                    }
                });
                var common = listLineOrderDiscountPositions.Intersect(listOrderDatePositions).Intersect(listLineOrderQuantityPositions).ToList();

                sw.Stop();
                testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
                testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime +
                    testResults.phase22IOTime + testResults.phase22ProbeTime +
                    testResults.phase23IOTime + testResults.phase23ProbeTime;
                sw.Reset();

                loOrderDate.Clear();
                dateHashTable.Clear();
                listLineOrderDiscountPositions.Clear();
                listLineOrderQuantityPositions.Clear();
                loQuantity.Clear();

                long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
                #endregion Probing Phase


                #region Value Extraction Phase
                long memoryStartPhase3 = GC.GetTotalMemory(true);
                sw.Start();
                loDiscount = Utils.ReadFromBinaryFiles<int>(loDiscountFile.Replace("BF", "BF" + scaleFactor));
                List<int> loExtendedPrice = Utils.ReadFromBinaryFiles<int>(loExtendedPriceFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase3IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                int totalRevenue = 0;
                object lockObject = new object();
                Parallel.ForEach(common, (index) =>
                {
                    try
                    {
                        lock (lockObject)
                        {
                            var revenue = loDiscount[index] * loExtendedPrice[index];
                            totalRevenue += revenue;
                        }
                        if (isFirst)
                        {
                            swInitialRecorder.Stop();
                            testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
                            isFirst = false;
                        }

                        outputRecordsCounter++;
                        if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
                        {
                            swOutputRecorder.Stop();
                            //testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
                            swOutputRecorder.Start();
                        }
                        // Console.WriteLine(l +", "+ dYear  + ", " + sNationOut + ", " + cNationOut);
                    }
                    catch (Exception)
                    {
                        throw;
                    }
                });
                sw.Stop();
                long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
                #endregion Value Extraction Phase
                testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
                testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
                testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
                //Console.WriteLine("[Invisble Join]: Time taken {0} ms.", sw.ElapsedMilliseconds);
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
                List<int> loOrderDate = null;
                List<int> loDiscount = null;
                List<int> loQuantity = null;
                Parallel.Invoke(parallelOptions, () => loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor)),
                    () => loDiscount = Utils.ReadFromBinaryFiles<int>(loDiscountFile.Replace("BF", "BF" + scaleFactor)),
                    () => loQuantity = Utils.ReadFromBinaryFiles<int>(loQuantityFile.Replace("BF", "BF" + scaleFactor)));

                sw.Stop();
                testResults.phase21IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var listOrderDatePositions = new List<int>();
                var listLineOrderDiscountPositions = new List<int>();
                var listLineOrderQuantityPositions = new List<int>();

                Parallel.Invoke(parallelOptions, () =>
                {
                    var i = 0;
                    foreach (var orderDate in loOrderDate)
                    {
                        string dYear = "";
                        if (dateHashTable.TryGetValue(orderDate, out dYear))
                        {
                            listOrderDatePositions.Add(i);
                        }
                        i++;
                    }
                },
                () =>
                {
                    var j = 0;
                    foreach (var _loDiscount in loDiscount)
                    {
                        if (_loDiscount >= 4 && _loDiscount <= 6)
                        {
                            listLineOrderDiscountPositions.Add(j);
                        }
                        j++;
                    }
                },
                () =>
                {
                    var k = 0;
                    foreach (var _loQuantity in loQuantity)
                    {
                        string sNationOut = string.Empty;
                        if (_loQuantity >= 26 && _loQuantity <= 35)
                        {
                            listLineOrderQuantityPositions.Add(k);
                        }
                        k++;
                    }
                });
                var common = listLineOrderDiscountPositions.Intersect(listOrderDatePositions).Intersect(listLineOrderQuantityPositions).ToList();

                sw.Stop();
                testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
                testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime +
                    testResults.phase22IOTime + testResults.phase22ProbeTime +
                    testResults.phase23IOTime + testResults.phase23ProbeTime;
                sw.Reset();

                loOrderDate.Clear();
                dateHashTable.Clear();
                listLineOrderDiscountPositions.Clear();
                listLineOrderQuantityPositions.Clear();
                loQuantity.Clear();

                long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
                #endregion Probing Phase


                #region Value Extraction Phase
                long memoryStartPhase3 = GC.GetTotalMemory(true);
                sw.Start();
                loDiscount = Utils.ReadFromBinaryFiles<int>(loDiscountFile.Replace("BF", "BF" + scaleFactor));
                List<int> loExtendedPrice = Utils.ReadFromBinaryFiles<int>(loExtendedPriceFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase3IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                int totalRevenue = 0;
                object lockObject = new object();
                Parallel.ForEach(common, (index) =>
                {
                    try
                    {
                        lock (lockObject)
                        {
                            var revenue = loDiscount[index] * loExtendedPrice[index];
                            totalRevenue += revenue;
                        }
                        if (isFirst)
                        {
                            swInitialRecorder.Stop();
                            testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
                            isFirst = false;
                        }

                        outputRecordsCounter++;
                        if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
                        {
                            swOutputRecorder.Stop();
                            //testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
                            swOutputRecorder.Start();
                        }
                        // Console.WriteLine(l +", "+ dYear  + ", " + sNationOut + ", " + cNationOut);
                    }
                    catch (Exception)
                    {
                        throw;
                    }
                });
                sw.Stop();
                long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
                #endregion Value Extraction Phase
                testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
                testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
                testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
                //Console.WriteLine("[Invisble Join]: Time taken {0} ms.", sw.ElapsedMilliseconds);
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
                    if (row.dYear.Equals("1994") && row.dWeekNumInYear == 6 )
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
                List<int> loOrderDate = null;
                List<int> loDiscount = null;
                List<int> loQuantity = null;
                Parallel.Invoke(parallelOptions, () => loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor)),
                    () => loDiscount = Utils.ReadFromBinaryFiles<int>(loDiscountFile.Replace("BF", "BF" + scaleFactor)),
                    () => loQuantity = Utils.ReadFromBinaryFiles<int>(loQuantityFile.Replace("BF", "BF" + scaleFactor)));

                sw.Stop();
                testResults.phase21IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var listOrderDatePositions = new List<int>();
                var listLineOrderDiscountPositions = new List<int>();
                var listLineOrderQuantityPositions = new List<int>();

                Parallel.Invoke(parallelOptions, () =>
                {
                    var i = 0;
                    foreach (var orderDate in loOrderDate)
                    {
                        string dYear = "";
                        if (dateHashTable.TryGetValue(orderDate, out dYear))
                        {
                            listOrderDatePositions.Add(i);
                        }
                        i++;
                    }
                },
                () =>
                {
                    var j = 0;
                    foreach (var _loDiscount in loDiscount)
                    {
                        if (_loDiscount >= 5 && _loDiscount <= 7)
                        {
                            listLineOrderDiscountPositions.Add(j);
                        }
                        j++;
                    }
                },
                () =>
                {
                    var k = 0;
                    foreach (var _loQuantity in loQuantity)
                    {
                        string sNationOut = string.Empty;
                        if (_loQuantity >= 26 && _loQuantity <= 35)
                        {
                            listLineOrderQuantityPositions.Add(k);
                        }
                        k++;
                    }
                });
                var common = listLineOrderDiscountPositions.Intersect(listOrderDatePositions).Intersect(listLineOrderQuantityPositions).ToList();

                sw.Stop();
                testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
                testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime +
                    testResults.phase22IOTime + testResults.phase22ProbeTime +
                    testResults.phase23IOTime + testResults.phase23ProbeTime;
                sw.Reset();

                loOrderDate.Clear();
                dateHashTable.Clear();
                listLineOrderDiscountPositions.Clear();
                listLineOrderQuantityPositions.Clear();
                loQuantity.Clear();

                long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
                #endregion Probing Phase


                #region Value Extraction Phase
                long memoryStartPhase3 = GC.GetTotalMemory(true);
                sw.Start();
                loDiscount = Utils.ReadFromBinaryFiles<int>(loDiscountFile.Replace("BF", "BF" + scaleFactor));
                List<int> loExtendedPrice = Utils.ReadFromBinaryFiles<int>(loExtendedPriceFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase3IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                int totalRevenue = 0;
                object lockObject = new object();
                Parallel.ForEach(common, (index) =>
                {
                    try
                    {
                        lock (lockObject)
                        {
                            var revenue = loDiscount[index] * loExtendedPrice[index];
                            totalRevenue += revenue;
                        }
                        if (isFirst)
                        {
                            swInitialRecorder.Stop();
                            testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
                            isFirst = false;
                        }

                        outputRecordsCounter++;
                        if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
                        {
                            swOutputRecorder.Stop();
                            //testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
                            swOutputRecorder.Start();
                        }
                        // Console.WriteLine(l +", "+ dYear  + ", " + sNationOut + ", " + cNationOut);
                    }
                    catch (Exception)
                    {
                        throw;
                    }
                });
                sw.Stop();
                long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
                #endregion Value Extraction Phase
                testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
                testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
                testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
                //Console.WriteLine("[Invisble Join]: Time taken {0} ms.", sw.ElapsedMilliseconds);
                testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
                testResults.totalNumberOfOutput = totalRevenue;
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

                sw.Reset();

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
                var listOrderDatePositions = new List<int>();
                var listCustomerKeyPositions = new List<int>();
                var listSupplierKeyPositions = new List<int>();

                Parallel.Invoke(parallelOptions, () =>
                {
                    var i = 0;
                    foreach (var orderDate in loOrderDate)
                    {
                        string dYear = "";
                        if (dateHashTable.TryGetValue(orderDate, out dYear))
                        {
                            listOrderDatePositions.Add(i);
                        }
                        i++;
                    }
                },
                () =>
                {
                    var j = 0;
                    foreach (var custKey in loCustomerKey)
                    {
                        string cNationOut = string.Empty;
                        if (customerHashTable.TryGetValue(custKey, out cNationOut))
                        {
                            listCustomerKeyPositions.Add(j);
                        }
                        j++;
                    }
                },
                () =>
                {
                    var k = 0;
                    foreach (var suppKey in loSupplierKey)
                    {
                        string sNationOut = string.Empty;
                        if (supplierHashTable.TryGetValue(suppKey, out sNationOut))
                        {
                            listSupplierKeyPositions.Add(k);
                        }
                        k++;
                    }
                });

                var common = listCustomerKeyPositions.Intersect(listOrderDatePositions).Intersect(listSupplierKeyPositions).ToList();

                sw.Stop();
                testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
                testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime;
                sw.Reset();

                loOrderDate.Clear();
                dateHashTable.Clear();
                loCustomerKey.Clear();
                customerHashTable.Clear();
                loSupplierKey.Clear();
                supplierHashTable.Clear();

                long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
                #endregion Probing Phase

                #region Value Extraction Phase
                long memoryStartPhase3 = GC.GetTotalMemory(true);
                sw.Start();
                List<string> cNation = null;
                List<string> sNation = null;
                List<int> loRevenue = null;
                Parallel.Invoke(parallelOptions, () => loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor)),
                () => loCustomerKey = Utils.ReadFromBinaryFiles<int>(loCustKeyFile.Replace("BF", "BF" + scaleFactor)),
                () => loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor)),
                () => cNation = Utils.ReadFromBinaryFiles<string>(cNationFile.Replace("BF", "BF" + scaleFactor)),
                () => sNation = Utils.ReadFromBinaryFiles<string>(sNationFile.Replace("BF", "BF" + scaleFactor)),
                () => loRevenue = Utils.ReadFromBinaryFiles<int>(loRevenueFile.Replace("BF", "BF" + scaleFactor)),
                () => dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor))
                );

                foreach (var row in dateDimension)
                {
                    if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
                        dateHashTable.Add(row.dDateKey, row.dYear);
                }

                dateDimension.Clear();
                sw.Stop();
                testResults.phase3IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var joinOutputIntermediate = new ConcurrentDictionary<int, string>();
                Parallel.ForEach(common, (index) =>
                {
                    try
                    {
                        var dateKey = 0;
                        var custKey = 0;
                        var suppKey = 0;
                        string cNationOut = "";
                        string sNationOut = "";
                        string dYear = "";

                        dateKey = loOrderDate[index];
                        dateHashTable.TryGetValue(dateKey, out dYear);
                        custKey = loCustomerKey[index];
                        cNationOut = cNation[custKey];
                        suppKey = loSupplierKey[index];
                        sNationOut = sNation[suppKey - 1];
                        if (isFirst)
                        {
                            swInitialRecorder.Stop();
                            testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
                            isFirst = false;
                        }
                        outputRecordsCounter++;
                        if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
                        {
                            swOutputRecorder.Stop();
                            //testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
                            swOutputRecorder.Start();
                        }
                        // Console.WriteLine(l +", "+ dYear  + ", " + sNationOut + ", " + cNationOut);
                        joinOutputIntermediate.TryAdd(index, cNationOut + "," + sNationOut + "," + dYear);
                    }
                    catch (Exception)
                    {
                        throw;
                    }
                });

                var joinOutputFinal = new Dictionary<int, string>();
                foreach (var item in joinOutputIntermediate)
                {
                    joinOutputFinal.Add(item.Key, item.Value + "," + loRevenue[item.Key]);
                }

                sw.Stop();

                long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
                #endregion Value Extraction Phase

                testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
                testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
                testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
                // Console.WriteLine("[Nimble Join]: Time taken {0} ms.", testResults.totalExecutionTime);
                testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
                testResults.totalNumberOfOutput = joinOutputFinal.Count;

            }
            catch (Exception ex)
            {

                throw;
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

                sw.Reset();

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
                var listOrderDatePositions = new List<int>();
                var listCustomerKeyPositions = new List<int>();
                var listSupplierKeyPositions = new List<int>();

                Parallel.Invoke(parallelOptions, () =>
                {
                    var i = 0;
                    foreach (var orderDate in loOrderDate)
                    {
                        string dYear = "";
                        if (dateHashTable.TryGetValue(orderDate, out dYear))
                        {
                            listOrderDatePositions.Add(i);
                        }
                        i++;
                    }
                },
                () =>
                {
                    var j = 0;
                    foreach (var custKey in loCustomerKey)
                    {
                        string cCityOut = string.Empty;
                        if (customerHashTable.TryGetValue(custKey, out cCityOut))
                        {
                            listCustomerKeyPositions.Add(j);
                        }
                        j++;
                    }
                },
                () =>
                {
                    var k = 0;
                    foreach (var suppKey in loSupplierKey)
                    {
                        string sCityOut = string.Empty;
                        if (supplierHashTable.TryGetValue(suppKey, out sCityOut))
                        {
                            listSupplierKeyPositions.Add(k);
                        }
                        k++;
                    }
                });

                var common = listCustomerKeyPositions.Intersect(listOrderDatePositions).Intersect(listSupplierKeyPositions).ToList();

                sw.Stop();
                testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
                testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime;
                sw.Reset();

                loOrderDate.Clear();
                dateHashTable.Clear();
                loCustomerKey.Clear();
                customerHashTable.Clear();
                loSupplierKey.Clear();
                supplierHashTable.Clear();

                long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
                #endregion Probing Phase

                #region Value Extraction Phase
                long memoryStartPhase3 = GC.GetTotalMemory(true);
                sw.Start();
                List<string> cCity = null;
                List<string> sCity = null;
                List<int> loRevenue = null;
                Parallel.Invoke(parallelOptions, () => loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor)),
                () => loCustomerKey = Utils.ReadFromBinaryFiles<int>(loCustKeyFile.Replace("BF", "BF" + scaleFactor)),
                () => loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor)),
                () => cCity = Utils.ReadFromBinaryFiles<string>(cCityFile.Replace("BF", "BF" + scaleFactor)),
                () => sCity = Utils.ReadFromBinaryFiles<string>(sCityFile.Replace("BF", "BF" + scaleFactor)),
                () => loRevenue = Utils.ReadFromBinaryFiles<int>(loRevenueFile.Replace("BF", "BF" + scaleFactor)),
                () => dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor))
                );

                foreach (var row in dateDimension)
                {
                    if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
                        dateHashTable.Add(row.dDateKey, row.dYear);
                }

                dateDimension.Clear();
                sw.Stop();
                testResults.phase3IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var joinOutputIntermediate = new ConcurrentDictionary<int, string>();
                Parallel.ForEach(common, (index) =>
                {
                    try
                    {
                        var dateKey = 0;
                        var custKey = 0;
                        var suppKey = 0;
                        string cCityOut = "";
                        string sCityOut = "";
                        string dYear = "";

                        dateKey = loOrderDate[index];
                        dateHashTable.TryGetValue(dateKey, out dYear);
                        custKey = loCustomerKey[index];
                        cCityOut = cCity[custKey];
                        suppKey = loSupplierKey[index];
                        sCityOut = sCity[suppKey - 1];
                        if (isFirst)
                        {
                            swInitialRecorder.Stop();
                            testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
                            isFirst = false;
                        }
                        outputRecordsCounter++;
                        if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
                        {
                            swOutputRecorder.Stop();
                            //testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
                            swOutputRecorder.Start();
                        }
                        // Console.WriteLine(l +", "+ dYear  + ", " + sNationOut + ", " + cNationOut);
                        joinOutputIntermediate.TryAdd(index, cCityOut + "," + sCityOut + "," + dYear);
                    }
                    catch (Exception)
                    {
                        throw;
                    }
                });

                var joinOutputFinal = new Dictionary<int, string>();
                foreach (var item in joinOutputIntermediate)
                {
                    joinOutputFinal.Add(item.Key, item.Value + "," + loRevenue[item.Key]);
                }

                sw.Stop();

                long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
                #endregion Value Extraction Phase

                testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
                testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
                testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
                // Console.WriteLine("[Nimble Join]: Time taken {0} ms.", testResults.totalExecutionTime);
                testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
                testResults.totalNumberOfOutput = joinOutputFinal.Count;

            }
            catch (Exception ex)
            {

                throw;
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

                sw.Reset();

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
                var listOrderDatePositions = new List<int>();
                var listCustomerKeyPositions = new List<int>();
                var listSupplierKeyPositions = new List<int>();

                Parallel.Invoke(parallelOptions, () =>
                {
                    var i = 0;
                    foreach (var orderDate in loOrderDate)
                    {
                        string dYear = "";
                        if (dateHashTable.TryGetValue(orderDate, out dYear))
                        {
                            listOrderDatePositions.Add(i);
                        }
                        i++;
                    }
                },
                () =>
                {
                    var j = 0;
                    foreach (var custKey in loCustomerKey)
                    {
                        string cCityOut = string.Empty;
                        if (customerHashTable.TryGetValue(custKey, out cCityOut))
                        {
                            listCustomerKeyPositions.Add(j);
                        }
                        j++;
                    }
                },
                () =>
                {
                    var k = 0;
                    foreach (var suppKey in loSupplierKey)
                    {
                        string sCityOut = string.Empty;
                        if (supplierHashTable.TryGetValue(suppKey, out sCityOut))
                        {
                            listSupplierKeyPositions.Add(k);
                        }
                        k++;
                    }
                });

                var common = listCustomerKeyPositions.Intersect(listOrderDatePositions).Intersect(listSupplierKeyPositions).ToList();

                sw.Stop();
                testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
                testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime;
                sw.Reset();

                loOrderDate.Clear();
                dateHashTable.Clear();
                loCustomerKey.Clear();
                customerHashTable.Clear();
                loSupplierKey.Clear();
                supplierHashTable.Clear();

                long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
                #endregion Probing Phase

                #region Value Extraction Phase
                long memoryStartPhase3 = GC.GetTotalMemory(true);
                sw.Start();
                List<string> cCity = null;
                List<string> sCity = null;
                List<int> loRevenue = null;
                Parallel.Invoke(parallelOptions, () => loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor)),
                () => loCustomerKey = Utils.ReadFromBinaryFiles<int>(loCustKeyFile.Replace("BF", "BF" + scaleFactor)),
                () => loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor)),
                () => cCity = Utils.ReadFromBinaryFiles<string>(cCityFile.Replace("BF", "BF" + scaleFactor)),
                () => sCity = Utils.ReadFromBinaryFiles<string>(sCityFile.Replace("BF", "BF" + scaleFactor)),
                () => loRevenue = Utils.ReadFromBinaryFiles<int>(loRevenueFile.Replace("BF", "BF" + scaleFactor)),
                () => dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor))
                );

                foreach (var row in dateDimension)
                {
                    if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
                        dateHashTable.Add(row.dDateKey, row.dYear);
                }

                dateDimension.Clear();
                sw.Stop();
                testResults.phase3IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var joinOutputIntermediate = new ConcurrentDictionary<int, string>();
                Parallel.ForEach(common, (index) =>
                {
                    try
                    {
                        var dateKey = 0;
                        var custKey = 0;
                        var suppKey = 0;
                        string cCityOut = "";
                        string sCityOut = "";
                        string dYear = "";

                        dateKey = loOrderDate[index];
                        dateHashTable.TryGetValue(dateKey, out dYear);
                        custKey = loCustomerKey[index];
                        cCityOut = cCity[custKey];
                        suppKey = loSupplierKey[index];
                        sCityOut = sCity[suppKey - 1];
                        if (isFirst)
                        {
                            swInitialRecorder.Stop();
                            testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
                            isFirst = false;
                        }
                        outputRecordsCounter++;
                        if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
                        {
                            swOutputRecorder.Stop();
                            //testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
                            swOutputRecorder.Start();
                        }
                        // Console.WriteLine(l +", "+ dYear  + ", " + sNationOut + ", " + cNationOut);
                        joinOutputIntermediate.TryAdd(index, cCityOut + "," + sCityOut + "," + dYear);
                    }
                    catch (Exception)
                    {
                        throw;
                    }
                });

                var joinOutputFinal = new Dictionary<int, string>();
                foreach (var item in joinOutputIntermediate)
                {
                    joinOutputFinal.Add(item.Key, item.Value + "," + loRevenue[item.Key]);
                }

                sw.Stop();

                long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
                #endregion Value Extraction Phase

                testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
                testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
                testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
                // Console.WriteLine("[Nimble Join]: Time taken {0} ms.", testResults.totalExecutionTime);
                testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
                testResults.totalNumberOfOutput = joinOutputFinal.Count;

            }
            catch (Exception ex)
            {

                throw;
            }

        }
    }
}
