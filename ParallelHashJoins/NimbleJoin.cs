using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParallelHashJoins
{
    struct Triplets
    {
        public string x;
        public string y;
        public string z;
    };
    class NimbleJoin
    {

        private static string binaryFilesDirectory = @"C:\Raw_Data_Source_For_Test\SSBM - DBGEN\BF";
        private string scaleFactor { get; set; }
        private MemoryManagement memoryManagement { get; set; }
        public NimbleJoin(string scaleFactor)
        {
            this.scaleFactor = scaleFactor;
            testResults.totalRAMAvailable = Utils.getAvailableRAM();
        }
        ~NimbleJoin()
        {
            //saveAndPrintResults();
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
                var _maat = new MAAT(loOrderDate.Count);
                var k = 0;
                foreach (var orderDate in loOrderDate)
                {
                    string dYear = string.Empty;
                    Record record = new Record();
                    if (dateHashTable.TryGetValue(orderDate, out dYear))
                    {
                        record.s1 = dYear;
                        _maat.AddOrUpdate(k, record);
                        _maat.positions.Add(k);
                    }
                    k++;
                }

                sw.Stop();
                testResults.phase22ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

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
                //List<int> loRevenue = Utils.ReadFromBinaryFiles<int>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
                List<int> loExtendedPrice = Utils.ReadFromBinaryFiles<int>(loExtendedPriceFile.Replace("BF", "BF" + scaleFactor));
                List<int> loDiscount = Utils.ReadFromBinaryFiles<int>(loDiscountFile.Replace("BF", "BF" + scaleFactor));
                List<int> loQuantity = Utils.ReadFromBinaryFiles<int>(loQuantityFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase3IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                //var joinOutputFinal = new Dictionary<int, string>();
                //foreach (var item in intermediateHashTable)
                //{
                //    joinOutputFinal.Add(item.Key, item.Value + ", " + loRevenue[item.Key]); // Direct array lookup
                //}

                var j = 0;
                int totalRevenue = 0;
                foreach (var key in _maat.positions)
                {
                    string cNation = string.Empty;
                    if (loQuantity[key] < 25)
                    {
                        int discount = loDiscount[key];
                        if (discount >= 1 && discount <= 3)
                        {
                            var revenue = (loExtendedPrice[key] * discount);
                            totalRevenue += revenue;
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
                }

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
                var _maat = new MAAT(loOrderDate.Count);
                var k = 0;
                foreach (var orderDate in loOrderDate)
                {
                    string dYear = string.Empty;
                    Record record = new Record();
                    if (dateHashTable.TryGetValue(orderDate, out dYear))
                    {
                        record.s1 = dYear;
                        _maat.AddOrUpdate(k, record);
                        _maat.positions.Add(k);
                    }
                    k++;
                }

                sw.Stop();
                testResults.phase22ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

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
                //List<int> loRevenue = Utils.ReadFromBinaryFiles<int>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
                List<int> loExtendedPrice = Utils.ReadFromBinaryFiles<int>(loExtendedPriceFile.Replace("BF", "BF" + scaleFactor));
                List<int> loDiscount = Utils.ReadFromBinaryFiles<int>(loDiscountFile.Replace("BF", "BF" + scaleFactor));
                List<int> loQuantity = Utils.ReadFromBinaryFiles<int>(loQuantityFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase3IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                //var joinOutputFinal = new Dictionary<int, string>();
                //foreach (var item in intermediateHashTable)
                //{
                //    joinOutputFinal.Add(item.Key, item.Value + ", " + loRevenue[item.Key]); // Direct array lookup
                //}

                var j = 0;
                int totalRevenue = 0;
                foreach (var key in _maat.positions)
                {
                    string cNation = string.Empty;
                    int quantity = loQuantity[key];
                    if (quantity >= 26 && quantity <= 35)
                    {
                        int discount = loDiscount[key];
                        if (discount >= 4 && discount <= 6)
                        {
                            var revenue = (loExtendedPrice[key] * discount);
                            totalRevenue += revenue;
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
                }

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
                var _maat = new MAAT(loOrderDate.Count);
                var k = 0;
                foreach (var orderDate in loOrderDate)
                {
                    string dYear = string.Empty;
                    Record record = new Record();
                    if (dateHashTable.TryGetValue(orderDate, out dYear))
                    {
                        record.s1 = dYear;
                        _maat.AddOrUpdate(k, record);
                        _maat.positions.Add(k);
                    }
                    k++;
                }

                sw.Stop();
                testResults.phase22ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

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
                //List<int> loRevenue = Utils.ReadFromBinaryFiles<int>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
                List<int> loExtendedPrice = Utils.ReadFromBinaryFiles<int>(loExtendedPriceFile.Replace("BF", "BF" + scaleFactor));
                List<int> loDiscount = Utils.ReadFromBinaryFiles<int>(loDiscountFile.Replace("BF", "BF" + scaleFactor));
                List<int> loQuantity = Utils.ReadFromBinaryFiles<int>(loQuantityFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase3IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                //var joinOutputFinal = new Dictionary<int, string>();
                //foreach (var item in intermediateHashTable)
                //{
                //    joinOutputFinal.Add(item.Key, item.Value + ", " + loRevenue[item.Key]); // Direct array lookup
                //}

                var j = 0;
                int totalRevenue = 0;
                foreach (var key in _maat.positions)
                {
                    string cNation = string.Empty;
                    int quantity = loQuantity[key];
                    if (quantity >= 26 && quantity <= 35)
                    {
                        int discount = loDiscount[key];
                        if (discount >= 5 && discount <= 7)
                        {
                            var revenue = (loExtendedPrice[key] * discount);
                            totalRevenue += revenue;
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
                }

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
                dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase11IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                foreach (var row in dateDimension)
                {
                    dateHashTable.Add(row.dDateKey, row.dYear);
                }
                sw.Stop();
                testResults.phase11HashTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                partDimension = Utils.ReadFromBinaryFiles<Part>(partFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase12IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                foreach (var row in partDimension)
                {
                    if (row.pCategory.Equals("MFGR#12"))
                        partHashTable.Add(row.pPartKey, row.pBrand);
                }
                sw.Stop();
                testResults.phase12HashTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase13IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                foreach (var row in supplierDimension)
                {
                    if (row.sRegion.Equals("AMERICA"))
                        supplierHashTable.Add(row.sSuppKey, row.sNation);
                }
                sw.Stop();
                testResults.phase13HashTime = sw.ElapsedMilliseconds;
                testResults.phase1Time = testResults.phase11HashTime + testResults.phase11IOTime +
                    testResults.phase12HashTime + testResults.phase12IOTime +
                    testResults.phase13HashTime + testResults.phase13IOTime;
                sw.Reset();

                partDimension.Clear();
                dateDimension.Clear();
                supplierDimension.Clear();

                long memoryUsedPhase1 = GC.GetTotalMemory(true) - memoryStartPhase1;
                #endregion Key Hashing Phase

                #region Probing Phase
                long memoryStartPhase2 = GC.GetTotalMemory(true);
                sw.Start();
                List<int> loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));

                sw.Stop();
                testResults.phase21IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var _maat = new MAAT(loSupplierKey.Count);
                var k = 0;
                foreach (var suppKey in loSupplierKey)
                {
                    string sNationOut = string.Empty;
                    Record record = new Record();
                    if (supplierHashTable.TryGetValue(suppKey, out sNationOut))
                    {
                        record.s1 = sNationOut;
                        _maat.AddOrUpdate(k, record);
                    }
                    k++;
                }
                sw.Stop();
                testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<int> loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));

                sw.Stop();
                testResults.phase22IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var i = 0;
                foreach (var orderDate in loOrderDate)
                {
                    string dYear = "";
                    if (dateHashTable.TryGetValue(orderDate, out dYear))
                    {
                        Record record = _maat.GetValue(i);
                        if (record != null)
                        {
                            record.s2 = dYear;
                            _maat.AddOrUpdate(i, record);
                        }
                    }
                    else
                    {
                        _maat.Remove(i);
                    }
                    i++;
                }

                sw.Stop();
                testResults.phase22ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<int> loPartKey = Utils.ReadFromBinaryFiles<int>(loPartKeyFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase23IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var j = 0;
                foreach (var partKey in loPartKey)
                {
                    string pBrandOut = string.Empty;
                    if (partHashTable.TryGetValue(partKey, out pBrandOut))
                    {
                        Record record = _maat.GetValue(j);
                        if (record != null)
                        {
                            record.s3 = pBrandOut;
                            _maat.AddOrUpdate(j, record);
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
                            _maat.positions.Add(j);
                        }
                    }
                    else
                    {
                        _maat.Remove(j);
                    }
                    j++;
                }

                sw.Stop();
                testResults.phase23ProbeTime = sw.ElapsedMilliseconds;
                testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime +
                    testResults.phase22IOTime + testResults.phase22ProbeTime +
                    testResults.phase23IOTime + testResults.phase23ProbeTime;
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

                var joinOutputFinal = new MAAT(_maat.positions.Count);
                var o = 0;
                foreach (var postion in _maat.positions)
                {
                    var item = _maat.GetValue(postion);
                    item.i1 = loRevenue[postion];
                    joinOutputFinal.AddOrUpdate(o, item);
                    o++;
                }
                sw.Stop();
                long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
                #endregion Value Extraction Phase
                testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
                testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
                testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
                //Console.WriteLine("[Invisble Join]: Time taken {0} ms.", sw.ElapsedMilliseconds);
                testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
                testResults.totalNumberOfOutput = joinOutputFinal.Count();
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
                dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase11IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                foreach (var row in dateDimension)
                {
                    dateHashTable.Add(row.dDateKey, row.dYear);
                }
                sw.Stop();
                testResults.phase11HashTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                partDimension = Utils.ReadFromBinaryFiles<Part>(partFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase12IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                foreach (var row in partDimension)
                {
                    if (String.CompareOrdinal(row.pBrand, "MFGR#2221") >= 0 && String.CompareOrdinal(row.pBrand, "MFGR#2228") <= 0)
                        partHashTable.Add(row.pPartKey, row.pBrand);
                }
                sw.Stop();
                testResults.phase12HashTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase13IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                foreach (var row in supplierDimension)
                {
                    if (row.sRegion.Equals("ASIA"))
                        supplierHashTable.Add(row.sSuppKey, row.sNation);
                }
                sw.Stop();
                testResults.phase13HashTime = sw.ElapsedMilliseconds;
                testResults.phase1Time = testResults.phase11HashTime + testResults.phase11IOTime +
                    testResults.phase12HashTime + testResults.phase12IOTime +
                    testResults.phase13HashTime + testResults.phase13IOTime;
                sw.Reset();

                partDimension.Clear();
                dateDimension.Clear();
                supplierDimension.Clear();

                long memoryUsedPhase1 = GC.GetTotalMemory(true) - memoryStartPhase1;
                #endregion Key Hashing Phase

                #region Probing Phase
                long memoryStartPhase2 = GC.GetTotalMemory(true);
                sw.Start();
                List<int> loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));

                sw.Stop();
                testResults.phase21IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var _maat = new MAAT(loSupplierKey.Count);
                var k = 0;
                foreach (var suppKey in loSupplierKey)
                {
                    string sNationOut = string.Empty;
                    Record record = new Record();
                    if (supplierHashTable.TryGetValue(suppKey, out sNationOut))
                    {
                        record.s1 = sNationOut;
                        _maat.AddOrUpdate(k, record);
                    }
                    k++;
                }
                sw.Stop();
                testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<int> loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));

                sw.Stop();
                testResults.phase22IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var i = 0;
                foreach (var orderDate in loOrderDate)
                {
                    string dYear = "";
                    if (dateHashTable.TryGetValue(orderDate, out dYear))
                    {
                        Record record = _maat.GetValue(i);
                        if (record != null)
                        {
                            record.s2 = dYear;
                            _maat.AddOrUpdate(i, record);
                        }
                    }
                    else
                    {
                        _maat.Remove(i);
                    }
                    i++;
                }

                sw.Stop();
                testResults.phase22ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<int> loPartKey = Utils.ReadFromBinaryFiles<int>(loPartKeyFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase23IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var j = 0;
                foreach (var partKey in loPartKey)
                {
                    string pBrandOut = string.Empty;
                    if (partHashTable.TryGetValue(partKey, out pBrandOut))
                    {
                        Record record = _maat.GetValue(j);
                        if (record != null)
                        {
                            record.s3 = pBrandOut;
                            _maat.AddOrUpdate(j, record);
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
                            _maat.positions.Add(j);
                        }
                    }
                    else
                    {
                        _maat.Remove(j);
                    }
                    j++;
                }

                sw.Stop();
                testResults.phase23ProbeTime = sw.ElapsedMilliseconds;
                testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime +
                    testResults.phase22IOTime + testResults.phase22ProbeTime +
                    testResults.phase23IOTime + testResults.phase23ProbeTime;
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

                var joinOutputFinal = new MAAT(_maat.positions.Count);
                var o = 0;
                foreach (var postion in _maat.positions)
                {
                    var item = _maat.GetValue(postion);
                    item.i1 = loRevenue[postion];
                    joinOutputFinal.AddOrUpdate(o, item);
                    o++;
                }
                sw.Stop();
                long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
                #endregion Value Extraction Phase
                testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
                testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
                testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
                //Console.WriteLine("[Invisble Join]: Time taken {0} ms.", sw.ElapsedMilliseconds);
                testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
                testResults.totalNumberOfOutput = joinOutputFinal.Count();
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
                dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase11IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                foreach (var row in dateDimension)
                {
                    dateHashTable.Add(row.dDateKey, row.dYear);
                }
                sw.Stop();
                testResults.phase11HashTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                partDimension = Utils.ReadFromBinaryFiles<Part>(partFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase12IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                foreach (var row in partDimension)
                {
                    if (row.pBrand.Equals("MFGR#2221"))
                        partHashTable.Add(row.pPartKey, row.pBrand);
                }
                sw.Stop();
                testResults.phase12HashTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase13IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                foreach (var row in supplierDimension)
                {
                    if (row.sRegion.Equals("EUROPE"))
                        supplierHashTable.Add(row.sSuppKey, row.sNation);
                }
                sw.Stop();
                testResults.phase13HashTime = sw.ElapsedMilliseconds;
                testResults.phase1Time = testResults.phase11HashTime + testResults.phase11IOTime +
                    testResults.phase12HashTime + testResults.phase12IOTime +
                    testResults.phase13HashTime + testResults.phase13IOTime;
                sw.Reset();

                partDimension.Clear();
                dateDimension.Clear();
                supplierDimension.Clear();

                long memoryUsedPhase1 = GC.GetTotalMemory(true) - memoryStartPhase1;
                #endregion Key Hashing Phase

                #region Probing Phase
                long memoryStartPhase2 = GC.GetTotalMemory(true);
                sw.Start();
                List<int> loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));

                sw.Stop();
                testResults.phase21IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var _maat = new MAAT(loSupplierKey.Count);
                var k = 0;
                foreach (var suppKey in loSupplierKey)
                {
                    string sNationOut = string.Empty;
                    Record record = new Record();
                    if (supplierHashTable.TryGetValue(suppKey, out sNationOut))
                    {
                        record.s1 = sNationOut;
                        _maat.AddOrUpdate(k, record);
                    }
                    k++;
                }
                sw.Stop();
                testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<int> loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));

                sw.Stop();
                testResults.phase22IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var i = 0;
                foreach (var orderDate in loOrderDate)
                {
                    string dYear = "";
                    if (dateHashTable.TryGetValue(orderDate, out dYear))
                    {
                        Record record = _maat.GetValue(i);
                        if (record != null)
                        {
                            record.s2 = dYear;
                            _maat.AddOrUpdate(i, record);
                        }
                    }
                    else
                    {
                        _maat.Remove(i);
                    }
                    i++;
                }

                sw.Stop();
                testResults.phase22ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<int> loPartKey = Utils.ReadFromBinaryFiles<int>(loPartKeyFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase23IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var j = 0;
                foreach (var partKey in loPartKey)
                {
                    string pBrandOut = string.Empty;
                    if (partHashTable.TryGetValue(partKey, out pBrandOut))
                    {
                        Record record = _maat.GetValue(j);
                        if (record != null)
                        {
                            record.s3 = pBrandOut;
                            _maat.AddOrUpdate(j, record);
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
                            _maat.positions.Add(j);
                        }
                    }
                    else
                    {
                        _maat.Remove(j);
                    }
                    j++;
                }

                sw.Stop();
                testResults.phase23ProbeTime = sw.ElapsedMilliseconds;
                testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime +
                    testResults.phase22IOTime + testResults.phase22ProbeTime +
                    testResults.phase23IOTime + testResults.phase23ProbeTime;
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

                var joinOutputFinal = new MAAT(_maat.positions.Count);
                var o = 0;
                foreach (var postion in _maat.positions)
                {
                    var item = _maat.GetValue(postion);
                    item.i1 = loRevenue[postion];
                    joinOutputFinal.AddOrUpdate(o, item);
                    o++;
                }
                sw.Stop();
                long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
                #endregion Value Extraction Phase
                testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
                testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
                testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
                //Console.WriteLine("[Invisble Join]: Time taken {0} ms.", sw.ElapsedMilliseconds);
                testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
                testResults.totalNumberOfOutput = joinOutputFinal.Count();
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
                var customerHashTable = new Dictionary<int, string>();
                var supplierHashTable = new Dictionary<int, string>();
                var partHashTable = new Dictionary<int, string>();
                Stopwatch sw = new Stopwatch();
                Stopwatch swInitialRecorder = new Stopwatch();
                Stopwatch swOutputRecorder = new Stopwatch();

                #region Key Hashing Phase
                List<Date> dateDimension = null;
                List<Supplier> supplierDimension = null;
                List<Customer> customerDimension = null;
                List<Part> partDimension = null;

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
                    // if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
                    dateHashTable.Add(row.dDateKey, row.dYear);
                }
                sw.Stop();
                testResults.phase11HashTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase12IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                foreach (var row in customerDimension)
                {
                    if (row.cRegion.Equals("AMERICA"))
                        customerHashTable.Add(row.cCustKey, row.cNation);
                }
                sw.Stop();
                testResults.phase12HashTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase13IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                foreach (var row in supplierDimension)
                {
                    if (row.sRegion.Equals("AMERICA"))
                        supplierHashTable.Add(row.sSuppKey, row.sNation);
                }
                sw.Stop();
                testResults.phase13HashTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                partDimension = Utils.ReadFromBinaryFiles<Part>(partFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase14IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                foreach (var row in partDimension)
                {
                    if (row.pMFGR.Equals("MFGR#1") || row.pMFGR.Equals("MFGR#2"))
                        partHashTable.Add(row.pPartKey, row.pMFGR);
                }
                sw.Stop();
                testResults.phase14HashTime = sw.ElapsedMilliseconds;

                testResults.phase1Time = testResults.phase11HashTime + testResults.phase11IOTime +
                    testResults.phase12HashTime + testResults.phase12IOTime +
                    testResults.phase13HashTime + testResults.phase13IOTime + testResults.phase14HashTime + testResults.phase14IOTime;
                sw.Reset();

                customerDimension.Clear();
                dateDimension.Clear();
                supplierDimension.Clear();

                long memoryUsedPhase1 = GC.GetTotalMemory(true) - memoryStartPhase1;
                #endregion Key Hashing Phase

                #region Probing Phase
                long memoryStartPhase2 = GC.GetTotalMemory(true);
                sw.Start();
                List<int> loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase21IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                //var listSupplierKeyPositions = new List<int>();
                var _maat = new MAAT(loSupplierKey.Count);
                var k = 0;
                foreach (var suppKey in loSupplierKey)
                {
                    string sNationOut = string.Empty;
                    Record record = new Record();
                    if (supplierHashTable.TryGetValue(suppKey, out sNationOut))
                    {
                        //record.s1 = sNationOut;
                        _maat.AddOrUpdate(k, record);
                    }
                    k++;
                }
                sw.Stop();
                testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<int> loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase22IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var i = 0;
                foreach (var orderDate in loOrderDate)
                {
                    string dYear = "";
                    if (dateHashTable.TryGetValue(orderDate, out dYear))
                    {
                        Record record = _maat.GetValue(i);
                        if (record != null)
                        {
                            record.s2 = dYear;
                            _maat.AddOrUpdate(i, record);
                        }
                    }
                    else
                    {
                        _maat.Remove(i);
                    }
                    i++;
                }
                sw.Stop();
                testResults.phase22ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<int> loCustomerKey = Utils.ReadFromBinaryFiles<int>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase23IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var j = 0;
                foreach (var custKey in loCustomerKey)
                {
                    string cNationOut = string.Empty;
                    if (customerHashTable.TryGetValue(custKey, out cNationOut))
                    {
                        Record record = _maat.GetValue(j);
                        if (record != null)
                        {
                            record.s3 = cNationOut;
                            _maat.AddOrUpdate(j, record);
                        }
                    }
                    else
                    {
                        _maat.Remove(j);
                    }
                    j++;
                }
                sw.Stop();
                testResults.phase23ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();



                sw.Start();
                List<int> loPartKey = Utils.ReadFromBinaryFiles<int>(loPartKeyFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase24IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var l = 0;
                foreach (var partKey in loPartKey)
                {
                    string pMFGR = string.Empty;
                    if (partHashTable.TryGetValue(partKey, out pMFGR))
                    {
                        Record record = _maat.GetValue(l);
                        if (record != null)
                        {
                            // record.s1 = pMFGR;
                            // _maat.AddOrUpdate(l,record); // dont need it as we are not updating any attribute
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
                            _maat.positions.Add(l);
                        }
                    }
                    else
                    {
                        _maat.Remove(l);
                    }
                    l++;
                }
                sw.Stop();
                testResults.phase24ProbeTime = sw.ElapsedMilliseconds;
                testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime +
                    testResults.phase22IOTime + testResults.phase22ProbeTime +
                    testResults.phase23IOTime + testResults.phase23ProbeTime +
                    testResults.phase24IOTime + testResults.phase24ProbeTime;
                sw.Reset();

                loOrderDate.Clear();
                dateHashTable.Clear();
                loCustomerKey.Clear();
                customerHashTable.Clear();
                loSupplierKey.Clear();
                supplierHashTable.Clear();
                loPartKey.Clear();
                partHashTable.Clear();

                long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
                #endregion Probing Phase


                #region Value Extraction Phase
                long memoryStartPhase3 = GC.GetTotalMemory(true);
                sw.Start();
                // List<string> sNation = Utils.ReadFromBinaryFiles<string>(sNationFile.Replace("BF", "BF" + scaleFactor));
                List<int> loRevenue = Utils.ReadFromBinaryFiles<int>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
                List<int> loSupplyCost = Utils.ReadFromBinaryFiles<int>(loSupplyCostFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase3IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();

                var joinOutputFinal = new MAAT(_maat.positions.Count);
                var o = 0;
                foreach (var postion in _maat.positions)
                {
                    var item = _maat.GetValue(postion);
                    item.i1 = loRevenue[postion];
                    item.i2 = loSupplyCost[postion];
                    joinOutputFinal.AddOrUpdate(o, item);
                    o++;
                }

                sw.Stop();
                long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
                #endregion Value Extraction Phase
                testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
                testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
                testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
                //Console.WriteLine("[Invisble Join]: Time taken {0} ms.", sw.ElapsedMilliseconds);
                testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
                testResults.totalNumberOfOutput = joinOutputFinal.Count();
            }
            catch (Exception ex)
            {
                throw;
            }
        }
        public void Query_4_2()
        {
            try
            {
                long memoryStartPhase1 = GC.GetTotalMemory(true);
                outputRecordsCounter = 0;
                var dateHashTable = new Dictionary<int, string>();
                var customerHashTable = new Dictionary<int, string>();
                var supplierHashTable = new Dictionary<int, string>();
                var partHashTable = new Dictionary<int, string>();
                Stopwatch sw = new Stopwatch();
                Stopwatch swInitialRecorder = new Stopwatch();
                Stopwatch swOutputRecorder = new Stopwatch();

                #region Key Hashing Phase
                List<Date> dateDimension = null;
                List<Supplier> supplierDimension = null;
                List<Customer> customerDimension = null;
                List<Part> partDimension = null;

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
                    if (row.dYear.Equals("1997") || row.dYear.Equals("1998"))
                        dateHashTable.Add(row.dDateKey, row.dYear);
                }
                sw.Stop();
                testResults.phase11HashTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase12IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                foreach (var row in customerDimension)
                {
                    if (row.cRegion.Equals("AMERICA"))
                        customerHashTable.Add(row.cCustKey, row.cNation);
                }
                sw.Stop();
                testResults.phase12HashTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase13IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                foreach (var row in supplierDimension)
                {
                    if (row.sRegion.Equals("AMERICA"))
                        supplierHashTable.Add(row.sSuppKey, row.sNation);
                }
                sw.Stop();
                testResults.phase13HashTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                partDimension = Utils.ReadFromBinaryFiles<Part>(partFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase14IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                foreach (var row in partDimension)
                {
                    if (row.pMFGR.Equals("MFGR#1") || row.pMFGR.Equals("MFGR#2"))
                        partHashTable.Add(row.pPartKey, row.pCategory);
                }
                sw.Stop();
                testResults.phase14HashTime = sw.ElapsedMilliseconds;

                testResults.phase1Time = testResults.phase11HashTime + testResults.phase11IOTime +
                    testResults.phase12HashTime + testResults.phase12IOTime +
                    testResults.phase13HashTime + testResults.phase13IOTime + testResults.phase14HashTime + testResults.phase14IOTime;
                sw.Reset();

                customerDimension.Clear();
                dateDimension.Clear();
                supplierDimension.Clear();

                long memoryUsedPhase1 = GC.GetTotalMemory(true) - memoryStartPhase1;
                #endregion Key Hashing Phase

                #region Probing Phase
                long memoryStartPhase2 = GC.GetTotalMemory(true);
                sw.Start();
                List<int> loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase21IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                //var listSupplierKeyPositions = new List<int>();
                var _maat = new MAAT(loSupplierKey.Count);
                var k = 0;
                foreach (var suppKey in loSupplierKey)
                {
                    string sNationOut = string.Empty;
                    Record record = new Record();
                    if (supplierHashTable.TryGetValue(suppKey, out sNationOut))
                    {
                        record.s1 = sNationOut;
                        _maat.AddOrUpdate(k, record);
                    }
                    k++;
                }
                sw.Stop();
                testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<int> loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase22IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var i = 0;
                foreach (var orderDate in loOrderDate)
                {
                    string dYear = "";
                    if (dateHashTable.TryGetValue(orderDate, out dYear))
                    {
                        Record record = _maat.GetValue(i);
                        if (record != null)
                        {
                            record.s2 = dYear;
                            _maat.AddOrUpdate(i, record);
                        }
                    }
                    else
                    {
                        _maat.Remove(i);
                    }
                    i++;
                }
                sw.Stop();
                testResults.phase22ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<int> loCustomerKey = Utils.ReadFromBinaryFiles<int>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase23IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var j = 0;
                foreach (var custKey in loCustomerKey)
                {
                    string cNationOut = string.Empty;
                    if (customerHashTable.TryGetValue(custKey, out cNationOut))
                    {
                        Record record = _maat.GetValue(j);
                        if (record != null)
                        {
                            //record.s3 = cNationOut;
                            //_maat.AddOrUpdate(j, record);
                        }
                    }
                    else
                    {
                        _maat.Remove(j);
                    }
                    j++;
                }
                sw.Stop();
                testResults.phase23ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();



                sw.Start();
                List<int> loPartKey = Utils.ReadFromBinaryFiles<int>(loPartKeyFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase24IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var l = 0;
                foreach (var partKey in loPartKey)
                {
                    string pCategory = string.Empty;
                    if (partHashTable.TryGetValue(partKey, out pCategory))
                    {
                        Record record = _maat.GetValue(l);
                        if (record != null)
                        {
                            record.s3 = pCategory;
                            _maat.AddOrUpdate(l, record);
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
                            _maat.positions.Add(l);
                        }
                    }
                    else
                    {
                        _maat.Remove(l);
                    }
                    l++;
                }
                sw.Stop();
                testResults.phase24ProbeTime = sw.ElapsedMilliseconds;
                testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime +
                    testResults.phase22IOTime + testResults.phase22ProbeTime +
                    testResults.phase23IOTime + testResults.phase23ProbeTime +
                    testResults.phase24IOTime + testResults.phase24ProbeTime;
                sw.Reset();

                loOrderDate.Clear();
                dateHashTable.Clear();
                loCustomerKey.Clear();
                customerHashTable.Clear();
                loSupplierKey.Clear();
                supplierHashTable.Clear();
                loPartKey.Clear();
                partHashTable.Clear();

                long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
                #endregion Probing Phase


                #region Value Extraction Phase
                long memoryStartPhase3 = GC.GetTotalMemory(true);
                sw.Start();
                // List<string> sNation = Utils.ReadFromBinaryFiles<string>(sNationFile.Replace("BF", "BF" + scaleFactor));
                List<int> loRevenue = Utils.ReadFromBinaryFiles<int>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
                List<int> loSupplyCost = Utils.ReadFromBinaryFiles<int>(loSupplyCostFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase3IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();

                var joinOutputFinal = new MAAT(_maat.positions.Count);
                var o = 0;
                foreach (var postion in _maat.positions)
                {
                    var item = _maat.GetValue(postion);
                    item.i1 = loRevenue[postion];
                    item.i2 = loSupplyCost[postion];
                    joinOutputFinal.AddOrUpdate(o, item);
                    o++;
                }

                sw.Stop();
                long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
                #endregion Value Extraction Phase
                testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
                testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
                testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
                //Console.WriteLine("[Invisble Join]: Time taken {0} ms.", sw.ElapsedMilliseconds);
                testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
                testResults.totalNumberOfOutput = joinOutputFinal.Count();
            }
            catch (Exception ex)
            {
                throw;
            }
        }
        public void Query_4_3()
        {
            try
            {
                long memoryStartPhase1 = GC.GetTotalMemory(true);
                outputRecordsCounter = 0;
                var dateHashTable = new Dictionary<int, string>();
                var customerHashTable = new Dictionary<int, string>();
                var supplierHashTable = new Dictionary<int, string>();
                var partHashTable = new Dictionary<int, string>();
                Stopwatch sw = new Stopwatch();
                Stopwatch swInitialRecorder = new Stopwatch();
                Stopwatch swOutputRecorder = new Stopwatch();

                #region Key Hashing Phase
                List<Date> dateDimension = null;
                List<Supplier> supplierDimension = null;
                List<Customer> customerDimension = null;
                List<Part> partDimension = null;

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
                    if (row.dYear.Equals("1997") || row.dYear.Equals("1998"))
                        dateHashTable.Add(row.dDateKey, row.dYear);
                }
                sw.Stop();
                testResults.phase11HashTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase12IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                foreach (var row in customerDimension)
                {
                    if (row.cRegion.Equals("AMERICA"))
                        customerHashTable.Add(row.cCustKey, row.cNation);
                }
                sw.Stop();
                testResults.phase12HashTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase13IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                foreach (var row in supplierDimension)
                {
                    if (row.sNation.Equals("UNITED STATES"))
                        supplierHashTable.Add(row.sSuppKey, row.sCity);
                }
                sw.Stop();
                testResults.phase13HashTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                partDimension = Utils.ReadFromBinaryFiles<Part>(partFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase14IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                foreach (var row in partDimension)
                {
                    if (row.pCategory.Equals("MFGR#14"))
                        partHashTable.Add(row.pPartKey, row.pBrand);
                }
                sw.Stop();
                testResults.phase14HashTime = sw.ElapsedMilliseconds;

                testResults.phase1Time = testResults.phase11HashTime + testResults.phase11IOTime +
                    testResults.phase12HashTime + testResults.phase12IOTime +
                    testResults.phase13HashTime + testResults.phase13IOTime + testResults.phase14HashTime + testResults.phase14IOTime;
                sw.Reset();

                customerDimension.Clear();
                dateDimension.Clear();
                supplierDimension.Clear();

                long memoryUsedPhase1 = GC.GetTotalMemory(true) - memoryStartPhase1;
                #endregion Key Hashing Phase

                #region Probing Phase
                long memoryStartPhase2 = GC.GetTotalMemory(true);
                sw.Start();
                List<int> loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase21IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                //var listSupplierKeyPositions = new List<int>();
                var _maat = new MAAT(loSupplierKey.Count);
                var k = 0;
                foreach (var suppKey in loSupplierKey)
                {
                    string sCityOut = string.Empty;
                    Record record = new Record();
                    if (supplierHashTable.TryGetValue(suppKey, out sCityOut))
                    {
                        record.s1 = sCityOut;
                        _maat.AddOrUpdate(k, record);
                    }
                    k++;
                }
                sw.Stop();
                testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<int> loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase22IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var i = 0;
                foreach (var orderDate in loOrderDate)
                {
                    string dYear = "";
                    if (dateHashTable.TryGetValue(orderDate, out dYear))
                    {
                        Record record = _maat.GetValue(i);
                        if (record != null)
                        {
                            record.s2 = dYear;
                            _maat.AddOrUpdate(i, record);
                        }
                    }
                    else
                    {
                        _maat.Remove(i);
                    }
                    i++;
                }
                sw.Stop();
                testResults.phase22ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<int> loCustomerKey = Utils.ReadFromBinaryFiles<int>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase23IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var j = 0;
                foreach (var custKey in loCustomerKey)
                {
                    string cNationOut = string.Empty;
                    if (customerHashTable.TryGetValue(custKey, out cNationOut))
                    {
                        Record record = _maat.GetValue(j);
                        if (record != null)
                        {
                            //record.s3 = cNationOut;
                            //_maat.AddOrUpdate(j, record);
                        }
                    }
                    else
                    {
                        _maat.Remove(j);
                    }
                    j++;
                }
                sw.Stop();
                testResults.phase23ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();



                sw.Start();
                List<int> loPartKey = Utils.ReadFromBinaryFiles<int>(loPartKeyFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase24IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var l = 0;
                foreach (var partKey in loPartKey)
                {
                    string pBrand = string.Empty;
                    if (partHashTable.TryGetValue(partKey, out pBrand))
                    {
                        Record record = _maat.GetValue(l);
                        if (record != null)
                        {
                            record.s3 = pBrand;
                            _maat.AddOrUpdate(l, record);
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
                            _maat.positions.Add(l);
                        }
                    }
                    else
                    {
                        _maat.Remove(l);
                    }
                    l++;
                }
                sw.Stop();
                testResults.phase24ProbeTime = sw.ElapsedMilliseconds;
                testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime +
                    testResults.phase22IOTime + testResults.phase22ProbeTime +
                    testResults.phase23IOTime + testResults.phase23ProbeTime +
                    testResults.phase24IOTime + testResults.phase24ProbeTime;
                sw.Reset();

                loOrderDate.Clear();
                dateHashTable.Clear();
                loCustomerKey.Clear();
                customerHashTable.Clear();
                loSupplierKey.Clear();
                supplierHashTable.Clear();
                loPartKey.Clear();
                partHashTable.Clear();

                long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
                #endregion Probing Phase


                #region Value Extraction Phase
                long memoryStartPhase3 = GC.GetTotalMemory(true);
                sw.Start();
                // List<string> sNation = Utils.ReadFromBinaryFiles<string>(sNationFile.Replace("BF", "BF" + scaleFactor));
                List<int> loRevenue = Utils.ReadFromBinaryFiles<int>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
                List<int> loSupplyCost = Utils.ReadFromBinaryFiles<int>(loSupplyCostFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase3IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();

                var joinOutputFinal = new MAAT(_maat.positions.Count);
                var o = 0;
                foreach (var postion in _maat.positions)
                {
                    var item = _maat.GetValue(postion);
                    item.i1 = loRevenue[postion];
                    item.i2 = loSupplyCost[postion];
                    joinOutputFinal.AddOrUpdate(o, item);
                    o++;
                }

                sw.Stop();
                long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
                #endregion Value Extraction Phase
                testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
                testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
                testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
                //Console.WriteLine("[Invisble Join]: Time taken {0} ms.", sw.ElapsedMilliseconds);
                testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
                testResults.totalNumberOfOutput = joinOutputFinal.Count();
            }
            catch (Exception ex)
            {
                throw;
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
                dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase11IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                foreach (var row in dateDimension)
                {
                    if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
                        dateHashTable.Add(row.dDateKey, row.dYear);
                }
                sw.Stop();
                testResults.phase11HashTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase12IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                foreach (var row in customerDimension)
                {
                    if (row.cRegion.Equals("ASIA"))
                        customerHashTable.Add(row.cCustKey, row.cNation);
                }
                sw.Stop();
                testResults.phase12HashTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase13IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                foreach (var row in supplierDimension)
                {
                    if (row.sRegion.Equals("ASIA"))
                        supplierHashTable.Add(row.sSuppKey, row.sNation);
                }
                sw.Stop();
                testResults.phase13HashTime = sw.ElapsedMilliseconds;
                testResults.phase1Time = testResults.phase11HashTime + testResults.phase11IOTime +
                    testResults.phase12HashTime + testResults.phase12IOTime +
                    testResults.phase13HashTime + testResults.phase13IOTime;
                sw.Reset();

                customerDimension.Clear();
                dateDimension.Clear();
                supplierDimension.Clear();

                long memoryUsedPhase1 = GC.GetTotalMemory(true) - memoryStartPhase1;
                #endregion Key Hashing Phase

                #region Probing Phase
                long memoryStartPhase2 = GC.GetTotalMemory(true);
                sw.Start();
                List<int> loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));

                sw.Stop();
                testResults.phase21IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var _maat = new MAAT(loSupplierKey.Count);
                var i = 0;
                foreach (var suppKey in loSupplierKey)
                {
                    string sNation = string.Empty;
                    Record record = new Record();
                    if (supplierHashTable.TryGetValue(suppKey, out sNation))
                    {
                        record.s1 = sNation;
                        _maat.AddOrUpdate(i, record);
                    }
                    i++;
                }

                sw.Stop();
                testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();
                sw.Start();
                List<int> loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase22IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();

                var k = 0;
                foreach (var orderDate in loOrderDate)
                {
                    string dYear = string.Empty;
                    if (dateHashTable.TryGetValue(orderDate, out dYear))
                    {
                        Record outValues = _maat.GetValue(k);
                        if (outValues != null)
                        {
                            outValues.s2 = dYear;
                            _maat.AddOrUpdate(k, outValues);
                        }
                    }
                    else
                    {
                        _maat.Remove(k);
                    }
                    k++;
                }

                sw.Stop();
                testResults.phase22ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<int> loCustomerKey = Utils.ReadFromBinaryFiles<int>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase23IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var j = 0;
                foreach (var custKey in loCustomerKey)
                {
                    string cNation = string.Empty;
                    if (customerHashTable.TryGetValue(custKey, out cNation))
                    {
                        string values = string.Empty;
                        Record outValues = _maat.GetValue(j);
                        if (outValues != null)
                        {
                            outValues.s3 = cNation;
                            _maat.AddOrUpdate(j, outValues);
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
                            _maat.positions.Add(j);
                        }
                    }
                    else
                    {
                        _maat.Remove(j);
                    }
                    j++;
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
                var joinOutputFinal = new MAAT(_maat.positions.Count);
                var o = 0;
                foreach (var postion in _maat.positions)
                {
                    var item = _maat.GetValue(postion);
                    item.i1 = loRevenue[postion];
                    joinOutputFinal.AddOrUpdate(o, item);

                    o++;
                }
                sw.Stop();

                long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
                #endregion Value Extraction Phase
                testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
                testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
                testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
                // Console.WriteLine("[Nimble Join]: Time taken {0} ms.", testResults.totalExecutionTime);
                testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
                testResults.totalNumberOfOutput = joinOutputFinal.Count();
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
                Console.WriteLine(String.Format("[Nimble Join] T0 Time: {0}", t0));
                sw.Reset();
                #endregion Key Hashing Phase

                var _maat = new MAATIM(loSupplierKey.Count);

                #region Probing Phase
                sw.Start();
                for (int i = 0; i < loCustomerKey.Count; i++)
                {
                    int custKey = loCustomerKey[i];
                    int suppKey = loSupplierKey[i];
                    int orderDate = loOrderDate[i];
                    string cNation = string.Empty;
                    string sNation = string.Empty;
                    string dYear = string.Empty;
                    if (customerHashTable.TryGetValue(custKey, out cNation)
                        && supplierHashTable.TryGetValue(suppKey, out sNation)
                        && dateHashTable.TryGetValue(orderDate, out dYear))
                    {
                        _maat.AddOrUpdate(i, new List<object> { cNation, sNation, dYear, loRevenue[i] });
                    }
                }

                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[Nimble Join] T1 Time: {0}", t1));
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
                Console.WriteLine(String.Format("[Nimble Join] T2 Time: {0}", t2));
                Console.WriteLine(String.Format("[Nimble Join] Total Time: {0}", t0 + t1 + t2));
                //Console.WriteLine(String.Format("[Nimble Join] Total Count: {0}", joinOutputFinal.Count()));
                Console.WriteLine();
                #endregion Value Extraction Phase
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void Query_3_1_BitMap()
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
                dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase11IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                foreach (var row in dateDimension)
                {
                    if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
                        dateHashTable.Add(row.dDateKey, row.dYear);
                }
                sw.Stop();
                testResults.phase11HashTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase12IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                foreach (var row in customerDimension)
                {
                    if (row.cRegion.Equals("ASIA"))
                        customerHashTable.Add(row.cCustKey, row.cNation);
                }
                sw.Stop();
                testResults.phase12HashTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase13IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                foreach (var row in supplierDimension)
                {
                    if (row.sRegion.Equals("ASIA"))
                        supplierHashTable.Add(row.sSuppKey, row.sNation);
                }
                sw.Stop();
                testResults.phase13HashTime = sw.ElapsedMilliseconds;
                testResults.phase1Time = testResults.phase11HashTime + testResults.phase11IOTime +
                    testResults.phase12HashTime + testResults.phase12IOTime +
                    testResults.phase13HashTime + testResults.phase13IOTime;
                sw.Reset();

                customerDimension.Clear();
                dateDimension.Clear();
                supplierDimension.Clear();

                long memoryUsedPhase1 = GC.GetTotalMemory(true) - memoryStartPhase1;
                #endregion Key Hashing Phase

                #region Probing Phase
                long memoryStartPhase2 = GC.GetTotalMemory(true);
                sw.Start();
                List<int> loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));

                sw.Stop();
                testResults.phase21IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var _maat = new MAATB(loSupplierKey.Count);
                var i = 0;
                foreach (var suppKey in loSupplierKey)
                {
                    string sNation = string.Empty;
                    Record record = new Record();
                    if (supplierHashTable.TryGetValue(suppKey, out sNation))
                    {
                        record.s1 = sNation;
                        _maat.AddOrUpdate(i, record);
                        _maat.bitMap.Set(i, true);
                    }
                    i++;
                }

                sw.Stop();
                testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();
                sw.Start();
                List<int> loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase22IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();

                var k = 0;
                foreach (var orderDate in loOrderDate)
                {
                    string dYear = string.Empty;
                    if (dateHashTable.TryGetValue(orderDate, out dYear))
                    {
                        if (_maat.bitMap.Get(k))
                        {
                            Record outValues = _maat.GetValue(k);
                            outValues.s2 = dYear;
                            _maat.AddOrUpdate(k, outValues);
                        }
                    }
                    else
                    {
                        _maat.bitMap.Set(k, false);
                        _maat.Remove(k);
                    }
                    k++;
                }

                sw.Stop();
                testResults.phase22ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<int> loCustomerKey = Utils.ReadFromBinaryFiles<int>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase23IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var j = 0;
                foreach (var custKey in loCustomerKey)
                {
                    string cNation = string.Empty;
                    if (customerHashTable.TryGetValue(custKey, out cNation))
                    {
                        if (_maat.bitMap.Get(j))
                        {
                            Record outValues = _maat.GetValue(j);
                            outValues.s3 = cNation;
                            _maat.AddOrUpdate(j, outValues);
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
                        _maat.bitMap.Set(j, false);
                        _maat.Remove(j);
                    }
                    j++;
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
                var joinOutputFinal = new MAATB(_maat.bitMap.Count);
                var o = 0;
                foreach (bool postion in _maat.bitMap)
                {
                    if (postion)
                    {
                        var item = _maat.GetValue(o);
                        item.i1 = loRevenue[o];
                        joinOutputFinal.AddOrUpdate(o, item);
                    }
                    o++;
                }
                sw.Stop();

                long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
                #endregion Value Extraction Phase
                testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
                testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
                testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
                // Console.WriteLine("[Nimble Join]: Time taken {0} ms.", testResults.totalExecutionTime);
                testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
                testResults.totalNumberOfOutput = joinOutputFinal.Count();
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
                dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase11IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                foreach (var row in dateDimension)
                {
                    if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
                        dateHashTable.Add(row.dDateKey, row.dYear);
                }
                sw.Stop();
                testResults.phase11HashTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase12IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                foreach (var row in customerDimension)
                {
                    if (row.cNation.Equals("UNITED STATES"))
                        customerHashTable.Add(row.cCustKey, row.cCity);
                }
                sw.Stop();
                testResults.phase12HashTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase13IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                foreach (var row in supplierDimension)
                {
                    if (row.sNation.Equals("UNITED STATES"))
                        supplierHashTable.Add(row.sSuppKey, row.sCity);
                }
                sw.Stop();
                testResults.phase13HashTime = sw.ElapsedMilliseconds;
                testResults.phase1Time = testResults.phase11HashTime + testResults.phase11IOTime +
                    testResults.phase12HashTime + testResults.phase12IOTime +
                    testResults.phase13HashTime + testResults.phase13IOTime;
                sw.Reset();

                customerDimension.Clear();
                dateDimension.Clear();
                supplierDimension.Clear();

                long memoryUsedPhase1 = GC.GetTotalMemory(true) - memoryStartPhase1;
                #endregion Key Hashing Phase

                #region Probing Phase
                long memoryStartPhase2 = GC.GetTotalMemory(true);
                sw.Start();
                List<int> loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase21IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var _maat = new MAAT(loSupplierKey.Count);
                var i = 0;
                foreach (var suppKey in loSupplierKey)
                {
                    string sCity = string.Empty;
                    Record record = new Record();
                    if (supplierHashTable.TryGetValue(suppKey, out sCity))
                    {
                        record.s1 = sCity;
                        _maat.AddOrUpdate(i, record);
                    }
                    i++;
                }
                sw.Stop();
                testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<int> loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase22IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var k = 0;
                foreach (var orderDate in loOrderDate)
                {
                    string dYear = string.Empty;
                    if (dateHashTable.TryGetValue(orderDate, out dYear))
                    {
                        Record outValues = _maat.GetValue(k);
                        if (outValues != null)
                        {
                            outValues.s2 = dYear;
                            _maat.AddOrUpdate(k, outValues);
                        }
                    }
                    else
                    {
                        _maat.Remove(k);
                    }
                    k++;
                }
                sw.Stop();
                testResults.phase22ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<int> loCustomerKey = Utils.ReadFromBinaryFiles<int>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase23IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var j = 0;
                foreach (var custKey in loCustomerKey)
                {
                    string cCity = string.Empty;
                    if (customerHashTable.TryGetValue(custKey, out cCity))
                    {
                        string values = string.Empty;
                        Record outValues = _maat.GetValue(j);
                        if (outValues != null)
                        {
                            outValues.s3 = cCity;
                            _maat.AddOrUpdate(j, outValues);
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
                            _maat.positions.Add(j);
                        }
                    }
                    else
                    {
                        _maat.Remove(j);
                    }
                    j++;
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
                var joinOutputFinal = new MAAT(_maat.positions.Count);
                var o = 0;
                foreach (var postion in _maat.positions)
                {
                    var item = _maat.GetValue(postion);
                    item.i1 = loRevenue[postion];
                    joinOutputFinal.AddOrUpdate(o, item);
                    o++;
                }
                sw.Stop();

                long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
                #endregion Value Extraction Phase
                testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
                testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
                testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
                // Console.WriteLine("[Nimble Join]: Time taken {0} ms.", testResults.totalExecutionTime);
                testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
                testResults.totalNumberOfOutput = joinOutputFinal.Count();
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
                dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase11IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                foreach (var row in dateDimension)
                {
                    if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
                        dateHashTable.Add(row.dDateKey, row.dYear);
                }
                sw.Stop();
                testResults.phase11HashTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase12IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                foreach (var row in customerDimension)
                {
                    if (row.cCity.Equals("UNITED KI1") || row.cCity.Equals("UNITED KI5"))
                        customerHashTable.Add(row.cCustKey, row.cCity);
                }
                sw.Stop();
                testResults.phase12HashTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase13IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                foreach (var row in supplierDimension)
                {
                    if (row.sCity.Equals("UNITED KI1") || row.sCity.Equals("UNITED KI5"))
                        supplierHashTable.Add(row.sSuppKey, row.sCity);
                }
                sw.Stop();
                testResults.phase13HashTime = sw.ElapsedMilliseconds;
                testResults.phase1Time = testResults.phase11HashTime + testResults.phase11IOTime +
                    testResults.phase12HashTime + testResults.phase12IOTime +
                    testResults.phase13HashTime + testResults.phase13IOTime;
                sw.Reset();

                customerDimension.Clear();
                dateDimension.Clear();
                supplierDimension.Clear();

                long memoryUsedPhase1 = GC.GetTotalMemory(true) - memoryStartPhase1;
                #endregion Key Hashing Phase

                #region Probing Phase
                long memoryStartPhase2 = GC.GetTotalMemory(true);
                sw.Start();
                List<int> loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase21IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var _maat = new MAAT(loSupplierKey.Count);
                var i = 0;
                foreach (var suppKey in loSupplierKey)
                {
                    string sCity = string.Empty;
                    Record record = new Record();
                    if (supplierHashTable.TryGetValue(suppKey, out sCity))
                    {
                        record.s1 = sCity;
                        _maat.AddOrUpdate(i, record);
                    }
                    i++;
                }
                sw.Stop();
                testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<int> loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase22IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var k = 0;
                foreach (var orderDate in loOrderDate)
                {
                    string dYear = string.Empty;
                    if (dateHashTable.TryGetValue(orderDate, out dYear))
                    {
                        Record outValues = _maat.GetValue(k);
                        if (outValues != null)
                        {
                            outValues.s2 = dYear;
                            _maat.AddOrUpdate(k, outValues);
                        }
                    }
                    else
                    {
                        _maat.Remove(k);
                    }
                    k++;
                }
                sw.Stop();
                testResults.phase22ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<int> loCustomerKey = Utils.ReadFromBinaryFiles<int>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase23IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                sw.Start();
                var j = 0;
                foreach (var custKey in loCustomerKey)
                {
                    string cCity = string.Empty;
                    if (customerHashTable.TryGetValue(custKey, out cCity))
                    {
                        string values = string.Empty;
                        Record outValues = _maat.GetValue(j);
                        if (outValues != null)
                        {
                            outValues.s3 = cCity;
                            _maat.AddOrUpdate(j, outValues);
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
                            _maat.positions.Add(j);
                        }
                    }
                    else
                    {
                        _maat.Remove(j);
                    }
                    j++;
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
                var joinOutputFinal = new MAAT(_maat.positions.Count);
                var o = 0;
                foreach (var postion in _maat.positions)
                {
                    var item = _maat.GetValue(postion);
                    item.i1 = loRevenue[postion];
                    joinOutputFinal.AddOrUpdate(o, item);
                    o++;
                }
                sw.Stop();

                long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
                #endregion Value Extraction Phase
                testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
                testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
                testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
                // Console.WriteLine("[Nimble Join]: Time taken {0} ms.", testResults.totalExecutionTime);
                testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
                testResults.totalNumberOfOutput = joinOutputFinal.Count();
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
                dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase11IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                foreach (var row in dateDimension)
                {
                    if (row.dYearMonth.Equals("Dec1997"))
                        dateHashTable.Add(row.dDateKey, row.dYear);
                }
                sw.Stop();
                testResults.phase11HashTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase12IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                foreach (var row in customerDimension)
                {
                    if (row.cCity.Equals("UNITED KI1") || row.cCity.Equals("UNITED KI5"))
                        customerHashTable.Add(row.cCustKey, row.cCity);
                }
                sw.Stop();
                testResults.phase12HashTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase13IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                foreach (var row in supplierDimension)
                {
                    if (row.sCity.Equals("UNITED KI1") || row.sCity.Equals("UNITED KI5"))
                        supplierHashTable.Add(row.sSuppKey, row.sCity);
                }
                sw.Stop();
                testResults.phase13HashTime = sw.ElapsedMilliseconds;
                testResults.phase1Time = testResults.phase11HashTime + testResults.phase11IOTime +
                    testResults.phase12HashTime + testResults.phase12IOTime +
                    testResults.phase13HashTime + testResults.phase13IOTime;
                sw.Reset();

                customerDimension.Clear();
                dateDimension.Clear();
                supplierDimension.Clear();

                long memoryUsedPhase1 = GC.GetTotalMemory(true) - memoryStartPhase1;
                #endregion Key Hashing Phase

                #region Probing Phase
                long memoryStartPhase2 = GC.GetTotalMemory(true);
                sw.Start();
                List<int> loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase21IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var _maat = new MAAT(loSupplierKey.Count);
                var i = 0;
                foreach (var suppKey in loSupplierKey)
                {
                    string sCity = string.Empty;
                    Record record = new Record();
                    if (supplierHashTable.TryGetValue(suppKey, out sCity))
                    {
                        record.s1 = sCity;
                        _maat.AddOrUpdate(i, record);
                    }
                    i++;
                }
                sw.Stop();
                testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<int> loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase22IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var k = 0;
                foreach (var orderDate in loOrderDate)
                {
                    string dYear = string.Empty;
                    if (dateHashTable.TryGetValue(orderDate, out dYear))
                    {
                        Record outValues = _maat.GetValue(k);
                        if (outValues != null)
                        {
                            outValues.s2 = dYear;
                            _maat.AddOrUpdate(k, outValues);
                        }
                    }
                    else
                    {
                        _maat.Remove(k);
                    }
                    k++;
                }
                sw.Stop();
                testResults.phase22ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<int> loCustomerKey = Utils.ReadFromBinaryFiles<int>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase23IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                sw.Start();
                var j = 0;
                foreach (var custKey in loCustomerKey)
                {
                    string cCity = string.Empty;
                    if (customerHashTable.TryGetValue(custKey, out cCity))
                    {
                        string values = string.Empty;
                        Record outValues = _maat.GetValue(j);
                        if (outValues != null)
                        {
                            outValues.s3 = cCity;
                            _maat.AddOrUpdate(j, outValues);
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
                            _maat.positions.Add(j);
                        }
                    }
                    else
                    {
                        _maat.Remove(j);
                    }
                    j++;
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
                var joinOutputFinal = new MAAT(_maat.positions.Count);
                var o = 0;
                foreach (var postion in _maat.positions)
                {
                    var item = _maat.GetValue(postion);
                    item.i1 = loRevenue[postion];
                    joinOutputFinal.AddOrUpdate(o, item);
                    o++;
                }
                sw.Stop();

                long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
                #endregion Value Extraction Phase
                testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
                testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
                testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
                // Console.WriteLine("[Nimble Join]: Time taken {0} ms.", testResults.totalExecutionTime);
                testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
                testResults.totalNumberOfOutput = joinOutputFinal.Count();
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
                        dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                        sw.Stop();
                        testResults.phase11IOTime = sw.ElapsedMilliseconds;
                        sw.Reset();

                        sw.Start();
                        foreach (var row in dateDimension)
                        {
                            if (row.dYear.Equals("1992"))
                                dateHashTable.Add(row.dDateKey, row.dYear);
                        }
                        sw.Stop();
                        testResults.phase11HashTime = sw.ElapsedMilliseconds;
                        sw.Reset();

                        sw.Start();
                        customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
                        sw.Stop();
                        testResults.phase12IOTime = sw.ElapsedMilliseconds;
                        sw.Reset();

                        sw.Start();
                        foreach (var row in customerDimension)
                        {
                            if (row.cRegion.Equals("ASIA"))
                                customerHashTable.Add(row.cCustKey, row.cNation);
                        }
                        sw.Stop();
                        testResults.phase12HashTime = sw.ElapsedMilliseconds;
                        sw.Reset();

                        sw.Start();
                        supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
                        sw.Stop();
                        testResults.phase13IOTime = sw.ElapsedMilliseconds;
                        sw.Reset();

                        sw.Start();
                        foreach (var row in supplierDimension)
                        {
                            if (row.sRegion.Equals("ASIA"))
                                supplierHashTable.Add(row.sSuppKey, row.sNation);
                        }
                        sw.Stop();
                        break;
                    case "0.07":
                        sw.Start();
                        dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                        sw.Stop();
                        testResults.phase11IOTime = sw.ElapsedMilliseconds;
                        sw.Reset();

                        sw.Start();
                        foreach (var row in dateDimension)
                        {
                            if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1996") <= 0)
                                dateHashTable.Add(row.dDateKey, row.dYear);
                        }
                        sw.Stop();
                        testResults.phase11HashTime = sw.ElapsedMilliseconds;
                        sw.Reset();

                        sw.Start();
                        customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
                        sw.Stop();
                        testResults.phase12IOTime = sw.ElapsedMilliseconds;
                        sw.Reset();

                        sw.Start();
                        foreach (var row in customerDimension)
                        {
                            if (row.cRegion.Equals("ASIA") || row.cRegion.Equals("AFRICA"))
                                customerHashTable.Add(row.cCustKey, row.cNation);
                        }
                        sw.Stop();
                        testResults.phase12HashTime = sw.ElapsedMilliseconds;
                        sw.Reset();

                        sw.Start();
                        supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
                        sw.Stop();
                        testResults.phase13IOTime = sw.ElapsedMilliseconds;
                        sw.Reset();

                        sw.Start();
                        foreach (var row in supplierDimension)
                        {
                            if (row.sRegion.Equals("ASIA"))
                                supplierHashTable.Add(row.sSuppKey, row.sNation);
                        }
                        sw.Stop();
                        break;
                    case "0.7":
                        sw.Start();
                        dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                        sw.Stop();
                        testResults.phase11IOTime = sw.ElapsedMilliseconds;
                        sw.Reset();

                        sw.Start();
                        foreach (var row in dateDimension)
                        {
                            if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1999") <= 0)
                                dateHashTable.Add(row.dDateKey, row.dYear);
                        }
                        sw.Stop();
                        testResults.phase11HashTime = sw.ElapsedMilliseconds;
                        sw.Reset();

                        sw.Start();
                        customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
                        sw.Stop();
                        testResults.phase12IOTime = sw.ElapsedMilliseconds;
                        sw.Reset();

                        sw.Start();
                        foreach (var row in customerDimension)
                        {
                            if (row.cRegion.Equals("ASIA") || row.cRegion.Equals("AMERICA") || row.cRegion.Equals("EUROPE") || row.cRegion.Equals("AFRICA"))
                                customerHashTable.Add(row.cCustKey, row.cNation);
                        }
                        sw.Stop();
                        testResults.phase12HashTime = sw.ElapsedMilliseconds;
                        sw.Reset();

                        sw.Start();
                        supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
                        sw.Stop();
                        testResults.phase13IOTime = sw.ElapsedMilliseconds;
                        sw.Reset();

                        sw.Start();
                        foreach (var row in supplierDimension)
                        {
                            if (row.sRegion.Equals("ASIA") || row.sRegion.Equals("AMERICA") || row.sRegion.Equals("EUROPE") || row.sRegion.Equals("AFRICA"))
                                supplierHashTable.Add(row.sSuppKey, row.sNation);
                        }
                        sw.Stop();
                        break;
                }

                testResults.phase13HashTime = sw.ElapsedMilliseconds;
                testResults.phase1Time = testResults.phase11HashTime + testResults.phase11IOTime +
                    testResults.phase12HashTime + testResults.phase12IOTime +
                    testResults.phase13HashTime + testResults.phase13IOTime;
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
                var joinOutputFinal = new Dictionary<int, Int64[]>();

                for (int i = 0; i < loCommitDate.Count; i++)
                {
                    int commitDate = loCommitDate[i];
                    Int64[] values = null;
                    if (joinOutputFinal.TryGetValue(commitDate, out values))
                    {
                        switch (numberOfAggregations)
                        {
                            case 1:
                                values[0] += loTax[i];
                                break;
                            case 2:
                                values[0] += loTax[i];
                                values[1] += loDiscount[i];
                                break;
                            case 3:
                                values[0] += loTax[i];
                                values[1] += loDiscount[i];
                                values[2] += loQuantity[i];
                                break;
                            case 4:
                                values[0] += loTax[i];
                                values[1] += loDiscount[i];
                                values[2] += loQuantity[i];
                                values[3] += loSupplyCost[i];
                                break;
                            case 5:
                                values[0] += loTax[i];
                                values[1] += loDiscount[i];
                                values[2] += loQuantity[i];
                                values[3] += loSupplyCost[i];
                                values[4] += loRevenue[i];
                                break;
                            case 6:
                                values[0] += loTax[i];
                                values[1] += loDiscount[i];
                                values[2] += loQuantity[i];
                                values[3] += loSupplyCost[i];
                                values[4] += loRevenue[i];
                                values[5] += loOrderTotalPrice[i];
                                break;
                        }

                    }
                    else
                    {
                        values = new Int64[numberOfAggregations];
                        switch (numberOfAggregations)
                        {
                            case 1:
                                values[0] += loTax[i];
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
                        joinOutputFinal.Add(commitDate, values);
                    }
                }

                sw.Stop();
                long t2 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[Nimble Join] Total Time: {0}", t2));
                // Console.WriteLine(String.Format("[Invisible Join] Total Time: {0}", t0 + t1 + t2));
                Console.WriteLine(String.Format("[Nimble Join] Total : {0}", joinOutputFinal.Count));
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
                Console.WriteLine(String.Format("[Nimble Join] AGTest2 T0 Time: {0}", t0));
                #endregion Key Hashing Phase
                var _maat = new MAATIM(loCustomerKey.Count);

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
                        _maat.AddOrUpdate(i, new List<object> { cRegion, pMFGR, values });
                    }
                }

                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[Nimble Join] T1 Time: {0}", t1));
                sw.Reset();

                #endregion Probing Phase

                #region Value Extraction Phase
                sw.Start();
                var joinOutputFinal = new Dictionary<string, long[]>();
                int index = 0;
                foreach (var item in _maat.GetAll())
                {
                    if (item != null)
                    {
                        string key = item[0] + ", " + item[1];
                        long[] values = null;
                        if (joinOutputFinal.TryGetValue(key, out values))
                        {
                            switch (numberOfAggregations)
                            {
                                case 1:
                                    values[0] += loTax[index];
                                    break;
                                case 2:
                                    values[0] += loTax[index];
                                    values[1] += loDiscount[index];
                                    break;
                                case 3:
                                    values[0] += loTax[index];
                                    values[1] += loDiscount[index];
                                    values[2] += loQuantity[index];
                                    break;
                                case 4:
                                    values[0] += loTax[index];
                                    values[1] += loDiscount[index];
                                    values[2] += loQuantity[index];
                                    values[3] += loSupplyCost[index];
                                    break;
                                case 5:
                                    values[0] += loTax[index];
                                    values[1] += loDiscount[index];
                                    values[2] += loQuantity[index];
                                    values[3] += loSupplyCost[index];
                                    values[4] += loRevenue[index];
                                    break;
                                case 6:
                                    values[0] += loTax[index];
                                    values[1] += loDiscount[index];
                                    values[2] += loQuantity[index];
                                    values[3] += loSupplyCost[index];
                                    values[4] += loRevenue[index];
                                    values[5] += loOrderTotalPrice[index];
                                    break;
                            }
                        }
                        else
                        {
                            values = new long[numberOfAggregations];
                            switch (numberOfAggregations)
                            {
                                case 1:
                                    values[0] = loTax[index];
                                    break;
                                case 2:
                                    values[0] = loTax[index];
                                    values[1] = loDiscount[index];
                                    break;
                                case 3:
                                    values[0] = loTax[index];
                                    values[1] = loDiscount[index];
                                    values[2] = loQuantity[index];
                                    break;
                                case 4:
                                    values[0] = loTax[index];
                                    values[1] = loDiscount[index];
                                    values[2] = loQuantity[index];
                                    values[3] = loSupplyCost[index];
                                    break;
                                case 5:
                                    values[0] = loTax[index];
                                    values[1] = loDiscount[index];
                                    values[2] = loQuantity[index];
                                    values[3] = loSupplyCost[index];
                                    values[4] = loRevenue[index];
                                    break;
                                case 6:
                                    values[0] = loTax[index];
                                    values[1] = loDiscount[index];
                                    values[2] = loQuantity[index];
                                    values[3] = loSupplyCost[index];
                                    values[4] = loRevenue[index];
                                    values[5] = loOrderTotalPrice[index];
                                    break;
                            }
                            joinOutputFinal.Add(key, values);
                        }
                    }
                    index++;
                }

                sw.Stop();
                long t2 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[Nimble Join] AGTest2 T2 Time: {0}", t2));
                Console.WriteLine(String.Format("[Nimble Join] AGTest2 Total Time: {0}", t0 + t1 + t2));
                Console.WriteLine(String.Format("[Nimble Join] AGTest2 Total : {0}", joinOutputFinal.Count));
                Console.WriteLine();
                #endregion Value Extraction Phase
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
                Console.WriteLine(String.Format("[Nimble Join] JSTest T0 Time: {0}", t0));
                #endregion Key Hashing Phase
                var _maat = new MAATIM(loCustomerKey.Count);
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
                                _maat.AddOrUpdate(i, new List<object> { cRegionOut, loTax[i] });
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
                                _maat.AddOrUpdate(i, new List<object> { cRegionOut, sRegionOut, loTax[i] });
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
                                _maat.AddOrUpdate(i, new List<object> { cRegionOut, sRegionOut, yearOut, loTax[i] });
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
                                _maat.AddOrUpdate(i, new List<object> { cRegionOut, sRegionOut, yearOut, pMFGROut, loTax[i] });
                            }
                        }
                        break;
                }

                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[Nimble Join] JSTest T1 Time: {0}", t1));
                sw.Reset();
                #endregion Probing Phase

                #region Value Extraction Phase
                sw.Start();
                var joinOutputFinal = new Dictionary<string, long>();
                int index = 0;
                foreach (var item in _maat.GetAll())
                {
                    try
                    {
                        if (item != null)
                        {
                            string key = string.Empty;
                            switch (numberOfJoins)
                            {
                                case 1:
                                    key = Convert.ToString(item[0]);
                                    break;
                                case 2:
                                    key = item[0] + ", " + item[1];

                                    break;
                                case 3:
                                    key = item[0] + ", " + item[1] + ", " + item[2];
                                    break;
                                case 4:
                                    key = item[0] + ", " + item[1] + ", " + item[2] + "," + item[3];
                                    break;
                            }

                            long tax = 0;
                            if (joinOutputFinal.TryGetValue(key, out tax))
                            {
                                joinOutputFinal[key] = tax + loTax[index];
                            }
                            else
                            {
                                joinOutputFinal.Add(key, loTax[index]);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(index);
                        throw;
                    }
                    index++;
                }

                sw.Stop();
                long t2 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[Nimble Join] JSTest T2 Time: {0}", t2));
                Console.WriteLine(String.Format("[Nimble Join] JSTest Total Time: {0}", t0 + t1 + t2));
                Console.WriteLine(String.Format("[Nimble Join] JSTest Total : {0}", joinOutputFinal.Count));
                Console.WriteLine();
                #endregion Value Extraction Phase
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
                List<Customer> customerDimension = null;
                List<Supplier> supplierDimension = null;
                List<Date> dateDimension = null;
                List<int> loCustomerKey = null;
                List<int> loSupplierKey = null;
                List<int> loOrderDate = null;

                customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
                supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
                dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));

                loCustomerKey = Utils.ReadFromBinaryFiles<int>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));

                List<int> loTax = Utils.ReadFromBinaryFiles<int>(loTaxFile.Replace("BF", "BF" + scaleFactor));

                sw.Start();
                #region Key Hashing Phase 

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
                Console.WriteLine(String.Format("[Nimble Join] GSTest T0 Time: {0}", t0));
                #endregion Key Hashing Phase
                var _maat = new MAATIM(loCustomerKey.Count);
                #region Probing Phase
                sw.Reset();
                sw.Start();

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
                        _maat.AddOrUpdate(i, new List<object> { cOut, sOut, dOut, loTax[i] });
                    }
                }

                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[Nimble Join] GSTest T1 Time: {0}", t1));
                sw.Reset();
                #endregion Probing Phase

                #region Value Extraction Phase
                sw.Start();
                var joinOutputFinal = new Dictionary<string, long>();
                Tuple<string, string> custGA = null;
                Tuple<string, string> suppGA = null;
                Tuple<string, string> dateGA = null;
                int index = 0;
                foreach (var item in _maat.GetAll())
                {
                    try
                    {
                        if (item != null)
                        {
                            string key = string.Empty;
                            switch (numberOfGroupingAttributes)
                            {
                                case 1:
                                    key = ((Tuple<string, string>)item[0]).Item1;
                                    break;
                                case 2:
                                    custGA = ((Tuple<string, string>)item[0]);
                                    key = custGA.Item1 + ", " + custGA.Item2;
                                    break;
                                case 3:
                                    custGA = ((Tuple<string, string>)item[0]);
                                    suppGA = ((Tuple<string, string>)item[1]);
                                    key = custGA.Item1 + ", " + custGA.Item2 + ", " + suppGA.Item1;
                                    break;
                                case 4:
                                    custGA = ((Tuple<string, string>)item[0]);
                                    suppGA = ((Tuple<string, string>)item[1]);
                                    key = custGA.Item1 + ", " + custGA.Item2 + ", " + suppGA.Item1 + ", " + suppGA.Item2;
                                    break;
                                case 5:
                                    custGA = ((Tuple<string, string>)item[0]);
                                    suppGA = ((Tuple<string, string>)item[1]);
                                    dateGA = ((Tuple<string, string>)item[2]);
                                    key = custGA.Item1 + ", " + custGA.Item2 + ", " + suppGA.Item1 + ", " + suppGA.Item2 + ", " + dateGA.Item1;
                                    break;
                                case 6:
                                    custGA = ((Tuple<string, string>)item[0]);
                                    suppGA = ((Tuple<string, string>)item[1]);
                                    dateGA = ((Tuple<string, string>)item[2]);
                                    key = custGA.Item1 + ", " + custGA.Item2 + ", " + suppGA.Item1 + ", " + suppGA.Item2 + ", " + dateGA.Item1 + ", " + dateGA.Item2;
                                    break;
                            }

                            long tax = 0;
                            if (joinOutputFinal.TryGetValue(key, out tax))
                            {
                                joinOutputFinal[key] = tax + loTax[index];
                            }
                            else
                            {
                                joinOutputFinal.Add(key, loTax[index]);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(index);
                        throw;
                    }
                    index++;
                }

                sw.Stop();
                long t2 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[Nimble Join] GSTest T2 Time: {0}", t2));
                Console.WriteLine(String.Format("[Nimble Join] GSTest Total Time: {0}", t0 + t1 + t2));
                Console.WriteLine(String.Format("[Nimble Join] GSTest Total : {0}", joinOutputFinal.Count));
                Console.WriteLine();
                #endregion Value Extraction Phase
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void saveAndPrintResults()
        {
            TestResultsDatabase.nimbleJoinOutput.Add(testResults.toString());
            Console.WriteLine("Nimble: " + testResults.toString());
            Console.WriteLine();
        }
    }
}
