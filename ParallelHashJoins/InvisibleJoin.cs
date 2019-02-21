using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParallelHashJoins
{
    public class InvisibleJoin
    {
        private static string binaryFilesDirectory = @"C:\Raw_Data_Source_For_Test\SSBM - DBGEN\BF";
        private string scaleFactor { get; set; }

        private MemoryManagement memoryManagement { get; set; }

        public InvisibleJoin()
        {
        }
        public InvisibleJoin(string scaleFactor)
        {
            this.scaleFactor = scaleFactor;
            testResults.totalRAMAvailable = Utils.getAvailableRAM();
        }
        ~InvisibleJoin()
        {
            //saveAndPrInt64Results();
        }

        #region Private Variables
        private Object obj;
        private List<Int64> cCustKey = new List<Int64>();
        private List<string> cName = new List<string>();
        private List<string> cAddress = new List<string>();
        private List<string> cCity = new List<string>();
        private List<string> cNation = new List<string>();
        private List<string> cRegion = new List<string>();
        private List<string> cPhone = new List<string>();
        private List<string> cMktSegment = new List<string>();

        private List<Int64> sSuppKey = new List<Int64>();
        private List<string> sName = new List<string>();
        private List<string> sAddress = new List<string>();
        private List<string> sCity = new List<string>();
        private List<string> sNation = new List<string>();
        private List<string> sRegion = new List<string>();
        private List<string> sPhone = new List<string>();

        private List<Int64> pSize = new List<Int64>();
        private List<Int64> pPartKey = new List<Int64>();
        private List<string> pName = new List<string>();
        private List<string> pMFGR = new List<string>();
        private List<string> pCategory = new List<string>();
        private List<string> pBrand = new List<string>();
        private List<string> pColor = new List<string>();
        private List<string> pType = new List<string>();
        private List<string> pContainer = new List<string>();

        private List<Int64> loOrderKey = new List<Int64>();
        private List<Int64> loLineNumber = new List<Int64>();
        private List<Int64> loCustKey = new List<Int64>();
        private List<Int64> loPartKey = new List<Int64>();
        private List<Int64> loSuppKey = new List<Int64>();
        private List<Int64> loOrderDate = new List<Int64>();
        private List<char> loShipPriority = new List<char>();
        private List<Int64> loQuantity = new List<Int64>();
        private List<Tuple<Int64, Int64>> loQuantityWithId = new List<Tuple<Int64, Int64>>();

        private List<Int64> loExtendedPrice = new List<Int64>();
        private List<Int64> loOrdTotalPrice = new List<Int64>();
        private List<Int64> loDiscount = new List<Int64>();
        private List<Tuple<Int64, Int64>> loDiscountWithId = new List<Tuple<Int64, Int64>>();
        private List<Int64> loRevenue = new List<Int64>();
        private List<Int64> loSupplyCost = new List<Int64>();
        private List<Int64> loTax = new List<Int64>();
        private List<Int64> loCommitDate = new List<Int64>();
        private List<string> loShipMode = new List<string>();
        private List<string> loOrderPriority = new List<string>();

        private List<Int64> dDateKey = new List<Int64>();
        private List<Int64> dYear = new List<Int64>();
        private List<Int64> dYearMonthNum = new List<Int64>();
        private List<Int64> dDayNumInWeek = new List<Int64>();
        private List<Int64> dDayNumInMonth = new List<Int64>();
        private List<Int64> dDayNumInYear = new List<Int64>();
        private List<Int64> dMonthNumInYear = new List<Int64>();
        private List<Int64> dWeekNumInYear = new List<Int64>();
        private List<Int64> dLastDayInWeekFL = new List<Int64>();
        private List<Int64> dLastDayInMonthFL = new List<Int64>();
        private List<Int64> dHolidayFL = new List<Int64>();
        private List<Int64> dWeekDayFL = new List<Int64>();
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
        private const Int64 NUMBER_OF_RECORDS_OUTPUT = 10000;
        private Int64 outputRecordsCounter = 0;
        #endregion Private Variables


        public TestResults testResults = new TestResults();

        public void Query_1_1()
        {
            try
            {
                long memoryStartPhase1 = GC.GetTotalMemory(true);
                outputRecordsCounter = 0;
                var dateHashTable = new Dictionary<Int64, string>();
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
                List<Int64> loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase21IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var listOrderDatePositions = new List<Int64>();
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
                sw.Stop();
                testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<Int64> loDiscount = Utils.ReadFromBinaryFiles<Int64>(loDiscountFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase22IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var listLineOrderDiscountPositions = new List<Int64>();
                var j = 0;
                foreach (var _loDiscount in loDiscount)
                {
                    if (_loDiscount >= 1 && _loDiscount <= 3)
                    {
                        listLineOrderDiscountPositions.Add(j);
                    }
                    j++;
                }
                sw.Stop();
                testResults.phase22ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<Int64> loQuantity = Utils.ReadFromBinaryFiles<Int64>(loQuantityFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase23IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var listLineOrderQuantityPositions = new List<Int64>();
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
                var common = listLineOrderDiscountPositions.Intersect(listOrderDatePositions).Intersect(listLineOrderQuantityPositions).ToList();
                sw.Stop();
                testResults.phase23ProbeTime = sw.ElapsedMilliseconds;
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
                loDiscount = Utils.ReadFromBinaryFiles<Int64>(loDiscountFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loExtendedPrice = Utils.ReadFromBinaryFiles<Int64>(loExtendedPriceFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase3IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                Int64 totalRevenue = 0;
                foreach (Int64 index in common)
                {
                    try
                    {
                        var revenue = loDiscount[index] * loExtendedPrice[index];
                        totalRevenue += revenue;
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
                }
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
                var dateHashTable = new Dictionary<Int64, string>();
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
                List<Int64> loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase21IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var listOrderDatePositions = new List<Int64>();
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
                sw.Stop();
                testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<Int64> loDiscount = Utils.ReadFromBinaryFiles<Int64>(loDiscountFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase22IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var listLineOrderDiscountPositions = new List<Int64>();
                var j = 0;
                foreach (var _loDiscount in loDiscount)
                {
                    if (_loDiscount >= 4 && _loDiscount <= 6)
                    {
                        listLineOrderDiscountPositions.Add(j);
                    }
                    j++;
                }
                sw.Stop();
                testResults.phase22ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<Int64> loQuantity = Utils.ReadFromBinaryFiles<Int64>(loQuantityFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase23IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var listLineOrderQuantityPositions = new List<Int64>();
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
                var common = listLineOrderDiscountPositions.Intersect(listOrderDatePositions).Intersect(listLineOrderQuantityPositions).ToList();
                sw.Stop();
                testResults.phase23ProbeTime = sw.ElapsedMilliseconds;
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
                loDiscount = Utils.ReadFromBinaryFiles<Int64>(loDiscountFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loExtendedPrice = Utils.ReadFromBinaryFiles<Int64>(loExtendedPriceFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase3IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                Int64 totalRevenue = 0;
                foreach (Int64 index in common)
                {
                    try
                    {
                        var revenue = loDiscount[index] * loExtendedPrice[index];
                        totalRevenue += revenue;
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
                }
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
                var dateHashTable = new Dictionary<Int64, string>();
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
                List<Int64> loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase21IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var listOrderDatePositions = new List<Int64>();
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
                sw.Stop();
                testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<Int64> loDiscount = Utils.ReadFromBinaryFiles<Int64>(loDiscountFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase22IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var listLineOrderDiscountPositions = new List<Int64>();
                var j = 0;
                foreach (var _loDiscount in loDiscount)
                {
                    if (_loDiscount >= 5 && _loDiscount <= 7)
                    {
                        listLineOrderDiscountPositions.Add(j);
                    }
                    j++;
                }
                sw.Stop();
                testResults.phase22ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<Int64> loQuantity = Utils.ReadFromBinaryFiles<Int64>(loQuantityFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase23IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var listLineOrderQuantityPositions = new List<Int64>();
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
                var common = listLineOrderDiscountPositions.Intersect(listOrderDatePositions).Intersect(listLineOrderQuantityPositions).ToList();
                sw.Stop();
                testResults.phase23ProbeTime = sw.ElapsedMilliseconds;
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
                loDiscount = Utils.ReadFromBinaryFiles<Int64>(loDiscountFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loExtendedPrice = Utils.ReadFromBinaryFiles<Int64>(loExtendedPriceFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase3IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                Int64 totalRevenue = 0;
                foreach (Int64 index in common)
                {
                    try
                    {
                        var revenue = loDiscount[index] * loExtendedPrice[index];
                        totalRevenue += revenue;
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
                }
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


        public void Query_2_1()
        {

            try
            {
                long memoryStartPhase1 = GC.GetTotalMemory(true);
                outputRecordsCounter = 0;
                var dateHashTable = new Dictionary<Int64, string>();
                var partHashTable = new Dictionary<Int64, string>();
                var supplierHashTable = new Dictionary<Int64, string>();
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
                List<Int64> loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase21IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var listOrderDatePositions = new List<Int64>();
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
                sw.Stop();
                testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<Int64> loPartKey = Utils.ReadFromBinaryFiles<Int64>(loPartKeyFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase22IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var listPartKeyPositions = new List<Int64>();
                var j = 0;
                foreach (var partKey in loPartKey)
                {
                    string pBrandOut = string.Empty;
                    if (partHashTable.TryGetValue(partKey, out pBrandOut))
                    {
                        listPartKeyPositions.Add(j);
                    }
                    j++;
                }
                sw.Stop();
                testResults.phase22ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<Int64> loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase23IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var listSupplierKeyPositions = new List<Int64>();
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
                var common = listPartKeyPositions.Intersect(listOrderDatePositions).Intersect(listSupplierKeyPositions).ToList();
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
                loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                loPartKey = Utils.ReadFromBinaryFiles<Int64>(loPartKeyFile.Replace("BF", "BF" + scaleFactor));
                // loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                foreach (var row in dateDimension)
                {
                    dateHashTable.Add(row.dDateKey, row.dYear);
                }
                dateDimension.Clear();

                List<string> pBrand = Utils.ReadFromBinaryFiles<string>(pBrandFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase3IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var joinOutputFinal = new Dictionary<Int64, string>();
                foreach (Int64 index in common)
                {
                    try
                    {
                        var dateKey = loOrderDate[index];
                        var partKey = loPartKey[index];
                        var revenue = loRevenue[index];

                        // Position Look UP
                        string dYear;
                        dateHashTable.TryGetValue(dateKey, out dYear);

                        string pBrandOut = pBrand[partKey];
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
                        joinOutputFinal.Add(index, revenue + "," + dYear + "," + pBrandOut);

                    }
                    catch (Exception)
                    {
                        throw;
                    }
                }
                //var joinOutputFinal = new Dictionary<Int64, string>();
                //foreach (var item in joinOutputIntermediate)
                //{
                //    joinOutputFinal.Add(item.Key, item.Value + "," + loRevenue[item.Key]);
                //}
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
                var dateHashTable = new Dictionary<Int64, string>();
                var partHashTable = new Dictionary<Int64, string>();
                var supplierHashTable = new Dictionary<Int64, string>();
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
                List<Int64> loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase21IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var listOrderDatePositions = new List<Int64>();
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
                sw.Stop();
                testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<Int64> loPartKey = Utils.ReadFromBinaryFiles<Int64>(loPartKeyFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase22IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var listPartKeyPositions = new List<Int64>();
                var j = 0;
                foreach (var partKey in loPartKey)
                {
                    string pBrandOut = string.Empty;
                    if (partHashTable.TryGetValue(partKey, out pBrandOut))
                    {
                        listPartKeyPositions.Add(j);
                    }
                    j++;
                }
                sw.Stop();
                testResults.phase22ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<Int64> loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase23IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var listSupplierKeyPositions = new List<Int64>();
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
                var common = listPartKeyPositions.Intersect(listOrderDatePositions).Intersect(listSupplierKeyPositions).ToList();
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
                loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                loPartKey = Utils.ReadFromBinaryFiles<Int64>(loPartKeyFile.Replace("BF", "BF" + scaleFactor));
                // loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                foreach (var row in dateDimension)
                {
                    dateHashTable.Add(row.dDateKey, row.dYear);
                }
                dateDimension.Clear();

                List<string> pBrand = Utils.ReadFromBinaryFiles<string>(pBrandFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase3IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var joinOutputFinal = new Dictionary<Int64, string>();
                foreach (Int64 index in common)
                {
                    try
                    {
                        var dateKey = loOrderDate[index];
                        var partKey = loPartKey[index];
                        var revenue = loRevenue[index];

                        // Position Look UP
                        string dYear;
                        dateHashTable.TryGetValue(dateKey, out dYear);

                        string pBrandOut = pBrand[partKey];
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
                        joinOutputFinal.Add(index, revenue + "," + dYear + "," + pBrandOut);

                    }
                    catch (Exception)
                    {
                        throw;
                    }
                }
                //var joinOutputFinal = new Dictionary<Int64, string>();
                //foreach (var item in joinOutputIntermediate)
                //{
                //    joinOutputFinal.Add(item.Key, item.Value + "," + loRevenue[item.Key]);
                //}
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
                var dateHashTable = new Dictionary<Int64, string>();
                var partHashTable = new Dictionary<Int64, string>();
                var supplierHashTable = new Dictionary<Int64, string>();
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
                List<Int64> loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase21IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var listOrderDatePositions = new List<Int64>();
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
                sw.Stop();
                testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<Int64> loPartKey = Utils.ReadFromBinaryFiles<Int64>(loPartKeyFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase22IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var listPartKeyPositions = new List<Int64>();
                var j = 0;
                foreach (var partKey in loPartKey)
                {
                    string pBrandOut = string.Empty;
                    if (partHashTable.TryGetValue(partKey, out pBrandOut))
                    {
                        listPartKeyPositions.Add(j);
                    }
                    j++;
                }
                sw.Stop();
                testResults.phase22ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<Int64> loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase23IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var listSupplierKeyPositions = new List<Int64>();
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
                var common = listPartKeyPositions.Intersect(listOrderDatePositions).Intersect(listSupplierKeyPositions).ToList();
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
                loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                loPartKey = Utils.ReadFromBinaryFiles<Int64>(loPartKeyFile.Replace("BF", "BF" + scaleFactor));
                // loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                foreach (var row in dateDimension)
                {
                    dateHashTable.Add(row.dDateKey, row.dYear);
                }
                dateDimension.Clear();

                List<string> pBrand = Utils.ReadFromBinaryFiles<string>(pBrandFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase3IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var joinOutputFinal = new Dictionary<Int64, string>();
                foreach (Int64 index in common)
                {
                    try
                    {
                        var dateKey = loOrderDate[index];
                        var partKey = loPartKey[index];
                        var revenue = loRevenue[index];

                        // Position Look UP
                        string dYear;
                        dateHashTable.TryGetValue(dateKey, out dYear);

                        string pBrandOut = pBrand[partKey];
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
                        joinOutputFinal.Add(index, revenue + "," + dYear + "," + pBrandOut);

                    }
                    catch (Exception)
                    {
                        throw;
                    }
                }
                //var joinOutputFinal = new Dictionary<Int64, string>();
                //foreach (var item in joinOutputIntermediate)
                //{
                //    joinOutputFinal.Add(item.Key, item.Value + "," + loRevenue[item.Key]);
                //}
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
                var dateHashTable = new Dictionary<Int64, string>();
                var customerHashTable = new Dictionary<Int64, string>();
                var supplierHashTable = new Dictionary<Int64, string>();
                var partHashTable = new Dictionary<Int64, string>();
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
                List<Int64> loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase21IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var listOrderDatePositions = new List<Int64>();
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
                sw.Stop();
                testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<Int64> loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase22IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var listCustomerKeyPositions = new List<Int64>();
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
                sw.Stop();
                testResults.phase22ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<Int64> loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase23IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var listSupplierKeyPositions = new List<Int64>();
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
                sw.Stop();
                testResults.phase23ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<Int64> loPartKey = Utils.ReadFromBinaryFiles<Int64>(loPartKeyFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase24IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var listPartKeyPositions = new List<Int64>();
                var l = 0;
                foreach (var partKey in loPartKey)
                {
                    string pMFGR = string.Empty;
                    if (partHashTable.TryGetValue(partKey, out pMFGR))
                    {
                        listPartKeyPositions.Add(l);
                    }
                    l++;
                }
                var common = listCustomerKeyPositions.Intersect(listOrderDatePositions).Intersect(listSupplierKeyPositions).Intersect(listPartKeyPositions).ToList();
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
                loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                //loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                //loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                foreach (var row in dateDimension)
                {
                    // if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
                    dateHashTable.Add(row.dDateKey, row.dYear);
                }
                dateDimension.Clear();

                List<string> cNation = Utils.ReadFromBinaryFiles<string>(cNationFile.Replace("BF", "BF" + scaleFactor));
                // List<string> sNation = Utils.ReadFromBinaryFiles<string>(sNationFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loSupplyCost = Utils.ReadFromBinaryFiles<Int64>(loSupplyCostFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase3IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var joinOutputFinal = new Dictionary<Int64, string>();
                foreach (Int64 index in common)
                {
                    try
                    {
                        var dateKey = loOrderDate[index];
                        var custKey = loCustomerKey[index];

                        var revenue = loRevenue[index];
                        var supplyCost = loSupplyCost[index];

                        // Position Look UP
                        string dYear;
                        dateHashTable.TryGetValue(dateKey, out dYear);

                        string cNationOut = cNation[custKey];
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
                        joinOutputFinal.Add(index, dYear + "," + cNationOut + "," + revenue + "," + supplyCost);

                    }
                    catch (Exception)
                    {
                        throw;
                    }
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
                throw;
            }
        }
        public void Query_4_2()
        {
            try
            {
                long memoryStartPhase1 = GC.GetTotalMemory(true);
                outputRecordsCounter = 0;
                var dateHashTable = new Dictionary<Int64, string>();
                var customerHashTable = new Dictionary<Int64, string>();
                var supplierHashTable = new Dictionary<Int64, string>();
                var partHashTable = new Dictionary<Int64, string>();
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
                partDimension.Clear();

                long memoryUsedPhase1 = GC.GetTotalMemory(true) - memoryStartPhase1;
                #endregion Key Hashing Phase

                #region Probing Phase
                long memoryStartPhase2 = GC.GetTotalMemory(true);
                sw.Start();
                List<Int64> loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase21IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var listOrderDatePositions = new List<Int64>();
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
                sw.Stop();
                testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<Int64> loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase22IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var listCustomerKeyPositions = new List<Int64>();
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
                sw.Stop();
                testResults.phase22ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<Int64> loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase23IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var listSupplierKeyPositions = new List<Int64>();
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
                sw.Stop();
                testResults.phase23ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<Int64> loPartKey = Utils.ReadFromBinaryFiles<Int64>(loPartKeyFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase24IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var listPartKeyPositions = new List<Int64>();
                var l = 0;
                foreach (var partKey in loPartKey)
                {
                    string pCategory = string.Empty;
                    if (partHashTable.TryGetValue(partKey, out pCategory))
                    {
                        listPartKeyPositions.Add(l);
                    }
                    l++;
                }
                var common = listCustomerKeyPositions.Intersect(listOrderDatePositions).Intersect(listSupplierKeyPositions).Intersect(listPartKeyPositions).ToList();
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
                loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                //loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                loPartKey = Utils.ReadFromBinaryFiles<Int64>(loPartKeyFile.Replace("BF", "BF" + scaleFactor));
                dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));

                foreach (var row in dateDimension)
                {
                    if (row.dYear.Equals("1997") || row.dYear.Equals("1998"))
                        dateHashTable.Add(row.dDateKey, row.dYear);
                }
                dateDimension.Clear();

                partDimension = Utils.ReadFromBinaryFiles<Part>(partFile.Replace("BF", "BF" + scaleFactor));
                foreach (var row in partDimension)
                {
                    if (row.pMFGR.Equals("MFGR#1") || row.pMFGR.Equals("MFGR#2"))
                        partHashTable.Add(row.pPartKey, row.pCategory);
                }
                partDimension.Clear();

                //List<string> cNation = Utils.ReadFromBinaryFiles<string>(cNationFile.Replace("BF", "BF" + scaleFactor));
                List<string> sNation = Utils.ReadFromBinaryFiles<string>(sNationFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loSupplyCost = Utils.ReadFromBinaryFiles<Int64>(loSupplyCostFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase3IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var joinOutputFinal = new Dictionary<Int64, string>();
                foreach (Int64 index in common)
                {
                    try
                    {
                        var dateKey = loOrderDate[index];
                        var suppKey = loSupplierKey[index];
                        var partKey = loPartKey[index];
                        var revenue = loRevenue[index];
                        var supplyCost = loSupplyCost[index];

                        // Position Look UP
                        string dYear;
                        dateHashTable.TryGetValue(dateKey, out dYear);

                        string pCategory;
                        partHashTable.TryGetValue(partKey, out pCategory);

                        string sNationOut = sNation[suppKey - 1];
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
                        joinOutputFinal.Add(index, dYear + "," + sNationOut + "," + pCategory + "," + revenue + "," + supplyCost);

                    }

                    catch (Exception)
                    {
                        throw;
                    }
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
                throw;
            }
        }
        public void Query_4_3()
        {
            try
            {
                long memoryStartPhase1 = GC.GetTotalMemory(true);
                outputRecordsCounter = 0;
                var dateHashTable = new Dictionary<Int64, string>();
                var customerHashTable = new Dictionary<Int64, string>();
                var supplierHashTable = new Dictionary<Int64, string>();
                var partHashTable = new Dictionary<Int64, string>();
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
                partDimension.Clear();

                long memoryUsedPhase1 = GC.GetTotalMemory(true) - memoryStartPhase1;
                #endregion Key Hashing Phase

                #region Probing Phase
                long memoryStartPhase2 = GC.GetTotalMemory(true);
                sw.Start();
                List<Int64> loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase21IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var listOrderDatePositions = new List<Int64>();
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
                sw.Stop();
                testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<Int64> loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase22IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var listCustomerKeyPositions = new List<Int64>();
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
                sw.Stop();
                testResults.phase22ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<Int64> loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase23IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var listSupplierKeyPositions = new List<Int64>();
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
                sw.Stop();
                testResults.phase23ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<Int64> loPartKey = Utils.ReadFromBinaryFiles<Int64>(loPartKeyFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase24IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var listPartKeyPositions = new List<Int64>();
                var l = 0;
                foreach (var partKey in loPartKey)
                {
                    string pMFGR = string.Empty;
                    if (partHashTable.TryGetValue(partKey, out pMFGR))
                    {
                        listPartKeyPositions.Add(l);
                    }
                    l++;
                }
                var common = listCustomerKeyPositions.Intersect(listOrderDatePositions).Intersect(listSupplierKeyPositions).Intersect(listPartKeyPositions).ToList();
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
                loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                //loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                loPartKey = Utils.ReadFromBinaryFiles<Int64>(loPartKeyFile.Replace("BF", "BF" + scaleFactor));
                dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));

                foreach (var row in dateDimension)
                {
                    if (row.dYear.Equals("1997") || row.dYear.Equals("1998"))
                        dateHashTable.Add(row.dDateKey, row.dYear);
                }
                dateDimension.Clear();

                partDimension = Utils.ReadFromBinaryFiles<Part>(partFile.Replace("BF", "BF" + scaleFactor));
                foreach (var row in partDimension)
                {
                    if (row.pCategory.Equals("MFGR#14"))
                        partHashTable.Add(row.pPartKey, row.pBrand);
                }
                partDimension.Clear();

                //List<string> cNation = Utils.ReadFromBinaryFiles<string>(cNationFile.Replace("BF", "BF" + scaleFactor));
                List<string> sCity = Utils.ReadFromBinaryFiles<string>(sCityFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loSupplyCost = Utils.ReadFromBinaryFiles<Int64>(loSupplyCostFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase3IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var joinOutputFinal = new Dictionary<Int64, string>();
                foreach (Int64 index in common)
                {
                    try
                    {
                        var dateKey = loOrderDate[index];
                        var suppKey = loSupplierKey[index];
                        var partKey = loPartKey[index];
                        var revenue = loRevenue[index];
                        var supplyCost = loSupplyCost[index];

                        // Position Look UP
                        string dYear;
                        dateHashTable.TryGetValue(dateKey, out dYear);

                        string pBrand;
                        partHashTable.TryGetValue(partKey, out pBrand);

                        string sCityOut = sCity[suppKey - 1];
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
                        joinOutputFinal.Add(index, dYear + "," + sCityOut + "," + pBrand + "," + revenue + "," + supplyCost);

                    }
                    catch (Exception)
                    {
                        throw;
                    }
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
                throw;
            }
        }

        public void Query_3_1()
        {
            try
            {
                long memoryStartPhase1 = GC.GetTotalMemory(true);
                outputRecordsCounter = 0;
                var dateHashTable = new Dictionary<Int64, string>();
                var customerHashTable = new Dictionary<Int64, string>();
                var supplierHashTable = new Dictionary<Int64, string>();
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
                List<Int64> loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase21IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var listOrderDatePositions = new List<Int64>();
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
                sw.Stop();
                testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<Int64> loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase22IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var listCustomerKeyPositions = new List<Int64>();
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
                sw.Stop();
                testResults.phase22ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<Int64> loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase23IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var listSupplierKeyPositions = new List<Int64>();
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
                var common = listCustomerKeyPositions.Intersect(listOrderDatePositions).Intersect(listSupplierKeyPositions).ToList();
                sw.Stop();
                testResults.phase23ProbeTime = sw.ElapsedMilliseconds;
                testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime +
                    testResults.phase22IOTime + testResults.phase22ProbeTime +
                    testResults.phase23IOTime + testResults.phase23ProbeTime;
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
                loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                foreach (var row in dateDimension)
                {
                    if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
                        dateHashTable.Add(row.dDateKey, row.dYear);
                }
                dateDimension.Clear();

                List<string> cNation = Utils.ReadFromBinaryFiles<string>(cNationFile.Replace("BF", "BF" + scaleFactor));
                List<string> sNation = Utils.ReadFromBinaryFiles<string>(sNationFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase3IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var joinOutputIntermediate = new Dictionary<Int64, string>();
                foreach (Int64 index in common)
                {
                    try
                    {
                        var dateKey = loOrderDate[index];
                        var custKey = loCustomerKey[index];
                        var suppKey = loSupplierKey[index];

                        // Position Look UP
                        string dYear;
                        dateHashTable.TryGetValue(dateKey, out dYear);

                        string cNationOut = cNation[custKey];
                        string sNationOut = sNation[suppKey - 1];
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
                        joinOutputIntermediate.Add(index, cNationOut + "," + sNationOut + "," + dYear);

                    }
                    catch (Exception)
                    {
                        throw;
                    }
                }
                var joinOutputFinal = new Dictionary<Int64, string>();
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
                //Console.WriteLine("[Invisble Join]: Time taken {0} ms.", sw.ElapsedMilliseconds);
                testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
                testResults.totalNumberOfOutput = joinOutputFinal.Count;
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
                var dateHashTable = new Dictionary<Int64, string>();
                var customerHashTable = new Dictionary<Int64, string>();
                var supplierHashTable = new Dictionary<Int64, string>();
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
                List<Int64> loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase21IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var listOrderDatePositions = new List<Int64>();
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
                sw.Stop();
                testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<Int64> loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase22IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var listCustomerKeyPositions = new List<Int64>();
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
                sw.Stop();
                testResults.phase22ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<Int64> loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase23IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var listSupplierKeyPositions = new List<Int64>();
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
                var common = listCustomerKeyPositions.Intersect(listOrderDatePositions).Intersect(listSupplierKeyPositions).ToList();
                sw.Stop();
                testResults.phase23ProbeTime = sw.ElapsedMilliseconds;
                testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime +
                    testResults.phase22IOTime + testResults.phase22ProbeTime +
                    testResults.phase23IOTime + testResults.phase23ProbeTime;
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
                loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                foreach (var row in dateDimension)
                {
                    if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
                        dateHashTable.Add(row.dDateKey, row.dYear);
                }
                dateDimension.Clear();

                List<string> cCity = Utils.ReadFromBinaryFiles<string>(cCityFile.Replace("BF", "BF" + scaleFactor));
                List<string> sCity = Utils.ReadFromBinaryFiles<string>(sCityFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase3IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var joinOutputIntermediate = new Dictionary<Int64, string>();
                foreach (Int64 index in common)
                {
                    try
                    {
                        var dateKey = loOrderDate[index];
                        var custKey = loCustomerKey[index];
                        var suppKey = loSupplierKey[index];

                        // Position Look UP
                        string dYear;
                        dateHashTable.TryGetValue(dateKey, out dYear);

                        string cCityOut = cCity[custKey];
                        string sCityOut = sCity[suppKey - 1];
                        if (isFirst)
                        {
                            swInitialRecorder.Stop();
                            testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
                            isFirst = false;
                        }
                        // Console.WriteLine(l +", "+ dYear  + ", " + sNationOut + ", " + cNationOut);
                        joinOutputIntermediate.Add(index, cCityOut + "," + sCityOut + "," + dYear);
                        outputRecordsCounter++;
                        if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
                        {
                            swOutputRecorder.Stop();
                            // testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
                            swOutputRecorder.Start();
                        }

                    }
                    catch (Exception)
                    {
                        throw;
                    }
                }
                var joinOutputFinal = new Dictionary<Int64, string>();
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
                //Console.WriteLine("[Invisble Join]: Time taken {0} ms.", sw.ElapsedMilliseconds);
                testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
                testResults.totalNumberOfOutput = joinOutputFinal.Count;
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
                var dateHashTable = new Dictionary<Int64, string>();
                var customerHashTable = new Dictionary<Int64, string>();
                var supplierHashTable = new Dictionary<Int64, string>();
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
                List<Int64> loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase21IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var listOrderDatePositions = new List<Int64>();
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
                sw.Stop();
                testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<Int64> loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase22IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var listCustomerKeyPositions = new List<Int64>();
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
                sw.Stop();
                testResults.phase22ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<Int64> loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase23IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var listSupplierKeyPositions = new List<Int64>();
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
                var common = listCustomerKeyPositions.Intersect(listOrderDatePositions).Intersect(listSupplierKeyPositions).ToList();
                sw.Stop();
                testResults.phase23ProbeTime = sw.ElapsedMilliseconds;
                testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime +
                    testResults.phase22IOTime + testResults.phase22ProbeTime +
                    testResults.phase23IOTime + testResults.phase23ProbeTime;
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
                loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                foreach (var row in dateDimension)
                {
                    if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
                        dateHashTable.Add(row.dDateKey, row.dYear);
                }
                dateDimension.Clear();

                List<string> cCity = Utils.ReadFromBinaryFiles<string>(cCityFile.Replace("BF", "BF" + scaleFactor));
                List<string> sCity = Utils.ReadFromBinaryFiles<string>(sCityFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase3IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var joinOutputIntermediate = new Dictionary<Int64, string>();
                foreach (Int64 index in common)
                {
                    try
                    {
                        var dateKey = loOrderDate[index];
                        var custKey = loCustomerKey[index];
                        var suppKey = loSupplierKey[index];

                        // Position Look UP
                        string dYear;
                        dateHashTable.TryGetValue(dateKey, out dYear);

                        string cCityOut = cCity[custKey];
                        string sCityOut = sCity[suppKey - 1];
                        if (isFirst)
                        {
                            swInitialRecorder.Stop();
                            testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
                            isFirst = false;
                        }
                        // Console.WriteLine(l +", "+ dYear  + ", " + sNationOut + ", " + cNationOut);
                        joinOutputIntermediate.Add(index, cCityOut + "," + sCityOut + "," + dYear);
                        outputRecordsCounter++;
                        if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
                        {
                            swOutputRecorder.Stop();
                            //testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
                            swOutputRecorder.Start();
                        }

                    }
                    catch (Exception)
                    {
                        throw;
                    }
                }
                var joinOutputFinal = new Dictionary<Int64, string>();
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
                //Console.WriteLine("[Invisble Join]: Time taken {0} ms.", sw.ElapsedMilliseconds);
                testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
                testResults.totalNumberOfOutput = joinOutputFinal.Count;
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
                var dateHashTable = new Dictionary<Int64, string>();
                var customerHashTable = new Dictionary<Int64, string>();
                var supplierHashTable = new Dictionary<Int64, string>();
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
                List<Int64> loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase21IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var listOrderDatePositions = new List<Int64>();
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
                sw.Stop();
                testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<Int64> loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase22IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var listCustomerKeyPositions = new List<Int64>();
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
                sw.Stop();
                testResults.phase22ProbeTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                List<Int64> loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase23IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var listSupplierKeyPositions = new List<Int64>();
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
                var common = listCustomerKeyPositions.Intersect(listOrderDatePositions).Intersect(listSupplierKeyPositions).ToList();
                sw.Stop();
                testResults.phase23ProbeTime = sw.ElapsedMilliseconds;
                testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime +
                    testResults.phase22IOTime + testResults.phase22ProbeTime +
                    testResults.phase23IOTime + testResults.phase23ProbeTime;
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
                loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                foreach (var row in dateDimension)
                {
                    if (row.dYearMonth.Equals("Dec1997"))
                        dateHashTable.Add(row.dDateKey, row.dYear);
                }
                dateDimension.Clear();

                List<string> cCity = Utils.ReadFromBinaryFiles<string>(cCityFile.Replace("BF", "BF" + scaleFactor));
                List<string> sCity = Utils.ReadFromBinaryFiles<string>(sCityFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
                sw.Stop();
                testResults.phase3IOTime = sw.ElapsedMilliseconds;
                sw.Reset();

                sw.Start();
                var joinOutputIntermediate = new Dictionary<Int64, string>();
                foreach (Int64 index in common)
                {
                    try
                    {
                        var dateKey = loOrderDate[index];
                        var custKey = loCustomerKey[index];
                        var suppKey = loSupplierKey[index];

                        // Position Look UP
                        string dYear;
                        dateHashTable.TryGetValue(dateKey, out dYear);

                        string cCityOut = cCity[custKey];
                        string sCityOut = sCity[suppKey - 1];
                        if (isFirst)
                        {
                            swInitialRecorder.Stop();
                            testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
                            isFirst = false;
                        }
                        // Console.WriteLine(l +", "+ dYear  + ", " + sNationOut + ", " + cNationOut);
                        joinOutputIntermediate.Add(index, cCityOut + "," + sCityOut + "," + dYear);
                        outputRecordsCounter++;
                        if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
                        {
                            swOutputRecorder.Stop();
                            //testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
                            swOutputRecorder.Start();
                        }

                    }
                    catch (Exception)
                    {
                        throw;
                    }
                }
                var joinOutputFinal = new Dictionary<Int64, string>();
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
                //Console.WriteLine("[Invisble Join]: Time taken {0} ms.", sw.ElapsedMilliseconds);
                testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
                testResults.totalNumberOfOutput = joinOutputFinal.Count;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        //public void Query_3_1(string selectivityRatio)
        //{
        //    try
        //    {
        //        outputRecordsCounter = 0;
        //        var dateHashTable = new Dictionary<Int64, string>();
        //        var customerHashTable = new Dictionary<Int64, string>();
        //        var supplierHashTable = new Dictionary<Int64, string>();
        //        Stopwatch sw = new Stopwatch();
        //        Stopwatch swInitialRecorder = new Stopwatch();
        //        Stopwatch swOutputRecorder = new Stopwatch();

        //        #region Key Hashing Phase
        //        List<Date> dateDimension = null;
        //        List<Supplier> supplierDimension = null;
        //        List<Customer> customerDimension = null;
        //        swInitialRecorder.Start();
        //        swOutputRecorder.Start();
        //        switch (selectivityRatio)
        //        {
        //            case "0.007":
        //                sw.Start();
        //                dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
        //                sw.Stop();
        //                testResults.phase11IOTime = sw.ElapsedMilliseconds;
        //                sw.Reset();

        //                sw.Start();
        //                foreach (var row in dateDimension)
        //                {
        //                    if (row.dYear.Equals("1992"))
        //                        dateHashTable.Add(row.dDateKey, row.dYear);
        //                }
        //                sw.Stop();
        //                testResults.phase11HashTime = sw.ElapsedMilliseconds;
        //                sw.Reset();

        //                sw.Start();
        //                customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
        //                sw.Stop();
        //                testResults.phase12IOTime = sw.ElapsedMilliseconds;
        //                sw.Reset();

        //                sw.Start();
        //                foreach (var row in customerDimension)
        //                {
        //                    if (row.cRegion.Equals("ASIA"))
        //                        customerHashTable.Add(row.cCustKey, row.cNation);
        //                }
        //                sw.Stop();
        //                testResults.phase12HashTime = sw.ElapsedMilliseconds;
        //                sw.Reset();

        //                sw.Start();
        //                supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
        //                sw.Stop();
        //                testResults.phase13IOTime = sw.ElapsedMilliseconds;
        //                sw.Reset();

        //                sw.Start();
        //                foreach (var row in supplierDimension)
        //                {
        //                    if (row.sRegion.Equals("ASIA"))
        //                        supplierHashTable.Add(row.sSuppKey, row.sNation);
        //                }
        //                sw.Stop();
        //                break;
        //            case "0.07":
        //                sw.Start();
        //                dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
        //                sw.Stop();
        //                testResults.phase11IOTime = sw.ElapsedMilliseconds;
        //                sw.Reset();

        //                sw.Start();
        //                foreach (var row in dateDimension)
        //                {
        //                    if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1996") <= 0)
        //                        dateHashTable.Add(row.dDateKey, row.dYear);
        //                }
        //                sw.Stop();
        //                testResults.phase11HashTime = sw.ElapsedMilliseconds;
        //                sw.Reset();

        //                sw.Start();
        //                customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
        //                sw.Stop();
        //                testResults.phase12IOTime = sw.ElapsedMilliseconds;
        //                sw.Reset();

        //                sw.Start();
        //                foreach (var row in customerDimension)
        //                {
        //                    if (row.cRegion.Equals("ASIA") || row.cRegion.Equals("AFRICA"))
        //                        customerHashTable.Add(row.cCustKey, row.cNation);
        //                }
        //                sw.Stop();
        //                testResults.phase12HashTime = sw.ElapsedMilliseconds;
        //                sw.Reset();

        //                sw.Start();
        //                supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
        //                sw.Stop();
        //                testResults.phase13IOTime = sw.ElapsedMilliseconds;
        //                sw.Reset();

        //                sw.Start();
        //                foreach (var row in supplierDimension)
        //                {
        //                    if (row.sRegion.Equals("ASIA"))
        //                        supplierHashTable.Add(row.sSuppKey, row.sNation);
        //                }
        //                sw.Stop();
        //                break;
        //            case "0.7":
        //                sw.Start();
        //                dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
        //                sw.Stop();
        //                testResults.phase11IOTime = sw.ElapsedMilliseconds;
        //                sw.Reset();

        //                sw.Start();
        //                foreach (var row in dateDimension)
        //                {
        //                    if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1999") <= 0)
        //                        dateHashTable.Add(row.dDateKey, row.dYear);
        //                }
        //                sw.Stop();
        //                testResults.phase11HashTime = sw.ElapsedMilliseconds;
        //                sw.Reset();

        //                sw.Start();
        //                customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
        //                sw.Stop();
        //                testResults.phase12IOTime = sw.ElapsedMilliseconds;
        //                sw.Reset();

        //                sw.Start();
        //                foreach (var row in customerDimension)
        //                {
        //                    if (row.cRegion.Equals("ASIA") || row.cRegion.Equals("AMERICA") || row.cRegion.Equals("EUROPE") || row.cRegion.Equals("AFRICA"))
        //                        customerHashTable.Add(row.cCustKey, row.cNation);
        //                }
        //                sw.Stop();
        //                testResults.phase12HashTime = sw.ElapsedMilliseconds;
        //                sw.Reset();

        //                sw.Start();
        //                supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
        //                sw.Stop();
        //                testResults.phase13IOTime = sw.ElapsedMilliseconds;
        //                sw.Reset();

        //                sw.Start();
        //                foreach (var row in supplierDimension)
        //                {
        //                    if (row.sRegion.Equals("ASIA") || row.sRegion.Equals("AMERICA") || row.sRegion.Equals("EUROPE") || row.sRegion.Equals("AFRICA"))
        //                        supplierHashTable.Add(row.sSuppKey, row.sNation);
        //                }
        //                sw.Stop();
        //                break;
        //        }

        //        testResults.phase13HashTime = sw.ElapsedMilliseconds;
        //        testResults.phase1Time = testResults.phase11HashTime + testResults.phase11IOTime +
        //            testResults.phase12HashTime + testResults.phase12IOTime +
        //            testResults.phase13HashTime + testResults.phase13IOTime;
        //        sw.Reset();

        //        customerDimension.Clear();
        //        dateDimension.Clear();
        //        supplierDimension.Clear();

        //        #endregion Key Hashing Phase

        //        #region Probing Phase

        //        sw.Start();
        //        List<Int64> loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
        //        sw.Stop();
        //        testResults.phase21IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        var listOrderDatePositions = new List<Int64>();
        //        var i = 0;
        //        foreach (var orderDate in loOrderDate)
        //        {
        //            string dYear = "";
        //            if (dateHashTable.TryGetValue(orderDate, out dYear))
        //            {
        //                listOrderDatePositions.Add(i);
        //            }
        //            i++;
        //        }
        //        sw.Stop();
        //        testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        List<Int64> loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
        //        sw.Stop();
        //        testResults.phase22IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        var listCustomerKeyPositions = new List<Int64>();
        //        var j = 0;
        //        foreach (var custKey in loCustomerKey)
        //        {
        //            string cNationOut = string.Empty;
        //            if (customerHashTable.TryGetValue(custKey, out cNationOut))
        //            {
        //                listCustomerKeyPositions.Add(j);
        //            }
        //            j++;
        //        }
        //        sw.Stop();
        //        testResults.phase22ProbeTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        List<Int64> loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
        //        sw.Stop();
        //        testResults.phase23IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        var listSupplierKeyPositions = new List<Int64>();
        //        var k = 0;
        //        foreach (var suppKey in loSupplierKey)
        //        {
        //            string sNationOut = string.Empty;
        //            if (supplierHashTable.TryGetValue(suppKey, out sNationOut))
        //            {
        //                listSupplierKeyPositions.Add(k);
        //            }
        //            k++;
        //        }
        //        var common = listCustomerKeyPositions.Intersect(listOrderDatePositions).Intersect(listSupplierKeyPositions).ToList();
        //        sw.Stop();
        //        testResults.phase23ProbeTime = sw.ElapsedMilliseconds;
        //        testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime +
        //            testResults.phase22IOTime + testResults.phase22ProbeTime +
        //            testResults.phase23IOTime + testResults.phase23ProbeTime;
        //        sw.Reset();

        //        loOrderDate.Clear();
        //        dateHashTable.Clear();
        //        loCustomerKey.Clear();
        //        customerHashTable.Clear();
        //        loSupplierKey.Clear();
        //        supplierHashTable.Clear();

        //        #endregion Probing Phase

        //        #region Value Extraction Phase

        //        sw.Start();
        //        loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
        //        loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
        //        loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
        //        List<string> cNation = Utils.ReadFromBinaryFiles<string>(cNationFile.Replace("BF", "BF" + scaleFactor));
        //        List<string> sNation = Utils.ReadFromBinaryFiles<string>(sNationFile.Replace("BF", "BF" + scaleFactor));
        //        switch (selectivityRatio)
        //        {
        //            case "0.007":
        //                dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
        //                foreach (var row in dateDimension)
        //                {
        //                    if (row.dYear.Equals("1992"))
        //                        dateHashTable.Add(row.dDateKey, row.dYear);
        //                }
        //                break;
        //            case "0.07":
        //                dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
        //                foreach (var row in dateDimension)
        //                {
        //                    if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1996") <= 0)
        //                        dateHashTable.Add(row.dDateKey, row.dYear);
        //                }
        //                break;
        //            case "0.7":
        //                dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
        //                foreach (var row in dateDimension)
        //                {
        //                    if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1999") <= 0)
        //                        dateHashTable.Add(row.dDateKey, row.dYear);
        //                }
        //                break;
        //        }
        //        dateDimension.Clear();
        //        List<Int64> loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
        //        sw.Stop();
        //        testResults.phase3IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        var joinOutputIntermediate = new Dictionary<Int64, string>();
        //        foreach (Int64 index in common)
        //        {

        //            try
        //            {
        //                var dateKey = loOrderDate[index];
        //                var custKey = loCustomerKey[index];
        //                var suppKey = loSupplierKey[index];

        //                // Position Look UP
        //                string dYear;
        //                dateHashTable.TryGetValue(dateKey, out dYear);

        //                string cNationOut = cNation[custKey];
        //                string sNationOut = sNation[suppKey - 1];
        //                if (isFirst)
        //                {
        //                    swInitialRecorder.Stop();
        //                    testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
        //                    isFirst = false;
        //                }
        //                // Console.WriteLine(l +", "+ dYear  + ", " + sNationOut + ", " + cNationOut);
        //                joinOutputIntermediate.Add(index, cNationOut + "," + sNationOut + "," + dYear);
        //                outputRecordsCounter++;
        //                if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
        //                {
        //                    swOutputRecorder.Stop();
        //                    testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
        //                    swOutputRecorder.Start();
        //                }
        //            }
        //            catch (Exception)
        //            {
        //                throw;
        //            }
        //        }
        //        var joinOutputFinal = new Dictionary<Int64, string>();
        //        foreach (var item in joinOutputIntermediate)
        //        {
        //            joinOutputFinal.Add(item.Key, item.Value + "," + loRevenue[item.Key]);
        //        }
        //        sw.Stop();
        //        testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
        //        testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;

        //        #endregion Value Extraction Phase
        //        testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
        //        // Console.WriteLine("[Invisble Join]: Time taken {0} ms.", sw.ElapsedMilliseconds);
        //    }
        //    catch (Exception ex)
        //    {
        //        Console.WriteLine(ex.ToString());
        //    }
        //}

        public void AggregationScalabilityTest1(Int64 numberOfAggregations)
        {
            try
            {
                Stopwatch sw = new Stopwatch();

                List<Int64> loTax = null;
                List<Int64> loDiscount = null;
                List<Int64> loQuantity = null;
                List<Int64> loSupplyCost = null;
                List<Int64> loRevenue = null;
                List<Int64> loOrderTotalPrice = null;
                List<Int64> loCommitDate = Utils.ReadFromBinaryFiles<Int64>(loCommitDateFile.Replace("BF", "BF" + scaleFactor));
                switch (numberOfAggregations)
                {
                    case 1:
                        loTax = Utils.ReadFromBinaryFiles<Int64>(loTaxFile.Replace("BF", "BF" + scaleFactor));
                        break;
                    case 2:
                        loTax = Utils.ReadFromBinaryFiles<Int64>(loTaxFile.Replace("BF", "BF" + scaleFactor));
                        loDiscount = Utils.ReadFromBinaryFiles<Int64>(loDiscountFile.Replace("BF", "BF" + scaleFactor));
                        break;
                    case 3:
                        loTax = Utils.ReadFromBinaryFiles<Int64>(loTaxFile.Replace("BF", "BF" + scaleFactor));
                        loDiscount = Utils.ReadFromBinaryFiles<Int64>(loDiscountFile.Replace("BF", "BF" + scaleFactor));
                        loQuantity = Utils.ReadFromBinaryFiles<Int64>(loQuantityFile.Replace("BF", "BF" + scaleFactor));
                        break;
                    case 4:
                        loTax = Utils.ReadFromBinaryFiles<Int64>(loTaxFile.Replace("BF", "BF" + scaleFactor));
                        loDiscount = Utils.ReadFromBinaryFiles<Int64>(loDiscountFile.Replace("BF", "BF" + scaleFactor));
                        loQuantity = Utils.ReadFromBinaryFiles<Int64>(loQuantityFile.Replace("BF", "BF" + scaleFactor));
                        loSupplyCost = Utils.ReadFromBinaryFiles<Int64>(loSupplyCostFile.Replace("BF", "BF" + scaleFactor));
                        break;
                    case 5:
                        loTax = Utils.ReadFromBinaryFiles<Int64>(loTaxFile.Replace("BF", "BF" + scaleFactor));
                        loDiscount = Utils.ReadFromBinaryFiles<Int64>(loDiscountFile.Replace("BF", "BF" + scaleFactor));
                        loQuantity = Utils.ReadFromBinaryFiles<Int64>(loQuantityFile.Replace("BF", "BF" + scaleFactor));
                        loSupplyCost = Utils.ReadFromBinaryFiles<Int64>(loSupplyCostFile.Replace("BF", "BF" + scaleFactor));
                        loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
                        break;
                    case 6:
                        loTax = Utils.ReadFromBinaryFiles<Int64>(loTaxFile.Replace("BF", "BF" + scaleFactor));
                        loDiscount = Utils.ReadFromBinaryFiles<Int64>(loDiscountFile.Replace("BF", "BF" + scaleFactor));
                        loQuantity = Utils.ReadFromBinaryFiles<Int64>(loQuantityFile.Replace("BF", "BF" + scaleFactor));
                        loSupplyCost = Utils.ReadFromBinaryFiles<Int64>(loSupplyCostFile.Replace("BF", "BF" + scaleFactor));
                        loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
                        loOrderTotalPrice = Utils.ReadFromBinaryFiles<Int64>(loOrdTotalPriceFile.Replace("BF", "BF" + scaleFactor));
                        break;
                }

                #region Value Extraction Phase
                sw.Start();
                var joinOutputFinal = new Dictionary<Int64, Int64[]>();

                for (Int64 i = 0; i < loCommitDate.Count; i++)
                {
                    Int64 commitDate = loCommitDate[i];
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
                Console.WriteLine(String.Format("[Invisible Join] Total Time: {0}", t2));
                // Console.WriteLine(String.Format("[Invisible Join] Total Time: {0}", t0 + t1 + t2));
                Console.WriteLine(String.Format("[Invisible Join] Total : {0}", joinOutputFinal.Count));
                Console.WriteLine();
                #endregion Value Extraction Phase
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void AggregationScalabilityTest2(Int64 numberOfAggregations)
        {
            try
            {
                Stopwatch sw = new Stopwatch();

                List<Int64> loTax = null;
                List<Int64> loDiscount = null;
                List<Int64> loQuantity = null;
                List<Int64> loSupplyCost = null;
                List<Int64> loRevenue = null;
                List<Int64> loOrderTotalPrice = null;
                List<Int64> loCommitDate = Utils.ReadFromBinaryFiles<Int64>(loCommitDateFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loPartKey = Utils.ReadFromBinaryFiles<Int64>(loPartKeyFile.Replace("BF", "BF" + scaleFactor));
                List<Customer> customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
                List<Part> partDimension = Utils.ReadFromBinaryFiles<Part>(partFile.Replace("BF", "BF" + scaleFactor));

                switch (numberOfAggregations)
                {
                    case 1:
                        loTax = Utils.ReadFromBinaryFiles<Int64>(loTaxFile.Replace("BF", "BF" + scaleFactor));
                        break;
                    case 2:
                        loTax = Utils.ReadFromBinaryFiles<Int64>(loTaxFile.Replace("BF", "BF" + scaleFactor));
                        loDiscount = Utils.ReadFromBinaryFiles<Int64>(loDiscountFile.Replace("BF", "BF" + scaleFactor));
                        break;
                    case 3:
                        loTax = Utils.ReadFromBinaryFiles<Int64>(loTaxFile.Replace("BF", "BF" + scaleFactor));
                        loDiscount = Utils.ReadFromBinaryFiles<Int64>(loDiscountFile.Replace("BF", "BF" + scaleFactor));
                        loQuantity = Utils.ReadFromBinaryFiles<Int64>(loQuantityFile.Replace("BF", "BF" + scaleFactor));
                        break;
                    case 4:
                        loTax = Utils.ReadFromBinaryFiles<Int64>(loTaxFile.Replace("BF", "BF" + scaleFactor));
                        loDiscount = Utils.ReadFromBinaryFiles<Int64>(loDiscountFile.Replace("BF", "BF" + scaleFactor));
                        loQuantity = Utils.ReadFromBinaryFiles<Int64>(loQuantityFile.Replace("BF", "BF" + scaleFactor));
                        loSupplyCost = Utils.ReadFromBinaryFiles<Int64>(loSupplyCostFile.Replace("BF", "BF" + scaleFactor));
                        break;
                    case 5:
                        loTax = Utils.ReadFromBinaryFiles<Int64>(loTaxFile.Replace("BF", "BF" + scaleFactor));
                        loDiscount = Utils.ReadFromBinaryFiles<Int64>(loDiscountFile.Replace("BF", "BF" + scaleFactor));
                        loQuantity = Utils.ReadFromBinaryFiles<Int64>(loQuantityFile.Replace("BF", "BF" + scaleFactor));
                        loSupplyCost = Utils.ReadFromBinaryFiles<Int64>(loSupplyCostFile.Replace("BF", "BF" + scaleFactor));
                        loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
                        break;
                    case 6:
                        loTax = Utils.ReadFromBinaryFiles<Int64>(loTaxFile.Replace("BF", "BF" + scaleFactor));
                        loDiscount = Utils.ReadFromBinaryFiles<Int64>(loDiscountFile.Replace("BF", "BF" + scaleFactor));
                        loQuantity = Utils.ReadFromBinaryFiles<Int64>(loQuantityFile.Replace("BF", "BF" + scaleFactor));
                        loSupplyCost = Utils.ReadFromBinaryFiles<Int64>(loSupplyCostFile.Replace("BF", "BF" + scaleFactor));
                        loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
                        loOrderTotalPrice = Utils.ReadFromBinaryFiles<Int64>(loOrdTotalPriceFile.Replace("BF", "BF" + scaleFactor));
                        break;
                }

                sw.Start();
                #region Key Hashing Phase 

                var customerHashTable = new Dictionary<Int64, string>();
                var partHashTable = new Dictionary<Int64, string>();


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
                Console.WriteLine(String.Format("[Invisible Join] AGTest2 T0 Time: {0}", t0));
                #endregion Key Hashing Phase

                #region Probing Phase
                sw.Reset();
                sw.Start();

                var listCustomerKeyPositions = new BitArray(loCustomerKey.Count);
                var j = 0;
                foreach (var custKey in loCustomerKey)
                {
                    string cNationOut = string.Empty;
                    if (customerHashTable.TryGetValue(custKey, out cNationOut))
                    {
                        listCustomerKeyPositions.Set(j, true);
                    }
                    j++;
                }

                var listPartKeyPositions = new BitArray(loPartKey.Count);
                var k = 0;
                foreach (var partKey in loPartKey)
                {
                    string pMFGR = string.Empty;
                    if (partHashTable.TryGetValue(partKey, out pMFGR))
                    {
                        listPartKeyPositions.Set(k, true);
                    }
                    k++;
                }

                var common = listCustomerKeyPositions.And(listPartKeyPositions);
                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[Invisible Join] AGTest2 T1 Time: {0}", t1));
                sw.Reset();

                #endregion Probing Phase

                #region Value Extraction Phase
                sw.Start();
                var joinOutputFinal = new Dictionary<string, long[]>();
                Int64 i = 0;
                foreach (bool bit in common)
                {

                    try
                    {
                        if (bit)
                        {
                            var custKey = loCustomerKey[i];
                            var partKey = loPartKey[i];

                            // Position Look UP
                            string cRegionOut;
                            customerHashTable.TryGetValue(custKey, out cRegionOut);
                            string pMFGROut;
                            partHashTable.TryGetValue(partKey, out pMFGROut);
                            string key = cRegionOut + ", " + pMFGROut;
                            long[] values = null;
                            if (joinOutputFinal.TryGetValue(key, out values))
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
                                values = new long[numberOfAggregations];
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
                                joinOutputFinal.Add(key, values);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(i);
                        throw;
                    }
                    i++;
                }

                sw.Stop();
                long t2 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[Invisible Join] AGTest2 T2 Time: {0}", t2));
                Console.WriteLine(String.Format("[Invisible Join] AGTest2 Total Time: {0}", t0 + t1 + t2));
                Console.WriteLine(String.Format("[Invisible Join] AGTest2 Total : {0}", joinOutputFinal.Count));
                Console.WriteLine();
                #endregion Value Extraction Phase
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void JoinScalabilityTest(Int64 numberOfJoins)
        {
            try
            {
                Stopwatch sw = new Stopwatch();
                List<Customer> customerDimension = null;
                List<Supplier> supplierDimension = null;
                List<Date> dateDimension = null;
                List<Part> partDimension = null;
                List<Int64> loCustomerKey = null;
                List<Int64> loSupplierKey = null;
                List<Int64> loOrderDate = null;
                List<Int64> loPartKey = null;
                switch (numberOfJoins)
                {
                    case 1:
                        customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));

                        loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                        break;
                    case 2:
                        customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
                        supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));

                        loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                        loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                        break;
                    case 3:
                        customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
                        supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
                        dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));

                        loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                        loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                        loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                        break;
                    case 4:
                        customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
                        supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
                        dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                        partDimension = Utils.ReadFromBinaryFiles<Part>(partFile.Replace("BF", "BF" + scaleFactor));

                        loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                        loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                        loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                        loPartKey = Utils.ReadFromBinaryFiles<Int64>(loPartKeyFile.Replace("BF", "BF" + scaleFactor));

                        break;

                }

                List<Int64> loTax = Utils.ReadFromBinaryFiles<Int64>(loTaxFile.Replace("BF", "BF" + scaleFactor));

                sw.Start();
                #region Key Hashing Phase 

                var customerHashTable = new Dictionary<Int64, string>();
                var supplierHashTable = new Dictionary<Int64, string>();
                var dateHashTable = new Dictionary<Int64, string>();
                var partHashTable = new Dictionary<Int64, string>();
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
                Console.WriteLine(String.Format("[Invisible Join] JSTest T0 Time: {0}", t0));
                #endregion Key Hashing Phase

                #region Probing Phase
                sw.Reset();
                sw.Start();

                var listOrderDatePositions = new BitArray(loCustomerKey.Count);
                var listCustomerKeyPositions = new BitArray(loCustomerKey.Count);
                var listSupplierKeyPositions = new BitArray(loCustomerKey.Count);
                var listPartKeyPositions = new BitArray(loCustomerKey.Count);
                BitArray common = new BitArray(loCustomerKey.Count); ;
                var i = 0;
                var j = 0;
                var k = 0;
                var l = 0;
                switch (numberOfJoins)
                {
                    case 1:
                        j = 0;
                        foreach (var custKey in loCustomerKey)
                        {
                            string cRegionOut = string.Empty;
                            if (customerHashTable.TryGetValue(custKey, out cRegionOut))
                            {
                                listCustomerKeyPositions.Set(j, true);
                            }
                            j++;
                        }
                        common = listCustomerKeyPositions;
                        break;
                    case 2:
                        j = 0;
                        foreach (var custKey in loCustomerKey)
                        {
                            string cRegionOut = string.Empty;
                            if (customerHashTable.TryGetValue(custKey, out cRegionOut))
                            {
                                listCustomerKeyPositions.Set(j, true);
                            }
                            j++;
                        }

                        k = 0;
                        foreach (var suppKey in loSupplierKey)
                        {
                            string sRegionOut = string.Empty;
                            if (supplierHashTable.TryGetValue(suppKey, out sRegionOut))
                            {
                                listSupplierKeyPositions.Set(k, true);
                            }
                            k++;
                        }
                        common = listCustomerKeyPositions.And(listSupplierKeyPositions);
                        break;
                    case 3:
                        j = 0;
                        foreach (var custKey in loCustomerKey)
                        {
                            string cRegionOut = string.Empty;
                            if (customerHashTable.TryGetValue(custKey, out cRegionOut))
                            {
                                listCustomerKeyPositions.Set(j, true);
                            }
                            j++;
                        }

                        k = 0;
                        foreach (var suppKey in loSupplierKey)
                        {
                            string sRegionOut = string.Empty;
                            if (supplierHashTable.TryGetValue(suppKey, out sRegionOut))
                            {
                                listSupplierKeyPositions.Set(k, true);
                            }
                            k++;
                        }
                        i = 0;
                        foreach (var orderDate in loOrderDate)
                        {
                            string dYear = "";
                            if (dateHashTable.TryGetValue(orderDate, out dYear))
                            {
                                listOrderDatePositions.Set(i, true);
                            }
                            i++;
                        }
                        common = listCustomerKeyPositions.And(listSupplierKeyPositions).And(listOrderDatePositions);
                        break;
                    case 4:
                        j = 0;
                        foreach (var custKey in loCustomerKey)
                        {
                            string cRegionOut = string.Empty;
                            if (customerHashTable.TryGetValue(custKey, out cRegionOut))
                            {
                                listCustomerKeyPositions.Set(j, true);
                            }
                            j++;
                        }

                        k = 0;
                        foreach (var suppKey in loSupplierKey)
                        {
                            string sRegionOut = string.Empty;
                            if (supplierHashTable.TryGetValue(suppKey, out sRegionOut))
                            {
                                listSupplierKeyPositions.Set(k, true);
                            }
                            k++;
                        }
                        i = 0;
                        foreach (var orderDate in loOrderDate)
                        {
                            string dYear = "";
                            if (dateHashTable.TryGetValue(orderDate, out dYear))
                            {
                                listOrderDatePositions.Set(i, true);
                            }
                            i++;
                        }
                        l = 0;
                        foreach (var partKey in loPartKey)
                        {
                            string pMFGROut = string.Empty;
                            if (partHashTable.TryGetValue(partKey, out pMFGROut))
                            {
                                listPartKeyPositions.Set(l, true);
                            }
                            l++;
                        }
                        common = listCustomerKeyPositions.And(listSupplierKeyPositions).And(listOrderDatePositions).And(listPartKeyPositions);
                        break;
                }

                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[Invisible Join] JSTest T1 Time: {0}", t1));
                sw.Reset();
                #endregion Probing Phase

                #region Value Extraction Phase
                sw.Start();
                var joinOutputFinal = new Dictionary<string, long>();
                Int64 index = 0;
                Int64 setBitCount = 0;
                foreach (bool bit in common)
                {
                    try
                    {
                        if (bit)
                        {
                            setBitCount++;
                            var dateKey = 0;
                            var custKey = 0;
                            var suppKey = 0;
                            var partKey = 0;
                            string cRegionOut;
                            string sRegionOut;
                            string dYear;
                            string pMFGROut;
                            string key = string.Empty;
                            switch (numberOfJoins)
                            {
                                case 1:
                                    custKey = loCustomerKey[index];
                                    customerHashTable.TryGetValue(custKey, out cRegionOut);
                                    key = cRegionOut;

                                    break;
                                case 2:
                                    custKey = loCustomerKey[index];
                                    suppKey = loSupplierKey[index];
                                    customerHashTable.TryGetValue(custKey, out cRegionOut);
                                    supplierHashTable.TryGetValue(suppKey, out sRegionOut);
                                    key = cRegionOut + ", " + sRegionOut;

                                    break;
                                case 3:
                                    dateKey = loOrderDate[index];
                                    custKey = loCustomerKey[index];
                                    suppKey = loSupplierKey[index];

                                    dateHashTable.TryGetValue(dateKey, out dYear);
                                    customerHashTable.TryGetValue(custKey, out cRegionOut);
                                    supplierHashTable.TryGetValue(suppKey, out sRegionOut);
                                    key = cRegionOut + ", " + sRegionOut + ", " + dYear;

                                    break;
                                case 4:
                                    dateKey = loOrderDate[index];
                                    custKey = loCustomerKey[index];
                                    suppKey = loSupplierKey[index];
                                    partKey = loPartKey[index];
                                    // Position Look UP
                                    dateHashTable.TryGetValue(dateKey, out dYear);
                                    customerHashTable.TryGetValue(custKey, out cRegionOut);
                                    supplierHashTable.TryGetValue(suppKey, out sRegionOut);
                                    partHashTable.TryGetValue(partKey, out pMFGROut);

                                    key = cRegionOut + ", " + sRegionOut + ", " + dYear + ", " + pMFGROut;
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
                Console.WriteLine(String.Format("[Invisible Join] JSTest T2 Time: {0}", t2));
                Console.WriteLine(String.Format("[Invisible Join] JSTest Total Time: {0}", t0 + t1 + t2));
                Console.WriteLine(String.Format("[Invisible Join] JSTest Set BIT Count : {0}", setBitCount));
                Console.WriteLine(String.Format("[Invisible Join] JSTest Total : {0}", joinOutputFinal.Count));
                Console.WriteLine();
                #endregion Value Extraction Phase
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void GroupingAttributeScalabilityTest(Int64 numberOfGroupingAttributes)
        {
            try
            {
                Stopwatch sw = new Stopwatch();
                List<Customer> customerDimension = null;
                List<Supplier> supplierDimension = null;
                List<Date> dateDimension = null;
                List<Int64> loCustomerKey = null;
                List<Int64> loSupplierKey = null;
                List<Int64> loOrderDate = null;

                customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
                supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
                dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));

                loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));

                List<Int64> loTax = Utils.ReadFromBinaryFiles<Int64>(loTaxFile.Replace("BF", "BF" + scaleFactor));

                sw.Start();
                #region Key Hashing Phase 

                var customerHashTable = new Dictionary<Int64, Tuple<string, string>>();
                var supplierHashTable = new Dictionary<Int64, Tuple<string, string>>();
                var dateHashTable = new Dictionary<Int64, Tuple<string, string>>();
                var partHashTable = new Dictionary<Int64, Tuple<string, string>>();

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
                Console.WriteLine(String.Format("[Invisible Join] GATest T0 Time: {0}", t0));
                #endregion Key Hashing Phase

                #region Probing Phase
                sw.Reset();
                sw.Start();

                var listOrderDatePositions = new BitArray(loCustomerKey.Count);
                var listCustomerKeyPositions = new BitArray(loCustomerKey.Count);
                var listSupplierKeyPositions = new BitArray(loCustomerKey.Count);
                BitArray common = new BitArray(loCustomerKey.Count); ;
                var i = 0;
                var j = 0;
                var k = 0;
                var l = 0;

                j = 0;
                foreach (var custKey in loCustomerKey)
                {
                    Tuple<string, string> cOut = null;
                    if (customerHashTable.TryGetValue(custKey, out cOut))
                    {
                        listCustomerKeyPositions.Set(j, true);
                    }
                    j++;
                }

                k = 0;
                foreach (var suppKey in loSupplierKey)
                {
                    Tuple<string, string> sOut = null;
                    if (supplierHashTable.TryGetValue(suppKey, out sOut))
                    {
                        listSupplierKeyPositions.Set(k, true);
                    }
                    k++;
                }

                i = 0;
                foreach (var orderDate in loOrderDate)
                {
                    Tuple<string, string> dOut = null;
                    if (dateHashTable.TryGetValue(orderDate, out dOut))
                    {
                        listOrderDatePositions.Set(i, true);
                    }
                    i++;
                }

                common = listCustomerKeyPositions.And(listSupplierKeyPositions).And(listOrderDatePositions);


                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[Invisible Join] GSTest T1 Time: {0}", t1));
                sw.Reset();
                #endregion Probing Phase

                #region Value Extraction Phase
                sw.Start();
                var joinOutputFinal = new Dictionary<string, long>();
                Int64 index = 0;
                Int64 setBitCount = 0;
                foreach (bool bit in common)
                {
                    try
                    {
                        if (bit)
                        {
                            setBitCount++;
                            var dateKey = 0;
                            var custKey = 0;
                            var suppKey = 0;
                            Tuple<string, string> cOut;
                            Tuple<string, string> sOut;
                            Tuple<string, string> dOut;
                            string key = string.Empty;

                            dateKey = loOrderDate[index];
                            custKey = loCustomerKey[index];
                            suppKey = loSupplierKey[index];
                            // Position Look UP
                            dateHashTable.TryGetValue(dateKey, out dOut);
                            customerHashTable.TryGetValue(custKey, out cOut);
                            supplierHashTable.TryGetValue(suppKey, out sOut);

                            switch (numberOfGroupingAttributes)
                            {
                                case 1:
                                    key = cOut.Item1;
                                    break;
                                case 2:
                                    key = cOut.Item1 + ", " + cOut.Item2;
                                    break;
                                case 3:
                                    key = cOut.Item1 + ", " + cOut.Item2 + ", " + sOut.Item1;
                                    break;
                                case 4:
                                    key = cOut.Item1 + ", " + cOut.Item2 + ", " + sOut.Item1 + ", " + sOut.Item2;
                                    break;
                                case 5:
                                    key = cOut.Item1 + ", " + cOut.Item2 + ", " + sOut.Item1 + ", " + sOut.Item2 + ", " + dOut.Item1;
                                    break;
                                case 6:
                                    key = cOut.Item1 + ", " + cOut.Item2 + ", " + sOut.Item1 + ", " + sOut.Item2 + ", " + dOut.Item1 + ", " + dOut.Item2;
                                    break;
                                default:
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
                Console.WriteLine(String.Format("[Invisible Join] GSTest T2 Time: {0}", t2));
                Console.WriteLine(String.Format("[Invisible Join] GSTest Total Time: {0}", t0 + t1 + t2));
                // Console.WriteLine(String.Format("[Invisible Join] GSTest Set BIT Count : {0}", setBitCount));
                Console.WriteLine(String.Format("[Invisible Join] GSTest Total : {0}", joinOutputFinal.Count));
                Console.WriteLine();
                #endregion Value Extraction Phase
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }


        /// <summary>
        /// IM refers inmemory version of the algorithm
        /// </summary>
        public void Query_3_1_IM()
        {
            try
            {
                Stopwatch sw = new Stopwatch();

                List<Customer> customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
                List<Supplier> supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
                List<Date> dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
                List<string> cNation = Utils.ReadFromBinaryFiles<string>(cNationFile.Replace("BF", "BF" + scaleFactor));
                List<string> sNation = Utils.ReadFromBinaryFiles<string>(sNationFile.Replace("BF", "BF" + scaleFactor));

                sw.Start();
                #region Key Hashing Phase 

                var customerHashTable = new Dictionary<Int64, string>();
                var supplierHashTable = new Dictionary<Int64, string>();
                var dateHashTable = new Dictionary<Int64, string>();

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
                Console.WriteLine(String.Format("[Invisible Join] T0 Time: {0}", t0));
                #endregion Key Hashing Phase

                #region Probing Phase
                sw.Reset();
                sw.Start();

                var listOrderDatePositions = new BitArray(loOrderDate.Count);
                var i = 0;
                foreach (var orderDate in loOrderDate)
                {
                    string dYear = "";
                    if (dateHashTable.TryGetValue(orderDate, out dYear))
                    {
                        listOrderDatePositions.Set(i, true);
                    }
                    i++;
                }

                var listCustomerKeyPositions = new BitArray(loCustomerKey.Count);
                var j = 0;
                foreach (var custKey in loCustomerKey)
                {
                    string cNationOut = string.Empty;
                    if (customerHashTable.TryGetValue(custKey, out cNationOut))
                    {
                        listCustomerKeyPositions.Set(j, true);
                    }
                    j++;
                }

                var listSupplierKeyPositions = new BitArray(loSupplierKey.Count);
                var k = 0;
                foreach (var suppKey in loSupplierKey)
                {
                    string sNationOut = string.Empty;
                    if (supplierHashTable.TryGetValue(suppKey, out sNationOut))
                    {
                        listSupplierKeyPositions.Set(k, true);
                    }
                    k++;
                }

                var common = listCustomerKeyPositions.And(listOrderDatePositions).And(listSupplierKeyPositions);
                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[Invisible Join] T1 Time: {0}", t1));
                sw.Reset();

                #endregion Probing Phase


                #region Value Extraction Phase
                sw.Start();
                var joinOutputFinal = new Dictionary<string, long>();
                Int64 index = 0;
                foreach (bool bit in common)
                {

                    try
                    {
                        if (bit)
                        {
                            var dateKey = loOrderDate[index];
                            var custKey = loCustomerKey[index];
                            var suppKey = loSupplierKey[index];

                            // Position Look UP
                            string dYear;
                            dateHashTable.TryGetValue(dateKey, out dYear);
                            string cNationOut;
                            customerHashTable.TryGetValue(custKey, out cNationOut);
                            string sNationOut;
                            supplierHashTable.TryGetValue(suppKey, out sNationOut);
                            string key = cNationOut + ", " + sNationOut + ", " + dYear;
                            long revenue = 0;
                            if (joinOutputFinal.TryGetValue(key, out revenue))
                            {
                                joinOutputFinal[key] = revenue + loRevenue[index];
                            }
                            else
                            {
                                joinOutputFinal.Add(key, loRevenue[index]);
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
                Console.WriteLine(String.Format("[Invisible Join] T2 Time: {0}", t2));
                Console.WriteLine(String.Format("[Invisible Join] Total Time: {0}", t0 + t1 + t2));
                // Console.WriteLine(String.Format("[Invisible Join] Total : {0}", joinOutputFinal.Count));
                Console.WriteLine();
                #endregion Value Extraction Phase
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void Query_3_2_IM()
        {
            try
            {
                Stopwatch sw = new Stopwatch();

                List<Customer> customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
                List<Supplier> supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
                List<Date> dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
                List<string> cNation = Utils.ReadFromBinaryFiles<string>(cNationFile.Replace("BF", "BF" + scaleFactor));
                List<string> sNation = Utils.ReadFromBinaryFiles<string>(sNationFile.Replace("BF", "BF" + scaleFactor));

                sw.Start();
                #region Key Hashing Phase 

                var customerHashTable = new Dictionary<Int64, string>();
                var supplierHashTable = new Dictionary<Int64, string>();
                var dateHashTable = new Dictionary<Int64, string>();

                foreach (var row in dateDimension)
                {
                    if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
                        dateHashTable.Add(row.dDateKey, row.dYear);
                }

                foreach (var row in customerDimension)
                {
                    if (row.cNation.Equals("UNITED STATES"))
                        customerHashTable.Add(row.cCustKey, row.cCity);
                }

                foreach (var row in supplierDimension)
                {
                    if (row.sNation.Equals("UNITED STATES"))
                        supplierHashTable.Add(row.sSuppKey, row.sCity);
                }

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[Invisible Join] T0 Time: {0}", t0));
                #endregion Key Hashing Phase

                #region Probing Phase
                sw.Reset();
                sw.Start();

                var listOrderDatePositions = new BitArray(loOrderDate.Count);
                var i = 0;
                foreach (var orderDate in loOrderDate)
                {
                    string dYear = "";
                    if (dateHashTable.TryGetValue(orderDate, out dYear))
                    {
                        listOrderDatePositions.Set(i, true);
                    }
                    i++;
                }

                var listCustomerKeyPositions = new BitArray(loCustomerKey.Count);
                var j = 0;
                foreach (var custKey in loCustomerKey)
                {
                    string cCityOut = string.Empty;
                    if (customerHashTable.TryGetValue(custKey, out cCityOut))
                    {
                        listCustomerKeyPositions.Set(j, true);
                    }
                    j++;
                }

                var listSupplierKeyPositions = new BitArray(loSupplierKey.Count);
                var k = 0;
                foreach (var suppKey in loSupplierKey)
                {
                    string sCityOut = string.Empty;
                    if (supplierHashTable.TryGetValue(suppKey, out sCityOut))
                    {
                        listSupplierKeyPositions.Set(k, true);
                    }
                    k++;
                }

                var common = listCustomerKeyPositions.And(listOrderDatePositions).And(listSupplierKeyPositions);
                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[Invisible Join] T1 Time: {0}", t1));
                sw.Reset();

                #endregion Probing Phase


                #region Value Extraction Phase
                sw.Start();
                var joinOutputFinal = new Dictionary<string, long>();
                Int64 index = 0;
                foreach (bool bit in common)
                {
                    try
                    {
                        if (bit)
                        {
                            var dateKey = loOrderDate[index];
                            var custKey = loCustomerKey[index];
                            var suppKey = loSupplierKey[index];

                            // Position Look UP
                            string dYear;
                            dateHashTable.TryGetValue(dateKey, out dYear);
                            string cCityOut;
                            customerHashTable.TryGetValue(custKey, out cCityOut);
                            string sCityOut;
                            supplierHashTable.TryGetValue(suppKey, out sCityOut);
                            string key = cCityOut + ", " + sCityOut + ", " + dYear;
                            long revenue = 0;
                            if (joinOutputFinal.TryGetValue(key, out revenue))
                            {
                                joinOutputFinal[key] = revenue + loRevenue[index];
                            }
                            else
                            {
                                joinOutputFinal.Add(key, loRevenue[index]);
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
                Console.WriteLine(String.Format("[Invisible Join] T2 Time: {0}", t2));
                Console.WriteLine(String.Format("[Invisible Join] Total Time: {0}", t0 + t1 + t2));
                // Console.WriteLine(String.Format("[Invisible Join] Total : {0}", joinOutputFinal.Count));
                Console.WriteLine();
                #endregion Value Extraction Phase
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        private void saveAndPrInt64Results()
        {
            TestResultsDatabase.invisibleJoinOutput.Add(testResults.toString());
            Console.WriteLine("Invisible: " + testResults.toString());
            Console.WriteLine();
        }
    }
}
