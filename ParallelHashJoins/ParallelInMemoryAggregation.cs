using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParallelHashJoins
{
    class ParallelInMemoryAggregation
    {
        private ParallelOptions parallelOptions = null;
        private static string binaryFilesDirectory = @"C:\Raw_Data_Source_For_Test\SSBM - DBGEN\BF";
        private string scaleFactor { get; set; }

        public TestResults testResults = new TestResults();
        public ParallelInMemoryAggregation(string scaleFactor, Int32 degreeOfParallelism = 1)
        {
            this.scaleFactor = scaleFactor;
            testResults.totalRAMAvailable = Utils.getAvailableRAM();
            parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = degreeOfParallelism };
        }

        ~ParallelInMemoryAggregation()
        {
            saveAndPrInt64Results();
        }

        #region Private Variables
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

        #endregion Private Variables

        public void Query_2_1_IM()
        {

            Stopwatch sw = new Stopwatch();

            List<Part> partDimension = Utils.ReadFromBinaryFiles<Part>(partFile.Replace("BF", "BF" + scaleFactor));
            List<Supplier> supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
            List<Date> dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));


            #region Step 1 & 2
            sw.Start();
            Dictionary<Int64, Int64> kvPartDim = new Dictionary<Int64, Int64>();
            DataTable tempTablePartDim = new DataTable();
            tempTablePartDim.Columns.Add("partBrand", typeof(string));
            tempTablePartDim.Columns.Add("denseGroupingKey", typeof(Int64));

            Dictionary<Int64, Int64> kvSupplierDim = new Dictionary<Int64, Int64>();
            DataTable tempTableSupplierDim = new DataTable();
            tempTableSupplierDim.Columns.Add("supplierNation", typeof(string));
            tempTableSupplierDim.Columns.Add("denseGroupingKey", typeof(Int64));

            Dictionary<Int64, Int64> kvDateDim = new Dictionary<Int64, Int64>();
            DataTable tempTableDateDim = new DataTable();
            tempTableDateDim.Columns.Add("year", typeof(string));
            tempTableDateDim.Columns.Add("denseGroupingKey", typeof(Int64));

            Parallel.Invoke(parallelOptions,
                () =>
                {
                    Int64 partIndex = 0;
                    Int64 dgKeyPart = 0;
                    foreach (var part in partDimension)
                    {
                        if (part.pCategory.Equals("MFGR#12"))
                        {
                            string pBrand = part.pBrand;
                            if (tempTablePartDim.Rows.Count > 0)
                            {
                                var tempTable = tempTablePartDim.Copy();
                                var found = false;
                                foreach (DataRow row in tempTablePartDim.Rows)
                                {
                                    var partBrand = row.Field<string>("partBrand");
                                    if (partBrand.Equals(pBrand))
                                    {
                                        Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                        kvPartDim.Add(partIndex + 1, dgKey);
                                        found = true;
                                        break;
                                    }
                                }
                                if (!found)
                                {
                                    dgKeyPart++;
                                    tempTable.Rows.Add(pBrand, dgKeyPart);
                                    kvPartDim.Add(partIndex + 1, dgKeyPart);
                                }
                                tempTablePartDim = tempTable;
                            }
                            else
                            {
                                dgKeyPart++;
                                tempTablePartDim.Rows.Add(pBrand, dgKeyPart);
                                kvPartDim.Add(partIndex + 1, dgKeyPart);
                            }
                        }
                        else
                        {
                            kvPartDim.Add(partIndex + 1, 0);
                        }

                        partIndex++;
                    }
                },
                () =>
                {
                    Int64 supplierIndex = 0;
                    Int64 dgKeySupplier = 0;
                    foreach (var supplier in supplierDimension)
                    {
                        if (supplier.sRegion.Equals("AMERICA"))
                        {
                            string sNation = supplier.sNation;
                            if (tempTableSupplierDim.Rows.Count > 0)
                            {
                                var tempTable = tempTableSupplierDim.Copy();
                                var found = false;
                                foreach (DataRow row in tempTableSupplierDim.Rows)
                                {
                                    var supplierNation = row.Field<string>("supplierNation");
                                    if (supplierNation.Equals(sNation))
                                    {
                                        Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                        kvSupplierDim.Add(supplierIndex + 1, dgKey);
                                        found = true;
                                        break;
                                    }
                                }
                                if (!found)
                                {
                                    dgKeySupplier++;
                                    tempTable.Rows.Add(sNation, dgKeySupplier);
                                    kvSupplierDim.Add(supplierIndex + 1, dgKeySupplier);
                                }
                                tempTableSupplierDim = tempTable;
                            }
                            else
                            {
                                dgKeySupplier++;
                                tempTableSupplierDim.Rows.Add(sNation, dgKeySupplier);
                                kvSupplierDim.Add(supplierIndex + 1, dgKeySupplier);
                            }
                        }
                        else
                        {
                            kvSupplierDim.Add(supplierIndex + 1, 0);
                        }
                        supplierIndex++;
                    }
                },
                () =>
                {
                    Int64 dgKeyDate = 0;
                    foreach (var date in dateDimension)
                    {

                        string dYear = date.dYear;
                        if (tempTableDateDim.Rows.Count > 0)
                        {
                            var tempTable = tempTableDateDim.Copy();
                            var found = false;
                            foreach (DataRow row in tempTableDateDim.Rows)
                            {
                                var year = row.Field<string>("year");
                                if (year.Equals(dYear))
                                {
                                    Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                    kvDateDim.Add(date.dDateKey, dgKey);
                                    found = true;
                                    break;
                                }
                            }
                            if (!found)
                            {
                                dgKeyDate++;
                                tempTable.Rows.Add(dYear, dgKeyDate);
                                kvDateDim.Add(date.dDateKey, dgKeyDate);
                            }
                            tempTableDateDim = tempTable;
                        }
                        else
                        {
                            dgKeyDate++;
                            tempTableDateDim.Rows.Add(dYear, dgKeyDate);
                            kvDateDim.Add(date.dDateKey, dgKeyDate);
                        }
                    }
                });

            sw.Stop();
            long t1 = sw.ElapsedMilliseconds;
            Console.WriteLine("[PIMA] Phase1 Time: " + t1);
            #endregion Step 1 & 2

            List<Int64> loPartKey = Utils.ReadFromBinaryFiles<Int64>(loPartKeyFile.Replace("BF", "BF" + scaleFactor));
            List<Int64> loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
            List<Int64> loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
            List<Int64> loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor));

            #region Step 3, 4 & 5

            sw.Reset();
            sw.Start();
            Int64 dgkLengthPart = tempTablePartDim.Rows.Count + 1;
            Int64 dgkLengthDate = tempTableDateDim.Rows.Count + 1;

            Int64[,] inMemoryAccumulator = new Int64[dgkLengthDate, dgkLengthPart];

            var partitionIndexes = Utils.getPartitionIndexes(loPartKey.Count(), parallelOptions.MaxDegreeOfParallelism);

            var tasks = new List<Task>();
            foreach (var indexes in partitionIndexes)
            {
                var t = Task.Factory.StartNew(() =>
                {
                    for (Int32 i = indexes.Item1; i <= indexes.Item2; i++)
                    {
                        Int64 partKey = loPartKey[i];
                        Int64 suppKey = loSupplierKey[i];
                        Int64 dateKey = loOrderDate[i];
                        Int64 dgkPartDim = 0;
                        Int64 dgkSupplierDim = 0;
                        Int64 dgkDateDim = 0;
                        if (kvPartDim.TryGetValue(partKey, out dgkPartDim)
                            && kvSupplierDim.TryGetValue(suppKey, out dgkSupplierDim)
                            && kvDateDim.TryGetValue(dateKey, out dgkDateDim))
                        {
                            if (dgkPartDim == 0 || dgkSupplierDim == 0 || dgkDateDim == 0)
                            {
                                // skip
                            }
                            else
                            {
                                inMemoryAccumulator[dgkDateDim, dgkPartDim] += loRevenue[i];
                            }
                        }
                    }
                });
                tasks.Add(t);
            }

            Task.WaitAll(tasks.ToArray());

            List<string> finalTable = new List<string>();
            foreach (DataRow pdRow in tempTablePartDim.Rows)
            {
                foreach (DataRow ddRow in tempTableDateDim.Rows)
                {
                    Int64 sumRevenue = inMemoryAccumulator[ddRow.Field<Int64>("denseGroupingKey"), pdRow.Field<Int64>("denseGroupingKey")];
                    if (sumRevenue != 0)
                    {
                        finalTable.Add(ddRow.Field<string>("year") + ", " + pdRow.Field<string>("partBrand") + ", " + sumRevenue);
                    }
                }

            }

            sw.Stop();
            long t2 = sw.ElapsedMilliseconds;
            Console.WriteLine("[PIMA] Phase2 Time: " + t2);
            Console.WriteLine(String.Format("[PIMA] Total Time: {0}", t1 + t2));
            Console.WriteLine(String.Format("[PIMA] Total Items: {0}", finalTable.Count));
            Console.WriteLine();
            #endregion Step 3, 4 & 5

            //Console.WriteLine("==============================================");
            //Console.WriteLine("[IMA_V3] Memory Used: " + memoryUsed + ", Total:" + finalTable.Count);
            //Console.WriteLine("[IMA_V3] Time Elaspsed: " + sw.ElapsedMilliseconds + " ms");
            // Console.WriteLine("[IMA] Total: " + finalTable.Count);

            //foreach (var item in finalTable)
            //{
            //    Console.WriteLine(item);
            //}
            //System.IO.File.WriteAllLines(@"C:\Results\PIMA.txt", finalTable);
        }

        public void Query_2_2_IM()
        {

            Stopwatch sw = new Stopwatch();

            List<Part> partDimension = Utils.ReadFromBinaryFiles<Part>(partFile.Replace("BF", "BF" + scaleFactor));
            List<Supplier> supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
            List<Date> dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));


            #region Step 1 & 2
            sw.Start();
            Dictionary<Int64, Int64> kvPartDim = new Dictionary<Int64, Int64>();
            DataTable tempTablePartDim = new DataTable();
            tempTablePartDim.Columns.Add("partBrand", typeof(string));
            tempTablePartDim.Columns.Add("denseGroupingKey", typeof(Int64));

            Dictionary<Int64, Int64> kvSupplierDim = new Dictionary<Int64, Int64>();
            DataTable tempTableSupplierDim = new DataTable();
            tempTableSupplierDim.Columns.Add("supplierNation", typeof(string));
            tempTableSupplierDim.Columns.Add("denseGroupingKey", typeof(Int64));

            Dictionary<Int64, Int64> kvDateDim = new Dictionary<Int64, Int64>();
            DataTable tempTableDateDim = new DataTable();
            tempTableDateDim.Columns.Add("year", typeof(string));
            tempTableDateDim.Columns.Add("denseGroupingKey", typeof(Int64));

            Parallel.Invoke(parallelOptions,
                () =>
                {
                    Int64 partIndex = 0;
                    Int64 dgKeyPart = 0;
                    foreach (var part in partDimension)
                    {
                        if (String.CompareOrdinal(part.pBrand, "MFGR#2221") >= 0 && String.CompareOrdinal(part.pBrand, "MFGR#2228") <= 0)
                        {
                            string pBrand = part.pBrand;
                            if (tempTablePartDim.Rows.Count > 0)
                            {
                                var tempTable = tempTablePartDim.Copy();
                                var found = false;
                                foreach (DataRow row in tempTablePartDim.Rows)
                                {
                                    var partBrand = row.Field<string>("partBrand");
                                    if (partBrand.Equals(pBrand))
                                    {
                                        Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                        kvPartDim.Add(partIndex + 1, dgKey);
                                        found = true;
                                        break;
                                    }
                                }
                                if (!found)
                                {
                                    dgKeyPart++;
                                    tempTable.Rows.Add(pBrand, dgKeyPart);
                                    kvPartDim.Add(partIndex + 1, dgKeyPart);
                                }
                                tempTablePartDim = tempTable;
                            }
                            else
                            {
                                dgKeyPart++;
                                tempTablePartDim.Rows.Add(pBrand, dgKeyPart);
                                kvPartDim.Add(partIndex + 1, dgKeyPart);
                            }
                        }
                        else
                        {
                            kvPartDim.Add(partIndex + 1, 0);
                        }

                        partIndex++;
                    }
                },
                () =>
                {
                    Int64 supplierIndex = 0;
                    Int64 dgKeySupplier = 0;
                    foreach (var supplier in supplierDimension)
                    {
                        if (supplier.sRegion.Equals("ASIA"))
                        {
                            string sNation = supplier.sNation;
                            if (tempTableSupplierDim.Rows.Count > 0)
                            {
                                var tempTable = tempTableSupplierDim.Copy();
                                var found = false;
                                foreach (DataRow row in tempTableSupplierDim.Rows)
                                {
                                    var supplierNation = row.Field<string>("supplierNation");
                                    if (supplierNation.Equals(sNation))
                                    {
                                        Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                        kvSupplierDim.Add(supplierIndex + 1, dgKey);
                                        found = true;
                                        break;
                                    }
                                }
                                if (!found)
                                {
                                    dgKeySupplier++;
                                    tempTable.Rows.Add(sNation, dgKeySupplier);
                                    kvSupplierDim.Add(supplierIndex + 1, dgKeySupplier);
                                }
                                tempTableSupplierDim = tempTable;
                            }
                            else
                            {
                                dgKeySupplier++;
                                tempTableSupplierDim.Rows.Add(sNation, dgKeySupplier);
                                kvSupplierDim.Add(supplierIndex + 1, dgKeySupplier);
                            }
                        }
                        else
                        {
                            kvSupplierDim.Add(supplierIndex + 1, 0);
                        }
                        supplierIndex++;
                    }
                },
                () =>
                {
                    Int64 dgKeyDate = 0;
                    foreach (var date in dateDimension)
                    {
                        string dYear = date.dYear;
                        if (tempTableDateDim.Rows.Count > 0)
                        {
                            var tempTable = tempTableDateDim.Copy();
                            var found = false;
                            foreach (DataRow row in tempTableDateDim.Rows)
                            {
                                var year = row.Field<string>("year");
                                if (year.Equals(dYear))
                                {
                                    Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                    kvDateDim.Add(date.dDateKey, dgKey);
                                    found = true;
                                    break;
                                }
                            }
                            if (!found)
                            {
                                dgKeyDate++;
                                tempTable.Rows.Add(dYear, dgKeyDate);
                                kvDateDim.Add(date.dDateKey, dgKeyDate);
                            }
                            tempTableDateDim = tempTable;
                        }
                        else
                        {
                            dgKeyDate++;
                            tempTableDateDim.Rows.Add(dYear, dgKeyDate);
                            kvDateDim.Add(date.dDateKey, dgKeyDate);
                        }
                    }
                });

            sw.Stop();
            long t1 = sw.ElapsedMilliseconds;
            Console.WriteLine("[PIMA] Phase1 Time: " + t1);
            #endregion Step 1 & 2

            List<Int64> loPartKey = Utils.ReadFromBinaryFiles<Int64>(loPartKeyFile.Replace("BF", "BF" + scaleFactor));
            List<Int64> loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
            List<Int64> loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
            List<Int64> loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor));

            #region Step 3, 4 & 5

            sw.Reset();
            sw.Start();
            Int64 dgkLengthPart = tempTablePartDim.Rows.Count + 1;
            Int64 dgkLengthDate = tempTableDateDim.Rows.Count + 1;

            Int64[,] inMemoryAccumulator = new Int64[dgkLengthDate, dgkLengthPart];

            var partitionIndexes = Utils.getPartitionIndexes(loPartKey.Count(), parallelOptions.MaxDegreeOfParallelism);

            var tasks = new List<Task>();
            foreach (var indexes in partitionIndexes)
            {
                var t = Task.Factory.StartNew(() =>
                {
                    for (Int32 i = indexes.Item1; i <= indexes.Item2; i++)
                    {
                        Int64 partKey = loPartKey[i];
                        Int64 dateKey = loOrderDate[i];
                        Int64 suppKey = loSupplierKey[i];
                        Int64 dgkPartDim = 0;
                        Int64 dgkDateDim = 0;
                        Int64 dgkSuppDim = 0;
                        if (kvPartDim.TryGetValue(partKey, out dgkPartDim)
                            && kvDateDim.TryGetValue(dateKey, out dgkDateDim)
                            && kvSupplierDim.TryGetValue(suppKey, out dgkSuppDim))
                        {
                            if (dgkPartDim == 0 || dgkDateDim == 0)
                            {
                                // skip
                            }
                            else
                            {
                                inMemoryAccumulator[dgkDateDim, dgkPartDim] += loRevenue[i];
                            }
                        }
                    }
                });
                tasks.Add(t);
            }

            Task.WaitAll(tasks.ToArray());

            List<string> finalTable = new List<string>();
            foreach (DataRow pdRow in tempTablePartDim.Rows)
            {
                foreach (DataRow ddRow in tempTableDateDim.Rows)
                {
                    Int64 sumRevenue = inMemoryAccumulator[ddRow.Field<Int64>("denseGroupingKey"), pdRow.Field<Int64>("denseGroupingKey")];
                    if (sumRevenue != 0)
                    {
                        finalTable.Add(ddRow.Field<string>("year") + ", " + pdRow.Field<string>("partBrand") + ", " + sumRevenue);
                    }
                }
            }

            sw.Stop();
            long t2 = sw.ElapsedMilliseconds;
            Console.WriteLine("[PIMA] Phase2 Time: " + t2);
            Console.WriteLine(String.Format("[PIMA] Total Time: {0}", t1 + t2));
            Console.WriteLine(String.Format("[PIMA] Total Items: {0}", finalTable.Count));
            Console.WriteLine();
            #endregion Step 3, 4 & 5

            //Console.WriteLine("==============================================");
            //Console.WriteLine("[IMA_V3] Memory Used: " + memoryUsed + ", Total:" + finalTable.Count);
            //Console.WriteLine("[IMA_V3] Time Elaspsed: " + sw.ElapsedMilliseconds + " ms");
            // Console.WriteLine("[IMA] Total: " + finalTable.Count);

            //foreach (var item in finalTable)
            //{
            //    Console.WriteLine(item);
            //}
            //System.IO.File.WriteAllLines(@"C:\Results\PIMA.txt", finalTable);
        }

        public void Query_2_3_IM()
        {

            Stopwatch sw = new Stopwatch();

            List<Part> partDimension = Utils.ReadFromBinaryFiles<Part>(partFile.Replace("BF", "BF" + scaleFactor));
            List<Supplier> supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
            List<Date> dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));


            #region Step 1 & 2
            sw.Start();
            Dictionary<Int64, Int64> kvPartDim = new Dictionary<Int64, Int64>();
            DataTable tempTablePartDim = new DataTable();
            tempTablePartDim.Columns.Add("partBrand", typeof(string));
            tempTablePartDim.Columns.Add("denseGroupingKey", typeof(Int64));

            Dictionary<Int64, Int64> kvSupplierDim = new Dictionary<Int64, Int64>();
            DataTable tempTableSupplierDim = new DataTable();
            tempTableSupplierDim.Columns.Add("supplierNation", typeof(string));
            tempTableSupplierDim.Columns.Add("denseGroupingKey", typeof(Int64));

            Dictionary<Int64, Int64> kvDateDim = new Dictionary<Int64, Int64>();
            DataTable tempTableDateDim = new DataTable();
            tempTableDateDim.Columns.Add("year", typeof(string));
            tempTableDateDim.Columns.Add("denseGroupingKey", typeof(Int64));

            Parallel.Invoke(parallelOptions,
                () =>
                {
                    Int64 partIndex = 0;
                    Int64 dgKeyPart = 0;
                    foreach (var part in partDimension)
                    {
                        if (part.pBrand.Equals("MFGR#2221"))
                        {
                            string pBrand = part.pBrand;
                            if (tempTablePartDim.Rows.Count > 0)
                            {
                                var tempTable = tempTablePartDim.Copy();
                                var found = false;
                                foreach (DataRow row in tempTablePartDim.Rows)
                                {
                                    var partBrand = row.Field<string>("partBrand");
                                    if (partBrand.Equals(pBrand))
                                    {
                                        Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                        kvPartDim.Add(partIndex + 1, dgKey);
                                        found = true;
                                        break;
                                    }
                                }
                                if (!found)
                                {
                                    dgKeyPart++;
                                    tempTable.Rows.Add(pBrand, dgKeyPart);
                                    kvPartDim.Add(partIndex + 1, dgKeyPart);
                                }
                                tempTablePartDim = tempTable;
                            }
                            else
                            {
                                dgKeyPart++;
                                tempTablePartDim.Rows.Add(pBrand, dgKeyPart);
                                kvPartDim.Add(partIndex + 1, dgKeyPart);
                            }
                        }
                        else
                        {
                            kvPartDim.Add(partIndex + 1, 0);
                        }

                        partIndex++;
                    }
                },
                () =>
                {
                    Int64 supplierIndex = 0;
                    Int64 dgKeySupplier = 0;
                    foreach (var supplier in supplierDimension)
                    {
                        if (supplier.sRegion.Equals("EUROPE"))
                        {
                            string sNation = supplier.sNation;
                            if (tempTableSupplierDim.Rows.Count > 0)
                            {
                                var tempTable = tempTableSupplierDim.Copy();
                                var found = false;
                                foreach (DataRow row in tempTableSupplierDim.Rows)
                                {
                                    var supplierNation = row.Field<string>("supplierNation");
                                    if (supplierNation.Equals(sNation))
                                    {
                                        Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                        kvSupplierDim.Add(supplierIndex + 1, dgKey);
                                        found = true;
                                        break;
                                    }
                                }
                                if (!found)
                                {
                                    dgKeySupplier++;
                                    tempTable.Rows.Add(sNation, dgKeySupplier);
                                    kvSupplierDim.Add(supplierIndex + 1, dgKeySupplier);
                                }
                                tempTableSupplierDim = tempTable;
                            }
                            else
                            {
                                dgKeySupplier++;
                                tempTableSupplierDim.Rows.Add(sNation, dgKeySupplier);
                                kvSupplierDim.Add(supplierIndex + 1, dgKeySupplier);
                            }
                        }
                        else
                        {
                            kvSupplierDim.Add(supplierIndex + 1, 0);
                        }
                        supplierIndex++;
                    }
                },
                () =>
                {
                    Int64 dgKeyDate = 0;
                    foreach (var date in dateDimension)
                    {
                        string dYear = date.dYear;
                        if (tempTableDateDim.Rows.Count > 0)
                        {
                            var tempTable = tempTableDateDim.Copy();
                            var found = false;
                            foreach (DataRow row in tempTableDateDim.Rows)
                            {
                                var year = row.Field<string>("year");
                                if (year.Equals(dYear))
                                {
                                    Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                    kvDateDim.Add(date.dDateKey, dgKey);
                                    found = true;
                                    break;
                                }
                            }
                            if (!found)
                            {
                                dgKeyDate++;
                                tempTable.Rows.Add(dYear, dgKeyDate);
                                kvDateDim.Add(date.dDateKey, dgKeyDate);
                            }
                            tempTableDateDim = tempTable;
                        }
                        else
                        {
                            dgKeyDate++;
                            tempTableDateDim.Rows.Add(dYear, dgKeyDate);
                            kvDateDim.Add(date.dDateKey, dgKeyDate);
                        }
                    }
                });

            sw.Stop();
            long t1 = sw.ElapsedMilliseconds;
            Console.WriteLine("[PIMA] Phase1 Time: " + t1);
            #endregion Step 1 & 2

            List<Int64> loPartKey = Utils.ReadFromBinaryFiles<Int64>(loPartKeyFile.Replace("BF", "BF" + scaleFactor));
            List<Int64> loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
            List<Int64> loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
            List<Int64> loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor));

            #region Step 3, 4 & 5

            sw.Reset();
            sw.Start();
            Int64 dgkLengthPart = tempTablePartDim.Rows.Count + 1;
            Int64 dgkLengthDate = tempTableDateDim.Rows.Count + 1;

            Int64[,] inMemoryAccumulator = new Int64[dgkLengthDate, dgkLengthPart];

            var partitionIndexes = Utils.getPartitionIndexes(loPartKey.Count(), parallelOptions.MaxDegreeOfParallelism);

            var tasks = new List<Task>();
            foreach (var indexes in partitionIndexes)
            {
                var t = Task.Factory.StartNew(() =>
                {
                    for (Int32 i = indexes.Item1; i <= indexes.Item2; i++)
                    {
                        Int64 partKey = loPartKey[i];
                        Int64 dateKey = loOrderDate[i];
                        Int64 suppKey = loSupplierKey[i];
                        Int64 dgkPartDim = 0;
                        Int64 dgkDateDim = 0;
                        Int64 dgkSupplierDim = 0;
                        if (kvPartDim.TryGetValue(partKey, out dgkPartDim)
                            && kvDateDim.TryGetValue(dateKey, out dgkDateDim)
                            && kvSupplierDim.TryGetValue(suppKey, out dgkSupplierDim))
                        {
                            if (dgkPartDim == 0 || dgkDateDim == 0)
                            {
                                // skip
                            }
                            else
                            {
                                inMemoryAccumulator[dgkDateDim, dgkPartDim] += loRevenue[i];
                            }
                        }
                    }
                });
                tasks.Add(t);
            }

            Task.WaitAll(tasks.ToArray());

            List<string> finalTable = new List<string>();
            foreach (DataRow pdRow in tempTablePartDim.Rows)
            {
                foreach (DataRow ddRow in tempTableDateDim.Rows)
                {
                    Int64 sumRevenue = inMemoryAccumulator[ddRow.Field<Int64>("denseGroupingKey"), pdRow.Field<Int64>("denseGroupingKey")];
                    if (sumRevenue != 0)
                    {
                        finalTable.Add(ddRow.Field<string>("year") + ", " + pdRow.Field<string>("partBrand") + ", " + sumRevenue);
                    }
                }
            }

            sw.Stop();
            long t2 = sw.ElapsedMilliseconds;
            Console.WriteLine("[PIMA] Phase2 Time: " + t2);
            Console.WriteLine(String.Format("[PIMA] Total Time: {0}", t1 + t2));
            Console.WriteLine(String.Format("[PIMA] Total Items: {0}", finalTable.Count));
            Console.WriteLine();
            #endregion Step 3, 4 & 5

            //Console.WriteLine("==============================================");
            //Console.WriteLine("[IMA_V3] Memory Used: " + memoryUsed + ", Total:" + finalTable.Count);
            //Console.WriteLine("[IMA_V3] Time Elaspsed: " + sw.ElapsedMilliseconds + " ms");
            // Console.WriteLine("[IMA] Total: " + finalTable.Count);

            //foreach (var item in finalTable)
            //{
            //    Console.WriteLine(item);
            //}
            //System.IO.File.WriteAllLines(@"C:\Results\PIMA.txt", finalTable);
        }

        /// <summary>
        /// Key Vector is implemented as Dictionary <Int64, Int64>
        /// InMemory Accumulator is a MultiDimensional Array
        /// Temporary Table is a Datatable
        /// </summary>
        public void Query_3_1_IM()
        {

            Stopwatch sw = new Stopwatch();

            List<Customer> customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
            List<Supplier> supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
            List<Date> dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));


            #region Step 1 & 2
            sw.Start();
            Dictionary<Int64, Int64> kvCustomerDim = new Dictionary<Int64, Int64>();
            DataTable tempTableCustomerDim = new DataTable();
            tempTableCustomerDim.Columns.Add("customerNation", typeof(string));
            tempTableCustomerDim.Columns.Add("denseGroupingKey", typeof(Int64));

            Dictionary<Int64, Int64> kvSupplierDim = new Dictionary<Int64, Int64>();
            DataTable tempTableSupplierDim = new DataTable();
            tempTableSupplierDim.Columns.Add("supplierNation", typeof(string));
            tempTableSupplierDim.Columns.Add("denseGroupingKey", typeof(Int64));

            Dictionary<Int64, Int64> kvDateDim = new Dictionary<Int64, Int64>();
            DataTable tempTableDateDim = new DataTable();
            tempTableDateDim.Columns.Add("year", typeof(string));
            tempTableDateDim.Columns.Add("denseGroupingKey", typeof(Int64));

            Parallel.Invoke(parallelOptions,
                () =>
                {
                    Int64 customerIndex = 0;
                    Int64 dgKeyCustomer = 0;
                    foreach (var customer in customerDimension)
                    {
                        if (customer.cRegion.Equals("ASIA"))
                        {
                            string cNation = customer.cNation;
                            if (tempTableCustomerDim.Rows.Count > 0)
                            {
                                var tempTable = tempTableCustomerDim.Copy();
                                var found = false;
                                foreach (DataRow row in tempTableCustomerDim.Rows)
                                {
                                    var customerNation = row.Field<string>("customerNation");
                                    if (customerNation.Equals(cNation))
                                    {
                                        Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                        kvCustomerDim.Add(customerIndex + 1, dgKey);
                                        found = true;
                                        break;
                                    }
                                }
                                if (!found)
                                {
                                    dgKeyCustomer++;
                                    tempTable.Rows.Add(cNation, dgKeyCustomer);
                                    kvCustomerDim.Add(customerIndex + 1, dgKeyCustomer);
                                }
                                tempTableCustomerDim = tempTable;
                            }
                            else
                            {
                                dgKeyCustomer++;
                                tempTableCustomerDim.Rows.Add(cNation, dgKeyCustomer);
                                kvCustomerDim.Add(customerIndex + 1, dgKeyCustomer);
                            }
                        }
                        else
                        {
                            kvCustomerDim.Add(customerIndex + 1, 0);
                        }
                        customerIndex++;
                    }
                },
                () =>
                {
                    Int64 supplierIndex = 0;
                    Int64 dgKeySupplier = 0;
                    foreach (var supplier in supplierDimension)
                    {
                        if (supplier.sRegion.Equals("ASIA"))
                        {
                            string sNation = supplier.sNation;
                            if (tempTableSupplierDim.Rows.Count > 0)
                            {
                                var tempTable = tempTableSupplierDim.Copy();
                                var found = false;
                                foreach (DataRow row in tempTableSupplierDim.Rows)
                                {
                                    var supplierNation = row.Field<string>("supplierNation");
                                    if (supplierNation.Equals(sNation))
                                    {
                                        Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                        kvSupplierDim.Add(supplierIndex + 1, dgKey);
                                        found = true;
                                        break;
                                    }
                                }
                                if (!found)
                                {
                                    dgKeySupplier++;
                                    tempTable.Rows.Add(sNation, dgKeySupplier);
                                    kvSupplierDim.Add(supplierIndex + 1, dgKeySupplier);
                                }
                                tempTableSupplierDim = tempTable;
                            }
                            else
                            {
                                dgKeySupplier++;
                                tempTableSupplierDim.Rows.Add(sNation, dgKeySupplier);
                                kvSupplierDim.Add(supplierIndex + 1, dgKeySupplier);
                            }
                        }
                        else
                        {
                            kvSupplierDim.Add(supplierIndex + 1, 0);
                        }
                        supplierIndex++;
                    }
                },
                () =>
                {
                    Int64 dgKeyDate = 0;
                    foreach (var date in dateDimension)
                    {
                        if (date.dYear.CompareTo("1992") >= 0 && date.dYear.CompareTo("1997") <= 0)
                        {
                            string dYear = date.dYear;
                            if (tempTableDateDim.Rows.Count > 0)
                            {
                                var tempTable = tempTableDateDim.Copy();
                                var found = false;
                                foreach (DataRow row in tempTableDateDim.Rows)
                                {
                                    var year = row.Field<string>("year");
                                    if (year.Equals(dYear))
                                    {
                                        Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                        kvDateDim.Add(date.dDateKey, dgKey);
                                        found = true;
                                        break;
                                    }
                                }
                                if (!found)
                                {
                                    dgKeyDate++;
                                    tempTable.Rows.Add(dYear, dgKeyDate);
                                    kvDateDim.Add(date.dDateKey, dgKeyDate);
                                }
                                tempTableDateDim = tempTable;
                            }
                            else
                            {
                                dgKeyDate++;
                                tempTableDateDim.Rows.Add(dYear, dgKeyDate);
                                kvDateDim.Add(date.dDateKey, dgKeyDate);
                            }
                        }
                        else
                        {
                            kvDateDim.Add(date.dDateKey, 0);
                        }
                    }
                });

            sw.Stop();
            long t1 = sw.ElapsedMilliseconds;
            Console.WriteLine("[PIMA] Phase1 Time: " + t1);
            #endregion Step 1 & 2

            List<Int64> loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
            List<Int64> loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
            List<Int64> loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
            List<Int64> loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor));

            #region Step 3, 4 & 5

            sw.Reset();
            sw.Start();
            Int64 dgkLengthCustomer = tempTableCustomerDim.Rows.Count + 1;
            Int64 dgkLengthSupplier = tempTableSupplierDim.Rows.Count + 1;
            Int64 dgkLengthDate = tempTableDateDim.Rows.Count + 1;

            Int64[,,] inMemoryAccumulator = new Int64[dgkLengthCustomer, dgkLengthSupplier, dgkLengthDate];

            var partitionIndexes = Utils.getPartitionIndexes(loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);

            var tasks = new List<Task>();
            foreach (var indexes in partitionIndexes)
            {
                var t = Task.Factory.StartNew(() =>
                {
                    for (Int32 i = indexes.Item1; i <= indexes.Item2; i++)
                    {
                        Int64 custKey = loCustomerKey[i];
                        Int64 suppKey = loSupplierKey[i];
                        Int64 dateKey = loOrderDate[i];
                        Int64 dgkCustomerDim = 0;
                        Int64 dgkSupplierDim = 0;
                        Int64 dgkDateDim = 0;
                        if (kvCustomerDim.TryGetValue(custKey, out dgkCustomerDim)
                            && kvSupplierDim.TryGetValue(suppKey, out dgkSupplierDim)
                            && kvDateDim.TryGetValue(dateKey, out dgkDateDim))
                        {
                            if (dgkCustomerDim == 0 || dgkSupplierDim == 0 || dgkDateDim == 0)
                            {
                                // skip
                            }
                            else
                            {
                                inMemoryAccumulator[dgkCustomerDim, dgkSupplierDim, dgkDateDim] += loRevenue[i];
                            }
                        }
                    }
                });
                tasks.Add(t);
            }

            Task.WaitAll(tasks.ToArray());

            List<string> finalTable = new List<string>();
            foreach (DataRow cdRow in tempTableCustomerDim.Rows)
            {
                foreach (DataRow sdRow in tempTableSupplierDim.Rows)
                {
                    foreach (DataRow ddRow in tempTableDateDim.Rows)
                    {
                        Int64 sumRevenue = inMemoryAccumulator[cdRow.Field<Int64>("denseGroupingKey")
                            , sdRow.Field<Int64>("denseGroupingKey")
                            , ddRow.Field<Int64>("denseGroupingKey")];
                        if (sumRevenue != 0)
                        {
                            finalTable.Add(cdRow.Field<string>("customerNation") + ", " + sdRow.Field<string>("supplierNation") + ", " + ddRow.Field<string>("year") + ", " + sumRevenue);
                        }
                    }

                }
            }

            sw.Stop();
            long t2 = sw.ElapsedMilliseconds;
            Console.WriteLine("[PIMA] Phase2 Time: " + t2);
            Console.WriteLine(String.Format("[PIMA] Total Time: {0}", t1 + t2));
            Console.WriteLine(String.Format("[PIMA] Total Count: {0}", finalTable.Count));
            Console.WriteLine();
            #endregion Step 3, 4 & 5

            //Console.WriteLine("==============================================");
            //Console.WriteLine("[IMA_V3] Memory Used: " + memoryUsed + ", Total:" + finalTable.Count);
            //Console.WriteLine("[IMA_V3] Time Elaspsed: " + sw.ElapsedMilliseconds + " ms");
            // Console.WriteLine("[IMA] Total: " + finalTable.Count);

            //foreach (var item in finalTable)
            //{
            //    Console.WriteLine(item);
            //}
            //System.IO.File.WriteAllLines(@"C:\Results\PIMA.txt", finalTable);
        }

        public void Query_3_2_IM()
        {

            Stopwatch sw = new Stopwatch();

            List<Customer> customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
            List<Supplier> supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
            List<Date> dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));


            #region Step 1 & 2
            sw.Start();
            Dictionary<Int64, Int64> kvCustomerDim = new Dictionary<Int64, Int64>();
            DataTable tempTableCustomerDim = new DataTable();
            tempTableCustomerDim.Columns.Add("customerCity", typeof(string));
            tempTableCustomerDim.Columns.Add("denseGroupingKey", typeof(Int64));

            Dictionary<Int64, Int64> kvSupplierDim = new Dictionary<Int64, Int64>();
            DataTable tempTableSupplierDim = new DataTable();
            tempTableSupplierDim.Columns.Add("supplierCity", typeof(string));
            tempTableSupplierDim.Columns.Add("denseGroupingKey", typeof(Int64));

            Dictionary<Int64, Int64> kvDateDim = new Dictionary<Int64, Int64>();
            DataTable tempTableDateDim = new DataTable();
            tempTableDateDim.Columns.Add("year", typeof(string));
            tempTableDateDim.Columns.Add("denseGroupingKey", typeof(Int64));

            Parallel.Invoke(parallelOptions,
                () =>
                {
                    Int64 customerIndex = 0;
                    Int64 dgKeyCustomer = 0;
                    foreach (var customer in customerDimension)
                    {
                        if (customer.cNation.Equals("UNITED STATES"))
                        {
                            string cCity = customer.cCity;
                            if (tempTableCustomerDim.Rows.Count > 0)
                            {
                                var tempTable = tempTableCustomerDim.Copy();
                                var found = false;
                                foreach (DataRow row in tempTableCustomerDim.Rows)
                                {
                                    var customerCity = row.Field<string>("customerCity");
                                    if (customerCity.Equals(cCity))
                                    {
                                        Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                        kvCustomerDim.Add(customerIndex + 1, dgKey);
                                        found = true;
                                        break;
                                    }
                                }
                                if (!found)
                                {
                                    dgKeyCustomer++;
                                    tempTable.Rows.Add(cCity, dgKeyCustomer);
                                    kvCustomerDim.Add(customerIndex + 1, dgKeyCustomer);
                                }
                                tempTableCustomerDim = tempTable;
                            }
                            else
                            {
                                dgKeyCustomer++;
                                tempTableCustomerDim.Rows.Add(cCity, dgKeyCustomer);
                                kvCustomerDim.Add(customerIndex + 1, dgKeyCustomer);
                            }
                        }
                        else
                        {
                            kvCustomerDim.Add(customerIndex + 1, 0);
                        }
                        customerIndex++;
                    }
                },
                () =>
                {
                    Int64 supplierIndex = 0;
                    Int64 dgKeySupplier = 0;
                    foreach (var supplier in supplierDimension)
                    {
                        if (supplier.sNation.Equals("UNITED STATES"))
                        {
                            string sCity = supplier.sCity;
                            if (tempTableSupplierDim.Rows.Count > 0)
                            {
                                var tempTable = tempTableSupplierDim.Copy();
                                var found = false;
                                foreach (DataRow row in tempTableSupplierDim.Rows)
                                {
                                    var supplierCity = row.Field<string>("supplierCity");
                                    if (supplierCity.Equals(sCity))
                                    {
                                        Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                        kvSupplierDim.Add(supplierIndex + 1, dgKey);
                                        found = true;
                                        break;
                                    }
                                }
                                if (!found)
                                {
                                    dgKeySupplier++;
                                    tempTable.Rows.Add(sCity, dgKeySupplier);
                                    kvSupplierDim.Add(supplierIndex + 1, dgKeySupplier);
                                }
                                tempTableSupplierDim = tempTable;
                            }
                            else
                            {
                                dgKeySupplier++;
                                tempTableSupplierDim.Rows.Add(sCity, dgKeySupplier);
                                kvSupplierDim.Add(supplierIndex + 1, dgKeySupplier);
                            }
                        }
                        else
                        {
                            kvSupplierDim.Add(supplierIndex + 1, 0);
                        }
                        supplierIndex++;
                    }
                },
                () =>
                {
                    Int64 dgKeyDate = 0;
                    foreach (var date in dateDimension)
                    {
                        if (date.dYear.CompareTo("1992") >= 0 && date.dYear.CompareTo("1997") <= 0)
                        {
                            string dYear = date.dYear;
                            if (tempTableDateDim.Rows.Count > 0)
                            {
                                var tempTable = tempTableDateDim.Copy();
                                var found = false;
                                foreach (DataRow row in tempTableDateDim.Rows)
                                {
                                    var year = row.Field<string>("year");
                                    if (year.Equals(dYear))
                                    {
                                        Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                        kvDateDim.Add(date.dDateKey, dgKey);
                                        found = true;
                                        break;
                                    }
                                }
                                if (!found)
                                {
                                    dgKeyDate++;
                                    tempTable.Rows.Add(dYear, dgKeyDate);
                                    kvDateDim.Add(date.dDateKey, dgKeyDate);
                                }
                                tempTableDateDim = tempTable;
                            }
                            else
                            {
                                dgKeyDate++;
                                tempTableDateDim.Rows.Add(dYear, dgKeyDate);
                                kvDateDim.Add(date.dDateKey, dgKeyDate);
                            }
                        }
                        else
                        {
                            kvDateDim.Add(date.dDateKey, 0);
                        }
                    }
                });

            sw.Stop();
            long t1 = sw.ElapsedMilliseconds;
            Console.WriteLine("[PIMA] Phase1 Time: " + t1);
            #endregion Step 1 & 2

            List<Int64> loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
            List<Int64> loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
            List<Int64> loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
            List<Int64> loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor));

            #region Step 3, 4 & 5

            sw.Reset();
            sw.Start();
            Int64 dgkLengthCustomer = tempTableCustomerDim.Rows.Count + 1;
            Int64 dgkLengthSupplier = tempTableSupplierDim.Rows.Count + 1;
            Int64 dgkLengthDate = tempTableDateDim.Rows.Count + 1;

            Int64[,,] inMemoryAccumulator = new Int64[dgkLengthCustomer, dgkLengthSupplier, dgkLengthDate];

            var partitionIndexes = Utils.getPartitionIndexes(loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);

            var tasks = new List<Task>();
            foreach (var indexes in partitionIndexes)
            {
                var t = Task.Factory.StartNew(() =>
                {
                    for (Int32 i = indexes.Item1; i <= indexes.Item2; i++)
                    {
                        Int64 custKey = loCustomerKey[i];
                        Int64 suppKey = loSupplierKey[i];
                        Int64 dateKey = loOrderDate[i];
                        Int64 dgkCustomerDim = 0;
                        Int64 dgkSupplierDim = 0;
                        Int64 dgkDateDim = 0;
                        if (kvCustomerDim.TryGetValue(custKey, out dgkCustomerDim)
                            && kvSupplierDim.TryGetValue(suppKey, out dgkSupplierDim)
                            && kvDateDim.TryGetValue(dateKey, out dgkDateDim))
                        {
                            if (dgkCustomerDim == 0 || dgkSupplierDim == 0 || dgkDateDim == 0)
                            {
                                // skip
                            }
                            else
                            {
                                inMemoryAccumulator[dgkCustomerDim, dgkSupplierDim, dgkDateDim] += loRevenue[i];
                            }
                        }
                    }
                });
                tasks.Add(t);
            }

            Task.WaitAll(tasks.ToArray());

            List<string> finalTable = new List<string>();
            foreach (DataRow cdRow in tempTableCustomerDim.Rows)
            {
                foreach (DataRow sdRow in tempTableSupplierDim.Rows)
                {
                    foreach (DataRow ddRow in tempTableDateDim.Rows)
                    {
                        Int64 sumRevenue = inMemoryAccumulator[cdRow.Field<Int64>("denseGroupingKey")
                            , sdRow.Field<Int64>("denseGroupingKey")
                            , ddRow.Field<Int64>("denseGroupingKey")];
                        if (sumRevenue != 0)
                        {
                            finalTable.Add(cdRow.Field<string>("customerCity") + ", " + sdRow.Field<string>("supplierCity") + ", " + ddRow.Field<string>("year") + ", " + sumRevenue);
                        }
                    }

                }
            }

            sw.Stop();
            long t2 = sw.ElapsedMilliseconds;
            Console.WriteLine("[PIMA] Phase2 Time: " + t2);
            Console.WriteLine(String.Format("[PIMA] Total Time: {0}", t1 + t2));
            Console.WriteLine(String.Format("[PIMA] Total Count: {0}", finalTable.Count));
            Console.WriteLine();
            #endregion Step 3, 4 & 5

            //Console.WriteLine("==============================================");
            //Console.WriteLine("[IMA_V3] Memory Used: " + memoryUsed + ", Total:" + finalTable.Count);
            //Console.WriteLine("[IMA_V3] Time Elaspsed: " + sw.ElapsedMilliseconds + " ms");
            // Console.WriteLine("[IMA] Total: " + finalTable.Count);

            //foreach (var item in finalTable)
            //{
            //    Console.WriteLine(item);
            //}
            //System.IO.File.WriteAllLines(@"C:\Results\PIMA.txt", finalTable);
        }

        public void Query_3_3_IM()
        {

            Stopwatch sw = new Stopwatch();

            List<Customer> customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
            List<Supplier> supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
            List<Date> dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));


            #region Step 1 & 2
            sw.Start();
            Dictionary<Int64, Int64> kvCustomerDim = new Dictionary<Int64, Int64>();
            DataTable tempTableCustomerDim = new DataTable();
            tempTableCustomerDim.Columns.Add("customerCity", typeof(string));
            tempTableCustomerDim.Columns.Add("denseGroupingKey", typeof(Int64));

            Dictionary<Int64, Int64> kvSupplierDim = new Dictionary<Int64, Int64>();
            DataTable tempTableSupplierDim = new DataTable();
            tempTableSupplierDim.Columns.Add("supplierCity", typeof(string));
            tempTableSupplierDim.Columns.Add("denseGroupingKey", typeof(Int64));

            Dictionary<Int64, Int64> kvDateDim = new Dictionary<Int64, Int64>();
            DataTable tempTableDateDim = new DataTable();
            tempTableDateDim.Columns.Add("year", typeof(string));
            tempTableDateDim.Columns.Add("denseGroupingKey", typeof(Int64));

            Parallel.Invoke(parallelOptions,
                () =>
                {
                    Int64 customerIndex = 0;
                    Int64 dgKeyCustomer = 0;
                    foreach (var customer in customerDimension)
                    {
                        if (customer.cCity.Equals("UNITED KI1") || customer.cCity.Equals("UNITED KI5"))
                        {
                            string cCity = customer.cCity;
                            if (tempTableCustomerDim.Rows.Count > 0)
                            {
                                var tempTable = tempTableCustomerDim.Copy();
                                var found = false;
                                foreach (DataRow row in tempTableCustomerDim.Rows)
                                {
                                    var customerCity = row.Field<string>("customerCity");
                                    if (customerCity.Equals(cCity))
                                    {
                                        Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                        kvCustomerDim.Add(customerIndex + 1, dgKey);
                                        found = true;
                                        break;
                                    }
                                }
                                if (!found)
                                {
                                    dgKeyCustomer++;
                                    tempTable.Rows.Add(cCity, dgKeyCustomer);
                                    kvCustomerDim.Add(customerIndex + 1, dgKeyCustomer);
                                }
                                tempTableCustomerDim = tempTable;
                            }
                            else
                            {
                                dgKeyCustomer++;
                                tempTableCustomerDim.Rows.Add(cCity, dgKeyCustomer);
                                kvCustomerDim.Add(customerIndex + 1, dgKeyCustomer);
                            }
                        }
                        else
                        {
                            kvCustomerDim.Add(customerIndex + 1, 0);
                        }
                        customerIndex++;
                    }
                },
                () =>
                {
                    Int64 supplierIndex = 0;
                    Int64 dgKeySupplier = 0;
                    foreach (var supplier in supplierDimension)
                    {
                        if (supplier.sCity.Equals("UNITED KI1") || supplier.sCity.Equals("UNITED KI5"))
                        {
                            string sCity = supplier.sCity;
                            if (tempTableSupplierDim.Rows.Count > 0)
                            {
                                var tempTable = tempTableSupplierDim.Copy();
                                var found = false;
                                foreach (DataRow row in tempTableSupplierDim.Rows)
                                {
                                    var supplierCity = row.Field<string>("supplierCity");
                                    if (supplierCity.Equals(sCity))
                                    {
                                        Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                        kvSupplierDim.Add(supplierIndex + 1, dgKey);
                                        found = true;
                                        break;
                                    }
                                }
                                if (!found)
                                {
                                    dgKeySupplier++;
                                    tempTable.Rows.Add(sCity, dgKeySupplier);
                                    kvSupplierDim.Add(supplierIndex + 1, dgKeySupplier);
                                }
                                tempTableSupplierDim = tempTable;
                            }
                            else
                            {
                                dgKeySupplier++;
                                tempTableSupplierDim.Rows.Add(sCity, dgKeySupplier);
                                kvSupplierDim.Add(supplierIndex + 1, dgKeySupplier);
                            }
                        }
                        else
                        {
                            kvSupplierDim.Add(supplierIndex + 1, 0);
                        }
                        supplierIndex++;
                    }
                },
                () =>
                {
                    Int64 dgKeyDate = 0;
                    foreach (var date in dateDimension)
                    {
                        if (date.dYear.CompareTo("1992") >= 0 && date.dYear.CompareTo("1997") <= 0)
                        {
                            string dYear = date.dYear;
                            if (tempTableDateDim.Rows.Count > 0)
                            {
                                var tempTable = tempTableDateDim.Copy();
                                var found = false;
                                foreach (DataRow row in tempTableDateDim.Rows)
                                {
                                    var year = row.Field<string>("year");
                                    if (year.Equals(dYear))
                                    {
                                        Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                        kvDateDim.Add(date.dDateKey, dgKey);
                                        found = true;
                                        break;
                                    }
                                }
                                if (!found)
                                {
                                    dgKeyDate++;
                                    tempTable.Rows.Add(dYear, dgKeyDate);
                                    kvDateDim.Add(date.dDateKey, dgKeyDate);
                                }
                                tempTableDateDim = tempTable;
                            }
                            else
                            {
                                dgKeyDate++;
                                tempTableDateDim.Rows.Add(dYear, dgKeyDate);
                                kvDateDim.Add(date.dDateKey, dgKeyDate);
                            }
                        }
                        else
                        {
                            kvDateDim.Add(date.dDateKey, 0);
                        }
                    }
                });

            sw.Stop();
            long t1 = sw.ElapsedMilliseconds;
            Console.WriteLine("[PIMA] Phase1 Time: " + t1);
            #endregion Step 1 & 2

            List<Int64> loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
            List<Int64> loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
            List<Int64> loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
            List<Int64> loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor));

            #region Step 3, 4 & 5

            sw.Reset();
            sw.Start();
            Int64 dgkLengthCustomer = tempTableCustomerDim.Rows.Count + 1;
            Int64 dgkLengthSupplier = tempTableSupplierDim.Rows.Count + 1;
            Int64 dgkLengthDate = tempTableDateDim.Rows.Count + 1;

            Int64[,,] inMemoryAccumulator = new Int64[dgkLengthCustomer, dgkLengthSupplier, dgkLengthDate];

            var partitionIndexes = Utils.getPartitionIndexes(loCustomerKey.Count, parallelOptions.MaxDegreeOfParallelism);

            var tasks = new List<Task>();
            foreach (var indexes in partitionIndexes)
            {
                var t = Task.Factory.StartNew(() =>
                {
                    for (Int32 i = indexes.Item1; i <= indexes.Item2; i++)
                    {
                        Int64 custKey = loCustomerKey[i];
                        Int64 suppKey = loSupplierKey[i];
                        Int64 dateKey = loOrderDate[i];
                        Int64 dgkCustomerDim = 0;
                        Int64 dgkSupplierDim = 0;
                        Int64 dgkDateDim = 0;
                        if (kvCustomerDim.TryGetValue(custKey, out dgkCustomerDim)
                            && kvSupplierDim.TryGetValue(suppKey, out dgkSupplierDim)
                            && kvDateDim.TryGetValue(dateKey, out dgkDateDim))
                        {
                            if (dgkCustomerDim == 0 || dgkSupplierDim == 0 || dgkDateDim == 0)
                            {
                                // skip
                            }
                            else
                            {
                                inMemoryAccumulator[dgkCustomerDim, dgkSupplierDim, dgkDateDim] += loRevenue[i];
                            }
                        }
                    }
                });
                tasks.Add(t);
            }

            Task.WaitAll(tasks.ToArray());

            List<string> finalTable = new List<string>();
            foreach (DataRow cdRow in tempTableCustomerDim.Rows)
            {
                foreach (DataRow sdRow in tempTableSupplierDim.Rows)
                {
                    foreach (DataRow ddRow in tempTableDateDim.Rows)
                    {
                        Int64 sumRevenue = inMemoryAccumulator[cdRow.Field<Int64>("denseGroupingKey")
                            , sdRow.Field<Int64>("denseGroupingKey")
                            , ddRow.Field<Int64>("denseGroupingKey")];
                        if (sumRevenue != 0)
                        {
                            finalTable.Add(cdRow.Field<string>("customerCity") + ", " + sdRow.Field<string>("supplierCity") + ", " + ddRow.Field<string>("year") + ", " + sumRevenue);
                        }
                    }

                }
            }

            sw.Stop();
            long t2 = sw.ElapsedMilliseconds;
            Console.WriteLine("[PIMA] Phase2 Time: " + t2);
            Console.WriteLine(String.Format("[PIMA] Total Time: {0}", t1 + t2));
            Console.WriteLine(String.Format("[PIMA] Total Count: {0}", finalTable.Count));
            Console.WriteLine();
            #endregion Step 3, 4 & 5

            //Console.WriteLine("==============================================");
            //Console.WriteLine("[IMA_V3] Memory Used: " + memoryUsed + ", Total:" + finalTable.Count);
            //Console.WriteLine("[IMA_V3] Time Elaspsed: " + sw.ElapsedMilliseconds + " ms");
            // Console.WriteLine("[IMA] Total: " + finalTable.Count);

            //foreach (var item in finalTable)
            //{
            //    Console.WriteLine(item);
            //}
            //System.IO.File.WriteAllLines(@"C:\Results\PIMA.txt", finalTable);
        }

        public void Query_3_4_IM()
        {

            Stopwatch sw = new Stopwatch();

            List<Customer> customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
            List<Supplier> supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
            List<Date> dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));


            #region Step 1 & 2
            sw.Start();
            Dictionary<Int64, Int64> kvCustomerDim = new Dictionary<Int64, Int64>();
            DataTable tempTableCustomerDim = new DataTable();
            tempTableCustomerDim.Columns.Add("customerCity", typeof(string));
            tempTableCustomerDim.Columns.Add("denseGroupingKey", typeof(Int64));

            Dictionary<Int64, Int64> kvSupplierDim = new Dictionary<Int64, Int64>();
            DataTable tempTableSupplierDim = new DataTable();
            tempTableSupplierDim.Columns.Add("supplierCity", typeof(string));
            tempTableSupplierDim.Columns.Add("denseGroupingKey", typeof(Int64));

            Dictionary<Int64, Int64> kvDateDim = new Dictionary<Int64, Int64>();
            DataTable tempTableDateDim = new DataTable();
            tempTableDateDim.Columns.Add("year", typeof(string));
            tempTableDateDim.Columns.Add("denseGroupingKey", typeof(Int64));

            Parallel.Invoke(parallelOptions,
                () =>
                {
                    Int64 customerIndex = 0;
                    Int64 dgKeyCustomer = 0;
                    foreach (var customer in customerDimension)
                    {
                        if (customer.cCity.Equals("UNITED KI1") || customer.cCity.Equals("UNITED KI5"))
                        {
                            string cCity = customer.cCity;
                            if (tempTableCustomerDim.Rows.Count > 0)
                            {
                                var tempTable = tempTableCustomerDim.Copy();
                                var found = false;
                                foreach (DataRow row in tempTableCustomerDim.Rows)
                                {
                                    var customerCity = row.Field<string>("customerCity");
                                    if (customerCity.Equals(cCity))
                                    {
                                        Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                        kvCustomerDim.Add(customerIndex + 1, dgKey);
                                        found = true;
                                        break;
                                    }
                                }
                                if (!found)
                                {
                                    dgKeyCustomer++;
                                    tempTable.Rows.Add(cCity, dgKeyCustomer);
                                    kvCustomerDim.Add(customerIndex + 1, dgKeyCustomer);
                                }
                                tempTableCustomerDim = tempTable;
                            }
                            else
                            {
                                dgKeyCustomer++;
                                tempTableCustomerDim.Rows.Add(cCity, dgKeyCustomer);
                                kvCustomerDim.Add(customerIndex + 1, dgKeyCustomer);
                            }
                        }
                        else
                        {
                            kvCustomerDim.Add(customerIndex + 1, 0);
                        }
                        customerIndex++;
                    }
                },
                () =>
                {
                    Int64 supplierIndex = 0;
                    Int64 dgKeySupplier = 0;
                    foreach (var supplier in supplierDimension)
                    {
                        if (supplier.sCity.Equals("UNITED KI1") || supplier.sCity.Equals("UNITED KI5"))
                        {
                            string sCity = supplier.sCity;
                            if (tempTableSupplierDim.Rows.Count > 0)
                            {
                                var tempTable = tempTableSupplierDim.Copy();
                                var found = false;
                                foreach (DataRow row in tempTableSupplierDim.Rows)
                                {
                                    var supplierCity = row.Field<string>("supplierCity");
                                    if (supplierCity.Equals(sCity))
                                    {
                                        Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                        kvSupplierDim.Add(supplierIndex + 1, dgKey);
                                        found = true;
                                        break;
                                    }
                                }
                                if (!found)
                                {
                                    dgKeySupplier++;
                                    tempTable.Rows.Add(sCity, dgKeySupplier);
                                    kvSupplierDim.Add(supplierIndex + 1, dgKeySupplier);
                                }
                                tempTableSupplierDim = tempTable;
                            }
                            else
                            {
                                dgKeySupplier++;
                                tempTableSupplierDim.Rows.Add(sCity, dgKeySupplier);
                                kvSupplierDim.Add(supplierIndex + 1, dgKeySupplier);
                            }
                        }
                        else
                        {
                            kvSupplierDim.Add(supplierIndex + 1, 0);
                        }
                        supplierIndex++;
                    }
                },
                () =>
                {
                    Int64 dgKeyDate = 0;
                    foreach (var date in dateDimension)
                    {
                        if (date.dYearMonth.Equals("Dec1997"))
                        {
                            string dYear = date.dYear;
                            if (tempTableDateDim.Rows.Count > 0)
                            {
                                var tempTable = tempTableDateDim.Copy();
                                var found = false;
                                foreach (DataRow row in tempTableDateDim.Rows)
                                {
                                    var year = row.Field<string>("year");
                                    if (year.Equals(dYear))
                                    {
                                        Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                        kvDateDim.Add(date.dDateKey, dgKey);
                                        found = true;
                                        break;
                                    }
                                }
                                if (!found)
                                {
                                    dgKeyDate++;
                                    tempTable.Rows.Add(dYear, dgKeyDate);
                                    kvDateDim.Add(date.dDateKey, dgKeyDate);
                                }
                                tempTableDateDim = tempTable;
                            }
                            else
                            {
                                dgKeyDate++;
                                tempTableDateDim.Rows.Add(dYear, dgKeyDate);
                                kvDateDim.Add(date.dDateKey, dgKeyDate);
                            }
                        }
                        else
                        {
                            kvDateDim.Add(date.dDateKey, 0);
                        }
                    }
                });

            sw.Stop();
            long t1 = sw.ElapsedMilliseconds;
            Console.WriteLine("[PIMA] Phase1 Time: " + t1);
            #endregion Step 1 & 2

            List<Int64> loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
            List<Int64> loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
            List<Int64> loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
            List<Int64> loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor));

            #region Step 3, 4 & 5

            sw.Reset();
            sw.Start();
            Int64 dgkLengthCustomer = tempTableCustomerDim.Rows.Count + 1;
            Int64 dgkLengthSupplier = tempTableSupplierDim.Rows.Count + 1;
            Int64 dgkLengthDate = tempTableDateDim.Rows.Count + 1;

            Int64[,,] inMemoryAccumulator = new Int64[dgkLengthCustomer, dgkLengthSupplier, dgkLengthDate];

            var partitionIndexes = Utils.getPartitionIndexes(loCustomerKey.Count, parallelOptions.MaxDegreeOfParallelism);

            var tasks = new List<Task>();
            foreach (var indexes in partitionIndexes)
            {
                var t = Task.Factory.StartNew(() =>
                {
                    for (Int32 i = indexes.Item1; i <= indexes.Item2; i++)
                    {
                        Int64 custKey = loCustomerKey[i];
                        Int64 suppKey = loSupplierKey[i];
                        Int64 dateKey = loOrderDate[i];
                        Int64 dgkCustomerDim = 0;
                        Int64 dgkSupplierDim = 0;
                        Int64 dgkDateDim = 0;
                        if (kvCustomerDim.TryGetValue(custKey, out dgkCustomerDim)
                            && kvSupplierDim.TryGetValue(suppKey, out dgkSupplierDim)
                            && kvDateDim.TryGetValue(dateKey, out dgkDateDim))
                        {
                            if (dgkCustomerDim == 0 || dgkSupplierDim == 0 || dgkDateDim == 0)
                            {
                                // skip
                            }
                            else
                            {
                                inMemoryAccumulator[dgkCustomerDim, dgkSupplierDim, dgkDateDim] += loRevenue[i];
                            }
                        }
                    }
                });
                tasks.Add(t);
            }

            Task.WaitAll(tasks.ToArray());

            List<string> finalTable = new List<string>();
            foreach (DataRow cdRow in tempTableCustomerDim.Rows)
            {
                foreach (DataRow sdRow in tempTableSupplierDim.Rows)
                {
                    foreach (DataRow ddRow in tempTableDateDim.Rows)
                    {
                        Int64 sumRevenue = inMemoryAccumulator[cdRow.Field<Int64>("denseGroupingKey")
                            , sdRow.Field<Int64>("denseGroupingKey")
                            , ddRow.Field<Int64>("denseGroupingKey")];
                        if (sumRevenue != 0)
                        {
                            finalTable.Add(cdRow.Field<string>("customerCity") + ", " + sdRow.Field<string>("supplierCity") + ", " + ddRow.Field<string>("year") + ", " + sumRevenue);
                        }
                    }

                }
            }

            sw.Stop();
            long t2 = sw.ElapsedMilliseconds;
            Console.WriteLine("[PIMA] Phase2 Time: " + t2);
            Console.WriteLine(String.Format("[PIMA] Total Time: {0}", t1 + t2));
            Console.WriteLine(String.Format("[PIMA] Total Count: {0}", finalTable.Count));
            Console.WriteLine();
            #endregion Step 3, 4 & 5

            //Console.WriteLine("==============================================");
            //Console.WriteLine("[IMA_V3] Memory Used: " + memoryUsed + ", Total:" + finalTable.Count);
            //Console.WriteLine("[IMA_V3] Time Elaspsed: " + sw.ElapsedMilliseconds + " ms");
            // Console.WriteLine("[IMA] Total: " + finalTable.Count);

            //foreach (var item in finalTable)
            //{
            //    Console.WriteLine(item);
            //}
            //System.IO.File.WriteAllLines(@"C:\Results\PIMA.txt", finalTable);
        }

        public void Query_4_1_IM()
        {

            Stopwatch sw = new Stopwatch();

            List<Customer> customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
            List<Supplier> supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
            List<Date> dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
            List<Part> partDimension = Utils.ReadFromBinaryFiles<Part>(partFile.Replace("BF", "BF" + scaleFactor));


            #region Step 1 & 2
            sw.Start();
            Dictionary<Int64, Int64> kvCustomerDim = new Dictionary<Int64, Int64>();
            DataTable tempTableCustomerDim = new DataTable();
            tempTableCustomerDim.Columns.Add("customerNation", typeof(string));
            tempTableCustomerDim.Columns.Add("denseGroupingKey", typeof(Int64));

            Dictionary<Int64, Int64> kvSupplierDim = new Dictionary<Int64, Int64>();
            DataTable tempTableSupplierDim = new DataTable();
            tempTableSupplierDim.Columns.Add("supplierNation", typeof(string));
            tempTableSupplierDim.Columns.Add("denseGroupingKey", typeof(Int64));

            Dictionary<Int64, Int64> kvDateDim = new Dictionary<Int64, Int64>();
            DataTable tempTableDateDim = new DataTable();
            tempTableDateDim.Columns.Add("year", typeof(string));
            tempTableDateDim.Columns.Add("denseGroupingKey", typeof(Int64));

            Dictionary<Int64, Int64> kvPartDim = new Dictionary<Int64, Int64>();
            DataTable tempTablePartDim = new DataTable();
            tempTablePartDim.Columns.Add("partMFGR", typeof(string));
            tempTablePartDim.Columns.Add("denseGroupingKey", typeof(Int64));

            Parallel.Invoke(parallelOptions,
                () =>
                {
                    Int64 customerIndex = 0;
                    Int64 dgKeyCustomer = 0;
                    foreach (var customer in customerDimension)
                    {
                        if (customer.cRegion.Equals("AMERICA"))
                        {
                            string cNation = customer.cNation;
                            if (tempTableCustomerDim.Rows.Count > 0)
                            {
                                var tempTable = tempTableCustomerDim.Copy();
                                var found = false;
                                foreach (DataRow row in tempTableCustomerDim.Rows)
                                {
                                    var customerNation = row.Field<string>("customerNation");
                                    if (customerNation.Equals(cNation))
                                    {
                                        Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                        kvCustomerDim.Add(customerIndex + 1, dgKey);
                                        found = true;
                                        break;
                                    }
                                }
                                if (!found)
                                {
                                    dgKeyCustomer++;
                                    tempTable.Rows.Add(cNation, dgKeyCustomer);
                                    kvCustomerDim.Add(customerIndex + 1, dgKeyCustomer);
                                }
                                tempTableCustomerDim = tempTable;
                            }
                            else
                            {
                                dgKeyCustomer++;
                                tempTableCustomerDim.Rows.Add(cNation, dgKeyCustomer);
                                kvCustomerDim.Add(customerIndex + 1, dgKeyCustomer);
                            }
                        }
                        else
                        {
                            kvCustomerDim.Add(customerIndex + 1, 0);
                        }
                        customerIndex++;
                    }
                },
                () =>
                {
                    Int64 supplierIndex = 0;
                    Int64 dgKeySupplier = 0;
                    foreach (var supplier in supplierDimension)
                    {
                        if (supplier.sRegion.Equals("AMERICA"))
                        {
                            string sNation = supplier.sNation;
                            if (tempTableSupplierDim.Rows.Count > 0)
                            {
                                var tempTable = tempTableSupplierDim.Copy();
                                var found = false;
                                foreach (DataRow row in tempTableSupplierDim.Rows)
                                {
                                    var supplierNation = row.Field<string>("supplierNation");
                                    if (supplierNation.Equals(sNation))
                                    {
                                        Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                        kvSupplierDim.Add(supplierIndex + 1, dgKey);
                                        found = true;
                                        break;
                                    }
                                }
                                if (!found)
                                {
                                    dgKeySupplier++;
                                    tempTable.Rows.Add(sNation, dgKeySupplier);
                                    kvSupplierDim.Add(supplierIndex + 1, dgKeySupplier);
                                }
                                tempTableSupplierDim = tempTable;
                            }
                            else
                            {
                                dgKeySupplier++;
                                tempTableSupplierDim.Rows.Add(sNation, dgKeySupplier);
                                kvSupplierDim.Add(supplierIndex + 1, dgKeySupplier);
                            }
                        }
                        else
                        {
                            kvSupplierDim.Add(supplierIndex + 1, 0);
                        }
                        supplierIndex++;
                    }
                },
                () =>
                {
                    Int64 dgKeyDate = 0;
                    foreach (var date in dateDimension)
                    {
                        string dYear = date.dYear;
                        if (tempTableDateDim.Rows.Count > 0)
                        {
                            var tempTable = tempTableDateDim.Copy();
                            var found = false;
                            foreach (DataRow row in tempTableDateDim.Rows)
                            {
                                var year = row.Field<string>("year");
                                if (year.Equals(dYear))
                                {
                                    Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                    kvDateDim.Add(date.dDateKey, dgKey);
                                    found = true;
                                    break;
                                }
                            }
                            if (!found)
                            {
                                dgKeyDate++;
                                tempTable.Rows.Add(dYear, dgKeyDate);
                                kvDateDim.Add(date.dDateKey, dgKeyDate);
                            }
                            tempTableDateDim = tempTable;
                        }
                        else
                        {
                            dgKeyDate++;
                            tempTableDateDim.Rows.Add(dYear, dgKeyDate);
                            kvDateDim.Add(date.dDateKey, dgKeyDate);
                        }
                    }
                },
                () =>
                {
                    Int64 partIndex = 0;
                    Int64 dgKeyPart = 0;
                    foreach (var part in partDimension)
                    {
                        if (part.pMFGR.Equals("MFGR#1") || part.pMFGR.Equals("MFGR#2"))
                        {
                            string pMFGR = part.pMFGR;
                            if (tempTablePartDim.Rows.Count > 0)
                            {
                                var tempTable = tempTablePartDim.Copy();
                                var found = false;
                                foreach (DataRow row in tempTablePartDim.Rows)
                                {
                                    var partMFGR = row.Field<string>("partMFGR");
                                    if (partMFGR.Equals(pMFGR))
                                    {
                                        Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                        kvPartDim.Add(partIndex + 1, dgKey);
                                        found = true;
                                        break;
                                    }
                                }
                                if (!found)
                                {
                                    dgKeyPart++;
                                    tempTable.Rows.Add(pMFGR, dgKeyPart);
                                    kvPartDim.Add(partIndex + 1, dgKeyPart);
                                }
                                tempTablePartDim = tempTable;
                            }
                            else
                            {
                                dgKeyPart++;
                                tempTablePartDim.Rows.Add(pMFGR, dgKeyPart);
                                kvPartDim.Add(partIndex + 1, dgKeyPart);
                            }

                        }
                        else
                        {
                            kvPartDim.Add(partIndex + 1, 0);
                        }
                        partIndex++;
                    }
                });

            sw.Stop();
            long t1 = sw.ElapsedMilliseconds;
            Console.WriteLine("[PIMA] Phase1 Time: " + t1);
            #endregion Step 1 & 2

            List<Int64> loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
            List<Int64> loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
            List<Int64> loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
            List<Int64> loPartKey = Utils.ReadFromBinaryFiles<Int64>(loPartKeyFile.Replace("BF", "BF" + scaleFactor));

            List<Int64> loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
            List<Int64> loSupplyCost = Utils.ReadFromBinaryFiles<Int64>(loSupplyCostFile.Replace("BF", "BF" + scaleFactor));

            #region Step 3, 4 & 5

            sw.Reset();
            sw.Start();
            Int64 dgkLengthCustomer = tempTableCustomerDim.Rows.Count + 1;
            Int64 dgkLengthSupplier = tempTableSupplierDim.Rows.Count + 1;
            Int64 dgkLengthDate = tempTableDateDim.Rows.Count + 1;
            Int64 dgkLengthPart = tempTableDateDim.Rows.Count + 1;

            Int64[,] inMemoryAccumulator = new Int64[dgkLengthCustomer, dgkLengthDate];

            var partitionIndexes = Utils.getPartitionIndexes(loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);

            var tasks = new List<Task>();
            foreach (var indexes in partitionIndexes)
            {
                var t = Task.Factory.StartNew(() =>
                {
                    for (Int32 i = indexes.Item1; i <= indexes.Item2; i++)
                    {
                        Int64 custKey = loCustomerKey[i];
                        Int64 dateKey = loOrderDate[i];
                        Int64 suppKey = loSupplierKey[i];
                        Int64 partKey = loPartKey[i];
                        Int64 dgkCustomerDim = 0;
                        Int64 dgkDateDim = 0;
                        if (kvCustomerDim.TryGetValue(custKey, out dgkCustomerDim)
                            && kvDateDim.TryGetValue(dateKey, out dgkDateDim)
                            && kvSupplierDim.ContainsKey(suppKey)
                            && kvPartDim.ContainsKey(partKey))
                        {
                            if (dgkCustomerDim == 0 || dgkDateDim == 0)
                            {
                                // skip
                            }
                            else
                            {
                                inMemoryAccumulator[dgkCustomerDim, dgkDateDim] += (loRevenue[i] - loSupplyCost[i]);
                            }
                        }
                    }
                });
                tasks.Add(t);
            }

            Task.WaitAll(tasks.ToArray());

            List<string> finalTable = new List<string>();
            foreach (DataRow cdRow in tempTableCustomerDim.Rows)
            {
                foreach (DataRow ddRow in tempTableDateDim.Rows)
                {
                    Int64 sumProfit = inMemoryAccumulator[cdRow.Field<Int64>("denseGroupingKey")
                        , ddRow.Field<Int64>("denseGroupingKey")];
                    if (sumProfit != 0)
                    {
                        finalTable.Add(ddRow.Field<string>("year") + ", " + cdRow.Field<string>("customerNation") + ", " + sumProfit);
                    }
                }

            }

            sw.Stop();
            long t2 = sw.ElapsedMilliseconds;
            Console.WriteLine("[PIMA] Phase2 Time: " + t2);
            Console.WriteLine(String.Format("[PIMA] Total Time: {0}", t1 + t2));
            Console.WriteLine(String.Format("[PIMA] Total Count: {0}", finalTable.Count));
            Console.WriteLine();
            #endregion Step 3, 4 & 5

            //Console.WriteLine("==============================================");
            //Console.WriteLine("[IMA_V3] Memory Used: " + memoryUsed + ", Total:" + finalTable.Count);
            //Console.WriteLine("[IMA_V3] Time Elaspsed: " + sw.ElapsedMilliseconds + " ms");
            // Console.WriteLine("[IMA] Total: " + finalTable.Count);

            //foreach (var item in finalTable)
            //{
            //    Console.WriteLine(item);
            //}
            //System.IO.File.WriteAllLines(@"C:\Results\PIMA.txt", finalTable);
        }

        public void Query_4_2_IM()
        {

            Stopwatch sw = new Stopwatch();

            List<Customer> customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
            List<Supplier> supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
            List<Date> dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
            List<Part> partDimension = Utils.ReadFromBinaryFiles<Part>(partFile.Replace("BF", "BF" + scaleFactor));


            #region Step 1 & 2
            sw.Start();
            Dictionary<Int64, Int64> kvCustomerDim = new Dictionary<Int64, Int64>();
            DataTable tempTableCustomerDim = new DataTable();
            tempTableCustomerDim.Columns.Add("customerNation", typeof(string));
            tempTableCustomerDim.Columns.Add("denseGroupingKey", typeof(Int64));

            Dictionary<Int64, Int64> kvSupplierDim = new Dictionary<Int64, Int64>();
            DataTable tempTableSupplierDim = new DataTable();
            tempTableSupplierDim.Columns.Add("supplierNation", typeof(string));
            tempTableSupplierDim.Columns.Add("denseGroupingKey", typeof(Int64));

            Dictionary<Int64, Int64> kvDateDim = new Dictionary<Int64, Int64>();
            DataTable tempTableDateDim = new DataTable();
            tempTableDateDim.Columns.Add("year", typeof(string));
            tempTableDateDim.Columns.Add("denseGroupingKey", typeof(Int64));

            Dictionary<Int64, Int64> kvPartDim = new Dictionary<Int64, Int64>();
            DataTable tempTablePartDim = new DataTable();
            tempTablePartDim.Columns.Add("partCategory", typeof(string));
            tempTablePartDim.Columns.Add("denseGroupingKey", typeof(Int64));

            Parallel.Invoke(parallelOptions,
                () =>
                {
                    Int64 customerIndex = 0;
                    Int64 dgKeyCustomer = 0;
                    foreach (var customer in customerDimension)
                    {
                        if (customer.cRegion.Equals("AMERICA"))
                        {
                            string cNation = customer.cNation;
                            if (tempTableCustomerDim.Rows.Count > 0)
                            {
                                var tempTable = tempTableCustomerDim.Copy();
                                var found = false;
                                foreach (DataRow row in tempTableCustomerDim.Rows)
                                {
                                    var customerNation = row.Field<string>("customerNation");
                                    if (customerNation.Equals(cNation))
                                    {
                                        Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                        kvCustomerDim.Add(customerIndex + 1, dgKey);
                                        found = true;
                                        break;
                                    }
                                }
                                if (!found)
                                {
                                    dgKeyCustomer++;
                                    tempTable.Rows.Add(cNation, dgKeyCustomer);
                                    kvCustomerDim.Add(customerIndex + 1, dgKeyCustomer);
                                }
                                tempTableCustomerDim = tempTable;
                            }
                            else
                            {
                                dgKeyCustomer++;
                                tempTableCustomerDim.Rows.Add(cNation, dgKeyCustomer);
                                kvCustomerDim.Add(customerIndex + 1, dgKeyCustomer);
                            }
                        }
                        else
                        {
                            kvCustomerDim.Add(customerIndex + 1, 0);
                        }
                        customerIndex++;
                    }
                },
                () =>
                {
                    Int64 supplierIndex = 0;
                    Int64 dgKeySupplier = 0;
                    foreach (var supplier in supplierDimension)
                    {
                        if (supplier.sRegion.Equals("AMERICA"))
                        {
                            string sNation = supplier.sNation;
                            if (tempTableSupplierDim.Rows.Count > 0)
                            {
                                var tempTable = tempTableSupplierDim.Copy();
                                var found = false;
                                foreach (DataRow row in tempTableSupplierDim.Rows)
                                {
                                    var supplierNation = row.Field<string>("supplierNation");
                                    if (supplierNation.Equals(sNation))
                                    {
                                        Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                        kvSupplierDim.Add(supplierIndex + 1, dgKey);
                                        found = true;
                                        break;
                                    }
                                }
                                if (!found)
                                {
                                    dgKeySupplier++;
                                    tempTable.Rows.Add(sNation, dgKeySupplier);
                                    kvSupplierDim.Add(supplierIndex + 1, dgKeySupplier);
                                }
                                tempTableSupplierDim = tempTable;
                            }
                            else
                            {
                                dgKeySupplier++;
                                tempTableSupplierDim.Rows.Add(sNation, dgKeySupplier);
                                kvSupplierDim.Add(supplierIndex + 1, dgKeySupplier);
                            }
                        }
                        else
                        {
                            kvSupplierDim.Add(supplierIndex + 1, 0);
                        }
                        supplierIndex++;
                    }
                },
                () =>
                {
                    Int64 dgKeyDate = 0;
                    foreach (var date in dateDimension)
                    {
                        if (date.dYear.Equals("1997") || date.dYear.Equals("1998"))
                        {
                            string dYear = date.dYear;
                            if (tempTableDateDim.Rows.Count > 0)
                            {
                                var tempTable = tempTableDateDim.Copy();
                                var found = false;
                                foreach (DataRow row in tempTableDateDim.Rows)
                                {
                                    var year = row.Field<string>("year");
                                    if (year.Equals(dYear))
                                    {
                                        Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                        kvDateDim.Add(date.dDateKey, dgKey);
                                        found = true;
                                        break;
                                    }
                                }
                                if (!found)
                                {
                                    dgKeyDate++;
                                    tempTable.Rows.Add(dYear, dgKeyDate);
                                    kvDateDim.Add(date.dDateKey, dgKeyDate);
                                }
                                tempTableDateDim = tempTable;
                            }
                            else
                            {
                                dgKeyDate++;
                                tempTableDateDim.Rows.Add(dYear, dgKeyDate);
                                kvDateDim.Add(date.dDateKey, dgKeyDate);
                            }
                        }
                        else
                        {
                            kvDateDim.Add(date.dDateKey, 0);
                        }
                    }

                },
                () =>
                {
                    Int64 partIndex = 0;
                    Int64 dgKeyPart = 0;
                    foreach (var part in partDimension)
                    {
                        if (part.pMFGR.Equals("MFGR#1") || part.pMFGR.Equals("MFGR#2"))
                        {
                            string pCategory = part.pCategory;
                            if (tempTablePartDim.Rows.Count > 0)
                            {
                                var tempTable = tempTablePartDim.Copy();
                                var found = false;
                                foreach (DataRow row in tempTablePartDim.Rows)
                                {
                                    var partCategory = row.Field<string>("partCategory");
                                    if (partCategory.Equals(pCategory))
                                    {
                                        Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                        kvPartDim.Add(partIndex + 1, dgKey);
                                        found = true;
                                        break;
                                    }
                                }
                                if (!found)
                                {
                                    dgKeyPart++;
                                    tempTable.Rows.Add(pCategory, dgKeyPart);
                                    kvPartDim.Add(partIndex + 1, dgKeyPart);
                                }
                                tempTablePartDim = tempTable;
                            }
                            else
                            {
                                dgKeyPart++;
                                tempTablePartDim.Rows.Add(pCategory, dgKeyPart);
                                kvPartDim.Add(partIndex + 1, dgKeyPart);
                            }

                        }
                        else
                        {
                            kvPartDim.Add(partIndex + 1, 0);
                        }
                        partIndex++;
                    }
                });

            sw.Stop();
            long t1 = sw.ElapsedMilliseconds;
            Console.WriteLine("[PIMA] Phase1 Time: " + t1);
            #endregion Step 1 & 2

            List<Int64> loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
            List<Int64> loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
            List<Int64> loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
            List<Int64> loPartKey = Utils.ReadFromBinaryFiles<Int64>(loPartKeyFile.Replace("BF", "BF" + scaleFactor));

            List<Int64> loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
            List<Int64> loSupplyCost = Utils.ReadFromBinaryFiles<Int64>(loSupplyCostFile.Replace("BF", "BF" + scaleFactor));

            #region Step 3, 4 & 5

            sw.Reset();
            sw.Start();
            Int64 dgkLengthSupplier = tempTableSupplierDim.Rows.Count + 1;
            Int64 dgkLengthDate = tempTableDateDim.Rows.Count + 1;
            Int64 dgkLengthPart = tempTablePartDim.Rows.Count + 1;

            Int64[,,] inMemoryAccumulator = new Int64[dgkLengthDate, dgkLengthSupplier, dgkLengthPart];

            var partitionIndexes = Utils.getPartitionIndexes(loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);

            var tasks = new List<Task>();
            foreach (var indexes in partitionIndexes)
            {
                var t = Task.Factory.StartNew(() =>
                {
                    for (Int32 i = indexes.Item1; i <= indexes.Item2; i++)
                    {
                        Int64 suppKey = loSupplierKey[i];
                        Int64 dateKey = loOrderDate[i];
                        Int64 partKey = loPartKey[i];
                        Int64 custKey = loCustomerKey[i];
                        Int64 dgkSupplierDim = 0;
                        Int64 dgkDateDim = 0;
                        Int64 dgkPartDim = 0;
                        if (kvSupplierDim.TryGetValue(suppKey, out dgkSupplierDim)
                            && kvDateDim.TryGetValue(dateKey, out dgkDateDim)
                            && kvPartDim.TryGetValue(partKey, out dgkPartDim)
                            && kvCustomerDim.ContainsKey(custKey))
                        {
                            if (dgkSupplierDim == 0 || dgkDateDim == 0 || dgkPartDim == 0)
                            {
                                // skip
                            }
                            else
                            {
                                inMemoryAccumulator[dgkDateDim, dgkSupplierDim, dgkPartDim] += (loRevenue[i] - loSupplyCost[i]);
                            }
                        }
                    }
                });
                tasks.Add(t);
            }

            Task.WaitAll(tasks.ToArray());

            List<string> finalTable = new List<string>();
            foreach (DataRow sdRow in tempTableSupplierDim.Rows)
            {
                foreach (DataRow ddRow in tempTableDateDim.Rows)
                {
                    foreach (DataRow pdRow in tempTablePartDim.Rows)
                    {
                        Int64 sumProfit = inMemoryAccumulator[ddRow.Field<Int64>("denseGroupingKey"), sdRow.Field<Int64>("denseGroupingKey")
                                                , pdRow.Field<Int64>("denseGroupingKey")];
                        if (sumProfit != 0)
                        {
                            finalTable.Add(ddRow.Field<string>("year") + ", " + sdRow.Field<string>("supplierNation") + ", " + pdRow.Field<string>("partCategory") + ", " + sumProfit);
                        }
                    }

                }

            }

            sw.Stop();
            long t2 = sw.ElapsedMilliseconds;
            Console.WriteLine("[PIMA] Phase2 Time: " + t2);
            Console.WriteLine(String.Format("[PIMA] Total Time: {0}", t1 + t2));
            Console.WriteLine(String.Format("[PIMA] Total Count: {0}", finalTable.Count));
            Console.WriteLine();
            #endregion Step 3, 4 & 5

            //Console.WriteLine("==============================================");
            //Console.WriteLine("[IMA_V3] Memory Used: " + memoryUsed + ", Total:" + finalTable.Count);
            //Console.WriteLine("[IMA_V3] Time Elaspsed: " + sw.ElapsedMilliseconds + " ms");
            // Console.WriteLine("[IMA] Total: " + finalTable.Count);

            //foreach (var item in finalTable)
            //{
            //    Console.WriteLine(item);
            //}
            //System.IO.File.WriteAllLines(@"C:\Results\PIMA.txt", finalTable);
        }

        public void Query_4_3_IM()
        {

            Stopwatch sw = new Stopwatch();

            List<Customer> customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
            List<Supplier> supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
            List<Date> dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
            List<Part> partDimension = Utils.ReadFromBinaryFiles<Part>(partFile.Replace("BF", "BF" + scaleFactor));


            #region Step 1 & 2
            sw.Start();
            Dictionary<Int64, Int64> kvCustomerDim = new Dictionary<Int64, Int64>();
            DataTable tempTableCustomerDim = new DataTable();
            tempTableCustomerDim.Columns.Add("customerNation", typeof(string));
            tempTableCustomerDim.Columns.Add("denseGroupingKey", typeof(Int64));

            Dictionary<Int64, Int64> kvSupplierDim = new Dictionary<Int64, Int64>();
            DataTable tempTableSupplierDim = new DataTable();
            tempTableSupplierDim.Columns.Add("supplierCity", typeof(string));
            tempTableSupplierDim.Columns.Add("denseGroupingKey", typeof(Int64));

            Dictionary<Int64, Int64> kvDateDim = new Dictionary<Int64, Int64>();
            DataTable tempTableDateDim = new DataTable();
            tempTableDateDim.Columns.Add("year", typeof(string));
            tempTableDateDim.Columns.Add("denseGroupingKey", typeof(Int64));

            Dictionary<Int64, Int64> kvPartDim = new Dictionary<Int64, Int64>();
            DataTable tempTablePartDim = new DataTable();
            tempTablePartDim.Columns.Add("partBrand", typeof(string));
            tempTablePartDim.Columns.Add("denseGroupingKey", typeof(Int64));

            Parallel.Invoke(parallelOptions,
                () =>
                {
                    Int64 customerIndex = 0;
                    Int64 dgKeyCustomer = 0;
                    foreach (var customer in customerDimension)
                    {
                        if (customer.cRegion.Equals("AMERICA"))
                        {
                            string cNation = customer.cNation;
                            if (tempTableCustomerDim.Rows.Count > 0)
                            {
                                var tempTable = tempTableCustomerDim.Copy();
                                var found = false;
                                foreach (DataRow row in tempTableCustomerDim.Rows)
                                {
                                    var customerNation = row.Field<string>("customerNation");
                                    if (customerNation.Equals(cNation))
                                    {
                                        Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                        kvCustomerDim.Add(customerIndex + 1, dgKey);
                                        found = true;
                                        break;
                                    }
                                }
                                if (!found)
                                {
                                    dgKeyCustomer++;
                                    tempTable.Rows.Add(cNation, dgKeyCustomer);
                                    kvCustomerDim.Add(customerIndex + 1, dgKeyCustomer);
                                }
                                tempTableCustomerDim = tempTable;
                            }
                            else
                            {
                                dgKeyCustomer++;
                                tempTableCustomerDim.Rows.Add(cNation, dgKeyCustomer);
                                kvCustomerDim.Add(customerIndex + 1, dgKeyCustomer);
                            }
                        }
                        else
                        {
                            kvCustomerDim.Add(customerIndex + 1, 0);
                        }
                        customerIndex++;
                    }
                },
                () =>
                {
                    Int64 supplierIndex = 0;
                    Int64 dgKeySupplier = 0;
                    foreach (var supplier in supplierDimension)
                    {
                        if (supplier.sNation.Equals("UNITED STATES"))
                        {
                            string sCity = supplier.sCity;
                            if (tempTableSupplierDim.Rows.Count > 0)
                            {
                                var tempTable = tempTableSupplierDim.Copy();
                                var found = false;
                                foreach (DataRow row in tempTableSupplierDim.Rows)
                                {
                                    var supplierCity = row.Field<string>("supplierCity");
                                    if (supplierCity.Equals(sCity))
                                    {
                                        Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                        kvSupplierDim.Add(supplierIndex + 1, dgKey);
                                        found = true;
                                        break;
                                    }
                                }
                                if (!found)
                                {
                                    dgKeySupplier++;
                                    tempTable.Rows.Add(sCity, dgKeySupplier);
                                    kvSupplierDim.Add(supplierIndex + 1, dgKeySupplier);
                                }
                                tempTableSupplierDim = tempTable;
                            }
                            else
                            {
                                dgKeySupplier++;
                                tempTableSupplierDim.Rows.Add(sCity, dgKeySupplier);
                                kvSupplierDim.Add(supplierIndex + 1, dgKeySupplier);
                            }
                        }
                        else
                        {
                            kvSupplierDim.Add(supplierIndex + 1, 0);
                        }
                        supplierIndex++;
                    }
                },
                () =>
                {
                    Int64 dgKeyDate = 0;
                    foreach (var date in dateDimension)
                    {
                        if (date.dYear.Equals("1997") || date.dYear.Equals("1998"))
                        {
                            string dYear = date.dYear;
                            if (tempTableDateDim.Rows.Count > 0)
                            {
                                var tempTable = tempTableDateDim.Copy();
                                var found = false;
                                foreach (DataRow row in tempTableDateDim.Rows)
                                {
                                    var year = row.Field<string>("year");
                                    if (year.Equals(dYear))
                                    {
                                        Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                        kvDateDim.Add(date.dDateKey, dgKey);
                                        found = true;
                                        break;
                                    }
                                }
                                if (!found)
                                {
                                    dgKeyDate++;
                                    tempTable.Rows.Add(dYear, dgKeyDate);
                                    kvDateDim.Add(date.dDateKey, dgKeyDate);
                                }
                                tempTableDateDim = tempTable;
                            }
                            else
                            {
                                dgKeyDate++;
                                tempTableDateDim.Rows.Add(dYear, dgKeyDate);
                                kvDateDim.Add(date.dDateKey, dgKeyDate);
                            }
                        }
                        else
                        {
                            kvDateDim.Add(date.dDateKey, 0);
                        }
                    }

                },
                () =>
                {
                    Int64 partIndex = 0;
                    Int64 dgKeyPart = 0;
                    foreach (var part in partDimension)
                    {
                        if (part.pCategory.Equals("MFGR#14"))
                        {
                            string pBrand = part.pBrand;
                            if (tempTablePartDim.Rows.Count > 0)
                            {
                                var tempTable = tempTablePartDim.Copy();
                                var found = false;
                                foreach (DataRow row in tempTablePartDim.Rows)
                                {
                                    var partBrand = row.Field<string>("partBrand");
                                    if (partBrand.Equals(pBrand))
                                    {
                                        Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                        kvPartDim.Add(partIndex + 1, dgKey);
                                        found = true;
                                        break;
                                    }
                                }
                                if (!found)
                                {
                                    dgKeyPart++;
                                    tempTable.Rows.Add(pBrand, dgKeyPart);
                                    kvPartDim.Add(partIndex + 1, dgKeyPart);
                                }
                                tempTablePartDim = tempTable;
                            }
                            else
                            {
                                dgKeyPart++;
                                tempTablePartDim.Rows.Add(pBrand, dgKeyPart);
                                kvPartDim.Add(partIndex + 1, dgKeyPart);
                            }

                        }
                        else
                        {
                            kvPartDim.Add(partIndex + 1, 0);
                        }
                        partIndex++;
                    }
                });

            sw.Stop();
            long t1 = sw.ElapsedMilliseconds;
            Console.WriteLine("[PIMA] Phase1 Time: " + t1);
            #endregion Step 1 & 2

            List<Int64> loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
            List<Int64> loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
            List<Int64> loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
            List<Int64> loPartKey = Utils.ReadFromBinaryFiles<Int64>(loPartKeyFile.Replace("BF", "BF" + scaleFactor));

            List<Int64> loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
            List<Int64> loSupplyCost = Utils.ReadFromBinaryFiles<Int64>(loSupplyCostFile.Replace("BF", "BF" + scaleFactor));

            #region Step 3, 4 & 5

            sw.Reset();
            sw.Start();
            Int64 dgkLengthSupplier = tempTableSupplierDim.Rows.Count + 1;
            Int64 dgkLengthDate = tempTableDateDim.Rows.Count + 1;
            Int64 dgkLengthPart = tempTablePartDim.Rows.Count + 1;

            Int64[,,] inMemoryAccumulator = new Int64[dgkLengthDate, dgkLengthSupplier, dgkLengthPart];

            var partitionIndexes = Utils.getPartitionIndexes(loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);

            var tasks = new List<Task>();
            foreach (var indexes in partitionIndexes)
            {
                var t = Task.Factory.StartNew(() =>
                {
                    for (Int32 i = indexes.Item1; i <= indexes.Item2; i++)
                    {
                        Int64 suppKey = loSupplierKey[i];
                        Int64 dateKey = loOrderDate[i];
                        Int64 partKey = loPartKey[i];
                        Int64 custKey = loCustomerKey[i];
                        Int64 dgkSupplierDim = 0;
                        Int64 dgkDateDim = 0;
                        Int64 dgkPartDim = 0;
                        if (kvSupplierDim.TryGetValue(suppKey, out dgkSupplierDim)
                            && kvDateDim.TryGetValue(dateKey, out dgkDateDim)
                            && kvPartDim.TryGetValue(partKey, out dgkPartDim)
                            && kvCustomerDim.ContainsKey(custKey))
                        {
                            if (dgkSupplierDim == 0 || dgkDateDim == 0 || dgkPartDim == 0)
                            {
                                // skip
                            }
                            else
                            {
                                inMemoryAccumulator[dgkDateDim, dgkSupplierDim, dgkPartDim] += (loRevenue[i] - loSupplyCost[i]);
                            }
                        }
                    }
                });
                tasks.Add(t);
            }

            Task.WaitAll(tasks.ToArray());

            List<string> finalTable = new List<string>();
            foreach (DataRow sdRow in tempTableSupplierDim.Rows)
            {
                foreach (DataRow ddRow in tempTableDateDim.Rows)
                {
                    foreach (DataRow pdRow in tempTablePartDim.Rows)
                    {
                        Int64 sumProfit = inMemoryAccumulator[ddRow.Field<Int64>("denseGroupingKey"), sdRow.Field<Int64>("denseGroupingKey")
                                                , pdRow.Field<Int64>("denseGroupingKey")];
                        if (sumProfit != 0)
                        {
                            finalTable.Add(ddRow.Field<string>("year") + ", " + sdRow.Field<string>("supplierCity") + ", " + pdRow.Field<string>("partBrand") + ", " + sumProfit);
                        }
                    }

                }

            }

            sw.Stop();
            long t2 = sw.ElapsedMilliseconds;
            Console.WriteLine("[PIMA] Phase2 Time: " + t2);
            Console.WriteLine(String.Format("[PIMA] Total Time: {0}", t1 + t2));
            Console.WriteLine(String.Format("[PIMA] Total Count: {0}", finalTable.Count));
            Console.WriteLine();
            #endregion Step 3, 4 & 5

            //Console.WriteLine("==============================================");
            //Console.WriteLine("[IMA_V3] Memory Used: " + memoryUsed + ", Total:" + finalTable.Count);
            //Console.WriteLine("[IMA_V3] Time Elaspsed: " + sw.ElapsedMilliseconds + " ms");
            // Console.WriteLine("[IMA] Total: " + finalTable.Count);

            //foreach (var item in finalTable)
            //{
            //    Console.WriteLine(item);
            //}
            //System.IO.File.WriteAllLines(@"C:\Results\PIMA.txt", finalTable);
        }

        public void saveAndPrInt64Results()
        {
            //TestResultsDatabase.nimbleJoinOutput.Add(testResults.toString());
            //Console.WriteLine("IMA: " + testResults.toString());
            //Console.WriteLine();
        }
    }
}
