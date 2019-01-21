﻿using System;
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
        private List<int> storeId = new List<int>();
        private List<string> storeName = new List<string>();
        private List<char> storeType = new List<char>();

        private List<int> productId = new List<int>();
        private List<string> productName = new List<string>();
        private List<char> supplierDimension = new List<char>();
        private List<int> sID = new List<int>();
        private List<int> pID = new List<int>();
        private List<int> revenue = new List<int>();
        Random rand = new Random();
        private ParallelOptions parallelOptions = null;
        int numberOfDimensionRecords = 6000;
        int numberOfFactRecords = 1000000;

        private static string binaryFilesDirectory = @"C:\Raw_Data_Source_For_Test\SSBM - DBGEN\BF";
        private string scaleFactor { get; set; }

        public TestResults testResults = new TestResults();
        public ParallelInMemoryAggregation(string scaleFactor, int degreeOfParallelism = 1)
        {
            this.scaleFactor = scaleFactor;
            testResults.totalRAMAvailable = Utils.getAvailableRAM();
            parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = degreeOfParallelism };
        }

        ~ParallelInMemoryAggregation()
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
        /// Key Vector is implemented as Dictionary <int, int>
        /// InMemory Accumulator is a MultiDimensional Array
        /// Temporary Table is a Datatable
        /// </summary>
        public void Query_3_1()
        {

            Stopwatch sw = new Stopwatch();

            List<Customer> customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
            List<Supplier> supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
            List<Date> dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));


            #region Step 1 & 2
            sw.Start();
            Dictionary<int, int> kvCustomerDim = new Dictionary<int, int>();
            DataTable tempTableCustomerDim = new DataTable();
            tempTableCustomerDim.Columns.Add("customerNation", typeof(string));
            tempTableCustomerDim.Columns.Add("denseGroupingKey", typeof(int));
           
            Dictionary<int, int> kvSupplierDim = new Dictionary<int, int>();
            DataTable tempTableSupplierDim = new DataTable();
            tempTableSupplierDim.Columns.Add("supplierNation", typeof(string));
            tempTableSupplierDim.Columns.Add("denseGroupingKey", typeof(int));

            Dictionary<int, int> kvDateDim = new Dictionary<int, int>();
            DataTable tempTableDateDim = new DataTable();
            tempTableDateDim.Columns.Add("year", typeof(string));
            tempTableDateDim.Columns.Add("denseGroupingKey", typeof(int));

            Parallel.Invoke(parallelOptions,
                () => {
                    int customerIndex = 0;
                    int dgKeyCustomer = 0;
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
                                        int dgKey = row.Field<int>("denseGroupingKey");
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
                () => {
                    int supplierIndex = 0;
                    int dgKeySupplier = 0;
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
                                        int dgKey = row.Field<int>("denseGroupingKey");
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
                () => {
                    int dgKeyDate = 0;
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
                                        int dgKey = row.Field<int>("denseGroupingKey");
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

            List<int> loCustomerKey = Utils.ReadFromBinaryFiles<int>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
            List<int> loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
            List<int> loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
            List<int> loRevenue = Utils.ReadFromBinaryFiles<int>(loRevenueFile.Replace("BF", "BF" + scaleFactor));

            #region Step 3, 4 & 5

            sw.Reset();
            sw.Start();
            int dgkLengthCustomer = tempTableCustomerDim.Rows.Count + 1;
            int dgkLengthSupplier = tempTableSupplierDim.Rows.Count + 1;
            int dgkLengthDate = tempTableDateDim.Rows.Count + 1;

            int[,,] inMemoryAccumulator = new int[dgkLengthCustomer, dgkLengthSupplier, dgkLengthDate];

            var partitionIndexes = Utils.getPartitionIndexes(loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);

            var tasks = new List<Task>();
            foreach (var indexes in partitionIndexes)
            {
                var t = Task.Factory.StartNew(() => {
                    for (int i = indexes.Item1; i <= indexes.Item2; i++)
                    {
                        int custKey = loCustomerKey[i];
                        int suppKey = loSupplierKey[i];
                        int dateKey = loOrderDate[i];
                        int dgkCustomerDim = 0;
                        int dgkSupplierDim = 0;
                        int dgkDateDim = 0;
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
                        int sumRevenue = inMemoryAccumulator[cdRow.Field<int>("denseGroupingKey")
                            , sdRow.Field<int>("denseGroupingKey")
                            , ddRow.Field<int>("denseGroupingKey")];
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
            //Console.WriteLine(String.Format("[PIMA] Total Items: {0}", finalTable.Count));
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
        public void saveAndPrintResults()
        {
            //TestResultsDatabase.nimbleJoinOutput.Add(testResults.toString());
            //Console.WriteLine("IMA: " + testResults.toString());
            //Console.WriteLine();
        }
    }
}