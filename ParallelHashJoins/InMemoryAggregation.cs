using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParallelHashJoins
{

    class InMemoryAggregation
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
        ParallelOptions po = new ParallelOptions() { MaxDegreeOfParallelism = Environment.ProcessorCount };
        int numberOfDimensionRecords = 6000;
        int numberOfFactRecords = 1000000;

        private static string binaryFilesDirectory = @"C:\Raw_Data_Source_For_Test\SSBM - DBGEN\BF";
        private string scaleFactor { get; set; }

        public TestResults testResults = new TestResults();
        public InMemoryAggregation(string scaleFactor)
        {
            this.scaleFactor = scaleFactor;
            testResults.totalRAMAvailable = Utils.getAvailableRAM();
        }

        ~InMemoryAggregation()
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


        public void generateSmallSampleData()
        {
            storeId.Add(1);
            storeId.Add(2);
            storeId.Add(3);
            storeId.Add(4);
            storeId.Add(5);

            storeName.Add("sa1");
            storeName.Add("sb1");
            storeName.Add("sa2");
            storeName.Add("sb2");
            storeName.Add("sa3");

            storeType.Add('a');
            storeType.Add('b');
            storeType.Add('a');
            storeType.Add('b');
            storeType.Add('a');

            productId.Add(1);
            productId.Add(2);
            productId.Add(3);
            productId.Add(4);
            productId.Add(5);

            productName.Add("pb1");
            productName.Add("pb2");
            productName.Add("pa1");
            productName.Add("pa2");
            productName.Add("pb3");

            supplierDimension.Add('b');
            supplierDimension.Add('b');
            supplierDimension.Add('a');
            supplierDimension.Add('a');
            supplierDimension.Add('b');

            sID.Add(3);
            sID.Add(1);
            sID.Add(3);
            sID.Add(2);
            sID.Add(4);
            sID.Add(5);
            sID.Add(2);
            sID.Add(1);
            sID.Add(3);
            sID.Add(4);

            pID.Add(4);
            pID.Add(2);
            pID.Add(1);
            pID.Add(2);
            pID.Add(5);
            pID.Add(4);
            pID.Add(2);
            pID.Add(3);
            pID.Add(1);
            pID.Add(3);

            revenue.Add(100);
            revenue.Add(120);
            revenue.Add(311);
            revenue.Add(144);
            revenue.Add(150);
            revenue.Add(120);
            revenue.Add(250);
            revenue.Add(364);
            revenue.Add(129);
            revenue.Add(450);
        }

        public void generateBigSampleData()
        {
            for (int i = 0; i < numberOfDimensionRecords; i++)
            {
                storeId.Add(i);
                storeName.Add(getRandomLetter() + " " + getRandomLetter());
                storeType.Add(getRandomLetter());
                productId.Add(i);
                productName.Add(getRandomLetter() + " " + getRandomLetter());
                supplierDimension.Add(getRandomLetter());
            }

            for (int i = 0; i < numberOfFactRecords; i++)
            {
                sID.Add(getRandomInt(0, numberOfDimensionRecords));
                pID.Add(getRandomInt(0, numberOfDimensionRecords));
                revenue.Add(getRandomInt(0, 2000));
            }
        }

        public void IMA_Simple()
        {
            long memoryStart = GC.GetTotalMemory(true);
            Stopwatch sw = new Stopwatch();
            sw.Start();

            Dictionary<int, int> kvStoreDim = new Dictionary<int, int>();
            //DataTable tempTableStoreDim = new DataTable();
            //tempTableStoreDim.Columns.Add("storeName", typeof(string));
            //tempTableStoreDim.Columns.Add("denseGroupingKey", typeof(int));
            Dictionary<int, int> tempTableStoreDim = new Dictionary<int, int>();


            int storeIndex = 0;
            int dgKeyStore = 0;
            foreach (var type in storeType)
            {
                if (type == 'a')
                {
                    dgKeyStore++;
                    kvStoreDim.Add(storeIndex + 1, dgKeyStore);
                    tempTableStoreDim.Add(storeIndex + 1, dgKeyStore);
                }
                else
                {
                    kvStoreDim.Add(storeIndex + 1, 0);
                }
                storeIndex++;
            }

            Dictionary<int, int> kvProductDim = new Dictionary<int, int>();
            Dictionary<int, int> tempTableProductDim = new Dictionary<int, int>();
            int productIndex = 0;
            int dgKeyProduct = 0;
            foreach (var type in supplierDimension)
            {
                if (type == 'b')
                {
                    dgKeyProduct++;
                    kvProductDim.Add(productIndex + 1, dgKeyProduct);
                    tempTableProductDim.Add(productIndex + 1, dgKeyProduct);
                }
                else
                {
                    kvProductDim.Add(productIndex + 1, 0);
                }
                productIndex++;
            }

            int dgkLengthStore = tempTableStoreDim.Count() + 1;
            int dgkLengthProduct = tempTableProductDim.Count() + 1;

            int[,] inMemoryAccumulator = new int[dgkLengthStore, dgkLengthProduct];

            for (int i = 0; i < sID.Count(); i++)
            {
                int storeId = sID[i];
                int productId = pID[i];
                int dgkStoreDim = 0;
                int dgkProductDim = 0;
                if (kvStoreDim.TryGetValue(storeId, out dgkStoreDim) && kvProductDim.TryGetValue(productId, out dgkProductDim))
                {
                    if (dgkStoreDim == 0 || dgkProductDim == 0)
                    {
                        // skip
                    }
                    else
                    {
                        inMemoryAccumulator[dgkStoreDim, dgkProductDim] += revenue[i];
                    }
                }
            }

            List<string> finalTable = new List<string>();
            foreach (var sdRow in tempTableStoreDim)
            {
                foreach (var pdRow in tempTableProductDim)
                {
                    int sumRevenue = inMemoryAccumulator[sdRow.Value, pdRow.Value];
                    if (sumRevenue != 0)
                    {
                        finalTable.Add(sdRow.Key + ", " + pdRow.Key + ", " + sumRevenue);
                    }

                }
            }

            long memoryUsed = GC.GetTotalMemory(true) - memoryStart;
            sw.Stop();

            //foreach (var item in finalTable)
            //{
            //    Console.WriteLine(item);
            //}

            Console.WriteLine("==============================================");
            Console.WriteLine("[IMA] Memory Used: " + memoryUsed);
            Console.WriteLine("[IMA] Time Elaspsed: " + sw.ElapsedMilliseconds + " ms");

        }

        public void IMA_Simple_V2()
        {
            long memoryStart = GC.GetTotalMemory(true);
            Stopwatch sw = new Stopwatch();
            sw.Start();

            Dictionary<int, int> kvStoreDim = new Dictionary<int, int>();
            //DataTable tempTableStoreDim = new DataTable();
            //tempTableStoreDim.Columns.Add("storeName", typeof(string));
            //tempTableStoreDim.Columns.Add("denseGroupingKey", typeof(int));
            Dictionary<string, int> tempTableStoreDim = new Dictionary<string, int>();


            int storeIndex = 0;
            int dgKeyStore = 0;
            foreach (var type in storeType)
            {
                if (type == 'a')
                {
                    string sName = storeName[storeIndex];
                    int dgKey = 0;
                    if (tempTableStoreDim.TryGetValue(sName, out dgKey))
                    {
                        kvStoreDim.Add(storeIndex + 1, dgKey);
                    }
                    else
                    {
                        dgKeyStore++;
                        kvStoreDim.Add(storeIndex + 1, dgKeyStore);
                        tempTableStoreDim.Add(sName, dgKeyStore);
                    }
                }
                else
                {
                    kvStoreDim.Add(storeIndex + 1, 0);
                }
                storeIndex++;
            }

            Dictionary<int, int> kvProductDim = new Dictionary<int, int>();
            Dictionary<string, int> tempTableProductDim = new Dictionary<string, int>();
            int productIndex = 0;
            int dgKeyProduct = 0;
            foreach (var type in supplierDimension)
            {
                if (type == 'b')
                {

                    string pName = productName[productIndex];
                    int dgKey = 0;
                    if (tempTableProductDim.TryGetValue(pName, out dgKey))
                    {
                        kvProductDim.Add(productIndex + 1, dgKey);
                    }
                    else
                    {
                        dgKeyProduct++;
                        kvProductDim.Add(productIndex + 1, dgKeyProduct);
                        tempTableProductDim.Add(pName, dgKeyProduct);
                    }

                }
                else
                {
                    kvProductDim.Add(productIndex + 1, 0);
                }
                productIndex++;
            }

            int dgkLengthStore = tempTableStoreDim.Count() + 1;
            int dgkLengthProduct = tempTableProductDim.Count() + 1;

            int[,] inMemoryAccumulator = new int[dgkLengthStore, dgkLengthProduct];

            for (int i = 0; i < sID.Count(); i++)
            {
                int storeId = sID[i];
                int productId = pID[i];
                int dgkStoreDim = 0;
                int dgkProductDim = 0;
                if (kvStoreDim.TryGetValue(storeId, out dgkStoreDim) && kvProductDim.TryGetValue(productId, out dgkProductDim))
                {
                    if (dgkStoreDim == 0 || dgkProductDim == 0)
                    {
                        // skip
                    }
                    else
                    {
                        inMemoryAccumulator[dgkStoreDim, dgkProductDim] += revenue[i];
                    }
                }
            }

            List<string> finalTable = new List<string>();
            foreach (var sdRow in tempTableStoreDim)
            {
                foreach (var pdRow in tempTableProductDim)
                {
                    int sumRevenue = inMemoryAccumulator[sdRow.Value, pdRow.Value];
                    if (sumRevenue != 0)
                    {
                        finalTable.Add(sdRow.Key + ", " + pdRow.Key + ", " + sumRevenue);
                    }

                }
            }

            long memoryUsed = GC.GetTotalMemory(true) - memoryStart;
            sw.Stop();

            //foreach (var item in finalTable)
            //{
            //    Console.WriteLine(item);
            //}

            Console.WriteLine("==============================================");
            Console.WriteLine("[IMA] Memory Used: " + memoryUsed + ", Total:" + finalTable.Count);
            Console.WriteLine("[IMA] Time Elaspsed: " + sw.ElapsedMilliseconds + " ms");

        }

        /// <summary>
        /// Key Vector is implemented as Dictionary <int, int>
        /// InMemory Accumulator is a MultiDimensional Array
        /// Temporary Table is a Datatable
        /// </summary>
        public void IMA_V3()
        {
            long memoryStart = GC.GetTotalMemory(true);
            Stopwatch sw = new Stopwatch();
            sw.Start();

            Dictionary<int, int> kvStoreDim = new Dictionary<int, int>();
            DataTable tempTableStoreDim = new DataTable();
            tempTableStoreDim.Columns.Add("storeName", typeof(string));
            tempTableStoreDim.Columns.Add("denseGroupingKey", typeof(int));
            // Dictionary<string, int> tempTableStoreDim = new Dictionary<string, int>();


            int storeIndex = 0;
            int dgKeyStore = 0;
            foreach (var type in storeType)
            {
                if (type == 'a')
                {
                    string sName = storeName[storeIndex];
                    if (tempTableStoreDim.Rows.Count > 0)
                    {
                        var tempTable = tempTableStoreDim.Copy();
                        var found = false;
                        foreach (DataRow row in tempTableStoreDim.Rows)
                        {
                            var storeName = row.Field<string>("storeName");
                            if (storeName.Equals(sName))
                            {
                                int dgKey = row.Field<int>("denseGroupingKey");
                                kvStoreDim.Add(storeIndex + 1, dgKey);
                                found = true;
                                break;
                            }
                        }
                        if (!found)
                        {
                            dgKeyStore++;
                            tempTable.Rows.Add(sName, dgKeyStore);
                            kvStoreDim.Add(storeIndex + 1, dgKeyStore);
                        }
                        tempTableStoreDim = tempTable;
                    }
                    else
                    {
                        dgKeyStore++;
                        tempTableStoreDim.Rows.Add(sName, dgKeyStore);
                        kvStoreDim.Add(storeIndex + 1, dgKeyStore);
                    }
                }
                else
                {
                    kvStoreDim.Add(storeIndex + 1, 0);
                }
                storeIndex++;
            }

            Dictionary<int, int> kvProductDim = new Dictionary<int, int>();
            DataTable tempTableProductDim = new DataTable();
            tempTableProductDim.Columns.Add("productName", typeof(string));
            tempTableProductDim.Columns.Add("denseGroupingKey", typeof(int));

            int productIndex = 0;
            int dgKeyProduct = 0;
            foreach (var type in supplierDimension)
            {
                if (type == 'b')
                {
                    string pName = productName[productIndex];
                    if (tempTableProductDim.Rows.Count > 0)
                    {
                        var tempTable = tempTableProductDim.Copy();
                        var found = false;
                        foreach (DataRow row in tempTableProductDim.Rows)
                        {
                            var productName = row.Field<string>("productName");
                            if (productName.Equals(pName))
                            {
                                int dgKey = row.Field<int>("denseGroupingKey");
                                kvProductDim.Add(productIndex + 1, dgKey);
                                found = true;
                                break;
                            }
                        }
                        if (!found)
                        {
                            dgKeyProduct++;
                            tempTable.Rows.Add(pName, dgKeyProduct);
                            kvProductDim.Add(productIndex + 1, dgKeyProduct);
                        }


                        tempTableProductDim = tempTable;
                    }
                    else
                    {
                        dgKeyProduct++;
                        tempTableProductDim.Rows.Add(pName, dgKeyProduct);
                        kvProductDim.Add(productIndex + 1, dgKeyProduct);
                    }

                }
                else
                {
                    kvProductDim.Add(productIndex + 1, 0);
                }
                productIndex++;
            }

            int dgkLengthStore = tempTableStoreDim.Rows.Count + 1;
            int dgkLengthProduct = tempTableProductDim.Rows.Count + 1;

            int[,] inMemoryAccumulator = new int[dgkLengthStore, dgkLengthProduct];

            for (int i = 0; i < sID.Count(); i++)
            {
                int storeId = sID[i];
                int productId = pID[i];
                int dgkStoreDim = 0;
                int dgkProductDim = 0;
                if (kvStoreDim.TryGetValue(storeId, out dgkStoreDim) && kvProductDim.TryGetValue(productId, out dgkProductDim))
                {
                    if (dgkStoreDim == 0 || dgkProductDim == 0)
                    {
                        // skip
                    }
                    else
                    {
                        inMemoryAccumulator[dgkStoreDim, dgkProductDim] += revenue[i];
                    }
                }
            }

            List<string> finalTable = new List<string>();
            foreach (DataRow sdRow in tempTableStoreDim.Rows)
            {
                foreach (DataRow pdRow in tempTableProductDim.Rows)
                {
                    int sumRevenue = inMemoryAccumulator[sdRow.Field<int>("denseGroupingKey"), pdRow.Field<int>("denseGroupingKey")];
                    if (sumRevenue != 0)
                    {
                        finalTable.Add(sdRow.Field<string>("storeName") + ", " + pdRow.Field<string>("productName") + ", " + sumRevenue);
                    }

                }
            }

            long memoryUsed = GC.GetTotalMemory(true) - memoryStart;
            sw.Stop();

            //foreach (var item in finalTable)
            //{
            //    Console.WriteLine(item);
            //}

            Console.WriteLine("==============================================");
            Console.WriteLine("[IMA_V3] Memory Used: " + memoryUsed + ", Total:" + finalTable.Count);
            Console.WriteLine("[IMA_V3] Time Elaspsed: " + sw.ElapsedMilliseconds + " ms");

        }

        /// <summary>
        /// Based on IMA V3
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
            // Dictionary<string, int> tempTableStoreDim = new Dictionary<string, int>();

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

            Dictionary<int, int> kvSupplierDim = new Dictionary<int, int>();
            DataTable tempTableSupplierDim = new DataTable();
            tempTableSupplierDim.Columns.Add("supplierNation", typeof(string));
            tempTableSupplierDim.Columns.Add("denseGroupingKey", typeof(int));

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


            Dictionary<int, int> kvDateDim = new Dictionary<int, int>();
            DataTable tempTableDateDim = new DataTable();
            tempTableDateDim.Columns.Add("year", typeof(string));
            tempTableDateDim.Columns.Add("denseGroupingKey", typeof(int));

            // int dateIndex = 0;
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
                // dateIndex++;
            }

            sw.Stop();
            long t1 = sw.ElapsedMilliseconds;
            Console.WriteLine("[IMA] Phase1 Time: " + t1);
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

            for (int i = 0; i < loCustomerKey.Count(); i++)
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
            Console.WriteLine("[IMA] Phase2 Time: " + t2);
            Console.WriteLine(String.Format("Total Time: {0}", t1 + t2));
            Console.WriteLine();
            #endregion Step 3, 4 & 5

            //Console.WriteLine("==============================================");
            //Console.WriteLine("[IMA_V3] Memory Used: " + memoryUsed + ", Total:" + finalTable.Count);
            //Console.WriteLine("[IMA_V3] Time Elaspsed: " + sw.ElapsedMilliseconds + " ms");
            // Console.WriteLine("[IMA] Total: " + finalTable.Count);

        }

        public void IMA_V3_Parallel()
        {
            long memoryStart = GC.GetTotalMemory(true);
            Stopwatch sw = new Stopwatch();
            sw.Start();

            Dictionary<int, int> kvStoreDim = new Dictionary<int, int>();
            DataTable tempTableStoreDim = new DataTable();
            tempTableStoreDim.Columns.Add("storeName", typeof(string));
            tempTableStoreDim.Columns.Add("denseGroupingKey", typeof(int));
            // Dictionary<string, int> tempTableStoreDim = new Dictionary<string, int>();

            Dictionary<int, int> kvProductDim = new Dictionary<int, int>();
            DataTable tempTableProductDim = new DataTable();
            tempTableProductDim.Columns.Add("productName", typeof(string));
            tempTableProductDim.Columns.Add("denseGroupingKey", typeof(int));
            Parallel.Invoke(po, () =>
            {

                int storeIndex = 0;
                int dgKeyStore = 0;
                foreach (var type in storeType)
                {
                    if (type == 'a')
                    {
                        string sName = storeName[storeIndex];
                        if (tempTableStoreDim.Rows.Count > 0)
                        {
                            var tempTable = tempTableStoreDim.Copy();
                            var found = false;
                            foreach (DataRow row in tempTableStoreDim.Rows)
                            {
                                var storeName = row.Field<string>("storeName");
                                if (storeName.Equals(sName))
                                {
                                    int dgKey = row.Field<int>("denseGroupingKey");
                                    kvStoreDim.Add(storeIndex + 1, dgKey);
                                    found = true;
                                    break;
                                }
                            }
                            if (!found)
                            {
                                dgKeyStore++;
                                tempTable.Rows.Add(sName, dgKeyStore);
                                kvStoreDim.Add(storeIndex + 1, dgKeyStore);
                            }
                            tempTableStoreDim = tempTable;
                        }
                        else
                        {
                            dgKeyStore++;
                            tempTableStoreDim.Rows.Add(sName, dgKeyStore);
                            kvStoreDim.Add(storeIndex + 1, dgKeyStore);
                        }
                    }
                    else
                    {
                        kvStoreDim.Add(storeIndex + 1, 0);
                    }
                    storeIndex++;
                }
            }, () =>
            {
                int productIndex = 0;
                int dgKeyProduct = 0;
                foreach (var type in supplierDimension)
                {
                    if (type == 'b')
                    {
                        string pName = productName[productIndex];
                        if (tempTableProductDim.Rows.Count > 0)
                        {
                            var tempTable = tempTableProductDim.Copy();
                            var found = false;
                            foreach (DataRow row in tempTableProductDim.Rows)
                            {
                                var productName = row.Field<string>("productName");
                                if (productName.Equals(pName))
                                {
                                    int dgKey = row.Field<int>("denseGroupingKey");
                                    kvProductDim.Add(productIndex + 1, dgKey);
                                    found = true;
                                    break;
                                }
                            }
                            if (!found)
                            {
                                dgKeyProduct++;
                                tempTable.Rows.Add(pName, dgKeyProduct);
                                kvProductDim.Add(productIndex + 1, dgKeyProduct);
                            }


                            tempTableProductDim = tempTable;
                        }
                        else
                        {
                            dgKeyProduct++;
                            tempTableProductDim.Rows.Add(pName, dgKeyProduct);
                            kvProductDim.Add(productIndex + 1, dgKeyProduct);
                        }

                    }
                    else
                    {
                        kvProductDim.Add(productIndex + 1, 0);
                    }
                    productIndex++;
                }

            });

            int dgkLengthStore = tempTableStoreDim.Rows.Count + 1;
            int dgkLengthProduct = tempTableProductDim.Rows.Count + 1;

            int[,] inMemoryAccumulator = new int[dgkLengthStore, dgkLengthProduct];

            for (int i = 0; i < sID.Count(); i++)
            {
                int storeId = sID[i];
                int productId = pID[i];
                int dgkStoreDim = 0;
                int dgkProductDim = 0;
                if (kvStoreDim.TryGetValue(storeId, out dgkStoreDim) && kvProductDim.TryGetValue(productId, out dgkProductDim))
                {
                    if (dgkStoreDim == 0 || dgkProductDim == 0)
                    {
                        // skip
                    }
                    else
                    {
                        inMemoryAccumulator[dgkStoreDim, dgkProductDim] += revenue[i];
                    }
                }
            }

            List<string> finalTable = new List<string>();
            foreach (DataRow sdRow in tempTableStoreDim.Rows)
            {
                foreach (DataRow pdRow in tempTableProductDim.Rows)
                {
                    int sumRevenue = inMemoryAccumulator[sdRow.Field<int>("denseGroupingKey"), pdRow.Field<int>("denseGroupingKey")];
                    if (sumRevenue != 0)
                    {
                        finalTable.Add(sdRow.Field<string>("storeName") + ", " + pdRow.Field<string>("productName") + ", " + sumRevenue);
                    }

                }
            }

            long memoryUsed = GC.GetTotalMemory(true) - memoryStart;
            sw.Stop();

            //foreach (var item in finalTable)
            //{
            //    Console.WriteLine(item);
            //}

            Console.WriteLine("==============================================");
            Console.WriteLine("[IMA_V3_Parallel] Memory Used: " + memoryUsed + ", Total:" + finalTable.Count);
            Console.WriteLine("[IMA_V3_Parallel] Time Elaspsed: " + sw.ElapsedMilliseconds + " ms");

        }

        /// <summary>
        /// Uses List with ROW ID and Revenue Value and stores it in Hash Table with DICTIONARY of GROUPING attribute
        /// </summary>
        public void ABC()
        {
            long memoryStart = GC.GetTotalMemory(true);
            Stopwatch sw = new Stopwatch();
            sw.Start();

            Dictionary<int, string> storeDict = new Dictionary<int, string>();
            int storeIndex = 0;
            foreach (var type in storeType)
            {
                if (type == 'a')
                {
                    string sName = storeName[storeIndex];
                    storeDict.Add(storeIndex + 1, sName);
                }
                storeIndex++;
            }

            Dictionary<int, string> productDict = new Dictionary<int, string>();
            int productIndex = 0;
            foreach (var type in supplierDimension)
            {
                if (type == 'b')
                {
                    string pName = productName[productIndex];
                    productDict.Add(productIndex + 1, pName);
                }
                productIndex++;
            }

            Dictionary<string, List<Tuple<int, int>>> storeGroupDict = new Dictionary<string, List<Tuple<int, int>>>();
            int scounter = 0;
            foreach (var sid in sID)
            {
                string sName = string.Empty;
                if (storeDict.TryGetValue(sid, out sName))
                {
                    List<Tuple<int, int>> listAggColl = null;
                    if (storeGroupDict.TryGetValue(sName, out listAggColl))
                    {
                        listAggColl.Add(Tuple.Create(scounter + 1, revenue[scounter]));
                        storeGroupDict[sName] = listAggColl;
                    }
                    else
                    {
                        listAggColl = new List<Tuple<int, int>>();
                        listAggColl.Add(Tuple.Create(scounter + 1, revenue[scounter]));
                        storeGroupDict.Add(sName, listAggColl);
                    }
                }
                scounter++;
            }

            Dictionary<string, List<Tuple<int, int>>> productGroupDict = new Dictionary<string, List<Tuple<int, int>>>();
            int pcounter = 0;
            foreach (var pid in pID)
            {
                string pName = string.Empty;
                if (productDict.TryGetValue(pid, out pName))
                {
                    List<Tuple<int, int>> listAggColl = null;
                    if (productGroupDict.TryGetValue(pName, out listAggColl))
                    {
                        listAggColl.Add(Tuple.Create(pcounter + 1, revenue[pcounter]));
                        productGroupDict[pName] = listAggColl;
                    }
                    else
                    {
                        listAggColl = new List<Tuple<int, int>>();
                        listAggColl.Add(Tuple.Create(pcounter + 1, revenue[pcounter]));
                        productGroupDict.Add(pName, listAggColl);
                    }
                }
                pcounter++;
            }

            List<string> finalTable = new List<string>();
            foreach (var sGItem in storeGroupDict)
            {
                foreach (var pGItem in productGroupDict)
                {
                    var collectionIntersect = sGItem.Value.Intersect(pGItem.Value);
                    if (collectionIntersect.Count() > 0)
                    {
                        int sum = 0;
                        foreach (var item in collectionIntersect)
                        {
                            sum += item.Item2;
                        }
                        finalTable.Add(sGItem.Key + ", " + pGItem.Key + ", " + sum);
                    }
                }
            }
            long memoryUsed = GC.GetTotalMemory(true) - memoryStart;
            sw.Stop();

            //foreach (var item in finalTable)
            //{
            //    Console.WriteLine(item);
            //}

            Console.WriteLine("==============================================");
            Console.WriteLine("[ABC] Memory Used: " + memoryUsed + ", Total:" + finalTable.Count);
            Console.WriteLine("[ABC] Time Elaspsed: " + sw.ElapsedMilliseconds + " ms");
        }

        /// <summary>
        /// used DIC in step 1
        /// Uses DICTIONARY to store GROUPING attribute as key and row ID as value
        /// </summary>
        public void ABC_V2()
        {
            long memoryStart = GC.GetTotalMemory(true);
            Stopwatch sw = new Stopwatch();
            sw.Start();

            Dictionary<int, string> storeDict = new Dictionary<int, string>();
            int storeIndex = 0;
            foreach (var type in storeType)
            {
                if (type == 'a')
                {
                    string sName = storeName[storeIndex];
                    storeDict.Add(storeIndex + 1, sName);
                }
                storeIndex++;
            }

            Dictionary<int, string> productDict = new Dictionary<int, string>();
            int productIndex = 0;
            foreach (var type in supplierDimension)
            {
                if (type == 'b')
                {
                    string pName = productName[productIndex];
                    productDict.Add(productIndex + 1, pName);
                }
                productIndex++;
            }

            Dictionary<string, HashSet<int>> storeGroupDict = new Dictionary<string, HashSet<int>>();
            int scounter = 0;
            foreach (var sid in sID)
            {
                string sName = string.Empty;
                if (storeDict.TryGetValue(sid, out sName))
                {
                    HashSet<int> coll = null;
                    if (storeGroupDict.TryGetValue(sName, out coll))
                    {
                        coll.Add(scounter + 1);
                        storeGroupDict[sName] = coll;
                    }
                    else
                    {
                        coll = new HashSet<int>();
                        coll.Add(scounter + 1);
                        storeGroupDict.Add(sName, coll);
                    }
                }
                scounter++;
            }

            Dictionary<string, HashSet<int>> productGroupDict = new Dictionary<string, HashSet<int>>();
            int pcounter = 0;
            foreach (var pid in pID)
            {
                string pName = string.Empty;
                if (productDict.TryGetValue(pid, out pName))
                {
                    HashSet<int> coll = null;
                    if (productGroupDict.TryGetValue(pName, out coll))
                    {
                        coll.Add(pcounter + 1);
                        productGroupDict[pName] = coll;
                    }
                    else
                    {
                        coll = new HashSet<int>();
                        coll.Add(pcounter + 1);
                        productGroupDict.Add(pName, coll);
                    }
                }
                pcounter++;
            }

            List<string> finalTable = new List<string>();
            foreach (var sGItem in storeGroupDict)
            {
                foreach (var pGItem in productGroupDict)
                {
                    var collectionIntersect = sGItem.Value.Intersect(pGItem.Value);
                    if (collectionIntersect.Count() > 0)
                    {
                        int sum = 0;
                        foreach (var item in collectionIntersect)
                        {
                            sum += revenue[item - 1];
                        }
                        finalTable.Add(sGItem.Key + ", " + pGItem.Key + ", " + sum);
                    }
                }
            }
            long memoryUsed = GC.GetTotalMemory(true) - memoryStart;
            sw.Stop();

            //foreach (var item in finalTable)
            //{
            //    Console.WriteLine(item);
            //}

            Console.WriteLine("==============================================");
            Console.WriteLine("[ABC_V2] Memory Used: " + memoryUsed + ", Total:" + finalTable.Count);
            Console.WriteLine("[ABC_V2] Time Elaspsed: " + sw.ElapsedMilliseconds + " ms");
        }

        /// <summary>
        /// used HashSet in step 1
        /// Uses DICTIONARY to store GROUPING attribute as key and HASH SET of row ID as value
        /// </summary>
        public void ABC_V3()
        {
            long memoryStart = GC.GetTotalMemory(true);
            Stopwatch sw = new Stopwatch();
            sw.Start();

            HashSet<int> storeDict = new HashSet<int>();
            int storeIndex = 0;
            foreach (var type in storeType)
            {
                if (type == 'a')
                {
                    storeDict.Add(storeIndex + 1);
                }
                storeIndex++;
            }

            HashSet<int> productDict = new HashSet<int>();
            int productIndex = 0;
            foreach (var type in supplierDimension)
            {
                if (type == 'b')
                {
                    productDict.Add(productIndex + 1);
                }
                productIndex++;
            }

            Dictionary<string, HashSet<int>> storeGroupDict = new Dictionary<string, HashSet<int>>();
            int scounter = 0;
            foreach (var sid in sID)
            {
                if (storeDict.Contains(sid))
                {
                    string sName = storeName[sid - 1];
                    HashSet<int> coll = null;
                    if (storeGroupDict.TryGetValue(sName, out coll))
                    {
                        coll.Add(scounter + 1);
                        storeGroupDict[sName] = coll;
                    }
                    else
                    {
                        coll = new HashSet<int>();
                        coll.Add(scounter + 1);
                        storeGroupDict.Add(sName, coll);
                    }
                }
                scounter++;
            }

            Dictionary<string, HashSet<int>> productGroupDict = new Dictionary<string, HashSet<int>>();
            int pcounter = 0;
            foreach (var pid in pID)
            {
                if (productDict.Contains(pid))
                {
                    string pName = productName[pid - 1];
                    HashSet<int> coll = null;
                    if (productGroupDict.TryGetValue(pName, out coll))
                    {
                        coll.Add(pcounter + 1);
                        productGroupDict[pName] = coll;
                    }
                    else
                    {
                        coll = new HashSet<int>();
                        coll.Add(pcounter + 1);
                        productGroupDict.Add(pName, coll);
                    }
                }
                pcounter++;
            }

            List<string> finalTable = new List<string>();
            foreach (var sGItem in storeGroupDict)
            {
                foreach (var pGItem in productGroupDict)
                {
                    // http://codebetter.com/patricksmacchia/2011/06/16/linq-intersect-2-7x-faster-with-hashset/
                    //                    If the HashSet < T > is bigger than the other sequence, gains factor can be high (like 15x).
                    //If the HashSet < T > is smaller than the other sequence, gains factor tends to 1x.
                    //   If the HashSet<T> size is comparable to the other sequence size, the gain factor is around 2.7.

                    var collectionIntersect = sGItem.Value.Count > pGItem.Value.Count ? sGItem.Value.Intersect(pGItem.Value) : pGItem.Value.Intersect(sGItem.Value);
                    if (collectionIntersect.Count() > 0)
                    {
                        int sum = 0;
                        foreach (var item in collectionIntersect)
                        {
                            sum += revenue[item - 1];
                        }
                        finalTable.Add(sGItem.Key + ", " + pGItem.Key + ", " + sum);
                    }
                }
            }
            long memoryUsed = GC.GetTotalMemory(true) - memoryStart;
            sw.Stop();

            //foreach (var item in finalTable)
            //{
            //    Console.WriteLine(item);
            //}

            Console.WriteLine("==============================================");
            Console.WriteLine("[ABC_V3] Memory Used: " + memoryUsed + ", Total:" + finalTable.Count);
            Console.WriteLine("[ABC_V3] Time Elaspsed: " + sw.ElapsedMilliseconds + " ms");
        }


        public void ABC_V3_Parallel()
        {
            long memoryStart = GC.GetTotalMemory(true);
            Stopwatch sw = new Stopwatch();
            sw.Start();
            HashSet<int> storeDict = new HashSet<int>();
            HashSet<int> productDict = new HashSet<int>();

            Parallel.Invoke(po, () =>
            {

                int storeIndex = 0;
                foreach (var type in storeType)
                {
                    if (type == 'a')
                    {
                        storeDict.Add(storeIndex + 1);
                    }
                    storeIndex++;
                }
            }, () =>
            {

                int productIndex = 0;
                foreach (var type in supplierDimension)
                {
                    if (type == 'b')
                    {
                        productDict.Add(productIndex + 1);
                    }
                    productIndex++;
                }
            });

            Dictionary<string, HashSet<int>> storeGroupDict = new Dictionary<string, HashSet<int>>();
            Dictionary<string, HashSet<int>> productGroupDict = new Dictionary<string, HashSet<int>>();
            Parallel.Invoke(po, () =>
            {
                int scounter = 0;
                foreach (var sid in sID)
                {
                    if (storeDict.Contains(sid))
                    {
                        string sName = storeName[sid - 1];
                        HashSet<int> coll = null;
                        if (storeGroupDict.TryGetValue(sName, out coll))
                        {
                            coll.Add(scounter + 1);
                            storeGroupDict[sName] = coll;
                        }
                        else
                        {
                            coll = new HashSet<int>();
                            coll.Add(scounter + 1);
                            storeGroupDict.Add(sName, coll);
                        }
                    }
                    scounter++;
                }
            }, () =>
            {
                int pcounter = 0;
                foreach (var pid in pID)
                {
                    if (productDict.Contains(pid))
                    {
                        string pName = productName[pid - 1];
                        HashSet<int> coll = null;
                        if (productGroupDict.TryGetValue(pName, out coll))
                        {
                            coll.Add(pcounter + 1);
                            productGroupDict[pName] = coll;
                        }
                        else
                        {
                            coll = new HashSet<int>();
                            coll.Add(pcounter + 1);
                            productGroupDict.Add(pName, coll);
                        }
                    }
                    pcounter++;
                }
            });

            List<string> finalTable = new List<string>();
            foreach (var sGItem in storeGroupDict)
            {
                foreach (var pGItem in productGroupDict)
                {
                    // http://codebetter.com/patricksmacchia/2011/06/16/linq-intersect-2-7x-faster-with-hashset/
                    //                    If the HashSet < T > is bigger than the other sequence, gains factor can be high (like 15x).
                    //If the HashSet < T > is smaller than the other sequence, gains factor tends to 1x.
                    //   If the HashSet<T> size is comparable to the other sequence size, the gain factor is around 2.7.

                    var collectionIntersect = sGItem.Value.Count > pGItem.Value.Count ? sGItem.Value.Intersect(pGItem.Value) : pGItem.Value.Intersect(sGItem.Value);
                    if (collectionIntersect.Count() > 0)
                    {
                        int sum = 0;
                        foreach (var item in collectionIntersect)
                        {
                            sum += revenue[item - 1];
                        }
                        finalTable.Add(sGItem.Key + ", " + pGItem.Key + ", " + sum);
                    }
                }
            }
            long memoryUsed = GC.GetTotalMemory(true) - memoryStart;
            sw.Stop();

            //foreach (var item in finalTable)
            //{
            //    Console.WriteLine(item);
            //}

            Console.WriteLine("==============================================");
            Console.WriteLine("[ABC_V3_Parallel] Memory Used: " + memoryUsed + ", Total:" + finalTable.Count);
            Console.WriteLine("[ABC_V3_Parallel] Time Elaspsed: " + sw.ElapsedMilliseconds + " ms");
        }

        /// <summary>
        /// Failed Experiment
        /// used HashSet in step 1
        /// Uses DICTIONARY to store GROUPING attribute as key and BIT MAP of row ID as value
        /// </summary>
        public void ABC_V4()
        {
            long memoryStart = GC.GetTotalMemory(true);
            Stopwatch sw = new Stopwatch();
            sw.Start();

            HashSet<int> storeDict = new HashSet<int>();
            int storeIndex = 0;
            foreach (var type in storeType)
            {
                if (type == 'a')
                {
                    storeDict.Add(storeIndex + 1);
                }
                storeIndex++;
            }

            HashSet<int> productDict = new HashSet<int>();
            int productIndex = 0;
            foreach (var type in supplierDimension)
            {
                if (type == 'b')
                {
                    productDict.Add(productIndex + 1);
                }
                productIndex++;
            }

            Dictionary<string, BitArray> storeGroupDict = new Dictionary<string, BitArray>();
            int scounter = 0;
            foreach (var sid in sID)
            {
                if (storeDict.Contains(sid))
                {
                    string sName = storeName[sid - 1];
                    BitArray coll = null;
                    if (storeGroupDict.TryGetValue(sName, out coll))
                    {
                        coll.Set(scounter + 1, true);
                        storeGroupDict[sName] = coll;
                    }
                    else
                    {
                        coll = new BitArray(sID.Count);
                        coll.Set(scounter + 1, true);
                        storeGroupDict.Add(sName, coll);
                    }
                }
                scounter++;
            }

            Dictionary<string, BitArray> productGroupDict = new Dictionary<string, BitArray>();
            int pcounter = 0;
            foreach (var pid in pID)
            {
                if (productDict.Contains(pid))
                {
                    string pName = productName[pid - 1];
                    BitArray coll = null;
                    if (productGroupDict.TryGetValue(pName, out coll))
                    {
                        coll.Set(pcounter + 1, true);
                        productGroupDict[pName] = coll;
                    }
                    else
                    {
                        coll = new BitArray(pID.Count);
                        coll.Set(pcounter + 1, true);
                        productGroupDict.Add(pName, coll);
                    }
                }
                pcounter++;
            }

            List<string> finalTable = new List<string>();

            foreach (var sGItem in storeGroupDict)
            {
                foreach (var pGItem in productGroupDict)
                {
                    // http://codebetter.com/patricksmacchia/2011/06/16/linq-intersect-2-7x-faster-with-hashset/
                    //                    If the HashSet < T > is bigger than the other sequence, gains factor can be high (like 15x).
                    //If the HashSet < T > is smaller than the other sequence, gains factor tends to 1x.
                    //   If the HashSet<T> size is comparable to the other sequence size, the gain factor is around 2.7.


                    BitArray collectionIntersect = (BitArray)sGItem.Value.Clone();
                    BitArray item2 = (BitArray)pGItem.Value.Clone();
                    collectionIntersect.And(item2);
                    var isTrue = false;
                    foreach (bool bit in collectionIntersect)
                    {
                        if (bit)
                        {
                            isTrue = true;
                            break;
                        }
                    }
                    if (isTrue)
                    {
                        int sum = 0;
                        int index = 0;
                        foreach (bool item in collectionIntersect)
                        {
                            if (item)
                                sum += revenue[index - 1];
                            index++;
                        }
                        finalTable.Add(sGItem.Key + ", " + pGItem.Key + ", " + sum);
                    }


                }
            }
            long memoryUsed = GC.GetTotalMemory(true) - memoryStart;
            sw.Stop();

            //foreach (var item in finalTable)
            //{
            //    Console.WriteLine(item);
            //}

            Console.WriteLine("==============================================");
            Console.WriteLine("[ABC_V4] Memory Used: " + memoryUsed + ", Total:" + finalTable.Count);
            Console.WriteLine("[ABC_V4] Time Elaspsed: " + sw.ElapsedMilliseconds + " ms");
        }

        /// <summary>
        /// Failed Experiment
        /// 
        /// </summary>
        public void ABC_V5()
        {
            long memoryStart = GC.GetTotalMemory(true);
            Stopwatch sw = new Stopwatch();
            sw.Start();

            BitArray storeBA = new BitArray(storeId.Count + 1);
            int storeIndex = 1;
            foreach (var type in storeType)
            {
                if (type == 'a')
                {
                    storeBA.Set(storeIndex, true);
                }
                storeIndex++;
            }

            BitArray productBA = new BitArray(productId.Count + 1);
            int productIndex = 1;
            foreach (var type in supplierDimension)
            {
                if (type == 'b')
                {
                    productBA.Set(productIndex, true);
                }
                productIndex++;
            }

            Dictionary<int, HashSet<int>> storeGroupDict = new Dictionary<int, HashSet<int>>();
            int scounter = 0;
            foreach (var sid in sID)
            {
                if (storeBA[sid])
                {
                    //string sName = storeName[sid - 1];
                    HashSet<int> coll = null;
                    if (storeGroupDict.TryGetValue(sid, out coll))
                    {
                        coll.Add(scounter + 1);
                        storeGroupDict[sid] = coll;
                    }
                    else
                    {
                        coll = new HashSet<int>();
                        coll.Add(scounter + 1);
                        storeGroupDict.Add(sid, coll);
                    }
                }
                scounter++;
            }

            Dictionary<int, HashSet<int>> productGroupDict = new Dictionary<int, HashSet<int>>();
            int pcounter = 0;
            foreach (var pid in pID)
            {
                if (productBA[pid])
                {
                    // string pName = productName[pid - 1];
                    HashSet<int> coll = null;
                    if (productGroupDict.TryGetValue(pid, out coll))
                    {
                        coll.Add(pcounter + 1);
                        productGroupDict[pid] = coll;
                    }
                    else
                    {
                        coll = new HashSet<int>();
                        coll.Add(pcounter + 1);
                        productGroupDict.Add(pid, coll);
                    }
                }
                pcounter++;
            }

            List<string> finalTable = new List<string>();
            foreach (var sGItem in storeGroupDict)
            {
                foreach (var pGItem in productGroupDict)
                {
                    // http://codebetter.com/patricksmacchia/2011/06/16/linq-intersect-2-7x-faster-with-hashset/
                    //                    If the HashSet < T > is bigger than the other sequence, gains factor can be high (like 15x).
                    //If the HashSet < T > is smaller than the other sequence, gains factor tends to 1x.
                    //   If the HashSet<T> size is comparable to the other sequence size, the gain factor is around 2.7.

                    var collectionIntersect = sGItem.Value.Count > pGItem.Value.Count ? sGItem.Value.Intersect(pGItem.Value) : pGItem.Value.Intersect(sGItem.Value);
                    if (collectionIntersect.Count() > 0)
                    {
                        int sum = 0;
                        foreach (var item in collectionIntersect)
                        {
                            sum += revenue[item - 1];
                        }
                        finalTable.Add(storeName[sGItem.Key - 1] + ", " + productName[pGItem.Key - 1] + ", " + sum);
                    }
                }
            }
            long memoryUsed = GC.GetTotalMemory(true) - memoryStart;
            sw.Stop();

            //foreach (var item in finalTable)
            //{
            //    Console.WriteLine(item);
            //}

            Console.WriteLine("==============================================");
            Console.WriteLine("[ABC_V5] Memory Used: " + memoryUsed + ", Total:" + finalTable.Count);
            Console.WriteLine("[ABC_V5] Time Elaspsed: " + sw.ElapsedMilliseconds + " ms");
        }

        public void GroupByAttributeSameAsJoinAttribute()
        {
            long memoryStart = GC.GetTotalMemory(true);
            Stopwatch sw = new Stopwatch();
            sw.Start();

            Dictionary<int, List<Tuple<int, int>>> storeDictionary = new Dictionary<int, List<Tuple<int, int>>>();
            int indexSID = 0;
            foreach (var id in sID)
            {
                List<Tuple<int, int>> value = new List<Tuple<int, int>>();
                if (!storeDictionary.TryGetValue(id, out value))
                {
                    List<Tuple<int, int>> item = new List<Tuple<int, int>>();
                    item.Add(Tuple.Create(indexSID + 1, revenue[indexSID]));
                    storeDictionary.Add(id, item);
                }
                else
                {
                    value.Add(Tuple.Create(indexSID + 1, revenue[indexSID]));
                }
                indexSID++;
            }

            Dictionary<int, List<Tuple<int, int>>> productDictionary = new Dictionary<int, List<Tuple<int, int>>>();
            int indexPID = 0;
            foreach (var id in pID)
            {
                List<Tuple<int, int>> value = new List<Tuple<int, int>>();
                if (!productDictionary.TryGetValue(id, out value))
                {
                    List<Tuple<int, int>> item = new List<Tuple<int, int>>();
                    item.Add(Tuple.Create(indexPID + 1, revenue[indexPID]));
                    productDictionary.Add(id, item);
                }
                else
                {
                    value.Add(Tuple.Create(indexPID + 1, revenue[indexPID]));
                }
                indexPID++;

            }

            HashSet<int> storeSet = new HashSet<int>();
            int indexStoreType = 1;
            foreach (var type in storeType)
            {
                if (type.Equals('a')) // search condition
                {
                    storeSet.Add(indexStoreType);
                }
                indexStoreType++;
            }

            HashSet<int> productSet = new HashSet<int>();
            int indexProductType = 1;
            foreach (var type in supplierDimension)
            {
                if (type.Equals('b')) // search condition
                {
                    productSet.Add(indexProductType);
                }
                indexProductType++;
            }

            var tempStoreDict = new Dictionary<int, List<Tuple<int, int>>>(storeDictionary);
            foreach (var item in tempStoreDict)
            {
                if (!storeSet.Contains(item.Key))
                {
                    storeDictionary.Remove(item.Key);
                }

            }

            var tempProductDict = new Dictionary<int, List<Tuple<int, int>>>(productDictionary);
            foreach (var item in tempProductDict)
            {
                if (!productSet.Contains(item.Key))
                {
                    productDictionary.Remove(item.Key);
                }

            }

            List<String> finalTable = new List<string>();
            // Serial part in the algorithm
            foreach (var _store in storeDictionary)
            {
                foreach (var _product in productDictionary)
                {
                    var collectionIntersect = _store.Value.Intersect(_product.Value);
                    if (collectionIntersect.Count() > 0)
                    {
                        int sum = 0;
                        foreach (var item in collectionIntersect)
                        {
                            sum += item.Item2;
                        }
                        finalTable.Add(_store.Key + ", " + _product.Key + ", " + sum);
                    }
                }
            }
            long memoryUsed = GC.GetTotalMemory(true) - memoryStart;
            sw.Stop();

            //foreach (var item in finalTable)
            //{
            //    Console.WriteLine(item);
            //}

            Console.WriteLine("==============================================");
            Console.WriteLine("[GASJA] Memory Used: " + memoryUsed);
            Console.WriteLine("[GASJA] Time Elaspsed: " + sw.ElapsedMilliseconds + " ms");
        }

        public void ParallelGroupByAttributeSameAsJoinAttribute()
        {
            long memoryStart = GC.GetTotalMemory(true);
            Stopwatch sw = new Stopwatch();
            sw.Start();

            Dictionary<int, List<Tuple<int, int>>> storeDictionary = new Dictionary<int, List<Tuple<int, int>>>();
            int indexSID = 0;

            Dictionary<int, List<Tuple<int, int>>> productDictionary = new Dictionary<int, List<Tuple<int, int>>>();
            int indexPID = 0;

            HashSet<int> storeSet = new HashSet<int>();
            int indexStoreType = 1;

            HashSet<int> productSet = new HashSet<int>();
            int indexProductType = 1;

            Parallel.Invoke(po, () =>
            {
                foreach (var id in sID)
                {
                    List<Tuple<int, int>> value = new List<Tuple<int, int>>();
                    if (!storeDictionary.TryGetValue(id, out value))
                    {
                        List<Tuple<int, int>> item = new List<Tuple<int, int>>();
                        item.Add(Tuple.Create(indexSID + 1, revenue[indexSID]));
                        storeDictionary.Add(id, item);
                    }
                    else
                    {
                        value.Add(Tuple.Create(indexSID + 1, revenue[indexSID]));
                    }
                    indexSID++;
                }
            },
            () =>
            {
                foreach (var id in pID)
                {
                    List<Tuple<int, int>> value = new List<Tuple<int, int>>();
                    if (!productDictionary.TryGetValue(id, out value))
                    {
                        List<Tuple<int, int>> item = new List<Tuple<int, int>>();
                        item.Add(Tuple.Create(indexPID + 1, revenue[indexPID]));
                        productDictionary.Add(id, item);
                    }
                    else
                    {
                        value.Add(Tuple.Create(indexPID + 1, revenue[indexPID]));
                    }
                    indexPID++;

                }
            }, () =>
            {
                foreach (var type in storeType)
                {
                    if (type.Equals('a')) // search condition
                    {
                        storeSet.Add(indexStoreType);
                    }
                    indexStoreType++;
                }
            }, () =>
            {
                foreach (var type in supplierDimension)
                {
                    if (type.Equals('b')) // search condition
                    {
                        productSet.Add(indexProductType);
                    }
                    indexProductType++;
                }
            }

                );

            Parallel.Invoke(po, () =>
            {
                var tempStoreDict = new Dictionary<int, List<Tuple<int, int>>>(storeDictionary);
                foreach (var item in tempStoreDict)
                {
                    if (!storeSet.Contains(item.Key))
                    {
                        storeDictionary.Remove(item.Key);
                    }

                }
            }, () =>
            {
                var tempProductDict = new Dictionary<int, List<Tuple<int, int>>>(productDictionary);
                foreach (var item in tempProductDict)
                {
                    if (!productSet.Contains(item.Key))
                    {
                        productDictionary.Remove(item.Key);
                    }

                }
            });


            List<String> finalTable = new List<string>();
            // Serial part in the algorithm
            foreach (var _store in storeDictionary)
            {
                foreach (var _product in productDictionary)
                {
                    var collectionIntersect = _store.Value.Intersect(_product.Value);
                    if (collectionIntersect.Count() > 0)
                    {
                        int sum = 0;
                        foreach (var item in collectionIntersect)
                        {
                            sum += item.Item2;
                        }
                        finalTable.Add(_store.Key + ", " + _product.Key + ", " + sum);
                    }
                }
            }
            long memoryUsed = GC.GetTotalMemory(true) - memoryStart;
            sw.Stop();

            //foreach (var item in finalTable)
            //{
            //    Console.WriteLine(item);
            //}

            Console.WriteLine("==============================================");
            Console.WriteLine("[PGASJA] Memory Used: " + memoryUsed);
            Console.WriteLine("[PGASJA] Time Elaspsed: " + sw.ElapsedMilliseconds + " ms");
        }

        public void GroupByAttributeDifferentFromJoinAttribute()
        {
            long memoryStart = GC.GetTotalMemory(true);
            Stopwatch sw = new Stopwatch();
            sw.Start();

            Dictionary<int, string> storeDict = new Dictionary<int, string>();
            int storeIndex = 0;
            foreach (var type in storeType)
            {
                if (type == 'a')
                {
                    string sName = storeName[storeIndex];
                    storeDict.Add(storeIndex + 1, sName);
                }
                storeIndex++;
            }

            Dictionary<int, string> productDict = new Dictionary<int, string>();
            int productIndex = 0;
            foreach (var type in supplierDimension)
            {
                if (type == 'b')
                {
                    string pName = productName[productIndex];
                    productDict.Add(productIndex + 1, pName);
                }
                productIndex++;
            }

            Dictionary<string, List<Tuple<int, int>>> storeGroupDict = new Dictionary<string, List<Tuple<int, int>>>();
            int scounter = 0;
            foreach (var sid in sID)
            {
                string storeName = string.Empty;
                if (storeDict.TryGetValue(sid, out storeName))
                {
                    List<Tuple<int, int>> listAggColl = null;
                    if (storeGroupDict.TryGetValue(storeName, out listAggColl))
                    {
                        listAggColl.Add(Tuple.Create(scounter + 1, revenue[scounter]));
                        storeGroupDict[storeName] = listAggColl;
                    }
                    else
                    {
                        listAggColl = new List<Tuple<int, int>>();
                        listAggColl.Add(Tuple.Create(scounter + 1, revenue[scounter]));
                        storeGroupDict.Add(storeName, listAggColl);
                    }
                }
                scounter++;
            }

            Dictionary<string, List<Tuple<int, int>>> productGroupDict = new Dictionary<string, List<Tuple<int, int>>>();
            int pcounter = 0;
            foreach (var pid in pID)
            {
                string productName = string.Empty;
                if (productDict.TryGetValue(pid, out productName))
                {
                    List<Tuple<int, int>> listAggColl = null;
                    if (productGroupDict.TryGetValue(productName, out listAggColl))
                    {
                        listAggColl.Add(Tuple.Create(pcounter + 1, revenue[pcounter]));
                        productGroupDict[productName] = listAggColl;
                    }
                    else
                    {
                        listAggColl = new List<Tuple<int, int>>();
                        listAggColl.Add(Tuple.Create(pcounter + 1, revenue[pcounter]));
                        productGroupDict.Add(productName, listAggColl);
                    }
                }
                pcounter++;
            }

            List<string> finalTable = new List<string>();
            foreach (var sGItem in storeGroupDict)
            {
                foreach (var pGItem in productGroupDict)
                {
                    var collectionIntersect = sGItem.Value.Intersect(pGItem.Value);
                    if (collectionIntersect.Count() > 0)
                    {
                        int sum = 0;
                        foreach (var item in collectionIntersect)
                        {
                            sum += item.Item2;
                        }
                        finalTable.Add(sGItem.Key + ", " + pGItem.Key + ", " + sum);
                    }
                }
            }
            long memoryUsed = GC.GetTotalMemory(true) - memoryStart;
            sw.Stop();

            //foreach (var item in finalTable)
            //{
            //    Console.WriteLine(item);
            //}

            Console.WriteLine("==============================================");
            Console.WriteLine("[GADJA] Memory Used: " + memoryUsed);
            Console.WriteLine("[GADJA] Time Elaspsed: " + sw.ElapsedMilliseconds + " ms");

        }

        public void ParallelGroupByAttributeDifferentFromJoinAttribute()
        {
            long memoryStart = GC.GetTotalMemory(true);
            Stopwatch sw = new Stopwatch();
            sw.Start();

            Dictionary<int, string> storeDict = new Dictionary<int, string>();
            int storeIndex = 0;

            Dictionary<int, string> productDict = new Dictionary<int, string>();
            int productIndex = 0;

            Parallel.Invoke(po, () =>
            {
                foreach (var type in storeType)
                {
                    if (type == 'a')
                    {
                        string sName = storeName[storeIndex];
                        storeDict.Add(storeIndex + 1, sName);
                    }
                    storeIndex++;
                }
            }, () =>
            {
                foreach (var type in supplierDimension)
                {
                    if (type == 'b')
                    {
                        string pName = productName[productIndex];
                        productDict.Add(productIndex + 1, pName);
                    }
                    productIndex++;
                }
            });

            Dictionary<string, List<Tuple<int, int>>> storeGroupDict = new Dictionary<string, List<Tuple<int, int>>>();
            int scounter = 0;

            Dictionary<string, List<Tuple<int, int>>> productGroupDict = new Dictionary<string, List<Tuple<int, int>>>();
            int pcounter = 0;

            Parallel.Invoke(po, () =>
            {
                foreach (var sid in sID)
                {
                    string storeName = string.Empty;
                    if (storeDict.TryGetValue(sid, out storeName))
                    {
                        List<Tuple<int, int>> listAggColl = null;
                        if (storeGroupDict.TryGetValue(storeName, out listAggColl))
                        {
                            listAggColl.Add(Tuple.Create(scounter + 1, revenue[scounter]));
                            storeGroupDict[storeName] = listAggColl;
                        }
                        else
                        {
                            listAggColl = new List<Tuple<int, int>>();
                            listAggColl.Add(Tuple.Create(scounter + 1, revenue[scounter]));
                            storeGroupDict.Add(storeName, listAggColl);
                        }
                    }
                    scounter++;
                }
            }, () =>
            {
                foreach (var pid in pID)
                {
                    string productName = string.Empty;
                    if (productDict.TryGetValue(pid, out productName))
                    {
                        List<Tuple<int, int>> listAggColl = null;
                        if (productGroupDict.TryGetValue(productName, out listAggColl))
                        {
                            listAggColl.Add(Tuple.Create(pcounter + 1, revenue[pcounter]));
                            productGroupDict[productName] = listAggColl;
                        }
                        else
                        {
                            listAggColl = new List<Tuple<int, int>>();
                            listAggColl.Add(Tuple.Create(pcounter + 1, revenue[pcounter]));
                            productGroupDict.Add(productName, listAggColl);
                        }
                    }
                    pcounter++;
                }
            });

            List<string> finalTable = new List<string>();
            foreach (var sGItem in storeGroupDict)
            {
                foreach (var pGItem in productGroupDict)
                {
                    var collectionIntersect = sGItem.Value.Intersect(pGItem.Value);
                    if (collectionIntersect.Count() > 0)
                    {
                        int sum = 0;
                        foreach (var item in collectionIntersect)
                        {
                            sum += item.Item2;
                        }
                        finalTable.Add(sGItem.Key + ", " + pGItem.Key + ", " + sum);
                    }
                }
            }
            long memoryUsed = GC.GetTotalMemory(true) - memoryStart;
            sw.Stop();

            //foreach (var item in finalTable)
            //{
            //    Console.WriteLine(item);
            //}

            Console.WriteLine("==============================================");
            Console.WriteLine("[PGADJA] Memory Used: " + memoryUsed);
            Console.WriteLine("[PGADJA] Time Elaspsed: " + sw.ElapsedMilliseconds + " ms");

        }

        public int getRandomInt(int minimum, int maximum)
        {
            return rand.Next(minimum, maximum);
        }

        public char getRandomLetter()
        {
            int num = getRandomInt(0, 26);
            char let = Convert.ToChar('a' + num);
            return let;
        }

        public void saveAndPrintResults()
        {
            //TestResultsDatabase.nimbleJoinOutput.Add(testResults.toString());
            //Console.WriteLine("IMA: " + testResults.toString());
            //Console.WriteLine();
        }
    }

}

