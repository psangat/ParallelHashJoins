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
        private List<Int64> storeId = new List<Int64>();
        private List<string> storeName = new List<string>();
        private List<char> storeType = new List<char>();

        private List<Int64> productId = new List<Int64>();
        private List<string> productName = new List<string>();
        private List<char> supplierDimension = new List<char>();
        private List<Int64> sID = new List<Int64>();
        private List<Int64> pID = new List<Int64>();
        private List<Int64> revenue = new List<Int64>();
        Random rand = new Random();
        ParallelOptions po = new ParallelOptions() { MaxDegreeOfParallelism = Environment.ProcessorCount };
        Int64 numberOfDimensionRecords = 6000;
        Int64 numberOfFactRecords = 1000000;

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
            //saveAndPrInt64Results();
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
            for (Int64 i = 0; i < numberOfDimensionRecords; i++)
            {
                storeId.Add(i);
                storeName.Add(getRandomLetter() + " " + getRandomLetter());
                storeType.Add(getRandomLetter());
                productId.Add(i);
                productName.Add(getRandomLetter() + " " + getRandomLetter());
                supplierDimension.Add(getRandomLetter());
            }

            for (Int64 i = 0; i < numberOfFactRecords; i++)
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

            Dictionary<Int64, Int64> kvStoreDim = new Dictionary<Int64, Int64>();
            //DataTable tempTableStoreDim = new DataTable();
            //tempTableStoreDim.Columns.Add("storeName", typeof(string));
            //tempTableStoreDim.Columns.Add("denseGroupingKey", typeof(Int64));
            Dictionary<Int64, Int64> tempTableStoreDim = new Dictionary<Int64, Int64>();


            Int64 storeIndex = 0;
            Int64 dgKeyStore = 0;
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

            Dictionary<Int64, Int64> kvProductDim = new Dictionary<Int64, Int64>();
            Dictionary<Int64, Int64> tempTableProductDim = new Dictionary<Int64, Int64>();
            Int64 productIndex = 0;
            Int64 dgKeyProduct = 0;
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

            Int64 dgkLengthStore = tempTableStoreDim.Count() + 1;
            Int64 dgkLengthProduct = tempTableProductDim.Count() + 1;

            Int64[,] inMemoryAccumulator = new Int64[dgkLengthStore, dgkLengthProduct];

            for (Int64 i = 0; i < sID.Count(); i++)
            {
                Int64 storeId = sID[i];
                Int64 productId = pID[i];
                Int64 dgkStoreDim = 0;
                Int64 dgkProductDim = 0;
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
                    Int64 sumRevenue = inMemoryAccumulator[sdRow.Value, pdRow.Value];
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

            Dictionary<Int64, Int64> kvStoreDim = new Dictionary<Int64, Int64>();
            //DataTable tempTableStoreDim = new DataTable();
            //tempTableStoreDim.Columns.Add("storeName", typeof(string));
            //tempTableStoreDim.Columns.Add("denseGroupingKey", typeof(Int64));
            Dictionary<string, Int64> tempTableStoreDim = new Dictionary<string, Int64>();


            Int64 storeIndex = 0;
            Int64 dgKeyStore = 0;
            foreach (var type in storeType)
            {
                if (type == 'a')
                {
                    string sName = storeName[storeIndex];
                    Int64 dgKey = 0;
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

            Dictionary<Int64, Int64> kvProductDim = new Dictionary<Int64, Int64>();
            Dictionary<string, Int64> tempTableProductDim = new Dictionary<string, Int64>();
            Int64 productIndex = 0;
            Int64 dgKeyProduct = 0;
            foreach (var type in supplierDimension)
            {
                if (type == 'b')
                {

                    string pName = productName[productIndex];
                    Int64 dgKey = 0;
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

            Int64 dgkLengthStore = tempTableStoreDim.Count() + 1;
            Int64 dgkLengthProduct = tempTableProductDim.Count() + 1;

            Int64[,] inMemoryAccumulator = new Int64[dgkLengthStore, dgkLengthProduct];

            for (Int64 i = 0; i < sID.Count(); i++)
            {
                Int64 storeId = sID[i];
                Int64 productId = pID[i];
                Int64 dgkStoreDim = 0;
                Int64 dgkProductDim = 0;
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
                    Int64 sumRevenue = inMemoryAccumulator[sdRow.Value, pdRow.Value];
                    if (sumRevenue != 0)
                    {
                        finalTable.Add(sdRow.Key + ", " + pdRow.Key + ", " + sumRevenue);
                    }

                }
            }

            long memoryUsed = GC.GetTotalMemory(true) - memoryStart;
            sw.Stop();



            Console.WriteLine("==============================================");
            Console.WriteLine("[IMA] Memory Used: " + memoryUsed + ", Total:" + finalTable.Count);
            Console.WriteLine("[IMA] Time Elaspsed: " + sw.ElapsedMilliseconds + " ms");
            foreach (var item in finalTable)
            {
                Console.WriteLine(item);
            }


        }

        /// <summary>
        /// Key Vector is implemented as Dictionary <Int64, Int64>
        /// InMemory Accumulator is a MultiDimensional Array
        /// Temporary Table is a Datatable
        /// </summary>
        public void IMA_V3()
        {
            long memoryStart = GC.GetTotalMemory(true);
            Stopwatch sw = new Stopwatch();
            sw.Start();

            Dictionary<Int64, Int64> kvStoreDim = new Dictionary<Int64, Int64>();
            DataTable tempTableStoreDim = new DataTable();
            tempTableStoreDim.Columns.Add("storeName", typeof(string));
            tempTableStoreDim.Columns.Add("denseGroupingKey", typeof(Int64));
            // Dictionary<string, Int64> tempTableStoreDim = new Dictionary<string, Int64>();


            Int64 storeIndex = 0;
            Int64 dgKeyStore = 0;
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
                                Int64 dgKey = row.Field<Int64>("denseGroupingKey");
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

            Dictionary<Int64, Int64> kvProductDim = new Dictionary<Int64, Int64>();
            DataTable tempTableProductDim = new DataTable();
            tempTableProductDim.Columns.Add("productName", typeof(string));
            tempTableProductDim.Columns.Add("denseGroupingKey", typeof(Int64));

            Int64 productIndex = 0;
            Int64 dgKeyProduct = 0;
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
                                Int64 dgKey = row.Field<Int64>("denseGroupingKey");
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

            Int64 dgkLengthStore = tempTableStoreDim.Rows.Count + 1;
            Int64 dgkLengthProduct = tempTableProductDim.Rows.Count + 1;

            Int64[,] inMemoryAccumulator = new Int64[dgkLengthStore, dgkLengthProduct];

            for (Int64 i = 0; i < sID.Count(); i++)
            {
                Int64 storeId = sID[i];
                Int64 productId = pID[i];
                Int64 dgkStoreDim = 0;
                Int64 dgkProductDim = 0;
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
                    Int64 sumRevenue = inMemoryAccumulator[sdRow.Field<Int64>("denseGroupingKey"), pdRow.Field<Int64>("denseGroupingKey")];
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
            Dictionary<Int64, Int64> kvCustomerDim = new Dictionary<Int64, Int64>();
            DataTable tempTableCustomerDim = new DataTable();
            tempTableCustomerDim.Columns.Add("customerNation", typeof(string));
            tempTableCustomerDim.Columns.Add("denseGroupingKey", typeof(Int64));
            // Dictionary<string, Int64> tempTableStoreDim = new Dictionary<string, Int64>();

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

            Dictionary<Int64, Int64> kvSupplierDim = new Dictionary<Int64, Int64>();
            DataTable tempTableSupplierDim = new DataTable();
            tempTableSupplierDim.Columns.Add("supplierNation", typeof(string));
            tempTableSupplierDim.Columns.Add("denseGroupingKey", typeof(Int64));

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


            Dictionary<Int64, Int64> kvDateDim = new Dictionary<Int64, Int64>();
            DataTable tempTableDateDim = new DataTable();
            tempTableDateDim.Columns.Add("year", typeof(string));
            tempTableDateDim.Columns.Add("denseGroupingKey", typeof(Int64));

            // Int64 dateIndex = 0;
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
                // dateIndex++;
            }

            sw.Stop();
            long t1 = sw.ElapsedMilliseconds;
            Console.WriteLine("[IMA] Phase1 Time: " + t1);
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

            for (Int64 i = 0; i < loCustomerKey.Count(); i++)
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
            Console.WriteLine("[IMA] Phase2 Time: " + t2);
            Console.WriteLine(String.Format("[IMA] Total Time: {0}", t1 + t2));
            // Console.WriteLine(String.Format("[IMA] Total Items: {0}", finalTable.Count));
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
            //System.IO.File.WriteAllLines(@"C:\Results\IMA.txt", finalTable);
        }

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
                Console.WriteLine(String.Format("[ATire Join] Total Time: {0}", t2));
                // Console.WriteLine(String.Format("[Invisible Join] Total Time: {0}", t0 + t1 + t2));
                Console.WriteLine(String.Format("[ATire Join] Total : {0}", joinOutputFinal.Count));
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


            #region Step 1 & 2
            sw.Start();
            Dictionary<Int64, Int64> kvCustomerDim = new Dictionary<Int64, Int64>();
            DataTable tempTableCustomerDim = new DataTable();
            tempTableCustomerDim.Columns.Add("customerRegion", typeof(string));
            tempTableCustomerDim.Columns.Add("denseGroupingKey", typeof(Int64));
            // Dictionary<string, Int64> tempTableStoreDim = new Dictionary<string, Int64>();

            Int64 customerIndex = 0;
            Int64 dgKeyCustomer = 0;
            foreach (var customer in customerDimension)
            {
                // if (customer.cRegion.Equals("ASIA"))
                //{
                string cRegion = customer.cRegion;
                if (tempTableCustomerDim.Rows.Count > 0)
                {
                    var tempTable = tempTableCustomerDim.Copy();
                    var found = false;
                    foreach (DataRow row in tempTableCustomerDim.Rows)
                    {
                        var customerRegion = row.Field<string>("customerRegion");
                        if (customerRegion.Equals(cRegion))
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
                        tempTable.Rows.Add(cRegion, dgKeyCustomer);
                        kvCustomerDim.Add(customerIndex + 1, dgKeyCustomer);
                    }
                    tempTableCustomerDim = tempTable;
                }
                else
                {
                    dgKeyCustomer++;
                    tempTableCustomerDim.Rows.Add(cRegion, dgKeyCustomer);
                    kvCustomerDim.Add(customerIndex + 1, dgKeyCustomer);
                }
                //}
                //else
                //{
                //    kvCustomerDim.Add(customerIndex + 1, 0);
                //}
                customerIndex++;
            }

            Dictionary<Int64, Int64> kvPartDim = new Dictionary<Int64, Int64>();
            DataTable tempTablePartDim = new DataTable();
            tempTablePartDim.Columns.Add("partMFGR", typeof(string));
            tempTablePartDim.Columns.Add("denseGroupingKey", typeof(Int64));

            Int64 partIndex = 0;
            Int64 dgKeyPart = 0;
            foreach (var part in partDimension)
            {
                //if (part.sRegion.Equals("ASIA"))
                //{
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
                //}
                //else
                //{
                //   kvPartDim.Add(partIndex + 1, 0);
                //}
                partIndex++;
            }
            sw.Stop();
            long t1 = sw.ElapsedMilliseconds;
            Console.WriteLine("[IMA] AGTest2 T0 Time: " + t1);
            #endregion Step 1 & 2

            #region Step 3, 4 & 5

            sw.Reset();
            sw.Start();
            Int64 dgkLengthCustomer = tempTableCustomerDim.Rows.Count + 1;
            Int64 dgkLengthPart = tempTablePartDim.Rows.Count + 1;

            Int64[,] inMemoryAccumulatorTax = new Int64[dgkLengthCustomer, dgkLengthPart];
            Int64[,] inMemoryAccumulatorDiscount = new Int64[dgkLengthCustomer, dgkLengthPart];
            Int64[,] inMemoryAccumulatorQuantity = new Int64[dgkLengthCustomer, dgkLengthPart];
            Int64[,] inMemoryAccumulatorSupplyCost = new Int64[dgkLengthCustomer, dgkLengthPart];
            Int64[,] inMemoryAccumulatorRevenue = new Int64[dgkLengthCustomer, dgkLengthPart];
            Int64[,] inMemoryAccumulatorOrderTotalPrice = new Int64[dgkLengthCustomer, dgkLengthPart];

            for (Int64 i = 0; i < loCustomerKey.Count(); i++)
            {
                Int64 custKey = loCustomerKey[i];
                Int64 partKey = loPartKey[i];
                Int64 dgkCustomerDim = 0;
                Int64 dgkPartDim = 0;
                if (kvCustomerDim.TryGetValue(custKey, out dgkCustomerDim)
                    && kvPartDim.TryGetValue(partKey, out dgkPartDim))
                {
                    if (dgkCustomerDim == 0 || dgkPartDim == 0)
                    {
                        // skip
                    }
                    else
                    {
                        switch (numberOfAggregations)
                        {
                            case 1:
                                inMemoryAccumulatorTax[dgkCustomerDim, dgkPartDim] += loTax[i];
                                break;
                            case 2:
                                inMemoryAccumulatorTax[dgkCustomerDim, dgkPartDim] += loTax[i];
                                inMemoryAccumulatorDiscount[dgkCustomerDim, dgkPartDim] += loDiscount[i];
                                break;
                            case 3:
                                inMemoryAccumulatorTax[dgkCustomerDim, dgkPartDim] += loTax[i];
                                inMemoryAccumulatorDiscount[dgkCustomerDim, dgkPartDim] += loDiscount[i];
                                inMemoryAccumulatorQuantity[dgkCustomerDim, dgkPartDim] += loQuantity[i];
                                break;
                            case 4:
                                inMemoryAccumulatorTax[dgkCustomerDim, dgkPartDim] += loTax[i];
                                inMemoryAccumulatorDiscount[dgkCustomerDim, dgkPartDim] += loDiscount[i];
                                inMemoryAccumulatorQuantity[dgkCustomerDim, dgkPartDim] += loQuantity[i];
                                inMemoryAccumulatorSupplyCost[dgkCustomerDim, dgkPartDim] += loSupplyCost[i];
                                break;
                            case 5:
                                inMemoryAccumulatorTax[dgkCustomerDim, dgkPartDim] += loTax[i];
                                inMemoryAccumulatorDiscount[dgkCustomerDim, dgkPartDim] += loDiscount[i];
                                inMemoryAccumulatorQuantity[dgkCustomerDim, dgkPartDim] += loQuantity[i];
                                inMemoryAccumulatorSupplyCost[dgkCustomerDim, dgkPartDim] += loSupplyCost[i];
                                inMemoryAccumulatorRevenue[dgkCustomerDim, dgkPartDim] += loRevenue[i];
                                break;
                            case 6:
                                inMemoryAccumulatorTax[dgkCustomerDim, dgkPartDim] += loTax[i];
                                inMemoryAccumulatorDiscount[dgkCustomerDim, dgkPartDim] += loDiscount[i];
                                inMemoryAccumulatorQuantity[dgkCustomerDim, dgkPartDim] += loQuantity[i];
                                inMemoryAccumulatorSupplyCost[dgkCustomerDim, dgkPartDim] += loSupplyCost[i];
                                inMemoryAccumulatorRevenue[dgkCustomerDim, dgkPartDim] += loRevenue[i];
                                inMemoryAccumulatorOrderTotalPrice[dgkCustomerDim, dgkPartDim] += loOrderTotalPrice[i];
                                break;
                        }

                    }
                }
            }

            List<string> finalTable = new List<string>();
            foreach (DataRow cdRow in tempTableCustomerDim.Rows)
            {
                foreach (DataRow pdRow in tempTablePartDim.Rows)
                {
                    Int64 sumTax = 0;
                    Int64 sumDiscount = 0;
                    Int64 sumQuantity = 0;
                    Int64 sumSupplyCost = 0;
                    Int64 sumRevenue = 0;
                    Int64 sumOrderTotalPrice = 0;
                    Int64 cdKey = cdRow.Field<Int64>("denseGroupingKey");
                    Int64 pdKey = pdRow.Field<Int64>("denseGroupingKey");
                    switch (numberOfAggregations)
                    {
                        case 1:
                            sumTax = inMemoryAccumulatorTax[cdKey, pdKey];
                            if (sumTax != 0)
                            {
                                finalTable.Add(cdRow.Field<string>("customerRegion")
                                    + ", " + pdRow.Field<string>("partMFGR")
                                    + ", " + sumTax);
                            }
                            break;
                        case 2:
                            sumTax = inMemoryAccumulatorTax[cdKey, pdKey];
                            sumDiscount = inMemoryAccumulatorDiscount[cdKey, pdKey];
                            if (sumTax != 0 && sumDiscount != 0)
                            {
                                finalTable.Add(cdRow.Field<string>("customerRegion")
                                    + ", " + pdRow.Field<string>("partMFGR")
                                    + ", " + sumTax
                                    + ", " + sumDiscount);
                            }
                            break;
                        case 3:
                            sumTax = inMemoryAccumulatorTax[cdKey, pdKey];
                            sumDiscount = inMemoryAccumulatorDiscount[cdKey, pdKey];
                            sumQuantity = inMemoryAccumulatorQuantity[cdKey, pdKey];
                            if (sumTax != 0 && sumDiscount != 0 && sumQuantity != 0)
                            {
                                finalTable.Add(cdRow.Field<string>("customerRegion")
                                    + ", " + pdRow.Field<string>("partMFGR")
                                    + ", " + sumTax
                                    + ", " + sumDiscount
                                    + ", " + sumQuantity);
                            }
                            break;
                        case 4:
                            sumTax = inMemoryAccumulatorTax[cdKey, pdKey];
                            sumDiscount = inMemoryAccumulatorDiscount[cdKey, pdKey];
                            sumQuantity = inMemoryAccumulatorQuantity[cdKey, pdKey];
                            sumSupplyCost = inMemoryAccumulatorSupplyCost[cdKey, pdKey];
                            if (sumTax != 0 && sumDiscount != 0 && sumQuantity != 0 && sumSupplyCost != 0)
                            {
                                finalTable.Add(cdRow.Field<string>("customerRegion")
                                    + ", " + pdRow.Field<string>("partMFGR")
                                    + ", " + sumTax
                                    + ", " + sumDiscount
                                    + ", " + sumQuantity
                                    + ", " + sumSupplyCost);
                            }
                            break;
                        case 5:
                            sumTax = inMemoryAccumulatorTax[cdKey, pdKey];
                            sumDiscount = inMemoryAccumulatorDiscount[cdKey, pdKey];
                            sumQuantity = inMemoryAccumulatorQuantity[cdKey, pdKey];
                            sumSupplyCost = inMemoryAccumulatorSupplyCost[cdKey, pdKey];
                            sumRevenue = inMemoryAccumulatorRevenue[cdKey, pdKey];
                            if (sumTax != 0 && sumDiscount != 0 && sumQuantity != 0 && sumSupplyCost != 0 && sumRevenue != 0)
                            {
                                finalTable.Add(cdRow.Field<string>("customerRegion")
                                    + ", " + pdRow.Field<string>("partMFGR")
                                    + ", " + sumTax
                                    + ", " + sumDiscount
                                    + ", " + sumQuantity
                                    + ", " + sumSupplyCost
                                    + ", " + sumRevenue);
                            }
                            break;
                        case 6:
                            sumTax = inMemoryAccumulatorTax[cdKey, pdKey];
                            sumDiscount = inMemoryAccumulatorDiscount[cdKey, pdKey];
                            sumQuantity = inMemoryAccumulatorQuantity[cdKey, pdKey];
                            sumSupplyCost = inMemoryAccumulatorSupplyCost[cdKey, pdKey];
                            sumRevenue = inMemoryAccumulatorRevenue[cdKey, pdKey];
                            sumOrderTotalPrice = inMemoryAccumulatorOrderTotalPrice[cdKey, pdKey];
                            if (sumTax != 0 && sumDiscount != 0 && sumQuantity != 0 && sumSupplyCost != 0 && sumRevenue != 0 && sumOrderTotalPrice != 0)
                            {
                                finalTable.Add(cdRow.Field<string>("customerRegion")
                                    + ", " + pdRow.Field<string>("partMFGR")
                                    + ", " + sumTax
                                    + ", " + sumDiscount
                                    + ", " + sumQuantity
                                    + ", " + sumSupplyCost
                                    + ", " + sumRevenue
                                    + ", " + sumOrderTotalPrice);
                            }
                            break;
                    }
                }
            }

            sw.Stop();
            long t2 = sw.ElapsedMilliseconds;
            Console.WriteLine("[IMA] AGTest2 T1 Time: " + t2);
            Console.WriteLine(String.Format("[IMA] AGTest2 Total Time: {0}", t1 + t2));
            Console.WriteLine(String.Format("[IMA] AGTest2 Total Items: {0}", finalTable.Count));
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
            //System.IO.File.WriteAllLines(@"C:\Results\IMA.txt", finalTable);
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

                Dictionary<Int64, Int64> kvCustomerDim = new Dictionary<Int64, Int64>();
                DataTable tempTableCustomerDim = new DataTable();
                Dictionary<Int64, Int64> kvSupplierDim = new Dictionary<Int64, Int64>();
                DataTable tempTableSupplierDim = new DataTable();
                Dictionary<Int64, Int64> kvDateDim = new Dictionary<Int64, Int64>();
                DataTable tempTableDateDim = new DataTable();
                Dictionary<Int64, Int64> kvPartDim = new Dictionary<Int64, Int64>();
                DataTable tempTablePartDim = new DataTable();
                Int64 customerIndex = 0;
                Int64 dgKeyCustomer = 0;

                Int64 supplierIndex = 0;
                Int64 dgKeySupplier = 0;

                Int64 dateIndex = 0;
                Int64 dgKeyDate = 0;

                Int64 partIndex = 0;
                Int64 dgKeyPart = 0;

                sw.Start();
                switch (numberOfJoins)
                {

                    case 1:
                        tempTableCustomerDim.Columns.Add("customerRegion", typeof(string));
                        tempTableCustomerDim.Columns.Add("denseGroupingKey", typeof(Int64));

                        foreach (var customer in customerDimension)
                        {
                            string cRegion = customer.cRegion;
                            if (tempTableCustomerDim.Rows.Count > 0)
                            {
                                var tempTable = tempTableCustomerDim.Copy();
                                var found = false;
                                foreach (DataRow row in tempTableCustomerDim.Rows)
                                {
                                    var customerRegion = row.Field<string>("customerRegion");
                                    if (customerRegion.Equals(cRegion))
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
                                    tempTable.Rows.Add(cRegion, dgKeyCustomer);
                                    kvCustomerDim.Add(customerIndex + 1, dgKeyCustomer);
                                }
                                tempTableCustomerDim = tempTable;
                            }
                            else
                            {
                                dgKeyCustomer++;
                                tempTableCustomerDim.Rows.Add(cRegion, dgKeyCustomer);
                                kvCustomerDim.Add(customerIndex + 1, dgKeyCustomer);
                            }
                            customerIndex++;
                        }
                        break;
                    case 2:
                        tempTableCustomerDim.Columns.Add("customerRegion", typeof(string));
                        tempTableCustomerDim.Columns.Add("denseGroupingKey", typeof(Int64));

                        foreach (var customer in customerDimension)
                        {
                            string cRegion = customer.cRegion;
                            if (tempTableCustomerDim.Rows.Count > 0)
                            {
                                var tempTable = tempTableCustomerDim.Copy();
                                var found = false;
                                foreach (DataRow row in tempTableCustomerDim.Rows)
                                {
                                    var customerRegion = row.Field<string>("customerRegion");
                                    if (customerRegion.Equals(cRegion))
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
                                    tempTable.Rows.Add(cRegion, dgKeyCustomer);
                                    kvCustomerDim.Add(customerIndex + 1, dgKeyCustomer);
                                }
                                tempTableCustomerDim = tempTable;
                            }
                            else
                            {
                                dgKeyCustomer++;
                                tempTableCustomerDim.Rows.Add(cRegion, dgKeyCustomer);
                                kvCustomerDim.Add(customerIndex + 1, dgKeyCustomer);
                            }
                            customerIndex++;
                        }

                        tempTableSupplierDim.Columns.Add("supplierRegion", typeof(string));
                        tempTableSupplierDim.Columns.Add("denseGroupingKey", typeof(Int64));

                        foreach (var supplier in supplierDimension)
                        {
                            string sRegion = supplier.sRegion;
                            if (tempTableSupplierDim.Rows.Count > 0)
                            {
                                var tempTable = tempTableSupplierDim.Copy();
                                var found = false;
                                foreach (DataRow row in tempTableSupplierDim.Rows)
                                {
                                    var supplierRegion = row.Field<string>("supplierRegion");
                                    if (supplierRegion.Equals(sRegion))
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
                                    tempTable.Rows.Add(sRegion, dgKeySupplier);
                                    kvSupplierDim.Add(supplierIndex + 1, dgKeySupplier);
                                }
                                tempTableSupplierDim = tempTable;
                            }
                            else
                            {
                                dgKeySupplier++;
                                tempTableSupplierDim.Rows.Add(sRegion, dgKeySupplier);
                                kvSupplierDim.Add(supplierIndex + 1, dgKeySupplier);
                            }
                            supplierIndex++;
                        }
                        break;
                    case 3:
                        tempTableCustomerDim.Columns.Add("customerRegion", typeof(string));
                        tempTableCustomerDim.Columns.Add("denseGroupingKey", typeof(Int64));

                        foreach (var customer in customerDimension)
                        {
                            string cRegion = customer.cRegion;
                            if (tempTableCustomerDim.Rows.Count > 0)
                            {
                                var tempTable = tempTableCustomerDim.Copy();
                                var found = false;
                                foreach (DataRow row in tempTableCustomerDim.Rows)
                                {
                                    var customerRegion = row.Field<string>("customerRegion");
                                    if (customerRegion.Equals(cRegion))
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
                                    tempTable.Rows.Add(cRegion, dgKeyCustomer);
                                    kvCustomerDim.Add(customerIndex + 1, dgKeyCustomer);
                                }
                                tempTableCustomerDim = tempTable;
                            }
                            else
                            {
                                dgKeyCustomer++;
                                tempTableCustomerDim.Rows.Add(cRegion, dgKeyCustomer);
                                kvCustomerDim.Add(customerIndex + 1, dgKeyCustomer);
                            }
                            customerIndex++;
                        }

                        tempTableSupplierDim.Columns.Add("supplierRegion", typeof(string));
                        tempTableSupplierDim.Columns.Add("denseGroupingKey", typeof(Int64));

                        foreach (var supplier in supplierDimension)
                        {
                            string sRegion = supplier.sRegion;
                            if (tempTableSupplierDim.Rows.Count > 0)
                            {
                                var tempTable = tempTableSupplierDim.Copy();
                                var found = false;
                                foreach (DataRow row in tempTableSupplierDim.Rows)
                                {
                                    var supplierRegion = row.Field<string>("supplierRegion");
                                    if (supplierRegion.Equals(sRegion))
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
                                    tempTable.Rows.Add(sRegion, dgKeySupplier);
                                    kvSupplierDim.Add(supplierIndex + 1, dgKeySupplier);
                                }
                                tempTableSupplierDim = tempTable;
                            }
                            else
                            {
                                dgKeySupplier++;
                                tempTableSupplierDim.Rows.Add(sRegion, dgKeySupplier);
                                kvSupplierDim.Add(supplierIndex + 1, dgKeySupplier);
                            }
                            supplierIndex++;
                        }

                        tempTableDateDim.Columns.Add("dateYear", typeof(string));
                        tempTableDateDim.Columns.Add("denseGroupingKey", typeof(Int64));

                        foreach (var date in dateDimension)
                        {
                            string dYear = date.dYear;
                            if (tempTableDateDim.Rows.Count > 0)
                            {
                                var tempTable = tempTableDateDim.Copy();
                                var found = false;
                                foreach (DataRow row in tempTableDateDim.Rows)
                                {
                                    var dateYear = row.Field<string>("dateYear");
                                    if (dateYear.Equals(dYear))
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
                        break;
                    case 4:
                        tempTableCustomerDim.Columns.Add("customerRegion", typeof(string));
                        tempTableCustomerDim.Columns.Add("denseGroupingKey", typeof(Int64));

                        foreach (var customer in customerDimension)
                        {
                            string cRegion = customer.cRegion;
                            if (tempTableCustomerDim.Rows.Count > 0)
                            {
                                var tempTable = tempTableCustomerDim.Copy();
                                var found = false;
                                foreach (DataRow row in tempTableCustomerDim.Rows)
                                {
                                    var customerRegion = row.Field<string>("customerRegion");
                                    if (customerRegion.Equals(cRegion))
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
                                    tempTable.Rows.Add(cRegion, dgKeyCustomer);
                                    kvCustomerDim.Add(customerIndex + 1, dgKeyCustomer);
                                }
                                tempTableCustomerDim = tempTable;
                            }
                            else
                            {
                                dgKeyCustomer++;
                                tempTableCustomerDim.Rows.Add(cRegion, dgKeyCustomer);
                                kvCustomerDim.Add(customerIndex + 1, dgKeyCustomer);
                            }
                            customerIndex++;
                        }

                        tempTableSupplierDim.Columns.Add("supplierRegion", typeof(string));
                        tempTableSupplierDim.Columns.Add("denseGroupingKey", typeof(Int64));

                        foreach (var supplier in supplierDimension)
                        {
                            string sRegion = supplier.sRegion;
                            if (tempTableSupplierDim.Rows.Count > 0)
                            {
                                var tempTable = tempTableSupplierDim.Copy();
                                var found = false;
                                foreach (DataRow row in tempTableSupplierDim.Rows)
                                {
                                    var supplierRegion = row.Field<string>("supplierRegion");
                                    if (supplierRegion.Equals(sRegion))
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
                                    tempTable.Rows.Add(sRegion, dgKeySupplier);
                                    kvSupplierDim.Add(supplierIndex + 1, dgKeySupplier);
                                }
                                tempTableSupplierDim = tempTable;
                            }
                            else
                            {
                                dgKeySupplier++;
                                tempTableSupplierDim.Rows.Add(sRegion, dgKeySupplier);
                                kvSupplierDim.Add(supplierIndex + 1, dgKeySupplier);
                            }
                            supplierIndex++;
                        }

                        tempTableDateDim.Columns.Add("dateYear", typeof(string));
                        tempTableDateDim.Columns.Add("denseGroupingKey", typeof(Int64));

                        foreach (var date in dateDimension)
                        {
                            string dYear = date.dYear;
                            if (tempTableDateDim.Rows.Count > 0)
                            {
                                var tempTable = tempTableDateDim.Copy();
                                var found = false;
                                foreach (DataRow row in tempTableDateDim.Rows)
                                {
                                    var dateYear = row.Field<string>("dateYear");
                                    if (dateYear.Equals(dYear))
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

                        tempTablePartDim.Columns.Add("partMFGR", typeof(string));
                        tempTablePartDim.Columns.Add("denseGroupingKey", typeof(Int64));

                        foreach (var part in partDimension)
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
                            partIndex++;
                        }
                        break;
                }

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[IMA Join] JSTest T0 Time: {0}", t0));
                #endregion Key Hashing Phase

                #region Probing Phase
                sw.Reset();
                sw.Start();
                Int64 dgkLengthCustomer = 1;
                Int64 dgkLengthSupplier = 1;
                Int64 dgkLengthDate = 1;
                Int64 dgkLengthPart = 1;
                Int64[,,,] inMemoryAccumulatorTax = new Int64[dgkLengthCustomer, dgkLengthSupplier, dgkLengthDate, dgkLengthPart];
                List<string> finalTable = new List<string>();

                switch (numberOfJoins)
                {
                    case 1:
                        dgkLengthCustomer = tempTableCustomerDim.Rows.Count + 1;
                        inMemoryAccumulatorTax = new Int64[dgkLengthCustomer, dgkLengthSupplier, dgkLengthDate, dgkLengthPart];
                        for (Int64 i = 0; i < loCustomerKey.Count(); i++)
                        {
                            Int64 custKey = loCustomerKey[i];
                            Int64 dgkCustomerDim = 0;
                            if (kvCustomerDim.TryGetValue(custKey, out dgkCustomerDim))
                            {
                                if (dgkCustomerDim == 0)
                                {
                                    // skip
                                }
                                else
                                {
                                    inMemoryAccumulatorTax[dgkCustomerDim, 0, 0, 0] += loTax[i];
                                }
                            }
                        }

                        foreach (DataRow cdRow in tempTableCustomerDim.Rows)
                        {
                            Int64 cdKey = cdRow.Field<Int64>("denseGroupingKey");
                            Int64 sumTax = inMemoryAccumulatorTax[cdKey, 0, 0, 0];
                            if (sumTax != 0)
                            {
                                finalTable.Add(cdRow.Field<string>("customerRegion")
                                    + ", " + sumTax);
                            }
                        }
                        break;
                    case 2:
                        dgkLengthCustomer = tempTableCustomerDim.Rows.Count + 1;
                        dgkLengthSupplier = tempTableSupplierDim.Rows.Count + 1;
                        inMemoryAccumulatorTax = new Int64[dgkLengthCustomer, dgkLengthSupplier, dgkLengthDate, dgkLengthPart];
                        for (Int64 i = 0; i < loCustomerKey.Count(); i++)
                        {
                            Int64 custKey = loCustomerKey[i];
                            Int64 suppKey = loSupplierKey[i];
                            Int64 dgkCustomerDim = 0;
                            Int64 dgkSupplierDim = 0;
                            if (kvCustomerDim.TryGetValue(custKey, out dgkCustomerDim)
                                && kvSupplierDim.TryGetValue(suppKey, out dgkSupplierDim))
                            {
                                if (dgkCustomerDim == 0 || dgkSupplierDim == 0)
                                {
                                    // skip
                                }
                                else
                                {
                                    inMemoryAccumulatorTax[dgkCustomerDim, dgkSupplierDim, 0, 0] += loTax[i];
                                }
                            }
                        }

                        foreach (DataRow cdRow in tempTableCustomerDim.Rows)
                        {
                            foreach (DataRow sdRow in tempTableSupplierDim.Rows)
                            {
                                Int64 cdKey = cdRow.Field<Int64>("denseGroupingKey");
                                Int64 sdKey = sdRow.Field<Int64>("denseGroupingKey");
                                Int64 sumTax = inMemoryAccumulatorTax[cdKey, sdKey, 0, 0];
                                if (sumTax != 0)
                                {
                                    finalTable.Add(cdRow.Field<string>("customerRegion")
                                        + ", " + sdRow.Field<string>("supplierRegion")
                                        + ", " + sumTax);
                                }
                            }
                        }
                        break;
                    case 3:
                        dgkLengthCustomer = tempTableCustomerDim.Rows.Count + 1;
                        dgkLengthSupplier = tempTableSupplierDim.Rows.Count + 1;
                        dgkLengthDate = tempTableDateDim.Rows.Count + 1;
                        inMemoryAccumulatorTax = new Int64[dgkLengthCustomer, dgkLengthSupplier, dgkLengthDate, dgkLengthPart];
                        for (Int64 i = 0; i < loCustomerKey.Count(); i++)
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
                                    inMemoryAccumulatorTax[dgkCustomerDim, dgkSupplierDim, dgkDateDim, 0] += loTax[i];
                                }
                            }
                        }

                        foreach (DataRow cdRow in tempTableCustomerDim.Rows)
                        {
                            foreach (DataRow sdRow in tempTableSupplierDim.Rows)
                            {
                                foreach (DataRow ddRow in tempTableDateDim.Rows)
                                {
                                    Int64 cdKey = cdRow.Field<Int64>("denseGroupingKey");
                                    Int64 sdKey = sdRow.Field<Int64>("denseGroupingKey");
                                    Int64 ddKey = ddRow.Field<Int64>("denseGroupingKey");
                                    Int64 sumTax = inMemoryAccumulatorTax[cdKey, sdKey, ddKey, 0];
                                    if (sumTax != 0)
                                    {
                                        finalTable.Add(cdRow.Field<string>("customerRegion")
                                            + ", " + sdRow.Field<string>("supplierRegion")
                                            + ", " + ddRow.Field<string>("dateYear")
                                            + ", " + sumTax);
                                    }
                                }
                            }
                        }
                        break;
                    case 4:
                        dgkLengthCustomer = tempTableCustomerDim.Rows.Count + 1;
                        dgkLengthSupplier = tempTableSupplierDim.Rows.Count + 1;
                        dgkLengthDate = tempTableDateDim.Rows.Count + 1;
                        dgkLengthPart = tempTablePartDim.Rows.Count + 1;
                        inMemoryAccumulatorTax = new Int64[dgkLengthCustomer, dgkLengthSupplier, dgkLengthDate, dgkLengthPart];
                        for (Int64 i = 0; i < loCustomerKey.Count(); i++)
                        {
                            Int64 custKey = loCustomerKey[i];
                            Int64 suppKey = loSupplierKey[i];
                            Int64 dateKey = loOrderDate[i];
                            Int64 partKey = loPartKey[i];
                            Int64 dgkCustomerDim = 0;
                            Int64 dgkSupplierDim = 0;
                            Int64 dgkDateDim = 0;
                            Int64 dgkPartDim = 0;
                            if (kvCustomerDim.TryGetValue(custKey, out dgkCustomerDim)
                                && kvSupplierDim.TryGetValue(suppKey, out dgkSupplierDim)
                                && kvDateDim.TryGetValue(dateKey, out dgkDateDim)
                                && kvPartDim.TryGetValue(partKey, out dgkPartDim))
                            {
                                if (dgkCustomerDim == 0 || dgkSupplierDim == 0 || dgkDateDim == 0 || dgkPartDim == 0)
                                {
                                    // skip
                                }
                                else
                                {
                                    inMemoryAccumulatorTax[dgkCustomerDim, dgkSupplierDim, dgkDateDim, dgkPartDim] += loTax[i];
                                }
                            }
                        }

                        foreach (DataRow cdRow in tempTableCustomerDim.Rows)
                        {
                            foreach (DataRow sdRow in tempTableSupplierDim.Rows)
                            {
                                foreach (DataRow ddRow in tempTableDateDim.Rows)
                                {
                                    foreach (DataRow pdRow in tempTablePartDim.Rows)
                                    {
                                        Int64 cdKey = cdRow.Field<Int64>("denseGroupingKey");
                                        Int64 sdKey = sdRow.Field<Int64>("denseGroupingKey");
                                        Int64 ddKey = ddRow.Field<Int64>("denseGroupingKey");
                                        Int64 pdKey = pdRow.Field<Int64>("denseGroupingKey");
                                        Int64 sumTax = inMemoryAccumulatorTax[cdKey, sdKey, ddKey, pdKey];
                                        //if (sumTax != 0)
                                        {
                                            finalTable.Add(cdRow.Field<string>("customerRegion")
                                                + ", " + sdRow.Field<string>("supplierRegion")
                                                + ", " + ddRow.Field<string>("dateYear")
                                                + ", " + pdRow.Field<string>("partMFGR")
                                                + ", " + sumTax);
                                        }
                                    }
                                }
                            }
                        }
                        break;
                }

                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[IMO Join] JSTest T1 Time: {0}", t1));
                sw.Reset();
                #endregion Probing Phase

                Console.WriteLine(String.Format("[IMO Join] JSTest Total Time: {0}", t0 + t1));
                Console.WriteLine(String.Format("[IMO Join] JSTest Total : {0}", finalTable.Count));
                Console.WriteLine();
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


                #region Key Hashing Phase 
                sw.Start();
                Dictionary<Int64, Int64> kvCustomerRegionDim = new Dictionary<Int64, Int64>();
                DataTable tempTableCustomerRegionDim = new DataTable();
                Dictionary<Int64, Int64> kvSupplierRegionDim = new Dictionary<Int64, Int64>();
                DataTable tempTableSupplierRegionDim = new DataTable();
                Dictionary<Int64, Int64> kvDateYearDim = new Dictionary<Int64, Int64>();
                DataTable tempTableDateYearDim = new DataTable();

                Dictionary<Int64, Int64> kvCustomerNationDim = new Dictionary<Int64, Int64>();
                DataTable tempTableCustomerNationDim = new DataTable();
                Dictionary<Int64, Int64> kvSupplierNationDim = new Dictionary<Int64, Int64>();
                DataTable tempTableSupplierNationDim = new DataTable();
                Dictionary<Int64, Int64> kvDateMonthDim = new Dictionary<Int64, Int64>();
                DataTable tempTableDateMonthDim = new DataTable();

                Int64 customerRegionIndex = 0;
                Int64 dgKeyCustomerRegion = 0;

                Int64 supplierRegionIndex = 0;
                Int64 dgKeySupplierRegion = 0;

                Int64 dateYearIndex = 0;
                Int64 dgKeyDateYear = 0;

                Int64 customerNationIndex = 0;
                Int64 dgKeyCustomerNation = 0;

                Int64 supplierNationIndex = 0;
                Int64 dgKeySupplierNation = 0;

                Int64 dateMonthIndex = 0;
                Int64 dgKeyDateMonth = 0;

                tempTableCustomerRegionDim.Columns.Add("customerRegion", typeof(string));
                tempTableCustomerRegionDim.Columns.Add("denseGroupingKey", typeof(Int64));

                tempTableCustomerNationDim.Columns.Add("customerNation", typeof(string));
                tempTableCustomerNationDim.Columns.Add("denseGroupingKey", typeof(Int64));

                tempTableSupplierRegionDim.Columns.Add("supplierRegion", typeof(string));
                tempTableSupplierRegionDim.Columns.Add("denseGroupingKey", typeof(Int64));

                tempTableSupplierNationDim.Columns.Add("supplierNation", typeof(string));
                tempTableSupplierNationDim.Columns.Add("denseGroupingKey", typeof(Int64));

                tempTableDateYearDim.Columns.Add("dateYear", typeof(string));
                tempTableDateYearDim.Columns.Add("denseGroupingKey", typeof(Int64));

                tempTableDateMonthDim.Columns.Add("dateMonth", typeof(string));
                tempTableDateMonthDim.Columns.Add("denseGroupingKey", typeof(Int64));



                switch (numberOfGroupingAttributes)
                {
                    case 1:
                        foreach (var customer in customerDimension)
                        {
                            if (customer.cRegion.Equals("ASIA"))
                            {
                                string cNation = customer.cNation;
                                if (tempTableCustomerNationDim.Rows.Count > 0)
                                {
                                    var tempTable = tempTableCustomerNationDim.Copy();
                                    var found = false;
                                    foreach (DataRow row in tempTableCustomerNationDim.Rows)
                                    {
                                        var customerNation = row.Field<string>("customerNation");
                                        if (customerNation.Equals(cNation))
                                        {
                                            Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                            kvCustomerNationDim.Add(customerNationIndex + 1, dgKey);
                                            found = true;
                                            break;
                                        }
                                    }
                                    if (!found)
                                    {
                                        dgKeyCustomerNation++;
                                        tempTable.Rows.Add(cNation, dgKeyCustomerNation);
                                        kvCustomerNationDim.Add(customerNationIndex + 1, dgKeyCustomerNation);
                                    }
                                    tempTableCustomerNationDim = tempTable;
                                }
                                else
                                {
                                    dgKeyCustomerNation++;
                                    tempTableCustomerNationDim.Rows.Add(cNation, dgKeyCustomerNation);
                                    kvCustomerNationDim.Add(customerNationIndex + 1, dgKeyCustomerNation);
                                }
                            }
                            else
                            {
                                kvCustomerNationDim.Add(customerNationIndex + 1, 0);
                            }
                            customerNationIndex++;
                        }
                        break;
                    case 2:
                        foreach (var customer in customerDimension)
                        {
                            if (customer.cRegion.Equals("ASIA"))
                            {
                                string cRegion = customer.cRegion;
                                if (tempTableCustomerRegionDim.Rows.Count > 0)
                                {
                                    var tempTable = tempTableCustomerRegionDim.Copy();
                                    var found = false;
                                    foreach (DataRow row in tempTableCustomerRegionDim.Rows)
                                    {
                                        var customerRegion = row.Field<string>("customerRegion");
                                        if (customerRegion.Equals(cRegion))
                                        {
                                            Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                            kvCustomerRegionDim.Add(customerRegionIndex + 1, dgKey);
                                            found = true;
                                            break;
                                        }
                                    }
                                    if (!found)
                                    {
                                        dgKeyCustomerRegion++;
                                        tempTable.Rows.Add(cRegion, dgKeyCustomerRegion);
                                        kvCustomerRegionDim.Add(customerRegionIndex + 1, dgKeyCustomerRegion);
                                    }
                                    tempTableCustomerRegionDim = tempTable;
                                }
                                else
                                {
                                    dgKeyCustomerRegion++;
                                    tempTableCustomerRegionDim.Rows.Add(cRegion, dgKeyCustomerRegion);
                                    kvCustomerRegionDim.Add(customerRegionIndex + 1, dgKeyCustomerRegion);
                                }

                                string cNation = customer.cNation;
                                if (tempTableCustomerNationDim.Rows.Count > 0)
                                {
                                    var tempTable = tempTableCustomerNationDim.Copy();
                                    var found = false;
                                    foreach (DataRow row in tempTableCustomerNationDim.Rows)
                                    {
                                        var customerNation = row.Field<string>("customerNation");
                                        if (customerNation.Equals(cNation))
                                        {
                                            Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                            kvCustomerNationDim.Add(customerNationIndex + 1, dgKey);
                                            found = true;
                                            break;
                                        }
                                    }
                                    if (!found)
                                    {
                                        dgKeyCustomerNation++;
                                        tempTable.Rows.Add(cNation, dgKeyCustomerNation);
                                        kvCustomerNationDim.Add(customerNationIndex + 1, dgKeyCustomerNation);
                                    }
                                    tempTableCustomerNationDim = tempTable;
                                }
                                else
                                {
                                    dgKeyCustomerNation++;
                                    tempTableCustomerNationDim.Rows.Add(cNation, dgKeyCustomerNation);
                                    kvCustomerNationDim.Add(customerNationIndex + 1, dgKeyCustomerNation);
                                }
                            }
                            else
                            {
                                kvCustomerNationDim.Add(customerNationIndex + 1, 0);
                                kvCustomerRegionDim.Add(customerRegionIndex + 1, 0);
                            }
                            customerRegionIndex++;
                            customerNationIndex++;


                        }
                        break;
                    case 3:
                        foreach (var customer in customerDimension)
                        {
                            if (customer.cRegion.Equals("ASIA"))
                            {
                                string cRegion = customer.cRegion;
                                if (tempTableCustomerRegionDim.Rows.Count > 0)
                                {
                                    var tempTable = tempTableCustomerRegionDim.Copy();
                                    var found = false;
                                    foreach (DataRow row in tempTableCustomerRegionDim.Rows)
                                    {
                                        var customerRegion = row.Field<string>("customerRegion");
                                        if (customerRegion.Equals(cRegion))
                                        {
                                            Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                            kvCustomerRegionDim.Add(customerRegionIndex + 1, dgKey);
                                            found = true;
                                            break;
                                        }
                                    }
                                    if (!found)
                                    {
                                        dgKeyCustomerRegion++;
                                        tempTable.Rows.Add(cRegion, dgKeyCustomerRegion);
                                        kvCustomerRegionDim.Add(customerRegionIndex + 1, dgKeyCustomerRegion);
                                    }
                                    tempTableCustomerRegionDim = tempTable;
                                }
                                else
                                {
                                    dgKeyCustomerRegion++;
                                    tempTableCustomerRegionDim.Rows.Add(cRegion, dgKeyCustomerRegion);
                                    kvCustomerRegionDim.Add(customerRegionIndex + 1, dgKeyCustomerRegion);
                                }

                                string cNation = customer.cNation;
                                if (tempTableCustomerNationDim.Rows.Count > 0)
                                {
                                    var tempTable = tempTableCustomerNationDim.Copy();
                                    var found = false;
                                    foreach (DataRow row in tempTableCustomerNationDim.Rows)
                                    {
                                        var customerNation = row.Field<string>("customerNation");
                                        if (customerNation.Equals(cNation))
                                        {
                                            Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                            kvCustomerNationDim.Add(customerNationIndex + 1, dgKey);
                                            found = true;
                                            break;
                                        }
                                    }
                                    if (!found)
                                    {
                                        dgKeyCustomerNation++;
                                        tempTable.Rows.Add(cNation, dgKeyCustomerNation);
                                        kvCustomerNationDim.Add(customerNationIndex + 1, dgKeyCustomerNation);
                                    }
                                    tempTableCustomerNationDim = tempTable;
                                }
                                else
                                {
                                    dgKeyCustomerNation++;
                                    tempTableCustomerNationDim.Rows.Add(cNation, dgKeyCustomerNation);
                                    kvCustomerNationDim.Add(customerNationIndex + 1, dgKeyCustomerNation);
                                }
                            }
                            else
                            {
                                kvCustomerNationDim.Add(customerNationIndex + 1, 0);
                                kvCustomerRegionDim.Add(customerRegionIndex + 1, 0);
                            }
                            customerRegionIndex++;
                            customerNationIndex++;
                        }

                        foreach (var supplier in supplierDimension)
                        {
                            if (supplier.sRegion.Equals("ASIA"))
                            {
                                string sNation = supplier.sNation;
                                if (tempTableSupplierNationDim.Rows.Count > 0)
                                {
                                    var tempTable = tempTableSupplierNationDim.Copy();
                                    var found = false;
                                    foreach (DataRow row in tempTableSupplierNationDim.Rows)
                                    {
                                        var supplierNation = row.Field<string>("supplierNation");
                                        if (supplierNation.Equals(sNation))
                                        {
                                            Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                            kvSupplierNationDim.Add(supplierNationIndex + 1, dgKey);
                                            found = true;
                                            break;
                                        }
                                    }
                                    if (!found)
                                    {
                                        dgKeySupplierNation++;
                                        tempTable.Rows.Add(sNation, dgKeySupplierNation);
                                        kvSupplierNationDim.Add(supplierNationIndex + 1, dgKeySupplierNation);
                                    }
                                    tempTableSupplierNationDim = tempTable;
                                }
                                else
                                {
                                    dgKeySupplierNation++;
                                    tempTableSupplierNationDim.Rows.Add(sNation, dgKeySupplierNation);
                                    kvSupplierNationDim.Add(supplierNationIndex + 1, dgKeySupplierNation);
                                }
                            }
                            else
                            {
                                kvSupplierNationDim.Add(supplierNationIndex + 1, 0);
                            }
                            supplierNationIndex++;
                        }
                        break;
                    case 4:
                        foreach (var customer in customerDimension)
                        {
                            if (customer.cRegion.Equals("ASIA"))
                            {
                                string cRegion = customer.cRegion;
                                if (tempTableCustomerRegionDim.Rows.Count > 0)
                                {
                                    var tempTable = tempTableCustomerRegionDim.Copy();
                                    var found = false;
                                    foreach (DataRow row in tempTableCustomerRegionDim.Rows)
                                    {
                                        var customerRegion = row.Field<string>("customerRegion");
                                        if (customerRegion.Equals(cRegion))
                                        {
                                            Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                            kvCustomerRegionDim.Add(customerRegionIndex + 1, dgKey);
                                            found = true;
                                            break;
                                        }
                                    }
                                    if (!found)
                                    {
                                        dgKeyCustomerRegion++;
                                        tempTable.Rows.Add(cRegion, dgKeyCustomerRegion);
                                        kvCustomerRegionDim.Add(customerRegionIndex + 1, dgKeyCustomerRegion);
                                    }
                                    tempTableCustomerRegionDim = tempTable;
                                }
                                else
                                {
                                    dgKeyCustomerRegion++;
                                    tempTableCustomerRegionDim.Rows.Add(cRegion, dgKeyCustomerRegion);
                                    kvCustomerRegionDim.Add(customerRegionIndex + 1, dgKeyCustomerRegion);
                                }

                                string cNation = customer.cNation;
                                if (tempTableCustomerNationDim.Rows.Count > 0)
                                {
                                    var tempTable = tempTableCustomerNationDim.Copy();
                                    var found = false;
                                    foreach (DataRow row in tempTableCustomerNationDim.Rows)
                                    {
                                        var customerNation = row.Field<string>("customerNation");
                                        if (customerNation.Equals(cNation))
                                        {
                                            Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                            kvCustomerNationDim.Add(customerNationIndex + 1, dgKey);
                                            found = true;
                                            break;
                                        }
                                    }
                                    if (!found)
                                    {
                                        dgKeyCustomerNation++;
                                        tempTable.Rows.Add(cNation, dgKeyCustomerNation);
                                        kvCustomerNationDim.Add(customerNationIndex + 1, dgKeyCustomerNation);
                                    }
                                    tempTableCustomerNationDim = tempTable;
                                }
                                else
                                {
                                    dgKeyCustomerNation++;
                                    tempTableCustomerNationDim.Rows.Add(cNation, dgKeyCustomerNation);
                                    kvCustomerNationDim.Add(customerNationIndex + 1, dgKeyCustomerNation);
                                }
                            }
                            else
                            {
                                kvCustomerNationDim.Add(customerNationIndex + 1, 0);
                                kvCustomerRegionDim.Add(customerRegionIndex + 1, 0);
                            }
                            customerRegionIndex++;
                            customerNationIndex++;
                        }

                        foreach (var supplier in supplierDimension)
                        {
                            if (supplier.sRegion.Equals("ASIA"))
                            {
                                string sNation = supplier.sNation;
                                if (tempTableSupplierNationDim.Rows.Count > 0)
                                {
                                    var tempTable = tempTableSupplierNationDim.Copy();
                                    var found = false;
                                    foreach (DataRow row in tempTableSupplierNationDim.Rows)
                                    {
                                        var supplierNation = row.Field<string>("supplierNation");
                                        if (supplierNation.Equals(sNation))
                                        {
                                            Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                            kvSupplierNationDim.Add(supplierNationIndex + 1, dgKey);
                                            found = true;
                                            break;
                                        }
                                    }
                                    if (!found)
                                    {
                                        dgKeySupplierNation++;
                                        tempTable.Rows.Add(sNation, dgKeySupplierNation);
                                        kvSupplierNationDim.Add(supplierNationIndex + 1, dgKeySupplierNation);
                                    }
                                    tempTableSupplierNationDim = tempTable;
                                }
                                else
                                {
                                    dgKeySupplierNation++;
                                    tempTableSupplierNationDim.Rows.Add(sNation, dgKeySupplierNation);
                                    kvSupplierNationDim.Add(supplierNationIndex + 1, dgKeySupplierNation);
                                }

                                string sRegion = supplier.sRegion;
                                if (tempTableSupplierRegionDim.Rows.Count > 0)
                                {
                                    var tempTable = tempTableSupplierRegionDim.Copy();
                                    var found = false;
                                    foreach (DataRow row in tempTableSupplierRegionDim.Rows)
                                    {
                                        var supplierRegion = row.Field<string>("supplierRegion");
                                        if (supplierRegion.Equals(sRegion))
                                        {
                                            Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                            kvSupplierRegionDim.Add(supplierRegionIndex + 1, dgKey);
                                            found = true;
                                            break;
                                        }
                                    }
                                    if (!found)
                                    {
                                        dgKeySupplierRegion++;
                                        tempTable.Rows.Add(sRegion, dgKeySupplierRegion);
                                        kvSupplierRegionDim.Add(supplierRegionIndex + 1, dgKeySupplierRegion);
                                    }
                                    tempTableSupplierRegionDim = tempTable;
                                }
                                else
                                {
                                    dgKeySupplierRegion++;
                                    tempTableSupplierRegionDim.Rows.Add(sRegion, dgKeySupplierRegion);
                                    kvSupplierRegionDim.Add(supplierRegionIndex + 1, dgKeySupplierRegion);
                                }
                            }
                            else
                            {
                                kvSupplierNationDim.Add(supplierNationIndex + 1, 0);
                                kvSupplierRegionDim.Add(supplierRegionIndex + 1, 0);
                            }
                            supplierNationIndex++;
                            supplierRegionIndex++;
                        }
                        break;
                    case 5:
                        foreach (var customer in customerDimension)
                        {
                            if (customer.cRegion.Equals("ASIA"))
                            {
                                string cRegion = customer.cRegion;
                                if (tempTableCustomerRegionDim.Rows.Count > 0)
                                {
                                    var tempTable = tempTableCustomerRegionDim.Copy();
                                    var found = false;
                                    foreach (DataRow row in tempTableCustomerRegionDim.Rows)
                                    {
                                        var customerRegion = row.Field<string>("customerRegion");
                                        if (customerRegion.Equals(cRegion))
                                        {
                                            Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                            kvCustomerRegionDim.Add(customerRegionIndex + 1, dgKey);
                                            found = true;
                                            break;
                                        }
                                    }
                                    if (!found)
                                    {
                                        dgKeyCustomerRegion++;
                                        tempTable.Rows.Add(cRegion, dgKeyCustomerRegion);
                                        kvCustomerRegionDim.Add(customerRegionIndex + 1, dgKeyCustomerRegion);
                                    }
                                    tempTableCustomerRegionDim = tempTable;
                                }
                                else
                                {
                                    dgKeyCustomerRegion++;
                                    tempTableCustomerRegionDim.Rows.Add(cRegion, dgKeyCustomerRegion);
                                    kvCustomerRegionDim.Add(customerRegionIndex + 1, dgKeyCustomerRegion);
                                }

                                string cNation = customer.cNation;
                                if (tempTableCustomerNationDim.Rows.Count > 0)
                                {
                                    var tempTable = tempTableCustomerNationDim.Copy();
                                    var found = false;
                                    foreach (DataRow row in tempTableCustomerNationDim.Rows)
                                    {
                                        var customerNation = row.Field<string>("customerNation");
                                        if (customerNation.Equals(cNation))
                                        {
                                            Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                            kvCustomerNationDim.Add(customerNationIndex + 1, dgKey);
                                            found = true;
                                            break;
                                        }
                                    }
                                    if (!found)
                                    {
                                        dgKeyCustomerNation++;
                                        tempTable.Rows.Add(cNation, dgKeyCustomerNation);
                                        kvCustomerNationDim.Add(customerNationIndex + 1, dgKeyCustomerNation);
                                    }
                                    tempTableCustomerNationDim = tempTable;
                                }
                                else
                                {
                                    dgKeyCustomerNation++;
                                    tempTableCustomerNationDim.Rows.Add(cNation, dgKeyCustomerNation);
                                    kvCustomerNationDim.Add(customerNationIndex + 1, dgKeyCustomerNation);
                                }
                            }
                            else
                            {
                                kvCustomerNationDim.Add(customerNationIndex + 1, 0);
                                kvCustomerRegionDim.Add(customerRegionIndex + 1, 0);
                            }
                            customerRegionIndex++;
                            customerNationIndex++;
                        }

                        foreach (var supplier in supplierDimension)
                        {
                            if (supplier.sRegion.Equals("ASIA"))
                            {
                                string sNation = supplier.sNation;
                                if (tempTableSupplierNationDim.Rows.Count > 0)
                                {
                                    var tempTable = tempTableSupplierNationDim.Copy();
                                    var found = false;
                                    foreach (DataRow row in tempTableSupplierNationDim.Rows)
                                    {
                                        var supplierNation = row.Field<string>("supplierNation");
                                        if (supplierNation.Equals(sNation))
                                        {
                                            Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                            kvSupplierNationDim.Add(supplierNationIndex + 1, dgKey);
                                            found = true;
                                            break;
                                        }
                                    }
                                    if (!found)
                                    {
                                        dgKeySupplierNation++;
                                        tempTable.Rows.Add(sNation, dgKeySupplierNation);
                                        kvSupplierNationDim.Add(supplierNationIndex + 1, dgKeySupplierNation);
                                    }
                                    tempTableSupplierNationDim = tempTable;
                                }
                                else
                                {
                                    dgKeySupplierNation++;
                                    tempTableSupplierNationDim.Rows.Add(sNation, dgKeySupplierNation);
                                    kvSupplierNationDim.Add(supplierNationIndex + 1, dgKeySupplierNation);
                                }

                                string sRegion = supplier.sRegion;
                                if (tempTableSupplierRegionDim.Rows.Count > 0)
                                {
                                    var tempTable = tempTableSupplierRegionDim.Copy();
                                    var found = false;
                                    foreach (DataRow row in tempTableSupplierRegionDim.Rows)
                                    {
                                        var supplierRegion = row.Field<string>("supplierRegion");
                                        if (supplierRegion.Equals(sRegion))
                                        {
                                            Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                            kvSupplierRegionDim.Add(supplierRegionIndex + 1, dgKey);
                                            found = true;
                                            break;
                                        }
                                    }
                                    if (!found)
                                    {
                                        dgKeySupplierRegion++;
                                        tempTable.Rows.Add(sRegion, dgKeySupplierRegion);
                                        kvSupplierRegionDim.Add(supplierRegionIndex + 1, dgKeySupplierRegion);
                                    }
                                    tempTableSupplierRegionDim = tempTable;
                                }
                                else
                                {
                                    dgKeySupplierRegion++;
                                    tempTableSupplierRegionDim.Rows.Add(sRegion, dgKeySupplierRegion);
                                    kvSupplierRegionDim.Add(supplierRegionIndex + 1, dgKeySupplierRegion);
                                }
                            }
                            else
                            {
                                kvSupplierNationDim.Add(supplierNationIndex + 1, 0);
                                kvSupplierRegionDim.Add(supplierRegionIndex + 1, 0);
                            }
                            supplierNationIndex++;
                            supplierRegionIndex++;
                        }

                        foreach (var date in dateDimension)
                        {
                            if (date.dYear.CompareTo("1992") >= 0 && date.dYear.CompareTo("1997") <= 0)
                            {
                                string dYear = date.dYear;
                                if (tempTableDateYearDim.Rows.Count > 0)
                                {
                                    var tempTable = tempTableDateYearDim.Copy();
                                    var found = false;
                                    foreach (DataRow row in tempTableDateYearDim.Rows)
                                    {
                                        var dateYear = row.Field<string>("dateYear");
                                        if (dateYear.Equals(dYear))
                                        {
                                            Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                            kvDateYearDim.Add(date.dDateKey, dgKey);
                                            found = true;
                                            break;
                                        }
                                    }
                                    if (!found)
                                    {
                                        dgKeyDateYear++;
                                        tempTable.Rows.Add(dYear, dgKeyDateYear);
                                        kvDateYearDim.Add(date.dDateKey, dgKeyDateYear);
                                    }
                                    tempTableDateYearDim = tempTable;
                                }
                                else
                                {
                                    dgKeyDateYear++;
                                    tempTableDateYearDim.Rows.Add(dYear, dgKeyDateYear);
                                    kvDateYearDim.Add(date.dDateKey, dgKeyDateYear);
                                }
                            }
                            else
                            {
                                kvDateYearDim.Add(date.dDateKey, 0);
                            }
                        }
                        break;
                    case 6:
                        foreach (var customer in customerDimension)
                        {
                            if (customer.cRegion.Equals("ASIA"))
                            {
                                string cRegion = customer.cRegion;
                                if (tempTableCustomerRegionDim.Rows.Count > 0)
                                {
                                    var tempTable = tempTableCustomerRegionDim.Copy();
                                    var found = false;
                                    foreach (DataRow row in tempTableCustomerRegionDim.Rows)
                                    {
                                        var customerRegion = row.Field<string>("customerRegion");
                                        if (customerRegion.Equals(cRegion))
                                        {
                                            Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                            kvCustomerRegionDim.Add(customerRegionIndex + 1, dgKey);
                                            found = true;
                                            break;
                                        }
                                    }
                                    if (!found)
                                    {
                                        dgKeyCustomerRegion++;
                                        tempTable.Rows.Add(cRegion, dgKeyCustomerRegion);
                                        kvCustomerRegionDim.Add(customerRegionIndex + 1, dgKeyCustomerRegion);
                                    }
                                    tempTableCustomerRegionDim = tempTable;
                                }
                                else
                                {
                                    dgKeyCustomerRegion++;
                                    tempTableCustomerRegionDim.Rows.Add(cRegion, dgKeyCustomerRegion);
                                    kvCustomerRegionDim.Add(customerRegionIndex + 1, dgKeyCustomerRegion);
                                }

                                string cNation = customer.cNation;
                                if (tempTableCustomerNationDim.Rows.Count > 0)
                                {
                                    var tempTable = tempTableCustomerNationDim.Copy();
                                    var found = false;
                                    foreach (DataRow row in tempTableCustomerNationDim.Rows)
                                    {
                                        var customerNation = row.Field<string>("customerNation");
                                        if (customerNation.Equals(cNation))
                                        {
                                            Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                            kvCustomerNationDim.Add(customerNationIndex + 1, dgKey);
                                            found = true;
                                            break;
                                        }
                                    }
                                    if (!found)
                                    {
                                        dgKeyCustomerNation++;
                                        tempTable.Rows.Add(cNation, dgKeyCustomerNation);
                                        kvCustomerNationDim.Add(customerNationIndex + 1, dgKeyCustomerNation);
                                    }
                                    tempTableCustomerNationDim = tempTable;
                                }
                                else
                                {
                                    dgKeyCustomerNation++;
                                    tempTableCustomerNationDim.Rows.Add(cNation, dgKeyCustomerNation);
                                    kvCustomerNationDim.Add(customerNationIndex + 1, dgKeyCustomerNation);
                                }
                            }
                            else
                            {
                                kvCustomerNationDim.Add(customerNationIndex + 1, 0);
                                kvCustomerRegionDim.Add(customerRegionIndex + 1, 0);
                            }
                            customerRegionIndex++;
                            customerNationIndex++;
                        }

                        foreach (var supplier in supplierDimension)
                        {
                            if (supplier.sRegion.Equals("ASIA"))
                            {
                                string sNation = supplier.sNation;
                                if (tempTableSupplierNationDim.Rows.Count > 0)
                                {
                                    var tempTable = tempTableSupplierNationDim.Copy();
                                    var found = false;
                                    foreach (DataRow row in tempTableSupplierNationDim.Rows)
                                    {
                                        var supplierNation = row.Field<string>("supplierNation");
                                        if (supplierNation.Equals(sNation))
                                        {
                                            Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                            kvSupplierNationDim.Add(supplierNationIndex + 1, dgKey);
                                            found = true;
                                            break;
                                        }
                                    }
                                    if (!found)
                                    {
                                        dgKeySupplierNation++;
                                        tempTable.Rows.Add(sNation, dgKeySupplierNation);
                                        kvSupplierNationDim.Add(supplierNationIndex + 1, dgKeySupplierNation);
                                    }
                                    tempTableSupplierNationDim = tempTable;
                                }
                                else
                                {
                                    dgKeySupplierNation++;
                                    tempTableSupplierNationDim.Rows.Add(sNation, dgKeySupplierNation);
                                    kvSupplierNationDim.Add(supplierNationIndex + 1, dgKeySupplierNation);
                                }

                                string sRegion = supplier.sRegion;
                                if (tempTableSupplierRegionDim.Rows.Count > 0)
                                {
                                    var tempTable = tempTableSupplierRegionDim.Copy();
                                    var found = false;
                                    foreach (DataRow row in tempTableSupplierRegionDim.Rows)
                                    {
                                        var supplierRegion = row.Field<string>("supplierRegion");
                                        if (supplierRegion.Equals(sRegion))
                                        {
                                            Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                            kvSupplierRegionDim.Add(supplierRegionIndex + 1, dgKey);
                                            found = true;
                                            break;
                                        }
                                    }
                                    if (!found)
                                    {
                                        dgKeySupplierRegion++;
                                        tempTable.Rows.Add(sRegion, dgKeySupplierRegion);
                                        kvSupplierRegionDim.Add(supplierRegionIndex + 1, dgKeySupplierRegion);
                                    }
                                    tempTableSupplierRegionDim = tempTable;
                                }
                                else
                                {
                                    dgKeySupplierRegion++;
                                    tempTableSupplierRegionDim.Rows.Add(sRegion, dgKeySupplierRegion);
                                    kvSupplierRegionDim.Add(supplierRegionIndex + 1, dgKeySupplierRegion);
                                }
                            }
                            else
                            {
                                kvSupplierNationDim.Add(supplierNationIndex + 1, 0);
                                kvSupplierRegionDim.Add(supplierRegionIndex + 1, 0);
                            }
                            supplierNationIndex++;
                            supplierRegionIndex++;
                        }

                        foreach (var date in dateDimension)
                        {
                            if (date.dYear.CompareTo("1992") >= 0 && date.dYear.CompareTo("1997") <= 0)
                            {
                                string dYear = date.dYear;
                                if (tempTableDateYearDim.Rows.Count > 0)
                                {
                                    var tempTable = tempTableDateYearDim.Copy();
                                    var found = false;
                                    foreach (DataRow row in tempTableDateYearDim.Rows)
                                    {
                                        var dateYear = row.Field<string>("dateYear");
                                        if (dateYear.Equals(dYear))
                                        {
                                            Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                            kvDateYearDim.Add(date.dDateKey, dgKey);
                                            found = true;
                                            break;
                                        }
                                    }
                                    if (!found)
                                    {
                                        dgKeyDateYear++;
                                        tempTable.Rows.Add(dYear, dgKeyDateYear);
                                        kvDateYearDim.Add(date.dDateKey, dgKeyDateYear);
                                    }
                                    tempTableDateYearDim = tempTable;
                                }
                                else
                                {
                                    dgKeyDateYear++;
                                    tempTableDateYearDim.Rows.Add(dYear, dgKeyDateYear);
                                    kvDateYearDim.Add(date.dDateKey, dgKeyDateYear);
                                }

                                string dMonth = date.dMonth;
                                if (tempTableDateMonthDim.Rows.Count > 0)
                                {
                                    var tempTable = tempTableDateMonthDim.Copy();
                                    var found = false;
                                    foreach (DataRow row in tempTableDateMonthDim.Rows)
                                    {
                                        var dateMonth = row.Field<string>("dateMonth");
                                        if (dateMonth.Equals(dMonth))
                                        {
                                            Int64 dgKey = row.Field<Int64>("denseGroupingKey");
                                            kvDateMonthDim.Add(date.dDateKey, dgKey);
                                            found = true;
                                            break;
                                        }
                                    }
                                    if (!found)
                                    {
                                        dgKeyDateMonth++;
                                        tempTable.Rows.Add(dMonth, dgKeyDateMonth);
                                        kvDateMonthDim.Add(date.dDateKey, dgKeyDateMonth);
                                    }
                                    tempTableDateMonthDim = tempTable;
                                }
                                else
                                {
                                    dgKeyDateMonth++;
                                    tempTableDateMonthDim.Rows.Add(dMonth, dgKeyDateMonth);
                                    kvDateMonthDim.Add(date.dDateKey, dgKeyDateMonth);
                                }
                            }
                            else
                            {
                                kvDateYearDim.Add(date.dDateKey, 0);
                                kvDateMonthDim.Add(date.dDateKey, 0);
                            }
                        }
                        break;
                }
                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[IMA Join] GSTest T0 Time: {0}", t0));
                #endregion Key Hashing Phase

                #region Probing Phase
                sw.Reset();
                sw.Start();
                Int64 dgkLengthCustomerNation = 1;
                Int64 dgkLengthCustomerRegion = 1;
                Int64 dgkLengthSupplierNation = 1;
                Int64 dgkLengthSupplierRegion = 1;
                Int64 dgkLengthDateYear = 1;
                Int64 dgkLengthDateMonth = 1;

                Int64[,,,,,] inMemoryAccumulatorTax = new Int64[dgkLengthCustomerNation, dgkLengthCustomerRegion, dgkLengthSupplierNation, dgkLengthSupplierRegion, dgkLengthDateYear, dgkLengthDateMonth];
                List<string> finalTable = new List<string>();


                dgkLengthCustomerNation = tempTableCustomerNationDim.Rows.Count + 1;
                dgkLengthCustomerRegion = tempTableCustomerRegionDim.Rows.Count + 1;
                dgkLengthSupplierNation = tempTableSupplierNationDim.Rows.Count + 1;
                dgkLengthSupplierRegion = tempTableSupplierRegionDim.Rows.Count + 1;
                dgkLengthDateYear = tempTableDateYearDim.Rows.Count + 1;
                dgkLengthDateMonth = tempTableDateMonthDim.Rows.Count + 1;

                inMemoryAccumulatorTax = new Int64[dgkLengthCustomerNation, dgkLengthCustomerRegion, dgkLengthSupplierNation, dgkLengthSupplierRegion, dgkLengthDateYear, dgkLengthDateMonth];

                Int64 dgkCustomerNationDim = 0;
                Int64 dgkCustomerRegionDim = 0;
                Int64 dgkSupplierNationDim = 0;
                Int64 dgkSupplierRegionDim = 0;
                Int64 dgkDateYearDim = 0;
                Int64 dgkDateMonthDim = 0;
                switch (numberOfGroupingAttributes)
                {
                    case 1:
                        for (Int64 i = 0; i < loCustomerKey.Count(); i++)
                        {
                            Int64 custKey = loCustomerKey[i];
                            if (kvCustomerNationDim.TryGetValue(custKey, out dgkCustomerNationDim))
                            {
                                if (dgkCustomerNationDim == 0)
                                {
                                    // skip
                                }
                                else
                                {
                                    inMemoryAccumulatorTax[dgkCustomerNationDim, dgkCustomerRegionDim, dgkSupplierNationDim, dgkSupplierRegionDim, dgkDateYearDim, dgkDateMonthDim] += loTax[i];
                                }
                            }
                        }

                        foreach (DataRow cdRow in tempTableCustomerNationDim.Rows)
                        {
                            Int64 cdKey = cdRow.Field<Int64>("denseGroupingKey");
                            Int64 sumTax = inMemoryAccumulatorTax[cdKey, dgkCustomerRegionDim, dgkSupplierNationDim, dgkSupplierRegionDim, dgkDateYearDim, dgkDateMonthDim];
                            if (sumTax != 0)
                            {
                                finalTable.Add(cdRow.Field<string>("customerNation")
                                    + ", " + sumTax);
                            }
                        }
                        break;
                    case 2:
                        for (Int64 i = 0; i < loCustomerKey.Count(); i++)
                        {
                            Int64 custKey = loCustomerKey[i];
                            if (kvCustomerNationDim.TryGetValue(custKey, out dgkCustomerNationDim)
                                && kvCustomerRegionDim.TryGetValue(custKey, out dgkCustomerRegionDim))
                            {
                                if (dgkCustomerNationDim == 0 || dgkCustomerRegionDim == 0)
                                {
                                    // skip
                                }
                                else
                                {
                                    inMemoryAccumulatorTax[dgkCustomerNationDim, dgkCustomerRegionDim, dgkSupplierNationDim, dgkSupplierRegionDim, dgkDateYearDim, dgkDateMonthDim] += loTax[i];
                                }
                            }
                        }

                        foreach (DataRow cdNRow in tempTableCustomerNationDim.Rows)
                        {
                            foreach (DataRow cdRRow in tempTableCustomerRegionDim.Rows)
                            {
                                Int64 cdNKey = cdNRow.Field<Int64>("denseGroupingKey");
                                Int64 cdRKey = cdRRow.Field<Int64>("denseGroupingKey");

                                Int64 sumTax = inMemoryAccumulatorTax[cdNKey, cdRKey, dgkSupplierNationDim, dgkSupplierRegionDim, dgkDateYearDim, dgkDateMonthDim];
                                if (sumTax != 0)
                                {
                                    finalTable.Add(cdNRow.Field<string>("customerNation")
                                        + ", " + cdRRow.Field<string>("customerRegion")
                                        + ", " + sumTax);
                                }
                            }
                        }

                        break;
                    case 3:
                        for (Int64 i = 0; i < loCustomerKey.Count(); i++)
                        {
                            Int64 custKey = loCustomerKey[i];
                            Int64 suppKey = loSupplierKey[i];
                            if (kvCustomerNationDim.TryGetValue(custKey, out dgkCustomerNationDim)
                                && kvCustomerRegionDim.TryGetValue(custKey, out dgkCustomerRegionDim)
                                && kvSupplierNationDim.TryGetValue(suppKey, out dgkSupplierNationDim))
                            {
                                if (dgkCustomerNationDim == 0 || dgkCustomerRegionDim == 0 || dgkSupplierNationDim == 0)
                                {
                                    // skip
                                }
                                else
                                {
                                    inMemoryAccumulatorTax[dgkCustomerNationDim, dgkCustomerRegionDim, dgkSupplierNationDim, dgkSupplierRegionDim, dgkDateYearDim, dgkDateMonthDim] += loTax[i];
                                }
                            }
                        }

                        foreach (DataRow cdNRow in tempTableCustomerNationDim.Rows)
                        {
                            foreach (DataRow cdRRow in tempTableCustomerRegionDim.Rows)
                            {
                                foreach (DataRow sdNRow in tempTableSupplierNationDim.Rows)
                                {
                                    Int64 cdNKey = cdNRow.Field<Int64>("denseGroupingKey");
                                    Int64 cdRKey = cdRRow.Field<Int64>("denseGroupingKey");
                                    Int64 sdNKey = sdNRow.Field<Int64>("denseGroupingKey");

                                    Int64 sumTax = inMemoryAccumulatorTax[cdNKey, cdRKey, sdNKey, dgkSupplierRegionDim, dgkDateYearDim, dgkDateMonthDim];
                                    if (sumTax != 0)
                                    {
                                        finalTable.Add(cdNRow.Field<string>("customerNation")
                                            + ", " + cdRRow.Field<string>("customerRegion")
                                            + ", " + sdNRow.Field<string>("supplierNation")
                                            + ", " + sumTax);
                                    }
                                }
                            }
                        }
                        break;
                    case 4:
                        for (Int64 i = 0; i < loCustomerKey.Count(); i++)
                        {
                            Int64 custKey = loCustomerKey[i];
                            Int64 suppKey = loSupplierKey[i];
                            if (kvCustomerNationDim.TryGetValue(custKey, out dgkCustomerNationDim)
                                && kvCustomerRegionDim.TryGetValue(custKey, out dgkCustomerRegionDim)
                                && kvSupplierNationDim.TryGetValue(suppKey, out dgkSupplierNationDim)
                                && kvSupplierRegionDim.TryGetValue(suppKey, out dgkSupplierRegionDim))
                            {
                                if (dgkCustomerNationDim == 0 || dgkCustomerRegionDim == 0 || dgkSupplierNationDim == 0 || dgkSupplierRegionDim == 0)
                                {
                                    // skip
                                }
                                else
                                {
                                    inMemoryAccumulatorTax[dgkCustomerNationDim, dgkCustomerRegionDim, dgkSupplierNationDim, dgkSupplierRegionDim, dgkDateYearDim, dgkDateMonthDim] += loTax[i];
                                }
                            }
                        }

                        foreach (DataRow cdNRow in tempTableCustomerNationDim.Rows)
                        {
                            foreach (DataRow cdRRow in tempTableCustomerRegionDim.Rows)
                            {
                                foreach (DataRow sdNRow in tempTableSupplierNationDim.Rows)
                                {
                                    foreach (DataRow sdRRow in tempTableSupplierRegionDim.Rows)
                                    {
                                        Int64 cdNKey = cdNRow.Field<Int64>("denseGroupingKey");
                                        Int64 cdRKey = cdRRow.Field<Int64>("denseGroupingKey");
                                        Int64 sdNKey = sdNRow.Field<Int64>("denseGroupingKey");
                                        Int64 sdRKey = sdRRow.Field<Int64>("denseGroupingKey");

                                        Int64 sumTax = inMemoryAccumulatorTax[cdNKey, cdRKey, sdNKey, sdRKey, dgkDateYearDim, dgkDateMonthDim];
                                        if (sumTax != 0)
                                        {
                                            finalTable.Add(cdNRow.Field<string>("customerNation")
                                                + ", " + cdRRow.Field<string>("customerRegion")
                                                + ", " + sdNRow.Field<string>("supplierNation")
                                                + ", " + sdRRow.Field<string>("supplierRegion")
                                                + ", " + sumTax);
                                        }
                                    }
                                }
                            }
                        }
                        break;
                    case 5:
                        for (Int64 i = 0; i < loCustomerKey.Count(); i++)
                        {
                            Int64 custKey = loCustomerKey[i];
                            Int64 suppKey = loSupplierKey[i];
                            Int64 dateKey = loOrderDate[i];
                            if (kvCustomerNationDim.TryGetValue(custKey, out dgkCustomerNationDim)
                                && kvCustomerRegionDim.TryGetValue(custKey, out dgkCustomerRegionDim)
                                && kvSupplierNationDim.TryGetValue(suppKey, out dgkSupplierNationDim)
                                && kvSupplierRegionDim.TryGetValue(suppKey, out dgkSupplierRegionDim)
                                && kvDateYearDim.TryGetValue(dateKey, out dgkDateYearDim))
                            {
                                if (dgkCustomerNationDim == 0 || dgkCustomerRegionDim == 0 || dgkSupplierNationDim == 0 || dgkSupplierRegionDim == 0 || dgkDateYearDim == 0)
                                {
                                    // skip
                                }
                                else
                                {
                                    inMemoryAccumulatorTax[dgkCustomerNationDim, dgkCustomerRegionDim, dgkSupplierNationDim, dgkSupplierRegionDim, dgkDateYearDim, dgkDateMonthDim] += loTax[i];
                                }
                            }
                        }

                        foreach (DataRow cdNRow in tempTableCustomerNationDim.Rows)
                        {
                            foreach (DataRow cdRRow in tempTableCustomerRegionDim.Rows)
                            {
                                foreach (DataRow sdNRow in tempTableSupplierNationDim.Rows)
                                {
                                    foreach (DataRow sdRRow in tempTableSupplierRegionDim.Rows)
                                    {
                                        foreach (DataRow ddYRow in tempTableDateYearDim.Rows)
                                        {
                                            Int64 cdNKey = cdNRow.Field<Int64>("denseGroupingKey");
                                            Int64 cdRKey = cdRRow.Field<Int64>("denseGroupingKey");
                                            Int64 sdNKey = sdNRow.Field<Int64>("denseGroupingKey");
                                            Int64 sdRKey = sdRRow.Field<Int64>("denseGroupingKey");
                                            Int64 ddYKey = ddYRow.Field<Int64>("denseGroupingKey");
                                            Int64 sumTax = inMemoryAccumulatorTax[cdNKey, cdRKey, sdNKey, sdRKey, ddYKey, dgkDateMonthDim];
                                            if (sumTax != 0)
                                            {
                                                finalTable.Add(cdNRow.Field<string>("customerNation")
                                                    + ", " + cdRRow.Field<string>("customerRegion")
                                                    + ", " + sdNRow.Field<string>("supplierNation")
                                                    + ", " + sdRRow.Field<string>("supplierRegion")
                                                    + ", " + ddYRow.Field<string>("dateYear")
                                                    + ", " + sumTax);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        break;
                    case 6:
                        for (Int64 i = 0; i < loCustomerKey.Count(); i++)
                        {
                            Int64 custKey = loCustomerKey[i];
                            Int64 suppKey = loSupplierKey[i];
                            Int64 dateKey = loOrderDate[i];
                            if (kvCustomerNationDim.TryGetValue(custKey, out dgkCustomerNationDim)
                                && kvCustomerRegionDim.TryGetValue(custKey, out dgkCustomerRegionDim)
                                && kvSupplierNationDim.TryGetValue(suppKey, out dgkSupplierNationDim)
                                && kvSupplierRegionDim.TryGetValue(suppKey, out dgkSupplierRegionDim)
                                && kvDateYearDim.TryGetValue(dateKey, out dgkDateYearDim)
                                && kvDateMonthDim.TryGetValue(dateKey, out dgkDateMonthDim))
                            {
                                if (dgkCustomerNationDim == 0 || dgkCustomerRegionDim == 0 || dgkSupplierNationDim == 0 || dgkSupplierRegionDim == 0 || dgkDateYearDim == 0 || dgkDateMonthDim == 0)
                                {
                                    // skip
                                }
                                else
                                {
                                    inMemoryAccumulatorTax[dgkCustomerNationDim, dgkCustomerRegionDim, dgkSupplierNationDim, dgkSupplierRegionDim, dgkDateYearDim, dgkDateMonthDim] += loTax[i];
                                }
                            }
                        }

                        foreach (DataRow cdNRow in tempTableCustomerNationDim.Rows)
                        {
                            foreach (DataRow cdRRow in tempTableCustomerRegionDim.Rows)
                            {
                                foreach (DataRow sdNRow in tempTableSupplierNationDim.Rows)
                                {
                                    foreach (DataRow sdRRow in tempTableSupplierRegionDim.Rows)
                                    {
                                        foreach (DataRow ddYRow in tempTableDateYearDim.Rows)
                                        {
                                            foreach (DataRow ddMRow in tempTableDateMonthDim.Rows)
                                            {
                                                Int64 cdNKey = cdNRow.Field<Int64>("denseGroupingKey");
                                                Int64 cdRKey = cdRRow.Field<Int64>("denseGroupingKey");
                                                Int64 sdNKey = sdNRow.Field<Int64>("denseGroupingKey");
                                                Int64 sdRKey = sdRRow.Field<Int64>("denseGroupingKey");
                                                Int64 ddYKey = ddYRow.Field<Int64>("denseGroupingKey");
                                                Int64 ddMKey = ddMRow.Field<Int64>("denseGroupingKey");
                                                Int64 sumTax = inMemoryAccumulatorTax[cdNKey, cdRKey, sdNKey, sdRKey, ddYKey, ddMKey];
                                                if (sumTax != 0)
                                                {
                                                    finalTable.Add(cdNRow.Field<string>("customerNation")
                                                        + ", " + cdRRow.Field<string>("customerRegion")
                                                        + ", " + sdNRow.Field<string>("supplierNation")
                                                        + ", " + sdRRow.Field<string>("supplierRegion")
                                                        + ", " + ddYRow.Field<string>("dateYear")
                                                        + ", " + ddMRow.Field<string>("dateMonth")
                                                        + ", " + sumTax);
                                                }
                                            }

                                        }
                                    }
                                }
                            }
                        }
                        break;
                    default:
                        break;
                }


                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[IMA Join] GSTest T1 Time: {0}", t1));
                sw.Reset();
                #endregion Probing Phase

                Console.WriteLine(String.Format("[IMA Join] GSTest Total Time: {0}", t0 + t1));
                Console.WriteLine(String.Format("[IMA Join] GSTest Total : {0}", finalTable.Count));
                Console.WriteLine();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void IMA_V3_Parallel()
        {
            long memoryStart = GC.GetTotalMemory(true);
            Stopwatch sw = new Stopwatch();
            sw.Start();

            Dictionary<Int64, Int64> kvStoreDim = new Dictionary<Int64, Int64>();
            DataTable tempTableStoreDim = new DataTable();
            tempTableStoreDim.Columns.Add("storeName", typeof(string));
            tempTableStoreDim.Columns.Add("denseGroupingKey", typeof(Int64));
            // Dictionary<string, Int64> tempTableStoreDim = new Dictionary<string, Int64>();

            Dictionary<Int64, Int64> kvProductDim = new Dictionary<Int64, Int64>();
            DataTable tempTableProductDim = new DataTable();
            tempTableProductDim.Columns.Add("productName", typeof(string));
            tempTableProductDim.Columns.Add("denseGroupingKey", typeof(Int64));
            Parallel.Invoke(po, () =>
            {

                Int64 storeIndex = 0;
                Int64 dgKeyStore = 0;
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
                                    Int64 dgKey = row.Field<Int64>("denseGroupingKey");
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
                Int64 productIndex = 0;
                Int64 dgKeyProduct = 0;
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
                                    Int64 dgKey = row.Field<Int64>("denseGroupingKey");
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

            Int64 dgkLengthStore = tempTableStoreDim.Rows.Count + 1;
            Int64 dgkLengthProduct = tempTableProductDim.Rows.Count + 1;

            Int64[,] inMemoryAccumulator = new Int64[dgkLengthStore, dgkLengthProduct];

            for (Int64 i = 0; i < sID.Count(); i++)
            {
                Int64 storeId = sID[i];
                Int64 productId = pID[i];
                Int64 dgkStoreDim = 0;
                Int64 dgkProductDim = 0;
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
                    Int64 sumRevenue = inMemoryAccumulator[sdRow.Field<Int64>("denseGroupingKey"), pdRow.Field<Int64>("denseGroupingKey")];
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

            Dictionary<Int64, string> storeDict = new Dictionary<Int64, string>();
            Int64 storeIndex = 0;
            foreach (var type in storeType)
            {
                if (type == 'a')
                {
                    string sName = storeName[storeIndex];
                    storeDict.Add(storeIndex + 1, sName);
                }
                storeIndex++;
            }

            Dictionary<Int64, string> productDict = new Dictionary<Int64, string>();
            Int64 productIndex = 0;
            foreach (var type in supplierDimension)
            {
                if (type == 'b')
                {
                    string pName = productName[productIndex];
                    productDict.Add(productIndex + 1, pName);
                }
                productIndex++;
            }

            Dictionary<string, List<Tuple<Int64, Int64>>> storeGroupDict = new Dictionary<string, List<Tuple<Int64, Int64>>>();
            Int64 scounter = 0;
            foreach (var sid in sID)
            {
                string sName = string.Empty;
                if (storeDict.TryGetValue(sid, out sName))
                {
                    List<Tuple<Int64, Int64>> listAggColl = null;
                    if (storeGroupDict.TryGetValue(sName, out listAggColl))
                    {
                        listAggColl.Add(Tuple.Create(scounter + 1, revenue[scounter]));
                        storeGroupDict[sName] = listAggColl;
                    }
                    else
                    {
                        listAggColl = new List<Tuple<Int64, Int64>>();
                        listAggColl.Add(Tuple.Create(scounter + 1, revenue[scounter]));
                        storeGroupDict.Add(sName, listAggColl);
                    }
                }
                scounter++;
            }

            Dictionary<string, List<Tuple<Int64, Int64>>> productGroupDict = new Dictionary<string, List<Tuple<Int64, Int64>>>();
            Int64 pcounter = 0;
            foreach (var pid in pID)
            {
                string pName = string.Empty;
                if (productDict.TryGetValue(pid, out pName))
                {
                    List<Tuple<Int64, Int64>> listAggColl = null;
                    if (productGroupDict.TryGetValue(pName, out listAggColl))
                    {
                        listAggColl.Add(Tuple.Create(pcounter + 1, revenue[pcounter]));
                        productGroupDict[pName] = listAggColl;
                    }
                    else
                    {
                        listAggColl = new List<Tuple<Int64, Int64>>();
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
                        Int64 sum = 0;
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

            Dictionary<Int64, string> storeDict = new Dictionary<Int64, string>();
            Int64 storeIndex = 0;
            foreach (var type in storeType)
            {
                if (type == 'a')
                {
                    string sName = storeName[storeIndex];
                    storeDict.Add(storeIndex + 1, sName);
                }
                storeIndex++;
            }

            Dictionary<Int64, string> productDict = new Dictionary<Int64, string>();
            Int64 productIndex = 0;
            foreach (var type in supplierDimension)
            {
                if (type == 'b')
                {
                    string pName = productName[productIndex];
                    productDict.Add(productIndex + 1, pName);
                }
                productIndex++;
            }

            Dictionary<string, HashSet<Int64>> storeGroupDict = new Dictionary<string, HashSet<Int64>>();
            Int64 scounter = 0;
            foreach (var sid in sID)
            {
                string sName = string.Empty;
                if (storeDict.TryGetValue(sid, out sName))
                {
                    HashSet<Int64> coll = null;
                    if (storeGroupDict.TryGetValue(sName, out coll))
                    {
                        coll.Add(scounter + 1);
                        storeGroupDict[sName] = coll;
                    }
                    else
                    {
                        coll = new HashSet<Int64>();
                        coll.Add(scounter + 1);
                        storeGroupDict.Add(sName, coll);
                    }
                }
                scounter++;
            }

            Dictionary<string, HashSet<Int64>> productGroupDict = new Dictionary<string, HashSet<Int64>>();
            Int64 pcounter = 0;
            foreach (var pid in pID)
            {
                string pName = string.Empty;
                if (productDict.TryGetValue(pid, out pName))
                {
                    HashSet<Int64> coll = null;
                    if (productGroupDict.TryGetValue(pName, out coll))
                    {
                        coll.Add(pcounter + 1);
                        productGroupDict[pName] = coll;
                    }
                    else
                    {
                        coll = new HashSet<Int64>();
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
                        Int64 sum = 0;
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

            HashSet<Int64> storeDict = new HashSet<Int64>();
            Int64 storeIndex = 0;
            foreach (var type in storeType)
            {
                if (type == 'a')
                {
                    storeDict.Add(storeIndex + 1);
                }
                storeIndex++;
            }

            HashSet<Int64> productDict = new HashSet<Int64>();
            Int64 productIndex = 0;
            foreach (var type in supplierDimension)
            {
                if (type == 'b')
                {
                    productDict.Add(productIndex + 1);
                }
                productIndex++;
            }

            Dictionary<string, HashSet<Int64>> storeGroupDict = new Dictionary<string, HashSet<Int64>>();
            Int64 scounter = 0;
            foreach (var sid in sID)
            {
                if (storeDict.Contains(sid))
                {
                    string sName = storeName[sid - 1];
                    HashSet<Int64> coll = null;
                    if (storeGroupDict.TryGetValue(sName, out coll))
                    {
                        coll.Add(scounter + 1);
                        storeGroupDict[sName] = coll;
                    }
                    else
                    {
                        coll = new HashSet<Int64>();
                        coll.Add(scounter + 1);
                        storeGroupDict.Add(sName, coll);
                    }
                }
                scounter++;
            }

            Dictionary<string, HashSet<Int64>> productGroupDict = new Dictionary<string, HashSet<Int64>>();
            Int64 pcounter = 0;
            foreach (var pid in pID)
            {
                if (productDict.Contains(pid))
                {
                    string pName = productName[pid - 1];
                    HashSet<Int64> coll = null;
                    if (productGroupDict.TryGetValue(pName, out coll))
                    {
                        coll.Add(pcounter + 1);
                        productGroupDict[pName] = coll;
                    }
                    else
                    {
                        coll = new HashSet<Int64>();
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
                    // http://codebetter.com/patricksmacchia/2011/06/16/linq-Int64ersect-2-7x-faster-with-hashset/
                    //                    If the HashSet < T > is bigger than the other sequence, gains factor can be high (like 15x).
                    //If the HashSet < T > is smaller than the other sequence, gains factor tends to 1x.
                    //   If the HashSet<T> size is comparable to the other sequence size, the gain factor is around 2.7.

                    var collectionIntersect = sGItem.Value.Count > pGItem.Value.Count ? sGItem.Value.Intersect(pGItem.Value) : pGItem.Value.Intersect(sGItem.Value);
                    if (collectionIntersect.Count() > 0)
                    {
                        Int64 sum = 0;
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
            HashSet<Int64> storeDict = new HashSet<Int64>();
            HashSet<Int64> productDict = new HashSet<Int64>();

            Parallel.Invoke(po, () =>
            {

                Int64 storeIndex = 0;
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

                Int64 productIndex = 0;
                foreach (var type in supplierDimension)
                {
                    if (type == 'b')
                    {
                        productDict.Add(productIndex + 1);
                    }
                    productIndex++;
                }
            });

            Dictionary<string, HashSet<Int64>> storeGroupDict = new Dictionary<string, HashSet<Int64>>();
            Dictionary<string, HashSet<Int64>> productGroupDict = new Dictionary<string, HashSet<Int64>>();
            Parallel.Invoke(po, () =>
            {
                Int64 scounter = 0;
                foreach (var sid in sID)
                {
                    if (storeDict.Contains(sid))
                    {
                        string sName = storeName[sid - 1];
                        HashSet<Int64> coll = null;
                        if (storeGroupDict.TryGetValue(sName, out coll))
                        {
                            coll.Add(scounter + 1);
                            storeGroupDict[sName] = coll;
                        }
                        else
                        {
                            coll = new HashSet<Int64>();
                            coll.Add(scounter + 1);
                            storeGroupDict.Add(sName, coll);
                        }
                    }
                    scounter++;
                }
            }, () =>
            {
                Int64 pcounter = 0;
                foreach (var pid in pID)
                {
                    if (productDict.Contains(pid))
                    {
                        string pName = productName[pid - 1];
                        HashSet<Int64> coll = null;
                        if (productGroupDict.TryGetValue(pName, out coll))
                        {
                            coll.Add(pcounter + 1);
                            productGroupDict[pName] = coll;
                        }
                        else
                        {
                            coll = new HashSet<Int64>();
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
                    // http://codebetter.com/patricksmacchia/2011/06/16/linq-Int64ersect-2-7x-faster-with-hashset/
                    //                    If the HashSet < T > is bigger than the other sequence, gains factor can be high (like 15x).
                    //If the HashSet < T > is smaller than the other sequence, gains factor tends to 1x.
                    //   If the HashSet<T> size is comparable to the other sequence size, the gain factor is around 2.7.

                    var collectionIntersect = sGItem.Value.Count > pGItem.Value.Count ? sGItem.Value.Intersect(pGItem.Value) : pGItem.Value.Intersect(sGItem.Value);
                    if (collectionIntersect.Count() > 0)
                    {
                        Int64 sum = 0;
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

            HashSet<Int64> storeDict = new HashSet<Int64>();
            Int64 storeIndex = 0;
            foreach (var type in storeType)
            {
                if (type == 'a')
                {
                    storeDict.Add(storeIndex + 1);
                }
                storeIndex++;
            }

            HashSet<Int64> productDict = new HashSet<Int64>();
            Int64 productIndex = 0;
            foreach (var type in supplierDimension)
            {
                if (type == 'b')
                {
                    productDict.Add(productIndex + 1);
                }
                productIndex++;
            }

            Dictionary<string, BitArray> storeGroupDict = new Dictionary<string, BitArray>();
            Int64 scounter = 0;
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
            Int64 pcounter = 0;
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
                    // http://codebetter.com/patricksmacchia/2011/06/16/linq-Int64ersect-2-7x-faster-with-hashset/
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
                        Int64 sum = 0;
                        Int64 index = 0;
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
            Int64 storeIndex = 1;
            foreach (var type in storeType)
            {
                if (type == 'a')
                {
                    storeBA.Set(storeIndex, true);
                }
                storeIndex++;
            }

            BitArray productBA = new BitArray(productId.Count + 1);
            Int64 productIndex = 1;
            foreach (var type in supplierDimension)
            {
                if (type == 'b')
                {
                    productBA.Set(productIndex, true);
                }
                productIndex++;
            }

            Dictionary<Int64, HashSet<Int64>> storeGroupDict = new Dictionary<Int64, HashSet<Int64>>();
            Int64 scounter = 0;
            foreach (var sid in sID)
            {
                if (storeBA[sid])
                {
                    //string sName = storeName[sid - 1];
                    HashSet<Int64> coll = null;
                    if (storeGroupDict.TryGetValue(sid, out coll))
                    {
                        coll.Add(scounter + 1);
                        storeGroupDict[sid] = coll;
                    }
                    else
                    {
                        coll = new HashSet<Int64>();
                        coll.Add(scounter + 1);
                        storeGroupDict.Add(sid, coll);
                    }
                }
                scounter++;
            }

            Dictionary<Int64, HashSet<Int64>> productGroupDict = new Dictionary<Int64, HashSet<Int64>>();
            Int64 pcounter = 0;
            foreach (var pid in pID)
            {
                if (productBA[pid])
                {
                    // string pName = productName[pid - 1];
                    HashSet<Int64> coll = null;
                    if (productGroupDict.TryGetValue(pid, out coll))
                    {
                        coll.Add(pcounter + 1);
                        productGroupDict[pid] = coll;
                    }
                    else
                    {
                        coll = new HashSet<Int64>();
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
                    // http://codebetter.com/patricksmacchia/2011/06/16/linq-Int64ersect-2-7x-faster-with-hashset/
                    //                    If the HashSet < T > is bigger than the other sequence, gains factor can be high (like 15x).
                    //If the HashSet < T > is smaller than the other sequence, gains factor tends to 1x.
                    //   If the HashSet<T> size is comparable to the other sequence size, the gain factor is around 2.7.

                    var collectionIntersect = sGItem.Value.Count > pGItem.Value.Count ? sGItem.Value.Intersect(pGItem.Value) : pGItem.Value.Intersect(sGItem.Value);
                    if (collectionIntersect.Count() > 0)
                    {
                        Int64 sum = 0;
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

            Dictionary<Int64, List<Tuple<Int64, Int64>>> storeDictionary = new Dictionary<Int64, List<Tuple<Int64, Int64>>>();
            Int64 indexSID = 0;
            foreach (var id in sID)
            {
                List<Tuple<Int64, Int64>> value = new List<Tuple<Int64, Int64>>();
                if (!storeDictionary.TryGetValue(id, out value))
                {
                    List<Tuple<Int64, Int64>> item = new List<Tuple<Int64, Int64>>();
                    item.Add(Tuple.Create(indexSID + 1, revenue[indexSID]));
                    storeDictionary.Add(id, item);
                }
                else
                {
                    value.Add(Tuple.Create(indexSID + 1, revenue[indexSID]));
                }
                indexSID++;
            }

            Dictionary<Int64, List<Tuple<Int64, Int64>>> productDictionary = new Dictionary<Int64, List<Tuple<Int64, Int64>>>();
            Int64 indexPID = 0;
            foreach (var id in pID)
            {
                List<Tuple<Int64, Int64>> value = new List<Tuple<Int64, Int64>>();
                if (!productDictionary.TryGetValue(id, out value))
                {
                    List<Tuple<Int64, Int64>> item = new List<Tuple<Int64, Int64>>();
                    item.Add(Tuple.Create(indexPID + 1, revenue[indexPID]));
                    productDictionary.Add(id, item);
                }
                else
                {
                    value.Add(Tuple.Create(indexPID + 1, revenue[indexPID]));
                }
                indexPID++;

            }

            HashSet<Int64> storeSet = new HashSet<Int64>();
            Int64 indexStoreType = 1;
            foreach (var type in storeType)
            {
                if (type.Equals('a')) // search condition
                {
                    storeSet.Add(indexStoreType);
                }
                indexStoreType++;
            }

            HashSet<Int64> productSet = new HashSet<Int64>();
            Int64 indexProductType = 1;
            foreach (var type in supplierDimension)
            {
                if (type.Equals('b')) // search condition
                {
                    productSet.Add(indexProductType);
                }
                indexProductType++;
            }

            var tempStoreDict = new Dictionary<Int64, List<Tuple<Int64, Int64>>>(storeDictionary);
            foreach (var item in tempStoreDict)
            {
                if (!storeSet.Contains(item.Key))
                {
                    storeDictionary.Remove(item.Key);
                }

            }

            var tempProductDict = new Dictionary<Int64, List<Tuple<Int64, Int64>>>(productDictionary);
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
                        Int64 sum = 0;
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

            Dictionary<Int64, List<Tuple<Int64, Int64>>> storeDictionary = new Dictionary<Int64, List<Tuple<Int64, Int64>>>();
            Int64 indexSID = 0;

            Dictionary<Int64, List<Tuple<Int64, Int64>>> productDictionary = new Dictionary<Int64, List<Tuple<Int64, Int64>>>();
            Int64 indexPID = 0;

            HashSet<Int64> storeSet = new HashSet<Int64>();
            Int64 indexStoreType = 1;

            HashSet<Int64> productSet = new HashSet<Int64>();
            Int64 indexProductType = 1;

            Parallel.Invoke(po, () =>
            {
                foreach (var id in sID)
                {
                    List<Tuple<Int64, Int64>> value = new List<Tuple<Int64, Int64>>();
                    if (!storeDictionary.TryGetValue(id, out value))
                    {
                        List<Tuple<Int64, Int64>> item = new List<Tuple<Int64, Int64>>();
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
                    List<Tuple<Int64, Int64>> value = new List<Tuple<Int64, Int64>>();
                    if (!productDictionary.TryGetValue(id, out value))
                    {
                        List<Tuple<Int64, Int64>> item = new List<Tuple<Int64, Int64>>();
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
                var tempStoreDict = new Dictionary<Int64, List<Tuple<Int64, Int64>>>(storeDictionary);
                foreach (var item in tempStoreDict)
                {
                    if (!storeSet.Contains(item.Key))
                    {
                        storeDictionary.Remove(item.Key);
                    }

                }
            }, () =>
            {
                var tempProductDict = new Dictionary<Int64, List<Tuple<Int64, Int64>>>(productDictionary);
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
                        Int64 sum = 0;
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

            Dictionary<Int64, string> storeDict = new Dictionary<Int64, string>();
            Int64 storeIndex = 0;
            foreach (var type in storeType)
            {
                if (type == 'a')
                {
                    string sName = storeName[storeIndex];
                    storeDict.Add(storeIndex + 1, sName);
                }
                storeIndex++;
            }

            Dictionary<Int64, string> productDict = new Dictionary<Int64, string>();
            Int64 productIndex = 0;
            foreach (var type in supplierDimension)
            {
                if (type == 'b')
                {
                    string pName = productName[productIndex];
                    productDict.Add(productIndex + 1, pName);
                }
                productIndex++;
            }

            Dictionary<string, List<Tuple<Int64, Int64>>> storeGroupDict = new Dictionary<string, List<Tuple<Int64, Int64>>>();
            Int64 scounter = 0;
            foreach (var sid in sID)
            {
                string storeName = string.Empty;
                if (storeDict.TryGetValue(sid, out storeName))
                {
                    List<Tuple<Int64, Int64>> listAggColl = null;
                    if (storeGroupDict.TryGetValue(storeName, out listAggColl))
                    {
                        listAggColl.Add(Tuple.Create(scounter + 1, revenue[scounter]));
                        storeGroupDict[storeName] = listAggColl;
                    }
                    else
                    {
                        listAggColl = new List<Tuple<Int64, Int64>>();
                        listAggColl.Add(Tuple.Create(scounter + 1, revenue[scounter]));
                        storeGroupDict.Add(storeName, listAggColl);
                    }
                }
                scounter++;
            }

            Dictionary<string, List<Tuple<Int64, Int64>>> productGroupDict = new Dictionary<string, List<Tuple<Int64, Int64>>>();
            Int64 pcounter = 0;
            foreach (var pid in pID)
            {
                string productName = string.Empty;
                if (productDict.TryGetValue(pid, out productName))
                {
                    List<Tuple<Int64, Int64>> listAggColl = null;
                    if (productGroupDict.TryGetValue(productName, out listAggColl))
                    {
                        listAggColl.Add(Tuple.Create(pcounter + 1, revenue[pcounter]));
                        productGroupDict[productName] = listAggColl;
                    }
                    else
                    {
                        listAggColl = new List<Tuple<Int64, Int64>>();
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
                        Int64 sum = 0;
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

            Dictionary<Int64, string> storeDict = new Dictionary<Int64, string>();
            Int64 storeIndex = 0;

            Dictionary<Int64, string> productDict = new Dictionary<Int64, string>();
            Int64 productIndex = 0;

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

            Dictionary<string, List<Tuple<Int64, Int64>>> storeGroupDict = new Dictionary<string, List<Tuple<Int64, Int64>>>();
            Int64 scounter = 0;

            Dictionary<string, List<Tuple<Int64, Int64>>> productGroupDict = new Dictionary<string, List<Tuple<Int64, Int64>>>();
            Int64 pcounter = 0;

            Parallel.Invoke(po, () =>
            {
                foreach (var sid in sID)
                {
                    string storeName = string.Empty;
                    if (storeDict.TryGetValue(sid, out storeName))
                    {
                        List<Tuple<Int64, Int64>> listAggColl = null;
                        if (storeGroupDict.TryGetValue(storeName, out listAggColl))
                        {
                            listAggColl.Add(Tuple.Create(scounter + 1, revenue[scounter]));
                            storeGroupDict[storeName] = listAggColl;
                        }
                        else
                        {
                            listAggColl = new List<Tuple<Int64, Int64>>();
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
                        List<Tuple<Int64, Int64>> listAggColl = null;
                        if (productGroupDict.TryGetValue(productName, out listAggColl))
                        {
                            listAggColl.Add(Tuple.Create(pcounter + 1, revenue[pcounter]));
                            productGroupDict[productName] = listAggColl;
                        }
                        else
                        {
                            listAggColl = new List<Tuple<Int64, Int64>>();
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
                        Int64 sum = 0;
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

        public Int64 getRandomInt(Int64 minimum, Int64 maximum)
        {
            return rand.Next(minimum, maximum);
        }

        public char getRandomLetter()
        {
            Int64 num = getRandomInt(0, 26);
            char let = Convert.ToChar('a' + num);
            return let;
        }

        public void saveAndPrInt64Results()
        {
            //TestResultsDatabase.nimbleJoinOutput.Add(testResults.toString());
            //Console.WriteLine("IMA: " + testResults.toString());
            //Console.WriteLine();
        }
    }

}

