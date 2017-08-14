using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParallelHashJoins
{
    class ReturnInteger
    {
        public int id { get; set; }
        public string value { get; set; }
    }
    class Algorithms
    {
        #region Private Variables
        private static string folderPath = @"C:\Raw_Data_Source_For_Test\SSBM - DBGEN\SF 1";
        private static List<int> cCustKey = new List<int>();
        private static List<string> cName = new List<string>();
        private static List<string> cAddress = new List<string>();
        private static List<string> cCity = new List<string>();
        private static List<string> cNation = new List<string>();
        private static List<string> cRegion = new List<string>();
        private static List<string> cPhone = new List<string>();
        private static List<string> cMktSegment = new List<string>();

        private static List<int> sSuppKey = new List<int>();
        private static List<string> sName = new List<string>();
        private static List<string> sAddress = new List<string>();
        private static List<string> sCity = new List<string>();
        private static List<string> sNation = new List<string>();
        private static List<string> sRegion = new List<string>();
        private static List<string> sPhone = new List<string>();

        private static List<int> pSize = new List<int>();
        private static List<int> pPartKey = new List<int>();
        private static List<string> pName = new List<string>();
        private static List<string> pMFGR = new List<string>();
        private static List<string> pCategory = new List<string>();
        private static List<string> pBrand = new List<string>();
        private static List<string> pColor = new List<string>();
        private static List<string> pType = new List<string>();
        private static List<string> pContainer = new List<string>();

        private static List<int> loOrderKey = new List<int>();
        private static List<int> loLineNumber = new List<int>();
        private static List<int> loCustKey = new List<int>();
        private static List<int> loPartKey = new List<int>();
        private static List<int> loSuppKey = new List<int>();
        private static List<int> loOrderDate = new List<int>();
        private static List<char> loShipPriority = new List<char>();
        private static List<int> loQuantity = new List<int>();
        private static List<Tuple<int, int>> loQuantityWithId = new List<Tuple<int, int>>();

        private static List<int> loExtendedPrice = new List<int>();
        private static List<int> loOrdTotalPrice = new List<int>();
        private static List<int> loDiscount = new List<int>();
        private static List<Tuple<int, int>> loDiscountWithId = new List<Tuple<int, int>>();
        private static List<int> loRevenue = new List<int>();
        private static List<int> loSupplyCost = new List<int>();
        private static List<int> loTax = new List<int>();
        private static List<int> loCommitDate = new List<int>();
        private static List<string> loShipMode = new List<string>();
        private static List<string> loOrderPriority = new List<string>();

        private static List<int> dDateKey = new List<int>();
        private static List<int> dYear = new List<int>();
        private static List<int> dYearMonthNum = new List<int>();
        private static List<int> dDayNumInWeek = new List<int>();
        private static List<int> dDayNumInMonth = new List<int>();
        private static List<int> dDayNumInYear = new List<int>();
        private static List<int> dMonthNumInYear = new List<int>();
        private static List<int> dWeekNumInYear = new List<int>();
        private static List<int> dLastDayInWeekFL = new List<int>();
        private static List<int> dLastDayInMonthFL = new List<int>();
        private static List<int> dHolidayFL = new List<int>();
        private static List<int> dWeekDayFL = new List<int>();
        private static List<string> dDate = new List<string>();
        private static List<string> dDayOfWeek = new List<string>();
        private static List<string> dMonth = new List<string>();
        private static List<string> dYearMonth = new List<string>();
        private static List<string> dSellingSeason = new List<string>();

        private static List<Customer> customer = new List<Customer>();
        private static List<Supplier> supplier = new List<Supplier>();
        private static List<Part> part = new List<Part>();
        private static List<LineOrder> lineOrder = new List<LineOrder>();
        private static List<Date> date = new List<Date>();

        private static string binaryFilesDirectory = @"C:\Raw_Data_Source_For_Test\SSBM - DBGEN\BFSF4";
        private static string dateFile = Path.Combine(binaryFilesDirectory, "dateFile");
        private static string customerFile = Path.Combine(binaryFilesDirectory, "customerFile");
        private static string supplierFile = Path.Combine(binaryFilesDirectory, "supplierFile");
        private static string partFile = Path.Combine(binaryFilesDirectory, "partFile");

        private static string dDateKeyFile = Path.Combine(binaryFilesDirectory, @"\dDateKey\");
        private static string dYearFile = Path.Combine(binaryFilesDirectory, @"\dYear\");
        private static string dYearMonthNumFile = Path.Combine(binaryFilesDirectory, @"\dYearMonthNum\");
        private static string dDayNumInWeekFile = Path.Combine(binaryFilesDirectory, @"\dDayNumInWeek\");
        private static string dDayNumInMonthFile = Path.Combine(binaryFilesDirectory, @"\dDayNumInMont\");
        private static string dDayNumInYearFile = Path.Combine(binaryFilesDirectory, @"\dDayNumInYear\");
        private static string dMonthNumInYearFile = Path.Combine(binaryFilesDirectory, @"\dMonthNumInYear\");
        private static string dWeekNumInYearFile = Path.Combine(binaryFilesDirectory, @"\dWeekNumInYear\");
        private static string dLastDayInWeekFLFile = Path.Combine(binaryFilesDirectory, @"\dLastDayInWeekFL\");
        private static string dLastDayInMonthFLFile = Path.Combine(binaryFilesDirectory, @"\dLastDayInMonthFL\");
        private static string dHolidayFLFile = Path.Combine(binaryFilesDirectory, @"\dHolidayFL\");
        private static string dWeekDayFLFile = Path.Combine(binaryFilesDirectory, @"\dWeekDayFL\");
        private static string dDateFile = Path.Combine(binaryFilesDirectory, @"\dDate\");
        private static string dDayOfWeekFile = Path.Combine(binaryFilesDirectory, @"\dDayOfWeek\");
        private static string dMonthFile = Path.Combine(binaryFilesDirectory, @"\dMonth\");
        private static string dYearMonthFile = Path.Combine(binaryFilesDirectory, @"\dYearMonth\");
        private static string dSellingSeasonFile = Path.Combine(binaryFilesDirectory, @"\dSellingSeason\");

        private static string loOrderKeyFile = Path.Combine(binaryFilesDirectory, @"\loOrderKey\");
        private static string loLineNumberFile = Path.Combine(binaryFilesDirectory, @"\loLineNumber\");
        private static string loCustKeyFile = Path.Combine(binaryFilesDirectory, @"\loCustKey\");
        private static string loPartKeyFile = Path.Combine(binaryFilesDirectory, @"\loPartKey\");
        private static string loSuppKeyFile = Path.Combine(binaryFilesDirectory, @"\loSuppKey\");
        private static string loOrderDateFile = Path.Combine(binaryFilesDirectory, @"\loOrderDate\");
        private static string loShipPriorityFile = Path.Combine(binaryFilesDirectory, @"\loShipPriority\");
        private static string loQuantityFile = Path.Combine(binaryFilesDirectory, @"\loQuantity\");
        private static string loExtendedPriceFile = Path.Combine(binaryFilesDirectory, @"\loExtendedPrice\");
        private static string loOrdTotalPriceFile = Path.Combine(binaryFilesDirectory, @"\loOrdTotalPrice\");
        private static string loDiscountFile = Path.Combine(binaryFilesDirectory, @"\loDiscount\");
        private static string loRevenueFile = Path.Combine(binaryFilesDirectory, @"\loRevenue\");
        private static string loSupplyCostFile = Path.Combine(binaryFilesDirectory, @"\loSupplyCost\");
        private static string loTaxFile = Path.Combine(binaryFilesDirectory, @"\loTax\");
        private static string loCommitDateFile = Path.Combine(binaryFilesDirectory, @"\loCommitDate\");
        private static string loShipModeFile = Path.Combine(binaryFilesDirectory, @"\loShipMode\");
        private static string loOrderPriorityFile = Path.Combine(binaryFilesDirectory, @"\loOrderPriority\");

        private static string cCustKeyFile = Path.Combine(binaryFilesDirectory, @"\cCustKey\");
        private static string cNameFile = Path.Combine(binaryFilesDirectory, @"\cName\");
        private static string cAddressFile = Path.Combine(binaryFilesDirectory, @"\cAddress\");
        private static string cCityFile = Path.Combine(binaryFilesDirectory, @"\cCity\");
        private static string cNationFile = Path.Combine(binaryFilesDirectory, @"\cNation\");
        private static string cRegionFile = Path.Combine(binaryFilesDirectory, @"\cRegion\");
        private static string cPhoneFile = Path.Combine(binaryFilesDirectory, @"\cPhone\");
        private static string cMktSegmentFile = Path.Combine(binaryFilesDirectory, @"\cMktSegment\");

        private static string sSuppKeyFile = Path.Combine(binaryFilesDirectory, @"\sSuppKey\");
        private static string sNameFile = Path.Combine(binaryFilesDirectory, @"\sName\");
        private static string sAddressFile = Path.Combine(binaryFilesDirectory, @"\sAddress\");
        private static string sCityFile = Path.Combine(binaryFilesDirectory, @"\sCity\");
        private static string sNationFile = Path.Combine(binaryFilesDirectory, @"\sNation\");
        private static string sRegionFile = Path.Combine(binaryFilesDirectory, @"\sRegion\");
        private static string sPhoneFile = Path.Combine(binaryFilesDirectory, @"\sPhone\");

        private static string pSizeFile = Path.Combine(binaryFilesDirectory, @"\pSize\");
        private static string pPartKeyFile = Path.Combine(binaryFilesDirectory, @"\pPartKey\");
        private static string pNameFile = Path.Combine(binaryFilesDirectory, @"\pName\");
        private static string pMFGRFile = Path.Combine(binaryFilesDirectory, @"\pMFGR\");
        private static string pCategoryFile = Path.Combine(binaryFilesDirectory, @"\pCategory\");
        private static string pBrandFile = Path.Combine(binaryFilesDirectory, @"\pBrand\");
        private static string pColorFile = Path.Combine(binaryFilesDirectory, @"\pColor\");
        private static string pTypeFile = Path.Combine(binaryFilesDirectory, @"\pType\");
        private static string pContainerFile = Path.Combine(binaryFilesDirectory, @"\pContainer\");
        #endregion Private Variables

        private static long nanoTime()
        {
            long nano = 10000L * Stopwatch.GetTimestamp();
            nano /= TimeSpan.TicksPerMillisecond;
            nano *= 100L;
            return nano;
        }

        ParallelOptions po = new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount };
        public void NimbleJoin()
        {
            try
            {
                var dateHashTable = new Dictionary<int, string>();
                var customerHashTable = new Dictionary<int, string>();
                var supplierHashTable = new Dictionary<int, string>();
                Stopwatch sw = new Stopwatch();
                #region Key Hashing Phase
                sw.Start();
                List<Date> dateDimension = Utils.ReadFromBinaryFile<List<Date>>(dateFile);
                foreach (var row in dateDimension)
                {
                    if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
                    {
                        // var startTime =  nanoTime();
                        dateHashTable.Add(row.dDateKey, row.dYear);
                        // var endTime = nanoTime();
                        // Console.WriteLine(endTime - startTime);

                    }
                }
                dateDimension.Clear();

                List<Customer> customerDimension = Utils.ReadFromBinaryFile<List<Customer>>(customerFile);
                foreach (var row in customerDimension)
                {
                    if (row.cRegion.Equals("ASIA"))
                        customerHashTable.Add(row.cCustKey, row.cNation);
                }
                customerDimension.Clear();

                List<Supplier> supplierDimension = Utils.ReadFromBinaryFile<List<Supplier>>(supplierFile);
                foreach (var row in supplierDimension)
                {
                    if (row.sRegion.Equals("ASIA"))
                        supplierHashTable.Add(row.sSuppKey, row.sNation);
                }
                supplierDimension.Clear();
                //sw.Stop();
                //Console.WriteLine("[Nimble Join]: Key Hashing Phase takes {0} ms.", sw.ElapsedMilliseconds);
                //sw.Reset();
                #endregion Key Hashing Phase

                #region Probing Phase
                //Stopwatch sw1 = Stopwatch.StartNew();
                //sw.Start();
                List<int> loOrderDate = Utils.ReadFromBinaryFiles<int>(binaryFilesDirectory + loOrderDateFile);
                var matchedOrderDate = new Dictionary<int, string>();
                //var matchedOrderDate = new FastBitMap(loOrderDate.Count);

                var i = 0;
                foreach (var orderDate in loOrderDate)
                {
                    string dYear = "";
                    if (dateHashTable.TryGetValue(orderDate, out dYear))
                        matchedOrderDate.Add(i + 1, dYear.ToString());
                    //matchedOrderDate.Set(i, true, dYear);
                    i++;
                }
                loOrderDate.Clear();

                //sw.Stop();
                //Console.WriteLine("[Nimble Join]: Probing Phase [matchedOrderDate] takes {0} ms.", sw.ElapsedMilliseconds);
                //sw.Reset();
                //sw.Start();

                List<int> loCustomerKey = Utils.ReadFromBinaryFiles<int>(binaryFilesDirectory + loCustKeyFile);
                var matchedCustomerKey = new Dictionary<int, string>();
                //var matchedCustomerKey = new FastBitMap(loCustomerKey.Count);
                var j = 0;
                foreach (var custKey in loCustomerKey)
                {
                    string cNation = string.Empty;
                    if (customerHashTable.TryGetValue(custKey, out cNation))
                        matchedCustomerKey.Add(j + 1, cNation);
                    //matchedCustomerKey.Set(j, true, cNation);
                    j++;
                }
                loCustomerKey.Clear();

                //                sw.Stop();
                //              Console.WriteLine("[Nimble Join]: Probing Phase [matchedCustomerKey] takes {0} ms.", sw.ElapsedMilliseconds);
                //            sw.Reset();
                //sw.Start();
                //
                List<int> loSupplierKey = Utils.ReadFromBinaryFiles<int>(binaryFilesDirectory + loSuppKeyFile);
                var matchedSupplierKey = new Dictionary<int, string>();
                //var matchedSupplierKey = new FastBitMap(loSupplierKey.Count);

                var k = 0;
                foreach (var suppKey in loSupplierKey)
                {
                    string sNation = string.Empty;
                    if (supplierHashTable.TryGetValue(suppKey, out sNation))
                        matchedSupplierKey.Add(k + 1, sNation);
                    //matchedSupplierKey.Set(k, true, sNation);
                    k++;
                }
                loSupplierKey.Clear();
                //sw.Stop();
                //Console.WriteLine("[Nimble Join]: Probing Phase [matchedSupplierKey] takes {0} ms.", sw.ElapsedMilliseconds);
                ///sw.Reset();
                //sw.Start();

                var mcSize = matchedCustomerKey.Count();
                var msSize = matchedSupplierKey.Count();
                var moSize = matchedOrderDate.Count();
                var joinOutputIntermediate = new Dictionary<int, string>();

                if (mcSize < msSize && mcSize < moSize)
                {
                    foreach (var item in matchedCustomerKey)
                    {
                        string suppNation, orderDate;
                        if (matchedSupplierKey.TryGetValue(item.Key, out suppNation) && matchedOrderDate.TryGetValue(item.Key, out orderDate))
                        {
                            joinOutputIntermediate.Add(item.Key, item.Value + "," + suppNation + "," + orderDate);
                        }

                    }
                }
                else if (msSize < moSize)
                {
                    foreach (var item in matchedSupplierKey)
                    {
                        string custNation, orderDate;
                        if (matchedCustomerKey.TryGetValue(item.Key, out custNation) && matchedOrderDate.TryGetValue(item.Key, out orderDate))
                        {
                            joinOutputIntermediate.Add(item.Key, custNation + "," + item.Value + "," + orderDate);
                        }

                    }
                }
                else
                {
                    foreach (var item in matchedOrderDate)
                    {
                        string custNation, suppNation;
                        if (matchedCustomerKey.TryGetValue(item.Key, out custNation) && matchedSupplierKey.TryGetValue(item.Key, out suppNation))
                        {
                            joinOutputIntermediate.Add(item.Key, custNation + "," + suppNation + "," + item.Value);
                        }
                    }
                }

                //sw.Stop();
                //Console.WriteLine("[Nimble Join]: Probing Phase [JOIN] takes {0} ms.", sw.ElapsedMilliseconds);

                // sw1.Stop();
                #endregion Probing Phase
                //Console.WriteLine("[Nimble Join]: Probing Phase takes {0} ms.", sw1.ElapsedMilliseconds);

                #region Value Extraction Phase
                //sw.Reset();
                //sw.Start();
                List<int> loRevenue = Utils.ReadFromBinaryFiles<int>(binaryFilesDirectory + loRevenueFile);
                var joinOutputFinal = new Dictionary<int, string>();
                foreach (var item in joinOutputIntermediate)
                {
                    joinOutputFinal.Add(item.Key, item.Value + "," + loRevenue[item.Key - 1]); // Direct array lookup
                }
                // sw.Stop();
                //Console.WriteLine("[Nimble Join]: Value Extraction Phase takes {0} ms.", sw.ElapsedMilliseconds);
                #endregion Value Extraction Phase

                sw.Stop();
                Console.WriteLine("[Nimble Join]: Total time: {0} sec.", sw.ElapsedMilliseconds / 1000);
                Console.WriteLine("[Nimble Join]: Total Rows: {0}.", joinOutputFinal.Count);

            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void XYZV2()
        {
            try
            {
                var dateHashTable = new Dictionary<int, string>();
                var customerHashTable = new Dictionary<int, string>();
                var supplierHashTable = new Dictionary<int, string>();
                Stopwatch sw = new Stopwatch();
                #region Key Hashing Phase
                sw.Start();
                List<Date> dateDimension = Utils.ReadFromBinaryFile<List<Date>>(dateFile);
                foreach (var row in dateDimension)
                {
                    if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
                        dateHashTable.Add(row.dDateKey, row.dYear);
                }
                dateDimension.Clear();

                List<Customer> customerDimension = Utils.ReadFromBinaryFile<List<Customer>>(customerFile);
                foreach (var row in customerDimension)
                {
                    if (row.cRegion.Equals("ASIA"))
                        customerHashTable.Add(row.cCustKey, row.cNation);
                }
                customerDimension.Clear();

                List<Supplier> supplierDimension = Utils.ReadFromBinaryFile<List<Supplier>>(supplierFile);
                foreach (var row in supplierDimension)
                {
                    if (row.sRegion.Equals("ASIA"))
                        supplierHashTable.Add(row.sSuppKey, row.sNation);
                }
                supplierDimension.Clear();
                sw.Stop();

                #endregion Key Hashing Phase

                Console.WriteLine("[XYZ Join]: Key Hashing Phase took {0} ms.", sw.ElapsedMilliseconds);
                sw.Reset();

                #region Probing Phase
                sw.Start();
                Stopwatch sw1 = new Stopwatch();
                sw1.Start();

                List<int> loOrderDate = Utils.ReadFromBinaryFile<List<int>>(loOrderDateFile);
                var matchedOrderDate = new Dictionary<int, string>();
                var i = 0;
                foreach (var orderDate in loOrderDate)
                {
                    string dYear = "";
                    if (dateHashTable.TryGetValue(orderDate, out dYear))
                    {
                        matchedOrderDate.Add(i + 1, dYear);
                    }
                    i++;
                }
                loOrderDate.Clear();

                sw1.Stop();
                Console.WriteLine("[XYZ Join]: Probing Phase (matchedOrderDate) took {0} ms.", sw1.ElapsedMilliseconds);
                sw1.Reset();
                sw1.Start();

                List<int> loCustomerKey = Utils.ReadFromBinaryFile<List<int>>(loCustKeyFile);
                var matchedCustomerKey = new Dictionary<int, string>();
                var j = 0;
                foreach (var custKey in loCustomerKey)
                {
                    string cNation = string.Empty;
                    if (customerHashTable.TryGetValue(custKey, out cNation))
                    {
                        matchedCustomerKey.Add(j + 1, cNation);
                    }
                    j++;
                }
                loCustomerKey.Clear();

                sw1.Stop();
                Console.WriteLine("[XYZ Join]: Probing Phase (matchedCustomerKey) took {0} ms.", sw1.ElapsedMilliseconds);
                sw1.Reset();
                sw1.Start();

                List<int> loSupplierKey = Utils.ReadFromBinaryFile<List<int>>(loSuppKeyFile);
                var matchedSupplierKey = new Dictionary<int, string>();
                var k = 0;
                foreach (var suppKey in loSupplierKey)
                {
                    string sNation = string.Empty;
                    if (supplierHashTable.TryGetValue(suppKey, out sNation))
                    {
                        matchedSupplierKey.Add(k + 1, sNation);
                    }
                    k++;
                }
                loSupplierKey.Clear();

                sw1.Stop();
                Console.WriteLine("[XYZ Join]: Probing Phase (matchedSupplierKey) took {0} ms.", sw1.ElapsedMilliseconds);
                sw1.Reset();
                sw1.Start();

                //var joinOutputIntermediate2 = new Dictionary<int, string>();
                //foreach (var item in matchedOrderDate)
                //{
                //    string custNation, suppNation;
                //    if (matchedCustomerKey.TryGetValue(item.Key, out custNation))
                //    {
                //        if (matchedSupplierKey.TryGetValue(item.Key, out suppNation))
                //            joinOutputIntermediate2.Add(item.Key, item.Value + "," + custNation + "," + suppNation);
                //    }
                //}

                var joinOutputIntermediate1 = matchedSupplierKey.Where(x => matchedCustomerKey.ContainsKey(x.Key))
                     .ToDictionary(x => x.Key, x => x.Value + "," + matchedCustomerKey[x.Key]);

                var joinOutputIntermediate2 = joinOutputIntermediate1.Where(x => matchedOrderDate.ContainsKey(x.Key))
                    .ToDictionary(x => x.Key, x => x.Value + "," + matchedOrderDate[x.Key]);


                sw1.Stop();
                Console.WriteLine("[XYZ Join]: Probing Phase (JOIN) took {0} ms.", sw1.ElapsedMilliseconds);
                sw.Stop();
                #endregion Probing Phase

                Console.WriteLine("[XYZ Join]: Probing Phase took {0} ms.", sw.ElapsedMilliseconds);
                sw.Reset();

                #region Value Extraction Phase
                sw.Start();
                List<int> loRevenue = Utils.ReadFromBinaryFile<List<int>>(loRevenueFile);

                Dictionary<int, string> joinOutputFinal = new Dictionary<int, string>();
                foreach (var item in joinOutputIntermediate2)
                {
                    joinOutputFinal.Add(item.Key, item.Value + "," + loRevenue[item.Key]);
                }
                sw.Stop();
                #endregion Value Extraction Phase

                Console.WriteLine("[XYZ Join]: Value Extraction Phase took {0} ms.", sw.ElapsedMilliseconds);
                Console.WriteLine("[XYZ Join]: Total Rows: {0}.", joinOutputFinal.Count);

            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public Dictionary<int, string> getSmallestDictionary(List<Dictionary<int, string>> listOfDictionaries)
        {
            int smallest = -1;
            int i = 0;
            foreach (var dict in listOfDictionaries)
            {
                if (smallest == -1)
                {
                    smallest = dict.Count;
                }
                else if (dict.Count < smallest)
                {
                    smallest = dict.Count;
                    i++;
                }
            }
            return listOfDictionaries[i];
        }

        public void XYZJoin()
        {
            try
            {
                var dateHashTable = new Dictionary<int, string>();
                var customerHashTable = new Dictionary<int, string>();
                var supplierHashTable = new Dictionary<int, string>();
                Stopwatch sw = new Stopwatch();
                #region Key Hashing Phase
                sw.Start();
                List<Date> dateDimension = Utils.ReadFromBinaryFile<List<Date>>(dateFile);
                foreach (var row in dateDimension)
                {
                    if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
                        dateHashTable.Add(row.dDateKey, row.dYear);
                }
                dateDimension.Clear();

                List<Customer> customerDimension = Utils.ReadFromBinaryFile<List<Customer>>(customerFile);
                foreach (var row in customerDimension)
                {
                    if (row.cRegion.Equals("ASIA"))
                        customerHashTable.Add(row.cCustKey, row.cNation);
                }
                customerDimension.Clear();

                List<Supplier> supplierDimension = Utils.ReadFromBinaryFile<List<Supplier>>(supplierFile);
                foreach (var row in supplierDimension)
                {
                    if (row.sRegion.Equals("ASIA"))
                        supplierHashTable.Add(row.sSuppKey, row.sNation);
                }
                supplierDimension.Clear();
                sw.Stop();

                #endregion Key Hashing Phase

                Console.WriteLine("[XYZ Join]: Key Hashing Phase took {0} ms.", sw.ElapsedMilliseconds);
                sw.Reset();

                #region Probing Phase
                sw.Start();
                Stopwatch sw1 = new Stopwatch();
                sw1.Start();

                List<int> loOrderDate = Utils.ReadFromBinaryFile<List<int>>(loOrderDateFile);
                var matchedOrderDate = new Dictionary<int, string>();
                var i = 0;
                foreach (var orderDate in loOrderDate)
                {
                    if (dateHashTable.ContainsKey(orderDate))
                    {
                        string dYear = "";
                        dateHashTable.TryGetValue(orderDate, out dYear);
                        matchedOrderDate.Add(i + 1, dYear);
                    }
                    i++;
                }
                loOrderDate.Clear();

                sw1.Stop();
                Console.WriteLine("[XYZ Join]: Probing Phase (matchedOrderDate) took {0} ms.", sw1.ElapsedMilliseconds);
                sw1.Reset();
                sw1.Start();

                List<int> loCustomerKey = Utils.ReadFromBinaryFile<List<int>>(loCustKeyFile);
                var matchedCustomerKey = new Dictionary<int, string>();
                var j = 0;
                foreach (var custKey in loCustomerKey)
                {
                    if (customerHashTable.ContainsKey(custKey))
                    {
                        string cNation = string.Empty;
                        customerHashTable.TryGetValue(custKey, out cNation);
                        matchedCustomerKey.Add(j + 1, cNation);
                    }
                    j++;
                }
                loCustomerKey.Clear();

                sw1.Stop();
                Console.WriteLine("[XYZ Join]: Probing Phase (matchedCustomerKey) took {0} ms.", sw1.ElapsedMilliseconds);
                sw1.Reset();
                sw1.Start();

                List<int> loSupplierKey = Utils.ReadFromBinaryFile<List<int>>(loSuppKeyFile);
                var matchedSupplierKey = new Dictionary<int, string>();
                var k = 0;
                foreach (var suppKey in loSupplierKey)
                {
                    if (supplierHashTable.ContainsKey(suppKey))
                    {
                        string sNation = string.Empty;
                        supplierHashTable.TryGetValue(suppKey, out sNation);
                        matchedSupplierKey.Add(k + 1, sNation);
                    }
                    k++;
                }
                loSupplierKey.Clear();

                sw1.Stop();
                Console.WriteLine("[XYZ Join]: Probing Phase (matchedSupplierKey) took {0} ms.", sw1.ElapsedMilliseconds);
                sw1.Reset();
                sw1.Start();

                var joinOutputIntermediate1 = matchedSupplierKey.Where(x => matchedCustomerKey.ContainsKey(x.Key))
                     .ToDictionary(x => x.Key, x => x.Value + "," + matchedCustomerKey[x.Key]);

                var joinOutputIntermediate2 = joinOutputIntermediate1.Where(x => matchedOrderDate.ContainsKey(x.Key))
                    .ToDictionary(x => x.Key, x => x.Value + "," + matchedOrderDate[x.Key]);

                sw1.Stop();
                Console.WriteLine("[XYZ Join]: Probing Phase (JOIN) took {0} ms.", sw1.ElapsedMilliseconds);
                sw.Stop();
                #endregion Probing Phase

                Console.WriteLine("[XYZ Join]: Probing Phase took {0} ms.", sw.ElapsedMilliseconds);
                sw.Reset();

                #region Value Extraction Phase
                sw.Start();
                List<int> loRevenue = Utils.ReadFromBinaryFile<List<int>>(loRevenueFile);

                Dictionary<int, string> joinOutputFinal = new Dictionary<int, string>();
                foreach (var item in joinOutputIntermediate2)
                {
                    joinOutputFinal.Add(item.Key, item.Value + "," + loRevenue[item.Key]);
                }
                sw.Stop();
                #endregion Value Extraction Phase

                Console.WriteLine("[XYZ Join]: Value Extraction Phase took {0} ms.", sw.ElapsedMilliseconds);
                Console.WriteLine("[XYZ Join]: Total Rows: {0}.", joinOutputFinal.Count);

            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void ParallelXYZJoin()
        {
            try
            {
                var dateHashTable = new Dictionary<int, string>();
                var customerHashTable = new Dictionary<int, string>();
                var supplierHashTable = new Dictionary<int, string>();
                Stopwatch sw = new Stopwatch();

                #region Key Hashing Phase
                sw.Start();
                Parallel.Invoke(po, () =>
                {
                    List<Date> dateDimension = Utils.ReadFromBinaryFile<List<Date>>(dateFile);
                    foreach (var row in dateDimension)
                    {
                        if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
                            dateHashTable.Add(row.dDateKey, row.dYear);
                    }
                    dateDimension.Clear();
                },
                () =>
                {
                    List<Customer> customerDimension = Utils.ReadFromBinaryFile<List<Customer>>(customerFile);
                    foreach (var row in customerDimension)
                    {
                        if (row.cRegion.Equals("ASIA"))
                            customerHashTable.Add(row.cCustKey, row.cNation);
                    }
                    customerDimension.Clear();
                },
                () =>
                {
                    List<Supplier> supplierDimension = Utils.ReadFromBinaryFile<List<Supplier>>(supplierFile);
                    foreach (var row in supplierDimension)
                    {
                        if (row.sRegion.Equals("ASIA"))
                            supplierHashTable.Add(row.sSuppKey, row.sNation);
                    }
                    supplierDimension.Clear();
                });
                sw.Stop();
                #endregion Key Hashing Phase

                Console.WriteLine("[Parallel XYZ Join]: Key Hashing Phase took {0} ms.", sw.ElapsedMilliseconds);
                sw.Reset();

                #region Probing Phase
                sw.Start();
                var matchedOrderDate = new Dictionary<int, string>();
                var matchedCustomerKey = new Dictionary<int, string>();
                var matchedSupplierKey = new Dictionary<int, string>();
                Parallel.Invoke(po, () =>
                 {
                     List<int> loOrderDate = Utils.ReadFromBinaryFile<List<int>>(loOrderDateFile);

                     var i = 0;
                     foreach (var orderDate in loOrderDate)
                     {
                         if (dateHashTable.ContainsKey(orderDate))
                         {
                             string dYear = "";
                             dateHashTable.TryGetValue(orderDate, out dYear);
                             matchedOrderDate.Add(i + 1, dYear);
                         }
                         i++;
                     }
                     loOrderDate.Clear();
                 },
                () =>
                {
                    List<int> loCustomerKey = Utils.ReadFromBinaryFile<List<int>>(loCustKeyFile);
                    var j = 0;
                    foreach (var custKey in loCustomerKey)
                    {
                        if (customerHashTable.ContainsKey(custKey))
                        {
                            string cNation = string.Empty;
                            customerHashTable.TryGetValue(custKey, out cNation);
                            matchedCustomerKey.Add(j + 1, cNation);
                        }
                        j++;
                    }
                    loCustomerKey.Clear();
                },
                () =>
                {
                    List<int> loSupplierKey = Utils.ReadFromBinaryFile<List<int>>(loSuppKeyFile);
                    var k = 0;
                    foreach (var suppKey in loSupplierKey)
                    {
                        if (supplierHashTable.ContainsKey(suppKey))
                        {
                            string sNation = string.Empty;
                            supplierHashTable.TryGetValue(suppKey, out sNation);
                            matchedSupplierKey.Add(k + 1, sNation);
                        }
                        k++;
                    }
                    loSupplierKey.Clear();
                });

                // Left Bushy Tree
                var joinOutputIntermediate1 = matchedSupplierKey.Where(x => matchedCustomerKey.ContainsKey(x.Key))
                    .ToDictionary(x => x.Key, x => x.Value + "," + matchedCustomerKey[x.Key]);

                var joinOutputIntermediate2 = joinOutputIntermediate1.Where(x => matchedOrderDate.ContainsKey(x.Key))
                    .ToDictionary(x => x.Key, x => x.Value + "," + matchedOrderDate[x.Key]);
                sw.Stop();
                #endregion Probing Phase

                Console.WriteLine("[Parallel XYZ Join]: Key Hashing Phase took {0} ms.", sw.ElapsedMilliseconds);
                sw.Reset();

                #region Value Extraction Phase
                sw.Start();
                List<int> loRevenue = Utils.ReadFromBinaryFile<List<int>>(loRevenueFile);

                Dictionary<int, string> joinOutputFinal = new Dictionary<int, string>();
                foreach (var item in joinOutputIntermediate2)
                {
                    joinOutputFinal.Add(item.Key, item.Value + "," + loRevenue[item.Key]);
                }

                sw.Stop();
                #endregion Value Extraction Phase
                Console.WriteLine("[Parallel XYZ Join]: Key Hashing Phase took {0} ms.", sw.ElapsedMilliseconds);
                Console.WriteLine("[Parallel XYZ Join]: Total Rows {0}.", joinOutputFinal.Count);

            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void InvisibleJoin()
        {
            try
            {
                var dateHashTable = new Dictionary<int, string>();
                var customerHashTable = new Dictionary<int, string>();
                var supplierHashTable = new Dictionary<int, string>();
                Stopwatch sw = new Stopwatch();
                #region Key Hashing Phase
                sw.Start();
                List<Date> dateDimension = Utils.ReadFromBinaryFile<List<Date>>(dateFile);
                foreach (var row in dateDimension)
                {
                    if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
                        dateHashTable.Add(row.dDateKey, row.dYear);
                }
                dateDimension.Clear();

                List<Customer> customerDimension = Utils.ReadFromBinaryFile<List<Customer>>(customerFile);
                foreach (var row in customerDimension)
                {
                    if (row.cRegion.Equals("ASIA"))
                        customerHashTable.Add(row.cCustKey, row.cNation);
                }
                customerDimension.Clear();

                List<Supplier> supplierDimension = Utils.ReadFromBinaryFile<List<Supplier>>(supplierFile);
                foreach (var row in supplierDimension)
                {
                    if (row.sRegion.Equals("ASIA"))
                        supplierHashTable.Add(row.sSuppKey, row.sNation);
                }
                supplierDimension.Clear();
                ///sw.Stop();
                #endregion Key Hashing Phase

                //Console.WriteLine("[Invisble Join]: Key Hashing Phase took {0} ms.", sw.ElapsedMilliseconds);
                //sw.Reset();

                #region Probing Phase
                //              sw.Start();
                //            Stopwatch sw1 = new Stopwatch();
                //          sw1.Start();
                List<int> loOrderDate = Utils.ReadFromBinaryFiles<int>(binaryFilesDirectory + loOrderDateFile);

                var arraySize = loOrderDate.Count;
                BitArray baOrderDate = new BitArray(arraySize);
                var i = 0;
                foreach (var orderDate in loOrderDate)
                {
                    string date;
                    if (dateHashTable.TryGetValue(orderDate, out date))
                        baOrderDate.Set(i, true);
                    i++;
                }
                loOrderDate.Clear();

                //        sw1.Stop();
                //  Console.WriteLine("[Invisble Join]: Probing Phase (baOrderDate) took {0} ms.", sw1.ElapsedMilliseconds);
                ////      sw1.Reset();
                //sw1.Start();

                List<int> loCustomerKey = Utils.ReadFromBinaryFiles<int>(binaryFilesDirectory + loCustKeyFile);
                BitArray baCustomerKey = new BitArray(arraySize);
                var j = 0;
                foreach (var custKey in loCustomerKey)
                {
                    string custNation;
                    if (customerHashTable.TryGetValue(custKey, out custNation))
                        baCustomerKey.Set(j, true);
                    j++;
                }
                loCustomerKey.Clear();

                //sw1.Stop();
                ///Console.WriteLine("[Invisble Join]: Probing Phase (baCustomerKey) took {0} ms.", sw1.ElapsedMilliseconds);
                //sw1.Reset();
                //sw1.Start();

                List<int> loSupplierKey = Utils.ReadFromBinaryFiles<int>(binaryFilesDirectory + loSuppKeyFile);
                BitArray baSupplierKey = new BitArray(arraySize);
                var k = 0;
                foreach (var suppKey in loSupplierKey)
                {
                    string suppNation;
                    if (supplierHashTable.TryGetValue(suppKey, out suppNation))
                        baSupplierKey.Set(k, true);
                    k++;
                }
                loSupplierKey.Clear();

                //sw1.Stop();
                //Console.WriteLine("[Invisble Join]: Probing Phase (baSupplierKey) took {0} ms.", sw1.ElapsedMilliseconds);
                //sw1.Reset();
                //                sw1.Start();

                baCustomerKey.And(baSupplierKey);
                baCustomerKey.And(baOrderDate);

                //              sw1.Stop();
                //            Console.WriteLine("[Invisble Join]: Probing Phase (BITWISE AND) took {0} ms.", sw1.ElapsedMilliseconds);
                //          sw.Stop();
                #endregion Probing Phase

                //        Console.WriteLine("[Invisble Join]: Probing Phase took {0} ms.", sw.ElapsedMilliseconds);
                //      sw.Reset();

                #region Value Extraction Phase
                //    sw.Start();
                loOrderDate = Utils.ReadFromBinaryFiles<int>(binaryFilesDirectory + loOrderDateFile);
                loCustomerKey = Utils.ReadFromBinaryFiles<int>(binaryFilesDirectory + loCustKeyFile);
                loSupplierKey = Utils.ReadFromBinaryFiles<int>(binaryFilesDirectory + loSuppKeyFile);

                List<string> cNation = Utils.ReadFromBinaryFiles<string>(binaryFilesDirectory + cNationFile);
                List<string> sNation = Utils.ReadFromBinaryFiles<string>(binaryFilesDirectory + sNationFile);
                // Use Date Hash Table
                List<int> loRevenue = Utils.ReadFromBinaryFiles<int>(binaryFilesDirectory + loRevenueFile);

                var joinOutputIntermediate = new Dictionary<int, string>();
                var l = 0;
                foreach (bool bitValue in baCustomerKey)
                {
                    if (bitValue)
                    {
                        try
                        {
                            var dateKey = loOrderDate[l];
                            var custKey = loCustomerKey[l];
                            var suppKey = loSupplierKey[l];

                            // Position Look UP
                            string dYear;
                            dateHashTable.TryGetValue(dateKey, out dYear);

                            string cNationOut = cNation[custKey];
                            string sNationOut = sNation[suppKey-1];
                            joinOutputIntermediate.Add(l, cNationOut + "," + sNationOut + "," + dYear);
                        }
                        catch (Exception)
                        {
                            throw;
                        }

                    }
                    l++;
                }

                var joinOutputFinal = new Dictionary<int, string>();
                foreach (var item in joinOutputIntermediate)
                {
                    joinOutputFinal.Add(item.Key, item.Value + "," + loRevenue[item.Key]);
                }
                //sw.Stop();
                //Console.WriteLine("[Invisble Join]: Value Extraction Phase took {0} ms.", sw.ElapsedMilliseconds);
                #endregion Value Extraction Phase
                sw.Stop();
                Console.WriteLine("[Invisble Join]: Total Time {0} secs.", sw.ElapsedMilliseconds / 1000);
                Console.WriteLine("[Invisble Join]: Total Rows {0}.", joinOutputFinal.Count);
            }
            catch (Exception ex)
            {

                throw;
            }
        }

        public void ParallelInvisibleJoin()
        {

            try
            {
                var dateHashTable = new Dictionary<int, string>();
                var customerHashTable = new Dictionary<int, string>();
                var supplierHashTable = new Dictionary<int, string>();
                Stopwatch sw = new Stopwatch();
                #region Key Hashing Phase
                sw.Start();
                Parallel.Invoke(po, () =>
                {
                    List<Date> dateDimension = Utils.ReadFromBinaryFile<List<Date>>(dateFile);
                    foreach (var row in dateDimension)
                    {
                        if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
                            dateHashTable.Add(row.dDateKey, row.dYear);
                    }
                    dateDimension.Clear();
                },
               () =>
               {
                   List<Customer> customerDimension = Utils.ReadFromBinaryFile<List<Customer>>(customerFile);
                   foreach (var row in customerDimension)
                   {
                       if (row.cRegion.Equals("ASIA"))
                           customerHashTable.Add(row.cCustKey, row.cNation);
                   }
                   customerDimension.Clear();
               },
               () =>
               {
                   List<Supplier> supplierDimension = Utils.ReadFromBinaryFile<List<Supplier>>(supplierFile);
                   foreach (var row in supplierDimension)
                   {
                       if (row.sRegion.Equals("ASIA"))
                           supplierHashTable.Add(row.sSuppKey, row.sNation);
                   }
                   supplierDimension.Clear();
               });
                sw.Stop();
                #endregion Key Hashing Phase

                Console.WriteLine("[Parallel Invisible Join]: Key Hashing Phase took {0} ms.", sw.ElapsedMilliseconds);
                sw.Reset();

                #region Probing Phase
                sw.Start();
                List<int> loOrderDate = null;
                List<int> loCustomerKey = null;
                List<int> loSupplierKey = null;
                BitArray baCustomerKey = null;
                BitArray baOrderDate = null;
                BitArray baSupplierKey = null;

                Parallel.Invoke(po, () =>
                {
                    loOrderDate = Utils.ReadFromBinaryFile<List<int>>(loOrderDateFile);
                    baOrderDate = new BitArray(loOrderDate.Count);
                    var i = 0;
                    foreach (var orderDate in loOrderDate)
                    {
                        if (dateHashTable.ContainsKey(orderDate))
                            baOrderDate.Set(i, true);
                        i++;
                    }
                    loOrderDate.Clear();
                },
                () =>
                {
                    loCustomerKey = Utils.ReadFromBinaryFile<List<int>>(loCustKeyFile);
                    baCustomerKey = new BitArray(loCustomerKey.Count);
                    var j = 0;
                    foreach (var custKey in loCustomerKey)
                    {
                        if (customerHashTable.ContainsKey(custKey))
                            baCustomerKey.Set(j, true);
                        j++;
                    }
                    loCustomerKey.Clear();
                },
                () =>
                {
                    loSupplierKey = Utils.ReadFromBinaryFile<List<int>>(loSuppKeyFile);
                    baSupplierKey = new BitArray(loSupplierKey.Count);
                    var k = 0;
                    foreach (var suppKey in loSupplierKey)
                    {
                        if (supplierHashTable.ContainsKey(suppKey))
                            baSupplierKey.Set(k, true);
                        k++;
                    }
                    loSupplierKey.Clear();
                });

                baCustomerKey.And(baSupplierKey);
                baCustomerKey.And(baOrderDate);

                sw.Stop();
                #endregion Probing Phase

                Console.WriteLine("[Parallel Invisible Join]: Probing Phase took {0} ms.", sw.ElapsedMilliseconds);
                sw.Reset();

                #region Value Extraction Phase

                sw.Start();
                loOrderDate = Utils.ReadFromBinaryFile<List<int>>(loOrderDateFile);
                loCustomerKey = Utils.ReadFromBinaryFile<List<int>>(loCustKeyFile);
                loSupplierKey = Utils.ReadFromBinaryFile<List<int>>(loSuppKeyFile);

                List<string> cNation = Utils.ReadFromBinaryFile<List<string>>(cNationFile);
                List<string> sNation = Utils.ReadFromBinaryFile<List<string>>(sNationFile);
                // Use Date Hash Table
                List<int> loRevenue = Utils.ReadFromBinaryFile<List<int>>(loRevenueFile);

                var joinOutputIntermediate = new Dictionary<int, string>();
                var l = 0;
                foreach (bool bitValue in baCustomerKey)
                {
                    if (bitValue)
                    {
                        var dateKey = loOrderDate[l];
                        var custKey = loCustomerKey[l];
                        var suppKey = loSupplierKey[l];

                        // Position Look UP
                        string dYear;
                        dateHashTable.TryGetValue(dateKey, out dYear);

                        string cNationOut = cNation[custKey];
                        string sNationOut = sNation[suppKey];
                        joinOutputIntermediate.Add(l, cNationOut + "," + sNationOut + "," + dYear);
                    }
                    l++;
                }

                var joinOutputFinal = new Dictionary<int, string>();
                foreach (var item in joinOutputIntermediate)
                {
                    joinOutputFinal.Add(item.Key, item.Value + "," + loRevenue[item.Key]);
                }

                sw.Stop();
                #endregion Value Extraction Phase

                Console.WriteLine("[Parallel Invisible Join]: Value Extraction Phase took {0} ms.", sw.ElapsedMilliseconds);
                Console.WriteLine("[Parallel Invisible Join]: Total Rows {0}.", joinOutputFinal.Count);
            }
            catch (Exception ex)
            {

                throw;
            }

        }

        public void loadColumns()
        {
            foreach (var file in Directory.EnumerateFiles(folderPath, "*.tbl"))
            {
                var fileName = Path.GetFileNameWithoutExtension(file);
                String[] allLines = File.ReadAllLines(file);

                if (fileName.Equals("customer"))
                {
                    foreach (var line in allLines)
                    {
                        var data = line.Split('|');
                        cCustKey.Add(Convert.ToInt32(data[0]));
                        cName.Add(data[1]);
                        cAddress.Add(data[2]);
                        cCity.Add(data[3]);
                        cNation.Add(data[4]);
                        cRegion.Add(data[5]);
                        cPhone.Add(data[6]);
                        cMktSegment.Add(data[7]);
                    }
                }
                else if (fileName.Equals("supplier"))
                {
                    foreach (var line in allLines)
                    {
                        var data = line.Split('|');
                        sSuppKey.Add(Convert.ToInt32(data[0]));
                        sName.Add(data[1]);
                        sAddress.Add(data[2]);
                        sCity.Add(data[3]);
                        sNation.Add(data[4]);
                        sRegion.Add(data[5]);
                        sPhone.Add(data[6]);
                    }
                }
                else if (fileName.Equals("part"))
                {
                    foreach (var line in allLines)
                    {
                        var data = line.Split('|');
                        pPartKey.Add(Convert.ToInt32(data[0]));
                        pName.Add(data[1]);
                        pMFGR.Add(data[2]);
                        pCategory.Add(data[3]);
                        pBrand.Add(data[4]);
                        pColor.Add(data[5]);
                        pType.Add(data[6]);
                        pSize.Add(Convert.ToInt16(data[7]));
                        pContainer.Add(data[8]);
                    }
                }
                else if (fileName.Equals("date"))
                {
                    foreach (var line in allLines)
                    {
                        var data = line.Split('|');
                        dDateKey.Add(Convert.ToInt32(data[0]));
                        dDate.Add(data[1]);
                        dDayOfWeek.Add(data[2]);
                        dMonth.Add(data[3]);
                        dYear.Add(Convert.ToInt16(data[4]));
                        dYearMonthNum.Add(Convert.ToInt32(data[5]));
                        dYearMonth.Add(data[6]);
                        dDayNumInWeek.Add(Convert.ToInt16(data[8]));
                        dDayNumInMonth.Add(Convert.ToInt16(data[8]));
                        dDayNumInYear.Add(Convert.ToInt16(data[9]));
                        dMonthNumInYear.Add(Convert.ToInt16(data[10]));
                        dWeekNumInYear.Add(Convert.ToInt16(data[11]));
                        dSellingSeason.Add(data[12]);
                        dLastDayInMonthFL.Add(Convert.ToInt16(data[13]));
                        dHolidayFL.Add(Convert.ToInt16(data[14]));
                        dWeekDayFL.Add(Convert.ToInt16(data[15]));
                        dDayNumInYear.Add(Convert.ToInt16(data[16]));
                        dLastDayInWeekFL.Add(Convert.ToInt16(data[13]));
                    }
                }
                else if (fileName.Equals("lineorder"))
                {
                    var i = 0;
                    foreach (var line in allLines)
                    {
                        var data = line.Split('|');
                        loOrderKey.Add(Convert.ToInt32(data[0]));
                        loLineNumber.Add(Convert.ToInt16(data[1]));
                        loCustKey.Add(Convert.ToInt32(data[2]));
                        loPartKey.Add(Convert.ToInt32(data[3]));
                        loSuppKey.Add(Convert.ToInt16(data[4]));
                        loOrderDate.Add(Convert.ToInt32(data[5]));
                        loOrderPriority.Add(data[6]);
                        loShipPriority.Add(Convert.ToChar(data[7]));
                        loQuantity.Add(Convert.ToInt16(data[8]));
                        loExtendedPrice.Add(Convert.ToInt32(data[9]));
                        loOrdTotalPrice.Add(Convert.ToInt32(data[10]));
                        loDiscount.Add(Convert.ToInt16(data[11]));
                        loRevenue.Add(Convert.ToInt32(data[12]));
                        loSupplyCost.Add(Convert.ToInt32(data[13]));
                        loTax.Add(Convert.ToInt16(data[14]));
                        loCommitDate.Add(Convert.ToInt32(data[15]));
                        loShipMode.Add(data[15]);

                        loDiscountWithId.Add(new Tuple<int, int>(i, Convert.ToInt16(data[11])));
                        loQuantityWithId.Add(new Tuple<int, int>(i, Convert.ToInt16(data[8])));
                        i++;
                    }
                }
            }
        }

        public void loadTables()
        {
            foreach (var file in Directory.EnumerateFiles(folderPath, "*.tbl"))
            {
                var fileName = Path.GetFileNameWithoutExtension(file);
                String[] allLines = File.ReadAllLines(file);
                if (fileName.Equals("customer"))
                {
                    foreach (var line in allLines)
                    {
                        var data = line.Split('|');
                        customer.Add(new Customer(Convert.ToInt32(data[0]), data[1], data[2], data[3], data[4], data[5], data[6], data[7]));
                    }
                }
                else if (fileName.Equals("supplier"))
                {
                    foreach (var line in allLines)
                    {
                        var data = line.Split('|');
                        supplier.Add(new Supplier(Convert.ToInt32(data[0]), data[1], data[2], data[3], data[4], data[5], data[6]));
                    }
                }
                else if (fileName.Equals("part"))
                {
                    foreach (var line in allLines)
                    {
                        var data = line.Split('|');
                        part.Add(new Part(Convert.ToInt32(data[0]), data[1], data[2], data[3], data[4], data[5], data[6], Convert.ToInt32(data[7]), data[8]));
                    }
                }
                else if (fileName.Equals("date"))
                {
                    foreach (var line in allLines)
                    {
                        var data = line.Split('|');
                        date.Add(new Date(Convert.ToInt32(data[0]), data[1], data[2], data[3],
                            data[4], Convert.ToInt32(data[5]), data[6], Convert.ToInt32(data[7]),
                            Convert.ToInt32(data[8]), Convert.ToInt32(data[9]), Convert.ToInt32(data[10]), Convert.ToInt32(data[11]),
                            data[12], Convert.ToInt32(data[13]), Convert.ToInt32(data[14]), Convert.ToInt32(data[15]), Convert.ToInt32(data[16])));
                    }
                }
                //else if (fileName.Equals("lineorder"))
                //{
                //    foreach (var line in allLines)
                //    {
                //        var data = line.Split('|');
                //        lineOrder.Add(new LineOrder(Convert.ToInt32(data[0]),
                //            Convert.ToInt16(data[1]),
                //            Convert.ToInt32(data[2]),
                //            Convert.ToInt32(data[3]),
                //            Convert.ToInt16(data[4]),
                //            Convert.ToInt32(data[5]),
                //            data[6],
                //            Convert.ToChar(data[7]),
                //            Convert.ToInt16(data[8]),
                //            Convert.ToInt32(data[9]),
                //            Convert.ToInt32(data[10]),
                //            Convert.ToInt16(data[11]),
                //            Convert.ToInt32(data[12]),
                //            Convert.ToInt32(data[13]),
                //            Convert.ToInt16(data[14]),
                //            Convert.ToInt32(data[15]),
                //            data[16]));
                //    }
                //}
            }
        }

        public void createBinaryFiles()
        {
            Console.WriteLine("Starting to Create Binary Files");
            Utils.WriteToBinaryFile<List<Date>>(dateFile, date);
            Utils.WriteToBinaryFile<List<Customer>>(customerFile, customer);
            Utils.WriteToBinaryFile<List<Supplier>>(supplierFile, supplier);
            Utils.WriteToBinaryFile<List<Part>>(partFile, part);

            Console.WriteLine("Creating Binary Files for tables complete.");
            Utils.WriteToBinaryFile<List<int>>(dDateKeyFile, dDateKey);
            Utils.WriteToBinaryFile<List<int>>(dYearFile, dYear);
            Utils.WriteToBinaryFile<List<int>>(dYearMonthNumFile, dYearMonthNum);
            Utils.WriteToBinaryFile<List<int>>(dDayNumInWeekFile, dDayNumInWeek);
            Utils.WriteToBinaryFile<List<int>>(dDayNumInMonthFile, dDayNumInMonth);
            Utils.WriteToBinaryFile<List<int>>(dDayNumInYearFile, dDayNumInYear);
            Utils.WriteToBinaryFile<List<int>>(dMonthNumInYearFile, dMonthNumInYear);
            Utils.WriteToBinaryFile<List<int>>(dWeekNumInYearFile, dWeekNumInYear);
            Utils.WriteToBinaryFile<List<int>>(dLastDayInWeekFLFile, dLastDayInWeekFL);
            Utils.WriteToBinaryFile<List<int>>(dLastDayInMonthFLFile, dLastDayInMonthFL);
            Utils.WriteToBinaryFile<List<int>>(dHolidayFLFile, dHolidayFL);
            Utils.WriteToBinaryFile<List<int>>(dWeekDayFLFile, dWeekDayFL);
            Utils.WriteToBinaryFile<List<string>>(dDateFile, dDate);
            Utils.WriteToBinaryFile<List<string>>(dDayOfWeekFile, dDayOfWeek);
            Utils.WriteToBinaryFile<List<string>>(dMonthFile, dMonth);
            Utils.WriteToBinaryFile<List<string>>(dYearMonthFile, dYearMonth);
            Utils.WriteToBinaryFile<List<string>>(dSellingSeasonFile, dSellingSeason);
            Console.WriteLine("Creating Binary Files for columns in DATE table complete.");

            Utils.WriteToBinaryFile<List<int>>(loOrderKeyFile, loOrderKey);
            Utils.WriteToBinaryFile<List<int>>(loLineNumberFile, loLineNumber);
            Utils.WriteToBinaryFile<List<int>>(loCustKeyFile, loCustKey);
            Utils.WriteToBinaryFile<List<int>>(loPartKeyFile, loPartKey);
            Utils.WriteToBinaryFile<List<int>>(loSuppKeyFile, loSuppKey);
            Utils.WriteToBinaryFile<List<int>>(loOrderDateFile, loOrderDate);
            Utils.WriteToBinaryFile<List<char>>(loShipPriorityFile, loShipPriority);
            Utils.WriteToBinaryFile<List<int>>(loQuantityFile, loQuantity);
            Utils.WriteToBinaryFile<List<int>>(loExtendedPriceFile, loExtendedPrice);
            Utils.WriteToBinaryFile<List<int>>(loOrdTotalPriceFile, loOrdTotalPrice);
            Utils.WriteToBinaryFile<List<int>>(loDiscountFile, loDiscount);
            Utils.WriteToBinaryFile<List<int>>(loSupplyCostFile, loSupplyCost);
            Utils.WriteToBinaryFile<List<int>>(loTaxFile, loTax);
            Utils.WriteToBinaryFile<List<int>>(loRevenueFile, loRevenue);
            Utils.WriteToBinaryFile<List<int>>(loCommitDateFile, loCommitDate);
            Utils.WriteToBinaryFile<List<string>>(loShipModeFile, loShipMode);
            Utils.WriteToBinaryFile<List<string>>(loOrderPriorityFile, loOrderPriority);
            Console.WriteLine("Creating Binary Files for columns in LINEORDER table complete.");

            Utils.WriteToBinaryFile<List<int>>(cCustKeyFile, cCustKey);
            Utils.WriteToBinaryFile<List<string>>(cNameFile, cName);
            Utils.WriteToBinaryFile<List<string>>(cAddressFile, cAddress);
            Utils.WriteToBinaryFile<List<string>>(cCityFile, cCity);
            Utils.WriteToBinaryFile<List<string>>(cNationFile, cNation);
            Utils.WriteToBinaryFile<List<string>>(cRegionFile, cRegion);
            Utils.WriteToBinaryFile<List<string>>(cPhoneFile, cPhone);
            Utils.WriteToBinaryFile<List<string>>(cMktSegmentFile, cMktSegment);
            Console.WriteLine("Creating Binary Files for columns in CUSTOMER table complete.");

            Utils.WriteToBinaryFile<List<int>>(sSuppKeyFile, sSuppKey);
            Utils.WriteToBinaryFile<List<string>>(sNameFile, sName);
            Utils.WriteToBinaryFile<List<string>>(sAddressFile, sAddress);
            Utils.WriteToBinaryFile<List<string>>(sCityFile, sCity);
            Utils.WriteToBinaryFile<List<string>>(sNationFile, sNation);
            Utils.WriteToBinaryFile<List<string>>(sRegionFile, sRegion);
            Utils.WriteToBinaryFile<List<string>>(sPhoneFile, sPhone);
            Console.WriteLine("Creating Binary Files for columns in SUPPLIER table complete.");

            Utils.WriteToBinaryFile<List<int>>(pSizeFile, pSize);
            Utils.WriteToBinaryFile<List<int>>(pPartKeyFile, pPartKey);
            Utils.WriteToBinaryFile<List<string>>(pNameFile, pName);
            Utils.WriteToBinaryFile<List<string>>(pMFGRFile, pMFGR);
            Utils.WriteToBinaryFile<List<string>>(pCategoryFile, pCategory);
            Utils.WriteToBinaryFile<List<string>>(pBrandFile, pBrand);
            Utils.WriteToBinaryFile<List<string>>(pColorFile, pColor);
            Utils.WriteToBinaryFile<List<string>>(pTypeFile, pType);
            Utils.WriteToBinaryFile<List<string>>(pContainerFile, pContainer);
            Console.WriteLine("Creating Binary Files for columns in PART table complete.");
        }

    }
}
