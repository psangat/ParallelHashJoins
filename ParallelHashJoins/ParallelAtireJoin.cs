using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParallelHashJoins
{
    class ParallelAtireJoin
    {
        private static string binaryFilesDirectory = @"C:\Raw_Data_Source_For_Test\SSBM - DBGEN\BF";
        private string scaleFactor { get; set; }

        public TestResults testResults = new TestResults();
        private ParallelOptions parallelOptions = null;
        public ParallelAtireJoin(string scaleFactor, int degreeOfParallelism = 1)
        {
            this.scaleFactor = scaleFactor;
            testResults.totalRAMAvailable = Utils.getAvailableRAM();
            parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = degreeOfParallelism };
        }

        ~ParallelAtireJoin()
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
        public void Query_2_1_IM(bool isLockFree = true)
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

                sw.Start();

                var partitionIndexes = Utils.getPartitionIndexes(loSupplierKey.Count, parallelOptions.MaxDegreeOfParallelism);
                List<Task<Atire>> tasks = new List<Task<Atire>>();
                foreach (var indexes in partitionIndexes)
                {
                    Task<Atire> t = Task<Atire>.Factory.StartNew(() =>
                    {
                        Atire atire = new Atire();
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
                                atire.Insert(atire, new List<string> { dYear, pBrand }, isLockFree, loRevenue[i]);
                            }
                        }
                        return atire;
                    });
                    tasks.Add(t);
                }

                Task.WaitAll(tasks.ToArray());
                // Global Aggregation [Serial]
                Atire mergedAtire = null;
                if (tasks.Count == 1) // Number of procs = 1
                {
                    mergedAtire = tasks[0].Result;
                }
                else
                {
                    for (int i = 0; i < tasks.Count - 1; i++)
                    {
                        mergedAtire = tasks[i].Result.MergeAtires(tasks[i].Result, tasks[i + 1].Result);
                    }
                }
                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PAtire] T1 Time: {0}", t1));
                Console.WriteLine(String.Format("[PAtire] Total Time: {0}", t0 + t1));

                mergedAtire.GetResults(mergedAtire);
                var results = mergedAtire.results;
                Console.WriteLine(String.Format("[PAtire] Total Count: {0}", results.Count));
                //System.IO.File.WriteAllLines(@"C:\Results\PAtireJoin.txt", results);
                Console.WriteLine();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void Query_2_2_IM(bool isLockFree = true)
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

                sw.Start();

                var partitionIndexes = Utils.getPartitionIndexes(loSupplierKey.Count, parallelOptions.MaxDegreeOfParallelism);
                List<Task<Atire>> tasks = new List<Task<Atire>>();
                foreach (var indexes in partitionIndexes)
                {
                    Task<Atire> t = Task<Atire>.Factory.StartNew(() =>
                    {
                        Atire atire = new Atire();
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
                                atire.Insert(atire, new List<string> { dYear, pBrand }, isLockFree, loRevenue[i]);
                            }
                        }
                        return atire;
                    });
                    tasks.Add(t);
                }

                Task.WaitAll(tasks.ToArray());
                // Global Aggregation [Serial]
                Atire mergedAtire = null;
                if (tasks.Count == 1) // Number of procs = 1
                {
                    mergedAtire = tasks[0].Result;
                }
                else
                {
                    for (int i = 0; i < tasks.Count - 1; i++)
                    {
                        mergedAtire = tasks[i].Result.MergeAtires(tasks[i].Result, tasks[i + 1].Result);
                    }
                }
                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PAtire] T1 Time: {0}", t1));
                Console.WriteLine(String.Format("[PAtire] Total Time: {0}", t0 + t1));

                mergedAtire.GetResults(mergedAtire);
                var results = mergedAtire.results;
                Console.WriteLine(String.Format("[PAtire] Total Count: {0}", results.Count));
                //System.IO.File.WriteAllLines(@"C:\Results\PAtireJoin.txt", results);
                Console.WriteLine();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void Query_2_3_IM(bool isLockFree = true)
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

                sw.Start();

                var partitionIndexes = Utils.getPartitionIndexes(loSupplierKey.Count, parallelOptions.MaxDegreeOfParallelism);
                List<Task<Atire>> tasks = new List<Task<Atire>>();
                foreach (var indexes in partitionIndexes)
                {
                    Task<Atire> t = Task<Atire>.Factory.StartNew(() =>
                    {
                        Atire atire = new Atire();
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
                                atire.Insert(atire, new List<string> { dYear, pBrand }, isLockFree, loRevenue[i]);
                            }
                        }
                        return atire;
                    });
                    tasks.Add(t);
                }

                Task.WaitAll(tasks.ToArray());
                // Global Aggregation [Serial]
                Atire mergedAtire = null;
                if (tasks.Count == 1) // Number of procs = 1
                {
                    mergedAtire = tasks[0].Result;
                }
                else
                {
                    for (int i = 0; i < tasks.Count - 1; i++)
                    {
                        mergedAtire = tasks[i].Result.MergeAtires(tasks[i].Result, tasks[i + 1].Result);
                    }
                }
                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PAtire] T1 Time: {0}", t1));
                Console.WriteLine(String.Format("[PAtire] Total Time: {0}", t0 + t1));

                mergedAtire.GetResults(mergedAtire);
                var results = mergedAtire.results;
                Console.WriteLine(String.Format("[PAtire] Total Count: {0}", results.Count));
                //System.IO.File.WriteAllLines(@"C:\Results\PAtireJoin.txt", results);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void Query_3_1_IM(bool isLockFree = true)
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
                #region Phase 1

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
                Console.WriteLine(String.Format("[PAtire] T0 Time: {0}", t0));
                #endregion Phase1

                sw.Start();

                // Local Aggregation 
                var partitionIndexes = Utils.getPartitionIndexes(loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);
                List<Task<Atire>> tasks = new List<Task<Atire>>();
                foreach (var indexes in partitionIndexes)
                {
                    Task<Atire> t = Task<Atire>.Factory.StartNew(() =>
                    {
                        Atire atire = new Atire();
                        for (int i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            int custKey = loCustomerKey[i];
                            int suppKey = loSupplierKey[i];
                            int dateKey = loOrderDate[i];
                            string custNation = string.Empty;
                            string suppNation = string.Empty;
                            string dYear = string.Empty;
                            if (customerHashTable.TryGetValue(custKey, out custNation) && supplierHashTable.TryGetValue(suppKey, out suppNation) && dateHashTable.TryGetValue(dateKey, out dYear))
                            {
                                atire.Insert(atire, new List<string> { custNation, suppNation, dYear }, isLockFree, loRevenue[i]);
                            }
                        }
                        return atire;
                    });
                    tasks.Add(t);
                }

                Task.WaitAll(tasks.ToArray());

                // Global Aggregation [Serial]
                Atire mergedAtire = null;
                if (tasks.Count == 1) // Number of procs = 1
                {
                    mergedAtire = tasks[0].Result;
                }
                else
                {
                    for (int i = 0; i < tasks.Count - 1; i++)
                    {
                        mergedAtire = tasks[i].Result.MergeAtires(tasks[i].Result, tasks[i + 1].Result);
                    }
                }
                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PAtire] T1 Time: {0}", t1));
                Console.WriteLine(String.Format("[PAtire] Total Time: {0}", t0 + t1));

                mergedAtire.GetResults(mergedAtire);
                var results = mergedAtire.results;
                Console.WriteLine(String.Format("[PAtire] Total Count: {0}", results.Count));
                //System.IO.File.WriteAllLines(@"C:\Results\PAtireJoin.txt", results);
                Console.WriteLine();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void Query_3_2_IM(bool isLockFree = true)
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
                #region Phase 1

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
                Console.WriteLine(String.Format("[PAtire] T0 Time: {0}", t0));
                #endregion Phase1

                sw.Start();

                // Local Aggregation 
                var partitionIndexes = Utils.getPartitionIndexes(loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);
                List<Task<Atire>> tasks = new List<Task<Atire>>();
                foreach (var indexes in partitionIndexes)
                {
                    Task<Atire> t = Task<Atire>.Factory.StartNew(() =>
                    {
                        Atire atire = new Atire();
                        for (int i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            int custKey = loCustomerKey[i];
                            int suppKey = loSupplierKey[i];
                            int dateKey = loOrderDate[i];
                            string custCity = string.Empty;
                            string suppCity = string.Empty;
                            string dYear = string.Empty;
                            if (customerHashTable.TryGetValue(custKey, out custCity) && supplierHashTable.TryGetValue(suppKey, out suppCity) && dateHashTable.TryGetValue(dateKey, out dYear))
                            {
                                atire.Insert(atire, new List<string> { custCity, suppCity, dYear }, isLockFree, loRevenue[i]);
                            }
                        }
                        return atire;
                    });
                    tasks.Add(t);
                }

                Task.WaitAll(tasks.ToArray());

                // Global Aggregation [Serial]
                Atire mergedAtire = null;
                if (tasks.Count == 1) // Number of procs = 1
                {
                    mergedAtire = tasks[0].Result;
                }
                else
                {
                    for (int i = 0; i < tasks.Count - 1; i++)
                    {
                        mergedAtire = tasks[i].Result.MergeAtires(tasks[i].Result, tasks[i + 1].Result);
                    }
                }
                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PAtire] T1 Time: {0}", t1));
                Console.WriteLine(String.Format("[PAtire] Total Time: {0}", t0 + t1));

                mergedAtire.GetResults(mergedAtire);
                var results = mergedAtire.results;
                Console.WriteLine(String.Format("[PAtire] Total Count: {0}", results.Count));
                //System.IO.File.WriteAllLines(@"C:\Results\PAtireJoin.txt", results);
                Console.WriteLine();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void Query_3_3_IM(bool isLockFree = true)
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
                #region Phase 1

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
                Console.WriteLine(String.Format("[PAtire] T0 Time: {0}", t0));
                #endregion Phase1

                sw.Start();

                // Local Aggregation 
                var partitionIndexes = Utils.getPartitionIndexes(loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);
                List<Task<Atire>> tasks = new List<Task<Atire>>();
                foreach (var indexes in partitionIndexes)
                {
                    Task<Atire> t = Task<Atire>.Factory.StartNew(() =>
                    {
                        Atire atire = new Atire();
                        for (int i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            int custKey = loCustomerKey[i];
                            int suppKey = loSupplierKey[i];
                            int dateKey = loOrderDate[i];
                            string custCity = string.Empty;
                            string suppCity = string.Empty;
                            string dYear = string.Empty;
                            if (customerHashTable.TryGetValue(custKey, out custCity) && supplierHashTable.TryGetValue(suppKey, out suppCity) && dateHashTable.TryGetValue(dateKey, out dYear))
                            {
                                atire.Insert(atire, new List<string> { custCity, suppCity, dYear }, isLockFree, loRevenue[i]);
                            }
                        }
                        return atire;
                    });
                    tasks.Add(t);
                }

                Task.WaitAll(tasks.ToArray());

                // Global Aggregation [Serial]
                Atire mergedAtire = null;
                if (tasks.Count == 1) // Number of procs = 1
                {
                    mergedAtire = tasks[0].Result;
                }
                else
                {
                    for (int i = 0; i < tasks.Count - 1; i++)
                    {
                        mergedAtire = tasks[i].Result.MergeAtires(tasks[i].Result, tasks[i + 1].Result);
                    }
                }
                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PAtire] T1 Time: {0}", t1));
                Console.WriteLine(String.Format("[PAtire] Total Time: {0}", t0 + t1));

                mergedAtire.GetResults(mergedAtire);
                var results = mergedAtire.results;
                Console.WriteLine(String.Format("[PAtire] Total Count: {0}", results.Count));
                //System.IO.File.WriteAllLines(@"C:\Results\PAtireJoin.txt", results);
                Console.WriteLine();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void Query_3_4_IM(bool isLockFree = true)
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
                #region Phase 1

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
                Console.WriteLine(String.Format("[PAtire] T0 Time: {0}", t0));
                #endregion Phase1

                sw.Start();

                // Local Aggregation 
                var partitionIndexes = Utils.getPartitionIndexes(loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);
                List<Task<Atire>> tasks = new List<Task<Atire>>();
                foreach (var indexes in partitionIndexes)
                {
                    Task<Atire> t = Task<Atire>.Factory.StartNew(() =>
                    {
                        Atire atire = new Atire();
                        for (int i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            int custKey = loCustomerKey[i];
                            int suppKey = loSupplierKey[i];
                            int dateKey = loOrderDate[i];
                            string custCity = string.Empty;
                            string suppCity = string.Empty;
                            string dYear = string.Empty;
                            if (customerHashTable.TryGetValue(custKey, out custCity) && supplierHashTable.TryGetValue(suppKey, out suppCity) && dateHashTable.TryGetValue(dateKey, out dYear))
                            {
                                atire.Insert(atire, new List<string> { custCity, suppCity, dYear }, isLockFree, loRevenue[i]);
                            }
                        }
                        return atire;
                    });
                    tasks.Add(t);
                }

                Task.WaitAll(tasks.ToArray());

                // Global Aggregation [Serial]
                Atire mergedAtire = null;
                if (tasks.Count == 1) // Number of procs = 1
                {
                    mergedAtire = tasks[0].Result;
                }
                else
                {
                    for (int i = 0; i < tasks.Count - 1; i++)
                    {
                        mergedAtire = tasks[i].Result.MergeAtires(tasks[i].Result, tasks[i + 1].Result);
                    }
                }
                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PAtire] T1 Time: {0}", t1));
                Console.WriteLine(String.Format("[PAtire] Total Time: {0}", t0 + t1));

                mergedAtire.GetResults(mergedAtire);
                var results = mergedAtire.results;
                Console.WriteLine(String.Format("[PAtire] Total Count: {0}", results.Count));
                //System.IO.File.WriteAllLines(@"C:\Results\PAtireJoin.txt", results);
                Console.WriteLine();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void Query_4_1_IM(bool isLockFree = true)
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
                Console.WriteLine(String.Format("[PATire Join] T0 Time: {0}", t0));
                sw.Reset();
                #endregion Key Hashing Phase

                var partitionIndexes = Utils.getPartitionIndexes(loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);

                sw.Start();
                List<Task<Atire>> tasks = new List<Task<Atire>>();
                foreach (var indexes in partitionIndexes)
                {
                    Task<Atire> t = Task<Atire>.Factory.StartNew(() =>
                    {
                        Atire atire = new Atire();
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
                                atire.Insert(atire, new List<string> { dYear, custNation }, isLockFree, (loRevenue[i] - loSupplyCost[i]));
                            }
                        }
                        return atire;
                    });
                    tasks.Add(t);
                }

                Task.WaitAll(tasks.ToArray());

                // Global Aggregation [Serial]
                Atire mergedAtire = null;
                if (tasks.Count == 1) // Number of procs = 1
                {
                    mergedAtire = tasks[0].Result;
                }
                else
                {
                    for (int i = 0; i < tasks.Count - 1; i++)
                    {
                        mergedAtire = tasks[i].Result.MergeAtires(tasks[i].Result, tasks[i + 1].Result);
                    }
                }
                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PAtire] T1 Time: {0}", t1));
                Console.WriteLine(String.Format("[PAtire] Total Time: {0}", t0 + t1));

                mergedAtire.GetResults(mergedAtire);
                var results = mergedAtire.results;
                Console.WriteLine(String.Format("[PAtire] Total Count: {0}", results.Count));
                //System.IO.File.WriteAllLines(@"C:\Results\PAtireJoin.txt", results);
                Console.WriteLine();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void Query_4_2_IM(bool isLockFree = true)
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
                Console.WriteLine(String.Format("[PATire Join] T0 Time: {0}", t0));
                sw.Reset();
                #endregion Key Hashing Phase

                var _maat = new MAATIM(loSupplierKey.Count);
                var partitionIndexes = Utils.getPartitionIndexes(loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);
                
                sw.Start();
                List<Task<Atire>> tasks = new List<Task<Atire>>();

                foreach (var indexes in partitionIndexes)
                {
                    Task<Atire> t = Task<Atire>.Factory.StartNew(() =>
                    {
                        Atire atire = new Atire();
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
                                atire.Insert(atire, new List<string> { dYear, suppNation, pCategory }, isLockFree, (loRevenue[i] - loSupplyCost[i]));
                            }
                        }
                        return atire;
                    });
                    tasks.Add(t);
                }

                Task.WaitAll(tasks.ToArray());

                // Global Aggregation [Serial]
                Atire mergedAtire = null;
                if (tasks.Count == 1) // Number of procs = 1
                {
                    mergedAtire = tasks[0].Result;
                }
                else
                {
                    for (int i = 0; i < tasks.Count - 1; i++)
                    {
                        mergedAtire = tasks[i].Result.MergeAtires(tasks[i].Result, tasks[i + 1].Result);
                    }
                }
                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PAtire] T1 Time: {0}", t1));
                Console.WriteLine(String.Format("[PAtire] Total Time: {0}", t0 + t1));

                mergedAtire.GetResults(mergedAtire);
                var results = mergedAtire.results;
                Console.WriteLine(String.Format("[PAtire] Total Count: {0}", results.Count));
                //System.IO.File.WriteAllLines(@"C:\Results\PAtireJoin.txt", results);
                Console.WriteLine();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void Query_4_3_IM(bool isLockFree = true)
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

    
                var partitionIndexes = Utils.getPartitionIndexes(loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);

                sw.Start();
                List<Task<Atire>> tasks = new List<Task<Atire>>();
                foreach (var indexes in partitionIndexes)
                {
                    Task<Atire> t = Task<Atire>.Factory.StartNew(() =>
                    {
                        Atire atire = new Atire();
                        for (int i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            int custKey = loCustomerKey[i];
                            int dateKey = loOrderDate[i];
                            int suppKey = loSupplierKey[i];
                            int partKey = loPartKey[i];
                            string suppCity = string.Empty;
                            string dYear = string.Empty;
                            string pBrand = string.Empty;
                            if (customerHashTable.ContainsKey(custKey)
                            && dateHashTable.TryGetValue(dateKey, out dYear)
                            && supplierHashTable.TryGetValue(suppKey, out suppCity)
                            && partHashTable.TryGetValue(partKey, out pBrand))
                            {
                                atire.Insert(atire, new List<string> { dYear, suppCity, pBrand }, isLockFree, (loRevenue[i] - loSupplyCost[i]));
                            }
                        }
                        return atire;
                    });
                    tasks.Add(t);
                }

                Task.WaitAll(tasks.ToArray());

                // Global Aggregation [Serial]
                Atire mergedAtire = null;
                if (tasks.Count == 1) // Number of procs = 1
                {
                    mergedAtire = tasks[0].Result;
                }
                else
                {
                    for (int i = 0; i < tasks.Count - 1; i++)
                    {
                        mergedAtire = tasks[i].Result.MergeAtires(tasks[i].Result, tasks[i + 1].Result);
                    }
                }
                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PAtire] T1 Time: {0}", t1));
                Console.WriteLine(String.Format("[PAtire] Total Time: {0}", t0 + t1));

                mergedAtire.GetResults(mergedAtire);
                var results = mergedAtire.results;
                Console.WriteLine(String.Format("[PAtire] Total Count: {0}", results.Count));
                //System.IO.File.WriteAllLines(@"C:\Results\PAtireJoin.txt", results);
                Console.WriteLine();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public void saveAndPrintResults()
        {
            //TestResultsDatabase.nimbleJoinOutput.Add(testResults.toString());
            //Console.WriteLine("DGJoin: " + testResults.toString());
            //Console.WriteLine();
        }
    }
}
