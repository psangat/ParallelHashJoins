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
        private static readonly string binaryFilesDirectory = @"C:\Raw_Data_Source_For_Test\SSBM - DBGEN\BF";
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
            saveAndPrInt64Results();
        }

        #region Private Variables
        private readonly List<Int64> cCustKey = new List<Int64>();
        private readonly List<string> cName = new List<string>();
        private readonly List<string> cAddress = new List<string>();
        private readonly List<string> cCity = new List<string>();
        private readonly List<string> cNation = new List<string>();
        private readonly List<string> cRegion = new List<string>();
        private readonly List<string> cPhone = new List<string>();
        private readonly List<string> cMktSegment = new List<string>();

        private readonly List<Int64> sSuppKey = new List<Int64>();
        private readonly List<string> sName = new List<string>();
        private readonly List<string> sAddress = new List<string>();
        private readonly List<string> sCity = new List<string>();
        private readonly List<string> sNation = new List<string>();
        private readonly List<string> sRegion = new List<string>();
        private readonly List<string> sPhone = new List<string>();

        private readonly List<Int64> pSize = new List<Int64>();
        private readonly List<Int64> pPartKey = new List<Int64>();
        private readonly List<string> pName = new List<string>();
        private readonly List<string> pMFGR = new List<string>();
        private readonly List<string> pCategory = new List<string>();
        private readonly List<string> pBrand = new List<string>();
        private readonly List<string> pColor = new List<string>();
        private readonly List<string> pType = new List<string>();
        private readonly List<string> pContainer = new List<string>();

        private readonly List<Int64> loOrderKey = new List<Int64>();
        private readonly List<Int64> loLineNumber = new List<Int64>();
        private readonly List<Int64> loCustKey = new List<Int64>();
        private readonly List<Int64> loPartKey = new List<Int64>();
        private readonly List<Int64> loSuppKey = new List<Int64>();
        private readonly List<Int64> loOrderDate = new List<Int64>();
        private readonly List<char> loShipPriority = new List<char>();
        private readonly List<Int64> loQuantity = new List<Int64>();
        private readonly List<Tuple<Int64, Int64>> loQuantityWithId = new List<Tuple<Int64, Int64>>();

        private readonly List<Int64> loExtendedPrice = new List<Int64>();
        private readonly List<Int64> loOrdTotalPrice = new List<Int64>();
        private readonly List<Int64> loDiscount = new List<Int64>();
        private readonly List<Tuple<Int64, Int64>> loDiscountWithId = new List<Tuple<Int64, Int64>>();
        private readonly List<Int64> loRevenue = new List<Int64>();
        private readonly List<Int64> loSupplyCost = new List<Int64>();
        private readonly List<Int64> loTax = new List<Int64>();
        private readonly List<Int64> loCommitDate = new List<Int64>();
        private readonly List<string> loShipMode = new List<string>();
        private readonly List<string> loOrderPriority = new List<string>();

        private readonly List<Int64> dDateKey = new List<Int64>();
        private readonly List<Int64> dYear = new List<Int64>();
        private readonly List<Int64> dYearMonthNum = new List<Int64>();
        private readonly List<Int64> dDayNumInWeek = new List<Int64>();
        private readonly List<Int64> dDayNumInMonth = new List<Int64>();
        private readonly List<Int64> dDayNumInYear = new List<Int64>();
        private readonly List<Int64> dMonthNumInYear = new List<Int64>();
        private readonly List<Int64> dWeekNumInYear = new List<Int64>();
        private readonly List<Int64> dLastDayInWeekFL = new List<Int64>();
        private readonly List<Int64> dLastDayInMonthFL = new List<Int64>();
        private readonly List<Int64> dHolidayFL = new List<Int64>();
        private readonly List<Int64> dWeekDayFL = new List<Int64>();
        private readonly List<string> dDate = new List<string>();
        private readonly List<string> dDayOfWeek = new List<string>();
        private readonly List<string> dMonth = new List<string>();
        private readonly List<string> dYearMonth = new List<string>();
        private readonly List<string> dSellingSeason = new List<string>();

        private readonly List<Customer> customer = new List<Customer>();
        private readonly List<Supplier> supplier = new List<Supplier>();
        private readonly List<Part> part = new List<Part>();
        private readonly List<LineOrder> lineOrder = new List<LineOrder>();
        private readonly List<Date> date = new List<Date>();


        private string dateFile = binaryFilesDirectory + @"\date\";
        private string customerFile = binaryFilesDirectory + @"\customer\";
        private string supplierFile = binaryFilesDirectory + @"\supplier\";
        private string partFile = binaryFilesDirectory + @"\part\";

        private readonly string dDateKeyFile = binaryFilesDirectory + @"\dDateKey\";
        private readonly string dYearFile = binaryFilesDirectory + @"\dYear\";
        private readonly string dYearMonthNumFile = binaryFilesDirectory + @"\dYearMonthNum\";
        private readonly string dDayNumInWeekFile = binaryFilesDirectory + @"\dDayNumInWeek\";
        private readonly string dDayNumInMonthFile = binaryFilesDirectory + @"\dDayNumInMont\";
        private readonly string dDayNumInYearFile = binaryFilesDirectory + @"\dDayNumInYear\";
        private readonly string dMonthNumInYearFile = binaryFilesDirectory + @"\dMonthNumInYear\";
        private readonly string dWeekNumInYearFile = binaryFilesDirectory + @"\dWeekNumInYear\";
        private readonly string dLastDayInWeekFLFile = binaryFilesDirectory + @"\dLastDayInWeekFL\";
        private readonly string dLastDayInMonthFLFile = binaryFilesDirectory + @"\dLastDayInMonthFL\";
        private readonly string dHolidayFLFile = binaryFilesDirectory + @"\dHolidayFL\";
        private readonly string dWeekDayFLFile = binaryFilesDirectory + @"\dWeekDayFL\";
        private readonly string dDateFile = binaryFilesDirectory + @"\dDate\";
        private readonly string dDayOfWeekFile = binaryFilesDirectory + @"\dDayOfWeek\";
        private readonly string dMonthFile = binaryFilesDirectory + @"\dMonth\";
        private readonly string dYearMonthFile = binaryFilesDirectory + @"\dYearMonth\";
        private readonly string dSellingSeasonFile = binaryFilesDirectory + @"\dSellingSeason\";

        private readonly string loOrderKeyFile = binaryFilesDirectory + @"\loOrderKey\";
        private readonly string loLineNumberFile = binaryFilesDirectory + @"\loLineNumber\";
        private string loCustKeyFile = binaryFilesDirectory + @"\loCustKey\";
        private string loPartKeyFile = binaryFilesDirectory + @"\loPartKey\";
        private string loSuppKeyFile = binaryFilesDirectory + @"\loSuppKey\";
        private string loOrderDateFile = binaryFilesDirectory + @"\loOrderDate\";
        private readonly string loShipPriorityFile = binaryFilesDirectory + @"\loShipPriority\";
        private readonly string loQuantityFile = binaryFilesDirectory + @"\loQuantity\";
        private readonly string loExtendedPriceFile = binaryFilesDirectory + @"\loExtendedPrice\";
        private readonly string loOrdTotalPriceFile = binaryFilesDirectory + @"\loOrdTotalPrice\";
        private readonly string loDiscountFile = binaryFilesDirectory + @"\loDiscount\";
        private string loRevenueFile = binaryFilesDirectory + @"\loRevenue\";
        private string loSupplyCostFile = binaryFilesDirectory + @"\loSupplyCost\";
        private readonly string loTaxFile = binaryFilesDirectory + @"\loTax\";
        private readonly string loCommitDateFile = binaryFilesDirectory + @"\loCommitDate\";
        private readonly string loShipModeFile = binaryFilesDirectory + @"\loShipMode\";
        private readonly string loOrderPriorityFile = binaryFilesDirectory + @"\loOrderPriority\";

        private readonly string cCustKeyFile = binaryFilesDirectory + @"\cCustKey\";
        private readonly string cNameFile = binaryFilesDirectory + @"\cName\";
        private readonly string cAddressFile = binaryFilesDirectory + @"\cAddress\";
        private readonly string cCityFile = binaryFilesDirectory + @"\cCity\";
        private readonly string cNationFile = binaryFilesDirectory + @"\cNation\";
        private readonly string cRegionFile = binaryFilesDirectory + @"\cRegion\";
        private readonly string cPhoneFile = binaryFilesDirectory + @"\cPhone\";
        private readonly string cMktSegmentFile = binaryFilesDirectory + @"\cMktSegment\";

        private readonly string sSuppKeyFile = binaryFilesDirectory + @"\sSuppKey\";
        private readonly string sNameFile = binaryFilesDirectory + @"\sName\";
        private readonly string sAddressFile = binaryFilesDirectory + @"\sAddress\";
        private readonly string sCityFile = binaryFilesDirectory + @"\sCity\";
        private readonly string sNationFile = binaryFilesDirectory + @"\sNation\";
        private readonly string sRegionFile = binaryFilesDirectory + @"\sRegion\";
        private readonly string sPhoneFile = binaryFilesDirectory + @"\sPhone\";

        private readonly string pSizeFile = binaryFilesDirectory + @"\pSize\";
        private readonly string pPartKeyFile = binaryFilesDirectory + @"\pPartKey\";
        private readonly string pNameFile = binaryFilesDirectory + @"\pName\";
        private readonly string pMFGRFile = binaryFilesDirectory + @"\pMFGR\";
        private readonly string pCategoryFile = binaryFilesDirectory + @"\pCategory\";
        private readonly string pBrandFile = binaryFilesDirectory + @"\pBrand\";
        private readonly string pColorFile = binaryFilesDirectory + @"\pColor\";
        private readonly string pTypeFile = binaryFilesDirectory + @"\pType\";
        private readonly string pContainerFile = binaryFilesDirectory + @"\pContainer\";

        #endregion Private Variables

        public void Query_2_1_IM(bool isLockFree = true)
        {
            try
            {
                Stopwatch sw = new Stopwatch();

                List<Part> partDimension = Utils.ReadFromBinaryFiles<Part>(partFile.Replace("BF", "BF" + scaleFactor));
                List<Supplier> supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
                List<Date> dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loPartKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor));

                sw.Start();
                #region Key Hashing Phase 

                Dictionary<Int64, string> partHashTable = new Dictionary<Int64, string>();
                Dictionary<Int64, string> supplierHashTable = new Dictionary<Int64, string>();
                Dictionary<Int64, string> dateHashTable = new Dictionary<Int64, string>();

                Parallel.Invoke(parallelOptions,
                () =>
                {
                    foreach (Date row in dateDimension)
                    {
                        dateHashTable.Add(row.dDateKey, row.dYear);
                    }
                },
                () =>
                {
                    foreach (Part row in partDimension)
                    {
                        if (row.pCategory.Equals("MFGR#12"))
                        {
                            partHashTable.Add(row.pPartKey, row.pBrand);
                        }
                    }
                },
                () =>
                {
                    foreach (Supplier row in supplierDimension)
                    {
                        if (row.sRegion.Equals("AMERICA"))
                        {
                            supplierHashTable.Add(row.sSuppKey, row.sNation);
                        }
                    }
                });

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(string.Format("[PNimble Join] T0 Time: {0}", t0));
                sw.Reset();

                #endregion Key Hashing Phase

                sw.Start();

                List<Tuple<Int32, Int32>> partitionIndexes = Utils.getPartitionIndexes(loSupplierKey.Count, parallelOptions.MaxDegreeOfParallelism);
                List<Task<Atire>> tasks = new List<Task<Atire>>();
                foreach (Tuple<Int32, Int32> indexes in partitionIndexes)
                {
                    Task<Atire> t = Task<Atire>.Factory.StartNew(() =>
                    {
                        Atire atire = new Atire();
                        for (Int32 i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            Int64 partKey = loPartKey[i];
                            Int64 dateKey = loOrderDate[i];
                            Int64 suppKey = loSupplierKey[i];
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
                    for (Int32 i = 0; i < tasks.Count - 1; i++)
                    {
                        mergedAtire = tasks[i].Result.MergeAtires(tasks[i].Result, tasks[i + 1].Result);
                    }
                }
                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(string.Format("[PAtire] T1 Time: {0}", t1));
                Console.WriteLine(string.Format("[PAtire] Total Time: {0}", t0 + t1));

                mergedAtire.GetResults(mergedAtire);
                List<string> results = mergedAtire.results;
                Console.WriteLine(string.Format("[PAtire] Total Count: {0}", results.Count));
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
                List<Int64> loPartKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor));

                sw.Start();
                #region Key Hashing Phase 

                Dictionary<Int64, string> partHashTable = new Dictionary<Int64, string>();
                Dictionary<Int64, string> supplierHashTable = new Dictionary<Int64, string>();
                Dictionary<Int64, string> dateHashTable = new Dictionary<Int64, string>();

                Parallel.Invoke(parallelOptions,
               () =>
               {
                   foreach (Date row in dateDimension)
                   {
                       dateHashTable.Add(row.dDateKey, row.dYear);
                   }
               },
               () =>
               {
                   foreach (Part row in partDimension)
                   {
                       if (string.CompareOrdinal(row.pBrand, "MFGR#2221") >= 0 && string.CompareOrdinal(row.pBrand, "MFGR#2228") <= 0)
                       {
                           partHashTable.Add(row.pPartKey, row.pBrand);
                       }
                   }
               },
               () =>
               {
                   foreach (Supplier row in supplierDimension)
                   {
                       if (row.sRegion.Equals("ASIA"))
                       {
                           supplierHashTable.Add(row.sSuppKey, row.sNation);
                       }
                   }
               });

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(string.Format("[PNimble Join] T0 Time: {0}", t0));
                sw.Reset();

                #endregion Key Hashing Phase

                sw.Start();

                List<Tuple<Int32, Int32>> partitionIndexes = Utils.getPartitionIndexes(loSupplierKey.Count, parallelOptions.MaxDegreeOfParallelism);
                List<Task<Atire>> tasks = new List<Task<Atire>>();
                foreach (Tuple<Int32, Int32> indexes in partitionIndexes)
                {
                    Task<Atire> t = Task<Atire>.Factory.StartNew(() =>
                    {
                        Atire atire = new Atire();
                        for (Int32 i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            Int64 partKey = loPartKey[i];
                            Int64 dateKey = loOrderDate[i];
                            Int64 suppKey = loSupplierKey[i];
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
                    for (Int32 i = 0; i < tasks.Count - 1; i++)
                    {
                        mergedAtire = tasks[i].Result.MergeAtires(tasks[i].Result, tasks[i + 1].Result);
                    }
                }
                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(string.Format("[PAtire] T1 Time: {0}", t1));
                Console.WriteLine(string.Format("[PAtire] Total Time: {0}", t0 + t1));

                mergedAtire.GetResults(mergedAtire);
                List<string> results = mergedAtire.results;
                Console.WriteLine(string.Format("[PAtire] Total Count: {0}", results.Count));
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
                List<Int64> loPartKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor));

                sw.Start();
                #region Key Hashing Phase 

                Dictionary<Int64, string> partHashTable = new Dictionary<Int64, string>();
                Dictionary<Int64, string> supplierHashTable = new Dictionary<Int64, string>();
                Dictionary<Int64, string> dateHashTable = new Dictionary<Int64, string>();

                Parallel.Invoke(parallelOptions, () =>
                {
                    foreach (Date row in dateDimension)
                    {
                        dateHashTable.Add(row.dDateKey, row.dYear);
                    }
                },
                     () =>
                     {
                         foreach (Part row in partDimension)
                         {
                             if (row.pBrand.Equals("MFGR#2221"))
                             {
                                 partHashTable.Add(row.pPartKey, row.pBrand);
                             }
                         }
                     },
                     () =>
                     {
                         foreach (Supplier row in supplierDimension)
                         {
                             if (row.sRegion.Equals("EUROPE"))
                             {
                                 supplierHashTable.Add(row.sSuppKey, row.sNation);
                             }
                         }
                     });

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(string.Format("[PNimble Join] T0 Time: {0}", t0));
                sw.Reset();

                #endregion Key Hashing Phase

                sw.Start();

                List<Tuple<Int32, Int32>> partitionIndexes = Utils.getPartitionIndexes(loSupplierKey.Count, parallelOptions.MaxDegreeOfParallelism);
                List<Task<Atire>> tasks = new List<Task<Atire>>();
                foreach (Tuple<Int32, Int32> indexes in partitionIndexes)
                {
                    Task<Atire> t = Task<Atire>.Factory.StartNew(() =>
                    {
                        Atire atire = new Atire();
                        for (Int32 i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            Int64 partKey = loPartKey[i];
                            Int64 dateKey = loOrderDate[i];
                            Int64 suppKey = loSupplierKey[i];
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
                    for (Int32 i = 0; i < tasks.Count - 1; i++)
                    {
                        mergedAtire = tasks[i].Result.MergeAtires(tasks[i].Result, tasks[i + 1].Result);
                    }
                }
                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(string.Format("[PAtire] T1 Time: {0}", t1));
                Console.WriteLine(string.Format("[PAtire] Total Time: {0}", t0 + t1));

                mergedAtire.GetResults(mergedAtire);
                List<string> results = mergedAtire.results;
                Console.WriteLine(string.Format("[PAtire] Total Count: {0}", results.Count));
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
                List<Int64> loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor));

                sw.Start();
                #region Phase 1

                Dictionary<Int64, string> customerHashTable = new Dictionary<Int64, string>();
                Dictionary<Int64, string> supplierHashTable = new Dictionary<Int64, string>();
                Dictionary<Int64, string> dateHashTable = new Dictionary<Int64, string>();

                Parallel.Invoke(parallelOptions,
                () =>
                {
                    foreach (Date row in dateDimension)
                    {
                        if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
                        {
                            dateHashTable.Add(row.dDateKey, row.dYear);
                        }
                    }
                },
                () =>
                {
                    foreach (Customer row in customerDimension)
                    {
                        if (row.cRegion.Equals("ASIA"))
                        {
                            customerHashTable.Add(row.cCustKey, row.cNation);
                        }
                    }
                },
                () =>
                {
                    foreach (Supplier row in supplierDimension)
                    {
                        if (row.sRegion.Equals("ASIA"))
                        {
                            supplierHashTable.Add(row.sSuppKey, row.sNation);
                        }
                    }
                });

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(string.Format("[PAtire] T0 Time: {0}", t0));
                #endregion Phase1

                sw.Start();

                // Local Aggregation 
                List<Tuple<Int32, Int32>> partitionIndexes = Utils.getPartitionIndexes(loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);
                List<Task<Atire>> tasks = new List<Task<Atire>>();
                foreach (Tuple<Int32, Int32> indexes in partitionIndexes)
                {
                    Task<Atire> t = Task<Atire>.Factory.StartNew(() =>
                    {
                        Atire atire = new Atire();
                        for (Int32 i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            Int64 custKey = loCustomerKey[i];
                            Int64 suppKey = loSupplierKey[i];
                            Int64 dateKey = loOrderDate[i];
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
                    for (Int32 i = 0; i < tasks.Count - 1; i++)
                    {
                        mergedAtire = tasks[i].Result.MergeAtires(tasks[i].Result, tasks[i + 1].Result);
                    }
                }
                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(string.Format("[PAtire] T1 Time: {0}", t1));
                Console.WriteLine(string.Format("[PAtire] Total Time: {0}", t0 + t1));

                mergedAtire.GetResults(mergedAtire);
                List<string> results = mergedAtire.results;
                Console.WriteLine(string.Format("[PAtire] Total Count: {0}", results.Count));
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
                List<Int64> loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor));

                sw.Start();
                #region Phase 1

                Dictionary<Int64, string> customerHashTable = new Dictionary<Int64, string>();
                Dictionary<Int64, string> supplierHashTable = new Dictionary<Int64, string>();
                Dictionary<Int64, string> dateHashTable = new Dictionary<Int64, string>();

                Parallel.Invoke(parallelOptions,
                () =>
                {
                    foreach (Date row in dateDimension)
                    {
                        if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
                        {
                            dateHashTable.Add(row.dDateKey, row.dYear);
                        }
                    }
                },
                () =>
                {
                    foreach (Customer row in customerDimension)
                    {
                        if (row.cNation.Equals("UNITED STATES"))
                        {
                            customerHashTable.Add(row.cCustKey, row.cCity);
                        }
                    }
                },
                () =>
                {
                    foreach (Supplier row in supplierDimension)
                    {
                        if (row.sNation.Equals("UNITED STATES"))
                        {
                            supplierHashTable.Add(row.sSuppKey, row.sCity);
                        }
                    }
                });

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(string.Format("[PAtire] T0 Time: {0}", t0));
                #endregion Phase1

                sw.Start();

                // Local Aggregation 
                List<Tuple<Int32, Int32>> partitionIndexes = Utils.getPartitionIndexes(loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);
                List<Task<Atire>> tasks = new List<Task<Atire>>();
                foreach (Tuple<Int32, Int32> indexes in partitionIndexes)
                {
                    Task<Atire> t = Task<Atire>.Factory.StartNew(() =>
                    {
                        Atire atire = new Atire();
                        for (Int32 i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            Int64 custKey = loCustomerKey[i];
                            Int64 suppKey = loSupplierKey[i];
                            Int64 dateKey = loOrderDate[i];
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
                    for (Int32 i = 0; i < tasks.Count - 1; i++)
                    {
                        mergedAtire = tasks[i].Result.MergeAtires(tasks[i].Result, tasks[i + 1].Result);
                    }
                }
                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(string.Format("[PAtire] T1 Time: {0}", t1));
                Console.WriteLine(string.Format("[PAtire] Total Time: {0}", t0 + t1));

                mergedAtire.GetResults(mergedAtire);
                List<string> results = mergedAtire.results;
                Console.WriteLine(string.Format("[PAtire] Total Count: {0}", results.Count));
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
                List<Int64> loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor));

                sw.Start();
                #region Phase 1

                Dictionary<Int64, string> customerHashTable = new Dictionary<Int64, string>();
                Dictionary<Int64, string> supplierHashTable = new Dictionary<Int64, string>();
                Dictionary<Int64, string> dateHashTable = new Dictionary<Int64, string>();

                Parallel.Invoke(parallelOptions, () =>
                {
                    foreach (Date row in dateDimension)
                    {
                        if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
                        {
                            dateHashTable.Add(row.dDateKey, row.dYear);
                        }
                    }
                },
                 () =>
                 {
                     foreach (Customer row in customerDimension)
                     {
                         if (row.cCity.Equals("UNITED KI1") || row.cCity.Equals("UNITED KI5"))
                         {
                             customerHashTable.Add(row.cCustKey, row.cCity);
                         }
                     }
                 },
                 () =>
                 {
                     foreach (Supplier row in supplierDimension)
                     {
                         if (row.sCity.Equals("UNITED KI1") || row.sCity.Equals("UNITED KI5"))
                         {
                             supplierHashTable.Add(row.sSuppKey, row.sCity);
                         }
                     }
                 });

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(string.Format("[PAtire] T0 Time: {0}", t0));
                #endregion Phase1

                sw.Start();

                // Local Aggregation 
                List<Tuple<Int32, Int32>> partitionIndexes = Utils.getPartitionIndexes(loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);
                List<Task<Atire>> tasks = new List<Task<Atire>>();
                foreach (Tuple<Int32, Int32> indexes in partitionIndexes)
                {
                    Task<Atire> t = Task<Atire>.Factory.StartNew(() =>
                    {
                        Atire atire = new Atire();
                        for (Int32 i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            Int64 custKey = loCustomerKey[i];
                            Int64 suppKey = loSupplierKey[i];
                            Int64 dateKey = loOrderDate[i];
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
                    for (Int32 i = 0; i < tasks.Count - 1; i++)
                    {
                        mergedAtire = tasks[i].Result.MergeAtires(tasks[i].Result, tasks[i + 1].Result);
                    }
                }
                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(string.Format("[PAtire] T1 Time: {0}", t1));
                Console.WriteLine(string.Format("[PAtire] Total Time: {0}", t0 + t1));

                mergedAtire.GetResults(mergedAtire);
                List<string> results = mergedAtire.results;
                Console.WriteLine(string.Format("[PAtire] Total Count: {0}", results.Count));
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
                List<Int64> loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor));

                sw.Start();
                #region Phase 1

                Dictionary<Int64, string> customerHashTable = new Dictionary<Int64, string>();
                Dictionary<Int64, string> supplierHashTable = new Dictionary<Int64, string>();
                Dictionary<Int64, string> dateHashTable = new Dictionary<Int64, string>();

                Parallel.Invoke(parallelOptions, () =>
                {
                    foreach (Date row in dateDimension)
                    {
                        if (row.dYearMonth.Equals("Dec1997"))
                        {
                            dateHashTable.Add(row.dDateKey, row.dYear);
                        }
                    }
                },
                () =>
                {
                    foreach (Customer row in customerDimension)
                    {
                        if (row.cCity.Equals("UNITED KI1") || row.cCity.Equals("UNITED KI5"))
                        {
                            customerHashTable.Add(row.cCustKey, row.cCity);
                        }
                    }
                },
                () =>
                {
                    foreach (Supplier row in supplierDimension)
                    {
                        if (row.sCity.Equals("UNITED KI1") || row.sCity.Equals("UNITED KI5"))
                        {
                            supplierHashTable.Add(row.sSuppKey, row.sCity);
                        }
                    }
                });

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(string.Format("[PAtire] T0 Time: {0}", t0));
                #endregion Phase1

                sw.Start();

                // Local Aggregation 
                List<Tuple<Int32, Int32>> partitionIndexes = Utils.getPartitionIndexes(loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);
                List<Task<Atire>> tasks = new List<Task<Atire>>();
                foreach (Tuple<Int32, Int32> indexes in partitionIndexes)
                {
                    Task<Atire> t = Task<Atire>.Factory.StartNew(() =>
                    {
                        Atire atire = new Atire();
                        for (Int32 i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            Int64 custKey = loCustomerKey[i];
                            Int64 suppKey = loSupplierKey[i];
                            Int64 dateKey = loOrderDate[i];
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
                    for (Int32 i = 0; i < tasks.Count - 1; i++)
                    {
                        mergedAtire = tasks[i].Result.MergeAtires(tasks[i].Result, tasks[i + 1].Result);
                    }
                }
                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(string.Format("[PAtire] T1 Time: {0}", t1));
                Console.WriteLine(string.Format("[PAtire] Total Time: {0}", t0 + t1));

                mergedAtire.GetResults(mergedAtire);
                List<string> results = mergedAtire.results;
                Console.WriteLine(string.Format("[PAtire] Total Count: {0}", results.Count));
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

                List<Int64> loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loPartKey = Utils.ReadFromBinaryFiles<Int64>(loPartKeyFile.Replace("BF", "BF" + scaleFactor));

                List<Int64> loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loSupplyCost = Utils.ReadFromBinaryFiles<Int64>(loSupplyCostFile.Replace("BF", "BF" + scaleFactor));

                sw.Start();
                #region Key Hashing Phase 

                Dictionary<Int64, string> customerHashTable = new Dictionary<Int64, string>();
                Dictionary<Int64, string> supplierHashTable = new Dictionary<Int64, string>();
                Dictionary<Int64, string> dateHashTable = new Dictionary<Int64, string>();
                Dictionary<Int64, string> partHashTable = new Dictionary<Int64, string>();

                Parallel.Invoke(parallelOptions,
                () =>
                {
                    foreach (Date row in dateDimension)
                    {
                        dateHashTable.Add(row.dDateKey, row.dYear);
                    }
                },
                () =>
                {
                    foreach (Customer row in customerDimension)
                    {
                        if (row.cRegion.Equals("AMERICA"))
                        {
                            customerHashTable.Add(row.cCustKey, row.cNation);
                        }
                    }
                },
                () =>
                {
                    foreach (Supplier row in supplierDimension)
                    {
                        if (row.sRegion.Equals("AMERICA"))
                        {
                            supplierHashTable.Add(row.sSuppKey, row.sNation);
                        }
                    }
                },
                () =>
                {
                    foreach (Part row in partDimension)
                    {
                        if (row.pMFGR.Equals("MFGR#1") || row.pMFGR.Equals("MFGR#2"))
                        {
                            partHashTable.Add(row.pPartKey, row.pMFGR);
                        }
                    }
                });

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(string.Format("[PATire Join] T0 Time: {0}", t0));
                sw.Reset();
                #endregion Key Hashing Phase

                List<Tuple<Int32, Int32>> partitionIndexes = Utils.getPartitionIndexes(loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);

                sw.Start();
                List<Task<Atire>> tasks = new List<Task<Atire>>();
                foreach (Tuple<Int32, Int32> indexes in partitionIndexes)
                {
                    Task<Atire> t = Task<Atire>.Factory.StartNew(() =>
                    {
                        Atire atire = new Atire();
                        for (Int32 i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            Int64 custKey = loCustomerKey[i];
                            Int64 dateKey = loOrderDate[i];
                            Int64 suppKey = loSupplierKey[i];
                            Int64 partKey = loPartKey[i];
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
                    for (Int32 i = 0; i < tasks.Count - 1; i++)
                    {
                        mergedAtire = tasks[i].Result.MergeAtires(tasks[i].Result, tasks[i + 1].Result);
                    }
                }
                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(string.Format("[PAtire] T1 Time: {0}", t1));
                Console.WriteLine(string.Format("[PAtire] Total Time: {0}", t0 + t1));

                mergedAtire.GetResults(mergedAtire);
                List<string> results = mergedAtire.results;
                Console.WriteLine(string.Format("[PAtire] Total Count: {0}", results.Count));
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

                List<Int64> loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loPartKey = Utils.ReadFromBinaryFiles<Int64>(loPartKeyFile.Replace("BF", "BF" + scaleFactor));

                List<Int64> loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loSupplyCost = Utils.ReadFromBinaryFiles<Int64>(loSupplyCostFile.Replace("BF", "BF" + scaleFactor));

                sw.Start();
                #region Key Hashing Phase 

                Dictionary<Int64, string> customerHashTable = new Dictionary<Int64, string>();
                Dictionary<Int64, string> supplierHashTable = new Dictionary<Int64, string>();
                Dictionary<Int64, string> dateHashTable = new Dictionary<Int64, string>();
                Dictionary<Int64, string> partHashTable = new Dictionary<Int64, string>();

                Parallel.Invoke(parallelOptions,
               () =>
               {
                   foreach (Date row in dateDimension)
                   {
                       if (row.dYear.Equals("1997") || row.dYear.Equals("1998"))
                       {
                           dateHashTable.Add(row.dDateKey, row.dYear);
                       }
                   }
               },
               () =>
               {
                   foreach (Customer row in customerDimension)
                   {
                       if (row.cRegion.Equals("AMERICA"))
                       {
                           customerHashTable.Add(row.cCustKey, row.cNation);
                       }
                   }
               },
               () =>
               {
                   foreach (Supplier row in supplierDimension)
                   {
                       if (row.sRegion.Equals("AMERICA"))
                       {
                           supplierHashTable.Add(row.sSuppKey, row.sNation);
                       }
                   }
               },
               () =>
               {
                   foreach (Part row in partDimension)
                   {
                       if (row.pMFGR.Equals("MFGR#1") || row.pMFGR.Equals("MFGR#2"))
                       {
                           partHashTable.Add(row.pPartKey, row.pCategory);
                       }
                   }
               });

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(string.Format("[PATire Join] T0 Time: {0}", t0));
                sw.Reset();
                #endregion Key Hashing Phase

                MAATIM _maat = new MAATIM(loSupplierKey.Count);
                var partitionIndexes = Utils.getPartitionIndexes(loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);

                sw.Start();
                List<Task<Atire>> tasks = new List<Task<Atire>>();

                foreach (var indexes in partitionIndexes)
                {
                    Task<Atire> t = Task<Atire>.Factory.StartNew(() =>
                    {
                        Atire atire = new Atire();
                        for (Int32 i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            Int64 suppKey = loSupplierKey[i];
                            Int64 dateKey = loOrderDate[i];
                            Int64 partKey = loPartKey[i];
                            Int64 custKey = loCustomerKey[i];
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
                    for (Int32 i = 0; i < tasks.Count - 1; i++)
                    {
                        mergedAtire = tasks[i].Result.MergeAtires(tasks[i].Result, tasks[i + 1].Result);
                    }
                }
                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(string.Format("[PAtire] T1 Time: {0}", t1));
                Console.WriteLine(string.Format("[PAtire] Total Time: {0}", t0 + t1));

                mergedAtire.GetResults(mergedAtire);
                List<string> results = mergedAtire.results;
                Console.WriteLine(string.Format("[PAtire] Total Count: {0}", results.Count));
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

                List<Int64> loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loPartKey = Utils.ReadFromBinaryFiles<Int64>(loPartKeyFile.Replace("BF", "BF" + scaleFactor));

                List<Int64> loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
                List<Int64> loSupplyCost = Utils.ReadFromBinaryFiles<Int64>(loSupplyCostFile.Replace("BF", "BF" + scaleFactor));

                sw.Start();
                #region Key Hashing Phase 

                Dictionary<Int64, string> customerHashTable = new Dictionary<Int64, string>();
                Dictionary<Int64, string> supplierHashTable = new Dictionary<Int64, string>();
                Dictionary<Int64, string> dateHashTable = new Dictionary<Int64, string>();
                Dictionary<Int64, string> partHashTable = new Dictionary<Int64, string>();

                Parallel.Invoke(parallelOptions,
                () =>
                {
                    foreach (Date row in dateDimension)
                    {
                        if (row.dYear.Equals("1997") || row.dYear.Equals("1998"))
                        {
                            dateHashTable.Add(row.dDateKey, row.dYear);
                        }
                    }
                },
                () =>
                {
                    foreach (Customer row in customerDimension)
                    {
                        if (row.cRegion.Equals("AMERICA"))
                        {
                            customerHashTable.Add(row.cCustKey, row.cNation);
                        }
                    }
                },
                () =>
                {
                    foreach (Supplier row in supplierDimension)
                    {
                        if (row.sNation.Equals("UNITED STATES"))
                        {
                            supplierHashTable.Add(row.sSuppKey, row.sCity);
                        }
                    }
                },
                () =>
                {
                    foreach (Part row in partDimension)
                    {
                        if (row.pCategory.Equals("MFGR#14"))
                        {
                            partHashTable.Add(row.pPartKey, row.pBrand);
                        }
                    }
                });

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(string.Format("[PNimble Join] T0 Time: {0}", t0));
                sw.Reset();
                #endregion Key Hashing Phase


                List<Tuple<Int32, Int32>> partitionIndexes = Utils.getPartitionIndexes(loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);

                sw.Start();
                List<Task<Atire>> tasks = new List<Task<Atire>>();
                foreach (Tuple<Int32, Int32> indexes in partitionIndexes)
                {
                    Task<Atire> t = Task<Atire>.Factory.StartNew(() =>
                    {
                        Atire atire = new Atire();
                        for (Int32 i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            Int64 custKey = loCustomerKey[i];
                            Int64 dateKey = loOrderDate[i];
                            Int64 suppKey = loSupplierKey[i];
                            Int64 partKey = loPartKey[i];
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
                    for (Int32 i = 0; i < tasks.Count - 1; i++)
                    {
                        mergedAtire = tasks[i].Result.MergeAtires(tasks[i].Result, tasks[i + 1].Result);
                    }
                }
                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(string.Format("[PAtire] T1 Time: {0}", t1));
                Console.WriteLine(string.Format("[PAtire] Total Time: {0}", t0 + t1));

                mergedAtire.GetResults(mergedAtire);
                List<string> results = mergedAtire.results;
                Console.WriteLine(string.Format("[PAtire] Total Count: {0}", results.Count));
                //System.IO.File.WriteAllLines(@"C:\Results\PAtireJoin.txt", results);
                Console.WriteLine();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void saveAndPrInt64Results()
        {
            //TestResultsDatabase.nimbleJoinOutput.Add(testResults.toString());
            //Console.WriteLine("DGJoin: " + testResults.toString());
            //Console.WriteLine();
        }
    }
}
