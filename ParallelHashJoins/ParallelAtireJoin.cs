using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParallelHashJoins
{
    internal class ParallelAtireJoin
    {
        private static readonly string binaryFilesDirectory = @"C:\Raw_Data_Source_For_Test\SSBM - DBGEN\BF";
        private string scaleFactor { get; set; }
        private bool isLockFree { get; set; }
        private ParallelOptions parallelOptions = null;
        private TestResults testResults = new TestResults();

        public ParallelAtireJoin(string _scaleFactor, ParallelOptions _parallelOptions, bool _isLockFree = true)
        {
            scaleFactor = _scaleFactor;
            parallelOptions = _parallelOptions;
            isLockFree = _isLockFree;
        }

        ~ParallelAtireJoin()
        {
            saveAndPrintResults();
        }

        public void Query_2_1_IM(bool isLockFree = true)
        {
            try
            {
                Stopwatch sw = new Stopwatch();
                sw.Start();
                #region Key Hashing Phase 

                Dictionary<long, string> partHashTable = new Dictionary<long, string>();
                Dictionary<long, string> supplierHashTable = new Dictionary<long, string>();
                Dictionary<long, string> dateHashTable = new Dictionary<long, string>();

                Parallel.Invoke(parallelOptions,
                () =>
                {
                    foreach (Date row in InMemoryData.dateDimension)
                    {
                        dateHashTable.Add(row.dDateKey, row.dYear);
                    }
                },
                () =>
                {
                    foreach (Part row in InMemoryData.partDimension)
                    {
                        if (row.pCategory.Equals("MFGR#12"))
                        {
                            partHashTable.Add(row.pPartKey, row.pBrand);
                        }
                    }
                },
                () =>
                {
                    foreach (Supplier row in InMemoryData.supplierDimension)
                    {
                        if (row.sRegion.Equals("AMERICA"))
                        {
                            supplierHashTable.Add(row.sSuppKey, row.sNation);
                        }
                    }
                });

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(string.Format("[PAJ] T0 Time: {0}", t0));
                sw.Reset();

                #endregion Key Hashing Phase

                sw.Start();

                List<Tuple<int, int>> partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loSupplierKey.Count, parallelOptions.MaxDegreeOfParallelism);
                List<Task<Atire>> tasks = new List<Task<Atire>>();
                foreach (Tuple<int, int> indexes in partitionIndexes)
                {
                    Task<Atire> t = Task<Atire>.Factory.StartNew(() =>
                    {
                        Atire atire = new Atire();
                        for (int i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            long partKey = InMemoryData.loPartKey[i];
                            long dateKey = InMemoryData.loOrderDate[i];
                            long suppKey = InMemoryData.loSupplierKey[i];
                            string pBrand = string.Empty;
                            string dYear = string.Empty;
                            if (partHashTable.TryGetValue(partKey, out pBrand)
                            && dateHashTable.TryGetValue(dateKey, out dYear)
                            && supplierHashTable.ContainsKey(suppKey))
                            {
                                atire.Insert(atire, new List<string> { dYear, pBrand }, isLockFree, InMemoryData.loRevenue[i]);
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
                Console.WriteLine(string.Format("[PAJ] T1 Time: {0}", t1));
                Console.WriteLine(string.Format("[PAJ] Total Time: {0}", t0 + t1));

                mergedAtire.GetResults(mergedAtire);
                List<string> results = mergedAtire.results;
                Console.WriteLine(string.Format("[PAJ] Total Count: {0}", results.Count));
                //System.IO.File.WriteAllLines(@"C:\Results\PAJJoin.txt", results);
                Console.WriteLine();
                testResults.phase1Time = t0;
                testResults.phase2Time = t1;
                testResults.phase3Time = 0; ;
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

                sw.Start();
                #region Key Hashing Phase 

                Dictionary<long, string> partHashTable = new Dictionary<long, string>();
                Dictionary<long, string> supplierHashTable = new Dictionary<long, string>();
                Dictionary<long, string> dateHashTable = new Dictionary<long, string>();

                Parallel.Invoke(parallelOptions,
               () =>
               {
                   foreach (Date row in InMemoryData.dateDimension)
                   {
                       dateHashTable.Add(row.dDateKey, row.dYear);
                   }
               },
               () =>
               {
                   foreach (Part row in InMemoryData.partDimension)
                   {
                       if (string.CompareOrdinal(row.pBrand, "MFGR#2221") >= 0 && string.CompareOrdinal(row.pBrand, "MFGR#2228") <= 0)
                       {
                           partHashTable.Add(row.pPartKey, row.pBrand);
                       }
                   }
               },
               () =>
               {
                   foreach (Supplier row in InMemoryData.supplierDimension)
                   {
                       if (row.sRegion.Equals("ASIA"))
                       {
                           supplierHashTable.Add(row.sSuppKey, row.sNation);
                       }
                   }
               });

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(string.Format("[PAJ] T0 Time: {0}", t0));
                sw.Reset();

                #endregion Key Hashing Phase

                sw.Start();

                List<Tuple<int, int>> partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loSupplierKey.Count, parallelOptions.MaxDegreeOfParallelism);
                List<Task<Atire>> tasks = new List<Task<Atire>>();
                foreach (Tuple<int, int> indexes in partitionIndexes)
                {
                    Task<Atire> t = Task<Atire>.Factory.StartNew(() =>
                    {
                        Atire atire = new Atire();
                        for (int i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            long partKey = InMemoryData.loPartKey[i];
                            long dateKey = InMemoryData.loOrderDate[i];
                            long suppKey = InMemoryData.loSupplierKey[i];
                            string pBrand = string.Empty;
                            string dYear = string.Empty;
                            if (partHashTable.TryGetValue(partKey, out pBrand)
                            && dateHashTable.TryGetValue(dateKey, out dYear)
                            && supplierHashTable.ContainsKey(suppKey))
                            {
                                atire.Insert(atire, new List<string> { dYear, pBrand }, isLockFree, InMemoryData.loRevenue[i]);
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
                Console.WriteLine(string.Format("[PAJ] T1 Time: {0}", t1));
                Console.WriteLine(string.Format("[PAJ] Total Time: {0}", t0 + t1));

                mergedAtire.GetResults(mergedAtire);
                List<string> results = mergedAtire.results;
                Console.WriteLine(string.Format("[PAJ] Total Count: {0}", results.Count));
                //System.IO.File.WriteAllLines(@"C:\Results\PAJJoin.txt", results);
                Console.WriteLine();
                testResults.phase1Time = t0;
                testResults.phase2Time = t1;
                testResults.phase3Time = 0; ;
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

                sw.Start();
                #region Key Hashing Phase 

                Dictionary<long, string> partHashTable = new Dictionary<long, string>();
                Dictionary<long, string> supplierHashTable = new Dictionary<long, string>();
                Dictionary<long, string> dateHashTable = new Dictionary<long, string>();

                Parallel.Invoke(parallelOptions, () =>
                {
                    foreach (Date row in InMemoryData.dateDimension)
                    {
                        dateHashTable.Add(row.dDateKey, row.dYear);
                    }
                },
                     () =>
                     {
                         foreach (Part row in InMemoryData.partDimension)
                         {
                             if (row.pBrand.Equals("MFGR#2221"))
                             {
                                 partHashTable.Add(row.pPartKey, row.pBrand);
                             }
                         }
                     },
                     () =>
                     {
                         foreach (Supplier row in InMemoryData.supplierDimension)
                         {
                             if (row.sRegion.Equals("EUROPE"))
                             {
                                 supplierHashTable.Add(row.sSuppKey, row.sNation);
                             }
                         }
                     });

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(string.Format("[PAJ] T0 Time: {0}", t0));
                sw.Reset();

                #endregion Key Hashing Phase

                sw.Start();

                List<Tuple<int, int>> partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loSupplierKey.Count, parallelOptions.MaxDegreeOfParallelism);
                List<Task<Atire>> tasks = new List<Task<Atire>>();
                foreach (Tuple<int, int> indexes in partitionIndexes)
                {
                    Task<Atire> t = Task<Atire>.Factory.StartNew(() =>
                    {
                        Atire atire = new Atire();
                        for (int i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            long partKey = InMemoryData.loPartKey[i];
                            long dateKey = InMemoryData.loOrderDate[i];
                            long suppKey = InMemoryData.loSupplierKey[i];
                            string pBrand = string.Empty;
                            string dYear = string.Empty;
                            if (partHashTable.TryGetValue(partKey, out pBrand)
                            && dateHashTable.TryGetValue(dateKey, out dYear)
                            && supplierHashTable.ContainsKey(suppKey))
                            {
                                atire.Insert(atire, new List<string> { dYear, pBrand }, isLockFree, InMemoryData.loRevenue[i]);
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
                Console.WriteLine(string.Format("[PAJ] T1 Time: {0}", t1));
                Console.WriteLine(string.Format("[PAJ] Total Time: {0}", t0 + t1));

                mergedAtire.GetResults(mergedAtire);
                List<string> results = mergedAtire.results;
                Console.WriteLine(string.Format("[PAJ] Total Count: {0}", results.Count));
                //System.IO.File.WriteAllLines(@"C:\Results\PAJJoin.txt", results);
                Console.WriteLine();
                testResults.phase1Time = t0;
                testResults.phase2Time = t1;
                testResults.phase3Time = 0; ;
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

                sw.Start();
                #region Phase 1

                Dictionary<long, string> customerHashTable = new Dictionary<long, string>();
                Dictionary<long, string> supplierHashTable = new Dictionary<long, string>();
                Dictionary<long, string> dateHashTable = new Dictionary<long, string>();

                Parallel.Invoke(parallelOptions,
                () =>
                {
                    foreach (Date row in InMemoryData.dateDimension)
                    {
                        if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
                        {
                            dateHashTable.Add(row.dDateKey, row.dYear);
                        }
                    }
                },
                () =>
                {
                    foreach (Customer row in InMemoryData.customerDimension)
                    {
                        if (row.cRegion.Equals("ASIA"))
                        {
                            customerHashTable.Add(row.cCustKey, row.cNation);
                        }
                    }
                },
                () =>
                {
                    foreach (Supplier row in InMemoryData.supplierDimension)
                    {
                        if (row.sRegion.Equals("ASIA"))
                        {
                            supplierHashTable.Add(row.sSuppKey, row.sNation);
                        }
                    }
                });

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(string.Format("[PAJ] T0 Time: {0}", t0));
                #endregion Phase1

                sw.Start();

                // Local Aggregation 
                List<Tuple<int, int>> partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);
                List<Task<Atire>> tasks = new List<Task<Atire>>();
                foreach (Tuple<int, int> indexes in partitionIndexes)
                {
                    Task<Atire> t = Task<Atire>.Factory.StartNew(() =>
                    {
                        Atire atire = new Atire();
                        for (int i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            long custKey = InMemoryData.loCustomerKey[i];
                            long suppKey = InMemoryData.loSupplierKey[i];
                            long dateKey = InMemoryData.loOrderDate[i];
                            string custNation = string.Empty;
                            string suppNation = string.Empty;
                            string dYear = string.Empty;
                            if (customerHashTable.TryGetValue(custKey, out custNation) && supplierHashTable.TryGetValue(suppKey, out suppNation) && dateHashTable.TryGetValue(dateKey, out dYear))
                            {
                                atire.Insert(atire, new List<string> { custNation, suppNation, dYear }, isLockFree, InMemoryData.loRevenue[i]);
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
                Console.WriteLine(string.Format("[PAJ] T1 Time: {0}", t1));
                Console.WriteLine(string.Format("[PAJ] Total Time: {0}", t0 + t1));

                mergedAtire.GetResults(mergedAtire);
                List<string> results = mergedAtire.results;
                Console.WriteLine(string.Format("[PAJ] Total Count: {0}", results.Count));
                //System.IO.File.WriteAllLines(@"C:\Results\PAJJoin.txt", results);
                Console.WriteLine();
                testResults.phase1Time = t0;
                testResults.phase2Time = t1;
                testResults.phase3Time = 0; ;
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

                sw.Start();
                #region Phase 1

                Dictionary<long, string> customerHashTable = new Dictionary<long, string>();
                Dictionary<long, string> supplierHashTable = new Dictionary<long, string>();
                Dictionary<long, string> dateHashTable = new Dictionary<long, string>();

                Parallel.Invoke(parallelOptions,
                () =>
                {
                    foreach (Date row in InMemoryData.dateDimension)
                    {
                        if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
                        {
                            dateHashTable.Add(row.dDateKey, row.dYear);
                        }
                    }
                },
                () =>
                {
                    foreach (Customer row in InMemoryData.customerDimension)
                    {
                        if (row.cNation.Equals("UNITED STATES"))
                        {
                            customerHashTable.Add(row.cCustKey, row.cCity);
                        }
                    }
                },
                () =>
                {
                    foreach (Supplier row in InMemoryData.supplierDimension)
                    {
                        if (row.sNation.Equals("UNITED STATES"))
                        {
                            supplierHashTable.Add(row.sSuppKey, row.sCity);
                        }
                    }
                });

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(string.Format("[PAJ] T0 Time: {0}", t0));
                #endregion Phase1

                sw.Start();

                // Local Aggregation 
                List<Tuple<int, int>> partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);
                List<Task<Atire>> tasks = new List<Task<Atire>>();
                foreach (Tuple<int, int> indexes in partitionIndexes)
                {
                    Task<Atire> t = Task<Atire>.Factory.StartNew(() =>
                    {
                        Atire atire = new Atire();
                        for (int i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            long custKey = InMemoryData.loCustomerKey[i];
                            long suppKey = InMemoryData.loSupplierKey[i];
                            long dateKey = InMemoryData.loOrderDate[i];
                            string custCity = string.Empty;
                            string suppCity = string.Empty;
                            string dYear = string.Empty;
                            if (customerHashTable.TryGetValue(custKey, out custCity)
                            && supplierHashTable.TryGetValue(suppKey, out suppCity)
                            && dateHashTable.TryGetValue(dateKey, out dYear))
                            {
                                atire.Insert(atire, new List<string> { custCity, suppCity, dYear }, isLockFree, InMemoryData.loRevenue[i]);
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
                Console.WriteLine(string.Format("[PAJ] T1 Time: {0}", t1));
                Console.WriteLine(string.Format("[PAJ] Total Time: {0}", t0 + t1));

                mergedAtire.GetResults(mergedAtire);
                List<string> results = mergedAtire.results;
                Console.WriteLine(string.Format("[PAJ] Total Count: {0}", results.Count));
                //System.IO.File.WriteAllLines(@"C:\Results\PAJJoin.txt", results);
                Console.WriteLine();
                testResults.phase1Time = t0;
                testResults.phase2Time = t1;
                testResults.phase3Time = 0; ;
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

                sw.Start();
                #region Phase 1

                Dictionary<long, string> customerHashTable = new Dictionary<long, string>();
                Dictionary<long, string> supplierHashTable = new Dictionary<long, string>();
                Dictionary<long, string> dateHashTable = new Dictionary<long, string>();

                Parallel.Invoke(parallelOptions, () =>
                {
                    foreach (Date row in InMemoryData.dateDimension)
                    {
                        if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
                        {
                            dateHashTable.Add(row.dDateKey, row.dYear);
                        }
                    }
                },
                 () =>
                 {
                     foreach (Customer row in InMemoryData.customerDimension)
                     {
                         if (row.cCity.Equals("UNITED KI1") || row.cCity.Equals("UNITED KI5"))
                         {
                             customerHashTable.Add(row.cCustKey, row.cCity);
                         }
                     }
                 },
                 () =>
                 {
                     foreach (Supplier row in InMemoryData.supplierDimension)
                     {
                         if (row.sCity.Equals("UNITED KI1") || row.sCity.Equals("UNITED KI5"))
                         {
                             supplierHashTable.Add(row.sSuppKey, row.sCity);
                         }
                     }
                 });

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(string.Format("[PAJ] T0 Time: {0}", t0));
                #endregion Phase1

                sw.Start();

                // Local Aggregation 
                List<Tuple<int, int>> partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);
                List<Task<Atire>> tasks = new List<Task<Atire>>();
                foreach (Tuple<int, int> indexes in partitionIndexes)
                {
                    Task<Atire> t = Task<Atire>.Factory.StartNew(() =>
                    {
                        Atire atire = new Atire();
                        for (int i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            long custKey = InMemoryData.loCustomerKey[i];
                            long suppKey = InMemoryData.loSupplierKey[i];
                            long dateKey = InMemoryData.loOrderDate[i];
                            string custCity = string.Empty;
                            string suppCity = string.Empty;
                            string dYear = string.Empty;
                            if (customerHashTable.TryGetValue(custKey, out custCity) && supplierHashTable.TryGetValue(suppKey, out suppCity) && dateHashTable.TryGetValue(dateKey, out dYear))
                            {
                                atire.Insert(atire, new List<string> { custCity, suppCity, dYear }, isLockFree, InMemoryData.loRevenue[i]);
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
                Console.WriteLine(string.Format("[PAJ] T1 Time: {0}", t1));
                Console.WriteLine(string.Format("[PAJ] Total Time: {0}", t0 + t1));

                mergedAtire.GetResults(mergedAtire);
                List<string> results = mergedAtire.results;
                Console.WriteLine(string.Format("[PAJ] Total Count: {0}", results.Count));
                //System.IO.File.WriteAllLines(@"C:\Results\PAJJoin.txt", results);
                Console.WriteLine();
                testResults.phase1Time = t0;
                testResults.phase2Time = t1;
                testResults.phase3Time = 0; ;
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

                sw.Start();
                #region Phase 1

                Dictionary<long, string> customerHashTable = new Dictionary<long, string>();
                Dictionary<long, string> supplierHashTable = new Dictionary<long, string>();
                Dictionary<long, string> dateHashTable = new Dictionary<long, string>();

                Parallel.Invoke(parallelOptions, () =>
                {
                    foreach (Date row in InMemoryData.dateDimension)
                    {
                        if (row.dYearMonth.Equals("Dec1997"))
                        {
                            dateHashTable.Add(row.dDateKey, row.dYear);
                        }
                    }
                },
                () =>
                {
                    foreach (Customer row in InMemoryData.customerDimension)
                    {
                        if (row.cCity.Equals("UNITED KI1") || row.cCity.Equals("UNITED KI5"))
                        {
                            customerHashTable.Add(row.cCustKey, row.cCity);
                        }
                    }
                },
                () =>
                {
                    foreach (Supplier row in InMemoryData.supplierDimension)
                    {
                        if (row.sCity.Equals("UNITED KI1") || row.sCity.Equals("UNITED KI5"))
                        {
                            supplierHashTable.Add(row.sSuppKey, row.sCity);
                        }
                    }
                });

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(string.Format("[PAJ] T0 Time: {0}", t0));
                #endregion Phase1

                sw.Start();

                // Local Aggregation 
                List<Tuple<int, int>> partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);
                List<Task<Atire>> tasks = new List<Task<Atire>>();
                foreach (Tuple<int, int> indexes in partitionIndexes)
                {
                    Task<Atire> t = Task<Atire>.Factory.StartNew(() =>
                    {
                        Atire atire = new Atire();
                        for (int i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            long custKey = InMemoryData.loCustomerKey[i];
                            long suppKey = InMemoryData.loSupplierKey[i];
                            long dateKey = InMemoryData.loOrderDate[i];
                            string custCity = string.Empty;
                            string suppCity = string.Empty;
                            string dYear = string.Empty;
                            if (customerHashTable.TryGetValue(custKey, out custCity) && supplierHashTable.TryGetValue(suppKey, out suppCity) && dateHashTable.TryGetValue(dateKey, out dYear))
                            {
                                atire.Insert(atire, new List<string> { custCity, suppCity, dYear }, isLockFree, InMemoryData.loRevenue[i]);
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
                Console.WriteLine(string.Format("[PAJ] T1 Time: {0}", t1));
                Console.WriteLine(string.Format("[PAJ] Total Time: {0}", t0 + t1));

                mergedAtire.GetResults(mergedAtire);
                List<string> results = mergedAtire.results;
                Console.WriteLine(string.Format("[PAJ] Total Count: {0}", results.Count));
                //System.IO.File.WriteAllLines(@"C:\Results\PAJJoin.txt", results);
                Console.WriteLine();
                testResults.phase1Time = t0;
                testResults.phase2Time = t1;
                testResults.phase3Time = 0; ;
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

                sw.Start();
                #region Key Hashing Phase 

                Dictionary<long, string> customerHashTable = new Dictionary<long, string>();
                Dictionary<long, string> supplierHashTable = new Dictionary<long, string>();
                Dictionary<long, string> dateHashTable = new Dictionary<long, string>();
                Dictionary<long, string> partHashTable = new Dictionary<long, string>();

                Parallel.Invoke(parallelOptions,
                () =>
                {
                    foreach (Date row in InMemoryData.dateDimension)
                    {
                        dateHashTable.Add(row.dDateKey, row.dYear);
                    }
                },
                () =>
                {
                    foreach (Customer row in InMemoryData.customerDimension)
                    {
                        if (row.cRegion.Equals("AMERICA"))
                        {
                            customerHashTable.Add(row.cCustKey, row.cNation);
                        }
                    }
                },
                () =>
                {
                    foreach (Supplier row in InMemoryData.supplierDimension)
                    {
                        if (row.sRegion.Equals("AMERICA"))
                        {
                            supplierHashTable.Add(row.sSuppKey, row.sNation);
                        }
                    }
                },
                () =>
                {
                    foreach (Part row in InMemoryData.partDimension)
                    {
                        if (row.pMFGR.Equals("MFGR#1") || row.pMFGR.Equals("MFGR#2"))
                        {
                            partHashTable.Add(row.pPartKey, row.pMFGR);
                        }
                    }
                });

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(string.Format("[PAJ] T0 Time: {0}", t0));
                sw.Reset();
                #endregion Key Hashing Phase

                List<Tuple<int, int>> partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);

                sw.Start();
                List<Task<Atire>> tasks = new List<Task<Atire>>();
                foreach (Tuple<int, int> indexes in partitionIndexes)
                {
                    Task<Atire> t = Task<Atire>.Factory.StartNew(() =>
                    {
                        Atire atire = new Atire();
                        for (int i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            long custKey = InMemoryData.loCustomerKey[i];
                            long dateKey = InMemoryData.loOrderDate[i];
                            long suppKey = InMemoryData.loSupplierKey[i];
                            long partKey = InMemoryData.loPartKey[i];
                            string custNation = string.Empty;
                            string dYear = string.Empty;
                            if (customerHashTable.TryGetValue(custKey, out custNation)
                            && dateHashTable.TryGetValue(dateKey, out dYear)
                            && supplierHashTable.ContainsKey(suppKey)
                            && partHashTable.ContainsKey(partKey))
                            {
                                atire.Insert(atire, new List<string> { dYear, custNation }, isLockFree, (InMemoryData.loRevenue[i] - InMemoryData.loSupplyCost[i]));
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
                Console.WriteLine(string.Format("[PAJ] T1 Time: {0}", t1));
                Console.WriteLine(string.Format("[PAJ] Total Time: {0}", t0 + t1));

                mergedAtire.GetResults(mergedAtire);
                List<string> results = mergedAtire.results;
                Console.WriteLine(string.Format("[PAJ] Total Count: {0}", results.Count));
                //System.IO.File.WriteAllLines(@"C:\Results\PAJJoin.txt", results);
                Console.WriteLine();
                testResults.phase1Time = t0;
                testResults.phase2Time = t1;
                testResults.phase3Time = 0; ;
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

                sw.Start();
                #region Key Hashing Phase 

                Dictionary<long, string> customerHashTable = new Dictionary<long, string>();
                Dictionary<long, string> supplierHashTable = new Dictionary<long, string>();
                Dictionary<long, string> dateHashTable = new Dictionary<long, string>();
                Dictionary<long, string> partHashTable = new Dictionary<long, string>();

                Parallel.Invoke(parallelOptions,
               () =>
               {
                   foreach (Date row in InMemoryData.dateDimension)
                   {
                       if (row.dYear.Equals("1997") || row.dYear.Equals("1998"))
                       {
                           dateHashTable.Add(row.dDateKey, row.dYear);
                       }
                   }
               },
               () =>
               {
                   foreach (Customer row in InMemoryData.customerDimension)
                   {
                       if (row.cRegion.Equals("AMERICA"))
                       {
                           customerHashTable.Add(row.cCustKey, row.cNation);
                       }
                   }
               },
               () =>
               {
                   foreach (Supplier row in InMemoryData.supplierDimension)
                   {
                       if (row.sRegion.Equals("AMERICA"))
                       {
                           supplierHashTable.Add(row.sSuppKey, row.sNation);
                       }
                   }
               },
               () =>
               {
                   foreach (Part row in InMemoryData.partDimension)
                   {
                       if (row.pMFGR.Equals("MFGR#1") || row.pMFGR.Equals("MFGR#2"))
                       {
                           partHashTable.Add(row.pPartKey, row.pCategory);
                       }
                   }
               });

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(string.Format("[PAJ] T0 Time: {0}", t0));
                sw.Reset();
                #endregion Key Hashing Phase

                List<Tuple<int, int>> partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);

                sw.Start();
                List<Task<Atire>> tasks = new List<Task<Atire>>();

                foreach (Tuple<int, int> indexes in partitionIndexes)
                {
                    Task<Atire> t = Task<Atire>.Factory.StartNew(() =>
                    {
                        Atire atire = new Atire();
                        for (int i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            long suppKey = InMemoryData.loSupplierKey[i];
                            long dateKey = InMemoryData.loOrderDate[i];
                            long partKey = InMemoryData.loPartKey[i];
                            long custKey = InMemoryData.loCustomerKey[i];
                            string suppNation = string.Empty;
                            string dYear = string.Empty;
                            string pCategory = string.Empty;
                            if (supplierHashTable.TryGetValue(suppKey, out suppNation)
                            && partHashTable.TryGetValue(partKey, out pCategory)
                            && dateHashTable.TryGetValue(dateKey, out dYear)
                            && customerHashTable.ContainsKey(custKey))
                            {
                                atire.Insert(atire, new List<string> { dYear, suppNation, pCategory }, isLockFree, (InMemoryData.loRevenue[i] - InMemoryData.loSupplyCost[i]));
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
                Console.WriteLine(string.Format("[PAJ] T1 Time: {0}", t1));
                Console.WriteLine(string.Format("[PAJ] Total Time: {0}", t0 + t1));

                mergedAtire.GetResults(mergedAtire);
                List<string> results = mergedAtire.results;
                Console.WriteLine(string.Format("[PAJ] Total Count: {0}", results.Count));
                //System.IO.File.WriteAllLines(@"C:\Results\PAJJoin.txt", results);
                Console.WriteLine();
                testResults.phase1Time = t0;
                testResults.phase2Time = t1;
                testResults.phase3Time = 0; ;
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

                sw.Start();
                #region Key Hashing Phase 

                Dictionary<long, string> customerHashTable = new Dictionary<long, string>();
                Dictionary<long, string> supplierHashTable = new Dictionary<long, string>();
                Dictionary<long, string> dateHashTable = new Dictionary<long, string>();
                Dictionary<long, string> partHashTable = new Dictionary<long, string>();

                Parallel.Invoke(parallelOptions,
                () =>
                {
                    foreach (Date row in InMemoryData.dateDimension)
                    {
                        if (row.dYear.Equals("1997") || row.dYear.Equals("1998"))
                        {
                            dateHashTable.Add(row.dDateKey, row.dYear);
                        }
                    }
                },
                () =>
                {
                    foreach (Customer row in InMemoryData.customerDimension)
                    {
                        if (row.cRegion.Equals("AMERICA"))
                        {
                            customerHashTable.Add(row.cCustKey, row.cNation);
                        }
                    }
                },
                () =>
                {
                    foreach (Supplier row in InMemoryData.supplierDimension)
                    {
                        if (row.sNation.Equals("UNITED STATES"))
                        {
                            supplierHashTable.Add(row.sSuppKey, row.sCity);
                        }
                    }
                },
                () =>
                {
                    foreach (Part row in InMemoryData.partDimension)
                    {
                        if (row.pCategory.Equals("MFGR#14"))
                        {
                            partHashTable.Add(row.pPartKey, row.pBrand);
                        }
                    }
                });

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(string.Format("[PAJ] T0 Time: {0}", t0));
                sw.Reset();
                #endregion Key Hashing Phase


                List<Tuple<int, int>> partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);

                sw.Start();
                List<Task<Atire>> tasks = new List<Task<Atire>>();
                foreach (Tuple<int, int> indexes in partitionIndexes)
                {
                    Task<Atire> t = Task<Atire>.Factory.StartNew(() =>
                    {
                        Atire atire = new Atire();
                        for (int i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            long custKey = InMemoryData.loCustomerKey[i];
                            long dateKey = InMemoryData.loOrderDate[i];
                            long suppKey = InMemoryData.loSupplierKey[i];
                            long partKey = InMemoryData.loPartKey[i];
                            string suppCity = string.Empty;
                            string dYear = string.Empty;
                            string pBrand = string.Empty;
                            if (customerHashTable.ContainsKey(custKey)
                            && dateHashTable.TryGetValue(dateKey, out dYear)
                            && supplierHashTable.TryGetValue(suppKey, out suppCity)
                            && partHashTable.TryGetValue(partKey, out pBrand))
                            {
                                atire.Insert(atire, new List<string> { dYear, suppCity, pBrand }, isLockFree, (InMemoryData.loRevenue[i] - InMemoryData.loSupplyCost[i]));
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
                Console.WriteLine(string.Format("[PAJ] T1 Time: {0}", t1));
                Console.WriteLine(string.Format("[PAJ] Total Time: {0}", t0 + t1));

                mergedAtire.GetResults(mergedAtire);
                List<string> results = mergedAtire.results;
                Console.WriteLine(string.Format("[PAJ] Total Count: {0}", results.Count));
                //System.IO.File.WriteAllLines(@"C:\Results\PAJJoin.txt", results);
                Console.WriteLine();
                testResults.phase1Time = t0;
                testResults.phase2Time = t1;
                testResults.phase3Time = 0; ;
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
                long memoryStartPhase3 = GC.GetTotalMemory(true);
                sw.Start();
                #region Phase 1

                Dictionary<long, Tuple<string, string, string>> customerHashTable = new Dictionary<long, Tuple<string, string, string>>();
                Dictionary<long, Tuple<string, string, string>> supplierHashTable = new Dictionary<long, Tuple<string, string, string>>();
                Dictionary<long, Tuple<string, string>> dateHashTable = new Dictionary<long, Tuple<string, string>>();
                Dictionary<long, Tuple<string, string>> partHashTable = new Dictionary<long, Tuple<string, string>>();

                Parallel.Invoke(parallelOptions,
               () =>
               {
                   foreach (Date row in InMemoryData.dateDimension)
                   {
                       if (row.dYear.Equals("1997") || row.dYear.Equals("1998"))
                       {
                           dateHashTable.Add(row.dDateKey, Tuple.Create(row.dYear, row.dMonth));
                       }
                   }
               },
               () =>
               {
                   foreach (Customer row in InMemoryData.customerDimension)
                   {
                       if (row.cRegion.Equals("AMERICA"))
                       {
                           customerHashTable.Add(row.cCustKey, Tuple.Create(row.cNation, row.cRegion, row.cCity));
                       }
                   }
               },
               () =>
               {
                   foreach (Supplier row in InMemoryData.supplierDimension)
                   {
                       if (row.sNation.Equals("UNITED STATES"))
                       {
                           supplierHashTable.Add(row.sSuppKey, Tuple.Create(row.sNation, row.sRegion, row.sCity));
                       }
                   }
               },
               () =>
               {
                   foreach (Part row in InMemoryData.partDimension)
                   {
                       if (row.pCategory.Equals("MFGR#14"))
                       {
                           partHashTable.Add(row.pPartKey, Tuple.Create(row.pBrand, row.pMFGR));
                       }
                   }
               });

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(string.Format("[PAJ] GSTest T0 Time: {0}", t0));
                sw.Reset();
                #endregion Phase1

                List<Tuple<int, int>> partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);

                sw.Start();
                List<Task<Atire>> tasks = new List<Task<Atire>>();
                foreach (Tuple<int, int> indexes in partitionIndexes)
                {
                    Task<Atire> t = Task<Atire>.Factory.StartNew(() =>
                    {
                        Atire atire = new Atire();
                        List<string> groupingAttributes = new List<string>();
                        for (int i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            long custKey = InMemoryData.loCustomerKey[i];
                            long dateKey = InMemoryData.loOrderDate[i];
                            long suppKey = InMemoryData.loSupplierKey[i];
                            long partKey = InMemoryData.loPartKey[i];
                            Tuple<string, string, string> cOut = null;
                            Tuple<string, string> dOut = null;
                            Tuple<string, string, string> sOut = null;
                            Tuple<string, string> pOut = null;

                            if (customerHashTable.TryGetValue(custKey, out cOut)
                            && dateHashTable.TryGetValue(dateKey, out dOut)
                            && supplierHashTable.TryGetValue(suppKey, out sOut)
                            && partHashTable.TryGetValue(partKey, out pOut))
                            {
                                switch (numberOfGroupingAttributes)
                                {
                                    case 1:
                                        groupingAttributes.Add(dOut.Item1);
                                        break;
                                    case 2:
                                        groupingAttributes.Add(dOut.Item1);
                                        groupingAttributes.Add(dOut.Item2);

                                        break;
                                    case 3:
                                        groupingAttributes.Add(dOut.Item1);
                                        groupingAttributes.Add(dOut.Item2);
                                        groupingAttributes.Add(sOut.Item1);
                                        break;
                                    case 4:
                                        groupingAttributes.Add(dOut.Item1);
                                        groupingAttributes.Add(dOut.Item2);
                                        groupingAttributes.Add(sOut.Item1);
                                        groupingAttributes.Add(sOut.Item2);
                                        break;
                                    case 5:
                                        groupingAttributes.Add(dOut.Item1);
                                        groupingAttributes.Add(dOut.Item2);
                                        groupingAttributes.Add(sOut.Item1);
                                        groupingAttributes.Add(sOut.Item2);
                                        groupingAttributes.Add(sOut.Item3);
                                        break;
                                    case 6:
                                        groupingAttributes.Add(dOut.Item1);
                                        groupingAttributes.Add(dOut.Item2);
                                        groupingAttributes.Add(sOut.Item1);
                                        groupingAttributes.Add(sOut.Item2);
                                        groupingAttributes.Add(sOut.Item3);
                                        groupingAttributes.Add(cOut.Item1);
                                        break;
                                    case 7:
                                        groupingAttributes.Add(dOut.Item1);
                                        groupingAttributes.Add(dOut.Item2);
                                        groupingAttributes.Add(sOut.Item1);
                                        groupingAttributes.Add(sOut.Item2);
                                        groupingAttributes.Add(sOut.Item3);
                                        groupingAttributes.Add(cOut.Item1);
                                        groupingAttributes.Add(cOut.Item2);
                                        break;
                                    case 8:
                                        groupingAttributes.Add(dOut.Item1);
                                        groupingAttributes.Add(dOut.Item2);
                                        groupingAttributes.Add(sOut.Item1);
                                        groupingAttributes.Add(sOut.Item2);
                                        groupingAttributes.Add(sOut.Item3);
                                        groupingAttributes.Add(cOut.Item1);
                                        groupingAttributes.Add(cOut.Item2);
                                        groupingAttributes.Add(cOut.Item3);
                                        break;
                                    case 9:
                                        groupingAttributes.Add(dOut.Item1);
                                        groupingAttributes.Add(dOut.Item2);
                                        groupingAttributes.Add(sOut.Item1);
                                        groupingAttributes.Add(sOut.Item2);
                                        groupingAttributes.Add(sOut.Item3);
                                        groupingAttributes.Add(cOut.Item1);
                                        groupingAttributes.Add(cOut.Item2);
                                        groupingAttributes.Add(cOut.Item3);
                                        groupingAttributes.Add(pOut.Item1);
                                        break;
                                    case 10:
                                        groupingAttributes.Add(dOut.Item1);
                                        groupingAttributes.Add(dOut.Item2);
                                        groupingAttributes.Add(sOut.Item1);
                                        groupingAttributes.Add(sOut.Item2);
                                        groupingAttributes.Add(sOut.Item3);
                                        groupingAttributes.Add(cOut.Item1);
                                        groupingAttributes.Add(cOut.Item2);
                                        groupingAttributes.Add(cOut.Item3);
                                        groupingAttributes.Add(pOut.Item1);
                                        groupingAttributes.Add(pOut.Item2);
                                        break;
                                }
                                atire.Insert(atire, groupingAttributes, isLockFree, (InMemoryData.loRevenue[i] - InMemoryData.loSupplyCost[i]));
                                groupingAttributes.Clear();
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
                long memoryUsed = GC.GetTotalMemory(true) - memoryStartPhase3;
                Console.WriteLine(string.Format("[PAJ] GSTest T1 Time: {0}", t1));
                Console.WriteLine(string.Format("[PAJ] GSTest Total Time: {0}", t0 + t1));
                Console.WriteLine(String.Format("[PAJ] GSTest Memory Used : {0}", memoryUsed));
                mergedAtire.GetResults(mergedAtire);
                List<string> results = mergedAtire.results;
                // Console.WriteLine(string.Format("[PAJ] GSTest Total Count: {0}", results.Count));
                //System.IO.File.WriteAllLines(@"C:\Results\PAJJoin.txt", results);
                Console.WriteLine();
                testResults.phase1Time = t0;
                testResults.phase2Time = t1;
                testResults.phase3Time = 0; ;
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
            if (isLockFree)
            {
                TestResultsDatabase.pATireJoinOutputLF.Add(testResults.toString());
            }
            else
            {
                TestResultsDatabase.pATireJoinOutputLC.Add(testResults.toString());
            }
        }
    }
}
