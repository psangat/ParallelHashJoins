using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParallelHashJoins
{
    internal class ParallelInMemoryAggregation
    {
        private static readonly string binaryFilesDirectory = @"C:\Raw_Data_Source_For_Test\SSBM - DBGEN\BF";
        private string scaleFactor { get; set; }
        private ParallelOptions parallelOptions = null;
        private TestResults testResults = new TestResults();

        public ParallelInMemoryAggregation(string _scaleFactor, ParallelOptions _parallelOptions)
        {
            scaleFactor = _scaleFactor;
            parallelOptions = _parallelOptions;
        }

        ~ParallelInMemoryAggregation()
        {
            saveAndPrintResults();
        }

        public void Query_2_1_IM()
        {

            Stopwatch sw = new Stopwatch();

            #region Step 1 & 2
            sw.Start();
            Dictionary<long, long> kvPartDim = new Dictionary<long, long>();
            DataTable tempTablePartDim = new DataTable();
            tempTablePartDim.Columns.Add("partBrand", typeof(string));
            tempTablePartDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvSupplierDim = new Dictionary<long, long>();
            DataTable tempTableSupplierDim = new DataTable();
            tempTableSupplierDim.Columns.Add("supplierNation", typeof(string));
            tempTableSupplierDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvDateDim = new Dictionary<long, long>();
            DataTable tempTableDateDim = new DataTable();
            tempTableDateDim.Columns.Add("year", typeof(string));
            tempTableDateDim.Columns.Add("denseGroupingKey", typeof(long));

            Parallel.Invoke(parallelOptions,
                () =>
                {
                    long partIndex = 0;
                    long dgKeyPart = 0;
                    foreach (Part part in InMemoryData.partDimension)
                    {
                        if (part.pCategory.Equals("MFGR#12"))
                        {
                            string pBrand = part.pBrand;
                            if (tempTablePartDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTablePartDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTablePartDim.Rows)
                                {
                                    string partBrand = row.Field<string>("partBrand");
                                    if (partBrand.Equals(pBrand))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
                    long supplierIndex = 0;
                    long dgKeySupplier = 0;
                    foreach (Supplier supplier in InMemoryData.supplierDimension)
                    {
                        if (supplier.sRegion.Equals("AMERICA"))
                        {
                            string sNation = supplier.sNation;
                            if (tempTableSupplierDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableSupplierDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableSupplierDim.Rows)
                                {
                                    string supplierNation = row.Field<string>("supplierNation");
                                    if (supplierNation.Equals(sNation))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
                    long dgKeyDate = 0;
                    foreach (Date date in InMemoryData.dateDimension)
                    {

                        string dYear = date.dYear;
                        if (tempTableDateDim.Rows.Count > 0)
                        {
                            DataTable tempTable = tempTableDateDim.Copy();
                            bool found = false;
                            foreach (DataRow row in tempTableDateDim.Rows)
                            {
                                string year = row.Field<string>("year");
                                if (year.Equals(dYear))
                                {
                                    long dgKey = row.Field<long>("denseGroupingKey");
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

            #region Step 3, 4 & 5

            sw.Reset();
            sw.Start();
            long dgkLengthPart = tempTablePartDim.Rows.Count + 1;
            long dgkLengthDate = tempTableDateDim.Rows.Count + 1;

            long[,] inMemoryAccumulator = new long[dgkLengthDate, dgkLengthPart];

            List<Tuple<int, int>> partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loPartKey.Count(), parallelOptions.MaxDegreeOfParallelism);

            List<Task> tasks = new List<Task>();
            foreach (Tuple<int, int> indexes in partitionIndexes)
            {
                Task t = Task.Factory.StartNew(() =>
                {
                    for (int i = indexes.Item1; i <= indexes.Item2; i++)
                    {
                        long partKey = InMemoryData.loPartKey[i];
                        long suppKey = InMemoryData.loSupplierKey[i];
                        long dateKey = InMemoryData.loOrderDate[i];
                        if (kvPartDim.TryGetValue(partKey, out long dgkPartDim)
                            && kvSupplierDim.TryGetValue(suppKey, out long dgkSupplierDim)
                            && kvDateDim.TryGetValue(dateKey, out long dgkDateDim))
                        {
                            if (dgkPartDim == 0 || dgkSupplierDim == 0 || dgkDateDim == 0)
                            {
                                // skip
                            }
                            else
                            {
                                inMemoryAccumulator[dgkDateDim, dgkPartDim] += InMemoryData.loRevenue[i];
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
                    long sumRevenue = inMemoryAccumulator[ddRow.Field<long>("denseGroupingKey"), pdRow.Field<long>("denseGroupingKey")];
                    if (sumRevenue != 0)
                    {
                        finalTable.Add(ddRow.Field<string>("year") + ", " + pdRow.Field<string>("partBrand") + ", " + sumRevenue);
                    }
                }

            }

            sw.Stop();
            long t2 = sw.ElapsedMilliseconds;
            Console.WriteLine("[PIMA] Phase2 Time: " + t2);
            Console.WriteLine(string.Format("[PIMA] Total Time: {0}", t1 + t2));
            Console.WriteLine(string.Format("[PIMA] Total Items: {0}", finalTable.Count));
            Console.WriteLine();
            #endregion Step 3, 4 & 5
            testResults.phase1Time = t1;
            testResults.phase2Time = t2;
            testResults.phase3Time = 0; ;
        }

        public void Query_2_2_IM()
        {

            Stopwatch sw = new Stopwatch();

            #region Step 1 & 2
            sw.Start();
            Dictionary<long, long> kvPartDim = new Dictionary<long, long>();
            DataTable tempTablePartDim = new DataTable();
            tempTablePartDim.Columns.Add("partBrand", typeof(string));
            tempTablePartDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvSupplierDim = new Dictionary<long, long>();
            DataTable tempTableSupplierDim = new DataTable();
            tempTableSupplierDim.Columns.Add("supplierNation", typeof(string));
            tempTableSupplierDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvDateDim = new Dictionary<long, long>();
            DataTable tempTableDateDim = new DataTable();
            tempTableDateDim.Columns.Add("year", typeof(string));
            tempTableDateDim.Columns.Add("denseGroupingKey", typeof(long));

            Parallel.Invoke(parallelOptions,
                () =>
                {
                    long partIndex = 0;
                    long dgKeyPart = 0;
                    foreach (Part part in InMemoryData.partDimension)
                    {
                        if (string.CompareOrdinal(part.pBrand, "MFGR#2221") >= 0 && string.CompareOrdinal(part.pBrand, "MFGR#2228") <= 0)
                        {
                            string pBrand = part.pBrand;
                            if (tempTablePartDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTablePartDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTablePartDim.Rows)
                                {
                                    string partBrand = row.Field<string>("partBrand");
                                    if (partBrand.Equals(pBrand))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
                    long supplierIndex = 0;
                    long dgKeySupplier = 0;
                    foreach (Supplier supplier in InMemoryData.supplierDimension)
                    {
                        if (supplier.sRegion.Equals("ASIA"))
                        {
                            string sNation = supplier.sNation;
                            if (tempTableSupplierDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableSupplierDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableSupplierDim.Rows)
                                {
                                    string supplierNation = row.Field<string>("supplierNation");
                                    if (supplierNation.Equals(sNation))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
                    long dgKeyDate = 0;
                    foreach (Date date in InMemoryData.dateDimension)
                    {
                        string dYear = date.dYear;
                        if (tempTableDateDim.Rows.Count > 0)
                        {
                            DataTable tempTable = tempTableDateDim.Copy();
                            bool found = false;
                            foreach (DataRow row in tempTableDateDim.Rows)
                            {
                                string year = row.Field<string>("year");
                                if (year.Equals(dYear))
                                {
                                    long dgKey = row.Field<long>("denseGroupingKey");
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

            #region Step 3, 4 & 5

            sw.Reset();
            sw.Start();
            long dgkLengthPart = tempTablePartDim.Rows.Count + 1;
            long dgkLengthDate = tempTableDateDim.Rows.Count + 1;

            long[,] inMemoryAccumulator = new long[dgkLengthDate, dgkLengthPart];

            List<Tuple<int, int>> partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loPartKey.Count(), parallelOptions.MaxDegreeOfParallelism);

            List<Task> tasks = new List<Task>();
            foreach (Tuple<int, int> indexes in partitionIndexes)
            {
                Task t = Task.Factory.StartNew(() =>
                {
                    for (int i = indexes.Item1; i <= indexes.Item2; i++)
                    {
                        long partKey = InMemoryData.loPartKey[i];
                        long dateKey = InMemoryData.loOrderDate[i];
                        long suppKey = InMemoryData.loSupplierKey[i];
                        if (kvPartDim.TryGetValue(partKey, out long dgkPartDim)
                            && kvDateDim.TryGetValue(dateKey, out long dgkDateDim)
                            && kvSupplierDim.TryGetValue(suppKey, out long dgkSuppDim))
                        {
                            if (dgkPartDim == 0 || dgkDateDim == 0)
                            {
                                // skip
                            }
                            else
                            {
                                inMemoryAccumulator[dgkDateDim, dgkPartDim] += InMemoryData.loRevenue[i];
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
                    long sumRevenue = inMemoryAccumulator[ddRow.Field<long>("denseGroupingKey"), pdRow.Field<long>("denseGroupingKey")];
                    if (sumRevenue != 0)
                    {
                        finalTable.Add(ddRow.Field<string>("year") + ", " + pdRow.Field<string>("partBrand") + ", " + sumRevenue);
                    }
                }
            }

            sw.Stop();
            long t2 = sw.ElapsedMilliseconds;
            Console.WriteLine("[PIMA] Phase2 Time: " + t2);
            Console.WriteLine(string.Format("[PIMA] Total Time: {0}", t1 + t2));
            Console.WriteLine(string.Format("[PIMA] Total Items: {0}", finalTable.Count));
            Console.WriteLine();
            #endregion Step 3, 4 & 5
            testResults.phase1Time = t1;
            testResults.phase2Time = t2;
            testResults.phase3Time = 0; ;
        }

        public void Query_2_3_IM()
        {

            Stopwatch sw = new Stopwatch();

            #region Step 1 & 2
            sw.Start();
            Dictionary<long, long> kvPartDim = new Dictionary<long, long>();
            DataTable tempTablePartDim = new DataTable();
            tempTablePartDim.Columns.Add("partBrand", typeof(string));
            tempTablePartDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvSupplierDim = new Dictionary<long, long>();
            DataTable tempTableSupplierDim = new DataTable();
            tempTableSupplierDim.Columns.Add("supplierNation", typeof(string));
            tempTableSupplierDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvDateDim = new Dictionary<long, long>();
            DataTable tempTableDateDim = new DataTable();
            tempTableDateDim.Columns.Add("year", typeof(string));
            tempTableDateDim.Columns.Add("denseGroupingKey", typeof(long));

            Parallel.Invoke(parallelOptions,
                () =>
                {
                    long partIndex = 0;
                    long dgKeyPart = 0;
                    foreach (Part part in InMemoryData.partDimension)
                    {
                        if (part.pBrand.Equals("MFGR#2221"))
                        {
                            string pBrand = part.pBrand;
                            if (tempTablePartDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTablePartDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTablePartDim.Rows)
                                {
                                    string partBrand = row.Field<string>("partBrand");
                                    if (partBrand.Equals(pBrand))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
                    long supplierIndex = 0;
                    long dgKeySupplier = 0;
                    foreach (Supplier supplier in InMemoryData.supplierDimension)
                    {
                        if (supplier.sRegion.Equals("EUROPE"))
                        {
                            string sNation = supplier.sNation;
                            if (tempTableSupplierDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableSupplierDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableSupplierDim.Rows)
                                {
                                    string supplierNation = row.Field<string>("supplierNation");
                                    if (supplierNation.Equals(sNation))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
                    long dgKeyDate = 0;
                    foreach (Date date in InMemoryData.dateDimension)
                    {
                        string dYear = date.dYear;
                        if (tempTableDateDim.Rows.Count > 0)
                        {
                            DataTable tempTable = tempTableDateDim.Copy();
                            bool found = false;
                            foreach (DataRow row in tempTableDateDim.Rows)
                            {
                                string year = row.Field<string>("year");
                                if (year.Equals(dYear))
                                {
                                    long dgKey = row.Field<long>("denseGroupingKey");
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

            #region Step 3, 4 & 5

            sw.Reset();
            sw.Start();
            long dgkLengthPart = tempTablePartDim.Rows.Count + 1;
            long dgkLengthDate = tempTableDateDim.Rows.Count + 1;

            long[,] inMemoryAccumulator = new long[dgkLengthDate, dgkLengthPart];

            List<Tuple<int, int>> partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loPartKey.Count(), parallelOptions.MaxDegreeOfParallelism);

            List<Task> tasks = new List<Task>();
            foreach (Tuple<int, int> indexes in partitionIndexes)
            {
                Task t = Task.Factory.StartNew(() =>
                {
                    for (int i = indexes.Item1; i <= indexes.Item2; i++)
                    {
                        long partKey = InMemoryData.loPartKey[i];
                        long dateKey = InMemoryData.loOrderDate[i];
                        long suppKey = InMemoryData.loSupplierKey[i];
                        if (kvPartDim.TryGetValue(partKey, out long dgkPartDim)
                            && kvDateDim.TryGetValue(dateKey, out long dgkDateDim)
                            && kvSupplierDim.TryGetValue(suppKey, out long dgkSupplierDim))
                        {
                            if (dgkPartDim == 0 || dgkDateDim == 0)
                            {
                                // skip
                            }
                            else
                            {
                                inMemoryAccumulator[dgkDateDim, dgkPartDim] += InMemoryData.loRevenue[i];
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
                    long sumRevenue = inMemoryAccumulator[ddRow.Field<long>("denseGroupingKey"), pdRow.Field<long>("denseGroupingKey")];
                    if (sumRevenue != 0)
                    {
                        finalTable.Add(ddRow.Field<string>("year") + ", " + pdRow.Field<string>("partBrand") + ", " + sumRevenue);
                    }
                }
            }

            sw.Stop();
            long t2 = sw.ElapsedMilliseconds;
            Console.WriteLine("[PIMA] Phase2 Time: " + t2);
            Console.WriteLine(string.Format("[PIMA] Total Time: {0}", t1 + t2));
            Console.WriteLine(string.Format("[PIMA] Total Items: {0}", finalTable.Count));
            Console.WriteLine();
            #endregion Step 3, 4 & 5
            testResults.phase1Time = t1;
            testResults.phase2Time = t2;
            testResults.phase3Time = 0; ;
        }

        /// <summary>
        /// Key Vector is implemented as Dictionary <Int64, Int64>
        /// InMemory Accumulator is a MultiDimensional Array
        /// Temporary Table is a Datatable
        /// </summary>
        public void Query_3_1_IM()
        {

            Stopwatch sw = new Stopwatch();

            #region Step 1 & 2
            sw.Start();
            Dictionary<long, long> kvCustomerDim = new Dictionary<long, long>();
            DataTable tempTableCustomerDim = new DataTable();
            tempTableCustomerDim.Columns.Add("customerNation", typeof(string));
            tempTableCustomerDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvSupplierDim = new Dictionary<long, long>();
            DataTable tempTableSupplierDim = new DataTable();
            tempTableSupplierDim.Columns.Add("supplierNation", typeof(string));
            tempTableSupplierDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvDateDim = new Dictionary<long, long>();
            DataTable tempTableDateDim = new DataTable();
            tempTableDateDim.Columns.Add("year", typeof(string));
            tempTableDateDim.Columns.Add("denseGroupingKey", typeof(long));

            Parallel.Invoke(parallelOptions,
                () =>
                {
                    long customerIndex = 0;
                    long dgKeyCustomer = 0;
                    foreach (Customer customer in InMemoryData.customerDimension)
                    {
                        if (customer.cRegion.Equals("ASIA"))
                        {
                            string cNation = customer.cNation;
                            if (tempTableCustomerDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableCustomerDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableCustomerDim.Rows)
                                {
                                    string customerNation = row.Field<string>("customerNation");
                                    if (customerNation.Equals(cNation))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
                    long supplierIndex = 0;
                    long dgKeySupplier = 0;
                    foreach (Supplier supplier in InMemoryData.supplierDimension)
                    {
                        if (supplier.sRegion.Equals("ASIA"))
                        {
                            string sNation = supplier.sNation;
                            if (tempTableSupplierDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableSupplierDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableSupplierDim.Rows)
                                {
                                    string supplierNation = row.Field<string>("supplierNation");
                                    if (supplierNation.Equals(sNation))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
                    long dgKeyDate = 0;
                    foreach (Date date in InMemoryData.dateDimension)
                    {
                        if (date.dYear.CompareTo("1992") >= 0 && date.dYear.CompareTo("1997") <= 0)
                        {
                            string dYear = date.dYear;
                            if (tempTableDateDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableDateDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableDateDim.Rows)
                                {
                                    string year = row.Field<string>("year");
                                    if (year.Equals(dYear))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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

            #region Step 3, 4 & 5

            sw.Reset();
            sw.Start();
            long dgkLengthCustomer = tempTableCustomerDim.Rows.Count + 1;
            long dgkLengthSupplier = tempTableSupplierDim.Rows.Count + 1;
            long dgkLengthDate = tempTableDateDim.Rows.Count + 1;

            long[,,] inMemoryAccumulator = new long[dgkLengthCustomer, dgkLengthSupplier, dgkLengthDate];

            List<Tuple<int, int>> partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);

            List<Task> tasks = new List<Task>();
            foreach (Tuple<int, int> indexes in partitionIndexes)
            {
                Task t = Task.Factory.StartNew(() =>
                {
                    for (int i = indexes.Item1; i <= indexes.Item2; i++)
                    {
                        long custKey = InMemoryData.loCustomerKey[i];
                        long suppKey = InMemoryData.loSupplierKey[i];
                        long dateKey = InMemoryData.loOrderDate[i];
                        if (kvCustomerDim.TryGetValue(custKey, out long dgkCustomerDim)
                            && kvSupplierDim.TryGetValue(suppKey, out long dgkSupplierDim)
                            && kvDateDim.TryGetValue(dateKey, out long dgkDateDim))
                        {
                            if (dgkCustomerDim == 0 || dgkSupplierDim == 0 || dgkDateDim == 0)
                            {
                                // skip
                            }
                            else
                            {
                                inMemoryAccumulator[dgkCustomerDim, dgkSupplierDim, dgkDateDim] += InMemoryData.loRevenue[i];
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
                        long sumRevenue = inMemoryAccumulator[cdRow.Field<long>("denseGroupingKey")
                            , sdRow.Field<long>("denseGroupingKey")
                            , ddRow.Field<long>("denseGroupingKey")];
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
            Console.WriteLine(string.Format("[PIMA] Total Time: {0}", t1 + t2));
            Console.WriteLine(string.Format("[PIMA] Total Count: {0}", finalTable.Count));
            Console.WriteLine();
            #endregion Step 3, 4 & 5
            testResults.phase1Time = t1;
            testResults.phase2Time = t2;
            testResults.phase3Time = 0; ;
        }

        public void Query_3_2_IM()
        {

            Stopwatch sw = new Stopwatch();

            #region Step 1 & 2
            sw.Start();
            Dictionary<long, long> kvCustomerDim = new Dictionary<long, long>();
            DataTable tempTableCustomerDim = new DataTable();
            tempTableCustomerDim.Columns.Add("customerCity", typeof(string));
            tempTableCustomerDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvSupplierDim = new Dictionary<long, long>();
            DataTable tempTableSupplierDim = new DataTable();
            tempTableSupplierDim.Columns.Add("supplierCity", typeof(string));
            tempTableSupplierDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvDateDim = new Dictionary<long, long>();
            DataTable tempTableDateDim = new DataTable();
            tempTableDateDim.Columns.Add("year", typeof(string));
            tempTableDateDim.Columns.Add("denseGroupingKey", typeof(long));

            Parallel.Invoke(parallelOptions,
                () =>
                {
                    long customerIndex = 0;
                    long dgKeyCustomer = 0;
                    foreach (Customer customer in InMemoryData.customerDimension)
                    {
                        if (customer.cNation.Equals("UNITED STATES"))
                        {
                            string cCity = customer.cCity;
                            if (tempTableCustomerDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableCustomerDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableCustomerDim.Rows)
                                {
                                    string customerCity = row.Field<string>("customerCity");
                                    if (customerCity.Equals(cCity))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
                    long supplierIndex = 0;
                    long dgKeySupplier = 0;
                    foreach (Supplier supplier in InMemoryData.supplierDimension)
                    {
                        if (supplier.sNation.Equals("UNITED STATES"))
                        {
                            string sCity = supplier.sCity;
                            if (tempTableSupplierDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableSupplierDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableSupplierDim.Rows)
                                {
                                    string supplierCity = row.Field<string>("supplierCity");
                                    if (supplierCity.Equals(sCity))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
                    long dgKeyDate = 0;
                    foreach (Date date in InMemoryData.dateDimension)
                    {
                        if (date.dYear.CompareTo("1992") >= 0 && date.dYear.CompareTo("1997") <= 0)
                        {
                            string dYear = date.dYear;
                            if (tempTableDateDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableDateDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableDateDim.Rows)
                                {
                                    string year = row.Field<string>("year");
                                    if (year.Equals(dYear))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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

            #region Step 3, 4 & 5

            sw.Reset();
            sw.Start();
            long dgkLengthCustomer = tempTableCustomerDim.Rows.Count + 1;
            long dgkLengthSupplier = tempTableSupplierDim.Rows.Count + 1;
            long dgkLengthDate = tempTableDateDim.Rows.Count + 1;

            long[,,] inMemoryAccumulator = new long[dgkLengthCustomer, dgkLengthSupplier, dgkLengthDate];

            List<Tuple<int, int>> partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);

            List<Task> tasks = new List<Task>();
            foreach (Tuple<int, int> indexes in partitionIndexes)
            {
                Task t = Task.Factory.StartNew(() =>
                {
                    for (int i = indexes.Item1; i <= indexes.Item2; i++)
                    {
                        long custKey = InMemoryData.loCustomerKey[i];
                        long suppKey = InMemoryData.loSupplierKey[i];
                        long dateKey = InMemoryData.loOrderDate[i];
                        if (kvCustomerDim.TryGetValue(custKey, out long dgkCustomerDim)
                            && kvSupplierDim.TryGetValue(suppKey, out long dgkSupplierDim)
                            && kvDateDim.TryGetValue(dateKey, out long dgkDateDim))
                        {
                            if (dgkCustomerDim == 0 || dgkSupplierDim == 0 || dgkDateDim == 0)
                            {
                                // skip
                            }
                            else
                            {
                                inMemoryAccumulator[dgkCustomerDim, dgkSupplierDim, dgkDateDim] += InMemoryData.loRevenue[i];
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
                        long sumRevenue = inMemoryAccumulator[cdRow.Field<long>("denseGroupingKey")
                            , sdRow.Field<long>("denseGroupingKey")
                            , ddRow.Field<long>("denseGroupingKey")];
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
            Console.WriteLine(string.Format("[PIMA] Total Time: {0}", t1 + t2));
            Console.WriteLine(string.Format("[PIMA] Total Count: {0}", finalTable.Count));
            Console.WriteLine();
            #endregion Step 3, 4 & 5
            testResults.phase1Time = t1;
            testResults.phase2Time = t2;
            testResults.phase3Time = 0; ;
        }

        public void Query_3_3_IM()
        {

            Stopwatch sw = new Stopwatch();

            #region Step 1 & 2
            sw.Start();
            Dictionary<long, long> kvCustomerDim = new Dictionary<long, long>();
            DataTable tempTableCustomerDim = new DataTable();
            tempTableCustomerDim.Columns.Add("customerCity", typeof(string));
            tempTableCustomerDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvSupplierDim = new Dictionary<long, long>();
            DataTable tempTableSupplierDim = new DataTable();
            tempTableSupplierDim.Columns.Add("supplierCity", typeof(string));
            tempTableSupplierDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvDateDim = new Dictionary<long, long>();
            DataTable tempTableDateDim = new DataTable();
            tempTableDateDim.Columns.Add("year", typeof(string));
            tempTableDateDim.Columns.Add("denseGroupingKey", typeof(long));

            Parallel.Invoke(parallelOptions,
                () =>
                {
                    long customerIndex = 0;
                    long dgKeyCustomer = 0;
                    foreach (Customer customer in InMemoryData.customerDimension)
                    {
                        if (customer.cCity.Equals("UNITED KI1") || customer.cCity.Equals("UNITED KI5"))
                        {
                            string cCity = customer.cCity;
                            if (tempTableCustomerDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableCustomerDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableCustomerDim.Rows)
                                {
                                    string customerCity = row.Field<string>("customerCity");
                                    if (customerCity.Equals(cCity))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
                    long supplierIndex = 0;
                    long dgKeySupplier = 0;
                    foreach (Supplier supplier in InMemoryData.supplierDimension)
                    {
                        if (supplier.sCity.Equals("UNITED KI1") || supplier.sCity.Equals("UNITED KI5"))
                        {
                            string sCity = supplier.sCity;
                            if (tempTableSupplierDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableSupplierDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableSupplierDim.Rows)
                                {
                                    string supplierCity = row.Field<string>("supplierCity");
                                    if (supplierCity.Equals(sCity))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
                    long dgKeyDate = 0;
                    foreach (Date date in InMemoryData.dateDimension)
                    {
                        if (date.dYear.CompareTo("1992") >= 0 && date.dYear.CompareTo("1997") <= 0)
                        {
                            string dYear = date.dYear;
                            if (tempTableDateDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableDateDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableDateDim.Rows)
                                {
                                    string year = row.Field<string>("year");
                                    if (year.Equals(dYear))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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

            #region Step 3, 4 & 5

            sw.Reset();
            sw.Start();
            long dgkLengthCustomer = tempTableCustomerDim.Rows.Count + 1;
            long dgkLengthSupplier = tempTableSupplierDim.Rows.Count + 1;
            long dgkLengthDate = tempTableDateDim.Rows.Count + 1;

            long[,,] inMemoryAccumulator = new long[dgkLengthCustomer, dgkLengthSupplier, dgkLengthDate];

            List<Tuple<int, int>> partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loCustomerKey.Count, parallelOptions.MaxDegreeOfParallelism);

            List<Task> tasks = new List<Task>();
            foreach (Tuple<int, int> indexes in partitionIndexes)
            {
                Task t = Task.Factory.StartNew(() =>
                {
                    for (int i = indexes.Item1; i <= indexes.Item2; i++)
                    {
                        long custKey = InMemoryData.loCustomerKey[i];
                        long suppKey = InMemoryData.loSupplierKey[i];
                        long dateKey = InMemoryData.loOrderDate[i];
                        if (kvCustomerDim.TryGetValue(custKey, out long dgkCustomerDim)
                            && kvSupplierDim.TryGetValue(suppKey, out long dgkSupplierDim)
                            && kvDateDim.TryGetValue(dateKey, out long dgkDateDim))
                        {
                            if (dgkCustomerDim == 0 || dgkSupplierDim == 0 || dgkDateDim == 0)
                            {
                                // skip
                            }
                            else
                            {
                                inMemoryAccumulator[dgkCustomerDim, dgkSupplierDim, dgkDateDim] += InMemoryData.loRevenue[i];
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
                        long sumRevenue = inMemoryAccumulator[cdRow.Field<long>("denseGroupingKey")
                            , sdRow.Field<long>("denseGroupingKey")
                            , ddRow.Field<long>("denseGroupingKey")];
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
            Console.WriteLine(string.Format("[PIMA] Total Time: {0}", t1 + t2));
            Console.WriteLine(string.Format("[PIMA] Total Count: {0}", finalTable.Count));
            Console.WriteLine();
            #endregion Step 3, 4 & 5
            testResults.phase1Time = t1;
            testResults.phase2Time = t2;
            testResults.phase3Time = 0; ;
        }

        public void Query_3_4_IM()
        {

            Stopwatch sw = new Stopwatch();

            #region Step 1 & 2
            sw.Start();
            Dictionary<long, long> kvCustomerDim = new Dictionary<long, long>();
            DataTable tempTableCustomerDim = new DataTable();
            tempTableCustomerDim.Columns.Add("customerCity", typeof(string));
            tempTableCustomerDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvSupplierDim = new Dictionary<long, long>();
            DataTable tempTableSupplierDim = new DataTable();
            tempTableSupplierDim.Columns.Add("supplierCity", typeof(string));
            tempTableSupplierDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvDateDim = new Dictionary<long, long>();
            DataTable tempTableDateDim = new DataTable();
            tempTableDateDim.Columns.Add("year", typeof(string));
            tempTableDateDim.Columns.Add("denseGroupingKey", typeof(long));

            Parallel.Invoke(parallelOptions,
                () =>
                {
                    long customerIndex = 0;
                    long dgKeyCustomer = 0;
                    foreach (Customer customer in InMemoryData.customerDimension)
                    {
                        if (customer.cCity.Equals("UNITED KI1") || customer.cCity.Equals("UNITED KI5"))
                        {
                            string cCity = customer.cCity;
                            if (tempTableCustomerDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableCustomerDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableCustomerDim.Rows)
                                {
                                    string customerCity = row.Field<string>("customerCity");
                                    if (customerCity.Equals(cCity))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
                    long supplierIndex = 0;
                    long dgKeySupplier = 0;
                    foreach (Supplier supplier in InMemoryData.supplierDimension)
                    {
                        if (supplier.sCity.Equals("UNITED KI1") || supplier.sCity.Equals("UNITED KI5"))
                        {
                            string sCity = supplier.sCity;
                            if (tempTableSupplierDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableSupplierDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableSupplierDim.Rows)
                                {
                                    string supplierCity = row.Field<string>("supplierCity");
                                    if (supplierCity.Equals(sCity))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
                    long dgKeyDate = 0;
                    foreach (Date date in InMemoryData.dateDimension)
                    {
                        if (date.dYearMonth.Equals("Dec1997"))
                        {
                            string dYear = date.dYear;
                            if (tempTableDateDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableDateDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableDateDim.Rows)
                                {
                                    string year = row.Field<string>("year");
                                    if (year.Equals(dYear))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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

            #region Step 3, 4 & 5

            sw.Reset();
            sw.Start();
            long dgkLengthCustomer = tempTableCustomerDim.Rows.Count + 1;
            long dgkLengthSupplier = tempTableSupplierDim.Rows.Count + 1;
            long dgkLengthDate = tempTableDateDim.Rows.Count + 1;

            long[,,] inMemoryAccumulator = new long[dgkLengthCustomer, dgkLengthSupplier, dgkLengthDate];

            List<Tuple<int, int>> partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loCustomerKey.Count, parallelOptions.MaxDegreeOfParallelism);

            List<Task> tasks = new List<Task>();
            foreach (Tuple<int, int> indexes in partitionIndexes)
            {
                Task t = Task.Factory.StartNew(() =>
                {
                    for (int i = indexes.Item1; i <= indexes.Item2; i++)
                    {
                        long custKey = InMemoryData.loCustomerKey[i];
                        long suppKey = InMemoryData.loSupplierKey[i];
                        long dateKey = InMemoryData.loOrderDate[i];
                        if (kvCustomerDim.TryGetValue(custKey, out long dgkCustomerDim)
                            && kvSupplierDim.TryGetValue(suppKey, out long dgkSupplierDim)
                            && kvDateDim.TryGetValue(dateKey, out long dgkDateDim))
                        {
                            if (dgkCustomerDim == 0 || dgkSupplierDim == 0 || dgkDateDim == 0)
                            {
                                // skip
                            }
                            else
                            {
                                inMemoryAccumulator[dgkCustomerDim, dgkSupplierDim, dgkDateDim] += InMemoryData.loRevenue[i];
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
                        long sumRevenue = inMemoryAccumulator[cdRow.Field<long>("denseGroupingKey")
                            , sdRow.Field<long>("denseGroupingKey")
                            , ddRow.Field<long>("denseGroupingKey")];
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
            Console.WriteLine(string.Format("[PIMA] Total Time: {0}", t1 + t2));
            Console.WriteLine(string.Format("[PIMA] Total Count: {0}", finalTable.Count));
            Console.WriteLine();
            #endregion Step 3, 4 & 5
            testResults.phase1Time = t1;
            testResults.phase2Time = t2;
            testResults.phase3Time = 0; ;
        }

        public void Query_4_1_IM()
        {

            Stopwatch sw = new Stopwatch();

            #region Step 1 & 2
            sw.Start();
            Dictionary<long, long> kvCustomerDim = new Dictionary<long, long>();
            DataTable tempTableCustomerDim = new DataTable();
            tempTableCustomerDim.Columns.Add("customerNation", typeof(string));
            tempTableCustomerDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvSupplierDim = new Dictionary<long, long>();
            DataTable tempTableSupplierDim = new DataTable();
            tempTableSupplierDim.Columns.Add("supplierNation", typeof(string));
            tempTableSupplierDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvDateDim = new Dictionary<long, long>();
            DataTable tempTableDateDim = new DataTable();
            tempTableDateDim.Columns.Add("year", typeof(string));
            tempTableDateDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvPartDim = new Dictionary<long, long>();
            DataTable tempTablePartDim = new DataTable();
            tempTablePartDim.Columns.Add("partMFGR", typeof(string));
            tempTablePartDim.Columns.Add("denseGroupingKey", typeof(long));

            Parallel.Invoke(parallelOptions,
                () =>
                {
                    long customerIndex = 0;
                    long dgKeyCustomer = 0;
                    foreach (Customer customer in InMemoryData.customerDimension)
                    {
                        if (customer.cRegion.Equals("AMERICA"))
                        {
                            string cNation = customer.cNation;
                            if (tempTableCustomerDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableCustomerDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableCustomerDim.Rows)
                                {
                                    string customerNation = row.Field<string>("customerNation");
                                    if (customerNation.Equals(cNation))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
                    long supplierIndex = 0;
                    long dgKeySupplier = 0;
                    foreach (Supplier supplier in InMemoryData.supplierDimension)
                    {
                        if (supplier.sRegion.Equals("AMERICA"))
                        {
                            string sNation = supplier.sNation;
                            if (tempTableSupplierDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableSupplierDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableSupplierDim.Rows)
                                {
                                    string supplierNation = row.Field<string>("supplierNation");
                                    if (supplierNation.Equals(sNation))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
                    long dgKeyDate = 0;
                    foreach (Date date in InMemoryData.dateDimension)
                    {
                        string dYear = date.dYear;
                        if (tempTableDateDim.Rows.Count > 0)
                        {
                            DataTable tempTable = tempTableDateDim.Copy();
                            bool found = false;
                            foreach (DataRow row in tempTableDateDim.Rows)
                            {
                                string year = row.Field<string>("year");
                                if (year.Equals(dYear))
                                {
                                    long dgKey = row.Field<long>("denseGroupingKey");
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
                    long partIndex = 0;
                    long dgKeyPart = 0;
                    foreach (Part part in InMemoryData.partDimension)
                    {
                        if (part.pMFGR.Equals("MFGR#1") || part.pMFGR.Equals("MFGR#2"))
                        {
                            string pMFGR = part.pMFGR;
                            if (tempTablePartDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTablePartDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTablePartDim.Rows)
                                {
                                    string partMFGR = row.Field<string>("partMFGR");
                                    if (partMFGR.Equals(pMFGR))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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

            #region Step 3, 4 & 5

            sw.Reset();
            sw.Start();
            long dgkLengthCustomer = tempTableCustomerDim.Rows.Count + 1;
            long dgkLengthSupplier = tempTableSupplierDim.Rows.Count + 1;
            long dgkLengthDate = tempTableDateDim.Rows.Count + 1;
            long dgkLengthPart = tempTableDateDim.Rows.Count + 1;

            long[,] inMemoryAccumulator = new long[dgkLengthCustomer, dgkLengthDate];

            List<Tuple<int, int>> partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);

            List<Task> tasks = new List<Task>();
            foreach (Tuple<int, int> indexes in partitionIndexes)
            {
                Task t = Task.Factory.StartNew(() =>
                {
                    for (int i = indexes.Item1; i <= indexes.Item2; i++)
                    {
                        long custKey = InMemoryData.loCustomerKey[i];
                        long dateKey = InMemoryData.loOrderDate[i];
                        long suppKey = InMemoryData.loSupplierKey[i];
                        long partKey = InMemoryData.loPartKey[i];
                        if (kvCustomerDim.TryGetValue(custKey, out long dgkCustomerDim)
                            && kvDateDim.TryGetValue(dateKey, out long dgkDateDim)
                            && kvSupplierDim.ContainsKey(suppKey)
                            && kvPartDim.ContainsKey(partKey))
                        {
                            if (dgkCustomerDim == 0 || dgkDateDim == 0)
                            {
                                // skip
                            }
                            else
                            {
                                inMemoryAccumulator[dgkCustomerDim, dgkDateDim] += (InMemoryData.loRevenue[i] - InMemoryData.loSupplyCost[i]);
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
                    long sumProfit = inMemoryAccumulator[cdRow.Field<long>("denseGroupingKey")
                        , ddRow.Field<long>("denseGroupingKey")];
                    if (sumProfit != 0)
                    {
                        finalTable.Add(ddRow.Field<string>("year") + ", " + cdRow.Field<string>("customerNation") + ", " + sumProfit);
                    }
                }

            }

            sw.Stop();
            long t2 = sw.ElapsedMilliseconds;
            Console.WriteLine("[PIMA] Phase2 Time: " + t2);
            Console.WriteLine(string.Format("[PIMA] Total Time: {0}", t1 + t2));
            Console.WriteLine(string.Format("[PIMA] Total Count: {0}", finalTable.Count));
            Console.WriteLine();
            #endregion Step 3, 4 & 5
            testResults.phase1Time = t1;
            testResults.phase2Time = t2;
            testResults.phase3Time = 0; ;
        }

        public void Query_4_2_IM()
        {

            Stopwatch sw = new Stopwatch();

            #region Step 1 & 2
            sw.Start();
            Dictionary<long, long> kvCustomerDim = new Dictionary<long, long>();
            DataTable tempTableCustomerDim = new DataTable();
            tempTableCustomerDim.Columns.Add("customerNation", typeof(string));
            tempTableCustomerDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvSupplierDim = new Dictionary<long, long>();
            DataTable tempTableSupplierDim = new DataTable();
            tempTableSupplierDim.Columns.Add("supplierNation", typeof(string));
            tempTableSupplierDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvDateDim = new Dictionary<long, long>();
            DataTable tempTableDateDim = new DataTable();
            tempTableDateDim.Columns.Add("year", typeof(string));
            tempTableDateDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvPartDim = new Dictionary<long, long>();
            DataTable tempTablePartDim = new DataTable();
            tempTablePartDim.Columns.Add("partCategory", typeof(string));
            tempTablePartDim.Columns.Add("denseGroupingKey", typeof(long));

            Parallel.Invoke(parallelOptions,
                () =>
                {
                    long customerIndex = 0;
                    long dgKeyCustomer = 0;
                    foreach (Customer customer in InMemoryData.customerDimension)
                    {
                        if (customer.cRegion.Equals("AMERICA"))
                        {
                            string cNation = customer.cNation;
                            if (tempTableCustomerDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableCustomerDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableCustomerDim.Rows)
                                {
                                    string customerNation = row.Field<string>("customerNation");
                                    if (customerNation.Equals(cNation))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
                    long supplierIndex = 0;
                    long dgKeySupplier = 0;
                    foreach (Supplier supplier in InMemoryData.supplierDimension)
                    {
                        if (supplier.sRegion.Equals("AMERICA"))
                        {
                            string sNation = supplier.sNation;
                            if (tempTableSupplierDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableSupplierDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableSupplierDim.Rows)
                                {
                                    string supplierNation = row.Field<string>("supplierNation");
                                    if (supplierNation.Equals(sNation))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
                    long dgKeyDate = 0;
                    foreach (Date date in InMemoryData.dateDimension)
                    {
                        if (date.dYear.Equals("1997") || date.dYear.Equals("1998"))
                        {
                            string dYear = date.dYear;
                            if (tempTableDateDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableDateDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableDateDim.Rows)
                                {
                                    string year = row.Field<string>("year");
                                    if (year.Equals(dYear))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
                    long partIndex = 0;
                    long dgKeyPart = 0;
                    foreach (Part part in InMemoryData.partDimension)
                    {
                        if (part.pMFGR.Equals("MFGR#1") || part.pMFGR.Equals("MFGR#2"))
                        {
                            string pCategory = part.pCategory;
                            if (tempTablePartDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTablePartDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTablePartDim.Rows)
                                {
                                    string partCategory = row.Field<string>("partCategory");
                                    if (partCategory.Equals(pCategory))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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

            #region Step 3, 4 & 5

            sw.Reset();
            sw.Start();
            long dgkLengthSupplier = tempTableSupplierDim.Rows.Count + 1;
            long dgkLengthDate = tempTableDateDim.Rows.Count + 1;
            long dgkLengthPart = tempTablePartDim.Rows.Count + 1;

            long[,,] inMemoryAccumulator = new long[dgkLengthDate, dgkLengthSupplier, dgkLengthPart];

            List<Tuple<int, int>> partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);

            List<Task> tasks = new List<Task>();
            foreach (Tuple<int, int> indexes in partitionIndexes)
            {
                Task t = Task.Factory.StartNew(() =>
                {
                    for (int i = indexes.Item1; i <= indexes.Item2; i++)
                    {
                        long suppKey = InMemoryData.loSupplierKey[i];
                        long dateKey = InMemoryData.loOrderDate[i];
                        long partKey = InMemoryData.loPartKey[i];
                        long custKey = InMemoryData.loCustomerKey[i];
                        if (kvSupplierDim.TryGetValue(suppKey, out long dgkSupplierDim)
                            && kvDateDim.TryGetValue(dateKey, out long dgkDateDim)
                            && kvPartDim.TryGetValue(partKey, out long dgkPartDim)
                            && kvCustomerDim.ContainsKey(custKey))
                        {
                            if (dgkSupplierDim == 0 || dgkDateDim == 0 || dgkPartDim == 0)
                            {
                                // skip
                            }
                            else
                            {
                                inMemoryAccumulator[dgkDateDim, dgkSupplierDim, dgkPartDim] += (InMemoryData.loRevenue[i] - InMemoryData.loSupplyCost[i]);
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
                        long sumProfit = inMemoryAccumulator[ddRow.Field<long>("denseGroupingKey"), sdRow.Field<long>("denseGroupingKey")
                                                , pdRow.Field<long>("denseGroupingKey")];
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
            Console.WriteLine(string.Format("[PIMA] Total Time: {0}", t1 + t2));
            Console.WriteLine(string.Format("[PIMA] Total Count: {0}", finalTable.Count));
            Console.WriteLine();
            #endregion Step 3, 4 & 5
            testResults.phase1Time = t1;
            testResults.phase2Time = t2;
            testResults.phase3Time = 0; ;
        }

        public void Query_4_3_IM()
        {

            Stopwatch sw = new Stopwatch();

            #region Step 1 & 2
            sw.Start();
            Dictionary<long, long> kvCustomerDim = new Dictionary<long, long>();
            DataTable tempTableCustomerDim = new DataTable();
            tempTableCustomerDim.Columns.Add("customerNation", typeof(string));
            tempTableCustomerDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvSupplierDim = new Dictionary<long, long>();
            DataTable tempTableSupplierDim = new DataTable();
            tempTableSupplierDim.Columns.Add("supplierCity", typeof(string));
            tempTableSupplierDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvDateDim = new Dictionary<long, long>();
            DataTable tempTableDateDim = new DataTable();
            tempTableDateDim.Columns.Add("year", typeof(string));
            tempTableDateDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvPartDim = new Dictionary<long, long>();
            DataTable tempTablePartDim = new DataTable();
            tempTablePartDim.Columns.Add("partBrand", typeof(string));
            tempTablePartDim.Columns.Add("denseGroupingKey", typeof(long));

            Parallel.Invoke(parallelOptions,
                () =>
                {
                    long customerIndex = 0;
                    long dgKeyCustomer = 0;
                    foreach (Customer customer in InMemoryData.customerDimension)
                    {
                        if (customer.cRegion.Equals("AMERICA"))
                        {
                            string cNation = customer.cNation;
                            if (tempTableCustomerDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableCustomerDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableCustomerDim.Rows)
                                {
                                    string customerNation = row.Field<string>("customerNation");
                                    if (customerNation.Equals(cNation))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
                    long supplierIndex = 0;
                    long dgKeySupplier = 0;
                    foreach (Supplier supplier in InMemoryData.supplierDimension)
                    {
                        if (supplier.sNation.Equals("UNITED STATES"))
                        {
                            string sCity = supplier.sCity;
                            if (tempTableSupplierDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableSupplierDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableSupplierDim.Rows)
                                {
                                    string supplierCity = row.Field<string>("supplierCity");
                                    if (supplierCity.Equals(sCity))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
                    long dgKeyDate = 0;
                    foreach (Date date in InMemoryData.dateDimension)
                    {
                        if (date.dYear.Equals("1997") || date.dYear.Equals("1998"))
                        {
                            string dYear = date.dYear;
                            if (tempTableDateDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableDateDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableDateDim.Rows)
                                {
                                    string year = row.Field<string>("year");
                                    if (year.Equals(dYear))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
                    long partIndex = 0;
                    long dgKeyPart = 0;
                    foreach (Part part in InMemoryData.partDimension)
                    {
                        if (part.pCategory.Equals("MFGR#14"))
                        {
                            string pBrand = part.pBrand;
                            if (tempTablePartDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTablePartDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTablePartDim.Rows)
                                {
                                    string partBrand = row.Field<string>("partBrand");
                                    if (partBrand.Equals(pBrand))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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

            #region Step 3, 4 & 5

            sw.Reset();
            sw.Start();
            long dgkLengthSupplier = tempTableSupplierDim.Rows.Count + 1;
            long dgkLengthDate = tempTableDateDim.Rows.Count + 1;
            long dgkLengthPart = tempTablePartDim.Rows.Count + 1;

            long[,,] inMemoryAccumulator = new long[dgkLengthDate, dgkLengthSupplier, dgkLengthPart];

            List<Tuple<int, int>> partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loCustomerKey.Count, parallelOptions.MaxDegreeOfParallelism);

            List<Task> tasks = new List<Task>();
            foreach (Tuple<int, int> indexes in partitionIndexes)
            {
                Task t = Task.Factory.StartNew(() =>
                {
                    for (int i = indexes.Item1; i <= indexes.Item2; i++)
                    {
                        long suppKey = InMemoryData.loSupplierKey[i];
                        long dateKey = InMemoryData.loOrderDate[i];
                        long partKey = InMemoryData.loPartKey[i];
                        long custKey = InMemoryData.loCustomerKey[i];
                        if (kvSupplierDim.TryGetValue(suppKey, out long dgkSupplierDim)
                            && kvDateDim.TryGetValue(dateKey, out long dgkDateDim)
                            && kvPartDim.TryGetValue(partKey, out long dgkPartDim)
                            && kvCustomerDim.ContainsKey(custKey))
                        {
                            if (dgkSupplierDim == 0 || dgkDateDim == 0 || dgkPartDim == 0)
                            {
                                // skip
                            }
                            else
                            {
                                inMemoryAccumulator[dgkDateDim, dgkSupplierDim, dgkPartDim] += (InMemoryData.loRevenue[i] - InMemoryData.loSupplyCost[i]);
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
                        long sumProfit = inMemoryAccumulator[ddRow.Field<long>("denseGroupingKey"), sdRow.Field<long>("denseGroupingKey")
                                                , pdRow.Field<long>("denseGroupingKey")];
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
            Console.WriteLine(string.Format("[PIMA] Total Time: {0}", t1 + t2));
            Console.WriteLine(string.Format("[PIMA] Total Count: {0}", finalTable.Count));
            Console.WriteLine();
            #endregion Step 3, 4 & 5
            testResults.phase1Time = t1;
            testResults.phase2Time = t2;
            testResults.phase3Time = 0;
        }

        public void GroupingAttributeScalabilityTest(long numberOfGroupingAttributes)
        {
            long memoryStartPhase3 = GC.GetTotalMemory(true);
            switch (numberOfGroupingAttributes)
            {
                case 1:
                    GAS1();
                    break;
                case 2:
                    GAS2();
                    break;
                case 3:
                    GAS3();
                    break;
                case 4:
                    GAS4();
                    break;
                case 5:
                    GAS5();
                    break;
                case 6:
                    GAS6();
                    break;
                case 7:
                    GAS7();
                    break;
                case 8:
                    GAS8();
                    break;
                case 9:
                    GAS9();
                    break;
                case 10:
                    GAS10();
                    break;

                default:
                    break;
            }
            long memoryUsed = GC.GetTotalMemory(true) - memoryStartPhase3;
            testResults.memoryUsed = Convert.ToString( memoryUsed);
            Console.WriteLine(String.Format("[PIMA] GSTest Memory Used : {0}", memoryUsed));
            Console.WriteLine();

        }

        public void GAS1()
        {
            Stopwatch sw = new Stopwatch();

            #region Step 1 & 2
            sw.Start();
            Dictionary<long, long> kvCustomerDim = new Dictionary<long, long>();
            Dictionary<long, long> kvSupplierDim = new Dictionary<long, long>();

            Dictionary<long, long> kvDateDim = new Dictionary<long, long>();
            DataTable tempTableDateDim = new DataTable();
            tempTableDateDim.Columns.Add("year", typeof(string));
            tempTableDateDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvPartDim = new Dictionary<long, long>();

            Parallel.Invoke(parallelOptions,
                () =>
                {
                    long customerIndex = 0;
                    long dgKeyCustomer = 0;
                    foreach (Customer customer in InMemoryData.customerDimension)
                    {
                        if (customer.cRegion.Equals("AMERICA"))
                        {
                            kvCustomerDim.Add(customerIndex + 1, dgKeyCustomer);
                        }
                        customerIndex++;
                    }
                },
                () =>
                {
                    long supplierIndex = 0;
                    long dgKeySupplier = 0;
                    foreach (Supplier supplier in InMemoryData.supplierDimension)
                    {
                        if (supplier.sNation.Equals("UNITED STATES"))
                        {
                            kvSupplierDim.Add(supplierIndex + 1, dgKeySupplier);
                        }
                        supplierIndex++;
                    }
                },
                () =>
                {
                    long dgKeyDate = 0;
                    foreach (Date date in InMemoryData.dateDimension)
                    {
                        if (date.dYear.Equals("1997") || date.dYear.Equals("1998"))
                        {
                            string dYear = date.dYear;
                            if (tempTableDateDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableDateDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableDateDim.Rows)
                                {
                                    string year = row.Field<string>("year");
                                    if (year.Equals(dYear))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
                    long partIndex = 0;
                    long dgKeyPart = 0;
                    foreach (Part part in InMemoryData.partDimension)
                    {
                        if (part.pCategory.Equals("MFGR#14"))
                        {
                            kvPartDim.Add(partIndex + 1, dgKeyPart);
                        }
                        partIndex++;
                    }
                });

            sw.Stop();
            long t1 = sw.ElapsedMilliseconds;
            Console.WriteLine("[PIMA] GSTest Phase1 Time: " + t1);
            #endregion Step 1 & 2

            #region Step 3, 4 & 5

            sw.Reset();
            sw.Start();

            long dgkLengthDate = tempTableDateDim.Rows.Count + 1;


            long[] inMemoryAccumulator = new long[dgkLengthDate];

            List<Tuple<int, int>> partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loCustomerKey.Count, parallelOptions.MaxDegreeOfParallelism);

            List<Task> tasks = new List<Task>();
            foreach (Tuple<int, int> indexes in partitionIndexes)
            {
                Task t = Task.Factory.StartNew(() =>
                {
                    for (int i = indexes.Item1; i <= indexes.Item2; i++)
                    {
                        long suppKey = InMemoryData.loSupplierKey[i];
                        long dateKey = InMemoryData.loOrderDate[i];
                        long partKey = InMemoryData.loPartKey[i];
                        long custKey = InMemoryData.loCustomerKey[i];
                        if (kvSupplierDim.TryGetValue(suppKey, out long dgkSupplierDim)
                            && kvDateDim.TryGetValue(dateKey, out long dgkDateDim)
                            && kvPartDim.TryGetValue(partKey, out long dgkPartDim)
                            && kvCustomerDim.TryGetValue(custKey, out long dgkCustomerDim))
                        {
                            if (dgkDateDim == 0)
                            {
                                // skip
                            }
                            else
                            {
                                inMemoryAccumulator[dgkDateDim] += (InMemoryData.loRevenue[i] - InMemoryData.loSupplyCost[i]);
                            }
                        }
                    }
                });
                tasks.Add(t);
            }

            Task.WaitAll(tasks.ToArray());

            List<string> finalTable = new List<string>();
            foreach (DataRow ddRow in tempTableDateDim.Rows)
            {
                long sumProfit = inMemoryAccumulator[ddRow.Field<long>("denseGroupingKey")];
                if (sumProfit != 0)
                {
                    finalTable.Add(ddRow.Field<string>("year") + ", " + sumProfit);
                }
            }

            sw.Stop();
            long t2 = sw.ElapsedMilliseconds;
            Console.WriteLine("[PIMA] GSTest Phase2 Time: " + t2);
            Console.WriteLine(string.Format("[PIMA] GSTest Total Time: {0}", t1 + t2));
            // Console.WriteLine(string.Format("[PIMA] Total Count: {0}", finalTable.Count));
            Console.WriteLine();
            #endregion Step 3, 4 & 5
            testResults.phase1Time = t1;
            testResults.phase2Time = t2;
            testResults.phase3Time = 0;
        }

        public void GAS2()
        {
            Stopwatch sw = new Stopwatch();

            #region Step 1 & 2
            sw.Start();
            Dictionary<long, long> kvCustomerDim = new Dictionary<long, long>();
            Dictionary<long, long> kvSupplierDim = new Dictionary<long, long>();

            Dictionary<long, long> kvDateDim = new Dictionary<long, long>();
            DataTable tempTableDateDim = new DataTable();
            tempTableDateDim.Columns.Add("year", typeof(string));
            tempTableDateDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvPartDim = new Dictionary<long, long>();

            Parallel.Invoke(parallelOptions,
                () =>
                {
                    long customerIndex = 0;
                    long dgKeyCustomer = 0;
                    foreach (Customer customer in InMemoryData.customerDimension)
                    {
                        if (customer.cRegion.Equals("AMERICA"))
                        {
                            kvCustomerDim.Add(customerIndex + 1, dgKeyCustomer);
                        }
                        customerIndex++;
                    }
                },
                () =>
                {
                    long supplierIndex = 0;
                    long dgKeySupplier = 0;
                    foreach (Supplier supplier in InMemoryData.supplierDimension)
                    {
                        if (supplier.sNation.Equals("UNITED STATES"))
                        {
                            kvSupplierDim.Add(supplierIndex + 1, dgKeySupplier);
                        }
                        supplierIndex++;
                    }
                },
                () =>
                {
                    long dgKeyDate = 0;
                    foreach (Date date in InMemoryData.dateDimension)
                    {
                        if (date.dYear.Equals("1997") || date.dYear.Equals("1998"))
                        {
                            string dYear = date.dYear + date.dMonth;
                            if (tempTableDateDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableDateDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableDateDim.Rows)
                                {
                                    string year = row.Field<string>("year");
                                    if (year.Equals(dYear))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
                    long partIndex = 0;
                    long dgKeyPart = 0;
                    foreach (Part part in InMemoryData.partDimension)
                    {
                        if (part.pCategory.Equals("MFGR#14"))
                        {
                            kvPartDim.Add(partIndex + 1, dgKeyPart);
                        }
                        partIndex++;
                    }
                });

            sw.Stop();
            long t1 = sw.ElapsedMilliseconds;
            Console.WriteLine("[PIMA] GSTest Phase1 Time: " + t1);
            #endregion Step 1 & 2

            #region Step 3, 4 & 5

            sw.Reset();
            sw.Start();

            long dgkLengthDate = tempTableDateDim.Rows.Count + 1;


            long[] inMemoryAccumulator = new long[dgkLengthDate];

            List<Tuple<int, int>> partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loCustomerKey.Count, parallelOptions.MaxDegreeOfParallelism);

            List<Task> tasks = new List<Task>();
            foreach (Tuple<int, int> indexes in partitionIndexes)
            {
                Task t = Task.Factory.StartNew(() =>
                {
                    for (int i = indexes.Item1; i <= indexes.Item2; i++)
                    {
                        long suppKey = InMemoryData.loSupplierKey[i];
                        long dateKey = InMemoryData.loOrderDate[i];
                        long partKey = InMemoryData.loPartKey[i];
                        long custKey = InMemoryData.loCustomerKey[i];
                        if (kvSupplierDim.TryGetValue(suppKey, out long dgkSupplierDim)
                            && kvDateDim.TryGetValue(dateKey, out long dgkDateDim)
                            && kvPartDim.TryGetValue(partKey, out long dgkPartDim)
                            && kvCustomerDim.TryGetValue(custKey, out long dgkCustomerDim))
                        {
                            if (dgkDateDim == 0)
                            {
                                // skip
                            }
                            else
                            {
                                inMemoryAccumulator[dgkDateDim] += (InMemoryData.loRevenue[i] - InMemoryData.loSupplyCost[i]);
                            }
                        }
                    }
                });
                tasks.Add(t);
            }

            Task.WaitAll(tasks.ToArray());

            List<string> finalTable = new List<string>();
            foreach (DataRow ddRow in tempTableDateDim.Rows)
            {
                long sumProfit = inMemoryAccumulator[ddRow.Field<long>("denseGroupingKey")];
                if (sumProfit != 0)
                {
                    finalTable.Add(ddRow.Field<string>("year") + ", " + sumProfit);
                }
            }

            sw.Stop();
            long t2 = sw.ElapsedMilliseconds;
            Console.WriteLine("[PIMA] GSTest Phase2 Time: " + t2);
            Console.WriteLine(string.Format("[PIMA] GSTest Total Time: {0}", t1 + t2));
            // Console.WriteLine(string.Format("[PIMA] Total Count: {0}", finalTable.Count));
            Console.WriteLine();
            #endregion Step 3, 4 & 5
            testResults.phase1Time = t1;
            testResults.phase2Time = t2;
            testResults.phase3Time = 0;
        }

        public void GAS3()
        {
            Stopwatch sw = new Stopwatch();

            #region Step 1 & 2
            sw.Start();
            Dictionary<long, long> kvCustomerDim = new Dictionary<long, long>();


            Dictionary<long, long> kvSupplierDim = new Dictionary<long, long>();
            DataTable tempTableSupplierDim = new DataTable();
            tempTableSupplierDim.Columns.Add("supplierCity", typeof(string));
            tempTableSupplierDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvDateDim = new Dictionary<long, long>();
            DataTable tempTableDateDim = new DataTable();
            tempTableDateDim.Columns.Add("year", typeof(string));
            tempTableDateDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvPartDim = new Dictionary<long, long>();

            Parallel.Invoke(parallelOptions,
                () =>
                {
                    long customerIndex = 0;
                    long dgKeyCustomer = 0;
                    foreach (Customer customer in InMemoryData.customerDimension)
                    {
                        if (customer.cRegion.Equals("AMERICA"))
                        {
                            kvCustomerDim.Add(customerIndex + 1, dgKeyCustomer);
                        }
                        customerIndex++;
                    }
                },
                () =>
                {
                    long supplierIndex = 0;
                    long dgKeySupplier = 0;
                    foreach (Supplier supplier in InMemoryData.supplierDimension)
                    {
                        if (supplier.sNation.Equals("UNITED STATES"))
                        {
                            string sCity = supplier.sNation;
                            if (tempTableSupplierDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableSupplierDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableSupplierDim.Rows)
                                {
                                    string supplierCity = row.Field<string>("supplierCity");
                                    if (supplierCity.Equals(sCity))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
                    long dgKeyDate = 0;
                    foreach (Date date in InMemoryData.dateDimension)
                    {
                        if (date.dYear.Equals("1997") || date.dYear.Equals("1998"))
                        {
                            string dYear = date.dYear + ", " + date.dMonth;
                            if (tempTableDateDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableDateDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableDateDim.Rows)
                                {
                                    string year = row.Field<string>("year");
                                    if (year.Equals(dYear))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
                    long partIndex = 0;
                    long dgKeyPart = 0;
                    foreach (Part part in InMemoryData.partDimension)
                    {
                        if (part.pCategory.Equals("MFGR#14"))
                        {
                            kvPartDim.Add(partIndex + 1, dgKeyPart);
                        }
                        partIndex++;
                    }
                });

            sw.Stop();
            long t1 = sw.ElapsedMilliseconds;
            Console.WriteLine("[PIMA] GSTest Phase1 Time: " + t1);
            #endregion Step 1 & 2

            #region Step 3, 4 & 5

            sw.Reset();
            sw.Start();
            long dgkLengthSupplier = tempTableSupplierDim.Rows.Count + 1;
            long dgkLengthDate = tempTableDateDim.Rows.Count + 1;


            long[,] inMemoryAccumulator = new long[dgkLengthDate, dgkLengthSupplier];

            List<Tuple<int, int>> partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loCustomerKey.Count, parallelOptions.MaxDegreeOfParallelism);

            List<Task> tasks = new List<Task>();
            foreach (Tuple<int, int> indexes in partitionIndexes)
            {
                Task t = Task.Factory.StartNew(() =>
                {
                    for (int i = indexes.Item1; i <= indexes.Item2; i++)
                    {
                        long suppKey = InMemoryData.loSupplierKey[i];
                        long dateKey = InMemoryData.loOrderDate[i];
                        long partKey = InMemoryData.loPartKey[i];
                        long custKey = InMemoryData.loCustomerKey[i];
                        if (kvSupplierDim.TryGetValue(suppKey, out long dgkSupplierDim)
                            && kvDateDim.TryGetValue(dateKey, out long dgkDateDim)
                            && kvPartDim.TryGetValue(partKey, out long dgkPartDim)
                            && kvCustomerDim.TryGetValue(custKey, out long dgkCustomerDim))
                        {
                            if (dgkSupplierDim == 0 || dgkDateDim == 0)
                            {
                                // skip
                            }
                            else
                            {
                                inMemoryAccumulator[dgkDateDim, dgkSupplierDim] += (InMemoryData.loRevenue[i] - InMemoryData.loSupplyCost[i]);
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
                    long sumProfit = inMemoryAccumulator[ddRow.Field<long>("denseGroupingKey"), sdRow.Field<long>("denseGroupingKey")];
                    if (sumProfit != 0)
                    {
                        finalTable.Add(ddRow.Field<string>("year") + ", " + sdRow.Field<string>("supplierCity") + ", " + sumProfit);
                    }
                }
            }

            sw.Stop();
            long t2 = sw.ElapsedMilliseconds;
            Console.WriteLine("[PIMA] GSTest Phase2 Time: " + t2);
            Console.WriteLine(string.Format("[PIMA] GSTest Total Time: {0}", t1 + t2));
            // Console.WriteLine(string.Format("[PIMA] Total Count: {0}", finalTable.Count));
            Console.WriteLine();
            #endregion Step 3, 4 & 5
            testResults.phase1Time = t1;
            testResults.phase2Time = t2;
            testResults.phase3Time = 0;
        }

        public void GAS4()
        {
            Stopwatch sw = new Stopwatch();

            #region Step 1 & 2
            sw.Start();
            Dictionary<long, long> kvCustomerDim = new Dictionary<long, long>();


            Dictionary<long, long> kvSupplierDim = new Dictionary<long, long>();
            DataTable tempTableSupplierDim = new DataTable();
            tempTableSupplierDim.Columns.Add("supplierCity", typeof(string));
            tempTableSupplierDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvDateDim = new Dictionary<long, long>();
            DataTable tempTableDateDim = new DataTable();
            tempTableDateDim.Columns.Add("year", typeof(string));
            tempTableDateDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvPartDim = new Dictionary<long, long>();

            Parallel.Invoke(parallelOptions,
                () =>
                {
                    long customerIndex = 0;
                    long dgKeyCustomer = 0;
                    foreach (Customer customer in InMemoryData.customerDimension)
                    {
                        if (customer.cRegion.Equals("AMERICA"))
                        {
                            kvCustomerDim.Add(customerIndex + 1, dgKeyCustomer);
                        }
                        customerIndex++;
                    }
                },
                () =>
                {
                    long supplierIndex = 0;
                    long dgKeySupplier = 0;
                    foreach (Supplier supplier in InMemoryData.supplierDimension)
                    {
                        if (supplier.sNation.Equals("UNITED STATES"))
                        {
                            string sCity = supplier.sNation + ", " + supplier.sRegion;
                            if (tempTableSupplierDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableSupplierDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableSupplierDim.Rows)
                                {
                                    string supplierCity = row.Field<string>("supplierCity");
                                    if (supplierCity.Equals(sCity))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
                    long dgKeyDate = 0;
                    foreach (Date date in InMemoryData.dateDimension)
                    {
                        if (date.dYear.Equals("1997") || date.dYear.Equals("1998"))
                        {
                            string dYear = date.dYear + ", " + date.dMonth;
                            if (tempTableDateDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableDateDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableDateDim.Rows)
                                {
                                    string year = row.Field<string>("year");
                                    if (year.Equals(dYear))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
                    long partIndex = 0;
                    long dgKeyPart = 0;
                    foreach (Part part in InMemoryData.partDimension)
                    {
                        if (part.pCategory.Equals("MFGR#14"))
                        {
                            kvPartDim.Add(partIndex + 1, dgKeyPart);
                        }
                        partIndex++;
                    }
                });

            sw.Stop();
            long t1 = sw.ElapsedMilliseconds;
            Console.WriteLine("[PIMA] GSTest Phase1 Time: " + t1);
            #endregion Step 1 & 2

            #region Step 3, 4 & 5

            sw.Reset();
            sw.Start();
            long dgkLengthSupplier = tempTableSupplierDim.Rows.Count + 1;
            long dgkLengthDate = tempTableDateDim.Rows.Count + 1;


            long[,] inMemoryAccumulator = new long[dgkLengthDate, dgkLengthSupplier];

            List<Tuple<int, int>> partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loCustomerKey.Count, parallelOptions.MaxDegreeOfParallelism);

            List<Task> tasks = new List<Task>();
            foreach (Tuple<int, int> indexes in partitionIndexes)
            {
                Task t = Task.Factory.StartNew(() =>
                {
                    for (int i = indexes.Item1; i <= indexes.Item2; i++)
                    {
                        long suppKey = InMemoryData.loSupplierKey[i];
                        long dateKey = InMemoryData.loOrderDate[i];
                        long partKey = InMemoryData.loPartKey[i];
                        long custKey = InMemoryData.loCustomerKey[i];
                        if (kvSupplierDim.TryGetValue(suppKey, out long dgkSupplierDim)
                            && kvDateDim.TryGetValue(dateKey, out long dgkDateDim)
                            && kvPartDim.TryGetValue(partKey, out long dgkPartDim)
                            && kvCustomerDim.TryGetValue(custKey, out long dgkCustomerDim))
                        {
                            if (dgkSupplierDim == 0 || dgkDateDim == 0)
                            {
                                // skip
                            }
                            else
                            {
                                inMemoryAccumulator[dgkDateDim, dgkSupplierDim] += (InMemoryData.loRevenue[i] - InMemoryData.loSupplyCost[i]);
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
                    long sumProfit = inMemoryAccumulator[ddRow.Field<long>("denseGroupingKey"), sdRow.Field<long>("denseGroupingKey")];
                    if (sumProfit != 0)
                    {
                        finalTable.Add(ddRow.Field<string>("year") + ", " + sdRow.Field<string>("supplierCity") + ", " + sumProfit);
                    }
                }
            }

            sw.Stop();
            long t2 = sw.ElapsedMilliseconds;
            Console.WriteLine("[PIMA] GSTest Phase2 Time: " + t2);
            Console.WriteLine(string.Format("[PIMA] GSTest Total Time: {0}", t1 + t2));
            // Console.WriteLine(string.Format("[PIMA] Total Count: {0}", finalTable.Count));
            Console.WriteLine();
            #endregion Step 3, 4 & 5
            testResults.phase1Time = t1;
            testResults.phase2Time = t2;
            testResults.phase3Time = 0;
        }

        public void GAS5()
        {
            Stopwatch sw = new Stopwatch();

            #region Step 1 & 2
            sw.Start();
            Dictionary<long, long> kvCustomerDim = new Dictionary<long, long>();


            Dictionary<long, long> kvSupplierDim = new Dictionary<long, long>();
            DataTable tempTableSupplierDim = new DataTable();
            tempTableSupplierDim.Columns.Add("supplierCity", typeof(string));
            tempTableSupplierDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvDateDim = new Dictionary<long, long>();
            DataTable tempTableDateDim = new DataTable();
            tempTableDateDim.Columns.Add("year", typeof(string));
            tempTableDateDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvPartDim = new Dictionary<long, long>();

            Parallel.Invoke(parallelOptions,
                () =>
                {
                    long customerIndex = 0;
                    long dgKeyCustomer = 0;
                    foreach (Customer customer in InMemoryData.customerDimension)
                    {
                        if (customer.cRegion.Equals("AMERICA"))
                        {
                            kvCustomerDim.Add(customerIndex + 1, dgKeyCustomer);
                        }
                        customerIndex++;
                    }
                },
                () =>
                {
                    long supplierIndex = 0;
                    long dgKeySupplier = 0;
                    foreach (Supplier supplier in InMemoryData.supplierDimension)
                    {
                        if (supplier.sNation.Equals("UNITED STATES"))
                        {
                            string sCity = supplier.sNation + ", " + supplier.sRegion + ", " + supplier.sCity;
                            if (tempTableSupplierDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableSupplierDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableSupplierDim.Rows)
                                {
                                    string supplierCity = row.Field<string>("supplierCity");
                                    if (supplierCity.Equals(sCity))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
                    long dgKeyDate = 0;
                    foreach (Date date in InMemoryData.dateDimension)
                    {
                        if (date.dYear.Equals("1997") || date.dYear.Equals("1998"))
                        {
                            string dYear = date.dYear + ", " + date.dMonth;
                            if (tempTableDateDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableDateDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableDateDim.Rows)
                                {
                                    string year = row.Field<string>("year");
                                    if (year.Equals(dYear))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
                    long partIndex = 0;
                    long dgKeyPart = 0;
                    foreach (Part part in InMemoryData.partDimension)
                    {
                        if (part.pCategory.Equals("MFGR#14"))
                        {
                            kvPartDim.Add(partIndex + 1, dgKeyPart);
                        }
                        partIndex++;
                    }
                });

            sw.Stop();
            long t1 = sw.ElapsedMilliseconds;
            Console.WriteLine("[PIMA] GSTest Phase1 Time: " + t1);
            #endregion Step 1 & 2

            #region Step 3, 4 & 5

            sw.Reset();
            sw.Start();
            long dgkLengthSupplier = tempTableSupplierDim.Rows.Count + 1;
            long dgkLengthDate = tempTableDateDim.Rows.Count + 1;


            long[,] inMemoryAccumulator = new long[dgkLengthDate, dgkLengthSupplier];

            List<Tuple<int, int>> partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loCustomerKey.Count, parallelOptions.MaxDegreeOfParallelism);

            List<Task> tasks = new List<Task>();
            foreach (Tuple<int, int> indexes in partitionIndexes)
            {
                Task t = Task.Factory.StartNew(() =>
                {
                    for (int i = indexes.Item1; i <= indexes.Item2; i++)
                    {
                        long suppKey = InMemoryData.loSupplierKey[i];
                        long dateKey = InMemoryData.loOrderDate[i];
                        long partKey = InMemoryData.loPartKey[i];
                        long custKey = InMemoryData.loCustomerKey[i];
                        if (kvSupplierDim.TryGetValue(suppKey, out long dgkSupplierDim)
                            && kvDateDim.TryGetValue(dateKey, out long dgkDateDim)
                            && kvPartDim.TryGetValue(partKey, out long dgkPartDim)
                            && kvCustomerDim.TryGetValue(custKey, out long dgkCustomerDim))
                        {
                            if (dgkSupplierDim == 0 || dgkDateDim == 0)
                            {
                                // skip
                            }
                            else
                            {
                                inMemoryAccumulator[dgkDateDim, dgkSupplierDim] += (InMemoryData.loRevenue[i] - InMemoryData.loSupplyCost[i]);
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
                    long sumProfit = inMemoryAccumulator[ddRow.Field<long>("denseGroupingKey"), sdRow.Field<long>("denseGroupingKey")];
                    if (sumProfit != 0)
                    {
                        finalTable.Add(ddRow.Field<string>("year") + ", " + sdRow.Field<string>("supplierCity") + ", " + sumProfit);
                    }
                }
            }

            sw.Stop();
            long t2 = sw.ElapsedMilliseconds;
            Console.WriteLine("[PIMA] GSTest Phase2 Time: " + t2);
            Console.WriteLine(string.Format("[PIMA] GSTest Total Time: {0}", t1 + t2));
            // Console.WriteLine(string.Format("[PIMA] Total Count: {0}", finalTable.Count));
            Console.WriteLine();
            #endregion Step 3, 4 & 5
            testResults.phase1Time = t1;
            testResults.phase2Time = t2;
            testResults.phase3Time = 0;
        }

        public void GAS6()
        {
            Stopwatch sw = new Stopwatch();

            #region Step 1 & 2
            sw.Start();
            Dictionary<long, long> kvCustomerDim = new Dictionary<long, long>();
            DataTable tempTableCustomerDim = new DataTable();
            tempTableCustomerDim.Columns.Add("customerNation", typeof(string));
            tempTableCustomerDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvSupplierDim = new Dictionary<long, long>();
            DataTable tempTableSupplierDim = new DataTable();
            tempTableSupplierDim.Columns.Add("supplierCity", typeof(string));
            tempTableSupplierDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvDateDim = new Dictionary<long, long>();
            DataTable tempTableDateDim = new DataTable();
            tempTableDateDim.Columns.Add("year", typeof(string));
            tempTableDateDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvPartDim = new Dictionary<long, long>();

            Parallel.Invoke(parallelOptions,
                () =>
                {
                    long customerIndex = 0;
                    long dgKeyCustomer = 0;
                    foreach (Customer customer in InMemoryData.customerDimension)
                    {
                        if (customer.cRegion.Equals("AMERICA"))
                        {
                            string cNation = customer.cNation;
                            if (tempTableCustomerDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableCustomerDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableCustomerDim.Rows)
                                {
                                    string customerNation = row.Field<string>("customerNation");
                                    if (customerNation.Equals(cNation))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
                    long supplierIndex = 0;
                    long dgKeySupplier = 0;
                    foreach (Supplier supplier in InMemoryData.supplierDimension)
                    {
                        if (supplier.sNation.Equals("UNITED STATES"))
                        {
                            string sCity = supplier.sNation + ", " + supplier.sRegion + ", " + supplier.sCity;
                            if (tempTableSupplierDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableSupplierDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableSupplierDim.Rows)
                                {
                                    string supplierCity = row.Field<string>("supplierCity");
                                    if (supplierCity.Equals(sCity))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
                    long dgKeyDate = 0;
                    foreach (Date date in InMemoryData.dateDimension)
                    {
                        if (date.dYear.Equals("1997") || date.dYear.Equals("1998"))
                        {
                            string dYear = date.dYear + ", " + date.dMonth;
                            if (tempTableDateDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableDateDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableDateDim.Rows)
                                {
                                    string year = row.Field<string>("year");
                                    if (year.Equals(dYear))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
                    long partIndex = 0;
                    long dgKeyPart = 0;
                    foreach (Part part in InMemoryData.partDimension)
                    {
                        if (part.pCategory.Equals("MFGR#14"))
                        {
                            kvPartDim.Add(partIndex + 1, dgKeyPart);

                        }
                        partIndex++;
                    }
                });

            sw.Stop();
            long t1 = sw.ElapsedMilliseconds;
            Console.WriteLine("[PIMA] GSTest Phase1 Time: " + t1);
            #endregion Step 1 & 2

            #region Step 3, 4 & 5

            sw.Reset();
            sw.Start();
            long dgkLengthSupplier = tempTableSupplierDim.Rows.Count + 1;
            long dgkLengthDate = tempTableDateDim.Rows.Count + 1;
            long dgkLengthCustomer = tempTableCustomerDim.Rows.Count + 1;

            long[,,] inMemoryAccumulator = new long[dgkLengthDate, dgkLengthSupplier, dgkLengthCustomer];

            List<Tuple<int, int>> partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loCustomerKey.Count, parallelOptions.MaxDegreeOfParallelism);

            List<Task> tasks = new List<Task>();
            foreach (Tuple<int, int> indexes in partitionIndexes)
            {
                Task t = Task.Factory.StartNew(() =>
                {
                    for (int i = indexes.Item1; i <= indexes.Item2; i++)
                    {
                        long suppKey = InMemoryData.loSupplierKey[i];
                        long dateKey = InMemoryData.loOrderDate[i];
                        long partKey = InMemoryData.loPartKey[i];
                        long custKey = InMemoryData.loCustomerKey[i];
                        if (kvSupplierDim.TryGetValue(suppKey, out long dgkSupplierDim)
                            && kvDateDim.TryGetValue(dateKey, out long dgkDateDim)
                            && kvPartDim.TryGetValue(partKey, out long dgkPartDim)
                            && kvCustomerDim.TryGetValue(custKey, out long dgkCustomerDim))
                        {
                            if (dgkCustomerDim == 0 || dgkSupplierDim == 0 || dgkDateDim == 0 )
                            {
                                // skip
                            }
                            else
                            {
                                inMemoryAccumulator[dgkDateDim, dgkSupplierDim, dgkCustomerDim] += (InMemoryData.loRevenue[i] - InMemoryData.loSupplyCost[i]);
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
                        foreach (DataRow cdRow in tempTableCustomerDim.Rows)
                        {
                            long sumProfit = inMemoryAccumulator[ddRow.Field<long>("denseGroupingKey"), sdRow.Field<long>("denseGroupingKey")
                                                , cdRow.Field<long>("denseGroupingKey")];
                            if (sumProfit != 0)
                            {
                                finalTable.Add(ddRow.Field<string>("year") + ", " + sdRow.Field<string>("supplierCity") + ", " + sumProfit);
                            }
                        }
                }
            }

            sw.Stop();
            long t2 = sw.ElapsedMilliseconds;
            Console.WriteLine("[PIMA] GSTest Phase2 Time: " + t2);
            Console.WriteLine(string.Format("[PIMA] GSTest Total Time: {0}", t1 + t2));
            // Console.WriteLine(string.Format("[PIMA] Total Count: {0}", finalTable.Count));
            Console.WriteLine();
            #endregion Step 3, 4 & 5
            testResults.phase1Time = t1;
            testResults.phase2Time = t2;
            testResults.phase3Time = 0;
        }

        public void GAS7() {
            Stopwatch sw = new Stopwatch();

            #region Step 1 & 2
            sw.Start();
            Dictionary<long, long> kvCustomerDim = new Dictionary<long, long>();
            DataTable tempTableCustomerDim = new DataTable();
            tempTableCustomerDim.Columns.Add("customerNation", typeof(string));
            tempTableCustomerDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvSupplierDim = new Dictionary<long, long>();
            DataTable tempTableSupplierDim = new DataTable();
            tempTableSupplierDim.Columns.Add("supplierCity", typeof(string));
            tempTableSupplierDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvDateDim = new Dictionary<long, long>();
            DataTable tempTableDateDim = new DataTable();
            tempTableDateDim.Columns.Add("year", typeof(string));
            tempTableDateDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvPartDim = new Dictionary<long, long>();

            Parallel.Invoke(parallelOptions,
                () =>
                {
                    long customerIndex = 0;
                    long dgKeyCustomer = 0;
                    foreach (Customer customer in InMemoryData.customerDimension)
                    {
                        if (customer.cRegion.Equals("AMERICA"))
                        {
                            string cNation = customer.cNation + ", " + customer.cRegion;
                            if (tempTableCustomerDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableCustomerDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableCustomerDim.Rows)
                                {
                                    string customerNation = row.Field<string>("customerNation");
                                    if (customerNation.Equals(cNation))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
                    long supplierIndex = 0;
                    long dgKeySupplier = 0;
                    foreach (Supplier supplier in InMemoryData.supplierDimension)
                    {
                        if (supplier.sNation.Equals("UNITED STATES"))
                        {
                            string sCity = supplier.sNation + ", " + supplier.sRegion + ", " + supplier.sCity;
                            if (tempTableSupplierDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableSupplierDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableSupplierDim.Rows)
                                {
                                    string supplierCity = row.Field<string>("supplierCity");
                                    if (supplierCity.Equals(sCity))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
                    long dgKeyDate = 0;
                    foreach (Date date in InMemoryData.dateDimension)
                    {
                        if (date.dYear.Equals("1997") || date.dYear.Equals("1998"))
                        {
                            string dYear = date.dYear + ", " + date.dMonth;
                            if (tempTableDateDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableDateDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableDateDim.Rows)
                                {
                                    string year = row.Field<string>("year");
                                    if (year.Equals(dYear))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
                    long partIndex = 0;
                    long dgKeyPart = 0;
                    foreach (Part part in InMemoryData.partDimension)
                    {
                        if (part.pCategory.Equals("MFGR#14"))
                        {
                            kvPartDim.Add(partIndex + 1, dgKeyPart);

                        }
                        partIndex++;
                    }
                });

            sw.Stop();
            long t1 = sw.ElapsedMilliseconds;
            Console.WriteLine("[PIMA] GSTest Phase1 Time: " + t1);
            #endregion Step 1 & 2

            #region Step 3, 4 & 5

            sw.Reset();
            sw.Start();
            long dgkLengthSupplier = tempTableSupplierDim.Rows.Count + 1;
            long dgkLengthDate = tempTableDateDim.Rows.Count + 1;
            long dgkLengthCustomer = tempTableCustomerDim.Rows.Count + 1;

            long[,,] inMemoryAccumulator = new long[dgkLengthDate, dgkLengthSupplier, dgkLengthCustomer];

            List<Tuple<int, int>> partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loCustomerKey.Count, parallelOptions.MaxDegreeOfParallelism);

            List<Task> tasks = new List<Task>();
            foreach (Tuple<int, int> indexes in partitionIndexes)
            {
                Task t = Task.Factory.StartNew(() =>
                {
                    for (int i = indexes.Item1; i <= indexes.Item2; i++)
                    {
                        long suppKey = InMemoryData.loSupplierKey[i];
                        long dateKey = InMemoryData.loOrderDate[i];
                        long partKey = InMemoryData.loPartKey[i];
                        long custKey = InMemoryData.loCustomerKey[i];
                        if (kvSupplierDim.TryGetValue(suppKey, out long dgkSupplierDim)
                            && kvDateDim.TryGetValue(dateKey, out long dgkDateDim)
                            && kvPartDim.TryGetValue(partKey, out long dgkPartDim)
                            && kvCustomerDim.TryGetValue(custKey, out long dgkCustomerDim))
                        {
                            if (dgkCustomerDim == 0 || dgkSupplierDim == 0 || dgkDateDim == 0)
                            {
                                // skip
                            }
                            else
                            {
                                inMemoryAccumulator[dgkDateDim, dgkSupplierDim, dgkCustomerDim] += (InMemoryData.loRevenue[i] - InMemoryData.loSupplyCost[i]);
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
                    foreach (DataRow cdRow in tempTableCustomerDim.Rows)
                    {
                        long sumProfit = inMemoryAccumulator[ddRow.Field<long>("denseGroupingKey"), sdRow.Field<long>("denseGroupingKey")
                                            , cdRow.Field<long>("denseGroupingKey")];
                        if (sumProfit != 0)
                        {
                            finalTable.Add(ddRow.Field<string>("year") + ", " + sdRow.Field<string>("supplierCity") + ", " + sumProfit);
                        }
                    }
                }
            }

            sw.Stop();
            long t2 = sw.ElapsedMilliseconds;
            Console.WriteLine("[PIMA] GSTest Phase2 Time: " + t2);
            Console.WriteLine(string.Format("[PIMA] GSTest Total Time: {0}", t1 + t2));
            // Console.WriteLine(string.Format("[PIMA] Total Count: {0}", finalTable.Count));
            Console.WriteLine();
            #endregion Step 3, 4 & 5
            testResults.phase1Time = t1;
            testResults.phase2Time = t2;
            testResults.phase3Time = 0;
        }

        public void GAS8() {
            Stopwatch sw = new Stopwatch();

            #region Step 1 & 2
            sw.Start();
            Dictionary<long, long> kvCustomerDim = new Dictionary<long, long>();
            DataTable tempTableCustomerDim = new DataTable();
            tempTableCustomerDim.Columns.Add("customerNation", typeof(string));
            tempTableCustomerDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvSupplierDim = new Dictionary<long, long>();
            DataTable tempTableSupplierDim = new DataTable();
            tempTableSupplierDim.Columns.Add("supplierCity", typeof(string));
            tempTableSupplierDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvDateDim = new Dictionary<long, long>();
            DataTable tempTableDateDim = new DataTable();
            tempTableDateDim.Columns.Add("year", typeof(string));
            tempTableDateDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvPartDim = new Dictionary<long, long>();

            Parallel.Invoke(parallelOptions,
                () =>
                {
                    long customerIndex = 0;
                    long dgKeyCustomer = 0;
                    foreach (Customer customer in InMemoryData.customerDimension)
                    {
                        if (customer.cRegion.Equals("AMERICA"))
                        {
                            string cNation = customer.cNation + ", " + customer.cRegion + ", " + customer.cCity;
                            if (tempTableCustomerDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableCustomerDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableCustomerDim.Rows)
                                {
                                    string customerNation = row.Field<string>("customerNation");
                                    if (customerNation.Equals(cNation))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
                    long supplierIndex = 0;
                    long dgKeySupplier = 0;
                    foreach (Supplier supplier in InMemoryData.supplierDimension)
                    {
                        if (supplier.sNation.Equals("UNITED STATES"))
                        {
                            string sCity = supplier.sNation + ", " + supplier.sRegion + ", " + supplier.sCity;
                            if (tempTableSupplierDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableSupplierDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableSupplierDim.Rows)
                                {
                                    string supplierCity = row.Field<string>("supplierCity");
                                    if (supplierCity.Equals(sCity))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
                    long dgKeyDate = 0;
                    foreach (Date date in InMemoryData.dateDimension)
                    {
                        if (date.dYear.Equals("1997") || date.dYear.Equals("1998"))
                        {
                            string dYear = date.dYear + ", " + date.dMonth;
                            if (tempTableDateDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableDateDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableDateDim.Rows)
                                {
                                    string year = row.Field<string>("year");
                                    if (year.Equals(dYear))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
                    long partIndex = 0;
                    long dgKeyPart = 0;
                    foreach (Part part in InMemoryData.partDimension)
                    {
                        if (part.pCategory.Equals("MFGR#14"))
                        {
                            kvPartDim.Add(partIndex + 1, dgKeyPart);

                        }
                        partIndex++;
                    }
                });

            sw.Stop();
            long t1 = sw.ElapsedMilliseconds;
            Console.WriteLine("[PIMA] GSTest Phase1 Time: " + t1);
            #endregion Step 1 & 2

            #region Step 3, 4 & 5

            sw.Reset();
            sw.Start();
            long dgkLengthSupplier = tempTableSupplierDim.Rows.Count + 1;
            long dgkLengthDate = tempTableDateDim.Rows.Count + 1;
            long dgkLengthCustomer = tempTableCustomerDim.Rows.Count + 1;

            long[,,] inMemoryAccumulator = new long[dgkLengthDate, dgkLengthSupplier, dgkLengthCustomer];

            List<Tuple<int, int>> partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loCustomerKey.Count, parallelOptions.MaxDegreeOfParallelism);

            List<Task> tasks = new List<Task>();
            foreach (Tuple<int, int> indexes in partitionIndexes)
            {
                Task t = Task.Factory.StartNew(() =>
                {
                    for (int i = indexes.Item1; i <= indexes.Item2; i++)
                    {
                        long suppKey = InMemoryData.loSupplierKey[i];
                        long dateKey = InMemoryData.loOrderDate[i];
                        long partKey = InMemoryData.loPartKey[i];
                        long custKey = InMemoryData.loCustomerKey[i];
                        if (kvSupplierDim.TryGetValue(suppKey, out long dgkSupplierDim)
                            && kvDateDim.TryGetValue(dateKey, out long dgkDateDim)
                            && kvPartDim.TryGetValue(partKey, out long dgkPartDim)
                            && kvCustomerDim.TryGetValue(custKey, out long dgkCustomerDim))
                        {
                            if (dgkCustomerDim == 0 || dgkSupplierDim == 0 || dgkDateDim == 0)
                            {
                                // skip
                            }
                            else
                            {
                                inMemoryAccumulator[dgkDateDim, dgkSupplierDim, dgkCustomerDim] += (InMemoryData.loRevenue[i] - InMemoryData.loSupplyCost[i]);
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
                    foreach (DataRow cdRow in tempTableCustomerDim.Rows)
                    {
                        long sumProfit = inMemoryAccumulator[ddRow.Field<long>("denseGroupingKey"), sdRow.Field<long>("denseGroupingKey")
                                            , cdRow.Field<long>("denseGroupingKey")];
                        if (sumProfit != 0)
                        {
                            finalTable.Add(ddRow.Field<string>("year") + ", " + sdRow.Field<string>("supplierCity") + ", " + sumProfit);
                        }
                    }
                }
            }

            sw.Stop();
            long t2 = sw.ElapsedMilliseconds;
            Console.WriteLine("[PIMA] GSTest Phase2 Time: " + t2);
            Console.WriteLine(string.Format("[PIMA] GSTest Total Time: {0}", t1 + t2));
            // Console.WriteLine(string.Format("[PIMA] Total Count: {0}", finalTable.Count));
            Console.WriteLine();
            #endregion Step 3, 4 & 5
            testResults.phase1Time = t1;
            testResults.phase2Time = t2;
            testResults.phase3Time = 0;
        }

        public void GAS9() {
            Stopwatch sw = new Stopwatch();

            #region Step 1 & 2
            sw.Start();
            Dictionary<long, long> kvCustomerDim = new Dictionary<long, long>();
            DataTable tempTableCustomerDim = new DataTable();
            tempTableCustomerDim.Columns.Add("customerNation", typeof(string));
            tempTableCustomerDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvSupplierDim = new Dictionary<long, long>();
            DataTable tempTableSupplierDim = new DataTable();
            tempTableSupplierDim.Columns.Add("supplierCity", typeof(string));
            tempTableSupplierDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvDateDim = new Dictionary<long, long>();
            DataTable tempTableDateDim = new DataTable();
            tempTableDateDim.Columns.Add("year", typeof(string));
            tempTableDateDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvPartDim = new Dictionary<long, long>();
            DataTable tempTablePartDim = new DataTable();
            tempTablePartDim.Columns.Add("partBrand", typeof(string));
            tempTablePartDim.Columns.Add("denseGroupingKey", typeof(long));

            Parallel.Invoke(parallelOptions,
                () =>
                {
                    long customerIndex = 0;
                    long dgKeyCustomer = 0;
                    foreach (Customer customer in InMemoryData.customerDimension)
                    {
                        if (customer.cRegion.Equals("AMERICA"))
                        {
                            string cNation = customer.cNation + ", " + customer.cRegion + ", " + customer.cCity;
                            if (tempTableCustomerDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableCustomerDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableCustomerDim.Rows)
                                {
                                    string customerNation = row.Field<string>("customerNation");
                                    if (customerNation.Equals(cNation))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
                    long supplierIndex = 0;
                    long dgKeySupplier = 0;
                    foreach (Supplier supplier in InMemoryData.supplierDimension)
                    {
                        if (supplier.sNation.Equals("UNITED STATES"))
                        {
                            string sCity = supplier.sNation + ", " + supplier.sRegion + ", " + supplier.sCity;
                            if (tempTableSupplierDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableSupplierDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableSupplierDim.Rows)
                                {
                                    string supplierCity = row.Field<string>("supplierCity");
                                    if (supplierCity.Equals(sCity))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
                    long dgKeyDate = 0;
                    foreach (Date date in InMemoryData.dateDimension)
                    {
                        if (date.dYear.Equals("1997") || date.dYear.Equals("1998"))
                        {
                            string dYear = date.dYear + ", " + date.dMonth;
                            if (tempTableDateDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableDateDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableDateDim.Rows)
                                {
                                    string year = row.Field<string>("year");
                                    if (year.Equals(dYear))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
                    long partIndex = 0;
                    long dgKeyPart = 0;
                    foreach (Part part in InMemoryData.partDimension)
                    {
                        if (part.pCategory.Equals("MFGR#14"))
                        {
                            string pBrand = part.pBrand;
                            if (tempTablePartDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTablePartDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTablePartDim.Rows)
                                {
                                    string partBrand = row.Field<string>("partBrand");
                                    if (partBrand.Equals(pBrand))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
            Console.WriteLine("[PIMA] GSTest Phase1 Time: " + t1);
            #endregion Step 1 & 2

            #region Step 3, 4 & 5

            sw.Reset();
            sw.Start();
            long dgkLengthSupplier = tempTableSupplierDim.Rows.Count + 1;
            long dgkLengthDate = tempTableDateDim.Rows.Count + 1;
            long dgkLengthPart = tempTablePartDim.Rows.Count + 1;
            long dgkLengthCustomer = tempTableCustomerDim.Rows.Count + 1;

            long[,,,] inMemoryAccumulator = new long[dgkLengthDate, dgkLengthSupplier, dgkLengthCustomer, dgkLengthPart];

            List<Tuple<int, int>> partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loCustomerKey.Count, parallelOptions.MaxDegreeOfParallelism);

            List<Task> tasks = new List<Task>();
            foreach (Tuple<int, int> indexes in partitionIndexes)
            {
                Task t = Task.Factory.StartNew(() =>
                {
                    for (int i = indexes.Item1; i <= indexes.Item2; i++)
                    {
                        long suppKey = InMemoryData.loSupplierKey[i];
                        long dateKey = InMemoryData.loOrderDate[i];
                        long partKey = InMemoryData.loPartKey[i];
                        long custKey = InMemoryData.loCustomerKey[i];
                        if (kvSupplierDim.TryGetValue(suppKey, out long dgkSupplierDim)
                            && kvDateDim.TryGetValue(dateKey, out long dgkDateDim)
                            && kvPartDim.TryGetValue(partKey, out long dgkPartDim)
                            && kvCustomerDim.TryGetValue(custKey, out long dgkCustomerDim))
                        {
                            if (dgkCustomerDim == 0 || dgkSupplierDim == 0 || dgkDateDim == 0 || dgkPartDim == 0)
                            {
                                // skip
                            }
                            else
                            {
                                inMemoryAccumulator[dgkDateDim, dgkSupplierDim, dgkCustomerDim, dgkPartDim] += (InMemoryData.loRevenue[i] - InMemoryData.loSupplyCost[i]);
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
                        foreach (DataRow cdRow in tempTableCustomerDim.Rows)
                        {
                            long sumProfit = inMemoryAccumulator[ddRow.Field<long>("denseGroupingKey"), sdRow.Field<long>("denseGroupingKey")
                                                , cdRow.Field<long>("denseGroupingKey"), pdRow.Field<long>("denseGroupingKey")];
                            if (sumProfit != 0)
                            {
                                finalTable.Add(ddRow.Field<string>("year") + ", " + sdRow.Field<string>("supplierCity") + ", " + pdRow.Field<string>("partBrand") + ", " + sumProfit);
                            }
                        }
                    }

                }
            }

            sw.Stop();
            long t2 = sw.ElapsedMilliseconds;
            Console.WriteLine("[PIMA] GSTest Phase2 Time: " + t2);
            Console.WriteLine(string.Format("[PIMA] GSTest Total Time: {0}", t1 + t2));
            // Console.WriteLine(string.Format("[PIMA] Total Count: {0}", finalTable.Count));
            Console.WriteLine();
            #endregion Step 3, 4 & 5
            testResults.phase1Time = t1;
            testResults.phase2Time = t2;
            testResults.phase3Time = 0;
        }

        public void GAS10()
        {
            Stopwatch sw = new Stopwatch();

            #region Step 1 & 2
            sw.Start();
            Dictionary<long, long> kvCustomerDim = new Dictionary<long, long>();
            DataTable tempTableCustomerDim = new DataTable();
            tempTableCustomerDim.Columns.Add("customerNation", typeof(string));
            tempTableCustomerDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvSupplierDim = new Dictionary<long, long>();
            DataTable tempTableSupplierDim = new DataTable();
            tempTableSupplierDim.Columns.Add("supplierCity", typeof(string));
            tempTableSupplierDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvDateDim = new Dictionary<long, long>();
            DataTable tempTableDateDim = new DataTable();
            tempTableDateDim.Columns.Add("year", typeof(string));
            tempTableDateDim.Columns.Add("denseGroupingKey", typeof(long));

            Dictionary<long, long> kvPartDim = new Dictionary<long, long>();
            DataTable tempTablePartDim = new DataTable();
            tempTablePartDim.Columns.Add("partBrand", typeof(string));
            tempTablePartDim.Columns.Add("denseGroupingKey", typeof(long));

            Parallel.Invoke(parallelOptions,
                () =>
                {
                    long customerIndex = 0;
                    long dgKeyCustomer = 0;
                    foreach (Customer customer in InMemoryData.customerDimension)
                    {
                        if (customer.cRegion.Equals("AMERICA"))
                        {
                            string cNation = customer.cNation + ", " + customer.cRegion + ", " + customer.cCity;
                            if (tempTableCustomerDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableCustomerDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableCustomerDim.Rows)
                                {
                                    string customerNation = row.Field<string>("customerNation");
                                    if (customerNation.Equals(cNation))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
                    long supplierIndex = 0;
                    long dgKeySupplier = 0;
                    foreach (Supplier supplier in InMemoryData.supplierDimension)
                    {
                        if (supplier.sNation.Equals("UNITED STATES"))
                        {
                            string sCity = supplier.sNation + ", " + supplier.sRegion + ", " + supplier.sCity;
                            if (tempTableSupplierDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableSupplierDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableSupplierDim.Rows)
                                {
                                    string supplierCity = row.Field<string>("supplierCity");
                                    if (supplierCity.Equals(sCity))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
                    long dgKeyDate = 0;
                    foreach (Date date in InMemoryData.dateDimension)
                    {
                        if (date.dYear.Equals("1997") || date.dYear.Equals("1998"))
                        {
                            string dYear = date.dYear + ", " + date.dMonth;
                            if (tempTableDateDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTableDateDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTableDateDim.Rows)
                                {
                                    string year = row.Field<string>("year");
                                    if (year.Equals(dYear))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
                    long partIndex = 0;
                    long dgKeyPart = 0;
                    foreach (Part part in InMemoryData.partDimension)
                    {
                        if (part.pCategory.Equals("MFGR#14"))
                        {
                            string pBrand = part.pBrand + ", " + part.pMFGR;
                            if (tempTablePartDim.Rows.Count > 0)
                            {
                                DataTable tempTable = tempTablePartDim.Copy();
                                bool found = false;
                                foreach (DataRow row in tempTablePartDim.Rows)
                                {
                                    string partBrand = row.Field<string>("partBrand");
                                    if (partBrand.Equals(pBrand))
                                    {
                                        long dgKey = row.Field<long>("denseGroupingKey");
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
            Console.WriteLine("[PIMA] GSTest Phase1 Time: " + t1);
            #endregion Step 1 & 2

            #region Step 3, 4 & 5

            sw.Reset();
            sw.Start();
            long dgkLengthSupplier = tempTableSupplierDim.Rows.Count + 1;
            long dgkLengthDate = tempTableDateDim.Rows.Count + 1;
            long dgkLengthPart = tempTablePartDim.Rows.Count + 1;
            long dgkLengthCustomer = tempTableCustomerDim.Rows.Count + 1;

            long[,,,] inMemoryAccumulator = new long[dgkLengthDate, dgkLengthSupplier, dgkLengthCustomer, dgkLengthPart];

            List<Tuple<int, int>> partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loCustomerKey.Count, parallelOptions.MaxDegreeOfParallelism);

            List<Task> tasks = new List<Task>();
            foreach (Tuple<int, int> indexes in partitionIndexes)
            {
                Task t = Task.Factory.StartNew(() =>
                {
                    for (int i = indexes.Item1; i <= indexes.Item2; i++)
                    {
                        long suppKey = InMemoryData.loSupplierKey[i];
                        long dateKey = InMemoryData.loOrderDate[i];
                        long partKey = InMemoryData.loPartKey[i];
                        long custKey = InMemoryData.loCustomerKey[i];
                        if (kvSupplierDim.TryGetValue(suppKey, out long dgkSupplierDim)
                            && kvDateDim.TryGetValue(dateKey, out long dgkDateDim)
                            && kvPartDim.TryGetValue(partKey, out long dgkPartDim)
                            && kvCustomerDim.TryGetValue(custKey, out long dgkCustomerDim))
                        {
                            if (dgkCustomerDim == 0 || dgkSupplierDim == 0 || dgkDateDim == 0 || dgkPartDim == 0)
                            {
                                // skip
                            }
                            else
                            {
                                inMemoryAccumulator[dgkDateDim, dgkSupplierDim, dgkCustomerDim, dgkPartDim] += (InMemoryData.loRevenue[i] - InMemoryData.loSupplyCost[i]);
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
                        foreach (DataRow cdRow in tempTableCustomerDim.Rows)
                        {
                            long sumProfit = inMemoryAccumulator[ddRow.Field<long>("denseGroupingKey"), sdRow.Field<long>("denseGroupingKey")
                                                , cdRow.Field<long>("denseGroupingKey"), pdRow.Field<long>("denseGroupingKey")];
                            if (sumProfit != 0)
                            {
                                finalTable.Add(ddRow.Field<string>("year") + ", " + sdRow.Field<string>("supplierCity") + ", " + pdRow.Field<string>("partBrand") + ", " + sumProfit);
                            }
                        }
                    }

                }
            }

            sw.Stop();
            long t2 = sw.ElapsedMilliseconds;
            
            Console.WriteLine("[PIMA] GSTest Phase2 Time: " + t2);
            Console.WriteLine(string.Format("[PIMA] GSTest Total Time: {0}", t1 + t2));
            
            // Console.WriteLine(string.Format("[PIMA] Total Count: {0}", finalTable.Count));
            Console.WriteLine();
            #endregion Step 3, 4 & 5
            testResults.phase1Time = t1;
            testResults.phase2Time = t2;
            testResults.phase3Time = 0;
        }

        public void GroupingAttributeScalabilityTest_Old(long numberOfGroupingAttributes)
        {
            try
            {
                Stopwatch sw = new Stopwatch();

                #region Key Hashing Phase 
                sw.Start();
                Dictionary<long, long> kvCustomerRegionDim = new Dictionary<long, long>();
                DataTable tempTableCustomerRegionDim = new DataTable();
                Dictionary<long, long> kvSupplierRegionDim = new Dictionary<long, long>();
                DataTable tempTableSupplierRegionDim = new DataTable();
                Dictionary<long, long> kvDateYearDim = new Dictionary<long, long>();
                DataTable tempTableDateYearDim = new DataTable();

                Dictionary<long, long> kvCustomerNationDim = new Dictionary<long, long>();
                DataTable tempTableCustomerNationDim = new DataTable();
                Dictionary<long, long> kvSupplierNationDim = new Dictionary<long, long>();
                DataTable tempTableSupplierNationDim = new DataTable();
                Dictionary<long, long> kvDateMonthDim = new Dictionary<long, long>();
                DataTable tempTableDateMonthDim = new DataTable();

                long customerRegionIndex = 0;
                long dgKeyCustomerRegion = 0;

                long supplierRegionIndex = 0;
                long dgKeySupplierRegion = 0;

                long dateYearIndex = 0;
                long dgKeyDateYear = 0;

                long customerNationIndex = 0;
                long dgKeyCustomerNation = 0;

                long supplierNationIndex = 0;
                long dgKeySupplierNation = 0;

                long dateMonthIndex = 0;
                long dgKeyDateMonth = 0;

                tempTableCustomerRegionDim.Columns.Add("customerRegion", typeof(string));
                tempTableCustomerRegionDim.Columns.Add("denseGroupingKey", typeof(long));

                tempTableCustomerNationDim.Columns.Add("customerNation", typeof(string));
                tempTableCustomerNationDim.Columns.Add("denseGroupingKey", typeof(long));

                tempTableSupplierRegionDim.Columns.Add("supplierRegion", typeof(string));
                tempTableSupplierRegionDim.Columns.Add("denseGroupingKey", typeof(long));

                tempTableSupplierNationDim.Columns.Add("supplierNation", typeof(string));
                tempTableSupplierNationDim.Columns.Add("denseGroupingKey", typeof(long));

                tempTableDateYearDim.Columns.Add("dateYear", typeof(string));
                tempTableDateYearDim.Columns.Add("denseGroupingKey", typeof(long));

                tempTableDateMonthDim.Columns.Add("dateMonth", typeof(string));
                tempTableDateMonthDim.Columns.Add("denseGroupingKey", typeof(long));



                switch (numberOfGroupingAttributes)
                {
                    case 1:
                        foreach (Customer customer in InMemoryData.customerDimension)
                        {
                            if (customer.cRegion.Equals("ASIA"))
                            {
                                string cNation = customer.cNation;
                                if (tempTableCustomerNationDim.Rows.Count > 0)
                                {
                                    DataTable tempTable = tempTableCustomerNationDim.Copy();
                                    bool found = false;
                                    foreach (DataRow row in tempTableCustomerNationDim.Rows)
                                    {
                                        string customerNation = row.Field<string>("customerNation");
                                        if (customerNation.Equals(cNation))
                                        {
                                            long dgKey = row.Field<long>("denseGroupingKey");
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
                        foreach (Customer customer in InMemoryData.customerDimension)
                        {
                            if (customer.cRegion.Equals("ASIA"))
                            {
                                string cRegion = customer.cRegion;
                                if (tempTableCustomerRegionDim.Rows.Count > 0)
                                {
                                    DataTable tempTable = tempTableCustomerRegionDim.Copy();
                                    bool found = false;
                                    foreach (DataRow row in tempTableCustomerRegionDim.Rows)
                                    {
                                        string customerRegion = row.Field<string>("customerRegion");
                                        if (customerRegion.Equals(cRegion))
                                        {
                                            long dgKey = row.Field<long>("denseGroupingKey");
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
                                    DataTable tempTable = tempTableCustomerNationDim.Copy();
                                    bool found = false;
                                    foreach (DataRow row in tempTableCustomerNationDim.Rows)
                                    {
                                        string customerNation = row.Field<string>("customerNation");
                                        if (customerNation.Equals(cNation))
                                        {
                                            long dgKey = row.Field<long>("denseGroupingKey");
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
                        foreach (Customer customer in InMemoryData.customerDimension)
                        {
                            if (customer.cRegion.Equals("ASIA"))
                            {
                                string cRegion = customer.cRegion;
                                if (tempTableCustomerRegionDim.Rows.Count > 0)
                                {
                                    DataTable tempTable = tempTableCustomerRegionDim.Copy();
                                    bool found = false;
                                    foreach (DataRow row in tempTableCustomerRegionDim.Rows)
                                    {
                                        string customerRegion = row.Field<string>("customerRegion");
                                        if (customerRegion.Equals(cRegion))
                                        {
                                            long dgKey = row.Field<long>("denseGroupingKey");
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
                                    DataTable tempTable = tempTableCustomerNationDim.Copy();
                                    bool found = false;
                                    foreach (DataRow row in tempTableCustomerNationDim.Rows)
                                    {
                                        string customerNation = row.Field<string>("customerNation");
                                        if (customerNation.Equals(cNation))
                                        {
                                            long dgKey = row.Field<long>("denseGroupingKey");
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

                        foreach (Supplier supplier in InMemoryData.supplierDimension)
                        {
                            if (supplier.sRegion.Equals("ASIA"))
                            {
                                string sNation = supplier.sNation;
                                if (tempTableSupplierNationDim.Rows.Count > 0)
                                {
                                    DataTable tempTable = tempTableSupplierNationDim.Copy();
                                    bool found = false;
                                    foreach (DataRow row in tempTableSupplierNationDim.Rows)
                                    {
                                        string supplierNation = row.Field<string>("supplierNation");
                                        if (supplierNation.Equals(sNation))
                                        {
                                            long dgKey = row.Field<long>("denseGroupingKey");
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
                        foreach (Customer customer in InMemoryData.customerDimension)
                        {
                            if (customer.cRegion.Equals("ASIA"))
                            {
                                string cRegion = customer.cRegion;
                                if (tempTableCustomerRegionDim.Rows.Count > 0)
                                {
                                    DataTable tempTable = tempTableCustomerRegionDim.Copy();
                                    bool found = false;
                                    foreach (DataRow row in tempTableCustomerRegionDim.Rows)
                                    {
                                        string customerRegion = row.Field<string>("customerRegion");
                                        if (customerRegion.Equals(cRegion))
                                        {
                                            long dgKey = row.Field<long>("denseGroupingKey");
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
                                    DataTable tempTable = tempTableCustomerNationDim.Copy();
                                    bool found = false;
                                    foreach (DataRow row in tempTableCustomerNationDim.Rows)
                                    {
                                        string customerNation = row.Field<string>("customerNation");
                                        if (customerNation.Equals(cNation))
                                        {
                                            long dgKey = row.Field<long>("denseGroupingKey");
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

                        foreach (Supplier supplier in InMemoryData.supplierDimension)
                        {
                            if (supplier.sRegion.Equals("ASIA"))
                            {
                                string sNation = supplier.sNation;
                                if (tempTableSupplierNationDim.Rows.Count > 0)
                                {
                                    DataTable tempTable = tempTableSupplierNationDim.Copy();
                                    bool found = false;
                                    foreach (DataRow row in tempTableSupplierNationDim.Rows)
                                    {
                                        string supplierNation = row.Field<string>("supplierNation");
                                        if (supplierNation.Equals(sNation))
                                        {
                                            long dgKey = row.Field<long>("denseGroupingKey");
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
                                    DataTable tempTable = tempTableSupplierRegionDim.Copy();
                                    bool found = false;
                                    foreach (DataRow row in tempTableSupplierRegionDim.Rows)
                                    {
                                        string supplierRegion = row.Field<string>("supplierRegion");
                                        if (supplierRegion.Equals(sRegion))
                                        {
                                            long dgKey = row.Field<long>("denseGroupingKey");
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
                        foreach (Customer customer in InMemoryData.customerDimension)
                        {
                            if (customer.cRegion.Equals("ASIA"))
                            {
                                string cRegion = customer.cRegion;
                                if (tempTableCustomerRegionDim.Rows.Count > 0)
                                {
                                    DataTable tempTable = tempTableCustomerRegionDim.Copy();
                                    bool found = false;
                                    foreach (DataRow row in tempTableCustomerRegionDim.Rows)
                                    {
                                        string customerRegion = row.Field<string>("customerRegion");
                                        if (customerRegion.Equals(cRegion))
                                        {
                                            long dgKey = row.Field<long>("denseGroupingKey");
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
                                    DataTable tempTable = tempTableCustomerNationDim.Copy();
                                    bool found = false;
                                    foreach (DataRow row in tempTableCustomerNationDim.Rows)
                                    {
                                        string customerNation = row.Field<string>("customerNation");
                                        if (customerNation.Equals(cNation))
                                        {
                                            long dgKey = row.Field<long>("denseGroupingKey");
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

                        foreach (Supplier supplier in InMemoryData.supplierDimension)
                        {
                            if (supplier.sRegion.Equals("ASIA"))
                            {
                                string sNation = supplier.sNation;
                                if (tempTableSupplierNationDim.Rows.Count > 0)
                                {
                                    DataTable tempTable = tempTableSupplierNationDim.Copy();
                                    bool found = false;
                                    foreach (DataRow row in tempTableSupplierNationDim.Rows)
                                    {
                                        string supplierNation = row.Field<string>("supplierNation");
                                        if (supplierNation.Equals(sNation))
                                        {
                                            long dgKey = row.Field<long>("denseGroupingKey");
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
                                    DataTable tempTable = tempTableSupplierRegionDim.Copy();
                                    bool found = false;
                                    foreach (DataRow row in tempTableSupplierRegionDim.Rows)
                                    {
                                        string supplierRegion = row.Field<string>("supplierRegion");
                                        if (supplierRegion.Equals(sRegion))
                                        {
                                            long dgKey = row.Field<long>("denseGroupingKey");
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

                        foreach (Date date in InMemoryData.dateDimension)
                        {
                            if (date.dYear.CompareTo("1992") >= 0 && date.dYear.CompareTo("1997") <= 0)
                            {
                                string dYear = date.dYear;
                                if (tempTableDateYearDim.Rows.Count > 0)
                                {
                                    DataTable tempTable = tempTableDateYearDim.Copy();
                                    bool found = false;
                                    foreach (DataRow row in tempTableDateYearDim.Rows)
                                    {
                                        string dateYear = row.Field<string>("dateYear");
                                        if (dateYear.Equals(dYear))
                                        {
                                            long dgKey = row.Field<long>("denseGroupingKey");
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
                        foreach (Customer customer in InMemoryData.customerDimension)
                        {
                            if (customer.cRegion.Equals("ASIA"))
                            {
                                string cRegion = customer.cRegion;
                                if (tempTableCustomerRegionDim.Rows.Count > 0)
                                {
                                    DataTable tempTable = tempTableCustomerRegionDim.Copy();
                                    bool found = false;
                                    foreach (DataRow row in tempTableCustomerRegionDim.Rows)
                                    {
                                        string customerRegion = row.Field<string>("customerRegion");
                                        if (customerRegion.Equals(cRegion))
                                        {
                                            long dgKey = row.Field<long>("denseGroupingKey");
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
                                    DataTable tempTable = tempTableCustomerNationDim.Copy();
                                    bool found = false;
                                    foreach (DataRow row in tempTableCustomerNationDim.Rows)
                                    {
                                        string customerNation = row.Field<string>("customerNation");
                                        if (customerNation.Equals(cNation))
                                        {
                                            long dgKey = row.Field<long>("denseGroupingKey");
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

                        foreach (Supplier supplier in InMemoryData.supplierDimension)
                        {
                            if (supplier.sRegion.Equals("ASIA"))
                            {
                                string sNation = supplier.sNation;
                                if (tempTableSupplierNationDim.Rows.Count > 0)
                                {
                                    DataTable tempTable = tempTableSupplierNationDim.Copy();
                                    bool found = false;
                                    foreach (DataRow row in tempTableSupplierNationDim.Rows)
                                    {
                                        string supplierNation = row.Field<string>("supplierNation");
                                        if (supplierNation.Equals(sNation))
                                        {
                                            long dgKey = row.Field<long>("denseGroupingKey");
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
                                    DataTable tempTable = tempTableSupplierRegionDim.Copy();
                                    bool found = false;
                                    foreach (DataRow row in tempTableSupplierRegionDim.Rows)
                                    {
                                        string supplierRegion = row.Field<string>("supplierRegion");
                                        if (supplierRegion.Equals(sRegion))
                                        {
                                            long dgKey = row.Field<long>("denseGroupingKey");
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

                        foreach (Date date in InMemoryData.dateDimension)
                        {
                            if (date.dYear.CompareTo("1992") >= 0 && date.dYear.CompareTo("1997") <= 0)
                            {
                                string dYear = date.dYear;
                                if (tempTableDateYearDim.Rows.Count > 0)
                                {
                                    DataTable tempTable = tempTableDateYearDim.Copy();
                                    bool found = false;
                                    foreach (DataRow row in tempTableDateYearDim.Rows)
                                    {
                                        string dateYear = row.Field<string>("dateYear");
                                        if (dateYear.Equals(dYear))
                                        {
                                            long dgKey = row.Field<long>("denseGroupingKey");
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
                                    DataTable tempTable = tempTableDateMonthDim.Copy();
                                    bool found = false;
                                    foreach (DataRow row in tempTableDateMonthDim.Rows)
                                    {
                                        string dateMonth = row.Field<string>("dateMonth");
                                        if (dateMonth.Equals(dMonth))
                                        {
                                            long dgKey = row.Field<long>("denseGroupingKey");
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
                Console.WriteLine(string.Format("[IMA Join] GSTest T0 Time: {0}", t0));
                #endregion Key Hashing Phase

                #region Probing Phase
                sw.Reset();
                sw.Start();
                long dgkLengthCustomerNation = 1;
                long dgkLengthCustomerRegion = 1;
                long dgkLengthSupplierNation = 1;
                long dgkLengthSupplierRegion = 1;
                long dgkLengthDateYear = 1;
                long dgkLengthDateMonth = 1;

                long[,,,,,] inMemoryAccumulatorTax = new long[dgkLengthCustomerNation, dgkLengthCustomerRegion, dgkLengthSupplierNation, dgkLengthSupplierRegion, dgkLengthDateYear, dgkLengthDateMonth];
                List<string> finalTable = new List<string>();


                dgkLengthCustomerNation = tempTableCustomerNationDim.Rows.Count + 1;
                dgkLengthCustomerRegion = tempTableCustomerRegionDim.Rows.Count + 1;
                dgkLengthSupplierNation = tempTableSupplierNationDim.Rows.Count + 1;
                dgkLengthSupplierRegion = tempTableSupplierRegionDim.Rows.Count + 1;
                dgkLengthDateYear = tempTableDateYearDim.Rows.Count + 1;
                dgkLengthDateMonth = tempTableDateMonthDim.Rows.Count + 1;

                inMemoryAccumulatorTax = new long[dgkLengthCustomerNation, dgkLengthCustomerRegion, dgkLengthSupplierNation, dgkLengthSupplierRegion, dgkLengthDateYear, dgkLengthDateMonth];

                long dgkCustomerNationDim = 0;
                long dgkCustomerRegionDim = 0;
                long dgkSupplierNationDim = 0;
                long dgkSupplierRegionDim = 0;
                long dgkDateYearDim = 0;
                long dgkDateMonthDim = 0;
                switch (numberOfGroupingAttributes)
                {
                    case 1:
                        for (int i = 0; i < InMemoryData.loCustomerKey.Count; i++)
                        {
                            long custKey = InMemoryData.loCustomerKey[i];
                            if (kvCustomerNationDim.TryGetValue(custKey, out dgkCustomerNationDim))
                            {
                                if (dgkCustomerNationDim == 0)
                                {
                                    // skip
                                }
                                else
                                {
                                    inMemoryAccumulatorTax[dgkCustomerNationDim, dgkCustomerRegionDim, dgkSupplierNationDim, dgkSupplierRegionDim, dgkDateYearDim, dgkDateMonthDim] += InMemoryData.loRevenue[i];
                                }
                            }
                        }

                        foreach (DataRow cdRow in tempTableCustomerNationDim.Rows)
                        {
                            long cdKey = cdRow.Field<long>("denseGroupingKey");
                            long sumTax = inMemoryAccumulatorTax[cdKey, dgkCustomerRegionDim, dgkSupplierNationDim, dgkSupplierRegionDim, dgkDateYearDim, dgkDateMonthDim];
                            if (sumTax != 0)
                            {
                                finalTable.Add(cdRow.Field<string>("customerNation")
                                    + ", " + sumTax);
                            }
                        }
                        break;
                    case 2:
                        for (int i = 0; i < InMemoryData.loCustomerKey.Count(); i++)
                        {
                            long custKey = InMemoryData.loCustomerKey[i];
                            if (kvCustomerNationDim.TryGetValue(custKey, out dgkCustomerNationDim)
                                && kvCustomerRegionDim.TryGetValue(custKey, out dgkCustomerRegionDim))
                            {
                                if (dgkCustomerNationDim == 0 || dgkCustomerRegionDim == 0)
                                {
                                    // skip
                                }
                                else
                                {
                                    inMemoryAccumulatorTax[dgkCustomerNationDim, dgkCustomerRegionDim, dgkSupplierNationDim, dgkSupplierRegionDim, dgkDateYearDim, dgkDateMonthDim] += InMemoryData.loRevenue[i];
                                }
                            }
                        }

                        foreach (DataRow cdNRow in tempTableCustomerNationDim.Rows)
                        {
                            foreach (DataRow cdRRow in tempTableCustomerRegionDim.Rows)
                            {
                                long cdNKey = cdNRow.Field<long>("denseGroupingKey");
                                long cdRKey = cdRRow.Field<long>("denseGroupingKey");

                                long sumTax = inMemoryAccumulatorTax[cdNKey, cdRKey, dgkSupplierNationDim, dgkSupplierRegionDim, dgkDateYearDim, dgkDateMonthDim];
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
                        for (int i = 0; i < InMemoryData.loCustomerKey.Count(); i++)
                        {
                            long custKey = InMemoryData.loCustomerKey[i];
                            long suppKey = InMemoryData.loSupplierKey[i];
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
                                    inMemoryAccumulatorTax[dgkCustomerNationDim, dgkCustomerRegionDim, dgkSupplierNationDim, dgkSupplierRegionDim, dgkDateYearDim, dgkDateMonthDim] += InMemoryData.loRevenue[i];
                                }
                            }
                        }

                        foreach (DataRow cdNRow in tempTableCustomerNationDim.Rows)
                        {
                            foreach (DataRow cdRRow in tempTableCustomerRegionDim.Rows)
                            {
                                foreach (DataRow sdNRow in tempTableSupplierNationDim.Rows)
                                {
                                    long cdNKey = cdNRow.Field<long>("denseGroupingKey");
                                    long cdRKey = cdRRow.Field<long>("denseGroupingKey");
                                    long sdNKey = sdNRow.Field<long>("denseGroupingKey");

                                    long sumTax = inMemoryAccumulatorTax[cdNKey, cdRKey, sdNKey, dgkSupplierRegionDim, dgkDateYearDim, dgkDateMonthDim];
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
                        for (int i = 0; i < InMemoryData.loCustomerKey.Count(); i++)
                        {
                            long custKey = InMemoryData.loCustomerKey[i];
                            long suppKey = InMemoryData.loSupplierKey[i];
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
                                    inMemoryAccumulatorTax[dgkCustomerNationDim, dgkCustomerRegionDim, dgkSupplierNationDim, dgkSupplierRegionDim, dgkDateYearDim, dgkDateMonthDim] += InMemoryData.loRevenue[i];
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
                                        long cdNKey = cdNRow.Field<long>("denseGroupingKey");
                                        long cdRKey = cdRRow.Field<long>("denseGroupingKey");
                                        long sdNKey = sdNRow.Field<long>("denseGroupingKey");
                                        long sdRKey = sdRRow.Field<long>("denseGroupingKey");

                                        long sumTax = inMemoryAccumulatorTax[cdNKey, cdRKey, sdNKey, sdRKey, dgkDateYearDim, dgkDateMonthDim];
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
                        for (int i = 0; i < InMemoryData.loCustomerKey.Count(); i++)
                        {
                            long custKey = InMemoryData.loCustomerKey[i];
                            long suppKey = InMemoryData.loSupplierKey[i];
                            long dateKey = InMemoryData.loOrderDate[i];
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
                                    inMemoryAccumulatorTax[dgkCustomerNationDim, dgkCustomerRegionDim, dgkSupplierNationDim, dgkSupplierRegionDim, dgkDateYearDim, dgkDateMonthDim] += InMemoryData.loRevenue[i];
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
                                            long cdNKey = cdNRow.Field<long>("denseGroupingKey");
                                            long cdRKey = cdRRow.Field<long>("denseGroupingKey");
                                            long sdNKey = sdNRow.Field<long>("denseGroupingKey");
                                            long sdRKey = sdRRow.Field<long>("denseGroupingKey");
                                            long ddYKey = ddYRow.Field<long>("denseGroupingKey");
                                            long sumTax = inMemoryAccumulatorTax[cdNKey, cdRKey, sdNKey, sdRKey, ddYKey, dgkDateMonthDim];
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
                        for (int i = 0; i < InMemoryData.loCustomerKey.Count(); i++)
                        {
                            long custKey = InMemoryData.loCustomerKey[i];
                            long suppKey = InMemoryData.loSupplierKey[i];
                            long dateKey = InMemoryData.loOrderDate[i];
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
                                    inMemoryAccumulatorTax[dgkCustomerNationDim, dgkCustomerRegionDim, dgkSupplierNationDim, dgkSupplierRegionDim, dgkDateYearDim, dgkDateMonthDim] += InMemoryData.loRevenue[i];
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
                                                long cdNKey = cdNRow.Field<long>("denseGroupingKey");
                                                long cdRKey = cdRRow.Field<long>("denseGroupingKey");
                                                long sdNKey = sdNRow.Field<long>("denseGroupingKey");
                                                long sdRKey = sdRRow.Field<long>("denseGroupingKey");
                                                long ddYKey = ddYRow.Field<long>("denseGroupingKey");
                                                long ddMKey = ddMRow.Field<long>("denseGroupingKey");
                                                long sumTax = inMemoryAccumulatorTax[cdNKey, cdRKey, sdNKey, sdRKey, ddYKey, ddMKey];
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
                Console.WriteLine(string.Format("[IMA Join] GSTest T1 Time: {0}", t1));
                sw.Reset();
                #endregion Probing Phase

                Console.WriteLine(string.Format("[IMA Join] GSTest Total Time: {0}", t0 + t1));
                Console.WriteLine(string.Format("[IMA Join] GSTest Total : {0}", finalTable.Count));
                Console.WriteLine();
                testResults.phase1Time = t0;
                testResults.phase2Time = t1;
                testResults.phase3Time = 0;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void saveAndPrintResults()
        {
            //TestResultsDatabase.nimbleJoinOutput.Add(testResults.toString());
            //Console.WriteLine("IMA: " + testResults.toString());
            //Console.WriteLine();
            TestResultsDatabase.pInMemoryAggregationOutput.Add(testResults.toString());
        }
    }
}
