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
        private static string binaryFilesDirectory = @"C:\Raw_Data_Source_For_Test\SSBM - DBGEN\BF";
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
                    foreach (var part in InMemoryData.partDimension)
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
                    foreach (var supplier in InMemoryData.supplierDimension)
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
                    foreach (var date in InMemoryData.dateDimension)
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

            #region Step 3, 4 & 5

            sw.Reset();
            sw.Start();
            Int64 dgkLengthPart = tempTablePartDim.Rows.Count + 1;
            Int64 dgkLengthDate = tempTableDateDim.Rows.Count + 1;

            Int64[,] inMemoryAccumulator = new Int64[dgkLengthDate, dgkLengthPart];

            var partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loPartKey.Count(), parallelOptions.MaxDegreeOfParallelism);

            var tasks = new List<Task>();
            foreach (var indexes in partitionIndexes)
            {
                var t = Task.Factory.StartNew(() =>
                {
                    for (Int32 i = indexes.Item1; i <= indexes.Item2; i++)
                    {
                        Int64 partKey = InMemoryData.loPartKey[i];
                        Int64 suppKey = InMemoryData.loSupplierKey[i];
                        Int64 dateKey = InMemoryData.loOrderDate[i];
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
            testResults.phase1Time = t1;
            testResults.phase2Time = t2;
            testResults.phase3Time = 0; ;
        }

        public void Query_2_2_IM()
        {

            Stopwatch sw = new Stopwatch();

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
                    foreach (var part in InMemoryData.partDimension)
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
                    foreach (var supplier in InMemoryData.supplierDimension)
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
                    foreach (var date in InMemoryData.dateDimension)
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

            #region Step 3, 4 & 5

            sw.Reset();
            sw.Start();
            Int64 dgkLengthPart = tempTablePartDim.Rows.Count + 1;
            Int64 dgkLengthDate = tempTableDateDim.Rows.Count + 1;

            Int64[,] inMemoryAccumulator = new Int64[dgkLengthDate, dgkLengthPart];

            var partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loPartKey.Count(), parallelOptions.MaxDegreeOfParallelism);

            var tasks = new List<Task>();
            foreach (var indexes in partitionIndexes)
            {
                var t = Task.Factory.StartNew(() =>
                {
                    for (Int32 i = indexes.Item1; i <= indexes.Item2; i++)
                    {
                        Int64 partKey = InMemoryData.loPartKey[i];
                        Int64 dateKey = InMemoryData.loOrderDate[i];
                        Int64 suppKey = InMemoryData.loSupplierKey[i];
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
            testResults.phase1Time = t1;
            testResults.phase2Time = t2;
            testResults.phase3Time = 0; ;
        }

        public void Query_2_3_IM()
        {

            Stopwatch sw = new Stopwatch();

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
                    foreach (var part in InMemoryData.partDimension)
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
                    foreach (var supplier in InMemoryData.supplierDimension)
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
                    foreach (var date in InMemoryData.dateDimension)
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

            #region Step 3, 4 & 5

            sw.Reset();
            sw.Start();
            Int64 dgkLengthPart = tempTablePartDim.Rows.Count + 1;
            Int64 dgkLengthDate = tempTableDateDim.Rows.Count + 1;

            Int64[,] inMemoryAccumulator = new Int64[dgkLengthDate, dgkLengthPart];

            var partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loPartKey.Count(), parallelOptions.MaxDegreeOfParallelism);

            var tasks = new List<Task>();
            foreach (var indexes in partitionIndexes)
            {
                var t = Task.Factory.StartNew(() =>
                {
                    for (Int32 i = indexes.Item1; i <= indexes.Item2; i++)
                    {
                        Int64 partKey = InMemoryData.loPartKey[i];
                        Int64 dateKey = InMemoryData.loOrderDate[i];
                        Int64 suppKey = InMemoryData.loSupplierKey[i];
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
                    foreach (var customer in InMemoryData.customerDimension)
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
                    foreach (var supplier in InMemoryData.supplierDimension)
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
                    foreach (var date in InMemoryData.dateDimension)
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

            #region Step 3, 4 & 5

            sw.Reset();
            sw.Start();
            Int64 dgkLengthCustomer = tempTableCustomerDim.Rows.Count + 1;
            Int64 dgkLengthSupplier = tempTableSupplierDim.Rows.Count + 1;
            Int64 dgkLengthDate = tempTableDateDim.Rows.Count + 1;

            Int64[,,] inMemoryAccumulator = new Int64[dgkLengthCustomer, dgkLengthSupplier, dgkLengthDate];

            var partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);

            var tasks = new List<Task>();
            foreach (var indexes in partitionIndexes)
            {
                var t = Task.Factory.StartNew(() =>
                {
                    for (Int32 i = indexes.Item1; i <= indexes.Item2; i++)
                    {
                        Int64 custKey = InMemoryData.loCustomerKey[i];
                        Int64 suppKey = InMemoryData.loSupplierKey[i];
                        Int64 dateKey = InMemoryData.loOrderDate[i];
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
            testResults.phase1Time = t1;
            testResults.phase2Time = t2;
            testResults.phase3Time = 0; ;
        }

        public void Query_3_2_IM()
        {

            Stopwatch sw = new Stopwatch();

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
                    foreach (var customer in InMemoryData.customerDimension)
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
                    foreach (var supplier in InMemoryData.supplierDimension)
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
                    foreach (var date in InMemoryData.dateDimension)
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

            #region Step 3, 4 & 5

            sw.Reset();
            sw.Start();
            Int64 dgkLengthCustomer = tempTableCustomerDim.Rows.Count + 1;
            Int64 dgkLengthSupplier = tempTableSupplierDim.Rows.Count + 1;
            Int64 dgkLengthDate = tempTableDateDim.Rows.Count + 1;

            Int64[,,] inMemoryAccumulator = new Int64[dgkLengthCustomer, dgkLengthSupplier, dgkLengthDate];

            var partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);

            var tasks = new List<Task>();
            foreach (var indexes in partitionIndexes)
            {
                var t = Task.Factory.StartNew(() =>
                {
                    for (Int32 i = indexes.Item1; i <= indexes.Item2; i++)
                    {
                        Int64 custKey = InMemoryData.loCustomerKey[i];
                        Int64 suppKey = InMemoryData.loSupplierKey[i];
                        Int64 dateKey = InMemoryData.loOrderDate[i];
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
            testResults.phase1Time = t1;
            testResults.phase2Time = t2;
            testResults.phase3Time = 0; ;
        }

        public void Query_3_3_IM()
        {

            Stopwatch sw = new Stopwatch();

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
                    foreach (var customer in InMemoryData.customerDimension)
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
                    foreach (var supplier in InMemoryData.supplierDimension)
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
                    foreach (var date in InMemoryData.dateDimension)
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

            #region Step 3, 4 & 5

            sw.Reset();
            sw.Start();
            Int64 dgkLengthCustomer = tempTableCustomerDim.Rows.Count + 1;
            Int64 dgkLengthSupplier = tempTableSupplierDim.Rows.Count + 1;
            Int64 dgkLengthDate = tempTableDateDim.Rows.Count + 1;

            Int64[,,] inMemoryAccumulator = new Int64[dgkLengthCustomer, dgkLengthSupplier, dgkLengthDate];

            var partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loCustomerKey.Count, parallelOptions.MaxDegreeOfParallelism);

            var tasks = new List<Task>();
            foreach (var indexes in partitionIndexes)
            {
                var t = Task.Factory.StartNew(() =>
                {
                    for (Int32 i = indexes.Item1; i <= indexes.Item2; i++)
                    {
                        Int64 custKey = InMemoryData.loCustomerKey[i];
                        Int64 suppKey = InMemoryData.loSupplierKey[i];
                        Int64 dateKey = InMemoryData.loOrderDate[i];
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
            testResults.phase1Time = t1;
            testResults.phase2Time = t2;
            testResults.phase3Time = 0; ;
        }

        public void Query_3_4_IM()
        {

            Stopwatch sw = new Stopwatch();

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
                    foreach (var customer in InMemoryData.customerDimension)
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
                    foreach (var supplier in InMemoryData.supplierDimension)
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
                    foreach (var date in InMemoryData.dateDimension)
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

            #region Step 3, 4 & 5

            sw.Reset();
            sw.Start();
            Int64 dgkLengthCustomer = tempTableCustomerDim.Rows.Count + 1;
            Int64 dgkLengthSupplier = tempTableSupplierDim.Rows.Count + 1;
            Int64 dgkLengthDate = tempTableDateDim.Rows.Count + 1;

            Int64[,,] inMemoryAccumulator = new Int64[dgkLengthCustomer, dgkLengthSupplier, dgkLengthDate];

            var partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loCustomerKey.Count, parallelOptions.MaxDegreeOfParallelism);

            var tasks = new List<Task>();
            foreach (var indexes in partitionIndexes)
            {
                var t = Task.Factory.StartNew(() =>
                {
                    for (Int32 i = indexes.Item1; i <= indexes.Item2; i++)
                    {
                        Int64 custKey = InMemoryData.loCustomerKey[i];
                        Int64 suppKey = InMemoryData.loSupplierKey[i];
                        Int64 dateKey = InMemoryData.loOrderDate[i];
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
            testResults.phase1Time = t1;
            testResults.phase2Time = t2;
            testResults.phase3Time = 0; ;
        }

        public void Query_4_1_IM()
        {

            Stopwatch sw = new Stopwatch();
            
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
                    foreach (var customer in InMemoryData.customerDimension)
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
                    foreach (var supplier in InMemoryData.supplierDimension)
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
                    foreach (var date in InMemoryData.dateDimension)
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
                    foreach (var part in InMemoryData.partDimension)
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

            #region Step 3, 4 & 5

            sw.Reset();
            sw.Start();
            Int64 dgkLengthCustomer = tempTableCustomerDim.Rows.Count + 1;
            Int64 dgkLengthSupplier = tempTableSupplierDim.Rows.Count + 1;
            Int64 dgkLengthDate = tempTableDateDim.Rows.Count + 1;
            Int64 dgkLengthPart = tempTableDateDim.Rows.Count + 1;

            Int64[,] inMemoryAccumulator = new Int64[dgkLengthCustomer, dgkLengthDate];

            var partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);

            var tasks = new List<Task>();
            foreach (var indexes in partitionIndexes)
            {
                var t = Task.Factory.StartNew(() =>
                {
                    for (Int32 i = indexes.Item1; i <= indexes.Item2; i++)
                    {
                        Int64 custKey = InMemoryData.loCustomerKey[i];
                        Int64 dateKey = InMemoryData.loOrderDate[i];
                        Int64 suppKey = InMemoryData.loSupplierKey[i];
                        Int64 partKey = InMemoryData.loPartKey[i];
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
            testResults.phase1Time = t1;
            testResults.phase2Time = t2;
            testResults.phase3Time = 0; ;
        }

        public void Query_4_2_IM()
        {

            Stopwatch sw = new Stopwatch();

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
                    foreach (var customer in InMemoryData.customerDimension)
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
                    foreach (var supplier in InMemoryData.supplierDimension)
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
                    foreach (var date in InMemoryData.dateDimension)
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
                    foreach (var part in InMemoryData.partDimension)
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

            #region Step 3, 4 & 5

            sw.Reset();
            sw.Start();
            Int64 dgkLengthSupplier = tempTableSupplierDim.Rows.Count + 1;
            Int64 dgkLengthDate = tempTableDateDim.Rows.Count + 1;
            Int64 dgkLengthPart = tempTablePartDim.Rows.Count + 1;

            Int64[,,] inMemoryAccumulator = new Int64[dgkLengthDate, dgkLengthSupplier, dgkLengthPart];

            var partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);

            var tasks = new List<Task>();
            foreach (var indexes in partitionIndexes)
            {
                var t = Task.Factory.StartNew(() =>
                {
                    for (Int32 i = indexes.Item1; i <= indexes.Item2; i++)
                    {
                        Int64 suppKey = InMemoryData.loSupplierKey[i];
                        Int64 dateKey = InMemoryData.loOrderDate[i];
                        Int64 partKey = InMemoryData.loPartKey[i];
                        Int64 custKey = InMemoryData.loCustomerKey[i];
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
            testResults.phase1Time = t1;
            testResults.phase2Time = t2;
            testResults.phase3Time = 0; ;
        }

        public void Query_4_3_IM()
        {

            Stopwatch sw = new Stopwatch();

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
                    foreach (var customer in InMemoryData.customerDimension)
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
                    foreach (var supplier in InMemoryData.supplierDimension)
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
                    foreach (var date in InMemoryData.dateDimension)
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
                    foreach (var part in InMemoryData.partDimension)
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

            #region Step 3, 4 & 5

            sw.Reset();
            sw.Start();
            Int64 dgkLengthSupplier = tempTableSupplierDim.Rows.Count + 1;
            Int64 dgkLengthDate = tempTableDateDim.Rows.Count + 1;
            Int64 dgkLengthPart = tempTablePartDim.Rows.Count + 1;

            Int64[,,] inMemoryAccumulator = new Int64[dgkLengthDate, dgkLengthSupplier, dgkLengthPart];

            var partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loCustomerKey.Count, parallelOptions.MaxDegreeOfParallelism);

            var tasks = new List<Task>();
            foreach (var indexes in partitionIndexes)
            {
                var t = Task.Factory.StartNew(() =>
                {
                    for (Int32 i = indexes.Item1; i <= indexes.Item2; i++)
                    {
                        Int64 suppKey = InMemoryData.loSupplierKey[i];
                        Int64 dateKey = InMemoryData.loOrderDate[i];
                        Int64 partKey = InMemoryData.loPartKey[i];
                        Int64 custKey = InMemoryData.loCustomerKey[i];
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
            testResults.phase1Time = t1;
            testResults.phase2Time = t2;
            testResults.phase3Time = 0; ;
        }

        public void GroupingAttributeScalabilityTest(Int64 numberOfGroupingAttributes)
        {
            try
            {
                Stopwatch sw = new Stopwatch();
               
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
                        foreach (var customer in InMemoryData.customerDimension)
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
                        foreach (var customer in InMemoryData.customerDimension)
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
                        foreach (var customer in InMemoryData.customerDimension)
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

                        foreach (var supplier in InMemoryData.supplierDimension)
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
                        foreach (var customer in InMemoryData.customerDimension)
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

                        foreach (var supplier in InMemoryData.supplierDimension)
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
                        foreach (var customer in InMemoryData.customerDimension)
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

                        foreach (var supplier in InMemoryData.supplierDimension)
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

                        foreach (var date in InMemoryData.dateDimension)
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
                        foreach (var customer in InMemoryData.customerDimension)
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

                        foreach (var supplier in InMemoryData.supplierDimension)
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

                        foreach (var date in InMemoryData.dateDimension)
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
                        for (Int32 i = 0; i < InMemoryData.loCustomerKey.Count; i++)
                        {
                            Int64 custKey = InMemoryData.loCustomerKey[i];
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
                        for (Int32 i = 0; i < InMemoryData.loCustomerKey.Count(); i++)
                        {
                            Int64 custKey = InMemoryData.loCustomerKey[i];
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
                        for (Int32 i = 0; i < InMemoryData.loCustomerKey.Count(); i++)
                        {
                            Int64 custKey = InMemoryData.loCustomerKey[i];
                            Int64 suppKey = InMemoryData.loSupplierKey[i];
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
                        for (Int32 i = 0; i < InMemoryData.loCustomerKey.Count(); i++)
                        {
                            Int64 custKey = InMemoryData.loCustomerKey[i];
                            Int64 suppKey = InMemoryData.loSupplierKey[i];
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
                        for (Int32 i = 0; i < InMemoryData.loCustomerKey.Count(); i++)
                        {
                            Int64 custKey = InMemoryData.loCustomerKey[i];
                            Int64 suppKey = InMemoryData.loSupplierKey[i];
                            Int64 dateKey = InMemoryData.loOrderDate[i];
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
                        for (Int32 i = 0; i < InMemoryData.loCustomerKey.Count(); i++)
                        {
                            Int64 custKey = InMemoryData.loCustomerKey[i];
                            Int64 suppKey = InMemoryData.loSupplierKey[i];
                            Int64 dateKey = InMemoryData.loOrderDate[i];
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
