using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParallelHashJoins
{
    class ParallelNimbleJoin
    {
        private static string binaryFilesDirectory = @"C:\Raw_Data_Source_For_Test\SSBM - DBGEN\BF";
        private string scaleFactor { get; set; }
        private ParallelOptions parallelOptions = null;
        private TestResults testResults = new TestResults();

        public ParallelNimbleJoin(string _scaleFactor, ParallelOptions _parallelOptions)
        {
            scaleFactor = _scaleFactor;
            parallelOptions = _parallelOptions;
        }
        ~ParallelNimbleJoin()
        {
            saveAndPrintResults();
        }

        //public void Query_1_1()
        //{
        //    try
        //    {
        //        long memoryStartPhase1 = GC.GetTotalMemory(true);
        //        outputRecordsCounter = 0;
        //        var dateHashTable = new Dictionary<Int64, string>();
        //        Stopwatch sw = new Stopwatch();
        //        Stopwatch swInitialRecorder = new Stopwatch();
        //        Stopwatch swOutputRecorder = new Stopwatch();

        //        #region Key Hashing Phase
        //        List<Date> dateDimension = null;

        //        swInitialRecorder.Start();
        //        swOutputRecorder.Start();
        //        sw.Start();
        //        dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
        //        sw.Stop();
        //        testResults.phase11IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        foreach (var row in dateDimension)
        //        {
        //            if (row.dYear.Equals("1993"))
        //                dateHashTable.Add(row.dDateKey, row.dYear);
        //        }
        //        sw.Stop();
        //        testResults.phase11HashTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        testResults.phase13HashTime = sw.ElapsedMilliseconds;
        //        testResults.phase1Time = testResults.phase11HashTime + testResults.phase11IOTime +
        //            testResults.phase12HashTime + testResults.phase12IOTime +
        //            testResults.phase13HashTime + testResults.phase13IOTime;
        //        sw.Reset();

        //        dateDimension.Clear();

        //        long memoryUsedPhase1 = GC.GetTotalMemory(true) - memoryStartPhase1;
        //        #endregion Key Hashing Phase

        //        #region Probing Phase
        //        long memoryStartPhase2 = GC.GetTotalMemory(true);

        //        sw.Start();
        //        List<Int64> loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
        //        sw.Stop();
        //        testResults.phase22IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        var Int64ermediateHashTable = new Dictionary<Int64, Triplets>();
        //        var k = 0;
        //        foreach (var orderDate in loOrderDate)
        //        {
        //            string dYear = string.Empty;
        //            Triplets values = new Triplets();
        //            if (dateHashTable.TryGetValue(orderDate, out dYear))
        //            {
        //                Int64ermediateHashTable.Add(k, values);
        //            }
        //            k++;
        //        }

        //        sw.Stop();
        //        testResults.phase22ProbeTime = sw.ElapsedMilliseconds;

        //        testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime +
        //            testResults.phase22IOTime + testResults.phase22ProbeTime +
        //            testResults.phase23IOTime + testResults.phase23ProbeTime;
        //        sw.Reset();

        //        loOrderDate.Clear();

        //        long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
        //        #endregion Probing Phase

        //        #region Value Extraction Phase
        //        long memoryStartPhase3 = GC.GetTotalMemory(true);
        //        sw.Start();
        //        List<Int64> loExtendedPrice = null;
        //        List<Int64> loDiscount = null;
        //        List<Int64> loQuantity = null;

        //        Parallel.Invoke(parallelOptions, () => loExtendedPrice = Utils.ReadFromBinaryFiles<Int64>(loExtendedPriceFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loDiscount = Utils.ReadFromBinaryFiles<Int64>(loDiscountFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loQuantity = Utils.ReadFromBinaryFiles<Int64>(loQuantityFile.Replace("BF", "BF" + scaleFactor)));
        //        sw.Stop();
        //        testResults.phase3IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();

        //        var j = 0;
        //        Int64 totalRevenue = 0;
        //        object lockObject = new object();
        //        Parallel.ForEach(IntermediateHashTable, (row) =>
        //        {
        //            string cNation = string.Empty;
        //            Int32 key = row.Key;
        //            if (loQuantity[key] < 25)
        //            {
        //                Int64 discount = loDiscount[key];
        //                if (discount >= 1 && discount <= 3)
        //                {
        //                    lock (lockObject)
        //                    {
        //                        var revenue = (loExtendedPrice[key] * discount);
        //                        totalRevenue += revenue;
        //                    }

        //                    if (isFirst)
        //                    {
        //                        swInitialRecorder.Stop();
        //                        testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
        //                        isFirst = false;
        //                    }
        //                    //Console.WriteLine(k +", "+ values.ToString() + ", " + sNation);
        //                    outputRecordsCounter++;
        //                    if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
        //                    {
        //                        swOutputRecorder.Stop();
        //                        testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
        //                        swOutputRecorder.Start();
        //                    }
        //                }
        //                else
        //                {
        //                    // Int64ermediateHashTable.Remove(j);
        //                }
        //            }
        //            else
        //            {
        //                // Int64ermediateHashTable.Remove(j);
        //            }
        //            j++;
        //        });

        //        sw.Stop();

        //        long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
        //        #endregion Value Extraction Phase
        //        testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
        //        testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
        //        testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
        //        // Console.WriteLine("[Nimble Join]: Time taken {0} ms.", testResults.totalExecutionTime);
        //        testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
        //        testResults.totalNumberOfOutput = totalRevenue;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}
        //public void Query_1_2()
        //{
        //    try
        //    {
        //        long memoryStartPhase1 = GC.GetTotalMemory(true);
        //        outputRecordsCounter = 0;
        //        var dateHashTable = new Dictionary<Int64, string>();
        //        Stopwatch sw = new Stopwatch();
        //        Stopwatch swInitialRecorder = new Stopwatch();
        //        Stopwatch swOutputRecorder = new Stopwatch();

        //        #region Key Hashing Phase
        //        List<Date> dateDimension = null;

        //        swInitialRecorder.Start();
        //        swOutputRecorder.Start();
        //        sw.Start();
        //        dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
        //        sw.Stop();
        //        testResults.phase11IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        foreach (var row in dateDimension)
        //        {
        //            if (row.dYearMonthNum == 199401)
        //                dateHashTable.Add(row.dDateKey, row.dYear);
        //        }
        //        sw.Stop();
        //        testResults.phase11HashTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        testResults.phase13HashTime = sw.ElapsedMilliseconds;
        //        testResults.phase1Time = testResults.phase11HashTime + testResults.phase11IOTime +
        //            testResults.phase12HashTime + testResults.phase12IOTime +
        //            testResults.phase13HashTime + testResults.phase13IOTime;
        //        sw.Reset();

        //        dateDimension.Clear();

        //        long memoryUsedPhase1 = GC.GetTotalMemory(true) - memoryStartPhase1;
        //        #endregion Key Hashing Phase

        //        #region Probing Phase
        //        long memoryStartPhase2 = GC.GetTotalMemory(true);

        //        sw.Start();
        //        List<Int64> loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
        //        sw.Stop();
        //        testResults.phase22IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        var Int64ermediateHashTable = new Dictionary<Int64, Triplets>();
        //        var k = 0;
        //        foreach (var orderDate in loOrderDate)
        //        {
        //            string dYear = string.Empty;
        //            Triplets values = new Triplets();
        //            if (dateHashTable.TryGetValue(orderDate, out dYear))
        //            {
        //                Int64ermediateHashTable.Add(k, values);
        //            }
        //            k++;
        //        }

        //        sw.Stop();
        //        testResults.phase22ProbeTime = sw.ElapsedMilliseconds;

        //        testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime +
        //            testResults.phase22IOTime + testResults.phase22ProbeTime +
        //            testResults.phase23IOTime + testResults.phase23ProbeTime;
        //        sw.Reset();

        //        loOrderDate.Clear();

        //        long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
        //        #endregion Probing Phase

        //        #region Value Extraction Phase
        //        long memoryStartPhase3 = GC.GetTotalMemory(true);
        //        sw.Start();
        //        List<Int64> loExtendedPrice = null;
        //        List<Int64> loDiscount = null;
        //        List<Int64> loQuantity = null;

        //        Parallel.Invoke(parallelOptions, () => loExtendedPrice = Utils.ReadFromBinaryFiles<Int64>(loExtendedPriceFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loDiscount = Utils.ReadFromBinaryFiles<Int64>(loDiscountFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loQuantity = Utils.ReadFromBinaryFiles<Int64>(loQuantityFile.Replace("BF", "BF" + scaleFactor)));
        //        sw.Stop();
        //        testResults.phase3IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();

        //        var j = 0;
        //        Int64 totalRevenue = 0;
        //        object lockObject = new object();
        //        Parallel.ForEach(Int64ermediateHashTable, (row) =>
        //        {
        //            string cNation = string.Empty;
        //            Int64 key = row.Key;
        //            Int64 quantity = loQuantity[key];
        //            if (quantity >= 26 && quantity <= 35)
        //            {
        //                Int64 discount = loDiscount[key];
        //                if (discount >= 4 && discount <= 6)
        //                {
        //                    lock (lockObject)
        //                    {
        //                        var revenue = (loExtendedPrice[key] * discount);
        //                        totalRevenue += revenue;
        //                    }

        //                    if (isFirst)
        //                    {
        //                        swInitialRecorder.Stop();
        //                        testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
        //                        isFirst = false;
        //                    }
        //                    //Console.WriteLine(k +", "+ values.ToString() + ", " + sNation);
        //                    outputRecordsCounter++;
        //                    if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
        //                    {
        //                        swOutputRecorder.Stop();
        //                        testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
        //                        swOutputRecorder.Start();
        //                    }
        //                }
        //                else
        //                {
        //                    // Int64ermediateHashTable.Remove(j);
        //                }
        //            }
        //            else
        //            {
        //                // Int64ermediateHashTable.Remove(j);
        //            }
        //            j++;
        //        });

        //        sw.Stop();

        //        long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
        //        #endregion Value Extraction Phase
        //        testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
        //        testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
        //        testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
        //        // Console.WriteLine("[Nimble Join]: Time taken {0} ms.", testResults.totalExecutionTime);
        //        testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
        //        testResults.totalNumberOfOutput = totalRevenue;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}
        //public void Query_1_3()
        //{
        //    try
        //    {
        //        long memoryStartPhase1 = GC.GetTotalMemory(true);
        //        outputRecordsCounter = 0;
        //        var dateHashTable = new Dictionary<Int64, string>();
        //        Stopwatch sw = new Stopwatch();
        //        Stopwatch swInitialRecorder = new Stopwatch();
        //        Stopwatch swOutputRecorder = new Stopwatch();

        //        #region Key Hashing Phase
        //        List<Date> dateDimension = null;

        //        swInitialRecorder.Start();
        //        swOutputRecorder.Start();
        //        sw.Start();
        //        dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
        //        sw.Stop();
        //        testResults.phase11IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        foreach (var row in dateDimension)
        //        {
        //            if (row.dYear.Equals("1994") && row.dWeekNumInYear == 6)
        //                dateHashTable.Add(row.dDateKey, row.dYear);
        //        }
        //        sw.Stop();
        //        testResults.phase11HashTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        testResults.phase13HashTime = sw.ElapsedMilliseconds;
        //        testResults.phase1Time = testResults.phase11HashTime + testResults.phase11IOTime +
        //            testResults.phase12HashTime + testResults.phase12IOTime +
        //            testResults.phase13HashTime + testResults.phase13IOTime;
        //        sw.Reset();

        //        dateDimension.Clear();

        //        long memoryUsedPhase1 = GC.GetTotalMemory(true) - memoryStartPhase1;
        //        #endregion Key Hashing Phase

        //        #region Probing Phase
        //        long memoryStartPhase2 = GC.GetTotalMemory(true);

        //        sw.Start();
        //        List<Int64> loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
        //        sw.Stop();
        //        testResults.phase22IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        var Int64ermediateHashTable = new Dictionary<Int64, Triplets>();
        //        var k = 0;
        //        foreach (var orderDate in loOrderDate)
        //        {
        //            string dYear = string.Empty;
        //            Triplets values = new Triplets();
        //            if (dateHashTable.TryGetValue(orderDate, out dYear))
        //            {
        //                Int64ermediateHashTable.Add(k, values);
        //            }
        //            k++;
        //        }

        //        sw.Stop();
        //        testResults.phase22ProbeTime = sw.ElapsedMilliseconds;

        //        testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime +
        //            testResults.phase22IOTime + testResults.phase22ProbeTime +
        //            testResults.phase23IOTime + testResults.phase23ProbeTime;
        //        sw.Reset();

        //        loOrderDate.Clear();

        //        long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
        //        #endregion Probing Phase

        //        #region Value Extraction Phase
        //        long memoryStartPhase3 = GC.GetTotalMemory(true);
        //        sw.Start();
        //        List<Int64> loExtendedPrice = null;
        //        List<Int64> loDiscount = null;
        //        List<Int64> loQuantity = null;

        //        Parallel.Invoke(parallelOptions, () => loExtendedPrice = Utils.ReadFromBinaryFiles<Int64>(loExtendedPriceFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loDiscount = Utils.ReadFromBinaryFiles<Int64>(loDiscountFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loQuantity = Utils.ReadFromBinaryFiles<Int64>(loQuantityFile.Replace("BF", "BF" + scaleFactor)));
        //        sw.Stop();
        //        testResults.phase3IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();

        //        var j = 0;
        //        Int64 totalRevenue = 0;
        //        object lockObject = new object();
        //        Parallel.ForEach(Int64ermediateHashTable, (row) =>
        //        {
        //            string cNation = string.Empty;
        //            Int64 key = row.Key;
        //            Int64 quantity = loQuantity[key];
        //            if (quantity >= 26 && quantity <= 35)
        //            {
        //                Int64 discount = loDiscount[key];
        //                if (discount >= 5 && discount <= 7)
        //                {
        //                    lock (lockObject)
        //                    {
        //                        var revenue = (loExtendedPrice[key] * discount);
        //                        totalRevenue += revenue;
        //                    }

        //                    if (isFirst)
        //                    {
        //                        swInitialRecorder.Stop();
        //                        testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
        //                        isFirst = false;
        //                    }
        //                    //Console.WriteLine(k +", "+ values.ToString() + ", " + sNation);
        //                    outputRecordsCounter++;
        //                    if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
        //                    {
        //                        swOutputRecorder.Stop();
        //                        testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
        //                        swOutputRecorder.Start();
        //                    }
        //                }
        //                else
        //                {
        //                    // Int64ermediateHashTable.Remove(j);
        //                }
        //            }
        //            else
        //            {
        //                // Int64ermediateHashTable.Remove(j);
        //            }
        //            j++;
        //        });

        //        sw.Stop();

        //        long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
        //        #endregion Value Extraction Phase
        //        testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
        //        testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
        //        testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
        //        // Console.WriteLine("[Nimble Join]: Time taken {0} ms.", testResults.totalExecutionTime);
        //        testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
        //        testResults.totalNumberOfOutput = totalRevenue;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}
        //public void Query_2_1()
        //{
        //    try
        //    {
        //        long memoryStartPhase1 = GC.GetTotalMemory(true);
        //        outputRecordsCounter = 0;
        //        var dateHashTable = new Dictionary<Int64, string>();
        //        var partHashTable = new Dictionary<Int64, string>();
        //        var supplierHashTable = new Dictionary<Int64, string>();
        //        Stopwatch sw = new Stopwatch();
        //        Stopwatch swInitialRecorder = new Stopwatch();
        //        Stopwatch swOutputRecorder = new Stopwatch();

        //        #region Key Hashing Phase
        //        List<Date> dateDimension = null;
        //        List<Supplier> supplierDimension = null;
        //        List<Part> partDimension = null;

        //        swInitialRecorder.Start();
        //        swOutputRecorder.Start();
        //        sw.Start();
        //        Parallel.Invoke(parallelOptions,
        //            () => dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor)),
        //            () => partDimension = Utils.ReadFromBinaryFiles<Part>(partFile.Replace("BF", "BF" + scaleFactor)),
        //            () => supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor)));
        //        sw.Stop();
        //        testResults.phase11IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        Parallel.Invoke(parallelOptions, () =>
        //        {
        //            foreach (var row in dateDimension)
        //            {
        //                dateHashTable.Add(row.dDateKey, row.dYear);
        //            }
        //        },
        //            () =>
        //            {
        //                foreach (var row in partDimension)
        //                {
        //                    if (row.pCategory.Equals("MFGR#12"))
        //                        partHashTable.Add(row.pPartKey, row.pBrand);
        //                }
        //            },
        //            () =>
        //            {
        //                foreach (var row in supplierDimension)
        //                {
        //                    if (row.sRegion.Equals("AMERICA"))
        //                        supplierHashTable.Add(row.sSuppKey, row.sNation);
        //                }
        //            }
        //            );

        //        sw.Stop();
        //        testResults.phase11HashTime = sw.ElapsedMilliseconds;
        //        testResults.phase1Time = testResults.phase11HashTime + testResults.phase11IOTime;
        //        sw.Reset();

        //        partDimension.Clear();
        //        dateDimension.Clear();
        //        supplierDimension.Clear();

        //        long memoryUsedPhase1 = GC.GetTotalMemory(true) - memoryStartPhase1;
        //        #endregion Key Hashing Phase

        //        #region Probing Phase
        //        long memoryStartPhase2 = GC.GetTotalMemory(true);
        //        sw.Start();
        //        List<Int64> loOrderDate = null;
        //        List<Int64> loPartKey = null;
        //        List<Int64> loSupplierKey = null;
        //        Parallel.Invoke(parallelOptions,
        //            () => loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loPartKey = Utils.ReadFromBinaryFiles<Int64>(loPartKeyFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor)));
        //        sw.Stop();
        //        testResults.phase21IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        // var _maat = new MAAT(loOrderDate.Count);
        //        var _maat = new Dictionary<Int64, string>();
        //        var k = 0;
        //        foreach (var suppKey in loSupplierKey)
        //        {
        //            string sNationOut = string.Empty;
        //            if (supplierHashTable.TryGetValue(suppKey, out sNationOut))
        //            {
        //                _maat.Add(k, sNationOut);
        //            }
        //            k++;
        //        }

        //        var i = 0;
        //        foreach (var orderDate in loOrderDate)
        //        {
        //            string dYear = "";
        //            if (dateHashTable.TryGetValue(orderDate, out dYear))
        //            {
        //                string record;
        //                if (_maat.TryGetValue(i, out record))
        //                {
        //                    _maat[i] = record + "," + dYear;
        //                }
        //            }
        //            else
        //            {
        //                _maat.Remove(i);
        //            }
        //            i++;
        //        }

        //        var j = 0;
        //        foreach (var partKey in loPartKey)
        //        {
        //            string pBrandOut = string.Empty;
        //            if (partHashTable.TryGetValue(partKey, out pBrandOut))
        //            {
        //                string record;
        //                if (_maat.TryGetValue(j, out record))
        //                {

        //                    _maat[j] = record + "," + pBrandOut;

        //                    if (isFirst)
        //                    {
        //                        swInitialRecorder.Stop();
        //                        testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
        //                        isFirst = false;
        //                    }
        //                    //Console.WriteLine(k +", "+ values.ToString() + ", " + sNation);
        //                    outputRecordsCounter++;
        //                    if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
        //                    {
        //                        swOutputRecorder.Stop();
        //                        testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
        //                        swOutputRecorder.Start();
        //                    }
        //                    //_maat.positions.Add(j);
        //                }
        //            }
        //            else
        //            {
        //                _maat.Remove(j);
        //            }
        //            j++;
        //        }


        //        sw.Stop();
        //        testResults.phase21ProbeTime = sw.ElapsedMilliseconds;

        //        testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime;
        //        sw.Reset();

        //        loOrderDate.Clear();
        //        dateHashTable.Clear();
        //        loPartKey.Clear();
        //        partHashTable.Clear();
        //        loSupplierKey.Clear();
        //        supplierHashTable.Clear();

        //        long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
        //        #endregion Probing Phase


        //        #region Value Extraction Phase
        //        long memoryStartPhase3 = GC.GetTotalMemory(true);
        //        sw.Start();
        //        List<Int64> loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
        //        sw.Stop();
        //        testResults.phase3IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        var joinOutputFinal = new Dictionary<Int64, string>();
        //        foreach (var item in _maat)
        //        {
        //            joinOutputFinal.Add(item.Key, item.Value + ", " + loRevenue[item.Key]); // Direct array lookup
        //        }
        //        sw.Stop();

        //        long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
        //        #endregion Value Extraction Phase
        //        testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
        //        testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
        //        testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
        //        //Console.WriteLine("[Invisble Join]: Time taken {0} ms.", sw.ElapsedMilliseconds);
        //        testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
        //        testResults.totalNumberOfOutput = joinOutputFinal.Count;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}
        //public void Query_2_2()
        //{
        //    try
        //    {
        //        long memoryStartPhase1 = GC.GetTotalMemory(true);
        //        outputRecordsCounter = 0;
        //        var dateHashTable = new Dictionary<Int64, string>();
        //        var partHashTable = new Dictionary<Int64, string>();
        //        var supplierHashTable = new Dictionary<Int64, string>();
        //        Stopwatch sw = new Stopwatch();
        //        Stopwatch swInitialRecorder = new Stopwatch();
        //        Stopwatch swOutputRecorder = new Stopwatch();

        //        #region Key Hashing Phase
        //        List<Date> dateDimension = null;
        //        List<Supplier> supplierDimension = null;
        //        List<Part> partDimension = null;

        //        swInitialRecorder.Start();
        //        swOutputRecorder.Start();
        //        sw.Start();
        //        Parallel.Invoke(parallelOptions,
        //            () => dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor)),
        //            () => partDimension = Utils.ReadFromBinaryFiles<Part>(partFile.Replace("BF", "BF" + scaleFactor)),
        //            () => supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor)));
        //        sw.Stop();
        //        testResults.phase11IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        Parallel.Invoke(parallelOptions, () =>
        //        {
        //            foreach (var row in dateDimension)
        //            {
        //                dateHashTable.Add(row.dDateKey, row.dYear);
        //            }
        //        },
        //            () =>
        //            {
        //                foreach (var row in partDimension)
        //                {
        //                    if (String.CompareOrdinal(row.pBrand, "MFGR#2221") >= 0 && String.CompareOrdinal(row.pBrand, "MFGR#2228") <= 0)
        //                        partHashTable.Add(row.pPartKey, row.pBrand);
        //                }
        //            },
        //            () =>
        //            {
        //                foreach (var row in supplierDimension)
        //                {
        //                    if (row.sRegion.Equals("ASIA"))
        //                        supplierHashTable.Add(row.sSuppKey, row.sNation);
        //                }
        //            }
        //            );

        //        sw.Stop();
        //        testResults.phase11HashTime = sw.ElapsedMilliseconds;
        //        testResults.phase1Time = testResults.phase11HashTime + testResults.phase11IOTime;
        //        sw.Reset();

        //        partDimension.Clear();
        //        dateDimension.Clear();
        //        supplierDimension.Clear();

        //        long memoryUsedPhase1 = GC.GetTotalMemory(true) - memoryStartPhase1;
        //        #endregion Key Hashing Phase

        //        #region Probing Phase
        //        long memoryStartPhase2 = GC.GetTotalMemory(true);
        //        sw.Start();
        //        List<Int64> loOrderDate = null;
        //        List<Int64> loPartKey = null;
        //        List<Int64> loSupplierKey = null;
        //        Parallel.Invoke(parallelOptions,
        //            () => loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loPartKey = Utils.ReadFromBinaryFiles<Int64>(loPartKeyFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor)));
        //        sw.Stop();
        //        testResults.phase21IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        // var _maat = new MAAT(loOrderDate.Count);
        //        var _maat = new Dictionary<Int64, string>();
        //        var k = 0;
        //        foreach (var suppKey in loSupplierKey)
        //        {
        //            string sNationOut = string.Empty;
        //            if (supplierHashTable.TryGetValue(suppKey, out sNationOut))
        //            {
        //                _maat.Add(k, sNationOut);
        //            }
        //            k++;
        //        }

        //        var i = 0;
        //        foreach (var orderDate in loOrderDate)
        //        {
        //            string dYear = "";
        //            if (dateHashTable.TryGetValue(orderDate, out dYear))
        //            {
        //                string record;
        //                if (_maat.TryGetValue(i, out record))
        //                {
        //                    _maat[i] = record + "," + dYear;
        //                }
        //            }
        //            else
        //            {
        //                _maat.Remove(i);
        //            }
        //            i++;
        //        }

        //        var j = 0;
        //        foreach (var partKey in loPartKey)
        //        {
        //            string pBrandOut = string.Empty;
        //            if (partHashTable.TryGetValue(partKey, out pBrandOut))
        //            {
        //                string record;
        //                if (_maat.TryGetValue(j, out record))
        //                {

        //                    _maat[j] = record + "," + pBrandOut;

        //                    if (isFirst)
        //                    {
        //                        swInitialRecorder.Stop();
        //                        testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
        //                        isFirst = false;
        //                    }
        //                    //Console.WriteLine(k +", "+ values.ToString() + ", " + sNation);
        //                    outputRecordsCounter++;
        //                    if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
        //                    {
        //                        swOutputRecorder.Stop();
        //                        testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
        //                        swOutputRecorder.Start();
        //                    }
        //                    //_maat.positions.Add(j);
        //                }
        //            }
        //            else
        //            {
        //                _maat.Remove(j);
        //            }
        //            j++;
        //        }


        //        sw.Stop();
        //        testResults.phase21ProbeTime = sw.ElapsedMilliseconds;

        //        testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime;
        //        sw.Reset();

        //        loOrderDate.Clear();
        //        dateHashTable.Clear();
        //        loPartKey.Clear();
        //        partHashTable.Clear();
        //        loSupplierKey.Clear();
        //        supplierHashTable.Clear();

        //        long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
        //        #endregion Probing Phase


        //        #region Value Extraction Phase
        //        long memoryStartPhase3 = GC.GetTotalMemory(true);
        //        sw.Start();
        //        List<Int64> loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
        //        sw.Stop();
        //        testResults.phase3IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        var joinOutputFinal = new Dictionary<Int64, string>();
        //        foreach (var item in _maat)
        //        {
        //            joinOutputFinal.Add(item.Key, item.Value + ", " + loRevenue[item.Key]); // Direct array lookup
        //        }
        //        sw.Stop();

        //        long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
        //        #endregion Value Extraction Phase
        //        testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
        //        testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
        //        testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
        //        //Console.WriteLine("[Invisble Join]: Time taken {0} ms.", sw.ElapsedMilliseconds);
        //        testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
        //        testResults.totalNumberOfOutput = joinOutputFinal.Count;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}
        //public void Query_2_3()
        //{
        //    try
        //    {
        //        long memoryStartPhase1 = GC.GetTotalMemory(true);
        //        outputRecordsCounter = 0;
        //        var dateHashTable = new Dictionary<Int64, string>();
        //        var partHashTable = new Dictionary<Int64, string>();
        //        var supplierHashTable = new Dictionary<Int64, string>();
        //        Stopwatch sw = new Stopwatch();
        //        Stopwatch swInitialRecorder = new Stopwatch();
        //        Stopwatch swOutputRecorder = new Stopwatch();

        //        #region Key Hashing Phase
        //        List<Date> dateDimension = null;
        //        List<Supplier> supplierDimension = null;
        //        List<Part> partDimension = null;

        //        swInitialRecorder.Start();
        //        swOutputRecorder.Start();
        //        sw.Start();
        //        Parallel.Invoke(parallelOptions,
        //            () => dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor)),
        //            () => partDimension = Utils.ReadFromBinaryFiles<Part>(partFile.Replace("BF", "BF" + scaleFactor)),
        //            () => supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor)));
        //        sw.Stop();
        //        testResults.phase11IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        Parallel.Invoke(parallelOptions, () =>
        //        {
        //            foreach (var row in dateDimension)
        //            {
        //                dateHashTable.Add(row.dDateKey, row.dYear);
        //            }
        //        },
        //            () =>
        //            {
        //                foreach (var row in partDimension)
        //                {
        //                    if (row.pBrand.Equals("MFGR#2221"))
        //                        partHashTable.Add(row.pPartKey, row.pBrand);
        //                }
        //            },
        //            () =>
        //            {
        //                foreach (var row in supplierDimension)
        //                {
        //                    if (row.sRegion.Equals("EUROPE"))
        //                        supplierHashTable.Add(row.sSuppKey, row.sNation);
        //                }
        //            }
        //            );

        //        sw.Stop();
        //        testResults.phase11HashTime = sw.ElapsedMilliseconds;
        //        testResults.phase1Time = testResults.phase11HashTime + testResults.phase11IOTime;
        //        sw.Reset();

        //        partDimension.Clear();
        //        dateDimension.Clear();
        //        supplierDimension.Clear();

        //        long memoryUsedPhase1 = GC.GetTotalMemory(true) - memoryStartPhase1;
        //        #endregion Key Hashing Phase

        //        #region Probing Phase
        //        long memoryStartPhase2 = GC.GetTotalMemory(true);
        //        sw.Start();
        //        List<Int64> loOrderDate = null;
        //        List<Int64> loPartKey = null;
        //        List<Int64> loSupplierKey = null;
        //        Parallel.Invoke(parallelOptions,
        //            () => loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loPartKey = Utils.ReadFromBinaryFiles<Int64>(loPartKeyFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor)));
        //        sw.Stop();
        //        testResults.phase21IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        // var _maat = new MAAT(loOrderDate.Count);
        //        var _maat = new Dictionary<Int64, string>();
        //        var k = 0;
        //        foreach (var suppKey in loSupplierKey)
        //        {
        //            string sNationOut = string.Empty;
        //            if (supplierHashTable.TryGetValue(suppKey, out sNationOut))
        //            {
        //                _maat.Add(k, sNationOut);
        //            }
        //            k++;
        //        }

        //        var i = 0;
        //        foreach (var orderDate in loOrderDate)
        //        {
        //            string dYear = "";
        //            if (dateHashTable.TryGetValue(orderDate, out dYear))
        //            {
        //                string record;
        //                if (_maat.TryGetValue(i, out record))
        //                {
        //                    _maat[i] = record + "," + dYear;
        //                }
        //            }
        //            else
        //            {
        //                _maat.Remove(i);
        //            }
        //            i++;
        //        }

        //        var j = 0;
        //        foreach (var partKey in loPartKey)
        //        {
        //            string pBrandOut = string.Empty;
        //            if (partHashTable.TryGetValue(partKey, out pBrandOut))
        //            {
        //                string record;
        //                if (_maat.TryGetValue(j, out record))
        //                {

        //                    _maat[j] = record + "," + pBrandOut;

        //                    if (isFirst)
        //                    {
        //                        swInitialRecorder.Stop();
        //                        testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
        //                        isFirst = false;
        //                    }
        //                    //Console.WriteLine(k +", "+ values.ToString() + ", " + sNation);
        //                    outputRecordsCounter++;
        //                    if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
        //                    {
        //                        swOutputRecorder.Stop();
        //                        testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
        //                        swOutputRecorder.Start();
        //                    }
        //                    //_maat.positions.Add(j);
        //                }
        //            }
        //            else
        //            {
        //                _maat.Remove(j);
        //            }
        //            j++;
        //        }


        //        sw.Stop();
        //        testResults.phase21ProbeTime = sw.ElapsedMilliseconds;

        //        testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime;
        //        sw.Reset();

        //        loOrderDate.Clear();
        //        dateHashTable.Clear();
        //        loPartKey.Clear();
        //        partHashTable.Clear();
        //        loSupplierKey.Clear();
        //        supplierHashTable.Clear();

        //        long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
        //        #endregion Probing Phase


        //        #region Value Extraction Phase
        //        long memoryStartPhase3 = GC.GetTotalMemory(true);
        //        sw.Start();
        //        List<Int64> loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
        //        sw.Stop();
        //        testResults.phase3IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        var joinOutputFinal = new Dictionary<Int64, string>();
        //        foreach (var item in _maat)
        //        {
        //            joinOutputFinal.Add(item.Key, item.Value + ", " + loRevenue[item.Key]); // Direct array lookup
        //        }
        //        sw.Stop();

        //        long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
        //        #endregion Value Extraction Phase
        //        testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
        //        testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
        //        testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
        //        //Console.WriteLine("[Invisble Join]: Time taken {0} ms.", sw.ElapsedMilliseconds);
        //        testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
        //        testResults.totalNumberOfOutput = joinOutputFinal.Count;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}
        //public void Query_4_1()
        //{

        //    try
        //    {
        //        long memoryStartPhase1 = GC.GetTotalMemory(true);
        //        outputRecordsCounter = 0;
        //        var dateHashTable = new Dictionary<Int64, string>();
        //        var partHashTable = new Dictionary<Int64, string>();
        //        var supplierHashTable = new Dictionary<Int64, string>();
        //        var customerHashTable = new Dictionary<Int64, string>();
        //        Stopwatch sw = new Stopwatch();
        //        Stopwatch swInitialRecorder = new Stopwatch();
        //        Stopwatch swOutputRecorder = new Stopwatch();

        //        #region Key Hashing Phase
        //        List<Date> dateDimension = null;
        //        List<Supplier> supplierDimension = null;
        //        List<Part> partDimension = null;
        //        List<Customer> customerDimension = null;

        //        swInitialRecorder.Start();
        //        swOutputRecorder.Start();
        //        sw.Start();
        //        Parallel.Invoke(parallelOptions,
        //            () => dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor)),
        //            () => partDimension = Utils.ReadFromBinaryFiles<Part>(partFile.Replace("BF", "BF" + scaleFactor)),
        //            () => supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor)),
        //            () => customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor)));
        //        sw.Stop();
        //        testResults.phase11IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        Parallel.Invoke(parallelOptions,
        //            () =>
        //            {
        //                foreach (var row in dateDimension)
        //                {
        //                    dateHashTable.Add(row.dDateKey, row.dYear);
        //                }
        //            },
        //            () =>
        //            {
        //                foreach (var row in partDimension)
        //                {
        //                    if (row.pMFGR.Equals("MFGR#1") || row.pMFGR.Equals("MFGR#2"))
        //                        partHashTable.Add(row.pPartKey, row.pMFGR);
        //                }
        //            },
        //            () =>
        //            {
        //                foreach (var row in supplierDimension)
        //                {
        //                    if (row.sRegion.Equals("AMERICA"))
        //                        supplierHashTable.Add(row.sSuppKey, row.sNation);
        //                }
        //            },
        //            () =>
        //            {
        //                foreach (var row in customerDimension)
        //                {
        //                    if (row.cRegion.Equals("AMERICA"))
        //                        customerHashTable.Add(row.cCustKey, row.cNation);
        //                }
        //            }
        //            );

        //        sw.Stop();
        //        testResults.phase11HashTime = sw.ElapsedMilliseconds;
        //        testResults.phase1Time = testResults.phase11HashTime + testResults.phase11IOTime;
        //        sw.Reset();

        //        partDimension.Clear();
        //        dateDimension.Clear();
        //        supplierDimension.Clear();
        //        customerDimension.Clear();

        //        long memoryUsedPhase1 = GC.GetTotalMemory(true) - memoryStartPhase1;
        //        #endregion Key Hashing Phase

        //        #region Probing Phase
        //        long memoryStartPhase2 = GC.GetTotalMemory(true);
        //        sw.Start();
        //        List<Int64> loOrderDate = null;
        //        List<Int64> loPartKey = null;
        //        List<Int64> loSupplierKey = null;
        //        List<Int64> loCustomerKey = null;
        //        Parallel.Invoke(parallelOptions,
        //            () => loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loPartKey = Utils.ReadFromBinaryFiles<Int64>(loPartKeyFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor)));
        //        sw.Stop();
        //        testResults.phase21IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        // var _maat = new MAAT(loOrderDate.Count);
        //        var _maat = new Dictionary<Int64, string>();
        //        var k = 0;
        //        foreach (var suppKey in loSupplierKey)
        //        {
        //            string sNationOut = string.Empty;
        //            if (supplierHashTable.TryGetValue(suppKey, out sNationOut))
        //            {
        //                _maat.Add(k, sNationOut);
        //            }
        //            k++;
        //        }

        //        var i = 0;
        //        foreach (var orderDate in loOrderDate)
        //        {
        //            string dYear = "";
        //            if (dateHashTable.TryGetValue(orderDate, out dYear))
        //            {
        //                string record;
        //                if (_maat.TryGetValue(i, out record))
        //                {
        //                    _maat[i] = record + "," + dYear;
        //                }
        //            }
        //            else
        //            {
        //                _maat.Remove(i);
        //            }
        //            i++;
        //        }

        //        var j = 0;
        //        foreach (var custKey in loCustomerKey)
        //        {
        //            string cNationOut = string.Empty;
        //            if (customerHashTable.TryGetValue(custKey, out cNationOut))
        //            {
        //                string record;
        //                if (_maat.TryGetValue(j, out record))
        //                {
        //                    _maat[j] = record + "," + cNationOut;
        //                }
        //            }
        //            else
        //            {
        //                _maat.Remove(j);
        //            }
        //            j++;
        //        }

        //        var l = 0;
        //        foreach (var partKey in loPartKey)
        //        {
        //            string pBrandOut = string.Empty;
        //            if (partHashTable.TryGetValue(partKey, out pBrandOut))
        //            {
        //                string record;
        //                if (_maat.TryGetValue(l, out record))
        //                {

        //                    //_maat[l] = record + "," + pBrandOut;

        //                    if (isFirst)
        //                    {
        //                        swInitialRecorder.Stop();
        //                        testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
        //                        isFirst = false;
        //                    }
        //                    //Console.WriteLine(k +", "+ values.ToString() + ", " + sNation);
        //                    outputRecordsCounter++;
        //                    if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
        //                    {
        //                        swOutputRecorder.Stop();
        //                        testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
        //                        swOutputRecorder.Start();
        //                    }
        //                    //_maat.positions.Add(j);
        //                }
        //            }
        //            else
        //            {
        //                _maat.Remove(l);
        //            }
        //            l++;
        //        }


        //        sw.Stop();
        //        testResults.phase21ProbeTime = sw.ElapsedMilliseconds;

        //        testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime;
        //        sw.Reset();

        //        loOrderDate.Clear();
        //        dateHashTable.Clear();
        //        loPartKey.Clear();
        //        partHashTable.Clear();
        //        loSupplierKey.Clear();
        //        supplierHashTable.Clear();
        //        loCustomerKey.Clear();
        //        customerHashTable.Clear();

        //        long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
        //        #endregion Probing Phase


        //        #region Value Extraction Phase
        //        long memoryStartPhase3 = GC.GetTotalMemory(true);
        //        sw.Start();
        //        List<Int64> loRevenue = null;
        //        List<Int64> loSupplyCost = null;

        //        Parallel.Invoke(parallelOptions,
        //            () => loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loSupplyCost = Utils.ReadFromBinaryFiles<Int64>(loSupplyCostFile.Replace("BF", "BF" + scaleFactor)));

        //        sw.Stop();
        //        testResults.phase3IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        var joinOutputFinal = new Dictionary<Int64, string>();
        //        foreach (var item in _maat)
        //        {
        //            joinOutputFinal.Add(item.Key, item.Value + ", " + loRevenue[item.Key] + ", " + loSupplyCost[item.Key]); // Direct array lookup
        //        }
        //        sw.Stop();

        //        long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
        //        #endregion Value Extraction Phase
        //        testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
        //        testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
        //        testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
        //        //Console.WriteLine("[Invisble Join]: Time taken {0} ms.", sw.ElapsedMilliseconds);
        //        testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
        //        testResults.totalNumberOfOutput = joinOutputFinal.Count;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}
        //public void Query_4_2()
        //{
        //    try
        //    {
        //        long memoryStartPhase1 = GC.GetTotalMemory(true);
        //        outputRecordsCounter = 0;
        //        var dateHashTable = new Dictionary<Int64, string>();
        //        var partHashTable = new Dictionary<Int64, string>();
        //        var supplierHashTable = new Dictionary<Int64, string>();
        //        var customerHashTable = new Dictionary<Int64, string>();
        //        Stopwatch sw = new Stopwatch();
        //        Stopwatch swInitialRecorder = new Stopwatch();
        //        Stopwatch swOutputRecorder = new Stopwatch();

        //        #region Key Hashing Phase
        //        List<Date> dateDimension = null;
        //        List<Supplier> supplierDimension = null;
        //        List<Part> partDimension = null;
        //        List<Customer> customerDimension = null;

        //        swInitialRecorder.Start();
        //        swOutputRecorder.Start();
        //        sw.Start();
        //        Parallel.Invoke(parallelOptions,
        //            () => dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor)),
        //            () => partDimension = Utils.ReadFromBinaryFiles<Part>(partFile.Replace("BF", "BF" + scaleFactor)),
        //            () => supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor)),
        //            () => customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor)));
        //        sw.Stop();
        //        testResults.phase11IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        Parallel.Invoke(parallelOptions,
        //            () =>
        //            {
        //                foreach (var row in dateDimension)
        //                {
        //                    if (row.dYear.Equals("1997") || row.dYear.Equals("1998"))
        //                        dateHashTable.Add(row.dDateKey, row.dYear);
        //                }
        //            },
        //            () =>
        //            {
        //                foreach (var row in partDimension)
        //                {
        //                    if (row.pMFGR.Equals("MFGR#1") || row.pMFGR.Equals("MFGR#2"))
        //                        partHashTable.Add(row.pPartKey, row.pCategory);
        //                }
        //            },
        //            () =>
        //            {
        //                foreach (var row in supplierDimension)
        //                {
        //                    if (row.sRegion.Equals("AMERICA"))
        //                        supplierHashTable.Add(row.sSuppKey, row.sNation);
        //                }
        //            },
        //            () =>
        //            {
        //                foreach (var row in customerDimension)
        //                {
        //                    if (row.cRegion.Equals("AMERICA"))
        //                        customerHashTable.Add(row.cCustKey, row.cNation);
        //                }
        //            }
        //            );

        //        sw.Stop();
        //        testResults.phase11HashTime = sw.ElapsedMilliseconds;
        //        testResults.phase1Time = testResults.phase11HashTime + testResults.phase11IOTime;
        //        sw.Reset();

        //        partDimension.Clear();
        //        dateDimension.Clear();
        //        supplierDimension.Clear();
        //        customerDimension.Clear();

        //        long memoryUsedPhase1 = GC.GetTotalMemory(true) - memoryStartPhase1;
        //        #endregion Key Hashing Phase

        //        #region Probing Phase
        //        long memoryStartPhase2 = GC.GetTotalMemory(true);
        //        sw.Start();
        //        List<Int64> loOrderDate = null;
        //        List<Int64> loPartKey = null;
        //        List<Int64> loSupplierKey = null;
        //        List<Int64> loCustomerKey = null;
        //        Parallel.Invoke(parallelOptions,
        //            () => loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loPartKey = Utils.ReadFromBinaryFiles<Int64>(loPartKeyFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor)));
        //        sw.Stop();
        //        testResults.phase21IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        // var _maat = new MAAT(loOrderDate.Count);
        //        var _maat = new Dictionary<Int64, string>();
        //        var k = 0;
        //        foreach (var suppKey in loSupplierKey)
        //        {
        //            string sNationOut = string.Empty;
        //            if (supplierHashTable.TryGetValue(suppKey, out sNationOut))
        //            {
        //                _maat.Add(k, sNationOut);
        //            }
        //            k++;
        //        }

        //        var i = 0;
        //        foreach (var orderDate in loOrderDate)
        //        {
        //            string dYear = "";
        //            if (dateHashTable.TryGetValue(orderDate, out dYear))
        //            {
        //                string record;
        //                if (_maat.TryGetValue(i, out record))
        //                {
        //                    _maat[i] = record + "," + dYear;
        //                }
        //            }
        //            else
        //            {
        //                _maat.Remove(i);
        //            }
        //            i++;
        //        }

        //        var j = 0;
        //        foreach (var custKey in loCustomerKey)
        //        {
        //            string cNationOut = string.Empty;
        //            if (customerHashTable.TryGetValue(custKey, out cNationOut))
        //            {
        //                string record;
        //                if (_maat.TryGetValue(j, out record))
        //                {
        //                    // _maat[j] = record + "," + cNationOut;
        //                }
        //            }
        //            else
        //            {
        //                _maat.Remove(j);
        //            }
        //            j++;
        //        }

        //        var l = 0;
        //        foreach (var partKey in loPartKey)
        //        {
        //            string pCategoryOut = string.Empty;
        //            if (partHashTable.TryGetValue(partKey, out pCategoryOut))
        //            {
        //                string record;
        //                if (_maat.TryGetValue(l, out record))
        //                {

        //                    _maat[l] = record + "," + pCategoryOut;

        //                    if (isFirst)
        //                    {
        //                        swInitialRecorder.Stop();
        //                        testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
        //                        isFirst = false;
        //                    }
        //                    //Console.WriteLine(k +", "+ values.ToString() + ", " + sNation);
        //                    outputRecordsCounter++;
        //                    if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
        //                    {
        //                        swOutputRecorder.Stop();
        //                        testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
        //                        swOutputRecorder.Start();
        //                    }
        //                    //_maat.positions.Add(j);
        //                }
        //            }
        //            else
        //            {
        //                _maat.Remove(l);
        //            }
        //            l++;
        //        }


        //        sw.Stop();
        //        testResults.phase21ProbeTime = sw.ElapsedMilliseconds;

        //        testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime;
        //        sw.Reset();

        //        loOrderDate.Clear();
        //        dateHashTable.Clear();
        //        loPartKey.Clear();
        //        partHashTable.Clear();
        //        loSupplierKey.Clear();
        //        supplierHashTable.Clear();
        //        loCustomerKey.Clear();
        //        customerHashTable.Clear();

        //        long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
        //        #endregion Probing Phase


        //        #region Value Extraction Phase
        //        long memoryStartPhase3 = GC.GetTotalMemory(true);
        //        sw.Start();
        //        List<Int64> loRevenue = null;
        //        List<Int64> loSupplyCost = null;

        //        Parallel.Invoke(parallelOptions,
        //            () => loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loSupplyCost = Utils.ReadFromBinaryFiles<Int64>(loSupplyCostFile.Replace("BF", "BF" + scaleFactor)));

        //        sw.Stop();
        //        testResults.phase3IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        var joinOutputFinal = new Dictionary<Int64, string>();
        //        foreach (var item in _maat)
        //        {
        //            joinOutputFinal.Add(item.Key, item.Value + ", " + loRevenue[item.Key] + ", " + loSupplyCost[item.Key]); // Direct array lookup
        //        }
        //        sw.Stop();

        //        long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
        //        #endregion Value Extraction Phase
        //        testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
        //        testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
        //        testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
        //        //Console.WriteLine("[Invisble Join]: Time taken {0} ms.", sw.ElapsedMilliseconds);
        //        testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
        //        testResults.totalNumberOfOutput = joinOutputFinal.Count;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }

        //}
        //public void Query_4_3()
        //{

        //    try
        //    {
        //        long memoryStartPhase1 = GC.GetTotalMemory(true);
        //        outputRecordsCounter = 0;
        //        var dateHashTable = new Dictionary<Int64, string>();
        //        var partHashTable = new Dictionary<Int64, string>();
        //        var supplierHashTable = new Dictionary<Int64, string>();
        //        var customerHashTable = new Dictionary<Int64, string>();
        //        Stopwatch sw = new Stopwatch();
        //        Stopwatch swInitialRecorder = new Stopwatch();
        //        Stopwatch swOutputRecorder = new Stopwatch();

        //        #region Key Hashing Phase
        //        List<Date> dateDimension = null;
        //        List<Supplier> supplierDimension = null;
        //        List<Part> partDimension = null;
        //        List<Customer> customerDimension = null;

        //        swInitialRecorder.Start();
        //        swOutputRecorder.Start();
        //        sw.Start();
        //        Parallel.Invoke(parallelOptions,
        //            () => dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor)),
        //            () => partDimension = Utils.ReadFromBinaryFiles<Part>(partFile.Replace("BF", "BF" + scaleFactor)),
        //            () => supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor)),
        //            () => customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor)));
        //        sw.Stop();
        //        testResults.phase11IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        Parallel.Invoke(parallelOptions,
        //            () =>
        //            {
        //                foreach (var row in dateDimension)
        //                {
        //                    if (row.dYear.Equals("1997") || row.dYear.Equals("1998"))
        //                        dateHashTable.Add(row.dDateKey, row.dYear);
        //                }
        //            },
        //            () =>
        //            {
        //                foreach (var row in partDimension)
        //                {
        //                    if (row.pCategory.Equals("MFGR#14"))
        //                        partHashTable.Add(row.pPartKey, row.pBrand);
        //                }
        //            },
        //            () =>
        //            {
        //                foreach (var row in supplierDimension)
        //                {
        //                    if (row.sNation.Equals("UNITED STATES"))
        //                        supplierHashTable.Add(row.sSuppKey, row.sNation);
        //                }
        //            },
        //            () =>
        //            {
        //                foreach (var row in customerDimension)
        //                {
        //                    if (row.cRegion.Equals("AMERICA"))
        //                        customerHashTable.Add(row.cCustKey, row.cNation);
        //                }
        //            }
        //            );

        //        sw.Stop();
        //        testResults.phase11HashTime = sw.ElapsedMilliseconds;
        //        testResults.phase1Time = testResults.phase11HashTime + testResults.phase11IOTime;
        //        sw.Reset();

        //        partDimension.Clear();
        //        dateDimension.Clear();
        //        supplierDimension.Clear();
        //        customerDimension.Clear();

        //        long memoryUsedPhase1 = GC.GetTotalMemory(true) - memoryStartPhase1;
        //        #endregion Key Hashing Phase

        //        #region Probing Phase
        //        long memoryStartPhase2 = GC.GetTotalMemory(true);
        //        sw.Start();
        //        List<Int64> loOrderDate = null;
        //        List<Int64> loPartKey = null;
        //        List<Int64> loSupplierKey = null;
        //        List<Int64> loCustomerKey = null;
        //        Parallel.Invoke(parallelOptions,
        //            () => loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loPartKey = Utils.ReadFromBinaryFiles<Int64>(loPartKeyFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor)));
        //        sw.Stop();
        //        testResults.phase21IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        // var _maat = new MAAT(loOrderDate.Count);
        //        var _maat = new Dictionary<Int64, string>();
        //        var k = 0;
        //        foreach (var suppKey in loSupplierKey)
        //        {
        //            string sNationOut = string.Empty;
        //            if (supplierHashTable.TryGetValue(suppKey, out sNationOut))
        //            {
        //                _maat.Add(k, sNationOut);
        //            }
        //            k++;
        //        }

        //        var i = 0;
        //        foreach (var orderDate in loOrderDate)
        //        {
        //            string dYear = "";
        //            if (dateHashTable.TryGetValue(orderDate, out dYear))
        //            {
        //                string record;
        //                if (_maat.TryGetValue(i, out record))
        //                {
        //                    _maat[i] = record + "," + dYear;
        //                }
        //            }
        //            else
        //            {
        //                _maat.Remove(i);
        //            }
        //            i++;
        //        }

        //        var j = 0;
        //        foreach (var custKey in loCustomerKey)
        //        {
        //            string cNationOut = string.Empty;
        //            if (customerHashTable.TryGetValue(custKey, out cNationOut))
        //            {
        //                string record;
        //                if (_maat.TryGetValue(j, out record))
        //                {
        //                    // _maat[j] = record + "," + cNationOut;
        //                }
        //            }
        //            else
        //            {
        //                _maat.Remove(j);
        //            }
        //            j++;
        //        }

        //        var l = 0;
        //        foreach (var partKey in loPartKey)
        //        {
        //            string pBrandOut = string.Empty;
        //            if (partHashTable.TryGetValue(partKey, out pBrandOut))
        //            {
        //                string record;
        //                if (_maat.TryGetValue(l, out record))
        //                {

        //                    _maat[l] = record + "," + pBrandOut;

        //                    if (isFirst)
        //                    {
        //                        swInitialRecorder.Stop();
        //                        testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
        //                        isFirst = false;
        //                    }
        //                    //Console.WriteLine(k +", "+ values.ToString() + ", " + sNation);
        //                    outputRecordsCounter++;
        //                    if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
        //                    {
        //                        swOutputRecorder.Stop();
        //                        testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
        //                        swOutputRecorder.Start();
        //                    }
        //                    //_maat.positions.Add(j);
        //                }
        //            }
        //            else
        //            {
        //                _maat.Remove(l);
        //            }
        //            l++;
        //        }


        //        sw.Stop();
        //        testResults.phase21ProbeTime = sw.ElapsedMilliseconds;

        //        testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime;
        //        sw.Reset();

        //        loOrderDate.Clear();
        //        dateHashTable.Clear();
        //        loPartKey.Clear();
        //        partHashTable.Clear();
        //        loSupplierKey.Clear();
        //        supplierHashTable.Clear();
        //        loCustomerKey.Clear();
        //        customerHashTable.Clear();

        //        long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
        //        #endregion Probing Phase


        //        #region Value Extraction Phase
        //        long memoryStartPhase3 = GC.GetTotalMemory(true);
        //        sw.Start();
        //        List<Int64> loRevenue = null;
        //        List<Int64> loSupplyCost = null;

        //        Parallel.Invoke(parallelOptions,
        //            () => loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loSupplyCost = Utils.ReadFromBinaryFiles<Int64>(loSupplyCostFile.Replace("BF", "BF" + scaleFactor)));

        //        sw.Stop();
        //        testResults.phase3IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        var joinOutputFinal = new Dictionary<Int64, string>();
        //        foreach (var item in _maat)
        //        {
        //            joinOutputFinal.Add(item.Key, item.Value + ", " + loRevenue[item.Key] + ", " + loSupplyCost[item.Key]); // Direct array lookup
        //        }
        //        sw.Stop();

        //        long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
        //        #endregion Value Extraction Phase
        //        testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
        //        testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
        //        testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
        //        //Console.WriteLine("[Invisble Join]: Time taken {0} ms.", sw.ElapsedMilliseconds);
        //        testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
        //        testResults.totalNumberOfOutput = joinOutputFinal.Count;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}
        //public void Query_3_1()
        //{
        //    try
        //    {
        //        long memoryStartPhase1 = GC.GetTotalMemory(true);
        //        outputRecordsCounter = 0;
        //        var dateHashTable = new Dictionary<Int64, string>();
        //        var customerHashTable = new Dictionary<Int64, string>();
        //        var supplierHashTable = new Dictionary<Int64, string>();
        //        Stopwatch sw = new Stopwatch();
        //        Stopwatch swInitialRecorder = new Stopwatch();
        //        Stopwatch swOutputRecorder = new Stopwatch();

        //        #region Key Hashing Phase
        //        List<Date> dateDimension = null;
        //        List<Supplier> supplierDimension = null;
        //        List<Customer> customerDimension = null;

        //        swInitialRecorder.Start();
        //        swOutputRecorder.Start();
        //        sw.Start();

        //        Parallel.Invoke(parallelOptions, () => dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor)),
        //        () => customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor)),
        //        () => supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor))
        //             );
        //        sw.Stop();
        //        testResults.phase11IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        Parallel.Invoke(parallelOptions, () =>
        //        {
        //            foreach (var row in dateDimension)
        //            {
        //                if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
        //                    dateHashTable.Add(row.dDateKey, row.dYear);
        //            }
        //        },
        //        () =>
        //        {
        //            foreach (var row in customerDimension)
        //            {
        //                if (row.cRegion.Equals("ASIA"))
        //                    customerHashTable.Add(row.cCustKey, row.cNation);
        //            }
        //        },
        //        () =>
        //        {
        //            foreach (var row in supplierDimension)
        //            {
        //                if (row.sRegion.Equals("ASIA"))
        //                    supplierHashTable.Add(row.sSuppKey, row.sNation);
        //            }
        //        });
        //        sw.Stop();
        //        testResults.phase11HashTime = sw.ElapsedMilliseconds;
        //        testResults.phase1Time = testResults.phase11HashTime + testResults.phase11IOTime;
        //        sw.Reset();

        //        customerDimension.Clear();
        //        dateDimension.Clear();
        //        supplierDimension.Clear();

        //        long memoryUsedPhase1 = GC.GetTotalMemory(true) - memoryStartPhase1;
        //        #endregion Key Hashing Phase

        //        #region Probing Phase
        //        long memoryStartPhase2 = GC.GetTotalMemory(true);
        //        sw.Start();
        //        List<Int64> loOrderDate = null;
        //        List<Int64> loCustomerKey = null;
        //        List<Int64> loSupplierKey = null;
        //        Parallel.Invoke(parallelOptions, () => loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor)),
        //          () => loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor)),
        //          () => loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor)));
        //        sw.Stop();
        //        testResults.phase21IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        var Int64ermediateHashTable = new Dictionary<Int64, string>();
        //        var i = 0;
        //        foreach (var suppKey in loSupplierKey)
        //        {
        //            string sNation = string.Empty;
        //            if (supplierHashTable.TryGetValue(suppKey, out sNation))
        //            {
        //                Int64ermediateHashTable.Add(i, sNation);
        //            }
        //            i++;
        //        }

        //        var k = 0;
        //        foreach (var orderDate in loOrderDate)
        //        {
        //            string dYear = string.Empty;
        //            if (dateHashTable.TryGetValue(orderDate, out dYear))
        //            {
        //                string values = string.Empty;
        //                if (Int64ermediateHashTable.TryGetValue(k, out values))
        //                {
        //                    Int64ermediateHashTable[k] = values + ", " + dYear;
        //                }
        //            }
        //            else
        //            {
        //                Int64ermediateHashTable.Remove(k);
        //            }
        //            k++;
        //        }

        //        var j = 0;
        //        foreach (var custKey in loCustomerKey)
        //        {
        //            string cNation = string.Empty;
        //            if (customerHashTable.TryGetValue(custKey, out cNation))
        //            {
        //                string values = string.Empty;
        //                if (Int64ermediateHashTable.TryGetValue(j, out values))
        //                {
        //                    Int64ermediateHashTable[j] = values + ", " + cNation;
        //                    if (isFirst)
        //                    {
        //                        swInitialRecorder.Stop();
        //                        testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
        //                        isFirst = false;
        //                    }
        //                    //Console.WriteLine(k +", "+ values.ToString() + ", " + sNation);

        //                    outputRecordsCounter++;
        //                    if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
        //                    {
        //                        swOutputRecorder.Stop();
        //                        testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
        //                        swOutputRecorder.Start();
        //                    }
        //                }
        //            }
        //            else
        //            {
        //                Int64ermediateHashTable.Remove(j);
        //            }
        //            j++;
        //        }

        //        sw.Stop();
        //        testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
        //        testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime;
        //        sw.Reset();

        //        loOrderDate.Clear();
        //        loCustomerKey.Clear();
        //        loSupplierKey.Clear();

        //        long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
        //        #endregion Probing Phase

        //        #region Value Extraction Phase
        //        long memoryStartPhase3 = GC.GetTotalMemory(true);
        //        sw.Start();
        //        List<Int64> loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
        //        sw.Stop();
        //        testResults.phase3IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        var joinOutputFinal = new Dictionary<Int64, string>();
        //        foreach (var item in Int64ermediateHashTable)
        //        {
        //            joinOutputFinal.Add(item.Key, item.Value + ", " + loRevenue[item.Key]); // Direct array lookup
        //        }
        //        sw.Stop();

        //        long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
        //        #endregion Value Extraction Phase
        //        testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
        //        testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
        //        testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
        //        // Console.WriteLine("[Nimble Join]: Time taken {0} ms.", testResults.totalExecutionTime);
        //        testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
        //        testResults.totalNumberOfOutput = Int64ermediateHashTable.Count;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}
        //public void Query_3_1(string selectivityRatio)
        //{
        //    try
        //    {
        //        outputRecordsCounter = 0;
        //        var dateHashTable = new Dictionary<Int64, string>();
        //        var customerHashTable = new Dictionary<Int64, string>();
        //        var supplierHashTable = new Dictionary<Int64, string>();
        //        Stopwatch sw = new Stopwatch();
        //        Stopwatch swInitialRecorder = new Stopwatch();
        //        Stopwatch swOutputRecorder = new Stopwatch();

        //        #region Key Hashing Phase
        //        List<Date> dateDimension = null;
        //        List<Supplier> supplierDimension = null;
        //        List<Customer> customerDimension = null;
        //        swInitialRecorder.Start();
        //        swOutputRecorder.Start();
        //        switch (selectivityRatio)
        //        {
        //            case "0.007":
        //                sw.Start();
        //                Parallel.Invoke(parallelOptions, () => dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor)),
        //                () => customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor)),
        //                () => supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor))
        //                     );
        //                sw.Stop();
        //                testResults.phase11IOTime = sw.ElapsedMilliseconds;
        //                sw.Reset();

        //                sw.Start();
        //                Parallel.Invoke(parallelOptions, () =>
        //                {
        //                    foreach (var row in dateDimension)
        //                    {
        //                        if (row.dYear.Equals("1992"))
        //                            dateHashTable.Add(row.dDateKey, row.dYear);
        //                    }
        //                },
        //                () =>
        //                {
        //                    foreach (var row in customerDimension)
        //                    {
        //                        if (row.cRegion.Equals("ASIA"))
        //                            customerHashTable.Add(row.cCustKey, row.cNation);
        //                    }
        //                },
        //                () =>
        //                {
        //                    foreach (var row in supplierDimension)
        //                    {
        //                        if (row.sRegion.Equals("ASIA"))
        //                            supplierHashTable.Add(row.sSuppKey, row.sNation);
        //                    }
        //                });
        //                sw.Stop();
        //                break;
        //            case "0.07":
        //                sw.Start();

        //                Parallel.Invoke(parallelOptions, () => dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor)),
        //                () => customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor)),
        //                () => supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor))
        //                     );
        //                sw.Stop();
        //                testResults.phase11IOTime = sw.ElapsedMilliseconds;
        //                sw.Reset();

        //                sw.Start();
        //                Parallel.Invoke(parallelOptions, () =>
        //                {
        //                    foreach (var row in dateDimension)
        //                    {
        //                        if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1996") <= 0)
        //                            dateHashTable.Add(row.dDateKey, row.dYear);
        //                    }
        //                },
        //                () =>
        //                {
        //                    foreach (var row in customerDimension)
        //                    {
        //                        if (row.cRegion.Equals("ASIA") || row.cRegion.Equals("AFRICA"))
        //                            customerHashTable.Add(row.cCustKey, row.cNation);
        //                    }
        //                },
        //                () =>
        //                {
        //                    foreach (var row in supplierDimension)
        //                    {
        //                        if (row.sRegion.Equals("ASIA"))
        //                            supplierHashTable.Add(row.sSuppKey, row.sNation);
        //                    }
        //                });
        //                sw.Stop();
        //                break;
        //            case "0.7":
        //                sw.Start();

        //                Parallel.Invoke(parallelOptions, () => dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor)),
        //                () => customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor)),
        //                () => supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor))
        //                     );
        //                sw.Stop();
        //                testResults.phase11IOTime = sw.ElapsedMilliseconds;
        //                sw.Reset();

        //                sw.Start();
        //                Parallel.Invoke(parallelOptions, () =>
        //                {
        //                    foreach (var row in dateDimension)
        //                    {
        //                        if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1999") <= 0)
        //                            dateHashTable.Add(row.dDateKey, row.dYear);
        //                    }
        //                },
        //                () =>
        //                {
        //                    foreach (var row in customerDimension)
        //                    {
        //                        if (row.cRegion.Equals("ASIA") || row.cRegion.Equals("AMERICA") || row.cRegion.Equals("EUROPE") || row.cRegion.Equals("AFRICA"))
        //                            customerHashTable.Add(row.cCustKey, row.cNation);
        //                    }
        //                },
        //                () =>
        //                {
        //                    foreach (var row in supplierDimension)
        //                    {
        //                        if (row.sRegion.Equals("ASIA") || row.sRegion.Equals("AMERICA") || row.sRegion.Equals("EUROPE") || row.sRegion.Equals("AFRICA"))
        //                            supplierHashTable.Add(row.sSuppKey, row.sNation);
        //                    }
        //                });
        //                sw.Stop();
        //                break;
        //        }
        //        testResults.phase11HashTime = sw.ElapsedMilliseconds;
        //        testResults.phase1Time = testResults.phase11HashTime + testResults.phase11IOTime;
        //        sw.Reset();

        //        customerDimension.Clear();
        //        dateDimension.Clear();
        //        supplierDimension.Clear();

        //        #endregion Key Hashing Phase

        //        #region Probing Phase

        //        sw.Start();
        //        List<Int64> loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
        //        sw.Stop();
        //        testResults.phase21IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        var Int64ermediateHashTable = new Dictionary<Int64, string>();
        //        var i = 0;
        //        foreach (var orderDate in loOrderDate)
        //        {
        //            string dYear = "";
        //            if (dateHashTable.TryGetValue(orderDate, out dYear))
        //            {
        //                Int64ermediateHashTable.Add(i, dYear);
        //            }
        //            i++;
        //        }
        //        sw.Stop();
        //        testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        List<Int64> loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
        //        sw.Stop();
        //        testResults.phase22IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        var j = 0;
        //        foreach (var custKey in loCustomerKey)
        //        {
        //            string cNation = string.Empty;
        //            if (customerHashTable.TryGetValue(custKey, out cNation))
        //            {
        //                string values = string.Empty;
        //                if (Int64ermediateHashTable.TryGetValue(j, out values))
        //                {
        //                    Int64ermediateHashTable[j] = values + ", " + cNation;
        //                }
        //            }
        //            else
        //            {
        //                Int64ermediateHashTable.Remove(j);
        //            }
        //            j++;
        //        }
        //        sw.Stop();
        //        testResults.phase22ProbeTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        List<Int64> loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
        //        sw.Stop();
        //        testResults.phase23IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        var k = 0;
        //        foreach (var suppKey in loSupplierKey)
        //        {
        //            string sNation = string.Empty;
        //            if (supplierHashTable.TryGetValue(suppKey, out sNation))
        //            {
        //                string values = string.Empty;
        //                if (Int64ermediateHashTable.TryGetValue(k, out values))
        //                {
        //                    Int64ermediateHashTable[k] = values + ", " + sNation;
        //                    if (isFirst)
        //                    {
        //                        swInitialRecorder.Stop();
        //                        testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
        //                        isFirst = false;
        //                    }
        //                    //Console.WriteLine(k +", "+ values.ToString() + ", " + sNation);
        //                    outputRecordsCounter++;
        //                    if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
        //                    {
        //                        sw.Stop();
        //                        testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, sw.ElapsedTicks));
        //                        sw.Start();
        //                    }
        //                }
        //            }
        //            else
        //            {
        //                Int64ermediateHashTable.Remove(k);
        //            }
        //            k++;
        //        }
        //        sw.Stop();
        //        testResults.phase23ProbeTime = sw.ElapsedMilliseconds;
        //        testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime +
        //            testResults.phase22IOTime + testResults.phase22ProbeTime +
        //            testResults.phase23IOTime + testResults.phase23ProbeTime;
        //        sw.Reset();

        //        loOrderDate.Clear();
        //        loCustomerKey.Clear();
        //        loSupplierKey.Clear();
        //        #endregion Probing Phase

        //        #region Value Extraction Phase

        //        sw.Start();
        //        List<Int64> loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
        //        sw.Stop();
        //        testResults.phase3IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        var joinOutputFinal = new Dictionary<Int64, string>();
        //        foreach (var item in Int64ermediateHashTable)
        //        {
        //            joinOutputFinal.Add(item.Key, item.Value + ", " + loRevenue[item.Key]); // Direct array lookup
        //        }
        //        sw.Stop();
        //        testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
        //        testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;

        //        #endregion Value Extraction Phase
        //        testResults.totalNumberOfOutput = joinOutputFinal.Count;
        //        testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
        //        // Console.WriteLine("[Nimble Join]: Time taken {0} ms.", sw.ElapsedMilliseconds);

        //    }
        //    catch (Exception ex)
        //    {
        //        Console.WriteLine(ex.ToString());
        //    }
        //}
        //public void Query_3_2()
        //{
        //    try
        //    {
        //        long memoryStartPhase1 = GC.GetTotalMemory(true);
        //        outputRecordsCounter = 0;
        //        var dateHashTable = new Dictionary<Int64, string>();
        //        var customerHashTable = new Dictionary<Int64, string>();
        //        var supplierHashTable = new Dictionary<Int64, string>();
        //        Stopwatch sw = new Stopwatch();
        //        Stopwatch swInitialRecorder = new Stopwatch();
        //        Stopwatch swOutputRecorder = new Stopwatch();

        //        #region Key Hashing Phase
        //        List<Date> dateDimension = null;
        //        List<Supplier> supplierDimension = null;
        //        List<Customer> customerDimension = null;

        //        swInitialRecorder.Start();
        //        swOutputRecorder.Start();
        //        sw.Start();

        //        Parallel.Invoke(parallelOptions, () => dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor)),
        //        () => customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor)),
        //        () => supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor))
        //             );
        //        sw.Stop();
        //        testResults.phase11IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        Parallel.Invoke(parallelOptions, () =>
        //        {
        //            foreach (var row in dateDimension)
        //            {
        //                if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
        //                    dateHashTable.Add(row.dDateKey, row.dYear);
        //            }
        //        },
        //        () =>
        //        {
        //            foreach (var row in customerDimension)
        //            {
        //                if (row.cNation.Equals("UNITED STATES"))
        //                    customerHashTable.Add(row.cCustKey, row.cCity);
        //            }
        //        },
        //        () =>
        //        {
        //            foreach (var row in supplierDimension)
        //            {
        //                if (row.sNation.Equals("UNITED STATES"))
        //                    supplierHashTable.Add(row.sSuppKey, row.sCity);
        //            }
        //        });
        //        sw.Stop();
        //        testResults.phase11HashTime = sw.ElapsedMilliseconds;
        //        testResults.phase1Time = testResults.phase11HashTime + testResults.phase11IOTime;
        //        sw.Reset();

        //        customerDimension.Clear();
        //        dateDimension.Clear();
        //        supplierDimension.Clear();

        //        long memoryUsedPhase1 = GC.GetTotalMemory(true) - memoryStartPhase1;
        //        #endregion Key Hashing Phase

        //        #region Probing Phase
        //        long memoryStartPhase2 = GC.GetTotalMemory(true);
        //        sw.Start();
        //        List<Int64> loOrderDate = null;
        //        List<Int64> loCustomerKey = null;
        //        List<Int64> loSupplierKey = null;
        //        Parallel.Invoke(parallelOptions, () => loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor)),
        //          () => loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor)),
        //          () => loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor)));
        //        sw.Stop();
        //        testResults.phase21IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        var Int64ermediateHashTable = new Dictionary<Int64, string>();
        //        var i = 0;
        //        foreach (var suppKey in loSupplierKey)
        //        {
        //            string sCity = string.Empty;
        //            if (supplierHashTable.TryGetValue(suppKey, out sCity))
        //            {
        //                Int64ermediateHashTable.Add(i, sCity);
        //            }
        //            i++;
        //        }

        //        var k = 0;
        //        foreach (var orderDate in loOrderDate)
        //        {
        //            string dYear = string.Empty;
        //            if (dateHashTable.TryGetValue(orderDate, out dYear))
        //            {
        //                string values = string.Empty;
        //                if (Int64ermediateHashTable.TryGetValue(k, out values))
        //                {
        //                    Int64ermediateHashTable[k] = values + ", " + dYear;
        //                }
        //            }
        //            else
        //            {
        //                Int64ermediateHashTable.Remove(k);
        //            }
        //            k++;
        //        }

        //        var j = 0;
        //        foreach (var custKey in loCustomerKey)
        //        {
        //            string cCity = string.Empty;
        //            if (customerHashTable.TryGetValue(custKey, out cCity))
        //            {
        //                string values = string.Empty;
        //                if (Int64ermediateHashTable.TryGetValue(j, out values))
        //                {
        //                    Int64ermediateHashTable[j] = values + ", " + cCity;
        //                    if (isFirst)
        //                    {
        //                        swInitialRecorder.Stop();
        //                        testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
        //                        isFirst = false;
        //                    }
        //                    //Console.WriteLine(k +", "+ values.ToString() + ", " + sNation);

        //                    outputRecordsCounter++;
        //                    if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
        //                    {
        //                        swOutputRecorder.Stop();
        //                        testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
        //                        swOutputRecorder.Start();
        //                    }
        //                }
        //            }
        //            else
        //            {
        //                Int64ermediateHashTable.Remove(j);
        //            }
        //            j++;
        //        }

        //        sw.Stop();
        //        testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
        //        testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime;
        //        sw.Reset();

        //        loOrderDate.Clear();
        //        loCustomerKey.Clear();
        //        loSupplierKey.Clear();

        //        long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
        //        #endregion Probing Phase

        //        #region Value Extraction Phase
        //        long memoryStartPhase3 = GC.GetTotalMemory(true);
        //        sw.Start();
        //        List<Int64> loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
        //        sw.Stop();
        //        testResults.phase3IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        var joinOutputFinal = new Dictionary<Int64, string>();
        //        foreach (var item in Int64ermediateHashTable)
        //        {
        //            joinOutputFinal.Add(item.Key, item.Value + ", " + loRevenue[item.Key]); // Direct array lookup
        //        }
        //        sw.Stop();

        //        long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
        //        #endregion Value Extraction Phase
        //        testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
        //        testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
        //        testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
        //        // Console.WriteLine("[Nimble Join]: Time taken {0} ms.", testResults.totalExecutionTime);
        //        testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
        //        testResults.totalNumberOfOutput = Int64ermediateHashTable.Count;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}
        //public void Query_3_3()
        //{
        //    try
        //    {
        //        long memoryStartPhase1 = GC.GetTotalMemory(true);
        //        outputRecordsCounter = 0;
        //        var dateHashTable = new Dictionary<Int64, string>();
        //        var customerHashTable = new Dictionary<Int64, string>();
        //        var supplierHashTable = new Dictionary<Int64, string>();
        //        Stopwatch sw = new Stopwatch();
        //        Stopwatch swInitialRecorder = new Stopwatch();
        //        Stopwatch swOutputRecorder = new Stopwatch();

        //        #region Key Hashing Phase
        //        List<Date> dateDimension = null;
        //        List<Supplier> supplierDimension = null;
        //        List<Customer> customerDimension = null;

        //        swInitialRecorder.Start();
        //        swOutputRecorder.Start();
        //        sw.Start();

        //        Parallel.Invoke(parallelOptions, () => dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor)),
        //        () => customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor)),
        //        () => supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor))
        //             );
        //        sw.Stop();
        //        testResults.phase11IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        Parallel.Invoke(parallelOptions, () =>
        //        {
        //            foreach (var row in dateDimension)
        //            {
        //                if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
        //                    dateHashTable.Add(row.dDateKey, row.dYear);
        //            }
        //        },
        //        () =>
        //        {
        //            foreach (var row in customerDimension)
        //            {
        //                if (row.cCity.Equals("UNITED KI1") || row.cCity.Equals("UNITED KI5"))
        //                    customerHashTable.Add(row.cCustKey, row.cCity);
        //            }
        //        },
        //        () =>
        //        {
        //            foreach (var row in supplierDimension)
        //            {
        //                if (row.sCity.Equals("UNITED KI1") || row.sCity.Equals("UNITED KI5"))
        //                    supplierHashTable.Add(row.sSuppKey, row.sCity);
        //            }
        //        });
        //        sw.Stop();
        //        testResults.phase11HashTime = sw.ElapsedMilliseconds;
        //        testResults.phase1Time = testResults.phase11HashTime + testResults.phase11IOTime;
        //        sw.Reset();

        //        customerDimension.Clear();
        //        dateDimension.Clear();
        //        supplierDimension.Clear();

        //        long memoryUsedPhase1 = GC.GetTotalMemory(true) - memoryStartPhase1;
        //        #endregion Key Hashing Phase

        //        #region Probing Phase
        //        long memoryStartPhase2 = GC.GetTotalMemory(true);
        //        sw.Start();
        //        List<Int64> loOrderDate = null;
        //        List<Int64> loCustomerKey = null;
        //        List<Int64> loSupplierKey = null;
        //        Parallel.Invoke(parallelOptions, () => loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor)),
        //          () => loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor)),
        //          () => loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor)));
        //        sw.Stop();
        //        testResults.phase21IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        var Int64ermediateHashTable = new Dictionary<Int64, string>();
        //        var i = 0;
        //        foreach (var suppKey in loSupplierKey)
        //        {
        //            string sCity = string.Empty;
        //            if (supplierHashTable.TryGetValue(suppKey, out sCity))
        //            {
        //                Int64ermediateHashTable.Add(i, sCity);
        //            }
        //            i++;
        //        }

        //        var k = 0;
        //        foreach (var orderDate in loOrderDate)
        //        {
        //            string dYear = string.Empty;
        //            if (dateHashTable.TryGetValue(orderDate, out dYear))
        //            {
        //                string values = string.Empty;
        //                if (Int64ermediateHashTable.TryGetValue(k, out values))
        //                {
        //                    Int64ermediateHashTable[k] = values + ", " + dYear;
        //                }
        //            }
        //            else
        //            {
        //                Int64ermediateHashTable.Remove(k);
        //            }
        //            k++;
        //        }

        //        var j = 0;
        //        foreach (var custKey in loCustomerKey)
        //        {
        //            string cCity = string.Empty;
        //            if (customerHashTable.TryGetValue(custKey, out cCity))
        //            {
        //                string values = string.Empty;
        //                if (Int64ermediateHashTable.TryGetValue(j, out values))
        //                {
        //                    Int64ermediateHashTable[j] = values + ", " + cCity;
        //                    if (isFirst)
        //                    {
        //                        swInitialRecorder.Stop();
        //                        testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
        //                        isFirst = false;
        //                    }
        //                    //Console.WriteLine(k +", "+ values.ToString() + ", " + sNation);

        //                    outputRecordsCounter++;
        //                    if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
        //                    {
        //                        swOutputRecorder.Stop();
        //                        testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
        //                        swOutputRecorder.Start();
        //                    }
        //                }
        //            }
        //            else
        //            {
        //                Int64ermediateHashTable.Remove(j);
        //            }
        //            j++;
        //        }

        //        sw.Stop();
        //        testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
        //        testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime;
        //        sw.Reset();

        //        loOrderDate.Clear();
        //        loCustomerKey.Clear();
        //        loSupplierKey.Clear();

        //        long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
        //        #endregion Probing Phase

        //        #region Value Extraction Phase
        //        long memoryStartPhase3 = GC.GetTotalMemory(true);
        //        sw.Start();
        //        List<Int64> loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
        //        sw.Stop();
        //        testResults.phase3IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        var joinOutputFinal = new Dictionary<Int64, string>();
        //        foreach (var item in Int64ermediateHashTable)
        //        {
        //            joinOutputFinal.Add(item.Key, item.Value + ", " + loRevenue[item.Key]); // Direct array lookup
        //        }
        //        sw.Stop();

        //        long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
        //        #endregion Value Extraction Phase
        //        testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
        //        testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
        //        testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
        //        // Console.WriteLine("[Nimble Join]: Time taken {0} ms.", testResults.totalExecutionTime);
        //        testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
        //        testResults.totalNumberOfOutput = Int64ermediateHashTable.Count;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}
        //public void Query_3_4()
        //{
        //    try
        //    {
        //        long memoryStartPhase1 = GC.GetTotalMemory(true);
        //        outputRecordsCounter = 0;
        //        var dateHashTable = new Dictionary<Int64, string>();
        //        var customerHashTable = new Dictionary<Int64, string>();
        //        var supplierHashTable = new Dictionary<Int64, string>();
        //        Stopwatch sw = new Stopwatch();
        //        Stopwatch swInitialRecorder = new Stopwatch();
        //        Stopwatch swOutputRecorder = new Stopwatch();

        //        #region Key Hashing Phase
        //        List<Date> dateDimension = null;
        //        List<Supplier> supplierDimension = null;
        //        List<Customer> customerDimension = null;

        //        swInitialRecorder.Start();
        //        swOutputRecorder.Start();
        //        sw.Start();

        //        Parallel.Invoke(parallelOptions, () => dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor)),
        //        () => customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor)),
        //        () => supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor))
        //             );
        //        sw.Stop();
        //        testResults.phase11IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        Parallel.Invoke(parallelOptions, () =>
        //        {
        //            foreach (var row in dateDimension)
        //            {
        //                if (row.dYearMonth.Equals("Dec1997"))
        //                    dateHashTable.Add(row.dDateKey, row.dYear);
        //            }
        //        },
        //        () =>
        //        {
        //            foreach (var row in customerDimension)
        //            {
        //                if (row.cCity.Equals("UNITED KI1") || row.cCity.Equals("UNITED KI5"))
        //                    customerHashTable.Add(row.cCustKey, row.cCity);
        //            }
        //        },
        //        () =>
        //        {
        //            foreach (var row in supplierDimension)
        //            {
        //                if (row.sCity.Equals("UNITED KI1") || row.sCity.Equals("UNITED KI5"))
        //                    supplierHashTable.Add(row.sSuppKey, row.sCity);
        //            }
        //        });
        //        sw.Stop();
        //        testResults.phase11HashTime = sw.ElapsedMilliseconds;
        //        testResults.phase1Time = testResults.phase11HashTime + testResults.phase11IOTime;
        //        sw.Reset();

        //        customerDimension.Clear();
        //        dateDimension.Clear();
        //        supplierDimension.Clear();

        //        long memoryUsedPhase1 = GC.GetTotalMemory(true) - memoryStartPhase1;
        //        #endregion Key Hashing Phase

        //        #region Probing Phase
        //        long memoryStartPhase2 = GC.GetTotalMemory(true);
        //        sw.Start();
        //        List<Int64> loOrderDate = null;
        //        List<Int64> loCustomerKey = null;
        //        List<Int64> loSupplierKey = null;
        //        Parallel.Invoke(parallelOptions, () => loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor)),
        //          () => loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor)),
        //          () => loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor)));
        //        sw.Stop();
        //        testResults.phase21IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        var Int64ermediateHashTable = new Dictionary<Int64, string>();
        //        var i = 0;
        //        foreach (var suppKey in loSupplierKey)
        //        {
        //            string sCity = string.Empty;
        //            if (supplierHashTable.TryGetValue(suppKey, out sCity))
        //            {
        //                Int64ermediateHashTable.Add(i, sCity);
        //            }
        //            i++;
        //        }

        //        var k = 0;
        //        foreach (var orderDate in loOrderDate)
        //        {
        //            string dYear = string.Empty;
        //            if (dateHashTable.TryGetValue(orderDate, out dYear))
        //            {
        //                string values = string.Empty;
        //                if (Int64ermediateHashTable.TryGetValue(k, out values))
        //                {
        //                    Int64ermediateHashTable[k] = values + ", " + dYear;
        //                }
        //            }
        //            else
        //            {
        //                Int64ermediateHashTable.Remove(k);
        //            }
        //            k++;
        //        }

        //        var j = 0;
        //        foreach (var custKey in loCustomerKey)
        //        {
        //            string cCity = string.Empty;
        //            if (customerHashTable.TryGetValue(custKey, out cCity))
        //            {
        //                string values = string.Empty;
        //                if (Int64ermediateHashTable.TryGetValue(j, out values))
        //                {
        //                    Int64ermediateHashTable[j] = values + ", " + cCity;
        //                    if (isFirst)
        //                    {
        //                        swInitialRecorder.Stop();
        //                        testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
        //                        isFirst = false;
        //                    }
        //                    //Console.WriteLine(k +", "+ values.ToString() + ", " + sNation);

        //                    outputRecordsCounter++;
        //                    if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
        //                    {
        //                        swOutputRecorder.Stop();
        //                        testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
        //                        swOutputRecorder.Start();
        //                    }
        //                }
        //            }
        //            else
        //            {
        //                Int64ermediateHashTable.Remove(j);
        //            }
        //            j++;
        //        }

        //        sw.Stop();
        //        testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
        //        testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime;
        //        sw.Reset();

        //        loOrderDate.Clear();
        //        loCustomerKey.Clear();
        //        loSupplierKey.Clear();

        //        long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
        //        #endregion Probing Phase

        //        #region Value Extraction Phase
        //        long memoryStartPhase3 = GC.GetTotalMemory(true);
        //        sw.Start();
        //        List<Int64> loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
        //        sw.Stop();
        //        testResults.phase3IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        var joinOutputFinal = new Dictionary<Int64, string>();
        //        foreach (var item in Int64ermediateHashTable)
        //        {
        //            joinOutputFinal.Add(item.Key, item.Value + ", " + loRevenue[item.Key]); // Direct array lookup
        //        }
        //        sw.Stop();

        //        long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
        //        #endregion Value Extraction Phase
        //        testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
        //        testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
        //        testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
        //        // Console.WriteLine("[Nimble Join]: Time taken {0} ms.", testResults.totalExecutionTime);
        //        testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
        //        testResults.totalNumberOfOutput = Int64ermediateHashTable.Count;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        public void Query_2_1_IM()
        {
            try
            {
                Stopwatch sw = new Stopwatch();

                sw.Start();
                #region Key Hashing Phase 

                var partHashTable = new Dictionary<Int64, string>();
                var supplierHashTable = new Dictionary<Int64, string>();
                var dateHashTable = new Dictionary<Int64, string>();

                Parallel.Invoke(parallelOptions,
                () =>
                {
                    foreach (var row in InMemoryData.dateDimension)
                    {
                        dateHashTable.Add(row.dDateKey, row.dYear);
                    }
                },
                () =>
                {
                    foreach (var row in InMemoryData.partDimension)
                    {
                        if (row.pCategory.Equals("MFGR#12"))
                            partHashTable.Add(row.pPartKey, row.pBrand);
                    }
                },
                () =>
                {
                    foreach (var row in InMemoryData.supplierDimension)
                    {
                        if (row.sRegion.Equals("AMERICA"))
                            supplierHashTable.Add(row.sSuppKey, row.sNation);
                    }
                });

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNJ] T0 Time: {0}", t0));
                sw.Reset();

                #endregion Key Hashing Phase

                var _maat = new MAATIM(InMemoryData.loSupplierKey.Count);
                var partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loSupplierKey.Count, parallelOptions.MaxDegreeOfParallelism);
                #region Probing Phase
                sw.Start();

                List<Task> tasks = new List<Task>();
                foreach (var indexes in partitionIndexes)
                {
                    Task t = Task.Factory.StartNew(() =>
                    {
                        for (Int32 i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            Int64 partKey = InMemoryData.loPartKey[i];
                            Int64 dateKey = InMemoryData.loOrderDate[i];
                            Int64 suppKey = InMemoryData.loSupplierKey[i];
                            string pBrand = string.Empty;
                            string dYear = string.Empty;
                            if (partHashTable.TryGetValue(partKey, out pBrand)
                            && dateHashTable.TryGetValue(dateKey, out dYear)
                             && supplierHashTable.ContainsKey(suppKey))
                            {
                                _maat.AddOrUpdate(i, new List<object> { dYear, pBrand, InMemoryData.loRevenue[i] });
                            }
                        }
                    });
                    tasks.Add(t);
                }

                Task.WaitAll(tasks.ToArray());

                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNJ] T1 Time: {0}", t1));
                sw.Reset();

                #endregion Probing Phase
                #region Value Extraction Phase
                sw.Start();

                var joinOutputFinal = new Dictionary<string, Int64>();
                Int64 index = 0;
                foreach (var item in _maat.GetAll())
                {
                    if (item != null)
                    {
                        string key = item[0] + ", " + item[1];
                        Int64 revenue = 0;
                        if (joinOutputFinal.TryGetValue(key, out revenue))
                        {
                            joinOutputFinal[key] = revenue + Convert.ToInt32(item[2]);
                        }
                        else
                        {
                            joinOutputFinal.Add(key, Convert.ToInt32(item[2]));
                        }
                    }
                    index++;
                }

                sw.Stop();
                long t2 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNJ] T2 Time: {0}", t2));
                Console.WriteLine(String.Format("[PNJ] Total Time: {0}", t0 + t1 + t2));
                Console.WriteLine(String.Format("[PNJ] Total Count: {0}", joinOutputFinal.Count()));
                Console.WriteLine();
                #endregion Value Extraction Phase
                testResults.phase1Time = t0;
                testResults.phase2Time = t1;
                testResults.phase3Time = t2;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void Query_2_2_IM()
        {
            try
            {
                Stopwatch sw = new Stopwatch();

                sw.Start();
                #region Key Hashing Phase 

                var partHashTable = new Dictionary<Int64, string>();
                var supplierHashTable = new Dictionary<Int64, string>();
                var dateHashTable = new Dictionary<Int64, string>();

                Parallel.Invoke(parallelOptions,
               () =>
               {
                   foreach (var row in InMemoryData.dateDimension)
                   {
                       dateHashTable.Add(row.dDateKey, row.dYear);
                   }
               },
               () =>
               {
                   foreach (var row in InMemoryData.partDimension)
                   {
                       if (String.CompareOrdinal(row.pBrand, "MFGR#2221") >= 0 && String.CompareOrdinal(row.pBrand, "MFGR#2228") <= 0)
                           partHashTable.Add(row.pPartKey, row.pBrand);
                   }
               },
               () =>
               {
                   foreach (var row in InMemoryData.supplierDimension)
                   {
                       if (row.sRegion.Equals("ASIA"))
                           supplierHashTable.Add(row.sSuppKey, row.sNation);
                   }
               });

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNJ] T0 Time: {0}", t0));
                sw.Reset();

                #endregion Key Hashing Phase

                var _maat = new MAATIM(InMemoryData.loSupplierKey.Count);
                var partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loSupplierKey.Count, parallelOptions.MaxDegreeOfParallelism);
                #region Probing Phase
                sw.Start();

                List<Task> tasks = new List<Task>();
                foreach (var indexes in partitionIndexes)
                {
                    Task t = Task.Factory.StartNew(() =>
                    {
                        for (Int32 i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            Int64 partKey = InMemoryData.loPartKey[i];
                            Int64 dateKey = InMemoryData.loOrderDate[i];
                            Int64 suppKey = InMemoryData.loSupplierKey[i];
                            string pBrand = string.Empty;
                            string dYear = string.Empty;
                            if (partHashTable.TryGetValue(partKey, out pBrand)
                            && dateHashTable.TryGetValue(dateKey, out dYear)
                             && supplierHashTable.ContainsKey(suppKey))
                            {
                                _maat.AddOrUpdate(i, new List<object> { dYear, pBrand, InMemoryData.loRevenue[i] });
                            }
                        }
                    });
                    tasks.Add(t);
                }

                Task.WaitAll(tasks.ToArray());

                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNJ] T1 Time: {0}", t1));
                sw.Reset();

                #endregion Probing Phase
                #region Value Extraction Phase
                sw.Start();

                var joinOutputFinal = new Dictionary<string, Int64>();
                Int64 index = 0;
                foreach (var item in _maat.GetAll())
                {
                    if (item != null)
                    {
                        string key = item[0] + ", " + item[1];
                        Int64 revenue = 0;
                        if (joinOutputFinal.TryGetValue(key, out revenue))
                        {
                            joinOutputFinal[key] = revenue + Convert.ToInt32(item[2]);
                        }
                        else
                        {
                            joinOutputFinal.Add(key, Convert.ToInt32(item[2]));
                        }
                    }
                    index++;
                }

                sw.Stop();
                long t2 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNJ] T2 Time: {0}", t2));
                Console.WriteLine(String.Format("[PNJ] Total Time: {0}", t0 + t1 + t2));
                Console.WriteLine(String.Format("[PNJ] Total Count: {0}", joinOutputFinal.Count()));
                Console.WriteLine();
                #endregion Value Extraction Phase
                testResults.phase1Time = t0;
                testResults.phase2Time = t1;
                testResults.phase3Time = t2;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void Query_2_3_IM()
        {
            try
            {
                Stopwatch sw = new Stopwatch();

                sw.Start();
                #region Key Hashing Phase 

                var partHashTable = new Dictionary<Int64, string>();
                var supplierHashTable = new Dictionary<Int64, string>();
                var dateHashTable = new Dictionary<Int64, string>();

                Parallel.Invoke(parallelOptions, () =>
                {
                    foreach (var row in InMemoryData.dateDimension)
                    {
                        dateHashTable.Add(row.dDateKey, row.dYear);
                    }
                },
                     () =>
                     {
                         foreach (var row in InMemoryData.partDimension)
                         {
                             if (row.pBrand.Equals("MFGR#2221"))
                                 partHashTable.Add(row.pPartKey, row.pBrand);
                         }
                     },
                     () =>
                     {
                         foreach (var row in InMemoryData.supplierDimension)
                         {
                             if (row.sRegion.Equals("EUROPE"))
                                 supplierHashTable.Add(row.sSuppKey, row.sNation);
                         }
                     });

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNJ] T0 Time: {0}", t0));
                sw.Reset();

                #endregion Key Hashing Phase

                var _maat = new MAATIM(InMemoryData.loSupplierKey.Count);
                var partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loSupplierKey.Count, parallelOptions.MaxDegreeOfParallelism);
                #region Probing Phase
                sw.Start();

                List<Task> tasks = new List<Task>();
                foreach (var indexes in partitionIndexes)
                {
                    Task t = Task.Factory.StartNew(() =>
                    {
                        for (Int32 i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            Int64 partKey = InMemoryData.loPartKey[i];
                            Int64 dateKey = InMemoryData.loOrderDate[i];
                            Int64 suppKey = InMemoryData.loSupplierKey[i];
                            string pBrand = string.Empty;
                            string dYear = string.Empty;
                            if (partHashTable.TryGetValue(partKey, out pBrand)
                            && dateHashTable.TryGetValue(dateKey, out dYear)
                             && supplierHashTable.ContainsKey(suppKey))
                            {
                                _maat.AddOrUpdate(i, new List<object> { dYear, pBrand, InMemoryData.loRevenue[i] });
                            }
                        }
                    });
                    tasks.Add(t);
                }

                Task.WaitAll(tasks.ToArray());

                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNJ] T1 Time: {0}", t1));
                sw.Reset();

                #endregion Probing Phase
                #region Value Extraction Phase
                sw.Start();

                var joinOutputFinal = new Dictionary<string, Int64>();
                Int64 index = 0;
                foreach (var item in _maat.GetAll())
                {
                    if (item != null)
                    {
                        string key = item[0] + ", " + item[1];
                        Int64 revenue = 0;
                        if (joinOutputFinal.TryGetValue(key, out revenue))
                        {
                            joinOutputFinal[key] = revenue + Convert.ToInt32(item[2]);
                        }
                        else
                        {
                            joinOutputFinal.Add(key, Convert.ToInt32(item[2]));
                        }
                    }
                    index++;
                }

                sw.Stop();
                long t2 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNJ] T2 Time: {0}", t2));
                Console.WriteLine(String.Format("[PNJ] Total Time: {0}", t0 + t1 + t2));
                Console.WriteLine(String.Format("[PNJ] Total Count: {0}", joinOutputFinal.Count()));
                Console.WriteLine();
                #endregion Value Extraction Phase
                testResults.phase1Time = t0;
                testResults.phase2Time = t1;
                testResults.phase3Time = t2;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
       
        public void Query_3_1_IM()
        {
            try
            {
                Stopwatch sw = new Stopwatch();

                sw.Start();
                #region Key Hashing Phase 

                var customerHashTable = new Dictionary<Int64, string>();
                var supplierHashTable = new Dictionary<Int64, string>();
                var dateHashTable = new Dictionary<Int64, string>();

                Parallel.Invoke(parallelOptions,
                () =>
                {
                    foreach (var row in InMemoryData.dateDimension)
                    {
                        if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
                            dateHashTable.Add(row.dDateKey, row.dYear);
                    }
                },
                () =>
                {
                    foreach (var row in InMemoryData.customerDimension)
                    {
                        if (row.cRegion.Equals("ASIA"))
                            customerHashTable.Add(row.cCustKey, row.cNation);
                    }
                },
                () =>
                {
                    foreach (var row in InMemoryData.supplierDimension)
                    {
                        if (row.sRegion.Equals("ASIA"))
                            supplierHashTable.Add(row.sSuppKey, row.sNation);
                    }
                });

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNJ] T0 Time: {0}", t0));
                sw.Reset();
                #endregion Key Hashing Phase

                var _maat = new MAATIM(InMemoryData.loSupplierKey.Count);
                var partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);
                #region Probing Phase
                sw.Start();

                List<Task> tasks = new List<Task>();
                foreach (var indexes in partitionIndexes)
                {
                    Task t = Task.Factory.StartNew(() =>
                    {
                        for (Int32 i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            Int64 custKey = InMemoryData.loCustomerKey[i];
                            Int64 suppKey = InMemoryData.loSupplierKey[i];
                            Int64 dateKey = InMemoryData.loOrderDate[i];
                            string custNation = string.Empty;
                            string suppNation = string.Empty;
                            string dYear = string.Empty;
                            if (customerHashTable.TryGetValue(custKey, out custNation)
                            && supplierHashTable.TryGetValue(suppKey, out suppNation)
                            && dateHashTable.TryGetValue(dateKey, out dYear))
                            {
                                _maat.AddOrUpdate(i, new List<object> { custNation, suppNation, dYear, InMemoryData.loRevenue[i] });
                            }
                        }
                    });
                    tasks.Add(t);
                }

                Task.WaitAll(tasks.ToArray());

                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNJ] T1 Time: {0}", t1));
                sw.Reset();

                #endregion Probing Phase

                #region Value Extraction Phase
                sw.Start();

                var joinOutputFinal = new Dictionary<string, Int64>();
                Int64 index = 0;
                foreach (var item in _maat.GetAll())
                {
                    if (item != null)
                    {
                        string key = item[0] + ", " + item[1] + ", " + item[2];
                        Int64 revenue = 0;
                        if (joinOutputFinal.TryGetValue(key, out revenue))
                        {
                            joinOutputFinal[key] = revenue + Convert.ToInt32(item[3]);
                        }
                        else
                        {
                            joinOutputFinal.Add(key, Convert.ToInt32(item[3]));
                        }
                    }
                    index++;
                }

                sw.Stop();
                long t2 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNJ] T2 Time: {0}", t2));
                Console.WriteLine(String.Format("[PNJ] Total Time: {0}", t0 + t1 + t2));
                Console.WriteLine(String.Format("[PNJ] Total Count: {0}", joinOutputFinal.Count()));
                Console.WriteLine();
                #endregion Value Extraction Phase
                testResults.phase1Time = t0;
                testResults.phase2Time = t1;
                testResults.phase3Time = t2;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void Query_3_2_IM()
        {
            try
            {
                Stopwatch sw = new Stopwatch();

                sw.Start();
                #region Key Hashing Phase 

                var customerHashTable = new Dictionary<Int64, string>();
                var supplierHashTable = new Dictionary<Int64, string>();
                var dateHashTable = new Dictionary<Int64, string>();

                Parallel.Invoke(parallelOptions,
                () =>
                {
                    foreach (var row in InMemoryData.dateDimension)
                    {
                        if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
                            dateHashTable.Add(row.dDateKey, row.dYear);
                    }
                },
                () =>
                {
                    foreach (var row in InMemoryData.customerDimension)
                    {
                        if (row.cNation.Equals("UNITED STATES"))
                            customerHashTable.Add(row.cCustKey, row.cCity);
                    }
                },
                () =>
                {
                    foreach (var row in InMemoryData.supplierDimension)
                    {
                        if (row.sNation.Equals("UNITED STATES"))
                            supplierHashTable.Add(row.sSuppKey, row.sCity);
                    }
                });

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNJ] T0 Time: {0}", t0));
                sw.Reset();
                #endregion Key Hashing Phase

                var _maat = new MAATIM(InMemoryData.loSupplierKey.Count);
                var partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);
                #region Probing Phase
                sw.Start();

                List<Task> tasks = new List<Task>();
                foreach (var indexes in partitionIndexes)
                {
                    Task t = Task.Factory.StartNew(() =>
                    {
                        for (Int32 i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            Int64 custKey = InMemoryData.loCustomerKey[i];
                            Int64 suppKey = InMemoryData.loSupplierKey[i];
                            Int64 dateKey = InMemoryData.loOrderDate[i];
                            string custCity = string.Empty;
                            string suppCity = string.Empty;
                            string dYear = string.Empty;
                            if (customerHashTable.TryGetValue(custKey, out custCity)
                            && supplierHashTable.TryGetValue(suppKey, out suppCity)
                            && dateHashTable.TryGetValue(dateKey, out dYear))
                            {
                                _maat.AddOrUpdate(i, new List<object> { custCity, suppCity, dYear, InMemoryData.loRevenue[i] });
                            }
                        }
                    });
                    tasks.Add(t);
                }

                Task.WaitAll(tasks.ToArray());

                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNJ] T1 Time: {0}", t1));
                sw.Reset();

                #endregion Probing Phase

                #region Value Extraction Phase
                sw.Start();

                var joinOutputFinal = new Dictionary<string, Int64>();
                Int64 index = 0;
                foreach (var item in _maat.GetAll())
                {
                    if (item != null)
                    {
                        string key = item[0] + ", " + item[1] + ", " + item[2];
                        Int64 revenue = 0;
                        if (joinOutputFinal.TryGetValue(key, out revenue))
                        {
                            joinOutputFinal[key] = revenue + Convert.ToInt32(item[3]);
                        }
                        else
                        {
                            joinOutputFinal.Add(key, Convert.ToInt32(item[3]));
                        }
                    }
                    index++;
                }

                sw.Stop();
                long t2 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNJ] T2 Time: {0}", t2));
                Console.WriteLine(String.Format("[PNJ] Total Time: {0}", t0 + t1 + t2));
                Console.WriteLine(String.Format("[PNJ] Total Count: {0}", joinOutputFinal.Count()));
                Console.WriteLine();
                #endregion Value Extraction Phase
                testResults.phase1Time = t0;
                testResults.phase2Time = t1;
                testResults.phase3Time = t2;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void Query_3_3_IM()
        {
            try
            {
                Stopwatch sw = new Stopwatch();

                sw.Start();
                #region Key Hashing Phase 

                var customerHashTable = new Dictionary<Int64, string>();
                var supplierHashTable = new Dictionary<Int64, string>();
                var dateHashTable = new Dictionary<Int64, string>();

                Parallel.Invoke(parallelOptions, () =>
                {
                    foreach (var row in InMemoryData.dateDimension)
                    {
                        if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
                            dateHashTable.Add(row.dDateKey, row.dYear);
                    }
                },
                () =>
                {
                    foreach (var row in InMemoryData.customerDimension)
                    {
                        if (row.cCity.Equals("UNITED KI1") || row.cCity.Equals("UNITED KI5"))
                            customerHashTable.Add(row.cCustKey, row.cCity);
                    }
                },
                () =>
                {
                    foreach (var row in InMemoryData.supplierDimension)
                    {
                        if (row.sCity.Equals("UNITED KI1") || row.sCity.Equals("UNITED KI5"))
                            supplierHashTable.Add(row.sSuppKey, row.sCity);
                    }
                });

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNJ] T0 Time: {0}", t0));
                sw.Reset();
                #endregion Key Hashing Phase

                var _maat = new MAATIM(InMemoryData.loSupplierKey.Count);
                var partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);
                #region Probing Phase
                sw.Start();

                List<Task> tasks = new List<Task>();
                foreach (var indexes in partitionIndexes)
                {
                    Task t = Task.Factory.StartNew(() =>
                    {
                        for (Int32 i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            Int64 custKey = InMemoryData.loCustomerKey[i];
                            Int64 suppKey = InMemoryData.loSupplierKey[i];
                            Int64 dateKey = InMemoryData.loOrderDate[i];
                            string custCity = string.Empty;
                            string suppCity = string.Empty;
                            string dYear = string.Empty;
                            if (customerHashTable.TryGetValue(custKey, out custCity)
                            && supplierHashTable.TryGetValue(suppKey, out suppCity)
                            && dateHashTable.TryGetValue(dateKey, out dYear))
                            {
                                _maat.AddOrUpdate(i, new List<object> { custCity, suppCity, dYear, InMemoryData.loRevenue[i] });
                            }
                        }
                    });
                    tasks.Add(t);
                }

                Task.WaitAll(tasks.ToArray());

                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNJ] T1 Time: {0}", t1));
                sw.Reset();

                #endregion Probing Phase

                #region Value Extraction Phase
                sw.Start();

                var joinOutputFinal = new Dictionary<string, Int64>();
                Int64 index = 0;
                foreach (var item in _maat.GetAll())
                {
                    if (item != null)
                    {
                        string key = item[0] + ", " + item[1] + ", " + item[2];
                        Int64 revenue = 0;
                        if (joinOutputFinal.TryGetValue(key, out revenue))
                        {
                            joinOutputFinal[key] = revenue + Convert.ToInt32(item[3]);
                        }
                        else
                        {
                            joinOutputFinal.Add(key, Convert.ToInt32(item[3]));
                        }
                    }
                    index++;
                }

                sw.Stop();
                long t2 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNJ] T2 Time: {0}", t2));
                Console.WriteLine(String.Format("[PNJ] Total Time: {0}", t0 + t1 + t2));
                Console.WriteLine(String.Format("[PNJ] Total Count: {0}", joinOutputFinal.Count()));
                Console.WriteLine();
                #endregion Value Extraction Phase
                testResults.phase1Time = t0;
                testResults.phase2Time = t1;
                testResults.phase3Time = t2;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void Query_3_4_IM()
        {
            try
            {
                Stopwatch sw = new Stopwatch();

                
                sw.Start();
                #region Key Hashing Phase 

                var customerHashTable = new Dictionary<Int64, string>();
                var supplierHashTable = new Dictionary<Int64, string>();
                var dateHashTable = new Dictionary<Int64, string>();

                Parallel.Invoke(parallelOptions, () =>
                {
                    foreach (var row in InMemoryData.dateDimension)
                    {
                        if (row.dYearMonth.Equals("Dec1997"))
                            dateHashTable.Add(row.dDateKey, row.dYear);
                    }
                },
                () =>
                {
                    foreach (var row in InMemoryData.customerDimension)
                    {
                        if (row.cCity.Equals("UNITED KI1") || row.cCity.Equals("UNITED KI5"))
                            customerHashTable.Add(row.cCustKey, row.cCity);
                    }
                },
                () =>
                {
                    foreach (var row in InMemoryData.supplierDimension)
                    {
                        if (row.sCity.Equals("UNITED KI1") || row.sCity.Equals("UNITED KI5"))
                            supplierHashTable.Add(row.sSuppKey, row.sCity);
                    }
                });

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNJ] T0 Time: {0}", t0));
                sw.Reset();
                #endregion Key Hashing Phase

                var _maat = new MAATIM(InMemoryData.loSupplierKey.Count);
                var partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);
                #region Probing Phase
                sw.Start();

                List<Task> tasks = new List<Task>();
                foreach (var indexes in partitionIndexes)
                {
                    Task t = Task.Factory.StartNew(() =>
                    {
                        for (Int32 i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            Int64 custKey = InMemoryData.loCustomerKey[i];
                            Int64 suppKey = InMemoryData.loSupplierKey[i];
                            Int64 dateKey = InMemoryData.loOrderDate[i];
                            string custCity = string.Empty;
                            string suppCity = string.Empty;
                            string dYear = string.Empty;
                            if (customerHashTable.TryGetValue(custKey, out custCity)
                            && supplierHashTable.TryGetValue(suppKey, out suppCity)
                            && dateHashTable.TryGetValue(dateKey, out dYear))
                            {
                                _maat.AddOrUpdate(i, new List<object> { custCity, suppCity, dYear, InMemoryData.loRevenue[i] });
                            }
                        }
                    });
                    tasks.Add(t);
                }

                Task.WaitAll(tasks.ToArray());

                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNJ] T1 Time: {0}", t1));
                sw.Reset();

                #endregion Probing Phase

                #region Value Extraction Phase
                sw.Start();

                var joinOutputFinal = new Dictionary<string, Int64>();
                Int64 index = 0;
                foreach (var item in _maat.GetAll())
                {
                    if (item != null)
                    {
                        string key = item[0] + ", " + item[1] + ", " + item[2];
                        Int64 revenue = 0;
                        if (joinOutputFinal.TryGetValue(key, out revenue))
                        {
                            joinOutputFinal[key] = revenue + Convert.ToInt32(item[3]);
                        }
                        else
                        {
                            joinOutputFinal.Add(key, Convert.ToInt32(item[3]));
                        }
                    }
                    index++;
                }

                sw.Stop();
                long t2 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNJ] T2 Time: {0}", t2));
                Console.WriteLine(String.Format("[PNJ] Total Time: {0}", t0 + t1 + t2));
                Console.WriteLine(String.Format("[PNJ] Total Count: {0}", joinOutputFinal.Count()));
                Console.WriteLine();
                #endregion Value Extraction Phase
                testResults.phase1Time = t0;
                testResults.phase2Time = t1;
                testResults.phase3Time = t2;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void Query_4_1_IM()
        {
            try
            {
                Stopwatch sw = new Stopwatch();

                sw.Start();
                #region Key Hashing Phase 

                var customerHashTable = new Dictionary<Int64, string>();
                var supplierHashTable = new Dictionary<Int64, string>();
                var dateHashTable = new Dictionary<Int64, string>();
                var partHashTable = new Dictionary<Int64, string>();

                Parallel.Invoke(parallelOptions,
                () =>
                {
                    foreach (var row in InMemoryData.dateDimension)
                    {
                        dateHashTable.Add(row.dDateKey, row.dYear);
                    }
                },
                () =>
                {
                    foreach (var row in InMemoryData.customerDimension)
                    {
                        if (row.cRegion.Equals("AMERICA"))
                            customerHashTable.Add(row.cCustKey, row.cNation);
                    }
                },
                () =>
                {
                    foreach (var row in InMemoryData.supplierDimension)
                    {
                        if (row.sRegion.Equals("AMERICA"))
                            supplierHashTable.Add(row.sSuppKey, row.sNation);
                    }
                },
                () =>
                {
                    foreach (var row in InMemoryData.partDimension)
                    {
                        if (row.pMFGR.Equals("MFGR#1") || row.pMFGR.Equals("MFGR#2"))
                        {
                            partHashTable.Add(row.pPartKey, row.pMFGR);
                        }
                    }
                });

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNJ] T0 Time: {0}", t0));
                sw.Reset();
                #endregion Key Hashing Phase

                var _maat = new MAATIM(InMemoryData.loSupplierKey.Count);
                var partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);
                #region Probing Phase
                sw.Start();

                List<Task> tasks = new List<Task>();
                foreach (var indexes in partitionIndexes)
                {
                    Task t = Task.Factory.StartNew(() =>
                    {
                        for (Int32 i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            Int64 custKey = InMemoryData.loCustomerKey[i];
                            Int64 dateKey = InMemoryData.loOrderDate[i];
                            Int64 suppKey = InMemoryData.loSupplierKey[i];
                            Int64 partKey = InMemoryData.loPartKey[i];
                            string custNation = string.Empty;
                            string dYear = string.Empty;
                            if (customerHashTable.TryGetValue(custKey, out custNation)
                            && dateHashTable.TryGetValue(dateKey, out dYear)
                            && supplierHashTable.ContainsKey(suppKey)
                            && partHashTable.ContainsKey(partKey))
                            {
                                _maat.AddOrUpdate(i, new List<object> { dYear, custNation, (InMemoryData.loRevenue[i] - InMemoryData.loSupplyCost[i]) });
                            }
                        }
                    });
                    tasks.Add(t);
                }

                Task.WaitAll(tasks.ToArray());

                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNJ] T1 Time: {0}", t1));
                sw.Reset();

                #endregion Probing Phase

                #region Value Extraction Phase
                sw.Start();

                var joinOutputFinal = new Dictionary<string, Int64>();
                Int64 index = 0;
                foreach (var item in _maat.GetAll())
                {
                    if (item != null)
                    {
                        string key = item[0] + ", " + item[1];
                        Int64 revenue = 0;
                        if (joinOutputFinal.TryGetValue(key, out revenue))
                        {
                            joinOutputFinal[key] = revenue + Convert.ToInt32(item[2]);
                        }
                        else
                        {
                            joinOutputFinal.Add(key, Convert.ToInt32(item[2]));
                        }
                    }
                    index++;
                }

                sw.Stop();
                long t2 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNJ] T2 Time: {0}", t2));
                Console.WriteLine(String.Format("[PNJ] Total Time: {0}", t0 + t1 + t2));
                Console.WriteLine(String.Format("[PNJ] Total Count: {0}", joinOutputFinal.Count()));
                Console.WriteLine();
                #endregion Value Extraction Phase
                testResults.phase1Time = t0;
                testResults.phase2Time = t1;
                testResults.phase3Time = t2;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void Query_4_2_IM()
        {
            try
            {
                Stopwatch sw = new Stopwatch();

                sw.Start();
                #region Key Hashing Phase 

                var customerHashTable = new Dictionary<Int64, string>();
                var supplierHashTable = new Dictionary<Int64, string>();
                var dateHashTable = new Dictionary<Int64, string>();
                var partHashTable = new Dictionary<Int64, string>();

                Parallel.Invoke(parallelOptions,
               () =>
               {
                   foreach (var row in InMemoryData.dateDimension)
                   {
                       if (row.dYear.Equals("1997") || row.dYear.Equals("1998"))
                       {
                           dateHashTable.Add(row.dDateKey, row.dYear);
                       }
                   }
               },
               () =>
               {
                   foreach (var row in InMemoryData.customerDimension)
                   {
                       if (row.cRegion.Equals("AMERICA"))
                           customerHashTable.Add(row.cCustKey, row.cNation);
                   }
               },
               () =>
               {
                   foreach (var row in InMemoryData.supplierDimension)
                   {
                       if (row.sRegion.Equals("AMERICA"))
                           supplierHashTable.Add(row.sSuppKey, row.sNation);
                   }
               },
               () =>
               {
                   foreach (var row in InMemoryData.partDimension)
                   {
                       if (row.pMFGR.Equals("MFGR#1") || row.pMFGR.Equals("MFGR#2"))
                       {
                           partHashTable.Add(row.pPartKey, row.pCategory);
                       }
                   }
               });

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNJ] T0 Time: {0}", t0));
                sw.Reset();
                #endregion Key Hashing Phase

                var _maat = new MAATIM(InMemoryData.loSupplierKey.Count);
                var partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);
                #region Probing Phase
                sw.Start();

                List<Task> tasks = new List<Task>();
                foreach (var indexes in partitionIndexes)
                {
                    Task t = Task.Factory.StartNew(() =>
                    {
                        for (Int32 i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            Int64 suppKey = InMemoryData.loSupplierKey[i];
                            Int64 dateKey = InMemoryData.loOrderDate[i];
                            Int64 partKey = InMemoryData.loPartKey[i];
                            Int64 custKey = InMemoryData.loCustomerKey[i];
                            string suppNation = string.Empty;
                            string dYear = string.Empty;
                            string pCategory = string.Empty;
                            if (supplierHashTable.TryGetValue(suppKey, out suppNation)
                            && partHashTable.TryGetValue(partKey, out pCategory)
                            && dateHashTable.TryGetValue(dateKey, out dYear)
                            && customerHashTable.ContainsKey(custKey))
                            {
                                _maat.AddOrUpdate(i, new List<object> { dYear, suppNation, pCategory, (InMemoryData.loRevenue[i] - InMemoryData.loSupplyCost[i]) });
                            }
                        }
                    });
                    tasks.Add(t);
                }

                Task.WaitAll(tasks.ToArray());

                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNJ] T1 Time: {0}", t1));
                sw.Reset();

                #endregion Probing Phase

                #region Value Extraction Phase
                sw.Start();

                var joinOutputFinal = new Dictionary<string, Int64>();
                Int64 index = 0;
                foreach (var item in _maat.GetAll())
                {
                    if (item != null)
                    {
                        string key = item[0] + ", " + item[1] + ", " + item[2];
                        Int64 revenue = 0;
                        if (joinOutputFinal.TryGetValue(key, out revenue))
                        {
                            joinOutputFinal[key] = revenue + Convert.ToInt32(item[3]);
                        }
                        else
                        {
                            joinOutputFinal.Add(key, Convert.ToInt32(item[3]));
                        }
                    }
                    index++;
                }

                sw.Stop();
                long t2 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNJ] T2 Time: {0}", t2));
                Console.WriteLine(String.Format("[PNJ] Total Time: {0}", t0 + t1 + t2));
                Console.WriteLine(String.Format("[PNJ] Total Count: {0}", joinOutputFinal.Count()));
                Console.WriteLine();
                #endregion Value Extraction Phase
                testResults.phase1Time = t0;
                testResults.phase2Time = t1;
                testResults.phase3Time = t2;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void Query_4_3_IM()
        {
            try
            {
                Stopwatch sw = new Stopwatch();
                               
                sw.Start();
                #region Key Hashing Phase 

                var customerHashTable = new Dictionary<Int64, string>();
                var supplierHashTable = new Dictionary<Int64, string>();
                var dateHashTable = new Dictionary<Int64, string>();
                var partHashTable = new Dictionary<Int64, string>();

                Parallel.Invoke(parallelOptions,
                () =>
                {
                    foreach (var row in InMemoryData.dateDimension)
                    {
                        if (row.dYear.Equals("1997") || row.dYear.Equals("1998"))
                            dateHashTable.Add(row.dDateKey, row.dYear);
                    }
                },
                () =>
                {
                    foreach (var row in InMemoryData.customerDimension)
                    {
                        if (row.cRegion.Equals("AMERICA"))
                            customerHashTable.Add(row.cCustKey, row.cNation);
                    }
                },
                () =>
                {
                    foreach (var row in InMemoryData.supplierDimension)
                    {
                        if (row.sNation.Equals("UNITED STATES"))
                            supplierHashTable.Add(row.sSuppKey, row.sCity);
                    }
                },
                () =>
                {
                    foreach (var row in InMemoryData.partDimension)
                    {
                        if (row.pCategory.Equals("MFGR#14"))
                            partHashTable.Add(row.pPartKey, row.pBrand);
                    }
                });

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNJ] T0 Time: {0}", t0));
                sw.Reset();
                #endregion Key Hashing Phase

                var _maat = new MAATIM(InMemoryData.loSupplierKey.Count);
                var partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);
                #region Probing Phase
                sw.Start();

                List<Task> tasks = new List<Task>();
                foreach (var indexes in partitionIndexes)
                {
                    Task t = Task.Factory.StartNew(() =>
                    {
                        for (Int32 i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            Int64 suppKey = InMemoryData.loSupplierKey[i];
                            Int64 dateKey = InMemoryData.loOrderDate[i];
                            Int64 partKey = InMemoryData.loPartKey[i];
                            Int64 custKey = InMemoryData.loCustomerKey[i];
                            string suppCity = string.Empty;
                            string dYear = string.Empty;
                            string pBrand = string.Empty;
                            if (supplierHashTable.TryGetValue(suppKey, out suppCity)
                            && partHashTable.TryGetValue(partKey, out pBrand)
                            && dateHashTable.TryGetValue(dateKey, out dYear)
                            && customerHashTable.ContainsKey(custKey))
                            {
                                _maat.AddOrUpdate(i, new List<object> { dYear, suppCity, pBrand, (InMemoryData.loRevenue[i] - InMemoryData.loSupplyCost[i]) });
                            }
                        }
                    });
                    tasks.Add(t);
                }

                Task.WaitAll(tasks.ToArray());

                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNJ] T1 Time: {0}", t1));
                sw.Reset();

                #endregion Probing Phase

                #region Value Extraction Phase
                sw.Start();

                var joinOutputFinal = new Dictionary<string, Int64>();
                Int64 index = 0;
                foreach (var item in _maat.GetAll())
                {
                    if (item != null)
                    {
                        string key = item[0] + ", " + item[1] + ", " + item[2];
                        Int64 revenue = 0;
                        if (joinOutputFinal.TryGetValue(key, out revenue))
                        {
                            joinOutputFinal[key] = revenue + Convert.ToInt32(item[3]);
                        }
                        else
                        {
                            joinOutputFinal.Add(key, Convert.ToInt32(item[3]));
                        }
                    }
                    index++;
                }

                sw.Stop();
                long t2 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNJ] T2 Time: {0}", t2));
                Console.WriteLine(String.Format("[PNJ] Total Time: {0}", t0 + t1 + t2));
                Console.WriteLine(String.Format("[PNJ] Total Count: {0}", joinOutputFinal.Count()));
                Console.WriteLine();
                #endregion Value Extraction Phase
                testResults.phase1Time = t0;
                testResults.phase2Time = t1;
                testResults.phase3Time = t2;
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
                long memoryStartPhase3 = GC.GetTotalMemory(true);
                sw.Start();
                #region Key Hashing Phase 

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
                Console.WriteLine(String.Format("[PNJ] GSTest T0 Time: {0}", t0));
                #endregion Key Hashing Phase
                var _maat = new MAATIM(InMemoryData.loCustomerKey.Count);
                List<Tuple<int, int>> partitionIndexes = Utils.getPartitionIndexes(InMemoryData.loCustomerKey.Count(), parallelOptions.MaxDegreeOfParallelism);

                #region Probing Phase
                sw.Reset();
                sw.Start();

                List<Task> tasks = new List<Task>();
                foreach (var indexes in partitionIndexes)
                {
                    Task t = Task.Factory.StartNew(() =>
                    {
                        for (Int32 i = indexes.Item1; i <= indexes.Item2; i++)
                        {
                            Int64 suppKey = InMemoryData.loSupplierKey[i];
                            Int64 dateKey = InMemoryData.loOrderDate[i];
                            Int64 partKey = InMemoryData.loPartKey[i];
                            Int64 custKey = InMemoryData.loCustomerKey[i];
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
                                        _maat.AddOrUpdate(i, new List<object> { dOut.Item1, (InMemoryData.loRevenue[i] - InMemoryData.loSupplyCost[i]) });
                                       
                                        break;
                                    case 2:
                                        _maat.AddOrUpdate(i, new List<object> { dOut.Item1, dOut.Item2, (InMemoryData.loRevenue[i] - InMemoryData.loSupplyCost[i]) });
                                        break;
                                    case 3:
                                        _maat.AddOrUpdate(i, new List<object> { dOut.Item1, dOut.Item2, sOut.Item1,(InMemoryData.loRevenue[i] - InMemoryData.loSupplyCost[i]) });
                                        break;
                                    case 4:
                                        _maat.AddOrUpdate(i, new List<object> { dOut.Item1, dOut.Item2, sOut.Item1, sOut.Item2, (InMemoryData.loRevenue[i] - InMemoryData.loSupplyCost[i]) });
                                        break;
                                    case 5:
                                        _maat.AddOrUpdate(i, new List<object> { dOut.Item1, dOut.Item2, sOut.Item1, sOut.Item2, sOut.Item3, (InMemoryData.loRevenue[i] - InMemoryData.loSupplyCost[i]) });
                                        break;
                                    case 6:
                                        _maat.AddOrUpdate(i, new List<object> { dOut.Item1, dOut.Item2, sOut.Item1, sOut.Item2, sOut.Item3, cOut.Item1,(InMemoryData.loRevenue[i] - InMemoryData.loSupplyCost[i]) });
                                        break;
                                    case 7:
                                        _maat.AddOrUpdate(i, new List<object> { dOut.Item1, dOut.Item2, sOut.Item1, sOut.Item2, sOut.Item3, cOut.Item1, cOut.Item2, (InMemoryData.loRevenue[i] - InMemoryData.loSupplyCost[i]) });
                                        break;
                                    case 8:
                                        _maat.AddOrUpdate(i, new List<object> { dOut.Item1, dOut.Item2, sOut.Item1, sOut.Item2, sOut.Item3, cOut.Item1, cOut.Item2, cOut.Item3, (InMemoryData.loRevenue[i] - InMemoryData.loSupplyCost[i]) });
                                        break;
                                    case 9:
                                        _maat.AddOrUpdate(i, new List<object> { dOut.Item1, dOut.Item2, sOut.Item1, sOut.Item2, sOut.Item3, cOut.Item1, cOut.Item2, cOut.Item3, pOut.Item1, (InMemoryData.loRevenue[i] - InMemoryData.loSupplyCost[i]) });
                                        break;
                                    case 10:
                                        _maat.AddOrUpdate(i, new List<object> { dOut.Item1, dOut.Item2, sOut.Item1, sOut.Item2, sOut.Item3, cOut.Item1, cOut.Item2, cOut.Item3, pOut.Item1, pOut.Item2, (InMemoryData.loRevenue[i] - InMemoryData.loSupplyCost[i]) });
                                        break;
                                }
                            }
                        }
                    });
                    tasks.Add(t);
                }

                Task.WaitAll(tasks.ToArray());

                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PNJ] GSTest T1 Time: {0}", t1));
                sw.Reset();
                #endregion Probing Phase

                #region Value Extraction Phase
                sw.Start();
                var joinOutputFinal = new Dictionary<string, long>();
                Tuple<string, string, string> custGA = null;
                Tuple<string, string, string> suppGA = null;
                Tuple<string, string> dateGA = null;
                Tuple<string, string> partGA = null;
                Int32 index = 0;
                foreach (var item in _maat.GetAll())
                {
                    try
                    {
                        if (item != null)
                        {
                            string key = string.Empty;
                            Int64 revenue = 0;
                            switch (numberOfGroupingAttributes)
                            {
                                case 1:
                                    key = Convert.ToString(item[0]);
                                    revenue = Convert.ToInt64(item[1]);
                                    break;
                                case 2:
                                    key = Convert.ToString(item[0]) + ", " + Convert.ToString(item[1]);
                                    revenue = Convert.ToInt64(item[2]);
                                    break;
                                case 3:
                                    key = Convert.ToString(item[0]) + ", " + Convert.ToString(item[1]) + ", " + Convert.ToString(item[2]);
                                    revenue = Convert.ToInt64(item[3]);
                                    break;
                                case 4:
                                    key = Convert.ToString(item[0]) + ", " + Convert.ToString(item[1]) + ", " + Convert.ToString(item[2]) + ", " + Convert.ToString(item[3]);
                                    revenue = Convert.ToInt64(item[4]);
                                    break;
                                case 5:
                                    key = Convert.ToString(item[0]) + ", " + Convert.ToString(item[1]) + ", " + Convert.ToString(item[2]) + ", " + Convert.ToString(item[3]) + ", " + Convert.ToString(item[4]);
                                    revenue = Convert.ToInt64(item[5]);
                                    break;
                                case 6:
                                    key = Convert.ToString(item[0]) + ", " + Convert.ToString(item[1]) + ", " + Convert.ToString(item[2]) + ", " + Convert.ToString(item[3]) + ", " + Convert.ToString(item[4]) + ", " + Convert.ToString(item[5]);
                                    revenue = Convert.ToInt64(item[6]);
                                    break;
                                case 7:
                                    key = Convert.ToString(item[0]) + ", " + Convert.ToString(item[1]) + ", " + Convert.ToString(item[2]) + ", " + Convert.ToString(item[3]) + ", " + Convert.ToString(item[4]) + ", " + Convert.ToString(item[5]) + ", " + Convert.ToString(item[6]);
                                    revenue = Convert.ToInt64(item[7]);
                                    break;
                                case 8:
                                    key = Convert.ToString(item[0]) + ", " + Convert.ToString(item[1]) + ", " + Convert.ToString(item[2]) + ", " + Convert.ToString(item[3]) + ", " + Convert.ToString(item[4]) + ", " + Convert.ToString(item[5]) + ", " + Convert.ToString(item[6]) + ", " + Convert.ToString(item[7]);
                                    revenue = Convert.ToInt64(item[8]);
                                    break;
                                case 9:
                                    key = Convert.ToString(item[0]) + ", " + Convert.ToString(item[1]) + ", " + Convert.ToString(item[2]) + ", " + Convert.ToString(item[3]) + ", " + Convert.ToString(item[4]) + ", " + Convert.ToString(item[5]) + ", " + Convert.ToString(item[6]) + ", " + Convert.ToString(item[7]) + ", " + Convert.ToString(item[8]);
                                    revenue = Convert.ToInt64(item[9]);
                                    break;
                                case 10:
                                    key = Convert.ToString(item[0]) + ", " + Convert.ToString(item[1]) + ", " + Convert.ToString(item[2]) + ", " + Convert.ToString(item[3]) + ", " + Convert.ToString(item[4]) + ", " + Convert.ToString(item[5]) + ", " + Convert.ToString(item[6]) + ", " + Convert.ToString(item[7]) + ", " + Convert.ToString(item[8]) + ", " + Convert.ToString(item[9]);
                                    revenue = Convert.ToInt64(item[10]);
                                    break;
                            }

                            Int64 rOut = 0;
                            if (joinOutputFinal.TryGetValue(key, out rOut))
                            {
                                joinOutputFinal[key] = revenue + rOut;
                            }
                            else
                            {
                                joinOutputFinal.Add(key, revenue);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(index);
                        throw;
                    }
                    index++;
                }

                sw.Stop();
                long t2 = sw.ElapsedMilliseconds;
                long memoryUsed = GC.GetTotalMemory(true) - memoryStartPhase3;
                Console.WriteLine(String.Format("[PNJ] GSTest T2 Time: {0}", t2));
                Console.WriteLine(String.Format("[PNJ] GSTest Total Time: {0}", t0 + t1 + t2));
                Console.WriteLine(String.Format("[PNJ] GSTest Memory Used : {0}", memoryUsed));
                Console.WriteLine();
                #endregion Value Extraction Phase
                testResults.phase1Time = t0;
                testResults.phase2Time = t1;
                testResults.phase3Time = t2;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        private void saveAndPrintResults()
        {
            //TestResultsDatabase.pNimbleJoinOutput.Add(testResults.toString());
            //Console.WriteLine("Parallel Nimble: " + testResults.toString());
            //Console.WriteLine();
            TestResultsDatabase.pNimbleJoinOutput.Add(testResults.toString());
        }
    }
}
