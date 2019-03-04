using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParallelHashJoins
{
    class ParallelInvisibleJoin
    {
        private static string binaryFilesDirectory = @"C:\Raw_Data_Source_For_Test\SSBM - DBGEN\BF";
        private string scaleFactor { get; set; }
        private ParallelOptions parallelOptions = null;
        private TestResults testResults = new TestResults();

        public ParallelInvisibleJoin(string _scaleFactor, ParallelOptions _parallelOptions)
        {
            scaleFactor = _scaleFactor;
            parallelOptions = _parallelOptions;
        }

        ~ParallelInvisibleJoin()
        {
            saveAndPrintResults();
        }

        //#region old Queries
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
        //        List<Int64> loOrderDate = null;
        //        List<Int64> loDiscount = null;
        //        List<Int64> loQuantity = null;
        //        Parallel.Invoke(parallelOptions, () => loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loDiscount = Utils.ReadFromBinaryFiles<Int64>(loDiscountFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loQuantity = Utils.ReadFromBinaryFiles<Int64>(loQuantityFile.Replace("BF", "BF" + scaleFactor)));

        //        sw.Stop();
        //        testResults.phase21IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        var listOrderDatePositions = new List<Int64>();
        //        var listLineOrderDiscountPositions = new List<Int64>();
        //        var listLineOrderQuantityPositions = new List<Int64>();

        //        Parallel.Invoke(parallelOptions, () =>
        //        {
        //            var i = 0;
        //            foreach (var orderDate in loOrderDate)
        //            {
        //                string dYear = "";
        //                if (dateHashTable.TryGetValue(orderDate, out dYear))
        //                {
        //                    listOrderDatePositions.Add(i);
        //                }
        //                i++;
        //            }
        //        },
        //        () =>
        //        {
        //            var j = 0;
        //            foreach (var _loDiscount in loDiscount)
        //            {
        //                if (_loDiscount >= 1 && _loDiscount <= 3)
        //                {
        //                    listLineOrderDiscountPositions.Add(j);
        //                }
        //                j++;
        //            }
        //        },
        //        () =>
        //        {
        //            var k = 0;
        //            foreach (var _loQuantity in loQuantity)
        //            {
        //                string sNationOut = string.Empty;
        //                if (_loQuantity < 25)
        //                {
        //                    listLineOrderQuantityPositions.Add(k);
        //                }
        //                k++;
        //            }
        //        });
        //        var common = listLineOrderDiscountPositions.Intersect(listOrderDatePositions).Intersect(listLineOrderQuantityPositions).ToList();

        //        sw.Stop();
        //        testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
        //        testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime +
        //            testResults.phase22IOTime + testResults.phase22ProbeTime +
        //            testResults.phase23IOTime + testResults.phase23ProbeTime;
        //        sw.Reset();

        //        loOrderDate.Clear();
        //        dateHashTable.Clear();
        //        listLineOrderDiscountPositions.Clear();
        //        listLineOrderQuantityPositions.Clear();
        //        loQuantity.Clear();

        //        long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
        //        #endregion Probing Phase


        //        #region Value Extraction Phase
        //        long memoryStartPhase3 = GC.GetTotalMemory(true);
        //        sw.Start();
        //        loDiscount = Utils.ReadFromBinaryFiles<Int64>(loDiscountFile.Replace("BF", "BF" + scaleFactor));
        //        List<Int64> loExtendedPrice = Utils.ReadFromBinaryFiles<Int64>(loExtendedPriceFile.Replace("BF", "BF" + scaleFactor));
        //        sw.Stop();
        //        testResults.phase3IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        Int64 totalRevenue = 0;
        //        object lockObject = new object();
        //        Parallel.ForEach(common, (index) =>
        //        {
        //            try
        //            {
        //                lock (lockObject)
        //                {
        //                    var revenue = loDiscount[index] * loExtendedPrice[index];
        //                    totalRevenue += revenue;
        //                }
        //                if (isFirst)
        //                {
        //                    swInitialRecorder.Stop();
        //                    testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
        //                    isFirst = false;
        //                }

        //                outputRecordsCounter++;
        //                if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
        //                {
        //                    swOutputRecorder.Stop();
        //                    //testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
        //                    swOutputRecorder.Start();
        //                }
        //                // Console.WriteLine(l +", "+ dYear  + ", " + sNationOut + ", " + cNationOut);
        //            }
        //            catch (Exception)
        //            {
        //                throw;
        //            }
        //        });
        //        sw.Stop();
        //        long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
        //        #endregion Value Extraction Phase
        //        testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
        //        testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
        //        testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
        //        //Console.WriteLine("[Invisble Join]: Time taken {0} ms.", sw.ElapsedMilliseconds);
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
        //        List<Int64> loOrderDate = null;
        //        List<Int64> loDiscount = null;
        //        List<Int64> loQuantity = null;
        //        Parallel.Invoke(parallelOptions, () => loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loDiscount = Utils.ReadFromBinaryFiles<Int64>(loDiscountFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loQuantity = Utils.ReadFromBinaryFiles<Int64>(loQuantityFile.Replace("BF", "BF" + scaleFactor)));

        //        sw.Stop();
        //        testResults.phase21IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        var listOrderDatePositions = new List<Int64>();
        //        var listLineOrderDiscountPositions = new List<Int64>();
        //        var listLineOrderQuantityPositions = new List<Int64>();

        //        Parallel.Invoke(parallelOptions, () =>
        //        {
        //            var i = 0;
        //            foreach (var orderDate in loOrderDate)
        //            {
        //                string dYear = "";
        //                if (dateHashTable.TryGetValue(orderDate, out dYear))
        //                {
        //                    listOrderDatePositions.Add(i);
        //                }
        //                i++;
        //            }
        //        },
        //        () =>
        //        {
        //            var j = 0;
        //            foreach (var _loDiscount in loDiscount)
        //            {
        //                if (_loDiscount >= 4 && _loDiscount <= 6)
        //                {
        //                    listLineOrderDiscountPositions.Add(j);
        //                }
        //                j++;
        //            }
        //        },
        //        () =>
        //        {
        //            var k = 0;
        //            foreach (var _loQuantity in loQuantity)
        //            {
        //                string sNationOut = string.Empty;
        //                if (_loQuantity >= 26 && _loQuantity <= 35)
        //                {
        //                    listLineOrderQuantityPositions.Add(k);
        //                }
        //                k++;
        //            }
        //        });
        //        var common = listLineOrderDiscountPositions.Intersect(listOrderDatePositions).Intersect(listLineOrderQuantityPositions).ToList();

        //        sw.Stop();
        //        testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
        //        testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime +
        //            testResults.phase22IOTime + testResults.phase22ProbeTime +
        //            testResults.phase23IOTime + testResults.phase23ProbeTime;
        //        sw.Reset();

        //        loOrderDate.Clear();
        //        dateHashTable.Clear();
        //        listLineOrderDiscountPositions.Clear();
        //        listLineOrderQuantityPositions.Clear();
        //        loQuantity.Clear();

        //        long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
        //        #endregion Probing Phase


        //        #region Value Extraction Phase
        //        long memoryStartPhase3 = GC.GetTotalMemory(true);
        //        sw.Start();
        //        loDiscount = Utils.ReadFromBinaryFiles<Int64>(loDiscountFile.Replace("BF", "BF" + scaleFactor));
        //        List<Int64> loExtendedPrice = Utils.ReadFromBinaryFiles<Int64>(loExtendedPriceFile.Replace("BF", "BF" + scaleFactor));
        //        sw.Stop();
        //        testResults.phase3IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        Int64 totalRevenue = 0;
        //        object lockObject = new object();
        //        Parallel.ForEach(common, (index) =>
        //        {
        //            try
        //            {
        //                lock (lockObject)
        //                {
        //                    var revenue = loDiscount[index] * loExtendedPrice[index];
        //                    totalRevenue += revenue;
        //                }
        //                if (isFirst)
        //                {
        //                    swInitialRecorder.Stop();
        //                    testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
        //                    isFirst = false;
        //                }

        //                outputRecordsCounter++;
        //                if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
        //                {
        //                    swOutputRecorder.Stop();
        //                    //testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
        //                    swOutputRecorder.Start();
        //                }
        //                // Console.WriteLine(l +", "+ dYear  + ", " + sNationOut + ", " + cNationOut);
        //            }
        //            catch (Exception)
        //            {
        //                throw;
        //            }
        //        });
        //        sw.Stop();
        //        long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
        //        #endregion Value Extraction Phase
        //        testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
        //        testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
        //        testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
        //        //Console.WriteLine("[Invisble Join]: Time taken {0} ms.", sw.ElapsedMilliseconds);
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
        //        List<Int64> loOrderDate = null;
        //        List<Int64> loDiscount = null;
        //        List<Int64> loQuantity = null;
        //        Parallel.Invoke(parallelOptions, () => loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loDiscount = Utils.ReadFromBinaryFiles<Int64>(loDiscountFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loQuantity = Utils.ReadFromBinaryFiles<Int64>(loQuantityFile.Replace("BF", "BF" + scaleFactor)));

        //        sw.Stop();
        //        testResults.phase21IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        var listOrderDatePositions = new List<Int64>();
        //        var listLineOrderDiscountPositions = new List<Int64>();
        //        var listLineOrderQuantityPositions = new List<Int64>();

        //        Parallel.Invoke(parallelOptions, () =>
        //        {
        //            var i = 0;
        //            foreach (var orderDate in loOrderDate)
        //            {
        //                string dYear = "";
        //                if (dateHashTable.TryGetValue(orderDate, out dYear))
        //                {
        //                    listOrderDatePositions.Add(i);
        //                }
        //                i++;
        //            }
        //        },
        //        () =>
        //        {
        //            var j = 0;
        //            foreach (var _loDiscount in loDiscount)
        //            {
        //                if (_loDiscount >= 5 && _loDiscount <= 7)
        //                {
        //                    listLineOrderDiscountPositions.Add(j);
        //                }
        //                j++;
        //            }
        //        },
        //        () =>
        //        {
        //            var k = 0;
        //            foreach (var _loQuantity in loQuantity)
        //            {
        //                string sNationOut = string.Empty;
        //                if (_loQuantity >= 26 && _loQuantity <= 35)
        //                {
        //                    listLineOrderQuantityPositions.Add(k);
        //                }
        //                k++;
        //            }
        //        });
        //        var common = listLineOrderDiscountPositions.Intersect(listOrderDatePositions).Intersect(listLineOrderQuantityPositions).ToList();

        //        sw.Stop();
        //        testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
        //        testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime +
        //            testResults.phase22IOTime + testResults.phase22ProbeTime +
        //            testResults.phase23IOTime + testResults.phase23ProbeTime;
        //        sw.Reset();

        //        loOrderDate.Clear();
        //        dateHashTable.Clear();
        //        listLineOrderDiscountPositions.Clear();
        //        listLineOrderQuantityPositions.Clear();
        //        loQuantity.Clear();

        //        long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
        //        #endregion Probing Phase


        //        #region Value Extraction Phase
        //        long memoryStartPhase3 = GC.GetTotalMemory(true);
        //        sw.Start();
        //        loDiscount = Utils.ReadFromBinaryFiles<Int64>(loDiscountFile.Replace("BF", "BF" + scaleFactor));
        //        List<Int64> loExtendedPrice = Utils.ReadFromBinaryFiles<Int64>(loExtendedPriceFile.Replace("BF", "BF" + scaleFactor));
        //        sw.Stop();
        //        testResults.phase3IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        Int64 totalRevenue = 0;
        //        object lockObject = new object();
        //        Parallel.ForEach(common, (index) =>
        //        {
        //            try
        //            {
        //                lock (lockObject)
        //                {
        //                    var revenue = loDiscount[index] * loExtendedPrice[index];
        //                    totalRevenue += revenue;
        //                }
        //                if (isFirst)
        //                {
        //                    swInitialRecorder.Stop();
        //                    testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
        //                    isFirst = false;
        //                }

        //                outputRecordsCounter++;
        //                if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
        //                {
        //                    swOutputRecorder.Stop();
        //                    //testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
        //                    swOutputRecorder.Start();
        //                }
        //                // Console.WriteLine(l +", "+ dYear  + ", " + sNationOut + ", " + cNationOut);
        //            }
        //            catch (Exception)
        //            {
        //                throw;
        //            }
        //        });
        //        sw.Stop();
        //        long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
        //        #endregion Value Extraction Phase
        //        testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
        //        testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
        //        testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
        //        //Console.WriteLine("[Invisble Join]: Time taken {0} ms.", sw.ElapsedMilliseconds);
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
        //        var listOrderDatePositions = new List<Int64>();
        //        var listPartKeyPositions = new List<Int64>();
        //        var listSupplierKeyPositions = new List<Int64>();

        //        Parallel.Invoke(parallelOptions,
        //            () =>
        //            {
        //                var i = 0;
        //                foreach (var orderDate in loOrderDate)
        //                {
        //                    string dYear = "";
        //                    if (dateHashTable.TryGetValue(orderDate, out dYear))
        //                    {
        //                        listOrderDatePositions.Add(i);
        //                    }
        //                    i++;
        //                }
        //            },
        //            () =>
        //            {
        //                var j = 0;
        //                foreach (var partKey in loPartKey)
        //                {
        //                    string pBrandOut = string.Empty;
        //                    if (partHashTable.TryGetValue(partKey, out pBrandOut))
        //                    {
        //                        listPartKeyPositions.Add(j);
        //                    }
        //                    j++;
        //                }

        //            },
        //            () =>
        //            {
        //                var k = 0;
        //                foreach (var suppKey in loSupplierKey)
        //                {
        //                    string sNationOut = string.Empty;
        //                    if (supplierHashTable.TryGetValue(suppKey, out sNationOut))
        //                    {
        //                        listSupplierKeyPositions.Add(k);
        //                    }
        //                    k++;
        //                }
        //            });
        //        var common = listPartKeyPositions.Intersect(listOrderDatePositions).Intersect(listSupplierKeyPositions).ToList();

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
        //        List<string> pBrand = null;
        //        List<Int64> loRevenue = null;
        //        Parallel.Invoke(parallelOptions,
        //            () => loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loPartKey = Utils.ReadFromBinaryFiles<Int64>(loPartKeyFile.Replace("BF", "BF" + scaleFactor)),
        //            () =>
        //            {
        //                dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
        //                foreach (var row in dateDimension)
        //                {
        //                    dateHashTable.Add(row.dDateKey, row.dYear);
        //                }
        //                dateDimension.Clear();
        //            },
        //            () => pBrand = Utils.ReadFromBinaryFiles<string>(pBrandFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor)));

        //        sw.Stop();
        //        testResults.phase3IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        var joinOutputFinal = new Dictionary<Int64, string>();
        //        object lockObject = new object();
        //        Parallel.ForEach(common, (index) =>
        //         {
        //             try
        //             {
        //                 var dateKey = loOrderDate[index];
        //                 var partKey = loPartKey[index];
        //                 var revenue = loRevenue[index];

        //                 // Position Look UP
        //                 string dYear;
        //                 dateHashTable.TryGetValue(dateKey, out dYear);

        //                 string pBrandOut = pBrand[partKey];
        //                 if (isFirst)
        //                 {
        //                     swInitialRecorder.Stop();
        //                     testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
        //                     isFirst = false;
        //                 }

        //                 outputRecordsCounter++;
        //                 if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
        //                 {
        //                     swOutputRecorder.Stop();
        //                     //testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
        //                     swOutputRecorder.Start();
        //                 }
        //                 // Console.WriteLine(l +", "+ dYear  + ", " + sNationOut + ", " + cNationOut);
        //                 lock (lockObject)
        //                 {
        //                     joinOutputFinal.Add(index, revenue + "," + dYear + "," + pBrandOut);
        //                 }

        //             }
        //             catch (Exception)
        //             {
        //                 throw;
        //             }
        //         });

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
        //        var listOrderDatePositions = new List<Int64>();
        //        var listPartKeyPositions = new List<Int64>();
        //        var listSupplierKeyPositions = new List<Int64>();

        //        Parallel.Invoke(parallelOptions,
        //            () =>
        //            {
        //                var i = 0;
        //                foreach (var orderDate in loOrderDate)
        //                {
        //                    string dYear = "";
        //                    if (dateHashTable.TryGetValue(orderDate, out dYear))
        //                    {
        //                        listOrderDatePositions.Add(i);
        //                    }
        //                    i++;
        //                }
        //            },
        //            () =>
        //            {
        //                var j = 0;
        //                foreach (var partKey in loPartKey)
        //                {
        //                    string pBrandOut = string.Empty;
        //                    if (partHashTable.TryGetValue(partKey, out pBrandOut))
        //                    {
        //                        listPartKeyPositions.Add(j);
        //                    }
        //                    j++;
        //                }

        //            },
        //            () =>
        //            {
        //                var k = 0;
        //                foreach (var suppKey in loSupplierKey)
        //                {
        //                    string sNationOut = string.Empty;
        //                    if (supplierHashTable.TryGetValue(suppKey, out sNationOut))
        //                    {
        //                        listSupplierKeyPositions.Add(k);
        //                    }
        //                    k++;
        //                }
        //            });
        //        var common = listPartKeyPositions.Intersect(listOrderDatePositions).Intersect(listSupplierKeyPositions).ToList();

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
        //        List<string> pBrand = null;
        //        List<Int64> loRevenue = null;
        //        Parallel.Invoke(parallelOptions,
        //            () => loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loPartKey = Utils.ReadFromBinaryFiles<Int64>(loPartKeyFile.Replace("BF", "BF" + scaleFactor)),
        //            () =>
        //            {
        //                dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
        //                foreach (var row in dateDimension)
        //                {
        //                    dateHashTable.Add(row.dDateKey, row.dYear);
        //                }
        //                dateDimension.Clear();
        //            },
        //            () => pBrand = Utils.ReadFromBinaryFiles<string>(pBrandFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor)));

        //        sw.Stop();
        //        testResults.phase3IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        var joinOutputFinal = new Dictionary<Int64, string>();
        //        object lockObject = new object();
        //        Parallel.ForEach(common, (index) =>
        //        {
        //            try
        //            {
        //                var dateKey = loOrderDate[index];
        //                var partKey = loPartKey[index];
        //                var revenue = loRevenue[index];

        //                // Position Look UP
        //                string dYear;
        //                dateHashTable.TryGetValue(dateKey, out dYear);

        //                string pBrandOut = pBrand[partKey];
        //                if (isFirst)
        //                {
        //                    swInitialRecorder.Stop();
        //                    testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
        //                    isFirst = false;
        //                }

        //                outputRecordsCounter++;
        //                if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
        //                {
        //                    swOutputRecorder.Stop();
        //                    //testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
        //                    swOutputRecorder.Start();
        //                }
        //                // Console.WriteLine(l +", "+ dYear  + ", " + sNationOut + ", " + cNationOut);
        //                lock (lockObject)
        //                {
        //                    joinOutputFinal.Add(index, revenue + "," + dYear + "," + pBrandOut);
        //                }

        //            }
        //            catch (Exception)
        //            {
        //                throw;
        //            }
        //        });

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
        //        var listOrderDatePositions = new List<Int64>();
        //        var listPartKeyPositions = new List<Int64>();
        //        var listSupplierKeyPositions = new List<Int64>();

        //        Parallel.Invoke(parallelOptions,
        //            () =>
        //            {
        //                var i = 0;
        //                foreach (var orderDate in loOrderDate)
        //                {
        //                    string dYear = "";
        //                    if (dateHashTable.TryGetValue(orderDate, out dYear))
        //                    {
        //                        listOrderDatePositions.Add(i);
        //                    }
        //                    i++;
        //                }
        //            },
        //            () =>
        //            {
        //                var j = 0;
        //                foreach (var partKey in loPartKey)
        //                {
        //                    string pBrandOut = string.Empty;
        //                    if (partHashTable.TryGetValue(partKey, out pBrandOut))
        //                    {
        //                        listPartKeyPositions.Add(j);
        //                    }
        //                    j++;
        //                }

        //            },
        //            () =>
        //            {
        //                var k = 0;
        //                foreach (var suppKey in loSupplierKey)
        //                {
        //                    string sNationOut = string.Empty;
        //                    if (supplierHashTable.TryGetValue(suppKey, out sNationOut))
        //                    {
        //                        listSupplierKeyPositions.Add(k);
        //                    }
        //                    k++;
        //                }
        //            });
        //        var common = listPartKeyPositions.Intersect(listOrderDatePositions).Intersect(listSupplierKeyPositions).ToList();

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
        //        List<string> pBrand = null;
        //        List<Int64> loRevenue = null;
        //        Parallel.Invoke(parallelOptions,
        //            () => loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loPartKey = Utils.ReadFromBinaryFiles<Int64>(loPartKeyFile.Replace("BF", "BF" + scaleFactor)),
        //            () =>
        //            {
        //                dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
        //                foreach (var row in dateDimension)
        //                {
        //                    dateHashTable.Add(row.dDateKey, row.dYear);
        //                }
        //                dateDimension.Clear();
        //            },
        //            () => pBrand = Utils.ReadFromBinaryFiles<string>(pBrandFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor)));

        //        sw.Stop();
        //        testResults.phase3IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        var joinOutputFinal = new Dictionary<Int64, string>();
        //        object lockObject = new object();
        //        Parallel.ForEach(common, (index) =>
        //        {
        //            try
        //            {
        //                var dateKey = loOrderDate[index];
        //                var partKey = loPartKey[index];
        //                var revenue = loRevenue[index];

        //                // Position Look UP
        //                string dYear;
        //                dateHashTable.TryGetValue(dateKey, out dYear);

        //                string pBrandOut = pBrand[partKey];
        //                if (isFirst)
        //                {
        //                    swInitialRecorder.Stop();
        //                    testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
        //                    isFirst = false;
        //                }

        //                outputRecordsCounter++;
        //                if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
        //                {
        //                    swOutputRecorder.Stop();
        //                    //testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
        //                    swOutputRecorder.Start();
        //                }
        //                // Console.WriteLine(l +", "+ dYear  + ", " + sNationOut + ", " + cNationOut);
        //                lock (lockObject)
        //                {
        //                    joinOutputFinal.Add(index, revenue + "," + dYear + "," + pBrandOut);
        //                }

        //            }
        //            catch (Exception)
        //            {
        //                throw;
        //            }
        //        });

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
        //        var customerHashTable = new Dictionary<Int64, string>();
        //        var supplierHashTable = new Dictionary<Int64, string>();
        //        var partHashTable = new Dictionary<Int64, string>();
        //        Stopwatch sw = new Stopwatch();
        //        Stopwatch swInitialRecorder = new Stopwatch();
        //        Stopwatch swOutputRecorder = new Stopwatch();

        //        #region Key Hashing Phase
        //        List<Date> dateDimension = null;
        //        List<Supplier> supplierDimension = null;
        //        List<Customer> customerDimension = null;
        //        List<Part> partDimension = null;

        //        swInitialRecorder.Start();
        //        swOutputRecorder.Start();
        //        sw.Start();
        //        Parallel.Invoke(parallelOptions,
        //            () => dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor)),
        //            () => customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor)),
        //            () => supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor)),
        //            () => partDimension = Utils.ReadFromBinaryFiles<Part>(partFile.Replace("BF", "BF" + scaleFactor))
        //            );
        //        sw.Stop();
        //        testResults.phase11IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();

        //        Parallel.Invoke(parallelOptions,
        //            () =>
        //            {
        //                foreach (var row in dateDimension)
        //                {
        //                    // if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
        //                    dateHashTable.Add(row.dDateKey, row.dYear);
        //                }
        //            },
        //            () =>
        //            {
        //                foreach (var row in customerDimension)
        //                {
        //                    if (row.cRegion.Equals("AMERICA"))
        //                        customerHashTable.Add(row.cCustKey, row.cNation);
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
        //                foreach (var row in partDimension)
        //                {
        //                    if (row.pMFGR.Equals("MFGR#1") || row.pMFGR.Equals("MFGR#2"))
        //                        partHashTable.Add(row.pPartKey, row.pMFGR);
        //                }
        //            });

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
        //        List<Int64> loPartKey = null;

        //        Parallel.Invoke(parallelOptions,
        //            () => loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loPartKey = Utils.ReadFromBinaryFiles<Int64>(loPartKeyFile.Replace("BF", "BF" + scaleFactor)));
        //        sw.Stop();
        //        testResults.phase21IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        var listOrderDatePositions = new List<Int64>();
        //        var listCustomerKeyPositions = new List<Int64>();
        //        var listSupplierKeyPositions = new List<Int64>();
        //        var listPartKeyPositions = new List<Int64>();

        //        Parallel.Invoke(parallelOptions,
        //            () =>
        //            {
        //                var i = 0;
        //                foreach (var orderDate in loOrderDate)
        //                {
        //                    string dYear = "";
        //                    if (dateHashTable.TryGetValue(orderDate, out dYear))
        //                    {
        //                        listOrderDatePositions.Add(i);
        //                    }
        //                    i++;
        //                }
        //            },
        //            () =>
        //            {
        //                var j = 0;
        //                foreach (var custKey in loCustomerKey)
        //                {
        //                    string cNationOut = string.Empty;
        //                    if (customerHashTable.TryGetValue(custKey, out cNationOut))
        //                    {
        //                        listCustomerKeyPositions.Add(j);
        //                    }
        //                    j++;
        //                }
        //            },
        //            () =>
        //            {
        //                var k = 0;
        //                foreach (var suppKey in loSupplierKey)
        //                {
        //                    string sNationOut = string.Empty;
        //                    if (supplierHashTable.TryGetValue(suppKey, out sNationOut))
        //                    {
        //                        listSupplierKeyPositions.Add(k);
        //                    }
        //                    k++;
        //                }
        //            },
        //            () =>
        //            {
        //                var l = 0;
        //                foreach (var partKey in loPartKey)
        //                {
        //                    string pMFGR = string.Empty;
        //                    if (partHashTable.TryGetValue(partKey, out pMFGR))
        //                    {
        //                        listPartKeyPositions.Add(l);
        //                    }
        //                    l++;
        //                }
        //            });
        //        var common = listCustomerKeyPositions.Intersect(listOrderDatePositions).Intersect(listSupplierKeyPositions).Intersect(listPartKeyPositions).ToList();

        //        sw.Stop();
        //        testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
        //        testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime +
        //            testResults.phase22IOTime + testResults.phase22ProbeTime +
        //            testResults.phase23IOTime + testResults.phase23ProbeTime +
        //            testResults.phase24IOTime + testResults.phase24ProbeTime;
        //        sw.Reset();

        //        loOrderDate.Clear();
        //        dateHashTable.Clear();
        //        loCustomerKey.Clear();
        //        customerHashTable.Clear();
        //        loSupplierKey.Clear();
        //        supplierHashTable.Clear();
        //        loPartKey.Clear();
        //        partHashTable.Clear();

        //        long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
        //        #endregion Probing Phase


        //        #region Value Extraction Phase
        //        long memoryStartPhase3 = GC.GetTotalMemory(true);
        //        sw.Start();
        //        List<string> cNation = null;
        //        List<Int64> loRevenue = null;
        //        List<Int64> loSupplyCost = null;
        //        Parallel.Invoke(parallelOptions,
        //              () => loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor)),
        //              () => loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor)),
        //              () =>
        //              {
        //                  dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
        //                  foreach (var row in dateDimension)
        //                  {
        //                      // if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
        //                      dateHashTable.Add(row.dDateKey, row.dYear);
        //                  }
        //                  dateDimension.Clear();
        //              },
        //              () => cNation = Utils.ReadFromBinaryFiles<string>(cNationFile.Replace("BF", "BF" + scaleFactor)),
        //              () => loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor)),
        //              () => loSupplyCost = Utils.ReadFromBinaryFiles<Int64>(loSupplyCostFile.Replace("BF", "BF" + scaleFactor)));

        //        sw.Stop();
        //        testResults.phase3IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        var joinOutputFinal = new Dictionary<Int64, string>();
        //        object lockObject = new object();
        //        Parallel.ForEach(common, (index) =>
        //         {
        //             try
        //             {
        //                 var dateKey = loOrderDate[index];
        //                 var custKey = loCustomerKey[index];

        //                 var revenue = loRevenue[index];
        //                 var supplyCost = loSupplyCost[index];

        //                 // Position Look UP
        //                 string dYear;
        //                 dateHashTable.TryGetValue(dateKey, out dYear);

        //                 string cNationOut = cNation[custKey];
        //                 if (isFirst)
        //                 {
        //                     swInitialRecorder.Stop();
        //                     testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
        //                     isFirst = false;
        //                 }

        //                 outputRecordsCounter++;
        //                 if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
        //                 {
        //                     swOutputRecorder.Stop();
        //                     //testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
        //                     swOutputRecorder.Start();
        //                 }
        //                 // Console.WriteLine(l +", "+ dYear  + ", " + sNationOut + ", " + cNationOut);
        //                 lock (lockObject)
        //                 {
        //                     joinOutputFinal.Add(index, dYear + "," + cNationOut + "," + revenue + "," + supplyCost);
        //                 }
        //             }
        //             catch (Exception)
        //             {
        //                 throw;
        //             }
        //         });

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
        //        throw;
        //    }
        //}
        //public void Query_4_2()
        //{
        //    try
        //    {
        //        long memoryStartPhase1 = GC.GetTotalMemory(true);
        //        outputRecordsCounter = 0;
        //        var dateHashTable = new Dictionary<Int64, string>();
        //        var customerHashTable = new Dictionary<Int64, string>();
        //        var supplierHashTable = new Dictionary<Int64, string>();
        //        var partHashTable = new Dictionary<Int64, string>();
        //        Stopwatch sw = new Stopwatch();
        //        Stopwatch swInitialRecorder = new Stopwatch();
        //        Stopwatch swOutputRecorder = new Stopwatch();

        //        #region Key Hashing Phase
        //        List<Date> dateDimension = null;
        //        List<Supplier> supplierDimension = null;
        //        List<Customer> customerDimension = null;
        //        List<Part> partDimension = null;

        //        swInitialRecorder.Start();
        //        swOutputRecorder.Start();
        //        sw.Start();
        //        Parallel.Invoke(parallelOptions,
        //            () => dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor)),
        //            () => customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor)),
        //            () => supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor)),
        //            () => partDimension = Utils.ReadFromBinaryFiles<Part>(partFile.Replace("BF", "BF" + scaleFactor))
        //            );
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
        //                foreach (var row in customerDimension)
        //                {
        //                    if (row.cRegion.Equals("AMERICA"))
        //                        customerHashTable.Add(row.cCustKey, row.cNation);
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
        //                foreach (var row in partDimension)
        //                {
        //                    if (row.pMFGR.Equals("MFGR#1") || row.pMFGR.Equals("MFGR#2"))
        //                        partHashTable.Add(row.pPartKey, row.pCategory);
        //                }
        //            });

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
        //        List<Int64> loPartKey = null;

        //        Parallel.Invoke(parallelOptions,
        //            () => loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loPartKey = Utils.ReadFromBinaryFiles<Int64>(loPartKeyFile.Replace("BF", "BF" + scaleFactor)));
        //        sw.Stop();
        //        testResults.phase21IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        var listOrderDatePositions = new List<Int64>();
        //        var listCustomerKeyPositions = new List<Int64>();
        //        var listSupplierKeyPositions = new List<Int64>();
        //        var listPartKeyPositions = new List<Int64>();

        //        Parallel.Invoke(parallelOptions,
        //            () =>
        //            {
        //                var i = 0;
        //                foreach (var orderDate in loOrderDate)
        //                {
        //                    string dYear = "";
        //                    if (dateHashTable.TryGetValue(orderDate, out dYear))
        //                    {
        //                        listOrderDatePositions.Add(i);
        //                    }
        //                    i++;
        //                }
        //            },
        //            () =>
        //            {
        //                var j = 0;
        //                foreach (var custKey in loCustomerKey)
        //                {
        //                    string cNationOut = string.Empty;
        //                    if (customerHashTable.TryGetValue(custKey, out cNationOut))
        //                    {
        //                        listCustomerKeyPositions.Add(j);
        //                    }
        //                    j++;
        //                }
        //            },
        //            () =>
        //            {
        //                var k = 0;
        //                foreach (var suppKey in loSupplierKey)
        //                {
        //                    string sNationOut = string.Empty;
        //                    if (supplierHashTable.TryGetValue(suppKey, out sNationOut))
        //                    {
        //                        listSupplierKeyPositions.Add(k);
        //                    }
        //                    k++;
        //                }
        //            },
        //            () =>
        //            {
        //                var l = 0;
        //                foreach (var partKey in loPartKey)
        //                {
        //                    string pCategory = string.Empty;
        //                    if (partHashTable.TryGetValue(partKey, out pCategory))
        //                    {
        //                        listPartKeyPositions.Add(l);
        //                    }
        //                    l++;
        //                }
        //            });
        //        var common = listCustomerKeyPositions.Intersect(listOrderDatePositions).Intersect(listSupplierKeyPositions).Intersect(listPartKeyPositions).ToList();

        //        sw.Stop();
        //        testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
        //        testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime +
        //            testResults.phase22IOTime + testResults.phase22ProbeTime +
        //            testResults.phase23IOTime + testResults.phase23ProbeTime +
        //            testResults.phase24IOTime + testResults.phase24ProbeTime;
        //        sw.Reset();

        //        loOrderDate.Clear();
        //        dateHashTable.Clear();
        //        loCustomerKey.Clear();
        //        customerHashTable.Clear();
        //        loSupplierKey.Clear();
        //        supplierHashTable.Clear();
        //        loPartKey.Clear();
        //        partHashTable.Clear();

        //        long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
        //        #endregion Probing Phase


        //        #region Value Extraction Phase
        //        long memoryStartPhase3 = GC.GetTotalMemory(true);
        //        sw.Start();
        //        List<string> sNation = null;
        //        List<Int64> loRevenue = null;
        //        List<Int64> loSupplyCost = null;
        //        Parallel.Invoke(parallelOptions,
        //              () => loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor)),
        //              () => loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor)),
        //              () => loPartKey = Utils.ReadFromBinaryFiles<Int64>(loPartKeyFile.Replace("BF", "BF" + scaleFactor)),
        //              () =>
        //              {
        //                  partDimension = Utils.ReadFromBinaryFiles<Part>(partFile.Replace("BF", "BF" + scaleFactor));
        //                  foreach (var row in partDimension)
        //                  {
        //                      if (row.pMFGR.Equals("MFGR#1") || row.pMFGR.Equals("MFGR#2"))
        //                          partHashTable.Add(row.pPartKey, row.pCategory);
        //                  }
        //                  partDimension.Clear();
        //              },
        //              () =>
        //              {
        //                  dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
        //                  foreach (var row in dateDimension)
        //                  {
        //                      // if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
        //                      dateHashTable.Add(row.dDateKey, row.dYear);
        //                  }
        //                  dateDimension.Clear();
        //              },
        //              () => sNation = Utils.ReadFromBinaryFiles<string>(sNationFile.Replace("BF", "BF" + scaleFactor)),
        //              () => loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor)),
        //              () => loSupplyCost = Utils.ReadFromBinaryFiles<Int64>(loSupplyCostFile.Replace("BF", "BF" + scaleFactor)));

        //        sw.Stop();
        //        testResults.phase3IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        var joinOutputFinal = new Dictionary<Int64, string>();
        //        object lockObject = new object();
        //        Parallel.ForEach(common, (index) =>
        //        {
        //            try
        //            {
        //                var dateKey = loOrderDate[index];
        //                var suppKey = loSupplierKey[index];
        //                var partKey = loPartKey[index];
        //                var revenue = loRevenue[index];
        //                var supplyCost = loSupplyCost[index];

        //                // Position Look UP
        //                string dYear;
        //                dateHashTable.TryGetValue(dateKey, out dYear);

        //                string pCategory;
        //                partHashTable.TryGetValue(partKey, out pCategory);

        //                string sNationOut = sNation[suppKey - 1];
        //                if (isFirst)
        //                {
        //                    swInitialRecorder.Stop();
        //                    testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
        //                    isFirst = false;
        //                }

        //                outputRecordsCounter++;
        //                if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
        //                {
        //                    swOutputRecorder.Stop();
        //                    //testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
        //                    swOutputRecorder.Start();
        //                }
        //                // Console.WriteLine(l +", "+ dYear  + ", " + sNationOut + ", " + cNationOut);
        //                lock (lockObject)
        //                {
        //                    joinOutputFinal.Add(index, dYear + "," + sNationOut + "," + pCategory + "," + revenue + "," + supplyCost);
        //                }
        //            }

        //            catch (Exception)
        //            {
        //                throw;
        //            }
        //        });

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
        //        throw;
        //    }
        //}
        //public void Query_4_3()
        //{
        //    try
        //    {
        //        long memoryStartPhase1 = GC.GetTotalMemory(true);
        //        outputRecordsCounter = 0;
        //        var dateHashTable = new Dictionary<Int64, string>();
        //        var customerHashTable = new Dictionary<Int64, string>();
        //        var supplierHashTable = new Dictionary<Int64, string>();
        //        var partHashTable = new Dictionary<Int64, string>();
        //        Stopwatch sw = new Stopwatch();
        //        Stopwatch swInitialRecorder = new Stopwatch();
        //        Stopwatch swOutputRecorder = new Stopwatch();

        //        #region Key Hashing Phase
        //        List<Date> dateDimension = null;
        //        List<Supplier> supplierDimension = null;
        //        List<Customer> customerDimension = null;
        //        List<Part> partDimension = null;

        //        swInitialRecorder.Start();
        //        swOutputRecorder.Start();
        //        sw.Start();
        //        Parallel.Invoke(parallelOptions,
        //            () => dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor)),
        //            () => customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor)),
        //            () => supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor)),
        //            () => partDimension = Utils.ReadFromBinaryFiles<Part>(partFile.Replace("BF", "BF" + scaleFactor))
        //            );
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
        //                foreach (var row in customerDimension)
        //                {
        //                    if (row.cRegion.Equals("AMERICA"))
        //                        customerHashTable.Add(row.cCustKey, row.cNation);
        //                }
        //            },
        //            () =>
        //            {
        //                foreach (var row in supplierDimension)
        //                {
        //                    if (row.sNation.Equals("UNITED STATES"))
        //                        supplierHashTable.Add(row.sSuppKey, row.sCity);
        //                }
        //            },
        //            () =>
        //            {
        //                foreach (var row in partDimension)
        //                {
        //                    if (row.pCategory.Equals("MFGR#14"))
        //                        partHashTable.Add(row.pPartKey, row.pBrand);
        //                }
        //            });

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
        //        List<Int64> loPartKey = null;

        //        Parallel.Invoke(parallelOptions,
        //            () => loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor)),
        //            () => loPartKey = Utils.ReadFromBinaryFiles<Int64>(loPartKeyFile.Replace("BF", "BF" + scaleFactor)));
        //        sw.Stop();
        //        testResults.phase21IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        var listOrderDatePositions = new List<Int64>();
        //        var listCustomerKeyPositions = new List<Int64>();
        //        var listSupplierKeyPositions = new List<Int64>();
        //        var listPartKeyPositions = new List<Int64>();

        //        Parallel.Invoke(parallelOptions,
        //            () =>
        //            {
        //                var i = 0;
        //                foreach (var orderDate in loOrderDate)
        //                {
        //                    string dYear = "";
        //                    if (dateHashTable.TryGetValue(orderDate, out dYear))
        //                    {
        //                        listOrderDatePositions.Add(i);
        //                    }
        //                    i++;
        //                }
        //            },
        //            () =>
        //            {
        //                var j = 0;
        //                foreach (var custKey in loCustomerKey)
        //                {
        //                    string cNationOut = string.Empty;
        //                    if (customerHashTable.TryGetValue(custKey, out cNationOut))
        //                    {
        //                        listCustomerKeyPositions.Add(j);
        //                    }
        //                    j++;
        //                }
        //            },
        //            () =>
        //            {
        //                var k = 0;
        //                foreach (var suppKey in loSupplierKey)
        //                {
        //                    string sNationOut = string.Empty;
        //                    if (supplierHashTable.TryGetValue(suppKey, out sNationOut))
        //                    {
        //                        listSupplierKeyPositions.Add(k);
        //                    }
        //                    k++;
        //                }
        //            },
        //            () =>
        //            {
        //                var l = 0;
        //                foreach (var partKey in loPartKey)
        //                {
        //                    string pMFGR = string.Empty;
        //                    if (partHashTable.TryGetValue(partKey, out pMFGR))
        //                    {
        //                        listPartKeyPositions.Add(l);
        //                    }
        //                    l++;
        //                }
        //            });
        //        var common = listCustomerKeyPositions.Intersect(listOrderDatePositions).Intersect(listSupplierKeyPositions).Intersect(listPartKeyPositions).ToList();

        //        sw.Stop();
        //        testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
        //        testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime +
        //            testResults.phase22IOTime + testResults.phase22ProbeTime +
        //            testResults.phase23IOTime + testResults.phase23ProbeTime +
        //            testResults.phase24IOTime + testResults.phase24ProbeTime;
        //        sw.Reset();

        //        loOrderDate.Clear();
        //        dateHashTable.Clear();
        //        loCustomerKey.Clear();
        //        customerHashTable.Clear();
        //        loSupplierKey.Clear();
        //        supplierHashTable.Clear();
        //        loPartKey.Clear();
        //        partHashTable.Clear();

        //        long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
        //        #endregion Probing Phase


        //        #region Value Extraction Phase
        //        long memoryStartPhase3 = GC.GetTotalMemory(true);
        //        sw.Start();
        //        List<string> sNation = null;
        //        List<Int64> loRevenue = null;
        //        List<Int64> loSupplyCost = null;
        //        Parallel.Invoke(parallelOptions,
        //              () => loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor)),
        //              () => loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor)),
        //              () => loPartKey = Utils.ReadFromBinaryFiles<Int64>(loPartKeyFile.Replace("BF", "BF" + scaleFactor)),
        //              () =>
        //              {
        //                  partDimension = Utils.ReadFromBinaryFiles<Part>(partFile.Replace("BF", "BF" + scaleFactor));
        //                  foreach (var row in partDimension)
        //                  {
        //                      if (row.pCategory.Equals("MFGR#14"))
        //                          partHashTable.Add(row.pPartKey, row.pBrand);
        //                  }
        //                  partDimension.Clear();
        //              },
        //              () =>
        //              {
        //                  dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
        //                  foreach (var row in dateDimension)
        //                  {
        //                      if (row.dYear.Equals("1997") || row.dYear.Equals("1998"))
        //                          dateHashTable.Add(row.dDateKey, row.dYear);
        //                  }
        //                  dateDimension.Clear();
        //              },
        //              () => sNation = Utils.ReadFromBinaryFiles<string>(sNationFile.Replace("BF", "BF" + scaleFactor)),
        //              () => loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor)),
        //              () => loSupplyCost = Utils.ReadFromBinaryFiles<Int64>(loSupplyCostFile.Replace("BF", "BF" + scaleFactor)));

        //        sw.Stop();
        //        testResults.phase3IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        var joinOutputFinal = new Dictionary<Int64, string>();
        //        object lockObject = new object();
        //        Parallel.ForEach(common, (index) =>
        //        {
        //            try
        //            {
        //                var dateKey = loOrderDate[index];
        //                var suppKey = loSupplierKey[index];
        //                var partKey = loPartKey[index];
        //                var revenue = loRevenue[index];
        //                var supplyCost = loSupplyCost[index];

        //                // Position Look UP
        //                string dYear;
        //                dateHashTable.TryGetValue(dateKey, out dYear);

        //                string pCategory;
        //                partHashTable.TryGetValue(partKey, out pCategory);

        //                string sNationOut = sNation[suppKey - 1];
        //                if (isFirst)
        //                {
        //                    swInitialRecorder.Stop();
        //                    testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
        //                    isFirst = false;
        //                }

        //                outputRecordsCounter++;
        //                if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
        //                {
        //                    swOutputRecorder.Stop();
        //                    //testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
        //                    swOutputRecorder.Start();
        //                }
        //                // Console.WriteLine(l +", "+ dYear  + ", " + sNationOut + ", " + cNationOut);
        //                lock (lockObject)
        //                {
        //                    joinOutputFinal.Add(index, dYear + "," + sNationOut + "," + pCategory + "," + revenue + "," + supplyCost);
        //                }
        //            }

        //            catch (Exception)
        //            {
        //                throw;
        //            }
        //        });

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
        //        throw;
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
        //         () => customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor)),
        //         () => supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor))
        //              );

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

        //        sw.Reset();

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
        //        var listOrderDatePositions = new List<Int64>();
        //        var listCustomerKeyPositions = new List<Int64>();
        //        var listSupplierKeyPositions = new List<Int64>();

        //        Parallel.Invoke(parallelOptions, () =>
        //        {
        //            var i = 0;
        //            foreach (var orderDate in loOrderDate)
        //            {
        //                string dYear = "";
        //                if (dateHashTable.TryGetValue(orderDate, out dYear))
        //                {
        //                    listOrderDatePositions.Add(i);
        //                }
        //                i++;
        //            }
        //        },
        //        () =>
        //        {
        //            var j = 0;
        //            foreach (var custKey in loCustomerKey)
        //            {
        //                string cNationOut = string.Empty;
        //                if (customerHashTable.TryGetValue(custKey, out cNationOut))
        //                {
        //                    listCustomerKeyPositions.Add(j);
        //                }
        //                j++;
        //            }
        //        },
        //        () =>
        //        {
        //            var k = 0;
        //            foreach (var suppKey in loSupplierKey)
        //            {
        //                string sNationOut = string.Empty;
        //                if (supplierHashTable.TryGetValue(suppKey, out sNationOut))
        //                {
        //                    listSupplierKeyPositions.Add(k);
        //                }
        //                k++;
        //            }
        //        });

        //        var common = listCustomerKeyPositions.Intersect(listOrderDatePositions).Intersect(listSupplierKeyPositions).ToList();

        //        sw.Stop();
        //        testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
        //        testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime;
        //        sw.Reset();

        //        loOrderDate.Clear();
        //        dateHashTable.Clear();
        //        loCustomerKey.Clear();
        //        customerHashTable.Clear();
        //        loSupplierKey.Clear();
        //        supplierHashTable.Clear();

        //        long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
        //        #endregion Probing Phase

        //        #region Value Extraction Phase
        //        long memoryStartPhase3 = GC.GetTotalMemory(true);
        //        sw.Start();
        //        List<string> cNation = null;
        //        List<string> sNation = null;
        //        List<Int64> loRevenue = null;
        //        Parallel.Invoke(parallelOptions, () => loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor)),
        //        () => loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor)),
        //        () => loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor)),
        //        () => cNation = Utils.ReadFromBinaryFiles<string>(cNationFile.Replace("BF", "BF" + scaleFactor)),
        //        () => sNation = Utils.ReadFromBinaryFiles<string>(sNationFile.Replace("BF", "BF" + scaleFactor)),
        //        () => loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor)),
        //        () => dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor))
        //        );

        //        foreach (var row in dateDimension)
        //        {
        //            if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
        //                dateHashTable.Add(row.dDateKey, row.dYear);
        //        }

        //        dateDimension.Clear();
        //        sw.Stop();
        //        testResults.phase3IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        var joinOutputIntermediate = new ConcurrentDictionary<Int64, string>();
        //        Parallel.ForEach(common, (index) =>
        //        {
        //            try
        //            {
        //                var dateKey = 0;
        //                var custKey = 0;
        //                var suppKey = 0;
        //                string cNationOut = "";
        //                string sNationOut = "";
        //                string dYear = "";

        //                dateKey = loOrderDate[index];
        //                dateHashTable.TryGetValue(dateKey, out dYear);
        //                custKey = loCustomerKey[index];
        //                cNationOut = cNation[custKey];
        //                suppKey = loSupplierKey[index];
        //                sNationOut = sNation[suppKey - 1];
        //                if (isFirst)
        //                {
        //                    swInitialRecorder.Stop();
        //                    testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
        //                    isFirst = false;
        //                }
        //                outputRecordsCounter++;
        //                if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
        //                {
        //                    swOutputRecorder.Stop();
        //                    //testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
        //                    swOutputRecorder.Start();
        //                }
        //                // Console.WriteLine(l +", "+ dYear  + ", " + sNationOut + ", " + cNationOut);
        //                joinOutputIntermediate.TryAdd(index, cNationOut + "," + sNationOut + "," + dYear);
        //            }
        //            catch (Exception)
        //            {
        //                throw;
        //            }
        //        });

        //        var joinOutputFinal = new Dictionary<Int64, string>();
        //        foreach (var item in joinOutputIntermediate)
        //        {
        //            joinOutputFinal.Add(item.Key, item.Value + "," + loRevenue[item.Key]);
        //        }

        //        sw.Stop();

        //        long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
        //        #endregion Value Extraction Phase

        //        testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
        //        testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
        //        testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
        //        // Console.WriteLine("[Nimble Join]: Time taken {0} ms.", testResults.totalExecutionTime);
        //        testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
        //        testResults.totalNumberOfOutput = joinOutputFinal.Count;

        //    }
        //    catch (Exception ex)
        //    {

        //        throw;
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
        //         () => customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor)),
        //         () => supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor))
        //              );

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

        //        sw.Reset();

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
        //        var listOrderDatePositions = new List<Int64>();
        //        var listCustomerKeyPositions = new List<Int64>();
        //        var listSupplierKeyPositions = new List<Int64>();

        //        Parallel.Invoke(parallelOptions, () =>
        //        {
        //            var i = 0;
        //            foreach (var orderDate in loOrderDate)
        //            {
        //                string dYear = "";
        //                if (dateHashTable.TryGetValue(orderDate, out dYear))
        //                {
        //                    listOrderDatePositions.Add(i);
        //                }
        //                i++;
        //            }
        //        },
        //        () =>
        //        {
        //            var j = 0;
        //            foreach (var custKey in loCustomerKey)
        //            {
        //                string cCityOut = string.Empty;
        //                if (customerHashTable.TryGetValue(custKey, out cCityOut))
        //                {
        //                    listCustomerKeyPositions.Add(j);
        //                }
        //                j++;
        //            }
        //        },
        //        () =>
        //        {
        //            var k = 0;
        //            foreach (var suppKey in loSupplierKey)
        //            {
        //                string sCityOut = string.Empty;
        //                if (supplierHashTable.TryGetValue(suppKey, out sCityOut))
        //                {
        //                    listSupplierKeyPositions.Add(k);
        //                }
        //                k++;
        //            }
        //        });

        //        var common = listCustomerKeyPositions.Intersect(listOrderDatePositions).Intersect(listSupplierKeyPositions).ToList();

        //        sw.Stop();
        //        testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
        //        testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime;
        //        sw.Reset();

        //        loOrderDate.Clear();
        //        dateHashTable.Clear();
        //        loCustomerKey.Clear();
        //        customerHashTable.Clear();
        //        loSupplierKey.Clear();
        //        supplierHashTable.Clear();

        //        long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
        //        #endregion Probing Phase

        //        #region Value Extraction Phase
        //        long memoryStartPhase3 = GC.GetTotalMemory(true);
        //        sw.Start();
        //        List<string> cCity = null;
        //        List<string> sCity = null;
        //        List<Int64> loRevenue = null;
        //        Parallel.Invoke(parallelOptions, () => loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor)),
        //        () => loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor)),
        //        () => loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor)),
        //        () => cCity = Utils.ReadFromBinaryFiles<string>(cCityFile.Replace("BF", "BF" + scaleFactor)),
        //        () => sCity = Utils.ReadFromBinaryFiles<string>(sCityFile.Replace("BF", "BF" + scaleFactor)),
        //        () => loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor)),
        //        () => dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor))
        //        );

        //        foreach (var row in dateDimension)
        //        {
        //            if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
        //                dateHashTable.Add(row.dDateKey, row.dYear);
        //        }

        //        dateDimension.Clear();
        //        sw.Stop();
        //        testResults.phase3IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        var joinOutputIntermediate = new ConcurrentDictionary<Int64, string>();
        //        Parallel.ForEach(common, (index) =>
        //        {
        //            try
        //            {
        //                var dateKey = 0;
        //                var custKey = 0;
        //                var suppKey = 0;
        //                string cCityOut = "";
        //                string sCityOut = "";
        //                string dYear = "";

        //                dateKey = loOrderDate[index];
        //                dateHashTable.TryGetValue(dateKey, out dYear);
        //                custKey = loCustomerKey[index];
        //                cCityOut = cCity[custKey];
        //                suppKey = loSupplierKey[index];
        //                sCityOut = sCity[suppKey - 1];
        //                if (isFirst)
        //                {
        //                    swInitialRecorder.Stop();
        //                    testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
        //                    isFirst = false;
        //                }
        //                outputRecordsCounter++;
        //                if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
        //                {
        //                    swOutputRecorder.Stop();
        //                    //testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
        //                    swOutputRecorder.Start();
        //                }
        //                // Console.WriteLine(l +", "+ dYear  + ", " + sNationOut + ", " + cNationOut);
        //                joinOutputIntermediate.TryAdd(index, cCityOut + "," + sCityOut + "," + dYear);
        //            }
        //            catch (Exception)
        //            {
        //                throw;
        //            }
        //        });

        //        var joinOutputFinal = new Dictionary<Int64, string>();
        //        foreach (var item in joinOutputIntermediate)
        //        {
        //            joinOutputFinal.Add(item.Key, item.Value + "," + loRevenue[item.Key]);
        //        }

        //        sw.Stop();

        //        long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
        //        #endregion Value Extraction Phase

        //        testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
        //        testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
        //        testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
        //        // Console.WriteLine("[Nimble Join]: Time taken {0} ms.", testResults.totalExecutionTime);
        //        testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
        //        testResults.totalNumberOfOutput = joinOutputFinal.Count;

        //    }
        //    catch (Exception ex)
        //    {

        //        throw;
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
        //         () => customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor)),
        //         () => supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor))
        //              );

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

        //        sw.Reset();

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
        //        var listOrderDatePositions = new List<Int64>();
        //        var listCustomerKeyPositions = new List<Int64>();
        //        var listSupplierKeyPositions = new List<Int64>();

        //        Parallel.Invoke(parallelOptions, () =>
        //        {
        //            var i = 0;
        //            foreach (var orderDate in loOrderDate)
        //            {
        //                string dYear = "";
        //                if (dateHashTable.TryGetValue(orderDate, out dYear))
        //                {
        //                    listOrderDatePositions.Add(i);
        //                }
        //                i++;
        //            }
        //        },
        //        () =>
        //        {
        //            var j = 0;
        //            foreach (var custKey in loCustomerKey)
        //            {
        //                string cCityOut = string.Empty;
        //                if (customerHashTable.TryGetValue(custKey, out cCityOut))
        //                {
        //                    listCustomerKeyPositions.Add(j);
        //                }
        //                j++;
        //            }
        //        },
        //        () =>
        //        {
        //            var k = 0;
        //            foreach (var suppKey in loSupplierKey)
        //            {
        //                string sCityOut = string.Empty;
        //                if (supplierHashTable.TryGetValue(suppKey, out sCityOut))
        //                {
        //                    listSupplierKeyPositions.Add(k);
        //                }
        //                k++;
        //            }
        //        });

        //        var common = listCustomerKeyPositions.Intersect(listOrderDatePositions).Intersect(listSupplierKeyPositions).ToList();

        //        sw.Stop();
        //        testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
        //        testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime;
        //        sw.Reset();

        //        loOrderDate.Clear();
        //        dateHashTable.Clear();
        //        loCustomerKey.Clear();
        //        customerHashTable.Clear();
        //        loSupplierKey.Clear();
        //        supplierHashTable.Clear();

        //        long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
        //        #endregion Probing Phase

        //        #region Value Extraction Phase
        //        long memoryStartPhase3 = GC.GetTotalMemory(true);
        //        sw.Start();
        //        List<string> cCity = null;
        //        List<string> sCity = null;
        //        List<Int64> loRevenue = null;
        //        Parallel.Invoke(parallelOptions, () => loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor)),
        //        () => loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor)),
        //        () => loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor)),
        //        () => cCity = Utils.ReadFromBinaryFiles<string>(cCityFile.Replace("BF", "BF" + scaleFactor)),
        //        () => sCity = Utils.ReadFromBinaryFiles<string>(sCityFile.Replace("BF", "BF" + scaleFactor)),
        //        () => loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor)),
        //        () => dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor))
        //        );

        //        foreach (var row in dateDimension)
        //        {
        //            if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
        //                dateHashTable.Add(row.dDateKey, row.dYear);
        //        }

        //        dateDimension.Clear();
        //        sw.Stop();
        //        testResults.phase3IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        var joinOutputIntermediate = new ConcurrentDictionary<Int64, string>();
        //        Parallel.ForEach(common, (index) =>
        //        {
        //            try
        //            {
        //                var dateKey = 0;
        //                var custKey = 0;
        //                var suppKey = 0;
        //                string cCityOut = "";
        //                string sCityOut = "";
        //                string dYear = "";

        //                dateKey = loOrderDate[index];
        //                dateHashTable.TryGetValue(dateKey, out dYear);
        //                custKey = loCustomerKey[index];
        //                cCityOut = cCity[custKey];
        //                suppKey = loSupplierKey[index];
        //                sCityOut = sCity[suppKey - 1];
        //                if (isFirst)
        //                {
        //                    swInitialRecorder.Stop();
        //                    testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
        //                    isFirst = false;
        //                }
        //                outputRecordsCounter++;
        //                if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
        //                {
        //                    swOutputRecorder.Stop();
        //                    //testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
        //                    swOutputRecorder.Start();
        //                }
        //                // Console.WriteLine(l +", "+ dYear  + ", " + sNationOut + ", " + cNationOut);
        //                joinOutputIntermediate.TryAdd(index, cCityOut + "," + sCityOut + "," + dYear);
        //            }
        //            catch (Exception)
        //            {
        //                throw;
        //            }
        //        });

        //        var joinOutputFinal = new Dictionary<Int64, string>();
        //        foreach (var item in joinOutputIntermediate)
        //        {
        //            joinOutputFinal.Add(item.Key, item.Value + "," + loRevenue[item.Key]);
        //        }

        //        sw.Stop();

        //        long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
        //        #endregion Value Extraction Phase

        //        testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
        //        testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
        //        testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
        //        // Console.WriteLine("[Nimble Join]: Time taken {0} ms.", testResults.totalExecutionTime);
        //        testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
        //        testResults.totalNumberOfOutput = joinOutputFinal.Count;

        //    }
        //    catch (Exception ex)
        //    {

        //        throw;
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
        //         () => customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor)),
        //         () => supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor))
        //              );

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

        //        sw.Reset();

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
        //        var listOrderDatePositions = new List<Int64>();
        //        var listCustomerKeyPositions = new List<Int64>();
        //        var listSupplierKeyPositions = new List<Int64>();

        //        Parallel.Invoke(parallelOptions, () =>
        //        {
        //            var i = 0;
        //            foreach (var orderDate in loOrderDate)
        //            {
        //                string dYear = "";
        //                if (dateHashTable.TryGetValue(orderDate, out dYear))
        //                {
        //                    listOrderDatePositions.Add(i);
        //                }
        //                i++;
        //            }
        //        },
        //        () =>
        //        {
        //            var j = 0;
        //            foreach (var custKey in loCustomerKey)
        //            {
        //                string cCityOut = string.Empty;
        //                if (customerHashTable.TryGetValue(custKey, out cCityOut))
        //                {
        //                    listCustomerKeyPositions.Add(j);
        //                }
        //                j++;
        //            }
        //        },
        //        () =>
        //        {
        //            var k = 0;
        //            foreach (var suppKey in loSupplierKey)
        //            {
        //                string sCityOut = string.Empty;
        //                if (supplierHashTable.TryGetValue(suppKey, out sCityOut))
        //                {
        //                    listSupplierKeyPositions.Add(k);
        //                }
        //                k++;
        //            }
        //        });

        //        var common = listCustomerKeyPositions.Intersect(listOrderDatePositions).Intersect(listSupplierKeyPositions).ToList();

        //        sw.Stop();
        //        testResults.phase21ProbeTime = sw.ElapsedMilliseconds;
        //        testResults.phase2Time = testResults.phase21IOTime + testResults.phase21ProbeTime;
        //        sw.Reset();

        //        loOrderDate.Clear();
        //        dateHashTable.Clear();
        //        loCustomerKey.Clear();
        //        customerHashTable.Clear();
        //        loSupplierKey.Clear();
        //        supplierHashTable.Clear();

        //        long memoryUsedPhase2 = GC.GetTotalMemory(true) - memoryStartPhase2;
        //        #endregion Probing Phase

        //        #region Value Extraction Phase
        //        long memoryStartPhase3 = GC.GetTotalMemory(true);
        //        sw.Start();
        //        List<string> cCity = null;
        //        List<string> sCity = null;
        //        List<Int64> loRevenue = null;
        //        Parallel.Invoke(parallelOptions, () => loOrderDate = Utils.ReadFromBinaryFiles<Int64>(loOrderDateFile.Replace("BF", "BF" + scaleFactor)),
        //        () => loCustomerKey = Utils.ReadFromBinaryFiles<Int64>(loCustKeyFile.Replace("BF", "BF" + scaleFactor)),
        //        () => loSupplierKey = Utils.ReadFromBinaryFiles<Int64>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor)),
        //        () => cCity = Utils.ReadFromBinaryFiles<string>(cCityFile.Replace("BF", "BF" + scaleFactor)),
        //        () => sCity = Utils.ReadFromBinaryFiles<string>(sCityFile.Replace("BF", "BF" + scaleFactor)),
        //        () => loRevenue = Utils.ReadFromBinaryFiles<Int64>(loRevenueFile.Replace("BF", "BF" + scaleFactor)),
        //        () => dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor))
        //        );

        //        foreach (var row in dateDimension)
        //        {
        //            if (row.dYearMonth.Equals("Dec1997"))
        //                dateHashTable.Add(row.dDateKey, row.dYear);
        //        }

        //        dateDimension.Clear();
        //        sw.Stop();
        //        testResults.phase3IOTime = sw.ElapsedMilliseconds;
        //        sw.Reset();

        //        sw.Start();
        //        var joinOutputIntermediate = new ConcurrentDictionary<Int64, string>();
        //        Parallel.ForEach(common, (index) =>
        //        {
        //            try
        //            {
        //                var dateKey = 0;
        //                var custKey = 0;
        //                var suppKey = 0;
        //                string cCityOut = "";
        //                string sCityOut = "";
        //                string dYear = "";

        //                dateKey = loOrderDate[index];
        //                dateHashTable.TryGetValue(dateKey, out dYear);
        //                custKey = loCustomerKey[index];
        //                cCityOut = cCity[custKey];
        //                suppKey = loSupplierKey[index];
        //                sCityOut = sCity[suppKey - 1];
        //                if (isFirst)
        //                {
        //                    swInitialRecorder.Stop();
        //                    testResults.initialResposeTime = swInitialRecorder.ElapsedMilliseconds;
        //                    isFirst = false;
        //                }
        //                outputRecordsCounter++;
        //                if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
        //                {
        //                    swOutputRecorder.Stop();
        //                    //testResults.outputRateList.Add(new Tuple<long, long>(outputRecordsCounter, swOutputRecorder.ElapsedMilliseconds));
        //                    swOutputRecorder.Start();
        //                }
        //                // Console.WriteLine(l +", "+ dYear  + ", " + sNationOut + ", " + cNationOut);
        //                joinOutputIntermediate.TryAdd(index, cCityOut + "," + sCityOut + "," + dYear);
        //            }
        //            catch (Exception)
        //            {
        //                throw;
        //            }
        //        });

        //        var joinOutputFinal = new Dictionary<Int64, string>();
        //        foreach (var item in joinOutputIntermediate)
        //        {
        //            joinOutputFinal.Add(item.Key, item.Value + "," + loRevenue[item.Key]);
        //        }

        //        sw.Stop();

        //        long memoryUsedPhase3 = GC.GetTotalMemory(true) - memoryStartPhase3;
        //        #endregion Value Extraction Phase

        //        testResults.phase3ExtractionTime = sw.ElapsedMilliseconds;
        //        testResults.phase3Time = testResults.phase3IOTime + testResults.phase3ExtractionTime;
        //        testResults.totalExecutionTime = testResults.phase1Time + testResults.phase2Time + testResults.phase3Time;
        //        // Console.WriteLine("[Nimble Join]: Time taken {0} ms.", testResults.totalExecutionTime);
        //        testResults.memoryUsed = memoryUsedPhase1 + "," + memoryUsedPhase2 + "," + memoryUsedPhase3 + "," + (memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) + "," + (((memoryUsedPhase1 + memoryUsedPhase2 + memoryUsedPhase3) / testResults.totalRAMAvailable) * 100) + "%";
        //        testResults.totalNumberOfOutput = joinOutputFinal.Count;

        //    }
        //    catch (Exception ex)
        //    {

        //        throw;
        //    }

        //}
        //#endregion

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
                Console.WriteLine(String.Format("[PIJ] T0 Time: {0}", t0));
                #endregion Key Hashing Phase

                #region Probing Phase
                sw.Reset();
                sw.Start();

                var listOrderDatePositions = new BitArray(InMemoryData.loOrderDate.Count);
                var listPartKeyPositions = new BitArray(InMemoryData.loPartKey.Count);
                var listSupplierKeyPositions = new BitArray(InMemoryData.loSupplierKey.Count);

                Parallel.Invoke(parallelOptions,
                    () =>
                    {
                        var i = 0;
                        foreach (var orderDate in InMemoryData.loOrderDate)
                        {
                            string dYear = "";
                            if (dateHashTable.TryGetValue(orderDate, out dYear))
                            {
                                listOrderDatePositions.Set(i, true);
                            }
                            i++;
                        }
                    },
                    () =>
                    {
                        var j = 0;
                        foreach (var partKey in InMemoryData.loPartKey)
                        {
                            string pBrandOut = string.Empty;
                            if (partHashTable.TryGetValue(partKey, out pBrandOut))
                            {
                                listPartKeyPositions.Set(j, true);
                            }
                            j++;
                        }

                    },
                    () =>
                    {
                        var k = 0;
                        foreach (var suppKey in InMemoryData.loSupplierKey)
                        {
                            string sNationOut = string.Empty;
                            if (supplierHashTable.TryGetValue(suppKey, out sNationOut))
                            {
                                listSupplierKeyPositions.Set(k, true);
                            }
                            k++;
                        }
                    });

                var common = listPartKeyPositions.And(listOrderDatePositions).And(listSupplierKeyPositions);
                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PIJ] T1 Time: {0}", t1));
                sw.Reset();
                #endregion Probing Phase


                #region Value Extraction Phase
                sw.Start();
                var joinOutputFinal = new Dictionary<string, Int64>();
                Int32 index = 0;
                foreach (bool bit in common)
                {
                    try
                    {
                        if (bit)
                        {
                            var dateKey = InMemoryData.loOrderDate[index];
                            var partKey = InMemoryData.loPartKey[index];

                            // Position Look UP
                            string dYear;
                            dateHashTable.TryGetValue(dateKey, out dYear);
                            string pBrandOut;
                            partHashTable.TryGetValue(partKey, out pBrandOut);
                            string key = dYear + ", " + pBrandOut;
                            Int64 revenue = 0;
                            if (joinOutputFinal.TryGetValue(key, out revenue))
                            {
                                joinOutputFinal[key] = revenue + InMemoryData.loRevenue[index];
                            }
                            else
                            {
                                joinOutputFinal.Add(key, InMemoryData.loRevenue[index]);
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
                Console.WriteLine(String.Format("[PIJ] T2 Time: {0}", t2));
                Console.WriteLine(String.Format("[PIJ] Total Time: {0}", t0 + t1 + t2));
                Console.WriteLine(String.Format("[PIJ] Total Count : {0}", joinOutputFinal.Count));
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
                Console.WriteLine(String.Format("[PIJ] T0 Time: {0}", t0));
                #endregion Key Hashing Phase

                #region Probing Phase
                sw.Reset();
                sw.Start();

                var listOrderDatePositions = new BitArray(InMemoryData.loOrderDate.Count);
                var listPartKeyPositions = new BitArray(InMemoryData.loPartKey.Count);
                var listSupplierKeyPositions = new BitArray(InMemoryData.loSupplierKey.Count);

                Parallel.Invoke(parallelOptions,
                    () =>
                    {
                        var i = 0;
                        foreach (var orderDate in InMemoryData.loOrderDate)
                        {
                            string dYear = "";
                            if (dateHashTable.TryGetValue(orderDate, out dYear))
                            {
                                listOrderDatePositions.Set(i, true);
                            }
                            i++;
                        }
                    },
                    () =>
                    {
                        var j = 0;
                        foreach (var partKey in InMemoryData.loPartKey)
                        {
                            string pBrandOut = string.Empty;
                            if (partHashTable.TryGetValue(partKey, out pBrandOut))
                            {
                                listPartKeyPositions.Set(j, true);
                            }
                            j++;
                        }

                    },
                    () =>
                    {
                        var k = 0;
                        foreach (var suppKey in InMemoryData.loSupplierKey)
                        {
                            string sNationOut = string.Empty;
                            if (supplierHashTable.TryGetValue(suppKey, out sNationOut))
                            {
                                listSupplierKeyPositions.Set(k, true);
                            }
                            k++;
                        }
                    });

                var common = listPartKeyPositions.And(listOrderDatePositions).And(listSupplierKeyPositions);
                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PIJ] T1 Time: {0}", t1));
                sw.Reset();
                #endregion Probing Phase


                #region Value Extraction Phase
                sw.Start();
                var joinOutputFinal = new Dictionary<string, Int64>();
                Int32 index = 0;
                foreach (bool bit in common)
                {
                    try
                    {
                        if (bit)
                        {
                            var dateKey = InMemoryData.loOrderDate[index];
                            var partKey = InMemoryData.loPartKey[index];

                            // Position Look UP
                            string dYear;
                            dateHashTable.TryGetValue(dateKey, out dYear);
                            string pBrandOut;
                            partHashTable.TryGetValue(partKey, out pBrandOut);
                            string key = dYear + ", " + pBrandOut;
                            Int64 revenue = 0;
                            if (joinOutputFinal.TryGetValue(key, out revenue))
                            {
                                joinOutputFinal[key] = revenue + InMemoryData.loRevenue[index];
                            }
                            else
                            {
                                joinOutputFinal.Add(key, InMemoryData.loRevenue[index]);
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
                Console.WriteLine(String.Format("[PIJ] T2 Time: {0}", t2));
                Console.WriteLine(String.Format("[PIJ] Total Time: {0}", t0 + t1 + t2));
                Console.WriteLine(String.Format("[PIJ] Total Count: {0}", joinOutputFinal.Count));
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
                    }
                    );

                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PIJ] T0 Time: {0}", t0));
                #endregion Key Hashing Phase

                #region Probing Phase
                sw.Reset();
                sw.Start();

                var listOrderDatePositions = new BitArray(InMemoryData.loOrderDate.Count);
                var listPartKeyPositions = new BitArray(InMemoryData.loPartKey.Count);
                var listSupplierKeyPositions = new BitArray(InMemoryData.loSupplierKey.Count);

                Parallel.Invoke(parallelOptions,
                    () =>
                    {
                        var i = 0;
                        foreach (var orderDate in InMemoryData.loOrderDate)
                        {
                            string dYear = "";
                            if (dateHashTable.TryGetValue(orderDate, out dYear))
                            {
                                listOrderDatePositions.Set(i, true);
                            }
                            i++;
                        }
                    },
                    () =>
                    {
                        var j = 0;
                        foreach (var partKey in InMemoryData.loPartKey)
                        {
                            string pBrandOut = string.Empty;
                            if (partHashTable.TryGetValue(partKey, out pBrandOut))
                            {
                                listPartKeyPositions.Set(j, true);
                            }
                            j++;
                        }

                    },
                    () =>
                    {
                        var k = 0;
                        foreach (var suppKey in InMemoryData.loSupplierKey)
                        {
                            string sNationOut = string.Empty;
                            if (supplierHashTable.TryGetValue(suppKey, out sNationOut))
                            {
                                listSupplierKeyPositions.Set(k, true);
                            }
                            k++;
                        }
                    });

                var common = listPartKeyPositions.And(listOrderDatePositions).And(listSupplierKeyPositions);
                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PIJ] T1 Time: {0}", t1));
                sw.Reset();
                #endregion Probing Phase


                #region Value Extraction Phase
                sw.Start();
                var joinOutputFinal = new Dictionary<string, Int64>();
                Int32 index = 0;
                foreach (bool bit in common)
                {
                    try
                    {
                        if (bit)
                        {
                            var dateKey = InMemoryData.loOrderDate[index];
                            var partKey = InMemoryData.loPartKey[index];

                            // Position Look UP
                            string dYear;
                            dateHashTable.TryGetValue(dateKey, out dYear);
                            string pBrandOut;
                            partHashTable.TryGetValue(partKey, out pBrandOut);
                            string key = dYear + ", " + pBrandOut;
                            Int64 revenue = 0;
                            if (joinOutputFinal.TryGetValue(key, out revenue))
                            {
                                joinOutputFinal[key] = revenue + InMemoryData.loRevenue[index];
                            }
                            else
                            {
                                joinOutputFinal.Add(key, InMemoryData.loRevenue[index]);
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
                Console.WriteLine(String.Format("[PIJ] T2 Time: {0}", t2));
                Console.WriteLine(String.Format("[PIJ] Total Time: {0}", t0 + t1 + t2));
                Console.WriteLine(String.Format("[PIJ] Total : {0}", joinOutputFinal.Count));
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
                Console.WriteLine(String.Format("[PIJ] T0 Time: {0}", t0));
                #endregion Key Hashing Phase

                #region Probing Phase
                sw.Reset();
                sw.Start();

                var listOrderDatePositions = new BitArray(InMemoryData.loOrderDate.Count);
                var listCustomerKeyPositions = new BitArray(InMemoryData.loCustomerKey.Count);
                var listSupplierKeyPositions = new BitArray(InMemoryData.loSupplierKey.Count);

                Parallel.Invoke(parallelOptions,
                    () =>
                    {
                        var i = 0;
                        foreach (var orderDate in InMemoryData.loOrderDate)
                        {
                            string dYear = "";
                            if (dateHashTable.TryGetValue(orderDate, out dYear))
                            {
                                listOrderDatePositions.Set(i, true);
                            }
                            i++;
                        }
                    },
                    () =>
                    {
                        var j = 0;
                        foreach (var custKey in InMemoryData.loCustomerKey)
                        {
                            string cNationOut = string.Empty;
                            if (customerHashTable.TryGetValue(custKey, out cNationOut))
                            {
                                listCustomerKeyPositions.Set(j, true);
                            }
                            j++;
                        }

                    },
                    () =>
                    {
                        var k = 0;
                        foreach (var suppKey in InMemoryData.loSupplierKey)
                        {
                            string sNationOut = string.Empty;
                            if (supplierHashTable.TryGetValue(suppKey, out sNationOut))
                            {
                                listSupplierKeyPositions.Set(k, true);
                            }
                            k++;
                        }
                    });

                var common = listCustomerKeyPositions.And(listOrderDatePositions).And(listSupplierKeyPositions);
                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PIJ] T1 Time: {0}", t1));
                sw.Reset();
                #endregion Probing Phase


                #region Value Extraction Phase
                sw.Start();
                var joinOutputFinal = new Dictionary<string, Int64>();
                Int32 index = 0;
                foreach (bool bit in common)
                {
                    try
                    {
                        if (bit)
                        {
                            var dateKey = InMemoryData.loOrderDate[index];
                            var custKey = InMemoryData.loCustomerKey[index];
                            var suppKey = InMemoryData.loSupplierKey[index];

                            // Position Look UP
                            string dYear;
                            dateHashTable.TryGetValue(dateKey, out dYear);
                            string cNationOut;
                            customerHashTable.TryGetValue(custKey, out cNationOut);
                            string sNationOut;
                            supplierHashTable.TryGetValue(suppKey, out sNationOut);
                            string key = cNationOut + ", " + sNationOut + ", " + dYear;
                            Int64 revenue = 0;
                            if (joinOutputFinal.TryGetValue(key, out revenue))
                            {
                                joinOutputFinal[key] = revenue + InMemoryData.loRevenue[index];
                            }
                            else
                            {
                                joinOutputFinal.Add(key, InMemoryData.loRevenue[index]);
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
                Console.WriteLine(String.Format("[PIJ] T2 Time: {0}", t2));
                Console.WriteLine(String.Format("[PIJ] Total Time: {0}", t0 + t1 + t2));
                Console.WriteLine(String.Format("[PIJ] Total Count: {0}", joinOutputFinal.Count));
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
                Console.WriteLine(String.Format("[PIJ] T0 Time: {0}", t0));
                #endregion Key Hashing Phase

                #region Probing Phase
                sw.Reset();
                sw.Start();

                var listOrderDatePositions = new BitArray(InMemoryData.loOrderDate.Count);
                var listCustomerKeyPositions = new BitArray(InMemoryData.loCustomerKey.Count);
                var listSupplierKeyPositions = new BitArray(InMemoryData.loSupplierKey.Count);

                Parallel.Invoke(parallelOptions,
                    () =>
                    {
                        var i = 0;
                        foreach (var orderDate in InMemoryData.loOrderDate)
                        {
                            string dYear = "";
                            if (dateHashTable.TryGetValue(orderDate, out dYear))
                            {
                                listOrderDatePositions.Set(i, true);
                            }
                            i++;
                        }
                    },
                    () =>
                    {
                        var j = 0;
                        foreach (var custKey in InMemoryData.loCustomerKey)
                        {
                            string cCityOut = string.Empty;
                            if (customerHashTable.TryGetValue(custKey, out cCityOut))
                            {
                                listCustomerKeyPositions.Set(j, true);
                            }
                            j++;
                        }

                    },
                    () =>
                    {
                        var k = 0;
                        foreach (var suppKey in InMemoryData.loSupplierKey)
                        {
                            string sCityOut = string.Empty;
                            if (supplierHashTable.TryGetValue(suppKey, out sCityOut))
                            {
                                listSupplierKeyPositions.Set(k, true);
                            }
                            k++;
                        }
                    });

                var common = listCustomerKeyPositions.And(listOrderDatePositions).And(listSupplierKeyPositions);
                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PIJ] T1 Time: {0}", t1));
                sw.Reset();
                #endregion Probing Phase


                #region Value Extraction Phase
                sw.Start();
                var joinOutputFinal = new Dictionary<string, long>();
                Int32 index = 0;
                foreach (bool bit in common)
                {
                    try
                    {
                        if (bit)
                        {
                            var dateKey = InMemoryData.loOrderDate[index];
                            var custKey = InMemoryData.loCustomerKey[index];
                            var suppKey = InMemoryData.loSupplierKey[index];

                            string dYear;
                            dateHashTable.TryGetValue(dateKey, out dYear);
                            string cCityOut;
                            customerHashTable.TryGetValue(custKey, out cCityOut);
                            string sCityOut;
                            supplierHashTable.TryGetValue(suppKey, out sCityOut);
                            string key = cCityOut + ", " + sCityOut + ", " + dYear;
                            long revenue = 0;
                            if (joinOutputFinal.TryGetValue(key, out revenue))
                            {
                                joinOutputFinal[key] = revenue + InMemoryData.loRevenue[index];
                            }
                            else
                            {
                                joinOutputFinal.Add(key, InMemoryData.loRevenue[index]);
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
                Console.WriteLine(String.Format("[PIJ] T2 Time: {0}", t2));
                Console.WriteLine(String.Format("[PIJ] Total Time: {0}", t0 + t1 + t2));
                Console.WriteLine(String.Format("[PIJ] Total Count: {0}", joinOutputFinal.Count));
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

                sw.Start();
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
                Console.WriteLine(String.Format("[PIJ] T0 Time: {0}", t0));
                #endregion Key Hashing Phase

                #region Probing Phase
                sw.Reset();
                sw.Start();

                var listOrderDatePositions = new BitArray(InMemoryData.loOrderDate.Count);
                var listCustomerKeyPositions = new BitArray(InMemoryData.loCustomerKey.Count);
                var listSupplierKeyPositions = new BitArray(InMemoryData.loSupplierKey.Count);

                Parallel.Invoke(parallelOptions,
                    () =>
                    {
                        var i = 0;
                        foreach (var orderDate in InMemoryData.loOrderDate)
                        {
                            string dYear = "";
                            if (dateHashTable.TryGetValue(orderDate, out dYear))
                            {
                                listOrderDatePositions.Set(i, true);
                            }
                            i++;
                        }
                    },
                    () =>
                    {
                        var j = 0;
                        foreach (var custKey in InMemoryData.loCustomerKey)
                        {
                            string cCityOut = string.Empty;
                            if (customerHashTable.TryGetValue(custKey, out cCityOut))
                            {
                                listCustomerKeyPositions.Set(j, true);
                            }
                            j++;
                        }

                    },
                    () =>
                    {
                        var k = 0;
                        foreach (var suppKey in InMemoryData.loSupplierKey)
                        {
                            string sCityOut = string.Empty;
                            if (supplierHashTable.TryGetValue(suppKey, out sCityOut))
                            {
                                listSupplierKeyPositions.Set(k, true);
                            }
                            k++;
                        }
                    });

                var common = listCustomerKeyPositions.And(listOrderDatePositions).And(listSupplierKeyPositions);
                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PIJ] T1 Time: {0}", t1));
                sw.Reset();
                #endregion Probing Phase


                #region Value Extraction Phase
                sw.Start();
                var joinOutputFinal = new Dictionary<string, long>();
                Int32 index = 0;
                foreach (bool bit in common)
                {
                    try
                    {
                        if (bit)
                        {
                            var dateKey = InMemoryData.loOrderDate[index];
                            var custKey = InMemoryData.loCustomerKey[index];
                            var suppKey = InMemoryData.loSupplierKey[index];

                            string dYear;
                            dateHashTable.TryGetValue(dateKey, out dYear);
                            string cCityOut;
                            customerHashTable.TryGetValue(custKey, out cCityOut);
                            string sCityOut;
                            supplierHashTable.TryGetValue(suppKey, out sCityOut);
                            string key = cCityOut + ", " + sCityOut + ", " + dYear;
                            long revenue = 0;
                            if (joinOutputFinal.TryGetValue(key, out revenue))
                            {
                                joinOutputFinal[key] = revenue + InMemoryData.loRevenue[index];
                            }
                            else
                            {
                                joinOutputFinal.Add(key, InMemoryData.loRevenue[index]);
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
                Console.WriteLine(String.Format("[PIJ] T2 Time: {0}", t2));
                Console.WriteLine(String.Format("[PIJ] Total Time: {0}", t0 + t1 + t2));
                Console.WriteLine(String.Format("[PIJ] Total Count: {0}", joinOutputFinal.Count));
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

                sw.Start();
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
                Console.WriteLine(String.Format("[PIJ] T0 Time: {0}", t0));
                #endregion Key Hashing Phase

                #region Probing Phase
                sw.Reset();
                sw.Start();

                var listOrderDatePositions = new BitArray(InMemoryData.loOrderDate.Count);
                var listCustomerKeyPositions = new BitArray(InMemoryData.loCustomerKey.Count);
                var listSupplierKeyPositions = new BitArray(InMemoryData.loSupplierKey.Count);

                Parallel.Invoke(parallelOptions,
                    () =>
                    {
                        var i = 0;
                        foreach (var orderDate in InMemoryData.loOrderDate)
                        {
                            string dYear = "";
                            if (dateHashTable.TryGetValue(orderDate, out dYear))
                            {
                                listOrderDatePositions.Set(i, true);
                            }
                            i++;
                        }
                    },
                    () =>
                    {
                        var j = 0;
                        foreach (var custKey in InMemoryData.loCustomerKey)
                        {
                            string cCityOut = string.Empty;
                            if (customerHashTable.TryGetValue(custKey, out cCityOut))
                            {
                                listCustomerKeyPositions.Set(j, true);
                            }
                            j++;
                        }

                    },
                    () =>
                    {
                        var k = 0;
                        foreach (var suppKey in InMemoryData.loSupplierKey)
                        {
                            string sCityOut = string.Empty;
                            if (supplierHashTable.TryGetValue(suppKey, out sCityOut))
                            {
                                listSupplierKeyPositions.Set(k, true);
                            }
                            k++;
                        }
                    });

                var common = listCustomerKeyPositions.And(listOrderDatePositions).And(listSupplierKeyPositions);
                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PIJ] T1 Time: {0}", t1));
                sw.Reset();
                #endregion Probing Phase


                #region Value Extraction Phase
                sw.Start();
                var joinOutputFinal = new Dictionary<string, long>();
                Int32 index = 0;
                foreach (bool bit in common)
                {
                    try
                    {
                        if (bit)
                        {
                            var dateKey = InMemoryData.loOrderDate[index];
                            var custKey = InMemoryData.loCustomerKey[index];
                            var suppKey = InMemoryData.loSupplierKey[index];

                            string dYear;
                            dateHashTable.TryGetValue(dateKey, out dYear);
                            string cCityOut;
                            customerHashTable.TryGetValue(custKey, out cCityOut);
                            string sCityOut;
                            supplierHashTable.TryGetValue(suppKey, out sCityOut);
                            string key = cCityOut + ", " + sCityOut + ", " + dYear;
                            long revenue = 0;
                            if (joinOutputFinal.TryGetValue(key, out revenue))
                            {
                                joinOutputFinal[key] = revenue + InMemoryData.loRevenue[index];
                            }
                            else
                            {
                                joinOutputFinal.Add(key, InMemoryData.loRevenue[index]);
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
                Console.WriteLine(String.Format("[PIJ] T2 Time: {0}", t2));
                Console.WriteLine(String.Format("[PIJ] Total Time: {0}", t0 + t1 + t2));
                Console.WriteLine(String.Format("[PIJ] Total Count: {0}", joinOutputFinal.Count));
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
                Console.WriteLine(String.Format("[PIJ] T0 Time: {0}", t0));
                #endregion Key Hashing Phase

                #region Probing Phase
                sw.Reset();
                sw.Start();

                var listOrderDatePositions = new BitArray(InMemoryData.loOrderDate.Count);
                var listCustomerKeyPositions = new BitArray(InMemoryData.loCustomerKey.Count);
                var listSupplierKeyPositions = new BitArray(InMemoryData.loSupplierKey.Count);
                var listPartKeyPositions = new BitArray(InMemoryData.loPartKey.Count);

                Parallel.Invoke(parallelOptions,
                    () =>
                    {
                        var i = 0;
                        foreach (var orderDate in InMemoryData.loOrderDate)
                        {
                            string dYear = "";
                            if (dateHashTable.TryGetValue(orderDate, out dYear))
                            {
                                listOrderDatePositions.Set(i, true);
                            }
                            i++;
                        }
                    },
                    () =>
                    {
                        var j = 0;
                        foreach (var custKey in InMemoryData.loCustomerKey)
                        {
                            string cNationOut = string.Empty;
                            if (customerHashTable.TryGetValue(custKey, out cNationOut))
                            {
                                listCustomerKeyPositions.Set(j, true);
                            }
                            j++;
                        }

                    },
                    () =>
                    {
                        var k = 0;
                        foreach (var suppKey in InMemoryData.loSupplierKey)
                        {
                            string sNationOut = string.Empty;
                            if (supplierHashTable.TryGetValue(suppKey, out sNationOut))
                            {
                                listSupplierKeyPositions.Set(k, true);
                            }
                            k++;
                        }
                    },

                    () =>
                    {
                        var k = 0;
                        foreach (var partKey in InMemoryData.loPartKey)
                        {
                            string pMFGROut = string.Empty;
                            if (supplierHashTable.TryGetValue(partKey, out pMFGROut))
                            {
                                listPartKeyPositions.Set(k, true);
                            }
                            k++;
                        }
                    });

                var common = listCustomerKeyPositions.And(listOrderDatePositions).And(listSupplierKeyPositions).And(listPartKeyPositions);
                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PIJ] T1 Time: {0}", t1));
                sw.Reset();
                #endregion Probing Phase


                #region Value Extraction Phase
                sw.Start();
                var joinOutputFinal = new Dictionary<string, Int64>();
                Int32 index = 0;
                foreach (bool bit in common)
                {
                    try
                    {
                        if (bit)
                        {
                            var dateKey = InMemoryData.loOrderDate[index];
                            var custKey = InMemoryData.loCustomerKey[index];

                            // Position Look UP
                            string dYear;
                            dateHashTable.TryGetValue(dateKey, out dYear);
                            string cNationOut;
                            customerHashTable.TryGetValue(custKey, out cNationOut);
                            string key = dYear + ", " + cNationOut;
                            Int64 profit = 0;
                            if (joinOutputFinal.TryGetValue(key, out profit))
                            {
                                joinOutputFinal[key] = profit + (InMemoryData.loRevenue[index] - InMemoryData.loSupplyCost[index]);
                            }
                            else
                            {
                                joinOutputFinal.Add(key, (InMemoryData.loRevenue[index] - InMemoryData.loSupplyCost[index]));
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
                Console.WriteLine(String.Format("[PIJ] T2 Time: {0}", t2));
                Console.WriteLine(String.Format("[PIJ] Total Time: {0}", t0 + t1 + t2));
                Console.WriteLine(String.Format("[PIJ] Total Count: {0}", joinOutputFinal.Count));
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
                Console.WriteLine(String.Format("[PIJ] T0 Time: {0}", t0));
                #endregion Key Hashing Phase

                #region Probing Phase
                sw.Reset();
                sw.Start();

                var listOrderDatePositions = new BitArray(InMemoryData.loOrderDate.Count);
                var listCustomerKeyPositions = new BitArray(InMemoryData.loCustomerKey.Count);
                var listSupplierKeyPositions = new BitArray(InMemoryData.loSupplierKey.Count);
                var listPartKeyPositions = new BitArray(InMemoryData.loPartKey.Count);

                Parallel.Invoke(parallelOptions,
                    () =>
                    {
                        var i = 0;
                        foreach (var orderDate in InMemoryData.loOrderDate)
                        {
                            string dYear = "";
                            if (dateHashTable.TryGetValue(orderDate, out dYear))
                            {
                                listOrderDatePositions.Set(i, true);
                            }
                            i++;
                        }
                    },
                    () =>
                    {
                        var j = 0;
                        foreach (var custKey in InMemoryData.loCustomerKey)
                        {
                            string cNationOut = string.Empty;
                            if (customerHashTable.TryGetValue(custKey, out cNationOut))
                            {
                                listCustomerKeyPositions.Set(j, true);
                            }
                            j++;
                        }

                    },
                    () =>
                    {
                        var k = 0;
                        foreach (var suppKey in InMemoryData.loSupplierKey)
                        {
                            string sNationOut = string.Empty;
                            if (supplierHashTable.TryGetValue(suppKey, out sNationOut))
                            {
                                listSupplierKeyPositions.Set(k, true);
                            }
                            k++;
                        }
                    },

                    () =>
                    {
                        var l = 0;
                        foreach (var partKey in InMemoryData.loPartKey)
                        {
                            string pCategoryOut = string.Empty;
                            if (partHashTable.TryGetValue(partKey, out pCategoryOut))
                            {
                                listPartKeyPositions.Set(l, true);
                            }
                            l++;
                        }
                    });

                var common = listCustomerKeyPositions.And(listOrderDatePositions).And(listSupplierKeyPositions).And(listPartKeyPositions);
                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PIJ] T1 Time: {0}", t1));
                sw.Reset();
                #endregion Probing Phase


                #region Value Extraction Phase
                sw.Start();
                var joinOutputFinal = new Dictionary<string, Int64>();
                Int32 index = 0;
                foreach (bool bit in common)
                {
                    try
                    {
                        if (bit)
                        {
                            var dateKey = InMemoryData.loOrderDate[index];
                            var partKey = InMemoryData.loPartKey[index];
                            var suppKey = InMemoryData.loSupplierKey[index];
                            var custKey = InMemoryData.loCustomerKey[index];

                            // Position Look UP
                            string dYear;
                            dateHashTable.TryGetValue(dateKey, out dYear);
                            string pCategoryOut;
                            partHashTable.TryGetValue(partKey, out pCategoryOut);
                            string sNationOut;
                            supplierHashTable.TryGetValue(suppKey, out sNationOut);
                            string key = dYear + ", " + sNationOut + ", " + pCategoryOut;
                            Int64 profit = 0;
                            if (joinOutputFinal.TryGetValue(key, out profit))
                            {
                                joinOutputFinal[key] = profit + (InMemoryData.loRevenue[index] - InMemoryData.loSupplyCost[index]);
                            }
                            else
                            {
                                joinOutputFinal.Add(key, (InMemoryData.loRevenue[index] - InMemoryData.loSupplyCost[index]));
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
                Console.WriteLine(String.Format("[PIJ] T2 Time: {0}", t2));
                Console.WriteLine(String.Format("[PIJ] Total Time: {0}", t0 + t1 + t2));
                Console.WriteLine(String.Format("[PIJ] Total Count: {0}", joinOutputFinal.Count));
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
                Console.WriteLine(String.Format("[PIJ] T0 Time: {0}", t0));
                #endregion Key Hashing Phase

                #region Probing Phase
                sw.Reset();
                sw.Start();

                var listOrderDatePositions = new BitArray(InMemoryData.loOrderDate.Count);
                var listCustomerKeyPositions = new BitArray(InMemoryData.loCustomerKey.Count);
                var listSupplierKeyPositions = new BitArray(InMemoryData.loSupplierKey.Count);
                var listPartKeyPositions = new BitArray(InMemoryData.loPartKey.Count);

                Parallel.Invoke(parallelOptions,
                    () =>
                    {
                        var i = 0;
                        foreach (var orderDate in InMemoryData.loOrderDate)
                        {
                            string dYear = "";
                            if (dateHashTable.TryGetValue(orderDate, out dYear))
                            {
                                listOrderDatePositions.Set(i, true);
                            }
                            i++;
                        }
                    },
                    () =>
                    {
                        var j = 0;
                        foreach (var custKey in InMemoryData.loCustomerKey)
                        {
                            string cNationOut = string.Empty;
                            if (customerHashTable.TryGetValue(custKey, out cNationOut))
                            {
                                listCustomerKeyPositions.Set(j, true);
                            }
                            j++;
                        }

                    },
                    () =>
                    {
                        var k = 0;
                        foreach (var suppKey in InMemoryData.loSupplierKey)
                        {
                            string sCityOut = string.Empty;
                            if (supplierHashTable.TryGetValue(suppKey, out sCityOut))
                            {
                                listSupplierKeyPositions.Set(k, true);
                            }
                            k++;
                        }
                    },

                    () =>
                    {
                        var l = 0;
                        foreach (var partKey in InMemoryData.loPartKey)
                        {
                            string pBrandOut = string.Empty;
                            if (partHashTable.TryGetValue(partKey, out pBrandOut))
                            {
                                listPartKeyPositions.Set(l, true);
                            }
                            l++;
                        }
                    });

                var common = listCustomerKeyPositions.And(listOrderDatePositions).And(listSupplierKeyPositions).And(listPartKeyPositions);
                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[PIJ] T1 Time: {0}", t1));
                sw.Reset();
                #endregion Probing Phase


                #region Value Extraction Phase
                sw.Start();
                var joinOutputFinal = new Dictionary<string, Int64>();
                Int32 index = 0;
                foreach (bool bit in common)
                {
                    try
                    {
                        if (bit)
                        {
                            var dateKey = InMemoryData.loOrderDate[index];
                            var partKey = InMemoryData.loPartKey[index];
                            var suppKey = InMemoryData.loSupplierKey[index];

                            // Position Look UP
                            string dYear;
                            dateHashTable.TryGetValue(dateKey, out dYear);
                            string pBrandOut;
                            partHashTable.TryGetValue(partKey, out pBrandOut);
                            string sCityOut;
                            supplierHashTable.TryGetValue(suppKey, out sCityOut);
                            string key = dYear + ", " + sCityOut + ", " + pBrandOut;
                            Int64 profit = 0;
                            if (joinOutputFinal.TryGetValue(key, out profit))
                            {
                                joinOutputFinal[key] = profit + (InMemoryData.loRevenue[index] - InMemoryData.loSupplyCost[index]);
                            }
                            else
                            {
                                joinOutputFinal.Add(key, (InMemoryData.loRevenue[index] - InMemoryData.loSupplyCost[index]));
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
                Console.WriteLine(String.Format("[PIJ] T2 Time: {0}", t2));
                Console.WriteLine(String.Format("[PIJ] Total Time: {0}", t0 + t1 + t2));
                Console.WriteLine(String.Format("[PIJ] Total Count: {0}", joinOutputFinal.Count));
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
               
                sw.Start();
                #region Key Hashing Phase 

                var customerHashTable = new Dictionary<Int64, Tuple<string, string>>();
                var supplierHashTable = new Dictionary<Int64, Tuple<string, string>>();
                var dateHashTable = new Dictionary<Int64, Tuple<string, string>>();
                var partHashTable = new Dictionary<Int64, Tuple<string, string>>();

                foreach (var row in InMemoryData.dateDimension)
                {
                    if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
                        dateHashTable.Add(row.dDateKey, Tuple.Create(row.dYear, row.dMonth));
                }

                foreach (var row in InMemoryData.customerDimension)
                {
                    if (row.cRegion.Equals("ASIA"))
                        customerHashTable.Add(row.cCustKey, Tuple.Create(row.cNation, row.cRegion));
                }

                foreach (var row in InMemoryData.supplierDimension)
                {
                    if (row.sRegion.Equals("ASIA"))
                        supplierHashTable.Add(row.sSuppKey, Tuple.Create(row.sNation, row.sRegion));
                }



                sw.Stop();
                long t0 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[Invisible Join] GATest T0 Time: {0}", t0));
                #endregion Key Hashing Phase

                #region Probing Phase
                sw.Reset();
                sw.Start();

                var listOrderDatePositions = new BitArray(InMemoryData.loCustomerKey.Count);
                var listCustomerKeyPositions = new BitArray(InMemoryData.loCustomerKey.Count);
                var listSupplierKeyPositions = new BitArray(InMemoryData.loCustomerKey.Count);
                BitArray common = new BitArray(InMemoryData.loCustomerKey.Count); 
                var i = 0;
                var j = 0;
                var k = 0;
                var l = 0;

                j = 0;
                foreach (var custKey in InMemoryData.loCustomerKey)
                {
                    Tuple<string, string> cOut = null;
                    if (customerHashTable.TryGetValue(custKey, out cOut))
                    {
                        listCustomerKeyPositions.Set(j, true);
                    }
                    j++;
                }

                k = 0;
                foreach (var suppKey in InMemoryData.loSupplierKey)
                {
                    Tuple<string, string> sOut = null;
                    if (supplierHashTable.TryGetValue(suppKey, out sOut))
                    {
                        listSupplierKeyPositions.Set(k, true);
                    }
                    k++;
                }

                i = 0;
                foreach (var orderDate in InMemoryData.loOrderDate)
                {
                    Tuple<string, string> dOut = null;
                    if (dateHashTable.TryGetValue(orderDate, out dOut))
                    {
                        listOrderDatePositions.Set(i, true);
                    }
                    i++;
                }

                common = listCustomerKeyPositions.And(listSupplierKeyPositions).And(listOrderDatePositions);


                sw.Stop();
                long t1 = sw.ElapsedMilliseconds;
                Console.WriteLine(String.Format("[Invisible Join] GSTest T1 Time: {0}", t1));
                sw.Reset();
                #endregion Probing Phase

                #region Value Extraction Phase
                sw.Start();
                var joinOutputFinal = new Dictionary<string, long>();
                Int32 index = 0;
                Int64 setBitCount = 0;
                foreach (bool bit in common)
                {
                    try
                    {
                        if (bit)
                        {
                            setBitCount++;
                            Int64 dateKey = 0;
                            Int64 custKey = 0;
                            Int64 suppKey = 0;
                            Tuple<string, string> cOut;
                            Tuple<string, string> sOut;
                            Tuple<string, string> dOut;
                            string key = string.Empty;

                            dateKey = InMemoryData.loOrderDate[index];
                            custKey = InMemoryData.loCustomerKey[index];
                            suppKey = InMemoryData.loSupplierKey[index];
                            // Position Look UP
                            dateHashTable.TryGetValue(dateKey, out dOut);
                            customerHashTable.TryGetValue(custKey, out cOut);
                            supplierHashTable.TryGetValue(suppKey, out sOut);

                            switch (numberOfGroupingAttributes)
                            {
                                case 1:
                                    key = cOut.Item1;
                                    break;
                                case 2:
                                    key = cOut.Item1 + ", " + cOut.Item2;
                                    break;
                                case 3:
                                    key = cOut.Item1 + ", " + cOut.Item2 + ", " + sOut.Item1;
                                    break;
                                case 4:
                                    key = cOut.Item1 + ", " + cOut.Item2 + ", " + sOut.Item1 + ", " + sOut.Item2;
                                    break;
                                case 5:
                                    key = cOut.Item1 + ", " + cOut.Item2 + ", " + sOut.Item1 + ", " + sOut.Item2 + ", " + dOut.Item1;
                                    break;
                                case 6:
                                    key = cOut.Item1 + ", " + cOut.Item2 + ", " + sOut.Item1 + ", " + sOut.Item2 + ", " + dOut.Item1 + ", " + dOut.Item2;
                                    break;
                                default:
                                    break;
                            }

                            long tax = 0;
                            if (joinOutputFinal.TryGetValue(key, out tax))
                            {
                                joinOutputFinal[key] = tax + InMemoryData.loRevenue[index];
                            }
                            else
                            {
                                joinOutputFinal.Add(key, InMemoryData.loRevenue[index]);
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
                Console.WriteLine(String.Format("[Invisible Join] GSTest T2 Time: {0}", t2));
                Console.WriteLine(String.Format("[Invisible Join] GSTest Total Time: {0}", t0 + t1 + t2));
                // Console.WriteLine(String.Format("[Invisible Join] GSTest Set BIT Count : {0}", setBitCount));
                Console.WriteLine(String.Format("[Invisible Join] GSTest Total : {0}", joinOutputFinal.Count));
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
            // TestResultsDatabase.pInvisibleJoinOutput.Add(testResults.toString());
            // Console.WriteLine("Parallel Invisible: " + testResults.toString());
            // Console.WriteLine();
            TestResultsDatabase.pInvisibleJoinOutput.Add(testResults.toString());
            
        }
    }
}
