using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ParallelHashJoins
{
    internal class main
    {
        private const int TIMETOSLEEP = 1000;
        private static readonly ParallelOptions parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = 14 };
        private static readonly string scaleFactor = "SF 1";


        private static void Main()
        {
            List<string> scaleFactors = new List<string>() {"SF 25", "SF 50", "SF 75", "SF 100" };
            List<int> noOfCores = new List<int>() { 2, 4, 6, 8, 10, 12, 14 }; //, 14

            Console.Write(string.Format("[{0}] Enter the Test Type <D for DATASIZE, N for NOOFCORES, S for SCALABILITY>, su for SCALEUP: ", DateTime.Now));
            string testType = Console.ReadLine().ToUpper();

            switch (testType)
            {
                case "D":
                    Console.Write(string.Format("[{0}] Enter the Scale Factor(SF) <SF 1, SF 25, SF 50, SF 75, SF 100>: ", DateTime.Now));
                    string scaleFactor = Console.ReadLine().ToUpper();

                    Console.WriteLine(string.Format("[{0}] Loading data into memory using {1} cores.", DateTime.Now, parallelOptions.MaxDegreeOfParallelism));
                    Stopwatch sw = Stopwatch.StartNew();
                    InMemoryData inMemoryData = new InMemoryData(scaleFactor, parallelOptions);
                    sw.Stop();
                    Console.WriteLine(string.Format("[{0}] Time to load {1}: {2} ms", DateTime.Now, scaleFactor, sw.ElapsedMilliseconds));
                    RunAllTests(scaleFactor, parallelOptions);
                    break;
                case "N":
                    Console.WriteLine(string.Format("[{0}] Running test on SF 100 dataset.", DateTime.Now));
                    Console.WriteLine(string.Format("[{0}] Loading data into memory using {0} cores.", DateTime.Now, parallelOptions.MaxDegreeOfParallelism));
                    Stopwatch sw1 = Stopwatch.StartNew();
                    InMemoryData inMemoryData1 = new InMemoryData("SF 100", parallelOptions);
                    sw1.Stop();
                    Console.WriteLine(string.Format("[{0}] Time to load {1}: {2} ms", DateTime.Now, "SF 100", sw1.ElapsedMilliseconds));

                    foreach (int noOfCore in noOfCores)
                    {
                        RunAllTests("SF 100", new ParallelOptions { MaxDegreeOfParallelism = noOfCore });
                    }
                    break;
                case "S":
                    Console.Write(string.Format("[{0}] Enter the Scale Factor(SF) <1, 25, 50, 75, 100>: ", DateTime.Now));
                    string sf = "SF " +  Console.ReadLine().ToUpper();
                    Console.WriteLine(string.Format("[{0}] Loading data into memory using {1} cores.", DateTime.Now, parallelOptions.MaxDegreeOfParallelism));
                    Stopwatch sw2 = Stopwatch.StartNew();
                    InMemoryData inMemoryData2 = new InMemoryData(sf, parallelOptions);
                    sw2.Stop();
                    Console.WriteLine(string.Format("[0] Time to load {1}: {2} ms", DateTime.Now, sf, sw2.ElapsedMilliseconds));
                    RunGroupingScalabilityTest(sf);
                    break;
                case "SU":
                    RunScaleUpTest();
                    break;
                default:
                    break;
            }
            Console.WriteLine(string.Format("[{0}] {1} test completed.", DateTime.Now, testType));
            Console.ReadKey();
        }

        public static void RunScaleUpTest() {
            List<string> scaleFactors = new List<string>() { "SF 1" ,"SF 25", "SF 50", "SF 75", "SF 100" };
            List<int> noOfCores = new List<int>() { 1, 4, 8 , 12, 16 }; //, 14
            for (int i = 0; i < 5; i++)
            {
                Console.WriteLine(string.Format("[{0}] Loading data into memory using {1} cores.", DateTime.Now, parallelOptions.MaxDegreeOfParallelism));
                Stopwatch sw2 = Stopwatch.StartNew();
                InMemoryData inMemoryData = new InMemoryData(scaleFactors[i], parallelOptions);
                sw2.Stop();
                Console.WriteLine(string.Format("[0] Time to load {1}: {2} ms", DateTime.Now, scaleFactors[i], sw2.ElapsedMilliseconds));
                for (int j = 1; j <= 21; j++) // Number of iterations
                {
                    runGroupingScalabilityTest(j, 10, scaleFactor, noOfCores[i]);
                }
                printResultsToFile(string.Format("{0}_GroupingScalabilityTest_{1}_PC{2}.txt", scaleFactor, 10, noOfCores[i]));
                Thread.Sleep(10000);
            }
        }

        public static void RunGroupingScalabilityTest(string scaleFactor)
        {

            for (int i = 1; i <= 10; i++) // No of grouping attributes
            {
                for (int j = 1; j <= 21; j++) // Number of iterations
                {
                    runGroupingScalabilityTest(j, i, scaleFactor);
                }
                printResultsToFile(string.Format("{0}_GroupingScalabilityTest_{1}_PC{2}.txt", scaleFactor, i, parallelOptions.MaxDegreeOfParallelism));
                Thread.Sleep(10000);
            }
        }

        public static void RunAllTests(string scaleFactor, ParallelOptions parallelOptions)
        {
            for (int i = 2; i <= 4; i++)
            {
                for (int j = 1; j <= 4; j++)
                {
                    for (int k = 1; k <= 21; k++)
                    {
                        if (i == 3 && j == 4)
                        {
                            runQueries(scaleFactor, parallelOptions, i, j, k);
                        }
                        else if (j == 4)
                        {
                            // do nth
                        }
                        else
                        {
                            runQueries(scaleFactor, parallelOptions, i, j, k);
                           
                        }
                    }
                    printResultsToFile(string.Format("{0}_Q{1}{2}_PC{3}.txt", scaleFactor, i, j, parallelOptions.MaxDegreeOfParallelism));
                    Thread.Sleep(10000);
                }
            }
            // Run only 3.1 for LF vs LC
            //for (int k = 1; k <= 21; k++)
            //{

            //        runQueries(scaleFactor, parallelOptions, 3, 1, k);

            //}
            //printResultsToFile(string.Format("{0}_LFvsLC_Q{1}{2}_PC{3}.txt", scaleFactor, 3, 1, parallelOptions.MaxDegreeOfParallelism));
            //Thread.Sleep(10000);
        }

        private static void runQueries(string scaleFactor, ParallelOptions parallelOptions, int i, int j, int k, bool isLockFree = true)
        {
            Console.WriteLine(string.Format("Run #{0} for Query {1}.{2}", k, i, j));
            Console.WriteLine();

            //Invoker.CreateAndInvoke("ParallelHashJoins.ParallelInvisibleJoin", new object[] { scaleFactor, parallelOptions }, string.Format("Query_{0}_{1}_IM", i, j), null);
            //GC.Collect(2, GCCollectionMode.Forced, true);
            //Thread.Sleep(TIMETOSLEEP);
            //Console.WriteLine();

            //Invoker.CreateAndInvoke("ParallelHashJoins.ParallelNimbleJoin", new object[] { scaleFactor, parallelOptions }, string.Format("Query_{0}_{1}_IM", i, j), null);
            //GC.Collect(2, GCCollectionMode.Forced, true);
            //Thread.Sleep(TIMETOSLEEP);
            //Console.WriteLine();

            //Invoker.CreateAndInvoke("ParallelHashJoins.ParallelInMemoryAggregation", new object[] { scaleFactor, parallelOptions }, string.Format("Query_{0}_{1}_IM", i, j), null);
            //GC.Collect(2, GCCollectionMode.Forced, true);
            //Thread.Sleep(TIMETOSLEEP);

            Invoker.CreateAndInvoke("ParallelHashJoins.ParallelAtireJoin", new object[] { scaleFactor, parallelOptions, true }, string.Format("Query_{0}_{1}_IM", i, j), new object[] { true });
            GC.Collect(2, GCCollectionMode.Forced, true);
            Thread.Sleep(TIMETOSLEEP);

            Invoker.CreateAndInvoke("ParallelHashJoins.ParallelAtireJoin", new object[] { scaleFactor, parallelOptions, false }, string.Format("Query_{0}_{1}_IM", i, j), new object[] { false });
            GC.Collect(2, GCCollectionMode.Forced, true);
            Thread.Sleep(TIMETOSLEEP);

            Console.WriteLine("============================================================================");
            Console.WriteLine();
        }

        private static void runGroupingScalabilityTest(int iterationNumber, int noOfGroupingAttributes, string scaleFactor, int noOfCores = 14 )
        {
            ParallelOptions parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = noOfCores };

            Console.WriteLine(string.Format("Run #{0} for {1} Grouping Attributes", iterationNumber, noOfGroupingAttributes));
            Console.WriteLine();

            Invoker.CreateAndInvoke("ParallelHashJoins.ParallelInvisibleJoin", new object[] { scaleFactor, parallelOptions }, "GroupingAttributeScalabilityTest", new object[] { noOfGroupingAttributes });
            GC.Collect(2, GCCollectionMode.Forced, true);
            Thread.Sleep(TIMETOSLEEP);
            Console.WriteLine();

            Invoker.CreateAndInvoke("ParallelHashJoins.ParallelNimbleJoin", new object[] { scaleFactor, parallelOptions }, "GroupingAttributeScalabilityTest", new object[] { noOfGroupingAttributes });
            GC.Collect(2, GCCollectionMode.Forced, true);
            Thread.Sleep(TIMETOSLEEP);
            Console.WriteLine();

            Invoker.CreateAndInvoke("ParallelHashJoins.ParallelInMemoryAggregation", new object[] { scaleFactor, parallelOptions }, "GroupingAttributeScalabilityTest", new object[] { noOfGroupingAttributes });
            GC.Collect(2, GCCollectionMode.Forced, true);
            Thread.Sleep(TIMETOSLEEP);

            Invoker.CreateAndInvoke("ParallelHashJoins.ParallelAtireJoin", new object[] { scaleFactor, parallelOptions, true }, "GroupingAttributeScalabilityTest", new object[] { noOfGroupingAttributes });
            GC.Collect(2, GCCollectionMode.Forced, true);
            Thread.Sleep(TIMETOSLEEP);

            Console.WriteLine("============================================================================");
            Console.WriteLine();
        }

        public static void printResultsToFile(string fileName)
        {
            //string fileName = scaleFactor + "_Q31.txt";
            string today = DateTime.Now.ToString("dd MMMM yyyy");

            string pInvisibleJoinOutputFilePath = @"Results\" + today + "\\Parallel Invisible Join\\";
            if (!Directory.Exists(pInvisibleJoinOutputFilePath))
            {
                Directory.CreateDirectory(pInvisibleJoinOutputFilePath);
            }

            System.IO.File.WriteAllLines(pInvisibleJoinOutputFilePath + fileName, TestResultsDatabase.pInvisibleJoinOutput);

            string pNimbleJoinOutputFilePath = @"Results\" + today + "\\Parallel Nimble Join\\";
            if (!Directory.Exists(pNimbleJoinOutputFilePath))
            {
                Directory.CreateDirectory(pNimbleJoinOutputFilePath);
            }

            System.IO.File.WriteAllLines(pNimbleJoinOutputFilePath + fileName, TestResultsDatabase.pNimbleJoinOutput);

            string pInMemoryAggregationOutputFilePath = @"Results\" + today + "\\Parallel InMemoryAggregation\\";
            if (!Directory.Exists(pInMemoryAggregationOutputFilePath))
            {
                Directory.CreateDirectory(pInMemoryAggregationOutputFilePath);
            }

            System.IO.File.WriteAllLines(pInMemoryAggregationOutputFilePath + fileName, TestResultsDatabase.pInMemoryAggregationOutput);

            string pATireJoinOutputLFFilePath = @"Results\" + today + "\\Parallel ATire Join LF\\";
            if (!Directory.Exists(pATireJoinOutputLFFilePath))
            {
                Directory.CreateDirectory(pATireJoinOutputLFFilePath);
            }

            System.IO.File.WriteAllLines(pATireJoinOutputLFFilePath + fileName, TestResultsDatabase.pATireJoinOutputLF);

            string pATireJoinOutputLCFilePath = @"Results\" + today + "\\Parallel ATire Join LC\\";
            if (!Directory.Exists(pATireJoinOutputLCFilePath))
            {
                Directory.CreateDirectory(pATireJoinOutputLCFilePath);
            }

            System.IO.File.WriteAllLines(pATireJoinOutputLCFilePath + fileName, TestResultsDatabase.pATireJoinOutputLC);

            TestResultsDatabase.clearAllDatabase();
        }

        public static void MultiAttributeAssociativeArrayTest()
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            int totalSize = 300000;
            long memoryStart1 = GC.GetTotalMemory(true);
            MAAT _maaa = new MAAT(totalSize);
            for (int i = 0; i < totalSize; i++)
            {
                if (i % 2 == 0)
                {
                    Record values = new Record
                    {
                        s1 = "X"
                    };
                    _maaa.AddOrUpdate(i, values);
                }

            }
            long memoryUsed1 = GC.GetTotalMemory(true) - memoryStart1;
            sw.Stop();
            Console.WriteLine("Insert Time MAAA: " + sw.ElapsedMilliseconds + " ms, Memory Used: " + memoryUsed1);
            sw.Reset();
            sw.Restart();
            long memoryStart2 = GC.GetTotalMemory(true);
            Hashtable _ht = new Hashtable(totalSize);
            for (int i = 0; i < totalSize; i++)
            {
                if (i % 2 == 0)
                {
                    Record values = new Record
                    {
                        s1 = "X"
                    };
                    _ht.Add(i, values);
                }
            }
            long memoryUsed2 = GC.GetTotalMemory(true) - memoryStart2;
            sw.Stop();
            Console.WriteLine("Insert Time HashTable: " + sw.ElapsedMilliseconds + " ms, Memory Used: " + memoryUsed2);
            sw.Reset();
            sw.Start();
            for (int i = 0; i < totalSize; i++)
            {
                if (i % 2 == 0)
                {
                    Record value1 = _maaa.GetValue(i);
                }
            }
            sw.Stop();
            Console.WriteLine("Retrieve Time MAAA: " + sw.ElapsedMilliseconds + " ms");

            sw.Start();
            for (int i = 0; i < totalSize; i++)
            {
                if (i % 2 == 0)
                {
                    Record value2 = (Record)_ht[i];
                }
            }
            sw.Stop();
            Console.WriteLine("Retrieve Time HashTable: " + sw.ElapsedMilliseconds + " ms");

            sw.Reset();
            sw.Start();
            for (int i = 0; i < totalSize; i++)
            {
                if (i % 2 == 0)
                {
                    _maaa.Remove(i);
                }
            }
            sw.Stop();
            Console.WriteLine("Remove Time MAAA: " + sw.ElapsedMilliseconds + " ms");

            sw.Start();
            for (int i = 0; i < totalSize; i++)
            {
                if (i % 2 == 0)
                {
                    _ht.Remove(i);
                }
            }
            sw.Stop();
            Console.WriteLine("Remove Time HashTable: " + sw.ElapsedMilliseconds + " ms");
        }

        public static void ATireTest()
        {
            //List<List<string>> data1 = new List<List<string>>();
            //List<List<string>> data2 = new List<List<string>>();

            //List<string> item1 = new List<string>() { "1", "2", "3" };
            //List<string> item2 = new List<string>() { "1", "2", "4" };
            //List<string> item3 = new List<string>() { "1", "3", "4" };
            //List<string> item4 = new List<string>() { "1", "2", "3" };
            //List<string> item5 = new List<string>() { "3", "2", "3" };

            //List<string> item6 = new List<string>() { "1", "2", "3" };
            //List<string> item7 = new List<string>() { "2", "3", "4" };
            ////List<string> item8 = new List<string>() { "1", "2", "3" };
            //List<string> item9 = new List<string>() { "1", "3", "3" };
            //List<string> item10 = new List<string>() { "1", "3", "3" };


            //data1.Add(item1);
            //data1.Add(item2);
            ////data1.Add(item3);
            ////data1.Add(item4);
            ////data1.Add(item5);

            ////data2.Add(item8);
            //data2.Add(item6);
            //data2.Add(item7);

            ////data2.Add(item9);
            ////data2.Add(item10);

            //Atire tire1 = new Atire();
            //foreach (var item in data1)
            //{
            //    tire1.Insert(tire1, item, 10);
            //}

            //Atire tire2 = new Atire();
            //foreach (var item in data2)
            //{
            //    tire2.Insert(tire2, item, 5);
            //}

            //var merged = tire2.MergeAtires(tire1, tire2);
            //merged.GetResults(merged);
            ////Atire atire = new Atire();
            ////tire1.MergeAtires(atire, tire1, tire2);



            //var chunkIndexes = Utils.getPartitionIndexes(1150, 4);
            //Console.ReadKey();
            //Console.ReadKey();
        }

        //public static void selectivityTest(string scaleFactor)
        //{
        //    //"SF1", "SF2", "SF3", "SF4"
        // "0.007", "0.07", "0.7"
        //    List<string> selectivityRatios = new List<string>() { "0.07" };
        //    foreach (var selectivityRatio in selectivityRatios)
        //    {
        //        for (int i = 0; i < 20; i++)
        //        {
        //            ParallelNimbleJoin pnimbleJoin = new ParallelNimbleJoin(scaleFactor, 4);
        //            pnimbleJoin.Query_3_1(selectivityRatio);
        //            GC.Collect(2, GCCollectionMode.Forced, true);
        //            Thread.Sleep(100);
        //        }
        //    }
        //}
    }
}
