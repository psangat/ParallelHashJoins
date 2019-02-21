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
        private static readonly ParallelOptions parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = 10 };

        private static void Main()
        {
            BinaryFileCreator algo = new BinaryFileCreator("SF 1");
            algo.CreateBinaryFiles();
        }

        //static void Main()
        //{
        //    string scaleFactor = "SF 3";
        //    int numberOfProcessor = 1;
        //    for (int i = 0; i < 3; i++)
        //    {
        //        Console.WriteLine("Iteration " + (i + 1));
        //        Console.WriteLine();

        //        //InvisibleJoin InJ = new InvisibleJoin(scaleFactor);
        //        //InJ.Query_3_1_IM();
        //        //GC.Collect(2, GCCollectionMode.Forced, true);
        //        //Thread.Sleep(100);

        //        //ParallelInvisibleJoin PInJ = new ParallelInvisibleJoin(scaleFactor, i + 1);
        //        //PInJ.Query_3_4_IM();
        //        //GC.Collect(2, GCCollectionMode.Forced, true);
        //        //Thread.Sleep(100);
        //        //PInJ.Query_2_2_IM();
        //        //GC.Collect(2, GCCollectionMode.Forced, true);
        //        //Thread.Sleep(100);
        //        //PInJ.Query_2_3_IM();
        //        //GC.Collect(2, GCCollectionMode.Forced, true);
        //        //Thread.Sleep(100);
        //        //PInJ.Query_3_1_IM();
        //        //GC.Collect(2, GCCollectionMode.Forced, true);
        //        //Thread.Sleep(100);
        //        //PInJ.Query_3_2_IM();
        //        //GC.Collect(2, GCCollectionMode.Forced, true);
        //        //Thread.Sleep(100);
        //        //PInJ.Query_3_3_IM();
        //        //GC.Collect(2, GCCollectionMode.Forced, true);
        //        //Thread.Sleep(100);
        //        //PInJ.Query_3_4_IM();
        //        //GC.Collect(2, GCCollectionMode.Forced, true);
        //        //Thread.Sleep(100);
        //        //PInJ.Query_4_1_IM();
        //        //GC.Collect(2, GCCollectionMode.Forced, true);
        //        //Thread.Sleep(100);
        //        //PInJ.Query_4_2_IM();
        //        //GC.Collect(2, GCCollectionMode.Forced, true);
        //        //Thread.Sleep(100);
        //        //PInJ.Query_4_3_IM();
        //        //GC.Collect(2, GCCollectionMode.Forced, true);
        //        //Thread.Sleep(100);

        //        ////InJ.AggregationScalabilityTest2(i + 1);
        //        ////GC.Collect(2, GCCollectionMode.Forced, true);
        //        ////Thread.Sleep(100);

        //        //NimbleJoin NJ = new NimbleJoin(scaleFactor);
        //        //NJ.GroupingAttributeScalabilityTest(i + 1);
        //        //GC.Collect(2, GCCollectionMode.Forced, true);
        //        //Thread.Sleep(100);

        //        //InMemoryAggregation IMA = new InMemoryAggregation(scaleFactor);
        //        //IMA.GroupingAttributeScalabilityTest(i + 1);
        //        //GC.Collect(2, GCCollectionMode.Forced, true);
        //        //Thread.Sleep(100);

        //        //AtireJoin AJ = new AtireJoin(scaleFactor);
        //        //AJ.Query_3_1();
        //        //GC.Collect(2, GCCollectionMode.Forced, true);
        //        //Thread.Sleep(100);

        //        //AtireJoin ATireJoin = new AtireJoin(scaleFactor);
        //        //ATireJoin.GroupingAttributeScalabilityTest(i + 1);
        //        //GC.Collect(2, GCCollectionMode.Forced, true);
        //        //Thread.Sleep(100);



        //        ParallelNimbleJoin PNJ = new ParallelNimbleJoin(scaleFactor, i + 1);
        //        PNJ.Query_2_3_IM();
        //        GC.Collect(2, GCCollectionMode.Forced, true);
        //        Thread.Sleep(100);

        //        ParallelInMemoryAggregation PIMA = new ParallelInMemoryAggregation(scaleFactor, i + 1);
        //        PIMA.Query_2_3_IM();
        //        GC.Collect(2, GCCollectionMode.Forced, true);
        //        Thread.Sleep(100);

        //        ParallelAtireJoin PATireJoin = new ParallelAtireJoin(scaleFactor, i + 1);
        //        PATireJoin.Query_2_3_IM(true);
        //        GC.Collect(2, GCCollectionMode.Forced, true);
        //        //Thread.Sleep(100);
        //        //Console.WriteLine("Was using lock");
        //        //Console.WriteLine();

        //        //PATireJoin.Query_3_1(true);
        //        //GC.Collect(2, GCCollectionMode.Forced, true);
        //        //Thread.Sleep(100);
        //        //Console.WriteLine("Wasnt using lock");
        //        //Console.WriteLine();

        //    }
        //    Console.ReadKey();
        //}

        //private static void Main()
        //{
        //    RunAllTests("SF 25", 12);
        //}

        public static void RunAllTests(string scaleFactor, int processorCount)
        {

            for (int i = 2; i <= 4; i++)
            {
                for (int j = 1; j <= 4; j++)
                {
                    for (int k = 1; k <= 1; k++)
                    {
                        if (i == 3 && j == 4)
                        {
                            runQueries(scaleFactor, processorCount, i, j, k);
                        }
                        else if (j == 4)
                        {
                            // do nth
                        }
                        else
                        {
                            runQueries(scaleFactor, processorCount, i, j, k);
                        }
                    }
                    // printResultsToFile(String.Format("{0}_Q{1}{2}_PC{3}.txt", scaleFactor, i, j, processorCount));
                    Thread.Sleep(10000);
                }
            }
        }

        private static void runQueries(string scaleFactor, int processorCount, int i, int j, int k)
        {
            Console.WriteLine(string.Format("Run #{0} for Query {1}.{2}", k, i, j));
            Console.WriteLine();

            Invoker.CreateAndInvoke("ParallelHashJoins.ParallelInvisibleJoin", new object[] { scaleFactor, processorCount }, string.Format("Query_{0}_{1}_IM", i, j), null);
            GC.Collect(2, GCCollectionMode.Forced, true);
            Thread.Sleep(TIMETOSLEEP);
            Console.WriteLine();

            Invoker.CreateAndInvoke("ParallelHashJoins.ParallelNimbleJoin", new object[] { scaleFactor, processorCount }, string.Format("Query_{0}_{1}_IM", i, j), null);
            GC.Collect(2, GCCollectionMode.Forced, true);
            Thread.Sleep(TIMETOSLEEP);
            Console.WriteLine();

            Invoker.CreateAndInvoke("ParallelHashJoins.ParallelInMemoryAggregation", new object[] { scaleFactor, processorCount }, string.Format("Query_{0}_{1}_IM", i, j), null);
            GC.Collect(2, GCCollectionMode.Forced, true);
            Thread.Sleep(TIMETOSLEEP);

            Invoker.CreateAndInvoke("ParallelHashJoins.ParallelAtireJoin", new object[] { scaleFactor, processorCount }, string.Format("Query_{0}_{1}_IM", i, j), new object[] { true });
            GC.Collect(2, GCCollectionMode.Forced, true);
            Thread.Sleep(TIMETOSLEEP);

            Console.WriteLine("============================================================================");
            Console.WriteLine();
        }

        public static void printResultsToFile(string fileName)
        {
            ////string fileName = scaleFactor + "_Q31.txt";
            //string today = DateTime.Now.ToString("dd MMMM yyyy");
            //string nimbleJoinOutputFilePath = @"Results\" + today + "\\Nimble Join\\";
            //if (!Directory.Exists(nimbleJoinOutputFilePath))
            //    Directory.CreateDirectory(nimbleJoinOutputFilePath);
            //System.IO.File.WriteAllLines(nimbleJoinOutputFilePath + fileName, TestResultsDatabase.nimbleJoinOutput);

            //string invisibleJoinOutputFilePath = @"Results\" + today + "\\Invisible Join\\";
            //if (!Directory.Exists(invisibleJoinOutputFilePath))
            //    Directory.CreateDirectory(invisibleJoinOutputFilePath);
            //System.IO.File.WriteAllLines(invisibleJoinOutputFilePath + fileName, TestResultsDatabase.invisibleJoinOutput);

            //string pNimbleJoinOutputFilePath = @"Results\" + today + "\\Parallel Nimble Join\\";
            //if (!Directory.Exists(pNimbleJoinOutputFilePath))
            //    Directory.CreateDirectory(pNimbleJoinOutputFilePath);
            //System.IO.File.WriteAllLines(pNimbleJoinOutputFilePath + fileName, TestResultsDatabase.pNimbleJoinOutput);

            //string pInvisibleJoinOutputFilePath = @"Results\" + today + "\\Parallel Invisible Join\\";
            //if (!Directory.Exists(pInvisibleJoinOutputFilePath))
            //    Directory.CreateDirectory(pInvisibleJoinOutputFilePath);
            //System.IO.File.WriteAllLines(pInvisibleJoinOutputFilePath + fileName, TestResultsDatabase.pInvisibleJoinOutput);

            //TestResultsDatabase.clearAllDatabase();
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
