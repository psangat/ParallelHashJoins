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
    class main
    {
        private const int TIMETOSLEEP = 1000;

        static void Main1()
        {

            int[,,,] array = new int[4, 4, 4, 4];
            array[0, 0, 0, 0] = 1;
            array[3, 3, 3, 3] = 1;

            for (int i = 0; i < 4; i++)
            {
                for (int j = 0; j < 4; j++)
                {
                    for (int k = 0; k < 4; k++)
                    {
                        for (int l = 0; l < 4; l++)
                        {
                            Console.WriteLine(i + "," + j + "," + k + "," + l + " : " + array[i, j, k, l]);

                        }
                    }

                }

            }
            Console.Read();
        }
        static void Main()
        {

            List<List<string>> data1 = new List<List<string>>();
            List<List<string>> data2 = new List<List<string>>();

            List<string> item1 = new List<string>() { "1", "2", "3" };
            List<string> item2 = new List<string>() { "1", "2", "4" };
            List<string> item3 = new List<string>() { "1", "3", "4" };
            List<string> item4 = new List<string>() { "1", "2", "3" };
            List<string> item5 = new List<string>() { "3", "2", "3" };

            List<string> item6 = new List<string>() { "1", "2", "3" };
            List<string> item7 = new List<string>() { "2", "3", "4" };
            //List<string> item8 = new List<string>() { "1", "2", "3" };
            List<string> item9 = new List<string>() { "1", "3", "3" };
            List<string> item10 = new List<string>() { "1", "3", "3" };


            data1.Add(item1);
            data1.Add(item2);
            //data1.Add(item3);
            //data1.Add(item4);
            //data1.Add(item5);

            //data2.Add(item8);
            //data2.Add(item6);
            //data2.Add(item7);
           
            //data2.Add(item9);
            //data2.Add(item10);

            Atire tire1 = new Atire();
            foreach (var item in data1)
            {
                tire1.Insert(tire1, item, 10);
            }

            //Atire tire2 = new Atire();
            //foreach (var item in data2)
            //{
            //    tire2.Insert(tire2, item, 5);
            //}

            //var merged = tire2.MergeAtires(tire1, tire2);
            tire1.GetResults(tire1);
            //Atire atire = new Atire();
            //tire1.MergeAtires(atire, tire1, tire2);

            Console.ReadKey();

            var chunkIndexes = Utils.getPartitionIndexes(1150, 4);
            Console.ReadKey();
            //for (int i = 0; i < 5; i++)
            //{
            //    DictionaryGroupByJoin DGJoin = new DictionaryGroupByJoin("SF3");

            //    DGJoin.Query_3_1_TRIE();
            //    GC.Collect(2, GCCollectionMode.Forced, true);
            //    Thread.Sleep(100);


            //    ParallelAtire aTireJoin = new ParallelAtire("SF3", 3);
            //    aTireJoin.Query_3_1();
            //    GC.Collect(2, GCCollectionMode.Forced, true);
            //    Thread.Sleep(100);

            //    InMemoryAggregation IMA = new InMemoryAggregation("SF3");
            //    IMA.Query_3_1();
            //    GC.Collect(2, GCCollectionMode.Forced, true);
            //    Thread.Sleep(100);

            //    ParallelInMemoryAggregation PIMA = new ParallelInMemoryAggregation("SF3", 3);
            //    PIMA.Query_3_1();
            //    GC.Collect(2, GCCollectionMode.Forced, true);
            //    Thread.Sleep(100);
            //}


            //List<int> l1 = new List<int>() { 1,3,5,7,9};
            //List<int> l2 = new List<int>() { 2, 4, 6, 8, 10, 9 };

            //InMemoryAggregation im = new InMemoryAggregation();
            ////im.generateSmallSampleData();
            //im.generateBigSampleData();

            //for (int i = 0; i < 5; i++)
            //{
            //    //im.IMA_Simple_V2();
            //    //GC.Collect(2, GCCollectionMode.Forced, true);
            //    //Thread.Sleep(100);

            //    im.IMA_V3();
            //    GC.Collect(2, GCCollectionMode.Forced, true);
            //    Thread.Sleep(100);


            //    im.IMA_V3_Parallel();
            //    GC.Collect(2, GCCollectionMode.Forced, true);
            //    Thread.Sleep(100);
            //    //im.ABC();
            //    //GC.Collect(2, GCCollectionMode.Forced, true);
            //    //Thread.Sleep(100);
            //    im.ABC_V3();
            //    GC.Collect(2, GCCollectionMode.Forced, true);
            //    Thread.Sleep(100);

            //    im.ABC_V3_Parallel();
            //    GC.Collect(2, GCCollectionMode.Forced, true);
            //    Thread.Sleep(100);

            //    // Failed Experiment
            //    //im.ABC_V5();
            //    //GC.Collect(2, GCCollectionMode.Forced, true);
            //    //Thread.Sleep(100);

            //    //im.ABC_V3();
            //    //GC.Collect(2, GCCollectionMode.Forced, true);
            //    //Thread.Sleep(100);
            //}
            Console.ReadKey();
        }

        //static void Main(string[] args)
        //{
        //    //for (int i = 0; i < 10; i++)
        //    //{
        //    //    Console.WriteLine("===============================================================================");
        //    //    MultiAttributeAssociativeArrayTest();
        //    //    Console.WriteLine("===============================================================================");
        //    //}
        //    string scaleFactor = string.Empty;
        //    if (args.Length <= 0)
        //    {
        //        scaleFactor = "SF1";
        //    }
        //    else
        //    {
        //        scaleFactor = args[0];
        //    }

        //    //RunAllTests(scaleFactor, Environment.ProcessorCount);
        //    ////RunAllTests(scaleFactor, 1);
        //    ////RunAllTests(scaleFactor, 2);
        //    ////RunAllTests(scaleFactor, 4);
        //    ////RunAllTests(scaleFactor, 6);
        //    ////RunAllTests(scaleFactor, 8);
        //    ////RunAllTests(scaleFactor, 10);


        //    //for (int i = 1; i <= 5; i++)
        //    //{
        //    //    //InvisibleJoin invisibleJoin = new InvisibleJoin(scaleFactor);
        //    //    //invisibleJoin.Query_4_3();
        //    //    //GC.Collect(2, GCCollectionMode.Forced, true);
        //    //    //Thread.Sleep(100);
        //    //    ////invisibleJoinOutput.Add(invisibleJoin.testResults.toString());
        //    //    ////Console.WriteLine("Invisible: " + invisibleJoin.testResults.toString());
        //    //    //Console.WriteLine();
        //    //    //TestResultsDatabase.clearAllDatabase();

        //    //    //ParallelInvisibleJoin pInvisibleJoin = new ParallelInvisibleJoin(scaleFactor);
        //    //    //pInvisibleJoin.Query_4_3();
        //    //    //GC.Collect(2, GCCollectionMode.Forced, true);
        //    //    //Thread.Sleep(100);
        //    //    ////pInvisibleJoinOutput.Add(pInvisibleJoin.testResults.toString());
        //    //    ////Console.WriteLine("Parallel Invisible: " + pInvisibleJoin.testResults.toString());
        //    //    //Console.WriteLine();
        //    //    //TestResultsDatabase.clearAllDatabase();

        //    //    NimbleJoin nimbleJoin = new NimbleJoin(scaleFactor);
        //    //    nimbleJoin.Query_3_1();
        //    //    GC.Collect(2, GCCollectionMode.Forced, true);
        //    //    Thread.Sleep(100);
        //    //    //nimbleJoinOutput.Add(nimbleJoin.testResults.toString());
        //    //    // nimbleJoin.saveAndPrintResults();
        //    //    Console.WriteLine("NJ");
        //    //    TestResultsDatabase.clearAllDatabase();

        //    //    NimbleJoin nimbleJoin1 = new NimbleJoin(scaleFactor);
        //    //    nimbleJoin1.Query_3_1_BitMap();
        //    //    GC.Collect(2, GCCollectionMode.Forced, true);
        //    //    Thread.Sleep(100);
        //    //    //nimbleJoinOutput.Add(nimbleJoin.testResults.toString());
        //    //    // nimbleJoin.saveAndPrintResults();
        //    //    Console.WriteLine("NJB");
        //    //    TestResultsDatabase.clearAllDatabase();


        //    //    //ParallelNimbleJoin pNimbleJoin = new ParallelNimbleJoin(scaleFactor, 4);
        //    //    //pNimbleJoin.Query_4_3();
        //    //    //GC.Collect(2, GCCollectionMode.Forced, true);
        //    //    //Thread.Sleep(100);
        //    //    ////pNimbleJoinOutput.Add(pNimbleJoin.testResults.toString());
        //    //    //// Console.WriteLine("Parallel Nimble: " + pNimbleJoin.testResults.toString());
        //    //    //Console.WriteLine();
        //    //    //TestResultsDatabase.clearAllDatabase();

        //    //}

        //    selectivityTest(scaleFactor);
        //    Console.WriteLine("Test Complete.");
        //    Console.ReadKey();
        //}

        public static void RunAllTests(string scaleFactor, int processorCount)
        {

            for (int i = 1; i <= 4; i++)
            {
                for (int j = 1; j <= 4; j++)
                {

                    for (int k = 1; k <= 20; k++)
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
                    printResultsToFile(String.Format("{0}_Q{1}{2}_PC{3}.txt", scaleFactor, i, j, processorCount));
                    Thread.Sleep(10000);
                }
            }
        }

        private static void runQueries(string scaleFactor, int processorCount, int i, int j, int k)
        {
            Console.WriteLine(String.Format("Run #{0} for Query {1}.{2}", k, i, j));
            Console.WriteLine();

            Invoker.CreateAndInvoke("ParallelHashJoins.InvisibleJoin", new object[] { scaleFactor }, String.Format("Query_{0}_{1}", i, j), null);
            GC.Collect(2, GCCollectionMode.Forced, true);
            Thread.Sleep(TIMETOSLEEP);

            Invoker.CreateAndInvoke("ParallelHashJoins.NimbleJoin", new object[] { scaleFactor }, String.Format("Query_{0}_{1}", i, j), null);
            GC.Collect(2, GCCollectionMode.Forced, true);
            Thread.Sleep(TIMETOSLEEP);


            Invoker.CreateAndInvoke("ParallelHashJoins.ParallelInvisibleJoin", new object[] { scaleFactor, processorCount }, String.Format("Query_{0}_{1}", i, j), null);
            GC.Collect(2, GCCollectionMode.Forced, true);
            Thread.Sleep(TIMETOSLEEP);
            Console.WriteLine();

            Invoker.CreateAndInvoke("ParallelHashJoins.ParallelNimbleJoin", new object[] { scaleFactor, processorCount }, String.Format("Query_{0}_{1}", i, j), null);
            GC.Collect(2, GCCollectionMode.Forced, true);
            Thread.Sleep(TIMETOSLEEP);
            Console.WriteLine();

            Console.WriteLine("============================================================================");
            Console.WriteLine();
        }

        public static void printResultsToFile(string fileName)
        {
            //string fileName = scaleFactor + "_Q31.txt";
            string today = DateTime.Now.ToString("dd MMMM yyyy");
            string nimbleJoinOutputFilePath = @"Results\" + today + "\\Nimble Join\\";
            if (!Directory.Exists(nimbleJoinOutputFilePath))
                Directory.CreateDirectory(nimbleJoinOutputFilePath);
            System.IO.File.WriteAllLines(nimbleJoinOutputFilePath + fileName, TestResultsDatabase.nimbleJoinOutput);

            string invisibleJoinOutputFilePath = @"Results\" + today + "\\Invisible Join\\";
            if (!Directory.Exists(invisibleJoinOutputFilePath))
                Directory.CreateDirectory(invisibleJoinOutputFilePath);
            System.IO.File.WriteAllLines(invisibleJoinOutputFilePath + fileName, TestResultsDatabase.invisibleJoinOutput);

            string pNimbleJoinOutputFilePath = @"Results\" + today + "\\Parallel Nimble Join\\";
            if (!Directory.Exists(pNimbleJoinOutputFilePath))
                Directory.CreateDirectory(pNimbleJoinOutputFilePath);
            System.IO.File.WriteAllLines(pNimbleJoinOutputFilePath + fileName, TestResultsDatabase.pNimbleJoinOutput);

            string pInvisibleJoinOutputFilePath = @"Results\" + today + "\\Parallel Invisible Join\\";
            if (!Directory.Exists(pInvisibleJoinOutputFilePath))
                Directory.CreateDirectory(pInvisibleJoinOutputFilePath);
            System.IO.File.WriteAllLines(pInvisibleJoinOutputFilePath + fileName, TestResultsDatabase.pInvisibleJoinOutput);

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
                    var values = new Record();
                    values.s1 = "X";
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
                    var values = new Record();
                    values.s1 = "X";
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
        public static void selectivityTest(string scaleFactor)
        {
            //"SF1", "SF2", "SF3", "SF4"
            // "0.007", "0.07", "0.7"
            List<string> selectivityRatios = new List<string>() { "0.07" };
            foreach (var selectivityRatio in selectivityRatios)
            {
                for (int i = 0; i < 20; i++)
                {
                    ParallelNimbleJoin pnimbleJoin = new ParallelNimbleJoin(scaleFactor, 4);
                    pnimbleJoin.Query_3_1(selectivityRatio);
                    GC.Collect(2, GCCollectionMode.Forced, true);
                    Thread.Sleep(100);
                }
            }
        }
    }
}
