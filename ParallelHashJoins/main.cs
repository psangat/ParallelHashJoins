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
        static void Main(string[] args)
        {
            //MultiAttributeAssociativeArrayTest();
            string scaleFactor = string.Empty;
            if (args.Length <= 0)
            {
                scaleFactor = "SF1";
            }
            else
            {
                scaleFactor = args[0];
            }
            // RunAllTests(scaleFactor, Environment.ProcessorCount);
            //RunAllTests(scaleFactor, 2);
            //RunAllTests(scaleFactor, 4);
            //RunAllTests(scaleFactor, 6);
            //RunAllTests(scaleFactor, 8);
            //RunAllTests(scaleFactor, 10);
            for (int i = 1; i <= 5; i++)
            {
                InvisibleJoin invisibleJoin = new InvisibleJoin(scaleFactor);
                invisibleJoin.Query_4_3();
                GC.Collect(2, GCCollectionMode.Forced, true);
                Thread.Sleep(100);
                //invisibleJoinOutput.Add(invisibleJoin.testResults.toString());
                //Console.WriteLine("Invisible: " + invisibleJoin.testResults.toString());
                Console.WriteLine();
                TestResultsDatabase.clearAllDatabase();


                ParallelInvisibleJoin pInvisibleJoin = new ParallelInvisibleJoin(scaleFactor, 4);
                pInvisibleJoin.Query_4_3();
                GC.Collect(2, GCCollectionMode.Forced, true);
                Thread.Sleep(100);
                //pInvisibleJoinOutput.Add(pInvisibleJoin.testResults.toString());
                //Console.WriteLine("Parallel Invisible: " + pInvisibleJoin.testResults.toString());
                Console.WriteLine();

                TestResultsDatabase.clearAllDatabase();
            }

            //for (int i = 1; i <= 20; i++)
            //{
            //    Console.WriteLine("Run #" + i);
            //    Console.WriteLine();

            //    //InvisibleJoin invisibleJoin = new InvisibleJoin(scaleFactor);
            //    //invisibleJoin.Query_1_3();
            //    //GC.Collect(2, GCCollectionMode.Forced, true);
            //    //Thread.Sleep(100);
            //    ////invisibleJoinOutput.Add(invisibleJoin.testResults.toString());
            //    ////Console.WriteLine("Invisible: " + invisibleJoin.testResults.toString());
            //    //Console.WriteLine();

            //    NimbleJoin nimbleJoin = new NimbleJoin(scaleFactor);
            //    nimbleJoin.Query_1_1();
            //    GC.Collect(2, GCCollectionMode.Forced, true);
            //    Thread.Sleep(100);
            //    //nimbleJoinOutput.Add(nimbleJoin.testResults.toString());
            //   // nimbleJoin.saveAndPrintResults();
            //    Console.WriteLine();
            //    TestResultsDatabase.clearAllDatabase();

            //    NimbleJoin nimbleJoinMAAA = new NimbleJoin(scaleFactor);
            //    nimbleJoinMAAA.Query_3_1_MAAT();
            //    GC.Collect(2, GCCollectionMode.Forced, true);
            //    Thread.Sleep(100);
            //    //nimbleJoinOutput.Add(nimbleJoin.testResults.toString());

            //    //Console.WriteLine("NimbleMAAT: " + nimbleJoinMAAA.testResults.toString());
            //    Console.WriteLine();
            //    TestResultsDatabase.clearAllDatabase();

            //    // ParallelInvisibleJoin pInvisibleJoin = new ParallelInvisibleJoin(scaleFactor);
            //    // pInvisibleJoin.Query_1_3();
            //    // GC.Collect(2, GCCollectionMode.Forced, true);
            //    // Thread.Sleep(100);
            //    // //pInvisibleJoinOutput.Add(pInvisibleJoin.testResults.toString());
            //    // //Console.WriteLine("Parallel Invisible: " + pInvisibleJoin.testResults.toString());
            //    // Console.WriteLine();

            //    // ParallelNimbleJoin pNimbleJoin = new ParallelNimbleJoin(scaleFactor);
            //    // pNimbleJoin.Query_1_3();
            //    // GC.Collect(2, GCCollectionMode.Forced, true);
            //    // Thread.Sleep(100);
            //    // //pNimbleJoinOutput.Add(pNimbleJoin.testResults.toString());
            //    //// Console.WriteLine("Parallel Nimble: " + pNimbleJoin.testResults.toString());
            //    // Console.WriteLine();


            //    Console.WriteLine("============================================================================");
            //    Console.WriteLine();

            //}

            //printResultsToFile(scaleFactor + "_Q13.txt");
            //selectivityTest("SF3");

            //Console.WriteLine("Processing Complete.");
            Console.ReadKey();
        }

        public static void RunAllTests(string scaleFactor, int processorCount)
        {

            for (int i = 1; i <= 1; i++)
            {
                for (int j = 1; j <= 3; j++)
                {

                    for (int k = 1; k <= 2; k++)
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

                    printResultsToFile(String.Format("{0}_Q{1}{2}_PC{3}.txt", scaleFactor, i, j, processorCount));
                    //selectivityTest("SF3");
                    Thread.Sleep(5000);
                    //Console.WriteLine("Processing Complete.");
                    //Console.WriteLine(TestResultsDatabase.invisibleJoinOutput.Count());
                    //Console.ReadKey();
                }

            }
        }

        public static void printResultsToFile(string fileName)
        {
            //string fileName = scaleFactor + "_Q31.txt";
            string nimbleJoinOutputFilePath = @"Results\Nimble Join\" + DateTime.Now.ToString("dd MMMM yyyy") + "\\";
            if (!Directory.Exists(nimbleJoinOutputFilePath))
                Directory.CreateDirectory(nimbleJoinOutputFilePath);
            System.IO.File.WriteAllLines(nimbleJoinOutputFilePath + fileName, TestResultsDatabase.nimbleJoinOutput);

            string invisibleJoinOutputFilePath = @"Results\Invisible Join\" + DateTime.Now.ToString("dd MMMM yyyy") + "\\";
            if (!Directory.Exists(invisibleJoinOutputFilePath))
                Directory.CreateDirectory(invisibleJoinOutputFilePath);
            System.IO.File.WriteAllLines(invisibleJoinOutputFilePath + fileName, TestResultsDatabase.invisibleJoinOutput);

            string pNimbleJoinOutputFilePath = @"Results\Parallel Nimble Join\" + DateTime.Now.ToString("dd MMMM yyyy") + "\\";
            if (!Directory.Exists(pNimbleJoinOutputFilePath))
                Directory.CreateDirectory(pNimbleJoinOutputFilePath);
            System.IO.File.WriteAllLines(pNimbleJoinOutputFilePath + fileName, TestResultsDatabase.pNimbleJoinOutput);

            string pInvisibleJoinOutputFilePath = @"Results\Parallel Invisible Join\" + DateTime.Now.ToString("dd MMMM yyyy") + "\\";
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
            sw.Stop();
            Console.WriteLine("Insert Time MAAA: " + sw.ElapsedMilliseconds + " ms");
            sw.Reset();
            sw.Restart();
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
            sw.Stop();
            Console.WriteLine("Insert Time HashTable: " + sw.ElapsedMilliseconds + " ms");
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
            Console.ReadKey();
        }
        //public static void selectivityTest(string scaleFactor)
        //{
        //    //"SF1", "SF2", "SF3", "SF4"
        //    // "0.007", "0.07", "0.7"
        //    List<string> selectivityRatios = new List<string>() { "0.07" };
        //    foreach (var selectivityRatio in selectivityRatios)
        //    {
        //        for (int i = 0; i < 20; i++)
        //        {
        //            NimbleJoin nimbleJoin = new NimbleJoin(scaleFactor);
        //            nimbleJoin.Query_3_1(selectivityRatio);
        //            GC.Collect(2, GCCollectionMode.Forced, true);
        //            Thread.Sleep(100);
        //            nimbleJoinOutput.Add(nimbleJoin.testResults.toString());

        //            InvisibleJoin invisibleJoin = new InvisibleJoin(scaleFactor);
        //            invisibleJoin.Query_3_1(selectivityRatio);
        //            GC.Collect(2, GCCollectionMode.Forced, true);
        //            Thread.Sleep(100);
        //            invisibleJoinOutput.Add(invisibleJoin.testResults.toString());
        //        }

        //        string fileName = scaleFactor + "_" + selectivityRatio + ".txt";
        //        string nimbleJoinOutputFilePath = @"C:\Users\psangats\Google Drive\Study\0190 Doctor of Philosophy\My Research - Publication Works\Nimble Join\" + DateTime.Now.ToString("dd MMMM yyyy") + "\\";
        //        if (!Directory.Exists(nimbleJoinOutputFilePath))
        //            Directory.CreateDirectory(nimbleJoinOutputFilePath);
        //        System.IO.File.WriteAllLines(nimbleJoinOutputFilePath + fileName, nimbleJoinOutput);
        //        nimbleJoinOutput.Clear();
        //        string invisibleJoinOutputFilePath = @"C:\Users\psangats\Google Drive\Study\0190 Doctor of Philosophy\My Research - Publication Works\Invisible Join\" + DateTime.Now.ToString("dd MMMM yyyy") + "\\";
        //        if (!Directory.Exists(invisibleJoinOutputFilePath))
        //            Directory.CreateDirectory(invisibleJoinOutputFilePath);
        //        System.IO.File.WriteAllLines(invisibleJoinOutputFilePath + fileName, invisibleJoinOutput);
        //        invisibleJoinOutput.Clear();
        //    }
        //}
    }
}
