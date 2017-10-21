using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ParallelHashJoins
{
    class main
    {
        private static List<String> nimbleJoinOutput = new List<string>();
        private static List<String> invisibleJoinOutput = new List<string>();
        private static List<String> pNimbleJoinOutput = new List<string>();
        private static List<String> pInvisibleJoinOutput = new List<string>();
        static void Main(string[] args)
        {
            string scaleFactor = string.Empty;
            if (args.Length <= 0)
            {
                scaleFactor = "SF1";
            }
            else
            {
                scaleFactor = args[0];
            }

            for (int i = 1; i <= 20; i++)
            {
                Console.WriteLine("Run #" + i);
                Console.WriteLine();

                InvisibleJoin invisibleJoin = new InvisibleJoin(scaleFactor);
                invisibleJoin.Query_3_3();
                GC.Collect(2, GCCollectionMode.Forced, true);
                Thread.Sleep(100);
                invisibleJoinOutput.Add(invisibleJoin.testResults.toString());
                Console.WriteLine("Invisible: " + invisibleJoin.testResults.toString());
                Console.WriteLine();

                NimbleJoin nimbleJoin = new NimbleJoin(scaleFactor);
                nimbleJoin.Query_3_3();
                GC.Collect(2, GCCollectionMode.Forced, true);
                Thread.Sleep(100);
                nimbleJoinOutput.Add(nimbleJoin.testResults.toString());
                Console.WriteLine("Nimble: " + nimbleJoin.testResults.toString());
                Console.WriteLine();


                ParallelInvisibleJoin pInvisibleJoin = new ParallelInvisibleJoin(scaleFactor);
                pInvisibleJoin.Query_3_3();
                GC.Collect(2, GCCollectionMode.Forced, true);
                Thread.Sleep(100);
                pInvisibleJoinOutput.Add(pInvisibleJoin.testResults.toString());
                Console.WriteLine("Parallel Invisible: " + pInvisibleJoin.testResults.toString());
                Console.WriteLine();

                ParallelNimbleJoin pNimbleJoin = new ParallelNimbleJoin(scaleFactor);
                pNimbleJoin.Query_3_3();
                GC.Collect(2, GCCollectionMode.Forced, true);
                Thread.Sleep(100);
                pNimbleJoinOutput.Add(pNimbleJoin.testResults.toString());
                Console.WriteLine("Parallel Nimble: " + pNimbleJoin.testResults.toString());
                Console.WriteLine();


                Console.WriteLine("============================================================================");
                Console.WriteLine();

            }

            printResultsToFile(scaleFactor + "_Q33.txt");
            //selectivityTest("SF3");

            Console.WriteLine("Processing Complete.");
            // Console.ReadKey();
        }

        public static void printResultsToFile(string fileName) {
            //string fileName = scaleFactor + "_Q31.txt";
            string nimbleJoinOutputFilePath = @"Results\Nimble Join\" + DateTime.Now.ToString("dd MMMM yyyy") + "\\";
            if (!Directory.Exists(nimbleJoinOutputFilePath))
                Directory.CreateDirectory(nimbleJoinOutputFilePath);
            System.IO.File.WriteAllLines(nimbleJoinOutputFilePath + fileName, nimbleJoinOutput);

            string invisibleJoinOutputFilePath = @"Results\Invisible Join\" + DateTime.Now.ToString("dd MMMM yyyy") + "\\";
            if (!Directory.Exists(invisibleJoinOutputFilePath))
                Directory.CreateDirectory(invisibleJoinOutputFilePath);
            System.IO.File.WriteAllLines(invisibleJoinOutputFilePath + fileName, invisibleJoinOutput);

            string pNimbleJoinOutputFilePath = @"Results\Parallel Nimble Join\" + DateTime.Now.ToString("dd MMMM yyyy") + "\\";
            if (!Directory.Exists(pNimbleJoinOutputFilePath))
                Directory.CreateDirectory(pNimbleJoinOutputFilePath);
            System.IO.File.WriteAllLines(pNimbleJoinOutputFilePath + fileName, pNimbleJoinOutput);

            string pInvisibleJoinOutputFilePath = @"Results\Parallel Invisible Join\" + DateTime.Now.ToString("dd MMMM yyyy") + "\\";
            if (!Directory.Exists(pInvisibleJoinOutputFilePath))
                Directory.CreateDirectory(pInvisibleJoinOutputFilePath);
            System.IO.File.WriteAllLines(pInvisibleJoinOutputFilePath + fileName, pInvisibleJoinOutput);
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
                    NimbleJoin nimbleJoin = new NimbleJoin(scaleFactor);
                    nimbleJoin.Query_3_1(selectivityRatio);
                    GC.Collect(2, GCCollectionMode.Forced, true);
                    Thread.Sleep(100);
                    nimbleJoinOutput.Add(nimbleJoin.testResults.toString());

                    InvisibleJoin invisibleJoin = new InvisibleJoin(scaleFactor);
                    invisibleJoin.Query_3_1(selectivityRatio);
                    GC.Collect(2, GCCollectionMode.Forced, true);
                    Thread.Sleep(100);
                    invisibleJoinOutput.Add(invisibleJoin.testResults.toString());
                }

                string fileName = scaleFactor + "_" + selectivityRatio + ".txt";
                string nimbleJoinOutputFilePath = @"C:\Users\psangats\Google Drive\Study\0190 Doctor of Philosophy\My Research - Publication Works\Nimble Join\" + DateTime.Now.ToString("dd MMMM yyyy") + "\\";
                if (!Directory.Exists(nimbleJoinOutputFilePath))
                    Directory.CreateDirectory(nimbleJoinOutputFilePath);
                System.IO.File.WriteAllLines(nimbleJoinOutputFilePath + fileName, nimbleJoinOutput);
                nimbleJoinOutput.Clear();
                string invisibleJoinOutputFilePath = @"C:\Users\psangats\Google Drive\Study\0190 Doctor of Philosophy\My Research - Publication Works\Invisible Join\" + DateTime.Now.ToString("dd MMMM yyyy") + "\\";
                if (!Directory.Exists(invisibleJoinOutputFilePath))
                    Directory.CreateDirectory(invisibleJoinOutputFilePath);
                System.IO.File.WriteAllLines(invisibleJoinOutputFilePath + fileName, invisibleJoinOutput);
                invisibleJoinOutput.Clear();
            }
        }
    }
}
