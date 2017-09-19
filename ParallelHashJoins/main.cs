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
        static void Main(string[] args)
        {

            for (int i = 0; i < 10; i++)
            {
                NimbleJoin nimbleJoin = new NimbleJoin("SF4");
                nimbleJoin.Query_3_1();
                GC.Collect(2, GCCollectionMode.Forced, true);
                Thread.Sleep(1000);
                //nimbleJoinOutput.Add(nimbleJoin.testResults.toString());
                Console.WriteLine(nimbleJoin.testResults.toString());

                InvisibleJoin invisibleJoin = new InvisibleJoin("SF4");
                invisibleJoin.Query_3_1();
                GC.Collect(2, GCCollectionMode.Forced, true);
                Thread.Sleep(1000);
                //invisibleJoinOutput.Add(invisibleJoin.testResults.toString());
                Console.WriteLine(invisibleJoin.testResults.toString());
            }
            //string fileName = "Q31.txt";
            //string nimbleJoinOutputFilePath = @"C:\Users\psangats\Google Drive\Study\0190 Doctor of Philosophy\My Research - Publication Works\Nimble Join\17_09_2017\";
            //if (!Directory.Exists(nimbleJoinOutputFilePath))
            //    Directory.CreateDirectory(nimbleJoinOutputFilePath);
            //// System.IO.File.WriteAllLines(nimbleJoinOutputFilePath + fileName, nimbleJoinOutput);

            //string invisibleJoinOutputFilePath = @"C:\Users\psangats\Google Drive\Study\0190 Doctor of Philosophy\My Research - Publication Works\Invisible Join\17_09_2017\";
            //if (!Directory.Exists(invisibleJoinOutputFilePath))
            //    Directory.CreateDirectory(invisibleJoinOutputFilePath);
            ////System.IO.File.WriteAllLines(invisibleJoinOutputFilePath + fileName, invisibleJoinOutput);



            //selectivityTest("SF3");

            Console.WriteLine("Processing Complete.");
            Console.ReadKey();
        }

        public static void selectivityTest(string scaleFactor)
        {
            //"SF1", "SF2", "SF3", "SF4"
            // "0.007", "0.07", "0.7"
            List<string> selectivityRatios = new List<string>() {"0.07"};
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
