using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParallelHashJoins
{
    class TestFastBitMap
    {

        static void Main(string[] args)
        {
            Algorithms ag = new Algorithms();
            //// Use if Needed to Create Binary Files Again
            ////Console.WriteLine("Loading Columns...");
            ////ag.loadColumns();
            ////Console.WriteLine("Loading Columns Completed.");
            ////Console.WriteLine("Loading Tables...");
            ////ag.loadTables();
            ////Console.WriteLine("Loading Tables Completed.");
            ////Console.WriteLine("Creating Binary Files ...");
            ////ag.createBinaryFiles();
            ////Console.WriteLine("Creating Binary Files Completed.");
            ////ag.ParallelXYZJoin();
            ag.XYZJoinNewTry();
            ////Console.WriteLine();
            //////ag.ParallelInvisibleJoin();
            ////for (int i = 0; i < 10; i++)
            ////{
            ////    ag.XYZV2();
            ////    Console.WriteLine();
            ////}

            ////ag.XYZJoin1();
            //for (int i = 0; i < 10; i++)
            //{
            //    Stopwatch sw = Stopwatch.StartNew();
            //    ag.InvisibleJoin();
            //    sw.Stop();
            //    Console.WriteLine("[Invisible Join] Total Time: {0} ms.", sw.ElapsedMilliseconds);
            //    sw.Reset();
            //    sw.Start();

            //    ag.XYZV2();
            //    sw.Stop();
            //    Console.WriteLine("[XYZ V2 Join] Total Time: {0} ms.", sw.ElapsedMilliseconds);
            //    sw.Reset();
            //    sw.Start();

            //    ag.XYZJoinNewTry();
            //    sw.Stop();
            //    Console.WriteLine("[XYZ New Join] Total Time: {0} ms.", sw.ElapsedMilliseconds);

            //}
            Console.ReadKey();
        }
    }
}
