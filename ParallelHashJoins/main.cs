using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParallelHashJoins
{
    class main
    {
        static DataTable dt = null;
        static void Main(string[] args)
        {
            Algorithms ag = new Algorithms();
            List<String> lines = new List<string>();
            string header = "TotalRecords, #D1, #D2, #D3, TotalOutput, Selectivity Ratio, P1Time, P2Time, P3Time, TET, IRT";
            Console.WriteLine(header);
            lines.Add(header);
            for (int i = 0; i < 20; i++)
            {
                ag.resetGlobalVariables();
                ag.NimbleJoinV2();
                string record = ag.totalNumberOfRecords + ", "+ ag.totalRecordsD1 + ", " + ag.totalRecordsD2 + ", " + ag.totalRecordsD3+ ", " + ag.totalNumberOfOutput + ", " + ((Double)ag.totalNumberOfOutput / ag.totalNumberOfRecords) + ", " + ag.phase1Time + ", " + ag.phase2Time + ", " + ag.phase3Time + ", " + (ag.phase1Time + ag.phase2Time + ag.phase3Time) + ", " + ag.initialResposeTime;
                Console.WriteLine(record);
                lines.Add(record);
                //foreach (var item in ag.outputRecordsList)
                //{
                //    Console.WriteLine(item);
                //}
            }
            System.IO.File.WriteAllLines(@"C:\Users\psangats\Google Drive\Study\0190 Doctor of Philosophy\My Research - Publication Works\Nimble Join\SF4_1.txt", lines);
            //Console.ReadKey();
        }
    }
}
