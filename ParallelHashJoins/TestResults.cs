using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParallelHashJoins
{
    public class TestResults
    {
        public long phase1Time { get; set; }
        public long phase11IOTime { get; set; }
        public long phase11HashTime { get; set; }
        public long phase12IOTime { get; set; }
        public long phase12HashTime { get; set; }
        public long phase13IOTime { get; set; }
        public long phase13HashTime { get; set; }
        public long phase14IOTime { get; set; }
        public long phase14HashTime { get; set; }


        public long phase2Time { get; set; }
        public long phase21IOTime { get; set; }
        public long phase21ProbeTime { get; set; }
        public long phase22IOTime { get; set; }
        public long phase22ProbeTime { get; set; }
        public long phase23IOTime { get; set; }
        public long phase23ProbeTime { get; set; }
        public long phase24IOTime { get; set; }
        public long phase24ProbeTime { get; set; }

        public long phase3Time { get; set; }
        public long phase3IOTime { get; set; }
        public long phase3ExtractionTime { get; set; }

        public long initialResposeTime { get; set; }
        public long totalExecutionTime { get; set; }

        public string memoryUsed { get; set; }

        public Int64 totalNumberOfOutput { get; set; }

        public List<Tuple<long, long>> outputRateList { get; set; }

        public float totalRAMAvailable { get; set; }

        public TestResults()
        {
            outputRateList = new List<Tuple<long, long>>();
        }
        public string toString()
        {
            StringBuilder sb = new StringBuilder();
            //foreach (var item in outputRateList)
            //{
            //    sb.Append(item.Item1 + "," + item.Item2 + ",");
            //}
            sb.Append(totalNumberOfOutput+ ","+ memoryUsed + ",");
            //sb.Append(memoryUsed + ",");
            sb.Append(phase11IOTime + "," + phase11HashTime + "," + phase12IOTime + "," + phase12HashTime + "," + phase13IOTime + "," + phase13HashTime + "," + phase1Time + "," +
                phase21IOTime + "," + phase21ProbeTime + "," + phase22IOTime + "," + phase22ProbeTime + "," + phase23IOTime + "," + phase23ProbeTime + "," + phase2Time + "," +
                phase3IOTime + "," + phase3ExtractionTime + "," + phase3Time + "," +
                initialResposeTime + "," + totalExecutionTime);
            return sb.ToString();
        }
    }
}
