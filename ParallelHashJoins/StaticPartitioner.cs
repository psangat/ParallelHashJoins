using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParallelHashJoins
{
    public static class StaticPartitioner
    {

        public static List<List<T>> GetPartitions<T>(this List<T> source)
        {
            int partitionChunk = CalculatePartitions(4, source.Count);
            return source
                .Select((x, i) => new { Index = i, Value = x })
                .GroupBy(x => x.Index / partitionChunk)
                .Select(x => x.Select(v => v.Value).ToList())
                .ToList();
        }
       

        private static int CalculatePartitions(int partitionCount, int sourceLength)
        {
            return sourceLength / partitionCount;
        }
    }
}
