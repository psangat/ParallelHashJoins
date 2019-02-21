using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParallelHashJoins
{
    class MAATIM
    {
        private List<object>[] recordList;
        public List<Int32> positions { get; set; }

        public MAATIM(Int64 arraySize)
        {
            recordList = new List<object>[arraySize];
            positions = new List<Int32>();
            //for (Int64 i = 0; i < recordList.Length; i++)
            //{
            //    recordList[i] = new List<object>(10);
            //}
        }

        public void AddOrUpdate(Int64 key, List<object> value)
        {
            recordList[key] = value;
        }

        public List<object> GetValue(Int64 key)
        {
            return recordList[key];
        }

        public void Remove(Int64 key)
        {
            recordList[key] = null;
        }

        public List<object>[] GetAll()
        {
            return recordList;
        }

        public Int64 Count()
        {
            return recordList.Length;
        }
    }
}
