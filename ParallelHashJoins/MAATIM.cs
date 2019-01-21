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

        public MAATIM(int arraySize)
        {
            recordList = new List<object>[arraySize];
            positions = new List<Int32>();
            //for (int i = 0; i < recordList.Length; i++)
            //{
            //    recordList[i] = new List<object>(10);
            //}
        }

        public void AddOrUpdate(int key, List<object> value)
        {
            recordList[key] = value;
        }

        public List<object> GetValue(int key)
        {
            return recordList[key];
        }

        public void Remove(int key)
        {
            recordList[key] = null;
        }

        public List<object>[] GetAll()
        {
            return recordList;
        }

        public int Count()
        {
            return recordList.Length;
        }
    }
}
