using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParallelHashJoins
{
    class MAATB
    {

        private Record[] recordList;
        public BitArray bitMap;

        public MAATB(Int32 arraySize)
        {
            recordList = new Record[arraySize];
            bitMap = new BitArray(arraySize);
        }

        public void AddOrUpdate(Int64 key, Record value)
        {
            recordList[key] = value;
        }

        public Record GetValue(Int64 key)
        {
            return recordList[key];
        }

        public void Remove(Int64 key)
        {
            recordList[key] = null;
        }

        public Record[] GetAll()
        {
            return recordList;
        }

        public Int64 Count()
        {
            return recordList.Length;
        }
    }
}
