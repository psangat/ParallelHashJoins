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

        public MAATB(int arraySize)
        {
            recordList = new Record[arraySize];
            bitMap = new BitArray(arraySize);
        }

        public void AddOrUpdate(int key, Record value)
        {
            recordList[key] = value;
        }

        public Record GetValue(int key)
        {
            return recordList[key];
        }

        public void Remove(int key)
        {
            recordList[key] = null;
        }

        public Record[] GetAll()
        {
            return recordList;
        }

        public int Count()
        {
            return recordList.Length;
        }
    }
}
