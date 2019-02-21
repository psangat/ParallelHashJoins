using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParallelHashJoins
{
    class Record
    {
        public string s1;
        public string s2;
        public string s3;
        public Int64 i1;
        public Int64 i2;
    }


    class MAAT
    {

        private Record[] recordList;
        public BitArray bitMap;

        public List<Int64> positions;
        public MAAT(Int32 arraySize)
        {
            recordList = new Record[arraySize];
            positions = new List<Int64>();
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
