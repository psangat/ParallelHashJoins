using System;
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
        public int i1;
        public int i2;
    }


    class MAAT
    {

        private Record[] recordList;

        public List<int> positions;
        public MAAT(int arraySize)
        {
            recordList = new Record[arraySize];
            positions = new List<int>();
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
