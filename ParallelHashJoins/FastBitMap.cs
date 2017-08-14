using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParallelHashJoins
{
    class FastBitMap
    {
        public struct KVPair
        {
            public bool key;
            public string values;
        };

        private KVPair[] data;
        private int length;

        public FastBitMap(int size)
        {
            data = new KVPair[size];
            length = size;
        }

        public void Set(int index, bool key, string value)
        {
            data[index].key = key;
            data[index].values = value;
        }

        private KVPair Get(int index)
        {
            return data[index];
        }

        public KVPair[] Get()
        {
            return data;
        }
       
        public int Length()
        {
            return length;
        }

        public FastBitMap And(FastBitMap value)
        {
            int i = 0;
            foreach (var kvPair in value.Get())
            {
                if (data[i].key &= kvPair.key)
                    data[i].values += ", " + kvPair.values;
                i++;
            }
            return this;
        }
    }
}
