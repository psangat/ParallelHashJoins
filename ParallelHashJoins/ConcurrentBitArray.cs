using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParallelHashJoins
{
    class ConcurrentBitArray
    {
        private bool[] _bitArray;
        private object _sync = new object();

        public ConcurrentBitArray(int length)
        {
            _bitArray = new bool[length];
        }

        public void AddorUpdate(int index, bool value)
        {
           // lock (_sync)
            {
                _bitArray[index] = value;
            }
        }

        public bool Get(int index)
        {
            return _bitArray[index];
        }

        public bool[] GetArray()
        {
            //lock (_sync)
            {
                return _bitArray;
            }
        }

        public int Length()
        {
            //lock (_sync)
            {
                return _bitArray.Length;
            }
        }
    }
}
