using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParallelHashJoins
{
    class ConcurrentBitMap
    {
        private bool[] _bitMap;
        private volatile Dictionary<int, bool> _dictCheck;
        private readonly object _sync = new object();

        public ConcurrentBitMap(int length)
        {
            _bitMap = new bool[length];
            _dictCheck = new Dictionary<int, bool>(length);
        }

        public void Add(int index, bool condition)
        {
            lock (_sync)
            {
                if (!_dictCheck.ContainsKey(index))
                {
                    _dictCheck.Add(index, true);
                    _bitMap[index] = condition;
                }
            }

            if (_dictCheck[index] && !_bitMap[index])
            {
                //cannot change
            }
            else
            {
                _bitMap[index] = condition;
            }

        }

        public bool[] GetAllTuples()
        {
            return _bitMap;
        }

        public bool Get(int index)
        {
            return _bitMap[index];
        }

        public int Length()
        {
            return _bitMap.Length;
        }
    }
}
