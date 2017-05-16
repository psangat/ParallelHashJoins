using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParallelHashJoins
{
    [Serializable]
    class Supplier
    {
        public int sSuppKey { get; set; }
        public string sName { get; set; }
        public string sAddress { get; set; }
        public string sCity { get; set; }
        public string sNation { get; set; }
        public string sRegion { get; set; }
        public string sPhone { get; set; }

        public Supplier(int sSuppKey,
            string sName,
            string sAddress,
            string sCity, 
            string sNation, 
            string sRegion, 
            string sPhone)
        {
            this.sSuppKey = sSuppKey;
            this.sName = sName;
            this.sAddress = sAddress;
            this.sCity = sCity;
            this.sNation = sNation;
            this.sRegion = sRegion;
            this.sPhone = sPhone;
        }
    }
}
