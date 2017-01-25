using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParallelHashJoins
{
    class Customer
    {
        public int cCustKey { get; set; }
        public string cName { get; set; }
        public string cAddress { get; set; }
        public string cCity { get; set; }
        public string cNation { get; set; }
        public string cRegion { get; set; }
        public string cPhone { get; set; }
        public string cMktSegment { get; set; }

        public Customer(int cCustKey,
            string cName,
            string cAddress,
            string cCity, 
            string cNation, 
            string cRegion, 
            string cPhone, 
            string cMktSegment)
        {
            this.cCustKey = cCustKey;
            this.cName = cName;
            this.cAddress = cAddress;
            this.cCity = cCity;
            this.cNation = cNation;
            this.cRegion = cRegion;
            this.cPhone = cPhone;
            this.cMktSegment = cMktSegment;
        }
    }
}
