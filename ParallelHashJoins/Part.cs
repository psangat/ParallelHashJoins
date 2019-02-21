using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParallelHashJoins
{
    [Serializable]
    class Part
    {
        public Int64 pPartKey { get; set; }
        public string pName { get; set; }
        public string pMFGR { get; set; }
        public string pCategory { get; set; }
        public string pBrand { get; set; }
        public string pColor { get; set; }
        public string pType { get; set; }
        public Int64 pSize { get; set; }
        public string pContainer { get; set; }

        public Part(Int64 pPartKey, 
            string pName, 
            string pMFGR, 
            string pCategory, 
            string pBrand, 
            string pColor, 
            string pType, 
            Int64 pSize, 
            string pContainer)
        {
            this.pPartKey = pPartKey;
            this.pName = pName;
            this.pMFGR = pMFGR;
            this.pCategory = pCategory;
            this.pBrand = pBrand;
            this.pColor = pColor;
            this.pType = pType;
            this.pSize = pSize;
            this.pContainer = pContainer;
        }
    }
}
