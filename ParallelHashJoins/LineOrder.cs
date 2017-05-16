using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace ParallelHashJoins
{
    [Serializable]
    public class LineOrder
    {
        public int loOrderKey { get; set; }
        public int loLineNumber { get; set; }
        public int loCustKey { get; set; }
        public int loPartKey { get; set; }
        public int loSuppKey { get; set; }
        public int loOrderDate { get; set; }
        public string loOrderPriority { get; set; }
        public char loShipPriority { get; set; }
        public int loQuantity { get; set; }
        public int loExtendedPrice { get; set; }
        public int loOrderTotalPrice { get; set; }
        public int loDiscount { get; set; }
        public int loRevenue { get; set; }
        public int loSupplyCost { get; set; }
        public int loTax { get; set; }
        public int loCommitDateKey { get; set; }
        public string loShipMode { get; set; }

        public LineOrder(int loOrderKey,
            int loLineNumber,
            int loCustKey,
            int loPartKey,
            int loSuppKey,
            int loOrderDate,
            string loOrderPriority,
            char loShipPriority,
            int loQuantity,
            int loExtendedPrice,
            int loOrderTotalPrice,
            int loDiscount,
            int loRevenue,
            int loSupplyCost,
            int loTax,
            int loCommitDateKey,
            string loShipMode)
        {

            this.loOrderKey = loOrderKey;
            this.loLineNumber = loLineNumber;
            this.loCustKey = loCustKey;
            this.loPartKey = loPartKey;
            this.loSuppKey = loSuppKey;
            this.loOrderDate = loOrderDate;
            this.loOrderPriority = loOrderPriority;
            this.loShipPriority = loShipPriority;
            this.loQuantity = loQuantity;
            this.loExtendedPrice = loExtendedPrice;
            this.loOrderTotalPrice = loOrderTotalPrice;
            this.loDiscount = loDiscount;
            this.loRevenue = loRevenue;
            this.loSupplyCost = loSupplyCost;
            this.loTax = loTax;
            this.loCommitDateKey = loCommitDateKey;
            this.loShipMode = loShipMode;
        }

        public LineOrder()
        {

        }
    }
}