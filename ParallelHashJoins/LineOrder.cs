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
        public Int64 loOrderKey { get; set; }
        public Int64 loLineNumber { get; set; }
        public Int64 loCustKey { get; set; }
        public Int64 loPartKey { get; set; }
        public Int64 loSuppKey { get; set; }
        public Int64 loOrderDate { get; set; }
        public string loOrderPriority { get; set; }
        public char loShipPriority { get; set; }
        public Int64 loQuantity { get; set; }
        public Int64 loExtendedPrice { get; set; }
        public Int64 loOrderTotalPrice { get; set; }
        public Int64 loDiscount { get; set; }
        public Int64 loRevenue { get; set; }
        public Int64 loSupplyCost { get; set; }
        public Int64 loTax { get; set; }
        public Int64 loCommitDateKey { get; set; }
        public string loShipMode { get; set; }

        public LineOrder(Int64 loOrderKey,
            Int64 loLineNumber,
            Int64 loCustKey,
            Int64 loPartKey,
            Int64 loSuppKey,
            Int64 loOrderDate,
            string loOrderPriority,
            char loShipPriority,
            Int64 loQuantity,
            Int64 loExtendedPrice,
            Int64 loOrderTotalPrice,
            Int64 loDiscount,
            Int64 loRevenue,
            Int64 loSupplyCost,
            Int64 loTax,
            Int64 loCommitDateKey,
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