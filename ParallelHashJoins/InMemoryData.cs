using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParallelHashJoins
{
    internal class InMemoryData
    {
        #region Private Variables
        private static readonly string folderPath = @"C:\Raw_Data_Source_For_Test\SSBM - DBGEN\SF 100";
        private static readonly string binaryFilesDirectory = @"C:\Raw_Data_Source_For_Test\SSBM - DBGEN\BF";
        
        private static string dateFile = binaryFilesDirectory + @"\date\";
        private static string customerFile = binaryFilesDirectory + @"\customer\";
        private static string supplierFile = binaryFilesDirectory + @"\supplier\";
        private static string partFile = binaryFilesDirectory + @"\part\";
        
        private static readonly string loOrderKeyFile = binaryFilesDirectory + @"\loOrderKey\";
        private static string loCustKeyFile = binaryFilesDirectory + @"\loCustKey\";
        private static string loPartKeyFile = binaryFilesDirectory + @"\loPartKey\";
        private static string loSuppKeyFile = binaryFilesDirectory + @"\loSuppKey\";
        private static string loOrderDateFile = binaryFilesDirectory + @"\loOrderDate\";
        private static string loRevenueFile = binaryFilesDirectory + @"\loRevenue\";
        private static string loSupplyCostFile = binaryFilesDirectory + @"\loSupplyCost\";
        private static string scaleFactor = "";
        
        public static List<Customer> customerDimension = new List<Customer>();
        public static List<Supplier> supplierDimension = new List<Supplier>();
        public static List<Part> partDimension = new List<Part>();
        public static List<Date> dateDimension = new List<Date>();
        
        public static List<long> loCustomerKey = new List<long>();
        public static List<long> loPartKey = new List<long>();
        public static List<long> loSupplierKey = new List<long>();
        public static List<long> loOrderDate = new List<long>();
        public static List<long> loRevenue = new List<long>();
        public static List<long> loSupplyCost = new List<long>();

        public static ParallelOptions parallelOptions = null;

        #endregion Private Variables

        public InMemoryData(string _scaleFactor, ParallelOptions _parallelOptions)
        {
            scaleFactor = _scaleFactor;
            parallelOptions = _parallelOptions;

            Parallel.Invoke(parallelOptions, () =>
            {
                customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
                Console.WriteLine(String.Format("[{0}] Customer Dimension Load Complete.", DateTime.Now));
            }, () =>
            {
                supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
                Console.WriteLine(String.Format("[{0}] Supplier Dimension Load Complete.", DateTime.Now));
            }, () =>
            {
                dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                Console.WriteLine(String.Format("[{0}] Date Dimension Load Complete.", DateTime.Now));
            }, () =>
            {
                partDimension = Utils.ReadFromBinaryFiles<Part>(partFile.Replace("BF", "BF" + scaleFactor));
                Console.WriteLine(String.Format("[{0}] Part Dimension Load Complete.", DateTime.Now));
            }, () =>
            {
                loCustomerKey = Utils.ReadFromBinaryFiles<long>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                Console.WriteLine(String.Format("[{0}] loCustomerKey Column Load Complete.", DateTime.Now));
            }, () =>
            {
                loSupplierKey = Utils.ReadFromBinaryFiles<long>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                Console.WriteLine(String.Format("[{0}] loSupplierKey Column Load Complete.", DateTime.Now));
            }, () =>
            {
                loOrderDate = Utils.ReadFromBinaryFiles<long>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                Console.WriteLine(String.Format("[{0}] loOrderDate Column Load Complete.", DateTime.Now));
            }, () =>
            {
                loPartKey = Utils.ReadFromBinaryFiles<long>(loPartKeyFile.Replace("BF", "BF" + scaleFactor));
                Console.WriteLine(String.Format("[{0}] loPartKey Column Load Complete.", DateTime.Now));
            }, () =>
            {
                loRevenue = Utils.ReadFromBinaryFiles<long>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
                Console.WriteLine(String.Format("[{0}] loRevenue Column Load Complete.", DateTime.Now));
            }, () =>
            {
                loSupplyCost = Utils.ReadFromBinaryFiles<long>(loSupplyCostFile.Replace("BF", "BF" + scaleFactor));
                Console.WriteLine(String.Format("[{0}] loSupplyCost Column Load Complete.", DateTime.Now));
            }
            );
        }
    }
}
