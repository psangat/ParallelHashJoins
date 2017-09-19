using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParallelHashJoins
{
    public class Pair
    {
        public int id { get; set; }
        public string value { get; set; }

        public Pair(int id, string value)
        {
            this.id = id;
            this.value = value;
        }
    }

    public class ComparePair : IEqualityComparer<Pair>
    {
        public bool Equals(Pair x, Pair y)
        {
            return x.id == y.id;
        }

        public int GetHashCode(Pair obj)
        {
            return obj.id.GetHashCode();
        }
    }


    class Algorithms
    {
        private static string binaryFilesDirectory = @"C:\Raw_Data_Source_For_Test\SSBM - DBGEN\BF";
        private string scaleFactor = "";
        public Algorithms(string scaleFactor)
        {
            this.scaleFactor = scaleFactor;
        }
        #region Private Variables
        private string folderPath = @"C:\Raw_Data_Source_For_Test\SSBM - DBGEN\SF 1";
        private List<int> cCustKey = new List<int>();
        private List<string> cName = new List<string>();
        private List<string> cAddress = new List<string>();
        private List<string> cCity = new List<string>();
        private List<string> cNation = new List<string>();
        private List<string> cRegion = new List<string>();
        private List<string> cPhone = new List<string>();
        private List<string> cMktSegment = new List<string>();

        private List<int> sSuppKey = new List<int>();
        private List<string> sName = new List<string>();
        private List<string> sAddress = new List<string>();
        private List<string> sCity = new List<string>();
        private List<string> sNation = new List<string>();
        private List<string> sRegion = new List<string>();
        private List<string> sPhone = new List<string>();

        private List<int> pSize = new List<int>();
        private List<int> pPartKey = new List<int>();
        private List<string> pName = new List<string>();
        private List<string> pMFGR = new List<string>();
        private List<string> pCategory = new List<string>();
        private List<string> pBrand = new List<string>();
        private List<string> pColor = new List<string>();
        private List<string> pType = new List<string>();
        private List<string> pContainer = new List<string>();

        private List<int> loOrderKey = new List<int>();
        private List<int> loLineNumber = new List<int>();
        private List<int> loCustKey = new List<int>();
        private List<int> loPartKey = new List<int>();
        private List<int> loSuppKey = new List<int>();
        private List<int> loOrderDate = new List<int>();
        private List<char> loShipPriority = new List<char>();
        private List<int> loQuantity = new List<int>();
        private List<Tuple<int, int>> loQuantityWithId = new List<Tuple<int, int>>();

        private List<int> loExtendedPrice = new List<int>();
        private List<int> loOrdTotalPrice = new List<int>();
        private List<int> loDiscount = new List<int>();
        private List<Tuple<int, int>> loDiscountWithId = new List<Tuple<int, int>>();
        private List<int> loRevenue = new List<int>();
        private List<int> loSupplyCost = new List<int>();
        private List<int> loTax = new List<int>();
        private List<int> loCommitDate = new List<int>();
        private List<string> loShipMode = new List<string>();
        private List<string> loOrderPriority = new List<string>();

        private List<int> dDateKey = new List<int>();
        private List<int> dYear = new List<int>();
        private List<int> dYearMonthNum = new List<int>();
        private List<int> dDayNumInWeek = new List<int>();
        private List<int> dDayNumInMonth = new List<int>();
        private List<int> dDayNumInYear = new List<int>();
        private List<int> dMonthNumInYear = new List<int>();
        private List<int> dWeekNumInYear = new List<int>();
        private List<int> dLastDayInWeekFL = new List<int>();
        private List<int> dLastDayInMonthFL = new List<int>();
        private List<int> dHolidayFL = new List<int>();
        private List<int> dWeekDayFL = new List<int>();
        private List<string> dDate = new List<string>();
        private List<string> dDayOfWeek = new List<string>();
        private List<string> dMonth = new List<string>();
        private List<string> dYearMonth = new List<string>();
        private List<string> dSellingSeason = new List<string>();

        private List<Customer> customer = new List<Customer>();
        private List<Supplier> supplier = new List<Supplier>();
        private List<Part> part = new List<Part>();
        private List<LineOrder> lineOrder = new List<LineOrder>();
        private List<Date> date = new List<Date>();


        private string dateFile = binaryFilesDirectory + @"\date\";
        private string customerFile = binaryFilesDirectory + @"\customer\";
        private string supplierFile = binaryFilesDirectory + @"\supplier\";
        private string partFile = binaryFilesDirectory + @"\part\";

        private string dDateKeyFile = binaryFilesDirectory + @"\dDateKey\";
        private string dYearFile = binaryFilesDirectory + @"\dYear\";
        private string dYearMonthNumFile = binaryFilesDirectory + @"\dYearMonthNum\";
        private string dDayNumInWeekFile = binaryFilesDirectory + @"\dDayNumInWeek\";
        private string dDayNumInMonthFile = binaryFilesDirectory + @"\dDayNumInMont\";
        private string dDayNumInYearFile = binaryFilesDirectory + @"\dDayNumInYear\";
        private string dMonthNumInYearFile = binaryFilesDirectory + @"\dMonthNumInYear\";
        private string dWeekNumInYearFile = binaryFilesDirectory + @"\dWeekNumInYear\";
        private string dLastDayInWeekFLFile = binaryFilesDirectory + @"\dLastDayInWeekFL\";
        private string dLastDayInMonthFLFile = binaryFilesDirectory + @"\dLastDayInMonthFL\";
        private string dHolidayFLFile = binaryFilesDirectory + @"\dHolidayFL\";
        private string dWeekDayFLFile = binaryFilesDirectory + @"\dWeekDayFL\";
        private string dDateFile = binaryFilesDirectory + @"\dDate\";
        private string dDayOfWeekFile = binaryFilesDirectory + @"\dDayOfWeek\";
        private string dMonthFile = binaryFilesDirectory + @"\dMonth\";
        private string dYearMonthFile = binaryFilesDirectory + @"\dYearMonth\";
        private string dSellingSeasonFile = binaryFilesDirectory + @"\dSellingSeason\";

        private string loOrderKeyFile = binaryFilesDirectory + @"\loOrderKey\";
        private string loLineNumberFile = binaryFilesDirectory + @"\loLineNumber\";
        private string loCustKeyFile = binaryFilesDirectory + @"\loCustKey\";
        private string loPartKeyFile = binaryFilesDirectory + @"\loPartKey\";
        private string loSuppKeyFile = binaryFilesDirectory + @"\loSuppKey\";
        private string loOrderDateFile = binaryFilesDirectory + @"\loOrderDate\";
        private string loShipPriorityFile = binaryFilesDirectory + @"\loShipPriority\";
        private string loQuantityFile = binaryFilesDirectory + @"\loQuantity\";
        private string loExtendedPriceFile = binaryFilesDirectory + @"\loExtendedPrice\";
        private string loOrdTotalPriceFile = binaryFilesDirectory + @"\loOrdTotalPrice\";
        private string loDiscountFile = binaryFilesDirectory + @"\loDiscount\";
        private string loRevenueFile = binaryFilesDirectory + @"\loRevenue\";
        private string loSupplyCostFile = binaryFilesDirectory + @"\loSupplyCost\";
        private string loTaxFile = binaryFilesDirectory + @"\loTax\";
        private string loCommitDateFile = binaryFilesDirectory + @"\loCommitDate\";
        private string loShipModeFile = binaryFilesDirectory + @"\loShipMode\";
        private string loOrderPriorityFile = binaryFilesDirectory + @"\loOrderPriority\";

        private string cCustKeyFile = binaryFilesDirectory + @"\cCustKey\";
        private string cNameFile = binaryFilesDirectory + @"\cName\";
        private string cAddressFile = binaryFilesDirectory + @"\cAddress\";
        private string cCityFile = binaryFilesDirectory + @"\cCity\";
        private string cNationFile = binaryFilesDirectory + @"\cNation\";
        private string cRegionFile = binaryFilesDirectory + @"\cRegion\";
        private string cPhoneFile = binaryFilesDirectory + @"\cPhone\";
        private string cMktSegmentFile = binaryFilesDirectory + @"\cMktSegment\";

        private string sSuppKeyFile = binaryFilesDirectory + @"\sSuppKey\";
        private string sNameFile = binaryFilesDirectory + @"\sName\";
        private string sAddressFile = binaryFilesDirectory + @"\sAddress\";
        private string sCityFile = binaryFilesDirectory + @"\sCity\";
        private string sNationFile = binaryFilesDirectory + @"\sNation\";
        private string sRegionFile = binaryFilesDirectory + @"\sRegion\";
        private string sPhoneFile = binaryFilesDirectory + @"\sPhone\";

        private string pSizeFile = binaryFilesDirectory + @"\pSize\";
        private string pPartKeyFile = binaryFilesDirectory + @"\pPartKey\";
        private string pNameFile = binaryFilesDirectory + @"\pName\";
        private string pMFGRFile = binaryFilesDirectory + @"\pMFGR\";
        private string pCategoryFile = binaryFilesDirectory + @"\pCategory\";
        private string pBrandFile = binaryFilesDirectory + @"\pBrand\";
        private string pColorFile = binaryFilesDirectory + @"\pColor\";
        private string pTypeFile = binaryFilesDirectory + @"\pType\";
        private string pContainerFile = binaryFilesDirectory + @"\pContainer\";

        private Boolean isFirst = true;

        #endregion Private Variables

        public long phase1Time { get; set; }
        public long phase11IOTime { get; set; }
        public long phase11HashTime { get; set; }
        public long phase12IOTime { get; set; }
        public long phase12HashTime { get; set; }
        public long phase13IOTime { get; set; }
        public long phase13HashTime { get; set; }
        public long phase2Time { get; set; }
        public long phase21IOTime { get; set; }
        public long phase21HashTime { get; set; }
        public long phase22IOTime { get; set; }
        public long phase22HashTime { get; set; }
        public long phase23IOTime { get; set; }
        public long phase23HashTime { get; set; }
        public long phase3Time { get; set; }
        public long phase4Time { get; set; }
        public long initialResposeTime { get; set; }
        public long totalNumberOfRecords { get; set; }
        public int totalRecordsD1 { get; set; }
        public int totalRecordsHashD1 { get; set; }
        public int totalRecordsD2 { get; set; }
        public int totalRecordsHashD2 { get; set; }
        public int totalRecordsD3 { get; set; }
        public int totalRecordsHashD3 { get; set; }
        public int totalRecordsD4 { get; set; }
        public int totalRecordsHashD4 { get; set; }

        public int pass1HashTableSize { get; set; }
        public int pass2HashTableSize { get; set; }
        public int pass3HashTableSize { get; set; }






        private int outputRecordsCounter = 0;
        public List<Tuple<long, long>> outputRecordsList = null;
        private const int NUMBER_OF_RECORDS_OUTPUT = 10000;
       
        ParallelOptions po = new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount };
        public long NimbleJoinV1()
        {
            try
            {
                var dateHashTable = new Dictionary<int, string>();
                var customerHashTable = new Dictionary<int, string>();
                var supplierHashTable = new Dictionary<int, string>();
                Stopwatch sw = new Stopwatch();
                #region Key Hashing Phase
                sw.Start();
                List<Date> dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                foreach (var row in dateDimension)
                {
                    if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1999") <= 0)
                    {
                        dateHashTable.Add(row.dDateKey, row.dYear);
                    }
                }
                dateDimension.Clear();

                List<Customer> customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
                foreach (var row in customerDimension)
                {
                    if (row.cRegion.Equals("ASIA"))
                        customerHashTable.Add(row.cCustKey, row.cNation);
                }
                customerDimension.Clear();

                List<Supplier> supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
                foreach (var row in supplierDimension)
                {
                    if (row.sRegion.Equals("ASIA"))
                        supplierHashTable.Add(row.sSuppKey, row.sNation);
                }
                supplierDimension.Clear();
                //sw.Stop();
                //Console.WriteLine("[Nimble Join]: Key Hashing Phase takes {0} ms.", sw.ElapsedMilliseconds);
                //sw.Reset();
                #endregion Key Hashing Phase

                #region Probing Phase
                Stopwatch sw1 = Stopwatch.StartNew();
                //sw.Start();
                List<int> loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));

                var matchedOrderDate = new Dictionary<int, string>();
                //var matchedOrderDate = new FastBitMap(loOrderDate.Count);

                var i = 0;
                foreach (var orderDate in loOrderDate)
                {
                    string dYear = "";
                    if (dateHashTable.TryGetValue(orderDate, out dYear))
                        matchedOrderDate.Add(i + 1, dYear.ToString());
                    //matchedOrderDate.Set(i, true, dYear);
                    i++;
                }
                loOrderDate.Clear();

                //sw.Stop();
                //Console.WriteLine("[Nimble Join]: Probing Phase [matchedOrderDate] takes {0} ms.", sw.ElapsedMilliseconds);
                //sw.Reset();
                //sw.Start();

                List<int> loCustomerKey = Utils.ReadFromBinaryFiles<int>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                var matchedCustomerKey = new Dictionary<int, string>();
                var j = 0;
                foreach (var custKey in loCustomerKey)
                {
                    string cNation = string.Empty;
                    if (customerHashTable.TryGetValue(custKey, out cNation))
                        matchedCustomerKey.Add(j + 1, cNation);
                    j++;
                }
                loCustomerKey.Clear();

                //                sw.Stop();
                //              Console.WriteLine("[Nimble Join]: Probing Phase [matchedCustomerKey] takes {0} ms.", sw.ElapsedMilliseconds);
                //            sw.Reset();
                //sw.Start();
                //
                List<int> loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                var matchedSupplierKey = new Dictionary<int, string>();

                var k = 0;
                foreach (var suppKey in loSupplierKey)
                {
                    string sNation = string.Empty;
                    if (supplierHashTable.TryGetValue(suppKey, out sNation))
                        matchedSupplierKey.Add(k + 1, sNation);
                    k++;
                }
                loSupplierKey.Clear();
                //sw.Stop();
                //Console.WriteLine("[Nimble Join]: Probing Phase [matchedSupplierKey] takes {0} ms.", sw.ElapsedMilliseconds);
                ///sw.Reset();
                //sw.Start();

                var mcSize = matchedCustomerKey.Count();
                var msSize = matchedSupplierKey.Count();
                var moSize = matchedOrderDate.Count();
                var joinOutputIntermediate = new Dictionary<int, string>();

                if (mcSize <= msSize && mcSize <= moSize)
                {
                    foreach (var item in matchedCustomerKey)
                    {
                        string suppNation, orderDate;
                        if (matchedSupplierKey.TryGetValue(item.Key, out suppNation) && matchedOrderDate.TryGetValue(item.Key, out orderDate))
                        {
                            if (isFirst)
                            {
                                sw.Stop();
                                initialResposeTime = sw.ElapsedMilliseconds;
                                isFirst = false;
                                sw.Start();
                            }
                            joinOutputIntermediate.Add(item.Key, item.Value + "," + suppNation + "," + orderDate);
                        }
                    }
                }
                else if (msSize <= moSize)
                {
                    foreach (var item in matchedSupplierKey)
                    {
                        string custNation, orderDate;
                        if (matchedCustomerKey.TryGetValue(item.Key, out custNation) && matchedOrderDate.TryGetValue(item.Key, out orderDate))
                        {
                            if (isFirst)
                            {
                                sw.Stop();
                                initialResposeTime = sw.ElapsedMilliseconds;
                                isFirst = false;
                                sw.Start();
                            }
                            joinOutputIntermediate.Add(item.Key, custNation + "," + item.Value + "," + orderDate);
                        }
                    }
                }
                else
                {
                    foreach (var item in matchedOrderDate)
                    {
                        string custNation, suppNation;
                        if (matchedSupplierKey.TryGetValue(item.Key, out suppNation) && matchedCustomerKey.TryGetValue(item.Key, out custNation))
                        {
                            if (isFirst)
                            {
                                sw.Stop();
                                initialResposeTime = sw.ElapsedMilliseconds;
                                isFirst = false;
                                sw.Start();
                            }
                            joinOutputIntermediate.Add(item.Key, custNation + "," + suppNation + "," + item.Value);
                        }
                    }
                }

                sw1.Stop();
                Console.WriteLine("[Nimble Join]: Probing Phase [JOIN] takes {0} ms.", sw1.ElapsedMilliseconds);

                // sw1.Stop();
                #endregion Probing Phase
                //Console.WriteLine("[Nimble Join]: Probing Phase takes {0} ms.", sw1.ElapsedMilliseconds);

                #region Value Extraction Phase
                //sw.Reset();
                //sw.Start();
                List<int> loRevenue = Utils.ReadFromBinaryFiles<int>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
                var joinOutputFinal = new Dictionary<int, string>();
                foreach (var item in joinOutputIntermediate)
                {
                    joinOutputFinal.Add(item.Key, item.Value + "," + loRevenue[item.Key - 1]); // Direct array lookup
                }
                // sw.Stop();
                //Console.WriteLine("[Nimble Join]: Value Extraction Phase takes {0} ms.", sw.ElapsedMilliseconds);
                #endregion Value Extraction Phase

                sw.Stop();
                //Console.WriteLine("[Nimble Join]: Total time: {0} milli sec.", sw.ElapsedMilliseconds);
                //Console.WriteLine("[Nimble Join]: Total Rows: {0} .", totalRecords);
                //Console.WriteLine("[Nimble Join]: Total Selected Rows: {0}.", joinOutputFinal.Count);
                //Console.WriteLine("[Nimble Join]: Selectivity Ratio: {0}.", (Double)joinOutputFinal.Count / totalRecords);
                return sw.ElapsedMilliseconds;

            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public void resetGlobalVariables()
        {
            isFirst = true;
            phase1Time = 0;
            phase2Time = 0;
            phase3Time = 0;
            phase4Time = 0;
            initialResposeTime = 0;
            totalNumberOfRecords = 0;
            pass3HashTableSize = 0;

        }
        /// <summary>
        /// Supports Cumilative aggregations for SUM and COUNT
        /// </summary>
        /// <returns></returns>
        public void NimbleJoinV2(string selectivityRatio)
        {
            try
            {
                outputRecordsCounter = 0;
                outputRecordsList = new List<Tuple<long, long>>();
                var dateHashTable = new Dictionary<int, string>();
                var customerHashTable = new Dictionary<int, string>();
                var supplierHashTable = new Dictionary<int, string>();
                Stopwatch sw = new Stopwatch();
                #region Key Hashing Phase
                sw.Start();
                List<Date> dateDimension = null;

                List<Supplier> supplierDimension = null;
                List<Customer> customerDimension = null;
                Stopwatch sw1 = new Stopwatch();
                switch (selectivityRatio)
                {
                    case "0.007":
                        sw1.Start();
                        dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                        sw1.Stop();
                        phase11IOTime = sw1.ElapsedMilliseconds;
                        sw1.Reset();
                        sw1.Restart();
                        foreach (var row in dateDimension)
                        {
                            if (row.dYear.Equals("1992"))
                                dateHashTable.Add(row.dDateKey, row.dYear);
                        }
                        sw1.Stop();
                        phase11HashTime = sw1.ElapsedMilliseconds;
                        sw1.Reset();
                        sw1.Restart();
                        customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
                        sw1.Stop();
                        phase12IOTime = sw1.ElapsedMilliseconds;
                        sw1.Reset();
                        sw1.Restart();
                        foreach (var row in customerDimension)
                        {
                            if (row.cRegion.Equals("ASIA"))
                                customerHashTable.Add(row.cCustKey, row.cNation);
                        }
                        sw1.Stop();
                        phase12HashTime = sw1.ElapsedMilliseconds;
                        sw1.Reset();
                        sw1.Restart();
                        supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
                        sw1.Stop();
                        phase13IOTime = sw1.ElapsedMilliseconds;
                        sw1.Reset();
                        sw1.Restart();
                        foreach (var row in supplierDimension)
                        {
                            if (row.sRegion.Equals("ASIA"))
                                supplierHashTable.Add(row.sSuppKey, row.sNation);
                        }
                        sw1.Stop();
                        phase13HashTime = sw1.ElapsedMilliseconds;
                        break;
                    case "0.07":
                        sw1.Start();
                        dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                        sw1.Stop();
                        phase11IOTime = sw1.ElapsedMilliseconds;
                        sw1.Reset();
                        sw1.Restart();
                        foreach (var row in dateDimension)
                        {
                            if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1996") <= 0)
                                dateHashTable.Add(row.dDateKey, row.dYear);
                        }
                        sw1.Stop();
                        phase11HashTime = sw1.ElapsedMilliseconds;
                        sw1.Reset();
                        sw1.Restart();
                        customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
                        sw1.Stop();
                        phase12IOTime = sw1.ElapsedMilliseconds;
                        sw1.Reset();
                        sw1.Restart();
                        foreach (var row in customerDimension)
                        {
                            if (row.cRegion.Equals("ASIA") || row.cRegion.Equals("AFRICA"))
                                customerHashTable.Add(row.cCustKey, row.cNation);
                        }
                        sw1.Stop();
                        phase12HashTime = sw1.ElapsedMilliseconds;
                        sw1.Reset();
                        sw1.Restart();
                        supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
                        sw1.Stop();
                        phase13IOTime = sw1.ElapsedMilliseconds;
                        sw1.Reset();
                        sw1.Restart();
                        foreach (var row in supplierDimension)
                        {
                            if (row.sRegion.Equals("ASIA"))
                                supplierHashTable.Add(row.sSuppKey, row.sNation);
                        }
                        sw1.Stop();
                        phase13HashTime = sw1.ElapsedMilliseconds;

                        break;
                    case "0.7":
                        sw1.Start();
                        dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                        sw1.Stop();
                        phase11IOTime = sw1.ElapsedMilliseconds;
                        sw1.Reset();
                        sw1.Restart();
                        foreach (var row in dateDimension)
                        {
                            if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1999") <= 0)
                                dateHashTable.Add(row.dDateKey, row.dYear);
                        }
                        sw1.Stop();
                        phase11HashTime = sw1.ElapsedMilliseconds;
                        sw1.Reset();
                        sw1.Restart();
                        customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
                        sw1.Stop();
                        phase12IOTime = sw1.ElapsedMilliseconds;
                        sw1.Reset();
                        sw1.Restart();
                        foreach (var row in customerDimension)
                        {
                            if (row.cRegion.Equals("ASIA") || row.cRegion.Equals("AMERICA") || row.cRegion.Equals("EUROPE") || row.cRegion.Equals("AFRICA"))
                                customerHashTable.Add(row.cCustKey, row.cNation);
                        }
                        sw1.Stop();
                        phase12HashTime = sw1.ElapsedMilliseconds;
                        sw1.Reset();
                        sw1.Restart();
                        supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
                        sw1.Stop();
                        phase13IOTime = sw1.ElapsedMilliseconds;
                        sw1.Reset();
                        sw1.Restart();
                        foreach (var row in supplierDimension)
                        {
                            if (row.sRegion.Equals("ASIA") || row.sRegion.Equals("AMERICA") || row.sRegion.Equals("EUROPE") || row.sRegion.Equals("AFRICA"))
                                supplierHashTable.Add(row.sSuppKey, row.sNation);
                        }
                        sw1.Stop();
                        phase13HashTime = sw1.ElapsedMilliseconds;
                        break;
                }

                totalRecordsD1 = customerDimension.Count;
                totalRecordsHashD1 = customerHashTable.Count;
                totalRecordsD2 = dateDimension.Count;
                totalRecordsHashD2 = dateHashTable.Count;
                totalRecordsD3 = supplierDimension.Count;
                totalRecordsHashD3 = supplierHashTable.Count;
                customerDimension.Clear();
                dateDimension.Clear();
                supplierDimension.Clear();

                sw.Stop();
                phase1Time = sw.ElapsedMilliseconds;
                sw.Reset();
                #endregion Key Hashing Phase

                #region Probing Phase
                sw.Start();
                sw1.Start();
                List<int> loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                sw1.Stop();
                phase21IOTime = sw1.ElapsedMilliseconds;
                sw1.Reset();
                sw1.Restart();
                var intermediateHashTable = new Dictionary<int, string>();
                var i = 0;
                foreach (var orderDate in loOrderDate)
                {
                    string dYear = "";
                    if (dateHashTable.TryGetValue(orderDate, out dYear))
                    {
                        intermediateHashTable.Add(i, dYear);
                    }
                    i++;
                }
                loOrderDate.Clear();
                sw1.Stop();
                phase21HashTime = sw1.ElapsedMilliseconds;
                pass1HashTableSize = intermediateHashTable.Count;
                sw1.Reset();
                sw1.Restart();
                List<int> loCustomerKey = Utils.ReadFromBinaryFiles<int>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                sw1.Stop();
                phase22IOTime = sw1.ElapsedMilliseconds;
                sw1.Reset();
                sw1.Restart();
                var j = 0;
                foreach (var custKey in loCustomerKey)
                {
                    string cNation = string.Empty;
                    if (customerHashTable.TryGetValue(custKey, out cNation))
                    {
                        string values = string.Empty;
                        if (intermediateHashTable.TryGetValue(j, out values))
                        {
                            intermediateHashTable[j] = values + ", " + cNation;
                        }
                    }
                    else
                    {
                        intermediateHashTable.Remove(j);
                    }
                    j++;
                }
                loCustomerKey.Clear();
                sw1.Stop();
                phase22HashTime = sw1.ElapsedMilliseconds;
                pass2HashTableSize = intermediateHashTable.Count;
                sw1.Reset();
                sw1.Restart();
                List<int> loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                sw1.Stop();
                phase23IOTime = sw1.ElapsedMilliseconds;
                sw1.Reset();
                sw1.Restart();
                var k = 0;
                foreach (var suppKey in loSupplierKey)
                {
                    string sNation = string.Empty;
                    if (supplierHashTable.TryGetValue(suppKey, out sNation))
                    {
                        string values = string.Empty;
                        if (intermediateHashTable.TryGetValue(k, out values))
                        {
                            intermediateHashTable[k] = values + ", " + sNation;
                            if (isFirst)
                            {
                                sw.Stop();
                                initialResposeTime = sw.ElapsedMilliseconds;
                                isFirst = false;
                                sw.Start();
                            }
                            //Console.WriteLine(k +", "+ values.ToString() + ", " + sNation);
                            outputRecordsCounter++;
                            if (outputRecordsCounter % NUMBER_OF_RECORDS_OUTPUT == 0)
                            {
                                sw.Stop();
                                outputRecordsList.Add(new Tuple<long, long>(outputRecordsCounter, sw.ElapsedMilliseconds));
                                sw.Start();

                            }
                        }
                    }
                    else
                    {
                        intermediateHashTable.Remove(k);
                    }
                    k++;
                }
                loSupplierKey.Clear();
                sw1.Stop();
                phase23HashTime = sw1.ElapsedMilliseconds;
                sw.Stop();
                phase2Time = sw.ElapsedMilliseconds;
                totalNumberOfRecords = i;
                pass3HashTableSize = intermediateHashTable.Count;
                sw.Reset();
                sw1.Reset();

                #endregion Probing Phase

                #region Value Extraction Phase
                sw.Start();
                List<int> loRevenue = Utils.ReadFromBinaryFiles<int>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
                var joinOutputFinal = new Dictionary<int, string>();
                foreach (var item in intermediateHashTable)
                {
                    joinOutputFinal.Add(item.Key, item.Value + ", " + loRevenue[item.Key]); // Direct array lookup
                }
                sw.Stop();
                phase3Time = sw.ElapsedMilliseconds;
                #endregion Value Extraction Phase
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public void NimbleJoinV3(string selectivityRatio)
        {
            try
            {
                outputRecordsCounter = 0;
                outputRecordsList = new List<Tuple<long, long>>();
                var dateHashTable = new Dictionary<int, string>();
                var customerHashTable = new Dictionary<int, string>();
                var supplierHashTable = new Dictionary<int, string>();
                Stopwatch sw = new Stopwatch();
                Stopwatch sw2 = new Stopwatch();
                #region Key Hashing Phase
                sw.Start();
                sw2.Start();
                List<Date> dateDimension = null;

                List<Supplier> supplierDimension = null;
                List<Customer> customerDimension = null;
                Stopwatch sw1 = new Stopwatch();
                switch (selectivityRatio)
                {
                    case "0.007":
                        sw1.Start();
                        dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                        sw1.Stop();
                        phase11IOTime = sw1.ElapsedMilliseconds;
                        sw1.Reset();
                        sw1.Restart();
                        foreach (var row in dateDimension)
                        {
                            if (row.dYear.Equals("1992"))
                                dateHashTable.Add(row.dDateKey, row.dYear);
                        }
                        sw1.Stop();
                        phase11HashTime = sw1.ElapsedMilliseconds;
                        sw1.Reset();
                        sw1.Restart();
                        customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
                        sw1.Stop();
                        phase12IOTime = sw1.ElapsedMilliseconds;
                        sw1.Reset();
                        sw1.Restart();
                        foreach (var row in customerDimension)
                        {
                            if (row.cRegion.Equals("ASIA"))
                                customerHashTable.Add(row.cCustKey, row.cNation);
                        }
                        sw1.Stop();
                        phase12HashTime = sw1.ElapsedMilliseconds;
                        sw1.Reset();
                        sw1.Restart();
                        supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
                        sw1.Stop();
                        phase13IOTime = sw1.ElapsedMilliseconds;
                        sw1.Reset();
                        sw1.Restart();
                        foreach (var row in supplierDimension)
                        {
                            if (row.sRegion.Equals("ASIA"))
                                supplierHashTable.Add(row.sSuppKey, row.sNation);
                        }
                        sw1.Stop();
                        phase13HashTime = sw1.ElapsedMilliseconds;
                        break;
                    case "0.07":
                        sw1.Start();
                        dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                        sw1.Stop();
                        phase11IOTime = sw1.ElapsedMilliseconds;
                        sw1.Reset();
                        sw1.Restart();
                        foreach (var row in dateDimension)
                        {
                            if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1996") <= 0)
                                dateHashTable.Add(row.dDateKey, row.dYear);
                        }
                        sw1.Stop();
                        phase11HashTime = sw1.ElapsedMilliseconds;
                        sw1.Reset();
                        sw1.Restart();
                        customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
                        sw1.Stop();
                        phase12IOTime = sw1.ElapsedMilliseconds;
                        sw1.Reset();
                        sw1.Restart();
                        foreach (var row in customerDimension)
                        {
                            if (row.cRegion.Equals("ASIA") || row.cRegion.Equals("AFRICA"))
                                customerHashTable.Add(row.cCustKey, row.cNation);
                        }
                        sw1.Stop();
                        phase12HashTime = sw1.ElapsedMilliseconds;
                        sw1.Reset();
                        sw1.Restart();
                        supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
                        sw1.Stop();
                        phase13IOTime = sw1.ElapsedMilliseconds;
                        sw1.Reset();
                        sw1.Restart();
                        foreach (var row in supplierDimension)
                        {
                            if (row.sRegion.Equals("ASIA"))
                                supplierHashTable.Add(row.sSuppKey, row.sNation);
                        }
                        sw1.Stop();
                        phase13HashTime = sw1.ElapsedMilliseconds;

                        break;
                    case "0.7":
                        sw1.Start();
                        dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                        sw1.Stop();
                        phase11IOTime = sw1.ElapsedMilliseconds;
                        sw1.Reset();
                        sw1.Restart();
                        foreach (var row in dateDimension)
                        {
                            if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1999") <= 0)
                                dateHashTable.Add(row.dDateKey, row.dYear);
                        }
                        sw1.Stop();
                        phase11HashTime = sw1.ElapsedMilliseconds;
                        sw1.Reset();
                        sw1.Restart();
                        customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
                        sw1.Stop();
                        phase12IOTime = sw1.ElapsedMilliseconds;
                        sw1.Reset();
                        sw1.Restart();
                        foreach (var row in customerDimension)
                        {
                            if (row.cRegion.Equals("ASIA") || row.cRegion.Equals("AMERICA") || row.cRegion.Equals("EUROPE") || row.cRegion.Equals("AFRICA"))
                                customerHashTable.Add(row.cCustKey, row.cNation);
                        }
                        sw1.Stop();
                        phase12HashTime = sw1.ElapsedMilliseconds;
                        sw1.Reset();
                        sw1.Restart();
                        supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
                        sw1.Stop();
                        phase13IOTime = sw1.ElapsedMilliseconds;
                        sw1.Reset();
                        sw1.Restart();
                        foreach (var row in supplierDimension)
                        {
                            if (row.sRegion.Equals("ASIA") || row.sRegion.Equals("AMERICA") || row.sRegion.Equals("EUROPE") || row.sRegion.Equals("AFRICA"))
                                supplierHashTable.Add(row.sSuppKey, row.sNation);
                        }
                        sw1.Stop();
                        phase13HashTime = sw1.ElapsedMilliseconds;
                        break;
                }

                totalRecordsD1 = customerDimension.Count;
                totalRecordsHashD1 = customerHashTable.Count;
                totalRecordsD2 = dateDimension.Count;
                totalRecordsHashD2 = dateHashTable.Count;
                totalRecordsD3 = supplierDimension.Count;
                totalRecordsHashD3 = supplierHashTable.Count;
                customerDimension.Clear();
                dateDimension.Clear();
                supplierDimension.Clear();

                sw.Stop();
                phase1Time = sw.ElapsedMilliseconds;
                sw.Reset();
                #endregion Key Hashing Phase

                #region Probing Phase
                sw.Start();
                sw1.Start();
                List<int> loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                sw1.Stop();
                phase21IOTime = sw1.ElapsedMilliseconds;
                sw1.Reset();
                sw1.Restart();
                var listOrderDatePositions = new List<Pair>();
                var i = 0;
                foreach (var orderDate in loOrderDate)
                {
                    string dYear = "";
                    if (dateHashTable.TryGetValue(orderDate, out dYear))
                    {
                        listOrderDatePositions.Add(new Pair(i, dYear));
                    }
                    i++;
                }
                loOrderDate.Clear();
                dateHashTable.Clear();
                sw1.Stop();
                phase21HashTime = sw1.ElapsedMilliseconds;
                pass1HashTableSize = listOrderDatePositions.Count;
                sw1.Reset();
                sw1.Restart();
                List<int> loCustomerKey = Utils.ReadFromBinaryFiles<int>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                sw1.Stop();
                phase22IOTime = sw1.ElapsedMilliseconds;
                sw1.Reset();
                sw1.Restart();
                var listCustomerKeyPositions = new List<Pair>();
                var j = 0;
                foreach (var custKey in loCustomerKey)
                {
                    string cNationOut = string.Empty;
                    if (customerHashTable.TryGetValue(custKey, out cNationOut))
                    {
                        listCustomerKeyPositions.Add(new Pair(j, cNationOut));
                    }
                    j++;
                }
                loCustomerKey.Clear();
                customerHashTable.Clear();
                sw1.Stop();
                phase22HashTime = sw1.ElapsedMilliseconds;
                pass2HashTableSize = listCustomerKeyPositions.Count;
                sw1.Reset();
                sw1.Restart();
                List<int> loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                sw1.Stop();
                phase23IOTime = sw1.ElapsedMilliseconds;
                sw1.Reset();
                sw1.Restart();
                var listSupplierKeyPositions = new List<Pair>();
                var k = 0;
                foreach (var suppKey in loSupplierKey)
                {
                    string sNationOut = string.Empty;
                    if (supplierHashTable.TryGetValue(suppKey, out sNationOut))
                    {
                        listSupplierKeyPositions.Add(new Pair(k, sNationOut));
                    }
                    k++;
                }
                loSupplierKey.Clear();
                supplierHashTable.Clear();
                sw1.Stop();
                phase23HashTime = sw1.ElapsedMilliseconds;
                sw.Stop();
                phase2Time = sw.ElapsedMilliseconds;
                totalNumberOfRecords = i;
                pass3HashTableSize = listSupplierKeyPositions.Count;
                sw.Reset();
                sw1.Reset();

                var common = listCustomerKeyPositions.Intersect<Pair>(listOrderDatePositions, new ComparePair()).Intersect<Pair>(listSupplierKeyPositions, new ComparePair()).ToList();
                #endregion Probing Phase


                #region Value Extraction Phase
                sw.Start();
                List<int> loRevenue = Utils.ReadFromBinaryFiles<int>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
                var joinOutputFinal = new Dictionary<int, int>();
                foreach (var item in common)
                {
                    joinOutputFinal.Add(item.id, loRevenue[item.id]); // Direct array lookup
                }
                sw.Stop();
                phase3Time = sw.ElapsedMilliseconds;
                #endregion Value Extraction Phase
                sw2.Stop();
                Console.WriteLine("[Invisble Join]:Time taken {0} ms.", sw2.ElapsedMilliseconds);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public void InvisibleJoinV2(string selectivityRatio)
        {
            try
            {
                outputRecordsCounter = 0;
                outputRecordsList = new List<Tuple<long, long>>();
                var dateHashTable = new Dictionary<int, string>();
                var customerHashTable = new Dictionary<int, string>();
                var supplierHashTable = new Dictionary<int, string>();
                Stopwatch sw = new Stopwatch();
                Stopwatch sw2 = new Stopwatch();
                #region Key Hashing Phase
                sw.Start();
                sw2.Start();
                List<Date> dateDimension = null;

                List<Supplier> supplierDimension = null;
                List<Customer> customerDimension = null;
                Stopwatch sw1 = new Stopwatch();
                switch (selectivityRatio)
                {
                    case "0.007":
                        sw1.Start();
                        dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                        sw1.Stop();
                        phase11IOTime = sw1.ElapsedMilliseconds;
                        sw1.Reset();
                        sw1.Restart();
                        foreach (var row in dateDimension)
                        {
                            if (row.dYear.Equals("1992"))
                                dateHashTable.Add(row.dDateKey, row.dYear);
                        }
                        sw1.Stop();
                        phase11HashTime = sw1.ElapsedMilliseconds;
                        sw1.Reset();
                        sw1.Restart();
                        customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
                        sw1.Stop();
                        phase12IOTime = sw1.ElapsedMilliseconds;
                        sw1.Reset();
                        sw1.Restart();
                        foreach (var row in customerDimension)
                        {
                            if (row.cRegion.Equals("ASIA"))
                                customerHashTable.Add(row.cCustKey, row.cNation);
                        }
                        sw1.Stop();
                        phase12HashTime = sw1.ElapsedMilliseconds;
                        sw1.Reset();
                        sw1.Restart();
                        supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
                        sw1.Stop();
                        phase13IOTime = sw1.ElapsedMilliseconds;
                        sw1.Reset();
                        sw1.Restart();
                        foreach (var row in supplierDimension)
                        {
                            if (row.sRegion.Equals("ASIA"))
                                supplierHashTable.Add(row.sSuppKey, row.sNation);
                        }
                        sw1.Stop();
                        phase13HashTime = sw1.ElapsedMilliseconds;
                        break;
                    case "0.07":
                        sw1.Start();
                        dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                        sw1.Stop();
                        phase11IOTime = sw1.ElapsedMilliseconds;
                        sw1.Reset();
                        sw1.Restart();
                        foreach (var row in dateDimension)
                        {
                            if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1996") <= 0)
                                dateHashTable.Add(row.dDateKey, row.dYear);
                        }
                        sw1.Stop();
                        phase11HashTime = sw1.ElapsedMilliseconds;
                        sw1.Reset();
                        sw1.Restart();
                        customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
                        sw1.Stop();
                        phase12IOTime = sw1.ElapsedMilliseconds;
                        sw1.Reset();
                        sw1.Restart();
                        foreach (var row in customerDimension)
                        {
                            if (row.cRegion.Equals("ASIA") || row.cRegion.Equals("AFRICA"))
                                customerHashTable.Add(row.cCustKey, row.cNation);
                        }
                        sw1.Stop();
                        phase12HashTime = sw1.ElapsedMilliseconds;
                        sw1.Reset();
                        sw1.Restart();
                        supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
                        sw1.Stop();
                        phase13IOTime = sw1.ElapsedMilliseconds;
                        sw1.Reset();
                        sw1.Restart();
                        foreach (var row in supplierDimension)
                        {
                            if (row.sRegion.Equals("ASIA"))
                                supplierHashTable.Add(row.sSuppKey, row.sNation);
                        }
                        sw1.Stop();
                        phase13HashTime = sw1.ElapsedMilliseconds;

                        break;
                    case "0.7":
                        sw1.Start();
                        dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                        sw1.Stop();
                        phase11IOTime = sw1.ElapsedMilliseconds;
                        sw1.Reset();
                        sw1.Restart();
                        foreach (var row in dateDimension)
                        {
                            if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1999") <= 0)
                                dateHashTable.Add(row.dDateKey, row.dYear);
                        }
                        sw1.Stop();
                        phase11HashTime = sw1.ElapsedMilliseconds;
                        sw1.Reset();
                        sw1.Restart();
                        customerDimension = Utils.ReadFromBinaryFiles<Customer>(customerFile.Replace("BF", "BF" + scaleFactor));
                        sw1.Stop();
                        phase12IOTime = sw1.ElapsedMilliseconds;
                        sw1.Reset();
                        sw1.Restart();
                        foreach (var row in customerDimension)
                        {
                            if (row.cRegion.Equals("ASIA") || row.cRegion.Equals("AMERICA") || row.cRegion.Equals("EUROPE") || row.cRegion.Equals("AFRICA"))
                                customerHashTable.Add(row.cCustKey, row.cNation);
                        }
                        sw1.Stop();
                        phase12HashTime = sw1.ElapsedMilliseconds;
                        sw1.Reset();
                        sw1.Restart();
                        supplierDimension = Utils.ReadFromBinaryFiles<Supplier>(supplierFile.Replace("BF", "BF" + scaleFactor));
                        sw1.Stop();
                        phase13IOTime = sw1.ElapsedMilliseconds;
                        sw1.Reset();
                        sw1.Restart();
                        foreach (var row in supplierDimension)
                        {
                            if (row.sRegion.Equals("ASIA") || row.sRegion.Equals("AMERICA") || row.sRegion.Equals("EUROPE") || row.sRegion.Equals("AFRICA"))
                                supplierHashTable.Add(row.sSuppKey, row.sNation);
                        }
                        sw1.Stop();
                        phase13HashTime = sw1.ElapsedMilliseconds;
                        break;
                }

                totalRecordsD1 = customerDimension.Count;
                totalRecordsHashD1 = customerHashTable.Count;
                totalRecordsD2 = dateDimension.Count;
                totalRecordsHashD2 = dateHashTable.Count;
                totalRecordsD3 = supplierDimension.Count;
                totalRecordsHashD3 = supplierHashTable.Count;
                customerDimension.Clear();
                dateDimension.Clear();
                supplierDimension.Clear();

                sw.Stop();
                phase1Time = sw.ElapsedMilliseconds;
                sw.Reset();
                #endregion Key Hashing Phase

                #region Probing Phase
                sw.Start();
                sw1.Start();
                List<int> loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                sw1.Stop();
                phase21IOTime = sw1.ElapsedMilliseconds;
                sw1.Reset();
                sw1.Restart();
                var listOrderDatePositions = new List<int>();
                var i = 0;
                foreach (var orderDate in loOrderDate)
                {
                    string dYear = "";
                    if (dateHashTable.TryGetValue(orderDate, out dYear))
                    {
                        listOrderDatePositions.Add(i);
                    }
                    i++;
                }
                loOrderDate.Clear();
                dateHashTable.Clear();
                sw1.Stop();
                phase21HashTime = sw1.ElapsedMilliseconds;
                pass1HashTableSize = listOrderDatePositions.Count;
                sw1.Reset();
                sw1.Restart();
                List<int> loCustomerKey = Utils.ReadFromBinaryFiles<int>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                sw1.Stop();
                phase22IOTime = sw1.ElapsedMilliseconds;
                sw1.Reset();
                sw1.Restart();
                var listCustomerKeyPositions = new List<int>();
                var j = 0;
                foreach (var custKey in loCustomerKey)
                {
                    string cNationOut = string.Empty;
                    if (customerHashTable.TryGetValue(custKey, out cNationOut))
                    {
                        listCustomerKeyPositions.Add(j);
                    }
                    j++;
                }
                loCustomerKey.Clear();
                customerHashTable.Clear();
                sw1.Stop();
                phase22HashTime = sw1.ElapsedMilliseconds;
                pass2HashTableSize = listCustomerKeyPositions.Count;
                sw1.Reset();
                sw1.Restart();
                List<int> loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                sw1.Stop();
                phase23IOTime = sw1.ElapsedMilliseconds;
                sw1.Reset();
                sw1.Restart();
                var listSupplierKeyPositions = new List<int>();
                var k = 0;
                foreach (var suppKey in loSupplierKey)
                {
                    string sNationOut = string.Empty;
                    if (supplierHashTable.TryGetValue(suppKey, out sNationOut))
                    {
                        listSupplierKeyPositions.Add(k);
                    }
                    k++;
                }
                loSupplierKey.Clear();
                supplierHashTable.Clear();
                sw1.Stop();
                phase23HashTime = sw1.ElapsedMilliseconds;
                sw.Stop();
                phase2Time = sw.ElapsedMilliseconds;
                totalNumberOfRecords = i;
                pass3HashTableSize = listSupplierKeyPositions.Count;
                sw.Reset();
                sw1.Reset();

                var common = listCustomerKeyPositions.Intersect(listOrderDatePositions).Intersect(listSupplierKeyPositions).ToList();
                #endregion Probing Phase


                #region Value Extraction Phase
                sw.Start();
                loOrderDate = Utils.ReadFromBinaryFiles<int>(loOrderDateFile.Replace("BF", "BF" + scaleFactor));
                loCustomerKey = Utils.ReadFromBinaryFiles<int>(loCustKeyFile.Replace("BF", "BF" + scaleFactor));
                // loCommitDate = Utils.ReadFromBinaryFiles<int>(binaryFilesDirectory + loCommitDateFile);
                loSupplierKey = Utils.ReadFromBinaryFiles<int>(loSuppKeyFile.Replace("BF", "BF" + scaleFactor));
                // loSupplyCost = Utils.ReadFromBinaryFiles<int>(binaryFilesDirectory + loSupplyCostFile);
                // loPartKey = Utils.ReadFromBinaryFiles<int>(binaryFilesDirectory + loPartKeyFile);
                List<string> cNation = Utils.ReadFromBinaryFiles<string>(cNationFile.Replace("BF", "BF" + scaleFactor));
                List<string> sNation = Utils.ReadFromBinaryFiles<string>(sNationFile.Replace("BF", "BF" + scaleFactor));
                // Use Date Hash Table
                //dateDimension = Utils.ReadFromBinaryFile<List<Date>>(dateFile);
                switch (selectivityRatio)
                {
                    case "0.007":
                        dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                        foreach (var row in dateDimension)
                        {
                            if (row.dYear.Equals("1992"))
                                dateHashTable.Add(row.dDateKey, row.dYear);
                        }
                        break;
                    case "0.07":
                        sw1.Start();
                        dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                        foreach (var row in dateDimension)
                        {
                            if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1996") <= 0)
                                dateHashTable.Add(row.dDateKey, row.dYear);
                        }
                        break;
                    case "0.7":
                        dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                        foreach (var row in dateDimension)
                        {
                            if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1999") <= 0)
                                dateHashTable.Add(row.dDateKey, row.dYear);
                        }
                        break;
                }
                dateDimension.Clear();
                List<int> loRevenue = Utils.ReadFromBinaryFiles<int>(loRevenueFile.Replace("BF", "BF" + scaleFactor));
                var joinOutputIntermediate = new Dictionary<int, string>();
                foreach (int index in common)
                {

                    try
                    {
                        var dateKey = loOrderDate[index];
                        var custKey = loCustomerKey[index];
                        var suppKey = loSupplierKey[index];

                        // Position Look UP
                        string dYear;
                        dateHashTable.TryGetValue(dateKey, out dYear);

                        string cNationOut = cNation[custKey];
                        string sNationOut = sNation[suppKey];
                        if (isFirst)
                        {
                            sw.Stop();
                            initialResposeTime = sw.ElapsedMilliseconds;
                            isFirst = false;
                            sw.Start();
                        }
                        // Console.WriteLine(l +", "+ dYear  + ", " + sNationOut + ", " + cNationOut);
                        joinOutputIntermediate.Add(index, cNationOut + "," + sNationOut + "," + dYear);

                    }
                    catch (Exception)
                    {
                        throw;
                    }
                }
                var joinOutputFinal = new Dictionary<int, string>();
                foreach (var item in joinOutputIntermediate)
                {
                    joinOutputFinal.Add(item.Key, item.Value + "," + loRevenue[item.Key]);
                }
                sw.Stop();
                // Console.WriteLine("[Invisble Join]: Value Extraction Phase took {0} ms.", sw.ElapsedMilliseconds);
                phase3Time = sw.ElapsedMilliseconds;
                #endregion Value Extraction Phase
                sw2.Stop();
                Console.WriteLine("[Invisble Join]:Time taken {0} ms.", sw2.ElapsedMilliseconds);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public Dictionary<int, string> getSmallestDictionary(List<Dictionary<int, string>> listOfDictionaries)
        {
            int smallest = -1;
            int i = 0;
            foreach (var dict in listOfDictionaries)
            {
                if (smallest == -1)
                {
                    smallest = dict.Count;
                }
                else if (dict.Count < smallest)
                {
                    smallest = dict.Count;
                    i++;
                }
            }
            return listOfDictionaries[i];
        }

        public void ParallelXYZJoin()
        {
            try
            {
                var dateHashTable = new Dictionary<int, string>();
                var customerHashTable = new Dictionary<int, string>();
                var supplierHashTable = new Dictionary<int, string>();
                Stopwatch sw = new Stopwatch();

                #region Key Hashing Phase
                sw.Start();
                Parallel.Invoke(po, () =>
                {
                    List<Date> dateDimension = Utils.ReadFromBinaryFile<List<Date>>(dateFile);
                    foreach (var row in dateDimension)
                    {
                        if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
                            dateHashTable.Add(row.dDateKey, row.dYear);
                    }
                    dateDimension.Clear();
                },
                () =>
                {
                    List<Customer> customerDimension = Utils.ReadFromBinaryFile<List<Customer>>(customerFile);
                    foreach (var row in customerDimension)
                    {
                        if (row.cRegion.Equals("ASIA"))
                            customerHashTable.Add(row.cCustKey, row.cNation);
                    }
                    customerDimension.Clear();
                },
                () =>
                {
                    List<Supplier> supplierDimension = Utils.ReadFromBinaryFile<List<Supplier>>(supplierFile);
                    foreach (var row in supplierDimension)
                    {
                        if (row.sRegion.Equals("ASIA"))
                            supplierHashTable.Add(row.sSuppKey, row.sNation);
                    }
                    supplierDimension.Clear();
                });
                sw.Stop();
                #endregion Key Hashing Phase

                Console.WriteLine("[Parallel XYZ Join]: Key Hashing Phase took {0} ms.", sw.ElapsedMilliseconds);
                sw.Reset();

                #region Probing Phase
                sw.Start();
                var matchedOrderDate = new Dictionary<int, string>();
                var matchedCustomerKey = new Dictionary<int, string>();
                var matchedSupplierKey = new Dictionary<int, string>();
                Parallel.Invoke(po, () =>
                 {
                     List<int> loOrderDate = Utils.ReadFromBinaryFile<List<int>>(loOrderDateFile);

                     var i = 0;
                     foreach (var orderDate in loOrderDate)
                     {
                         if (dateHashTable.ContainsKey(orderDate))
                         {
                             string dYear = "";
                             dateHashTable.TryGetValue(orderDate, out dYear);
                             matchedOrderDate.Add(i + 1, dYear);
                         }
                         i++;
                     }
                     loOrderDate.Clear();
                 },
                () =>
                {
                    List<int> loCustomerKey = Utils.ReadFromBinaryFile<List<int>>(loCustKeyFile);
                    var j = 0;
                    foreach (var custKey in loCustomerKey)
                    {
                        if (customerHashTable.ContainsKey(custKey))
                        {
                            string cNation = string.Empty;
                            customerHashTable.TryGetValue(custKey, out cNation);
                            matchedCustomerKey.Add(j + 1, cNation);
                        }
                        j++;
                    }
                    loCustomerKey.Clear();
                },
                () =>
                {
                    List<int> loSupplierKey = Utils.ReadFromBinaryFile<List<int>>(loSuppKeyFile);
                    var k = 0;
                    foreach (var suppKey in loSupplierKey)
                    {
                        if (supplierHashTable.ContainsKey(suppKey))
                        {
                            string sNation = string.Empty;
                            supplierHashTable.TryGetValue(suppKey, out sNation);
                            matchedSupplierKey.Add(k + 1, sNation);
                        }
                        k++;
                    }
                    loSupplierKey.Clear();
                });

                // Left Bushy Tree
                var joinOutputIntermediate1 = matchedSupplierKey.Where(x => matchedCustomerKey.ContainsKey(x.Key))
                    .ToDictionary(x => x.Key, x => x.Value + "," + matchedCustomerKey[x.Key]);

                var joinOutputIntermediate2 = joinOutputIntermediate1.Where(x => matchedOrderDate.ContainsKey(x.Key))
                    .ToDictionary(x => x.Key, x => x.Value + "," + matchedOrderDate[x.Key]);
                sw.Stop();
                #endregion Probing Phase

                Console.WriteLine("[Parallel XYZ Join]: Key Hashing Phase took {0} ms.", sw.ElapsedMilliseconds);
                sw.Reset();

                #region Value Extraction Phase
                sw.Start();
                List<int> loRevenue = Utils.ReadFromBinaryFile<List<int>>(loRevenueFile);

                Dictionary<int, string> joinOutputFinal = new Dictionary<int, string>();
                foreach (var item in joinOutputIntermediate2)
                {
                    joinOutputFinal.Add(item.Key, item.Value + "," + loRevenue[item.Key]);
                }

                sw.Stop();
                #endregion Value Extraction Phase
                Console.WriteLine("[Parallel XYZ Join]: Key Hashing Phase took {0} ms.", sw.ElapsedMilliseconds);
                Console.WriteLine("[Parallel XYZ Join]: Total Rows {0}.", joinOutputFinal.Count);

            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public double InvisibleJoin()
        {
            try
            {
                var dateHashTable = new Dictionary<int, string>();
                var customerHashTable = new Dictionary<int, string>();
                var supplierHashTable = new Dictionary<int, string>();
                Stopwatch sw = new Stopwatch();
                #region Key Hashing Phase
                sw.Start();
                List<Date> dateDimension = Utils.ReadFromBinaryFile<List<Date>>(dateFile);
                foreach (var row in dateDimension)
                {
                    if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
                        dateHashTable.Add(row.dDateKey, row.dYear);
                }
                dateDimension.Clear();

                List<Customer> customerDimension = Utils.ReadFromBinaryFile<List<Customer>>(customerFile);
                foreach (var row in customerDimension)
                {
                    if (row.cRegion.Equals("ASIA"))
                        customerHashTable.Add(row.cCustKey, row.cNation);
                }
                customerDimension.Clear();

                List<Supplier> supplierDimension = Utils.ReadFromBinaryFile<List<Supplier>>(supplierFile);
                foreach (var row in supplierDimension)
                {
                    if (row.sRegion.Equals("ASIA"))
                        supplierHashTable.Add(row.sSuppKey, row.sNation);
                }
                supplierDimension.Clear();
                // sw.Stop();
                #endregion Key Hashing Phase

                // Console.WriteLine("[Invisble Join]: Key Hashing Phase took {0} ms.", sw.ElapsedMilliseconds);
                // sw.Reset();

                #region Probing Phase
                // sw.Start();
                // Stopwatch sw1 = new Stopwatch();
                // sw1.Start();
                List<int> loOrderDate = Utils.ReadFromBinaryFiles<int>(binaryFilesDirectory + loOrderDateFile);

                var arraySize = loOrderDate.Count;
                BitArray baOrderDate = new BitArray(arraySize);
                var i = 0;
                foreach (var orderDate in loOrderDate)
                {
                    string date;
                    if (dateHashTable.TryGetValue(orderDate, out date))
                        baOrderDate.Set(i, true);
                    i++;
                }
                loOrderDate.Clear();
                dateHashTable.Clear();

                // sw1.Stop();
                // Console.WriteLine("[Invisble Join]: Probing Phase (baOrderDate) took {0} ms.", sw1.ElapsedMilliseconds);
                // sw1.Reset();
                // sw1.Start();

                List<int> loCustomerKey = Utils.ReadFromBinaryFiles<int>(binaryFilesDirectory + loCustKeyFile);
                BitArray baCustomerKey = new BitArray(arraySize);
                var j = 0;
                foreach (var custKey in loCustomerKey)
                {
                    string custNation;
                    if (customerHashTable.TryGetValue(custKey, out custNation))
                        baCustomerKey.Set(j, true);
                    j++;
                }
                loCustomerKey.Clear();
                customerHashTable.Clear();

                // sw1.Stop();
                // Console.WriteLine("[Invisble Join]: Probing Phase (baCustomerKey) took {0} ms.", sw1.ElapsedMilliseconds);
                // sw1.Reset();
                // sw1.Start();

                List<int> loSupplierKey = Utils.ReadFromBinaryFiles<int>(binaryFilesDirectory + loSuppKeyFile);
                BitArray baSupplierKey = new BitArray(arraySize);
                var k = 0;
                foreach (var suppKey in loSupplierKey)
                {
                    string suppNation;
                    if (supplierHashTable.TryGetValue(suppKey, out suppNation))
                        baSupplierKey.Set(k, true);
                    k++;
                }
                loSupplierKey.Clear();
                supplierHashTable.Clear();

                // sw1.Stop();
                // Console.WriteLine("[Invisble Join]: Probing Phase (baSupplierKey) took {0} ms.", sw1.ElapsedMilliseconds);
                // sw1.Reset();
                // sw1.Start();

                baCustomerKey.And(baSupplierKey);
                baCustomerKey.And(baOrderDate);

                // sw1.Stop();
                // Console.WriteLine("[Invisble Join]: Probing Phase (BITWISE AND) took {0} ms.", sw1.ElapsedMilliseconds);
                // sw.Stop();
                #endregion Probing Phase

                // Console.WriteLine("[Invisble Join]: Probing Phase took {0} ms.", sw.ElapsedMilliseconds);
                // sw.Reset();

                #region Value Extraction Phase
                // sw.Start();
                loOrderDate = Utils.ReadFromBinaryFiles<int>(binaryFilesDirectory + loOrderDateFile);
                loCustomerKey = Utils.ReadFromBinaryFiles<int>(binaryFilesDirectory + loCustKeyFile);
                loCommitDate = Utils.ReadFromBinaryFiles<int>(binaryFilesDirectory + loCommitDateFile);
                loSupplierKey = Utils.ReadFromBinaryFiles<int>(binaryFilesDirectory + loSuppKeyFile);
                loSupplyCost = Utils.ReadFromBinaryFiles<int>(binaryFilesDirectory + loSupplyCostFile);
                loPartKey = Utils.ReadFromBinaryFiles<int>(binaryFilesDirectory + loPartKeyFile);
                List<string> cNation = Utils.ReadFromBinaryFiles<string>(binaryFilesDirectory + cNationFile);
                List<string> sNation = Utils.ReadFromBinaryFiles<string>(binaryFilesDirectory + sNationFile);
                // Use Date Hash Table
                dateDimension = Utils.ReadFromBinaryFiles<Date>(dateFile.Replace("BF", "BF" + scaleFactor));
                foreach (var row in dateDimension)
                {
                    if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
                        dateHashTable.Add(row.dDateKey, row.dYear);
                }
                dateDimension.Clear();
                List<int> loRevenue = Utils.ReadFromBinaryFiles<int>(binaryFilesDirectory + loRevenueFile);

                var joinOutputIntermediate = new Dictionary<int, string>();
                var l = 0;
                foreach (bool bitValue in baCustomerKey)
                {
                    if (bitValue)
                    {
                        try
                        {
                            var dateKey = loOrderDate[l];
                            var custKey = loCustomerKey[l];
                            var suppKey = loSupplierKey[l];

                            // Position Look UP
                            string dYear;
                            dateHashTable.TryGetValue(dateKey, out dYear);

                            string cNationOut = cNation[custKey];
                            string sNationOut = sNation[suppKey - 1];
                            if (isFirst)
                            {
                                sw.Stop();
                                initialResposeTime = sw.ElapsedMilliseconds;
                                isFirst = false;
                                sw.Start();
                            }
                            // Console.WriteLine(l +", "+ dYear  + ", " + sNationOut + ", " + cNationOut);
                            joinOutputIntermediate.Add(l, cNationOut + "," + sNationOut + "," + dYear);

                        }
                        catch (Exception)
                        {
                            throw;
                        }

                    }
                    l++;
                }

                var joinOutputFinal = new Dictionary<int, string>();
                foreach (var item in joinOutputIntermediate)
                {
                    joinOutputFinal.Add(item.Key, item.Value + "," + loRevenue[item.Key]);
                }
                //sw.Stop();
                //Console.WriteLine("[Invisble Join]: Value Extraction Phase took {0} ms.", sw.ElapsedMilliseconds);
                #endregion Value Extraction Phase
                sw.Stop();
                //Console.WriteLine("[Invisble Join]: Total Time {0} milli secs.", sw.ElapsedMilliseconds);
                //Console.WriteLine("[Invisble Join]: Total Rows {0}.", joinOutputFinal.Count);
                //Console.WriteLine("[Invisible Join]: Initial Output Time : {0} ms.", initialOutputTime);
                return sw.ElapsedMilliseconds;
            }
            catch (Exception ex)
            {

                throw;
            }
        }

        public void ParallelInvisibleJoin()
        {

            try
            {
                var dateHashTable = new Dictionary<int, string>();
                var customerHashTable = new Dictionary<int, string>();
                var supplierHashTable = new Dictionary<int, string>();
                Stopwatch sw = new Stopwatch();
                #region Key Hashing Phase
                sw.Start();
                Parallel.Invoke(po, () =>
                {
                    List<Date> dateDimension = Utils.ReadFromBinaryFile<List<Date>>(dateFile);
                    foreach (var row in dateDimension)
                    {
                        if (row.dYear.CompareTo("1992") >= 0 && row.dYear.CompareTo("1997") <= 0)
                            dateHashTable.Add(row.dDateKey, row.dYear);
                    }
                    dateDimension.Clear();
                },
               () =>
               {
                   List<Customer> customerDimension = Utils.ReadFromBinaryFile<List<Customer>>(customerFile);
                   foreach (var row in customerDimension)
                   {
                       if (row.cRegion.Equals("ASIA"))
                           customerHashTable.Add(row.cCustKey, row.cNation);
                   }
                   customerDimension.Clear();
               },
               () =>
               {
                   List<Supplier> supplierDimension = Utils.ReadFromBinaryFile<List<Supplier>>(supplierFile);
                   foreach (var row in supplierDimension)
                   {
                       if (row.sRegion.Equals("ASIA"))
                           supplierHashTable.Add(row.sSuppKey, row.sNation);
                   }
                   supplierDimension.Clear();
               });
                sw.Stop();
                #endregion Key Hashing Phase

                Console.WriteLine("[Parallel Invisible Join]: Key Hashing Phase took {0} ms.", sw.ElapsedMilliseconds);
                sw.Reset();

                #region Probing Phase
                sw.Start();
                List<int> loOrderDate = null;
                List<int> loCustomerKey = null;
                List<int> loSupplierKey = null;
                BitArray baCustomerKey = null;
                BitArray baOrderDate = null;
                BitArray baSupplierKey = null;

                Parallel.Invoke(po, () =>
                {
                    loOrderDate = Utils.ReadFromBinaryFile<List<int>>(loOrderDateFile);
                    baOrderDate = new BitArray(loOrderDate.Count);
                    var i = 0;
                    foreach (var orderDate in loOrderDate)
                    {
                        if (dateHashTable.ContainsKey(orderDate))
                            baOrderDate.Set(i, true);
                        i++;
                    }
                    loOrderDate.Clear();
                },
                () =>
                {
                    loCustomerKey = Utils.ReadFromBinaryFile<List<int>>(loCustKeyFile);
                    baCustomerKey = new BitArray(loCustomerKey.Count);
                    var j = 0;
                    foreach (var custKey in loCustomerKey)
                    {
                        if (customerHashTable.ContainsKey(custKey))
                            baCustomerKey.Set(j, true);
                        j++;
                    }
                    loCustomerKey.Clear();
                },
                () =>
                {
                    loSupplierKey = Utils.ReadFromBinaryFile<List<int>>(loSuppKeyFile);
                    baSupplierKey = new BitArray(loSupplierKey.Count);
                    var k = 0;
                    foreach (var suppKey in loSupplierKey)
                    {
                        if (supplierHashTable.ContainsKey(suppKey))
                            baSupplierKey.Set(k, true);
                        k++;
                    }
                    loSupplierKey.Clear();
                });

                baCustomerKey.And(baSupplierKey);
                baCustomerKey.And(baOrderDate);

                sw.Stop();
                #endregion Probing Phase

                Console.WriteLine("[Parallel Invisible Join]: Probing Phase took {0} ms.", sw.ElapsedMilliseconds);
                sw.Reset();

                #region Value Extraction Phase

                sw.Start();
                loOrderDate = Utils.ReadFromBinaryFile<List<int>>(loOrderDateFile);
                loCustomerKey = Utils.ReadFromBinaryFile<List<int>>(loCustKeyFile);
                loSupplierKey = Utils.ReadFromBinaryFile<List<int>>(loSuppKeyFile);

                List<string> cNation = Utils.ReadFromBinaryFile<List<string>>(cNationFile);
                List<string> sNation = Utils.ReadFromBinaryFile<List<string>>(sNationFile);
                // Use Date Hash Table
                List<int> loRevenue = Utils.ReadFromBinaryFile<List<int>>(loRevenueFile);

                var joinOutputIntermediate = new Dictionary<int, string>();
                var l = 0;
                foreach (bool bitValue in baCustomerKey)
                {
                    if (bitValue)
                    {
                        var dateKey = loOrderDate[l];
                        var custKey = loCustomerKey[l];
                        var suppKey = loSupplierKey[l];

                        // Position Look UP
                        string dYear;
                        dateHashTable.TryGetValue(dateKey, out dYear);

                        string cNationOut = cNation[custKey];
                        string sNationOut = sNation[suppKey];
                        joinOutputIntermediate.Add(l, cNationOut + "," + sNationOut + "," + dYear);
                    }
                    l++;
                }

                var joinOutputFinal = new Dictionary<int, string>();
                foreach (var item in joinOutputIntermediate)
                {
                    joinOutputFinal.Add(item.Key, item.Value + "," + loRevenue[item.Key]);
                }

                sw.Stop();
                #endregion Value Extraction Phase

                Console.WriteLine("[Parallel Invisible Join]: Value Extraction Phase took {0} ms.", sw.ElapsedMilliseconds);
                Console.WriteLine("[Parallel Invisible Join]: Total Rows {0}.", joinOutputFinal.Count);
            }
            catch (Exception ex)
            {

                throw;
            }

        }

        public void loadColumns()
        {
            foreach (var file in Directory.EnumerateFiles(folderPath, "*.tbl"))
            {
                var fileName = Path.GetFileNameWithoutExtension(file);
                String[] allLines = File.ReadAllLines(file);

                if (fileName.Equals("customer"))
                {
                    foreach (var line in allLines)
                    {
                        var data = line.Split('|');
                        cCustKey.Add(Convert.ToInt32(data[0]));
                        cName.Add(data[1]);
                        cAddress.Add(data[2]);
                        cCity.Add(data[3]);
                        cNation.Add(data[4]);
                        cRegion.Add(data[5]);
                        cPhone.Add(data[6]);
                        cMktSegment.Add(data[7]);
                    }
                }
                else if (fileName.Equals("supplier"))
                {
                    foreach (var line in allLines)
                    {
                        var data = line.Split('|');
                        sSuppKey.Add(Convert.ToInt32(data[0]));
                        sName.Add(data[1]);
                        sAddress.Add(data[2]);
                        sCity.Add(data[3]);
                        sNation.Add(data[4]);
                        sRegion.Add(data[5]);
                        sPhone.Add(data[6]);
                    }
                }
                else if (fileName.Equals("part"))
                {
                    foreach (var line in allLines)
                    {
                        var data = line.Split('|');
                        pPartKey.Add(Convert.ToInt32(data[0]));
                        pName.Add(data[1]);
                        pMFGR.Add(data[2]);
                        pCategory.Add(data[3]);
                        pBrand.Add(data[4]);
                        pColor.Add(data[5]);
                        pType.Add(data[6]);
                        pSize.Add(Convert.ToInt16(data[7]));
                        pContainer.Add(data[8]);
                    }
                }
                else if (fileName.Equals("date"))
                {
                    foreach (var line in allLines)
                    {
                        var data = line.Split('|');
                        dDateKey.Add(Convert.ToInt32(data[0]));
                        dDate.Add(data[1]);
                        dDayOfWeek.Add(data[2]);
                        dMonth.Add(data[3]);
                        dYear.Add(Convert.ToInt16(data[4]));
                        dYearMonthNum.Add(Convert.ToInt32(data[5]));
                        dYearMonth.Add(data[6]);
                        dDayNumInWeek.Add(Convert.ToInt16(data[8]));
                        dDayNumInMonth.Add(Convert.ToInt16(data[8]));
                        dDayNumInYear.Add(Convert.ToInt16(data[9]));
                        dMonthNumInYear.Add(Convert.ToInt16(data[10]));
                        dWeekNumInYear.Add(Convert.ToInt16(data[11]));
                        dSellingSeason.Add(data[12]);
                        dLastDayInMonthFL.Add(Convert.ToInt16(data[13]));
                        dHolidayFL.Add(Convert.ToInt16(data[14]));
                        dWeekDayFL.Add(Convert.ToInt16(data[15]));
                        dDayNumInYear.Add(Convert.ToInt16(data[16]));
                        dLastDayInWeekFL.Add(Convert.ToInt16(data[13]));
                    }
                }
                else if (fileName.Equals("lineorder"))
                {
                    var i = 0;
                    foreach (var line in allLines)
                    {
                        var data = line.Split('|');
                        loOrderKey.Add(Convert.ToInt32(data[0]));
                        loLineNumber.Add(Convert.ToInt16(data[1]));
                        loCustKey.Add(Convert.ToInt32(data[2]));
                        loPartKey.Add(Convert.ToInt32(data[3]));
                        loSuppKey.Add(Convert.ToInt16(data[4]));
                        loOrderDate.Add(Convert.ToInt32(data[5]));
                        loOrderPriority.Add(data[6]);
                        loShipPriority.Add(Convert.ToChar(data[7]));
                        loQuantity.Add(Convert.ToInt16(data[8]));
                        loExtendedPrice.Add(Convert.ToInt32(data[9]));
                        loOrdTotalPrice.Add(Convert.ToInt32(data[10]));
                        loDiscount.Add(Convert.ToInt16(data[11]));
                        loRevenue.Add(Convert.ToInt32(data[12]));
                        loSupplyCost.Add(Convert.ToInt32(data[13]));
                        loTax.Add(Convert.ToInt16(data[14]));
                        loCommitDate.Add(Convert.ToInt32(data[15]));
                        loShipMode.Add(data[15]);

                        loDiscountWithId.Add(new Tuple<int, int>(i, Convert.ToInt16(data[11])));
                        loQuantityWithId.Add(new Tuple<int, int>(i, Convert.ToInt16(data[8])));
                        i++;
                    }
                }
            }
        }

        public void loadTables()
        {
            foreach (var file in Directory.EnumerateFiles(folderPath, "*.tbl"))
            {
                var fileName = Path.GetFileNameWithoutExtension(file);
                String[] allLines = File.ReadAllLines(file);
                if (fileName.Equals("customer"))
                {
                    foreach (var line in allLines)
                    {
                        var data = line.Split('|');
                        customer.Add(new Customer(Convert.ToInt32(data[0]), data[1], data[2], data[3], data[4], data[5], data[6], data[7]));
                    }
                }
                else if (fileName.Equals("supplier"))
                {
                    foreach (var line in allLines)
                    {
                        var data = line.Split('|');
                        supplier.Add(new Supplier(Convert.ToInt32(data[0]), data[1], data[2], data[3], data[4], data[5], data[6]));
                    }
                }
                else if (fileName.Equals("part"))
                {
                    foreach (var line in allLines)
                    {
                        var data = line.Split('|');
                        part.Add(new Part(Convert.ToInt32(data[0]), data[1], data[2], data[3], data[4], data[5], data[6], Convert.ToInt32(data[7]), data[8]));
                    }
                }
                else if (fileName.Equals("date"))
                {
                    foreach (var line in allLines)
                    {
                        var data = line.Split('|');
                        date.Add(new Date(Convert.ToInt32(data[0]), data[1], data[2], data[3],
                            data[4], Convert.ToInt32(data[5]), data[6], Convert.ToInt32(data[7]),
                            Convert.ToInt32(data[8]), Convert.ToInt32(data[9]), Convert.ToInt32(data[10]), Convert.ToInt32(data[11]),
                            data[12], Convert.ToInt32(data[13]), Convert.ToInt32(data[14]), Convert.ToInt32(data[15]), Convert.ToInt32(data[16])));
                    }
                }
                else if (fileName.Equals("lineorder"))
                {
                    foreach (var line in allLines)
                    {
                        var data = line.Split('|');
                        lineOrder.Add(new LineOrder(Convert.ToInt32(data[0]),
                            Convert.ToInt16(data[1]),
                            Convert.ToInt32(data[2]),
                            Convert.ToInt32(data[3]),
                            Convert.ToInt16(data[4]),
                            Convert.ToInt32(data[5]),
                            data[6],
                            Convert.ToChar(data[7]),
                            Convert.ToInt16(data[8]),
                            Convert.ToInt32(data[9]),
                            Convert.ToInt32(data[10]),
                            Convert.ToInt16(data[11]),
                            Convert.ToInt32(data[12]),
                            Convert.ToInt32(data[13]),
                            Convert.ToInt16(data[14]),
                            Convert.ToInt32(data[15]),
                            data[16]));
                    }
                }
            }
        }

        public void createBinaryFiles()
        {
            Console.WriteLine("Starting to Create Binary Files");
            Utils.WriteToBinaryFile<List<Date>>(dateFile, date);
            Utils.WriteToBinaryFile<List<Customer>>(customerFile, customer);
            Utils.WriteToBinaryFile<List<Supplier>>(supplierFile, supplier);
            Utils.WriteToBinaryFile<List<Part>>(partFile, part);

            Console.WriteLine("Creating Binary Files for tables complete.");
            Utils.WriteToBinaryFile<List<int>>(dDateKeyFile, dDateKey);
            Utils.WriteToBinaryFile<List<int>>(dYearFile, dYear);
            Utils.WriteToBinaryFile<List<int>>(dYearMonthNumFile, dYearMonthNum);
            Utils.WriteToBinaryFile<List<int>>(dDayNumInWeekFile, dDayNumInWeek);
            Utils.WriteToBinaryFile<List<int>>(dDayNumInMonthFile, dDayNumInMonth);
            Utils.WriteToBinaryFile<List<int>>(dDayNumInYearFile, dDayNumInYear);
            Utils.WriteToBinaryFile<List<int>>(dMonthNumInYearFile, dMonthNumInYear);
            Utils.WriteToBinaryFile<List<int>>(dWeekNumInYearFile, dWeekNumInYear);
            Utils.WriteToBinaryFile<List<int>>(dLastDayInWeekFLFile, dLastDayInWeekFL);
            Utils.WriteToBinaryFile<List<int>>(dLastDayInMonthFLFile, dLastDayInMonthFL);
            Utils.WriteToBinaryFile<List<int>>(dHolidayFLFile, dHolidayFL);
            Utils.WriteToBinaryFile<List<int>>(dWeekDayFLFile, dWeekDayFL);
            Utils.WriteToBinaryFile<List<string>>(dDateFile, dDate);
            Utils.WriteToBinaryFile<List<string>>(dDayOfWeekFile, dDayOfWeek);
            Utils.WriteToBinaryFile<List<string>>(dMonthFile, dMonth);
            Utils.WriteToBinaryFile<List<string>>(dYearMonthFile, dYearMonth);
            Utils.WriteToBinaryFile<List<string>>(dSellingSeasonFile, dSellingSeason);
            Console.WriteLine("Creating Binary Files for columns in DATE table complete.");

            Utils.WriteToBinaryFile<List<int>>(loOrderKeyFile, loOrderKey);
            Utils.WriteToBinaryFile<List<int>>(loLineNumberFile, loLineNumber);
            Utils.WriteToBinaryFile<List<int>>(loCustKeyFile, loCustKey);
            Utils.WriteToBinaryFile<List<int>>(loPartKeyFile, loPartKey);
            Utils.WriteToBinaryFile<List<int>>(loSuppKeyFile, loSuppKey);
            Utils.WriteToBinaryFile<List<int>>(loOrderDateFile, loOrderDate);
            Utils.WriteToBinaryFile<List<char>>(loShipPriorityFile, loShipPriority);
            Utils.WriteToBinaryFile<List<int>>(loQuantityFile, loQuantity);
            Utils.WriteToBinaryFile<List<int>>(loExtendedPriceFile, loExtendedPrice);
            Utils.WriteToBinaryFile<List<int>>(loOrdTotalPriceFile, loOrdTotalPrice);
            Utils.WriteToBinaryFile<List<int>>(loDiscountFile, loDiscount);
            Utils.WriteToBinaryFile<List<int>>(loSupplyCostFile, loSupplyCost);
            Utils.WriteToBinaryFile<List<int>>(loTaxFile, loTax);
            Utils.WriteToBinaryFile<List<int>>(loRevenueFile, loRevenue);
            Utils.WriteToBinaryFile<List<int>>(loCommitDateFile, loCommitDate);
            Utils.WriteToBinaryFile<List<string>>(loShipModeFile, loShipMode);
            Utils.WriteToBinaryFile<List<string>>(loOrderPriorityFile, loOrderPriority);
            Console.WriteLine("Creating Binary Files for columns in LINEORDER table complete.");

            Utils.WriteToBinaryFile<List<int>>(cCustKeyFile, cCustKey);
            Utils.WriteToBinaryFile<List<string>>(cNameFile, cName);
            Utils.WriteToBinaryFile<List<string>>(cAddressFile, cAddress);
            Utils.WriteToBinaryFile<List<string>>(cCityFile, cCity);
            Utils.WriteToBinaryFile<List<string>>(cNationFile, cNation);
            Utils.WriteToBinaryFile<List<string>>(cRegionFile, cRegion);
            Utils.WriteToBinaryFile<List<string>>(cPhoneFile, cPhone);
            Utils.WriteToBinaryFile<List<string>>(cMktSegmentFile, cMktSegment);
            Console.WriteLine("Creating Binary Files for columns in CUSTOMER table complete.");

            Utils.WriteToBinaryFile<List<int>>(sSuppKeyFile, sSuppKey);
            Utils.WriteToBinaryFile<List<string>>(sNameFile, sName);
            Utils.WriteToBinaryFile<List<string>>(sAddressFile, sAddress);
            Utils.WriteToBinaryFile<List<string>>(sCityFile, sCity);
            Utils.WriteToBinaryFile<List<string>>(sNationFile, sNation);
            Utils.WriteToBinaryFile<List<string>>(sRegionFile, sRegion);
            Utils.WriteToBinaryFile<List<string>>(sPhoneFile, sPhone);
            Console.WriteLine("Creating Binary Files for columns in SUPPLIER table complete.");

            Utils.WriteToBinaryFile<List<int>>(pSizeFile, pSize);
            Utils.WriteToBinaryFile<List<int>>(pPartKeyFile, pPartKey);
            Utils.WriteToBinaryFile<List<string>>(pNameFile, pName);
            Utils.WriteToBinaryFile<List<string>>(pMFGRFile, pMFGR);
            Utils.WriteToBinaryFile<List<string>>(pCategoryFile, pCategory);
            Utils.WriteToBinaryFile<List<string>>(pBrandFile, pBrand);
            Utils.WriteToBinaryFile<List<string>>(pColorFile, pColor);
            Utils.WriteToBinaryFile<List<string>>(pTypeFile, pType);
            Utils.WriteToBinaryFile<List<string>>(pContainerFile, pContainer);
            Console.WriteLine("Creating Binary Files for columns in PART table complete.");
        }

    }
}
