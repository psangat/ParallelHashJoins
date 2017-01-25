using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParallelHashJoins
{
    class Program
    {
        private static string folderPath = @"C:\Raw_Data_Source_For_Test\SSBM - DBGEN\SF 1";
        private static List<int> cCustKey = new List<int>();
        private static List<string> cName = new List<string>();
        private static List<string> cAddress = new List<string>();
        private static List<string> cCity = new List<string>();
        private static List<string> cNation = new List<string>();
        private static List<string> cRegion = new List<string>();
        private static List<string> cPhone = new List<string>();
        private static List<string> cMktSegment = new List<string>();

        private static List<int> sSuppKey = new List<int>();
        private static List<string> sName = new List<string>();
        private static List<string> sAddress = new List<string>();
        private static List<string> sCity = new List<string>();
        private static List<string> sNation = new List<string>();
        private static List<string> sRegion = new List<string>();
        private static List<string> sPhone = new List<string>();

        private static List<int> pSize = new List<int>();
        private static List<int> pPartKey = new List<int>();
        private static List<string> pName = new List<string>();
        private static List<string> pMFGR = new List<string>();
        private static List<string> pCategory = new List<string>();
        private static List<string> pBrand = new List<string>();
        private static List<string> pColor = new List<string>();
        private static List<string> pType = new List<string>();
        private static List<string> pContainer = new List<string>();

        private static List<int> loOrderKey = new List<int>();
        private static List<int> loLineNumber = new List<int>();
        private static List<int> loCustKey = new List<int>();
        private static List<int> loPartKey = new List<int>();
        private static List<int> loSuppKey = new List<int>();
        private static List<int> loOrderDate = new List<int>();
        private static List<int> loShipPriority = new List<int>();
        private static List<int> loQuantity = new List<int>();
        private static List<int> loExtendedPrice = new List<int>();
        private static List<int> loOrdTotalPrice = new List<int>();
        private static List<int> loDiscount = new List<int>();
        private static List<int> loRevenue = new List<int>();
        private static List<int> loSupplyCost = new List<int>();
        private static List<int> loTax = new List<int>();
        private static List<int> loCommitDate = new List<int>();


        private static List<string> loShipMode = new List<string>();
        private static List<string> loOrderPriority = new List<string>();

        private static List<int> dDateKey = new List<int>();
        private static List<int> dYear = new List<int>();

        private static List<int> dYearMonthNum = new List<int>();
        private static List<int> dDayNumInWeek = new List<int>();
        private static List<int> dDayNumInMonth = new List<int>();
        private static List<int> dDayNumInYear = new List<int>();
        private static List<int> dMonthNumInYear = new List<int>();
        private static List<int> dWeekNumInYear = new List<int>();
        private static List<int> dLastDayInWeekFL = new List<int>();
        private static List<int> dLastDayInMonthFL = new List<int>();
        private static List<int> dHolidayFL = new List<int>();
        private static List<int> dWeekDayFL = new List<int>();
        private static List<string> dDate = new List<string>();
        private static List<string> dDayOfWeek = new List<string>();
        private static List<string> dMonth = new List<string>();
        private static List<string> dYearMonth = new List<string>();
        private static List<string> dSellingSeason = new List<string>();

        private static List<Customer> customer = new List<Customer>();
        private static List<Supplier> supplier = new List<Supplier>();
        private static List<Part> part = new List<Part>();
        private static List<LineOrder> lineOrder = new List<LineOrder>();
        private static List<Date> date = new List<Date>();


        private static void loadColumns()
        {
            foreach (var file in Directory.EnumerateFiles(folderPath, "*.tbl"))
            {
                var fileName = Path.GetFileNameWithoutExtension(file);
                String[] allLines = File.ReadAllLines(file);

                if (fileName.Equals("customer"))
                {
                    //foreach (var line in allLines)
                    //{
                    //    var data = line.Split('|');
                    //    cCustKey.Add(Convert.ToInt32(data[0]));
                    //    cName.Add(data[1]);
                    //    cAddress.Add(data[2]);
                    //    cCity.Add(data[3]);
                    //    cNation.Add(data[4]);
                    //    cRegion.Add(data[5]);
                    //    cPhone.Add(data[6]);
                    //    cMktSegment.Add(data[7]);
                    //}
                }
                else if (fileName.Equals("supplier"))
                {
                    //foreach (var line in allLines)
                    //{
                    //    var data = line.Split('|');
                    //    sSuppKey.Add(Convert.ToInt32(data[0]));
                    //    sName.Add(data[1]);
                    //    sAddress.Add(data[2]);
                    //    sCity.Add(data[3]);
                    //    sNation.Add(data[4]);
                    //    sRegion.Add(data[5]);
                    //    sPhone.Add(data[6]);
                    //}
                }
                else if (fileName.Equals("part"))
                {
                    //foreach (var line in allLines)
                    //{
                    //    var data = line.Split('|');
                    //    pPartKey.Add(Convert.ToInt32(data[0]));
                    //    pName.Add(data[1]);
                    //    pMFGR.Add(data[2]);
                    //    pCategory.Add(data[3]);
                    //    pBrand.Add(data[4]);
                    //    pColor.Add(data[5]);
                    //    pType.Add(data[6]);
                    //    pSize.Add(Convert.ToInt16(data[7]));
                    //    pContainer.Add(data[8]);
                    //}
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
                        dDayNumInWeek.Add(Convert.ToInt16(data[7]));
                        dDayNumInMonth.Add(Convert.ToInt16(data[8]));
                        dDayNumInYear.Add(Convert.ToInt16(data[9]));
                        dMonthNumInYear.Add(Convert.ToInt16(data[10]));
                        dWeekNumInYear.Add(Convert.ToInt16(data[11]));
                        dSellingSeason.Add(data[12]);
                        dLastDayInMonthFL.Add(Convert.ToInt16(data[13]));
                        dHolidayFL.Add(Convert.ToInt16(data[14]));
                        dWeekDayFL.Add(Convert.ToInt16(data[15]));
                        dDayNumInYear.Add(Convert.ToInt16(data[16]));
                    }
                }
                else if (fileName.Equals("lineorder"))
                {
                    foreach (var line in allLines)
                    {
                        var data = line.Split('|');
                        loOrderKey.Add(Convert.ToInt32(data[0]));
                        loLineNumber.Add(Convert.ToInt16(data[1]));
                        loCustKey.Add(Convert.ToInt16(data[2]));
                        loPartKey.Add(Convert.ToInt32(data[3]));
                        loSuppKey.Add(Convert.ToInt16(data[4]));
                        loOrderDate.Add(Convert.ToInt32(data[5]));
                        loOrderPriority.Add(data[6]);
                        loShipPriority.Add(Convert.ToInt16(data[7]));
                        loQuantity.Add(Convert.ToInt16(data[8]));
                        loExtendedPrice.Add(Convert.ToInt32(data[9]));
                        loOrdTotalPrice.Add(Convert.ToInt32(data[10]));
                        loDiscount.Add(Convert.ToInt16(data[11]));
                        loRevenue.Add(Convert.ToInt32(data[12]));
                        loSupplyCost.Add(Convert.ToInt32(data[13]));
                        loTax.Add(Convert.ToInt16(data[14]));
                        loCommitDate.Add(Convert.ToInt32(data[15]));
                        loShipMode.Add(data[15]);
                    }
                }

            }
        }

        private static void loadTables()
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
                            Convert.ToInt32(data[4]), Convert.ToInt32(data[5]), data[6], Convert.ToInt32(data[7]),
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

        private static void HashJoinQ11()
        {
            var dateHash = new Dictionary<int, int>();
            Int64 totalRevenue = 0;
            foreach (var d in date)
            {
                if (d.dYear == 1993)
                    dateHash.Add(d.dDateKey, d.dYear);
            }
            foreach (var lo in lineOrder)
            {
                if (dateHash.ContainsKey(lo.loOrderDate) && lo.loDiscount >= 1 && lo.loDiscount <= 3 && lo.loQuantity < 25)
                {
                    var revenue = (lo.loExtendedPrice * lo.loDiscount);
                    totalRevenue += revenue;
                }
            }
            //Console.WriteLine(String.Format("[RJ] Revenue is : {0}", totalRevenue));
        }

        private static void columnHashJoinQ11()
        {
            var dateHash = new Dictionary<int, int>();
            Int64 totalRevenue = 0;
            foreach (var d in date)
            {
                if (d.dYear == 1993)
                    dateHash.Add(d.dDateKey, d.dYear);
            }

            var arraySize = loDiscount.Count;
            BitArray baDis = new BitArray(arraySize);
            var dis = 0;
            foreach (var d in loDiscount)
            {
                if (d >= 1 && d <= 3)
                    baDis.Set(dis, true);
                dis++;
            }

            BitArray baQty = new BitArray(arraySize);
            var qty = 0;
            foreach (var d in loQuantity)
            {
                if (d < 25)
                    baQty.Set(qty, true);
                qty++;
            }

            BitArray baOD = new BitArray(arraySize);
            var i = 0;
            foreach (var lo in loOrderDate)
            {
                if (dateHash.ContainsKey(lo))
                {
                    baOD.Set(i, true);
                }
                i++;
            }

            baOD.And(baDis);
            baOD.And(baQty);

            for (int k = 0; k < baOD.Count; k++)
            {
                if (baOD.Get(k)) // bit set to true
                {
                    var revenue = (loExtendedPrice[k] * loDiscount[k]);
                    totalRevenue += revenue;
                }
            }
            //Console.WriteLine(String.Format("[CJ] Revenue is : {0}", totalRevenue));
        }

        private static void improvedColumnHashJoinQ11()
        {
            var dateHash = new Dictionary<int, int>();
            Int64 totalRevenue = 0;
            foreach (var d in date)
            {
                if (d.dYear == 1993)
                    dateHash.Add(d.dDateKey, d.dYear);
            }

            bool[] bitMap = new bool[loOrderDate.Count];
            var i = 0;
            foreach (var lo in loOrderDate)
            {
                if (dateHash.ContainsKey(lo))
                {
                    bitMap[i] = true;
                }
                i++;
            }

            var dis = 0;
            foreach (var d in loDiscount)
            {
                if (d >= 1 && d <= 3 && bitMap[dis])
                {
                    // do nothing
                    // check to make sure the set
                }
                else
                    bitMap[dis] = false;
                dis++;
            }

            var qty = 0;
            foreach (var d in loQuantity)
            {
                if (d < 25 && bitMap[qty])
                {
                    // do nothing
                }
                else
                    bitMap[qty] = false;
                qty++;
            }

            for (int k = 0; k < bitMap.LongLength; k++)
            {
                if (bitMap[k]) // bit set to true
                {
                    var revenue = (loExtendedPrice[k] * loDiscount[k]);
                    totalRevenue += revenue;
                }
            }
            //Console.WriteLine(String.Format("[ICJ] Revenue is : {0}", totalRevenue));

        }

        private static void parallelColumnHashJoinQ11()
        {


        }

        static void Main(string[] args)
        {
            var sDate = DateTime.Now;
            loadColumns();
            loadTables();
            var eDate = DateTime.Now;
            Console.WriteLine(String.Format("Load Time: {0} secs", ((eDate - sDate).Seconds)));

            string[] results = new string[50];
            for (int i = 0; i < 50; i++)
            {
                var sRJoinTime = DateTime.Now;
                HashJoinQ11();
                var eRJoinTime = DateTime.Now;

                var sCJoinTime = DateTime.Now;
                columnHashJoinQ11();
                var eCJoinTime = DateTime.Now;

                var sICJoinTime = DateTime.Now;
                improvedColumnHashJoinQ11();
                var eICJoinTime = DateTime.Now;

                Console.WriteLine(String.Format("Iteration: {0}, RJT: {1}, CJT: {2}, ICJT: {3} ms", i, ((eRJoinTime - sRJoinTime).Milliseconds), ((eCJoinTime - sCJoinTime).Milliseconds), ((eICJoinTime - sICJoinTime).Milliseconds)));
                results[i] = String.Format("{0}, {1}, {2}, {3}", i + 1, ((eRJoinTime - sRJoinTime).Milliseconds), ((eCJoinTime - sCJoinTime).Milliseconds), ((eICJoinTime - sICJoinTime).Milliseconds));
            }
           // File.WriteAllLines(@"C:\Users\psangats\Google Drive\Study\0190 Doctor of Philosophy\My Research - Publication Works\test.txt", results);

            Console.ReadKey();

        }
    }
}
