using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
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
        private static List<char> loShipPriority = new List<char>();
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

        private static string binaryFilesDirectory = @"C:\Raw_Data_Source_For_Test\SSBM - DBGEN\binaryFiles";
        private static string dateFile = Path.Combine(binaryFilesDirectory, "dateFile.bin");
        private static string dDateKeyFile = Path.Combine(binaryFilesDirectory, "dDateKeyFile.bin");
        private static string dYearFile = Path.Combine(binaryFilesDirectory, "dYearFile.bin");
        private static string dYearMonthNumFile = Path.Combine(binaryFilesDirectory, "dYearMonthNumFile.bin");
        private static string dDayNumInWeekFile = Path.Combine(binaryFilesDirectory, "dDayNumInWeekFile.bin");
        private static string dDayNumInMonthFile = Path.Combine(binaryFilesDirectory, "dDayNumInMonthFile.bin");
        private static string dDayNumInYearFile = Path.Combine(binaryFilesDirectory, "dDayNumInYearFile.bin");
        private static string dMonthNumInYearFile = Path.Combine(binaryFilesDirectory, "dMonthNumInYearFile.bin");
        private static string dWeekNumInYearFile = Path.Combine(binaryFilesDirectory, "dWeekNumInYearFile.bin");
        private static string dLastDayInWeekFLFile = Path.Combine(binaryFilesDirectory, "dLastDayInWeekFLFile.bin");
        private static string dLastDayInMonthFLFile = Path.Combine(binaryFilesDirectory, "dLastDayInMonthFLFile.bin");
        private static string dHolidayFLFile = Path.Combine(binaryFilesDirectory, "dHolidayFLFile.bin");
        private static string dWeekDayFLFile = Path.Combine(binaryFilesDirectory, "dWeekDayFLFile.bin");
        private static string dDateFile = Path.Combine(binaryFilesDirectory, "dDateFile.bin");
        private static string dDayOfWeekFile = Path.Combine(binaryFilesDirectory, "dDayOfWeekFile.bin");
        private static string dMonthFile = Path.Combine(binaryFilesDirectory, "dMonthFile.bin");
        private static string dYearMonthFile = Path.Combine(binaryFilesDirectory, "dYearMonthFile.bin");
        private static string dSellingSeasonFile = Path.Combine(binaryFilesDirectory, "dSellingSeasonFile.bin");

        private static string loOrderKeyFile = Path.Combine(binaryFilesDirectory, "loOrderKeyFile.bin");
        private static string loLineNumberFile = Path.Combine(binaryFilesDirectory, "loLineNumberFile.bin");
        private static string loCustKeyFile = Path.Combine(binaryFilesDirectory, "loCustKeyFile.bin");
        private static string loPartKeyFile = Path.Combine(binaryFilesDirectory, "loPartKeyFile.bin");
        private static string loSuppKeyFile = Path.Combine(binaryFilesDirectory, "loSuppKeyFile.bin");
        private static string loOrderDateFile = Path.Combine(binaryFilesDirectory, "loOrderDateFile.bin");
        private static string loShipPriorityFile = Path.Combine(binaryFilesDirectory, "loShipPriorityFile.bin");
        private static string loQuantityFile = Path.Combine(binaryFilesDirectory, "loQuantityFile.bin");
        private static string loExtendedPriceFile = Path.Combine(binaryFilesDirectory, "loExtendedPriceFile.bin");
        private static string loOrdTotalPriceFile = Path.Combine(binaryFilesDirectory, "loOrdTotalPriceFile.bin");
        private static string loDiscountFile = Path.Combine(binaryFilesDirectory, "loDiscountFile.bin");
        private static string loRevenueFile = Path.Combine(binaryFilesDirectory, "loRevenueFile.bin");
        private static string loSupplyCostFile = Path.Combine(binaryFilesDirectory, "loSupplyCostFile.bin");
        private static string loTaxFile = Path.Combine(binaryFilesDirectory, "loTaxFile.bin");
        private static string loCommitDateFile = Path.Combine(binaryFilesDirectory, "loCommitDateFile.bin");
        private static string loShipModeFile = Path.Combine(binaryFilesDirectory, "loShipModeFile.bin");
        private static string loOrderPriorityFile = Path.Combine(binaryFilesDirectory, "loOrderPriorityFile.bin");

        private static void loadColumns()
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

        private static void selectAllHashJoin()
        {
            // SELECT * FROM lineorder, date where lo_orderdate = d_datekey;
            var dateHash = new Dictionary<int, Date>();
            List<Tuple<LineOrder, Date>> joinedTuples = new List<Tuple<LineOrder, Date>>();
            foreach (var d in date)
            {
                dateHash.Add(d.dDateKey, d);
            }
            foreach (var lo in lineOrder)
            {
                if (dateHash.ContainsKey(lo.loOrderDate))
                {
                    Date dateObj;
                    dateHash.TryGetValue(lo.loOrderDate, out dateObj);
                    joinedTuples.Add(new Tuple<LineOrder, Date>(lo, dateObj));
                }
            }
            foreach (var tuple in joinedTuples)
            {
                // Console.WriteLine(String.Format("{0},{1}", Utils.convertObjectToCSVString(tuple.Item1), Utils.convertObjectToCSVString(tuple.Item2)));
            }
        }

        private static void selectAllColumnJoinUsingLateMaterailization()
        {
            var dateHash = new Dictionary<int, int>();
            List<string> joinedTuples = new List<string>();
            var j = 0;
            foreach (var d in date)
            {
                dateHash.Add(d.dDateKey, j);
                j++;
            }

            var i = 0;
            foreach (var orderDate in loOrderDate)
            {
                if (dateHash.ContainsKey(orderDate))
                {
                    int idx;
                    dateHash.TryGetValue(orderDate, out idx);
                    joinedTuples.Add(String.Format(@"{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13},{14},{15},{16},{17},
                        {18},{19},{20},{21},{22},{23},{24},{25},{26},{27},{28},{29},{30},{31},{32}",
                        loOrderKey[i], loLineNumber[i], loCustKey[i], loPartKey[i], loSuppKey[i], loOrderDate[i], loOrderPriority[i],
                        loShipPriority[i], loQuantity[i], loExtendedPrice[i], loOrdTotalPrice[i], loDiscount[i], loRevenue[i], loSupplyCost[i],
                        loTax[i], loCommitDate[i], loShipMode[i], dDateKey[idx], dDate[idx], dDayOfWeek[idx], dMonth[idx], dYear[idx], dYearMonthNum[idx],
                        dYearMonth[idx], dDayNumInWeek[idx], dDayNumInMonth[idx], dMonthNumInYear[idx], dWeekNumInYear[idx], dSellingSeason[idx],
                        dLastDayInMonthFL[idx], dHolidayFL[idx], dWeekDayFL[idx], dDayNumInYear[idx]));
                }
                i++;
            }
            foreach (var tuple in joinedTuples)
            {
                //Console.WriteLine(tuple);
            }
        }

        private static void selectedColumnJoinUsingLateMaterailization()
        {
            var dateHash = new Dictionary<int, int>();
            List<string> joinedTuples = new List<string>();
            var j = 0;
            foreach (var d in date)
            {
                dateHash.Add(d.dDateKey, j);
                j++;
            }

            var i = 0;
            foreach (var orderDate in loOrderDate)
            {
                if (dateHash.ContainsKey(orderDate))
                {
                    int idx;
                    dateHash.TryGetValue(orderDate, out idx);
                    joinedTuples.Add(String.Format(@"{0},{1}", loExtendedPrice[i], loDiscount[i]));
                }
                i++;
            }
            foreach (var tuple in joinedTuples)
            {
                //Console.WriteLine(tuple);
            }
        }

        private static void selectAllColumnJoinUsingEarlyMaterialization()
        {
            List<Tuple<LineOrder, Date>> joinedTuples = new List<Tuple<LineOrder, Date>>();
            List<LineOrder> lineOrder = new List<LineOrder>();
            List<Date> date = new List<Date>();
            int i = 0;
            foreach (var orderDate in loOrderDate)
            {
                lineOrder.Add(new LineOrder(loOrderKey[i], loLineNumber[i], loCustKey[i], loPartKey[i], loSuppKey[i], loOrderDate[i], loOrderPriority[i],
                  Convert.ToChar(loShipPriority[i]), loQuantity[i], loExtendedPrice[i], loOrdTotalPrice[i], loDiscount[i], loRevenue[i], loSupplyCost[i],
                  loTax[i], loCommitDate[i], loShipMode[i]));
                i++;
            }
            int j = 0;
            foreach (var dt in dYear)
            {
                date.Add(new Date(dDateKey[j], dDate[j], dDayOfWeek[j], dMonth[j], dYear[j], dYearMonthNum[j],
                        dYearMonth[j], dDayNumInWeek[j], dDayNumInMonth[j], dDayNumInMonth[j], dMonthNumInYear[j], dWeekNumInYear[j], dSellingSeason[j],
                        dLastDayInMonthFL[j], dLastDayInMonthFL[j], dHolidayFL[j], dWeekDayFL[j]));
                j++;
            }

            var dateHash = new Dictionary<int, Date>();
            foreach (var d in date)
            {
                dateHash.Add(d.dDateKey, d);
            }
            foreach (var lo in lineOrder)
            {
                if (dateHash.ContainsKey(lo.loOrderDate))
                {
                    Date dateObj;
                    dateHash.TryGetValue(lo.loOrderDate, out dateObj);
                    joinedTuples.Add(new Tuple<LineOrder, Date>(lo, dateObj));
                }
            }
            foreach (var tuple in joinedTuples)
            {
                // Console.WriteLine(String.Format("{0},{1}", Utils.convertObjectToCSVString(tuple.Item1), Utils.convertObjectToCSVString(tuple.Item2)));
            }
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

        private static void createBinaryFiles()
        {
            Utils.WriteToBinaryFile<List<Date>>(dateFile, date);

            var chunkedList = lineOrder.ChunkBy<LineOrder>(5000);
            int q = 0;
            foreach (var chunk in chunkedList)
            {
                string lineOrderFile = Path.Combine(@"C:\Raw_Data_Source_For_Test\SSBM - DBGEN\binaryFiles\lineorder", String.Format("lineOrderFile{0}.bin", q));
                Utils.WriteToBinaryFile<List<LineOrder>>(lineOrderFile, chunk);
                q++;
            }

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

        }

        private static void selectAllBlockProcessingHashJoin()
        {
            // SELECT * FROM lineorder, date where lo_orderdate = d_datekey;
            var dateHash = new Dictionary<int, Date>();
            List<Tuple<LineOrder, Date>> joinedTuples = new List<Tuple<LineOrder, Date>>();
            List<Date> date = Utils.ReadFromBinaryFile<List<Date>>(dateFile);

            foreach (var d in date)
            {
                dateHash.Add(d.dDateKey, d);
            }

            var chunkedFiles = Directory.GetFiles(@"C:\Raw_Data_Source_For_Test\SSBM - DBGEN\binaryFiles\lineorder");
            foreach (var chunk in chunkedFiles)
            {
                List<LineOrder> lineOrder = Utils.ReadFromBinaryFile<List<LineOrder>>(chunk);
                foreach (var lo in lineOrder)
                {
                    if (dateHash.ContainsKey(lo.loOrderDate))
                    {
                        Date dateObj;
                        dateHash.TryGetValue(lo.loOrderDate, out dateObj);
                        joinedTuples.Add(new Tuple<LineOrder, Date>(lo, dateObj));
                    }
                }
            }

            foreach (var tuple in joinedTuples)
            {
                // Console.WriteLine(String.Format("{0},{1}", Utils.convertObjectToCSVString(tuple.Item1), Utils.convertObjectToCSVString(tuple.Item2)));
            }

            Console.WriteLine("Select All Row: " + joinedTuples.Count);
        }

        private static void selectAllColumnBlockedJoinUsingEarlyMaterialization()
        {
            List<Tuple<LineOrder, Date>> joinedTuples = new List<Tuple<LineOrder, Date>>();
            List<LineOrder> lineOrder = new List<LineOrder>();
            List<Date> date = new List<Date>();

            List<int> loOrderKey = Utils.ReadFromBinaryFile<List<int>>(loOrderKeyFile);
            List<int> loCustKey = Utils.ReadFromBinaryFile<List<int>>(loCustKeyFile);
            List<int> loLineNumber = Utils.ReadFromBinaryFile<List<int>>(loLineNumberFile);
            List<int> loPartKey = Utils.ReadFromBinaryFile<List<int>>(loPartKeyFile);
            List<int> loSuppKey = Utils.ReadFromBinaryFile<List<int>>(loSuppKeyFile);
            List<int> loOrderDate = Utils.ReadFromBinaryFile<List<int>>(loOrderDateFile);
            List<string> loOrderPriority = Utils.ReadFromBinaryFile<List<string>>(loOrderPriorityFile);
            List<char> loShipPriority = Utils.ReadFromBinaryFile<List<char>>(loShipPriorityFile);
            List<int> loQuantity = Utils.ReadFromBinaryFile<List<int>>(loQuantityFile);
            List<int> loExtendedPrice = Utils.ReadFromBinaryFile<List<int>>(loExtendedPriceFile);
            List<int> loOrdTotalPrice = Utils.ReadFromBinaryFile<List<int>>(loOrdTotalPriceFile);
            List<int> loDiscount = Utils.ReadFromBinaryFile<List<int>>(loDiscountFile);
            List<int> loRevenue = Utils.ReadFromBinaryFile<List<int>>(loRevenueFile);
            List<int> loSupplyCost = Utils.ReadFromBinaryFile<List<int>>(loSupplyCostFile);
            List<int> loTax = Utils.ReadFromBinaryFile<List<int>>(loTaxFile);
            List<int> loCommitDate = Utils.ReadFromBinaryFile<List<int>>(loCommitDateFile);
            List<string> loShipMode = Utils.ReadFromBinaryFile<List<string>>(loShipModeFile);

            List<int> dDateKey = Utils.ReadFromBinaryFile<List<int>>(dDateKeyFile);
            List<string> dDate = Utils.ReadFromBinaryFile<List<string>>(dDateFile);
            List<string> dDayOfWeek = Utils.ReadFromBinaryFile<List<string>>(dDayOfWeekFile);
            List<string> dMonth = Utils.ReadFromBinaryFile<List<string>>(dMonthFile);
            List<int> dYear = Utils.ReadFromBinaryFile<List<int>>(dYearFile);
            List<int> dYearMonthNum = Utils.ReadFromBinaryFile<List<int>>(dYearMonthNumFile);
            List<string> dYearMonth = Utils.ReadFromBinaryFile<List<string>>(dYearMonthFile);
            List<int> dDayNumInMonth = Utils.ReadFromBinaryFile<List<int>>(dDayNumInMonthFile);
            List<int> dDayNumInWeek = Utils.ReadFromBinaryFile<List<int>>(dDayNumInWeekFile); 
            List<int> dMonthNumInYear = Utils.ReadFromBinaryFile<List<int>>(dMonthNumInYearFile);
            List<int> dWeekNumInYear = Utils.ReadFromBinaryFile<List<int>>(dWeekNumInYearFile);
            List<string> dSellingSeason = Utils.ReadFromBinaryFile<List<string>>(dSellingSeasonFile);
            List<int> dLastDayInWeekFL = Utils.ReadFromBinaryFile<List<int>>(dLastDayInWeekFLFile);
            List<int> dLastDayInMonthFL = Utils.ReadFromBinaryFile<List<int>>(dLastDayInMonthFLFile);
            List<int> dHolidayFL = Utils.ReadFromBinaryFile<List<int>>(dHolidayFLFile);
            List<int> dWeekDayFL = Utils.ReadFromBinaryFile<List<int>>(dWeekDayFLFile);

            int i = 0;
            foreach (var orderDate in loOrderDate)
            {
                lineOrder.Add(new LineOrder(loOrderKey[i], loLineNumber[i], loCustKey[i], loPartKey[i], loSuppKey[i], loOrderDate[i], loOrderPriority[i],
                  Convert.ToChar(loShipPriority[i]), loQuantity[i], loExtendedPrice[i], loOrdTotalPrice[i], loDiscount[i], loRevenue[i], loSupplyCost[i],
                  loTax[i], loCommitDate[i], loShipMode[i]));
                i++;
            }
            int j = 0;
            foreach (var dt in dYear)
            {
                date.Add(new Date(dDateKey[j], dDate[j], dDayOfWeek[j], dMonth[j],
                    dYear[j], dYearMonthNum[j],
                        dYearMonth[j], dDayNumInWeek[j], dDayNumInMonth[j],
                        dDayNumInMonth[j], dMonthNumInYear[j], dWeekNumInYear[j],
                        dSellingSeason[j],
                        dLastDayInWeekFL[j], dLastDayInMonthFL[j], dHolidayFL[j],
                        dWeekDayFL[j]));
                j++;
            }

            var dateHash = new Dictionary<int, Date>();
            foreach (var d in date)
            {
                dateHash.Add(d.dDateKey, d);
            }
            foreach (var lo in lineOrder)
            {
                if (dateHash.ContainsKey(lo.loOrderDate))
                {
                    Date dateObj;
                    dateHash.TryGetValue(lo.loOrderDate, out dateObj);
                    joinedTuples.Add(new Tuple<LineOrder, Date>(lo, dateObj));
                }
            }
            foreach (var tuple in joinedTuples)
            {
                // Console.WriteLine(String.Format("{0},{1}", Utils.convertObjectToCSVString(tuple.Item1), Utils.convertObjectToCSVString(tuple.Item2)));
            }
            Console.WriteLine("Select All Column: " + joinedTuples.Count);
        }

        private static void selectedColumnBlockedJoinUsingLateMaterailization()
        {
            var dateHash = new Dictionary<int, int>();
            List<string> joinedTuples = new List<string>();
            List<Date> date = Utils.ReadFromBinaryFile<List<Date>>(dateFile);

            var j = 0;
            foreach (var d in date)
            {
                dateHash.Add(d.dDateKey, j);
                j++;
            }

            List<int> loOrderDate = Utils.ReadFromBinaryFile<List<int>>(loOrderDateFile);
            List<int> loDiscount = Utils.ReadFromBinaryFile<List<int>>(loDiscountFile);
            List<int> loExtendedPrice = Utils.ReadFromBinaryFile<List<int>>(loExtendedPriceFile);
            var i = 0;
            foreach (var orderDate in loOrderDate)
            {
                if (dateHash.ContainsKey(orderDate))
                {
                    int idx;
                    dateHash.TryGetValue(orderDate, out idx);
                    joinedTuples.Add(String.Format(@"{0},{1}", loExtendedPrice[i], loDiscount[i]));
                }
                i++;
            }
            foreach (var tuple in joinedTuples)
            {
                //Console.WriteLine(tuple);
            }
            Console.WriteLine("Selected: " + joinedTuples.Count);
        }

        static void Main(string[] args)
        {
            var sDate = DateTime.Now;
            //loadColumns();
            //loadTables();
            //createBinaryFiles();
            var eDate = DateTime.Now;
            Console.WriteLine(String.Format("Load Time: {0} secs", ((eDate - sDate).Seconds)));

            //Console.WriteLine(String.Format("Date: {0} ms , LO: {1} ms, LOD: {2} ms", dateSerializationTime, lineOrderSerializationTime, lineOrderDeSerializationTime));

            #region joins
            string[] results = new string[50];
            for (int i = 0; i < 10; i++)
            {
                var sRJoinTime = DateTime.Now;
                //HashJoinQ11();
                //selectAllHashJoin();
                var eRJoinTime = DateTime.Now;

                var sCJoinTime = DateTime.Now;
                // selectAllColumnJoinUsingEarlyMaterialization();
                // columnHashJoinQ11();
                var eCJoinTime = DateTime.Now;

                var sICJoinTime = DateTime.Now;
                //improvedColumnHashJoinQ11();
                //selectAllColumnJoinUsingLateMaterailization();
                var eICJoinTime = DateTime.Now;

                Stopwatch sw = Stopwatch.StartNew();
                selectAllColumnBlockedJoinUsingEarlyMaterialization();
                sw.Stop();
                Console.Write(String.Format("EM Column Hash: {0} ", sw.ElapsedMilliseconds));
                long a = sw.ElapsedMilliseconds;

                sw.Reset();
                sw.Start();
                //selectAllColumnJoinUsingLateMaterailization();
                sw.Stop();
                Console.Write(String.Format("LM Column Hash: {0} ", sw.ElapsedMilliseconds));
                long b = sw.ElapsedMilliseconds;

                sw.Reset();
                sw.Start();
                selectAllBlockProcessingHashJoin();
                sw.Stop();
                Console.Write(String.Format("Row Hash: {0} ", sw.ElapsedMilliseconds));
                long c = sw.ElapsedMilliseconds;

                sw.Reset();
                sw.Start();
                selectedColumnBlockedJoinUsingLateMaterailization();
                sw.Stop();
                Console.Write(String.Format("LM Selected Hash: {0} ", sw.ElapsedMilliseconds));
                long d = sw.ElapsedMilliseconds;

                Console.WriteLine();
                //Console.WriteLine(String.Format("Iteration: {0}, RJT: {1}, CJT: {2}, ICJT: {3} ms", i, ((eRJoinTime - sRJoinTime).Milliseconds), ((eCJoinTime - sCJoinTime).Milliseconds), ((eICJoinTime - sICJoinTime).Milliseconds)));
                // Console.WriteLine(String.Format("Iteration: {0}, RowHash: {1}, EarlyM: {2}, LateM: {3} ms", i, ((eRJoinTime - sRJoinTime).Milliseconds), ((eCJoinTime - sCJoinTime).Milliseconds), ((eICJoinTime - sICJoinTime).Milliseconds)));
                results[i] = String.Format("{0}, {1}, {2}, {3}", i + 1, a, c, d);
            }
            File.WriteAllLines(@"C:\Users\psangats\Google Drive\Study\0190 Doctor of Philosophy\My Research - Publication Works\test2-Binaryfilesreading2.txt", results);
            #endregion
            Console.ReadKey();

        }
    }
}
