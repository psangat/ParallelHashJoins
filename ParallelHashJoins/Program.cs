using System;
using System.Collections;
using System.Collections.Concurrent;
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
        private static List<Tuple<int, int>> loQuantityWithId = new List<Tuple<int, int>>();

        private static List<int> loExtendedPrice = new List<int>();
        private static List<int> loOrdTotalPrice = new List<int>();
        private static List<int> loDiscount = new List<int>();
        private static List<Tuple<int, int>> loDiscountWithId = new List<Tuple<int, int>>();
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

        private static string binaryFilesDirectory = @"C:\Raw_Data_Source_For_Test\SSBM - DBGEN\BFSF1";
        private static string dateFile = Path.Combine(binaryFilesDirectory, "dateFile.bin");
        private static string customerFile = Path.Combine(binaryFilesDirectory, "customerFile.bin");
        private static string supplierFile = Path.Combine(binaryFilesDirectory, "supplierFile.bin");
        private static string partFile = Path.Combine(binaryFilesDirectory, "partFile.bin");


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

        private static string cCustKeyFile = Path.Combine(binaryFilesDirectory, "cCustKeyFile.bin");
        private static string cNameFile = Path.Combine(binaryFilesDirectory, "cNameFile.bin");
        private static string cAddressFile = Path.Combine(binaryFilesDirectory, "cAddressFile.bom");
        private static string cCityFile = Path.Combine(binaryFilesDirectory, "cCityFile.bin");
        private static string cNationFile = Path.Combine(binaryFilesDirectory, "cNationFile.bin");
        private static string cRegionFile = Path.Combine(binaryFilesDirectory, "cRegionFile.bin");
        private static string cPhoneFile = Path.Combine(binaryFilesDirectory, "cPhoneFile.bin");
        private static string cMktSegmentFile = Path.Combine(binaryFilesDirectory, "cMktSegmentFile.bin");

        private static string sSuppKeyFile = Path.Combine(binaryFilesDirectory, "sSuppKeyFile.bin");
        private static string sNameFile = Path.Combine(binaryFilesDirectory, "sNameFile.bin");
        private static string sAddressFile = Path.Combine(binaryFilesDirectory, "sAddressFile.bin");
        private static string sCityFile = Path.Combine(binaryFilesDirectory, "sCityFile.bin");
        private static string sNationFile = Path.Combine(binaryFilesDirectory, "sNationFile.bin");
        private static string sRegionFile = Path.Combine(binaryFilesDirectory, "sRegionFile.bin");
        private static string sPhoneFile = Path.Combine(binaryFilesDirectory, "sPhoneFile.bin");

        private static string pSizeFile = Path.Combine(binaryFilesDirectory, "pSizeFile.bin");
        private static string pPartKeyFile = Path.Combine(binaryFilesDirectory, "pPartKeyFile.bin");
        private static string pNameFile = Path.Combine(binaryFilesDirectory, "pNameFile.bin");
        private static string pMFGRFile = Path.Combine(binaryFilesDirectory, "pMFGRFile.bin");
        private static string pCategoryFile = Path.Combine(binaryFilesDirectory, "pCategoryFile.bin");
        private static string pBrandFile = Path.Combine(binaryFilesDirectory, "pBrandFile.bin");
        private static string pColorFile = Path.Combine(binaryFilesDirectory, "pColorFile.bin");
        private static string pTypeFile = Path.Combine(binaryFilesDirectory, "pTypeFile.bin");
        private static string pContainerFile = Path.Combine(binaryFilesDirectory, "pContainerFile.bin");



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

        //private static void HashJoinQ11()
        //{
        //    var dateHash = new Dictionary<int, int>();
        //    Int64 totalRevenue = 0;
        //    foreach (var d in date)
        //    {
        //        if (d.dYear == 1993)
        //            dateHash.Add(d.dDateKey, d.dYear);
        //    }
        //    foreach (var lo in lineOrder)
        //    {
        //        if (dateHash.ContainsKey(lo.loOrderDate) && lo.loDiscount >= 1 && lo.loDiscount <= 3 && lo.loQuantity < 25)
        //        {
        //            var revenue = (lo.loExtendedPrice * lo.loDiscount);
        //            totalRevenue += revenue;
        //        }
        //    }
        //    //Console.WriteLine(String.Format("[RJ] Revenue is : {0}", totalRevenue));
        //}

        //private static void selectAllHashJoin()
        //{
        //    // SELECT * FROM lineorder, date where lo_orderdate = d_datekey;
        //    var dateHash = new Dictionary<int, Date>();
        //    List<Tuple<LineOrder, Date>> joinedTuples = new List<Tuple<LineOrder, Date>>();
        //    foreach (var d in date)
        //    {
        //        dateHash.Add(d.dDateKey, d);
        //    }
        //    foreach (var lo in lineOrder)
        //    {
        //        if (dateHash.ContainsKey(lo.loOrderDate))
        //        {
        //            Date dateObj;
        //            dateHash.TryGetValue(lo.loOrderDate, out dateObj);
        //            joinedTuples.Add(new Tuple<LineOrder, Date>(lo, dateObj));
        //        }
        //    }
        //    foreach (var tuple in joinedTuples)
        //    {
        //        // Console.WriteLine(String.Format("{0},{1}", Utils.convertObjectToCSVString(tuple.Item1), Utils.convertObjectToCSVString(tuple.Item2)));
        //    }
        //}

        //private static void selectAllColumnJoinUsingLateMaterailization()
        //{
        //    var dateHash = new Dictionary<int, int>();
        //    List<string> joinedTuples = new List<string>();
        //    var j = 0;
        //    foreach (var d in date)
        //    {
        //        dateHash.Add(d.dDateKey, j);
        //        j++;
        //    }

        //    var i = 0;
        //    foreach (var orderDate in loOrderDate)
        //    {
        //        if (dateHash.ContainsKey(orderDate))
        //        {
        //            int idx;
        //            dateHash.TryGetValue(orderDate, out idx);
        //            joinedTuples.Add(String.Format(@"{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13},{14},{15},{16},{17},
        //                {18},{19},{20},{21},{22},{23},{24},{25},{26},{27},{28},{29},{30},{31},{32}",
        //                loOrderKey[i], loLineNumber[i], loCustKey[i], loPartKey[i], loSuppKey[i], loOrderDate[i], loOrderPriority[i],
        //                loShipPriority[i], loQuantity[i], loExtendedPrice[i], loOrdTotalPrice[i], loDiscount[i], loRevenue[i], loSupplyCost[i],
        //                loTax[i], loCommitDate[i], loShipMode[i], dDateKey[idx], dDate[idx], dDayOfWeek[idx], dMonth[idx], dYear[idx], dYearMonthNum[idx],
        //                dYearMonth[idx], dDayNumInWeek[idx], dDayNumInMonth[idx], dMonthNumInYear[idx], dWeekNumInYear[idx], dSellingSeason[idx],
        //                dLastDayInMonthFL[idx], dHolidayFL[idx], dWeekDayFL[idx], dDayNumInYear[idx]));
        //        }
        //        i++;
        //    }
        //    foreach (var tuple in joinedTuples)
        //    {
        //        //Console.WriteLine(tuple);
        //    }
        //}

        //private static void selectedColumnJoinUsingLateMaterailization()
        //{
        //    var dateHash = new Dictionary<int, int>();
        //    List<string> joinedTuples = new List<string>();
        //    var j = 0;
        //    foreach (var d in date)
        //    {
        //        dateHash.Add(d.dDateKey, j);
        //        j++;
        //    }

        //    var i = 0;
        //    foreach (var orderDate in loOrderDate)
        //    {
        //        if (dateHash.ContainsKey(orderDate))
        //        {
        //            int idx;
        //            dateHash.TryGetValue(orderDate, out idx);
        //            joinedTuples.Add(String.Format(@"{0},{1}", loExtendedPrice[i], loDiscount[i]));
        //        }
        //        i++;
        //    }
        //    foreach (var tuple in joinedTuples)
        //    {
        //        //Console.WriteLine(tuple);
        //    }
        //}

        //private static void selectAllColumnJoinUsingEarlyMaterialization()
        //{
        //    List<Tuple<LineOrder, Date>> joinedTuples = new List<Tuple<LineOrder, Date>>();
        //    List<LineOrder> lineOrder = new List<LineOrder>();
        //    List<Date> date = new List<Date>();
        //    int i = 0;
        //    foreach (var orderDate in loOrderDate)
        //    {
        //        lineOrder.Add(new LineOrder(loOrderKey[i], loLineNumber[i], loCustKey[i], loPartKey[i], loSuppKey[i], loOrderDate[i], loOrderPriority[i],
        //          Convert.ToChar(loShipPriority[i]), loQuantity[i], loExtendedPrice[i], loOrdTotalPrice[i], loDiscount[i], loRevenue[i], loSupplyCost[i],
        //          loTax[i], loCommitDate[i], loShipMode[i]));
        //        i++;
        //    }
        //    int j = 0;
        //    foreach (var dt in dYear)
        //    {
        //        date.Add(new Date(dDateKey[j], dDate[j], dDayOfWeek[j], dMonth[j], dYear[j], dYearMonthNum[j],
        //                dYearMonth[j], dDayNumInWeek[j], dDayNumInMonth[j], dDayNumInMonth[j], dMonthNumInYear[j], dWeekNumInYear[j], dSellingSeason[j],
        //                dLastDayInMonthFL[j], dLastDayInMonthFL[j], dHolidayFL[j], dWeekDayFL[j]));
        //        j++;
        //    }

        //    var dateHash = new Dictionary<int, Date>();
        //    foreach (var d in date)
        //    {
        //        dateHash.Add(d.dDateKey, d);
        //    }
        //    foreach (var lo in lineOrder)
        //    {
        //        if (dateHash.ContainsKey(lo.loOrderDate))
        //        {
        //            Date dateObj;
        //            dateHash.TryGetValue(lo.loOrderDate, out dateObj);
        //            joinedTuples.Add(new Tuple<LineOrder, Date>(lo, dateObj));
        //        }
        //    }
        //    foreach (var tuple in joinedTuples)
        //    {
        //        // Console.WriteLine(String.Format("{0},{1}", Utils.convertObjectToCSVString(tuple.Item1), Utils.convertObjectToCSVString(tuple.Item2)));
        //    }
        //}

        ////private static void columnHashJoinQ11()
        ////{
        ////    var dateHash = new Dictionary<int, int>();
        ////    Int64 totalRevenue = 0;
        ////    foreach (var d in date)
        ////    {
        ////        if (d.dYear == 1993)
        ////            dateHash.Add(d.dDateKey, d.dYear);
        ////    }

        ////    var arraySize = loDiscount.Count;
        ////    BitArray baDis = new BitArray(arraySize);
        ////    var dis = 0;
        ////    foreach (var d in loDiscount)
        ////    {
        ////        if (d >= 1 && d <= 3)
        ////            baDis.Set(dis, true);
        ////        dis++;
        ////    }

        ////    BitArray baQty = new BitArray(arraySize);
        ////    var qty = 0;
        ////    foreach (var d in loQuantity)
        ////    {
        ////        if (d < 25)
        ////            baQty.Set(qty, true);
        ////        qty++;
        ////    }

        ////    BitArray baOD = new BitArray(arraySize);
        ////    var i = 0;
        ////    foreach (var lo in loOrderDate)
        ////    {
        ////        if (dateHash.ContainsKey(lo))
        ////        {
        ////            baOD.Set(i, true);
        ////        }
        ////        i++;
        ////    }

        ////    baOD.And(baDis);
        ////    baOD.And(baQty);

        ////    for (int k = 0; k < baOD.Count; k++)
        ////    {
        ////        if (baOD.Get(k)) // bit set to true
        ////        {
        ////            var revenue = (loExtendedPrice[k] * loDiscount[k]);
        ////            totalRevenue += revenue;
        ////        }
        ////    }
        ////    //Console.WriteLine(String.Format("[CJ] Revenue is : {0}, bitsetcount: {1}", totalRevenue, bitsetcount));
        ////}

        //private static void improvedColumnHashJoinQ11()
        //{
        //    var dateHash = new Dictionary<int, int>();
        //    Int64 totalRevenue = 0;
        //    foreach (var d in date)
        //    {
        //        if (d.dYear == 1993)
        //            dateHash.Add(d.dDateKey, d.dYear);
        //    }

        //    bool[] bitMap = new bool[loOrderDate.Count];
        //    var i = 0;
        //    foreach (var lo in loOrderDate)
        //    {
        //        if (dateHash.ContainsKey(lo))
        //        {
        //            bitMap[i] = true;
        //        }
        //        i++;
        //    }

        //    var dis = 0;
        //    foreach (var d in loDiscount)
        //    {
        //        if (d >= 1 && d <= 3 && bitMap[dis])
        //        {
        //            // do nothing
        //            // check to make sure the set
        //        }
        //        else
        //            bitMap[dis] = false;
        //        dis++;
        //    }

        //    var qty = 0;
        //    foreach (var d in loQuantity)
        //    {
        //        if (d < 25 && bitMap[qty])
        //        {
        //            // do nothing
        //        }
        //        else
        //            bitMap[qty] = false;
        //        qty++;
        //    }

        //    for (int k = 0; k < bitMap.LongLength; k++)
        //    {
        //        if (bitMap[k]) // bit set to true
        //        {
        //            var revenue = (loExtendedPrice[k] * loDiscount[k]);
        //            totalRevenue += revenue;
        //        }
        //    }
        //    // Console.WriteLine(String.Format("[RIJoin] Revenue is : {0}, Time to loop: {1}", totalRevenue, sw.ElapsedMilliseconds));
        //}

        ////private static void improvedColumnHashJoinQ11WithSmartPredicateFiltering()
        ////{
        ////    var dateHash = new Dictionary<int, int>();
        ////    Int64 totalRevenue = 0;
        ////    foreach (var d in date)
        ////    {
        ////        if (d.dYear == 1993)
        ////            dateHash.Add(d.dDateKey, d.dYear);
        ////    }

        ////    var i = 0;
        ////    foreach (var lo in loOrderDate)
        ////    {
        ////        if (dateHash.ContainsKey(lo))
        ////        {
        ////            if (loQuantity[i] < 25)
        ////            {
        ////                var discount = loDiscount[i];
        ////                if (discount >= 1 && discount <= 3)
        ////                {
        ////                    // do nothing Flag was already set
        ////                    var revenue = (loExtendedPrice[i] * loDiscount[i]);
        ////                    totalRevenue += revenue;
        ////                }
        ////            }
        ////        }
        ////        i++;
        ////    }

        ////    //bool[] bitMap = new bool[loOrderDate.Count];
        ////    //var j = 0;
        ////    //foreach (var lo in loOrderDate)
        ////    //{
        ////    //    if (dateHash.ContainsKey(lo))
        ////    //    {
        ////    //        bitMap[j] = true;
        ////    //    }
        ////    //    j++;
        ////    //}

        ////    //for (int i = 0; i < bitMap.Count(); i++)
        ////    //{
        ////    //    if (bitMap[i])
        ////    //    {
        ////    //        if (loQuantity[i] < 25)
        ////    //        {
        ////    //            var discount = loDiscount[i];
        ////    //            if (discount >= 1 && discount <= 3)
        ////    //            {
        ////    //                // do nothing Flag was already set
        ////    //            }
        ////    //            else
        ////    //                bitMap[i] = false;
        ////    //        }
        ////    //        else
        ////    //            bitMap[i] = false;
        ////    //    }
        ////    //}

        ////    //for (int k = 0; k < bitMap.LongLength; k++)
        ////    //{
        ////    //    if (bitMap[k]) // bit set to true
        ////    //    {
        ////    //        var revenue = (loExtendedPrice[k] * loDiscount[k]);
        ////    //        totalRevenue += revenue;
        ////    //    }
        ////    //}
        ////    Console.WriteLine(String.Format("Revenue is : {0}", totalRevenue));
        ////}

        ////private static void parallelImprovedColumnHashJoinQ11()
        ////{
        ////    var dateHash = new Dictionary<int, int>();
        ////    Int64 totalRevenue = 0;
        ////    foreach (var d in date)
        ////    {
        ////        if (d.dYear == 1993)
        ////            dateHash.Add(d.dDateKey, d.dYear);
        ////    }

        ////    // ConcurrentBitMap bA = new ConcurrentBitMap(loDiscount.Count);
        ////    ConcurrentBitArray bA = new ConcurrentBitArray(loDiscount.Count);

        ////    // ThreadSafeBitArray bM = new ThreadSafeBitArray(loDiscount.Count);
        ////    corderDate(dateHash, bA);

        ////    Task[] taskArray = { //Task.Factory.StartNew(() => corderDate(dateHash, bA)),
        ////        Task.Factory.StartNew(() => cdiscounts(bA)),
        ////         Task.Factory.StartNew(() => cquantity(bA))
        ////    };

        ////    Task.WaitAll(taskArray);

        ////    // var bitMap = bM.GetAllTuples();
        ////    for (int k = 0; k < bA.Length(); k++)
        ////    {
        ////        if (bA.Get(k)) // bit set to true
        ////        {
        ////            var revenue = (loExtendedPrice[k] * loDiscount[k]);
        ////            totalRevenue += revenue;
        ////        }
        ////    }
        ////    // Console.WriteLine("[PaRI] Total Revenue: {0}, bitsetCount: {1}", totalRevenue);
        ////}



        //private static int parallelTasks(int i, Dictionary<int, int> dateHash, List<int> loOrderDate, List<int> loDiscount, List<int> loQuantity, List<int> loExtendedPrice, List<bool> bitMaps)
        //{
        //    int localRevenue = 0;

        //    for (int j = 0; j < loOrderDate.Count; j++)
        //    {
        //        if (dateHash.ContainsKey(loOrderDate[j]))
        //            bitMaps[j] = true;
        //    }

        //    for (int k = 0; k < loDiscount.Count; k++)
        //    {
        //        var d = loDiscount[k];
        //        if (d >= 1 && d <= 3 && bitMaps[k])
        //        {
        //            // do nothing
        //            // check to make sure the set
        //        }
        //        else
        //            bitMaps[k] = false;
        //    }

        //    for (int l = 0; l < loQuantity.Count; l++)
        //    {
        //        if (loQuantity[l] < 25 && bitMaps[l])
        //        {
        //            // do nothing
        //        }
        //        else
        //            bitMaps[l] = false;
        //    }

        //    for (int m = 0; m < bitMaps.Count; m++)
        //    {
        //        if (bitMaps[m]) // bit set to true
        //        {
        //            var revenue = (loExtendedPrice[m] * loDiscount[m]);
        //            localRevenue += revenue;
        //        }
        //    }
        //    return localRevenue;
        //}

        //private static void parallelColumnHashJoinQ11()
        //{

        //    var dateHash = new Dictionary<int, int>();
        //    Int64 totalRevenue = 0;
        //    foreach (var d in date)
        //    {
        //        if (d.dYear == 1993)
        //            dateHash.Add(d.dDateKey, d.dYear);
        //    }
        //    var arraySize = loDiscount.Count;

        //    Task<BitArray>[] taskArray = { Task<BitArray>.Factory.StartNew(() => discounts()),
        //         Task<BitArray>.Factory.StartNew(() => quantity()),
        //         Task<BitArray>.Factory.StartNew(() => orderDate(dateHash))
        //    };
        //    Task.WaitAll(taskArray);

        //    taskArray[0].Result.And(taskArray[1].Result).And(taskArray[2].Result);

        //    int bitsetcount = 0;
        //    for (int k = 0; k < taskArray[0].Result.Count; k++)
        //    {
        //        if (taskArray[0].Result.Get(k)) // bit set to true
        //        {
        //            var revenue = (loExtendedPrice[k] * loDiscount[k]);
        //            totalRevenue += revenue;
        //            bitsetcount++;
        //        }
        //    }

        //    // Console.WriteLine("[RIJoin] Total Revenue: {0}, bitsetCount: {1}", totalRevenue, bitsetcount);
        //}

        //private static void createBinaryFiles()
        //{
        //    //var chunkSize = 690;
        //    //var chunkedDate = date.ChunkBy<Date>(chunkSize);
        //    //for (int i = 0; i < chunkedDate.Count; i++)
        //    //{
        //    //    string filePath = Path.Combine(@"C:\Raw_Data_Source_For_Test\SSBM - DBGEN\binaryFiles\date", String.Format("dateFile{0}.bin", i));
        //    //    Utils.WriteToBinaryFile<List<Date>>(filePath, chunkedDate[i]);
        //    //}

        //    //var chunkedLineOrder = lineOrder.ChunkBy<LineOrder>(chunkSize);
        //    //for (int i = 0; i < chunkedLineOrder.Count; i++)
        //    //{
        //    //    string lineOrderFile = Path.Combine(@"C:\Raw_Data_Source_For_Test\SSBM - DBGEN\binaryFiles\lineorder", String.Format("lineOrderFile{0}.bin", i));
        //    //    Utils.WriteToBinaryFile<List<LineOrder>>(lineOrderFile, chunkedLineOrder[i]);
        //    //}
        //    Console.WriteLine("Starting to Create Binary Files");
        //    Utils.WriteToBinaryFile<List<Date>>(dateFile, date);
        //    Utils.WriteToBinaryFile<List<Customer>>(customerFile, customer);
        //    Utils.WriteToBinaryFile<List<Supplier>>(customerFile, supplier);
        //    Utils.WriteToBinaryFile<List<Part>>(customerFile, part);
        //    Console.WriteLine("Creating Binary Files for tables complete.");
        //    Utils.WriteToBinaryFile<List<int>>(dDateKeyFile, dDateKey);
        //    Utils.WriteToBinaryFile<List<int>>(dYearFile, dYear);
        //    Utils.WriteToBinaryFile<List<int>>(dYearMonthNumFile, dYearMonthNum);
        //    Utils.WriteToBinaryFile<List<int>>(dDayNumInWeekFile, dDayNumInWeek);
        //    Utils.WriteToBinaryFile<List<int>>(dDayNumInMonthFile, dDayNumInMonth);
        //    Utils.WriteToBinaryFile<List<int>>(dDayNumInYearFile, dDayNumInYear);
        //    Utils.WriteToBinaryFile<List<int>>(dMonthNumInYearFile, dMonthNumInYear);
        //    Utils.WriteToBinaryFile<List<int>>(dWeekNumInYearFile, dWeekNumInYear);
        //    Utils.WriteToBinaryFile<List<int>>(dLastDayInWeekFLFile, dLastDayInWeekFL);
        //    Utils.WriteToBinaryFile<List<int>>(dLastDayInMonthFLFile, dLastDayInMonthFL);
        //    Utils.WriteToBinaryFile<List<int>>(dHolidayFLFile, dHolidayFL);
        //    Utils.WriteToBinaryFile<List<int>>(dWeekDayFLFile, dWeekDayFL);
        //    Utils.WriteToBinaryFile<List<string>>(dDateFile, dDate);
        //    Utils.WriteToBinaryFile<List<string>>(dDayOfWeekFile, dDayOfWeek);
        //    Utils.WriteToBinaryFile<List<string>>(dMonthFile, dMonth);
        //    Utils.WriteToBinaryFile<List<string>>(dYearMonthFile, dYearMonth);
        //    Utils.WriteToBinaryFile<List<string>>(dSellingSeasonFile, dSellingSeason);
        //    Console.WriteLine("Creating Binary Files for columns in DATE table complete.");

        //    Utils.WriteToBinaryFile<List<int>>(loOrderKeyFile, loOrderKey);
        //    Utils.WriteToBinaryFile<List<int>>(loLineNumberFile, loLineNumber);
        //    Utils.WriteToBinaryFile<List<int>>(loCustKeyFile, loCustKey);
        //    Utils.WriteToBinaryFile<List<int>>(loPartKeyFile, loPartKey);
        //    Utils.WriteToBinaryFile<List<int>>(loSuppKeyFile, loSuppKey);
        //    Utils.WriteToBinaryFile<List<int>>(loOrderDateFile, loOrderDate);
        //    Utils.WriteToBinaryFile<List<char>>(loShipPriorityFile, loShipPriority);
        //    Utils.WriteToBinaryFile<List<int>>(loQuantityFile, loQuantity);
        //    Utils.WriteToBinaryFile<List<int>>(loExtendedPriceFile, loExtendedPrice);
        //    Utils.WriteToBinaryFile<List<int>>(loOrdTotalPriceFile, loOrdTotalPrice);
        //    Utils.WriteToBinaryFile<List<int>>(loDiscountFile, loDiscount);
        //    Utils.WriteToBinaryFile<List<int>>(loSupplyCostFile, loSupplyCost);
        //    Utils.WriteToBinaryFile<List<int>>(loTaxFile, loTax);
        //    Utils.WriteToBinaryFile<List<int>>(loRevenueFile, loRevenue);
        //    Utils.WriteToBinaryFile<List<int>>(loCommitDateFile, loCommitDate);
        //    Utils.WriteToBinaryFile<List<string>>(loShipModeFile, loShipMode);
        //    Utils.WriteToBinaryFile<List<string>>(loOrderPriorityFile, loOrderPriority);
        //    Console.WriteLine("Creating Binary Files for columns in LINEORDER table complete.");

        //    Utils.WriteToBinaryFile<List<int>>(cCustKeyFile, cCustKey);
        //    Utils.WriteToBinaryFile<List<string>>(cNameFile, cName);
        //    Utils.WriteToBinaryFile<List<string>>(cAddressFile, cAddress);
        //    Utils.WriteToBinaryFile<List<string>>(cCityFile, cCity);
        //    Utils.WriteToBinaryFile<List<string>>(cNationFile, cNation);
        //    Utils.WriteToBinaryFile<List<string>>(cRegionFile, cRegion);
        //    Utils.WriteToBinaryFile<List<string>>(cPhoneFile, cPhone);
        //    Utils.WriteToBinaryFile<List<string>>(cMktSegmentFile, cMktSegment);
        //    Console.WriteLine("Creating Binary Files for columns in CUSTOMER table complete.");

        //    Utils.WriteToBinaryFile<List<int>>(sSuppKeyFile, sSuppKey);
        //    Utils.WriteToBinaryFile<List<string>>(sNameFile, sName);
        //    Utils.WriteToBinaryFile<List<string>>(sAddressFile, sAddress);
        //    Utils.WriteToBinaryFile<List<string>>(sCityFile, sCity);
        //    Utils.WriteToBinaryFile<List<string>>(sNationFile, sNation);
        //    Utils.WriteToBinaryFile<List<string>>(sRegionFile, sRegion);
        //    Utils.WriteToBinaryFile<List<string>>(sPhoneFile, sPhone);
        //    Console.WriteLine("Creating Binary Files for columns in SUPPLIER table complete.");

        //    Utils.WriteToBinaryFile<List<int>>(pSizeFile, pSize);
        //    Utils.WriteToBinaryFile<List<int>>(pPartKeyFile, pPartKey);
        //    Utils.WriteToBinaryFile<List<string>>(pNameFile, pName);
        //    Utils.WriteToBinaryFile<List<string>>(pMFGRFile, pMFGR);
        //    Utils.WriteToBinaryFile<List<string>>(pCategoryFile, pCategory);
        //    Utils.WriteToBinaryFile<List<string>>(pBrandFile, pBrand);
        //    Utils.WriteToBinaryFile<List<string>>(pColorFile, pColor);
        //    Utils.WriteToBinaryFile<List<string>>(pTypeFile, pType);
        //    Utils.WriteToBinaryFile<List<string>>(pContainerFile, pContainer);
        //    Console.WriteLine("Creating Binary Files for columns in PART table complete.");
        //}

        //private static void selectAllBlockProcessingHashJoin()
        //{
        //    // SELECT * FROM lineorder, date where lo_orderdate = d_datekey;
        //    var dateHash = new Dictionary<int, Date>();
        //    List<Tuple<LineOrder, Date>> joinedTuples = new List<Tuple<LineOrder, Date>>();
        //    List<Date> date = Utils.ReadFromBinaryFile<List<Date>>(dateFile);

        //    foreach (var d in date)
        //    {
        //        dateHash.Add(d.dDateKey, d);
        //    }

        //    var chunkedFiles = Directory.GetFiles(@"C:\Raw_Data_Source_For_Test\SSBM - DBGEN\binaryFiles\lineorder");
        //    foreach (var chunk in chunkedFiles)
        //    {
        //        List<LineOrder> lineOrder = Utils.ReadFromBinaryFile<List<LineOrder>>(chunk);
        //        foreach (var lo in lineOrder)
        //        {
        //            if (dateHash.ContainsKey(lo.loOrderDate))
        //            {
        //                Date dateObj;
        //                dateHash.TryGetValue(lo.loOrderDate, out dateObj);
        //                joinedTuples.Add(new Tuple<LineOrder, Date>(lo, dateObj));
        //            }
        //        }
        //    }

        //    foreach (var tuple in joinedTuples)
        //    {
        //        // Console.WriteLine(String.Format("{0},{1}", Utils.convertObjectToCSVString(tuple.Item1), Utils.convertObjectToCSVString(tuple.Item2)));
        //    }

        //    Console.WriteLine("Select All Row: " + joinedTuples.Count);
        //}

        //private static void selectAllParallelBlockProcessingHashJoin()
        //{
        //    // SELECT * FROM lineorder, date where lo_orderdate = d_datekey;
        //    var dateHash = new Dictionary<int, Date>();
        //    ConcurrentBag<Tuple<LineOrder, Date>> joinedTuples = new ConcurrentBag<Tuple<LineOrder, Date>>();

        //    List<Date> date = Utils.ReadFromBinaryFile<List<Date>>(dateFile);

        //    foreach (var d in date)
        //    {
        //        dateHash.Add(d.dDateKey, d);
        //    }

        //    var chunkedFiles = Directory.GetFiles(@"C:\Raw_Data_Source_For_Test\SSBM - DBGEN\binaryFiles\lineorder");
        //    Parallel.ForEach(chunkedFiles, new ParallelOptions { MaxDegreeOfParallelism = 3 }, (chunk) =>
        //     {
        //         List<LineOrder> lineOrder = Utils.ReadFromBinaryFile<List<LineOrder>>(chunk);
        //         foreach (var lo in lineOrder)
        //         {
        //             if (dateHash.ContainsKey(lo.loOrderDate))
        //             {
        //                 Date dateObj;
        //                 dateHash.TryGetValue(lo.loOrderDate, out dateObj);
        //                 joinedTuples.Add(new Tuple<LineOrder, Date>(lo, dateObj));
        //             }
        //         }
        //     });

        //    foreach (var tuple in joinedTuples)
        //    {
        //        // Console.WriteLine(String.Format("{0},{1}", Utils.convertObjectToCSVString(tuple.Item1), Utils.convertObjectToCSVString(tuple.Item2)));
        //    }

        //    Console.WriteLine("Select All Parallel Row: " + joinedTuples.Count);
        //}

        //private static void selectAllColumnBlockedJoinUsingEarlyMaterialization()
        //{
        //    List<Tuple<LineOrder, Date>> joinedTuples = new List<Tuple<LineOrder, Date>>();
        //    List<LineOrder> lineOrder = new List<LineOrder>();
        //    List<Date> date = new List<Date>();

        //    List<int> loOrderKey = Utils.ReadFromBinaryFile<List<int>>(loOrderKeyFile);
        //    List<int> loCustKey = Utils.ReadFromBinaryFile<List<int>>(loCustKeyFile);
        //    List<int> loLineNumber = Utils.ReadFromBinaryFile<List<int>>(loLineNumberFile);
        //    List<int> loPartKey = Utils.ReadFromBinaryFile<List<int>>(loPartKeyFile);
        //    List<int> loSuppKey = Utils.ReadFromBinaryFile<List<int>>(loSuppKeyFile);
        //    List<int> loOrderDate = Utils.ReadFromBinaryFile<List<int>>(loOrderDateFile);
        //    List<string> loOrderPriority = Utils.ReadFromBinaryFile<List<string>>(loOrderPriorityFile);
        //    List<char> loShipPriority = Utils.ReadFromBinaryFile<List<char>>(loShipPriorityFile);
        //    List<int> loQuantity = Utils.ReadFromBinaryFile<List<int>>(loQuantityFile);
        //    List<int> loExtendedPrice = Utils.ReadFromBinaryFile<List<int>>(loExtendedPriceFile);
        //    List<int> loOrdTotalPrice = Utils.ReadFromBinaryFile<List<int>>(loOrdTotalPriceFile);
        //    List<int> loDiscount = Utils.ReadFromBinaryFile<List<int>>(loDiscountFile);
        //    List<int> loRevenue = Utils.ReadFromBinaryFile<List<int>>(loRevenueFile);
        //    List<int> loSupplyCost = Utils.ReadFromBinaryFile<List<int>>(loSupplyCostFile);
        //    List<int> loTax = Utils.ReadFromBinaryFile<List<int>>(loTaxFile);
        //    List<int> loCommitDate = Utils.ReadFromBinaryFile<List<int>>(loCommitDateFile);
        //    List<string> loShipMode = Utils.ReadFromBinaryFile<List<string>>(loShipModeFile);

        //    List<int> dDateKey = Utils.ReadFromBinaryFile<List<int>>(dDateKeyFile);
        //    List<string> dDate = Utils.ReadFromBinaryFile<List<string>>(dDateFile);
        //    List<string> dDayOfWeek = Utils.ReadFromBinaryFile<List<string>>(dDayOfWeekFile);
        //    List<string> dMonth = Utils.ReadFromBinaryFile<List<string>>(dMonthFile);
        //    List<int> dYear = Utils.ReadFromBinaryFile<List<int>>(dYearFile);
        //    List<int> dYearMonthNum = Utils.ReadFromBinaryFile<List<int>>(dYearMonthNumFile);
        //    List<string> dYearMonth = Utils.ReadFromBinaryFile<List<string>>(dYearMonthFile);
        //    List<int> dDayNumInMonth = Utils.ReadFromBinaryFile<List<int>>(dDayNumInMonthFile);
        //    List<int> dDayNumInWeek = Utils.ReadFromBinaryFile<List<int>>(dDayNumInWeekFile);
        //    List<int> dMonthNumInYear = Utils.ReadFromBinaryFile<List<int>>(dMonthNumInYearFile);
        //    List<int> dWeekNumInYear = Utils.ReadFromBinaryFile<List<int>>(dWeekNumInYearFile);
        //    List<string> dSellingSeason = Utils.ReadFromBinaryFile<List<string>>(dSellingSeasonFile);
        //    List<int> dLastDayInWeekFL = Utils.ReadFromBinaryFile<List<int>>(dLastDayInWeekFLFile);
        //    List<int> dLastDayInMonthFL = Utils.ReadFromBinaryFile<List<int>>(dLastDayInMonthFLFile);
        //    List<int> dHolidayFL = Utils.ReadFromBinaryFile<List<int>>(dHolidayFLFile);
        //    List<int> dWeekDayFL = Utils.ReadFromBinaryFile<List<int>>(dWeekDayFLFile);

        //    int i = 0;
        //    foreach (var orderDate in loOrderDate)
        //    {
        //        lineOrder.Add(new LineOrder(loOrderKey[i], loLineNumber[i], loCustKey[i], loPartKey[i], loSuppKey[i], loOrderDate[i], loOrderPriority[i],
        //          Convert.ToChar(loShipPriority[i]), loQuantity[i], loExtendedPrice[i], loOrdTotalPrice[i], loDiscount[i], loRevenue[i], loSupplyCost[i],
        //          loTax[i], loCommitDate[i], loShipMode[i]));
        //        i++;
        //    }
        //    int j = 0;
        //    foreach (var dt in dYear)
        //    {
        //        date.Add(new Date(dDateKey[j], dDate[j], dDayOfWeek[j], dMonth[j],
        //            dYear[j], dYearMonthNum[j],
        //                dYearMonth[j], dDayNumInWeek[j], dDayNumInMonth[j],
        //                dDayNumInMonth[j], dMonthNumInYear[j], dWeekNumInYear[j],
        //                dSellingSeason[j],
        //                dLastDayInWeekFL[j], dLastDayInMonthFL[j], dHolidayFL[j],
        //                dWeekDayFL[j]));
        //        j++;
        //    }

        //    var dateHash = new Dictionary<int, Date>();
        //    foreach (var d in date)
        //    {
        //        dateHash.Add(d.dDateKey, d);
        //    }
        //    foreach (var lo in lineOrder)
        //    {
        //        if (dateHash.ContainsKey(lo.loOrderDate))
        //        {
        //            Date dateObj;
        //            dateHash.TryGetValue(lo.loOrderDate, out dateObj);
        //            joinedTuples.Add(new Tuple<LineOrder, Date>(lo, dateObj));
        //        }
        //    }
        //    foreach (var tuple in joinedTuples)
        //    {
        //        // Console.WriteLine(String.Format("{0},{1}", Utils.convertObjectToCSVString(tuple.Item1), Utils.convertObjectToCSVString(tuple.Item2)));
        //    }
        //    Console.WriteLine("Select All Column: " + joinedTuples.Count);
        //}

        //private static void selectAllColumnParallelBlockedJoinUsingEarlyMaterialization()
        //{
        //    ConcurrentBag<Tuple<LineOrder, Date>> joinedTuples = new ConcurrentBag<Tuple<LineOrder, Date>>();
        //    List<LineOrder> lineOrder = new List<LineOrder>();
        //    List<Date> date = new List<Date>();
        //    List<int> loOrderKey = null;
        //    List<int> loCustKey = null;
        //    List<int> loLineNumber = null;
        //    List<int> loPartKey = null;
        //    List<int> loSuppKey = null;
        //    List<int> loOrderDate = null;
        //    List<string> loOrderPriority = null;
        //    List<char> loShipPriority = null;
        //    List<int> loQuantity = null;
        //    List<int> loExtendedPrice = null;
        //    List<int> loOrdTotalPrice = null;
        //    List<int> loDiscount = null;
        //    List<int> loRevenue = null;
        //    List<int> loSupplyCost = null;
        //    List<int> loTax = null;
        //    List<int> loCommitDate = null;
        //    List<string> loShipMode = null;
        //    List<int> dDateKey = null;
        //    List<string> dDate = null;
        //    List<string> dDayOfWeek = null;
        //    List<string> dMonth = null;
        //    List<int> dYear = null;
        //    List<int> dYearMonthNum = null;
        //    List<string> dYearMonth = null;
        //    List<int> dDayNumInMonth = null;
        //    List<int> dDayNumInWeek = null;
        //    List<int> dMonthNumInYear = null;
        //    List<int> dWeekNumInYear = null;
        //    List<string> dSellingSeason = null;
        //    List<int> dLastDayInWeekFL = null;
        //    List<int> dLastDayInMonthFL = null;
        //    List<int> dHolidayFL = null;
        //    List<int> dWeekDayFL = null;
        //    ParallelOptions po = new ParallelOptions { MaxDegreeOfParallelism = 3 };
        //    Parallel.Invoke(po,
        //         () => loOrderKey = Utils.ReadFromBinaryFile<List<int>>(loOrderKeyFile),
        //         () => loCustKey = Utils.ReadFromBinaryFile<List<int>>(loCustKeyFile),
        //         () => loLineNumber = Utils.ReadFromBinaryFile<List<int>>(loLineNumberFile),
        //         () => loPartKey = Utils.ReadFromBinaryFile<List<int>>(loPartKeyFile),
        //         () => loSuppKey = Utils.ReadFromBinaryFile<List<int>>(loSuppKeyFile),
        //         () => loOrderDate = Utils.ReadFromBinaryFile<List<int>>(loOrderDateFile),
        //         () => loOrderPriority = Utils.ReadFromBinaryFile<List<string>>(loOrderPriorityFile),
        //         () => loShipPriority = Utils.ReadFromBinaryFile<List<char>>(loShipPriorityFile),
        //         () => loQuantity = Utils.ReadFromBinaryFile<List<int>>(loQuantityFile),
        //         () => loExtendedPrice = Utils.ReadFromBinaryFile<List<int>>(loExtendedPriceFile),
        //         () => loOrdTotalPrice = Utils.ReadFromBinaryFile<List<int>>(loOrdTotalPriceFile),
        //         () => loDiscount = Utils.ReadFromBinaryFile<List<int>>(loDiscountFile),
        //         () => loRevenue = Utils.ReadFromBinaryFile<List<int>>(loRevenueFile),
        //         () => loSupplyCost = Utils.ReadFromBinaryFile<List<int>>(loSupplyCostFile),
        //         () => loTax = Utils.ReadFromBinaryFile<List<int>>(loTaxFile),
        //         () => loCommitDate = Utils.ReadFromBinaryFile<List<int>>(loCommitDateFile),
        //         () => loShipMode = Utils.ReadFromBinaryFile<List<string>>(loShipModeFile),
        //         () => dDateKey = Utils.ReadFromBinaryFile<List<int>>(dDateKeyFile),
        //        () => dDate = Utils.ReadFromBinaryFile<List<string>>(dDateFile),
        //        () => dDayOfWeek = Utils.ReadFromBinaryFile<List<string>>(dDayOfWeekFile),
        //        () => dMonth = Utils.ReadFromBinaryFile<List<string>>(dMonthFile),
        //        () => dYear = Utils.ReadFromBinaryFile<List<int>>(dYearFile),
        //        () => dYearMonthNum = Utils.ReadFromBinaryFile<List<int>>(dYearMonthNumFile),
        //        () => dYearMonth = Utils.ReadFromBinaryFile<List<string>>(dYearMonthFile),
        //        () => dDayNumInMonth = Utils.ReadFromBinaryFile<List<int>>(dDayNumInMonthFile),
        //        () => dDayNumInWeek = Utils.ReadFromBinaryFile<List<int>>(dDayNumInWeekFile),
        //        () => dMonthNumInYear = Utils.ReadFromBinaryFile<List<int>>(dMonthNumInYearFile),
        //        () => dWeekNumInYear = Utils.ReadFromBinaryFile<List<int>>(dWeekNumInYearFile),
        //        () => dSellingSeason = Utils.ReadFromBinaryFile<List<string>>(dSellingSeasonFile),
        //        () => dLastDayInWeekFL = Utils.ReadFromBinaryFile<List<int>>(dLastDayInWeekFLFile),
        //        () => dLastDayInMonthFL = Utils.ReadFromBinaryFile<List<int>>(dLastDayInMonthFLFile),
        //        () => dHolidayFL = Utils.ReadFromBinaryFile<List<int>>(dHolidayFLFile),
        //        () => dWeekDayFL = Utils.ReadFromBinaryFile<List<int>>(dWeekDayFLFile)
        //             );


        //    Task[] taskArray = {
        //        Task.Factory.StartNew(() => {
        //            for (int i = 0; i < loOrderDate.Count; i++) {
        //                lineOrder.Add(new LineOrder(loOrderKey[i], loLineNumber[i], loCustKey[i], loPartKey[i], loSuppKey[i], loOrderDate[i], loOrderPriority[i],
        //                Convert.ToChar(loShipPriority[i]), loQuantity[i], loExtendedPrice[i], loOrdTotalPrice[i], loDiscount[i], loRevenue[i], loSupplyCost[i],
        //                loTax[i], loCommitDate[i], loShipMode[i]));
        //        }
        //       }),
        //        Task.Factory.StartNew(()=> {
        //            for (int j = 0; j < dYear.Count; j++) {
        //                date.Add(new Date(dDateKey[j], dDate[j], dDayOfWeek[j], dMonth[j], dYear[j], dYearMonthNum[j],
        //                dYearMonth[j], dDayNumInWeek[j], dDayNumInMonth[j],
        //                dDayNumInMonth[j], dMonthNumInYear[j], dWeekNumInYear[j],
        //                dSellingSeason[j],
        //                dLastDayInWeekFL[j], dLastDayInMonthFL[j], dHolidayFL[j],
        //                dWeekDayFL[j]));
        //    }})};

        //    Task.WaitAll(taskArray);

        //    var dateHash = new Dictionary<int, Date>();
        //    foreach (var d in date)
        //    {
        //        dateHash.Add(d.dDateKey, d);
        //    }

        //    Parallel.ForEach(lineOrder, po, (lo) =>
        //     {
        //         if (dateHash.ContainsKey(lo.loOrderDate))
        //         {
        //             Date dateObj;
        //             dateHash.TryGetValue(lo.loOrderDate, out dateObj);
        //             joinedTuples.Add(new Tuple<LineOrder, Date>(lo, dateObj));
        //         }
        //     }
        //    );

        //    foreach (var tuple in joinedTuples)
        //    {
        //        // Console.WriteLine(String.Format("{0},{1}", Utils.convertObjectToCSVString(tuple.Item1), Utils.convertObjectToCSVString(tuple.Item2)));
        //    }
        //    Console.WriteLine("Select All Column: " + joinedTuples.Count);
        //}

        //private static void selectedColumnBlockedJoinUsingLateMaterailization()
        //{
        //    var dateHash = new Dictionary<int, int>();
        //    List<string> joinedTuples = new List<string>();
        //    List<Date> date = Utils.ReadFromBinaryFile<List<Date>>(dateFile);

        //    var j = 0;
        //    foreach (var d in date)
        //    {
        //        dateHash.Add(d.dDateKey, j);
        //        j++;
        //    }

        //    List<int> loOrderDate = Utils.ReadFromBinaryFile<List<int>>(loOrderDateFile);
        //    List<int> loDiscount = Utils.ReadFromBinaryFile<List<int>>(loDiscountFile);
        //    List<int> loExtendedPrice = Utils.ReadFromBinaryFile<List<int>>(loExtendedPriceFile);
        //    var i = 0;
        //    foreach (var orderDate in loOrderDate)
        //    {
        //        if (dateHash.ContainsKey(orderDate))
        //        {
        //            int idx;
        //            dateHash.TryGetValue(orderDate, out idx);
        //            joinedTuples.Add(String.Format(@"{0},{1}", loExtendedPrice[i], loDiscount[i]));
        //        }
        //        i++;
        //    }
        //    foreach (var tuple in joinedTuples)
        //    {
        //        //Console.WriteLine(tuple);
        //    }
        //    Console.WriteLine("Selected: " + joinedTuples.Count);
        //}

        //private static void Q11DMSerialEarlyMaterialization()
        //{
        //    List<LineOrder> lineOrder = new List<LineOrder>();
        //    List<Date> date = new List<Date>();

        //    Stopwatch sw = Stopwatch.StartNew();
        //    List<int> loOrderDate = Utils.ReadFromBinaryFile<List<int>>(loOrderDateFile);
        //    List<int> loQuantity = Utils.ReadFromBinaryFile<List<int>>(loQuantityFile);
        //    List<int> loExtendedPrice = Utils.ReadFromBinaryFile<List<int>>(loExtendedPriceFile);
        //    List<int> loDiscount = Utils.ReadFromBinaryFile<List<int>>(loDiscountFile);

        //    List<int> dDateKey = Utils.ReadFromBinaryFile<List<int>>(dDateKeyFile);
        //    List<int> dYear = Utils.ReadFromBinaryFile<List<int>>(dYearFile);
        //    sw.Stop();

        //    Console.Write("I/O time: {0} ms ", sw.ElapsedMilliseconds);

        //    sw.Reset();
        //    sw.Start();
        //    for (int i = 0; i < loOrderDate.Count; i++)
        //    {
        //        LineOrder lineOrderObj = new LineOrder();
        //        lineOrderObj.loOrderDate = loOrderDate[i];
        //        lineOrderObj.loQuantity = loQuantity[i];
        //        lineOrderObj.loExtendedPrice = loExtendedPrice[i];
        //        lineOrderObj.loDiscount = loDiscount[i];
        //        lineOrder.Add(lineOrderObj);
        //    }

        //    for (int i = 0; i < dYear.Count; i++)
        //    {
        //        Date dateObj = new Date();
        //        dateObj.dDateKey = dDateKey[i];
        //        dateObj.dYear = dYear[i];
        //        date.Add(dateObj);
        //    }

        //    var dateHash = new Dictionary<int, Date>();
        //    foreach (var d in date)
        //    {
        //        if (d.dYear == 1993)
        //            dateHash.Add(d.dDateKey, d);
        //    }

        //    Int64 totalRevenue = 0;
        //    foreach (var lo in lineOrder)
        //    {
        //        if (dateHash.ContainsKey(lo.loOrderDate) && lo.loDiscount <= 3 && lo.loDiscount >= 1 && lo.loQuantity < 25)
        //        {
        //            var revenue = (lo.loExtendedPrice * lo.loDiscount);
        //            totalRevenue += revenue;
        //        }
        //    }
        //    sw.Stop();
        //    Console.WriteLine("Memory Operation Time: {0} ms", sw.ElapsedMilliseconds);
        //    //Console.WriteLine("[Q11DMSerialEarlyMaterialization] Revenue: {0}", totalRevenue);
        //}

        //private static void Q11DMParallelEarlyMaterialization(int maxDegreeOfParallelism)
        //{
        //    ParallelOptions parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = maxDegreeOfParallelism };
        //    List<LineOrder> lineOrder = new List<LineOrder>();
        //    List<Date> date = new List<Date>();

        //    List<int> loOrderDate = new List<int>();
        //    List<int> loQuantity = new List<int>();
        //    List<int> loExtendedPrice = new List<int>();
        //    List<int> loDiscount = new List<int>();

        //    List<int> dDateKey = new List<int>();
        //    List<int> dYear = new List<int>();

        //    Stopwatch sw = Stopwatch.StartNew();
        //    Parallel.Invoke(parallelOptions,
        //        () => loOrderDate = Utils.ReadFromBinaryFile<List<int>>(loOrderDateFile),
        //        () => loQuantity = Utils.ReadFromBinaryFile<List<int>>(loQuantityFile),
        //        () => loExtendedPrice = Utils.ReadFromBinaryFile<List<int>>(loExtendedPriceFile),
        //        () => loDiscount = Utils.ReadFromBinaryFile<List<int>>(loDiscountFile),
        //        () => dDateKey = Utils.ReadFromBinaryFile<List<int>>(dDateKeyFile),
        //        () => dYear = Utils.ReadFromBinaryFile<List<int>>(dYearFile)
        //        );
        //    sw.Stop();
        //    Console.Write("I/O time: {0} ms ", sw.ElapsedMilliseconds);
        //    sw.Reset();
        //    sw.Start();
        //    Parallel.Invoke(parallelOptions,
        //        () =>
        //        {
        //            for (int i = 0; i < loOrderDate.Count; i++)
        //            {
        //                LineOrder lineOrderObj = new LineOrder();
        //                lineOrderObj.loOrderDate = loOrderDate[i];
        //                lineOrderObj.loQuantity = loQuantity[i];
        //                lineOrderObj.loExtendedPrice = loExtendedPrice[i];
        //                lineOrderObj.loDiscount = loDiscount[i];
        //                lineOrder.Add(lineOrderObj);
        //            }
        //        },
        //        () =>
        //        {
        //            for (int i = 0; i < dYear.Count; i++)
        //            {
        //                Date dateObj = new Date();
        //                dateObj.dDateKey = dDateKey[i];
        //                dateObj.dYear = dYear[i];
        //                date.Add(dateObj);
        //            }
        //        });

        //    var dateHash = new Dictionary<int, Date>();
        //    foreach (var d in date)
        //    {
        //        if (d.dYear == 1993)
        //            dateHash.Add(d.dDateKey, d);
        //    }

        //    Int64 totalRevenue = 0;
        //    foreach (var lo in lineOrder)
        //    {
        //        if (dateHash.ContainsKey(lo.loOrderDate) && lo.loDiscount <= 3 && lo.loDiscount >= 1 && lo.loQuantity < 25)
        //        {
        //            var revenue = (lo.loExtendedPrice * lo.loDiscount);
        //            totalRevenue += revenue;
        //        }
        //    }
        //    sw.Stop();
        //    Console.WriteLine("Memory Operation Time: {0} ms", sw.ElapsedMilliseconds);
        //    // Console.WriteLine("[Q11DMParallelEarlyMaterialization] Revenue: {0}", totalRevenue);
        //}

        //private static void Q11DMSerialInvisibleJoin()
        //{
        //    var dateHash = new Dictionary<int, int>();
        //    Int64 totalRevenue = 0;
        //    Stopwatch sw = Stopwatch.StartNew();
        //    List<Date> date = Utils.ReadFromBinaryFile<List<Date>>(dateFile);
        //    List<int> loOrderDate = Utils.ReadFromBinaryFile<List<int>>(loOrderDateFile);
        //    List<int> loQuantity = Utils.ReadFromBinaryFile<List<int>>(loQuantityFile);
        //    List<int> loExtendedPrice = Utils.ReadFromBinaryFile<List<int>>(loExtendedPriceFile);
        //    List<int> loDiscount = Utils.ReadFromBinaryFile<List<int>>(loDiscountFile);
        //    sw.Stop();
        //    //Console.Write("I/O time: {0} ms ", sw.ElapsedMilliseconds);
        //    sw.Reset();
        //    sw.Start();
        //    foreach (var d in date)
        //    {
        //        if (d.dYear == 1993)
        //            dateHash.Add(d.dDateKey, d.dYear);
        //    }

        //    var arraySize = loDiscount.Count;
        //    BitArray baDis = new BitArray(arraySize);
        //    var dis = 0;
        //    foreach (var d in loDiscount)
        //    {
        //        if (d >= 1 && d <= 3)
        //            baDis.Set(dis, true);
        //        dis++;
        //    }

        //    BitArray baQty = new BitArray(arraySize);
        //    var qty = 0;
        //    foreach (var d in loQuantity)
        //    {
        //        if (d < 25)
        //            baQty.Set(qty, true);
        //        qty++;
        //    }

        //    BitArray baOD = new BitArray(arraySize);
        //    var i = 0;
        //    foreach (var lo in loOrderDate)
        //    {
        //        if (dateHash.ContainsKey(lo))
        //        {
        //            baOD.Set(i, true);
        //        }
        //        i++;
        //    }

        //    baOD.And(baDis);
        //    baOD.And(baQty);

        //    for (int k = 0; k < baOD.Count; k++)
        //    {
        //        if (baOD.Get(k)) // bit set to true
        //        {
        //            var revenue = (loExtendedPrice[k] * loDiscount[k]);
        //            totalRevenue += revenue;
        //        }
        //    }
        //    sw.Stop();
        //    // Console.WriteLine("Memory operation time: {0} ms", sw.ElapsedMilliseconds);
        //    //Console.WriteLine(String.Format("[CJ] Revenue is : {0}, bitsetcount: {1}", totalRevenue, bitsetcount));
        //}

        //private static void Q12DMSerialInvisibleJoin()
        //{
        //    var dateHash = new Dictionary<int, int>();
        //    Int64 totalRevenue = 0;
        //    Stopwatch sw = Stopwatch.StartNew();
        //    List<Date> date = Utils.ReadFromBinaryFile<List<Date>>(dateFile);
        //    List<int> loOrderDate = Utils.ReadFromBinaryFile<List<int>>(loOrderDateFile);
        //    List<int> loQuantity = Utils.ReadFromBinaryFile<List<int>>(loQuantityFile);
        //    List<int> loExtendedPrice = Utils.ReadFromBinaryFile<List<int>>(loExtendedPriceFile);
        //    List<int> loDiscount = Utils.ReadFromBinaryFile<List<int>>(loDiscountFile);
        //    sw.Stop();
        //    // Console.Write("I/O time: {0} ms ", sw.ElapsedMilliseconds);
        //    sw.Reset();
        //    sw.Start();
        //    foreach (var d in date)
        //    {
        //        if (d.dYearMonthNum == 199401)
        //            dateHash.Add(d.dDateKey, d.dYear);
        //    }

        //    var arraySize = loDiscount.Count;
        //    BitArray baDis = new BitArray(arraySize);
        //    var dis = 0;
        //    foreach (var d in loDiscount)
        //    {
        //        if (d >= 4 && d <= 6)
        //            baDis.Set(dis, true);
        //        dis++;
        //    }

        //    BitArray baQty = new BitArray(arraySize);
        //    var qty = 0;
        //    foreach (var d in loQuantity)
        //    {
        //        if (d >= 26 && d <= 35)
        //            baQty.Set(qty, true);
        //        qty++;
        //    }

        //    BitArray baOD = new BitArray(arraySize);
        //    var i = 0;
        //    foreach (var lo in loOrderDate)
        //    {
        //        if (dateHash.ContainsKey(lo))
        //        {
        //            baOD.Set(i, true);
        //        }
        //        i++;
        //    }

        //    baOD.And(baDis);
        //    baOD.And(baQty);

        //    for (int k = 0; k < baOD.Count; k++)
        //    {
        //        if (baOD.Get(k)) // bit set to true
        //        {
        //            var revenue = (loExtendedPrice[k] * loDiscount[k]);
        //            totalRevenue += revenue;
        //        }
        //    }
        //    sw.Stop();
        //    //Console.WriteLine("Memory operation time: {0} ms", sw.ElapsedMilliseconds);
        //    //Console.WriteLine(String.Format("[CJ] Revenue is : {0}, bitsetcount: {1}", totalRevenue, bitsetcount));
        //}

        //private static void Q13DMSerialInvisibleJoin()
        //{
        //    var dateHash = new Dictionary<int, int>();
        //    Int64 totalRevenue = 0;
        //    Stopwatch sw = Stopwatch.StartNew();
        //    List<Date> date = Utils.ReadFromBinaryFile<List<Date>>(dateFile);
        //    List<int> loOrderDate = Utils.ReadFromBinaryFile<List<int>>(loOrderDateFile);
        //    List<int> loQuantity = Utils.ReadFromBinaryFile<List<int>>(loQuantityFile);
        //    List<int> loExtendedPrice = Utils.ReadFromBinaryFile<List<int>>(loExtendedPriceFile);
        //    List<int> loDiscount = Utils.ReadFromBinaryFile<List<int>>(loDiscountFile);
        //    sw.Stop();
        //    //Console.Write("I/O time: {0} ms ", sw.ElapsedMilliseconds);
        //    sw.Reset();
        //    sw.Start();
        //    foreach (var d in date)
        //    {
        //        if (d.dWeekNumInYear == 6 && d.dYear == 1994)
        //            dateHash.Add(d.dDateKey, d.dYear);
        //    }

        //    var arraySize = loDiscount.Count;
        //    BitArray baDis = new BitArray(arraySize);
        //    var dis = 0;
        //    foreach (var d in loDiscount)
        //    {
        //        if (d >= 5 && d <= 7)
        //            baDis.Set(dis, true);
        //        dis++;
        //    }

        //    BitArray baQty = new BitArray(arraySize);
        //    var qty = 0;
        //    foreach (var d in loQuantity)
        //    {
        //        if (d >= 26 && d <= 35)
        //            baQty.Set(qty, true);
        //        qty++;
        //    }

        //    BitArray baOD = new BitArray(arraySize);
        //    var i = 0;
        //    foreach (var lo in loOrderDate)
        //    {
        //        if (dateHash.ContainsKey(lo))
        //        {
        //            baOD.Set(i, true);
        //        }
        //        i++;
        //    }

        //    baOD.And(baDis);
        //    baOD.And(baQty);

        //    for (int k = 0; k < baOD.Count; k++)
        //    {
        //        if (baOD.Get(k)) // bit set to true
        //        {
        //            var revenue = (loExtendedPrice[k] * loDiscount[k]);
        //            totalRevenue += revenue;
        //        }
        //    }
        //    sw.Stop();
        //    //Console.WriteLine("Memory operation time: {0} ms", sw.ElapsedMilliseconds);
        //    //Console.WriteLine(String.Format("[CJ] Revenue is : {0}, bitsetcount: {1}", totalRevenue, bitsetcount));
        //}

        //private static void Q21DMSerialInvisibleJoin()
        //{
        //    var dateHash = new Dictionary<int, int>();
        //    Int64 totalRevenue = 0;
        //    Stopwatch sw = Stopwatch.StartNew();
        //    List<Date> date = Utils.ReadFromBinaryFile<List<Date>>(dateFile);
        //    List<int> loOrderDate = Utils.ReadFromBinaryFile<List<int>>(loOrderDateFile);
        //    List<int> loQuantity = Utils.ReadFromBinaryFile<List<int>>(loQuantityFile);
        //    List<int> loExtendedPrice = Utils.ReadFromBinaryFile<List<int>>(loExtendedPriceFile);
        //    List<int> loDiscount = Utils.ReadFromBinaryFile<List<int>>(loDiscountFile);
        //    sw.Stop();
        //    //Console.Write("I/O time: {0} ms ", sw.ElapsedMilliseconds);
        //    sw.Reset();
        //    sw.Start();
        //    foreach (var d in date)
        //    {
        //        if (d.dWeekNumInYear == 6 && d.dYear == 1994)
        //            dateHash.Add(d.dDateKey, d.dYear);
        //    }

        //    var arraySize = loDiscount.Count;
        //    BitArray baDis = new BitArray(arraySize);
        //    var dis = 0;
        //    foreach (var d in loDiscount)
        //    {
        //        if (d >= 5 && d <= 7)
        //            baDis.Set(dis, true);
        //        dis++;
        //    }

        //    BitArray baQty = new BitArray(arraySize);
        //    var qty = 0;
        //    foreach (var d in loQuantity)
        //    {
        //        if (d >= 26 && d <= 35)
        //            baQty.Set(qty, true);
        //        qty++;
        //    }

        //    BitArray baOD = new BitArray(arraySize);
        //    var i = 0;
        //    foreach (var lo in loOrderDate)
        //    {
        //        if (dateHash.ContainsKey(lo))
        //        {
        //            baOD.Set(i, true);
        //        }
        //        i++;
        //    }

        //    baOD.And(baDis);
        //    baOD.And(baQty);

        //    for (int k = 0; k < baOD.Count; k++)
        //    {
        //        if (baOD.Get(k)) // bit set to true
        //        {
        //            var revenue = (loExtendedPrice[k] * loDiscount[k]);
        //            totalRevenue += revenue;
        //        }
        //    }
        //    sw.Stop();
        //    //Console.WriteLine("Memory operation time: {0} ms", sw.ElapsedMilliseconds);
        //    //Console.WriteLine(String.Format("[CJ] Revenue is : {0}, bitsetcount: {1}", totalRevenue, bitsetcount));
        //}

        //private static void Q11DMParallelInvisibleJoin(int maxDegreeOfParallelism)
        //{
        //    ParallelOptions parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = maxDegreeOfParallelism };
        //    var dateHash = new Dictionary<int, int>();
        //    Int64 totalRevenue = 0;

        //    Stopwatch sw = Stopwatch.StartNew();
        //    List<Date> date = new List<Date>();
        //    List<int> loOrderDate = new List<int>();
        //    List<int> loQuantity = new List<int>();
        //    List<int> loExtendedPrice = new List<int>();
        //    List<int> loDiscount = new List<int>();

        //    Parallel.Invoke(parallelOptions,
        //        () => loOrderDate = Utils.ReadFromBinaryFile<List<int>>(loOrderDateFile),
        //        () => loQuantity = Utils.ReadFromBinaryFile<List<int>>(loQuantityFile),
        //        () => loExtendedPrice = Utils.ReadFromBinaryFile<List<int>>(loExtendedPriceFile),
        //        () => loDiscount = Utils.ReadFromBinaryFile<List<int>>(loDiscountFile),
        //        () => date = Utils.ReadFromBinaryFile<List<Date>>(dateFile)
        //        );
        //    sw.Stop();
        //    //Console.Write("I/O time: {0} ms ", sw.ElapsedMilliseconds);
        //    sw.Reset();
        //    sw.Start();

        //    foreach (var d in date)
        //    {
        //        if (d.dYear == 1993)
        //            dateHash.Add(d.dDateKey, d.dYear);
        //    }
        //    var arraySize = loDiscount.Count;
        //    BitArray baDis = new BitArray(arraySize);
        //    BitArray baQty = new BitArray(arraySize);
        //    BitArray baOD = new BitArray(arraySize);

        //    Parallel.Invoke(parallelOptions,
        //       () =>
        //       {
        //           var dis = 0;
        //           foreach (var d in loDiscount)
        //           {
        //               if (d >= 1 && d <= 3)
        //                   baDis.Set(dis, true);
        //               dis++;
        //           }
        //       },
        //       () =>
        //       {
        //           var qty = 0;
        //           foreach (var d in loQuantity)
        //           {
        //               if (d < 25)
        //                   baQty.Set(qty, true);
        //               qty++;
        //           }
        //       },
        //       () =>
        //       {
        //           var i = 0;
        //           foreach (var lo in loOrderDate)
        //           {
        //               if (dateHash.ContainsKey(lo))
        //               {
        //                   baOD.Set(i, true);
        //               }
        //               i++;
        //           }
        //       }
        //       );
        //    baOD.And(baDis);
        //    baOD.And(baQty);

        //    for (int k = 0; k < baOD.Count; k++)
        //    {
        //        if (baOD.Get(k)) // bit set to true
        //        {
        //            var revenue = (loExtendedPrice[k] * loDiscount[k]);
        //            totalRevenue += revenue;
        //        }
        //    }
        //    sw.Stop();
        //    // Console.WriteLine("Memory operation time: {0}ms", sw.ElapsedMilliseconds);
        //    //Console.WriteLine(String.Format("[CJ] Revenue is : {0}, bitsetcount: {1}", totalRevenue, bitsetcount));
        //}

        //private static void Q12DMParallelInvisibleJoin(int maxDegreeOfParallelism)
        //{
        //    ParallelOptions parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = maxDegreeOfParallelism };
        //    var dateHash = new Dictionary<int, int>();
        //    Int64 totalRevenue = 0;

        //    Stopwatch sw = Stopwatch.StartNew();
        //    List<Date> date = new List<Date>();
        //    List<int> loOrderDate = new List<int>();
        //    List<int> loQuantity = new List<int>();
        //    List<int> loExtendedPrice = new List<int>();
        //    List<int> loDiscount = new List<int>();

        //    Parallel.Invoke(parallelOptions,
        //        () => loOrderDate = Utils.ReadFromBinaryFile<List<int>>(loOrderDateFile),
        //        () => loQuantity = Utils.ReadFromBinaryFile<List<int>>(loQuantityFile),
        //        () => loExtendedPrice = Utils.ReadFromBinaryFile<List<int>>(loExtendedPriceFile),
        //        () => loDiscount = Utils.ReadFromBinaryFile<List<int>>(loDiscountFile),
        //        () => date = Utils.ReadFromBinaryFile<List<Date>>(dateFile)
        //        );
        //    sw.Stop();
        //    //Console.Write("I/O time: {0} ms ", sw.ElapsedMilliseconds);
        //    sw.Reset();
        //    sw.Start();

        //    foreach (var d in date)
        //    {
        //        if (d.dYearMonthNum == 199401)
        //            dateHash.Add(d.dDateKey, d.dYear);
        //    }
        //    var arraySize = loDiscount.Count;
        //    BitArray baDis = new BitArray(arraySize);
        //    BitArray baQty = new BitArray(arraySize);
        //    BitArray baOD = new BitArray(arraySize);

        //    Parallel.Invoke(parallelOptions,
        //       () =>
        //       {
        //           var dis = 0;
        //           foreach (var d in loDiscount)
        //           {
        //               if (d >= 4 && d <= 6)
        //                   baDis.Set(dis, true);
        //               dis++;
        //           }
        //       },
        //       () =>
        //       {
        //           var qty = 0;
        //           foreach (var d in loQuantity)
        //           {
        //               if (d >= 26 && d <= 35)
        //                   baQty.Set(qty, true);
        //               qty++;
        //           }
        //       },
        //       () =>
        //       {
        //           var i = 0;
        //           foreach (var lo in loOrderDate)
        //           {
        //               if (dateHash.ContainsKey(lo))
        //               {
        //                   baOD.Set(i, true);
        //               }
        //               i++;
        //           }
        //       }
        //       );
        //    baOD.And(baDis);
        //    baOD.And(baQty);

        //    for (int k = 0; k < baOD.Count; k++)
        //    {
        //        if (baOD.Get(k)) // bit set to true
        //        {
        //            var revenue = (loExtendedPrice[k] * loDiscount[k]);
        //            totalRevenue += revenue;
        //        }
        //    }
        //    sw.Stop();
        //    //Console.WriteLine("Memory operation time: {0}ms", sw.ElapsedMilliseconds);
        //    //Console.WriteLine(String.Format("[CJ] Revenue is : {0}, bitsetcount: {1}", totalRevenue, bitsetcount));
        //}

        //private static void Q13DMParallelInvisibleJoin(int maxDegreeOfParallelism)
        //{
        //    ParallelOptions parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = maxDegreeOfParallelism };
        //    var dateHash = new Dictionary<int, int>();
        //    Int64 totalRevenue = 0;

        //    Stopwatch sw = Stopwatch.StartNew();
        //    List<Date> date = new List<Date>();
        //    List<int> loOrderDate = new List<int>();
        //    List<int> loQuantity = new List<int>();
        //    List<int> loExtendedPrice = new List<int>();
        //    List<int> loDiscount = new List<int>();

        //    Parallel.Invoke(parallelOptions,
        //        () => loOrderDate = Utils.ReadFromBinaryFile<List<int>>(loOrderDateFile),
        //        () => loQuantity = Utils.ReadFromBinaryFile<List<int>>(loQuantityFile),
        //        () => loExtendedPrice = Utils.ReadFromBinaryFile<List<int>>(loExtendedPriceFile),
        //        () => loDiscount = Utils.ReadFromBinaryFile<List<int>>(loDiscountFile),
        //        () => date = Utils.ReadFromBinaryFile<List<Date>>(dateFile)
        //        );
        //    sw.Stop();
        //    //Console.Write("I/O time: {0} ms ", sw.ElapsedMilliseconds);
        //    sw.Reset();
        //    sw.Start();

        //    foreach (var d in date)
        //    {
        //        if (d.dWeekNumInYear == 6 && d.dYear == 1994)
        //            dateHash.Add(d.dDateKey, d.dYear);
        //    }
        //    var arraySize = loDiscount.Count;
        //    BitArray baDis = new BitArray(arraySize);
        //    BitArray baQty = new BitArray(arraySize);
        //    BitArray baOD = new BitArray(arraySize);

        //    Parallel.Invoke(parallelOptions,
        //       () =>
        //       {
        //           var dis = 0;
        //           foreach (var d in loDiscount)
        //           {
        //               if (d >= 5 && d <= 7)
        //                   baDis.Set(dis, true);
        //               dis++;
        //           }
        //       },
        //       () =>
        //       {
        //           var qty = 0;
        //           foreach (var d in loQuantity)
        //           {
        //               if (d >= 26 && d <= 35)
        //                   baQty.Set(qty, true);
        //               qty++;
        //           }
        //       },
        //       () =>
        //       {
        //           var i = 0;
        //           foreach (var lo in loOrderDate)
        //           {
        //               if (dateHash.ContainsKey(lo))
        //               {
        //                   baOD.Set(i, true);
        //               }
        //               i++;
        //           }
        //       }
        //       );
        //    baOD.And(baDis);
        //    baOD.And(baQty);

        //    for (int k = 0; k < baOD.Count; k++)
        //    {
        //        if (baOD.Get(k)) // bit set to true
        //        {
        //            var revenue = (loExtendedPrice[k] * loDiscount[k]);
        //            totalRevenue += revenue;
        //        }
        //    }
        //    sw.Stop();
        //    //Console.WriteLine("Memory operation time: {0}ms", sw.ElapsedMilliseconds);
        //    //Console.WriteLine(String.Format("[CJ] Revenue is : {0}, bitsetcount: {1}", totalRevenue, bitsetcount));
        //}

        //private static void Q11DMRIJoin()
        //{
        //    var dateHash = new Dictionary<int, int>();
        //    Int64 totalRevenue = 0;
        //    Stopwatch sw = Stopwatch.StartNew();
        //    List<Date> date = Utils.ReadFromBinaryFile<List<Date>>(dateFile);
        //    List<int> loOrderDate = Utils.ReadFromBinaryFile<List<int>>(loOrderDateFile);
        //    List<int> loQuantity = Utils.ReadFromBinaryFile<List<int>>(loQuantityFile);
        //    List<int> loExtendedPrice = Utils.ReadFromBinaryFile<List<int>>(loExtendedPriceFile);
        //    List<int> loDiscount = Utils.ReadFromBinaryFile<List<int>>(loDiscountFile);
        //    sw.Stop();
        //    //Console.Write("I/O time: {0} ms ", sw.ElapsedMilliseconds);
        //    sw.Reset();
        //    sw.Start();
        //    foreach (var d in date)
        //    {
        //        if (d.dYear == 1993)
        //            dateHash.Add(d.dDateKey, d.dYear);
        //    }

        //    bool[] bitMap = new bool[loOrderDate.Count];
        //    var i = 0;
        //    foreach (var lo in loOrderDate)
        //    {
        //        if (dateHash.ContainsKey(lo))
        //        {
        //            bitMap[i] = true;
        //        }
        //        i++;
        //    }

        //    var dis = 0;
        //    foreach (var d in loDiscount)
        //    {
        //        if (d >= 1 && d <= 3 && bitMap[dis])
        //        {
        //            // do nothing
        //            // check to make sure the set
        //        }
        //        else
        //            bitMap[dis] = false;
        //        dis++;
        //    }

        //    var qty = 0;
        //    foreach (var d in loQuantity)
        //    {
        //        if (d < 25 && bitMap[qty])
        //        {
        //            // do nothing
        //        }
        //        else
        //            bitMap[qty] = false;
        //        qty++;
        //    }

        //    var k = 0;

        //    foreach (var bit in bitMap)
        //    {
        //        if (bit) // bit set to true
        //        {
        //            var revenue = (loExtendedPrice[k] * loDiscount[k]);
        //            totalRevenue += revenue;
        //        }
        //        k++;
        //    }
        //    sw.Stop();
        //    // Console.WriteLine("Memory operation time: {0} ms", sw.ElapsedMilliseconds);
        //    // Console.WriteLine(String.Format("[RIJoin] Revenue is : {0}, Time to loop: {1}", totalRevenue, sw.ElapsedMilliseconds));
        //}

        //private static void Q11DMRIJoinWithSmartPredicateFiltering()
        //{
        //    var dateHash = new Dictionary<int, int>();
        //    Int64 totalRevenue = 0;
        //    Stopwatch sw = Stopwatch.StartNew();
        //    List<Date> date = Utils.ReadFromBinaryFile<List<Date>>(dateFile);
        //    List<int> loOrderDate = Utils.ReadFromBinaryFile<List<int>>(loOrderDateFile);
        //    List<int> loQuantity = Utils.ReadFromBinaryFile<List<int>>(loQuantityFile);
        //    List<int> loExtendedPrice = Utils.ReadFromBinaryFile<List<int>>(loExtendedPriceFile);
        //    List<int> loDiscount = Utils.ReadFromBinaryFile<List<int>>(loDiscountFile);
        //    sw.Stop();
        //    //Console.Write("I/O time: {0} ms ", sw.ElapsedMilliseconds);
        //    sw.Reset();
        //    sw.Start();

        //    foreach (var d in date)
        //    {
        //        if (d.dYear == 1993)
        //            dateHash.Add(d.dDateKey, d.dYear);
        //    }

        //    bool[] bitMap = new bool[loOrderDate.Count];
        //    var j = 0;
        //    foreach (var lo in loOrderDate)
        //    {
        //        if (dateHash.ContainsKey(lo))
        //        {
        //            bitMap[j] = true;
        //        }
        //        j++;
        //    }

        //    var i = 0;
        //    foreach (var bit in bitMap)
        //    {
        //        if (bit)
        //        {
        //            if (loQuantity[i] < 25)
        //            {
        //                var discount = loDiscount[i];
        //                if (discount >= 1 && discount <= 3)
        //                {
        //                    // do nothing Flag was already set
        //                    var revenue = (loExtendedPrice[i] * loDiscount[i]);
        //                    totalRevenue += revenue;
        //                }
        //                else
        //                {
        //                    // bitMap[i] = false;
        //                }
        //            }
        //            else
        //            {
        //                // bitMap[i] = false;
        //            }
        //        }
        //        i++;
        //    }

        //    sw.Stop();
        //    //Console.WriteLine("Memory operation time: {0} ms", sw.ElapsedMilliseconds);

        //}

        //private static void Q12DMRIJoinWithSmartPredicateFiltering()
        //{
        //    var dateHash = new Dictionary<int, int>();
        //    Int64 totalRevenue = 0;
        //    Stopwatch sw = Stopwatch.StartNew();
        //    List<Date> date = Utils.ReadFromBinaryFile<List<Date>>(dateFile);
        //    List<int> loOrderDate = Utils.ReadFromBinaryFile<List<int>>(loOrderDateFile);
        //    List<int> loQuantity = Utils.ReadFromBinaryFile<List<int>>(loQuantityFile);
        //    List<int> loExtendedPrice = Utils.ReadFromBinaryFile<List<int>>(loExtendedPriceFile);
        //    List<int> loDiscount = Utils.ReadFromBinaryFile<List<int>>(loDiscountFile);
        //    sw.Stop();
        //    // Console.Write("I/O time: {0} ms ", sw.ElapsedMilliseconds);
        //    sw.Reset();
        //    sw.Start();

        //    foreach (var d in date)
        //    {
        //        if (d.dYearMonthNum == 199401)
        //            dateHash.Add(d.dDateKey, d.dYear);
        //    }

        //    bool[] bitMap = new bool[loOrderDate.Count];
        //    var j = 0;
        //    foreach (var lo in loOrderDate)
        //    {
        //        if (dateHash.ContainsKey(lo))
        //        {
        //            bitMap[j] = true;
        //        }
        //        j++;
        //    }

        //    var i = 0;
        //    foreach (var bit in bitMap)
        //    {
        //        if (bit)
        //        {
        //            if (loQuantity[i] >= 26 && loQuantity[i] <= 35)
        //            {
        //                var discount = loDiscount[i];
        //                if (discount >= 4 && discount <= 6)
        //                {
        //                    // do nothing Flag was already set
        //                    var revenue = (loExtendedPrice[i] * loDiscount[i]);
        //                    totalRevenue += revenue;
        //                }
        //                else
        //                {
        //                    // bitMap[i] = false;
        //                }
        //            }
        //            else
        //            {
        //                // bitMap[i] = false;
        //            }
        //        }
        //        i++;
        //    }
        //    sw.Stop();
        //    //Console.WriteLine("Memory operation time: {0} ms", sw.ElapsedMilliseconds);

        //}

        //private static void Q13DMRIJoinWithSmartPredicateFiltering()
        //{
        //    var dateHash = new Dictionary<int, int>();
        //    Int64 totalRevenue = 0;
        //    Stopwatch sw = Stopwatch.StartNew();
        //    List<Date> date = Utils.ReadFromBinaryFile<List<Date>>(dateFile);
        //    List<int> loOrderDate = Utils.ReadFromBinaryFile<List<int>>(loOrderDateFile);
        //    List<int> loQuantity = Utils.ReadFromBinaryFile<List<int>>(loQuantityFile);
        //    List<int> loExtendedPrice = Utils.ReadFromBinaryFile<List<int>>(loExtendedPriceFile);
        //    List<int> loDiscount = Utils.ReadFromBinaryFile<List<int>>(loDiscountFile);
        //    sw.Stop();
        //    //Console.Write("I/O time: {0} ms ", sw.ElapsedMilliseconds);
        //    sw.Reset();
        //    sw.Start();

        //    foreach (var d in date)
        //    {
        //        if (d.dYear == 1994 && d.dWeekNumInYear == 6)
        //            dateHash.Add(d.dDateKey, d.dYear);
        //    }

        //    bool[] bitMap = new bool[loOrderDate.Count];
        //    var j = 0;
        //    foreach (var lo in loOrderDate)
        //    {
        //        if (dateHash.ContainsKey(lo))
        //        {
        //            bitMap[j] = true;
        //        }
        //        j++;
        //    }

        //    var i = 0;
        //    foreach (var bit in bitMap)
        //    {
        //        if (bit)
        //        {
        //            if (loQuantity[i] >= 26 && loQuantity[i] <= 35)
        //            {
        //                var discount = loDiscount[i];
        //                if (discount >= 5 && discount <= 7)
        //                {
        //                    // do nothing Flag was already set
        //                    var revenue = (loExtendedPrice[i] * loDiscount[i]);
        //                    totalRevenue += revenue;
        //                }
        //                else
        //                {
        //                    // bitMap[i] = false;
        //                }
        //            }
        //            else
        //            {
        //                // bitMap[i] = false;
        //            }
        //        }
        //        i++;
        //    }
        //    sw.Stop();
        //    // Console.WriteLine("Memory operation time: {0} ms", sw.ElapsedMilliseconds);

        //}

        //private static void Q11DMPaRIUsingPartition(int maxDegreeOfParallelism)
        //{
        //    ParallelOptions parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = maxDegreeOfParallelism };
        //    Int64 totalRevenueCHP = 0;
        //    Stopwatch sw = Stopwatch.StartNew();
        //    List<Date> date = new List<Date>();
        //    List<int> loOrderDate = new List<int>();
        //    List<int> loQuantity = new List<int>();
        //    List<int> loExtendedPrice = new List<int>();
        //    List<int> loDiscount = new List<int>();

        //    Parallel.Invoke(parallelOptions,
        //        () => loOrderDate = Utils.ReadFromBinaryFile<List<int>>(loOrderDateFile),
        //        () => loQuantity = Utils.ReadFromBinaryFile<List<int>>(loQuantityFile),
        //        () => loExtendedPrice = Utils.ReadFromBinaryFile<List<int>>(loExtendedPriceFile),
        //        () => loDiscount = Utils.ReadFromBinaryFile<List<int>>(loDiscountFile),
        //        () => date = Utils.ReadFromBinaryFile<List<Date>>(dateFile)
        //        );
        //    sw.Stop();

        //    //Console.Write("I/O time: {0} ms ", sw.ElapsedMilliseconds);
        //    var dateHash = new Dictionary<int, int>();
        //    foreach (var d in date)
        //    {
        //        if (d.dYear == 1993)
        //            dateHash.Add(d.dDateKey, d.dYear);
        //    }

        //    List<bool> bitMap = Enumerable.Repeat(false, loDiscount.Count).ToList();
        //    List<List<int>> loDiscountChunks = null;
        //    List<List<int>> loOrderDateChunks = null;
        //    List<List<int>> loQuantityChunks = null;
        //    List<List<int>> loExtendedPriceChunks = null;
        //    List<List<bool>> bitMapChunks = null;

        //    Parallel.Invoke(parallelOptions,
        //        () => loDiscountChunks = loDiscount.GetPartitions<int>(),
        //        () => loOrderDateChunks = loOrderDate.GetPartitions<int>(),
        //        () => loQuantityChunks = loQuantity.GetPartitions<int>(),
        //        () => loExtendedPriceChunks = loExtendedPrice.GetPartitions<int>(),
        //        () => bitMapChunks = bitMap.GetPartitions<bool>());
        //    List<Task<int>> tasks = new List<Task<int>>();
        //    for (int i = 0; i < bitMapChunks.Count - 1; i++)
        //    {
        //        Task<int> task = Task<int>.Factory.StartNew(() => parallelTasks(i, dateHash, loOrderDateChunks[i], loDiscountChunks[i], loQuantityChunks[i], loExtendedPriceChunks[i], bitMapChunks[i]));
        //        tasks.Add(task);
        //    }

        //    Task.WaitAll(tasks.ToArray());

        //    foreach (var task in tasks)
        //    {
        //        totalRevenueCHP += task.Result;
        //    }
        //    //Console.WriteLine("[PaRI - Lock Free] Total Revenue: {0}", totalRevenueCHP);

        //}

        //private static void Q11DMPaRI(int maxDegreeOfParallelism)
        //{
        //    ParallelOptions parallelOptions = new ParallelOptions() { MaxDegreeOfParallelism = maxDegreeOfParallelism };
        //    var dateHash = new Dictionary<int, int>();
        //    Int64 totalRevenue = 0;
        //    Stopwatch sw = Stopwatch.StartNew();
        //    List<Date> date = new List<Date>();
        //    List<int> loOrderDate = new List<int>();
        //    List<int> loQuantity = new List<int>();
        //    List<int> loExtendedPrice = new List<int>();
        //    List<int> loDiscount = new List<int>();

        //    Parallel.Invoke(parallelOptions,
        //        () => loOrderDate = Utils.ReadFromBinaryFile<List<int>>(loOrderDateFile),
        //        () => loQuantity = Utils.ReadFromBinaryFile<List<int>>(loQuantityFile),
        //        () => loExtendedPrice = Utils.ReadFromBinaryFile<List<int>>(loExtendedPriceFile),
        //        () => loDiscount = Utils.ReadFromBinaryFile<List<int>>(loDiscountFile),
        //        () => date = Utils.ReadFromBinaryFile<List<Date>>(dateFile)
        //        );
        //    sw.Stop();
        //    // Console.Write("I/O time: {0} ms ", sw.ElapsedMilliseconds);
        //    sw.Reset();
        //    sw.Start();

        //    foreach (var d in date)
        //    {
        //        if (d.dYear == 1993)
        //            dateHash.Add(d.dDateKey, d.dYear);
        //    }

        //    // ConcurrentBitMap bA = new ConcurrentBitMap(loDiscount.Count);
        //    ConcurrentBitArray bA = new ConcurrentBitArray(loDiscount.Count);

        //    // ThreadSafeBitArray bM = new ThreadSafeBitArray(loDiscount.Count);
        //    corderDate(dateHash, bA, loOrderDate);

        //    Parallel.Invoke(parallelOptions, () => cdiscounts(bA, loDiscount), () => cquantity(bA, loQuantity));

        //    var bitMap = bA.GetArray();
        //    var k = 0;
        //    foreach (var bit in bitMap)
        //    {
        //        if (bit) // bit set to true
        //        {
        //            var revenue = (loExtendedPrice[k] * loDiscount[k]);
        //            totalRevenue += revenue;
        //        }
        //        k++;
        //    }

        //    // Console.WriteLine("[PaRI] Total Revenue: {0}, bitsetCount: {1}", totalRevenue);
        //}

        //private static void Q12DMPaRI(int maxDegreeOfParallelism)
        //{
        //    ParallelOptions parallelOptions = new ParallelOptions() { MaxDegreeOfParallelism = maxDegreeOfParallelism };
        //    var dateHash = new Dictionary<int, int>();
        //    Int64 totalRevenue = 0;
        //    Stopwatch sw = Stopwatch.StartNew();
        //    List<Date> date = new List<Date>();
        //    List<int> loOrderDate = new List<int>();
        //    List<int> loQuantity = new List<int>();
        //    List<int> loExtendedPrice = new List<int>();
        //    List<int> loDiscount = new List<int>();

        //    Parallel.Invoke(parallelOptions,
        //        () => loOrderDate = Utils.ReadFromBinaryFile<List<int>>(loOrderDateFile),
        //        () => loQuantity = Utils.ReadFromBinaryFile<List<int>>(loQuantityFile),
        //        () => loExtendedPrice = Utils.ReadFromBinaryFile<List<int>>(loExtendedPriceFile),
        //        () => loDiscount = Utils.ReadFromBinaryFile<List<int>>(loDiscountFile),
        //        () => date = Utils.ReadFromBinaryFile<List<Date>>(dateFile)
        //        );
        //    sw.Stop();
        //    // Console.Write("I/O time: {0} ms ", sw.ElapsedMilliseconds);
        //    sw.Reset();
        //    sw.Start();

        //    foreach (var d in date)
        //    {
        //        if (d.dYearMonthNum == 199401)
        //            dateHash.Add(d.dDateKey, d.dYear);
        //    }

        //    // ConcurrentBitMap bA = new ConcurrentBitMap(loDiscount.Count);
        //    ConcurrentBitArray bA = new ConcurrentBitArray(loDiscount.Count);

        //    // ThreadSafeBitArray bM = new ThreadSafeBitArray(loDiscount.Count);
        //    corderDate(dateHash, bA, loOrderDate);

        //    Parallel.Invoke(parallelOptions, () => Q12cdiscounts(bA, loDiscount), () => Q12cquantity(bA, loQuantity));

        //    var bitMap = bA.GetArray();
        //    var k = 0;
        //    foreach (var bit in bitMap)
        //    {
        //        if (bit) // bit set to true
        //        {
        //            var revenue = (loExtendedPrice[k] * loDiscount[k]);
        //            totalRevenue += revenue;
        //        }
        //        k++;
        //    }

        //    // Console.WriteLine("[PaRI] Total Revenue: {0}, bitsetCount: {1}", totalRevenue);
        //}

        //private static void Q13DMPaRI(int maxDegreeOfParallelism)
        //{
        //    ParallelOptions parallelOptions = new ParallelOptions() { MaxDegreeOfParallelism = maxDegreeOfParallelism };
        //    var dateHash = new Dictionary<int, int>();
        //    Int64 totalRevenue = 0;
        //    Stopwatch sw = Stopwatch.StartNew();
        //    List<Date> date = new List<Date>();
        //    List<int> loOrderDate = new List<int>();
        //    List<int> loQuantity = new List<int>();
        //    List<int> loExtendedPrice = new List<int>();
        //    List<int> loDiscount = new List<int>();

        //    Parallel.Invoke(parallelOptions,
        //        () => loOrderDate = Utils.ReadFromBinaryFile<List<int>>(loOrderDateFile),
        //        () => loQuantity = Utils.ReadFromBinaryFile<List<int>>(loQuantityFile),
        //        () => loExtendedPrice = Utils.ReadFromBinaryFile<List<int>>(loExtendedPriceFile),
        //        () => loDiscount = Utils.ReadFromBinaryFile<List<int>>(loDiscountFile),
        //        () => date = Utils.ReadFromBinaryFile<List<Date>>(dateFile)
        //        );
        //    sw.Stop();
        //    // Console.Write("I/O time: {0} ms ", sw.ElapsedMilliseconds);
        //    sw.Reset();
        //    sw.Start();

        //    foreach (var d in date)
        //    {
        //        if (d.dYear == 1994 && d.dWeekNumInYear == 6)
        //            dateHash.Add(d.dDateKey, d.dYear);
        //    }

        //    // ConcurrentBitMap bA = new ConcurrentBitMap(loDiscount.Count);
        //    ConcurrentBitArray bA = new ConcurrentBitArray(loDiscount.Count);

        //    // ThreadSafeBitArray bM = new ThreadSafeBitArray(loDiscount.Count);
        //    corderDate(dateHash, bA, loOrderDate);

        //    Parallel.Invoke(parallelOptions, () => Q13cdiscounts(bA, loDiscount), () => Q13cquantity(bA, loQuantity));

        //    var bitMap = bA.GetArray();
        //    var k = 0;
        //    foreach (var bit in bitMap)
        //    {
        //        if (bit) // bit set to true
        //        {
        //            var revenue = (loExtendedPrice[k] * loDiscount[k]);
        //            totalRevenue += revenue;
        //        }
        //        k++;
        //    }

        //    // Console.WriteLine("[PaRI] Total Revenue: {0}, bitsetCount: {1}", totalRevenue);
        //}

        //static void Main(string[] args)
        //{

        //    Algorithms ag = new Algorithms();
        //    // Use if Needed to Create Binary Files Again
        //    Console.WriteLine("Loading Columns...");
        //    ag.loadColumns();
        //    Console.WriteLine("Loading Columns Completed.");
        //    Console.WriteLine("Loading Tables...");
        //    ag.loadTables();
        //    Console.WriteLine("Loading Tables Completed.");
        //    ag.createBinaryFiles();
        //    //ag.ParallelXYZJoin();
        //    // ag.InvisibleJoin();
        //    Console.WriteLine();
        //    //ag.ParallelInvisibleJoin();
        //    // ag.XYZJoin();
        //    // Console.WriteLine();
        //    //ag.XYZJoin1();
        //    //ag.XYZJoinNewTry();
        //    //Console.ReadKey();
        //}
        //var stopWatch = Stopwatch.StartNew();
        ////loadColumns();
        ////loadTables();
        //createBinaryFiles();
        //stopWatch.Stop();
        //Console.WriteLine(String.Format("Load Time: {0} secs", stopWatch.Elapsed.Seconds));
        //string[] results = new string[50];
        //for (int i = 0; i < 10; i++)
        //{
        //    Console.WriteLine("======================================================================");
        //    Stopwatch sw = Stopwatch.StartNew();
        //    Q11DMSerialInvisibleJoin();
        //    sw.Stop();
        //    long a = sw.ElapsedMilliseconds;
        //    Console.WriteLine(String.Format("Q11DMSerialInvisibleJoin: {0} ms ", a));

        //    sw.Reset();
        //    sw.Start();
        //    Q12DMSerialInvisibleJoin();
        //    sw.Stop();
        //    long b = sw.ElapsedMilliseconds;
        //    Console.Write(String.Format("Q12DMSerialInvisibleJoin: {0} ms ", b));

        //    sw.Reset();
        //    sw.Start();
        //    Q13DMSerialInvisibleJoin();
        //    sw.Stop();
        //    long c = sw.ElapsedMilliseconds;
        //    Console.Write(String.Format("Q13DMSerialInvisibleJoin: {0} ms ", c));

        //    sw.Reset();
        //    sw.Start();
        //    Q11DMParallelInvisibleJoin(4);
        //    sw.Stop();
        //    long d = sw.ElapsedMilliseconds;
        //    Console.WriteLine(String.Format("Q11DMParallelInvisibleJoin: {0} ms ", d));

        //    sw.Reset();
        //    sw.Start();
        //    Q12DMParallelInvisibleJoin(4);
        //    sw.Stop();
        //    long e = sw.ElapsedMilliseconds;
        //    Console.Write(String.Format("Q12DMParallelInvisibleJoin: {0} ms ", e));

        //    sw.Reset();
        //    sw.Start();
        //    Q13DMParallelInvisibleJoin(4);
        //    sw.Stop();
        //    long f = sw.ElapsedMilliseconds;
        //    Console.Write(String.Format("Q13DMParallelInvisibleJoin: {0} ms ", f));


        //    sw.Reset();
        //    sw.Start();
        //    Q11DMRIJoinWithSmartPredicateFiltering();
        //    sw.Stop();
        //    long g = sw.ElapsedMilliseconds;
        //    Console.WriteLine(String.Format("Q11DMRIJoinWithSmartPredicateFiltering: {0} ms ", g));

        //    sw.Reset();
        //    sw.Start();
        //    Q12DMRIJoinWithSmartPredicateFiltering();
        //    sw.Stop();
        //    long h = sw.ElapsedMilliseconds;
        //    Console.Write(String.Format("Q12DMRIJoinWithSmartPredicateFiltering: {0} ms ", h));

        //    sw.Reset();
        //    sw.Start();
        //    Q13DMRIJoinWithSmartPredicateFiltering();
        //    sw.Stop();
        //    long j = sw.ElapsedMilliseconds;
        //    Console.Write(String.Format("Q13DMRIJoinWithSmartPredicateFiltering: {0} ms ", j));

        //    sw.Reset();
        //    sw.Start();
        //    Q11DMPaRI(4);
        //    sw.Stop();
        //    long k = sw.ElapsedMilliseconds;
        //    Console.WriteLine(String.Format("Q11DMPaRI: {0} ms ", k));

        //    sw.Reset();
        //    sw.Start();
        //    Q12DMPaRI(4);
        //    sw.Stop();
        //    long l = sw.ElapsedMilliseconds;
        //    Console.Write(String.Format("Q12DMPaRI: {0} ms ", l));

        //    sw.Reset();
        //    sw.Start();
        //    Q13DMPaRI(4);
        //    sw.Stop();
        //    long m = sw.ElapsedMilliseconds;
        //    Console.Write(String.Format("Q13DMPaRI: {0} ms ", m));

        //    //sw.Reset();
        //    //sw.Start();
        //    //Q11DMPaRI(4);
        //    //sw.Stop();
        //    //long h = sw.ElapsedMilliseconds;
        //    //Console.WriteLine(String.Format("Q11DMPaRI: {0} ms ", h));



        //    //results[i] = String.Format("{0},{1},{2},{3},{4},{5},{6},{7}", i + 1, a, b, c, d, e, f, g);
        //}
        //// File.WriteAllLines(@"C:\Users\psangats\Google Drive\Study\0190 Doctor of Philosophy\Data\sf1_ff0_01945_op116883_All_Algorithm_Run2.txt", results);
        ////#region joins
        ////string[] results = new string[50];
        ////for (int i = 0; i < 10; i++)
        ////{
        ////    Stopwatch sw = Stopwatch.StartNew();
        ////    selectAllColumnBlockedJoinUsingEarlyMaterialization();
        ////    //selectAllBlockProcessingHashJoin();
        ////    //columnHashJoinQ11();
        ////    sw.Stop();
        ////    Console.WriteLine(String.Format("PaRI Join: {0} ", sw.ElapsedMilliseconds));
        ////    long a = sw.ElapsedMilliseconds;

        ////    sw.Reset();
        ////    sw.Start();
        ////    //parallelColumnHashJoinQ11();
        ////    //selectAllParallelBlockProcessingHashJoin();
        ////    //selectAllColumnJoinUsingLateMaterailization();
        ////    sw.Stop();
        ////    Console.WriteLine(String.Format("Parallel Invisible Join: {0} ", sw.ElapsedMilliseconds));
        ////    //Console.Write(String.Format("RCO: {0} ", sw.ElapsedMilliseconds));
        ////    long b = sw.ElapsedMilliseconds;

        ////    sw.Reset();
        ////    sw.Start();
        ////    //selectAllColumnParallelBlockedJoinUsingEarlyMaterialization();
        ////    //improvedColumnHashJoinQ11();
        ////    // selectAllBlockProcessingHashJoin();
        ////    sw.Stop();
        ////    Console.WriteLine(String.Format("RIJoin (Smart Predicate Filtering): {0} ", sw.ElapsedMilliseconds));
        ////    // Console.Write(String.Format("PCO: {0} ", sw.ElapsedMilliseconds));
        ////    long c = sw.ElapsedMilliseconds;

        ////    sw.Reset();
        ////    sw.Start();
        ////    improvedColumnHashJoinQ11WithSmartPredicateFiltering();
        ////    //parallelImprovedColumnHashJoinQ11();
        ////    //selectedColumnBlockedJoinUsingLateMaterailization();
        ////    sw.Stop();
        ////    Console.WriteLine(String.Format("RIJoin: {0} ", sw.ElapsedMilliseconds));
        ////    long d = sw.ElapsedMilliseconds;


        ////    sw.Reset();
        ////    sw.Start();
        ////    //parallelImprovedColumnHashJoinQ11();
        ////    //parallelImprovedColumnHashJoinQ11UsingPartition();
        ////    //selectedColumnBlockedJoinUsingLateMaterailization();
        ////    sw.Stop();
        ////    Console.WriteLine(String.Format("Invisible: {0} ", sw.ElapsedMilliseconds));
        ////    long e = sw.ElapsedMilliseconds;

        ////    Console.WriteLine();
        ////    //Console.WriteLine(String.Format("Iteration: {0}, RJT: {1}, CJT: {2}, ICJT: {3} ms", i, ((eRJoinTime - sRJoinTime).Milliseconds), ((eCJoinTime - sCJoinTime).Milliseconds), ((eICJoinTime - sICJoinTime).Milliseconds)));
        ////    // Console.WriteLine(String.Format("Iteration: {0}, RowHash: {1}, EarlyM: {2}, LateM: {3} ms", i, ((eRJoinTime - sRJoinTime).Milliseconds), ((eCJoinTime - sCJoinTime).Milliseconds), ((eICJoinTime - sICJoinTime).Milliseconds)));
        ////    results[i] = String.Format("{0}, {1}, {2}, {3}, {4}, {5}", i + 1, a, b, c, d, e);
        ////}
        //////File.WriteAllLines(@"C:\Users\psangats\Google Drive\Study\0190 Doctor of Philosophy\Data\sf1_ff0_01945_op116883_All_Algorithm.txt", results);
        ////#endregion
        //Console.ReadKey();

        //}

        //private static void cdiscounts(ConcurrentBitMap bA)
        //{
        //    for (int i = 0; i < loDiscount.Count; i++)
        //    {
        //        bool condition = bA.Get(i);
        //        var attributeValue = loDiscount[i];
        //        bool setCondition = attributeValue >= 1 && attributeValue <= 3 ? true : false;
        //        bA.Add(i, setCondition);
        //    }
        //}

        //private static void cquantity(ConcurrentBitMap bA)
        //{
        //    for (int i = 0; i < loQuantity.Count; i++)
        //    {
        //        bool condition = bA.Get(i);
        //        var attributeValue = loQuantity[i];
        //        bool setCondition = attributeValue < 25 ? true : false;
        //        bA.Add(i, setCondition);
        //    }
        //}

        //private static void corderDate(Dictionary<int, int> dateHash, ConcurrentBitMap bA)
        //{
        //    for (int i = 0; i < loOrderDate.Count; i++)
        //    {
        //        bool condition = bA.Get(i);
        //        var attributeValue = loOrderDate[i];
        //        bool setCondition = dateHash.ContainsKey(attributeValue);
        //        bA.Add(i, setCondition);
        //    }
        //}

        //private static void cdiscounts(ThreadSafeBitArray bA)
        //{
        //    var dis = 0;
        //    foreach (var d in loDiscount)
        //    {
        //        //if (d >= 1 && d <= 3)
        //        //{
        //        //    if (bA.Get(dis).Item1 == 0)
        //        //        bA.AddorUpdate(dis, new Tuple<int, bool>(1, true));
        //        //    else if (bA.Get(dis).Item2 == true)
        //        //        bA.AddorUpdate(dis, new Tuple<int, bool>(1, true));
        //        //}

        //        if (d >= 1 && d <= 3 && bA[dis])
        //        {
        //        }
        //        else
        //            bA[dis] = false;
        //        dis++;
        //    }
        //}

        //private static void cquantity(ThreadSafeBitArray bA)
        //{
        //    var qty = 0;
        //    foreach (var d in loQuantity)
        //    {
        //        //if (d < 25)
        //        //{
        //        //    if (bA.Get(qty).Item1 == 0)
        //        //        bA.AddorUpdate(qty, new Tuple<int, bool>(1, true));
        //        //    else if (bA.Get(qty).Item2 == true)
        //        //        bA.AddorUpdate(qty, new Tuple<int, bool>(1, true));
        //        //}

        //        if (d < 25 && bA[qty])
        //        {
        //        }
        //        else
        //            bA[qty] = false;
        //        qty++;
        //    }
        //}

        //private static void corderDate(ConcurrentDictionary<int, int> dateHash, ThreadSafeBitArray bA)
        //{
        //    var i = 0;
        //    foreach (var lo in loOrderDate)
        //    {
        //        if (dateHash.ContainsKey(lo))
        //        {
        //            //if (bA.Get(i).Item1 == 0)
        //            //    bA.AddorUpdate(i, new Tuple<int, bool>(1, true));
        //            //else if (bA.Get(i).Item2 == true)
        //            //    bA.AddorUpdate(i, new Tuple<int, bool>(1, true));
        //            bA[i] = true;
        //        }
        //        i++;
        //    }
        //}

        //private static void cdiscounts(ConcurrentBitArray bA, List<int> loDiscount)
        //{
        //    var dis = 0;
        //    foreach (var d in loDiscount)
        //    {
        //        if (d >= 1 && d <= 3 && bA.Get(dis))
        //        {

        //        }
        //        else
        //            bA.AddorUpdate(dis, false);
        //        dis++;
        //    }
        //}

        //private static void Q12cdiscounts(ConcurrentBitArray bA, List<int> loDiscount)
        //{
        //    var dis = 0;
        //    foreach (var d in loDiscount)
        //    {
        //        if (d >= 4 && d <= 6 && bA.Get(dis))
        //        {

        //        }
        //        else
        //            bA.AddorUpdate(dis, false);
        //        dis++;
        //    }
        //}

        //private static void Q13cdiscounts(ConcurrentBitArray bA, List<int> loDiscount)
        //{
        //    var dis = 0;
        //    foreach (var d in loDiscount)
        //    {
        //        if (d >= 5 && d <= 7 && bA.Get(dis))
        //        {

        //        }
        //        else
        //            bA.AddorUpdate(dis, false);
        //        dis++;
        //    }
        //}

        //private static void cquantity(ConcurrentBitArray bA, List<int> loQuantity)
        //{
        //    var dis = 0;
        //    foreach (var d in loQuantity)
        //    {
        //        if (d < 25 && bA.Get(dis))
        //        {

        //        }
        //        else
        //            bA.AddorUpdate(dis, false);
        //        dis++;
        //    }
        //}

        //private static void Q12cquantity(ConcurrentBitArray bA, List<int> loQuantity)
        //{
        //    var i = 0;
        //    foreach (var d in loQuantity)
        //    {
        //        if (d >= 26 && d <= 35 && bA.Get(i))
        //        {

        //        }
        //        else
        //            bA.AddorUpdate(i, false);
        //        i++;
        //    }
        //}

        //private static void Q13cquantity(ConcurrentBitArray bA, List<int> loQuantity)
        //{
        //    var i = 0;
        //    foreach (var d in loQuantity)
        //    {
        //        if (d >= 26 && d <= 35 && bA.Get(i))
        //        {

        //        }
        //        else
        //            bA.AddorUpdate(i, false);
        //        i++;
        //    }
        //}

        //private static void corderDate(Dictionary<int, int> dateHash, ConcurrentBitArray bA, List<int> loOrderDate)
        //{
        //    var dis = 0;
        //    foreach (var lo in loOrderDate)
        //    {
        //        if (dateHash.ContainsKey(lo))
        //        {
        //            bA.AddorUpdate(dis, true);
        //        }
        //        dis++;
        //    }
        //}

        //private static BitArray discounts()
        //{
        //    BitArray baDis = new BitArray(loDiscount.Count);
        //    var dis = 0;
        //    foreach (var d in loDiscount)
        //    {
        //        if (d >= 1 && d <= 3)
        //            baDis.Set(dis, true);
        //        dis++;
        //    }
        //    return baDis;
        //}

        //private static BitArray quantity()
        //{
        //    BitArray baQty = new BitArray(loQuantity.Count);
        //    var qty = 0;
        //    foreach (var d in loQuantity)
        //    {
        //        if (d < 25)
        //            baQty.Set(qty, true);
        //        qty++;
        //    }

        //    return baQty;
        //}

        //private static BitArray orderDate(Dictionary<int, int> dateHash)
        //{
        //    BitArray baOD = new BitArray(loOrderDate.Count);
        //    var i = 0;
        //    foreach (var lo in loOrderDate)
        //    {
        //        if (dateHash.ContainsKey(lo))
        //        {
        //            baOD.Set(i, true);
        //        }
        //        i++;
        //    }
        //    return baOD;
        //}
    }
}
