using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParallelHashJoins
{
    class Algorithms
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

        private static string binaryFilesDirectory = @"C:\Raw_Data_Source_For_Test\SSBM - DBGEN\binaryFilesForXYZJoin";
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

        public void XYZJoin()
        {
            try
            {
                Int64 totalRevenue = 0;
                var dateHashTable = new Dictionary<int, int>();
                var customerHashTable = new Dictionary<int, string>();
                var supplierHashTable = new Dictionary<int, string>();

                #region Key Hashing Phase
                List<Date> dateDimension = Utils.ReadFromBinaryFile<List<Date>>(dateFile);
                foreach (var row in dateDimension)
                {
                    if (row.dYear >= 1992 && row.dYear <= 1997)
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
                #endregion Key Hashing Phase

                #region Probing Phase
                List<int> loOrderDate = Utils.ReadFromBinaryFile<List<int>>(loOrderDateFile);
                var matchedOrderDate = new Dictionary<int, int>();
                var i = 0;
                foreach (var orderDate in loOrderDate)
                {
                    if (dateHashTable.ContainsKey(orderDate))
                    {
                        int dYear = -1;
                        dateHashTable.TryGetValue(orderDate, out dYear);
                        matchedOrderDate.Add(i + 1, dYear);
                    }
                    i++;
                }
                loOrderDate.Clear();


                List<int> loCustomerKey = Utils.ReadFromBinaryFile<List<int>>(loCustKeyFile);
                var matchedCustomerKey = new Dictionary<int, string>();
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

                List<int> loSupplierKey = Utils.ReadFromBinaryFile<List<int>>(loSuppKeyFile);
                var matchedSupplierKey = new Dictionary<int, string>();
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

                var joinOutputIntermediate1 = matchedSupplierKey.Where(x => matchedCustomerKey.ContainsKey(x.Key)).ToDictionary(x => x.Key, x => x.Value + "," + matchedCustomerKey[x.Key]);

                var joinOutputIntermediate2 = joinOutputIntermediate1.Where(x => matchedOrderDate.ContainsKey(x.Key))
                    .ToDictionary(x => x.Key, x => x.Value + "," + matchedOrderDate[x.Key]);
                #endregion Probing Phase

                #region Value Extraction Phase
                List<int> loRevenue = Utils.ReadFromBinaryFile<List<int>>(loRevenueFile);

                var joinOutputFinal = new Dictionary<int, string>();
                foreach (var item in joinOutputIntermediate2)
                {
                    joinOutputFinal.Add(item.Key, item.Value + "," + loRevenue[item.Key]);

                }
                #endregion Value Extraction Phase

                Console.WriteLine("Total Rows: " + joinOutputFinal.Count);

            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void InvisibleJoin()
        {
            try
            {
                Int64 totalRevenue = 0;
                var dateHashTable = new Dictionary<int, int>();
                var customerHashTable = new Dictionary<int, string>();
                var supplierHashTable = new Dictionary<int, string>();

                #region Key Hashing Phase
                List<Date> dateDimension = Utils.ReadFromBinaryFile<List<Date>>(dateFile);
                foreach (var row in dateDimension)
                {
                    if (row.dYear >= 1992 && row.dYear <= 1997)
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
                #endregion Key Hashing Phase

                #region Probing Phase
                List<int> loOrderDate = Utils.ReadFromBinaryFile<List<int>>(loOrderDateFile);

                var arraySize = loOrderDate.Count;
                BitArray baOrderDate = new BitArray(arraySize);
                var i = 0;
                foreach (var orderDate in loOrderDate)
                {
                    if (dateHashTable.ContainsKey(orderDate))
                        baOrderDate.Set(i, true);
                    i++;
                }
                loOrderDate.Clear();


                List<int> loCustomerKey = Utils.ReadFromBinaryFile<List<int>>(loCustKeyFile);
                BitArray baCustomerKey = new BitArray(arraySize);
                var j = 0;
                foreach (var custKey in loCustomerKey)
                {
                    if (customerHashTable.ContainsKey(custKey))
                        baCustomerKey.Set(j, true);
                    j++;
                }
                loCustomerKey.Clear();

                List<int> loSupplierKey = Utils.ReadFromBinaryFile<List<int>>(loSuppKeyFile);
                BitArray baSupplierKey = new BitArray(arraySize);
                var k = 0;
                foreach (var suppKey in loSupplierKey)
                {
                    if (supplierHashTable.ContainsKey(suppKey))
                        baSupplierKey.Set(k, true);
                    k++;
                }
                loSupplierKey.Clear();

                baCustomerKey.And(baSupplierKey);
                baCustomerKey.And(baOrderDate);
                #endregion Probing Phase

                #region Value Extraction Phase
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
                        int dYear;
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
                #endregion Value Extraction Phase
                Console.WriteLine("Total Rows: " + joinOutputFinal.Count);
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
                            Convert.ToInt32(data[4]), Convert.ToInt32(data[5]), data[6], Convert.ToInt32(data[7]),
                            Convert.ToInt32(data[8]), Convert.ToInt32(data[9]), Convert.ToInt32(data[10]), Convert.ToInt32(data[11]),
                            data[12], Convert.ToInt32(data[13]), Convert.ToInt32(data[14]), Convert.ToInt32(data[15]), Convert.ToInt32(data[16])));
                    }
                }
                //else if (fileName.Equals("lineorder"))
                //{
                //    foreach (var line in allLines)
                //    {
                //        var data = line.Split('|');
                //        lineOrder.Add(new LineOrder(Convert.ToInt32(data[0]),
                //            Convert.ToInt16(data[1]),
                //            Convert.ToInt32(data[2]),
                //            Convert.ToInt32(data[3]),
                //            Convert.ToInt16(data[4]),
                //            Convert.ToInt32(data[5]),
                //            data[6],
                //            Convert.ToChar(data[7]),
                //            Convert.ToInt16(data[8]),
                //            Convert.ToInt32(data[9]),
                //            Convert.ToInt32(data[10]),
                //            Convert.ToInt16(data[11]),
                //            Convert.ToInt32(data[12]),
                //            Convert.ToInt32(data[13]),
                //            Convert.ToInt16(data[14]),
                //            Convert.ToInt32(data[15]),
                //            data[16]));
                //    }
                //}
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
