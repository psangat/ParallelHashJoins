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
    internal class BinaryFileCreator
    {
        private readonly string folderPath = @"C:\Raw_Data_Source_For_Test\SSBM - DBGEN\SF 100";
        private static readonly string binaryFilesDirectory = @"C:\Raw_Data_Source_For_Test\SSBM - DBGEN\BFSF 100";
        private readonly string scaleFactor = "";
        private readonly ParallelOptions parallelOptions = null;


        public BinaryFileCreator(string scaleFactor, int degreeOfParallelism = 12)
        {
            this.scaleFactor = scaleFactor;
            parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = degreeOfParallelism };
        }

        #region Private Variables
        private readonly List<long> cCustKey = new List<long>();
        private readonly List<string> cName = new List<string>();
        private readonly List<string> cAddress = new List<string>();
        private readonly List<string> cCity = new List<string>();
        private readonly List<string> cNation = new List<string>();
        private readonly List<string> cRegion = new List<string>();
        private readonly List<string> cPhone = new List<string>();
        private readonly List<string> cMktSegment = new List<string>();

        private readonly List<long> sSuppKey = new List<long>();
        private readonly List<string> sName = new List<string>();
        private readonly List<string> sAddress = new List<string>();
        private readonly List<string> sCity = new List<string>();
        private readonly List<string> sNation = new List<string>();
        private readonly List<string> sRegion = new List<string>();
        private readonly List<string> sPhone = new List<string>();

        private readonly List<long> pSize = new List<long>();
        private readonly List<long> pPartKey = new List<long>();
        private readonly List<string> pName = new List<string>();
        private readonly List<string> pMFGR = new List<string>();
        private readonly List<string> pCategory = new List<string>();
        private readonly List<string> pBrand = new List<string>();
        private readonly List<string> pColor = new List<string>();
        private readonly List<string> pType = new List<string>();
        private readonly List<string> pContainer = new List<string>();

        private readonly List<long> loOrderKey = new List<long>();
        private readonly List<long> loLineNumber = new List<long>();
        private List<long> loCustKey = new List<long>();
        private List<long> loPartKey = new List<long>();
        private List<long> loSuppKey = new List<long>();
        private List<long> loOrderDate = new List<long>();
        private readonly List<char> loShipPriority = new List<char>();
        private readonly List<long> loQuantity = new List<long>();
        private readonly List<Tuple<long, long>> loQuantityWithId = new List<Tuple<long, long>>();

        private readonly List<long> loExtendedPrice = new List<long>();
        private readonly List<long> loOrdTotalPrice = new List<long>();
        private readonly List<long> loDiscount = new List<long>();
        private readonly List<Tuple<long, long>> loDiscountWithId = new List<Tuple<long, long>>();
        private List<long> loRevenue = new List<long>();
        private List<long> loSupplyCost = new List<long>();
        private readonly List<long> loTax = new List<long>();
        private readonly List<long> loCommitDate = new List<long>();
        private readonly List<string> loShipMode = new List<string>();
        private readonly List<string> loOrderPriority = new List<string>();

        private readonly List<long> dDateKey = new List<long>();
        private readonly List<long> dYear = new List<long>();
        private readonly List<long> dYearMonthNum = new List<long>();
        private readonly List<long> dDayNumInWeek = new List<long>();
        private readonly List<long> dDayNumInMonth = new List<long>();
        private readonly List<long> dDayNumInYear = new List<long>();
        private readonly List<long> dMonthNumInYear = new List<long>();
        private readonly List<long> dWeekNumInYear = new List<long>();
        private readonly List<long> dLastDayInWeekFL = new List<long>();
        private readonly List<long> dLastDayInMonthFL = new List<long>();
        private readonly List<long> dHolidayFL = new List<long>();
        private readonly List<long> dWeekDayFL = new List<long>();
        private readonly List<string> dDate = new List<string>();
        private readonly List<string> dDayOfWeek = new List<string>();
        private readonly List<string> dMonth = new List<string>();
        private readonly List<string> dYearMonth = new List<string>();
        private readonly List<string> dSellingSeason = new List<string>();

        private List<Customer> customer = new List<Customer>();
        private List<Supplier> supplier = new List<Supplier>();
        private List<Part> part = new List<Part>();
        private readonly List<LineOrder> lineOrder = new List<LineOrder>();
        private List<Date> date = new List<Date>();


        private readonly string dateFile = binaryFilesDirectory + @"\date\";
        private readonly string customerFile = binaryFilesDirectory + @"\customer\";
        private readonly string supplierFile = binaryFilesDirectory + @"\supplier\";
        private readonly string partFile = binaryFilesDirectory + @"\part\";

        private readonly string dDateKeyFile = binaryFilesDirectory + @"\dDateKey\";
        private readonly string dYearFile = binaryFilesDirectory + @"\dYear\";
        private readonly string dYearMonthNumFile = binaryFilesDirectory + @"\dYearMonthNum\";
        private readonly string dDayNumInWeekFile = binaryFilesDirectory + @"\dDayNumInWeek\";
        private readonly string dDayNumInMonthFile = binaryFilesDirectory + @"\dDayNumInMont\";
        private readonly string dDayNumInYearFile = binaryFilesDirectory + @"\dDayNumInYear\";
        private readonly string dMonthNumInYearFile = binaryFilesDirectory + @"\dMonthNumInYear\";
        private readonly string dWeekNumInYearFile = binaryFilesDirectory + @"\dWeekNumInYear\";
        private readonly string dLastDayInWeekFLFile = binaryFilesDirectory + @"\dLastDayInWeekFL\";
        private readonly string dLastDayInMonthFLFile = binaryFilesDirectory + @"\dLastDayInMonthFL\";
        private readonly string dHolidayFLFile = binaryFilesDirectory + @"\dHolidayFL\";
        private readonly string dWeekDayFLFile = binaryFilesDirectory + @"\dWeekDayFL\";
        private readonly string dDateFile = binaryFilesDirectory + @"\dDate\";
        private readonly string dDayOfWeekFile = binaryFilesDirectory + @"\dDayOfWeek\";
        private readonly string dMonthFile = binaryFilesDirectory + @"\dMonth\";
        private readonly string dYearMonthFile = binaryFilesDirectory + @"\dYearMonth\";
        private readonly string dSellingSeasonFile = binaryFilesDirectory + @"\dSellingSeason\";

        private readonly string loOrderKeyFile = binaryFilesDirectory + @"\loOrderKey\";
        private readonly string loLineNumberFile = binaryFilesDirectory + @"\loLineNumber\";
        private readonly string loCustKeyFile = binaryFilesDirectory + @"\loCustKey\";
        private readonly string loPartKeyFile = binaryFilesDirectory + @"\loPartKey\";
        private readonly string loSuppKeyFile = binaryFilesDirectory + @"\loSuppKey\";
        private readonly string loOrderDateFile = binaryFilesDirectory + @"\loOrderDate\";
        private readonly string loShipPriorityFile = binaryFilesDirectory + @"\loShipPriority\";
        private readonly string loQuantityFile = binaryFilesDirectory + @"\loQuantity\";
        private readonly string loExtendedPriceFile = binaryFilesDirectory + @"\loExtendedPrice\";
        private readonly string loOrdTotalPriceFile = binaryFilesDirectory + @"\loOrdTotalPrice\";
        private readonly string loDiscountFile = binaryFilesDirectory + @"\loDiscount\";
        private readonly string loRevenueFile = binaryFilesDirectory + @"\loRevenue\";
        private readonly string loSupplyCostFile = binaryFilesDirectory + @"\loSupplyCost\";
        private readonly string loTaxFile = binaryFilesDirectory + @"\loTax\";
        private readonly string loCommitDateFile = binaryFilesDirectory + @"\loCommitDate\";
        private readonly string loShipModeFile = binaryFilesDirectory + @"\loShipMode\";
        private readonly string loOrderPriorityFile = binaryFilesDirectory + @"\loOrderPriority\";

        private readonly string cCustKeyFile = binaryFilesDirectory + @"\cCustKey\";
        private readonly string cNameFile = binaryFilesDirectory + @"\cName\";
        private readonly string cAddressFile = binaryFilesDirectory + @"\cAddress\";
        private readonly string cCityFile = binaryFilesDirectory + @"\cCity\";
        private readonly string cNationFile = binaryFilesDirectory + @"\cNation\";
        private readonly string cRegionFile = binaryFilesDirectory + @"\cRegion\";
        private readonly string cPhoneFile = binaryFilesDirectory + @"\cPhone\";
        private readonly string cMktSegmentFile = binaryFilesDirectory + @"\cMktSegment\";

        private readonly string sSuppKeyFile = binaryFilesDirectory + @"\sSuppKey\";
        private readonly string sNameFile = binaryFilesDirectory + @"\sName\";
        private readonly string sAddressFile = binaryFilesDirectory + @"\sAddress\";
        private readonly string sCityFile = binaryFilesDirectory + @"\sCity\";
        private readonly string sNationFile = binaryFilesDirectory + @"\sNation\";
        private readonly string sRegionFile = binaryFilesDirectory + @"\sRegion\";
        private readonly string sPhoneFile = binaryFilesDirectory + @"\sPhone\";

        private readonly string pSizeFile = binaryFilesDirectory + @"\pSize\";
        private readonly string pPartKeyFile = binaryFilesDirectory + @"\pPartKey\";
        private readonly string pNameFile = binaryFilesDirectory + @"\pName\";
        private readonly string pMFGRFile = binaryFilesDirectory + @"\pMFGR\";
        private readonly string pCategoryFile = binaryFilesDirectory + @"\pCategory\";
        private readonly string pBrandFile = binaryFilesDirectory + @"\pBrand\";
        private readonly string pColorFile = binaryFilesDirectory + @"\pColor\";
        private readonly string pTypeFile = binaryFilesDirectory + @"\pType\";
        private readonly string pContainerFile = binaryFilesDirectory + @"\pContainer\";

        private readonly bool isFirst = true;
        private readonly int chunkQuantity = 10000;

        #endregion Private Variables



        public void CreateBinaryFiles()
        {
            List<Task> tasks = new List<Task>();
            foreach (string file in Directory.EnumerateFiles(folderPath, "*.tbl"))
            {
                string fileName = Path.GetFileNameWithoutExtension(file);
                Task t = null;
                switch (fileName)
                {
                    case "customer":
                        t = Task.Factory.StartNew(() =>
                        {
                            int count = 0;
                            int name = 0;
                            foreach (string line in File.ReadLines(file))
                            {
                                string[] data = line.Split('|');
                                customer.Add(new Customer(Convert.ToInt64(data[0]), data[1], data[2], data[3], data[4], data[5], data[6], data[7]));
                                if (count == chunkQuantity)
                                {
                                    Utils.WriteToBinaryFile<List<Customer>>(customerFile, Path.Combine(customerFile, string.Format("{0}.bin", name)), customer, false);
                                    Console.WriteLine(string.Format("Customer dimension chunk #{0} created.", name));
                                    name++;
                                    customer.Clear();
                                    count = 0;
                                }
                                count++;
                            }
                            Utils.WriteToBinaryFile<List<Customer>>(customerFile, Path.Combine(customerFile, string.Format("{0}.bin", name)), customer, false);
                            Console.WriteLine(string.Format("Customer dimension chunk #{0} created.", name));
                        });
                        tasks.Add(t);
                        break;
                    case "supplier":
                        t = Task.Factory.StartNew(() =>
                        {
                            int count = 0;
                            int name = 0;
                            foreach (string line in File.ReadLines(file))
                            {
                                string[] data = line.Split('|');
                                supplier.Add(new Supplier(Convert.ToInt64(data[0]), data[1], data[2], data[3], data[4], data[5], data[6]));
                                if (count == chunkQuantity)
                                {
                                    Utils.WriteToBinaryFile<List<Supplier>>(supplierFile, Path.Combine(supplierFile, string.Format("{0}.bin", name)), supplier, false);
                                    Console.WriteLine(string.Format("Supplier dimension chunk #{0} created.", name));
                                    name++;
                                    supplier.Clear();
                                    count = 0;
                                }
                                count++;
                            }
                            Utils.WriteToBinaryFile<List<Supplier>>(supplierFile, Path.Combine(supplierFile, string.Format("{0}.bin", name)), supplier, false);
                            Console.WriteLine(string.Format("Supplier dimension chunk #{0} created.", name));
                        });
                        tasks.Add(t);
                        break;
                    case "part":
                        t = Task.Factory.StartNew(() =>
                        {
                            int count = 0;
                            int name = 0;
                            foreach (string line in File.ReadLines(file))
                            {
                                string[] data = line.Split('|');
                                part.Add(new Part(Convert.ToInt64(data[0]), data[1], data[2], data[3], data[4], data[5], data[6], Convert.ToInt64(data[7]), data[8]));
                                if (count == chunkQuantity)
                                {
                                    Utils.WriteToBinaryFile<List<Part>>(partFile, Path.Combine(partFile, string.Format("{0}.bin", name)), part, false);
                                    Console.WriteLine(string.Format("Part dimension chunk #{0} created.", name));
                                    name++;
                                    part.Clear();
                                    count = 0;
                                }
                                count++;
                            }
                            Utils.WriteToBinaryFile<List<Part>>(partFile, Path.Combine(partFile, string.Format("{0}.bin", name)), part, false);
                            Console.WriteLine(string.Format("Part dimension chunk #{0} created.", name));
                        });
                        tasks.Add(t);

                        break;
                    case "date":
                        t = Task.Factory.StartNew(() =>
                        {
                            int count = 0;
                            int name = 0;
                            foreach (string line in File.ReadLines(file))
                            {
                                string[] data = line.Split('|');
                                date.Add(new Date(Convert.ToInt64(data[0]), data[1], data[2], data[3],
                            data[4], Convert.ToInt64(data[5]), data[6], Convert.ToInt64(data[7]),
                            Convert.ToInt64(data[8]), Convert.ToInt64(data[9]), Convert.ToInt64(data[10]), Convert.ToInt64(data[11]),
                            data[12], Convert.ToInt64(data[13]), Convert.ToInt64(data[14]), Convert.ToInt64(data[15]), Convert.ToInt64(data[16])));
                                if (count == chunkQuantity)
                                {
                                    Utils.WriteToBinaryFile<List<Date>>(dateFile, Path.Combine(dateFile, string.Format("{0}.bin", name)), date, false);
                                    Console.WriteLine(string.Format("Date dimension chunk #{0} created.", name));
                                    name++;
                                    date.Clear();
                                    count = 0;
                                }
                                count++;
                            }
                            Utils.WriteToBinaryFile<List<Date>>(dateFile, Path.Combine(dateFile, string.Format("{0}.bin", name)), date, false);
                            Console.WriteLine(string.Format("Date dimension chunk #{0} created.", name));
                        });
                        tasks.Add(t);

                        break;
                    case "lineorder":
                        t = Task.Factory.StartNew(() =>
                        {
                            int count = 0;
                            int name = 0;
                            foreach (string line in File.ReadLines(file))
                            {
                                string[] data = line.Split('|');
                                loCustKey.Add(Convert.ToInt64(data[2]));
                                loPartKey.Add(Convert.ToInt64(data[3]));
                                loSuppKey.Add(Convert.ToInt64(data[4]));
                                loOrderDate.Add(Convert.ToInt64(data[5]));
                                loRevenue.Add(Convert.ToInt64(data[12]));
                                loSupplyCost.Add(Convert.ToInt64(data[13]));

                                if (count == chunkQuantity)
                                {

                                    Utils.WriteToBinaryFile<List<Int64>>(loCustKeyFile, Path.Combine(loCustKeyFile, string.Format("{0}.bin", name)), loCustKey, false);
                                    Utils.WriteToBinaryFile<List<Int64>>(loPartKeyFile, Path.Combine(loPartKeyFile, string.Format("{0}.bin", name)), loPartKey, false);
                                    Utils.WriteToBinaryFile<List<Int64>>(loSuppKeyFile, Path.Combine(loSuppKeyFile, string.Format("{0}.bin", name)), loSuppKey, false);
                                    Utils.WriteToBinaryFile<List<Int64>>(loOrderDateFile, Path.Combine(loOrderDateFile, string.Format("{0}.bin", name)), loOrderDate, false);
                                    Utils.WriteToBinaryFile<List<Int64>>(loRevenueFile, Path.Combine(loRevenueFile, string.Format("{0}.bin", name)), loRevenue, false);
                                    Utils.WriteToBinaryFile<List<Int64>>(loSupplyCostFile, Path.Combine(loSupplyCostFile, string.Format("{0}.bin", name)), loSupplyCost, false);
                                    Console.WriteLine(string.Format("Chunk #{0} created.", name));
                                    name++;
                                    loCustKey.Clear();
                                    loPartKey.Clear();
                                    loSuppKey.Clear();
                                    loOrderDate.Clear();
                                    loRevenue.Clear();
                                    loSupplyCost.Clear();
                                    count = 0;
                                }
                                count++;
                            }
                            Utils.WriteToBinaryFile<List<Int64>>(loCustKeyFile, Path.Combine(loCustKeyFile, string.Format("{0}.bin", name)), loCustKey, false);
                            Utils.WriteToBinaryFile<List<Int64>>(loPartKeyFile, Path.Combine(loPartKeyFile, string.Format("{0}.bin", name)), loPartKey, false);
                            Utils.WriteToBinaryFile<List<Int64>>(loSuppKeyFile, Path.Combine(loSuppKeyFile, string.Format("{0}.bin", name)), loSuppKey, false);
                            Utils.WriteToBinaryFile<List<Int64>>(loOrderDateFile, Path.Combine(loOrderDateFile, string.Format("{0}.bin", name)), loOrderDate, false);
                            Utils.WriteToBinaryFile<List<Int64>>(loRevenueFile, Path.Combine(loRevenueFile, string.Format("{0}.bin", name)), loRevenue, false);
                            Utils.WriteToBinaryFile<List<Int64>>(loSupplyCostFile, Path.Combine(loSupplyCostFile, string.Format("{0}.bin", name)), loSupplyCost, false);
                            Console.WriteLine(string.Format("Chunk #{0} created.", name));

                        });
                        tasks.Add(t);

                        break;
                }

            }
            Task.WaitAll(tasks.ToArray());
        }

        public void createBinaryFilesDimensionTables()
        {
            Console.WriteLine("Starting to Create Binary Files");
            Parallel.Invoke(parallelOptions, () =>
            {
                List<List<Date>> chunkedDateList = date.ChunkBy<Date>(chunkQuantity);
                Utils.writeToBinaryFilesInChunks<Date>(chunkedDateList, dateFile);
                Console.WriteLine("Binary Date Table Creation Completed.");
            }, () =>
            {
                List<List<Customer>> chunkeCustomerList = customer.ChunkBy<Customer>(chunkQuantity);
                Utils.writeToBinaryFilesInChunks<Customer>(chunkeCustomerList, customerFile);
                Console.WriteLine("Binary Customer Table Creation Completed.");
            }, () =>
            {
                List<List<Supplier>> chunkedSupplierList = supplier.ChunkBy<Supplier>(chunkQuantity);
                Utils.writeToBinaryFilesInChunks<Supplier>(chunkedSupplierList, supplierFile);
                Console.WriteLine("Binary Supplier Table Creation Completed.");
            }, () =>
            {
                List<List<Part>> chunkedPartList = part.ChunkBy<Part>(chunkQuantity);
                Utils.writeToBinaryFilesInChunks<Part>(chunkedPartList, partFile);
                Console.WriteLine("Binary Part Table Creation Completed.");
            });
        }

        public void createBinaryFilesFactColumns()
        {
            foreach (string file in Directory.EnumerateFiles(folderPath, "*.tbl"))
            {
                string fileName = Path.GetFileNameWithoutExtension(file);
                if (fileName.Equals("lineorder"))
                {
                    string[] allLines = File.ReadAllLines(file);
                    int count = 0;
                    int name = 0;
                    foreach (string line in allLines)
                    {
                        string[] data = line.Split('|');
                        loCustKey.Add(Convert.ToInt64(data[2]));
                        loPartKey.Add(Convert.ToInt64(data[3]));
                        loSuppKey.Add(Convert.ToInt64(data[4]));
                        loOrderDate.Add(Convert.ToInt64(data[5]));
                        loRevenue.Add(Convert.ToInt64(data[12]));
                        loSupplyCost.Add(Convert.ToInt64(data[13]));

                        if (count == chunkQuantity)
                        {

                            Utils.WriteToBinaryFile<List<Int64>>(loCustKeyFile, Path.Combine(loCustKeyFile, string.Format("{0}.bin", name)), loCustKey, false);
                            Utils.WriteToBinaryFile<List<Int64>>(loPartKeyFile, Path.Combine(loPartKeyFile, string.Format("{0}.bin", name)), loPartKey, false);
                            Utils.WriteToBinaryFile<List<Int64>>(loSuppKeyFile, Path.Combine(loSuppKeyFile, string.Format("{0}.bin", name)), loSuppKey, false);
                            Utils.WriteToBinaryFile<List<Int64>>(loOrderDateFile, Path.Combine(loOrderDateFile, string.Format("{0}.bin", name)), loOrderDate, false);
                            Utils.WriteToBinaryFile<List<Int64>>(loRevenueFile, Path.Combine(loRevenueFile, string.Format("{0}.bin", name)), loRevenue, false);
                            Utils.WriteToBinaryFile<List<Int64>>(loSupplyCostFile, Path.Combine(loSupplyCostFile, string.Format("{0}.bin", name)), loSupplyCost, false);
                            Console.WriteLine(string.Format("Chunk #{0} created.", name));
                            name++;
                            loCustKey.Clear();
                            loPartKey.Clear();
                            loSuppKey.Clear();
                            loOrderDate.Clear();
                            loRevenue.Clear();
                            loSupplyCost.Clear();
                            count = 0;
                        }
                        count++;
                    }
                    Utils.WriteToBinaryFile<List<Int64>>(loCustKeyFile, Path.Combine(loCustKeyFile, string.Format("{0}.bin", name)), loCustKey, false);
                    Utils.WriteToBinaryFile<List<Int64>>(loPartKeyFile, Path.Combine(loPartKeyFile, string.Format("{0}.bin", name)), loPartKey, false);
                    Utils.WriteToBinaryFile<List<Int64>>(loSuppKeyFile, Path.Combine(loSuppKeyFile, string.Format("{0}.bin", name)), loSuppKey, false);
                    Utils.WriteToBinaryFile<List<Int64>>(loOrderDateFile, Path.Combine(loOrderDateFile, string.Format("{0}.bin", name)), loOrderDate, false);
                    Utils.WriteToBinaryFile<List<Int64>>(loRevenueFile, Path.Combine(loRevenueFile, string.Format("{0}.bin", name)), loRevenue, false);
                    Utils.WriteToBinaryFile<List<Int64>>(loSupplyCostFile, Path.Combine(loSupplyCostFile, string.Format("{0}.bin", name)), loSupplyCost, false);
                    Console.WriteLine(string.Format("Chunk #{0} created.", name));
                }
            }
        }

        //public void createBinaryFilesFactColumns()
        //{
        //    Console.WriteLine("Starting to Create Binary Files");
        //    Parallel.Invoke(parallelOptions, () =>
        //    {
        //        List<List<long>> chunkedCustKey = loCustKey.ChunkBy<Int64>(chunkQuantity);
        //        Utils.writeToBinaryFilesInChunks<Int64>(chunkedCustKey, loCustKeyFile);
        //        Console.WriteLine("Binary loCustKey Column Creation Completed.");
        //    }, () =>
        //    {
        //        List<List<long>> chunkedPartKey = loPartKey.ChunkBy<Int64>(chunkQuantity);
        //        Utils.writeToBinaryFilesInChunks<Int64>(chunkedPartKey, loPartKeyFile);
        //        Console.WriteLine("Binary loPartKey Column  Creation Completed.");
        //    }, () =>
        //    {
        //        List<List<long>> chunkedSuppKey = loSuppKey.ChunkBy<Int64>(chunkQuantity);
        //        Utils.writeToBinaryFilesInChunks<Int64>(chunkedSuppKey, loSuppKeyFile);
        //        Console.WriteLine("Binary loSuppKey Column  Creation Completed.");
        //    }, () =>
        //    {
        //        List<List<long>> chunkedOrderDate = loOrderDate.ChunkBy<Int64>(chunkQuantity);
        //        Utils.writeToBinaryFilesInChunks<Int64>(chunkedOrderDate, loOrderDateFile);
        //        Console.WriteLine("Binary loOrderDate Column  Creation Completed.");
        //    }, () =>
        //    {
        //        List<List<long>> chunkedSupplyCost = loSupplyCost.ChunkBy<Int64>(chunkQuantity);
        //        Utils.writeToBinaryFilesInChunks<Int64>(chunkedSupplyCost, loSupplyCostFile);
        //        Console.WriteLine("Binary loSupplyCost Column  Creation Completed.");
        //    }, () =>
        //    {
        //        List<List<long>> chunkedRevenue = loRevenue.ChunkBy<Int64>(chunkQuantity);
        //        Utils.writeToBinaryFilesInChunks<Int64>(chunkedRevenue, loRevenueFile);
        //        Console.WriteLine("Binary loRevenue Column  Creation Completed.");
        //    });

        //}
    }
}
