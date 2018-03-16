using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading.Tasks;

namespace ParallelHashJoins
{
    static class DataCreator
    {
        #region Private Variables
        private static string folderPath = @"C:\Raw_Data_Source_For_Test\SSBM - DBGEN\SF 10";
        private static string binaryFilesDirectory = @"C:\Raw_Data_Source_For_Test\SSBM - DBGEN\BFSF10";
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

        
        private static string dateFile = Path.Combine(binaryFilesDirectory, @"\date\dateFileFile");
        private static string customerFile = Path.Combine(binaryFilesDirectory, @"\customer\customerFile");
        private static string supplierFile = Path.Combine(binaryFilesDirectory, @"\supplier\supplierFile");
        private static string partFile = Path.Combine(binaryFilesDirectory, @"\part\partFile");

        private static string dDateKeyFile = Path.Combine(binaryFilesDirectory, @"\dDateKey\dDateKeyFile");
        private static string dYearFile = Path.Combine(binaryFilesDirectory, @"\dYear\dYearFile");
        private static string dYearMonthNumFile = Path.Combine(binaryFilesDirectory, @"\dYearMonthNum\dYearMonthNumFile");
        private static string dDayNumInWeekFile = Path.Combine(binaryFilesDirectory, @"\dDayNumInWeek\dDayNumInWeekFile");
        private static string dDayNumInMonthFile = Path.Combine(binaryFilesDirectory, @"\dDayNumInMont\dDayNumInMonthFile");
        private static string dDayNumInYearFile = Path.Combine(binaryFilesDirectory, @"\dDayNumInYear\dDayNumInYearFile");
        private static string dMonthNumInYearFile = Path.Combine(binaryFilesDirectory, @"\dMonthNumInYear\dMonthNumInYearFile");
        private static string dWeekNumInYearFile = Path.Combine(binaryFilesDirectory, @"\dWeekNumInYear\dWeekNumInYearFile");
        private static string dLastDayInWeekFLFile = Path.Combine(binaryFilesDirectory, @"\dLastDayInWeekFL\dLastDayInWeekFLFile");
        private static string dLastDayInMonthFLFile = Path.Combine(binaryFilesDirectory, @"\dLastDayInMonthFL\dLastDayInMonthFLFile");
        private static string dHolidayFLFile = Path.Combine(binaryFilesDirectory, @"\dHolidayFL\dHolidayFLFile");
        private static string dWeekDayFLFile = Path.Combine(binaryFilesDirectory, @"\dWeekDayFL\dWeekDayFLFile");
        private static string dDateFile = Path.Combine(binaryFilesDirectory, @"\dDate\dDateFile");
        private static string dDayOfWeekFile = Path.Combine(binaryFilesDirectory, @"\dDayOfWeek\dDayOfWeekFile");
        private static string dMonthFile = Path.Combine(binaryFilesDirectory, @"\dMonth\dMonthFile");
        private static string dYearMonthFile = Path.Combine(binaryFilesDirectory, @"\dYearMonth\dYearMonthFile");
        private static string dSellingSeasonFile = Path.Combine(binaryFilesDirectory, @"\dSellingSeason\dSellingSeasonFile");

        private static string loOrderKeyFile = Path.Combine(binaryFilesDirectory, @"\loOrderKey\loOrderKeyFile");
        private static string loLineNumberFile = Path.Combine(binaryFilesDirectory, @"\loLineNumber\loLineNumberFile");
        private static string loCustKeyFile = Path.Combine(binaryFilesDirectory, @"\loCustKey\loCustKeyFile");
        private static string loPartKeyFile = Path.Combine(binaryFilesDirectory, @"\loPartKey\loPartKeyFile");
        private static string loSuppKeyFile = Path.Combine(binaryFilesDirectory, @"\loSuppKey\loSuppKeyFile");
        private static string loOrderDateFile = Path.Combine(binaryFilesDirectory, @"\loOrderDate\loOrderDateFile");
        private static string loShipPriorityFile = Path.Combine(binaryFilesDirectory, @"\loShipPriority\loShipPriorityFile");
        private static string loQuantityFile = Path.Combine(binaryFilesDirectory, @"\loQuantity\loQuantityFile");
        private static string loExtendedPriceFile = Path.Combine(binaryFilesDirectory, @"\loExtendedPrice\loExtendedPriceFile");
        private static string loOrdTotalPriceFile = Path.Combine(binaryFilesDirectory, @"\loOrdTotalPrice\loOrdTotalPriceFile");
        private static string loDiscountFile = Path.Combine(binaryFilesDirectory, @"\loDiscount\loDiscountFile");
        private static string loRevenueFile = Path.Combine(binaryFilesDirectory, @"\loRevenue\loRevenueFile");
        private static string loSupplyCostFile = Path.Combine(binaryFilesDirectory, @"\loSupplyCost\loSupplyCostFile");
        private static string loTaxFile = Path.Combine(binaryFilesDirectory, @"\loTax\loTaxFile");
        private static string loCommitDateFile = Path.Combine(binaryFilesDirectory, @"\loCommitDate\loCommitDateFile");
        private static string loShipModeFile = Path.Combine(binaryFilesDirectory, @"\loShipMode\loShipModeFile");
        private static string loOrderPriorityFile = Path.Combine(binaryFilesDirectory, @"\loOrderPriority\loOrderPriorityFile");

        private static string cCustKeyFile = Path.Combine(binaryFilesDirectory, @"\cCustKey\cCustKeyFile");
        private static string cNameFile = Path.Combine(binaryFilesDirectory, @"\cName\cNameFile");
        private static string cAddressFile = Path.Combine(binaryFilesDirectory, @"\cAddress\cAddressFile");
        private static string cCityFile = Path.Combine(binaryFilesDirectory, @"\cCity\cCityFile");
        private static string cNationFile = Path.Combine(binaryFilesDirectory, @"\cNation\cNationFile");
        private static string cRegionFile = Path.Combine(binaryFilesDirectory, @"\cRegion\cRegionFile");
        private static string cPhoneFile = Path.Combine(binaryFilesDirectory, @"\cPhone\cPhoneFile");
        private static string cMktSegmentFile = Path.Combine(binaryFilesDirectory, @"\cMktSegment\cMktSegmentFile");

        private static string sSuppKeyFile = Path.Combine(binaryFilesDirectory, @"\sSuppKey\sSuppKeyFile");
        private static string sNameFile = Path.Combine(binaryFilesDirectory, @"\sName\sNameFile");
        private static string sAddressFile = Path.Combine(binaryFilesDirectory, @"\sAddress\sAddressFile");
        private static string sCityFile = Path.Combine(binaryFilesDirectory, @"\sCity\sCityFile");
        private static string sNationFile = Path.Combine(binaryFilesDirectory, @"\sNation\sNationFile");
        private static string sRegionFile = Path.Combine(binaryFilesDirectory, @"\sRegion\sRegionFile");
        private static string sPhoneFile = Path.Combine(binaryFilesDirectory, @"\sPhone\sPhoneFile");

        private static string pSizeFile = Path.Combine(binaryFilesDirectory, @"\pSize\pSizeFile");
        private static string pPartKeyFile = Path.Combine(binaryFilesDirectory, @"\pPartKey\pPartKeyFile");
        private static string pNameFile = Path.Combine(binaryFilesDirectory, @"\pName\pNameFile");
        private static string pMFGRFile = Path.Combine(binaryFilesDirectory, @"\pMFGR\pMFGRFile");
        private static string pCategoryFile = Path.Combine(binaryFilesDirectory, @"\pCategory\pCategoryFile");
        private static string pBrandFile = Path.Combine(binaryFilesDirectory, @"\pBrand\pBrandFile");
        private static string pColorFile = Path.Combine(binaryFilesDirectory, @"\pColor\pColorFile");
        private static string pTypeFile = Path.Combine(binaryFilesDirectory, @"\pType\pTypeFile");
        private static string pContainerFile = Path.Combine(binaryFilesDirectory, @"\pContainer\pContainerFile");

        private static int CHUNK_SIZE = 500;
        #endregion Private Variables

        public static void loadColumns()
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

        public static void loadTables()
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

        public static void createBinaryChunks<T>(string filePath, List<T> sourceList, int chunkSize)
        {
            var lists = sourceList.ChunkBy<T>(chunkSize);
            var counter = 0;
            if (!Directory.Exists((binaryFilesDirectory + filePath)))
            {
                Directory.CreateDirectory((binaryFilesDirectory + filePath));
            }
            foreach (var list in lists)
            {
                Utils.WriteToBinaryFile<List<T>>(binaryFilesDirectory + filePath + counter + ".bin", list);
                counter++;
            }
        }

        public static void createBinaryFiles()
        {
            Console.WriteLine("Starting to Create Binary Files");
            //Utils.WriteToBinaryFile<List<Date>>(dateFile, date);
            //Utils.WriteToBinaryFile<List<Customer>>(customerFile, customer);
            //Utils.WriteToBinaryFile<List<Supplier>>(supplierFile, supplier);
            //Utils.WriteToBinaryFile<List<Part>>(partFile, part);

            createBinaryChunks<Date>(dateFile, date, CHUNK_SIZE);
            createBinaryChunks<Customer>(customerFile, customer, CHUNK_SIZE);
            createBinaryChunks<Supplier>(supplierFile, supplier, CHUNK_SIZE);
            createBinaryChunks<Part>(partFile, part, CHUNK_SIZE);

            Console.WriteLine("Creating Binary Files for tables complete.");

            createBinaryChunks<int>(dDateKeyFile, dDateKey, CHUNK_SIZE);
            createBinaryChunks<int>(dYearFile, dYear, CHUNK_SIZE);
            createBinaryChunks<int>(dYearMonthNumFile, dYearMonthNum, CHUNK_SIZE);
            createBinaryChunks<int>(dDayNumInWeekFile, dDayNumInWeek, CHUNK_SIZE);
            createBinaryChunks<int>(dDayNumInMonthFile, dDayNumInMonth, CHUNK_SIZE);
            createBinaryChunks<int>(dDayNumInYearFile, dDayNumInYear, CHUNK_SIZE);
            createBinaryChunks<int>(dMonthNumInYearFile, dMonthNumInYear, CHUNK_SIZE);
            createBinaryChunks<int>(dWeekNumInYearFile, dWeekNumInYear, CHUNK_SIZE);
            createBinaryChunks<int>(dLastDayInWeekFLFile, dLastDayInWeekFL, CHUNK_SIZE);
            createBinaryChunks<int>(dLastDayInMonthFLFile, dLastDayInMonthFL, CHUNK_SIZE);
            createBinaryChunks<int>(dHolidayFLFile, dHolidayFL, CHUNK_SIZE);
            createBinaryChunks<int>(dWeekDayFLFile, dWeekDayFL, CHUNK_SIZE);
            createBinaryChunks<string>(dDateFile, dDate, CHUNK_SIZE);
            createBinaryChunks<string>(dDayOfWeekFile, dDayOfWeek, CHUNK_SIZE);
            createBinaryChunks<string>(dMonthFile, dMonth, CHUNK_SIZE);
            createBinaryChunks<string>(dYearMonthFile, dYearMonth, CHUNK_SIZE);
            createBinaryChunks<string>(dSellingSeasonFile, dSellingSeason, CHUNK_SIZE);
            Console.WriteLine("Creating Binary Files for columns in DATE table complete.");

            createBinaryChunks<int>(loOrderKeyFile, loOrderKey, CHUNK_SIZE);
            createBinaryChunks<int>(loLineNumberFile, loLineNumber, CHUNK_SIZE);
            createBinaryChunks<int>(loCustKeyFile, loCustKey, CHUNK_SIZE);
            createBinaryChunks<int>(loPartKeyFile, loPartKey, CHUNK_SIZE);
            createBinaryChunks<int>(loSuppKeyFile, loSuppKey, CHUNK_SIZE);
            createBinaryChunks<int>(loOrderDateFile, loOrderDate, CHUNK_SIZE);
            createBinaryChunks<char>(loShipPriorityFile, loShipPriority, CHUNK_SIZE);
            createBinaryChunks<int>(loQuantityFile, loQuantity, CHUNK_SIZE);
            createBinaryChunks<int>(loExtendedPriceFile, loExtendedPrice, CHUNK_SIZE);
            createBinaryChunks<int>(loOrdTotalPriceFile, loOrdTotalPrice, CHUNK_SIZE);
            createBinaryChunks<int>(loDiscountFile, loDiscount, CHUNK_SIZE);
            createBinaryChunks<int>(loSupplyCostFile, loSupplyCost, CHUNK_SIZE);
            createBinaryChunks<int>(loTaxFile, loTax, CHUNK_SIZE);
            createBinaryChunks<int>(loRevenueFile, loRevenue, CHUNK_SIZE);
            createBinaryChunks<int>(loCommitDateFile, loCommitDate, CHUNK_SIZE);
            createBinaryChunks<string>(loShipModeFile, loShipMode, CHUNK_SIZE);
            createBinaryChunks<string>(loOrderPriorityFile, loOrderPriority, CHUNK_SIZE);
            Console.WriteLine("Creating Binary Files for columns in LINEORDER table complete.");

            createBinaryChunks<int>(cCustKeyFile, cCustKey, CHUNK_SIZE);
            createBinaryChunks<string>(cNameFile, cName, CHUNK_SIZE);
            createBinaryChunks<string>(cAddressFile, cAddress, CHUNK_SIZE);
            createBinaryChunks<string>(cCityFile, cCity, CHUNK_SIZE);
            createBinaryChunks<string>(cNationFile, cNation, CHUNK_SIZE);
            createBinaryChunks<string>(cRegionFile, cRegion, CHUNK_SIZE);
            createBinaryChunks<string>(cPhoneFile, cPhone, CHUNK_SIZE);
            createBinaryChunks<string>(cMktSegmentFile, cMktSegment, CHUNK_SIZE);
            Console.WriteLine("Creating Binary Files for columns in CUSTOMER table complete.");

            createBinaryChunks<int>(sSuppKeyFile, sSuppKey, CHUNK_SIZE);
            createBinaryChunks<string>(sNameFile, sName, CHUNK_SIZE);
            createBinaryChunks<string>(sAddressFile, sAddress, CHUNK_SIZE);
            createBinaryChunks<string>(sCityFile, sCity, CHUNK_SIZE);
            createBinaryChunks<string>(sNationFile, sNation, CHUNK_SIZE);
            createBinaryChunks<string>(sRegionFile, sRegion, CHUNK_SIZE);
            createBinaryChunks<string>(sPhoneFile, sPhone, CHUNK_SIZE);
            Console.WriteLine("Creating Binary Files for columns in SUPPLIER table complete.");

            createBinaryChunks<int>(pSizeFile, pSize, CHUNK_SIZE);
            createBinaryChunks<int>(pPartKeyFile, pPartKey, CHUNK_SIZE);
            createBinaryChunks<string>(pNameFile, pName, CHUNK_SIZE);
            createBinaryChunks<string>(pMFGRFile, pMFGR, CHUNK_SIZE);
            createBinaryChunks<string>(pCategoryFile, pCategory, CHUNK_SIZE);
            createBinaryChunks<string>(pBrandFile, pBrand, CHUNK_SIZE);
            createBinaryChunks<string>(pColorFile, pColor, CHUNK_SIZE);
            createBinaryChunks<string>(pTypeFile, pType, CHUNK_SIZE);
            createBinaryChunks<string>(pContainerFile, pContainer, CHUNK_SIZE);
            Console.WriteLine("Creating Binary Files for columns in PART table complete.");
        }

        //static void Main(string[] args)
        //{
        //    // Use if Needed to Create Binary Files Again
        //    Console.WriteLine("Loading Columns...");
        //    //loadColumns();
        //    Console.WriteLine("Loading Columns Completed.");
        //    Console.WriteLine("Loading Tables...");
        //    loadTables();
        //    Console.WriteLine("Loading Tables Completed.");
        //    Console.WriteLine("Creating Binary Files...");
        //    createBinaryFiles();
        //    Console.WriteLine("Creating Binary Files Completed.");
        //    Console.ReadKey();
        //}
    }
}
