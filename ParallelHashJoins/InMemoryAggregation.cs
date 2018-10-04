using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParallelHashJoins
{

    class InMemoryAggregation
    {
        private List<int> storeId = new List<int>();
        private List<string> storeName = new List<string>();
        private List<char> storeType = new List<char>();

        private List<int> productId = new List<int>();
        private List<string> productName = new List<string>();
        private List<char> productType = new List<char>();
        private List<int> sID = new List<int>();
        private List<int> pID = new List<int>();
        private List<int> revenue = new List<int>();
        Random rand = new Random();
        ParallelOptions po = new ParallelOptions() { MaxDegreeOfParallelism = Environment.ProcessorCount };
        int numberOfDimensionRecords = 6000;
        int numberOfFactRecords = 1000000;


        public void generateSmallSampleData()
        {
            storeId.Add(1);
            storeId.Add(2);
            storeId.Add(3);
            storeId.Add(4);
            storeId.Add(5);

            storeName.Add("sa1");
            storeName.Add("sb1");
            storeName.Add("sa2");
            storeName.Add("sb2");
            storeName.Add("sa3");

            storeType.Add('a');
            storeType.Add('b');
            storeType.Add('a');
            storeType.Add('b');
            storeType.Add('a');

            productId.Add(1);
            productId.Add(2);
            productId.Add(3);
            productId.Add(4);
            productId.Add(5);

            productName.Add("pb1");
            productName.Add("pb2");
            productName.Add("pa1");
            productName.Add("pa2");
            productName.Add("pb3");

            productType.Add('b');
            productType.Add('b');
            productType.Add('a');
            productType.Add('a');
            productType.Add('b');

            sID.Add(3);
            sID.Add(1);
            sID.Add(3);
            sID.Add(2);
            sID.Add(4);
            sID.Add(5);
            sID.Add(2);
            sID.Add(1);
            sID.Add(3);
            sID.Add(4);

            pID.Add(4);
            pID.Add(2);
            pID.Add(1);
            pID.Add(2);
            pID.Add(5);
            pID.Add(4);
            pID.Add(2);
            pID.Add(3);
            pID.Add(1);
            pID.Add(3);

            revenue.Add(100);
            revenue.Add(120);
            revenue.Add(311);
            revenue.Add(144);
            revenue.Add(150);
            revenue.Add(120);
            revenue.Add(250);
            revenue.Add(364);
            revenue.Add(129);
            revenue.Add(450);
        }

        public void generateBigSampleData()
        {
            for (int i = 0; i < numberOfDimensionRecords; i++)
            {
                storeId.Add(i);
                storeName.Add(getRandomLetter() + " " + getRandomLetter());
                storeType.Add(getRandomLetter());
                productId.Add(i);
                productName.Add(getRandomLetter() + " " + getRandomLetter());
                productType.Add(getRandomLetter());
            }

            for (int i = 0; i < numberOfFactRecords; i++)
            {
                sID.Add(getRandomInt(0, numberOfDimensionRecords));
                pID.Add(getRandomInt(0, numberOfDimensionRecords));
                revenue.Add(getRandomInt(0, 2000));
            }
        }

        public void IMA_Simple()
        {
            long memoryStart = GC.GetTotalMemory(true);
            Stopwatch sw = new Stopwatch();
            sw.Start();

            Dictionary<int, int> kvStoreDim = new Dictionary<int, int>();
            //DataTable tempTableStoreDim = new DataTable();
            //tempTableStoreDim.Columns.Add("storeName", typeof(string));
            //tempTableStoreDim.Columns.Add("denseGroupingKey", typeof(int));
            Dictionary<int, int> tempTableStoreDim = new Dictionary<int, int>();


            int storeIndex = 0;
            int dgKeyStore = 0;
            foreach (var type in storeType)
            {
                if (type == 'a')
                {
                    dgKeyStore++;
                    kvStoreDim.Add(storeIndex + 1, dgKeyStore);
                    tempTableStoreDim.Add(storeIndex + 1, dgKeyStore);
                }
                else
                {
                    kvStoreDim.Add(storeIndex + 1, 0);
                }
                storeIndex++;
            }

            Dictionary<int, int> kvProductDim = new Dictionary<int, int>();
            Dictionary<int, int> tempTableProductDim = new Dictionary<int, int>();
            int productIndex = 0;
            int dgKeyProduct = 0;
            foreach (var type in productType)
            {
                if (type == 'b')
                {
                    dgKeyProduct++;
                    kvProductDim.Add(productIndex + 1, dgKeyProduct);
                    tempTableProductDim.Add(productIndex + 1, dgKeyProduct);
                }
                else
                {
                    kvProductDim.Add(productIndex + 1, 0);
                }
                productIndex++;
            }

            int dgkLengthStore = tempTableStoreDim.Count() + 1;
            int dgkLengthProduct = tempTableProductDim.Count() + 1;

            int[,] inMemoryAccumulator = new int[dgkLengthStore, dgkLengthProduct];

            for (int i = 0; i < sID.Count(); i++)
            {
                int storeId = sID[i];
                int productId = pID[i];
                int dgkStoreDim = 0;
                int dgkProductDim = 0;
                if (kvStoreDim.TryGetValue(storeId, out dgkStoreDim) && kvProductDim.TryGetValue(productId, out dgkProductDim))
                {
                    if (dgkStoreDim == 0 || dgkProductDim == 0)
                    {
                        // skip
                    }
                    else
                    {
                        inMemoryAccumulator[dgkStoreDim, dgkProductDim] += revenue[i];
                    }
                }
            }

            List<string> finalTable = new List<string>();
            foreach (var sdRow in tempTableStoreDim)
            {
                foreach (var pdRow in tempTableProductDim)
                {
                    int sumRevenue = inMemoryAccumulator[sdRow.Value, pdRow.Value];
                    if (sumRevenue != 0)
                    {
                        finalTable.Add(sdRow.Key + ", " + pdRow.Key + ", " + sumRevenue);
                    }

                }
            }

            long memoryUsed = GC.GetTotalMemory(true) - memoryStart;
            sw.Stop();

            //foreach (var item in finalTable)
            //{
            //    Console.WriteLine(item);
            //}

            Console.WriteLine("==============================================");
            Console.WriteLine("[IMA] Memory Used: " + memoryUsed);
            Console.WriteLine("[IMA] Time Elaspsed: " + sw.ElapsedMilliseconds + " ms");

        }

        public void IMA_Simple_V2()
        {
            long memoryStart = GC.GetTotalMemory(true);
            Stopwatch sw = new Stopwatch();
            sw.Start();

            Dictionary<int, int> kvStoreDim = new Dictionary<int, int>();
            //DataTable tempTableStoreDim = new DataTable();
            //tempTableStoreDim.Columns.Add("storeName", typeof(string));
            //tempTableStoreDim.Columns.Add("denseGroupingKey", typeof(int));
            Dictionary<string, int> tempTableStoreDim = new Dictionary<string, int>();


            int storeIndex = 0;
            int dgKeyStore = 0;
            foreach (var type in storeType)
            {
                if (type == 'a')
                {
                    string sName = storeName[storeIndex];
                    int dgKey = 0;
                    if (tempTableStoreDim.TryGetValue(sName, out dgKey))
                    {
                        kvStoreDim.Add(storeIndex + 1, dgKey);
                    }
                    else
                    {
                        dgKeyStore++;
                        kvStoreDim.Add(storeIndex + 1, dgKeyStore);
                        tempTableStoreDim.Add(sName, dgKeyStore);
                    }
                }
                else
                {
                    kvStoreDim.Add(storeIndex + 1, 0);
                }
                storeIndex++;
            }

            Dictionary<int, int> kvProductDim = new Dictionary<int, int>();
            Dictionary<string, int> tempTableProductDim = new Dictionary<string, int>();
            int productIndex = 0;
            int dgKeyProduct = 0;
            foreach (var type in productType)
            {
                if (type == 'b')
                {

                    string pName = productName[productIndex];
                    int dgKey = 0;
                    if (tempTableProductDim.TryGetValue(pName, out dgKey))
                    {
                        kvProductDim.Add(productIndex + 1, dgKey);
                    }
                    else
                    {
                        dgKeyProduct++;
                        kvProductDim.Add(productIndex + 1, dgKeyProduct);
                        tempTableProductDim.Add(pName, dgKeyProduct);
                    }

                }
                else
                {
                    kvProductDim.Add(productIndex + 1, 0);
                }
                productIndex++;
            }

            int dgkLengthStore = tempTableStoreDim.Count() + 1;
            int dgkLengthProduct = tempTableProductDim.Count() + 1;

            int[,] inMemoryAccumulator = new int[dgkLengthStore, dgkLengthProduct];

            for (int i = 0; i < sID.Count(); i++)
            {
                int storeId = sID[i];
                int productId = pID[i];
                int dgkStoreDim = 0;
                int dgkProductDim = 0;
                if (kvStoreDim.TryGetValue(storeId, out dgkStoreDim) && kvProductDim.TryGetValue(productId, out dgkProductDim))
                {
                    if (dgkStoreDim == 0 || dgkProductDim == 0)
                    {
                        // skip
                    }
                    else
                    {
                        inMemoryAccumulator[dgkStoreDim, dgkProductDim] += revenue[i];
                    }
                }
            }

            List<string> finalTable = new List<string>();
            foreach (var sdRow in tempTableStoreDim)
            {
                foreach (var pdRow in tempTableProductDim)
                {
                    int sumRevenue = inMemoryAccumulator[sdRow.Value, pdRow.Value];
                    if (sumRevenue != 0)
                    {
                        finalTable.Add(sdRow.Key + ", " + pdRow.Key + ", " + sumRevenue);
                    }

                }
            }

            long memoryUsed = GC.GetTotalMemory(true) - memoryStart;
            sw.Stop();

            //foreach (var item in finalTable)
            //{
            //    Console.WriteLine(item);
            //}

            Console.WriteLine("==============================================");
            Console.WriteLine("[IMA] Memory Used: " + memoryUsed + ", Total:" + finalTable.Count);
            Console.WriteLine("[IMA] Time Elaspsed: " + sw.ElapsedMilliseconds + " ms");

        }

        /// <summary>
        /// Key Vector is implemented as Dictionary <int, int>
        /// InMemory Accumulator is a MultiDimensional Array
        /// Temporary Table is a Datatable
        /// </summary>
        public void IMA_V3()
        {
            long memoryStart = GC.GetTotalMemory(true);
            Stopwatch sw = new Stopwatch();
            sw.Start();

            Dictionary<int, int> kvStoreDim = new Dictionary<int, int>();
            DataTable tempTableStoreDim = new DataTable();
            tempTableStoreDim.Columns.Add("storeName", typeof(string));
            tempTableStoreDim.Columns.Add("denseGroupingKey", typeof(int));
            // Dictionary<string, int> tempTableStoreDim = new Dictionary<string, int>();


            int storeIndex = 0;
            int dgKeyStore = 0;
            foreach (var type in storeType)
            {
                if (type == 'a')
                {
                    string sName = storeName[storeIndex];
                    if (tempTableStoreDim.Rows.Count > 0)
                    {
                        var tempTable = tempTableStoreDim.Copy();
                        var found = false;
                        foreach (DataRow row in tempTableStoreDim.Rows)
                        {
                            var storeName = row.Field<string>("storeName");
                            if (storeName.Equals(sName))
                            {
                                int dgKey = row.Field<int>("denseGroupingKey");
                                kvStoreDim.Add(storeIndex + 1, dgKey);
                                found = true;
                                break;
                            }
                        }
                        if (!found)
                        {
                            dgKeyStore++;
                            tempTable.Rows.Add(sName, dgKeyStore);
                            kvStoreDim.Add(storeIndex + 1, dgKeyStore);
                        }
                        tempTableStoreDim = tempTable;
                    }
                    else
                    {
                        dgKeyStore++;
                        tempTableStoreDim.Rows.Add(sName, dgKeyStore);
                        kvStoreDim.Add(storeIndex + 1, dgKeyStore);
                    }
                }
                else
                {
                    kvStoreDim.Add(storeIndex + 1, 0);
                }
                storeIndex++;
            }

            Dictionary<int, int> kvProductDim = new Dictionary<int, int>();
            DataTable tempTableProductDim = new DataTable();
            tempTableProductDim.Columns.Add("productName", typeof(string));
            tempTableProductDim.Columns.Add("denseGroupingKey", typeof(int));

            int productIndex = 0;
            int dgKeyProduct = 0;
            foreach (var type in productType)
            {
                if (type == 'b')
                {
                    string pName = productName[productIndex];
                    if (tempTableProductDim.Rows.Count > 0)
                    {
                        var tempTable = tempTableProductDim.Copy();
                        var found = false;
                        foreach (DataRow row in tempTableProductDim.Rows)
                        {
                            var productName = row.Field<string>("productName");
                            if (productName.Equals(pName))
                            {
                                int dgKey = row.Field<int>("denseGroupingKey");
                                kvProductDim.Add(productIndex + 1, dgKey);
                                found = true;
                                break;
                            }
                        }
                        if (!found)
                        {
                            dgKeyProduct++;
                            tempTable.Rows.Add(pName, dgKeyProduct);
                            kvProductDim.Add(productIndex + 1, dgKeyProduct);
                        }


                        tempTableProductDim = tempTable;
                    }
                    else
                    {
                        dgKeyProduct++;
                        tempTableProductDim.Rows.Add(pName, dgKeyProduct);
                        kvProductDim.Add(productIndex + 1, dgKeyProduct);
                    }

                }
                else
                {
                    kvProductDim.Add(productIndex + 1, 0);
                }
                productIndex++;
            }

            int dgkLengthStore = tempTableStoreDim.Rows.Count + 1;
            int dgkLengthProduct = tempTableProductDim.Rows.Count + 1;

            int[,] inMemoryAccumulator = new int[dgkLengthStore, dgkLengthProduct];

            for (int i = 0; i < sID.Count(); i++)
            {
                int storeId = sID[i];
                int productId = pID[i];
                int dgkStoreDim = 0;
                int dgkProductDim = 0;
                if (kvStoreDim.TryGetValue(storeId, out dgkStoreDim) && kvProductDim.TryGetValue(productId, out dgkProductDim))
                {
                    if (dgkStoreDim == 0 || dgkProductDim == 0)
                    {
                        // skip
                    }
                    else
                    {
                        inMemoryAccumulator[dgkStoreDim, dgkProductDim] += revenue[i];
                    }
                }
            }

            List<string> finalTable = new List<string>();
            foreach (DataRow sdRow in tempTableStoreDim.Rows)
            {
                foreach (DataRow pdRow in tempTableProductDim.Rows)
                {
                    int sumRevenue = inMemoryAccumulator[sdRow.Field<int>("denseGroupingKey"), pdRow.Field<int>("denseGroupingKey")];
                    if (sumRevenue != 0)
                    {
                        finalTable.Add(sdRow.Field<string>("storeName") + ", " + pdRow.Field<string>("productName") + ", " + sumRevenue);
                    }

                }
            }

            long memoryUsed = GC.GetTotalMemory(true) - memoryStart;
            sw.Stop();

            //foreach (var item in finalTable)
            //{
            //    Console.WriteLine(item);
            //}

            Console.WriteLine("==============================================");
            Console.WriteLine("[IMA_V3] Memory Used: " + memoryUsed + ", Total:" + finalTable.Count);
            Console.WriteLine("[IMA_V3] Time Elaspsed: " + sw.ElapsedMilliseconds + " ms");

        }

        public void IMA_V3_Parallel()
        {
            long memoryStart = GC.GetTotalMemory(true);
            Stopwatch sw = new Stopwatch();
            sw.Start();

            Dictionary<int, int> kvStoreDim = new Dictionary<int, int>();
            DataTable tempTableStoreDim = new DataTable();
            tempTableStoreDim.Columns.Add("storeName", typeof(string));
            tempTableStoreDim.Columns.Add("denseGroupingKey", typeof(int));
            // Dictionary<string, int> tempTableStoreDim = new Dictionary<string, int>();

            Dictionary<int, int> kvProductDim = new Dictionary<int, int>();
            DataTable tempTableProductDim = new DataTable();
            tempTableProductDim.Columns.Add("productName", typeof(string));
            tempTableProductDim.Columns.Add("denseGroupingKey", typeof(int));
            Parallel.Invoke(po, () =>
            {

                int storeIndex = 0;
                int dgKeyStore = 0;
                foreach (var type in storeType)
                {
                    if (type == 'a')
                    {
                        string sName = storeName[storeIndex];
                        if (tempTableStoreDim.Rows.Count > 0)
                        {
                            var tempTable = tempTableStoreDim.Copy();
                            var found = false;
                            foreach (DataRow row in tempTableStoreDim.Rows)
                            {
                                var storeName = row.Field<string>("storeName");
                                if (storeName.Equals(sName))
                                {
                                    int dgKey = row.Field<int>("denseGroupingKey");
                                    kvStoreDim.Add(storeIndex + 1, dgKey);
                                    found = true;
                                    break;
                                }
                            }
                            if (!found)
                            {
                                dgKeyStore++;
                                tempTable.Rows.Add(sName, dgKeyStore);
                                kvStoreDim.Add(storeIndex + 1, dgKeyStore);
                            }
                            tempTableStoreDim = tempTable;
                        }
                        else
                        {
                            dgKeyStore++;
                            tempTableStoreDim.Rows.Add(sName, dgKeyStore);
                            kvStoreDim.Add(storeIndex + 1, dgKeyStore);
                        }
                    }
                    else
                    {
                        kvStoreDim.Add(storeIndex + 1, 0);
                    }
                    storeIndex++;
                }
            }, () =>
            {
                int productIndex = 0;
                int dgKeyProduct = 0;
                foreach (var type in productType)
                {
                    if (type == 'b')
                    {
                        string pName = productName[productIndex];
                        if (tempTableProductDim.Rows.Count > 0)
                        {
                            var tempTable = tempTableProductDim.Copy();
                            var found = false;
                            foreach (DataRow row in tempTableProductDim.Rows)
                            {
                                var productName = row.Field<string>("productName");
                                if (productName.Equals(pName))
                                {
                                    int dgKey = row.Field<int>("denseGroupingKey");
                                    kvProductDim.Add(productIndex + 1, dgKey);
                                    found = true;
                                    break;
                                }
                            }
                            if (!found)
                            {
                                dgKeyProduct++;
                                tempTable.Rows.Add(pName, dgKeyProduct);
                                kvProductDim.Add(productIndex + 1, dgKeyProduct);
                            }


                            tempTableProductDim = tempTable;
                        }
                        else
                        {
                            dgKeyProduct++;
                            tempTableProductDim.Rows.Add(pName, dgKeyProduct);
                            kvProductDim.Add(productIndex + 1, dgKeyProduct);
                        }

                    }
                    else
                    {
                        kvProductDim.Add(productIndex + 1, 0);
                    }
                    productIndex++;
                }

            });
            
            int dgkLengthStore = tempTableStoreDim.Rows.Count + 1;
            int dgkLengthProduct = tempTableProductDim.Rows.Count + 1;

            int[,] inMemoryAccumulator = new int[dgkLengthStore, dgkLengthProduct];

            for (int i = 0; i < sID.Count(); i++)
            {
                int storeId = sID[i];
                int productId = pID[i];
                int dgkStoreDim = 0;
                int dgkProductDim = 0;
                if (kvStoreDim.TryGetValue(storeId, out dgkStoreDim) && kvProductDim.TryGetValue(productId, out dgkProductDim))
                {
                    if (dgkStoreDim == 0 || dgkProductDim == 0)
                    {
                        // skip
                    }
                    else
                    {
                        inMemoryAccumulator[dgkStoreDim, dgkProductDim] += revenue[i];
                    }
                }
            }

            List<string> finalTable = new List<string>();
            foreach (DataRow sdRow in tempTableStoreDim.Rows)
            {
                foreach (DataRow pdRow in tempTableProductDim.Rows)
                {
                    int sumRevenue = inMemoryAccumulator[sdRow.Field<int>("denseGroupingKey"), pdRow.Field<int>("denseGroupingKey")];
                    if (sumRevenue != 0)
                    {
                        finalTable.Add(sdRow.Field<string>("storeName") + ", " + pdRow.Field<string>("productName") + ", " + sumRevenue);
                    }

                }
            }

            long memoryUsed = GC.GetTotalMemory(true) - memoryStart;
            sw.Stop();

            //foreach (var item in finalTable)
            //{
            //    Console.WriteLine(item);
            //}

            Console.WriteLine("==============================================");
            Console.WriteLine("[IMA_V3_Parallel] Memory Used: " + memoryUsed + ", Total:" + finalTable.Count);
            Console.WriteLine("[IMA_V3_Parallel] Time Elaspsed: " + sw.ElapsedMilliseconds + " ms");

        }

        /// <summary>
        /// Uses List with ROW ID and Revenue Value and stores it in Hash Table with DICTIONARY of GROUPING attribute
        /// </summary>
        public void ABC()
        {
            long memoryStart = GC.GetTotalMemory(true);
            Stopwatch sw = new Stopwatch();
            sw.Start();

            Dictionary<int, string> storeDict = new Dictionary<int, string>();
            int storeIndex = 0;
            foreach (var type in storeType)
            {
                if (type == 'a')
                {
                    string sName = storeName[storeIndex];
                    storeDict.Add(storeIndex + 1, sName);
                }
                storeIndex++;
            }

            Dictionary<int, string> productDict = new Dictionary<int, string>();
            int productIndex = 0;
            foreach (var type in productType)
            {
                if (type == 'b')
                {
                    string pName = productName[productIndex];
                    productDict.Add(productIndex + 1, pName);
                }
                productIndex++;
            }

            Dictionary<string, List<Tuple<int, int>>> storeGroupDict = new Dictionary<string, List<Tuple<int, int>>>();
            int scounter = 0;
            foreach (var sid in sID)
            {
                string sName = string.Empty;
                if (storeDict.TryGetValue(sid, out sName))
                {
                    List<Tuple<int, int>> listAggColl = null;
                    if (storeGroupDict.TryGetValue(sName, out listAggColl))
                    {
                        listAggColl.Add(Tuple.Create(scounter + 1, revenue[scounter]));
                        storeGroupDict[sName] = listAggColl;
                    }
                    else
                    {
                        listAggColl = new List<Tuple<int, int>>();
                        listAggColl.Add(Tuple.Create(scounter + 1, revenue[scounter]));
                        storeGroupDict.Add(sName, listAggColl);
                    }
                }
                scounter++;
            }

            Dictionary<string, List<Tuple<int, int>>> productGroupDict = new Dictionary<string, List<Tuple<int, int>>>();
            int pcounter = 0;
            foreach (var pid in pID)
            {
                string pName = string.Empty;
                if (productDict.TryGetValue(pid, out pName))
                {
                    List<Tuple<int, int>> listAggColl = null;
                    if (productGroupDict.TryGetValue(pName, out listAggColl))
                    {
                        listAggColl.Add(Tuple.Create(pcounter + 1, revenue[pcounter]));
                        productGroupDict[pName] = listAggColl;
                    }
                    else
                    {
                        listAggColl = new List<Tuple<int, int>>();
                        listAggColl.Add(Tuple.Create(pcounter + 1, revenue[pcounter]));
                        productGroupDict.Add(pName, listAggColl);
                    }
                }
                pcounter++;
            }

            List<string> finalTable = new List<string>();
            foreach (var sGItem in storeGroupDict)
            {
                foreach (var pGItem in productGroupDict)
                {
                    var collectionIntersect = sGItem.Value.Intersect(pGItem.Value);
                    if (collectionIntersect.Count() > 0)
                    {
                        int sum = 0;
                        foreach (var item in collectionIntersect)
                        {
                            sum += item.Item2;
                        }
                        finalTable.Add(sGItem.Key + ", " + pGItem.Key + ", " + sum);
                    }
                }
            }
            long memoryUsed = GC.GetTotalMemory(true) - memoryStart;
            sw.Stop();

            //foreach (var item in finalTable)
            //{
            //    Console.WriteLine(item);
            //}

            Console.WriteLine("==============================================");
            Console.WriteLine("[ABC] Memory Used: " + memoryUsed + ", Total:" + finalTable.Count);
            Console.WriteLine("[ABC] Time Elaspsed: " + sw.ElapsedMilliseconds + " ms");
        }

        /// <summary>
        /// used DIC in step 1
        /// Uses DICTIONARY to store GROUPING attribute as key and row ID as value
        /// </summary>
        public void ABC_V2()
        {
            long memoryStart = GC.GetTotalMemory(true);
            Stopwatch sw = new Stopwatch();
            sw.Start();

            Dictionary<int, string> storeDict = new Dictionary<int, string>();
            int storeIndex = 0;
            foreach (var type in storeType)
            {
                if (type == 'a')
                {
                    string sName = storeName[storeIndex];
                    storeDict.Add(storeIndex + 1, sName);
                }
                storeIndex++;
            }

            Dictionary<int, string> productDict = new Dictionary<int, string>();
            int productIndex = 0;
            foreach (var type in productType)
            {
                if (type == 'b')
                {
                    string pName = productName[productIndex];
                    productDict.Add(productIndex + 1, pName);
                }
                productIndex++;
            }

            Dictionary<string, HashSet<int>> storeGroupDict = new Dictionary<string, HashSet<int>>();
            int scounter = 0;
            foreach (var sid in sID)
            {
                string sName = string.Empty;
                if (storeDict.TryGetValue(sid, out sName))
                {
                    HashSet<int> coll = null;
                    if (storeGroupDict.TryGetValue(sName, out coll))
                    {
                        coll.Add(scounter + 1);
                        storeGroupDict[sName] = coll;
                    }
                    else
                    {
                        coll = new HashSet<int>();
                        coll.Add(scounter + 1);
                        storeGroupDict.Add(sName, coll);
                    }
                }
                scounter++;
            }

            Dictionary<string, HashSet<int>> productGroupDict = new Dictionary<string, HashSet<int>>();
            int pcounter = 0;
            foreach (var pid in pID)
            {
                string pName = string.Empty;
                if (productDict.TryGetValue(pid, out pName))
                {
                    HashSet<int> coll = null;
                    if (productGroupDict.TryGetValue(pName, out coll))
                    {
                        coll.Add(pcounter + 1);
                        productGroupDict[pName] = coll;
                    }
                    else
                    {
                        coll = new HashSet<int>();
                        coll.Add(pcounter + 1);
                        productGroupDict.Add(pName, coll);
                    }
                }
                pcounter++;
            }

            List<string> finalTable = new List<string>();
            foreach (var sGItem in storeGroupDict)
            {
                foreach (var pGItem in productGroupDict)
                {
                    var collectionIntersect = sGItem.Value.Intersect(pGItem.Value);
                    if (collectionIntersect.Count() > 0)
                    {
                        int sum = 0;
                        foreach (var item in collectionIntersect)
                        {
                            sum += revenue[item - 1];
                        }
                        finalTable.Add(sGItem.Key + ", " + pGItem.Key + ", " + sum);
                    }
                }
            }
            long memoryUsed = GC.GetTotalMemory(true) - memoryStart;
            sw.Stop();

            //foreach (var item in finalTable)
            //{
            //    Console.WriteLine(item);
            //}

            Console.WriteLine("==============================================");
            Console.WriteLine("[ABC_V2] Memory Used: " + memoryUsed + ", Total:" + finalTable.Count);
            Console.WriteLine("[ABC_V2] Time Elaspsed: " + sw.ElapsedMilliseconds + " ms");
        }

        /// <summary>
        /// used HashSet in step 1
        /// Uses DICTIONARY to store GROUPING attribute as key and HASH SET of row ID as value
        /// </summary>
        public void ABC_V3()
        {
            long memoryStart = GC.GetTotalMemory(true);
            Stopwatch sw = new Stopwatch();
            sw.Start();

            HashSet<int> storeDict = new HashSet<int>();
            int storeIndex = 0;
            foreach (var type in storeType)
            {
                if (type == 'a')
                {
                    storeDict.Add(storeIndex + 1);
                }
                storeIndex++;
            }

            HashSet<int> productDict = new HashSet<int>();
            int productIndex = 0;
            foreach (var type in productType)
            {
                if (type == 'b')
                {
                    productDict.Add(productIndex + 1);
                }
                productIndex++;
            }

            Dictionary<string, HashSet<int>> storeGroupDict = new Dictionary<string, HashSet<int>>();
            int scounter = 0;
            foreach (var sid in sID)
            {
                if (storeDict.Contains(sid))
                {
                    string sName = storeName[sid - 1];
                    HashSet<int> coll = null;
                    if (storeGroupDict.TryGetValue(sName, out coll))
                    {
                        coll.Add(scounter + 1);
                        storeGroupDict[sName] = coll;
                    }
                    else
                    {
                        coll = new HashSet<int>();
                        coll.Add(scounter + 1);
                        storeGroupDict.Add(sName, coll);
                    }
                }
                scounter++;
            }

            Dictionary<string, HashSet<int>> productGroupDict = new Dictionary<string, HashSet<int>>();
            int pcounter = 0;
            foreach (var pid in pID)
            {
                if (productDict.Contains(pid))
                {
                    string pName = productName[pid - 1];
                    HashSet<int> coll = null;
                    if (productGroupDict.TryGetValue(pName, out coll))
                    {
                        coll.Add(pcounter + 1);
                        productGroupDict[pName] = coll;
                    }
                    else
                    {
                        coll = new HashSet<int>();
                        coll.Add(pcounter + 1);
                        productGroupDict.Add(pName, coll);
                    }
                }
                pcounter++;
            }

            List<string> finalTable = new List<string>();
            foreach (var sGItem in storeGroupDict)
            {
                foreach (var pGItem in productGroupDict)
                {
                    // http://codebetter.com/patricksmacchia/2011/06/16/linq-intersect-2-7x-faster-with-hashset/
                    //                    If the HashSet < T > is bigger than the other sequence, gains factor can be high (like 15x).
                    //If the HashSet < T > is smaller than the other sequence, gains factor tends to 1x.
                    //   If the HashSet<T> size is comparable to the other sequence size, the gain factor is around 2.7.

                    var collectionIntersect = sGItem.Value.Count > pGItem.Value.Count ? sGItem.Value.Intersect(pGItem.Value) : pGItem.Value.Intersect(sGItem.Value);
                    if (collectionIntersect.Count() > 0)
                    {
                        int sum = 0;
                        foreach (var item in collectionIntersect)
                        {
                            sum += revenue[item - 1];
                        }
                        finalTable.Add(sGItem.Key + ", " + pGItem.Key + ", " + sum);
                    }
                }
            }
            long memoryUsed = GC.GetTotalMemory(true) - memoryStart;
            sw.Stop();

            //foreach (var item in finalTable)
            //{
            //    Console.WriteLine(item);
            //}

            Console.WriteLine("==============================================");
            Console.WriteLine("[ABC_V3] Memory Used: " + memoryUsed + ", Total:" + finalTable.Count);
            Console.WriteLine("[ABC_V3] Time Elaspsed: " + sw.ElapsedMilliseconds + " ms");
        }


        public void ABC_V3_Parallel()
        {
            long memoryStart = GC.GetTotalMemory(true);
            Stopwatch sw = new Stopwatch();
            sw.Start();
            HashSet<int> storeDict = new HashSet<int>();
            HashSet<int> productDict = new HashSet<int>();

            Parallel.Invoke(po, () =>
            {

                int storeIndex = 0;
                foreach (var type in storeType)
                {
                    if (type == 'a')
                    {
                        storeDict.Add(storeIndex + 1);
                    }
                    storeIndex++;
                }
            }, () =>
            {

                int productIndex = 0;
                foreach (var type in productType)
                {
                    if (type == 'b')
                    {
                        productDict.Add(productIndex + 1);
                    }
                    productIndex++;
                }
            });

            Dictionary<string, HashSet<int>> storeGroupDict = new Dictionary<string, HashSet<int>>();
            Dictionary<string, HashSet<int>> productGroupDict = new Dictionary<string, HashSet<int>>();
            Parallel.Invoke(po, () =>
            {
                int scounter = 0;
                foreach (var sid in sID)
                {
                    if (storeDict.Contains(sid))
                    {
                        string sName = storeName[sid - 1];
                        HashSet<int> coll = null;
                        if (storeGroupDict.TryGetValue(sName, out coll))
                        {
                            coll.Add(scounter + 1);
                            storeGroupDict[sName] = coll;
                        }
                        else
                        {
                            coll = new HashSet<int>();
                            coll.Add(scounter + 1);
                            storeGroupDict.Add(sName, coll);
                        }
                    }
                    scounter++;
                }
            }, () =>
            {
                int pcounter = 0;
                foreach (var pid in pID)
                {
                    if (productDict.Contains(pid))
                    {
                        string pName = productName[pid - 1];
                        HashSet<int> coll = null;
                        if (productGroupDict.TryGetValue(pName, out coll))
                        {
                            coll.Add(pcounter + 1);
                            productGroupDict[pName] = coll;
                        }
                        else
                        {
                            coll = new HashSet<int>();
                            coll.Add(pcounter + 1);
                            productGroupDict.Add(pName, coll);
                        }
                    }
                    pcounter++;
                }
            });

            List<string> finalTable = new List<string>();
            foreach (var sGItem in storeGroupDict)
            {
                foreach (var pGItem in productGroupDict)
                {
                    // http://codebetter.com/patricksmacchia/2011/06/16/linq-intersect-2-7x-faster-with-hashset/
                    //                    If the HashSet < T > is bigger than the other sequence, gains factor can be high (like 15x).
                    //If the HashSet < T > is smaller than the other sequence, gains factor tends to 1x.
                    //   If the HashSet<T> size is comparable to the other sequence size, the gain factor is around 2.7.

                    var collectionIntersect = sGItem.Value.Count > pGItem.Value.Count ? sGItem.Value.Intersect(pGItem.Value) : pGItem.Value.Intersect(sGItem.Value);
                    if (collectionIntersect.Count() > 0)
                    {
                        int sum = 0;
                        foreach (var item in collectionIntersect)
                        {
                            sum += revenue[item - 1];
                        }
                        finalTable.Add(sGItem.Key + ", " + pGItem.Key + ", " + sum);
                    }
                }
            }
            long memoryUsed = GC.GetTotalMemory(true) - memoryStart;
            sw.Stop();

            //foreach (var item in finalTable)
            //{
            //    Console.WriteLine(item);
            //}

            Console.WriteLine("==============================================");
            Console.WriteLine("[ABC_V3_Parallel] Memory Used: " + memoryUsed + ", Total:" + finalTable.Count);
            Console.WriteLine("[ABC_V3_Parallel] Time Elaspsed: " + sw.ElapsedMilliseconds + " ms");
        }

        /// <summary>
        /// Failed Experiment
        /// used HashSet in step 1
        /// Uses DICTIONARY to store GROUPING attribute as key and BIT MAP of row ID as value
        /// </summary>
        public void ABC_V4()
        {
            long memoryStart = GC.GetTotalMemory(true);
            Stopwatch sw = new Stopwatch();
            sw.Start();

            HashSet<int> storeDict = new HashSet<int>();
            int storeIndex = 0;
            foreach (var type in storeType)
            {
                if (type == 'a')
                {
                    storeDict.Add(storeIndex + 1);
                }
                storeIndex++;
            }

            HashSet<int> productDict = new HashSet<int>();
            int productIndex = 0;
            foreach (var type in productType)
            {
                if (type == 'b')
                {
                    productDict.Add(productIndex + 1);
                }
                productIndex++;
            }

            Dictionary<string, BitArray> storeGroupDict = new Dictionary<string, BitArray>();
            int scounter = 0;
            foreach (var sid in sID)
            {
                if (storeDict.Contains(sid))
                {
                    string sName = storeName[sid - 1];
                    BitArray coll = null;
                    if (storeGroupDict.TryGetValue(sName, out coll))
                    {
                        coll.Set(scounter + 1, true);
                        storeGroupDict[sName] = coll;
                    }
                    else
                    {
                        coll = new BitArray(sID.Count);
                        coll.Set(scounter + 1, true);
                        storeGroupDict.Add(sName, coll);
                    }
                }
                scounter++;
            }

            Dictionary<string, BitArray> productGroupDict = new Dictionary<string, BitArray>();
            int pcounter = 0;
            foreach (var pid in pID)
            {
                if (productDict.Contains(pid))
                {
                    string pName = productName[pid - 1];
                    BitArray coll = null;
                    if (productGroupDict.TryGetValue(pName, out coll))
                    {
                        coll.Set(pcounter + 1, true);
                        productGroupDict[pName] = coll;
                    }
                    else
                    {
                        coll = new BitArray(pID.Count);
                        coll.Set(pcounter + 1, true);
                        productGroupDict.Add(pName, coll);
                    }
                }
                pcounter++;
            }

            List<string> finalTable = new List<string>();

            foreach (var sGItem in storeGroupDict)
            {
                foreach (var pGItem in productGroupDict)
                {
                    // http://codebetter.com/patricksmacchia/2011/06/16/linq-intersect-2-7x-faster-with-hashset/
                    //                    If the HashSet < T > is bigger than the other sequence, gains factor can be high (like 15x).
                    //If the HashSet < T > is smaller than the other sequence, gains factor tends to 1x.
                    //   If the HashSet<T> size is comparable to the other sequence size, the gain factor is around 2.7.


                    BitArray collectionIntersect = (BitArray)sGItem.Value.Clone();
                    BitArray item2 = (BitArray)pGItem.Value.Clone();
                    collectionIntersect.And(item2);
                    var isTrue = false;
                    foreach (bool bit in collectionIntersect)
                    {
                        if (bit)
                        {
                            isTrue = true;
                            break;
                        }
                    }
                    if (isTrue)
                    {
                        int sum = 0;
                        int index = 0;
                        foreach (bool item in collectionIntersect)
                        {
                            if (item)
                                sum += revenue[index - 1];
                            index++;
                        }
                        finalTable.Add(sGItem.Key + ", " + pGItem.Key + ", " + sum);
                    }


                }
            }
            long memoryUsed = GC.GetTotalMemory(true) - memoryStart;
            sw.Stop();

            //foreach (var item in finalTable)
            //{
            //    Console.WriteLine(item);
            //}

            Console.WriteLine("==============================================");
            Console.WriteLine("[ABC_V4] Memory Used: " + memoryUsed + ", Total:" + finalTable.Count);
            Console.WriteLine("[ABC_V4] Time Elaspsed: " + sw.ElapsedMilliseconds + " ms");
        }

        /// <summary>
        /// Failed Experiment
        /// 
        /// </summary>
        public void ABC_V5()
        {
            long memoryStart = GC.GetTotalMemory(true);
            Stopwatch sw = new Stopwatch();
            sw.Start();

            BitArray storeBA = new BitArray(storeId.Count + 1);
            int storeIndex = 1;
            foreach (var type in storeType)
            {
                if (type == 'a')
                {
                    storeBA.Set(storeIndex, true);
                }
                storeIndex++;
            }

            BitArray productBA = new BitArray(productId.Count + 1);
            int productIndex = 1;
            foreach (var type in productType)
            {
                if (type == 'b')
                {
                    productBA.Set(productIndex, true);
                }
                productIndex++;
            }

            Dictionary<int, HashSet<int>> storeGroupDict = new Dictionary<int, HashSet<int>>();
            int scounter = 0;
            foreach (var sid in sID)
            {
                if (storeBA[sid])
                {
                    //string sName = storeName[sid - 1];
                    HashSet<int> coll = null;
                    if (storeGroupDict.TryGetValue(sid, out coll))
                    {
                        coll.Add(scounter + 1);
                        storeGroupDict[sid] = coll;
                    }
                    else
                    {
                        coll = new HashSet<int>();
                        coll.Add(scounter + 1);
                        storeGroupDict.Add(sid, coll);
                    }
                }
                scounter++;
            }

            Dictionary<int, HashSet<int>> productGroupDict = new Dictionary<int, HashSet<int>>();
            int pcounter = 0;
            foreach (var pid in pID)
            {
                if (productBA[pid])
                {
                    // string pName = productName[pid - 1];
                    HashSet<int> coll = null;
                    if (productGroupDict.TryGetValue(pid, out coll))
                    {
                        coll.Add(pcounter + 1);
                        productGroupDict[pid] = coll;
                    }
                    else
                    {
                        coll = new HashSet<int>();
                        coll.Add(pcounter + 1);
                        productGroupDict.Add(pid, coll);
                    }
                }
                pcounter++;
            }

            List<string> finalTable = new List<string>();
            foreach (var sGItem in storeGroupDict)
            {
                foreach (var pGItem in productGroupDict)
                {
                    // http://codebetter.com/patricksmacchia/2011/06/16/linq-intersect-2-7x-faster-with-hashset/
                    //                    If the HashSet < T > is bigger than the other sequence, gains factor can be high (like 15x).
                    //If the HashSet < T > is smaller than the other sequence, gains factor tends to 1x.
                    //   If the HashSet<T> size is comparable to the other sequence size, the gain factor is around 2.7.

                    var collectionIntersect = sGItem.Value.Count > pGItem.Value.Count ? sGItem.Value.Intersect(pGItem.Value) : pGItem.Value.Intersect(sGItem.Value);
                    if (collectionIntersect.Count() > 0)
                    {
                        int sum = 0;
                        foreach (var item in collectionIntersect)
                        {
                            sum += revenue[item - 1];
                        }
                        finalTable.Add(storeName[sGItem.Key - 1] + ", " + productName[pGItem.Key - 1] + ", " + sum);
                    }
                }
            }
            long memoryUsed = GC.GetTotalMemory(true) - memoryStart;
            sw.Stop();

            //foreach (var item in finalTable)
            //{
            //    Console.WriteLine(item);
            //}

            Console.WriteLine("==============================================");
            Console.WriteLine("[ABC_V5] Memory Used: " + memoryUsed + ", Total:" + finalTable.Count);
            Console.WriteLine("[ABC_V5] Time Elaspsed: " + sw.ElapsedMilliseconds + " ms");
        }

        public void GroupByAttributeSameAsJoinAttribute()
        {
            long memoryStart = GC.GetTotalMemory(true);
            Stopwatch sw = new Stopwatch();
            sw.Start();

            Dictionary<int, List<Tuple<int, int>>> storeDictionary = new Dictionary<int, List<Tuple<int, int>>>();
            int indexSID = 0;
            foreach (var id in sID)
            {
                List<Tuple<int, int>> value = new List<Tuple<int, int>>();
                if (!storeDictionary.TryGetValue(id, out value))
                {
                    List<Tuple<int, int>> item = new List<Tuple<int, int>>();
                    item.Add(Tuple.Create(indexSID + 1, revenue[indexSID]));
                    storeDictionary.Add(id, item);
                }
                else
                {
                    value.Add(Tuple.Create(indexSID + 1, revenue[indexSID]));
                }
                indexSID++;
            }

            Dictionary<int, List<Tuple<int, int>>> productDictionary = new Dictionary<int, List<Tuple<int, int>>>();
            int indexPID = 0;
            foreach (var id in pID)
            {
                List<Tuple<int, int>> value = new List<Tuple<int, int>>();
                if (!productDictionary.TryGetValue(id, out value))
                {
                    List<Tuple<int, int>> item = new List<Tuple<int, int>>();
                    item.Add(Tuple.Create(indexPID + 1, revenue[indexPID]));
                    productDictionary.Add(id, item);
                }
                else
                {
                    value.Add(Tuple.Create(indexPID + 1, revenue[indexPID]));
                }
                indexPID++;

            }

            HashSet<int> storeSet = new HashSet<int>();
            int indexStoreType = 1;
            foreach (var type in storeType)
            {
                if (type.Equals('a')) // search condition
                {
                    storeSet.Add(indexStoreType);
                }
                indexStoreType++;
            }

            HashSet<int> productSet = new HashSet<int>();
            int indexProductType = 1;
            foreach (var type in productType)
            {
                if (type.Equals('b')) // search condition
                {
                    productSet.Add(indexProductType);
                }
                indexProductType++;
            }

            var tempStoreDict = new Dictionary<int, List<Tuple<int, int>>>(storeDictionary);
            foreach (var item in tempStoreDict)
            {
                if (!storeSet.Contains(item.Key))
                {
                    storeDictionary.Remove(item.Key);
                }

            }

            var tempProductDict = new Dictionary<int, List<Tuple<int, int>>>(productDictionary);
            foreach (var item in tempProductDict)
            {
                if (!productSet.Contains(item.Key))
                {
                    productDictionary.Remove(item.Key);
                }

            }

            List<String> finalTable = new List<string>();
            // Serial part in the algorithm
            foreach (var _store in storeDictionary)
            {
                foreach (var _product in productDictionary)
                {
                    var collectionIntersect = _store.Value.Intersect(_product.Value);
                    if (collectionIntersect.Count() > 0)
                    {
                        int sum = 0;
                        foreach (var item in collectionIntersect)
                        {
                            sum += item.Item2;
                        }
                        finalTable.Add(_store.Key + ", " + _product.Key + ", " + sum);
                    }
                }
            }
            long memoryUsed = GC.GetTotalMemory(true) - memoryStart;
            sw.Stop();

            //foreach (var item in finalTable)
            //{
            //    Console.WriteLine(item);
            //}

            Console.WriteLine("==============================================");
            Console.WriteLine("[GASJA] Memory Used: " + memoryUsed);
            Console.WriteLine("[GASJA] Time Elaspsed: " + sw.ElapsedMilliseconds + " ms");
        }

        public void ParallelGroupByAttributeSameAsJoinAttribute()
        {
            long memoryStart = GC.GetTotalMemory(true);
            Stopwatch sw = new Stopwatch();
            sw.Start();

            Dictionary<int, List<Tuple<int, int>>> storeDictionary = new Dictionary<int, List<Tuple<int, int>>>();
            int indexSID = 0;

            Dictionary<int, List<Tuple<int, int>>> productDictionary = new Dictionary<int, List<Tuple<int, int>>>();
            int indexPID = 0;

            HashSet<int> storeSet = new HashSet<int>();
            int indexStoreType = 1;

            HashSet<int> productSet = new HashSet<int>();
            int indexProductType = 1;

            Parallel.Invoke(po, () =>
            {
                foreach (var id in sID)
                {
                    List<Tuple<int, int>> value = new List<Tuple<int, int>>();
                    if (!storeDictionary.TryGetValue(id, out value))
                    {
                        List<Tuple<int, int>> item = new List<Tuple<int, int>>();
                        item.Add(Tuple.Create(indexSID + 1, revenue[indexSID]));
                        storeDictionary.Add(id, item);
                    }
                    else
                    {
                        value.Add(Tuple.Create(indexSID + 1, revenue[indexSID]));
                    }
                    indexSID++;
                }
            },
            () =>
            {
                foreach (var id in pID)
                {
                    List<Tuple<int, int>> value = new List<Tuple<int, int>>();
                    if (!productDictionary.TryGetValue(id, out value))
                    {
                        List<Tuple<int, int>> item = new List<Tuple<int, int>>();
                        item.Add(Tuple.Create(indexPID + 1, revenue[indexPID]));
                        productDictionary.Add(id, item);
                    }
                    else
                    {
                        value.Add(Tuple.Create(indexPID + 1, revenue[indexPID]));
                    }
                    indexPID++;

                }
            }, () =>
            {
                foreach (var type in storeType)
                {
                    if (type.Equals('a')) // search condition
                    {
                        storeSet.Add(indexStoreType);
                    }
                    indexStoreType++;
                }
            }, () =>
            {
                foreach (var type in productType)
                {
                    if (type.Equals('b')) // search condition
                    {
                        productSet.Add(indexProductType);
                    }
                    indexProductType++;
                }
            }

                );

            Parallel.Invoke(po, () =>
            {
                var tempStoreDict = new Dictionary<int, List<Tuple<int, int>>>(storeDictionary);
                foreach (var item in tempStoreDict)
                {
                    if (!storeSet.Contains(item.Key))
                    {
                        storeDictionary.Remove(item.Key);
                    }

                }
            }, () =>
            {
                var tempProductDict = new Dictionary<int, List<Tuple<int, int>>>(productDictionary);
                foreach (var item in tempProductDict)
                {
                    if (!productSet.Contains(item.Key))
                    {
                        productDictionary.Remove(item.Key);
                    }

                }
            });


            List<String> finalTable = new List<string>();
            // Serial part in the algorithm
            foreach (var _store in storeDictionary)
            {
                foreach (var _product in productDictionary)
                {
                    var collectionIntersect = _store.Value.Intersect(_product.Value);
                    if (collectionIntersect.Count() > 0)
                    {
                        int sum = 0;
                        foreach (var item in collectionIntersect)
                        {
                            sum += item.Item2;
                        }
                        finalTable.Add(_store.Key + ", " + _product.Key + ", " + sum);
                    }
                }
            }
            long memoryUsed = GC.GetTotalMemory(true) - memoryStart;
            sw.Stop();

            //foreach (var item in finalTable)
            //{
            //    Console.WriteLine(item);
            //}

            Console.WriteLine("==============================================");
            Console.WriteLine("[PGASJA] Memory Used: " + memoryUsed);
            Console.WriteLine("[PGASJA] Time Elaspsed: " + sw.ElapsedMilliseconds + " ms");
        }

        public void GroupByAttributeDifferentFromJoinAttribute()
        {
            long memoryStart = GC.GetTotalMemory(true);
            Stopwatch sw = new Stopwatch();
            sw.Start();

            Dictionary<int, string> storeDict = new Dictionary<int, string>();
            int storeIndex = 0;
            foreach (var type in storeType)
            {
                if (type == 'a')
                {
                    string sName = storeName[storeIndex];
                    storeDict.Add(storeIndex + 1, sName);
                }
                storeIndex++;
            }

            Dictionary<int, string> productDict = new Dictionary<int, string>();
            int productIndex = 0;
            foreach (var type in productType)
            {
                if (type == 'b')
                {
                    string pName = productName[productIndex];
                    productDict.Add(productIndex + 1, pName);
                }
                productIndex++;
            }

            Dictionary<string, List<Tuple<int, int>>> storeGroupDict = new Dictionary<string, List<Tuple<int, int>>>();
            int scounter = 0;
            foreach (var sid in sID)
            {
                string storeName = string.Empty;
                if (storeDict.TryGetValue(sid, out storeName))
                {
                    List<Tuple<int, int>> listAggColl = null;
                    if (storeGroupDict.TryGetValue(storeName, out listAggColl))
                    {
                        listAggColl.Add(Tuple.Create(scounter + 1, revenue[scounter]));
                        storeGroupDict[storeName] = listAggColl;
                    }
                    else
                    {
                        listAggColl = new List<Tuple<int, int>>();
                        listAggColl.Add(Tuple.Create(scounter + 1, revenue[scounter]));
                        storeGroupDict.Add(storeName, listAggColl);
                    }
                }
                scounter++;
            }

            Dictionary<string, List<Tuple<int, int>>> productGroupDict = new Dictionary<string, List<Tuple<int, int>>>();
            int pcounter = 0;
            foreach (var pid in pID)
            {
                string productName = string.Empty;
                if (productDict.TryGetValue(pid, out productName))
                {
                    List<Tuple<int, int>> listAggColl = null;
                    if (productGroupDict.TryGetValue(productName, out listAggColl))
                    {
                        listAggColl.Add(Tuple.Create(pcounter + 1, revenue[pcounter]));
                        productGroupDict[productName] = listAggColl;
                    }
                    else
                    {
                        listAggColl = new List<Tuple<int, int>>();
                        listAggColl.Add(Tuple.Create(pcounter + 1, revenue[pcounter]));
                        productGroupDict.Add(productName, listAggColl);
                    }
                }
                pcounter++;
            }

            List<string> finalTable = new List<string>();
            foreach (var sGItem in storeGroupDict)
            {
                foreach (var pGItem in productGroupDict)
                {
                    var collectionIntersect = sGItem.Value.Intersect(pGItem.Value);
                    if (collectionIntersect.Count() > 0)
                    {
                        int sum = 0;
                        foreach (var item in collectionIntersect)
                        {
                            sum += item.Item2;
                        }
                        finalTable.Add(sGItem.Key + ", " + pGItem.Key + ", " + sum);
                    }
                }
            }
            long memoryUsed = GC.GetTotalMemory(true) - memoryStart;
            sw.Stop();

            //foreach (var item in finalTable)
            //{
            //    Console.WriteLine(item);
            //}

            Console.WriteLine("==============================================");
            Console.WriteLine("[GADJA] Memory Used: " + memoryUsed);
            Console.WriteLine("[GADJA] Time Elaspsed: " + sw.ElapsedMilliseconds + " ms");

        }

        public void ParallelGroupByAttributeDifferentFromJoinAttribute()
        {
            long memoryStart = GC.GetTotalMemory(true);
            Stopwatch sw = new Stopwatch();
            sw.Start();

            Dictionary<int, string> storeDict = new Dictionary<int, string>();
            int storeIndex = 0;

            Dictionary<int, string> productDict = new Dictionary<int, string>();
            int productIndex = 0;

            Parallel.Invoke(po, () =>
            {
                foreach (var type in storeType)
                {
                    if (type == 'a')
                    {
                        string sName = storeName[storeIndex];
                        storeDict.Add(storeIndex + 1, sName);
                    }
                    storeIndex++;
                }
            }, () =>
            {
                foreach (var type in productType)
                {
                    if (type == 'b')
                    {
                        string pName = productName[productIndex];
                        productDict.Add(productIndex + 1, pName);
                    }
                    productIndex++;
                }
            });

            Dictionary<string, List<Tuple<int, int>>> storeGroupDict = new Dictionary<string, List<Tuple<int, int>>>();
            int scounter = 0;

            Dictionary<string, List<Tuple<int, int>>> productGroupDict = new Dictionary<string, List<Tuple<int, int>>>();
            int pcounter = 0;

            Parallel.Invoke(po, () =>
            {
                foreach (var sid in sID)
                {
                    string storeName = string.Empty;
                    if (storeDict.TryGetValue(sid, out storeName))
                    {
                        List<Tuple<int, int>> listAggColl = null;
                        if (storeGroupDict.TryGetValue(storeName, out listAggColl))
                        {
                            listAggColl.Add(Tuple.Create(scounter + 1, revenue[scounter]));
                            storeGroupDict[storeName] = listAggColl;
                        }
                        else
                        {
                            listAggColl = new List<Tuple<int, int>>();
                            listAggColl.Add(Tuple.Create(scounter + 1, revenue[scounter]));
                            storeGroupDict.Add(storeName, listAggColl);
                        }
                    }
                    scounter++;
                }
            }, () =>
            {
                foreach (var pid in pID)
                {
                    string productName = string.Empty;
                    if (productDict.TryGetValue(pid, out productName))
                    {
                        List<Tuple<int, int>> listAggColl = null;
                        if (productGroupDict.TryGetValue(productName, out listAggColl))
                        {
                            listAggColl.Add(Tuple.Create(pcounter + 1, revenue[pcounter]));
                            productGroupDict[productName] = listAggColl;
                        }
                        else
                        {
                            listAggColl = new List<Tuple<int, int>>();
                            listAggColl.Add(Tuple.Create(pcounter + 1, revenue[pcounter]));
                            productGroupDict.Add(productName, listAggColl);
                        }
                    }
                    pcounter++;
                }
            });

            List<string> finalTable = new List<string>();
            foreach (var sGItem in storeGroupDict)
            {
                foreach (var pGItem in productGroupDict)
                {
                    var collectionIntersect = sGItem.Value.Intersect(pGItem.Value);
                    if (collectionIntersect.Count() > 0)
                    {
                        int sum = 0;
                        foreach (var item in collectionIntersect)
                        {
                            sum += item.Item2;
                        }
                        finalTable.Add(sGItem.Key + ", " + pGItem.Key + ", " + sum);
                    }
                }
            }
            long memoryUsed = GC.GetTotalMemory(true) - memoryStart;
            sw.Stop();

            //foreach (var item in finalTable)
            //{
            //    Console.WriteLine(item);
            //}

            Console.WriteLine("==============================================");
            Console.WriteLine("[PGADJA] Memory Used: " + memoryUsed);
            Console.WriteLine("[PGADJA] Time Elaspsed: " + sw.ElapsedMilliseconds + " ms");

        }

        public int getRandomInt(int minimum, int maximum)
        {
            return rand.Next(minimum, maximum);
        }

        public char getRandomLetter()
        {
            int num = getRandomInt(0, 26);
            char let = Convert.ToChar('a' + num);
            return let;
        }
    }

}
