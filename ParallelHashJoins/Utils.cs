using Microsoft.VisualBasic.Devices;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace ParallelHashJoins
{
    public static class Utils
    {
        public static float getAvailableRAM()
        {
            return new ComputerInfo().TotalPhysicalMemory;
        }
        public static string convertObjectToCSVString(this Object value)
        {
            PropertyInfo[] properties = value.GetType().GetProperties();
            List<string> lst = new List<string>();
            foreach (PropertyInfo property in properties)
            {
                lst.Add(property.GetValue(value).ToString());
            }
            return String.Join(",", lst);
        }

        public static List<List<T>> ChunkBy<T>(this List<T> source, int chunkSize)
        {
            return source
                .Select((x, i) => new { Index = i, Value = x })
                .GroupBy(x => x.Index / chunkSize)
                .Select(x => x.Select(v => v.Value).ToList())
                .ToList();
        }

        public static Dictionary<int, string> getSmallestDictionary(List<Dictionary<int, string>> listOfDictionaries)
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


        /// <summary>
        /// Writes the given object instance to a binary file.
        /// <para>Object type (and all child types) must be decorated with the [Serializable] attribute.</para>
        /// <para>To prevent a variable from being serialized, decorate it with the [NonSerialized] attribute; cannot be applied to properties.</para>
        /// </summary>
        /// <typeparam name="T">The type of object being written to the XML file.</typeparam>
        /// <param name="filePath">The file path to write the object instance to.</param>
        /// <param name="objectToWrite">The object instance to write to the XML file.</param>
        /// <param name="append">If false the file will be overwritten if it already exists. If true the contents will be appended to the file.</param>
        public static void WriteToBinaryFile<T>(string directoryPath, string filePath, T objectToWrite, bool append = false)
        {
            if (!Directory.Exists(directoryPath)) {
                Directory.CreateDirectory(directoryPath);
            }
            using (Stream stream = File.Open(filePath, append ? FileMode.Append : FileMode.Create))
            {
                var binaryFormatter = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();
                binaryFormatter.Serialize(stream, objectToWrite);
            }
        }

        public static void writeToBinaryFilesInChunks<T>(List<List<T>> chunkedList, string directoryName)
        {
            int i = 0;
            foreach (var chunk in chunkedList)
            {
                string filePath = Path.Combine(directoryName, String.Format("{0}.bin", i));
                WriteToBinaryFile<List<T>>(directoryName, filePath, chunk);
                i++;
            }
        }

        /// <summary>
        /// Reads an object instance from a binary file.
        /// </summary>
        /// <typeparam name="T">The type of object to read from the XML.</typeparam>
        /// <param name="filePath">The file path to read the object instance from.</param>
        /// <returns>Returns a new instance of the object read from the binary file.</returns>
        public static T ReadFromBinaryFile<T>(string filePath)
        {
            using (Stream stream = File.Open(filePath, FileMode.Open))
            {
                var binaryFormatter = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();
                return (T)binaryFormatter.Deserialize(stream);
            }
        }

        public static List<T> ReadFromBinaryFiles<T>(string filePath)
        {
            List<T> list = new List<T>();
            foreach (var file in Directory.EnumerateFiles(filePath, "*.bin"))
            {
                list.AddRange(ReadFromBinaryFile<List<T>>(file));
            }
            return list;
        }

        /// <summary>
        /// Returns the mutually exclusive boundaries
        /// </summary>
        /// <param name="total"></param>
        /// <param name="processorCount"></param>
        /// <returns></returns>
        public static List<Tuple<Int32, Int32>> getPartitionIndexes(Int32 total, int processorCount)
        {
            List<Tuple<Int32, Int32>> boundaries = new List<Tuple<Int32, Int32>>();
            Int32 min = 0;
            Int32 max = total / processorCount;

            for (int i = 0; i < processorCount; i++)
            {
                if (i == processorCount - 1)
                {
                    max = total - 1;
                }

                boundaries.Add(new Tuple<Int32, Int32>(min, max));
                min = max + 1;
                max = max + total/processorCount;

            }
            return boundaries;

        }
    }
}
