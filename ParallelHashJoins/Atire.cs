using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParallelHashJoins
{
    class Atire
    {
        public int value { get; set; }

        private List<string> attributes = new List<string>();

        public Dictionary<string, Atire> children { get; set; }

        public Atire()
        {
            this.value = 0;
            this.children = new Dictionary<string, Atire>();
        }

        public void Insert(Atire root, List<string> attributes, int value)
        {
            try
            {
                Atire node = root;
                foreach (var attribute in attributes)
                {
                    if (!node.children.ContainsKey(attribute))
                    {
                        node.children.Add(attribute, new Atire());
                    }
                    node = node.children[attribute];
                }
                // Store value in the terminal node
                // Aggregation on the fly
                node.value += value;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }



        public List<string> GetResults(Atire root)
        {
            List<string> finalResult = new List<string>();
            foreach (var l0 in root.children)
            {
                foreach (var l1 in l0.Value.children)
                {
                    foreach (var l2 in l1.Value.children)
                    {
                        finalResult.Add(String.Format("{0}, {1}, {2}, {3}", l0.Key, l1.Key, l2.Key, l2.Value.value));
                        Console.WriteLine(String.Format("{0}, {1}, {2}, {3}", l0.Key, l1.Key, l2.Key, l2.Value.value));
                    }
                }
            }

            //foreach (var key in root.children.Keys)
            //{
            //    attributes.Add(key);
            //    Console.Write(String.Format("{0}, ", key));
            //    var tempAtire = root.children[key];
            //    if (tempAtire.value != 0)
            //    {
            //        Console.WriteLine(String.Format("{0}", tempAtire.value));
            //        attributes.Clear();
            //        break;
            //    }
            //    GetResults(tempAtire);
            //}
            return finalResult;
        }

        public Atire MergeAtires(Atire atire1, Atire atire2)
        {
            foreach (var key in atire2.children.Keys)
            {
                attributes.Add(key);
                var tempAtire = atire2.children[key];
                if (tempAtire.value != 0)
                {
                    //Console.WriteLine(String.Format("{0},{1},{2},{3}", keys[0], keys[1], keys[2], tempAtire.value));
                    Insert(atire1, attributes, tempAtire.value);
                    attributes.Clear();
                    break;
                }
                MergeAtires(atire1, tempAtire);
            }
            return atire1;
        }
    }
}
