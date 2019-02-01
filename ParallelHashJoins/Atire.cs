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
        private int value { get; set; }

        private long[] values { get; set; }

        private int height { get; set; }

        private Dictionary<int, string> attributes = new Dictionary<int, string>();

        private List<string> mergingAttributes = new List<string>();
        private Dictionary<string, Atire> children { get; set; }

        public List<string> results = new List<string>();

        private readonly object nodeLock = new object();

        public Atire()
        {
            this.value = 0;
            this.values = new long[10];
            this.children = new Dictionary<string, Atire>();
            this.height = -1;
        }

        /// <summary>
        /// Insert the items or attributes in the Atire
        /// </summary>
        /// <param name="root">First node of the Atire</param>
        /// <param name="attributes">List of attiributes to insert into the Atire</param>
        /// <param name="value">Value associated with the grouping of the attributes</param>

        public void Insert(Atire root, List<string> attributes, bool isLockFree = true , params long[] values)
        {
            try
            {
                Atire node = root;
                int height = -1;
                if (isLockFree)
                {
                    foreach (var attribute in attributes)
                    {

                        if (!node.children.ContainsKey(attribute))
                        {
                            node.children.Add(attribute, new Atire());
                        }
                        node.height = ++height;
                        node = node.children[attribute];

                    }
                    node.height = ++height;
                    node.values = new long[values.Count()];
                    // Store value in the terminal node
                    // Aggregation on the fly
                    for (int i = 0; i < values.Count(); i++)
                    {
                        node.values[i] += values[i];
                    } 
                    
                }
                else {
                    lock (nodeLock)
                    {
                        foreach (var attribute in attributes)
                        {

                            if (!node.children.ContainsKey(attribute))
                            {
                                node.children.Add(attribute, new Atire());
                            }
                            node.height = ++height;
                            node = node.children[attribute];

                        }
                        node.height = ++height;
                        // Store value in the terminal node
                        // Aggregation on the fly
                        for (int i = 0; i < values.Count(); i++)
                        {
                            node.values[i] += values[i];
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Display the result after the grouping.
        /// </summary>
        /// <param name="root">Fully formed Atire</param>
        public void GetResults(Atire root)
        {
            if (root.children.Count == 0)
            {
                if (attributes.ContainsKey(root.height))
                {
                    attributes[root.height] = Convert.ToString(root.value);
                }
                else
                {
                    attributes.Add(root.height, Convert.ToString(root.value));
                }
                StringBuilder sb = new StringBuilder();
                foreach (var item in attributes)
                {
                    sb.Append(String.Format("{0} ", item.Value));
                }
                //Console.WriteLine(sb.ToString());
                results.Add(sb.ToString());
                return;
            }

            foreach (var child in root.children)
            {
                if (attributes.ContainsKey(root.height))
                {
                    attributes[root.height] = child.Key;
                }
                else
                {
                    attributes.Add(root.height, child.Key);
                }
                GetResults(child.Value);
            }

        }

        /// <summary>
        /// Merge LEFT Atires to get a single Atire
        /// </summary>
        /// <param name="atire1">LEFT Atire</param>
        /// <param name="atire2">RIGHT Atire to be merged into LEFT Atire</param>
        /// <returns>LEFT Aitre</returns>
        public Atire MergeAtires(Atire atire1, Atire atire2)
        {
            foreach (var key in atire2.children.Keys)
            {
                mergingAttributes.Add(key);
                var tempAtire = atire2.children[key];
                if (tempAtire.values != null)
                {
                    Insert(atire1,  mergingAttributes, true, tempAtire.values);
                    mergingAttributes.Clear();
                    break;
                }
                MergeAtires(atire1, tempAtire);
            }
            return atire1;
        }
    }
}
