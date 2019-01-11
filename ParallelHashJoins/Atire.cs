using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParallelHashJoins
{
    class Atire
    {
        private int value { get; set; }
        private IDictionary<string, Atire> children { get; set; }

        public Atire()
        {
            this.value = 0;
            this.children = new Dictionary<string, Atire>();
        }

        public void insert(Atire root, List<string> attributes, int value)
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

         
        public List<string> getResults(Atire root)
        {
            List<string> finalResult = new List<string>();
            foreach (var l0 in root.children)
            {
                foreach (var l1 in l0.Value.children)
                {
                    foreach (var l2 in l1.Value.children)
                    {
                        finalResult.Add(String.Format("{0}, {1}, {2}, {3}", l0.Key, l1.Key, l2.Key, l2.Value.value));
                    }
                }
            }
            return finalResult;
        }
    }
}
