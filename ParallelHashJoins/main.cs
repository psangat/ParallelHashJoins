using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParallelHashJoins
{
    class main
    {
        static void Main(string[] args)
        {
            Algorithms ag = new Algorithms();
            for (int i = 0; i < 10; i++)
            {
                ag.InvisibleJoin();
                ag.NimbleJoin();
            }
           
            Console.ReadKey();
        }
    }
}
