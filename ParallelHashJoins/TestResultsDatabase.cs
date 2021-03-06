﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParallelHashJoins
{
    class TestResultsDatabase
    {
        public static List<String> nimbleJoinOutput = new List<string>();
        public static List<String> invisibleJoinOutput = new List<string>();
        public static List<String> pNimbleJoinOutput = new List<string>();
        public static List<String> pInvisibleJoinOutput = new List<string>();
        public static List<String> pInMemoryAggregationOutput = new List<string>();
        public static List<String> pATireJoinOutputLF = new List<string>();
        public static List<String> pATireJoinOutputLC = new List<string>();

        public static void clearAllDatabase() {
            nimbleJoinOutput.Clear();
            invisibleJoinOutput.Clear();
            pNimbleJoinOutput.Clear();
            pInvisibleJoinOutput.Clear();
            pInMemoryAggregationOutput.Clear();
            pATireJoinOutputLF.Clear();
            pATireJoinOutputLC.Clear();
        }
    }
}
