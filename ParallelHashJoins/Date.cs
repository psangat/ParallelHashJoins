using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace ParallelHashJoins
{
    [Serializable]
    class Date
    {
        public Int64 dDateKey { get; set; }
        public string dDate { get; set; }
        public string dDayOfWeek { get; set; }
        public string dMonth { get; set; }
        public string dYear { get; set; }
        public Int64 dYearMonthNum { get; set; }
        public string dYearMonth { get; set; }
        public Int64 dDayNumInWeek { get; set; }
        public Int64 dDayNumInMonth { get; set; }
        public Int64 dDayNumInYear { get; set; }
        public Int64 dMonthNumInYear { get; set; }
        public Int64 dWeekNumInYear { get; set; }
        public string dSellingSeason { get; set; }
        public Int64 dLastDayInWeekFL { get; set; }
        public Int64 dLastDayInMonthFL { get; set; }
        public Int64 dHolidayFL { get; set; }
        public Int64 dWeekDayFL { get; set; }

        private string dbPath;

        public Date()
        {

        }

        public Date(Int64 dDateKey, 
            string dDate, 
            string dDayOfWeek, 
            string dMonth,
            string dYear, 
            Int64 dYearMonthNum, 
            string dYearMonth, 
            Int64 dDayNumInWeek, 
            Int64 dDayNumInMonth,
            Int64 dDateNumInYear, 
            Int64 dMonthNumInYear, 
            Int64 dWeekNumInYear, 
            string dSellingSeason,
            Int64 dLastDayInWeekFL, 
            Int64 dLastDayInMonthFL, 
            Int64 dHolidayFL, 
            Int64 dWeekDayFL)
        {
            this.dDateKey = dDateKey;
            this.dDate = dDate;
            this.dDayOfWeek = dDayOfWeek;
            this.dMonth = dMonth;
            this.dYear = dYear;
            this.dYearMonthNum = dYearMonthNum;
            this.dYearMonth = dYearMonth;
            this.dDayNumInWeek = dDayNumInWeek;
            this.dDayNumInMonth = dDayNumInMonth;
            this.dDayNumInYear = dDayNumInYear;
            this.dMonthNumInYear = dMonthNumInYear;
            this.dWeekNumInYear = dWeekNumInYear;
            this.dSellingSeason = dSellingSeason;
            this.dLastDayInWeekFL = dLastDayInWeekFL;
            this.dLastDayInMonthFL = dLastDayInMonthFL;
            this.dHolidayFL = dHolidayFL;
            this.dWeekDayFL = dWeekDayFL;
        }
    }
}
