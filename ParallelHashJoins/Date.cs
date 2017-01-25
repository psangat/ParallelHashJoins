using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParallelHashJoins
{
    class Date
    {
        public int dDateKey { get; set; }
        public string dDate { get; set; }
        public string dDayOfWeek { get; set; }
        public string dMonth { get; set; }
        public int dYear { get; set; }
        public int dYearMonthNum { get; set; }
        public string dYearMonth { get; set; }
        public int dDayNumInWeek { get; set; }
        public int dDayNumInMonth { get; set; }
        public int dDayNumInYear { get; set; }
        public int dMonthNumInYear { get; set; }
        public int dWeekNumInYear { get; set; }
        public string dSellingSeason { get; set; }
        public int dLastDayInWeekFL { get; set; }
        public int dLastDayInMonthFL { get; set; }
        public int dHolidayFL { get; set; }
        public int dWeekDayFL { get; set; }

        public Date(int dDateKey, 
            string dDate, 
            string dDayOfWeek, 
            string dMonth,
            int dYear, 
            int dYearMonthNum, 
            string dYearMonth, 
            int dDayNumInWeek, 
            int dDayNumInMonth,
            int dDateNumInYear, 
            int dMonthNumInYear, 
            int dWeekNumInYear, 
            string dSellingSeason,
            int dLastDayInWeekFL, 
            int dLastDayInMonthFL, 
            int dHolidayFL, 
            int dWeekDayFL)
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
