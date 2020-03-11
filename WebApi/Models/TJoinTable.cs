using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace WebApi.Models
{
    public class TJoinTable
    {
        public string TTableName { get; set; }
        public string TJoinTable1 { get; set; }
        public string TJoinTable2 { get; set; }
        public string TJoinCondition { get; set; }
    }
}