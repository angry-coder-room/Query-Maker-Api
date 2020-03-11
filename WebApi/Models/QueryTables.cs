using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace WebApi.Models
{
    public class QueryTables
    {
        public string TTableName { get; set; }
        public string TColumnName { get; set; }
        public string TJoinKey { get; set; }
        public string TSelectKey { get; set; }
    }
}