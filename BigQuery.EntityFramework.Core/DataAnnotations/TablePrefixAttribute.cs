using System;

namespace BigQuery.EntityFramework.Core.DataAnnotations
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public class TablePrefixAttribute : Attribute
    {
        public string TablePrefix { get; private set; }

        public TablePrefixAttribute(string tablePrefix)
        {
            TablePrefix = tablePrefix;
        }
    }
}
