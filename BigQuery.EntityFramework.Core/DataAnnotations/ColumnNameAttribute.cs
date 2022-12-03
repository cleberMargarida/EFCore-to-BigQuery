using System;

namespace BigQuery.EntityFramework.Core.DataAnnotations
{
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = false)]
    public class ColumnNameAttribute : Attribute
    {
        public string ColumnName { get; private set; }

        public ColumnNameAttribute(string columnName)
        {
            ColumnName = columnName;
        }
    }
}
