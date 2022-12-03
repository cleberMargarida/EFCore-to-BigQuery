using BigQuery.EntityFramework.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.EntityFramework.Core.DataAnnotations
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public class TableAttribute : Attribute
    {
        public string TableFullName { get; private set; }

        public TableAttribute(string tableFullName)
        {
            TableFullName = tableFullName;
        }

        public TableAttribute(string projectId, string dataset, string tableName)
        {
            TableFullName = $"`{projectId}.{dataset}.{tableName}`";
        }
    }

    internal static class TableNameAttributeHelper
    {
        public static string GetTableName<T>()
        {
            var attr = typeof(T).GetCustomAttribute<TableAttribute>();
            if (attr == null) throw new ArgumentException($"{nameof(T)} should use {nameof(KeyAttribute)}.");
            return attr.TableFullName;
        }
    }

    internal static class KeyColumnAttributeHelper
    {
        public static string[] GetKeyProperties<T>()
        {
            var properties = (from prop in typeof(T).GetProperties()
                              where Attribute.IsDefined(prop, typeof(KeyAttribute))
                              select prop.Name)
                             .ToArray();

            if (properties.Any())
            {
                return properties;
            }

            throw new ArgumentException($"{nameof(T)} doesn't have any properites defined by {nameof(KeyAttribute)}.");
        }
    }
}
