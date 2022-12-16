using Google.Apis.Bigquery.v2.Data;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Google.Apis.Bigquery.v2;
using Google.Cloud.BigQuery.V2;
using Newtonsoft.Json;
using BigQuery.EntityFramework.Core;
using BigQuery.EntityFramework.Core.Utils;
using BigQuery.EntityFramework.Core.DataAnnotations;
using System.Reflection;

namespace BigQuery.EntityFramework.Core
{
    public class QueryResponse<T>
    {
        private readonly BigQueryResults results;

        private QueryResponse(BigQueryResults results)
        {
            this.results = results;
        }

        /// <summary>
        /// Get paging result.
        /// </summary>
        public List<T> ToList()
        {
            return GetEnumerable().ToList();
        }

        /// <summary>
        /// Get paging result.
        /// </summary>
        public Task<List<T>> ToListAsync()
        {
            return Task.FromResult(GetEnumerable().ToList());
        }

        public T Single()
        {
            return Cast(results.FirstOrDefault());
        }

        public Task<T> SingleAsync()
        {
            return Task.FromResult(Single());
        }

        internal IEnumerable<T> GetEnumerable()
        {
            foreach (var result in results) yield return Cast(result);
        }

        internal static QueryResponse<T> FromResults(BigQueryResults results)
        {
            return new QueryResponse<T>(results);
        }

        internal static T Cast(BigQueryRow row)
        {
            if (!row.RawRow.F.Any(x => x.V != null))
            {
                return default;
            }

            if (typeof(T).IsPrimitiveOrString())
            {
                return (T)Convert.ChangeType(row[0], typeof(T));
            }

            if (typeof(T).IsAnonymousType())
            {
                var values = row.RawRow.F.Select(x => x.V).ToArray();
                var ctor = typeof(T).GetConstructors().First();
                var parameters = ctor.GetParameters();
                for (int i = 0; i < parameters.Length; i++)
                {
                    values[i] = Convert.ChangeType(values[i], parameters[i].ParameterType);
                }
                return (T)ctor.Invoke(values);
            }

            if (typeof(T).IsClass)
            {
                T instance = Activator.CreateInstance<T>();

                foreach (var property in typeof(T).GetProperties())
                {
                    var columnNameAttr = property.GetCustomAttribute<ColumnNameAttribute>(true);
                    var columnName = columnNameAttr?.ColumnName ?? property.Name;

                    object value = default;

                    //Record
                    string key = GetKeyFromBigQueryRow(row, typeof(T).Name, property.PropertyType.Name, columnName);

                    if (row[key] is Dictionary<string, object> dictionary)
                    {
                        value = GetValueOfDictionary(property, dictionary);
                    }
                    else
                    {
                        value = row[columnName];
                    }

                    property.SetValue(instance, Convert.ChangeType(value, property.PropertyType));
                }

                return instance;
            }

            throw new NotSupportedException($"The type {typeof(T).Name} its not convertible from bigquery.");

            static object GetValueOfDictionary(PropertyInfo propertyOrigin, Dictionary<string, object> dictionaryOrigin)
            {
                var columnNameAttr = propertyOrigin.GetCustomAttribute<ColumnNameAttribute>(true);
                var columnName = columnNameAttr?.ColumnName ?? propertyOrigin.Name;

                var keys = dictionaryOrigin
                    .Select(x => x.Key)
                    .ToArray();

                var key = keys.Where(x => (new[] { columnName, propertyOrigin.PropertyType.Name })
                    .Contains(x))
                    .FirstOrDefault();

                var propertyType = propertyOrigin.PropertyType;
                var properties = propertyType.GetProperties();

                if (properties.Length == keys?.Length)
                {
                    var obj = Activator.CreateInstance(propertyType);
                    foreach (var property in properties)
                    {
                        property.SetValue(obj, Convert.ChangeType(GetValueOfDictionary(property, dictionaryOrigin), property.PropertyType));
                    }
                    return obj;
                }
                else if (dictionaryOrigin[key] is Dictionary<string, object> dictionary)
                {
                    return GetValueOfDictionary(propertyOrigin, dictionary);
                }

                if (keys is null)
                {
                    return null;
                }

                return dictionaryOrigin[key];
            }

            static string GetKeyFromBigQueryRow(BigQueryRow row, params string[] keys)
            {
                var _fieldNameIndexMap = typeof(BigQueryRow).GetField("_fieldNameIndexMap", BindingFlags.NonPublic | BindingFlags.Instance).GetValue(row) as IDictionary<string, int>;
                return _fieldNameIndexMap.Select(x => x.Key).Where(x => keys.Contains(x)).First();
            }
        }
    }
}