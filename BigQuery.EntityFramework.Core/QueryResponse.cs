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
            var single = results.FirstOrDefault();
            return (T)Convert.ChangeType(single.RawRow.F[0].V, typeof(T));
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
            Type type = typeof(T);
            T instance = Activator.CreateInstance<T>();
            var properties = type.GetProperties();

            foreach (var property in properties) property.SetValue(instance,
                                                 Convert.ChangeType(row[property.Name.ToLower()],
                                                 property.PropertyType));

            return instance;
        }
    }
}