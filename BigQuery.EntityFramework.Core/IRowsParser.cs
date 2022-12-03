using System;
using System.Collections.Generic;
using BigQuery.EntityFramework.Core.Utils;
using Google.Apis.Bigquery.v2.Data;

namespace BigQuery.EntityFramework.Core
{
    /// <summary>
    /// Parses rows as a specified type
    /// </summary>
    public interface IRowsParser
    {
        IEnumerable<T> Parse<T>(
            Dictionary<Type, CustomDeserializeFallback> fallbacks,
            bool isConvertResultUtcToLocalTime,
            TableSchema schema,
            IEnumerable<TableRow> rows,
            bool isDynamic);
    }
}