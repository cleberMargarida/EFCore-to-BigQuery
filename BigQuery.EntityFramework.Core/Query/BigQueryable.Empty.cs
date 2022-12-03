using BigQuery.EntityFramework.Core.Linq;
using System;
using System.Collections.Generic;
using System.Text;

namespace BigQuery.EntityFramework.Core.Query
{
    internal class EmptyBigQueryable<TSource> : BigQueryable, IWhereBigQueryable<TSource>
    {
        public EmptyBigQueryable(IBigQueryable parent) : base(parent) { }

        internal override int Order => 0;

        public override string BuildQueryString(int depth)
        {
            return string.Empty;
        }
    }
}
