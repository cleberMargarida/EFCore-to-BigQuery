using BigQuery.EntityFramework.Core.Linq;
using BigQuery.EntityFramework.Core.Query;
using System;
using System.Collections.Generic;
using System.Text;

namespace BigQuery.EntityFramework.Core.Query
{
    internal class CountBigQueryable<T> : BigQueryable, ICountBigQueryable<T>
    {
        public CountBigQueryable(IWhereBigQueryable<T> parent) : base(parent)
        {
        }

        internal override int Order => 0;

        public override string BuildQueryString(int depth)
        {
            return "SELECT COUNT(*) \n"+Parent.ToString();
        }
    }
}
