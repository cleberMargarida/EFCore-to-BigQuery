﻿using BigQuery.EntityFramework.Core.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.EntityFramework.Core.Query
{
    internal class HavingBigQueryable<TSource> : ExecutableBigQueryableBase<TSource>, IHavingBigQueryable<TSource>
    {
        readonly Expression<Func<TSource, bool>> predicate;
        internal override int Order
        {
            get { return 5; }
        }

        internal HavingBigQueryable(IBigQueryable parent, Expression<Func<TSource, bool>> predicate)
            : base(parent)
        {
            this.predicate = predicate;
        }

        public override string BuildQueryString(int depth)
        {
            var command = BigQueryTranslateVisitor.BuildQuery(depth + 1, QueryContext.IndentSize, predicate);

            var sb = new StringBuilder();
            sb.Append(Indent(depth));
            sb.AppendLine("HAVING");
            sb.Append(Indent(depth + 1));
            sb.Append(command);

            return sb.ToString();
        }
    }

}
