using BigQuery.EntityFramework.Core.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.EntityFramework.Core.Query
{
    internal class WhereBigQueryable<TSource> : ExecutableBigQueryableBase<TSource>, IWhereBigQueryable<TSource>
    {
        internal Expression<Func<TSource, bool>> Predicate { get; }
     
        internal override int Order
        {
            get { return 3; }
        }

        internal WhereBigQueryable(IBigQueryable parent, Expression<Func<TSource, bool>> predicate)
            : base(parent)
        {
            this.Predicate = predicate;
        }

        internal IWhereBigQueryable<TSource> CombineWhere(Expression<Func<TSource, bool>> condition)
        {
            var newBody = Expression.AndAlso(this.Predicate.Body, condition.Body);
            var newPredicate = Expression.Lambda<Func<TSource, bool>>(newBody, this.Predicate.Parameters);

            return new WhereBigQueryable<TSource>(Parent, newPredicate);
        }

        public override string BuildQueryString(int depth)
        {
            var command = BigQueryTranslateVisitor.BuildQuery(depth + 1, QueryContext.IndentSize, Predicate);

            var sb = new StringBuilder();
            sb.Append(Indent(depth));
            sb.AppendLine("WHERE");
            sb.Append(Indent(depth + 1));
            sb.Append(command);

            return sb.ToString();
        }
    }
}
