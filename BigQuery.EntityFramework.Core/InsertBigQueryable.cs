using BigQuery.EntityFramework.Core.DataAnnotations;
using BigQuery.EntityFramework.Core.Linq;
using BigQuery.EntityFramework.Core.Utils;
using System;

namespace BigQuery.EntityFramework.Core
{
    public interface IInsertBigQueryable<T> : IBigQueryable, IExecutableQueueBigQueryable
    {
    }

    internal class InsertBigQueryable<T> : ExecutableQueueBigQueryable, IInsertBigQueryable<T>
    {
        private T entity;

        public InsertBigQueryable(IWhereBigQueryable<T> parent, T entity) : base(parent)
        {
            this.entity = entity;
        }

        internal override int Order => 1;

        public override string BuildQueryString(int depth)
        {
            var tuple = BigQueryTranslateInsertVisitor.BuildColumnValue(1, 1, ExpressionHelper.GetEqualForAllProperties(entity));

            string query = "INSERT INTO" + Environment.NewLine +
                                Indent(1) + TableNameAttributeHelper.GetTableName<T>() + $" ({tuple.Item1})" +
                                Environment.NewLine +
                                "VALUES" + Environment.NewLine +
                                Indent(1) + $" ({tuple.Item2})";
            return query;
        }
    }
}
