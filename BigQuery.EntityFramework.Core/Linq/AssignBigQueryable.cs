using BigQuery.EntityFramework.Core.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace BigQuery.EntityFramework.Core.Linq
{
    public interface IAssignBigQueryable<T> : IBigQueryable
    {
    }

    internal class AssignBigQueryable<T> : BigQueryable, IAssignBigQueryable<T>
    {
        private readonly T entity;

        internal override int Order
        {
            get { return 1; }
        }

        internal AssignBigQueryable(IBigQueryable parent, T entity)
            : base(parent)
        {
            this.entity = entity;
        }

        public override string BuildQueryString(int depth)
        {
            string command = string.Empty;
            foreach (var expression in ExpressionHelper.GetCompareFromAllProperties(entity))
                command += Indent(depth) +
                BigQueryTranslateAssignVisitor.BuildQuery(depth + 1, QueryContext.IndentSize, expression) +
                Environment.NewLine;

            command = command.RemoveLast(',');

            return command + Environment.NewLine + Parent?.ToString();
        }
    }
}
