using BigQuery.EntityFramework.Core.DataAnnotations;
using BigQuery.EntityFramework.Core.Linq;
using BigQuery.EntityFramework.Core.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.EntityFramework.Core
{
    public interface IUpdateBigQueryable<T> : IAssignBigQueryable<T>, IExecutableQueueBigQueryable
    {
    }

    internal class UpdateBigQueryable<T> : ExecutableQueueBigQueryable, IUpdateBigQueryable<T>
    {
        internal override int Order
        {
            get { return 1; }
        }

        internal UpdateBigQueryable(IBigQueryable parent)
            : base(parent)
        {
        }

        public override string BuildQueryString(int depth)
        {
            return "UPDATE"
                + Environment.NewLine
                + Indent(depth)
                + TableNameAttributeHelper.GetTableName<T>()
                + Environment.NewLine
                + "SET"
                + Environment.NewLine
                + Parent.ToString();
        }
    }
}
