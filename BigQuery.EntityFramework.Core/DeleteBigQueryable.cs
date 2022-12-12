using BigQuery.EntityFramework.Core.Linq;
using System.Collections.Generic;
using System.Text;

namespace BigQuery.EntityFramework.Core
{
    public interface IDeleteBigQueryable<T> : IBigQueryable, IExecutableQueueBigQueryable
    {
    }

    internal class DeleteBigQueryable<T> : ExecutableQueueBigQueryable, IDeleteBigQueryable<T>
    {
        internal override int Order => 1;

        public DeleteBigQueryable(IBigQueryable parent) : base(parent)
        {
        }


        public override string BuildQueryString(int depth)
        {
            return "DELETE " + Parent?.ToString() + QueryEnd;
        }
    }
}
