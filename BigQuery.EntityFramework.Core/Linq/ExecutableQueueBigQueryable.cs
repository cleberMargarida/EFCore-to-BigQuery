using BigQuery.EntityFramework.Core.Linq;
using Google.Apis.Bigquery.v2;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace BigQuery.EntityFramework.Core.Linq
{
    public interface IExecutableQueueBigQueryable : IBigQueryable
    {
        void Enqueue(IBigQueryable queryable);
        Task RunAsync();

        public static IExecutableQueueBigQueryable operator +(IExecutableQueueBigQueryable a, IExecutableQueueBigQueryable b)
        {
            if (a is not null) a.Enqueue(b);
            return a ?? b;
        }
    }

    internal abstract class ExecutableQueueBigQueryable : BigQueryable, IExecutableQueueBigQueryable
    {
        private Queue<IBigQueryable> queue = new();

        public ExecutableQueueBigQueryable(IBigQueryable parent) : base(parent)
        {
        }

        public void Enqueue(IBigQueryable queryable)
        {
            queue.Enqueue(queryable);
        }

        public async Task RunAsync()
        {
            var query = BuildString();
            if (string.IsNullOrWhiteSpace(query)) { return; }
            await QueryContext.RunAsync(query);
        }

        public string BuildString()
        {
            string query = ToString() + "\n";
            while (queue.Any()) query += queue.Dequeue().ToString() + "\n";
            return query;
        }

    }
}
