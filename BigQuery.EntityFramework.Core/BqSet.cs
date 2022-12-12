using BigQuery.EntityFramework.Core.Utils;
using Google.Apis.Bigquery.v2;
using Google.Cloud.BigQuery.V2;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;
using BigQuery.EntityFramework.Core.Query;
using BigQuery.EntityFramework.Core.Linq;
using static BigQuery.EntityFramework.Core.Utils.ExpressionHelper;
using static BigQuery.EntityFramework.Core.DataAnnotations.KeyColumnAttributeHelper;
using static BigQuery.EntityFramework.Core.DataAnnotations.TableNameAttributeHelper;

namespace BigQuery.EntityFramework.Core
{
    internal class BqSetImpl<TEntity> : BigQueryable, BqSet<TEntity>
        where TEntity : class
    {
        private IExecutableQueueBigQueryable operationsQueue;
        private static readonly DeserializerRowsParser rowsParser = new DeserializerRowsParser();

        internal override int Order => 1;

        public BqSetImpl(BigqueryService service, BigQueryClient bigQueryClient, string projectId)
            : base(new BigQueryContext(rowsParser, service, bigQueryClient, projectId))
        {
        }

        public async Task SaveAsync()
        {
            await operationsQueue.RunAsync();
        }

        #region CREATE
        public void AddRange(IEnumerable<TEntity> entities)
        {
            foreach (TEntity entity in entities) Add(entity);
        }

        public void Add(TEntity entity)
        {
            operationsQueue += this.Insert(entity);
        }
        #endregion

        #region UPDATE
        public void Update(TEntity newest)
        {
            var predicate = GetEqualForSpecificProperties(newest, GetKeyProperties<TEntity>());
            Update(predicate, newest);
        }

        public void Update<TProp>(Expression<Func<TEntity, TProp>> keyColumn, TEntity newest)
        {
            Update(GetEqualFromConstantProperty(keyColumn), newest);
        }

        public void UpdateRange(IEnumerable<TEntity> newests)
        {
            foreach (var newest in newests) Update(newest);
        }

        public void Update(Expression<Func<TEntity, bool>> predicate, TEntity newest)
        {
            var operation = new EmptyBigQueryable<TEntity>(
                            new RootBigQueryable<TEntity>(QueryContext))
                            .Where(predicate).Update(newest);

            operationsQueue += operation;
        }
        #endregion

        #region DELETE
        public void Remove(TEntity entity)
        {
            var predicate = GetEqualForSpecificProperties(entity, GetKeyProperties<TEntity>());

            Remove(predicate);
        }

        public void Remove<TProp>(Expression<Func<TEntity, TProp>> keyColumn)
        {
            Remove(GetEqualFromConstantProperty(keyColumn));
        }

        public void Remove(Expression<Func<TEntity, bool>> predicate)
        {
            var operation = QueryContext.From<TEntity>().Delete(predicate);
            operationsQueue += operation;
        }

        public void RemoveRange(IEnumerable<TEntity> entities)
        {
            foreach (var entity in entities) Remove(entity);
        }


        #endregion
        public override string BuildQueryString(int depth)
        {
            return Indent(depth) + "FROM" + Environment.NewLine +
                   Indent(depth + 1) + GetTableName<TEntity>();
        }
    }
}
