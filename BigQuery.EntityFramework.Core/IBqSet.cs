using BigQuery.EntityFramework.Core.Linq;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.EntityFramework.Core
{
    public interface BqSet<TEntity> : IFromBigQueryable<TEntity>
    {
        void Add(TEntity entity);
        void AddRange(IEnumerable<TEntity> entities);
        void Remove(Expression<Func<TEntity, bool>> predicate);
        void Remove(TEntity entity);
        void Remove<TProp>(Expression<Func<TEntity, TProp>> keyColumn);
        void RemoveRange(IEnumerable<TEntity> entities);
        Task SaveAsync();
        void Update(Expression<Func<TEntity, bool>> predicate, TEntity newest);
        void Update(TEntity newest);
        void Update<TProp>(Expression<Func<TEntity, TProp>> keyColumn, TEntity newest);
        void UpdateRange(IEnumerable<TEntity> newests);
    }
}
