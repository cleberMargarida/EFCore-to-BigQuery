using BigQuery.EntityFramework.Core.Linq;
using System;
using System.Linq.Expressions;
using System.Reflection;

namespace BigQuery.EntityFramework.Core.Utils
{
    public static class EntityBigQueryableExtensions
    {
        public static IDeleteBigQueryable<T> Delete<T>(this IWhereBigQueryable<T> source, Expression<Func<T, bool>> condition)
        {
            return new DeleteBigQueryable<T>(source.Where(condition));
        }

        public static IUpdateBigQueryable<T> Update<T>(this IWhereBigQueryable<T> source, T newest)
        {
            return new UpdateBigQueryable<T>(new AssignBigQueryable<T>(source, newest));
        }

        public static IInsertBigQueryable<T> Insert<T>(this IWhereBigQueryable<T> source, T entity)
        {
            return new InsertBigQueryable<T>(source, entity);
        }
    }
}
