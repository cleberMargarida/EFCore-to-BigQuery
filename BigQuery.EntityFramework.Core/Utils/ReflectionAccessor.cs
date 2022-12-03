using System;
using System.Linq;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace BigQuery.EntityFramework.Core.Utils
{
    internal interface IReflectionAccessor
    {
        object GetValue(object source);
    }

    internal static class ReflectionAccessor
    {
        public static IReflectionAccessor Create(MemberInfo memberInfo)
        {
            var propInfo = memberInfo as PropertyInfo;
            if (propInfo != null)
            {
                return new PropertyInfoAccessor(propInfo);
            }

            var fieldInfo = memberInfo as FieldInfo;
            if (fieldInfo != null)
            {
                return new FieldInfoAccessor(fieldInfo);
            }

            throw new ArgumentException("invalid member info:" + memberInfo.GetType());
        }

        class PropertyInfoAccessor : IReflectionAccessor
        {
            readonly MethodInfo methodInfo;

            public PropertyInfoAccessor(PropertyInfo propInfo)
            {
                methodInfo = propInfo.GetGetMethod();
            }

            public object GetValue(object source)
            {
                return methodInfo.Invoke(source, null);
            }
        }

        class FieldInfoAccessor : IReflectionAccessor
        {
            readonly FieldInfo fieldInfo;

            public FieldInfoAccessor(FieldInfo fieldInfo)
            {
                this.fieldInfo = fieldInfo;
            }

            public object GetValue(object source)
            {
                return fieldInfo.GetValue(source);
            }
        }
    }

    internal static class ExpressionHelper
    {
        public static object GetValue(Expression expression)
        {
            if (expression is ConstantExpression) return ((ConstantExpression)expression).Value;
            if (expression is NewExpression)
            {
                var expr = (NewExpression)expression;
                var parameters = expr.Arguments.Select(x => GetValue(x)).ToArray();

                return Activator.CreateInstance(expr.Constructor.DeclaringType);
            }

            var memberExpressions = new List<MemberExpression>();
            while (!(expression is ConstantExpression))
            {
                if ((expression is UnaryExpression) && (expression.NodeType == ExpressionType.Convert))
                {
                    expression = ((UnaryExpression)expression).Operand;
                    continue;
                }

                var memberExpression = (MemberExpression)expression;
                memberExpressions.Add(memberExpression);
                var nextExpression = memberExpression.Expression;
                if (nextExpression == null) break;
                expression = nextExpression;
            }

            var rootExpression = expression as ConstantExpression;
            var value = (rootExpression != null)
                    ? rootExpression.Value
                    : null;

            for (int i = memberExpressions.Count - 1; i >= 0; i--)
            {
                var expr = memberExpressions[i];

                var accessor = ReflectionAccessor.Create(expr.Member);
                value = accessor.GetValue(value);
                if (value == null) return null; // avoid null exception.
            }

            return value;
        }

        public static Expression<Func<TSource, bool>> BuildLambdaEquals<TSource>(string propertyName, object value)
        {
            ParameterExpression parameter = Expression.Parameter(typeof(TSource), "x");
            MemberExpression left = Expression.Property(parameter, propertyName);
            Expression right = Expression.Constant(value);
            BinaryExpression equals = Expression.Equal(left, right);
            LambdaExpression predicate = Expression.Lambda(equals, parameter);
            return (Expression<Func<TSource, bool>>)predicate;
        }

        public static Expression BuildLambdaPropertyAcessor<TSource>(string propertyName)
        {
            ParameterExpression parameter = Expression.Parameter(typeof(TSource), "x");
            MemberExpression property = Expression.Property(parameter, propertyName);
            return Expression.Lambda(property, parameter);
        }

        public static TProp GetValue<TSource, TProp>(Expression<Func<TSource, TProp>> lambda)
        {
            TProp propertyValue = default(TProp);
            Expression expression = lambda.Body as MemberExpression;
            MemberInfo memberInfo = (expression as MemberExpression).Member;
            while (expression is not ConstantExpression)
            {
                if (expression is MemberExpression)
                {
                    expression = (expression as MemberExpression).Expression;
                }

                if (expression is ConstantExpression)
                {
                    var displayValue = (expression as ConstantExpression).Value;
                    var classValue = (TSource)ReflectionAccessor.Create(displayValue.GetType().GetFields()[0]).GetValue(displayValue);
                    propertyValue = (TProp)ReflectionAccessor.Create(memberInfo).GetValue(classValue);
                }
            }

            return propertyValue;
        }

        public static Expression<Func<TClass, bool>> GetEqualFromConstantProperty<TClass, TProp>(Expression<Func<TClass, TProp>> id)
            where TClass : class
        {
            MemberExpression body = id.Body as MemberExpression;
            ParameterExpression parameter = Expression.Parameter(typeof(TClass), "x");
            MemberExpression left = Expression.Property(parameter, body.Member.Name);
            Expression right = body;
            BinaryExpression equals = Expression.Equal(left, right);
            LambdaExpression predicate = Expression.Lambda(equals, id.Parameters);
            return (Expression<Func<TClass, bool>>)predicate;
        }

        public static Expression<Func<TEntity, bool>> GetEqualForAllProperties<TEntity>(TEntity entity)
        {
            var predicates = (from property in typeof(TEntity).GetProperties()
                              let value = property.GetValue(entity)
                              select BuildLambdaEquals<TEntity>(property.Name, value)).ToArray();


            Expression<Func<TEntity, bool>> predicate = predicates[0];

            for (int i = 1; i < predicates.Length; i++)
            {
                var condition = predicates[i];
                var newBody = Expression.AndAlso(predicate.Body, condition.Body);
                var newPredicate = Expression.Lambda<Func<TEntity, bool>>(newBody, predicate.Parameters);
                predicate = newPredicate;
            }

            return predicate;
        }

        public static Expression<Func<TEntity, bool>> GetEqualForSpecificProperties<TEntity>(TEntity entity, params string[] properties)
        {
            var predicates = (from property in typeof(TEntity).GetProperties()
                              let value = property.GetValue(entity)
                              where properties.Contains(property.Name)
                              select BuildLambdaEquals<TEntity>(property.Name, value)).ToArray();


            Expression<Func<TEntity, bool>> predicate = predicates[0];

            for (int i = 1; i < predicates.Length; i++)
            {
                var condition = predicates[i];
                var newBody = Expression.AndAlso(predicate.Body, condition.Body);
                var newPredicate = Expression.Lambda<Func<TEntity, bool>>(newBody, predicate.Parameters);
                predicate = newPredicate;
            }

            return predicate;
        }

        public static IEnumerable<Expression<Func<TEntity, bool>>> GetCompareFromAllProperties<TEntity>(TEntity entity)
        {
            return from property in typeof(TEntity).GetProperties()
                   let value = property.GetValue(entity)
                   select BuildLambdaEquals<TEntity>(property.Name, value);
        }

        public static IEnumerable<Expression> GetAcessorFromAllProperties<TEntity>()
        {
            return from property in typeof(TEntity).GetProperties()
                   select BuildLambdaPropertyAcessor<TEntity>(property.Name);
        }
    }
}