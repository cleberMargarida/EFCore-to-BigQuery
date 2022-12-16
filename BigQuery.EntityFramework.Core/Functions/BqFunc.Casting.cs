using BigQuery.EntityFramework.Core.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.EntityFramework.Core
{
    public static partial class BqFunc
    {
        // https://developers.google.com/bigquery/query-reference#castingfunctions
        // Casting functions change the data type of a numeric expression.
        // Casting functions are particularly useful for ensuring that arguments in a comparison function have the same data type.

        /// <summary>
        /// Converts expr into a variable of type type.
        /// </summary>
        [FunctionName("SAFE_CAST", SpecifiedFormatterType = typeof(CastFormatter))]
        public static T Cast<T>(object expr) { throw Invalid(); }

        class CastFormatter : ISpecifiedFormatter
        {
            public string Format(int depth, int indentSize, string functionName, MethodCallExpression node)
            {
                var type = node.Method.GetGenericArguments()[0];
                var dataType = DataTypeUtility.ToDataType(type);
                var identifier = dataType.ToIdentifier();

                var innerTranslator = new BigQueryTranslateVisitor();
                var expr = innerTranslator.VisitAndClearBuffer(node.Arguments[0]);

                return $"{functionName}({expr} AS {identifier})";
            }
        }
    }
}
