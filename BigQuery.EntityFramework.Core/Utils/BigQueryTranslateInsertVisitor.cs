using BigQuery.EntityFramework;
using BigQuery.EntityFramework.Core.DataAnnotations;
using BigQuery.EntityFramework.Core.Linq;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.EntityFramework.Core.Utils
{
    // Expression to BigQuery Query Translater
    internal class BigQueryTranslateInsertVisitor : ExpressionVisitor
    {
        readonly int depth = 1;
        readonly int indentSize = 2;

        StringBuilder columnSb = new StringBuilder();
        StringBuilder valuesSb = new StringBuilder();

        public BigQueryTranslateInsertVisitor()
        {
            depth = 0;
            indentSize = 0;
        }

        public BigQueryTranslateInsertVisitor(int depth, int indentSize)
        {
            this.depth = depth;
            this.indentSize = indentSize;
        }

        // EntryPoint
        public static (string, string) BuildColumnValue(int depth, int indentSize, Expression expression)
        {
            var visitor = new BigQueryTranslateInsertVisitor(depth, indentSize);
            visitor.Visit(expression);
            string item1 = visitor.columnSb.ToString().RemoveLast(',');
            string item2 = visitor.valuesSb.ToString().RemoveLast(',');
            return (item1, item2);
        }

        string BuildIndent()
        {
            return new string(' ', indentSize * depth);
        }

        static readonly PropertyInfo now = typeof(DateTime).GetProperty("Now");
        static readonly PropertyInfo utcNow = typeof(DateTime).GetProperty("UtcNow");
        static readonly PropertyInfo nowOffset = typeof(DateTimeOffset).GetProperty("Now");
        static readonly PropertyInfo utcNowOffset = typeof(DateTimeOffset).GetProperty("UtcNow");
        bool FormatIfExprIsDateTime(Expression expr)
        {
            if (expr is NewExpression)
            {
                var node = expr as NewExpression;
                if (node.Constructor.DeclaringType == typeof(DateTime) || node.Constructor.DeclaringType == typeof(DateTimeOffset))
                {

                    var parameters = node.Arguments.Select(x => ExpressionHelper.GetValue(x)).ToArray();
                    var datetime = node.Constructor.Invoke(parameters);
                    var v = DataTypeFormatter.Format(datetime);
                    valuesSb.Append(v);
                    return true;
                }
            }
            if (expr is MemberExpression)
            {
                var node = expr as MemberExpression;
                if (node.Member == now) { valuesSb.Append(DataTypeFormatter.Format(DateTime.Now)); return true; }
                if (node.Member == utcNow) { valuesSb.Append(DataTypeFormatter.Format(DateTime.UtcNow)); return true; }
                if (node.Member == nowOffset) { valuesSb.Append(DataTypeFormatter.Format(DateTimeOffset.Now)); return true; }
                if (node.Member == utcNowOffset) { valuesSb.Append(DataTypeFormatter.Format(DateTimeOffset.UtcNow)); return true; }
            }

            return false;
        }

        protected override Expression VisitMemberInit(MemberInitExpression node)
        {
            var indent = BuildIndent();
            var innerTranslator = new BigQueryTranslateVisitor(depth, indentSize);

            var merge = node.Bindings.Select(expr =>
            {
                var assignment = expr as MemberAssignment;
                var x = assignment.Member;
                var y = assignment.Expression;

                var rightValue = innerTranslator.VisitAndClearBuffer(y);

                if (x.Name == rightValue.Trim('`', '`')) return "`" + x.Name + "`";

                return rightValue + " AS " + "`" + x.Name + "`";
            });

            var command = string.Join("," + Environment.NewLine,
                merge.Select(x => indent + x));

            columnSb.Append(command);

            return node;
        }

        // append field access
        protected override Expression VisitMember(MemberExpression node)
        {
            // WindowFunction, relieve property call and compile.
            if (node.Member.GetCustomAttributes<WindowFunctionAttribute>().Any())
            {
                var methodNode = node.Expression as MethodCallExpression;

                var root = (MethodCallExpression)methodNode.Object;
                while (true)
                {
                    if (root == null)
                    {
                        root = methodNode;
                        break;
                    }
                    var parent = root.Object as MethodCallExpression;
                    if (parent == null) break;
                    root = parent;
                }

                var para = root.Arguments[0] as ParameterExpression;
                var windowFunction = Expression.Lambda(methodNode, para);
                var compiledWindowFunction = windowFunction.Compile();
                var windowQuery = compiledWindowFunction.DynamicInvoke(new object[1]);
                columnSb.Append(windowQuery.ToString());
                return node;
            }

            // specialize for DateTime
            if (FormatIfExprIsDateTime(node)) return node;

            bool isRootIsParameter = false; // as external field or parameter
            var nodes = new List<MemberExpression>();
            var next = node;
            while (next != null)
            {
                isRootIsParameter = next.Expression.NodeType == ExpressionType.Parameter;
                nodes.Add(next);

                var nextExpr = next.Expression;
                next = nextExpr as MemberExpression;

                if (next == null)
                {
                    // skip indexer access for repeated field
                    var binaryNext = nextExpr;
                    while (binaryNext is BinaryExpression)
                    {
                        binaryNext = ((BinaryExpression)binaryNext).Left;
                        if (binaryNext is MemberExpression)
                        {
                            next = (MemberExpression)binaryNext;
                            break;
                        }
                    }
                }
            }

            if (isRootIsParameter)
            {
                columnSb.Append("`");
                for (int i = nodes.Count - 1; i >= 0; i--)
                {
                    var memberInfo = nodes[i].Member;
                    var columnNameAttr = memberInfo.GetCustomAttribute<ColumnNameAttribute>(true);

                    columnSb.Append(columnNameAttr?.ColumnName ?? memberInfo.Name);

                    // If Nullable don't emit .Value
                    if (nodes[i].Type.IsNullable()) break;

                    if (nodes.Count != 1 && i != 0)
                    {
                        columnSb.Append(".");
                    }
                }
                columnSb.Append("`,");
            }
            else
            {
                var v = ExpressionHelper.GetValue(nodes[0]);
                var str = DataTypeFormatter.Format(v);
                columnSb.Append(str);
            }

            return node;
        }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            var expr = DataTypeFormatter.Format(node.Value);
            valuesSb.Append(expr);
            valuesSb.Append(",");

            return node;
        }
    }
}