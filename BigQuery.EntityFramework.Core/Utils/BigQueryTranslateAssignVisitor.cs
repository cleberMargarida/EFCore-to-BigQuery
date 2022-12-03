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
    internal class BigQueryTranslateAssignVisitor : ExpressionVisitor
    {
        readonly int depth = 1;
        readonly int indentSize = 2;

        StringBuilder sb = new StringBuilder();

        public BigQueryTranslateAssignVisitor()
        {
            depth = 0;
            indentSize = 0;
        }

        public BigQueryTranslateAssignVisitor(int depth, int indentSize)
        {
            this.depth = depth;
            this.indentSize = indentSize;
        }

        // EntryPoint
        public static string BuildQuery(int depth, int indentSize, Expression expression)
        {
            var visitor = new BigQueryTranslateAssignVisitor(depth, indentSize);
            visitor.Visit(expression);
            return visitor.sb.ToString();
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
                    sb.Append(v);
                    return true;
                }
            }
            if (expr is MemberExpression)
            {
                var node = expr as MemberExpression;
                if (node.Member == now) { sb.Append(DataTypeFormatter.Format(DateTime.Now)); return true; }
                if (node.Member == utcNow) { sb.Append(DataTypeFormatter.Format(DateTime.UtcNow)); return true; }
                if (node.Member == nowOffset) { sb.Append(DataTypeFormatter.Format(DateTimeOffset.Now)); return true; }
                if (node.Member == utcNowOffset) { sb.Append(DataTypeFormatter.Format(DateTimeOffset.UtcNow)); return true; }
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

            sb.Append(command);

            return node;
        }

        protected override Expression VisitBinary(BinaryExpression node)
        {
            string expr;
            bool isNull = false;
            switch (node.NodeType)
            {
                case ExpressionType.Equal:
                    expr = "=";
                    break;
                default:
                    throw new InvalidOperationException("Invalid node type:" + node.NodeType);
            }

            base.Visit(node.Left); // run to left

            sb.Append(" " + expr);

            if (!isNull)
            {
                sb.Append(" ");
                base.Visit(node.Right); // run to right
            }

            sb.Append(",");

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
                sb.Append(windowQuery.ToString());
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
                sb.Append("`");
                for (int i = nodes.Count - 1; i >= 0; i--)
                {
                    var memberInfo = nodes[i].Member;
                    var columnNameAttr = memberInfo.GetCustomAttribute<ColumnNameAttribute>(true);

                    sb.Append(columnNameAttr?.ColumnName ?? memberInfo.Name);

                    // If Nullable don't emit .Value
                    if (nodes[i].Type.IsNullable()) break;

                    if (nodes.Count != 1 && i != 0)
                    {
                        sb.Append(".");
                    }
                }
                sb.Append("`");
            }
            else
            {
                var v = ExpressionHelper.GetValue(nodes[0]);
                var str = DataTypeFormatter.Format(v);
                sb.Append(str);
            }

            return node;
        }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            var expr = DataTypeFormatter.Format(node.Value);
            sb.Append(expr);

            return node;
        }
    }
}