﻿using BigQuery.EntityFramework.Core.Linq;
using BigQuery.EntityFramework.Core.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.EntityFramework.Core.Query
{
    internal enum DecorateType
    {
        Snapshot,
        Range
    }

    internal class TableDecoratorBigQueryable<T> : BigQueryable, ITableDecoratorBigQueryable<T>, ITableName
    {
        public static DateTimeOffset Zero = DateTimeOffset.MinValue;

        readonly IFromBigQueryable<T> typedParent;
        readonly DecorateType type;
        readonly TimeSpan? relativeTime1;
        readonly TimeSpan? relativeTime2;
        readonly DateTimeOffset? absoluteTime1;
        readonly DateTimeOffset? absoluteTime2;
        internal override int Order
        {
            get { return 1; }
        }

        internal TableDecoratorBigQueryable(IFromBigQueryable<T> parent, DecorateType type, DateTimeOffset? absoluteTime1 = null, DateTimeOffset? absoluteTime2 = null, TimeSpan? relativeTime1 = null, TimeSpan? relativeTime2 = null)
            : base(new RootBigQueryable<T>(parent.QueryContext))
        {
            this.type = type;
            this.typedParent = parent;
            this.absoluteTime1 = absoluteTime1;
            this.absoluteTime2 = absoluteTime2;
            this.relativeTime1 = relativeTime1;
            this.relativeTime2 = relativeTime2;
        }

        public override string BuildQueryString(int depth)
        {
            var sb = new StringBuilder();

            sb.Append(Indent(depth));
            sb.AppendLine("FROM");
            sb.Append(Indent(depth + 1));
            sb.Append(GetTableName());

            return sb.ToString();
        }

        public string GetTableName()
        {
            var parent = (FromBigQueryable<T>)typedParent;

            var sb = new StringBuilder();

            var isFirst = true;
            foreach (var item in parent.tableNames)
            {
                if (isFirst)
                {
                    isFirst = false;
                }
                else
                {
                    sb.Append(", ");
                }

                var tablename = item.Trim('`', '`');
                sb.Append("`").Append(tablename).Append("@");

                if (type == DecorateType.Snapshot)
                {
                    if (relativeTime1 != null)
                    {
                        sb.Append("-").Append(Math.Floor(relativeTime1.Value.TotalMilliseconds));
                    }
                    else if (absoluteTime1 == Zero)
                    {
                        sb.Append("0");
                    }
                    else
                    {
                        sb.Append(absoluteTime1.Value.ToUnixTimestamp());
                    }
                }
                else
                {
                    if (relativeTime1 != null)
                    {
                        sb.Append("-").Append(Math.Floor(relativeTime1.Value.TotalMilliseconds));
                    }
                    else if (absoluteTime1.HasValue)
                    {
                        sb.Append(absoluteTime1.Value.ToUnixTimestamp());
                    }
                    sb.Append("-");
                    if (relativeTime2 != null)
                    {
                        sb.Append("-").Append(Math.Floor(relativeTime2.Value.TotalMilliseconds));
                    }
                    else if (absoluteTime2.HasValue)
                    {
                        sb.Append(absoluteTime2.Value.ToUnixTimestamp());
                    }
                }

                sb.Append("`");
            }

            return sb.ToString();
        }
    }
}