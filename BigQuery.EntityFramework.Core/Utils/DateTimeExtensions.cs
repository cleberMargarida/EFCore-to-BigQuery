﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.EntityFramework.Core.Utils
{
    public static class DateTimeExtensions
    {
        static readonly DateTime unixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        static readonly DateTimeOffset unixEpochOffset = new DateTimeOffset(unixEpoch);

        internal static long ToUnixTimestamp(this DateTime target)
        {
            return (long)(target - unixEpoch).TotalMilliseconds;
        }

        internal static long ToUnixTimestamp(this DateTimeOffset target)
        {
            return (long)(target - unixEpochOffset).TotalMilliseconds;
        }

        public static long ToBigQueryTimestamp(this DateTime target)
        {
            return (long)(target - unixEpoch).TotalMilliseconds * 1000;
        }

        public static long ToBigQueryTimestamp(this DateTimeOffset target)
        {
            return (long)(target - unixEpochOffset).TotalMilliseconds * 1000;
        }

        /// <summary>
        /// From long with 6 millisecond digits(YYYY-MM-DD HH:MM:SS[.uuuuuu].)
        /// </summary>
        public static DateTimeOffset FromBigQueryTimestamp(this long timestamp)
        {
            var seconds = Math.Round(((double)timestamp / 1000));
            var date = unixEpochOffset.AddMilliseconds(seconds);
            return date;
        }

        public static DateTimeOffset FromTimestampSeconds(this long timestampSecond)
        {
            var date = unixEpochOffset.AddSeconds(timestampSecond);
            return date;
        }

        public static DateTimeOffset FromTimestampMilliSeconds(this long timestampMillisecond)
        {
            var date = unixEpochOffset.AddMilliseconds(timestampMillisecond);
            return date;
        }

        /// <summary>
        /// From long with 6 millisecond digits(YYYY-MM-DD HH:MM:SS[.uuuuuu].)
        /// </summary>
        public static DateTimeOffset FromBigQueryTimestamp(this ulong timestamp)
        {
            var seconds = Math.Round(((double)timestamp / 1000));
            var date = unixEpochOffset.AddMilliseconds(seconds);
            return date;
        }

        public static DateTimeOffset FromTimestampSeconds(this ulong timestampSecond)
        {
            var date = unixEpochOffset.AddSeconds(timestampSecond);
            return date;
        }

        public static DateTimeOffset FromTimestampMilliSeconds(this ulong timestampMillisecond)
        {
            var date = unixEpochOffset.AddMilliseconds(timestampMillisecond);
            return date;
        }
    }
}