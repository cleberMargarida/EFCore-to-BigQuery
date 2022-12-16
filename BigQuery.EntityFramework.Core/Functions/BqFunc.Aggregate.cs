﻿using BigQuery.EntityFramework.Core;
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
        // https://developers.google.com/bigquery/query-reference#aggfunctions

        /// <summary>
        /// Returns the average of the values for a group of rows computed by numeric_expr. Rows with a NULL value are not included in the calculation.
        /// </summary>
        [FunctionName("AVG")]
        public static double Average(long? numericExpression) { throw Invalid(); }
        /// <summary>
        /// Returns the average of the values for a group of rows computed by numeric_expr. Rows with a NULL value are not included in the calculation.
        /// </summary>
        [FunctionName("AVG")]
        public static double Average(double? numericExpression) { throw Invalid(); }

        /// <summary>
        /// Returns the result of a bitwise AND operation between each instance of numeric_expr across all rows. NULL values are ignored. This function returns NULL if all instances of numeric_expr evaluate to NULL.
        /// </summary>
        [FunctionName("BIT_AND")]
        public static double BitAnd(long? numericExpression) { throw Invalid(); }

        /// <summary>
        /// Returns the result of a bitwise OR operation between each instance of numeric_expr across all rows. NULL values are ignored. This function returns NULL if all instances of numeric_expr evaluate to NULL.
        /// </summary>
        [FunctionName("BIT_OR")]
        public static double BitOr(long? numericExpression) { throw Invalid(); }

        /// <summary>
        /// Returns the result of a bitwise XOR operation between each instance of numeric_expr across all rows. NULL values are ignored. This function returns NULL if all instances of numeric_expr evaluate to NULL.
        /// </summary>
        [FunctionName("BIT_XOR")]
        public static double BitXor(long? numericExpression) { throw Invalid(); }

        /// <summary>
        /// Returns the Pearson correlation coefficient of a set of number pairs.
        /// </summary>
        [FunctionName("CORR")]
        public static double Correlation(long? numericExpression1, long? numericExpression2)
        {
            throw Invalid();
        }
        /// <summary>
        /// Returns the Pearson correlation coefficient of a set of number pairs.
        /// </summary>
        [FunctionName("CORR")]
        public static double Correlation(double? numericExpression1, double? numericExpression2)
        {
            throw Invalid();
        }

        /// <summary>
        /// COUNT(*). Returns the total number of values (NULL and non-NULL) in the scope of the function. Unless you are using COUNT(*) with the TOP function, it is better to explicitly specify the field to count.
        /// </summary>
        [FunctionName("COUNT", SpecifiedFormatterType = typeof(CountAllFormatter))]
        public static long Count()
        {
            throw Invalid();
        }

        /// <summary>
        /// Returns the total number of non-NULL values in the scope of the function.
        /// If you use the DISTINCT keyword, the function returns the number of distinct values for the specified field. Note that the returned value for DISTINCT is a statistical approximation and is not guaranteed to be exact.
        /// If you require greater accuracy from COUNT(DISTINCT), you can specify a second parameter, n, which gives the threshold below which exact results are guaranteed. By default, n is 1000, but if you give a larger n, you will get exact results for COUNT(DISTINCT) up to that value of n. However, giving larger values of n will reduce scalability of this operator and may substantially increase query execution time or cause the query to fail.
        /// To compute the exact number of distinct values in a scalable fashion, consider using GROUP EACH BY on the relevant field(s) and then applying COUNT(*). This approach is more scalable but might incur a slight up-front performance penalty.
        /// </summary>
        [FunctionName("COUNT", SpecifiedFormatterType = typeof(CountFieldFormatter))]
        public static long Count(object field)
        {
            throw Invalid();
        }

        /// <summary>
        /// Returns the total number of non-NULL values in the scope of the function.
        /// If you use the DISTINCT keyword, the function returns the number of distinct values for the specified field. Note that the returned value for DISTINCT is a statistical approximation and is not guaranteed to be exact.
        /// If you require greater accuracy from COUNT(DISTINCT), you can specify a second parameter, n, which gives the threshold below which exact results are guaranteed. By default, n is 1000, but if you give a larger n, you will get exact results for COUNT(DISTINCT) up to that value of n. However, giving larger values of n will reduce scalability of this operator and may substantially increase query execution time or cause the query to fail.
        /// To compute the exact number of distinct values in a scalable fashion, consider using GROUP EACH BY on the relevant field(s) and then applying COUNT(*). This approach is more scalable but might incur a slight up-front performance penalty.
        /// </summary>
        [FunctionName("COUNT(DISTINCT)", SpecifiedFormatterType = typeof(CountDistinctFormatter))]
        public static long CountDistinct(object field)
        {
            throw Invalid();
        }

        /// <summary>
        /// Returns the total number of non-NULL values in the scope of the function.
        /// If you use the DISTINCT keyword, the function returns the number of distinct values for the specified field. Note that the returned value for DISTINCT is a statistical approximation and is not guaranteed to be exact.
        /// If you require greater accuracy from COUNT(DISTINCT), you can specify a second parameter, n, which gives the threshold below which exact results are guaranteed. By default, n is 1000, but if you give a larger n, you will get exact results for COUNT(DISTINCT) up to that value of n. However, giving larger values of n will reduce scalability of this operator and may substantially increase query execution time or cause the query to fail.
        /// To compute the exact number of distinct values in a scalable fashion, consider using GROUP EACH BY on the relevant field(s) and then applying COUNT(*). This approach is more scalable but might incur a slight up-front performance penalty.
        /// </summary>
        [FunctionName("COUNT(DISTINCT)", SpecifiedFormatterType = typeof(CountDistinctFormatter))]
        public static long CountDistinct(object field, int n)
        {
            throw Invalid();
        }

        /// <summary>Computes the population covariance of the values computed by numeric_expr1 and numeric_expr2.</summary>
        [FunctionName("COVAR_POP")]
        public static double CovariancePopulation(long? numericExpression1, long? numericExpression2) { throw Invalid(); }
        /// <summary>Computes the population covariance of the values computed by numeric_expr1 and numeric_expr2.</summary>
        [FunctionName("COVAR_POP")]
        public static double CovariancePopulation(double? numericExpression1, double? numericExpression2) { throw Invalid(); }

        /// <summary>Computes the sample covariance of the values computed by numeric_expr1 and numeric_expr2.</summary>
        [FunctionName("COVAR_SAMP")]
        public static double CovarianceSample(long? numericExpression1, long? numericExpression2) { throw Invalid(); }
        /// <summary>Computes the sample covariance of the values computed by numeric_expr1 and numeric_expr2.</summary>
        [FunctionName("COVAR_SAMP")]
        public static double CovarianceSample(double? numericExpression1, double? numericExpression2) { throw Invalid(); }

        /// <summary>
        /// Returns the exact number of non-NULL, distinct values for the specified field. For better scalability and performance, use COUNT(DISTINCT field).
        /// </summary>
        [FunctionName("EXACT_COUNT_DISTINCT")]
        public static long ExactCountDistinct(object field)
        {
            throw Invalid();
        }

        /// <summary>Returns the first sequential value in the scope of the function.</summary>
        [FunctionName("FIRST")]
        public static T First<T>(T expr)
        {
            throw Invalid();
        }

        /// <summary>Concatenates multiple strings into a single string, where each value is separated by the optional separator parameter. If separator is omitted, BigQuery returns a comma-separated string.</summary>
        [FunctionName("GROUP_CONCAT")]
        public static string GroupConcat(string str)
        {
            throw Invalid();
        }

        /// <summary>Concatenates multiple strings into a single string, where each value is separated by the optional separator parameter. If separator is omitted, BigQuery returns a comma-separated string.</summary>
        [FunctionName("GROUP_CONCAT")]
        public static string GroupConcat(string str, string separator)
        {
            throw Invalid();
        }

        /// <summary>Concatenates multiple strings into a single string, where each value is separated by the optional separator parameter. If separator is omitted, BigQuery returns a comma-separated string. Unlike GROUP_CONCAT, this function will not add double quotes to returned values that include a double quote character.</summary>
        [FunctionName("GROUP_CONCAT_UNQUOTED")]
        public static string GroupConcatUnquoted(string str)
        {
            throw Invalid();
        }

        /// <summary>Concatenates multiple strings into a single string, where each value is separated by the optional separator parameter. If separator is omitted, BigQuery returns a comma-separated string. Unlike GROUP_CONCAT, this function will not add double quotes to returned values that include a double quote character.</summary>
        [FunctionName("STRING_AGG")]
        public static string GroupConcatUnquoted(string str, string separator)
        {
            throw Invalid();
        }

        /// <summary>
        /// Returns the last sequential value in the scope of the function. The LAST function works only with the WITHIN keyword.
        /// </summary>
        [FunctionName("LAST")]
        public static T Last<T>(T field)
        {
            throw Invalid();
        }

        /// <summary>
        /// Returns the maximum value in the scope of the function.
        /// </summary>
        [FunctionName("MAX")]
        public static T Max<T>(T field)
        {
            throw Invalid();
        }

        /// <summary>
        /// Returns the minimum value in the scope of the function.
        /// </summary>
        [FunctionName("MIN")]
        public static T Min<T>(T field)
        {
            throw Invalid();
        }

        /// <summary>
        /// Aggregates all values in the current aggregation scope into a repeated field. For example, the query "SELECT x, NEST(y) FROM ... GROUP BY x" returns one output record for each distinct x value, and contains a repeated field for all y values paired with x in the query input. The NEST function requires a GROUP BY clause.
        /// BigQuery automatically flattens query results, so if you use the NEST function on the top level query, the results won't contain repeated fields. Use the NEST function when using a subselect that produces intermediate results for immediate use by the same query.
        /// </summary>
        [FunctionName("ARRAY_AGG")]
        public static T Nest<T>(T field)
        {
            throw Invalid();
        }

        /// <summary>
        /// Returns the nth sequential value in the scope of the function, where n is a constant. The NTH function starts counting at 1, so there is no zeroth term. If the scope of the function has less than n values, the function returns NULL.
        /// </summary>
        [FunctionName("NTH")]
        public static T Nth<T>(long? n, T field)
        {
            throw Invalid();
        }

        /// <summary>
        /// Compares approximate quantiles for the input expression. The number of quantiles computed is controlled with the optional buckets parameter. The default value of buckets is 100. If specified explicitly, buckets must be greater than or equal to 2. The fractional error per quantile is epsilon = 1 / buckets.
        /// </summary>
        [FunctionName("QUANTILES")]
        public static T Quantiles<T>(T expr)
        {
            throw Invalid();
        }

        /// <summary>
        /// Compares approximate quantiles for the input expression. The number of quantiles computed is controlled with the optional buckets parameter. The default value of buckets is 100. If specified explicitly, buckets must be greater than or equal to 2. The fractional error per quantile is epsilon = 1 / buckets.
        /// </summary>
        [FunctionName("QUANTILES")]
        public static T Quantiles<T>(T expr, long? buckets)
        {
            throw Invalid();
        }

        /// <summary>Returns the standard deviation of the values computed by numeric_expr. Rows with a NULL value are not included in the calculation. The STDDEV function is an alias for STDDEV_SAMP.</summary>
        [FunctionName("STDDEV")]
        public static double StandardDeviation(long? numericExpression) { throw Invalid(); }
        /// <summary>Returns the standard deviation of the values computed by numeric_expr. Rows with a NULL value are not included in the calculation. The STDDEV function is an alias for STDDEV_SAMP.</summary>
        [FunctionName("STDDEV")]
        public static double StandardDeviation(double? numericExpression) { throw Invalid(); }

        /// <summary>Computes the population standard deviation of the value computed by numeric_expr.</summary>
        [FunctionName("STDDEV_POP")]
        public static double StandardDeviationPopulation(long? numericExpression) { throw Invalid(); }
        /// <summary>Computes the population standard deviation of the value computed by numeric_expr.</summary>
        [FunctionName("STDDEV_POP")]
        public static double StandardDeviationPopulation(double? numericExpression) { throw Invalid(); }

        /// <summary>Computes the sample standard deviation of the value computed by numeric_expr.</summary>
        [FunctionName("STDDEV_SAMP")]
        public static double StandardDeviationSample(long? numericExpression) { throw Invalid(); }
        /// <summary>Computes the sample standard deviation of the value computed by numeric_expr.</summary>
        [FunctionName("STDDEV_SAMP")]
        public static double StandardDeviationSample(double? numericExpression) { throw Invalid(); }

        /// <summary>
        /// Returns the sum total of the values in the scope of the function. For use with numerical data types only.
        /// </summary>
        [FunctionName("SUM")]
        public static long Sum(long? field)
        {
            throw Invalid();
        }

        /// <summary>
        /// Returns the sum total of the values in the scope of the function. For use with numerical data types only.
        /// </summary>
        [FunctionName("SUM")]
        public static double Sum(double? field)
        {
            throw Invalid();
        }

        /// <summary>
        /// Returns the sum total of the values in the scope of the function. For use with numerical data types only.
        /// </summary>
        [FunctionName("SUM")]
        public static long Sum(DateTimeOffset? field)
        {
            throw Invalid();
        }

        /// <summary>
        /// Returns the top max_records records by frequency.
        /// TOP is a function that is an alternative to the GROUP BY clause.
        /// When using TOP in a SELECT clause, you must include COUNT(*) as one of the fields.
        /// </summary>
        /// <param name="field">The field or alias to return.</param>
        /// <returns></returns>
        [FunctionName("TOP")]
        public static T Top<T>(T field)
        {
            throw Invalid();
        }

        /// <summary>
        /// Returns the top max_records records by frequency.
        /// TOP is a function that is an alternative to the GROUP BY clause.
        /// When using TOP in a SELECT clause, you must include COUNT(*) as one of the fields.
        /// </summary>
        /// <param name="field">The field or alias to return.</param>
        /// <param name="maxValues">[Optional] The maximum number of results to return. Default is 20.</param>
        /// <returns></returns>
        [FunctionName("TOP")]
        public static T Top<T>(T field, long? maxValues)
        {
            throw Invalid();
        }

        /// <summary>
        /// Returns the top max_records records by frequency.
        /// TOP is a function that is an alternative to the GROUP BY clause.
        /// When using TOP in a SELECT clause, you must include COUNT(*) as one of the fields.
        /// </summary>
        /// <param name="field">The field or alias to return.</param>
        /// <param name="maxValues">[Optional] The maximum number of results to return. Default is 20.</param>
        /// <param name="multiplier">A numeric constant, expression, or field that is multiplied with max_values to specify how many results to return.</param>
        /// <returns></returns>
        [FunctionName("TOP")]
        public static T Top<T>(T field, long? maxValues, long? multiplier)
        {
            throw Invalid();
        }

        /// <summary>
        /// Returns the set of unique, non-NULL values in the scope of the function in an undefined order.
        /// Similar to a large GROUP BY clause without the EACH keyword, the query will fail with a "Resources Exceeded" error if there are too many distinct values.
        /// Unlike GROUP BY, however, the UNIQUE function can be applied with scoped aggregation, allowing efficient operation on nested fields with a limited number of values.
        /// </summary>
        [FunctionName("UNIQUE")]
        public static T Unique<T>(T expr)
        {
            throw Invalid();
        }

        /// <summary>Computes the variance of the values computed by numeric_expr. Rows with a NULL value are not included in the calculation. The VARIANCE function is an alias for VAR_SAMP.</summary>
        [FunctionName("VARIANCE")]
        public static double Variance(long? numericExpression) { throw Invalid(); }
        /// <summary>Computes the variance of the values computed by numeric_expr. Rows with a NULL value are not included in the calculation. The VARIANCE function is an alias for VAR_SAMP.</summary>
        [FunctionName("VARIANCE")]
        public static double Variance(double? numericExpression) { throw Invalid(); }

        /// <summary>Computes the population variance of the values computed by numeric_expr.</summary>
        [FunctionName("VAR_POP")]
        public static double VariancePopulation(long? numericExpression) { throw Invalid(); }
        /// <summary>Computes the population variance of the values computed by numeric_expr.</summary>
        [FunctionName("VAR_POP")]
        public static double VariancePopulation(double? numericExpression) { throw Invalid(); }

        /// <summary>Computes the sample variance of the values computed by numeric_expr.</summary>
        [FunctionName("VAR_SAMP")]
        public static double VarianceSample(long? numericExpression) { throw Invalid(); }
        /// <summary>Computes the sample variance of the values computed by numeric_expr.</summary>
        [FunctionName("VAR_SAMP")]
        public static double VarianceSample(double? numericExpression) { throw Invalid(); }

        class CountAllFormatter : ISpecifiedFormatter
        {
            public string Format(int depth, int indentSize, string fuctionName, MethodCallExpression node)
            {
                return "COUNT(*)";
            }
        }

        class CountFieldFormatter : ISpecifiedFormatter
        {
            public string Format(int depth, int indentSize, string fuctionName, MethodCallExpression node)
            {
                var innerTranslator = new BigQueryTranslateVisitor();
                var args = string.Join(", ", node.Arguments.Select(x => innerTranslator.VisitAndClearBuffer(x)));

                return "COUNT(" + args + ")";
            }
        }

        class CountDistinctFormatter : ISpecifiedFormatter
        {
            public string Format(int depth, int indentSize, string fuctionName, MethodCallExpression node)
            {
                var innerTranslator = new BigQueryTranslateVisitor();
                var args = string.Join(", ", node.Arguments.Select(x => innerTranslator.VisitAndClearBuffer(x)));

                return "COUNT(DISTINCT " + args + ")";
            }
        }
    }
}
