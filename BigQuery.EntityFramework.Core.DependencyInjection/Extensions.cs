using BigQuery.EntityFramework.Core;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Bigquery.v2;
using Google.Cloud.BigQuery.V2;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Linq;

namespace BigQuery.EntityFramework.Core.DependencyInjection
{
    public static class Extensions
    {
        public static IServiceCollection AddBigQueryContext<T>(this IServiceCollection services, Action<BigQueryContextBuilder> options)
            where T : BqContext
        {
            var bigQueryOptions = new BigQueryContextBuilder();
            options.Invoke(bigQueryOptions);
            services.AddSingleton(bigQueryOptions);
            services.AddScoped<T>();
            return services;
        }
    }
}
