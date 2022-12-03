using Google.Apis.Auth.OAuth2;
using Google.Apis.Bigquery.v2;
using Google.Cloud.BigQuery.V2;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using System.Linq;
using Google.Apis.Services;
using System.Threading.Tasks;
using System.Collections;
using BigQuery.EntityFramework.Core.Linq;

namespace BigQuery.EntityFramework.Core
{
    public class BqContext
    {
        private readonly Type referenceType;

        public BqContext(GoogleCredential googleCredential, string projectId)
        {
            referenceType = GetType();
            
            var bqClient = BigQueryClient.CreateAsync(projectId, googleCredential).Result;
            var bqService = bqClient.Service;
            BigqueryService = bqService;
            BigQueryClient = bqClient;
            
            ProjectId = projectId;
            StartTablesInstance();
        }

        public BqContext(BigQueryContextBuilder builder) : this(builder.GoogleCredential, builder.ProjectId)
        {
        }

        protected BigqueryService BigqueryService { get; }
        protected BigQueryClient BigQueryClient { get; }
        IEnumerable<BigQueryParameter> EmptyParameters => Enumerable.Empty<BigQueryParameter>();

        public string ProjectId { get; }

        private void StartTablesInstance()
        {
            foreach (var property in from prop in referenceType.GetProperties()
                                     where typeof(IBigQueryable)
                                     .IsAssignableFrom(prop.PropertyType)
                                     select prop)

                property.SetValue(this, Activator.CreateInstance(
                    typeof(BqSetImpl<>).MakeGenericType(property.PropertyType.GenericTypeArguments),
                    BigqueryService,
                    BigQueryClient,
                    ProjectId));
        }

        public void ExecuteQuery(string query)
        {
            BigQueryClient.ExecuteQuery(query, EmptyParameters);
        }

        public async Task ExecuteQueryAsync(string query)
        {
            await BigQueryClient.ExecuteQueryAsync(query, EmptyParameters);
        }

        public IEnumerable<T> ExecuteQuery<T>(string query)
        {
            BigQueryResults bigQueryResults = BigQueryClient.ExecuteQuery(query, EmptyParameters);
            QueryResponse<T> queryResponse = QueryResponse<T>.FromResults(bigQueryResults);
            return queryResponse.ToList();
        }

        public async Task<IEnumerable<T>> ExecuteQueryAsync<T>(string query)
        {
            BigQueryResults bigQueryResults = await BigQueryClient.ExecuteQueryAsync(query, EmptyParameters);
            QueryResponse<T> queryResponse = QueryResponse<T>.FromResults(bigQueryResults);
            return await queryResponse.ToListAsync();
        }
    }
}
