using Google.Apis.Auth.OAuth2;
using Google.Cloud.BigQuery.V2;

namespace BigQuery.EntityFramework.Core
{
    public class BigQueryContextBuilder
    {
        public GoogleCredential GoogleCredential { get; set; }
        public string ProjectId { get; set; }
    }
}
