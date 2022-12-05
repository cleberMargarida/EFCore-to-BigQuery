using BigQuery.EntityFramework.Core;

namespace ApiBQ.Data
{
    public class BigQueryApplicationContext : BqContext
    {
        public BigQueryApplicationContext(BigQueryContextBuilder builder) : base(builder) 
        {
        }

        public BqSet<MyEntity> MyEntities { get; set; }
    }
}
