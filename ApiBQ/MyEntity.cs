using BigQuery.EntityFramework.Core.DataAnnotations;
using System;

namespace ApiBQ
{
    [Table("`data.myentity`")]
    public class MyEntity
    {
        [Key, ColumnName("id")]
        public int Id { get; set; }

        [ColumnName("name")]
        public string Name { get; set; }

        [ColumnName("update")]
        public DateTime Update { get; set; }

        public Test Test { get; set; }
    }

    public class Test
    {
        public string TestName { get; set; }
        public int TestId { get; set; }
        public TestIntern TestIntern { get; set; }
    }

    public class TestIntern
    {
        public string Description { get; set; }
    }

}
