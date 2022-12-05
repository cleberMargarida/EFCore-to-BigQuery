using BigQuery.EntityFramework.Core.DataAnnotations;
using System;

namespace ApiBQ
{
    [Table(projectId: "moonlit-text-367106", dataset: "data", tableName: "myentity")]
    public class MyEntity
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; }
        public DateTime Update { get; set; }
    }

}
