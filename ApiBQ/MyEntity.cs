using BigQuery.EntityFramework.Core.DataAnnotations;
using System;

namespace ApiBQ
{
    [Table("`data.myentity`")]
    public class MyEntity
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; }
        public DateTime Update { get; set; }
    }

}
