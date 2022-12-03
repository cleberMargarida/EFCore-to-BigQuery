using BigQuery.EntityFramework.Core.DataAnnotations;

namespace ApiBQ
{
    [Table(projectId: "moonlit-text-367106", dataset: "metadata", tableName: "faultmetadata")]
    //[TableName("`moonlit-text-367106.metadata.faultmetadata`")]
    public class Fault
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; }
    }

}
