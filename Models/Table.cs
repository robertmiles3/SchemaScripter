
namespace SchemaScripter.Models
{
    public class Table
    {
        public long ObjectId { get; set; }
        public string SchemaName { get; set; }
        public string TableName { get; set; }
        public string Definition { get; set; }
    }
}
