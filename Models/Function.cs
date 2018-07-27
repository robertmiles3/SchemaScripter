
namespace SchemaScripter.Models
{
    public class Function
    {
        public long ObjectId { get; set; }
        public string SchemaName { get; set; }
        public string FunctionName { get; set; }
        public string Definition { get; set; }
        public bool UsesAnsiNulls { get; set; }
        public bool UsesQuotedIdentifier { get; set; }
    }
}
