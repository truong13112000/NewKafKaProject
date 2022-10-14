using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace ApacheKafkaProducerDemo.Model
{
    public class KafkaEntities
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.None)]
        public int OrderId { get; set; }
        public int ProductId { get; set; }
        public int CustomerId { get; set; }
        public int Quantity { get; set; }
        public string Status { get; set; }
    }
}
