using ApacheKafkaProducerDemo.Model;
using ApacheKafkaProducerDemo.Service;
using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using System.Diagnostics;
using System.Net;
using System.Text.Json;

namespace ApacheKafkaProducerDemo.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ProducerController : ControllerBase
    {
        private readonly string bootrapServer = "localhost:9092";
        private readonly string topic = "test";
        private readonly IConsumerInterface _consumerInterface;
        private DataContext _context;
        public ProducerController(IConsumerInterface consumerInterface,DataContext context)
        {
            _consumerInterface = consumerInterface;
            _context = context;
        }

        [HttpPost]
        public async Task<IActionResult> Post( CancellationToken cancellationToken)
        {
            //string message = JsonSerializer.Serialize(request);
            return Ok(await SendOrderRequest(topic, cancellationToken));
        }

        private async Task<bool> SendOrderRequest(string topic, CancellationToken cancellationToken)
        {
            ProducerConfig config = new ProducerConfig
            {
                BootstrapServers = bootrapServer,
                ClientId = Dns.GetHostName(),
            };
            try
            {
                using (var producer = new ProducerBuilder<Null, string>(config).Build())
                {
                    var listKafka = _context.KafkaEntities.ToList();
                    _context.KafkaEntities.RemoveRange(listKafka);
                    _context.SaveChanges();
                    var newList = new List<KafkaEntities>();
                    for (var i = 0; i < 100; i++)
                    {
                        var newKafka = new KafkaEntities
                        {
                            OrderId = i,
                            ProductId = i+1, 
                            CustomerId = i+2,
                            Quantity = i+3,
                            Status = $"Hello {i}",
                        };

                        var message = JsonSerializer.Serialize(newKafka);
                      //  var value = $"Hello {i}";
                        var result = await producer.ProduceAsync("test", new Message<Null, string>()
                        {
                            Value = message,
                        });
                        Debug.WriteLine($"Delivery Timestamp:{result.Timestamp.UtcDateTime}");
                    }
                    producer.Flush(TimeSpan.FromSeconds(10));
                     _consumerInterface.StartAsync(cancellationToken);
                     return await Task.FromResult(true);  
               
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error occured: {ex.Message}");
            }
            return await Task.FromResult(false);
        }
    }
}
