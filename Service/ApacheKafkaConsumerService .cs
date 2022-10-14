using ApacheKafkaProducerDemo.Model;
using ApacheKafkaProducerDemo.Service;
using Confluent.Kafka;
using Newtonsoft.Json;
using System.Diagnostics;
using System.Text.Json;

namespace ApacheKafkaConsumerDemo.Service
{
    public class ApacheKafkaConsumerService : IConsumerInterface
    {
        private readonly string topic = "test";
        private readonly string groupId = "test_group";
        private readonly string bootstrapServers = "localhost:9092";
        private readonly DataContext _context;
        public ApacheKafkaConsumerService(DataContext context)
        {
            _context = context;
        }

        public async Task<bool> StartAsync(CancellationToken cancellationToken)
        {
            var config = new ConsumerConfig
            {
                GroupId = groupId,
                BootstrapServers = bootstrapServers,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            try
            {
                using (var consumerBuilder = new ConsumerBuilder
                <Ignore, string>(config).Build())
                {
                    consumerBuilder.Subscribe(topic);
                    var cancelToken = new CancellationTokenSource();
                    try
                    {
                        var i = 1;
                        var newListKafka = new List<KafkaEntities>();
                        while (i <= 101)
                        {
                            i = i+1;
                            var consumer = consumerBuilder.Consume(cancelToken.Token);
                            var orderRequest = JsonConvert.DeserializeObject<KafkaEntities>(consumer.Message.Value);
                            var newKafka = new KafkaEntities
                            {
                                OrderId = orderRequest.OrderId,
                                ProductId = orderRequest.ProductId,
                                CustomerId = orderRequest.CustomerId,
                                Quantity = orderRequest.Quantity,
                                Status = orderRequest.Status,
                            };
                            var existed = _context.KafkaEntities.FirstOrDefault(e => e.OrderId == newKafka.OrderId);
                            if (existed == null)
                            {
                                _context.KafkaEntities.Add(newKafka);
                                _context.SaveChanges();
                            }
                            Debug.WriteLine($"Processing Order Id: {i}");
                        };
                        return await Task.FromResult(true);
                    }
                    catch (Exception e)
                    {
                        return false;
                    }
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine(ex.Message);
            }

            return await Task.FromResult(false);
        }
        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}
