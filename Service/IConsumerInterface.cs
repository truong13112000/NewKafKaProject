namespace ApacheKafkaProducerDemo.Service
{
    public interface IConsumerInterface
    {
        Task<bool> StartAsync(CancellationToken cancellationToken);
        Task StopAsync(CancellationToken cancellationToken);
    }
}
