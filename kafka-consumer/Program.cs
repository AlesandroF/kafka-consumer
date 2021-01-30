using Confluent.Kafka;
using System;
using System.Threading;

namespace kafka_consumer
{
    public static class Program
    {
        public static void Main(string[] args)
        {
            var config = new ConsumerConfig
            {
                GroupId = "test-consumer-group",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
            {
                consumer.Subscribe("test-topic");

                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true;
                    cts.Cancel();
                };

                try
                {
                    while (true)
                    {
                        try
                        {
                            var consume = consumer.Consume(cts.Token);
                            Console.WriteLine($"Consumed message '{consume.Message.Value}' at '{consume.TopicPartitionOffset}'.");
                        }
                        catch (ConsumeException ex)
                        {
                            Console.WriteLine($"Error ocurred: {ex.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    consumer.Close();
                }
            }
        }
    }
}