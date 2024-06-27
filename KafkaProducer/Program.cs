using Confluent.Kafka;
using System;
using System.Threading.Tasks;

class Program
{
    private static readonly string kafkaBootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "";
    private static readonly string topicName = "test_topic";

    public static async Task Main(string[] args)
    {
        if (string.IsNullOrEmpty(kafkaBootstrapServers))
        {
            Console.WriteLine("KAFKA_BOOTSTRAP_SERVERS environment variable is not set.");
            return;
        }

        Console.WriteLine($"Kafka Bootstrap Servers: {kafkaBootstrapServers}");

        var config = new ProducerConfig
        {
            BootstrapServers = kafkaBootstrapServers
        };

        using var producer = new ProducerBuilder<Null, string>(config).Build();

        var message = "a";
        var drInit = await producer.ProduceAsync(topicName, new Message<Null, string> { Value = message });

        Console.WriteLine($"Enter messages to send to the Kafka topic named '{topicName}'. Type 'exit' to quit.");

        while (true)
        {
            message = Console.ReadLine();
            Console.WriteLine($"Message entered: {message}");
            if (message == null)
            {
                Console.WriteLine("Input message is NULL.");
                break;
            }

            if (message.ToLower() == "exit")
            {
                break;
            }

            try
            {
                Console.WriteLine($"Producing message");
                var dr = await producer.ProduceAsync(topicName, new Message<Null, string> { Value = message });
                Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
            }
            catch (ProduceException<Null, string> e)
            {
                Console.WriteLine($"Delivery failed: {e.Error.Reason}");
            }
        }
    }
}