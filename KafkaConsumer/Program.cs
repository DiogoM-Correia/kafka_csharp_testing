using Confluent.Kafka;
using System;
using System.Threading;
using System.Threading.Tasks;
class Program
{
   private static readonly string kafkaBootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "";
   private static readonly string topicName = "test_topic";
   private static readonly string groupId = "test_group";
   public static async Task Main(string[] args)
   {
       if (string.IsNullOrEmpty(kafkaBootstrapServers))
       {
           Console.WriteLine("KAFKA_BOOTSTRAP_SERVERS environment variable is not set.");
           return;
       }
       var config = new ConsumerConfig
       {
            BootstrapServers = kafkaBootstrapServers,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoOffsetStore = false,
            EnableAutoCommit = false,
            SecurityProtocol = Enum.Parse<SecurityProtocol>("Plaintext"),
            SaslMechanism = Enum.Parse<SaslMechanism>("Plain"),
            SaslUsername = "$ConnectionString",
            SaslPassword = "****",
            SessionTimeoutMs = 300000,
            MaxPollIntervalMs = 1800000
       };
       using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
       var cts = new CancellationTokenSource();
       // Monitor topic availability and handle initial absence.
       await EnsureTopicExists(consumer);
       consumer.Subscribe(topicName);
       await Task.WhenAll(
           MonitorTopicAvailability(consumer, cts.Token),
           ConsumeMessages(consumer, cts.Token)
       );
   }
   private static async Task EnsureTopicExists(IConsumer<Ignore, string> consumer)
   {
       var adminClientConfig = new AdminClientConfig { BootstrapServers = kafkaBootstrapServers };
       using var adminClient = new AdminClientBuilder(adminClientConfig).Build();
       while (true)
       {
           try
           {
               var metadata = adminClient.GetMetadata(topicName, TimeSpan.FromSeconds(5));
               if (metadata.Topics.Count > 0 && !metadata.Topics[0].Error.IsError)
               {
                   Console.WriteLine($"Topic {topicName} is available.");
                   break;
               }
               else
               {
                   // Trigger a request that would cause automatic topic creation
                   ExecuteAutoCreateTrigger();
               }
           }
           catch (KafkaException ex) when (ex.Error.Code == ErrorCode.UnknownTopicOrPart)
           {
               Console.WriteLine($"Topic {topicName} not available yet. Waiting for the broker to auto-create it.");
           }
           await Task.Delay(5000); // Wait for 5 seconds before retrying
       }
   }
   private static void ExecuteAutoCreateTrigger()
   {
       // Use a temporary producer to trigger auto creation of the topic
       var tempConfig = new ProducerConfig { BootstrapServers = kafkaBootstrapServers };
       using var tempProducer = new ProducerBuilder<Null, string>(tempConfig).Build();
       tempProducer.ProduceAsync(topicName, new Message<Null, string> { Value = "initial message" }).Wait();
       Console.WriteLine("Sent a message to trigger topic auto-creation.");
   }
   private static async Task MonitorTopicAvailability(IConsumer<Ignore, string> consumer, CancellationToken cancellationToken)
   {
       var adminClientConfig = new AdminClientConfig { BootstrapServers = kafkaBootstrapServers };
       using var adminClient = new AdminClientBuilder(adminClientConfig).Build();
       while (!cancellationToken.IsCancellationRequested)
       {
           try
           {
               var metadata = adminClient.GetMetadata(topicName, TimeSpan.FromSeconds(5));
               if (metadata.Topics.Count > 0 && !metadata.Topics[0].Error.IsError)
               {
                   if (consumer.Subscription.Count == 0)
                   {
                       consumer.Subscribe(topicName);
                       Console.WriteLine($"Resubscribed to topic {topicName}.");
                   }
               }
               else if (consumer.Subscription.Contains(topicName))
               {
                   consumer.Unsubscribe();
                   Console.WriteLine($"Unsubscribed from topic {topicName} as it is not available.");
               }
           }
           catch (KafkaException ex) when (ex.Error.Code == ErrorCode.UnknownTopicOrPart)
           {
               if (consumer.Subscription.Contains(topicName))
               {
                   consumer.Unsubscribe();
                   Console.WriteLine($"Unsubscribed from topic {topicName} as it is not available.");
               }
           }
           await Task.Delay(5000); // Wait for 5 seconds before checking again
       }
   }
   private static async Task ConsumeMessages(IConsumer<Ignore, string> consumer, CancellationToken cancellationToken)
   {
       try
       {
           while (!cancellationToken.IsCancellationRequested)
           {
               try
               {
                   var cr = consumer.Consume(cancellationToken);
                   Console.WriteLine($"Consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");
               }
               catch (ConsumeException e)
               {
                   Console.WriteLine($"Consume error: {e.Error.Reason}");
               }
               catch (OperationCanceledException)
               {
                   break;
               }
               catch (KafkaException e)
               {
                   Console.WriteLine($"Kafka error: {e.Error.Reason}");
               }
           }
       }
       finally
       {
           consumer.Close();
       }
   }
}