using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitMqAsyncPublisher
{
    internal class BrokerOrderingTest
    {
        private const string QueueName = "test.ordering.queue";

        public static async Task Main()
        {
            var connectionFactory = new ConnectionFactory
            {
                Uri = Program.RabbitMqUri,
                AutomaticRecoveryEnabled = false,
                TopologyRecoveryEnabled = false,
                ClientProvidedName = "rabbitmq-publish-tests",
                //RequestedHeartbeat = TimeSpan.Zero
            };
            using (var publishConnection = connectionFactory.CreateConnection("test-publish-connection"))
            using (var consumeConnection = connectionFactory.CreateConnection("test-consume-connection"))
            using (var publishModel = publishConnection.CreateModel())
            using (var consumeModel = consumeConnection.CreateModel())
            {
                publishModel.ConfirmSelect();
                consumeModel.ConfirmSelect();
                //consumeModel.QueueDeclare(QueueName, true, false, false);

                var publishTask = Publish(publishModel);
                
                await Consume(consumeModel);

                await publishTask;

                await Task.Delay(3_000);
                //await consumerTask;
                Console.WriteLine(" >> Finished consuming");
            }
        }

        private static async Task Publish(IModel publishModel)
        {
            publishModel.QueuePurge(QueueName);
            
            await Task.Run(async () =>
            {
                using (var publisher = new AsyncPublisher(publishModel))
                {
                    var tasks = new[]
                    {
                        Publish(1, 10 * 1024 * 1024),
                        Publish(2, 100),
                        Publish(3, 10 * 1024 * 1024)
                    };

                    async Task Publish(int counter, int size)
                    {
                        Console.WriteLine($" >> {DateTime.Now:O} Starting publish #{counter}");
                        var basicProperties = publishModel.CreateBasicProperties();
                        basicProperties.Headers = new Dictionary<string, object> {["X-Counter"] = counter};
                        var task = publisher.PublishAsync(
                            "", QueueName, new ReadOnlyMemory<byte>(new byte[size]), basicProperties, default);
                        Console.WriteLine($" >> {DateTime.Now:O} Finished sync part of publishing #{counter}");
                        await task.ConfigureAwait(false);
                        Console.WriteLine($" >> {DateTime.Now:O} Finished publishing #{counter}");
                    }

                    Func<Task> PublishRaw(int counter, int size)
                    {
                        return () => Task.Run(() =>
                        {
                            Console.WriteLine($" >> {DateTime.Now:O} Starting publish #{counter}");
                            var basicProperties = publishModel.CreateBasicProperties();
                            basicProperties.Headers = new Dictionary<string, object> {["X-Counter"] = counter};
                            publishModel.BasicPublish(
                                "", QueueName, false, basicProperties, new ReadOnlyMemory<byte>(new byte[size]));
                            Console.WriteLine($" >> {DateTime.Now:O} Finished publishing #{counter}");
                        });
                    }
                    //
                    // foreach (var task in tasks)
                    // {
                    //     await task();
                    // }

                    await Task.WhenAll(tasks);
                }
            });
            
            Console.WriteLine(" >> Finished publishing all");
        }

        private static async Task Consume(IModel consumeModel)
        {
            var consumerTask = Task.Run(() =>
            {
                var lastSeenNumber = int.MinValue;

                var consumer = new EventingBasicConsumer(consumeModel);
                consumer.Received += (sender, receiveArgs) =>
                {
                    try
                    {
                        Console.WriteLine(" >> Received message");
                        var receivedNumber = int.Parse(receiveArgs.BasicProperties.Headers["X-Counter"].ToString());
                        Console.WriteLine($" >> Received header {receivedNumber}");

                        if (receivedNumber < lastSeenNumber)
                        {
                            Console.WriteLine($" !!!! Received {receivedNumber}, last seen {lastSeenNumber}");
                        }

                        lastSeenNumber = receivedNumber;
                        consumeModel.BasicAck(receiveArgs.DeliveryTag, false);

                        Console.WriteLine(" >> Finished receive handler");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($" >> Receive exception {ex}");
                    }
                };
                var consumerTag = "test-basic-consume-ordering";
                //consumeModel.BasicCancel(consumerTag);
                consumeModel.BasicConsume(QueueName, false, consumerTag, consumer);
            });

            await consumerTask;
        }
    }
}