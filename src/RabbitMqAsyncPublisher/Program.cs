using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace RabbitMqAsyncPublisher
{
    internal class Program
    {
        private static readonly Uri RabbitMqUri = new Uri("amqp://guest:guest@localhost:5672/");
        private const string QueueName = "test_queue";

        private const int MessageCount = 100_000;
        private const int MessageSize = 1024 * 1;

        private const int NonAcknowledgedSizeLimit = 10_000_000;

        private static int _counter;

        public static void Main()
        {
            var source = new CancellationTokenSource(2000);
            
            Task.Run(async () =>
            {
                try
                {
                    await Foo(source.Token);
                }
                catch (TaskCanceledException)
                {
                    Console.WriteLine("Foo cancelled");
                }
                catch (Exception)
                {
                    Console.WriteLine("Foo exception");
                }

                Console.WriteLine("Foo completed");
            }, source.Token).Wait(source.Token);
            
            Console.WriteLine("Main completed");
        }

        private static async Task Foo(CancellationToken cancellationToken)
        {
            try
            {
                Console.WriteLine("Starting ...");
                await Task.Delay(3000, cancellationToken);
                Console.WriteLine("Throwing ...");
                throw new Exception();
            }
            catch (Exception)
            {
                Console.WriteLine("Exception processing ...");
                await Task.Delay(3000, cancellationToken);
                Console.WriteLine("Exception processed");
            }
            finally
            {
                Console.WriteLine("Finally");
            }
        }
        
        public static void Main1()
        {
            ThreadPool.SetMaxThreads(100, 100);
            ThreadPool.SetMinThreads(100, 100);

            using (var connection = new ConnectionFactory {Uri = RabbitMqUri, AutomaticRecoveryEnabled = true}
                .CreateConnection())
            using (var model = connection.CreateModel())
            {
                connection.ConnectionShutdown +=
                    (sender, args) => Console.WriteLine(" >> Connection:ConnectionShutdown");
                ((IAutorecoveringConnection) connection).RecoverySucceeded += (sender, args) =>
                    Console.WriteLine(" >> Connection:RecoverySucceeded");
                ((IAutorecoveringConnection) connection).ConnectionRecoveryError += (sender, args) =>
                    Console.WriteLine(" >> Connection:ConnectionRecoveryError");

                model.ConfirmSelect();
                // model.QueueDeclare(QueueName, true, false, false);

                var publisher = AsyncPublisherDeclaringDecorator.Create(
                    new AsyncPublisher(model),
                    AsyncPublisherDeclaringDecorator.QueueDeclarator(QueueName)
                );

                for (var i = 0; i < 1000; i++)
                {
                    try
                    {
                        Console.WriteLine($" >> Publising#{i} ...");
                        publisher.PublishAsync("", QueueName, Encoding.UTF8.GetBytes(Utils.GenerateString(1024)),
                                CancellationToken.None)
                            .Wait();
                        Console.WriteLine($" >> Published#{i}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($" >> Failed#{i} => {ex.GetType().Name}");
                    }

                    Thread.Sleep(5000);
                }

                model.Close();
                connection.Close();
            }
        }

        public static void Main2()
        {
            ThreadPool.SetMaxThreads(100, 100);
            ThreadPool.SetMinThreads(100, 100);

            Console.WriteLine(" >> Generating messages ...");
            var messages = Utils.GenerateMessages(MessageCount, MessageSize);
            Console.WriteLine(" >> Generating messages completed");

            var scheduledCount = messages.Count;

            using (var connection = new ConnectionFactory {Uri = RabbitMqUri}.CreateConnection())
            using (var model = connection.CreateModel())
            {
                model.ConfirmSelect();
                model.QueueDeclare(QueueName, true, false, false);
                model.QueuePurge(QueueName);

                StartRateMeasurement();

                var stopwatch = Stopwatch.StartNew();

                // PublishAllSync(model, messages);
                PublishAllAsync(model, messages, NonAcknowledgedSizeLimit);

                stopwatch.Stop();
                Console.WriteLine(scheduledCount / stopwatch.ElapsedMilliseconds * 1000);
            }
        }

        private static void PublishAllSync(
            IModel model,
            Queue<ReadOnlyMemory<byte>> messages)
        {
            var publisher = new SyncPublisher(model);

            while (messages.Count > 0)
            {
                publisher.Publish(messages.Dequeue());
                Interlocked.Increment(ref _counter);
            }
        }

        private static void PublishAllAsync(
            IModel model,
            Queue<ReadOnlyMemory<byte>> messages,
            int nonAcknowledgedSizeLimit)
        {
            // var publisher = new AsyncRetryingPublisher(new AsyncPublisher(model, QueueName));
            var publisher = new AsyncPublisherAdapter<bool>(
                AsyncPublisherDeclaringDecorator.Create(
                    new AsyncPublisher(model),
                    AsyncPublisherDeclaringDecorator.QueueDeclarator(QueueName)
                ),
                QueueName);
            var tasks = new List<Task>();
            var manualResetEvent = new ManualResetEventSlim(true);
            var nonAcknowledgedSize = 0;

            while (messages.Count > 0)
            {
                manualResetEvent.Wait();

                var message = messages.Dequeue();
                if (Interlocked.Add(ref nonAcknowledgedSize, message.Length) > nonAcknowledgedSizeLimit)
                {
                    manualResetEvent.Reset();
                }

                tasks.Add(publisher.PublishAsync(message).ContinueWith(_ =>
                {
                    if (Interlocked.Add(ref nonAcknowledgedSize, -message.Length) < nonAcknowledgedSizeLimit / 2)
                    {
                        manualResetEvent.Set();
                    }

                    Interlocked.Increment(ref _counter);
                }));
            }

            Task.WaitAll(tasks.ToArray());
        }

        private static void StartRateMeasurement()
        {
            Task.Run(() =>
            {
                while (true)
                {
                    var prev = Volatile.Read(ref _counter);
                    Thread.Sleep(1000);
                    var curr = Volatile.Read(ref _counter);
                    Console.WriteLine($" >> Rate = {curr - prev}");
                }
            });
        }
    }
}