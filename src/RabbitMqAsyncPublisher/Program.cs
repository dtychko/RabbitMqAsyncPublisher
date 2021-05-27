﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.Remoting.Channels;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace RabbitMqAsyncPublisher
{
    public class Example
    {
        public static async Task Main0()
        {
            ThreadPool.SetMinThreads(10, 10);
            ThreadPool.SetMaxThreads(10, 10);

            var cancellationTokenSource = new CancellationTokenSource();
            var cancellationToken = cancellationTokenSource.Token;
            var cancellationTokenRegistration =
                cancellationToken.Register(() =>
                {
                    Thread.Sleep(3000);
                    Console.WriteLine("Registered callback");
                });

            cancellationTokenSource.Cancel();
            Console.WriteLine("Cancelled");

            // var tcs = new TaskCompletionSource<bool>();
            //
            // var t1 = Task.Run(() =>
            // {
            //     Thread.Sleep(3000);
            //
            //     Console.WriteLine("setting result ...");
            //     Task.Run(() => tcs.TrySetResult(true));
            //     // tcs.TrySetResult(true);
            //     Console.WriteLine("setting result completed");
            // }).ContinueWith(_ => Console.WriteLine("t1 completed"));
            //
            // var t2 = Task.Run(async () =>
            // {
            //     await tcs.Task.ConfigureAwait(false);
            //
            //     Console.WriteLine("running continuation ...");
            //     Thread.Sleep(3000);
            //     // throw new Exception();
            //     Console.WriteLine("running continuation completed");
            // }).ContinueWith(_ => Console.WriteLine("t2 completed"));
            //
            // Task.WaitAll(t1, t2);
        }
    }

    internal class Program
    {
        public static readonly Uri RabbitMqUri = new Uri("amqp://guest:guest@localhost:5678/");
        private const string QueueName = "test_queue";

        private const int MessageCount = 2000;
        private const int MessageSize = 1024 * 100;

        private const int NonAcknowledgedSizeLimit = 10_000_000;

        private static int _counter;

        public static void Main2()
        {
            ThreadPool.SetMaxThreads(100, 100);
            ThreadPool.SetMinThreads(100, 100);

            Console.WriteLine("Sync context " + SynchronizationContext.Current);
            Console.WriteLine("Task scheduler " + TaskScheduler.Default);

            var connectionFactory = new ConnectionFactory
            {
                Uri = RabbitMqUri,
                AutomaticRecoveryEnabled = false,
                TopologyRecoveryEnabled = false,
                ClientProvidedName = "rabbitmq-publish-tests",
                RequestedHeartbeat = TimeSpan.Zero
            };

            using (var publisherProxy = new AsyncPublisherProxy<bool>())
            using (var retryingPublisher = new AsyncPublisherWithRetries(
                publisherProxy, TimeSpan.FromSeconds(3), new AsyncPublisherWithRetriesConsoleDiagnostics()))
            {
                using (AutoRecovery.StartConnection(
                    connectionFactory,
                    _ => TimeSpan.FromSeconds(3),
                    new AutoRecoveryConsoleDiagnostics("connection/1"),
                    AutoRecovery.StartAutoRecoveryHealthCheck(TimeSpan.FromMinutes(1)),
                    connection =>
                    {
                        return AutoRecovery.StartModel(
                            connection,
                            _ => TimeSpan.FromSeconds(3),
                            new AutoRecoveryConsoleDiagnostics("model/1"),
                            model =>
                            {
                                model.ConfirmSelect();
                                var innerPublisher = new AsyncPublisher(model,
                                    new AsyncPublisherConsoleDiagnostics());
                                return publisherProxy.ConnectTo(innerPublisher);
                            });
                    }))
                {
                    foreach (var message in Utils.GenerateMessages(100, 10240))
                    {
                        Console.WriteLine("Sending next message");
                        retryingPublisher.PublishAsync("", "test.queue", message, Utils.CreateBasicProperties(),
                                default)
                            .Wait();
                        Console.WriteLine("Sent message");

                        Thread.Sleep(1000);
                    }

                    Console.WriteLine("DISPOSING AUTO_RECOVERY");
                }
            }

            Console.WriteLine("DISPOSED AUTO_RECOVERY");
            Thread.Sleep(5000);
            Console.WriteLine("EXIT");
        }

        public static void Main43()
        {
            ThreadPool.SetMaxThreads(100, 100);
            ThreadPool.SetMinThreads(100, 100);

            using (var connection = new ConnectionFactory
                    {Uri = RabbitMqUri, AutomaticRecoveryEnabled = true, ClientProvidedName = "rabbitmq-publish-tests"}
                .CreateConnection())
            using (var model = connection.CreateModel())
            {
                connection.ConnectionShutdown +=
                    (sender, args) =>
                    {
                        Console.WriteLine(
                            $" >> [{Thread.CurrentThread.ManagedThreadId}] Connection:ConnectionShutdown");
                    };
                ((IAutorecoveringConnection) connection).RecoverySucceeded += (sender, args) =>
                    Console.WriteLine($" >> [{Thread.CurrentThread.ManagedThreadId}] Connection:RecoverySucceeded");
                ((IAutorecoveringConnection) connection).ConnectionRecoveryError += (sender, args) =>
                    Console.WriteLine(
                        $" >> [{Thread.CurrentThread.ManagedThreadId}] Connection:ConnectionRecoveryError");

                model.BasicAcks +=
                    (sender, args) =>
                        Console.WriteLine($" >> [{Thread.CurrentThread.ManagedThreadId}] Model:BasicAcks");
                model.ModelShutdown +=
                    (sender, args) =>
                    {
                        Console.WriteLine(
                            $" >> [{Thread.CurrentThread.ManagedThreadId}] Model:ModelShutdown SeqNo={model.NextPublishSeqNo}");
                        Thread.Sleep(12000);
                        Console.WriteLine(
                            $" >> [{Thread.CurrentThread.ManagedThreadId}] Model:ModelShutdown2 SeqNo={model.NextPublishSeqNo}");
                    };
                ((IRecoverable) model).Recovery +=
                    (sender, args) =>
                        Console.WriteLine(
                            $" >> [{Thread.CurrentThread.ManagedThreadId}] Model:Recovery SeqNo={model.NextPublishSeqNo}");

                model.ConfirmSelect();
                model.QueueDeclare(QueueName, true, false, false);

                var publisher = new AsyncPublisher(model);

                for (var i = 0; i < 1000; i++)
                {
                    try
                    {
                        Console.WriteLine($" >> [{Thread.CurrentThread.ManagedThreadId}] Publishing#{i} ...");
                        var properties = model.CreateBasicProperties();
                        properties.Persistent = true;
                        Console.WriteLine(
                            $" ** [{Thread.CurrentThread.ManagedThreadId}] Next seqno: {model.NextPublishSeqNo}");
                        publisher.PublishAsync(
                                "",
                                QueueName,
                                Encoding.UTF8.GetBytes(Utils.GenerateString(1024)),
                                properties,
                                CancellationToken.None)
                            .Wait();
                        Console.WriteLine($" >> [{Thread.CurrentThread.ManagedThreadId}] Published#{i}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(
                            $" >> [{Thread.CurrentThread.ManagedThreadId}] Failed#{i} => {ex.GetType().Name}");
                    }

                    Thread.Sleep(5000);
                }

                model.Close();
                connection.Close();
            }
        }

        public static void Main85()
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

                model.ExchangeDeclare("test_exchange", "topic", true, false);
                for (var i = 0; i < 14; i++)
                {
                    var queueName = $"test_queue_{i}";
                    model.QueueDeclare(queueName, true, false, false);
                    model.QueuePurge(queueName);
                    model.QueueBind(queueName, "test_exchange", "#");
                }

                // model.QueueDeclare(QueueName, true, false, false);
                // model.QueuePurge(QueueName);

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
                var properties = model.CreateBasicProperties();
                properties.Persistent = true;
                publisher.Publish(messages.Dequeue(), properties);
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
                // new AsyncPublisherSyncDecorator<bool>(
                new AsyncPublisher(model),
                // ),
                "test_exchange",
                "some topic"
            );
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

                var properties = model.CreateBasicProperties();
                properties.Persistent = true;
                tasks.Add(publisher.PublishAsync(message, properties).ContinueWith(_ =>
                {
                    if (Interlocked.Add(ref nonAcknowledgedSize, -message.Length) <
                        Math.Max(1, nonAcknowledgedSizeLimit / 2))
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

    public class AsyncPublisherAdapter<TResult>
    {
        private readonly IAsyncPublisher<TResult> _publisher;
        private readonly string _exchange;
        private readonly string _queueName;

        public AsyncPublisherAdapter(IAsyncPublisher<TResult> publisher, string exchange, string queueName)
        {
            _publisher = publisher;
            _exchange = exchange;
            _queueName = queueName;
        }

        public Task<TResult> PublishAsync(ReadOnlyMemory<byte> message, IBasicProperties properties)
        {
            return _publisher.PublishAsync(_exchange, _queueName, message, properties, CancellationToken.None);
        }
    }
}