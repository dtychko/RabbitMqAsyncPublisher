using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitMqAsyncPublisher
{
    public interface IAsyncPublisherDiagnostics
    {
        void TrackBasicAcksEventProcessing(BasicAckEventArgs args);

        void TrackBasicAcksEventProcessingFailed(BasicAckEventArgs args, TimeSpan duration, Exception ex);

        void TrackBasicAcksEventProcessingCompleted(BasicAckEventArgs args, TimeSpan duration);

        void TrackBasicNacksEventProcessing(BasicNackEventArgs args);

        void TrackBasicNacksEventProcessingFailed(BasicNackEventArgs args, TimeSpan duration, Exception ex);

        void TrackBasicNacksEventProcessingCompleted(BasicNackEventArgs args, TimeSpan duration);

        void TrackModelShutdownEventProcessing(ShutdownEventArgs args);

        void TrackModelShutdownEventProcessingFailed(ShutdownEventArgs args, TimeSpan duration, Exception ex);

        void TrackModelShutdownEventProcessingCompleted(ShutdownEventArgs args, TimeSpan duration);

        void TrackRecoveryEventProcessing();

        void TrackRecoveryEventProcessingFailed(TimeSpan duration, Exception ex);

        void TrackRecoveryEventProcessingCompleted(TimeSpan duration);

        void TrackUnsupportedSignal(string state, string signal);

        void TrackPublishUnsafe(PublishUnsafeArgs args);

        void TrackPublishUnsafeCanceled(PublishUnsafeArgs args, TimeSpan duration);

        void TrackPublishUnsafeFailed(PublishUnsafeArgs args, TimeSpan duration, Exception ex);

        void TrackPublishUnsafePublished(PublishUnsafeArgs args, TimeSpan duration);

        void TrackPublishUnsafeCompleted(PublishUnsafeArgs args, TimeSpan duration, bool acknowledged);
    }

    public class PublishUnsafeArgs
    {
        public string Exchange { get; }

        public string RoutingKey { get; }

        public ReadOnlyMemory<byte> Body { get; }

        public IBasicProperties Properties { get; }

        public PublishUnsafeArgs(
            string exchange,
            string routingKey,
            ReadOnlyMemory<byte> body,
            IBasicProperties properties)
        {
            Exchange = exchange;
            RoutingKey = routingKey;
            Body = body;
            Properties = properties;
        }
    }

    internal class EmptyDiagnostics : IAsyncPublisherDiagnostics
    {
        public static readonly EmptyDiagnostics Instance = new EmptyDiagnostics();

        private EmptyDiagnostics()
        {
        }

        public void TrackBasicAcksEventProcessing(BasicAckEventArgs args)
        {
        }

        public void TrackBasicAcksEventProcessingFailed(BasicAckEventArgs args, TimeSpan duration, Exception ex)
        {
        }

        public void TrackBasicAcksEventProcessingCompleted(BasicAckEventArgs args, TimeSpan duration)
        {
        }

        public void TrackBasicNacksEventProcessing(BasicNackEventArgs args)
        {
        }

        public void TrackBasicNacksEventProcessingFailed(BasicNackEventArgs args, TimeSpan duration, Exception ex)
        {
        }

        public void TrackBasicNacksEventProcessingCompleted(BasicNackEventArgs args, TimeSpan duration)
        {
        }

        public void TrackModelShutdownEventProcessing(ShutdownEventArgs args)
        {
        }

        public void TrackModelShutdownEventProcessingFailed(ShutdownEventArgs args, TimeSpan duration, Exception ex)
        {
        }

        public void TrackModelShutdownEventProcessingCompleted(ShutdownEventArgs args, TimeSpan duration)
        {
        }

        public void TrackRecoveryEventProcessing()
        {
        }

        public void TrackRecoveryEventProcessingFailed(TimeSpan duration, Exception ex)
        {
        }

        public void TrackRecoveryEventProcessingCompleted(TimeSpan duration)
        {
        }

        public void TrackUnsupportedSignal(string state, string signal)
        {
        }

        public void TrackPublishUnsafe(PublishUnsafeArgs args)
        {
        }

        public void TrackPublishUnsafeCanceled(PublishUnsafeArgs args, TimeSpan duration)
        {
        }

        public void TrackPublishUnsafeFailed(PublishUnsafeArgs args, TimeSpan duration, Exception ex)
        {
        }

        public void TrackPublishUnsafePublished(PublishUnsafeArgs args, TimeSpan duration)
        {
        }

        public void TrackPublishUnsafeCompleted(PublishUnsafeArgs args, TimeSpan duration, bool acknowledged)
        {
        }
    }

    public class Example
    {
        public static async Task Main()
        {
            ThreadPool.SetMinThreads(10, 10);
            ThreadPool.SetMaxThreads(10, 10);

            var cts = new CancellationTokenSource(2000);
            var ct = cts.Token;

            var task = Task.Run(() =>
            {
                Console.WriteLine("Running ...");

                while (true)
                {
                    ct.ThrowIfCancellationRequested();
                }
            }, ct).ContinueWith(_ => { Console.WriteLine($"Running continuation ({_.Status}) ..."); }, ct);

            try
            {
                await Task.WhenAll(task, Task.FromException(new Exception()));
                // Console.WriteLine(t.Status);
                // Console.WriteLine(t.Id);
            }
            catch (OperationCanceledException ex)
            {
                Console.WriteLine("CANCELLED {0}", ex);
            }
            catch (Exception ex)
            {
                Console.WriteLine("UNEXPECTED ERROR  {0}", ex);
            }
            finally
            {
                Console.WriteLine(task.Status);
                cts.Dispose();
            }
        }
    }

    internal class Program
    {
        private static readonly Uri RabbitMqUri = new Uri("amqp://guest:guest@localhost:5672/");
        private const string QueueName = "test_queue";

        private const int MessageCount = 100;
        private const int MessageSize = 1024 * 10000;

        private const int NonAcknowledgedSizeLimit = 50;

        private static int _counter;

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
                        var properties = model.CreateBasicProperties();
                        properties.Persistent = true;
                        publisher.PublishUnsafeAsync(
                                "",
                                QueueName,
                                Encoding.UTF8.GetBytes(Utils.GenerateString(1024)),
                                properties,
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
                new AsyncPublisherSyncDecorator<bool>(
                    AsyncPublisherDeclaringDecorator.Create(
                        new AsyncPublisher(model),
                        AsyncPublisherDeclaringDecorator.QueueDeclarator(QueueName)
                    )
                ),
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
}