using System;
using System.Collections.Generic;
using System.Diagnostics;
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
            var publisher = new AsyncPublisher(model, QueueName);
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