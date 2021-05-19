using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using RabbitMqAsyncPublisher;

namespace Tests
{
    [TestFixture]
    public class Tests
    {
        private static readonly Random _random = new Random();

        [Test]
        public async Task Test1([Values(1_000)] int publishTaskCount)
        {
            ThreadPool.SetMaxThreads(1000, 1000);
            ThreadPool.SetMinThreads(1000, 1000);

            var counter = 0;
            
            var model = new TestRabbitModel(request =>
            {
                if (Interlocked.Increment(ref counter) % 100 == 0)
                {
                    throw new AlreadyClosedException(new ShutdownEventArgs(ShutdownInitiator.Peer, 0, String.Empty));
                }

                async Task<bool> Run()
                {
                    await Task.Delay(_random.Next(0, 100));
                    return true;
                }

                return Run();
            });

            var cts = new CancellationTokenSource(Debugger.IsAttached
                ? TimeSpan.FromMinutes(5)
                : TimeSpan.FromSeconds(10));

            using (var publisher =
                new AsyncPublisherSyncDecorator<RetryingPublisherResult>(
                    new AsyncPublisherWithRetries(new AsyncPublisher(model), TimeSpan.FromMilliseconds(10))))
            {
                var publishTasks = new ConcurrentBag<Task>();

                for (var i = 0; i < publishTaskCount; i++)
                {
                    var body = Encoding.UTF8.GetBytes($"Message #{i}");
                    publishTasks.Add(Task.Run(() => publisher.PublishUnsafeAsync(
                        "test-exchange", "no-routing", body, new TestBasicProperties(), cts.Token), cts.Token));
                }

                await Task.WhenAll(publishTasks);

                Assert.AreEqual(publishTaskCount, model.PublishCalls.Count);
                Assert.AreEqual(1010, counter);
            }
        }
    }
}