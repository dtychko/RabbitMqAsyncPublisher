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
using Shouldly;

namespace Tests
{
    [TestFixture]
    public class Tests
    {
        private static readonly Random _random = new Random();

        [Test]
        public async Task ShouldRetryWhenBasicPublishThrowsSynchronousException(
            [Values(1000)] int publishTaskCount,
            [Values(100)] int failEveryNth)
        {
            ConfigureThreadPool(publishTaskCount);

            var modelPublishCount = 0;

            var model = new TestRabbitModel(request =>
            {
                if (Interlocked.Increment(ref modelPublishCount) % failEveryNth == 0)
                {
                    throw new AlreadyClosedException(new ShutdownEventArgs(ShutdownInitiator.Peer, 0, String.Empty));
                }

                async Task<bool> Run()
                {
                    await Task.Delay(_random.Next(1, 129));
                    return true;
                }

                return Run();
            });

            await RunWithTimeout(async cancellationToken =>
            {
                using (var publisher =
                    new AsyncPublisherSyncDecorator<RetryingPublisherResult>(
                        new AsyncPublisherWithRetries(new AsyncPublisher(model), TimeSpan.FromMilliseconds(10))))
                {
                    var publishTasks = new ConcurrentBag<Task>();

                    for (var i = 0; i < publishTaskCount; i++)
                    {
                        var body = Encoding.UTF8.GetBytes($"Message #{i}");
                        publishTasks.Add(Task.Run(() => publisher.PublishUnsafeAsync(
                                "test-exchange", "no-routing", body, new TestBasicProperties(), cancellationToken),
                            cancellationToken));
                    }

                    await Task.WhenAll(publishTasks);

                    var expectedPublishCount = publishTaskCount + publishTaskCount / failEveryNth;
                    model.PublishCalls.Count.ShouldBe(expectedPublishCount);
                    modelPublishCount.ShouldBe(expectedPublishCount);
                }
            });
        }

        private static void ConfigureThreadPool(int numberOfPublishers)
        {
            var totalThreadCount = numberOfPublishers + 10;
            ThreadPool.SetMaxThreads(totalThreadCount, totalThreadCount);
            ThreadPool.SetMinThreads(totalThreadCount, totalThreadCount);
        }

        private static async Task RunWithTimeout(Func<CancellationToken, Task> test)
        {
            using (var cts = new CancellationTokenSource(Debugger.IsAttached
                ? TimeSpan.FromMinutes(5)
                : TimeSpan.FromSeconds(10)))
            {
                var testTask = test(cts.Token);

                var resultTask = await Task.WhenAny(
                    Task.Delay(-1, cts.Token),
                    testTask);

                cts.IsCancellationRequested.ShouldBeFalse();

                if (resultTask == testTask)
                {
                    await testTask;
                }
            }
        }
    }
}