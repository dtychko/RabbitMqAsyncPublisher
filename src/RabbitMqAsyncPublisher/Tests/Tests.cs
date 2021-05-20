using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
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

        [Test]
        [TestCase(1, 1, 2, 2, 4, 20, 80, 10)]
        [TestCase(100, 1, 2, 2, 4, 20, 80, 10)]
        [TestCase(500, 1, 2, 1, 2, 5, 95, 3)]
        public async Task ShouldRetryWhenModelShutdownsAndRecovers(
            int publishTaskCount,
            int minSyncPublishWaitTime,
            int maxSyncPublishWaitTime,
            int minAsyncPublishWaitTime,
            int maxAsyncPublishWaitTime,
            int shutdownDuration,
            int recoveryDuration,
            int retryDelayMs)
        {
            ConfigureThreadPool(publishTaskCount);

            var model = new TestRabbitModel(async request =>
            {
                Console.WriteLine($"Starting next publish for {request.DeliveryTag}");
                SyncWait(_random.Next(minSyncPublishWaitTime, maxSyncPublishWaitTime));
                await Task.Delay(_random.Next(minAsyncPublishWaitTime, minAsyncPublishWaitTime));
                return true;
            });

            var allPublishesFinished = false;

            await RunWithTimeout(async cancellationToken =>
            {
                var diagnostics = new TestDiagnostics();
                using (var publisher =
                    new AsyncPublisherSyncDecorator<RetryingPublisherResult>(
                        new AsyncPublisherWithRetries(
                            new AsyncPublisher(model, diagnostics),
                            TimeSpan.FromMilliseconds(retryDelayMs), diagnostics)))
                {
                    Task modelLifecycleTask = default;
                    modelLifecycleTask = Task.Run(() =>
                    {
                        //Task.Delay(_random.Next(5, 8), cancellationToken).Wait(cancellationToken);
                        model.FireModelShutdown(new ShutdownEventArgs(default, default, default));

                        while (!Volatile.Read(ref allPublishesFinished))
                        {
                            Console.WriteLine(
                                $"Starting next model lifecycle step: {allPublishesFinished}, {cancellationToken.IsCancellationRequested}, {modelLifecycleTask.Status}");

                            Task.Delay(shutdownDuration, cancellationToken).Wait(cancellationToken);
                            model.FireRecovery(EventArgs.Empty);

                            Task.Delay(recoveryDuration, cancellationToken).Wait(cancellationToken);
                            model.FireModelShutdown(new ShutdownEventArgs(default, default, default));
                        }
                    }, cancellationToken);

                    var publishTasks = new ConcurrentBag<Task>();

                    for (var i = 0; i < publishTaskCount; i++)
                    {
                        var messageTag = $"Message #{i}";
                        var body = Encoding.UTF8.GetBytes(messageTag);
                        publishTasks.Add(Task.Run(
                            () =>
                            {
                                var testBasicProperties = new TestBasicProperties {TestTag = messageTag};
                                return publisher.PublishUnsafeAsync(
                                    "test-exchange", "no-routing", body, testBasicProperties, cancellationToken);
                            },
                            cancellationToken));
                    }

                    try
                    {
                        await Task.WhenAll(publishTasks);
                    }
                    finally
                    {
                        Volatile.Write(ref allPublishesFinished, true);
                    }

                    Console.WriteLine(
                        $"~~~ finished with {model.PublishCalls.Count} publishes: {string.Join(",", publishTasks.Select(x => x.Status))}");

                    await modelLifecycleTask;
                }

                diagnostics.FailedRetryAttemptCount.ShouldBeGreaterThan(0);
                model.PublishCalls.Count.ShouldBeGreaterThanOrEqualTo(publishTaskCount);
            });
        }

        private static void SyncWait(int iterations = 5)
        {
            var wait = new SpinWait();
            for (int i = 0; i < iterations; i++)
            {
                wait.SpinOnce();
            }
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
                : TimeSpan.FromSeconds(30)))
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

    internal class TestDiagnostics : EmptyDiagnostics
    {
        public int FailedRetryAttemptCount { get; private set; }
        
        public override void TrackPublishUnsafeAttemptFailed(PublishUnsafeAttemptArgs args, TimeSpan duration,
            Exception ex)
        {
            FailedRetryAttemptCount++;
            Console.WriteLine($"{GetTestTag(args)}/{args.Attempt}/error {duration.TotalMilliseconds}");
        }

        public override void TrackPublishUnsafeAttemptCompleted(PublishUnsafeAttemptArgs args, TimeSpan duration,
            bool acknowledged)
        {
            Console.WriteLine($"{GetTestTag(args)}/{args.Attempt}/completed {duration.TotalMilliseconds}");
        }

        public override void TrackRecoveryEventProcessing()
        {
            Console.WriteLine("model/recovery/processing");
        }

        public override void TrackRecoveryEventProcessingCompleted(TimeSpan duration)
        {
            Console.WriteLine($"model/recovery/completed");
        }

        public override void TrackModelShutdownEventProcessing(ShutdownEventArgs args)
        {
            Console.WriteLine("model/shutdown/processing");
        }

        public override void TrackModelShutdownEventProcessingCompleted(ShutdownEventArgs args, TimeSpan duration)
        {
            Console.WriteLine("model/shutdown/completed");
        }

        private static string GetTestTag(PublishArgs args) => ((TestBasicProperties) args.Properties).TestTag;
    }
}