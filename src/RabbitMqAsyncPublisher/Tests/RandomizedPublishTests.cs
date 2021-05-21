using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using RabbitMqAsyncPublisher;
using Shouldly;
using static Tests.TestUtils;

namespace Tests
{
    [TestFixture]
    public class RandomizedPublishTests
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
                using (var publisher = CreatePublisher(model, new TestDiagnostics(), TimeSpan.FromMilliseconds(10)))
                {
                    var publishTasks = new ConcurrentBag<Task>();
                    var counter = 0;

                    for (var i = 0; i < publishTaskCount; i++)
                    {
                        publishTasks.Add(Task.Run(() =>
                            {
                                var body = Encoding.UTF8.GetBytes($"Message #{Interlocked.Increment(ref counter)}");

                                return publisher.PublishUnsafeAsync(
                                    "test-exchange", "no-routing", body, new TestBasicProperties(), cancellationToken);
                            },
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
        [TestCase(500, 10, 20, 20, 40, 30, 70, 10)]
        [TestCase(500, 1, 2, 1, 2, 20, 80, 10)]
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
                Console.WriteLine($"Starting next publish for {GetMessageTag(request.Properties)}");
                SyncWait(_random.Next(minSyncPublishWaitTime, maxSyncPublishWaitTime));
                await Task.Delay(_random.Next(minAsyncPublishWaitTime, minAsyncPublishWaitTime));
                return true;
            });

            var allPublishesFinished = false;

            await RunWithTimeout(async cancellationToken =>
            {
                var diagnostics = new TestDiagnostics();
                var publishUnsafeTasks = new List<PublishTask>();
                using (var publisher = CreatePublisher(model, diagnostics, TimeSpan.FromMilliseconds(retryDelayMs)))
                {
                    Task modelLifecycleTask = default;
                    modelLifecycleTask = Task.Run(() =>
                    {
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

                    var publishTasks = new List<Task>();
                    var counter = 0;

                    for (var i = 0; i < publishTaskCount; i++)
                    {
                        publishTasks.Add(Task.Run(
                            () =>
                            {
                                // Publish tasks are enqueued into publisher under lock
                                // to ensure that they start executing in predictable order,
                                // so that we can make test assertions regarding their retry order.
                                lock (publishTasks)
                                {
                                    var messageIndex = Interlocked.Increment(ref counter);
                                    var messageTag = $"Message #{messageIndex}";
                                    var body = Encoding.UTF8.GetBytes(messageTag);
                                    var testBasicProperties = new TestBasicProperties {TestTag = messageTag};
                                    var unsafeTask = publisher.PublishUnsafeAsync(
                                        "test-exchange", "no-routing", body, testBasicProperties, cancellationToken);
                                    publishUnsafeTasks.Add(
                                        new PublishTask {Task = unsafeTask, MessageIndex = messageIndex, MessageTag = messageTag});
                                    return unsafeTask;
                                }
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

                AssertRetriesAreOrdered(publishUnsafeTasks);
                AssertAllPublishesAreAcked(model, publishUnsafeTasks);
            });
        }

        /// <remarks>
        /// By the end of the test all messages should be successfully published (with or without retries).
        /// This method checks that test model received all necessary <see cref="TestRabbitModel.BasicPublish"/> calls,
        /// was able to complete them and fire ACK messages via <see cref="TestRabbitModel.BasicAcks"/>. 
        /// </remarks>
        private static void AssertAllPublishesAreAcked(
            TestRabbitModel model,
            IEnumerable<PublishTask> publishTasks)
        {
            var unackedTags = new List<string>();

            foreach (var pt in publishTasks)
            {
                if (model.Acks.Count(a => pt.MessageTag == GetMessageTag(a.Properties)) != 1)
                {
                    unackedTags.Add(pt.MessageTag);
                }
            }

            if (unackedTags.Any())
            {
                Assert.Fail($"There is no ack for {string.Join(", ", unackedTags)}");
            }
        }

        /// <remarks>
        /// When publish error happens, publisher transitions into "sequential" state,
        /// and retries are attempted in the same order the messages were enqueued into publisher,
        /// so that output message order is preserved.
        /// This method checks that when retries happen, they are indeed ordered.
        /// </remarks>
        private static void AssertRetriesAreOrdered(
            IEnumerable<PublishTask> publishTasks)
        {
            var retries = publishTasks
                .Where(x => x.Task.Result.Retries > 0)
                .ToList();

            Console.WriteLine($"Retries count = {retries.Count}");

            retries.Aggregate((a, b) =>
            {
                if (a.MessageIndex > b.MessageIndex)
                {
                    Assert.Fail(
                        $"Publish retry order is broken: {a.MessageTag}({a.MessageIndex}) > {b.MessageTag}({b.MessageIndex}). All retries: {string.Join(",", retries.Select(r => $"{r.MessageTag}({r.MessageIndex})"))}");
                }

                return b;
            });
        }

        private AsyncPublisherSyncDecorator<RetryingPublisherResult> CreatePublisher(
            TestRabbitModel model, TestDiagnostics diagnostics,
            TimeSpan retryDelay)
        {
            return new AsyncPublisherSyncDecorator<RetryingPublisherResult>(
                new AsyncPublisherWithRetries(
                    new AsyncPublisher(model, diagnostics),
                    retryDelay, diagnostics));
        }

        /// <summary>
        /// Represents a task/job to publish a single message in test scenario
        /// </summary>
        private class PublishTask
        {
            /// <summary>
            /// Task returned by publisher implementation.
            /// </summary>
            public Task<RetryingPublisherResult> Task { get; set; }

            /// <summary>
            /// Represents message generation order, later messages have larger values.
            /// </summary>
            public int MessageIndex { get; set; }
            
            /// <summary>
            /// Unique message identifier which is also appended to message's <see cref="TestBasicProperties"/>.
            /// Can be used to correlate messages in assertions and under debug.
            /// </summary>
            public string MessageTag { get; set; }
        }
    }
}