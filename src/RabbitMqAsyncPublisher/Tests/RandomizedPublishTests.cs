using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NSubstitute;
using NUnit.Framework;
using RabbitMQ.Client;
using RabbitMqAsyncPublisher;
using Shouldly;
using static Tests.TestUtils;

namespace Tests
{
    [TestFixture]
    public class RandomizedPublishTests
    {
        [OneTimeSetUp]
        public void TestFixtureSetUp()
        {
            ConfigureThreadPool(1000);
        }

        [Test]
        [TestCase(87654, 10, 5, 10, 20, 40, 20, 80, 10)]
        public async Task LifecycleIntegrationTest(
            int seed,
            int publishTaskCount,
            int minSyncPublishSpinIterations,
            int maxSyncPublishSpinIterations,
            int minAsyncPublishWaitTime,
            int maxAsyncPublishWaitTime,
            int minShutdownDuration,
            int maxShutdownDuration,
            int retryDuration)
        {
            var random = new Random(seed);
            var createdModels = new List<TestNonRecoverableRabbitModel>();

            var connectionFactory = Substitute.For<IConnectionFactory>();
            connectionFactory.CreateConnection().ReturnsForAnyArgs(c =>
            {
                return new TestNonRecoverableConnection(() =>
                {
                    var model = new TestNonRecoverableRabbitModel(async publishArgs =>
                    {
                        Console.WriteLine($" >> Starting next publish for UNKNOWN_MESSAGE_TAG");
                        SyncWait(random.Next(minSyncPublishSpinIterations, maxSyncPublishSpinIterations));
                        await Task.Delay(random.Next(minAsyncPublishWaitTime, maxAsyncPublishWaitTime));
                        return true;
                    });
                    createdModels.Add(model);
                    return model;
                });
            });

            var diagnostics = new TestDiagnostics();

            await RunWithTimeout(async cancellationToken =>
            {
                IReadOnlyList<Task> publishTasks;
                var publishTaskDescriptors = new ConcurrentBag<PublishTask>();

                using (var publisherProxy = new AsyncPublisherProxy<bool>())
                using (var retryingPublisher = new AsyncPublisherWithRetries(
                    publisherProxy, TimeSpan.FromMilliseconds(retryDuration), diagnostics))
                {
                    using (StartAutoRecovery(connectionFactory, CreatePublisher, ImitateShutdownBound))
                    {
                        var counter = 0;
                        var publishSyncRoot = new object();

                        publishTasks = Enumerable
                            .Range(1, publishTaskCount)
                            .Select(_ => Task.Run(() =>
                            {
                                // Publish tasks are enqueued into publisher under lock
                                // to ensure that they start executing in predictable order,
                                // so that we can make test assertions regarding their retry order.
                                lock (publishSyncRoot)
                                {
                                    var messageIndex = Interlocked.Increment(ref counter);
                                    var messageTag = $"Message #{messageIndex}";
                                    var body = Encoding.UTF8.GetBytes(messageTag);
                                    var testBasicProperties = new TestMessageProperties {TestTag = messageTag};
                                    var task = retryingPublisher.PublishAsync(
                                        "test-exchange", "no-routing", body, testBasicProperties, default,
                                        cancellationToken);
                                    publishTaskDescriptors.Add(
                                        new PublishTask
                                            {Task = task, MessageIndex = messageIndex, MessageTag = messageTag});
                                    return task;
                                }
                            }, cancellationToken))
                            .ToList();

                        await Task.WhenAll(publishTasks);
                    }

                    IDisposable CreatePublisher(IModel model)
                    {
                        var innerPublisher =
                            new AsyncPublisher(model);
                        return publisherProxy.ConnectTo(innerPublisher);
                    }
                }

                var publishCalls = createdModels.SelectMany(x => x.PublishCalls).ToList();
                Console.WriteLine(
                    $" >> Finished publishing with {createdModels.Count} model(s) and {publishCalls.Count} total publish call(s)");
                Console.WriteLine("Publish tasks status: " + string.Join(",", publishTasks.Select(x => x.Status)));

                diagnostics.FailedRetryAttemptCount.ShouldBeGreaterThan(0,
                    "Test sanity check: at least 1 retry should actually happen");
                publishCalls.Count.ShouldBeGreaterThanOrEqualTo(publishTaskCount);
                AssertRetriesAreOrdered(publishTaskDescriptors);
                AssertAllPublishesAreAcked(createdModels.SelectMany(x => x.SuccessfullyCompletedPublishes).ToList(),
                    publishTaskDescriptors);

                IDisposable ImitateShutdownBound(IConnection connection)
                {
                    return ImitateConnectionShutdown(
                        (TestNonRecoverableConnection) connection,
                        random.Next(minShutdownDuration, maxShutdownDuration));
                }
            });
        }

        /// <summary>
        /// Starts a hierarchy of connection -> model -> publisher with auto-recovery and a concurrent shutdown loop,
        /// which periodically causes connection and models to be shut down,
        /// which in turn should stop publishers bound to models, failing currently active publish tasks.
        /// </summary>
        private static AutoRecovery<AutoRecoveryConnection> StartAutoRecovery(
            IConnectionFactory connectionFactory,
            Func<IModel, IDisposable> createPublisher,
            Func<IConnection, IDisposable> imitateShutdown)
        {
            return AutoRecovery.StartConnection(
                connectionFactory,
                _ => TimeSpan.FromMilliseconds(1),
                new AutoRecoveryConsoleDiagnostics("Connection"),
                connection => AutoRecovery.StartModel(
                    connection,
                    _ => TimeSpan.FromMilliseconds(1),
                    new AutoRecoveryConsoleDiagnostics($"Model/{Guid.NewGuid():D}"),
                    createPublisher),
                imitateShutdown);
        }

        private static IDisposable ImitateConnectionShutdown(TestNonRecoverableConnection connection,
            int shutdownDuration)
        {
            var isDisposed = false;

            Task.Run(async () =>
            {
                if (!isDisposed)
                {
                    await Task.Delay(shutdownDuration);
                    connection.ImitateClose(new ShutdownEventArgs(ShutdownInitiator.Application, 200, "Test shutdown"));
                }
            });

            return new Disposable(() => isDisposed = true);
        }

        /// <remarks>
        /// By the end of the test all messages should be successfully published (with or without retries).
        /// This method checks that test model received all necessary <see cref="TestRecoverableRabbitModel.BasicPublish"/> calls,
        /// was able to complete them and fire ACK messages via <see cref="TestRecoverableRabbitModel.BasicAcks"/>. 
        /// </remarks>
        private static void AssertAllPublishesAreAcked(
            IReadOnlyCollection<PublishRequest> acks,
            IEnumerable<PublishTask> publishTasks)
        {
            var unackedTags = new List<string>();

            // Console.WriteLine($" >> Acks: {string.Join(",", acks.Select(a => GetMessageTag(a.Properties)))}");

            foreach (var pt in publishTasks)
            {
                throw new NotImplementedException();
                // TODO: Pass message tag in another way
                // if (acks.Count(a => pt.MessageTag == GetMessageTag(a.Properties)) == 0)
                // {
                //     unackedTags.Add(pt.MessageTag);
                // }
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
            /// Unique message identifier which is also appended to message's <see cref="TestMessageProperties"/>.
            /// Can be used to correlate messages in assertions and under debug.
            /// </summary>
            public string MessageTag { get; set; }
        }
    }
}