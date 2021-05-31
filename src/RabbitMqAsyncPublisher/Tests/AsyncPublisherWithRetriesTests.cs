using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using RabbitMqAsyncPublisher;
using Shouldly;

namespace Tests
{
    using static TestUtils;

    [TestFixture]
    [Timeout(3000)]
    public class AsyncPublisherWithRetriesTests
    {
        [OneTimeSetUp]
        public void SetUp()
        {
            ThreadPool.SetMaxThreads(100, 10);
            ThreadPool.SetMinThreads(100, 10);
        }

        private static IAsyncPublisher<RetryPublishResult> CreateTarget(Func<string, Task<bool>> handlePublish)
        {
            return CreateTarget(new AsyncPublisherMock<bool>(handlePublish));
        }

        private static IAsyncPublisher<RetryPublishResult> CreateTarget(Func<Task<bool>> handlePublish)
        {
            return CreateTarget(new AsyncPublisherMock<bool>(handlePublish));
        }

        private static IAsyncPublisher<RetryPublishResult> CreateTarget(IAsyncPublisher<bool> decorated)
        {
            return new AsyncPublisherWithRetries(decorated, TimeSpan.FromMilliseconds(0));
        }

        [Test]
        public async Task ShouldPublishConcurrently()
        {
            var sources = new ConcurrentQueue<TaskCompletionSource<bool>>();
            using (var publisher = CreateTarget(() =>
            {
                var tcs = new TaskCompletionSource<bool>();
                sources.Enqueue(tcs);
                return tcs.Task;
            }))
            {
                var t1 = TestPublish(publisher);
                var t2 = TestPublish(publisher);
                var t3 = TestPublish(publisher);

                await SpinWaitFor(() => sources.Count == 3);

                t1.IsCompleted.ShouldBeFalse();
                t2.IsCompleted.ShouldBeFalse();
                t3.IsCompleted.ShouldBeFalse();

                await DequeueAndSetResultAsync(sources, true);
                (await t1).ShouldNotBeNull();
                t2.IsCompleted.ShouldBe(false);
                t3.IsCompleted.ShouldBe(false);

                await DequeueAndSetResultAsync(sources, true);
                (await t2).ShouldNotBeNull();
                t3.IsCompleted.ShouldBe(false);

                await DequeueAndSetResultAsync(sources, true);
                await Task.WhenAny(t3);
                (await t3).ShouldNotBeNull();
            }
        }

        [Test]
        public async Task ShouldRetry()
        {
            var sources = new ConcurrentQueue<TaskCompletionSource<bool>>();
            using (var publisher = CreateTarget(() =>
            {
                var tcs = new TaskCompletionSource<bool>();
                sources.Enqueue(tcs);
                return tcs.Task;
            }))
            {
                var t = TestPublish(publisher);

                await SpinWaitFor(() => sources.Count == 1);

                await DequeueAndSetExceptionAsync(sources,
                    new AlreadyClosedException(new ShutdownEventArgs(ShutdownInitiator.Application, default, default)));
                await SpinWaitFor(() => sources.Count == 1);
                t.IsCompleted.ShouldBeFalse();

                await DequeueAndSetResultAsync(sources, false);
                await SpinWaitFor(() => sources.Count == 1);
                t.IsCompleted.ShouldBeFalse();

                await DequeueAndSetResultAsync(sources, true);
                (await t).Retries.ShouldBe(2);
            }
        }

        [Test]
        public async Task ShouldSerializeFailedPublishRetries()
        {
            var sources = new ConcurrentQueue<TaskCompletionSource<bool>>();
            using (var publisher = CreateTarget(() =>
            {
                var tcs = new TaskCompletionSource<bool>();
                sources.Enqueue(tcs);
                return tcs.Task;
            }))
            {
                var t1 = TestPublish(publisher);
                var t2 = TestPublish(publisher);
                var t3 = TestPublish(publisher);

                await SpinWaitFor(() => sources.Count == 3);

                // t2 attempt#1 failed
                Task.Run(() => sources.ToArray()[1].SetResult(false));
                await Task.Delay(100);
                sources.Count.ShouldBe(3);

                // t3 attempt#1 failed
                Task.Run(() => sources.ToArray()[2].SetResult(false));
                await Task.Delay(100);
                sources.Count.ShouldBe(3);

                // t1 attempt#1 failed
                Task.Run(() => sources.ToArray()[0].SetResult(false));
                await Task.Delay(100);

                // t1 attempt#2 started
                sources.Count.ShouldBe(4);

                // t1 attempt#2 failed
                Task.Run(() => sources.ToArray()[3].SetResult(false));
                await Task.Delay(100);

                // t1 attempt#3 started
                sources.Count.ShouldBe(5);

                // t1 attempt#3 succeeded
                Task.Run(() => sources.ToArray()[4].SetResult(true));
                (await t1).Retries.ShouldBe(2);
                t2.IsCompleted.ShouldBeFalse();
                t3.IsCompleted.ShouldBeFalse();
                await Task.Delay(100);

                // t2 attempt#2 started
                sources.Count.ShouldBe(6);

                // t2 attempt#2 succeeded 
                Task.Run(() => sources.ToArray()[5].SetResult(true));
                (await t2).Retries.ShouldBe(1);
                t3.IsCompleted.ShouldBeFalse();

                // t3 attempt#2 started
                await Task.Delay(100);
                sources.Count.ShouldBe(7);

                // t4 scheduled
                var t4 = TestPublish(publisher);
                await Task.Delay(100);

                // t4 not started yet
                sources.Count.ShouldBe(7);

                // t3 attempt#2 succeeded
                Task.Run(() => sources.ToArray()[6].SetResult(true));
                (await t3).Retries.ShouldBe(1);
                t4.IsCompleted.ShouldBeFalse();

                // t4 attempt#1 started
                await Task.Delay(100);
                sources.Count.ShouldBe(8);

                // t4 attempt#1 succeeded
                Task.Run(() => sources.ToArray()[7].SetResult(true));
                (await t4).Retries.ShouldBe(0);

                await Task.Delay(100);
                sources.Count.ShouldBe(8);
            }
        }

        [Test]
        public async Task ShouldPublishConcurrentlyWhenRetriesCompleted()
        {
            var sources = new ConcurrentQueue<TaskCompletionSource<bool>>();
            using (var publisher = CreateTarget(() =>
            {
                var tcs = new TaskCompletionSource<bool>();
                sources.Enqueue(tcs);
                return tcs.Task;
            }))
            {
                var retryingTask = TestPublish(publisher);
                await SpinWaitFor(() => sources.Count == 1);

                await DequeueAndSetResultAsync(sources, false);

                await SpinWaitFor(() => sources.Count == 1);

                var t1 = TestPublish(publisher);
                var t2 = TestPublish(publisher);
                var t3 = TestPublish(publisher);

                await Task.Delay(100);
                sources.Count.ShouldBe(1);

                await DequeueAndSetResultAsync(sources, true);
                await retryingTask;

                await SpinWaitFor(() => sources.Count == 3);

                await DequeueAndSetResultAsync(sources, true);
                await DequeueAndSetResultAsync(sources, true);
                await DequeueAndSetResultAsync(sources, true);

                (await t1).Retries.ShouldBe(0);
                (await t2).Retries.ShouldBe(0);
                (await t3).Retries.ShouldBe(0);
            }
        }

        [Test]
        public async Task ShouldCancel()
        {
            var sources = new ConcurrentQueue<TaskCompletionSource<bool>>();
            using (var publisher = CreateTarget(() =>
            {
                var tcs = new TaskCompletionSource<bool>();
                sources.Enqueue(tcs);
                return tcs.Task;
            }))
            {
                var retryingTaskCts = new CancellationTokenSource();
                var retryingTask = TestPublish(publisher, cancellationToken: retryingTaskCts.Token);

                await SpinWaitFor(() => sources.Count == 1);

                await DequeueAndSetResultAsync(sources, false);
                await SpinWaitFor(() => sources.Count == 1);

                var waitingTaskCts = new CancellationTokenSource();
                var waitingTask = TestPublish(publisher, cancellationToken: waitingTaskCts.Token);
                waitingTaskCts.CancelAfter(100);

                await Task.WhenAny(waitingTask);
                waitingTask.IsCanceled.ShouldBeTrue();

                retryingTaskCts.Cancel();
                await DequeueAndSetResultAsync(sources, false);

                await Task.WhenAny(retryingTask);
                retryingTask.IsCanceled.ShouldBeTrue();

                var immediatelyCancelledTask = TestPublish(publisher, cancellationToken: new CancellationToken(true));
                immediatelyCancelledTask.IsCanceled.ShouldBeTrue();
            }
        }

        [Test]
        public async Task ShouldDispose()
        {
            var sources = new ConcurrentQueue<TaskCompletionSource<bool>>();
            using (var publisher = CreateTarget(() =>
            {
                var tcs = new TaskCompletionSource<bool>();
                sources.Enqueue(tcs);
                return tcs.Task;
            }))
            {
                var retryingTask = TestPublish(publisher);

                await SpinWaitFor(() => sources.Count == 1);

                await DequeueAndSetResultAsync(sources, false);
                await SpinWaitFor(() => sources.Count == 1);

                var waitingTask = TestPublish(publisher);

                await Task.Delay(100);

                publisher.Dispose();

                await Task.WhenAny(waitingTask);
                waitingTask.IsFaulted.ShouldBeTrue();
                waitingTask.Exception.InnerException.ShouldBeOfType<ObjectDisposedException>();

                await DequeueAndSetResultAsync(sources, false);

                await Task.WhenAny(retryingTask);
                retryingTask.IsFaulted.ShouldBeTrue();
                retryingTask.Exception.InnerException.ShouldBeOfType<ObjectDisposedException>();

                Assert.Throws<ObjectDisposedException>(() => TestPublish(publisher));
            }
        }

        [Test]
        [TestCase(10000, 12345)]
        [TestCase(10000, 54321)]
        public async Task RandomTest(int messageCount, int seed)
        {
            var random = new Random(seed);
            var messages = new ConcurrentQueue<(string exchange, ReadOnlyMemory<byte> body, int retries)>();
            var handlers = new Dictionary<string, ConcurrentQueue<(int iterationsToSpin, bool result)>>();
            for (var i = 0; i < messageCount; i++)
            {
                var exchange = $"exchange#{i}";
                var retries = Math.Max(0, random.Next(10) - 7);

                messages.Enqueue((
                    exchange,
                    body: new ReadOnlyMemory<byte>(new byte[random.Next(1000)]),
                    retries
                ));

                var queue = new ConcurrentQueue<(int iterationsToSpin, bool result)>();
                for (var j = 0; j < retries; j++)
                {
                    queue.Enqueue((iterationsToSpin: random.Next(100), result: false));
                }

                queue.Enqueue((iterationsToSpin: random.Next(100), result: true));

                handlers[exchange] = queue;
            }

            using (var publisher = CreateTarget(exchange =>
            {
                if (!handlers[exchange].TryDequeue(out var item))
                {
                    throw new InvalidOperationException();
                }

                var tcs = new TaskCompletionSource<bool>();
                Task.Run(() =>
                {
                    Thread.SpinWait(item.iterationsToSpin);
                    tcs.SetResult(item.result);
                });
                return tcs.Task;
            }))
            {
                var tasks = new List<Task<RetryPublishResult>>();
                var expectedResults = new List<RetryPublishResult>();

                while (messages.TryDequeue(out var item))
                {
                    tasks.Add(publisher.PublishAsync(item.exchange, string.Empty, item.body, default));
                    expectedResults.Add(new RetryPublishResult(true, item.retries));
                }

                var actualResults = await Task.WhenAll(tasks);

                for (var i = 0; i < actualResults.Length; i++)
                {
                    if (actualResults[i].Retries != expectedResults[i].Retries)
                    {
                        Console.WriteLine(i);
                    }

                    actualResults[i].Retries.ShouldBe(expectedResults[i].Retries);
                }
            }
        }

        [Test]
        [Explicit]
        [Timeout(10000)]
        public async Task Perf()
        {
            using (var publisher = CreateTarget(() =>
            {
                var tcs = new TaskCompletionSource<bool>();
                Task.Run(() =>
                {
                    Thread.SpinWait(10);
                    tcs.SetResult(true);
                });
                return tcs.Task;
            }))
            {
                for (var i = 0; i < 10000; i++)
                {
                    var tasks = new List<Task<RetryPublishResult>>();

                    for (var j = 0; j < 100; j++)
                    {
                        tasks.Add(TestPublish(publisher, new ReadOnlyMemory<byte>(new byte[100])));
                    }

                    await Task.WhenAll(tasks);
                }
            }
        }
    }
}