using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using RabbitMqAsyncPublisher;
using Shouldly;
using static Tests.TestUtils;

namespace Tests
{
    [TestFixture]
    [Timeout(3000)]
    public class AsyncPublisherWithBufferTestsBase
    {
        [OneTimeSetUp]
        public void SetUp()
        {
            ThreadPool.SetMaxThreads(100, 10);
            ThreadPool.SetMinThreads(100, 10);
        }

        private static IAsyncPublisher<TResult> CreateTarget<TResult>(IAsyncPublisher<TResult> decorated,
            int processingMessagesLimit = int.MaxValue,
            int processingBytesLimit = int.MaxValue)
        {
            return new AsyncPublisherWithBuffer<TResult>(decorated, processingMessagesLimit, processingBytesLimit);
        }

        [Test]
        public async Task ShouldReturnOriginalResults()
        {
            var sources = new ConcurrentQueue<TaskCompletionSource<bool>>();
            var publisherMock = new AsyncPublisherMock<bool>(() =>
            {
                var tcs = new TaskCompletionSource<bool>();
                sources.Enqueue(tcs);
                return tcs.Task;
            });
            var publisher = CreateTarget(publisherMock);

            var tTrue = TestPublish(publisher);
            var tFalse = TestPublish(publisher);
            var tThrow = TestPublish(publisher);
            var tCancel = TestPublish(publisher);

            await SpinWaitFor(() => sources.Count == 4);

            await DequeueAndSetResultAsync(sources, true);
            await DequeueAndSetResultAsync(sources, false);
            await DequeueAndSetExceptionAsync(sources, new Exception("Decorated publisher exception"));
            await DequeueAndSetCanceledAsync(sources);

            tTrue.Result.ShouldBeTrue();
            tFalse.Result.ShouldBeFalse();
            tThrow.Exception.InnerException.Message.ShouldBe("Decorated publisher exception");
            tCancel.IsCanceled.ShouldBeTrue();
        }

        [Test]
        public async Task ShouldPublishConcurrently()
        {
            var sources = new ConcurrentQueue<TaskCompletionSource<bool>>();
            using (var publisherMock = new AsyncPublisherMock<bool>(() =>
            {
                var tcs = new TaskCompletionSource<bool>();
                sources.Enqueue(tcs);
                return tcs.Task;
            }))
            using (var publisher = CreateTarget(publisherMock))
            {
                var t1 = TestPublish(publisher, new ReadOnlyMemory<byte>(new byte[1000]));
                var t2 = TestPublish(publisher, new ReadOnlyMemory<byte>(new byte[1000]));
                var t3 = TestPublish(publisher, new ReadOnlyMemory<byte>(new byte[1000]));

                await SpinWaitFor(() => sources.Count == 3);

                t1.IsCompleted.ShouldBeFalse();
                t2.IsCompleted.ShouldBeFalse();
                t3.IsCompleted.ShouldBeFalse();

                await DequeueAndSetResultAsync(sources, true);
                await Task.WhenAny(t1);
                t1.IsCompleted.ShouldBe(true);
                t1.Result.ShouldBeTrue();
                t2.IsCompleted.ShouldBe(false);
                t3.IsCompleted.ShouldBe(false);

                await DequeueAndSetResultAsync(sources, false);
                await Task.WhenAny(t2);
                t2.IsCompleted.ShouldBe(true);
                t2.Result.ShouldBe(false);
                t3.IsCompleted.ShouldBe(false);

                await DequeueAndSetResultAsync(sources, true);
                await Task.WhenAny(t3);
                t3.IsCompleted.ShouldBe(true);
                t3.Result.ShouldBe(true);
            }
        }

        [Test]
        public async Task ShouldRespectLimits()
        {
            var sources = new ConcurrentQueue<TaskCompletionSource<bool>>();
            var publisherMock = new AsyncPublisherMock<bool>(() =>
            {
                var tcs = new TaskCompletionSource<bool>();
                sources.Enqueue(tcs);
                return tcs.Task;
            });
            var publisher = CreateTarget(publisherMock, 3, 2000);

            var t100 = TestPublish(publisher, new ReadOnlyMemory<byte>(new byte[100]));
            var t200 = TestPublish(publisher, new ReadOnlyMemory<byte>(new byte[200]));
            var t300 = TestPublish(publisher, new ReadOnlyMemory<byte>(new byte[300]));
            var t4000 = TestPublish(publisher, new ReadOnlyMemory<byte>(new byte[4000]));
            var t500 = TestPublish(publisher, new ReadOnlyMemory<byte>(new byte[500]));
            var t600 = TestPublish(publisher, new ReadOnlyMemory<byte>(new byte[600]));

            // Processing: 100, 200, 300 (count=3; bytes=600)
            // Waiting: 4000, 500, 600
            // Reached Limits: messages limit
            await SpinWaitFor(() => sources.Count == 3);

            // Processing: 100
            await DequeueAndSetResultAsync(sources, true);
            t100.Result.ShouldBeTrue();

            // Processing: 200, 300, 4000 (count=3; bytes=4500)
            // Waiting: 500, 600
            // Reached Limits: messages limit, soft bytes limit
            await SpinWaitFor(() => sources.Count == 3);

            // Processing: 200
            await DequeueAndSetResultAsync(sources, true);
            t200.Result.ShouldBeTrue();

            // Processing: 300, 4000 (count=2; bytes=4300)
            // Waiting: 500, 600
            // Reached Limits: soft bytes limit
            await SpinWaitFor(() => sources.Count == 2);

            // Processing: 300
            await DequeueAndSetResultAsync(sources, true);
            t300.Result.ShouldBeTrue();

            // Processing: 4000 (count=1; bytes=4000)
            // Waiting: 500, 600
            // Reached Limits: soft bytes limit
            await SpinWaitFor(() => sources.Count == 1);

            // Processing: 4000
            await DequeueAndSetResultAsync(sources, true);
            t4000.Result.ShouldBeTrue();

            // Processing: 500, 600 (count=2; bytes=1100)
            // Waiting: -
            // Reached Limits: -
            await SpinWaitFor(() => sources.Count == 2);

            // Processing: 500, 600
            await DequeueAndSetResultAsync(sources, true);
            await DequeueAndSetResultAsync(sources, true);
            t500.Result.ShouldBeTrue();
            t600.Result.ShouldBeTrue();

            // Processing: - (count=0; bytes=0)
            // Waiting: -
            // Reached Limits: -
            sources.Count.ShouldBe(0);
        }

        [Test]
        [Repeat(100)]
        public async Task ShouldCancel()
        {
            await RunWithTimeout(async rootCancellationToken =>
            {
                var sources = new ConcurrentQueue<TaskCompletionSource<bool>>();
                var publisherMock = new AsyncPublisherMock<bool>(() =>
                {
                    var tcs = new TaskCompletionSource<bool>();
                    sources.Enqueue(tcs);
                    return tcs.Task;
                });
                var publisher = CreateTarget(publisherMock, 1);

                var innerPublisherCancelled = TestPublish(publisher);

                await SpinWaitFor(() => sources.Count == 1);

                var cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(rootCancellationToken);
                var eventuallyCancelled = TestPublish(publisher, cancellationToken: cancellationTokenSource.Token);

                eventuallyCancelled.IsCompleted.ShouldBeFalse();

                cancellationTokenSource.Cancel();

                await Task.WhenAny(eventuallyCancelled);
                eventuallyCancelled.IsCompleted.ShouldBeTrue();
                eventuallyCancelled.IsCanceled.ShouldBeTrue();

                var immediatelyCancelled = TestPublish(publisher, cancellationToken: cancellationTokenSource.Token);

                immediatelyCancelled.IsCompleted.ShouldBeTrue();
                immediatelyCancelled.IsCanceled.ShouldBeTrue();

                await DequeueAndSetCanceledAsync(sources);

                await Task.WhenAny(innerPublisherCancelled);
                innerPublisherCancelled.IsCompleted.ShouldBeTrue();
                innerPublisherCancelled.IsCanceled.ShouldBeTrue();
            });
        }

        [Test]
        [Repeat(100)]
        public async Task ShouldDispose()
        {
            var sources = new ConcurrentQueue<TaskCompletionSource<bool>>();
            var publisherMock = new AsyncPublisherMock<bool>(() =>
            {
                var tcs = new TaskCompletionSource<bool>();
                sources.Enqueue(tcs);
                return tcs.Task;
            });
            var publisher = CreateTarget(publisherMock, 1);

            var eventuallyProcessed = TestPublish(publisher, new ReadOnlyMemory<byte>(new byte[1]));
            await SpinWaitFor(() => sources.Count == 1);

            var eventuallyDisposed = TestPublish(publisher, new ReadOnlyMemory<byte>(new byte[2]));
            eventuallyDisposed.IsCompleted.ShouldBeFalse();

            publisher.Dispose();

            await Task.WhenAny(eventuallyDisposed);
            eventuallyDisposed.IsFaulted.ShouldBeTrue();
            eventuallyDisposed.Exception.InnerException.ShouldBeOfType<ObjectDisposedException>();

            Assert.Throws<ObjectDisposedException>(() => TestPublish(publisher, new ReadOnlyMemory<byte>(new byte[3])));

            await DequeueAndSetResultAsync(sources, true);

            eventuallyProcessed.IsFaulted.ShouldBeFalse();
            eventuallyProcessed.Result.ShouldBeTrue();
        }

        [Test]
        [TestCase(10000, 12345)]
        [TestCase(10000, 54321)]
        public async Task RandomTest(int messageCount, int seed)
        {
            var random = new Random(seed);
            var messages = new ConcurrentQueue<(ReadOnlyMemory<byte> body, int result)>();
            var handlers = new ConcurrentQueue<(int iterationsToSpin, int result)>();
            for (var i = 0; i < messageCount; i++)
            {
                messages.Enqueue((
                    body: new ReadOnlyMemory<byte>(new byte[random.Next(1000)]),
                    result: i
                ));
                handlers.Enqueue((
                    iterationsToSpin: random.Next(100),
                    result: i
                ));
            }

            using (var publisherMock = new AsyncPublisherMock<int>(exchange =>
            {
                var tcs = new TaskCompletionSource<int>();
                handlers.TryDequeue(out var item);
                Task.Run(() =>
                {
                    Thread.SpinWait(item.iterationsToSpin);
                    tcs.SetResult(item.result);
                });
                return tcs.Task;
            }))
            using (var publisher = CreateTarget(publisherMock, 10, 1000))
            {
                var tasks = new List<Task<int>>();
                var expectedResults = new List<int>();

                while (messages.TryDequeue(out var item))
                {
                    tasks.Add(publisher.PublishAsync(item.result.ToString(), string.Empty, item.body, default,
                        default));
                    expectedResults.Add(item.result);
                }

                var actualResults = await Task.WhenAll(tasks);

                for (var i = 0; i < actualResults.Length; i++)
                {
                    if (actualResults[i] != expectedResults[i])
                    {
                        Console.WriteLine(i);
                    }

                    actualResults[i].ShouldBe(expectedResults[i]);
                }
            }
        }

        [Test]
        [Explicit]
        [Timeout(10000)]
        public async Task Perf()
        {
            var publisherMock = new AsyncPublisherMock<bool>(() =>
            {
                var tcs = new TaskCompletionSource<bool>();
                Task.Run(() =>
                {
                    Thread.SpinWait(10);
                    tcs.SetResult(true);
                });
                return tcs.Task;
            });
            var publisher = CreateTarget(publisherMock, 10);


            for (var i = 0; i < 10000; i++)
            {
                var tasks = new List<Task<bool>>();

                for (var j = 0; j < 100; j++)
                {
                    tasks.Add(TestPublish(publisher, new ReadOnlyMemory<byte>(new byte[100])));
                }

                await Task.WhenAll(tasks);
            }
        }

        private static Task<T> TestPublish<T>(IAsyncPublisher<T> publisher,
            ReadOnlyMemory<byte> body = default, CancellationToken cancellationToken = default)
        {
            return publisher.PublishAsync(string.Empty, string.Empty, body, default, default, cancellationToken);
        }

        private static Task DequeueAndSetResultAsync<T>(ConcurrentQueue<TaskCompletionSource<T>> queue, T result)
        {
            return Task.Run(() =>
            {
                queue.TryDequeue(out var item);
                item.SetResult(result);
            });
        }

        private static Task DequeueAndSetExceptionAsync<T>(ConcurrentQueue<TaskCompletionSource<T>> queue, Exception ex)
        {
            return Task.Run(() =>
            {
                queue.TryDequeue(out var item);
                item.SetException(ex);
            });
        }

        private static Task DequeueAndSetCanceledAsync<T>(ConcurrentQueue<TaskCompletionSource<T>> queue)
        {
            return Task.Run(() =>
            {
                queue.TryDequeue(out var item);
                item.SetCanceled();
            });
        }
    }

    public class AsyncPublisherMock<TResult> : IAsyncPublisher<TResult>
    {
        private readonly Func<string, Task<TResult>> _publishCallback;
        private bool _isDisposed;

        public AsyncPublisherMock(Func<string, Task<TResult>> publishCallback)
        {
            _publishCallback = publishCallback;
        }

        public AsyncPublisherMock(Func<Task<TResult>> publishCallback)
        {
            _publishCallback = _ => publishCallback();
        }

        public Task<TResult> PublishAsync(string exchange, string routingKey, ReadOnlyMemory<byte> body,
            MessageProperties properties,
            string correlationId = null,
            CancellationToken cancellationToken = default)
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException(nameof(AsyncPublisherMock<TResult>));
            }

            return _publishCallback(exchange);
        }

        public void Dispose()
        {
            _isDisposed = true;
        }
    }
}