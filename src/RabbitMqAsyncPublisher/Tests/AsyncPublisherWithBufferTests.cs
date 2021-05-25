﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using RabbitMQ.Client;
using RabbitMqAsyncPublisher;
using Shouldly;

namespace Tests
{
    [TestFixture]
    public class AsyncPublisherWithBufferTests
    {
        [OneTimeSetUp]
        public void SetUp()
        {
            ThreadPool.SetMaxThreads(100, 10);
            ThreadPool.SetMinThreads(100, 10);
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
            var publisher = new AsyncPublisherWithBuffer<bool>(publisherMock);

            var tTrue = TestPublish(publisher);
            var tFalse = TestPublish(publisher);
            var tThrow = TestPublish(publisher);
            var tCancel = TestPublish(publisher);

            await WaitFor(() => sources.Count == 4);

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
            var publisherMock = new AsyncPublisherMock<bool>(() =>
            {
                var tcs = new TaskCompletionSource<bool>();
                sources.Enqueue(tcs);
                return tcs.Task;
            });
            var publisher = new AsyncPublisherWithBuffer<bool>(publisherMock);

            var t1 = TestPublish(publisher, new ReadOnlyMemory<byte>(new byte[1000]));
            var t2 = TestPublish(publisher, new ReadOnlyMemory<byte>(new byte[1000]));
            var t3 = TestPublish(publisher, new ReadOnlyMemory<byte>(new byte[1000]));

            await WaitFor(() => sources.Count == 3);

            t1.IsCompleted.ShouldBeFalse();
            t2.IsCompleted.ShouldBeFalse();
            t3.IsCompleted.ShouldBeFalse();

            await DequeueAndSetResultAsync(sources, true);

            t1.Result.ShouldBeTrue();
            t1.Result.ShouldBeTrue();
            t1.Result.ShouldBeTrue();
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
            var publisher = new AsyncPublisherWithBuffer<bool>(publisherMock, 3, 2000);

            var t100 = TestPublish(publisher, new ReadOnlyMemory<byte>(new byte[100]));
            var t200 = TestPublish(publisher, new ReadOnlyMemory<byte>(new byte[200]));
            var t300 = TestPublish(publisher, new ReadOnlyMemory<byte>(new byte[300]));
            var t4000 = TestPublish(publisher, new ReadOnlyMemory<byte>(new byte[4000]));
            var t500 = TestPublish(publisher, new ReadOnlyMemory<byte>(new byte[500]));
            var t600 = TestPublish(publisher, new ReadOnlyMemory<byte>(new byte[600]));

            // Processing: 100, 200, 300 (count=3; bytes=600)
            // Waiting: 4000, 500, 600
            // Reached Limits: messages limit
            await WaitFor(() => sources.Count == 3);

            // Processing: 100
            await DequeueAndSetResultAsync(sources, true);
            t100.Result.ShouldBeTrue();

            // Processing: 200, 300, 4000 (count=3; bytes=4500)
            // Waiting: 500, 600
            // Reached Limits: messages limit, soft bytes limit
            await WaitFor(() => sources.Count == 3);

            // Processing: 200
            await DequeueAndSetResultAsync(sources, true);
            t200.Result.ShouldBeTrue();

            // Processing: 300, 4000 (count=2; bytes=4300)
            // Waiting: 500, 600
            // Reached Limits: soft bytes limit
            await WaitFor(() => sources.Count == 2);

            // Processing: 300
            await DequeueAndSetResultAsync(sources, true);
            t300.Result.ShouldBeTrue();

            // Processing: 4000 (count=1; bytes=4000)
            // Waiting: 500, 600
            // Reached Limits: soft bytes limit
            await WaitFor(() => sources.Count == 1);

            // Processing: 4000
            await DequeueAndSetResultAsync(sources, true);
            t4000.Result.ShouldBeTrue();

            // Processing: 500, 600 (count=2; bytes=1100)
            // Waiting: -
            // Reached Limits: -
            await WaitFor(() => sources.Count == 2);

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
        public async Task ShouldCancel()
        {
            var sources = new ConcurrentQueue<TaskCompletionSource<bool>>();
            var publisherMock = new AsyncPublisherMock<bool>(() =>
            {
                var tcs = new TaskCompletionSource<bool>();
                sources.Enqueue(tcs);
                return tcs.Task;
            });
            var publisher = new AsyncPublisherWithBuffer<bool>(publisherMock, 1);

            var innerPublisherCancelled = TestPublish(publisher);

            var cancellationTokenSource = new CancellationTokenSource();
            var eventuallyCancelled = TestPublish(publisher, cancellationToken: cancellationTokenSource.Token);

            eventuallyCancelled.IsCompleted.ShouldBeFalse();

            cancellationTokenSource.Cancel();

            await Task.WhenAny(eventuallyCancelled);
            eventuallyCancelled.IsCompleted.ShouldBeTrue();
            eventuallyCancelled.IsCanceled.ShouldBeTrue();

            var immediatelyCancelled = publisher.PublishAsync(default, default, ReadOnlyMemory<byte>.Empty, default,
                cancellationTokenSource.Token);

            immediatelyCancelled.IsCompleted.ShouldBeTrue();
            immediatelyCancelled.IsCanceled.ShouldBeTrue();

            await WaitFor(() => sources.Count == 1);

            await DequeueAndSetCanceledAsync(sources);

            await Task.WhenAny(innerPublisherCancelled);
            innerPublisherCancelled.IsCompleted.ShouldBeTrue();
            innerPublisherCancelled.IsCanceled.ShouldBeTrue();
        }


        [Test]
        public async Task ShouldDispose()
        {
            var sources = new ConcurrentQueue<TaskCompletionSource<bool>>();
            var publisherMock = new AsyncPublisherMock<bool>(() =>
            {
                var tcs = new TaskCompletionSource<bool>();
                sources.Enqueue(tcs);
                return tcs.Task;
            });
            var publisher = new AsyncPublisherWithBuffer<bool>(publisherMock, 1);

            var eventuallyProcessed = TestPublish(publisher);

            var eventuallyDisposed = TestPublish(publisher);
            eventuallyDisposed.IsFaulted.ShouldBeFalse();

            publisher.Dispose();

            await Task.WhenAny(eventuallyDisposed);
            eventuallyDisposed.IsFaulted.ShouldBeTrue();
            eventuallyDisposed.Exception.InnerException.ShouldBeOfType<ObjectDisposedException>();

            var immediatelyDisposed = TestPublish(publisher);
            immediatelyDisposed.IsFaulted.ShouldBeTrue();
            immediatelyDisposed.Exception.InnerException.ShouldBeOfType<ObjectDisposedException>();

            await WaitFor(() => sources.Count == 1);
            await DequeueAndSetResultAsync(sources, true);

            eventuallyProcessed.Result.ShouldBeTrue();
        }

        [Ignore(
            "Publishing order is broken. SemaphoreSlim should be replaced with another sync primitive that support FIFO semantics.")]
        [Test]
        [TestCase(100, 123)]
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

            var publisherMock = new AsyncPublisherMock<int>(exchange =>
            {
                Console.WriteLine($" << {exchange}");
                var tcs = new TaskCompletionSource<int>();
                handlers.TryDequeue(out var item);
                Task.Run(() =>
                {
                    Thread.SpinWait(item.iterationsToSpin);
                    tcs.SetResult(item.result);
                });
                return tcs.Task;
            });
            var publisher = new AsyncPublisherWithBuffer<int>(publisherMock, 10, 1000);

            var tasks = new List<Task<int>>();
            var expectedResults = new List<int>();

            while (messages.TryDequeue(out var item))
            {
                Console.WriteLine($" {item.result.ToString()} ({item.body.Length}) >>");
                tasks.Add(publisher.PublishAsync(item.result.ToString(), string.Empty, item.body, default, default));
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

        [Test]
        [Explicit]
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
            var publisher = new AsyncPublisherWithBuffer<bool>(publisherMock, 10);

            var tasks = new List<Task<bool>>();

            for (var i = 0; i < 1000; i++)
            {
                for (var j = 0; j < 1000; j++)
                {
                    tasks.Add(TestPublish(publisher, new ReadOnlyMemory<byte>(new byte[100])));
                    // tasks.Add(TestPublish(publisherMock, new ReadOnlyMemory<byte>(new byte[100])));
                }

                Thread.SpinWait(10);
            }

            await Task.WhenAll(tasks);
        }

        private static Task<T> TestPublish<T>(IAsyncPublisher<T> publisher,
            ReadOnlyMemory<byte> body = default, CancellationToken cancellationToken = default)
        {
            return publisher.PublishAsync(string.Empty, string.Empty, body, default, cancellationToken);
        }

        private static async Task WaitFor(Func<bool> condition)
        {
            using (var cancellationTokenSource = new CancellationTokenSource(1000))
            {
                var cancellationToke = cancellationTokenSource.Token;
                var winner = await Task.WhenAny(
                    Task.Delay(-1, cancellationToke),
                    Task.Run(() =>
                    {
                        var spinWait = new SpinWait();
                        while (!condition())
                        {
                            cancellationToke.ThrowIfCancellationRequested();
                            spinWait.SpinOnce();
                        }
                    }, cancellationToke)
                );
                await winner;
            }
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

        [Test]
        public void Foo()
        {
            var sources = new Queue<TaskCompletionSource<bool>>();
            var publisherMock = new AsyncPublisherMock<bool>(() =>
            {
                var tcs = new TaskCompletionSource<bool>();
                sources.Enqueue(tcs);
                return tcs.Task;
            });
            var publisher = new AsyncPublisherWithBuffer<bool>(publisherMock, 1);

            var t1 = publisher.PublishAsync(default, default, default, default, default);
            sources.Dequeue().SetResult(true);
            t1.Result.ShouldBeTrue();

            var t2 = publisher.PublishAsync(default, default, default, default, default);
            var t3 = publisher.PublishAsync(default, default, default, default, default);

            publisher.Dispose();

            t3.ContinueWith(_ => { }).Wait();
            t3.IsFaulted.ShouldBeTrue();
            t3.Exception.InnerException.ShouldBeOfType<ObjectDisposedException>();

            var t4 = publisher.PublishAsync(default, default, default, default, default);
            t4.ContinueWith(_ => { }).Wait();
            t4.IsFaulted.ShouldBeTrue();
            t4.Exception.InnerException.ShouldBeOfType<ObjectDisposedException>();

            sources.Dequeue().SetResult(true);
            t2.Result.ShouldBeTrue();
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
            IBasicProperties properties,
            CancellationToken cancellationToken)
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