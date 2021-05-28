using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMqAsyncPublisher;
using Shouldly;
using static Tests.TestUtils;

namespace Tests
{
    [TestFixture]
    [Timeout(3000)]
    public class AsyncPublisherTests
    {
        [OneTimeSetUp]
        public void SetUp()
        {
            ThreadPool.SetMaxThreads(100, 10);
            ThreadPool.SetMinThreads(100, 10);
        }

        private static AsyncPublisher CreateTarget(Func<PublishRequest, Task<bool>> handlePublish)
        {
            return CreateTarget(new TestNonRecoverableRabbitModel(handlePublish));
        }

        private static AsyncPublisher CreateTarget(IModel model)
        {
            return new AsyncPublisher(model);
        }

        [Test]
        public async Task ShouldHandleBasicAcksAndBasicNacks()
        {
            var sources = new ConcurrentQueue<TaskCompletionSource<bool>>();
            var model = new TestNonRecoverableRabbitModel(req =>
            {
                var tcs = new TaskCompletionSource<bool>();
                sources.Enqueue(tcs);
                return tcs.Task;
            });
            using (var publisher = CreateTarget(model))
            {
                var tasks = new[]
                {
                    TestPublish(publisher),
                    TestPublish(publisher),
                    TestPublish(publisher),
                    TestPublish(publisher),
                    TestPublish(publisher),
                    TestPublish(publisher)
                };

                await SpinWaitFor(() => sources.Count == tasks.Length);

                var sourcesArray = sources.ToArray();

                Task.Run(() => model.FireBasicAcks(new BasicAckEventArgs {DeliveryTag = 2, Multiple = false}));
                (await tasks[1]).ShouldBeTrue();
                tasks.Count(x => !x.IsCompleted).ShouldBe(5);

                Task.Run(() => model.FireBasicNacks(new BasicNackEventArgs {DeliveryTag = 5, Multiple = false}));
                (await tasks[4]).ShouldBeFalse();
                tasks.Count(x => !x.IsCompleted).ShouldBe(4);

                Task.Run(() => model.FireBasicAcks(new BasicAckEventArgs {DeliveryTag = 3, Multiple = true}));
                (await tasks[0]).ShouldBeTrue();
                (await tasks[2]).ShouldBeTrue();
                tasks.Count(x => !x.IsCompleted).ShouldBe(2);

                Task.Run(() => model.FireBasicNacks(new BasicNackEventArgs {DeliveryTag = 6, Multiple = true}));
                (await tasks[3]).ShouldBeFalse();
                (await tasks[5]).ShouldBeFalse();
                tasks.Count(x => !x.IsCompleted).ShouldBe(0);
            }
        }

        [Test]
        public async Task ShouldReturnFailedTaskWhenBasicPublishThrows()
        {
            using (var publisher = CreateTarget(req => throw new Exception("Unable to publish")))
            {
                try
                {
                    await TestPublish(publisher);
                    Assert.Fail();
                }
                catch (Exception ex)
                {
                    ex.Message.ShouldBe("Unable to publish");
                }
            }
        }

        [Test]
        public async Task ShouldReturnResultAsSoonAsAckIsReceived()
        {
            var sources = new ConcurrentQueue<TaskCompletionSource<bool>>();
            using (var publisher = CreateTarget(req =>
            {
                var tcs = new TaskCompletionSource<bool>();
                sources.Enqueue(tcs);
                return tcs.Task;
            }))
            {
                var t1 = TestPublish(publisher);
                var t2 = TestPublish(publisher);

                await SpinWaitFor(() => sources.Count == 2);

                sources.TryDequeue(out var source1);
                sources.TryDequeue(out var source2);

                Task.Run(() => source2.TrySetResult(true));

                (await t2).ShouldBeTrue();
                t1.IsCompleted.ShouldBeFalse();

                Task.Run(() => source1.TrySetResult(true));

                (await t1).ShouldBeTrue();
            }
        }

        [Test]
        public async Task ShouldPublishConcurrently()
        {
            var sources = new ConcurrentQueue<TaskCompletionSource<bool>>();
            using (var publisher = CreateTarget(req =>
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
                await Task.WhenAny(t1);
                t1.Result.ShouldBeTrue();
                t2.IsCompleted.ShouldBe(false);
                t3.IsCompleted.ShouldBe(false);

                await DequeueAndSetResultAsync(sources, false);
                await Task.WhenAny(t2);
                t2.Result.ShouldBe(false);
                t3.IsCompleted.ShouldBe(false);

                await DequeueAndSetResultAsync(sources, true);
                await Task.WhenAny(t3);
                t3.IsCompleted.ShouldBe(true);
                t3.Result.ShouldBe(true);
            }
        }

        [Test]
        public async Task ShouldSerializeBasicPublishCalls()
        {
            var events = new ConcurrentQueue<ManualResetEventSlim>();
            using (var publisher = CreateTarget(req =>
            {
                var mre = new ManualResetEventSlim(false);
                events.Enqueue(mre);
                mre.Wait();
                return Task.FromResult(true);
            }))
            {
                var tasks = new[]
                {
                    TestPublish(publisher),
                    TestPublish(publisher),
                    TestPublish(publisher)
                };

                for (var i = 0; i < tasks.Length; i++)
                {
                    await SpinWaitFor(() => events.Count == 1);
                    await Task.Delay(100);
                    events.Count.ShouldBe(1);
                    events.TryDequeue(out var e);
                    e.Set();
                    e.Dispose();
                }

                await Task.Delay(100);
                events.Count.ShouldBe(0);
            }
        }

        [Test]
        public async Task ShouldCancel()
        {
            var events = new ConcurrentQueue<ManualResetEventSlim>();
            var sources = new ConcurrentQueue<TaskCompletionSource<bool>>();
            using (var publisher = CreateTarget(req =>
            {
                var mre = new ManualResetEventSlim(false);
                events.Enqueue(mre);
                mre.Wait();
                var tcs = new TaskCompletionSource<bool>();
                sources.Enqueue(tcs);
                return tcs.Task;
            }))
            {
                var currentlyPublishing = TestPublish(publisher);

                await SpinWaitFor(() => events.Count == 1);

                var cancellationTokenSource = new CancellationTokenSource();
                var eventuallyCancelled = TestPublish(publisher, cancellationToken: cancellationTokenSource.Token);

                eventuallyCancelled.IsCompleted.ShouldBeFalse();

                cancellationTokenSource.Cancel();

                await Task.WhenAny(eventuallyCancelled);
                eventuallyCancelled.IsCompleted.ShouldBeTrue();
                eventuallyCancelled.IsCanceled.ShouldBeTrue();

                var immediatelyCancelled = TestPublish(publisher, cancellationToken: cancellationTokenSource.Token);

                immediatelyCancelled.IsCompleted.ShouldBeTrue();
                immediatelyCancelled.IsCanceled.ShouldBeTrue();

                events.TryDequeue(out var e);
                e.Set();

                await SpinWaitFor(() => sources.Count == 1);
                await DequeueAndSetResultAsync(sources, true);

                (await currentlyPublishing).ShouldBeTrue();
            }
        }

        [Test]
        public async Task ShouldNoCancelAfterBasicPublishIsCalled()
        {
            var events = new ConcurrentQueue<ManualResetEventSlim>();
            var sources = new ConcurrentQueue<TaskCompletionSource<bool>>();
            using (var publisher = CreateTarget(req =>
            {
                var mre = new ManualResetEventSlim(false);
                events.Enqueue(mre);
                mre.Wait();
                var tcs = new TaskCompletionSource<bool>();
                sources.Enqueue(tcs);
                return tcs.Task;
            }))
            {
                var cancellationTokenSource = new CancellationTokenSource();
                var currentlyPublishing = TestPublish(publisher, cancellationToken: cancellationTokenSource.Token);

                await SpinWaitFor(() => events.Count == 1);

                cancellationTokenSource.Cancel();

                await Task.Delay(100);
                currentlyPublishing.IsCompleted.ShouldBeFalse();

                events.TryDequeue(out var e);
                e.Set();

                await SpinWaitFor(() => sources.Count == 1);
                await DequeueAndSetResultAsync(sources, true);

                (await currentlyPublishing).ShouldBeTrue();
            }
        }

        [Test]
        public async Task ShouldDispose()
        {
            var events = new ConcurrentQueue<ManualResetEventSlim>();
            var sources = new ConcurrentQueue<TaskCompletionSource<bool>>();
            using (var publisher = CreateTarget(req =>
            {
                var mre = new ManualResetEventSlim(false);
                events.Enqueue(mre);
                mre.Wait();
                var tcs = new TaskCompletionSource<bool>();
                sources.Enqueue(tcs);
                return tcs.Task;
            }))
            {
                var currentlyPublishing = TestPublish(publisher, new ReadOnlyMemory<byte>(new byte[1]));
                await SpinWaitFor(() => events.Count == 1);

                var eventuallyDisposed = TestPublish(publisher, new ReadOnlyMemory<byte>(new byte[2]));
                eventuallyDisposed.IsCompleted.ShouldBeFalse();

                publisher.Dispose();

                events.TryDequeue(out var e);
                e.Set();

                Console.WriteLine("await Task.WhenAny(eventuallyDisposed)");
                await Task.WhenAny(eventuallyDisposed);
                eventuallyDisposed.IsFaulted.ShouldBeTrue();
                eventuallyDisposed.Exception.InnerException.ShouldBeOfType<ObjectDisposedException>();

                Assert.Throws<ObjectDisposedException>(() =>
                    TestPublish(publisher, new ReadOnlyMemory<byte>(new byte[3])));

                await SpinWaitFor(() => sources.Count == 1);
                await DequeueAndSetResultAsync(sources, true);

                await Task.WhenAny(currentlyPublishing);
                currentlyPublishing.IsFaulted.ShouldBeTrue();
                currentlyPublishing.Exception.InnerException.ShouldBeOfType<ObjectDisposedException>();
            }
        }

        [Test]
        [TestCase(10000, 12345)]
        [TestCase(10000, 54321)]
        public async Task RandomTest(int messageCount, int seed)
        {
            var random = new Random(seed);
            var messages = new ConcurrentQueue<(ReadOnlyMemory<byte> body, bool result)>();
            var handlers = new ConcurrentQueue<(int iterationsToSpin, bool result)>();
            for (var i = 0; i < messageCount; i++)
            {
                var result = random.Next(1000) > 200;
                messages.Enqueue((
                    body: new ReadOnlyMemory<byte>(new byte[random.Next(1000)]),
                    result
                ));
                handlers.Enqueue((
                    iterationsToSpin: random.Next(100),
                    result
                ));
            }

            using (var publisher = CreateTarget(req =>
            {
                var tcs = new TaskCompletionSource<bool>();
                handlers.TryDequeue(out var item);
                Task.Run(() =>
                {
                    Thread.SpinWait(item.iterationsToSpin);
                    tcs.SetResult(item.result);
                });
                return tcs.Task;
            }))
            {
                var tasks = new List<Task<bool>>();
                var expectedResults = new List<bool>();

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
            using (var publisher = CreateTarget(req =>
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
                var tasks = new List<Task<bool>>();

                for (var i = 0; i < 1000; i++)
                {
                    for (var j = 0; j < 1000; j++)
                    {
                        tasks.Add(TestPublish(publisher, new ReadOnlyMemory<byte>(new byte[100])));
                    }

                    Thread.SpinWait(10);
                }

                await Task.WhenAll(tasks);
            }
        }

        private static Task<T> TestPublish<T>(IAsyncPublisher<T> publisher,
            ReadOnlyMemory<byte> body = default, CancellationToken cancellationToken = default)
        {
            return publisher.PublishAsync(string.Empty, string.Empty, body, default, cancellationToken);
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
}