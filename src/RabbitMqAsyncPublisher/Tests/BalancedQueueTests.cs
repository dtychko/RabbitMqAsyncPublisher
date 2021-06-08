using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using RabbitMqAsyncPublisher;
using Shouldly;

namespace Tests
{
    [TestFixture]
    public class BalancedQueueTests
    {
        [OneTimeSetUp]
        public void TestFixtureSetUp()
        {
            ThreadPool.SetMaxThreads(100, 100);
            ThreadPool.SetMinThreads(100, 100);
        }

        [Test]
        public async Task ShouldBalance()
        {
            var queue = new BalancedQueue<int>(int.MaxValue);

            queue.Enqueue(101, "partition/1");
            queue.Enqueue(102, "partition/1");

            queue.Enqueue(201, "partition/2");
            queue.Enqueue(202, "partition/2");
            queue.Enqueue(203, "partition/2");

            queue.Status.ShouldBe(new BalancedQueueStatus(5, 2, 2));

            var results = new List<(int, string)>();

            while (queue.Status.ValueCount > 0)
            {
                var handler = await queue.DequeueAsync();
                await handler((value, partitionKey) =>
                {
                    results.Add((value, partitionKey));
                    return Task.CompletedTask;
                });
            }

            results.Count.ShouldBe(5);
            results[0].ShouldBe((101, "partition/1"));
            results[1].ShouldBe((201, "partition/2"));
            results[2].ShouldBe((102, "partition/1"));
            results[3].ShouldBe((202, "partition/2"));
            results[4].ShouldBe((203, "partition/2"));
        }

        [Test]
        public async Task ShouldIgnoreExceptions()
        {
            var queue = new BalancedQueue<int>(int.MaxValue);

            queue.Enqueue(101, "partition/1");

            var handler = await queue.DequeueAsync();
            await handler((value, partitionKey) => Task.FromException(new InvalidOperationException()));

            queue.Status.ShouldBe(new BalancedQueueStatus(0, 0, 0));
        }

        [Test]
        public async Task ShouldComplete()
        {
            var queue = new BalancedQueue<int>(1);

            queue.Enqueue(101, "partition/1");
            queue.Enqueue(102, "partition/1");

            var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            var handler1 = await queue.DequeueAsync();
            var handlerTask1 = handler1((value, partitionKey) => tcs.Task);

            var dequeueTask2 = queue.DequeueAsync();
            var dequeueTask3 = queue.DequeueAsync();

            queue.TryComplete(new InvalidOperationException()).ShouldBeTrue();
            queue.TryComplete(new OperationCanceledException()).ShouldBeFalse();

            await Task.Delay(100);
            dequeueTask2.IsCompleted.ShouldBe(false);
            dequeueTask3.IsCompleted.ShouldBe(false);

            tcs.SetResult(true);

            (await handlerTask1).ShouldBe((101, "partition/1"));

            var handler2 = await dequeueTask2;
            (await handler2((value, partitionKey) => Task.CompletedTask)).ShouldBe((102, "partition/1"));

            await Task.WhenAny(dequeueTask3);
            dequeueTask3.Exception.InnerException.ShouldBeOfType<InvalidOperationException>();

            Assert.Throws<BalancedQueueCompletedException>(() => queue.Enqueue(103, "partition/1"));
            queue.DequeueAsync().IsFaulted.ShouldBeTrue();
            queue.TryDequeue(out _).ShouldBeFalse();
        }

        [Test]
        [Repeat(2)]
        public async Task ShouldDequeueUntilQueueIsEmpty()
        {
            var queue = new BalancedQueue<int>(int.MaxValue);

            for (var i = 0; i < 100_000; i++)
            {
                queue.Enqueue(i, "single_partition");
            }

            var mre = new ManualResetEventSlim(false);

            var readerCount = Math.Min(Environment.ProcessorCount, 10);
            var readersReadyEvent = new CountdownEvent(readerCount);
            var readers = new List<Task<(int processed, int remaining)>>();

            for (var i = 0; i < readerCount; i++)
            {
                readers.Add(Task.Run(async () =>
                {
                    var processed = 0;

                    readersReadyEvent.Signal();
                    mre.Wait();

                    while (queue.TryDequeue(out var handler))
                    {
                        await handler(async (v, p) => { await Task.Yield(); });
                        processed += 1;
                    }

                    return (processed, remaining: queue.Status.ValueCount);
                }));
            }

            readersReadyEvent.Wait();
            mre.Set();

            var counters = await Task.WhenAll(readers);

            counters.All(c => c.processed > 0 && c.remaining == 0).ShouldBeTrue();
        }

        [Test]
        public async Task ShouldLimitDegreeOfParallelismPerPartition()
        {
            var queue = new BalancedQueue<int>(2);

            queue.Enqueue(101, "partition/1");
            queue.Enqueue(102, "partition/1");
            queue.Enqueue(103, "partition/1");

            var dequeueTask1 = queue.DequeueAsync();
            var dequeueTask2 = queue.DequeueAsync();
            var dequeueTask3 = queue.DequeueAsync();

            await Task.Delay(100);

            dequeueTask1.IsCompleted.ShouldBeTrue();
            dequeueTask2.IsCompleted.ShouldBeTrue();
            dequeueTask3.IsCompleted.ShouldBeFalse();

            var handler1 = await dequeueTask1;
            var handler2 = await dequeueTask2;

            (await handler2((value, partitionKey) => Task.CompletedTask)).ShouldBe((102, "partition/1"));

            var handler3 = await dequeueTask3;

            (await handler3((value, partitionKey) => Task.CompletedTask)).ShouldBe((103, "partition/1"));
            (await handler1((value, partitionKey) => Task.CompletedTask)).ShouldBe((101, "partition/1"));
        }

        [Test]
        [Timeout(3000)]
        public async Task RandomTest(
            [Values(1, 10)] int partitionProcessingLimit,
            [Values(1, 10)] int partitionCount,
            [Values(100_000)] int valueCount,
            [Values(5)] int writerCount,
            [Values(5)] int readerCount)
        {
            var random = new Random(123);
            var mre = new ManualResetEventSlim(false);

            var queue = new BalancedQueue<int>(partitionProcessingLimit);

            var writersReadyEvent = new CountdownEvent(writerCount);
            var readersReadyEvent = new CountdownEvent(readerCount);

            var writers = new List<Task>();
            var valuesPerWriter = valueCount / writerCount;

            for (var i = 0; i < writerCount; i++)
            {
                writers.Add(Task.Run(() =>
                {
                    writersReadyEvent.Signal();
                    mre.Wait();

                    for (var j = 0; j < valuesPerWriter; j++)
                    {
                        queue.Enqueue(j, $"partition#{random.Next(partitionCount)}");
                    }
                }));
            }

            var readers = new List<Task<string[]>>();
            var cts = new CancellationTokenSource();
            var counter = 0;

            for (var i = 0; i < readerCount; i++)
            {
                readers.Add(Task.Run(async () =>
                {
                    var values = new ConcurrentQueue<string>();

                    try
                    {
                        readersReadyEvent.Signal();
                        mre.Wait();

                        while (!cts.IsCancellationRequested)
                        {
                            var handler = await queue.DequeueAsync(cts.Token).ConfigureAwait(false);

                            await handler((value, partitionKey) =>
                            {
                                return Task.Run(() =>
                                {
                                    values.Enqueue(partitionKey);
                                    Interlocked.Increment(ref counter);

                                    if (counter == writerCount * valuesPerWriter)
                                    {
                                        cts.Cancel();
                                    }
                                });
                            }).ConfigureAwait(false);
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        // Ignore
                    }

                    return values.ToArray();
                }, cts.Token));
            }

            readersReadyEvent.Wait();
            writersReadyEvent.Wait();

            var stopwatch = Stopwatch.StartNew();
            mre.Set();

            await Task.WhenAll(writers).ConfigureAwait(false);

            var results = await Task.WhenAll(readers).ConfigureAwait(false);
            var resultCount = results.Aggregate(0, (count, arr) => count + arr.Length);

            Console.WriteLine($"Total = {resultCount}");
            Console.WriteLine($"Duration = {stopwatch.ElapsedMilliseconds} ms");

            resultCount.ShouldBe(writerCount * valuesPerWriter);
            queue.Status.ShouldBe(new BalancedQueueStatus(0, 0, 0));
        }
    }
}