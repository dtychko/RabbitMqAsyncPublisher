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
        [Test]
        [Repeat(2)]
        [Timeout(3000)]
        public async Task RandomSyncTest(
            [Values(1, 10, 100)] int partitionProcessingLimit,
            // [Values(1, 10)] int partitionCount,
            [Values(10)] int partitionCount,
            [Values(100_000)] int valueCount,
            [Values(1, 5, 10)] int writerCount,
            [Values(5, 10)] int readerCount)
        {
            ThreadPool.SetMaxThreads(100, 100);
            ThreadPool.SetMinThreads(100, 100);

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
                        queue.Enqueue($"partition#{random.Next(partitionCount)}", j);
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
                                        Console.WriteLine("cts.Cancel()");
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

            Console.WriteLine("await Task.WhenAll(writers).ConfigureAwait(false)");
            await Task.WhenAll(writers).ConfigureAwait(false);

            // Task.Delay(1000).ContinueWith(_ => { Console.WriteLine(counter); }, cts.Token);

            Console.WriteLine("await Task.WhenAll(readers).ConfigureAwait(false)");
            var results = await Task.WhenAll(readers).ConfigureAwait(false);
            var resultCount = results.Aggregate(0, (count, arr) => count + arr.Length);

            Console.WriteLine($"Total = {resultCount}");
            Console.WriteLine($"Duration = {stopwatch.ElapsedMilliseconds} ms");
            // Console.WriteLine($"Partitions = {queue.PartitionCount}");

            resultCount.ShouldBe(writerCount * valuesPerWriter);
            // queue.PartitionCount.ShouldBe(0);

            // foreach (var result in results)
            // {
            //     Console.WriteLine("=======================================================");
            //     Console.WriteLine($"Total = {result.Length}");
            //     Console.WriteLine(string.Join("; ",
            //         result.GroupBy(x => x)
            //             .OrderBy(x => x.Key)
            //             .Select(x => $"{x.Key} = {x.Count()}")));
            // }
        }

        [Test]
        [Repeat(2)]
        public async Task TillTheEnd()
        {
            ThreadPool.SetMaxThreads(100, 100);
            ThreadPool.SetMinThreads(100, 100);

            // var queue = new BalancedQueue<int>(int.MaxValue);
            var queue = new BalancedQueue<int>(int.MaxValue);

            for (var i = 0; i < 1_000_000; i++)
            {
                queue.Enqueue("single_partition", i);
            }

            var mre = new ManualResetEventSlim(false);

            var readerCount = Math.Min(Environment.ProcessorCount * 2, 10);
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
    }
}