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
    public class BalancedProcessorTests
    {
        [OneTimeSetUp]
        public void TestFixtureSetUp()
        {
            ThreadPool.SetMaxThreads(100, 100);
            ThreadPool.SetMinThreads(100, 100);
        }

        [Test]
        [Repeat(10)]
        [Timeout(3000)]
        public async Task ShouldProcessWithLimitedDegreeOfParallelism()
        {
            var sources = new ConcurrentQueue<TaskCompletionSource<bool>>();
            var processor = new BalancedProcessor<int>((value, partitionKey, cancellationToken) =>
            {
                var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                sources.Enqueue(tcs);
                return tcs.Task;
            }, 2);
            processor.Start();

            var t101 = processor.ProcessAsync(101, "partition/1");
            var t201 = processor.ProcessAsync(201, "partition/2");
            var t102 = processor.ProcessAsync(102, "partition/1");
            var t202 = processor.ProcessAsync(202, "partition/2");

            await SpinWaitFor(() => sources.Count == 2);
            new[] {t101, t201, t102, t202}.All(x => !x.IsCompleted).ShouldBeTrue();
            sources.TryDequeue(out var source);
            source.SetResult(true);

            Console.WriteLine("await t101");
            await t101;

            Console.WriteLine("await SpinWaitFor(() => sources.Count == 2)");
            await SpinWaitFor(() => sources.Count == 2);
            new[] {t201, t102, t202}.All(x => !x.IsCompleted).ShouldBeTrue();
            sources.TryDequeue(out source);
            source.SetResult(true);

            Console.WriteLine("await t201");
            await t201;

            await SpinWaitFor(() => sources.Count == 2);
            new[] {t102, t202}.All(x => !x.IsCompleted).ShouldBeTrue();
            sources.TryDequeue(out source);
            source.SetResult(true);
            sources.TryDequeue(out source);
            source.SetResult(true);

            Console.WriteLine("await t102");
            await t102;
            Console.WriteLine("await t202");
            await t202;

            processor.Status.ProcessingCount.ShouldBe(0);
            processor.Status.ProcessedTotalCount.ShouldBe(4);
            processor.Dispose();

            Console.WriteLine("done");
        }

        [Test]
        public async Task ShouldProcessWithBalancing()
        {
            var sources = new ConcurrentQueue<TaskCompletionSource<bool>>();
            var processor = new BalancedProcessor<int>((value, partitionKey, cancellationToken) =>
            {
                var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                sources.Enqueue(tcs);
                return tcs.Task;
            }, 2);
            processor.Start();

            var t101 = processor.ProcessAsync(101, "partition/1");
            var t201 = processor.ProcessAsync(201, "partition/2");
            var t102 = processor.ProcessAsync(102, "partition/1");
            var t202 = processor.ProcessAsync(202, "partition/2");
            var t301 = processor.ProcessAsync(301, "partition/3");
            var t401 = processor.ProcessAsync(401, "partition/4");

            await SpinWaitFor(() => sources.Count == 2);
            sources.TryDequeue(out var source);
            source.SetResult(true);
            sources.TryDequeue(out source);
            source.SetResult(true);

            await t101;
            await t201;

            await SpinWaitFor(() => sources.Count == 2);
            sources.TryDequeue(out source);
            source.SetResult(true);
            sources.TryDequeue(out source);
            source.SetResult(true);

            await t301;
            await t401;

            await SpinWaitFor(() => sources.Count == 2);
            sources.TryDequeue(out source);
            source.SetResult(true);
            sources.TryDequeue(out source);
            source.SetResult(true);

            await t102;
            await t202;
        }

        [Test]
        public async Task ShouldDispose()
        {
            var sources = new ConcurrentQueue<TaskCompletionSource<bool>>();
            var processor = new BalancedProcessor<int>((value, partitionKey, cancellationToken) =>
            {
                var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                sources.Enqueue(tcs);
                return Task.Delay(-1, cancellationToken);
            }, 1);
            processor.Start();

            var t101 = processor.ProcessAsync(101, "partition/1");
            var t102 = processor.ProcessAsync(102, "partition/1");

            await SpinWaitFor(() => sources.Count == 1);

            processor.Dispose();

            await Task.WhenAll(
                Task.WhenAny(t101),
                Task.WhenAny(t102)
            );

            t101.Exception.InnerException.ShouldBeOfType<ObjectDisposedException>();
            t102.Exception.InnerException.ShouldBeOfType<ObjectDisposedException>();
        }

        [Test]
        public async Task RandomTest()
        {
            var random = new Random(123);
            var messages = new List<string>();
            var iterations = new ConcurrentQueue<int>();

            for (var i = 0; i < 100_000; i++)
            {
                messages.Add($"partition/{random.Next(100)}");
                iterations.Enqueue(random.Next(100));
            }

            var processor = new BalancedProcessor<int>((value, partitionKey, cancellationToken) =>
            {
                var tcs = new TaskCompletionSource<bool>();
                iterations.TryDequeue(out var it);
                Task.Run(() =>
                {
                    Thread.SpinWait(it);
                    tcs.SetResult(true);
                });
                return tcs.Task;
            }, 10);
            processor.Start();

            var tasks = new List<Task>();

            for (var i = 0; i < messages.Count; i++)
            {
                tasks.Add(processor.ProcessAsync(i, messages[i]));
            }

            await Task.WhenAll(tasks);
        }

        [Test]
        [Explicit]
        public async Task DisposePerf(
            [Values(100_000)] int taskCount)
        {
            var processor =
                new BalancedProcessor<int>(
                    (_, __, cancellationToken) => Task.Delay(-1, cancellationToken), 1);
            processor.Start();

            var tasks = new List<Task>();
            var random = new Random(123);

            for (var i = 0; i < taskCount; i++)
            {
                tasks.Add(processor.ProcessAsync(i, $"partition/{random.Next(10)}"));
            }

            var stopwatch = Stopwatch.StartNew();
            processor.Dispose();
            Console.WriteLine($"Dispose completed in {stopwatch.ElapsedMilliseconds} ms");

            stopwatch.Restart();
            await Task.WhenAll(tasks.Select(t => Task.WhenAny(t)));
            Console.WriteLine($"All tasks resolved in {stopwatch.ElapsedMilliseconds} ms");

            tasks.All(t => t.Exception.InnerException is ObjectDisposedException).ShouldBeTrue();
        }

        private static async Task SpinWaitFor(Func<bool> condition)
        {
            using (var cancellationTokenSource = new CancellationTokenSource(Debugger.IsAttached ? 60_000 : 1000))
            {
                var cancellationToken = cancellationTokenSource.Token;
                var winner = await Task.WhenAny(
                    Task.Delay(-1, cancellationToken),
                    Task.Run(() =>
                    {
                        var spinWait = new SpinWait();
                        while (!condition())
                        {
                            cancellationToken.ThrowIfCancellationRequested();
                            spinWait.SpinOnce();
                        }
                    }, cancellationToken)
                );
                cancellationToken.IsCancellationRequested.ShouldBe(false,
                    $"{nameof(SpinWaitFor)} cancelled by a timeout");
                await winner;
            }
        }
    }
}