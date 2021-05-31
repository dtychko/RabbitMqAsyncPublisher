using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMqAsyncPublisher;
using Shouldly;

namespace Tests
{
    internal static class TestUtils
    {
        /// <remarks>
        /// Adds some small blocking delay.
        /// Thread.Sleep is not used because it can't actually sleep for less than 15-20ms, depending on OS.
        /// </remarks>
        /// <param name="iterations"></param>
        public static void SyncWait(int iterations = 5)
        {
            var wait = new SpinWait();
            for (var i = 0; i < iterations; i++)
            {
                wait.SpinOnce();
            }
        }

        public static void ConfigureThreadPool(int numberOfPublishers)
        {
            var totalThreadCount = numberOfPublishers + 10;
            ThreadPool.SetMaxThreads(totalThreadCount, totalThreadCount);
            ThreadPool.SetMinThreads(totalThreadCount, totalThreadCount);
        }
        
        public static async Task RunWithTimeout(Func<CancellationToken, Task> test)
        {
            using (var cts = new CancellationTokenSource(Debugger.IsAttached
                ? TimeSpan.FromMinutes(5)
                : TimeSpan.FromSeconds(10)))
            {
                var testTask = test(cts.Token);

                var resultTask = await Task.WhenAny(
                    Task.Delay(-1, cts.Token),
                    testTask);

                cts.IsCancellationRequested.ShouldBeFalse($"{nameof(RunWithTimeout)} cancelled by a timeout");

                if (resultTask == testTask)
                {
                    await testTask;
                }
            }
        }
        
        public static string GetMessageTag(MessageProperties properties) => ((TestMessageProperties) properties).TestTag;

        public static string GetMessageTag(IBasicProperties properties) =>
            properties.Headers.TryGetValue(TestMessageProperties.TagName, out var value)
                ? value.ToString()
                : null;

        public static bool WaitFor(
            Func<bool> predicate,
            TimeSpan? timeout = null,
            TimeSpan? checkInterval = null)
        {
            var cts = new CancellationTokenSource(timeout ?? TimeSpan.FromSeconds(1));

            while (!cts.IsCancellationRequested)
            {
                if (predicate())
                {
                    return true;
                }

                Thread.Sleep(checkInterval ?? TimeSpan.FromMilliseconds(33));
            }

            return false;
        }
        
        public static async Task SpinWaitFor(Func<bool> condition)
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
                cancellationToken.IsCancellationRequested.ShouldBe(false, $"{nameof(SpinWaitFor)} cancelled by a timeout");
                await winner;
            }
        }
        
        public static Task<T> TestPublish<T>(IAsyncPublisher<T> publisher,
            ReadOnlyMemory<byte> body = default, CancellationToken cancellationToken = default)
        {
            return publisher.PublishAsync(string.Empty, string.Empty, body, MessageProperties.Default, default,
                cancellationToken);
        }

        public static Task DequeueAndSetResultAsync<T>(ConcurrentQueue<TaskCompletionSource<T>> queue, T result)
        {
            return Task.Run(() =>
            {
                queue.TryDequeue(out var item);
                item.SetResult(result);
            });
        }

        public static Task DequeueAndSetExceptionAsync<T>(ConcurrentQueue<TaskCompletionSource<T>> queue, Exception ex)
        {
            return Task.Run(() =>
            {
                queue.TryDequeue(out var item);
                item.SetException(ex);
            });
        }

        public static Task DequeueAndSetCanceledAsync<T>(ConcurrentQueue<TaskCompletionSource<T>> queue)
        {
            return Task.Run(() =>
            {
                queue.TryDequeue(out var item);
                item.SetCanceled();
            });
        }
    }
}