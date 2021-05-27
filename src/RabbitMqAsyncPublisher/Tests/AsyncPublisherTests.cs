using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using RabbitMqAsyncPublisher;
using Shouldly;
using static Tests.TestUtils;

namespace Tests
{
    [TestFixture]
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
            return new AsyncPublisher(
                new TestNonRecoverableRabbitModel(handlePublish),
                new AsyncPublisherConsoleDiagnostics());
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

                // TODO: this is not needed for AsyncPublisherWithBufferTests. Consider unifying.
                await SpinWaitFor(() => t1.IsCompleted);
                t1.Result.ShouldBeTrue();
                t2.IsCompleted.ShouldBe(false);
                t3.IsCompleted.ShouldBe(false);

                await DequeueAndSetResultAsync(sources, false);
                // TODO: this is not needed for AsyncPublisherWithBufferTests. Consider unifying.
                await SpinWaitFor(() => t2.IsCompleted);
                t2.Result.ShouldBe(false);
                t3.IsCompleted.ShouldBe(false);

                await DequeueAndSetResultAsync(sources, true);
                // TODO: this is not needed for AsyncPublisherWithBufferTests. Consider unifying.
                await SpinWaitFor(() => t3.IsCompleted);
                t3.IsCompleted.ShouldBe(true);
                t3.Result.ShouldBe(true);
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