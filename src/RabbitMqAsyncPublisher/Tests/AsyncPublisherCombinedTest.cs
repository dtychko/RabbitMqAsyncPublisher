using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using RabbitMqAsyncPublisher;
using static Tests.TestUtils;

namespace Tests
{
    [TestFixture]
    public class AsyncPublisherCombinedTest
    {
        [Test]
        [Explicit]
        public async Task Perf()
        {
            var model = new TestNonRecoverableRabbitModel(_ =>
            {
                var tcs = new TaskCompletionSource<bool>();
                Task.Run(() =>
                {
                    Thread.SpinWait(10);
                    tcs.SetResult(true);
                });
                return tcs.Task;
            });

            using (
                var publisher = new AsyncPublisherWithBuffer<RetryPublishResult>(
                    new AsyncPublisherWithRetries(
                        new AsyncPublisher(model),
                        TimeSpan.Zero
                    ),
                    10
                )
            )
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