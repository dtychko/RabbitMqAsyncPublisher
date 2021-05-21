using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
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
                : TimeSpan.FromSeconds(60)))
            {
                var testTask = test(cts.Token);

                var resultTask = await Task.WhenAny(
                    Task.Delay(-1, cts.Token),
                    testTask);

                cts.IsCancellationRequested.ShouldBeFalse();

                if (resultTask == testTask)
                {
                    await testTask;
                }
            }
        }

        public static string GetMessageTag(IBasicProperties properties) => ((TestBasicProperties) properties).TestTag;
    }
}