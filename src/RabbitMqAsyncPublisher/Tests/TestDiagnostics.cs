using System;
using RabbitMQ.Client;
using RabbitMqAsyncPublisher;

namespace Tests
{
    internal class TestDiagnostics : EmptyDiagnostics
    {
        public int FailedRetryAttemptCount { get; private set; }

        public override void TrackPublishUnsafeAttemptFailed(PublishUnsafeAttemptArgs args, TimeSpan duration,
            Exception ex)
        {
            FailedRetryAttemptCount++;
            Console.WriteLine($"{GetTestTag(args)}/{args.Attempt}/error {duration.TotalMilliseconds}");
        }

        public override void TrackPublishUnsafeAttemptCompleted(PublishUnsafeAttemptArgs args, TimeSpan duration,
            bool acknowledged)
        {
            Console.WriteLine($"{GetTestTag(args)}/{args.Attempt}/completed {duration.TotalMilliseconds}");
        }

        public override void TrackRecoveryEventProcessing()
        {
            Console.WriteLine("model/recovery/processing");
        }

        public override void TrackRecoveryEventProcessingCompleted(TimeSpan duration)
        {
            Console.WriteLine($"model/recovery/completed");
        }

        public override void TrackModelShutdownEventProcessing(ShutdownEventArgs args)
        {
            Console.WriteLine("model/shutdown/processing");
        }

        public override void TrackModelShutdownEventProcessingCompleted(ShutdownEventArgs args, TimeSpan duration)
        {
            Console.WriteLine("model/shutdown/completed");
        }

        public override void TrackPublishUnsafeAttempt(PublishUnsafeAttemptArgs args)
        {
            Console.WriteLine($"{GetTestTag(args)}/{args.Attempt}/started");
        }

        private static string GetTestTag(PublishArgs args) => TestUtils.GetMessageTag(args.Properties);

        private static string GetTestTag(PublishUnsafeAttemptArgs args) => TestUtils.GetMessageTag(args.Properties);
    }
}