using System;

namespace RabbitMqAsyncPublisher
{
    public interface IAsyncPublisherDiagnostics : IUnexpectedExceptionDiagnostics
    {
        void TrackPublishTaskEnqueued(PublishArgs publishArgs, AsyncPublisherStatus status);

        void TrackPublishStarted(PublishArgs publishArgs, ulong deliveryTag);

        void TrackPublishSucceeded(PublishArgs publishArgs, ulong deliveryTag, TimeSpan duration);

        void TrackPublishFailed(PublishArgs publishArgs, ulong deliveryTag, TimeSpan duration, Exception ex);

        void TrackAckJobEnqueued(AckArgs ackArgs, AsyncPublisherStatus status);

        void TrackAckStarted(AckArgs ackArgs);

        void TrackAckSucceeded(AckArgs ackArgs, TimeSpan duration);

        void TrackDisposeStarted();

        void TrackDisposeSucceeded(TimeSpan duration);

        void TrackStatus(AsyncPublisherStatus status);
    }

    public class AsyncPublisherStatus
    {
        public int PublishQueueSize { get; }
        public int AckQueueSize { get; }
        public int CompletionSourceRegistrySize { get; }

        public AsyncPublisherStatus(int publishQueueSize, int ackQueueSize, int completionSourceRegistrySize)
        {
            PublishQueueSize = publishQueueSize;
            AckQueueSize = ackQueueSize;
            CompletionSourceRegistrySize = completionSourceRegistrySize;
        }

        public override string ToString()
        {
            return $"{nameof(PublishQueueSize)}: {PublishQueueSize}, {nameof(AckQueueSize)}: {AckQueueSize}, {nameof(CompletionSourceRegistrySize)}: {CompletionSourceRegistrySize}";
        }
    }

    public class AckArgs
    {
        public ulong DeliveryTag { get; }
        public bool Multiple { get; }
        public bool Ack { get; }

        public AckArgs(ulong deliveryTag, bool multiple, bool ack)
        {
            DeliveryTag = deliveryTag;
            Multiple = multiple;
            Ack = ack;
        }
    }

    public class AsyncPublisherEmptyDiagnostics : IAsyncPublisherDiagnostics
    {
        public static readonly IAsyncPublisherDiagnostics NoDiagnostics =
            new AsyncPublisherEmptyDiagnostics();

        protected AsyncPublisherEmptyDiagnostics()
        {
        }

        public virtual void TrackUnexpectedException(string message, Exception ex)
        {
        }

        public virtual void TrackPublishTaskEnqueued(PublishArgs publishArgs, AsyncPublisherStatus status)
        {
        }

        public virtual void TrackPublishStarted(PublishArgs publishArgs, ulong deliveryTag)
        {
        }

        public virtual void TrackPublishSucceeded(PublishArgs publishArgs, ulong deliveryTag, TimeSpan duration)
        {
        }

        public virtual void TrackPublishFailed(PublishArgs publishArgs, ulong deliveryTag, TimeSpan duration,
            Exception ex)
        {
        }

        public virtual void TrackAckJobEnqueued(AckArgs ackArgs, AsyncPublisherStatus status)
        {
        }

        public virtual void TrackAckStarted(AckArgs ackArgs)
        {
        }

        public virtual void TrackAckSucceeded(AckArgs ackArgs, TimeSpan duration)
        {
        }

        public virtual void TrackDisposeStarted()
        {
        }

        public virtual void TrackDisposeSucceeded(TimeSpan duration)
        {
        }

        public virtual void TrackStatus(AsyncPublisherStatus status)
        {
        }
    }

    public class AsyncPublisherConsoleDiagnostics : IAsyncPublisherDiagnostics
    {
        public void TrackUnexpectedException(string message, Exception ex)
        {
            Console.WriteLine($" >> {nameof(TrackUnexpectedException)}: {message} {ex}");
        }

        public void TrackPublishTaskEnqueued(PublishArgs publishArgs, AsyncPublisherStatus status)
        {
            Console.WriteLine($" >> {nameof(TrackPublishTaskEnqueued)}: {status}");
        }

        public void TrackPublishStarted(PublishArgs publishArgs, ulong deliveryTag)
        {
            Console.WriteLine($" >> {nameof(TrackPublishStarted)}: {deliveryTag}");
        }

        public void TrackPublishSucceeded(PublishArgs publishArgs, ulong deliveryTag, TimeSpan duration)
        {
            Console.WriteLine($" >> {nameof(TrackPublishSucceeded)}: {deliveryTag} in {duration.TotalMilliseconds}ms");
        }

        public void TrackPublishFailed(PublishArgs publishArgs, ulong deliveryTag, TimeSpan duration, Exception ex)
        {
            Console.WriteLine($" >> {nameof(TrackPublishFailed)}: {deliveryTag} in {duration.TotalMilliseconds}ms {ex}");
        }

        public void TrackAckJobEnqueued(AckArgs ackArgs, AsyncPublisherStatus status)
        {
            Console.WriteLine($" >> {nameof(TrackAckJobEnqueued)}: {ackArgs.DeliveryTag}, {status}");
        }

        public void TrackAckStarted(AckArgs ackArgs)
        {
            Console.WriteLine($" >> {nameof(TrackAckStarted)}: {ackArgs.DeliveryTag}");
        }

        public void TrackAckSucceeded(AckArgs ackArgs, TimeSpan duration)
        {
            Console.WriteLine($" >> {nameof(TrackAckSucceeded)}: {ackArgs.DeliveryTag} in {duration.TotalMilliseconds}ms");
        }

        public void TrackDisposeStarted()
        {
            Console.WriteLine($" >> {nameof(AsyncPublisher)}/{nameof(TrackDisposeStarted)}");
        }

        public void TrackDisposeSucceeded(TimeSpan duration)
        {
            Console.WriteLine($" >> {nameof(AsyncPublisher)}/{nameof(TrackDisposeSucceeded)} in {duration.TotalMilliseconds}");
        }

        public void TrackStatus(AsyncPublisherStatus status)
        {
            Console.WriteLine($" >> {nameof(AsyncPublisher)}/{nameof(TrackStatus)}: {status}");
        }
    }
}