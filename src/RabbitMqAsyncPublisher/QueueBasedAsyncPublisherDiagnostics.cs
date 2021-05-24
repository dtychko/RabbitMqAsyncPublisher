using System;

namespace RabbitMqAsyncPublisher
{
    public interface IQueueBasedAsyncPublisherDiagnostics : IUnexpectedExceptionDiagnostics
    {
        void TrackPublishTaskEnqueued(PublishArgs publishArgs, QueueBasedAsyncPublisherStatus status);

        void TrackPublishStarted(PublishArgs publishArgs, ulong deliveryTag);

        void TrackPublishSucceeded(PublishArgs publishArgs, ulong deliveryTag, TimeSpan duration);

        void TrackPublishFailed(PublishArgs publishArgs, ulong deliveryTag, TimeSpan duration, Exception ex);

        void TrackAckTaskEnqueued(AckArgs ackArgs, QueueBasedAsyncPublisherStatus status);

        void TrackAckStarted(AckArgs ackArgs);

        void TrackAckSucceeded(AckArgs ackArgs, TimeSpan duration);

        void TrackDisposeStarted();

        void TrackDisposeSucceeded(TimeSpan duration);

        void TrackStatus(QueueBasedAsyncPublisherStatus status);
    }

    public class QueueBasedAsyncPublisherStatus
    {
        public int PublishQueueSize { get; }
        public int AckQueueSize { get; }
        public int CompletionSourceRegistrySize { get; }

        public QueueBasedAsyncPublisherStatus(int publishQueueSize, int ackQueueSize, int completionSourceRegistrySize)
        {
            PublishQueueSize = publishQueueSize;
            AckQueueSize = ackQueueSize;
            CompletionSourceRegistrySize = completionSourceRegistrySize;
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

    public class QueueBasedAsyncPublisherDiagnostics : IQueueBasedAsyncPublisherDiagnostics
    {
        public static readonly IQueueBasedAsyncPublisherDiagnostics NoDiagnostics =
            new QueueBasedAsyncPublisherDiagnostics();

        protected QueueBasedAsyncPublisherDiagnostics()
        {
        }

        public virtual void TrackUnexpectedException(string message, Exception ex)
        {
        }

        public virtual void TrackPublishTaskEnqueued(PublishArgs publishArgs, QueueBasedAsyncPublisherStatus status)
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

        public virtual void TrackAckTaskEnqueued(AckArgs ackArgs, QueueBasedAsyncPublisherStatus status)
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

        public virtual void TrackStatus(QueueBasedAsyncPublisherStatus status)
        {
        }
    }
}