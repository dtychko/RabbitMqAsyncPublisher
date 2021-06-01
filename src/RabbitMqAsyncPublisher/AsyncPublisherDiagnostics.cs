using System;

namespace RabbitMqAsyncPublisher
{
    public interface IUnexpectedExceptionDiagnostics
    {
        void TrackUnexpectedException(string message, Exception ex);
    }

    public interface IPublisherDiagnostics : IUnexpectedExceptionDiagnostics
    {
        void TrackPublishStarted(PublishArgs publishArgs);

        void TrackPublishCompleted(PublishArgs publishArgs, TimeSpan duration);

        void TrackPublishCancelled(PublishArgs publishArgs, TimeSpan duration);

        void TrackPublishFailed(PublishArgs publishArgs, TimeSpan duration, Exception ex);
    }

    public interface IPublisherDiagnostics<in TStatus> : IPublisherDiagnostics
    {
        void TrackDisposeStarted(TStatus status);

        void TrackDisposeCompleted(TStatus status, TimeSpan duration);
    }

    public interface IQueueBasedPublisherDiagnostics<in TStatus> : IPublisherDiagnostics<TStatus>
    {
        void TrackPublishJobEnqueued(PublishArgs publishArgs, TStatus status);
    }

    public interface IAsyncPublisherDiagnostics : IQueueBasedPublisherDiagnostics<AsyncPublisherStatus>
    {
        void TrackPublishJobStarted(PublishArgs publishArgs, AsyncPublisherStatus status);

        void TrackPublishJobCompleted(PublishArgs publishArgs, AsyncPublisherStatus status, ulong deliveryTag,
            TimeSpan duration);

        void TrackPublishJobCancelled(PublishArgs publishArgs, AsyncPublisherStatus status, ulong deliveryTag,
            TimeSpan duration);

        void TrackPublishJobFailed(PublishArgs publishArgs, AsyncPublisherStatus status, ulong deliveryTag,
            TimeSpan duration, Exception ex);

        void TrackAckJobEnqueued(AckArgs ackArgs, AsyncPublisherStatus status);

        void TrackAckJobStarted(AckArgs ackArgs, AsyncPublisherStatus status);

        void TrackAckJobCompleted(AckArgs ackArgs, AsyncPublisherStatus status, TimeSpan duration);
    }

    public class AsyncPublisherDiagnostics : IAsyncPublisherDiagnostics
    {
        public static readonly IAsyncPublisherDiagnostics NoDiagnostics = new AsyncPublisherDiagnostics();

        protected AsyncPublisherDiagnostics()
        {
        }

        public virtual void TrackUnexpectedException(string message, Exception ex)
        {
        }

        public virtual void TrackPublishStarted(PublishArgs publishArgs)
        {
        }

        public virtual void TrackPublishCompleted(PublishArgs publishArgs, TimeSpan duration)
        {
        }

        public virtual void TrackPublishCancelled(PublishArgs publishArgs, TimeSpan duration)
        {
        }

        public virtual void TrackPublishFailed(PublishArgs publishArgs, TimeSpan duration, Exception ex)
        {
        }

        public virtual void TrackPublishJobEnqueued(PublishArgs publishArgs, AsyncPublisherStatus status)
        {
        }

        public virtual void TrackPublishJobStarting(PublishArgs publishArgs, AsyncPublisherStatus status)
        {
        }

        public virtual void TrackPublishJobStarted(PublishArgs publishArgs, AsyncPublisherStatus status,
            ulong deliveryTag)
        {
        }

        public virtual void TrackPublishJobCompleted(PublishArgs publishArgs, AsyncPublisherStatus status,
            ulong deliveryTag, TimeSpan duration)
        {
        }

        public virtual void TrackPublishJobCancelled(PublishArgs publishArgs, AsyncPublisherStatus status,
            ulong deliveryTag, TimeSpan duration)
        {
        }

        public virtual void TrackPublishJobFailed(PublishArgs publishArgs, AsyncPublisherStatus status,
            ulong deliveryTag, TimeSpan duration, Exception ex)
        {
        }

        public virtual void TrackAckJobEnqueued(AckArgs ackArgs, AsyncPublisherStatus status)
        {
        }

        public virtual void TrackAckJobStarted(AckArgs ackArgs, AsyncPublisherStatus status)
        {
        }

        public virtual void TrackAckJobCompleted(AckArgs ackArgs, AsyncPublisherStatus status, TimeSpan duration)
        {
        }

        public virtual void TrackDisposeStarted(AsyncPublisherStatus status)
        {
        }

        public virtual void TrackDisposeCompleted(AsyncPublisherStatus status, TimeSpan duration)
        {
        }
    }
}