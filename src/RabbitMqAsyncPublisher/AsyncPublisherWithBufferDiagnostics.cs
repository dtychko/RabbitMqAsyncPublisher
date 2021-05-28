using System;

namespace RabbitMqAsyncPublisher
{
    public interface IAsyncPublisherWithBufferDiagnostics :
        IQueueBasedPublisherDiagnostics<AsyncPublisherWithBufferStatus>, IUnexpectedExceptionDiagnostics
    {
        void TrackPublishJobStarting(PublishArgs publishArgs, AsyncPublisherWithBufferStatus status);

        void TrackPublishJobStarted(PublishArgs publishArgs, AsyncPublisherWithBufferStatus status);

        void TrackPublishJobCompleted(PublishArgs publishArgs, AsyncPublisherWithBufferStatus status,
            TimeSpan duration);

        void TrackPublishJobCancelled(PublishArgs publishArgs, AsyncPublisherWithBufferStatus status,
            TimeSpan duration);

        void TrackPublishJobFailed(PublishArgs publishArgs, AsyncPublisherWithBufferStatus status,
            TimeSpan duration, Exception ex);

        void TrackDisposeStarted(AsyncPublisherWithBufferStatus status);

        void TrackDisposeSucceeded(AsyncPublisherWithBufferStatus status, TimeSpan duration);
    }

    public class AsyncPublisherWithBufferDiagnostics : IAsyncPublisherWithBufferDiagnostics
    {
        public static readonly IAsyncPublisherWithBufferDiagnostics NoDiagnostics =
            new AsyncPublisherWithBufferDiagnostics();

        protected AsyncPublisherWithBufferDiagnostics()
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

        public virtual void TrackPublishJobEnqueued(PublishArgs publishArgs, AsyncPublisherWithBufferStatus status)
        {
        }

        public virtual void TrackPublishJobStarting(PublishArgs publishArgs, AsyncPublisherWithBufferStatus status)
        {
        }

        public virtual void TrackPublishJobStarted(PublishArgs publishArgs, AsyncPublisherWithBufferStatus status)
        {
        }

        public virtual void TrackPublishJobCompleted(PublishArgs publishArgs, AsyncPublisherWithBufferStatus status,
            TimeSpan duration)
        {
        }

        public virtual void TrackPublishJobCancelled(PublishArgs publishArgs, AsyncPublisherWithBufferStatus status,
            TimeSpan duration)
        {
        }

        public virtual void TrackPublishJobFailed(PublishArgs publishArgs, AsyncPublisherWithBufferStatus status,
            TimeSpan duration,
            Exception ex)
        {
        }

        public virtual void TrackDisposeStarted(AsyncPublisherWithBufferStatus status)
        {
        }

        public virtual void TrackDisposeSucceeded(AsyncPublisherWithBufferStatus status, TimeSpan duration)
        {
        }
    }
}