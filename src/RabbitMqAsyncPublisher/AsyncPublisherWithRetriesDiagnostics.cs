using System;

namespace RabbitMqAsyncPublisher
{
    public interface IAsyncPublisherWithRetriesDiagnostics
        : IQueueBasedPublisherDiagnostics<AsyncPublisherWithRetriesStatus>
    {
        void TrackPublishJobStarted(PublishArgs publishArgs, AsyncPublisherWithRetriesStatus status);

        void TrackPublishJobCompleted(PublishArgs publishArgs, AsyncPublisherWithRetriesStatus status,
            TimeSpan duration, bool acknowledged);

        void TrackPublishJobCancelled(PublishArgs publishArgs, AsyncPublisherWithRetriesStatus status,
            TimeSpan duration);

        void TrackPublishJobFailed(PublishArgs publishArgs, AsyncPublisherWithRetriesStatus status,
            TimeSpan duration, Exception ex);

        void TrackPublishAttemptStarted(PublishArgs publishArgs, int attempt);

        void TrackPublishAttemptCompleted(PublishArgs publishArgs, int attempt, TimeSpan duration, bool acknowledged,
            bool shouldRetry);

        void TrackPublishAttemptCancelled(PublishArgs publishArgs, int attempt, TimeSpan duration);

        void TrackPublishAttemptFailed(PublishArgs publishArgs, int attempt, TimeSpan duration, Exception ex,
            bool shouldRetry);
    }

    public class AsyncPublisherWithRetriesDiagnostics : IAsyncPublisherWithRetriesDiagnostics
    {
        public static readonly IAsyncPublisherWithRetriesDiagnostics NoDiagnostics =
            new AsyncPublisherWithRetriesDiagnostics();

        protected AsyncPublisherWithRetriesDiagnostics()
        {
        }

        public virtual void TrackPublishJobEnqueued(PublishArgs publishArgs, AsyncPublisherWithRetriesStatus status)
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

        public virtual void TrackUnexpectedException(string message, Exception ex)
        {
        }

        public virtual void TrackPublishJobStarted(PublishArgs publishArgs, AsyncPublisherWithRetriesStatus status)
        {
        }

        public virtual void TrackPublishJobCompleted(PublishArgs publishArgs, AsyncPublisherWithRetriesStatus status,
            TimeSpan duration, bool acknowledged)
        {
        }

        public virtual void TrackPublishJobCancelled(PublishArgs publishArgs, AsyncPublisherWithRetriesStatus status,
            TimeSpan duration)
        {
        }

        public virtual void TrackPublishJobFailed(PublishArgs publishArgs, AsyncPublisherWithRetriesStatus status,
            TimeSpan duration,
            Exception ex)
        {
        }

        public virtual void TrackPublishAttemptStarted(PublishArgs publishArgs, int attempt)
        {
        }

        public virtual void TrackPublishAttemptCompleted(PublishArgs publishArgs, int attempt, TimeSpan duration,
            bool acknowledged, bool shouldRetry)
        {
        }

        public virtual void TrackPublishAttemptCancelled(PublishArgs publishArgs, int attempt, TimeSpan duration)
        {
        }

        public virtual void TrackPublishAttemptFailed(PublishArgs publishArgs, int attempt, TimeSpan duration,
            Exception ex, bool shouldRetry)
        {
        }

        public virtual void TrackDisposeStarted(AsyncPublisherWithRetriesStatus status)
        {
        }

        public virtual void TrackDisposeCompleted(AsyncPublisherWithRetriesStatus status, TimeSpan duration)
        {
        }
    }
}