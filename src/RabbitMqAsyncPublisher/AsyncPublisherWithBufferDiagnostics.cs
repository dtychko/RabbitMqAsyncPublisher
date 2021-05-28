using System;

namespace RabbitMqAsyncPublisher
{
    public interface IAsyncPublisherWithBufferDiagnostics : IUnexpectedExceptionDiagnostics
    {
        void TrackJobEnqueued(PublishArgs publishArgs, AsyncPublisherWithBufferStatus status);

        void TrackJobStarting(PublishArgs publishArgs, AsyncPublisherWithBufferStatus status, TimeSpan elapsed);

        void TrackJobStarted(PublishArgs publishArgs, AsyncPublisherWithBufferStatus status, TimeSpan elapsed);

        void TrackJobSucceeded(PublishArgs publishArgs, AsyncPublisherWithBufferStatus status, TimeSpan elapsed);

        void TrackJobCancelled(PublishArgs publishArgs, AsyncPublisherWithBufferStatus status, TimeSpan elapsed);

        void TrackJobFailed(PublishArgs publishArgs, AsyncPublisherWithBufferStatus status, TimeSpan elapsed,
            Exception ex);

        void TrackDisposeStarted(AsyncPublisherWithBufferStatus status);

        void TrackDisposeSucceeded(AsyncPublisherWithBufferStatus status, TimeSpan duration);
    }

    public readonly struct AsyncPublisherWithBufferStatus
    {
        public readonly int JobQueueSize;
        public readonly int ProcessingMessages;
        public readonly int ProcessingBytes;

        public AsyncPublisherWithBufferStatus(int jobQueueSize, int processingMessages, int processingBytes)
        {
            JobQueueSize = jobQueueSize;
            ProcessingMessages = processingMessages;
            ProcessingBytes = processingBytes;
        }
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

        public virtual void TrackJobEnqueued(PublishArgs publishArgs, AsyncPublisherWithBufferStatus status)
        {
        }

        public virtual void TrackJobStarting(PublishArgs publishArgs, AsyncPublisherWithBufferStatus status,
            TimeSpan elapsed)
        {
        }

        public virtual void TrackJobStarted(PublishArgs publishArgs, AsyncPublisherWithBufferStatus status,
            TimeSpan elapsed)
        {
        }

        public virtual void TrackJobSucceeded(PublishArgs publishArgs, AsyncPublisherWithBufferStatus status,
            TimeSpan elapsed)
        {
        }

        public virtual void TrackJobCancelled(PublishArgs publishArgs, AsyncPublisherWithBufferStatus status,
            TimeSpan elapsed)
        {
        }

        public virtual void TrackJobFailed(PublishArgs publishArgs, AsyncPublisherWithBufferStatus status,
            TimeSpan elapsed, Exception ex)
        {
        }

        public virtual void TrackDisposeStarted(AsyncPublisherWithBufferStatus status)
        {
        }

        public virtual void TrackDisposeSucceeded(AsyncPublisherWithBufferStatus status, TimeSpan duration)
        {
        }

        public virtual void TrackReaderLoopIterationStarted(AsyncPublisherWithBufferStatus status)
        {
        }
    }
}