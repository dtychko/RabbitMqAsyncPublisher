using System;
using RabbitMQ.Client;

namespace RabbitMqAsyncPublisher
{
    public interface IAutoRecoveryDiagnostics
    {
        void TrackCreateResourceAttemptStarted(string sessionId, int attempt);

        void TrackCreateResourceAttemptSucceeded(string sessionId, int attempt, TimeSpan duration);

        void TrackCreateResourceAttemptFailed(string sessionId, int attempt, TimeSpan duration, Exception ex);

        void TrackCreateResourceSucceeded(string sessionId, TimeSpan duration);

        void TrackCreateResourceFailed(string sessionId, TimeSpan duration, Exception ex);

        void TrackCreateResourceCompleted(string sessionId, int createdCount, int expectedCount, TimeSpan duration);

        void TrackCleanUpStarted(string resourceId, string sessionId);

        void TrackCleanUpCompleted(string resourceId, string sessionId, TimeSpan duration);

        void TrackResourceClosed(string resourceId, ShutdownEventArgs args);

        void TrackDisposeStarted(string resourceId);

        void TrackDisposeCompleted(string resourceId, TimeSpan duration);

        void TrackUnexpectedException(string resourceId, string message, Exception ex);
    }

    public class AutoRecoveryConsoleDiagnostics : IAutoRecoveryDiagnostics
    {
        public void TrackCreateResourceAttemptStarted(string sessionId, int attempt)
        {
            Console.WriteLine(
                $" >> {nameof(TrackCreateResourceAttemptStarted)} {nameof(sessionId)}={sessionId} {nameof(attempt)}={attempt} {nameof(DateTime.UtcNow)}={DateTime.UtcNow}");
        }

        public void TrackCreateResourceAttemptSucceeded(string sessionId, int attempt, TimeSpan duration)
        {
            Console.WriteLine(
                $" >> {nameof(TrackCreateResourceAttemptSucceeded)} {nameof(sessionId)}={sessionId} {nameof(attempt)}={attempt} {nameof(duration)}={duration.TotalMilliseconds}");
        }

        public void TrackCreateResourceAttemptFailed(string sessionId, int attempt, TimeSpan duration, Exception ex)
        {
            Console.WriteLine(
                $" >> {nameof(TrackCreateResourceAttemptFailed)} {nameof(sessionId)}={sessionId} {nameof(attempt)}={attempt} {nameof(duration)}={duration.TotalMilliseconds}");
            Console.WriteLine(ex.Message);
        }

        public void TrackCreateResourceSucceeded(string sessionId, TimeSpan duration)
        {
            Console.WriteLine(
                $" >> {nameof(TrackCreateResourceSucceeded)} {nameof(sessionId)}={sessionId} {nameof(duration)}={duration.TotalMilliseconds}");
        }

        public void TrackCreateResourceFailed(string sessionId, TimeSpan duration, Exception ex)
        {
            Console.WriteLine(
                $" >> {nameof(TrackCreateResourceFailed)} {nameof(sessionId)}={sessionId} {nameof(duration)}={duration.TotalMilliseconds}");
            Console.WriteLine(ex.Message);
        }

        public void TrackCreateResourceCompleted(string sessionId, int createdCount, int expectedCount,
            TimeSpan duration)
        {
            Console.WriteLine(
                $" >> {nameof(TrackCreateResourceCompleted)} {nameof(sessionId)}={sessionId} {nameof(createdCount)}={createdCount} {nameof(expectedCount)}={expectedCount} {nameof(duration)}={duration.TotalMilliseconds}");
        }

        public void TrackCleanUpStarted(string resourceId, string sessionId)
        {
            Console.WriteLine($" >> {resourceId}/{nameof(TrackCleanUpStarted)} {nameof(sessionId)}={sessionId}");
        }

        public void TrackCleanUpCompleted(string resourceId, string sessionId, TimeSpan duration)
        {
            Console.WriteLine(
                $" >> {resourceId}/{nameof(TrackCleanUpCompleted)} {nameof(sessionId)}={sessionId} {nameof(duration)}={duration.TotalMilliseconds}");
        }

        public void TrackResourceClosed(string resourceId, ShutdownEventArgs args)
        {
            Console.WriteLine($" >> {resourceId}/{nameof(TrackResourceClosed)} {nameof(args)}={args}");
        }

        public void TrackDisposeStarted(string resourceId)
        {
            Console.WriteLine($" >> {resourceId}/{nameof(TrackDisposeStarted)}");
        }

        public void TrackDisposeCompleted(string resourceId, TimeSpan duration)
        {
            Console.WriteLine($" >> {resourceId}/{nameof(TrackDisposeCompleted)} {nameof(duration)}={duration.TotalMilliseconds}");
        }

        public void TrackUnexpectedException(string resourceId, string message, Exception ex)
        {
            Console.WriteLine($" >> {resourceId}/{nameof(TrackUnexpectedException)} {nameof(message)}={message}");
            Console.WriteLine(ex.ToString());
        }
    }

    public class AutoRecoveryDiagnostics : IAutoRecoveryDiagnostics
    {
        public static IAutoRecoveryDiagnostics NoDiagnostics = new AutoRecoveryDiagnostics();

        protected AutoRecoveryDiagnostics()
        {
        }

        public virtual void TrackCreateResourceAttemptStarted(string sessionId, int attempt)
        {
        }

        public virtual void TrackCreateResourceAttemptSucceeded(string sessionId, int attempt, TimeSpan duration)
        {
        }

        public virtual void TrackCreateResourceAttemptFailed(string sessionId, int attempt, TimeSpan duration, Exception ex)
        {
        }

        public virtual void TrackCreateResourceSucceeded(string sessionId, TimeSpan duration)
        {
        }

        public virtual void TrackCreateResourceFailed(string sessionId, TimeSpan duration, Exception ex)
        {
        }

        public virtual void TrackCreateResourceCompleted(string sessionId, int createdCount, int expectedCount,
            TimeSpan duration)
        {
        }

        public virtual void TrackCleanUpStarted(string resourceId, string sessionId)
        {
        }

        public virtual void TrackCleanUpCompleted(string resourceId, string sessionId, TimeSpan duration)
        {
        }

        public virtual void TrackResourceClosed(string resourceId, ShutdownEventArgs args)
        {
        }

        public virtual void TrackDisposeStarted(string resourceId)
        {
        }

        public virtual void TrackDisposeCompleted(string resourceId, TimeSpan duration)
        {
        }

        public virtual void TrackUnexpectedException(string resourceId, string message, Exception ex)
        {
        }
    }
}