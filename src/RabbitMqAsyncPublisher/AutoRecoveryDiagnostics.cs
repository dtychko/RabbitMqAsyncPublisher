using System;
using RabbitMQ.Client;

namespace RabbitMqAsyncPublisher
{
    public interface IUnexpectedExceptionDiagnostics
    {
        void TrackUnexpectedException(string message, Exception ex);
    }

    public interface IAutoRecoveryDiagnostics : IUnexpectedExceptionDiagnostics
    {
        void TrackCreateResourceAttemptStarted(string sessionId, int attempt);

        void TrackCreateResourceAttemptSucceeded(string sessionId, int attempt, TimeSpan duration);

        void TrackCreateResourceAttemptFailed(string sessionId, int attempt, TimeSpan duration, Exception ex);

        void TrackCreateResourceSucceeded(string sessionId, TimeSpan duration);

        void TrackCreateResourceFailed(string sessionId, TimeSpan duration, Exception ex);

        void TrackCreateResourceCompleted(string sessionId, int createdCount, int expectedCount, TimeSpan duration);

        void TrackCleanUpStarted(string sessionId);

        void TrackCleanUpCompleted(string sessionId, TimeSpan duration);

        void TrackResourceClosed(string sessionId, ShutdownEventArgs args);

        void TrackDisposeStarted();

        void TrackDisposeCompleted(TimeSpan duration);
    }

    public class AutoRecoveryConsoleDiagnostics : IAutoRecoveryDiagnostics
    {
        private readonly string _resourceId;

        public AutoRecoveryConsoleDiagnostics(string resourceId)
        {
            _resourceId = resourceId;
        }

        public void TrackCreateResourceAttemptStarted(string sessionId, int attempt)
        {
            Console.WriteLine(
                $" >> {_resourceId}/{nameof(TrackCreateResourceAttemptStarted)} {nameof(sessionId)}={sessionId} {nameof(attempt)}={attempt} {nameof(DateTime.UtcNow)}={DateTime.UtcNow}");
        }

        public void TrackCreateResourceAttemptSucceeded(string sessionId, int attempt, TimeSpan duration)
        {
            Console.WriteLine(
                $" >> {_resourceId}/{nameof(TrackCreateResourceAttemptSucceeded)} {nameof(sessionId)}={sessionId} {nameof(attempt)}={attempt} {nameof(duration)}={duration.TotalMilliseconds}");
        }

        public void TrackCreateResourceAttemptFailed(string sessionId, int attempt, TimeSpan duration, Exception ex)
        {
            Console.WriteLine(
                $" >> {_resourceId}/{nameof(TrackCreateResourceAttemptFailed)} {nameof(sessionId)}={sessionId} {nameof(attempt)}={attempt} {nameof(duration)}={duration.TotalMilliseconds}");
            Console.WriteLine(ex.Message);
        }

        public void TrackCreateResourceSucceeded(string sessionId, TimeSpan duration)
        {
            Console.WriteLine(
                $" >> {_resourceId}/{nameof(TrackCreateResourceSucceeded)} {nameof(sessionId)}={sessionId} {nameof(duration)}={duration.TotalMilliseconds}");
        }

        public void TrackCreateResourceFailed(string sessionId, TimeSpan duration, Exception ex)
        {
            Console.WriteLine(
                $" >> {_resourceId}/{nameof(TrackCreateResourceFailed)} {nameof(sessionId)}={sessionId} {nameof(duration)}={duration.TotalMilliseconds}");
            Console.WriteLine(ex.Message);
        }

        public void TrackCreateResourceCompleted(string sessionId, int createdCount, int expectedCount,
            TimeSpan duration)
        {
            Console.WriteLine(
                $" >> {_resourceId}/{nameof(TrackCreateResourceCompleted)} {nameof(sessionId)}={sessionId} {nameof(createdCount)}={createdCount} {nameof(expectedCount)}={expectedCount} {nameof(duration)}={duration.TotalMilliseconds}");
        }

        public void TrackCleanUpStarted(string sessionId)
        {
            Console.WriteLine($" >> {_resourceId}/{nameof(TrackCleanUpStarted)} {nameof(sessionId)}={sessionId}");
        }

        public void TrackCleanUpCompleted(string sessionId, TimeSpan duration)
        {
            Console.WriteLine(
                $" >> {_resourceId}/{nameof(TrackCleanUpCompleted)} {nameof(sessionId)}={sessionId} {nameof(duration)}={duration.TotalMilliseconds}");
        }

        public void TrackResourceClosed(string sessionId, ShutdownEventArgs args)
        {
            Console.WriteLine(
                $" >> {_resourceId}/{nameof(TrackResourceClosed)} {nameof(sessionId)}={sessionId} {nameof(args)}={args}");
        }

        public void TrackDisposeStarted()
        {
            Console.WriteLine($" >> {_resourceId}/{nameof(TrackDisposeStarted)}");
        }

        public void TrackDisposeCompleted(TimeSpan duration)
        {
            Console.WriteLine(
                $" >> {_resourceId}/{nameof(TrackDisposeCompleted)} {nameof(duration)}={duration.TotalMilliseconds}");
        }

        public void TrackUnexpectedException(string message, Exception ex)
        {
            Console.WriteLine($" >> {_resourceId}/{nameof(TrackUnexpectedException)} {nameof(message)}={message}");
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

        public virtual void TrackCreateResourceAttemptFailed(string sessionId, int attempt, TimeSpan duration,
            Exception ex)
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

        public virtual void TrackCleanUpStarted(string sessionId)
        {
        }

        public virtual void TrackCleanUpCompleted(string sessionId, TimeSpan duration)
        {
        }

        public virtual void TrackResourceClosed(string sessionId, ShutdownEventArgs args)
        {
        }

        public virtual void TrackDisposeStarted()
        {
        }

        public virtual void TrackDisposeCompleted(TimeSpan duration)
        {
        }

        public virtual void TrackUnexpectedException(string message, Exception ex)
        {
        }
    }
}