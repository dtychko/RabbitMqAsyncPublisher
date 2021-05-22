using System;
using RabbitMQ.Client;

namespace RabbitMqAsyncPublisher
{
    public interface IAutoRecoveryDiagnostics
    {
        void TrackConnectAttemptStarted(string sessionId, int attempt);

        void TrackConnectAttemptSucceeded(string sessionId, int attempt, TimeSpan duration);

        void TrackConnectAttemptFailed(string sessionId, int attempt, TimeSpan duration, Exception ex);

        void TrackCreateConnectionSucceeded(string sessionId, TimeSpan duration);

        void TrackCreateConnectionFailed(string sessionId, TimeSpan duration, Exception ex);

        void TrackCreateComponentsCompleted(string sessionId, int createdCount, int expectedCount, TimeSpan duration);

        void TrackCleanUpStarted(string sessionId);

        void TrackCleanUpCompleted(string sessionId, TimeSpan duration);

        void TrackConnectionClosed(ShutdownEventArgs args);

        void TrackDisposeStarted();

        void TrackDisposeCompleted(TimeSpan duration);

        void TrackUnexpectedException(string message, Exception ex);
    }

    public class AutoRecoveryConsoleDiagnostics : IAutoRecoveryDiagnostics
    {
        public void TrackConnectAttemptStarted(string sessionId, int attempt)
        {
            Console.WriteLine(
                $" >> {nameof(TrackConnectAttemptStarted)} {nameof(sessionId)}={sessionId} {nameof(attempt)}={attempt} {nameof(DateTime.UtcNow)}={DateTime.UtcNow}");
        }

        public void TrackConnectAttemptSucceeded(string sessionId, int attempt, TimeSpan duration)
        {
            Console.WriteLine(
                $" >> {nameof(TrackConnectAttemptSucceeded)} {nameof(sessionId)}={sessionId} {nameof(attempt)}={attempt} {nameof(duration)}={duration.TotalMilliseconds}");
        }

        public void TrackConnectAttemptFailed(string sessionId, int attempt, TimeSpan duration, Exception ex)
        {
            Console.WriteLine(
                $" >> {nameof(TrackConnectAttemptFailed)} {nameof(sessionId)}={sessionId} {nameof(attempt)}={attempt} {nameof(duration)}={duration.TotalMilliseconds}");
            Console.WriteLine(ex.Message);
        }

        public void TrackCreateConnectionSucceeded(string sessionId, TimeSpan duration)
        {
            Console.WriteLine(
                $" >> {nameof(TrackCreateConnectionSucceeded)} {nameof(sessionId)}={sessionId} {nameof(duration)}={duration.TotalMilliseconds}");
        }

        public void TrackCreateConnectionFailed(string sessionId, TimeSpan duration, Exception ex)
        {
            Console.WriteLine(
                $" >> {nameof(TrackCreateConnectionFailed)} {nameof(sessionId)}={sessionId} {nameof(duration)}={duration.TotalMilliseconds}");
            Console.WriteLine(ex.Message);
        }

        public void TrackCreateComponentsCompleted(string sessionId, int createdCount, int expectedCount,
            TimeSpan duration)
        {
            Console.WriteLine(
                $" >> {nameof(TrackCreateComponentsCompleted)} {nameof(sessionId)}={sessionId} {nameof(createdCount)}={createdCount} {nameof(expectedCount)}={expectedCount} {nameof(duration)}={duration.TotalMilliseconds}");
        }

        public void TrackCleanUpStarted(string sessionId)
        {
            Console.WriteLine($" >> {nameof(TrackCleanUpStarted)} {nameof(sessionId)}={sessionId}");
        }

        public void TrackCleanUpCompleted(string sessionId, TimeSpan duration)
        {
            Console.WriteLine(
                $" >> {nameof(TrackCleanUpCompleted)} {nameof(sessionId)}={sessionId} {nameof(duration)}={duration.TotalMilliseconds}");
        }

        public void TrackConnectionClosed(ShutdownEventArgs args)
        {
            Console.WriteLine($" >> {nameof(TrackConnectionClosed)} {nameof(args)}={args}");
        }

        public void TrackDisposeStarted()
        {
            Console.WriteLine($" >> {nameof(TrackDisposeStarted)}");
        }

        public void TrackDisposeCompleted(TimeSpan duration)
        {
            Console.WriteLine($" >> {nameof(TrackDisposeCompleted)} {nameof(duration)}={duration.TotalMilliseconds}");
        }

        public void TrackUnexpectedException(string message, Exception ex)
        {
            Console.WriteLine($" >> {nameof(TrackUnexpectedException)} {nameof(message)}={message}");
            Console.WriteLine(ex.Message);
        }
    }

    public class AutoRecoveryDiagnostics : IAutoRecoveryDiagnostics
    {
        public static IAutoRecoveryDiagnostics NoDiagnostics = new AutoRecoveryDiagnostics();

        protected AutoRecoveryDiagnostics()
        {
        }

        public virtual void TrackConnectAttemptStarted(string sessionId, int attempt)
        {
        }

        public virtual void TrackConnectAttemptSucceeded(string sessionId, int attempt, TimeSpan duration)
        {
        }

        public virtual void TrackConnectAttemptFailed(string sessionId, int attempt, TimeSpan duration, Exception ex)
        {
        }

        public virtual void TrackCreateConnectionSucceeded(string sessionId, TimeSpan duration)
        {
        }

        public virtual void TrackCreateConnectionFailed(string sessionId, TimeSpan duration, Exception ex)
        {
        }

        public virtual void TrackCreateComponentsCompleted(string sessionId, int createdCount, int expectedCount,
            TimeSpan duration)
        {
        }

        public virtual void TrackCleanUpStarted(string sessionId)
        {
        }

        public virtual void TrackCleanUpCompleted(string sessionId, TimeSpan duration)
        {
        }

        public virtual void TrackConnectionClosed(ShutdownEventArgs args)
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