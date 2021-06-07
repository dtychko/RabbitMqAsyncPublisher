using System;

namespace RabbitMqAsyncPublisher
{
    public interface IBalancedProcessorDiagnostics<TValue> : IUnexpectedExceptionDiagnostics
    {
        void TrackProcessJobEnqueued(ProcessArgs<TValue> processArgs, BalancedProcessorStatus status);

        void TrackProcessJobStarted(ProcessArgs<TValue> processArgs, BalancedProcessorStatus status);

        void TrackProcessJobCompleted(ProcessArgs<TValue> processArgs, BalancedProcessorStatus status,
            TimeSpan duration);

        void TrackProcessJobCancelled(ProcessArgs<TValue> processArgs, BalancedProcessorStatus status,
            TimeSpan duration);

        void TrackProcessJobFailed(ProcessArgs<TValue> processArgs, BalancedProcessorStatus status, TimeSpan duration,
            Exception ex);

        void TrackDisposeStarted(BalancedProcessorStatus status);

        void TrackDisposeCompleted(BalancedProcessorStatus status, TimeSpan duration);
    }

    public class BalancedProcessorDiagnostics<TValue> : IBalancedProcessorDiagnostics<TValue>
    {
        public static readonly IBalancedProcessorDiagnostics<TValue> NoDiagnostics =
            new BalancedProcessorDiagnostics<TValue>();

        protected BalancedProcessorDiagnostics()
        {
        }

        public virtual void TrackUnexpectedException(string message, Exception ex)
        {
        }

        public virtual void TrackProcessJobEnqueued(ProcessArgs<TValue> processArgs, BalancedProcessorStatus status)
        {
        }

        public virtual void TrackProcessJobStarted(ProcessArgs<TValue> processArgs, BalancedProcessorStatus status)
        {
        }

        public virtual void TrackProcessJobCompleted(ProcessArgs<TValue> processArgs, BalancedProcessorStatus status,
            TimeSpan duration)
        {
        }

        public virtual void TrackProcessJobCancelled(ProcessArgs<TValue> processArgs, BalancedProcessorStatus status,
            TimeSpan duration)
        {
        }

        public virtual void TrackProcessJobFailed(ProcessArgs<TValue> processArgs, BalancedProcessorStatus status,
            TimeSpan duration, Exception ex)
        {
        }

        public virtual void TrackDisposeStarted(BalancedProcessorStatus status)
        {
        }

        public virtual void TrackDisposeCompleted(BalancedProcessorStatus status, TimeSpan duration)
        {
        }
    }
}