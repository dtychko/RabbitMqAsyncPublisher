using System;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitMqAsyncPublisher
{
    public interface IAsyncPublisherDiagnostics
    {
        void TrackBasicAcksEventProcessing(BasicAckEventArgs args);

        void TrackBasicAcksEventProcessingFailed(BasicAckEventArgs args, TimeSpan duration, Exception ex);

        void TrackBasicAcksEventProcessingCompleted(BasicAckEventArgs args, TimeSpan duration);

        void TrackBasicNacksEventProcessing(BasicNackEventArgs args);

        void TrackBasicNacksEventProcessingFailed(BasicNackEventArgs args, TimeSpan duration, Exception ex);

        void TrackBasicNacksEventProcessingCompleted(BasicNackEventArgs args, TimeSpan duration);

        void TrackModelShutdownEventProcessing(ShutdownEventArgs args);

        void TrackModelShutdownEventProcessingFailed(ShutdownEventArgs args, TimeSpan duration, Exception ex);

        void TrackModelShutdownEventProcessingCompleted(ShutdownEventArgs args, TimeSpan duration);

        void TrackRecoveryEventProcessing();

        void TrackRecoveryEventProcessingFailed(TimeSpan duration, Exception ex);

        void TrackRecoveryEventProcessingCompleted(TimeSpan duration);

        void TrackPublishUnsafe(PublishUnsafeArgs args);

        void TrackPublishUnsafeCanceled(PublishUnsafeArgs args, TimeSpan duration);

        void TrackPublishUnsafeFailed(PublishUnsafeArgs args, TimeSpan duration, Exception ex);

        void TrackPublishUnsafeBasicPublishCompleted(PublishUnsafeArgs args, TimeSpan duration);

        void TrackPublishUnsafeCompleted(PublishUnsafeArgs args, TimeSpan duration, bool acknowledged);

        void TrackCompletionSourceRegistrySize(int size);

        void TrackDispose();

        void TrackDisposeCompleted();
    }

    public interface IAsyncPublisherWithRetriesDiagnostics
    {
        void TrackPublishUnsafeAttempt(PublishUnsafeAttemptArgs args);

        void TrackPublishUnsafeAttemptFailed(PublishUnsafeAttemptArgs args, TimeSpan duration, Exception ex);

        void TrackPublishUnsafeAttemptCompleted(PublishUnsafeAttemptArgs args, TimeSpan duration, bool acknowledged);

        void TrackCanPublishWait(PublishArgs args);

        void TrackRetryDelay(PublishUnsafeAttemptArgs args, TimeSpan delay);
    }

    public class PublishArgs
    {
        public string Exchange { get; }

        public string RoutingKey { get; }

        public ReadOnlyMemory<byte> Body { get; }

        public IBasicProperties Properties { get; }

        public PublishArgs(
            string exchange,
            string routingKey,
            ReadOnlyMemory<byte> body,
            IBasicProperties properties)
        {
            Exchange = exchange;
            RoutingKey = routingKey;
            Body = body;
            Properties = properties;
        }
    }

    public class PublishUnsafeArgs : PublishArgs
    {
        public ulong DeliveryTag { get; }

        public PublishUnsafeArgs(
            string exchange,
            string routingKey,
            ReadOnlyMemory<byte> body,
            IBasicProperties properties,
            ulong deliveryTag)
            : base(exchange, routingKey, body, properties)
        {
            DeliveryTag = deliveryTag;
        }
    }

    public class PublishUnsafeAttemptArgs : PublishArgs
    {
        public int Attempt { get; }

        public PublishUnsafeAttemptArgs(
            string exchange,
            string routingKey,
            ReadOnlyMemory<byte> body,
            IBasicProperties properties,
            int attempt)
            : base(exchange, routingKey, body, properties)
        {
            Attempt = attempt;
        }
    }

    public class ConsoleAsyncPublisherWithRetriesDiagnostics : IAsyncPublisherWithRetriesDiagnostics
    {
        public void TrackPublishUnsafeAttemptFailed(PublishUnsafeAttemptArgs args, TimeSpan duration,
            Exception ex)
        {
            Console.WriteLine($" >> WithRetries/{nameof(TrackPublishUnsafeAttemptFailed)}/{args.Attempt}/error {duration.TotalMilliseconds}");
        }

        public void TrackPublishUnsafeAttemptCompleted(PublishUnsafeAttemptArgs args, TimeSpan duration,
            bool acknowledged)
        {
            Console.WriteLine($" >> WithRetries/{nameof(TrackPublishUnsafeAttemptCompleted)}/{args.Attempt}/completed {duration.TotalMilliseconds}");
        }

        public void TrackCanPublishWait(PublishArgs args)
        {
            Console.WriteLine($" >> WithRetries/{nameof(TrackCanPublishWait)}");
        }

        public void TrackRetryDelay(PublishUnsafeAttemptArgs args, TimeSpan delay)
        {
            Console.WriteLine($" >> WithRetries/{nameof(TrackRetryDelay)}/{args.Attempt}: {delay.TotalMilliseconds}ms");
        }

        public void TrackPublishUnsafeAttempt(PublishUnsafeAttemptArgs args)
        {
            Console.WriteLine($" >> WithRetries/{nameof(TrackPublishUnsafeAttempt)}/{args.Attempt}/started");
        }

    }

    public class EmptyDiagnostics : IAsyncPublisherDiagnostics, IAsyncPublisherWithRetriesDiagnostics
    {
        public static readonly EmptyDiagnostics Instance = new EmptyDiagnostics();

        protected EmptyDiagnostics()
        {
        }

        public virtual void TrackBasicAcksEventProcessing(BasicAckEventArgs args)
        {
        }

        public virtual void TrackBasicAcksEventProcessingFailed(BasicAckEventArgs args, TimeSpan duration, Exception ex)
        {
        }

        public virtual void TrackBasicAcksEventProcessingCompleted(BasicAckEventArgs args, TimeSpan duration)
        {
        }

        public virtual void TrackBasicNacksEventProcessing(BasicNackEventArgs args)
        {
        }

        public virtual void TrackBasicNacksEventProcessingFailed(BasicNackEventArgs args, TimeSpan duration,
            Exception ex)
        {
        }

        public virtual void TrackBasicNacksEventProcessingCompleted(BasicNackEventArgs args, TimeSpan duration)
        {
        }

        public virtual void TrackModelShutdownEventProcessing(ShutdownEventArgs args)
        {
        }

        public virtual void TrackModelShutdownEventProcessingFailed(ShutdownEventArgs args, TimeSpan duration,
            Exception ex)
        {
        }

        public virtual void TrackModelShutdownEventProcessingCompleted(ShutdownEventArgs args, TimeSpan duration)
        {
        }

        public virtual void TrackRecoveryEventProcessing()
        {
        }

        public virtual void TrackRecoveryEventProcessingFailed(TimeSpan duration, Exception ex)
        {
        }

        public virtual void TrackRecoveryEventProcessingCompleted(TimeSpan duration)
        {
        }

        public virtual void TrackPublishUnsafe(PublishUnsafeArgs args)
        {
        }

        public virtual void TrackPublishUnsafeCanceled(PublishUnsafeArgs args, TimeSpan duration)
        {
        }

        public virtual void TrackPublishUnsafeFailed(PublishUnsafeArgs args, TimeSpan duration, Exception ex)
        {
        }

        public virtual void TrackPublishUnsafeBasicPublishCompleted(PublishUnsafeArgs args, TimeSpan duration)
        {
        }

        public virtual void TrackPublishUnsafeCompleted(PublishUnsafeArgs args, TimeSpan duration, bool acknowledged)
        {
        }

        public void TrackCompletionSourceRegistrySize(int size)
        {
        }

        public void TrackDispose()
        {
        }

        public void TrackDisposeCompleted()
        {
        }

        public virtual void TrackPublishUnsafeAttempt(PublishUnsafeAttemptArgs args)
        {
        }

        public virtual void TrackPublishUnsafeAttemptFailed(PublishUnsafeAttemptArgs args, TimeSpan duration,
            Exception ex)
        {
        }

        public virtual void TrackPublishUnsafeAttemptCompleted(PublishUnsafeAttemptArgs args, TimeSpan duration,
            bool acknowledged)
        {
        }

        public virtual void TrackCanPublishWait(PublishArgs args)
        {
        }

        public virtual void TrackRetryDelay(PublishUnsafeAttemptArgs args, TimeSpan delay)
        {
        }
    }

    public static class DiagnosticsUtils
    {
        public static void TrackSafe<T1>(Action<T1> track, T1 arg1)
        {
            TrackSafe(() => track(arg1));
        }

        public static void TrackSafe<T1, T2>(Action<T1, T2> track, T1 arg1, T2 arg2)
        {
            TrackSafe(() => track(arg1, arg2));
        }

        public static void TrackSafe<T1, T2, T3>(Action<T1, T2, T3> track, T1 arg1, T2 arg2, T3 arg3)
        {
            TrackSafe(() => track(arg1, arg2, arg3));
        }

        public static void TrackSafe<T1, T2, T3, T4>(Action<T1, T2, T3, T4> track, T1 arg1, T2 arg2, T3 arg3, T4 arg4)
        {
            TrackSafe(() => track(arg1, arg2, arg3, arg4));
        }

        public static void TrackSafe(Action track)
        {
            try
            {
                track();
            }
            catch
            {
                // Ignore all exceptions during tracking diagnostics data 
            }
        }
    }
}