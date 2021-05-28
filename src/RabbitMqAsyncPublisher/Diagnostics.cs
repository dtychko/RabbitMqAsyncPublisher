using System;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitMqAsyncPublisher
{
    public interface IAsyncPublisherWithRetriesDiagnostics
    {
        void TrackPublishUnsafeAttempt(PublishUnsafeAttemptArgs args);

        void TrackPublishUnsafeAttemptFailed(PublishUnsafeAttemptArgs args, TimeSpan duration, Exception ex);

        void TrackPublishUnsafeAttemptCompleted(PublishUnsafeAttemptArgs args, TimeSpan duration, bool acknowledged);

        void TrackCanPublishWait(PublishArgs args);

        void TrackRetryDelay(PublishUnsafeAttemptArgs args, TimeSpan delay);
    }

    public readonly struct PublishUnsafeArgs
    {
        public readonly string Exchange;
        public readonly string RoutingKey;
        public readonly ReadOnlyMemory<byte> Body;
        public readonly IBasicProperties Properties;
        public readonly ulong DeliveryTag;

        public PublishUnsafeArgs(string exchange, string routingKey, ReadOnlyMemory<byte> body,
            IBasicProperties properties, ulong deliveryTag)
        {
            Exchange = exchange;
            RoutingKey = routingKey;
            Body = body;
            Properties = properties;
            DeliveryTag = deliveryTag;
        }
    }

    public readonly struct PublishUnsafeAttemptArgs
    {
        public readonly string Exchange;
        public readonly string RoutingKey;
        public readonly ReadOnlyMemory<byte> Body;
        public readonly MessageProperties Properties;
        public readonly int Attempt;

        public PublishUnsafeAttemptArgs(string exchange, string routingKey, ReadOnlyMemory<byte> body,
            MessageProperties properties, int attempt)
        {
            Exchange = exchange;
            RoutingKey = routingKey;
            Body = body;
            Properties = properties;
            Attempt = attempt;
        }
    }

    public class AsyncPublisherWithRetriesConsoleDiagnostics : IAsyncPublisherWithRetriesDiagnostics
    {
        public void TrackPublishUnsafeAttemptFailed(PublishUnsafeAttemptArgs args, TimeSpan duration,
            Exception ex)
        {
            Console.WriteLine(
                $" >> WithRetries/{nameof(TrackPublishUnsafeAttemptFailed)}/{args.Attempt}/error {duration.TotalMilliseconds}");
        }

        public void TrackPublishUnsafeAttemptCompleted(PublishUnsafeAttemptArgs args, TimeSpan duration,
            bool acknowledged)
        {
            Console.WriteLine(
                $" >> WithRetries/{nameof(TrackPublishUnsafeAttemptCompleted)}/{args.Attempt}/completed {duration.TotalMilliseconds}");
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

    public class EmptyDiagnostics : IAsyncPublisherWithRetriesDiagnostics
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
}