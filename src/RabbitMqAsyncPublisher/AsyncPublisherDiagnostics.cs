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

        void TrackUnsupportedSignal(string state, string signal);

        void TrackPublishUnsafe(PublishUnsafeArgs args);

        void TrackPublishUnsafeCanceled(PublishUnsafeArgs args, TimeSpan duration);

        void TrackPublishUnsafeFailed(PublishUnsafeArgs args, TimeSpan duration, Exception ex);

        void TrackPublishUnsafePublished(PublishUnsafeArgs args, TimeSpan duration);

        void TrackPublishUnsafeCompleted(PublishUnsafeArgs args, TimeSpan duration, bool acknowledged);
    }

    public class PublishUnsafeArgs
    {
        public string Exchange { get; }

        public string RoutingKey { get; }

        public ReadOnlyMemory<byte> Body { get; }

        public IBasicProperties Properties { get; }

        public ulong DeliveryTag { get; }
        
        public PublishUnsafeArgs(
            string exchange,
            string routingKey,
            ReadOnlyMemory<byte> body,
            IBasicProperties properties,
            ulong deliveryTag)
        {
            Exchange = exchange;
            RoutingKey = routingKey;
            Body = body;
            Properties = properties;
            DeliveryTag = deliveryTag;
        }
    }

    internal class EmptyDiagnostics : IAsyncPublisherDiagnostics
    {
        public static readonly EmptyDiagnostics Instance = new EmptyDiagnostics();

        private EmptyDiagnostics()
        {
        }

        public void TrackBasicAcksEventProcessing(BasicAckEventArgs args)
        {
        }

        public void TrackBasicAcksEventProcessingFailed(BasicAckEventArgs args, TimeSpan duration, Exception ex)
        {
        }

        public void TrackBasicAcksEventProcessingCompleted(BasicAckEventArgs args, TimeSpan duration)
        {
        }

        public void TrackBasicNacksEventProcessing(BasicNackEventArgs args)
        {
        }

        public void TrackBasicNacksEventProcessingFailed(BasicNackEventArgs args, TimeSpan duration, Exception ex)
        {
        }

        public void TrackBasicNacksEventProcessingCompleted(BasicNackEventArgs args, TimeSpan duration)
        {
        }

        public void TrackModelShutdownEventProcessing(ShutdownEventArgs args)
        {
        }

        public void TrackModelShutdownEventProcessingFailed(ShutdownEventArgs args, TimeSpan duration, Exception ex)
        {
        }

        public void TrackModelShutdownEventProcessingCompleted(ShutdownEventArgs args, TimeSpan duration)
        {
        }

        public void TrackRecoveryEventProcessing()
        {
        }

        public void TrackRecoveryEventProcessingFailed(TimeSpan duration, Exception ex)
        {
        }

        public void TrackRecoveryEventProcessingCompleted(TimeSpan duration)
        {
        }

        public void TrackUnsupportedSignal(string state, string signal)
        {
        }

        public void TrackPublishUnsafe(PublishUnsafeArgs args)
        {
        }

        public void TrackPublishUnsafeCanceled(PublishUnsafeArgs args, TimeSpan duration)
        {
        }

        public void TrackPublishUnsafeFailed(PublishUnsafeArgs args, TimeSpan duration, Exception ex)
        {
        }

        public void TrackPublishUnsafePublished(PublishUnsafeArgs args, TimeSpan duration)
        {
        }

        public void TrackPublishUnsafeCompleted(PublishUnsafeArgs args, TimeSpan duration, bool acknowledged)
        {
        }
    }
}