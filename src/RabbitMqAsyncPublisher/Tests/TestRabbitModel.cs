using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Tests
{
    internal class TestBasicProperties : IBasicProperties
    {
        ushort IContentHeader.ProtocolClassId { get; }

        string IContentHeader.ProtocolClassName { get; }

        void IBasicProperties.ClearAppId()
        {
            throw new NotImplementedException();
        }

        void IBasicProperties.ClearClusterId()
        {
            throw new NotImplementedException();
        }

        void IBasicProperties.ClearContentEncoding()
        {
            throw new NotImplementedException();
        }

        void IBasicProperties.ClearContentType()
        {
            throw new NotImplementedException();
        }

        void IBasicProperties.ClearCorrelationId()
        {
            throw new NotImplementedException();
        }

        void IBasicProperties.ClearDeliveryMode()
        {
            throw new NotImplementedException();
        }

        void IBasicProperties.ClearExpiration()
        {
            throw new NotImplementedException();
        }

        void IBasicProperties.ClearHeaders()
        {
            throw new NotImplementedException();
        }

        void IBasicProperties.ClearMessageId()
        {
            throw new NotImplementedException();
        }

        void IBasicProperties.ClearPriority()
        {
            throw new NotImplementedException();
        }

        void IBasicProperties.ClearReplyTo()
        {
            throw new NotImplementedException();
        }

        void IBasicProperties.ClearTimestamp()
        {
            throw new NotImplementedException();
        }

        void IBasicProperties.ClearType()
        {
            throw new NotImplementedException();
        }

        void IBasicProperties.ClearUserId()
        {
            throw new NotImplementedException();
        }

        bool IBasicProperties.IsAppIdPresent()
        {
            throw new NotImplementedException();
        }

        bool IBasicProperties.IsClusterIdPresent()
        {
            throw new NotImplementedException();
        }

        bool IBasicProperties.IsContentEncodingPresent()
        {
            throw new NotImplementedException();
        }

        bool IBasicProperties.IsContentTypePresent()
        {
            throw new NotImplementedException();
        }

        bool IBasicProperties.IsCorrelationIdPresent()
        {
            throw new NotImplementedException();
        }

        bool IBasicProperties.IsDeliveryModePresent()
        {
            throw new NotImplementedException();
        }

        bool IBasicProperties.IsExpirationPresent()
        {
            throw new NotImplementedException();
        }

        bool IBasicProperties.IsHeadersPresent()
        {
            throw new NotImplementedException();
        }

        bool IBasicProperties.IsMessageIdPresent()
        {
            throw new NotImplementedException();
        }

        bool IBasicProperties.IsPriorityPresent()
        {
            throw new NotImplementedException();
        }

        bool IBasicProperties.IsReplyToPresent()
        {
            throw new NotImplementedException();
        }

        bool IBasicProperties.IsTimestampPresent()
        {
            throw new NotImplementedException();
        }

        bool IBasicProperties.IsTypePresent()
        {
            throw new NotImplementedException();
        }

        bool IBasicProperties.IsUserIdPresent()
        {
            throw new NotImplementedException();
        }

        string IBasicProperties.AppId { get; set; }

        string IBasicProperties.ClusterId { get; set; }

        string IBasicProperties.ContentEncoding { get; set; }

        string IBasicProperties.ContentType { get; set; }

        string IBasicProperties.CorrelationId { get; set; }

        byte IBasicProperties.DeliveryMode { get; set; }

        string IBasicProperties.Expiration { get; set; }

        IDictionary<string, object> IBasicProperties.Headers { get; set; }

        string IBasicProperties.MessageId { get; set; }

        bool IBasicProperties.Persistent { get; set; }

        byte IBasicProperties.Priority { get; set; }

        string IBasicProperties.ReplyTo { get; set; }

        PublicationAddress IBasicProperties.ReplyToAddress { get; set; }

        AmqpTimestamp IBasicProperties.Timestamp { get; set; }

        string IBasicProperties.Type { get; set; }

        string IBasicProperties.UserId { get; set; }
    }

    internal class PublishRequest
    {
        public IBasicProperties Properties { get; set; }
        public ReadOnlyMemory<byte> Body { get; set; }
        public ulong DeliveryTag { get; set; }
    }

    internal class TestRabbitModel : IModel, IRecoverable
    {
        private readonly Func<PublishRequest, Task<bool>> _handlePublish;
        private readonly object _gate = new object();

        public TestRabbitModel(Func<PublishRequest, Task<bool>> handlePublish)
        {
            _handlePublish = handlePublish;
        }

        public ulong NextPublishSeqNo { get; private set; } = 1;

        public readonly List<(IBasicProperties Properties, ReadOnlyMemory<byte> Body)> PublishCalls =
            new List<(IBasicProperties Properties, ReadOnlyMemory<byte> Body)>();

        public void BasicPublish(string exchange, string routingKey, bool mandatory, IBasicProperties basicProperties,
            ReadOnlyMemory<byte> body)
        {
            PublishRequest request;
            lock (_gate)
            {
                request = new PublishRequest
                {
                    DeliveryTag = NextPublishSeqNo++,
                    Properties = basicProperties,
                    Body = body
                };

                PublishCalls.Add((basicProperties, body));
            }

            var task = _handlePublish(request);
            task.ContinueWith(t =>
            {
                if (t.Result)
                {
                    FireBasicAcks(new BasicAckEventArgs {DeliveryTag = request.DeliveryTag, Multiple = false});
                }
                else
                {
                    FireBasicNacks(new BasicNackEventArgs
                        {DeliveryTag = request.DeliveryTag, Multiple = false, Requeue = false});
                }
            });
        }

        void IDisposable.Dispose()
        {
        }

        void IModel.Abort()
        {
            throw new NotImplementedException();
        }

        void IModel.Abort(ushort replyCode, string replyText)
        {
            throw new NotImplementedException();
        }

        void IModel.BasicAck(ulong deliveryTag, bool multiple)
        {
            throw new NotImplementedException();
        }

        void IModel.BasicCancel(string consumerTag)
        {
            throw new NotImplementedException();
        }

        void IModel.BasicCancelNoWait(string consumerTag)
        {
            throw new NotImplementedException();
        }

        string IModel.BasicConsume(string queue, bool autoAck, string consumerTag, bool noLocal, bool exclusive,
            IDictionary<string, object> arguments,
            IBasicConsumer consumer)
        {
            throw new NotImplementedException();
        }

        BasicGetResult IModel.BasicGet(string queue, bool autoAck)
        {
            throw new NotImplementedException();
        }

        void IModel.BasicNack(ulong deliveryTag, bool multiple, bool requeue)
        {
            throw new NotImplementedException();
        }

        void IModel.BasicQos(uint prefetchSize, ushort prefetchCount, bool global)
        {
            throw new NotImplementedException();
        }

        void IModel.BasicRecover(bool requeue)
        {
            throw new NotImplementedException();
        }

        void IModel.BasicRecoverAsync(bool requeue)
        {
            throw new NotImplementedException();
        }

        void IModel.BasicReject(ulong deliveryTag, bool requeue)
        {
            throw new NotImplementedException();
        }

        void IModel.Close()
        {
            throw new NotImplementedException();
        }

        void IModel.Close(ushort replyCode, string replyText)
        {
            throw new NotImplementedException();
        }

        void IModel.ConfirmSelect()
        {
            throw new NotImplementedException();
        }

        IBasicPublishBatch IModel.CreateBasicPublishBatch()
        {
            throw new NotImplementedException();
        }

        IBasicProperties IModel.CreateBasicProperties()
        {
            throw new NotImplementedException();
        }

        void IModel.ExchangeBind(string destination, string source, string routingKey,
            IDictionary<string, object> arguments)
        {
            throw new NotImplementedException();
        }

        void IModel.ExchangeBindNoWait(string destination, string source, string routingKey,
            IDictionary<string, object> arguments)
        {
            throw new NotImplementedException();
        }

        void IModel.ExchangeDeclare(string exchange, string type, bool durable, bool autoDelete,
            IDictionary<string, object> arguments)
        {
            throw new NotImplementedException();
        }

        void IModel.ExchangeDeclareNoWait(string exchange, string type, bool durable, bool autoDelete,
            IDictionary<string, object> arguments)
        {
            throw new NotImplementedException();
        }

        void IModel.ExchangeDeclarePassive(string exchange)
        {
            throw new NotImplementedException();
        }

        void IModel.ExchangeDelete(string exchange, bool ifUnused)
        {
            throw new NotImplementedException();
        }

        void IModel.ExchangeDeleteNoWait(string exchange, bool ifUnused)
        {
            throw new NotImplementedException();
        }

        void IModel.ExchangeUnbind(string destination, string source, string routingKey,
            IDictionary<string, object> arguments)
        {
            throw new NotImplementedException();
        }

        void IModel.ExchangeUnbindNoWait(string destination, string source, string routingKey,
            IDictionary<string, object> arguments)
        {
            throw new NotImplementedException();
        }

        void IModel.QueueBind(string queue, string exchange, string routingKey, IDictionary<string, object> arguments)
        {
            throw new NotImplementedException();
        }

        void IModel.QueueBindNoWait(string queue, string exchange, string routingKey,
            IDictionary<string, object> arguments)
        {
            throw new NotImplementedException();
        }

        QueueDeclareOk IModel.QueueDeclare(string queue, bool durable, bool exclusive, bool autoDelete,
            IDictionary<string, object> arguments)
        {
            throw new NotImplementedException();
        }

        void IModel.QueueDeclareNoWait(string queue, bool durable, bool exclusive, bool autoDelete,
            IDictionary<string, object> arguments)
        {
            throw new NotImplementedException();
        }

        QueueDeclareOk IModel.QueueDeclarePassive(string queue)
        {
            throw new NotImplementedException();
        }

        uint IModel.MessageCount(string queue)
        {
            throw new NotImplementedException();
        }

        uint IModel.ConsumerCount(string queue)
        {
            throw new NotImplementedException();
        }

        uint IModel.QueueDelete(string queue, bool ifUnused, bool ifEmpty)
        {
            throw new NotImplementedException();
        }

        void IModel.QueueDeleteNoWait(string queue, bool ifUnused, bool ifEmpty)
        {
            throw new NotImplementedException();
        }

        uint IModel.QueuePurge(string queue)
        {
            throw new NotImplementedException();
        }

        void IModel.QueueUnbind(string queue, string exchange, string routingKey, IDictionary<string, object> arguments)
        {
            throw new NotImplementedException();
        }

        void IModel.TxCommit()
        {
            throw new NotImplementedException();
        }

        void IModel.TxRollback()
        {
            throw new NotImplementedException();
        }

        void IModel.TxSelect()
        {
            throw new NotImplementedException();
        }

        bool IModel.WaitForConfirms()
        {
            throw new NotImplementedException();
        }

        bool IModel.WaitForConfirms(TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        bool IModel.WaitForConfirms(TimeSpan timeout, out bool timedOut)
        {
            throw new NotImplementedException();
        }

        void IModel.WaitForConfirmsOrDie()
        {
            throw new NotImplementedException();
        }

        void IModel.WaitForConfirmsOrDie(TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        int IModel.ChannelNumber { get; }

        ShutdownEventArgs IModel.CloseReason { get; }

        IBasicConsumer IModel.DefaultConsumer { get; set; }

        bool IModel.IsClosed { get; }

        bool IModel.IsOpen { get; }

        TimeSpan IModel.ContinuationTimeout { get; set; }

        public event EventHandler<BasicAckEventArgs> BasicAcks;

        public void FireBasicAcks(BasicAckEventArgs args) => BasicAcks?.Invoke(this, args);

        public event EventHandler<BasicNackEventArgs> BasicNacks;

        public void FireBasicNacks(BasicNackEventArgs args) => BasicNacks?.Invoke(this, args);

        public event EventHandler<EventArgs> BasicRecoverOk;

        public void FireBasicRecoverOk(EventArgs args) => BasicRecoverOk?.Invoke(this, args);

        public event EventHandler<BasicReturnEventArgs> BasicReturn;

        public void FireBasicReturn(BasicReturnEventArgs args) => BasicReturn?.Invoke(this, args);

        public event EventHandler<CallbackExceptionEventArgs> CallbackException;

        public void FireCallbackException(CallbackExceptionEventArgs args) => CallbackException?.Invoke(this, args);

        public event EventHandler<FlowControlEventArgs> FlowControl;

        public void FireFlowControl(FlowControlEventArgs args) => FlowControl?.Invoke(this, args);

        public event EventHandler<ShutdownEventArgs> ModelShutdown;

        public void FireModelShutdown(ShutdownEventArgs args) => ModelShutdown?.Invoke(this, args);

        public event EventHandler<EventArgs> Recovery;

        public void FireRecovery(EventArgs args) => Recovery?.Invoke(this, args);
    }
}