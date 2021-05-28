using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace Tests
{
    internal class PublishRequest
    {
        public IBasicProperties Properties { get; set; }
        public ReadOnlyMemory<byte> Body { get; set; }
        public ulong DeliveryTag { get; set; }
    }

    internal class TestNonRecoverableRabbitModel : IModel
    {
        private readonly Func<PublishRequest, Task<bool>> _handlePublish;
        private readonly object _gate = new object();

        public TestNonRecoverableRabbitModel(Func<PublishRequest, Task<bool>> handlePublish)
        {
            _handlePublish = handlePublish;
        }

        public ulong NextPublishSeqNo { get; private set; } = 1;

        public readonly List<PublishRequest> PublishCalls = new List<PublishRequest>();

        public readonly ConcurrentBag<PublishRequest> SuccessfullyCompletedPublishes =
            new ConcurrentBag<PublishRequest>();

        public void BasicPublish(string exchange, string routingKey, bool mandatory, IBasicProperties basicProperties,
            ReadOnlyMemory<byte> body)
        {
            // Console.WriteLine("test-model/basic-publish/starting");

            PublishRequest request;
            lock (_gate)
            {
                request = new PublishRequest
                {
                    DeliveryTag = NextPublishSeqNo++,
                    Properties = basicProperties,
                    Body = body
                };

                PublishCalls.Add(request);
            }

            var shutdown = _currentShutdown;
            if (shutdown != null)
            {
                Console.WriteLine("test-model/basic-publish/already-closed");
                throw new AlreadyClosedException(shutdown);
            }

            var task = _handlePublish(request);
            task.ContinueWith(t =>
            {
                lock (_eventSyncRoot)
                {
                    if (_currentShutdown != null)
                    {
                        return;
                    }

                    if (t.Result)
                    {
                        FireBasicAcks(new BasicAckEventArgs {DeliveryTag = request.DeliveryTag, Multiple = false});
                        SuccessfullyCompletedPublishes.Add(request);
                    }
                    else
                    {
                        FireBasicNacks(new BasicNackEventArgs
                            {DeliveryTag = request.DeliveryTag, Multiple = false, Requeue = false});
                    }
                }
            });
        }

        private volatile ShutdownEventArgs _currentShutdown;

        public event EventHandler<ShutdownEventArgs> ModelShutdown;

        private readonly object _eventSyncRoot = new object();

        public void FireModelShutdown(ShutdownEventArgs args)
        {
            lock (_eventSyncRoot)
            {
                _currentShutdown = args;
                Console.WriteLine("Test model moved to Shutdown state");
                ModelShutdown?.Invoke(this, args);
            }
        }

        public ShutdownEventArgs CloseReason => _currentShutdown;

        public event EventHandler<BasicAckEventArgs> BasicAcks;

        public void FireBasicAcks(BasicAckEventArgs args)
        {
            lock (_eventSyncRoot)
            {
                BasicAcks?.Invoke(this, args);
            }
        }

        public event EventHandler<BasicNackEventArgs> BasicNacks;

        public void FireBasicNacks(BasicNackEventArgs args)
        {
            lock (_eventSyncRoot)
            {
                BasicNacks?.Invoke(this, args);
            }
        }

        void IDisposable.Dispose()
        {
        }

        #region Unused in tests

        void IModel.Abort() => throw new NotImplementedException();

        void IModel.Abort(ushort replyCode, string replyText) => throw new NotImplementedException();

        void IModel.BasicAck(ulong deliveryTag, bool multiple) => throw new NotImplementedException();

        void IModel.BasicCancel(string consumerTag) => throw new NotImplementedException();

        void IModel.BasicCancelNoWait(string consumerTag) => throw new NotImplementedException();

        string IModel.BasicConsume(string queue, bool autoAck, string consumerTag, bool noLocal, bool exclusive,
            IDictionary<string, object> arguments,
            IBasicConsumer consumer)
        {
            throw new NotImplementedException();
        }

        BasicGetResult IModel.BasicGet(string queue, bool autoAck) => throw new NotImplementedException();

        void IModel.BasicNack(ulong deliveryTag, bool multiple, bool requeue) => throw new NotImplementedException();

        void IModel.BasicQos(uint prefetchSize, ushort prefetchCount, bool global) =>
            throw new NotImplementedException();

        void IModel.BasicRecover(bool requeue) => throw new NotImplementedException();

        void IModel.BasicRecoverAsync(bool requeue) => throw new NotImplementedException();

        void IModel.BasicReject(ulong deliveryTag, bool requeue) => throw new NotImplementedException();

        void IModel.Close() => throw new NotImplementedException();

        void IModel.Close(ushort replyCode, string replyText) => throw new NotImplementedException();

        void IModel.ConfirmSelect() => throw new NotImplementedException();

        IBasicPublishBatch IModel.CreateBasicPublishBatch() => throw new NotImplementedException();

        IBasicProperties IModel.CreateBasicProperties() => new TestBasicProperties();

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

        void IModel.ExchangeDeclarePassive(string exchange) => throw new NotImplementedException();

        void IModel.ExchangeDelete(string exchange, bool ifUnused) => throw new NotImplementedException();

        void IModel.ExchangeDeleteNoWait(string exchange, bool ifUnused) => throw new NotImplementedException();

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

        QueueDeclareOk IModel.QueueDeclarePassive(string queue) => throw new NotImplementedException();

        uint IModel.MessageCount(string queue) => throw new NotImplementedException();

        uint IModel.ConsumerCount(string queue) => throw new NotImplementedException();

        uint IModel.QueueDelete(string queue, bool ifUnused, bool ifEmpty) => throw new NotImplementedException();

        void IModel.QueueDeleteNoWait(string queue, bool ifUnused, bool ifEmpty) => throw new NotImplementedException();

        uint IModel.QueuePurge(string queue) => throw new NotImplementedException();

        void IModel.QueueUnbind(string queue, string exchange, string routingKey, IDictionary<string, object> arguments)
        {
            throw new NotImplementedException();
        }

        void IModel.TxCommit() => throw new NotImplementedException();

        void IModel.TxRollback() => throw new NotImplementedException();

        void IModel.TxSelect() => throw new NotImplementedException();

        bool IModel.WaitForConfirms() => throw new NotImplementedException();

        bool IModel.WaitForConfirms(TimeSpan timeout) => throw new NotImplementedException();

        bool IModel.WaitForConfirms(TimeSpan timeout, out bool timedOut) => throw new NotImplementedException();

        void IModel.WaitForConfirmsOrDie() => throw new NotImplementedException();

        void IModel.WaitForConfirmsOrDie(TimeSpan timeout) => throw new NotImplementedException();

        int IModel.ChannelNumber => throw new NotImplementedException();

        IBasicConsumer IModel.DefaultConsumer
        {
            get => throw new NotImplementedException();
            set => throw new NotImplementedException();
        }

        bool IModel.IsClosed => throw new NotImplementedException();

        bool IModel.IsOpen => throw new NotImplementedException();

        TimeSpan IModel.ContinuationTimeout
        {
            get => throw new NotImplementedException();
            set => throw new NotImplementedException();
        }

        event EventHandler<EventArgs> IModel.BasicRecoverOk
        {
            add => throw new NotImplementedException();
            remove => throw new NotImplementedException();
        }

        event EventHandler<BasicReturnEventArgs> IModel.BasicReturn
        {
            add => throw new NotImplementedException();
            remove => throw new NotImplementedException();
        }

        event EventHandler<CallbackExceptionEventArgs> IModel.CallbackException
        {
            add => throw new NotImplementedException();
            remove => throw new NotImplementedException();
        }

        event EventHandler<FlowControlEventArgs> IModel.FlowControl
        {
            add => throw new NotImplementedException();
            remove => throw new NotImplementedException();
        }

        #endregion
    }

    internal class TestBasicProperties : IBasicProperties
    {
        public ushort ProtocolClassId { get; set; }
        public string ProtocolClassName { get; set; }
        public void ClearAppId()
        {
            throw new NotImplementedException();
        }

        public void ClearClusterId()
        {
            throw new NotImplementedException();
        }

        public void ClearContentEncoding()
        {
            throw new NotImplementedException();
        }

        public void ClearContentType()
        {
            throw new NotImplementedException();
        }

        public void ClearCorrelationId()
        {
            throw new NotImplementedException();
        }

        public void ClearDeliveryMode()
        {
            throw new NotImplementedException();
        }

        public void ClearExpiration()
        {
            throw new NotImplementedException();
        }

        public void ClearHeaders()
        {
            throw new NotImplementedException();
        }

        public void ClearMessageId()
        {
            throw new NotImplementedException();
        }

        public void ClearPriority()
        {
            throw new NotImplementedException();
        }

        public void ClearReplyTo()
        {
            throw new NotImplementedException();
        }

        public void ClearTimestamp()
        {
            throw new NotImplementedException();
        }

        public void ClearType()
        {
            throw new NotImplementedException();
        }

        public void ClearUserId()
        {
            throw new NotImplementedException();
        }

        public bool IsAppIdPresent()
        {
            throw new NotImplementedException();
        }

        public bool IsClusterIdPresent()
        {
            throw new NotImplementedException();
        }

        public bool IsContentEncodingPresent()
        {
            throw new NotImplementedException();
        }

        public bool IsContentTypePresent()
        {
            throw new NotImplementedException();
        }

        public bool IsCorrelationIdPresent()
        {
            throw new NotImplementedException();
        }

        public bool IsDeliveryModePresent()
        {
            throw new NotImplementedException();
        }

        public bool IsExpirationPresent()
        {
            throw new NotImplementedException();
        }

        public bool IsHeadersPresent()
        {
            throw new NotImplementedException();
        }

        public bool IsMessageIdPresent()
        {
            throw new NotImplementedException();
        }

        public bool IsPriorityPresent()
        {
            throw new NotImplementedException();
        }

        public bool IsReplyToPresent()
        {
            throw new NotImplementedException();
        }

        public bool IsTimestampPresent()
        {
            throw new NotImplementedException();
        }

        public bool IsTypePresent()
        {
            throw new NotImplementedException();
        }

        public bool IsUserIdPresent()
        {
            throw new NotImplementedException();
        }

        public string AppId { get; set; }
        public string ClusterId { get; set; }
        public string ContentEncoding { get; set; }
        public string ContentType { get; set; }
        public string CorrelationId { get; set; }
        public byte DeliveryMode { get; set; }
        public string Expiration { get; set; }
        public IDictionary<string, object> Headers { get; set; }
        public string MessageId { get; set; }
        public bool Persistent { get; set; }
        public byte Priority { get; set; }
        public string ReplyTo { get; set; }
        public PublicationAddress ReplyToAddress { get; set; }
        public AmqpTimestamp Timestamp { get; set; }
        public string Type { get; set; }
        public string UserId { get; set; }
    }
}