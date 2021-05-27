using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace Tests
{
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

        public readonly ConcurrentBag<PublishRequest> SuccessfullyCompletedPublishes = new ConcurrentBag<PublishRequest>();

        public void BasicPublish(string exchange, string routingKey, bool mandatory, IBasicProperties basicProperties,
            ReadOnlyMemory<byte> body)
        {
            Console.WriteLine("test-model/basic-publish/starting");

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

        IBasicProperties IModel.CreateBasicProperties() => throw new NotImplementedException();

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
}