using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace RabbitMqAsyncPublisher
{
    public class AsyncPublisher2 : IAsyncPublisherStateContext, IDisposable
    {
        private readonly IModel _model;
        private readonly string _queueName;

        private IAsyncPublisherState _state;

        public AsyncPublisher2(IModel model, string queueName)
        {
            _model = model;
            _queueName = queueName;

            _state = new AsyncPublisherReadyState(this);

            // TODO: Think about _model.BasicRecoverOk event
            // TODO: Think about _model.BasicReturn event

            _model.BasicAcks += OnBasicAcks;
            _model.BasicNacks += OnBasicNacks;
            _model.ModelShutdown += OnModelShutdown;

            if (_model is IRecoverable recoverableModel)
            {
                recoverableModel.Recovery += OnRecovery;
            }
        }

        private void OnBasicAcks(object sender, BasicAckEventArgs args)
        {
            _state.Ack(args.DeliveryTag, args.Multiple);
        }

        private void OnBasicNacks(object sender, BasicNackEventArgs args)
        {
            _state.Nack(args.DeliveryTag, args.Multiple);
        }

        private void OnModelShutdown(object sender, ShutdownEventArgs args)
        {
            _state.Shutdown(args);
        }

        private void OnRecovery(object sender, EventArgs args)
        {
            _state.Recovery();
        }

        public Task<bool> PublishAsync(ReadOnlyMemory<byte> message)
        {
            return _state.PublishAsync(message);
        }

        public void Dispose()
        {
            _state.Dispose();

            _model.BasicAcks -= OnBasicAcks;
            _model.BasicNacks -= OnBasicNacks;
            _model.ModelShutdown -= OnModelShutdown;

            if (_model is IRecoverable recoverableModel)
            {
                recoverableModel.Recovery -= OnRecovery;
            }
        }

        IModel IAsyncPublisherStateContext.Model => _model;

        string IAsyncPublisherStateContext.QueueName => _queueName;

        void IAsyncPublisherStateContext.SetState(IAsyncPublisherState state)
        {
            _state = state;
        }
    }

    internal interface IAsyncPublisherStateContext
    {
        IModel Model { get; }

        string QueueName { get; }

        void SetState(IAsyncPublisherState state);
    }

    internal interface IAsyncPublisherState
    {
        Task<bool> PublishAsync(ReadOnlyMemory<byte> message);

        void Ack(ulong deliveryTag, bool multiple);

        void Nack(ulong deliveryTag, bool multiple);

        void Shutdown(ShutdownEventArgs args);

        void Recovery();

        void Dispose();
    }

    internal class AsyncPublisherReadyState : IAsyncPublisherState
    {
        private readonly IAsyncPublisherStateContext _context;

        private readonly ConcurrentDictionary<ulong, TaskCompletionSource<bool>> _sources =
            new ConcurrentDictionary<ulong, TaskCompletionSource<bool>>();

        private readonly ConcurrentQueue<ulong> _queue = new ConcurrentQueue<ulong>();

        private readonly object _ackSyncRoot = new object();
        private readonly object _publishSyncRoot = new object();

        public AsyncPublisherReadyState(IAsyncPublisherStateContext context)
        {
            _context = context;
        }

        public Task<bool> PublishAsync(ReadOnlyMemory<byte> message)
        {
            lock (_publishSyncRoot)
            {
                var taskCompletionSource = new TaskCompletionSource<bool>();
                var seqNo = _context.Model.NextPublishSeqNo;
                Console.WriteLine($"seqno = {seqNo}");

                Console.WriteLine($"_source.Count = {_sources.Count}; _queue.Count = {_queue.Count}");
                _sources[seqNo] = taskCompletionSource;
                _queue.Enqueue(seqNo);

                _context.Model.BasicPublish("", _context.QueueName, _context.Model.CreateBasicProperties(), message);

                return taskCompletionSource.Task;
            }
        }

        public void Ack(ulong deliveryTag, bool multiple)
        {
            lock (_ackSyncRoot)
            {
                if (multiple)
                {
                    AckMultiple(deliveryTag);
                }
                else
                {
                    Ack(deliveryTag);
                }
            }
        }

        public void Nack(ulong deliveryTag, bool multiple)
        {
            throw new NotImplementedException();
        }

        public void Shutdown(ShutdownEventArgs args)
        {
            lock (_publishSyncRoot)
            lock (_ackSyncRoot)
            {
                while (_queue.TryDequeue(out var seqno))
                {
                    if (_sources.TryRemove(seqno, out var source))
                    {
                        source.TrySetException(new AlreadyClosedException(args));
                    }
                }
            }

            _context.SetState(new AsyncPublisherShutdownState(_context, args));
        }

        public void Recovery()
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }

        private void Ack(ulong deliveryTag)
        {
            if (_queue.TryPeek(out var seqNo) && seqNo == deliveryTag)
            {
                _queue.TryDequeue(out _);
            }

            if (_sources.TryRemove(seqNo, out var source))
            {
                source.SetResult(true);
            }
        }

        private void AckMultiple(ulong deliveryTag)
        {
            while (_queue.TryPeek(out var seqNo) && seqNo <= deliveryTag)
            {
                _queue.TryDequeue(out _);

                if (_sources.TryRemove(seqNo, out var source))
                {
                    source.SetResult(true);
                }
            }
        }
    }

    internal class AsyncPublisherShutdownState : IAsyncPublisherState
    {
        private readonly IAsyncPublisherStateContext _context;
        private readonly ShutdownEventArgs _args;

        public AsyncPublisherShutdownState(IAsyncPublisherStateContext context, ShutdownEventArgs args)
        {
            _context = context;
            _args = args;
        }

        public Task<bool> PublishAsync(ReadOnlyMemory<byte> message)
        {
            return Task.FromException<bool>(new AlreadyClosedException(_args));
        }

        public void Ack(ulong deliveryTag, bool multiple)
        {
        }

        public void Nack(ulong deliveryTag, bool multiple)
        {
        }

        public void Shutdown(ShutdownEventArgs args)
        {
        }

        public void Recovery()
        {
            _context.SetState(new AsyncPublisherReadyState(_context));
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }
    }

    internal class AsyncPublisherDisposeState : IAsyncPublisherState
    {
        public Task<bool> PublishAsync(ReadOnlyMemory<byte> message)
        {
            throw new ObjectDisposedException("Publisher disposed");
        }

        public void Ack(ulong deliveryTag, bool multiple)
        {
            throw new ObjectDisposedException("Publisher disposed");
        }

        public void Nack(ulong deliveryTag, bool multiple)
        {
            throw new ObjectDisposedException("Publisher disposed");
        }

        public void Shutdown(ShutdownEventArgs args)
        {
            throw new ObjectDisposedException("Publisher disposed");
        }

        public void Recovery()
        {
            throw new ObjectDisposedException("Publisher disposed");
        }

        public void Dispose()
        {
            throw new ObjectDisposedException("Publisher disposed");
        }
    }
}