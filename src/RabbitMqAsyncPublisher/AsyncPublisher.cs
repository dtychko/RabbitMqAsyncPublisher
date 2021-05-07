using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace RabbitMqAsyncPublisher
{
    public interface IModelAware
    {
        IModel Model { get; }
    }

    public interface IAsyncPublisher<TResult> : IModelAware, IDisposable
    {
        Task<TResult> PublishAsync(
            string exchange,
            string routingKey,
            ReadOnlyMemory<byte> body,
            CancellationToken cancellationToken);
    }

    /// <summary>
    /// Limitations:
    /// 1. Doesn't listen to "IModel.BasicReturn" event, as results doesn't handle "returned" messages
    /// </summary>
    public class AsyncPublisher : IAsyncPublisher<bool>, IAsyncPublisherStateContext
    {
        private readonly object _stateSyncRoot = new object();
        private IAsyncPublisherState _state;
        private int _disposed;

        public IModel Model { get; }

        public AsyncPublisher(IModel model)
        {
            Model = model;

            _state = new AsyncPublisherOpenState(this);

            Model.BasicAcks += OnBasicAcks;
            Model.BasicNacks += OnBasicNacks;
            Model.ModelShutdown += OnModelShutdown;

            if (Model is IRecoverable recoverableModel)
            {
                recoverableModel.Recovery += OnRecovery;
            }
        }

        private void OnBasicAcks(object sender, BasicAckEventArgs args)
        {
            SyncStateAccess(state => state.Ack(args.DeliveryTag, args.Multiple));
        }

        private void OnBasicNacks(object sender, BasicNackEventArgs args)
        {
            SyncStateAccess(state => state.Nack(args.DeliveryTag, args.Multiple));
        }

        private void OnModelShutdown(object sender, ShutdownEventArgs args)
        {
            SyncStateAccess(state => state.Shutdown(args));
        }

        private void OnRecovery(object sender, EventArgs args)
        {
            SyncStateAccess(state => state.Recovery());
        }

        public Task<bool> PublishAsync(
            string exchange,
            string routingKey,
            ReadOnlyMemory<byte> body,
            CancellationToken cancellationToken)
        {
            // TODO: Try to publish without lock. Publish method access could be synced in decorator
            return SyncStateAccess(state => state.PublishAsync(exchange, routingKey, body, cancellationToken));
        }

        public void Dispose()
        {
            var disposer = Interlocked.Exchange(ref _disposed, 1) == 0;

            // Make sure that after calling "Dispose" method publisher is always moved to "Disposed" state,
            // even if the current call wasn't chosen to be "disposer".
            SyncStateAccess(state => state.Dispose());

            if (!disposer)
            {
                return;
            }

            Model.BasicAcks -= OnBasicAcks;
            Model.BasicNacks -= OnBasicNacks;
            Model.ModelShutdown -= OnModelShutdown;

            if (Model is IRecoverable recoverableModel)
            {
                recoverableModel.Recovery -= OnRecovery;
            }
        }

        private void SyncStateAccess(Action<IAsyncPublisherState> access)
        {
            lock (_stateSyncRoot)
            {
                access(_state);
            }
        }

        private TResult SyncStateAccess<TResult>(Func<IAsyncPublisherState, TResult> access)
        {
            lock (_stateSyncRoot)
            {
                return access(_state);
            }
        }

        void IAsyncPublisherStateContext.SetState(IAsyncPublisherState state)
        {
            _state = state;
        }
    }

    internal interface IAsyncPublisherStateContext
    {
        IModel Model { get; }

        void SetState(IAsyncPublisherState state);
    }

    internal interface IAsyncPublisherState
    {
        Task<bool> PublishAsync(
            string exchange,
            string routingKey,
            ReadOnlyMemory<byte> body,
            CancellationToken cancellationToken);

        void Ack(ulong deliveryTag, bool multiple);

        void Nack(ulong deliveryTag, bool multiple);

        void Shutdown(ShutdownEventArgs args);

        void Recovery();

        void Dispose();
    }

    internal class AsyncPublisherTaskRegistry
    {
        private readonly Dictionary<ulong, SourceEntry> _sources = new Dictionary<ulong, SourceEntry>();
        private readonly LinkedList<ulong> _deliveryTagQueue = new LinkedList<ulong>();

        public Task<bool> Register(ulong deliveryTag, CancellationToken cancellationToken)
        {
            var taskCompletionSource = new TaskCompletionSource<bool>();
            var cancellationTokenRegistration =
                cancellationToken.Register(() => taskCompletionSource.TrySetCanceled(cancellationToken));

            // ReSharper disable once MethodSupportsCancellation
            taskCompletionSource.Task.ContinueWith(_ => cancellationTokenRegistration.Dispose());

            _sources[deliveryTag] = new SourceEntry(taskCompletionSource, _deliveryTagQueue.AddLast(deliveryTag));

            return taskCompletionSource.Task;
        }

        public void SetResult(ulong deliveryTag, bool result)
        {
            if (_sources.TryGetValue(deliveryTag, out var entry))
            {
                _sources.Remove(deliveryTag);
                _deliveryTagQueue.Remove(entry.QueueNode);
                entry.Source.TrySetResult(result);
            }
        }

        public void SetResultForAllUpTo(ulong deliveryTag, bool result)
        {
            foreach (var source in GetAllUpTo(deliveryTag))
            {
                source.TrySetResult(result);
            }
        }

        public void SetExceptionForAll(Exception exception)
        {
            foreach (var source in GetAllUpTo(ulong.MaxValue))
            {
                source.TrySetException(exception);
            }
        }

        private IEnumerable<TaskCompletionSource<bool>> GetAllUpTo(ulong deliveryTag)
        {
            while (_deliveryTagQueue.Count > 0 && _deliveryTagQueue.First.Value <= deliveryTag)
            {
                if (_sources.TryGetValue(_deliveryTagQueue.First.Value, out var entry))
                {
                    _sources.Remove(_deliveryTagQueue.First.Value);
                    yield return entry.Source;
                }

                _deliveryTagQueue.RemoveFirst();
            }
        }

        private readonly struct SourceEntry
        {
            public TaskCompletionSource<bool> Source { get; }

            public LinkedListNode<ulong> QueueNode { get; }

            public SourceEntry(TaskCompletionSource<bool> source, LinkedListNode<ulong> queueNode)
            {
                Source = source;
                QueueNode = queueNode;
            }
        }
    }

    internal class AsyncPublisherOpenState : IAsyncPublisherState
    {
        private readonly AsyncPublisherTaskRegistry _taskRegistry = new AsyncPublisherTaskRegistry();
        private readonly IAsyncPublisherStateContext _context;

        public AsyncPublisherOpenState(IAsyncPublisherStateContext context)
        {
            _context = context;
        }

        public Task<bool> PublishAsync(
            string exchange,
            string routingKey,
            ReadOnlyMemory<byte> body,
            CancellationToken cancellationToken)
        {
            var seqNo = _context.Model.NextPublishSeqNo;
            cancellationToken.ThrowIfCancellationRequested();
            _context.Model.BasicPublish(exchange, routingKey, _context.Model.CreateBasicProperties(), body);

            return _taskRegistry.Register(seqNo, cancellationToken);
        }

        public void Ack(ulong deliveryTag, bool multiple)
        {
            ProcessDeliveryTag(deliveryTag, multiple, true);
        }

        public void Nack(ulong deliveryTag, bool multiple)
        {
            ProcessDeliveryTag(deliveryTag, multiple, false);
        }

        public void Shutdown(ShutdownEventArgs args)
        {
            _taskRegistry.SetExceptionForAll(new AlreadyClosedException(args));
            _context.SetState(new AsyncPublisherClosedState(_context, args));
        }

        public void Recovery()
        {
            // TODO: Make sure that Recovery method couldn't be executed for "Open" state 
            // TODO: Perhaps log unexpected command
        }

        public void Dispose()
        {
            _taskRegistry.SetExceptionForAll(new ObjectDisposedException("Publisher was disposed"));
            _context.SetState(new AsyncPublisherDisposedState());
        }

        private void ProcessDeliveryTag(ulong deliveryTag, bool multiple, bool ack)
        {
            if (!multiple)
            {
                _taskRegistry.SetResult(deliveryTag, ack);
                return;
            }

            _taskRegistry.SetResultForAllUpTo(deliveryTag, ack);
        }
    }

    internal class AsyncPublisherClosedState : IAsyncPublisherState
    {
        private readonly IAsyncPublisherStateContext _context;
        private readonly ShutdownEventArgs _args;

        public AsyncPublisherClosedState(IAsyncPublisherStateContext context, ShutdownEventArgs args)
        {
            _context = context;
            _args = args;
        }

        public Task<bool> PublishAsync(
            string exchange,
            string routingKey,
            ReadOnlyMemory<byte> body,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            throw new AlreadyClosedException(_args);
        }

        public void Ack(ulong deliveryTag, bool multiple)
        {
            // TODO: Log unexpected command
        }

        public void Nack(ulong deliveryTag, bool multiple)
        {
            // TODO: Log unexpected command
        }

        public void Shutdown(ShutdownEventArgs args)
        {
            // TODO: Log unexpected command
        }

        public void Recovery()
        {
            _context.SetState(new AsyncPublisherOpenState(_context));
        }

        public void Dispose()
        {
            _context.SetState(new AsyncPublisherDisposedState());
        }
    }

    internal class AsyncPublisherDisposedState : IAsyncPublisherState
    {
        public Task<bool> PublishAsync(
            string exchange,
            string routingKey,
            ReadOnlyMemory<byte> body,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            throw new ObjectDisposedException("Publisher was disposed");
        }

        public void Ack(ulong deliveryTag, bool multiple)
        {
            // TODO: Log unexpected command
        }

        public void Nack(ulong deliveryTag, bool multiple)
        {
            // TODO: Log unexpected command
        }

        public void Shutdown(ShutdownEventArgs args)
        {
            // TODO: Log unexpected command
        }

        public void Recovery()
        {
            // TODO: Log unexpected command
        }

        public void Dispose()
        {
        }
    }
}