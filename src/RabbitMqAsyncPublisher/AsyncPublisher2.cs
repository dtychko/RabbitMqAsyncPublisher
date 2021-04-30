using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace RabbitMqAsyncPublisher
{
    public class AsyncPublisher2Decorator
    {
        private readonly AsyncPublisher2 _decorated;

        public AsyncPublisher2Decorator(AsyncPublisher2 decorated)
        {
            _decorated = decorated;
        }

        public async Task PublishAsync(ReadOnlyMemory<byte> message)
        {
            Task<bool> result;

            // Publish lock
            lock (_decorated)
            {
                do
                {
                    try
                    {
                        // ClosedState should always throw
                        result = _decorated.PublishAsync(message);
                        break;
                    }
                    catch
                    {
                        Thread.Sleep(1000);
                    }
                } while (true);
            }

            do
            {
                try
                {
                    await result;
                    break;
                }
                catch (Exception ex)
                {
                    // Move to closed state

                    // Retry lock
                    lock (result)
                    {
                        result = _decorated.PublishAsync(message);
                    }
                }
            } while (true);
        }
    }

    /// <summary>
    /// Limitations:
    /// 1. Doesn't listen to "IModel.BasicReturn" event, as results doesn't handle "returned" messages
    /// </summary>
    public class AsyncPublisher2 : IAsyncPublisherStateContext, IDisposable
    {
        private readonly object _stateSyncRoot = new object();
        private IAsyncPublisherState _state;
        private int _disposed;

        public IModel Model { get; }

        public string QueueName { get; }

        public AsyncPublisher2(IModel model, string queueName)
        {
            Model = model;
            QueueName = queueName;

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

        public Task<bool> PublishAsync(ReadOnlyMemory<byte> message)
        {
            return SyncStateAccess(state => state.PublishAsync(message));
        }

        public void Dispose()
        {
            var disposer = Interlocked.Exchange(ref _disposed, 1) != 0;

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

    internal class AsyncPublisherTaskCompletionSourceRegistry
    {
        private readonly ConcurrentDictionary<ulong, TaskCompletionSource<bool>> _sources =
            new ConcurrentDictionary<ulong, TaskCompletionSource<bool>>();

        private readonly ConcurrentQueue<ulong> _queue = new ConcurrentQueue<ulong>();

        public TaskCompletionSource<bool> Add(ulong seqNo)
        {
            var taskCompletionSource = new TaskCompletionSource<bool>();

            _sources[seqNo] = taskCompletionSource;
            _queue.Enqueue(seqNo);

            return taskCompletionSource;
        }

        public TaskCompletionSource<bool> Get(ulong deliveryTag)
        {
            if (_queue.TryPeek(out var seqNo) && seqNo == deliveryTag)
            {
                _queue.TryDequeue(out _);
            }

            return _sources.TryRemove(seqNo, out var source) ? source : null;
        }

        public IEnumerable<TaskCompletionSource<bool>> GetAllUpTo(ulong deliveryTag)
        {
            while (_queue.TryPeek(out var seqNo) && seqNo <= deliveryTag)
            {
                _queue.TryDequeue(out _);

                if (_sources.TryRemove(seqNo, out var source))
                {
                    yield return source;
                }
            }
        }

        public IEnumerable<TaskCompletionSource<bool>> GetAll()
        {
            while (_queue.TryDequeue(out var seqNo))
            {
                if (_sources.TryRemove(seqNo, out var source))
                {
                    yield return source;
                }
            }
        }
    }

    internal class AsyncPublisherTaskCompletionSourceRegistry2
    {
        private readonly Dictionary<ulong, SourceEntry> _sources = new Dictionary<ulong, SourceEntry>();
        private readonly LinkedList<ulong> _deliveryTagQueue = new LinkedList<ulong>();

        public TaskCompletionSource<bool> Add(ulong deliveryTag)
        {
            var taskCompletionSource = new TaskCompletionSource<bool>();

            _sources[deliveryTag] = new SourceEntry(taskCompletionSource, _deliveryTagQueue.AddLast(deliveryTag));

            return taskCompletionSource;
        }

        public TaskCompletionSource<bool> Get(ulong deliveryTag)
        {
            if (_sources.TryGetValue(deliveryTag, out var entry))
            {
                _sources.Remove(deliveryTag);
                _deliveryTagQueue.Remove(entry.QueueNode);
                return entry.Source;
            }

            return null;
        }

        public IEnumerable<TaskCompletionSource<bool>> GetAllUpTo(ulong deliveryTag)
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

        public IEnumerable<TaskCompletionSource<bool>> GetAll()
        {
            return GetAllUpTo(ulong.MaxValue);
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
        private readonly AsyncPublisherTaskCompletionSourceRegistry2 _completionSourceRegistry =
            new AsyncPublisherTaskCompletionSourceRegistry2();

        private readonly IAsyncPublisherStateContext _context;

        public AsyncPublisherOpenState(IAsyncPublisherStateContext context)
        {
            _context = context;
        }

        public Task<bool> PublishAsync(ReadOnlyMemory<byte> message)
        {
            var seqNo = _context.Model.NextPublishSeqNo;

            _context.Model.BasicPublish("", _context.QueueName, _context.Model.CreateBasicProperties(), message);

            return _completionSourceRegistry.Add(seqNo).Task;
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
            foreach (var source in _completionSourceRegistry.GetAll())
            {
                source.TrySetException(new AlreadyClosedException(args));
            }

            _context.SetState(new AsyncPublisherClosedState(_context, args));
        }

        public void Recovery()
        {
            // TODO: Make sure that Recovery method couldn't be executed for "Open" state 
            // TODO: Perhaps log unexpected command
        }

        public void Dispose()
        {
            foreach (var source in _completionSourceRegistry.GetAll())
            {
                source.TrySetException(new ObjectDisposedException("Publisher was disposed"));
            }

            _context.SetState(new AsyncPublisherDisposedState());
        }

        private void ProcessDeliveryTag(ulong deliveryTag, bool multiple, bool ack)
        {
            if (!multiple)
            {
                _completionSourceRegistry.Get(deliveryTag)?.SetResult(ack);
                return;
            }

            foreach (var source in _completionSourceRegistry.GetAllUpTo(deliveryTag))
            {
                source.SetResult(ack);
            }
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

        public Task<bool> PublishAsync(ReadOnlyMemory<byte> message)
        {
            return Task.FromException<bool>(new AlreadyClosedException(_args));
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
        public Task<bool> PublishAsync(ReadOnlyMemory<byte> message)
        {
            throw new ObjectDisposedException("Publisher was disposed");
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
        }

        public void Dispose()
        {
        }
    }
}