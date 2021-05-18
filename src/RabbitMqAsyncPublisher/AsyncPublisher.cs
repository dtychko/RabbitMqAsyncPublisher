using System;
using System.Collections.Generic;
using System.Diagnostics;
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
        Task<TResult> PublishUnsafeAsync(
            string exchange,
            string routingKey,
            ReadOnlyMemory<byte> body,
            IBasicProperties properties,
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
        private readonly IAsyncPublisherDiagnostics _diagnostics;
        private int _isDisposed;

        public IModel Model { get; }

        IAsyncPublisherDiagnostics IAsyncPublisherStateContext.Diagnostics => _diagnostics;

        public AsyncPublisher(IModel model)
            : this(model, EmptyDiagnostics.Instance)
        {
        }

        public AsyncPublisher(IModel model, IAsyncPublisherDiagnostics diagnostics)
        {
            // Heuristic based on reverse engineering of "RabbitMQ.Client" lib
            // that helps to make sure that "ConfirmSelect" method was called on the model
            // to enable confirm mode.
            if (model.NextPublishSeqNo == 0)
            {
                throw new ArgumentException("Channel should be in confirm mode.");
            }

            Model = model;
            _diagnostics = diagnostics;
            SetState(new AsyncPublisherOpenState(this));

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
            ProcessEvent(
                state => state.Ack(args.DeliveryTag, args.Multiple),
                () => _diagnostics.TrackBasicAcksEventProcessing(args),
                duration => _diagnostics.TrackBasicAcksEventProcessingCompleted(args, duration),
                (duration, ex) => _diagnostics.TrackBasicAcksEventProcessingFailed(args, duration, ex)
            );
        }

        private void OnBasicNacks(object sender, BasicNackEventArgs args)
        {
            ProcessEvent(
                state => state.Nack(args.DeliveryTag, args.Multiple),
                () => _diagnostics.TrackBasicNacksEventProcessing(args),
                duration => _diagnostics.TrackBasicNacksEventProcessingCompleted(args, duration),
                (duration, ex) => _diagnostics.TrackBasicNacksEventProcessingFailed(args, duration, ex)
            );
        }

        private void OnModelShutdown(object sender, ShutdownEventArgs args)
        {
            ProcessEvent(
                state => state.Shutdown(args),
                () => _diagnostics.TrackModelShutdownEventProcessing(args),
                duration => _diagnostics.TrackModelShutdownEventProcessingCompleted(args, duration),
                (duration, ex) => _diagnostics.TrackModelShutdownEventProcessingFailed(args, duration, ex)
            );
        }

        private void OnRecovery(object sender, EventArgs args)
        {
            ProcessEvent(
                state => state.Recovery(),
                () => _diagnostics.TrackRecoveryEventProcessing(),
                duration => _diagnostics.TrackRecoveryEventProcessingCompleted(duration),
                (duration, ex) => _diagnostics.TrackRecoveryEventProcessingFailed(duration, ex)
            );
        }

        public async Task<bool> PublishUnsafeAsync(
            string exchange,
            string routingKey,
            ReadOnlyMemory<byte> body,
            IBasicProperties properties,
            CancellationToken cancellationToken)
        {
            var args = new PublishUnsafeArgs(exchange, routingKey, body, properties);
            _diagnostics.TrackPublishUnsafe(args);
            var stopwatch = Stopwatch.StartNew();

            try
            {
                cancellationToken.ThrowIfCancellationRequested();

                var publishTask = _state.PublishAsync(exchange, routingKey, body, properties, cancellationToken);
                _diagnostics.TrackPublishUnsafePublished(args, stopwatch.Elapsed);

                var acknowledged = await publishTask;
                _diagnostics.TrackPublishUnsafeCompleted(args, stopwatch.Elapsed, acknowledged);

                return acknowledged;
            }
            catch (OperationCanceledException)
            {
                _diagnostics.TrackPublishUnsafeCanceled(args, stopwatch.Elapsed);

                throw;
            }
            catch (Exception ex)
            {
                _diagnostics.TrackPublishUnsafeFailed(args, stopwatch.Elapsed, ex);

                throw;
            }
        }

        public void Dispose()
        {
            // Make sure that after calling "Dispose" method publisher is always moved to "Disposed" state,
            // even if the current call wasn't chosen to be "disposer".
            SyncStateAccess(state => state.Dispose());

            if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
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

        void IAsyncPublisherStateContext.SetState(IAsyncPublisherState state)
        {
            SetState(state);
        }

        private void SetState(IAsyncPublisherState state)
        {
            _state = state;
        }

        private void SyncStateAccess(Action<IAsyncPublisherState> access)
        {
            lock (_stateSyncRoot)
            {
                access(_state);
            }
        }

        private void ProcessEvent(
            Action<IAsyncPublisherState> process,
            Action onProcessing,
            Action<TimeSpan> onProcessingCompleted,
            Action<TimeSpan, Exception> onProcessingFailed)
        {
            onProcessing();
            var stopwatch = Stopwatch.StartNew();

            try
            {
                SyncStateAccess(process);
                onProcessingCompleted(stopwatch.Elapsed);
            }
            catch (Exception ex)
            {
                onProcessingFailed(stopwatch.Elapsed, ex);
            }
        }
    }

    internal interface IAsyncPublisherStateContext
    {
        IModel Model { get; }

        IAsyncPublisherDiagnostics Diagnostics { get; }

        void SetState(IAsyncPublisherState state);
    }

    internal interface IAsyncPublisherState
    {
        Task<bool> PublishAsync(
            string exchange,
            string routingKey,
            ReadOnlyMemory<byte> body,
            IBasicProperties properties,
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
        private readonly object _syncRoot = new object();

        public Task<bool> Register(ulong deliveryTag, CancellationToken cancellationToken)
        {
            var taskCompletionSource = new TaskCompletionSource<bool>();
            var cancellationTokenRegistration =
                cancellationToken.Register(() => taskCompletionSource.TrySetCanceled(cancellationToken));

            // ReSharper disable once MethodSupportsCancellation
            taskCompletionSource.Task.ContinueWith(_ => cancellationTokenRegistration.Dispose());

            lock (_syncRoot)
            {
                _sources[deliveryTag] = new SourceEntry(taskCompletionSource, _deliveryTagQueue.AddLast(deliveryTag));
            }

            return taskCompletionSource.Task;
        }

        public void SetResult(ulong deliveryTag, bool result)
        {
            GetSingle(deliveryTag)?.TrySetResult(result);
        }

        public void SetException(ulong deliveryTag, Exception ex)
        {
            GetSingle(deliveryTag)?.TrySetException(ex);
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

        private TaskCompletionSource<bool> GetSingle(ulong deliveryTag)
        {
            lock (_syncRoot)
            {
                if (_sources.TryGetValue(deliveryTag, out var entry))
                {
                    _sources.Remove(deliveryTag);
                    _deliveryTagQueue.Remove(entry.QueueNode);
                    return entry.Source;
                }

                return null;
            }
        }

        private IEnumerable<TaskCompletionSource<bool>> GetAllUpTo(ulong deliveryTag)
        {
            lock (_syncRoot)
            {
                var result = new List<TaskCompletionSource<bool>>();

                while (_deliveryTagQueue.Count > 0 && _deliveryTagQueue.First.Value <= deliveryTag)
                {
                    if (_sources.TryGetValue(_deliveryTagQueue.First.Value, out var entry))
                    {
                        _sources.Remove(_deliveryTagQueue.First.Value);
                        result.Add(entry.Source);
                    }

                    _deliveryTagQueue.RemoveFirst();
                }

                return result;
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
            IBasicProperties properties,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var seqNo = _context.Model.NextPublishSeqNo;

            // Task should be registered before calling "BasicPublish" method
            // to make sure that the task already presents in the registry
            // when "Ack" from the broker is received.
            // Otherwise race condition could occur because the model notifies about received acks in parallel thread.  
            var task = _taskRegistry.Register(seqNo, cancellationToken);

            try
            {
                _context.Model.BasicPublish(exchange, routingKey, properties, body);
            }
            catch (Exception ex)
            {
                // Make sure that task is removed from the registry in case of immediate exception.
                // Otherwise memory leak could occur.
                _taskRegistry.SetException(seqNo, ex);
            }

            return task;
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
            _context.Diagnostics.TrackUnsupportedSignal(GetType().Name, nameof(Recovery));
        }

        public void Dispose()
        {
            _taskRegistry.SetExceptionForAll(new ObjectDisposedException(nameof(AsyncPublisher)));
            _context.SetState(new AsyncPublisherDisposedState(_context));
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
            IBasicProperties properties,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            throw new AlreadyClosedException(_args);
        }

        public void Ack(ulong deliveryTag, bool multiple)
        {
            _context.Diagnostics.TrackUnsupportedSignal(GetType().Name, nameof(Ack));
        }

        public void Nack(ulong deliveryTag, bool multiple)
        {
            _context.Diagnostics.TrackUnsupportedSignal(GetType().Name, nameof(Nack));
        }

        public void Shutdown(ShutdownEventArgs args)
        {
            _context.Diagnostics.TrackUnsupportedSignal(GetType().Name, nameof(Shutdown));
        }

        public void Recovery()
        {
            _context.SetState(new AsyncPublisherOpenState(_context));
        }

        public void Dispose()
        {
            _context.SetState(new AsyncPublisherDisposedState(_context));
        }
    }

    internal class AsyncPublisherDisposedState : IAsyncPublisherState
    {
        private readonly IAsyncPublisherStateContext _context;

        public AsyncPublisherDisposedState(IAsyncPublisherStateContext context)
        {
            _context = context;
        }

        public Task<bool> PublishAsync(
            string exchange,
            string routingKey,
            ReadOnlyMemory<byte> body,
            IBasicProperties properties,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            throw new ObjectDisposedException(nameof(AsyncPublisher));
        }

        public void Ack(ulong deliveryTag, bool multiple)
        {
            _context.Diagnostics.TrackUnsupportedSignal(GetType().Name, nameof(Ack));
        }

        public void Nack(ulong deliveryTag, bool multiple)
        {
            _context.Diagnostics.TrackUnsupportedSignal(GetType().Name, nameof(Nack));
        }

        public void Shutdown(ShutdownEventArgs args)
        {
            _context.Diagnostics.TrackUnsupportedSignal(GetType().Name, nameof(Shutdown));
        }

        public void Recovery()
        {
            _context.Diagnostics.TrackUnsupportedSignal(GetType().Name, nameof(Recovery));
        }

        public void Dispose()
        {
        }
    }
}