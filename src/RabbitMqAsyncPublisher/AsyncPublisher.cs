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
        // TODO: Rename to something like "PublishUnsafeAsync" 
        Task<TResult> PublishAsync(
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
        private readonly ILogger _logger;
        private int _isDisposed;

        public IModel Model { get; }

        ILogger IAsyncPublisherStateContext.Logger => _logger;

        public AsyncPublisher(IModel model)
            : this(model, EmptyLogger.Instance)
        {
        }

        public AsyncPublisher(IModel model, ILogger logger)
        {
            // Heuristic based on reverse engineering of "RabbitMQ.Client" lib
            // that helps to make sure that "ConfirmSelect" method was called on the model
            // to enable confirm mode.
            if (model.NextPublishSeqNo == 0)
            {
                throw new ArgumentException("Channel should be in confirm mode.");
            }

            Model = model;
            _logger = logger;
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
            SyncStateAccess(state => state.Ack(args.DeliveryTag, args.Multiple));
        }

        private void OnBasicNacks(object sender, BasicNackEventArgs args)
        {
            SyncStateAccess(state => state.Nack(args.DeliveryTag, args.Multiple));
        }

        private void OnModelShutdown(object sender, ShutdownEventArgs args)
        {
            SyncStateAccess(state =>
            {
                _logger.Warn(
                    "{0} received event '{1}'. {2}",
                    nameof(AsyncPublisher),
                    nameof(IModel.ModelShutdown),
                    args.ToString()
                );

                state.Shutdown(args);
            });
        }

        private void OnRecovery(object sender, EventArgs args)
        {
            SyncStateAccess(state =>
            {
                _logger.Info(
                    "{0} received event '{1}'.",
                    nameof(AsyncPublisher),
                    nameof(IRecoverable.Recovery)
                );

                state.Recovery();
            });
        }

        public Task<bool> PublishAsync(
            string exchange,
            string routingKey,
            ReadOnlyMemory<byte> body,
            IBasicProperties properties,
            CancellationToken cancellationToken)
        {
            // TODO: Try to publish without lock. Publish method access could be synced in decorator
            return _state.PublishAsync(exchange, routingKey, body, properties, cancellationToken);
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
            _state = new AsyncPublisherStateDiagnosticsDecorator(state, _logger);
        }

        private void SyncStateAccess(Action<IAsyncPublisherState> access)
        {
            lock (_stateSyncRoot)
            {
                access(_state);
            }
        }
    }

    internal interface IAsyncPublisherStateContext
    {
        IModel Model { get; }

        ILogger Logger { get; }

        void SetState(IAsyncPublisherState state);
    }

    internal class AsyncPublisherStateDiagnosticsDecorator : IAsyncPublisherState
    {
        private readonly IAsyncPublisherState _decorated;
        private readonly ILogger _logger;

        public AsyncPublisherStateDiagnosticsDecorator(IAsyncPublisherState decorated, ILogger logger)
        {
            _decorated = decorated;
            _logger = logger;
        }

        public Task<bool> PublishAsync(
            string exchange,
            string routingKey,
            ReadOnlyMemory<byte> body,
            IBasicProperties properties,
            CancellationToken cancellationToken)
        {
            _logger.LogAsyncPublisherStateSignal(_decorated, nameof(PublishAsync));
            return _decorated.PublishAsync(exchange, routingKey, body, properties, cancellationToken);
        }

        public void Ack(ulong deliveryTag, bool multiple)
        {
            _logger.LogAsyncPublisherStateSignal(_decorated, nameof(Ack));
            _decorated.Ack(deliveryTag, multiple);
        }

        public void Nack(ulong deliveryTag, bool multiple)
        {
            _logger.LogAsyncPublisherStateSignal(_decorated, nameof(Nack));
            _decorated.Nack(deliveryTag, multiple);
        }

        public void Shutdown(ShutdownEventArgs args)
        {
            _logger.LogAsyncPublisherStateSignal(_decorated, nameof(Shutdown));
            _decorated.Shutdown(args);
        }

        public void Recovery()
        {
            _logger.LogAsyncPublisherStateSignal(_decorated, nameof(Recovery));
            _decorated.Recovery();
        }

        public void Dispose()
        {
            _logger.LogAsyncPublisherStateSignal(_decorated, nameof(Dispose));
            _decorated.Dispose();
        }
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
            var task = _taskRegistry.Register(seqNo, cancellationToken);

            try
            {
                _context.Model.BasicPublish(exchange, routingKey, properties, body);
            }
            catch (Exception ex)
            {
                // TODO: Warn("Unable to publish message")
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
            _context.Logger.LogAsyncPublisherStateUnsupportedSignal(this, nameof(Recovery));
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
            _context.Logger.LogAsyncPublisherStateUnsupportedSignal(this, nameof(Ack));
        }

        public void Nack(ulong deliveryTag, bool multiple)
        {
            _context.Logger.LogAsyncPublisherStateUnsupportedSignal(this, nameof(Nack));
        }

        public void Shutdown(ShutdownEventArgs args)
        {
            _context.Logger.LogAsyncPublisherStateUnsupportedSignal(this, nameof(Shutdown));
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
            _context.Logger.LogAsyncPublisherStateUnsupportedSignal(this, nameof(Ack));
        }

        public void Nack(ulong deliveryTag, bool multiple)
        {
            _context.Logger.LogAsyncPublisherStateUnsupportedSignal(this, nameof(Nack));
        }

        public void Shutdown(ShutdownEventArgs args)
        {
            _context.Logger.LogAsyncPublisherStateUnsupportedSignal(this, nameof(Shutdown));
        }

        public void Recovery()
        {
            _context.Logger.LogAsyncPublisherStateUnsupportedSignal(this, nameof(Recovery));
        }

        public void Dispose()
        {
        }
    }

    internal static class AsyncPublisherStateLoggerExtensions
    {
        public static void LogAsyncPublisherStateSignal(
            this ILogger logger,
            IAsyncPublisherState state,
            string signalName)
        {
            logger.Debug(
                "{0} in state '{1}' received signal '{2}'.",
                nameof(AsyncPublisher),
                state.GetType().Name,
                signalName
            );
        }

        public static void LogAsyncPublisherStateUnsupportedSignal(
            this ILogger logger,
            IAsyncPublisherState state,
            string signalName)
        {
            logger.Warn(
                "{0} in state '{1}' doesn't support signal '{2}'.",
                nameof(AsyncPublisher),
                state.GetType().Name,
                signalName
            );
        }
    }
}