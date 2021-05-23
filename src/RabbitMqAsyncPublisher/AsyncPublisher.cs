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
    public class AsyncPublisher : IAsyncPublisher<bool>
    {
        private readonly IAsyncPublisherDiagnostics _diagnostics;

        private readonly AsyncPublisherTaskCompletionSourceRegistry _taskCompletionSourceRegistry =
            new AsyncPublisherTaskCompletionSourceRegistry();

        private ShutdownEventArgs _shutdownEventArgs;
        private int _isDisposed;

        public IModel Model { get; }

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
                () => ProcessDeliveryTag(args.DeliveryTag, args.Multiple, true),
                () => _diagnostics.TrackBasicAcksEventProcessing(args),
                duration => _diagnostics.TrackBasicAcksEventProcessingCompleted(args, duration),
                (duration, ex) => _diagnostics.TrackBasicAcksEventProcessingFailed(args, duration, ex)
            );
        }

        private void OnBasicNacks(object sender, BasicNackEventArgs args)
        {
            ProcessEvent(
                () => ProcessDeliveryTag(args.DeliveryTag, args.Multiple, false),
                () => _diagnostics.TrackBasicNacksEventProcessing(args),
                duration => _diagnostics.TrackBasicNacksEventProcessingCompleted(args, duration),
                (duration, ex) => _diagnostics.TrackBasicNacksEventProcessingFailed(args, duration, ex)
            );
        }

        private void ProcessDeliveryTag(ulong deliveryTag, bool multiple, bool ack)
        {
            lock (_taskCompletionSourceRegistry)
            {
                if (!multiple)
                {
                    if (_taskCompletionSourceRegistry.TryRemoveSingle(deliveryTag, out var source))
                    {
                        Task.Run(() => source.TrySetResult(ack));
                    }
                }
                else
                {
                    foreach (var source in _taskCompletionSourceRegistry.RemoveAllUpTo(deliveryTag))
                    {
                        Task.Run(() => source.TrySetResult(ack));
                    }
                }

                _diagnostics.TrackCompletionSourceRegistrySize(_taskCompletionSourceRegistry.Count);
            }
        }

        private void OnModelShutdown(object sender, ShutdownEventArgs args)
        {
            ProcessEvent(
                () =>
                {
                    lock (_taskCompletionSourceRegistry)
                    {
                        _shutdownEventArgs = args;

                        foreach (var source in _taskCompletionSourceRegistry.RemoveAll())
                        {
                            Task.Run(() => source.TrySetException(new AlreadyClosedException(args)));
                        }

                        _diagnostics.TrackCompletionSourceRegistrySize(_taskCompletionSourceRegistry.Count);
                    }
                },
                () => _diagnostics.TrackModelShutdownEventProcessing(args),
                duration => _diagnostics.TrackModelShutdownEventProcessingCompleted(args, duration),
                (duration, ex) => _diagnostics.TrackModelShutdownEventProcessingFailed(args, duration, ex)
            );
        }

        private void OnRecovery(object sender, EventArgs args)
        {
            ProcessEvent(
                () =>
                {
                    lock (_taskCompletionSourceRegistry)
                    {
                        _shutdownEventArgs = null;
                    }
                },
                () => _diagnostics.TrackRecoveryEventProcessing(),
                duration => _diagnostics.TrackRecoveryEventProcessingCompleted(duration),
                (duration, ex) => _diagnostics.TrackRecoveryEventProcessingFailed(duration, ex));
        }

        public async Task<bool> PublishUnsafeAsync(
            string exchange,
            string routingKey,
            ReadOnlyMemory<byte> body,
            IBasicProperties properties,
            CancellationToken cancellationToken)
        {
            ThrowIfDisposed();

            var args = new PublishUnsafeArgs(exchange, routingKey, body, properties, Model.NextPublishSeqNo);
            _diagnostics.TrackPublishUnsafe(args);
            var stopwatch = Stopwatch.StartNew();

            ulong seqNo;

            try
            {
                cancellationToken.ThrowIfCancellationRequested();

                TaskCompletionSource<bool> publishTaskCompletionSource;

                // Lock here is used to synchronize Publish calls, which can come from different threads,
                // with RabbitMQ lifecycle events (Ack, Nack, Shutdown, etc.), which are handled on a single thread.
                // If we don't do that we can have race conditions when some tasks are not cleared on shutdown.
                // TODO: think about replacing with kind of PriorityLock (PrioritySemaphore)
                // TODO: in order to process model events with higher priority than publish requests
                lock (_taskCompletionSourceRegistry)
                {
                    if (!(_shutdownEventArgs is null))
                    {
                        throw new AlreadyClosedException(_shutdownEventArgs);
                    }

                    seqNo = Model.NextPublishSeqNo;
                    Model.BasicPublish(exchange, routingKey, properties, body);
                    publishTaskCompletionSource = _taskCompletionSourceRegistry.Register(seqNo);

                    _diagnostics.TrackCompletionSourceRegistrySize(_taskCompletionSourceRegistry.Count);
                }

                _diagnostics.TrackPublishUnsafeBasicPublishCompleted(args, stopwatch.Elapsed);

                using (cancellationToken.Register(() =>
                    // ReSharper disable once MethodSupportsCancellation
                    Task.Run(() =>
                    {
                        lock (_taskCompletionSourceRegistry)
                        {
                            _taskCompletionSourceRegistry.TryRemoveSingle(seqNo, out _);
                        }

                        return publishTaskCompletionSource.TrySetCanceled(cancellationToken);
                    })))
                {
                    var acknowledged = await publishTaskCompletionSource.Task;
                    _diagnostics.TrackPublishUnsafeCompleted(args, stopwatch.Elapsed, acknowledged);

                    return acknowledged;
                }
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

        private static void ProcessEvent(
            Action process,
            Action onProcessing,
            Action<TimeSpan> onProcessingCompleted,
            Action<TimeSpan, Exception> onProcessingFailed)
        {
            onProcessing();
            var stopwatch = Stopwatch.StartNew();

            try
            {
                process();
                onProcessingCompleted(stopwatch.Elapsed);
            }
            catch (Exception ex)
            {
                onProcessingFailed(stopwatch.Elapsed, ex);
            }
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
            {
                return;
            }

            _diagnostics.TrackDispose();

            Model.BasicAcks -= OnBasicAcks;
            Model.BasicNacks -= OnBasicNacks;
            Model.ModelShutdown -= OnModelShutdown;

            lock (_taskCompletionSourceRegistry)
            {
                foreach (var source in _taskCompletionSourceRegistry.RemoveAll())
                {
                    Task.Run(() => source.TrySetException(new ObjectDisposedException(nameof(AsyncPublisher))));
                }

                _diagnostics.TrackCompletionSourceRegistrySize(_taskCompletionSourceRegistry.Count);
            }

            _diagnostics.TrackDisposeCompleted();
        }

        private void ThrowIfDisposed()
        {
            if (_isDisposed == 1)
            {
                throw new ObjectDisposedException(nameof(AsyncPublisher));
            }
        }
    }

    internal class AsyncPublisherTaskCompletionSourceRegistry
    {
        private readonly Dictionary<ulong, SourceEntry> _sources = new Dictionary<ulong, SourceEntry>();
        private readonly LinkedList<ulong> _deliveryTagQueue = new LinkedList<ulong>();

        public int Count => _sources.Count;

        public TaskCompletionSource<bool> Register(ulong deliveryTag)
        {
            var taskCompletionSource = new TaskCompletionSource<bool>();
            _sources[deliveryTag] = new SourceEntry(taskCompletionSource, _deliveryTagQueue.AddLast(deliveryTag));
            return taskCompletionSource;
        }

        public TaskCompletionSource<bool> Register(ulong deliveryTag, TaskCompletionSource<bool> taskCompletionSource)
        {
            _sources[deliveryTag] = new SourceEntry(taskCompletionSource, _deliveryTagQueue.AddLast(deliveryTag));
            return taskCompletionSource;
        }

        public bool TryRemoveSingle(ulong deliveryTag, out TaskCompletionSource<bool> source)
        {
            if (_sources.TryGetValue(deliveryTag, out var entry))
            {
                _sources.Remove(deliveryTag);
                _deliveryTagQueue.Remove(entry.QueueNode);
                source = entry.Source;
                return true;
            }

            source = default;
            return false;
        }

        // ReSharper disable once ReturnTypeCanBeEnumerable.Global
        public IReadOnlyList<TaskCompletionSource<bool>> RemoveAll()
        {
            return RemoveAllUpTo(ulong.MaxValue);
        }

        // ReSharper disable once ReturnTypeCanBeEnumerable.Global
        public IReadOnlyList<TaskCompletionSource<bool>> RemoveAllUpTo(ulong deliveryTag)
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
}