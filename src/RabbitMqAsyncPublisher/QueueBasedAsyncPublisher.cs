using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace RabbitMqAsyncPublisher
{
    using static DiagnosticsUtils;

    public class QueueBasedAsyncPublisher : IAsyncPublisher<bool>
    {
        private readonly IQueueBasedAsyncPublisherDiagnostics _diagnostics;
        private readonly ConcurrentQueue<PublishQueueItem> _publishQueue = new ConcurrentQueue<PublishQueueItem>();
        private readonly ConcurrentQueue<AckQueueItem> _ackQueue = new ConcurrentQueue<AckQueueItem>();

        private readonly AsyncPublisherTaskCompletionSourceRegistry _completionSourceRegistry =
            new AsyncPublisherTaskCompletionSourceRegistry();

        private volatile int _publishQueueSize;
        private volatile int _ackQueueSize;
        private int _completionSourceRegistrySize;

        private readonly AsyncManualResetEvent _publishEvent = new AsyncManualResetEvent(false);
        private readonly AsyncManualResetEvent _ackEvent = new AsyncManualResetEvent(false);

        private readonly Task _publishLoop;
        private readonly Task _ackLoop;

        private volatile int _isDisposed;

        public IModel Model { get; }

        public QueueBasedAsyncPublisher(IModel model, IQueueBasedAsyncPublisherDiagnostics diagnostics = null)
        {
            Model = model;
            _diagnostics = diagnostics ?? QueueBasedAsyncPublisherDiagnostics.NoDiagnostics;

            // TODO: Make sure that "BasicAcks" and "BasicNacks" events are always fired on a single thread.
            Model.BasicAcks += OnBasicAcks;
            Model.BasicNacks += OnBasicNacks;

            _publishLoop = Task.Run(StartPublishLoop);
            _ackLoop = Task.Run(StartAckLoop);
        }

        private void OnBasicAcks(object sender, BasicAckEventArgs e)
        {
            EnqueueAck(new AckQueueItem(e.DeliveryTag, e.Multiple, true));
            TrackSafe(_diagnostics.TrackAckTaskEnqueued,
                new AckArgs(e.DeliveryTag, e.Multiple, true),
                new QueueBasedAsyncPublisherStatus(_publishQueueSize, _ackQueueSize, _completionSourceRegistrySize));
            _ackEvent.SetAsync();
        }

        private void OnBasicNacks(object sender, BasicNackEventArgs e)
        {
            EnqueueAck(new AckQueueItem(e.DeliveryTag, e.Multiple, false));
            TrackSafe(_diagnostics.TrackAckTaskEnqueued,
                new AckArgs(e.DeliveryTag, e.Multiple, false),
                new QueueBasedAsyncPublisherStatus(_publishQueueSize, _ackQueueSize, _completionSourceRegistrySize));
            _ackEvent.SetAsync();
        }

        private async void StartPublishLoop()
        {
            try
            {
                await _publishEvent.WaitAsync().ConfigureAwait(false);
                TrackStatusSafe();

                while (_isDisposed == 0 || _publishQueueSize > 0)
                {
                    _publishEvent.Reset();
                    RunPublishInnerLoop();
                    await _publishEvent.WaitAsync().ConfigureAwait(false);
                    TrackStatusSafe();
                }
            }
            catch (Exception ex)
            {
                TrackSafe(_diagnostics.TrackUnexpectedException,
                    "Unexpected publish loop exception: " +
                    $"publishQueueSize={_publishQueueSize}; ackQueueSize={_ackQueueSize}; completionSourceRegistrySize={_completionSourceRegistrySize}",
                    ex);

                // TODO: ? Move publisher to state when it throws on each attempt to publish a message
                // TODO: ? Restart the loop after some delay
            }
        }

        private async void StartAckLoop()
        {
            try
            {
                await _ackEvent.WaitAsync().ConfigureAwait(false);
                TrackStatusSafe();

                while (_isDisposed == 0 || _ackQueueSize > 0)
                {
                    _ackEvent.Reset();
                    RunAckInnerLoop();
                    await _ackEvent.WaitAsync().ConfigureAwait(false);
                    TrackStatusSafe();
                }
            }
            catch (Exception ex)
            {
                TrackSafe(_diagnostics.TrackUnexpectedException,
                    "Unexpected ack loop exception: " +
                    $"publishQueueSize={_publishQueueSize}; ackQueueSize={_ackQueueSize}; completionSourceRegistrySize={_completionSourceRegistrySize}",
                    ex);

                // TODO: ? Move publisher to state when it throws on each attempt to publish a message
                // TODO: ? Restart the loop after some delay
            }
        }

        private void TrackStatusSafe()
        {
            TrackSafe(_diagnostics.TrackStatus,
                new QueueBasedAsyncPublisherStatus(_publishQueueSize, _ackQueueSize,
                    _completionSourceRegistrySize));
        }

        private void RunPublishInnerLoop()
        {
            while (TryDequeuePublish(out var publishQueueItem))
            {
                var cancellationToke = publishQueueItem.CancellationToken;
                var taskCompletionSource = publishQueueItem.TaskCompletionSource;
                ulong seqNo;

                try
                {
                    if (cancellationToke.IsCancellationRequested)
                    {
                        Task.Run(() => taskCompletionSource.TrySetCanceled());
                    }

                    if (_isDisposed == 1)
                    {
                        Task.Run(() =>
                            taskCompletionSource.TrySetException(
                                new ObjectDisposedException(nameof(QueueBasedAsyncPublisher))));
                        continue;
                    }

                    if (IsModelClosed(out var shutdownEventArgs))
                    {
                        Task.Run(() =>
                            taskCompletionSource.TrySetException(new AlreadyClosedException(shutdownEventArgs)));
                        continue;
                    }

                    if (cancellationToke.CanBeCanceled)
                    {
                        var registration = cancellationToke.Register(() =>
                        {
                            Task.Run(() => taskCompletionSource.TrySetCanceled());
                        });
                        taskCompletionSource.Task.ContinueWith(_ => { registration.Dispose(); });
                    }

                    seqNo = Model.NextPublishSeqNo;
                }
                catch (Exception ex)
                {
                    TrackSafe(_diagnostics.TrackUnexpectedException, "Unable to start message publishing.", ex);
                    Task.Run(() => taskCompletionSource.TrySetException(ex));
                    continue;
                }

                var publishArgs = new PublishArgs(publishQueueItem.Exchange, publishQueueItem.RoutingKey,
                    publishQueueItem.Body, publishQueueItem.Properties);
                TrackSafe(_diagnostics.TrackPublishStarted, publishArgs, seqNo);
                var stopwatch = Stopwatch.StartNew();

                try
                {
                    RegisterTaskCompletionSource(seqNo, taskCompletionSource);
                    Model.BasicPublish(publishQueueItem.Exchange, publishQueueItem.RoutingKey,
                        publishQueueItem.Properties, publishQueueItem.Body);
                }
                catch (Exception ex)
                {
                    TrackSafe(_diagnostics.TrackPublishFailed, publishArgs, seqNo, stopwatch.Elapsed, ex);
                    TryRemoveSingleTaskCompletionSource(seqNo, out _);
                    Task.Run(() => taskCompletionSource.TrySetException(ex));
                    continue;
                }

                TrackSafe(_diagnostics.TrackPublishSucceeded, publishArgs, seqNo, stopwatch.Elapsed);
            }
        }

        private void RunAckInnerLoop()
        {
            while (TryDequeueAck(out var ackQueueItem))
            {
                var deliveryTag = ackQueueItem.DeliveryTag;
                var multiple = ackQueueItem.Multiple;
                var ack = ackQueueItem.Ack;

                var ackArgs = new AckArgs(deliveryTag, multiple, ack);
                TrackSafe(_diagnostics.TrackAckStarted, ackArgs);
                var stopwatch = Stopwatch.StartNew();

                try
                {
                    if (!multiple)
                    {
                        if (TryRemoveSingleTaskCompletionSource(deliveryTag, out var source))
                        {
                            Task.Run(() => source.TrySetResult(ack));
                        }
                    }
                    else
                    {
                        foreach (var source in RemoveAllTaskCompletionSourcesUpTo(deliveryTag))
                        {
                            Task.Run(() => source.TrySetResult(ack));
                        }
                    }
                }
                catch (Exception ex)
                {
                    TrackSafe(_diagnostics.TrackUnexpectedException,
                        $"Unable to process ack queue item: deliveryTag={deliveryTag}; multiple={multiple}; ack={ack}.",
                        ex);
                    continue;
                }

                TrackSafe(_diagnostics.TrackAckSucceeded, ackArgs, stopwatch.Elapsed);
            }
        }

        public Task<bool> PublishUnsafeAsync(
            string exchange,
            string routingKey,
            ReadOnlyMemory<byte> body,
            IBasicProperties properties,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var taskCompletionSource = new TaskCompletionSource<bool>();
            var queueItem = new PublishQueueItem(exchange, routingKey, body, properties, cancellationToken,
                taskCompletionSource);

            lock (_publishQueue)
            {
                if (_isDisposed == 1)
                {
                    throw new ObjectDisposedException(nameof(QueueBasedAsyncPublisher));
                }

                EnqueuePublish(queueItem);
            }

            TrackSafe(_diagnostics.TrackPublishTaskEnqueued,
                new PublishArgs(exchange, routingKey, body, properties),
                new QueueBasedAsyncPublisherStatus(_publishQueueSize, _ackQueueSize, _completionSourceRegistrySize));
            _publishEvent.SetAsync();

            return taskCompletionSource.Task;
        }

        public void Dispose()
        {
            bool shouldDispose;

            lock (_publishQueue)
            {
                shouldDispose = _isDisposed == 0;
                _isDisposed = 1;
            }

            if (!shouldDispose)
            {
                Task.WaitAll(_publishLoop, _ackLoop);
                return;
            }

            TrackSafe(_diagnostics.TrackDisposeStarted);
            var stopwatch = Stopwatch.StartNew();

            Model.BasicAcks -= OnBasicAcks;
            Model.BasicNacks -= OnBasicNacks;

            Task.WaitAll(_publishEvent.SetAsync(), _ackEvent.SetAsync(), _publishLoop, _ackLoop);

            foreach (var source in RemoveAllTaskCompletionSourcesUpTo(ulong.MaxValue))
            {
                Task.Run(() => source.TrySetException(new ObjectDisposedException(nameof(QueueBasedAsyncPublisher))));
            }

            TrackSafe(_diagnostics.TrackDisposeSucceeded, stopwatch.Elapsed);
        }

        private void RegisterTaskCompletionSource(ulong deliveryTag, TaskCompletionSource<bool> taskCompletionSource)
        {
            lock (_completionSourceRegistry)
            {
                Interlocked.Increment(ref _completionSourceRegistrySize);
                _completionSourceRegistry.Register(deliveryTag, taskCompletionSource);
            }
        }

        private bool TryRemoveSingleTaskCompletionSource(ulong deliveryTag,
            out TaskCompletionSource<bool> taskCompletionSource)
        {
            lock (_completionSourceRegistry)
            {
                if (_completionSourceRegistry.TryRemoveSingle(deliveryTag, out taskCompletionSource))
                {
                    Interlocked.Decrement(ref _completionSourceRegistrySize);
                    return true;
                }

                return false;
            }
        }

        private IReadOnlyList<TaskCompletionSource<bool>> RemoveAllTaskCompletionSourcesUpTo(ulong deliveryTag)
        {
            lock (_completionSourceRegistry)
            {
                var result = _completionSourceRegistry.RemoveAllUpTo(deliveryTag);
                Interlocked.Add(ref _completionSourceRegistrySize, -result.Count);
                return result;
            }
        }

        private void EnqueueAck(AckQueueItem item)
        {
            Interlocked.Increment(ref _ackQueueSize);
            _ackQueue.Enqueue(item);
        }

        private bool TryDequeueAck(out AckQueueItem item)
        {
            if (_ackQueue.TryDequeue(out item))
            {
                Interlocked.Decrement(ref _ackQueueSize);
                return true;
            }

            return false;
        }

        private void EnqueuePublish(PublishQueueItem item)
        {
            Interlocked.Increment(ref _publishQueueSize);
            _publishQueue.Enqueue(item);
        }

        private bool TryDequeuePublish(out PublishQueueItem item)
        {
            if (_publishQueue.TryDequeue(out item))
            {
                Interlocked.Decrement(ref _publishQueueSize);
                return true;
            }

            return false;
        }

        private bool IsModelClosed(out ShutdownEventArgs shutdownEventArgs)
        {
            shutdownEventArgs = default;
            Interlocked.Exchange(ref shutdownEventArgs, Model.CloseReason);
            return !(shutdownEventArgs is null);
        }

        private class PublishQueueItem
        {
            public string Exchange { get; }
            public string RoutingKey { get; }
            public ReadOnlyMemory<byte> Body { get; }
            public IBasicProperties Properties { get; }
            public CancellationToken CancellationToken { get; }
            public TaskCompletionSource<bool> TaskCompletionSource { get; }

            public PublishQueueItem(string exchange, string routingKey, ReadOnlyMemory<byte> body,
                IBasicProperties properties, CancellationToken cancellationToken,
                TaskCompletionSource<bool> taskCompletionSource)
            {
                Exchange = exchange;
                RoutingKey = routingKey;
                Body = body;
                Properties = properties;
                CancellationToken = cancellationToken;
                TaskCompletionSource = taskCompletionSource;
            }
        }

        private class AckQueueItem
        {
            public ulong DeliveryTag { get; }
            public bool Multiple { get; }
            public bool Ack { get; }

            public AckQueueItem(ulong deliveryTag, bool multiple, bool ack)
            {
                DeliveryTag = deliveryTag;
                Multiple = multiple;
                Ack = ack;
            }
        }
    }
}