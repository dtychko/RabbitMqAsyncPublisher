using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace RabbitMqAsyncPublisher
{
    public class QueueBasedAsyncPublisherWithRetries : IAsyncPublisher<RetryingPublisherResult>
    {
        private readonly IAsyncPublisher<bool> _decorated;
        private readonly TimeSpan _retryDelay;
        private readonly IAsyncPublisherWithRetriesDiagnostics _diagnostics;
        private readonly LinkedList<QueueEntry> _orderQueue = new LinkedList<QueueEntry>();

        private readonly ConcurrentQueue<PublishQueueItem> _publishQueue = new ConcurrentQueue<PublishQueueItem>();
        private volatile int _publishQueueSize;
        private readonly AsyncManualResetEvent _publishEvent = new AsyncManualResetEvent(false);
        private readonly Task _publishLoop;
        private volatile int _isDisposed;

        public IModel Model => _decorated.Model;

        public QueueBasedAsyncPublisherWithRetries(
            IAsyncPublisher<bool> decorated,
            TimeSpan retryDelay)
        {
            _decorated = decorated;
            _retryDelay = retryDelay;

            _publishLoop = Task.Run(StartPublishLoop);
        }

        private async void StartPublishLoop()
        {
            try
            {
                await _publishEvent.WaitAsync().ConfigureAwait(false);
                // TrackStatusSafe();

                while (_isDisposed == 0 || _publishQueueSize > 0)
                {
                    _publishEvent.Reset();
                    await RunPublishInnerLoop();
                    await _publishEvent.WaitAsync().ConfigureAwait(false);
                    // TrackStatusSafe();
                }
            }
            catch (Exception ex)
            {
                // TrackSafe(_diagnostics.TrackUnexpectedException,
                //     "Unexpected publish loop exception: " +
                //     $"publishQueueSize={_publishQueueSize}; ackQueueSize={_ackQueueSize}; completionSourceRegistrySize={_completionSourceRegistrySize}",
                //     ex);
            }
        }

        private async Task RunPublishInnerLoop()
        {
            while (TryDequeuePublish(out var publishQueueItem))
            {
                var cancellationToken = publishQueueItem.CancellationToken;
                var taskCompletionSource = publishQueueItem.TaskCompletionSource;

                var orderQueueNode = AddLastSynced(new QueueEntry());

                try
                {
                    for (var attempt = 1;; attempt += 1)
                    {
                        if (cancellationToken.IsCancellationRequested)
                        {
                            Task.Run(() => taskCompletionSource.TrySetCanceled());
                            continue;
                        }

                        if (_isDisposed == 1)
                        {
                            Task.Run(() =>
                                taskCompletionSource.TrySetException(
                                    new ObjectDisposedException(nameof(QueueBasedAsyncPublisher))));
                            continue;
                        }

                        if (attempt == 1)
                        {
                            // TODO: Implement cancellationToken observation (cancellationToken.Register(() => { })
                        }

                        if (attempt == 2)
                        {
                            LinkedListNode<QueueEntry> nextQueueNode;

                            while ((nextQueueNode = GetFirstSynced()) != orderQueueNode)
                            {
                                // TODO: Replace with taskCompletionSource.TrySetException()
                                // ThrowIfDisposed();

                                await Task.WhenAny(
                                    Task.Delay(-1, cancellationToken),
                                    nextQueueNode.Value.CompletionSource.Task
                                );
                            }
                        }

                        if (attempt > 2)
                        {
                            await Task.Delay(_retryDelay, cancellationToken);
                        }

                        if (await TryPublishAsync(attempt, publishQueueItem.Exchange,
                            publishQueueItem.RoutingKey,
                            publishQueueItem.Body, publishQueueItem.Properties, publishQueueItem.CancellationToken))
                        {
                            taskCompletionSource.TrySetResult(new RetryingPublisherResult(attempt - 1));
                        }
                    }
                }
                catch (Exception ex)
                {
                    Task.Run(() => taskCompletionSource.TrySetException(ex));
                    continue;
                }
                finally
                {
                    RemoveSynced(orderQueueNode, () => { });
                    orderQueueNode.Value.CompletionSource.TrySetResult(true);
                }
            }
        }

        private async Task<bool> TryPublishAsync(
            int attempt,
            string exchange,
            string routingKey,
            ReadOnlyMemory<byte> body,
            IBasicProperties properties,
            CancellationToken cancellationToken)
        {
            try
            {
                return await _decorated.PublishUnsafeAsync(exchange, routingKey, body, properties, cancellationToken);
            }
            catch (AlreadyClosedException ex)
            {
                return false;
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        public Task<RetryingPublisherResult> PublishUnsafeAsync(
            string exchange,
            string routingKey,
            ReadOnlyMemory<byte> body,
            IBasicProperties properties,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var taskCompletionSource = new TaskCompletionSource<RetryingPublisherResult>();
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

            // TrackSafe(_diagnostics.TrackPublishTaskEnqueued,
            //     new PublishArgs(exchange, routingKey, body, properties),
            //     new QueueBasedAsyncPublisherStatus(_publishQueueSize, _ackQueueSize, _completionSourceRegistrySize));
            _publishEvent.SetAsync();

            return taskCompletionSource.Task;
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

        private LinkedListNode<QueueEntry> AddLastSynced(QueueEntry queueEntry)
        {
            lock (_orderQueue)
            {
                return _orderQueue.AddLast(queueEntry);
            }
        }

        private LinkedListNode<QueueEntry> GetFirstSynced()
        {
            lock (_orderQueue)
            {
                return _orderQueue.First;
            }
        }

        private void RemoveSynced(LinkedListNode<QueueEntry> node, Action onEmpty)
        {
            lock (_orderQueue)
            {
                _orderQueue.Remove(node);

                if (_orderQueue.Count == 0)
                {
                    onEmpty();
                }
            }
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
                _publishLoop.Wait();
                return;
            }

            // TrackSafe(_diagnostics.TrackDisposeStarted);
            // var stopwatch = Stopwatch.StartNew();

            Task.WaitAll(_publishEvent.SetAsync(), _publishLoop);

            // TODO: Replace with "disposing" remaining _orderQueue items
            // foreach (var source in RemoveAllTaskCompletionSourcesUpTo(ulong.MaxValue))
            // {
            // Task.Run(() => source.TrySetException(new ObjectDisposedException(nameof(QueueBasedAsyncPublisher))));
            // }

            // TrackSafe(_diagnostics.TrackDisposeSucceeded, stopwatch.Elapsed);
        }

        private class QueueEntry
        {
            public TaskCompletionSource<bool> CompletionSource { get; } = new TaskCompletionSource<bool>();
        }

        private class PublishQueueItem
        {
            public string Exchange { get; }
            public string RoutingKey { get; }
            public ReadOnlyMemory<byte> Body { get; }
            public IBasicProperties Properties { get; }
            public CancellationToken CancellationToken { get; }
            public TaskCompletionSource<RetryingPublisherResult> TaskCompletionSource { get; }

            public PublishQueueItem(string exchange, string routingKey, ReadOnlyMemory<byte> body,
                IBasicProperties properties, CancellationToken cancellationToken,
                TaskCompletionSource<RetryingPublisherResult> taskCompletionSource)
            {
                Exchange = exchange;
                RoutingKey = routingKey;
                Body = body;
                Properties = properties;
                CancellationToken = cancellationToken;
                TaskCompletionSource = taskCompletionSource;
            }
        }
    }
}