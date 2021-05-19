using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace RabbitMqAsyncPublisher
{
    public class AsyncPublisherWithRetries : IAsyncPublisher<RetryingPublisherResult>
    {
        private readonly IAsyncPublisher<bool> _decorated;
        private readonly TimeSpan _retryDelay;
        private volatile bool _isDisposed;
        private readonly LinkedList<QueueEntry> _queue = new LinkedList<QueueEntry>();
        private readonly ManualResetEventSlim _canPublish = new ManualResetEventSlim(true);

        public IModel Model => _decorated.Model;

        public AsyncPublisherWithRetries(IAsyncPublisher<bool> decorated, TimeSpan retryDelay)
        {
            _decorated = decorated;
            _retryDelay = retryDelay;
        }

        public async Task<RetryingPublisherResult> PublishUnsafeAsync(
            string exchange,
            string routingKey,
            ReadOnlyMemory<byte> body,
            IBasicProperties properties,
            CancellationToken cancellationToken)
        {
            ThrowIfDisposed();

            _canPublish.Wait(cancellationToken);

            using (StartPublishing(out var queueNode))
            {
                if (await TryPublishAsync(1, exchange, routingKey, body, properties, cancellationToken))
                {
                    return RetryingPublisherResult.NoRetries;
                }

                _canPublish.Reset();
                return await RetryAsync(queueNode, exchange, routingKey, body, properties, cancellationToken);
            }
        }

        private IDisposable StartPublishing(out LinkedListNode<QueueEntry> queueNode)
        {
            var addedQueueNode = AddLastSynced(new QueueEntry());
            queueNode = addedQueueNode;

            return new Disposable(() =>
            {
                RemoveSynced(addedQueueNode, () =>
                {
                    if (!_canPublish.IsSet)
                    {
                        _canPublish.Set();
                    }
                });
                addedQueueNode.Value.CompletionSource.TrySetResult(true);
            });
        }

        private async Task<RetryingPublisherResult> RetryAsync(
            LinkedListNode<QueueEntry> queueNode,
            string exchange,
            string routingKey,
            ReadOnlyMemory<byte> body,
            IBasicProperties properties,
            CancellationToken cancellationToken)
        {
            LinkedListNode<QueueEntry> nextQueueNode;

            while ((nextQueueNode = GetFirstSynced()) != queueNode)
            {
                ThrowIfDisposed();

                await Task.WhenAny(
                    Task.Delay(-1, cancellationToken),
                    nextQueueNode.Value.CompletionSource.Task
                );
            }

            for (var attempt = 2;; attempt++)
            {
                ThrowIfDisposed();

                await Task.Delay(_retryDelay, cancellationToken);

                if (await TryPublishAsync(attempt, exchange, routingKey, body, properties, cancellationToken))
                {
                    return new RetryingPublisherResult(attempt - 1);
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
            catch (AlreadyClosedException)
            {
                // TODO: Use callback to determine if publish should be retried
                return false;
            }
        }

        private LinkedListNode<QueueEntry> AddLastSynced(QueueEntry queueEntry)
        {
            lock (_queue)
            {
                return _queue.AddLast(queueEntry);
            }
        }

        private LinkedListNode<QueueEntry> GetFirstSynced()
        {
            lock (_queue)
            {
                return _queue.First;
            }
        }

        private void RemoveSynced(LinkedListNode<QueueEntry> node, Action onEmpty)
        {
            lock (_queue)
            {
                _queue.Remove(node);

                if (_queue.Count == 0)
                {
                    onEmpty();
                }
            }
        }

        public void Dispose()
        {
            _decorated.Dispose();
            _isDisposed = true;

            _canPublish.Dispose();
            _canPublish.Set();
        }

        private void ThrowIfDisposed()
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException(nameof(AsyncPublisherWithRetries));
            }
        }

        private class QueueEntry
        {
            public TaskCompletionSource<bool> CompletionSource { get; } = new TaskCompletionSource<bool>();
        }
    }

    public readonly struct RetryingPublisherResult
    {
        public static readonly RetryingPublisherResult NoRetries = new RetryingPublisherResult(0);

        public int Retries { get; }

        public RetryingPublisherResult(int retries)
        {
            Retries = retries;
        }
    }
}