using System;
using System.Collections.Generic;
using System.Diagnostics;
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
        private readonly IAsyncPublisherWithRetriesDiagnostics _diagnostics;
        private readonly LinkedList<QueueEntry> _queue = new LinkedList<QueueEntry>();
        private readonly ManualResetEventSlim _canPublish = new ManualResetEventSlim(true);
        private bool _isDisposed;

        public IModel Model => _decorated.Model;

        public AsyncPublisherWithRetries(
            IAsyncPublisher<bool> decorated,
            TimeSpan retryDelay)
            : this(decorated, retryDelay, EmptyDiagnostics.Instance)
        {
        }

        public AsyncPublisherWithRetries(
            IAsyncPublisher<bool> decorated,
            TimeSpan retryDelay,
            IAsyncPublisherWithRetriesDiagnostics diagnostics)
        {
            _decorated = decorated;
            _retryDelay = retryDelay;
            _diagnostics = diagnostics;
        }

        public async Task<RetryingPublisherResult> PublishUnsafeAsync(
            string exchange,
            string routingKey,
            ReadOnlyMemory<byte> body,
            IBasicProperties properties,
            CancellationToken cancellationToken)
        {
            ThrowIfDisposed();

            if (!_canPublish.IsSet)
            {
                _diagnostics.TrackCanPublishWait(new PublishArgs(exchange, routingKey, body,
                    properties));
            }

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

                Task.Run(() => addedQueueNode.Value.CompletionSource.TrySetResult(true));
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

                _diagnostics.TrackRetryDelay(
                    new PublishUnsafeAttemptArgs(exchange, routingKey, body, properties, attempt),
                    _retryDelay
                );

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
            var args = new PublishUnsafeAttemptArgs(exchange, routingKey, body, properties, attempt);
            _diagnostics.TrackPublishUnsafeAttempt(args);
            var stopwatch = Stopwatch.StartNew();

            try
            {
                var acknowledged =
                    await _decorated.PublishUnsafeAsync(exchange, routingKey, body, properties, cancellationToken);
                _diagnostics.TrackPublishUnsafeAttemptCompleted(args, stopwatch.Elapsed, acknowledged);
                return acknowledged;
            }
            catch (AlreadyClosedException ex)
            {
                _diagnostics.TrackPublishUnsafeAttemptFailed(args, stopwatch.Elapsed, ex);

                // TODO: Use callback to determine if publish should be retried
                return false;
            }
            catch (Exception ex)
            {
                _diagnostics.TrackPublishUnsafeAttemptFailed(args, stopwatch.Elapsed, ex);

                throw;
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