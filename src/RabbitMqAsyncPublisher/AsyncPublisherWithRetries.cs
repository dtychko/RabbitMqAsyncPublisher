using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace RabbitMqAsyncPublisher
{
    public readonly struct RetryingPublisherResult
    {
        public static readonly RetryingPublisherResult NoRetries = new RetryingPublisherResult(0);

        public int Retries { get; }

        public RetryingPublisherResult(int retries)
        {
            Retries = retries;
        }
    }

    public class AsyncPublisherWithRetries : IAsyncPublisher<RetryingPublisherResult>
    {
        private readonly AsyncPublisher _decorated;
        private readonly TimeSpan _retryDelay;
        private volatile bool _isDisposed;
        private readonly LinkedList<QueueEntry> _queue = new LinkedList<QueueEntry>();
        private readonly ManualResetEventSlim _canPublish = new ManualResetEventSlim(true);

        public IModel Model => _decorated.Model;

        public AsyncPublisherWithRetries(AsyncPublisher decorated, TimeSpan retryDelay)
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
            _canPublish.Wait(cancellationToken);
            ThrowIfDisposed();

            // TODO: Think about replacing "QueueEntry" with "TCS"
            var queueNode = AddLastSynced(new QueueEntry());

            try
            {
                // TODO: Treat "false" results as an exception
                await _decorated.PublishUnsafeAsync(exchange, routingKey, body, properties, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                RemoveSynced(queueNode);
                queueNode.Value.CompletionSource.TrySetResult(true);

                throw;
            }
            // TODO: handle only RabbitMq.Client related exceptions
            catch (Exception ex)
            {
                Console.WriteLine($"WithRetries: caught exception {ex}");
                // TODO: Use callback to determine if publish should be retried
                _canPublish.Reset();
                var retries = await RetryAsync(queueNode, exchange, routingKey, body, properties, cancellationToken);
                return new RetryingPublisherResult(retries);
            }

            RemoveSynced(queueNode);
            queueNode.Value.CompletionSource.TrySetResult(true);

            return RetryingPublisherResult.NoRetries;
        }

        private async Task<int> RetryAsync(
            LinkedListNode<QueueEntry> queueNode,
            string exchange,
            string routingKey,
            ReadOnlyMemory<byte> body,
            IBasicProperties properties,
            CancellationToken cancellationToken)
        {
            LinkedListNode<QueueEntry> nextQueueNode;

            // TODO: catch cancellation and remove queueNode and set result
            while ((nextQueueNode = GetFirstSynced()) != queueNode)
            {
                await Task.WhenAny(
                    Task.Delay(-1, cancellationToken),
                    nextQueueNode.Value.CompletionSource.Task
                );
            }

            var retries = 0;

            while (true)
            {
                retries += 1;

                try
                {
                    // TODO: Treat "false" results as an exception
                    await _decorated.PublishUnsafeAsync(exchange, routingKey, body, properties, cancellationToken);

                    var queueCount = RemoveSynced(queueNode);
                    queueNode.Value.CompletionSource.TrySetResult(true);

                    if (queueCount == 0)
                    {
                        _canPublish.Set();
                    }

                    return retries;
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                // TODO: handle only RabbitMq.Client related exceptions
                catch (Exception)
                {
                    await Task.Delay(_retryDelay, cancellationToken);
                }
            }
        }

        public void Dispose()
        {
            _isDisposed = true;
            _decorated.Dispose();
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

        private int RemoveSynced(LinkedListNode<QueueEntry> node)
        {
            lock (_queue)
            {
                _queue.Remove(node);
                return _queue.Count;
            }
        }

        private class QueueEntry
        {
            public TaskCompletionSource<bool> CompletionSource { get; } = new TaskCompletionSource<bool>();
        }
    }
}