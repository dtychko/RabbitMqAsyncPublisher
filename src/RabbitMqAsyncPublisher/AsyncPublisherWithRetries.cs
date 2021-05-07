using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace RabbitMqAsyncPublisher
{
    public readonly struct RetryingPublisherResult
    {
        public static readonly RetryingPublisherResult NoReties = new RetryingPublisherResult(0);

        public int Retries { get; }

        public RetryingPublisherResult(int retries)
        {
            Retries = retries;
        }
    }

    public class AsyncPublisherWithRetries : IAsyncPublisher<RetryingPublisherResult>
    {
        private readonly AsyncPublisher _decorated;
        private readonly LinkedList<QueueEntry> _queue = new LinkedList<QueueEntry>();
        private readonly ManualResetEventSlim _canPublish = new ManualResetEventSlim(true);

        public IModel Model => _decorated.Model;

        public AsyncPublisherWithRetries(AsyncPublisher decorated)
        {
            _decorated = decorated;
        }

        public async Task<RetryingPublisherResult> PublishAsync(
            string exchange,
            string routingKey,
            ReadOnlyMemory<byte> body,
            CancellationToken cancellationToken)
        {
            _canPublish.Wait(cancellationToken);

            // TODO: Think about replacing "QueueEntry" with "TCS"
            var queueNode = AddLastSynced(new QueueEntry());

            try
            {
                // TODO: Treat "false" results as an exception
                await _decorated.PublishAsync(exchange, routingKey, body, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                // Should rethrow?
                throw;
            }
            catch (Exception)
            {
                // TODO: Use callback to determine if publish should be retried
                _canPublish.Reset();
                var retries = await RetryAsync(queueNode, exchange, routingKey, body, cancellationToken);
                return new RetryingPublisherResult(retries);
            }

            RemoveSynced(queueNode);
            return RetryingPublisherResult.NoReties;
        }

        private async Task<int> RetryAsync(
            LinkedListNode<QueueEntry> queueNode,
            string exchange,
            string routingKey,
            ReadOnlyMemory<byte> body,
            CancellationToken cancellationToken)
        {
            LinkedListNode<QueueEntry> nextQueueNode;

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
                    await _decorated.PublishAsync(exchange, routingKey, body, cancellationToken);

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
                catch (Exception)
                {
                    await Task.Delay(1000, cancellationToken);
                }
            }
        }

        public void Dispose()
        {
            _decorated.Dispose();
            throw new NotImplementedException();
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