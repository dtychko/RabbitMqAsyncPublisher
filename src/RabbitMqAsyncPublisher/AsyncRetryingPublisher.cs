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

    public class AsyncRetryingPublisher : IAsyncPublisher<RetryingPublisherResult>
    {
        private readonly AsyncPublisher _decorated;
        private readonly LinkedList<QueueEntry> _queue = new LinkedList<QueueEntry>();
        private readonly SemaphoreSlim _publishSemaphore = new SemaphoreSlim(1, 1);
        private readonly ManualResetEventSlim _canPublish = new ManualResetEventSlim(true);

        public IModel Model => _decorated.Model;

        public AsyncRetryingPublisher(AsyncPublisher decorated)
        {
            _decorated = decorated;
        }

        public async Task<RetryingPublisherResult> PublishAsync(
            string exchange,
            string routingKey,
            ReadOnlyMemory<byte> body,
            CancellationToken cancellationToken)
        {
            // TODO: Think about moving "publish lock" to outer decorator
            await _publishSemaphore.WaitAsync(cancellationToken);
            _canPublish.Wait(cancellationToken);

            // TODO: Think about replacing "QueueEntry" with "TCS"
            var queueNode = AddLastSynced(new QueueEntry());
            Task<bool> publishTask = null;

            try
            {
                publishTask = _decorated.PublishAsync(exchange, routingKey, body, cancellationToken);
                _publishSemaphore.Release();

                // TODO: Treat "false" results as an exception
                await publishTask;

                return RetryingPublisherResult.NoReties;
            }
            catch (OperationCanceledException)
            {
                // ? Move to "finally"
                if (publishTask is null)
                {
                    _publishSemaphore.Release();
                }

                // Should rethrow?
                throw;
            }
            catch (Exception)
            {
                _canPublish.Reset();
                if (publishTask is null)
                {
                    _publishSemaphore.Release();
                }

                var retries = await RetryAsync(queueNode, exchange, routingKey, body, cancellationToken);
                return new RetryingPublisherResult(retries);
            }
            finally
            {
                var queueCount = RemoveSynced(queueNode);
                queueNode.Value.CompletionSource.TrySetResult(true);

                if (queueCount == 0)
                {
                    _canPublish.Set();
                }
            }
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
                try
                {
                    retries += 1;

                    // TODO: Treat "false" results as an exception
                    await _decorated.PublishAsync(exchange, routingKey, body, cancellationToken);

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