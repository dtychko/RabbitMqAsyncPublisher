using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMqAsyncPublisher
{
    public class AsyncPublisher2Decorator
    {
        private readonly AsyncPublisher2 _decorated;

        public AsyncPublisher2Decorator(AsyncPublisher2 decorated)
        {
            _decorated = decorated;
        }

        private readonly LinkedList<QueueEntry> _queue = new LinkedList<QueueEntry>();
        private readonly SemaphoreSlim _publishSemaphore = new SemaphoreSlim(1, 1);
        private readonly ManualResetEventSlim _canPublish = new ManualResetEventSlim(true);

        public async Task PublishAsync(ReadOnlyMemory<byte> message)
        {
            await _publishSemaphore.WaitAsync();
            _canPublish.Wait();

            var queueNode = AddLastSynced(new QueueEntry {Message = message});
            Task<bool> publishTask = null;

            try
            {
                publishTask = _decorated.PublishAsync(message);
                _publishSemaphore.Release();

                // TODO: Treat "false" results as an exception
                await publishTask;
            }
            catch (Exception)
            {
                _canPublish.Reset();
                if (publishTask is null)
                {
                    _publishSemaphore.Release();
                }

                await RetryAsync(queueNode);
            }
        }

        private async Task RetryAsync(LinkedListNode<QueueEntry> queueNode)
        {
            // Start retries:
            //  1. Peek entry from the queue
            //  2. If not the current entry then wait for the entry event
            //  3. Retry in a loop with timeout
            //  4. If it was the last entry in the queue then move publisher to Open state

            LinkedListNode<QueueEntry> nextQueueNode;

            while ((nextQueueNode = GetFirstSynced()) != queueNode)
            {
                await nextQueueNode.Value.CompletionSource.Task;
            }

            while (true)
            {
                try
                {
                    await _decorated.PublishAsync(queueNode.Value.Message);
                    var queueCount = RemoveSynced(queueNode);

                    queueNode.Value.CompletionSource.TrySetResult(true);

                    if (queueCount == 0)
                    {
                        _canPublish.Set();
                    }

                    return;
                }
                catch (Exception)
                {
                    Thread.Sleep(1000);
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
            public ReadOnlyMemory<byte> Message { get; set; }

            public TaskCompletionSource<bool> CompletionSource { get; } = new TaskCompletionSource<bool>();
        }
    }
}