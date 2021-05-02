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

        private class QueueEntry
        {
            public ReadOnlyMemory<byte> Message { get; set; }

            public TaskCompletionSource<bool> CompletionSource { get; } = new TaskCompletionSource<bool>();
        }

        private async Task RetryAsync(LinkedListNode<QueueEntry> queueNode)
        {
            // Start retries:
            //  1. Peek entry from the queue
            //  2. If not the current entry then wait for the entry event
            //  3. Retry in a loop with timeout
            //  4. If it was the last entry in the queue then move publisher to Open state

            while (_queue.First != queueNode)
            {
                await _queue.First.Value.CompletionSource.Task;
            }

            while (true)
            {
                try
                {
                    await _decorated.PublishAsync(queueNode.Value.Message);
                    _queue.Remove(queueNode);
                    queueNode.Value.CompletionSource.TrySetResult(true);

                    if (_queue.Count == 0)
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

        private readonly SemaphoreSlim _publishSemaphore = new SemaphoreSlim(1, 1);
        private readonly ManualResetEventSlim _canPublish = new ManualResetEventSlim(true);

        public async Task PublishAsync(ReadOnlyMemory<byte> message)
        {
            await _publishSemaphore.WaitAsync();
            _canPublish.Wait();

            var queueNode = _queue.AddLast(new QueueEntry {Message = message});
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

            // try
            // {
            //     publishTask = _decorated.PublishAsync(message);
            // }
            // catch (Exception)
            // {
            //     await RetryAsync(queueNode);
            //     return;
            // }
            // finally
            // {
            //     _publishSemaphore.Release();
            // }
            //
            // try
            // {
            //     await publishTask;
            //     _queue.Remove(queueNode);
            //     queueNode.Value.CompletionSource.TrySetResult(true);
            // }
            // catch (Exception)
            // {
            //     await RetryAsync(queueNode);
            // }
        }

        // public async Task PublishAsync(ReadOnlyMemory<byte> message)
        // {
        //     Task<bool> result;
        //
        //     // Publish lock
        //     lock (_decorated)
        //     {
        //         // Push a new entry to the queue
        //         
        //         do
        //         {
        //             try
        //             {
        //                 // ? ClosedState should always throw
        //                 result = _decorated.PublishAsync(message);
        //                 break;
        //             }
        //             catch
        //             {
        //                 // Start retries:
        //                 //  1. ...
        //                 //  ...
        //                 
        //                 Thread.Sleep(1000);
        //             }
        //         } while (true);
        //     }
        //
        //     do
        //     {
        //         try
        //         {
        //             await result;
        //             
        //             // Remove entry from the queue
        //             // Set the entry event
        //
        //             break;
        //         }
        //         catch (Exception ex)
        //         {
        //             // Start retries:
        //             //  1. Peek entry from the queue
        //             //  2. If not the current entry then wait for the entry event
        //             //  3. Retry in a loop with timeout
        //             //  4. If it was the last entry in the queue then move publisher to Open state
        //             
        //             // ?Move to closed state
        //
        //             // Retry lock
        //             lock (result)
        //             {
        //                 result = _decorated.PublishAsync(message);
        //             }
        //         }
        //     } while (true);
        // }
    }
}