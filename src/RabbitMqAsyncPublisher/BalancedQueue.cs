using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using static RabbitMqAsyncPublisher.AsyncPublisherUtils;

namespace RabbitMqAsyncPublisher
{
    public interface IBalancedQueue<TValue>
    {
        void Enqueue(string partitionKey, TValue value);

        bool TryDequeue(out Func<Func<TValue, string, Task>, Task> handler);

        Task<Func<Func<TValue, string, Task>, Task>> DequeueAsync(CancellationToken cancellationToken = default);

        bool TryComplete(Exception ex);
    }

    public class BalancedQueue<TValue> : IBalancedQueue<TValue>
    {
        private readonly int _partitionProcessingLimit;
        private readonly IUnexpectedExceptionDiagnostics _diagnostics;

        private readonly ConcurrentDictionary<string, Partition> _partitionRegistry =
            new ConcurrentDictionary<string, Partition>();

        private readonly ConcurrentQueue<Partition> _partitionQueue = new ConcurrentQueue<Partition>();

        private readonly LinkedListQueue<TaskCompletionSource<(TValue, Partition)>> _waiterQueue =
            new LinkedListQueue<TaskCompletionSource<(TValue, Partition)>>();

        private readonly object _syncRoot = new object();

        private Exception _completionException;

        private volatile int _valueCount;
        private volatile int _partitionCount;
        private volatile int _partitionQueueCount;

        public BalancedQueueStatus Status =>
            new BalancedQueueStatus(_valueCount, _partitionCount, _partitionQueueCount);

        public BalancedQueue(int partitionProcessingLimit, IUnexpectedExceptionDiagnostics diagnostics = null)
        {
            _partitionProcessingLimit = partitionProcessingLimit;
            _diagnostics = diagnostics;
        }

        public void Enqueue(string partitionKey, TValue value)
        {
            if (partitionKey is null)
            {
                throw new ArgumentNullException(nameof(partitionKey));
            }

            lock (_syncRoot)
            {
                if (IsEnqueueCompleted)
                {
                    throw new BalancedQueueCompletedException();
                }

                EnqueueValue(
                    GetPartition(partitionKey),
                    value
                );
            }
        }

        public bool TryDequeue(out Func<Func<TValue, string, Task>, Task> handler)
        {
            lock (_syncRoot)
            {
                if (!TryDequeuePartition(out var partition))
                {
                    handler = default;
                    return false;
                }

                var value = DequeueValue(partition);

                if (partition.Queue.Count > 0 && partition.ProcessingCount < _partitionProcessingLimit)
                {
                    // Partition could be enqueued back to the partition queue
                    // if it still contains some values and its processing limit wasn't reached yet
                    EnqueuePartition(partition);
                }

                handler = CreateValueHandler(value, partition);
                return true;
            }
        }

        public async Task<Func<Func<TValue, string, Task>, Task>> DequeueAsync(
            CancellationToken cancellationToken = default)
        {
            TaskCompletionSource<(TValue, Partition)> waiter;
            Func<bool> tryRemoveWaiter;

            lock (_syncRoot)
            {
                if (IsDequeueCompleted)
                {
                    throw new BalancedQueueCompletedException();
                }

                if (TryDequeue(out var handler))
                {
                    return handler;
                }

                waiter = new TaskCompletionSource<(TValue, Partition)>();
                tryRemoveWaiter = _waiterQueue.Enqueue(waiter);
            }

            var (value, partition) = await WaitForCompletedOrCancelled(waiter.Task, tryRemoveWaiter, cancellationToken)
                .ConfigureAwait(false);
            return CreateValueHandler(value, partition);
        }

        public bool TryComplete(Exception ex)
        {
            if (ex is null)
            {
                throw new ArgumentNullException(nameof(ex));
            }

            lock (_syncRoot)
            {
                if (!(_completionException is null))
                {
                    return false;
                }

                _completionException = ex;

                if (IsDequeueCompleted)
                {
                    // There is no more values to dequeue, all active waiters should be rejected immediately
                    ScheduleRejectAllWaiters();
                }

                return true;
            }
        }

        private static async Task<TResult> WaitForCompletedOrCancelled<TResult>(Task<TResult> waiterTask,
            Func<bool> tryRemoveWaiterTask, CancellationToken cancellationToken)
        {
            if (cancellationToken.CanBeCanceled)
            {
                var completedTask = await Task.WhenAny(
                    waiterTask,
                    Task.Delay(-1, cancellationToken)
                ).ConfigureAwait(false);

                if (completedTask != waiterTask && tryRemoveWaiterTask())
                {
                    cancellationToken.ThrowIfCancellationRequested();
                }
            }

            return await waiterTask.ConfigureAwait(false);
        }

        private Func<Func<TValue, string, Task>, Task> CreateValueHandler(TValue value, Partition partition)
        {
            return handle => HandleSafe(handle, value, partition);
        }

        private async Task HandleSafe(Func<TValue, string, Task> handle, TValue value, Partition partition)
        {
            try
            {
                await handle(value, partition.Name).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                if (!(_diagnostics is null))
                {
                    TrackSafe(_diagnostics.TrackUnexpectedException,
                        $"Unable to handle a value in partition '{partition.Name}'", ex);
                }
            }

            lock (_syncRoot)
            {
                CompleteValueProcessing(partition);

                if (partition.Queue.Count > 0 && partition.ProcessingCount == _partitionProcessingLimit - 1)
                {
                    // Partition could be enqueued to the partition queue
                    // because it has some values for processing and its processing counter is under limit again
                    EnqueuePartition(partition);
                }
            }
        }

        private Partition GetPartition(string partitionKey)
        {
            var partition = _partitionRegistry.GetOrAdd(partitionKey, _ => new Partition(partitionKey));

            if (partition.IsEmpty)
            {
                // New empty partition is created
                Interlocked.Increment(ref _partitionCount);
            }

            return partition;
        }

        private void EnqueuePartition(Partition partition)
        {
            if (_waiterQueue.TryDequeue(out var waiter))
            {
                var value = DequeueValue(partition);
                ScheduleTrySetResult(waiter, (value, partition));
                return;
            }

            _partitionQueue.Enqueue(partition);
            Interlocked.Increment(ref _partitionQueueCount);
        }

        private bool TryDequeuePartition(out Partition partition)
        {
            if (!_partitionQueue.TryDequeue(out partition))
            {
                return false;
            }

            Interlocked.Decrement(ref _partitionQueueCount);
            return true;
        }

        private void EnqueueValue(Partition partition, TValue value)
        {
            partition.Queue.Enqueue(value);
            Interlocked.Increment(ref _valueCount);

            if (partition.Queue.Count == 1 && partition.ProcessingCount < _partitionProcessingLimit)
            {
                // The first value was added to the partition.
                // It could be enqueued to the partition queue if its processing limit wasn't reached yet. 
                EnqueuePartition(partition);
            }
        }

        private TValue DequeueValue(Partition partition)
        {
            var value = partition.Queue.Dequeue();
            partition.ProcessingCount += 1;
            Interlocked.Decrement(ref _valueCount);

            if (IsDequeueCompleted)
            {
                // If queue is already completed and empty then reject all active waiters immediately
                // because no more values will be enqueued
                ScheduleRejectAllWaiters();
            }

            return value;
        }

        private void ScheduleRejectAllWaiters()
        {
            if (_waiterQueue.Size == 0)
            {
                return;
            }

            var waiters = new List<TaskCompletionSource<(TValue, Partition)>>();

            while (_waiterQueue.TryDequeue(out var waiter))
            {
                waiters.Add(waiter);
            }

            Task.Run(() =>
            {
                foreach (var waiter in waiters)
                {
                    waiter.SetException(_completionException);
                }
            });
        }

        private void CompleteValueProcessing(Partition partition)
        {
            partition.ProcessingCount -= 1;

            if (partition.IsEmpty)
            {
                _partitionRegistry.TryRemove(partition.Name, out _);
                Interlocked.Decrement(ref _partitionCount);
            }
        }

        private bool IsEnqueueCompleted => !(_completionException is null);

        private bool IsDequeueCompleted => !(_completionException is null) && _valueCount == 0;

        private class Partition
        {
            public readonly string Name;
            public readonly Queue<TValue> Queue;
            public int ProcessingCount;

            public bool IsEmpty => Queue.Count == 0 && ProcessingCount == 0;

            public Partition(string name)
            {
                Name = name;
                Queue = new Queue<TValue>();
            }
        }
    }

    public class BalancedQueueCompletedException : Exception
    {
        public BalancedQueueCompletedException()
            : base("BalancedQueue is already completed")
        {
        }
    }

    public struct BalancedQueueStatus
    {
        public readonly int ValueCount;
        public readonly int PartitionCount;
        public readonly int ReadyPartitionCount;

        public BalancedQueueStatus(int valueCount, int partitionCount, int readyPartitionCount)
        {
            ValueCount = valueCount;
            PartitionCount = partitionCount;
            ReadyPartitionCount = readyPartitionCount;
        }

        public override string ToString()
        {
            return $"{nameof(ValueCount)}={ValueCount}; " +
                   $"{nameof(PartitionCount)}={PartitionCount}; " +
                   $"{nameof(ReadyPartitionCount)}={ReadyPartitionCount}";
        }
    }
}