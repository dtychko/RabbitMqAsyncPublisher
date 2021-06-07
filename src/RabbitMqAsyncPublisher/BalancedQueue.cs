using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using static RabbitMqAsyncPublisher.AsyncPublisherUtils;

namespace RabbitMqAsyncPublisher
{
    public class BalancedQueue<TValue> : IBalancedQueue<TValue>
    {
        private readonly int _partitionProcessingLimit;
        private readonly IUnexpectedExceptionDiagnostics _diagnostics;

        private readonly ConcurrentDictionary<string, Partition> _partitionRegistry =
            new ConcurrentDictionary<string, Partition>();

        private readonly ConcurrentQueue<Partition> _partitionQueue = new ConcurrentQueue<Partition>();

        private readonly LinkedListQueue<TaskCompletionSource<(TValue, Partition)>> _waiterQueue =
            new LinkedListQueue<TaskCompletionSource<(TValue, Partition)>>();

        private readonly object _gateEventSyncRoot = new object();
        private readonly object _enqueuePartitionSyncRoot = new object();

        private volatile int _partitionRegistryCount;
        private volatile int _partitionQueueCount;

        private AsyncManualResetEvent _gateEvent;

        public int PartitionCount => _partitionRegistryCount;

        public BalancedQueue(int partitionProcessingLimit, IUnexpectedExceptionDiagnostics diagnostics = null)
        {
            _partitionProcessingLimit = partitionProcessingLimit;
            _diagnostics = diagnostics ?? UnexpectedExceptionDiagnostics.NoDiagnostics;
        }

        public void Enqueue(string partitionKey, TValue value)
        {
            while (true)
            {
                var partition = _partitionRegistry.GetOrAdd(partitionKey, _ => new Partition(partitionKey));
                bool shouldEnqueuePartition;

                lock (partition)
                {
                    if (partition.IsRemoved)
                    {
                        continue;
                    }

                    if (partition.IsNew)
                    {
                        partition.IsNew = false;
                        Interlocked.Increment(ref _partitionRegistryCount);
                    }

                    partition.Queue.Enqueue(value);
                    shouldEnqueuePartition =
                        partition.Queue.Count == 1 && partition.ProcessingCount < _partitionProcessingLimit;
                }

                if (shouldEnqueuePartition)
                {
                    EnqueuePartition(partition);
                }

                return;
            }
        }

        public async Task WaitToDequeueAsync(CancellationToken cancellationToken = default)
        {
            if (_gateEvent is null)
            {
                lock (_gateEventSyncRoot)
                {
                    if (_gateEvent is null)
                    {
                        _gateEvent = new AsyncManualResetEvent(false);
                        AdjustGate();
                    }
                }
            }

            await _gateEvent.WaitAsync(cancellationToken).ConfigureAwait(false);
        }

        public async Task<Func<Func<TValue, string, Task>, Task>> DequeueAsync(
            CancellationToken cancellationToken = default)
        {
            if (TryDequeue(out var handler))
            {
                return handler;
            }

            TaskCompletionSource<(TValue, Partition)> waiter;
            Func<bool> tryRemoveWaiter;

            lock (_enqueuePartitionSyncRoot)
            {
                if (TryDequeue(out handler))
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
            throw new NotImplementedException();
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

        public bool TryDequeue(out Func<Func<TValue, string, Task>, Task> handler)
        {
            if (!_partitionQueue.TryDequeue(out var partition))
            {
                // TODO: spin and try again if any partition is "in progress" now 
                handler = default;
                return false;
            }

            Interlocked.Decrement(ref _partitionQueueCount);
            AdjustGate();

            TValue value;
            bool shouldEnqueuePartition;

            lock (partition)
            {
                value = partition.Queue.Dequeue();
                partition.ProcessingCount += 1;
                shouldEnqueuePartition =
                    partition.Queue.Count > 0 && partition.ProcessingCount < _partitionProcessingLimit;
            }

            if (shouldEnqueuePartition)
            {
                EnqueuePartition(partition);
            }

            handler = CreateValueHandler(value, partition);
            return true;
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
                TrackSafe(_diagnostics.TrackUnexpectedException,
                    $"Unable to handle a value in partition '{partition.Name}'", ex);
            }

            bool shouldEnqueuePartition;

            lock (partition)
            {
                partition.ProcessingCount -= 1;

                if (partition.Queue.Count == 0 && partition.ProcessingCount == 0)
                {
                    partition.IsRemoved = true;
                    _partitionRegistry.TryRemove(partition.Name, out _);
                    Interlocked.Decrement(ref _partitionRegistryCount);

                    return;
                }

                shouldEnqueuePartition = partition.Queue.Count > 0 &&
                                         partition.ProcessingCount == _partitionProcessingLimit - 1;
            }

            if (shouldEnqueuePartition)
            {
                EnqueuePartition(partition);
            }
        }

        private void EnqueuePartition(Partition partition)
        {
            lock (_enqueuePartitionSyncRoot)
            {
                if (_waiterQueue.TryDequeue(out var waiter))
                {
                    TValue value;
                    bool shouldEnqueuePartition;

                    lock (partition)
                    {
                        value = partition.Queue.Dequeue();
                        partition.ProcessingCount += 1;
                        shouldEnqueuePartition =
                            partition.Queue.Count > 0 && partition.ProcessingCount < _partitionProcessingLimit;
                    }

                    Task.Run(() =>
                    {
                        if (shouldEnqueuePartition)
                        {
                            EnqueuePartition(partition);
                        }

                        waiter.SetResult((value, partition));
                    });

                    return;
                }

                _partitionQueue.Enqueue(partition);
                Interlocked.Increment(ref _partitionQueueCount);
                AdjustGate();
            }
        }

        private void AdjustGate()
        {
            if (_gateEvent is null)
            {
                return;
            }

            lock (_gateEventSyncRoot)
            {
                if (_partitionQueueCount > 0)
                {
                    _gateEvent.Set();
                }
                else
                {
                    _gateEvent.Reset();
                }
            }
        }

        private class Partition
        {
            public readonly string Name;
            public readonly Queue<TValue> Queue;
            public int ProcessingCount;
            public bool IsNew;
            public bool IsRemoved;

            public Partition(string name)
            {
                Name = name;
                Queue = new Queue<TValue>();
                IsNew = true;
            }
        }
    }
}