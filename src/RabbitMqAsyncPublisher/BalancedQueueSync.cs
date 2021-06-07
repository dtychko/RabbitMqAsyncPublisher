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

    public class UnexpectedExceptionDiagnostics : IUnexpectedExceptionDiagnostics
    {
        public static readonly IUnexpectedExceptionDiagnostics NoDiagnostics = new UnexpectedExceptionDiagnostics();

        protected UnexpectedExceptionDiagnostics()
        {
        }

        public virtual void TrackUnexpectedException(string message, Exception ex)
        {
        }
    }

    public class BalancedQueueSync<TValue> : IBalancedQueue<TValue>
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

        public int ValueCount => _valueCount;

        public int PartitionCount => _partitionCount;

        public int ReadyPartitionCount => _partitionQueueCount;

        public BalancedQueueSync(int partitionProcessingLimit, IUnexpectedExceptionDiagnostics diagnostics = null)
        {
            _partitionProcessingLimit = partitionProcessingLimit;
            _diagnostics = diagnostics ?? UnexpectedExceptionDiagnostics.NoDiagnostics;
        }

        public void Enqueue(string partitionKey, TValue value)
        {
            lock (_syncRoot)
            {
                if (!(_completionException is null))
                {
                    throw new Exception("BalancedQueue is already completed");
                }

                var partition = _partitionRegistry.GetOrAdd(partitionKey, _ => new Partition(partitionKey));

                if (partition.Queue.Count == 0 && partition.ProcessingCount == 0)
                {
                    Interlocked.Increment(ref _partitionCount);
                }

                partition.Queue.Enqueue(value);
                Interlocked.Increment(ref _valueCount);

                if (partition.Queue.Count == 1 && partition.ProcessingCount < _partitionProcessingLimit)
                {
                    PromoteToBeEnqueued(partition);
                }
            }
        }

        public bool TryDequeue(out Func<Func<TValue, string, Task>, Task> handler)
        {
            lock (_syncRoot)
            {
                if (!_partitionQueue.TryDequeue(out var partition))
                {
                    handler = default;
                    return false;
                }

                Interlocked.Decrement(ref _partitionQueueCount);

                var value = partition.Queue.Dequeue();
                partition.ProcessingCount += 1;
                Interlocked.Decrement(ref _valueCount);

                if (partition.Queue.Count > 0 && partition.ProcessingCount < _partitionProcessingLimit)
                {
                    _partitionQueue.Enqueue(partition);
                    Interlocked.Increment(ref _partitionQueueCount);
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
                if (TryDequeue(out var handler))
                {
                    return handler;
                }

                if (!(_completionException is null) && _valueCount == 0)
                {
                    throw new Exception("BalancedQueue is already completed");
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

                if (_valueCount == 0)
                {
                    while (_waiterQueue.TryDequeue(out var waiter))
                    {
                        Task.Run(() => waiter.SetException(ex));
                    }
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
                TrackSafe(_diagnostics.TrackUnexpectedException,
                    $"Unable to handle a value in partition '{partition.Name}'", ex);
            }

            lock (_syncRoot)
            {
                partition.ProcessingCount -= 1;

                if (partition.Queue.Count == 0 && partition.ProcessingCount == 0)
                {
                    _partitionRegistry.TryRemove(partition.Name, out _);
                    Interlocked.Decrement(ref _partitionCount);
                    return;
                }

                if (!(_completionException is null) && _valueCount == 0)
                {
                    while (_waiterQueue.TryDequeue(out var waiter))
                    {
                        ScheduleTrySetException(waiter, _completionException);
                    }

                    return;
                }

                if (partition.Queue.Count > 0 && partition.ProcessingCount == _partitionProcessingLimit - 1)
                {
                    PromoteToBeEnqueued(partition);
                }
            }
        }

        private void PromoteToBeEnqueued(Partition partition)
        {
            if (_waiterQueue.TryDequeue(out var waiter))
            {
                var value = partition.Queue.Dequeue();
                partition.ProcessingCount += 1;
                Interlocked.Decrement(ref _valueCount);
                Task.Run(() => waiter.SetResult((value, partition)));
                return;
            }

            _partitionQueue.Enqueue(partition);
            Interlocked.Increment(ref _partitionQueueCount);
        }

        private class Partition
        {
            public readonly string Name;
            public readonly Queue<TValue> Queue;
            public int ProcessingCount;

            public Partition(string name)
            {
                Name = name;
                Queue = new Queue<TValue>();
            }
        }
    }
}