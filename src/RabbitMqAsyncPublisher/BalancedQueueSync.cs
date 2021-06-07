using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using static RabbitMqAsyncPublisher.AsyncPublisherUtils;

namespace RabbitMqAsyncPublisher
{
    public interface IBalancedProcessor<in TValue>
    {
        Task ProcessAsync(string partitionKey, TValue value);
    }

    public class BalancedProcessor<TValue> : IBalancedProcessor<TValue>
    {
        private readonly IBalancedQueue<Job> _queue;
        private readonly Func<TValue, string, CancellationToken, Task> _onProcess;
        private readonly int _degreeOfParallelism;
        private Task[] _workerTasks;

        private readonly CancellationTokenSource _disposeCancellationSource;
        private readonly CancellationToken _disposeCancellationToken;

        public BalancedProcessor(Func<TValue, string, CancellationToken, Task> onProcess,
            int partitionProcessingLimit = 1,
            int degreeOfParallelism = 1)
        {
            _queue = new BalancedQueueSync<Job>(partitionProcessingLimit);
            _onProcess = onProcess;
            _degreeOfParallelism = degreeOfParallelism;

            _disposeCancellationSource = new CancellationTokenSource();
            _disposeCancellationToken = _disposeCancellationSource.Token;
        }

        private async Task StartWorker()
        {
            while (true)
            {
                Func<Func<Job, string, Task>, Task> handler;

                try
                {
                    handler = await _queue.DequeueAsync(_disposeCancellationToken)
                        .ConfigureAwait(false);
                }
                catch (OperationCanceledException ex)
                {
                    // if (_disposeCancellationToken.IsCancellationRequested)
                    if (ex.CancellationToken == _disposeCancellationToken)
                    {
                        return;
                    }

                    // TODO: Log unexpected error
                    continue;
                }
                catch (Exception ex)
                {
                    // TODO: Log unexpected error
                    continue;
                }

                await handler(async (job, partitionKey) =>
                {
                    try
                    {
                        _disposeCancellationToken.ThrowIfCancellationRequested();
                        await _onProcess(job.Value, partitionKey, _disposeCancellationToken).ConfigureAwait(false);
                        ScheduleTrySetResult(job.TaskCompletionSource, true);
                    }
                    catch (OperationCanceledException ex)
                    {
                        if (_disposeCancellationToken.IsCancellationRequested)
                        {
                            ScheduleTrySetException(job.TaskCompletionSource,
                                new ObjectDisposedException(GetType().Name));
                            return;
                        }

                        ScheduleTrySetCanceled(job.TaskCompletionSource, ex.CancellationToken);
                    }
                    catch (Exception ex)
                    {
                        ScheduleTrySetException(job.TaskCompletionSource, ex);
                    }
                }).ConfigureAwait(false);
            }
        }

        public void Start()
        {
            _workerTasks = Enumerable.Range(0, _degreeOfParallelism)
                .Select(_ => StartWorker())
                .ToArray();
        }

        public Task ProcessAsync(string partitionKey, TValue value)
        {
            var job = new Job(value);
            _queue.Enqueue(partitionKey, job);

            return job.TaskCompletionSource.Task;
        }

        public void Dispose()
        {
            lock (_disposeCancellationSource)
            {
                if (_disposeCancellationSource.IsCancellationRequested)
                {
                    return;
                }

                _disposeCancellationSource.Cancel();
                _disposeCancellationSource.Dispose();
            }

            _queue.TryComplete(new OperationCanceledException(_disposeCancellationToken));

            Task.WaitAll(_workerTasks);
        }

        private struct Job
        {
            public readonly TValue Value;
            public readonly TaskCompletionSource<bool> TaskCompletionSource;

            public Job(TValue value)
            {
                Value = value;
                TaskCompletionSource = new TaskCompletionSource<bool>();
            }
        }
    }

    public interface IBalancedQueue<TValue>
    {
        void Enqueue(string partitionKey, TValue value);

        Task WaitToDequeueAsync(CancellationToken cancellationToken = default);

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

        private AsyncManualResetEvent _gateEvent;

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

        public async Task WaitToDequeueAsync(CancellationToken cancellationToken = default)
        {
            if (_gateEvent is null)
            {
                lock (_syncRoot)
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
                AdjustGate();

                var value = partition.Queue.Dequeue();
                partition.ProcessingCount += 1;
                Interlocked.Decrement(ref _valueCount);

                if (partition.Queue.Count > 0 && partition.ProcessingCount < _partitionProcessingLimit)
                {
                    _partitionQueue.Enqueue(partition);
                    Interlocked.Increment(ref _partitionQueueCount);
                    AdjustGate();
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
                        Task.Run(() => waiter.SetException(_completionException));
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
            AdjustGate();
        }

        private void AdjustGate()
        {
            if (_gateEvent is null)
            {
                return;
            }

            if (_partitionQueueCount > 0)
            {
                _gateEvent.Set();
            }
            else
            {
                _gateEvent.Reset();
            }
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