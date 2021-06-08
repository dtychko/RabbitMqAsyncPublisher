using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using static RabbitMqAsyncPublisher.AsyncPublisherUtils;

namespace RabbitMqAsyncPublisher
{
    public interface IBalancedProcessor<in TValue>
    {
        Task ProcessAsync(TValue value, string partitionKey, string correlationId = null);
    }

    public class BalancedProcessor<TValue> : IBalancedProcessor<TValue>, IDisposable
    {
        private readonly BalancedQueue<PublishJob> _queue;
        private readonly Func<TValue, string, CancellationToken, Task> _onProcess;
        private readonly int _degreeOfParallelism;
        private readonly IBalancedProcessorDiagnostics<TValue> _diagnostics;

        private readonly CancellationTokenSource _disposeCancellationSource;
        private readonly CancellationToken _disposeCancellationToken;

        private readonly object _startDisposeSyncRoot = new object();

        private Task[] _workerTasks;

        private volatile int _processingCount;
        private volatile int _processedTotalCount;

        public BalancedProcessorStatus Status => CreateStatus();

        public BalancedProcessor(
            Func<TValue, string, CancellationToken, Task> onProcess,
            int degreeOfParallelism,
            int partitionProcessingLimit = 1,
            IBalancedProcessorDiagnostics<TValue> diagnostics = null)
        {
            if (partitionProcessingLimit < 1)
            {
                throw new ArgumentException("Positive number is expected.", nameof(partitionProcessingLimit));
            }

            if (degreeOfParallelism < 1)
            {
                throw new ArgumentException("Positive number is expected.", nameof(degreeOfParallelism));
            }

            _onProcess = onProcess ?? throw new ArgumentNullException(nameof(onProcess));
            _degreeOfParallelism = degreeOfParallelism;
            _diagnostics = diagnostics ?? BalancedProcessorDiagnostics<TValue>.NoDiagnostics;

            _queue = new BalancedQueue<PublishJob>(partitionProcessingLimit);

            _disposeCancellationSource = new CancellationTokenSource();
            _disposeCancellationToken = _disposeCancellationSource.Token;
        }

        public void Start()
        {
            lock (_startDisposeSyncRoot)
            {
                if (_disposeCancellationToken.IsCancellationRequested)
                {
                    throw new ObjectDisposedException(GetType().Name);
                }

                if (!(_workerTasks is null))
                {
                    throw new InvalidOperationException("Already started");
                }

                _workerTasks = Enumerable.Range(0, _degreeOfParallelism)
                    .Select(_ => StartWorker())
                    .ToArray();
            }
        }

        private async Task StartWorker()
        {
            while (true)
            {
                Func<Func<PublishJob, string, Task>, Task> handler;

                try
                {
                    // ReSharper disable once MethodSupportsCancellation
                    handler = await _queue.DequeueAsync().ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (_disposeCancellationToken.IsCancellationRequested)
                {
                    // "OperationCanceledException" could be thrown when "_queue" is completed and empty.
                    // It means that no more work could be enqueued, as result current worker loop should exit.
                    return;
                }
                catch (Exception ex)
                {
                    TrackSafe(_diagnostics.TrackUnexpectedException,
                        $"[CRITICAL] Unexpected exception in '{GetType().Name}' job processing loop", ex);

                    // Wait for a moment before going to the next iteration.
                    // Otherwise all worker loops could move to the state when they are spinning CPU continuously
                    // if for some reason unexpected error is thrown permanently.
                    await Task.WhenAny(
                        Task.Delay(TimeSpan.FromSeconds(1), _disposeCancellationToken)
                    ).ConfigureAwait(false);
                    continue;
                }

                try
                {
                    await handler(HandleJobAsync).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    TrackSafe(_diagnostics.TrackUnexpectedException,
                        $"[CRITICAL] Unexpected exception in '{GetType().Name}' during handling a job", ex);
                }
            }
        }

        private async Task HandleJobAsync(PublishJob publishJob, string _)
        {
            Interlocked.Increment(ref _processingCount);

            TrackSafe(_diagnostics.TrackProcessJobStarted, publishJob.Args, CreateStatus());
            var stopwatch = Stopwatch.StartNew();

            if (_disposeCancellationToken.IsCancellationRequested)
            {
                OnDispose();
                return;
            }

            try
            {
                await _onProcess(publishJob.Args.Value, publishJob.Args.PartitionKey, _disposeCancellationToken)
                    .ConfigureAwait(false);

                TrackSafe(_diagnostics.TrackProcessJobCompleted, publishJob.Args, UpdateState(), stopwatch.Elapsed);
                ScheduleTrySetResult(publishJob.TaskCompletionSource, true);
            }
            catch (OperationCanceledException ex)
            {
                if (_disposeCancellationToken.IsCancellationRequested)
                {
                    OnDispose();
                    return;
                }

                TrackSafe(_diagnostics.TrackProcessJobCancelled, publishJob.Args, UpdateState(), stopwatch.Elapsed);
                ScheduleTrySetCanceled(publishJob.TaskCompletionSource, ex.CancellationToken);
            }
            catch (Exception ex)
            {
                TrackSafe(_diagnostics.TrackProcessJobFailed, publishJob.Args, UpdateState(), stopwatch.Elapsed, ex);
                ScheduleTrySetException(publishJob.TaskCompletionSource, ex);
            }

            void OnDispose()
            {
                var dex = new ObjectDisposedException(GetType().Name);
                TrackSafe(_diagnostics.TrackProcessJobFailed, publishJob.Args, UpdateState(), stopwatch.Elapsed,
                    dex);
                ScheduleTrySetException(publishJob.TaskCompletionSource, dex);
            }

            BalancedProcessorStatus UpdateState()
            {
                Interlocked.Decrement(ref _processingCount);
                Interlocked.Increment(ref _processedTotalCount);

                return CreateStatus();
            }
        }

        public Task ProcessAsync(TValue value, string partitionKey, string correlationId = null)
        {
            if (_disposeCancellationToken.IsCancellationRequested)
            {
                throw new ObjectDisposedException(GetType().Name);
            }

            var processArgs = new ProcessArgs<TValue>(value, partitionKey, correlationId);
            var publishJob = new PublishJob(processArgs);
            _queue.Enqueue(publishJob, partitionKey);

            TrackSafe(_diagnostics.TrackProcessJobEnqueued, publishJob.Args, CreateStatus());

            return publishJob.TaskCompletionSource.Task;
        }

        public void Dispose()
        {
            lock (_startDisposeSyncRoot)
            {
                if (_disposeCancellationSource.IsCancellationRequested)
                {
                    return;
                }

                _disposeCancellationSource.Cancel();
                _disposeCancellationSource.Dispose();
            }

            TrackSafe(_diagnostics.TrackDisposeStarted, CreateStatus());
            var stopwatch = Stopwatch.StartNew();

            try
            {
                _queue.TryComplete(new OperationCanceledException(_disposeCancellationToken));

                if (!(_workerTasks is null))
                {
                    Task.WaitAll(_workerTasks);
                }

                TrackSafe(_diagnostics.TrackDisposeCompleted, CreateStatus(), stopwatch.Elapsed);
            }
            catch (Exception ex)
            {
                TrackSafe(_diagnostics.TrackUnexpectedException,
                    $"[CRITICAL] Unable to dispose processor '{GetType().Name}'", ex);
            }
        }

        private BalancedProcessorStatus CreateStatus()
        {
            return new BalancedProcessorStatus(_queue.Status, _processingCount, _processedTotalCount);
        }

        private struct PublishJob
        {
            public readonly ProcessArgs<TValue> Args;
            public readonly TaskCompletionSource<bool> TaskCompletionSource;

            public PublishJob(ProcessArgs<TValue> args)
            {
                Args = args;
                TaskCompletionSource = new TaskCompletionSource<bool>();
            }
        }
    }

    public struct ProcessArgs<TValue>
    {
        public readonly TValue Value;
        public readonly string PartitionKey;
        public readonly string CorrelationId;
        public readonly DateTimeOffset StartedAt;

        public ProcessArgs(TValue value, string partitionKey, string correlationId)
        {
            Value = value;
            PartitionKey = partitionKey;
            CorrelationId = correlationId ?? Guid.NewGuid().ToString("D");
            StartedAt = DateTimeOffset.UtcNow;
        }

        public override string ToString()
        {
            return $"{nameof(Value)}=({Value}); " +
                   $"{nameof(PartitionKey)}={PartitionKey}; " +
                   $"{nameof(CorrelationId)}={CorrelationId}; " +
                   $"{nameof(StartedAt)}={StartedAt}";
        }
    }

    public struct BalancedProcessorStatus
    {
        public readonly int QueuedCount;
        public readonly int PartitionCount;
        public readonly int ReadyPartitionCount;
        public readonly int ProcessingCount;
        public readonly int ProcessedTotalCount;

        public BalancedProcessorStatus(BalancedQueueStatus queueStatus, int processingCount, int processedTotalCount)
        {
            QueuedCount = queueStatus.ValueCount;
            PartitionCount = queueStatus.PartitionCount;
            ReadyPartitionCount = queueStatus.ReadyPartitionCount;
            ProcessingCount = processingCount;
            ProcessedTotalCount = processedTotalCount;
        }

        public override string ToString()
        {
            return $"{nameof(QueuedCount)}={QueuedCount}; " +
                   $"{nameof(PartitionCount)}={PartitionCount}; " +
                   $"{nameof(ReadyPartitionCount)}={ReadyPartitionCount}; " +
                   $"{nameof(ProcessingCount)}={ProcessingCount}; " +
                   $"{nameof(ProcessedTotalCount)}={ProcessedTotalCount}";
        }
    }
}