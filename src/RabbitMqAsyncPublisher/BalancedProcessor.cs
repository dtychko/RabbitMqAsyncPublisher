using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

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

        public void Start()
        {
            lock (_disposeCancellationSource)
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
                Func<Func<Job, string, Task>, Task> handler;

                try
                {
                    handler = await _queue.DequeueAsync(_disposeCancellationToken)
                        .ConfigureAwait(false);
                }
                catch (OperationCanceledException ex)
                {
                    if (_disposeCancellationToken.IsCancellationRequested)
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
                        AsyncPublisherUtils.ScheduleTrySetResult(job.TaskCompletionSource, true);
                    }
                    catch (OperationCanceledException ex)
                    {
                        if (_disposeCancellationToken.IsCancellationRequested)
                        {
                            AsyncPublisherUtils.ScheduleTrySetException(job.TaskCompletionSource,
                                new ObjectDisposedException(GetType().Name));
                            return;
                        }

                        AsyncPublisherUtils.ScheduleTrySetCanceled(job.TaskCompletionSource, ex.CancellationToken);
                    }
                    catch (Exception ex)
                    {
                        AsyncPublisherUtils.ScheduleTrySetException(job.TaskCompletionSource, ex);
                    }
                }).ConfigureAwait(false);
            }
        }

        public Task ProcessAsync(string partitionKey, TValue value)
        {
            if (_disposeCancellationToken.IsCancellationRequested)
            {
                throw new ObjectDisposedException(GetType().Name);
            }

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

            if (!(_workerTasks is null))
            {
                Task.WaitAll(_workerTasks);
            }
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
}