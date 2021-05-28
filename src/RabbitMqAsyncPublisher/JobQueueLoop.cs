using System;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMqAsyncPublisher
{
    using static DiagnosticsUtils;

    internal class JobQueueLoop<TJob>
    {
        private readonly Func<Func<TJob>, CancellationToken, Task> _handleJob;
        private readonly IUnexpectedExceptionDiagnostics _diagnostics;

        private readonly JobQueue<TJob> _jobQueue = new JobQueue<TJob>();
        private readonly AsyncManualResetEvent _jobQueueReadyEvent = new AsyncManualResetEvent(false);
        private readonly Task _jobQueueTask;

        private readonly CancellationTokenSource _stopCancellation = new CancellationTokenSource();
        private readonly CancellationToken _stopCancellationToken;

        public int QueueSize => _jobQueue.Size;

        public JobQueueLoop(Action<Func<TJob>, Func<bool>> handleJob,
            IUnexpectedExceptionDiagnostics diagnostics)
            : this((dequeue, stopToken) =>
            {
                handleJob(dequeue, () => stopToken.IsCancellationRequested);
                return Task.CompletedTask;
            }, diagnostics)
        {
        }

        public JobQueueLoop(Func<Func<TJob>, CancellationToken, Task> handleJob,
            IUnexpectedExceptionDiagnostics diagnostics)
        {
            _handleJob = handleJob;
            _diagnostics = diagnostics;

            _stopCancellationToken = _stopCancellation.Token;

            _jobQueueTask = Task.Run(StartLoop);
        }

        public Func<bool> Enqueue(TJob job)
        {
            if (_stopCancellationToken.IsCancellationRequested)
            {
                throw new InvalidOperationException("Job queue loop is already stopped.");
            }

            var tryRemove = _jobQueue.Enqueue(job);
            _jobQueueReadyEvent.Set();
            return tryRemove;
        }

        private async void StartLoop()
        {
            try
            {
                await _jobQueueReadyEvent.WaitAsync(_stopCancellationToken).ConfigureAwait(false);

                while (!_stopCancellationToken.IsCancellationRequested || _jobQueue.CanDequeueJob())
                {
                    _jobQueueReadyEvent.Reset();

                    while (_jobQueue.CanDequeueJob())
                    {
                        await _handleJob(() => _jobQueue.DequeueJob(), _stopCancellationToken);
                    }

                    await _jobQueueReadyEvent.WaitAsync(_stopCancellationToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (_stopCancellationToken.IsCancellationRequested)
            {
                // Job loop gracefully stopped
            }
            catch (Exception ex)
            {
                TrackSafe(_diagnostics.TrackUnexpectedException,
                    $"Unexpected exception in job queue loop '{GetType().Name}': jobQueueSize={_jobQueue.Size}",
                    ex);

                // TODO: ? Move publisher to state when it throws on each attempt to publish a message
                // TODO: ? Restart the loop after some delay
            }
        }

        public Task StopAsync()
        {
            lock (_stopCancellation)
            {
                if (!_stopCancellation.IsCancellationRequested)
                {
                    _jobQueue.Complete();
                    _jobQueueReadyEvent.Set();

                    _stopCancellation.Cancel();
                    _stopCancellation.Dispose();
                }
            }

            return _jobQueueTask;
        }
    }
}