using System;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMqAsyncPublisher
{
    using static AsyncPublisherUtils;

    internal class JobQueueLoop<TJob>
    {
        private readonly Func<Func<TJob>, Task> _handleJob;
        private readonly IUnexpectedExceptionDiagnostics _diagnostics;

        private readonly LinkedListQueue<TJob> _jobQueue = new LinkedListQueue<TJob>();
        private readonly AsyncManualResetEvent _jobQueueReadyEvent = new AsyncManualResetEvent(false);
        private readonly Task _jobQueueTask;

        private readonly CancellationTokenSource _stopCancellation = new CancellationTokenSource();
        private readonly CancellationToken _stopCancellationToken;

        public int QueueSize => _jobQueue.Size;

        public JobQueueLoop(Action<Func<TJob>> handleJob,
            IUnexpectedExceptionDiagnostics diagnostics)
            : this(dequeue =>
            {
                handleJob(dequeue);
                return Task.CompletedTask;
            }, diagnostics)
        {
        }

        public JobQueueLoop(Func<Func<TJob>, Task> handleJob,
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

                while (!_stopCancellationToken.IsCancellationRequested)
                {
                    _jobQueueReadyEvent.Reset();

                    while (_jobQueue.CanDequeue())
                    {
                        await HandleJobAsync().ConfigureAwait(false);
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
                    $"[CRITICAL] Unexpected exception in job queue loop '{typeof(TJob).Name}': jobQueueSize={_jobQueue.Size}",
                    ex);

                return;
            }

            // Handle jobs that could remain in the job queue
            while (_jobQueue.CanDequeue())
            {
                await HandleJobAsync().ConfigureAwait(false);
            }
        }

        private async Task HandleJobAsync()
        {
            try
            {
                await _handleJob(() => _jobQueue.Dequeue()).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                TrackSafe(_diagnostics.TrackUnexpectedException,
                    $"[CRITICAL] Unexpected exception in job queue loop '{typeof(TJob).Name}' iteration",
                    ex);

                // Not expected to happen in production, safety measure if it happens for some reason, to avoid burning CPU. 
                // ReSharper disable once MethodSupportsCancellation
                await Task.Delay(TimeSpan.FromSeconds(1)).ConfigureAwait(false);
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