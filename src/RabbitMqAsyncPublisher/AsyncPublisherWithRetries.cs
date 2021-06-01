using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client.Exceptions;

namespace RabbitMqAsyncPublisher
{
    using static AsyncPublisherUtils;

    public class AsyncPublisherWithRetries : IAsyncPublisher<RetryPublishResult>
    {
        private static readonly Func<Exception, int, bool> RetryOnAlreadyClosedException =
            (ex, _) => ex is AlreadyClosedException;

        private static readonly Func<int, bool> RetryOnNack = _ => true;

        private readonly IAsyncPublisher<bool> _decorated;
        private readonly TimeSpan _retryDelay;
        private readonly Func<Exception, int, bool> _shouldRetryOnException;
        private readonly Func<int, bool> _shouldRetryOnNack;
        private readonly IAsyncPublisherWithRetriesDiagnostics _diagnostics;

        private readonly JobQueueLoop<PublishJob<RetryPublishResult>> _publishLoop;

        private readonly AsyncManualResetEvent _gateEvent = new AsyncManualResetEvent(true);

        private readonly LinkedListQueue<Publishing> _publishingQueue = new LinkedListQueue<Publishing>();

        private readonly CancellationTokenSource _disposeCancellationSource = new CancellationTokenSource();
        private readonly CancellationToken _disposeCancellationToken;

        public AsyncPublisherWithRetries(
            IAsyncPublisher<bool> decorated,
            TimeSpan retryDelay,
            Func<Exception, int, bool> shouldRetryOnException = null,
            Func<int, bool> shouldRetryOnNack = null,
            IAsyncPublisherWithRetriesDiagnostics diagnostics = null)
        {
            _decorated = decorated ?? throw new ArgumentNullException(nameof(decorated));
            _retryDelay = retryDelay;
            _shouldRetryOnException = shouldRetryOnException ?? RetryOnAlreadyClosedException;
            _shouldRetryOnNack = shouldRetryOnNack ?? RetryOnNack;
            _diagnostics = diagnostics ?? AsyncPublisherWithRetriesDiagnostics.NoDiagnostics;

            _disposeCancellationToken = _disposeCancellationSource.Token;

            _publishLoop =
                new JobQueueLoop<PublishJob<RetryPublishResult>>(HandlePublishJobAsync,
                    AsyncPublisherDiagnostics.NoDiagnostics);
        }

        private async Task HandlePublishJobAsync(Func<PublishJob<RetryPublishResult>> dequeueJob)
        {
            try
            {
                await _gateEvent.WaitAsync(_disposeCancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                var publishJob = dequeueJob();
                var ex = new ObjectDisposedException(GetType().Name);
                TrackSafe(_diagnostics.TrackPublishJobFailed, publishJob.Args, CreateStatus(), TimeSpan.Zero, ex);
                ScheduleTrySetException(publishJob.TaskCompletionSource, ex);
                return;
            }

            HandlePublishJobWithRetriesAsync(dequeueJob());
        }

        private async void HandlePublishJobWithRetriesAsync(PublishJob<RetryPublishResult> publishJob)
        {
            var currentPublishing = new Publishing(new TaskCompletionSource<bool>());
            var tryRemoveCurrentPublishing = _publishingQueue.Enqueue(currentPublishing);

            TrackSafe(_diagnostics.TrackPublishJobStarted, publishJob.Args, CreateStatus());
            var stopwatch = Stopwatch.StartNew();

            try
            {
                // ReSharper disable once UseDeconstruction
                var firstResult = await TryPublishAsync(publishJob, 1).ConfigureAwait(false);
                if (firstResult.Completed)
                {
                    TrackSafe(_diagnostics.TrackPublishJobCompleted,
                        publishJob.Args, CreateStatus(), stopwatch.Elapsed, firstResult.Acknowledged);
                    ScheduleTrySetResult(publishJob.TaskCompletionSource,
                        new RetryPublishResult(firstResult.Acknowledged, 0));
                    return;
                }

                _gateEvent.Reset();

                await WaitForPublishCouldBeRetried(publishJob, currentPublishing).ConfigureAwait(false);

                for (var attempt = 2;; attempt++)
                {
                    await WaitForRetryDelay(publishJob).ConfigureAwait(false);

                    // ReSharper disable once UseDeconstruction
                    var retryResult = await TryPublishAsync(publishJob, attempt).ConfigureAwait(false);
                    if (retryResult.Completed)
                    {
                        TrackSafe(_diagnostics.TrackPublishJobCompleted,
                            publishJob.Args, CreateStatus(), stopwatch.Elapsed, firstResult.Acknowledged);
                        ScheduleTrySetResult(publishJob.TaskCompletionSource,
                            new RetryPublishResult(retryResult.Acknowledged, attempt - 1));
                        return;
                    }
                }
            }
            catch (OperationCanceledException ex)
            {
                if (ex.CancellationToken == _disposeCancellationToken)
                {
                    var dex = new ObjectDisposedException(GetType().Name);
                    TrackSafe(_diagnostics.TrackPublishJobFailed,
                        publishJob.Args, CreateStatus(), stopwatch.Elapsed, dex);
                    ScheduleTrySetException(publishJob.TaskCompletionSource, dex);
                    return;
                }

                TrackSafe(_diagnostics.TrackPublishJobCancelled,
                    publishJob.Args, CreateStatus(), stopwatch.Elapsed);
                ScheduleTrySetCanceled(publishJob.TaskCompletionSource, ex.CancellationToken);
            }
            catch (Exception ex)
            {
                TrackSafe(_diagnostics.TrackPublishJobFailed, publishJob.Args, CreateStatus(), stopwatch.Elapsed, ex);
                ScheduleTrySetException(publishJob.TaskCompletionSource, ex);
            }
            finally
            {
                tryRemoveCurrentPublishing();
                ScheduleTrySetResult(currentPublishing.TaskCompletionSource, true);

                if (_publishingQueue.Size == 0)
                {
                    _gateEvent.Set();
                }
            }
        }

        private async Task WaitForPublishCouldBeRetried(
            PublishJob<RetryPublishResult> publishJob,
            Publishing targetPublishing)
        {
            TaskCompletionSource<bool> tcs;

            while ((tcs = _publishingQueue.Peek().TaskCompletionSource) != targetPublishing.TaskCompletionSource)
            {
                if (publishJob.CancellationToken.CanBeCanceled)
                {
                    using (var combinedToken =
                        CancellationTokenSource.CreateLinkedTokenSource(
                            _disposeCancellationToken, publishJob.CancellationToken))
                    {
                        await Task.WhenAny(
                            tcs.Task,
                            Task.Delay(-1, combinedToken.Token)
                        ).ConfigureAwait(false);

                        _disposeCancellationToken.ThrowIfCancellationRequested();
                        publishJob.CancellationToken.ThrowIfCancellationRequested();

                        return;
                    }
                }

                await Task.WhenAny(
                    tcs.Task,
                    Task.Delay(-1, _disposeCancellationToken)
                ).ConfigureAwait(false);

                _disposeCancellationToken.ThrowIfCancellationRequested();
            }
        }

        private async Task WaitForRetryDelay(PublishJob<RetryPublishResult> publishJob)
        {
            if (publishJob.CancellationToken.CanBeCanceled)
            {
                using (var combinedToken =
                    CancellationTokenSource.CreateLinkedTokenSource(
                        _disposeCancellationToken, publishJob.CancellationToken))
                {
                    // Using "Task.WhenAny" not to throw OperationCancelledException linked to "combinedToken" immediately.
                    // We should throw this exception linked directly
                    // to either "_disposeCancellationToken" or "publishJob.CancellationToken" 
                    await Task.WhenAny(
                        Task.Delay(_retryDelay, combinedToken.Token)
                    ).ConfigureAwait(false);

                    _disposeCancellationToken.ThrowIfCancellationRequested();
                    publishJob.CancellationToken.ThrowIfCancellationRequested();

                    return;
                }
            }

            await Task.Delay(_retryDelay, _disposeCancellationToken).ConfigureAwait(false);
        }

        private async Task<(bool Completed, bool Acknowledged)> TryPublishAsync(
            PublishJob<RetryPublishResult> publishJob, int attempt)
        {
            _disposeCancellationToken.ThrowIfCancellationRequested();
            publishJob.CancellationToken.ThrowIfCancellationRequested();

            TrackSafe(_diagnostics.TrackPublishAttemptStarted, publishJob.Args, attempt);
            var stopwatch = Stopwatch.StartNew();

            try
            {
                var acknowledged = await _decorated.PublishAsync(
                        publishJob.Args.Exchange, publishJob.Args.RoutingKey, publishJob.Args.Body,
                        publishJob.Args.Properties, publishJob.Args.CorrelationId, publishJob.CancellationToken)
                    .ConfigureAwait(false);

                var shouldRetry = !acknowledged && ShouldRetryOnNack(attempt);

                TrackSafe(_diagnostics.TrackPublishAttemptCompleted,
                    publishJob.Args, attempt, stopwatch.Elapsed, acknowledged, shouldRetry);

                return shouldRetry
                    ? (false, false)
                    : (true, acknowledged);
            }
            catch (OperationCanceledException)
            {
                TrackSafe(_diagnostics.TrackPublishAttemptCancelled, publishJob.Args, attempt, stopwatch.Elapsed);
                throw;
            }
            catch (Exception ex)
            {
                var shouldRetry = ShouldRetryOnException(ex, attempt);

                TrackSafe(_diagnostics.TrackPublishAttemptFailed,
                    publishJob.Args, attempt, stopwatch.Elapsed, ex, shouldRetry);

                if (shouldRetry)
                {
                    return (false, default);
                }

                throw;
            }
        }

        private bool ShouldRetryOnNack(int attempt)
        {
            try
            {
                return _shouldRetryOnNack(attempt);
            }
            catch (Exception ex)
            {
                TrackSafe(_diagnostics.TrackUnexpectedException,
                    $"Unable to call '{nameof(_shouldRetryOnNack)}' callback: {nameof(attempt)}={attempt}",
                    ex);
                return false;
            }
        }

        private bool ShouldRetryOnException(Exception exception, int attempt)
        {
            try
            {
                return _shouldRetryOnException(exception, attempt);
            }
            catch (Exception ex)
            {
                TrackSafe(_diagnostics.TrackUnexpectedException,
                    $"Unable to call '{nameof(_shouldRetryOnException)}' callback: {nameof(attempt)}={attempt}",
                    ex);
                return false;
            }
        }

        public Task<RetryPublishResult> PublishAsync(string exchange, string routingKey,
            ReadOnlyMemory<byte> body,
            MessageProperties properties,
            string correlationId = null, CancellationToken cancellationToken = default)
        {
            return PublishAsyncCore(
                new PublishArgs(exchange, routingKey, body, properties, correlationId), cancellationToken,
                GetType(), _diagnostics, _publishLoop, CreateStatus, _disposeCancellationToken
            );
        }

        public void Dispose()
        {
            DisposeCore(GetType(), OnDispose, _diagnostics, CreateStatus, _disposeCancellationSource);

            async Task OnDispose()
            {
                _decorated.Dispose();
                await _publishLoop.StopAsync().ConfigureAwait(false);
            }
        }

        private AsyncPublisherWithRetriesStatus CreateStatus()
        {
            return new AsyncPublisherWithRetriesStatus(_publishLoop.QueueSize, _publishingQueue.Size);
        }

        private readonly struct Publishing
        {
            public readonly TaskCompletionSource<bool> TaskCompletionSource;

            public Publishing(TaskCompletionSource<bool> taskCompletionSource)
            {
                TaskCompletionSource = taskCompletionSource;
            }
        }
    }

    public readonly struct AsyncPublisherWithRetriesStatus
    {
        public readonly int JobQueueSize;
        public readonly int PublishingQueueSize;

        public AsyncPublisherWithRetriesStatus(int jobQueueSize, int publishingQueueSize)
        {
            JobQueueSize = jobQueueSize;
            PublishingQueueSize = publishingQueueSize;
        }

        public override string ToString()
        {
            return $"{nameof(JobQueueSize)}={JobQueueSize}; " +
                   $"{nameof(PublishingQueueSize)}={PublishingQueueSize}";
        }
    }

    public readonly struct RetryPublishResult
    {
        public readonly bool IsAcknowledged;
        public readonly int Retries;

        public RetryPublishResult(bool isAcknowledged, int retries)
        {
            IsAcknowledged = isAcknowledged;
            Retries = retries;
        }

        public override string ToString()
        {
            return $"{nameof(IsAcknowledged)}={IsAcknowledged}; " +
                   $"{nameof(Retries)}={Retries}";
        }
    }
}