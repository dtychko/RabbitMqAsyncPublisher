using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client.Exceptions;

namespace RabbitMqAsyncPublisher
{
    using static AsyncPublisherUtils;

    public interface IQueueBasedAsyncPublisherWithRetriesDiagnostics
        : IQueueBasedPublisherDiagnostics<AsyncPublisherWithRetriesStatus>
    {
        void TrackPublishJobStarted(PublishArgs publishArgs, AsyncPublisherWithRetriesStatus status);

        void TrackPublishJobCompleted(PublishArgs publishArgs, AsyncPublisherWithRetriesStatus status,
            TimeSpan duration);

        void TrackPublishJobCancelled(PublishArgs publishArgs, AsyncPublisherWithRetriesStatus status,
            TimeSpan duration);

        void TrackPublishJobFailed(PublishArgs publishArgs, AsyncPublisherWithRetriesStatus status,
            TimeSpan duration, Exception ex);

        void TrackPublishAttemptStarted(PublishArgs publishArgs, int attempt);

        void TrackPublishAttemptCompleted(PublishArgs publishArgs, int attempt, TimeSpan duration, bool acknowledged,
            bool shouldRetry);

        void TrackPublishAttemptCancelled(PublishArgs publishArgs, int attempt, TimeSpan duration);

        void TrackPublishAttemptFailed(PublishArgs publishArgs, int attempt, TimeSpan duration, Exception ex,
            bool shouldRetry);

        void TrackDisposeStarted(AsyncPublisherWithRetriesStatus status);

        void TrackDisposeSucceeded(AsyncPublisherWithRetriesStatus status, TimeSpan duration);
    }

    public class QueueBasedAsyncPublisherWithRetriesDiagnostics : IQueueBasedAsyncPublisherWithRetriesDiagnostics
    {
        public static readonly IQueueBasedAsyncPublisherWithRetriesDiagnostics NoDiagnostics =
            new QueueBasedAsyncPublisherWithRetriesDiagnostics();

        protected QueueBasedAsyncPublisherWithRetriesDiagnostics()
        {
        }

        public virtual void TrackPublishJobEnqueued(PublishArgs publishArgs, AsyncPublisherWithRetriesStatus status)
        {
        }

        public virtual void TrackPublishStarted(PublishArgs publishArgs)
        {
        }

        public virtual void TrackPublishCompleted(PublishArgs publishArgs, TimeSpan duration)
        {
        }

        public virtual void TrackPublishCancelled(PublishArgs publishArgs, TimeSpan duration)
        {
        }

        public virtual void TrackPublishFailed(PublishArgs publishArgs, TimeSpan duration, Exception ex)
        {
        }

        public virtual void TrackUnexpectedException(string message, Exception ex)
        {
        }

        public virtual void TrackPublishJobStarted(PublishArgs publishArgs, AsyncPublisherWithRetriesStatus status)
        {
        }

        public virtual void TrackPublishJobCompleted(PublishArgs publishArgs, AsyncPublisherWithRetriesStatus status,
            TimeSpan duration)
        {
        }

        public virtual void TrackPublishJobCancelled(PublishArgs publishArgs, AsyncPublisherWithRetriesStatus status,
            TimeSpan duration)
        {
        }

        public virtual void TrackPublishJobFailed(PublishArgs publishArgs, AsyncPublisherWithRetriesStatus status,
            TimeSpan duration,
            Exception ex)
        {
        }

        public virtual void TrackPublishAttemptStarted(PublishArgs publishArgs, int attempt)
        {
        }

        public virtual void TrackPublishAttemptCompleted(PublishArgs publishArgs, int attempt, TimeSpan duration,
            bool acknowledged,
            bool shouldRetry)
        {
        }

        public virtual void TrackPublishAttemptCancelled(PublishArgs publishArgs, int attempt, TimeSpan duration)
        {
        }

        public virtual void TrackPublishAttemptFailed(PublishArgs publishArgs, int attempt, TimeSpan duration,
            Exception ex, bool shouldRetry)
        {
        }

        public virtual void TrackDisposeStarted(AsyncPublisherWithRetriesStatus status)
        {
        }

        public virtual void TrackDisposeSucceeded(AsyncPublisherWithRetriesStatus status, TimeSpan duration)
        {
        }
    }

    public class QueueBasedAsyncPublisherWithRetries : IAsyncPublisher<RetryingPublisherResult>
    {
        private static readonly Func<Exception, int, bool> RetryOnAlreadyClosedException =
            (ex, _) => ex is AlreadyClosedException;

        private static readonly Func<int, bool> RetryOnNack = _ => true;

        private readonly IAsyncPublisher<bool> _decorated;
        private readonly TimeSpan _retryDelay;
        private readonly Func<Exception, int, bool> _shouldRetryOnException;
        private readonly Func<int, bool> _shouldRetryOnNack;
        private readonly IQueueBasedAsyncPublisherWithRetriesDiagnostics _diagnostics;

        private readonly JobQueueLoop<PublishJob<RetryingPublisherResult>> _publishLoop;

        private readonly AsyncManualResetEvent _gateEvent = new AsyncManualResetEvent(true);

        private readonly OrderQueue _publishingQueue = new OrderQueue();

        private readonly CancellationTokenSource _disposeCancellationSource = new CancellationTokenSource();
        private readonly CancellationToken _disposeCancellationToken;


        public QueueBasedAsyncPublisherWithRetries(IAsyncPublisher<bool> decorated,
            TimeSpan retryDelay,
            Func<Exception, int, bool> shouldRetryOnException = null,
            Func<int, bool> shouldRetryOnNack = null,
            IQueueBasedAsyncPublisherWithRetriesDiagnostics diagnostics = null)
        {
            _decorated = decorated ?? throw new ArgumentNullException(nameof(decorated));
            _retryDelay = retryDelay;
            _shouldRetryOnException = shouldRetryOnException ?? RetryOnAlreadyClosedException;
            _shouldRetryOnNack = shouldRetryOnNack ?? RetryOnNack;
            _diagnostics = diagnostics ?? QueueBasedAsyncPublisherWithRetriesDiagnostics.NoDiagnostics;

            _disposeCancellationToken = _disposeCancellationSource.Token;

            _publishLoop =
                new JobQueueLoop<PublishJob<RetryingPublisherResult>>(HandlePublishJobAsync,
                    AsyncPublisherDiagnostics.NoDiagnostics);
        }

        private async Task HandlePublishJobAsync(Func<PublishJob<RetryingPublisherResult>> dequeueJob)
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

        private async void HandlePublishJobWithRetriesAsync(PublishJob<RetryingPublisherResult> publishJob)
        {
            var currentPublishing = new OrderQueueEntry(new TaskCompletionSource<bool>());
            var tryRemoveCurrentPublishing = _publishingQueue.Enqueue(currentPublishing);

            TrackSafe(_diagnostics.TrackPublishJobStarted, publishJob.Args, CreateStatus());
            var stopwatch = Stopwatch.StartNew();

            try
            {
                if (await TryPublishAsync(publishJob, 1).ConfigureAwait(false))
                {
                    TrackSafe(_diagnostics.TrackPublishJobCompleted,
                        publishJob.Args, CreateStatus(), stopwatch.Elapsed);
                    ScheduleTrySetResult(publishJob.TaskCompletionSource, RetryingPublisherResult.NoRetries);
                    return;
                }

                _gateEvent.Reset();

                await WaitForPublishCouldBeRetried(publishJob, currentPublishing).ConfigureAwait(false);

                for (var attempt = 2;; attempt++)
                {
                    await WaitForRetryDelay(publishJob).ConfigureAwait(false);

                    if (await TryPublishAsync(publishJob, attempt).ConfigureAwait(false))
                    {
                        TrackSafe(_diagnostics.TrackPublishJobCompleted,
                            publishJob.Args, CreateStatus(), stopwatch.Elapsed);
                        ScheduleTrySetResult(publishJob.TaskCompletionSource, new RetryingPublisherResult(attempt - 1));
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
                }
                else
                {
                    TrackSafe(_diagnostics.TrackPublishJobCancelled,
                        publishJob.Args, CreateStatus(), stopwatch.Elapsed);
                    ScheduleTrySetCanceled(publishJob.TaskCompletionSource, ex.CancellationToken);
                }
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
            PublishJob<RetryingPublisherResult> publishJob,
            OrderQueueEntry targetPublishing)
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

        private async Task WaitForRetryDelay(PublishJob<RetryingPublisherResult> publishJob)
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

        private async Task<bool> TryPublishAsync(PublishJob<RetryingPublisherResult> publishJob, int attempt)
        {
            _disposeCancellationToken.ThrowIfCancellationRequested();
            publishJob.CancellationToken.ThrowIfCancellationRequested();

            TrackSafe(_diagnostics.TrackPublishAttemptStarted, publishJob.Args, attempt);
            var stopwatch = Stopwatch.StartNew();

            try
            {
                var acknowledged = await _decorated.PublishAsync(
                    publishJob.Args.Exchange, publishJob.Args.RoutingKey, publishJob.Args.Body,
                    publishJob.Args.Properties, publishJob.Args.CorrelationId, publishJob.CancellationToken);

                // TODO: Replace _shouldRetryOnNack with ShouldRetryOnNackSafe
                var shouldRetry = !acknowledged && _shouldRetryOnNack(attempt);

                TrackSafe(_diagnostics.TrackPublishAttemptCompleted,
                    publishJob.Args, attempt, stopwatch.Elapsed, acknowledged, shouldRetry);

                return !shouldRetry;
            }
            catch (OperationCanceledException)
            {
                TrackSafe(_diagnostics.TrackPublishAttemptCancelled, publishJob.Args, attempt, stopwatch.Elapsed);
                throw;
            }
            catch (Exception ex)
            {
                // TODO: Replace _shouldRetryOnNack with ShouldRetryOnNackSafe
                var shouldRetry = _shouldRetryOnException(ex, attempt);
                TrackSafe(_diagnostics.TrackPublishAttemptFailed,
                    publishJob.Args, attempt, stopwatch.Elapsed, ex, shouldRetry);

                if (shouldRetry)
                {
                    return false;
                }

                throw;
            }
        }

        public Task<RetryingPublisherResult> PublishAsync(string exchange, string routingKey, ReadOnlyMemory<byte> body,
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
            lock (_disposeCancellationSource)
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
                _decorated.Dispose();

                // ReSharper disable once MethodSupportsCancellation
                _publishLoop.StopAsync().Wait();

                TrackSafe(_diagnostics.TrackDisposeSucceeded, CreateStatus(), stopwatch.Elapsed);
            }
            catch (Exception ex)
            {
                TrackSafe(_diagnostics.TrackUnexpectedException, $"Unable to dispose publisher '{GetType().Name}'", ex);
            }
        }

        private AsyncPublisherWithRetriesStatus CreateStatus()
        {
            return new AsyncPublisherWithRetriesStatus(_publishLoop.QueueSize, _publishingQueue.Size);
        }

        private struct OrderQueueEntry
        {
            public readonly TaskCompletionSource<bool> TaskCompletionSource;

            public OrderQueueEntry(TaskCompletionSource<bool> taskCompletionSource)
            {
                TaskCompletionSource = taskCompletionSource;
            }
        }

        // TODO: Think about unifying with JobQueue<Job>
        private class OrderQueue
        {
            private readonly LinkedList<OrderQueueEntry> _queue = new LinkedList<OrderQueueEntry>();

            private volatile int _size;

            public int Size => _size;

            public Func<bool> Enqueue(OrderQueueEntry job)
            {
                LinkedListNode<OrderQueueEntry> queueNode;

                lock (_queue)
                {
                    queueNode = _queue.AddLast(job);
                    _size = _queue.Count;
                }

                return () => TryRemoveJob(queueNode);
            }

            private bool TryRemoveJob(LinkedListNode<OrderQueueEntry> jobNode)
            {
                lock (_queue)
                {
                    if (jobNode.List is null)
                    {
                        return false;
                    }

                    _queue.Remove(jobNode);
                    _size = _queue.Count;
                }

                return true;
            }

            public OrderQueueEntry Peek()
            {
                lock (_queue)
                {
                    return _queue.First.Value;
                }
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
            return $"{nameof(JobQueueSize)}: {JobQueueSize}; " +
                   $"{nameof(PublishingQueueSize)}: {PublishingQueueSize}";
        }
    }
}