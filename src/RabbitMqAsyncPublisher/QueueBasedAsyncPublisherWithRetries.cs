using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client.Exceptions;

namespace RabbitMqAsyncPublisher
{
    using static AsyncPublisherUtils;

    public interface IAsyncPublisherWithRetriesDiagnostics2
        : IQueueBasedPublisherDiagnostics<AsyncPublisherWithRetriesStatus>, IUnexpectedExceptionDiagnostics
    {
    }

    public class AsyncPublisherWithRetriesDiagnostics2 : IAsyncPublisherWithRetriesDiagnostics2
    {
        public static readonly IAsyncPublisherWithRetriesDiagnostics2 NoDiagnostics =
            new AsyncPublisherWithRetriesDiagnostics2();

        protected AsyncPublisherWithRetriesDiagnostics2()
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
    }

    public class QueueBasedAsyncPublisherWithRetries : IAsyncPublisher<bool>
    {
        private readonly IAsyncPublisher<bool> _decorated;
        private readonly TimeSpan _retryDelay;
        private readonly IAsyncPublisherWithRetriesDiagnostics2 _diagnostics;

        private readonly JobQueueLoop<PublishJob<bool>> _publishLoop;

        private readonly AsyncManualResetEvent _gateEvent = new AsyncManualResetEvent(true);

        private readonly CancellationTokenSource _disposeCancellationSource = new CancellationTokenSource();
        private readonly CancellationToken _disposeCancellationToken;

        public QueueBasedAsyncPublisherWithRetries(IAsyncPublisher<bool> decorated, TimeSpan retryDelay,
            IAsyncPublisherWithRetriesDiagnostics2 diagnostics = null)
        {
            _decorated = decorated ?? throw new ArgumentNullException(nameof(decorated));
            _retryDelay = retryDelay;
            _diagnostics = diagnostics ?? AsyncPublisherWithRetriesDiagnostics2.NoDiagnostics;

            _disposeCancellationToken = _disposeCancellationSource.Token;

            _publishLoop =
                new JobQueueLoop<PublishJob<bool>>(HandlePublishJobAsync, AsyncPublisherDiagnostics.NoDiagnostics);
        }

        private async Task HandlePublishJobAsync(Func<PublishJob<bool>> dequeueJob)
        {
            try
            {
                await _gateEvent.WaitAsync(_disposeCancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                var publishJob = dequeueJob();
                var ex = new ObjectDisposedException(GetType().Name);
                // TrackSafe(_diagnostics.TrackPublishJobFailed, publishJob.Args, CreateStatus(), TimeSpan.Zero, ex);
                ScheduleTrySetException(publishJob.TaskCompletionSource, ex);
                return;
            }

            HandlePublishJob(dequeueJob);
        }

        private void HandlePublishJob(Func<PublishJob<bool>> dequeueJob)
        {
            var publishJob = dequeueJob();
            // TrackSafe(_diagnostics.TrackPublishJobStarting, publishJob.Args, CreateStatus());

            if (_disposeCancellationToken.IsCancellationRequested)
            {
                var ex = (Exception) new ObjectDisposedException(GetType().Name);
                // TrackSafe(_diagnostics.TrackPublishJobFailed, publishJob.Args, CreateStatus(), TimeSpan.Zero, ex);
                ScheduleTrySetException(publishJob.TaskCompletionSource, ex);
                return;
            }

            if (publishJob.CancellationToken.IsCancellationRequested)
            {
                // TrackSafe(_diagnostics.TrackPublishJobCancelled, publishJob.Args, CreateStatus(), TimeSpan.Zero);
                ScheduleTrySetCanceled(publishJob.TaskCompletionSource, publishJob.CancellationToken);
            }

            // TrackSafe(_diagnostics.TrackPublishJobStarted, publishJob.Args, CreateStatus());
            // var stopwatch = Stopwatch.StartNew();

            StartPublishAttempts(publishJob);
        }

        private readonly OrderQueue _orderQueue = new OrderQueue();

        private async Task StartPublishAttempts(PublishJob<bool> publishJob)
        {
            var queueEntry = new OrderQueueEntry(new TaskCompletionSource<bool>());
            var tryRemove = _orderQueue.Enqueue(queueEntry);

            try
            {
                if (await TryPublishAsync(publishJob).ConfigureAwait(false))
                {
                    ScheduleTrySetResult(publishJob.TaskCompletionSource, true);
                    return;
                }

                _gateEvent.Reset();

                TaskCompletionSource<bool> tcs;
                while ((tcs = _orderQueue.Peek().TaskCompletionSource) != queueEntry.TaskCompletionSource)
                {
                    // TODO: Check if publishJob.CancellationToken can be cancelled
                    using (var combinedToken =
                        CancellationTokenSource.CreateLinkedTokenSource(_disposeCancellationToken,
                            publishJob.CancellationToken))
                    {
                        await Task.WhenAny(
                            tcs.Task,
                            Task.Delay(-1, combinedToken.Token)
                        ).ConfigureAwait(false);
                    }

                    if (_disposeCancellationToken.IsCancellationRequested)
                    {
                        throw new ObjectDisposedException(GetType().Name);
                    }

                    publishJob.CancellationToken.ThrowIfCancellationRequested();
                }

                for (var attempt = 2;; attempt++)
                {
                    // TODO: Check if publishJob.CancellationToken can be cancelled
                    using (var combinedToken =
                        CancellationTokenSource.CreateLinkedTokenSource(_disposeCancellationToken,
                            publishJob.CancellationToken))
                    {
                        await Task.Delay(_retryDelay, combinedToken.Token).ConfigureAwait(false);
                    }

                    if (await TryPublishAsync(publishJob).ConfigureAwait(false))
                    {
                        ScheduleTrySetResult(publishJob.TaskCompletionSource, true);
                        return;
                    }
                }
            }
            catch (OperationCanceledException ex)
            {
                ScheduleTrySetCanceled(publishJob.TaskCompletionSource, ex.CancellationToken);
            }
            catch (Exception ex)
            {
                ScheduleTrySetException(publishJob.TaskCompletionSource, ex);
            }
            finally
            {
                tryRemove();
                ScheduleTrySetResult(queueEntry.TaskCompletionSource, true);
            }
        }

        private async Task<bool> TryPublishAsync(PublishJob<bool> publishJob)
        {
            if (_disposeCancellationToken.IsCancellationRequested)
            {
                throw new ObjectDisposedException(GetType().Name);
            }

            publishJob.CancellationToken.ThrowIfCancellationRequested();

            try
            {
                var publishResult = await _decorated.PublishAsync(publishJob.Args.Exchange, publishJob.Args.RoutingKey,
                    publishJob.Args.Body, publishJob.Args.Properties, publishJob.Args.CorrelationId,
                    publishJob.CancellationToken);

                return publishResult;
            }
            catch (AlreadyClosedException ex)
            {
                return false;
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        public Task<bool> PublishAsync(string exchange, string routingKey, ReadOnlyMemory<byte> body,
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

            // TrackSafe(_diagnostics.TrackDisposeStarted, CreateStatus());
            // var stopwatch = Stopwatch.StartNew();

            try
            {
                _decorated.Dispose();

                // ReSharper disable once MethodSupportsCancellation
                _publishLoop.StopAsync().Wait();

                // TrackSafe(_diagnostics.TrackDisposeSucceeded, CreateStatus(), stopwatch.Elapsed);
            }
            catch (Exception ex)
            {
                TrackSafe(_diagnostics.TrackUnexpectedException, $"Unable to dispose publisher '{GetType().Name}'", ex);
            }
        }

        private AsyncPublisherWithRetriesStatus CreateStatus()
        {
            return new AsyncPublisherWithRetriesStatus(_publishLoop.QueueSize, _orderQueue.Size);
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
        public readonly int OrderQueueSize;

        public AsyncPublisherWithRetriesStatus(int jobQueueSize, int orderQueueSize)
        {
            JobQueueSize = jobQueueSize;
            OrderQueueSize = orderQueueSize;
        }

        public override string ToString()
        {
            return $"{nameof(JobQueueSize)}: {JobQueueSize}; " +
                   $"{nameof(OrderQueueSize)}: {OrderQueueSize}";
        }
    }
}