using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace RabbitMqAsyncPublisher
{
    using static DiagnosticsUtils;

    public interface IAsyncPublisherWithBufferDiagnostics : IUnexpectedExceptionDiagnostics
    {
        void TrackJobEnqueued(PublishArgs publishArgs, AsyncPublisherWithBufferStatus status);

        void TrackJobStarting(PublishArgs publishArgs);

        void TrackJobStarted(PublishArgs publishArgs);

        void TrackJobSucceeded(PublishArgs publishArgs, TimeSpan duration);

        void TrackJobCancelled(PublishArgs publishArgs, TimeSpan duration);

        void TrackJobFailed(PublishArgs publishArgs, TimeSpan duration, Exception ex);

        void TrackDisposeStarted();

        void TrackDisposeSucceeded(TimeSpan duration);

        void TrackStatus(AsyncPublisherWithBufferStatus status);
    }

    public class AsyncPublisherWithBufferStatus
    {
        public int JobQueueSize { get; }
        public int ProcessingMessages { get; }
        public int ProcessingBytes { get; }

        public AsyncPublisherWithBufferStatus(int jobQueueSize, int processingMessages, int processingBytes)
        {
            JobQueueSize = jobQueueSize;
            ProcessingMessages = processingMessages;
            ProcessingBytes = processingBytes;
        }
    }

    public class AsyncPublisherWithBuffer<TResult> : IAsyncPublisher<TResult>
    {
        private readonly IAsyncPublisher<TResult> _decorated;
        private readonly IAsyncPublisherWithBufferDiagnostics _diagnostics;

        private readonly Task _readLoopTask;

        private readonly JobQueue _jobQueue = new JobQueue();
        private readonly AsyncManualResetEvent _jobQueueReadyEvent = new AsyncManualResetEvent(false);

        private readonly AsyncManualResetEvent _gateEvent = new AsyncManualResetEvent(true);
        private readonly object _currentStateSyncRoot = new object();
        private readonly int _processingMessagesLimit;
        private readonly int _processingBytesSoftLimit;
        private volatile int _processingMessages;
        private volatile int _processingBytes;

        private readonly CancellationTokenSource _disposeCancellationSource = new CancellationTokenSource();
        private readonly CancellationToken _disposeCancellationToken;

        public AsyncPublisherWithBuffer(
            IAsyncPublisher<TResult> decorated,
            int processingMessagesLimit = int.MaxValue,
            int processingBytesSoftLimit = int.MaxValue,
            IAsyncPublisherWithBufferDiagnostics diagnostics = default)
        {
            if (processingMessagesLimit <= 0)
            {
                throw new ArgumentException("Positive number is expected.", nameof(processingMessagesLimit));
            }

            if (processingBytesSoftLimit <= 0)
            {
                throw new ArgumentException("Positive number is expected.", nameof(processingBytesSoftLimit));
            }

            _decorated = decorated;
            _processingMessagesLimit = processingMessagesLimit;
            _processingBytesSoftLimit = processingBytesSoftLimit;
            _diagnostics = diagnostics;

            _disposeCancellationToken = _disposeCancellationSource.Token;

            _readLoopTask = Task.Run(RunReaderLoop);
        }

        private async void RunReaderLoop()
        {
            try
            {
                await _jobQueueReadyEvent.WaitAsync(_disposeCancellationToken).ConfigureAwait(false);
                TrackStatusSafe();

                while (!_disposeCancellationToken.IsCancellationRequested)
                {
                    _jobQueueReadyEvent.Reset();

                    while (_jobQueue.CanDequeueJob())
                    {
                        await _gateEvent.WaitAsync(_disposeCancellationToken).ConfigureAwait(false);
                        HandleJob(_jobQueue.DequeueJob());
                    }

                    await _jobQueueReadyEvent.WaitAsync(_disposeCancellationToken).ConfigureAwait(false);
                    TrackStatusSafe();
                }
            }
            catch (OperationCanceledException) when (_disposeCancellationToken.IsCancellationRequested)
            {
                // Reader loop gracefully stopped
            }
            catch (Exception ex)
            {
                TrackSafe(_diagnostics.TrackUnexpectedException,
                    "Unexpected reader loop exception: " +
                    $"jobQueueSize={_jobQueue.Size}; processingMessages={_processingMessages}; processingBytes={_processingBytes}",
                    ex);
            }
        }

        private void HandleJob(Job job)
        {
            var publishArgs = new PublishArgs(job.Exchange, job.RoutingKey, job.Body, job.Properties);
            TrackSafe(_diagnostics.TrackJobStarting, publishArgs);
            var stopwatch = Stopwatch.StartNew();

            try
            {
                job.CancellationToken.ThrowIfCancellationRequested();

                UpdateState(1, job.Body.Length);
                var handleJobTask = _decorated.PublishAsync(job.Exchange, job.RoutingKey, job.Body,
                    job.Properties, job.CancellationToken);
                TrackSafe(_diagnostics.TrackJobStarted, publishArgs);

                // ReSharper disable once MethodSupportsCancellation
                Task.Run(async () =>
                {
                    Action resolve;

                    try
                    {
                        var result = await handleJobTask.ConfigureAwait(false);
                        TrackSafe(_diagnostics.TrackJobSucceeded, publishArgs, stopwatch.Elapsed);
                        resolve = () => job.TaskCompletionSource.TrySetResult(result);
                    }
                    catch (OperationCanceledException ex)
                    {
                        TrackSafe(_diagnostics.TrackJobCancelled, publishArgs, stopwatch.Elapsed);
                        resolve = () => job.TaskCompletionSource.TrySetCanceled(ex.CancellationToken);
                    }
                    catch (Exception ex)
                    {
                        TrackSafe(_diagnostics.TrackJobFailed, publishArgs, stopwatch.Elapsed, ex);
                        resolve = () => job.TaskCompletionSource.TrySetException(ex);
                    }

                    UpdateState(-1, -job.Body.Length);
                    resolve();
                });
            }
            catch (OperationCanceledException ex)
            {
                TrackSafe(_diagnostics.TrackJobCancelled, publishArgs, stopwatch.Elapsed);

                // ReSharper disable once MethodSupportsCancellation
                Task.Run(() => job.TaskCompletionSource.TrySetCanceled(ex.CancellationToken));
            }
            catch (Exception ex)
            {
                TrackSafe(_diagnostics.TrackJobFailed, publishArgs, stopwatch.Elapsed, ex);

                // ReSharper disable once MethodSupportsCancellation
                Task.Run(() => job.TaskCompletionSource.TrySetException(ex));
            }
        }

        private void UpdateState(int deltaMessages, int deltaBytes)
        {
            lock (_currentStateSyncRoot)
            {
                Interlocked.Add(ref _processingMessages, deltaMessages);
                Interlocked.Add(ref _processingBytes, deltaBytes);

                if (_processingMessages < _processingMessagesLimit
                    && _processingBytes < _processingBytesSoftLimit)
                {
                    // Console.WriteLine(" >> Opening gate");
                    _gateEvent.Set();
                }
                else
                {
                    // Console.WriteLine(" >> Closing gate");
                    _gateEvent.Reset();
                }
            }
        }

        public Task<TResult> PublishAsync(
            string exchange, string routingKey, ReadOnlyMemory<byte> body, IBasicProperties properties,
            CancellationToken cancellationToken)
        {
            if (_disposeCancellationToken.IsCancellationRequested)
            {
                throw new ObjectDisposedException(nameof(AsyncPublisherWithBuffer<TResult>));
            }

            if (cancellationToken.IsCancellationRequested)
            {
                return Task.FromCanceled<TResult>(cancellationToken);
            }

            var job = new Job
            {
                Exchange = exchange,
                RoutingKey = routingKey,
                Body = body,
                Properties = properties,
                CancellationToken = cancellationToken,
                TaskCompletionSource = new TaskCompletionSource<TResult>()
            };
            var tryCancelJob = _jobQueue.Enqueue(job);

            TrackSafe(_diagnostics.TrackJobEnqueued,
                new PublishArgs(exchange, routingKey, body, properties),
                new AsyncPublisherWithBufferStatus(_jobQueue.Size, _processingMessages, _processingBytes)
            );

            _jobQueueReadyEvent.Set();

            return WaitForPublishCompletedOrCancelled(job, tryCancelJob, cancellationToken);
        }

        private static async Task<TResult> WaitForPublishCompletedOrCancelled(Job job, Func<bool> tryCancelJob,
            CancellationToken cancellationToken)
        {
            var jobTask = job.TaskCompletionSource.Task;
            var firstCompletedTask = await Task.WhenAny(
                Task.Delay(-1, cancellationToken),
                jobTask
            ).ConfigureAwait(false);

            if (firstCompletedTask != jobTask && tryCancelJob())
            {
                return await Task.FromCanceled<TResult>(cancellationToken).ConfigureAwait(false);
            }

            return await jobTask.ConfigureAwait(false);
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

            TrackSafe(_diagnostics.TrackDisposeStarted);
            var stopwatch = Stopwatch.StartNew();

            _decorated.Dispose();

            // ReSharper disable once MethodSupportsCancellation
            _readLoopTask.Wait();

            while (_jobQueue.CanDequeueJob())
            {
                var job = _jobQueue.DequeueJob();

                // ReSharper disable once MethodSupportsCancellation
                Task.Run(() =>
                    job.TaskCompletionSource.TrySetException(
                        new ObjectDisposedException(nameof(AsyncPublisherWithBuffer<TResult>))));
            }

            TrackSafe(_diagnostics.TrackDisposeSucceeded, stopwatch.Elapsed);
        }

        private void TrackStatusSafe()
        {
            TrackSafe(_diagnostics.TrackStatus,
                new AsyncPublisherWithBufferStatus(_jobQueue.Size, _processingMessages, _processingBytes)
            );
        }

        private struct Job
        {
            public string Exchange;
            public string RoutingKey;
            public ReadOnlyMemory<byte> Body;
            public IBasicProperties Properties;
            public CancellationToken CancellationToken;
            public TaskCompletionSource<TResult> TaskCompletionSource;
        }

        private class JobQueue
        {
            private readonly LinkedList<Job> _queue = new LinkedList<Job>();
            private volatile int _size;

            public int Size => _size;

            public Func<bool> Enqueue(Job job)
            {
                LinkedListNode<Job> queueNode;

                Interlocked.Increment(ref _size);

                lock (_queue)
                {
                    queueNode = _queue.AddLast(job);
                }

                return () => TryRemoveJob(queueNode);
            }

            private bool TryRemoveJob(LinkedListNode<Job> jobNode)
            {
                lock (_queue)
                {
                    if (jobNode.List is null)
                    {
                        return false;
                    }

                    _queue.Remove(jobNode);
                }

                Interlocked.Decrement(ref _size);
                return true;
            }

            public bool CanDequeueJob()
            {
                lock (_queue)
                {
                    return _queue.Count > 0;
                }
            }

            public Job DequeueJob()
            {
                Job job;

                lock (_queue)
                {
                    if (_queue.Count == 0)
                    {
                        throw new InvalidOperationException("Job queue is empty.");
                    }

                    job = _queue.First.Value;
                    _queue.RemoveFirst();
                }

                Interlocked.Decrement(ref _size);
                return job;
            }
        }
    }
}