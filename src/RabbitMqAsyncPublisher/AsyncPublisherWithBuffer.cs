using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace RabbitMqAsyncPublisher
{
    using static AsyncPublisherUtils;

    public class AsyncPublisherWithBuffer<TResult> : IAsyncPublisher<TResult>
    {
        private readonly IAsyncPublisher<TResult> _decorated;
        private readonly IAsyncPublisherWithBufferDiagnostics _diagnostics;

        private readonly JobQueueLoop<PublishJob<TResult>> _publishLoop;

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
            IAsyncPublisherWithBufferDiagnostics diagnostics = null)
        {
            if (processingMessagesLimit <= 0)
            {
                throw new ArgumentException("Positive number is expected.", nameof(processingMessagesLimit));
            }

            if (processingBytesSoftLimit <= 0)
            {
                throw new ArgumentException("Positive number is expected.", nameof(processingBytesSoftLimit));
            }

            _decorated = decorated ?? throw new ArgumentNullException(nameof(decorated));
            _processingMessagesLimit = processingMessagesLimit;
            _processingBytesSoftLimit = processingBytesSoftLimit;
            _diagnostics = diagnostics ?? AsyncPublisherWithBufferDiagnostics.NoDiagnostics;

            _disposeCancellationToken = _disposeCancellationSource.Token;

            _publishLoop = new JobQueueLoop<PublishJob<TResult>>(HandlePublishJobAsync, _diagnostics);
        }

        private async Task HandlePublishJobAsync(Func<PublishJob<TResult>> dequeueJob)
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

            HandlePublishJob(dequeueJob);
        }

        private void HandlePublishJob(Func<PublishJob<TResult>> dequeueJob)
        {
            var publishJob = dequeueJob();
            TrackSafe(_diagnostics.TrackPublishJobStarting, publishJob.Args, CreateStatus());

            if (_disposeCancellationToken.IsCancellationRequested)
            {
                var ex = (Exception) new ObjectDisposedException(GetType().Name);
                TrackSafe(_diagnostics.TrackPublishJobFailed, publishJob.Args, CreateStatus(), TimeSpan.Zero, ex);
                ScheduleTrySetException(publishJob.TaskCompletionSource, ex);
                return;
            }

            if (publishJob.CancellationToken.IsCancellationRequested)
            {
                TrackSafe(_diagnostics.TrackPublishJobCancelled, publishJob.Args, CreateStatus(), TimeSpan.Zero);
                ScheduleTrySetCanceled(publishJob.TaskCompletionSource, publishJob.CancellationToken);
            }

            TrackSafe(_diagnostics.TrackPublishJobStarted, publishJob.Args, CreateStatus());
            var stopwatch = Stopwatch.StartNew();

            try
            {
                UpdateState(1, publishJob.Args.Body.Length);
                var handleJobTask = _decorated.PublishAsync(publishJob.Args.Exchange, publishJob.Args.RoutingKey,
                    publishJob.Args.Body, publishJob.Args.Properties, publishJob.Args.CorrelationId,
                    publishJob.CancellationToken);

                // ReSharper disable once MethodSupportsCancellation
                Task.Run(async () =>
                {
                    Action resolve;

                    try
                    {
                        var result = await handleJobTask.ConfigureAwait(false);
                        resolve = () =>
                        {
                            TrackSafe(_diagnostics.TrackPublishJobCompleted, publishJob.Args, CreateStatus(),
                                stopwatch.Elapsed);
                            publishJob.TaskCompletionSource.TrySetResult(result);
                        };
                    }
                    catch (OperationCanceledException ex)
                    {
                        resolve = () =>
                        {
                            TrackSafe(_diagnostics.TrackPublishJobCancelled, publishJob.Args, CreateStatus(),
                                stopwatch.Elapsed);
                            publishJob.TaskCompletionSource.TrySetCanceled(ex.CancellationToken);
                        };
                    }
                    catch (Exception ex)
                    {
                        resolve = () =>
                        {
                            TrackSafe(_diagnostics.TrackPublishJobFailed, publishJob.Args, CreateStatus(),
                                stopwatch.Elapsed, ex);
                            publishJob.TaskCompletionSource.TrySetException(ex);
                        };
                    }

                    UpdateState(-1, -publishJob.Args.Body.Length);
                    resolve();
                });
            }
            catch (OperationCanceledException ex)
            {
                TrackSafe(_diagnostics.TrackPublishJobCancelled, publishJob.Args, CreateStatus(), stopwatch.Elapsed);
                ScheduleTrySetCanceled(publishJob.TaskCompletionSource, ex.CancellationToken);
            }
            catch (Exception ex)
            {
                TrackSafe(_diagnostics.TrackPublishJobFailed, publishJob.Args, CreateStatus(),
                    stopwatch.Elapsed, ex);
                ScheduleTrySetException(publishJob.TaskCompletionSource, ex);
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
                    _gateEvent.Set();
                }
                else
                {
                    _gateEvent.Reset();
                }
            }
        }

        public Task<TResult> PublishAsync(string exchange, string routingKey, ReadOnlyMemory<byte> body,
            MessageProperties properties, string correlationId = null, CancellationToken cancellationToken = default)
        {
            if (_disposeCancellationToken.IsCancellationRequested)
            {
                var ex = new ObjectDisposedException(GetType().Name);
                TrackSafe(_diagnostics.TrackUnexpectedException,
                    $"Publisher '{GetType().Name}' is already disposed: {nameof(exchange)}={exchange}; {nameof(routingKey)}={routingKey}",
                    ex);

                throw ex;
            }

            return PublishAsyncCore(
                new PublishArgs(exchange, routingKey, body, properties, correlationId), cancellationToken,
                _diagnostics, _publishLoop, CreateStatus);
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

        private AsyncPublisherWithBufferStatus CreateStatus()
        {
            return new AsyncPublisherWithBufferStatus(_publishLoop.QueueSize, _processingMessages, _processingBytes);
        }
    }

    public readonly struct AsyncPublisherWithBufferStatus
    {
        public readonly int JobQueueSize;
        public readonly int ProcessingMessages;
        public readonly int ProcessingBytes;

        public AsyncPublisherWithBufferStatus(int jobQueueSize, int processingMessages, int processingBytes)
        {
            JobQueueSize = jobQueueSize;
            ProcessingMessages = processingMessages;
            ProcessingBytes = processingBytes;
        }

        public override string ToString()
        {
            return $"{nameof(JobQueueSize)}: {JobQueueSize}; " +
                   $"{nameof(ProcessingMessages)}: {ProcessingMessages}; " +
                   $"{nameof(ProcessingBytes)}: {ProcessingBytes}";
        }
    }
}