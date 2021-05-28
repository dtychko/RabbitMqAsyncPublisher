using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace RabbitMqAsyncPublisher
{
    using static DiagnosticsUtils;

    public class AsyncPublisherWithBuffer<TResult> : IAsyncPublisher<TResult>
    {
        private readonly IAsyncPublisher<TResult> _decorated;
        private readonly IAsyncPublisherWithBufferDiagnostics _diagnostics;

        private readonly JobQueueLoop<PublishJob> _publishLoop;

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

            _publishLoop = new JobQueueLoop<PublishJob>(HandlePublishJobAsync, _diagnostics);
        }

        private async Task HandlePublishJobAsync(Func<PublishJob> dequeueJob)
        {
            Console.WriteLine("HandlePublishJobAsync");
            
            try
            {
                await _gateEvent.WaitAsync(_disposeCancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                var tcs = dequeueJob().TaskCompletionSource;
#pragma warning disable 4014
                // ReSharper disable once MethodSupportsCancellation
                Task.Run(() =>
                    tcs.TrySetException(new ObjectDisposedException(nameof(AsyncPublisherWithBuffer<TResult>))));
#pragma warning restore 4014
                return;
            }

            HandlePublishJob(dequeueJob);
        }

        private void HandlePublishJob(Func<PublishJob> dequeueJob)
        {
            Console.WriteLine("HandlePublishJob");
            
            var publishJob = dequeueJob();
            TrackSafe(_diagnostics.TrackJobStarting, publishJob.Args, CreateStatus(), publishJob.Stopwatch.Elapsed);

            try
            {
                publishJob.CancellationToken.ThrowIfCancellationRequested();

                UpdateState(1, publishJob.Args.Body.Length);
                var handleJobTask = _decorated.PublishAsync(publishJob.Args.Exchange, publishJob.Args.RoutingKey,
                    publishJob.Args.Body,
                    publishJob.Args.Properties, publishJob.CancellationToken);
                TrackSafe(_diagnostics.TrackJobStarted, publishJob.Args, CreateStatus(), publishJob.Stopwatch.Elapsed);

#pragma warning disable 4014
                // ReSharper disable once MethodSupportsCancellation
                Task.Run(async () =>
                {
                    Action resolve;

                    try
                    {
                        var result = await handleJobTask.ConfigureAwait(false);
                        resolve = () =>
                        {
                            TrackSafe(_diagnostics.TrackJobSucceeded, publishJob.Args, CreateStatus(),
                                publishJob.Stopwatch.Elapsed);
                            publishJob.TaskCompletionSource.TrySetResult(result);
                        };
                    }
                    catch (OperationCanceledException ex)
                    {
                        resolve = () =>
                        {
                            TrackSafe(_diagnostics.TrackJobCancelled, publishJob.Args, CreateStatus(),
                                publishJob.Stopwatch.Elapsed);
                            publishJob.TaskCompletionSource.TrySetCanceled(ex.CancellationToken);
                        };
                    }
                    catch (Exception ex)
                    {
                        resolve = () =>
                        {
                            TrackSafe(_diagnostics.TrackJobFailed, publishJob.Args, CreateStatus(),
                                publishJob.Stopwatch.Elapsed,
                                ex);
                            publishJob.TaskCompletionSource.TrySetException(ex);
                        };
                    }

                    UpdateState(-1, -publishJob.Args.Body.Length);
                    resolve();
                });
#pragma warning restore 4014
            }
            catch (OperationCanceledException ex)
            {
                TrackSafe(_diagnostics.TrackJobCancelled, publishJob.Args, CreateStatus(),
                    publishJob.Stopwatch.Elapsed);

#pragma warning disable 4014
                // ReSharper disable once MethodSupportsCancellation
                Task.Run(() => publishJob.TaskCompletionSource.TrySetCanceled(ex.CancellationToken));
#pragma warning restore 4014
            }
            catch (Exception ex)
            {
                TrackSafe(_diagnostics.TrackJobFailed, publishJob.Args, CreateStatus(), publishJob.Stopwatch.Elapsed,
                    ex);

#pragma warning disable 4014
                // ReSharper disable once MethodSupportsCancellation
                Task.Run(() => publishJob.TaskCompletionSource.TrySetException(ex));
#pragma warning restore 4014
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
                TrackSafe(_diagnostics.TrackJobCancelled,
                    new PublishArgs(exchange, routingKey, body, properties),
                    CreateStatus(),
                    TimeSpan.Zero);
                return Task.FromCanceled<TResult>(cancellationToken);
            }

            var job = new PublishJob(exchange, routingKey, body, properties, cancellationToken,
                new TaskCompletionSource<TResult>(), Stopwatch.StartNew());
            var tryCancelJob = _publishLoop.Enqueue(job);

            TrackSafe(_diagnostics.TrackJobEnqueued,
                new PublishArgs(exchange, routingKey, body, properties),
                CreateStatus()
            );

            return WaitForPublishCompletedOrCancelled(job, tryCancelJob, cancellationToken);
        }

        private async Task<TResult> WaitForPublishCompletedOrCancelled(PublishJob publishJob, Func<bool> tryCancelJob,
            CancellationToken cancellationToken)
        {
            var jobTask = publishJob.TaskCompletionSource.Task;
            var firstCompletedTask = await Task.WhenAny(
                Task.Delay(-1, cancellationToken),
                jobTask
            ).ConfigureAwait(false);

            if (firstCompletedTask != jobTask && tryCancelJob())
            {
                TrackSafe(_diagnostics.TrackJobCancelled, publishJob.Args, CreateStatus(),
                    publishJob.Stopwatch.Elapsed);
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

            TrackSafe(_diagnostics.TrackDisposeStarted, CreateStatus());
            var stopwatch = Stopwatch.StartNew();

            _decorated.Dispose();

            // Console.WriteLine("_publishLoop.StopAsync().Wait()");
            // ReSharper disable once MethodSupportsCancellation
            _publishLoop.StopAsync().Wait();

            // while (_jobQueue.CanDequeueJob())
            // {
            //     var job = _jobQueue.DequeueJob();
            //
            //     // ReSharper disable once MethodSupportsCancellation
            //     Task.Run(() =>
            //     {
            //         var ex = new ObjectDisposedException(nameof(AsyncPublisherWithBuffer<TResult>));
            //         TrackSafe(_diagnostics.TrackJobFailed,
            //             new PublishArgs(job.Exchange, job.RoutingKey, job.Body, job.Properties),
            //             CreateStatus(),
            //             job.Stopwatch.Elapsed,
            //             ex);
            //         job.TaskCompletionSource.TrySetException(
            //             new ObjectDisposedException(nameof(AsyncPublisherWithBuffer<TResult>)));
            //     });
            // }

            TrackSafe(_diagnostics.TrackDisposeSucceeded, CreateStatus(), stopwatch.Elapsed);
        }

        private AsyncPublisherWithBufferStatus CreateStatus()
        {
            return new AsyncPublisherWithBufferStatus(_publishLoop.QueueSize, _processingMessages, _processingBytes);
        }

        private readonly struct PublishJob
        {
            public readonly PublishArgs Args;
            public readonly CancellationToken CancellationToken;
            public readonly TaskCompletionSource<TResult> TaskCompletionSource;
            public readonly Stopwatch Stopwatch;

            public PublishJob(string exchange, string routingKey, ReadOnlyMemory<byte> body,
                IBasicProperties properties,
                CancellationToken cancellationToken, TaskCompletionSource<TResult> taskCompletionSource,
                Stopwatch stopwatch)
            {
                Args = new PublishArgs(exchange, routingKey, body, properties);
                CancellationToken = cancellationToken;
                TaskCompletionSource = taskCompletionSource;
                Stopwatch = stopwatch;
            }
        }
    }
}