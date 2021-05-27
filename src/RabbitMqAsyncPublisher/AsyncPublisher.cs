using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace RabbitMqAsyncPublisher
{
    using static DiagnosticsUtils;

    internal class JobQueueLoop<TJob>
    {
        private readonly Action<Func<TJob>, Func<bool>> _handleJob;
        private readonly IUnexpectedExceptionDiagnostics _diagnostics;

        private readonly JobQueue<TJob> _jobQueue = new JobQueue<TJob>();
        private readonly AsyncManualResetEvent _jobQueueReadyEvent = new AsyncManualResetEvent(false);
        private readonly Task _jobQueueTask;

        private readonly CancellationTokenSource _stopCancellation = new CancellationTokenSource();
        private readonly CancellationToken _stopCancellationToken;

        public int QueueSize => _jobQueue.Size;

        public JobQueueLoop(Action<Func<TJob>, Func<bool>> handleJob,
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
                throw new ObjectDisposedException(nameof(JobQueueLoop<TJob>));
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

                    while (_jobQueue.CanDequeueJob())
                    {
                        _handleJob(() => _jobQueue.DequeueJob(), () => _stopCancellationToken.IsCancellationRequested);
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

        public Task Stop()
        {
            lock (_stopCancellation)
            {
                if (!_stopCancellation.IsCancellationRequested)
                {
                    _stopCancellation.Cancel();
                    _stopCancellation.Dispose();
                }
            }

            return _jobQueueTask;
        }
    }

    public class AsyncPublisher : IAsyncPublisher<bool>
    {
        private readonly IModel _model;
        private readonly IAsyncPublisherDiagnostics _diagnostics;

        private readonly JobQueueLoop<PublishQueueItem> _publishLoop;
        private readonly JobQueueLoop<AckJob> _ackLoop;

        private readonly AsyncPublisherTaskCompletionSourceRegistry _completionSourceRegistry =
            new AsyncPublisherTaskCompletionSourceRegistry();

        private int _completionSourceRegistrySize;

        private readonly CancellationTokenSource _disposeCancellationSource = new CancellationTokenSource();
        private readonly CancellationToken _disposeCancellationToken;

        public AsyncPublisher(IModel model, IAsyncPublisherDiagnostics diagnostics = null)
        {
            if (model is null)
            {
                throw new ArgumentNullException(nameof(model));
            }

            // Heuristic based on reverse engineering of "RabbitMQ.Client" lib
            // that helps to make sure that "ConfirmSelect" method was called on the model
            // to enable confirm mode.
            if (model.NextPublishSeqNo == 0)
            {
                throw new ArgumentException("Channel should be in confirm mode.");
            }

            _model = model;
            _diagnostics = diagnostics ?? AsyncPublisherEmptyDiagnostics.NoDiagnostics;

            _disposeCancellationToken = _disposeCancellationSource.Token;

            _publishLoop = new JobQueueLoop<PublishQueueItem>(HandlePublishJob, _diagnostics);
            _ackLoop = new JobQueueLoop<AckJob>(HandleAckJob, _diagnostics);

            _model.BasicAcks += OnBasicAcks;
            _model.BasicNacks += OnBasicNacks;
        }

        private void OnBasicAcks(object sender, BasicAckEventArgs e)
        {
            _ackLoop.Enqueue(new AckJob(e.DeliveryTag, e.Multiple, true));
            TrackSafe(_diagnostics.TrackAckJobEnqueued,
                new AckArgs(e.DeliveryTag, e.Multiple, true),
                CreateStatus());
        }

        private void OnBasicNacks(object sender, BasicNackEventArgs e)
        {
            _ackLoop.Enqueue(new AckJob(e.DeliveryTag, e.Multiple, false));
            TrackSafe(_diagnostics.TrackAckJobEnqueued,
                new AckArgs(e.DeliveryTag, e.Multiple, false),
                CreateStatus());
        }

        private void HandlePublishJob(Func<PublishQueueItem> dequeuePublishQueueItem, Func<bool> isStopped)
        {
            if (isStopped())
            {
                return;
            }

            var publishQueueItem = dequeuePublishQueueItem();
            var cancellationToken = publishQueueItem.CancellationToken;
            var taskCompletionSource = publishQueueItem.TaskCompletionSource;
            ulong seqNo;

            try
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    Task.Run(() => taskCompletionSource.TrySetCanceled());
                    return;
                }

                if (_disposeCancellationToken.IsCancellationRequested)
                {
                    Task.Run(() =>
                        taskCompletionSource.TrySetException(
                            new ObjectDisposedException(nameof(AsyncPublisher))));
                    return;
                }

                if (IsModelClosed(out var shutdownEventArgs))
                {
                    Task.Run(() =>
                        taskCompletionSource.TrySetException(new AlreadyClosedException(shutdownEventArgs)));
                    return;
                }

                if (cancellationToken.CanBeCanceled)
                {
                    var registration = cancellationToken.Register(() =>
                    {
                        Task.Run(() => taskCompletionSource.TrySetCanceled());
                    });
                    taskCompletionSource.Task.ContinueWith(_ => { registration.Dispose(); });
                }

                seqNo = _model.NextPublishSeqNo;
            }
            catch (Exception ex)
            {
                TrackSafe(_diagnostics.TrackUnexpectedException, "Unable to start message publishing.", ex);
                Task.Run(() => taskCompletionSource.TrySetException(ex));
                return;
            }

            var publishArgs = new PublishArgs(publishQueueItem.Exchange, publishQueueItem.RoutingKey,
                publishQueueItem.Body, publishQueueItem.Properties);
            TrackSafe(_diagnostics.TrackPublishStarted, publishArgs, seqNo);
            var stopwatch = Stopwatch.StartNew();

            try
            {
                RegisterTaskCompletionSource(seqNo, taskCompletionSource);
                _model.BasicPublish(publishQueueItem.Exchange, publishQueueItem.RoutingKey,
                    publishQueueItem.Properties, publishQueueItem.Body);
            }
            catch (Exception ex)
            {
                TrackSafe(_diagnostics.TrackPublishFailed, publishArgs, seqNo, stopwatch.Elapsed, ex);
                TryRemoveSingleTaskCompletionSource(seqNo, out _);
                Task.Run(() => taskCompletionSource.TrySetException(ex));
                return;
            }

            TrackSafe(_diagnostics.TrackPublishSucceeded, publishArgs, seqNo, stopwatch.Elapsed);
        }

        private void HandleAckJob(Func<AckJob> dequeueAckQueueItem, Func<bool> isStopped)
        {
            var ackQueueItem = dequeueAckQueueItem();
            var deliveryTag = ackQueueItem.DeliveryTag;
            var multiple = ackQueueItem.Multiple;
            var ack = ackQueueItem.Ack;

            var ackArgs = new AckArgs(deliveryTag, multiple, ack);
            TrackSafe(_diagnostics.TrackAckStarted, ackArgs);
            var stopwatch = Stopwatch.StartNew();

            try
            {
                if (!multiple)
                {
                    if (TryRemoveSingleTaskCompletionSource(deliveryTag, out var source))
                    {
                        Task.Run(() => source.TrySetResult(ack));
                    }
                }
                else
                {
                    foreach (var source in RemoveAllTaskCompletionSourcesUpTo(deliveryTag))
                    {
                        Task.Run(() => source.TrySetResult(ack));
                    }
                }
            }
            catch (Exception ex)
            {
                TrackSafe(_diagnostics.TrackUnexpectedException,
                    $"Unable to process ack queue item: deliveryTag={deliveryTag}; multiple={multiple}; ack={ack}.",
                    ex);
                return;
            }

            TrackSafe(_diagnostics.TrackAckSucceeded, ackArgs, stopwatch.Elapsed);
        }

        public Task<bool> PublishAsync(
            string exchange, string routingKey, ReadOnlyMemory<byte> body, IBasicProperties properties,
            CancellationToken cancellationToken)
        {
            if (_disposeCancellationToken.IsCancellationRequested)
            {
                throw new ObjectDisposedException(nameof(AsyncPublisher));
            }

            if (cancellationToken.IsCancellationRequested)
            {
                return Task.FromCanceled<bool>(cancellationToken);
            }

            var publishJob = new PublishQueueItem(exchange, routingKey, body, properties, cancellationToken,
                new TaskCompletionSource<bool>());
            var tryCancelPublishJob = _publishLoop.Enqueue(publishJob);

            TrackSafe(_diagnostics.TrackPublishTaskEnqueued,
                new PublishArgs(exchange, routingKey, body, properties),
                CreateStatus());

            return WaitForPublishCompletedOrCancelled(publishJob, tryCancelPublishJob, cancellationToken);
        }

        private async Task<bool> WaitForPublishCompletedOrCancelled(PublishQueueItem publishJob,
            Func<bool> tryCancelJob, CancellationToken cancellationToken)
        {
            var jobTask = publishJob.TaskCompletionSource.Task;
            var firstCompletedTask = await Task.WhenAny(
                Task.Delay(-1, cancellationToken),
                jobTask
            ).ConfigureAwait(false);

            if (firstCompletedTask != jobTask && tryCancelJob())
            {
                return await Task.FromCanceled<bool>(cancellationToken).ConfigureAwait(false);
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

            _model.BasicAcks -= OnBasicAcks;
            _model.BasicNacks -= OnBasicNacks;

            _publishLoop.Stop().Wait();
            _ackLoop.Stop().Wait();

            // TODO: Set ObjectDisposedException for remaining publish jobs
            foreach (var source in RemoveAllTaskCompletionSourcesUpTo(ulong.MaxValue))
            {
                Task.Run(() => source.TrySetException(new ObjectDisposedException(nameof(AsyncPublisher))));
            }

            TrackSafe(_diagnostics.TrackDisposeSucceeded, stopwatch.Elapsed);
        }

        private void RegisterTaskCompletionSource(ulong deliveryTag, TaskCompletionSource<bool> taskCompletionSource)
        {
            lock (_completionSourceRegistry)
            {
                Interlocked.Increment(ref _completionSourceRegistrySize);
                _completionSourceRegistry.Register(deliveryTag, taskCompletionSource);
            }
        }

        private bool TryRemoveSingleTaskCompletionSource(ulong deliveryTag,
            out TaskCompletionSource<bool> taskCompletionSource)
        {
            lock (_completionSourceRegistry)
            {
                if (_completionSourceRegistry.TryRemoveSingle(deliveryTag, out taskCompletionSource))
                {
                    Interlocked.Decrement(ref _completionSourceRegistrySize);
                    return true;
                }

                return false;
            }
        }

        private IReadOnlyList<TaskCompletionSource<bool>> RemoveAllTaskCompletionSourcesUpTo(ulong deliveryTag)
        {
            lock (_completionSourceRegistry)
            {
                var result = _completionSourceRegistry.RemoveAllUpTo(deliveryTag);
                Interlocked.Add(ref _completionSourceRegistrySize, -result.Count);
                return result;
            }
        }

        private AsyncPublisherStatus CreateStatus()
        {
            return new AsyncPublisherStatus(_publishLoop.QueueSize, _ackLoop.QueueSize, _completionSourceRegistrySize);
        }

        private bool IsModelClosed(out ShutdownEventArgs shutdownEventArgs)
        {
            shutdownEventArgs = default;
            Interlocked.Exchange(ref shutdownEventArgs, _model.CloseReason);
            return !(shutdownEventArgs is null);
        }

        private class PublishQueueItem
        {
            public string Exchange { get; }
            public string RoutingKey { get; }
            public ReadOnlyMemory<byte> Body { get; }
            public IBasicProperties Properties { get; }
            public CancellationToken CancellationToken { get; }
            public TaskCompletionSource<bool> TaskCompletionSource { get; }

            public PublishQueueItem(string exchange, string routingKey, ReadOnlyMemory<byte> body,
                IBasicProperties properties, CancellationToken cancellationToken,
                TaskCompletionSource<bool> taskCompletionSource)
            {
                Exchange = exchange;
                RoutingKey = routingKey;
                Body = body;
                Properties = properties;
                CancellationToken = cancellationToken;
                TaskCompletionSource = taskCompletionSource;
            }
        }

        private readonly struct AckJob
        {
            public readonly ulong DeliveryTag;
            public readonly bool Multiple;
            public readonly bool Ack;

            public AckJob(ulong deliveryTag, bool multiple, bool ack)
            {
                DeliveryTag = deliveryTag;
                Multiple = multiple;
                Ack = ack;
            }
        }
    }
}