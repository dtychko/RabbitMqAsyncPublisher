using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace RabbitMqAsyncPublisher
{
    using static DiagnosticsUtils;

    public class AsyncPublisher : IAsyncPublisher<bool>
    {
        private readonly IModel _model;
        private readonly IAsyncPublisherDiagnostics _diagnostics;

        private readonly JobQueueLoop<PublishQueueItem> _publishLoop;
        private readonly JobQueueLoop<AckJob> _ackLoop;

        private readonly AsyncPublisherTaskCompletionSourceRegistry _completionSourceRegistry =
            new AsyncPublisherTaskCompletionSourceRegistry();

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
            var publishQueueItem = dequeuePublishQueueItem();
            var cancellationToken = publishQueueItem.CancellationToken;
            var taskCompletionSource = publishQueueItem.TaskCompletionSource;

            if (isStopped())
            {
                Task.Run(() =>
                    taskCompletionSource.TrySetException(
                        new ObjectDisposedException(nameof(AsyncPublisher))));
                return;
            }

            ulong seqNo;

            try
            {
                if (IsModelClosed(out var shutdownEventArgs))
                {
                    Task.Run(() =>
                        taskCompletionSource.TrySetException(new AlreadyClosedException(shutdownEventArgs)));
                    return;
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
                _completionSourceRegistry.Register(seqNo, taskCompletionSource);

                if (cancellationToken.IsCancellationRequested)
                {
                    _completionSourceRegistry.TryRemoveSingle(seqNo, out _);
                    Task.Run(() => taskCompletionSource.TrySetCanceled(cancellationToken));
                    return;
                }

                _model.BasicPublish(publishQueueItem.Exchange, publishQueueItem.RoutingKey,
                    publishQueueItem.Properties, publishQueueItem.Body);
            }
            catch (Exception ex)
            {
                TrackSafe(_diagnostics.TrackPublishFailed, publishArgs, seqNo, stopwatch.Elapsed, ex);
                _completionSourceRegistry.TryRemoveSingle(seqNo, out _);
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
                    if (_completionSourceRegistry.TryRemoveSingle(deliveryTag, out var source))
                    {
                        Task.Run(() => source.TrySetResult(ack));
                    }
                }
                else
                {
                    foreach (var source in _completionSourceRegistry.RemoveAllUpTo(deliveryTag))
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

            // TODO: Do we want to support cancellation after message is already published but not acked?

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

            _publishLoop.StopAsync().Wait();
            _ackLoop.StopAsync().Wait();

            foreach (var source in _completionSourceRegistry.RemoveAllUpTo(ulong.MaxValue))
            {
                Task.Run(() => source.TrySetException(new ObjectDisposedException(nameof(AsyncPublisher))));
            }

            TrackSafe(_diagnostics.TrackDisposeSucceeded, stopwatch.Elapsed);
        }

        private AsyncPublisherStatus CreateStatus()
        {
            return new AsyncPublisherStatus(_publishLoop.QueueSize, _ackLoop.QueueSize, _completionSourceRegistry.Size);
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