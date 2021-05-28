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

        private readonly JobQueueLoop<PublishJob> _publishLoop;
        private readonly JobQueueLoop<AckArgs> _ackLoop;

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

            _publishLoop = new JobQueueLoop<PublishJob>(HandlePublishJob, _diagnostics);
            _ackLoop = new JobQueueLoop<AckArgs>(HandleAckJob, _diagnostics);

            _model.BasicAcks += OnBasicAcks;
            _model.BasicNacks += OnBasicNacks;
        }

        private void OnBasicAcks(object sender, BasicAckEventArgs e)
        {
            var ackJob = new AckArgs(e.DeliveryTag, e.Multiple, true);
            _ackLoop.Enqueue(ackJob);
            TrackSafe(_diagnostics.TrackAckJobEnqueued, ackJob, CreateStatus());
        }

        private void OnBasicNacks(object sender, BasicNackEventArgs e)
        {
            var ackJob = new AckArgs(e.DeliveryTag, e.Multiple, false);
            _ackLoop.Enqueue(ackJob);
            TrackSafe(_diagnostics.TrackAckJobEnqueued, ackJob, CreateStatus());
        }

        private void HandlePublishJob(Func<PublishJob> dequeuePublishJob, CancellationToken cancellationToken)
        {
            var publishJob = dequeuePublishJob();

            if (cancellationToken.IsCancellationRequested)
            {
                // ReSharper disable once MethodSupportsCancellation
                Task.Run(() =>
                    publishJob.TaskCompletionSource.TrySetException(
                        new ObjectDisposedException(nameof(AsyncPublisher))));
                return;
            }

            ulong seqNo;

            try
            {
                if (IsModelClosed(out var shutdownEventArgs))
                {
                    // ReSharper disable once MethodSupportsCancellation
                    Task.Run(() =>
                        publishJob.TaskCompletionSource.TrySetException(new AlreadyClosedException(shutdownEventArgs)));
                    return;
                }

                seqNo = _model.NextPublishSeqNo;
            }
            catch (Exception ex)
            {
                TrackSafe(_diagnostics.TrackUnexpectedException, "Unable to start message publishing.", ex);
                // ReSharper disable once MethodSupportsCancellation
                Task.Run(() => publishJob.TaskCompletionSource.TrySetException(ex));
                return;
            }

            TrackSafe(_diagnostics.TrackPublishStarted, publishJob.Args, seqNo);
            var stopwatch = Stopwatch.StartNew();

            try
            {
                _completionSourceRegistry.Register(seqNo, publishJob.TaskCompletionSource);

                if (publishJob.CancellationToken.IsCancellationRequested)
                {
                    _completionSourceRegistry.TryRemoveSingle(seqNo, out _);
                    // ReSharper disable once MethodSupportsCancellation
                    Task.Run(() => publishJob.TaskCompletionSource.TrySetCanceled(publishJob.CancellationToken));
                    return;
                }

                _model.BasicPublish(publishJob.Args.Exchange, publishJob.Args.RoutingKey,
                    publishJob.Args.Properties, publishJob.Args.Body);
            }
            catch (Exception ex)
            {
                TrackSafe(_diagnostics.TrackPublishFailed, publishJob.Args, seqNo, stopwatch.Elapsed, ex);
                _completionSourceRegistry.TryRemoveSingle(seqNo, out _);
                // ReSharper disable once MethodSupportsCancellation
                Task.Run(() => publishJob.TaskCompletionSource.TrySetException(ex));
                return;
            }

            TrackSafe(_diagnostics.TrackPublishSucceeded, publishJob.Args, seqNo, stopwatch.Elapsed);
        }

        private void HandleAckJob(Func<AckArgs> dequeueAckJob, CancellationToken cancellationToken)
        {
            var ackJob = dequeueAckJob();

            TrackSafe(_diagnostics.TrackAckStarted, ackJob);
            var stopwatch = Stopwatch.StartNew();

            try
            {
                if (!ackJob.Multiple)
                {
                    if (_completionSourceRegistry.TryRemoveSingle(ackJob.DeliveryTag, out var source))
                    {
                        // ReSharper disable once MethodSupportsCancellation
                        Task.Run(() => source.TrySetResult(ackJob.Ack));
                    }
                }
                else
                {
                    foreach (var source in _completionSourceRegistry.RemoveAllUpTo(ackJob.DeliveryTag))
                    {
                        // ReSharper disable once MethodSupportsCancellation
                        Task.Run(() => source.TrySetResult(ackJob.Ack));
                    }
                }
            }
            catch (Exception ex)
            {
                TrackSafe(_diagnostics.TrackUnexpectedException,
                    $"Unable to process ack queue item: deliveryTag={ackJob.DeliveryTag}; multiple={ackJob.Multiple}; ack={ackJob.Ack}.",
                    ex);
                return;
            }

            TrackSafe(_diagnostics.TrackAckSucceeded, ackJob, stopwatch.Elapsed);
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

            var publishJob = new PublishJob(exchange, routingKey, body, properties, cancellationToken,
                new TaskCompletionSource<bool>());
            var tryCancelPublishJob = _publishLoop.Enqueue(publishJob);

            TrackSafe(_diagnostics.TrackPublishTaskEnqueued, publishJob.Args, CreateStatus());

            return WaitForPublishCompletedOrCancelled(publishJob, tryCancelPublishJob, cancellationToken);
        }

        private async Task<bool> WaitForPublishCompletedOrCancelled(PublishJob job,
            Func<bool> tryCancelJob, CancellationToken cancellationToken)
        {
            var jobTask = job.TaskCompletionSource.Task;
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

            // ReSharper disable MethodSupportsCancellation
            _publishLoop.StopAsync().Wait();
            _ackLoop.StopAsync().Wait();
            // ReSharper restore MethodSupportsCancellation

            foreach (var source in _completionSourceRegistry.RemoveAllUpTo(ulong.MaxValue))
            {
                // ReSharper disable once MethodSupportsCancellation
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

        private readonly struct PublishJob
        {
            public readonly PublishArgs Args;
            public readonly CancellationToken CancellationToken;
            public readonly TaskCompletionSource<bool> TaskCompletionSource;

            public PublishJob(string exchange, string routingKey, ReadOnlyMemory<byte> body,
                IBasicProperties properties, CancellationToken cancellationToken,
                TaskCompletionSource<bool> taskCompletionSource)
            {
                Args = new PublishArgs(exchange, routingKey, body, properties);
                CancellationToken = cancellationToken;
                TaskCompletionSource = taskCompletionSource;
            }
        }
    }

    public readonly struct PublishArgs
    {
        public string Exchange { get; }
        public string RoutingKey { get; }
        public ReadOnlyMemory<byte> Body { get; }
        public IBasicProperties Properties { get; }

        public PublishArgs(string exchange, string routingKey, ReadOnlyMemory<byte> body,
            IBasicProperties properties)
        {
            Exchange = exchange;
            RoutingKey = routingKey;
            Body = body;
            Properties = properties;
        }
    }

    public readonly struct AckArgs
    {
        public ulong DeliveryTag { get; }
        public bool Multiple { get; }
        public bool Ack { get; }

        public AckArgs(ulong deliveryTag, bool multiple, bool ack)
        {
            DeliveryTag = deliveryTag;
            Multiple = multiple;
            Ack = ack;
        }
    }
}