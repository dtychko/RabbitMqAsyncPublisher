using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace RabbitMqAsyncPublisher
{
    using static AsyncPublisherUtils;

    public class AsyncPublisher : IAsyncPublisher<bool>
    {
        private readonly IModel _model;
        private readonly IAsyncPublisherDiagnostics _diagnostics;

        private readonly JobQueueLoop<PublishJob<bool>> _publishLoop;
        private readonly JobQueueLoop<AckArgs> _ackLoop;

        private readonly AsyncPublisherTaskCompletionSourceRegistry _completionSourceRegistry =
            new AsyncPublisherTaskCompletionSourceRegistry();

        private readonly CancellationTokenSource _disposeCancellationSource = new CancellationTokenSource();
        private readonly CancellationToken _disposeCancellationToken;

        public AsyncPublisher(
            IModel model,
            IAsyncPublisherDiagnostics diagnostics = null)
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
            _diagnostics = diagnostics ?? AsyncPublisherDiagnostics.NoDiagnostics;

            _disposeCancellationToken = _disposeCancellationSource.Token;

            _publishLoop = new JobQueueLoop<PublishJob<bool>>(HandlePublishJob, _diagnostics);
            _ackLoop = new JobQueueLoop<AckArgs>(HandleAckJob, _diagnostics);

            _model.BasicAcks += OnBasicAcks;
            _model.BasicNacks += OnBasicNacks;
        }

        private void HandlePublishJob(Func<PublishJob<bool>> dequeuePublishJob)
        {
            var publishJob = dequeuePublishJob();

            TrackSafe(_diagnostics.TrackPublishJobStarted, publishJob.Args, CreateStatus());
            var stopwatch = Stopwatch.StartNew();

            if (!CanHandlePublishJob(publishJob, stopwatch, out var seqNo))
            {
                return;
            }

            try
            {
                _completionSourceRegistry.Register(seqNo, publishJob.TaskCompletionSource);
                _model.BasicPublish(publishJob.Args.Exchange, publishJob.Args.RoutingKey,
                    publishJob.Args.Properties.ApplyTo(_model.CreateBasicProperties()),
                    publishJob.Args.Body);
                TrackSafe(_diagnostics.TrackPublishJobCompleted,
                    publishJob.Args, CreateStatus(), seqNo, stopwatch.Elapsed);
            }
            catch (Exception ex)
            {
                TrackSafe(_diagnostics.TrackPublishJobFailed,
                    publishJob.Args, CreateStatus(), seqNo, stopwatch.Elapsed, ex);
                _completionSourceRegistry.TryRemoveSingle(seqNo, out _);
                ScheduleTrySetException(publishJob.TaskCompletionSource, ex);
            }
        }

        private bool CanHandlePublishJob(PublishJob<bool> publishJob, Stopwatch stopwatch, out ulong seqNo)
        {
            try
            {
                seqNo = _model.NextPublishSeqNo;

                if (_disposeCancellationToken.IsCancellationRequested)
                {
                    var ex = (Exception) new ObjectDisposedException(GetType().Name);
                    TrackSafe(_diagnostics.TrackPublishJobFailed,
                        publishJob.Args, CreateStatus(), seqNo, stopwatch.Elapsed, ex);
                    ScheduleTrySetException(publishJob.TaskCompletionSource, ex);
                    return false;
                }

                if (publishJob.CancellationToken.IsCancellationRequested)
                {
                    TrackSafe(_diagnostics.TrackPublishJobCancelled,
                        publishJob.Args, CreateStatus(), seqNo, stopwatch.Elapsed);
                    ScheduleTrySetCanceled(publishJob.TaskCompletionSource, publishJob.CancellationToken);
                    return false;
                }

                if (IsModelClosed(out var shutdownEventArgs))
                {
                    var ex = new AlreadyClosedException(shutdownEventArgs);
                    TrackSafe(_diagnostics.TrackPublishJobFailed,
                        publishJob.Args, CreateStatus(), seqNo, stopwatch.Elapsed, ex);
                    ScheduleTrySetException(publishJob.TaskCompletionSource, ex);
                    return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                TrackSafe(_diagnostics.TrackUnexpectedException,
                    $"Unable to handle publish job: " +
                    $"{publishJob.Args}; " +
                    $"{nameof(stopwatch.ElapsedMilliseconds)}={stopwatch.ElapsedMilliseconds}",
                    ex);
                ScheduleTrySetException(publishJob.TaskCompletionSource, ex);
                seqNo = default;
                return false;
            }
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

        private void HandleAckJob(Func<AckArgs> dequeueAckJob)
        {
            var ackJob = dequeueAckJob();

            TrackSafe(_diagnostics.TrackAckJobStarted, ackJob, CreateStatus());
            var stopwatch = Stopwatch.StartNew();

            try
            {
                if (!ackJob.Multiple)
                {
                    if (_completionSourceRegistry.TryRemoveSingle(ackJob.DeliveryTag, out var source))
                    {
                        ScheduleTrySetResult(source, ackJob.Ack);
                    }
                }
                else
                {
                    // TODO: consider using single Task.Run with foreach inside to optimize thread usage
                    foreach (var source in _completionSourceRegistry.RemoveAllUpTo(ackJob.DeliveryTag))
                    {
                        ScheduleTrySetResult(source, ackJob.Ack);
                    }
                }
            }
            catch (Exception ex)
            {
                TrackSafe(_diagnostics.TrackUnexpectedException,
                    $"Unable to process ack queue item: " +
                    $"{ackJob}; " +
                    $"{nameof(stopwatch.ElapsedMilliseconds)}={stopwatch.ElapsedMilliseconds}",
                    ex);
                return;
            }

            TrackSafe(_diagnostics.TrackAckJobCompleted, ackJob, CreateStatus(), stopwatch.Elapsed);
        }

        public Task<bool> PublishAsync(string exchange, string routingKey, ReadOnlyMemory<byte> body,
            MessageProperties properties, string correlationId = null, CancellationToken cancellationToken = default)
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
                _model.BasicAcks -= OnBasicAcks;
                _model.BasicNacks -= OnBasicNacks;

                await _publishLoop.StopAsync().ConfigureAwait(false);
                await _ackLoop.StopAsync().ConfigureAwait(false);

                foreach (var source in _completionSourceRegistry.RemoveAllUpTo(ulong.MaxValue))
                {
                    ScheduleTrySetException(source, new ObjectDisposedException(GetType().Name));
                }
            }
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
    }

    internal readonly struct PublishJob<TResult>
    {
        public readonly PublishArgs Args;
        public readonly CancellationToken CancellationToken;
        public readonly TaskCompletionSource<TResult> TaskCompletionSource;

        public PublishJob(PublishArgs args, CancellationToken cancellationToken,
            TaskCompletionSource<TResult> taskCompletionSource)
        {
            Args = args;
            CancellationToken = cancellationToken;
            TaskCompletionSource = taskCompletionSource;
        }
    }

    public readonly struct PublishArgs
    {
        public string Exchange { get; }
        public string RoutingKey { get; }
        public ReadOnlyMemory<byte> Body { get; }
        public MessageProperties Properties { get; }
        public string CorrelationId { get; }
        public DateTimeOffset StartedAt { get; }

        public PublishArgs(string exchange, string routingKey, ReadOnlyMemory<byte> body,
            MessageProperties properties, string correlationId)
        {
            Exchange = exchange;
            RoutingKey = routingKey;
            Body = body;
            Properties = properties ?? MessageProperties.Default;
            CorrelationId = correlationId ?? Guid.NewGuid().ToString("D");
            StartedAt = DateTimeOffset.UtcNow;
        }

        public override string ToString()
        {
            return $"{nameof(Exchange)}={Exchange}; " +
                   $"{nameof(RoutingKey)}={RoutingKey}; " +
                   $"{nameof(Body)}.{nameof(Body.Length)}={Body.Length}; " +
                   $"{nameof(Properties)}={Properties}; " +
                   $"{nameof(CorrelationId)}={CorrelationId}; " +
                   $"{nameof(StartedAt)}={StartedAt}";
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

        public override string ToString()
        {
            return $"{nameof(DeliveryTag)}={DeliveryTag}; " +
                   $"{nameof(Multiple)}={Multiple}; " +
                   $"{nameof(Ack)}={Ack}";
        }
    }

    public readonly struct AsyncPublisherStatus
    {
        public readonly int PublishQueueSize;
        public readonly int AckQueueSize;
        public readonly int CompletionSourceRegistrySize;

        public AsyncPublisherStatus(int publishQueueSize, int ackQueueSize, int completionSourceRegistrySize)
        {
            PublishQueueSize = publishQueueSize;
            AckQueueSize = ackQueueSize;
            CompletionSourceRegistrySize = completionSourceRegistrySize;
        }

        public override string ToString()
        {
            return $"{nameof(PublishQueueSize)}={PublishQueueSize}; " +
                   $"{nameof(AckQueueSize)}={AckQueueSize}; " +
                   $"{nameof(CompletionSourceRegistrySize)}={CompletionSourceRegistrySize}";
        }
    }
}