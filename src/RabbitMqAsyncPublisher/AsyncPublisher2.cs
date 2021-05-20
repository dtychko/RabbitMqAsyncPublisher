using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace RabbitMqAsyncPublisher
{
    /// <summary>
    /// Limitations:
    /// 1. Doesn't listen to "IModel.BasicReturn" event, as results doesn't handle "returned" messages
    /// </summary>
    public class AsyncPublisher : IAsyncPublisher<bool>
    {
        private readonly IAsyncPublisherDiagnostics _diagnostics;
        private readonly AsyncPublisherTaskRegistry _taskRegistry = new AsyncPublisherTaskRegistry();
        private int _isDisposed;

        public IModel Model { get; }

        public AsyncPublisher(IModel model)
            : this(model, EmptyDiagnostics.Instance)
        {
        }

        public AsyncPublisher(IModel model, IAsyncPublisherDiagnostics diagnostics)
        {
            // Heuristic based on reverse engineering of "RabbitMQ.Client" lib
            // that helps to make sure that "ConfirmSelect" method was called on the model
            // to enable confirm mode.
            if (model.NextPublishSeqNo == 0)
            {
                throw new ArgumentException("Channel should be in confirm mode.");
            }

            Model = model;
            _diagnostics = diagnostics;

            Model.BasicAcks += OnBasicAcks;
            Model.BasicNacks += OnBasicNacks;
            Model.ModelShutdown += OnModelShutdown;
        }

        private void OnBasicAcks(object sender, BasicAckEventArgs args)
        {
            ProcessEvent(
                () => ProcessDeliveryTag(args.DeliveryTag, args.Multiple, true),
                () => _diagnostics.TrackBasicAcksEventProcessing(args),
                duration => _diagnostics.TrackBasicAcksEventProcessingCompleted(args, duration),
                (duration, ex) => _diagnostics.TrackBasicAcksEventProcessingFailed(args, duration, ex)
            );
        }

        private void OnBasicNacks(object sender, BasicNackEventArgs args)
        {
            ProcessEvent(
                () => ProcessDeliveryTag(args.DeliveryTag, args.Multiple, false),
                () => _diagnostics.TrackBasicNacksEventProcessing(args),
                duration => _diagnostics.TrackBasicNacksEventProcessingCompleted(args, duration),
                (duration, ex) => _diagnostics.TrackBasicNacksEventProcessingFailed(args, duration, ex)
            );
        }

        private void OnModelShutdown(object sender, ShutdownEventArgs args)
        {
            ProcessEvent(
                () => _taskRegistry.SetExceptionForAll(new AlreadyClosedException(args)),
                () => _diagnostics.TrackModelShutdownEventProcessing(args),
                duration => _diagnostics.TrackModelShutdownEventProcessingCompleted(args, duration),
                (duration, ex) => _diagnostics.TrackModelShutdownEventProcessingFailed(args, duration, ex)
            );
        }

        private void ProcessDeliveryTag(ulong deliveryTag, bool multiple, bool ack)
        {
            if (!multiple)
            {
                _taskRegistry.SetResult(deliveryTag, ack);
                return;
            }

            _taskRegistry.SetResultForAllUpTo(deliveryTag, ack);
        }

        public async Task<bool> PublishUnsafeAsync(
            string exchange,
            string routingKey,
            ReadOnlyMemory<byte> body,
            IBasicProperties properties,
            CancellationToken cancellationToken)
        {
            ThrowIfDisposed();

            var args = new PublishUnsafeArgs(exchange, routingKey, body, properties, Model.NextPublishSeqNo);
            _diagnostics.TrackPublishUnsafe(args);
            var stopwatch = Stopwatch.StartNew();

            try
            {
                // var publishTask = _state.PublishAsync(exchange, routingKey, body, properties, cancellationToken);
                cancellationToken.ThrowIfCancellationRequested();

                var seqNo = Model.NextPublishSeqNo;

                // Task should be registered before calling "BasicPublish" method
                // to make sure that the task already presents in the registry
                // when "Ack" from the broker is received.
                // Otherwise race condition could occur because the model notifies about received acks in parallel thread.  
                var publishTask = _taskRegistry.Register(seqNo, cancellationToken);

                try
                {
                    Model.BasicPublish(exchange, routingKey, properties, body);
                }
                catch (Exception ex)
                {
                    // Make sure that task is removed from the registry in case of immediate exception.
                    // Otherwise memory leak could occur.
                    _taskRegistry.SetException(seqNo, ex);
                }

                _diagnostics.TrackPublishUnsafeBasicPublishCompleted(args, stopwatch.Elapsed);

                var acknowledged = await publishTask;
                _diagnostics.TrackPublishUnsafeCompleted(args, stopwatch.Elapsed, acknowledged);

                return acknowledged;
            }
            catch (OperationCanceledException)
            {
                _diagnostics.TrackPublishUnsafeCanceled(args, stopwatch.Elapsed);

                throw;
            }
            catch (Exception ex)
            {
                _diagnostics.TrackPublishUnsafeFailed(args, stopwatch.Elapsed, ex);

                throw;
            }
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
            {
                return;
            }

            Model.BasicAcks -= OnBasicAcks;
            Model.BasicNacks -= OnBasicNacks;
            Model.ModelShutdown -= OnModelShutdown;

            _taskRegistry.SetExceptionForAll(new ObjectDisposedException(nameof(AsyncPublisher)));
        }

        private void ThrowIfDisposed()
        {
            if (_isDisposed == 1)
            {
                throw new ObjectDisposedException(nameof(AsyncPublisher));
            }
        }

        private static void ProcessEvent(
            Action process,
            Action onProcessing,
            Action<TimeSpan> onProcessingCompleted,
            Action<TimeSpan, Exception> onProcessingFailed)
        {
            onProcessing();
            var stopwatch = Stopwatch.StartNew();

            try
            {
                process();
                onProcessingCompleted(stopwatch.Elapsed);
            }
            catch (Exception ex)
            {
                onProcessingFailed(stopwatch.Elapsed, ex);
            }
        }
    }
}