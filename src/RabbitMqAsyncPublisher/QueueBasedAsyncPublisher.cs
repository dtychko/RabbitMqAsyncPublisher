using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace RabbitMqAsyncPublisher
{
    public class QueueBasedAsyncPublisher : IAsyncPublisher<bool>
    {
        private readonly ConcurrentQueue<PublishQueueItem> _publishQueue = new ConcurrentQueue<PublishQueueItem>();
        private readonly ConcurrentQueue<AckQueueItem> _ackQueue = new ConcurrentQueue<AckQueueItem>();

        private readonly AsyncManualResetEvent _publishEvent = new AsyncManualResetEvent(false);
        private readonly AsyncManualResetEvent _ackEvent = new AsyncManualResetEvent(false);

        private readonly Task _publishLoop;
        private readonly Task _ackLoop;

        private readonly AsyncPublisherTaskCompletionSourceRegistry _completionSourceRegistry =
            new AsyncPublisherTaskCompletionSourceRegistry();

        private volatile int _isDisposed;

        public IModel Model { get; }

        public QueueBasedAsyncPublisher(IModel model)
        {
            Model = model;

            // TODO: Make sure that "BasicAcks" and "BasicNacks" events are always fired on a single thread.
            Model.BasicAcks += OnBasicAcks;
            Model.BasicNacks += OnBasicNacks;

            _publishLoop = Task.Run(StartPublishLoop);
            _ackLoop = Task.Run(StartAckLoop);
        }

        private void OnBasicAcks(object sender, BasicAckEventArgs e)
        {
            _ackQueue.Enqueue(new AckQueueItem(e.DeliveryTag, e.Multiple, true));
            _ackEvent.Set();
        }

        private void OnBasicNacks(object sender, BasicNackEventArgs e)
        {
            _ackQueue.Enqueue(new AckQueueItem(e.DeliveryTag, e.Multiple, false));
            _ackEvent.Set();
        }

        private async void StartPublishLoop()
        {
            try
            {
                await _publishEvent.WaitAsync().ConfigureAwait(false);

                while (_isDisposed == 0 || !_publishQueue.IsEmpty)
                {
                    _publishEvent.Reset();
                    RunPublishInnerLoop();
                    await _publishEvent.WaitAsync().ConfigureAwait(false);
                }
            }
            catch
            {
                // Ignore
            }
        }

        private async void StartAckLoop()
        {
            try
            {
                await _ackEvent.WaitAsync().ConfigureAwait(false);

                while (_isDisposed == 0 || !_ackQueue.IsEmpty)
                {
                    _ackEvent.Reset();
                    RunAckInnerLoop();
                    await _ackEvent.WaitAsync().ConfigureAwait(false);
                }
            }
            catch
            {
                // Ignore
            }
        }

        private void RunPublishInnerLoop()
        {
            while (_publishQueue.TryDequeue(out var publishQueueItem))
            {
                var cancellationToke = publishQueueItem.CancellationToken;
                var taskCompletionSource = publishQueueItem.TaskCompletionSource;

                if (cancellationToke.IsCancellationRequested)
                {
                    Task.Run(() => taskCompletionSource.TrySetCanceled());
                }

                // TODO: Support delayed cancellation
                // TODO: cancellationToke.Register(() => taskCompletionSource.TrySetCanceled());

                if (_isDisposed == 1)
                {
                    Task.Run(() =>
                        taskCompletionSource.TrySetException(
                            new ObjectDisposedException(nameof(QueueBasedAsyncPublisher))));
                    continue;
                }

                if (IsModelClosed(out var shutdownEventArgs))
                {
                    Task.Run(() => taskCompletionSource.TrySetException(new AlreadyClosedException(shutdownEventArgs)));
                    continue;
                }

                var seqNo = Model.NextPublishSeqNo;
                RegisterTaskCompletionSource(seqNo, taskCompletionSource);

                try
                {
                    Model.BasicPublish(publishQueueItem.Exchange, publishQueueItem.RoutingKey,
                        publishQueueItem.Properties, publishQueueItem.Body);
                }
                catch (Exception ex)
                {
                    TryRemoveSingleTaskCompletionSource(seqNo, out _);
                    Task.Run(() => taskCompletionSource.TrySetException(ex));
                }
            }
        }

        private void RunAckInnerLoop()
        {
            while (_ackQueue.TryDequeue(out var ackQueueItem))
            {
                var deliveryTag = ackQueueItem.DeliveryTag;
                var multiple = ackQueueItem.Multiple;
                var ack = ackQueueItem.Ack;

                if (!multiple)
                {
                    if (TryRemoveSingleTaskCompletionSource(deliveryTag, out var source))
                    {
                        Task.Run(() => source.TrySetResult(ack));
                    }

                    continue;
                }

                foreach (var source in RemoveAllTaskCompletionSourcesUpTo(deliveryTag))
                {
                    Task.Run(() => source.TrySetResult(ack));
                }
            }
        }

        public Task<bool> PublishUnsafeAsync(
            string exchange,
            string routingKey,
            ReadOnlyMemory<byte> body,
            IBasicProperties properties,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var taskCompletionSource = new TaskCompletionSource<bool>();
            var queueItem = new PublishQueueItem(exchange, routingKey, body, properties, cancellationToken,
                taskCompletionSource);

            lock (_publishQueue)
            {
                if (_isDisposed == 1)
                {
                    throw new ObjectDisposedException(nameof(QueueBasedAsyncPublisher));
                }

                _publishQueue.Enqueue(queueItem);
            }

            _publishEvent.Set();

            return taskCompletionSource.Task;
        }

        public void Dispose()
        {
            bool shouldDispose;

            lock (_publishQueue)
            {
                shouldDispose = _isDisposed == 0;
                _isDisposed = 1;
            }

            if (!shouldDispose)
            {
                Task.WaitAll(_publishLoop, _ackLoop);
                return;
            }

            Model.BasicAcks -= OnBasicAcks;
            Model.BasicNacks -= OnBasicNacks;

            _publishEvent.Set();
            _ackEvent.Set();

            Task.WaitAll(_publishLoop, _ackLoop);

            foreach (var source in RemoveAllTaskCompletionSourcesUpTo(ulong.MaxValue))
            {
                Task.Run(() => source.TrySetException(new ObjectDisposedException(nameof(QueueBasedAsyncPublisher))));
            }
        }


        private void RegisterTaskCompletionSource(ulong deliveryTag, TaskCompletionSource<bool> taskCompletionSource)
        {
            lock (_completionSourceRegistry)
            {
                _completionSourceRegistry.Register(deliveryTag, taskCompletionSource);
            }
        }

        private bool TryRemoveSingleTaskCompletionSource(ulong deliveryTag,
            out TaskCompletionSource<bool> taskCompletionSource)
        {
            lock (_completionSourceRegistry)
            {
                return _completionSourceRegistry.TryRemoveSingle(deliveryTag, out taskCompletionSource);
            }
        }

        private IReadOnlyList<TaskCompletionSource<bool>> RemoveAllTaskCompletionSourcesUpTo(ulong deliveryTag)
        {
            lock (_completionSourceRegistry)
            {
                return _completionSourceRegistry.RemoveAllUpTo(deliveryTag);
            }
        }

        private bool IsModelClosed(out ShutdownEventArgs shutdownEventArgs)
        {
            shutdownEventArgs = default;
            Interlocked.Exchange(ref shutdownEventArgs, Model.CloseReason);
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

        private class AckQueueItem
        {
            public ulong DeliveryTag { get; }
            public bool Multiple { get; }
            public bool Ack { get; }

            public AckQueueItem(ulong deliveryTag, bool multiple, bool ack)
            {
                DeliveryTag = deliveryTag;
                Multiple = multiple;
                Ack = ack;
            }
        }
    }
}