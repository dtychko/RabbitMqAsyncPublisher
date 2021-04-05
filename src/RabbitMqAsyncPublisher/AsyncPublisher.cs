using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitMqAsyncPublisher
{
    public class AsyncPublisher
    {
        private readonly IModel _model;
        private readonly string _queueName;

        private readonly ConcurrentDictionary<ulong, TaskCompletionSource<bool>> _sources =
            new ConcurrentDictionary<ulong, TaskCompletionSource<bool>>();

        private readonly ConcurrentQueue<ulong> _queue = new ConcurrentQueue<ulong>();

        private readonly object _ackSyncRoot = new object();
        private readonly object _publishSyncRoot = new object();

        public AsyncPublisher(IModel model, string queueName)
        {
            _model = model;
            _queueName = queueName;

            // TODO: _model.BasicNacks event should be listened as well
            _model.BasicAcks += OnBasicAcks;
        }

        private void OnBasicAcks(object sender, BasicAckEventArgs args)
        {
            lock (_ackSyncRoot)
            {
                if (args.Multiple)
                {
                    AckMultiple(args.DeliveryTag);
                }
                else
                {
                    Ack(args.DeliveryTag);
                }
            }
        }

        private void Ack(ulong deliveryTag)
        {
            if (_queue.TryPeek(out var seqNo) && seqNo == deliveryTag)
            {
                _queue.TryDequeue(out _);
            }

            if (_sources.TryRemove(seqNo, out var source))
            {
                source.SetResult(true);
            }
        }

        private void AckMultiple(ulong deliveryTag)
        {
            while (_queue.TryPeek(out var seqNo) && seqNo <= deliveryTag)
            {
                _queue.TryDequeue(out _);

                if (_sources.TryRemove(seqNo, out var source))
                {
                    source.SetResult(true);
                }
            }
        }

        public Task PublishAsync(ReadOnlyMemory<byte> message)
        {
            lock (_publishSyncRoot)
            {
                var taskCompletionSource = new TaskCompletionSource<bool>();
                var seqNo = _model.NextPublishSeqNo;

                _sources[seqNo] = taskCompletionSource;
                _queue.Enqueue(seqNo);

                _model.BasicPublish("", _queueName, _model.CreateBasicProperties(), message);

                return taskCompletionSource.Task;
            }
        }
    }
}