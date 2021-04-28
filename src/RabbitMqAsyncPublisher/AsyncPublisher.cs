using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

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

            _model.BasicAcks += OnBasicAcks;
            // TODO: Return "false" when message was nacked
            _model.BasicNacks += (sender, args) => { Console.WriteLine(" >> Model:BasicNacks"); };
            _model.BasicRecoverOk += (sender, args) => { Console.WriteLine(" >> Model:BasicRecoveryOk"); };
            _model.ModelShutdown += (sender, args) =>
            {
                lock (_publishSyncRoot)
                lock (_ackSyncRoot)
                {
                    while (_queue.TryDequeue(out var seqno))
                    {
                        if (_sources.TryRemove(seqno, out var source))
                        {
                            source.TrySetException(new AlreadyClosedException(args));
                        }
                    }
                }

                Console.WriteLine(" >> Model:ModelShutdown");
            };
            ((IRecoverable) _model).Recovery += (sender, args) => { Console.WriteLine(" >> Model:Recovery"); };
        }

        private void OnBasicAcks(object sender, BasicAckEventArgs args)
        {
            Console.WriteLine(" >> Model:BasicAcks");
            Console.WriteLine($"deliveryTag = {args.DeliveryTag}");

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

        /// <summary>
        /// </summary>
        /// <param name="message"></param>
        /// <returns>True id acked, False is nacked</returns>
        /// <exception cref="AlreadyClosedException">When model is already closed</exception>
        public Task<bool> PublishAsync(ReadOnlyMemory<byte> message)
        {
            lock (_publishSyncRoot)
            {
                var taskCompletionSource = new TaskCompletionSource<bool>();
                var seqNo = _model.NextPublishSeqNo;
                Console.WriteLine($"seqno = {seqNo}");

                Console.WriteLine($"_source.Count = {_sources.Count}; _queue.Count = {_queue.Count}");
                _sources[seqNo] = taskCompletionSource;
                _queue.Enqueue(seqNo);

                _model.BasicPublish("", _queueName, _model.CreateBasicProperties(), message);

                return taskCompletionSource.Task;
            }
        }
    }
}