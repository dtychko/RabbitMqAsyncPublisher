using System;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace RabbitMqAsyncPublisher
{
    public class AsyncPublisherSyncDecorator<TResult> : IAsyncPublisher<TResult>
    {
        private readonly IAsyncPublisher<TResult> _decorated;
        private readonly object _publishSyncRoot = new object();

        public IModel Model => _decorated.Model;

        public AsyncPublisherSyncDecorator(IAsyncPublisher<TResult> decorated)
        {
            _decorated = decorated;
        }

        public Task<TResult> PublishAsync(
            string exchange,
            string routingKey,
            ReadOnlyMemory<byte> body,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            lock (_publishSyncRoot)
            {
                return _decorated.PublishAsync(exchange, routingKey, body, cancellationToken);
            }
        }

        public void Dispose()
        {
            _decorated.Dispose();
        }
    }
}