using System;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace RabbitMqAsyncPublisher
{
    /// <remarks>
    /// RabbitMQ expects access to <see cref="IModel"/> to be mutually exclusive,
    /// i.e. multiple publishers can't call `BasicPublish` at the same time.
    /// </remarks>
    public class AsyncPublisherSyncDecorator<TResult> : IAsyncPublisher<TResult>
    {
        private readonly IAsyncPublisher<TResult> _decorated;
        private readonly object _publishSyncRoot = new object();

        public AsyncPublisherSyncDecorator(IAsyncPublisher<TResult> decorated)
        {
            _decorated = decorated;
        }

        public Task<TResult> PublishUnsafeAsync(
            string exchange,
            string routingKey,
            ReadOnlyMemory<byte> body,
            IBasicProperties properties,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            lock (_publishSyncRoot)
            {
                return _decorated.PublishUnsafeAsync(exchange, routingKey, body, properties, cancellationToken);
            }
        }

        public void Dispose()
        {
            _decorated.Dispose();
        }
    }
}