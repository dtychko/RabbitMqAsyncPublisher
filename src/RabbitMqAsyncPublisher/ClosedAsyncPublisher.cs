using System;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace RabbitMqAsyncPublisher
{
    internal class ClosedAsyncPublisher<TResult> : IAsyncPublisher<TResult>
    {
        private readonly ShutdownEventArgs _closeReason;

        public ClosedAsyncPublisher(ShutdownEventArgs closeReason)
        {
            _closeReason = closeReason;
        }

        public Task<TResult> PublishAsync(string exchange,
            string routingKey,
            ReadOnlyMemory<byte> body,
            MessageProperties properties,
            string correlationId = null,
            CancellationToken cancellationToken = default)
        {
            throw new AlreadyClosedException(_closeReason);
        }

        void IDisposable.Dispose()
        {
        }
    }
}