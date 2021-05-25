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

        public Task<TResult> PublishUnsafeAsync(string exchange, string routingKey, ReadOnlyMemory<byte> body,
            IBasicProperties properties,
            CancellationToken cancellationToken)
        {
            throw new AlreadyClosedException(_closeReason);
        }

        void IDisposable.Dispose()
        {
        }
    }
}