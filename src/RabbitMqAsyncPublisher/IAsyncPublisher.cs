using System;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace RabbitMqAsyncPublisher
{
    public interface IAsyncPublisher<TResult> : IDisposable
    {
        Task<TResult> PublishAsync(string exchange,
            string routingKey,
            ReadOnlyMemory<byte> body,
            IBasicProperties properties,
            string correlationId = null,
            CancellationToken cancellationToken = default);
    }
}