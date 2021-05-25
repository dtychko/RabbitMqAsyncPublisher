using System;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace RabbitMqAsyncPublisher
{
    public class AsyncPublisherProxy<TResult> : IAsyncPublisher<TResult>
    {
        private readonly object _syncRoot = new object();
        private IAsyncPublisher<TResult> _implementation;

        public AsyncPublisherProxy()
        {
            _implementation = new ClosedAsyncPublisher<TResult>(new ShutdownEventArgs(ShutdownInitiator.Application,
                Constants.ReplySuccess, "Proxy not initialized"));
        }

        public async Task<TResult> PublishUnsafeAsync(
            string exchange, string routingKey, ReadOnlyMemory<byte> body,
            IBasicProperties properties,
            CancellationToken cancellationToken)
        {
            try
            {
                return await _implementation.PublishUnsafeAsync(exchange, routingKey, body, properties,
                    cancellationToken).ConfigureAwait(false);
            }
            catch (ObjectDisposedException)
            {
                throw new AlreadyClosedException(new ShutdownEventArgs(ShutdownInitiator.Application,
                    Constants.ReplySuccess, "Publisher implementation disposed"));
            }
        }

        public void SetImplementation(IAsyncPublisher<TResult> implementation)
        {
            lock (_syncRoot)
            {
                _implementation = implementation;
            }
        }

        public void Reset()
        {
            lock (_syncRoot)
            {
                _implementation = new ClosedAsyncPublisher<TResult>(new ShutdownEventArgs(ShutdownInitiator.Application,
                    Constants.ReplySuccess, "Proxy reset"));
            }
        }

        void IDisposable.Dispose()
        {
        }
    }
}