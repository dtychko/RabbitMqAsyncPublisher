using System;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace RabbitMqAsyncPublisher
{
    /// <summary>
    /// Allows to swap publisher implementation at runtime.
    /// Useful to integrate auto-recovery (which recreates publishers on recovery)
    /// with retrying/buffering publisher, which must be kept alive to preserve its state.
    /// </summary>
    public class AsyncPublisherProxy<TResult> : IAsyncPublisher<TResult>
    {
        private readonly object _syncRoot = new object();
        private IAsyncPublisher<TResult> _implementation;

        public AsyncPublisherProxy()
        {
            _implementation = new ClosedAsyncPublisher<TResult>(new ShutdownEventArgs(ShutdownInitiator.Application,
                200, "Proxy not initialized"));
        }

        public async Task<TResult> PublishAsync(string exchange, string routingKey, ReadOnlyMemory<byte> body,
            MessageProperties properties, string correlationId = null, CancellationToken cancellationToken = default)
        {
            try
            {
                return await _implementation.PublishAsync(exchange, routingKey, body, properties,
                    correlationId, cancellationToken).ConfigureAwait(false);
            }
            catch (ObjectDisposedException)
            {
                // If implementation is disposed, then most likely model/connection is closed,
                // so we should adapt such exception to AlreadyClosed,
                // which is expected to be thrown in such cases when publisher can't complete publish.
                throw new AlreadyClosedException(new ShutdownEventArgs(ShutdownInitiator.Application,
                    200, "Publisher implementation disposed"));
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
                    200, "Proxy reset"));
            }
        }

        void IDisposable.Dispose()
        {
            // Lifecycle of proxied implementation is controlled by external code, no need to dispose it here
        }

        public IDisposable ConnectTo(IAsyncPublisher<TResult> innerPublisher)
        {
            SetImplementation(innerPublisher);
            return new Disposable(() =>
            {
                // todo: catch when first throws an exception
                Reset();
                innerPublisher.Dispose();
            });
        }
    }
}