using System;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace RabbitMqAsyncPublisher
{
    public class AsyncPublisherWithBuffer<TResult> : IAsyncPublisher<TResult>
    {
        private readonly IAsyncPublisher<TResult> _decorated;
        private readonly ManualResetEventSlim _manualResetEvent = new ManualResetEventSlim(true);
        private volatile bool _isDisposed;

        private readonly object _syncRoot = new object();
        private readonly int _nonAcknowledgedBytesSoftLimit;
        private int _nonAcknowledgedBytes;

        public AsyncPublisherWithBuffer(IAsyncPublisher<TResult> decorated, int nonAcknowledgedBytesSoftLimit)
        {
            _decorated = decorated;
            _nonAcknowledgedBytesSoftLimit = nonAcknowledgedBytesSoftLimit;
        }

        public async Task<TResult> PublishAsync(
            string exchange,
            string routingKey,
            ReadOnlyMemory<byte> body,
            IBasicProperties properties,
            CancellationToken cancellationToken)
        {
            ThrowIfDisposed();

            _manualResetEvent.Wait(cancellationToken);

            AddNonAcknowledgedBytes(body.Length);

            try
            {
                return await _decorated.PublishAsync(exchange, routingKey, body, properties, cancellationToken);
            }
            finally
            {
                AddNonAcknowledgedBytes(-body.Length);
            }
        }

        public void Dispose()
        {
            _isDisposed = true;
            _decorated.Dispose();

            _manualResetEvent.Dispose();
            _manualResetEvent.Set();
        }

        private void ThrowIfDisposed()
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException(nameof(AsyncPublisherWithBuffer<TResult>));
            }
        }

        private void AddNonAcknowledgedBytes(int bytes)
        {
            lock (_syncRoot)
            {
                _nonAcknowledgedBytes += bytes;

                if (_nonAcknowledgedBytes > _nonAcknowledgedBytesSoftLimit)
                {
                    if (_manualResetEvent.IsSet)
                    {
                        _manualResetEvent.Reset();
                    }

                    return;
                }

                if (!_manualResetEvent.IsSet)
                {
                    _manualResetEvent.Set();
                }
            }
        }
    }
}