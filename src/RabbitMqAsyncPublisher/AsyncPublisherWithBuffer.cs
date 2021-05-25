using System;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace RabbitMqAsyncPublisher
{
    public class AsyncPublisherWithBuffer<TResult> : IAsyncPublisher<TResult>
    {
        private readonly IAsyncPublisher<TResult> _decorated;
        private readonly int _processingMessagesLimit;
        private readonly int _processingBytesSoftLimit;
        private readonly ManualResetEventSlim _manualResetEvent = new ManualResetEventSlim(true);
        private volatile bool _isDisposed;

        private readonly object _syncRoot = new object();
        private int _processingMessages;
        private int _processingBytes;

        public AsyncPublisherWithBuffer(
            IAsyncPublisher<TResult> decorated,
            int processingMessagesLimit = int.MaxValue,
            int processingBytesSoftLimit = int.MaxValue)
        {
            if (processingMessagesLimit <= 0)
            {
                throw new ArgumentException("Positive number is expected.", nameof(processingMessagesLimit));
            }

            if (processingBytesSoftLimit <= 0)
            {
                throw new ArgumentException("Positive number is expected.", nameof(processingBytesSoftLimit));
            }

            _decorated = decorated;
            _processingMessagesLimit = processingMessagesLimit;
            _processingBytesSoftLimit = processingBytesSoftLimit;
        }

        private readonly SemaphoreSlim _semaphore = new SemaphoreSlim(1, 1);

        public async Task<TResult> PublishAsync(
            string exchange,
            string routingKey,
            ReadOnlyMemory<byte> body,
            IBasicProperties properties,
            CancellationToken cancellationToken)
        {
            ThrowIfDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                await Task.WhenAny(
                    Task.Delay(-1, CancellationToken.None),
                    _semaphore.WaitAsync(cancellationToken)).ConfigureAwait(false);
            }
            catch (OperationCanceledException ex)
            {
                if (ex.CancellationToken == cancellationToken)
                {
                    throw;
                }
            }

            ThrowIfDisposed();

            lock (_semaphore)
            {
                _processingMessages += 1;
                _processingBytes += body.Length;

                if (_processingMessages < _processingMessagesLimit && _processingBytes < _processingBytesSoftLimit)
                {
                    _semaphore.Release();
                }
            }

            try
            {
                return await _decorated.PublishAsync(exchange, routingKey, body, properties, cancellationToken);
            }
            finally
            {
                lock (_semaphore)
                {
                    _processingMessages -= 1;
                    _processingBytes -= body.Length;

                    if (_processingMessages < _processingMessagesLimit
                        && _processingBytes < _processingBytesSoftLimit
                        && _semaphore.CurrentCount == 0)
                    {
                        _semaphore.Release();
                    }
                }
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
    }
}