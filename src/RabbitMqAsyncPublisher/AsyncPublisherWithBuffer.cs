using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace RabbitMqAsyncPublisher
{
    public class AsyncFifoSemaphore : IDisposable
    {
        private readonly int _maxCount;
        private volatile int _currentCount;

        private readonly LinkedList<TaskCompletionSource<bool>> _sources = new LinkedList<TaskCompletionSource<bool>>();
        private readonly object _syncRoot = new object();

        public int CurrentCount => _currentCount;

        public AsyncFifoSemaphore(int initialCount, int maxCount)
        {
            _currentCount = initialCount;
            _maxCount = maxCount;
        }

        public async Task WaitAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            LinkedListNode<TaskCompletionSource<bool>> sourceNode;

            lock (_syncRoot)
            {
                if (_currentCount > 0)
                {
                    // ReSharper disable once NonAtomicCompoundOperator
                    _currentCount -= 1;
                    return;
                }

                var source = new TaskCompletionSource<bool>();
                sourceNode = _sources.AddLast(source);
            }

            var task = await Task.WhenAny(sourceNode.Value.Task, Task.Delay(-1, cancellationToken))
                .ConfigureAwait(false);

            lock (_syncRoot)
            {
                if (task == sourceNode.Value.Task)
                {
                    return;
                }

                if (!(sourceNode.List is null))
                {
                    _sources.Remove(sourceNode);
                    cancellationToken.ThrowIfCancellationRequested();
                }
            }

            await task.ConfigureAwait(false);
        }

        public void Release()
        {
            lock (_syncRoot)
            {
                if (_sources.Count > 0)
                {
                    var source = _sources.First.Value;
                    _sources.RemoveFirst();
                    // source.SetResult(true);
                    Task.Run(() => source.SetResult(true));
                    return;
                }

                if (_currentCount == _maxCount)
                {
                    throw new InvalidOperationException("Semaphore is full.");
                }

                // ReSharper disable once NonAtomicCompoundOperator
                _currentCount += 1;
            }
        }

        public void Dispose()
        {
        }
    }

    public class AsyncPublisherWithBuffer<TResult> : IAsyncPublisher<TResult>
    {
        private readonly IAsyncPublisher<TResult> _decorated;

        private readonly int _processingMessagesLimit;
        private readonly int _processingBytesSoftLimit;
        private int _processingMessages;
        private int _processingBytes;

        private readonly AsyncFifoSemaphore _semaphore = new AsyncFifoSemaphore(1, 1);
        private readonly CancellationTokenSource _disposeCancellationTokenSource = new CancellationTokenSource();
        private readonly CancellationToken _disposeCancellationToken;

        private readonly object _syncRoot = new object();

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

            _disposeCancellationToken = _disposeCancellationTokenSource.Token;
        }

        public async Task<TResult> PublishAsync(
            string exchange,
            string routingKey,
            ReadOnlyMemory<byte> body,
            IBasicProperties properties,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                _disposeCancellationToken.ThrowIfCancellationRequested();

                Console.WriteLine($" >> {exchange} << [{Thread.CurrentThread.ManagedThreadId}]");

                if (cancellationToken.CanBeCanceled)
                {
                    using (var compositeCancellationTokenSource =
                        CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _disposeCancellationToken))
                    {
                        // TODO: replace with another sync primitive that supports FIFO semantics
                        await _semaphore.WaitAsync(compositeCancellationTokenSource.Token).ConfigureAwait(false);
                    }
                }
                else
                {
                    await _semaphore.WaitAsync(_disposeCancellationToken).ConfigureAwait(false);
                }

                Console.WriteLine($" -- {exchange} --");
            }
            catch (OperationCanceledException ex)
            {
                if (ex.CancellationToken == _disposeCancellationToken)
                {
                    throw new ObjectDisposedException(nameof(AsyncPublisherWithBuffer<TResult>));
                }

                throw;
            }

            try
            {
                UpdateState(1, body.Length);
                return await _decorated.PublishAsync(exchange, routingKey, body, properties, cancellationToken)
                    .ConfigureAwait(false);
            }
            finally
            {
                Console.WriteLine($"Processed {exchange}");
                UpdateState(-1, -body.Length);
            }
        }

        private void UpdateState(int deltaMessages, int deltaBytes)
        {
            lock (_syncRoot)
            {
                _processingMessages += deltaMessages;
                _processingBytes += deltaBytes;

                if (_semaphore.CurrentCount == 0
                    && _processingMessages < _processingMessagesLimit
                    && _processingBytes < _processingBytesSoftLimit
                    && !_disposeCancellationToken.IsCancellationRequested)
                {
                    Console.WriteLine("Release");
                    _semaphore.Release();
                }
            }
        }

        public void Dispose()
        {
            lock (_syncRoot)
            {
                if (_disposeCancellationTokenSource.IsCancellationRequested)
                {
                    return;
                }

                _disposeCancellationTokenSource.Cancel();
                _disposeCancellationTokenSource.Dispose();
            }

            _semaphore.Dispose();
            _decorated.Dispose();
        }
    }
}