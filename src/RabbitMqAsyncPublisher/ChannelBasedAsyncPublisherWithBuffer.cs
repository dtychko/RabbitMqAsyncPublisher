using System;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace RabbitMqAsyncPublisher
{
    public class ChannelBasedAsyncPublisherWithBuffer<TResult> : IAsyncPublisher<TResult>
    {
        private struct Job
        {
            public string Exchange;
            public string RoutingKey;
            public ReadOnlyMemory<byte> Body;
            public IBasicProperties Properties;
            public CancellationToken CancellationToken;
            public TaskCompletionSource<TResult> TaskCompletionSource;
        }

        private readonly IAsyncPublisher<TResult> _decorated;

        private readonly int _processingMessagesLimit;
        private readonly int _processingBytesSoftLimit;
        private int _processingMessages;
        private int _processingBytes;
        private readonly object _currentStateSyncRoot = new object();

        private readonly Channel<Job> _channel =
            Channel.CreateUnbounded<Job>(new UnboundedChannelOptions
                {SingleReader = true});

        private readonly AsyncManualResetEvent _gateEvent = new AsyncManualResetEvent(true);
        private readonly DisposeAwareCancellation _disposeCancellation = new DisposeAwareCancellation();

        public ChannelBasedAsyncPublisherWithBuffer(
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

            RunReaderLoop(_disposeCancellation.Token);
        }

        private async void RunReaderLoop(CancellationToken cancellationToken)
        {
            // TODO: async void error handling?
            while (!_channel.Reader.Completion.IsCompleted && !cancellationToken.IsCancellationRequested)
            {
                var job = await _channel.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                await _gateEvent.WaitAsync(cancellationToken).ConfigureAwait(false);
                HandleNextJob(job);
            }
        }

        private void HandleNextJob(Job job)
        {
            Task<TResult> innerTask;
            try
            {
                UpdateState(1, job.Body.Length);
                innerTask = _decorated.PublishAsync(job.Exchange, job.RoutingKey, job.Body, job.Properties,
                    job.CancellationToken);
            }
            catch (Exception ex)
            {
                // TODO: ? diagnostics
                Task.Run(() => job.TaskCompletionSource.TrySetException(ex));
                UpdateState(-1, -job.Body.Length);
                return;
            }

            Complete();

            async void Complete()
            {
                TResult result;
                try
                {
                    result = await innerTask;
                }
                catch (Exception ex)
                {
                    Task.Run(() => job.TaskCompletionSource.TrySetException(ex));
                    return;
                }
                finally
                {
                    UpdateState(-1, -job.Body.Length);
                }

                Task.Run(() => job.TaskCompletionSource.TrySetResult(result));
            }
        }

        public async Task<TResult> PublishAsync(
            string exchange,
            string routingKey,
            ReadOnlyMemory<byte> body,
            IBasicProperties properties,
            CancellationToken inputCancellationToken)
        {
            var (cancellationToken, cancellationDisposable) = _disposeCancellation.ResolveToken(
                nameof(ChannelBasedAsyncPublisherWithBuffer<TResult>),
                inputCancellationToken);
            
            var completionSource = new TaskCompletionSource<TResult>();
            
            // TODO: registration is executed synchronously when token is disposed, which executes task continuation synchronously as well
            var registration = cancellationToken.Register(() =>
            {
                if (_disposeCancellation.IsCancellationRequested)
                {
                    completionSource.TrySetException(
                        new ObjectDisposedException(nameof(ChannelBasedAsyncPublisherWithBuffer<TResult>)));
                }
                else
                {
                    completionSource.TrySetCanceled();
                }
            });

            var job = new Job
            {
                Exchange = exchange,
                RoutingKey = routingKey,
                Body = body,
                Properties = properties,
                CancellationToken = cancellationToken,
                TaskCompletionSource = completionSource
            };

            var resultTask = job.TaskCompletionSource.Task;
            
#pragma warning disable 4014
            resultTask.ContinueWith(_ =>
#pragma warning restore 4014
            {
                // TODO: custom error handling?
                cancellationDisposable.Dispose();
                registration.Dispose();
            });

            await _channel.Writer.WriteAsync(job, cancellationToken).ConfigureAwait(false);
            return await resultTask.ConfigureAwait(false);
        }

        private void UpdateState(int deltaMessages, int deltaBytes)
        {
            lock (_currentStateSyncRoot)
            {
                _processingMessages += deltaMessages;
                _processingBytes += deltaBytes;

                if (_processingMessages < _processingMessagesLimit
                    && _processingBytes < _processingBytesSoftLimit
                    && !_disposeCancellation.IsCancellationRequested)
                {
                    _gateEvent.Set();
                }
                else
                {
                    _gateEvent.Reset();
                }
            }
        }

        public void Dispose()
        {
            lock (_currentStateSyncRoot)
            {
                if (_disposeCancellation.IsCancellationRequested)
                {
                    return;
                }

                _disposeCancellation.Cancel();
                _disposeCancellation.Dispose();
            }

            _channel.Writer.Complete();
            _decorated.Dispose();
        }
    }
}