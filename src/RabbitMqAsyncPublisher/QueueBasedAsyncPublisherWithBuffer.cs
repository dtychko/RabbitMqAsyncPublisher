using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace RabbitMqAsyncPublisher
{
    public class QueueBasedAsyncPublisherWithBuffer<TResult> : IAsyncPublisher<TResult>
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
        private readonly ConcurrentQueue<Job> _jobQueue = new ConcurrentQueue<Job>();
        private readonly AsyncManualResetEvent _queueReadyEvent = new AsyncManualResetEvent(false);
        private readonly AsyncManualResetEvent _gateEvent = new AsyncManualResetEvent(true);
        private readonly DisposeAwareCancellation _disposeCancellation = new DisposeAwareCancellation();

        public QueueBasedAsyncPublisherWithBuffer(
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

            // TODO: use Task.Run?
            RunReaderLoop(_disposeCancellation.Token);
        }

        private async void RunReaderLoop(CancellationToken cancellationToken)
        {
            // TODO: async void error handling
            
            await _queueReadyEvent.WaitAsync(cancellationToken).ConfigureAwait(false);

            while (!cancellationToken.IsCancellationRequested)
            {
                _queueReadyEvent.Reset();
                while (_jobQueue.TryDequeue(out var job))
                {
                    await _gateEvent.WaitAsync(cancellationToken).ConfigureAwait(false);
                    HandleNextJob(job);
                }

                await _queueReadyEvent.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        private void HandleNextJob(Job job)
        {
            if (_disposeCancellation.IsCancellationRequested)
            {
                Task.Run(() =>
                    job.TaskCompletionSource.SetException(
                        new ObjectDisposedException(nameof(QueueBasedAsyncPublisherWithBuffer<TResult>))));
                return;
            }
            
            if (job.CancellationToken.IsCancellationRequested)
            {
                Task.Run(() => job.TaskCompletionSource.TrySetCanceled());
                return;
            }
            
            Console.WriteLine($" >> Starting next job {job.Body.Length}");
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
                    Console.WriteLine($" >> Job {job.Body.Length} finished");
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
                    Console.WriteLine(" >> Opening gate");
                    _gateEvent.Set();
                }
                else
                {
                    Console.WriteLine(" >> Closing gate");
                    _gateEvent.Reset();
                }
            }
        }

        public async Task<TResult> PublishAsync(
            string exchange, string routingKey, ReadOnlyMemory<byte> body, IBasicProperties properties,
            CancellationToken inputCancellationToken)
        {
            var (cancellationToken, cancellationDisposable) = _disposeCancellation.ResolveToken(
                nameof(ChannelBasedAsyncPublisherWithBuffer<TResult>),
                inputCancellationToken);

            var completionSource = new TaskCompletionSource<TResult>();
            
            var registration = cancellationToken.Register(() =>
            {
                if (_disposeCancellation.IsCancellationRequested)
                {
                    Console.WriteLine($" >> Publisher disposed, setting exception on job {body.Length}");
                            
                    completionSource.TrySetException(
                        new ObjectDisposedException(nameof(ChannelBasedAsyncPublisherWithBuffer<TResult>)));
                }
                else
                {
                    Console.WriteLine($" >> Publish task cancelled, cancelling nested task for job {body.Length}");
                    completionSource.TrySetCanceled(cancellationToken);
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

            _jobQueue.Enqueue(job);
            // TODO: do we need to await here?
            await _queueReadyEvent.SetAsync().ConfigureAwait(false);
            return await resultTask.ConfigureAwait(false);
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
        }
    }
}