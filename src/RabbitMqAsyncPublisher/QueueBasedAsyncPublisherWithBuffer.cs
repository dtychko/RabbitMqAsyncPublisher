using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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

        // private readonly ConcurrentQueue<Job> _jobQueue = new ConcurrentQueue<Job>();
        private readonly LinkedList<Job> _jobQueue = new LinkedList<Job>();
        private readonly AsyncManualResetEvent _queueReadyEvent = new AsyncManualResetEvent(false);
        private readonly AsyncManualResetEvent _gateEvent = new AsyncManualResetEvent(true);
        private readonly DisposeAwareCancellation _disposeCancellation = new DisposeAwareCancellation();
        private readonly Task _readLoopTask;

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

            _readLoopTask = Task.Run(RunReaderLoop);
        }

        private bool TryDequeueJob(out Job job)
        {
            lock (_jobQueue)
            {
                if (_jobQueue.Count > 0)
                {
                    job = _jobQueue.First.Value;
                    _jobQueue.RemoveFirst();
                    return true;
                }

                job = default;
                return false;
            }
        }

        private async void RunReaderLoop()
        {
            try
            {
                await _queueReadyEvent.WaitAsync(_disposeCancellation.Token).ConfigureAwait(false);

                while (!_disposeCancellation.IsCancellationRequested)
                {
                    _queueReadyEvent.Reset();

                    while (TryDequeueJob(out var job))
                    {
                        var taskCompletionSource = job.TaskCompletionSource;
                        var cancellationToken = job.CancellationToken;

                        try
                        {
                            Console.WriteLine("waiting gate ...");
                            await WaitForGateAsync(cancellationToken).ConfigureAwait(false);
                            Console.WriteLine("waiting gate completed");
                            var handleJobTask = HandleJobAsync(job);

                            Task.Run(async () =>
                            {
                                try
                                {
                                    var result = await handleJobTask.ConfigureAwait(false);
                                    taskCompletionSource.TrySetResult(result);
                                }
                                catch (OperationCanceledException ex)
                                {
                                    taskCompletionSource.TrySetCanceled(ex.CancellationToken);
                                }
                                catch (Exception ex)
                                {
                                    taskCompletionSource.TrySetException(ex);
                                }
                            });
                        }
                        catch (OperationCanceledException)
                        {
                            Console.WriteLine("operation cancelled");

                            if (_disposeCancellation.IsCancellationRequested)
                            {
                                Task.Run(() =>
                                    taskCompletionSource.TrySetException(
                                        new ObjectDisposedException(
                                            nameof(QueueBasedAsyncPublisherWithBuffer<TResult>))));

                                // Rethrow exception in order to gracefully stop reader loop
                                throw;
                            }

                            Task.Run(() => taskCompletionSource.TrySetCanceled(cancellationToken));
                        }
                        catch (Exception ex)
                        {
                            Task.Run(() => taskCompletionSource.TrySetException(ex));
                        }
                    }

                    await _queueReadyEvent.WaitAsync(_disposeCancellation.Token).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (_disposeCancellation.IsCancellationRequested)
            {
                // Reader loop gracefully stopped
            }
            catch (Exception ex)
            {
                // TODO: Log unexpected error
                Console.WriteLine(ex);
            }
        }

        private async Task<TResult> HandleJobAsync(Job job)
        {
            _disposeCancellation.Token.ThrowIfCancellationRequested();
            job.CancellationToken.ThrowIfCancellationRequested();

            // Console.WriteLine($" >> Starting next job {job.Body.Length}");

            try
            {
                UpdateState(1, job.Body.Length);
                return await _decorated.PublishAsync(job.Exchange, job.RoutingKey, job.Body, job.Properties,
                    job.CancellationToken).ConfigureAwait(false);
            }
            finally
            {
                UpdateState(-1, -job.Body.Length);
            }
        }

        private Task WaitForGateAsync(CancellationToken cancellationToken)
        {
            if (_disposeCancellation.IsCancellationRequested)
            {
                return Task.FromCanceled(_disposeCancellation.Token);
            }

            if (!cancellationToken.CanBeCanceled)
            {
                return _gateEvent.WaitAsync(_disposeCancellation.Token);
            }

            if (cancellationToken.IsCancellationRequested)
            {
                return Task.FromCanceled(cancellationToken);
            }

            using (var combinedSource =
                CancellationTokenSource.CreateLinkedTokenSource(_disposeCancellation.Token, cancellationToken))
            {
                return _gateEvent.WaitAsync(combinedSource.Token);
            }
        }

        private void UpdateState(int deltaMessages, int deltaBytes)
        {
            lock (_currentStateSyncRoot)
            {
                _processingMessages += deltaMessages;
                _processingBytes += deltaBytes;

                if (_processingMessages < _processingMessagesLimit
                    && _processingBytes < _processingBytesSoftLimit)
                {
                    // Console.WriteLine(" >> Opening gate");
                    _gateEvent.Set();
                }
                else
                {
                    // Console.WriteLine(" >> Closing gate");
                    _gateEvent.Reset();
                }
            }
        }

        public Task<TResult> PublishAsync(
            string exchange, string routingKey, ReadOnlyMemory<byte> body, IBasicProperties properties,
            CancellationToken cancellationToken)
        {
            if (_disposeCancellation.IsCancellationRequested)
            {
                return Task.FromException<TResult>(
                    new ObjectDisposedException(nameof(QueueBasedAsyncPublisherWithBuffer<TResult>)));
            }

            if (cancellationToken.IsCancellationRequested)
            {
                return Task.FromCanceled<TResult>(cancellationToken);
            }

            var job = new Job
            {
                Exchange = exchange,
                RoutingKey = routingKey,
                Body = body,
                Properties = properties,
                CancellationToken = cancellationToken,
                TaskCompletionSource = new TaskCompletionSource<TResult>()
            };

            var jobNode = EnqueueJob(job);
            _queueReadyEvent.Set();

            return Foo(jobNode, cancellationToken);
        }

        private async Task<TResult> Foo(LinkedListNode<Job> jobNode, CancellationToken cancellationToken)
        {
            var jobTask = jobNode.Value.TaskCompletionSource.Task;
            var completedTask = await Task.WhenAny(
                jobTask,
                Task.Delay(-1, cancellationToken)
            ).ConfigureAwait(false);

            if (completedTask != jobTask && TryRemoveJob(jobNode))
            {
                return await Task.FromCanceled<TResult>(cancellationToken).ConfigureAwait(false);
            }

            return await jobTask.ConfigureAwait(false);
        }

        private bool TryRemoveJob(LinkedListNode<Job> jobNode)
        {
            lock (_jobQueue)
            {
                if (jobNode.List is null)
                {
                    return false;
                }

                _jobQueue.Remove(jobNode);
                return true;
            }
        }

        private LinkedListNode<Job> EnqueueJob(Job job)
        {
            lock (_jobQueue)
            {
                return _jobQueue.AddLast(job);
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

            _decorated.Dispose();

            _readLoopTask.Wait();

            while (TryDequeueJob(out var job))
            {
                Task.Run(() =>
                    job.TaskCompletionSource.TrySetException(
                        new ObjectDisposedException(nameof(QueueBasedAsyncPublisherWithBuffer<TResult>))));
            }
        }
    }
}