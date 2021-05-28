﻿using System.Threading;
using System.Threading.Tasks;

namespace RabbitMqAsyncPublisher
{
    public class AsyncManualResetEvent
    {
        private volatile TaskCompletionSource<bool> _taskCompletionSource;

        public AsyncManualResetEvent(bool initialState)
        {
            _taskCompletionSource = new TaskCompletionSource<bool>();

            if (initialState)
            {
                _taskCompletionSource.SetResult(true);
            }
        }

        public Task<bool> WaitAsync(CancellationToken cancellationToken = default)
        {
            return WaitAsync(-1, cancellationToken);
        }

        public async Task<bool> WaitAsync(int millisecondsTimeout, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var capturedSource = _taskCompletionSource;
            var result = await Task.WhenAny(
                capturedSource.Task,
                Task.Delay(millisecondsTimeout, cancellationToken)
            ).ConfigureAwait(false);

            await result.ConfigureAwait(false);
            return result == capturedSource.Task;
        }

        public void Set()
        {
            var capturedSource = _taskCompletionSource;
            if (capturedSource.Task.IsCompleted)
            {
                return;
            }

            Task.Run(() => capturedSource.TrySetResult(true));
        }

        public void Reset()
        {
            while (true)
            {
                var capturedSource = _taskCompletionSource;

                if (!capturedSource.Task.IsCompleted
                    || Interlocked.CompareExchange(
                        ref _taskCompletionSource,
                        new TaskCompletionSource<bool>(),
                        capturedSource) == capturedSource)
                {
                    return;
                }
            }
        }
    }
}