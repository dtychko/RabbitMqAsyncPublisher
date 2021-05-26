using System.Threading;
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
            var capturedSource = _taskCompletionSource;
            var result = await Task.WhenAny(
                Task.Delay(millisecondsTimeout, cancellationToken),
                capturedSource.Task
            ).ConfigureAwait(false);

            await result;
            return result == capturedSource.Task;
        }

        public void Set()
        {
            _taskCompletionSource.TrySetResult(true);
        }

        public Task SetAsync()
        {
            var capturedSource = _taskCompletionSource;
            if (capturedSource.Task.IsCompleted)
            {
                return Task.CompletedTask;
            }

            return Task.Run(() => capturedSource.TrySetResult(true));
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