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

        public async Task WaitAsync(CancellationToken cancellationToken = default)
        {
            if (!cancellationToken.CanBeCanceled)
            {
                await _taskCompletionSource.Task;
                return;
            }

            cancellationToken.ThrowIfCancellationRequested();

            var result = await Task.WhenAny(
                _taskCompletionSource.Task,
                Task.Delay(-1, cancellationToken)
            ).ConfigureAwait(false);

            await result.ConfigureAwait(false);
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