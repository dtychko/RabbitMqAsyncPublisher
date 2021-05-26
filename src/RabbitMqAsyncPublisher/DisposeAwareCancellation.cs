using System;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMqAsyncPublisher
{
    public class DisposeAwareCancellation : IDisposable
    {
        private readonly CancellationTokenSource _disposeTokenSource;
        private readonly CancellationToken _disposeToken;

        public DisposeAwareCancellation()
        {
            _disposeTokenSource = new CancellationTokenSource();
            _disposeToken = _disposeTokenSource.Token;
        }

        public CancellationToken Token => _disposeToken;
        
        public bool IsCancellationRequested => _disposeTokenSource.IsCancellationRequested;

        public void Cancel() => _disposeTokenSource.Cancel();

        public async Task<TResult> HandleAsync<TResult>(
            string componentName,
            CancellationToken cancellationToken, Func<CancellationToken, Task<TResult>> action)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                _disposeToken.ThrowIfCancellationRequested();

                if (cancellationToken.CanBeCanceled)
                {
                    using (var compositeCancellationTokenSource =
                        CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _disposeToken))
                    {
                        return await action(compositeCancellationTokenSource.Token).ConfigureAwait(false);
                    }
                }

                return await action(_disposeToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException ex)
            {
                if (ex.CancellationToken == _disposeToken)
                {
                    throw new ObjectDisposedException(componentName);
                }

                throw;
            }
        }

        public void Dispose()
        {
            _disposeTokenSource.Dispose();
        }
    }
}