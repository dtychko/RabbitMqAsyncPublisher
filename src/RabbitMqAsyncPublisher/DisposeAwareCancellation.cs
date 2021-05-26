using System;
using System.Threading;

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

        public (CancellationToken, IDisposable Cleanup) ResolveToken(string componentName, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (_disposeToken.IsCancellationRequested)
            {
                throw new ObjectDisposedException(componentName);
            }

            if (cancellationToken.CanBeCanceled)
            {
                var disposable = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _disposeToken);
                return (disposable.Token, disposable);
            }

            return (_disposeToken, Disposable.Empty);
        }

        public void Dispose()
        {
            Console.WriteLine($" >> {nameof(DisposeAwareCancellation)}.{nameof(Dispose)}");
            _disposeTokenSource.Dispose();
        }
    }
}