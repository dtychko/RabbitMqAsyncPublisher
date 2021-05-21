using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace RabbitMqAsyncPublisher
{
    public static class AsyncPublisherDeclaringDecorator
    {
        public static AsyncPublisherDeclaringDecorator<TResult> Create<TResult>(
            IAsyncPublisher<TResult> decorated,
            Action<IModel> declare)
        {
            return new AsyncPublisherDeclaringDecorator<TResult>(decorated, declare);
        }

        public static Action<IModel> QueueDeclarator(string queueName)
        {
            return model => model.QueueDeclare(queueName, true, false, false);
        }
    }

    public class AsyncPublisherDeclaringDecorator<TResult> : IAsyncPublisher<TResult>
    {
        private readonly IAsyncPublisher<TResult> _decorated;
        private readonly Action<IModel> _declare;
        private readonly IAsyncPublisherDeclaringDecoratorDiagnostics _diagnostics;
        private bool _isDeclared;
        private int _disposed;

        public IModel Model => _decorated.Model;

        public AsyncPublisherDeclaringDecorator(
            IAsyncPublisher<TResult> decorated,
            Action<IModel> declare)
            : this(decorated, declare, EmptyDiagnostics.Instance)
        {
        }

        public AsyncPublisherDeclaringDecorator(
            IAsyncPublisher<TResult> decorated,
            Action<IModel> declare,
            IAsyncPublisherDeclaringDecoratorDiagnostics diagnostics)
        {
            _decorated = decorated;
            _declare = declare;
            _diagnostics = diagnostics;

            Model.ModelShutdown += OnModelShutdown;
        }

        private void OnModelShutdown(object sender, ShutdownEventArgs e)
        {
            // Current connection is closed.
            // Reset "_isDeclared" flag to make sure that everything is redeclared when connection is restored. 
            _isDeclared = false;
        }

        public Task<TResult> PublishUnsafeAsync(
            string exchange,
            string routingKey,
            ReadOnlyMemory<byte> body,
            IBasicProperties properties,
            CancellationToken cancellationToken)
        {
            ThrowIfDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            EnsureDeclared();

            return _decorated.PublishUnsafeAsync(exchange, routingKey, body, properties, cancellationToken);
        }

        private readonly object _declareSyncRoot = new object();

        private void EnsureDeclared()
        {
            if (_isDeclared)
            {
                return;
            }

            lock (_declareSyncRoot)
            {
                if (_isDeclared)
                {
                    return;
                }

                var stopwatch = Stopwatch.StartNew();

                try
                {
                    _diagnostics.TrackDeclare();
                    _declare(Model);
                    _diagnostics.TrackDeclareCompleted(stopwatch.Elapsed);
                }
                catch (Exception ex)
                {
                    _diagnostics.TrackDeclareFailed(stopwatch.Elapsed, ex);
                }

                _isDeclared = true;
            }
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                _decorated.Dispose();
                Model.ModelShutdown -= OnModelShutdown;
            }
        }

        private void ThrowIfDisposed()
        {
            if (_disposed == 1)
            {
                throw new ObjectDisposedException(nameof(AsyncPublisherWithRetries));
            }
        }
    }
}