using System;
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
        private readonly object _syncRoot = new object();
        private int _disposed;

        // Could be accessed concurrently, so should be marked as "volatile"
        private volatile bool _isDeclared;

        public IModel Model => _decorated.Model;

        public AsyncPublisherDeclaringDecorator(IAsyncPublisher<TResult> decorated, Action<IModel> declare)
        {
            _decorated = decorated;
            _declare = declare;

            Model.ModelShutdown += OnModelShutdown;
        }

        private void OnModelShutdown(object sender, ShutdownEventArgs e)
        {
            // Current connection is closed.
            // Reset "_isDeclared" flag to make sure that everything is redeclared when connection is restored. 
            _isDeclared = false;
        }

        public Task<TResult> PublishAsync(
            string exchange,
            string routingKey,
            ReadOnlyMemory<byte> body,
            IBasicProperties properties,
            CancellationToken cancellationToken)
        {
            lock (_syncRoot)
            {
                if (!_isDeclared)
                {
                    _declare(Model);
                    _isDeclared = true;
                }

                return _decorated.PublishAsync(exchange, routingKey, body, properties, cancellationToken);
            }
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                Model.ModelShutdown -= OnModelShutdown;
            }

            _decorated.Dispose();
        }
    }
}