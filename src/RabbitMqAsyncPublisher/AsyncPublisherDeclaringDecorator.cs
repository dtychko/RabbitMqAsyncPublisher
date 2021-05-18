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
        private readonly ILogger _logger;
        private int _disposed;

        // Could be accessed concurrently, so should be marked as "volatile"
        private volatile bool _isDeclared;

        public IModel Model => _decorated.Model;

        public AsyncPublisherDeclaringDecorator(
            IAsyncPublisher<TResult> decorated,
            Action<IModel> declare)
            : this(decorated, declare, EmptyLogger.Instance)
        {
        }

        public AsyncPublisherDeclaringDecorator(
            IAsyncPublisher<TResult> decorated,
            Action<IModel> declare,
            ILogger logger)
        {
            _decorated = decorated;
            _declare = declare;
            _logger = logger;

            Model.ModelShutdown += OnModelShutdown;
        }

        private void OnModelShutdown(object sender, ShutdownEventArgs e)
        {
            // Current connection is closed.
            // Reset "_isDeclared" flag to make sure that everything is redeclared when connection is restored. 
            _isDeclared = false;

            _logger.Info(
                "{0} received event '{1}'. Required RabbitMQ resources will be redeclared before publishing next message.",
                GetType().Name,
                nameof(IModel.ModelShutdown)
            );
        }

        public Task<TResult> PublishUnsafeAsync(
            string exchange,
            string routingKey,
            ReadOnlyMemory<byte> body,
            IBasicProperties properties,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (!_isDeclared)
            {
                DeclareUnsafe();
                _isDeclared = true;
            }

            return _decorated.PublishUnsafeAsync(exchange, routingKey, body, properties, cancellationToken);
        }

        private void DeclareUnsafe()
        {
            try
            {
                _logger.Info(
                    "{0} declaring required RabbitMQ resources ...",
                    GetType().Name
                );

                _declare(Model);

                _logger.Info(
                    "{0} completed declaration of required RabbitMQ resources.",
                    GetType().Name
                );
            }
            catch (Exception ex)
            {
                _logger.Error(
                    "{0} couldn't complete declaration of required RabbitMQ resources because of unexpected error.",
                    ex,
                    GetType().Name
                );

                throw;
            }
        }

        public void Dispose()
        {
            _decorated.Dispose();

            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                Model.ModelShutdown -= OnModelShutdown;
            }
        }
    }
}