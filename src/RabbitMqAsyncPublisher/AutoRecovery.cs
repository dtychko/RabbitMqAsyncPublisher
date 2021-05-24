using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace RabbitMqAsyncPublisher
{
    using static DiagnosticsUtils;

    public static class AutoRecovery
    {
        public static AutoRecovery<AutoRecoveryResourceConnection> Connection(
            IConnectionFactory connectionFactory,
            Func<int, TimeSpan> retryDelay,
            IAutoRecoveryDiagnostics diagnostics,
            params Func<IConnection, IDisposable>[] componentFactories)
        {
            return new AutoRecovery<AutoRecoveryResourceConnection>(
                () => new AutoRecoveryResourceConnection(connectionFactory.CreateConnection(), diagnostics),
                componentFactories
                    .Select(factory =>
                        (Func<AutoRecoveryResourceConnection, IDisposable>) (connection => factory(connection.Value)))
                    .ToArray(),
                retryDelay,
                diagnostics
            );
        }

        public static AutoRecovery<AutoRecoveryResourceModel> Model(
            IConnection connection,
            Func<int, TimeSpan> retryDelay,
            IAutoRecoveryDiagnostics diagnostics,
            params Func<IModel, IDisposable>[] componentFactories)
        {
            return new AutoRecovery<AutoRecoveryResourceModel>(
                () => new AutoRecoveryResourceModel(connection.CreateModel(), diagnostics),
                componentFactories
                    .Select(factory =>
                        (Func<AutoRecoveryResourceModel, IDisposable>) (model => factory(model.Value)))
                    .ToArray(),
                retryDelay,
                diagnostics
            );
        }
    }

    public class AutoRecovery<TResource> : IDisposable where TResource : class, IAutoRecoveryResource
    {
        private readonly Func<TResource> _createResource;
        private readonly IReadOnlyList<Func<TResource, IDisposable>> _componentFactories;
        private readonly Func<int, CancellationToken, Task> _reconnectDelay;
        private readonly IAutoRecoveryDiagnostics _diagnostics;
        private readonly SemaphoreSlim _connectionSemaphore = new SemaphoreSlim(1, 1);
        private string _currentSessionId;
        private TResource _currentResource;
        private IReadOnlyList<IDisposable> _currentComponents;
        private readonly CancellationTokenSource _cancellationTokenSource;
        private readonly CancellationToken _cancellationToken;
        private bool _isStarted;

        public AutoRecovery(
            Func<TResource> createResource,
            IReadOnlyList<Func<TResource, IDisposable>> componentFactories,
            Func<int, TimeSpan> reconnectDelay,
            IAutoRecoveryDiagnostics diagnostics = null)
            : this(createResource, componentFactories,
                (retryNumber, cancellationToken) => Task.Delay(reconnectDelay(retryNumber), cancellationToken),
                diagnostics ?? AutoRecoveryDiagnostics.NoDiagnostics)
        {
        }

        internal AutoRecovery(
            Func<TResource> createResource,
            IReadOnlyList<Func<TResource, IDisposable>> componentFactories,
            Func<int, CancellationToken, Task> reconnectDelay,
            IAutoRecoveryDiagnostics diagnostics)
        {
            _createResource = createResource;
            _componentFactories = componentFactories;
            _reconnectDelay = reconnectDelay;
            _diagnostics = diagnostics;

            _cancellationTokenSource = new CancellationTokenSource();
            _cancellationToken = _cancellationTokenSource.Token;
        }

        public void Start()
        {
            lock (_cancellationTokenSource)
            {
                if (_cancellationToken.IsCancellationRequested)
                {
                    throw new ObjectDisposedException(nameof(AutoRecovery<TResource>));
                }

                if (_isStarted)
                {
                    throw new InvalidOperationException("Already started.");
                }

                _isStarted = true;
            }

            CreateResource();
        }

        private async void CreateResource()
        {
            try
            {
                await CreateResourceAsync().ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Ignore
            }
            catch (Exception ex)
            {
                TrackSafe(_diagnostics.TrackUnexpectedException, "Unable to start create resource loop.", ex);
            }
        }

        private async Task CreateResourceAsync()
        {
            await _connectionSemaphore.WaitAsync(_cancellationToken).ConfigureAwait(false);

            try
            {
                await ConnectThreadUnsafeAsync().ConfigureAwait(false);
            }
            finally
            {
                _connectionSemaphore.Release();
            }
        }

        private async Task ConnectThreadUnsafeAsync()
        {
            // Make sure that previous connection is disposed together with its components.
            CleanUpThreadUnsafe();

            _currentSessionId = Guid.NewGuid().ToString("D");

            for (var attempt = 1; !_cancellationToken.IsCancellationRequested; attempt += 1)
            {
                TrackSafe(_diagnostics.TrackCreateResourceAttemptStarted, _currentSessionId, attempt);
                var stopwatch = Stopwatch.StartNew();

                try
                {
                    _currentResource = CreateResourceInstance();
                    _currentComponents = CreateComponents(_currentResource, _componentFactories);
                    SubscribeOnResourceShutdown(_currentResource);

                    // According to "RabbitMQ.Client" reference,
                    // "IConnection.ConnectionShutdown" and "IModel.ModelShutdown" events will be fired immediately
                    // if connection is already closed, as results we don't need to perform an additional check
                    // that connection is still open after subscribing on the event.

                    TrackSafe(_diagnostics.TrackCreateResourceAttemptSucceeded, _currentSessionId, attempt,
                        stopwatch.Elapsed);
                    return;
                }
                catch (Exception ex)
                {
                    TrackSafe(_diagnostics.TrackCreateResourceAttemptFailed, _currentSessionId, attempt,
                        stopwatch.Elapsed,
                        ex);
                    CleanUpThreadUnsafe();

                    await _reconnectDelay(attempt, _cancellationToken).ConfigureAwait(false);
                }
            }
        }

        private TResource CreateResourceInstance()
        {
            var stopwatch = Stopwatch.StartNew();

            try
            {
                var resource = _createResource();
                if (resource is null)
                {
                    throw new Exception("Created resource is null.");
                }

                TrackSafe(_diagnostics.TrackCreateResourceSucceeded, _currentSessionId, stopwatch.Elapsed);
                return resource;
            }
            catch (Exception ex)
            {
                TrackSafe(_diagnostics.TrackCreateResourceFailed, _currentSessionId, stopwatch.Elapsed, ex);
                throw;
            }
        }

        private IReadOnlyList<IDisposable> CreateComponents(
            TResource resource,
            IReadOnlyList<Func<TResource, IDisposable>> componentFactories)
        {
            var components = new List<IDisposable>();
            var stopwatch = Stopwatch.StartNew();

            for (var i = 0; i < componentFactories.Count; i++)
            {
                try
                {
                    // Make sure that connection is still open,
                    // otherwise throws exception instead of trying to create a component.
                    if (IsResourceClosed(resource, out var shutdownEventArgs))
                    {
                        throw new AlreadyClosedException(shutdownEventArgs);
                    }

                    components.Add(componentFactories[i](resource));
                }
                catch (Exception ex)
                {
                    TrackSafe(_diagnostics.TrackUnexpectedException, $"Unable to create component#{i}.", ex);

                    // Stop creating components if connection/model/etc. is already closed.
                    if (resource.CloseReason != null)
                    {
                        break;
                    }

                    // Otherwise continue, because we don't want to fail all if one component fails with unexpected exception.
                }
            }

            TrackSafe(_diagnostics.TrackCreateResourceCompleted, _currentSessionId, components.Count,
                componentFactories.Count, stopwatch.Elapsed);
            return components;
        }

        private static bool IsResourceClosed(TResource resource, out ShutdownEventArgs shutdownEventArgs)
        {
            shutdownEventArgs = default;

            // Use "Interlocked.Exchange" to make sure that "connection.CloseReason"
            // wouldn't be inlined in places where "closeReason" local variable is accessed,
            // because "connection.CloseReason" could be changed concurrently
            // and could have different values in different points in time.
            Interlocked.Exchange(ref shutdownEventArgs, resource.CloseReason);
            return !(shutdownEventArgs is null);
        }

        private void SubscribeOnResourceShutdown(TResource resource)
        {
            try
            {
                resource.Shutdown += OnResourceShutdown;
            }
            catch (Exception ex)
            {
                TrackSafe(_diagnostics.TrackUnexpectedException,
                    "Unable to subscribe on IAutoRecoveryResource.Shutdown event.", ex);
                throw;
            }
        }

        private void OnResourceShutdown(object sender, ShutdownEventArgs args)
        {
            TrackSafe(_diagnostics.TrackResourceClosed, _currentSessionId, args);
            Task.Run(CreateResource, _cancellationToken);
        }

        private void CleanUpThreadUnsafe()
        {
            if (_currentResource is null && _currentComponents is null)
            {
                return;
            }

            TrackSafe(_diagnostics.TrackCleanUpStarted, _currentSessionId);
            var stopwatch = Stopwatch.StartNew();

            if (!(_currentComponents is null))
            {
                for (var i = 0; i < _currentComponents.Count; i++)
                {
                    try
                    {
                        _currentComponents[i].Dispose();
                    }
                    catch (Exception ex)
                    {
                        TrackSafe(_diagnostics.TrackUnexpectedException, $"Unable to dispose component#{i}.", ex);
                    }
                }

                _currentComponents = null;
            }

            if (!(_currentResource is null))
            {
                try
                {
                    _currentResource.Shutdown -= OnResourceShutdown;
                }
                catch (Exception ex)
                {
                    TrackSafe(_diagnostics.TrackUnexpectedException,
                        "Unable to unsubscribe from IAutoRecoveryResource.Shutdown event.", ex);
                }

                try
                {
                    _currentResource.Dispose();
                }
                catch (Exception ex)
                {
                    TrackSafe(_diagnostics.TrackUnexpectedException, "Unable to dispose current resource.", ex);
                }

                _currentResource = null;
            }

            TrackSafe(_diagnostics.TrackCleanUpCompleted, _currentSessionId, stopwatch.Elapsed);
        }

        public void Dispose()
        {
            lock (_cancellationTokenSource)
            {
                if (_cancellationTokenSource.IsCancellationRequested)
                {
                    return;
                }

                _cancellationTokenSource.Cancel();
                _cancellationTokenSource.Dispose();
            }

            TrackSafe(_diagnostics.TrackDisposeStarted);
            var stopwatch = Stopwatch.StartNew();

            // ReSharper disable once MethodSupportsCancellation
            _connectionSemaphore.Wait();

            try
            {
                CleanUpThreadUnsafe();
            }
            finally
            {
                _connectionSemaphore.Release();
                _connectionSemaphore.Dispose();
            }

            TrackSafe(_diagnostics.TrackDisposeCompleted, stopwatch.Elapsed);
        }
    }
}