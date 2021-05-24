using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace RabbitMqAsyncPublisher
{
    using static DiagnosticsUtils;

    // TODO: integrate time-based connection check and re-init
    // TODO:
    // TODO: could be implemented as external component,
    // TODO: that periodically creates a model on the current connection,
    // TODO: if model can't be created then current connection should be explicitly closed,
    // TODO: see AutoRecoveryConnectionHealthCheck implementation
    // TODO:
    // TODO: perhaps we just need to rely on default connection heartbeat mechanism

    public interface IAutoRecoveryResource : IDisposable
    {
        ShutdownEventArgs CloseReason { get; }

        event EventHandler<ShutdownEventArgs> Shutdown;
    }

    public class AutoRecoveryResourceConnection : IAutoRecoveryResource
    {
        public AutoRecoveryResourceConnection(IConnection connection)
        {
            if (connection is null)
            {
                throw new ArgumentNullException(nameof(connection));
            }

            if (connection is IAutorecoveringConnection)
            {
                throw new ArgumentException("Auto recovering connection is not supported.", nameof(connection));
            }

            Value = connection;
        }

        public IConnection Value { get; }

        public ShutdownEventArgs CloseReason => Value.CloseReason;

        public event EventHandler<ShutdownEventArgs> Shutdown
        {
            add => Value.ConnectionShutdown += value;
            remove => Value.ConnectionShutdown -= value;
        }

        public void Dispose()
        {
            if (Value.CloseReason is null)
            {
                // "IConnection.Dispose()" method implementation calls "IConnection.Close()" with infinite timeout
                // that is used for waiting an internal "ManualResetEventSlim" instance,
                // as result dispose method call could hang for unpredictable time.
                // To prevent this hanging happen
                // we should call "IConnection.Close()" method with some finite timeout first,
                // only after that "IConnection.Dispose() could be safely called.
                // Make sure that current connection is closed.
                // Pass "TimeSpan.Zero" as a non-infinite timeout.
                try
                {
                    Value.Close(TimeSpan.Zero);
                }
                catch (Exception)
                {
                    // TODO: handle via unexpected diagnostics
                }
            }

            Value.Dispose();
        }
    }

    public class AutoRecoveryResourceModel : IAutoRecoveryResource
    {
        public AutoRecoveryResourceModel(IModel model)
        {
            if (model is null)
            {
                throw new ArgumentNullException(nameof(model));
            }

            // TODO: would be great to ensure that model itself is not recoverable
            // to avoid conflicts with RabbitMQ lib recovery behavior
            // Unfortunately, can't do that with simple `model is IRecoverable` check because all models implement this interface

            Value = model;
        }

        public IModel Value { get; }

        public ShutdownEventArgs CloseReason => Value.CloseReason;

        public event EventHandler<ShutdownEventArgs> Shutdown
        {
            add => Value.ModelShutdown += value;
            remove => Value.ModelShutdown -= value;
        }

        public void Dispose()
        {
            Value.Dispose();
        }
    }

    public class AutoRecovery<T> : IDisposable where T : class, IAutoRecoveryResource
    {
        private readonly Func<T> _createResource;
        private readonly IReadOnlyList<Func<T, IDisposable>> _componentFactories;
        private readonly Func<int, CancellationToken, Task> _reconnectDelay;
        private readonly IAutoRecoveryDiagnostics _diagnostics;
        private readonly SemaphoreSlim _connectionSemaphore = new SemaphoreSlim(1, 1);
        private string _currentSessionId;
        private T _currentResource;
        private IReadOnlyList<IDisposable> _currentComponents;
        private readonly CancellationTokenSource _cancellationTokenSource;
        private readonly CancellationToken _cancellationToken;
        private readonly string _resourceId;

        public AutoRecovery(
            Func<T> createResource,
            IReadOnlyList<Func<T, IDisposable>> componentFactories,
            Func<int, TimeSpan> reconnectDelay,
            IAutoRecoveryDiagnostics diagnostics = null)
            : this(createResource, componentFactories,
                (retryNumber, cancellationToken) => Task.Delay(reconnectDelay(retryNumber), cancellationToken),
                diagnostics ?? AutoRecoveryDiagnostics.NoDiagnostics)
        {
        }

        internal AutoRecovery(
            Func<T> createResource,
            IReadOnlyList<Func<T, IDisposable>> componentFactories,
            Func<int, CancellationToken, Task> reconnectDelay,
            IAutoRecoveryDiagnostics diagnostics)
        {
            _createResource = createResource;
            _componentFactories = componentFactories;
            _reconnectDelay = reconnectDelay;
            _diagnostics = diagnostics;

            _cancellationTokenSource = new CancellationTokenSource();
            _cancellationToken = _cancellationTokenSource.Token;
            _resourceId = typeof(T).Name + "/" + Guid.NewGuid().ToString("D");
        }

        public void Start()
        {
            // TODO: Make sure that "Start()" could be called only once

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
                // ignore
            }
            catch (Exception ex)
            {
                TrackSafe(_diagnostics.TrackUnexpectedException, _resourceId, "Unable to start connect loop.", ex);
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
                    // "T.ConnectionShutdown" event will be fired immediately if connection is already closed,
                    // as results we don't need to perform an additional check
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

        private T CreateResourceInstance()
        {
            var stopwatch = Stopwatch.StartNew();

            try
            {
                var resource = _createResource();
                if (resource is null)
                {
                    throw new Exception("Acquired resource but it is null.");
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
            T resource,
            IReadOnlyList<Func<T, IDisposable>> componentFactories)
        {
            var components = new List<IDisposable>();
            var stopwatch = Stopwatch.StartNew();

            for (var i = 0; i < componentFactories.Count; i++)
            {
                try
                {
                    // Make sure that connection is still open,
                    // otherwise throws exception instead of trying to create a component.

                    // Use "Interlocked.Exchange" to make sure that "connection.CloseReason"
                    // wouldn't be inlined in places where "closeReason" local variable is accessed,
                    // because "connection.CloseReason" could be changed concurrently
                    // and could have different values in different points in time.
                    ShutdownEventArgs closeReason = null;
                    Interlocked.Exchange(ref closeReason, resource.CloseReason);
                    if (!(closeReason is null))
                    {
                        throw new AlreadyClosedException(closeReason);
                    }

                    components.Add(componentFactories[i](resource));
                }
                catch (Exception ex)
                {
                    TrackSafe(_diagnostics.TrackUnexpectedException, _resourceId, $"Unable to create component#{i}.",
                        ex);

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

        private void SubscribeOnResourceShutdown(T resource)
        {
            try
            {
                resource.Shutdown += OnResourceShutdown;
            }
            catch (Exception ex)
            {
                // Subscribing on "T.ConnectionShutdown" throws at least when connection is already disposed.

                TrackSafe(_diagnostics.TrackUnexpectedException,
                    _resourceId, "Unable to subscribe on Shutdown event.", ex);
                throw;
            }
        }

        private void OnResourceShutdown(object sender, ShutdownEventArgs args)
        {
            TrackSafe(_diagnostics.TrackResourceClosed, _resourceId, args);

            Task.Run(CreateResource, _cancellationToken);
        }

        private void CleanUpThreadUnsafe()
        {
            if (_currentResource is null && _currentComponents is null)
            {
                return;
            }

            TrackSafe(_diagnostics.TrackCleanUpStarted, _resourceId, _currentSessionId);
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
                        TrackSafe(_diagnostics.TrackUnexpectedException, _resourceId,
                            $"Unable to dispose component#{i}.", ex);
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
                    // Unsubscribing from "T.ConnectionShutdown" throws at least when connection is already disposed.

                    TrackSafe(_diagnostics.TrackUnexpectedException,
                        _resourceId, "Unable to unsubscribe from T.Shutdown event.", ex);
                }

                try
                {
                    _currentResource.Dispose();
                }
                catch (Exception ex)
                {
                    TrackSafe(_diagnostics.TrackUnexpectedException, _resourceId,
                        "Unable to dispose current connection.", ex);
                }

                _currentResource = null;
            }

            TrackSafe(_diagnostics.TrackCleanUpCompleted, _resourceId, _currentSessionId, stopwatch.Elapsed);
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

            TrackSafe(_diagnostics.TrackDisposeStarted, _resourceId);
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

            TrackSafe(_diagnostics.TrackDisposeCompleted, _resourceId, stopwatch.Elapsed);
        }
    }
}