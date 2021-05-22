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

    public class AutoRecovery : IDisposable
    {
        private readonly Func<IConnection> _createConnection;
        private readonly IReadOnlyList<Func<IConnection, IDisposable>> _componentFactories;
        private readonly Func<int, Task> _reconnectDelay;
        private readonly IAutoRecoveryDiagnostics _diagnostics;
        private readonly SemaphoreSlim _connectionSemaphore = new SemaphoreSlim(1, 1);
        private string _currentSessionId;
        private IConnection _currentConnection;
        private IReadOnlyList<IDisposable> _currentComponents;
        private volatile int _disposed;

        public AutoRecovery(
            Func<IConnection> createConnection,
            IReadOnlyList<Func<IConnection, IDisposable>> componentFactories,
            TimeSpan reconnectDelay)
            : this(createConnection, componentFactories, _ => reconnectDelay, AutoRecoveryDiagnostics.NoDiagnostics)
        {
        }

        public AutoRecovery(
            Func<IConnection> createConnection,
            IReadOnlyList<Func<IConnection, IDisposable>> componentFactories,
            Func<int, TimeSpan> reconnectDelay,
            IAutoRecoveryDiagnostics diagnostics)
            : this(createConnection, componentFactories,
                retryNumber => Task.Delay(reconnectDelay(retryNumber)), diagnostics)
        {
        }

        internal AutoRecovery(
            Func<IConnection> createConnection,
            IReadOnlyList<Func<IConnection, IDisposable>> componentFactories,
            Func<int, Task> reconnectDelay,
            IAutoRecoveryDiagnostics diagnostics)
        {
            _createConnection = createConnection;
            _componentFactories = componentFactories;
            _reconnectDelay = reconnectDelay;
            _diagnostics = diagnostics;
        }

        public async void Start()
        {
            try
            {
                await ConnectAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                TrackSafe(_diagnostics.TrackUnexpectedException, "Unable to start connect loop.", ex);
            }
        }

        private async Task ConnectAsync()
        {
            await _connectionSemaphore.WaitAsync().ConfigureAwait(false);

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

            for (var attempt = 1; _disposed == 0; attempt += 1)
            {
                TrackSafe(_diagnostics.TrackConnectAttemptStarted, _currentSessionId, attempt);
                var stopwatch = Stopwatch.StartNew();

                try
                {
                    _currentConnection = CreateConnection();
                    _currentComponents = CreateComponents(_currentConnection, _componentFactories);
                    SubscribeOnConnectionShutdown(_currentConnection);

                    // According to "RabbitMQ.Client" reference,
                    // "IConnection.ConnectionShutdown" event will be fired immediately if connection is already closed,
                    // as results we don't need to perform an additional check
                    // that connection is still open after subscribing on the event.

                    TrackSafe(_diagnostics.TrackConnectAttemptSucceeded, _currentSessionId, attempt, stopwatch.Elapsed);
                    return;
                }
                catch (Exception ex)
                {
                    TrackSafe(_diagnostics.TrackConnectAttemptFailed, _currentSessionId, attempt, stopwatch.Elapsed,
                        ex);
                    CleanUpThreadUnsafe();

                    await _reconnectDelay(attempt).ConfigureAwait(false);
                }
            }
        }

        private void SubscribeOnConnectionShutdown(IConnection connection)
        {
            try
            {
                connection.ConnectionShutdown += OnConnectionShutdown;
            }
            catch (Exception ex)
            {
                // Subscribing on "IConnection.ConnectionShutdown" throws at least when connection is already disposed.

                TrackSafe(_diagnostics.TrackUnexpectedException,
                    "Unable to subscribe on IConnection.ConnectionShutdown event.", ex);
                throw;
            }
        }

        private IConnection CreateConnection()
        {
            var stopwatch = Stopwatch.StartNew();

            try
            {
                var connection = _createConnection();

                if (connection is null)
                {
                    throw new Exception("Acquired connection but it is null.");
                }

                if (connection is IAutorecoveringConnection)
                {
                    throw new Exception("Auto recovering connection is not supported.");
                }

                TrackSafe(_diagnostics.TrackCreateConnectionSucceeded, _currentSessionId, stopwatch.Elapsed);
                return connection;
            }
            catch (Exception ex)
            {
                TrackSafe(_diagnostics.TrackCreateConnectionFailed, _currentSessionId, stopwatch.Elapsed, ex);
                throw;
            }
        }

        private IReadOnlyList<IDisposable> CreateComponents(
            IConnection connection,
            IReadOnlyList<Func<IConnection, IDisposable>> componentFactories)
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
                    Interlocked.Exchange(ref closeReason, connection.CloseReason);
                    if (!(closeReason is null))
                    {
                        throw new AlreadyClosedException(closeReason);
                    }

                    components.Add(componentFactories[i](connection));
                }
                catch (Exception ex)
                {
                    TrackSafe(_diagnostics.TrackUnexpectedException, $"Unable to create component#{i}.", ex);

                    // Stop creating components if connection is already closed.
                    if (!connection.IsOpen)
                    {
                        break;
                    }

                    // Otherwise continue, because we don't want to fail all if one component fails with unexpected exception.
                }
            }

            TrackSafe(_diagnostics.TrackCreateComponentsCompleted, _currentSessionId, components.Count,
                componentFactories.Count, stopwatch.Elapsed);
            return components;
        }

        private void CleanUpThreadUnsafe()
        {
            if (_currentConnection is null && _currentComponents is null)
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

            if (!(_currentConnection is null))
            {
                try
                {
                    _currentConnection.ConnectionShutdown -= OnConnectionShutdown;
                }
                catch (Exception ex)
                {
                    // Unsubscribing from "IConnection.ConnectionShutdown" throws at least when connection is already disposed.

                    TrackSafe(_diagnostics.TrackUnexpectedException,
                        "Unable to unsubscribe from IConnection.ConnectionShutdown event.", ex);
                }

                // "IConnection.Dispose()" method implementation calls "IConnection.Close()" with infinite timeout
                // that is used for waiting an internal "ManualResetEventSlim" instance,
                // as result dispose method call could hang for unpredictable time.
                // To prevent this hanging happen
                // we should call "IConnection.Close()" method with some finite timeout first,
                // only after that "IConnection.Dispose() could be safely called.

                try
                {
                    // Make sure that current connection is closed.
                    // Pass "TimeSpan.Zero" as a non-infinite timeout.
                    _currentConnection.Close(TimeSpan.Zero);
                }
                catch (Exception ex)
                {
                    TrackSafe(_diagnostics.TrackUnexpectedException, "Unable to close current connection.", ex);
                }

                try
                {
                    _currentConnection.Dispose();
                }
                catch (Exception ex)
                {
                    TrackSafe(_diagnostics.TrackUnexpectedException, "Unable to dispose current connection.", ex);
                }

                _currentConnection = null;
            }

            TrackSafe(_diagnostics.TrackCleanUpCompleted, _currentSessionId, stopwatch.Elapsed);
        }

        private void OnConnectionShutdown(object sender, ShutdownEventArgs args)
        {
            TrackSafe(_diagnostics.TrackConnectionClosed, args);

            Task.Run(ConnectAsync);
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 1)
            {
                return;
            }

            TrackSafe(_diagnostics.TrackDisposeStarted);
            var stopwatch = Stopwatch.StartNew();

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