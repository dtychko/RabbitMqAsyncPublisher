using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace RabbitMqAsyncPublisher
{
    public class AutoRecovery : IDisposable
    {
        private readonly Func<IConnection> _connect;
        private readonly IReadOnlyList<Func<IConnection, IDisposable>> _componentRegistrations;
        private readonly TimeSpan _reconnectDelay;
        private readonly SemaphoreSlim _connectionSemaphore = new SemaphoreSlim(1, 1);
        private IConnection _currentConnection;
        private IReadOnlyList<IDisposable> _currentComponents;
        private volatile int _disposed;

        public AutoRecovery(
            Func<IConnection> connect,
            IReadOnlyList<Func<IConnection, IDisposable>> componentRegistrations,
            TimeSpan reconnectDelay)
        {
            _connect = connect;
            _componentRegistrations = componentRegistrations;
            _reconnectDelay = reconnectDelay;
        }

        public async void Start()
        {
            try
            {
                await ConnectAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Unable to start recovery loop: {ex}");
            }
        }

        private async Task ConnectAsync()
        {
            await _connectionSemaphore.WaitAsync();

            try
            {
                await ConnectThreadUnsafeAsync();
            }
            finally
            {
                _connectionSemaphore.Release();
            }
        }

        private async Task ConnectThreadUnsafeAsync()
        {
            // Make sure that previous connection is disposed together with its components
            CleanUpThreadUnsafe();

            for (var attempt = 1;; attempt += 1)
            {
                if (_disposed == 1)
                {
                    return;
                }

                Console.WriteLine($"Connect attempt#{attempt}");
                var stopwatch = Stopwatch.StartNew();

                try
                {
                    _currentConnection = CreateConnectionThreadUnsafe();
                    _currentComponents = InitializeComponentsThreadUnsafe(_currentConnection);

                    // 1. According to "RabbitMQ.Client" reference,
                    // "IConnection.ConnectionShutdown" event will be fired immediately if connection is already closed
                    // 2. Subscribing on "IConnection.ConnectionShutdown" throws if connection is already disposed
                    try
                    {
                        _currentConnection.ConnectionShutdown += OnConnectionShutdown;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Unable to subscribe on IConnection.ConnectionShutdown event: {ex}");
                        throw;
                    }

                    Console.WriteLine($"Connect attempt#{attempt} succeeded in {stopwatch.ElapsedMilliseconds} ms");

                    return;
                }
                catch (Exception)
                {
                    CleanUpThreadUnsafe();
                    await Task.Delay(_reconnectDelay);
                }
            }
        }

        private IConnection CreateConnectionThreadUnsafe()
        {
            try
            {
                var connection = _connect();

                if (connection is null)
                {
                    throw new Exception("Acquired connection but it is null");
                }

                if (connection is IAutorecoveringConnection)
                {
                    throw new Exception("Auto recovering connection is not supported");
                }

                return connection;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Unable to acquire connection: {ex}");
                throw;
            }
        }

        private IReadOnlyList<IDisposable> InitializeComponentsThreadUnsafe(IConnection connection)
        {
            var components = new List<IDisposable>();

            foreach (var registration in _componentRegistrations)
            {
                try
                {
                    ShutdownEventArgs closeReason = null;
                    Interlocked.Exchange(ref closeReason, connection.CloseReason);

                    if (!(closeReason is null))
                    {
                        throw new AlreadyClosedException(closeReason);
                    }

                    components.Add(registration(connection));
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Unable to initialize component: {ex}");
                    // continue because we don't want to fail all if one component fails
                }
            }

            return components;
        }

        private void CleanUpThreadUnsafe()
        {
            if (!(_currentComponents is null))
            {
                foreach (var component in _currentComponents)
                {
                    try
                    {
                        component.Dispose();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Unable to dispose component: {ex}");
                    }
                }

                _currentComponents = null;
            }

            if (!(_currentConnection is null))
            {
                // Unsubscribing from "IConnection.ConnectionShutdown" throws if connection is already disposed
                try
                {
                    _currentConnection.ConnectionShutdown -= OnConnectionShutdown;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Unable to unsubscribe from IConnection.ConnectionShutdown event: {ex}");
                }

                try
                {
                    _currentConnection.Dispose();
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Unable to dispose current connection: {ex}");
                }

                _currentConnection = null;
            }
        }

        private void OnConnectionShutdown(object sender, ShutdownEventArgs e)
        {
            Task.Run(() =>
            {
                Console.WriteLine("Connection Shutdown");
                return ConnectAsync();
            });
        }

        // TODO: integrate time-based connection check and re-init

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 1)
            {
                return;
            }

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
        }
    }
}