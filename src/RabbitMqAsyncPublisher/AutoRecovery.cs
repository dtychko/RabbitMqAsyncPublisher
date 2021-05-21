using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace RabbitMqAsyncPublisher
{
    public class AutoRecovery : IDisposable
    {
        private readonly Func<IConnection> _getConnection;
        private readonly TimeSpan _connectionAcquireInterval;
        private readonly SemaphoreSlim _connectionSemaphore = new SemaphoreSlim(1, 1);
        
        private IConnection _currentConnection;
        private volatile int _disposed;

        private readonly List<IDisposable> _activeComponents = new List<IDisposable>();
        private readonly IReadOnlyList<Func<IDisposable>> _componentRegistrations;
        
        public AutoRecovery(
            Func<IConnection> getConnection,
            IReadOnlyList<Func<IDisposable>> componentRegistrations,
            TimeSpan connectionAcquireInterval)
        {
            _getConnection = getConnection;
            _componentRegistrations = componentRegistrations;
            _connectionAcquireInterval = connectionAcquireInterval;
        }

        public async void Start()
        {
            try
            {
                await StartAsync(default);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Unable to start recovery loop: {ex}");
            }
        }

        private async Task StartAsync(CancellationToken cancellationToken)
        {
            await _connectionSemaphore.WaitAsync(cancellationToken);
            try
            {
                await StartThreadUnsafeAsync(cancellationToken);
            }
            finally
            {
                _connectionSemaphore.Release();
            }
        }

        private async Task StartThreadUnsafeAsync(CancellationToken cancellationToken)
        {
            while (_currentConnection is null
                   && _disposed == 0 && !cancellationToken.IsCancellationRequested && !CreateAndInitConnectionThreadUnsafe())
            {
                await Task.Delay(_connectionAcquireInterval, cancellationToken);
            }
        }

        private bool CreateAndInitConnectionThreadUnsafe()
        {
            try
            {
                _currentConnection =
                    _getConnection() ?? throw new Exception("Acquired connection but it is null");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Unable to acquire connection: {ex}");
                return false;
            }

            try
            {
                if (!_currentConnection.IsOpen)
                {
                    throw new Exception("Acquired connection but it's already closed");
                }

                foreach (var registration in _componentRegistrations)
                {
                    try
                    {
                        _activeComponents.Add(registration());
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Unable to initialize component: {ex}");
                        // continue because we don't want to fail all if one component fails
                    }
                }

                _currentConnection.ConnectionShutdown += OnConnectionShutdown;
                // TODO: here connection may become closed, and our OnConnectionShutdown schedules next loop iteration,
                // which may overlaps with outer loop, creating redundant cleanup/reinit.
                if (!_currentConnection.IsOpen)
                {
                    throw new Exception("Acquired connection but it's already closed");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Unable to init on connection: {ex}");
                CleanUpThreadUnsafe();
                return false;
            }

            return true;
        }

        private void CleanUpThreadUnsafe()
        {
            foreach (var component in _activeComponents)
            {
                try
                {
                    component.Dispose();
                }
                catch (Exception componentException)
                {
                    Console.WriteLine($"Unable to dispose component: {componentException}");
                }
            }

            _activeComponents.Clear();
            _currentConnection.Dispose();
            _currentConnection.ConnectionShutdown -= OnConnectionShutdown;
            _currentConnection = null;
        }

        private void OnConnectionShutdown(object sender, ShutdownEventArgs e)
        {
            // TODO: this could trigger unwanted cleanups and re-starts
            // when this task acquires semaphore after long time after main loop successfully completes initialization
            Task.Run(async () =>
            {
                await _connectionSemaphore.WaitAsync();
                try
                {
                    CleanUpThreadUnsafe();
                    await StartThreadUnsafeAsync(default);
                }
                finally
                {
                    _connectionSemaphore.Release();
                }
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
                if (_currentConnection != null)
                {
                    CleanUpThreadUnsafe();
                }
            }
            finally
            {
                _connectionSemaphore.Release();
                _connectionSemaphore.Dispose();
            }
        }
    }
}