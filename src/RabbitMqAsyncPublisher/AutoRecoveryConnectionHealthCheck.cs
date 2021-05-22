using System;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace RabbitMqAsyncPublisher
{
    public class AutoRecoveryConnectionHealthCheck : IDisposable
    {
        private readonly IConnection _connection;
        private readonly TimeSpan _checkInterval;

        private readonly CancellationTokenSource _cancellationTokenSource;
        private readonly CancellationToken _cancellationToken;

        public AutoRecoveryConnectionHealthCheck(IConnection connection, TimeSpan checkInterval)
        {
            _connection = connection;
            _checkInterval = checkInterval;

            _cancellationTokenSource = new CancellationTokenSource();
            _cancellationToken = _cancellationTokenSource.Token;

            Task.Run(ScheduleHealthCheckLoopIterationSafe, _cancellationToken);
        }

        private async void ScheduleHealthCheckLoopIterationSafe()
        {
            try
            {
                await Task.Delay(_checkInterval, _cancellationToken).ConfigureAwait(false);

                if (!IsConnectionAliveSafe())
                {
                    Console.WriteLine($"[{nameof(AutoRecoveryConnectionHealthCheck)}] Connection is no longer alive.");
                    // Connection should be closed, because it is no longer alive,
                    // no more health checks are required for the connection.
                    CloseConnectionSafe();
                    return;
                }
            }
            catch (OperationCanceledException)
            {
                // Health check loop is cancelled, just return.
                Console.WriteLine($"[{nameof(AutoRecoveryConnectionHealthCheck)}] Health check loop cancelled");
                return;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{nameof(AutoRecoveryConnectionHealthCheck)}] Unexpected error: {ex}");
                // ignored
            }

            // Probably the connection is still alive, schedule the next health check loop iteration.
#pragma warning disable 4014
            Task.Run(ScheduleHealthCheckLoopIterationSafe, _cancellationToken);
#pragma warning restore 4014
        }

        private bool IsConnectionAliveSafe()
        {
            try
            {
                using (_connection.CreateModel())
                {
                }

                return true;
            }
            catch
            {
                return false;
            }
        }

        private void CloseConnectionSafe()
        {
            try
            {
                // If timeout isn't passed than "Close" method call could hangs 
                _connection.Close(TimeSpan.Zero);
            }
            catch
            {
                // Most likely connection is already closed, just ignore an exception
            }
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
        }
    }
}