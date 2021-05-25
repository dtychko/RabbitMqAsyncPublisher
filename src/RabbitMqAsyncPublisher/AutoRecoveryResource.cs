using System;
using RabbitMQ.Client;

namespace RabbitMqAsyncPublisher
{
    using static DiagnosticsUtils;

    /// <summary>
    /// Represents a resource with shutdown-able lifecycle,
    /// which can be plugged into auto-recovery mechanism,
    /// e.g. RabbitMQ connection or model.
    /// </summary>
    public interface IAutoRecoveryResource : IDisposable
    {
        /// <summary>
        /// Null if resource is in open state.
        /// Otherwise, describes why resource was closed.
        /// </summary>
        ShutdownEventArgs CloseReason { get; }

        event EventHandler<ShutdownEventArgs> Shutdown;
    }

    public class AutoRecoveryConnection : IAutoRecoveryResource
    {
        private readonly IUnexpectedExceptionDiagnostics _diagnostics;

        public IConnection Value { get; }

        public ShutdownEventArgs CloseReason => Value.CloseReason;

        public AutoRecoveryConnection(IConnection connection)
            : this(connection, AutoRecoveryEmptyDiagnostics.NoDiagnostics)
        {
        }

        public AutoRecoveryConnection(IConnection connection, IUnexpectedExceptionDiagnostics diagnostics)
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
            _diagnostics = diagnostics;
        }

        public event EventHandler<ShutdownEventArgs> Shutdown
        {
            add
            {
                try
                {
                    // Subscribing to "IConnection.ConnectionShutdown" throws at least when connection is already disposed.
                    Value.ConnectionShutdown += value;
                }
                catch (Exception ex)
                {
                    TrackSafe(_diagnostics.TrackUnexpectedException,
                        "Unable to subscribe on IConnection.ConnectionShutdown event.", ex);
                }
            }
            remove
            {
                try
                {
                    // Unsubscribing from "IConnection.ConnectionShutdown" throws at least when connection is already disposed.
                    Value.ConnectionShutdown -= value;
                }
                catch (Exception ex)
                {
                    TrackSafe(_diagnostics.TrackUnexpectedException,
                        "Unable to unsubscribe from IConnection.ConnectionShutdown event.", ex);
                }
            }
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
                //
                // Make sure that current connection is closed.
                // Pass "TimeSpan.Zero" as a non-infinite timeout.
                try
                {
                    Value.Close(TimeSpan.Zero);
                }
                catch (Exception ex)
                {
                    _diagnostics.TrackUnexpectedException("Unable to close connection.", ex);
                }
            }

            try
            {
                Value.Dispose();
            }
            catch (Exception ex)
            {
                TrackSafe(_diagnostics.TrackUnexpectedException, "Unable to dispose connection.", ex);
            }
        }
    }

    public class AutoRecoveryModel : IAutoRecoveryResource
    {
        private readonly IUnexpectedExceptionDiagnostics _diagnostics;

        public IModel Value { get; }

        public ShutdownEventArgs CloseReason => Value.CloseReason;

        public AutoRecoveryModel(IModel model)
            : this(model, AutoRecoveryEmptyDiagnostics.NoDiagnostics)
        {
        }

        public AutoRecoveryModel(IModel model, IUnexpectedExceptionDiagnostics diagnostics)
        {
            if (model is null)
            {
                throw new ArgumentNullException(nameof(model));
            }

            // TODO: would be great to ensure that model itself is not recoverable
            // to avoid conflicts with RabbitMQ lib recovery behavior
            // Unfortunately, can't do that with simple `model is IRecoverable` check because all models implement this interface

            Value = model;
            _diagnostics = diagnostics;
        }

        public event EventHandler<ShutdownEventArgs> Shutdown
        {
            add
            {
                try
                {
                    Value.ModelShutdown += value;
                }
                catch (Exception ex)
                {
                    TrackSafe(_diagnostics.TrackUnexpectedException,
                        "Unable to subscribe on IModel.ModelShutdown event.", ex);
                }
            }
            remove
            {
                try
                {
                    Value.ModelShutdown -= value;
                }
                catch (Exception ex)
                {
                    TrackSafe(_diagnostics.TrackUnexpectedException,
                        "Unable to unsubscribe from IModel.ModelShutdown event.", ex);
                }
            }
        }

        public void Dispose()
        {
            try
            {
                Value.Dispose();
            }
            catch (Exception ex)
            {
                TrackSafe(_diagnostics.TrackUnexpectedException, "Unable to dispose model.", ex);
            }
        }
    }
}