using System;
using System.Collections.Generic;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Tests
{
    internal class TestNonRecoverableConnection : IConnection
    {
        private readonly Func<TestNonRecoverableRabbitModel> _createModel;

        public TestNonRecoverableConnection(Func<TestNonRecoverableRabbitModel> createModel)
        {
            _createModel = createModel;
        }

        public List<TestNonRecoverableRabbitModel> CreatedModels { get; } = new List<TestNonRecoverableRabbitModel>();

        public IModel CreateModel()
        {
            var model = _createModel();
            CreatedModels.Add(model);
            return model;
        }

        public ShutdownEventArgs CloseReason { get; private set; }

        public void ImitateClose(ShutdownEventArgs args)
        {
            if (CloseReason != null)
            {
                return;
            }
            
            CloseReason = args;
            ConnectionShutdown?.Invoke(this, args);
            foreach (var model in CreatedModels)
            {
                model.FireModelShutdown(args);
            }
        }

        public event EventHandler<ShutdownEventArgs> ConnectionShutdown;

        void IDisposable.Dispose()
        {
        }

        void IConnection.Close()
        {
            ImitateClose(new ShutdownEventArgs(ShutdownInitiator.Application, 200, "Closed via interface call"));
        }

        void IConnection.Close(ushort reasonCode, string reasonText)
        {
            ImitateClose(new ShutdownEventArgs(ShutdownInitiator.Application, reasonCode, reasonText));
        }

        void IConnection.Close(TimeSpan timeout)
        {
            IConnection c = this;
            c.Close();
        }

        void IConnection.Close(ushort reasonCode, string reasonText, TimeSpan timeout)
        {
            IConnection c = this;
            c.Close(reasonCode, reasonText);
        }

        #region Unused in tests
        int INetworkConnection.LocalPort => throw new NotImplementedException();

        int INetworkConnection.RemotePort => throw new NotImplementedException();
        
        void IConnection.UpdateSecret(string newSecret, string reason) => throw new NotImplementedException();

        void IConnection.Abort() => throw new NotImplementedException();

        void IConnection.Abort(ushort reasonCode, string reasonText) => throw new NotImplementedException();

        void IConnection.Abort(TimeSpan timeout) => throw new NotImplementedException();

        void IConnection.Abort(ushort reasonCode, string reasonText, TimeSpan timeout) => throw new NotImplementedException();

        void IConnection.HandleConnectionBlocked(string reason) => throw new NotImplementedException();

        void IConnection.HandleConnectionUnblocked() => throw new NotImplementedException();

        ushort IConnection.ChannelMax => throw new NotImplementedException();

        IDictionary<string, object> IConnection.ClientProperties => throw new NotImplementedException();

        AmqpTcpEndpoint IConnection.Endpoint => throw new NotImplementedException();

        uint IConnection.FrameMax => throw new NotImplementedException();

        TimeSpan IConnection.Heartbeat => throw new NotImplementedException();

        bool IConnection.IsOpen => throw new NotImplementedException();

        AmqpTcpEndpoint[] IConnection.KnownHosts => throw new NotImplementedException();

        IProtocol IConnection.Protocol => throw new NotImplementedException();

        IDictionary<string, object> IConnection.ServerProperties => throw new NotImplementedException();

        IList<ShutdownReportEntry> IConnection.ShutdownReport => throw new NotImplementedException();

        string IConnection.ClientProvidedName => throw new NotImplementedException();

        event EventHandler<CallbackExceptionEventArgs> IConnection.CallbackException
        {
            add => throw new NotImplementedException();
            remove => throw new NotImplementedException();
        }

        event EventHandler<ConnectionBlockedEventArgs> IConnection.ConnectionBlocked
        {
            add => throw new NotImplementedException();
            remove => throw new NotImplementedException();
        }

        event EventHandler<EventArgs> IConnection.ConnectionUnblocked
        {
            add => throw new NotImplementedException();
            remove => throw new NotImplementedException();
        }
        #endregion
    }
}