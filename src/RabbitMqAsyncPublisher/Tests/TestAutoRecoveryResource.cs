using System;
using RabbitMQ.Client;
using RabbitMqAsyncPublisher;

namespace Tests
{
    internal class TestAutoRecoveryResource : IAutoRecoveryResource
    {
        public void Dispose()
        {
        }

        public ShutdownEventArgs CloseReason { get; private set; }
        
        public event EventHandler<ShutdownEventArgs> Shutdown;

        public void FireShutdown()
        {
            var args = new ShutdownEventArgs(ShutdownInitiator.Application, 200, "Test");
            CloseReason = args;
            Shutdown?.Invoke(this, args);
        }
    }
}