using System;
using System.Collections;
using System.Collections.Generic;
using RabbitMQ.Client;
using RabbitMqAsyncPublisher;

namespace Tests
{
    internal class TestAutoRecoveryResource : IAutoRecoveryResource
    {
        private readonly Action _dispose;

        public TestAutoRecoveryResource(Action dispose = null)
        {
            _dispose = dispose;
        }
        
        public void Dispose()
        {
            _dispose?.Invoke();
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

    internal class TestAutoRecoveryResourceList : IReadOnlyCollection<TestAutoRecoveryResource>
    {
        private readonly Func<TestAutoRecoveryResource> _createNext;

        public TestAutoRecoveryResourceList(Func<TestAutoRecoveryResource> createNext)
        {
            _createNext = createNext;
        }

        public List<TestAutoRecoveryResource> Created { get; } = new List<TestAutoRecoveryResource>();

        public TestAutoRecoveryResource CreateItem()
        {
            var resource = _createNext();
            Created.Add(resource);
            return resource;
        }

        public IEnumerator<TestAutoRecoveryResource> GetEnumerator() => Created.GetEnumerator();

        public int Count => Created.Count;

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}