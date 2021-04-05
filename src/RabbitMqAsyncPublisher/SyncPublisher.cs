using System;
using RabbitMQ.Client;

namespace RabbitMqAsyncPublisher
{
    public class SyncPublisher
    {
        private readonly IModel _model;

        public SyncPublisher(IModel model)
        {
            _model = model;
        }

        public void Publish(ReadOnlyMemory<byte> message)
        {
            _model.BasicPublish("", "test_queue", _model.CreateBasicProperties(), message);
            _model.WaitForConfirms();
        }
    }
}