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

        public void Publish(ReadOnlyMemory<byte> message, IBasicProperties properties)
        {
            _model.BasicPublish("", "test_queue", properties, message);
            _model.WaitForConfirms();
        }
    }
}