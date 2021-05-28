using RabbitMqAsyncPublisher;

namespace Tests
{
    internal class TestBasicProperties : MessageProperties
    {
        public string TestTag { get; set; }
    }
}