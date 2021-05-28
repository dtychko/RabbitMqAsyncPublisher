using RabbitMqAsyncPublisher;

namespace Tests
{
    internal class TestMessageProperties : MessageProperties
    {
        public string TestTag { get; set; }
    }
}