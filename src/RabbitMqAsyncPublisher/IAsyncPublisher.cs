using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace RabbitMqAsyncPublisher
{
    public interface IAsyncPublisher<TResult> : IDisposable
    {
        Task<TResult> PublishAsync(string exchange,
            string routingKey,
            ReadOnlyMemory<byte> body,
            MessageProperties properties,
            string correlationId = null,
            CancellationToken cancellationToken = default);
    }

    // TODO: Think about converting to readonly struct
    public class MessageProperties
    {
        public static readonly MessageProperties Default = new MessageProperties();

        public bool? Persistent { get; }
        public string ReplyTo { get; }
        public string CorrelationId { get; }
        public TimeSpan? Expiration { get; }
        public IReadOnlyDictionary<string, object> Headers { get; }

        public MessageProperties(
            bool? persistent = null,
            string replyTo = null,
            string correlationId = null,
            TimeSpan? expiration = null,
            IReadOnlyDictionary<string, object> headers = null)
        {
            Persistent = persistent;
            ReplyTo = replyTo;
            CorrelationId = correlationId;
            Expiration = expiration;
            Headers = headers;
        }

        public IBasicProperties ApplyTo(IBasicProperties properties)
        {
            if (!(Persistent is null))
            {
                properties.Persistent = Persistent.Value;
            }

            if (!(CorrelationId is null))
            {
                properties.CorrelationId = CorrelationId;
            }

            if (!(ReplyTo is null))
            {
                properties.ReplyTo = ReplyTo;
            }

            if (!(Expiration is null))
            {
                properties.Expiration = SerializeExpiration(Expiration.Value);
            }

            if (!(Headers is null))
            {
                properties.Headers = Headers.ToDictionary(x => x.Key, x => x.Value);
            }

            return properties;
        }

        public static string SerializeExpiration(TimeSpan expiration)
        {
            return ((int) expiration.TotalMilliseconds).ToString(CultureInfo.InvariantCulture);
        }
    }
}