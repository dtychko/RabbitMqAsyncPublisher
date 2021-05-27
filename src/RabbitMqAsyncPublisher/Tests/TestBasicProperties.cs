using System;
using System.Collections.Generic;
using RabbitMQ.Client;

namespace Tests
{
    internal class TestBasicProperties : IBasicProperties
    {
        public string TestTag { get; set; }

        ushort IContentHeader.ProtocolClassId { get; }

        string IContentHeader.ProtocolClassName { get; }

        void IBasicProperties.ClearAppId()
        {
            throw new NotImplementedException();
        }

        void IBasicProperties.ClearClusterId()
        {
            throw new NotImplementedException();
        }

        void IBasicProperties.ClearContentEncoding()
        {
            throw new NotImplementedException();
        }

        void IBasicProperties.ClearContentType()
        {
            throw new NotImplementedException();
        }

        void IBasicProperties.ClearCorrelationId()
        {
            throw new NotImplementedException();
        }

        void IBasicProperties.ClearDeliveryMode()
        {
            throw new NotImplementedException();
        }

        void IBasicProperties.ClearExpiration()
        {
            throw new NotImplementedException();
        }

        void IBasicProperties.ClearHeaders()
        {
            throw new NotImplementedException();
        }

        void IBasicProperties.ClearMessageId()
        {
            throw new NotImplementedException();
        }

        void IBasicProperties.ClearPriority()
        {
            throw new NotImplementedException();
        }

        void IBasicProperties.ClearReplyTo()
        {
            throw new NotImplementedException();
        }

        void IBasicProperties.ClearTimestamp()
        {
            throw new NotImplementedException();
        }

        void IBasicProperties.ClearType()
        {
            throw new NotImplementedException();
        }

        void IBasicProperties.ClearUserId()
        {
            throw new NotImplementedException();
        }

        bool IBasicProperties.IsAppIdPresent()
        {
            throw new NotImplementedException();
        }

        bool IBasicProperties.IsClusterIdPresent()
        {
            throw new NotImplementedException();
        }

        bool IBasicProperties.IsContentEncodingPresent()
        {
            throw new NotImplementedException();
        }

        bool IBasicProperties.IsContentTypePresent()
        {
            throw new NotImplementedException();
        }

        bool IBasicProperties.IsCorrelationIdPresent()
        {
            throw new NotImplementedException();
        }

        bool IBasicProperties.IsDeliveryModePresent()
        {
            throw new NotImplementedException();
        }

        bool IBasicProperties.IsExpirationPresent()
        {
            throw new NotImplementedException();
        }

        bool IBasicProperties.IsHeadersPresent()
        {
            throw new NotImplementedException();
        }

        bool IBasicProperties.IsMessageIdPresent()
        {
            throw new NotImplementedException();
        }

        bool IBasicProperties.IsPriorityPresent()
        {
            throw new NotImplementedException();
        }

        bool IBasicProperties.IsReplyToPresent()
        {
            throw new NotImplementedException();
        }

        bool IBasicProperties.IsTimestampPresent()
        {
            throw new NotImplementedException();
        }

        bool IBasicProperties.IsTypePresent()
        {
            throw new NotImplementedException();
        }

        bool IBasicProperties.IsUserIdPresent()
        {
            throw new NotImplementedException();
        }

        string IBasicProperties.AppId { get; set; }

        string IBasicProperties.ClusterId { get; set; }

        string IBasicProperties.ContentEncoding { get; set; }

        string IBasicProperties.ContentType { get; set; }

        string IBasicProperties.CorrelationId { get; set; }

        byte IBasicProperties.DeliveryMode { get; set; }

        string IBasicProperties.Expiration { get; set; }

        IDictionary<string, object> IBasicProperties.Headers { get; set; } = new Dictionary<string, object>();

        string IBasicProperties.MessageId { get; set; }

        bool IBasicProperties.Persistent { get; set; }

        byte IBasicProperties.Priority { get; set; }

        string IBasicProperties.ReplyTo { get; set; }

        PublicationAddress IBasicProperties.ReplyToAddress { get; set; }

        AmqpTimestamp IBasicProperties.Timestamp { get; set; }

        string IBasicProperties.Type { get; set; }

        string IBasicProperties.UserId { get; set; }
    }
}