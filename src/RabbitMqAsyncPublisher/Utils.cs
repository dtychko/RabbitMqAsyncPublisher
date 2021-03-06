using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitMqAsyncPublisher
{
    public static class Utils
    {
        public static Queue<ReadOnlyMemory<byte>> GenerateMessages(int count, int size)
        {
            var messages = new Queue<ReadOnlyMemory<byte>>();

            for (var i = 0; i < count; i++)
            {
                messages.Enqueue(
                    new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes(GenerateString(size)))
                );
            }

            return messages;
        }

        private static string GenerateString(int length)
        {
            const string chars = "qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM";
            var builder = new StringBuilder(length);
            var rand = new Random(DateTime.UtcNow.Millisecond);

            for (var i = 0; i < length; i++)
            {
                builder.Append(chars[rand.Next(chars.Length)]);
            }

            return builder.ToString();
        }
    }
}