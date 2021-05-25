using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using RabbitMQ.Client;

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

        public static string GenerateString(int length)
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

        private static readonly Type _basicPropertiesType =
            typeof(IModel).Assembly.GetTypes().First(x => x.FullName == "RabbitMQ.Client.Framing.BasicProperties");
        
        public static IBasicProperties CreateBasicProperties()
        {
            var sw = new Stopwatch();
            sw.Start();
            var result =  (IBasicProperties) Activator.CreateInstance(_basicPropertiesType);
            Console.WriteLine($"Created in {sw.Elapsed.TotalMilliseconds}ms");
            return result;
        }
    }

    public class Disposable : IDisposable
    {
        private readonly Action _onDispose;

        public Disposable(Action onDispose)
        {
            _onDispose = onDispose;
        }

        public void Dispose()
        {
            _onDispose();
        }
    }
}