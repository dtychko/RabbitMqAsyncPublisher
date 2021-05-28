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

        public static readonly IDisposable Empty = new Disposable(() => { });
    }

    public class CompositeDisposable : IDisposable
    {
        private readonly IReadOnlyList<IDisposable> _children;

        public CompositeDisposable(IReadOnlyList<IDisposable> children)
        {
            _children = children;
        }

        public CompositeDisposable(params IDisposable[] childrenParams): this(children: childrenParams)
        {
        }
        
        public void Dispose()
        {
            var exceptions = new List<Exception>();
            foreach (var child in _children)
            {
                try
                {
                    child.Dispose();
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                }
            }

            if (exceptions.Count > 0)
            {
                throw new AggregateException(exceptions);
            }
        }
    }
}