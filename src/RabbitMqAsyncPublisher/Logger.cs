using System;

namespace RabbitMqAsyncPublisher
{
    public interface ILogger
    {
        void Debug(string format, params object[] args);

        void Info(string format, params object[] args);

        void Warn(string format, params object[] args);

        void Error(string format, Exception ex, params object[] args);
    }

    internal class EmptyLogger : ILogger
    {
        public static EmptyLogger Instance = new EmptyLogger();

        private EmptyLogger()
        {
        }

        public void Debug(string format, params object[] args)
        {
        }

        public void Info(string format, params object[] args)
        {
        }

        public void Warn(string format, params object[] args)
        {
        }

        public void Error(string format, Exception ex, params object[] args)
        {
        }
    }
}