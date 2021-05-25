using System.Collections.Generic;
using System.Threading.Tasks;

namespace RabbitMqAsyncPublisher
{
    internal class AsyncPublisherTaskCompletionSourceRegistry
    {
        private readonly Dictionary<ulong, SourceEntry> _sources = new Dictionary<ulong, SourceEntry>();
        private readonly LinkedList<ulong> _deliveryTagQueue = new LinkedList<ulong>();

        public void Register(ulong deliveryTag, TaskCompletionSource<bool> taskCompletionSource)
        {
            _sources[deliveryTag] = new SourceEntry(taskCompletionSource, _deliveryTagQueue.AddLast(deliveryTag));
        }

        public bool TryRemoveSingle(ulong deliveryTag, out TaskCompletionSource<bool> source)
        {
            if (_sources.TryGetValue(deliveryTag, out var entry))
            {
                _sources.Remove(deliveryTag);
                _deliveryTagQueue.Remove(entry.QueueNode);
                source = entry.Source;
                return true;
            }

            source = default;
            return false;
        }

        // ReSharper disable once ReturnTypeCanBeEnumerable.Global
        public IReadOnlyList<TaskCompletionSource<bool>> RemoveAllUpTo(ulong deliveryTag)
        {
            var result = new List<TaskCompletionSource<bool>>();

            while (_deliveryTagQueue.Count > 0 && _deliveryTagQueue.First.Value <= deliveryTag)
            {
                if (_sources.TryGetValue(_deliveryTagQueue.First.Value, out var entry))
                {
                    _sources.Remove(_deliveryTagQueue.First.Value);
                    result.Add(entry.Source);
                }

                _deliveryTagQueue.RemoveFirst();
            }

            return result;
        }

        private readonly struct SourceEntry
        {
            public TaskCompletionSource<bool> Source { get; }

            public LinkedListNode<ulong> QueueNode { get; }

            public SourceEntry(TaskCompletionSource<bool> source, LinkedListNode<ulong> queueNode)
            {
                Source = source;
                QueueNode = queueNode;
            }
        }
    }
}