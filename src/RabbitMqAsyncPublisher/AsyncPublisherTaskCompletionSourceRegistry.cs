using System.Collections.Generic;
using System.Threading.Tasks;

namespace RabbitMqAsyncPublisher
{
    internal class AsyncPublisherTaskCompletionSourceRegistry
    {
        private readonly Dictionary<ulong, SourceEntry> _sources = new Dictionary<ulong, SourceEntry>();
        private readonly LinkedList<ulong> _deliveryTagQueue = new LinkedList<ulong>();
        private volatile int _size;

        public int Size => _size;

        public void Register(ulong deliveryTag, TaskCompletionSource<bool> taskCompletionSource)
        {
            lock (_sources)
            {
                _sources.Add(deliveryTag,
                    new SourceEntry(taskCompletionSource, _deliveryTagQueue.AddLast(deliveryTag)));
                _size = _deliveryTagQueue.Count;
            }
        }

        public bool TryRemoveSingle(ulong deliveryTag, out TaskCompletionSource<bool> source)
        {
            lock (_sources)
            {
                if (_sources.TryGetValue(deliveryTag, out var entry))
                {
                    _sources.Remove(deliveryTag);
                    _deliveryTagQueue.Remove(entry.QueueNode);
                    _size = _deliveryTagQueue.Count;

                    source = entry.Source;
                    return true;
                }

                source = default;
                return false;
            }
        }

        // ReSharper disable once ReturnTypeCanBeEnumerable.Global
        public IReadOnlyList<TaskCompletionSource<bool>> RemoveAllUpTo(ulong deliveryTag)
        {
            lock (_sources)
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

                _size = _deliveryTagQueue.Count;

                return result;
            }
        }

        private struct SourceEntry
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