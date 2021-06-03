using System;
using System.Collections.Generic;

namespace RabbitMqAsyncPublisher
{
    internal class LinkedListQueue<T>
    {
        private readonly LinkedList<T> _queue = new LinkedList<T>();
        private volatile int _size;
        private bool _completed;

        public int Size => _size;

        public Func<bool> Enqueue(T job)
        {
            LinkedListNode<T> queueNode;

            lock (_queue)
            {
                if (_completed)
                {
                    throw new InvalidOperationException("Job queue is already completed.");
                }

                queueNode = _queue.AddLast(job);
                _size = _queue.Count;
            }

            return () => TryRemove(queueNode);
        }

        private bool TryRemove(LinkedListNode<T> node)
        {
            lock (_queue)
            {
                if (node.List is null)
                {
                    return false;
                }

                _queue.Remove(node);
                _size = _queue.Count;
            }

            return true;
        }

        public bool CanDequeue()
        {
            lock (_queue)
            {
                return _queue.Count > 0;
            }
        }

        public T Dequeue()
        {
            T item;

            lock (_queue)
            {
                if (_queue.Count == 0)
                {
                    throw new InvalidOperationException("Job queue is empty.");
                }

                item = _queue.First.Value;
                _queue.RemoveFirst();
                _size = _queue.Count;
            }

            return item;
        }

        public bool TryDequeue(out T value)
        {
            lock (_queue)
            {
                if (_queue.Count == 0)
                {
                    value = default;
                    return false;
                }

                value = _queue.First.Value;
                _queue.RemoveFirst();
                _size = _queue.Count;
                return true;
            }
        }

        public T Peek()
        {
            lock (_queue)
            {
                if (_queue.Count == 0)
                {
                    throw new InvalidOperationException("Job queue is empty");
                }

                return _queue.First.Value;
            }
        }

        public void Complete()
        {
            lock (_queue)
            {
                _completed = true;
            }
        }
    }
}