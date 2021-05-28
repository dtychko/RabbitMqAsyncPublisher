using System;
using System.Collections.Generic;

namespace RabbitMqAsyncPublisher
{
    internal class JobQueue<TJob>
    {
        private readonly LinkedList<TJob> _queue = new LinkedList<TJob>();
        private volatile int _size;
        private bool _completed;

        public int Size => _size;

        public Func<bool> Enqueue(TJob job)
        {
            LinkedListNode<TJob> queueNode;

            lock (_queue)
            {
                if (_completed)
                {
                    throw new InvalidOperationException("Job queue is already completed.");
                }

                queueNode = _queue.AddLast(job);
                _size = _queue.Count;
            }

            return () => TryRemoveJob(queueNode);
        }

        private bool TryRemoveJob(LinkedListNode<TJob> jobNode)
        {
            lock (_queue)
            {
                if (jobNode.List is null)
                {
                    return false;
                }

                _queue.Remove(jobNode);
                _size = _queue.Count;
            }

            return true;
        }

        public bool CanDequeueJob()
        {
            lock (_queue)
            {
                return _queue.Count > 0;
            }
        }

        public TJob DequeueJob()
        {
            TJob job;

            lock (_queue)
            {
                if (_queue.Count == 0)
                {
                    throw new InvalidOperationException("Job queue is empty.");
                }

                job = _queue.First.Value;
                _queue.RemoveFirst();
                _size = _queue.Count;
            }

            return job;
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