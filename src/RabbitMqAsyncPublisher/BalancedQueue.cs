using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMqAsyncPublisher
{
    public class BalancedQueue<TValue>
    {
        private readonly AsyncManualResetEvent _gateEvent = new AsyncManualResetEvent(false);
        private readonly int _partitionProcessingLimit;

        private readonly ConcurrentDictionary<string, Partition> _partitions =
            new ConcurrentDictionary<string, Partition>();

        private readonly ConcurrentQueue<Partition> _partitionQueue = new ConcurrentQueue<Partition>();
        private volatile int _partitionQueueCount;

        public BalancedQueue(int partitionProcessingLimit)
        {
            _partitionProcessingLimit = partitionProcessingLimit;
        }

        public void Enqueue(string partitionKey, TValue value)
        {
            var partition = _partitions.GetOrAdd(partitionKey, _ => new Partition(partitionKey));
            var shouldEnqueuePartition = false;

            lock (partition)
            {
                partition.Queue.Enqueue(value);

                if (partition.Queue.Count == 1 && partition.ProcessingCount < _partitionProcessingLimit)
                {
                    shouldEnqueuePartition = true;
                }
            }

            if (shouldEnqueuePartition)
            {
                _partitionQueue.Enqueue(partition);
                Interlocked.Increment(ref _partitionQueueCount);
                AdjustGate();
            }
        }

        public async Task WaitToDequeueAsync(CancellationToken cancellationToken = default)
        {
            await _gateEvent.WaitAsync(cancellationToken);
        }

        public bool TryDequeue(out Func<Func<TValue, string, Task>, Task> handler)
        {
            if (!_partitionQueue.TryDequeue(out var partition))
            {
                AdjustGate();
                handler = default;
                return false;
            }

            Interlocked.Decrement(ref _partitionQueueCount);
            AdjustGate();

            var shouldEnqueuePartition = false;
            TValue value;

            lock (partition)
            {
                value = partition.Queue.Dequeue();
                partition.ProcessingCount += 1;

                if (partition.Queue.Count > 0 && partition.ProcessingCount < _partitionProcessingLimit)
                {
                    shouldEnqueuePartition = true;
                }
            }

            if (shouldEnqueuePartition)
            {
                _partitionQueue.Enqueue(partition);
                Interlocked.Increment(ref _partitionQueueCount);
                AdjustGate();
            }

            handler = handle => HandleSafe(handle, value, partition);
            return true;
        }

        private async Task HandleSafe(Func<TValue, string, Task> handle, TValue value, Partition partition)
        {
            try
            {
                await handle(value, partition.Name).ConfigureAwait(false);
            }
            catch
            {
                // TODO: Log unexpected error
            }

            var shouldEnqueuePartition = false;

            lock (partition)
            {
                partition.ProcessingCount -= 1;

                if (partition.Queue.Count > 0 && partition.ProcessingCount == _partitionProcessingLimit - 1)
                {
                    shouldEnqueuePartition = true;
                }
            }

            if (shouldEnqueuePartition)
            {
                _partitionQueue.Enqueue(partition);
                Interlocked.Increment(ref _partitionQueueCount);
                AdjustGate();
            }
        }

        private void AdjustGate()
        {
            lock (_gateEvent)
            {
                if (_partitionQueueCount > 0)
                {
                    _gateEvent.Set();
                }
                else
                {
                    _gateEvent.Reset();
                }
            }
        }

        private class Partition
        {
            public readonly string Name;
            public readonly Queue<TValue> Queue = new Queue<TValue>();
            public int ProcessingCount;

            public Partition(string name)
            {
                Name = name;
            }
        }
    }
}