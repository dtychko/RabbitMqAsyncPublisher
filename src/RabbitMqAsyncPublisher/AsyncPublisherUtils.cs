using System;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMqAsyncPublisher
{
    internal static class AsyncPublisherUtils
    {
        public static void ScheduleTrySetResult<TResult>(TaskCompletionSource<TResult> source, TResult result)
        {
            Task.Run(() => source.TrySetResult(result));
        }

        public static void ScheduleTrySetException<TResult>(TaskCompletionSource<TResult> source, Exception ex)
        {
            Task.Run(() => source.TrySetException(ex));
        }

        public static void ScheduleTrySetCanceled<TResult>(TaskCompletionSource<TResult> source,
            CancellationToken cancellationToken)
        {
            // ReSharper disable once MethodSupportsCancellation
            Task.Run(() => source.TrySetCanceled(cancellationToken));
        }

        public static void TrackSafe<T1>(Action<T1> track, T1 arg1)
        {
            TrackSafe(() => track(arg1));
        }

        public static void TrackSafe<T1, T2>(Action<T1, T2> track, T1 arg1, T2 arg2)
        {
            TrackSafe(() => track(arg1, arg2));
        }

        public static void TrackSafe<T1, T2, T3>(Action<T1, T2, T3> track, T1 arg1, T2 arg2, T3 arg3)
        {
            TrackSafe(() => track(arg1, arg2, arg3));
        }

        public static void TrackSafe<T1, T2, T3, T4>(Action<T1, T2, T3, T4> track, T1 arg1, T2 arg2, T3 arg3, T4 arg4)
        {
            TrackSafe(() => track(arg1, arg2, arg3, arg4));
        }

        public static void TrackSafe<T1, T2, T3, T4, T5>(Action<T1, T2, T3, T4, T5> track, T1 arg1, T2 arg2, T3 arg3,
            T4 arg4, T5 arg5)
        {
            TrackSafe(() => track(arg1, arg2, arg3, arg4, arg5));
        }

        public static void TrackSafe(Action track)
        {
            try
            {
                track();
            }
            catch
            {
                // Ignore all exceptions during tracking diagnostics data 
            }
        }
    }
}