using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMqAsyncPublisher
{
    internal static class AsyncPublisherUtils
    {
        public static async Task<TResult> PublishAsyncCore<TResult, TStatus>(PublishArgs publishArgs,
            CancellationToken cancellationToken, IQueueBasedPublisherDiagnostics<TStatus> diagnostics,
            JobQueueLoop<PublishJob<TResult>> publishLoop, Func<TStatus> createStatus)
        {
            TrackSafe(diagnostics.TrackPublishStarted, publishArgs);
            var stopwatch = Stopwatch.StartNew();

            try
            {
                cancellationToken.ThrowIfCancellationRequested();

                var publishJob =
                    new PublishJob<TResult>(publishArgs, cancellationToken, new TaskCompletionSource<TResult>());
                var tryRemovePublishJob = publishLoop.Enqueue(publishJob);
                TrackSafe(diagnostics.TrackPublishJobEnqueued, publishJob.Args, createStatus());

                var result =
                    await WaitForPublishCompletedOrCancelled(publishJob, tryRemovePublishJob, cancellationToken)
                        .ConfigureAwait(false);
                TrackSafe(diagnostics.TrackPublishCompleted, publishArgs, stopwatch.Elapsed);

                return result;
            }
            catch (OperationCanceledException)
            {
                TrackSafe(diagnostics.TrackPublishCancelled, publishArgs, stopwatch.Elapsed);
                throw;
            }
            catch (Exception ex)
            {
                TrackSafe(diagnostics.TrackPublishFailed, publishArgs, stopwatch.Elapsed, ex);
                throw;
            }
        }

        private static async Task<TResult> WaitForPublishCompletedOrCancelled<TResult>(PublishJob<TResult> publishJob,
            Func<bool> tryCancelJob, CancellationToken cancellationToken)
        {
            var jobTask = publishJob.TaskCompletionSource.Task;
            var firstCompletedTask = await Task.WhenAny(
                Task.Delay(-1, cancellationToken),
                jobTask
            ).ConfigureAwait(false);

            if (firstCompletedTask != jobTask && tryCancelJob())
            {
                return await Task.FromCanceled<TResult>(cancellationToken).ConfigureAwait(false);
            }

            return await jobTask.ConfigureAwait(false);
        }

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