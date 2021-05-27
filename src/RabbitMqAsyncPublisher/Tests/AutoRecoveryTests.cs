using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using NUnit.Framework;
using RabbitMQ.Client;
using RabbitMqAsyncPublisher;
using Shouldly;

namespace Tests
{
    [TestFixture]
    public class AutoRecoveryTests
    {
        [Test]
        public void ShouldStartOnlyOnce()
        {
            const int taskCount = 10_000;
            TestUtils.ConfigureThreadPool(1000);

            var testComponent = new AutoRecoveryComponent();

            var diagnostics = Substitute.For<IAutoRecoveryDiagnostics>();

            var createResource = Substitute.For<Func<TestAutoRecoveryResource>>();
            createResource().Returns(_ => new TestAutoRecoveryResource());

            using (var autoRecovery = new AutoRecovery<TestAutoRecoveryResource>(
                createResource,
                new[] {testComponent.Delegate},
                _ => TimeSpan.FromMilliseconds(1),
                diagnostics))
            {
                var tasks = Enumerable
                    .Range(1, taskCount)
                    .Select(taskNumber => Task.Run(() =>
                    {
                        try
                        {
                            autoRecovery.Start();
                        }
                        catch (Exception ex)
                        {
                            return ex;
                        }

                        return null;
                    }))
                    .ToArray();

                Task.WaitAll(tasks);

                tasks.Length.ShouldBe(taskCount);
                tasks.Count(x => x.Result == null).ShouldBe(1);
                testComponent.CallCount.ShouldBe(1);
            }

            createResource.ReceivedCalls().Count().ShouldBe(1);
            diagnostics.ReceivedWithAnyArgs(1).TrackCreateResourceAttemptStarted(default, default);
            diagnostics.ReceivedWithAnyArgs(1).TrackCreateResourceAttemptSucceeded(default, default, default);
            diagnostics.ReceivedWithAnyArgs(1).TrackCreateResourceSucceeded(default, default);
        }

        [Test]
        public void ShouldRecreateResourceOnShutdown()
        {
            var diagnostics = Substitute.For<IAutoRecoveryDiagnostics>();
            var resourceList = new TestAutoRecoveryResourceList(() => new TestAutoRecoveryResource());

            using (var autoRecovery = new AutoRecovery<TestAutoRecoveryResource>(
                resourceList.CreateItem,
                new[] {new AutoRecoveryComponent().Delegate},
                _ => TimeSpan.FromMilliseconds(1),
                diagnostics))
            {
                autoRecovery.Start();

                diagnostics.Received(1).TrackCreateResourceSucceeded(Arg.Any<string>(), Arg.Any<TimeSpan>());
                resourceList.Count.ShouldBe(1);

                resourceList.Single().FireShutdown();
                TestUtils.WaitFor(() => resourceList.Count == 2).ShouldBe(true);
                diagnostics.Received(2).TrackCreateResourceSucceeded(Arg.Any<string>(), Arg.Any<TimeSpan>());

                resourceList.Last().FireShutdown();
                TestUtils.WaitFor(() => resourceList.Count == 3).ShouldBe(true);
                diagnostics.Received(3).TrackCreateResourceSucceeded(Arg.Any<string>(), Arg.Any<TimeSpan>());
            }
        }

        [Test]
        public void ShouldRetryWithoutThrowingWhenUnableToCreateResource()
        {
            var diagnostics = Substitute.For<IAutoRecoveryDiagnostics>();

            var exception = new Exception("Test exception");
            var createResource = Substitute.For<Func<TestAutoRecoveryResource>>();
            createResource().ThrowsForAnyArgs(exception);

            using (var autoRecovery = new AutoRecovery<TestAutoRecoveryResource>(
                createResource,
                new Func<TestAutoRecoveryResource, IDisposable>[] {new AutoRecoveryComponent().Delegate},
                _ => TimeSpan.FromMilliseconds(1),
                diagnostics))
            {
                autoRecovery.Start();
                Thread.Sleep(300);
            }

            diagnostics.ReceivedWithAnyArgs().TrackCreateResourceAttemptStarted(default, default);

            var attemptStartedCalls = diagnostics.ReceivedCalls()
                .Where(x => x.GetMethodInfo().Name == nameof(diagnostics.TrackCreateResourceAttemptStarted))
                .ToArray();
            attemptStartedCalls.Length.ShouldBeGreaterThan(1);

            diagnostics.Received()
                .TrackCreateResourceAttemptFailed(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<TimeSpan>(), exception);

            var failedDiagnosticsCalls = diagnostics.ReceivedCalls()
                .Where(x => x.GetMethodInfo().Name == nameof(diagnostics.TrackCreateResourceAttemptFailed))
                .ToArray();
            failedDiagnosticsCalls.Length.ShouldBeGreaterThan(1);

            createResource.ReceivedCalls().Count().ShouldBeGreaterThan(1);
        }

        /// <summary>
        /// Auto-recovery should create as many components as possible,
        /// logging failed ones. 
        /// </summary>
        [Test]
        public void ShouldNotThrowWhenUnableToCreateComponentFromResource()
        {
            var component1 = new AutoRecoveryComponent(_ => throw new Exception("Test component exception"));
            var component2 = new AutoRecoveryComponent();

            var diagnostics = Substitute.For<IAutoRecoveryDiagnostics>();

            using (var autoRecovery = new AutoRecovery<TestAutoRecoveryResource>(
                () => new TestAutoRecoveryResource(),
                new[] {component1.Delegate, component2.Delegate},
                _ => TimeSpan.FromMilliseconds(1),
                diagnostics))
            {
                autoRecovery.Start();

                component1.CallCount.ShouldBe(1);
                component2.CallCount.ShouldBe(1);
            }

            diagnostics.Received(1).TrackCreateComponentsCompleted(Arg.Any<string>(), 1, 2, Arg.Any<TimeSpan>());
            diagnostics.Received(1).TrackUnexpectedException(Arg.Any<string>(),
                Arg.Is<Exception>(ex => ex.Message == "Test component exception"));
        }

        /// <summary>
        /// Should dispose as many components as possible, logging failed disposals
        /// </summary>
        [Test]
        public void ShouldNotThrowWhenComponentDisposeThrows()
        {
            // 3 components with 2nd failing so that we don't depend on direct or reverse dispose order.
            var dispose1 = Substitute.For<IDisposable>();
            var component1 = new AutoRecoveryComponent(_ => dispose1);
            var dispose2 = Substitute.For<IDisposable>();
            dispose2.When(x => x.Dispose()).Throw(c => new Exception("Test dispose exception"));
            var component2 = new AutoRecoveryComponent(_ => dispose2);
            var dispose3 = Substitute.For<IDisposable>();
            var component3 = new AutoRecoveryComponent(_ => dispose3);

            var diagnostics = Substitute.For<IAutoRecoveryDiagnostics>();

            var resourceList = new TestAutoRecoveryResourceList(() => new TestAutoRecoveryResource());

            using (var autoRecovery = new AutoRecovery<TestAutoRecoveryResource>(
                resourceList.CreateItem,
                new[] {component1.Delegate, component2.Delegate, component3.Delegate},
                _ => TimeSpan.FromMilliseconds(1),
                diagnostics))
            {
                autoRecovery.Start();

                diagnostics.Received(1).TrackCreateComponentsCompleted(Arg.Any<string>(), 3, 3, Arg.Any<TimeSpan>());

                resourceList.Count.ShouldBe(1);
                resourceList.Single().FireShutdown();

                TestUtils.WaitFor(() => resourceList.Count == 2).ShouldBe(true);

                dispose1.Received(1).Dispose();
                dispose2.Received(1).Dispose();
                dispose3.Received(1).Dispose();
            }

            diagnostics.Received(2).TrackCreateResourceSucceeded(Arg.Any<string>(), Arg.Any<TimeSpan>());

            diagnostics.Received(1).TrackDisposeStarted();
            diagnostics.Received(1).TrackDisposeCompleted(Arg.Any<TimeSpan>());
        }

        /// <summary>
        /// When resource fires <see cref="IAutoRecoveryResource.Shutdown"/>,
        /// but its <see cref="IAutoRecoveryResource.Dispose"/> throws,
        /// it should not corrupt auto-recovery loop.
        /// Failed disposals should be logged via diagnostics
        /// </summary>
        [Test]
        public void ShouldNotThrowWhenResourceDisposeThrows()
        {
            var diagnostics = Substitute.For<IAutoRecoveryDiagnostics>();

            var dispose = Substitute.For<Action>();
            dispose.When(x => x()).Throw(_ => new Exception("Test resource dispose exception"));

            var resourceList = new TestAutoRecoveryResourceList(() => new TestAutoRecoveryResource(dispose));

            using (var autoRecovery = new AutoRecovery<TestAutoRecoveryResource>(
                resourceList.CreateItem,
                new[] {new AutoRecoveryComponent().Delegate},
                _ => TimeSpan.FromMilliseconds(1),
                diagnostics))
            {
                autoRecovery.Start();

                resourceList.Count.ShouldBe(1);
                
                CheckResourceDispose(0);
                resourceList.Single().FireShutdown();
                
                TestUtils.WaitFor(() => resourceList.Count == 2)
                    .ShouldBe(true, "Should create 2nd resource after first one is disposed");
                
                CheckResourceDispose(1);
            }

            dispose.Received(2)();
            diagnostics.Received(1).TrackDisposeCompleted(Arg.Any<TimeSpan>());

            void CheckResourceDispose(int expectedCount)
            {
                dispose.Received(expectedCount)();
                diagnostics.Received(expectedCount).TrackResourceClosed(Arg.Any<string>(), Arg.Any<ShutdownEventArgs>());
                diagnostics.Received(expectedCount).TrackUnexpectedException(Arg.Any<string>(),
                    Arg.Is<Exception>(x => x.Message == "Test resource dispose exception"));
            }
        }

        /// <summary>
        /// We assume that resource creation delay function (called when resource creation fails) should not throw exceptions.
        /// If that happens, auto-recovery loop is not started.
        /// However, because we keep the auto-recovery exception-free,
        /// it should not throw, but should just track such unexpected error via diagnostics. 
        /// </summary>
        [Test]
        public void ShouldNotStartLoopWhenDelayFunctionThrows()
        {
            var diagnostics = Substitute.For<IAutoRecoveryDiagnostics>();

            var createResourceCallCount = 0;
            var resourceList = new TestAutoRecoveryResourceList(() =>
            {
                if (Interlocked.Increment(ref createResourceCallCount) < 3)
                {
                    throw new Exception("Test create resource exception");
                }
                
                return new TestAutoRecoveryResource();
            });

            var delayFn = Substitute.For<Func<int, TimeSpan>>();
            delayFn(Arg.Any<int>()).Throws(_ => new Exception("Test delay exception"));

            var component = new AutoRecoveryComponent();

            using (var autoRecovery = new AutoRecovery<TestAutoRecoveryResource>(
                resourceList.CreateItem,
                new[] {component.Delegate},
                delayFn,
                diagnostics))
            {
                delayFn.ReceivedWithAnyArgs(0)(default);

                autoRecovery.Start();

                TestUtils
                    .WaitFor(() => resourceList.Count > 0, timeout: TimeSpan.FromMilliseconds(300))
                    .ShouldBe(false, "Should not be able to create any resources");

                delayFn.ReceivedWithAnyArgs(1)(default);
            }

            diagnostics.Received(1).TrackUnexpectedException("Unable to start create resource loop.",
                Arg.Is<Exception>(ex => ex.Message == "Test delay exception"));
        }

        private class AutoRecoveryComponent
        {
            public AutoRecoveryComponent(Func<IAutoRecoveryResource, IDisposable> implementation = null)
            {
                Delegate = resource =>
                {
                    Interlocked.Increment(ref _callCount);
                    return implementation is null ? Disposable.Empty : implementation(resource);
                };
            }

            private int _callCount;
            public int CallCount => _callCount;

            public Func<IAutoRecoveryResource, IDisposable> Delegate { get; }
        }
    }
}