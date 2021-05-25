using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using NUnit.Framework;
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
            var resourceList = new List<TestAutoRecoveryResource>();

            using (var autoRecovery = new AutoRecovery<TestAutoRecoveryResource>(
                () =>
                {
                    var resource = new TestAutoRecoveryResource();
                    resourceList.Add(resource);
                    return resource;
                },
                new[] {new AutoRecoveryComponent(_ => Disposable.Empty).Delegate},
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
            var component2 = new AutoRecoveryComponent(_ => Disposable.Empty);

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

            var resourceList = new List<TestAutoRecoveryResource>();

            using (var autoRecovery = new AutoRecovery<TestAutoRecoveryResource>(
                () =>
                {
                    var resource = new TestAutoRecoveryResource();
                    resourceList.Add(resource);
                    return resource;
                },
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

        [Test]
        public void WhenResourceDisposeThrows()
        {
            using (var autoRecovery = new AutoRecovery<TestAutoRecoveryResource>(
                () => new TestAutoRecoveryResource(),
                new[] {new AutoRecoveryComponent(_ => Disposable.Empty).Delegate},
                _ => TimeSpan.FromMilliseconds(1)))
            {
                throw new NotImplementedException();
            }
        }

        [Test]
        public void WhenDelayFunctionThrows()
        {
            throw new NotImplementedException();
        }

        private class AutoRecoveryComponent
        {
            public AutoRecoveryComponent(Func<IAutoRecoveryResource, IDisposable> implementation = null)
            {
                Delegate = resource =>
                {
                    Interlocked.Increment(ref _callCount);
                    return implementation is null ? new Disposable(() => { }) : implementation(resource);
                };
            }

            private int _callCount;
            public int CallCount => _callCount;

            public Func<IAutoRecoveryResource, IDisposable> Delegate { get; }
        }
    }
}