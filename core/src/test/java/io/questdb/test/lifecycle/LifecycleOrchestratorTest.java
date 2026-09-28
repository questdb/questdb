package io.questdb.test.lifecycle;

import io.questdb.lifecycle.Component;
import io.questdb.lifecycle.LifecycleContext;
import io.questdb.lifecycle.LifecycleOrchestrator;
import io.questdb.lifecycle.LifecycleSnapshot;
import io.questdb.lifecycle.LifecycleStartupException;
import io.questdb.lifecycle.ProgressEvent;
import io.questdb.lifecycle.State;
import io.questdb.std.ObjList;
import io.questdb.test.lifecycle.fakes.BarrierComponent;
import io.questdb.test.lifecycle.fakes.CapturingLog;
import io.questdb.test.lifecycle.fakes.ProbeComponent;
import io.questdb.test.lifecycle.fakes.ThrowingComponent;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class LifecycleOrchestratorTest {

    @Rule
    public Timeout timeout = Timeout.builder().withTimeout(30, TimeUnit.SECONDS).withLookingForStuckThread(true).build();

    @Test
    public void testCancellationExceptionWithoutStopFailsBoot() {
        final CancellationException cancellation = new CancellationException("unexpected cancellation");
        final LifecycleOrchestrator orch = new LifecycleOrchestrator(null, null, null);
        orch.register(new ProbeComponent("a") {
            @Override
            public void start(LifecycleContext ctx) {
                throw cancellation;
            }
        });
        try {
            orch.run();
            Assert.fail("expected LifecycleStartupException");
        } catch (LifecycleStartupException expected) {
            Assert.assertSame(cancellation, expected.getCause());
        } finally {
            orch.close();
        }
    }

    @Test
    public void testCloseAllowsOwnerReentryFromStop() throws Exception {
        final CountDownLatch closeReturned = new CountDownLatch(1);
        final AtomicReference<LifecycleOrchestrator> orchRef = new AtomicReference<>();
        final AtomicReference<Throwable> terminalError = new AtomicReference<>();
        final LifecycleOrchestrator orch = new LifecycleOrchestrator(null, null, null);
        orchRef.set(orch);
        orch.register(new ProbeComponent("a") {
            @Override
            public void stop() {
                orchRef.get().close();
                super.stop();
            }
        });
        orch.run();

        final Thread terminalClose = new Thread(() -> {
            try {
                orch.close();
            } catch (Throwable th) {
                terminalError.set(th);
            } finally {
                closeReturned.countDown();
            }
        });
        terminalClose.setDaemon(true);
        terminalClose.start();

        Assert.assertTrue("close() deadlocked after owner reentry", closeReturned.await(1, TimeUnit.SECONDS));
        terminalClose.join(TimeUnit.SECONDS.toMillis(10));
        Assert.assertNull(terminalError.get());
        Assert.assertTrue(orch.isStopComplete());
    }

    @Test
    public void testCloseRetriesFailedComponentStopWhenLoggingFails() {
        final AtomicInteger stopAttempts = new AtomicInteger();
        final LifecycleOrchestrator orch = new LifecycleOrchestrator(new CapturingLog(true), null, null);
        final ProbeComponent independent = new ProbeComponent("independent");
        final ProbeComponent dependency = new ProbeComponent("a");
        final ObjList<String> hardDeps = new ObjList<>();
        hardDeps.add("a");
        final ProbeComponent component = new ProbeComponent("b", hardDeps, new ObjList<>()) {
            @Override
            public void stop() {
                if (stopAttempts.getAndIncrement() == 0) {
                    throw new IllegalStateException("stop");
                }
                super.stop();
            }
        };
        orch.register(independent);
        orch.register(dependency);
        orch.register(component);
        orch.run();
        orch.close();
        Assert.assertFalse(orch.isStopComplete());
        Assert.assertEquals(State.READY, orch.stateOf("a"));
        Assert.assertEquals(State.STOPPING, orch.stateOf("b"));
        Assert.assertEquals(-1, dependency.getStopSeq());
        Assert.assertEquals(State.STOPPED, orch.stateOf("independent"));
        Assert.assertTrue(independent.getStopSeq() > -1);
        Assert.assertEquals(1, stopAttempts.get());
        orch.close();
        Assert.assertTrue(orch.isStopComplete());
        Assert.assertEquals(State.STOPPED, orch.stateOf("a"));
        Assert.assertEquals(State.STOPPED, orch.stateOf("b"));
        Assert.assertTrue(dependency.getStopSeq() > -1);
        Assert.assertEquals(2, stopAttempts.get());
    }

    @Test
    public void testCloseStopsSafeComponentsAfterPreStopHookAndLoggingFailure() {
        final AtomicInteger hookAttempts = new AtomicInteger();
        final LifecycleOrchestrator orch = new LifecycleOrchestrator(new CapturingLog(true), null, null);
        final ProbeComponent audit = new ProbeComponent(
                "audit",
                listOf("worker-pool-manager"),
                new ObjList<>()
        );
        final ProbeComponent auditConsumer = new ProbeComponent(
                "audit-consumer",
                listOf("audit"),
                new ObjList<>()
        );
        final ProbeComponent auth = new ProbeComponent("auth");
        final ProbeComponent factory = new ProbeComponent("factory");
        final ProbeComponent engine = new ProbeComponent("engine", listOf("factory"), new ObjList<>());
        final ProbeComponent hydration = new ProbeComponent("hydration", listOf("engine"), new ObjList<>());
        final ProbeComponent independent = new ProbeComponent("independent");
        final ProbeComponent workerPoolManager = new ProbeComponent(
                "worker-pool-manager",
                listOf("engine"),
                new ObjList<>()
        );
        final ProbeComponent listener = new ProbeComponent(
                "listener",
                listOf("worker-pool-manager", "auth", "audit"),
                new ObjList<>()
        );
        final ProbeComponent requestHandler = new ProbeComponent(
                "request-handler",
                listOf("listener", "audit"),
                new ObjList<>()
        );
        orch.register(auth);
        orch.register(factory);
        orch.register(engine);
        orch.register(hydration);
        orch.register(independent);
        orch.register(workerPoolManager);
        orch.register(listener);
        orch.register(audit);
        orch.register(auditConsumer);
        orch.register(requestHandler);
        orch.setPreStopHook(() -> {
            if (hookAttempts.getAndIncrement() == 0) {
                throw new IllegalStateException("worker pool halt failed");
            }
        });
        orch.run();

        orch.close();

        Assert.assertFalse(orch.isStopComplete());
        Assert.assertEquals(State.READY, orch.stateOf("audit"));
        Assert.assertEquals(State.READY, orch.stateOf("audit-consumer"));
        Assert.assertEquals(State.READY, orch.stateOf("auth"));
        Assert.assertEquals(State.READY, orch.stateOf("factory"));
        Assert.assertEquals(State.READY, orch.stateOf("engine"));
        Assert.assertEquals(State.STOPPED, orch.stateOf("hydration"));
        Assert.assertEquals(State.STOPPED, orch.stateOf("independent"));
        Assert.assertEquals(State.READY, orch.stateOf("worker-pool-manager"));
        Assert.assertEquals(State.READY, orch.stateOf("listener"));
        Assert.assertEquals(State.READY, orch.stateOf("request-handler"));

        orch.close();

        Assert.assertEquals(2, hookAttempts.get());
        Assert.assertTrue(orch.isStopComplete());
        Assert.assertEquals(State.STOPPED, orch.stateOf("audit"));
        Assert.assertEquals(State.STOPPED, orch.stateOf("audit-consumer"));
        Assert.assertEquals(State.STOPPED, orch.stateOf("auth"));
        Assert.assertEquals(State.STOPPED, orch.stateOf("factory"));
        Assert.assertEquals(State.STOPPED, orch.stateOf("engine"));
        Assert.assertEquals(State.STOPPED, orch.stateOf("worker-pool-manager"));
        Assert.assertEquals(State.STOPPED, orch.stateOf("listener"));
        Assert.assertEquals(State.STOPPED, orch.stateOf("request-handler"));
    }

    @Test
    public void testCloseClaimsOwnershipBeforeStopRequest() {
        final AtomicInteger stopRequests = new AtomicInteger();
        final AtomicReference<LifecycleOrchestrator> orchRef = new AtomicReference<>();
        final LifecycleOrchestrator orch = new LifecycleOrchestrator(null, null, null);
        orchRef.set(orch);
        orch.register(new ProbeComponent("a") {
            @Override
            public void requestStop() {
                if (stopRequests.incrementAndGet() == 1) {
                    orchRef.get().close();
                }
            }
        });
        orch.run();

        orch.close();
        orch.close();

        Assert.assertEquals(1, stopRequests.get());
        Assert.assertTrue(orch.isStopComplete());
    }

    @Test
    public void testCloseDispatchesTerminalComponentStop() {
        final AtomicInteger terminalStopCount = new AtomicInteger();
        final LifecycleOrchestrator orch = new LifecycleOrchestrator(null, null, null);
        orch.register(new ProbeComponent("a") {
            @Override
            public void stop() {
                terminalStopCount.incrementAndGet();
                super.stop();
            }
        });
        orch.run();

        orch.close();

        Assert.assertEquals(1, terminalStopCount.get());
        Assert.assertTrue(orch.isStopComplete());
    }

    @Test
    public void testCloseDoesNotHideConcurrentStartFailure() throws Exception {
        final CountDownLatch startEntered = new CountDownLatch(1);
        final CountDownLatch stopRequested = new CountDownLatch(1);
        final AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        final AtomicReference<Throwable> runFailure = new AtomicReference<>();
        final LifecycleOrchestrator orch = new LifecycleOrchestrator(null, null, null);
        orch.register(new ProbeComponent("a") {
            @Override
            public void requestStop() {
                stopRequested.countDown();
            }

            @Override
            public void start(LifecycleContext ctx) {
                startEntered.countDown();
                try {
                    stopRequested.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                throw new IllegalStateException("start failed");
            }
        });
        final Thread closer = new Thread(() -> {
            try {
                orch.close();
            } catch (Throwable th) {
                closeFailure.set(th);
            }
        });
        final Thread runner = new Thread(() -> {
            try {
                orch.run();
            } catch (Throwable th) {
                runFailure.set(th);
            }
        });
        runner.start();
        try {
            Assert.assertTrue(startEntered.await(10, TimeUnit.SECONDS));
            closer.start();
            runner.join(TimeUnit.SECONDS.toMillis(10));
            closer.join(TimeUnit.SECONDS.toMillis(10));
        } finally {
            stopRequested.countDown();
            runner.join(TimeUnit.SECONDS.toMillis(10));
            if (closer.getState() != Thread.State.NEW) {
                closer.join(TimeUnit.SECONDS.toMillis(10));
            }
            orch.close();
        }
        Assert.assertFalse(runner.isAlive());
        Assert.assertFalse(closer.isAlive());
        Assert.assertNull(closeFailure.get());
        Assert.assertTrue(runFailure.get() instanceof LifecycleStartupException);
        TestUtils.assertContains(runFailure.get().getMessage(), "start failed");
        Assert.assertTrue(orch.isStopComplete());
    }

    @Test
    public void testCloseDrainsInFlightWorkWhenCallerIsInterrupted() throws Exception {
        CountDownLatch awaitEntered = new CountDownLatch(1);
        CountDownLatch drainComplete = new CountDownLatch(1);
        AtomicBoolean hasAwaitCompleted = new AtomicBoolean();
        AtomicBoolean isDrainCompleteAtStop = new AtomicBoolean();
        AtomicReference<Throwable> releaserFailure = new AtomicReference<>();
        LifecycleOrchestrator orch = new LifecycleOrchestrator(null, null, null) {
            @Override
            protected boolean awaitInFlightWork() {
                awaitEntered.countDown();
                try {
                    boolean hasDrained = drainComplete.await(5, TimeUnit.SECONDS);
                    hasAwaitCompleted.set(hasDrained);
                    return hasDrained;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        };
        orch.setPreStopHook(() -> isDrainCompleteAtStop.set(hasAwaitCompleted.get()));
        orch.run();

        Thread releaser = new Thread(() -> {
            try {
                if (!awaitEntered.await(10, TimeUnit.SECONDS)) {
                    throw new AssertionError("close() did not enter the in-flight drain");
                }
                drainComplete.countDown();
            } catch (Throwable th) {
                releaserFailure.set(th);
            }
        }, "lifecycle-drain-releaser");
        releaser.setDaemon(true);
        releaser.start();

        try {
            Thread.currentThread().interrupt();
            orch.close();
            Assert.assertTrue("close() did not restore the caller interrupt flag", Thread.interrupted());
            releaser.join(TimeUnit.SECONDS.toMillis(10));
            Assert.assertFalse("lifecycle drain releaser did not stop", releaser.isAlive());
            if (releaserFailure.get() != null) {
                throw new AssertionError("lifecycle drain releaser failed", releaserFailure.get());
            }
            Assert.assertTrue("close() entered pre-stop before in-flight work drained", isDrainCompleteAtStop.get());
        } finally {
            Thread.interrupted();
            drainComplete.countDown();
            releaser.join(TimeUnit.SECONDS.toMillis(10));
            orch.close();
        }
    }

    @Test
    public void testCloseDrainsInFlightWorkWhenInterruptedDuringAwait() throws Exception {
        CountDownLatch awaitEntered = new CountDownLatch(1);
        CountDownLatch preStopEntered = new CountDownLatch(1);
        CountDownLatch releasePreStop = new CountDownLatch(1);
        CountDownLatch releaseTask = new CountDownLatch(1);
        CountDownLatch taskStarted = new CountDownLatch(1);
        AtomicBoolean hasTaskCompleted = new AtomicBoolean();
        AtomicBoolean isInterruptRestored = new AtomicBoolean();
        AtomicBoolean isPreStopInterrupted = new AtomicBoolean();
        AtomicReference<Throwable> closeFailure = new AtomicReference<>();

        class TestOrchestrator extends LifecycleOrchestrator {
            private TestOrchestrator() {
                super(null, null, null);
            }

            @Override
            protected boolean awaitInFlightWork() {
                awaitEntered.countDown();
                return super.awaitInFlightWork();
            }

            private void submitInFlightWork(Runnable task) {
                executor.execute(task);
            }
        }

        TestOrchestrator orch = new TestOrchestrator();
        orch.setPreStopHook(() -> {
            isPreStopInterrupted.set(Thread.currentThread().isInterrupted());
            preStopEntered.countDown();
            try {
                releasePreStop.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        Thread closeThread = new Thread(() -> {
            try {
                orch.close();
                isInterruptRestored.set(Thread.currentThread().isInterrupted());
            } catch (Throwable th) {
                closeFailure.set(th);
            }
        }, "lifecycle-close-test");

        try {
            orch.run();
            orch.submitInFlightWork(() -> {
                taskStarted.countDown();
                try {
                    releaseTask.await();
                    hasTaskCompleted.set(true);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            Assert.assertTrue("in-flight task did not start", taskStarted.await(5, TimeUnit.SECONDS));
            closeThread.start();

            Assert.assertTrue("close() did not enter the in-flight drain", awaitEntered.await(5, TimeUnit.SECONDS));
            closeThread.interrupt();
            TestUtils.assertEventually(() -> {
                Thread.State state = closeThread.getState();
                Assert.assertTrue(
                        preStopEntered.getCount() == 0
                                || (!closeThread.isInterrupted()
                                && (state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING))
                );
            }, 5);
            Assert.assertEquals("close() entered pre-stop before in-flight work drained", 1, preStopEntered.getCount());

            releaseTask.countDown();
            Assert.assertTrue("close() did not enter pre-stop", preStopEntered.await(5, TimeUnit.SECONDS));
            Assert.assertFalse("pre-stop inherited the drain interrupt", isPreStopInterrupted.get());
            releasePreStop.countDown();
            closeThread.join(TimeUnit.SECONDS.toMillis(10));
            Assert.assertFalse("close thread did not stop", closeThread.isAlive());
            Assert.assertNull("close failed", closeFailure.get());
            Assert.assertTrue("in-flight task did not complete", hasTaskCompleted.get());
            Assert.assertTrue("close() did not restore the caller interrupt flag", isInterruptRestored.get());
        } finally {
            releaseTask.countDown();
            releasePreStop.countDown();
            closeThread.join(TimeUnit.SECONDS.toMillis(10));
            orch.close();
        }
    }

    @Test
    public void testCloseSignalsInFlightStartBeforeWaitingForBootThread() throws Exception {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicInteger stopRequests = new AtomicInteger();
        final LifecycleOrchestrator orch = new LifecycleOrchestrator(null, null, null);
        orch.register(new Component() {
            @Override
            public ObjList<String> hardRequiredDependencies() {
                return new ObjList<>();
            }

            @Override
            public String name() {
                return "a";
            }

            @Override
            public void requestStop() {
                stopRequests.incrementAndGet();
                release.countDown();
            }

            @Override
            public ObjList<String> softDependencies() {
                return new ObjList<>();
            }

            @Override
            public void start(LifecycleContext ctx) {
                entered.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            @Override
            public void stop() {
            }
        });
        final Thread runner = new Thread(orch::run);
        runner.start();
        try {
            Assert.assertTrue(entered.await(10, TimeUnit.SECONDS));
            orch.close();
            Assert.assertEquals(1, stopRequests.get());
        } finally {
            release.countDown();
            runner.join();
            orch.close();
        }
    }

    @Test
    public void testCloseTreatsCancelledStartAsShutdown() throws Exception {
        final CountDownLatch cancel = new CountDownLatch(1);
        final CountDownLatch entered = new CountDownLatch(1);
        final AtomicBoolean isStartExited = new AtomicBoolean();
        final AtomicBoolean isStopAfterStart = new AtomicBoolean();
        final AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        final AtomicReference<Throwable> runFailure = new AtomicReference<>();
        final LifecycleOrchestrator orch = new LifecycleOrchestrator(null, null, null);
        orch.register(new Component() {
            @Override
            public ObjList<String> hardRequiredDependencies() {
                return new ObjList<>();
            }

            @Override
            public String name() {
                return "a";
            }

            @Override
            public void requestStop() {
                cancel.countDown();
            }

            @Override
            public ObjList<String> softDependencies() {
                return new ObjList<>();
            }

            @Override
            public void start(LifecycleContext ctx) {
                entered.countDown();
                try {
                    cancel.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    isStartExited.set(true);
                }
                throw new CancellationException("cancelled");
            }

            @Override
            public void stop() {
                isStopAfterStart.set(isStartExited.get());
            }
        });
        final Thread closer = new Thread(() -> {
            try {
                orch.close();
            } catch (Throwable th) {
                closeFailure.set(th);
            }
        });
        final Thread runner = new Thread(() -> {
            try {
                orch.run();
            } catch (Throwable th) {
                runFailure.set(th);
            }
        });
        runner.start();
        try {
            Assert.assertTrue(entered.await(10, TimeUnit.SECONDS));
            closer.start();
            runner.join(10_000L);
            closer.join(10_000L);
        } finally {
            cancel.countDown();
            runner.join(10_000L);
            if (closer.isAlive()) {
                closer.join(10_000L);
            }
            orch.close();
        }
        Assert.assertFalse(runner.isAlive());
        Assert.assertFalse(closer.isAlive());
        Assert.assertNull(closeFailure.get());
        Assert.assertNull(runFailure.get());
        Assert.assertTrue(isStopAfterStart.get());
        Assert.assertTrue(orch.isStopComplete());
    }

    @Test
    public void testCloseTreatsCancelledStartWithCleanupFailureAsFailure() throws Exception {
        final CountDownLatch cancel = new CountDownLatch(1);
        final CountDownLatch entered = new CountDownLatch(1);
        final AtomicReference<Throwable> runFailure = new AtomicReference<>();
        final IllegalStateException cleanupFailure = new IllegalStateException("cleanup failed");
        final LifecycleOrchestrator orch = new LifecycleOrchestrator(null, null, null);
        orch.register(new ProbeComponent("a") {
            @Override
            public void requestStop() {
                cancel.countDown();
            }

            @Override
            public void start(LifecycleContext ctx) {
                entered.countDown();
                try {
                    cancel.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                final CancellationException cancellation = new CancellationException("cancelled");
                cancellation.addSuppressed(cleanupFailure);
                throw cancellation;
            }
        });
        final Thread runner = new Thread(() -> {
            try {
                orch.run();
            } catch (Throwable th) {
                runFailure.set(th);
            }
        });
        runner.start();
        try {
            Assert.assertTrue(entered.await(10, TimeUnit.SECONDS));
            orch.close();
            runner.join(10_000L);
        } finally {
            cancel.countDown();
            runner.join(10_000L);
            orch.close();
        }
        Assert.assertFalse(runner.isAlive());
        Assert.assertTrue(runFailure.get() instanceof LifecycleStartupException);
        Assert.assertSame(cleanupFailure, runFailure.get().getCause().getSuppressed()[0]);
        Assert.assertTrue(orch.isStopComplete());
    }

    @Test
    public void testCloseWaitsForBootWhenInterruptedDuringAwait() throws Exception {
        final BarrierComponent component = new BarrierComponent("boot-blocker");
        final CountDownLatch preStopEntered = new CountDownLatch(1);
        final AtomicBoolean isInterruptRestored = new AtomicBoolean();
        final AtomicReference<Throwable> bootFailure = new AtomicReference<>();
        final AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        final LifecycleOrchestrator orch = new LifecycleOrchestrator(null, null, null);
        orch.register(component);
        orch.setPreStopHook(preStopEntered::countDown);

        final Thread bootThread = new Thread(() -> {
            try {
                orch.run();
            } catch (Throwable th) {
                bootFailure.set(th);
            }
        }, "lifecycle-boot-test");
        final Thread closeThread = new Thread(() -> {
            try {
                orch.close();
                isInterruptRestored.set(Thread.currentThread().isInterrupted());
            } catch (Throwable th) {
                closeFailure.set(th);
            }
        }, "lifecycle-boot-close-test");

        try {
            bootThread.start();
            Assert.assertTrue("component start did not block", component.awaitEntered(TimeUnit.SECONDS.toMillis(5)));
            closeThread.start();
            TestUtils.assertEventually(() -> {
                final Thread.State state = closeThread.getState();
                Assert.assertTrue(state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING);
            }, 5);

            closeThread.interrupt();
            TestUtils.assertEventually(() -> {
                if (preStopEntered.getCount() == 0) {
                    return;
                }
                Assert.assertFalse("close thread has not consumed the await interrupt", closeThread.isInterrupted());
                final Thread.State state = closeThread.getState();
                Assert.assertTrue(
                        "close thread did not resume the boot await",
                        state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING
                );
            }, 5);
            Assert.assertEquals("close entered pre-stop while boot was active", 1, preStopEntered.getCount());

            component.releaseBarrier();
            bootThread.join(TimeUnit.SECONDS.toMillis(5));
            closeThread.join(TimeUnit.SECONDS.toMillis(5));
            Assert.assertFalse("boot thread did not stop", bootThread.isAlive());
            Assert.assertFalse("close thread did not stop", closeThread.isAlive());
            Assert.assertNull("boot failed", bootFailure.get());
            Assert.assertNull("close failed", closeFailure.get());
            Assert.assertEquals("close did not run pre-stop", 0, preStopEntered.getCount());
            Assert.assertTrue("close did not restore the await interrupt", isInterruptRestored.get());
        } finally {
            component.releaseBarrier();
            bootThread.join(TimeUnit.SECONDS.toMillis(5));
            closeThread.join(TimeUnit.SECONDS.toMillis(5));
            orch.close();
        }
    }

    @Test
    public void testEnvelopeExtraDepsInjection() {
        // Verify polymorphic dispatch through workerPoolManagerExtraHardDeps()-style hook.
        // We model it abstractly here: an "envelope" component whose hardDeps are concatenated from
        // a base list ["base"] + an extra list supplied by an overridden hook. The override is on
        // a subclass; the polymorphic call must invoke the subclass override even though construction
        // happens through the base path.
        class EnvelopeBase {
            ObjList<String> extraHardDeps() {
                return new ObjList<>();
            }
        }
        class EnvelopeSub extends EnvelopeBase {
            @Override
            ObjList<String> extraHardDeps() {
                ObjList<String> deps = new ObjList<>();
                deps.add("ent-pre-services");
                return deps;
            }
        }
        // The base envelope picks up its deps via the hook, polymorphically.
        EnvelopeBase sub = new EnvelopeSub();
        ObjList<String> deps = sub.extraHardDeps();
        Assert.assertEquals(1, deps.size());
        Assert.assertEquals("ent-pre-services", deps.getQuick(0));

        // Now wire a real orchestrator with a probe that uses sub.extraHardDeps()'s output as its hard deps.
        LifecycleOrchestrator orch = newOrchestrator();
        ProbeComponent base = new ProbeComponent("ent-pre-services");
        ProbeComponent envelope = new ProbeComponent("envelope", sub.extraHardDeps(), new ObjList<>());
        orch.register(base);
        orch.register(envelope);
        orch.run();
        // Envelope's start ran AFTER its injected hard dep reached READY.
        Assert.assertTrue("envelope must start after ent-pre-services",
                base.getStartSeq() < envelope.getStartSeq());
        Assert.assertEquals(State.READY, orch.stateOf("envelope"));
        orch.close();
    }

    @Test
    public void testFailedIsTerminal() {
        LifecycleOrchestrator orch = newOrchestrator();
        ProbeComponent p = new ProbeComponent("a");
        orch.register(p);
        LifecycleContext ctx = orch.contextFor("a");
        ctx.publish(State.STARTING);
        ctx.publish(State.FAILED, "boom");
        ctx.publish(State.READY);
        Assert.assertEquals(State.FAILED, orch.stateOf("a"));
        orch.close();
    }

    @Test
    public void testFailureCascadeHardDeps() {
        LifecycleOrchestrator orch = newOrchestrator();
        try {
            ThrowingComponent a = new ThrowingComponent("a", () -> new RuntimeException("a-boom"));
            ProbeComponent b = new ProbeComponent("b", listOf("a"), new ObjList<>());
            ProbeComponent c = new ProbeComponent("c", listOf("b"), new ObjList<>());
            ProbeComponent d = new ProbeComponent("d", new ObjList<>(), listOf("c"));   // soft dep
            orch.register(a);
            orch.register(b);
            orch.register(c);
            orch.register(d);
            try {
                orch.run();
                Assert.fail("expected LifecycleStartupException");
            } catch (LifecycleStartupException expected) {
                Assert.assertEquals(State.FAILED, orch.stateOf("a"));
                Assert.assertEquals(State.FAILED, orch.stateOf("b"));
                Assert.assertEquals(State.FAILED, orch.stateOf("c"));
                // Soft dependent NOT auto-cascaded -- observed events instead.
                Assert.assertNotEquals(State.FAILED, orch.stateOf("d"));
                Assert.assertTrue(a.isStopped());
                Assert.assertEquals(-1, b.getStartSeq());
                Assert.assertEquals(-1, b.getStopSeq());
                Assert.assertEquals(-1, c.getStartSeq());
                Assert.assertEquals(-1, c.getStopSeq());
            }
        } finally {
            orch.close();
        }
    }

    @Test
    public void testInFlightStartNotInterrupted() throws Exception {
        // The runner thread blocks inside BarrierComponent.start() until releaseBarrier()
        // is called. If the STARTING-state assertion below were to fail (or any unexpected throw),
        // the runner would leak until JUnit's 30s @Rule Timeout fires. Wrap in try/finally so
        // releaseBarrier() + runner.join() + orch.close() always run.
        // The entered latch flips before start()'s blocking await, so a successful awaitEntered()
        // guarantees the runner has actually reached the barrier and the orchestrator has
        // published STARTING.
        LifecycleOrchestrator orch = newOrchestrator();
        BarrierComponent x = new BarrierComponent("x");
        orch.register(x);
        Thread runner = new Thread(() -> {
            try {
                orch.run();
            } catch (LifecycleStartupException ignore) {
            }
        });
        runner.start();
        try {
            Assert.assertTrue("BarrierComponent.start() did not enter within 10s -- runner did not schedule",
                    x.awaitEntered(10_000L));
            Assert.assertEquals(State.STARTING, orch.stateOf("x"));
        } finally {
            x.releaseBarrier();
            runner.join();
            orch.close();
        }
        Assert.assertTrue(x.isStarted());
    }

    @Test
    public void testParallelStartsIndependentComponents() {
        LifecycleOrchestrator orch = newOrchestrator();
        ProbeComponent a = new ProbeComponent("a");
        ProbeComponent b = new ProbeComponent("b", listOf("a"), new ObjList<>());
        ProbeComponent c = new ProbeComponent("c", listOf("a"), new ObjList<>());
        ProbeComponent d = new ProbeComponent("d", listOf("b", "c"), new ObjList<>());
        orch.register(a);
        orch.register(b);
        orch.register(c);
        orch.register(d);
        orch.run();
        Assert.assertEquals(State.READY, orch.stateOf("a"));
        Assert.assertEquals(State.READY, orch.stateOf("b"));
        Assert.assertEquals(State.READY, orch.stateOf("c"));
        Assert.assertEquals(State.READY, orch.stateOf("d"));
        orch.close();
    }

    @Test
    public void testProgressLatestOverwrites() {
        LifecycleOrchestrator orch = newOrchestrator();
        ProbeComponent p = new ProbeComponent("a");
        orch.register(p);
        LifecycleContext ctx = orch.contextFor("a");
        ctx.progress(new TestProgressEvent("first"));
        ctx.progress(new TestProgressEvent("second"));
        LifecycleSnapshot snap = orch.snapshot();
        LifecycleSnapshot.ComponentSnapshot cs = snap.components().getQuick(0);
        Assert.assertNotNull(cs.latestProgress());
        Assert.assertTrue(cs.latestProgress() instanceof TestProgressEvent);
        Assert.assertEquals("second", ((TestProgressEvent) cs.latestProgress()).tag());
        orch.close();
    }

    @Test
    public void testPropagatesDependencyState() {
        LifecycleOrchestrator orch = newOrchestrator();
        ProbeComponent a = new ProbeComponent("a");
        ProbeComponent b = new ProbeComponent("b", listOf("a"), new ObjList<>());
        orch.register(a);
        orch.register(b);
        orch.run();
        boolean sawAReady = false;
        for (ProbeComponent.Event e : b.events) {
            if ("a".equals(e.depName) && e.current == State.READY) sawAReady = true;
        }
        Assert.assertTrue("b did not observe a -> READY transition", sawAReady);
        orch.close();
    }

    @Test
    public void testRejectsDuplicateName() {
        LifecycleOrchestrator orch = newOrchestrator();
        ProbeComponent a1 = new ProbeComponent("a");
        ProbeComponent a2 = new ProbeComponent("a");
        orch.register(a1);
        try {
            orch.register(a2);
            Assert.fail("expected LifecycleStartupException for duplicate name");
        } catch (LifecycleStartupException expected) {
            TestUtils.assertContains(expected.getMessage(), "duplicate component name");
        }
        orch.close();
    }

    @Test
    public void testRejectsUnknownDependency() {
        LifecycleOrchestrator orch = newOrchestrator();
        try {
            ProbeComponent a = new ProbeComponent("a", listOf("nonexistent"), new ObjList<>());
            orch.register(a);
            try {
                orch.run();
                Assert.fail("expected LifecycleStartupException for unknown dep");
            } catch (LifecycleStartupException expected) {
                TestUtils.assertContains(expected.getMessage(), "unknown dependency");
            }
        } finally {
            orch.close();
        }
    }

    @Test
    public void testRequestStopBeforeRunPreventsStart() {
        final LifecycleOrchestrator orch = newOrchestrator();
        final ProbeComponent component = new ProbeComponent("a");
        orch.register(component);
        orch.requestStop();
        orch.run();
        Assert.assertEquals(-1, component.getStartSeq());
        Assert.assertEquals(State.INIT, orch.stateOf("a"));
        orch.close();
        Assert.assertTrue(orch.isStopComplete());
    }

    @Test
    public void testRequestStopContinuesAfterCallbackFailure() {
        final AtomicInteger stopRequests = new AtomicInteger();
        final LifecycleOrchestrator orch = newOrchestrator();
        orch.register(new ProbeComponent("a") {
            @Override
            public void requestStop() {
                throw new IllegalStateException("request");
            }
        });
        orch.register(new ProbeComponent("b") {
            @Override
            public void requestStop() {
                stopRequests.incrementAndGet();
            }
        });
        orch.run();
        orch.requestStop();
        Assert.assertEquals(1, stopRequests.get());
        orch.close();
        Assert.assertTrue(orch.isStopComplete());
    }

    @Test
    public void testReverseTopologicalShutdown() {
        LifecycleOrchestrator orch = newOrchestrator();
        ProbeComponent a = new ProbeComponent("a");
        ProbeComponent b = new ProbeComponent("b", listOf("a"), new ObjList<>());
        ProbeComponent c = new ProbeComponent("c", listOf("b"), new ObjList<>());
        orch.register(a);
        orch.register(b);
        orch.register(c);
        orch.run();
        orch.close();
        Assert.assertTrue(c.getStopSeq() < b.getStopSeq());
        Assert.assertTrue(b.getStopSeq() < a.getStopSeq());
    }

    @Test
    public void testRunIsRetryableAfterValidationFailure() {
        // A validation failure (unknown dep) must NOT lock the orchestrator into a
        // permanent "run may only be called once" state. After fixing the registry, run()
        // should proceed normally on the retry.
        LifecycleOrchestrator orch = newOrchestrator();
        try {
            ProbeComponent broken = new ProbeComponent("broken", listOf("nonexistent"), new ObjList<>());
            orch.register(broken);
            try {
                orch.run();
                Assert.fail("expected LifecycleStartupException for unknown dep");
            } catch (LifecycleStartupException expected) {
                TestUtils.assertContains(expected.getMessage(), "unknown dependency");
            }
            // Register the missing dep and retry.
            orch.register(new ProbeComponent("nonexistent"));
            orch.run();   // must NOT throw IllegalStateException("may only be called once")
            Assert.assertEquals(State.READY, orch.stateOf("broken"));
            Assert.assertEquals(State.READY, orch.stateOf("nonexistent"));
        } finally {
            orch.close();
        }
    }

    @Test
    public void testRunReportsFailurePublishedBeforeConcurrentClose() throws Exception {
        final CountDownLatch failurePublished = new CountDownLatch(1);
        final CountDownLatch releaseStart = new CountDownLatch(1);
        final CountDownLatch stopRequested = new CountDownLatch(1);
        final AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        final AtomicReference<Throwable> runFailure = new AtomicReference<>();
        final LifecycleOrchestrator orch = new LifecycleOrchestrator(null, null, null);
        orch.register(new ProbeComponent("a") {
            @Override
            public void requestStop() {
                stopRequested.countDown();
            }

            @Override
            public void start(LifecycleContext ctx) {
                ctx.publish(State.FAILED, "boom");
                failurePublished.countDown();
                try {
                    releaseStart.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        final Thread closer = new Thread(() -> {
            try {
                orch.close();
            } catch (Throwable th) {
                closeFailure.set(th);
            }
        });
        final Thread runner = new Thread(() -> {
            try {
                orch.run();
            } catch (Throwable th) {
                runFailure.set(th);
            }
        });
        runner.start();
        try {
            Assert.assertTrue(failurePublished.await(10, TimeUnit.SECONDS));
            closer.start();
            Assert.assertTrue(stopRequested.await(10, TimeUnit.SECONDS));
            releaseStart.countDown();
            runner.join(TimeUnit.SECONDS.toMillis(10));
            closer.join(TimeUnit.SECONDS.toMillis(10));
        } finally {
            releaseStart.countDown();
            runner.join(TimeUnit.SECONDS.toMillis(10));
            if (closer.getState() != Thread.State.NEW) {
                closer.join(TimeUnit.SECONDS.toMillis(10));
            }
            orch.close();
        }
        Assert.assertFalse(runner.isAlive());
        Assert.assertFalse(closer.isAlive());
        Assert.assertNull(closeFailure.get());
        Assert.assertTrue(runFailure.get() instanceof LifecycleStartupException);
        Assert.assertTrue(orch.isStopComplete());
    }

    @Test
    public void testFailedComponentStopsOnceAcrossCloseRetries() {
        final AtomicInteger failedStopCount = new AtomicInteger();
        final AtomicInteger flakyStopAttempts = new AtomicInteger();
        final LifecycleOrchestrator orch = new LifecycleOrchestrator(null, null, null);
        final ProbeComponent failed = new ProbeComponent("a") {
            @Override
            public void stop() {
                failedStopCount.incrementAndGet();
                super.stop();
            }
        };
        orch.register(failed);
        orch.register(new ProbeComponent("b") {
            @Override
            public void stop() {
                if (flakyStopAttempts.incrementAndGet() == 1) {
                    throw new IllegalStateException("first stop attempt fails");
                }
                super.stop();
            }
        });
        orch.run();
        failed.advanceTo(State.FAILED);

        orch.close();
        Assert.assertEquals(1, failedStopCount.get());
        Assert.assertEquals(1, flakyStopAttempts.get());
        Assert.assertFalse(orch.isStopComplete());

        orch.close();
        Assert.assertEquals(1, failedStopCount.get());
        Assert.assertEquals(2, flakyStopAttempts.get());
        Assert.assertTrue(orch.isStopComplete());
    }

    @Test
    public void testSnapshotEventuallyConsistent() {
        LifecycleOrchestrator orch = newOrchestrator();
        ProbeComponent a = new ProbeComponent("a");
        ProbeComponent b = new ProbeComponent("b");
        orch.register(a);
        orch.register(b);
        orch.run();
        LifecycleSnapshot snap = orch.snapshot();
        Assert.assertEquals(2, snap.components().size());
        Assert.assertTrue(snap.capturedAtMicros() > 0);
        for (int i = 0, n = snap.components().size(); i < n; i++) {
            LifecycleSnapshot.ComponentSnapshot cs = snap.components().getQuick(i);
            Assert.assertEquals(State.READY, cs.state());
            Assert.assertTrue(cs.lastTransitionMicros() > 0);
        }
        orch.close();
    }

    @Test
    public void testStartsInTopologicalOrder() {
        LifecycleOrchestrator orch = newOrchestrator();
        ProbeComponent a = new ProbeComponent("a");
        ProbeComponent b = new ProbeComponent("b", listOf("a"), new ObjList<>());
        ProbeComponent c = new ProbeComponent("c", listOf("b"), new ObjList<>());
        ProbeComponent d = new ProbeComponent("d", listOf("c"), new ObjList<>());
        orch.register(a);
        orch.register(b);
        orch.register(c);
        orch.register(d);
        orch.run();
        Assert.assertTrue("a must start before b", a.getStartSeq() < b.getStartSeq());
        Assert.assertTrue("b must start before c", b.getStartSeq() < c.getStartSeq());
        Assert.assertTrue("c must start before d", c.getStartSeq() < d.getStartSeq());
        orch.close();
    }

    @Test
    public void testStoppedOnlyFromStopping() {
        LifecycleOrchestrator orch = newOrchestrator();
        ProbeComponent p = new ProbeComponent("a");
        orch.register(p);
        LifecycleContext ctx = orch.contextFor("a");
        ctx.publish(State.STARTING);
        ctx.publish(State.READY);
        ctx.publish(State.STOPPED);   // direct READY -> STOPPED rejected
        Assert.assertEquals(State.READY, orch.stateOf("a"));
        orch.close();
    }

    @Test
    public void testTransitionLogShape() {
        CapturingLog capture = new CapturingLog();
        LifecycleOrchestrator orch = new LifecycleOrchestrator(capture, null, null);
        ProbeComponent a = new ProbeComponent("a");
        orch.register(a);
        orch.run();
        String text = capture.sink.toString();
        TestUtils.assertContains(text, "component=a");
        TestUtils.assertContains(text, " from=INIT");
        TestUtils.assertContains(text, " to=STARTING");
        TestUtils.assertContains(text, " ts=");
        TestUtils.assertContains(text, " since=");
        // FAILED transition includes reason.
        capture.sink.clear();
        LifecycleOrchestrator orch2 = new LifecycleOrchestrator(capture, null, null);
        ThrowingComponent t = new ThrowingComponent("t", () -> new RuntimeException("kaboom"));
        orch2.register(t);
        try {
            orch2.run();
        } catch (LifecycleStartupException ignore) {
        }
        TestUtils.assertContains(capture.sink.toString(), "to=FAILED");
        TestUtils.assertContains(capture.sink.toString(), "reason=\"");
        orch.close();
        orch2.close();
    }

    @Test
    public void testValidateBeforeRunningFlag() {
        // Orchestrator validates the DAG BEFORE flipping running=true.
        // A cycle in the registered components must throw LifecycleStartupException
        // from run(). After that throw, register() is closed off (single-shot lifecycle),
        // and close() must run cleanly without NPE on reverseTopoOrder.
        LifecycleOrchestrator orch = new LifecycleOrchestrator(null, null, null);
        ProbeComponent a = new ProbeComponent("a", listOf("b"), new ObjList<>());
        ProbeComponent b = new ProbeComponent("b", listOf("a"), new ObjList<>());
        orch.register(a);
        orch.register(b);
        try {
            orch.run();
            Assert.fail("expected LifecycleStartupException for cycle");
        } catch (LifecycleStartupException expected) {
            TestUtils.assertContains(expected.getMessage(), "dependency cycle detected");
        }
        // No component was ever asked to start.
        Assert.assertEquals(-1, a.getStartSeq());
        Assert.assertEquals(-1, b.getStartSeq());
        // close() must be defensive against null reverseTopoOrder -- must NOT throw NPE.
        orch.close();

        // A fresh orchestrator with a valid DAG works after the failed one was closed.
        LifecycleOrchestrator fresh = new LifecycleOrchestrator(null, null, null);
        ProbeComponent c = new ProbeComponent("c");
        fresh.register(c);
        fresh.run();
        Assert.assertEquals(State.READY, fresh.stateOf("c"));
        fresh.close();
    }

    @Test
    public void testValidatesDagAcyclic() {
        LifecycleOrchestrator orch = newOrchestrator();
        try {
            ProbeComponent a = new ProbeComponent("a", listOf("b"), new ObjList<>());
            ProbeComponent b = new ProbeComponent("b", listOf("a"), new ObjList<>());
            orch.register(a);
            orch.register(b);
            try {
                orch.run();
                Assert.fail("expected LifecycleStartupException for cycle");
            } catch (LifecycleStartupException expected) {
                TestUtils.assertContains(expected.getMessage(), "dependency cycle detected");
            }
        } finally {
            orch.close();
        }
    }

    @Test
    public void testWpmTwoPhaseDegradedToReady() {
        // Simulate WPM two-phase: register a "wpm" probe + a downstream "service".
        // wpm.start(ctx) publishes DEGRADED then registers onStableBelow("wpm", ...) callback
        // that publishes READY when the downstream is stable.
        LifecycleOrchestrator orch = newOrchestrator();
        AtomicReference<LifecycleContext> wpmCtxRef = new AtomicReference<>();
        ProbeComponent wpm = new ProbeComponent("wpm") {
            @Override
            public void start(LifecycleContext ctx) {
                super.start(ctx);
                wpmCtxRef.set(ctx);
                ctx.publish(State.DEGRADED);
                ctx.onStableBelow("wpm", () -> ctx.publish(State.READY));
            }
        };
        ProbeComponent service = new ProbeComponent("service", listOf("wpm"), new ObjList<>());
        orch.register(wpm);
        orch.register(service);
        orch.run();
        Assert.assertEquals(State.READY, orch.stateOf("wpm"));
        Assert.assertEquals(State.READY, orch.stateOf("service"));
        orch.close();
    }

    private static ObjList<String> listOf(String... names) {
        ObjList<String> out = new ObjList<>();
        for (String n : names) out.add(n);
        return out;
    }

    private static LifecycleOrchestrator newOrchestrator() {
        return new LifecycleOrchestrator(null, null, null);
    }

    record TestProgressEvent(String tag) implements ProgressEvent.TestOnly {
    }
}
