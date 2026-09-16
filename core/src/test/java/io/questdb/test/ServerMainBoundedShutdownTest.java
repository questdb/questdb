/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test;

import io.questdb.Bootstrap;
import io.questdb.PropBootstrapConfiguration;
import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.wal.QdbrWalLocker;
import io.questdb.cutlass.Services;
import io.questdb.cutlass.http.HttpServer;
import io.questdb.cutlass.http.HttpServerConfiguration;
import io.questdb.lifecycle.Component;
import io.questdb.lifecycle.LifecycleContext;
import io.questdb.lifecycle.LifecycleOrchestrator;
import io.questdb.lifecycle.State;
import io.questdb.mp.Job;
import io.questdb.mp.Job.WorkerContext;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

public class ServerMainBoundedShutdownTest extends AbstractBootstrapTest {

    private static final ObjList<String> NO_DEPENDENCIES = new ObjList<>();

    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(30, TimeUnit.SECONDS)
            .withLookingForStuckThread(true)
            .build();

    @Override
    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration(
                HTTP_PORT,
                0,
                PG_PORT,
                ILP_PORT,
                root,
                PropertyKey.HTTP_ENABLED + "=false",
                PropertyKey.HTTP_MIN_ENABLED + "=true",
                PropertyKey.HTTP_MIN_WORKER_COUNT + "=1",
                PropertyKey.PG_ENABLED + "=false",
                PropertyKey.LINE_TCP_ENABLED + "=false",
                PropertyKey.LINE_UDP_ENABLED + "=false"
        ));
        dbPath.parent().$();
    }

    @Test
    public void testLifecycleBoundedCloseWaitsForUnboundedOwner() throws Exception {
        final CountDownLatch releaseTerminalStop = new CountDownLatch(1);
        final CountDownLatch terminalStopEntered = new CountDownLatch(1);
        final CountDownLatch boundedReturned = new CountDownLatch(1);
        final AtomicInteger boundedStopCalls = new AtomicInteger();
        final AtomicInteger terminalStopCalls = new AtomicInteger();
        final AtomicReference<Throwable> boundedFailure = new AtomicReference<>();
        final AtomicReference<Throwable> terminalFailure = new AtomicReference<>();
        final LifecycleOrchestrator orchestrator = new LifecycleOrchestrator(null, null, null);
        orchestrator.register(new TestComponent("component") {
            @Override
            public void stop() {
                terminalStopCalls.incrementAndGet();
                terminalStopEntered.countDown();
                awaitUninterruptibly(releaseTerminalStop);
            }

            @Override
            public void stop(long deadlineNanos) {
                boundedStopCalls.incrementAndGet();
            }
        });
        orchestrator.run();

        final Thread terminalCloser = new Thread(() -> {
            try {
                orchestrator.close();
            } catch (Throwable th) {
                terminalFailure.set(th);
            }
        }, "lifecycle-terminal-closer");
        final Thread boundedCloser = new Thread(() -> {
            try {
                orchestrator.closeBy(System.nanoTime() + TimeUnit.SECONDS.toNanos(20));
            } catch (Throwable th) {
                boundedFailure.set(th);
            } finally {
                boundedReturned.countDown();
            }
        }, "lifecycle-bounded-closer");
        try {
            terminalCloser.start();
            Assert.assertTrue("terminal component stop was not entered", terminalStopEntered.await(5, TimeUnit.SECONDS));
            boundedCloser.start();
            assertWaitingForCloseOwner(boundedCloser, boundedReturned, boundedStopCalls);
        } finally {
            releaseTerminalStop.countDown();
            join(terminalCloser);
            join(boundedCloser);
            orchestrator.close();
        }
        Assert.assertNull(terminalFailure.get());
        Assert.assertNull(boundedFailure.get());
        Assert.assertEquals("component stop must have one terminal owner", 1, terminalStopCalls.get());
        Assert.assertEquals("bounded close must not enter component teardown concurrently", 0, boundedStopCalls.get());
        Assert.assertTrue(orchestrator.isCloseComplete());
    }

    @Test
    public void testLifecycleUnboundedCloseFinishesAfterBoundedTimeout() {
        final AtomicBoolean refuseBoundedDrain = new AtomicBoolean(true);
        final LifecycleOrchestrator orchestrator = new LifecycleOrchestrator(null, null, null) {
            @Override
            protected boolean awaitInFlightWork() {
                return true;
            }

            @Override
            protected boolean awaitInFlightWork(long deadlineNanos) {
                return !refuseBoundedDrain.get();
            }
        };
        try {
            orchestrator.closeBy(System.nanoTime() + TimeUnit.SECONDS.toNanos(5));
            Assert.assertFalse(orchestrator.isCloseComplete());
            orchestrator.close();
            Assert.assertTrue("terminal close did not finish the retained bounded attempt", orchestrator.isCloseComplete());
        } finally {
            refuseBoundedDrain.set(false);
            orchestrator.closeBy(Long.MAX_VALUE);
        }
    }

    @Test
    public void testMinHttpDeadlineRetainsResourcesAndRetryCompletes() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicBoolean releaseWorker = new AtomicBoolean(false);
            final AtomicBoolean resourceClosed = new AtomicBoolean(false);
            final AtomicReference<WorkerPool> minHttpPool = new AtomicReference<>();
            final CountDownLatch haltStarted = new CountDownLatch(1);
            final CountDownLatch jobEntered = new CountDownLatch(1);
            final CountDownLatch firstCloseReturned = new CountDownLatch(1);
            final AtomicReference<Boolean> firstCloseResult = new AtomicReference<>();
            final AtomicReference<Throwable> firstCloseFailure = new AtomicReference<>();
            final ServerMain server = new ServerMain(getServerMainArgs()) {
                @Override
                protected Services services() {
                    return new Services() {
                        @Override
                        public HttpServer createMinHttpServer(
                                HttpServerConfiguration configuration,
                                WorkerPool workerPool
                        ) {
                            minHttpPool.set(workerPool);
                            workerPool.assign(new Job() {
                                @Override
                                public void closeInstance() {
                                    resourceClosed.set(true);
                                }

                                @Override
                                public boolean run(WorkerContext workerContext) {
                                    jobEntered.countDown();
                                    while (!releaseWorker.get()) {
                                        Os.pause();
                                    }
                                    return false;
                                }
                            });
                            return Services.INSTANCE.createMinHttpServer(configuration, workerPool);
                        }
                    };
                }
            };
            final LifecycleOrchestrator orchestrator = new LifecycleOrchestrator(null, null, null);
            final Component minHttp = newMinHttpEnvelope(server);
            setField(server, "orchestrator", orchestrator);
            orchestrator.register(new TestComponent("factory-provider"));
            orchestrator.register(minHttp);
            Thread firstCloser = null;
            try {
                orchestrator.run();
                final WorkerPool pool = minHttpPool.get();
                Assert.assertNotNull(pool);
                Assert.assertTrue("min-http worker did not enter the blocking job", jobEntered.await(5, TimeUnit.SECONDS));
                pool.setAfterClosedSignalForTesting(haltStarted::countDown);

                final long firstDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(200);
                firstCloser = new Thread(() -> {
                    try {
                        firstCloseResult.set(server.closeBy(firstDeadline));
                    } catch (Throwable th) {
                        firstCloseFailure.set(th);
                    } finally {
                        firstCloseReturned.countDown();
                    }
                }, "min-http-bounded-closer");
                firstCloser.start();

                Assert.assertTrue("min-http halt was not attempted", haltStarted.await(5, TimeUnit.SECONDS));
                Assert.assertTrue(
                        "closeBy remained blocked after its deadline in min-http pool.halt()",
                        firstCloseReturned.await(5, TimeUnit.SECONDS)
                );
                Assert.assertNull(firstCloseFailure.get());
                Assert.assertEquals(Boolean.FALSE, firstCloseResult.get());
                Assert.assertFalse(server.isCloseComplete());
                Assert.assertEquals(State.STOPPING, orchestrator.stateOf("min-http"));
                Assert.assertSame("timed-out min-http stop released its pool", pool, readField(minHttp, "pool"));
                Assert.assertNotNull("timed-out min-http stop released its server", readField(minHttp, "server"));
                Assert.assertFalse("timed-out min-http stop closed worker-owned resources", resourceClosed.get());

                releaseWorker.set(true);
                Assert.assertTrue(server.closeBy(System.nanoTime() + TimeUnit.SECONDS.toNanos(20)));
                Assert.assertTrue(server.isCloseComplete());
                Assert.assertEquals(State.STOPPED, orchestrator.stateOf("min-http"));
                Assert.assertNull(readField(minHttp, "pool"));
                Assert.assertNull(readField(minHttp, "server"));
                Assert.assertTrue(resourceClosed.get());
            } finally {
                releaseWorker.set(true);
                if (firstCloser != null) {
                    join(firstCloser);
                }
                server.closeBy(System.nanoTime() + TimeUnit.SECONDS.toNanos(20));
            }
        });
    }

    @Test
    public void testBoundedCloseCancelsBlockedStart() throws Exception {
        assertMemoryLeak(() -> {
            final CountDownLatch startEntered = new CountDownLatch(1);
            final AtomicInteger cancelRequests = new AtomicInteger();
            final Component component = new TestComponent("component") {
                private volatile boolean isStopRequested;

                @Override
                public void requestStop() {
                    isStopRequested = true;
                    cancelRequests.incrementAndGet();
                }

                @Override
                public void start(LifecycleContext ctx) {
                    startEntered.countDown();
                    while (!isStopRequested) {
                        Os.pause();
                    }
                    ctx.publish(State.READY);
                }
            };
            final ServerMain server = new ServerMain(getServerMainArgs()) {
                @Override
                protected void registerComponents(LifecycleOrchestrator orchestrator) {
                    orchestrator.register(component);
                }
            };
            final Thread starter = new Thread(() -> {
                try {
                    server.start();
                } catch (Throwable ignore) {
                    // a cancelled boot may unwind exceptionally; closeBy completing is the contract
                }
            }, "blocked-starter");
            starter.setDaemon(true);
            try {
                starter.start();
                Assert.assertTrue("component start never entered", startEntered.await(10, TimeUnit.SECONDS));
                // start() boots under closeLock, so closeBy must signal cancellation before
                // waiting on the lock or the blocked start can never unwind to release it
                Assert.assertTrue(
                        "bounded close did not finish after cancelling the blocked start",
                        server.closeBy(System.nanoTime() + TimeUnit.SECONDS.toNanos(10))
                );
                starter.join(TimeUnit.SECONDS.toMillis(10));
                Assert.assertFalse("starter thread did not unwind", starter.isAlive());
                Assert.assertTrue("component never observed the stop request", cancelRequests.get() > 0);
                Assert.assertTrue(server.isCloseComplete());
            } finally {
                server.close();
            }
        });
    }

    @Test
    public void testLifecycleUnexpectedBoundedStopFailurePropagates() {
        final AtomicInteger terminalStopCalls = new AtomicInteger();
        final LifecycleOrchestrator orchestrator = new LifecycleOrchestrator(null, null, null);
        orchestrator.register(new TestComponent("component") {
            @Override
            public void stop() {
                terminalStopCalls.incrementAndGet();
            }

            @Override
            public void stop(long deadlineNanos) {
                throw new IllegalStateException("unexpected stop failure");
            }
        });
        orchestrator.run();

        try {
            try {
                orchestrator.closeBy(Long.MAX_VALUE);
                Assert.fail("unexpected stop failure was treated as incomplete shutdown");
            } catch (IllegalStateException expected) {
                Assert.assertEquals("unexpected stop failure", expected.getMessage());
            }
            Assert.assertEquals(State.STOPPING, orchestrator.stateOf("component"));
            Assert.assertFalse(orchestrator.isCloseComplete());
        } finally {
            orchestrator.close();
        }
        Assert.assertEquals(1, terminalStopCalls.get());
        Assert.assertEquals(State.STOPPED, orchestrator.stateOf("component"));
        Assert.assertTrue(orchestrator.isCloseComplete());
    }

    @Test
    public void testServerMainUnexpectedBoundedStopFailurePropagates() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicInteger terminalStopCalls = new AtomicInteger();
            final IllegalStateException stopFailure = new IllegalStateException("unexpected stop failure");
            final Component component = new TestComponent("component") {
                @Override
                public void stop() {
                    terminalStopCalls.incrementAndGet();
                }

                @Override
                public void stop(long deadlineNanos) {
                    throw stopFailure;
                }
            };
            final ServerMain server = new ServerMain(getServerMainArgs()) {
                @Override
                protected void registerComponents(LifecycleOrchestrator orchestrator) {
                    orchestrator.register(component);
                }
            };
            try {
                server.start();
                final LifecycleOrchestrator orchestrator = (LifecycleOrchestrator) readField(server, "orchestrator");
                try {
                    server.closeBy(Long.MAX_VALUE);
                    Assert.fail("unexpected component failure was converted to a timeout");
                } catch (IllegalStateException actual) {
                    Assert.assertSame(stopFailure, actual);
                }
                Assert.assertEquals(State.STOPPING, orchestrator.stateOf("component"));
                Assert.assertFalse(orchestrator.isCloseComplete());
                Assert.assertFalse(server.isCloseComplete());

                server.close();
                Assert.assertEquals(1, terminalStopCalls.get());
                Assert.assertEquals(State.STOPPED, orchestrator.stateOf("component"));
                Assert.assertTrue(orchestrator.isCloseComplete());
                Assert.assertTrue(server.isCloseComplete());
            } finally {
                server.close();
            }
        });
    }

    @Test
    public void testWorkerPoolBoundedHaltRestoresInterruptAfterLockDeadline() throws Exception {
        final CountDownLatch haltOwnerEntered = new CountDownLatch(1);
        final CountDownLatch releaseHaltOwner = new CountDownLatch(1);
        final AtomicBoolean interruptRestored = new AtomicBoolean();
        final AtomicReference<Boolean> boundedResult = new AtomicReference<>();
        final AtomicReference<Throwable> boundedFailure = new AtomicReference<>();
        final AtomicReference<Throwable> ownerFailure = new AtomicReference<>();
        final WorkerPool pool = new WorkerPool(new WorkerPoolConfiguration() {
            @Override
            public String getPoolName() {
                return "interrupt-restore";
            }

            @Override
            public int getWorkerCount() {
                return 0;
            }
        });
        pool.setAfterClosedSignalForTesting(() -> {
            haltOwnerEntered.countDown();
            awaitUninterruptibly(releaseHaltOwner);
        });
        final Thread haltOwner = new Thread(() -> {
            try {
                pool.halt();
            } catch (Throwable th) {
                ownerFailure.set(th);
            }
        }, "worker-pool-halt-owner");
        final Thread boundedWaiter = new Thread(() -> {
            try {
                boundedResult.set(pool.haltBy(System.nanoTime() + TimeUnit.SECONDS.toNanos(1)));
                interruptRestored.set(Thread.currentThread().isInterrupted());
            } catch (Throwable th) {
                boundedFailure.set(th);
            }
        }, "worker-pool-bounded-waiter");
        try {
            haltOwner.start();
            Assert.assertTrue("halt owner did not acquire the lock", haltOwnerEntered.await(5, TimeUnit.SECONDS));
            final ReentrantLock haltLock = (ReentrantLock) readField(pool, "haltLock");
            boundedWaiter.start();
            awaitQueued(haltLock, boundedWaiter);
            boundedWaiter.interrupt();
            join(boundedWaiter);

            Assert.assertNull(boundedFailure.get());
            Assert.assertEquals(Boolean.FALSE, boundedResult.get());
            Assert.assertTrue("bounded halt lost the consumed interrupt", interruptRestored.get());
        } finally {
            releaseHaltOwner.countDown();
            join(haltOwner);
            join(boundedWaiter);
            pool.halt();
        }
        Assert.assertNull(ownerFailure.get());
    }

    @Test
    public void testServerMainCloseWaitsForBoundedOwner() throws Exception {
        assertMemoryLeak(() -> {
            final CountDownLatch hydrationEntered = new CountDownLatch(1);
            final CountDownLatch releaseHydration = new CountDownLatch(1);
            final CountDownLatch boundedReturned = new CountDownLatch(1);
            final CountDownLatch terminalReturned = new CountDownLatch(1);
            final AtomicReference<Boolean> boundedResult = new AtomicReference<>();
            final AtomicReference<Throwable> boundedFailure = new AtomicReference<>();
            final AtomicReference<Throwable> terminalFailure = new AtomicReference<>();
            final ServerMain server = new ServerMain(getServerMainArgs());
            final Thread hydration = new Thread(() -> {
                hydrationEntered.countDown();
                awaitUninterruptibly(releaseHydration);
            }, "blocked-hydration");
            final Thread boundedCloser = new Thread(() -> {
                try {
                    boundedResult.set(server.closeBy(System.nanoTime() + TimeUnit.SECONDS.toNanos(20)));
                } catch (Throwable th) {
                    boundedFailure.set(th);
                } finally {
                    boundedReturned.countDown();
                }
            }, "server-bounded-closer");
            final Thread terminalCloser = new Thread(() -> {
                try {
                    server.close();
                } catch (Throwable th) {
                    terminalFailure.set(th);
                } finally {
                    terminalReturned.countDown();
                }
            }, "server-terminal-closer");
            try {
                hydration.start();
                Assert.assertTrue(hydrationEntered.await(5, TimeUnit.SECONDS));
                setField(server, "hydrateMetadataThread", hydration);
                final ReentrantLock closeLock = (ReentrantLock) readField(server, "closeLock");
                boundedCloser.start();
                awaitLocked(closeLock, boundedReturned);
                terminalCloser.start();
                assertWaitingForCloseOwner(terminalCloser, terminalReturned, null);
            } finally {
                releaseHydration.countDown();
                join(hydration);
                join(boundedCloser);
                join(terminalCloser);
                server.closeBy(System.nanoTime() + TimeUnit.SECONDS.toNanos(20));
            }
            Assert.assertNull(boundedFailure.get());
            Assert.assertNull(terminalFailure.get());
            Assert.assertEquals(Boolean.TRUE, boundedResult.get());
            Assert.assertTrue(server.isCloseComplete());
        });
    }

    @Test
    public void testServerMainRetainsFreeOnExitUntilEngineIsCloseReady() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicInteger closeCalls = new AtomicInteger();
            final AtomicInteger closeReadyCalls = new AtomicInteger();
            final AtomicBoolean isCloseReady = new AtomicBoolean();
            final Bootstrap bootstrap = new Bootstrap(new PropBootstrapConfiguration(), getServerMainArgs()) {
                @Override
                public CairoEngine newCairoEngine() {
                    final CairoConfiguration configuration = getConfiguration().getCairoConfiguration();
                    return new CairoEngine(configuration, new QdbrWalLocker(), true) {
                        @Override
                        public void close() {
                            closeCalls.incrementAndGet();
                            super.close();
                        }

                        @Override
                        public boolean isCloseReady(long deadlineNanos) {
                            closeReadyCalls.incrementAndGet();
                            return isCloseReady.get() && super.isCloseReady(deadlineNanos);
                        }
                    };
                }
            };
            final ServerMain server = new ServerMain(bootstrap);
            try {
                Assert.assertFalse(server.closeBy(
                        System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
                ));
                Assert.assertFalse(server.isCloseComplete());
                Assert.assertEquals(0, closeCalls.get());
                Assert.assertEquals(1, closeReadyCalls.get());

                isCloseReady.set(true);
                Assert.assertTrue(server.closeBy(
                        System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
                ));
                Assert.assertTrue(server.isCloseComplete());
                Assert.assertEquals(1, closeCalls.get());
                Assert.assertEquals(2, closeReadyCalls.get());
            } finally {
                server.close();
            }
        });
    }

    @Test
    public void testServerMainUnboundedCloseFinishesAfterBoundedTimeout() throws Exception {
        assertMemoryLeak(() -> {
            final CountDownLatch hydrationEntered = new CountDownLatch(1);
            final CountDownLatch releaseHydration = new CountDownLatch(1);
            final ServerMain server = new ServerMain(getServerMainArgs());
            final Thread hydration = new Thread(() -> {
                hydrationEntered.countDown();
                awaitUninterruptibly(releaseHydration);
            }, "blocked-hydration");
            try {
                hydration.start();
                Assert.assertTrue(hydrationEntered.await(5, TimeUnit.SECONDS));
                setField(server, "hydrateMetadataThread", hydration);
                Assert.assertFalse(server.closeBy(System.nanoTime()));
                Assert.assertFalse(server.isCloseComplete());
                releaseHydration.countDown();
                join(hydration);
                server.close();
                Assert.assertTrue("terminal close did not finish the retained bounded attempt", server.isCloseComplete());
            } finally {
                releaseHydration.countDown();
                join(hydration);
                server.closeBy(System.nanoTime() + TimeUnit.SECONDS.toNanos(20));
            }
        });
    }

    private static void assertWaitingForCloseOwner(
            Thread thread,
            CountDownLatch returned,
            AtomicInteger forbiddenCallCount
    ) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            if (forbiddenCallCount != null && forbiddenCallCount.get() != 0) {
                Assert.fail("second close owner entered component teardown concurrently");
            }
            if (returned.getCount() == 0) {
                Assert.fail("second close owner returned before the active owner completed");
            }
            final Thread.State state = thread.getState();
            if (state == Thread.State.BLOCKED || state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING) {
                return;
            }
            Thread.onSpinWait();
        }
        Assert.fail("second close owner did not wait for the active owner");
    }

    private static void awaitLocked(ReentrantLock lock, CountDownLatch ownerReturned) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            if (lock.isLocked()) {
                return;
            }
            if (ownerReturned.getCount() == 0) {
                Assert.fail("bounded close returned before acquiring close ownership");
            }
            Thread.onSpinWait();
        }
        Assert.fail("bounded close did not acquire close ownership");
    }

    private static void awaitQueued(ReentrantLock lock, Thread thread) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            if (lock.hasQueuedThread(thread)) {
                return;
            }
            if (!thread.isAlive()) {
                Assert.fail("bounded halt returned before waiting for the halt lock");
            }
            Thread.onSpinWait();
        }
        Assert.fail("bounded halt did not wait for the halt lock");
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        boolean isInterrupted = false;
        try {
            while (true) {
                try {
                    latch.await();
                    return;
                } catch (InterruptedException e) {
                    isInterrupted = true;
                }
            }
        } finally {
            if (isInterrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static void join(Thread thread) throws InterruptedException {
        if (thread == null) {
            return;
        }
        thread.join(TimeUnit.SECONDS.toMillis(20));
        Assert.assertFalse("thread did not terminate [name=" + thread.getName() + ']', thread.isAlive());
    }

    private static Component newMinHttpEnvelope(ServerMain server) throws Exception {
        final Class<?> envelopeClass = Class.forName("io.questdb.ServerMain$MinHttpEnvelope");
        final Constructor<?> constructor = envelopeClass.getDeclaredConstructor(ServerMain.class, io.questdb.log.Log.class);
        constructor.setAccessible(true);
        return (Component) constructor.newInstance(server, io.questdb.log.LogFactory.getLog(ServerMainBoundedShutdownTest.class));
    }

    private static Object readField(Object instance, String fieldName) throws Exception {
        Class<?> type = instance.getClass();
        while (type != null) {
            try {
                final Field field = type.getDeclaredField(fieldName);
                field.setAccessible(true);
                return field.get(instance);
            } catch (NoSuchFieldException ignored) {
                type = type.getSuperclass();
            }
        }
        throw new NoSuchFieldException(fieldName);
    }

    private static void setField(Object instance, String fieldName, Object value) throws Exception {
        Class<?> type = instance.getClass();
        while (type != null) {
            try {
                final Field field = type.getDeclaredField(fieldName);
                field.setAccessible(true);
                field.set(instance, value);
                return;
            } catch (NoSuchFieldException ignored) {
                type = type.getSuperclass();
            }
        }
        throw new NoSuchFieldException(fieldName);
    }

    private static class TestComponent implements Component {
        private final String name;

        private TestComponent(String name) {
            this.name = name;
        }

        @Override
        public ObjList<String> hardRequiredDependencies() {
            return NO_DEPENDENCIES;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public ObjList<String> softDependencies() {
            return NO_DEPENDENCIES;
        }

        @Override
        public void start(LifecycleContext ctx) {
            ctx.publish(State.READY);
        }

        @Override
        public void stop() {
        }
    }
}
