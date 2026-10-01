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
import io.questdb.cairo.view.ViewCompilerExecutionContext;
import io.questdb.cairo.wal.QdbrWalLocker;
import io.questdb.lifecycle.Component;
import io.questdb.lifecycle.LifecycleContext;
import io.questdb.lifecycle.LifecycleStartupException;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Regression test for #079 (Phase 9 Cycle 2 Critical).
 * <p>
 * Pre-fix HEAD: {@code onStartupAsyncHydrator()}, {@code hydrateRecentWriteTracker()}, and
 * {@code ViewCompilerJob.compileAllViews(...)} live ONLY in the dead
 * {@code bindAndStartNetworkServices(log)} / {@code initialize(log)} shims on
 * {@code ServerMain}. Production boot through {@code start(boolean)} +
 * {@code orchestrator.run()} never reaches those shims, so view compilation is
 * silently skipped on every production boot.
 * <p>
 * After fix: a post-engine-READY hydration envelope re-runs all three on the production
 * boot path, and {@code engine.createViewCompilerContext(0)} is invoked exactly once.
 * This test observes the call via a {@link CairoEngine} subclass that counts invocations.
 */
public class ServerMainHydrationEnvelopeTest extends AbstractBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        // Disable the dedicated view-compiler pool so its ViewCompilerJob constructors do not
        // call engine.createViewCompilerContext(...) and pollute the counter. With workerCount=0
        // the dedicated pool is skipped (ServerMain.setupDedicatedPools branch), and the ONLY
        // remaining caller of createViewCompilerContext on the boot path is the body of
        // ViewCompilerJob.compileAllViews(...) -- which itself runs only via the new
        // HydrationEnvelope after the #079 fix. Pre-fix HEAD has zero calls on the production
        // boot path; post-fix has exactly one.
        TestUtils.unchecked(() -> createDummyConfiguration(
                PropertyKey.VIEW_COMPILER_WORKER_COUNT + "=0"
        ));
        dbPath.parent().$();
    }

    @Test
    public void awaitStartupWaitsForHydrationWhenInterrupted() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicBoolean awaitReturned = new AtomicBoolean();
            final AtomicBoolean interruptRestored = new AtomicBoolean();
            final SOCountDownLatch hydrationStarted = new SOCountDownLatch(1);
            final SOCountDownLatch releaseHydration = new SOCountDownLatch(1);

            ServerMain serverMain = newServerMainWithBlockedHydration(hydrationStarted, releaseHydration);
            Thread awaitStartupThread = null;
            try {
                serverMain.start(false);
                Assert.assertTrue(
                        "metadata hydration did not start",
                        hydrationStarted.await(TimeUnit.SECONDS.toNanos(5))
                );

                awaitStartupThread = new Thread(() -> {
                    Thread.currentThread().interrupt();
                    serverMain.awaitStartup();
                    interruptRestored.set(Thread.currentThread().isInterrupted());
                    awaitReturned.set(true);
                }, "await-startup-test");
                awaitStartupThread.setDaemon(true);
                awaitStartupThread.start();

                final Thread thread = awaitStartupThread;
                TestUtils.assertEventually(
                        () -> Assert.assertTrue(awaitReturned.get() || thread.getState() == Thread.State.WAITING),
                        5
                );
                Assert.assertFalse("awaitStartup returned before hydration completed", awaitReturned.get());

                releaseHydration.countDown();
                awaitStartupThread.join(TimeUnit.SECONDS.toMillis(10));
                Assert.assertFalse("awaitStartup thread did not stop", awaitStartupThread.isAlive());
                Assert.assertTrue("awaitStartup did not restore interrupt status", interruptRestored.get());
            } finally {
                releaseHydration.countDown();
                if (awaitStartupThread != null) {
                    awaitStartupThread.join(TimeUnit.SECONDS.toMillis(10));
                }
                serverMain.close();
            }
        });
    }

    @Test
    public void hydrationStopWaitsForHydrationAndConsumesInterrupts() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicBoolean hasStopReturned = new AtomicBoolean();
            final AtomicBoolean isInterruptedAfterStop = new AtomicBoolean(true);
            final AtomicReference<Throwable> stopError = new AtomicReference<>();
            final AtomicReference<Thread> viewCompilerThread = new AtomicReference<>();
            final SOCountDownLatch compileViewsFinished = new SOCountDownLatch(1);
            final SOCountDownLatch hydrationStarted = new SOCountDownLatch(1);
            final SOCountDownLatch releaseHydration = new SOCountDownLatch(1);
            final SOCountDownLatch stopStarted = new SOCountDownLatch(1);

            ServerMain serverMain = newServerMainWithBlockedHydration(
                    hydrationStarted,
                    releaseHydration,
                    compileViewsFinished,
                    viewCompilerThread
            );
            Thread stopThread = null;
            try {
                serverMain.start(false);
                Assert.assertTrue(
                        "metadata hydration did not start",
                        hydrationStarted.await(TimeUnit.SECONDS.toNanos(5))
                );
                Assert.assertTrue(
                        "view compilation did not finish",
                        compileViewsFinished.await(TimeUnit.SECONDS.toNanos(5))
                );
                final Thread compilerThread = viewCompilerThread.get();
                Assert.assertNotNull("view compiler thread was not captured", compilerThread);
                compilerThread.join(TimeUnit.SECONDS.toMillis(5));
                Assert.assertFalse("view compiler thread did not stop", compilerThread.isAlive());

                final Component hydration = serverMain.getOrchestrator().getComponent("hydration");
                stopThread = new Thread(() -> {
                    Thread.currentThread().interrupt();
                    stopStarted.countDown();
                    try {
                        hydration.stop();
                    } catch (Throwable th) {
                        stopError.set(th);
                    } finally {
                        isInterruptedAfterStop.set(Thread.currentThread().isInterrupted());
                        hasStopReturned.set(true);
                    }
                }, "stop-hydration-test");
                stopThread.setDaemon(true);
                stopThread.start();

                Assert.assertTrue(
                        "hydration stop thread did not start",
                        stopStarted.await(TimeUnit.SECONDS.toNanos(5))
                );
                final Thread thread = stopThread;
                TestUtils.assertEventually(
                        () -> Assert.assertFalse("hydration stop did not consume the initial interrupt", thread.isInterrupted()),
                        5
                );
                Assert.assertFalse("hydration stop returned before hydration completed", hasStopReturned.get());

                thread.interrupt();
                TestUtils.assertEventually(
                        () -> Assert.assertFalse("hydration stop did not consume the mid-join interrupt", thread.isInterrupted()),
                        5
                );
                Assert.assertFalse("hydration stop returned after a mid-join interrupt", hasStopReturned.get());

                releaseHydration.countDown();
                stopThread.join(TimeUnit.SECONDS.toMillis(10));

                Assert.assertFalse("hydration stop thread did not stop", stopThread.isAlive());
                Assert.assertNull(stopError.get());
                Assert.assertTrue("hydration stop did not return", hasStopReturned.get());
                Assert.assertFalse("hydration stop restored interrupt status", isInterruptedAfterStop.get());
            } finally {
                releaseHydration.countDown();
                if (stopThread != null) {
                    stopThread.interrupt();
                    stopThread.join(TimeUnit.SECONDS.toMillis(10));
                }
                serverMain.close();
            }
        });
    }

    @Test
    public void compileAllViewsRanOnProductionBoot() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicInteger createViewCompilerContextCalls = new AtomicInteger();

            // Wire a Bootstrap that returns a CairoEngine subclass which counts
            // createViewCompilerContext invocations. With the dedicated view-compiler pool disabled
            // (see setUp), the ONLY OSS caller of this method on the boot path is
            // ViewCompilerJob.compileAllViews(engine, ...), so a single increment proves compileAllViews ran.
            Bootstrap bootstrap = new Bootstrap(new PropBootstrapConfiguration(), getServerMainArgs()) {
                @Override
                public CairoEngine newCairoEngine() {
                    CairoConfiguration cfg = getConfiguration().getCairoConfiguration();
                    return new CairoEngine(cfg, new QdbrWalLocker(), true) {
                        @Override
                        public ViewCompilerExecutionContext createViewCompilerContext(int workerCount) {
                            createViewCompilerContextCalls.incrementAndGet();
                            return super.createViewCompilerContext(workerCount);
                        }
                    };
                }
            };

            try (ServerMain serverMain = new ServerMain(bootstrap)) {
                serverMain.start(false);
                // Wait for the hydration thread to actually run its body. awaitStartup() joins both
                // hydrateMetadataThread and compileViewsThread. After the fix, the compile-views
                // thread is created+started by the new HydrationEnvelope; pre-fix it is null and
                // awaitStartup() returns immediately, so the counter assertion below is what fails.
                serverMain.awaitStartup();
                Assert.assertEquals(
                        "compileAllViews must run on the production boot path "
                                + "(observed createViewCompilerContext invocation count)",
                        1,
                        createViewCompilerContextCalls.get()
                );
            }
        });
    }

    private ServerMain newServerMainWithBlockedHydration(
            SOCountDownLatch hydrationStarted,
            SOCountDownLatch releaseHydration
    ) {
        return newServerMainWithBlockedHydration(hydrationStarted, releaseHydration, null, null);
    }

    private ServerMain newServerMainWithBlockedHydration(
            SOCountDownLatch hydrationStarted,
            SOCountDownLatch releaseHydration,
            SOCountDownLatch compileViewsFinished,
            AtomicReference<Thread> viewCompilerThread
    ) {
        Bootstrap bootstrap = new Bootstrap(new PropBootstrapConfiguration(), getServerMainArgs()) {
            @Override
            public CairoEngine newCairoEngine() {
                CairoConfiguration cfg = getConfiguration().getCairoConfiguration();
                return new CairoEngine(cfg, new QdbrWalLocker(), true) {
                    @Override
                    public void hydrateRecentWriteTracker() {
                        hydrationStarted.countDown();
                        releaseHydration.await();
                    }

                    @Override
                    public ViewCompilerExecutionContext createViewCompilerContext(int workerCount) {
                        if (compileViewsFinished == null) {
                            return super.createViewCompilerContext(workerCount);
                        }
                        viewCompilerThread.set(Thread.currentThread());
                        return new ViewCompilerExecutionContext(this, workerCount) {
                            @Override
                            public void close() {
                                try {
                                    super.close();
                                } finally {
                                    compileViewsFinished.countDown();
                                }
                            }
                        };
                    }
                };
            }
        };
        return new ServerMain(bootstrap);
    }
}
