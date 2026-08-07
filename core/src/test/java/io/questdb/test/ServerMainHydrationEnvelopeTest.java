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
import io.questdb.WorkerPoolManager;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.view.ViewCompilerExecutionContext;
import io.questdb.cairo.wal.QdbrWalLocker;
import io.questdb.lifecycle.Component;
import io.questdb.lifecycle.LifecycleContext;
import io.questdb.lifecycle.LifecycleOrchestrator;
import io.questdb.lifecycle.LifecycleStartupException;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
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

    @Test
    public void testStartupRollbackDoesNotWaitUnboundedlyForViewCompilation() throws Exception {
        assertMemoryLeak(() -> {
            final SOCountDownLatch compileEntered = new SOCountDownLatch(1);
            final SOCountDownLatch releaseCompile = new SOCountDownLatch(1);
            final AtomicReference<Throwable> startFailure = new AtomicReference<>();
            final Bootstrap bootstrap = new Bootstrap(new PropBootstrapConfiguration(), getServerMainArgs()) {
                @Override
                public CairoEngine newCairoEngine() {
                    final CairoConfiguration configuration = getConfiguration().getCairoConfiguration();
                    return new CairoEngine(configuration, new QdbrWalLocker(), true) {
                        @Override
                        public ViewCompilerExecutionContext createViewCompilerContext(int workerCount) {
                            compileEntered.countDown();
                            releaseCompile.await();
                            return super.createViewCompilerContext(workerCount);
                        }
                    };
                }
            };
            final ServerMain serverMain = new ServerMain(bootstrap) {
                @Override
                protected ObjList<Component> baseComponents() {
                    final ObjList<Component> components = super.baseComponents();
                    components.add(new Component() {
                        private final ObjList<String> empty = new ObjList<>();
                        private final ObjList<String> hardDeps = new ObjList<>();

                        {
                            hardDeps.add("hydration");
                        }

                        @Override
                        public ObjList<String> hardRequiredDependencies() {
                            return hardDeps;
                        }

                        @Override
                        public String name() {
                            return "post-hydration-failure";
                        }

                        @Override
                        public ObjList<String> softDependencies() {
                            return empty;
                        }

                        @Override
                        public void start(LifecycleContext ctx) {
                            if (!compileEntered.await(TimeUnit.SECONDS.toNanos(5))) {
                                throw new AssertionError("view compilation did not start");
                            }
                            throw new IllegalStateException("startup failure");
                        }

                        @Override
                        public void stop() {
                        }
                    });
                    return components;
                }

                @Override
                protected LifecycleOrchestrator newOrchestrator(
                        io.questdb.log.Log log,
                        WorkerPoolManager workerPoolManager,
                        Object tokioRuntime
                ) {
                    return new LifecycleOrchestrator(log, workerPoolManager, tokioRuntime) {
                        @Override
                        public void closeBy(long deadlineNanos) {
                            super.closeBy(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(10));
                        }
                    };
                }
            };
            final Thread startThread = new Thread(() -> {
                try {
                    serverMain.start(false);
                } catch (Throwable th) {
                    startFailure.set(th);
                } finally {
                    Path.clearThreadLocals();
                }
            });
            try {
                startThread.start();
                Assert.assertTrue(compileEntered.await(TimeUnit.SECONDS.toNanos(5)));
                startThread.join(5_000);
                Assert.assertFalse("startup rollback waited on view compilation", startThread.isAlive());
                Assert.assertTrue(startFailure.get() instanceof LifecycleStartupException);
                Assert.assertFalse(serverMain.isCloseComplete());
            } finally {
                releaseCompile.countDown();
                startThread.join(5_000);
                serverMain.close();
            }
            Assert.assertTrue(serverMain.isCloseComplete());
        });
    }
}
