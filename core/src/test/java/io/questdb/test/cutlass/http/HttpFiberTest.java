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

package io.questdb.test.cutlass.http;

import io.questdb.DefaultServerConfiguration;
import io.questdb.Metrics;
import io.questdb.ServerConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableWriter;
import io.questdb.cutlass.Services;
import io.questdb.cutlass.http.ActiveConnectionTracker;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpConnectionFiberTask;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.HttpRequestHandler;
import io.questdb.cutlass.http.HttpRequestHandlerFactory;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.cutlass.http.HttpRequestProcessorSelector;
import io.questdb.cutlass.http.HttpServer;
import io.questdb.cutlass.http.RescheduleContext;
import io.questdb.cutlass.http.WaitProcessor;
import io.questdb.cutlass.http.processors.JsonQueryProcessor;
import io.questdb.cutlass.http.processors.JsonQueryProcessorState;
import io.questdb.mp.Job;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolConfigurationWrapper;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.mp.continuation.SuspensionScope;
import io.questdb.mp.continuation.TimerShards;
import io.questdb.network.IODispatcher;
import io.questdb.network.IOOperation;
import io.questdb.network.IORequestProcessor;
import io.questdb.network.Net;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.PeerIsSlowToWriteException;
import io.questdb.network.PlainSocketFactory;
import io.questdb.network.ServerDisconnectException;
import io.questdb.std.LongList;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.test.AbstractTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Exercises HTTP query execution on pooled fibers.
 */
public class HttpFiberTest extends AbstractTest {

    @Test
    public void testBindingAfterSelectorReuseUpdatesSelectors() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            assertBindingAfterSelectorReuse(WorkerPoolMode.FIBER_HOST);
            assertBindingAfterSelectorReuse(WorkerPoolMode.LEGACY);
        });
    }

    @Test
    public void testSelectorPoolTracksDynamicFiberLimit() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final DefaultTestCairoConfiguration cairoConfiguration = new DefaultTestCairoConfiguration(root);
            final DefaultHttpServerConfiguration httpConfiguration = new HttpServerConfigurationBuilder()
                    .withBaseDir(root)
                    .withFiberEnabled(true)
                    .withPort(0)
                    .withWorkerCount(1)
                    .build(cairoConfiguration);
            final WorkerPoolConfigurationWrapper poolConfiguration = new WorkerPoolConfigurationWrapper();
            poolConfiguration.setDelegate(fiberHostConfiguration(1));
            try (WorkerPool workerPool = new TestWorkerPool(poolConfiguration)) {
                final FiberRuntime runtime = workerPool.getFiberRuntime();
                final HttpServer httpServer = new HttpServer(
                        httpConfiguration,
                        workerPool,
                        PlainSocketFactory.INSTANCE
                );
                try {
                    Assert.assertEquals(1, runtime.getConfigurationListenerCountForTesting());
                    Assert.assertEquals(1, httpServer.getMaxRecycledSelectorCountForTesting());

                    poolConfiguration.setDelegate(fiberHostConfiguration(3));

                    Assert.assertEquals(3, httpServer.getMaxRecycledSelectorCountForTesting());
                    final HttpRequestProcessorSelector first = httpServer.acquireSelectorForTesting();
                    final HttpRequestProcessorSelector second = httpServer.acquireSelectorForTesting();
                    final HttpRequestProcessorSelector third = httpServer.acquireSelectorForTesting();
                    httpServer.releaseSelectorForTesting(first);
                    httpServer.releaseSelectorForTesting(second);
                    httpServer.releaseSelectorForTesting(third);
                    Assert.assertEquals(3, httpServer.getRecycledSelectorCountForTesting());

                    final HttpRequestProcessorSelector leasedFirst = httpServer.acquireSelectorForTesting();
                    final HttpRequestProcessorSelector leasedSecond = httpServer.acquireSelectorForTesting();
                    final HttpRequestProcessorSelector leasedThird = httpServer.acquireSelectorForTesting();
                    Assert.assertEquals(0, httpServer.getRecycledSelectorCountForTesting());

                    poolConfiguration.setDelegate(fiberHostConfiguration(1));

                    Assert.assertEquals(1, httpServer.getMaxRecycledSelectorCountForTesting());
                    httpServer.releaseSelectorForTesting(leasedFirst);
                    httpServer.releaseSelectorForTesting(leasedSecond);
                    httpServer.releaseSelectorForTesting(leasedThird);
                    Assert.assertEquals(1, httpServer.getRecycledSelectorCountForTesting());

                    poolConfiguration.setDelegate(fiberHostConfiguration(3));
                    final HttpRequestProcessorSelector idleFirst = httpServer.acquireSelectorForTesting();
                    final HttpRequestProcessorSelector idleSecond = httpServer.acquireSelectorForTesting();
                    final HttpRequestProcessorSelector idleThird = httpServer.acquireSelectorForTesting();
                    httpServer.releaseSelectorForTesting(idleFirst);
                    httpServer.releaseSelectorForTesting(idleSecond);
                    httpServer.releaseSelectorForTesting(idleThird);
                    Assert.assertEquals(3, httpServer.getRecycledSelectorCountForTesting());

                    poolConfiguration.setDelegate(fiberHostConfiguration(1));

                    Assert.assertEquals(1, httpServer.getRecycledSelectorCountForTesting());
                } finally {
                    httpServer.close();
                }
                Assert.assertEquals(0, runtime.getConfigurationListenerCountForTesting());
                Assert.assertEquals(0, httpServer.getMaxRecycledSelectorCountForTesting());
                Assert.assertEquals(0, httpServer.getRecycledSelectorCountForTesting());

                poolConfiguration.setDelegate(fiberHostConfiguration(3));

                Assert.assertEquals(0, httpServer.getMaxRecycledSelectorCountForTesting());
            }
        });
    }

    @Test
    public void testSelectorAcquireCannotCompleteAcrossClose() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final DefaultTestCairoConfiguration cairoConfiguration = new DefaultTestCairoConfiguration(root);
            final DefaultHttpServerConfiguration httpConfiguration = new HttpServerConfigurationBuilder()
                    .withBaseDir(root)
                    .withFiberEnabled(true)
                    .withPort(0)
                    .withWorkerCount(1)
                    .build(cairoConfiguration);
            final CountDownLatch selectorPopped = new CountDownLatch(1);
            final CountDownLatch resumeAcquire = new CountDownLatch(1);
            try (WorkerPool workerPool = new TestWorkerPool(fiberHostConfiguration(1))) {
                final HttpServer httpServer = HttpServer.createWithSelectorPopHookForTesting(
                        httpConfiguration,
                        workerPool,
                        PlainSocketFactory.INSTANCE,
                        () -> {
                            selectorPopped.countDown();
                            TestUtils.await(resumeAcquire);
                        }
                );
                final HttpRequestProcessorSelector recycled = httpServer.acquireSelectorForTesting();
                httpServer.releaseSelectorForTesting(recycled);
                final CountDownLatch acquireCompleted = new CountDownLatch(1);
                final AtomicReference<HttpRequestProcessorSelector> acquiredSelector = new AtomicReference<>();
                final AtomicReference<Throwable> acquireFailure = new AtomicReference<>();
                final Thread acquiringThread = new Thread(() -> {
                    try {
                        acquiredSelector.set(httpServer.acquireSelectorForTesting());
                    } catch (Throwable th) {
                        acquireFailure.set(th);
                    } finally {
                        acquireCompleted.countDown();
                    }
                });
                boolean isServerClosed = false;
                try {
                    acquiringThread.start();
                    Assert.assertTrue(selectorPopped.await(10, TimeUnit.SECONDS));

                    httpServer.close();
                    isServerClosed = true;
                    Assert.assertThrows(IllegalStateException.class, httpServer::acquireSelectorForTesting);
                    resumeAcquire.countDown();

                    Assert.assertTrue(acquireCompleted.await(10, TimeUnit.SECONDS));
                    acquiringThread.join();
                    final HttpRequestProcessorSelector acquired = acquiredSelector.get();
                    if (acquired != null) {
                        httpServer.releaseSelectorForTesting(acquired);
                    }
                    Assert.assertNull(acquired);
                    Assert.assertTrue(acquireFailure.get() instanceof IllegalStateException);
                } finally {
                    resumeAcquire.countDown();
                    acquiringThread.join();
                    if (!isServerClosed) {
                        httpServer.close();
                    }
                }
            }
        });
    }

    @Test
    public void testSelectorPoolDynamicTrimContinuesAfterCloseFailure() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final DefaultTestCairoConfiguration cairoConfiguration = new DefaultTestCairoConfiguration(root);
            final DefaultHttpServerConfiguration httpConfiguration = new HttpServerConfigurationBuilder()
                    .withBaseDir(root)
                    .withFiberEnabled(true)
                    .withPort(0)
                    .withWorkerCount(1)
                    .build(cairoConfiguration);
            final WorkerPoolConfigurationWrapper poolConfiguration = new WorkerPoolConfigurationWrapper();
            poolConfiguration.setDelegate(fiberHostConfiguration(3));
            try (WorkerPool workerPool = new TestWorkerPool(poolConfiguration)) {
                final AtomicInteger failingCloseCount = new AtomicInteger();
                final AtomicInteger followingCloseCount = new AtomicInteger();
                final HttpServer httpServer = new HttpServer(
                        httpConfiguration,
                        workerPool,
                        PlainSocketFactory.INSTANCE
                );
                try {
                    httpServer.bind(closeTrackingHandlerFactory("/failing", failingCloseCount, true));
                    httpServer.bind(closeTrackingHandlerFactory("/following", followingCloseCount, false));
                    final HttpRequestProcessorSelector first = httpServer.acquireSelectorForTesting();
                    final HttpRequestProcessorSelector second = httpServer.acquireSelectorForTesting();
                    final HttpRequestProcessorSelector third = httpServer.acquireSelectorForTesting();
                    httpServer.releaseSelectorForTesting(first);
                    httpServer.releaseSelectorForTesting(second);
                    httpServer.releaseSelectorForTesting(third);

                    poolConfiguration.setDelegate(fiberHostConfiguration(1));

                    Assert.assertEquals(1, httpServer.getRecycledSelectorCountForTesting());
                    Assert.assertEquals(2, failingCloseCount.get());
                    Assert.assertEquals(2, followingCloseCount.get());
                } finally {
                    try {
                        httpServer.close();
                    } catch (RuntimeException ignored) {
                    }
                }
            }
        });
    }

    @Test
    public void testSelectorPoolTerminalCloseContinuesAfterCloseFailure() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final DefaultTestCairoConfiguration cairoConfiguration = new DefaultTestCairoConfiguration(root);
            final DefaultHttpServerConfiguration httpConfiguration = new HttpServerConfigurationBuilder()
                    .withBaseDir(root)
                    .withFiberEnabled(true)
                    .withPort(0)
                    .withWorkerCount(1)
                    .build(cairoConfiguration);
            try (WorkerPool workerPool = new TestWorkerPool(fiberHostConfiguration(3))) {
                final AtomicInteger failingCloseCount = new AtomicInteger();
                final AtomicInteger followingCloseCount = new AtomicInteger();
                final AtomicInteger serverCloseableCount = new AtomicInteger();
                final HttpServer httpServer = new HttpServer(
                        httpConfiguration,
                        workerPool,
                        PlainSocketFactory.INSTANCE
                );
                try {
                    httpServer.bind(closeTrackingHandlerFactory("/failing", failingCloseCount, true));
                    httpServer.bind(closeTrackingHandlerFactory("/following", followingCloseCount, false));
                    httpServer.registerClosable(serverCloseableCount::incrementAndGet);
                    httpServer.getSelectorByWorkerForTesting(0);
                    final HttpRequestProcessorSelector first = httpServer.acquireSelectorForTesting();
                    final HttpRequestProcessorSelector second = httpServer.acquireSelectorForTesting();
                    final HttpRequestProcessorSelector third = httpServer.acquireSelectorForTesting();
                    httpServer.releaseSelectorForTesting(first);
                    httpServer.releaseSelectorForTesting(second);
                    httpServer.releaseSelectorForTesting(third);

                    final RuntimeException failure = Assert.assertThrows(
                            RuntimeException.class,
                            httpServer::close
                    );

                    Assert.assertEquals(3, failure.getSuppressed().length);
                    Assert.assertEquals(4, failingCloseCount.get());
                    Assert.assertEquals(4, followingCloseCount.get());
                    Assert.assertEquals(1, serverCloseableCount.get());
                    Assert.assertEquals(0, httpServer.getRecycledSelectorCountForTesting());
                } finally {
                    if (serverCloseableCount.get() == 0) {
                        try {
                            httpServer.close();
                        } catch (RuntimeException ignored) {
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testBusyWriterRetryLaunchesRerunOnFiber() throws Exception {
        final HttpQueryTestBuilder builder = new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(2)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder().withFiberEnabled(true))
                .withTelemetry(false);
        TestUtils.assertMemoryLeak(() -> builder
                .run((engine, sqlExecutionContext) -> {
                    final int insertCount = 4;
                    try (TestHttpClient testHttpClient = new TestHttpClient()) {
                        testHttpClient.assertGet("{\"ddl\":\"OK\"}", "CREATE TABLE tab (x LONG)");

                        final WaitProcessor waitProcessor = builder.getHttpServer().getWaitProcessor();
                        final SOCountDownLatch inserted = new SOCountDownLatch(insertCount);
                        final AtomicReference<Throwable> insertError = new AtomicReference<>();
                        final ObjList<Thread> threads = new ObjList<>();
                        try (TableWriter ignore = engine.getWriter(engine.verifyTableName("tab"), "test")) {
                            // the held writer turns every INSERT into a busy-writer retry:
                            // the dispatch job parks it in the WaitProcessor, and each due
                            // rerun launches the connection's task on a pooled fiber,
                            // re-parking with growing backoff while the writer stays busy
                            final long parkedBaseline = waitProcessor.getRescheduleCount();
                            for (int i = 0; i < insertCount; i++) {
                                final int value = i;
                                Thread thread = new Thread(() -> {
                                    try (TestHttpClient insertClient = new TestHttpClient()) {
                                        insertClient.assertGet("{\"dml\":\"OK\"}", "INSERT INTO tab VALUES (" + value + ")");
                                    } catch (Throwable th) {
                                        insertError.set(th);
                                    } finally {
                                        inserted.countDown();
                                    }
                                });
                                thread.start();
                                threads.add(thread);
                            }
                            // every insert parks at least once; none can complete
                            TestUtils.assertEventually(() -> Assert.assertTrue(
                                    waitProcessor.getRescheduleCount() >= parkedBaseline + insertCount
                            ));
                            Assert.assertEquals(insertCount, inserted.getCount());
                        }
                        Assert.assertTrue(
                                "inserts did not complete after writer release",
                                inserted.await(TimeUnit.SECONDS.toNanos(10))
                        );
                        for (int i = 0, n = threads.size(); i < n; i++) {
                            threads.getQuick(i).join();
                        }
                        Assert.assertNull(insertError.get());
                        testHttpClient.assertGet(
                                "{\"query\":\"SELECT count() cnt FROM tab\",\"columns\":[{\"name\":\"cnt\",\"type\":\"LONG\"}],\"timestamp\":-1,\"dataset\":[[4]],\"count\":1}",
                                "SELECT count() cnt FROM tab"
                        );
                    }
                }));
    }

    @Test
    public void testClientDisconnectWhileRetryParkedOnFiber() throws Exception {
        final HttpQueryTestBuilder builder = new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(2)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder().withFiberEnabled(true))
                .withTelemetry(false);
        TestUtils.assertMemoryLeak(() -> builder
                .run((engine, sqlExecutionContext) -> {
                    try (TestHttpClient testHttpClient = new TestHttpClient()) {
                        testHttpClient.assertGet("{\"ddl\":\"OK\"}", "CREATE TABLE tab (x LONG)");
                        final WaitProcessor waitProcessor = builder.getHttpServer().getWaitProcessor();
                        final ActiveConnectionTracker connectionTracker = builder
                                .getHttpServer()
                                .getActiveConnectionTracker();
                        TestUtils.assertEventually(() -> Assert.assertEquals(
                                0,
                                connectionTracker.get(ActiveConnectionTracker.PROCESSOR_JSON)
                        ));
                        final long activeConnectionsBeforeRequest = connectionTracker.get(
                                ActiveConnectionTracker.PROCESSOR_JSON
                        );
                        try (TableWriter ignore = engine.getWriter(engine.verifyTableName("tab"), "test")) {
                            final long parkedBaseline = waitProcessor.getRescheduleCount();
                            final long fd = new SendAndReceiveRequestBuilder().connectAndSendRequest(
                                    "GET /query?query=INSERT+INTO+tab+VALUES+(42) HTTP/1.1\r\n"
                                            + "Host: localhost:9001\r\n"
                                            + "\r\n"
                            );
                            // the INSERT parks in the retry queue while the writer is busy
                            TestUtils.assertEventually(() -> Assert.assertTrue(
                                    waitProcessor.getRescheduleCount() > parkedBaseline
                            ));
                            TestUtils.assertEventually(() -> Assert.assertEquals(
                                    activeConnectionsBeforeRequest + 1,
                                    connectionTracker.get(ActiveConnectionTracker.PROCESSOR_JSON)
                            ));
                            // the client vanishes while its retry is parked; nothing
                            // observes the dead socket until the rerun touches it
                            Net.close(fd);
                        }
                        // A rerun either reaches the response write or trips the breaker.
                        // Both paths must reap the dead connection.
                        TestUtils.assertEventually(() -> Assert.assertEquals(
                                activeConnectionsBeforeRequest,
                                connectionTracker.get(ActiveConnectionTracker.PROCESSOR_JSON)
                        ));
                        testHttpClient.assertGet(
                                "{\"query\":\"SELECT 1 x\",\"columns\":[{\"name\":\"x\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[1]],\"count\":1}",
                                "SELECT 1 x"
                        );
                    }
                }));
    }

    @Test
    public void testHttpMinDedicatedPoolIsLegacy() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final DefaultTestCairoConfiguration cairoConfiguration = new DefaultTestCairoConfiguration(root);
            final DefaultHttpServerConfiguration httpConfiguration = new HttpServerConfigurationBuilder()
                    .withBaseDir(root)
                    .withFiberEnabled(false)
                    .withPort(0)
                    .withWorkerCount(1)
                    .build(cairoConfiguration);
            try (
                    WorkerPool workerPool = new TestWorkerPool(httpConfiguration);
                    HttpServer minHttpServer = Services.INSTANCE.createMinHttpServer(httpConfiguration, workerPool)
            ) {
                Assert.assertFalse(workerPool.isFiberHost());
                Assert.assertThrows(IllegalStateException.class, workerPool::getFiberRuntime);
                workerPool.start();
                try {
                    new SendAndReceiveRequestBuilder()
                            .withPort(minHttpServer.getPort())
                            .execute(
                                    "GET /status HTTP/1.1\r\n\r\n",
                                    "HTTP/1.1 200 OK"
                            );
                    Assert.assertThrows(IllegalStateException.class, workerPool::getFiberRuntime);
                } finally {
                    workerPool.halt();
                }
            }
        });
    }

    @Test
    public void testHttpMinUsesOrdinaryJobsOnSharedFiberHostPool() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final DefaultTestCairoConfiguration cairoConfiguration = new DefaultTestCairoConfiguration(root);
            final DefaultHttpServerConfiguration httpConfiguration = new HttpServerConfigurationBuilder()
                    .withBaseDir(root)
                    .withFiberEnabled(true)
                    .withPort(0)
                    .withWorkerCount(1)
                    .build(cairoConfiguration);
            final WorkerPoolConfiguration workerPoolConfiguration = new WorkerPoolConfiguration() {
                @Override
                public Metrics getMetrics() {
                    return Metrics.DISABLED;
                }

                @Override
                public int getWorkerCount() {
                    return 1;
                }

                @Override
                public WorkerPoolMode getWorkerPoolMode() {
                    return WorkerPoolMode.FIBER_HOST;
                }
            };
            try (
                    CairoEngine engine = new CairoEngine(cairoConfiguration);
                    WorkerPool workerPool = new TestWorkerPool(workerPoolConfiguration);
                    HttpServer minHttpServer = Services.INSTANCE.createMinHttpServer(httpConfiguration, workerPool);
                    HttpServer httpServer = new HttpServer(httpConfiguration, workerPool, PlainSocketFactory.INSTANCE)
            ) {
                httpServer.bind(createJsonQueryFactory("/query", httpConfiguration, engine));
                workerPool.start();
                try {
                    new SendAndReceiveRequestBuilder()
                            .withPort(minHttpServer.getPort())
                            .execute(
                                    "GET /status HTTP/1.1\r\n\r\n",
                                    "HTTP/1.1 200 OK"
                            );

                    final FiberRuntime runtime = workerPool.getFiberRuntime();
                    Assert.assertEquals(0, runtime.getCreatedFiberCount());
                    Assert.assertEquals(0, runtime.getLaunchCount(LaunchResult.LAUNCHED));
                    Assert.assertEquals(0, runtime.getMountCount());
                    Assert.assertEquals(0, runtime.getOutstandingTaskCount());

                    try (TestHttpClient testHttpClient = new TestHttpClient()) {
                        testHttpClient.assertGet(
                                "/query",
                                "{\"query\":\"SELECT 42 x\",\"columns\":[{\"name\":\"x\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[42]],\"count\":1}",
                                "SELECT 42 x",
                                "localhost",
                                httpServer.getPort(),
                                null,
                                null,
                                null
                        );
                    }
                    Assert.assertTrue(runtime.getMountCount() > 0);
                    Assert.assertTrue(runtime.getCreatedFiberCount() > 0);
                    Assert.assertTrue(runtime.getLaunchCount(LaunchResult.LAUNCHED) > 0);
                } finally {
                    workerPool.halt();
                }
            }
        });
    }

    @Test
    public void testDirectHttpServerQueryInstallsEngineTimerShards() throws Exception {
        TestUtils.assertMemoryLeak(() -> assertHttpQueryInstallsEngineTimerShards(false));
    }

    @Test
    public void testHttpTaskInstallsEngineTimerShards() throws Exception {
        TestUtils.assertMemoryLeak(() -> assertHttpQueryInstallsEngineTimerShards(true));
    }

    @Test
    public void testCloseDoesNotWaitForArmingRetry() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final DefaultHttpServerConfiguration configuration =
                    new DefaultHttpServerConfiguration(new DefaultTestCairoConfiguration(root));
            final TestHttpDispatcher dispatcher = new TestHttpDispatcher();
            final WaitProcessor waitProcessor = new WaitProcessor(
                    configuration.getWaitProcessorConfiguration(),
                    dispatcher
            );
            final DisconnectingHttpConnectionContext context =
                    new DisconnectingHttpConnectionContext(configuration);
            final HttpConnectionFiberTask task = HttpConnectionFiberTask.createForTesting(context, dispatcher);
            final long taskIncarnation = task.getIncarnation();
            final CountDownLatch closeComplete = new CountDownLatch(1);
            Thread closeThread = null;
            try {
                task.setScheduleStateForTesting(FiberTask.STATE_IDLE, FiberTask.STATE_ARMING);
                waitProcessor.reschedule(context, taskIncarnation);
                Assert.assertTrue(waitProcessor.runSerially());

                closeThread = new Thread(() -> {
                    try {
                        waitProcessor.close();
                    } finally {
                        closeComplete.countDown();
                    }
                });
                closeThread.start();
                Assert.assertTrue(context.retryCloseAttempted.await(10, TimeUnit.SECONDS));

                Assert.assertTrue(
                        "wait processor close blocked on an arming retry",
                        closeComplete.await(10, TimeUnit.SECONDS)
                );
                Assert.assertEquals(FiberTask.STATE_ARMING_DISCONNECTED, task.getScheduleState());
            } finally {
                if (closeThread != null && closeThread.isAlive()) {
                    if (task.getScheduleState() == FiberTask.STATE_ARMING) {
                        task.setScheduleStateForTesting(FiberTask.STATE_ARMING, FiberTask.STATE_IDLE);
                    }
                    closeThread.join(5_000);
                }
                waitProcessor.close();
                task.closeForTesting();
                context.close();
            }
        });
    }

    @Test
    public void testClearedRetryPublishesPendingWriteInsteadOfReschedule() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final DefaultHttpServerConfiguration configuration =
                    new DefaultHttpServerConfiguration(new DefaultTestCairoConfiguration(root));
            final TestHttpDispatcher dispatcher = new TestHttpDispatcher();
            final WaitProcessor waitProcessor = new WaitProcessor(
                    configuration.getWaitProcessorConfiguration(),
                    dispatcher
            );
            final HttpConnectionContext context = new HttpConnectionContext(configuration, PlainSocketFactory.INSTANCE) {
                @Override
                public boolean handleClientOperation(
                        int operation,
                        HttpRequestProcessorSelector selector,
                        RescheduleContext rescheduleContext
                ) throws PeerIsSlowToReadException, ServerDisconnectException {
                    scheduleRetry(getRejectProcessor(), rescheduleContext);
                    abandonRetry();
                    throw PeerIsSlowToReadException.INSTANCE;
                }
            };
            final HttpConnectionFiberTask task = HttpConnectionFiberTask.createForTesting(
                    context,
                    dispatcher,
                    waitProcessor,
                    null
            );
            try {
                Assert.assertEquals(LaunchResult.LAUNCHED, task.launchForTesting(runtime, IOOperation.READ));
                Assert.assertEquals(1, runtime.drain(8));
                Assert.assertEquals(0, waitProcessor.getRescheduleCount());
                Assert.assertEquals(1, dispatcher.registerCount);
                Assert.assertEquals(IOOperation.WRITE, dispatcher.registeredOperation);
            } finally {
                closeFiberRuntime(runtime);
                waitProcessor.close();
                task.closeForTesting();
                context.close();
            }
        });
    }

    @Test
    public void testCsvImportRetryResumesMultipartOnFiber() throws Exception {
        final HttpQueryTestBuilder builder = new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(2)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder().withFiberEnabled(true))
                .withTelemetry(false);
        TestUtils.assertMemoryLeak(() -> builder
                .run((engine, sqlExecutionContext) -> {
                    try (TestHttpClient testHttpClient = new TestHttpClient()) {
                        testHttpClient.assertGet("{\"ddl\":\"OK\"}", "CREATE TABLE test (a LONG)");

                        final String boundary = "----WebKitFormBoundaryOsOAD9cPKyHuxyBV";
                        final String body = "--" + boundary + "\r\n"
                                + "Content-Disposition: form-data; name=\"data\"\r\n"
                                + "\r\n"
                                + "1\r\n"
                                + "2\r\n"
                                + "3\r\n"
                                + "--" + boundary + "--\r\n";
                        final String importRequest = "POST /upload?fmt=json&name=test HTTP/1.1\r\n"
                                + "Host: localhost:9001\r\n"
                                + "Connection: keep-alive\r\n"
                                + "Content-Length: " + body.length() + "\r\n"
                                + "Content-Type: multipart/form-data; boundary=" + boundary + "\r\n"
                                + "\r\n"
                                + body;

                        final WaitProcessor waitProcessor = builder.getHttpServer().getWaitProcessor();
                        final SOCountDownLatch imported = new SOCountDownLatch(1);
                        final AtomicReference<Throwable> importError = new AtomicReference<>();
                        Thread thread;
                        try (TableWriter ignore = engine.getWriter(engine.verifyTableName("test"), "test")) {
                            // the held writer suspends the import mid-multipart: the parser
                            // state is saved on the context and every due rerun launches on
                            // a fiber, resuming multipart consumption once the writer frees
                            final long parkedBaseline = waitProcessor.getRescheduleCount();
                            thread = new Thread(() -> {
                                try {
                                    new SendAndReceiveRequestBuilder().execute(importRequest, "HTTP/1.1 200 OK");
                                } catch (Throwable th) {
                                    importError.set(th);
                                } finally {
                                    imported.countDown();
                                }
                            });
                            thread.start();
                            // the import parks at least once while the writer is busy
                            TestUtils.assertEventually(() -> Assert.assertTrue(
                                    waitProcessor.getRescheduleCount() > parkedBaseline
                            ));
                            Assert.assertEquals(1, imported.getCount());
                        }
                        Assert.assertTrue(
                                "import did not complete after writer release",
                                imported.await(TimeUnit.SECONDS.toNanos(10))
                        );
                        thread.join();
                        Assert.assertNull(importError.get());
                        testHttpClient.assertGet(
                                "{\"query\":\"SELECT count() cnt FROM test\",\"columns\":[{\"name\":\"cnt\",\"type\":\"LONG\"}],\"timestamp\":-1,\"dataset\":[[3]],\"count\":1}",
                                "SELECT count() cnt FROM test"
                        );
                    }
                }));
    }

    @Test
    public void testAbandonedTaskDisconnectsConnection() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final AtomicInteger abandonedRetryCount = new AtomicInteger();
            DisconnectingHttpConnectionContext context = null;
            HttpConnectionFiberTask task = null;
            try {
                context = new DisconnectingHttpConnectionContext(
                        new DefaultHttpServerConfiguration(new DefaultTestCairoConfiguration(root))
                ) {
                    @Override
                    public void abandonRetry() {
                        abandonedRetryCount.incrementAndGet();
                        super.abandonRetry();
                    }
                };
                final TestHttpDispatcher dispatcher = new TestHttpDispatcher();
                task = HttpConnectionFiberTask.createForTesting(context, dispatcher);

                Assert.assertEquals(
                        LaunchResult.LAUNCHED,
                        task.launchForTesting(runtime, IOOperation.READ)
                );
                runtime.beginQuiesce();
                Assert.assertEquals(1, runtime.drain(8));

                Assert.assertTrue(task.isCancelled());
                Assert.assertEquals(1, abandonedRetryCount.get());
                Assert.assertEquals(1, dispatcher.disconnectCount);
                Assert.assertEquals(IODispatcher.DISCONNECT_REASON_SERVER_SHUTDOWN, dispatcher.disconnectReason);
                Assert.assertEquals(0, runtime.getOutstandingTaskCount());
            } finally {
                closeFiberRuntime(runtime);
                if (task != null) {
                    task.closeForTesting();
                }
                if (context != null) {
                    context.close();
                }
            }
        });
    }

    @Test
    public void testDisconnectsWhenContextDoesNotProvideReason() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            DisconnectingHttpConnectionContext context = null;
            HttpConnectionFiberTask task = null;
            try {
                context = new DisconnectingHttpConnectionContext(
                        new DefaultHttpServerConfiguration(new DefaultTestCairoConfiguration(root))
                );
                context.registerDispatcherDisconnect(-1);
                final TestHttpDispatcher dispatcher = new TestHttpDispatcher();
                task = HttpConnectionFiberTask.createForTesting(context, dispatcher);

                Assert.assertEquals(
                        LaunchResult.LAUNCHED,
                        task.launchForTesting(runtime, IOOperation.READ)
                );
                Assert.assertEquals(1, runtime.drain(8));
                Assert.assertTrue(task.isDone());
                Assert.assertEquals(1, dispatcher.disconnectCount);
                Assert.assertEquals(-1, dispatcher.disconnectReason);
            } finally {
                closeFiberRuntime(runtime);
                if (task != null) {
                    task.closeForTesting();
                }
                if (context != null) {
                    context.close();
                }
            }
        });
    }

    @Test
    public void testQueriesRunOnPooledFibers() throws Exception {
        TestUtils.assertMemoryLeak(() -> new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(2)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder().withFiberEnabled(true))
                .withTelemetry(false)
                .run((engine, sqlExecutionContext) -> {
                    try (TestHttpClient testHttpClient = new TestHttpClient()) {
                        // a plain query end-to-end on a fiber
                        testHttpClient.assertGet(
                                "{\"query\":\"select 42 x\",\"columns\":[{\"name\":\"x\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[42]],\"count\":1}",
                                "select 42 x"
                        );
                        // a parking query: sleep() freezes the fiber on a timer wait;
                        // the timer fires and the frozen fiber resumes through the
                        // network pool's continuation queue to finish the response
                        final long sleepStart = System.nanoTime();
                        testHttpClient.assertGet(
                                "{\"query\":\"select count() cnt from sleep(0.25)\",\"columns\":[{\"name\":\"cnt\",\"type\":\"LONG\"}],\"timestamp\":-1,\"dataset\":[[1]],\"count\":1}",
                                "select count() cnt from sleep(0.25)"
                        );
                        final long sleepMillis = (System.nanoTime() - sleepStart) / 1_000_000;
                        Assert.assertTrue("sleep completed too early: " + sleepMillis + "ms", sleepMillis >= 240);
                        // the same connection keeps reusing its task and the pooled fiber
                        testHttpClient.assertGet(
                                "{\"query\":\"select 43 x\",\"columns\":[{\"name\":\"x\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[43]],\"count\":1}",
                                "select 43 x"
                        );
                    }
                }));
    }

    @Test
    public void testQuiescingDisconnectsConsumedRearmEvent() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(2);
            final DefaultHttpServerConfiguration configuration =
                    new DefaultHttpServerConfiguration(new DefaultTestCairoConfiguration(root));
            final TestHttpDispatcher dispatcher = new TestHttpDispatcher();
            final HttpConnectionContext context = new HttpConnectionContext(configuration, PlainSocketFactory.INSTANCE) {
                @Override
                public boolean handleClientOperation(
                        int operation,
                        HttpRequestProcessorSelector selector,
                        RescheduleContext rescheduleContext
                ) throws PeerIsSlowToWriteException {
                    throw PeerIsSlowToWriteException.INSTANCE;
                }
            };
            final HttpConnectionFiberTask task = HttpConnectionFiberTask.createForTesting(context, dispatcher);
            final Fiber reservedFiber = runtime.tryReserveFiber();
            Assert.assertNotNull(reservedFiber);
            dispatcher.isQuiesceBeforeWake = true;
            dispatcher.reservationEpoch = reservedFiber.getReservationEpoch();
            dispatcher.reservedFiber = reservedFiber;
            dispatcher.runtime = runtime;
            dispatcher.task = task;
            dispatcher.wakeOperation = IOOperation.WRITE;
            try {
                Assert.assertEquals(LaunchResult.LAUNCHED, task.launchForTesting(runtime, IOOperation.READ));
                Assert.assertTrue(runtime.drain(8) > 0);
                Assert.assertEquals(LaunchResult.ALREADY_OWNED, dispatcher.wakeResult);
                Assert.assertTrue(task.isCancelled());
                Assert.assertEquals(1, dispatcher.registerCount);
                Assert.assertEquals(1, dispatcher.disconnectCount);
            } finally {
                closeFiberRuntime(runtime);
                task.closeForTesting();
                context.close();
            }
        });
    }

    @Test
    public void testRequestJobReservesFiberForEachIoEvent() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(2);
            DisconnectingHttpConnectionContext firstContext = null;
            DisconnectingHttpConnectionContext secondContext = null;
            HttpConnectionFiberTask firstTask = null;
            HttpConnectionFiberTask secondTask = null;
            WaitProcessor waitProcessor = null;
            try {
                final DefaultHttpServerConfiguration configuration =
                        new DefaultHttpServerConfiguration(new DefaultTestCairoConfiguration(root));
                firstContext = new DisconnectingHttpConnectionContext(configuration);
                secondContext = new DisconnectingHttpConnectionContext(configuration);
                final BatchingTestHttpDispatcher dispatcher =
                        new BatchingTestHttpDispatcher(firstContext, secondContext);
                waitProcessor = new WaitProcessor(
                        configuration.getWaitProcessorConfiguration(),
                        dispatcher
                );
                firstTask = HttpConnectionFiberTask.createForTesting(firstContext, dispatcher);
                secondTask = HttpConnectionFiberTask.createForTesting(secondContext, dispatcher);

                Assert.assertTrue(HttpServer.runFiberRequestJobForTesting(
                        dispatcher,
                        waitProcessor,
                        runtime
                ));
                Assert.assertEquals(2, runtime.getOutstandingTaskCount());
                Assert.assertEquals(2, runtime.drain(8));
                Assert.assertEquals(0, runtime.getOutstandingTaskCount());
                Assert.assertTrue(firstTask.isDone());
                Assert.assertTrue(secondTask.isDone());
                Assert.assertEquals(2, dispatcher.getDisconnectCount());
            } finally {
                closeFiberRuntime(runtime);
                if (waitProcessor != null) {
                    waitProcessor.close();
                }
                if (firstTask != null) {
                    firstTask.closeForTesting();
                }
                if (secondTask != null) {
                    secondTask.closeForTesting();
                }
                if (firstContext != null) {
                    firstContext.close();
                }
                if (secondContext != null) {
                    secondContext.close();
                }
            }
        });
    }

    @Test
    public void testRequestJobSkipsIoQueueWhenSaturated() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final Fiber heldFiber = runtime.tryReserveFiber();
            Assert.assertNotNull(heldFiber);
            final long heldFiberEpoch = heldFiber.getReservationEpoch();
            WaitProcessor waitProcessor = null;
            try {
                final DefaultHttpServerConfiguration configuration =
                        new DefaultHttpServerConfiguration(new DefaultTestCairoConfiguration(root));
                final SaturatedTestHttpDispatcher dispatcher = new SaturatedTestHttpDispatcher();
                waitProcessor = new WaitProcessor(
                        configuration.getWaitProcessorConfiguration(),
                        dispatcher
                );

                Assert.assertFalse(HttpServer.runFiberRequestJobForTesting(
                        dispatcher,
                        waitProcessor,
                        runtime
                ));
                Assert.assertEquals(0, dispatcher.getProcessCount());
                Assert.assertTrue(dispatcher.hasPendingIOEvents());

                runtime.releaseReservedFiber(heldFiber, heldFiberEpoch);
                Assert.assertFalse(HttpServer.runFiberRequestJobForTesting(
                        dispatcher,
                        waitProcessor,
                        runtime
                ));
                Assert.assertEquals(1, dispatcher.getProcessCount());
                Assert.assertFalse(dispatcher.hasPendingIOEvents());
            } finally {
                if (runtime.getOutstandingTaskCount() > 0) {
                    runtime.releaseReservedFiber(heldFiber, heldFiberEpoch);
                }
                closeFiberRuntime(runtime);
                if (waitProcessor != null) {
                    waitProcessor.close();
                }
            }
        });
    }

    @Test
    public void testRerunWithoutPendingRetryRegistersRead() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final DefaultHttpServerConfiguration configuration =
                    new DefaultHttpServerConfiguration(new DefaultTestCairoConfiguration(root));
            final TestHttpDispatcher dispatcher = new TestHttpDispatcher();
            final HttpConnectionContext context = new HttpConnectionContext(configuration, PlainSocketFactory.INSTANCE) {
                @Override
                public boolean tryRerun(
                        HttpRequestProcessorSelector selector,
                        RescheduleContext rescheduleContext
                ) {
                    return true;
                }
            };
            final HttpConnectionFiberTask task = HttpConnectionFiberTask.createForTesting(context, dispatcher);
            try {
                Assert.assertEquals(LaunchResult.LAUNCHED, task.launchRerunForTesting(runtime));
                Assert.assertEquals(1, runtime.drain(8));
                Assert.assertEquals(1, dispatcher.registerCount);
                Assert.assertEquals(IOOperation.READ, dispatcher.registeredOperation);
            } finally {
                closeFiberRuntime(runtime);
                task.closeForTesting();
                context.close();
            }
        });
    }

    @Test
    public void testWorkerPoolModeControlsFiberExecution() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            assertQueryExecutionMode(false, WorkerPoolMode.LEGACY, false);
            assertQueryExecutionMode(false, WorkerPoolMode.FIBER_HOST, false);
            assertQueryExecutionMode(true, WorkerPoolMode.LEGACY, false);
            assertQueryExecutionMode(true, WorkerPoolMode.FIBER_HOST, true);
        });
    }

    @Test
    public void testResourceFailureRereadsTaskStateAfterArmingRace() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CountDownLatch reservationStarted = new CountDownLatch(1);
            final CountDownLatch allowResourceFailure = new CountDownLatch(1);
            final CountDownLatch stateRead = new CountDownLatch(1);
            final CountDownLatch continueResolution = new CountDownLatch(1);
            final AtomicReference<Throwable> error = new AtomicReference<>();
            final AtomicReference<LaunchResult> result = new AtomicReference<>();
            final FiberRuntime runtime = new FiberRuntime(1, 1, () -> {
                reservationStarted.countDown();
                try {
                    allowResourceFailure.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                throw new IllegalStateException("forced fiber acquire failure");
            });
            final HttpConnectionFiberTask task = HttpConnectionFiberTask.createForTesting(null, null, () -> {
                stateRead.countDown();
                try {
                    continueResolution.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            });

            Thread launchThread = null;
            try {
                launchThread = new Thread(() -> {
                    try {
                        result.set(task.launchForTesting(runtime, IOOperation.READ));
                    } catch (Throwable th) {
                        error.set(th);
                    }
                });
                launchThread.start();
                Assert.assertTrue(reservationStarted.await(5, TimeUnit.SECONDS));
                Assert.assertEquals(1, runtime.getOutstandingTaskCount());
                task.setScheduleStateForTesting(FiberTask.STATE_IDLE, FiberTask.STATE_ARMING);
                allowResourceFailure.countDown();
                Assert.assertTrue(stateRead.await(5, TimeUnit.SECONDS));
                task.setScheduleStateForTesting(FiberTask.STATE_ARMING, FiberTask.STATE_IDLE);
                continueResolution.countDown();
                launchThread.join();

                Assert.assertNull(error.get());
                Assert.assertEquals(LaunchResult.RESOURCE_FAILURE, result.get());
                Assert.assertTrue(task.isCancelled());
            } finally {
                allowResourceFailure.countDown();
                continueResolution.countDown();
                if (launchThread != null) {
                    launchThread.join();
                }
                task.closeForTesting();
                runtime.beginQuiesce();
                final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                Assert.assertTrue(runtime.awaitClosed(deadline));
                Assert.assertEquals(FiberRuntimeState.CLOSED, runtime.state());
                runtime.closeAfterDrained();
            }
        });
    }

    @Test
    public void testSaturationKeepsIoEventQueued() throws Exception {
        TestUtils.assertMemoryLeak(() -> new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(2)
                .withHttpServerConfigBuilder(
                        new HttpServerConfigurationBuilder()
                                .withFiberEnabled(true)
                                .withFiberMaxLiveCount(1)
                )
                .withTelemetry(false)
                .run((engine, sqlExecutionContext) -> {
                    final CountDownLatch completed = new CountDownLatch(2);
                    final AtomicReference<Throwable> error = new AtomicReference<>();
                    final Thread sleepingQuery = new Thread(() -> {
                        try (TestHttpClient client = new TestHttpClient()) {
                            client.assertGet(
                                    "{\"query\":\"SELECT count() cnt FROM sleep(1.0)\",\"columns\":[{\"name\":\"cnt\",\"type\":\"LONG\"}],\"timestamp\":-1,\"dataset\":[[1]],\"count\":1}",
                                    "SELECT count() cnt FROM sleep(1.0)"
                            );
                        } catch (Throwable th) {
                            error.compareAndSet(null, th);
                        } finally {
                            completed.countDown();
                        }
                    });
                    sleepingQuery.start();

                    final LongList queryIds = new LongList();
                    TestUtils.assertEventually(() -> {
                        engine.getQueryRegistry().getEntryIds(queryIds);
                        Assert.assertEquals(1, queryIds.size());
                    });

                    final Thread queuedQuery = new Thread(() -> {
                        try (TestHttpClient client = new TestHttpClient()) {
                            client.assertGet(
                                    "{\"query\":\"SELECT 42 x\",\"columns\":[{\"name\":\"x\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[42]],\"count\":1}",
                                    "SELECT 42 x"
                            );
                        } catch (Throwable th) {
                            error.compareAndSet(null, th);
                        } finally {
                            completed.countDown();
                        }
                    });
                    queuedQuery.start();

                    Assert.assertTrue(completed.await(10, TimeUnit.SECONDS));
                    sleepingQuery.join();
                    queuedQuery.join();
                    Assert.assertNull(error.get());
                }));
    }

    private void assertBindingAfterSelectorReuse(WorkerPoolMode workerPoolMode) throws Exception {
        final DefaultTestCairoConfiguration cairoConfiguration = new DefaultTestCairoConfiguration(root);
        final DefaultHttpServerConfiguration httpConfiguration = new HttpServerConfigurationBuilder()
                .withBaseDir(root)
                .withFiberEnabled(true)
                .withWorkerCount(1)
                .build(cairoConfiguration);
        final WorkerPoolConfiguration workerPoolConfiguration = new WorkerPoolConfiguration() {
            @Override
            public Metrics getMetrics() {
                return Metrics.DISABLED;
            }

            @Override
            public int getWorkerCount() {
                return 1;
            }

            @Override
            public WorkerPoolMode getWorkerPoolMode() {
                return workerPoolMode;
            }
        };
        try (
                CairoEngine engine = new CairoEngine(cairoConfiguration);
                WorkerPool workerPool = new TestWorkerPool(workerPoolConfiguration);
                HttpServer httpServer = new HttpServer(httpConfiguration, workerPool, PlainSocketFactory.INSTANCE)
        ) {
            httpServer.bind(createJsonQueryFactory("/query", httpConfiguration, engine));
            workerPool.start();
            try {
                try (TestHttpClient testHttpClient = new TestHttpClient()) {
                    testHttpClient.assertGet(
                            "{\"query\":\"SELECT 41 x\",\"columns\":[{\"name\":\"x\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[41]],\"count\":1}",
                            "SELECT 41 x"
                    );
                    httpServer.bind(createJsonQueryFactory("/query2", httpConfiguration, engine));
                    testHttpClient.assertGet(
                            "/query2",
                            "{\"query\":\"SELECT 42 x\",\"columns\":[{\"name\":\"x\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[42]],\"count\":1}",
                            "SELECT 42 x"
                    );
                }
            } finally {
                workerPool.halt();
            }
        }
    }

    private void assertHttpQueryInstallsEngineTimerShards(boolean isServicesConstruction) throws Exception {
        final DefaultTestCairoConfiguration cairoConfiguration = new DefaultTestCairoConfiguration(root);
        final DefaultHttpServerConfiguration httpConfiguration = new HttpServerConfigurationBuilder()
                .withBaseDir(root)
                .withFiberEnabled(true)
                .withPort(0)
                .withWorkerCount(1)
                .build(cairoConfiguration);
        final ServerConfiguration serverConfiguration = new DefaultServerConfiguration(root) {
            @Override
            public HttpFullFatServerConfiguration getHttpServerConfiguration() {
                return httpConfiguration;
            }
        };
        final AtomicReference<TimerShards> observedTimerShards = new AtomicReference<>();
        final CountDownLatch observed = new CountDownLatch(1);
        try (
                CairoEngine engine = new CairoEngine(cairoConfiguration);
                WorkerPool workerPool = new TestWorkerPool(fiberHostConfiguration(1));
                HttpServer httpServer = isServicesConstruction
                        ? Services.INSTANCE.createHttpServer(serverConfiguration, engine, workerPool, 1)
                        : new HttpServer(httpConfiguration, workerPool, PlainSocketFactory.INSTANCE)
        ) {
            httpServer.bind(new HttpRequestHandlerFactory() {
                @Override
                public ObjHashSet<String> getUrls() {
                    final ObjHashSet<String> urls = new ObjHashSet<>();
                    urls.add("/timer-query");
                    return urls;
                }

                @Override
                public HttpRequestHandler newInstance() {
                    return new JsonQueryProcessor(
                            httpConfiguration.getJsonQueryProcessorConfiguration(),
                            engine,
                            1
                    ) {
                        @Override
                        public void execute0(JsonQueryProcessorState state)
                                throws PeerDisconnectedException, PeerIsSlowToReadException {
                            try {
                                super.execute0(state);
                            } finally {
                                observedTimerShards.set(
                                        SuspensionScope.getTimerShards(SuspensionScope.scope())
                                );
                                observed.countDown();
                            }
                        }
                    };
                }
            });
            workerPool.start();
            try {
                try (TestHttpClient testHttpClient = new TestHttpClient()) {
                    testHttpClient.assertGet(
                            "/timer-query",
                            "{\"query\":\"SELECT 42 x\",\"columns\":[{\"name\":\"x\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[42]],\"count\":1}",
                            "SELECT 42 x",
                            "localhost",
                            httpServer.getPort(),
                            null,
                            null,
                            null
                    );
                }
                Assert.assertTrue(observed.await(10, TimeUnit.SECONDS));
                Assert.assertSame(engine.getTimerShards(), observedTimerShards.get());
            } finally {
                workerPool.halt();
            }
        }
    }

    private void assertQueryExecutionMode(
            boolean isFiberEnabled,
            WorkerPoolMode workerPoolMode,
            boolean isFiberExecutionExpected
    ) throws Exception {
        final DefaultTestCairoConfiguration cairoConfiguration = new DefaultTestCairoConfiguration(root);
        final DefaultHttpServerConfiguration httpConfiguration = new HttpServerConfigurationBuilder()
                .withBaseDir(root)
                .withFiberEnabled(isFiberEnabled)
                .withWorkerCount(1)
                .build(cairoConfiguration);
        final WorkerPoolConfiguration workerPoolConfiguration = new WorkerPoolConfiguration() {
            @Override
            public Metrics getMetrics() {
                return Metrics.DISABLED;
            }

            @Override
            public int getWorkerCount() {
                return 1;
            }

            @Override
            public WorkerPoolMode getWorkerPoolMode() {
                return workerPoolMode;
            }
        };
        try (
                CairoEngine engine = new CairoEngine(cairoConfiguration);
                WorkerPool workerPool = new TestWorkerPool(workerPoolConfiguration);
                HttpServer httpServer = new HttpServer(httpConfiguration, workerPool, PlainSocketFactory.INSTANCE)
        ) {
            httpServer.bind(new HttpRequestHandlerFactory() {
                @Override
                public ObjHashSet<String> getUrls() {
                    final ObjHashSet<String> urls = new ObjHashSet<>();
                    urls.add("/query");
                    return urls;
                }

                @Override
                public HttpRequestHandler newInstance() {
                    return new JsonQueryProcessor(
                            httpConfiguration.getJsonQueryProcessorConfiguration(),
                            engine,
                            1
                    );
                }
            });
            workerPool.start();
            try {
                try (TestHttpClient testHttpClient = new TestHttpClient()) {
                    testHttpClient.assertGet(
                            "{\"query\":\"SELECT 42 x\",\"columns\":[{\"name\":\"x\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[42]],\"count\":1}",
                            "SELECT 42 x"
                    );
                }

                Assert.assertEquals(workerPoolMode, workerPool.getWorkerPoolMode());
                Assert.assertEquals(
                        isFiberEnabled ? WorkerPoolMode.FIBER_HOST : WorkerPoolMode.LEGACY,
                        httpConfiguration.getWorkerPoolMode()
                );
                if (workerPoolMode == WorkerPoolMode.FIBER_HOST) {
                    Assert.assertEquals(
                            isFiberExecutionExpected,
                            workerPool.getFiberRuntime().getLaunchCount(LaunchResult.LAUNCHED) > 0
                    );
                    Assert.assertEquals(
                            isFiberExecutionExpected,
                            workerPool.getFiberRuntime().getCreatedFiberCount() > 0
                    );
                }
            } finally {
                workerPool.halt();
            }
        }
    }

    private void closeFiberRuntime(FiberRuntime runtime) {
        runtime.beginQuiesce();
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (runtime.state() != FiberRuntimeState.CLOSED && System.nanoTime() < deadline) {
            runtime.drain(64);
        }
        Assert.assertTrue(runtime.awaitClosed(deadline));
        runtime.closeAfterDrained();
    }

    private static HttpRequestHandlerFactory closeTrackingHandlerFactory(
            String url,
            AtomicInteger closeCount,
            boolean isCloseFailing
    ) {
        final ObjHashSet<String> urls = new ObjHashSet<>();
        urls.add(url);
        return new HttpRequestHandlerFactory() {
            @Override
            public ObjHashSet<String> getUrls() {
                return urls;
            }

            @Override
            public HttpRequestHandler newInstance() {
                return new CloseTrackingHttpRequestHandler(closeCount, isCloseFailing);
            }
        };
    }

    private static HttpRequestHandlerFactory createJsonQueryFactory(
            String url,
            DefaultHttpServerConfiguration httpConfiguration,
            CairoEngine engine
    ) {
        return new HttpRequestHandlerFactory() {
            @Override
            public ObjHashSet<String> getUrls() {
                final ObjHashSet<String> urls = new ObjHashSet<>();
                urls.add(url);
                return urls;
            }

            @Override
            public HttpRequestHandler newInstance() {
                return new JsonQueryProcessor(
                        httpConfiguration.getJsonQueryProcessorConfiguration(),
                        engine,
                        1
                );
            }
        };
    }

    private static WorkerPoolConfiguration fiberHostConfiguration(int maxLiveFiberCount) {
        return new WorkerPoolConfiguration() {
            @Override
            public int getFiberMaxLiveCount() {
                return maxLiveFiberCount;
            }

            @Override
            public int getFiberRetainedCount() {
                return 1;
            }

            @Override
            public Metrics getMetrics() {
                return Metrics.DISABLED;
            }

            @Override
            public int getWorkerCount() {
                return 1;
            }

            @Override
            public WorkerPoolMode getWorkerPoolMode() {
                return WorkerPoolMode.FIBER_HOST;
            }
        };
    }

    private static class CloseTrackingHttpRequestHandler implements HttpRequestHandler, QuietCloseable {
        private final AtomicInteger closeCount;
        private final boolean isCloseFailing;

        private CloseTrackingHttpRequestHandler(AtomicInteger closeCount, boolean isCloseFailing) {
            this.closeCount = closeCount;
            this.isCloseFailing = isCloseFailing;
        }

        @Override
        public void close() {
            closeCount.incrementAndGet();
            if (isCloseFailing) {
                throw new RuntimeException("injected handler close failure");
            }
        }

        @Override
        public HttpRequestProcessor getProcessor(HttpRequestHeader requestHeader) {
            return null;
        }
    }

    private static class BatchingTestHttpDispatcher extends TestHttpDispatcher {
        private final HttpConnectionContext firstContext;
        private boolean hasPendingEvents = true;
        private final HttpConnectionContext secondContext;

        private BatchingTestHttpDispatcher(
                HttpConnectionContext firstContext,
                HttpConnectionContext secondContext
        ) {
            this.firstContext = firstContext;
            this.secondContext = secondContext;
        }

        @Override
        public boolean hasPendingIOEvents() {
            return hasPendingEvents;
        }

        @Override
        public boolean processIOQueue(IORequestProcessor<HttpConnectionContext> processor) {
            hasPendingEvents = false;
            return processor.onRequest(IOOperation.READ, firstContext, this)
                    | processor.onRequest(IOOperation.READ, secondContext, this);
        }
    }

    private static class DisconnectingHttpConnectionContext extends HttpConnectionContext {
        private final CountDownLatch retryCloseAttempted = new CountDownLatch(1);

        private DisconnectingHttpConnectionContext(DefaultHttpServerConfiguration configuration) {
            super(configuration, PlainSocketFactory.INSTANCE);
        }

        @Override
        public boolean claimRetryClose(long taskIncarnation) {
            retryCloseAttempted.countDown();
            return super.claimRetryClose(taskIncarnation);
        }

        @Override
        public boolean handleClientOperation(
                int operation,
                HttpRequestProcessorSelector selector,
                RescheduleContext rescheduleContext
        ) throws ServerDisconnectException {
            throw ServerDisconnectException.INSTANCE;
        }
    }

    private static class SaturatedTestHttpDispatcher extends TestHttpDispatcher {
        private int processCount;

        public int getProcessCount() {
            return processCount;
        }

        @Override
        public boolean hasPendingIOEvents() {
            return processCount == 0;
        }

        @Override
        public boolean processIOQueue(IORequestProcessor<HttpConnectionContext> processor) {
            processCount++;
            return false;
        }
    }

    private static class TestHttpDispatcher implements IODispatcher<HttpConnectionContext> {
        private int disconnectCount;
        private int disconnectReason;
        private boolean isQuiesceBeforeWake;
        private int registerCount;
        private int registeredOperation = -1;
        private long reservationEpoch;
        private Fiber reservedFiber;
        private FiberRuntime runtime;
        private HttpConnectionFiberTask task;
        private int wakeOperation;
        private LaunchResult wakeResult;

        @Override
        public void close() {
        }

        @Override
        public void disconnect(HttpConnectionContext context, int reason) {
            disconnectCount++;
            disconnectReason = reason;
        }

        @Override
        public int getConnectionCount() {
            return 0;
        }

        public int getDisconnectCount() {
            return disconnectCount;
        }

        @Override
        public int getPort() {
            return 0;
        }

        @Override
        public boolean isListening() {
            return false;
        }

        @Override
        public boolean processIOQueue(IORequestProcessor<HttpConnectionContext> processor) {
            return false;
        }

        @Override
        public void registerChannel(HttpConnectionContext context, int operation) {
            registerCount++;
            registeredOperation = operation;
            if (wakeOperation != 0) {
                final int nextOperation = wakeOperation;
                wakeOperation = 0;
                if (isQuiesceBeforeWake) {
                    runtime.beginQuiesce();
                }
                if (reservedFiber != null) {
                    wakeResult = task.launchReservedForTesting(
                            runtime,
                            reservedFiber,
                            reservationEpoch,
                            nextOperation
                    );
                    reservedFiber = null;
                } else {
                    wakeResult = task.launchForTesting(runtime, nextOperation);
                }
            }
        }

        @Override
        public boolean run(Job.WorkerContext workerContext) {
            return false;
        }
    }
}
