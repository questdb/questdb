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

package io.questdb.test.cutlass.parquet;

import io.questdb.PropertyKey;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.parquet.CopyExportRequestJob;
import io.questdb.cutlass.text.CopyExportContext;
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.mp.continuation.SuspensionScope;
import io.questdb.std.FilesFacade;
import io.questdb.std.PerQueryMemoryTrackerProvider;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class CopyExportRequestJobTest extends AbstractCairoTest {

    @BeforeClass
    public static void setUpStatic() throws Exception {
        exportRoot = TestUtils.unchecked(() -> temp.newFolder("export").getAbsolutePath());
        inputRoot = exportRoot;
        staticOverrides.setProperty(PropertyKey.CAIRO_SQL_COPY_ROOT, exportRoot);
        staticOverrides.setProperty(PropertyKey.CAIRO_SQL_COPY_EXPORT_ROOT, exportRoot);
        AbstractCairoTest.setUpStatic();
    }

    @Override
    @Before
    public void setUp() {
        super.setUp();
        node1.setProperty(PropertyKey.CAIRO_SQL_COPY_EXPORT_ROOT, exportRoot);
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path path = new Path()) {
            path.of(exportRoot).$();
            if (ff.exists(path.$())) {
                ff.rmdir(path);
            }
        }
    }

    @Test
    public void testCancelledBeforeFiberDrainReleasesRequest() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicInteger callbackCount = new AtomicInteger();
            final CopyExportContext copyContext = engine.getCopyExportContext();
            final FiberRuntime runtime = new FiberRuntime(1);
            final CopyExportRequestJob job = new CopyExportRequestJob(
                    engine,
                    () -> {
                        callbackCount.incrementAndGet();
                        return null;
                    },
                    null,
                    runtime
            );
            try {
                publishCopy("COPY (SELECT 1 AS x) TO 'cancelled_before_drain' WITH FORMAT parquet");
                final long copyId = copyContext.getActiveExportId();
                Assert.assertNotEquals(CopyExportContext.INACTIVE_COPY_ID, copyId);
                Assert.assertNotNull(copyContext.getEntry(copyId));

                final PerQueryMemoryTrackerProvider trackerProvider =
                        (PerQueryMemoryTrackerProvider) engine.getMemoryTrackerProvider();
                final int pooledTrackerCount = trackerProvider.getPooledCount();

                Assert.assertTrue(job.run());
                Assert.assertEquals(1, runtime.getLaunchCount(LaunchResult.LAUNCHED));
                Assert.assertTrue(copyContext.cancel(copyId, null));
                Assert.assertEquals(1, runtime.drain(1));

                Assert.assertEquals(0, callbackCount.get());
                Assert.assertNull(copyContext.getEntry(copyId));
                Assert.assertEquals(pooledTrackerCount, trackerProvider.getPooledCount());
                assertExportDoesNotExist("cancelled_before_drain.parquet");
                assertQuery("""
                        SELECT status
                        FROM "sys.copy_export_log"
                        WHERE status = 'cancelled'
                        """)
                        .noLeakCheck()
                        .returns("""
                                status
                                cancelled
                                """);
            } finally {
                close(runtime);
                job.close();
            }
        });
    }

    @Test
    public void testCloseCancelsQueuedExport() throws Exception {
        assertMemoryLeak(() -> {
            final CopyExportRequestJob job = new CopyExportRequestJob(engine);
            try {
                publishCopy("COPY (SELECT 1 AS x) TO 'legacy_queued' WITH FORMAT parquet");
                job.close();

                assertQuery("""
                        SELECT status
                        FROM "sys.copy_export_log"
                        WHERE status = 'cancelled'
                        """)
                        .noLeakCheck()
                        .returns("""
                                status
                                cancelled
                                """);
                assertExportDoesNotExist("legacy_queued.parquet");
            } finally {
                job.close();
            }
        });
    }

    @Test
    public void testConstructionFailureLeavesQueuedExportAvailable() throws Exception {
        assertMemoryLeak(() -> {
            publishCopy("COPY (SELECT 1 AS x) TO 'queued' WITH FORMAT parquet");

            try {
                new CopyExportRequestJob(
                        engine,
                        null,
                        () -> {
                            throw new IllegalStateException("expected exporter construction failure");
                        }
                );
                Assert.fail();
            } catch (IllegalStateException e) {
                TestUtils.assertContains(e.getMessage(), "expected exporter construction failure");
            }

            try (CopyExportRequestJob job = new CopyExportRequestJob(engine)) {
                Assert.assertTrue(job.run());
                assertExportRowCount("queued.parquet", 1);
            }
        });
    }

    @Test
    public void testContainsSecretSurvivesFiberHandoffAndResetsOnReuse() throws Exception {
        assertMemoryLeak(() -> {
            final CopyExportContext copyContext = engine.getCopyExportContext();
            final AtomicReference<CopyExportContext.ExportTaskEntry> queuedEntry = new AtomicReference<>();
            final FiberRuntime runtime = new FiberRuntime(1);
            final CopyExportRequestJob job = new CopyExportRequestJob(
                    engine,
                    () -> {
                        final CopyExportContext.ExportTaskEntry entry = queuedEntry.get();
                        Assert.assertSame(entry, copyContext.getEntry(entry.getId()));
                        Assert.assertTrue(entry.containsSecret());
                        return null;
                    },
                    null,
                    runtime
            );
            CopyExportContext.ExportTaskEntry reusedEntry = null;
            try {
                final boolean previousContainsSecret = sqlExecutionContext.containsSecret();
                sqlExecutionContext.containsSecret(true);
                try {
                    publishCopy("COPY (SELECT 1 AS x) TO 'contains_secret' WITH FORMAT parquet");
                } finally {
                    sqlExecutionContext.containsSecret(previousContainsSecret);
                }

                final long copyId = copyContext.getActiveExportId();
                final CopyExportContext.ExportTaskEntry entry = copyContext.getEntry(copyId);
                Assert.assertNotNull(entry);
                Assert.assertTrue(entry.containsSecret());
                queuedEntry.set(entry);

                Assert.assertTrue(job.run());
                Assert.assertSame(entry, copyContext.getEntry(copyId));
                Assert.assertTrue(entry.containsSecret());
                Assert.assertEquals(1, runtime.drain(1));

                Assert.assertNull(copyContext.getEntry(copyId));
                assertExportRowCount("contains_secret.parquet", 1);

                reusedEntry = copyContext.assignExportEntry(
                        sqlExecutionContext.getSecurityContext(),
                        "next export",
                        "",
                        null,
                        CopyExportContext.CopyTrigger.HTTP
                );
                Assert.assertSame(entry, reusedEntry);
                Assert.assertFalse(reusedEntry.containsSecret());
            } finally {
                if (reusedEntry != null) {
                    copyContext.releaseEntry(reusedEntry);
                }
                close(runtime);
                job.close();
            }
        });
    }

    @Test
    public void testFiberLaunchSaturationRetainsRequestForRetry() throws Exception {
        assertMemoryLeak(() -> {
            final CopyExportContext copyContext = engine.getCopyExportContext();
            final FiberRuntime runtime = new FiberRuntime(1);
            final CopyExportRequestJob job = new CopyExportRequestJob(engine, runtime);
            Fiber reservedFiber = null;
            try {
                reservedFiber = runtime.tryReserveFiber();
                Assert.assertNotNull(reservedFiber);
                final long reservationEpoch = reservedFiber.getReservationEpoch();

                publishCopy("COPY (SELECT 1 AS x) TO 'saturated_retry' WITH FORMAT parquet");
                final long copyId = copyContext.getActiveExportId();
                final CopyExportContext.ExportTaskEntry entry = copyContext.getEntry(copyId);
                Assert.assertNotNull(entry);

                Assert.assertTrue(job.run());
                Assert.assertEquals(1, runtime.getLaunchCount(LaunchResult.SATURATED));
                Assert.assertSame(entry, copyContext.getEntry(copyId));
                assertExportDoesNotExist("saturated_retry.parquet");

                runtime.releaseReservedFiber(reservedFiber, reservationEpoch);
                reservedFiber = null;

                Assert.assertFalse(job.run());
                Assert.assertEquals(1, runtime.getLaunchCount(LaunchResult.LAUNCHED));
                Assert.assertSame(entry, copyContext.getEntry(copyId));
                Assert.assertEquals(1, runtime.drain(1));

                Assert.assertNull(copyContext.getEntry(copyId));
                Assert.assertEquals(0, runtime.getOutstandingTaskCount());
                assertExportRowCount("saturated_retry.parquet", 1);
            } finally {
                if (reservedFiber != null) {
                    runtime.releaseReservedFiber(reservedFiber, reservedFiber.getReservationEpoch());
                }
                close(runtime);
                job.close();
            }
        });
    }

    @Test
    public void testRequestFailureLeavesSuspensionModeUntouched() throws Exception {
        assertMemoryLeak(() -> {
            publishCopy("COPY (SELECT 1 AS x) TO 'failed' WITH FORMAT parquet");
            try (CopyExportRequestJob job = new CopyExportRequestJob(
                    engine,
                    () -> {
                        Assert.assertEquals(SuspensionScope.Mode.BLOCKING, SuspensionScope.getMode());
                        throw new IllegalStateException("expected callback failure");
                    }
            )) {
                final SuspensionScope.Mode previousMode = SuspensionScope.enter(SuspensionScope.Mode.BLOCKING);
                try {
                    Assert.assertTrue(job.run());
                    Assert.assertEquals(SuspensionScope.Mode.BLOCKING, SuspensionScope.getMode());
                } finally {
                    SuspensionScope.restore(previousMode);
                }
                assertExportDoesNotExist("failed.parquet");
            }
        });
    }

    @Test
    public void testRunsSuspendableExportSynchronously() throws Exception {
        assertMemoryLeak(() -> {
            final CopyExportRequestJob job = new CopyExportRequestJob(engine);
            try {
                publishCopy("COPY (SELECT * FROM sleep(0.001)) TO 'legacy' WITH FORMAT parquet");
                Assert.assertTrue(job.run());
                assertExportRowCount("legacy.parquet", 1);
            } finally {
                job.close();
            }
        });
    }

    @Test
    public void testRunsExportsOnConfiguredFiberRuntime() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicInteger callbackCount = new AtomicInteger();
            final FiberRuntime runtime = new FiberRuntime(1);
            final CopyExportRequestJob job = new CopyExportRequestJob(
                    engine,
                    () -> {
                        Assert.assertTrue(Fiber.isMounted());
                        callbackCount.incrementAndGet();
                        return null;
                    },
                    null,
                    runtime
            );
            try {
                publishCopy("COPY (SELECT 1 AS x) TO 'fiber_1' WITH FORMAT parquet");
                Assert.assertTrue(job.run());
                Assert.assertEquals(0, callbackCount.get());
                assertExportDoesNotExist("fiber_1.parquet");

                Assert.assertEquals(1, runtime.drain(1));
                Assert.assertEquals(1, callbackCount.get());
                assertExportRowCount("fiber_1.parquet", 1);

                publishCopy("COPY (SELECT 2 AS x) TO 'fiber_2' WITH FORMAT parquet");
                Assert.assertTrue(job.run());
                Assert.assertEquals(1, runtime.drain(1));
                Assert.assertEquals(2, callbackCount.get());
                assertExportRowCount("fiber_2.parquet", 1);
            } finally {
                close(runtime);
                job.close();
            }
        });
    }

    private void assertExportDoesNotExist(CharSequence fileName) {
        try (Path path = new Path()) {
            path.of(exportRoot).concat(fileName).$();
            Assert.assertFalse(configuration.getFilesFacade().exists(path.$()));
        }
    }

    private static void close(FiberRuntime runtime) {
        runtime.beginQuiesce();
        final long deadline = System.nanoTime() + 5_000_000_000L;
        while (runtime.state() != FiberRuntimeState.CLOSED && System.nanoTime() < deadline) {
            runtime.drain(64);
        }
        Assert.assertTrue(runtime.awaitClosed(deadline));
        Assert.assertEquals(0, runtime.getInlineSuspendViolationCount());
        runtime.closeAfterDrained();
    }

    private void assertExportRowCount(CharSequence fileName, long expected) throws Exception {
        try (Path path = new Path()) {
            path.of(exportRoot).concat(fileName).$();
            assertQuery("SELECT count() FROM read_parquet('" + path + "')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n" + expected + '\n');
        }
    }

    private void publishCopy(CharSequence sql) throws Exception {
        final SuspensionScope.Mode previousMode = SuspensionScope.enter(
                SuspensionScope.Mode.BLOCKING
        );
        try (
                RecordCursorFactory factory = select(sql);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            Assert.assertTrue(cursor.hasNext());
            Assert.assertNotNull(cursor.getRecord().getStrA(0));
            Assert.assertFalse(cursor.hasNext());
        } finally {
            SuspensionScope.restore(previousMode);
        }
    }
}
