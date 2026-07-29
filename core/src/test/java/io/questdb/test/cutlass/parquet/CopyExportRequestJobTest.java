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
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.mp.continuation.SuspensionScope;
import io.questdb.std.FilesFacade;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class CopyExportRequestJobTest extends AbstractCairoTest {

    @BeforeClass
    public static void setUpStatic() throws Exception {
        exportRoot = TestUtils.unchecked(() -> temp.newFolder("export").getAbsolutePath());
        inputRoot = exportRoot;
        staticOverrides.setProperty(PropertyKey.CAIRO_SQL_COPY_ROOT, exportRoot);
        staticOverrides.setProperty(PropertyKey.CAIRO_SQL_COPY_EXPORT_ROOT, exportRoot);
        AbstractCairoTest.setUpStatic();
    }

    private static void closeRuntime(FiberRuntime runtime) {
        runtime.beginQuiesce();
        drainUntilClosed(runtime);
        runtime.closeAfterDrained();
    }

    private static void drainUntilClosed(FiberRuntime runtime) {
        final long deadline = System.nanoTime() + 5_000_000_000L;
        while (runtime.state() != FiberRuntimeState.CLOSED && System.nanoTime() < deadline) {
            if (runtime.drain(64) == 0) {
                Os.sleep(1);
            }
        }
        Assert.assertEquals(FiberRuntimeState.CLOSED, runtime.state());
    }

    private static void drainUntilIdle(FiberRuntime runtime) {
        final long deadline = System.nanoTime() + 5_000_000_000L;
        while (runtime.getOutstandingTaskCount() != 0 && System.nanoTime() < deadline) {
            if (runtime.drain(64) == 0) {
                Os.sleep(1);
            }
        }
        Assert.assertEquals(0, runtime.getOutstandingTaskCount());
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
    public void testFiberHostCancelsParkedExportOnQuiesce() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final CopyExportRequestJob job = new CopyExportRequestJob(engine, runtime);
            try {
                publishCopy("COPY (SELECT * FROM sleep(60.0)) TO 'parked' WITH FORMAT parquet");
                Assert.assertTrue(job.run());
                Assert.assertEquals(1, runtime.drain(1));
                Assert.assertEquals(1, runtime.getOutstandingTaskCount());

                runtime.beginQuiesce();
                drainUntilClosed(runtime);

                assertQuery("""
                        SELECT status
                        FROM "sys.copy_export_log"
                        WHERE status = 'cancelled'
                        """)
                        .returns("""
                                status
                                cancelled
                                """);
                assertExportDoesNotExist("parked.parquet");
            } finally {
                try {
                    closeRuntime(runtime);
                } finally {
                    job.close();
                }
            }
        });
    }

    @Test
    public void testFiberHostRejectsLaunchAfterQuiesceAndReleasesTask() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final CopyExportRequestJob job = new CopyExportRequestJob(engine, runtime);
            try {
                runtime.beginQuiesce();

                publishCopy("COPY (SELECT 1 AS x) TO 'rejected_one' WITH FORMAT parquet");
                Assert.assertTrue(job.run());
                publishCopy("COPY (SELECT 2 AS x) TO 'rejected_two' WITH FORMAT parquet");
                Assert.assertTrue(job.run());

                assertQuery("""
                        SELECT count()
                        FROM "sys.copy_export_log"
                        WHERE status = 'cancelled'
                        """)
                        .expectSize()
                        .noRandomAccess()
                        .returns("""
                                count
                                2
                                """);
                Assert.assertEquals(0, runtime.getOutstandingTaskCount());
            } finally {
                try {
                    closeRuntime(runtime);
                } finally {
                    job.close();
                }
            }
        });
    }

    @Test
    public void testFiberHostSuspendsAndReusesFiber() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final CopyExportRequestJob job = new CopyExportRequestJob(engine, runtime);
            try {
                publishCopy("COPY (SELECT * FROM sleep(0.05)) TO 'fiber_one' WITH FORMAT parquet");
                Assert.assertTrue(job.run());
                Assert.assertEquals(1, runtime.getQueuedCount());
                Assert.assertEquals(1, runtime.getOutstandingTaskCount());

                Assert.assertEquals(1, runtime.drain(1));
                Assert.assertEquals(1, runtime.getOutstandingTaskCount());
                drainUntilIdle(runtime);
                Assert.assertEquals(1, runtime.getCreatedFiberCount());
                assertExportRowCount("fiber_one.parquet", 1);

                publishCopy("COPY (SELECT 2 AS x) TO 'fiber_two' WITH FORMAT parquet");
                Assert.assertTrue(job.run());
                drainUntilIdle(runtime);
                Assert.assertEquals(1, runtime.getCreatedFiberCount());
                assertExportRowCount("fiber_two.parquet", 1);
            } finally {
                try {
                    closeRuntime(runtime);
                } finally {
                    job.close();
                }
            }
        });
    }

    @Test
    public void testFiberSaturationLeavesExportRequestQueued() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final CopyExportRequestJob job = new CopyExportRequestJob(engine, runtime);
            Fiber heldFiber = null;
            try {
                publishCopy("COPY (SELECT 1 AS x) TO 'saturated' WITH FORMAT parquet");
                heldFiber = runtime.tryReserveFiber();
                Assert.assertNotNull(heldFiber);

                Assert.assertFalse(job.run());
                Assert.assertEquals(1, runtime.getOutstandingTaskCount());
                assertExportDoesNotExist("saturated.parquet");

                runtime.releaseReservedFiber(heldFiber);
                heldFiber = null;
                Assert.assertTrue(job.run());
                drainUntilIdle(runtime);
                assertExportRowCount("saturated.parquet", 1);
            } finally {
                if (heldFiber != null) {
                    runtime.releaseReservedFiber(heldFiber);
                }
                try {
                    closeRuntime(runtime);
                } finally {
                    job.close();
                }
            }
        });
    }

    @Test
    public void testLegacyRunsSuspendableExportSynchronously() throws Exception {
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

    private void assertExportDoesNotExist(CharSequence fileName) {
        try (Path path = new Path()) {
            path.of(exportRoot).concat(fileName).$();
            Assert.assertFalse(configuration.getFilesFacade().exists(path.$()));
        }
    }

    private void assertExportRowCount(CharSequence fileName, long expected) throws Exception {
        try (Path path = new Path()) {
            path.of(exportRoot).concat(fileName).$();
            assertQuery("SELECT count() FROM read_parquet('" + path + "')")
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
