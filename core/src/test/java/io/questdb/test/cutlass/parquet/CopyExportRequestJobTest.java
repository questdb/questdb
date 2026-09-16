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
import io.questdb.mp.continuation.SuspensionScope;
import io.questdb.std.FilesFacade;
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
