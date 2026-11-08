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

package io.questdb.test.cairo.covering;

import io.questdb.ParanoiaState;
import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.table.CoveringIndexRecordCursorFactory;
import io.questdb.griffin.engine.table.ExtraNullColumnCursorFactory;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

/**
 * Covers how a multi-key covering page-frame cursor frees the buffers it materializes
 * frames into. {@code releaseOpenPartitions()}, which the parquet exporters call after each
 * row group, frees every frame before the current one; {@code releaseConsumedFrames()},
 * which QWP egress calls before it pulls the next frame, frees them all. The per-query
 * memory tracker that pays for the buffers measures them exactly.
 */
public class CoveringIndexFrameReleaseTest extends AbstractCairoTest {
    private static final String COLUMNS = "ts, sym, price, qty, name, s, bin, arr";
    private static final String COVERED = "SELECT " + COLUMNS + " FROM t WHERE sym IN ('A', 'C')";
    // 'A' and 'C' match 13, 13 and 14 of the 20 rows of each day partition. At 4 rows per
    // frame the merge emits 4 frames a partition, each with fresh buffers of one capacity.
    private static final int FRAME_COUNT = 12;
    private static final int FRAME_ROWS = 4;
    private static final String REFERENCE = "SELECT /*+ no_covering */ " + COLUMNS + " FROM t WHERE sym IN ('A', 'C')";

    @Override
    public void tearDown() throws Exception {
        CoveringIndexRecordCursorFactory.setMaxRowsPerFrameForTesting(-1);
        super.tearDown();
    }

    @Test
    public void testFrameReleaseParanoiaOffOutsideJUnit() throws Exception {
        // The server runs with -ea (questdb.sh, the Docker entrypoint), so an assert cannot keep
        // the memset of every released frame out of production. A JVM with no test framework on
        // the stack, started with -ea like the server, must find the mode off.
        File javaExecutable = new File(new File(System.getProperty("java.home"), "bin"), "java");
        if (!javaExecutable.exists()) {
            javaExecutable = new File(javaExecutable.getPath() + ".exe");
        }
        final String classPath = Paths.get(
                FrameReleaseParanoiaMain.class.getProtectionDomain().getCodeSource().getLocation().toURI()
        ) + File.pathSeparator + Paths.get(
                ParanoiaState.class.getProtectionDomain().getCodeSource().getLocation().toURI()
        );
        final File outFile = temp.newFile();
        final File errFile = temp.newFile();
        final Process process = new ProcessBuilder(
                javaExecutable.getAbsolutePath(),
                "-ea",
                "-cp",
                classPath,
                FrameReleaseParanoiaMain.class.getName()
        ).redirectOutput(outFile).redirectError(errFile).start();
        try {
            process.getOutputStream().close();
            if (!process.waitFor(15, TimeUnit.SECONDS)) {
                Assert.fail("process timed out:\n" + Files.readString(errFile.toPath(), StandardCharsets.UTF_8));
            }
            final String err = Files.readString(errFile.toPath(), StandardCharsets.UTF_8);
            Assert.assertEquals(err, 0, process.exitValue());
            Assert.assertEquals(
                    err,
                    "FRAME_RELEASE_PARANOIA_MODE=false" + System.lineSeparator(),
                    Files.readString(outFile.toPath(), StandardCharsets.UTF_8)
            );
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly().onExit().join();
            }
        }
    }

    @Test
    public void testFrameReleaseParanoiaOnInsideJUnit() {
        // Every test that streams covered frames relies on the release overwriting the freed
        // buffers: without it, a frame read after its release passes on the stale bytes freed
        // memory keeps. ParanoiaState turns the mode on when it finds a test framework on the
        // stack of the thread that initializes it, as it does for LOG_PARANOIA_MODE.
        Assert.assertTrue(ParanoiaState.FRAME_RELEASE_PARANOIA_MODE);
    }

    @Test
    public void testLimitBreachWhenFramesAreKeptAndNoBreachWithRelease() throws Exception {
        // A per-query limit that holds three frames but not four: a caller that keeps every
        // frame, as the async consumers do, breaches it; one that releases each frame streams
        // the whole result under it, on the same factory right after the breach.
        assertMemoryLeak(() -> {
            createTable();
            CoveringIndexRecordCursorFactory.setMaxRowsPerFrameForTesting(FRAME_ROWS);
            final long frameBytes = measureFrameBytes();
            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 3 * frameBytes + frameBytes / 2);
            final String expected = printSqlToString(REFERENCE);
            try (RecordCursorFactory factory = select(COVERED)) {
                try (PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC)) {
                    int frames = 0;
                    try {
                        while (cursor.next(0) != null) {
                            frames++;
                        }
                        Assert.fail("expected a per-query memory breach, got " + frames + " frames");
                    } catch (CairoException e) {
                        Assert.assertTrue(e.isOutOfMemory());
                        TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                        Assert.assertEquals(3, frames);
                    }
                }
                try (PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                     FrameRows rows = new FrameRows(factory.getMetadata(), cursor)) {
                    final MemoryTracker tracker = sqlExecutionContext.getMemoryTracker();
                    int frames = 0;
                    PageFrame frame;
                    while ((frame = cursor.next(0)) != null) {
                        frames++;
                        rows.append(frame);
                        Assert.assertEquals(frameBytes, tracker.getUsed());
                        cursor.releaseConsumedFrames();
                    }
                    Assert.assertEquals(FRAME_COUNT, frames);
                    Assert.assertEquals(expected, rows.toString());
                }
            }
        });
    }

    @Test
    public void testReleaseBeforeFirstAndAfterLastFrame() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            CoveringIndexRecordCursorFactory.setMaxRowsPerFrameForTesting(FRAME_ROWS);
            final long frameBytes = measureFrameBytes();
            try (RecordCursorFactory factory = select(COVERED);
                 PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC)) {
                final MemoryTracker tracker = sqlExecutionContext.getMemoryTracker();
                // nothing handed out yet: both releases are no-ops
                cursor.releaseOpenPartitions();
                cursor.releaseConsumedFrames();
                Assert.assertEquals(0, tracker.getUsed());
                int frames = 0;
                while (cursor.next(0) != null) {
                    frames++;
                }
                Assert.assertEquals(FRAME_COUNT, frames);
                Assert.assertEquals(FRAME_COUNT * frameBytes, tracker.getUsed());
                // past the last frame, the most recent frame is still the current one
                cursor.releaseOpenPartitions();
                Assert.assertEquals(frameBytes, tracker.getUsed());
                cursor.releaseOpenPartitions();
                Assert.assertEquals(frameBytes, tracker.getUsed());
                cursor.releaseConsumedFrames();
                Assert.assertEquals(0, tracker.getUsed());
                cursor.releaseConsumedFrames();
                cursor.releaseOpenPartitions();
                Assert.assertEquals(0, tracker.getUsed());
                Assert.assertNull(cursor.next(0));
            }
        });
    }

    @Test
    public void testReleaseConsumedFramesFreesEveryFrame() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            CoveringIndexRecordCursorFactory.setMaxRowsPerFrameForTesting(FRAME_ROWS);
            final long frameBytes = measureFrameBytes();
            final String expected = printSqlToString(REFERENCE);
            try (RecordCursorFactory factory = select(COVERED);
                 PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                 FrameRows rows = new FrameRows(factory.getMetadata(), cursor)) {
                final MemoryTracker tracker = sqlExecutionContext.getMemoryTracker();
                int frames = 0;
                PageFrame frame;
                while ((frame = cursor.next(0)) != null) {
                    frames++;
                    rows.append(frame);
                    Assert.assertEquals(frameBytes, tracker.getUsed());
                    cursor.releaseConsumedFrames();
                    Assert.assertEquals(0, tracker.getUsed());
                }
                Assert.assertEquals(FRAME_COUNT, frames);
                Assert.assertEquals(expected, rows.toString());
            }
        });
    }

    @Test
    public void testReleaseOpenPartitionsKeepsCurrentFrame() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            CoveringIndexRecordCursorFactory.setMaxRowsPerFrameForTesting(FRAME_ROWS);
            final long frameBytes = measureFrameBytes();
            final String expected = printSqlToString(REFERENCE);
            try (RecordCursorFactory factory = select(COVERED);
                 PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                 FrameRows rows = new FrameRows(factory.getMetadata(), cursor)) {
                final MemoryTracker tracker = sqlExecutionContext.getMemoryTracker();
                PageFrame frame = cursor.next(0);
                Assert.assertNotNull(frame);
                Assert.assertEquals(frameBytes, tracker.getUsed());
                // the only frame is the current one: nothing to free
                cursor.releaseOpenPartitions();
                Assert.assertEquals(frameBytes, tracker.getUsed());
                rows.append(frame);
                int frames = 1;
                while ((frame = cursor.next(0)) != null) {
                    frames++;
                    Assert.assertEquals(2 * frameBytes, tracker.getUsed());
                    cursor.releaseOpenPartitions();
                    Assert.assertEquals(frameBytes, tracker.getUsed());
                    // the current frame survives the release of the ones before it
                    rows.append(frame);
                }
                Assert.assertEquals(FRAME_COUNT, frames);
                Assert.assertEquals(expected, rows.toString());
            }
        });
    }

    @Test
    public void testReleaseThenToTopAndReopen() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            CoveringIndexRecordCursorFactory.setMaxRowsPerFrameForTesting(FRAME_ROWS);
            final long frameBytes = measureFrameBytes();
            final String expected = printSqlToString(REFERENCE);
            try (RecordCursorFactory factory = select(COVERED)) {
                try (PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC)) {
                    final MemoryTracker tracker = sqlExecutionContext.getMemoryTracker();
                    Assert.assertNotNull(cursor.next(0));
                    Assert.assertNotNull(cursor.next(0));
                    cursor.releaseOpenPartitions();
                    Assert.assertNotNull(cursor.next(0));
                    cursor.releaseConsumedFrames();
                    Assert.assertNotNull(cursor.next(0));
                    Assert.assertEquals(frameBytes, tracker.getUsed());

                    // toTop() frees what is left and the scan starts over, keeping every frame
                    cursor.toTop();
                    Assert.assertEquals(0, tracker.getUsed());
                    try (FrameRows rows = new FrameRows(factory.getMetadata(), cursor)) {
                        PageFrame frame;
                        while ((frame = cursor.next(0)) != null) {
                            rows.append(frame);
                        }
                        Assert.assertEquals(FRAME_COUNT * frameBytes, tracker.getUsed());
                        Assert.assertEquals(expected, rows.toString());
                    }
                    cursor.releaseOpenPartitions();
                    Assert.assertEquals(frameBytes, tracker.getUsed());
                }
                // the factory hands out the same cursor again for the next execution
                try (PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                     FrameRows rows = new FrameRows(factory.getMetadata(), cursor)) {
                    final MemoryTracker tracker = sqlExecutionContext.getMemoryTracker();
                    Assert.assertEquals(0, tracker.getUsed());
                    PageFrame frame;
                    while ((frame = cursor.next(0)) != null) {
                        rows.append(frame);
                        cursor.releaseOpenPartitions();
                        Assert.assertEquals(frameBytes, tracker.getUsed());
                    }
                    Assert.assertEquals(expected, rows.toString());
                }
            }
        });
    }

    @Test
    public void testReleaseThroughNullPadWrapper() throws Exception {
        // Window joins with an always-false ON pad the master with NULL columns through
        // ExtraNullColumnCursorFactory, which must pass both releases to the covering cursor.
        assertMemoryLeak(() -> {
            createTable();
            CoveringIndexRecordCursorFactory.setMaxRowsPerFrameForTesting(FRAME_ROWS);
            final long frameBytes = measureFrameBytes();
            try (RecordCursorFactory outer = select(COVERED)) {
                // Unwrap the QueryProgress, whose page-frame cursor is no TablePageFrameCursor,
                // as CoveringIndexParallelDecodeTest does. The QueryProgress stays the only
                // owner of the covering factory; the wrapper allocates nothing to free.
                Assert.assertTrue(outer instanceof QueryProgress);
                final RecordCursorFactory base = outer.getBaseFactory();
                final RecordMetadata baseMetadata = base.getMetadata();
                final GenericRecordMetadata metadata = GenericRecordMetadata.copyOfNew(baseMetadata);
                metadata.add(new TableColumnMetadata("pad", ColumnType.LONG));
                final ExtraNullColumnCursorFactory padded = new ExtraNullColumnCursorFactory(metadata, baseMetadata.getColumnCount(), base);
                try (MemoryTracker tracker = engine.getMemoryTrackerProvider().acquire(
                        sqlExecutionContext.getSecurityContext(), 1L, MemoryTrackerWorkload.QUERY
                )) {
                    sqlExecutionContext.setMemoryTracker(tracker);
                    try (PageFrameCursor cursor = padded.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC)) {
                        Assert.assertNotNull(cursor.next(0));
                        Assert.assertNotNull(cursor.next(0));
                        Assert.assertEquals(2 * frameBytes, tracker.getUsed());
                        cursor.releaseOpenPartitions();
                        Assert.assertEquals(frameBytes, tracker.getUsed());
                        cursor.releaseConsumedFrames();
                        Assert.assertEquals(0, tracker.getUsed());
                    } finally {
                        sqlExecutionContext.setMemoryTracker(null);
                    }
                }
            }
        });
    }

    @Test
    public void testReleaseThroughProjection() throws Exception {
        // QueryProgress over SelectedRecord over the covering factory: both wrappers pass the
        // releases down, and the projected columns read right after each one.
        assertMemoryLeak(() -> {
            createTable();
            CoveringIndexRecordCursorFactory.setMaxRowsPerFrameForTesting(FRAME_ROWS);
            final String projection = "qty, name, sym, qty";
            final String sql = "SELECT " + projection + " FROM t WHERE sym IN ('A', 'C')";
            final String expected = printSqlToString("SELECT /*+ no_covering */ " + projection + " FROM t WHERE sym IN ('A', 'C')");
            final String plan = printSqlToString("EXPLAIN " + sql);
            TestUtils.assertContains(plan, "SelectedRecord");
            TestUtils.assertContains(plan, "CoveringIndex on: sym");
            try (RecordCursorFactory factory = select(sql);
                 PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                 FrameRows rows = new FrameRows(factory.getMetadata(), cursor)) {
                Assert.assertTrue(factory instanceof QueryProgress);
                final MemoryTracker tracker = sqlExecutionContext.getMemoryTracker();
                PageFrame frame = cursor.next(0);
                Assert.assertNotNull(frame);
                final long frameBytes = tracker.getUsed();
                Assert.assertTrue(frameBytes > 0);
                int frames = 0;
                do {
                    if (++frames % 2 == 1) {
                        // read the frame, then free it and every frame before it
                        rows.append(frame);
                        cursor.releaseConsumedFrames();
                        Assert.assertEquals(0, tracker.getUsed());
                    } else {
                        // free the frames before this one, then read this one
                        Assert.assertEquals(frameBytes, tracker.getUsed());
                        cursor.releaseOpenPartitions();
                        Assert.assertEquals(frameBytes, tracker.getUsed());
                        rows.append(frame);
                    }
                } while ((frame = cursor.next(0)) != null);
                Assert.assertEquals(FRAME_COUNT, frames);
                Assert.assertEquals(expected, rows.toString());
            }
        });
    }

    @Test
    public void testReleaseWithoutFrameBuffersIsNoOp() throws Exception {
        // Neither a single-key covering scan, whose frames hold no buffers of their own, nor a
        // table scan, whose frames point into mapped partitions, frees anything: their frames
        // stay readable after both releases, and the scan goes on.
        assertMemoryLeak(() -> {
            createTable();
            CoveringIndexRecordCursorFactory.setMaxRowsPerFrameForTesting(FRAME_ROWS);
            final String[] queries = {
                    "SELECT ts, qty, name FROM t WHERE sym = 'A'",
                    "SELECT ts, qty, name FROM t",
            };
            for (String sql : queries) {
                final String expected = printSqlToString(sql.replace("SELECT", "SELECT /*+ no_covering */"));
                try (RecordCursorFactory factory = select(sql);
                     PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                     FrameRows rows = new FrameRows(factory.getMetadata(), cursor)) {
                    final MemoryTracker tracker = sqlExecutionContext.getMemoryTracker();
                    int frames = 0;
                    PageFrame frame;
                    while ((frame = cursor.next(0)) != null) {
                        frames++;
                        cursor.releaseConsumedFrames();
                        cursor.releaseOpenPartitions();
                        rows.append(frame);
                    }
                    Assert.assertTrue(sql, frames > 1);
                    Assert.assertEquals(sql, 0, tracker.getUsed());
                    Assert.assertEquals(sql, expected, rows.toString());
                }
            }
        });
    }

    private static void createTable() throws Exception {
        execute("""
                CREATE TABLE t (
                    ts TIMESTAMP,
                    sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty, name, s, bin, arr),
                    price DOUBLE,
                    qty LONG,
                    name VARCHAR,
                    s STRING,
                    bin BINARY,
                    arr DOUBLE[]
                ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                """);
        // 60 rows 72 minutes apart: three day partitions of 20 rows. The names are too long
        // to inline, so VARCHAR fills its data buffer too.
        execute("""
                INSERT INTO t SELECT
                    ((x - 1) * 72 * 60_000_000L)::TIMESTAMP,
                    CASE x % 3 WHEN 0 THEN 'A' WHEN 1 THEN 'B' ELSE 'C' END,
                    CASE WHEN x % 11 = 0 THEN NULL ELSE x * 0.5 END,
                    x,
                    CASE WHEN x % 7 = 0 THEN NULL ELSE 'name of row ' || x END,
                    CASE WHEN x % 5 = 0 THEN NULL ELSE 's' || x END,
                    rnd_bin(1, 8, 1),
                    CASE WHEN x % 13 = 0 THEN NULL ELSE ARRAY[x::DOUBLE, -x::DOUBLE] END
                FROM long_sequence(60)
                """);
    }

    // The bytes the query's tracker pays for the buffers of one frame of the COVERED query.
    private static long measureFrameBytes() throws Exception {
        try (RecordCursorFactory factory = select(COVERED);
             PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC)) {
            final MemoryTracker tracker = sqlExecutionContext.getMemoryTracker();
            Assert.assertNotNull(tracker);
            Assert.assertEquals(0, tracker.getUsed());
            Assert.assertNotNull(cursor.next(0));
            final long frameBytes = tracker.getUsed();
            Assert.assertTrue(frameBytes > 0);
            return frameBytes;
        }
    }

    private static String printSqlToString(String sql) throws Exception {
        return TestUtils.printSqlToString(engine, sqlExecutionContext, sql, new StringSink());
    }

    /**
     * Prints frame rows the way {@link TestUtils#printSql} prints a cursor, and reads each
     * frame the way QWP egress does: through a page-frame address cache, memory pool and record.
     */
    private static class FrameRows implements QuietCloseable {
        private final PageFrameAddressCache addressCache = new PageFrameAddressCache();
        private final RecordMetadata metadata;
        private final PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration, 0L);
        private final PageFrameMemoryRecord record = new PageFrameMemoryRecord();
        private final StringSink sink = new StringSink();
        private int frameIndex;

        FrameRows(RecordMetadata metadata, PageFrameCursor cursor) {
            this.metadata = metadata;
            addressCache.of(metadata, cursor.getColumnMapping(), cursor.isExternal());
            pool.of(addressCache);
            record.of(cursor);
            CursorPrinter.println(metadata, sink);
        }

        @Override
        public void close() {
            Misc.free(record);
            Misc.free(pool);
            Misc.free(addressCache);
        }

        @Override
        public String toString() {
            return sink.toString();
        }

        void append(PageFrame frame) {
            addressCache.add(frameIndex, frame);
            pool.navigateTo(frameIndex, record);
            for (long r = 0, n = frame.getPartitionHi() - frame.getPartitionLo(); r < n; r++) {
                record.setRowIndex(r);
                TestUtils.println(record, metadata, sink);
            }
            frameIndex++;
        }
    }
}
