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

package io.questdb.test.cairo.sql;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypeTag;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.ColumnMapping;
import io.questdb.cairo.sql.ColumnVectorDescriptor;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Chars;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.types.TypeConformanceTypes;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.management.ManagementFactory;

/**
 * The column-vector descriptor (S14a, F38): every field for every existing type on frames with
 * and without a column top, native and Parquet; release on the success, error and reuse paths;
 * no allocation per frame; and concurrent use of one address cache by several workers.
 */
public class ColumnVectorDescriptorTest extends AbstractCairoTest {

    @Override
    public void setUp() {
        // many small frames, so parallel queries fan out over the workers
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1_000);
        super.setUp();
    }

    @Test
    public void testAddressCacheReleasesListsOnFailedOpen() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (i INT, s VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t VALUES (1, 'a', '1970-01-01T00:00:00.000000Z')");
            try (RecordCursorFactory factory = select("SELECT * FROM t")) {
                // raise the limit one list at a time (64 longs each): the open fails at every
                // list, and each failure must leave nothing allocated
                int failures = 0;
                boolean isOpen = false;
                final long used = Unsafe.getRssMemUsed();
                for (int lists = 0; lists <= 16 && !isOpen; lists++) {
                    try (PageFrameAddressCache cache = new PageFrameAddressCache()) {
                        Unsafe.setRssMemLimit(used + lists * 64L * Long.BYTES);
                        try {
                            cache.of(factory.getMetadata(), new ColumnMapping(), false);
                            isOpen = true;
                        } catch (CairoException e) {
                            Assert.assertTrue(e.isOutOfMemory());
                            Assert.assertEquals("failed open after " + lists + " lists leaks", used, Unsafe.getRssMemUsed());
                            failures++;
                        } finally {
                            Unsafe.setRssMemLimit(0);
                        }
                    }
                }
                Assert.assertTrue(isOpen);
                // four lists: data and aux addresses and sizes
                Assert.assertEquals(4, failures);
            }
        });
    }

    @Test
    public void testAddressCacheReuse() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (i INT, s VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t SELECT x::INT, x::VARCHAR, (x * 3_600_000_000L)::TIMESTAMP FROM long_sequence(100)");
            try (
                    RecordCursorFactory factory = select("SELECT * FROM t");
                    PageFrameAddressCache cache = new PageFrameAddressCache();
                    PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration)
            ) {
                final ColumnVectorDescriptor columnVectors = new ColumnVectorDescriptor();
                for (int round = 0; round < 3; round++) {
                    try (PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC)) {
                        cache.of(factory.getMetadata(), cursor.getColumnMapping(), cursor.isExternal());
                        pool.of(cache);
                        int frameIndex = 0;
                        PageFrame frame;
                        while ((frame = cursor.next()) != null) {
                            cache.add(frameIndex, frame);
                            cache.describeNativeFrame(frameIndex, columnVectors);
                            Assert.assertEquals(frame.getDataAddress(0), columnVectors.getDataAddress(0));
                            Assert.assertEquals(-1, columnVectors.getNullCount(0));
                            Assert.assertEquals(frame.getAuxAddress(1), pool.navigateTo(frameIndex).getColumnVectorDescriptor().getAuxAddress(1));
                            frameIndex++;
                        }
                        Assert.assertEquals(5, frameIndex);
                    }
                }
            }
        });
    }

    @Test
    public void testFieldsForEveryType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t VALUES ('1970-01-01T00:00:00.000000Z'), ('1970-01-01T00:00:01.000000Z')");
            // every storable type, added after day 1, so day 1 is a column top for each of them
            int typeCount = 0;
            for (int i = 0, n = TypeConformanceTypes.ALL.size(); i < n; i++) {
                final TypeConformanceTypes.Entry entry = TypeConformanceTypes.ALL.getQuick(i);
                if (isStorable(entry)) {
                    execute("ALTER TABLE t ADD COLUMN c" + i + " " + entry.ddl);
                    typeCount++;
                }
            }
            Assert.assertTrue(typeCount > 30);
            execute("INSERT INTO t (ts) VALUES ('1970-01-02T00:00:00.000000Z'), ('1970-01-02T00:00:01.000000Z')");
            execute("INSERT INTO t (ts) VALUES ('1970-01-03T00:00:00.000000Z')");
            assertFrames(new boolean[]{true, false, false}, new byte[]{PartitionFormat.NATIVE, PartitionFormat.NATIVE, PartitionFormat.NATIVE});

            // Parquet frames: the pool's decode buffers carry no validity lists
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < '1970-01-03'");
            assertFrames(new boolean[]{true, false, false}, new byte[]{PartitionFormat.PARQUET, PartitionFormat.PARQUET, PartitionFormat.NATIVE});
        });
    }

    @Test
    public void testNoAllocationPerFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (i INT, l LONG, s VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            // one row per hour over 32 days: 32 partitions, one frame each
            execute("INSERT INTO t SELECT x::INT, x, rnd_varchar(1, 20, 1), ((x - 1) * 3_600_000_000L)::TIMESTAMP FROM long_sequence(768)");
            final com.sun.management.ThreadMXBean mx = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
            try (
                    RecordCursorFactory factory = select("SELECT * FROM t");
                    PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                    PageFrameAddressCache cache = new PageFrameAddressCache();
                    PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration);
                    PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
                    DirectLongList dataAddresses = new DirectLongList(4, MemoryTag.NATIVE_DEFAULT);
                    DirectLongList auxAddresses = new DirectLongList(4, MemoryTag.NATIVE_DEFAULT)
            ) {
                cache.of(factory.getMetadata(), cursor.getColumnMapping(), cursor.isExternal());
                pool.of(cache);
                long checksum = 0;
                long bytes = -1;
                for (int pass = 0; pass < 4; pass++) {
                    cursor.toTop();
                    final long before = mx.getCurrentThreadAllocatedBytes();
                    int frameIndex = 0;
                    PageFrame frame;
                    while ((frame = cursor.next()) != null) {
                        cache.add(frameIndex, frame);
                        final PageFrameMemory frameMemory = pool.navigateTo(frameIndex);
                        record.init(frameMemory);
                        record.setRowIndex(0);
                        PageFrameReduceTask.populateJitAddresses(frameMemory, cache, dataAddresses, auxAddresses);
                        final ColumnVectorDescriptor columnVectors = frameMemory.getColumnVectorDescriptor();
                        for (int c = 0, n = columnVectors.getColumnCount(); c < n; c++) {
                            checksum += columnVectors.getDataSize(c) + columnVectors.getNullCount(c) + columnVectors.getNullPolicy(c).ordinal();
                        }
                        checksum += record.getLong(1) + dataAddresses.size();
                        frameIndex++;
                    }
                    Assert.assertEquals(32, frameIndex);
                    bytes = mx.getCurrentThreadAllocatedBytes() - before;
                }
                Assert.assertTrue(checksum != 0);
                // a warmed-up pass over 32 frames allocates nothing (FR-025)
                Assert.assertEquals(0, bytes);
            }
        });
    }

    @Test
    public void testParallelQueriesMatchSingleThreaded() throws Exception {
        assertMemoryLeak(() -> {
            final String[] queries = {
                    // compiled filter over many frames: the task and filter-slot lists
                    "SELECT * FROM t WHERE i > 10 AND l < 18_000",
                    "SELECT count() FROM t WHERE i > 10 AND l < 18_000",
                    // interpreted filter
                    "SELECT * FROM t WHERE s LIKE '%1%'",
                    // parallel GROUP BY, keyed and not keyed, with a compiled filter
                    "SELECT k, count(), sum(l), max(d) FROM t WHERE i <> 7 GROUP BY k ORDER BY k",
                    "SELECT count(), sum(l), min(d) FROM t WHERE i <> 7",
                    // vector aggregates
                    "SELECT k, sum(l), min(d) FROM t GROUP BY k ORDER BY k",
                    "SELECT sum(l), min(d), max(i) FROM t"
            };
            final StringSink[] expected = new StringSink[queries.length];
            final StringSink[] actual = new StringSink[queries.length];
            for (int i = 0; i < queries.length; i++) {
                expected[i] = new StringSink();
                actual[i] = new StringSink();
            }
            runQueries(null, queries, expected);
            runQueries(new WorkerPool(() -> 4), queries, actual);
            for (int i = 0; i < queries.length; i++) {
                // a header and at least one row: the header's line break is not the last character
                Assert.assertTrue(queries[i], Chars.indexOf(expected[i], '\n') < expected[i].length() - 1);
                TestUtils.assertEquals(queries[i], expected[i], actual[i]);
            }
        });
    }

    @Test
    public void testReduceTaskReleasesListsOnFailedConstruction() throws Exception {
        assertMemoryLeak(() -> {
            // raise the limit step by step until the task constructs: each failure on the way
            // must leave nothing allocated
            int failures = 0;
            boolean isConstructed = false;
            final long used = Unsafe.getRssMemUsed();
            for (long headroom = 0; headroom <= 1024 * 1024 && !isConstructed; headroom += Long.BYTES) {
                Unsafe.setRssMemLimit(used + headroom);
                PageFrameReduceTask task = null;
                try {
                    task = new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD);
                } catch (CairoException e) {
                    Assert.assertTrue(e.isOutOfMemory());
                    Assert.assertEquals("failed construction at headroom " + headroom + " leaks", used, Unsafe.getRssMemUsed());
                    failures++;
                } finally {
                    Unsafe.setRssMemLimit(0);
                }
                if (task != null) {
                    task.close();
                    isConstructed = true;
                }
            }
            Assert.assertTrue(isConstructed);
            Assert.assertTrue(failures > 0);
        });
    }

    private static boolean isStorable(TypeConformanceTypes.Entry entry) {
        return !entry.isLater()
                && entry.tag != ColumnTypeTag.VARCHAR_SLICE
                && !ColumnType.isInterval(entry.columnType);
    }

    private static void printQuery(SqlCompiler compiler, SqlExecutionContext context, String query, StringSink sink) throws Exception {
        context.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
        TestUtils.printSql(compiler, context, query, sink);
    }

    private static void runQueries(WorkerPool pool, String[] queries, StringSink[] sinks) throws Exception {
        TestUtils.execute(
                pool,
                (engine, compiler, context) -> {
                    engine.execute("DROP TABLE IF EXISTS t", context);
                    engine.execute(
                            "CREATE TABLE t (k SYMBOL, i INT, l LONG, d DOUBLE, s VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL",
                            context
                    );
                    engine.execute(
                            """
                                    INSERT INTO t SELECT CASE WHEN x % 3 = 0 THEN 'a' WHEN x % 3 = 1 THEN 'b' ELSE 'c' END,
                                        (x % 100)::INT, x, x / 7.0,
                                        CASE WHEN x % 11 = 0 THEN NULL ELSE (x % 1_000)::VARCHAR END,
                                        (x * 30_000_000L)::TIMESTAMP
                                    FROM long_sequence(20_000)""",
                            context
                    );
                    for (int i = 0; i < queries.length; i++) {
                        printQuery(compiler, context, queries[i], sinks[i]);
                    }
                },
                configuration,
                LOG
        );
    }

    private void assertFrames(boolean[] isColumnTopFrame, byte[] formats) throws Exception {
        try (
                RecordCursorFactory factory = select("SELECT * FROM t");
                PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                PageFrameAddressCache cache = new PageFrameAddressCache();
                PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration);
                PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER)
        ) {
            final RecordMetadata metadata = factory.getMetadata();
            cache.of(metadata, cursor.getColumnMapping(), cursor.isExternal());
            pool.of(cache);
            int frameIndex = 0;
            PageFrame frame;
            while ((frame = cursor.next()) != null) {
                cache.add(frameIndex, frame);
                Assert.assertEquals(formats[frameIndex], frame.getFormat());
                final PageFrameMemory frameMemory = pool.navigateTo(frameIndex);
                final ColumnVectorDescriptor columnVectors = frameMemory.getColumnVectorDescriptor();
                record.init(frameMemory);
                Assert.assertEquals(metadata.getColumnCount(), columnVectors.getColumnCount());
                try (PageFrameMemoryRecord copy = new PageFrameMemoryRecord(record, PageFrameMemoryRecord.RECORD_B_LETTER)) {
                    for (int c = 0, n = metadata.getColumnCount(); c < n; c++) {
                        final int columnType = metadata.getColumnType(c);
                        final String label = "frame " + frameIndex + ", column " + metadata.getColumnName(c) + " " + ColumnType.nameOf(columnType);
                        // the values no column has yet: no validity bitmap, NULL count unknown
                        Assert.assertEquals(label, 0, columnVectors.getValidityAddress(c));
                        Assert.assertEquals(label, 0, columnVectors.getValidityBitOffset(c));
                        Assert.assertEquals(label, -1, columnVectors.getNullCount(c));
                        Assert.assertEquals(label, metadata.getColumnNullPolicy(c), columnVectors.getNullPolicy(c));
                        if (frame.getFormat() == PartitionFormat.NATIVE) {
                            // "all NULL": a native column top has no data vector (a var-size
                            // column no aux vector); a Parquet frame decodes its column tops
                            final boolean isTop = c > 0 && isColumnTopFrame[frameIndex];
                            final long vector = ColumnType.isVarSize(columnType) ? columnVectors.getAuxAddress(c) : columnVectors.getDataAddress(c);
                            Assert.assertEquals(label, isTop, vector == 0);
                            // the frame's own answers, as the cache copied them
                            Assert.assertEquals(label, frame.getDataAddress(c), columnVectors.getDataAddress(c));
                            Assert.assertEquals(label, frame.getDataSize(c), columnVectors.getDataSize(c));
                            Assert.assertEquals(label, frame.getValidityAddress(c), columnVectors.getValidityAddress(c));
                            Assert.assertEquals(label, frame.getNullCount(c), columnVectors.getNullCount(c));
                            if (ColumnType.isVarSize(columnType)) {
                                Assert.assertEquals(label, frame.getAuxAddress(c), columnVectors.getAuxAddress(c));
                                Assert.assertEquals(label, frame.getAuxSize(c), columnVectors.getAuxSize(c));
                            } else {
                                Assert.assertEquals(label, 0, columnVectors.getAuxAddress(c));
                                Assert.assertEquals(label, 0, columnVectors.getAuxSize(c));
                            }
                        }
                        // both records read through their own copy of the descriptor
                        Assert.assertEquals(label, columnVectors.getDataAddress(c), record.getPageAddress(c));
                        Assert.assertEquals(label, columnVectors.getDataAddress(c), copy.getPageAddress(c));
                    }
                }
                frameIndex++;
            }
            Assert.assertEquals(isColumnTopFrame.length, frameIndex);
        }
    }
}
