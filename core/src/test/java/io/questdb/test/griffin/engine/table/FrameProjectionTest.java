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

package io.questdb.test.griffin.engine.table;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordBlock;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.FrameProjection;
import io.questdb.griffin.engine.table.VirtualRecordCursorFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * The virtual columns of a projection over an Async Filter, computed by the filtering threads
 * ({@link FrameProjection}), against the same queries with the frame dispatch switch off, where
 * the projection's thread computes every value. Values are compared as raw bits, read row by row
 * and through record blocks (as the egress reads them), mixing the two at random.
 */
public class FrameProjectionTest extends AbstractCairoTest {
    private static final String[] QUERIES = {
            // TAQ idx 11: the JIT filter
            "select ts, (bid + ask) / 2 mid from q where sym = 'A'",
            // TAQ idx 13: a selection between the projection and the filter
            "select ts, (bid + ask) / 2 mid from (select ts, sym, bid, ask, bsize, asize, x from q where sym = 'A')",
            // every computed type, a plain column in between, a column reused by name
            "select x * 3 lx, bsize + asize i, bid > ask b, (bsize % 100)::short sh, (bsize % 100)::byte by, bid * 2 f, " +
                    "sym, ts + 1000 t, ts::date d, (bid + ask) / 2 mid, mid * 2 mid2 from q where sym in ('A', 'B') and bsize > 10",
            // a Java (non-JIT) filter
            "select ts, (bid + ask) / 2 mid, bsize * 2 b2 from q where sym like 'A%' or x % 7 = 0",
            // bind variables and functions
            "select ts, (bid + ask) * $1 m, abs(bsize - $2) d from q where x % 3 = 0",
            // descending scans, LIMIT, negative LIMIT and an order over the projection
            "select ts, (bid + ask) / 2 mid from q where sym = 'B' order by ts desc",
            "select ts, (bid + ask) / 2 mid from q where sym = 'B' limit 777",
            "select ts, (bid + ask) / 2 mid from q where sym = 'B' limit -333",
            "select ts, (bid + ask) / 2 mid from q where sym = 'C' order by mid, ts",
            // NULLs from a column top
            "select ts, n + 1 n1, (bid + ask) / 2 mid from q where x % 5 = 1",
    };

    @Test
    public void testOnTestContext() throws Exception {
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext);
            assertArms(engine, sqlExecutionContext, false);
        });
    }

    @Test
    public void testOnWorkerPool() throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(pool, (engine, compiler, ctx) -> {
                createTable(engine, ctx);
                assertArms(engine, ctx, true);
            }, configuration, LOG);
        });
    }

    @Test
    public void testOtherRecordsAreComputedByTheProjection() throws Exception {
        // Only reads through the projection cursor's own record, at its own row, are served from
        // the frame: a record B, and record A moved by recordAt(), compute the row's values.
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FRAME_DISPATCH_ENABLED, "true");
            final String query = "select ts, (bid + ask) / 2 mid, bsize * 3 b3 from q where sym = 'A'";
            try (RecordCursorFactory factory = select(query)) {
                Assert.assertNotNull(findProjection(factory));
                Assert.assertTrue(factory.recordCursorSupportsRandomAccess());
                final io.questdb.std.LongList rowIds = new io.questdb.std.LongList();
                final io.questdb.std.LongList mids = new io.questdb.std.LongList();
                final io.questdb.std.LongList b3s = new io.questdb.std.LongList();
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final Record record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        rowIds.add(record.getRowId());
                        mids.add(Double.doubleToRawLongBits(record.getDouble(1)));
                        b3s.add(record.getInt(2));
                    }
                }
                Assert.assertTrue(rowIds.size() > 1000);
                final Rnd rnd = TestUtils.generateRandom(LOG);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final Record record = cursor.getRecord();
                    final Record recordB = cursor.getRecordB();
                    int i = 0;
                    while (cursor.hasNext()) {
                        if (i % 97 == 0) {
                            // record B at another row, while record A stands on a projected row
                            final int j = rnd.nextInt(rowIds.size());
                            cursor.recordAt(recordB, rowIds.getQuick(j));
                            Assert.assertEquals("B at " + j, mids.getQuick(j), Double.doubleToRawLongBits(recordB.getDouble(1)));
                            Assert.assertEquals("B at " + j, b3s.getQuick(j), recordB.getInt(2));
                            Assert.assertEquals("A at " + i, mids.getQuick(i), Double.doubleToRawLongBits(record.getDouble(1)));
                        }
                        if (i % 1001 == 500) {
                            // record A itself moved to another row
                            final int j = rnd.nextInt(rowIds.size());
                            cursor.recordAt(record, rowIds.getQuick(j));
                            Assert.assertEquals("A moved to " + j, mids.getQuick(j), Double.doubleToRawLongBits(record.getDouble(1)));
                            Assert.assertEquals("A moved to " + j, b3s.getQuick(j), record.getInt(2));
                            break;
                        }
                        i++;
                    }
                }
            }
        });
    }

    @Test
    public void testParquetFramesAreComputedByTheProjection() throws Exception {
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext);
            execute("alter table q convert partition to parquet where ts < '1970-01-02'");
            assertArms(engine, sqlExecutionContext, false);
        });
    }

    private static void assertArms(CairoEngine engine, SqlExecutionContext ctx, boolean pool) throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        long projectedFrames = 0;
        long kernelBatches = 0;
        long rowBatches = 0;
        for (String query : QUERIES) {
            bindVariableService.clear();
            bindVariableService.setDouble(0, 1.5);
            bindVariableService.setInt(1, 333);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FRAME_DISPATCH_ENABLED, "false");
            final String expected;
            try (RecordCursorFactory factory = engine.select(query, ctx)) {
                Assert.assertNull(query, findProjection(factory));
                expected = read(factory, ctx, null);
            }
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FRAME_DISPATCH_ENABLED, "true");
            try (RecordCursorFactory factory = engine.select(query, ctx)) {
                final FrameProjection projection = findProjection(factory);
                if (projection != null) {
                    projection.resetTestCounter();
                }
                for (int run = 0; run < 3; run++) {
                    TestUtils.assertEquals(query + " [rows, run " + run + "]", expected, read(factory, ctx, null));
                    TestUtils.assertEquals(query + " [blocks, run " + run + "]", expected, read(factory, ctx, rnd));
                }
                if (projection != null) {
                    projectedFrames += projection.getProjectedFrameCount();
                    kernelBatches += projection.getBatchCounts()[0];
                    rowBatches += projection.getBatchCounts()[1];
                }
            }
        }
        Assert.assertTrue("no frame was projected", projectedFrames > 0);
        // arithmetic is computed column-wise; a boolean, a cast and a column top row by row
        Assert.assertTrue("no column-wise batch", kernelBatches > 0);
        Assert.assertTrue("no row batch", rowBatches > 0);
    }

    private static void createTable(CairoEngine engine, SqlExecutionContext ctx) throws Exception {
        engine.execute(
                "create table q (sym symbol, bid float, bsize int, ask float, asize int, x long, ts timestamp)" +
                        " timestamp(ts) partition by day bypass wal",
                ctx
        );
        engine.execute(
                "insert into q select rnd_symbol('A', 'B', 'C', 'AB', null), case when x % 13 = 0 then null else rnd_float() end, " +
                        "rnd_int(1, 1000, 3), rnd_float(), rnd_int(1, 1000, 0), x, (x * 3_000_000L)::timestamp from long_sequence(60000)",
                ctx
        );
        engine.execute("alter table q add column n long", ctx);
        engine.execute(
                "insert into q select rnd_symbol('A', 'B', 'C'), rnd_float(), rnd_int(1, 1000, 0), rnd_float(), rnd_int(1, 1000, 0), " +
                        "60000 + x, ((60000 + x) * 3_000_000L)::timestamp, x from long_sequence(30000)",
                ctx
        );
    }

    private static FrameProjection findProjection(RecordCursorFactory factory) {
        for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
            if (f instanceof VirtualRecordCursorFactory virtual && virtual.getFrameProjection() != null) {
                return virtual.getFrameProjection();
            }
        }
        return null;
    }

    // The rows' values as raw bits, row by row, or through blocks mixed with rows when rnd is set.
    private static String read(RecordCursorFactory factory, SqlExecutionContext ctx, Rnd rnd) throws Exception {
        final StringSink sink = new StringSink();
        final RecordMetadata metadata = factory.getMetadata();
        try (RecordCursor cursor = factory.getCursor(ctx)) {
            final Record record = cursor.getRecord();
            while (true) {
                final RecordBlock block = rnd != null && rnd.nextInt(4) > 0 ? cursor.peekRecordBlock(1 + rnd.nextInt(500)) : null;
                if (block != null) {
                    final int rows = block.getRowCount();
                    final int taken = rnd.nextInt(5) == 0 ? 0 : 1 + rnd.nextInt(rows);
                    for (int r = 0; r < taken; r++) {
                        final Record blockRecord = block.getRecordAt(r);
                        for (int c = 0, n = metadata.getColumnCount(); c < n; c++) {
                            final long address = block.getColumnAddress(c);
                            if (address != 0) {
                                final long rowIndexes = block.getColumnRowIndexesAddress(c);
                                final long position = rowIndexes == 0 ? r : Unsafe.getLong(rowIndexes + 8L * r);
                                final long p = address + position * block.getColumnStride(c);
                                if (ColumnType.tagOf(metadata.getColumnType(c)) == ColumnType.SYMBOL) {
                                    sink.put(cursor.getSymbolTable(c).valueOf(Unsafe.getInt(p))).put('\t');
                                } else {
                                    appendBits(sink, p, metadata.getColumnType(c));
                                }
                            } else {
                                appendBits(sink, blockRecord, c, metadata.getColumnType(c));
                            }
                        }
                        sink.put('\n');
                    }
                    cursor.skipRecordBlock(taken);
                    continue;
                }
                if (!cursor.hasNext()) {
                    break;
                }
                for (int c = 0, n = metadata.getColumnCount(); c < n; c++) {
                    appendBits(sink, record, c, metadata.getColumnType(c));
                }
                sink.put('\n');
            }
        }
        return sink.toString();
    }

    private static void appendBits(StringSink sink, long p, int type) {
        switch (ColumnType.tagOf(type)) {
            case ColumnType.BOOLEAN, ColumnType.BYTE -> sink.put(Unsafe.getByte(p));
            case ColumnType.SHORT -> sink.put(Unsafe.getShort(p));
            case ColumnType.CHAR -> sink.put((int) Unsafe.getChar(p));
            case ColumnType.INT, ColumnType.SYMBOL -> sink.put(Unsafe.getInt(p));
            case ColumnType.FLOAT -> sink.put(Float.floatToRawIntBits(Unsafe.getFloat(p)));
            case ColumnType.DOUBLE -> sink.put(Double.doubleToRawLongBits(Unsafe.getDouble(p)));
            default -> sink.put(Unsafe.getLong(p));
        }
        sink.put('\t');
    }

    private static void appendBits(StringSink sink, Record record, int c, int type) {
        switch (ColumnType.tagOf(type)) {
            case ColumnType.BOOLEAN -> sink.put(record.getBool(c) ? 1 : 0);
            case ColumnType.BYTE -> sink.put(record.getByte(c));
            case ColumnType.SHORT -> sink.put(record.getShort(c));
            case ColumnType.CHAR -> sink.put((int) record.getChar(c));
            case ColumnType.INT -> sink.put(record.getInt(c));
            case ColumnType.SYMBOL -> sink.put(record.getSymA(c));
            case ColumnType.FLOAT -> sink.put(Float.floatToRawIntBits(record.getFloat(c)));
            case ColumnType.DOUBLE -> sink.put(Double.doubleToRawLongBits(record.getDouble(c)));
            case ColumnType.DATE -> sink.put(record.getDate(c));
            case ColumnType.TIMESTAMP -> sink.put(record.getTimestamp(c));
            default -> sink.put(record.getLong(c));
        }
        sink.put('\t');
    }
}
