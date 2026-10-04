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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordBlock;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.window.AsyncWindowRecordCursorFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Long256;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * {@link RecordCursor#peekRecordBlock(int)}: a cursor that offers blocks must return, through
 * them, exactly the rows its {@code hasNext()} returns. Each test reads a query twice: once row
 * by row, and once mixing blocks of random sizes, taking a random number of each block's rows
 * (none, some or all) and then carrying on with blocks or {@code hasNext()}. Every value a block
 * exposes in memory must equal the record getter's.
 */
public class RecordBlockTest extends AbstractCairoTest {
    // every fixed-size type, then the variable-size ones; NULLs throughout
    private static final String ALL_TYPES_DDL = "create table at (" +
            "b boolean, by byte, sh short, ch char, i int, ip ipv4, l long, d date, ts timestamp, tn timestamp_ns, " +
            "f float, db double, s symbol, s2 symbol, u uuid, l256 long256, g1 geohash(1c), g3 geohash(3c), g6 geohash(6c), " +
            "g12 geohash(12c), dc64 decimal(12,2), dc128 decimal(30,4), dc256 decimal(60,6), v varchar, st string, bn binary, " +
            "arr double[]) timestamp(ts) partition by DAY";
    private static final String ALL_TYPES_SELECT = "select x % 3 = 0, (x % 120)::byte, (x * 7 % 30000)::short, rnd_char(), " +
            "case when x % 11 = 0 then null else x::int end, case when x % 13 = 0 then null else rnd_ipv4() end, " +
            "case when x % 5 = 0 then null else x * 1000003 end, case when x % 9 = 0 then null else (x * 86400000)::date end, " +
            "(x * 900000000)::timestamp, case when x % 6 = 0 then null else (x * 1000)::timestamp_ns end, " +
            "case when x % 4 = 0 then null else (x / 3.0)::float end, case when x % 7 = 0 then null else x * 1.5 end, " +
            "case when x % 17 = 0 then null else 'k' || (x % 50) end, 'z' || (x % 7), rnd_uuid4(), rnd_long256(), " +
            "rnd_geohash(5), rnd_geohash(15), rnd_geohash(30), rnd_geohash(60), rnd_decimal(12,2,5), rnd_decimal(30,4,5), " +
            "rnd_decimal(60,6,5), case when x % 8 = 0 then null else 'v' || x end, case when x % 10 = 0 then null else 's' || x end, " +
            "rnd_bin(1, 8, 3), rnd_double_array(1, 1)";

    @Override
    public void setUp() {
        super.setUp();
        // small frames: every query crosses many frames and partitions
        sqlExecutionContext.changePageFrameSizes(1, 64);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_TASK_ROWS, 50);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MAX_KEY_ROWS, 400);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MIN_ROWS, 100);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_ROUND_ROWS, 200);
    }

    @Test
    public void testAsyncWindowOffersTaskChains() throws Exception {
        assertMemoryLeak(() -> {
            createWindowTable(engine, sqlExecutionContext);
            final Rnd rnd = TestUtils.generateRandom(LOG);
            sqlExecutionContext.setParallelWindowEnabled(true);
            final String query = windowQuery("('A', 'B', 'C3', 'C7', 'D', null)");
            assertAsync(query);
            Assert.assertTrue(assertBlocksMatchRows(engine, sqlExecutionContext, query, rnd) > 0);
            // a LIMIT over the window passes the task chains through, clamped
            Assert.assertTrue(assertBlocksMatchRows(engine, sqlExecutionContext, "select * from (" + query + ") limit 1777", rnd) > 0);
            Assert.assertTrue(assertBlocksMatchRows(engine, sqlExecutionContext, "select * from (" + query + ") limit 5, 905", rnd) > 0);
            // a variable-size column puts no block on offer, and the rows still come
            Assert.assertEquals(0, assertBlocksMatchRows(engine, sqlExecutionContext, windowQueryWithVarchar(), rnd));
            // the serial window offers none
            sqlExecutionContext.setParallelWindowEnabled(false);
            Assert.assertEquals(0, assertBlocksMatchRows(engine, sqlExecutionContext, query, rnd));
        });
    }

    @Test
    public void testAsyncWindowOffersTaskChainsOnWorkerPool() throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(pool, (engine, compiler, context) -> {
                final SqlExecutionContextImpl ctx = (SqlExecutionContextImpl) context;
                ctx.changePageFrameSizes(1, 64);
                createWindowTable(engine, ctx);
                final Rnd rnd = TestUtils.generateRandom(LOG);
                ctx.setParallelWindowEnabled(true);
                final String query = windowQuery("('A', 'B', 'C3', 'C7', 'D', null)");
                for (int run = 0; run < 3; run++) {
                    Assert.assertTrue(assertBlocksMatchRows(engine, ctx, query, rnd) > 0);
                }
                Assert.assertTrue(assertBlocksMatchRows(engine, ctx, "select * from (" + query + ") limit 3001", rnd) > 0);
            }, configuration, LOG);
        });
    }

    @Test
    public void testFilteredAndBackwardScansOfferNone() throws Exception {
        assertMemoryLeak(() -> {
            createAllTypes();
            final Rnd rnd = TestUtils.generateRandom(LOG);
            // the filtered results' top cursors are filter wrappers, which offer no blocks at all;
            // the backward scan is a page frame scan that refuses them
            assertSupportsBlocks(false, "select * from at where l > 5000 limit 100000");
            assertSupportsBlocks(false, "select * from at order by ts desc limit 100000");
            assertSupportsBlocks(false, "select * from at where s = 'k7' limit 100000");
            Assert.assertEquals(0, assertBlocksMatchRows(engine, sqlExecutionContext, "select * from at where l > 5000 limit 100000", rnd));
            Assert.assertEquals(0, assertBlocksMatchRows(engine, sqlExecutionContext, "select * from at order by ts desc limit 100000", rnd));
            Assert.assertEquals(0, assertBlocksMatchRows(engine, sqlExecutionContext, "select * from at where s = 'k7' limit 100000", rnd));
        });
    }

    @Test
    public void testParquetFramesOfferNone() throws Exception {
        assertMemoryLeak(() -> {
            createAllTypes();
            execute("alter table at convert partition to parquet list '1970-01-01', '1970-01-03'");
            final Rnd rnd = TestUtils.generateRandom(LOG);
            // the native partitions still offer theirs
            Assert.assertTrue(assertBlocksMatchRows(engine, sqlExecutionContext, "select * from at limit 1000000", rnd) > 0);
            Assert.assertTrue(assertBlocksMatchRows(engine, sqlExecutionContext, "select * from at limit 50, 2000", rnd) > 0);
            Assert.assertEquals(0, assertBlocksMatchRows(engine, sqlExecutionContext, "select * from at where ts < '1970-01-02' limit 100000", rnd));
        });
    }

    @Test
    public void testScanOffersFrames() throws Exception {
        assertMemoryLeak(() -> {
            createAllTypes();
            final Rnd rnd = TestUtils.generateRandom(LOG);
            Assert.assertTrue(assertBlocksMatchRows(engine, sqlExecutionContext, "at", rnd) > 0);
            Assert.assertTrue(assertBlocksMatchRows(engine, sqlExecutionContext, "select * from at limit 1000000", rnd) > 0);
            Assert.assertTrue(assertBlocksMatchRows(engine, sqlExecutionContext, "select * from at limit 7, 1501", rnd) > 0);
            Assert.assertTrue(assertBlocksMatchRows(engine, sqlExecutionContext, "select * from at limit -333", rnd) > 0);
            Assert.assertEquals(0, assertBlocksMatchRows(engine, sqlExecutionContext, "select * from at limit 0", rnd));
            Assert.assertTrue(assertBlocksMatchRows(engine, sqlExecutionContext, "select db, s, l, ts from at limit 100000", rnd) > 0);
            Assert.assertTrue(assertBlocksMatchRows(engine, sqlExecutionContext, "select * from at where ts in '1970-01-02' limit 100000", rnd) > 0);
        });
    }

    @Test
    public void testScanIntervalsAndShortLimits() throws Exception {
        assertMemoryLeak(() -> {
            createAllTypes();
            final Rnd rnd = TestUtils.generateRandom(LOG);
            for (int k = 0; k < 5; k++) {
                // two intervals; and LIMITs that end inside the first frame or before the last row
                Assert.assertTrue(assertBlocksMatchRows(engine, sqlExecutionContext, "select * from at where ts in '1970-01-02' or ts in '1970-01-04' limit 100000", rnd) > 0);
                Assert.assertTrue(assertBlocksMatchRows(engine, sqlExecutionContext, "select * from at where ts > '1970-01-02T05' limit -5, -1", rnd) >= 0);
                Assert.assertTrue(assertBlocksMatchRows(engine, sqlExecutionContext, "select * from at limit 3", rnd) >= 0);
                Assert.assertTrue(assertBlocksMatchRows(engine, sqlExecutionContext, "select * from (select * from at limit 400) limit 30, 350", rnd) > 0);
                Assert.assertTrue(assertBlocksMatchRows(engine, sqlExecutionContext, "select * from at order by ts limit 100000", rnd) > 0);
            }
        });
    }

    @Test
    public void testSupportsRecordBlocks() throws Exception {
        assertMemoryLeak(() -> {
            createAllTypes();
            createWindowTable(engine, sqlExecutionContext);
            sqlExecutionContext.setParallelWindowEnabled(true);
            // forward scans, and LIMIT over them
            assertSupportsBlocks(true, "at");
            assertSupportsBlocks(true, "select * from at limit 10");
            assertSupportsBlocks(true, "select * from at limit -10");
            assertSupportsBlocks(true, "select * from (select * from at limit 400) limit 30, 350");
            assertSupportsBlocks(true, windowQuery("('A', 'B')"));
            assertSupportsBlocks(true, "select * from (" + windowQuery("('A', 'B')") + ") limit 100");
            // none of these ever offers a block, so egress never asks them per row
            assertSupportsBlocks(false, "select l + 1, s from at");
            assertSupportsBlocks(false, "select * from at order by l limit 10");
            assertSupportsBlocks(false, "select s, count() from at");
            assertSupportsBlocks(false, "select s, count() from at limit 3");
            assertSupportsBlocks(false, "select * from at where b");
        });
    }

    @Test
    public void testWindowNegativeAndNestedLimits() throws Exception {
        assertMemoryLeak(() -> {
            createWindowTable(engine, sqlExecutionContext);
            final Rnd rnd = TestUtils.generateRandom(LOG);
            sqlExecutionContext.setParallelWindowEnabled(true);
            final String query = windowQuery("('A', 'B', 'C3', 'C7', 'D', null)");
            for (int k = 0; k < 5; k++) {
                Assert.assertTrue(assertBlocksMatchRows(engine, sqlExecutionContext, "select * from (" + query + ") limit -1500", rnd) > 0);
                Assert.assertTrue(assertBlocksMatchRows(engine, sqlExecutionContext, "select * from (" + query + ") limit -2500, -10", rnd) > 0);
                Assert.assertTrue(assertBlocksMatchRows(engine, sqlExecutionContext, "select * from (select * from (" + query + ") limit 2000) limit 100, 1900", rnd) > 0);
            }
        });
    }

    @Test
    public void testWindowRunningCarry() throws Exception {
        assertMemoryLeak(() -> {
            createWindowTable(engine, sqlExecutionContext);
            final Rnd rnd = TestUtils.generateRandom(LOG);
            sqlExecutionContext.setParallelWindowEnabled(true);
            // running windows: a continuing task's rows hold the key's running values only once
            // the carry is applied, in place, before the task is emitted; blocks expose those rows.
            // No ORDER BY in OVER: with one, this plans as the cached window
            final String query = "select sym, ts, l, i, " +
                    "sum(l) over (partition by sym rows between unbounded preceding and current row) s, " +
                    "count(*) over (partition by sym rows between unbounded preceding and current row) c, " +
                    "row_number() over (partition by sym) rn, " +
                    "max(i) over (partition by sym rows between unbounded preceding and current row) mx " +
                    "from w where sym in ('A', 'B', 'C3', 'C7', 'D', null) order by sym";
            printSql("explain " + query);
            TestUtils.assertContains(sink, "Async Window");
            TestUtils.assertContains(sink, "keySplit: running carry");
            for (int k = 0; k < 5; k++) {
                Assert.assertTrue(assertBlocksMatchRows(engine, sqlExecutionContext, query, rnd) > 0);
                Assert.assertTrue(assertBlocksMatchRows(engine, sqlExecutionContext, "select * from (" + query + ") limit 333, 4444", rnd) > 0);
            }
        });
    }

    private static void assertAddresses(RecordBlock block, int row, Record record, RecordMetadata metadata) {
        for (int c = 0, n = metadata.getColumnCount(); c < n; c++) {
            final long address = block.getColumnAddress(c);
            if (address == 0) {
                continue;
            }
            final int type = metadata.getColumnType(c);
            Assert.assertFalse(ColumnType.isVarSize(type));
            final long p = address + row * block.getColumnStride(c);
            final String msg = metadata.getColumnName(c);
            switch (ColumnType.tagOf(type)) {
                case ColumnType.BOOLEAN -> Assert.assertEquals(msg, record.getBool(c), Unsafe.getByte(p) == 1);
                case ColumnType.BYTE -> Assert.assertEquals(msg, record.getByte(c), Unsafe.getByte(p));
                case ColumnType.SHORT -> Assert.assertEquals(msg, record.getShort(c), Unsafe.getShort(p));
                case ColumnType.CHAR -> Assert.assertEquals(msg, record.getChar(c), Unsafe.getChar(p));
                case ColumnType.INT, ColumnType.SYMBOL -> Assert.assertEquals(msg, record.getInt(c), Unsafe.getInt(p));
                case ColumnType.IPv4 -> Assert.assertEquals(msg, record.getIPv4(c), Unsafe.getInt(p));
                case ColumnType.LONG -> Assert.assertEquals(msg, record.getLong(c), Unsafe.getLong(p));
                case ColumnType.DATE -> Assert.assertEquals(msg, record.getDate(c), Unsafe.getLong(p));
                case ColumnType.TIMESTAMP -> Assert.assertEquals(msg, record.getTimestamp(c), Unsafe.getLong(p));
                case ColumnType.DECIMAL64 -> Assert.assertEquals(msg, record.getDecimal64(c), Unsafe.getLong(p));
                case ColumnType.FLOAT ->
                        Assert.assertEquals(msg, Float.floatToRawIntBits(record.getFloat(c)), Unsafe.getInt(p));
                case ColumnType.DOUBLE ->
                        Assert.assertEquals(msg, Double.doubleToRawLongBits(record.getDouble(c)), Unsafe.getLong(p));
                case ColumnType.UUID -> {
                    Assert.assertEquals(msg, record.getLong128Lo(c), Unsafe.getLong(p));
                    Assert.assertEquals(msg, record.getLong128Hi(c), Unsafe.getLong(p + 8));
                }
                case ColumnType.LONG256 -> {
                    final Long256 v = record.getLong256A(c);
                    Assert.assertEquals(msg, v.getLong0(), Unsafe.getLong(p));
                    Assert.assertEquals(msg, v.getLong1(), Unsafe.getLong(p + 8));
                    Assert.assertEquals(msg, v.getLong2(), Unsafe.getLong(p + 16));
                    Assert.assertEquals(msg, v.getLong3(), Unsafe.getLong(p + 24));
                }
                default -> {
                    if (ColumnType.isGeoHash(type)) {
                        switch (ColumnType.sizeOf(type)) {
                            case 1 -> Assert.assertEquals(msg, record.getGeoByte(c), Unsafe.getByte(p));
                            case 2 -> Assert.assertEquals(msg, record.getGeoShort(c), Unsafe.getShort(p));
                            case 4 -> Assert.assertEquals(msg, record.getGeoInt(c), Unsafe.getInt(p));
                            default -> Assert.assertEquals(msg, record.getGeoLong(c), Unsafe.getLong(p));
                        }
                    }
                }
            }
        }
    }

    private static void assertAsync(String query) throws Exception {
        try (RecordCursorFactory factory = select(query)) {
            boolean found = false;
            for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
                found |= f instanceof AsyncWindowRecordCursorFactory;
            }
            Assert.assertTrue("Async Window expected", found);
        }
    }

    /**
     * Reads the query row by row, then again mixing blocks and rows, and compares the two.
     *
     * @return rows read from blocks
     */
    private static long assertBlocksMatchRows(CairoEngine engine, SqlExecutionContext ctx, String query, Rnd rnd) throws Exception {
        final StringSink expected = new StringSink();
        final StringSink actual = new StringSink();
        long blockRows = 0;
        try (RecordCursorFactory factory = engine.select(query, ctx)) {
            final RecordMetadata metadata = factory.getMetadata();
            try (RecordCursor cursor = factory.getCursor(ctx)) {
                final Record record = cursor.getRecord();
                while (cursor.hasNext()) {
                    CursorPrinter.println(record, metadata, expected);
                }
            }
            try (RecordCursor cursor = factory.getCursor(ctx)) {
                final Record record = cursor.getRecord();
                // asked once per open; a cursor that says no must never offer a block
                final boolean supportsBlocks = cursor.supportsRecordBlocks();
                while (true) {
                    final RecordBlock block = rnd.nextInt(4) > 0 ? cursor.peekRecordBlock(1 + rnd.nextInt(300)) : null;
                    if (block != null) {
                        Assert.assertTrue("a block from a cursor that does not support them", supportsBlocks);
                        final int rows = block.getRowCount();
                        Assert.assertTrue(rows > 0);
                        final int taken = rnd.nextInt(5) == 0 ? 0 : 1 + rnd.nextInt(rows);
                        for (int r = 0; r < taken; r++) {
                            final Record blockRecord = block.getRecordAt(r);
                            assertAddresses(block, r, blockRecord, metadata);
                            CursorPrinter.println(blockRecord, metadata, actual);
                        }
                        cursor.skipRecordBlock(taken);
                        blockRows += taken;
                        continue;
                    }
                    if (!cursor.hasNext()) {
                        break;
                    }
                    CursorPrinter.println(record, metadata, actual);
                }
            }
        }
        TestUtils.assertEquals(query, expected, actual);
        return blockRows;
    }

    private static void assertSupportsBlocks(boolean expected, String query) throws Exception {
        try (RecordCursorFactory factory = select(query); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Assert.assertEquals(query, expected, cursor.supportsRecordBlocks());
        }
    }

    private static void createWindowTable(CairoEngine engine, SqlExecutionContext ctx) throws Exception {
        // keys A and B above max.key.rows, the C keys small, NULL keys too; passthrough columns of
        // every fixed-size type a task chain can hold
        engine.execute("create table w (sym symbol index type posting, ex symbol, b boolean, by byte, sh short, ch char, " +
                "i int, ip ipv4, l long, d date, f float, db double, u uuid, g6 geohash(6c), dc64 decimal(12,2), " +
                "bid double, v varchar, ts timestamp) timestamp(ts) partition by DAY", ctx);
        engine.execute("insert into w select " +
                "case when x % 3 = 0 then 'A' when x % 5 = 0 then 'B' when x % 23 = 0 then null when x % 29 = 0 then 'D' else 'C' || (x % 9) end, " +
                "rnd_symbol('N', 'P', null), x % 2 = 0, (x % 100)::byte, (x % 999)::short, rnd_char(), " +
                "case when x % 11 = 0 then null else x::int end, rnd_ipv4(), case when x % 13 = 0 then null else x end, " +
                "(x * 1000)::date, case when x % 4 = 0 then null else (x / 7.0)::float end, x * 0.5, rnd_uuid4(), rnd_geohash(30), " +
                "rnd_decimal(12,2,5), case when x % 7 = 0 then null else rnd_double() end, 'v' || x, " +
                "(x * 30000000)::timestamp from long_sequence(6000)", ctx);
    }

    private static String windowQuery(String keys) {
        return "select sym, ex, b, by, sh, ch, i, ip, l, d, f, db, u, g6, dc64, ts, " +
                "avg(bid) over (partition by sym rows between 4 preceding and current row) a, " +
                "count(bid) over (partition by sym rows between 4 preceding and current row) c, " +
                "sum(l) over (partition by sym rows between 2 preceding and current row) sl " +
                "from w where sym in " + keys + " order by sym";
    }

    private static String windowQueryWithVarchar() {
        return "select sym, v, avg(bid) over (partition by sym rows between 4 preceding and current row) a " +
                "from w where sym in ('A', 'C1', 'C2') order by sym";
    }

    private void createAllTypes() throws Exception {
        execute(ALL_TYPES_DDL.replace(", u uuid, l256 long256, g1 geohash(1c), g3 geohash(3c), g6 geohash(6c), " +
                "g12 geohash(12c), dc64 decimal(12,2), dc128 decimal(30,4), dc256 decimal(60,6), v varchar, st string, bn binary, " +
                "arr double[]", ""));
        // the first rows have column tops for the columns added below
        execute("insert into at select x % 3 = 0, (x % 120)::byte, (x * 7 % 30000)::short, rnd_char(), x::int, rnd_ipv4(), x, " +
                "(x * 86400000)::date, (x * 900000000)::timestamp, (x * 1000)::timestamp_ns, (x / 3.0)::float, x * 1.5, " +
                "'k' || (x % 50), 'z' || (x % 7) from long_sequence(150)");
        execute("alter table at add column u uuid, l256 long256, g1 geohash(1c), g3 geohash(3c), g6 geohash(6c), " +
                "g12 geohash(12c), dc64 decimal(12,2), dc128 decimal(30,4), dc256 decimal(60,6), v varchar, st string, bn binary, arr double[]");
        execute("insert into at " + ALL_TYPES_SELECT.replace("(x * 900000000)::timestamp", "((x + 150) * 900000000)::timestamp") + " from long_sequence(600)");
    }
}
