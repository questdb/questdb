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
import io.questdb.std.MemoryTracker;
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
    public void testAsyncAsOfJoinOffersFrameRows() throws Exception {
        // many master page frames and slave time frames
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MIN_ROWS, 16);
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MAX_ROWS, 64);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 16);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 64);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(pool, (engine, compiler, ctx) -> {
                createAllTypes(engine, ctx);
                // trades keyed like at.s, some keys never quoted, NULL keys, trades before any quote
                engine.execute("create table tr as (select (case when x % 23 = 0 then null else 'k' || (x % 60) end)::symbol s, " +
                        "(x * 330000000 - 300000000)::timestamp ts, x from long_sequence(2200)) timestamp(ts) partition by DAY", ctx);
                final Rnd rnd = TestUtils.generateRandom(LOG);
                final String all = "select /*+ asof_parallel(tr at) */ tr.ts, tr.s, tr.x, at.* from tr asof join at on (s)";
                final String filtered = "select /*+ asof_parallel(tr at) */ tr.ts, tr.x, at.l, at.s, at.db, at.v, at.dc64 from tr asof join at on (s) where tr.x % 3 = 0";
                final String tolerance = "select /*+ asof_parallel(tr at) */ tr.ts, at.i, at.f, at.ts, at.s2 from tr asof join at on (s) tolerance 2h";
                for (String query : new String[]{all, filtered, tolerance}) {
                    io.questdb.test.griffin.engine.join.AsyncAsOfJoinTest.assertParallel(engine, ctx, query, true);
                    for (int k = 0; k < 3; k++) {
                        Assert.assertTrue(query, assertBlocksMatchRows(engine, ctx, query, rnd) > 0);
                        Assert.assertTrue(query, assertBlocksMatchRows(engine, ctx, query + " limit 3, 250", rnd) > 0);
                    }
                }
            }, configuration, LOG);
        });
    }

    @Test
    public void testAsyncFilterOffersSelectedRows() throws Exception {
        assertMemoryLeak(() -> {
            createAllTypes();
            final Rnd rnd = TestUtils.generateRandom(LOG);
            for (int k = 0; k < 3; k++) {
                assertFilterBlocks(engine, sqlExecutionContext, rnd);
            }
            // a backward scan returns each frame's rows in reverse, a negative LIMIT from a list
            // of its own: neither offers blocks
            assertSupportsBlocks(false, "select * from at where l > 5000 order by ts desc");
            assertSupportsBlocks(false, "select * from at where l > 5000 limit -100");
            Assert.assertEquals(0, assertBlocksMatchRows(engine, sqlExecutionContext, "select * from at where l > 5000 order by ts desc", rnd));
            Assert.assertEquals(0, assertBlocksMatchRows(engine, sqlExecutionContext, "select * from at where l > 5000 limit -100", rnd));
            // no selected row
            Assert.assertEquals(0, assertBlocksMatchRows(engine, sqlExecutionContext, "select * from at where l < 0", rnd));
        });
    }

    @Test
    public void testAsyncFilterOffersSelectedRowsOnWorkerPool() throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(pool, (engine, compiler, context) -> {
                final SqlExecutionContextImpl ctx = (SqlExecutionContextImpl) context;
                ctx.changePageFrameSizes(1, 64);
                createAllTypes(engine, ctx);
                final Rnd rnd = TestUtils.generateRandom(LOG);
                for (int run = 0; run < 3; run++) {
                    assertFilterBlocks(engine, ctx, rnd);
                }
            }, configuration, LOG);
        });
    }

    @Test
    public void testAsyncFilterOverParquetFrames() throws Exception {
        assertMemoryLeak(() -> {
            createAllTypes();
            execute("alter table at convert partition to parquet list '1970-01-01', '1970-01-03'");
            final Rnd rnd = TestUtils.generateRandom(LOG);
            // Parquet frames: the filter's columns decode first, the others for the selected rows
            // only (late materialization); blocks gather from the decoded buffers
            for (int k = 0; k < 3; k++) {
                assertFilterBlocks(engine, sqlExecutionContext, rnd);
                Assert.assertTrue(assertBlocksMatchRows(engine, sqlExecutionContext, "select * from at where i > 30 and ts < '1970-01-02'", rnd) > 0);
            }
        });
    }

    @Test
    public void testBackwardScanOffersNone() throws Exception {
        assertMemoryLeak(() -> {
            createAllTypes();
            final Rnd rnd = TestUtils.generateRandom(LOG);
            assertSupportsBlocks(false, "select * from at order by ts desc limit 100000");
            Assert.assertEquals(0, assertBlocksMatchRows(engine, sqlExecutionContext, "select * from at order by ts desc limit 100000", rnd));
        });
    }

    @Test
    public void testHashJoinLightOffersMasterRows() throws Exception {
        assertMemoryLeak(() -> {
            createJoinTable();
            final Rnd rnd = TestUtils.generateRandom(LOG);
            sqlExecutionContext.setParallelHashJoinProbeEnabled(true);
            // idx 59 and 70's Manual Opt shapes: a semi-join, master columns only
            final String mo59 = "select t.ts, t.ex, t.sym, t.v, t.size, t.price, t.x from t t " +
                    "join (select ex mex, sym msym, min(size) min_size from t) m on t.ex = m.mex and t.sym = m.msym and t.size = m.min_size";
            final String mo70 = "select ts, ex, sym, v, size, price, x from t " +
                    "join (select sym msym, min(price) min_price from t) m on t.sym = m.msym where price = min_price";
            // slave columns too, read through the block's record
            final String withSlave = "select t.ts, m.msym, t.price, m.min_price, t.x from t " +
                    "join (select sym msym, min(price) min_price from t) m on t.sym = m.msym where price = min_price";
            for (String query : new String[]{mo59, mo70, withSlave}) {
                assertPlanContains(query, "Async Hash Join Light");
                for (int k = 0; k < 3; k++) {
                    Assert.assertTrue(query, assertBlocksMatchRows(engine, sqlExecutionContext, query, rnd) > 0);
                    Assert.assertTrue(query, assertBlocksMatchRows(engine, sqlExecutionContext, query + " limit 3, 250", rnd) > 0);
                }
            }
        });
    }

    @Test
    public void testIndexScansOfferRowLists() throws Exception {
        assertMemoryLeak(() -> {
            for (String indexType : new String[]{"posting", "bitmap"}) {
                execute("drop table if exists ix");
                execute("create table ix (ts timestamp, x long) timestamp(ts) partition by DAY");
                execute("insert into ix select (x * 900000000)::timestamp, x from long_sequence(150)");
                // the first rows are column tops of the columns added here
                execute("alter table ix add column s symbol index type " + indexType + ", b boolean, sh short, i int, " +
                        "f float, d double, l long, s2 symbol, u uuid, v varchar");
                execute("insert into ix select ((x + 150) * 900000000)::timestamp, x, " +
                        "case when x % 17 = 0 then null else 'k' || (x % 13) end, x % 3 = 0, (x % 999)::short, " +
                        "case when x % 11 = 0 then null else x::int end, case when x % 4 = 0 then null else (x / 3.0)::float end, " +
                        "case when x % 7 = 0 then null else x * 1.5 end, case when x % 5 = 0 then null else x * 1000003 end, " +
                        "'z' || (x % 7), rnd_uuid4(), 'v' || x from long_sequence(1200)");
                final Rnd rnd = TestUtils.generateRandom(LOG);
                final String[] queries = {
                        "select * from ix where s = 'k7'",
                        "select * from ix where s = 'k7' and ts in '1970-01-03'",
                        "select * from ix where s = 'k7' and ts > '1970-01-02T05' limit 40",
                        "select * from ix where s = 'k7' and l > 3000",
                        "select * from ix where s in ('k1', 'k7', 'k11')",
                        "select * from ix where s = null",
                        "select ts, s, d, s2 from ix where s = 'k3'",
                };
                for (String query : queries) {
                    assertSupportsBlocks(true, query);
                    for (int k = 0; k < 3; k++) {
                        Assert.assertTrue(query, assertBlocksMatchRows(engine, sqlExecutionContext, query, rnd) > 0);
                    }
                }
                assertPlanContains(queries[0], "Index forward scan on: s");
            }
        });
    }

    @Test
    public void testIndexScanFilterErrorComesAfterTheRowsBeforeIt() throws Exception {
        assertMemoryLeak(() -> {
            for (String indexType : new String[]{"posting", "bitmap"}) {
                execute("drop table if exists fe");
                execute("create table fe (s symbol index type " + indexType + ", v varchar, x long, ts timestamp) timestamp(ts) partition by DAY");
                // the filter's implicit cast fails on x = 2422 alone: in the second partition, and not
                // on the first row of a page frame, which hasNext() reads; the rows after it are fine
                execute("insert into fe select 'k' || (x % 3), case when x = 2422 then 'bad' else '1970-01-01' end, x, " +
                        "(x * 60000000)::timestamp from long_sequence(3000)");
                final String query = "select * from fe where s = 'k1' and ts > v";
                assertPlanContains(query, "Index forward scan on: s");
                // the row fill returns every row before the failing one, then fails; reading ahead
                // must not fail any earlier
                final String expected = readRowByRow(query);
                TestUtils.assertContains(expected, "inconvertible value");
                for (int maxRows : new int[]{1, 7, 100, 5000}) {
                    TestUtils.assertEquals(query + ", maxRows=" + maxRows, expected, readPeekingBeforeEveryRow(query, maxRows));
                }
                // the rows read ahead and the error kept for hasNext() go with toTop() and a reopen:
                // row 801 is x = 2401, the first of the failing row's page frame (64 rows from 2400),
                // the peek after it reads ahead to the failing row
                assertReadAheadStateResets(query, expected, 801);
            }
        });
    }

    @Test
    public void testLatestOnIndexedScans() throws Exception {
        assertMemoryLeak(() -> {
            for (String indexType : new String[]{"posting", "bitmap"}) {
                execute("drop table if exists ix");
                execute("create table ix (s symbol index type " + indexType + ", s2 symbol, i int, d double, ts timestamp) " +
                        "timestamp(ts) partition by DAY");
                execute("insert into ix select case when x % 17 = 0 then null else 'k' || (x % 13) end, 'z' || (x % 5), x::int, " +
                        "x * 1.5, (x * 900000000)::timestamp from long_sequence(1500)");
                final Rnd rnd = TestUtils.generateRandom(LOG);
                final String[] queries = {
                        // the row cursor of one value's latest row ends the scan with NoMoreFramesException
                        "select * from ix where s = 'k7' latest on ts partition by s",
                        "select * from ix where s = 'k7' and ts < '1970-01-10' latest on ts partition by s",
                        "select ts, s, d from ix where s = 'k7' latest on ts partition by s",
                        "select ts, s, d + i, s2 from ix where s = 'k7' latest on ts partition by s",
                        "select * from (select * from ix where s = 'k7' latest on ts partition by s) limit 1",
                        "select * from ix latest by s where s = 'k7'",
                        // a value the symbol table does not hold yet: the deferred cursor
                        "select * from ix where s = 'k99' latest on ts partition by s",
                        "select * from ix where s = 'k7' and i > 10 latest on ts partition by s",
                        "select * from ix where s in ('k1', 'k7', 'k11') latest on ts partition by s",
                        "select * from ix where s = null latest on ts partition by s",
                        "select * from ix latest on ts partition by s",
                        "select * from ix where i > 10 latest on ts partition by s",
                        "select * from ix latest on ts partition by s2",
                        "select * from ix latest on ts partition by s, s2",
                };
                assertPlanContains(queries[0], "Index backward scan on: s");
                for (String query : queries) {
                    // as the egress loop reads: a peek before every row
                    final String expected = readRowByRow(query);
                    for (int maxRows : new int[]{1, 2, 1000}) {
                        TestUtils.assertEquals(query + ", maxRows=" + maxRows, expected, readPeekingBeforeEveryRow(query, maxRows));
                    }
                    for (int k = 0; k < 3; k++) {
                        assertBlocksMatchRows(engine, sqlExecutionContext, query, rnd);
                    }
                    assertReadAheadStateResets(query, expected, 1);
                }
            }
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
    public void testProjectionsPassBlocksThrough() throws Exception {
        assertMemoryLeak(() -> {
            allowFunctionMemoization();
            createAllTypes();
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final String[] queries = {
                    // column references read the base's memory, expressions go row by row
                    "select ts, (db + l) / 2 mid, s, l, f from at where l > 5000",
                    // idx 13's shape: a projection over a selection of every column
                    "select ts, (db + f) / 2 mid from (select * from at where s = 'k7')",
                    "select ts, s, db, b, ip from at where i > 100",
                    "select l + 1 a, a * 2 a2, a - 3 a3, s, ts from at where l > 100",
                    // a column the outer projection reads three times is memoized in the inner one,
                    // which computes it into memory the outer one cannot gather with its other
                    // columns, so the outer reads it through the record, and the memo, per row
                    "select a, a * 2 a2, a - 3 a3, s from (select l + i a, s, ts from at where l > 100)",
                    "select s, s2, l256, v, i * 2 from at where l > 100",
                    "select case when b then s else s2 end cs, l from at where l > 100",
                    // a projection over a plain scan
                    "select l + 1, s, ts from at",
                    "select ts, (db + l) / 2 mid, s from at where l > 5000 limit 10, 400",
            };
            for (String query : queries) {
                assertSupportsBlocks(true, query);
                for (int k = 0; k < 3; k++) {
                    Assert.assertTrue(query, assertBlocksMatchRows(engine, sqlExecutionContext, query, rnd) > 0);
                }
            }
            // the alias read twice is memoized, as in a server: a block's rows must each clear the memo
            assertPlanContains("select l + 1 a, a * 2 a2, a - 3 a3, s, ts from at where l > 100", "memoize(");
            // arithmetic over base columns is computed column-wise, into memory the block exposes;
            // a function with no column-wise loop (concat, a CASE) is read through the record
            assertComputedInMemory("select ts, (db + l) / 2 mid, l * 3 l3, (f - i)::double fi, s from at where l > 5000", true, true, true);
            assertComputedInMemory("select ts, (db + f) / 2 mid, s || 'x' sx, case when b then l end cl from at where i > 0", true, false, false);
            // a computed SYMBOL column would be read ahead of the other columns: no blocks
            assertSupportsBlocks(false, "select v::symbol vs, l from at where l > 100");
            Assert.assertEquals(0, assertBlocksMatchRows(engine, sqlExecutionContext, "select v::symbol vs, l from at where l > 100", rnd));
        });
    }

    @Test
    public void testProjectionKernelBuffersAreChargedToTheQuery() throws Exception {
        assertMemoryLeak(() -> {
            createAllTypes();
            // a plain scan, which allocates nothing of its own for native frames
            final String query = "select ts, (db + l) / 2 mid, l * 3 l3 from at";
            try (RecordCursorFactory factory = select(query)) {
                // twice: the second open reuses the factory's cursor and its compiled kernels
                for (int k = 0; k < 2; k++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        // the query's tracker, which the query registry binds while the cursor is open
                        final MemoryTracker tracker = sqlExecutionContext.getMemoryTracker();
                        Assert.assertNotNull(tracker);
                        // a scan offers no block before its first row
                        Assert.assertTrue(cursor.hasNext());
                        final long usedBefore = tracker.getUsed();
                        final RecordBlock block = cursor.peekRecordBlock(1000);
                        Assert.assertNotNull(block);
                        // computed column-wise into the kernels' buffers
                        Assert.assertNotEquals(0, block.getColumnAddress(1));
                        Assert.assertNotEquals(0, block.getColumnAddress(2));
                        Assert.assertTrue("the kernel buffers are charged to the query", tracker.getUsed() > usedBefore);
                    }
                }
            }
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
            // filters, and projections over blocks
            assertSupportsBlocks(true, "select * from at where b");
            assertSupportsBlocks(true, "select l + 1, s from at");
            assertSupportsBlocks(true, "select l + 1, s from at where b limit 5");
            // none of these ever offers a block, so egress never asks them per row
            assertSupportsBlocks(false, "select * from at order by l limit 10");
            assertSupportsBlocks(false, "select s, count() from at");
            assertSupportsBlocks(false, "select s, count() from at limit 3");
        });
    }

    @Test
    public void testWindowMinMaxFilterOffersBaseRows() throws Exception {
        assertMemoryLeak(() -> {
            createJoinTable();
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final String c = "ts, ex, sym, v, size, price, x";
            final String[] queries = {
                    // idx 59 and 70: base columns only
                    "select " + c + " from (select " + c + " from (select " + c + ", min(size) over (partition by ex, sym) min_size from t) where size = min_size)",
                    "select " + c + " from (select " + c + " from (select " + c + ", min(price) over (partition by sym) min_price from t) where price = min_price)",
                    // the window columns too, looked up through the block's record
                    "select " + c + ", mn, mx from (select " + c + ", min(price) over (partition by sym, v) mn, " +
                            "max(price) over (partition by sym, v) mx from t) where price = mn or price = mx",
            };
            for (String query : queries) {
                assertPlanContains(query, "Async Window Min/Max Filter");
                for (int k = 0; k < 3; k++) {
                    Assert.assertTrue(query, assertBlocksMatchRows(engine, sqlExecutionContext, query, rnd) > 0);
                    Assert.assertTrue(query, assertBlocksMatchRows(engine, sqlExecutionContext, query + " limit 2, 300", rnd) > 0);
                }
            }
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
            final long rowIndexes = block.getColumnRowIndexesAddress(c);
            final long position = rowIndexes == 0 ? row : Unsafe.getLong(rowIndexes + 8L * row);
            final long p = address + position * block.getColumnStride(c);
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
                // NaN is NULL, whatever its bits
                case ColumnType.FLOAT -> Assert.assertEquals(msg,
                        Float.floatToIntBits(record.getFloat(c)), Float.floatToIntBits(Unsafe.getFloat(p)));
                case ColumnType.DOUBLE -> Assert.assertEquals(msg,
                        Double.doubleToLongBits(record.getDouble(c)), Double.doubleToLongBits(Unsafe.getDouble(p)));
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

    private static void assertFilterBlocks(CairoEngine engine, SqlExecutionContext ctx, Rnd rnd) throws Exception {
        final String[] queries = {
                // JIT-compiled, over NULLs and column tops
                "select * from at where l > 5000",
                "select * from at where l > 5000 limit 1000",
                "select * from at where l > 5000 limit 7, 1501",
                "select * from at where s = 'k7'",
                "select * from at where i > 100 and b",
                // a Java filter
                "select * from at where v like '%3%'",
                "select * from at where i > 100 and ts in '1970-01-03'",
                // the selection keeps one row in a few, so most frames hold a handful
                "select * from at where l % 97 = 0",
        };
        for (String query : queries) {
            Assert.assertTrue(query, assertBlocksMatchRows(engine, ctx, query, rnd) > 0);
        }
    }

    // per column after the first, whether the first block holds its values in memory
    private static void assertComputedInMemory(String query, boolean... inMemory) throws Exception {
        try (RecordCursorFactory factory = select(query); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Assert.assertTrue(cursor.supportsRecordBlocks());
            final RecordBlock block = cursor.peekRecordBlock(1000);
            Assert.assertNotNull(block);
            for (int c = 0; c < inMemory.length; c++) {
                Assert.assertEquals(query + ", column " + (c + 1), inMemory[c], block.getColumnAddress(c + 1) != 0);
            }
        }
    }

    private static void assertPlanContains(String query, String fragment) throws Exception {
        printSql("explain " + query);
        TestUtils.assertContains(sink, fragment);
    }

    private static void assertSupportsBlocks(boolean expected, String query) throws Exception {
        try (RecordCursorFactory factory = select(query); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Assert.assertEquals(query, expected, cursor.supportsRecordBlocks());
        }
    }

    /**
     * Reads the first rows, then peeks, which reads ahead and may keep the error the row cursor
     * threw; then reads the query again after toTop(), and after closing and reopening the cursor:
     * neither may see what the first read left behind.
     */
    private static void assertReadAheadStateResets(String query, String expected, int rowsFirst) throws Exception {
        try (RecordCursorFactory factory = select(query)) {
            final RecordMetadata metadata = factory.getMetadata();
            for (int open = 0; open < 2; open++) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    for (int i = 0; i < rowsFirst && cursor.hasNext(); i++) {
                        cursor.peekRecordBlock(10_000);
                    }
                    cursor.peekRecordBlock(10_000);
                    if (open == 0) {
                        cursor.toTop();
                        final StringSink actual = new StringSink();
                        final Record record = cursor.getRecord();
                        try {
                            while (cursor.hasNext()) {
                                CursorPrinter.println(record, metadata, actual);
                            }
                        } catch (RuntimeException e) {
                            actual.put("error: ").put(e.getMessage());
                        }
                        TestUtils.assertEquals(query + ", after toTop()", expected, actual);
                    }
                }
            }
            // the second open's leftovers, read on a third open
            TestUtils.assertEquals(query + ", reopened", expected, readRowByRow(factory));
        }
    }

    /**
     * The query's rows as the egress loop reads them: a peek before every row, every row of a block
     * taken, else one row from hasNext(). Ends with the error the cursor threw, if any.
     */
    private static String readPeekingBeforeEveryRow(String query, int maxRows) throws Exception {
        final StringSink actual = new StringSink();
        try (RecordCursorFactory factory = select(query)) {
            final RecordMetadata metadata = factory.getMetadata();
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                final Record record = cursor.getRecord();
                try {
                    while (true) {
                        final RecordBlock block = cursor.peekRecordBlock(maxRows);
                        if (block != null) {
                            final int rows = block.getRowCount();
                            Assert.assertTrue(rows > 0 && rows <= maxRows);
                            for (int r = 0; r < rows; r++) {
                                final Record blockRecord = block.getRecordAt(r);
                                assertAddresses(block, r, blockRecord, metadata);
                                CursorPrinter.println(blockRecord, metadata, actual);
                            }
                            cursor.skipRecordBlock(rows);
                            continue;
                        }
                        if (!cursor.hasNext()) {
                            break;
                        }
                        CursorPrinter.println(record, metadata, actual);
                    }
                } catch (RuntimeException e) {
                    actual.put("error: ").put(e.getMessage());
                }
            }
        }
        return actual.toString();
    }

    /**
     * The query's rows through hasNext(), ending with the error the cursor threw, if any.
     */
    private static String readRowByRow(String query) throws Exception {
        try (RecordCursorFactory factory = select(query)) {
            return readRowByRow(factory);
        }
    }

    private static String readRowByRow(RecordCursorFactory factory) throws Exception {
        final StringSink expected = new StringSink();
        final RecordMetadata metadata = factory.getMetadata();
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            final Record record = cursor.getRecord();
            try {
                while (cursor.hasNext()) {
                    CursorPrinter.println(record, metadata, expected);
                }
            } catch (RuntimeException e) {
                expected.put("error: ").put(e.getMessage());
            }
        }
        return expected.toString();
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

    private static void createAllTypes(CairoEngine engine, SqlExecutionContext ctx) throws Exception {
        engine.execute(ALL_TYPES_DDL.replace(", u uuid, l256 long256, g1 geohash(1c), g3 geohash(3c), g6 geohash(6c), " +
                "g12 geohash(12c), dc64 decimal(12,2), dc128 decimal(30,4), dc256 decimal(60,6), v varchar, st string, bn binary, " +
                "arr double[]", ""), ctx);
        // the first rows have column tops for the columns added below
        engine.execute("insert into at select x % 3 = 0, (x % 120)::byte, (x * 7 % 30000)::short, rnd_char(), x::int, rnd_ipv4(), x, " +
                "(x * 86400000)::date, (x * 900000000)::timestamp, (x * 1000)::timestamp_ns, (x / 3.0)::float, x * 1.5, " +
                "'k' || (x % 50), 'z' || (x % 7) from long_sequence(150)", ctx);
        engine.execute("alter table at add column u uuid, l256 long256, g1 geohash(1c), g3 geohash(3c), g6 geohash(6c), " +
                "g12 geohash(12c), dc64 decimal(12,2), dc128 decimal(30,4), dc256 decimal(60,6), v varchar, st string, bn binary, arr double[]", ctx);
        engine.execute("insert into at " + ALL_TYPES_SELECT.replace("(x * 900000000)::timestamp", "((x + 150) * 900000000)::timestamp") + " from long_sequence(600)", ctx);
    }

    private void createAllTypes() throws Exception {
        createAllTypes(engine, sqlExecutionContext);
    }

    private void createJoinTable() throws Exception {
        execute("create table t as (select timestamp_sequence(0, 100000000) ts, rnd_symbol('A', 'B', 'C', null) ex, " +
                "rnd_symbol(40, 1, 3, 5) sym, rnd_varchar('p', 'q', 'r', null) v, " +
                "case when x % 37 = 0 then null else ((x * 7919) % 13)::float / 4 end size, " +
                "case when x % 41 = 0 then null else ((x * 31) % 17) / 3.0 end price, x " +
                "from long_sequence(3000)) timestamp(ts) partition by day");
    }
}
