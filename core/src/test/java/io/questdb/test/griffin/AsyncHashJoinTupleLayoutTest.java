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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.HashJoinTupleRing;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.test.griffin.ParallelHashJoinTestUtils.*;

/**
 * The layout of a parallel hash join's tuples: slots of 4 bytes when every value that an execution's
 * tuples may hold fits one, its probe rows, its stages' locators and their tagged chain heads, and
 * of 8 bytes otherwise. Both layouts return the ordinary plan's rows, in order, and its counts, over
 * every shape of tuple: misses, single matches, deferred chains, walked matches, locators of copied
 * payloads and of payloads read where they live, native and Parquet frames on either side, backward
 * probes, swapped inputs, continuations and LIMITs. A test hook narrows the slots to a few bits, so
 * that the layout's bounds meet tables of a few rows: each bound picks the narrow layout at the
 * largest value that fits and the wide one a value past it. Narrow slots forced on values that do not
 * fit fail the query, whichever thread writes the tuples, rather than join the wrong rows.
 */
public class AsyncHashJoinTupleLayoutTest extends AbstractCairoTest {
    // Joins over t, of 200 rows over four daily partitions, whose key k runs from 0 to 39, each of
    // which a test runs in both layouts.
    private static final String[] QUERIES = {
            // A LEFT join of one match per key, which misses for keys from 10 up.
            "SELECT t.v, d.w FROM t LEFT JOIN d ON t.k = d.k",
            // A post-join filter, whose stage walks its matches.
            "SELECT t.v, d.w, d.name FROM t JOIN d ON t.k = d.k WHERE d.w > t.v % 50",
            // Three matches per key, whose chains the consumer walks, and two such stages.
            "SELECT t.v, dd.w FROM t JOIN dd ON t.k = dd.k",
            "SELECT t.v, dd.w, e.w ew FROM t LEFT JOIN dd ON t.k = dd.k LEFT JOIN dd e ON t.k = e.k",
            // An ON residual over both inputs, whose stage walks its matches.
            "SELECT t.v, dd.w FROM t LEFT JOIN dd ON t.k = dd.k AND dd.w > t.v",
            // VARCHAR payloads, which no copy holds, so that the locators name the rows of the build's
            // frames: one match per key, and two, whose chains the consumer walks.
            "SELECT t.v, dv.name FROM t JOIN dv ON t.k = dv.k",
            "SELECT t.v, dvd.name FROM t LEFT JOIN dvd ON t.k = dvd.k",
            // A backward probe.
            "SELECT t.ts, t.v, d.w FROM t JOIN d ON t.k = d.k ORDER BY t.ts DESC",
            // A Parquet probe under a filter that the join steals, which decodes the consumer's rows
            // late, and a Parquet build read where it lives.
            "SELECT tp.v, tp.sym, d.w FROM tp JOIN d ON tp.k = d.k WHERE tp.v % 3 = 0",
            "SELECT tp.ts, tp.v, tp.sym, d.w FROM tp JOIN d ON tp.k = d.k WHERE tp.v % 3 = 0 ORDER BY tp.ts DESC",
            "SELECT t.v, dvp.name FROM t JOIN dvp ON t.k = dvp.k",
            // A LIMIT.
            "SELECT t.v, dd.w FROM t JOIN dd ON t.k = dd.k LIMIT 7",
            // A key of two columns, which the build stages, and whose chains the consumer walks.
            "SELECT t.v, ds.w FROM t JOIN ds ON t.k = ds.k AND t.sym = ds.sym",
    };
    // A stack over a join that hashes its left input, l, the smaller.
    private static final String SWAPPED = "SELECT l.v, t.v tv, d.w FROM l JOIN t ON l.k = t.k LEFT JOIN d ON l.k = d.k";

    @Override
    @Before
    public void setUp() {
        super.setUp();
        sqlExecutionContext.setParallelHashJoinEnabled(true);
    }

    @After
    public void tearDownTupleLayout() {
        sqlExecutionContext.restoreToDefaultPageFrameSizes();
        HashJoinTupleRing.setNarrowSlotBitsForTesting(Integer.SIZE);
        HashJoinTupleRing.setTupleLayoutForTesting(HashJoinTupleRing.TUPLE_LAYOUT_AUTO);
        AsyncHashJoinRecordCursorFactory.setFirstTaskRowsForTesting(AsyncHashJoinRecordCursorFactory.FIRST_TASK_ROWS);
    }

    @Test
    public void testBothLayoutsReturnTheOrdinaryPlansRows() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(16, 16);
            for (int layout : new int[]{HashJoinTupleRing.TUPLE_LAYOUT_AUTO, HashJoinTupleRing.TUPLE_LAYOUT_WIDE}) {
                HashJoinTupleRing.setTupleLayoutForTesting(layout);
                final boolean isNarrow = layout == HashJoinTupleRing.TUPLE_LAYOUT_AUTO;
                for (String sql : QUERIES) {
                    assertLayout(sql, isNarrow, false);
                }
                assertLayout(SWAPPED, isNarrow, true);
            }
        });
    }

    @Test
    public void testBothLayoutsReturnTheOrdinaryPlansRowsThroughContinuations() throws Exception {
        // A task's cap of three tuples, which the frames' rows outnumber: every frame's task leaves
        // the rest of the frame to the consumer's continuations, which write into the same entry.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 3);
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(16, 16);
            AsyncHashJoinRecordCursorFactory.setFirstTaskRowsForTesting(5);
            final String residual = "SELECT t.v, dd.w FROM t LEFT JOIN dd ON t.k = dd.k AND dd.w > t.v";
            // The rows that a probe filter keeps reach the cap too.
            final String filteredResidual = residual + " WHERE t.v % 2 = 0";
            for (int layout : new int[]{HashJoinTupleRing.TUPLE_LAYOUT_AUTO, HashJoinTupleRing.TUPLE_LAYOUT_WIDE}) {
                HashJoinTupleRing.setTupleLayoutForTesting(layout);
                final boolean isNarrow = layout == HashJoinTupleRing.TUPLE_LAYOUT_AUTO;
                for (String sql : new String[]{residual, filteredResidual, QUERIES[0], QUERIES[3], QUERIES[8]}) {
                    assertLayout(sql, isNarrow, false);
                }
                for (String sql : new String[]{residual, filteredResidual}) {
                    try (RecordCursorFactory factory = select(sql)) {
                        final AsyncHashJoinRecordCursorFactory join = find(factory);
                        Assert.assertNotNull(sql, join);
                        Assert.assertEquals(sql, ordinary(sql, sqlExecutionContext), result(factory, sqlExecutionContext));
                        Assert.assertTrue(sql, join.getAtom().getContinuationCount() > 0);
                    }
                }
            }
        });
    }

    @Test
    public void testChainHeadsAtTheSlotBound() throws Exception {
        // Slots of 8 bits, from -128 to 127. A build of 64 rows of 16 bytes, a link and a row id
        // each, ends at the link 63 * 16 + 8, whose tag, ~(1_016 >>> 3) = -128, fits; a 65th row's,
        // -130, does not. Every key of the build repeats, so the tuples hand the chains on.
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE d64 AS (SELECT (x % 8)::INT k, x * 100 w FROM long_sequence(64))");
            execute("CREATE TABLE d65 AS (SELECT (x % 8)::INT k, x * 100 w FROM long_sequence(65))");
            sqlExecutionContext.changePageFrameSizes(16, 16);
            HashJoinTupleRing.setNarrowSlotBitsForTesting(8);
            assertLayout("SELECT t.v, d64.w FROM t JOIN d64 ON t.k = d64.k", true, false);
            assertLayout("SELECT t.v, d65.w FROM t JOIN d65 ON t.k = d65.k", false, false);
            assertMaxEntries("SELECT t.v, d64.w FROM t JOIN d64 ON t.k = d64.k", 63, 63 * 16 + 8);
            assertMaxEntries("SELECT t.v, d65.w FROM t JOIN d65 ON t.k = d65.k", 64, 64 * 16 + 8);
        });
    }

    @Test
    public void testInPlaceLocatorsAtTheSlotBound() throws Exception {
        // Slots of 12 bits, from -2_048 to 2_047. db's VARCHAR payload, which no copy holds, names
        // its rows by their frame above their row in the bits of its largest frame, 64 rows, six:
        // 32 daily frames, the first and the last of 64 rows and the rest of one, end at the locator
        // 31 << 6 | 63 = 2_047, the bound of 32 frames, which fits; a 33rd frame raises the bound to
        // (33 << 6) - 1 = 2_111. The build's 158 rows, of 16 bytes, keep their chain heads within
        // the slots. An ON residual walks the matches, so that the tuples hold the locators of rows
        // of every frame.
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE db (k INT, v LONG, name VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO db SELECT (x % 40)::INT, x, 'first' || x, '2024-01-01'::TIMESTAMP + x FROM long_sequence(64)");
            execute("INSERT INTO db SELECT (x % 40)::INT, 100 + x, 'day' || x, '2024-01-01'::TIMESTAMP + x * 86_400_000_000L FROM long_sequence(30)");
            execute("INSERT INTO db SELECT (x % 40)::INT, 200 + x, 'last' || x, '2024-02-01'::TIMESTAMP + x FROM long_sequence(64)");
            sqlExecutionContext.changePageFrameSizes(64, 64);
            HashJoinTupleRing.setNarrowSlotBitsForTesting(12);
            final String sql = "SELECT t.v, db.v dv, db.name FROM t LEFT JOIN db ON t.k = db.k AND db.v > t.v % 100";
            assertLayout(sql, true, false);
            assertMaxEntries(sql, 31 << 6 | 63, 157 * 16 + 8);
            execute("INSERT INTO db VALUES (1, 300, 'one more', '2024-02-02')");
            assertLayout(sql, false, false);
            assertMaxEntries(sql, (33 << 6) - 1, 158 * 16 + 8);
        });
    }

    @Test
    public void testNarrowSlotsHalveTheRingAndDoubleTheWindow() throws Exception {
        // Frames of 16 rows, whose tuples of one stage take 16 * 2 * 4 = 128 bytes in narrow slots
        // and 256 in wide ones: a budget of 512 bytes holds four frames of narrow tuples, and two of
        // wide ones.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TUPLE_RING_MAX_SIZE, 512);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 8);
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(16, 16);
            final String sql = QUERIES[0];
            final long narrowBytes = ringBytesAfterFullRead(sql, HashJoinTupleRing.TUPLE_LAYOUT_AUTO, 4);
            final long wideBytes = ringBytesAfterFullRead(sql, HashJoinTupleRing.TUPLE_LAYOUT_WIDE, 2);
            Assert.assertTrue(narrowBytes > 0);
            Assert.assertEquals(2 * narrowBytes, wideBytes);
        });
    }

    @Test
    public void testProbeRowsAtTheSlotBound() throws Exception {
        // Slots of 8 bits, from -128 to 127: frames of 128 rows end at the probe row 127, which fits,
        // and frames of 129 rows at 128, which does not.
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE tb AS (SELECT (x % 10)::INT k, x v, timestamp_sequence('2024-01-01', 1_000_000) ts FROM long_sequence(300)) TIMESTAMP(ts) PARTITION BY DAY");
            HashJoinTupleRing.setNarrowSlotBitsForTesting(8);
            final String sql = "SELECT tb.v, d.w FROM tb JOIN d ON tb.k = d.k";
            for (int frameRows : new int[]{128, 129}) {
                sqlExecutionContext.changePageFrameSizes(frameRows, frameRows);
                assertLayout(sql, frameRows == 128, false);
                try (RecordCursorFactory factory = select(sql)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(join);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(frameRows, join.getProbeFrameRowCount(0));
                    }
                }
            }
        });
    }

    @Test
    public void testValueThatDoesNotFitFailsTheQuery() throws Exception {
        // Narrow slots of 8 bits, forced on a frame of 300 probe rows: the task that writes probe row
        // 128 fails the query, and so does the consumer's continuation that writes it, once the
        // frame's task joined only the first 100 rows, which fit. Either failure leaves nothing
        // behind, and the factory then joins in the layout that its values pick.
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE tb AS (SELECT (x % 10)::INT k, x v, timestamp_sequence('2024-01-01', 1_000_000) ts FROM long_sequence(300)) TIMESTAMP(ts) PARTITION BY DAY");
            sqlExecutionContext.changePageFrameSizes(300, 300);
            final String sql = "SELECT tb.v, d.w FROM tb LEFT JOIN d ON tb.k = d.k";
            final String expected = ordinary(sql, sqlExecutionContext);
            for (long firstTaskRows : new long[]{AsyncHashJoinRecordCursorFactory.FIRST_TASK_ROWS, 100}) {
                // A factory takes the first task's rows as it is compiled.
                AsyncHashJoinRecordCursorFactory.setFirstTaskRowsForTesting(firstTaskRows);
                try (RecordCursorFactory factory = select(sql)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(join);
                    HashJoinTupleRing.setNarrowSlotBitsForTesting(8);
                    HashJoinTupleRing.setTupleLayoutForTesting(HashJoinTupleRing.TUPLE_LAYOUT_NARROW);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        try {
                            //noinspection StatementWithEmptyBody
                            while (cursor.hasNext()) {
                            }
                            Assert.fail("expected a value that does not fit its slot to fail the query");
                        } catch (CairoException e) {
                            TestUtils.assertContains(e.getFlyweightMessage(), "hash join tuple value does not fit its slot [frame=0, slotBits=8]");
                        }
                        // The task wrote the frame's 300 rows, or the continuation the rows after the
                        // first 100.
                        Assert.assertEquals(firstTaskRows == 100 ? 1 : 0, join.getAtom().getContinuationCount());
                    }
                    Assert.assertEquals(0, join.getAtom().getRing().getAllocatedBytes());
                    Assert.assertEquals(0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                    HashJoinTupleRing.setTupleLayoutForTesting(HashJoinTupleRing.TUPLE_LAYOUT_AUTO);
                    Assert.assertEquals(expected, result(factory, sqlExecutionContext));
                }
            }
        });
    }

    // Runs the query in the layout that the test hooks leave, compares it with the ordinary plan, and
    // checks, after the first row, the layout that the execution picked, its tuples' bytes and the
    // count of the rows left, which reads the tuples of the frame that the cursor holds.
    private static void assertLayout(String sql, boolean isNarrow, boolean isSwapped) throws SqlException {
        assertParallelHashJoin(sql, sqlExecutionContext);
        final long rowCount = ordinaryRowCount(sql, sqlExecutionContext);
        Assert.assertTrue(sql, rowCount > 1);
        try (RecordCursorFactory factory = select(sql)) {
            final AsyncHashJoinRecordCursorFactory join = find(factory);
            Assert.assertNotNull(sql, join);
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(sql, cursor.hasNext());
                final HashJoinTupleRing ring = join.getAtom().getRing();
                Assert.assertEquals(sql, isSwapped, join.getAtom().isSwapped());
                Assert.assertEquals(sql, isNarrow, ring.isNarrow());
                Assert.assertEquals(sql, ring.getTupleStride() * (isNarrow ? Integer.BYTES : Long.BYTES), ring.getTupleBytes());
                final RecordCursor.Counter counter = new RecordCursor.Counter();
                cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                Assert.assertEquals(sql, rowCount - 1, counter.get());
            }
        }
    }

    // The query's one stage hands out locators and chain heads up to the given ones.
    private static void assertMaxEntries(String sql, long maxLocator, long maxChainHead) throws SqlException {
        try (RecordCursorFactory factory = select(sql)) {
            final AsyncHashJoinRecordCursorFactory join = find(factory);
            Assert.assertNotNull(sql, join);
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(sql, cursor.hasNext());
                Assert.assertEquals(sql, maxLocator, join.getAtom().getStage(0).getMaxLocator());
                Assert.assertEquals(sql, maxChainHead, join.getAtom().getStage(0).getFrozenBuild().getMaxChainHead());
            }
        }
    }

    // t: 200 rows over four daily partitions, whose key k runs from 0 to 39; tp, t with its first
    // three partitions in Parquet; d, one row for each key from 0 to 9; dd, three rows for each;
    // ds, two rows for each pair of a key from 0 to 9 and a SYMBOL;
    // dv, one row for each key from 0 to 39 over two daily partitions; dvd, two rows for each key
    // from 0 to 19 over two daily partitions; dvp, dv in Parquet but its active partition; and l,
    // eight rows, which a join with t hashes.
    private static void createTables() throws SqlException {
        execute("CREATE TABLE t AS (SELECT (x % 40)::INT k, x v, ('s' || (x % 4))::SYMBOL sym, timestamp_sequence('2024-01-01', 1_728_000_000) ts FROM long_sequence(200)) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE tp AS (SELECT * FROM t) TIMESTAMP(ts) PARTITION BY DAY");
        execute("ALTER TABLE tp CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-04'");
        execute("CREATE TABLE d AS (SELECT (x - 1)::INT k, x * 10 w, ('name' || x)::VARCHAR name FROM long_sequence(10))");
        execute("CREATE TABLE dd AS (SELECT (x % 10)::INT k, x * 100 w FROM long_sequence(30))");
        execute("CREATE TABLE ds AS (SELECT (x % 10)::INT k, ('s' || (x % 4))::SYMBOL sym, x * 1_000 w FROM long_sequence(80))");
        execute("CREATE TABLE dv AS (SELECT (x - 1)::INT k, ('v' || x)::VARCHAR name, timestamp_sequence('2024-01-01', 3_600_000_000) ts FROM long_sequence(40)) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE dvd AS (SELECT (x % 20)::INT k, ('w' || x)::VARCHAR name, timestamp_sequence('2024-01-01', 3_600_000_000) ts FROM long_sequence(40)) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE dvp AS (SELECT * FROM dv) TIMESTAMP(ts) PARTITION BY DAY");
        execute("ALTER TABLE dvp CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'");
        execute("CREATE TABLE l AS (SELECT (x * 3)::INT k, x v, timestamp_sequence('2024-01-01', 3_600_000_000) ts FROM long_sequence(8)) TIMESTAMP(ts) PARTITION BY DAY");
    }

    // Reads the query in the given layout and returns the ring's memory once the cursor read every
    // row, checking the window that the budget left after the first row.
    private static long ringBytesAfterFullRead(String sql, int layout, int window) throws SqlException {
        HashJoinTupleRing.setTupleLayoutForTesting(layout);
        try (RecordCursorFactory factory = select(sql)) {
            final AsyncHashJoinRecordCursorFactory join = find(factory);
            Assert.assertNotNull(sql, join);
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(sql, cursor.hasNext());
                final HashJoinTupleRing ring = join.getAtom().getRing();
                Assert.assertEquals(sql, window, ring.getDispatchLimit());
                //noinspection StatementWithEmptyBody
                while (cursor.hasNext()) {
                }
                return ring.getAllocatedBytes();
            }
        } finally {
            HashJoinTupleRing.setTupleLayoutForTesting(HashJoinTupleRing.TUPLE_LAYOUT_AUTO);
        }
    }
}
