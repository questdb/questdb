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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.test.griffin.ParallelHashJoinTestUtils.*;

/**
 * {@code cairo.sql.parallel.hash.join.page.frame.max.rows} caps the rows of the native frames that
 * a parallel hash join's probe scan cuts, below the context's page frame maximum: in the query's
 * orientation and in a swapped one, under a stack of joins, and below the frames that the fan-out
 * of the builds cuts. The build scans keep the context's frames, and a Parquet partition keeps its
 * row groups. The task cap and the ring budget follow the probe frames' rows by default, so a frame's
 * tuples, and with them the memory that a query needs under a per-query limit, shrink with it. The
 * rows always equal the ordinary plan's, in order.
 * <p>
 * The probe table t holds 100 rows in one partition, ten keys, which a cap of eight rows cuts into
 * twelve frames of eight rows and one of four.
 */
public class AsyncHashJoinProbeFrameMaxRowsTest extends AbstractCairoTest {
    // The joins that the cap cuts t's frames under, compared with the ordinary plan.
    private static final String[] QUERIES = {
            "SELECT t.v, d.w FROM t JOIN d ON t.k = d.k",
            "SELECT t.v, d.w FROM t JOIN d ON t.k = d.k ORDER BY t.ts DESC",
            // A selective probe filter.
            "SELECT t.v, d.w FROM t LEFT JOIN d ON t.k = d.k WHERE t.v % 7 = 0",
            // An INNER join that drops nine probe rows in ten.
            "SELECT t.v, m.w FROM t JOIN m ON t.k = m.k",
            // Duplicate matches, which the workers walk under a residual.
            "SELECT t.v, d3.w FROM t LEFT JOIN d3 ON t.k = d3.k AND d3.w > t.v",
            "SELECT t.v, d3.w FROM t JOIN d3 ON t.k = d3.k WHERE d3.w > t.v",
            // A stack.
            "SELECT t.v, d.w, d3.w w3, m.w wm FROM t JOIN d ON t.k = d.k LEFT JOIN d3 ON t.k = d3.k AND d3.w > t.v LEFT JOIN m ON t.k = m.k",
            // A swapped execution, which probes t's frames.
            "SELECT s.u, t.v FROM s JOIN t ON s.k = t.k",
            // LIMITs that reach the join, and one that does not.
            "SELECT t.v, d.w FROM t JOIN d ON t.k = d.k LIMIT 5",
            "SELECT t.v, d.w FROM t JOIN d ON t.k = d.k LIMIT 10, 30",
            "SELECT t.v, d.w FROM t JOIN d ON t.k = d.k LIMIT -5",
            "SELECT v, w, row_number() OVER () rn FROM (SELECT t.v, d.w FROM t JOIN d ON t.k = d.k) LIMIT 10",
    };

    @Override
    @Before
    public void setUp() {
        super.setUp();
        sqlExecutionContext.setParallelHashJoinEnabled(true);
    }

    @After
    public void tearDownProbeFrames() {
        sqlExecutionContext.setFirstReadRows(Long.MAX_VALUE);
        sqlExecutionContext.restoreToDefaultPageFrameSizes();
    }

    @Test
    public void testCapAboveTheContextsFramesChangesNothing() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // Frames of up to 50 rows, which the default cap of 100_000 rows leaves as they are: t's
            // 100 rows make two.
            sqlExecutionContext.changePageFrameSizes(1, 50);
            Assert.assertEquals(100_000, configuration.getSqlParallelHashJoinPageFrameMaxRows());
            assertProbeFrames("SELECT t.v, d.w FROM t JOIN d ON t.k = d.k", false, 50, 50);
            // A cap equal to the context's maximum.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_PAGE_FRAME_MAX_ROWS, 50);
            assertProbeFrames("SELECT t.v, d.w FROM t JOIN d ON t.k = d.k", false, 50, 50);
        });
    }

    @Test
    public void testFanOutFramesFollowTheSmallerCap() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_PAGE_FRAME_MAX_ROWS, 8);
            sqlExecutionContext.changePageFrameSizes(1, 50);
            final String[] walking = {
                    "SELECT t.v, d3.w FROM t LEFT JOIN d3 ON t.k = d3.k AND d3.w > t.v",
                    "SELECT t.v, d3.w FROM t JOIN d3 ON t.k = d3.k WHERE d3.w > t.v",
            };
            // d3 holds three rows of each key: a task cap of twelve tuples caps the frames at four
            // rows, below the cap of eight, and one of sixty at twenty rows, above it.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 12);
            for (String sql : walking) {
                assertProbeFrames(sql, false, frameRows(100, 4));
            }
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 60);
            for (String sql : walking) {
                assertProbeFrames(sql, false, frameRows(100, 8));
            }
        });
    }

    @Test
    public void testMemoryLimitHoldsOneProbeFrame() throws Exception {
        // tb holds 1_000_000 rows in one partition, every one of which meets one row of d. At the
        // server's page frame sizes, 100_000 to 1_000_000 rows, a context of one worker cuts one
        // frame of the partition's rows, and the probe's cap ten frames of 100_000 rows: a task
        // writes 100_000 tuples of two narrow slots, 800_000 bytes, which a query memory limit of
        // 2 MiB, whose quarter holds no task's tuples, holds once. A frame of 1_000_000 rows does not
        // fit it: the frame's task joins its first 4_096 rows, and the consumer's continuation the
        // rest, in chunks of as many rows as the frame joined before each, each into an entry of its
        // tuples' room, and the chunk of 262_144 rows takes 2_097_152 bytes, the whole limit.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 100_000);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            createLargeTables();
            final String sql = "SELECT tb.v, d.w FROM tb JOIN d ON tb.k = d.k";
            try {
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 2 * 1024 * 1024);
                try (RecordCursorFactory factory = select(sql)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(join);
                    long rows = 0;
                    long vSum = 0;
                    long wSum = 0;
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        final Record record = cursor.getRecord();
                        while (cursor.hasNext()) {
                            rows++;
                            vSum += record.getLong(0);
                            wSum += record.getLong(1);
                        }
                        Assert.assertEquals(10, join.getProbeFrameCount());
                        Assert.assertTrue(join.getAtom().getRing().isNarrow());
                        Assert.assertEquals(1, join.getAtom().getRing().getDispatchLimit());
                    }
                    // v runs from 1 to 1_000_000, and the row of v meets d's row of key v % 1_000,
                    // whose w is that key plus 1_000_000, a thousand times each.
                    Assert.assertEquals(1_000_000, rows);
                    Assert.assertEquals(1_000_000L * 1_000_001 / 2, vSum);
                    Assert.assertEquals(1_000L * (1_000L * 1_000_000 + 999L * 1_000 / 2), wSum);
                }
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_PAGE_FRAME_MAX_ROWS, 1_000_000);
                try (RecordCursorFactory factory = select(sql)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(join);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        try {
                            //noinspection StatementWithEmptyBody
                            while (cursor.hasNext()) {
                            }
                            Assert.fail("expected the query memory limit to fail the query");
                        } catch (CairoException e) {
                            Assert.assertTrue(e.getMessage(), e.isOutOfMemory());
                            // The one frame's entry, which takes the chunk's 2_097_152 bytes at once.
                            TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                            TestUtils.assertContains(e.getFlyweightMessage(), "size=2097152");
                        }
                    }
                }
            } finally {
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0);
            }
            // The table still serves the join.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_PAGE_FRAME_MAX_ROWS, null);
            assertQuery("SELECT count(), sum(tb.v) FROM tb JOIN d ON tb.k = d.k")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count\tsum
                            1000000\t500000500000
                            """);
        });
    }

    @Test
    public void testParquetProbeKeepsItsRowGroups() throws Exception {
        assertMemoryLeak(() -> {
            // Two daily partitions of twelve rows, the first converted to Parquet, whose frame is its
            // one row group of twelve rows, and the second native, which the cap of four rows cuts
            // into three frames.
            execute("CREATE TABLE p AS (SELECT (x % 4)::INT k, x v, timestamp_sequence('2024-01-01', 7_200_000_000) ts FROM long_sequence(24)) TIMESTAMP(ts) PARTITION BY DAY");
            // d is the smaller input, so that the join never swaps to probe it.
            execute("CREATE TABLE d AS (SELECT (x - 1)::INT k, 1_000 + x w FROM long_sequence(4))");
            execute("ALTER TABLE p CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'");
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_PAGE_FRAME_MAX_ROWS, 4);
            sqlExecutionContext.changePageFrameSizes(1, 12);
            assertProbeFrames("SELECT p.v, d.w FROM p JOIN d ON p.k = d.k", false, 12, 4, 4, 4);
            assertProbeFrames("SELECT p.v, d.w FROM p LEFT JOIN d ON p.k = d.k WHERE p.v % 3 = 0", false, 12, 4, 4, 4);
        });
    }

    @Test
    public void testProbeFramesCappedInEitherOrientation() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_PAGE_FRAME_MAX_ROWS, 8);
            // Frames of up to 50 rows: each build, of 30 rows or fewer, keeps one frame, and t's
            // probe frames hold eight rows.
            sqlExecutionContext.changePageFrameSizes(1, 50);
            final long[] capped = frameRows(100, 8);
            assertProbeFrames("SELECT t.v, d.w FROM t JOIN d ON t.k = d.k", false, capped);
            assertProbeFrames("SELECT t.v, d.w FROM t LEFT JOIN d ON t.k = d.k", false, capped);
            assertProbeFrames("SELECT t.v, d.w FROM t JOIN d ON t.k = d.k ORDER BY t.ts DESC", false, capped);
            // s, the smaller left input, is the one that a swapped execution hashes.
            assertProbeFrames("SELECT s.u, t.v FROM s JOIN t ON s.k = t.k", true, capped);
            // A stack: the probe's frames, whichever stage reads them.
            assertProbeFrames("SELECT t.v, d.w, d3.w w3, m.w wm FROM t JOIN d ON t.k = d.k LEFT JOIN d3 ON t.k = d3.k LEFT JOIN m ON t.k = m.k", false, capped);
            assertProbeFrames("SELECT s.u, t.v, d.w FROM s JOIN t ON s.k = t.k JOIN d ON s.k = d.k", true, capped);
        });
    }

    @Test
    public void testRingAtServerDefaults() throws Exception {
        // The server's page frame sizes, 100_000 to 1_000_000 rows, a dispatch limit of four and a
        // reduce queue of sixteen: the ring keeps four entries, and tb's frames of 100_000 rows,
        // whose rows each meet one row of each build, fill each with 100_000 tuples of a probe row
        // and an entry per join, in narrow slots of 4 bytes: 800_000 bytes for one join, 1_600_000
        // for three. At the former cap of a page frame's 1_000_000 rows, a context of one worker
        // cuts one frame of the partition's rows, the first frame, whose task joins its first 4_096
        // rows, and the consumer's continuation the rest, in chunks of as many rows as the frame
        // joined before each: the last chunk joins the 475_712 rows left after 524_288, into an
        // entry that takes 3_805_696 bytes for one join, and 7_611_392 for three.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 100_000);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1_000_000);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 4);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 16);
        assertMemoryLeak(() -> {
            createLargeTables();
            execute("CREATE TABLE d2 AS (SELECT (x - 1)::INT k, 2_000_000 + x - 1 w FROM long_sequence(1_000))");
            final String single = "SELECT tb.v, d.w FROM tb JOIN d ON tb.k = d.k";
            final String star = "SELECT tb.v, d.w, d2.w w2, d3.w w3 FROM tb JOIN d ON tb.k = d.k JOIN d2 ON tb.k = d2.k JOIN d d3 ON tb.k = d3.k";
            Assert.assertEquals(4 * 800_000, ringBytesAfterFullRead(single, 10));
            Assert.assertEquals(4 * 1_600_000, ringBytesAfterFullRead(star, 10));
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_PAGE_FRAME_MAX_ROWS, 1_000_000);
            Assert.assertEquals(475_712 * 2 * Integer.BYTES, ringBytesAfterFullRead(single, 1));
            Assert.assertEquals(475_712 * 4 * Integer.BYTES, ringBytesAfterFullRead(star, 1));
        });
    }

    @Test
    public void testRowsEqualTheOrdinaryPlanUnderTheCap() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(1, 50);
            // A cap of one row, of three rows, which a duplicate key's matches outnumber, and of eight.
            for (int cap : new int[]{1, 3, 8}) {
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_PAGE_FRAME_MAX_ROWS, cap);
                for (String sql : QUERIES) {
                    assertParallelHashJoin(sql, sqlExecutionContext);
                }
                // A client that reads ten rows first, as an HTTP page or a PG Execute row limit, and
                // reads on.
                for (String sql : new String[]{QUERIES[0], QUERIES[4], QUERIES[6], QUERIES[7]}) {
                    final String expected = ordinary(sql, sqlExecutionContext);
                    try (RecordCursorFactory factory = select(sql)) {
                        Assert.assertNotNull(sql, find(factory));
                        sqlExecutionContext.setFirstReadRows(10);
                        try {
                            Assert.assertEquals(sql + ", cap " + cap, expected, result(factory, sqlExecutionContext));
                        } finally {
                            sqlExecutionContext.setFirstReadRows(Long.MAX_VALUE);
                        }
                    }
                }
            }
        });
    }

    // Executes the query on the operator, reads every row, and checks that the execution swapped its
    // first join's inputs or not, that its probe frames hold the given rows, and that every stage's
    // build kept one frame. Compares the rows with the ordinary plan's.
    private static void assertProbeFrames(String sql, boolean isSwapped, long... frameRows) throws SqlException {
        assertParallelHashJoin(sql, sqlExecutionContext);
        try (RecordCursorFactory factory = select(sql)) {
            final AsyncHashJoinRecordCursorFactory join = find(factory);
            Assert.assertNotNull(sql, join);
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                //noinspection StatementWithEmptyBody
                while (cursor.hasNext()) {
                }
                Assert.assertEquals(sql, isSwapped, join.getAtom().isSwapped());
                Assert.assertEquals(sql, frameRows.length, join.getProbeFrameCount());
                for (int i = 0; i < frameRows.length; i++) {
                    Assert.assertEquals(sql + ", frame " + i, frameRows[i], join.getProbeFrameRowCount(i));
                }
                for (int k = 0, n = join.getSpec().getStageCount(); k < n; k++) {
                    Assert.assertEquals(sql + ", stage " + k, 1, join.getAtom().getStage(k).getBuildFrameCount());
                }
            }
        }
    }

    // tb: 1_000_000 rows in one daily partition, v from 1 to 1_000_000 and k = v % 1_000; d: a row
    // of each key, w = k + 1_000_000.
    private static void createLargeTables() throws SqlException {
        execute("""
                CREATE TABLE tb AS (
                    SELECT (x % 1_000)::INT k, x v, timestamp_sequence('2024-01-01', 50_000) ts
                    FROM long_sequence(1_000_000)
                ) TIMESTAMP(ts) PARTITION BY DAY
                """);
        execute("CREATE TABLE d AS (SELECT (x - 1)::INT k, 1_000_000 + x - 1 w FROM long_sequence(1_000))");
    }

    // t: 100 rows in one partition, ten keys; d: a row of each of 30 keys; d3: three rows of each of
    // t's keys; m: a row of key 3; s: a row of each of t's keys, fewer rows than t, so that a join of
    // s with t hashes s, and one row per key, so that no fan-out caps the frames of a stack that
    // walks s's matches.
    private static void createTables() throws SqlException {
        execute("""
                CREATE TABLE t AS (
                    SELECT (x % 10)::INT k, x v, timestamp_sequence('2024-01-01', 60_000_000) ts
                    FROM long_sequence(100)
                ) TIMESTAMP(ts) PARTITION BY DAY
                """);
        execute("CREATE TABLE d AS (SELECT (x - 1)::INT k, 1_000 + x w FROM long_sequence(30))");
        execute("CREATE TABLE d3 AS (SELECT (x % 10)::INT k, x * 4 w FROM long_sequence(30))");
        execute("CREATE TABLE m AS (SELECT 3 k, 7 w FROM long_sequence(1))");
        execute("CREATE TABLE s AS (SELECT (x - 1)::INT k, x * 11 u FROM long_sequence(10))");
    }

    // The rows of the frames that a cap cuts a partition of the given rows into: as many as the cap
    // holds, and the rows left in the last frame, which no frame grows past the cap to absorb.
    private static long[] frameRows(long partitionRows, long cap) {
        final long[] frames = new long[(int) ((partitionRows + cap - 1) / cap)];
        for (int i = 0; i < frames.length; i++) {
            frames[i] = Math.min(cap, partitionRows - i * cap);
        }
        return frames;
    }

    // Reads every row of the query on the operator, checks the probe frames it cut, and returns the
    // bytes that the ring's entries hold before the cursor closes.
    private static long ringBytesAfterFullRead(String sql, int frames) throws SqlException {
        try (RecordCursorFactory factory = select(sql)) {
            final AsyncHashJoinRecordCursorFactory join = find(factory);
            Assert.assertNotNull(sql, join);
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                long rows = 0;
                while (cursor.hasNext()) {
                    rows++;
                }
                Assert.assertEquals(sql, 1_000_000, rows);
                Assert.assertEquals(sql, frames, join.getProbeFrameCount());
                Assert.assertTrue(sql, join.getAtom().getRing().isNarrow());
                return join.getAtom().getRing().getAllocatedBytes();
            }
        }
    }
}
