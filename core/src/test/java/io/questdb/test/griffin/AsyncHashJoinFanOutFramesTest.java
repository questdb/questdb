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
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.test.TestLatchedCounterFunctionFactory;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.HashJoinBuildStage;
import io.questdb.griffin.engine.table.parquet.ParquetCompression;
import io.questdb.griffin.engine.table.parquet.ParquetVersion;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.std.Files;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static io.questdb.test.griffin.ParallelHashJoinTestUtils.*;

/**
 * The probe frames of a parallel hash join whose workers walk duplicate matches: once the builds are
 * done, the probe scan cuts frames of no more rows than a task's cap of tuples holds at the fan-out
 * that the builds lead one to expect, the product of the rows per key of every build whose matches
 * the workers walk, under an ON residual that the pairs run or a post-join filter. Such a frame
 * needs no continuation, where a frame of the configured rows leaves its rest to the consumer. The
 * frames never get fewer rows than 1,000, or {@code cairo.sql.page.frame.min.rows} when that is
 * fewer; a stage that hands its chains to the consumer, a LEFT join's among them whose build ran its
 * whole ON residual, a build of one row per key and an empty one lower nothing; a Parquet partition
 * keeps its row groups; and each execution of a cached factory sizes its frames from its own
 * builds. The rows always equal the ordinary plan's, in order.
 * <p>
 * The probe table t holds 24 rows in one partition, four keys, and d three rows of each key.
 */
public class AsyncHashJoinFanOutFramesTest extends AbstractCairoTest {
    // Joins whose single stage walks d's three matches of each key on the workers.
    private static final String[] ENUMERATED = {
            "SELECT t.v, d.w FROM t JOIN d ON t.k = d.k WHERE d.w > t.v - 1_000",
            "SELECT t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w > t.v - 1_000",
            "SELECT t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w > t.v - 1_000 ORDER BY ts DESC",
            "SELECT t.v, d.w FROM t JOIN d ON t.k = d.k WHERE t.v > 2 AND d.w > t.v - 1_000",
            // A projection that names a column twice keeps its own frame cursor over the scan's.
            "SELECT t.v, d.w FROM (SELECT ts, v, k k1, k k2 FROM t) t JOIN d ON t.k1 = d.k WHERE d.w > t.v - 1_000 + t.k2",
    };
    // LEFT joins whose ON residual reads d alone, which the build runs over d's rows, every one of
    // which it keeps: the pairs run no filter, and the stage hands d's chains of three matches to the
    // consumer, one entry per probe row, as a stage without a residual does.
    private static final String[] PUSHED = {
            "SELECT t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w > 0",
            "SELECT t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w > 0 ORDER BY ts DESC",
    };

    @Override
    @Before
    public void setUp() {
        super.setUp();
        sqlExecutionContext.setParallelHashJoinEnabled(true);
    }

    @After
    public void restorePageFrameSizes() {
        sqlExecutionContext.restoreToDefaultPageFrameSizes();
    }

    @Test
    public void testCappedFramesDoNotGrowToAbsorbAShortLastFrame() throws Exception {
        assertMemoryLeak(() -> {
            // Seven probe rows, a cap of nine tuples and three matches per key: frames of three rows.
            // A frame of three rows over a partition of seven leaves a last frame of one, which a frame
            // of at least two rows would otherwise grow to absorb, to four rows and twelve tuples.
            execute("CREATE TABLE t AS (SELECT (x % 4)::INT k, x v, timestamp_sequence('2024-01-01', 60_000_000) ts FROM long_sequence(7)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE d AS (SELECT (x % 4)::INT k, x * 100 w FROM long_sequence(12))");
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 9);
            sqlExecutionContext.changePageFrameSizes(2, 9);
            for (String sql : ENUMERATED) {
                assertFrames(sql, 3, 0);
            }
            // A stage that hands its chains on caps no frame: the partition's seven rows make one.
            for (String sql : PUSHED) {
                assertFrames(sql, 1, 0);
            }
        });
    }

    @Test
    public void testCapEndsWithTheExecution() throws Exception {
        assertMemoryLeak(() -> {
            // l, the left input, holds 24 rows, six of each key, and r, the right one, twelve, three
            // of each key: the first execution probes l's frames, which r's fan-out caps at two rows,
            // and hashes r's two frames of six rows. Rows added to r then make l the smaller input:
            // the next execution hashes l, whose scan cuts its frames as configured, four of six
            // rows, though it capped them in the execution before, and probes r's, which l's fan-out
            // caps at one row.
            execute("CREATE TABLE l AS (SELECT (x % 4)::INT k, x v, timestamp_sequence('2024-01-01', 60_000_000) ts FROM long_sequence(24)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE r AS (SELECT (x % 4)::INT k, x * 100 w, timestamp_sequence('2024-01-01', 60_000_000) ts FROM long_sequence(12)) TIMESTAMP(ts) PARTITION BY DAY");
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 6);
            sqlExecutionContext.changePageFrameSizes(1, 6);
            final String sql = "SELECT l.v, r.w FROM l JOIN r ON l.k = r.k WHERE r.w > l.v - 1_000";
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                Assert.assertEquals(ordinary(sql, sqlExecutionContext), result(factory, sqlExecutionContext));
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertFalse(join.getAtom().isSwapped());
                    Assert.assertEquals(12, join.getProbeFrameCount());
                    Assert.assertEquals(2, join.getAtom().getStage(0).getBuildFrameCount());
                }
                execute("INSERT INTO r SELECT (x % 4)::INT k, x * 100 w, timestamp_sequence('2024-01-01T01', 60_000_000) ts FROM long_sequence(36)");
                Assert.assertEquals(ordinary(sql, sqlExecutionContext), result(factory, sqlExecutionContext));
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertTrue(join.getAtom().isSwapped());
                    Assert.assertEquals(48, join.getProbeFrameCount());
                    Assert.assertEquals(4, join.getAtom().getStage(0).getBuildFrameCount());
                }
            }
        });
    }

    @Test
    public void testContinuedFramesFirstWindowHoldsTheTaskCap() throws Exception {
        // t holds 8_000 probe rows over a hundred keys, and d ten rows of each key: frames of the
        // 1_000-row floor, whose tasks stop at the cap of 5_000 tuples and leave the rest of their
        // frames to continuations. A ring budget of 80_000 bytes holds two such tasks' tuples of two
        // narrow slots, 40_000 bytes each, where the frames' rows, 8_000 bytes, would let the
        // dispatch limit's eight frames in.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 8);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 5_000);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TUPLE_RING_MAX_SIZE, 80_000);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT (x % 100)::INT k, x v, timestamp_sequence('2024-01-01', 5_000_000) ts FROM long_sequence(8_000)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE d AS (SELECT (x % 100)::INT k, 100_000 + x w FROM long_sequence(1_000))");
            sqlExecutionContext.changePageFrameSizes(2_000, 8_000);
            for (String sql : ENUMERATED) {
                assertFrames(sql, 8, -1);
                assertFirstWindow(sql, 2, 2, false);
            }
        });
    }

    @Test
    public void testDeferredUniqueAndEmptyBuildsKeepTheFrames() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE u AS (SELECT (x % 4)::INT k, x * 7 w FROM long_sequence(4))");
            execute("CREATE TABLE e (k INT, w LONG)");
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 6);
            sqlExecutionContext.changePageFrameSizes(1, 6);
            // A stage without a filter hands its chains to the consumer, one entry per probe row; a
            // build of one row per key and an empty LEFT build yield one row per probe row: frames
            // of the six configured rows, which need no continuation either.
            assertFrames("SELECT t.v, d.w FROM t JOIN d ON t.k = d.k", 4, 0);
            assertFrames("SELECT t.v, d.w FROM t LEFT JOIN d ON t.k = d.k", 4, 0);
            assertFrames("SELECT t.v, u.w FROM t JOIN u ON t.k = u.k WHERE u.w > t.v - 1_000", 4, 0);
            assertFrames("SELECT t.v, e.w FROM t LEFT JOIN e ON t.k = e.k AND e.w > t.v", 4, 0);
            assertFrames("SELECT t.v, e.w FROM t LEFT JOIN e ON t.k = e.k WHERE e.w IS NULL", 4, 0);
            // So does a LEFT join's stage whose build ran its whole ON residual over d's rows.
            for (String sql : PUSHED) {
                assertFrames(sql, 4, 0);
            }
        });
    }

    @Test
    public void testFanOutFramesBelowTheConfiguredMinimum() throws Exception {
        assertMemoryLeak(() -> {
            // t holds 8_000 probe rows over a hundred keys, which frames of at least 2_000 rows would
            // cut into four. The frames of a walking stage keep at least 1_000 rows instead, and a
            // cap of 5_000 tuples at f rows of d per key caps them at 5_000 / f rows: 1_666 rows at
            // f = 3, four frames and one of the 1_336 rows left, whose 4_998 tuples fit the cap;
            // 1_250 rows at f = 4, six frames and one of the 500 rows left, whose 5_000 tuples fill
            // the cap exactly, which ends the frame with its last row, so that no frame continues;
            // 1_000 rows at f = 5, eight frames that fill the cap as exactly; and 1_000 rows, the
            // floor, where the cap holds 833 at f = 6 and 500 at f = 10, eight frames whose 6_000
            // and 10_000 tuples pass the cap and continue.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 5_000);
            sqlExecutionContext.changePageFrameSizes(2_000, 8_000);
            final int[][] fanOutsFramesAndContinuations = {{3, 5, 0}, {4, 7, 0}, {5, 8, 0}, {6, 8, -1}, {10, 8, -1}};
            // A partitioned probe, one partition, and a probe without partitions cut the same frames.
            for (String partitionBy : new String[]{" PARTITION BY DAY", ""}) {
                execute("DROP TABLE IF EXISTS t");
                execute("CREATE TABLE t AS (SELECT (x % 100)::INT k, x v, timestamp_sequence('2024-01-01', 5_000_000) ts FROM long_sequence(8_000)) TIMESTAMP(ts)" + partitionBy);
                for (int[] fanOutFramesAndContinuations : fanOutsFramesAndContinuations) {
                    // d holds f rows of each key, every one of which the residuals and the post-join
                    // filters keep.
                    execute("DROP TABLE IF EXISTS d");
                    execute("CREATE TABLE d AS (SELECT (x % 100)::INT k, 100_000 + x w FROM long_sequence(" + 100 * fanOutFramesAndContinuations[0] + "))");
                    for (String sql : ENUMERATED) {
                        assertFrames(sql, fanOutFramesAndContinuations[1], fanOutFramesAndContinuations[2]);
                    }
                }
            }
            // A column that t gained once its partition held 8_000 rows starts there, and so does the
            // frame of the probe scan that reads it, in the middle of a frame of 1_250 rows at f = 4:
            // six frames of 1_250 rows, one of the 500 rows up to the column's top, and one of the
            // 1_000 rows after it.
            execute("DROP TABLE t");
            execute("CREATE TABLE t AS (SELECT (x % 100)::INT k, x v, timestamp_sequence('2024-01-01', 5_000_000) ts FROM long_sequence(8_000)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("ALTER TABLE t ADD COLUMN v2 LONG");
            execute("INSERT INTO t SELECT (x % 100)::INT k, 8_000 + x v, timestamp_sequence('2024-01-01T12', 5_000_000) ts, x v2 FROM long_sequence(1_000)");
            execute("DROP TABLE d");
            execute("CREATE TABLE d AS (SELECT (x % 100)::INT k, 100_000 + x w FROM long_sequence(400))");
            assertFrames("SELECT t.v, t.v2, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w > t.v - 1_000", 8, 0);
        });
    }

    @Test
    public void testFanOutFramesNeedNoContinuation() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 6);
            for (String sql : ENUMERATED) {
                // Frames of at least six rows, the cap, cannot shrink: each yields three tuples per row,
                // past the cap, and leaves its rest to a continuation.
                sqlExecutionContext.changePageFrameSizes(6, 6);
                assertFrames(sql, 4, -1);
                // Frames of at least one row shrink to two rows, six tuples, and need none.
                sqlExecutionContext.changePageFrameSizes(1, 6);
                assertFrames(sql, 12, 0);
                // Without a fan-out past the cap the frames keep their rows: twenty-four rows a frame,
                // three times as many tuples, fit a cap of a hundred.
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 100);
                sqlExecutionContext.changePageFrameSizes(1, 24);
                assertFrames(sql, 1, 0);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 6);
            }
            for (String sql : PUSHED) {
                // One tuple per probe row: frames of the six configured rows fit the cap of six.
                sqlExecutionContext.changePageFrameSizes(6, 6);
                assertFrames(sql, 4, 0);
                sqlExecutionContext.changePageFrameSizes(1, 6);
                assertFrames(sql, 4, 0);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 100);
                sqlExecutionContext.changePageFrameSizes(1, 24);
                assertFrames(sql, 1, 0);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 6);
            }
        });
    }

    @Test
    public void testFramesFollowThePushdownOfEachExecution() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 6);
            sqlExecutionContext.changePageFrameSizes(1, 6);
            // test_latched_counter() reads no column, so the ON residual consists of conjuncts of d
            // alone. An execution whose build of d meets a failure of it gives the conjuncts up and
            // walks d's three matches of each key under the whole residual: frames of two rows, six
            // tuples. The next execution runs the conjuncts over d's rows, every one of which they
            // keep, and hands d's chains on, one entry per probe row: frames of the six configured
            // rows. The one after it gives them up again.
            final String sql = "SELECT t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND test_latched_counter() AND d.w > 0";
            final AtomicBoolean isFailing = new AtomicBoolean();
            TestLatchedCounterFunctionFactory.reset(new TestLatchedCounterFunctionFactory.Callback() {
                @Override
                public boolean onGet(Record rec, int count) {
                    if (isFailing.get() && isOnStack("applyAbandonableBuildOnFilter")) {
                        throw CairoException.nonCritical().put("test value failure");
                    }
                    return true;
                }
            });
            try {
                final String expected = ordinary(sql, sqlExecutionContext);
                try (RecordCursorFactory factory = select(sql)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(join);
                    final int[] frames = {12, 4, 12};
                    for (int execution = 0; execution < frames.length; execution++) {
                        final String message = sql + ", execution " + execution;
                        isFailing.set(execution != 1);
                        // The first execution after each flip cuts the frames, so that a walk left over
                        // from the execution before shows.
                        Assert.assertEquals(message, frames[execution], probeFrames(factory, join));
                        Assert.assertEquals(message, 0, join.getAtom().getContinuationCount());
                        Assert.assertEquals(message, expected, result(factory, sqlExecutionContext));
                    }
                }
            } finally {
                TestLatchedCounterFunctionFactory.reset(null);
            }
        });
    }

    @Test
    public void testFramesNoSmallerThanTheMinimum() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 6);
            // Two rows fit the cap, but frames keep at least four rows, twelve tuples, which a
            // continuation finishes.
            sqlExecutionContext.changePageFrameSizes(4, 6);
            for (String sql : ENUMERATED) {
                assertFrames(sql, 6, -1);
            }
            // A stage that hands its chains on keeps the six configured rows a frame.
            for (String sql : PUSHED) {
                assertFrames(sql, 4, 0);
            }
        });
    }

    @Test
    public void testFramesPerExecutionOfCachedFactory() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 12);
            sqlExecutionContext.changePageFrameSizes(1, 12);
            // One row per key, then two, then six: frames of twelve rows, six, two, where the workers
            // walk g's matches; a stage whose build ran its whole residual over g's rows hands its
            // chains on, and keeps frames of twelve rows.
            final Object[][] queries = {
                    {"SELECT t.v, g.w FROM t LEFT JOIN g ON t.k = g.k AND g.w > t.v - 1_000", new int[]{2, 4, 12}},
                    {"SELECT t.v, g.w FROM t LEFT JOIN g ON t.k = g.k AND g.w > 0", new int[]{2, 2, 2}},
            };
            for (Object[] query : queries) {
                final String sql = (String) query[0];
                final int[] frames = (int[]) query[1];
                execute("DROP TABLE IF EXISTS g");
                execute("CREATE TABLE g AS (SELECT (x % 4)::INT k, x * 3 w FROM long_sequence(4))");
                try (RecordCursorFactory factory = select(sql)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(join);
                    final int[] inserts = {4, 16};
                    for (int execution = 0; execution < frames.length; execution++) {
                        if (execution > 0) {
                            execute("INSERT INTO g SELECT (x % 4)::INT k, x * 3 w FROM long_sequence(" + inserts[execution - 1] + ")");
                        }
                        final String expected = ordinary(sql, sqlExecutionContext);
                        Assert.assertEquals(sql, expected, result(factory, sqlExecutionContext));
                        Assert.assertEquals(sql, frames[execution], probeFrames(factory, join));
                        Assert.assertEquals(sql, 0, join.getAtom().getContinuationCount());
                    }
                    // Back to one row per key: frames of twelve rows again.
                    execute("TRUNCATE TABLE g");
                    execute("INSERT INTO g SELECT (x % 4)::INT k, x * 3 w FROM long_sequence(4)");
                    Assert.assertEquals(sql, ordinary(sql, sqlExecutionContext), result(factory, sqlExecutionContext));
                    Assert.assertEquals(sql, 2, probeFrames(factory, join));
                }
            }
        });
    }

    @Test
    public void testMisestimatedFanOutAtServerDefaultFrames() throws Exception {
        assertMemoryLeak(() -> {
            // dh holds one row of each of the probe's thousand keys and 50_000 rows of a key, -1,
            // that no probe row has: 51_000 rows over 1_001 keys lead the join to expect 50.95
            // matches per probe row, where each probe row meets one. At the server's default frame
            // sizes and task cap, a probe frame's 100_000 rows, the frames of 200_001 probe rows
            // shrink to 100_000 / 50.95 = 1_962 rows, 101 of them and one of the 1_839 rows left,
            // where the join's 100_000-row maximum would cut three: the misleading estimate costs
            // 102 frames, and no frame continues, the first one included, whose rows are fewer than
            // the 4_096 that an execution without a LIMIT joins first.
            execute("CREATE TABLE tl AS (SELECT (x % 1_000)::INT k, x v, timestamp_sequence('2024-01-01', 100_000) ts FROM long_sequence(200_001)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE dh AS (SELECT CASE WHEN x <= 1_000 THEN (x - 1)::INT ELSE -1 END k, x w FROM long_sequence(51_000))");
            sqlExecutionContext.changePageFrameSizes(100_000, 1_000_000);
            Assert.assertEquals(100_000, configuration.getSqlParallelHashJoinPageFrameMaxRows());
            Assert.assertEquals(100_000, configuration.getSqlParallelHashJoinTaskOutputMaxRows());
            Assert.assertEquals(102, assertDefaultFrames("SELECT tl.v, dh.w FROM tl JOIN dh ON tl.k = dh.k WHERE dh.w + tl.v > 0", 0));
        });
    }

    @Test
    public void testParquetFramesKeepTheirRowGroups() throws Exception {
        assertMemoryLeak(() -> {
            // Two daily partitions of twelve rows, the first converted to Parquet, whose frame is its one
            // row group, and the second native, whose frames shrink to two rows.
            execute("CREATE TABLE t AS (SELECT (x % 4)::INT k, x v, timestamp_sequence('2024-01-01', 7_200_000_000) ts FROM long_sequence(24)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE d AS (SELECT (x % 4)::INT k, x * 100 w FROM long_sequence(12))");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'");
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 6);
            sqlExecutionContext.changePageFrameSizes(1, 12);
            for (String sql : ENUMERATED) {
                // The Parquet frame's twelve rows yield thirty-six tuples, and leave a continuation.
                assertFrames(sql, 1 + 6, -1);
            }
            // A stage that hands its chains on writes twelve tuples for either partition: the native
            // one keeps a frame of its twelve rows.
            for (String sql : PUSHED) {
                assertFrames(sql, 1 + 1, 0);
            }
        });
    }

    @Test
    public void testParquetFirstWindowHoldsTheRowGroupTask() throws Exception {
        // As in testParquetFramesKeepTheirRowGroups: the Parquet partition's frame, its row group of
        // twelve rows, past the cap of six, leaves its task a cap of twelve tuples, 96 bytes of two
        // narrow slots, which the tasks of the native partition's frames of two rows never pass: a
        // ring budget of 192 bytes holds two such tasks, where the cap of six would let four in.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 8);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 6);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TUPLE_RING_MAX_SIZE, 192);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT (x % 4)::INT k, x v, timestamp_sequence('2024-01-01', 7_200_000_000) ts FROM long_sequence(24)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE d AS (SELECT (x % 4)::INT k, x * 100 w FROM long_sequence(12))");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'");
            sqlExecutionContext.changePageFrameSizes(1, 12);
            for (String sql : ENUMERATED) {
                assertFrames(sql, 1 + 6, -1);
                assertFirstWindow(sql, 2, 2, false);
            }
        });
    }

    @Test
    public void testReadParquetFramesKeepTheirRowGroups() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // read_parquet() reads files under the input root; the file holds t's 24 rows in row groups
            // of twelve, whose frames ignore the cap.
            inputRoot = root;
            writeParquet("t", "t.parquet");
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 6);
            sqlExecutionContext.changePageFrameSizes(1, 6);
            final String sql = "SELECT p.v, d.w FROM read_parquet('t.parquet') p LEFT JOIN d ON p.k = d.k AND d.w > p.v - 1_000";
            assertFrames(sql, 2, -1);
            TestUtils.assertContains(assertParallelHashJoin(sql, sqlExecutionContext), "parquet");
            // A stage whose build ran its whole residual writes twelve tuples per row group, which fit
            // the cap of the frame's rows.
            final String pushed = "SELECT p.v, d.w FROM read_parquet('t.parquet') p LEFT JOIN d ON p.k = d.k AND d.w > 0";
            assertFrames(pushed, 2, 0);
            TestUtils.assertContains(assertParallelHashJoin(pushed, sqlExecutionContext), "parquet");
        });
    }

    @Test
    public void testSharedStepperTemplate() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // The steppers' shared template, as on a JVM without hidden classes, cuts the same frames.
            useSteppers(false);
            try {
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 6);
                sqlExecutionContext.changePageFrameSizes(1, 6);
                for (String sql : ENUMERATED) {
                    assertFrames(sql, 12, 0);
                }
                for (String sql : PUSHED) {
                    assertFrames(sql, 4, 0);
                }
                sqlExecutionContext.changePageFrameSizes(6, 6);
                for (String sql : ENUMERATED) {
                    assertFrames(sql, 4, -1);
                }
                for (String sql : PUSHED) {
                    assertFrames(sql, 4, 0);
                }
            } finally {
                restoreSteppers();
            }
        });
    }

    @Test
    public void testStackMultipliesTheFanOuts() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // f holds two rows of each key.
            execute("CREATE TABLE f AS (SELECT (x % 4)::INT k, x * 10 w2 FROM long_sequence(8))");
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 12);
            sqlExecutionContext.changePageFrameSizes(1, 12);
            // Both stages walk their matches, three times two per probe row: frames of two rows. The
            // first stage's build ran its whole residual, but the second stage's residual reads its
            // matches.
            assertFrames("SELECT t.v, d.w, f.w2 FROM t LEFT JOIN d ON t.k = d.k AND d.w > 0 LEFT JOIN f ON t.k = f.k AND f.w2 > d.w - 1_000", 12, 0);
            assertFrames("SELECT t.v, d.w, f.w2 FROM t JOIN d ON t.k = d.k JOIN f ON t.k = f.k WHERE f.w2 > t.v - 1_000", 12, 0);
            // Only the first stage walks its matches, three per probe row, and the second hands its
            // chains to the consumer, with no residual or with one that its build ran whole: frames
            // of four rows.
            assertFrames("SELECT t.v, d.w, f.w2 FROM t LEFT JOIN d ON t.k = d.k AND d.w > t.v - 1_000 LEFT JOIN f ON t.k = f.k", 6, 0);
            assertFrames("SELECT t.v, d.w, f.w2 FROM t LEFT JOIN d ON t.k = d.k AND d.w > t.v - 1_000 LEFT JOIN f ON t.k = f.k AND f.w2 > 0", 6, 0);
            // Both stages' builds ran their whole residuals: both hand their chains on, and the
            // frames keep the twelve configured rows.
            assertFrames("SELECT t.v, d.w, f.w2 FROM t LEFT JOIN d ON t.k = d.k AND d.w > 0 LEFT JOIN f ON t.k = f.k", 2, 0);
            assertFrames("SELECT t.v, d.w, f.w2 FROM t LEFT JOIN d ON t.k = d.k AND d.w > 0 LEFT JOIN f ON t.k = f.k AND f.w2 > 0", 2, 0);
        });
    }

    @Test
    public void testSwappedFirstWindowHoldsTheTaskCap() throws Exception {
        // As in testSwappedInputsFollowTheLeftBuild: the join hashes l, three rows of each key, whose
        // matches the workers walk, and cuts r's frames to two rows, whose tasks fill the cap of six
        // tuples. A ring budget of 144 bytes holds three such tasks' tuples of one stage, in narrow
        // slots, 48 bytes each, and two of two stages, 72 bytes each, where the frames' rows would
        // let the dispatch limit's eight frames in, and six. The first row comes with the ramp's two
        // frames, within that window.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 8);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 6);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TUPLE_RING_MAX_SIZE, 144);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE l AS (SELECT (x % 4)::INT k, x v, timestamp_sequence('2024-01-01', 60_000_000) ts FROM long_sequence(12)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE r AS (SELECT (x % 4)::INT k, x * 100 w, timestamp_sequence('2024-01-01', 60_000_000) ts FROM long_sequence(24)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE u AS (SELECT (x % 4)::INT k, x * 7 w2 FROM long_sequence(4))");
            execute("CREATE TABLE d AS (SELECT (x % 4)::INT k, x * 100 w FROM long_sequence(12))");
            sqlExecutionContext.changePageFrameSizes(1, 6);
            final Object[][] queriesAndWindows = {
                    {"SELECT l.v, r.w FROM l JOIN r ON l.k = r.k WHERE r.w > l.v", 3},
                    {"SELECT l.v, r.w, u.w2 FROM l JOIN r ON l.k = r.k JOIN u ON l.k = u.k", 2},
                    {"SELECT l.v, r.w, d.w dw FROM l JOIN r ON l.k = r.k LEFT JOIN d ON l.k = d.k AND d.w > 0", 2},
            };
            for (Object[] query : queriesAndWindows) {
                final String sql = (String) query[0];
                final int window = (int) query[1];
                assertFrames(sql, 12, 0);
                assertFirstWindow(sql, window, Math.min(2, window), true);
            }
        });
    }

    @Test
    public void testSwappedInputsFollowTheLeftBuild() throws Exception {
        assertMemoryLeak(() -> {
            // l, the left input, holds twelve rows, three of each key, and r, the right one, twenty-four:
            // the join hashes l and probes r's frames, whose matches a post-join filter walks.
            execute("CREATE TABLE l AS (SELECT (x % 4)::INT k, x v, timestamp_sequence('2024-01-01', 60_000_000) ts FROM long_sequence(12)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE r AS (SELECT (x % 4)::INT k, x * 100 w, timestamp_sequence('2024-01-01', 60_000_000) ts FROM long_sequence(24)) TIMESTAMP(ts) PARTITION BY DAY");
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 6);
            sqlExecutionContext.changePageFrameSizes(1, 6);
            execute("CREATE TABLE u AS (SELECT (x % 4)::INT k, x * 7 w2 FROM long_sequence(4))");
            // d holds three rows of each key.
            execute("CREATE TABLE d AS (SELECT (x % 4)::INT k, x * 100 w FROM long_sequence(12))");
            for (String sql : new String[]{
                    "SELECT l.v, r.w FROM l JOIN r ON l.k = r.k WHERE r.w > l.v",
                    // A later stage reads its key from l's rows, so the workers walk l's matches,
                    // though no filter reads them; the query's orientation would hand them on.
                    "SELECT l.v, r.w, u.w2 FROM l JOIN r ON l.k = r.k JOIN u ON l.k = u.k",
                    // So they do ahead of a LEFT stage whose build ran its whole residual over d's
                    // rows, which hands d's chains on: only l's fan-out caps the frames.
                    "SELECT l.v, r.w, d.w dw FROM l JOIN r ON l.k = r.k LEFT JOIN d ON l.k = d.k AND d.w > 0",
            }) {
                assertFrames(sql, 12, 0);
                try (RecordCursorFactory factory = select(sql)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(join);
                    result(factory, sqlExecutionContext);
                    Assert.assertTrue(sql, join.getAtom().isSwapped());
                }
            }
        });
    }

    @Test
    public void testWalkingStageAtServerDefaultFrames() throws Exception {
        assertMemoryLeak(() -> {
            // One partition of 200_001 probe rows, and a build of eleven rows of each of the probe's
            // thousand keys, every pair of which the residual and the post-join filter keep. At the
            // server's default frame sizes and task cap, a probe frame's 100_000 rows, a walking
            // stage caps the frames at 100_000 / 11 = 9_090 rows, below the 100_000-row minimum,
            // whose frames would hold 1_100_000 tuples each and continue: 22 frames of 9_090 rows,
            // whose 99_990 tuples fit the cap, and one of the 21 rows left. Only the first frame
            // continues, as an execution without a LIMIT joins its first 4_096 rows first: two
            // chunks, of 4_096 rows and of the 898 left.
            execute("CREATE TABLE tl AS (SELECT (x % 1_000)::INT k, x v, timestamp_sequence('2024-01-01', 100_000) ts FROM long_sequence(200_001)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE dl AS (SELECT (x % 1_000)::INT k, x w FROM long_sequence(11_000))");
            sqlExecutionContext.changePageFrameSizes(100_000, 1_000_000);
            Assert.assertEquals(100_000, configuration.getSqlParallelHashJoinTaskOutputMaxRows());
            Assert.assertEquals(23, assertDefaultFrames("SELECT tl.v, dl.w FROM tl LEFT JOIN dl ON tl.k = dl.k AND dl.w + tl.v > 0", 2));
            Assert.assertEquals(23, assertDefaultFrames("SELECT tl.v, dl.w FROM tl JOIN dl ON tl.k = dl.k WHERE dl.w + tl.v > 0", 2));
        });
    }

    @Test
    public void testWholePushedResidualKeepsTheDefaultFrames() throws Exception {
        assertMemoryLeak(() -> {
            // One partition of 200_001 probe rows, and a build of eleven rows of each of the probe's
            // thousand keys, every one of which the ON residual keeps. At the server's default frame
            // sizes, a stage that walked its matches would cut frames of 100_000 / 11 = 9_090 rows,
            // see testWalkingStageAtServerDefaultFrames. A stage whose build ran its whole residual
            // cuts the frames of the join without the residual: those of the join's 100_000-row
            // maximum, two of them and one of the row left, which no frame grows past the maximum to
            // absorb, and which need no continuation but the first frame's, as an execution without
            // a LIMIT joins its first 4_096 rows first: chunks of 4_096, 8_192, 16_384 and 32_768
            // rows, and of the 34_464 left.
            execute("CREATE TABLE tl AS (SELECT (x % 1_000)::INT k, x v, timestamp_sequence('2024-01-01', 100_000) ts FROM long_sequence(200_001)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE dl AS (SELECT (x % 1_000)::INT k, x w FROM long_sequence(11_000))");
            sqlExecutionContext.changePageFrameSizes(100_000, 1_000_000);
            Assert.assertEquals(100_000, configuration.getSqlParallelHashJoinTaskOutputMaxRows());
            final String plain = "SELECT tl.v, dl.w FROM tl LEFT JOIN dl ON tl.k = dl.k";
            Assert.assertEquals(3, assertDefaultFrames(plain, 5));
            Assert.assertEquals(3, assertDefaultFrames(plain + " AND dl.w > 0", 5));
        });
    }

    // Compares the operator's rows with the ordinary plan's, in order, row by row, checks that the
    // operator's execution needed the given continuations, those of its first frame, and returns the
    // probe frames it cut.
    private static int assertDefaultFrames(String sql, int continuations) throws SqlException {
        final boolean isEnabledBefore = sqlExecutionContext.isParallelHashJoinEnabled();
        sqlExecutionContext.setParallelHashJoinEnabled(false);
        try (RecordCursorFactory expected = select(sql)) {
            Assert.assertNull(sql, find(expected));
            sqlExecutionContext.setParallelHashJoinEnabled(true);
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(sql, join);
                try (
                        RecordCursor expectedCursor = expected.getCursor(sqlExecutionContext);
                        RecordCursor cursor = factory.getCursor(sqlExecutionContext)
                ) {
                    TestUtils.assertEquals(expectedCursor, expected.getMetadata(), cursor, factory.getMetadata(), false);
                    Assert.assertEquals(sql, continuations, join.getAtom().getContinuationCount());
                    return join.getProbeFrameCount();
                }
            }
        } finally {
            sqlExecutionContext.setParallelHashJoinEnabled(isEnabledBefore);
        }
    }

    // An execution of the query, which swaps the first join's inputs or not, starts at the given
    // window of the ring, in narrow tuple slots, and its first row comes with the given frames
    // dispatched.
    private static void assertFirstWindow(String sql, int window, int dispatched, boolean isSwapped) throws SqlException {
        try (RecordCursorFactory factory = select(sql)) {
            final AsyncHashJoinRecordCursorFactory join = find(factory);
            Assert.assertNotNull(sql, join);
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(sql, cursor.hasNext());
                Assert.assertEquals(sql, isSwapped, join.getAtom().isSwapped());
                Assert.assertTrue(sql, join.getAtom().getRing().isNarrow());
                Assert.assertEquals(sql, window, join.getAtom().getRing().getDispatchLimit());
                Assert.assertEquals(sql, dispatched, join.getDispatchedFrameCount());
            }
        }
    }

    // Runs the query on the operator and compares it with the ordinary plan, and checks the probe
    // frames that an execution cut and the continuations it needed: none, or, given -1, at least one.
    private static void assertFrames(String sql, int frames, int continuations) throws SqlException {
        assertParallelHashJoin(sql, sqlExecutionContext);
        try (RecordCursorFactory factory = select(sql)) {
            final AsyncHashJoinRecordCursorFactory join = find(factory);
            Assert.assertNotNull(join);
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                //noinspection StatementWithEmptyBody
                while (cursor.hasNext()) {
                }
                Assert.assertEquals(sql, frames, join.getProbeFrameCount());
            }
            if (continuations < 0) {
                Assert.assertTrue(sql, join.getAtom().getContinuationCount() > 0);
            } else {
                Assert.assertEquals(sql, continuations, join.getAtom().getContinuationCount());
            }
            if (sql.contains("ORDER BY ts DESC")) {
                TestUtils.assertContains(plan(factory, sqlExecutionContext), "Frame backward scan on: t");
            }
            if (sql.contains("k k2")) {
                final String plan = plan(factory, sqlExecutionContext);
                Assert.assertTrue(plan, plan.indexOf("SelectedRecord", plan.indexOf("Probe")) > -1);
            }
        }
    }

    // True when the HashJoinBuildStage method is on the current thread's stack.
    private static boolean isOnStack(String methodName) {
        return StackWalker.getInstance().walk(frames -> frames.anyMatch(frame -> frame.getClassName().equals(HashJoinBuildStage.class.getName())
                && frame.getMethodName().equals(methodName)));
    }

    // The probe frames of an execution of the factory, which reads every row.
    private static int probeFrames(RecordCursorFactory factory, AsyncHashJoinRecordCursorFactory join) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            //noinspection StatementWithEmptyBody
            while (cursor.hasNext()) {
            }
            return join.getProbeFrameCount();
        }
    }

    // Writes the table's first partition to a Parquet file under the root, in row groups of twelve
    // rows, for read_parquet().
    private static void writeParquet(String table, String file) {
        try (
                Path path = new Path();
                PartitionDescriptor descriptor = new PartitionDescriptor();
                TableReader reader = engine.getReader(table)
        ) {
            path.of(root).concat(file);
            engine.getConfiguration().getFilesFacade().remove(path.$());
            PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
            PartitionEncoder.encodeWithOptions(
                    descriptor,
                    path,
                    ParquetCompression.COMPRESSION_UNCOMPRESSED,
                    true,
                    false,
                    12,
                    0,
                    ParquetVersion.PARQUET_VERSION_V1,
                    0.0
            );
            Assert.assertTrue(Files.exists(path.$()));
        }
    }

    private static void createTables() throws Exception {
        execute("""
                CREATE TABLE t AS (
                    SELECT (x % 4)::INT k, x v, timestamp_sequence('2024-01-01', 60_000_000) ts
                    FROM long_sequence(24)
                ) TIMESTAMP(ts) PARTITION BY DAY
                """);
        execute("CREATE TABLE d AS (SELECT (x % 4)::INT k, x * 100 w FROM long_sequence(12))");
    }
}
