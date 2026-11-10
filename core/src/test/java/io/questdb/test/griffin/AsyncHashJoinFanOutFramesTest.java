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
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
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

import static io.questdb.test.griffin.ParallelHashJoinTestUtils.*;

/**
 * The probe frames of a parallel hash join whose workers walk duplicate matches: once the builds are
 * done, the probe scan cuts frames of no more rows than a task's cap of tuples holds at the fan-out
 * that the builds lead one to expect, the product of the rows per key of every build whose matches
 * the workers walk, under an ON residual or a post-join filter. Such a frame needs no continuation,
 * where a frame of the configured rows leaves its rest to the consumer. The frames never get fewer
 * rows than {@code cairo.sql.page.frame.min.rows}; a stage that hands its chains to the consumer,
 * a build of one row per key and an empty one lower nothing; a Parquet partition keeps its row
 * groups; and each execution of a cached factory sizes its frames from its own builds. The rows
 * always equal the ordinary plan's, in order.
 * <p>
 * The probe table t holds 24 rows in one partition, four keys, and d three rows of each key.
 */
public class AsyncHashJoinFanOutFramesTest extends AbstractCairoTest {
    // Joins whose single stage walks d's three matches of each key on the workers.
    private static final String[] ENUMERATED = {
            "SELECT t.v, d.w FROM t JOIN d ON t.k = d.k WHERE d.w > t.v - 1_000",
            "SELECT t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w > 0",
            "SELECT t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w > 0 ORDER BY ts DESC",
            "SELECT t.v, d.w FROM t JOIN d ON t.k = d.k WHERE t.v > 2 AND d.w > t.v - 1_000",
            // A projection that names a column twice keeps its own frame cursor over the scan's.
            "SELECT t.v, d.w FROM (SELECT ts, v, k k1, k k2 FROM t) t JOIN d ON t.k1 = d.k WHERE d.w > t.v - 1_000 + t.k2",
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
        });
    }

    @Test
    public void testFramesPerExecutionOfCachedFactory() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE g AS (SELECT (x % 4)::INT k, x * 3 w FROM long_sequence(4))");
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 12);
            sqlExecutionContext.changePageFrameSizes(1, 12);
            final String sql = "SELECT t.v, g.w FROM t LEFT JOIN g ON t.k = g.k AND g.w > 0";
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                // One row per key, then two, then six: frames of twelve rows, six, two.
                final int[] frames = {2, 4, 12};
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
                // Back to one row per key: the frames grow back.
                execute("TRUNCATE TABLE g");
                execute("INSERT INTO g SELECT (x % 4)::INT k, x * 3 w FROM long_sequence(4)");
                Assert.assertEquals(ordinary(sql, sqlExecutionContext), result(factory, sqlExecutionContext));
                Assert.assertEquals(2, probeFrames(factory, join));
            }
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
            final String sql = "SELECT p.v, d.w FROM read_parquet('t.parquet') p LEFT JOIN d ON p.k = d.k AND d.w > 0";
            assertFrames(sql, 2, -1);
            TestUtils.assertContains(assertParallelHashJoin(sql, sqlExecutionContext), "parquet");
        });
    }

    @Test
    public void testSharedStepperTemplate() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // The steppers' shared template, as on a JVM without hidden classes, cuts the same frames.
            AsyncHashJoinRecordCursorFactory.setStepperCopiesEnabledForTesting(false);
            try {
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 6);
                sqlExecutionContext.changePageFrameSizes(1, 6);
                for (String sql : ENUMERATED) {
                    assertFrames(sql, 12, 0);
                }
                sqlExecutionContext.changePageFrameSizes(6, 6);
                for (String sql : ENUMERATED) {
                    assertFrames(sql, 4, -1);
                }
            } finally {
                AsyncHashJoinRecordCursorFactory.setStepperCopiesEnabledForTesting(true);
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
            // Both stages walk their matches, three times two per probe row: frames of two rows.
            assertFrames("SELECT t.v, d.w, f.w2 FROM t LEFT JOIN d ON t.k = d.k AND d.w > 0 LEFT JOIN f ON t.k = f.k AND f.w2 > d.w - 1_000", 12, 0);
            assertFrames("SELECT t.v, d.w, f.w2 FROM t JOIN d ON t.k = d.k JOIN f ON t.k = f.k WHERE f.w2 > t.v - 1_000", 12, 0);
            // Only the first stage walks its matches, three per probe row, and the second hands its
            // chains to the consumer: frames of four rows.
            assertFrames("SELECT t.v, d.w, f.w2 FROM t LEFT JOIN d ON t.k = d.k AND d.w > 0 LEFT JOIN f ON t.k = f.k", 6, 0);
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
            for (String sql : new String[]{
                    "SELECT l.v, r.w FROM l JOIN r ON l.k = r.k WHERE r.w > l.v",
                    // A later stage reads its key from l's rows, so the workers walk l's matches,
                    // though no filter reads them; the query's orientation would hand them on.
                    "SELECT l.v, r.w, u.w2 FROM l JOIN r ON l.k = r.k JOIN u ON l.k = u.k",
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
