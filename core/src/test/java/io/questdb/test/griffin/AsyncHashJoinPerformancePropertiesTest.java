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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.engine.table.AsyncHashJoinGroupByAtom;
import io.questdb.griffin.engine.table.AsyncHashJoinGroupByRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.HashJoinBuildStage;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.test.griffin.ParallelHashJoinTestUtils.*;

/**
 * Deterministic performance properties of the parallel hash join. The probe reducer and the
 * consumer allocate nothing on the heap once an execution's first row is out, whatever the rows
 * that follow: the probe tasks, which the owner runs here, as no worker serves the test's context,
 * and the consumer that reads their tuples. And an execution builds each stage once, in a known
 * number of rounds of tasks, however often its cursor goes back to the top or counts its rows.
 */
public class AsyncHashJoinPerformancePropertiesTest extends AbstractCairoTest {

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
    public void testBuildAndRoundCountsPerExecution() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(1_000, 1_000);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 256);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_PARALLEL_MIN_PROBE_RATIO, "1000000");
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MIN_PROBE_RATIO, "1000000");
            final String left = "SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k";
            // On the owner: one build and no round per execution.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, Long.MAX_VALUE);
            assertCounts(left, 1, 0);
            // In rounds: one build of two rounds, one that sorts the frames' rows into partitions and
            // one that fills the partitions.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 0);
            assertCounts(left, 1, 2);
            // And one more round when the build copies its payload.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_PARALLEL_MIN_PROBE_RATIO, "0");
            assertCounts(left, 1, 3);
            // The swapped orientation builds the left input, once per execution too.
            assertCounts("SELECT d.w, t.v FROM d JOIN t ON d.k = t.k", 1, 3);
            // An INNER join over an empty build builds once, and dispatches no probe task.
            execute("CREATE TABLE e (k INT, w LONG)");
            try (RecordCursorFactory factory = select("SELECT t.k, e.w FROM t JOIN e ON t.k = e.k WHERE t.ts IN '2024-01-01'")) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                final HashJoinBuildStage stage = join.getAtom().getStage(0);
                final long builds = stage.getBuildCount();
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertFalse(cursor.hasNext());
                    cursor.toTop();
                    Assert.assertFalse(cursor.hasNext());
                    Assert.assertEquals(0, join.getProbeFrameCount());
                }
                Assert.assertEquals(builds + 1, stage.getBuildCount());
            }
            // The fused GROUP BY's lone count over a partitioned build counts its key rows in a
            // third round.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 1);
            sqlExecutionContext.changePageFrameSizes(1_000, 100_000);
            try (RecordCursorFactory factory = select("SELECT count() FROM t JOIN d ON t.k = d.k")) {
                AsyncHashJoinGroupByRecordCursorFactory fused = null;
                for (RecordCursorFactory f = factory; f != null && fused == null; f = f.getBaseFactory()) {
                    if (f instanceof AsyncHashJoinGroupByRecordCursorFactory candidate) {
                        fused = candidate;
                    }
                }
                Assert.assertNotNull(fused);
                final AsyncHashJoinGroupByAtom atom = fused.getAtom();
                final HashJoinBuildStage stage = atom.getBuildingStage();
                final long builds = stage.getBuildCount();
                final long rounds = stage.getRoundCount();
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertTrue(atom.isCountedInRounds());
                }
                Assert.assertEquals(builds + 1, stage.getBuildCount());
                Assert.assertEquals(rounds + 3, stage.getRoundCount());
            }
        });
    }

    @Test
    public void testNoHeapAllocationPerRowInProbeReducerAndConsumer() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(1_000, 1_000);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 4);
            for (String sql : new String[]{
                    // Unique matches, whose locators the consumer positions its reader at.
                    "SELECT t.k, t.v, u.w FROM t LEFT JOIN u ON t.k = u.k",
                    // Several matches per key, whose chains the consumer walks.
                    "SELECT t.k, t.v, d.w, d.s FROM t LEFT JOIN d ON t.k = d.k",
                    // A residual, which the probe reducer runs per pair, and the continuations that
                    // the cap of one tuple per task leaves to the consumer.
                    "SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w > t.v % 100",
                    // A translated SYMBOL key, a staged LONG key and a probe filter.
                    "SELECT t.v, d.w, d.s FROM t JOIN d ON t.s = d.s WHERE t.ts IN '2024-01-01'",
                    "SELECT t.v, d.w FROM t LEFT JOIN d ON t.l = d.l WHERE t.v % 3 = 0",
                    // The swapped orientation, which probes t and reads d's columns from its build.
                    "SELECT d.w, d.s, t.v FROM d JOIN t ON d.k = t.k",
            }) {
                try (
                        RecordCursorFactory factory = select(sql);
                        TestUtils.ThreadMetricsScope<com.sun.management.ThreadMXBean> scope = TestUtils.threadAllocationScope()
                ) {
                    Assert.assertNotNull(sql, find(factory));
                    final RecordMetadata metadata = factory.getMetadata();
                    long minAllocated = Long.MAX_VALUE;
                    long rows = 0;
                    // The first executions warm up the steppers and the readers; a compile that the
                    // JVM queues inside the measured window charges the thread, which a later attempt
                    // does not repeat.
                    for (int attempt = 0; attempt < 12 && minAllocated > 0; attempt++) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            // The first row builds and dispatches the first frames.
                            Assert.assertTrue(sql, cursor.hasNext());
                            final Record record = cursor.getRecord();
                            final long before = scope.getBean().getCurrentThreadAllocatedBytes();
                            long count = 1;
                            long checksum = 0;
                            while (cursor.hasNext()) {
                                count++;
                                checksum += readRow(record, metadata);
                            }
                            final long allocated = scope.getBean().getCurrentThreadAllocatedBytes() - before;
                            Assert.assertTrue(checksum != 0);
                            rows = count;
                            minAllocated = Math.min(minAllocated, allocated);
                        }
                    }
                    Assert.assertTrue(sql + ": " + rows, rows > 10_000);
                    Assert.assertEquals(sql, 0, minAllocated);
                }
            }
        });
    }

    private static void assertCounts(String sql, int buildsPerExecution, int roundsPerExecution) throws Exception {
        try (RecordCursorFactory factory = select(sql)) {
            final AsyncHashJoinRecordCursorFactory join = find(factory);
            Assert.assertNotNull(sql, join);
            for (int execution = 0; execution < 2; execution++) {
                final HashJoinBuildStage stage;
                final long builds;
                final long rounds;
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    // The execution's first stage, as its orientation picks it.
                    stage = join.getAtom().getStage(0);
                    builds = stage.getBuildCount();
                    rounds = stage.getRoundCount();
                    long count = 0;
                    while (cursor.hasNext()) {
                        count++;
                    }
                    // Back to the top, a count of the rows and another pass reuse the build.
                    cursor.toTop();
                    final RecordCursor.Counter counter = new RecordCursor.Counter();
                    cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                    Assert.assertEquals(count, counter.get());
                    cursor.toTop();
                    Assert.assertTrue(cursor.hasNext());
                    cursor.toTop();
                    long again = 0;
                    while (cursor.hasNext()) {
                        again++;
                    }
                    Assert.assertEquals(count, again);
                }
                Assert.assertEquals(sql, builds + buildsPerExecution, stage.getBuildCount());
                Assert.assertEquals(sql, rounds + roundsPerExecution, stage.getRoundCount());
            }
        }
    }

    private static void createTables() throws Exception {
        execute("""
                CREATE TABLE t AS (
                    SELECT (x % 2_000)::INT k, x v, (x % 1_500)::LONG l, ('s' || (x % 300))::SYMBOL s,
                        timestamp_sequence('2024-01-01', 2_000_000) ts
                    FROM long_sequence(60_000)
                ) TIMESTAMP(ts) PARTITION BY DAY
                """);
        // d repeats its keys, u holds one row per key.
        execute("""
                CREATE TABLE d AS (
                    SELECT (x % 1_800)::INT k, x w, (x % 1_500)::LONG l, ('s' || (x % 250))::SYMBOL s
                    FROM long_sequence(5_000)
                )
                """);
        execute("CREATE TABLE u AS (SELECT x::INT k, x w FROM long_sequence(1_900))");
    }

    // Reads every column of the row through the getter of its type, as a client would, and returns a
    // value that depends on what it read.
    private static long readRow(Record record, RecordMetadata metadata) {
        long checksum = 0;
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            switch (metadata.getColumnType(i)) {
                case io.questdb.cairo.ColumnType.INT -> checksum += record.getInt(i);
                case io.questdb.cairo.ColumnType.LONG -> checksum += record.getLong(i);
                case io.questdb.cairo.ColumnType.SYMBOL -> {
                    final CharSequence value = record.getSymA(i);
                    checksum += value != null ? value.length() : 0;
                }
                default -> checksum++;
            }
        }
        return checksum;
    }
}
