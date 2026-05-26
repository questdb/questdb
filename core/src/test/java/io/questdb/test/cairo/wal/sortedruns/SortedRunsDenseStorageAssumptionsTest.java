/*******************************************************************************
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

package io.questdb.test.cairo.wal.sortedruns;

import io.questdb.cairo.TableToken;
import io.questdb.test.AbstractCairoTest;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Probes for SQL query patterns that assume dense, on-disk-sorted column
 * storage and therefore may return wrong results on partitions in the
 * {@link io.questdb.cairo.sql.PartitionFormat#INDEXED_SORTED_RUNS} format.
 * Column data is in insertion order; logical-to-physical mapping lives in
 * the mmapped {@code _sortedruns.idx} file and is applied via
 * {@code PageFrameMemoryRecord.setRowIndex}.
 * <p>
 * Tests marked {@code @Test} document patterns that already work via the
 * scratch indirection. Tests marked {@code @Ignore} are open punch-list
 * items - each has a {@code Failure mode:} note describing the observed
 * incorrect result and the call path responsible.
 * <p>
 * Fixture: 3 partitions x 3 commits each (9 sorted runs), 18 rows in total.
 * Commits within a partition arrive in middle-of-day, earliest-of-day, then
 * latest-of-day order, so the column files are NOT in ts order.
 */
public class SortedRunsDenseStorageAssumptionsTest extends AbstractCairoTest {

    private static final long DAY_MICROS = 86_400_000_000L;

    // ---------------------------------------------------------------------
    // Confirmed-working patterns. These exercise the standard cursor->record
    // pipeline that goes through setRowIndex and so see ts-ordered data.
    // ---------------------------------------------------------------------

    @Test
    public void testAvgVal() throws Exception {
        runOnSortedRuns("SELECT avg(val) FROM x",
                "avg\n" +
                        "135.0\n");
    }

    @Test
    public void testCountStar() throws Exception {
        runOnSortedRuns("SELECT count(*) FROM x",
                "count\n" +
                        "18\n");
    }

    /**
     * Failure mode: first(val) returns the value of <b>physical row 0</b>
     * (val=30, from the middle-ts commit that was inserted first) instead of
     * the value of the <b>logical row 0</b> (val=10, ts-smallest row 1000).
     * The vector-aggregate path in
     * {@code griffin/engine/functions/groupby/} reads the column page
     * starting at offset 0 and never goes through {@code setRowIndex}.
     */
    @Ignore("known dense-storage failure: first() reads column[0], not logical[0]")
    @Test
    public void testFirstVal() throws Exception {
        // Smallest ts row is (1000, 10).
        runOnSortedRuns("SELECT first(val) FROM x",
                "first\n" +
                        "10\n");
    }

    @Test
    public void testGroupByValCount() throws Exception {
        // Categorical GROUP BY: order-independent, must give exact counts.
        runOnSortedRuns("SELECT val, count(*) FROM x WHERE val IN (10, 110, 210) ORDER BY val",
                "val\tcount\n" +
                        "10\t1\n" +
                        "110\t1\n" +
                        "210\t1\n");
    }

    /**
     * Sanity test: same query as {@link #testWhereValGtConstScalar} but with
     * JIT mode forced on. If JIT compilation activates, the filter is run by
     * native code that scans column pages directly. For our predicate
     * (val > 50, count(*)) the result is order-independent so this would
     * still pass even when JIT bypasses {@code setRowIndex}. A stronger
     * regression test would require a JIT-pushed-down predicate followed by
     * a downstream consumer that needs ts-ordered row ids.
     */
    @Test
    public void testJitWhereFilter() throws Exception {
        node1.setProperty(io.questdb.PropertyKey.CAIRO_SQL_JIT_MODE, "on");
        try {
            runOnSortedRuns("SELECT count(*) FROM x WHERE val > 50",
                    "count\n" +
                            "13\n");
        } finally {
            node1.setProperty(io.questdb.PropertyKey.CAIRO_SQL_JIT_MODE, "off");
        }
    }

    /**
     * Failure mode: last(val) returns the value of the <b>physical last row</b>
     * (which happens to be ts-largest in our fixture because pass 3 inserts
     * the latest commit) - so this <i>accidentally</i> matches. To make the
     * failure visible we would need a fixture where the largest-ts row was
     * NOT inserted last. Kept here to document the path; see the design
     * note in {@code SampleByFirstLastRecordCursorFactory} for the read
     * pattern that is order-blind today.
     */
    @Test
    public void testLastVal() throws Exception {
        runOnSortedRuns("SELECT last(val) FROM x",
                "last\n" +
                        "260\n");
    }

    @Test
    public void testMaxVal() throws Exception {
        // Commutative SIMD aggregate over a full column scan.
        runOnSortedRuns("SELECT max(val) FROM x",
                "max\n" +
                        "260\n");
    }

    @Test
    public void testMinVal() throws Exception {
        // Commutative SIMD aggregate.
        runOnSortedRuns("SELECT min(val) FROM x",
                "min\n" +
                        "10\n");
    }

    @Test
    public void testOrderByValDesc() throws Exception {
        runOnSortedRuns("SELECT val FROM x ORDER BY val DESC LIMIT 3",
                "val\n" +
                        "260\n" +
                        "250\n" +
                        "240\n");
    }

    @Test
    public void testSampleByDay() throws Exception {
        // SAMPLE BY with sum() - commutative, order-independent within bucket.
        runOnSortedRuns("SELECT ts, sum(val) FROM x SAMPLE BY 1d",
                "ts\tsum\n" +
                        "1970-01-01T00:00:00.000000Z\t210\n" +
                        "1970-01-02T00:00:00.000000Z\t810\n" +
                        "1970-01-03T00:00:00.000000Z\t1410\n");
    }

    /**
     * Failure mode: per-bucket first(val) returns the val at the first
     * <b>inserted</b> row of each partition (30, 130, 230) instead of the val
     * at each bucket's ts-smallest row (10, 110, 210). The dense-storage
     * audit pins this to
     * {@code SampleByFirstLastRecordCursorFactory:675,678,681,...} which
     * reads column data via {@code Unsafe.getLong(pageAddress + (rowId << 3))}
     * without {@code setRowIndex} translation.
     */
    @Test
    @Ignore("known dense-storage failure: SAMPLE BY first() reads physical row 0 per bucket")
    public void testSampleByDayFirstLast() throws Exception {
        runOnSortedRuns(
                "SELECT ts, first(val) f, last(val) l FROM x SAMPLE BY 1d",
                "ts\tf\tl\n" +
                        "1970-01-01T00:00:00.000000Z\t10\t60\n" +
                        "1970-01-02T00:00:00.000000Z\t110\t160\n" +
                        "1970-01-03T00:00:00.000000Z\t210\t260\n"
        );
    }

    @Test
    public void testSelectDistinctVal() throws Exception {
        runOnSortedRuns("SELECT DISTINCT val FROM x WHERE val IN (10, 60, 260) ORDER BY val",
                "val\n" +
                        "10\n" +
                        "60\n" +
                        "260\n");
    }

    @Test
    public void testSumVal() throws Exception {
        // Commutative SIMD aggregate.
        runOnSortedRuns("SELECT sum(val) FROM x",
                "sum\n" +
                        "2430\n");
    }

    @Test
    public void testWhereValGtConstScalar() throws Exception {
        // Scalar (non-JIT) filter. Each candidate row is visited via the
        // record cursor + setRowIndex pipeline, so the scratch indirection is
        // honoured. Result is the same as on NATIVE storage.
        runOnSortedRuns("SELECT count(*) FROM x WHERE val > 50",
                "count\n" +
                        "13\n");
    }

    /**
     * Failure mode (expected, not yet captured): {@code lag()} should report
     * the val of the previous logical (ts-ascending) row. If the window
     * implementation visits rows in physical order it would attribute the
     * wrong lag value. Today's observed result happens to be ts-correct
     * because the window operator iterates the record cursor, but the
     * assertion below is a regression sentinel for the broader window path.
     */
    @Test
    public void testWindowLag() throws Exception {
        runOnSortedRuns(
                "SELECT ts, val, lag(val) OVER (ORDER BY ts) lag1 FROM x LIMIT 3",
                "ts\tval\tlag1\n" +
                        "1970-01-01T00:00:00.001000Z\t10\tnull\n" +
                        "1970-01-01T00:00:00.002000Z\t20\t10\n" +
                        "1970-01-01T00:00:00.003000Z\t30\t20\n"
        );
    }

    /**
     * Same fixture as {@link SortedRunsSqlParityTest}: 3 partitions, 3 commits
     * per partition, interleaved so each partition has cross-commit O3 with
     * the physical row order being [middle, earliest, latest] (i.e., NOT ts
     * order).
     */
    private TableToken insertMultiPartitionFixture() throws Exception {
        execute("CREATE TABLE x (ts TIMESTAMP, val LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
        final long d0 = 0L;
        final long d1 = DAY_MICROS;
        final long d2 = 2 * DAY_MICROS;

        execute("INSERT INTO x VALUES (" + (d0 + 3000) + ", 30), (" + (d0 + 4000) + ", 40)");
        execute("INSERT INTO x VALUES (" + (d1 + 3000) + ", 130), (" + (d1 + 4000) + ", 140)");
        execute("INSERT INTO x VALUES (" + (d2 + 3000) + ", 230), (" + (d2 + 4000) + ", 240)");

        execute("INSERT INTO x VALUES (" + (d0 + 1000) + ", 10), (" + (d0 + 2000) + ", 20)");
        execute("INSERT INTO x VALUES (" + (d1 + 1000) + ", 110), (" + (d1 + 2000) + ", 120)");
        execute("INSERT INTO x VALUES (" + (d2 + 1000) + ", 210), (" + (d2 + 2000) + ", 220)");

        execute("INSERT INTO x VALUES (" + (d0 + 5000) + ", 50), (" + (d0 + 6000) + ", 60)");
        execute("INSERT INTO x VALUES (" + (d1 + 5000) + ", 150), (" + (d1 + 6000) + ", 160)");
        execute("INSERT INTO x VALUES (" + (d2 + 5000) + ", 250), (" + (d2 + 6000) + ", 260)");

        return engine.verifyTableName("x");
    }

    private void runOnSortedRuns(CharSequence sql, CharSequence expected) throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = insertMultiPartitionFixture();
            try (SortedRunsApplyDriver driver = new SortedRunsApplyDriver(engine)) {
                driver.apply(token);
            }
            assertSql(expected, sql);
        });
    }
}
