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

package io.questdb.test.cairo.covering;

import io.questdb.PropertyKey;
import io.questdb.griffin.engine.table.CoveringIndexRecordCursorFactory;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;

public abstract class AbstractCoveringIndexQueryTest extends AbstractCairoTest {

    @Override
    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
        super.setUp();
    }

    /**
     * Assert the indexed arm returns exactly what a full scan returns.
     * <p>
     * The reference arm MUST use {@code /*+ no_index *}{@code /}. Do not use
     * {@code /*+ no_covering *}{@code /}: it does not disable the index, it routes to
     * FilterOnValues plus a serial group by -- a third execution path, and the slowest
     * of the four. An arm mislabelled that way has already produced one wrong baseline.
     */
    protected void assertSameResult(String indexedSql, String referenceSql) throws Exception {
        final StringSink indexed = new StringSink();
        final StringSink reference = new StringSink();
        printSql(indexedSql, indexed);
        printSql(referenceSql, reference);
        TestUtils.assertEquals(reference, indexed);
    }

    /**
     * RUN {@code sql} and assert the execution really took per-key (unordered) frame mode.
     * <p>
     * This is what {@code assertsPlanContaining("frames: per-key")} is NOT, and the distinction is
     * the one this package gets wrong most often. The plan prints the plan-stable PERMISSION the
     * compiler granted. Whether an execution exercises it is decided per open, from the
     * frame-count ceiling and the density estimate, and an open that declines returns the SAME
     * ROWS through the merge -- so a fixture drifting under the density crossover leaves every
     * plan assertion and every result assertion green while the mode under test stops running
     * altogether. Measured on {@code dec_tel} at 60 rows per pair: the plan says
     * {@code per-key (unordered)}, {@code perKeyOpens=0}, {@code mergedOpens=1}, class 14/14
     * green.
     * <p>
     * {@code assertsPlanContaining} does not even execute the query (it is documented as
     * "without running the query for a result"), so it cannot move these counters at all. Any arm
     * whose javadoc claims a control "DOES take per-key" needs this, not that.
     */
    protected void assertRunsPerKeyMode(String sql) throws Exception {
        CoveringIndexRecordCursorFactory.resetModeSelectionsForTesting();
        printSql(sql, new StringSink());
        final long perKeyOpens = CoveringIndexRecordCursorFactory.getPerKeyModeOpensForTesting();
        final long mergedOpens = CoveringIndexRecordCursorFactory.getMergedModeOpensForTesting();
        Assert.assertTrue(
                "this query was supposed to RUN per-key mode and did not: perKeyOpens=" + perKeyOpens
                        + ", mergedOpens=" + mergedOpens + ". The plan would still print"
                        + " 'frames: per-key (unordered)' here -- that is the permission, not the"
                        + " mode -- so whatever this arm is controlling for is no longer controlled."
                        + " Most likely the fixture fell under the density crossover. Query: " + sql,
                perKeyOpens > 0
        );
        Assert.assertEquals(
                "this query opened per-key mode but ALSO fell back to the merge on another open,"
                        + " so the arm below is averaging two modes. Query: " + sql,
                0,
                mergedOpens
        );
    }

    protected void createFlights() throws Exception {
        execute("CREATE TABLE flights (flight_ground INT, start_time TIMESTAMP, end_time TIMESTAMP)");
        execute("INSERT INTO flights VALUES (100, '1970-01-01T00:00:02.000000Z', '1970-01-01T00:00:08.000000Z')");
    }

    protected void createTelemetry() throws Exception {
        createTelemetryTable();
        execute("INSERT INTO telemetry SELECT (x * 1000000L)::timestamp," +
                " CASE WHEN x % 4 = 0 THEN 'SFID' WHEN x % 4 = 1 THEN 'HOTMIC'" +
                "      WHEN x % 4 = 2 THEN 'KCAS' ELSE 'CALT' END," +
                " x::double" +
                " FROM long_sequence(10000)");
    }

    /**
     * Same four keys and the same NULL pattern as {@link #createTelemetryWithNulls()}, but
     * 30 s apart instead of 1 s, so the 200 000 rows span ~69 days and land in ~70 daily
     * partitions instead of one.
     * <p>
     * The ROW COUNT is set by the density the per-key gate needs, not by the partition count:
     * 200 000 rows over 4 keys and 70 partitions is ~714 rows per (key, partition) pair. The
     * crossover it has to clear is per-CONSUMER -- {@code PER_KEY_MIN_ROWS_PER_PAIR_BASE} times
     * the passes that consumer makes over each frame -- so the bar differs between the suites
     * built on this fixture: 64 for the order-sensitive arms, which run on the async group by,
     * and 128 for the two-aggregate vectorized query the density suite uses. 714 clears both with
     * room, which is the point of the headroom.
     * <p>
     * It held 10 000 rows -- ~35 per pair, three above the gate -- while the constant was a flat
     * 32. When the crossover was first swept and that became a flat 256, this fixture fell under
     * it, and every suite built on it would have quietly stopped running per-key mode at all
     * while still passing: they asserted the PLAN, which prints the plan-stable permission and is
     * blind to the flip.
     * {@code CoveringIndexPerKeyDensityTest.testSharedMultiPartitionFixtureKeepsPerKey} is the
     * arm that is not, and it is what caught this. The order-sensitive arms over this fixture now
     * assert the mode themselves, through {@link #assertRunsPerKeyMode}.
     * <p>
     * The single-partition fixtures cannot exercise the invariant that per-key acceptance
     * rests on: "per-key mode iterates partitions OUTER, so one key's frames still arrive in
     * ascending partition order, hence frame-sequence order within a key IS timestamp order".
     * With one partition that claim holds vacuously -- those tests would pass identically if
     * per-key emitted a key's partitions in arbitrary order. Use this fixture for anything
     * asserting that first()/last() over per-key frames really is the earliest/latest row.
     * <p>
     * Deliberately a NEW method: the committed tests over {@link #createTelemetry()} and
     * {@link #createTelemetryWithNulls()} pin exact plan text (including a resolved
     * {@code intervals: [...]} line) and exact result rows derived from the 1 s spacing.
     */
    protected void createTelemetryMultiPartition() throws Exception {
        createTelemetryTable();
        execute("INSERT INTO telemetry SELECT (x * 30000000L)::timestamp," +
                " CASE WHEN x % 4 = 0 THEN 'SFID' WHEN x % 4 = 1 THEN 'HOTMIC'" +
                "      WHEN x % 4 = 2 THEN 'KCAS' ELSE 'CALT' END," +
                " CASE WHEN x % 5 = 0 THEN NULL ELSE x::double END" +
                " FROM long_sequence(200000)");
        // Non-vacuity guard: the whole point of this fixture is MANY partitions. If a future
        // edit to the spacing or the row count collapses it back to one, every test built on
        // it silently reverts to proving nothing about cross-partition frame order. The
        // spacing and the row count move TOGETHER -- halving one and doubling the other keeps
        // this count -- so this guard does not also protect the density; that is what the
        // density canary is for.
        final StringSink partitionCount = new StringSink();
        printSql("SELECT count() c FROM table_partitions('telemetry')", partitionCount);
        TestUtils.assertEquals("c\n70\n", partitionCount);
    }

    protected void createTelemetryWithNulls() throws Exception {
        createTelemetryTable();
        execute("INSERT INTO telemetry SELECT (x * 1000000L)::timestamp," +
                " CASE WHEN x % 4 = 0 THEN 'SFID' WHEN x % 4 = 1 THEN 'HOTMIC'" +
                "      WHEN x % 4 = 2 THEN 'KCAS' ELSE 'CALT' END," +
                " CASE WHEN x % 5 = 0 THEN NULL ELSE x::double END" +
                " FROM long_sequence(10000)");
    }

    private void createTelemetryTable() throws Exception {
        execute("CREATE TABLE telemetry (" +
                "  ts TIMESTAMP," +
                "  param_id SYMBOL INDEX TYPE POSTING INCLUDE (ts, value)," +
                "  value DOUBLE" +
                ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
    }
}
