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
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
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
     * 200 000 rows over 4 keys and 70 partitions is ~714 rows per (key, partition) pair, 2.8x
     * {@code PER_KEY_MIN_ROWS_PER_PAIR}. It held 10 000 rows -- ~35 per pair, three above the
     * gate -- while that constant was 32. When the crossover was swept directly and the
     * constant moved to 256, this fixture fell under it, and every suite built on it would have
     * quietly stopped running per-key mode at all while still passing: they assert the PLAN,
     * which prints the plan-stable permission and is blind to the flip.
     * {@code CoveringIndexPerKeyDensityTest.testSharedMultiPartitionFixtureKeepsPerKey} is the
     * arm that is not, and it is what caught this.
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
