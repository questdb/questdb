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

package io.questdb.test.griffin;

import io.questdb.griffin.engine.functions.ScalarSubQueryTimestampFunction;
import io.questdb.griffin.model.TimestampMonotonicInverter;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Boundary-path regression tests for a monotonic-wrapper timestamp predicate whose bound comes from
 * a scalar subquery, i.e. {@code dateadd('h', 1, ts) <op> (SELECT ...)}. Such a bound flows through
 * {@link ScalarSubQueryTimestampFunction}, which reads the cursor once at scan open and feeds
 * the value to {@link TimestampMonotonicInverter}.
 *
 * <p>The pre-existing NULL / precision suites (CoveringSubqueryBoundReproTest,
 * MonotonicTimestampPruningTest) exercise either a direct raw-cursor bound ({@code ts <op> (SELECT ...)})
 * or a constant / bind-variable monotonic bound. These tests pin the monotonic-wrapper +
 * scalar-subquery combination specifically, for the empty-cursor, one-row-NULL, cross-precision and
 * runtime-constant boundary cases. The strict Long.MAX_VALUE ceiling case for this exact path is
 * already locked by {@code CoveringSubqueryBoundReproTest.testStrictGreaterOverflowBoundPrunesToEmptyInterval}
 * / {@code testStrictLessUnderflowBoundStaysResidual}, so it is not duplicated here.
 */
public class MonotonicSubqueryBoundBoundaryTest extends AbstractCairoTest {

    // A zero-row scalar subquery bound resolves to the LONG_NULL no-rows sentinel. The inverter reads
    // it at scan open and imposes an empty interval, so the monotonic predicate matches no rows -
    // identical to a direct empty-subquery bound and to a constant NULL bound.
    @Test
    public void testMonotonicSubqueryZeroRowCursorReturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00.000000Z', 1), " +
                    "('2024-01-01T01:00:00.000000Z', 2), " +
                    "('2024-01-01T02:00:00.000000Z', 3)");
            execute("CREATE TABLE bounds (b TIMESTAMP)"); // left empty on purpose

            // The empty bound must prune the runtime interval to the empty set (intervals: []), not
            // merely leave a residual full scan that happens to reject every row. The plan assertion
            // pins that collapse so a regression that reopened the whole domain is caught.
            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= (SELECT b FROM bounds)")
                    .timestamp("ts")
                    .withPlanContaining("Interval forward scan on: t", "intervals: []")
                    .returns("ts\tv\n");

            // symmetric strict upper bound
            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) <= (SELECT b FROM bounds)")
                    .timestamp("ts")
                    .withPlanContaining("Interval forward scan on: t", "intervals: []")
                    .returns("ts\tv\n");
        });
    }

    // A one-row scalar subquery that yields a SQL NULL timestamp resolves to the same LONG_NULL
    // sentinel as an empty cursor, so the monotonic predicate again matches no rows.
    @Test
    public void testMonotonicSubqueryOneRowNullReturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00.000000Z', 1), " +
                    "('2024-01-01T01:00:00.000000Z', 2), " +
                    "('2024-01-01T02:00:00.000000Z', 3)");
            execute("CREATE TABLE nb (b TIMESTAMP)");
            execute("INSERT INTO nb VALUES (NULL)");

            // Same empty-interval collapse as the zero-row case: the LONG_NULL bound prunes the
            // interval to intervals: [], not a residual full scan.
            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= (SELECT b FROM nb)")
                    .timestamp("ts")
                    .withPlanContaining("Interval forward scan on: t", "intervals: []")
                    .returns("ts\tv\n");

            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) <= (SELECT b FROM nb)")
                    .timestamp("ts")
                    .withPlanContaining("Interval forward scan on: t", "intervals: []")
                    .returns("ts\tv\n");
        });
    }

    // Designated timestamp in microsecond precision, subquery bound in nanosecond precision. The
    // inverter must convert the bound into the wrapper's output domain (micros) before inverting the
    // transform, so dateadd('h',1,ts) >= 02:00 must invert to ts >= 01:00 regardless of the bound's
    // native precision.
    @Test
    public void testMonotonicSubqueryCrossPrecisionMicrosDesignatedNanosBound() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00.000000Z', 1), " +
                    "('2024-01-01T01:00:00.000000Z', 2), " +
                    "('2024-01-01T02:00:00.000000Z', 3), " +
                    "('2024-01-01T03:00:00.000000Z', 4)");
            execute("CREATE TABLE bounds_ns (b TIMESTAMP_NS)");
            execute("INSERT INTO bounds_ns VALUES ('2024-01-01T02:00:00.000000000Z')");

            String sub = "SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= (SELECT b FROM bounds_ns)";
            String constOracle = "SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= '2024-01-01T02:00:00.000000Z'";
            assertSqlCursors(constOracle, sub);
            assertQuery(sub)
                    .timestamp("ts")
                    .returns("ts\tv\n" +
                            "2024-01-01T01:00:00.000000Z\t2\n" +
                            "2024-01-01T02:00:00.000000Z\t3\n" +
                            "2024-01-01T03:00:00.000000Z\t4\n");
        });
    }

    // Symmetric cross-precision: designated timestamp in nanosecond precision, subquery bound in
    // microsecond precision. The bound must widen into nanos before the inversion.
    @Test
    public void testMonotonicSubqueryCrossPrecisionNanosDesignatedMicrosBound() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tn (ts TIMESTAMP_NS, v INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tn VALUES " +
                    "('2024-01-01T00:00:00.000000000Z', 1), " +
                    "('2024-01-01T01:00:00.000000000Z', 2), " +
                    "('2024-01-01T02:00:00.000000000Z', 3), " +
                    "('2024-01-01T03:00:00.000000000Z', 4)");
            execute("CREATE TABLE bounds_us (b TIMESTAMP)");
            execute("INSERT INTO bounds_us VALUES ('2024-01-01T02:00:00.000000Z')");

            String sub = "SELECT ts, v FROM tn WHERE dateadd('h', 1, ts) >= (SELECT b FROM bounds_us)";
            String constOracle = "SELECT ts, v FROM tn WHERE dateadd('h', 1, ts) >= '2024-01-01T02:00:00.000000000Z'";
            assertSqlCursors(constOracle, sub);
            assertQuery(sub)
                    .timestamp("ts")
                    .returns("ts\tv\n" +
                            "2024-01-01T01:00:00.000000000Z\t2\n" +
                            "2024-01-01T02:00:00.000000000Z\t3\n" +
                            "2024-01-01T03:00:00.000000000Z\t4\n");
        });
    }

    // A runtime-constant scalar-subquery bound (bind variable, now(), now_ns()) re-reads the same
    // execution-scoped snapshot on both cursor opens - once for the pruning inverter and once for
    // the retained residual filter - so it is stable within the execution and must PRUNE: the plan
    // has to show an interval scan with the inverted bound, not a full frame scan. The result
    // assertions additionally prove the pruned interval keeps exactly the matching rows. now() and
    // now_ns() are pinned via the test clock so both the interval and the result are deterministic.
    @Test
    public void testMonotonicSubqueryRuntimeConstantBoundPrunesSafely() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00.000000Z', 1), " +
                    "('2024-01-01T01:00:00.000000Z', 2), " +
                    "('2024-01-01T02:00:00.000000Z', 3), " +
                    "('2024-01-01T03:00:00.000000Z', 4)");

            // bind variable bound (runtime constant): 2024-01-01T02:00:00Z inverts to ts >= 01:00
            bindVariableService.clear();
            bindVariableService.setTimestamp(0, 1_704_074_400_000_000L);
            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= (SELECT $1::timestamp)")
                    .timestamp("ts")
                    .withPlanContaining(
                            "Interval forward scan on: t",
                            "intervals: [(\"2024-01-01T01:00:00.000000Z\"")
                    .returns("ts\tv\n" +
                            "2024-01-01T01:00:00.000000Z\t2\n" +
                            "2024-01-01T02:00:00.000000Z\t3\n" +
                            "2024-01-01T03:00:00.000000Z\t4\n");

            final long savedMicros = currentMicros;
            try {
                // pin the clock: now() = 2024-01-01T03:00:00Z, inverts to ts >= 02:00
                setCurrentMicros(1_704_078_000_000_000L);
                assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= (SELECT now())")
                        .timestamp("ts")
                        .withPlanContaining(
                                "Interval forward scan on: t",
                                "intervals: [(\"2024-01-01T02:00:00.000000Z\"")
                        .returns("ts\tv\n" +
                                "2024-01-01T02:00:00.000000Z\t3\n" +
                                "2024-01-01T03:00:00.000000Z\t4\n");

                // same instant in nanos: the TIMESTAMP_NS bound converts into the micro output
                // domain before the inversion, then prunes identically
                assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= (SELECT now_ns())")
                        .timestamp("ts")
                        .withPlanContaining(
                                "Interval forward scan on: t",
                                "intervals: [(\"2024-01-01T02:00:00.000000Z\"")
                        .returns("ts\tv\n" +
                                "2024-01-01T02:00:00.000000Z\t3\n" +
                                "2024-01-01T03:00:00.000000Z\t4\n");
            } finally {
                setCurrentMicros(savedMicros);
            }
        });
    }

    // A genuinely traversal-unstable scalar-subquery bound - rnd_timestamp() re-samples on every
    // cursor open - must NOT prune: the two compiled copies of the sub-query could disagree and
    // pruning would drop rows the residual filter keeps. The plan has to retain the full frame
    // scan with the predicate as a residual filter and no interval scan may appear. The random
    // range lies strictly below every row, so the predicate is always true and the result is
    // deterministic (all rows) even though the bound value is random.
    @Test
    public void testMonotonicSubqueryUnstableBoundStaysResidualOnly() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00.000000Z', 1), " +
                    "('2024-01-01T01:00:00.000000Z', 2), " +
                    "('2024-01-01T02:00:00.000000Z', 3), " +
                    "('2024-01-01T03:00:00.000000Z', 4)");

            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= " +
                    "(SELECT rnd_timestamp('2020-01-01T00:00:00.000000Z'::timestamp, '2020-01-02T00:00:00.000000Z'::timestamp, 0))")
                    .timestamp("ts")
                    .withPlanContaining("Frame forward scan on: t")
                    .withPlanNotContaining("Interval forward scan")
                    .returns("ts\tv\n" +
                            "2024-01-01T00:00:00.000000Z\t1\n" +
                            "2024-01-01T01:00:00.000000Z\t2\n" +
                            "2024-01-01T02:00:00.000000Z\t3\n" +
                            "2024-01-01T03:00:00.000000Z\t4\n");
        });
    }
}
