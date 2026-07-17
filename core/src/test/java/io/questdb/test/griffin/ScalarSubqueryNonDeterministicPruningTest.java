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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * A monotonic-wrapper timestamp predicate whose bound is a scalar sub-query
 * (e.g. {@code dateadd('h',1,ts) >= (select ...)}) is compiled twice: once for the interval-pruning
 * inverter and once for the retained residual filter. When that sub-query is NON-deterministic
 * (its projection evaluates {@code rnd_*} / {@code systimestamp()}), the two independent cursor opens
 * can yield different bounds and the pruning inverter can drop rows the residual filter would keep.
 *
 * <p>{@link io.questdb.griffin.WhereClauseParser#analyzeMonotonicTimestamp} therefore skips interval
 * pruning (residual-only) for a non-deterministic {@code ScalarSubQueryTimestampFunction} bound while
 * still pruning for deterministic sub-query bounds and for runtime-constant bounds (bind variables,
 * {@code now()}). Detection uses {@code RecordCursorFactory.isNonDeterministic()}.
 */
public class ScalarSubqueryNonDeterministicPruningTest extends AbstractCairoTest {

    private void createTables() throws Exception {
        execute("CREATE TABLE t (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO t VALUES " +
                "('2020-06-01T00:00:00.000000Z', 1), " +
                "('2020-06-02T00:00:00.000000Z', 2), " +
                "('2020-06-03T00:00:00.000000Z', 3)");
        execute("CREATE TABLE b (lo TIMESTAMP)");
        execute("INSERT INTO b VALUES ('2020-06-02T00:00:00.000000Z')");
    }

    // A deterministic single-row sub-query bound MUST still prune to an interval scan
    // (the PR's headline feature). isNonDeterministic()==false, so the guard does not fire.
    @Test
    public void testDeterministicSubqueryBoundStillPrunes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= (SELECT lo FROM b)")
                    .assertsPlanContaining("Interval forward scan on: t");
        });
    }

    // A deterministic aggregate sub-query bound MUST still prune to an interval scan.
    @Test
    public void testDeterministicAggregateSubqueryBoundStillPrunes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= (SELECT max(lo) FROM b)")
                    .assertsPlanContaining("Interval forward scan on: t");
        });
    }

    // A non-deterministic rnd_* sub-query bound MUST NOT prune: the predicate stays a residual
    // filter (no interval scan), so the single residual evaluation is the source of truth.
    @Test
    public void testRndSubqueryBoundNotPruned() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= " +
                    "(SELECT rnd_timestamp('2020-06-01T00:00:00.000000Z'::timestamp, '2020-06-03T00:00:00.000000Z'::timestamp, 0))")
                    .assertsPlanNotContaining("Interval forward scan on: t");
        });
    }

    // A NON-deterministic AGGREGATE sub-query bound (rnd_* inside max()) MUST NOT prune: the
    // non-determinism lives in the aggregate function held by the group-by factory, not in a
    // projection, filter or base factory. Two independent opens draw different bounds, so pruning
    // with one draw while filtering with another silently drops rows.
    @Test
    public void testNonDeterministicAggregateSubqueryBoundNotPruned() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= " +
                    "(SELECT max(rnd_timestamp('2020-06-01T00:00:00.000000Z'::timestamp, '2020-06-03T00:00:00.000000Z'::timestamp, 0)) FROM long_sequence(5))")
                    .assertsPlanNotContaining("Interval forward scan on: t");
        });
    }

    // Same shape over a table base (group-by factory over a page-frame scan) MUST NOT prune.
    @Test
    public void testNonDeterministicAggregateOverTableSubqueryBoundNotPruned() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= " +
                    "(SELECT max(rnd_timestamp('2020-06-01T00:00:00.000000Z'::timestamp, '2020-06-03T00:00:00.000000Z'::timestamp, 0)) FROM b)")
                    .assertsPlanNotContaining("Interval forward scan on: t");
        });
    }

    // A set-operation sub-query bound holding a non-deterministic aggregate MUST NOT prune.
    // AbstractSetRecordCursorFactory exposes neither a filter nor a base factory, so only a
    // fail-safe (prove-determinism) contract keeps this shape out of the pruning path.
    @Test
    public void testNonDeterministicUnionSubqueryBoundNotPruned() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= " +
                    "(SELECT max(rnd_timestamp('2020-06-01T00:00:00.000000Z'::timestamp, '2020-06-03T00:00:00.000000Z'::timestamp, 0)) FROM long_sequence(5) " +
                    "UNION ALL " +
                    "SELECT max(rnd_timestamp('2020-06-01T00:00:00.000000Z'::timestamp, '2020-06-03T00:00:00.000000Z'::timestamp, 0)) FROM long_sequence(5) " +
                    "LIMIT 1)")
                    .assertsPlanNotContaining("Interval forward scan on: t");
        });
    }

    // A non-deterministic systimestamp() sub-query bound MUST NOT prune.
    @Test
    public void testSystimestampSubqueryBoundNotPruned() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= (SELECT systimestamp())")
                    .assertsPlanNotContaining("Interval forward scan on: t");
        });
    }

    // BETWEEN with a non-deterministic sub-query bound MUST NOT prune (symmetric coverage).
    @Test
    public void testBetweenRndSubqueryBoundNotPruned() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) BETWEEN " +
                    "(SELECT rnd_timestamp('2020-06-01T00:00:00.000000Z'::timestamp, '2020-06-03T00:00:00.000000Z'::timestamp, 0)) " +
                    "AND '2020-06-03T00:00:00.000000Z'")
                    .assertsPlanNotContaining("Interval forward scan on: t");
        });
    }

    // Strict less-than (<=) with a non-deterministic sub-query bound MUST NOT prune (symmetric coverage).
    @Test
    public void testLessThanRndSubqueryBoundNotPruned() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) <= " +
                    "(SELECT rnd_timestamp('2020-06-01T00:00:00.000000Z'::timestamp, '2020-06-03T00:00:00.000000Z'::timestamp, 0))")
                    .assertsPlanNotContaining("Interval forward scan on: t");
        });
    }
}
