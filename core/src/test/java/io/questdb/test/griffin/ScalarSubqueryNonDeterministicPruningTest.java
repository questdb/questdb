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
 * (e.g. {@code dateadd('h',1,ts) >= (select ...)}) is used twice: by the interval-pruning inverter and
 * by the retained residual filter. The pruning bound evaluates the sub-query once per execution and
 * publishes its value, which the residual reads, so every sub-query bound prunes, including a
 * NON-deterministic one (its projection evaluates {@code rnd_*} / {@code systimestamp()}): both uses
 * see the same value. Bounds over bind variables and {@code now()} prune as before.
 */
public class ScalarSubqueryNonDeterministicPruningTest extends AbstractCairoTest {

    // Rows that EVERY indexed scalar sub-query bound must return once it prunes. The bound
    // resolves to 2020-06-02T00:00 (bi.lo for 'X'), and dateadd('h',1,ts) >= that instant means
    // ts >= 2020-06-01T23:00, so row 1 is excluded and rows 2-3 survive.
    //
    // Pinning the ROWS - not just the plan fragment - is what catches a prune that lands on the
    // WRONG interval: "Interval forward scan on: t" renders for any non-empty interval, so an
    // off-by-one-microsecond bound, a bad cross-precision widen, or a stale holder value would
    // silently truncate the result set while a plan-only assertion stayed green.
    private static final String INDEXED_BOUND_EXPECTED = "ts\tv\n" +
            "2020-06-02T00:00:00.000000Z\t2\n" +
            "2020-06-03T00:00:00.000000Z\t3\n";

    private void createTables() throws Exception {
        execute("CREATE TABLE t (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO t VALUES " +
                "('2020-06-01T00:00:00.000000Z', 1), " +
                "('2020-06-02T00:00:00.000000Z', 2), " +
                "('2020-06-03T00:00:00.000000Z', 3)");
        execute("CREATE TABLE b (lo TIMESTAMP)");
        execute("INSERT INTO b VALUES ('2020-06-02T00:00:00.000000Z')");
        // indexed symbol source: exercises index-driven sub-query bounds
        execute("CREATE TABLE bi (lo TIMESTAMP, sym SYMBOL INDEX, k INT)");
        execute("INSERT INTO bi VALUES " +
                "('2020-06-02T00:00:00.000000Z', 'X', 1), " +
                "('2020-06-05T00:00:00.000000Z', 'Y', 2)");
    }

    // A deterministic single-row sub-query bound prunes to an interval scan.
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

    @Test
    public void testRndSubqueryBoundPrunes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= " +
                    "(SELECT rnd_timestamp('2020-06-01T00:00:00.000000Z'::timestamp, '2020-06-03T00:00:00.000000Z'::timestamp, 0))")
                    .assertsPlanContaining("Interval forward scan on: t");
        });
    }

    @Test
    public void testNonDeterministicAggregateSubqueryBoundPrunes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= " +
                    "(SELECT max(rnd_timestamp('2020-06-01T00:00:00.000000Z'::timestamp, '2020-06-03T00:00:00.000000Z'::timestamp, 0)) FROM long_sequence(5))")
                    .assertsPlanContaining("Interval forward scan on: t");
        });
    }

    @Test
    public void testNonDeterministicAggregateOverTableSubqueryBoundPrunes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= " +
                    "(SELECT max(rnd_timestamp('2020-06-01T00:00:00.000000Z'::timestamp, '2020-06-03T00:00:00.000000Z'::timestamp, 0)) FROM b)")
                    .assertsPlanContaining("Interval forward scan on: t");
        });
    }

    // A UNION ALL ... LIMIT 1 sub-query bound with deterministic aggregates prunes.
    @Test
    public void testDeterministicUnionSubqueryBoundStillPrunes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= " +
                    "(SELECT max(lo) FROM b " +
                    "UNION ALL " +
                    "SELECT max(lo) FROM b " +
                    "LIMIT 1)")
                    .assertsPlanContaining("Interval forward scan on: t");
        });
    }

    @Test
    public void testNonDeterministicUnionSubqueryBoundPrunes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= " +
                    "(SELECT max(rnd_timestamp('2020-06-01T00:00:00.000000Z'::timestamp, '2020-06-03T00:00:00.000000Z'::timestamp, 0)) FROM long_sequence(5) " +
                    "UNION ALL " +
                    "SELECT max(rnd_timestamp('2020-06-01T00:00:00.000000Z'::timestamp, '2020-06-03T00:00:00.000000Z'::timestamp, 0)) FROM long_sequence(5) " +
                    "LIMIT 1)")
                    .assertsPlanContaining("Interval forward scan on: t");
        });
    }

    @Test
    public void testSystimestampSubqueryBoundPrunes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= (SELECT systimestamp())")
                    .assertsPlanContaining("Interval forward scan on: t");
        });
    }

    @Test
    public void testBetweenRndSubqueryBoundPrunes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) BETWEEN " +
                    "(SELECT rnd_timestamp('2020-06-01T00:00:00.000000Z'::timestamp, '2020-06-03T00:00:00.000000Z'::timestamp, 0)) " +
                    "AND '2020-06-03T00:00:00.000000Z'")
                    .assertsPlanContaining("Interval forward scan on: t");
        });
    }

    @Test
    public void testLessThanRndSubqueryBoundPrunes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) <= " +
                    "(SELECT rnd_timestamp('2020-06-01T00:00:00.000000Z'::timestamp, '2020-06-03T00:00:00.000000Z'::timestamp, 0))")
                    .assertsPlanContaining("Interval forward scan on: t");
        });
    }

    // A bind variable is non-deterministic across executions yet stable within one, and so is an
    // expression over it, so the bound prunes.
    @Test
    public void testExpressionWrappedBindVariableBoundStillPrunes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            bindVariableService.clear();
            // $1 = 2020-06-02T01:00:00Z; dateadd('h',-1,$1) = 2020-06-02T00:00:00Z
            bindVariableService.setTimestamp(0, 1_591_059_600_000_000L);
            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= (SELECT dateadd('h', -1, $1::timestamp))")
                    .timestamp("ts")
                    .withPlanContaining("Interval forward scan on: t")
                    .returns("ts\tv\n" +
                            "2020-06-02T00:00:00.000000Z\t2\n" +
                            "2020-06-03T00:00:00.000000Z\t3\n");
        });
    }

    // now() is frozen per execution, so an expression over it is stable within the execution
    // and must prune, exactly like a wrapped bind variable.
    @Test
    public void testExpressionWrappedNowBoundStillPrunes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) <= (SELECT dateadd('h', 1, now()))")
                    .assertsPlanContaining("Interval forward scan on: t");
        });
    }

    @Test
    public void testNestedCursorPredicateRndBoundPrunes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= " +
                    "(SELECT max(lo) FROM b WHERE lo BETWEEN " +
                    "(SELECT rnd_timestamp('2020-06-01T00:00:00.000000Z'::timestamp, '2020-06-03T00:00:00.000000Z'::timestamp, 0)) " +
                    "AND (SELECT rnd_timestamp('2020-06-05T00:00:00.000000Z'::timestamp, '2020-06-08T00:00:00.000000Z'::timestamp, 0)))")
                    .assertsPlanContaining("Interval forward scan on: t");
        });
    }

    // A serial keyed group-by (forced by the UNION ALL base) with a deterministic key expression
    // prunes.
    @Test
    public void testDeterministicGroupByKeyBoundStillPrunes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= " +
                    "(SELECT k FROM (SELECT dateadd('h', 0, lo) k, count() c " +
                    "FROM (SELECT lo FROM b UNION ALL SELECT lo FROM b)) LIMIT 1)")
                    .assertsPlanContaining("Interval forward scan on: t");
        });
    }

    @Test
    public void testRndGroupByKeyBoundPrunes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= " +
                    "(SELECT k FROM (SELECT rnd_timestamp('2020-06-01T00:00:00.000000Z'::timestamp, '2020-06-03T00:00:00.000000Z'::timestamp, 0) k, count() c " +
                    "FROM (SELECT lo FROM b UNION ALL SELECT lo FROM b)) LIMIT 1)")
                    .assertsPlanContaining("Interval forward scan on: t");
        });
    }

    // Indexed scalar sub-query bounds.

    // Fixed-literal indexed symbol lookup: the key is constant, so the bound prunes.
    @Test
    public void testIndexedLiteralSymbolBoundPrunes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= (SELECT lo FROM bi WHERE sym = 'X' LIMIT 1)")
                    .timestamp("ts")
                    // Constant-key baseline: the pruned interval scan returns exactly the
                    // residual-filter rows (no dropped rows).
                    .withPlanContaining("Interval forward scan on: t")
                    .returns(INDEXED_BOUND_EXPECTED);
        });
    }

    // Bind-variable indexed symbol lookup: the deferred index lookup prunes.
    @Test
    public void testIndexedBindSymbolBoundPrunes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            bindVariableService.clear();
            bindVariableService.setStr(0, "X");
            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= (SELECT lo FROM bi WHERE sym = $1 LIMIT 1)")
                    .timestamp("ts")
                    // Same rows as the constant-key baseline: the deferred bind-variable lookup
                    // must prune to the SAME interval, not merely to some interval.
                    .withPlanContaining("Interval forward scan on: t")
                    .returns(INDEXED_BOUND_EXPECTED);
        });
    }

    // Deterministic aggregate over an index-filtered scan prunes.
    @Test
    public void testIndexedAggregateBoundPrunes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= (SELECT max(lo) FROM bi WHERE sym = 'X')")
                    .timestamp("ts")
                    // max(lo) over the 'X' rows is 2020-06-02T00:00, so the aggregate bound must
                    // prune to the same interval as the direct lookup.
                    .withPlanContaining("Interval forward scan on: t")
                    .returns(INDEXED_BOUND_EXPECTED);
        });
    }

    // BETWEEN with two indexed literal lookups prunes on both ends.
    @Test
    public void testIndexedBetweenBoundPrunes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) BETWEEN " +
                    "(SELECT lo FROM bi WHERE sym = 'X' LIMIT 1) AND (SELECT lo FROM bi WHERE sym = 'Y' LIMIT 1)")
                    .timestamp("ts")
                    // Two-sided: ts+1h in [2020-06-02, 2020-06-05] => ts in
                    // [2020-06-01T23:00, 2020-06-04T23:00]. Both ends must land, so a widened or
                    // dropped upper bound is caught by the rows even though the plan is unchanged.
                    .withPlanContaining("Interval forward scan on: t")
                    .returns(INDEXED_BOUND_EXPECTED);
        });
    }

    // A residual filter on top of the indexed lookup prunes to the same interval.
    @Test
    public void testIndexedFilteredStableBoundPrunes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= (SELECT lo FROM bi WHERE sym = 'X' AND k >= 0 LIMIT 1)")
                    .timestamp("ts")
                    // The residual k >= 0 keeps the same row, so the filtered index cursor must
                    // prune to the same interval as the unfiltered lookup.
                    .withPlanContaining("Interval forward scan on: t")
                    .returns(INDEXED_BOUND_EXPECTED);
        });
    }

    @Test
    public void testIndexedRndSymbolKeyBoundPrunes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= " +
                    "(SELECT lo FROM bi WHERE sym = rnd_symbol('X', 'Y') LIMIT 1)")
                    .assertsPlanContaining("Interval forward scan on: t");
        });
    }

    @Test
    public void testIndexedRndResidualFilterBoundPrunes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= " +
                    "(SELECT lo FROM bi WHERE sym = 'X' AND k >= rnd_int(0, 5, 0) LIMIT 1)")
                    .assertsPlanContaining("Interval forward scan on: t");
        });
    }
}
