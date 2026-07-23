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
 * {@code now()}). Detection uses {@code RecordCursorFactory.isStableWithinExecution()}.
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
        // indexed symbol source: exercises the index-driven row-cursor stability composition
        execute("CREATE TABLE bi (lo TIMESTAMP, sym SYMBOL INDEX, k INT)");
        execute("INSERT INTO bi VALUES " +
                "('2020-06-02T00:00:00.000000Z', 'X', 1), " +
                "('2020-06-05T00:00:00.000000Z', 'Y', 2)");
    }

    // A deterministic single-row sub-query bound MUST still prune to an interval scan
    // (the PR's headline feature). isStableWithinExecution()==true, so the guard does not fire.
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

    // POSITIVE CONTROL for the set-operation shape: the same UNION ALL ... LIMIT 1 sub-query
    // bound with deterministic aggregates MUST prune. AbstractSetRecordCursorFactory composes
    // isStableWithinExecution() from both inputs, so only the rnd_* source in the negative twin
    // below blocks pruning - proving the twin fails for the right reason.
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

    // A set-operation sub-query bound holding a non-deterministic aggregate MUST NOT prune.
    // AbstractSetRecordCursorFactory composes isStableWithinExecution() from both inputs, so
    // the rnd_* aggregate arms keep this shape out of the pruning path.
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

    // A bind variable is non-deterministic across executions yet stable within one. Wrapping it
    // in an expression (dateadd) must not lose that stability: the wrapper interfaces compose
    // isStableWithinExecution() from their args, so the bound still prunes.
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

    // An rnd_* source hidden inside a nested cursor predicate (between(NCC) over CursorFunctions)
    // MUST NOT prune: CursorFunction stability delegates to the wrapped factory and the wrapper
    // interfaces and the async group-by factory (fused filter included) compose it through.
    @Test
    public void testNestedCursorPredicateRndBoundNotPruned() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= " +
                    "(SELECT max(lo) FROM b WHERE lo BETWEEN " +
                    "(SELECT rnd_timestamp('2020-06-01T00:00:00.000000Z'::timestamp, '2020-06-03T00:00:00.000000Z'::timestamp, 0)) " +
                    "AND (SELECT rnd_timestamp('2020-06-05T00:00:00.000000Z'::timestamp, '2020-06-08T00:00:00.000000Z'::timestamp, 0)))")
                    .assertsPlanNotContaining("Interval forward scan on: t");
        });
    }

    // POSITIVE CONTROL for the group-by-key shape: the same serial keyed group-by (forced by the
    // UNION ALL base) with a deterministic key expression MUST prune. The factory classifies its
    // key functions, so only the rnd_* key in the negative twin below blocks pruning - proving
    // the twin fails for the right reason.
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

    // An rnd_* GROUP BY key under LIMIT 1 keeps scalar cardinality at one while the selected key
    // changes across opens. The serial keyed group-by factory (forced by the UNION ALL base) must
    // classify its key functions, so this MUST NOT prune.
    @Test
    public void testRndGroupByKeyBoundNotPruned() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= " +
                    "(SELECT k FROM (SELECT rnd_timestamp('2020-06-01T00:00:00.000000Z'::timestamp, '2020-06-03T00:00:00.000000Z'::timestamp, 0) k, count() c " +
                    "FROM (SELECT lo FROM b UNION ALL SELECT lo FROM b)) LIMIT 1)")
                    .assertsPlanNotContaining("Interval forward scan on: t");
        });
    }

    // Indexed scalar sub-query bounds.
    // A sub-query bound that resolves through a symbol INDEX scan is stable within the execution
    // when its key (and any residual filter) is stable. Previously PageFrameRecordCursorFactory
    // reported EVERY index-driven cursor unstable, so these prunes were lost to a full outer scan.

    // Fixed-literal indexed symbol lookup: the key is constant, so the bound prunes.
    @Test
    public void testIndexedLiteralSymbolBoundPrunes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= (SELECT lo FROM bi WHERE sym = 'X' LIMIT 1)")
                    .timestamp("ts")
                    // dateadd('h',1,ts) >= 2020-06-02 => ts >= 2020-06-01T23:00, so row 1 is excluded;
                    // the pruned interval scan returns exactly the residual-filter rows (no dropped rows).
                    .withPlanContaining("Interval forward scan on: t")
                    .returns("ts\tv\n" +
                            "2020-06-02T00:00:00.000000Z\t2\n" +
                            "2020-06-03T00:00:00.000000Z\t3\n");
        });
    }

    // Bind-variable indexed symbol lookup: non-deterministic across executions yet stable within
    // one (frozen snapshot), so the deferred index lookup still prunes.
    @Test
    public void testIndexedBindSymbolBoundPrunes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            bindVariableService.clear();
            bindVariableService.setStr(0, "X");
            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= (SELECT lo FROM bi WHERE sym = $1 LIMIT 1)")
                    .assertsPlanContaining("Interval forward scan on: t");
        });
    }

    // Deterministic aggregate over an index-filtered scan: max(lo) over a fixed row set is stable,
    // so the bound prunes even though the aggregate's base is index-driven.
    @Test
    public void testIndexedAggregateBoundPrunes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= (SELECT max(lo) FROM bi WHERE sym = 'X')")
                    .assertsPlanContaining("Interval forward scan on: t");
        });
    }

    // BETWEEN with two indexed literal lookups: both ends are stable, so the range prunes.
    @Test
    public void testIndexedBetweenBoundPrunes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) BETWEEN " +
                    "(SELECT lo FROM bi WHERE sym = 'X' LIMIT 1) AND (SELECT lo FROM bi WHERE sym = 'Y' LIMIT 1)")
                    .assertsPlanContaining("Interval forward scan on: t");
        });
    }

    // Stable residual filter on top of the indexed lookup: both the symbol key and the filter are
    // stable, so the filtered index cursor is stable and the bound prunes.
    @Test
    public void testIndexedFilteredStableBoundPrunes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= (SELECT lo FROM bi WHERE sym = 'X' AND k >= 0 LIMIT 1)")
                    .assertsPlanContaining("Interval forward scan on: t");
        });
    }

    // GUARD: a non-deterministic index KEY (rnd_symbol) makes the selected row vary across opens,
    // so the composition must report unstable and this MUST NOT prune.
    @Test
    public void testIndexedRndSymbolKeyBoundNotPruned() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= " +
                    "(SELECT lo FROM bi WHERE sym = rnd_symbol('X', 'Y') LIMIT 1)")
                    .assertsPlanNotContaining("Interval forward scan on: t");
        });
    }

    // GUARD: a stable index key but a non-deterministic residual FILTER (rnd_*) must also stay
    // unstable - the filter composition is the second half of the AND.
    @Test
    public void testIndexedRndResidualFilterBoundNotPruned() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= " +
                    "(SELECT lo FROM bi WHERE sym = 'X' AND k >= rnd_int(0, 5, 0) LIMIT 1)")
                    .assertsPlanNotContaining("Interval forward scan on: t");
        });
    }
}
