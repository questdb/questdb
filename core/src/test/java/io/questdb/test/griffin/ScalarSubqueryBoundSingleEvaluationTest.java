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

import io.questdb.PropertyKey;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.griffin.engine.functions.test.TestTimestampCounterFactory;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * A monotonic-timestamp predicate whose bound is a table-backed scalar sub-query
 * (e.g. {@code dateadd('h',1,ts) >= (SELECT max(b) FROM bounds)}) is consumed twice within one
 * execution: once by the interval-pruning inverter and once by the retained residual row filter
 * (the inversion of a runtime bound is not proven exact at compile time, so the predicate always
 * stays a residual filter). Before value sharing each side compiled and opened the sub-query
 * independently, and a freshly borrowed reader is refreshed to the latest commit via
 * {@code goActive()} - so a commit to {@code bounds} landing between the two opens could make the
 * pruning bound stricter than the residual and silently drop qualifying rows.
 *
 * <p>{@link io.questdb.griffin.WhereClauseParser} now evaluates each dynamic bound exactly once: the
 * pruning inverter's {@link io.questdb.griffin.engine.functions.ScalarSubQueryTimestampFunction}
 * publishes its single per-execution value into a
 * {@link io.questdb.griffin.model.ScalarTimestampBoundHolder} at partition-frame open, and the
 * residual - re-compiled from the same sub-query node, including per-worker filter clones - reads
 * that frozen value through a
 * {@link io.questdb.griffin.engine.functions.ScalarSubQueryBoundRefFunction} instead of opening the
 * sub-query again. The plan-shape tests below pin that residual reader (it renders as
 * {@code scalar_subquery_bound}) so the residual can no longer diverge from the pruning bound, while
 * the oracle tests confirm the pruned result equals the equivalent literal-bound query.
 */
public class ScalarSubqueryBoundSingleEvaluationTest extends AbstractCairoTest {

    @Override
    @Before
    public void setUp() {
        // enables the test_timestamp_counter() function used to count sub-query executions
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        super.setUp();
    }

    @Test
    public void testBetweenTableBoundsResultMatchesLiteralOracle() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertSqlCursors(
                    "SELECT ts, v FROM t WHERE dateadd('d', 1, ts) BETWEEN '2020-06-01T00:00:00.000000Z' AND '2020-06-02T00:00:00.000000Z'",
                    "SELECT ts, v FROM t WHERE dateadd('d', 1, ts) BETWEEN (SELECT min(b) FROM bounds) AND (SELECT max(b) FROM bounds)"
            );
        });
    }

    // Both BETWEEN bounds prune AND both are read from the shared holder in the residual (rendered
    // twice as scalar_subquery_bound), rather than re-opening the two sub-queries independently.
    @Test
    public void testMonotonicBetweenBoundsShareValueWithResidual() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT ts, v FROM t WHERE dateadd('d', 1, ts) BETWEEN (SELECT min(b) FROM bounds) AND (SELECT max(b) FROM bounds)")
                    .assertsPlanContaining(
                            "between scalar_subquery_bound and scalar_subquery_bound",
                            "Interval forward scan on: t"
                    );
        });
    }

    // The residual filter reads the pruning bound's shared value (scalar_subquery_bound) instead of
    // re-opening the sub-query; the Interval scan proves pruning is still applied.
    @Test
    public void testMonotonicSingleBoundSharesValueWithResidual() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= (SELECT max(b) FROM bounds)")
                    .assertsPlanContaining(
                            "dateadd('h',1,ts)>=scalar_subquery_bound",
                            "Interval forward scan on: t"
                    );
        });
    }

    @Test
    public void testMonotonicWrappedTableBoundResultMatchesLiteralOracle() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // pruning and residual must agree: identical to the same predicate with the literal max
            assertSqlCursors(
                    "SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= '2020-06-02T00:00:00.000000Z'",
                    "SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= (SELECT max(b) FROM bounds)"
            );
        });
    }

    // Counts sub-query evaluations on the parallel stolen-filter top-K path.
    // test_timestamp_counter() increments once per row the sub-query cursor reads, and the scalar
    // sub-query yields exactly one row per open, so the counter equals the number of sub-query
    // opens. "Once" means once per query execution: the pruning bound opens the sub-query at scan
    // open and publishes the frozen value into the shared holder, which the residual filter and
    // all 4 workers read back (scalar_subquery_bound) instead of opening the sub-query themselves.
    // The plan pin proves the parallel top-K + interval-pruning shape actually engages, so the
    // count-of-1 assertion cannot pass vacuously through a serial plan.
    @Test
    public void testParallelTopKOverStolenFilterOpensSubQueryOncePerExecution() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String sql = "SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= " +
                    "(SELECT test_timestamp_counter(max(b)) FROM bounds) ORDER BY v LIMIT 10";
            // the bind variable service makes EXPLAIN render the plain-text (pg wire) plan format
            final BindVariableService bindVariableService = new BindVariableServiceImpl(configuration);
            try (SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(engine, bindVariableService, 4)) {
                assertQuery(sql)
                        .withContext(ctx)
                        .noLeakCheck()
                        .assertsPlanContaining(
                                "Async Top K",
                                "workers: 4",
                                "dateadd('h',1,ts)>=scalar_subquery_bound",
                                "Interval forward scan on: t"
                        );
                try (RecordCursorFactory factory = select(sql, ctx)) {
                    // reset after the compile: the JIT serialization attempt (SqlCodeGenerator.
                    // generateFilter0) opens a page-frame cursor at compile time on JIT-capable
                    // platforms, which evaluates the pruning bound once outside any execution
                    TestTimestampCounterFactory.COUNTER.set(0);
                    try (RecordCursor cursor = factory.getCursor(ctx)) {
                        assertCursor("""
                                ts\tv
                                2020-06-02T00:00:00.000000Z\t2
                                2020-06-03T00:00:00.000000Z\t3
                                """, cursor, factory.getMetadata(), true);
                    }
                    Assert.assertEquals(
                            "the sub-query must open once per execution, not once per worker",
                            1,
                            TestTimestampCounterFactory.COUNTER.get()
                    );

                    // a bound committed between executions: re-executing the same compiled factory
                    // opens the sub-query exactly once more and every side observes the new bound
                    execute("INSERT INTO bounds VALUES ('2020-06-03T00:00:00.000000Z')");
                    try (RecordCursor cursor = factory.getCursor(ctx)) {
                        assertCursor("""
                                ts\tv
                                2020-06-03T00:00:00.000000Z\t3
                                """, cursor, factory.getMetadata(), true);
                    }
                    Assert.assertEquals(
                            "re-execution must open the sub-query exactly once more",
                            2,
                            TestTimestampCounterFactory.COUNTER.get()
                    );
                }
            }
        });
    }

    // Parallel top-K steals the async filter's state and re-compiles per-worker filters from the
    // factory's cloned filter expression (getStealFilterExpr). The clone must carry the QUERY
    // node's ScalarTimestampBoundHolder; if deepClone drops it, each worker re-compiles the
    // sub-query as a fresh cursor comparison - a different function class than the stolen filter
    // (AssertionError under -ea) that would also re-open the sub-query per worker.
    @Test
    public void testParallelTopKOverStolenFilterSharesBoundWithWorkers() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(engine, 4)) {
                assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= (SELECT max(b) FROM bounds) ORDER BY v LIMIT 10")
                        .withContext(ctx)
                        .expectSize()
                        .returns("""
                                ts\tv
                                2020-06-02T00:00:00.000000Z\t2
                                2020-06-03T00:00:00.000000Z\t3
                                """);
            }
        });
    }

    // The counting variant of the worker-recompile shape: the non-thread-safe ~ conjunct forces 4
    // per-worker filter re-compiles from the cloned filter expression, and each re-compile must
    // resolve the sub-query node to the shared-holder reader (scalar_subquery_bound) rather than a
    // fresh sub-query open. test_timestamp_counter() increments once per sub-query open (one row
    // read per open), so a count of 1 proves the pruning bound performed the only open of the
    // execution and none of the worker re-compiles re-opened the sub-query.
    @Test
    public void testParallelTopKWorkerRecompileOpensSubQueryOncePerExecution() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE t2 (ts TIMESTAMP, v INT, s STRING) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t2 VALUES
                    ('2020-06-01T00:00:00.000000Z', 1, 'abc'),
                    ('2020-06-02T00:00:00.000000Z', 2, 'abc'),
                    ('2020-06-03T00:00:00.000000Z', 3, 'xyz')""");
            final String sql = "SELECT ts, v FROM t2 WHERE dateadd('h', 1, ts) >= " +
                    "(SELECT test_timestamp_counter(max(b)) FROM bounds) AND s ~ 'abc' ORDER BY v LIMIT 10";
            // the bind variable service makes EXPLAIN render the plain-text (pg wire) plan format
            final BindVariableService bindVariableService = new BindVariableServiceImpl(configuration);
            try (SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(engine, bindVariableService, 4)) {
                assertQuery(sql)
                        .withContext(ctx)
                        .noLeakCheck()
                        .assertsPlanContaining(
                                "Async Top K",
                                "workers: 4",
                                "scalar_subquery_bound",
                                "Interval forward scan on: t2"
                        );
                try (RecordCursorFactory factory = select(sql, ctx)) {
                    // reset after the compile: the JIT serialization attempt (SqlCodeGenerator.
                    // generateFilter0) opens a page-frame cursor at compile time on JIT-capable
                    // platforms, which evaluates the pruning bound once outside any execution
                    TestTimestampCounterFactory.COUNTER.set(0);
                    try (RecordCursor cursor = factory.getCursor(ctx)) {
                        assertCursor("""
                                ts\tv
                                2020-06-02T00:00:00.000000Z\t2
                                """, cursor, factory.getMetadata(), true);
                    }
                    Assert.assertEquals(
                            "the sub-query must open once per execution despite per-worker filter re-compiles",
                            1,
                            TestTimestampCounterFactory.COUNTER.get()
                    );
                }
            }
        });
    }

    // A non-thread-safe conjunct (constant-pattern ~ keeps a per-instance Matcher) forces
    // compileWorkerFiltersConditionally to re-compile per-worker filters even when the scalar
    // bound reader itself is thread-safe, so the worker re-compile must read the shared holder
    // through the cloned filter expression rather than re-opening the sub-query.
    @Test
    public void testParallelTopKWorkerRecompileReadsSharedBound() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE t2 (ts TIMESTAMP, v INT, s STRING) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t2 VALUES
                    ('2020-06-01T00:00:00.000000Z', 1, 'abc'),
                    ('2020-06-02T00:00:00.000000Z', 2, 'abc'),
                    ('2020-06-03T00:00:00.000000Z', 3, 'xyz')""");
            try (SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(engine, 4)) {
                assertQuery("SELECT ts, v FROM t2 WHERE dateadd('h', 1, ts) >= (SELECT max(b) FROM bounds) AND s ~ 'abc' ORDER BY v LIMIT 10")
                        .withContext(ctx)
                        .expectSize()
                        .returns("""
                                ts\tv
                                2020-06-02T00:00:00.000000Z\t2
                                """);
            }
        });
    }

    private void createTables() throws Exception {
        execute("CREATE TABLE t (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO t VALUES " +
                "('2020-06-01T00:00:00.000000Z', 1), " +
                "('2020-06-02T00:00:00.000000Z', 2), " +
                "('2020-06-03T00:00:00.000000Z', 3)");
        execute("CREATE TABLE bounds (b TIMESTAMP)");
        execute("INSERT INTO bounds VALUES ('2020-06-01T00:00:00.000000Z'), ('2020-06-02T00:00:00.000000Z')");
    }
}
