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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.griffin.engine.functions.test.TestTimestampCounterFactory;
import io.questdb.griffin.engine.functions.test.TestTimestampDriftFactory;
import io.questdb.mp.WorkerPool;
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
    // one day in TIMESTAMP (micro) units - large enough that a drifted bound moves across the
    // daily row spacing of table t, so a divergent bound changes the result set rather than
    // silently producing the same rows
    private static final long DRIFT_STEP_MICROS = 86_400_000_000L;
    // rows per daily partition at the 60s spacing used by createWideTables()
    private static final int ROWS_PER_PARTITION = 1440;
    // Number of daily partitions for the worker-pool tests, chosen from a measured sweep of how
    // often the async filter actually dispatches a residual read to a pool worker (100 executions
    // each, 4-worker pool): 1 partition => 100/100 runs stayed entirely on the requesting thread,
    // 10 => 0/100, 40 => 1/100. Below ~5 partitions the "parallel" tests quietly degrade into
    // single-threaded ones.
    private static final int WIDE_PARTITIONS = 10;

    @Override
    @Before
    public void setUp() {
        // enables the test_timestamp_counter()/test_timestamp_drift() functions used to count and
        // perturb sub-query evaluations
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        super.setUp();
        TestTimestampDriftFactory.OPENS.set(0);
        TestTimestampDriftFactory.STEP.set(DRIFT_STEP_MICROS);
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

    // End-to-end result guard for the stolen-filter top-K shape. It catches a bound that is never
    // published (the residual then reads an unpublished holder and trips the assert), but it does
    // NOT verify holder propagation through deepClone: dropping the holder on clone makes each
    // worker re-open the sub-query against the same static bound table, producing the same rows.
    // That mutation is caught by testParallelTopKWorkerRecompileOpensSubQueryOncePerExecution via
    // the open-counter, and by ExpressionNodeTest.testDeepCloneAndCopyFromCarryScalarBoundHolder.
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
    // bound reader itself is thread-safe. This is the result-shape guard for that path; as with
    // the stolen-filter test above it cannot detect a per-worker re-open on its own, because the
    // re-opened sub-query returns the same value from a static bound table. The counting sibling
    // (testParallelTopKWorkerRecompileOpensSubQueryOncePerExecution) is what pins the re-open.
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

    // ------------------------------------------------------------------------------------------
    // Value-level coverage.
    //
    // The tests above pin the residual's plan shape and the number of sub-query opens. Neither can
    // fail on a wrong *value*: `bounds` is static for the duration of a single assertion, so the
    // pruning bound and an independently re-opened residual would compute the same max(b) anyway.
    // test_timestamp_drift() closes that gap by returning a strictly later bound on every open,
    // turning the production race this PR exists to eliminate - a commit landing between the
    // pruning open and the residual read - into a deterministic outcome. Evaluate the bound once
    // and share it, and the result equals a literal-bound oracle; re-open it and the residual
    // bound is a day later, silently dropping a qualifying row.
    // ------------------------------------------------------------------------------------------

    // Drifts the LO bound only: a re-opened lo is strictly later than the pruned one, so the
    // residual rejects a row the interval scan admitted - the row-dropping direction. Drifting the
    // hi bound instead only widens the residual, and pruning has already bounded the scan on the
    // frozen hi, so a divergent hi cannot lose rows and only the open-counter would notice.
    @Test
    public void testBetweenLoBoundValueFrozenAgainstDriftingSubQuery() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertDriftingQueryMatchesOracle(
                    "SELECT ts, v FROM t WHERE dateadd('h', 1, ts) BETWEEN '2020-06-01T00:00:00.000000Z' AND '2020-06-02T00:00:00.000000Z'",
                    "SELECT ts, v FROM t WHERE dateadd('h', 1, ts) BETWEEN (SELECT test_timestamp_drift(min(b)) FROM bounds) AND (SELECT max(b) FROM bounds)"
            );
        });
    }

    // The frozen bound is the first evaluation - max(b) drifted by zero steps. A second open would
    // return max(b) + 1 day, pushing the residual past the 2020-06-02 row.
    @Test
    public void testMonotonicBoundValueFrozenAgainstDriftingSubQuery() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertDriftingQueryMatchesOracle(
                    "SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= '2020-06-02T00:00:00.000000Z'",
                    "SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= (SELECT test_timestamp_drift(max(b)) FROM bounds)"
            );
        });
    }

    // Both dimensions at once: the drifting sub-query makes any divergence between the published
    // and the read value change the result set, and the wide table gives the async filter enough
    // page frames for the pool's workers to consume the reduce queue, so residual reads land on
    // threads other than the publisher and exercise the volatile publication edge that
    // ScalarTimestampBoundHolder documents.
    //
    // Cross-thread execution is a scheduling race, not a guarantee - which is exactly why the
    // table is sized the way it is. Measured on the reference machine over 100 executions:
    // 1 partition => 100/100 runs read the bound only on the requesting thread (no worker ever
    // participates); 10 partitions => 0/100; 40 partitions => 1/100. WIDE_PARTITIONS therefore
    // sits at 10. The correctness assertion below holds regardless of which thread reads, so a
    // run that happens not to dispatch degrades to the single-threaded case rather than passing
    // vacuously - but at this width it will normally exercise the worker path.
    @Test
    public void testMonotonicBoundValueFrozenUnderRealWorkerPool() throws Exception {
        assertMemoryLeak(() -> {
            try (WorkerPool pool = new WorkerPool(() -> 4)) {
                TestUtils.execute(pool, (engine, compiler, executionContext) -> {
                    createWideTables(engine, executionContext);
                    final String oracleSql = "SELECT ts, v FROM wt WHERE dateadd('h', 1, ts) >= '2020-06-01T00:00:00.000000Z'";
                    final String driftingSql = "SELECT ts, v FROM wt WHERE dateadd('h', 1, ts) >= (SELECT test_timestamp_drift(max(b)) FROM wbounds)";
                    try (
                            RecordCursorFactory oracle = compiler.compile(oracleSql, executionContext).getRecordCursorFactory();
                            RecordCursorFactory drifting = compiler.compile(driftingSql, executionContext).getRecordCursorFactory()
                    ) {
                        // reset after both compiles: a JIT serialization attempt opens a page-frame
                        // cursor at compile time and evaluates the bound outside any execution
                        TestTimestampDriftFactory.OPENS.set(0);
                        try (
                                RecordCursor expected = oracle.getCursor(executionContext);
                                RecordCursor actual = drifting.getCursor(executionContext)
                        ) {
                            TestUtils.assertEquals(expected, oracle.getMetadata(), actual, drifting.getMetadata(), false);
                        }
                        Assert.assertEquals(
                                "the drifting bound must be evaluated exactly once across all workers",
                                1,
                                TestTimestampDriftFactory.OPENS.get()
                        );
                    }
                }, configuration, LOG);
            }
        });
    }

    // The BETWEEN form of the same guarantee under a real pool. Drifts the LO bound, which is the
    // row-dropping direction (see testBetweenLoBoundValueFrozenAgainstDriftingSubQuery); a plain
    // literal-vs-sub-query oracle here would be vacuous, because with a static bound table both
    // sides compute the same value whether or not they share one.
    @Test
    public void testBetweenBoundValueFrozenUnderRealWorkerPool() throws Exception {
        assertMemoryLeak(() -> {
            try (WorkerPool pool = new WorkerPool(() -> 4)) {
                TestUtils.execute(pool, (engine, compiler, executionContext) -> {
                    createWideTables(engine, executionContext);
                    final String oracleSql = "SELECT ts, v FROM wt WHERE dateadd('h', 1, ts) BETWEEN '2020-06-01T00:00:00.000000Z' AND '2020-06-06T00:00:00.000000Z'";
                    final String driftingSql = "SELECT ts, v FROM wt WHERE dateadd('h', 1, ts) BETWEEN (SELECT test_timestamp_drift(max(b)) FROM wbounds) AND '2020-06-06T00:00:00.000000Z'";
                    try (
                            RecordCursorFactory oracle = compiler.compile(oracleSql, executionContext).getRecordCursorFactory();
                            RecordCursorFactory drifting = compiler.compile(driftingSql, executionContext).getRecordCursorFactory()
                    ) {
                        TestTimestampDriftFactory.OPENS.set(0);
                        try (
                                RecordCursor expected = oracle.getCursor(executionContext);
                                RecordCursor actual = drifting.getCursor(executionContext)
                        ) {
                            TestUtils.assertEquals(expected, oracle.getMetadata(), actual, drifting.getMetadata(), false);
                        }
                        Assert.assertEquals(
                                "the drifting bound must be evaluated exactly once across all workers",
                                1,
                                TestTimestampDriftFactory.OPENS.get()
                        );
                    }
                }, configuration, LOG);
            }
        });
    }

    // Retained as an end-to-end result guard on the BETWEEN pruning path. Note what it does NOT
    // prove: with a static bound table it cannot fail on a divergent value, so it survives the
    // removal of the whole sharing mechanism. Value-level coverage lives in the drift tests.
    @Test
    public void testBetweenBoundMatchesOracleUnderRealWorkerPool() throws Exception {
        assertMemoryLeak(() -> {
            try (WorkerPool pool = new WorkerPool(() -> 4)) {
                TestUtils.execute(pool, (engine, _, executionContext) -> {
                    createTables(engine, executionContext);
                    TestUtils.assertSqlCursors(
                            engine,
                            executionContext,
                            "SELECT ts, v FROM t WHERE dateadd('d', 1, ts) BETWEEN '2020-06-01T00:00:00.000000Z' AND '2020-06-02T00:00:00.000000Z'",
                            "SELECT ts, v FROM t WHERE dateadd('d', 1, ts) BETWEEN (SELECT min(b) FROM bounds) AND (SELECT max(b) FROM bounds)",
                            LOG
                    );
                }, configuration, LOG);
            }
        });
    }

    // The holder is reused across executions of a cached factory, so it must be overwritten on
    // every run - the "stale value on a reused cached factory" failure mode.
    //
    // The commit deliberately moves the bound BACKWARD (an earlier min), because that is the only
    // direction that loses rows: a holder that fails to re-freeze leaves the residual reading the
    // previous, stricter bound while pruning admits the wider interval, so the residual rejects
    // rows the scan returned. Committing a *later* bound instead is masked - pruning has already
    // narrowed the scan to the fresh value, so a stale, looser residual cannot change the result
    // and the assertion would pass with the holder permanently stuck on its first value.
    @Test
    public void testReusedFactoryRefreezesBoundAfterCommit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "('2020-06-01T00:00:00.000000Z', 1), " +
                    "('2020-06-02T00:00:00.000000Z', 2), " +
                    "('2020-06-03T00:00:00.000000Z', 3)");
            execute("CREATE TABLE bounds (b TIMESTAMP)");
            execute("INSERT INTO bounds VALUES ('2020-06-02T00:00:00.000000Z'), ('2020-06-03T00:00:00.000000Z')");
            final String sql = "SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= (SELECT min(b) FROM bounds)";
            try (RecordCursorFactory factory = select(sql, sqlExecutionContext)) {
                assertFactoryMatchesOracle(factory, "SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= '2020-06-02T00:00:00.000000Z'");
                execute("INSERT INTO bounds VALUES ('2020-06-01T00:00:00.000000Z')");
                assertFactoryMatchesOracle(factory, "SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= '2020-06-01T00:00:00.000000Z'");
            }
        });
    }

    private static void assertDriftingQueryMatchesOracle(String oracleSql, String driftingSql) throws Exception {
        try (
                RecordCursorFactory oracle = select(oracleSql, sqlExecutionContext);
                RecordCursorFactory drifting = select(driftingSql, sqlExecutionContext)
        ) {
            // reset after both compiles: a JIT serialization attempt opens a page-frame cursor at
            // compile time and evaluates the bound outside any execution
            TestTimestampDriftFactory.OPENS.set(0);
            try (
                    RecordCursor expected = oracle.getCursor(sqlExecutionContext);
                    RecordCursor actual = drifting.getCursor(sqlExecutionContext)
            ) {
                TestUtils.assertEquals(expected, oracle.getMetadata(), actual, drifting.getMetadata(), false);
            }
            Assert.assertEquals(
                    "the drifting sub-query bound must be evaluated exactly once per execution",
                    1,
                    TestTimestampDriftFactory.OPENS.get()
            );
        }
    }

    private static void assertFactoryMatchesOracle(RecordCursorFactory factory, String oracleSql) throws Exception {
        try (
                RecordCursorFactory oracle = select(oracleSql, sqlExecutionContext);
                RecordCursor expected = oracle.getCursor(sqlExecutionContext);
                RecordCursor actual = factory.getCursor(sqlExecutionContext)
        ) {
            TestUtils.assertEquals(expected, oracle.getMetadata(), actual, factory.getMetadata(), false);
        }
    }

    // A table wide enough that the async filter splits into enough page frames for the pool's
    // workers to pick up reduce tasks. See WIDE_PARTITIONS for the measurement behind the size.
    private static void createWideTables(CairoEngine engine, SqlExecutionContext executionContext) throws SqlException {
        engine.execute("CREATE TABLE wt AS (SELECT timestamp_sequence('2020-06-01T00:00:00.000000Z', 60000000L) ts, x v " +
                "FROM long_sequence(" + (WIDE_PARTITIONS * ROWS_PER_PARTITION) + ")) TIMESTAMP(ts) PARTITION BY DAY", executionContext);
        engine.execute("CREATE TABLE wbounds (b TIMESTAMP)", executionContext);
        engine.execute("INSERT INTO wbounds VALUES ('2020-06-01T00:00:00.000000Z')", executionContext);
    }

    private static void createTables(CairoEngine engine, SqlExecutionContext executionContext) throws SqlException {
        engine.execute("CREATE TABLE t (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY", executionContext);
        engine.execute("INSERT INTO t VALUES " +
                "('2020-06-01T00:00:00.000000Z', 1), " +
                "('2020-06-02T00:00:00.000000Z', 2), " +
                "('2020-06-03T00:00:00.000000Z', 3)", executionContext);
        engine.execute("CREATE TABLE bounds (b TIMESTAMP)", executionContext);
        engine.execute("INSERT INTO bounds VALUES ('2020-06-01T00:00:00.000000Z'), ('2020-06-02T00:00:00.000000Z')", executionContext);
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
