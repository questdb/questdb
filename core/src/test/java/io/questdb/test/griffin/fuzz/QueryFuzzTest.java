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

package io.questdb.test.griffin.fuzz;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.fuzz.FailureFileFacade;
import io.questdb.test.griffin.fuzz.expr.BindContext;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;

/**
 * Seeded random query fuzzer. Generates 1..3 WAL tables with all supported
 * scalar types plus DECIMAL and DOUBLE arrays, inserts rows that span
 * multiple DAY partitions, then runs a budget of randomly generated
 * SELECT / GROUP BY / SAMPLE BY / ASOF-LT-SPLICE JOIN queries and
 * materializes every result row, additionally re-iterating each cursor
 * after {@code toTop()} and cross-checking {@code size()} /
 * {@code calculateSize()} against the materialized row count.
 * <p>
 * Oracle is crash-only: {@link SqlException} is swallowed (legitimate
 * user-facing error); anything else, including {@link CairoException},
 * is recorded as a failure, because a generated SELECT should never leak
 * an internal exception. Failures are collected for the whole run and
 * reported together at the end, so a single invocation surfaces every
 * bug in one go. The driving seeds are printed so a failure can be
 * reproduced deterministically.
 * <p>
 * Budget knobs:
 * <ul>
 *     <li>{@code -Dquestdb.fuzz.queries=N} &mdash; number of queries per
 *         run (default 100). Crank up locally when hunting bugs.</li>
 *     <li>{@code -Dquestdb.fuzz.diff.jit=true|false} &mdash; differential
 *         JIT-on/off mode (default true). When enabled, every query is run
 *         twice and the materializations are compared; any divergence is
 *         reported as a failure.</li>
 *     <li>{@code -Dquestdb.fuzz.diff.shadow=true|false} &mdash; differential
 *         storage mode (default true). Each fuzz table has a shadow sibling
 *         that holds identical data with independently random parquet/index
 *         settings; every query is run against both and the materializations
 *         compared. With both diffs enabled, three runs per query are
 *         needed (primary @ JIT-on, primary @ JIT-off, shadow @ JIT-off)
 *         and either divergence axis fails the query.</li>
 *     <li>{@code -Dquestdb.fuzz.verify.cursor=true|false} &mdash; verify
 *         per-cursor self-consistency (default true): every materialization
 *         is re-iterated after {@code toTop()} and must reproduce the same
 *         result set, {@code toTop()} must preserve {@code preComputedStateSize()},
 *         and {@code size()} / {@code calculateSize()} must agree with the
 *         materialized row count. These hold for every cursor, so a violation
 *         is reported as a failure with no skip valves.</li>
 *     <li>{@code -Dquestdb.fuzz.faults=true|false} &mdash; randomly enabled
 *         fault injection (default true). On a fraction of queries
 *         ({@code -Dquestdb.fuzz.fault.pct=N}, default 15) one fault is armed:
 *         a failing filesystem op, a native allocation that trips the RSS
 *         memory limit, or a thrown {@code test_fault()} woven into the query.
 *         The runner then asserts the factory frees its resources on the error
 *         path and that the same query runs cleanly once the fault is removed.
 *         Fault queries bypass the differential oracle. All three fault types
 *         run with parallel SQL execution enabled by default
 *         ({@code -Dquestdb.fuzz.fault.parallel}, default true), so the parallel
 *         filter / GROUP BY / top-K reduce error paths get exercised; pass false
 *         to run them serially. The writer pool is halted for the whole query
 *         loop, so no background job competes with the armed FILE / MALLOC fault
 *         (which would otherwise fire on a background-job file op or trip the
 *         process-global RSS ceiling on a background thread).</li>
 *     <li>{@code -Dquestdb.fuzz.window=true|false} &mdash; generate
 *         window-function shapes ({@code fn(...) OVER (PARTITION BY ...
 *         ORDER BY ts [frame])}) on a fraction of queries (default true).
 *         On by default like fault injection; it currently surfaces
 *         still-unfixed window-function defects, so the run goes red on
 *         the seeds that hit them. The WINDOW band is carved from the
 *         SIMPLE range, so the other shapes' frequencies are unchanged;
 *         pass {@code false} to drop window shapes and exercise the
 *         rest.</li>
 *     <li>{@code -Dquestdb.fuzz.s0=L -Dquestdb.fuzz.s1=L} - replay a
 *         specific seed pair, as printed in the run's "random seeds: ..."
 *         line. Use to reproduce a failure deterministically.</li>
 * </ul>
 * <p>
 * Each query also has a small chance of running with parallel SQL
 * execution disabled (parallel filter, GROUP BY, top-K, parquet read), so
 * the serial code paths get exercised alongside the parallel ones. The
 * coin flip pulls from the seeded rnd, so replaying with the same seeds
 * reproduces the same on/off pattern.
 * <p>
 * On a smaller fraction of queries the runner also generates a
 * bind-variable variant: the same query rewritten with a subset of
 * bindable typed constants replaced by named {@code :bN::TYPE}
 * placeholders, with values supplied through
 * {@code BindVariableService.setStr(name, value)}. The variant runs at
 * JIT-off and (when diff-JIT is enabled) at JIT-on, each compared
 * against the literal form's pivot at the same JIT mode, since the JIT
 * compiler has its own bind-variable handling that doesn't share code
 * with the Java-filter path. Any divergence between the literal and bind
 * paths (bind-time coercion vs constant fold) is reported as a fuzz
 * failure.
 */
public class QueryFuzzTest extends AbstractCairoTest {
    // Per-constant chance, in percent, of substituting a bindable literal
    // with a bind variable inside the bind variant.
    private static final int CONSTANT_BIND_PROBABILITY_PCT = 50;
    // Per-query chance, in percent, of generating a bind-variable variant.
    private static final int QUERY_BIND_PROBABILITY_PCT = 20;
    // Name of the query (SQL) worker pool. Its worker threads are named
    // QUERY_POOL_NAME + '_' + workerId, which the FILE fault's query-execution
    // scope matches to admit reduce workers alongside the test thread.
    private static final String QUERY_POOL_NAME = "fuzzQuery";
    // Per-query chance, in percent, of running with parallel SQL execution disabled.
    private static final int SERIAL_PROBABILITY_PCT = 5;

    @Test
    public void testDecimalAggregationOverflowToleratedByOracle() throws Exception {
        // Bug from window-function fuzzing: a sum/avg over a high-precision
        // DECIMAL (Decimal256-backed) whose running total exceeds Decimal256's
        // 256-bit capacity raises a CairoException "... aggregation failed: an
        // overflow occurred". This is a genuine, data-dependent arithmetic limit -
        // the plain group-by sum/avg overflows identically on the same data, and
        // WindowDecimalFunctionTest pins it - so the oracle must treat it as an
        // accepted skip rather than reporting it as an engine defect. Drive the
        // repro queries straight through QueryRunner.run() to exercise the real
        // classification path.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, g SYMBOL, d DECIMAL(76, 3)) TIMESTAMP(ts) PARTITION BY HOUR");
            // 73 integer nines + 3 fractional nines == DECIMAL(76, 3) maximum; two in
            // one partition overflow the Decimal256 accumulator at add time.
            String nearMax = "9".repeat(73) + ".999m";
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00', 'a', " + nearMax + "), " +
                    "('2024-01-01T00:01:00', 'a', " + nearMax + ")");

            QueryRunner runner = new QueryRunner(engine, sqlExecutionContext, false, false, true, new ObjList<>(), null);

            QueryRunner.Result avgResult = runner.run(
                    new GeneratedQuery("SELECT avg(d) OVER (PARTITION BY g ORDER BY ts) c FROM t", true));
            Assert.assertTrue("avg overflow must be tolerated, not failed: "
                    + (avgResult.getFailure() != null ? avgResult.getFailure().getMessage() : ""), avgResult.isSkipped());
            Assert.assertFalse(avgResult.isFailed());

            QueryRunner.Result sumResult = runner.run(
                    new GeneratedQuery("SELECT sum(d) OVER (PARTITION BY g ORDER BY ts) c FROM t", true));
            Assert.assertTrue("sum overflow must be tolerated, not failed: "
                    + (sumResult.getFailure() != null ? sumResult.getFailure().getMessage() : ""), sumResult.isSkipped());
            Assert.assertFalse(sumResult.isFailed());

            // A query that does not overflow still runs to completion: the carve-out
            // is narrow and does not turn a clean run into a skip.
            execute("CREATE TABLE small (ts TIMESTAMP, g SYMBOL, d DECIMAL(18, 2)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO small VALUES " +
                    "('2024-01-01T00:00:00', 'a', 1.00m), ('2024-01-01T00:01:00', 'a', 2.00m)");
            QueryRunner.Result okResult = runner.run(
                    new GeneratedQuery("SELECT avg(d) OVER (PARTITION BY g ORDER BY ts) c FROM small", true));
            Assert.assertFalse(okResult.isSkipped());
            Assert.assertFalse(okResult.isFailed());
        });
    }

    @Test
    public void testImplicitTimestampLimitTolerated() throws Exception {
        // Bug from fault-injection fuzzing: SELECT max(ts) FROM t WHERE test_fault()
        // swallowed the injected fault under parallel execution and was reported as a
        // swallowed-error failure. The optimiser
        // (SqlOptimiser.rewriteSingleFirstLastGroupBy) rewrites a lone
        // min/max/first/last over the designated timestamp into an
        // ORDER BY ts [DESC] LIMIT 1 scan, so a fault that fires on a frame past the
        // single-row cutoff is legitimately discarded -- the same early termination
        // an explicit LIMIT gives -- yet the SQL text carries no "limit" keyword. The
        // swallow oracle keys off the rewritten plan's pushed-down limit marker
        // instead. Pin that the rewrite still produces the marker for the four
        // timestamp aggregates, and that aggregates which do not get the rewrite
        // (and so must surface a fired fault) do not carry it.
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (c0 INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t SELECT x::int, timestamp_sequence('2024-01-01', 1_000_000_000L) " +
                    "FROM long_sequence(100)");

            // The timestamp min/max/first/last rewrite pushes LIMIT 1 into the scan.
            String[] pushed = {
                    "SELECT max(ts) AS a0 FROM t WHERE test_fault() ORDER BY a0",
                    "SELECT min(ts) AS a0 FROM t WHERE test_fault() ORDER BY a0",
                    "SELECT first(ts) AS a0 FROM t WHERE test_fault()",
                    "SELECT last(ts) AS a0 FROM t WHERE test_fault()",
            };
            for (String sql : pushed) {
                Assert.assertTrue("expected pushed-down limit in plan: " + sql,
                        QueryRunner.planHasPushedLimit(planOf(sql)));
            }

            // Aggregates that keep a real aggregation step have no pushed limit, so a
            // fired fault must surface and the oracle must not be relaxed for them.
            // This includes a LIMIT over a row-count-changing aggregate: the LIMIT is
            // trivially satisfied by the single aggregate row and pushes no scan limit,
            // so the async filter still scans every frame and a fired fault must
            // surface. Since the SQL text does carry "limit", the swallow oracle keeps
            // these from being silently tolerated through factoryHasBlockingAggregation
            // rather than the pushed-limit marker.
            String[] notPushed = {
                    "SELECT max(c0) AS a0 FROM t WHERE test_fault() ORDER BY a0",
                    "SELECT max(ts) AS a0, count() AS a1 FROM t WHERE test_fault()",
                    "SELECT count() AS a0 FROM t WHERE test_fault()",
                    "SELECT count() AS a0 FROM t WHERE test_fault() LIMIT 5",
                    "SELECT sum(c0) AS a0 FROM t WHERE test_fault() LIMIT 5",
                    "SELECT avg(c0) AS a0 FROM t WHERE test_fault() LIMIT 1",
            };
            for (String sql : notPushed) {
                Assert.assertFalse("unexpected pushed-down limit in plan: " + sql,
                        QueryRunner.planHasPushedLimit(planOf(sql)));
            }
        });
    }

    @Test
    public void testQueryFuzz() throws Exception {
        // Enable dev mode so the test_fault() function the FUNCTION fault relies on
        // is active; it folds to the constant true otherwise.
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        // Install a FailureFileFacade as the engine's files facade so the runner can
        // arm file-I/O faults; it is a transparent passthrough until armed.
        final FailureFileFacade faultFf = new FailureFileFacade(engine.getConfiguration().getFilesFacade());
        engine.clear();
        assertMemoryLeak(faultFf, () -> {
            // Query (SQL) jobs and writer (O3 / purge / index) jobs run on two
            // separate pools so runFuzz can halt the writer pool once the tables are
            // built, leaving the query loop with no background file ops or native
            // allocations on the shared workers. That quiesced query phase lets FILE
            // and MALLOC faults run under parallel SQL execution. The fresh
            // SqlExecutionContext advertises the real pool width to the planner (the
            // default test context reports getSharedQueryWorkerCount()=1).
            final int workerCount = Integer.getInteger("questdb.fuzz.workers", 4);
            final WorkerPool queryPool = new WorkerPool(new WorkerPoolConfiguration() {
                @Override
                public String getPoolName() {
                    return QUERY_POOL_NAME;
                }

                @Override
                public int getWorkerCount() {
                    return workerCount;
                }
            });
            WorkerPoolUtils.setupQueryJobs(queryPool, engine);
            final WorkerPool writerPool = new WorkerPool(new WorkerPoolConfiguration() {
                @Override
                public String getPoolName() {
                    return "fuzzWriter";
                }

                @Override
                public int getWorkerCount() {
                    return workerCount;
                }
            });
            WorkerPoolUtils.setupWriterJobs(writerPool, engine);
            queryPool.start(LOG);
            writerPool.start(LOG);
            try (
                    SqlExecutionContext parallelCtx = new SqlExecutionContextImpl(engine, workerCount)
                            .with(securityContext, bindVariableService, null, -1, circuitBreaker)
            ) {
                parallelCtx.initNow();
                runFuzz(parallelCtx, writerPool, QUERY_POOL_NAME + '_');
            } finally {
                // Suppress halt-time failures so they don't mask the original
                // assertion or test exception that's already on its way out. halt()
                // is idempotent, so halting the writer pool here is a no-op once
                // runFuzz has already halted it after the build phase.
                haltQuietly(writerPool);
                haltQuietly(queryPool);
            }
        });
    }

    private static AssertionError buildFailure(ObjList<QueryRunner.Result> failures) {
        StringBuilder sb = new StringBuilder("query fuzz found ").append(failures.size())
                .append(" unexpected failure(s):\n");
        for (int i = 0, n = failures.size(); i < n; i++) {
            QueryRunner.Result r = failures.getQuick(i);
            sb.append("  [").append(i + 1).append("] ")
                    .append(r.getFailure().getClass().getSimpleName())
                    .append(": ").append(r.getFailure().getMessage()).append('\n')
                    .append("        sql: ").append(r.getSql()).append('\n');
        }
        // Chain the first cause so the stack trace still points at real source.
        return new AssertionError(sb.toString(), failures.getQuick(0).getFailure());
    }

    private static String planOf(String sql) throws SqlException {
        try (RecordCursorFactory factory = engine.select(sql, sqlExecutionContext)) {
            TextPlanSink sink = new TextPlanSink();
            sink.of(factory, sqlExecutionContext);
            return sink.getSink().toString();
        }
    }

    private static void haltQuietly(WorkerPool pool) {
        try {
            pool.halt();
        } catch (Throwable t) {
            LOG.error().$("worker pool halt failed: ").$(t).$();
        }
    }

    private static void logSchema(FuzzTable t) {
        StringBuilder sb = new StringBuilder("fuzz schema ").append(t.getName())
                .append(" (parquet=").append(t.getParquetMode());
        if (t.getParquetPartitions() != null) {
            sb.append(" partitions=[").append(t.getParquetPartitions()).append(']');
        }
        sb.append("):");
        for (int j = 0, n = t.getColumnCount(); j < n; j++) {
            FuzzColumn c = t.getColumn(j);
            sb.append(' ').append(c.getName()).append('=').append(c.getType().getDdl());
            if (c.isIndexed()) {
                sb.append(c.getIndex().describe());
            }
        }
        LOG.info().$safe(sb.toString()).$();
    }

    private static BufferedWriter openDump(String path) throws IOException {
        if (path == null || path.isEmpty()) {
            return null;
        }
        return new BufferedWriter(new FileWriter(Paths.get(path).toFile(), true));
    }

    private static void runFuzz(SqlExecutionContext sqlExecutionContext, WorkerPool writerPool, String queryWorkerNamePrefix) throws Exception {
        Long s0 = Long.getLong("questdb.fuzz.s0");
        Long s1 = Long.getLong("questdb.fuzz.s1");
        Rnd rnd = (s0 != null && s1 != null)
                ? TestUtils.generateRandom(LOG, s0, s1)
                : TestUtils.generateRandom(LOG);
        FuzzConfig config = new FuzzConfig(rnd);

        LOG.info().$("fuzz config: tables=").$(config.getNumTables())
                .$(", rows=").$(config.getRowsPerTable())
                .$(", queries=").$(config.getNumQueries())
                .$(", diffJit=").$(config.isDiffJitEnabled())
                .$(", diffShadow=").$(config.isDiffShadowEnabled())
                .$(", verifyCursor=").$(config.isVerifyCursorEnabled())
                .$(", faults=").$(config.isFaultInjectionEnabled())
                .$(", faultPct=").$(config.getFaultProbabilityPct())
                .$(", parallelFaults=").$(config.isParallelFaultEnabled())
                .$(", window=").$(config.isWindowEnabled())
                .$();

        FuzzTableFactory factory = new FuzzTableFactory(config);
        ObjList<FuzzTable> tables = new ObjList<>();
        for (int i = 0; i < config.getNumTables(); i++) {
            FuzzTable t = factory.create(
                    rnd,
                    "fuzz_t" + i,
                    sql -> engine.execute(sql, sqlExecutionContext),
                    QueryFuzzTest::drainWalQueue
            );
            tables.add(t);
            logSchema(t);
            if (t.getShadow() != null) {
                logSchema(t.getShadow());
            }
        }

        // Writes are done: close the build-phase writers and halt the writer pool
        // so the query loop runs background-silent (see testQueryFuzz). A column
        // purge scheduled by the writer close is abandoned -- it only touches
        // on-disk files, which the leak oracle does not measure.
        engine.releaseInactive();
        writerPool.halt();

        QueryRunner runner = new QueryRunner(engine, sqlExecutionContext, config.isDiffJitEnabled(), config.isDiffShadowEnabled(), config.isVerifyCursorEnabled(), tables, queryWorkerNamePrefix);
        // Snapshot the four parallel-execution flags so the per-query serial
        // override can restore them. Snapshotting once outside the loop also
        // preserves any global override the user passed via system properties.
        final boolean savedParallelFilter = sqlExecutionContext.isParallelFilterEnabled();
        final boolean savedParallelGroupBy = sqlExecutionContext.isParallelGroupByEnabled();
        final boolean savedParallelReadParquet = sqlExecutionContext.isParallelReadParquetEnabled();
        final boolean savedParallelTopK = sqlExecutionContext.isParallelTopKEnabled();
        int bindGen = 0;
        int faultGen = 0;
        int skipped = 0;
        int serial = 0;
        ObjList<QueryRunner.Result> failures = new ObjList<>();
        try (BufferedWriter dump = openDump(config.getDumpPath())) {
            for (int q = 0; q < config.getNumQueries(); q++) {
                // Decide fault injection before generation so a FUNCTION fault can
                // ask the generator to emit the fault function. Drawn from the
                // seeded rnd, so replay reproduces the choice.
                FaultType faultType = (config.isFaultInjectionEnabled() && rnd.nextInt(100) < config.getFaultProbabilityPct())
                        ? runner.chooseFaultType(rnd)
                        : null;
                boolean injectFaultFn = faultType == FaultType.FUNCTION;
                long preGenS0 = rnd.getSeed0();
                long preGenS1 = rnd.getSeed1();
                GeneratedQuery query = QueryGenerator.generate(rnd, tables, null, injectFaultFn, config.isWindowEnabled());
                QueryRunner.Result result;
                if (faultType != null) {
                    // Fault queries use a crash-and-recover oracle, not the
                    // differential one, so they skip the bind/serial variants.
                    faultGen++;
                    if (dump != null) {
                        dump.write(query.sql());
                        dump.newLine();
                    }
                    // With the knob on, all fault types run under parallel SQL
                    // execution so the parallel filter / GROUP BY / top-K reduce
                    // error paths get exercised. The writer pool is halted for the
                    // whole query loop, so no background job competes: FUNCTION is
                    // data-scoped, FILE is scoped to the query execution, and
                    // MALLOC's process-global RSS ceiling can only be tripped by the
                    // query's own allocations.
                    boolean runFaultParallel = config.isParallelFaultEnabled();
                    LOG.info().$("fuzz fault (").$(faultType.name()).$(runFaultParallel ? ", parallel" : ", serial").$("): ").$safe(query.sql()).$();
                    if (!runFaultParallel) {
                        sqlExecutionContext.setParallelFilterEnabled(false);
                        sqlExecutionContext.setParallelGroupByEnabled(false);
                        sqlExecutionContext.setParallelReadParquetEnabled(false);
                        sqlExecutionContext.setParallelTopKEnabled(false);
                    }
                    try {
                        result = runner.runFault(query, faultType, rnd, runFaultParallel);
                    } finally {
                        if (!runFaultParallel) {
                            sqlExecutionContext.setParallelFilterEnabled(savedParallelFilter);
                            sqlExecutionContext.setParallelGroupByEnabled(savedParallelGroupBy);
                            sqlExecutionContext.setParallelReadParquetEnabled(savedParallelReadParquet);
                            sqlExecutionContext.setParallelTopKEnabled(savedParallelTopK);
                        }
                    }
                } else {
                    // With small probability, regenerate the same query with a
                    // BindContext threaded through so a fraction of bindable
                    // typed constants emit as ?::TYPE bind variables. The Rnd
                    // is rewound to the pre-literal state so the tree shape
                    // matches; the BindContext gets its own derived Rnd so the
                    // bind/no-bind decisions are deterministic per seed.
                    //
                    // Determinism invariant: the second QueryGenerator.generate()
                    // call must draw the same number and order of rnd ops as the
                    // first. The two calls only differ in whether they consult
                    // the BindContext (which uses its own seeded Rnd), so adding
                    // any rnd operation to the bind path - or skipping one on
                    // the literal path - desynchronises the two trees and the
                    // shapes will diverge. Take care when modifying any code
                    // reachable from QueryGenerator.generate().
                    if (rnd.nextInt(100) < QUERY_BIND_PROBABILITY_PCT) {
                        long bindS0 = rnd.nextLong();
                        long bindS1 = rnd.nextLong();
                        rnd.reset(preGenS0, preGenS1);
                        BindContext ctx = new BindContext(new Rnd(bindS0, bindS1), CONSTANT_BIND_PROBABILITY_PCT);
                        GeneratedQuery bindForm = QueryGenerator.generate(rnd, tables, ctx, injectFaultFn, config.isWindowEnabled());
                        if (ctx.getBindValues().size() > 0) {
                            query = query.withBind(bindForm.sql(), ctx.getBindNames(), ctx.getBindValues());
                            bindGen++;
                            LOG.info().$("fuzz bind: ").$safe(query.bindSql()).$();
                        }
                    }
                    if (dump != null) {
                        dump.write(query.sql());
                        dump.newLine();
                    }
                    boolean disableParallel = rnd.nextInt(100) < SERIAL_PROBABILITY_PCT;
                    if (disableParallel) {
                        serial++;
                        sqlExecutionContext.setParallelFilterEnabled(false);
                        sqlExecutionContext.setParallelGroupByEnabled(false);
                        sqlExecutionContext.setParallelReadParquetEnabled(false);
                        sqlExecutionContext.setParallelTopKEnabled(false);
                        LOG.info().$("fuzz serial: ").$safe(query.sql()).$();
                    }
                    try {
                        result = runner.run(query);
                    } finally {
                        if (disableParallel) {
                            sqlExecutionContext.setParallelFilterEnabled(savedParallelFilter);
                            sqlExecutionContext.setParallelGroupByEnabled(savedParallelGroupBy);
                            sqlExecutionContext.setParallelReadParquetEnabled(savedParallelReadParquet);
                            sqlExecutionContext.setParallelTopKEnabled(savedParallelTopK);
                        }
                    }
                }
                if (result.isSkipped()) {
                    skipped++;
                    LOG.info().$("fuzz skip (").$safe(result.getSkipReason()).$("): ").$safe(query.sql()).$();
                } else if (result.isFailed()) {
                    LOG.error().$("fuzz failure on query: ").$safe(query.sql())
                            .$(" -- ").$(result.getFailure().getClass().getName())
                            .$(": ").$safe(result.getFailure().getMessage())
                            .$();
                    failures.add(result);
                }
            }
        }
        LOG.info().$("fuzz done: ").$(config.getNumQueries()).$(" queries, ")
                .$(serial).$(" serial, ")
                .$(faultGen).$(" with fault injection (").$(runner.getFaultsFired()).$(" fired), ")
                .$(bindGen).$(" with bind variant, ")
                .$(skipped).$(" skipped on expected errors, ")
                .$(failures.size()).$(" failures")
                .$();
        LOG.info().$("fuzz faults by type: ")
                .$("FILE ").$(runner.getFaultsFired(FaultType.FILE)).$('/').$(runner.getFaultsArmed(FaultType.FILE))
                .$(", MALLOC ").$(runner.getFaultsFired(FaultType.MALLOC)).$('/').$(runner.getFaultsArmed(FaultType.MALLOC))
                .$(", FUNCTION ").$(runner.getFaultsFired(FaultType.FUNCTION)).$('/').$(runner.getFaultsArmed(FaultType.FUNCTION))
                .$(" (fired/armed)")
                .$();

        if (failures.size() > 0) {
            throw buildFailure(failures);
        }
    }
}
