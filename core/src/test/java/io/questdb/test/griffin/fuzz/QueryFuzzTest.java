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
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.mp.WorkerPool;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.fuzz.FailureFileFacade;
import io.questdb.test.griffin.fuzz.expr.BindContext;
import io.questdb.test.tools.TestUtils;
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
 *         Fault queries run serially and bypass the differential oracle.</li>
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
    // Per-query chance, in percent, of running with parallel SQL execution disabled.
    private static final int SERIAL_PROBABILITY_PCT = 5;

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
            // Run queries through a 4-thread shared worker pool so parallel filter,
            // GROUP BY, top-K, window/horizon join and parquet read paths are exercised.
            // The default test sqlExecutionContext reports getSharedQueryWorkerCount()=1,
            // so build a fresh one that advertises the actual pool width to the planner.
            final int workerCount = Integer.getInteger("questdb.fuzz.workers", 4);
            final WorkerPool pool = new WorkerPool(() -> workerCount);
            TestUtils.setupWorkerPool(pool, engine);
            pool.start(LOG);
            try (
                    SqlExecutionContext parallelCtx = new SqlExecutionContextImpl(engine, workerCount)
                            .with(securityContext, bindVariableService, null, -1, circuitBreaker)
            ) {
                parallelCtx.initNow();
                runFuzz(parallelCtx);
            } finally {
                // Suppress halt-time failures so they don't mask the original
                // assertion or test exception that's already on its way out.
                try {
                    pool.halt();
                } catch (Throwable t) {
                    LOG.error().$("worker pool halt failed: ").$(t).$();
                }
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

    private static void runFuzz(SqlExecutionContext sqlExecutionContext) throws Exception {
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

        QueryRunner runner = new QueryRunner(engine, sqlExecutionContext, config.isDiffJitEnabled(), config.isDiffShadowEnabled(), config.isVerifyCursorEnabled(), tables);
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
                    LOG.info().$("fuzz fault (").$(faultType.name()).$("): ").$safe(query.sql()).$();
                    // Run faults with parallel execution disabled. A fault thrown
                    // mid-parallel-execution can leave a pooled page-frame reduce
                    // task holding an un-released buffer, which then corrupts a
                    // later query that reuses the task. Serial execution exercises
                    // the resource-cleanup path without that shared-state hazard.
                    sqlExecutionContext.setParallelFilterEnabled(false);
                    sqlExecutionContext.setParallelGroupByEnabled(false);
                    sqlExecutionContext.setParallelReadParquetEnabled(false);
                    sqlExecutionContext.setParallelTopKEnabled(false);
                    try {
                        result = runner.runFault(query, faultType, rnd);
                    } finally {
                        sqlExecutionContext.setParallelFilterEnabled(savedParallelFilter);
                        sqlExecutionContext.setParallelGroupByEnabled(savedParallelGroupBy);
                        sqlExecutionContext.setParallelReadParquetEnabled(savedParallelReadParquet);
                        sqlExecutionContext.setParallelTopKEnabled(savedParallelTopK);
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
                    // any rnd operation to the bind path -- or skipping one on
                    // the literal path -- desynchronises the two trees and the
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

        if (failures.size() > 0) {
            throw buildFailure(failures);
        }
    }
}
