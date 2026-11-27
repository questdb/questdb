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

import io.questdb.Metrics;
import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestListener;
import io.questdb.test.cairo.fuzz.FailureFileFacade;
import io.questdb.test.griffin.ParallelHashJoinTestUtils;
import io.questdb.test.griffin.fuzz.clauses.EquiJoinClause;
import io.questdb.test.griffin.fuzz.expr.BindContext;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Seeded random query fuzzer. Generates 2..3 WAL tables with all supported
 * scalar types plus DECIMAL and DOUBLE arrays, inserts rows that span
 * multiple DAY partitions, then runs a budget of randomly generated
 * SELECT / GROUP BY / SAMPLE BY / LATEST ON / ASOF-LT-SPLICE / HORIZON /
 * WINDOW JOIN / equi-join GROUP BY / row-producing equi-join
 * queries and
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
 *         run (default 1,000). Crank up locally when hunting bugs.</li>
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
 *         memory limit, a per-query memory limit that the query's tracked
 *         allocations breach, or a thrown {@code test_fault()} woven into the
 *         query, which an equi-join query may also carry in an ON residual or a
 *         build input's WHERE clause.
 *         The runner then asserts the factory frees its resources on the error
 *         path and that the same query runs cleanly once the fault is removed.
 *         Each of these queries that its fault leaves whole, and each equi-join
 *         query that the comparisons below found whole, then runs once more
 *         under a cancellation: its circuit breaker cancels it at one of its
 *         first checks, on the owner or on a worker that runs its tasks, and the
 *         query must fail with the cancellation or return every row. The
 *         cancellations draw from a generator of their own, which the query's
 *         text seeds, so they change no query that a seed generates.
 *         Fault queries bypass the differential oracle. All fault types
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
 *     <li>{@code -Dquestdb.fuzz.lateston=true|false} &mdash; generate
 *         LATEST ON shapes ({@code ... LATEST ON ts PARTITION BY col[, ...]},
 *         the latest row per partition key) on a fraction of queries
 *         (default true). The LATEST ON band is carved from the SIMPLE
 *         range just below the WINDOW band, so the other shapes' frequencies
 *         are unchanged; pass {@code false} to drop them and give the band
 *         back to SIMPLE. The result is a stable multiset (timestamps are
 *         unique), so the differential oracle compares result sets row for
 *         row across JIT and the indexed-symbol storage shadow. On by
 *         default like window.</li>
 *     <li>{@code -Dquestdb.fuzz.horizonjoin=true|false} and
 *         {@code -Dquestdb.fuzz.windowjoin=true|false} &mdash; generate
 *         HORIZON JOIN (a keyed GROUP BY over offset-shifted ASOF matches)
 *         and WINDOW JOIN (a per-master-row aggregate over a slave time
 *         frame) shapes (both default true). They share the join band with
 *         the ASOF/LT/SPLICE temporal joins, so enabling them splits that
 *         band rather than widening it; with both off the band is
 *         temporal-only. Their WHERE is master-side only, so the FUNCTION
 *         fault is woven there like the other shapes.</li>
 *     <li>{@code -Dquestdb.fuzz.hashjoin=true|false} &mdash; generate
 *         equi-join GROUP BY shapes (an INNER, LEFT, RIGHT or FULL join on one
 *         or two of the shared key columns under a keyed or scalar aggregate),
 *         the shape the fused hash join GROUP BY replaces (default true). Their band is carved from
 *         the GROUP BY range, so the other shapes' frequencies are unchanged;
 *         pass {@code false} to give the band back to GROUP BY.</li>
 *     <li>{@code -Dquestdb.fuzz.diff.fused=true|false} &mdash; run every
 *         equi-join GROUP BY query a second time with the fused hash join
 *         GROUP BY disabled and compare the two materializations (default
 *         true). The JIT, storage and bind axes run the fused plan on both of
 *         their sides, so only this axis sees a fused plan that returns wrong
 *         rows. The run asserts that the fused plan ran on enough of the
 *         comparisons whose query compiled to rule out a planner that has
 *         stopped selecting it.</li>
 *     <li>{@code -Dquestdb.fuzz.equijoin=true|false} &mdash; generate
 *         row-producing equi-join shapes (an INNER or LEFT join on one or two of
 *         the shared key columns, alone or followed by up to five more joins over
 *         the run's tables, a star the parallel hash join stacks into stages, with
 *         ON residuals, post-join filters, intervals, window functions, ORDER BY
 *         and LIMIT), the shape the parallel hash join runs (default true). Their
 *         band is carved from the SIMPLE range, so the other shapes' frequencies
 *         are unchanged; pass {@code false} to give the band back to SIMPLE.</li>
 *     <li>{@code -Dquestdb.fuzz.diff.parallel.join=true|false} &mdash; run every
 *         equi-join query a second time with the parallel hash join disabled and
 *         compare the two materializations in order (default true). The run turns
 *         the operator on for every query, and the other axes run it on both of
 *         their sides, so only this axis sees an operator that returns wrong rows
 *         or the right rows in another order. The run asserts that the operator ran
 *         on enough of the comparisons whose query compiled, and stacked two joins
 *         or more on enough of those, to rule out a planner that has stopped
 *         selecting it or stacking it.</li>
 *     <li>{@code -Dquestdb.fuzz.parallel.join.cap=N} &mdash; the parallel hash
 *         join's cap of tuples per probe task (default: the seed picks one tuple,
 *         which leaves the rest of a frame with fan-out to continuations, or the
 *         operator's own default).</li>
 *     <li>{@code -Dquestdb.fuzz.parallel.join.frames=N},
 *         {@code -Dquestdb.fuzz.parallel.join.ring=BYTES} and
 *         {@code -Dquestdb.fuzz.parallel.join.dispatch=N} &mdash; the frame
 *         pressure that the run's equi-join queries run under: page frames of at
 *         most N rows, a tuple ring budget and a dispatch limit, so that the
 *         probe has more frames than the ring has entries and the ring's window
 *         moves (default: the seed picks each, see FuzzConfig). A query's every
 *         axis runs under the same knobs.</li>
 *     <li>{@code -Dquestdb.fuzz.parallel.join.probe.frames=N} &mdash; the
 *         parallel hash join's most rows of a native probe frame, which leaves
 *         the build scans' frames as they are (default: the seed picks 1 to 8
 *         rows, or the operator's own default).</li>
 *     <li>{@code -Dquestdb.fuzz.parquet.row.group=N} &mdash; the rows of a
 *         Parquet row group of the run's tables (default: the engine's).</li>
 *     <li>{@code -Dquestdb.fuzz.diff.stream=true|false} &mdash; run every
 *         query whose plan uses the stream GROUP BY a second time with it
 *         disabled and compare against the hash GROUP BY (default true). Inner
 *         SAMPLE BY subqueries put the stream plan under LATEST ON, joins and
 *         LIMIT. The run asserts that the axis compared at least one query
 *         once SAMPLE BY reaches the sample floor.</li>
 *     <li>{@code -Dquestdb.fuzz.diff.mappage=true|false} &mdash; run every
 *         query once more, and an equi-join GROUP BY query several more times,
 *         with {@code cairo.sql.small.map.page.size} cut to between 8 and 512
 *         bytes, and compare each run with the default-page one (default
 *         true). The tables are too small to grow a map with the
 *         default page, so only this axis runs operators over maps that grow
 *         while they hold cursors and flyweights into them.</li>
 *     <li>{@code -Dquestdb.fuzz.hashjoin.payload=row_ids|copied} &mdash; force
 *         every fused hash join GROUP BY to read its build's payload columns
 *         where they live, or from the copy its build makes (default: neither,
 *         the operator's own rule, which copies over the fuzz tables' equal
 *         row counts unless an interval leaves the probe under half the
 *         build).</li>
 *     <li>{@code -Dquestdb.fuzz.s0=L -Dquestdb.fuzz.s1=L} - replay a
 *         specific seed pair, as printed in the run's "random seeds: ..."
 *         line. Use to reproduce a failure deterministically.</li>
 * </ul>
 * <p>
 * Each query also has a small chance of running with parallel SQL
 * execution disabled (parallel filter, GROUP BY, fused hash join GROUP BY,
 * parallel hash join, top-K, parquet read), so
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
    // The rows of a page frame of the concurrent run of equi-joins, unless the seed picks fewer: its
    // tables of 60 to 150 rows then scan as 15 frames or more.
    private static final int CONCURRENT_EQUI_JOIN_PAGE_FRAME_ROWS = 4;
    private static final int CONCURRENT_PAGE_FRAME_ROWS = 100;
    private static final long CONCURRENT_STALL_NANOS = 60_000_000_000L;
    // Per-constant chance, in percent, of substituting a bindable literal
    // with a bind variable inside the bind variant.
    private static final int CONSTANT_BIND_PROBABILITY_PCT = 50;
    // Differential queries a shape needs before runFuzz holds it to MIN_ACCEPTED_PCT_PER_SHAPE.
    // Below this count the accepted rate is too noisy to assert on. FuzzConfig's default budget is
    // sized so every shape the run can draw clears this - at the old 100-query default only
    // SAMPLE_BY did, leaving the guard dormant for the other eight shapes. runFuzz logs any shape
    // that still falls short, so a generator that goes rare cannot silently stop being guarded; it
    // is logged rather than asserted because the rarest shape draws ~3.4% of a run, which at the
    // default budget dips under the floor often enough by chance alone to make an assertion flaky.
    // A shape that generated nothing at all is a different case and MIN_QUERIES_FOR_ZERO_GUARD
    // guards it: no budget lifts POSTING off zero, because it needs the run's random schema to
    // carry a posting-indexed SYMBOL.
    private static final int MIN_SHAPE_QUERIES_FOR_ACCEPT_FLOOR = 25;
    // Per-query chance, in percent, of generating a bind-variable variant.
    private static final int QUERY_BIND_PROBABILITY_PCT = 20;
    // Name of the query (SQL) worker pool. Its worker threads are named
    // QUERY_POOL_NAME + '_' + workerId, which the FILE fault's query-execution
    // scope matches to admit reduce workers alongside the test thread.
    private static final String QUERY_POOL_NAME = "fuzzQuery";
    private static final boolean[] SERIAL_FLAGS = new boolean[8];
    // Per-query chance, in percent, of running with parallel SQL execution disabled.
    private static final int SERIAL_PROBABILITY_PCT = 5;

    @Test
    public void testBinomialLowerTail() {
        // Exact rational arithmetic computed the expected values, outside the code under test.
        assertBinomialLowerTail(2.84302880299297e-06, 0, 25);
        assertBinomialLowerTail(5.022684218620914e-05, 1, 25);
        assertBinomialLowerTail(2.7766914642564675e-04, 2, 26);
        assertBinomialLowerTail(1.5917802375868418e-03, 3, 26);
        assertBinomialLowerTail(9.416824245541328e-06, 41, 170);
        assertBinomialLowerTail(1.9811797371565857e-05, 42, 170);
        // The first term, 0.6^2000, underflows a double, and the terms near k carry the tail.
        assertBinomialLowerTail(2.371628584995946e-06, 700, 2_000);
        assertBinomialLowerTail(0.5097100085964106, 800, 2_000);
        Assert.assertEquals(0.0, QueryFuzzDriver.binomialLowerTail(0, 2_000, 0.4), 0.0);
        Assert.assertEquals(1.0, QueryFuzzDriver.binomialLowerTail(26, 26, 0.4), 1e-12);
    }

    @Test
    public void testCancelFaultOverStackedJoin() throws Exception {
        // The CANCEL fault cancels a stacked parallel hash join at one of the first 1,023 checks of
        // its circuit breaker: in its open, its builds, its probe tasks or the rows it returns. The
        // cancelled query fails with the cancellation and leaves no native memory or file behind,
        // and the query runs once the breaker is gone, which the runner's oracles check.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT (x % 50)::INT k, (x % 7)::LONG lk, x v, timestamp_sequence('2024-01-01', 60_000_000) ts FROM long_sequence(2_000)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("CREATE TABLE d AS (SELECT (x % 40)::INT k, x w FROM long_sequence(600))");
            execute("CREATE TABLE e AS (SELECT (x % 7)::LONG lk, x ew FROM long_sequence(30))");
            sqlExecutionContext.setParallelHashJoinEnabled(true);
            final String sql = "SELECT t.v, d.w, e.ew FROM t LEFT JOIN d ON t.k = d.k JOIN e ON t.lk = e.lk";
            // The stack runs as one parallel hash join of two stages, which the cancellations then meet.
            try (RecordCursorFactory factory = select(sql)) {
                final String plan = ParallelHashJoinTestUtils.plan(factory, sqlExecutionContext);
                TestUtils.assertContains(plan, "Async Hash Join workers:");
                TestUtils.assertContains(plan, "stages: 2");
            }
            final SqlExecutionCircuitBreaker contextBreaker = sqlExecutionContext.getCircuitBreaker();
            final QueryRunner runner = new QueryRunner(engine, sqlExecutionContext, false, false, false, false, false, null, false, new ObjList<>(), null);
            Assert.assertTrue(runner.isFaultTypeAvailable(FaultType.CANCEL));
            final Rnd rnd = new Rnd(0x5eed, 0xca9ce1);
            for (int i = 0; i < 40; i++) {
                final QueryRunner.Result result = runner.runFault(new GeneratedQuery(sql, true), FaultType.CANCEL, rnd, false);
                Assert.assertFalse(result.getFailure() != null ? result.getFailure().toString() : "", result.isFailed());
                Assert.assertFalse(result.isSkipped());
            }
            Assert.assertEquals(40, runner.getFaultsArmed(FaultType.CANCEL));
            // The query checks its breaker at each of the matches of a key that it returns, some
            // hundred thousand, so every cancellation lands before its last check.
            Assert.assertEquals(40, runner.getFaultsFired(FaultType.CANCEL));
            // Under a LIMIT of three rows the query makes a few checks only: the cancellations that
            // the breaker makes within them fail the query, and it returns its three rows under the
            // rest, which come after its last check or never.
            for (int i = 0; i < 40; i++) {
                final QueryRunner.Result result = runner.runFault(new GeneratedQuery(sql + " LIMIT 3", true), FaultType.CANCEL, rnd, false);
                Assert.assertFalse(result.getFailure() != null ? result.getFailure().toString() : "", result.isFailed());
                Assert.assertFalse(result.isSkipped());
            }
            Assert.assertEquals(80, runner.getFaultsArmed(FaultType.CANCEL));
            final int firedUnderLimit = runner.getFaultsFired(FaultType.CANCEL) - 40;
            Assert.assertTrue(String.valueOf(firedUnderLimit), firedUnderLimit > 0 && firedUnderLimit < 40);
            // The fault takes its breaker off the context after each query.
            Assert.assertSame(contextBreaker, sqlExecutionContext.getCircuitBreaker());
            // The run's generator never draws the fault, which runs next to the others.
            for (int i = 0; i < 64; i++) {
                Assert.assertNotEquals(FaultType.CANCEL, runner.chooseFaultType(rnd));
            }
        });
    }

    // The concurrent run over small tables, four in five of whose queries are equi-joins: several
    // owners at once run the parallel hash join, alone, stacked and swapped, on a few shared query
    // workers and one set of reduce queues, next to each other and to the other shapes. Each owner
    // compares every one of its equi-joins with the ordinary plan, in order, reads its cursor part
    // of the way and executes its factory again. The tuple ring's entries, the build rounds, the
    // slots that the workers and the stealing owners take and the SYMBOL key caches then meet the
    // interleavings that a run of one owner never has.
    @Test
    public void testConcurrentFiberEquiJoinFuzz() throws Exception {
        runConcurrentFiberFuzz(true);
    }

    @Test
    public void testConcurrentFiberQueryFuzz() throws Exception {
        runConcurrentFiberFuzz(false);
    }

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

            QueryRunner runner = new QueryRunner(engine, sqlExecutionContext, false, false, false, false, false, null, true, new ObjList<>(), null);

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
    public void testFusedShareGuard() {
        // A working planner drew 2 fused of 26 completed comparisons on CI, which the old 15% floor
        // failed. Every share at or above the baseline passes.
        Assert.assertFalse(QueryFuzzDriver.isFusedShareImplausible(2, 26));
        Assert.assertFalse(QueryFuzzDriver.isFusedShareImplausible(1, 26));
        Assert.assertFalse(QueryFuzzDriver.isFusedShareImplausible(11, 26));
        Assert.assertFalse(QueryFuzzDriver.isFusedShareImplausible(26, 26));
        // A planner that stops fusing fails at the smallest guarded sample.
        Assert.assertTrue(QueryFuzzDriver.isFusedShareImplausible(0, QueryFuzzDriver.MIN_SHAPE_QUERIES_FOR_ACCEPT_FLOOR));
        Assert.assertFalse(QueryFuzzDriver.isFusedShareImplausible(1, QueryFuzzDriver.MIN_SHAPE_QUERIES_FOR_ACCEPT_FLOOR));
        // A soak's larger sample holds the share to more: 41 of 170 (24%) fails and 42 passes.
        Assert.assertTrue(QueryFuzzDriver.isFusedShareImplausible(41, 170));
        Assert.assertFalse(QueryFuzzDriver.isFusedShareImplausible(42, 170));
    }

    @Test
    public void testFpTolerantRowSetComparison() {
        final boolean[] fpMask = {false, true};
        // Last-digit drift in the FP column, rows in a different order.
        Assert.assertTrue(QueryRunner.rowsetEqualsWithFpTolerance(
                line("a", "1.0") + '\n' + line("b", "2.0") + '\n',
                line("b", "2.0000000000000004") + '\n' + line("a", "1.0000000000000002") + '\n',
                fpMask
        ));
        // An exact cell differs.
        Assert.assertFalse(QueryRunner.rowsetEqualsWithFpTolerance(
                line("a", "1.0") + '\n' + line("b", "2.0") + '\n',
                line("a", "1.0") + '\n' + line("c", "2.0") + '\n',
                fpMask
        ));
        // The FP cell differs by more than drift.
        Assert.assertFalse(QueryRunner.rowsetEqualsWithFpTolerance(
                line("a", "1.0") + '\n',
                line("a", "1.5") + '\n',
                fpMask
        ));
        // Different row counts.
        Assert.assertFalse(QueryRunner.rowsetEqualsWithFpTolerance(
                line("a", "1.0") + '\n',
                line("a", "1.0") + '\n' + line("a", "1.0") + '\n',
                fpMask
        ));
        // Rows that share their exact cells pair up by their FP cells, in any order...
        Assert.assertTrue(QueryRunner.rowsetEqualsWithFpTolerance(
                line("a", "1.0") + '\n' + line("a", "2.0") + '\n',
                line("a", "2.0000000000000004") + '\n' + line("a", "1.0000000000000002") + '\n',
                fpMask
        ));
        // ...and each row pairs once: two different rows cannot both take the same row.
        Assert.assertFalse(QueryRunner.rowsetEqualsWithFpTolerance(
                line("a", "1.0") + '\n' + line("a", "2.0") + '\n',
                line("a", "1.0") + '\n' + line("a", "1.0") + '\n',
                fpMask
        ));
    }

    // Comparing every row with every other row took over a minute at 125k rows and stalled
    // testConcurrentFiberQueryFuzz. The timeout turns a return of that into a failure, not a hang.
    @Test(timeout = 120_000)
    public void testFpTolerantRowSetComparisonOfLargeResult() {
        final int rowCount = 300_000;
        final StringSink forward = new StringSink();
        final StringSink reversed = new StringSink();
        for (int i = 0; i < rowCount; i++) {
            forward.put(line("k" + i, "1.0")).put('\n');
            reversed.put(line("k" + (rowCount - 1 - i), "1.0000000000000002")).put('\n');
        }
        Assert.assertTrue(QueryRunner.rowsetEqualsWithFpTolerance(forward, reversed, new boolean[]{false, true}));
        // One exact cell off in the last row.
        reversed.put(line("k0", "1.0")).put('\n');
        forward.put(line("k1", "1.0")).put('\n');
        Assert.assertFalse(QueryRunner.rowsetEqualsWithFpTolerance(forward, reversed, new boolean[]{false, true}));
    }

    @Test
    public void testParallelJoinShareGuard() {
        // A working planner runs the operator on about 70% of the completed comparisons; a share at
        // or above the baseline passes, and so does a sample at the measured share's low end.
        Assert.assertFalse(QueryFuzzDriver.isParallelJoinShareImplausible(13, 25));
        Assert.assertFalse(QueryFuzzDriver.isParallelJoinShareImplausible(19, 27));
        Assert.assertFalse(QueryFuzzDriver.isParallelJoinShareImplausible(5, 25));
        // A planner that stops running the operator fails at the smallest guarded sample.
        Assert.assertTrue(QueryFuzzDriver.isParallelJoinShareImplausible(0, MIN_SHAPE_QUERIES_FOR_ACCEPT_FLOOR));
        Assert.assertTrue(QueryFuzzDriver.isParallelJoinShareImplausible(2, MIN_SHAPE_QUERIES_FOR_ACCEPT_FLOOR));
        // A soak's larger sample holds the share to more.
        Assert.assertTrue(QueryFuzzDriver.isParallelJoinShareImplausible(200, 589));
        Assert.assertFalse(QueryFuzzDriver.isParallelJoinShareImplausible(260, 589));
    }

    @Test
    public void testStackedShareGuard() {
        // A working planner stacks about 38% of the operator's runs; a share at or above the
        // baseline passes, and so does a sample at the measured share's low end.
        Assert.assertFalse(QueryFuzzDriver.isStackedShareImplausible(8, 25));
        Assert.assertFalse(QueryFuzzDriver.isStackedShareImplausible(155, 383));
        Assert.assertFalse(QueryFuzzDriver.isStackedShareImplausible(2, 25));
        // A planner that stops stacking passes a small sample and fails a larger one.
        Assert.assertFalse(QueryFuzzDriver.isStackedShareImplausible(0, MIN_SHAPE_QUERIES_FOR_ACCEPT_FLOOR));
        Assert.assertFalse(QueryFuzzDriver.isStackedShareImplausible(0, 32));
        Assert.assertTrue(QueryFuzzDriver.isStackedShareImplausible(0, 33));
        // A soak's larger sample holds the share to more.
        Assert.assertTrue(QueryFuzzDriver.isStackedShareImplausible(60, 383));
        Assert.assertFalse(QueryFuzzDriver.isStackedShareImplausible(90, 383));
    }

    @Test
    public void testSeparatorCellsKeepFpToleranceAligned() {
        // Found by the fused on/off axis: in SELECT (l.lk)::CHAR e0, count(*), stddev_samp(...),
        // count(), count(r.k) FROM l LEFT JOIN r ON l.vk = r.vk, one group's CHAR key was code 9. Printed raw, the tab split its row into one more cell, so
        // the stddev_samp cell met the count column's exact rule, and its reduction-order drift
        // failed the query. The materialized cells escape their separators, so the drift is tolerated.
        final boolean[] fpMask = {false, false, true, false, false};
        for (String key : new String[]{"\t", "\n", "\r", "\\", "\t\n", "a"}) {
            Assert.assertTrue(key, QueryRunner.rowEqualsWithFpTolerance(
                    line(key, "74", "35.52387071528208", "74", "71"),
                    line(key, "74", "35.52387071528207", "74", "71"),
                    fpMask
            ));
            // The count cells still compare exactly.
            Assert.assertFalse(key, QueryRunner.rowEqualsWithFpTolerance(
                    line(key, "74", "35.52387071528208", "74", "71"),
                    line(key, "74", "35.52387071528208", "74", "72"),
                    fpMask
            ));
        }
        // An escaped separator stays distinct from the text of its escape.
        Assert.assertNotEquals(line("\t"), line("\\t"));
        Assert.assertNotEquals(line("\\", "t"), line("\t"));
    }

    @Test
    public void testHorizonJoinFloatSumStorageReductionOrderToleratedByOracle() throws Exception {
        // Bug from multi-table HORIZON JOIN fuzzing (storage diff): a non-keyed
        // HORIZON JOIN that sums a FLOAT slave column diverged between a native
        // master/slave and a parquet shadow - native returned 374.97897, parquet
        // 374.98038 (one row each, ~32x FLOAT epsilon). The parallel non-keyed path
        // accumulates a per-worker partial FLOAT sum per frame and merges the
        // partials (SumFloatGroupByFunction.merge is FLOAT + FLOAT) in an order set
        // by the worker/frame partition. Native page frames and parquet row groups
        // have different boundaries, so the partials - and the merge order - differ,
        // and FLOAT addition is not associative. The result is reduction-order noise,
        // not a row-set difference: count(p.c6) and sum(p.c6::double) are bit-identical
        // across the two storages, so every storage layout sums the same multiset of
        // FLOAT values; only the single-precision accumulation order differs. The
        // oracle's FLOAT tolerance was just under the drift on this path.
        //
        // Two regression guards below:
        //  1. Engine row-set identity (deterministic): with parallel HORIZON JOIN off,
        //     the single-threaded float sum is storage-independent (master-driven
        //     accumulation order), so native and parquet agree bit for bit on count,
        //     the FLOAT sum, and the DOUBLE sum. This confirms the frame row-set is
        //     identical - not an off-by-one frame boundary - which is the premise
        //     for tolerating the parallel divergence.
        //  2. Oracle tolerance: the refined per-cell FLOAT tolerance accepts the
        //     reported divergence and still rejects a real one.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE master_n (ts TIMESTAMP, sym SYMBOL) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("CREATE TABLE slave_n (ts TIMESTAMP, sym SYMBOL, c6 FLOAT) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("CREATE TABLE master_p (ts TIMESTAMP, sym SYMBOL) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("CREATE TABLE slave_p (ts TIMESTAMP, sym SYMBOL, c6 FLOAT) TIMESTAMP(ts) PARTITION BY HOUR");
            // Master every 20s, slave every 3s, so each RANGE FROM -4m TO 6m STEP 2m
            // offset finds an ASOF match and the sum runs over many FLOAT terms.
            execute("INSERT INTO master_n SELECT generate_series, rnd_symbol('a','b','c') " +
                    "FROM generate_series('2024-01-01', '2024-01-01T02', '20s')");
            execute("INSERT INTO slave_n SELECT generate_series, rnd_symbol('a','b','c'), rnd_float(8) " +
                    "FROM generate_series('2024-01-01', '2024-01-01T02', '3s')");
            // Identical data into the parquet shadow, then convert.
            execute("INSERT INTO master_p SELECT * FROM master_n");
            execute("INSERT INTO slave_p SELECT * FROM slave_n");
            execute("ALTER TABLE master_p CONVERT PARTITION TO PARQUET WHERE ts >= 0");
            execute("ALTER TABLE slave_p CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            final boolean savedParallelHorizonJoin = sqlExecutionContext.isParallelHorizonJoinEnabled();
            sqlExecutionContext.setParallelHorizonJoinEnabled(false);
            try {
                final String q = "SELECT count(p.c6) AS n, sum(p.c6) AS a0, sum(p.c6::double) AS d " +
                        "FROM %s t HORIZON JOIN %s p ON (t.sym = p.sym) RANGE FROM -4m TO 6m STEP 2m AS h";
                final StringSink nSink = new StringSink();
                final StringSink pSink = new StringSink();
                printSql(String.format(q, "master_n", "slave_n"), nSink);
                printSql(String.format(q, "master_p", "slave_p"), pSink);
                TestUtils.assertEquals("native vs parquet row-set must be identical", nSink, pSink);
            } finally {
                sqlExecutionContext.setParallelHorizonJoinEnabled(savedParallelHorizonJoin);
            }

            // The reported parallel storage divergence (~32x FLOAT epsilon, identical
            // row set) is now tolerated; the pre-fix bottom-of-binade ulp * 32 bound
            // rejected it (374.98 sits low in the [256, 512) binade, where the bare ulp
            // runs ~1.5x tight), the relative FLOAT epsilon * 64 bound covers it. The
            // single-column mask marks the cell FP-typed, so the tolerance applies.
            final boolean[] fpMask = {true};
            final boolean[] exactMask = {false};
            Assert.assertTrue("reported FLOAT-sum reduction-order drift must be tolerated",
                    QueryRunner.rowEqualsWithFpTolerance("374.97897", "374.98038", fpMask));
            // A real row-set divergence - e.g. one storage dropped a whole FLOAT term -
            // shifts the sum by thousands of FLOAT epsilons, far beyond reduction noise,
            // and must still be flagged.
            Assert.assertFalse("a real FLOAT-sum divergence must still be flagged",
                    QueryRunner.rowEqualsWithFpTolerance("374.97897", "375.51", fpMask));
            // An integer COUNT/SUM column must compare exactly: a one-unit divergence
            // at scale (1000000 vs 1000001) is within the relative FLOAT tolerance
            // (floatEps ~ 7.6 at this magnitude) and would be masked if the tolerance
            // applied. Gating to FP columns flags it instead.
            Assert.assertFalse("an integer-column divergence at scale must be flagged",
                    QueryRunner.rowEqualsWithFpTolerance("1000000", "1000001", exactMask));
            // The same drift on an FP-typed column is still tolerated, confirming the
            // gate keys on column type rather than magnitude.
            Assert.assertTrue("FP-column reduction drift at scale stays tolerated",
                    QueryRunner.rowEqualsWithFpTolerance("1000000", "1000001", fpMask));

            // The envelope is relative to the MAGNITUDE of the result, so a term much smaller than
            // the sum fits inside it: for {1_000_000, 1_000_000, 1_000_000, 9} the tolerance at 3e6
            // is roughly 23, and losing the 9 costs nothing. It cannot be tightened away either - a
            // sum over a FLOAT column is typed DOUBLE, so the metadata cannot tell a
            // float-precision accumulation from a double-precision one, and every FP aggregate has
            // to keep the FLOAT-sized envelope.
            // Characterizing the envelope as it stands, not requiring it: if the tolerance is ever
            // tightened this assertion is the one to update.
            Assert.assertTrue("the FP envelope currently absorbs a term far below the sum's magnitude",
                    QueryRunner.rowEqualsWithFpTolerance("3000009", "3000000", fpMask));
            // Which is why JoinClauseSupport.appendRowSetGuard projects an exact count of the
            // matched slave rows next to every join aggregate: the count is an integer column,
            // compared exactly, so the dropped row that hid in the sum is flagged by its neighbour.
            // It does not catch a same-cardinality shift (one row dropped, one admitted) - that
            // needs an absolute oracle.
            final boolean[] sumAndCountMask = {true, false};
            Assert.assertFalse("the exact matched-row count must flag the dropped term",
                    QueryRunner.rowEqualsWithFpTolerance("3000009\t4", "3000000\t3", sumAndCountMask));
            // ... while a genuine reduction-order difference over the SAME rows stays accepted, so
            // the guard costs no tolerance.
            Assert.assertTrue("reduction-order drift over an identical row set stays tolerated",
                    QueryRunner.rowEqualsWithFpTolerance("3000009\t4", "3000009.5\t4", sumAndCountMask));

            // The mask a reconcile uses must be the AND of the two projections it compares,
            // not whichever side ran last. On the bind axis the two projections are not
            // guaranteed identical - bind values are bound as STRINGs, so the bind form's
            // overload resolution can type a projection column DOUBLE where the literal form
            // types it INT. Reading the mask from the bind side alone would hand that integer
            // column the FP tolerance and silently absorb the one-unit divergence asserted
            // above.
            final boolean[] literalMask = {false, true};
            final boolean[] bindMask = {true, true};
            final boolean[] reconcileMask = QueryRunner.intersectFpColumnMasks(literalMask, bindMask);
            Assert.assertFalse("a column either side types as an integer must compare exactly", reconcileMask[0]);
            Assert.assertTrue("a column both sides type as FP keeps the tolerance", reconcileMask[1]);
            Assert.assertFalse("an integer column must not inherit the other side's FP tolerance",
                    QueryRunner.rowEqualsWithFpTolerance("1000000\t1.0", "1000001\t1.0", reconcileMask));
            Assert.assertTrue("the same drift on the shared FP column stays tolerated",
                    QueryRunner.rowEqualsWithFpTolerance("5\t1000000", "5\t1000001", reconcileMask));
        });
    }

    @Test
    public void testHorizonJoinIndexedMasterToleratedByOracle() throws Exception {
        // Bug from multi-table HORIZON JOIN fuzzing (storage diff): the
        // single-threaded HORIZON JOIN path requires the master to support random
        // access (it revisits master rows in sorted order). When the master filter
        // is fully served by a POSTING covering index, the master access path is a
        // bare CoveringIndex, which does not support random access, so the LHS is
        // rejected at compile time with "left-hand side of HORIZON JOIN can only be
        // a table with an optional filter". The non-indexed shadow sibling
        // full-scans, supports random access, and compiles, returning rows. The two
        // storage configs therefore diverge structurally (one compiles, one
        // rejects), not in data. This is the same planner-sensitivity asymmetry the
        // oracle already tolerates for the SPLICE/ASOF index-vs-scan rejections, so
        // isPlannerSensitivityAsymmetry must classify it as a skip rather than a
        // divergence. Drive the diverging pair straight through QueryRunner to
        // exercise the real storage-axis classification path.
        //
        // Parallel HORIZON JOIN is disabled below so the planner takes the
        // single-threaded path deterministically - the same path the engine uses
        // whenever it has no shared query workers - regardless of how many workers
        // the test pool happens to advertise. (With parallel HORIZON JOIN on, the
        // covering index supplies a page-frame cursor and the parallel path
        // accepts it; the rejection is specific to the single-threaded path.)
        final String horizonJoin = "SELECT (h.offset / 1_000_000) AS e0, count(p.c5) AS a0 " +
                "FROM master_%s t " +
                "HORIZON JOIN slave_%s p LIST (-3m, -2m, -1m, 0m) AS h " +
                "WHERE t.sym IS NULL " +
                "GROUP BY e0 ORDER BY e0";
        assertMemoryLeak(() -> {
            // Primary master: sym not indexed -> page-frame full scan, random access.
            execute("CREATE TABLE master_p (ts TIMESTAMP, sym SYMBOL, c2 FLOAT) TIMESTAMP(ts) PARTITION BY HOUR");
            // Shadow master: identical rows, sym POSTING-EF indexed covering c2. The
            // sym IS NULL filter is served entirely by the index, so the master
            // access path is a bare CoveringIndex with no random access.
            execute("CREATE TABLE master_s (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING EF INCLUDE (c2), c2 FLOAT) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("CREATE TABLE slave_p (ts TIMESTAMP, sym SYMBOL, c5 LONG) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("CREATE TABLE slave_s (ts TIMESTAMP, sym SYMBOL, c5 LONG) TIMESTAMP(ts) PARTITION BY HOUR");
            // Some master rows carry a NULL sym so the WHERE keeps them.
            final String masterRows = " VALUES ('2024-01-01T00:00:00.000000Z', NULL, 1.0), " +
                    "('2024-01-01T00:30:00.000000Z', 'a', 2.0), " +
                    "('2024-01-01T01:00:00.000000Z', NULL, 3.0)";
            execute("INSERT INTO master_p" + masterRows);
            execute("INSERT INTO master_s" + masterRows);
            final String slaveRows = " VALUES ('2024-01-01T00:00:00.000000Z', 'a', 10), " +
                    "('2024-01-01T00:30:00.000000Z', 'b', 20)";
            execute("INSERT INTO slave_p" + slaveRows);
            execute("INSERT INTO slave_s" + slaveRows);

            final boolean savedParallelHorizonJoin = sqlExecutionContext.isParallelHorizonJoinEnabled();
            sqlExecutionContext.setParallelHorizonJoinEnabled(false);
            try {
                // Sanity: the indexed (shadow) master rejects the HORIZON JOIN LHS
                // at compile time...
                try (RecordCursorFactory ignore = engine.select(String.format(horizonJoin, "s", "s"), sqlExecutionContext)) {
                    Assert.fail("indexed master must reject the HORIZON JOIN LHS");
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(),
                            "left-hand side of HORIZON JOIN can only be a table with an optional filter");
                }
                // ...while the non-indexed (primary) master compiles and returns rows.
                try (RecordCursorFactory factory = engine.select(String.format(horizonJoin, "p", "p"), sqlExecutionContext)) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        int rows = 0;
                        while (cursor.hasNext()) {
                            rows++;
                        }
                        Assert.assertEquals(4, rows);
                    }
                }

                // Wire the storage diff to rewrite master_p -> master_s, slave_p -> slave_s.
                final FuzzTable masterShadow = new FuzzTable("master_s", new ObjList<>(), "ts");
                final FuzzTable slaveShadow = new FuzzTable("slave_s", new ObjList<>(), "ts");
                final FuzzTable master = new FuzzTable("master_p", new ObjList<>(), "ts", FuzzTableFactory.ParquetMode.NONE, null, null, masterShadow);
                final FuzzTable slave = new FuzzTable("slave_p", new ObjList<>(), "ts", FuzzTableFactory.ParquetMode.NONE, null, null, slaveShadow);
                final ObjList<FuzzTable> tables = new ObjList<>();
                tables.add(master);
                tables.add(slave);

                // The storage-axis oracle tolerates the compile/reject asymmetry
                // rather than reporting it as a divergence. run() swallows a
                // tolerated per-axis skip into an overall ok result and only
                // surfaces a failed axis, so the regression assertion is that the
                // run is not failed; without isPlannerSensitivityAsymmetry the
                // storage axis would report a divergence and the run would fail.
                final QueryRunner runner = new QueryRunner(engine, sqlExecutionContext, false, true, false, false, false, null, false, tables, null);
                final QueryRunner.Result result = runner.run(new GeneratedQuery(String.format(horizonJoin, "p", "p"), true));
                Assert.assertFalse(
                        "storage divergence must be tolerated, not failed: "
                                + (result.getFailure() != null ? result.getFailure().getMessage() : ""),
                        result.isFailed());
            } finally {
                sqlExecutionContext.setParallelHorizonJoinEnabled(savedParallelHorizonJoin);
            }
        });
    }

    @Test
    public void testQueryMemoryLimitFaultOverStackedJoin() throws Exception {
        // The QUERY_MEMORY_LIMIT fault puts a stacked parallel hash join under per-query memory limits
        // from 512 bytes to 4 MiB: the limits that its builds, tuple ring and payload breach fail the
        // query with the limit's error and leave no native memory or file behind, and the query runs
        // once the limit is lifted, which the runner's oracles check.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT (x % 50)::INT k, (x % 7)::LONG lk, x v, timestamp_sequence('2024-01-01', 60_000_000) ts FROM long_sequence(2_000)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("CREATE TABLE d AS (SELECT (x % 40)::INT k, x w FROM long_sequence(600))");
            execute("CREATE TABLE e AS (SELECT (x % 7)::LONG lk, x ew FROM long_sequence(30))");
            sqlExecutionContext.setParallelHashJoinEnabled(true);
            final String sql = "SELECT t.v, d.w, e.ew FROM t LEFT JOIN d ON t.k = d.k JOIN e ON t.lk = e.lk";
            // The stack runs as one parallel hash join of two stages, which the limits then meet.
            try (RecordCursorFactory factory = select(sql)) {
                final String plan = ParallelHashJoinTestUtils.plan(factory, sqlExecutionContext);
                TestUtils.assertContains(plan, "Async Hash Join workers:");
                TestUtils.assertContains(plan, "stages: 2");
            }
            final QueryRunner runner = new QueryRunner(engine, sqlExecutionContext, false, false, false, false, false, null, node1.getConfigurationOverrides(), false, new ObjList<>(), null);
            Assert.assertTrue(runner.isFaultTypeAvailable(FaultType.QUERY_MEMORY_LIMIT));
            final Rnd rnd = new Rnd(0x5eed, 0x9e1);
            for (int i = 0; i < 40; i++) {
                final QueryRunner.Result result = runner.runFault(new GeneratedQuery(sql, true), FaultType.QUERY_MEMORY_LIMIT, rnd, false);
                Assert.assertFalse(result.getFailure() != null ? result.getFailure().toString() : "", result.isFailed());
                Assert.assertFalse(result.isSkipped());
            }
            Assert.assertEquals(40, runner.getFaultsArmed(FaultType.QUERY_MEMORY_LIMIT));
            // Most limits fall below what the stack takes, and the rest leave it to complete.
            final int fired = runner.getFaultsFired(FaultType.QUERY_MEMORY_LIMIT);
            Assert.assertTrue(String.valueOf(fired), fired > 10 && fired < 40);
            // The fault lifts the limit after each query.
            Assert.assertEquals(0, engine.getConfiguration().getQueryMemoryLimitBytes());
            // Without the configuration's overrides, the runner offers no such fault.
            Assert.assertFalse(new QueryRunner(engine, sqlExecutionContext, false, false, false, false, false, null, false, new ObjList<>(), null)
                    .isFaultTypeAvailable(FaultType.QUERY_MEMORY_LIMIT));
        });
    }

    @Test
    public void testDecimalScaleAlignedComparisonNotSkipped() throws Exception {
        // Comparing two decimals of different scale used to raise a bare
        // NumericException whenever aligning the smaller-scale operand left the
        // type's range, and the oracle swallowed it as an accepted skip.
        // Decimal64/128/256.compareTo now derives the ordering from the sign, so
        // the query completes. The oracle no longer accepts a bare
        // NumericException either, so a regression is reported as a failure
        // rather than skipped. Drive the query through QueryRunner.run() to
        // exercise the real classification path.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE cmp (a DECIMAL(76, 0), b DECIMAL(76, 1))");
            // 76 nines is the DECIMAL(76, 0) maximum; scaling it up by 10 to
            // meet b's scale is past what Decimal256 can hold.
            final String nines = "9".repeat(76);
            execute("INSERT INTO cmp VALUES (" + nines + "m, 1.0m), (-" + nines + "m, 1.0m)");

            QueryRunner runner = new QueryRunner(engine, sqlExecutionContext, false, false, false, false, false, null, true, new ObjList<>(), null);
            QueryRunner.Result result = runner.run(
                    new GeneratedQuery("SELECT count() c FROM cmp WHERE a > b", true));
            Assert.assertFalse("comparison must not be skipped: " + result.getSkipReason(), result.isSkipped());
            Assert.assertFalse("comparison must not fail: "
                    + (result.getFailure() != null ? result.getFailure().getMessage() : ""), result.isFailed());
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

    // Pins the first window of the parallel hash join's executions whose builds fan out, at the
    // default cap of a probe frame's 100,000 tuples per task. Frames of two rows and a ring budget of 192 bytes
    // give a stack of up to five joins a first window of two frames or more, up to the ring's
    // entries, which follow the host's query workers under the default dispatch limit; an execution
    // whose workers walk duplicate matches instead counts each task's tuples as the cap, and starts
    // at one frame. A pinned seed's queries over native probe frames start there, and every axis
    // compares their rows with the ordinary plan's.
    @Test
    public void testParallelJoinCapBoundFirstWindowAxis() throws Exception {
        withSystemProperties(
                new String[][]{
                        {FuzzConfig.QUERIES_PROP, "300"},
                        {"questdb.fuzz.s0", "3"},
                        {"questdb.fuzz.s1", "100"},
                        {FuzzConfig.PARALLEL_JOIN_FRAMES_PROP, "2"},
                        {FuzzConfig.PARALLEL_JOIN_PROBE_FRAMES_PROP, "0"},
                        {FuzzConfig.PARALLEL_JOIN_RING_PROP, "192"},
                        {FuzzConfig.PARALLEL_JOIN_DISPATCH_PROP, "-1"},
                        {FuzzConfig.PARALLEL_JOIN_CAP_PROP, "0"},
                        {FuzzConfig.PARQUET_ROW_GROUP_PROP, "0"},
                },
                () -> runQueryFuzz(runner -> Assert.assertTrue(
                        "cap-bound first windows " + runner.getParallelJoinCapBoundFirstWindowRuns(),
                        runner.getParallelJoinCapBoundFirstWindowRuns() > 0
                ))
        );
    }

    // Pins what the frame pressure knobs make the generated equi-join queries do to the parallel
    // hash join: with frames of two rows and a ring budget of four such frames of one join, a pinned
    // seed's probes have more frames than the ring has entries, and the frames of duplicate matches
    // narrow the window, down to one frame. A cap of one tuple per task leaves frames to
    // continuations, and the partial reads of the cursor-verify axis then meet stacked, swapped and
    // continued executions, among them executions without a LIMIT whose first frame's task joined
    // its first row alone, as a third of the queries compile it, and whose consumer continued the
    // frame past it, as the window ramps up. A third of the queries run the stages' stepper copies
    // from the first task, and another third switch to them once their first execution collected a
    // frame, within
    // the first pass over the cursor for some: the copies run stacked joins in the query's
    // orientation, and swapped ones, stacked among them, on copies defined as they compiled. Half of
    // the queries open their cursors under the rows that a client reads first, which the parallel
    // hash join whose rows are the query's own takes.
    @Test
    public void testParallelJoinFramePressureAxis() throws Exception {
        withSystemProperties(
                new String[][]{
                        {FuzzConfig.QUERIES_PROP, "300"},
                        {"questdb.fuzz.s0", "135"},
                        {"questdb.fuzz.s1", "4192"},
                        {FuzzConfig.PARALLEL_JOIN_FRAMES_PROP, "2"},
                        {FuzzConfig.PARALLEL_JOIN_PROBE_FRAMES_PROP, "0"},
                        {FuzzConfig.PARALLEL_JOIN_RING_PROP, "192"},
                        {FuzzConfig.PARALLEL_JOIN_DISPATCH_PROP, "-1"},
                        {FuzzConfig.PARALLEL_JOIN_CAP_PROP, "1"},
                        {FuzzConfig.PARQUET_ROW_GROUP_PROP, "0"},
                },
                () -> runQueryFuzz(runner -> {
                    final String counts = "aliased " + runner.getParallelJoinAliasedRuns()
                            + ", single frame window " + runner.getParallelJoinSingleFrameWindowRuns()
                            + ", narrowed " + runner.getParallelJoinNarrowedWindowRuns()
                            + ", partial reads " + runner.getParallelJoinPartialReadChecks()
                            + " (stacked " + runner.getParallelJoinPartialReadStacked()
                            + ", swapped " + runner.getParallelJoinPartialReadSwapped()
                            + ", continued " + runner.getParallelJoinPartialReadContinued()
                            + ", past a first task's head " + runner.getParallelJoinPartialReadHeadContinued() + ")"
                            + ", stepper copies " + runner.getParallelJoinStepperCopyRuns()
                            + " (stacked " + runner.getParallelJoinStepperCopyStackedRuns()
                            + ", swapped " + runner.getParallelJoinStepperCopySwappedRuns()
                            + " of which stacked " + runner.getParallelJoinStepperCopySwappedStackedRuns()
                            + ", switched " + runner.getParallelJoinStepperSwitchRuns()
                            + " of which swapped " + runner.getParallelJoinStepperSwitchSwappedRuns()
                            + ", in the first pass " + runner.getParallelJoinStepperFirstPassSwitchRuns() + ")"
                            + ", first reads " + runner.getParallelJoinFirstReadRuns()
                            + ", wide tuples " + runner.getParallelJoinWideRuns()
                            + " (stacked " + runner.getParallelJoinWideStackedRuns()
                            + ", swapped " + runner.getParallelJoinWideSwappedRuns() + ")"
                            + ", factories executed again " + runner.getParallelJoinReexecutionChecks()
                            + " (after an execution left part of the way " + runner.getParallelJoinReexecutionAbandoned() + ")";
                    Assert.assertTrue(counts, runner.getParallelJoinAliasedRuns() > 0);
                    Assert.assertTrue(counts, runner.getParallelJoinSingleFrameWindowRuns() > 0);
                    Assert.assertTrue(counts, runner.getParallelJoinNarrowedWindowRuns() > 0);
                    Assert.assertTrue(counts, runner.getParallelJoinPartialReadStacked() > 0);
                    Assert.assertTrue(counts, runner.getParallelJoinPartialReadSwapped() > 0);
                    Assert.assertTrue(counts, runner.getParallelJoinPartialReadContinued() > 0);
                    Assert.assertTrue(counts, runner.getParallelJoinPartialReadHeadContinued() > 0);
                    Assert.assertTrue(counts, runner.getParallelJoinStepperCopyRuns() > 0);
                    Assert.assertTrue(counts, runner.getParallelJoinStepperSwitchRuns() > 0);
                    // Some queries define their copies as they compile, apart from those that switch.
                    Assert.assertTrue(counts, runner.getParallelJoinStepperCopyRuns() > runner.getParallelJoinStepperSwitchRuns());
                    Assert.assertTrue(counts, runner.getParallelJoinStepperCopyStackedRuns() > 0);
                    Assert.assertTrue(counts, runner.getParallelJoinStepperCopySwappedRuns() > 0);
                    Assert.assertTrue(counts, runner.getParallelJoinStepperCopySwappedStackedRuns() > 0);
                    Assert.assertTrue(counts, runner.getParallelJoinStepperFirstPassSwitchRuns() > 0);
                    Assert.assertTrue(counts, runner.getParallelJoinFirstReadRuns() > 0);
                    Assert.assertTrue(counts, runner.getParallelJoinWideStackedRuns() > 0);
                    Assert.assertTrue(counts, runner.getParallelJoinWideSwappedRuns() > 0);
                    // The factories of the joins ran again, some of them after an execution that
                    // closed with rows unread.
                    Assert.assertTrue(counts, runner.getParallelJoinReexecutionChecks() > 0);
                    Assert.assertTrue(counts, runner.getParallelJoinReexecutionAbandoned() > 0);
                })
        );
    }

    // Pins the probe frames of the parallel hash join under its own cap of a probe frame's rows, two
    // rows, while the build scans keep the context's frames: a pinned seed's probes have more frames
    // than the ring has entries, which the frames in flight then share, and the cursor-verify axis
    // reads them in part.
    @Test
    public void testParallelJoinProbeFramesAxis() throws Exception {
        withSystemProperties(
                new String[][]{
                        {FuzzConfig.QUERIES_PROP, "300"},
                        {"questdb.fuzz.s0", "48514"},
                        {"questdb.fuzz.s1", "630374"},
                        {FuzzConfig.PARALLEL_JOIN_FRAMES_PROP, "0"},
                        {FuzzConfig.PARALLEL_JOIN_PROBE_FRAMES_PROP, "2"},
                        {FuzzConfig.PARALLEL_JOIN_RING_PROP, "0"},
                        {FuzzConfig.PARALLEL_JOIN_DISPATCH_PROP, "-1"},
                        {FuzzConfig.PARALLEL_JOIN_CAP_PROP, "1"},
                        {FuzzConfig.PARQUET_ROW_GROUP_PROP, "0"},
                },
                () -> runQueryFuzz(runner -> {
                    final String counts = "aliased " + runner.getParallelJoinAliasedRuns()
                            + ", partial reads " + runner.getParallelJoinPartialReadChecks()
                            + " (continued " + runner.getParallelJoinPartialReadContinued() + ")";
                    Assert.assertTrue(counts, runner.getParallelJoinAliasedRuns() > 0);
                    Assert.assertTrue(counts, runner.getParallelJoinPartialReadChecks() > 0);
                    Assert.assertTrue(counts, runner.getParallelJoinPartialReadContinued() > 0);
                })
        );
    }

    // Pins the forms without a LIMIT of the generated equi-join queries, see EquiJoinClause: the joins
    // under an ASOF or LT join of which they are the master, under a UNION ALL and under a WHERE
    // that holds no column, which the rows that a client reads first reach through the operator,
    // as the parallel join axis compares the rows with the ordinary plan's. Frames of two rows leave
    // a short first read fewer frames to dispatch than the probe has. One query in twenty is an
    // equi-join, and one in about forty of those runs a form of an operator without a LIMIT under
    // the rows a client reads first: the seed draws four such executions in 600 queries, where
    // most seeds draw none or one.
    @Test
    public void testParallelJoinFirstReadFormsAxis() throws Exception {
        withSystemProperties(
                new String[][]{
                        {FuzzConfig.QUERIES_PROP, "600"},
                        {"questdb.fuzz.s0", "2"},
                        {"questdb.fuzz.s1", "69"},
                        {FuzzConfig.PARALLEL_JOIN_FRAMES_PROP, "2"},
                        {FuzzConfig.PARALLEL_JOIN_PROBE_FRAMES_PROP, "0"},
                        {FuzzConfig.PARQUET_ROW_GROUP_PROP, "0"},
                },
                () -> runQueryFuzz(runner -> Assert.assertTrue(
                        "first-read forms " + runner.getParallelJoinFirstReadFormRuns(),
                        runner.getParallelJoinFirstReadFormRuns() > 0
                ))
        );
    }

    // Pins the LIMIT forms of the generated equi-join queries, see EquiJoinClause: the LIMIT of a
    // sub-query or a CTE over the joins, of a UNION ALL, of an ASOF or LT join of which the joins are
    // the master, and of the joins under a WHERE that holds no column, which the parallel hash join
    // takes, as the parallel join axis compares the rows with the ordinary plan's. Frames of two rows
    // leave a small LIMIT fewer frames to dispatch than the probe has.
    @Test
    public void testParallelJoinLimitFormsAxis() throws Exception {
        withSystemProperties(
                new String[][]{
                        {FuzzConfig.QUERIES_PROP, "600"},
                        {"questdb.fuzz.s0", "205"},
                        {"questdb.fuzz.s1", "6362"},
                        {FuzzConfig.PARALLEL_JOIN_FRAMES_PROP, "2"},
                        {FuzzConfig.PARALLEL_JOIN_PROBE_FRAMES_PROP, "0"},
                        {FuzzConfig.PARQUET_ROW_GROUP_PROP, "0"},
                },
                () -> runQueryFuzz(runner -> Assert.assertTrue(
                        "LIMIT forms " + runner.getParallelJoinLimitFormRuns(),
                        runner.getParallelJoinLimitFormRuns() > 0
                ))
        );
    }

    // A ring budget below one frame's tuples leaves the ring one entry, and a dispatch limit of zero
    // reduces every frame on the owner. A query of the pinned seed swaps its first join's inputs and
    // defines the stages' stepper copies while it runs. The dispatch limit of zero holds for every
    // query, not only the equi-join band's, whose pressure would otherwise lift it to the default,
    // which follows the host's query workers: two on a two-core host, which leaves rings of two
    // entries that the frames of the other queries share.
    @Test
    public void testParallelJoinSingleEntryRingAxis() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 0);
        withSystemProperties(
                new String[][]{
                        {FuzzConfig.QUERIES_PROP, "300"},
                        {"questdb.fuzz.s0", "8"},
                        {"questdb.fuzz.s1", "255"},
                        {FuzzConfig.PARALLEL_JOIN_FRAMES_PROP, "3"},
                        {FuzzConfig.PARALLEL_JOIN_PROBE_FRAMES_PROP, "0"},
                        {FuzzConfig.PARALLEL_JOIN_RING_PROP, "1"},
                        {FuzzConfig.PARALLEL_JOIN_DISPATCH_PROP, "-1"},
                        {FuzzConfig.PARALLEL_JOIN_CAP_PROP, "0"},
                        {FuzzConfig.PARQUET_ROW_GROUP_PROP, "0"},
                },
                () -> runQueryFuzz(runner -> {
                    final String counts = "aliased " + runner.getParallelJoinAliasedRuns()
                            + ", single frame window " + runner.getParallelJoinSingleFrameWindowRuns()
                            + ", partial reads " + runner.getParallelJoinPartialReadChecks()
                            + ", stepper copies swapped and switched " + runner.getParallelJoinStepperSwitchSwappedRuns();
                    // A dispatch limit of zero leaves one entry, which no frame shares with another
                    // in flight.
                    Assert.assertEquals(counts, 0, runner.getParallelJoinAliasedRuns());
                    Assert.assertTrue(counts, runner.getParallelJoinSingleFrameWindowRuns() > 0);
                    Assert.assertTrue(counts, runner.getParallelJoinPartialReadChecks() > 0);
                    Assert.assertTrue(counts, runner.getParallelJoinStepperSwitchSwappedRuns() > 0);
                })
        );
    }

    // Pins the storage shapes of the primary tables (see FuzzStorage) under the parallel hash join: a
    // pinned seed's tables include a non-WAL and a non-partitioned table, column tops, in join keys
    // and in Parquet partitions of row groups of four rows, and a dropped column, and the parallel
    // join axis runs the operator over each of them, and the diff-shadow axis compares each with its
    // plain shadow.
    @Test
    public void testParallelJoinStorageAxes() throws Exception {
        withSystemProperties(
                new String[][]{
                        {FuzzConfig.QUERIES_PROP, "600"},
                        {"questdb.fuzz.s0", "15"},
                        {"questdb.fuzz.s1", "472"},
                        {FuzzConfig.STORAGE_PROP, "true"},
                        {FuzzConfig.PARQUET_ROW_GROUP_PROP, "4"},
                },
                () -> runQueryFuzz(runner -> {
                    final String counts = "column top keys " + runner.getParallelJoinOverColumnTopKeys()
                            + ", Parquet column tops " + runner.getParallelJoinOverParquetColumnTops()
                            + ", non-partitioned " + runner.getParallelJoinOverNonPartitioned()
                            + ", non-WAL " + runner.getParallelJoinOverNonWal()
                            + ", dropped columns " + runner.getParallelJoinOverDroppedColumns();
                    Assert.assertTrue(counts, runner.getParallelJoinOverColumnTopKeys() > 0);
                    Assert.assertTrue(counts, runner.getParallelJoinOverParquetColumnTops() > 0);
                    Assert.assertTrue(counts, runner.getParallelJoinOverNonPartitioned() > 0);
                    Assert.assertTrue(counts, runner.getParallelJoinOverNonWal() > 0);
                    Assert.assertTrue(counts, runner.getParallelJoinOverDroppedColumns() > 0);
                })
        );
    }

    @Test
    public void testQueryFuzz() throws Exception {
        runQueryFuzz(runner -> {
        });
    }

    @Test
    public void testResultPastTheRowCapSkipped() throws Exception {
        // A query stops being read at 250,000 rows, and is skipped: one key that every row of both
        // tables holds, as a NULL key in many rows of a fuzz table does, pairs every row with every
        // other. 501 x 500 rows, one row past the cap, skip on the JIT axis, whose runs read the
        // parallel hash join, and on the parallel join axis, which reads the ordinary plan too;
        // 500 x 500 rows, at the cap, compare as usual.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 AS (SELECT 1 k, x v FROM long_sequence(501))");
            execute("CREATE TABLE t1 AS (SELECT 1 k, x w FROM long_sequence(500))");
            final boolean wasParallelHashJoinEnabled = sqlExecutionContext.isParallelHashJoinEnabled();
            sqlExecutionContext.setParallelHashJoinEnabled(true);
            try {
                final QueryRunner runner = new QueryRunner(engine, sqlExecutionContext, true, false, false, true, false, null, false, new ObjList<>(), null);
                final String pastTheCap = "SELECT l.v, r.w FROM t0 l JOIN t1 r ON l.k = r.k";
                try (RecordCursorFactory factory = select(pastTheCap)) {
                    Assert.assertNotNull(pastTheCap, ParallelHashJoinTestUtils.find(factory));
                }
                final QueryRunner.Result skipped = runner.run(new GeneratedQuery(pastTheCap, true).withShape(QueryShape.EQUI_JOIN));
                Assert.assertFalse(skipped.getFailure() != null ? skipped.getFailure().getMessage() : "", skipped.isFailed());
                Assert.assertTrue(skipped.isSkipped());
                Assert.assertEquals("ResultTooLargeException: the query returned more than 250000 rows", skipped.getSkipReason());
                final QueryRunner.Result compared = runner.run(new GeneratedQuery(pastTheCap + " WHERE l.v < 501", true).withShape(QueryShape.EQUI_JOIN));
                Assert.assertFalse(compared.getFailure() != null ? compared.getFailure().getMessage() : "", compared.isFailed());
                Assert.assertFalse(compared.getSkipReason(), compared.isSkipped());
                Assert.assertEquals(2, runner.getParallelJoinAxisRuns());
                Assert.assertEquals(1, runner.getParallelJoinAxisCompletedRuns());
            } finally {
                sqlExecutionContext.setParallelHashJoinEnabled(wasParallelHashJoinEnabled);
            }
        });
    }

    @Test
    public void testUntypedLimitBindComparedAcrossPlans() throws Exception {
        // The bind form of an equi-join reads its LIMIT and an expression of its SELECT list through
        // the positional $1, which the runner leaves undefined at compile, as a PostgreSQL Parse
        // without parameter types does, and binds after it. The runner compares the bind form with
        // the parallel hash join on and off: $1's type and the columns' types, as the SELECT list's
        // INT key makes them, and the rows in order, for a LIMIT and a negative LIMIT.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 AS (SELECT (x % 4)::INT k, x v, timestamp_sequence('2024-01-01', 60_000_000) ts FROM long_sequence(64)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t1 AS (SELECT (x % 4)::INT k, x * 10 w FROM long_sequence(4))");
            final boolean wasParallelHashJoinEnabled = sqlExecutionContext.isParallelHashJoinEnabled();
            sqlExecutionContext.setParallelHashJoinEnabled(true);
            try {
                final QueryRunner runner = new QueryRunner(engine, sqlExecutionContext, false, false, false, true, false, null, false, new ObjList<>(), null);
                for (String sign : new String[]{"", "-"}) {
                    final String bindSql = "SELECT l.v AS c0, l.k + $1 AS c1 FROM t0 l JOIN t1 r ON l.k = r.k LIMIT " + sign + "$1";
                    // The bind form runs the parallel hash join.
                    bindVariableService.clear();
                    try (RecordCursorFactory factory = select(bindSql)) {
                        Assert.assertNotNull(bindSql, ParallelHashJoinTestUtils.find(factory));
                    } finally {
                        bindVariableService.clear();
                    }
                    final GeneratedQuery query = new GeneratedQuery(bindSql.replace("$1", "3"), false)
                            .withShape(QueryShape.EQUI_JOIN)
                            .withBind(bindSql, new ObjList<>(), new ObjList<>(), "3");
                    final QueryRunner.Result result = runner.run(query);
                    Assert.assertFalse(result.getFailure() != null ? result.getFailure().getMessage() : "", result.isFailed());
                    Assert.assertFalse(result.getSkipReason(), result.isSkipped());
                }
                Assert.assertEquals(2, runner.getUntypedLimitAxisRuns());
            } finally {
                sqlExecutionContext.setParallelHashJoinEnabled(wasParallelHashJoinEnabled);
            }
        });
    }

    private static void withSystemProperties(String[][] properties, ThrowingRunnable code) throws Exception {
        final String[] previous = new String[properties.length];
        for (int i = 0; i < properties.length; i++) {
            previous[i] = System.setProperty(properties[i][0], properties[i][1]);
        }
        try {
            code.run();
        } finally {
            for (int i = 0; i < properties.length; i++) {
                if (previous[i] == null) {
                    System.clearProperty(properties[i][0]);
                } else {
                    System.setProperty(properties[i][0], previous[i]);
                }
            }
        }
    }

    // Runs a batch of generated queries from several owner fibers at once, each on a context of
    // its own, over shared query workers. An equi-join run draws small tables and mostly equi-join
    // queries, see generateConcurrentQuery(), compares each with the ordinary plan, and holds the
    // planner to the shares of the parallel hash join that the run of one owner holds it to.
    private void runConcurrentFiberFuzz(boolean isEquiJoinRun) throws Exception {
        assertMemoryLeak(() -> {
            final Long s0 = Long.getLong("questdb.fuzz.s0");
            final Long s1 = Long.getLong("questdb.fuzz.s1");
            final Rnd rnd = s0 != null && s1 != null ? TestUtils.generateRandom(LOG, s0, s1) : TestUtils.generateRandom(LOG);
            final FuzzConfig config = isEquiJoinRun ? FuzzConfig.forConcurrentEquiJoins(rnd) : FuzzConfig.forConcurrency(rnd);
            final int queryWorkerCount = 1 + rnd.nextInt(2);
            final int ownerWorkerCount = 2 + rnd.nextInt(2);
            final int ownerCount = ownerWorkerCount * (2 + rnd.nextInt(3));
            LOG.info().$("concurrent fuzz config: tables=").$(config.getNumTables())
                    .$(", rows=").$(config.getRowsPerTable())
                    .$(", queries=").$(config.getNumQueries())
                    .$(", queryWorkers=").$(queryWorkerCount)
                    .$(", ownerWorkers=").$(ownerWorkerCount)
                    .$(", owners=").$(ownerCount)
                    .$();
            if (config.getHashJoinPayloadLayout() != null) {
                config.getHashJoinPayloadLayout().apply(node1.getConfigurationOverrides());
            }
            config.getHashJoinBuildMode().applyProperties(node1.getConfigurationOverrides());
            setProperty(PropertyKey.CAIRO_SQL_LATEST_BY_JIT_ENABLED, Boolean.toString(rnd.nextBoolean()));
            // The frames of a context's scans: an equi-join run cuts its small tables into frames of
            // a few rows, so that every join has many frames in flight, and takes the tuple cap, the
            // ring budget and the probe frames that the seed picked, as the run of one owner does.
            // It leaves the dispatch limit alone, which that run flips for each query.
            final int pageFrameRows;
            if (isEquiJoinRun) {
                pageFrameRows = config.getParallelJoinFrameMaxRows() > 0 ? config.getParallelJoinFrameMaxRows() : CONCURRENT_EQUI_JOIN_PAGE_FRAME_ROWS;
                if (config.getParallelJoinTaskOutputMaxRows() > 0) {
                    node1.setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, config.getParallelJoinTaskOutputMaxRows());
                }
                if (config.getParallelJoinRingMaxSize() > 0) {
                    node1.setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TUPLE_RING_MAX_SIZE, config.getParallelJoinRingMaxSize());
                }
                if (config.getParallelJoinProbeFrameMaxRows() > 0) {
                    node1.setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_PAGE_FRAME_MAX_ROWS, config.getParallelJoinProbeFrameMaxRows());
                }
                LOG.info().$("concurrent equi-join fuzz [pageFrameRows=").$(pageFrameRows)
                        .$(", parallelJoinCap=").$(config.getParallelJoinTaskOutputMaxRows())
                        .$(", parallelJoinRing=").$(config.getParallelJoinRingMaxSize())
                        .$(", parallelJoinProbeFrames=").$(config.getParallelJoinProbeFrameMaxRows())
                        .I$();
            } else {
                pageFrameRows = CONCURRENT_PAGE_FRAME_ROWS;
            }

            final WorkerPool queryPool = new TestWorkerPool(QUERY_POOL_NAME, queryWorkerCount, Metrics.DISABLED, WorkerPoolMode.FIBER_HOST);
            WorkerPoolUtils.setupQueryJobs(queryPool, engine, true);
            final WorkerPool writerPool = TestWorkerPool.createWithRandomMode(rnd, new WorkerPoolConfiguration() {
                @Override
                public String getPoolName() {
                    return "fuzzWriter";
                }

                @Override
                public int getWorkerCount() {
                    return 2;
                }
            });
            WorkerPoolUtils.setupWriterJobs(writerPool, engine);
            final WorkerPool ownerPool = new TestWorkerPool("fuzzOwner", ownerWorkerCount, Metrics.DISABLED, WorkerPoolMode.FIBER_HOST);
            queryPool.start(LOG);
            writerPool.start(LOG);
            ownerPool.start(LOG);
            final ObjList<SqlExecutionContextImpl> contexts = new ObjList<>();
            try {
                final SqlExecutionContextImpl buildContext = newFuzzContext(queryWorkerCount);
                contexts.add(buildContext);
                final ObjList<FuzzTable> tables = createTables(rnd, config, buildContext);
                engine.releaseInactive();
                writerPool.halt();

                final ObjList<GeneratedQuery> queries = new ObjList<>();
                final boolean[] isSerial = new boolean[config.getNumQueries()];
                final int[] cancelAfterChecks = new int[config.getNumQueries()];
                // Keep cancellation draws independent of query generation and the serial picks.
                final Rnd cancelRnd = new Rnd(rnd.getSeed0() ^ 0x9E3779B97F4A7C15L, rnd.getSeed1() ^ 0xC2B2AE3D27D4EB4FL);
                for (int q = 0; q < config.getNumQueries(); q++) {
                    final long preGenS0 = rnd.getSeed0();
                    final long preGenS1 = rnd.getSeed1();
                    GeneratedQuery query = generateConcurrentQuery(rnd, tables, null, config, isEquiJoinRun);
                    if (rnd.nextInt(100) < QUERY_BIND_PROBABILITY_PCT) {
                        final long bindS0 = rnd.nextLong();
                        final long bindS1 = rnd.nextLong();
                        rnd.reset(preGenS0, preGenS1);
                        final BindContext bindContext = new BindContext(new Rnd(bindS0, bindS1), CONSTANT_BIND_PROBABILITY_PCT);
                        final GeneratedQuery bindForm = generateConcurrentQuery(rnd, tables, bindContext, config, isEquiJoinRun);
                        if (bindContext.getBindValues().size() > 0 || bindContext.getLimitValue() != null) {
                            query = query.withBind(bindForm.sql(), bindContext.getBindNames(), bindContext.getBindValues(), bindContext.getLimitValue());
                        }
                    }
                    queries.add(query);
                    isSerial[q] = rnd.nextInt(100) < SERIAL_PROBABILITY_PCT;
                    // The ordered-row oracle needs deterministic queries. Use early checks so
                    // the breaker trips even on these small tables.
                    cancelAfterChecks[q] = isEquiJoinRun && !isSerial[q] && query.shape() == QueryShape.EQUI_JOIN
                            && query.deterministic() && cancelRnd.nextBoolean() ? cancelRnd.nextInt(32) : -1;
                }

                final ObjList<QueryRunner> runners = new ObjList<>();
                final CountDownLatch doneLatch = new CountDownLatch(ownerCount);
                final AtomicInteger completedCount = new AtomicInteger();
                final AtomicReferenceArray<String> runningSql = new AtomicReferenceArray<>(ownerCount);
                final AtomicReference<Throwable> ownerError = new AtomicReference<>();
                final ObjList<QueryRunner.Result> failures = new ObjList<>();
                for (int o = 0; o < ownerCount; o++) {
                    final int ownerIndex = o;
                    final SqlExecutionContextImpl context = newFuzzContext(queryWorkerCount);
                    contexts.add(context);
                    context.changePageFrameSizes(pageFrameRows, pageFrameRows);
                    if (isEquiJoinRun) {
                        // The run turns the operator on, whatever the configuration says, as the
                        // run of one owner does.
                        context.setParallelHashJoinEnabled(true);
                    }
                    final QueryRunner runner = new QueryRunner(
                            engine,
                            context,
                            config.isDiffJitEnabled(),
                            config.isDiffShadowEnabled(),
                            config.isDiffFusedEnabled(),
                            // Only an equi-join run has equi-join queries to compare, see
                            // generateConcurrentQuery().
                            isEquiJoinRun && config.isDiffParallelJoinEnabled(),
                            config.isDiffStreamEnabled(),
                            null,
                            config.isVerifyCursorEnabled(),
                            tables,
                            QUERY_POOL_NAME + '_'
                    );
                    runners.add(runner);
                    final FiberTask owner = new FiberTask() {
                        @Override
                        protected void onError(Throwable th) {
                            ownerError.compareAndSet(null, th);
                        }

                        @Override
                        protected boolean runStep() {
                            try {
                                final boolean[] parallelFlags = parallelFlags(context);
                                for (int i = ownerIndex, n = queries.size(); i < n; i += ownerCount) {
                                    final GeneratedQuery query = queries.getQuick(i);
                                    runningSql.set(ownerIndex, query.sql());
                                    QueryRunner.Result result;
                                    if (isSerial[i]) {
                                        setParallelFlags(context, SERIAL_FLAGS);
                                        try {
                                            result = runner.run(query);
                                        } finally {
                                            setParallelFlags(context, parallelFlags);
                                        }
                                    } else {
                                        result = runner.run(query);
                                        if (!result.isFailed() && !result.isSkipped() && cancelAfterChecks[i] >= 0) {
                                            result = runner.runConcurrentCancelFault(query, cancelAfterChecks[i]);
                                        }
                                    }
                                    if (result.isFailed()) {
                                        synchronized (failures) {
                                            failures.add(result);
                                        }
                                    }
                                    completedCount.incrementAndGet();
                                }
                                runningSql.set(ownerIndex, null);
                            } finally {
                                doneLatch.countDown();
                            }
                            return true;
                        }
                    };
                    Assert.assertSame(LaunchResult.LAUNCHED, ownerPool.getFiberRuntime().launch(owner));
                }

                int lastCompletedCount = -1;
                long lastProgressNanos = System.nanoTime();
                while (!doneLatch.await(100, TimeUnit.MILLISECONDS)) {
                    final int completed = completedCount.get();
                    if (completed != lastCompletedCount) {
                        lastCompletedCount = completed;
                        lastProgressNanos = System.nanoTime();
                    } else if (System.nanoTime() - lastProgressNanos > CONCURRENT_STALL_NANOS) {
                        TestListener.dumpThreadStacks();
                        final StringBuilder running = new StringBuilder();
                        for (int o = 0; o < ownerCount; o++) {
                            final String sql = runningSql.get(o);
                            if (sql != null) {
                                running.append("\n  ").append(sql);
                            }
                        }
                        Assert.fail("concurrent query fuzz stalled after " + completed + " of " + queries.size() + " queries, running:" + running);
                    }
                }
                if (ownerError.get() != null) {
                    throw new AssertionError("query owner failed", ownerError.get());
                }
                if (failures.size() > 0) {
                    throw QueryFuzzDriver.buildFailure(failures);
                }
                if (isEquiJoinRun) {
                    assertConcurrentCancellations(runners);
                    if (config.isDiffParallelJoinEnabled()) {
                        assertConcurrentParallelJoinShares(runners);
                    }
                }
            } finally {
                haltQuietly(queryPool);
                haltQuietly(ownerPool);
                haltQuietly(writerPool);
                Misc.freeObjList(contexts);
            }
        });
    }

    private void runQueryFuzz(RunnerCheck check) throws Exception {
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
            // The pools' modes come from seeds of their own, which -Dfuzz.s0 and -Dfuzz.s1 replay, as
            // TestUtils.generateRandom(LOG) reads them; the log line below names both.
            final long modeS0 = Long.getLong("fuzz.s0", System.nanoTime());
            final long modeS1 = Long.getLong("fuzz.s1", System.currentTimeMillis());
            final Rnd modeRnd = TestUtils.generateRandom(LOG, modeS0, modeS1);
            final WorkerPool queryPool = TestWorkerPool.createWithRandomMode(modeRnd, new WorkerPoolConfiguration() {
                @Override
                public String getPoolName() {
                    return QUERY_POOL_NAME;
                }

                @Override
                public int getWorkerCount() {
                    return workerCount;
                }
            });
            WorkerPoolUtils.setupQueryJobs(queryPool, engine, true);
            final WorkerPool writerPool = TestWorkerPool.createWithRandomMode(modeRnd, new WorkerPoolConfiguration() {
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
            LOG.info().$("fuzz worker pools [queryMode=").$(queryPool.getWorkerPoolMode().name())
                    .$(", writerMode=").$(writerPool.getWorkerPoolMode().name())
                    .$(", replay=-Dfuzz.s0=").$(modeS0).$(" -Dfuzz.s1=").$(modeS1)
                    .I$();
            queryPool.start(LOG);
            writerPool.start(LOG);
            try (
                    SqlExecutionContext parallelCtx = new SqlExecutionContextImpl(engine, workerCount)
                            .with(securityContext, bindVariableService, null, -1, circuitBreaker)
            ) {
                parallelCtx.initNow();
                // The run turns the parallel hash join on for every query, whatever the configuration
                // says, so that every axis and the fault injection run it, and the parallel join axis
                // compares it with the ordinary plan.
                parallelCtx.setParallelHashJoinEnabled(true);
                check.check(runFuzz(parallelCtx, writerPool, QUERY_POOL_NAME + '_'));
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

    @Test
    public void testStorageSkipIsReported() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE skip_primary (i INT)");
            execute("CREATE TABLE skip_shadow (i INT)");
            ObjList<FuzzTable> tables = new ObjList<>();
            tables.add(new FuzzTable("skip_primary", new ObjList<>(), null,
                    FuzzTableFactory.ParquetMode.NONE, null, null,
                    new FuzzTable("skip_shadow", new ObjList<>(), null)));
            QueryRunner runner = new QueryRunner(engine, sqlExecutionContext,
                    false, true, false, false, false, null, true, tables, QUERY_POOL_NAME + '_');
            QueryRunner.Result result = runner.run(new GeneratedQuery(
                    "SELECT missing_column FROM skip_primary", true));
            Assert.assertTrue("rejected storage comparisons must not count as coverage", result.isSkipped());
        });
    }

    private static ObjList<FuzzTable> createTables(Rnd rnd, FuzzConfig config, SqlExecutionContext sqlExecutionContext) throws SqlException {
        final FuzzTableFactory factory = new FuzzTableFactory(config);
        final ObjList<FuzzTable> tables = new ObjList<>();
        for (int i = 0; i < config.getNumTables(); i++) {
            final FuzzTable t = factory.create(
                    rnd,
                    "fuzz_t" + i,
                    sql -> engine.execute(sql, sqlExecutionContext),
                    QueryFuzzTest::drainWalQueue
            );
            tables.add(t);
            QueryFuzzDriver.logSchema(t, config.getTableRows(i));
            if (t.getShadow() != null) {
                QueryFuzzDriver.logSchema(t.getShadow(), config.getTableRows(i));
            }
        }
        return tables;
    }

    private static void assertConcurrentCancellations(ObjList<QueryRunner> runners) {
        int armed = 0;
        int cancelled = 0;
        int stacked = 0;
        int swapped = 0;
        for (int i = 0, n = runners.size(); i < n; i++) {
            final QueryRunner runner = runners.getQuick(i);
            armed += runner.getFaultsArmed(FaultType.CANCEL);
            cancelled += runner.getFaultsFired(FaultType.CANCEL);
            stacked += runner.getParallelJoinCancelledStacked();
            swapped += runner.getParallelJoinCancelledSwapped();
        }
        LOG.info().$("concurrent fuzz cancellations [armed=").$(armed).$(", cancelled=").$(cancelled)
                .$(", stacked=").$(stacked).$(", swapped=").$(swapped).I$();
        if (armed >= MIN_SHAPE_QUERIES_FOR_ACCEPT_FLOOR) {
            Assert.assertTrue("no cancellations among " + armed + " armed concurrent joins", cancelled > 0);
        }
    }

    // Holds the planner of a concurrent equi-join run to the shares that the run of one owner holds
    // it to, over the comparisons of every owner's runner: a planner that stops running the parallel
    // hash join, or stacking it, leaves every comparison ordinary against ordinary, and the run
    // green. A run too small to tell says so.
    private static void assertConcurrentParallelJoinShares(ObjList<QueryRunner> runners) {
        int completed = 0;
        int selected = 0;
        int stacked = 0;
        int swapped = 0;
        int partialReads = 0;
        int reexecutions = 0;
        for (int i = 0, n = runners.size(); i < n; i++) {
            final QueryRunner runner = runners.getQuick(i);
            completed += runner.getParallelJoinAxisCompletedRuns();
            selected += runner.getParallelJoinAxisSelected();
            stacked += runner.getParallelJoinAxisStacked();
            swapped += runner.getParallelJoinAxisSwapped();
            partialReads += runner.getParallelJoinPartialReadChecks();
            reexecutions += runner.getParallelJoinReexecutionChecks();
        }
        LOG.info().$("concurrent fuzz parallel join axis: ").$(selected).$(" of ").$(completed)
                .$(" completed comparisons used the parallel hash join, ").$(stacked).$(" of them stacking two joins or more, ")
                .$(swapped).$(" swapping their first join's inputs; ").$(partialReads).$(" partial reads checked, ")
                .$(reexecutions).$(" factories executed again")
                .$();
        if (completed < MIN_SHAPE_QUERIES_FOR_ACCEPT_FLOOR) {
            LOG.info().$("concurrent fuzz parallel join axis below the sample size, NOT guarded this run: ")
                    .$(completed).$('/').$(MIN_SHAPE_QUERIES_FOR_ACCEPT_FLOOR)
                    .$(" completed comparisons; raise -D").$(FuzzConfig.QUERIES_PROP).$(" to guard it")
                    .$();
            return;
        }
        Assert.assertFalse(
                "the parallel hash join ran on only " + selected + " of " + completed + " completed comparisons of the concurrent run",
                QueryFuzzDriver.isParallelJoinShareImplausible(selected, completed)
        );
        Assert.assertFalse(
                "the parallel hash join stacked two joins or more in only " + stacked + " of its " + selected + " concurrent runs",
                QueryFuzzDriver.isStackedShareImplausible(stacked, selected)
        );
    }

    private static void assertBinomialLowerTail(double expected, int k, int n) {
        Assert.assertEquals(expected, QueryFuzzDriver.binomialLowerTail(k, n, 0.4), expected * 1e-9);
    }

    /**
     * Returns the exact binomial lower tail: the chance that {@code n} independent draws, each a
     * success with probability {@code p}, yield at most {@code k} successes. The loop builds each
     * term from the one before in log space, because the first term, (1 - p)^n, underflows a double
     * at a few thousand draws, while the terms near {@code k} still carry the sum.
     */
    private static double binomialLowerTail(int k, int n, double p) {
        final double logOdds = Math.log(p) - Math.log1p(-p);
        double logTerm = n * Math.log1p(-p);
        double tail = Math.exp(logTerm);
        for (int i = 1; i <= k; i++) {
            logTerm += Math.log((double) (n - i + 1) / i) + logOdds;
            tail += Math.exp(logTerm);
        }
        return tail;
    }

    // EquiJoinClause sizes its stars for the default config's tables of about 150 rows. On
    // forConcurrency()'s tables of 600 to 1,800 rows, one join over a key with NULLs returns hundreds
    // of thousands of rows: its window functions overflow the window store and the owners' copies of
    // the results exhaust the heap. The concurrent run over those tables therefore leaves the band to
    // SIMPLE, and the run over forConcurrentEquiJoins()'s small tables draws an equi-join, alone or a
    // star over the run's tables, for four queries in five, and any other shape for the fifth, so
    // that the owners run the joins next to the other parallel operators.
    private static GeneratedQuery generateConcurrentQuery(
            Rnd rnd,
            ObjList<FuzzTable> tables,
            BindContext bindContext,
            FuzzConfig config,
            boolean isEquiJoinRun
    ) {
        if (isEquiJoinRun && config.isEquiJoinEnabled() && rnd.nextInt(5) < 4) {
            // The run draws two tables at least, and the join's two inputs are two of them.
            final int left = rnd.nextInt(tables.size());
            final int right = (left + 1 + rnd.nextInt(tables.size() - 1)) % tables.size();
            return EquiJoinClause.generate(rnd, tables.getQuick(left), tables.getQuick(right), tables, bindContext, false)
                    .withShape(QueryShape.EQUI_JOIN);
        }
        return QueryGenerator.generate(
                rnd,
                tables,
                bindContext,
                false,
                false,
                config.isLatestOnEnabled(),
                config.isHorizonJoinEnabled(),
                config.isWindowJoinEnabled(),
                config.isHashJoinEnabled(),
                config.isGroupingSetsEnabled(),
                false,
                config
        );
    }

    private static boolean[] parallelFlags(SqlExecutionContext context) {
        return new boolean[]{
                context.isParallelFilterEnabled(),
                context.isParallelGroupByEnabled(),
                context.isParallelHashJoinEnabled(),
                context.isParallelHashJoinGroupByEnabled(),
                context.isParallelHorizonJoinEnabled(),
                context.isParallelReadParquetEnabled(),
                context.isParallelTopKEnabled(),
                context.isParallelWindowJoinEnabled()
        };
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

    // One materialized row, as QueryRunner.materialize() writes it, without its newline.
    private static String line(String... cells) {
        final StringSink sink = new StringSink();
        for (String cell : cells) {
            QueryRunner.appendCell(sink, cell);
            sink.put('\t');
        }
        return sink.toString();
    }

    private static SqlExecutionContextImpl newFuzzContext(int workerCount) {
        final SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, workerCount)
                .with(securityContext, new BindVariableServiceImpl(configuration), null, -1, null);
        context.initNow();
        return context;
    }

    private static QueryRunner runFuzz(SqlExecutionContext sqlExecutionContext, WorkerPool writerPool, String queryWorkerNamePrefix) throws Exception {
        Long s0 = Long.getLong("questdb.fuzz.s0");
        Long s1 = Long.getLong("questdb.fuzz.s1");
        Rnd rnd = (s0 != null && s1 != null)
                ? TestUtils.generateRandom(LOG, s0, s1)
                : TestUtils.generateRandom(LOG);
        FuzzConfig config = new FuzzConfig(rnd);

        // Composite partitions ship OFF; the test defaults (Overrides) turn them ON. A per-run coin
        // here keeps both configurations fuzzed rather than pinning ON every run: on the runs that
        // draw false the suite exercises the shipped default write path - the OFF branches in
        // TableWriter and O3PartitionJob - and FuzzTableFactory's per-table backdate lands as an
        // ordinary O3 overlap rather than a composite one (its outcome feeds only diagnostics, so the
        // diff-shadow oracle holds either way). The draw comes from the same seed logged above, so a
        // red run replays it.
        boolean mergeAppendEnabled = rnd.nextBoolean();
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, mergeAppendEnabled);
        LOG.info().$("merge-append coin [mergeAppendEnabled=").$(mergeAppendEnabled).I$();

        // The tables hold too few rows to pass the default sharding threshold.
        if (rnd.nextBoolean()) {
            final int shardingThreshold = 1 + rnd.nextInt(8);
            node1.setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, shardingThreshold);
            LOG.info().$("sharding threshold coin [threshold=").$(shardingThreshold).I$();
        }

        if (config.getPageFrameMaxRows() > 0) {
            node1.setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 1);
            node1.setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, config.getPageFrameMaxRows());
        }

        LOG.info().$("fuzz config: tables=").$(config.getNumTables())
                .$(", rows=").$(config.getRowsPerTable())
                .$(", pageFrameMaxRows=").$(config.getPageFrameMaxRows())
                .$(", queries=").$(config.getNumQueries())
                .$(", diffJit=").$(config.isDiffJitEnabled())
                .$(", diffShadow=").$(config.isDiffShadowEnabled())
                .$(", verifyCursor=").$(config.isVerifyCursorEnabled())
                .$(", faults=").$(config.isFaultInjectionEnabled())
                .$(", faultPct=").$(config.getFaultProbabilityPct())
                .$(", parallelFaults=").$(config.isParallelFaultEnabled())
                .$(", window=").$(config.isWindowEnabled())
                .$(", latestOn=").$(config.isLatestOnEnabled())
                .$(", horizonJoin=").$(config.isHorizonJoinEnabled())
                .$(", windowJoin=").$(config.isWindowJoinEnabled())
                .$(", hashJoin=").$(config.isHashJoinEnabled())
                .$(", equiJoin=").$(config.isEquiJoinEnabled())
                .$(", diffFused=").$(config.isDiffFusedEnabled())
                .$(", diffParallelJoin=").$(config.isDiffParallelJoinEnabled())
                .$(", diffStream=").$(config.isDiffStreamEnabled())
                .$(", diffMapPage=").$(config.isDiffMapPageEnabled())
                .$(", hashJoinPayload=").$(config.getHashJoinPayloadLayout() != null ? config.getHashJoinPayloadLayout().name() : "rule")
                .$(", hashJoinBuild=").$(config.getHashJoinBuildMode().name())
                .$(", parallelJoinCap=").$(config.getParallelJoinTaskOutputMaxRows())
                .$(", parallelJoinFrames=").$(config.getParallelJoinFrameMaxRows())
                .$(", parallelJoinProbeFrames=").$(config.getParallelJoinProbeFrameMaxRows())
                .$(", parallelJoinRing=").$(config.getParallelJoinRingMaxSize())
                .$(", parallelJoinDispatch=").$(config.getParallelJoinDispatchLimit())
                .$(", parquetRowGroup=").$(config.getParquetRowGroupSize())
                .$(", storage=").$(config.isStorageAxesEnabled())
                .$();
        if (config.getHashJoinPayloadLayout() != null) {
            config.getHashJoinPayloadLayout().apply(node1.getConfigurationOverrides());
        }
        config.getHashJoinBuildMode().applyProperties(node1.getConfigurationOverrides());
        if (config.getParallelJoinTaskOutputMaxRows() > 0) {
            node1.setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, config.getParallelJoinTaskOutputMaxRows());
        }
        if (config.getParallelJoinRingMaxSize() > 0) {
            node1.setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TUPLE_RING_MAX_SIZE, config.getParallelJoinRingMaxSize());
        }
        if (config.getParallelJoinProbeFrameMaxRows() > 0) {
            node1.setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_PAGE_FRAME_MAX_ROWS, config.getParallelJoinProbeFrameMaxRows());
        }
        if (config.getParquetRowGroupSize() > 0) {
            node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, config.getParquetRowGroupSize());
        }

        ObjList<FuzzTable> tables = createTables(rnd, config, sqlExecutionContext);

        // Writes are done: close the build-phase writers and halt the writer pool
        // so the query loop runs background-silent (see testQueryFuzz). A column
        // purge scheduled by the writer close is abandoned -- it only touches
        // on-disk files, which the leak oracle does not measure.
        engine.releaseInactive();
        writerPool.halt();

        return QueryFuzzDriver.run(engine, sqlExecutionContext, config, rnd, tables, queryWorkerNamePrefix,
                QueryFuzzDriver.Phase.MIXED,
                queryRnd -> setProperty(PropertyKey.CAIRO_SQL_LATEST_BY_JIT_ENABLED, Boolean.toString(queryRnd.nextBoolean())));
    }

    @FunctionalInterface
    private interface RunnerCheck {
        void check(QueryRunner runner);
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private static void setParallelFlags(SqlExecutionContext context, boolean[] flags) {
        context.setParallelFilterEnabled(flags[0]);
        context.setParallelGroupByEnabled(flags[1]);
        context.setParallelHashJoinEnabled(flags[2]);
        context.setParallelHashJoinGroupByEnabled(flags[3]);
        context.setParallelHorizonJoinEnabled(flags[4]);
        context.setParallelReadParquetEnabled(flags[5]);
        context.setParallelTopKEnabled(flags[6]);
        context.setParallelWindowJoinEnabled(flags[7]);
    }
}
