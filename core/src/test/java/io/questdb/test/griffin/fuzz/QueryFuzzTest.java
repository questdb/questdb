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
import io.questdb.cairo.sql.RecordCursor;
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
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.fuzz.FailureFileFacade;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Seeded random query fuzzer. Generates 2..3 WAL tables with all supported
 * scalar types plus DECIMAL and DOUBLE arrays, inserts rows that span
 * multiple DAY partitions, then runs a budget of randomly generated
 * SELECT / GROUP BY / SAMPLE BY / LATEST ON / ASOF-LT-SPLICE / HORIZON /
 * WINDOW JOIN
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
 *         temporal-only and draws the original rnd stream. Their WHERE is
 *         master-side only, so the FUNCTION fault is woven there like the
 *         other shapes.</li>
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
    private static final String QUERY_POOL_NAME = "fuzzQuery";

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
                final FuzzTable master = new FuzzTable("master_p", new ObjList<>(), "ts", FuzzTableFactory.ParquetMode.NONE, null, masterShadow);
                final FuzzTable slave = new FuzzTable("slave_p", new ObjList<>(), "ts", FuzzTableFactory.ParquetMode.NONE, null, slaveShadow);
                final ObjList<FuzzTable> tables = new ObjList<>();
                tables.add(master);
                tables.add(slave);

                // The storage-axis oracle tolerates the compile/reject asymmetry
                // rather than reporting it as a divergence. run() swallows a
                // tolerated per-axis skip into an overall ok result and only
                // surfaces a failed axis, so the regression assertion is that the
                // run is not failed; without isPlannerSensitivityAsymmetry the
                // storage axis would report a divergence and the run would fail.
                final QueryRunner runner = new QueryRunner(engine, sqlExecutionContext, false, true, false, tables, null);
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

            QueryRunner runner = new QueryRunner(engine, sqlExecutionContext, false, false, true, new ObjList<>(), null);
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
            final Rnd modeRnd = TestUtils.generateRandom(LOG);
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

    @Test
    public void testStorageSkipIsReported() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE skip_primary (i INT)");
            execute("CREATE TABLE skip_shadow (i INT)");
            ObjList<FuzzTable> tables = new ObjList<>();
            tables.add(new FuzzTable("skip_primary", new ObjList<>(), null,
                    FuzzTableFactory.ParquetMode.NONE, null,
                    new FuzzTable("skip_shadow", new ObjList<>(), null)));
            QueryRunner runner = new QueryRunner(engine, sqlExecutionContext,
                    false, true, true, tables, QUERY_POOL_NAME + '_');
            QueryRunner.Result result = runner.run(new GeneratedQuery(
                    "SELECT missing_column FROM skip_primary", true));
            Assert.assertTrue("rejected storage comparisons must not count as coverage", result.isSkipped());
        });
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
                .$(", latestOn=").$(config.isLatestOnEnabled())
                .$(", horizonJoin=").$(config.isHorizonJoinEnabled())
                .$(", windowJoin=").$(config.isWindowJoinEnabled())
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
        }

        // Writes are done: close the build-phase writers and halt the writer pool
        // so the query loop runs background-silent (see testQueryFuzz). A column
        // purge scheduled by the writer close is abandoned -- it only touches
        // on-disk files, which the leak oracle does not measure.
        engine.releaseInactive();
        writerPool.halt();

        QueryFuzzDriver.run(engine, sqlExecutionContext, config, rnd, tables, queryWorkerNamePrefix,
                QueryFuzzDriver.Phase.MIXED);
    }
}
