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

package io.questdb.test.griffin.engine.window;

import io.questdb.griffin.SqlException;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.BindVarTuple;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class MinMaxWindowFunctionTest extends AbstractCairoTest {

    @Test
    public void testBindVariableTarget() throws Exception {
        // minmax(ts, value, target) accepts a runtime-constant (bind-variable) target, read
        // PER-EXECUTION (shares BucketSelectWindowFunction.init with m4/lttb): the SAME compiled
        // factory produces keep-all vs bucketed keep-sets as $1 is re-bound between executions, and a
        // runtime out-of-range target throws at cursor-open (not compile).
        final ObjList<BindVarTuple> cases = new ObjList<>();
        // $1 = 8 over 6 rows: count(6) <= target(8) -> keep all (selectAll short-circuit).
        cases.add(BindVarTuple.ok(
                "target 8 (keep all)",
                """
                        ts\tv\tkeep
                        1970-01-01T00:00:00.000001Z\t10.0\ttrue
                        1970-01-01T00:00:00.000002Z\t20.0\ttrue
                        1970-01-01T00:00:00.000003Z\t30.0\ttrue
                        1970-01-01T00:00:00.000004Z\t40.0\ttrue
                        1970-01-01T00:00:00.000005Z\t50.0\ttrue
                        1970-01-01T00:00:00.000006Z\t60.0\ttrue
                        """,
                bindVariableService -> bindVariableService.setLong(0, 8)
        ));
        // Re-bind $1 = 2 on the same compiled factory: count(6) > 2 -> bucketing (numBuckets = 2/2 = 1),
        // a single bucket over 6 monotonic rows keeps {min,max} = rows 1 and 6. A different result from
        // the keep-all case above proves the target is read at execution, not frozen at compile.
        cases.add(BindVarTuple.ok(
                "target 2 (re-bind, min+max only)",
                """
                        ts\tv\tkeep
                        1970-01-01T00:00:00.000001Z\t10.0\ttrue
                        1970-01-01T00:00:00.000002Z\t20.0\tfalse
                        1970-01-01T00:00:00.000003Z\t30.0\tfalse
                        1970-01-01T00:00:00.000004Z\t40.0\tfalse
                        1970-01-01T00:00:00.000005Z\t50.0\tfalse
                        1970-01-01T00:00:00.000006Z\t60.0\ttrue
                        """,
                bindVariableService -> bindVariableService.setLong(0, 2)
        ));
        // Re-bind $1 = 1: out-of-range detected at cursor-open (range validation moved from
        // newInstance to per-execution init), same message/position as a constant would produce.
        cases.add(BindVarTuple.fails(
                "target 1 (runtime out of range)",
                28,
                "target points must be at least 2",
                bindVariableService -> bindVariableService.setLong(0, 1)
        ));

        assertQuery("select ts, v, minmax(ts, v, $1) over (order by ts) keep from t")
                .ddl("create table t (ts timestamp, v double) timestamp(ts)",
                        "insert into t select x::timestamp, x*10 from long_sequence(6)")
                .timestamp("ts")
                .expectSize()
                .assertBinds(cases);
    }

    @Test
    public void testConstantTargetOutOfRangeFailsAtCompileTime() throws Exception {
        // Fix 2: a constant target's range is validated at newInstance (compile time), matching the
        // pre-bind-var-support factory and the legacy SUBSAMPLE cursor's own constant handling - not
        // deferred to cursor-open. select(...) below only compiles the query (it never calls
        // factory.getCursor(...)), so a thrown SqlException here proves the failure happened during
        // compilation, not execution.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            try {
                select("select ts, v, minmax(ts, v, 1) over (order by ts) keep from t");
                Assert.fail("expected compilation to fail for an out-of-range constant target");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "target points must be at least 2");
                Assert.assertEquals(28, e.getPosition());
            }
        });
    }

    @Test
    public void testKeepsAllWhenFewRows() throws Exception {
        // n=3, target=8 -> numBuckets=4, but few rows: MinMaxAlgorithm.select naturally keeps all
        // of them (min/max collapse onto the same 3 points across the buckets).
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            execute("insert into t values (1::timestamp,10.0),(2::timestamp,20.0),(3::timestamp,30.0)");
            assertQuery("select ts, v, minmax(ts, v, 8) over (order by ts) keep from t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv\tkeep
                            1970-01-01T00:00:00.000001Z\t10.0\ttrue
                            1970-01-01T00:00:00.000002Z\t20.0\ttrue
                            1970-01-01T00:00:00.000003Z\t30.0\ttrue
                            """);
        });
    }

    @Test
    public void testKeepsAllRowsWhenCountAtTargetEvenIfBucketingWouldDrop() throws Exception {
        // Distinguishing case for the count <= target keep-all short-circuit: 2 monotonically
        // increasing rows with target=2 -> numBuckets=1 (single bucket over all rows). Bucketing
        // would collapse min=row0, max=row1 to {0,1} anyway here, so use 4 rows / target=4 instead:
        // numBuckets=2, but bufferCount(4) <= target(4), so captured legacy selectAll behavior
        // keeps every row rather than bucketing (which could dedup min==max within a bucket) -
        // minmax() must match, keeping all four.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            execute("insert into t values (1::timestamp,10.0),(2::timestamp,20.0),(3::timestamp,30.0),(4::timestamp,40.0)");
            assertQuery("select ts, v, minmax(ts, v, 4) over (order by ts) keep from t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv\tkeep
                            1970-01-01T00:00:00.000001Z\t10.0\ttrue
                            1970-01-01T00:00:00.000002Z\t20.0\ttrue
                            1970-01-01T00:00:00.000003Z\t30.0\ttrue
                            1970-01-01T00:00:00.000004Z\t40.0\ttrue
                            """);
            // Captured legacy behavior: the clause keeps all rows at the exact target.
            assertQuery("select ts, v from t SUBSAMPLE minmax(v, 4)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            1970-01-01T00:00:00.000001Z\t10.0
                            1970-01-01T00:00:00.000002Z\t20.0
                            1970-01-01T00:00:00.000003Z\t30.0
                            1970-01-01T00:00:00.000004Z\t40.0
                            """);
        });
    }

    @Test
    public void testMatchesMinMaxAlgorithmOnSpike() throws Exception {
        // Deterministic spike; keep min/max per time bucket. Expected output filled from the
        // captured legacy golden for the same dataset.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            execute("insert into t select x::timestamp, case when x%5=0 then 100.0 else x end from long_sequence(20)");
            assertQuery("select ts, v from (select ts, v, minmax(ts, v, 8) over (order by ts) keep from t) where keep")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize(false)
                    .returns("""
                            ts\tv
                            1970-01-01T00:00:00.000001Z\t1.0
                            1970-01-01T00:00:00.000004Z\t4.0
                            1970-01-01T00:00:00.000005Z\t100.0
                            1970-01-01T00:00:00.000006Z\t6.0
                            1970-01-01T00:00:00.000010Z\t100.0
                            1970-01-01T00:00:00.000011Z\t11.0
                            1970-01-01T00:00:00.000015Z\t100.0
                            1970-01-01T00:00:00.000016Z\t16.0
                            """);
        });
    }

    @Test
    public void testRejectsNonNumericValue() throws Exception {
        // SYMBOL is not implicitly castable to DOUBLE, so the overload resolver itself rejects this
        // before newInstance() ever runs - matching the FunctionParser's own type-mismatch
        // diagnostic, same as m4's equivalent case.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, s symbol) timestamp(ts)");
            assertQuery("select ts, minmax(ts, s, 8) over (order by ts) from t")
                    .noLeakCheck()
                    .fails(25, "argument type mismatch");
        });
    }

    @Test
    public void testRejectsNonNumericValueThatOverloadResolutionLetsThrough() throws Exception {
        // CHAR *is* implicitly widenable to DOUBLE per ColumnType's overload rules (it reaches
        // newInstance() as a fuzzy-match candidate), so this is the case that actually exercises our
        // manual numeric-type guard and its SUBSAMPLE-cursor-matching message.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, c char) timestamp(ts)");
            assertQuery("select ts, minmax(ts, c, 8) over (order by ts) from t")
                    .noLeakCheck()
                    .fails(22, "numeric column expected, got: CHAR");
        });
    }

    @Test
    public void testRejectsNonConstantTarget() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            assertQuery("select ts, minmax(ts, v, v::long) over (order by ts) from t")
                    .noLeakCheck()
                    .fails(26, "target must be a constant");
        });
    }

    @Test
    public void testRejectsTargetBelowTwo() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            assertQuery("select ts, minmax(ts, v, 1) over (order by ts) from t")
                    .noLeakCheck()
                    .fails(25, "target points must be at least 2");
        });
    }

    @Test
    public void testFiltersNullAndNaNRows() throws Exception {
        // A NULL/NaN value must not poison a bucket's min/max the way an unfiltered scan would:
        // MinMaxAlgorithm.select seeds a bucket's min/max from the first row it sees, and NaN
        // comparisons are always false, so if that seed row is NaN the real min/max in the
        // bucket would never be detected. Captured legacy SUBSAMPLE behavior drops NULL ts /
        // null-or-NaN value rows
        // before bucketing; minmax() must match it exactly.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            execute("""
                    insert into t values
                    (1::timestamp, null),
                    (2::timestamp, 5.0),
                    (3::timestamp, 100.0),
                    (4::timestamp, 1.0)
                    """);
            // Single bucket (target=4 -> numBuckets=2, but with the null row dropped only 3 rows
            // remain and 3 <= target(4), so the keepAll short-circuit applies: min=1.0@ts4,
            // max=100.0@ts3, all three non-null rows are kept and the null row is excluded outright
            // (an unfiltered scan would instead seed min/max on the null row at ts1 and never
            // recover, wrongly keeping ts1 and dropping ts2/ts3).
            assertQuery("select ts, v, minmax(ts, v, 4) over (order by ts) keep from t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv\tkeep
                            1970-01-01T00:00:00.000001Z\tnull\tfalse
                            1970-01-01T00:00:00.000002Z\t5.0\ttrue
                            1970-01-01T00:00:00.000003Z\t100.0\ttrue
                            1970-01-01T00:00:00.000004Z\t1.0\ttrue
                            """);

            // Captured legacy behavior for NULL filtering, asserted through the window-only clause.
            assertQuery("select ts, v from t SUBSAMPLE minmax(v, 4)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            1970-01-01T00:00:00.000002Z\t5.0
                            1970-01-01T00:00:00.000003Z\t100.0
                            1970-01-01T00:00:00.000004Z\t1.0
                            """);
        });
    }

    @Test
    public void testExplainPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            assertQuery("select ts, minmax(ts, v, 8) over (order by ts) from t")
                    .noLeakCheck()
                    .assertsPlan("CachedWindowLight\n" +
                            """
                                      unorderedFunctions: [minmax(ts,v,8) over (order by [ts])]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                                    """);
        });
    }

    @Test
    public void testLongMaxBeyondDoublePrecisionKept() throws Exception {
        // F2-M4-LONG red test: LONG is an explicitly supported value type (the factory's numeric
        // check enumerates it), so selection must distinguish LONG values that are distinct in
        // long but collapse when narrowed to double. 2^53 and 2^53 + 1 both round to
        // 9007199254740992.0, so a double-buffered compare loop cannot see that row 2 is the
        // bucket maximum and drops it. Oracle: plain max()/min() aggregates (exact LONG math)
        // prove the extrema are distinct; the minmax contract (min and max of the single bucket,
        // first occurrence wins) then hand-derives the expected keep set {row1=min, row2=max}.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v long) timestamp(ts)");
            execute("""
                    insert into t values
                    (1::timestamp, 9_007_199_254_740_992),
                    (2::timestamp, 9_007_199_254_740_993),
                    (3::timestamp, 9_007_199_254_740_992)
                    """);
            // Independent oracle: exact LONG aggregation sees two distinct extrema.
            assertQuery("select max(v), min(v) from t")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            max\tmin
                            9007199254740993\t9007199254740992
                            """);
            // OVER form: count(3) > target(2) -> one bucket; min = row 1 (first occurrence),
            // max = row 2. Row 3 duplicates the min value and must not be kept.
            assertQuery("select ts, v, minmax(ts, v, 2) over (order by ts) keep from t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv\tkeep
                            1970-01-01T00:00:00.000001Z\t9007199254740992\ttrue
                            1970-01-01T00:00:00.000002Z\t9007199254740993\ttrue
                            1970-01-01T00:00:00.000003Z\t9007199254740992\tfalse
                            """);
            // SUBSAMPLE fused form desugars to the same window function; both extrema must survive.
            assertQuery("select ts, v from t SUBSAMPLE minmax(v, 2)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            1970-01-01T00:00:00.000001Z\t9007199254740992
                            1970-01-01T00:00:00.000002Z\t9007199254740993
                            """);
        });
    }

    @Test
    public void testLongMinBeyondDoublePrecisionKeptNegativeValues() throws Exception {
        // F2-M4-LONG red test, min side with negative magnitudes: -(2^53 + 1) rounds to
        // -9007199254740992.0 in double, so the double compare loop misses that row 2 is the
        // bucket minimum. Exact LONG comparison keeps min = row 2 and max = row 1 (first
        // occurrence of the max value); MinMaxAlgorithm emits them in timestamp order.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v long) timestamp(ts)");
            execute("""
                    insert into t values
                    (1::timestamp, -9_007_199_254_740_992),
                    (2::timestamp, -9_007_199_254_740_993),
                    (3::timestamp, -9_007_199_254_740_992)
                    """);
            assertQuery("select max(v), min(v) from t")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            max\tmin
                            -9007199254740992\t-9007199254740993
                            """);
            assertQuery("select ts, v, minmax(ts, v, 2) over (order by ts) keep from t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv\tkeep
                            1970-01-01T00:00:00.000001Z\t-9007199254740992\ttrue
                            1970-01-01T00:00:00.000002Z\t-9007199254740993\ttrue
                            1970-01-01T00:00:00.000003Z\t-9007199254740992\tfalse
                            """);
        });
    }

    @Test
    public void testLongNullRowDroppedNotTreatedAsExtremum() throws Exception {
        // Preservation control (green pre-fix, must stay green): a NULL LONG value
        // (Numbers.LONG_NULL = Long.MIN_VALUE) must stay dropped from the buffer - it must never
        // enter selection as a huge negative magnitude. Two non-null rows with target 2 hit the
        // count <= target keep-all short-circuit, so this pins ONLY the null-dropping behavior,
        // independent of the precision defect under investigation.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v long) timestamp(ts)");
            execute("""
                    insert into t values
                    (1::timestamp, null),
                    (2::timestamp, 9_007_199_254_740_993),
                    (3::timestamp, 9_007_199_254_740_992)
                    """);
            assertQuery("select ts, v, minmax(ts, v, 2) over (order by ts) keep from t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv\tkeep
                            1970-01-01T00:00:00.000001Z\tnull\tfalse
                            1970-01-01T00:00:00.000002Z\t9007199254740993\ttrue
                            1970-01-01T00:00:00.000003Z\t9007199254740992\ttrue
                            """);
            assertQuery("select ts, v from t SUBSAMPLE minmax(v, 2)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            1970-01-01T00:00:00.000002Z\t9007199254740993
                            1970-01-01T00:00:00.000003Z\t9007199254740992
                            """);
        });
    }

    @Test
    public void testLongBelowDoublePrecisionLimitExtremaKept() throws Exception {
        // Preservation control (green pre-fix, must stay green): ordinary LONG values far below
        // 2^53 convert to double exactly, so selection already works; the repair must not disturb
        // it. One bucket over 3 rows: min = 10 @ row 1, max = 30 @ row 2, row 3 dropped.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v long) timestamp(ts)");
            execute("insert into t values (1::timestamp, 10), (2::timestamp, 30), (3::timestamp, 20)");
            assertQuery("select ts, v, minmax(ts, v, 2) over (order by ts) keep from t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv\tkeep
                            1970-01-01T00:00:00.000001Z\t10\ttrue
                            1970-01-01T00:00:00.000002Z\t30\ttrue
                            1970-01-01T00:00:00.000003Z\t20\tfalse
                            """);
        });
    }

    @Test
    public void testIntValueExtremaKept() throws Exception {
        // Preservation control (green pre-fix, must stay green): INT values are always exact in
        // double (|v| < 2^31 < 2^53). Pins the INT lane against the repair, including that INT
        // NULL (Numbers.INT_NULL) stays dropped.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v int) timestamp(ts)");
            execute("""
                    insert into t values
                    (1::timestamp, 10),
                    (2::timestamp, null),
                    (3::timestamp, 30),
                    (4::timestamp, 20)
                    """);
            assertQuery("select ts, v, minmax(ts, v, 2) over (order by ts) keep from t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv\tkeep
                            1970-01-01T00:00:00.000001Z\t10\ttrue
                            1970-01-01T00:00:00.000002Z\tnull\tfalse
                            1970-01-01T00:00:00.000003Z\t30\ttrue
                            1970-01-01T00:00:00.000004Z\t20\tfalse
                            """);
        });
    }

    @Test
    public void testShortValueExtremaKeptNoSentinelDropped() throws Exception {
        // Lane control for the dual-lane repair: SHORT has NO null sentinel, so no SHORT row may
        // ever be dropped as null. Short.MIN_VALUE (-32768) is the decisive probe - a
        // per-width MIN_VALUE sentinel convention (as LONG/INT use) would wrongly drop it; here
        // it must be buffered and selected as the bucket minimum. One bucket over 4 rows:
        // min = -32768 @ row 2, max = 32767 @ row 3.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v short) timestamp(ts)");
            execute("""
                    insert into t values
                    (1::timestamp, 100),
                    (2::timestamp, -32768),
                    (3::timestamp, 32767),
                    (4::timestamp, 0)
                    """);
            assertQuery("select ts, v, minmax(ts, v, 2) over (order by ts) keep from t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv\tkeep
                            1970-01-01T00:00:00.000001Z\t100\tfalse
                            1970-01-01T00:00:00.000002Z\t-32768\ttrue
                            1970-01-01T00:00:00.000003Z\t32767\ttrue
                            1970-01-01T00:00:00.000004Z\t0\tfalse
                            """);
            // SUBSAMPLE fused form shares the same function object and buffer.
            assertQuery("select ts, v from t SUBSAMPLE minmax(v, 2)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            1970-01-01T00:00:00.000002Z\t-32768
                            1970-01-01T00:00:00.000003Z\t32767
                            """);
        });
    }

    @Test
    public void testByteValueExtremaKeptNoSentinelDropped() throws Exception {
        // Lane control, BYTE arm: like SHORT, BYTE has NO null sentinel - Byte.MIN_VALUE (-128)
        // is a real value and must be buffered and selected as the bucket minimum, never
        // dropped. One bucket over 4 rows: min = -128 @ row 2, max = 127 @ row 3.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v byte) timestamp(ts)");
            execute("""
                    insert into t values
                    (1::timestamp, 10),
                    (2::timestamp, -128),
                    (3::timestamp, 127),
                    (4::timestamp, 0)
                    """);
            assertQuery("select ts, v, minmax(ts, v, 2) over (order by ts) keep from t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv\tkeep
                            1970-01-01T00:00:00.000001Z\t10\tfalse
                            1970-01-01T00:00:00.000002Z\t-128\ttrue
                            1970-01-01T00:00:00.000003Z\t127\ttrue
                            1970-01-01T00:00:00.000004Z\t0\tfalse
                            """);
        });
    }

    @Test
    public void testFloatValueExtremaKept() throws Exception {
        // Preservation control (green pre-fix, must stay green): FLOAT stays on the
        // floating-point lane (widened to double, NaN dropped); the integral-exactness repair
        // must not change it.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v float) timestamp(ts)");
            execute("""
                    insert into t values
                    (1::timestamp, 1.5),
                    (2::timestamp, null),
                    (3::timestamp, 3.5),
                    (4::timestamp, 2.5)
                    """);
            assertQuery("select ts, v, minmax(ts, v, 2) over (order by ts) keep from t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv\tkeep
                            1970-01-01T00:00:00.000001Z\t1.5\ttrue
                            1970-01-01T00:00:00.000002Z\tnull\tfalse
                            1970-01-01T00:00:00.000003Z\t3.5\ttrue
                            1970-01-01T00:00:00.000004Z\t2.5\tfalse
                            """);
        });
    }
}
