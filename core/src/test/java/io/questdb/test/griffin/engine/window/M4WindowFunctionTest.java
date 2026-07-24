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

public class M4WindowFunctionTest extends AbstractCairoTest {

    @Test
    public void testBindVariableTarget() throws Exception {
        // m4(ts, value, target) accepts a runtime-constant (bind-variable) target, read PER-EXECUTION:
        // the SAME compiled factory produces keep-all vs bucketed keep-sets as $1 is re-bound between
        // executions, and a runtime out-of-range target throws at cursor-open (not compile).
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
        // Re-bind $1 = 4 on the same compiled factory: count(6) > 4 -> bucketing (numBuckets = 4/4 = 1),
        // a single bucket over 6 monotonic rows keeps {first,last} = rows 1 and 6. A different result
        // from the keep-all case above proves the target is read at execution, not frozen at compile.
        cases.add(BindVarTuple.ok(
                "target 4 (re-bind, bucketed)",
                """
                        ts\tv\tkeep
                        1970-01-01T00:00:00.000001Z\t10.0\ttrue
                        1970-01-01T00:00:00.000002Z\t20.0\tfalse
                        1970-01-01T00:00:00.000003Z\t30.0\tfalse
                        1970-01-01T00:00:00.000004Z\t40.0\tfalse
                        1970-01-01T00:00:00.000005Z\t50.0\tfalse
                        1970-01-01T00:00:00.000006Z\t60.0\ttrue
                        """,
                bindVariableService -> bindVariableService.setLong(0, 4)
        ));
        // Re-bind $1 = 1: out-of-range detected at cursor-open (range validation moved from
        // newInstance to per-execution init), same message/position as a constant would produce.
        cases.add(BindVarTuple.fails(
                "target 1 (runtime out of range)",
                24,
                "target points must be at least 2",
                bindVariableService -> bindVariableService.setLong(0, 1)
        ));

        assertQuery("select ts, v, m4(ts, v, $1) over (order by ts) keep from t")
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
                select("select ts, v, m4(ts, v, 1) over (order by ts) keep from t");
                Assert.fail("expected compilation to fail for an out-of-range constant target");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "target points must be at least 2");
                Assert.assertEquals(24, e.getPosition());
            }
        });
    }

    @Test
    public void testKeepsAllWhenFewRows() throws Exception {
        // n=3, target=8 -> numBuckets=2, but few rows: M4Algorithm.select naturally keeps all of them
        // (first/min/max/last collapse onto the same 3 points across the 2 buckets).
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            execute("insert into t values (1::timestamp,10.0),(2::timestamp,20.0),(3::timestamp,30.0)");
            assertQuery("select ts, v, m4(ts, v, 8) over (order by ts) keep from t")
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
        // Distinguishing case for the count <= target keep-all short-circuit: 4 monotonically
        // increasing rows with target=4 -> numBuckets=1 (single bucket over all rows). Bucketing
        // would collapse first=min=row0 and last=max=row3 to just {0,3}, DROPPING the two interior
        // rows (pre-fix window output: true,false,false,true). But bufferCount(4) <= target(4), so
        // captured legacy selectAll behavior keeps every row - m4() must match, keeping all four.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            execute("insert into t values (1::timestamp,10.0),(2::timestamp,20.0),(3::timestamp,30.0),(4::timestamp,40.0)");
            assertQuery("select ts, v, m4(ts, v, 4) over (order by ts) keep from t")
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
            assertQuery("select ts, v from t SUBSAMPLE m4(v, 4)")
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
    public void testMatchesM4AlgorithmOnSpike() throws Exception {
        // Deterministic spike; keep first/min/max/last per time bucket. Expected output filled from
        // the captured legacy golden for the same dataset.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            execute("insert into t select x::timestamp, case when x%5=0 then 100.0 else x end from long_sequence(20)");
            assertQuery("select ts, v from (select ts, v, m4(ts, v, 8) over (order by ts) keep from t) where keep")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize(false)
                    .returns("""
                            ts\tv
                            1970-01-01T00:00:00.000001Z\t1.0
                            1970-01-01T00:00:00.000005Z\t100.0
                            1970-01-01T00:00:00.000009Z\t9.0
                            1970-01-01T00:00:00.000010Z\t100.0
                            1970-01-01T00:00:00.000011Z\t11.0
                            1970-01-01T00:00:00.000020Z\t100.0
                            """);
        });
    }

    @Test
    public void testRejectsNonNumericValue() throws Exception {
        // SYMBOL is not implicitly castable to DOUBLE, so the overload resolver itself rejects this
        // before newInstance() ever runs (it never gets a chance to raise our own "numeric column
        // expected" message here) - matching the FunctionParser's own type-mismatch diagnostic is the
        // correct outcome for a genuinely non-numeric argument type on a real function call. The
        // bespoke SUBSAMPLE grammar has no signature resolution to lean on and validates this explicitly.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, s symbol) timestamp(ts)");
            assertQuery("select ts, m4(ts, s, 8) over (order by ts) from t")
                    .noLeakCheck()
                    .fails(21, "argument type mismatch");
        });
    }

    @Test
    public void testRejectsNonNumericValueThatOverloadResolutionLetsThrough() throws Exception {
        // CHAR *is* implicitly widenable to DOUBLE per ColumnType's overload rules (it reaches
        // newInstance() as a fuzzy-match candidate), so this is the case that actually exercises our
        // manual numeric-type guard and its SUBSAMPLE-cursor-matching message.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, c char) timestamp(ts)");
            assertQuery("select ts, m4(ts, c, 8) over (order by ts) from t")
                    .noLeakCheck()
                    .fails(18, "numeric column expected, got: CHAR");
        });
    }

    @Test
    public void testRejectsNonConstantTarget() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            assertQuery("select ts, m4(ts, v, v::long) over (order by ts) from t")
                    .noLeakCheck()
                    .fails(22, "target must be a constant");
        });
    }

    @Test
    public void testRejectsTargetBelowTwo() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            assertQuery("select ts, m4(ts, v, 1) over (order by ts) from t")
                    .noLeakCheck()
                    .fails(21, "target points must be at least 2");
        });
    }

    @Test
    public void testFiltersNullAndNaNRows() throws Exception {
        // A NULL/NaN value must not poison a bucket's min/max the way an unfiltered scan would:
        // M4Algorithm.select seeds a bucket's min/max from the first row it sees, and NaN
        // comparisons are always false, so if that seed row is NaN the real min/max in the
        // bucket would never be detected - nor would the null row itself ever be excluded from
        // first/last. SUBSAMPLE's input buffering
        // drops NULL ts / null-or-NaN value rows before bucketing; m4() must match it exactly.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            execute("""
                    insert into t values
                    (1::timestamp, null),
                    (2::timestamp, 5.0),
                    (3::timestamp, 100.0),
                    (4::timestamp, 1.0)
                    """);
            // Single bucket (target=4 -> numBuckets=1). With the null row dropped: first=5.0@ts2,
            // min=1.0@ts4, max=100.0@ts3, last=1.0@ts4 - all three non-null rows are kept and the
            // null row is excluded outright (an unfiltered scan would instead seed min/max on the
            // null row at ts1 and never recover, wrongly keeping ts1 and dropping ts2/ts3).
            assertQuery("select ts, v, m4(ts, v, 4) over (order by ts) keep from t")
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
            assertQuery("select ts, v from t SUBSAMPLE m4(v, 4)")
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
    public void testPass2NoBaseReadByteIdentical() throws Exception {
        // Guard for the pass2 base-re-read elimination (cache pass1's null-pattern instead of
        // re-deriving isNullRow(record) via a random-access base re-read). Interleaved NULL / NaN
        // and normal values is the alignment-sensitive case: null rows are dropped from the bucket
        // buffer, so pass2 must skip exactly the same rows pass1 dropped or the keep-set shifts. We
        // assert BOTH the full m4() keep-flag column (pins the false rows around nulls) AND the
        // captured legacy kept-row golden - a single difference is a correctness failure.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            // x%7==0 -> null value; x%5==0 -> spike (100.0); else x. Nulls and spikes interleave so
            // the dropped rows are scattered through the buckets, exercising the pass2Ordinal/selIdx
            // walk against the cached null bitset. 40 rows, target=8 -> bucketing actually runs.
            execute("insert into t select x::timestamp, " +
                    "case when x%7=0 then null when x%5=0 then 100.0 else x::double end " +
                    "from long_sequence(40)");

            // Full keep-flag column (golden pins the exact per-row alignment, incl. false @ nulls).
            assertQuery("select ts, v, m4(ts, v, 8) over (order by ts) keep from t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv\tkeep
                            1970-01-01T00:00:00.000001Z\t1.0\ttrue
                            1970-01-01T00:00:00.000002Z\t2.0\tfalse
                            1970-01-01T00:00:00.000003Z\t3.0\tfalse
                            1970-01-01T00:00:00.000004Z\t4.0\tfalse
                            1970-01-01T00:00:00.000005Z\t100.0\ttrue
                            1970-01-01T00:00:00.000006Z\t6.0\tfalse
                            1970-01-01T00:00:00.000007Z\tnull\tfalse
                            1970-01-01T00:00:00.000008Z\t8.0\tfalse
                            1970-01-01T00:00:00.000009Z\t9.0\tfalse
                            1970-01-01T00:00:00.000010Z\t100.0\tfalse
                            1970-01-01T00:00:00.000011Z\t11.0\tfalse
                            1970-01-01T00:00:00.000012Z\t12.0\tfalse
                            1970-01-01T00:00:00.000013Z\t13.0\tfalse
                            1970-01-01T00:00:00.000014Z\tnull\tfalse
                            1970-01-01T00:00:00.000015Z\t100.0\tfalse
                            1970-01-01T00:00:00.000016Z\t16.0\tfalse
                            1970-01-01T00:00:00.000017Z\t17.0\tfalse
                            1970-01-01T00:00:00.000018Z\t18.0\tfalse
                            1970-01-01T00:00:00.000019Z\t19.0\ttrue
                            1970-01-01T00:00:00.000020Z\t100.0\ttrue
                            1970-01-01T00:00:00.000021Z\tnull\tfalse
                            1970-01-01T00:00:00.000022Z\t22.0\ttrue
                            1970-01-01T00:00:00.000023Z\t23.0\tfalse
                            1970-01-01T00:00:00.000024Z\t24.0\tfalse
                            1970-01-01T00:00:00.000025Z\t100.0\tfalse
                            1970-01-01T00:00:00.000026Z\t26.0\tfalse
                            1970-01-01T00:00:00.000027Z\t27.0\tfalse
                            1970-01-01T00:00:00.000028Z\tnull\tfalse
                            1970-01-01T00:00:00.000029Z\t29.0\tfalse
                            1970-01-01T00:00:00.000030Z\t100.0\tfalse
                            1970-01-01T00:00:00.000031Z\t31.0\tfalse
                            1970-01-01T00:00:00.000032Z\t32.0\tfalse
                            1970-01-01T00:00:00.000033Z\t33.0\tfalse
                            1970-01-01T00:00:00.000034Z\t34.0\tfalse
                            1970-01-01T00:00:00.000035Z\tnull\tfalse
                            1970-01-01T00:00:00.000036Z\t36.0\tfalse
                            1970-01-01T00:00:00.000037Z\t37.0\tfalse
                            1970-01-01T00:00:00.000038Z\t38.0\tfalse
                            1970-01-01T00:00:00.000039Z\t39.0\tfalse
                            1970-01-01T00:00:00.000040Z\t100.0\ttrue
                            """);

            // Independent captured golden for the clause path. This avoids comparing SUBSAMPLE to
            // the same M4 window implementation while still pinning null-bitset row alignment.
            assertQuery("select ts, v from t SUBSAMPLE m4(v, 8)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            1970-01-01T00:00:00.000001Z\t1.0
                            1970-01-01T00:00:00.000005Z\t100.0
                            1970-01-01T00:00:00.000019Z\t19.0
                            1970-01-01T00:00:00.000020Z\t100.0
                            1970-01-01T00:00:00.000022Z\t22.0
                            1970-01-01T00:00:00.000040Z\t100.0
                            """);
        });
    }

    @Test
    public void testExplainPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            assertQuery("select ts, m4(ts, v, 8) over (order by ts) from t")
                    .noLeakCheck()
                    .assertsPlan("CachedWindowLight\n" +
                            """
                                      unorderedFunctions: [m4(ts,v,8) over (order by [ts])]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                                    """);
        });
    }
}
