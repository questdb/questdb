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
        // numBuckets=2, but bufferCount(4) <= target(4), so the old SUBSAMPLE cursor's selectAll()
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
            // Byte-identical to the old SUBSAMPLE cursor on the same data (its selectAll() path).
            printSql("select ts, v from t SUBSAMPLE minmax(v, 4)");
            TestUtils.assertEquals("""
                    ts\tv
                    1970-01-01T00:00:00.000001Z\t10.0
                    1970-01-01T00:00:00.000002Z\t20.0
                    1970-01-01T00:00:00.000003Z\t30.0
                    1970-01-01T00:00:00.000004Z\t40.0
                    """, sink);
        });
    }

    @Test
    public void testMatchesMinMaxAlgorithmOnSpike() throws Exception {
        // Deterministic spike; keep min/max per time bucket. Expected output filled from the
        // FIRST (stable) run and cross-checked against the old-cursor
        // "SELECT ts, v FROM t SUBSAMPLE minmax(v, 8) ORDER BY ts" on the same dataset
        // (byte-for-byte match).
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
        // bucket would never be detected. The old SUBSAMPLE cursor
        // (SubsampleRecordCursorFactory.bufferInput()) drops NULL ts / null-or-NaN value rows
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

            // Byte-identical to the old SUBSAMPLE cursor on the same data: same rows kept.
            // Plain printSql/assertEquals (not the fluent assertQuery battery) - the SUBSAMPLE
            // cursor's recordCursorSupportsRandomAccess()/getRecordB() combination doesn't fit
            // assertQuery's noRandomAccess() expectations, same reason SubsampleTest.java uses a
            // bespoke assertSql helper instead of assertQuery for its own SUBSAMPLE assertions.
            printSql("select ts, v from t SUBSAMPLE minmax(v, 4)");
            TestUtils.assertEquals("""
                    ts\tv
                    1970-01-01T00:00:00.000002Z\t5.0
                    1970-01-01T00:00:00.000003Z\t100.0
                    1970-01-01T00:00:00.000004Z\t1.0
                    """, sink);
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
}
