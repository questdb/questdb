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

public class LttbWindowFunctionTest extends AbstractCairoTest {

    @Test
    public void testBindVariableTarget() throws Exception {
        // lttb(ts, value, target) accepts a runtime-constant (bind-variable) target, read PER-EXECUTION:
        // the SAME compiled factory produces keep-all vs downsampled keep-sets as $1 is re-bound between
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
        // Re-bind $1 = 2 on the same compiled factory: count(6) > 2 -> LTTB with no interior buckets
        // keeps only first (row 1) and last (row 6). A different result from the keep-all case above
        // proves the target is read at execution, not frozen at compile.
        cases.add(BindVarTuple.ok(
                "target 2 (re-bind, first+last only)",
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
                26,
                "target points must be at least 2",
                bindVariableService -> bindVariableService.setLong(0, 1)
        ));

        assertQuery("select ts, v, lttb(ts, v, $1) over (order by ts) keep from t")
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
                select("select ts, v, lttb(ts, v, 1) over (order by ts) keep from t");
                Assert.fail("expected compilation to fail for an out-of-range constant target");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "target points must be at least 2");
                Assert.assertEquals(26, e.getPosition());
            }
        });
    }

    @Test
    public void testKeepsAllWhenFewRows() throws Exception {
        // n=3, target=8 -> count <= target: the base's keepAll short-circuit fires before
        // LttbAlgorithm.select ever runs, so all rows are kept regardless of triangle areas.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            execute("insert into t values (1::timestamp,10.0),(2::timestamp,20.0),(3::timestamp,30.0)");
            assertQuery("select ts, v, lttb(ts, v, 8) over (order by ts) keep from t")
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
        // increasing rows with target=4. Plain LTTB bucketing (numBuckets=2 -> first, one point per
        // interior bucket, last) would likely keep all 4 anyway here, but the point of this test is
        // that bufferCount(4) <= target(4) takes the selectAll() path unconditionally, matching the
        // captured legacy selectAll behavior - not that LTTB's own math happens to agree.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            execute("insert into t values (1::timestamp,10.0),(2::timestamp,20.0),(3::timestamp,30.0),(4::timestamp,40.0)");
            assertQuery("select ts, v, lttb(ts, v, 4) over (order by ts) keep from t")
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
            assertQuery("select ts, v from t SUBSAMPLE lttb(v, 4)")
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
    public void testKeepsNanoTimestampPrecision() throws Exception {
        // double ulp near a 2024 nanosecond epoch (~1.7e18) is 256ns, so LttbAlgorithm must
        // compute triangle areas from long timestamp differences rather than absolute epochs
        // converted to double. Candidates sit 1ns apart with exact areas 4, 400, 8: the
        // algorithm must keep id 2, not fall back to the first candidate on an all-zero tie.
        assertMemoryLeak(() -> {
            execute("create table t (id int, v double, ts timestamp_ns) timestamp(ts)");
            execute("""
                    insert into t values
                    (0, 0.0, '2024-01-01T00:00:00.000000000Z'),
                    (1, 1.0, '2024-01-01T00:00:00.000000001Z'),
                    (2, 100.0, '2024-01-01T00:00:00.000000002Z'),
                    (3, 2.0, '2024-01-01T00:00:00.000000003Z'),
                    (4, 0.0, '2024-01-01T00:00:00.000000004Z')
                    """);
            assertQuery("select id, lttb(ts, v, 3) over (order by ts) keep from t")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tkeep
                            0\ttrue
                            1\tfalse
                            2\ttrue
                            3\tfalse
                            4\ttrue
                            """);
        });
    }

    @Test
    public void testMatchesLttbAlgorithmOnTenPoints() throws Exception {
        // Same dataset as SubsampleTest.testLttbBasic (10 points -> target 5): first and last are
        // always kept, plus one point per interior bucket chosen by largest triangle area. Expected
        // keep flags filled from the captured legacy golden for the same dataset.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            execute("""
                    insert into t values
                    ('2024-01-01T00:00:00.000000Z'::timestamp, 10.0),
                    ('2024-01-01T01:00:00.000000Z'::timestamp, 20.0),
                    ('2024-01-01T02:00:00.000000Z'::timestamp, 50.0),
                    ('2024-01-01T03:00:00.000000Z'::timestamp, 30.0),
                    ('2024-01-01T04:00:00.000000Z'::timestamp, 15.0),
                    ('2024-01-01T05:00:00.000000Z'::timestamp, 45.0),
                    ('2024-01-01T06:00:00.000000Z'::timestamp, 25.0),
                    ('2024-01-01T07:00:00.000000Z'::timestamp, 35.0),
                    ('2024-01-01T08:00:00.000000Z'::timestamp, 5.0),
                    ('2024-01-01T09:00:00.000000Z'::timestamp, 40.0)
                    """);
            assertQuery("select ts, v from (select ts, v, lttb(ts, v, 5) over (order by ts) keep from t) where keep")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize(false)
                    .returns("""
                            ts\tv
                            2024-01-01T00:00:00.000000Z\t10.0
                            2024-01-01T02:00:00.000000Z\t50.0
                            2024-01-01T04:00:00.000000Z\t15.0
                            2024-01-01T08:00:00.000000Z\t5.0
                            2024-01-01T09:00:00.000000Z\t40.0
                            """);

            // Captured legacy golden, asserted through the sole window-only clause path.
            assertQuery("select ts, v from t SUBSAMPLE lttb(v, 5)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            2024-01-01T00:00:00.000000Z\t10.0
                            2024-01-01T02:00:00.000000Z\t50.0
                            2024-01-01T04:00:00.000000Z\t15.0
                            2024-01-01T08:00:00.000000Z\t5.0
                            2024-01-01T09:00:00.000000Z\t40.0
                            """);
        });
    }

    @Test
    public void testGapPreservingSplitsSegments() throws Exception {
        // Same dataset as SubsampleTest.testLttbGapPreserving: an 4.5h gap between 00:30 and 05:00
        // with threshold '1h' splits the data into two 4-row segments, each budgeted 2 of the 4 target
        // points (proportional split) -> first+last of each segment.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            execute("""
                    insert into t values
                    ('2024-01-01T00:00:00.000000Z'::timestamp, 10.0),
                    ('2024-01-01T00:10:00.000000Z'::timestamp, 20.0),
                    ('2024-01-01T00:20:00.000000Z'::timestamp, 30.0),
                    ('2024-01-01T00:30:00.000000Z'::timestamp, 40.0),
                    ('2024-01-01T05:00:00.000000Z'::timestamp, 50.0),
                    ('2024-01-01T05:10:00.000000Z'::timestamp, 60.0),
                    ('2024-01-01T05:20:00.000000Z'::timestamp, 70.0),
                    ('2024-01-01T05:30:00.000000Z'::timestamp, 80.0)
                    """);
            assertQuery("select ts, v, lttb(ts, v, 4, '1h') over (order by ts) keep from t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv\tkeep
                            2024-01-01T00:00:00.000000Z\t10.0\ttrue
                            2024-01-01T00:10:00.000000Z\t20.0\tfalse
                            2024-01-01T00:20:00.000000Z\t30.0\tfalse
                            2024-01-01T00:30:00.000000Z\t40.0\ttrue
                            2024-01-01T05:00:00.000000Z\t50.0\ttrue
                            2024-01-01T05:10:00.000000Z\t60.0\tfalse
                            2024-01-01T05:20:00.000000Z\t70.0\tfalse
                            2024-01-01T05:30:00.000000Z\t80.0\ttrue
                            """);

            // Captured legacy gap-preserving golden, asserted through the window-only clause path.
            assertQuery("select ts, v from t SUBSAMPLE lttb(v, 4, '1h')")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            2024-01-01T00:00:00.000000Z\t10.0
                            2024-01-01T00:30:00.000000Z\t40.0
                            2024-01-01T05:00:00.000000Z\t50.0
                            2024-01-01T05:30:00.000000Z\t80.0
                            """);
        });
    }

    @Test
    public void testGapPreservingNoGapsFallsBackToPlainLttb() throws Exception {
        // Same dataset as SubsampleTest.testLttbGapPreservingNoGaps: no gap exceeds the '2h'
        // threshold, so one segment covers all 5 rows - same result as plain lttb(ts, v, 2).
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            execute("""
                    insert into t values
                    ('2024-01-01T00:00:00.000000Z'::timestamp, 10.0),
                    ('2024-01-01T01:00:00.000000Z'::timestamp, 50.0),
                    ('2024-01-01T02:00:00.000000Z'::timestamp, 20.0),
                    ('2024-01-01T03:00:00.000000Z'::timestamp, 30.0),
                    ('2024-01-01T04:00:00.000000Z'::timestamp, 40.0)
                    """);
            assertQuery("select ts, v, lttb(ts, v, 2, '2h') over (order by ts) keep from t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv\tkeep
                            2024-01-01T00:00:00.000000Z\t10.0\ttrue
                            2024-01-01T01:00:00.000000Z\t50.0\tfalse
                            2024-01-01T02:00:00.000000Z\t20.0\tfalse
                            2024-01-01T03:00:00.000000Z\t30.0\tfalse
                            2024-01-01T04:00:00.000000Z\t40.0\ttrue
                            """);
        });
    }

    @Test
    public void testFiltersNullAndNaNRows() throws Exception {
        // A NULL/NaN value must be dropped before bucketing, exactly like m4/minmax: the old SUBSAMPLE
        // SUBSAMPLE drops NULL ts / null-or-NaN value rows
        // before ever handing the buffer to the algorithm. Non-null count(3) <= target(4) here, so this
        // also exercises the keepAll short-circuit on top of the null filter (matches
        // M4WindowFunctionTest.testFiltersNullAndNaNRows's shape, function swapped to lttb).
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            execute("""
                    insert into t values
                    (1::timestamp, null),
                    (2::timestamp, 5.0),
                    (3::timestamp, 100.0),
                    (4::timestamp, 1.0)
                    """);
            assertQuery("select ts, v, lttb(ts, v, 4) over (order by ts) keep from t")
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

            // Captured legacy NULL-filter golden, asserted through the window-only clause path.
            assertQuery("select ts, v from t SUBSAMPLE lttb(v, 4)")
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
    public void testRejectsNonNumericValue() throws Exception {
        // SYMBOL is not implicitly castable to DOUBLE, so the overload resolver itself rejects this
        // before newInstance() ever runs, like M4WindowFunctionTest.testRejectsNonNumericValue - but
        // unlike m4 (a single 3-arg signature), lttb has two candidate signatures (3-arg and 4-arg
        // gap overload); with neither matching, FunctionParser can't pin the mismatch to one specific
        // argument and falls back to its generic "no matching function" diagnostic instead of the
        // single-candidate "argument type mismatch" message.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, s symbol) timestamp(ts)");
            assertQuery("select ts, lttb(ts, s, 8) over (order by ts) from t")
                    .noLeakCheck()
                    .fails(11, "there is no matching function `lttb` with the argument types: (TIMESTAMP, SYMBOL, INT)");
        });
    }

    @Test
    public void testRejectsNonNumericValueThatOverloadResolutionLetsThrough() throws Exception {
        // CHAR is implicitly widenable to DOUBLE per ColumnType's overload rules, so it reaches
        // newInstance() and exercises our manual numeric-type guard and its SUBSAMPLE-cursor-matching
        // message - matches M4WindowFunctionTest's equivalent test.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, c char) timestamp(ts)");
            assertQuery("select ts, lttb(ts, c, 8) over (order by ts) from t")
                    .noLeakCheck()
                    .fails(20, "numeric column expected, got: CHAR");
        });
    }

    @Test
    public void testRejectsNonConstantTarget() throws Exception {
        // lttb's target is uppercase 'L' in the signature ("lttb(NDL)", widened from the former
        // constant-only 'l' so a bind-variable target can reach newInstance), so - exactly like m4 -
        // a non-constant column target reaches newInstance0 and gets the friendly accept-check message
        // rather than FunctionParser's generic "no matching function" diagnostic.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            assertQuery("select ts, lttb(ts, v, v::long) over (order by ts) from t")
                    .noLeakCheck()
                    .fails(24, "target must be a constant or bind variable");
        });
    }

    @Test
    public void testRejectsTargetBelowTwo() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            assertQuery("select ts, lttb(ts, v, 1) over (order by ts) from t")
                    .noLeakCheck()
                    .fails(23, "target points must be at least 2");
        });
    }

    @Test
    public void testGapRejectsInvalidUnit() throws Exception {
        // Matches SubsampleTest.testLttbGapInvalidUnit's message; the gap arg is constant-enforced by
        // the signature ("lttb(NDls)"), so a malformed constant string reaches newInstance and this
        // factory's own parseGapThreshold reproduces the old cursor's error exactly.
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            assertQuery("select ts, lttb(ts, v, 5, '1M') over (order by ts) from t")
                    .noLeakCheck()
                    .fails(28, "unsupported interval unit");
        });
    }

    @Test
    public void testExplainPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            assertQuery("select ts, lttb(ts, v, 8) over (order by ts) from t")
                    .noLeakCheck()
                    .assertsPlan("CachedWindowLight\n" +
                            """
                                      unorderedFunctions: [lttb(ts,v,8) over (order by [ts])]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                                    """);
        });
    }

    @Test
    public void testExplainPlanWithGap() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, v double) timestamp(ts)");
            assertQuery("select ts, lttb(ts, v, 8, '1h') over (order by ts) from t")
                    .noLeakCheck()
                    .assertsPlan("CachedWindowLight\n" +
                            """
                                      unorderedFunctions: [lttb(ts,v,8) over (order by [ts])]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                                    """);
        });
    }
}
