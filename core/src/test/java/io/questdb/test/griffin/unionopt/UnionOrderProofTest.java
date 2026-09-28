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

package io.questdb.test.griffin.unionopt;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class UnionOrderProofTest extends AbstractCairoTest {
    private static final String AB_ROWS_ORDERED = """
            ts\tsym\tvenue\tpx
            2024-01-01T00:00:00.000000Z\tA\tV1\t1.0
            2024-01-01T00:05:00.000000Z\tB\tV2\t10.0
            2024-01-01T01:00:00.000000Z\tB\tV1\t20.0
            2024-01-01T01:30:00.000000Z\tA\tV2\t2.0
            2024-01-01T02:00:00.000000Z\tA\tV1\t3.0
            2024-01-01T02:05:00.000000Z\tB\tV2\t30.0
            """;
    private static final String MIXED_UNION = "(select * from vA union all (select * from vB order by px))";
    private static final String JOINED_ROWS_ORDERED = """
            ts\tsym\tpx\tregion
            2024-01-01T00:00:00.000000Z\tA\t1.0\tEU
            2024-01-01T00:05:00.000000Z\tB\t10.0\tUS
            2024-01-01T01:00:00.000000Z\tB\t20.0\tEU
            2024-01-01T01:30:00.000000Z\tA\t2.0\tUS
            2024-01-01T02:00:00.000000Z\tA\t3.0\tEU
            2024-01-01T02:05:00.000000Z\tB\t30.0\tUS
            """;
    private static final String HINT = "cannot prove timestamp order of UNION ALL for TIMESTAMP(ts); add ORDER BY ts";

    @Test
    public void testMergePlanNamesColumnWhenUnionIsAsofSlave() throws Exception {
        assertMemoryLeak(() -> {
            UnionOrderDemandTest.createFixture();
            assertQuery("select q.ts, q.venue, b.sym, b.px from q asof join (select * from vA union all select * from vB) b on (venue)")
                    .withPlanContaining("order: [ts asc]")
                    .withPlanNotContaining("order: [ asc]")
                    .noLeakCheck()
                    .inferTimestamp()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            ts\tvenue\tsym\tpx
                            2024-01-01T00:10:00.000000Z\tV1\tA\t1.0
                            2024-01-01T00:50:00.000000Z\tV2\tB\t10.0
                            2024-01-01T01:20:00.000000Z\tV1\tB\t20.0
                            2024-01-01T01:40:00.000000Z\tV2\tA\t2.0
                            """);
        });
    }

    @Test
    public void testMergePlanOverAsofJoinBranchesDoesNotThrow() throws Exception {
        // getBaseColumnName() used to NPE walking into a branch that is (or wraps) a join: a join's
        // getBaseFactory() is null, and the old Merge.getBaseColumnName() always delegated into
        // branch 0 regardless of whether the merge's own metadata already had a name.
        assertMemoryLeak(() -> {
            UnionOrderDemandTest.createFixture();
            assertQuery("((select t.ts, t.px, q.bid from t asof join q on (venue)) union all (select t.ts, t.px, q.bid from t asof join q on (venue))) order by ts")
                    .noLeakCheck()
                    .assertsPlanContaining("Union All Merge");
        });
    }

    @Test
    public void testMergePlanKeepsRenamedTimestampLabel() throws Exception {
        // A timestamp column that IS user-selected (renamed to k) must keep its real label: the
        // merge's own metadata already names it, so getBaseColumnName() must not walk into branch 0.
        assertMemoryLeak(() -> {
            UnionOrderDemandTest.createFixture();
            assertQuery("((select px, ts as k from t where sym = 'A') union all (select px, ts as k from t where sym = 'B')) order by k")
                    .noLeakCheck()
                    .assertsPlanContaining("Union All Merge", "order: [k asc]");
        });
    }

    @Test
    public void testMergePlanOverAsofJoinBranchesAsAsofSlaveWithImplicitTimestamp() throws Exception {
        // Reaches the null-base fallback in RecordCursorFactory.getBaseColumnName(): the union is an
        // ASOF slave that never selects its own ts (implicit timestamp, empty name on the merge's own
        // metadata, same as testMergePlanNamesColumnWhenUnionIsAsofSlave), AND each branch is itself
        // an ASOF join, so branch 0 (sourceFactories.getQuick(0)) is a SelectedRecordCursorFactory
        // wrapping a join whose getBaseFactory() is null. testMergePlanOverAsofJoinBranchesDoesNotThrow
        // does not cover this: it selects the timestamp explicitly, so it never reaches the default's
        // null-base branch and would still pass if that fallback were reverted.
        assertMemoryLeak(() -> {
            UnionOrderDemandTest.createFixture();
            execute("create table q2 (ts timestamp, venue symbol, bid double) timestamp(ts) partition by day bypass wal");
            execute("insert into q2 values ('2024-01-01T00:10:00.000000Z', 'V1', 0.1)");
            execute("insert into q2 values ('2024-01-01T00:50:00.000000Z', 'V2', 0.2)");
            execute("insert into q2 values ('2024-01-01T01:20:00.000000Z', 'V1', 0.3)");
            execute("insert into q2 values ('2024-01-01T01:40:00.000000Z', 'V2', 0.4)");
            assertQuery("select q.ts, u.px from q asof join ((select t.ts, t.venue, t.px, q2.bid from t asof join q2 on (venue)) union all (select t.ts, t.venue, t.px, q2.bid from t asof join q2 on (venue))) u on (venue)")
                    .noLeakCheck()
                    .inferTimestamp()
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("Union All Merge")
                    .withPlanContaining("order: [t.ts asc]")
                    .returns("""
                            ts\tpx
                            2024-01-01T00:10:00.000000Z\t1.0
                            2024-01-01T00:50:00.000000Z\t10.0
                            2024-01-01T01:20:00.000000Z\t20.0
                            2024-01-01T01:40:00.000000Z\t2.0
                            """);
        });
    }

    @Test
    public void testMixedBranchesFailWithHint() throws Exception {
        assertMemoryLeak(() -> {
            UnionOrderDemandTest.createFixture();
            assertQuery("select * from ((select * from vA union all (select * from vB order by px)) timestamp(ts))")
                    .noLeakCheck().failsWith(HINT);
            assertQuery("select * from (((select * from vA union all (select * from vB order by px)) order by ts) timestamp(ts))")
                    .noLeakCheck().timestampAsc("ts").inferRandomAccess().returns(AB_ROWS_ORDERED);
        });
    }

    @Test
    public void testDescendingBranchFailsWithHint() throws Exception {
        assertMemoryLeak(() -> {
            UnionOrderDemandTest.createFixture();
            assertQuery("select * from ((select * from vA union all (select * from vB order by ts desc)) timestamp(ts))")
                    .noLeakCheck().failsWith(HINT);
            assertQuery("select * from (((select * from vA union all (select * from vB order by ts desc)) order by ts) timestamp(ts))")
                    .noLeakCheck().timestampAsc("ts").inferRandomAccess().returns(AB_ROWS_ORDERED);
        });
    }

    @Test
    public void testTimestampTypeMismatchFailsWithHint() throws Exception {
        assertMemoryLeak(() -> {
            UnionOrderDemandTest.createFixture();
            execute("create table tn (ts timestamp_ns, sym symbol, venue symbol, px double) timestamp(ts) partition by day bypass wal");
            execute("insert into tn values ('2024-01-01T00:07:00.000000000Z', 'N', 'V1', 5.0)");
            assertQuery("select * from ((select * from vA union all select * from tn) timestamp(ts))")
                    .noLeakCheck().failsWith(HINT);
            assertQuery("select * from (((select * from vA union all select * from tn) order by ts) timestamp(ts))")
                    .noLeakCheck().timestampAsc("ts").inferRandomAccess()
                    .returns("""
                            ts\tsym\tvenue\tpx
                            2024-01-01T00:00:00.000000000Z\tA\tV1\t1.0
                            2024-01-01T00:07:00.000000000Z\tN\tV1\t5.0
                            2024-01-01T01:30:00.000000000Z\tA\tV2\t2.0
                            2024-01-01T02:00:00.000000000Z\tA\tV1\t3.0
                            """);
        });
    }

    @Test
    public void testTimestampPositionMismatchFailsWithHint() throws Exception {
        // Both branches have a designated timestamp of the same type, but at different column indexes: 0 in
        // branch A, 1 in t3 (whose ts2 lands in the union's ts column). The merge needs the same position, so
        // the union concatenates and the order of ts cannot be proven.
        assertMemoryLeak(() -> {
            UnionOrderDemandTest.createFixture();
            execute("create table t3 (ts2 timestamp, ts timestamp, sym symbol, px double) timestamp(ts) partition by day bypass wal");
            execute("insert into t3 values ('2024-01-01T00:10:00.000000Z', '2024-01-01T00:20:00.000000Z', 'D', 7.0)");
            final String u = "(select ts, ts ts2, sym, px from vA union all select ts2, ts, sym, px from t3)";
            assertQuery("select * from (" + u + " timestamp(ts))").noLeakCheck().failsWith(HINT);
            assertQuery("select * from ((" + u + " order by ts) timestamp(ts))")
                    .noLeakCheck().timestampAsc("ts").inferRandomAccess()
                    .returns("""
                            ts\tts2\tsym\tpx
                            2024-01-01T00:00:00.000000Z\t2024-01-01T00:00:00.000000Z\tA\t1.0
                            2024-01-01T00:10:00.000000Z\t2024-01-01T00:20:00.000000Z\tD\t7.0
                            2024-01-01T01:30:00.000000Z\t2024-01-01T01:30:00.000000Z\tA\t2.0
                            2024-01-01T02:00:00.000000Z\t2024-01-01T02:00:00.000000Z\tA\t3.0
                            """);
        });
    }

    @Test
    public void testTimestampOnNonDesignatedColumnFailsWithHint() throws Exception {
        // TIMESTAMP(ts2) names a copy of ts, not the branches' designated timestamp. Branch A still has a
        // designated timestamp and the merge failed (branch B is sorted by px), so the order of ts2 cannot be
        // proven either; the declaration is no longer trusted and the user is asked for ORDER BY.
        assertMemoryLeak(() -> {
            UnionOrderDemandTest.createFixture();
            final String u = "(select ts, ts ts2, px from vA union all (select ts, ts ts2, px from vB order by px))";
            assertQuery("select * from (" + u + " timestamp(ts2))")
                    .noLeakCheck().failsWith("cannot prove timestamp order of UNION ALL for TIMESTAMP(ts2); add ORDER BY ts2");
            assertQuery("select * from ((" + u + " order by ts2) timestamp(ts2))")
                    .noLeakCheck().timestampAsc("ts2").inferRandomAccess()
                    .returns("""
                            ts\tts2\tpx
                            2024-01-01T00:00:00.000000Z\t2024-01-01T00:00:00.000000Z\t1.0
                            2024-01-01T00:05:00.000000Z\t2024-01-01T00:05:00.000000Z\t10.0
                            2024-01-01T01:00:00.000000Z\t2024-01-01T01:00:00.000000Z\t20.0
                            2024-01-01T01:30:00.000000Z\t2024-01-01T01:30:00.000000Z\t2.0
                            2024-01-01T02:00:00.000000Z\t2024-01-01T02:00:00.000000Z\t3.0
                            2024-01-01T02:05:00.000000Z\t2024-01-01T02:05:00.000000Z\t30.0
                            """);
        });
    }

    @Test
    public void testUnprovableUnionUnderLimitFailsWithHint() throws Exception {
        assertMemoryLeak(() -> {
            UnionOrderDemandTest.createFixture();
            // without the declaration, the Limit sits above the concatenating union
            assertQuery("select * from (" + MIXED_UNION + " limit 10)").noLeakCheck()
                    .assertsPlanContaining("""
                            Limit value: 10 skip-rows-max: 0 take-rows-max: 10
                                UnionSymbolCast
                                  functions: [ts,sym::symbol,venue::symbol,px]
                                    Union All
                            """);
            assertQuery("select * from ((" + MIXED_UNION + " limit 10) timestamp(ts))").noLeakCheck().failsWith(HINT);
            assertQuery("select * from (((" + MIXED_UNION + " order by ts) limit 10) timestamp(ts))")
                    .noLeakCheck()
                    .withPlanContaining("""
                            Limit value: 10 skip-rows-max: 0 take-rows-max: 10
                                UnionSymbolCast
                                  functions: [ts,sym::symbol,venue::symbol,px]
                                    Union All Merge
                            """)
                    .timestampAsc("ts").inferRandomAccess()
                    .returns(AB_ROWS_ORDERED);
        });
    }

    @Test
    public void testUnprovableUnionUnderFilterFailsWithHint() throws Exception {
        assertMemoryLeak(() -> {
            UnionOrderDemandTest.createFixture();
            // without the declaration, the Filter sits above the concatenating union (not pushed into the branches)
            assertQuery("select * from " + MIXED_UNION + " where px > 0").noLeakCheck()
                    .assertsPlanContaining("""
                            SelectedRecord
                                Filter filter: 0<px
                                    UnionSymbolCast
                                      functions: [px,ts,sym::symbol,venue::symbol]
                                        Union All
                            """);
            assertQuery("select * from ((select * from " + MIXED_UNION + " where px > 0) timestamp(ts))").noLeakCheck().failsWith(HINT);
            assertQuery("select * from ((select * from (" + MIXED_UNION + " order by ts) where px > 0) timestamp(ts))")
                    .noLeakCheck()
                    .withPlanContaining("""
                            SelectedRecord
                                Filter filter: 0<px
                                    UnionSymbolCast
                                      functions: [px,ts,sym::symbol,venue::symbol]
                                        Union All Merge
                            """)
                    .timestampAsc("ts").inferRandomAccess()
                    .returns(AB_ROWS_ORDERED);
        });
    }

    @Test
    public void testUnprovableUnionUnderVirtualFailsWithHint() throws Exception {
        assertMemoryLeak(() -> {
            UnionOrderDemandTest.createFixture();
            // without the declaration, the computed projection stays above the concatenating union
            assertQuery("select ts, px * 2 p2 from " + MIXED_UNION).noLeakCheck()
                    .assertsPlanContaining("""
                            VirtualRecord
                              functions: [ts,px*2]
                                Union All
                            """);
            assertQuery("select * from ((select ts, px * 2 p2 from " + MIXED_UNION + ") timestamp(ts))").noLeakCheck().failsWith(HINT);
            assertQuery("select * from ((select ts, px * 2 p2 from (" + MIXED_UNION + " order by ts)) timestamp(ts))")
                    .noLeakCheck()
                    .withPlanContaining("""
                            VirtualRecord
                              functions: [ts,px*2]
                                Union All Merge
                            """)
                    .timestampAsc("ts").inferRandomAccess()
                    .returns("""
                            ts\tp2
                            2024-01-01T00:00:00.000000Z\t2.0
                            2024-01-01T00:05:00.000000Z\t20.0
                            2024-01-01T01:00:00.000000Z\t40.0
                            2024-01-01T01:30:00.000000Z\t4.0
                            2024-01-01T02:00:00.000000Z\t6.0
                            2024-01-01T02:05:00.000000Z\t60.0
                            """);
        });
    }

    @Test
    public void testProjectionPushedIntoBranchesFailsWithHint() throws Exception {
        // A plain projection is pushed into the branches, so no wrapper sits above the union. Branch A is then a
        // projection whose metadata used to be stripped of its designated timestamp by the union (removeTimestamp
        // mutated it in place), which made the union look like one over branches without a designated timestamp.
        assertMemoryLeak(() -> {
            UnionOrderDemandTest.createFixture();
            assertQuery("select ts, px from " + MIXED_UNION).noLeakCheck()
                    .assertsPlanContaining("""
                            Union All
                                SelectedRecord
                            """);
            assertQuery("select * from ((select ts, px from " + MIXED_UNION + ") timestamp(ts))").noLeakCheck().failsWith(HINT);
        });
    }

    @Test
    public void testNestedUnprovableUnionUnderLimitFailsWithHint() throws Exception {
        // The outer union's first branch is computed (no designated timestamp); the second hides an unprovable
        // union behind a Limit, which shares the union's timestamp-less metadata.
        assertMemoryLeak(() -> {
            UnionOrderDemandTest.createFixture();
            final String u = "(select (x * 1000000)::timestamp ts, 'S' sym, 'V0' venue, 0.5 px from long_sequence(2) union all ("
                    + MIXED_UNION + " limit 10))";
            assertQuery("select * from (" + u + " timestamp(ts))").noLeakCheck().failsWith(HINT);
            assertQuery("select * from ((" + u + " order by ts) timestamp(ts))")
                    .noLeakCheck().timestampAsc("ts").inferRandomAccess()
                    .returns("""
                            ts\tsym\tvenue\tpx
                            1970-01-01T00:00:01.000000Z\tS\tV0\t0.5
                            1970-01-01T00:00:02.000000Z\tS\tV0\t0.5
                            2024-01-01T00:00:00.000000Z\tA\tV1\t1.0
                            2024-01-01T00:05:00.000000Z\tB\tV2\t10.0
                            2024-01-01T01:00:00.000000Z\tB\tV1\t20.0
                            2024-01-01T01:30:00.000000Z\tA\tV2\t2.0
                            2024-01-01T02:00:00.000000Z\tA\tV1\t3.0
                            2024-01-01T02:05:00.000000Z\tB\tV2\t30.0
                            """);
        });
    }

    @Test
    public void testComputedBranchesStayTrusted() throws Exception {
        assertMemoryLeak(() -> assertQuery(
                "select * from ((select (x * 1000000)::timestamp ts from long_sequence(2) union all select (x * 1000000 + 5000000)::timestamp ts from long_sequence(2)) timestamp(ts))")
                .inferTimestamp().inferRandomAccess().expectSize()
                .returns("""
                        ts
                        1970-01-01T00:00:01.000000Z
                        1970-01-01T00:00:02.000000Z
                        1970-01-01T00:00:06.000000Z
                        1970-01-01T00:00:07.000000Z
                        """));
    }

    @Test
    public void testInnerJoinWithUnionMasterMergesUnderTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            UnionOrderDemandTest.createFixture();
            createVenues();
            assertQuery("select * from ((select a.ts, a.sym, a.px, v.region from (select * from vA union all select * from vB) a join venues v on (venue)) timestamp(ts))")
                    .withPlanContaining("Union All Merge")
                    .noLeakCheck().timestampAsc("ts").inferRandomAccess()
                    .returns(JOINED_ROWS_ORDERED);
        });
    }

    @Test
    public void testLeftJoinWithUnionMasterMergesUnderTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            UnionOrderDemandTest.createFixture();
            createVenues();
            assertQuery("select * from ((select a.ts, a.sym, a.px, v.region from (select * from vA union all select * from vB) a left join venues v on (venue)) timestamp(ts))")
                    .withPlanContaining("Union All Merge")
                    .noLeakCheck().timestampAsc("ts").inferRandomAccess()
                    .returns(JOINED_ROWS_ORDERED);
        });
    }

    @Test
    public void testLeftJoinWithUnionMasterStaysConcatWithoutTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            UnionOrderDemandTest.createFixture();
            createVenues();
            assertQuery("select a.ts, a.sym, a.px, v.region from (select * from vA union all select * from vB) a left join venues v on (venue)")
                    .withPlanContaining("Union All")
                    .withPlanNotContaining("Union All Merge")
                    .noLeakCheck().inferTimestamp().inferRandomAccess()
                    .returns("""
                            ts\tsym\tpx\tregion
                            2024-01-01T00:00:00.000000Z\tA\t1.0\tEU
                            2024-01-01T01:30:00.000000Z\tA\t2.0\tUS
                            2024-01-01T02:00:00.000000Z\tA\t3.0\tEU
                            2024-01-01T00:05:00.000000Z\tB\t10.0\tUS
                            2024-01-01T01:00:00.000000Z\tB\t20.0\tEU
                            2024-01-01T02:05:00.000000Z\tB\t30.0\tUS
                            """);
        });
    }

    private static void createVenues() throws Exception {
        execute("create table venues (venue symbol, region symbol)");
        execute("insert into venues values ('V1', 'EU'), ('V2', 'US')");
    }
}
