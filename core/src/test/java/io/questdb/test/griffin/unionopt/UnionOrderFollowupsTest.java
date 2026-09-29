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

public class UnionOrderFollowupsTest extends AbstractCairoTest {
    private static final String AB_ROWS_ORDERED = """
            ts\tsym\tvenue\tpx
            2024-01-01T00:00:00.000000Z\tA\tV1\t1.0
            2024-01-01T00:05:00.000000Z\tB\tV2\t10.0
            2024-01-01T01:00:00.000000Z\tB\tV1\t20.0
            2024-01-01T01:30:00.000000Z\tA\tV2\t2.0
            2024-01-01T02:00:00.000000Z\tA\tV1\t3.0
            2024-01-01T02:05:00.000000Z\tB\tV2\t30.0
            """;
    // venue V9 has no vA row, so the RIGHT/FULL join appends it with a null ts
    private static final String A_JOINED_ROWS_ORDERED = """
            ts\tpx\tregion
            \tnull\tZZ
            2024-01-01T00:00:00.000000Z\t1.0\tEU
            2024-01-01T01:30:00.000000Z\t2.0\tUS
            2024-01-01T02:00:00.000000Z\t3.0\tEU
            """;
    // (a.px > 2.5 or v.region = 'US') leaves 00:00 (V1, 1.0, EU) unmatched, and a RIGHT join drops unmatched master rows
    private static final String A_FILTERED_JOINED_ROWS_ORDERED = """
            ts\tpx\tregion
            \tnull\tZZ
            2024-01-01T01:30:00.000000Z\t2.0\tUS
            2024-01-01T02:00:00.000000Z\t3.0\tEU
            """;
    private static final String CROSS_V1_ROWS_ORDERED = """
            ts\tpx\tregion
            2024-01-01T00:00:00.000000Z\t1.0\tEU
            2024-01-01T00:05:00.000000Z\t10.0\tEU
            2024-01-01T01:00:00.000000Z\t20.0\tEU
            2024-01-01T01:30:00.000000Z\t2.0\tEU
            2024-01-01T02:00:00.000000Z\t3.0\tEU
            2024-01-01T02:05:00.000000Z\t30.0\tEU
            """;
    // every A/B row matches its venue except 02:05 (px 30.0 fails a.px < 25), which the LEFT join keeps unmatched
    private static final String CROSS_LEFT_ROWS_ORDERED = """
            ts\tpx\tregion
            2024-01-01T00:00:00.000000Z\t1.0\tEU
            2024-01-01T00:05:00.000000Z\t10.0\tUS
            2024-01-01T01:00:00.000000Z\t20.0\tEU
            2024-01-01T01:30:00.000000Z\t2.0\tUS
            2024-01-01T02:00:00.000000Z\t3.0\tEU
            2024-01-01T02:05:00.000000Z\t30.0\t
            """;
    private static final String FULL_FILTERED_JOINED_ROWS_ORDERED = """
            ts\tpx\tregion
            \tnull\tZZ
            2024-01-01T00:00:00.000000Z\t1.0\t
            2024-01-01T01:30:00.000000Z\t2.0\tUS
            2024-01-01T02:00:00.000000Z\t3.0\tEU
            """;
    private static final String JOIN_HINT = "cannot prove timestamp order of RIGHT/FULL JOIN for TIMESTAMP(ts); add ORDER BY ts";
    private static final String UNION_ALL_HINT = "cannot prove timestamp order of UNION ALL for TIMESTAMP(ts); add ORDER BY ts";
    private static final String UNION_HINT = "cannot prove timestamp order of UNION for TIMESTAMP(ts); add ORDER BY ts";

    @Test
    public void testCrossJoinOverRightJoinFailsWithHint() throws Exception {
        // the CROSS join's master is the RIGHT join; the walk looks through the CROSS join and reports it
        assertMemoryLeak(() -> {
            createFixture();
            final String join = "select a.ts, a.px, v.region, c.region cr from vA a right join venues v on (venue)"
                    + " cross join (venues where venue = 'V1') c";
            assertQuery(join).noLeakCheck().assertsPlanContaining("Cross Join", "Hash Right Outer Join Light");
            assertQuery("select * from ((" + join + ") timestamp(ts))").noLeakCheck().failsWith(JOIN_HINT);
        });
    }

    @Test
    public void testCrossJoinWithUnionMasterMergesUnderTimestamp() throws Exception {
        // CrossJoinRecordCursorFactory iterates its master on the outside, so TIMESTAMP(ts) over the join
        // reaches the union master and merges it
        assertMemoryLeak(() -> {
            createFixture();
            assertQuery("select * from ((select a.ts, a.px, v.region from (select * from vA union all select * from vB) a"
                    + " cross join (venues where venue = 'V1') v) timestamp(ts))")
                    .noLeakCheck()
                    .withPlanContaining("Cross Join", "Union All Merge")
                    .timestampAsc("ts").inferRandomAccess()
                    .returns(CROSS_V1_ROWS_ORDERED);
        });
    }

    @Test
    public void testCrossJoinWithUnprovableUnionMasterFailsWithHint() throws Exception {
        // branch B is sorted by px descending, so the union cannot merge; the walk must look through the CROSS join's
        // master and report the union instead of trusting the join
        assertMemoryLeak(() -> {
            createFixture();
            final String join = "select a.ts, a.px, v.region from (select * from vA union all (select * from vB order by px desc)) a"
                    + " cross join (venues where venue = 'V1') v";
            assertQuery(join).noLeakCheck().assertsPlanContaining("Cross Join", "Union All");
            assertQuery(join).noLeakCheck().assertsPlanNotContaining("Union All Merge");
            assertQuery("select * from ((" + join + ") timestamp(ts))").noLeakCheck().failsWith(UNION_ALL_HINT);
            assertQuery("select * from (((" + join + ") order by ts) timestamp(ts))")
                    .noLeakCheck().timestampAsc("ts").inferRandomAccess().returns(CROSS_V1_ROWS_ORDERED);
        });
    }

    @Test
    public void testCrossLeftJoinWithUnionMasterMergesUnderTimestamp() throws Exception {
        // a non-equi LEFT join is planned as JOIN_CROSS_LEFT (Nested Loop Left Join), which iterates its
        // master on the outside and emits each unmatched master row in place
        assertMemoryLeak(() -> {
            createFixture();
            final String join = "select a.ts, a.px, v.region from (select * from vA union all select * from vB) a"
                    + " left join venues v on a.venue::string = v.venue::string and a.px < 25";
            assertQuery(join).noLeakCheck().assertsPlanContaining("Nested Loop Left Join");
            assertQuery("select * from ((" + join + ") timestamp(ts))")
                    .noLeakCheck()
                    .withPlanContaining("Nested Loop Left Join", "Union All Merge")
                    .timestampAsc("ts").inferRandomAccess()
                    .returns(CROSS_LEFT_ROWS_ORDERED);
        });
    }

    @Test
    public void testCrossLeftJoinWithUnprovableUnionMasterFailsWithHint() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();
            final String join = "select a.ts, a.px, v.region from (select * from vA union all (select * from vB order by px desc)) a"
                    + " left join venues v on a.venue::string = v.venue::string and a.px < 25";
            assertQuery(join).noLeakCheck().assertsPlanContaining("Nested Loop Left Join", "Union All");
            assertQuery(join).noLeakCheck().assertsPlanNotContaining("Union All Merge");
            assertQuery("select * from ((" + join + ") timestamp(ts))").noLeakCheck().failsWith(UNION_ALL_HINT);
            assertQuery("select * from (((" + join + ") order by ts) timestamp(ts))")
                    .noLeakCheck().timestampAsc("ts").inferRandomAccess().returns(CROSS_LEFT_ROWS_ORDERED);
        });
    }

    @Test
    public void testFullJoinFullFatWithDesignatedMasterFailsWithHint() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();
            final String join = "select a.ts, a.px, v.region from vA a full join venues v on (venue)";
            assertQuery("select * from ((" + join + ") timestamp(ts))").noLeakCheck().fullFatJoins().failsWith(JOIN_HINT);
        });
    }

    @Test
    public void testFullJoinFullFatWithFilterFailsWithHint() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();
            final String join = "select a.ts, a.px, v.region from vA a full join venues v on a.venue = v.venue and (a.px > 2.5 or v.region = 'US')";
            assertQuery("select * from ((" + join + ") timestamp(ts))").noLeakCheck().fullFatJoins().failsWith(JOIN_HINT);
        });
    }

    @Test
    public void testFullJoinWithDesignatedMasterFailsWithHint() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();
            final String join = "select a.ts, a.px, v.region from vA a full join venues v on (venue)";
            assertQuery(join).noLeakCheck().assertsPlanContaining("Hash Full Outer Join Light");
            assertQuery("select * from ((" + join + ") timestamp(ts))").noLeakCheck().failsWith(JOIN_HINT);
            assertQuery("select * from (((" + join + ") order by ts) timestamp(ts))")
                    .noLeakCheck().timestampAsc("ts").inferRandomAccess().returns(A_JOINED_ROWS_ORDERED);
        });
    }

    @Test
    public void testFullJoinWithFilterFailsWithHint() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();
            final String join = "select a.ts, a.px, v.region from vA a full join venues v on a.venue = v.venue and (a.px > 2.5 or v.region = 'US')";
            assertQuery(join).noLeakCheck().assertsPlanContaining("Hash Full Outer Join Light", "filter:");
            assertQuery("select * from ((" + join + ") timestamp(ts))").noLeakCheck().failsWith(JOIN_HINT);
            assertQuery("select * from (((" + join + ") order by ts) timestamp(ts))")
                    .noLeakCheck().timestampAsc("ts").inferRandomAccess().returns(FULL_FILTERED_JOINED_ROWS_ORDERED);
        });
    }

    @Test
    public void testHintUnquotesTimestampColumn() throws Exception {
        // Pins parser behaviour: TIMESTAMP()'s quoted argument reaches the code generator already
        // unquoted (SqlParser's expectLiteral unquotes it), so the hint names the column as a user types it.
        assertMemoryLeak(() -> {
            createFixture();
            assertQuery("select * from ((select * from vA union all (select * from vB order by px)) timestamp(\"ts\"))")
                    .noLeakCheck().failsWith(UNION_ALL_HINT);
        });
    }

    @Test
    public void testMergePlanNamesImplicitTimestampThroughLimit() throws Exception {
        // Branch 0 is a LIMIT over a wrapper whose timestamp column is implicitly appended and blank-named.
        // The merge labels "order:" through branch 0's getBaseColumnName(), which must reach past the LIMIT.
        assertMemoryLeak(() -> {
            createFixture();
            assertQuery("select q.ts, u.px from q asof join ((select * from (vA limit 10)) union all (select * from (vB limit 10))) u on (venue)")
                    .noLeakCheck()
                    .withPlanContaining("Union All Merge", "order: [ts asc]")
                    .withPlanNotContaining("order: [ asc]")
                    .expectSize()
                    .inferTimestamp().inferRandomAccess()
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
    public void testMergePlanNamesImplicitTimestampThroughSymbolCast() throws Exception {
        // Branch 0 of the outer merge is a SelectedRecord over the inner union's UnionSymbolCast, which is not
        // flattened. The outer merge labels "order:" through it, so UnionSymbolCast must reach its base's name.
        assertMemoryLeak(() -> {
            createFixture();
            assertQuery("select q.ts, u.px from q asof join (((vA limit 10) union all (vB limit 10)) union all (vB limit 10)) u on (venue)")
                    .noLeakCheck()
                    .withPlanContaining("UnionSymbolCast", "Union All Merge", "order: [ts asc]")
                    .withPlanNotContaining("order: [ asc]")
                    .expectSize()
                    .inferTimestamp().inferRandomAccess()
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
    public void testNestedLoopFullJoinWithDesignatedMasterFailsWithHint() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();
            final String join = "select a.ts, a.px, v.region from vA a full join venues v on a.venue::string = v.venue::string and (a.px > 2.5 or v.region = 'US')";
            assertQuery(join).noLeakCheck().assertsPlanContaining("Nested Loop Full Join");
            assertQuery("select * from ((" + join + ") timestamp(ts))").noLeakCheck().failsWith(JOIN_HINT);
            assertQuery("select * from (((" + join + ") order by ts) timestamp(ts))")
                    .noLeakCheck().timestampAsc("ts").inferRandomAccess().returns(FULL_FILTERED_JOINED_ROWS_ORDERED);
        });
    }

    @Test
    public void testNestedLoopRightJoinOrderedByTimestampIsSorted() throws Exception {
        // the ORDER BY sorts the join's output; the join reports no scan direction, so the sort is kept
        assertMemoryLeak(() -> {
            createFixture();
            final String join = "select a.ts, a.px, v.region from vA a right join venues v on a.venue::string = v.venue::string and (a.px > 2.5 or v.region = 'US')";
            assertQuery("(" + join + ") timestamp(ts) order by ts")
                    .noLeakCheck()
                    .withPlanContaining("Encode sort\n  keys: [ts]", "Nested Loop Right Join")
                    .timestampAsc("ts").inferRandomAccess()
                    .returns(A_FILTERED_JOINED_ROWS_ORDERED);
            assertQuery("(" + join + ") timestamp(ts) order by ts desc").noLeakCheck().failsWith(JOIN_HINT);
        });
    }

    @Test
    public void testNestedLoopRightJoinWithDesignatedMasterFailsWithHint() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();
            final String join = "select a.ts, a.px, v.region from vA a right join venues v on a.venue::string = v.venue::string and (a.px > 2.5 or v.region = 'US')";
            assertQuery(join).noLeakCheck().assertsPlanContaining("Nested Loop Right Join");
            assertQuery("select * from ((" + join + ") timestamp(ts))").noLeakCheck().failsWith(JOIN_HINT);
            assertQuery("select * from (((" + join + ") order by ts) timestamp(ts))")
                    .noLeakCheck().timestampAsc("ts").inferRandomAccess().returns(A_FILTERED_JOINED_ROWS_ORDERED);
        });
    }

    @Test
    public void testOrderByAliasOfTimestampColumnIsSorted() throws Exception {
        // TIMESTAMP(ts) names the source column and ORDER BY t2 names its output alias; both resolve to the
        // same source column, so the sort makes the declaration true
        assertMemoryLeak(() -> {
            createFixture();
            assertQuery("select ts t2, px from (select * from vA union select * from vB) timestamp(ts) order by t2")
                    .noLeakCheck()
                    .withPlanContaining("Encode sort\n  keys: [t2]", "Union\n")
                    .timestampAsc("t2").inferRandomAccess()
                    .returns("""
                            t2\tpx
                            2024-01-01T00:00:00.000000Z\t1.0
                            2024-01-01T00:05:00.000000Z\t10.0
                            2024-01-01T01:00:00.000000Z\t20.0
                            2024-01-01T01:30:00.000000Z\t2.0
                            2024-01-01T02:00:00.000000Z\t3.0
                            2024-01-01T02:05:00.000000Z\t30.0
                            """);
            assertQuery("select ts t2, px from (select a.ts, a.px, v.region from vA a right join venues v on (venue)) timestamp(ts) order by t2")
                    .noLeakCheck()
                    .withPlanContaining("Encode sort\n  keys: [t2]", "Hash Right Outer Join Light")
                    .timestampAsc("t2").inferRandomAccess()
                    .returns("""
                            t2\tpx
                            \tnull
                            2024-01-01T00:00:00.000000Z\t1.0
                            2024-01-01T01:30:00.000000Z\t2.0
                            2024-01-01T02:00:00.000000Z\t3.0
                            """);
        });
    }

    @Test
    public void testOrderByAliasShadowingTimestampColumnFailsWithHint() throws Exception {
        // TIMESTAMP(ts) names the source ts (output t0), while ORDER BY ts names the output alias of px:
        // the same token, different columns. Sorting by px would leave t0 unordered.
        assertMemoryLeak(() -> {
            createFixture();
            assertQuery("select px as ts, ts as t0 from (select * from vA union select * from vB) timestamp(ts) order by ts")
                    .noLeakCheck().failsWith(UNION_HINT);
            assertQuery("select px as ts, ts as t0 from (select a.ts, a.px, v.region from vA a right join venues v on (venue)) timestamp(ts) order by ts")
                    .noLeakCheck().failsWith(JOIN_HINT);
        });
    }

    @Test
    public void testRightJoinFullFatWithDesignatedMasterFailsWithHint() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();
            final String join = "select a.ts, a.px, v.region from vA a right join venues v on (venue)";
            assertQuery("select * from ((" + join + ") timestamp(ts))").noLeakCheck().fullFatJoins().failsWith(JOIN_HINT);
        });
    }

    @Test
    public void testRightJoinFullFatWithFilterFailsWithHint() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();
            final String join = "select a.ts, a.px, v.region from vA a right join venues v on a.venue = v.venue and (a.px > 2.5 or v.region = 'US')";
            assertQuery("select * from ((" + join + ") timestamp(ts))").noLeakCheck().fullFatJoins().failsWith(JOIN_HINT);
        });
    }

    @Test
    public void testRightJoinInUnionAllBranchFailsWithHint() throws Exception {
        // the RIGHT join drops branch A's designated timestamp, so the union cannot merge; the union is reported
        assertMemoryLeak(() -> {
            createFixture();
            assertQuery("select * from (((select a.ts, a.px from vA a right join venues v on (venue)) union all (select ts, px from vB)) timestamp(ts))")
                    .noLeakCheck().failsWith(UNION_ALL_HINT);
        });
    }

    @Test
    public void testRightJoinOrderedByTimestampIsSorted() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();
            final String join = "select a.ts, a.px, v.region from vA a right join venues v on (venue)";
            assertQuery("(" + join + ") timestamp(ts) order by ts")
                    .noLeakCheck()
                    .withPlanContaining("Encode sort\n  keys: [ts]", "Hash Right Outer Join Light")
                    .timestampAsc("ts").inferRandomAccess()
                    .returns(A_JOINED_ROWS_ORDERED);
            assertQuery("(" + join + ") timestamp(ts) order by ts desc").noLeakCheck().failsWith(JOIN_HINT);
        });
    }

    @Test
    public void testRightJoinWithDesignatedMasterFailsWithHint() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();
            final String join = "select a.ts, a.px, v.region from vA a right join venues v on (venue)";
            assertQuery(join).noLeakCheck().assertsPlanContaining("Hash Right Outer Join Light");
            assertQuery("select * from ((" + join + ") timestamp(ts))").noLeakCheck().failsWith(JOIN_HINT);
            assertQuery("select * from (((" + join + ") order by ts) timestamp(ts))")
                    .noLeakCheck().timestampAsc("ts").inferRandomAccess().returns(A_JOINED_ROWS_ORDERED);
        });
    }

    @Test
    public void testRightJoinWithFilterFailsWithHint() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();
            final String join = "select a.ts, a.px, v.region from vA a right join venues v on a.venue = v.venue and (a.px > 2.5 or v.region = 'US')";
            assertQuery(join).noLeakCheck().assertsPlanContaining("Hash Right Outer Join Light", "filter:");
            assertQuery("select * from ((" + join + ") timestamp(ts))").noLeakCheck().failsWith(JOIN_HINT);
            assertQuery("select * from (((" + join + ") order by ts) timestamp(ts))")
                    .noLeakCheck().timestampAsc("ts").inferRandomAccess().returns(A_FILTERED_JOINED_ROWS_ORDERED);
        });
    }

    @Test
    public void testRightJoinWithoutDesignatedTimestampStaysTrusted() throws Exception {
        // neither side has a designated timestamp, so TIMESTAMP(ts) stays the user's assertion of order
        assertMemoryLeak(() -> assertQuery(
                "select * from ((select a.ts, a.k, b.k bk from (select (x * 1000000)::timestamp ts, x k from long_sequence(3)) a " +
                        "right join (select x k from long_sequence(4)) b on a.k = b.k) timestamp(ts))")
                .timestampUnordered("ts").inferRandomAccess()
                .returns("""
                        ts\tk\tbk
                        1970-01-01T00:00:01.000000Z\t1\t1
                        1970-01-01T00:00:02.000000Z\t2\t2
                        1970-01-01T00:00:03.000000Z\t3\t3
                        \tnull\t4
                        """));
    }

    @Test
    public void testUnionDistinctOfComputedBranchesStaysTrusted() throws Exception {
        assertMemoryLeak(() -> assertQuery(
                "select * from ((select (x * 1000000)::timestamp ts from long_sequence(2) union select (x * 1000000 + 5000000)::timestamp ts from long_sequence(2)) timestamp(ts))")
                .timestampUnordered("ts").inferRandomAccess()
                .returns("""
                        ts
                        1970-01-01T00:00:01.000000Z
                        1970-01-01T00:00:02.000000Z
                        1970-01-01T00:00:06.000000Z
                        1970-01-01T00:00:07.000000Z
                        """));
    }

    @Test
    public void testUnionDistinctOrderedByOtherColumnFirstFailsWithHint() throws Exception {
        // an ORDER BY that does not lead with ts leaves the rows unordered by ts
        assertMemoryLeak(() -> {
            createFixture();
            assertQuery("(select * from vA union select * from vB) timestamp(ts) order by px, ts")
                    .noLeakCheck().failsWith(UNION_HINT);
        });
    }

    @Test
    public void testUnionDistinctOrderedByTimestampDescFailsWithHint() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();
            assertQuery("(select * from vA union select * from vB) timestamp(ts) order by ts desc")
                    .noLeakCheck().failsWith(UNION_HINT);
        });
    }

    @Test
    public void testUnionDistinctOrderedByTimestampIsSorted() throws Exception {
        // the ORDER BY that sorts the TIMESTAMP-bearing select makes the declaration true
        assertMemoryLeak(() -> {
            createFixture();
            assertQuery("(select * from vA union select * from vB) timestamp(ts) order by ts")
                    .noLeakCheck()
                    .withPlanContaining("Encode sort\n  keys: [ts]", "Union\n")
                    .timestampAsc("ts").inferRandomAccess()
                    .returns(AB_ROWS_ORDERED);
            assertQuery("(select * from vA union select * from vB) timestamp(ts) order by ts, px")
                    .noLeakCheck()
                    .withPlanContaining("Encode sort\n  keys: [ts, px]", "Union\n")
                    .timestampAsc("ts").inferRandomAccess()
                    .returns(AB_ROWS_ORDERED);
        });
    }

    @Test
    public void testUnionDistinctWithDesignatedBranchesFailsWithHint() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();
            assertQuery("select * from ((select * from vA union select * from vB) timestamp(ts))")
                    .noLeakCheck().failsWith(UNION_HINT);
            assertQuery("select * from (((select * from vA union select * from vB) order by ts) timestamp(ts))")
                    .noLeakCheck().timestampAsc("ts").inferRandomAccess().returns(AB_ROWS_ORDERED);
        });
    }

    private static void createFixture() throws Exception {
        UnionOrderDemandTest.createFixture();
        execute("create table venues (venue symbol, region symbol)");
        execute("insert into venues values ('V1', 'EU'), ('V2', 'US'), ('V9', 'ZZ')");
    }
}
