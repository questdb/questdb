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
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class UnionOrderDemandTest extends AbstractCairoTest {

    // A/B rows never share a timestamp, so expected outputs have no tie ordering
    static void createFixture() throws Exception {
        execute("create table t (ts timestamp, sym symbol, venue symbol, px double) timestamp(ts) partition by day bypass wal");
        execute("insert into t values ('2024-01-01T00:00:00.000000Z', 'A', 'V1', 1.0)");
        execute("insert into t values ('2024-01-01T00:05:00.000000Z', 'B', 'V2', 10.0)");
        execute("insert into t values ('2024-01-01T00:30:00.000000Z', 'C', 'V1', 100.0)");
        execute("insert into t values ('2024-01-01T01:00:00.000000Z', 'B', 'V1', 20.0)");
        execute("insert into t values ('2024-01-01T01:30:00.000000Z', 'A', 'V2', 2.0)");
        execute("insert into t values ('2024-01-01T02:00:00.000000Z', 'A', 'V1', 3.0)");
        execute("insert into t values ('2024-01-01T02:05:00.000000Z', 'B', 'V2', 30.0)");
        execute("create table q (ts timestamp, venue symbol, bid double) timestamp(ts) partition by day bypass wal");
        execute("insert into q values ('2024-01-01T00:10:00.000000Z', 'V1', 0.1)");
        execute("insert into q values ('2024-01-01T00:50:00.000000Z', 'V2', 0.2)");
        execute("insert into q values ('2024-01-01T01:20:00.000000Z', 'V1', 0.3)");
        execute("insert into q values ('2024-01-01T01:40:00.000000Z', 'V2', 0.4)");
        execute("create view vA as (select * from t where sym = 'A')");
        execute("create view vB as (select * from t where sym = 'B')");
        drainWalAndViewQueues();
    }

    // checks rows alone, ahead of assertQuery's metadata battery, so a wrong-order regression fails on
    // the rows rather than first on the designated-timestamp scan-direction check
    private static void assertRows(String expected, String query) throws Exception {
        printSql(query);
        TestUtils.assertEquals(expected, sink);
    }

    @Test
    public void testAsofUnionOnMasterSide() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();
            assertQuery("select a.ts, a.sym, a.px, q.ts qts, q.bid from (select * from vA union all select * from vB) a asof join q on (venue)")
                    .noLeakCheck()
                    .withPlanContaining("Union All Merge")
                    .inferTimestamp()
                    .inferRandomAccess()
                    .returns("""
                            ts\tsym\tpx\tqts\tbid
                            2024-01-01T00:00:00.000000Z\tA\t1.0\t\tnull
                            2024-01-01T00:05:00.000000Z\tB\t10.0\t\tnull
                            2024-01-01T01:00:00.000000Z\tB\t20.0\t2024-01-01T00:10:00.000000Z\t0.1
                            2024-01-01T01:30:00.000000Z\tA\t2.0\t2024-01-01T00:50:00.000000Z\t0.2
                            2024-01-01T02:00:00.000000Z\tA\t3.0\t2024-01-01T01:20:00.000000Z\t0.3
                            2024-01-01T02:05:00.000000Z\tB\t30.0\t2024-01-01T01:40:00.000000Z\t0.4
                            """);
        });
    }

    @Test
    public void testAsofUnionOnMasterSideWithExplicitTimestamp() throws Exception {
        // the shape that returned rows joined to future quotes on master before #7613
        assertMemoryLeak(() -> {
            createFixture();
            assertQuery("select count() from ((select * from vA union all select * from vB) timestamp(ts)) a asof join q on (venue) where q.ts > a.ts")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("Union All Merge")
                    .returns("""
                            count
                            0
                            """);
        });
    }

    @Test
    public void testAsofUnionOnSlaveSide() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();
            assertQuery("select q.ts, q.venue, b.sym, b.px from q asof join (select * from vA union all select * from vB) b on (venue)")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("Union All Merge")
                    .inferTimestamp()
                    .inferRandomAccess()
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
    public void testSampleByOverUnion() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();
            assertQuery("select ts, sum(px) from (select * from vA union all select * from vB) sample by 1h")
                    .noLeakCheck()
                    .withPlanContaining("Union All Merge")
                    .inferTimestamp()
                    .inferRandomAccess()
                    .returns("""
                            ts\tsum
                            2024-01-01T00:00:00.000000Z\t11.0
                            2024-01-01T01:00:00.000000Z\t22.0
                            2024-01-01T02:00:00.000000Z\t33.0
                            """);
        });
    }

    @Test
    public void testSampleByOverExplicitTimestampUnion() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();
            assertQuery("select ts, sum(px) from ((select * from vA union all select * from vB) timestamp(ts)) sample by 1h")
                    .noLeakCheck()
                    .withPlanContaining("Union All Merge")
                    .inferTimestamp()
                    .inferRandomAccess()
                    .returns("""
                            ts\tsum
                            2024-01-01T00:00:00.000000Z\t11.0
                            2024-01-01T01:00:00.000000Z\t22.0
                            2024-01-01T02:00:00.000000Z\t33.0
                            """);
        });
    }

    @Test
    public void testExplicitTimestampOverUnionWithDescendingBranchFails() throws Exception {
        // the merge cannot run over a descending branch; concatenation would return rows that step
        // backwards at the seam while claiming an ascending designated timestamp, so this must error
        assertMemoryLeak(() -> {
            createFixture();
            assertQuery("select * from ((select * from vA union all (select * from vB order by ts desc)) timestamp(ts))")
                    .noLeakCheck()
                    .failsWith("cannot prove timestamp order of UNION ALL for TIMESTAMP(ts); add ORDER BY ts");
        });
    }

    @Test
    public void testExplicitTimestampOverOrderedUnionKeepsInnerOrderByAscLimit() throws Exception {
        // the sub-query's own ORDER BY px LIMIT 3 defines its rows; an enclosing TIMESTAMP(ts) must not
        // reach through it and turn the union into a timestamp merge, which would pick the first three
        // rows by ts (px 1, 10, 20) instead of the three smallest px
        assertMemoryLeak(() -> {
            createFixture();
            final String query = "select * from ((select ts, px from (select * from vA union all select * from vB) order by px limit 3) timestamp(ts))";
            final String expected = """
                    ts\tpx
                    2024-01-01T00:00:00.000000Z\t1.0
                    2024-01-01T01:30:00.000000Z\t2.0
                    2024-01-01T02:00:00.000000Z\t3.0
                    """;
            assertRows(expected, query);
            assertQuery(query)
                    .noLeakCheck()
                    .withPlanContaining("keys: [px]")
                    // the inner ORDER BY decides the order; merging the union below it is wasted work
                    .withPlanNotContaining("Union All Merge")
                    .timestampUnordered("ts")
                    .inferRandomAccess()
                    .returns(expected);
        });
    }

    @Test
    public void testExplicitTimestampOverOrderedUnionKeepsInnerOrderByDescLimit() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();
            final String query = "select * from ((select ts, px from (select * from vA union all select * from vB) order by px desc limit 3) timestamp(ts))";
            final String expected = """
                    ts\tpx
                    2024-01-01T02:05:00.000000Z\t30.0
                    2024-01-01T01:00:00.000000Z\t20.0
                    2024-01-01T00:05:00.000000Z\t10.0
                    """;
            assertRows(expected, query);
            assertQuery(query)
                    .noLeakCheck()
                    .withPlanContaining("keys: [px desc]")
                    // the inner ORDER BY decides the order; merging the union below it is wasted work
                    .withPlanNotContaining("Union All Merge")
                    .timestampUnordered("ts")
                    .inferRandomAccess()
                    .returns(expected);
        });
    }

    @Test
    public void testExplicitTimestampOverOrderedUnionKeepsInnerOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();
            final String query = "select * from ((select ts, px from (select * from vA union all select * from vB) order by px) timestamp(ts))";
            final String expected = """
                    ts\tpx
                    2024-01-01T00:00:00.000000Z\t1.0
                    2024-01-01T01:30:00.000000Z\t2.0
                    2024-01-01T02:00:00.000000Z\t3.0
                    2024-01-01T00:05:00.000000Z\t10.0
                    2024-01-01T01:00:00.000000Z\t20.0
                    2024-01-01T02:05:00.000000Z\t30.0
                    """;
            assertRows(expected, query);
            assertQuery(query)
                    .noLeakCheck()
                    .withPlanContaining("keys: [px]")
                    // the inner ORDER BY decides the order; merging the union below it is wasted work
                    .withPlanNotContaining("Union All Merge")
                    .timestampUnordered("ts")
                    .inferRandomAccess()
                    .returns(expected);
        });
    }

    @Test
    public void testAsofUnionOnMasterSideKeepsOrderByNonTimestamp() throws Exception {
        // the ASOF operand demands the merge, but ORDER BY px is not the merge's order: the join
        // passes the merge's followedOrderByAdvice() up, so the merge must not claim to follow it
        assertMemoryLeak(() -> {
            createFixture();
            assertQuery("select a.ts, a.px from (select * from vA union all select * from vB) a asof join q on (venue) order by a.px desc limit 3")
                    .noLeakCheck()
                    .withPlanContaining("Union All Merge", "keys: [px desc]")
                    .inferTimestamp()
                    .inferRandomAccess()
                    .returns("""
                            ts\tpx
                            2024-01-01T02:05:00.000000Z\t30.0
                            2024-01-01T01:00:00.000000Z\t20.0
                            2024-01-01T00:05:00.000000Z\t10.0
                            """);
        });
    }

    @Test
    public void testLtUnionOnMasterSideKeepsOrderByNonTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();
            assertQuery("select a.ts, a.px from (select * from vA union all select * from vB) a lt join q on (venue) order by a.px")
                    .noLeakCheck()
                    .withPlanContaining("Union All Merge", "keys: [px]")
                    .inferTimestamp()
                    .inferRandomAccess()
                    .returns("""
                            ts\tpx
                            2024-01-01T00:00:00.000000Z\t1.0
                            2024-01-01T01:30:00.000000Z\t2.0
                            2024-01-01T02:00:00.000000Z\t3.0
                            2024-01-01T00:05:00.000000Z\t10.0
                            2024-01-01T01:00:00.000000Z\t20.0
                            2024-01-01T02:05:00.000000Z\t30.0
                            """);
        });
    }

    @Test
    public void testExplicitTimestampOverUnionOrderedByNonTimestampSameLevel() throws Exception {
        // TIMESTAMP(ts) and ORDER BY px on the same model: the merge satisfies the TIMESTAMP demand, but
        // ORDER BY px is not its order, so the px sort must stay above it
        assertMemoryLeak(() -> {
            createFixture();
            final String query = "(select ts, px from vA union all select ts, px from vB) timestamp(ts) order by px desc limit 3";
            final String expected = """
                    ts\tpx
                    2024-01-01T02:05:00.000000Z\t30.0
                    2024-01-01T01:00:00.000000Z\t20.0
                    2024-01-01T00:05:00.000000Z\t10.0
                    """;
            assertRows(expected, query);
            assertQuery(query)
                    .noLeakCheck()
                    .withPlanContaining("keys: [px desc]")
                    .inferTimestamp()
                    .inferRandomAccess()
                    .returns(expected);
        });
    }

    @Test
    public void testHashJoinOverExplicitTimestampUnionOrderedBySlaveColumn() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();
            final String query = "select a.ts, a.px, q.bid from ((select * from vA union all select * from vB) timestamp(ts)) a join q on (venue) order by q.bid desc, a.px";
            final String expected = """
                    ts\tpx\tbid
                    2024-01-01T01:30:00.000000Z\t2.0\t0.4
                    2024-01-01T00:05:00.000000Z\t10.0\t0.4
                    2024-01-01T02:05:00.000000Z\t30.0\t0.4
                    2024-01-01T00:00:00.000000Z\t1.0\t0.3
                    2024-01-01T02:00:00.000000Z\t3.0\t0.3
                    2024-01-01T01:00:00.000000Z\t20.0\t0.3
                    2024-01-01T01:30:00.000000Z\t2.0\t0.2
                    2024-01-01T00:05:00.000000Z\t10.0\t0.2
                    2024-01-01T02:05:00.000000Z\t30.0\t0.2
                    2024-01-01T00:00:00.000000Z\t1.0\t0.1
                    2024-01-01T02:00:00.000000Z\t3.0\t0.1
                    2024-01-01T01:00:00.000000Z\t20.0\t0.1
                    """;
            assertRows(expected, query);
            assertQuery(query)
                    .noLeakCheck()
                    .withPlanContaining("keys: [bid desc, px]")
                    .inferTimestamp()
                    .inferRandomAccess()
                    .returns(expected);
        });
    }

    @Test
    public void testAsofUnionOnMasterSideOrderedBySlaveColumn() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();
            final String query = "select a.ts, a.px, q.bid from (select * from vA union all select * from vB) a asof join q on (venue) order by bid desc, px";
            final String expected = """
                    ts\tpx\tbid
                    2024-01-01T00:00:00.000000Z\t1.0\tnull
                    2024-01-01T00:05:00.000000Z\t10.0\tnull
                    2024-01-01T02:05:00.000000Z\t30.0\t0.4
                    2024-01-01T02:00:00.000000Z\t3.0\t0.3
                    2024-01-01T01:30:00.000000Z\t2.0\t0.2
                    2024-01-01T01:00:00.000000Z\t20.0\t0.1
                    """;
            assertRows(expected, query);
            assertQuery(query)
                    .noLeakCheck()
                    .withPlanContaining("Union All Merge", "keys: [bid desc, px]")
                    .inferTimestamp()
                    .inferRandomAccess()
                    .returns(expected);
        });
    }

    @Test
    public void testNestedMergeDoesNotInheritOrderClaimAcrossQueryLevels() throws Exception {
        // the inner merge follows its own ORDER BY ts; the outer merge is built for the TIMESTAMP(ts)
        // demand, not for ORDER BY px desc, so it must not inherit the inner merge's claim
        assertMemoryLeak(() -> {
            createFixture();
            final String query = "select ts, px from ((select ts, px from (select ts, px from (select * from vA union all select * from vB) order by ts) union all select ts, px from vA) timestamp(ts)) order by px desc limit 3";
            final String expected = """
                    ts\tpx
                    2024-01-01T02:05:00.000000Z\t30.0
                    2024-01-01T01:00:00.000000Z\t20.0
                    2024-01-01T00:05:00.000000Z\t10.0
                    """;
            assertRows(expected, query);
            assertQuery(query)
                    .noLeakCheck()
                    .withPlanContaining("keys: [px desc]")
                    .inferTimestamp()
                    .inferRandomAccess()
                    .returns(expected);
        });
    }

    @Test
    public void testExplicitTimestampOverUnionOfBranchesWithoutDesignatedTimestamp() throws Exception {
        // branches without a designated timestamp cannot be merged; TIMESTAMP(col) over them is the
        // user's assertion of order and must keep compiling
        assertMemoryLeak(() -> assertQuery("select * from ((select x::timestamp ts, x from long_sequence(2) union all select (x + 2)::timestamp ts, x from long_sequence(2)) timestamp(ts))")
                .noLeakCheck()
                .withPlanContaining("Union All")
                .timestampUnordered("ts")
                .inferRandomAccess()
                .expectSize()
                .returns("""
                        ts\tx
                        1970-01-01T00:00:00.000001Z\t1
                        1970-01-01T00:00:00.000002Z\t2
                        1970-01-01T00:00:00.000003Z\t1
                        1970-01-01T00:00:00.000004Z\t2
                        """));
    }

    @Test
    public void testExplicitTimestampOverUnionIsOrdered() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();
            assertQuery("select * from ((select * from vA union all select * from vB) timestamp(ts))")
                    .noLeakCheck()
                    .withPlanContaining("Union All Merge")
                    .timestampAsc("ts")
                    .inferRandomAccess()
                    .returns("""
                            ts\tsym\tvenue\tpx
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
    public void testWindowOrderByTsOverUnion() throws Exception {
        // existing optimiser hook (uniformWindowOrderColumn); pinned so later layers keep it
        assertMemoryLeak(() -> {
            createFixture();
            assertQuery("select ts, sym, sum(px) over (order by ts) cum from (select * from vA union all select * from vB)")
                    .noLeakCheck()
                    .withPlanContaining("Union All Merge")
                    .inferTimestamp()
                    .inferRandomAccess()
                    .returns("""
                            ts\tsym\tcum
                            2024-01-01T00:00:00.000000Z\tA\t1.0
                            2024-01-01T00:05:00.000000Z\tB\t11.0
                            2024-01-01T01:00:00.000000Z\tB\t31.0
                            2024-01-01T01:30:00.000000Z\tA\t33.0
                            2024-01-01T02:00:00.000000Z\tA\t36.0
                            2024-01-01T02:05:00.000000Z\tB\t66.0
                            """);
        });
    }

    @Test
    public void testLatestOnOverUnionIsCorrect() throws Exception {
        // generic LatestBy is correct on unordered input; its speed is a pushdown question, not ordering
        assertMemoryLeak(() -> {
            createFixture();
            assertQuery("select * from ((select * from vA union all select * from vB) latest on ts partition by venue) order by venue")
                    .noLeakCheck()
                    .expectSize()
                    .inferTimestamp()
                    .inferRandomAccess()
                    .returns("""
                            ts\tsym\tvenue\tpx
                            2024-01-01T02:00:00.000000Z\tA\tV1\t3.0
                            2024-01-01T02:05:00.000000Z\tB\tV2\t30.0
                            """);
        });
    }

    @Test
    public void testUnorderedBranchFailsInsteadOfWrongRows() throws Exception {
        // a descending branch cannot be merged ascending; this must error, never concatenate
        assertMemoryLeak(() -> {
            createFixture();
            assertQuery("select ts, sum(px) from (select * from vA union all (select * from vB order by ts desc)) sample by 1h")
                    .noLeakCheck()
                    .failsWith("base query does not provide designated TIMESTAMP column");
        });
    }
}
