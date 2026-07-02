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

package io.questdb.test.griffin;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class MergeUnionAllTest extends AbstractCairoTest {

    @Test
    public void testChainedUnionAllDescMergesFully() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (px double, ts timestamp) timestamp(ts) partition by day");
            execute("create table b (px double, ts timestamp) timestamp(ts) partition by day");
            execute("create table c (px double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into a values (10.0, 1), (40.0, 4)");
            execute("insert into b values (20.0, 2), (50.0, 5)");
            execute("insert into c values (30.0, 3), (60.0, 6)");
            assertQuery("select px from ((select * from a) union all (select * from b) union all (select * from c)) order by ts desc")
                    .withPlanContaining("Union All Merge")
                    .withPlanNotContaining("Encode sort")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            px
                            60.0
                            50.0
                            40.0
                            30.0
                            20.0
                            10.0
                            """);
        });
    }

    @Test
    public void testChainedUnionAllMergesFully() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (px double, ts timestamp) timestamp(ts) partition by day");
            execute("create table b (px double, ts timestamp) timestamp(ts) partition by day");
            execute("create table c (px double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into a values (10.0, 1), (40.0, 4)");
            execute("insert into b values (20.0, 2), (50.0, 5)");
            execute("insert into c values (30.0, 3), (60.0, 6)");
            assertQuery("select px from ((select * from a) union all (select * from b) union all (select * from c)) order by ts")
                    .withPlanContaining("Union All Merge")
                    .withPlanNotContaining("Encode sort")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            px
                            10.0
                            20.0
                            30.0
                            40.0
                            50.0
                            60.0
                            """);
        });
    }

    @Test
    public void testDescTiesPreserveABeforeB() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (px double, ts timestamp) timestamp(ts) partition by day");
            execute("create table b (px double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into a values (30.0, 3), (21.0, 2)");
            execute("insert into b values (22.0, 2), (10.0, 1)");
            assertQuery("select px from ((select * from a) union all (select * from b)) order by ts desc")
                    .withPlanContaining("Union All Merge", "order: [ts desc]")
                    .withPlanNotContaining("Sort")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            px
                            30.0
                            21.0
                            22.0
                            10.0
                            """);
        });
    }

    @Test
    public void testDescWindowOverUnionStreams() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (px double, ts timestamp) timestamp(ts) partition by day");
            execute("create table b (px double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into a values (10.0, 1), (30.0, 3)");
            execute("insert into b values (20.0, 2), (40.0, 4)");
            assertQuery("select px, first_value(px) over (order by ts desc) " +
                    "from ((select * from a order by ts desc) union all (select * from b order by ts desc))")
                    .withPlanContaining("Window", "Union All Merge", "order: [ts desc]", "Frame backward scan on: a", "Frame backward scan on: b")
                    .withPlanNotContaining("CachedWindow")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            px\tfirst_value
                            40.0\t40.0
                            30.0\t40.0
                            20.0\t40.0
                            10.0\t40.0
                            """);
        });
    }

    @Test
    public void testExplicitOrderByTsMergesAndElidesSort() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (px double, ts timestamp) timestamp(ts) partition by day");
            execute("create table b (px double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into a values (10.0, 1), (30.0, 3), (50.0, 5)");
            execute("insert into b values (20.0, 2), (40.0, 4)");
            assertQuery("select px from ((select * from a) union all (select * from b)) order by ts")
                    .withPlanContaining("Union All Merge", "order: [ts asc]")
                    .withPlanNotContaining("Sort")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            px
                            10.0
                            20.0
                            30.0
                            40.0
                            50.0
                            """);
        });
    }

    @Test
    public void testFilteredBranchStillMerges() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (px double, ts timestamp) timestamp(ts) partition by day");
            execute("create table b (px double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into a values (10.0, 1), (30.0, 3), (50.0, 5)");
            execute("insert into b values (20.0, 2), (40.0, 4)");
            assertQuery("select px from ((select * from a where px > 15.0) union all (select * from b)) order by ts")
                    .withPlanContaining("Union All Merge")
                    .withPlanNotContaining("Sort")
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            px
                            20.0
                            30.0
                            40.0
                            50.0
                            """);
        });
    }

    @Test
    public void testInnerUnionAsBranchMergesFully() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (px double, ts timestamp) timestamp(ts) partition by day");
            execute("create table b (px double, ts timestamp) timestamp(ts) partition by day");
            execute("create table c (px double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into a values (10.0, 1), (40.0, 4)");
            execute("insert into b values (20.0, 2), (50.0, 5)");
            execute("insert into c values (30.0, 3), (60.0, 6)");
            assertQuery("select px from ((select * from a) union all (select * from ((select * from b) union all (select * from c)))) order by ts desc")
                    .withPlanContaining("Union All Merge")
                    .withPlanNotContaining("Encode sort")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            px
                            60.0
                            50.0
                            40.0
                            30.0
                            20.0
                            10.0
                            """);
        });
    }

    @Test
    public void testMixedChainInnerMergesOuterConcats() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (px double, ts timestamp) timestamp(ts) partition by day");
            execute("create table b (px double, ts timestamp) timestamp(ts) partition by day");
            execute("create table c (px double, ts timestamp_ns) timestamp(ts) partition by day");
            execute("insert into a values (10.0, 1), (40.0, 4)");
            execute("insert into b values (20.0, 2)");
            execute("insert into c values (30.0, 3000)");
            assertQuery("select px from (((select * from a) union all (select * from b)) union all (select * from c)) order by ts")
                    .withPlanContaining("Union All Merge", "Encode sort")
                    .expectSize()
                    .returns("""
                            px
                            10.0
                            20.0
                            30.0
                            40.0
                            """);
        });
    }

    @Test
    public void testMultiKeyWindowOverDoesNotMerge() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (px double, ts timestamp) timestamp(ts) partition by day");
            execute("create table b (px double, ts timestamp) timestamp(ts) partition by day");
            assertQuery("select first_value(px) over (order by ts, px) from ((select * from a) union all (select * from b))")
                    .assertsPlanNotContaining("Union All Merge");
        });
    }

    @Test
    public void testMultipleWindowsDifferentOrdersDoNotMerge() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (px double, ts timestamp) timestamp(ts) partition by day");
            execute("create table b (px double, ts timestamp) timestamp(ts) partition by day");
            assertQuery("select first_value(px) over (order by ts), first_value(px) over (order by ts desc) " +
                    "from ((select * from a) union all (select * from b))")
                    .assertsPlanNotContaining("Union All Merge");
        });
    }

    @Test
    public void testPlainUnionWithoutOrderStaysConcat() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (px double, ts timestamp) timestamp(ts) partition by day");
            execute("create table b (px double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into a values (10.0, 1), (30.0, 3)");
            execute("insert into b values (20.0, 2)");
            assertQuery("(select px from a) union all (select px from b)")
                    .withPlanContaining("Union All")
                    .withPlanNotContaining("Union All Merge")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            px
                            10.0
                            30.0
                            20.0
                            """);
        });
    }

    @Test
    public void testTiesPreserveABeforeB() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (px double, ts timestamp) timestamp(ts) partition by day");
            execute("create table b (px double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into a values (10.0, 1), (21.0, 2)");
            execute("insert into b values (22.0, 2), (30.0, 3)");
            assertQuery("select px from ((select * from a) union all (select * from b)) order by ts")
                    .withPlanContaining("Union All Merge")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            px
                            10.0
                            21.0
                            22.0
                            30.0
                            """);
        });
    }

    @Test
    public void testTsTypeMismatchFallsBackToConcat() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (px double, ts timestamp) timestamp(ts) partition by day");
            execute("create table b (px double, ts timestamp_ns) timestamp(ts) partition by day");
            execute("insert into a values (10.0, 1)");
            execute("insert into b values (20.0, 2)");
            assertQuery("select px from ((select * from a) union all (select * from b)) order by ts")
                    .withPlanNotContaining("Union All Merge")
                    .expectSize()
                    .returns("""
                            px
                            20.0
                            10.0
                            """);
        });
    }

    @Test
    public void testTwoWindowsSameOrderMerge() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (px double, ts timestamp) timestamp(ts) partition by day");
            execute("create table b (px double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into a values (10.0, 1), (30.0, 3)");
            execute("insert into b values (20.0, 2), (40.0, 4)");
            assertQuery("select first_value(px) over (order by ts), sum(px) over (order by ts) " +
                    "from ((select * from a) union all (select * from b))")
                    .withPlanContaining("Window", "Union All Merge")
                    .withPlanNotContaining("CachedWindow")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            first_value\tsum
                            10.0\t10.0
                            10.0\t30.0
                            10.0\t60.0
                            10.0\t100.0
                            """);
        });
    }

    @Test
    public void testWindowOrderByNonTimestampDoesNotMerge() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (px double, ts timestamp) timestamp(ts) partition by day");
            execute("create table b (px double, ts timestamp) timestamp(ts) partition by day");
            assertQuery("select first_value(px) over (partition by ts order by px) from ((select * from a) union all (select * from b))")
                    .assertsPlanNotContaining("Union All Merge");
        });
    }

    @Test
    public void testWindowOverSymbolUnionStreams() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (sym symbol, px double, ts timestamp) timestamp(ts) partition by day");
            execute("create table b (sym symbol, px double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into a values ('x', 10.0, 1), ('y', 20.0, 3)");
            execute("insert into b values ('x', 30.0, 2), ('z', 40.0, 4)");
            assertQuery("select sym, px, first_value(px) over (partition by sym order by ts) " +
                    "from ((select * from a) union all (select * from b))")
                    .withPlanContaining("Window", "Union All Merge")
                    .withPlanNotContaining("CachedWindow")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            sym\tpx\tfirst_value
                            x\t10.0\t10.0
                            x\t30.0\t10.0
                            y\t20.0\t20.0
                            z\t40.0\t40.0
                            """);
        });
    }
}
