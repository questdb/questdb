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
        // the shape that returned rows joined to future quotes on 92926cb701
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
        // generic LatestBy is correct on unordered input; speed is PR 6 (pushdown), not ordering
        assertMemoryLeak(() -> {
            createFixture();
            assertQuery("select * from ((select * from vA union all select * from vB) latest on ts partition by sym) order by sym")
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
                    .failsWith("TIMESTAMP");
        });
    }
}
