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
}
