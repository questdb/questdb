/*******************************************************************************
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
import io.questdb.test.cairo.CairoTestConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Verifies the read-path behaviour of Task 12: on a "skipping primary" node (one where
 * {@link io.questdb.cairo.CairoConfiguration#skipReplicaOnlyIndexes()} returns true), a SYMBOL column
 * whose index is flagged REPLICA ONLY is treated as un-indexed by the query planner. The index is never
 * materialized on such a node (Task 11), so the planner MUST NOT emit an index scan / indexed LATEST BY
 * over the absent index -- it must full-scan instead, and queries must still return correct results.
 */
public class ReplicaOnlyIndexPlannerTest extends AbstractCairoTest {

    @BeforeClass
    public static void setUpStatic() throws Exception {
        configurationFactory = (root, telemetry, overrides) ->
                new CairoTestConfiguration(root, telemetry, overrides) {
                    @Override
                    public boolean skipReplicaOnlyIndexes() {
                        return true;
                    }
                };
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testLatestByFullScansCorrectly() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y (s symbol index replica only, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into y values ('a',1,0),('a',2,1000000),('b',3,2000000)");
            drainWalQueue();
            sink.clear();
            printSql("select s, v, ts from y latest on ts partition by s", sink);
            TestUtils.assertEquals(
                    "s\tv\tts\n" +
                            "a\t2.0\t1970-01-01T00:00:01.000000Z\n" +
                            "b\t3.0\t1970-01-01T00:00:02.000000Z\n",
                    sink);

            // plan must NOT use an indexed LATEST BY for s (the index is not materialized on a skipping primary)
            assertQuery("select s, v, ts from y latest on ts partition by s")
                    .noLeakCheck()
                    .assertsPlanNotContaining("Indexed", "Index backward scan", "Index forward scan");
        });
    }

    // An aliased projection over a replica-only-indexed symbol column (SqlCodeGenerator's
    // generateSelectChoose alias branch) must carry the replicaOnly flag onto the projected
    // metadata. If the flag is dropped, the alias (s2) looks like a normal materialized index,
    // and a WHERE = over the projection picks a symbol index scan over the absent index, which
    // fails / returns wrong results on a skipping primary. With the flag preserved, the planner
    // must full-scan and return correct rows.
    @Test
    public void testAliasedProjectionPreservesReplicaOnlyFlagAndFullScans() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (s symbol index capacity 256 replica only, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into x values ('a',1,0),('b',2,1000000),('a',3,2000000)");
            drainWalQueue();

            final String query = "select s2, v, ts from (select s as s2, v, ts from x) where s2 = 'a'";
            sink.clear();
            printSql(query, sink);
            TestUtils.assertEquals(
                    "s2\tv\tts\n" +
                            "a\t1.0\t1970-01-01T00:00:00.000000Z\n" +
                            "a\t3.0\t1970-01-01T00:00:02.000000Z\n",
                    sink);

            // plan must NOT use a symbol index scan over the aliased column on a skipping primary
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlanNotContaining("Index forward scan", "Index backward scan", "DeferredSingleSymbolFilterPageFrame");
        });
    }

    @Test
    public void testWhereEqualsFullScansCorrectly() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (s symbol index capacity 256 replica only, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into x values ('a',1,0),('b',2,1000000),('a',3,2000000)");
            drainWalQueue();
            sink.clear();
            printSql("select s, v, ts from x where s = 'a'", sink);
            TestUtils.assertEquals(
                    "s\tv\tts\n" +
                            "a\t1.0\t1970-01-01T00:00:00.000000Z\n" +
                            "a\t3.0\t1970-01-01T00:00:02.000000Z\n",
                    sink);

            // plan must NOT use a symbol index scan for s -- it must full-scan the frames instead
            assertQuery("select s, v, ts from x where s = 'a'")
                    .noLeakCheck()
                    .assertsPlanNotContaining("Index forward scan", "Index backward scan", "DeferredSingleSymbolFilterPageFrame");
        });
    }
}
