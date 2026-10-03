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

import java.util.List;

/**
 * Checks that queries whose UNION ALL is now merged by timestamp authorise exactly like a
 * hand-written equivalent, under every subset of a small grant lattice.
 * <p>
 * The ASOF tests exercise plans that #7613 already merges, so they are sanity checks. The SAMPLE BY
 * test compares against the hand-written ORDER BY + TIMESTAMP(ts) form, because SAMPLE BY over a
 * plain union did not compile before this change. The proof that merge and concatenation plans of
 * the same query authorise identically is in {@link UnionMergeVsConcatPermissionTest}.
 */
public class UnionOrderDemandPermissionTest extends AbstractCairoTest {
    private static final List<Grant> ATOMS = List.of(
            new Grant.View("vA"),
            new Grant.View("vB"),
            new Grant.Columns("t", "*"),
            new Grant.Columns("q", "*")
    );

    @Test
    public void testAsofMasterSideEquivalent() throws Exception {
        assertMemoryLeak(() -> {
            UnionOrderDemandTest.createFixture();
            EquivalenceHarness.assertEquivalentUnderAllGrants(
                    engine,
                    "select a.ts, a.px, q.bid from (select * from vA union all select * from vB) a asof join q on (venue)",
                    "select a.ts, a.px, q.bid from ((select * from vA union all select * from vB order by ts) timestamp(ts)) a asof join q on (venue)",
                    true,
                    ATOMS
            );
        });
    }

    @Test
    public void testAsofSlaveSideEquivalent() throws Exception {
        assertMemoryLeak(() -> {
            UnionOrderDemandTest.createFixture();
            EquivalenceHarness.assertEquivalentUnderAllGrants(
                    engine,
                    "select q.ts, b.px from q asof join (select * from vA union all select * from vB) b on (venue)",
                    "select q.ts, b.px from q asof join ((select * from vA union all select * from vB order by ts) timestamp(ts)) b on (venue)",
                    true,
                    ATOMS
            );
        });
    }

    @Test
    public void testSampleByEquivalent() throws Exception {
        assertMemoryLeak(() -> {
            UnionOrderDemandTest.createFixture();
            EquivalenceHarness.assertEquivalentUnderAllGrants(
                    engine,
                    "select ts, sum(px) from (select * from vA union all select * from vB) sample by 1h",
                    "select ts, sum(px) from ((select * from vA union all select * from vB order by ts) timestamp(ts)) sample by 1h",
                    true,
                    ATOMS
            );
        });
    }

    @Test
    public void testRevokeAfterCompileDenies() throws Exception {
        assertMemoryLeak(() -> {
            UnionOrderDemandTest.createFixture();
            final List<Grant> all = List.of(new Grant.View("vA"), new Grant.View("vB"), new Grant.Columns("q", "*"));
            EquivalenceHarness.assertRevokeBetweenCompileAndExecuteDenies(
                    engine,
                    "select ts, sum(px) from (select * from vA union all select * from vB) sample by 1h",
                    all,
                    ctx -> ctx.revokeView("vB"),
                    "vb"
            );
            EquivalenceHarness.assertRevokeBetweenCompileAndExecuteDenies(
                    engine,
                    "select a.ts, q.bid from (select * from vA union all select * from vB) a asof join q on (venue)",
                    all,
                    ctx -> ctx.revokeTable("q"),
                    "q.bid"
            );
        });
    }
}
