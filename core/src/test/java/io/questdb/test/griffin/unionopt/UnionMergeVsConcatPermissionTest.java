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
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Runs the SAME union shape once as plain concatenation ("Union All") and once as a
 * timestamp-ordered merge ("Union All Merge"), and proves both forms make identical permission
 * decisions, return identical rows, and trigger identical SELECT checks under every grant subset.
 * The plan assertions guard against the comparison silently degrading to merge-vs-merge.
 */
public class UnionMergeVsConcatPermissionTest extends AbstractCairoTest {
    private static final String BASE_A = "select ts, px from t where sym = 'A'";
    private static final String BASE_B = "select ts, px from t where sym = 'B'";
    private static final List<Grant> BASE_ATOMS = List.of(
            new Grant.Columns("t", "ts"),
            new Grant.Columns("t", "px"),
            new Grant.Columns("t", "sym")
    );

    @Test
    public void testBaseTableBranches() throws Exception {
        assertMemoryLeak(() -> {
            UnionOrderDemandTest.createFixture();
            assertMergeVsConcatEquivalent("ts, px", BASE_A, BASE_B, BASE_ATOMS);
        });
    }

    @Test
    public void testConcatWithoutTimestampReferenceNeedsNoTs() throws Exception {
        // Tripwire: this query never references ts, so it must be allowed without SELECT on t.ts.
        // A future rewrite must never add a column the query does not reference to the read set.
        assertMemoryLeak(() -> {
            UnionOrderDemandTest.createFixture();
            final String sql = "select px from (" + BASE_A + " union all " + BASE_B + ")";
            assertQuery(sql).noLeakCheck().assertsPlanContaining("Union All");
            assertQuery(sql).noLeakCheck().assertsPlanNotContaining("Union All Merge");
            final EquivalenceHarness.Outcome outcome = EquivalenceHarness.run(engine, sql, policyOf(
                    new Grant.Columns("t", "px"),
                    new Grant.Columns("t", "sym")
            ));
            Assert.assertFalse("unexpected denial: " + outcome.rows(), outcome.denied());
            Assert.assertEquals("[COLUMNS t [px, sym]]", outcome.checks().toString());
        });
    }

    @Test
    public void testTimestampClauseRequiresTsWithOrWithoutMerge() throws Exception {
        // TIMESTAMP(ts) itself references ts, so this query needs SELECT on t.ts no matter how it is
        // planned. Evidence: on master with #7613 and #7428 applied, before this change, the same SQL
        // planned as plain concatenation (no merge) and was already denied under {t.px, t.sym}, with
        // full-grant checks COLUMNS t [px, sym, ts]. The merge demand therefore does not change this
        // query's decision.
        assertMemoryLeak(() -> {
            UnionOrderDemandTest.createFixture();
            final String sql = "select px from ((" + BASE_A + " union all " + BASE_B + ") timestamp(ts))";
            assertQuery(sql).noLeakCheck().assertsPlanContaining("Union All Merge");
            final EquivalenceHarness.Outcome denied = EquivalenceHarness.run(engine, sql, policyOf(
                    new Grant.Columns("t", "px"),
                    new Grant.Columns("t", "sym")
            ));
            Assert.assertTrue("expected denial: " + denied.rows(), denied.denied());
            Assert.assertTrue(
                    "expected denial for [object=t.ts] but was: " + denied.rows(),
                    denied.rows().contains("[object=t.ts]")
            );
            final EquivalenceHarness.Outcome allowed = EquivalenceHarness.run(engine, sql, policyOf(
                    new Grant.Columns("t", "px"),
                    new Grant.Columns("t", "sym"),
                    new Grant.Columns("t", "ts")
            ));
            Assert.assertFalse("unexpected denial: " + allowed.rows(), allowed.denied());
            Assert.assertEquals("[COLUMNS t [px, sym, ts]]", allowed.checks().toString());
        });
    }

    @Test
    public void testColumnProjectingViewBranches() throws Exception {
        assertMemoryLeak(() -> {
            UnionOrderDemandTest.createFixture();
            execute("create view vAp as (select ts, px from t where sym = 'A')");
            execute("create view vBp as (select ts, px from t where sym = 'B')");
            drainWalAndViewQueues();
            assertMergeVsConcatEquivalent(
                    "ts, px",
                    "select ts, px from vAp",
                    "select ts, px from vBp",
                    List.of(
                            new Grant.View("vAp"),
                            new Grant.View("vBp"),
                            new Grant.Columns("t", "ts"),
                            new Grant.Columns("t", "px"),
                            new Grant.Columns("t", "sym")
                    )
            );
        });
    }

    @Test
    public void testUnionMasterOfLeftJoin() throws Exception {
        // A LEFT JOIN emits rows in its master's order, so TIMESTAMP(ts) over the join reaches the union master
        // and merges it. The merge must not change which columns the query reads.
        assertMemoryLeak(() -> {
            UnionOrderDemandTest.createFixture();
            execute("create table venues (venue symbol, region symbol)");
            execute("insert into venues values ('V1', 'EU'), ('V2', 'US')");
            final String concat = "select a.ts, a.px, v.region from (select ts, px, venue from t where sym = 'A'"
                    + " union all select ts, px, venue from t where sym = 'B') a left join venues v on (venue)";
            assertMergeVsConcatEquivalent(
                    concat,
                    "select * from (" + concat + ") timestamp(ts)",
                    List.of(
                            new Grant.Columns("t", "ts"),
                            new Grant.Columns("t", "px"),
                            new Grant.Columns("t", "venue"),
                            new Grant.Columns("t", "sym"),
                            new Grant.Columns("venues", "*")
                    )
            );
        });
    }

    @Test
    public void testViewBranchMixedWithBaseTableBranch() throws Exception {
        assertMemoryLeak(() -> {
            UnionOrderDemandTest.createFixture();
            assertMergeVsConcatEquivalent(
                    "ts, px",
                    "select ts, px from vA",
                    "select ts, px from t where sym = 'C'",
                    List.of(
                            new Grant.View("vA"),
                            new Grant.Columns("t", "ts"),
                            new Grant.Columns("t", "px"),
                            new Grant.Columns("t", "sym")
                    )
            );
        });
    }

    private void assertMergeVsConcatEquivalent(String outerColumns, String a, String b, List<Grant> atoms) throws Exception {
        final String concat = "select " + outerColumns + " from (" + a + " union all " + b + ")";
        final String merge = "select " + outerColumns + " from ((" + a + " union all " + b + ") timestamp(ts))";
        assertMergeVsConcatEquivalent(concat, merge, atoms);
    }

    private void assertMergeVsConcatEquivalent(String concat, String merge, List<Grant> atoms) throws Exception {
        assertQuery(concat).noLeakCheck().assertsPlanContaining("Union All");
        assertQuery(concat).noLeakCheck().assertsPlanNotContaining("Union All Merge");
        assertQuery(merge).noLeakCheck().assertsPlanContaining("Union All Merge");
        EquivalenceHarness.assertEquivalentUnderAllGrants(engine, concat, merge, true, atoms);
    }

    private static GrantPolicySecurityContext policyOf(Grant... grants) {
        final GrantPolicySecurityContext policy = new GrantPolicySecurityContext();
        for (Grant grant : grants) {
            grant.applyTo(policy);
        }
        return policy;
    }
}
