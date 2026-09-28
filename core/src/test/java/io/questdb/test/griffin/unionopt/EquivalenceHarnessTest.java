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

public class EquivalenceHarnessTest extends AbstractCairoTest {

    @Test
    public void testPolicyDeniesUngrantedViewAndRecordsChecks() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, sym symbol, px double) timestamp(ts) partition by day bypass wal");
            execute("insert into t values ('2024-01-01T00:00:00.000000Z', 'A', 1.0)");
            execute("create view vA as (select * from t where sym = 'A')");
            drainWalAndViewQueues();

            final GrantPolicySecurityContext allowed = new GrantPolicySecurityContext();
            new Grant.View("vA").applyTo(allowed);
            final EquivalenceHarness.Outcome ok = EquivalenceHarness.run(engine, "select * from vA", allowed);
            Assert.assertFalse(ok.denied());
            Assert.assertTrue(ok.checks().toString(), ok.checks().contains("VIEW va"));

            final GrantPolicySecurityContext none = new GrantPolicySecurityContext();
            final EquivalenceHarness.Outcome denied = EquivalenceHarness.run(engine, "select * from vA", none);
            Assert.assertTrue(denied.denied());
        });
    }

    @Test
    public void testLatticeAcceptsEquivalentQueries() throws Exception {
        assertMemoryLeak(() -> {
            createAbFixture();
            EquivalenceHarness.assertEquivalentUnderAllGrants(
                    engine,
                    "select * from vA",
                    "select * from (select * from vA)",
                    true,
                    java.util.List.of(new Grant.View("vA"), new Grant.View("vB"), new Grant.Columns("t", "*"))
            );
        });
    }

    @Test
    public void testLatticeRejectsDifferentDecisions() throws Exception {
        // negative control: vA and the base-table rewrite return the same rows, but a user
        // holding only vA is allowed on one and denied on the other; the harness must notice
        assertMemoryLeak(() -> {
            createAbFixture();
            final EquivalenceHarness.EquivalenceMismatch mismatch = Assert.assertThrows(
                    EquivalenceHarness.EquivalenceMismatch.class,
                    () -> EquivalenceHarness.assertEquivalentUnderAllGrants(
                            engine,
                            "select * from vA",
                            "select * from t where sym = 'A'",
                            false,
                            java.util.List.of(new Grant.View("vA"), new Grant.Columns("t", "*"))
                    ));
            Assert.assertTrue(mismatch.getMessage(), mismatch.getMessage().contains(" decision"));
        });
    }

    @Test
    public void testLatticeRejectsDifferentChecks() throws Exception {
        // negative control for the checks comparison alone: same rows, same decision for every
        // grant subset (one atom covers both), but the filter reads px, so the column check differs
        assertMemoryLeak(() -> {
            createAbFixture();
            final EquivalenceHarness.EquivalenceMismatch mismatch = Assert.assertThrows(
                    EquivalenceHarness.EquivalenceMismatch.class,
                    () -> EquivalenceHarness.assertEquivalentUnderAllGrants(
                            engine,
                            "select ts from t",
                            "select ts from t where px > -1",
                            true,
                            java.util.List.of(new Grant.Columns("t", "*"))
                    ));
            Assert.assertTrue(mismatch.getMessage(), mismatch.getMessage().contains(" checks"));
        });
    }

    @Test
    public void testRevokeAfterCompileDenies() throws Exception {
        assertMemoryLeak(() -> {
            createAbFixture();
            EquivalenceHarness.assertRevokeBetweenCompileAndExecuteDenies(
                    engine,
                    "select * from vA union all select * from vB",
                    java.util.List.of(new Grant.View("vA"), new Grant.View("vB")),
                    ctx -> ctx.revokeView("vB"),
                    "vb"
            );
        });
    }

    private void createAbFixture() throws Exception {
        execute("create table t (ts timestamp, sym symbol, px double) timestamp(ts) partition by day bypass wal");
        execute("insert into t values ('2024-01-01T00:00:00.000000Z', 'A', 1.0)");
        execute("insert into t values ('2024-01-01T00:05:00.000000Z', 'B', 10.0)");
        execute("create view vA as (select * from t where sym = 'A')");
        execute("create view vB as (select * from t where sym = 'B')");
        drainWalAndViewQueues();
    }
}
