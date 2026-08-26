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

package io.questdb.test.cairo.fuzz;

import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class CompositeFuzzTest extends AbstractCairoTest {

    @Test
    public void testFixedSeedTwinEquality() throws Exception {
        assertMemoryLeak(() -> {
            CompositeFuzzRunner runner = CompositeFuzzRunner.of(engine, new Rnd(1234L, 5678L));
            runner.createTables("fuzz1");
            runner.applyGeneratedTransactions(200, 20);
            runner.assertTwinEqual();
        });
    }

    @Test
    public void testAxesVaryAcrossSeeds() throws Exception {
        assertMemoryLeak(() -> {
            java.util.Set<String> seen = new java.util.HashSet<>();
            for (int i = 0; i < 12; i++) {
                CompositeFuzzRunner r = CompositeFuzzRunner.of(engine, new Rnd(i, i * 7L));
                r.createTables("axes" + i);
                seen.add(r.axes().toString());
            }
            org.junit.Assert.assertTrue("axes must vary across seeds, saw " + seen, seen.size() > 3);
        });
    }

    @Test
    public void testAllShapesCompared() throws Exception {
        assertMemoryLeak(() -> {
            CompositeFuzzRunner runner = CompositeFuzzRunner.of(engine, new Rnd(99L, 42L));
            runner.createTables("shapes");
            runner.applyGeneratedTransactions(500, 30);
            runner.assertTwinEqual();
            org.junit.Assert.assertEquals("all eleven shapes must be compared",
                    11, runner.comparedShapeCount());
        });
    }

    @Test
    public void testRunMustProveItExercisedComposite() throws Exception {
        assertMemoryLeak(() -> {
            CompositeFuzzRunner runner = CompositeFuzzRunner.of(engine, new Rnd(7L, 7L));
            runner.createTables("exercised");
            runner.applyGeneratedTransactions(800, 40);
            runner.assertTwinEqual();
            // Task 5 added a fifth floor ("gated operations attempted >= 1") to assertExercised() --
            // a run must also prove a composite gate actually rejects something on this run's shape,
            // not just that it routed cells. applyGeneratedTransactions() never generates a gated op
            // (every structural-DDL probability is 0.0 through Task 4), so this run must exercise one
            // explicitly before the floor can pass.
            // DROP COLUMN became SUPPORTED on a composite table in SP2 (2026-08-25), so it is no
            // longer a gate to prove. UPDATE replaces it deliberately: it is refused
            // PERMANENTLY by design, not pending work, so this lock will not need swapping
            // again the next time a capability lands.
            runner.applyGatedOperation("UPDATE " + runner.compositeName() + " SET qty = qty + 1");
            runner.assertExercised();   // must throw if the run was vacuous
        });
    }

    @Test
    public void testFloorsFailAVacuousRun() throws Exception {
        assertMemoryLeak(() -> {
            CompositeFuzzRunner runner = CompositeFuzzRunner.of(engine, new Rnd(7L, 7L));
            runner.createTables("vacuous");
            // no transactions applied at all -> nothing routed
            try {
                runner.assertExercised();
                org.junit.Assert.fail("expected the anti-vacuity floors to reject an unexercised run");
            } catch (AssertionError expected) {
                io.questdb.test.tools.TestUtils.assertContains(expected.getMessage(), "distinct cellKeys");
            }
        });
    }

    @Test
    public void testGatedOperationThrowsAndLeavesNoDamage() throws Exception {
        assertMemoryLeak(() -> {
            CompositeFuzzRunner runner = CompositeFuzzRunner.of(engine, new Rnd(11L, 13L));
            runner.createTables("gated");
            runner.applyGeneratedTransactions(400, 20);
            runner.assertTwinEqual();

            long before = runner.compositeRowCount();
            // DROP COLUMN became SUPPORTED on a composite table in SP2 (2026-08-25), so it is no
            // longer a gate to prove. UPDATE replaces it deliberately: it is refused
            // PERMANENTLY by design, not pending work, so this lock will not need swapping
            // again the next time a capability lands.
            runner.applyGatedOperation("UPDATE " + runner.compositeName() + " SET qty = qty + 1");
            org.junit.Assert.assertEquals("a rejected op must not change row count",
                    before, runner.compositeRowCount());
            runner.assertTwinEqual();   // and must leave the table twin-equal
        });
    }

    /**
     * ANTI-VACUITY LOCK for the ADD COLUMN enrolment (2026-08-26).
     * <p>
     * {@code CompositeFuzzRunner#dropUnsupportedAddColumnOps} filters out generated adds that would
     * hit a gate the subject already declares unsupported (var-size column, POSTING index). A filter
     * like that has two silent failure modes, and a passing twin-equality assertion catches NEITHER:
     * <ul>
     *   <li><b>Dead filter</b> -- never fires, so the green says nothing about the gates it exists
     *       for.</li>
     *   <li><b>Total filter</b> -- removes EVERY add, so ADD COLUMN is nominally enrolled at
     *       probability 0.05 while contributing exactly no coverage, and the suite stays green
     *       forever.</li>
     * </ul>
     * The total-filter mode is the dangerous one: it is indistinguishable from success unless
     * something asserts that columns really did arrive. So both sides are asserted -- the filter fired
     * at least once, AND at least one add survived it and reached the table.
     * <p>
     * Deliberately NOT pinned to one seed. It was, and that pin broke the moment {@code o3} was
     * flipped on: the flag changes the {@code Rnd} draw sequence, so the seed measured to generate a
     * var-size and a POSTING add stopped generating them and this test failed for a reason that had
     * nothing to do with what it exists to check. Sweeping a few seeds and requiring the property to
     * hold ACROSS them keeps the assertion meaningful without re-pinning something that will drift
     * again on the next probability change.
     * <p>
     * Filtering happens after generation and consumes no {@code Rnd}, so a given seed's generated op
     * stream is unchanged by the filter's existence -- this is reproducible for a given config, not
     * probabilistic.
     */
    @Test
    public void testAddColumnEnrolmentIsNeitherFilteredAwayNorUnfiltered() throws Exception {
        assertMemoryLeak(() -> {
            int totalDropped = 0;
            int seedsThatGainedColumns = 0;
            for (int i = 0; i < 8; i++) {
                CompositeFuzzRunner runner = CompositeFuzzRunner.of(engine, new Rnd(1333L + i * 53L, 1319L + i * 71L));
                runner.createTables("addcol" + i);
                final long columnsBefore = runner.compositeColumnCount();
                runner.applyGeneratedTransactions(600, 30);
                totalDropped += runner.droppedAddColumnOps();
                if (runner.compositeColumnCount() > columnsBefore) {
                    seedsThatGainedColumns++;
                }
                runner.assertTwinEqual();
            }

            org.junit.Assert.assertTrue(
                    "the unsupported-add filter never fired across 8 seeds -- either the generator no "
                            + "longer emits var-size/POSTING adds, or the filter is dead. Re-measure "
                            + "before weakening this assertion.",
                    totalDropped > 0);

            org.junit.Assert.assertTrue(
                    "no seed gained a column (" + totalDropped + " adds dropped across 8 seeds), so "
                            + "the filter is swallowing every generated ADD COLUMN and enrolling it "
                            + "bought no coverage at all",
                    seedsThatGainedColumns > 0);
        });
    }
}
