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

package io.questdb.test.cairo.fuzz;

import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

/**
 * Runs the composite differential fuzz across a SPREAD of seeds rather than one.
 * <p>
 * A single fixed seed resolves one set of axes (dimension set, layout, clustering, cardinality,
 * fast-append) and one data shape, and passes or fails as a unit. The defect this class was written
 * after — an interval scan retiring its interval at the first cell of a multi-cell day that did not
 * match, dropping every later sibling cell's rows — was invisible to every fixed-seed test in the suite
 * and surfaced on 1 seed in 40. One seed is not a fuzz run; it is a single sample.
 * <p>
 * Seeds are FIXED (derived from the loop index), not random, so a failure here is reproducible from the
 * reported seed alone and CI never reports a different result run to run. The spread, not the
 * randomness, is what does the work.
 */
public class CompositeFuzzSeedSweepTest extends AbstractCairoTest {

    /**
     * The seed whose divergence exposed the sibling-cell interval defect. Pinned by name so a
     * regression cannot be hidden by a future change to the sweep's index arithmetic.
     * <p>
     * The divergence it produced was recorded at the time as a suspected HARNESS fault (the shared-{@code
     * Rnd} replay handing the two twins different data). It was not: the replay is sound (see
     * {@link CompositeFuzzHarnessSoundnessTest}) and the divergence was a genuine product wrong-answer.
     */
    @Test
    public void testSeedThatFoundTheSiblingCellIntervalDefect() throws Exception {
        assertMemoryLeak(() -> {
            CompositeFuzzRunner runner = CompositeFuzzRunner.of(engine, new Rnd(1481L, 1683L));
            runner.createTables("pinned");
            runner.applyGeneratedTransactions(600, 30);
            runner.assertTwinEqual();
        });
    }

    /**
     * The seed that found the four-cause {@code DROP PARTITION} defect, pinned as a test rather than
     * left in {@link CompositeFuzzRunner}'s javadoc as a recipe.
     * <p>
     * It was the only deterministic reproduction that generation of the bug produced, and until now it
     * lived in prose — in the same class whose own comment warns that "prose recipes rot". They did:
     * two neighbouring javadocs still described the bug as open three sessions after it was fixed.
     * <p>
     * <b>{@link TestUtils#generateRandom}, never {@code new Rnd}.</b> generateRandom primes the stream
     * with two {@code nextBoolean()} calls, so a bare {@code new Rnd} with the same two longs replays a
     * DIFFERENT stream and this test would be silently vacuous — measured at the time: the bare form
     * PASSED against the unfixed product while the generateRandom form reproduced the divergence
     * byte-for-byte.
     * <p>
     * The drop probability is passed explicitly even though 0.05 is already the default, so that
     * changing the default cannot quietly retire this test's coverage.
     * <p>
     * Discriminating, verified by mutation on 2026-08-31: reverting the drop path's active-tail-reopen
     * skip (one of the four causes, commit 3f3a7bb0ba) makes this test FAIL, and restoring it makes it
     * pass. It is a lock, not a smoke test.
     */
    @Test
    public void testSeedThatFoundTheDropPartitionDefect() throws Exception {
        assertMemoryLeak(() -> {
            CompositeFuzzRunner runner = CompositeFuzzRunner
                    .of(engine, TestUtils.generateRandom(null, 345549849791363L, 1787735726165L))
                    .withDropPartitionProbability(0.05);
            runner.createTables("pinnedDrop");
            runner.applyGeneratedTransactions(600, 30);
            runner.assertTwinEqual();
        });
    }

    /**
     * The spread. Each seed gets its own engine state via a fresh table pair, and every failure is
     * collected so one bad seed reports as one named failure rather than aborting the sweep and hiding
     * the rest.
     */
    @Test
    public void testSeedSweep() throws Exception {
        final StringBuilder failures = new StringBuilder();
        int failed = 0;
        for (int i = 0; i < 24; i++) {
            final long seed0 = 1000L + i * 37L;
            final long seed1 = 500L + i * 91L;
            try {
                assertMemoryLeak(() -> {
                    CompositeFuzzRunner runner = CompositeFuzzRunner.of(engine, new Rnd(seed0, seed1));
                    runner.createTables("sweep" + seed0 + "_" + seed1);
                    runner.applyGeneratedTransactions(600, 30);
                    runner.assertTwinEqual();
                });
            } catch (Throwable t) {
                failed++;
                final String message = String.valueOf(t.getMessage()).replace('\n', ' ');
                failures.append("\n  seed=(").append(seed0).append(',').append(seed1).append(") ")
                        .append(message, 0, Math.min(300, message.length()));
            }
        }
        if (failed > 0) {
            throw new AssertionError("composite fuzz diverged from its plain twin on "
                    + failed + " of 24 seeds. Reproduce with CompositeFuzzRunner.of(engine, new Rnd(seed0, seed1))"
                    + " + createTables + applyGeneratedTransactions(600, 30):" + failures);
        }
    }
}
