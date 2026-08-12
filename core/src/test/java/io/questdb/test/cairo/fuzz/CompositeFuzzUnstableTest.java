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

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

/**
 * The UNSTABLE half of the composite differential fuzz: seeds come from the clock, so each run
 * explores composite shapes and data no fixed-seed test covers.
 * <p>
 * This is deliberately non-deterministic, exactly like {@code WalWriterFuzzTest}, and it is the
 * counterpart to the deterministic {@link CompositeFuzzTest} and {@link CompositeFuzzSeedSweepTest} —
 * those two are what a pull request needs to pass (fixed seeds, bounded, same result every run); this
 * one is what finds the next defect.
 * <p>
 * <b>Reproducing a failure.</b> Every run logs its seeds. Re-run the exact failing case with
 * {@code -Dfuzz.s0=<A> -Dfuzz.s1=<B>} (honoured by {@link TestUtils#generateRandom(Log)}), or pin them
 * in code with {@code new Rnd(A, B)}. That is the same recipe as every other fuzz test in this repo.
 * <p>
 * <b>Why a random-seed test earns its place.</b> The sibling-cell interval defect — an ordinary
 * {@code WHERE ts = ...} silently dropping rows — was invisible to all 328 composite tests and to every
 * fixed seed in the suite. It surfaced on 1 seed in 40. A fuzz harness that only ever replays seeds
 * someone already wrote down cannot find what nobody has thought of.
 */
public class CompositeFuzzUnstableTest extends AbstractCairoTest {
    private static final Log LOG = LogFactory.getLog(CompositeFuzzUnstableTest.class);

    /**
     * Several independent runs per execution: one run is one set of axes (dimension set, layout,
     * clustering, cardinality, fast-append), so a single run leaves most of the shape space untouched.
     */
    @Test
    public void testRandomSeeds() throws Exception {
        for (int i = 0; i < 6; i++) {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final int run = i;
            assertMemoryLeak(() -> {
                CompositeFuzzRunner runner = CompositeFuzzRunner.of(engine, rnd);
                runner.createTables("unstable" + run);
                runner.applyGeneratedTransactions(600, 30);
                runner.assertTwinEqual();
            });
        }
    }

    /**
     * A longer, heavier run: more rows over more transactions, so more commits land in
     * already-populated cells and more cells share a day. Kept to a single run because it costs
     * proportionally more.
     */
    @Test
    public void testRandomSeedLongRun() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            CompositeFuzzRunner runner = CompositeFuzzRunner.of(engine, rnd);
            runner.createTables("unstableLong");
            runner.applyGeneratedTransactions(4000, 80);
            runner.assertTwinEqual();
            // A run this size has no excuse for being vacuous -- if it did not actually route cells,
            // revisit an already-populated cell, and exercise a gate, the seed produced a degenerate
            // shape and the green above means nothing.
            runner.applyGatedOperation("ALTER TABLE " + runner.compositeName() + " DROP COLUMN qty");
            runner.assertExercised();
        });
    }
}
