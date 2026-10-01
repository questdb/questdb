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

package io.questdb.test;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;

/**
 * Base for the tests that fault a native allocation in the middle of a cursor operation and assert,
 * through the enclosing {@code assertMemoryLeak}, that the half-done operation leaves nothing behind.
 * <p>
 * Such a test walks an RSS memory ceiling across the operation's allocation points. A ceiling that
 * lets an earlier allocation succeed and trips a later one is what strands the earlier one, so the
 * sweep has to cross the whole failing-to-succeeding transition rather than only fail at the bottom
 * of the range. {@link #assertCursorOpenOomSweep} asserts both ends of that transition: an OOM (which
 * {@code slack = 0} guarantees) and an open that survived its ceiling. An over-trimmed range then
 * fails loudly instead of passing vacuously.
 */
public abstract class AbstractOomSweepTest extends AbstractCairoTest {
    // Ceiling range the cursor-open sweep walks. The cursor opens the operations under test allocate
    // a few KiB, so the sweep crosses their whole OOM/success transition with room to spare; the
    // armed-open assertion below fails loudly if an allocation-path change ever pushes the transition
    // past this.
    protected static final int CURSOR_OPEN_SLACK_MAX = 8 * 1024;
    // Step the sweep advances the ceiling by. It is load-bearing: it has to match the granularity of
    // the heaps cursor open allocates, or the sweep steps straight over the narrow window between
    // "the first heap allocated" and "the next one failed" - the only window in which the pre-fix
    // code leaks.
    protected static final int CURSOR_OPEN_SLACK_STEP = 8;

    /**
     * Sweeps the RSS ceiling across {@code query}'s cursor open and row iteration, but not its
     * compilation - for an operation whose allocation points are on the iteration path, which
     * {@link #assertCursorOpenOomSweep} never reaches because it opens the cursor without draining it.
     * <p>
     * Compilation runs at each point with the ceiling down, for the reason spelled out there: under
     * the ceiling a compiler allocation satisfies the fault instead, and the code under test never
     * runs, with neither {@code hasSeenOom} nor {@code hasRunUnderLimit} able to tell the difference.
     * Each point compiles its own factory, so a later successful drain cannot free what an earlier
     * faulted one stranded.
     * <p>
     * {@code beforeArm}, when given, also runs with the ceiling down; a sweep whose setup allocates
     * (dropping a pooled reader, say) needs it, or the setup competes with the code under test for
     * the fault.
     */
    protected static void assertCursorDrainOomSweep(int slackMax, int slackStep, @Nullable OomSweepStep beforeArm, String query) throws Exception {
        boolean hasRunUnderLimit = false;
        int maxOomSlack = -1;
        for (int slack = 0; slack <= slackMax; slack += slackStep) {
            if (beforeArm != null) {
                beforeArm.run();
            }
            try (RecordCursorFactory factory = select(query)) {
                RecordCursor cursor = null;
                // Arm immediately before the operation under test.
                Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + slack);
                try {
                    cursor = factory.getCursor(sqlExecutionContext);
                    //noinspection StatementWithEmptyBody
                    while (cursor.hasNext()) {
                        // Pull every row; the fault is on this path.
                    }
                    hasRunUnderLimit = true;
                } catch (CairoException e) {
                    Assert.assertTrue("expected an out-of-memory error, got: " + e.getMessage(), e.isOutOfMemory());
                    maxOomSlack = slack;
                } finally {
                    // Disarm before the cursor and the factory close, so neither trips the ceiling.
                    // The cursor cannot be a try-with-resources here, for the reason
                    // assertCursorOpenOomSweep gives.
                    Unsafe.setRssMemLimit(0);
                    Misc.free(cursor);
                }
            }
        }
        Assert.assertTrue("the swept operation only failed at the zero-slack endpoint", maxOomSlack > 0);
        Assert.assertTrue("the sweep never completed the operation under an armed ceiling, so it stopped "
                + "short of the transition the leak hides in; widen slackMax", hasRunUnderLimit);
        Unsafe.setRssMemLimit(0);
    }

    /**
     * Sweeps the RSS ceiling across the allocation points of {@code query}'s cursor open, and nothing
     * else, so only the code under test can trip the fault. Compiles above the ceiling and opens the
     * cursor without draining it: the leak happens while the cursor opens, and compilation and row
     * iteration would only add allocation noise the sweep would then have to cover.
     */
    protected static void assertCursorOpenOomSweep(String query) throws Exception {
        // Warm the reader and compiler pools, so the swept allocation failure lands inside cursor
        // open rather than in first-touch table open.
        drain(query);

        boolean hasOpenedUnderLimit = false;
        int maxOomSlack = -1;
        for (int slack = 0; slack <= CURSOR_OPEN_SLACK_MAX; slack += CURSOR_OPEN_SLACK_STEP) {
            // Compile outside the ceiling. Under it, a compiler allocation satisfies the fault
            // instead, and cursor open - the code under test - never runs. Each point compiles its
            // own factory: reusing one across points would let a later successful open clean up the
            // partial allocation the pre-fix code stranded, masking the leak.
            try (RecordCursorFactory factory = select(query)) {
                RecordCursor cursor = null;
                // Arm immediately before the operation under test.
                Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + slack);
                try {
                    cursor = factory.getCursor(sqlExecutionContext);
                    hasOpenedUnderLimit = true;
                } catch (CairoException e) {
                    Assert.assertTrue("expected an out-of-memory error, got: " + e.getMessage(), e.isOutOfMemory());
                    maxOomSlack = slack;
                } finally {
                    // Disarm before the cursor and the factory close, so neither trips the ceiling. The
                    // cursor cannot be a try-with-resources here: an extended try-with-resources closes
                    // its resource before the catch and finally of the same statement run, which would
                    // hold the ceiling armed across close() and let a close-time OOM pass for an
                    // open-time one.
                    Unsafe.setRssMemLimit(0);
                    Misc.free(cursor);
                }
            }
        }
        // The two assertions bracket the operation's allocation span. At slack = 0 the ceiling equals
        // current usage, so the first tracked allocation of the open fails; an OOM alone therefore
        // only shows the open allocates at all. Pairing it with an open that survived its ceiling is
        // what shows the sweep crossed the transition the leak hides in.
        Assert.assertTrue("cursor open only failed at the zero-slack endpoint", maxOomSlack > 0);
        Assert.assertTrue("sweep never opened the cursor under an armed ceiling, so it stopped short of "
                + "the transition the leak hides in; widen CURSOR_OPEN_SLACK_MAX", hasOpenedUnderLimit);

        // Recovery: with the ceiling removed the same query runs cleanly.
        Unsafe.setRssMemLimit(0);
        drain(query);
    }

    /**
     * Sweeps the RSS ceiling across the allocation points of {@code armed}, which runs with the
     * ceiling already set. {@code beforeArm}, when given, runs at each point with the ceiling down;
     * a sweep whose setup allocates (dropping a pooled reader, say) needs it, or the setup competes
     * with the code under test for the fault.
     * <p>
     * Asserts both ends of the failing-to-succeeding transition, for the reason
     * {@link #assertCursorOpenOomSweep} does: an OOM alone only shows the operation allocates, and
     * a range that never reaches the point where the operation survives its ceiling passes
     * vacuously.
     */
    protected static void assertOomSweep(int slackMax, int slackStep, @Nullable OomSweepStep beforeArm, OomSweepStep armed) throws Exception {
        boolean hasRunUnderLimit = false;
        int maxOomSlack = -1;
        for (int slack = 0; slack <= slackMax; slack += slackStep) {
            if (beforeArm != null) {
                beforeArm.run();
            }
            Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + slack);
            try {
                armed.run();
                hasRunUnderLimit = true;
            } catch (CairoException e) {
                Assert.assertTrue("expected an out-of-memory error, got: " + e.getMessage(), e.isOutOfMemory());
                maxOomSlack = slack;
            } finally {
                Unsafe.setRssMemLimit(0);
            }
        }
        Assert.assertTrue("the swept operation only failed at the zero-slack endpoint", maxOomSlack > 0);
        Assert.assertTrue("the sweep never completed the operation under an armed ceiling, so it stopped "
                + "short of the transition the leak hides in; widen slackMax", hasRunUnderLimit);
        Unsafe.setRssMemLimit(0);
    }

    protected static void drain(String query) throws Exception {
        try (RecordCursorFactory factory = select(query)) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                //noinspection StatementWithEmptyBody
                while (cursor.hasNext()) {
                    // Pull every row; no assertion reads them, so formatting them would be waste.
                }
            }
        }
    }

    @FunctionalInterface
    protected interface OomSweepStep {
        void run() throws Exception;
    }
}
