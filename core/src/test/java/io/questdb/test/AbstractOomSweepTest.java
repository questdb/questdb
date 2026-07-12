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
     * Sweeps the RSS ceiling across the allocation points of {@code query}'s cursor open, and nothing
     * else, so only the code under test can trip the fault. Compiles above the ceiling and opens the
     * cursor without draining it: the leak happens while the cursor opens, and compilation and row
     * iteration would only add allocation noise the sweep would then have to cover.
     */
    protected static void assertCursorOpenOomSweep(String query) throws Exception {
        // Warm the reader and compiler pools, so the swept allocation failure lands inside cursor
        // open rather than in first-touch table open.
        drain(query);

        boolean hasSeenOom = false;
        boolean hasOpenedUnderLimit = false;
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
                    hasSeenOom = true;
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
        Assert.assertTrue("cursor open made no tracked native allocation, so the sweep never faulted "
                + "the code under test", hasSeenOom);
        Assert.assertTrue("sweep never opened the cursor under an armed ceiling, so it stopped short of "
                + "the transition the leak hides in; widen CURSOR_OPEN_SLACK_MAX", hasOpenedUnderLimit);

        // Recovery: with the ceiling removed the same query runs cleanly.
        Unsafe.setRssMemLimit(0);
        drain(query);
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
}
