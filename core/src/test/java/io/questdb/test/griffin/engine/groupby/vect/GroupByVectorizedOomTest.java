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

package io.questdb.test.griffin.engine.groupby.vect;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractOomSweepTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Verifies that the vectorized (rosti) keyed GROUP BY backed by
 * {@code GroupByRecordCursorFactory} releases its resources when a native
 * allocation fails while the cursor is being opened.
 * <p>
 * {@code RostiRecordCursor.of()} reopens the factory's {@code PageFrameAddressCache},
 * which reallocates four off-heap {@code DirectLongList}s. If a later reopen trips
 * the RSS memory limit after an earlier one has already allocated, {@code of()}
 * throws and {@code getCursor()} never returns the cursor, so the caller never
 * closes it; the factory's {@code _close()} does not free the cache either, leaking
 * the already-reopened buffer (512 bytes, {@code NATIVE_DEFAULT}). The query fuzzer's
 * malloc fault injection surfaced this leak.
 * <p>
 * The cursor-open sweep runs in {@link AbstractOomSweepTest#assertCursorOpenOomSweep}. The parquet
 * sweep below targets a later operation and keeps its own loop: it compiles and opens above the
 * ceiling, then arms it for the {@code hasNext()} drain, where {@code buildRosti} publishes work
 * before the fault lands. Both compile a fresh factory per point: a reused one would let a later
 * success clean up a stranded partial allocation, and would hand the parquet survivor live pools
 * instead of the freed ones it must dereference.
 */
public class GroupByVectorizedOomTest extends AbstractOomSweepTest {

    // Ceiling range the buildRosti drain sweep walks. The drain allocates ~32 KiB, so the sweep
    // crosses its whole OOM/success transition with room to spare; the armed-drain assertion below
    // fails loudly if an allocation-path change ever pushes the transition past this.
    private static final int ROSTI_BUILD_SLACK_MAX = 48 * 1024;
    // The faulting allocations here are 128 B and larger, so a 64-byte step lands inside every one of
    // their windows many times over.
    private static final int ROSTI_BUILD_SLACK_STEP = 64;

    @Test
    public void testVectorizedGroupByCleansUpWhenCursorRunsOutOfMemory() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (k INT, v LONG)");
            execute("INSERT INTO tab SELECT (x % 16)::int, x FROM long_sequence(2000)");
            final String query = "SELECT k, sum(v) FROM tab GROUP BY k";

            // Confirm the plan really exercises the vectorized rosti cursor.
            assertQuery(query).noLeakCheck().assertsPlanContaining("GroupBy vectorized: true");

            assertCursorOpenOomSweep(query);
        });
    }

    @Test
    public void testWorkStolenEntryDoesNotOutliveFreedPoolsOverParquet() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, k INT, v LONG) TIMESTAMP(ts) PARTITION BY DAY");
            // One partition per day gives several page frames; aggregate entries get
            // published to the shared vector aggregate queue and drained in buildRosti's
            // finally block (runWhatsLeft).
            execute("INSERT INTO tab SELECT (x * 6 * 3600 * 1_000_000L)::timestamp, (x % 16)::int, x FROM long_sequence(2000)");
            execute("ALTER TABLE tab CONVERT PARTITION TO PARQUET WHERE ts >= 0");
            final String query = "SELECT k, sum(v) FROM tab GROUP BY k";

            assertQuery(query).noLeakCheck().assertsPlanContaining("GroupBy vectorized: true");

            // Warm the reader/compiler pools so the swept failure lands in cursor work,
            // not first-touch table open.
            drain(query);

            boolean hasSeenOom = false;
            boolean hasDrainedUnderLimit = false;
            // An OOM tripping a parquet decode inside the finally drain used to abort it,
            // leaving a published entry in the shared queue that referenced the frame
            // memory pools buildRosti then freed. The recovery drain after each OOM
            // work-steals that survivor and dereferences the freed pool (NPE pre-fix).
            for (int slack = 0; slack <= ROSTI_BUILD_SLACK_MAX; slack += ROSTI_BUILD_SLACK_STEP) {
                boolean hasOomed = false;
                // Compile and open above the ceiling, so only buildRosti - which the first
                // hasNext() triggers - can trip it. Under the ceiling, a compiler or cursor-open
                // allocation satisfies the fault instead, and no work is ever published.
                try (RecordCursorFactory factory = select(query)) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + slack);
                        try {
                            //noinspection StatementWithEmptyBody
                            while (cursor.hasNext()) {
                                // Pull every row; no assertion reads them, so formatting is waste.
                            }
                            hasDrainedUnderLimit = true;
                        } catch (CairoException e) {
                            Assert.assertTrue("expected an out-of-memory error, got: " + e.getMessage(), e.isOutOfMemory());
                            hasSeenOom = true;
                            hasOomed = true;
                        } finally {
                            // Disarm before the cursor and factory close, so neither trips the ceiling.
                            Unsafe.setRssMemLimit(0);
                        }
                    }
                }
                if (hasOomed) {
                    // Only an aborted drain can strand a published entry, so only then is there a
                    // survivor to work-steal. Compiling a fresh factory is load-bearing: the
                    // survivor must outlive the pools it points at, and reusing the factory above
                    // would hand it live pools and mask the NPE.
                    drain(query);
                }
            }
            // At slack = 0 the ceiling equals current usage, so the drain's first tracked allocation
            // fails; an OOM alone therefore only shows the drain allocates at all. Pairing it with a
            // drain that survived its ceiling is what shows the sweep crossed the transition, and
            // with it the window where work is published before the fault lands.
            Assert.assertTrue("the buildRosti drain made no tracked native allocation, so the sweep "
                    + "never faulted the code under test", hasSeenOom);
            Assert.assertTrue("sweep never drained under an armed ceiling, so it stopped short of the "
                    + "publish-then-fault window; widen ROSTI_BUILD_SLACK_MAX", hasDrainedUnderLimit);
        });
    }
}
