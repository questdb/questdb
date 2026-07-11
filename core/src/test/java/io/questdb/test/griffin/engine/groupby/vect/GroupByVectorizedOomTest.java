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
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
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
 * Each sweep arms the RSS ceiling on the operation it targets and nothing else, so only the code
 * under test can trip the fault and the swept range covers that operation alone. The cursor-open
 * sweep compiles above the ceiling and opens the cursor without draining it. The parquet sweep
 * compiles and opens above the ceiling, then arms it for the {@code hasNext()} drain, where
 * {@code buildRosti} publishes work before the fault lands. Both compile a fresh factory per point:
 * a reused one would let a later success clean up a stranded partial allocation, and would hand the
 * parquet survivor live pools instead of the freed ones it must dereference.
 */
public class GroupByVectorizedOomTest extends AbstractCairoTest {

    // Ceiling ranges the sweeps walk. Cursor open allocates ~2 KiB and the buildRosti drain ~32 KiB,
    // so each sweep crosses its whole OOM/success transition with room to spare; the armed-success
    // assertions fail loudly if a later allocation-path change ever pushes a transition past them.
    private static final int CURSOR_OPEN_SLACK_MAX = 8 * 1024;
    private static final int ROSTI_BUILD_SLACK_MAX = 48 * 1024;

    @Test
    public void testVectorizedGroupByCleansUpWhenCursorRunsOutOfMemory() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (k INT, v LONG)");
            execute("INSERT INTO tab SELECT (x % 16)::int, x FROM long_sequence(2000)");
            final String query = "SELECT k, sum(v) FROM tab GROUP BY k";

            // Confirm the plan really exercises the vectorized rosti cursor.
            printSql("EXPLAIN " + query);
            TestUtils.assertContains(sink, "GroupBy vectorized: true");

            // Warm the reader and compiler pools so the swept allocation failure lands
            // inside cursor open (the PageFrameAddressCache reopen), not in first-touch
            // table open.
            drain(query);

            boolean hasSeenOom = false;
            boolean hasOpenedUnderLimit = false;
            // Sweep the native-memory ceiling across the cursor-open allocation points.
            // Some ceiling lets an earlier PageFrameAddressCache list reopen() succeed
            // and trips a later one; the pre-fix code then leaked the earlier buffer.
            for (int slack = 0; slack <= CURSOR_OPEN_SLACK_MAX; slack += 8) {
                // Compile outside the ceiling. Under it, a compiler allocation satisfies the
                // fault instead, and cursor open - the code under test - never runs.
                try (RecordCursorFactory factory = select(query)) {
                    // Arm immediately before the operation under test, and open the cursor without
                    // draining it: the leak happens while the cursor opens, and buildRosti (which
                    // the first hasNext() triggers) would only add allocation noise on top.
                    Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + slack);
                    try (RecordCursor ignore = factory.getCursor(sqlExecutionContext)) {
                        hasOpenedUnderLimit = true;
                    } catch (CairoException e) {
                        Assert.assertTrue("expected an out-of-memory error, got: " + e.getMessage(), e.isOutOfMemory());
                        hasSeenOom = true;
                    } finally {
                        // Disarm before the factory closes, so close() cannot trip the ceiling.
                        Unsafe.setRssMemLimit(0);
                    }
                }
            }
            // slack=0 rejects the next allocation outright, so an OOM alone proves nothing. Pair it
            // with an open that survived its ceiling: together they bracket the whole cursor-open
            // allocation span, so the sweep provably crossed the failing-to-succeeding transition
            // the leak hides in.
            Assert.assertTrue("sweep never tripped the RSS limit; widen the range", hasSeenOom);
            Assert.assertTrue("sweep never opened the cursor under an armed ceiling, so it stopped short of "
                    + "the transition the leak hides in; widen CURSOR_OPEN_SLACK_MAX", hasOpenedUnderLimit);

            // Recovery: with the ceiling removed the same query runs cleanly.
            Unsafe.setRssMemLimit(0);
            drain(query);
        });
    }

    @Test
    public void testWorkStolenEntryDoesNotOutliveFreedPoolsOverParquet() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, k INT, v LONG) TIMESTAMP(ts) PARTITION BY DAY");
            // One partition per day gives several page frames; aggregate entries get
            // published to the shared vector aggregate queue and drained in buildRosti's
            // finally block (runWhatsLeft).
            execute("INSERT INTO tab SELECT (x * 6 * 3600 * 1000_000L)::timestamp, (x % 16)::int, x FROM long_sequence(2000)");
            execute("ALTER TABLE tab CONVERT PARTITION TO PARQUET WHERE ts >= 0");
            final String query = "SELECT k, sum(v) FROM tab GROUP BY k";

            printSql("EXPLAIN " + query);
            TestUtils.assertContains(sink, "GroupBy vectorized: true");

            // Warm the reader/compiler pools so the swept failure lands in cursor work,
            // not first-touch table open.
            drain(query);

            boolean hasSeenOom = false;
            boolean hasDrainedUnderLimit = false;
            // An OOM tripping a parquet decode inside the finally drain used to abort it,
            // leaving a published entry in the shared queue that referenced the frame
            // memory pools buildRosti then freed. The recovery drain after each OOM
            // work-steals that survivor and dereferences the freed pool (NPE pre-fix).
            for (int slack = 0; slack <= ROSTI_BUILD_SLACK_MAX; slack += 64) {
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
            // slack=0 rejects the next allocation outright, so an OOM alone proves nothing. Pair it
            // with a drain that survived its ceiling: together they bracket the whole buildRosti
            // allocation span, so the sweep provably crossed the failing-to-succeeding transition,
            // and with it the window where work is published before the fault lands.
            Assert.assertTrue("sweep never tripped the RSS limit; widen the range", hasSeenOom);
            Assert.assertTrue("sweep never drained under an armed ceiling, so it stopped short of the "
                    + "publish-then-fault window; widen ROSTI_BUILD_SLACK_MAX", hasDrainedUnderLimit);
        });
    }

    private static void drain(String query) throws Exception {
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
