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

package io.questdb.test.griffin.engine.join;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.join.AsOfJoinFastRecordCursorFactory;
import io.questdb.griffin.engine.join.FilteredAsOfJoinFastRecordCursorFactory;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Verifies that a keyed ASOF JOIN backed by {@code AsOfJoinFastRecordCursorFactory}
 * (and its filtered variant) releases its resources when a native allocation fails
 * while the cursor is being opened.
 * <p>
 * The cursor reopens two {@code SingleRecordSink} heaps in {@code of()}; if the
 * second {@code reopen()} trips the RSS memory limit after the first has already
 * allocated, {@code getCursor()} throws and the half-opened cursor is orphaned
 * (the factory's {@code _close()} does not free the reusable cursor). That leaked
 * the first sink's 8-byte heap, tagged {@code NATIVE_RECORD_CHAIN}. The query
 * fuzzer's malloc fault injection surfaced this leak.
 * <p>
 * The sweep arms the RSS ceiling on cursor open alone - it compiles the query above the ceiling
 * and opens the cursor without draining it - so only the code under test can trip the fault, and
 * the swept range covers cursor open rather than compilation and row iteration as well. Each point
 * compiles its own factory: reusing one across points would let a later successful open clean up
 * the partial allocation the pre-fix code stranded, masking the leak.
 */
public class AsOfJoinFastOomTest extends AbstractCairoTest {

    // Ceiling range the sweep walks. Cursor open allocates ~4 KiB, so the sweep crosses the whole
    // OOM/success transition with room to spare; the armed-open assertion below fails loudly if a
    // later allocation-path change ever pushes the transition past this.
    private static final int CURSOR_OPEN_SLACK_MAX = 8 * 1024;

    @Test
    public void testFilteredKeyedAsOfJoinCleansUpWhenCursorRunsOutOfMemory() throws Exception {
        // The filter has to sit in the slave sub-query. As a top-level WHERE it becomes a post-join
        // Filter over a plain AsOf Join Fast, and this test would silently duplicate the one below.
        assertNoLeakOnCursorOom(
                "SELECT m.k1, m.v, s.v FROM master m ASOF JOIN (SELECT * FROM slave WHERE v > 0) s ON (k1, k2)",
                FilteredAsOfJoinFastRecordCursorFactory.class
        );
    }

    @Test
    public void testKeyedAsOfJoinCleansUpWhenCursorRunsOutOfMemory() throws Exception {
        assertNoLeakOnCursorOom(
                "SELECT m.k1, m.v, s.v FROM master m ASOF JOIN slave s ON (k1, k2)",
                AsOfJoinFastRecordCursorFactory.class
        );
    }

    // Pins the query to the factory whose getCursor() the sweep is meant to fault. The EXPLAIN type
    // name cannot do this: "AsOf Join Fast" is emitted by both the keyed and the no-key fast factory
    // and is a substring of "Filtered AsOf Join Fast", which is itself emitted by two more, so a name
    // guard passes for four different factories. Match the class instead.
    private static void assertFactoryClass(RecordCursorFactory factory, Class<?> expected) {
        for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
            if (f.getClass() == expected) {
                return;
            }
        }
        Assert.fail("query did not compile to " + expected.getSimpleName()
                + "; top of the factory chain was " + factory.getClass().getSimpleName());
    }

    private void assertNoLeakOnCursorOom(String query, Class<? extends RecordCursorFactory> expectedFactory) throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE master AS (" +
                            "  SELECT rnd_symbol('a','b','c') k1, rnd_symbol('x','y') k2, rnd_int() v," +
                            "  timestamp_sequence(0, 60 * 1_000_000L) ts" +
                            "  FROM long_sequence(200)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "CREATE TABLE slave AS (" +
                            "  SELECT rnd_symbol('a','b','c') k1, rnd_symbol('x','y') k2, rnd_int() v," +
                            "  timestamp_sequence(0, 30 * 1_000_000L) ts" +
                            "  FROM long_sequence(200)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY"
            );

            // Confirm the query really exercises the cursor under test.
            try (RecordCursorFactory factory = select(query)) {
                assertFactoryClass(factory, expectedFactory);
            }

            // Warm the reader and compiler pools so the swept allocation failure lands
            // inside cursor open (the sink reopen()s), not in first-touch table open.
            drain(query);

            boolean hasSeenOom = false;
            boolean hasOpenedUnderLimit = false;
            // Sweep the native-memory ceiling across the cursor-open allocation points.
            // Some ceiling lets the first sink reopen() succeed and trips the second; the
            // pre-fix code then leaked the first sink's 8-byte heap. The 8-byte step matches
            // the sink heaps' granularity so the sweep lands inside that transition window.
            for (int slack = 0; slack <= CURSOR_OPEN_SLACK_MAX; slack += 8) {
                // Compile outside the ceiling. Under it, a compiler allocation satisfies the
                // fault instead, and cursor open - the code under test - never runs.
                try (RecordCursorFactory factory = select(query)) {
                    // Arm immediately before the operation under test, and open the cursor
                    // without draining it: the leak happens while the cursor opens, and rows
                    // would only add allocation noise the sweep would then have to cover.
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
            // the leak hides in. Over-trimming CURSOR_OPEN_SLACK_MAX now fails here instead of
            // silently skipping the transition.
            Assert.assertTrue("sweep never tripped the RSS limit; widen the range", hasSeenOom);
            Assert.assertTrue("sweep never opened the cursor under an armed ceiling, so it stopped short of "
                    + "the transition the leak hides in; widen CURSOR_OPEN_SLACK_MAX", hasOpenedUnderLimit);

            // Recovery: with the ceiling removed the same query runs cleanly.
            Unsafe.setRssMemLimit(0);
            drain(query);
        });
    }

    private void drain(String query) throws Exception {
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
