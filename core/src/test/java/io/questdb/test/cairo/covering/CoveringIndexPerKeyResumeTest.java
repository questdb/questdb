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

package io.questdb.test.cairo.covering;

import io.questdb.PropertyKey;
import org.junit.Before;
import org.junit.Test;

/**
 * Exercises the multi-chunk (key, partition) resume branch of the per-key
 * (unordered) covering scan.
 * <p>
 * {@code cairo.sql.page.frame.max.rows} defaults to 1,000,000, and every
 * (key, partition) measured elsewhere in this feature fit inside a single
 * frame -- so the resume branch, where a key's drain spans several frames, has
 * never run. That branch is where the original per-key loop bug lived:
 * {@code fillFrameForKeyCheap} clears its resume state when the frame it
 * returns was the key's LAST chunk, so a caller that advances to the next key
 * only when a frame comes back {@code null} re-opens any key that fits in a
 * SINGLE frame and emits it forever. The contract is documented on
 * {@code CoveringIndexRecordCursorFactory.MultiKeyCoveringPageFrameCursor#isKeyMidDrain()}:
 * a non-null return does not mean the key has more rows -- only
 * {@code isKeyMidDrain()} answers that.
 * <p>
 * This suite shrinks the frame cap to 100 rows so each of the four keys spans
 * roughly 100 chunks per partition, forcing {@code resumeKeyDrain()} to run
 * many times per key instead of zero.
 */
public class CoveringIndexPerKeyResumeTest extends AbstractCoveringIndexQueryTest {

    @Override
    @Before
    public void setUp() {
        // Must be set BEFORE super.setUp(), which builds the configuration from the
        // property overrides. The fixture's setUp enables parallel group by and then
        // calls AbstractCairoTest.setUp, which reads this property into the engine
        // configuration.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 100);
        super.setUp();
    }

    @Test
    public void testPerKeySpansManyFramesPerPartition() throws Exception {
        assertMemoryLeak(() -> {
            // 10 000 rows per key in one partition against a 100-row frame cap: ~100
            // chunks per key, so every key's drain spans many frames and resumeKeyDrain()
            // runs repeatedly instead of never.
            createTelemetryLarge();
            final String where = " WHERE param_id IN ('SFID','HOTMIC','KCAS','CALT') ORDER BY param_id";
            // Vacuity guard: confirm the query actually routes through the per-key
            // (unordered) scan before trusting the comparisons below -- otherwise a
            // regression that silently fell back to the merge would pass this test
            // having exercised nothing it claims to.
            assertQuery("SELECT param_id, max(value), count() FROM telemetry" + where)
                    .noLeakCheck()
                    .assertsPlanContaining("frames: per-key (unordered)");
            // count() is the arm that catches the original bug: if the per-key loop
            // re-opens a drained key instead of advancing past it, the key emits its
            // rows forever, count() diverges (grows unbounded) instead of hanging outright
            // for a bounded row cap, and this assertSameResult catches the inflated total
            // immediately rather than timing out.
            assertSameResult(
                    "SELECT param_id, max(value), count() FROM telemetry" + where,
                    "SELECT /*+ no_index */ param_id, max(value), count() FROM telemetry" + where
            );
            assertSameResult(
                    "SELECT param_id, first(value), last(value) FROM telemetry" + where,
                    "SELECT /*+ no_index */ param_id, first(value), last(value) FROM telemetry" + where
            );
        });
    }

    /**
     * Local to this class: the shared fixture's 10 000 rows over the DEFAULT
     * 1,000,000-row frame cap never span a frame boundary, which defeats the whole
     * point of this suite. Deliberately NOT added to the shared
     * {@link AbstractCoveringIndexQueryTest}: committed tests over the shared
     * generators pin exact plan text and exact result rows derived from their
     * specific spacing and row counts.
     */
    private void createTelemetryLarge() throws Exception {
        execute("CREATE TABLE telemetry (" +
                "  ts TIMESTAMP," +
                "  param_id SYMBOL INDEX TYPE POSTING INCLUDE (ts, value)," +
                "  value DOUBLE" +
                ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        // 40k rows over 4 keys, one partition, 100-row frames -> ~100 chunks per key.
        execute("INSERT INTO telemetry SELECT (x * 1000L)::timestamp," +
                " CASE WHEN x % 4 = 0 THEN 'SFID' WHEN x % 4 = 1 THEN 'HOTMIC'" +
                "      WHEN x % 4 = 2 THEN 'KCAS' ELSE 'CALT' END," +
                " x::double" +
                " FROM long_sequence(40000)");
    }
}
