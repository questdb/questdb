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

package io.questdb.test.griffin.engine.groupby;

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.test.AbstractOomSweepTest;
import org.junit.Test;

/**
 * Verifies that {@code SAMPLE BY ... FILL(LINEAR)} releases its resources when a
 * native allocation fails while the cursor is being constructed.
 * <p>
 * {@code SampleByInterpolateRecordCursorFactory} builds its record-key map, data
 * map and group-by allocator in the cursor constructor. If one of those
 * allocations trips the RSS memory limit, the constructor's error path calls
 * {@code close()} before the later fields are assigned, so {@code close()} must
 * be null-safe; otherwise it throws {@code NullPointerException} and masks the
 * real out-of-memory error. The query fuzzer's malloc fault injection surfaced
 * this, with the NPE replacing the {@code CairoException} the caller expects.
 */
public class SampleByInterpolateOomTest extends AbstractOomSweepTest {

    @Test
    public void testFillLinearCleansUpWhenConstructionRunsOutOfMemory() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE x AS (" +
                            "  SELECT rnd_symbol('a','b','c') s, rnd_double() v," +
                            "  timestamp_sequence(0, 60 * 1_000_000L) ts" +
                            "  FROM long_sequence(200)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY"
            );
            final String query = "SELECT s, count() c, ts FROM x SAMPLE BY 15m FILL(LINEAR) ALIGN TO CALENDAR ORDER BY s, ts";

            // Warm the reader and compiler pools so the swept allocation failure
            // lands inside cursor construction, not in first-touch table open. The
            // pools are intentionally left populated (no releaseInactive) so each
            // sweep reuses the warm reader and the first sizeable native allocation
            // in select() is the cursor's record-key map.
            drain(query);

            // Sweep the native-memory ceiling across the cursor-construction allocation
            // points. Some ceiling trips one of the cursor's map allocations; if the
            // constructor's error-path close() is not null-safe it throws NPE and masks
            // the out-of-memory error. Unlike the cursor-open sweeps, the fault has to
            // land in select(), so the arming brackets compilation rather than getCursor().
            assertOomSweep(32 * 1024, 1024, null, () -> {
                // Construction alone allocates the maps; close immediately.
                //noinspection EmptyTryBlock
                try (RecordCursorFactory ignore = select(query)) {
                    // factory built without tripping the limit; nothing to do
                }
            });

            // Recovery: with the ceiling removed the same query runs cleanly.
            drain(query);
        });
    }
}
