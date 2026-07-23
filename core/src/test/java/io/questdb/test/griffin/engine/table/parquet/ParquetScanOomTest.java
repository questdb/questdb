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

package io.questdb.test.griffin.engine.table.parquet;

import io.questdb.std.MemoryTag;
import io.questdb.test.AbstractOomSweepTest;
import org.junit.Test;

/**
 * Verifies that scanning a parquet partition releases its page-frame memory pool
 * buffers when a native allocation fails while a per-frame buffer is acquired.
 * <p>
 * {@code PageFrameMemoryPool} pulls a buffer off its free list and then calls
 * {@code reopen()} on it, which allocates the per-frame page-address lists one by
 * one. If that allocation trips the RSS memory limit, the buffer has already been
 * removed from the free list but not yet recorded in the cache, so the pool can
 * no longer reach it on close and the lists it managed to allocate leak. The
 * query fuzzer's malloc fault injection surfaced this as a small
 * {@code NATIVE_DEFAULT} leak.
 */
public class ParquetScanOomTest extends AbstractOomSweepTest {

    @Test
    public void testParquetScanCleansUpWhenBufferReopenRunsOutOfMemory() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (" +
                    "  SELECT x, rnd_double() d," +
                    "  timestamp_sequence(0, 30 * 60 * 1000000L) ts" +
                    "  FROM long_sequence(100)" +
                    ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '1970-01-01'");

            // Aggregates a constant, so the scan navigates the parquet frame but
            // decodes no columns; the page-frame pool's per-frame buffer reopen is
            // then the dominant native allocation, which is the path the fix guards.
            final String query = "SELECT min((length('OS'::VARCHAR))::TIMESTAMP) AS a0 FROM x";

            // Warm the reader and compiler pools so the swept allocation failure lands
            // inside the scan, not in first-touch table open.
            drain(query);

            // Fine sweep so the ceiling lands inside the buffer reopen, where the
            // page-address lists allocate one by one. The buffer reopen is on the
            // iteration path, so the sweep has to drain - but it must not compile
            // under the ceiling, or a compiler allocation would take the fault and
            // the scan would never run.
            assertCursorDrainOomSweep(24 * 1024, 64, null, query);

            // Recovery: with the ceiling removed the same query runs cleanly. The
            // enclosing assertMemoryLeak is the authoritative net leak check.
            drain(query);
        });
    }
}
