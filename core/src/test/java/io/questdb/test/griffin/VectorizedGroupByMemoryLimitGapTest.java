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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Documents a gap in per-query memory enforcement on the vectorized (Rosti)
 * GROUP BY path.
 * <p>
 * {@code cairo.query.memory.limit.bytes} and {@code ram.usage.limit.bytes} are
 * both enforced inside {@link io.questdb.std.Unsafe#malloc} /
 * {@code realloc}, via {@code checkAllocLimit} and
 * {@code checkPerQueryAllocLimit}. Rosti allocates its hash table in native
 * code and reports the size afterwards through
 * {@link io.questdb.std.Unsafe#recordMemAlloc}, which only increments counters
 * and performs no limit check. A vectorized GROUP BY can therefore grow past
 * both limits with nothing to stop it short of the OS OOM-killer.
 * <p>
 * The two tests below are the same query shape at the same cardinality under
 * the same limit, differing only in key type — which is what selects the
 * execution path:
 * <ul>
 *     <li>SYMBOL key vectorizes, is unenforced, and completes.</li>
 *     <li>VARCHAR key does not vectorize, routes through the tracker-aware
 *         async path, and is correctly rejected.</li>
 * </ul>
 * These tests assert <em>current</em> behaviour. If the vectorized path gains a
 * pre-allocation check, {@link #testVectorizedSymbolKeyGroupByIsNotBounded()}
 * will start failing — that is the intended signal, and the fix is to move the
 * test to expect rejection, not to weaken it.
 *
 * @see io.questdb.std.Rosti
 */
public class VectorizedGroupByMemoryLimitGapTest extends AbstractCairoTest {

    private static final int KEY_COUNT = 500_000;
    private static final int QUERY_MEMORY_LIMIT = 256 * 1024;

    /**
     * Control arm. A VARCHAR key defeats vectorization, so the query runs on
     * the async path whose allocations go through the tracked malloc family.
     * The limit applies and the query is rejected cleanly.
     */
    @Test
    public void testNonVectorizedVarcharKeyGroupByIsBounded() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, QUERY_MEMORY_LIMIT);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE cpu_varchar AS (" +
                    "SELECT ('host' || x)::varchar hostname, rnd_double() usage_user," +
                    " timestamp_sequence(0, 1000) ts" +
                    " FROM long_sequence(" + KEY_COUNT + ")) TIMESTAMP(ts) PARTITION BY DAY");
            assertException(
                    "SELECT hostname, avg(usage_user) FROM cpu_varchar GROUP BY hostname",
                    0,
                    "query memory limit exceeded"
            );
        });
    }

    /**
     * The gap. A SYMBOL key vectorizes, so the hash table is allocated inside
     * Rosti and only reported via recordMemAlloc, which cannot refuse it. The
     * query completes despite needing far more than the configured limit.
     */
    @Test
    public void testVectorizedSymbolKeyGroupByIsNotBounded() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, QUERY_MEMORY_LIMIT);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE cpu_symbol AS (" +
                    "SELECT ('host' || x)::symbol hostname, rnd_double() usage_user," +
                    " timestamp_sequence(0, 1000) ts" +
                    " FROM long_sequence(" + KEY_COUNT + ")) TIMESTAMP(ts) PARTITION BY DAY");
            // Succeeds today. Asserting the row count keeps this honest: the
            // query really does build all KEY_COUNT groups under a 256 KiB
            // limit, rather than passing because it silently did less work.
            assertQuery("SELECT count() FROM (SELECT hostname, avg(usage_user) FROM cpu_symbol GROUP BY hostname)")
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n" + KEY_COUNT + "\n");
        });
    }
}
