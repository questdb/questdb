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
 * Verifies the claim in {@link io.questdb.std.MemoryTrackerWorkload}'s javadoc: a runaway QUERY
 * workload that breaches {@code cairo.query.memory.limit.bytes} must fail with a clean
 * {@link io.questdb.cairo.CairoException} and must not draw against the WAL_APPLY workload's
 * separate budget, so ingestion (WAL apply) keeps working afterwards.
 * <p>
 * The breaching query is a keyed GROUP BY over 50,000 distinct symbol values, which routes
 * through {@link io.questdb.cairo.map.Map}, one of the allocators bound to the active
 * {@link io.questdb.std.MemoryTracker} (see {@link MapMemoryTrackerTest}). A 1 MiB per-query
 * limit is far too small for that map to hold 50,000 groups, so the breach is attributable to
 * the tracked GROUP BY map rather than to an unrelated failure.
 * <p>
 * Two adjustments versus the naive version of this test, both discovered by running it:
 * <ul>
 * <li>Symbols are generated with {@code ('host' || (x % 50000))::symbol} rather than
 * {@code rnd_symbol(50000,8,8,0)}. {@code rnd_symbol}'s backing {@code RndStringMemory} has its
 * own, unrelated worst-case memory precheck ({@code cairo.rnd.memory.page.size} *
 * {@code cairo.rnd.memory.max.pages}, 1 MiB by default) sized off the requested cardinality; at
 * 50,000 symbols it breaches during INSERT, before the per-query limit under test is even
 * relevant, and for the wrong reason ({@code RndStringMemory}, not the tracked GROUP BY map).</li>
 * <li>Parallel GROUP BY is disabled ({@code setParallelGroupByEnabled(false)}) for the breaching
 * query. As documented on {@link MapMemoryTrackerTest#testGroupByFailsOnLargeKeySet}, a breach
 * inside the parallel path's shard-merge job surfaces as a query cancellation rather than the
 * per-query OOM, so only the sync path keeps this test's error-message assertion stable.</li>
 * </ul>
 */
public class QueryMemoryLimitIsolationTest extends AbstractCairoTest {

    @Test
    public void testIngestSurvivesQueryMemoryExhaustion() throws Exception {
        // A query limit small enough that a keyed GROUP BY over 50,000 distinct symbols cannot
        // fit, while WAL apply keeps its own untouched budget (WAL_APPLY is a separate
        // MemoryTrackerWorkload from QUERY, so this property is left at its production default).
        node1.setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 1024 * 1024);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE cpu (hostname SYMBOL, usage_user DOUBLE, ts TIMESTAMP)" +
                    " TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO cpu SELECT ('host' || (x % 50000))::symbol, rnd_double()," +
                    " timestamp_sequence(0, 1000) FROM long_sequence(500000)");
            drainWalQueue();
            assertQuery("SELECT count() FROM cpu")
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n500000\n");

            // Pin the synchronous GROUP BY path so the breach lands at a single deterministic
            // site in the map, per MapMemoryTrackerTest#testGroupByFailsOnLargeKeySet: a breach
            // inside the parallel path's shard-merge job surfaces as a cancellation instead.
            sqlExecutionContext.setParallelGroupByEnabled(false);

            // The query must fail cleanly rather than killing the process. The real message
            // comes from Unsafe.checkPerQueryAllocLimit, not from the plan's guess.
            assertException(
                    "SELECT hostname, avg(usage_user) FROM cpu GROUP BY hostname",
                    0,
                    "query memory limit exceeded [workload=QUERY"
            );

            // ...and ingestion must still work afterwards: WAL apply is a distinct
            // MemoryTrackerWorkload with its own budget, untouched by the QUERY breach above.
            execute("INSERT INTO cpu VALUES ('after', 1.0, 999999999)");
            drainWalQueue();
            assertQuery("SELECT count() FROM cpu")
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n500001\n");
        });
    }
}
