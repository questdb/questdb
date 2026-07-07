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
import io.questdb.cairo.CairoException;
import io.questdb.mp.WorkerPool;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Proves that native memory allocated by the parallel covered-index decode path is
 * charged to (and therefore capped by) the per-query memory tracker, exactly like the
 * parquet decode path.
 * <p>
 * A low-selectivity {@code sym = 'A'} over many partitions materializes a wide covered
 * VARCHAR for every matched row into per-worker covered decode buffers. Those buffers
 * dwarf the rest of the query's working set, so a per-query limit that comfortably fits
 * the group-by map, address cache and filter scratch must still be breached by the
 * covered decode alone. A narrow {@code sym = 'RARE'} control proves the covered path
 * runs fine under the same limit when its decode footprint is small - i.e. the breach is
 * caused specifically by the covered decode growing past the ceiling, not by unrelated
 * machinery.
 */
public class CoveringIndexMemoryLimitTest extends AbstractCairoTest {

    private static final long QUERY_MEMORY_LIMIT = 8 * 1024 * 1024L;

    @Override
    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
        // Many small page frames so the covered scan fans out across the worker pool and
        // every worker's pool retains covered buffers for the whole query.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1_000);
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, QUERY_MEMORY_LIMIT);
        super.setUp();
    }

    @Test
    public void testCoveredDecodeIsCappedByPerQueryLimit() throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        engine.execute(
                                "CREATE TABLE t (" +
                                        "  ts TIMESTAMP," +
                                        "  sym SYMBOL INDEX TYPE POSTING INCLUDE (grp, payload)," +
                                        "  grp VARCHAR," +
                                        "  payload LONG" +
                                        ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL",
                                sqlExecutionContext
                        );
                        // ~100k rows over many day-partitions. Nearly all are sym='A' with a wide
                        // (200-byte) covered VARCHAR, so decoding the covered 'A' set materializes
                        // roughly 100k * ~200B ~ 20MB of covered buffers - far above the 8MB limit.
                        // A sprinkling of sym='RARE' rows gives a tiny covered set for the control.
                        engine.execute(
                                "INSERT INTO t SELECT" +
                                        "  (x * 600000000L)::timestamp," +
                                        "  CASE WHEN x % 10000 = 0 THEN 'RARE' ELSE 'A' END," +
                                        "  rpad(('g' || (x % 7))::varchar, 200, 'x')," +
                                        "  x" +
                                        " FROM long_sequence(100000)",
                                sqlExecutionContext
                        );
                        engine.releaseAllWriters();

                        // Control: the covered decode path runs fine under the same limit when the
                        // matched (covered) set is tiny. Proves the query is otherwise valid and the
                        // limit is not tripped by unrelated machinery.
                        final StringSink sink = new StringSink();
                        TestUtils.printSql(
                                engine,
                                sqlExecutionContext,
                                "SELECT grp, sum(payload) FROM t WHERE sym = 'RARE' GROUP BY grp ORDER BY grp",
                                sink
                        );

                        // Breach: the low-selectivity covered decode must push the per-query tracker
                        // past its ceiling. Before covered buffers are charged to the tracker this
                        // query completes on global-only accounting and no breach is raised.
                        try {
                            TestUtils.printSql(
                                    engine,
                                    sqlExecutionContext,
                                    "SELECT grp, sum(payload) FROM t WHERE sym = 'A' GROUP BY grp ORDER BY grp",
                                    sink
                            );
                            Assert.fail("expected a per-query memory breach from covered decode");
                        } catch (CairoException e) {
                            Assert.assertTrue(
                                    "expected isOutOfMemory(), got: " + e.getFlyweightMessage(),
                                    e.isOutOfMemory()
                            );
                            TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }
}
