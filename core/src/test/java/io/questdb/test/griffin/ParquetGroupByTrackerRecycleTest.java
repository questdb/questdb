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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Regression coverage for the per-query {@code MemoryTracker} recycle invariant
 * on the GROUP BY / aggregate path over a parquet partition.
 * <p>
 * A decoded parquet row group's native memory is charged to the per-query
 * tracker. The tracker is pooled and recycled across queries, so its {@code used}
 * word must be back to zero by the time the owning query releases it. If an
 * aggregate factory's decode buffers outlive the query (freed only at the next
 * execution's cursor setup instead of at end-of-query), the tracker is released
 * dirty and the next query that recycles the block sees a non-zero starting
 * {@code used}. {@link io.questdb.std.PerQueryMemoryTracker#init} asserts the
 * invariant, so re-executing an aggregate over a parquet partition under
 * {@code -ea} trips it if the buffers are not credited at end-of-query.
 * <p>
 * Each query is compiled once and the cursor opened/drained/closed several times,
 * so every iteration after the first recycles the tracker the previous iteration
 * released and the {@code init} guard fires on a regression.
 */
public class ParquetGroupByTrackerRecycleTest extends AbstractCairoTest {

    private static final int ROWS = 1_000;

    @Override
    public void setUp() {
        super.setUp();
        // Parallel aggregation over the parquet partition: the factory-owned per-worker
        // decode pools are the path whose buffers can outlive the per-query tracker.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_READ_PARQUET_ENABLED, "true");
        // Small row groups / page frames so the converted partition holds several
        // frames: the aggregate decodes more than one buffer per execution.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 100);
        // A configured (but generous) per-query limit keeps accounting on without
        // breaching, matching the production query_activity path.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 256 * 1024 * 1024L);
    }

    @Test
    public void testKeyedGroupByOverParquetRecyclesTrackerClean() throws Exception {
        // Keyed GROUP BY over the parquet partition: c in {0..4}, so 5 result rows.
        assertReExecutes("SELECT c, sum(a) s FROM t WHERE ts IN '1970-01-01' GROUP BY c", 5);
    }

    @Test
    public void testNotKeyedAggregateOverParquetRecyclesTrackerClean() throws Exception {
        // Not-keyed aggregate over the parquet partition, re-executed. This is the
        // shape of the cold-storage tests that first surfaced the defect.
        assertReExecutes("SELECT count() cnt, sum(a) s FROM t WHERE ts IN '1970-01-01'", 1);
    }

    @Test
    public void testNotKeyedAggregateWithFilterOverParquetRecyclesTrackerClean() throws Exception {
        // Not-keyed aggregate with a row filter: drives the late-materialization
        // decode path (decode the filter column, then the remaining aggregated
        // column into the same buffer). c < 1 selects c == 0 in the parquet
        // partition; the native sealing row (c = 9) is excluded.
        assertReExecutes("SELECT sum(a) s FROM t WHERE c < 1", 1);
    }

    private void assertReExecutes(String sql, int expectedRows) throws Exception {
        assertMemoryLeak(() -> {
            createParquetTable();
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                // Three executions of the same factory: iterations 2 and 3 recycle the
                // tracker released by the previous iteration. A tracker released dirty
                // trips the PerQueryMemoryTracker.init() guard here under -ea.
                for (int i = 0; i < 3; i++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        final Record record = cursor.getRecord();
                        int rows = 0;
                        while (cursor.hasNext()) {
                            rows++;
                        }
                        Assert.assertEquals("iteration " + i, expectedRows, rows);
                    }
                }
            }
        });
    }

    private void createParquetTable() throws Exception {
        execute("CREATE TABLE t (a LONG, c INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        // ROWS rows in the first (sealed) partition; a is the row index, c is a
        // low-cardinality key (x % 5). ts stays within 1970-01-01 (x seconds).
        execute(
                "INSERT INTO t " +
                        "SELECT x AS a, (x % 5)::int AS c, (x * 1_000_000L)::timestamp AS ts " +
                        "FROM long_sequence(" + ROWS + ")"
        );
        // A later, tiny partition seals the first so CONVERT can run on it. Its key
        // (c = 9) stays outside the c < 1 filter so it never pollutes the aggregate.
        execute("INSERT INTO t VALUES (-1, 9, '1970-01-02T00:00:00.000000Z')");
        execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '1970-01-01'");
    }
}
