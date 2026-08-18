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
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * O3 partition SPLIT declines the covered append path, in its own class ON
 * PURPOSE: the split properties below are static overrides and are only reset in
 * {@code @AfterClass}, so leaving them in the shared class silently turned every
 * later test's mid-partition write into a split. That made three sibling tests
 * report "append path never fired" while individually green - a leak that is
 * much easier to prevent structurally than to remember.
 */
public class MidPartitionAppendSplitDeclineTest extends AbstractCairoTest {

    private static final int COMMITS = 8;
    private static final int ROWS = 400;
    private static final int SEED = 40000;

    @Before
    public void enableCounters() {
        PostingIndexWriter.COVERING_COUNTERS_ENABLED = true;
        PostingIndexWriter.COVERING_MIDPART_APPEND_COUNT.set(0);
    }

    @After
    public void disableCounters() {
        PostingIndexWriter.COVERING_COUNTERS_ENABLED = false;
        PostingIndexWriter.COVERING_MIDPART_APPEND_DISABLED = false;
    }

    /**
     * O3 PARTITION SPLIT. canSkipRebuildForPartition requires
     * {@code o3SplitPartitionSize == 0}, so a split must decline the append path
     * and rebuild - untested in either direction. The split threshold is dropped
     * far below the partition size so ordinary O3 writes split rather than
     * rewrite.
     */
    @Test
    public void testPartitionSplitDeclinesAndStaysCorrect() throws Exception {
        // All three are required. Without MID_PARTITION_MAX_SPLITS a mid
        // partition never splits at all, which is how the first version of this
        // test passed while producing zero splits.
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 20);
        node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 20);
        assertMemoryLeak(() -> {
            createWalTables();
            insertBoth("2024-01-01", 0, 200);
            insertBoth("2024-01-03", 200, 200);
            insertBoth("2024-01-02", 400, SEED);

            PostingIndexWriter.COVERING_MIDPART_APPEND_COUNT.set(0);
            for (int c = 0; c < COMMITS; c++) {
                // land BEHIND the partition max, which forces a merge/split
                // rather than an append
                // near the END of a large partition: a big untouched prefix and
                // a small rewritten suffix is the shape that splits
                insertBothAt("2024-01-02", (long) SEED - 900L + (long) c * 7L, 100_000L + (long) c * ROWS, ROWS);
                }

            // Setup discriminator: without this the test passes when no split
            // ever happened, i.e. while testing nothing it names.
            final long partitions = selectLong("SELECT count() FROM table_partitions('t')");
            Assert.assertTrue("test setup: the O3 writes must actually SPLIT the partition, count="
                    + partitions, partitions > 3);
            // ... and the append path must have refused every one of them:
            // canSkipRebuildForPartition requires o3SplitPartitionSize == 0.
            Assert.assertEquals("a split must never take the covered append path",
                    0, PostingIndexWriter.COVERING_MIDPART_APPEND_COUNT.get());

            assertNotSuspended();
            assertIndexAgreesWithColumn();
            assertMatchesControl();
        });
    }


    private void assertIndexAgreesWithColumn() throws Exception {
        for (int s = 0; s < 4; s++) {
            assertSqlCursors(
                    "SELECT /*+ no_index */ ts, sym, value FROM t WHERE sym = 'S" + s + "' ORDER BY ts",
                    "SELECT ts, sym, value FROM t WHERE sym = 'S" + s + "' ORDER BY ts"
            );
        }
    }

    private void assertMatchesControl() throws Exception {
        assertSqlCursors(
                "SELECT ts, sym, value FROM ctl ORDER BY ts, value",
                "SELECT ts, sym, value FROM t ORDER BY ts, value"
        );
    }

    private void assertNotSuspended() {
        // BYPASS WAL has no sequencer: a failed commit throws from INSERT instead.
    }

    private void createWalTables() throws Exception {
        // BYPASS WAL: splits are observable directly here. On a WAL table the
        // apply path squashes them back, so table_partitions() cannot be used to
        // prove a split ever happened.
        execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (value), value DOUBLE)"
                + " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("CREATE TABLE ctl (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING, value DOUBLE)"
                + " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
    }

    private long selectLong(CharSequence sql) throws Exception {
        try (RecordCursorFactory factory = select(sql);
             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Assert.assertTrue("query must return one row [sql=" + sql + ']', cursor.hasNext());
            return cursor.getRecord().getLong(0);
        }
    }

    private void insertBoth(String day, long v0, int rows) throws Exception {
        insertBothAt(day, v0, v0, rows);
    }

    /**
     * tsBaseMicros is the timestamp base and is deliberately INDEPENDENT of v0,
     * which only feeds sym and value. Deriving the timestamp from v0 pushes the
     * rows past the partition max, turning an intended O3 write into a plain
     * append - which is how this test first ran green producing no splits.
     */
    private void insertBothAt(String day, long tsBaseMicros, long v0, int rows) throws Exception {
        final String tail = " SELECT dateadd('u', (" + tsBaseMicros + " + x)::INT,"
                + " '" + day + "T00:00:00Z'::TIMESTAMP), 'S' || ((" + v0 + " + x) % 4),"
                + " (" + v0 + " + x)::DOUBLE FROM long_sequence(" + rows + ")";
        execute("INSERT INTO t" + tail);
        execute("INSERT INTO ctl" + tail);
    }
}
