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

package io.questdb.test.cairo;

import io.questdb.PropertyKey;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Regression lock for the composite gate on {@code TableWriter#tryFastAppendInOrderBlock} (master's
 * WAL block fast-append, merged 2026-08-10).
 * <p>
 * That path appends a block's columns onto the writer's single shared "last partition" column
 * handles ({@code cthAppendWalColumnToLastPartition} + {@code applyFromWalLagToLastPartition}).
 * Those handles are keyed by DAY only and are never repointed at a real {@code <day>/<cell>}
 * segment for a composite table, so a routed composite row taking this path would be written into
 * the orphan bare day directory -- the identical hazard {@code applyFromWalLagToLastPartitionPossible}
 * already refuses for composite.
 * <p>
 * When this gate was added, a routed composite table provably DID enter the method and was rejected
 * only incidentally by the unrelated {@code isLastPartitionClosed() && !isEmptyTable()} guard. This
 * test pins the intended behaviour so that a future change to those incidental guards (or to when
 * composite keeps a partition open) cannot silently re-open the hazard.
 * <p>
 * {@link #testPlainTableStillUsesBlockFastAppend()} is the POSITIVE CONTROL: without it, the
 * composite assertion is vacuous -- it would pass simply because the test shape never drives the
 * block fast-append at all.
 */
public class CompositeBlockFastAppendGateTest extends AbstractCairoTest {

    @Test
    public void testCompositeTableNeverUsesBlockFastAppend() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 10_000_000);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY, exch WAL");
            // Route two distinct cells so the table is a REAL (non-dormant) composite.
            execute("INSERT INTO c VALUES ('2024-01-01T00:00:00Z', 'BTC', -1.0)");
            execute("INSERT INTO c VALUES ('2024-01-01T00:00:01Z', 'ETH', -2.0)");
            drainWalQueue();

            PostingIndexWriter.COVERING_COUNTERS_ENABLED = true;
            PostingIndexWriter.COVERING_BLOCK_FASTPATH_COUNT.set(0);
            try {
                appendAscendingBatches("c");
                drainWalQueue();

                Assert.assertEquals(
                        "a composite table must never take the cell-blind block fast-append",
                        0,
                        PostingIndexWriter.COVERING_BLOCK_FASTPATH_COUNT.get()
                );
                // ... and the rows are all present, via the cell-aware O3 routing.
                assertSqlCursors("SELECT 1002L AS c", "SELECT count(*) AS c FROM c");
            } finally {
                PostingIndexWriter.COVERING_COUNTERS_ENABLED = false;
            }
        });
    }

    @Test
    public void testPlainTableStillUsesBlockFastAppend() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 10_000_000);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE p (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO p VALUES ('2024-01-01T00:00:00Z', 'BTC', -1.0)");
            drainWalQueue();

            PostingIndexWriter.COVERING_COUNTERS_ENABLED = true;
            PostingIndexWriter.COVERING_BLOCK_FASTPATH_COUNT.set(0);
            try {
                appendAscendingBatches("p");
                drainWalQueue();

                Assert.assertTrue(
                        "POSITIVE CONTROL: this shape must actually drive the block fast-append on a plain"
                                + " table, else the composite assertion above proves nothing",
                        PostingIndexWriter.COVERING_BLOCK_FASTPATH_COUNT.get() > 0
                );
                assertSqlCursors("SELECT 1001L AS c", "SELECT count(*) AS c FROM p");
            } finally {
                PostingIndexWriter.COVERING_COUNTERS_ENABLED = false;
            }
        });
    }

    /**
     * Five ascending transactions drained as ONE block: the shape that reaches the in-order block
     * fast-append (a pure append past the committed max, inside the last partition).
     */
    private void appendAscendingBatches(String table) throws Exception {
        long base = 0;
        for (int b = 0; b < 5; b++) {
            execute("INSERT INTO " + table +
                    " SELECT dateadd('s', (" + base + " + x)::INT, '2024-01-01T00:10:00Z'::TIMESTAMP),"
                    + " 'S' || ((" + base + " + x) % 4), (" + base + " + x)::DOUBLE"
                    + " FROM long_sequence(200)");
            base += 200;
        }
    }
}
