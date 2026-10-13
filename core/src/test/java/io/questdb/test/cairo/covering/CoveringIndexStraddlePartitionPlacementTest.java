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

import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.test.AbstractCairoTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * The single-transaction prefix split commits part of a transaction to the last
 * partition and routes the rest through O3. The obvious way for that to be wrong
 * is for a row to land on the wrong side of the boundary, which content-equality
 * assertions alone would not localise. This asserts PLACEMENT directly.
 * <p>
 * The split point is {@code partitionTimestampHi} located by
 * {@code boundedBinarySearchIndexT}, and the fast path only runs on an ascending,
 * pure-append transaction, so the prefix is exactly the rows belonging to the last
 * partition. This test is the empirical check of that.
 */
public class CoveringIndexStraddlePartitionPlacementTest extends AbstractCairoTest {

    @After
    public void resetFlags() {
        PostingIndexWriter.COVERING_FASTPATH_DISABLED = false;
        PostingIndexWriter.COVERING_COUNTERS_ENABLED = false;
    }

    @Test
    public void testStraddleDoesNotMisplaceRows() throws Exception {
        assertMemoryLeak(() -> {
            PostingIndexWriter.COVERING_COUNTERS_ENABLED = true;

            // Reference placement: fast path forced off, so every transaction goes
            // through O3, which is the behaviour this change must reproduce.
            PostingIndexWriter.COVERING_FASTPATH_DISABLED = true;
            PostingIndexWriter.COVERING_BLOCK_FASTPATH_COUNT.set(0);
            build("ref");
            final long refFired = PostingIndexWriter.COVERING_BLOCK_FASTPATH_COUNT.get();

            PostingIndexWriter.COVERING_FASTPATH_DISABLED = false;
            PostingIndexWriter.COVERING_BLOCK_FASTPATH_COUNT.set(0);
            build("split");
            final long splitFired = PostingIndexWriter.COVERING_BLOCK_FASTPATH_COUNT.get();

            // Without this the comparison below could pass because the fast path
            // never ran at all.
            Assert.assertEquals("fast path fired while disabled", 0, refFired);
            Assert.assertTrue("fast path never fired -- test is vacuous", splitFired > 0);

            // Placement must match the O3 reference exactly: same partitions, same
            // row counts, same min/max timestamp in each.
            assertSqlCursors(
                    "SELECT name, numRows, minTimestamp, maxTimestamp FROM table_partitions('ref') ORDER BY name",
                    "SELECT name, numRows, minTimestamp, maxTimestamp FROM table_partitions('split') ORDER BY name"
            );

            // Intrinsic check, independent of the reference: a partition whose min
            // and max fall in different hours contains a row that does not belong
            // to it.
            assertSqlCursors(
                    "SELECT 0L AS straddlingPartitions",
                    "SELECT count() AS straddlingPartitions FROM table_partitions('split')" +
                            " WHERE date_trunc('hour', minTimestamp) <> date_trunc('hour', maxTimestamp)"
            );

            // Content, and the covering index against a full scan.
            assertSqlCursors(
                    "SELECT ts, sym, val FROM ref ORDER BY ts, sym, val",
                    "SELECT ts, sym, val FROM split ORDER BY ts, sym, val"
            );
            assertSqlCursors(
                    "SELECT ts, val FROM split WHERE /*+ no_index(sym) */ sym = 'S5' ORDER BY ts",
                    "SELECT ts, val FROM split WHERE sym = 'S5' ORDER BY ts"
            );
        });
    }

    private void build(String t) throws Exception {
        execute("CREATE TABLE " + t + " (ts TIMESTAMP," +
                " sym SYMBOL INDEX TYPE POSTING INCLUDE (val), val DOUBLE)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        execute("INSERT INTO " + t + " (ts, sym, val) " +
                "SELECT timestamp_sequence('2024-01-01T00:00:00', 1000), 'S' || (x % 97), x::double " +
                "FROM long_sequence(100000)");
        drainWalQueue();
        // Each insert starts inside the current last partition and runs past its
        // end, so the transaction straddles a boundary.
        for (int i = 0; i < 3; i++) {
            execute("INSERT INTO " + t + " (ts, sym, val) " +
                    "SELECT timestamp_sequence('2024-01-01T0" + i + ":59:30', 1000)," +
                    " 'S' || (x % 97), x::double FROM long_sequence(200000)");
            drainWalQueue();
        }
    }
}
