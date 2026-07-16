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

package io.questdb.test.cairo;

import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableWriter;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Plan 3 (composite partitioning), Task 1: the {@code _txn} attached-partition record gets a
 * per-table stride -- 4 longs (32 bytes, today's byte-identical layout) for a plain table, 8 longs
 * for a COMPOSITE table (cellKey at slot 4, slots 5-7 reserved; forced to 8 rather than 5 because
 * {@code LongList.binarySearchBlock} needs a power-of-2 block size). Both {@link TableWriter} (via
 * {@link io.questdb.cairo.TxWriter}) and {@link TableReader} (via {@link io.questdb.cairo.TxReader})
 * derive the stride independently from the same {@code metadata.getPartitionSpec().getDimensionCount()
 * > 0} signal.
 * <p>
 * This task only establishes the stride + cellKey accessor machinery; it does not yet write or
 * meaningfully read a real cellKey (Task 2).
 */
public class CompositeTxCellTest extends AbstractCairoTest {

    @Test
    public void testStrideDerivedFromComposite() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exchange symbol, x double) " +
                    "timestamp(ts) partition by day, exchange");           // composite (1 dimension)
            execute("create table p (ts timestamp, x double) timestamp(ts) partition by day"); // plain

            try (TableWriter cw = getWriter("c"); TableWriter pw = getWriter("p")) {
                Assert.assertEquals(8, cw.getTxWriter().getLongsPerAttachedPartition());
                Assert.assertEquals(4, pw.getTxWriter().getLongsPerAttachedPartition());
                // stride 4 has no cellKey slot: a plain table always reports 0, without ever
                // reading attachedPartitions (safe even though table p has 0 committed partitions).
                Assert.assertEquals(0, pw.getTxWriter().getPartitionCellKey(0));
            }

            // The reader side is threaded independently of the writer side (TableReader owns its own
            // TxReader); verify it derives the same per-table stride from the same PartitionSpec signal.
            try (TableReader cr = getReader("c"); TableReader pr = getReader("p")) {
                Assert.assertEquals(8, cr.getTxFile().getLongsPerAttachedPartition());
                Assert.assertEquals(4, pr.getTxFile().getLongsPerAttachedPartition());
                Assert.assertEquals(0, pr.getTxFile().getPartitionCellKey(0));
            }
        });
    }
}
