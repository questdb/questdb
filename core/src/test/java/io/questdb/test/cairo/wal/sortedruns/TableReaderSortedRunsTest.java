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

package io.questdb.test.cairo.wal.sortedruns;

import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.wal.sortedruns.SortedRunsReader;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Verifies that {@link TableReader} recognises indexed-sorted-runs partitions, exposes
 * the correct {@link PartitionFormat}, and lazily opens
 * {@link SortedRunsReader} per partition.
 */
public class TableReaderSortedRunsTest extends AbstractCairoTest {

    @Test
    public void testGetAndInitSortedRunsReaderProducesValidPermutation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, val LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // Cross-commit O3: later commits land at earlier ts.
            execute("INSERT INTO x VALUES (5000, 50), (6000, 60)");
            execute("INSERT INTO x VALUES (1000, 10), (2000, 20)");
            execute("INSERT INTO x VALUES (3000, 30), (4000, 40)");
            final TableToken token = engine.verifyTableName("x");
            try (SortedRunsApplyDriver driver = new SortedRunsApplyDriver(engine)) {
                driver.apply(token);
            }

            try (TableReader reader = engine.getReader(token)) {
                Assert.assertEquals(1, reader.getPartitionCount());
                Assert.assertEquals(PartitionFormat.INDEXED_SORTED_RUNS, reader.getPartitionFormatFromMetadata(0));

                final SortedRunsReader tsReader = reader.getAndInitSortedRunsReader(0);
                Assert.assertEquals(3, tsReader.recordCount());
                Assert.assertEquals(6L, tsReader.getLogicalRowCount());

                // Verify the driver-produced _sortedruns.idx mmap holds a
                // valid permutation in ts-ascending order.
                reader.openPartition(0);
                final int colBase = reader.getColumnBase(0);
                final int absoluteCol = TableReader.getPrimaryColumnIndex(colBase, reader.getMetadata().getTimestampIndex());
                final long tsBase = reader.getColumn(absoluteCol).getPageAddress(0);
                final io.questdb.cairo.wal.sortedruns.SortedRunsIndex index = reader.getAndInitSortedRunsIndex(0);
                final long total = index.getRowCount();
                Assert.assertEquals(6L, total);

                long prev = Long.MIN_VALUE;
                final boolean[] seen = new boolean[(int) total];
                for (int i = 0; i < total; i++) {
                    final long physRowId = Unsafe.getLong(index.getAddress() + (long) i * Long.BYTES);
                    final long ts = Unsafe.getLong(tsBase + physRowId * Long.BYTES);
                    Assert.assertTrue("ts " + ts + " < prev " + prev, ts >= prev);
                    Assert.assertFalse("duplicate physRowId " + physRowId, seen[(int) physRowId]);
                    seen[(int) physRowId] = true;
                    prev = ts;
                }
            }
        });
    }

    @Test
    public void testNativeFormatReportedWhenNoSortedRuns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, val LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x VALUES (1000, 10), (2000, 20)");
            drainWalQueue();

            final TableToken token = engine.verifyTableName("x");
            try (TableReader reader = engine.getReader(token)) {
                Assert.assertEquals(1, reader.getPartitionCount());
                Assert.assertEquals(PartitionFormat.NATIVE, reader.getPartitionFormatFromMetadata(0));
            }
        });
    }

    @Test
    public void testSortedRunsFormatReported() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, val LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x VALUES (1000, 10), (2000, 20)");
            final TableToken token = engine.verifyTableName("x");
            try (SortedRunsApplyDriver driver = new SortedRunsApplyDriver(engine)) {
                driver.apply(token);
            }

            try (TableReader reader = engine.getReader(token)) {
                Assert.assertEquals(1, reader.getPartitionCount());
                Assert.assertEquals(PartitionFormat.INDEXED_SORTED_RUNS, reader.getPartitionFormatFromMetadata(0));
                Assert.assertTrue(reader.getTxFile().isPartitionSortedRuns(0));
                Assert.assertEquals(2L, reader.getTxFile().getPartitionSize(0));
            }
        });
    }
}
