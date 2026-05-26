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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.cairo.wal.sortedruns.SortedRunsReader;
import io.questdb.cairo.wal.sortedruns.SortedRunsRecord;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class SortedRunsReaderTest extends AbstractCairoTest {

    @Test
    public void testFindRunsOverlapping() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, val LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x VALUES (1000, 10), (2000, 20)");
            execute("INSERT INTO x VALUES (5000, 50), (6000, 60)");
            execute("INSERT INTO x VALUES (9000, 90)");
            final TableToken token = engine.verifyTableName("x");
            try (SortedRunsApplyDriver driver = new SortedRunsApplyDriver(engine)) {
                driver.apply(token);
            }

            try (SortedRunsReader reader = openReader(token, 0L)) {
                Assert.assertEquals(3, reader.recordCount());

                final int[] loHi = new int[2];

                // Whole range.
                Assert.assertTrue(reader.findRunsOverlapping(0L, 10000L, loHi));
                Assert.assertEquals(0, loHi[0]);
                Assert.assertEquals(2, loHi[1]);

                // Below all.
                Assert.assertFalse(reader.findRunsOverlapping(0L, 500L, loHi));

                // Above all.
                Assert.assertFalse(reader.findRunsOverlapping(20000L, 30000L, loHi));

                // Overlaps middle run only.
                Assert.assertTrue(reader.findRunsOverlapping(5500L, 5500L, loHi));
                Assert.assertEquals(1, loHi[0]);
                Assert.assertEquals(1, loHi[1]);

                // Spans middle + last.
                Assert.assertTrue(reader.findRunsOverlapping(5500L, 9500L, loHi));
                Assert.assertEquals(1, loHi[0]);
                Assert.assertEquals(2, loHi[1]);
            }
        });
    }

    @Test
    public void testGlobalTsAndRowCount() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, val LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x VALUES (1000, 10), (2000, 20), (3000, 30)");
            execute("INSERT INTO x VALUES (4000, 40)");
            final TableToken token = engine.verifyTableName("x");
            try (SortedRunsApplyDriver driver = new SortedRunsApplyDriver(engine)) {
                driver.apply(token);
            }

            try (SortedRunsReader reader = openReader(token, 0L)) {
                Assert.assertEquals(2, reader.recordCount());
                Assert.assertEquals(1000L, reader.getGlobalMinTs());
                Assert.assertEquals(4000L, reader.getGlobalMaxTs());
                Assert.assertEquals(4L, reader.getLogicalRowCount());
            }
        });
    }

    @Test
    public void testMaterialiseSortedSliceForwardO3() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, val LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // Commit 1: ts 5000, 6000 (in-order within commit, but later
            // commit 2 has earlier ts -> cross-commit O3).
            execute("INSERT INTO x VALUES (5000, 50), (6000, 60)");
            execute("INSERT INTO x VALUES (1000, 10), (2000, 20)");
            execute("INSERT INTO x VALUES (3000, 30), (4000, 40)");
            final TableToken token = engine.verifyTableName("x");
            try (SortedRunsApplyDriver driver = new SortedRunsApplyDriver(engine)) {
                driver.apply(token);
            }

            try (SortedRunsReader reader = openReader(token, 0L);
                 MemoryCMR tsMem = openTsColumn(token, 0L, reader.getLogicalRowCount())) {
                final long total = reader.getLogicalRowCount();
                final long bytes = total * Long.BYTES;
                final long scratchAddr = io.questdb.std.Unsafe.malloc(bytes, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                try {
                    reader.materialiseSortedSlice(0, total, tsMem.addressOf(0), Long.BYTES, scratchAddr);
                    // Decode timestamps via physRowId and verify ascending order.
                    long prevTs = Long.MIN_VALUE;
                    final boolean[] seen = new boolean[(int) total];
                    for (int i = 0; i < total; i++) {
                        final long physRowId = io.questdb.std.Unsafe.getUnsafe().getLong(scratchAddr + (long) i * Long.BYTES);
                        final long ts = io.questdb.std.Unsafe.getUnsafe().getLong(tsMem.addressOf(0) + physRowId * Long.BYTES);
                        Assert.assertTrue("row " + i + " ts " + ts + " < prev " + prevTs, ts >= prevTs);
                        prevTs = ts;
                        Assert.assertTrue(physRowId >= 0 && physRowId < total);
                        Assert.assertFalse("duplicate physRowId " + physRowId, seen[(int) physRowId]);
                        seen[(int) physRowId] = true;
                    }
                } finally {
                    io.questdb.std.Unsafe.free(scratchAddr, bytes, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testMaterialiseSortedSliceReverse() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, val LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x VALUES (1000, 10), (2000, 20), (3000, 30)");
            execute("INSERT INTO x VALUES (4000, 40), (5000, 50)");
            final TableToken token = engine.verifyTableName("x");
            try (SortedRunsApplyDriver driver = new SortedRunsApplyDriver(engine)) {
                driver.apply(token);
            }

            try (SortedRunsReader reader = openReader(token, 0L);
                 MemoryCMR tsMem = openTsColumn(token, 0L, reader.getLogicalRowCount())) {
                final long total = reader.getLogicalRowCount();
                final long bytes = total * Long.BYTES;
                final long scratchAddr = io.questdb.std.Unsafe.malloc(bytes, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                try {
                    reader.materialiseSortedSliceReverse(0, total, tsMem.addressOf(0), Long.BYTES, scratchAddr);
                    long prevTs = Long.MAX_VALUE;
                    for (int i = 0; i < total; i++) {
                        final long physRowId = io.questdb.std.Unsafe.getUnsafe().getLong(scratchAddr + (long) i * Long.BYTES);
                        final long ts = io.questdb.std.Unsafe.getUnsafe().getLong(tsMem.addressOf(0) + physRowId * Long.BYTES);
                        Assert.assertTrue("row " + i + " ts " + ts + " > prev " + prevTs, ts <= prevTs);
                        prevTs = ts;
                    }
                } finally {
                    io.questdb.std.Unsafe.free(scratchAddr, bytes, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testRecordAt() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, val LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x VALUES (1000, 10), (2000, 20)");
            execute("INSERT INTO x VALUES (3000, 30)");
            final TableToken token = engine.verifyTableName("x");
            try (SortedRunsApplyDriver driver = new SortedRunsApplyDriver(engine)) {
                driver.apply(token);
            }

            try (SortedRunsReader reader = openReader(token, 0L)) {
                Assert.assertEquals(2, reader.recordCount());
                final SortedRunsRecord rec = new SortedRunsRecord();
                reader.recordAt(0, rec);
                Assert.assertEquals(0L, rec.getPhysRowStart());
                Assert.assertEquals(2, rec.getRowCount());
                Assert.assertEquals(1000L, rec.getMinTs());
                Assert.assertEquals(2000L, rec.getMaxTs());
                reader.recordAt(1, rec);
                Assert.assertEquals(2L, rec.getPhysRowStart());
                Assert.assertEquals(1, rec.getRowCount());
                Assert.assertEquals(3000L, rec.getMinTs());
                Assert.assertEquals(3000L, rec.getMaxTs());
            }
        });
    }

    private static MemoryCMR openTsColumn(TableToken token, long partitionTimestamp, long rowCount) {
        final long nameTxn;
        try (Path tablePath = new Path();
             TxReader txReader = new TxReader(engine.getConfiguration().getFilesFacade())) {
            tablePath.of(engine.getConfiguration().getDbRoot()).concat(token).concat(TableUtils.TXN_FILE_NAME);
            txReader.ofRO(tablePath.$(), ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY);
            txReader.unsafeLoadAll();
            final TimestampDriver driver = ColumnType.getTimestampDriver(ColumnType.TIMESTAMP_MICRO);
            final long partitionFloor = driver.getPartitionFloorMethod(PartitionBy.DAY).floor(partitionTimestamp);
            int idx = -1;
            for (int i = 0, n = txReader.getPartitionCount(); i < n; i++) {
                if (txReader.getPartitionTimestampByIndex(i) == partitionFloor) {
                    idx = i;
                    break;
                }
            }
            Assert.assertTrue("partition not found", idx >= 0);
            nameTxn = txReader.getPartitionNameTxn(idx);
        }
        final Path path = new Path();
        path.of(engine.getConfiguration().getDbRoot()).concat(token);
        TableUtils.setPathForNativePartition(path, ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY, 0L, nameTxn);
        TableUtils.dFile(path, "ts");
        final MemoryCMR mem = Vm.getCMRInstance();
        mem.of(engine.getConfiguration().getFilesFacade(), path.$(), 0, rowCount * Long.BYTES, MemoryTag.MMAP_DEFAULT);
        path.close();
        return mem;
    }

    private static SortedRunsReader openReader(TableToken token, long partitionTimestamp) {
        final long validSize;
        final long nameTxn;
        try (Path tablePath = new Path();
             TxReader txReader = new TxReader(engine.getConfiguration().getFilesFacade())) {
            tablePath.of(engine.getConfiguration().getDbRoot()).concat(token).concat(TableUtils.TXN_FILE_NAME);
            txReader.ofRO(tablePath.$(), ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY);
            txReader.unsafeLoadAll();
            final TimestampDriver driver = ColumnType.getTimestampDriver(ColumnType.TIMESTAMP_MICRO);
            final long partitionFloor = driver.getPartitionFloorMethod(PartitionBy.DAY).floor(partitionTimestamp);
            int idx = -1;
            for (int i = 0, n = txReader.getPartitionCount(); i < n; i++) {
                if (txReader.getPartitionTimestampByIndex(i) == partitionFloor) {
                    idx = i;
                    break;
                }
            }
            Assert.assertTrue("partition not found", idx >= 0);
            Assert.assertTrue("sorted-runs flag not set", txReader.isPartitionSortedRuns(idx));
            nameTxn = txReader.getPartitionNameTxn(idx);
            validSize = txReader.getPartitionSortedRunsFileSize(idx);
        }

        final Path path = new Path();
        path.of(engine.getConfiguration().getDbRoot()).concat(token);
        TableUtils.setPathForNativePartition(path, ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY, 0L, nameTxn);
        path.concat(io.questdb.cairo.wal.sortedruns.SortedRunsFormat.FILE_NAME);
        final SortedRunsReader reader = new SortedRunsReader();
        reader.of(engine.getConfiguration().getFilesFacade(), path.$(), validSize);
        path.close();
        return reader;
    }
}
