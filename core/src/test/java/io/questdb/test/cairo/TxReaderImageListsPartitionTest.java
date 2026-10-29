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

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TxReader;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.LongConsumer;

import static io.questdb.cairo.TableUtils.*;

public class TxReaderImageListsPartitionTest extends AbstractCairoTest {
    private static final int ENTRY_BYTES = LONGS_PER_TX_ATTACHED_PARTITION * Long.BYTES;
    // TxReader.PARTITION_NAME_TX_OFFSET and PARTITION_TS_OFFSET are protected: the word indexes of an entry's
    // name txn and timestamp.
    private static final int NAME_TXN_WORD = 2;
    private static final int TS_WORD = 0;

    @Test
    public void testAreaBIsReadWhenVersionIsOdd() throws Exception {
        final int entryCount = 3;
        final long size = tableEnd(0, entryCount);
        withImage(size, addr -> {
            putTable(addr, 0, entryCount);
            Unsafe.getUnsafe().putLong(addr + TX_BASE_OFFSET_VERSION_64, 1);
            // Area A says the image is damaged, so a lookup that reads area A answers "listed" for anything.
            putArea(addr, true, -1, 0, entryCount * ENTRY_BYTES);
            putArea(addr, false, TX_BASE_HEADER_SIZE, 0, entryCount * ENTRY_BYTES);
            Assert.assertTrue(TxReader.imageListsPartition(addr, size, timestampAt(1), nameTxnAt(1)));
            Assert.assertFalse(TxReader.imageListsPartition(addr, size, timestampAt(1) + 1, nameTxnAt(1)));
        });
    }

    @Test
    public void testDuplicateTimestampListsBothNameTxns() throws Exception {
        final long size = tableEnd(0, 4);
        withImage(size, addr -> {
            putArea(addr, true, TX_BASE_HEADER_SIZE, 0, 4 * ENTRY_BYTES);
            putEntry(addr, 0, 0, 100, -1);
            putEntry(addr, 0, 1, 200, 3);
            putEntry(addr, 0, 2, 200, 5);
            putEntry(addr, 0, 3, 300, -1);
            Assert.assertTrue(TxReader.imageListsPartition(addr, size, 100, -1));
            Assert.assertTrue(TxReader.imageListsPartition(addr, size, 200, 3));
            Assert.assertTrue(TxReader.imageListsPartition(addr, size, 200, 5));
            Assert.assertTrue(TxReader.imageListsPartition(addr, size, 300, -1));
            Assert.assertFalse(TxReader.imageListsPartition(addr, size, 200, -1));
            Assert.assertFalse(TxReader.imageListsPartition(addr, size, 200, 4));
            Assert.assertFalse(TxReader.imageListsPartition(addr, size, 300, 5));
        });
    }

    @Test
    public void testEmptyPartitionTableListsNothing() throws Exception {
        final long size = tableEnd(0, 0);
        withImage(size, addr -> {
            putArea(addr, true, TX_BASE_HEADER_SIZE, 0, 0);
            Assert.assertFalse(TxReader.imageListsPartition(addr, size, 0, -1));
            Assert.assertFalse(TxReader.imageListsPartition(addr, size, timestampAt(0), nameTxnAt(0)));
        });
    }

    @Test
    public void testEveryEntryListedAndEveryOtherVersionMissing() throws Exception {
        for (int entryCount = 1; entryCount < 9; entryCount++) {
            assertEveryEntryListedAndEveryOtherVersionMissing(entryCount);
        }
        assertEveryEntryListedAndEveryOtherVersionMissing(1_000);
    }

    @Test
    public void testImageShorterThanHeaderListed() throws Exception {
        final long size = tableEnd(0, 2);
        withImage(size, addr -> {
            putTable(addr, 0, 2);
            Assert.assertFalse(TxReader.imageListsPartition(addr, size, timestampAt(0) + 1, -1));
            Assert.assertTrue(TxReader.imageListsPartition(addr, TX_BASE_HEADER_SIZE - 1, timestampAt(0) + 1, -1));
            Assert.assertTrue(TxReader.imageListsPartition(addr, 0, timestampAt(0) + 1, -1));
        });
    }

    @Test
    public void testNegativeAndExtremeTimestamps() throws Exception {
        final long size = tableEnd(0, 4);
        withImage(size, addr -> {
            putArea(addr, true, TX_BASE_HEADER_SIZE, 0, 4 * ENTRY_BYTES);
            putEntry(addr, 0, 0, Long.MIN_VALUE, -1);
            putEntry(addr, 0, 1, -86_400_000_000L, 2);
            putEntry(addr, 0, 2, 0, -1);
            putEntry(addr, 0, 3, Long.MAX_VALUE, 7);
            Assert.assertTrue(TxReader.imageListsPartition(addr, size, Long.MIN_VALUE, -1));
            Assert.assertTrue(TxReader.imageListsPartition(addr, size, -86_400_000_000L, 2));
            Assert.assertTrue(TxReader.imageListsPartition(addr, size, 0, -1));
            Assert.assertTrue(TxReader.imageListsPartition(addr, size, Long.MAX_VALUE, 7));
            Assert.assertFalse(TxReader.imageListsPartition(addr, size, Long.MIN_VALUE, 0));
            Assert.assertFalse(TxReader.imageListsPartition(addr, size, Long.MIN_VALUE + 1, -1));
            Assert.assertFalse(TxReader.imageListsPartition(addr, size, -1, -1));
            Assert.assertFalse(TxReader.imageListsPartition(addr, size, 1, -1));
            Assert.assertFalse(TxReader.imageListsPartition(addr, size, Long.MAX_VALUE - 1, 7));
            Assert.assertFalse(TxReader.imageListsPartition(addr, size, Long.MAX_VALUE, -1));
        });
    }

    @Test
    public void testNegativeBaseOffsetListed() throws Exception {
        final long size = tableEnd(0, 2);
        withImage(size, addr -> {
            putTable(addr, 0, 2);
            Assert.assertFalse(TxReader.imageListsPartition(addr, size, timestampAt(0) + 1, -1));
            putArea(addr, true, -1, 0, 2 * ENTRY_BYTES);
            Assert.assertTrue(TxReader.imageListsPartition(addr, size, timestampAt(0) + 1, -1));
        });
    }

    @Test
    public void testNegativePartitionsSizeListed() throws Exception {
        final long size = tableEnd(0, 2);
        withImage(size, addr -> {
            putTable(addr, 0, 2);
            Assert.assertFalse(TxReader.imageListsPartition(addr, size, timestampAt(0) + 1, -1));
            putArea(addr, true, TX_BASE_HEADER_SIZE, 0, -ENTRY_BYTES);
            Assert.assertTrue(TxReader.imageListsPartition(addr, size, timestampAt(0) + 1, -1));
        });
    }

    @Test
    public void testNegativeSymbolsSizeListed() throws Exception {
        final long size = tableEnd(0, 2);
        withImage(size, addr -> {
            putTable(addr, 0, 2);
            Assert.assertFalse(TxReader.imageListsPartition(addr, size, timestampAt(0) + 1, -1));
            putArea(addr, true, TX_BASE_HEADER_SIZE, -Long.BYTES, 2 * ENTRY_BYTES);
            Assert.assertTrue(TxReader.imageListsPartition(addr, size, timestampAt(0) + 1, -1));
        });
    }

    @Test
    public void testPartitionsSizeNotWholeEntriesListed() throws Exception {
        final int entryCount = 4;
        final int partitionsSize = entryCount * ENTRY_BYTES + Long.BYTES;
        final long size = tableEnd(0, 0) + partitionsSize;
        // The buffer holds one more whole entry than the stated size, so a lookup that reads the partial entry
        // past the stated partitions size stays inside the test's memory.
        withImage(tableEnd(0, entryCount + 1), addr -> {
            putTable(addr, 0, entryCount);
            Assert.assertFalse(TxReader.imageListsPartition(addr, tableEnd(0, entryCount), timestampAt(0) + 1, -1));
            putArea(addr, true, TX_BASE_HEADER_SIZE, 0, partitionsSize);
            Assert.assertTrue(TxReader.imageListsPartition(addr, size, timestampAt(0) + 1, -1));
        });
    }

    @Test
    public void testTableEndPastImageSizeListed() throws Exception {
        final long size = tableEnd(0, 2);
        withImage(size, addr -> {
            putTable(addr, 0, 2);
            Assert.assertFalse(TxReader.imageListsPartition(addr, size, timestampAt(0) + 1, -1));
            Assert.assertTrue(TxReader.imageListsPartition(addr, size - 1, timestampAt(0) + 1, -1));
        });
    }

    @Test
    public void testTxnFileOfDayPartitionedTableWithO3Insert() throws Exception {
        // Only the whole-partition rewrite gives the older day a new name txn, so pin the production default.
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        assertMemoryLeak(() -> {
            // The symbol column puts symbol counts in front of the partition table, as a real writer lays them out.
            execute("CREATE TABLE x (s SYMBOL, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            // 4 rows a day over 10 days.
            execute("INSERT INTO x SELECT 's' || (x % 3), x, '2024-01-01'::TIMESTAMP + (x - 1) * 21_600_000_000L FROM long_sequence(40)");
            // Out of order, between two rows of an older day.
            execute("INSERT INTO x VALUES ('s9', 100, '2024-01-03T01:00:00.000000Z')");

            final TableToken tableToken = engine.verifyTableName("x");
            final FilesFacade ff = configuration.getFilesFacade();
            try (Path path = new Path(); TxReader txReader = new TxReader(ff)) {
                path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME);
                txReader.ofRO(path.$(), ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY);
                Assert.assertTrue(txReader.unsafeLoadAll());
                Assert.assertEquals(10, txReader.getPartitionCount());
                Assert.assertEquals(1, txReader.getSymbolColumnCount());
                Assert.assertEquals(4, txReader.getSymbolValueCount(0));

                final long size = ff.length(path.$());
                final long fd = ff.openRO(path.$());
                Assert.assertTrue(fd > -1);
                try {
                    final long addr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
                    try {
                        Assert.assertEquals(size, ff.read(fd, addr, size, 0));
                        boolean hasRewrittenPartition = false;
                        for (int i = 0, n = txReader.getPartitionCount(); i < n; i++) {
                            final long timestamp = txReader.getPartitionTimestampByIndex(i);
                            final long nameTxn = txReader.getPartitionNameTxn(i);
                            hasRewrittenPartition |= nameTxn != -1;
                            Assert.assertTrue(TxReader.imageListsPartition(addr, size, timestamp, nameTxn));
                            Assert.assertFalse(TxReader.imageListsPartition(addr, size, timestamp, nameTxn + 1));
                            Assert.assertFalse(TxReader.imageListsPartition(addr, size, timestamp + 1, nameTxn));
                        }
                        Assert.assertTrue("the O3 insert must rewrite a partition under a new name txn", hasRewrittenPartition);
                        Assert.assertFalse(TxReader.imageListsPartition(addr, size, txReader.getPartitionTimestampByIndex(0) - 1, -1));
                    } finally {
                        Unsafe.free(addr, size, MemoryTag.NATIVE_DEFAULT);
                    }
                } finally {
                    ff.close(fd);
                }
            }
        });
    }

    private static void assertEveryEntryListedAndEveryOtherVersionMissing(int entryCount) throws Exception {
        // Two symbol counts move the partition table, so the lookup's offset math runs with symbols.
        final int symbolCount = 2;
        final long size = tableEnd(symbolCount, entryCount);
        withImage(size, addr -> {
            putTable(addr, symbolCount, entryCount);
            Assert.assertFalse(TxReader.imageListsPartition(addr, size, timestampAt(0) - 1, nameTxnAt(0)));
            for (int i = 0; i < entryCount; i++) {
                Assert.assertTrue(TxReader.imageListsPartition(addr, size, timestampAt(i), nameTxnAt(i)));
                Assert.assertFalse(TxReader.imageListsPartition(addr, size, timestampAt(i), nameTxnAt(i) + 1));
                // The gap after entry i; for the last entry, a timestamp above every entry.
                Assert.assertFalse(TxReader.imageListsPartition(addr, size, timestampAt(i) + 5, nameTxnAt(i)));
            }
        });
    }

    private static long nameTxnAt(int entryIndex) {
        return entryIndex % 3 == 0 ? -1 : entryIndex;
    }

    private static void putArea(long addr, boolean isA, long baseOffset, int symbolsSize, int partitionsSize) {
        Unsafe.getUnsafe().putInt(addr + (isA ? TX_BASE_OFFSET_A_32 : TX_BASE_OFFSET_B_32), (int) baseOffset);
        Unsafe.getUnsafe().putInt(addr + (isA ? TX_BASE_OFFSET_SYMBOLS_SIZE_A_32 : TX_BASE_OFFSET_SYMBOLS_SIZE_B_32), symbolsSize);
        Unsafe.getUnsafe().putInt(addr + (isA ? TX_BASE_OFFSET_PARTITIONS_SIZE_A_32 : TX_BASE_OFFSET_PARTITIONS_SIZE_B_32), partitionsSize);
    }

    private static void putEntry(long addr, int symbolCount, int entryIndex, long timestamp, long nameTxn) {
        final long entryAddr = addr + TX_BASE_HEADER_SIZE
                + getPartitionTableIndexOffset(getPartitionTableSizeOffset(symbolCount), entryIndex * LONGS_PER_TX_ATTACHED_PARTITION);
        Unsafe.getUnsafe().putLong(entryAddr + TS_WORD * Long.BYTES, timestamp);
        Unsafe.getUnsafe().putLong(entryAddr + NAME_TXN_WORD * Long.BYTES, nameTxn);
    }

    // Writes a version 0 image whose area A record starts right after the header and holds entryCount entries.
    private static void putTable(long addr, int symbolCount, int entryCount) {
        Unsafe.getUnsafe().putLong(addr + TX_BASE_OFFSET_VERSION_64, 0);
        putArea(addr, true, TX_BASE_HEADER_SIZE, symbolCount * Long.BYTES, entryCount * ENTRY_BYTES);
        for (int i = 0; i < entryCount; i++) {
            putEntry(addr, symbolCount, i, timestampAt(i), nameTxnAt(i));
        }
    }

    // The image size up to the end of a partition table of entryCount entries, for a record right after the header.
    private static long tableEnd(int symbolCount, int entryCount) {
        return TX_BASE_HEADER_SIZE + getPartitionTableSizeOffset(symbolCount) + Integer.BYTES + (long) entryCount * ENTRY_BYTES;
    }

    private static long timestampAt(int entryIndex) {
        return 1_000 + entryIndex * 10L;
    }

    private static void withImage(long allocSize, LongConsumer code) throws Exception {
        assertMemoryLeak(() -> {
            final long addr = Unsafe.calloc(allocSize, MemoryTag.NATIVE_DEFAULT);
            try {
                code.accept(addr);
            } finally {
                Unsafe.free(addr, allocSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }
}
