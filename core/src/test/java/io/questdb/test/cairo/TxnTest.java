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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.SymbolCountProvider;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.TxWriter;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.TableUtils.LONGS_PER_TX_ATTACHED_PARTITION;
import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;

public class TxnTest extends AbstractCairoTest {
    private static final Log LOG = LogFactory.getLog(TxnTest.class);

    // Representative geometry for the helper unit tests: 2 symbols + 1 partition.
    // symbolBytes = 2*8 = 16; partitionBytes = 1*4*8 = 32; partitionTableStart = 132 + 16 = 148;
    // recordSize = 132 + 16 + 4 + 32 = 184. Covered: [0,80) U [148,184). Excluded: [80,148).
    private static final int BC_PARTITION_BYTES = 1 * LONGS_PER_TX_ATTACHED_PARTITION * Long.BYTES;
    private static final int BC_SYMBOL_BYTES = 2 * Long.BYTES;
    private static final long BC_PARTITION_TABLE_START = TableUtils.getPartitionTableSizeOffset(BC_SYMBOL_BYTES / Long.BYTES);
    private static final long BC_RECORD_SIZE = TableUtils.calculateTxRecordSize(BC_SYMBOL_BYTES, BC_PARTITION_BYTES);

    @Test
    public void testBodyChecksumChangesWhenCoveredByteChanges() {
        long addr = Unsafe.malloc(BC_RECORD_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            fillPattern(addr, BC_RECORD_SIZE, 0x1234);
            long base = bodyChecksum(addr);

            // Every byte in the covered union [0,80) U [partitionTableStart, recordSize) must change the result.
            for (long off = 0; off < BC_RECORD_SIZE; off++) {
                boolean covered = (off < TableUtils.TX_OFFSET_SEQ_TXN_64) || (off >= BC_PARTITION_TABLE_START);
                if (!covered) {
                    continue;
                }
                byte orig = Unsafe.getByte(addr + off);
                Unsafe.putByte(addr + off, (byte) (orig ^ 0x5a));
                Assert.assertNotEquals("checksum did not change for covered byte offset " + off, base, bodyChecksum(addr));
                Unsafe.putByte(addr + off, orig);
            }
            // Determinism after restore.
            Assert.assertEquals(base, bodyChecksum(addr));
        } finally {
            Unsafe.free(addr, BC_RECORD_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testBodyChecksumIsDeterministic() {
        long addr = Unsafe.malloc(BC_RECORD_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            fillPattern(addr, BC_RECORD_SIZE, 0xabcd);
            Assert.assertEquals(bodyChecksum(addr), bodyChecksum(addr));
            Assert.assertNotEquals(0L, bodyChecksum(addr));
        } finally {
            Unsafe.free(addr, BC_RECORD_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testBodyChecksumNeverReturnsZero() {
        long addr = Unsafe.malloc(BC_RECORD_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int seed = 0; seed < 100_000; seed++) {
                fillPattern(addr, BC_RECORD_SIZE, seed);
                Assert.assertNotEquals("checksum returned 0 for seed " + seed, 0L, bodyChecksum(addr));
            }
            // The all-zero body (avalanche of 0) must still be remapped away from the 0 = "absent" sentinel.
            Vect.memset(addr, BC_RECORD_SIZE, 0);
            Assert.assertNotEquals(0L, bodyChecksum(addr));
        } finally {
            Unsafe.free(addr, BC_RECORD_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testBodyChecksumUnchangedWhenExcludedRegionChanges() {
        long addr = Unsafe.malloc(BC_RECORD_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            fillPattern(addr, BC_RECORD_SIZE, 0x9999);
            long base = bodyChecksum(addr);

            // 1) The seqTxn + lag region [80,116) is excluded: scribbling it must NOT change the checksum.
            for (long off = TableUtils.TX_OFFSET_SEQ_TXN_64; off < TableUtils.TX_OFFSET_BODY_CHECKSUM_64; off++) {
                Unsafe.putByte(addr + off, (byte) (Unsafe.getByte(addr + off) ^ 0x37));
            }
            Assert.assertEquals("lag region must be excluded", base, bodyChecksum(addr));

            // 2) The checksum slot + reserved gap [116,128) is excluded.
            Unsafe.putLong(addr + TableUtils.TX_OFFSET_BODY_CHECKSUM_64, 0xdeadbeefcafef00dL);
            Unsafe.putInt(addr + TableUtils.TX_OFFSET_BODY_CHECKSUM_64 + Long.BYTES, 0x7fffffff);
            Assert.assertEquals("checksum slot + gap must be excluded", base, bodyChecksum(addr));

            // 3) The symbol-count region [128, partitionTableStart) is excluded - this is the race-critical
            //    region mutated in place by writeTransientSymbolCount.
            for (long off = TableUtils.TX_OFFSET_MAP_WRITER_COUNT_32; off < BC_PARTITION_TABLE_START; off++) {
                Unsafe.putByte(addr + off, (byte) (Unsafe.getByte(addr + off) ^ 0xa1));
            }
            Assert.assertEquals("symbol-count region must be excluded", base, bodyChecksum(addr));
        } finally {
            Unsafe.free(addr, BC_RECORD_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testBodyChecksumChangesWhenPartitionRecordChanges() {
        long addr = Unsafe.malloc(BC_RECORD_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            fillPattern(addr, BC_RECORD_SIZE, 0x55aa);
            long base = bodyChecksum(addr);
            // Flip a byte inside the single partition record (which starts at partitionTableStart + 4).
            long partitionRecordByte = BC_PARTITION_TABLE_START + Integer.BYTES + 3;
            Unsafe.putByte(addr + partitionRecordByte, (byte) (Unsafe.getByte(addr + partitionRecordByte) ^ 0x5a));
            Assert.assertNotEquals("partition record change must be detected", base, bodyChecksum(addr));
        } finally {
            Unsafe.free(addr, BC_RECORD_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testNoFalsePositiveUnderConcurrentSymbolWrites() throws Throwable {
        // THE KEY REGRESSION. A writer mutates the live committed record IN PLACE - bumping transient
        // symbol counts (collectValueCount -> writeTransientSymbolCount) and resetting lag - WITHOUT a
        // version bump, while reader threads loop unsafeLoadAll/safeReadTxn thousands of times. Because the
        // body checksum covers only commit-immutable bytes, NOT the symbol-count or lag regions, this must
        // produce ZERO checksum-mismatch throws and ZERO A/B fallbacks. If the checksum covered those
        // regions, every in-place mutation would stale it and the reader would falsely fall back / throw.
        TestUtils.assertMemoryLeak(() -> {
            final String tableName = "noFalsePositive";
            final FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            final int symbolColumnCount = 8;
            final int partitionCount = 4;
            final int targetReads = 5_000;

            TableModel model = new TableModel(configuration, tableName, PartitionBy.HOUR);
            model.timestamp();
            AbstractCairoTest.create(model);
            final int timestampType = TableUtils.getTimestampType(model);

            ObjList<SymbolCountProvider> symbolCounts = new ObjList<>();
            for (int i = 0; i < symbolColumnCount; i++) {
                symbolCounts.add(new SymbolCountProviderImpl(1));
            }

            TxReader.resetBodyChecksumFallbackCount();

            final CyclicBarrier start = new CyclicBarrier(2);
            final AtomicInteger readsDone = new AtomicInteger();
            final AtomicInteger readerFinished = new AtomicInteger();
            final ConcurrentLinkedQueue<Throwable> exceptions = new ConcurrentLinkedQueue<>();

            // Writer thread: lay down ONE stable committed record (symbols + partitions, fresh version +
            // checksum), then continuously mutate IN PLACE the regions that DON'T bump the version - the
            // transient symbol counts (collectValueCount -> writeTransientSymbolCount) and the lag fields
            // (resetLagAppliedRows). It keeps going until the reader has done enough loads.
            Thread writer = new Thread(() -> {
                try (
                        Path path = new Path();
                        TxWriter txWriter = new TxWriter(ff, configuration)
                ) {
                    TableToken tableToken = engine.verifyTableName(tableName);
                    path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                    txWriter.ofRW(path.$(), timestampType, PartitionBy.HOUR);

                    txWriter.bumpMetadataAndColumnStructureVersion(symbolCounts);
                    for (int i = 0; i < partitionCount; i++) {
                        txWriter.updatePartitionSizeByTimestamp(i * Micros.HOUR_MICROS, 10L + i);
                    }
                    txWriter.setMaxTimestamp((partitionCount - 1) * Micros.HOUR_MICROS);
                    txWriter.commit(symbolCounts);

                    start.await();
                    int r = 0;
                    while (readerFinished.get() == 0 && exceptions.isEmpty()) {
                        int sym = r % symbolColumnCount;
                        // In-place transient symbol-count bump (excluded region). No version bump.
                        txWriter.collectValueCount(sym, (r % 100) + 1);
                        // Periodically reset lag in place (also an excluded region, no version bump).
                        if ((r & 0xff) == 0) {
                            txWriter.resetLagAppliedRows();
                        }
                        r++;
                    }
                } catch (Throwable e) {
                    exceptions.add(e);
                    LOG.error().$(e).$();
                }
            });

            // Reader thread: call unsafeLoadAll() directly (NOT safeReadTxn, whose fast path would skip the
            // load when the version is stable) so the body checksum is verified on EVERY iteration.
            Thread reader = new Thread(() -> {
                try (
                        Path path = new Path();
                        TxReader txReader = new TxReader(ff)
                ) {
                    TableToken tableToken = engine.verifyTableName(tableName);
                    path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                    txReader.ofRO(path.$(), timestampType, PartitionBy.HOUR);
                    start.await();
                    while (readsDone.get() < targetReads && exceptions.isEmpty()) {
                        // A spurious checksum mismatch on this healthy table would throw CairoException here.
                        if (txReader.unsafeLoadAll()) {
                            // The committed structure never changes during the in-place phase: validate it.
                            Assert.assertEquals(partitionCount, txReader.getPartitionCount());
                            Assert.assertEquals(symbolColumnCount, txReader.getSymbolColumnCount());
                            readsDone.incrementAndGet();
                        }
                    }
                } catch (Throwable e) {
                    exceptions.add(e);
                    LOG.error().$(e).$();
                } finally {
                    readerFinished.incrementAndGet();
                }
            });

            writer.start();
            reader.start();
            reader.join();
            writer.join();

            if (!exceptions.isEmpty()) {
                Assert.fail(exceptions.poll().toString());
            }
            Assert.assertTrue("reader did not loop enough: " + readsDone.get(), readsDone.get() >= targetReads);
            Assert.assertEquals(
                    "in-place symbol/lag writes triggered spurious A/B fallbacks",
                    0L,
                    TxReader.getBodyChecksumFallbackCount()
            );
            LOG.infoW().$("concurrent-symbol-write successful reads ").$(readsDone.get()).$();
        });
    }

    @Test
    public void testOpenOldFormatTxn_noBodyChecksum() throws Exception {
        // A record whose checksum slot [116,124) is 0 (old format / pre-feature, or freshly created) must
        // load with the verify SKIPPED (back-compatible, no false rejection, no fallback).
        TestUtils.assertMemoryLeak(() -> {
            final String tableName = "oldFormatTxn";
            final FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            TableModel model = new TableModel(configuration, tableName, PartitionBy.HOUR);
            model.timestamp();
            AbstractCairoTest.create(model);
            final int timestampType = TableUtils.getTimestampType(model);

            ObjList<SymbolCountProvider> symbolCounts = new ObjList<>();
            long currentBaseOffset;
            long expectedTxn;
            try (Path path = new Path(); TxWriter txWriter = new TxWriter(ff, configuration)) {
                TableToken tableToken = engine.verifyTableName(tableName);
                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                txWriter.ofRW(path.$(), timestampType, PartitionBy.HOUR);
                // Two partitions so partition 0 is not the "last" one (whose size is the transient row count).
                txWriter.updatePartitionSizeByTimestamp(0, 42);
                txWriter.updatePartitionSizeByTimestamp(Micros.HOUR_MICROS, 43);
                txWriter.setMaxTimestamp(Micros.HOUR_MICROS);
                txWriter.commit(symbolCounts);
                currentBaseOffset = txWriter.getBaseOffset();
                expectedTxn = txWriter.getTxn();
            }

            // Simulate an "old format" record by zeroing the checksum slot of the current area.
            try (Path path = new Path()) {
                TableToken tableToken = engine.verifyTableName(tableName);
                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                pokeLong(ff, path.$(), currentBaseOffset + TableUtils.TX_OFFSET_BODY_CHECKSUM_64, 0L);
            }

            TxReader.resetBodyChecksumFallbackCount();
            try (Path path = new Path(); TxReader txReader = new TxReader(ff)) {
                TableToken tableToken = engine.verifyTableName(tableName);
                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                txReader.ofRO(path.$(), timestampType, PartitionBy.HOUR);
                Assert.assertTrue(txReader.unsafeLoadAll());
                Assert.assertEquals("absent checksum must load without rejection", expectedTxn, txReader.getTxn());
                Assert.assertEquals(42, txReader.getPartitionSize(0));
                Assert.assertEquals("absent checksum must not trigger fallback", 0L, TxReader.getBodyChecksumFallbackCount());
            }
        });
    }

    @Test
    public void testTornTxnBodyBothAreasCorrupt() throws Exception {
        // Corrupt the covered region of BOTH A and B (a [0,80) scalar) without fixing either checksum.
        // The reader must surface a hard CairoException - never a silently wrong value.
        TestUtils.assertMemoryLeak(() -> {
            final String tableName = "tornBothCorrupt";
            final FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            final int timestampType = setupTwoCommitTable(tableName, ff, 100L, 200L);

            try (Path path = new Path()) {
                TableToken tableToken = engine.verifyTableName(tableName);
                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                long aOffset = peekInt(ff, path.$(), TableUtils.TX_BASE_OFFSET_A_32);
                long bOffset = peekInt(ff, path.$(), TableUtils.TX_BASE_OFFSET_B_32);
                // Corrupt fixedRowCount (a covered [0,80) scalar) in BOTH areas, leaving both checksums stale.
                pokeLong(ff, path.$(), aOffset + TableUtils.TX_OFFSET_FIXED_ROW_COUNT_64, 0x1111_1111L);
                pokeLong(ff, path.$(), bOffset + TableUtils.TX_OFFSET_FIXED_ROW_COUNT_64, 0x2222_2222L);
            }

            TxReader.resetBodyChecksumFallbackCount();
            try (Path path = new Path(); TxReader txReader = new TxReader(ff)) {
                TableToken tableToken = engine.verifyTableName(tableName);
                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                txReader.ofRO(path.$(), timestampType, PartitionBy.HOUR);
                try {
                    txReader.unsafeLoadAll();
                    Assert.fail("expected CairoException - both areas corrupt");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "_txn body checksum mismatch in both A and B areas");
                }
                Assert.assertEquals(1L, TxReader.getBodyChecksumFallbackCount());
            }
        });
    }

    @Test
    public void testTornTxnBodyDetectedAndRecovered() throws Exception {
        // Two commits => A and B both hold valid, checksummed records (fixedRowCount 100 then 200).
        // Corrupt ONLY the version-selected area's fixedRowCount, leaving its checksum stale. The reader
        // must detect the mismatch, fall back to the intact other area, and return the PRIOR correct state.
        TestUtils.assertMemoryLeak(() -> {
            final String tableName = "tornRecovered";
            final FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            final int timestampType = setupTwoCommitTable(tableName, ff, 100L, 200L);

            long selectedBaseOffset;
            try (Path path = new Path(); TxReader txReader = new TxReader(ff)) {
                TableToken tableToken = engine.verifyTableName(tableName);
                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                txReader.ofRO(path.$(), timestampType, PartitionBy.HOUR);
                Assert.assertTrue(txReader.unsafeLoadAll());
                Assert.assertEquals("latest committed fixedRowCount", 200L, txReader.getFixedRowCount());
                selectedBaseOffset = txReader.getBaseOffset();
            }

            // Corrupt the selected (latest) area's fixedRowCount without fixing its checksum.
            try (Path path = new Path()) {
                TableToken tableToken = engine.verifyTableName(tableName);
                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                pokeLong(ff, path.$(), selectedBaseOffset + TableUtils.TX_OFFSET_FIXED_ROW_COUNT_64, 0xdead_beefL);
            }

            TxReader.resetBodyChecksumFallbackCount();
            try (Path path = new Path(); TxReader txReader = new TxReader(ff)) {
                TableToken tableToken = engine.verifyTableName(tableName);
                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                txReader.ofRO(path.$(), timestampType, PartitionBy.HOUR);
                Assert.assertTrue(txReader.unsafeLoadAll());
                // Fell back to the OTHER (intact, prior) area: returns the 100 state, NOT the corrupted value.
                Assert.assertEquals("must recover prior correct state via A/B fallback", 100L, txReader.getFixedRowCount());
                Assert.assertEquals("exactly one fallback expected", 1L, TxReader.getBodyChecksumFallbackCount());
            }
        });
    }

    @Test
    public void testFailedTxWriterDoesNotCorruptTable() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            FilesFacade errorFf = new TestFilesFacadeImpl() {
                @Override
                public long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
                    return -1;
                }
            };

            FilesFacadeImpl cleanFf = new TestFilesFacadeImpl();
            assertMemoryLeak(() -> {
                String tableName = "txntest";
                TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY);
                model.timestamp();
                AbstractCairoTest.create(model);

                try (Path path = new Path()) {
                    TableToken tableToken = engine.verifyTableName(tableName);
                    path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                    int testPartitionCount = 3000;
                    try (TxWriter txWriter = new TxWriter(cleanFf, configuration).ofRW(path.$(), TableUtils.getTimestampType(model), PartitionBy.DAY)) {
                        // Add lots of partitions
                        for (int i = 0; i < testPartitionCount; i++) {
                            txWriter.updatePartitionSizeByTimestamp(i * Micros.DAY_MICROS, i + 1);
                        }
                        txWriter.updateMaxTimestamp(testPartitionCount * Micros.DAY_MICROS + 1);
                        txWriter.finishPartitionSizeUpdate();
                        txWriter.commit(new ObjList<>());
                    }

                    // Reopen without OS errors
                    try (TxWriter txWriter = new TxWriter(cleanFf, configuration).ofRW(path.$(), TableUtils.getTimestampType(model), PartitionBy.DAY)) {
                        // Read lots of partitions
                        Assert.assertEquals(testPartitionCount, txWriter.getPartitionCount());
                        for (int i = 0; i < testPartitionCount - 1; i++) {
                            Assert.assertEquals(i + 1, txWriter.getPartitionSize(i));
                        }
                    }

                    // Open with OS error to file extend
                    try (TxWriter ignored = new TxWriter(errorFf, configuration).ofRW(path.$(), TableUtils.getTimestampType(model), PartitionBy.DAY)) {
                        Assert.fail("Should not be able to extend on opening");
                    } catch (CairoException ex) {
                        // expected
                    }

                    // Reopen without OS errors
                    try (TxWriter txWriter = new TxWriter(cleanFf, configuration).ofRW(path.$(), TableUtils.getTimestampType(model), PartitionBy.DAY)) {
                        // Read lots of partitions
                        Assert.assertEquals(testPartitionCount, txWriter.getPartitionCount());
                        for (int i = 0; i < testPartitionCount - 1; i++) {
                            Assert.assertEquals(i + 1, txWriter.getPartitionSize(i));
                        }
                    }
                }
            });
        });
    }

    @Test
    public void testLoadAllFrom() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            assertMemoryLeak(() -> {
                String tableName = "txntest";
                TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY);
                model.timestamp();
                AbstractCairoTest.create(model);

                try (Path path = new Path()) {
                    TableToken tableToken = engine.verifyTableName(tableName);
                    path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                    int testPartitionCount = 2;
                    try (TxWriter txWriter = new TxWriter(ff, configuration)) {
                        txWriter.ofRW(path.$(), TableUtils.getTimestampType(model), PartitionBy.DAY);
                        for (int i = 0; i < testPartitionCount; i++) {
                            txWriter.updatePartitionSizeByTimestamp(i * Micros.DAY_MICROS, i + 1);
                        }
                        txWriter.updateMaxTimestamp(testPartitionCount * Micros.DAY_MICROS + 1);
                        txWriter.finishPartitionSizeUpdate();
                        txWriter.commit(new ObjList<>());
                    }

                    try (
                            TxReader txReader = new TxReader(ff);
                            MemoryCARW dumpMem = Vm.getCARWInstance(ff.getPageSize(), Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                            TxReader txCopyReader = new TxReader(ff);
                            MemoryCARW dumpCopyMem = Vm.getCARWInstance(ff.getPageSize(), Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)
                    ) {
                        txReader.ofRO(path.$(), TableUtils.getTimestampType(model), PartitionBy.DAY);

                        txReader.unsafeLoadAll();
                        final String expected = """
                                {txn: 1, attachedPartitions: [
                                {ts: '1970-01-01T00:00:00.000000Z', rowCount: 1, nameTxn: -1},
                                {ts: '1970-01-02T00:00:00.000000Z', rowCount: 2, nameTxn: -1}
                                ], transientRowCount: 2, fixedRowCount: 1, minTimestamp: '294247-01-10T04:00:54.775807Z', maxTimestamp: '1970-01-03T00:00:00.000001Z', dataVersion: 0, structureVersion: 0, partitionTableVersion: 0, columnVersion: 0, truncateVersion: 0, seqTxn: 0, symbolColumnCount: 0, lagRowCount: 0, lagMinTimestamp: '294247-01-10T04:00:54.775807Z', lagMaxTimestamp: '', lagTxnCount: 0, lagOrdered: true}""";
                        Assert.assertEquals(expected, txReader.toString());

                        txCopyReader.loadAllFrom(txReader);
                        Assert.assertEquals(expected, txCopyReader.toString());

                        Assert.assertTrue(txReader.getRecordSize() > 0);
                        Assert.assertEquals(txReader.getRecordSize(), txCopyReader.getRecordSize());

                        // Make sure to zero the memory before the dump to avoid garbage bytes in paddings.
                        dumpMem.jumpTo(txReader.getRecordSize());
                        dumpMem.zero();
                        dumpCopyMem.jumpTo(txCopyReader.getRecordSize());
                        dumpCopyMem.zero();

                        txReader.dumpTo(dumpMem);
                        txCopyReader.dumpTo(dumpCopyMem);
                        Assert.assertTrue(Vect.memeq(dumpMem.addressOf(0), dumpCopyMem.addressOf(0), txReader.getRecordSize()));
                    }
                }
            });
        });
    }

    @Test
    public void testLoadTxn() throws IOException {
        try (Path p = new Path()) {
            final String incrementalLoad;
            try (TxWriter tw = new TxWriter(engine.getConfiguration().getFilesFacade(), engine.getConfiguration())) {
                loadTxnWriter(tw, p, "/txn/sys.acl_entities~1/_txn");
                loadTxnWriter(tw, p, "/txn/sys.acl_passwords~5/_txn");
                incrementalLoad = tw.toString();
            }

            try (TxWriter tw = new TxWriter(engine.getConfiguration().getFilesFacade(), engine.getConfiguration())) {
                loadTxnWriter(tw, p, "/txn/sys.acl_passwords~5/_txn");
                TestUtils.assertEquals(incrementalLoad, tw.toString());
            }
        }
    }

    @Test
    public void testSquashCounterOverflow() throws IOException {
        try (Path p = new Path()) {
            try (TxWriter tw = new TxWriter(engine.getConfiguration().getFilesFacade(), engine.getConfiguration())) {
                loadTxnWriter(tw, p, "/txn/sys.acl_entities~1/_txn");
                //noinspection StatementWithEmptyBody
                while (tw.incrementPartitionSquashCounter(0)) {
                }

                Assert.assertEquals(TxReader.PARTITION_SQUASH_COUNTER_MAX, tw.getPartitionSquashCount(0));

                Assert.assertFalse(tw.incrementPartitionSquashCounter(0));
                Assert.assertEquals(TxReader.PARTITION_SQUASH_COUNTER_MAX, tw.getPartitionSquashCount(0));
            }
        }
    }

    @Test
    public void testToString() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            assertMemoryLeak(() -> {

                String tableName = "txntest";
                TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY);
                model.timestamp();
                AbstractCairoTest.create(model);

                try (Path path = new Path()) {
                    TableToken tableToken = engine.verifyTableName(tableName);
                    path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                    int testPartitionCount = 2;
                    try (TxWriter txWriter = new TxWriter(ff, configuration)) {
                        txWriter.ofRW(path.$(), TableUtils.getTimestampType(model), PartitionBy.DAY);
                        for (int i = 0; i < testPartitionCount; i++) {
                            txWriter.updatePartitionSizeByTimestamp(i * Micros.DAY_MICROS, i + 1);
                        }
                        TestUtils.assertContains(txWriter.toString(), """
                                [
                                {ts: '1970-01-01T00:00:00.000000Z', rowCount: 1, nameTxn: -1},
                                {ts: '1970-01-02T00:00:00.000000Z', rowCount: 2, nameTxn: -1}
                                ]""");
                    }
                }
            });
        });
    }

    @Test
    public void testTxReadTruncateConcurrent() throws Throwable {
        TestUtils.assertMemoryLeak(() -> {
            int readerThreads = 2;

            CyclicBarrier start = new CyclicBarrier(readerThreads + 1);
            AtomicInteger done = new AtomicInteger();
            AtomicInteger reloadCount = new AtomicInteger();
            int iterations = 1000;
            ConcurrentLinkedQueue<Throwable> exceptions = new ConcurrentLinkedQueue<>();
            Rnd rnd = TestUtils.generateRandom(LOG);

            String tableName = "testTxReadWriteConcurrent";
            FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            int maxPartitionCount = Math.max((int) (Files.PAGE_SIZE / 8 / 4), 4096);
            int maxSymbolCount = (int) (Files.PAGE_SIZE / 8 / 4);
            AtomicInteger partitionCountCheck = new AtomicInteger();

            TableModel model = new TableModel(configuration, tableName, PartitionBy.HOUR);
            model.timestamp();
            AbstractCairoTest.create(model);
            int truncateIteration = 33;
            Thread writerThread = createWriterThread(
                    start,
                    done,
                    iterations,
                    exceptions,
                    rnd,
                    tableName,
                    ff,
                    maxPartitionCount,
                    maxSymbolCount,
                    partitionCountCheck,
                    truncateIteration,
                    TableUtils.getTimestampType(model)
            );

            Rnd readerRnd = new Rnd(rnd.nextLong(), rnd.nextLong());
            Thread[] readers = new Thread[readerThreads];
            for (int th = 0; th < readerThreads; th++) {
                Thread readerThread = new Thread(() -> {
                    try (
                            Path path = new Path();
                            TxReader txReader = new TxReader(ff)
                    ) {
                        TableToken tableToken = engine.verifyTableName(tableName);
                        path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                        txReader.ofRO(path.$(), TableUtils.getTimestampType(model), PartitionBy.HOUR);
                        MillisecondClock clock = engine.getConfiguration().getMillisecondClock();
                        long duration = 5_000;
                        start.await();
                        while (done.get() == 0 || partitionCountCheck.get() != txReader.getPartitionCount() - 1) {
                            TableUtils.safeReadTxn(txReader, clock, duration);
                            long txn = txReader.getTxn();

                            // Each writer iteration creates 2 txn commits.
                            // Every truncateIteration writer iteration truncates.
                            if (((txn - 1) / 2) % truncateIteration == 0) {
                                // must be truncated
                                if (txReader.getPartitionCount() > 1) {
                                    Assert.assertTrue(
                                            "Txn " + txn + " not read as truncated. Partition count: " + txReader.getPartitionCount(),
                                            txReader.getPartitionCount() < 2);
                                }
                            } else if (txReader.getPartitionCount() > 2) {
                                reloadCount.incrementAndGet();
                            }
                            if (readerRnd.nextBoolean()) {
                                txReader.ofRO(path.$(), TableUtils.getTimestampType(model), PartitionBy.HOUR);
                            }
                            Os.pause();
                        }

                    } catch (Throwable e) {
                        exceptions.add(e);
                        LOG.error().$(e).$();
                    }
                });
                readers[th] = readerThread;
                readerThread.start();
            }

            writerThread.start();

            writerThread.join();
            for (int th = 0; th < readerThreads; th++) {
                readers[th].join();
            }

            if (!exceptions.isEmpty()) {
                Assert.fail(exceptions.poll().toString());
            }
            Assert.assertTrue(reloadCount.get() > 10);
            LOG.infoW().$("total reload count ").$(reloadCount.get()).$();
        });
    }

    @Test
    public void testTxReadWriteConcurrent() throws Throwable {
        TestUtils.assertMemoryLeak(() -> {
            int readerThreads = 4;

            CyclicBarrier start = new CyclicBarrier(readerThreads + 1);
            AtomicInteger done = new AtomicInteger();
            AtomicInteger reloadCount = new AtomicInteger();
            int iterations = 1000;
            ConcurrentLinkedQueue<Throwable> exceptions = new ConcurrentLinkedQueue<>();
            Rnd rnd = TestUtils.generateRandom(LOG);

            String tableName = "testTxReadWriteConcurrent";
            FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            int maxPartitionCount = Math.max((int) (Files.PAGE_SIZE / 8 / 4), 4096);
            int maxSymbolCount = (int) (Files.PAGE_SIZE / 8 / 4);
            AtomicInteger partitionCountCheck = new AtomicInteger();

            TableModel model = new TableModel(configuration, tableName, PartitionBy.HOUR);
            model.timestamp();
            AbstractCairoTest.create(model);
            Thread writerThread = createWriterThread(
                    start,
                    done,
                    iterations,
                    exceptions,
                    rnd,
                    tableName,
                    ff,
                    maxPartitionCount,
                    maxSymbolCount,
                    partitionCountCheck,
                    Integer.MAX_VALUE,
                    TableUtils.getTimestampType(model)
            );

            Rnd readerRnd = TestUtils.generateRandom(LOG);

            Thread[] readers = new Thread[readerThreads];
            for (int th = 0; th < readerThreads; th++) {
                Thread readerThread = new Thread(() -> {
                    try (
                            Path path = new Path();
                            TxReader txReader = new TxReader(ff)
                    ) {
                        TableToken tableToken = engine.verifyTableName(tableName);
                        path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                        txReader.ofRO(path.$(), TableUtils.getTimestampType(model), PartitionBy.HOUR);
                        MillisecondClock clock = engine.getConfiguration().getMillisecondClock();
                        long duration = 30_000;
                        start.await();
                        while (done.get() == 0) {
                            TableUtils.safeReadTxn(txReader, clock, duration);
                            reloadCount.incrementAndGet();
                            Assert.assertTrue(txReader.getPartitionCount() <= maxPartitionCount);
                            Assert.assertTrue(txReader.getSymbolColumnCount() <= maxSymbolCount);

                            for (int i = txReader.getSymbolColumnCount() - 1; i > -1; i--) {
                                if (i != txReader.getSymbolColumnCount()) {
                                    String trace = String.format(
                                            "[txn=%d, structureVersion=%d, partitionCount=%d, symbolCount=%d] ",
                                            txReader.getTxn(),
                                            txReader.getMetadataVersion(),
                                            txReader.getPartitionCount(),
                                            txReader.getSymbolColumnCount()
                                    );
                                    Assert.assertEquals(trace, i, txReader.getSymbolValueCount(i));
                                }
                            }

                            long offset = txReader.getTxn() - txReader.getMetadataVersion();
                            for (int i = txReader.getPartitionCount() - 2; i > -1; i--) {
                                if (offset + i != txReader.getPartitionSize(i)) {
                                    String trace = String.format(
                                            "[txn=%d, structureVersion=%d, partitionCount=%d, symbolCount=%d] ",
                                            txReader.getTxn(),
                                            txReader.getMetadataVersion(),
                                            txReader.getPartitionCount(),
                                            txReader.getSymbolColumnCount()
                                    );
                                    Assert.assertEquals(trace + buildActualSizes(txReader), offset + i, txReader.getPartitionSize(i));
                                }
                            }

                            if (readerRnd.nextBoolean()) {
                                // Reopen txn file
                                txReader.ofRO(path.$(), TableUtils.getTimestampType(model), PartitionBy.HOUR);
                            }
                        }
                        TableUtils.safeReadTxn(txReader, clock, duration);
                        Assert.assertEquals(partitionCountCheck.get(), txReader.getPartitionCount() - 1);

                    } catch (Throwable e) {
                        LOG.error().$(e).$();
                        exceptions.add(e);
                    }
                });
                readers[th] = readerThread;
                readerThread.start();
            }

            writerThread.start();

            writerThread.join();
            for (int th = 0; th < readerThreads; th++) {
                readers[th].join();
            }

            if (!exceptions.isEmpty()) {
                Assert.fail(exceptions.poll().toString());
            }
            Assert.assertTrue(reloadCount.get() > 10);
            LOG.infoW().$("total reload count ").$(reloadCount.get()).$();
        });
    }

    private static long bodyChecksum(long addr) {
        return TableUtils.calculateTxnBodyChecksum(addr, BC_RECORD_SIZE, BC_PARTITION_TABLE_START);
    }

    // Positional 8-byte read of the _txn file (no mmap, so it cannot truncate the file on close).
    private static long peekLong(FilesFacade ff, LPSZ path, long offset) {
        long fd = ff.openRO(path);
        Assert.assertTrue(fd > -1);
        long buf = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            Assert.assertEquals(Long.BYTES, ff.read(fd, buf, Long.BYTES, offset));
            return Unsafe.getLong(buf);
        } finally {
            Unsafe.free(buf, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            ff.close(fd);
        }
    }

    private static int peekInt(FilesFacade ff, LPSZ path, long offset) {
        return (int) (peekLong(ff, path, offset) & 0xffffffffL);
    }

    // Positional 8-byte write of the _txn file. Used to corrupt a committed record on disk WITHOUT
    // recomputing its body checksum and WITHOUT truncating the file (a writable mmap would truncate).
    private static void pokeLong(FilesFacade ff, LPSZ path, long offset, long value) {
        long fd = ff.openRW(path, CairoConfiguration.O_NONE);
        Assert.assertTrue(fd > -1);
        long buf = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putLong(buf, value);
            Assert.assertEquals(Long.BYTES, ff.write(fd, buf, Long.BYTES, offset));
            ff.fsync(fd);
        } finally {
            Unsafe.free(buf, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            ff.close(fd);
        }
    }

    // Creates a partitioned table and commits twice (fixedRowCount fixed1 then fixed2), so the A and B
    // areas each hold a valid, body-checksummed record. Returns the timestamp type.
    private int setupTwoCommitTable(String tableName, FilesFacade ff, long fixed1, long fixed2) {
        TableModel model = new TableModel(configuration, tableName, PartitionBy.HOUR);
        model.timestamp();
        AbstractCairoTest.create(model);
        final int timestampType = TableUtils.getTimestampType(model);
        ObjList<SymbolCountProvider> symbolCounts = new ObjList<>();
        try (Path path = new Path(); TxWriter txWriter = new TxWriter(ff, configuration)) {
            TableToken tableToken = engine.verifyTableName(tableName);
            path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
            txWriter.ofRW(path.$(), timestampType, PartitionBy.HOUR);
            // Lay down two partitions so the partition table is non-empty and covered.
            txWriter.updatePartitionSizeByTimestamp(0, 10);
            txWriter.updatePartitionSizeByTimestamp(Micros.HOUR_MICROS, 11);
            txWriter.setMaxTimestamp(Micros.HOUR_MICROS);
            txWriter.reset(fixed1, txWriter.getTransientRowCount(), txWriter.getMaxTimestamp(), symbolCounts);
            // Second commit: change fixedRowCount, flipping to the other A/B area.
            txWriter.reset(fixed2, txWriter.getTransientRowCount(), txWriter.getMaxTimestamp(), symbolCounts);
        }
        return timestampType;
    }

    private static void fillPattern(long addr, long size, int seed) {
        // Deterministic, seed-dependent pseudo-random byte fill (xorshift), so every byte position
        // carries entropy and distinct seeds produce distinct content.
        int x = seed | 1; // avoid the zero-stuck xorshift state
        for (long i = 0; i < size; i++) {
            x ^= x << 13;
            x ^= x >>> 17;
            x ^= x << 5;
            Unsafe.putByte(addr + i, (byte) x);
        }
    }

    private static void loadTxnWriter(TxWriter tw, Path p, String resourceFile) throws IOException {
        try (final InputStream is = TxnTest.class.getResourceAsStream(resourceFile)) {
            // Create temp file
            java.nio.file.Path tempFile = java.nio.file.Files.createTempFile("test-", ".tmp");
            tempFile.toFile().deleteOnExit(); // Ensure it is deleted on exit

            // Copy resource content to temp file
            java.nio.file.Files.copy(Objects.requireNonNull(is), tempFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING);

            p.of(tempFile.toString()).$();
            tw.ofRW(p.$(), ColumnType.TIMESTAMP, PartitionBy.MONTH);
            tw.unsafeLoadAll();
        }
    }

    private String buildActualSizes(TxReader txReader) {
        StringSink ss = new StringSink();
        for (int i = 0; i < txReader.getPartitionCount() - 1; i++) {
            if (i > 0) {
                ss.put(',');
            }
            ss.put(txReader.getPartitionSize(i));
        }
        return ss.toString();
    }

    @NotNull
    private Thread createWriterThread(
            CyclicBarrier start,
            AtomicInteger done,
            int iterations,
            ConcurrentLinkedQueue<Throwable> exceptions,
            Rnd rnd,
            String tableName,
            FilesFacade ff,
            int maxPartitionCount,
            int maxSymbolCount,
            AtomicInteger partitionCountCheck,
            int truncateIteration,
            int timestampType
    ) {
        ObjList<SymbolCountProvider> symbolCounts = new ObjList<>();
        ObjList<SymbolCountProvider> zeroSymbolCounts = new ObjList<>();
        return new Thread(() -> {
            try (
                    Path path = new Path();
                    TxWriter txWriter = new TxWriter(ff, configuration)
            ) {
                TableToken tableToken = engine.verifyTableName(tableName);
                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                txWriter.ofRW(path.$(), timestampType, PartitionBy.HOUR);

                start.await();
                for (int j = 0; j < iterations; j++) {
                    if (j % truncateIteration == 0) {
                        txWriter.truncate(0, zeroSymbolCounts);
                        LOG.info().$("writer truncated at ").$(txWriter.getTxn()).$();
                        // Create last partition back.
                        txWriter.setMaxTimestamp((maxPartitionCount + 1) * Micros.HOUR_MICROS);
                        txWriter.updatePartitionSizeByTimestamp(txWriter.getMaxTimestamp() * Micros.HOUR_MICROS, 1);
                        txWriter.commit(symbolCounts);
                        partitionCountCheck.set(0);
                    } else {
                        // Set txn file with random number of symbols and random number of partitions
                        int symbolCount = rnd.nextInt(maxSymbolCount);
                        if (symbolCount > symbolCounts.size()) {
                            for (int i = symbolCounts.size(); i < symbolCount; i++) {
                                symbolCounts.add(new SymbolCountProviderImpl(i));
                                zeroSymbolCounts.add(new SymbolCountProviderImpl(0));
                            }
                        } else {
                            symbolCounts.setPos(symbolCount);
                            zeroSymbolCounts.setPos(symbolCount);
                        }
                        txWriter.bumpMetadataAndColumnStructureVersion(symbolCounts);

                        // Set random number of partitions
                        int partitionCount = rnd.nextInt(maxPartitionCount);
                        int partitions = txWriter.getPartitionCount() - 1; // Last partition always stays

                        long offset = txWriter.getTxn() + 1 - txWriter.getMetadataVersion();
                        // Add / Update
                        for (int i = 0; i < partitionCount; i++) {
                            txWriter.updatePartitionSizeByTimestamp(i * Micros.HOUR_MICROS, offset + i);
                        }
                        // Remove from the end
                        for (int i = partitionCount; i < partitions; i++) {
                            txWriter.removeAttachedPartitions(i * Micros.HOUR_MICROS);
                        }
                        txWriter.bumpPartitionTableVersion();
                        assert txWriter.getPartitionCount() - 1 == partitionCount;

                        txWriter.setMaxTimestamp(partitionCount * Micros.HOUR_MICROS);
                        txWriter.commit(symbolCounts);
                        partitionCountCheck.set(partitionCount);
                    }

                    if (rnd.nextBoolean()) {
                        // Reopen txn file for writing
                        txWriter.ofRW(path.$(), timestampType, PartitionBy.HOUR);
                    }

                    if (!exceptions.isEmpty()) {
                        break;
                    }
                }
            } catch (Throwable e) {
                exceptions.add(e);
                LOG.error().$(e).$();
            } finally {
                done.incrementAndGet();
            }
        });
    }

    static class SymbolCountProviderImpl implements SymbolCountProvider {
        private final int count;

        SymbolCountProviderImpl(int count) {
            this.count = count;
        }

        @Override
        public int getSymbolCount() {
            return count;
        }
    }

}
