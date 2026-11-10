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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MemorySerializer;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriteMode;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.cairo.wal.DefaultWalDirectoryPolicy;
import io.questdb.cairo.wal.LocalDurabilityPolicy;
import io.questdb.cairo.wal.UnsupportedWalTxnTypeHandler;
import io.questdb.cairo.wal.WalEventCursor;
import io.questdb.cairo.wal.WalTxnDetails;
import io.questdb.cairo.wal.WalTxnTypeHandler;
import io.questdb.cairo.wal.seq.TableMetadataChangeLog;
import io.questdb.cairo.wal.seq.TableSequencer;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.cairo.wal.seq.TableTransactionLogFile;
import io.questdb.cairo.wal.seq.TableTransactionLogV1;
import io.questdb.cairo.wal.seq.TableTransactionLogV2;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.std.DirectLongList;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static io.questdb.cairo.wal.WalUtils.DELTA_TABLE_SWITCH_VERSION;
import static io.questdb.cairo.wal.WalUtils.DELTA_TABLE_SWITCH_WAL_ID;
import static io.questdb.cairo.wal.WalUtils.DROP_TABLE_WAL_ID;
import static io.questdb.cairo.wal.WalUtils.SEQ_DIR;
import static io.questdb.cairo.wal.WalUtils.TXNLOG_FILE_NAME;
import static io.questdb.cairo.wal.WalUtils.TXNLOG_PARTS_DIR;

public class DeltaPackSequencerTest extends AbstractCairoTest {
    private static final MemorySerializer SERIALIZER = new MemorySerializer() {
        @Override
        public void fromSink(Object instance, MemoryCR memory, long offsetLo, long offsetHi) {
        }

        @Override
        public short getCommandType(Object instance) {
            return 1;
        }

        @Override
        public void toSink(Object obj, MemoryA sink) {
        }
    };

    private static int legacyRecoveryCalls;
    private static long legacyRecoveryTxn = -1;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        engineFactory = configuration -> new CairoEngine(configuration) {
            @Override
            public WalTxnTypeHandler getWalTxnTypeHandler() {
                return new WalTxnTypeHandler() {
                    @Override
                    public int applyUnknownWalTxn(byte type, TableWriter writer, WalEventCursor cursor, long seqTxn) {
                        return UnsupportedWalTxnTypeHandler.INSTANCE.applyUnknownWalTxn(type, writer, cursor, seqTxn);
                    }

                    @Override
                    public long recoverDeltaTable(CairoEngine engine, TableToken token) {
                        if (legacyRecoveryTxn < 0) {
                            return WalTxnTypeHandler.super.recoverDeltaTable(engine, token);
                        }
                        legacyRecoveryCalls++;
                        return legacyRecoveryTxn;
                    }
                };
            }

            @Override
            public boolean isDeltaTableSupported() {
                return true;
            }
        };
        AbstractCairoTest.setUpStatic();
    }

    @Before
    public void resetRecoveryFixture() {
        legacyRecoveryCalls = 0;
        legacyRecoveryTxn = -1;
    }

    @Test
    public void testAdaptivePackedAdmissionFailureV1() throws Exception {
        testAdaptivePackedAdmissionFailure(0);
    }

    @Test
    public void testAdaptivePackedAdmissionFailureV2() throws Exception {
        testAdaptivePackedAdmissionFailure(2);
    }

    @Test
    public void testAdaptivePackedAdmissionV1() throws Exception {
        testAdaptivePackedAdmission(0);
    }

    @Test
    public void testAdaptivePackedAdmissionV2() throws Exception {
        testAdaptivePackedAdmission(2);
    }

    @Test
    public void testAdaptivePackedSchemaFailureV1() throws Exception {
        testAdaptivePackedSchemaFailure(0);
    }

    @Test
    public void testAdaptivePackedSchemaFailureV2() throws Exception {
        testAdaptivePackedSchemaFailure(2);
    }

    @Test
    public void testBoundaryActivatesAtCreationAndSwitch() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE d (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY DELTA");
            execute("CREATE TABLE w (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            TableToken delta = engine.verifyTableName("d");
            TableToken wal = engine.verifyTableName("w");
            TableSequencerAPI api = engine.getTableSequencerAPI();
            Assert.assertEquals(1, api.getPackStartTxn(delta));
            Assert.assertThrows(CairoException.class, () -> api.setPackStartTxn(wal, 1));
            Assert.assertEquals(1, api.beginDeltaTableSwitch(wal, 0));
            Assert.assertEquals(2, api.getPackStartTxn(wal));
            api.setPackStartTxn(wal, 2);
            api.setPackStartTxn(delta, 1);
            Assert.assertTrue(api.releaseAll());
            Assert.assertEquals(1, api.getPackStartTxn(delta));
            Assert.assertEquals(2, api.getPackStartTxn(wal));
        });
    }

    @Test
    public void testBoundaryFlushFailureRequiresRetry() throws Exception {
        AtomicBoolean fail = new AtomicBoolean();
        FilesFacadeImpl ff = new FilesFacadeImpl() {
            @Override
            public void fdatasync(long fd) {
                if (fail.getAndSet(false)) {
                    throw CairoException.critical(0).put("injected boundary flush failure");
                }
                super.fdatasync(fd);
            }
        };
        assertMemoryLeak(ff, () -> {
            for (int partCount : new int[]{0, 2}) {
                try (Path path = new Path(); TableTransactionLogFile log = newLog(partCount)) {
                    path.of(root).concat("failed-boundary").put(partCount);
                    ff.mkdir(path.$(), configuration.getMkDirMode());
                    log.create(path, 10_000_000_000L);
                    log.open(path);
                    fail.set(true);
                    Assert.assertThrows(CairoException.class, () -> log.setPackStartTxn(1));
                    Assert.assertEquals(0, log.getPackStartTxn());
                    Assert.assertEquals(0, log.lastTxn());
                    log.setPackStartTxn(1);
                    Assert.assertEquals(1, log.getPackStartTxn());
                    Assert.assertEquals(1, log.addEntry(0, 0, 1, 4, 10_000_000_001L, 1, 1, 1));
                }
            }
        });
    }

    @Test
    public void testBoundaryFlushFailureFencesSequencer() throws Exception {
        AtomicBoolean fail = new AtomicBoolean();
        FilesFacadeImpl ff = new FilesFacadeImpl() {
            @Override
            public void fdatasync(long fd) {
                if (fail.getAndSet(false)) {
                    throw CairoException.dataSyncFailure(this, 5, "fdatasync", "injected boundary flush failure");
                }
                super.fdatasync(fd);
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE d (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            TableToken token = engine.verifyTableName("d");
            TableSequencerAPI api = engine.getTableSequencerAPI();
            Assert.assertEquals(0, api.getPackStartTxn(token));
            fail.set(true);
            Assert.assertThrows(CairoException.class, () -> api.beginDeltaTableSwitch(token, 0));
            Assert.assertTrue(api.getTxnTracker(token).isDurableFrontierFenced());
            Assert.assertEquals(0, api.getPackStartTxn(token));
            Assert.assertEquals(1, api.lastTxn(token));
            Assert.assertTrue(api.releaseAll());
            // The accepted SWITCH survives the failed boundary flush. Recovery
            // restores its transport even when no client retries or inserts.
            Assert.assertEquals(2, api.getPackStartTxn(token));
        });
    }

    @Test
    public void testCoordinatesUseBoundaryAndPreserveUnsignedBits() throws Exception {
        assertMemoryLeak(() -> {
            for (int partCount : new int[]{0, 2}) {
                try (Path path = new Path(); TableTransactionLogFile log = newLog(partCount)) {
                    path.of(root).concat("coordinate-kinds-").put(partCount);
                    configuration.getFilesFacade().mkdir(path.$(), configuration.getMkDirMode());
                    log.create(path, 10_000_000_000L);
                    log.open(path);
                    log.beginMetadataChangeEntry(1, SERIALIZER, null, 10_000_000_001L);
                    Assert.assertEquals(1, log.endMetadataChangeEntry());
                    Assert.assertEquals(2, log.addDeltaTableSwitchEntry(1, 10_000_000_002L));
                    log.setPackStartTxn(3);
                    log.beginMetadataChangeEntry(2, SERIALIZER, null, 10_000_000_003L, -1, -1, -1);
                    Assert.assertEquals(3, log.endMetadataChangeEntry());
                    Assert.assertEquals(4, log.addEntry(2, 0, 0, 4, 10_000_000_004L, 1, 1, 1));
                    // Controls keep their meaning on either side of the boundary.
                    Assert.assertEquals(5, log.addDeltaTableSwitchEntry(2, 10_000_000_005L));
                    Assert.assertEquals(6, log.addEntry(2, DROP_TABLE_WAL_ID, -1, -1, 10_000_000_006L, 0, 0, 0));
                    try (TransactionLogCursor cursor = log.getCursor(0, path)) {
                        assertCursorEntry(cursor, 1, -1, -1, -1, false);
                        assertCursorEntry(cursor, 2, DELTA_TABLE_SWITCH_WAL_ID, DELTA_TABLE_SWITCH_VERSION, TableWriteMode.DELTA_RUNS, false);
                        assertCursorEntry(cursor, 3, -1, -1, -1, true);
                        assertCursorEntry(cursor, 4, 0, 0, 4, true);
                        assertCursorEntry(cursor, 5, DELTA_TABLE_SWITCH_WAL_ID, DELTA_TABLE_SWITCH_VERSION, TableWriteMode.DELTA_RUNS, false);
                        assertCursorEntry(cursor, 6, DROP_TABLE_WAL_ID, -1, -1, false);
                        Assert.assertFalse(cursor.hasNext());
                    }
                }
            }
        });
    }

    @Test
    public void testPackedAdmissionRequiresValidReservationV1() throws Exception {
        testPackedAdmissionRequiresValidReservation(0);
    }

    @Test
    public void testPackedAdmissionRequiresValidReservationV2() throws Exception {
        testPackedAdmissionRequiresValidReservation(2);
    }

    @Test
    public void testLegacyRecoveryBeforeFirstPackedTransaction() throws Exception {
        testLegacyRecoveryBeforeFirstPackedTransaction(false);
    }

    @Test
    public void testLegacyRecoveryWithBoundaryBeforeFirstPackedTransaction() throws Exception {
        testLegacyRecoveryBeforeFirstPackedTransaction(true);
    }

    @Test
    public void testMalformedSequencerKindsFailOnReopen() throws Exception {
        assertMemoryLeak(() -> {
            for (int partCount : new int[]{0, 2}) {
                node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, partCount);
                for (int corruption = 0; corruption < 3; corruption++) {
                    String tableName = "invalid_kind_" + partCount + "_" + corruption;
                    execute("CREATE TABLE " + tableName + " (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY "
                            + (corruption == 0 ? "WAL" : "DELTA"));
                    TableToken token = engine.verifyTableName(tableName);
                    TableSequencerAPI api = engine.getTableSequencerAPI();
                    if (corruption > 0) {
                        api.setPackStartTxn(token, 1);
                    }
                    Assert.assertTrue(api.releaseAll());
                    // Write a malformed, checksummed record to exercise validation on reopening
                    // the real sequencer, independently of admission validation.
                    try (Path path = new Path(); TableTransactionLogFile log = newLog(partCount)) {
                        path.of(root).concat(token).concat(SEQ_DIR);
                        log.open(path);
                        int walId = corruption == 0 ? Integer.MIN_VALUE : DELTA_TABLE_SWITCH_WAL_ID;
                        int segmentId = corruption == 2 ? DELTA_TABLE_SWITCH_VERSION : 0;
                        int segmentTxn = corruption == 0 ? 2 : 0;
                        Assert.assertEquals(1, log.addEntry(0, walId, segmentId, segmentTxn, 10_000_000_001L, 0, 0, 0));
                        log.fdatasyncTxnLog();
                    }
                    CairoException error = Assert.assertThrows(CairoException.class, () -> api.lastTxn(token));
                    TestUtils.assertContains(error.getFlyweightMessage(), corruption == 0
                            ? "unsupported sequencer transaction kind"
                            : "invalid Delta table switch record");
                }
            }
        });
    }

    @Test
    public void testPackedMetadataV1() throws Exception {
        testPackedMetadata(0);
    }

    @Test
    public void testPackedMetadataV2() throws Exception {
        testPackedMetadata(2);
    }

    @Test
    public void testDeltaEventReplayUsesRecoveryHandler() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE d (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY DELTA");
            TableToken token = engine.verifyTableName("d");
            TableSequencerAPI api = engine.getTableSequencerAPI();
            api.setPackStartTxn(token, 1);
            long txn = api.reserveDeltaTxn(token, 0, 0);
            api.acceptDeltaPackTxn(token, txn, 0, 0, 1, 4, 10_000_000_001L, 1, 1, 1);
            api.finishDeltaTxn(token, txn);
            legacyRecoveryTxn = 1;
            drainWalQueue();
            Assert.assertEquals(1, legacyRecoveryCalls);
            Assert.assertFalse(api.isSuspended(token));
            Assert.assertEquals(1, api.getTxnTracker(token).getWriterTxn());
            Assert.assertEquals(1, api.lastTxn(token));
        });
    }

    @Test
    public void testPackedV1AdaptiveSchemaTail() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path(); TableTransactionLogFile log = newLog(0); MemoryCMARW header = Vm.getCMARWInstance()) {
                path.of(root).concat("adaptive-pack");
                configuration.getFilesFacade().mkdir(path.$(), configuration.getMkDirMode());
                int dirLen = path.size();
                log.create(path, 10_000_000_000L);
                log.open(path);
                log.setPackStartTxn(1);
                log.addEntry(0xffff_ffffL, 0, 1, 4, 10_000_000_001L, 1, 1, 1);
                // Crossing the low 32-bit boundary exercises V1's torn schema-word check.
                log.beginMetadataChangeEntry(0x1_0000_0000L, SERIALIZER, null, 10_000_000_002L, Integer.MIN_VALUE, 0, 4);
                log.endMetadataChangeEntry();
                log.addEntry(0x1_0000_0000L, -1, -1, -1, 10_000_000_003L, 1, 1, 1);
                log.fdatasyncTxnLog();
                log.close();
                header.smallFile(configuration.getFilesFacade(), path.concat(TXNLOG_FILE_NAME).$(), MemoryTag.MMAP_DEFAULT);
                header.putLong(40, 1);
                Assert.assertTrue(TableTransactionLogV1.isTailIntact(configuration.getFilesFacade(), header.getFd(), 3, 1, true));
                // A short read of the final record must not certify the retained packed tail.
                FilesFacadeImpl shortReader = new FilesFacadeImpl() {
                    @Override
                    public long read(long fd, long address, long len, long offset) {
                        return super.read(fd, address, len - 1, offset);
                    }
                };
                Assert.assertFalse(TableTransactionLogV1.isTailIntact(shortReader, header.getFd(), 3, 1, true));
                header.close(false);
                path.trimTo(dirLen);
                Assert.assertEquals(0x1_0000_0000L, log.open(path));
                Assert.assertEquals(3, log.lastTxn());
            }
        });
    }

    @Test
    public void testPackedV1Reopen() throws Exception {
        testPackedLog(0);
    }

    @Test
    public void testPackedV2Reopen() throws Exception {
        testPackedLog(2);
    }

    @Test
    public void testPackedV2RequiresRecordChecksum() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path(); TableTransactionLogFile log = newLog(2); MemoryCMARW part = Vm.getCMARWInstance()) {
                path.of(root).concat("crc-pack");
                configuration.getFilesFacade().mkdir(path.$(), configuration.getMkDirMode());
                int dirLen = path.size();
                log.create(path, 10_000_000_000L);
                log.open(path);
                log.setPackStartTxn(1);
                log.addEntry(0, 0, 1, 4, 10_000_000_001L, 1, 1, 1);
                log.fdatasyncTxnLog();
                part.smallFile(configuration.getFilesFacade(), path.concat(TXNLOG_PARTS_DIR).concat("0").$(), MemoryTag.MMAP_DEFAULT);
                part.putLong(TableTransactionLogV2.RESERVED_OFFSET, 0);
                path.trimTo(dirLen);
                try (TransactionLogCursor cursor = log.getCursor(0, path)) {
                    CairoException error = Assert.assertThrows(CairoException.class, cursor::hasNext);
                    Assert.assertTrue(error.getFlyweightMessage().toString().contains("missing CRC on packed sequencer record"));
                }
            }
        });
    }

    @Test
    public void testSharedV2FixtureChecksums() throws Exception {
        assertMemoryLeak(() -> {
            // Rust's independently constructed sequencer fixtures use these exact raw records.
            // Their trailing min/max timestamps and row count are zero.
            long[][] fixtures = {
                    {0, 3, 4, 5, 1000, 0xdaafa8f432889639L},
                    {1, -3, 1, 1, 1002, 0xc5e5edcfb6013fa1L},
                    {1, 0, 0, 4, 1003, 0x08c248a3c96ffa49L},
                    {1, Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE, 1004, 0x8f50659e47b2ac44L},
                    {2, -1, -1, -1, 1005, 0xd3ae046e26109e7eL}
            };
            long size = TableTransactionLogV2.RESERVED_OFFSET;
            long address = Unsafe.calloc(size, MemoryTag.NATIVE_DEFAULT);
            try {
                for (long[] fixture : fixtures) {
                    Unsafe.putLong(address, fixture[0]);
                    Unsafe.putInt(address + 8, (int) fixture[1]);
                    Unsafe.putInt(address + 12, (int) fixture[2]);
                    Unsafe.putInt(address + 16, (int) fixture[3]);
                    Unsafe.putLong(address + 20, fixture[4]);
                    Assert.assertEquals(fixture[5], TableUtils.calculateCvAreaChecksum(address, size));
                }
            } finally {
                Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testSwitchedV1Boundary() throws Exception {
        testSwitchedLog(0);
    }

    @Test
    public void testSwitchedV2Boundary() throws Exception {
        testSwitchedLog(2);
    }

    @Test
    public void testSyncPackedAdmissionFailureRetainsPublishedTxnV1() throws Exception {
        testSyncPackedAdmissionFailureRetainsPublishedTxn(0);
    }

    @Test
    public void testSyncPackedAdmissionFailureRetainsPublishedTxnV2() throws Exception {
        testSyncPackedAdmissionFailureRetainsPublishedTxn(2);
    }

    private static void assertCursorEntry(TransactionLogCursor cursor, long txn, int walId, int segmentId, int segmentTxn, boolean isPacked) {
        Assert.assertTrue(cursor.hasNext());
        Assert.assertEquals(txn, cursor.getTxn());
        Assert.assertEquals(walId, cursor.getWalId());
        Assert.assertEquals(segmentId, cursor.getSegmentId());
        Assert.assertEquals(segmentTxn, cursor.getSegmentTxn());
        Assert.assertEquals(isPacked, cursor.isPacked());
    }

    private TableTransactionLogFile newLog(int partCount) {
        return partCount == 0
                ? new TableTransactionLogV1(configuration, LocalDurabilityPolicy.ALWAYS_ON, () -> new long[]{0, 0})
                : new TableTransactionLogV2(configuration, partCount, DefaultWalDirectoryPolicy.INSTANCE);
    }

    private void testAdaptivePackedAdmission(int partCount) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, 50_000);
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, partCount);
        final DeltaSyncFacade ff = new DeltaSyncFacade();
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE d (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY DELTA");
            final TableToken token = engine.verifyTableName("d");
            final TableSequencerAPI api = engine.getTableSequencerAPI();
            api.setPackStartTxn(token, 1);
            // Includes V2 part rotation. No WAL writer or group-commit waiter follows this admission.
            for (int txn = 1; txn <= 3; txn++) {
                Assert.assertEquals(txn, api.reserveDeltaTxn(token, 0, txn - 1));
                try {
                    ff.fdatasyncCount = 0;
                    Assert.assertEquals(txn, api.acceptDeltaPackTxn(token, txn, 0, 0, 1, txn * 8,
                            10_000_000_000L + txn, txn, txn, 1));
                    Assert.assertTrue("Delta admission returned before its device barrier", ff.fdatasyncCount > 0);
                    Assert.assertEquals(txn, api.getTxnTracker(token).getLocalDurableSeqTxn());
                } finally {
                    api.finishDeltaTxn(token, txn);
                }
            }
            final AlterOperationBuilder builder = new AlterOperationBuilder().ofAddColumn(0, token, token.getTableId());
            builder.ofAddColumn("extra", 0, ColumnType.LONG, 0, false, (byte) 0, 0);
            try (AlterOperation alter = builder.build()) {
                Assert.assertEquals(4, api.reserveDeltaTxn(token, 0, 3));
                try {
                    ff.syncMsyncCount = 0;
                    Assert.assertEquals(4, api.acceptDeltaStructureTxn(token, 4, 0, alter, 0, 1, 32));
                    Assert.assertTrue("Delta schema admission must keep the synchronous metadata path", ff.syncMsyncCount > 0);
                    Assert.assertEquals(4, api.getTxnTracker(token).getLocalDurableSeqTxn());
                } finally {
                    api.finishDeltaTxn(token, 4);
                }
            }
            Assert.assertTrue(api.releaseAll());
            Assert.assertEquals(4, api.lastTxn(token));
            try (TransactionLogCursor cursor = api.getCursor(token, 0)) {
                for (int txn = 1; txn <= 4; txn++) {
                    assertCursorEntry(cursor, txn, 0, 1, txn * 8, true);
                    Assert.assertEquals(txn == 4 ? 1 : 0, cursor.getStructureVersion());
                }
                Assert.assertFalse(cursor.hasNext());
            }
        });
    }

    private void testAdaptivePackedAdmissionFailure(int partCount) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, 50_000);
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, partCount);
        final DeltaSyncFacade ff = new DeltaSyncFacade();
        assertMemoryLeak(ff, () -> {
            // V2 has separate part/header barriers; neither failure may erase accepted coordinates.
            for (int barrier = 1, n = partCount == 0 ? 1 : 2; barrier <= n; barrier++) {
                final String tableName = "d" + barrier;
                execute("CREATE TABLE " + tableName + " (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY DELTA");
                final TableToken token = engine.verifyTableName(tableName);
                final TableSequencerAPI api = engine.getTableSequencerAPI();
                api.setPackStartTxn(token, 1);
                Assert.assertEquals(1, api.reserveDeltaTxn(token, 0, 0));
                final long durableBefore = api.getTxnTracker(token).getLocalDurableSeqTxn();
                try {
                    ff.fdatasyncCount = 0;
                    ff.failBarrier = barrier;
                    final CairoException error = Assert.assertThrows(CairoException.class,
                            () -> api.acceptDeltaPackTxn(token, 1, 0, 0, 1, 4, 10_000_000_001L, 1, 1, 1));
                    TestUtils.assertContains(error.getFlyweightMessage(), "injected Delta sequencer barrier failure");
                    Assert.assertEquals(0, ff.failBarrier);
                    Assert.assertTrue(api.getTxnTracker(token).isDurableFrontierFenced());
                    Assert.assertEquals(durableBefore, api.getTxnTracker(token).getLocalDurableSeqTxn());
                } finally {
                    ff.failBarrier = 0;
                    // The failed barrier distresses the owner. Recovery reopens it instead of
                    // continuing through its reservation; the accepted address must survive that.
                    Assert.assertTrue(api.releaseAll());
                }
                Assert.assertEquals(1, api.lastTxn(token));
                try (TransactionLogCursor cursor = api.getCursor(token, 0)) {
                    assertCursorEntry(cursor, 1, 0, 1, 4, true);
                    Assert.assertFalse(cursor.hasNext());
                }
            }
        });
    }

    private void testAdaptivePackedSchemaFailure(int partCount) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, 50_000);
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, partCount);
        final DeltaSyncFacade ff = new DeltaSyncFacade();
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE d (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY DELTA");
            final TableToken token = engine.verifyTableName("d");
            final TableSequencerAPI api = engine.getTableSequencerAPI();
            api.setPackStartTxn(token, 1);
            final AlterOperationBuilder builder = new AlterOperationBuilder().ofAddColumn(0, token, token.getTableId());
            builder.ofAddColumn("extra", 0, ColumnType.LONG, 0, false, (byte) 0, 0);
            try (AlterOperation alter = builder.build()) {
                Assert.assertEquals(1, api.reserveDeltaTxn(token, 0, 0));
                try {
                    ff.failSync = true;
                    final CairoException error = Assert.assertThrows(CairoException.class,
                            () -> api.acceptDeltaStructureTxn(token, 1, 0, alter, 0, 1, 4));
                    TestUtils.assertContains(error.getFlyweightMessage(), "injected Delta metadata sync failure");
                    Assert.assertFalse(ff.failSync);
                    Assert.assertTrue(api.getTxnTracker(token).isDurableFrontierFenced());
                } finally {
                    ff.failSync = false;
                    Assert.assertTrue(api.releaseAll());
                }
            }
            Assert.assertEquals(0, api.lastTxn(token));
            Assert.assertEquals(1, api.reserveDeltaTxn(token, 0, 0));
            api.finishDeltaTxn(token, 1);
        });
    }

    private void testLegacyRecoveryBeforeFirstPackedTransaction(boolean activateBoundary) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE legacy (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY DELTA");
            final TableToken token = engine.verifyTableName("legacy");
            final TableSequencerAPI api = engine.getTableSequencerAPI();
            Assert.assertTrue(api.releaseAll());
            // Seed retained legacy history directly: new Delta producers only accept packed records.
            try (Path path = new Path(); MemoryCMARW header = Vm.getCMARWInstance()) {
                path.of(root).concat(token).concat(SEQ_DIR).concat(TXNLOG_FILE_NAME);
                header.smallFile(configuration.getFilesFacade(), path.$(), MemoryTag.MMAP_DEFAULT);
                header.putLong(TableTransactionLogFile.PACK_START_TXN_OFFSET_64, 0);
                header.sync(false);
            }
            try (Path path = new Path(); TableTransactionLogFile log = newLog(configuration.getDefaultSeqPartTxnCount())) {
                path.of(root).concat(token).concat(SEQ_DIR);
                log.open(path);
                Assert.assertEquals(1, log.addEntry(0, 1, 0, 0, 10_000_000_001L, 1, 1, 1));
                log.fdatasyncTxnLog();
            }
            if (activateBoundary) {
                api.setPackStartTxn(token, 2);
            }
            Assert.assertTrue(api.initTxnTracker(token, 0, 1));
            engine.notifyWalTxnCommitted(token);
            legacyRecoveryCalls = 0;
            legacyRecoveryTxn = 1;
            drainWalQueue();
            Assert.assertEquals(1, legacyRecoveryCalls);
            Assert.assertEquals(1, api.getTxnTracker(token).getWriterTxn());
            Assert.assertFalse(api.isSuspended(token));
        });
    }

    private void testPackedAdmissionRequiresValidReservation(int partCount) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, partCount);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE d (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY DELTA");
            final TableToken token = engine.verifyTableName("d");
            final TableSequencerAPI api = engine.getTableSequencerAPI();
            Assert.assertThrows(CairoException.class, () -> api.setPackStartTxn(token, 0));
            Assert.assertEquals(1, api.getPackStartTxn(token));
            api.setPackStartTxn(token, 1);
            final CairoException missingReservation = Assert.assertThrows(CairoException.class,
                    () -> api.acceptDeltaPackTxn(token, 1, 0, 0, 0, 4, 10_000_000_001L, 1, 1, 1));
            TestUtils.assertContains(missingReservation.getFlyweightMessage(), "Delta transaction reservation changed");
            Assert.assertEquals(0, api.lastTxn(token));

            Assert.assertEquals(1, api.reserveDeltaTxn(token, 0, 0));
            try {
                for (int[] coordinates : new int[][]{
                        {DROP_TABLE_WAL_ID, 0, 4},
                        {DELTA_TABLE_SWITCH_WAL_ID, 0, 4},
                        {0, 0, 0}, {0, 0, 1}, {0, 0, 2}, {0, 0, 3}
                }) {
                    final CairoException error = Assert.assertThrows(CairoException.class,
                            () -> api.acceptDeltaPackTxn(token, 1, 0, coordinates[0], coordinates[1], coordinates[2], 10_000_000_001L, 1, 1, 1));
                    TestUtils.assertContains(error.getFlyweightMessage(), coordinates[0] < 0
                            ? "pack transport is not active"
                            : "invalid local event coordinates");
                    Assert.assertEquals(0, api.lastTxn(token));
                }
                final CairoException changedReservation = Assert.assertThrows(CairoException.class,
                        () -> api.acceptDeltaPackTxn(token, 2, 0, 0, 0, 4, 10_000_000_001L, 1, 1, 1));
                TestUtils.assertContains(changedReservation.getFlyweightMessage(), "Delta transaction reservation changed");
                final CairoException changedStructure = Assert.assertThrows(CairoException.class,
                        () -> api.acceptDeltaPackTxn(token, 1, 1, 0, 0, 4, 10_000_000_001L, 1, 1, 1));
                TestUtils.assertContains(changedStructure.getFlyweightMessage(), "Delta transaction reservation changed");
                Assert.assertEquals(0, api.lastTxn(token));
                Assert.assertEquals(1, api.acceptDeltaPackTxn(token, 1, 0, 0, 0, 4, 10_000_000_001L, 1, 1, 1));
            } finally {
                api.finishDeltaTxn(token, 1);
            }

            final AlterOperationBuilder builder = new AlterOperationBuilder().ofAddColumn(0, token, token.getTableId());
            builder.ofAddColumn("extra", 0, ColumnType.LONG, 0, false, (byte) 0, 0);
            try (AlterOperation alter = builder.build()) {
                Assert.assertEquals(2, api.reserveDeltaTxn(token, 0, 1));
                try {
                    final CairoException changedStructure = Assert.assertThrows(CairoException.class,
                            () -> api.acceptDeltaStructureTxn(token, 2, 1, alter, -1, -1, -1));
                    TestUtils.assertContains(changedStructure.getFlyweightMessage(), "Delta metadata reservation changed");
                    Assert.assertEquals(1, api.lastTxn(token));
                    Assert.assertEquals(0, api.getTxnTracker(token).getStructureVersion());
                    Assert.assertEquals(2, api.acceptDeltaStructureTxn(token, 2, 0, alter, -1, -1, -1));
                } finally {
                    api.finishDeltaTxn(token, 2);
                }
            }
            Assert.assertEquals(3, api.reserveDeltaTxn(token, 1, 2));
            try {
                Assert.assertEquals(3, api.acceptDeltaPackTxn(token, 3, 1,
                        Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE, 10_000_000_003L, 1, 1, 1));
            } finally {
                api.finishDeltaTxn(token, 3);
            }
            Assert.assertTrue(api.releaseAll());
            try (TransactionLogCursor cursor = api.getCursor(token, 0)) {
                assertCursorEntry(cursor, 1, 0, 0, 4, true);
                Assert.assertEquals(0, cursor.getStructureVersion());
                assertCursorEntry(cursor, 2, -1, -1, -1, true);
                Assert.assertEquals(1, cursor.getStructureVersion());
                assertCursorEntry(cursor, 3, Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE, true);
                Assert.assertEquals(1, cursor.getStructureVersion());
                Assert.assertFalse(cursor.hasNext());
            }
        });
    }

    private void testPackedLog(int partCount) throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path(); TableTransactionLogFile log = newLog(partCount)) {
                path.of(root).concat("pack-seq");
                configuration.getFilesFacade().mkdir(path.$(), configuration.getMkDirMode());
                log.create(path, 10_000_000_000L);
                log.open(path);
                Assert.assertEquals(0, log.getPackStartTxn());
                Assert.assertThrows(CairoException.class, () -> log.setPackStartTxn(2));
                Assert.assertEquals(0, log.lastTxn());
                log.setPackStartTxn(1);
                log.setPackStartTxn(1);
                Assert.assertThrows(CairoException.class, () -> log.setPackStartTxn(2));
                Assert.assertEquals(1, log.addEntry(0, 0, 1, 4, 10_000_000_001L, 1, 1, 1));
                log.beginMetadataChangeEntry(1, SERIALIZER, null, 10_000_000_002L, -1, -1, -1);
                Assert.assertEquals(2, log.endMetadataChangeEntry());
                Assert.assertEquals(3, log.addEntry(1, Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE, 10_000_000_003L, 2, 2, 1));
                log.fdatasyncTxnLog();
                log.close();
                try (TableTransactionLogFile reopened = newLog(partCount)) {
                    reopened.open(path);
                    Assert.assertEquals(1, reopened.getPackStartTxn());
                    Assert.assertEquals(3, reopened.lastTxn());
                    try (TransactionLogCursor cursor = reopened.getCursor(0, path)) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertTrue(cursor.isPacked());
                        Assert.assertEquals(0, cursor.getWalId());
                        Assert.assertEquals(1, cursor.getSegmentId());
                        Assert.assertEquals(4, cursor.getSegmentTxn());
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertTrue(cursor.isPacked());
                        Assert.assertEquals(-1, cursor.getWalId());
                        Assert.assertEquals(-1, cursor.getSegmentId());
                        Assert.assertEquals(-1, cursor.getSegmentTxn());
                        Assert.assertEquals(1, cursor.getStructureVersion());
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertTrue(cursor.isPacked());
                        Assert.assertEquals(Integer.MIN_VALUE, cursor.getWalId());
                        Assert.assertEquals(Integer.MIN_VALUE, cursor.getSegmentId());
                        Assert.assertEquals(Integer.MIN_VALUE, cursor.getSegmentTxn());
                        Assert.assertFalse(cursor.hasNext());
                    }
                }
            }
        });
    }

    private void testPackedMetadata(int partCount) throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, partCount);
            execute("CREATE TABLE d (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY DELTA");
            TableToken token = engine.verifyTableName("d");
            TableSequencerAPI api = engine.getTableSequencerAPI();
            api.setPackStartTxn(token, 1);
            Assert.assertThrows(CairoException.class, () -> api.setPackStartTxn(token, 2));
            long txn = api.reserveDeltaTxn(token, 0, 0);
            CairoException invalidOffset = Assert.assertThrows(CairoException.class,
                    () -> api.acceptDeltaPackTxn(token, txn, 0, 1, 0, 0, 10_000_000_001L, 1, 1, 1));
            TestUtils.assertContains(invalidOffset.getFlyweightMessage(), "invalid local event coordinates");
            Assert.assertEquals(0, api.lastTxn(token));
            Assert.assertEquals(1, api.acceptDeltaPackTxn(token, txn, 0, 0, 1, 4, 10_000_000_001L, 1, 1, 1));
            api.finishDeltaTxn(token, txn);
            AlterOperationBuilder builder = new AlterOperationBuilder().ofAddColumn(0, token, token.getTableId());
            builder.ofAddColumn("extra", 0, ColumnType.LONG, 0, false, (byte) 0, 0);
            try (AlterOperation alter = builder.build()) {
                Assert.assertThrows(CairoException.class, () -> api.nextDeltaStructureTxn(token, 0, alter));
                for (int[] coordinates : new int[][]{
                        {DROP_TABLE_WAL_ID, 0, 4},
                        {DELTA_TABLE_SWITCH_WAL_ID, 0, 4},
                        {0, 0, 0},
                        {0, 0, 1},
                        {0, 0, 2},
                        {0, 0, 3}
                }) {
                    CairoException error = Assert.assertThrows(CairoException.class,
                            () -> api.nextDeltaStructureTxn(token, 0, alter, coordinates[0], coordinates[1], coordinates[2]));
                    TestUtils.assertContains(error.getFlyweightMessage(), coordinates[0] < 0
                            ? "pack transport is not active"
                            : "invalid local event coordinates");
                    if (coordinates[0] == 0) {
                        TestUtils.assertContains(error.getFlyweightMessage(), "offset8=" + coordinates[2]);
                    }
                    Assert.assertEquals(1, api.lastTxn(token));
                    Assert.assertEquals(0, api.getTxnTracker(token).getStructureVersion());
                }
                Assert.assertEquals(2, api.nextDeltaStructureTxn(token, 0, alter, -1, -1, -1));
                // A stale writer must reload metadata; it cannot publish a second schema change.
                Assert.assertEquals(TableSequencer.NO_TXN, api.nextDeltaStructureTxn(token, 0, alter, 0, 2, 4));
            }
            Assert.assertEquals(1, api.getTxnTracker(token).getStructureVersion());
            Assert.assertTrue(api.releaseAll());
            Assert.assertEquals(2, api.lastTxn(token));
            Assert.assertEquals(1, api.getPackStartTxn(token));
            try (TableMetadataChangeLog changes = api.getMetadataChangeLog(token, 0)) {
                Assert.assertTrue(changes.hasNext());
                Assert.assertEquals(AlterOperation.ADD_COLUMN, ((AlterOperation) changes.next()).getCommand());
                Assert.assertFalse(changes.hasNext());
            }
            try (TransactionLogCursor cursor = api.getCursor(token, 0);
                 DirectLongList txns = new DirectLongList(8, MemoryTag.NATIVE_DEFAULT)) {
                CairoException error = Assert.assertThrows(CairoException.class, () -> WalTxnDetails.loadTxns(cursor, 1, txns));
                Assert.assertTrue(error.getFlyweightMessage().toString().contains("event pack replay is not enabled"));
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(-1, cursor.getWalId());
                Assert.assertEquals(-1, cursor.getSegmentId());
                Assert.assertEquals(-1, cursor.getSegmentTxn());
                Assert.assertEquals(1, cursor.getStructureVersion());
            }
        });
    }

    private void testSwitchedLog(int partCount) throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path(); TableTransactionLogFile log = newLog(partCount)) {
                path.of(root).concat("switch-seq");
                configuration.getFilesFacade().mkdir(path.$(), configuration.getMkDirMode());
                log.create(path, 10_000_000_000L);
                log.open(path);
                Assert.assertEquals(1, log.addEntry(0, 1, 0, 0, 10_000_000_001L, 1, 1, 1));
                Assert.assertEquals(2, log.addDeltaTableSwitchEntry(0, 10_000_000_002L));
                log.setPackStartTxn(3);
                log.close();
                try (TableTransactionLogFile reopened = newLog(partCount)) {
                    reopened.open(path);
                    Assert.assertEquals(3, reopened.getPackStartTxn());
                    Assert.assertEquals(2, reopened.lastTxn());
                    try (TransactionLogCursor cursor = reopened.getCursor(0, path)) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertFalse(cursor.isPacked());
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertFalse(cursor.isPacked());
                        Assert.assertEquals(DELTA_TABLE_SWITCH_WAL_ID, cursor.getWalId());
                        Assert.assertFalse(cursor.hasNext());
                    }
                }
            }
        });
    }

    private void testSyncPackedAdmissionFailureRetainsPublishedTxn(int partCount) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, partCount);
        final DeltaSyncFacade ff = new DeltaSyncFacade();
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE d (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY DELTA");
            final TableToken token = engine.verifyTableName("d");
            final TableSequencerAPI api = engine.getTableSequencerAPI();
            api.setPackStartTxn(token, 1);
            Assert.assertEquals(1, api.reserveDeltaTxn(token, 0, 0));
            final long durableBefore = api.getTxnTracker(token).getLocalDurableSeqTxn();
            try {
                ff.failSync = true;
                final CairoException error = Assert.assertThrows(CairoException.class,
                        () -> api.acceptDeltaPackTxn(token, 1, 0, 0, 1, 4, 10_000_000_001L, 1, 1, 1));
                TestUtils.assertContains(error.getFlyweightMessage(), "injected Delta metadata sync failure");
                Assert.assertFalse(ff.failSync);
                // The record is visible before its flush fails. The wrapper must refresh its cached
                // lastTxn in finally so acceptance notifies this frontier before any owner is reopened.
                Assert.assertEquals(1, api.getTxnTracker(token).getSeqTxn());
                Assert.assertTrue(api.getTxnTracker(token).isDurableFrontierFenced());
                Assert.assertEquals(durableBefore, api.getTxnTracker(token).getLocalDurableSeqTxn());
            } finally {
                ff.failSync = false;
                Assert.assertTrue(api.releaseAll());
            }
            Assert.assertEquals(1, api.lastTxn(token));
            try (TransactionLogCursor cursor = api.getCursor(token, 0)) {
                assertCursorEntry(cursor, 1, 0, 1, 4, true);
                Assert.assertFalse(cursor.hasNext());
            }
        });
    }

    private static class DeltaSyncFacade extends TestFilesFacadeImpl {
        private int failBarrier;
        private boolean failSync;
        private int fdatasyncCount;
        private int syncMsyncCount;

        @Override
        public void fdatasync(long fd) {
            if (++fdatasyncCount == failBarrier) {
                failBarrier = 0;
                throw CairoException.dataSyncFailure(this, 5, "fdatasync", "injected Delta sequencer barrier failure");
            }
            super.fdatasync(fd);
        }

        @Override
        public void msync(long address, long len, boolean async) {
            if (!async) {
                syncMsyncCount++;
                if (failSync) {
                    failSync = false;
                    throw CairoException.dataSyncFailure(this, 5, "msync", "injected Delta metadata sync failure");
                }
            }
            super.msync(address, len, async);
        }
    }
}
