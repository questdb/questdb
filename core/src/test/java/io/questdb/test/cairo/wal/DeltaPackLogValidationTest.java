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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.MemorySerializer;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.cairo.wal.DefaultWalDirectoryPolicy;
import io.questdb.cairo.wal.LocalDurabilityPolicy;
import io.questdb.cairo.wal.seq.TableTransactionLogFile;
import io.questdb.cairo.wal.seq.TableTransactionLogV1;
import io.questdb.cairo.wal.seq.TableTransactionLogV2;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.wal.WalUtils.TXNLOG_FILE_NAME;

public class DeltaPackLogValidationTest extends AbstractCairoTest {
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

    @Test
    public void testMetadataChangesRequireCoordinatesOnlyAfterBoundary() throws Exception {
        assertMemoryLeak(() -> {
            for (int partCount : new int[]{0, 2}) {
                try (Path path = new Path(); TableTransactionLogFile log = newLog(partCount)) {
                    path.of(root).concat("metadata-boundary-").put(partCount);
                    configuration.getFilesFacade().mkdir(path.$(), configuration.getMkDirMode());
                    log.create(path, 10_000_000_000L);
                    log.open(path);
                    log.beginMetadataChangeEntry(1, SERIALIZER, null, 10_000_000_001L);
                    Assert.assertEquals(1, log.endMetadataChangeEntry());
                    log.setPackStartTxn(2);
                    CairoException error = Assert.assertThrows(CairoException.class,
                            () -> log.beginMetadataChangeEntry(2, SERIALIZER, null, 10_000_000_002L));
                    TestUtils.assertContains(error.getFlyweightMessage(), "requires pack coordinates");
                    Assert.assertEquals(1, log.lastTxn());
                    log.beginMetadataChangeEntry(2, SERIALIZER, null, 10_000_000_002L, 0, 1, 4);
                    Assert.assertEquals(2, log.endMetadataChangeEntry());
                    log.close();
                    // TableTransactionLog recreates its physical log object on reopen.
                    try (TableTransactionLogFile reopened = newLog(partCount)) {
                        Assert.assertEquals(2, reopened.open(path));
                        try (TransactionLogCursor cursor = reopened.getCursor(0, path)) {
                            Assert.assertTrue(cursor.hasNext());
                            Assert.assertEquals(1, cursor.getStructureVersion());
                            Assert.assertEquals(-1, cursor.getWalId());
                            Assert.assertFalse(cursor.isPacked());
                            Assert.assertTrue(cursor.hasNext());
                            Assert.assertEquals(2, cursor.getStructureVersion());
                            Assert.assertEquals(0, cursor.getWalId());
                            Assert.assertEquals(4, cursor.getSegmentTxn());
                            Assert.assertTrue(cursor.isPacked());
                            Assert.assertFalse(cursor.hasNext());
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testNegativeBoundaryCannotBeReadAsLegacyHistory() throws Exception {
        assertMemoryLeak(() -> {
            for (int partCount : new int[]{0, 2}) {
                try (Path path = new Path(); TableTransactionLogFile log = newLog(partCount); MemoryCMARW header = Vm.getCMARWInstance()) {
                    path.of(root).concat("invalid-boundary-").put(partCount);
                    configuration.getFilesFacade().mkdir(path.$(), configuration.getMkDirMode());
                    int dirLen = path.size();
                    log.create(path, 10_000_000_000L);
                    log.open(path);
                    log.setPackStartTxn(1);
                    log.addEntry(0, 1, 1, 4, 10_000_000_001L, 1, 1, 1);
                    log.fdatasyncTxnLog();
                    header.smallFile(configuration.getFilesFacade(), path.concat(TXNLOG_FILE_NAME).$(), MemoryTag.MMAP_DEFAULT);
                    path.trimTo(dirLen);
                    try (TransactionLogCursor cursor = log.getCursor(0, path)) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertTrue(cursor.isPacked());
                        header.putLong(TableTransactionLogFile.PACK_START_TXN_OFFSET_64, -1);
                        CairoException error = partCount == 0
                                ? Assert.assertThrows(CairoException.class, cursor::isPacked)
                                : Assert.assertThrows(CairoException.class, cursor::extend);
                        TestUtils.assertContains(error.getFlyweightMessage(), "pack boundary");
                    }
                    if (partCount == 0) {
                        Assert.assertFalse(TableTransactionLogV1.isTailIntact(configuration.getFilesFacade(), header.getFd(), 1, 1, true));
                    }
                    CairoException cursorError = Assert.assertThrows(CairoException.class, () -> {
                        try (TransactionLogCursor ignored = log.getCursor(0, path)) {
                            Assert.fail("opened a cursor without a valid transport boundary");
                        }
                    });
                    TestUtils.assertContains(cursorError.getFlyweightMessage(), "pack boundary");
                    log.close();
                    try (TableTransactionLogFile reopened = newLog(partCount)) {
                        CairoException openError = Assert.assertThrows(CairoException.class, () -> reopened.open(path));
                        TestUtils.assertContains(openError.getFlyweightMessage(), "pack boundary");
                    }
                }
            }
        });
    }

    @Test
    public void testV1RejectsInvalidPackedOffsetsAndSchemaRollover() throws Exception {
        assertMemoryLeak(() -> {
            for (int corruption : new int[]{0, 1, 2, 3, 4}) {
                try (Path path = new Path(); TableTransactionLogFile log = newLog(0); MemoryCMARW header = Vm.getCMARWInstance()) {
                    path.of(root).concat("torn-pack-").put(corruption);
                    configuration.getFilesFacade().mkdir(path.$(), configuration.getMkDirMode());
                    int dirLen = path.size();
                    log.create(path, 10_000_000_000L);
                    log.open(path);
                    log.setPackStartTxn(1);
                    log.addEntry(0xffff_ffffL, 0, 1, 4, 10_000_000_001L, 1, 1, 1);
                    log.beginMetadataChangeEntry(0x1_0000_0000L, SERIALIZER, null, 10_000_000_002L, 0, 1, 8);
                    log.endMetadataChangeEntry();
                    log.fdatasyncTxnLog();
                    log.close();
                    header.smallFile(configuration.getFilesFacade(), path.concat(TXNLOG_FILE_NAME).$(), MemoryTag.MMAP_DEFAULT);
                    Assert.assertTrue(TableTransactionLogV1.isTailIntact(configuration.getFilesFacade(), header.getFd(), 2, 1, true));
                    long record = TableTransactionLogFile.HEADER_SIZE + TableTransactionLogV1.RECORD_SIZE;
                    if (corruption < 4) {
                        // 8-byte offset units 0..3 point into the local event-file header.
                        header.putInt(record + TableTransactionLogFile.TX_LOG_SEGMENT_TXN_OFFSET, corruption);
                    } else {
                        // A packed SCHEMA may advance the structure version once, never jump.
                        header.putLong(record + TableTransactionLogFile.TX_LOG_STRUCTURE_VERSION_OFFSET, 0x2_0000_0000L);
                    }
                    header.putLong(TableTransactionLogV1.UNSYNCED_FROM_TXN_OFFSET_64, 1);
                    Assert.assertFalse(TableTransactionLogV1.isTailIntact(configuration.getFilesFacade(), header.getFd(), 2, 1, true));
                    header.close(false);
                    path.trimTo(dirLen);
                    Assert.assertEquals(0xffff_ffffL, log.open(path));
                    Assert.assertEquals(1, log.lastTxn());
                    try (TransactionLogCursor cursor = log.getCursor(0, path)) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(0xffff_ffffL, cursor.getStructureVersion());
                        Assert.assertEquals(4, cursor.getSegmentTxn());
                        Assert.assertFalse(cursor.hasNext());
                    }
                }
            }
        });
    }

    @Test
    public void testV1RejectsZeroWalIdInLegacyTail() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path(); TableTransactionLogFile log = newLog(0); MemoryCMARW header = Vm.getCMARWInstance()) {
                path.of(root).concat("torn-legacy");
                configuration.getFilesFacade().mkdir(path.$(), configuration.getMkDirMode());
                int dirLen = path.size();
                log.create(path, 10_000_000_000L);
                log.open(path);
                log.addEntry(0, 1, 0, 0, 10_000_000_001L, 1, 1, 1);
                log.addEntry(0, 1, 0, 1, 10_000_000_002L, 1, 1, 1);
                log.fdatasyncTxnLog();
                log.close();
                header.smallFile(configuration.getFilesFacade(), path.concat(TXNLOG_FILE_NAME).$(), MemoryTag.MMAP_DEFAULT);
                Assert.assertTrue(TableTransactionLogV1.isTailIntact(configuration.getFilesFacade(), header.getFd(), 2, 1, true));
                header.putInt(TableTransactionLogFile.HEADER_SIZE + TableTransactionLogV1.RECORD_SIZE + TableTransactionLogFile.TX_LOG_WAL_ID_OFFSET, 0);
                header.putLong(TableTransactionLogV1.UNSYNCED_FROM_TXN_OFFSET_64, 1);
                Assert.assertFalse(TableTransactionLogV1.isTailIntact(configuration.getFilesFacade(), header.getFd(), 2, 1, true));
                header.close(false);
                path.trimTo(dirLen);
                Assert.assertEquals(0, log.open(path));
                Assert.assertEquals(1, log.lastTxn());
            }
        });
    }

    @Test
    public void testV2CursorRefreshesBoundaryWithoutNewTransactions() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path(); TableTransactionLogFile log = newLog(2)) {
                path.of(root).concat("cursor-boundary");
                configuration.getFilesFacade().mkdir(path.$(), configuration.getMkDirMode());
                log.create(path, 10_000_000_000L);
                log.open(path);
                log.addEntry(0, 1, 0, 0, 10_000_000_001L, 1, 1, 1);
                try (TransactionLogCursor cursor = log.getCursor(0, path)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertFalse(cursor.isPacked());
                    log.setPackStartTxn(2);
                    Assert.assertFalse(cursor.extend());
                    Assert.assertFalse(cursor.isPacked());
                    Assert.assertEquals(2, log.addEntry(0, 0, 1, 4, 10_000_000_002L, 1, 1, 1));
                    Assert.assertTrue(cursor.extend());
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(2, cursor.getTxn());
                    Assert.assertTrue(cursor.isPacked());
                    Assert.assertEquals(4, cursor.getSegmentTxn());
                    Assert.assertFalse(cursor.hasNext());
                }
            }
        });
    }

    private static TableTransactionLogFile newLog(int partCount) {
        return partCount == 0
                ? new TableTransactionLogV1(configuration, LocalDurabilityPolicy.ALWAYS_ON, () -> new long[]{0, 0})
                : new TableTransactionLogV2(configuration, partCount, DefaultWalDirectoryPolicy.INSTANCE);
    }
}
