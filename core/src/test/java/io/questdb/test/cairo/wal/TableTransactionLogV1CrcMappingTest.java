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

package io.questdb.test.cairo.wal;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.seq.TableTransactionLogV1;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.cairo.wal.seq.TxnLogCrcSidecar;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.LongHashSet;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

public class TableTransactionLogV1CrcMappingTest extends AbstractCairoTest {
    private int directoryIndex;

    @Test
    public void testConcurrentAppendAndCursorGrowth() throws Exception {
        withLog(false, (log, path, ff) -> {
            append(log, 1);
            try (TransactionLogCursor cursor = log.getCursor(0, path)) {
                assertTransactions(cursor, 1, 1);
                AtomicReference<Throwable> failure = new AtomicReference<>();
                Thread writer = new Thread(() -> {
                    try {
                        append(log, 20_000);
                    } catch (Throwable th) {
                        failure.set(th);
                    }
                });
                int reads = ff.readCount;
                writer.start();
                try {
                    long deadline = System.nanoTime() + 30_000_000_000L;
                    for (int txn = 2; txn <= 20_001; txn++) {
                        while (!cursor.hasNext()) {
                            if (failure.get() != null) {
                                throw new AssertionError(failure.get());
                            }
                            Assert.assertTrue("writer did not publish the transaction", System.nanoTime() < deadline);
                            Thread.yield();
                        }
                        Assert.assertEquals(txn, cursor.getTxn());
                        Assert.assertEquals(txn, cursor.getCommitTimestamp());
                        Assert.assertEquals(txn - 1, cursor.getSegmentTxn());
                    }
                } finally {
                    writer.join();
                }
                Assert.assertNull(failure.get());
                Assert.assertEquals(reads, ff.readCount);
                Assert.assertTrue(ff.remapCount > 0);
            }
        });
    }

    @Test
    public void testCursorGrowth() throws Exception {
        assertGrowth(false);
    }

    @Test
    public void testExplicitExtend() throws Exception {
        assertGrowth(true);
    }

    @Test
    public void testLengthFailureFallsBackToRead() throws Exception {
        withLog(false, (log, path, ff) -> {
            append(log, 3);
            ff.isLengthFailure = true;
            try (TransactionLogCursor cursor = log.getCursor(0, path)) {
                int reads = ff.readCount;
                assertTransactions(cursor, 1, 3);
                Assert.assertEquals(3, ff.readCount - reads);
            }
        });
    }

    @Test
    public void testMmapFailureStillDetectsCorruption() throws Exception {
        withLog(false, (log, path, ff) -> {
            append(log, 2);
            writeSidecarLong(path, TxnLogCrcSidecar.BODY_OFFSET, 0);
            ff.isMmapFailure = true;
            try (TransactionLogCursor cursor = log.getCursor(0, path)) {
                int reads = ff.readCount;
                assertTorn(cursor);
                Assert.assertEquals(1, ff.mapCount);
                Assert.assertEquals(1, ff.readCount - reads);
            }
        });
    }

    @Test
    public void testNoPerEntryIO() throws Exception {
        assertNoPerEntryIO(false);
    }

    @Test
    public void testNoPerEntryIOWithFdCacheBypassed() throws Exception {
        assertNoPerEntryIO(true);
    }

    @Test
    public void testRemapFailureFallsBackAndRetries() throws Exception {
        withLog(false, (log, path, ff) -> {
            append(log, 1);
            try (TransactionLogCursor cursor = log.getCursor(0, path)) {
                assertTransactions(cursor, 1, 1);
                int count = (int) (2 * ff.getPageSize() / TxnLogCrcSidecar.ENTRY_SIZE);
                append(log, count);
                ff.isMmapFailure = true;
                int reads = ff.readCount;
                assertTransactions(cursor, 2, count + 1);
                Assert.assertEquals(count, ff.readCount - reads);
                Assert.assertEquals(1, ff.remapCount);

                ff.isMmapFailure = false;
                append(log, 1);
                reads = ff.readCount;
                assertTransactions(cursor, count + 2, count + 2);
                Assert.assertEquals(reads, ff.readCount);
                Assert.assertEquals(2, ff.remapCount);

                // Rewinding must verify again, not trust a verdict cached by the first pass.
                writeSidecarLong(path, TxnLogCrcSidecar.BODY_OFFSET, 0);
                cursor.toTop();
                assertTorn(cursor);
                Assert.assertEquals(reads, ff.readCount);
            }
        });
    }

    @Test
    public void testTruncatedSidecar() throws Exception {
        for (long size : new long[]{0, 8, 23, 24, 31, 32, 47, 48, 56}) {
            withLog(false, (log, path, ff) -> {
                append(log, 3);
                log.close();
                int len = path.size();
                long fd = ff.openRW(path.concat(WalUtils.TXNLOG_CRC_FILE_NAME).$(), CairoConfiguration.O_NONE);
                path.trimTo(len);
                Assert.assertTrue(fd > -1);
                try {
                    Assert.assertTrue(ff.truncate(fd, size));
                } finally {
                    ff.close(fd);
                }
                try (TransactionLogCursor cursor = log.getCursor(0, path)) {
                    int reads = ff.readCount;
                    assertTransactions(cursor, 1, 3);
                    Assert.assertFalse(cursor.hasNext());
                    Assert.assertEquals(reads, ff.readCount);
                }
            });
        }
    }

    @Test
    public void testWatermarkAndStampGate() throws Exception {
        withLog(false, (log, path, ff) -> {
            append(log, 3);
            log.close();
            int len = path.size();
            path.concat(WalUtils.TXNLOG_CRC_FILE_NAME);
            ff.remove(path.$());
            Assert.assertFalse(ff.exists(path.$()));
            path.trimTo(len);
            log.open(path);
            append(log, 2);
            // The new sidecar starts at txn 4, not txn 1. A missing stamp must still skip the CRC.
            writeSidecarLong(path, TxnLogCrcSidecar.BODY_OFFSET, 0);
            writeSidecarLong(path, TxnLogCrcSidecar.BODY_OFFSET + TxnLogCrcSidecar.ENTRY_STAMP_OFFSET, 0);
            try (TransactionLogCursor cursor = log.getCursor(0, path)) {
                assertTransactions(cursor, 1, 5);
                writeSidecarLong(path, TxnLogCrcSidecar.BODY_OFFSET + TxnLogCrcSidecar.ENTRY_STAMP_OFFSET, 4);
                cursor.toTop();
                assertTransactions(cursor, 1, 3);
                assertTorn(cursor);
            }
        });
    }

    @Test
    public void testWriterCloseRetainsPublishedPrefix() throws Exception {
        withLog(false, (log, path, ff) -> {
            append(log, 3);
            try (TransactionLogCursor cursor = log.getCursor(0, path)) {
                assertTransactions(cursor, 1, 1);
                // close trims unused preallocation, but must not invalidate mapped published entries.
                log.close();
                assertTransactions(cursor, 2, 3);
                cursor.toTop();
                assertTransactions(cursor, 1, 3);
                Assert.assertFalse(cursor.hasNext());
            }
        });
    }

    private static void append(TableTransactionLogV1 log, int count) {
        for (int i = 0; i < count; i++) {
            long txn = log.lastTxn() + 1;
            Assert.assertEquals(txn, log.addEntry(0, 1, 0, (int) txn - 1, txn, 0, 0, 1));
        }
    }

    private static void assertTorn(TransactionLogCursor cursor) {
        try {
            cursor.hasNext();
            Assert.fail("expected a checksum mismatch");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "sequencer txnlog record");
        }
    }

    private static void assertTransactions(TransactionLogCursor cursor, int first, int last) {
        for (int txn = first; txn <= last; txn++) {
            Assert.assertTrue(cursor.hasNext());
            Assert.assertEquals(txn, cursor.getTxn());
            Assert.assertEquals(txn, cursor.getCommitTimestamp());
            Assert.assertEquals(txn - 1, cursor.getSegmentTxn());
            Assert.assertEquals(1, cursor.getWalId());
        }
    }

    private static void writeSidecarLong(Path path, long offset, long value) {
        FilesFacade ff = FilesFacadeImpl.INSTANCE;
        int len = path.size();
        long fd = ff.openRW(path.concat(WalUtils.TXNLOG_CRC_FILE_NAME).$(), CairoConfiguration.O_NONE);
        path.trimTo(len);
        Assert.assertTrue(fd > -1);
        long buf = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putLong(buf, value);
            Assert.assertEquals(Long.BYTES, ff.write(fd, buf, Long.BYTES, offset));
        } finally {
            Unsafe.free(buf, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            ff.close(fd);
        }
    }

    private void assertGrowth(boolean explicitExtend) throws Exception {
        withLog(false, (log, path, ff) -> {
            append(log, 1);
            try (TransactionLogCursor cursor = log.getCursor(0, path)) {
                assertTransactions(cursor, 1, 1);
                int count = (int) (2 * ff.getPageSize() / TxnLogCrcSidecar.ENTRY_SIZE);
                append(log, count);
                int reads = ff.readCount;
                if (explicitExtend) {
                    Assert.assertTrue(cursor.extend());
                    cursor.toTop();
                    assertTransactions(cursor, 1, count + 1);
                } else {
                    assertTransactions(cursor, 2, count + 1);
                }
                Assert.assertEquals(reads, ff.readCount);
                Assert.assertEquals(1, ff.remapCount);
                Assert.assertFalse(cursor.hasNext());
                Assert.assertFalse(cursor.extend());
            }
        });
    }

    private void assertNoPerEntryIO(boolean bypassFdCache) throws Exception {
        withLog(bypassFdCache, (log, path, ff) -> {
            append(log, 256);
            try (TransactionLogCursor cursor = log.getCursor(0, path)) {
                int reads = ff.readCount;
                int lengths = ff.lengthCount;
                for (int pass = 0; pass < 2; pass++) {
                    cursor.toTop();
                    assertTransactions(cursor, 1, 256);
                    Assert.assertFalse(cursor.hasNext());
                }
                Assert.assertEquals("cursor entries must not issue pread", reads, ff.readCount);
                Assert.assertEquals("cursor entries must not issue fstat", lengths, ff.lengthCount);
                Assert.assertEquals(1, ff.mapCount);
                Assert.assertEquals(0, ff.remapCount);
                Assert.assertEquals(bypassFdCache ? 1 : 0, ff.bypassedOpenCount);
            }
        });
    }

    private void withLog(boolean bypassFdCache, CursorTest test) throws Exception {
        assertMemoryLeak(() -> {
            TrackingFilesFacade ff = new TrackingFilesFacade();
            CairoConfiguration cfg = new CairoConfigurationWrapper(configuration) {
                @Override
                public boolean getBypassWalFdCache() {
                    return bypassFdCache;
                }

                @Override
                public int getCommitMode() {
                    return CommitMode.NOSYNC;
                }

                @Override
                public FilesFacade getFilesFacade() {
                    return ff;
                }
            };
            try (Path path = new Path(); TableTransactionLogV1 log = new TableTransactionLogV1(cfg)) {
                path.of(root).concat("crc_mapping").put(directoryIndex++);
                Assert.assertEquals(0, ff.mkdir(path.$(), configuration.getMkDirMode()));
                log.create(path, 1);
                log.open(path);
                test.run(log, path, ff);
            }
            Assert.assertEquals(0, ff.crcFds.size());
            Assert.assertEquals(0, ff.crcMappings.size());
        });
    }

    @FunctionalInterface
    private interface CursorTest {
        void run(TableTransactionLogV1 log, Path path, TrackingFilesFacade ff) throws Exception;
    }

    private static class TrackingFilesFacade extends FilesFacadeImpl {
        private final LongHashSet crcFds = new LongHashSet();
        private final LongHashSet crcMappings = new LongHashSet();
        private int bypassedOpenCount;
        private boolean isLengthFailure;
        private boolean isMmapFailure;
        private int lengthCount;
        private int mapCount;
        private int readCount;
        private int remapCount;

        @Override
        public boolean close(long fd) {
            crcFds.remove(fd);
            return super.close(fd);
        }

        @Override
        public long length(long fd) {
            if (crcFds.contains(fd)) {
                lengthCount++;
                if (isLengthFailure) {
                    return -1;
                }
            }
            return super.length(fd);
        }

        @Override
        public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
            boolean isCrc = crcFds.contains(fd);
            if (isCrc) {
                mapCount++;
                Assert.assertEquals(Files.MAP_RO, flags);
                Assert.assertTrue(len > 0 && len <= super.length(fd));
                if (isMmapFailure) {
                    return MAP_FAILED;
                }
            }
            long address = super.mmap(fd, len, offset, flags, memoryTag);
            if (isCrc && address != MAP_FAILED) {
                crcMappings.add(address);
            }
            return address;
        }

        @Override
        public long mremap(long fd, long address, long previousSize, long newSize, long offset, int flags, int memoryTag) {
            boolean isCrc = crcFds.contains(fd);
            if (isCrc) {
                remapCount++;
                Assert.assertEquals(Files.MAP_RO, flags);
                Assert.assertTrue(newSize > 0 && newSize <= super.length(fd));
                if (isMmapFailure) {
                    return MAP_FAILED;
                }
            }
            long newAddress = super.mremap(fd, address, previousSize, newSize, offset, flags, memoryTag);
            if (isCrc && newAddress != MAP_FAILED) {
                crcMappings.remove(address);
                crcMappings.add(newAddress);
            }
            return newAddress;
        }

        @Override
        public void munmap(long address, long size, int memoryTag) {
            crcMappings.remove(address);
            super.munmap(address, size, memoryTag);
        }

        @Override
        public long openRO(LPSZ path) {
            long fd = super.openRO(path);
            if (fd > -1 && Utf8s.endsWithAscii(path, WalUtils.TXNLOG_CRC_FILE_NAME)) {
                crcFds.add(fd);
            }
            return fd;
        }

        @Override
        public long openRONoCache(LPSZ path) {
            long fd = super.openRONoCache(path);
            if (fd > -1 && Utf8s.endsWithAscii(path, WalUtils.TXNLOG_CRC_FILE_NAME)) {
                crcFds.add(fd);
                bypassedOpenCount++;
            }
            return fd;
        }

        @Override
        public long read(long fd, long buf, long len, long offset) {
            if (crcFds.contains(fd)) {
                readCount++;
            }
            return super.read(fd, buf, len, offset);
        }
    }
}
