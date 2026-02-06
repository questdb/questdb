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

import io.questdb.cairo.wal.WalWriterRingManager;
import io.questdb.cairo.wal.WalWriterRingManager.WalWriterRingColumn;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class WalWriterRingManagerTest extends AbstractTest {

    private static final IOURingFacade rf = IOURingFacadeImpl.INSTANCE;

    @Test
    public void testPackUnpackFsync() {
        for (int slot = 0; slot < 1000; slot++) {
            long packed = WalWriterRingManager.packFsyncId(slot);
            Assert.assertEquals(WalWriterRingManager.OP_FSYNC, WalWriterRingManager.unpackOpKind(packed));
            Assert.assertEquals(slot, WalWriterRingManager.unpackColumnSlot(packed));
        }
    }

    @Test
    public void testPackUnpackSnapshot() {
        for (int slot = 0; slot < 1000; slot++) {
            long packed = WalWriterRingManager.packSnapshotId(slot);
            Assert.assertEquals(WalWriterRingManager.OP_SNAPSHOT, WalWriterRingManager.unpackOpKind(packed));
            Assert.assertEquals(slot, WalWriterRingManager.unpackColumnSlot(packed));
        }
    }

    @Test
    public void testPackUnpackWriteEdgeValues() {
        // Max column slot (18 bits).
        int maxSlot = (1 << 18) - 1;
        long packed = WalWriterRingManager.packWriteId(maxSlot, 0);
        Assert.assertEquals(WalWriterRingManager.OP_WRITE, WalWriterRingManager.unpackOpKind(packed));
        Assert.assertEquals(maxSlot, WalWriterRingManager.unpackColumnSlot(packed));
        Assert.assertEquals(0, WalWriterRingManager.unpackPageId(packed));

        // Max page id (44 bits).
        long maxPage = (1L << 44) - 1;
        packed = WalWriterRingManager.packWriteId(0, maxPage);
        Assert.assertEquals(WalWriterRingManager.OP_WRITE, WalWriterRingManager.unpackOpKind(packed));
        Assert.assertEquals(0, WalWriterRingManager.unpackColumnSlot(packed));
        Assert.assertEquals(maxPage, WalWriterRingManager.unpackPageId(packed));

        // Both max.
        packed = WalWriterRingManager.packWriteId(maxSlot, maxPage);
        Assert.assertEquals(WalWriterRingManager.OP_WRITE, WalWriterRingManager.unpackOpKind(packed));
        Assert.assertEquals(maxSlot, WalWriterRingManager.unpackColumnSlot(packed));
        Assert.assertEquals(maxPage, WalWriterRingManager.unpackPageId(packed));
    }

    @Test
    public void testPackUnpackWriteRoundtrip() {
        int[] slots = {0, 1, 42, 255, 1000};
        long[] pages = {0, 1, 42, 1_000_000L, (1L << 44) - 1};

        for (int slot : slots) {
            for (long page : pages) {
                long packed = WalWriterRingManager.packWriteId(slot, page);
                Assert.assertEquals(WalWriterRingManager.OP_WRITE, WalWriterRingManager.unpackOpKind(packed));
                Assert.assertEquals(slot, WalWriterRingManager.unpackColumnSlot(packed));
                Assert.assertEquals(page, WalWriterRingManager.unpackPageId(packed));
            }
        }
    }

    @Test
    public void testWaitForAllNoOpsWhenEmpty() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            try (WalWriterRingManager mgr = new WalWriterRingManager(rf, 32)) {
                TestColumn col = new TestColumn();
                mgr.registerColumn(col);

                Assert.assertEquals(0, mgr.getInFlightCount());
                // Should return immediately without syscall or crash.
                mgr.waitForAll();
                Assert.assertEquals(0, mgr.getInFlightCount());
            }
        });
    }

    @Test
    public void testRegisterUnregister() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            try (WalWriterRingManager mgr = new WalWriterRingManager(rf, 32)) {
                TestColumn col0 = new TestColumn();
                TestColumn col1 = new TestColumn();

                int slot0 = mgr.registerColumn(col0);
                int slot1 = mgr.registerColumn(col1);

                Assert.assertEquals(0, slot0);
                Assert.assertEquals(1, slot1);

                mgr.unregisterColumn(slot0);
                // Re-register should use next sequential slot.
                TestColumn col2 = new TestColumn();
                int slot2 = mgr.registerColumn(col2);
                Assert.assertEquals(2, slot2);
            }
        });
    }

    @Test
    public void testEnqueueWriteAndDrain() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final int bufLen = 4096;
            long buf = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < bufLen; i++) {
                    Unsafe.getUnsafe().putByte(buf + i, (byte) (i & 0xFF));
                }

                File file = temp.newFile();
                try (Path path = new Path()) {
                    long fd = Files.openRW(path.of(file.getAbsolutePath()).$());
                    Assert.assertTrue(fd > -1);

                    try (WalWriterRingManager mgr = new WalWriterRingManager(rf, 32)) {
                        TestColumn col = new TestColumn();
                        int slot = mgr.registerColumn(col);

                        mgr.enqueueWrite(slot, 0, fd, 0, buf, bufLen);
                        Assert.assertEquals(1, mgr.getInFlightCount());

                        mgr.waitForAll();
                        Assert.assertEquals(0, mgr.getInFlightCount());

                        Assert.assertEquals(1, col.writeCompletedCount.get());
                        Assert.assertEquals(0, col.lastWritePageId.get());
                        Assert.assertEquals(bufLen, col.lastWriteRes.get());
                    } finally {
                        Files.close(fd);
                    }
                }
            } finally {
                Unsafe.free(buf, bufLen, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testEnqueueWriteMultipleColumns() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final int bufLen = 1024;
            long buf0 = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);
            long buf1 = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);
            try {
                File file0 = temp.newFile();
                File file1 = temp.newFile();
                try (Path path = new Path()) {
                    long fd0 = Files.openRW(path.of(file0.getAbsolutePath()).$());
                    long fd1 = Files.openRW(path.of(file1.getAbsolutePath()).$());
                    Assert.assertTrue(fd0 > -1);
                    Assert.assertTrue(fd1 > -1);

                    try (WalWriterRingManager mgr = new WalWriterRingManager(rf, 32)) {
                        TestColumn col0 = new TestColumn();
                        TestColumn col1 = new TestColumn();
                        int slot0 = mgr.registerColumn(col0);
                        int slot1 = mgr.registerColumn(col1);

                        mgr.enqueueWrite(slot0, 0, fd0, 0, buf0, bufLen);
                        mgr.enqueueWrite(slot1, 5, fd1, 0, buf1, bufLen);
                        Assert.assertEquals(2, mgr.getInFlightCount());

                        mgr.waitForAll();
                        Assert.assertEquals(0, mgr.getInFlightCount());

                        Assert.assertEquals(1, col0.writeCompletedCount.get());
                        Assert.assertEquals(0, col0.lastWritePageId.get());
                        Assert.assertEquals(bufLen, col0.lastWriteRes.get());

                        Assert.assertEquals(1, col1.writeCompletedCount.get());
                        Assert.assertEquals(5, col1.lastWritePageId.get());
                        Assert.assertEquals(bufLen, col1.lastWriteRes.get());
                    } finally {
                        Files.close(fd0);
                        Files.close(fd1);
                    }
                }
            } finally {
                Unsafe.free(buf0, bufLen, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(buf1, bufLen, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testEnqueueSnapshotWrite() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final int bufLen = 512;
            long buf = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);
            try {
                File file = temp.newFile();
                try (Path path = new Path()) {
                    long fd = Files.openRW(path.of(file.getAbsolutePath()).$());
                    Assert.assertTrue(fd > -1);

                    try (WalWriterRingManager mgr = new WalWriterRingManager(rf, 32)) {
                        TestColumn col = new TestColumn();
                        int slot = mgr.registerColumn(col);

                        mgr.enqueueSnapshotWrite(slot, fd, 0, buf, bufLen);
                        Assert.assertEquals(1, mgr.getInFlightCount());

                        mgr.waitForAll();
                        Assert.assertEquals(0, mgr.getInFlightCount());

                        // Should call onSnapshotCompleted, not onWriteCompleted.
                        Assert.assertEquals(0, col.writeCompletedCount.get());
                        Assert.assertEquals(1, col.snapshotCompletedCount.get());
                        Assert.assertEquals(bufLen, col.lastSnapshotRes.get());
                    } finally {
                        Files.close(fd);
                    }
                }
            } finally {
                Unsafe.free(buf, bufLen, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testEnqueueFsync() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            File file = temp.newFile();
            try (Path path = new Path()) {
                long fd = Files.openRW(path.of(file.getAbsolutePath()).$());
                Assert.assertTrue(fd > -1);

                try (WalWriterRingManager mgr = new WalWriterRingManager(rf, 32)) {
                    TestColumn col = new TestColumn();
                    int slot = mgr.registerColumn(col);

                    mgr.enqueueFsync(slot, fd);
                    Assert.assertEquals(1, mgr.getInFlightCount());

                    mgr.waitForAll();
                    Assert.assertEquals(0, mgr.getInFlightCount());

                    // Fsync should not dispatch to column callbacks.
                    Assert.assertEquals(0, col.writeCompletedCount.get());
                    Assert.assertEquals(0, col.snapshotCompletedCount.get());
                } finally {
                    Files.close(fd);
                }
            }
        });
    }

    @Test
    public void testErrorCqe() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final int bufLen = 1024;
            long buf = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);
            try (WalWriterRingManager mgr = new WalWriterRingManager(rf, 32)) {
                TestColumn col = new TestColumn();
                int slot = mgr.registerColumn(col);

                // Write to an invalid fd. This should produce a CQE with negative res.
                mgr.enqueueWrite(slot, 0, -1, 0, buf, bufLen);
                mgr.submitAndDrainAll();

                Assert.assertEquals(1, col.writeCompletedCount.get());
                Assert.assertTrue("CQE res should be negative for invalid fd", col.lastWriteRes.get() < 0);
            } finally {
                Unsafe.free(buf, bufLen, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testWaitForPage() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final int bufLen = 1024;
            long buf0 = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);
            long buf1 = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);
            long buf2 = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);
            try {
                File file = temp.newFile();
                try (Path path = new Path()) {
                    long fd = Files.openRW(path.of(file.getAbsolutePath()).$());
                    Assert.assertTrue(fd > -1);

                    try (WalWriterRingManager mgr = new WalWriterRingManager(rf, 32)) {
                        TestColumn col = new TestColumn();
                        int slot = mgr.registerColumn(col);

                        mgr.enqueueWrite(slot, 0, fd, 0, buf0, bufLen);
                        mgr.enqueueWrite(slot, 1, fd, bufLen, buf1, bufLen);
                        mgr.enqueueWrite(slot, 2, fd, bufLen * 2L, buf2, bufLen);

                        // Wait for page 1 specifically.
                        mgr.waitForPage(slot, 1);

                        // Page 1 should be confirmed.
                        Assert.assertTrue(col.isPageConfirmed(1));
                    } finally {
                        Files.close(fd);
                    }
                }
            } finally {
                Unsafe.free(buf0, bufLen, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(buf1, bufLen, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(buf2, bufLen, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testDistressedColumnBreaksWait() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final int bufLen = 1024;
            long buf0 = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);
            long buf1 = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);
            try {
                File file = temp.newFile();
                try (Path path = new Path()) {
                    long fd = Files.openRW(path.of(file.getAbsolutePath()).$());
                    Assert.assertTrue(fd > -1);

                    try (WalWriterRingManager mgr = new WalWriterRingManager(rf, 32)) {
                        DistressedColumn col0 = new DistressedColumn();
                        TestColumn col1 = new TestColumn();
                        int slot0 = mgr.registerColumn(col0);
                        int slot1 = mgr.registerColumn(col1);

                        // First: write to invalid fd -> triggers distress on col0.
                        mgr.enqueueWrite(slot0, 0, -1, 0, buf0, bufLen);
                        // Second: valid write on col1 to keep inFlight > 0 after first CQE.
                        mgr.enqueueWrite(slot1, 0, fd, 0, buf1, bufLen);

                        try {
                            mgr.waitForAll();
                            // If all CQEs drain quickly, waitForAll may succeed.
                            // In that case, verify the distressed state was set.
                            Assert.assertTrue("col0 should be distressed", col0.isDistressed());
                        } catch (io.questdb.cairo.CairoException e) {
                            Assert.assertTrue(e.getMessage().contains("io_uring write error"));
                        }
                    } finally {
                        Files.close(fd);
                    }
                }
            } finally {
                Unsafe.free(buf0, bufLen, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(buf1, bufLen, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    private static class TestColumn implements WalWriterRingColumn {
        final IntList confirmedPages = new IntList();
        final AtomicInteger lastSnapshotRes = new AtomicInteger();
        final AtomicLong lastWritePageId = new AtomicLong(-1);
        final AtomicInteger lastWriteRes = new AtomicInteger();
        final AtomicInteger snapshotCompletedCount = new AtomicInteger();
        final AtomicInteger writeCompletedCount = new AtomicInteger();

        @Override
        public boolean isDistressed() {
            return false;
        }

        @Override
        public boolean isPageConfirmed(long pageId) {
            for (int i = 0, n = confirmedPages.size(); i < n; i++) {
                if (confirmedPages.getQuick(i) == (int) pageId) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public void onSnapshotCompleted(int cqeRes) {
            snapshotCompletedCount.incrementAndGet();
            lastSnapshotRes.set(cqeRes);
        }

        @Override
        public void onSwapWriteError(int cqeRes) {
        }

        @Override
        public void onWriteCompleted(long pageId, int cqeRes) {
            writeCompletedCount.incrementAndGet();
            lastWritePageId.set(pageId);
            lastWriteRes.set(cqeRes);
            if (cqeRes > 0) {
                confirmedPages.add((int) pageId);
            }
        }
    }

    private static class DistressedColumn implements WalWriterRingColumn {
        private volatile boolean distressed;

        @Override
        public boolean isDistressed() {
            return distressed;
        }

        @Override
        public boolean isPageConfirmed(long pageId) {
            return false;
        }

        @Override
        public void onSnapshotCompleted(int cqeRes) {
        }

        @Override
        public void onSwapWriteError(int cqeRes) {
            distressed = true;
        }

        @Override
        public void onWriteCompleted(long pageId, int cqeRes) {
            if (cqeRes < 0) {
                distressed = true;
            }
        }
    }
}
