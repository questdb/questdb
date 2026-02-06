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

package io.questdb.test.cairo.vm;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.vm.MemoryPURImpl;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.wal.WalWriterBufferPool;
import io.questdb.cairo.wal.WalWriterRingManager;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;

public class MemoryPURImplTest extends AbstractTest {

    private static final IOURingFacade rf = IOURingFacadeImpl.INSTANCE;
    private static final FilesFacade ff = FilesFacadeImpl.INSTANCE;

    @Test
    public void testSequentialLongWrites() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = 4096;
            final int numLongs = (int) (pageSize * 3 / Long.BYTES) + 100; // Spans 3+ pages

            File file = temp.newFile();
            try (
                    Path path = new Path();
                    WalWriterRingManager mgr = new WalWriterRingManager(rf, 64);
                    WalWriterBufferPool pool = new WalWriterBufferPool(pageSize, 16, MemoryTag.NATIVE_TABLE_WAL_WRITER)
            ) {
                pool.setRingManager(mgr);
                mgr.setPool(pool);
                pool.registerWithKernel();
                try (MemoryPURImpl mem = new MemoryPURImpl(ff, path.of(file.getAbsolutePath()).$(),
                        pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER, 0, mgr, pool)) {
                    for (int i = 0; i < numLongs; i++) {
                        mem.putLong(i);
                    }
                }

                // Read file back and verify.
                long fd = Files.openRO(path.of(file.getAbsolutePath()).$());
                Assert.assertTrue(fd > -1);
                try {
                    long bufLen = numLongs * Long.BYTES;
                    long buf = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);
                    try {
                        long bytesRead = Files.read(fd, buf, bufLen, 0);
                        Assert.assertEquals(bufLen, bytesRead);
                        for (int i = 0; i < numLongs; i++) {
                            long actual = Unsafe.getUnsafe().getLong(buf + (long) i * Long.BYTES);
                            Assert.assertEquals("mismatch at index " + i, i, actual);
                        }
                    } finally {
                        Unsafe.free(buf, bufLen, MemoryTag.NATIVE_DEFAULT);
                    }
                } finally {
                    Files.close(fd);
                }
            }
        });
    }

    @Test
    public void testPutBlockOfBytesAcrossPageBoundary() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = 1024;
            final int blockSize = 512;

            File file = temp.newFile();
            try (
                    Path path = new Path();
                    WalWriterRingManager mgr = new WalWriterRingManager(rf, 64);
                    WalWriterBufferPool pool = new WalWriterBufferPool(pageSize, 16, MemoryTag.NATIVE_TABLE_WAL_WRITER)
            ) {
                pool.setRingManager(mgr);
                mgr.setPool(pool);
                pool.registerWithKernel();
                long blockBuf = Unsafe.malloc(blockSize, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < blockSize; i++) {
                        Unsafe.getUnsafe().putByte(blockBuf + i, (byte) (i & 0xFF));
                    }

                    try (MemoryPURImpl mem = new MemoryPURImpl(ff, path.of(file.getAbsolutePath()).$(),
                            pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER, 0, mgr, pool)) {
                        mem.jumpTo(pageSize - 128);
                        mem.putBlockOfBytes(blockBuf, blockSize);
                    }

                    long fd = Files.openRO(path.of(file.getAbsolutePath()).$());
                    Assert.assertTrue(fd > -1);
                    try {
                        long readBuf = Unsafe.malloc(blockSize, MemoryTag.NATIVE_DEFAULT);
                        try {
                            long bytesRead = Files.read(fd, readBuf, blockSize, pageSize - 128);
                            Assert.assertEquals(blockSize, bytesRead);
                            for (int i = 0; i < blockSize; i++) {
                                Assert.assertEquals("mismatch at byte " + i,
                                        (byte) (i & 0xFF),
                                        Unsafe.getUnsafe().getByte(readBuf + i));
                            }
                        } finally {
                            Unsafe.free(readBuf, blockSize, MemoryTag.NATIVE_DEFAULT);
                        }
                    } finally {
                        Files.close(fd);
                    }
                } finally {
                    Unsafe.free(blockBuf, blockSize, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testPageEviction() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = 1024;

            File file = temp.newFile();
            try (
                    Path path = new Path();
                    WalWriterRingManager mgr = new WalWriterRingManager(rf, 64);
                    WalWriterBufferPool pool = new WalWriterBufferPool(pageSize, 16, MemoryTag.NATIVE_TABLE_WAL_WRITER)
            ) {
                pool.setRingManager(mgr);
                mgr.setPool(pool);
                pool.registerWithKernel();
                try (MemoryPURImpl mem = new MemoryPURImpl(ff, path.of(file.getAbsolutePath()).$(),
                        pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER, 0, mgr, pool)) {
                    for (int page = 0; page < 5; page++) {
                        mem.jumpTo(page * pageSize);
                        for (int i = 0; i < pageSize / Long.BYTES; i++) {
                            mem.putLong(page * 1000L + i);
                        }
                    }

                    int currentPage = mem.pageIndex(mem.getAppendOffset() - 1);
                    long currentAddr = mem.getPageAddress(currentPage);
                    Assert.assertTrue("current page should be resident", currentAddr > 0);
                }
            }
        });
    }

    @Test
    public void testFilePreallocation() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = 4096;

            File file = temp.newFile();
            try (
                    Path path = new Path();
                    WalWriterRingManager mgr = new WalWriterRingManager(rf, 64);
                    WalWriterBufferPool pool = new WalWriterBufferPool(pageSize, 16, MemoryTag.NATIVE_TABLE_WAL_WRITER)
            ) {
                pool.setRingManager(mgr);
                mgr.setPool(pool);
                pool.registerWithKernel();
                try (MemoryPURImpl mem = new MemoryPURImpl(ff, path.of(file.getAbsolutePath()).$(),
                        pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER, 0, mgr, pool)) {
                    for (int i = 0; i < 10; i++) {
                        mem.putLong(i);
                    }
                }
            }
        });
    }

    @Test
    public void testAddressOfBoundsCheck() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = 4096;

            File file = temp.newFile();
            try (
                    Path path = new Path();
                    WalWriterRingManager mgr = new WalWriterRingManager(rf, 64);
                    WalWriterBufferPool pool = new WalWriterBufferPool(pageSize, 16, MemoryTag.NATIVE_TABLE_WAL_WRITER)
            ) {
                pool.setRingManager(mgr);
                mgr.setPool(pool);
                pool.registerWithKernel();
                try (MemoryPURImpl mem = new MemoryPURImpl(ff, path.of(file.getAbsolutePath()).$(),
                        pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER, 0, mgr, pool)) {
                    mem.putLong(42);
                    mem.putLong(84);

                    long appendOffset = mem.getAppendOffset();
                    long addr = mem.addressOf(appendOffset - 1);
                    Assert.assertTrue(addr > 0);

                    try {
                        mem.addressOf(appendOffset);
                        Assert.fail("Should throw CairoException");
                    } catch (CairoException e) {
                        Assert.assertTrue(e.getMessage().contains("addressOf beyond append offset"));
                    }
                }
            }
        });
    }

    @Test
    public void testAddressOfSubmittedPage() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = 1024;

            File file = temp.newFile();
            try (
                    Path path = new Path();
                    WalWriterRingManager mgr = new WalWriterRingManager(rf, 64);
                    WalWriterBufferPool pool = new WalWriterBufferPool(pageSize, 16, MemoryTag.NATIVE_TABLE_WAL_WRITER)
            ) {
                pool.setRingManager(mgr);
                mgr.setPool(pool);
                pool.registerWithKernel();
                try (MemoryPURImpl mem = new MemoryPURImpl(ff, path.of(file.getAbsolutePath()).$(),
                        pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER, 0, mgr, pool)) {
                    for (int i = 0; i < pageSize / Long.BYTES; i++) {
                        mem.putLong(i);
                    }
                    mem.putLong(999);

                    long val = Unsafe.getUnsafe().getLong(mem.addressOf(0));
                    Assert.assertEquals(0, val);
                    val = Unsafe.getUnsafe().getLong(mem.addressOf(8));
                    Assert.assertEquals(1, val);
                }
            }
        });
    }

    @Test
    public void testJumpToRollback() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = 1024;

            File file = temp.newFile();
            try (
                    Path path = new Path();
                    WalWriterRingManager mgr = new WalWriterRingManager(rf, 64);
                    WalWriterBufferPool pool = new WalWriterBufferPool(pageSize, 16, MemoryTag.NATIVE_TABLE_WAL_WRITER)
            ) {
                pool.setRingManager(mgr);
                mgr.setPool(pool);
                pool.registerWithKernel();
                try (MemoryPURImpl mem = new MemoryPURImpl(ff, path.of(file.getAbsolutePath()).$(),
                        pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER, 0, mgr, pool)) {
                    int longsPerPage = (int) (pageSize / Long.BYTES);
                    for (int i = 0; i < longsPerPage * 3; i++) {
                        mem.putLong(i);
                    }

                    long rollbackOffset = Long.BYTES * 10;
                    mem.jumpTo(rollbackOffset);

                    for (int i = 0; i < 5; i++) {
                        mem.putLong(1000 + i);
                    }
                }

                long fd = Files.openRO(path.of(file.getAbsolutePath()).$());
                Assert.assertTrue(fd > -1);
                try {
                    long bufLen = 15 * Long.BYTES;
                    long buf = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);
                    try {
                        long bytesRead = Files.read(fd, buf, bufLen, 0);
                        Assert.assertEquals(bufLen, bytesRead);
                        for (int i = 0; i < 10; i++) {
                            Assert.assertEquals("mismatch at " + i, i, Unsafe.getUnsafe().getLong(buf + (long) i * Long.BYTES));
                        }
                        for (int i = 0; i < 5; i++) {
                            Assert.assertEquals("mismatch at " + (10 + i), 1000 + i,
                                    Unsafe.getUnsafe().getLong(buf + (long) (10 + i) * Long.BYTES));
                        }
                    } finally {
                        Unsafe.free(buf, bufLen, MemoryTag.NATIVE_DEFAULT);
                    }
                } finally {
                    Files.close(fd);
                }
            }
        });
    }

    @Test
    public void testReadBackEvictedPage() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = 1024;

            File file = temp.newFile();
            try (
                    Path path = new Path();
                    WalWriterRingManager mgr = new WalWriterRingManager(rf, 64);
                    WalWriterBufferPool pool = new WalWriterBufferPool(pageSize, 16, MemoryTag.NATIVE_TABLE_WAL_WRITER)
            ) {
                pool.setRingManager(mgr);
                mgr.setPool(pool);
                pool.registerWithKernel();
                try (MemoryPURImpl mem = new MemoryPURImpl(ff, path.of(file.getAbsolutePath()).$(),
                        pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER, 0, mgr, pool)) {
                    int longsPerPage = (int) (pageSize / Long.BYTES);

                    for (int page = 0; page < 5; page++) {
                        for (int i = 0; i < longsPerPage; i++) {
                            mem.putLong(page * 1000L + i);
                        }
                    }

                    long val = Unsafe.getUnsafe().getLong(mem.addressOf(0));
                    Assert.assertEquals(0, val);
                    val = Unsafe.getUnsafe().getLong(mem.addressOf(8));
                    Assert.assertEquals(1, val);

                    val = Unsafe.getUnsafe().getLong(mem.addressOf(pageSize));
                    Assert.assertEquals(1000, val);
                }
            }
        });
    }

    @Test
    public void testSyncSync() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = 4096;

            File file = temp.newFile();
            try (
                    Path path = new Path();
                    WalWriterRingManager mgr = new WalWriterRingManager(rf, 64);
                    WalWriterBufferPool pool = new WalWriterBufferPool(pageSize, 16, MemoryTag.NATIVE_TABLE_WAL_WRITER)
            ) {
                pool.setRingManager(mgr);
                mgr.setPool(pool);
                pool.registerWithKernel();
                try (MemoryPURImpl mem = new MemoryPURImpl(ff, path.of(file.getAbsolutePath()).$(),
                        pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER, 0, mgr, pool)) {
                    for (int i = 0; i < 100; i++) {
                        mem.putLong(i);
                    }
                    mem.sync(false); // SYNC mode.
                }

                long fd = Files.openRO(path.of(file.getAbsolutePath()).$());
                Assert.assertTrue(fd > -1);
                try {
                    long bufLen = 100 * Long.BYTES;
                    long buf = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);
                    try {
                        long bytesRead = Files.read(fd, buf, bufLen, 0);
                        Assert.assertEquals(bufLen, bytesRead);
                        for (int i = 0; i < 100; i++) {
                            Assert.assertEquals(i, Unsafe.getUnsafe().getLong(buf + (long) i * Long.BYTES));
                        }
                    } finally {
                        Unsafe.free(buf, bufLen, MemoryTag.NATIVE_DEFAULT);
                    }
                } finally {
                    Files.close(fd);
                }
            }
        });
    }

    @Test
    public void testSyncSwapBasic() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = 4096;

            File file = temp.newFile();
            try (
                    Path path = new Path();
                    WalWriterRingManager mgr = new WalWriterRingManager(rf, 64);
                    WalWriterBufferPool pool = new WalWriterBufferPool(pageSize, 16, MemoryTag.NATIVE_TABLE_WAL_WRITER)
            ) {
                pool.setRingManager(mgr);
                mgr.setPool(pool);
                pool.registerWithKernel();
                try (MemoryPURImpl mem = new MemoryPURImpl(ff, path.of(file.getAbsolutePath()).$(),
                        pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER, 0, mgr, pool)) {
                    for (int i = 0; i < 50; i++) {
                        mem.putLong(i);
                    }
                    mem.syncSwap();
                    mgr.waitForAll();

                    // Continue writing after swap.
                    for (int i = 50; i < 100; i++) {
                        mem.putLong(i);
                    }
                }

                // Verify all data written.
                long fd = Files.openRO(path.of(file.getAbsolutePath()).$());
                Assert.assertTrue(fd > -1);
                try {
                    long bufLen = 100 * Long.BYTES;
                    long buf = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);
                    try {
                        long bytesRead = Files.read(fd, buf, bufLen, 0);
                        Assert.assertEquals(bufLen, bytesRead);
                        for (int i = 0; i < 100; i++) {
                            Assert.assertEquals("mismatch at " + i, i,
                                    Unsafe.getUnsafe().getLong(buf + (long) i * Long.BYTES));
                        }
                    } finally {
                        Unsafe.free(buf, bufLen, MemoryTag.NATIVE_DEFAULT);
                    }
                } finally {
                    Files.close(fd);
                }
            }
        });
    }

    @Test
    public void testSyncSwapSkipsClean() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = 4096;

            File file = temp.newFile();
            try (
                    Path path = new Path();
                    WalWriterRingManager mgr = new WalWriterRingManager(rf, 64);
                    WalWriterBufferPool pool = new WalWriterBufferPool(pageSize, 16, MemoryTag.NATIVE_TABLE_WAL_WRITER)
            ) {
                pool.setRingManager(mgr);
                mgr.setPool(pool);
                pool.registerWithKernel();
                try (MemoryPURImpl mem = new MemoryPURImpl(ff, path.of(file.getAbsolutePath()).$(),
                        pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER, 0, mgr, pool)) {
                    for (int i = 0; i < 50; i++) {
                        mem.putLong(i);
                    }

                    mem.syncSwap();
                    mgr.waitForAll();

                    // Second syncSwap without writes should be a no-op.
                    int freeCountBefore = pool.getFreeCount();
                    mem.syncSwap();
                    Assert.assertEquals("clean column should not consume a buffer", freeCountBefore, pool.getFreeCount());
                    Assert.assertEquals(0, mgr.getInFlightCount());
                }
            }
        });
    }

    @Test
    public void testSyncSwapMultipleOnSamePage() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = 4096;

            File file = temp.newFile();
            try (
                    Path path = new Path();
                    WalWriterRingManager mgr = new WalWriterRingManager(rf, 64);
                    WalWriterBufferPool pool = new WalWriterBufferPool(pageSize, 16, MemoryTag.NATIVE_TABLE_WAL_WRITER)
            ) {
                pool.setRingManager(mgr);
                mgr.setPool(pool);
                pool.registerWithKernel();
                try (MemoryPURImpl mem = new MemoryPURImpl(ff, path.of(file.getAbsolutePath()).$(),
                        pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER, 0, mgr, pool)) {
                    // First batch.
                    for (int i = 0; i < 10; i++) {
                        mem.putLong(i);
                    }
                    mem.syncSwap();

                    // Second batch on same page.
                    for (int i = 10; i < 20; i++) {
                        mem.putLong(i);
                    }
                    mem.syncSwap();

                    // Third batch.
                    for (int i = 20; i < 30; i++) {
                        mem.putLong(i);
                    }
                }

                // Verify all data written.
                long fd = Files.openRO(path.of(file.getAbsolutePath()).$());
                Assert.assertTrue(fd > -1);
                try {
                    long bufLen = 30 * Long.BYTES;
                    long buf = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);
                    try {
                        long bytesRead = Files.read(fd, buf, bufLen, 0);
                        Assert.assertEquals(bufLen, bytesRead);
                        for (int i = 0; i < 30; i++) {
                            Assert.assertEquals("mismatch at " + i, i,
                                    Unsafe.getUnsafe().getLong(buf + (long) i * Long.BYTES));
                        }
                    } finally {
                        Unsafe.free(buf, bufLen, MemoryTag.NATIVE_DEFAULT);
                    }
                } finally {
                    Files.close(fd);
                }
            }
        });
    }

    @Test
    public void testSyncSwapPageRollover() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = 1024;
            int longsPerPage = (int) (pageSize / Long.BYTES);

            File file = temp.newFile();
            try (
                    Path path = new Path();
                    WalWriterRingManager mgr = new WalWriterRingManager(rf, 64);
                    WalWriterBufferPool pool = new WalWriterBufferPool(pageSize, 16, MemoryTag.NATIVE_TABLE_WAL_WRITER)
            ) {
                pool.setRingManager(mgr);
                mgr.setPool(pool);
                pool.registerWithKernel();
                try (MemoryPURImpl mem = new MemoryPURImpl(ff, path.of(file.getAbsolutePath()).$(),
                        pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER, 0, mgr, pool)) {
                    // Write half of page 0.
                    for (int i = 0; i < longsPerPage / 2; i++) {
                        mem.putLong(i);
                    }
                    mem.syncSwap();

                    // Continue to fill page 0 and spill into page 1.
                    for (int i = longsPerPage / 2; i < longsPerPage + 10; i++) {
                        mem.putLong(i);
                    }
                    mem.syncSwap();

                    // Write more into page 1.
                    for (int i = longsPerPage + 10; i < longsPerPage * 2; i++) {
                        mem.putLong(i);
                    }
                }

                // Verify all data.
                long fd = Files.openRO(path.of(file.getAbsolutePath()).$());
                Assert.assertTrue(fd > -1);
                try {
                    long bufLen = longsPerPage * 2L * Long.BYTES;
                    long buf = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);
                    try {
                        long bytesRead = Files.read(fd, buf, bufLen, 0);
                        Assert.assertEquals(bufLen, bytesRead);
                        for (int i = 0; i < longsPerPage * 2; i++) {
                            Assert.assertEquals("mismatch at " + i, i,
                                    Unsafe.getUnsafe().getLong(buf + (long) i * Long.BYTES));
                        }
                    } finally {
                        Unsafe.free(buf, bufLen, MemoryTag.NATIVE_DEFAULT);
                    }
                } finally {
                    Files.close(fd);
                }
            }
        });
    }

    @Test
    public void testSyncAsync() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = 4096;

            File file = temp.newFile();
            try (
                    Path path = new Path();
                    WalWriterRingManager mgr = new WalWriterRingManager(rf, 64);
                    WalWriterBufferPool pool = new WalWriterBufferPool(pageSize, 16, MemoryTag.NATIVE_TABLE_WAL_WRITER)
            ) {
                pool.setRingManager(mgr);
                mgr.setPool(pool);
                pool.registerWithKernel();
                try (MemoryPURImpl mem = new MemoryPURImpl(ff, path.of(file.getAbsolutePath()).$(),
                        pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER, 0, mgr, pool)) {
                    for (int i = 0; i < 50; i++) {
                        mem.putLong(i);
                    }
                    mem.sync(true); // ASYNC mode — now uses syncSwap.

                    // Continue writing after async sync.
                    for (int i = 50; i < 100; i++) {
                        mem.putLong(i);
                    }
                }

                long fd = Files.openRO(path.of(file.getAbsolutePath()).$());
                Assert.assertTrue(fd > -1);
                try {
                    long bufLen = 100 * Long.BYTES;
                    long buf = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);
                    try {
                        long bytesRead = Files.read(fd, buf, bufLen, 0);
                        Assert.assertEquals(bufLen, bytesRead);
                        for (int i = 0; i < 100; i++) {
                            Assert.assertEquals("mismatch at " + i, i,
                                    Unsafe.getUnsafe().getLong(buf + (long) i * Long.BYTES));
                        }
                    } finally {
                        Unsafe.free(buf, bufLen, MemoryTag.NATIVE_DEFAULT);
                    }
                } finally {
                    Files.close(fd);
                }
            }
        });
    }

    @Test
    public void testSyncAsyncBackpressure() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = 4096;

            File file = temp.newFile();
            try (
                    Path path = new Path();
                    WalWriterRingManager mgr = new WalWriterRingManager(rf, 64);
                    WalWriterBufferPool pool = new WalWriterBufferPool(pageSize, 16, MemoryTag.NATIVE_TABLE_WAL_WRITER)
            ) {
                pool.setRingManager(mgr);
                mgr.setPool(pool);
                pool.registerWithKernel();
                try (MemoryPURImpl mem = new MemoryPURImpl(ff, path.of(file.getAbsolutePath()).$(),
                        pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER, 0, mgr, pool)) {
                    mem.putLong(42);
                    mem.sync(true);
                    mem.putLong(84);
                    mem.sync(true);
                    mem.putLong(126);
                }

                long fd = Files.openRO(path.of(file.getAbsolutePath()).$());
                Assert.assertTrue(fd > -1);
                try {
                    long bufLen = 3 * Long.BYTES;
                    long buf = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);
                    try {
                        long bytesRead = Files.read(fd, buf, bufLen, 0);
                        Assert.assertEquals(bufLen, bytesRead);
                        Assert.assertEquals(42, Unsafe.getUnsafe().getLong(buf));
                        Assert.assertEquals(84, Unsafe.getUnsafe().getLong(buf + 8));
                        Assert.assertEquals(126, Unsafe.getUnsafe().getLong(buf + 16));
                    } finally {
                        Unsafe.free(buf, bufLen, MemoryTag.NATIVE_DEFAULT);
                    }
                } finally {
                    Files.close(fd);
                }
            }
        });
    }

    @Test
    public void testSyncOnEmpty() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = 4096;

            File file = temp.newFile();
            try (
                    Path path = new Path();
                    WalWriterRingManager mgr = new WalWriterRingManager(rf, 64);
                    WalWriterBufferPool pool = new WalWriterBufferPool(pageSize, 16, MemoryTag.NATIVE_TABLE_WAL_WRITER)
            ) {
                pool.setRingManager(mgr);
                mgr.setPool(pool);
                pool.registerWithKernel();
                try (MemoryPURImpl mem = new MemoryPURImpl(ff, path.of(file.getAbsolutePath()).$(),
                        pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER, 0, mgr, pool)) {
                    mem.sync(false);
                    mem.sync(true);
                }
            }
        });
    }

    @Test
    public void testSwitchTo() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = 4096;

            File fileA = temp.newFile();
            File fileB = temp.newFile();
            try (
                    Path pathA = new Path();
                    Path pathB = new Path();
                    WalWriterRingManager mgr = new WalWriterRingManager(rf, 64);
                    WalWriterBufferPool pool = new WalWriterBufferPool(pageSize, 16, MemoryTag.NATIVE_TABLE_WAL_WRITER)
            ) {
                pool.setRingManager(mgr);
                mgr.setPool(pool);
                pool.registerWithKernel();
                long fdB = Files.openRW(pathB.of(fileB.getAbsolutePath()).$());
                Assert.assertTrue(fdB > -1);

                try (MemoryPURImpl mem = new MemoryPURImpl(ff, pathA.of(fileA.getAbsolutePath()).$(),
                        pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER, 0, mgr, pool)) {
                    for (int i = 0; i < 10; i++) {
                        mem.putLong(i);
                    }
                    mem.switchTo(ff, fdB, pageSize, 0, true, Vm.TRUNCATE_TO_POINTER);
                    for (int i = 100; i < 110; i++) {
                        mem.putLong(i);
                    }
                }

                long fdARead = Files.openRO(pathA.of(fileA.getAbsolutePath()).$());
                Assert.assertTrue(fdARead > -1);
                try {
                    long bufLen = 10 * Long.BYTES;
                    long buf = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);
                    try {
                        long bytesRead = Files.read(fdARead, buf, bufLen, 0);
                        Assert.assertEquals(bufLen, bytesRead);
                        for (int i = 0; i < 10; i++) {
                            Assert.assertEquals(i, Unsafe.getUnsafe().getLong(buf + (long) i * Long.BYTES));
                        }
                    } finally {
                        Unsafe.free(buf, bufLen, MemoryTag.NATIVE_DEFAULT);
                    }
                } finally {
                    Files.close(fdARead);
                }

                long fdBRead = Files.openRO(pathB.of(fileB.getAbsolutePath()).$());
                Assert.assertTrue(fdBRead > -1);
                try {
                    long bufLen = 10 * Long.BYTES;
                    long buf = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);
                    try {
                        long bytesRead = Files.read(fdBRead, buf, bufLen, 0);
                        Assert.assertEquals(bufLen, bytesRead);
                        for (int i = 0; i < 10; i++) {
                            Assert.assertEquals(100 + i, Unsafe.getUnsafe().getLong(buf + (long) i * Long.BYTES));
                        }
                    } finally {
                        Unsafe.free(buf, bufLen, MemoryTag.NATIVE_DEFAULT);
                    }
                } finally {
                    Files.close(fdBRead);
                }
            }
        });
    }

    @Test
    public void testSwitchToWithOffset() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = 4096;

            File fileA = temp.newFile();
            File fileB = temp.newFile();
            try (
                    Path pathA = new Path();
                    Path pathB = new Path();
                    WalWriterRingManager mgr = new WalWriterRingManager(rf, 64);
                    WalWriterBufferPool pool = new WalWriterBufferPool(pageSize, 16, MemoryTag.NATIVE_TABLE_WAL_WRITER)
            ) {
                pool.setRingManager(mgr);
                mgr.setPool(pool);
                pool.registerWithKernel();
                long fdB = Files.openRW(pathB.of(fileB.getAbsolutePath()).$());
                Assert.assertTrue(fdB > -1);

                long offset = 256;
                try (MemoryPURImpl mem = new MemoryPURImpl(ff, pathA.of(fileA.getAbsolutePath()).$(),
                        pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER, 0, mgr, pool)) {
                    mem.putLong(999);
                    mem.switchTo(ff, fdB, pageSize, offset, true, Vm.TRUNCATE_TO_POINTER);
                    mem.putLong(42);

                    Assert.assertEquals(offset + Long.BYTES, mem.getAppendOffset());
                }

                long fdBRead = Files.openRO(pathB.of(fileB.getAbsolutePath()).$());
                Assert.assertTrue(fdBRead > -1);
                try {
                    long buf = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                    try {
                        long bytesRead = Files.read(fdBRead, buf, Long.BYTES, offset);
                        Assert.assertEquals(Long.BYTES, bytesRead);
                        Assert.assertEquals(42, Unsafe.getUnsafe().getLong(buf));
                    } finally {
                        Unsafe.free(buf, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                    }
                } finally {
                    Files.close(fdBRead);
                }
            }
        });
    }

    @Test
    public void testSwitchToDrainsCqes() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = 1024;

            File fileA = temp.newFile();
            File fileB = temp.newFile();
            try (
                    Path pathA = new Path();
                    Path pathB = new Path();
                    WalWriterRingManager mgr = new WalWriterRingManager(rf, 64);
                    WalWriterBufferPool pool = new WalWriterBufferPool(pageSize, 16, MemoryTag.NATIVE_TABLE_WAL_WRITER)
            ) {
                pool.setRingManager(mgr);
                mgr.setPool(pool);
                pool.registerWithKernel();
                long fdB = Files.openRW(pathB.of(fileB.getAbsolutePath()).$());
                Assert.assertTrue(fdB > -1);

                try (MemoryPURImpl mem = new MemoryPURImpl(ff, pathA.of(fileA.getAbsolutePath()).$(),
                        pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER, 0, mgr, pool)) {
                    int longsPerPage = (int) (pageSize / Long.BYTES);
                    for (int i = 0; i < longsPerPage * 3; i++) {
                        mem.putLong(i);
                    }
                    mem.switchTo(ff, fdB, pageSize, 0, true, Vm.TRUNCATE_TO_POINTER);
                    Assert.assertEquals(0, mgr.getInFlightCount());
                    mem.putLong(42);
                }
            }
        });
    }

    @Test
    public void testSyncSubmitInPlaceSkipsCleanColumn() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = 4096;

            File file = temp.newFile();
            try (
                    Path path = new Path();
                    WalWriterRingManager mgr = new WalWriterRingManager(rf, 64);
                    WalWriterBufferPool pool = new WalWriterBufferPool(pageSize, 16, MemoryTag.NATIVE_TABLE_WAL_WRITER)
            ) {
                pool.setRingManager(mgr);
                mgr.setPool(pool);
                pool.registerWithKernel();
                try (MemoryPURImpl mem = new MemoryPURImpl(ff, path.of(file.getAbsolutePath()).$(),
                        pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER, 0, mgr, pool)) {
                    for (int i = 0; i < 50; i++) {
                        mem.putLong(i);
                    }

                    mem.syncSubmitInPlace();
                    mgr.waitForAll();
                    mem.resumeWriteAfterSync();

                    mem.syncSubmitInPlace();
                    Assert.assertEquals("clean column should skip submission", 0, mgr.getInFlightCount());
                }
            }
        });
    }

    @Test
    public void testSyncSubmitInPlaceNoDuplicateWrites() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = 4096;

            File file = temp.newFile();
            try (
                    Path path = new Path();
                    WalWriterRingManager mgr = new WalWriterRingManager(rf, 64);
                    WalWriterBufferPool pool = new WalWriterBufferPool(pageSize, 16, MemoryTag.NATIVE_TABLE_WAL_WRITER)
            ) {
                pool.setRingManager(mgr);
                mgr.setPool(pool);
                pool.registerWithKernel();
                try (MemoryPURImpl mem = new MemoryPURImpl(ff, path.of(file.getAbsolutePath()).$(),
                        pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER, 0, mgr, pool)) {
                    // Write 50 longs (400 bytes), sync, wait, resume.
                    for (int i = 0; i < 50; i++) {
                        mem.putLong(i);
                    }
                    mem.syncSubmitInPlace();
                    mgr.waitForAll();
                    mem.resumeWriteAfterSync();

                    // Write 50 more longs (400 bytes), sync again.
                    for (int i = 50; i < 100; i++) {
                        mem.putLong(i);
                    }
                    // After the fix, resumeWriteAfterSync() advances pageDirtyStart
                    // so only the new 400 bytes are submitted, not all 800 from page start.
                    mem.syncSubmitInPlace();
                    Assert.assertTrue("dirty column should submit", mgr.getInFlightCount() > 0);
                    mgr.waitForAll();
                    mem.resumeWriteAfterSync();

                    // Third sync with no new writes should be a no-op.
                    mem.syncSubmitInPlace();
                    Assert.assertEquals("clean column should skip submission", 0, mgr.getInFlightCount());
                }

                // Read file back and verify all 100 values.
                long fd = Files.openRO(path.of(file.getAbsolutePath()).$());
                Assert.assertTrue(fd > -1);
                try {
                    long bufLen = 100 * Long.BYTES;
                    long buf = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);
                    try {
                        long bytesRead = Files.read(fd, buf, bufLen, 0);
                        Assert.assertEquals(bufLen, bytesRead);
                        for (int i = 0; i < 100; i++) {
                            Assert.assertEquals("mismatch at " + i, i,
                                    Unsafe.getUnsafe().getLong(buf + (long) i * Long.BYTES));
                        }
                    } finally {
                        Unsafe.free(buf, bufLen, MemoryTag.NATIVE_DEFAULT);
                    }
                } finally {
                    Files.close(fd);
                }
            }
        });
    }

    @Test
    public void testSyncSubmitInPlaceAfterMoreWrites() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = 4096;

            File file = temp.newFile();
            try (
                    Path path = new Path();
                    WalWriterRingManager mgr = new WalWriterRingManager(rf, 64);
                    WalWriterBufferPool pool = new WalWriterBufferPool(pageSize, 16, MemoryTag.NATIVE_TABLE_WAL_WRITER)
            ) {
                pool.setRingManager(mgr);
                mgr.setPool(pool);
                pool.registerWithKernel();
                try (MemoryPURImpl mem = new MemoryPURImpl(ff, path.of(file.getAbsolutePath()).$(),
                        pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER, 0, mgr, pool)) {
                    for (int i = 0; i < 50; i++) {
                        mem.putLong(i);
                    }

                    mem.syncSubmitInPlace();
                    mgr.waitForAll();
                    mem.resumeWriteAfterSync();

                    for (int i = 50; i < 100; i++) {
                        mem.putLong(i);
                    }

                    mem.syncSubmitInPlace();
                    Assert.assertTrue("dirty column should submit", mgr.getInFlightCount() > 0);
                    mgr.waitForAll();
                    mem.resumeWriteAfterSync();
                }

                long fd = Files.openRO(path.of(file.getAbsolutePath()).$());
                Assert.assertTrue(fd > -1);
                try {
                    long bufLen = 100 * Long.BYTES;
                    long buf = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);
                    try {
                        long bytesRead = Files.read(fd, buf, bufLen, 0);
                        Assert.assertEquals(bufLen, bytesRead);
                        for (int i = 0; i < 100; i++) {
                            Assert.assertEquals("mismatch at " + i, i,
                                    Unsafe.getUnsafe().getLong(buf + (long) i * Long.BYTES));
                        }
                    } finally {
                        Unsafe.free(buf, bufLen, MemoryTag.NATIVE_DEFAULT);
                    }
                } finally {
                    Files.close(fd);
                }
            }
        });
    }

    @Test
    public void testSyncAsyncSkipsCleanViaSyncBoolean() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = 4096;

            File file = temp.newFile();
            try (
                    Path path = new Path();
                    WalWriterRingManager mgr = new WalWriterRingManager(rf, 64);
                    WalWriterBufferPool pool = new WalWriterBufferPool(pageSize, 16, MemoryTag.NATIVE_TABLE_WAL_WRITER)
            ) {
                pool.setRingManager(mgr);
                mgr.setPool(pool);
                pool.registerWithKernel();
                try (MemoryPURImpl mem = new MemoryPURImpl(ff, path.of(file.getAbsolutePath()).$(),
                        pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER, 0, mgr, pool)) {
                    for (int i = 0; i < 50; i++) {
                        mem.putLong(i);
                    }

                    mem.sync(true);
                    mgr.waitForAll();

                    mem.sync(true);
                    Assert.assertEquals("clean column should skip swap sync", 0, mgr.getInFlightCount());
                }
            }
        });
    }

    @Test
    public void testMultipleCloseIsIdempotent() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = 4096;

            File file = temp.newFile();
            try (
                    Path path = new Path();
                    WalWriterRingManager mgr = new WalWriterRingManager(rf, 64);
                    WalWriterBufferPool pool = new WalWriterBufferPool(pageSize, 16, MemoryTag.NATIVE_TABLE_WAL_WRITER)
            ) {
                pool.setRingManager(mgr);
                mgr.setPool(pool);
                pool.registerWithKernel();
                MemoryPURImpl mem = new MemoryPURImpl(ff, path.of(file.getAbsolutePath()).$(),
                        pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER, 0, mgr, pool);
                mem.putLong(42);
                mem.close();
                mem.close(); // Should be idempotent.
            }
        });
    }

    @Test
    public void testPoolBackpressure() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = 1024;
            // Small pool to force backpressure.
            final int poolSize = 4;

            File file = temp.newFile();
            try (
                    Path path = new Path();
                    WalWriterRingManager mgr = new WalWriterRingManager(rf, 64);
                    WalWriterBufferPool pool = new WalWriterBufferPool(pageSize, poolSize, MemoryTag.NATIVE_TABLE_WAL_WRITER)
            ) {
                pool.setRingManager(mgr);
                mgr.setPool(pool);
                pool.registerWithKernel();
                try (MemoryPURImpl mem = new MemoryPURImpl(ff, path.of(file.getAbsolutePath()).$(),
                        pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER, 0, mgr, pool)) {
                    // Write enough data to exhaust the pool; swap-syncs should trigger
                    // backpressure via CQE drain.
                    int longsPerPage = (int) (pageSize / Long.BYTES);
                    for (int batch = 0; batch < 6; batch++) {
                        for (int i = 0; i < longsPerPage / 4; i++) {
                            mem.putLong(batch * 1000L + i);
                        }
                        mem.syncSwap();
                    }
                }
                // If we got here without crashing, backpressure worked.
            }
        });
    }

    @Test
    public void testRollbackAfterSwap() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = 4096;

            File file = temp.newFile();
            try (
                    Path path = new Path();
                    WalWriterRingManager mgr = new WalWriterRingManager(rf, 64);
                    WalWriterBufferPool pool = new WalWriterBufferPool(pageSize, 16, MemoryTag.NATIVE_TABLE_WAL_WRITER)
            ) {
                pool.setRingManager(mgr);
                mgr.setPool(pool);
                pool.registerWithKernel();
                try (MemoryPURImpl mem = new MemoryPURImpl(ff, path.of(file.getAbsolutePath()).$(),
                        pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER, 0, mgr, pool)) {
                    // Write some data and swap-sync.
                    for (int i = 0; i < 20; i++) {
                        mem.putLong(i);
                    }
                    mem.syncSwap();
                    mgr.waitForAll();

                    // Write more.
                    for (int i = 20; i < 40; i++) {
                        mem.putLong(i);
                    }

                    // Rollback to offset 10 longs.
                    long rollbackOffset = 10 * Long.BYTES;
                    mem.jumpTo(rollbackOffset);

                    // Refresh from file — data before rollback is on disk.
                    mem.refreshCurrentPageFromFile();

                    // Write new data from rollback point.
                    for (int i = 0; i < 5; i++) {
                        mem.putLong(2000 + i);
                    }
                }

                // Verify: first 10 longs are original, then 5 new longs.
                long fd = Files.openRO(path.of(file.getAbsolutePath()).$());
                Assert.assertTrue(fd > -1);
                try {
                    long bufLen = 15 * Long.BYTES;
                    long buf = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);
                    try {
                        long bytesRead = Files.read(fd, buf, bufLen, 0);
                        Assert.assertEquals(bufLen, bytesRead);
                        for (int i = 0; i < 10; i++) {
                            Assert.assertEquals("mismatch at " + i, i,
                                    Unsafe.getUnsafe().getLong(buf + (long) i * Long.BYTES));
                        }
                        for (int i = 0; i < 5; i++) {
                            Assert.assertEquals("mismatch at " + (10 + i), 2000 + i,
                                    Unsafe.getUnsafe().getLong(buf + (long) (10 + i) * Long.BYTES));
                        }
                    } finally {
                        Unsafe.free(buf, bufLen, MemoryTag.NATIVE_DEFAULT);
                    }
                } finally {
                    Files.close(fd);
                }
            }
        });
    }
}
