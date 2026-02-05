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
                    WalWriterRingManager mgr = new WalWriterRingManager(rf, 64)
            ) {
                try (MemoryPURImpl mem = new MemoryPURImpl(ff, path.of(file.getAbsolutePath()).$(),
                        pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER, 0, mgr)) {
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
            // Write a block that straddles the page boundary.
            final int blockSize = 512;

            File file = temp.newFile();
            try (
                    Path path = new Path();
                    WalWriterRingManager mgr = new WalWriterRingManager(rf, 64)
            ) {
                long blockBuf = Unsafe.malloc(blockSize, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < blockSize; i++) {
                        Unsafe.getUnsafe().putByte(blockBuf + i, (byte) (i & 0xFF));
                    }

                    try (MemoryPURImpl mem = new MemoryPURImpl(ff, path.of(file.getAbsolutePath()).$(),
                            pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER, 0, mgr)) {
                        // Jump to near end of first page.
                        mem.jumpTo(pageSize - 128);
                        mem.putBlockOfBytes(blockBuf, blockSize);
                    }

                    // Verify file contents.
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
                    WalWriterRingManager mgr = new WalWriterRingManager(rf, 64)
            ) {
                try (MemoryPURImpl mem = new MemoryPURImpl(ff, path.of(file.getAbsolutePath()).$(),
                        pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER, 0, mgr)) {
                    // Write data across 5 pages.
                    for (int page = 0; page < 5; page++) {
                        mem.jumpTo(page * pageSize);
                        for (int i = 0; i < pageSize / Long.BYTES; i++) {
                            mem.putLong(page * 1000L + i);
                        }
                    }

                    // After writing 5 pages, earlier confirmed pages should be evicted.
                    // The current WRITING page should always be resident.
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
                    WalWriterRingManager mgr = new WalWriterRingManager(rf, 64)
            ) {
                try (MemoryPURImpl mem = new MemoryPURImpl(ff, path.of(file.getAbsolutePath()).$(),
                        pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER, 0, mgr)) {
                    // Write a few longs.
                    for (int i = 0; i < 10; i++) {
                        mem.putLong(i);
                    }
                    // File should be pre-allocated.
                    // We can't easily verify fallocate with KEEP_SIZE since visible size may be 0.
                    // But writing should not fail.
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
                    WalWriterRingManager mgr = new WalWriterRingManager(rf, 64)
            ) {
                try (MemoryPURImpl mem = new MemoryPURImpl(ff, path.of(file.getAbsolutePath()).$(),
                        pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER, 0, mgr)) {
                    mem.putLong(42);
                    mem.putLong(84);

                    long appendOffset = mem.getAppendOffset();

                    // addressOf(appendOffset - 1) should succeed.
                    long addr = mem.addressOf(appendOffset - 1);
                    Assert.assertTrue(addr > 0);

                    // addressOf(appendOffset) should throw.
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
                    WalWriterRingManager mgr = new WalWriterRingManager(rf, 64)
            ) {
                try (MemoryPURImpl mem = new MemoryPURImpl(ff, path.of(file.getAbsolutePath()).$(),
                        pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER, 0, mgr)) {
                    // Fill page 0 completely.
                    for (int i = 0; i < pageSize / Long.BYTES; i++) {
                        mem.putLong(i);
                    }
                    // This putLong triggers page boundary -> page 0 becomes SUBMITTED, page 1 allocated.
                    mem.putLong(999);

                    // Page 0 is SUBMITTED but buffer is still resident.
                    // addressOf should read from the in-memory buffer (no blocking, no pread).
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
                    WalWriterRingManager mgr = new WalWriterRingManager(rf, 64)
            ) {
                try (MemoryPURImpl mem = new MemoryPURImpl(ff, path.of(file.getAbsolutePath()).$(),
                        pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER, 0, mgr)) {
                    // Write 3 pages worth of data.
                    int longsPerPage = (int) (pageSize / Long.BYTES);
                    for (int i = 0; i < longsPerPage * 3; i++) {
                        mem.putLong(i);
                    }

                    // Roll back to middle of page 0.
                    long rollbackOffset = Long.BYTES * 10;
                    mem.jumpTo(rollbackOffset);

                    // Write new data from that offset.
                    for (int i = 0; i < 5; i++) {
                        mem.putLong(1000 + i);
                    }
                }

                // Verify file: first 10 longs are original, then 5 new longs.
                long fd = Files.openRO(path.of(file.getAbsolutePath()).$());
                Assert.assertTrue(fd > -1);
                try {
                    long bufLen = 15 * Long.BYTES;
                    long buf = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);
                    try {
                        long bytesRead = Files.read(fd, buf, bufLen, 0);
                        Assert.assertEquals(bufLen, bytesRead);
                        // First 10 longs should be original.
                        for (int i = 0; i < 10; i++) {
                            Assert.assertEquals("mismatch at " + i, i, Unsafe.getUnsafe().getLong(buf + (long) i * Long.BYTES));
                        }
                        // Next 5 longs should be the new data.
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
                    WalWriterRingManager mgr = new WalWriterRingManager(rf, 64)
            ) {
                try (MemoryPURImpl mem = new MemoryPURImpl(ff, path.of(file.getAbsolutePath()).$(),
                        pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER, 0, mgr)) {
                    int longsPerPage = (int) (pageSize / Long.BYTES);

                    // Write 5 pages to trigger eviction of earlier pages.
                    for (int page = 0; page < 5; page++) {
                        for (int i = 0; i < longsPerPage; i++) {
                            mem.putLong(page * 1000L + i);
                        }
                    }

                    // Now read back page 0 via addressOf -- should pread from disk.
                    long val = Unsafe.getUnsafe().getLong(mem.addressOf(0));
                    Assert.assertEquals(0, val);
                    val = Unsafe.getUnsafe().getLong(mem.addressOf(8));
                    Assert.assertEquals(1, val);

                    // Read from page 1.
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
                    WalWriterRingManager mgr = new WalWriterRingManager(rf, 64)
            ) {
                try (MemoryPURImpl mem = new MemoryPURImpl(ff, path.of(file.getAbsolutePath()).$(),
                        pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER, 0, mgr)) {
                    for (int i = 0; i < 100; i++) {
                        mem.putLong(i);
                    }
                    mem.sync(false); // SYNC mode.
                }

                // Verify file contents via fresh fd read.
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
    public void testSyncAsync() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = 4096;

            File file = temp.newFile();
            try (
                    Path path = new Path();
                    WalWriterRingManager mgr = new WalWriterRingManager(rf, 64)
            ) {
                try (MemoryPURImpl mem = new MemoryPURImpl(ff, path.of(file.getAbsolutePath()).$(),
                        pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER, 0, mgr)) {
                    for (int i = 0; i < 50; i++) {
                        mem.putLong(i);
                    }
                    mem.sync(true); // ASYNC mode.

                    // Continue writing after async sync (page still WRITING).
                    for (int i = 50; i < 100; i++) {
                        mem.putLong(i);
                    }
                }

                // Verify file contents.
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
                    WalWriterRingManager mgr = new WalWriterRingManager(rf, 64)
            ) {
                try (MemoryPURImpl mem = new MemoryPURImpl(ff, path.of(file.getAbsolutePath()).$(),
                        pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER, 0, mgr)) {
                    mem.putLong(42);
                    mem.sync(true); // First async sync.
                    mem.putLong(84);
                    mem.sync(true); // Second async sync -- should wait for first.
                    mem.putLong(126);
                }

                // Verify all data written correctly.
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
                    WalWriterRingManager mgr = new WalWriterRingManager(rf, 64)
            ) {
                try (MemoryPURImpl mem = new MemoryPURImpl(ff, path.of(file.getAbsolutePath()).$(),
                        pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER, 0, mgr)) {
                    mem.sync(false); // No-op sync on empty memory.
                    mem.sync(true);  // No-op async sync.
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
                    WalWriterRingManager mgr = new WalWriterRingManager(rf, 64)
            ) {
                // Open file B's fd manually for switchTo.
                long fdB = Files.openRW(pathB.of(fileB.getAbsolutePath()).$());
                Assert.assertTrue(fdB > -1);

                try (MemoryPURImpl mem = new MemoryPURImpl(ff, pathA.of(fileA.getAbsolutePath()).$(),
                        pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER, 0, mgr)) {
                    // Write to file A.
                    for (int i = 0; i < 10; i++) {
                        mem.putLong(i);
                    }

                    // Switch to file B at offset 0.
                    mem.switchTo(ff, fdB, pageSize, 0, true, Vm.TRUNCATE_TO_POINTER);

                    // Write to file B.
                    for (int i = 100; i < 110; i++) {
                        mem.putLong(i);
                    }
                }

                // Verify file A.
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

                // Verify file B.
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
                    WalWriterRingManager mgr = new WalWriterRingManager(rf, 64)
            ) {
                long fdB = Files.openRW(pathB.of(fileB.getAbsolutePath()).$());
                Assert.assertTrue(fdB > -1);

                long offset = 256;
                try (MemoryPURImpl mem = new MemoryPURImpl(ff, pathA.of(fileA.getAbsolutePath()).$(),
                        pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER, 0, mgr)) {
                    mem.putLong(999);

                    // Switch to file B at a non-zero offset.
                    mem.switchTo(ff, fdB, pageSize, offset, true, Vm.TRUNCATE_TO_POINTER);
                    mem.putLong(42);

                    Assert.assertEquals(offset + Long.BYTES, mem.getAppendOffset());
                }

                // Verify file B has data at the offset.
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
                    WalWriterRingManager mgr = new WalWriterRingManager(rf, 64)
            ) {
                long fdB = Files.openRW(pathB.of(fileB.getAbsolutePath()).$());
                Assert.assertTrue(fdB > -1);

                try (MemoryPURImpl mem = new MemoryPURImpl(ff, pathA.of(fileA.getAbsolutePath()).$(),
                        pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER, 0, mgr)) {
                    // Write data spanning multiple pages to have in-flight CQEs.
                    int longsPerPage = (int) (pageSize / Long.BYTES);
                    for (int i = 0; i < longsPerPage * 3; i++) {
                        mem.putLong(i);
                    }

                    // switchTo should drain all CQEs before freeing buffers and switching fd.
                    mem.switchTo(ff, fdB, pageSize, 0, true, Vm.TRUNCATE_TO_POINTER);

                    // Verify no in-flight operations.
                    Assert.assertEquals(0, mgr.getInFlightCount());

                    // Write to new fd.
                    mem.putLong(42);
                }
            }
        });
    }

    @Test
    public void testSyncAsyncNoSnapshotSkipsCleanColumn() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = 4096;

            File file = temp.newFile();
            try (
                    Path path = new Path();
                    WalWriterRingManager mgr = new WalWriterRingManager(rf, 64)
            ) {
                try (MemoryPURImpl mem = new MemoryPURImpl(ff, path.of(file.getAbsolutePath()).$(),
                        pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER, 0, mgr)) {
                    for (int i = 0; i < 50; i++) {
                        mem.putLong(i);
                    }

                    // First no-snapshot sync: should submit data.
                    mem.syncAsyncNoSnapshot();
                    mgr.waitForAll();
                    mem.resumeWriteAfterSync();

                    // Second no-snapshot sync without new writes: should be a no-op.
                    mem.syncAsyncNoSnapshot();
                    Assert.assertEquals("clean column should skip submission", 0, mgr.getInFlightCount());
                    // No waitForAll/resumeWriteAfterSync needed since nothing was submitted.
                }
            }
        });
    }

    @Test
    public void testSyncAsyncNoSnapshotAfterMoreWrites() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = 4096;

            File file = temp.newFile();
            try (
                    Path path = new Path();
                    WalWriterRingManager mgr = new WalWriterRingManager(rf, 64)
            ) {
                try (MemoryPURImpl mem = new MemoryPURImpl(ff, path.of(file.getAbsolutePath()).$(),
                        pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER, 0, mgr)) {
                    for (int i = 0; i < 50; i++) {
                        mem.putLong(i);
                    }

                    // First sync cycle.
                    mem.syncAsyncNoSnapshot();
                    mgr.waitForAll();
                    mem.resumeWriteAfterSync();

                    // Write more data.
                    for (int i = 50; i < 100; i++) {
                        mem.putLong(i);
                    }

                    // Second sync should submit new data.
                    mem.syncAsyncNoSnapshot();
                    Assert.assertTrue("dirty column should submit", mgr.getInFlightCount() > 0);
                    mgr.waitForAll();
                    mem.resumeWriteAfterSync();
                }

                // Verify all 100 longs written to disk.
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
    public void testSyncAsyncSkipsCleanColumn() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final long pageSize = 4096;

            File file = temp.newFile();
            try (
                    Path path = new Path();
                    WalWriterRingManager mgr = new WalWriterRingManager(rf, 64)
            ) {
                try (MemoryPURImpl mem = new MemoryPURImpl(ff, path.of(file.getAbsolutePath()).$(),
                        pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER, 0, mgr)) {
                    for (int i = 0; i < 50; i++) {
                        mem.putLong(i);
                    }

                    // First snapshot-based async sync.
                    mem.sync(true);
                    mgr.waitForAll();

                    // Second sync(true) without new writes: should be a no-op.
                    mem.sync(true);
                    Assert.assertEquals("clean column should skip snapshot sync", 0, mgr.getInFlightCount());
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
                    WalWriterRingManager mgr = new WalWriterRingManager(rf, 64)
            ) {
                MemoryPURImpl mem = new MemoryPURImpl(ff, path.of(file.getAbsolutePath()).$(),
                        pageSize, MemoryTag.NATIVE_TABLE_WAL_WRITER, 0, mgr);
                mem.putLong(42);
                mem.close();
                mem.close(); // Should be idempotent.
            }
        });
    }
}
