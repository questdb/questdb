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

package io.questdb.test.std;

import io.questdb.cairo.CairoException;
import io.questdb.std.*;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;

public class IOURingImplTest extends AbstractTest {

    private static final IOURingFacade rf = new IOURingFacadeImpl();

    @Test
    public void testFallocateKeepSize() throws Exception {
        Assume.assumeTrue(Os.isLinux());

        TestUtils.assertMemoryLeak(() -> {
            File file = temp.newFile();
            try (Path path = new Path()) {
                long fd = Files.openRW(path.of(file.getAbsolutePath()).$());
                Assert.assertTrue(fd > -1);

                try {
                    long allocSize = 1024 * 1024; // 1 MB
                    boolean ok = Files.fallocateKeepSize(fd, 0, allocSize);
                    Assert.assertTrue("fallocateKeepSize should succeed", ok);

                    // Visible file size should remain 0 (FALLOC_FL_KEEP_SIZE).
                    Assert.assertEquals(0, Files.length(fd));

                    // Write into the pre-allocated region via pwrite and verify.
                    final int bufLen = 4096;
                    long buf = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);
                    try {
                        for (int i = 0; i < bufLen; i++) {
                            Unsafe.getUnsafe().putByte(buf + i, (byte) 0xAB);
                        }
                        long written = Files.write(fd, buf, bufLen, 0);
                        Assert.assertEquals(bufLen, written);

                        // Read back and verify.
                        long readBuf = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);
                        try {
                            long bytesRead = Files.read(fd, readBuf, bufLen, 0);
                            Assert.assertEquals(bufLen, bytesRead);
                            for (int i = 0; i < bufLen; i++) {
                                Assert.assertEquals((byte) 0xAB, Unsafe.getUnsafe().getByte(readBuf + i));
                            }
                        } finally {
                            Unsafe.free(readBuf, bufLen, MemoryTag.NATIVE_DEFAULT);
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

    @Test(expected = CairoException.class)
    public void testFailsToInit() {
        final IOURingFacade rf = new IOURingFacadeImpl() {
            @Override
            public long create(int capacity, int flags) {
                return -42;
            }
        };
        try (IOURing ignored = rf.newInstance(32)) {
            Assert.fail();
        }
    }

    @Test
    public void testIsAvailableOn() {
        Assert.assertFalse(IOURingFacadeImpl.isAvailableOn("6.1"));
        Assert.assertFalse(IOURingFacadeImpl.isAvailableOn("4.0.0"));
        Assert.assertFalse(IOURingFacadeImpl.isAvailableOn("5.0.0"));
        Assert.assertFalse(IOURingFacadeImpl.isAvailableOn("5.1.0"));
        Assert.assertFalse(IOURingFacadeImpl.isAvailableOn("5.10.2"));

        Assert.assertTrue(IOURingFacadeImpl.isAvailableOn("5.12.0"));
        Assert.assertTrue(IOURingFacadeImpl.isAvailableOn("5.14.0-1044-oem"));
        Assert.assertTrue(IOURingFacadeImpl.isAvailableOn("5.13.1"));
        Assert.assertTrue(IOURingFacadeImpl.isAvailableOn("6.2.2"));
        Assert.assertTrue(IOURingFacadeImpl.isAvailableOn("7.1.1"));
    }

    @Test
    public void testFsync() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final int bufLen = 4096;
            long writeBuf = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < bufLen; i++) {
                    Unsafe.getUnsafe().putByte(writeBuf + i, (byte) (i & 0xFF));
                }

                File file = temp.newFile();
                try (Path path = new Path()) {
                    long fd = Files.openRW(path.of(file.getAbsolutePath()).$());
                    Assert.assertTrue(fd > -1);

                    try (IOURing ring = rf.newInstance(4)) {
                        // Write then fsync.
                        long writeId = ring.enqueueWrite(fd, 0, writeBuf, bufLen);
                        Assert.assertTrue(writeId > -1);
                        long fsyncId = ring.enqueueFsync(fd);
                        Assert.assertTrue(fsyncId > -1);

                        int submitted = ring.submitAndWait();
                        Assert.assertEquals(2, submitted);

                        // Drain both CQEs.
                        int cqeCount = 0;
                        for (int i = 0; i < 2; i++) {
                            while (!ring.nextCqe()) {
                                Os.pause();
                            }
                            Assert.assertTrue("CQE res should be >= 0, got " + ring.getCqeRes(), ring.getCqeRes() >= 0);
                            cqeCount++;
                        }
                        Assert.assertEquals(2, cqeCount);
                    } finally {
                        Files.close(fd);
                    }
                }
            } finally {
                Unsafe.free(writeBuf, bufLen, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testFsyncWithExplicitUserData() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            File file = temp.newFile();
            try (Path path = new Path()) {
                long fd = Files.openRW(path.of(file.getAbsolutePath()).$());
                Assert.assertTrue(fd > -1);

                try (IOURing ring = rf.newInstance(4)) {
                    long expectedId = 0xCAFEBABEL;
                    ring.enqueueFsync(fd, expectedId);

                    ring.submitAndWait();
                    Assert.assertTrue(ring.nextCqe());
                    Assert.assertEquals(expectedId, ring.getCqeId());
                    Assert.assertTrue(ring.getCqeRes() >= 0);
                } finally {
                    Files.close(fd);
                }
            }
        });
    }

    @Test
    public void testWrite() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final int bufLen = 4096;
            long writeBuf = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);
            long readBuf = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);
            try {
                // Fill write buffer with a known pattern.
                for (int i = 0; i < bufLen; i++) {
                    Unsafe.getUnsafe().putByte(writeBuf + i, (byte) (i & 0xFF));
                }

                File file = temp.newFile();
                try (Path path = new Path()) {
                    long fd = Files.openRW(path.of(file.getAbsolutePath()).$());
                    Assert.assertTrue(fd > -1);

                    try (IOURing ring = rf.newInstance(4)) {
                        // Write via io_uring.
                        long id = ring.enqueueWrite(fd, 0, writeBuf, bufLen);
                        Assert.assertTrue(id > -1);

                        int submitted = ring.submitAndWait();
                        Assert.assertEquals(1, submitted);

                        Assert.assertTrue(ring.nextCqe());
                        Assert.assertEquals(id, ring.getCqeId());
                        Assert.assertEquals(bufLen, ring.getCqeRes());

                        // Read back via io_uring and verify.
                        long readId = ring.enqueueRead(fd, 0, readBuf, bufLen);
                        Assert.assertTrue(readId > -1);

                        ring.submitAndWait();
                        Assert.assertTrue(ring.nextCqe());
                        Assert.assertEquals(readId, ring.getCqeId());
                        Assert.assertEquals(bufLen, ring.getCqeRes());

                        for (int i = 0; i < bufLen; i++) {
                            Assert.assertEquals(
                                    "mismatch at byte " + i,
                                    (byte) (i & 0xFF),
                                    Unsafe.getUnsafe().getByte(readBuf + i)
                            );
                        }
                    } finally {
                        Files.close(fd);
                    }
                }
            } finally {
                Unsafe.free(writeBuf, bufLen, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(readBuf, bufLen, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testWriteWithExplicitUserData() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final int bufLen = 1024;
            long writeBuf = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < bufLen; i++) {
                    Unsafe.getUnsafe().putByte(writeBuf + i, (byte) 42);
                }

                File file = temp.newFile();
                try (Path path = new Path()) {
                    long fd = Files.openRW(path.of(file.getAbsolutePath()).$());
                    Assert.assertTrue(fd > -1);

                    try (IOURing ring = rf.newInstance(4)) {
                        long expectedId = 0xDEADBEEFL;
                        ring.enqueueWrite(fd, 0, writeBuf, bufLen, expectedId);

                        ring.submitAndWait();
                        Assert.assertTrue(ring.nextCqe());
                        Assert.assertEquals(expectedId, ring.getCqeId());
                        Assert.assertEquals(bufLen, ring.getCqeRes());
                    } finally {
                        Files.close(fd);
                    }
                }
            } finally {
                Unsafe.free(writeBuf, bufLen, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testWriteReadBackWithPread() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final int bufLen = 8192;
            long writeBuf = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);
            long readBuf = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < bufLen; i++) {
                    Unsafe.getUnsafe().putByte(writeBuf + i, (byte) ((i * 7) & 0xFF));
                }

                File file = temp.newFile();
                try (Path path = new Path()) {
                    long fd = Files.openRW(path.of(file.getAbsolutePath()).$());
                    Assert.assertTrue(fd > -1);

                    try (IOURing ring = rf.newInstance(4)) {
                        long id = ring.enqueueWrite(fd, 0, writeBuf, bufLen);
                        Assert.assertTrue(id > -1);

                        ring.submitAndWait();
                        Assert.assertTrue(ring.nextCqe());
                        Assert.assertEquals(id, ring.getCqeId());
                        Assert.assertEquals(bufLen, ring.getCqeRes());
                    }

                    // Read back with Files.read (pread) instead of io_uring.
                    long bytesRead = Files.read(fd, readBuf, bufLen, 0);
                    Assert.assertEquals(bufLen, bytesRead);

                    for (int i = 0; i < bufLen; i++) {
                        Assert.assertEquals(
                                "mismatch at byte " + i,
                                (byte) ((i * 7) & 0xFF),
                                Unsafe.getUnsafe().getByte(readBuf + i)
                        );
                    }

                    Files.close(fd);
                }
            } finally {
                Unsafe.free(writeBuf, bufLen, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(readBuf, bufLen, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testRegisterAndUnregisterBuffers() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final int bufCount = 4;
            final int bufLen = 4096;
            long[] bufs = new long[bufCount];
            for (int i = 0; i < bufCount; i++) {
                bufs[i] = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);
            }

            // Build native iovec array: 16 bytes per entry (8 iov_base + 8 iov_len).
            long iovecSize = 16L;
            long iovsAddr = Unsafe.malloc(iovecSize * bufCount, MemoryTag.NATIVE_DEFAULT);
            try (IOURing ring = rf.newInstance(4)) {
                for (int i = 0; i < bufCount; i++) {
                    Unsafe.getUnsafe().putLong(iovsAddr + i * iovecSize, bufs[i]);
                    Unsafe.getUnsafe().putLong(iovsAddr + i * iovecSize + 8, bufLen);
                }

                int ret = ring.registerBuffers(iovsAddr, bufCount);
                Assert.assertEquals("registerBuffers should return 0", 0, ret);

                ret = ring.unregisterBuffers();
                Assert.assertEquals("unregisterBuffers should return 0", 0, ret);
            } finally {
                Unsafe.free(iovsAddr, iovecSize * bufCount, MemoryTag.NATIVE_DEFAULT);
                for (int i = 0; i < bufCount; i++) {
                    Unsafe.free(bufs[i], bufLen, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testRegisterBuffersDoubleRegisterFails() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final int bufLen = 4096;
            long buf = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);
            long iovecSize = 16L;
            long iovsAddr = Unsafe.malloc(iovecSize, MemoryTag.NATIVE_DEFAULT);
            try (IOURing ring = rf.newInstance(4)) {
                Unsafe.getUnsafe().putLong(iovsAddr, buf);
                Unsafe.getUnsafe().putLong(iovsAddr + 8, bufLen);

                int ret = ring.registerBuffers(iovsAddr, 1);
                Assert.assertEquals(0, ret);

                // Second register without unregister should fail with -EBUSY.
                ret = ring.registerBuffers(iovsAddr, 1);
                Assert.assertTrue("double register should fail", ret < 0);

                ring.unregisterBuffers();
            } finally {
                Unsafe.free(iovsAddr, iovecSize, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(buf, bufLen, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testWriteFixedBasic() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final int bufLen = 4096;
            long buf = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);
            long readBuf = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < bufLen; i++) {
                    Unsafe.getUnsafe().putByte(buf + i, (byte) (i & 0xFF));
                }

                // Build and register iovec.
                long iovecSize = 16L;
                long iovsAddr = Unsafe.malloc(iovecSize, MemoryTag.NATIVE_DEFAULT);

                File file = temp.newFile();
                try (Path path = new Path()) {
                    long fd = Files.openRW(path.of(file.getAbsolutePath()).$());
                    Assert.assertTrue(fd > -1);

                    try (IOURing ring = rf.newInstance(4)) {
                        Unsafe.getUnsafe().putLong(iovsAddr, buf);
                        Unsafe.getUnsafe().putLong(iovsAddr + 8, bufLen);
                        Assert.assertEquals(0, ring.registerBuffers(iovsAddr, 1));

                        long expectedId = 0xCAFEL;
                        ring.enqueueWriteFixed(fd, 0, buf, bufLen, 0, expectedId);

                        ring.submitAndWait();
                        Assert.assertTrue(ring.nextCqe());
                        Assert.assertEquals(expectedId, ring.getCqeId());
                        Assert.assertEquals(bufLen, ring.getCqeRes());

                        ring.unregisterBuffers();
                    } finally {
                        Files.close(fd);
                    }

                    // Read back with pread and verify.
                    long fdRead = Files.openRO(path.of(file.getAbsolutePath()).$());
                    try {
                        long bytesRead = Files.read(fdRead, readBuf, bufLen, 0);
                        Assert.assertEquals(bufLen, bytesRead);
                        for (int i = 0; i < bufLen; i++) {
                            Assert.assertEquals(
                                    "mismatch at byte " + i,
                                    (byte) (i & 0xFF),
                                    Unsafe.getUnsafe().getByte(readBuf + i)
                            );
                        }
                    } finally {
                        Files.close(fdRead);
                    }
                }
                Unsafe.free(iovsAddr, iovecSize, MemoryTag.NATIVE_DEFAULT);
            } finally {
                Unsafe.free(buf, bufLen, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(readBuf, bufLen, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testWriteFixedMultipleBuffers() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final int bufCount = 4;
            final int bufLen = 4096;
            long[] bufs = new long[bufCount];
            for (int i = 0; i < bufCount; i++) {
                bufs[i] = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);
                for (int j = 0; j < bufLen; j++) {
                    Unsafe.getUnsafe().putByte(bufs[i] + j, (byte) (i + 1));
                }
            }

            long iovecSize = 16L;
            long iovsAddr = Unsafe.malloc(iovecSize * bufCount, MemoryTag.NATIVE_DEFAULT);
            long readBuf = Unsafe.malloc(bufLen * bufCount, MemoryTag.NATIVE_DEFAULT);

            File file = temp.newFile();
            try (Path path = new Path()) {
                long fd = Files.openRW(path.of(file.getAbsolutePath()).$());
                Assert.assertTrue(fd > -1);

                try (IOURing ring = rf.newInstance(8)) {
                    for (int i = 0; i < bufCount; i++) {
                        Unsafe.getUnsafe().putLong(iovsAddr + i * iovecSize, bufs[i]);
                        Unsafe.getUnsafe().putLong(iovsAddr + i * iovecSize + 8, bufLen);
                    }
                    Assert.assertEquals(0, ring.registerBuffers(iovsAddr, bufCount));

                    for (int i = 0; i < bufCount; i++) {
                        ring.enqueueWriteFixed(fd, (long) i * bufLen, bufs[i], bufLen, i, i);
                    }

                    ring.submitAndWait();
                    int drained = 0;
                    while (drained < bufCount) {
                        while (!ring.nextCqe()) {
                            Os.pause();
                        }
                        Assert.assertEquals(bufLen, ring.getCqeRes());
                        drained++;
                    }

                    ring.unregisterBuffers();
                } finally {
                    Files.close(fd);
                }

                // Read back and verify each region.
                long fdRead = Files.openRO(path.of(file.getAbsolutePath()).$());
                try {
                    long totalLen = (long) bufLen * bufCount;
                    long bytesRead = Files.read(fdRead, readBuf, totalLen, 0);
                    Assert.assertEquals(totalLen, bytesRead);
                    for (int i = 0; i < bufCount; i++) {
                        for (int j = 0; j < bufLen; j++) {
                            Assert.assertEquals(
                                    "mismatch at buffer " + i + " byte " + j,
                                    (byte) (i + 1),
                                    Unsafe.getUnsafe().getByte(readBuf + (long) i * bufLen + j)
                            );
                        }
                    }
                } finally {
                    Files.close(fdRead);
                }
            } finally {
                Unsafe.free(iovsAddr, iovecSize * bufCount, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(readBuf, (long) bufLen * bufCount, MemoryTag.NATIVE_DEFAULT);
                for (int i = 0; i < bufCount; i++) {
                    Unsafe.free(bufs[i], bufLen, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testWriteFixedPartialBuffer() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final int bufLen = 4096;
            long buf = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);
            long readBuf = Unsafe.malloc(1024, MemoryTag.NATIVE_DEFAULT);
            try {
                // Fill only bytes [1024..2048) with pattern, rest zeros.
                Unsafe.getUnsafe().setMemory(buf, bufLen, (byte) 0);
                for (int i = 0; i < 1024; i++) {
                    Unsafe.getUnsafe().putByte(buf + 1024 + i, (byte) 0xAB);
                }

                long iovecSize = 16L;
                long iovsAddr = Unsafe.malloc(iovecSize, MemoryTag.NATIVE_DEFAULT);

                File file = temp.newFile();
                try (Path path = new Path()) {
                    long fd = Files.openRW(path.of(file.getAbsolutePath()).$());
                    Assert.assertTrue(fd > -1);

                    try (IOURing ring = rf.newInstance(4)) {
                        Unsafe.getUnsafe().putLong(iovsAddr, buf);
                        Unsafe.getUnsafe().putLong(iovsAddr + 8, bufLen);
                        Assert.assertEquals(0, ring.registerBuffers(iovsAddr, 1));

                        // Write partial: 1024 bytes starting at offset 1024 within the buffer.
                        ring.enqueueWriteFixed(fd, 0, buf + 1024, 1024, 0, 0xBEEF);

                        ring.submitAndWait();
                        Assert.assertTrue(ring.nextCqe());
                        Assert.assertEquals(0xBEEF, ring.getCqeId());
                        Assert.assertEquals(1024, ring.getCqeRes());

                        ring.unregisterBuffers();
                    } finally {
                        Files.close(fd);
                    }

                    long fdRead = Files.openRO(path.of(file.getAbsolutePath()).$());
                    try {
                        long bytesRead = Files.read(fdRead, readBuf, 1024, 0);
                        Assert.assertEquals(1024, bytesRead);
                        for (int i = 0; i < 1024; i++) {
                            Assert.assertEquals((byte) 0xAB, Unsafe.getUnsafe().getByte(readBuf + i));
                        }
                    } finally {
                        Files.close(fdRead);
                    }
                }
                Unsafe.free(iovsAddr, iovecSize, MemoryTag.NATIVE_DEFAULT);
            } finally {
                Unsafe.free(buf, bufLen, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(readBuf, 1024, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testWriteFixedAfterReRegister() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final int bufLen = 4096;
            // Phase 1: register 2 buffers, write via WRITE_FIXED.
            long buf0 = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);
            long buf1 = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);
            // Phase 2: re-register with 4 buffers (2 old + 2 new).
            long buf2 = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);
            long buf3 = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);
            long readBuf = Unsafe.malloc(bufLen, MemoryTag.NATIVE_DEFAULT);

            for (int i = 0; i < bufLen; i++) {
                Unsafe.getUnsafe().putByte(buf0 + i, (byte) 0xAA);
                Unsafe.getUnsafe().putByte(buf2 + i, (byte) 0xCC);
                Unsafe.getUnsafe().putByte(buf3 + i, (byte) 0xDD);
            }

            long iovecSize = 16L;
            long iovsAddr2 = Unsafe.malloc(iovecSize * 2, MemoryTag.NATIVE_DEFAULT);
            long iovsAddr4 = Unsafe.malloc(iovecSize * 4, MemoryTag.NATIVE_DEFAULT);

            File file = temp.newFile();
            try (Path path = new Path()) {
                long fd = Files.openRW(path.of(file.getAbsolutePath()).$());
                Assert.assertTrue(fd > -1);

                try (IOURing ring = rf.newInstance(8)) {
                    // Phase 1: register 2 buffers.
                    Unsafe.getUnsafe().putLong(iovsAddr2, buf0);
                    Unsafe.getUnsafe().putLong(iovsAddr2 + 8, bufLen);
                    Unsafe.getUnsafe().putLong(iovsAddr2 + iovecSize, buf1);
                    Unsafe.getUnsafe().putLong(iovsAddr2 + iovecSize + 8, bufLen);
                    Assert.assertEquals(0, ring.registerBuffers(iovsAddr2, 2));

                    ring.enqueueWriteFixed(fd, 0, buf0, bufLen, 0, 1);
                    ring.submitAndWait();
                    Assert.assertTrue(ring.nextCqe());
                    Assert.assertEquals(bufLen, ring.getCqeRes());

                    // Phase 2: unregister, re-register with 4 buffers.
                    ring.unregisterBuffers();

                    Unsafe.getUnsafe().putLong(iovsAddr4, buf0);
                    Unsafe.getUnsafe().putLong(iovsAddr4 + 8, bufLen);
                    Unsafe.getUnsafe().putLong(iovsAddr4 + iovecSize, buf1);
                    Unsafe.getUnsafe().putLong(iovsAddr4 + iovecSize + 8, bufLen);
                    Unsafe.getUnsafe().putLong(iovsAddr4 + 2 * iovecSize, buf2);
                    Unsafe.getUnsafe().putLong(iovsAddr4 + 2 * iovecSize + 8, bufLen);
                    Unsafe.getUnsafe().putLong(iovsAddr4 + 3 * iovecSize, buf3);
                    Unsafe.getUnsafe().putLong(iovsAddr4 + 3 * iovecSize + 8, bufLen);
                    Assert.assertEquals(0, ring.registerBuffers(iovsAddr4, 4));

                    // Write using new buffer index 2.
                    ring.enqueueWriteFixed(fd, bufLen, buf2, bufLen, 2, 2);
                    ring.submitAndWait();
                    Assert.assertTrue(ring.nextCqe());
                    Assert.assertEquals(bufLen, ring.getCqeRes());

                    ring.unregisterBuffers();
                } finally {
                    Files.close(fd);
                }

                // Verify file: first 4096 bytes = 0xAA, next 4096 = 0xCC.
                long fdRead = Files.openRO(path.of(file.getAbsolutePath()).$());
                try {
                    long bytesRead = Files.read(fdRead, readBuf, bufLen, 0);
                    Assert.assertEquals(bufLen, bytesRead);
                    for (int i = 0; i < bufLen; i++) {
                        Assert.assertEquals((byte) 0xAA, Unsafe.getUnsafe().getByte(readBuf + i));
                    }

                    bytesRead = Files.read(fdRead, readBuf, bufLen, bufLen);
                    Assert.assertEquals(bufLen, bytesRead);
                    for (int i = 0; i < bufLen; i++) {
                        Assert.assertEquals((byte) 0xCC, Unsafe.getUnsafe().getByte(readBuf + i));
                    }
                } finally {
                    Files.close(fdRead);
                }
            } finally {
                Unsafe.free(iovsAddr2, iovecSize * 2, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(iovsAddr4, iovecSize * 4, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(buf0, bufLen, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(buf1, bufLen, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(buf2, bufLen, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(buf3, bufLen, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(readBuf, bufLen, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testRead() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final int inFlight = 34;
            final int txtLen = 42;
            StringSink sink = new StringSink();
            for (int i = 0; i < inFlight; i++) {
                for (int j = 0; j < txtLen; j++) {
                    sink.put(i);
                }
            }
            String txt = sink.toString();

            File file = temp.newFile();
            TestUtils.writeStringToFile(file, txt);

            try (Path path = new Path()) {
                long fd = Files.openRO(path.of(file.getAbsolutePath()).$());
                Assert.assertTrue(fd > -1);
                long[] bufs = new long[inFlight];
                for (int i = 0; i < inFlight; i++) {
                    bufs[i] = Unsafe.malloc(txtLen, MemoryTag.NATIVE_DEFAULT);
                }

                try (IOURing ring = rf.newInstance(Numbers.ceilPow2(inFlight))) {
                    LongList expectedIds = new LongList();
                    for (int i = 0; i < inFlight; i++) {
                        long id = ring.enqueueRead(fd, i * txtLen, bufs[i], txtLen);
                        Assert.assertTrue(id > -1);
                        expectedIds.add(id);
                    }

                    int submitted = ring.submit();
                    Assert.assertEquals(inFlight, submitted);

                    LongList actualIds = new LongList();
                    for (int i = 0; i < inFlight; i++) {
                        while (!ring.nextCqe()) {
                            Os.pause();
                        }
                        actualIds.add(ring.getCqeId());
                        Assert.assertEquals(txtLen, ring.getCqeRes());
                    }

                    DirectUtf8String txtInBuf = new DirectUtf8String();
                    for (int i = 0; i < inFlight; i++) {
                        txtInBuf.of(bufs[i], bufs[i] + txtLen);
                        TestUtils.assertEquals(txt.substring(i * txtLen, (i + 1) * txtLen), txtInBuf);
                    }

                    TestUtils.assertEquals(expectedIds, actualIds);
                } finally {
                    Files.close(fd);
                    for (int i = 0; i < inFlight; i++) {
                        Unsafe.free(bufs[i], txtLen, MemoryTag.NATIVE_DEFAULT);
                    }
                }
            }
        });
    }

    @Test
    public void testSqOverflow() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            final int inFlight = 32;
            final int iterations = 100;
            try (IOURing ring = rf.newInstance(inFlight)) {
                for (int it = 0; it < iterations; it++) {
                    for (int i = 0; i < inFlight; i++) {
                        long id = ring.enqueueNop();
                        Assert.assertTrue(id > -1);
                    }
                    Assert.assertEquals(-1, ring.enqueueNop());

                    int submitted = ring.submit();
                    Assert.assertEquals(inFlight, submitted);

                    for (int i = 0; i < inFlight; i++) {
                        while (!ring.nextCqe()) {
                            Os.pause();
                        }
                        Assert.assertTrue(ring.getCqeId() > -1);
                        Assert.assertTrue(ring.getCqeRes() > -1);
                    }
                    Assert.assertFalse(ring.nextCqe());
                    Assert.assertEquals(-1, ring.getCqeId());
                    Assert.assertEquals(-1, ring.getCqeRes());
                }
            }
        });
    }

    @Test
    public void testSubmitAndWait() throws Exception {
        Assume.assumeTrue(rf.isAvailable());

        TestUtils.assertMemoryLeak(() -> {
            String txt = "1234";
            final int txtLen = txt.length();
            File file = temp.newFile();
            TestUtils.writeStringToFile(file, txt);

            try (Path path = new Path()) {
                long fd = Files.openRO(path.of(file.getAbsolutePath()).$());
                Assert.assertTrue(fd > -1);
                long buf = Unsafe.malloc(txtLen, MemoryTag.NATIVE_DEFAULT);

                try (IOURing ring = rf.newInstance(4)) {
                    long id = ring.enqueueRead(fd, 0, buf, txtLen);
                    Assert.assertTrue(id > -1);

                    int submitted = ring.submitAndWait();
                    Assert.assertEquals(1, submitted);

                    Assert.assertTrue(ring.nextCqe());
                    Assert.assertEquals(id, ring.getCqeId());
                    Assert.assertEquals(txtLen, ring.getCqeRes());

                    DirectUtf8String txtInBuf = new DirectUtf8String().of(buf, buf + txtLen);
                    TestUtils.assertEquals(txt.substring(0, txtLen), txtInBuf);
                } finally {
                    Files.close(fd);
                    Unsafe.free(buf, txtLen, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }
}
