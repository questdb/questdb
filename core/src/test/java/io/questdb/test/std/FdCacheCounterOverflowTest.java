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

import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class FdCacheCounterOverflowTest extends AbstractTest {
    private static final byte CONTENT_A = 'A';
    private static final byte CONTENT_B = 'B';
    private static final int FILE_SIZE = 1024;
    private Path testFileA;
    private Path testFileB;

    @Before
    public void setUp() {
        super.setUp();
        testFileA = new Path();
        testFileB = new Path();
        setupTestFiles();
    }

    @After
    public void tearDown() throws Exception {
        Misc.free(testFileA);
        Misc.free(testFileB);
        // Reset FD‐cache counter to default (1) to avoid cross-test interference
        Files.setFDCacheCounter(1);
        super.tearDown();
    }

    @Test
    public void testConcurrentOpenClose() throws Exception {
        // Concurrent test that spawns a single writer that writes an append-only file, and multiple workers.
        // It sets the FD counter near the overflow initially, and the reader threads actively open/read/close the file.

        Files.setFDCacheCounter(Integer.MAX_VALUE - 50);

        assertMemoryLeak(() -> {
            final int numReaderThreads = 4;
            final int numIterations = 1000;
            final Thread[] readerThreads = new Thread[numReaderThreads];
            final Throwable[] exceptions = new Throwable[numReaderThreads + 1];
            final AtomicBoolean shouldStop = new AtomicBoolean(false);

            // Writer thread - continuously appends to file A
            Thread writerThread = new Thread(() -> {
                byte[] appendData = new byte[16];
                Arrays.fill(appendData, CONTENT_A);
                long buf = Unsafe.getUnsafe().allocateMemory(appendData.length);
                Unsafe.getUnsafe().copyMemory(appendData, sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET, null, buf, appendData.length);

                long appendFd = -1;
                try {
                    appendFd = Files.openAppend(testFileA.$());
                    Assert.assertTrue("Failed to open file for appending", appendFd > -1);
                    while (!shouldStop.get()) {
                        final long res = Files.append(appendFd, buf, appendData.length);
                        Assert.assertTrue("Files.append failed: " + Os.errno(), res > -1);
                        Os.sleep(10);
                    }
                } catch (Exception e) {
                    exceptions[numReaderThreads] = e;
                } finally {
                    Unsafe.getUnsafe().freeMemory(buf);
                    close(appendFd);
                }
            });

            // Reader threads - continuously open/read/close file A
            for (int threadIdx = 0; threadIdx < numReaderThreads; threadIdx++) {
                final int threadIndex = threadIdx;
                readerThreads[threadIdx] = new Thread(() -> {
                    try {
                        for (int i = 0; i < numIterations; i++) {
                            long readFd = -1;
                            try {
                                readFd = openRO(testFileA);
                                long size = Files.length(readFd);
                                long addr = Files.mmap(readFd, size, 0, Files.MAP_RO, MemoryTag.MMAP_DEFAULT);
                                Assert.assertTrue("Memory mapping failed", addr > 0);
                                try {
                                    byte content = Unsafe.getUnsafe().getByte(addr + size - 1);
                                    Assert.assertEquals("File content mismatch in reader thread " + threadIndex, CONTENT_A, content);
                                } finally {
                                    Files.munmap(addr, size, MemoryTag.MMAP_DEFAULT);
                                }
                                Os.sleep(1);
                            } finally {
                                close(readFd);
                            }
                        }
                    } catch (Throwable e) {
                        exceptions[threadIndex] = e;
                    }
                });
            }

            writerThread.start();
            for (Thread readerThread : readerThreads) {
                readerThread.start();
            }

            for (Thread readerThread : readerThreads) {
                readerThread.join(60_000);
            }
            shouldStop.set(true);
            writerThread.join(60_000);

            boolean someThreadIsHanging = false;
            for (int i = 0; i < readerThreads.length; i++) {
                if (readerThreads[i].isAlive()) {
                    someThreadIsHanging = true;
                    System.out.println("Reader thread " + i + " did not finish within timeout");
                }
            }
            if (writerThread.isAlive()) {
                someThreadIsHanging = true;
                System.out.println("Writer thread did not finish within timeout");
            }

            Throwable lastException = null;
            // Check for exceptions
            for (Throwable e : exceptions) {
                if (e != null) {
                    e.printStackTrace(System.out);
                    lastException = e;
                }
            }
            if (lastException != null) {
                throw new AssertionError(lastException);
            }
            Assert.assertFalse("Some threads didn't finish within timeout", someThreadIsHanging);
        });
    }

    @Test
    public void testOpenATwiceThenCloseOnceThenOpenBThenReadA() throws Exception {
        wraparoundFdCounter();

        // Test scenario:
        // 1. Open the same file twice, read-only
        // 2. Close it once, FD reuse logic should leave the file open, but a bug caused it to close.
        // 3. Open file B. Due to FdCache bug and OS's FD reuse, this gets the same FD of the now-closed file A.
        // 4. Read file B -- fine
        // 5. Read file A -- reads content of B <<< FAILURE

        assertMemoryLeak(() -> {
            long roA1 = -1;
            long roA2 = -1;
            long roB = -1;
            try {
                roA1 = openRO(testFileA);
                roA2 = openRO(testFileA);
                Assert.assertTrue("Failed to open file A", roA1 > -1 && roA2 > -1);
                assertFdCanBeMmapedAndAssertContent(roA2, CONTENT_A);

                roA2 = close(roA2);

                roB = openRO(testFileB);
                Assert.assertTrue("Failed to open file B", roB > -1);
                assertFdCanBeMmapedAndAssertContent(roB, CONTENT_B);

                assertFdCanBeMmapedAndAssertContent(roA1, CONTENT_A);
            } finally {
                close(roA1);
                close(roA2);
                close(roB);
            }
        });
    }

    @Test
    public void testOpenATwiceThenCloseOnceThenOpenBrwThenReadA() throws Exception {
        wraparoundFdCounter();

        // Test scenario:
        // 1. Open the same file twice, read-only
        // 2. Close it once, FD reuse logic should leave the file open, but a bug caused it to close.
        // 3. Open file B as RW. Due to FdCache bug and OS's FD reuse, this gets the same FD of the now-closed file A.
        // 4. Read file B -- fine
        // 5. Read file A and get content of B <<< FAILURE

        assertMemoryLeak(() -> {
            long roA1 = -1;
            long roA2 = -1;
            long rwB = -1;
            try {
                roA1 = openRO(testFileA);
                roA2 = openRO(testFileA);
                Assert.assertTrue("Failed to open file A", roA1 > -1 && roA2 > -1);
                assertFdCanBeMmapedAndAssertContent(roA2, CONTENT_A);

                roA2 = close(roA2);

                rwB = openRW(testFileB);
                Assert.assertTrue("Failed to open file B", rwB > -1);
                assertFdCanBeMmapedAndAssertContent(rwB, CONTENT_B);

                assertFdCanBeMmapedAndAssertContent(roA1, CONTENT_A);
            } finally {
                close(roA1);
                close(roA2);
                close(rwB);
            }
        });
    }

    @Test
    public void testOpenTwoRoThenCloseFirstThenReadSecond() throws Exception {
        wraparoundFdCounter();

        // Test scenario:
        // 1. Open the same file twice, read-only
        // 2. Close it once, FD reuse logic should leave the file open, but a bug caused it to close.
        // 3. Read the remaining "open" file, it's actually closed <<< FAILURE

        assertMemoryLeak(() -> {
            long roA1 = -1;
            long roA2 = -1;
            try {
                roA1 = openRO(testFileA);
                roA2 = openRO(testFileA);
                Assert.assertTrue("Failed to open a file", roA1 > -1 && roA2 > -1);

                assertFdCanBeMmapedAndAssertContent(roA1, CONTENT_A);
                roA1 = close(roA1);

                assertFdCanBeMmapedAndAssertContent(roA2, CONTENT_A);
            } finally {
                close(roA1);
                close(roA2);
            }
        });
    }

    @Test
    public void testRwInterleaveRwWithRoSharing() throws Exception {
        wraparoundFdCounter();

        // Test scenario: Mix RW (non-shared) with RO (shared) operations
        // 1. Open file A as RW (gets fresh FD, never shared)
        // 2. Open file A as RO twice (these share a different FD from the RW one)
        // 3. Close one RO handle - bug causes shared RO FD to close prematurely
        // 4. Open file B as RW - OS reuses the prematurely closed RO FD
        // 5. Read from the remaining RO handle of A - gets content from B instead

        assertMemoryLeak(() -> {
            long rwA = -1;
            long rwB = -1;
            long roA1 = -1;
            long roA2 = -1;
            try {
                rwA = openRW(testFileA);
                roA1 = openRO(testFileA);
                roA2 = openRO(testFileA);
                Assert.assertTrue("Failed to open file A", rwA > -1 && roA1 > -1 && roA2 > -1);
                assertFdCanBeMmapedAndAssertContent(roA1, CONTENT_A);

                roA1 = close(roA1);

                rwB = openRW(testFileB);
                Assert.assertTrue("Failed to open file B", rwB > -1);
                assertFdCanBeMmapedAndAssertContent(rwB, CONTENT_B);
                assertFdCanBeMmapedAndAssertContent(rwA, CONTENT_A);

                assertFdCanBeMmapedAndAssertContent(roA2, CONTENT_A);
            } finally {
                close(rwA);
                close(rwB);
                close(roA1);
                close(roA2);
            }
        });
    }

    private static void assertFdCanBeMmapedAndAssertContent(long fd1, byte expectedContent) {
        long addr = Files.mmap(fd1, FILE_SIZE, 0, Files.MAP_RO, MemoryTag.MMAP_DEFAULT);
        Assert.assertTrue("failed to mmap", addr > 0);
        try {
            for (int i = 0; i < FILE_SIZE; i++) {
                Assert.assertEquals(expectedContent, Unsafe.getUnsafe().getByte(addr + i));
            }
        } finally {
            Files.munmap(addr, FILE_SIZE, MemoryTag.MMAP_DEFAULT);
        }
    }

    private static long close(long fd) {
        if (fd != -1) {
            Files.close(fd);
        }
        return -1;
    }

    private static long openRO(Path p) {
        long fd = Files.openRO(p.$());
        Assert.assertTrue("Failed to open RO file " + p, fd > -1);
        return fd;
    }

    private static long openRW(Path p) {
        long fd = Files.openRW(p.$());
        Assert.assertTrue("Failed to open RW file " + p, fd > -1);
        return fd;
    }

    private void setupTestFiles() {
        try {
            File file = temp.newFile("overflow_test_A" + System.nanoTime() + ".dat");
            try (FileOutputStream fos = new FileOutputStream(file)) {
                byte[] data = new byte[FILE_SIZE];
                Arrays.fill(data, CONTENT_A);
                fos.write(data);
            }
            testFileA.of(file.getAbsolutePath());

            file = temp.newFile("overflow_test_B" + System.nanoTime() + ".dat");
            try (FileOutputStream fos = new FileOutputStream(file)) {
                byte[] data = new byte[FILE_SIZE];
                Arrays.fill(data, CONTENT_B);
                fos.write(data);
            }
            testFileB.of(file.getAbsolutePath());
        } catch (IOException e) {
            throw new RuntimeException("Failed to setup test files", e);
        }
    }

    // Sets fdCounter inside FdCache to Integer.MIN_VALUE. This will happen
    // in production after enough file open-close operations have occurred
    // to cause integer wraparound in the counter.
    private void wraparoundFdCounter() {
        Files.setFDCacheCounter(Integer.MIN_VALUE);
    }
}
