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

import io.questdb.ParanoiaState;
import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@RunWith(Parameterized.class)
public class FilesCacheFuzzTest extends AbstractTest {
    private static final int FILE_SIZE = (int) (Files.PAGE_SIZE * 10);
    private static final int NUM_THREADS = 4;
    private static final int NUM_FILES = NUM_THREADS;
    private static final int OPERATIONS_PER_THREAD = 500;
    private static boolean savedFdParanoia;
    private long beforeFdResused;
    private long beforeMmapResused;
    private Rnd rndRoot;
    private Path[] testFilePaths;

    public FilesCacheFuzzTest(boolean fdCacheEnabled, boolean asyncMunmapEnabled) {
        Files.FS_CACHE_ENABLED = fdCacheEnabled;
        Files.ASYNC_MUNMAP_ENABLED = asyncMunmapEnabled;
    }

    @Parameterized.Parameters(name = "fd_cache_enabled_{0}, async_munmap_{1}")
    public static Collection<Object[]> data() {
        return Os.isPosix() ? Arrays.asList(new Object[][]{
                {true, true},
                {true, false},
                {false, true},
                {false, false}
        }) : Arrays.asList(new Object[][]{
                {true, false},
                {false, false}
        });
    }

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractTest.setUpStatic();
        savedFdParanoia = ParanoiaState.FD_PARANOIA_MODE;
        ParanoiaState.FD_PARANOIA_MODE = true;
    }

    @AfterClass
    public static void tearDownStatic() {
        AbstractTest.tearDownStatic();
        ParanoiaState.FD_PARANOIA_MODE = savedFdParanoia;
        Files.ASYNC_MUNMAP_ENABLED = false;
    }

    @Before
    public void setUp() {
        super.setUp();
        setupTestFiles();
        beforeFdResused = Files.getFdReuseCount();
        beforeMmapResused = Files.getMmapReuseCount();
    }

    @After
    public void tearDown() throws Exception {
        Misc.free(testFilePaths);
        super.tearDown();
        LOG.info().$("FD cached reused: ").$(Files.getFdReuseCount() - beforeFdResused)
                .$(", Mmap cached reused: ").$(Files.getMmapReuseCount() - beforeMmapResused)
                .$();
    }

    @Test
    public void testConcurrentFileOperations() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CyclicBarrier barrier = new CyclicBarrier(NUM_THREADS);
            AtomicBoolean failed = new AtomicBoolean(false);
            ConcurrentLinkedQueue<Exception> exceptions = new ConcurrentLinkedQueue<>();
            AtomicLong totalOperations = new AtomicLong(0);
            AtomicInteger openCount = new AtomicInteger(0);
            AtomicInteger closeCount = new AtomicInteger(0);

            Thread[] threads = new Thread[NUM_THREADS];

            for (int i = 0; i < NUM_THREADS; i++) {
                int threadId = i; // Capture thread index for lambda
                Rnd rnd = new Rnd(rndRoot.nextLong(), rndRoot.nextLong());
                threads[i] = new Thread(() -> {
                    try {
                        barrier.await();

                        for (int op = 0; op < OPERATIONS_PER_THREAD; op++) {
                            if (failed.get()) break;

                            Path filePath = testFilePaths[threadId];

                            long fd;
                            if (rnd.nextBoolean()) {
                                fd = Files.openRO(filePath.$());
                            } else {
                                fd = Files.openRW(filePath.$());
                            }

                            if (fd != -1) {
                                openCount.incrementAndGet();

                                long fileLength = Files.length(fd);
                                Assert.assertEquals("File length should match", FILE_SIZE, fileLength);

                                if (rnd.nextInt(3) == 0) {
                                    long position = Files.getLastModified(filePath.$());
                                    Assert.assertTrue("Last modified should be positive", position > 0);
                                }

                                int closeResult = Files.close(fd);
                                Assert.assertEquals("Close should succeed", 0, closeResult);
                                closeCount.incrementAndGet();
                            }

                            totalOperations.incrementAndGet();

                        }
                    } catch (Exception e) {
                        exceptions.add(e);
                        failed.set(true);
                    }
                });
                threads[i].start();
            }

            for (Thread thread : threads) {
                thread.join();
            }

            if (!exceptions.isEmpty()) {
                Exception first = exceptions.poll();
                LOG.error().$("Total operations: ").$(totalOperations.get()).$();
                LOG.error().$("Open/Close counts: ").$(openCount.get()).$('/').$(closeCount.get()).$();
                throw new RuntimeException("File operations test failed", first);
            }

            Assert.assertTrue("Should have completed operations", totalOperations.get() > 0);
            Assert.assertEquals("Open and close counts should match", openCount.get(), closeCount.get());
            LOG.info().$("File operations - Total: ").$(totalOperations.get())
                    .$(", Open/Close: ").$(openCount.get()).$('/').$(closeCount.get()).$();
        });
    }

    @Test
    public void testConcurrentFileSystemOperations() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CyclicBarrier barrier = new CyclicBarrier(NUM_THREADS);
            AtomicBoolean failed = new AtomicBoolean(false);
            ConcurrentLinkedQueue<Exception> exceptions = new ConcurrentLinkedQueue<>();
            AtomicInteger operationCount = new AtomicInteger(0);

            Thread[] threads = new Thread[NUM_THREADS];

            for (int i = 0; i < NUM_THREADS; i++) {
                int threadId = i;
                threads[i] = new Thread(() -> {
                    try {
                        barrier.await();

                        for (int op = 0; op < OPERATIONS_PER_THREAD / 4; op++) {
                            if (failed.get()) break;

                            Path filePath = testFilePaths[threadId];

                            try {
                                if (Files.exists(filePath.$())) {
                                    long length = Files.length(filePath.$());
                                    Assert.assertTrue("File length should be positive", length > 0);

                                    long lastModified = Files.getLastModified(filePath.$());
                                    Assert.assertTrue("Last modified should be positive", lastModified > 0);

                                    boolean isDirTest = Files.isDirOrSoftLinkDirNoDots(filePath,
                                            filePath.size(),
                                            filePath.ptr(),
                                            filePath.size());
                                    Assert.assertFalse("Should not be directory", isDirTest);

                                    operationCount.incrementAndGet();
                                }
                            } catch (Exception e) {
                                exceptions.add(e);
                                failed.set(true);
                                break;
                            }
                        }
                    } catch (Exception e) {
                        exceptions.add(e);
                        failed.set(true);
                    }
                });
                threads[i].start();
            }

            for (Thread thread : threads) {
                thread.join();
            }

            if (!exceptions.isEmpty()) {
                Exception first = exceptions.poll();
                LOG.error().$("FS operations count: ").$(operationCount.get()).$();
                throw new RuntimeException("File system operations test failed", first);
            }

            Assert.assertTrue("Should have completed operations", operationCount.get() > 0);
            LOG.info().$("File system operations completed: ").$(operationCount.get()).$();
        });
    }

    @Test
    public void testConcurrentMmapOperations() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CyclicBarrier barrier = new CyclicBarrier(NUM_THREADS);
            AtomicBoolean failed = new AtomicBoolean(false);
            ConcurrentLinkedQueue<Exception> exceptions = new ConcurrentLinkedQueue<>();
            AtomicInteger mmapCount = new AtomicInteger(0);
            AtomicInteger munmapCount = new AtomicInteger(0);

            Thread[] threads = new Thread[NUM_THREADS];

            for (int i = 0; i < NUM_THREADS; i++) {
                Rnd rnd = new Rnd(rndRoot.nextLong(), rndRoot.nextLong());
                int threadId = i; // Capture thread index for lambda
                threads[i] = new Thread(() -> {
                    try {
                        barrier.await();

                        for (int op = 0; op < OPERATIONS_PER_THREAD / 2; op++) {
                            if (failed.get()) break;

                            Path filePath = testFilePaths[threadId];

                            long fd = -1;
                            long address = 0;
                            int mapSize = 0;
                            try {
                                fd = Files.openRO(filePath.$());
                                if (fd != -1) {
                                    mapSize = 1024 + rnd.nextInt(FILE_SIZE - 1024);
                                    address = Files.mmap(fd, mapSize, 0, Files.MAP_RO, MemoryTag.MMAP_DEFAULT);

                                    if (address != -1) {
                                        mmapCount.incrementAndGet();

                                        if (rnd.nextInt(3) == 0) {
                                            int newSize = mapSize + 1024;
                                            if (newSize <= FILE_SIZE) {
                                                long newAddress = Files.mremap(fd, address, mapSize, newSize, 0, Files.MAP_RO, MemoryTag.MMAP_DEFAULT);
                                                if (newAddress != -1) {
                                                    address = newAddress;
                                                    mapSize = newSize;
                                                }
                                            }
                                        }

                                        Files.munmap(address, mapSize, MemoryTag.MMAP_DEFAULT);
                                        munmapCount.incrementAndGet();
                                        address = 0;
                                    }

                                    Files.close(fd);
                                    fd = -1;
                                }
                            } catch (Exception e) {
                                if (address != 0) {
                                    try {
                                        Files.munmap(address, mapSize, MemoryTag.MMAP_DEFAULT);
                                        munmapCount.incrementAndGet();
                                    } catch (Exception ignored) {
                                    }
                                }
                                if (fd != -1) {
                                    try {
                                        Files.close(fd);
                                    } catch (Exception ignored) {
                                    }
                                }
                                exceptions.add(e);
                                failed.set(true);
                                break;
                            }
                        }
                    } catch (Exception e) {
                        exceptions.add(e);
                        failed.set(true);
                    }
                });
                threads[i].start();
            }

            for (Thread thread : threads) {
                thread.join();
            }

            if (!exceptions.isEmpty()) {
                Exception first = exceptions.poll();
                LOG.error().$("Mmap/Munmap counts: ").$(mmapCount.get()).$('/').$(munmapCount.get()).$();
                throw new RuntimeException("Mmap operations test failed", first);
            }

            Assert.assertEquals("Mmap and munmap counts should match", mmapCount.get(), munmapCount.get());
            LOG.info().$("Mmap operations - Mmap/Munmap: ").$(mmapCount.get()).$('/').$(munmapCount.get()).$();
        });
    }

    @Test
    public void testConcurrentSameFileMmap() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CyclicBarrier barrier = new CyclicBarrier(NUM_THREADS);
            AtomicBoolean failed = new AtomicBoolean(false);
            ConcurrentLinkedQueue<Exception> exceptions = new ConcurrentLinkedQueue<>();
            AtomicInteger mmapCount = new AtomicInteger(0);
            AtomicInteger munmapCount = new AtomicInteger(0);

            Path singleFile = testFilePaths[0];
            Thread[] threads = new Thread[NUM_THREADS];

            for (int i = 0; i < NUM_THREADS; i++) {
                Rnd rnd = new Rnd(rndRoot.nextLong(), rndRoot.nextLong());
                threads[i] = new Thread(() -> {
                    try {
                        barrier.await();

                        for (int op = 0; op < OPERATIONS_PER_THREAD / 4; op++) {
                            if (failed.get()) break;

                            long fd = -1;
                            long address = 0;
                            int mapSize = 0;
                            try {
                                fd = Files.openRW(singleFile.$());
                                if (fd != -1) {
                                    mapSize = 2048 + rnd.nextInt(FILE_SIZE / 2);
                                    address = Files.mmap(fd, mapSize, 0, Files.MAP_RW, MemoryTag.MMAP_DEFAULT);

                                    if (address != -1) {
                                        mmapCount.incrementAndGet();

                                        Thread.sleep(rnd.nextInt(5));

                                        Files.munmap(address, mapSize, MemoryTag.MMAP_DEFAULT);
                                        munmapCount.incrementAndGet();
                                        address = 0;
                                    }

                                    Files.close(fd);
                                    fd = -1;
                                }
                            } catch (Exception e) {
                                if (address != 0) {
                                    try {
                                        Files.munmap(address, mapSize, MemoryTag.MMAP_DEFAULT);
                                        munmapCount.incrementAndGet();
                                    } catch (Exception ignored) {
                                    }
                                }
                                if (fd != -1) {
                                    try {
                                        Files.close(fd);
                                    } catch (Exception ignored) {
                                    }
                                }
                                exceptions.add(e);
                                failed.set(true);
                                break;
                            }
                        }
                    } catch (Exception e) {
                        exceptions.add(e);
                        failed.set(true);
                    }
                });
                threads[i].start();
            }

            for (Thread thread : threads) {
                thread.join();
            }

            if (!exceptions.isEmpty()) {
                Exception first = exceptions.poll();
                LOG.error().$("Same file mmap/munmap counts: ").$(mmapCount.get()).$('/').$(munmapCount.get()).$();
                throw new RuntimeException("Same file mmap test failed", first);
            }

            Assert.assertEquals("Mmap and munmap counts should match", mmapCount.get(), munmapCount.get());
            LOG.info().$("Same file mmap - Mmap/Munmap: ").$(mmapCount.get()).$('/').$(munmapCount.get()).$();
        });
    }

    @Test
    public void testMmapCacheSameFileReusePattern() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CyclicBarrier barrier = new CyclicBarrier(NUM_THREADS);
            AtomicBoolean failed = new AtomicBoolean(false);
            ConcurrentLinkedQueue<Exception> exceptions = new ConcurrentLinkedQueue<>();
            AtomicInteger mappingOperations = new AtomicInteger(0);

            Path singleFile = testFilePaths[0]; // Use single file for better cache testing
            Thread[] threads = new Thread[NUM_THREADS];

            for (int i = 0; i < NUM_THREADS; i++) {
                Rnd rnd = new Rnd(rndRoot.nextLong(), rndRoot.nextLong());
                int threadId = i; // Capture thread index for lambda

                threads[i] = new Thread(() -> {
                    try {
                        barrier.await();

                        for (int op = 0; op < OPERATIONS_PER_THREAD / 4; op++) {
                            if (failed.get()) break;

                            long fd = Files.openRW(singleFile.$());
                            Assert.assertTrue(fd > -1);
                            long mapSize = Files.PAGE_SIZE + rnd.nextInt(FILE_SIZE);

                            // Create mapping
                            long address = Files.mmap(fd, mapSize, 0, Files.MAP_RW, MemoryTag.MMAP_DEFAULT);
                            Assert.assertTrue(address > 0);
                            mappingOperations.incrementAndGet();

                            // Brief delay to allow cache interaction
                            if (rnd.nextInt(5) == 0) {
                                Thread.sleep(1);
                            }

                            long address2 = Files.mmap(fd, mapSize / 2, 0, Files.MAP_RO, MemoryTag.MMAP_DEFAULT);

                            int testValue = threadId * NUM_THREADS + op + 1;

                            long writeOffset = threadId * 8 + rnd.nextInt(2) * Files.PAGE_SIZE;
                            if (writeOffset > mapSize / 2 - 8) {
                                writeOffset = threadId * 8;
                                // Ensure writeOffset is within bounds, the mapping is big enough
                                assert writeOffset < (mapSize - 8);
                            }

                            Unsafe.getUnsafe().putLong(address + writeOffset, testValue);

                            long read = Unsafe.getUnsafe().getLong(address2 + writeOffset);
                            Assert.assertEquals("Read value should match written value",
                                    testValue, read);

                            long fd2 = Files.openRO(singleFile.$());
                            Assert.assertTrue(fd2 > -1);
                            long mapSize3 = mapSize / 2 + rnd.nextLong(mapSize / 2);
                            long address3 = Files.mmap(fd2, mapSize3, 0, Files.MAP_RO, MemoryTag.MMAP_DEFAULT);
                            long read2 = Unsafe.getUnsafe().getLong(address2 + writeOffset);
                            Assert.assertEquals("Read value should match written value",
                                    testValue, read2);

                            Files.munmap(address, mapSize, MemoryTag.MMAP_DEFAULT);
                            Files.munmap(address2, mapSize / 2, MemoryTag.MMAP_DEFAULT);
                            Files.munmap(address3, mapSize3, MemoryTag.MMAP_DEFAULT);

                            Files.close(fd);
                            Files.close(fd2);
                        }
                    } catch (Exception e) {
                        exceptions.add(e);
                        failed.set(true);
                    }
                });
                threads[i].start();
            }

            for (Thread thread : threads) {
                thread.join();
            }

            if (!exceptions.isEmpty()) {
                Exception first = exceptions.poll();
                LOG.error().$("MmapCache reuse - Mapping operations: ").$(mappingOperations.get()).$();
                throw new RuntimeException("MmapCache reuse test failed", first);
            }

            Assert.assertTrue("Should have mapping operations", mappingOperations.get() > 0);
            LOG.info().$("MmapCache reuse - Mappings: ").$(mappingOperations.get()).$();
        });
    }

    @Test
    public void testRemovedAndReopen() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CyclicBarrier barrier = new CyclicBarrier(NUM_THREADS);
            AtomicBoolean failed = new AtomicBoolean(false);
            ConcurrentLinkedQueue<Throwable> exceptions = new ConcurrentLinkedQueue<>();
            AtomicInteger openOperations = new AtomicInteger(0);
            AtomicInteger markRemovedOperations = new AtomicInteger(0);
            AtomicInteger reopenAfterRemoved = new AtomicInteger(0);

            Thread[] threads = new Thread[NUM_THREADS];
            // Operate all threads on the same file
            Path singleFile = testFilePaths[0];

            for (int i = 0; i < NUM_THREADS; i++) {
                int thread = i; // Capture thread index for lambda
                threads[i] = new Thread(() -> {
                    long buff = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
                    try {
                        barrier.await();

                        for (int op = 0; op < OPERATIONS_PER_THREAD && !failed.get(); op++) {

                            // Open file first to get it into cache
                            long fd1 = Files.openRW(singleFile.$());
                            if (fd1 == -1) {
                                LOG.error().$("Failed to open file: ").$(singleFile).$();
                                failed.set(true);
                                continue;
                            }
                            Unsafe.getUnsafe().putLong(buff, op + 1);
                            Files.write(fd1, buff, 8, thread * 8);

                            openOperations.incrementAndGet();

                            long fd2 = Files.openRO(singleFile.$());
                            Files.close(fd1);

                            if (fd2 > -1) {
                                if (Files.read(fd2, buff, 8, thread * 8) == 8) {
                                    long value = Unsafe.getUnsafe().getLong(buff);
                                    if (value != 0) {
                                        Assert.assertEquals(op + 1, value);
                                    }
                                }

                                Files.close(fd2);
                            }

                            if (TestUtils.remove(singleFile.$())) {
                                markRemovedOperations.incrementAndGet();
                            }
                        }
                    } catch (Throwable e) {
                        exceptions.add(e);
                        failed.set(true);
                    } finally {
                        Unsafe.free(buff, 8, MemoryTag.NATIVE_DEFAULT);
                    }
                });
                threads[i].start();
            }

            for (Thread thread : threads) {
                thread.join();
            }

            if (!exceptions.isEmpty()) {
                Throwable first = exceptions.poll();
                LOG.error().$("MarkPathRemoved test - Opens: ").$(openOperations.get())
                        .$(", Removed: ").$(markRemovedOperations.get()).$(", Reopen attempts: ").$(reopenAfterRemoved.get()).$();
                throw new RuntimeException("MarkPathRemoved test failed", first);
            }

            Assert.assertTrue("Should have open operations", openOperations.get() > 0);
            LOG.info().$("MarkPathRemoved test - Opens: ").$(openOperations.get())
                    .$(", Removed: ").$(markRemovedOperations.get()).$(", Reopen attempts: ").$(reopenAfterRemoved.get()).$();
        });
    }

    private void setupTestFiles() {
        try {

            String[] testFilePaths = new String[NUM_FILES];
            this.testFilePaths = new Path[NUM_FILES];

            byte[] data = new byte[FILE_SIZE];
            rndRoot = TestUtils.generateRandom(null);
            rndRoot.nextBytes(data);

            for (int i = 0; i < NUM_FILES; i++) {
                File tempFile = temp.newFile("files_fuzz_test_" + System.nanoTime() + "_" + i + ".dat");
                testFilePaths[i] = tempFile.getAbsolutePath();
                //noinspection resource
                this.testFilePaths[i] = new Path();
                this.testFilePaths[i].of(testFilePaths[i]);

                try (FileOutputStream fos = new FileOutputStream(tempFile)) {
                    fos.write(data);
                    fos.flush();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to setup test files", e);
        }
    }
}