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

package io.questdb.test.std;

import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.MmapCache;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Late eviction of released read-only mappings: with async munmap on, releasing the last reference to a file's
 * cached mapping leaves it mapped, with an eviction hint on the async munmap queue. Draining the queue, or a full
 * queue, is what unmaps it. Mapped bytes are tracked by memory tag, which moves only on a real mmap or munmap.
 */
public class MmapCacheTest extends AbstractTest {
    private static final long FILE_SIZE = 16 * Files.PAGE_SIZE;
    private static final long LEN = 4 * Files.PAGE_SIZE;
    private static final int TAG = MemoryTag.MMAP_DEFAULT;
    private final MmapCache cache = Files.getMmapCache();
    private final Path[] paths = new Path[3];
    private boolean savedAsyncMunmapEnabled;
    private boolean savedFsCacheEnabled;

    @Before
    public void setUp() {
        Assume.assumeTrue("async munmap is supported on POSIX only", Os.isPosix());
        super.setUp();
        savedFsCacheEnabled = Files.FS_CACHE_ENABLED;
        savedAsyncMunmapEnabled = Files.ASYNC_MUNMAP_ENABLED;
        Files.FS_CACHE_ENABLED = true;
        Files.ASYNC_MUNMAP_ENABLED = true;
        for (int i = 0; i < paths.length; i++) {
            paths[i] = new Path().of(createFile(i));
        }
    }

    @After
    public void tearDown() throws Exception {
        if (Os.isPosix()) {
            cache.asyncMunmap();
            Files.FS_CACHE_ENABLED = savedFsCacheEnabled;
            Files.ASYNC_MUNMAP_ENABLED = savedAsyncMunmapEnabled;
        }
        Misc.free(paths);
        super.tearDown();
    }

    @Test
    public void testConcurrentMapReleaseAndDrain() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int threadCount = 4;
            final int iterations = 2_000;
            // Keeps the files' mmap cache keys alive across the threads' own opens and closes.
            final long[] anchorFds = new long[paths.length];
            for (int i = 0; i < paths.length; i++) {
                anchorFds[i] = openRO(i);
            }
            final long reuseBefore = Files.getMmapReuseCount();
            final AtomicBoolean done = new AtomicBoolean();
            final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
            final CyclicBarrier barrier = new CyclicBarrier(threadCount + 1);
            final Rnd rndRoot = TestUtils.generateRandom(LOG);
            final Thread[] threads = new Thread[threadCount];
            try {
                for (int t = 0; t < threadCount; t++) {
                    final Rnd rnd = new Rnd(rndRoot.nextLong(), rndRoot.nextLong());
                    threads[t] = new Thread(() -> {
                        try {
                            barrier.await();
                            for (int i = 0; i < iterations && errors.isEmpty(); i++) {
                                final int index = rnd.nextInt(paths.length);
                                final long fd = openRO(index);
                                try {
                                    long len = Files.PAGE_SIZE * (1 + rnd.nextInt(8));
                                    long address = Files.mmap(fd, len, 0, Files.MAP_RO, TAG);
                                    Assert.assertTrue(address > 0);
                                    assertContent(address, len, index, rnd);
                                    if (rnd.nextInt(4) == 0) {
                                        final long newLen = len + Files.PAGE_SIZE * (1 + rnd.nextInt(4));
                                        final long newAddress = Files.mremap(fd, address, len, newLen, 0, Files.MAP_RO, TAG);
                                        Assert.assertTrue(newAddress > 0);
                                        address = newAddress;
                                        len = newLen;
                                        assertContent(address, len, index, rnd);
                                    }
                                    if (rnd.nextBoolean()) {
                                        Os.pause();
                                    }
                                    assertContent(address, len, index, rnd);
                                    Files.munmap(address, len, TAG);
                                } finally {
                                    Files.close(fd);
                                }
                            }
                        } catch (Throwable e) {
                            errors.add(e);
                        }
                    });
                    threads[t].start();
                }
                final Thread drainer = new Thread(() -> {
                    try {
                        barrier.await();
                        while (!done.get()) {
                            if (!cache.asyncMunmap()) {
                                Os.pause();
                            }
                        }
                    } catch (Throwable e) {
                        errors.add(e);
                    }
                });
                drainer.start();
                for (Thread thread : threads) {
                    thread.join();
                }
                done.set(true);
                drainer.join();
            } finally {
                for (long fd : anchorFds) {
                    Files.close(fd);
                }
            }
            if (!errors.isEmpty()) {
                throw new AssertionError("concurrent map/release/drain failed", errors.poll());
            }
            LOG.info().$("mmap cache reuse: ").$(Files.getMmapReuseCount() - reuseBefore).$();
            cache.asyncMunmap();
        });
    }

    @Test
    public void testDuplicateHintsReclaimOnce() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long fd = openRO(0);
            try {
                final long mem = mappedBytes();
                long address = Files.mmap(fd, LEN, 0, Files.MAP_RO, TAG);
                Files.munmap(address, LEN, TAG);
                Assert.assertEquals(address, Files.mmap(fd, LEN, 0, Files.MAP_RO, TAG));
                Files.munmap(address, LEN, TAG);
                Assert.assertEquals(mem + LEN, mappedBytes());

                // Two hints for the same unused mapping: the first evicts it, the second finds it retired.
                cache.asyncMunmap();
                Assert.assertEquals(mem, mappedBytes());

                // Evicted means gone from the cache too: the next map is a fresh one.
                final long reuse = Files.getMmapReuseCount();
                address = Files.mmap(fd, LEN, 0, Files.MAP_RO, TAG);
                Assert.assertEquals(reuse, Files.getMmapReuseCount());
                Assert.assertEquals(mem + LEN, mappedBytes());
                assertContent(address, LEN, 0);
                Files.munmap(address, LEN, TAG);
                cache.asyncMunmap();
                Assert.assertEquals(mem, mappedBytes());
            } finally {
                Files.close(fd);
            }
        });
    }

    @Test
    public void testHintForRecycledRecordEvictsItsUnusedMapping() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long fd0 = openRO(0);
            final long fd1 = openRO(1);
            try {
                final long mem = mappedBytes();
                final long other = recycleRecordWithPendingHint(fd0, fd1, 1);
                // The recycled record's mapping is released as well, which queues a hint of its own. The stale
                // hint comes first and evicts it; the new one finds it retired.
                Files.munmap(other, LEN, TAG);
                cache.asyncMunmap();
                Assert.assertEquals(mem, mappedBytes());
            } finally {
                Files.close(fd0);
                Files.close(fd1);
            }
        });
    }

    @Test
    public void testHintForRecycledRecordSkipsItsMappingInUse() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long fd0 = openRO(0);
            final long fd1 = openRO(1);
            try {
                final long mem = mappedBytes();
                final long other = recycleRecordWithPendingHint(fd0, fd1, 1);
                // The stale hint finds the recycled record in use and leaves its mapping alone.
                cache.asyncMunmap();
                Assert.assertEquals(mem + LEN, mappedBytes());
                assertContent(other, LEN, 1);
                Files.munmap(other, LEN, TAG);
                cache.asyncMunmap();
                Assert.assertEquals(mem, mappedBytes());
            } finally {
                Files.close(fd0);
                Files.close(fd1);
            }
        });
    }

    @Test
    public void testQueueFullReleaseUnmapsSynchronously() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long fd0 = openRO(0);
            final long fd1 = openRO(1);
            try {
                final long mem = mappedBytes();
                final long address = Files.mmap(fd0, LEN, 0, Files.MAP_RO, TAG);
                fillMunmapQueue(fd1);
                final long queuedMem = mappedBytes();
                Files.munmap(address, LEN, TAG);
                Assert.assertEquals(queuedMem - LEN, mappedBytes());

                // Retired, not cached: the next map is a fresh one.
                final long reuse = Files.getMmapReuseCount();
                final long address2 = Files.mmap(fd0, LEN, 0, Files.MAP_RO, TAG);
                Assert.assertEquals(reuse, Files.getMmapReuseCount());
                assertContent(address2, LEN, 0);
                Files.munmap(address2, LEN, TAG);
                cache.asyncMunmap();
                Assert.assertEquals(mem, mappedBytes());
            } finally {
                Files.close(fd0);
                Files.close(fd1);
            }
        });
    }

    @Test
    public void testReleasedMappingRevivedBeforeEviction() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long fd = openRO(0);
            try {
                final long mem = mappedBytes();
                final long address = Files.mmap(fd, LEN, 0, Files.MAP_RO, TAG);
                Assert.assertEquals(mem + LEN, mappedBytes());
                Files.munmap(address, LEN, TAG);
                // Released, not unmapped.
                Assert.assertEquals(mem + LEN, mappedBytes());

                final long reuse = Files.getMmapReuseCount();
                // A shorter request is served by the released mapping as well.
                Assert.assertEquals(address, Files.mmap(fd, LEN / 2, 0, Files.MAP_RO, TAG));
                Assert.assertEquals(reuse + 1, Files.getMmapReuseCount());

                // The hint finds the mapping in use and leaves it alone.
                cache.asyncMunmap();
                Assert.assertEquals(mem + LEN, mappedBytes());
                assertContent(address, LEN, 0);

                Files.munmap(address, LEN / 2, TAG);
                cache.asyncMunmap();
                Assert.assertEquals(mem, mappedBytes());
            } finally {
                Files.close(fd);
            }
        });
    }

    @Test
    public void testRemapOfSupersededMappingRevivesReleasedCurrentMapping() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long fd = openRO(0);
            try {
                final long mem = mappedBytes();
                final long address = Files.mmap(fd, LEN, 0, Files.MAP_RO, TAG);

                // Too short for this request: a longer mapping supersedes the held one in the file cache.
                final long reuse = Files.getMmapReuseCount();
                final long longLen = 2 * LEN;
                final long longAddress = Files.mmap(fd, longLen, 0, Files.MAP_RO, TAG);
                Assert.assertEquals(reuse, Files.getMmapReuseCount());
                Assert.assertNotEquals(address, longAddress);
                Assert.assertEquals(mem + LEN + longLen, mappedBytes());

                // Released, not unmapped: the longer mapping stays the file's current one, with a hint queued.
                Files.munmap(longAddress, longLen, TAG);
                Assert.assertEquals(mem + LEN + longLen, mappedBytes());

                // Growing the superseded mapping revives the released longer one. The superseded mapping loses its
                // only user: mremap() retires it and queues its unmap.
                Assert.assertEquals(longAddress, Files.mremap(fd, address, LEN, longLen, 0, Files.MAP_RO, TAG));
                Assert.assertEquals(reuse + 1, Files.getMmapReuseCount());
                Assert.assertEquals(mem + LEN + longLen, mappedBytes());

                // The drain unmaps the superseded mapping. The hint finds the longer mapping in use and leaves it
                // alone.
                cache.asyncMunmap();
                Assert.assertEquals(mem + longLen, mappedBytes());
                assertContent(longAddress, longLen, 0);

                Files.munmap(longAddress, longLen, TAG);
                cache.asyncMunmap();
                Assert.assertEquals(mem, mappedBytes());
            } finally {
                Files.close(fd);
            }
        });
    }

    @Test
    public void testRevivedMappingRemappedBeforeEviction() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long fd = openRO(0);
            try {
                final long mem = mappedBytes();
                final long address = Files.mmap(fd, LEN, 0, Files.MAP_RO, TAG);
                Files.munmap(address, LEN, TAG);
                Assert.assertEquals(address, Files.mmap(fd, LEN, 0, Files.MAP_RO, TAG));

                // The only user grows the revived mapping in place: the record now has a new length, and maybe
                // a new address.
                final long newLen = 3 * LEN;
                final long newAddress = Files.mremap(fd, address, LEN, newLen, 0, Files.MAP_RO, TAG);
                Assert.assertTrue(newAddress > 0);
                Assert.assertEquals(mem + newLen, mappedBytes());
                assertContent(newAddress, newLen, 0);
                Files.munmap(newAddress, newLen, TAG);

                // The first hint, queued before the remap, evicts the mapping as it is now; the second one finds
                // it retired.
                cache.asyncMunmap();
                Assert.assertEquals(mem, mappedBytes());
            } finally {
                Files.close(fd);
            }
        });
    }

    @Test
    public void testSkippedHintDoesNotBlockNextRelease() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long fd = openRO(0);
            try {
                final long mem = mappedBytes();
                final long address = Files.mmap(fd, LEN, 0, Files.MAP_RO, TAG);
                Files.munmap(address, LEN, TAG);
                Assert.assertEquals(address, Files.mmap(fd, LEN, 0, Files.MAP_RO, TAG));
                cache.asyncMunmap();
                Assert.assertEquals(mem + LEN, mappedBytes());

                Files.munmap(address, LEN, TAG);
                Assert.assertEquals(mem + LEN, mappedBytes());
                cache.asyncMunmap();
                Assert.assertEquals(mem, mappedBytes());
            } finally {
                Files.close(fd);
            }
        });
    }

    @Test
    public void testSupersededMappingEvictionKeepsCurrentMapping() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long fd = openRO(0);
            try {
                final long mem = mappedBytes();
                final long shortAddress = Files.mmap(fd, LEN, 0, Files.MAP_RO, TAG);
                Files.munmap(shortAddress, LEN, TAG);

                // Too short for this request: a longer mapping replaces the released one in the file cache.
                final long reuse = Files.getMmapReuseCount();
                final long longLen = 2 * LEN;
                final long longAddress = Files.mmap(fd, longLen, 0, Files.MAP_RO, TAG);
                Assert.assertEquals(reuse, Files.getMmapReuseCount());
                Assert.assertNotEquals(shortAddress, longAddress);
                Assert.assertEquals(mem + LEN + longLen, mappedBytes());

                // Evicting the superseded mapping leaves the current one cached.
                cache.asyncMunmap();
                Assert.assertEquals(mem + longLen, mappedBytes());
                Assert.assertEquals(longAddress, Files.mmap(fd, LEN, 0, Files.MAP_RO, TAG));
                Assert.assertEquals(reuse + 1, Files.getMmapReuseCount());

                Files.munmap(longAddress, LEN, TAG);
                Files.munmap(longAddress, longLen, TAG);
                cache.asyncMunmap();
                Assert.assertEquals(mem, mappedBytes());
            } finally {
                Files.close(fd);
            }
        });
    }

    private static void assertContent(long address, long len, int index) {
        for (long offset = 0; offset < len; offset += Files.PAGE_SIZE / 2) {
            Assert.assertEquals(taggedOffset(index, offset), Unsafe.getLong(address + offset));
        }
        Assert.assertEquals(taggedOffset(index, len - Long.BYTES), Unsafe.getLong(address + len - Long.BYTES));
    }

    private static void assertContent(long address, long len, int index, Rnd rnd) {
        final long offset = rnd.nextLong(len / Long.BYTES) * Long.BYTES;
        Assert.assertEquals(taggedOffset(index, offset), Unsafe.getLong(address + offset));
    }

    private static long mappedBytes() {
        return Unsafe.getMemUsedByTag(TAG);
    }

    /**
     * The long at the given offset of the file at the given index: the offset, with the file's tag in the top byte.
     * Tags start at 1, so a mapping of another file, or of zeroes, fails {@link #assertContent}.
     */
    private static long taggedOffset(int index, long offset) {
        return offset ^ ((long) (index + 1) << 56);
    }

    private String createFile(int index) {
        try {
            final File file = temp.newFile("mmap_cache_test_" + System.nanoTime() + "_" + index + ".dat");
            final ByteBuffer buffer = ByteBuffer.allocate((int) FILE_SIZE).order(ByteOrder.nativeOrder());
            for (long offset = 0; offset < FILE_SIZE; offset += Long.BYTES) {
                buffer.putLong(taggedOffset(index, offset));
            }
            try (FileOutputStream out = new FileOutputStream(file)) {
                out.write(buffer.array());
            }
            return file.getAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Queues uncached unmaps until the queue is full, which shows as the first unmap that happens right away.
     */
    private void fillMunmapQueue(long fd) {
        for (int i = 0; i < 1_000_000; i++) {
            final long address = Files.mmapNoCache(fd, Files.PAGE_SIZE, 0, Files.MAP_RO, TAG);
            Assert.assertTrue(address > 0);
            final long mem = mappedBytes();
            Files.munmap(address, Files.PAGE_SIZE, TAG);
            if (mappedBytes() < mem) {
                return;
            }
        }
        Assert.fail("async munmap queue never filled up");
    }

    private long openRO(int index) {
        final long fd = Files.openRO(paths[index].$());
        Assert.assertTrue(fd > -1);
        return fd;
    }

    /**
     * Leaves an eviction hint queued for a record that has since been retired and reused for a mapping of another
     * file, held by the caller. Returns that mapping of the file at otherIndex, opened as otherFd, {@link #LEN} long.
     */
    private long recycleRecordWithPendingHint(long fd, long otherFd, int otherIndex) {
        final long address = Files.mmap(fd, LEN, 0, Files.MAP_RO, TAG);
        // Queues the hint that goes stale.
        Files.munmap(address, LEN, TAG);

        // Revive the mapping for two users. Growing it for one of them while the other holds it maps a longer
        // mapping that supersedes it in the file cache; releasing the other then retires the superseded record at
        // once, without a hint of its own.
        Assert.assertEquals(address, Files.mmap(fd, LEN, 0, Files.MAP_RO, TAG));
        Assert.assertEquals(address, Files.mmap(fd, LEN, 0, Files.MAP_RO, TAG));
        final long longAddress = Files.mremap(fd, address, LEN, 2 * LEN, 0, Files.MAP_RO, TAG);
        Assert.assertTrue(longAddress > 0);
        Assert.assertNotEquals(address, longAddress);
        Files.munmap(address, LEN, TAG);
        Files.munmap(longAddress, 2 * LEN, TAG);

        // The next cached mapping takes the retired record from the pool.
        final long other = Files.mmap(otherFd, LEN, 0, Files.MAP_RO, TAG);
        Assert.assertTrue(other > 0);
        assertContent(other, LEN, otherIndex);
        return other;
    }
}
