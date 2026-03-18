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

package io.questdb.test.cairo;

import io.questdb.cairo.idx.PostingIndexBwdReader;
import io.questdb.cairo.idx.PostingIndexFwdReader;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

/**
 * Minimal reproducer for SIGSEGV crash in PostingIndexFwdReader/BwdReader
 * during concurrent reader/writer access.  The crash occurs when the
 * MmapCache shares a mapping between multiple readers: one reader's
 * extend() can trigger mremap (moving the virtual address), while
 * another reader still holds raw addresses into the old mapping.
 * <p>
 * With the fix (bypassFdCache=true for valueMem), each reader gets its
 * own fd and mmap, so extends never interfere across readers.
 */
public class PostingIndexConcurrencyTest extends AbstractCairoTest {

    private static final int BP_BATCH = PostingIndexUtils.BLOCK_CAPACITY;

    @Test
    public void testConcurrentFwdReadersWhileWriterCommits() throws Exception {
        final String dbRoot = configuration.getDbRoot().toString();
        final String name = "conc_fwd_repro";
        final int numReaders = 4;
        final int writerCommits = 200;
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final AtomicInteger committedBatches = new AtomicInteger(0);
        final CountDownLatch writerDone = new CountDownLatch(1);

        try (Path path = new Path().of(dbRoot)) {
            try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                // Seed initial data
                for (int v = 0; v < BP_BATCH; v++) {
                    writer.add(0, v);
                }
                writer.setMaxValue(BP_BATCH - 1);
                writer.commit();
                committedBatches.set(1);

                // Start reader threads
                Thread[] readers = new Thread[numReaders];
                for (int r = 0; r < numReaders; r++) {
                    final int readerId = r;
                    readers[r] = new Thread(() -> {
                        try (Path rPath = new Path().of(dbRoot);
                             PostingIndexFwdReader reader = new PostingIndexFwdReader(
                                     configuration, rPath, name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                            int iterations = 0;
                            while (writerDone.getCount() > 0 || committedBatches.get() <= writerCommits) {
                                reader.reloadConditionally();
                                RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                                long prev = -1;
                                int count = 0;
                                while (cursor.hasNext()) {
                                    long val = cursor.next();
                                    if (val <= prev) {
                                        throw new AssertionError(
                                                "fwd reader " + readerId + ": non-ascending "
                                                        + prev + " -> " + val + " at pos " + count);
                                    }
                                    prev = val;
                                    count++;
                                }
                                if (count % BP_BATCH != 0) {
                                    throw new AssertionError(
                                            "fwd reader " + readerId + ": partial batch, count=" + count);
                                }
                                if (count == 0) {
                                    throw new AssertionError(
                                            "fwd reader " + readerId + ": zero values");
                                }
                                iterations++;
                                // No yield — keep hot to maximize contention
                            }
                        } catch (Throwable t) {
                            error.compareAndSet(null, t);
                        }
                    });
                    readers[r].setDaemon(true);
                    readers[r].start();
                }

                // Writer commits in a tight loop
                for (int batch = 1; batch < writerCommits; batch++) {
                    long base = (long) batch * BP_BATCH;
                    for (int v = 0; v < BP_BATCH; v++) {
                        writer.add(0, base + v);
                    }
                    writer.setMaxValue(base + BP_BATCH - 1);
                    writer.commit();
                    committedBatches.incrementAndGet();
                }
                writerDone.countDown();

                for (Thread t : readers) {
                    t.join(30_000);
                    if (t.isAlive()) {
                        t.interrupt();
                    }
                }

                if (error.get() != null) {
                    throw new AssertionError("Concurrent fwd reader failed", error.get());
                }
            }
        }
    }

    @Test
    public void testConcurrentBwdReadersWhileWriterCommits() throws Exception {
        final String dbRoot = configuration.getDbRoot().toString();
        final String name = "conc_bwd_repro";
        final int numReaders = 4;
        final int writerCommits = 200;
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final AtomicInteger committedBatches = new AtomicInteger(0);
        final CountDownLatch writerDone = new CountDownLatch(1);

        try (Path path = new Path().of(dbRoot)) {
            try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                for (int v = 0; v < BP_BATCH; v++) {
                    writer.add(0, v);
                }
                writer.setMaxValue(BP_BATCH - 1);
                writer.commit();
                committedBatches.set(1);

                Thread[] readers = new Thread[numReaders];
                for (int r = 0; r < numReaders; r++) {
                    final int readerId = r;
                    readers[r] = new Thread(() -> {
                        try (Path rPath = new Path().of(dbRoot);
                             PostingIndexBwdReader reader = new PostingIndexBwdReader(
                                     configuration, rPath, name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                            while (writerDone.getCount() > 0 || committedBatches.get() <= writerCommits) {
                                reader.reloadConditionally();
                                RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                                long prev = Long.MAX_VALUE;
                                int count = 0;
                                while (cursor.hasNext()) {
                                    long val = cursor.next();
                                    if (val >= prev) {
                                        throw new AssertionError(
                                                "bwd reader " + readerId + ": non-descending "
                                                        + prev + " -> " + val + " at pos " + count);
                                    }
                                    prev = val;
                                    count++;
                                }
                                if (count % BP_BATCH != 0) {
                                    throw new AssertionError(
                                            "bwd reader " + readerId + ": partial batch, count=" + count);
                                }
                                if (count == 0) {
                                    throw new AssertionError(
                                            "bwd reader " + readerId + ": zero values");
                                }
                            }
                        } catch (Throwable t) {
                            error.compareAndSet(null, t);
                        }
                    });
                    readers[r].setDaemon(true);
                    readers[r].start();
                }

                for (int batch = 1; batch < writerCommits; batch++) {
                    long base = (long) batch * BP_BATCH;
                    for (int v = 0; v < BP_BATCH; v++) {
                        writer.add(0, base + v);
                    }
                    writer.setMaxValue(base + BP_BATCH - 1);
                    writer.commit();
                    committedBatches.incrementAndGet();
                }
                writerDone.countDown();

                for (Thread t : readers) {
                    t.join(30_000);
                    if (t.isAlive()) {
                        t.interrupt();
                    }
                }

                if (error.get() != null) {
                    throw new AssertionError("Concurrent bwd reader failed", error.get());
                }
            }
        }
    }

    @Test
    public void testConcurrentMixedReadersWhileWriterCommits() throws Exception {
        final String dbRoot = configuration.getDbRoot().toString();
        final String name = "conc_mixed_repro";
        final int writerCommits = 200;
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final CountDownLatch writerDone = new CountDownLatch(1);

        try (Path path = new Path().of(dbRoot)) {
            try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                for (int v = 0; v < BP_BATCH; v++) {
                    writer.add(0, v);
                }
                writer.setMaxValue(BP_BATCH - 1);
                writer.commit();

                // 2 forward + 2 backward readers
                Thread[] threads = new Thread[4];
                for (int r = 0; r < 4; r++) {
                    final int readerId = r;
                    final boolean isForward = r < 2;
                    threads[r] = new Thread(() -> {
                        try {
                            if (isForward) {
                                try (Path rPath = new Path().of(dbRoot);
                                     PostingIndexFwdReader reader = new PostingIndexFwdReader(
                                             configuration, rPath, name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                                    while (writerDone.getCount() > 0) {
                                        reader.reloadConditionally();
                                        RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                                        long prev = -1;
                                        int count = 0;
                                        while (cursor.hasNext()) {
                                            long val = cursor.next();
                                            Assert.assertTrue(
                                                    "fwd " + readerId + ": non-ascending " + prev + " -> " + val,
                                                    val > prev);
                                            prev = val;
                                            count++;
                                        }
                                        Assert.assertTrue("fwd " + readerId + ": count=" + count,
                                                count > 0 && count % BP_BATCH == 0);
                                    }
                                }
                            } else {
                                try (Path rPath = new Path().of(dbRoot);
                                     PostingIndexBwdReader reader = new PostingIndexBwdReader(
                                             configuration, rPath, name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                                    while (writerDone.getCount() > 0) {
                                        reader.reloadConditionally();
                                        RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                                        long prev = Long.MAX_VALUE;
                                        int count = 0;
                                        while (cursor.hasNext()) {
                                            long val = cursor.next();
                                            Assert.assertTrue(
                                                    "bwd " + readerId + ": non-descending " + prev + " -> " + val,
                                                    val < prev);
                                            prev = val;
                                            count++;
                                        }
                                        Assert.assertTrue("bwd " + readerId + ": count=" + count,
                                                count > 0 && count % BP_BATCH == 0);
                                    }
                                }
                            }
                        } catch (Throwable t) {
                            error.compareAndSet(null, t);
                        }
                    });
                    threads[r].setDaemon(true);
                    threads[r].start();
                }

                for (int batch = 1; batch < writerCommits; batch++) {
                    long base = (long) batch * BP_BATCH;
                    for (int v = 0; v < BP_BATCH; v++) {
                        writer.add(0, base + v);
                    }
                    writer.setMaxValue(base + BP_BATCH - 1);
                    writer.commit();
                }
                writerDone.countDown();

                for (Thread t : threads) {
                    t.join(30_000);
                    if (t.isAlive()) {
                        t.interrupt();
                    }
                }

                if (error.get() != null) {
                    throw new AssertionError("Concurrent mixed reader failed", error.get());
                }
            }
        }
    }
}
