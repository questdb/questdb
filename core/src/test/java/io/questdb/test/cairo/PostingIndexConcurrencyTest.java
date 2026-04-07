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
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

/**
 * Concurrent reader/writer tests for the PostingIndex seqlock protocol.
 * Each test runs in under 1 second by using a small number of commits
 * and short join timeouts.
 */
public class PostingIndexConcurrencyTest extends AbstractCairoTest {

    private static final int BP_BATCH = PostingIndexUtils.BLOCK_CAPACITY;
    private static final int COMMITS = 10;
    private static final int JOIN_MS = 500;

    @Test
    public void testConcurrentBwdReadersWhileWriterCommits() throws Exception {
        runConcurrentTest("conc_bwd", 4, false, true);
    }

    @Test
    public void testConcurrentFwdReadersWhileWriterCommits() throws Exception {
        runConcurrentTest("conc_fwd", 4, true, false);
    }

    @Test
    public void testConcurrentMixedReadersWhileWriterCommits() throws Exception {
        runConcurrentTest("conc_mixed", 4, true, true);
    }

    @Test
    public void testHighContentionManyReaders() throws Exception {
        runConcurrentTest("conc_high", 16, true, false);
    }

    private static void readBackward(String dbRoot, String name, int id,
                                     CountDownLatch writerDone, AtomicInteger committed) {
        try (Path rPath = new Path().of(dbRoot);
             PostingIndexBwdReader reader = new PostingIndexBwdReader(
                     configuration, rPath, name, COLUMN_NAME_TXN_NONE, -1, 0)) {
            while (!Thread.interrupted() && (writerDone.getCount() > 0 || committed.get() < COMMITS)) {
                reader.reloadConditionally();
                RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                long prev = Long.MAX_VALUE;
                int count = 0;
                while (cursor.hasNext()) {
                    long val = cursor.next();
                    if (val >= prev) {
                        throw new AssertionError(
                                "bwd " + id + ": non-descending " + prev + " -> " + val);
                    }
                    prev = val;
                    count++;
                }
                if (count == 0) {
                    throw new AssertionError("bwd " + id + ": zero values");
                }
                if (count % BP_BATCH != 0) {
                    throw new AssertionError("bwd " + id + ": partial batch, count=" + count);
                }
            }
        }
    }

    private static void readForward(String dbRoot, String name, int id,
                                    CountDownLatch writerDone, AtomicInteger committed) {
        try (Path rPath = new Path().of(dbRoot);
             PostingIndexFwdReader reader = new PostingIndexFwdReader(
                     configuration, rPath, name, COLUMN_NAME_TXN_NONE, -1, 0)) {
            while (!Thread.interrupted() && (writerDone.getCount() > 0 || committed.get() < COMMITS)) {
                reader.reloadConditionally();
                RowCursor cursor = reader.getCursor(false, 0, 0, Long.MAX_VALUE);
                long prev = -1;
                int count = 0;
                while (cursor.hasNext()) {
                    long val = cursor.next();
                    if (val <= prev) {
                        throw new AssertionError(
                                "fwd " + id + ": non-ascending " + prev + " -> " + val);
                    }
                    prev = val;
                    count++;
                }
                if (count == 0) {
                    throw new AssertionError("fwd " + id + ": zero values");
                }
                if (count % BP_BATCH != 0) {
                    throw new AssertionError("fwd " + id + ": partial batch, count=" + count);
                }
            }
        }
    }

    private void runConcurrentTest(String name, int numReaders, boolean useFwd, boolean useBwd) throws Exception {
        assertMemoryLeak(64, () -> {
            final String dbRoot = configuration.getDbRoot();
            final AtomicReference<Throwable> error = new AtomicReference<>();
            final AtomicInteger committed = new AtomicInteger(0);
            final CountDownLatch writerDone = new CountDownLatch(1);

            try (Path path = new Path().of(dbRoot)) {
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                    // Seed initial data
                    for (int v = 0; v < BP_BATCH; v++) {
                        writer.add(0, v);
                    }
                    writer.setMaxValue(BP_BATCH - 1);
                    writer.commit();
                    committed.set(1);

                    Thread[] readers = new Thread[numReaders];
                    for (int r = 0; r < numReaders; r++) {
                        final int id = r;
                        // Alternate fwd/bwd when both are requested
                        final boolean forward = useFwd && (!useBwd || r % 2 == 0);
                        readers[r] = new Thread(() -> {
                            try {
                                if (forward) {
                                    readForward(dbRoot, name, id, writerDone, committed);
                                } else {
                                    readBackward(dbRoot, name, id, writerDone, committed);
                                }
                            } catch (Throwable t) {
                                error.compareAndSet(null, t);
                            }
                        });
                        readers[r].setDaemon(true);
                        readers[r].start();
                    }

                    // Writer commits in a tight loop
                    for (int batch = 1; batch < COMMITS; batch++) {
                        long base = (long) batch * BP_BATCH;
                        for (int v = 0; v < BP_BATCH; v++) {
                            writer.add(0, base + v);
                        }
                        writer.setMaxValue(base + BP_BATCH - 1);
                        writer.commit();
                        committed.incrementAndGet();
                    }
                    writerDone.countDown();

                    for (Thread t : readers) {
                        t.join(JOIN_MS);
                        if (t.isAlive()) {
                            t.interrupt();
                        }
                    }
                    // Wait for interrupted threads to fully exit before writer closes
                    for (Thread t : readers) {
                        t.join(JOIN_MS);
                    }

                    if (error.get() != null) {
                        throw new AssertionError("Concurrent reader failed", error.get());
                    }
                }
            }
        });
    }
}
