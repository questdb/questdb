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

package io.questdb.test.cairo;

import io.questdb.cairo.idx.PostingIndexBwdReader;
import io.questdb.cairo.idx.PostingIndexFwdReader;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
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
    public void testConcurrentEmptyCursor() throws Exception {
        assertMemoryLeak(64, () -> {
            final String dbRoot = configuration.getDbRoot();
            final AtomicReference<Throwable> error = new AtomicReference<>();
            final CountDownLatch writerDone = new CountDownLatch(1);
            final AtomicInteger committed = new AtomicInteger(0);
            final int missingKey = 99;

            try (Path path = new Path().of(dbRoot)) {
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, "conc_empty", COLUMN_NAME_TXN_NONE)) {
                    for (int v = 0; v < BP_BATCH; v++) writer.add(0, v);
                    writer.setMaxValue(BP_BATCH - 1);
                    writer.commit();
                    committed.set(1);

                    Thread[] readers = new Thread[4];
                    for (int r = 0; r < readers.length; r++) {
                        final int id = r;
                        readers[r] = new Thread(() -> {
                            try (Path rPath = new Path().of(dbRoot);
                                 PostingIndexFwdReader reader = new PostingIndexFwdReader(
                                         configuration, rPath, "conc_empty", COLUMN_NAME_TXN_NONE, -1, 0)) {
                                while (!Thread.interrupted() && writerDone.getCount() > 0) {
                                    reader.reloadConditionally();
                                    RowCursor cursor = reader.getCursor(missingKey, 0, Long.MAX_VALUE);
                                    if (cursor.hasNext()) {
                                        throw new AssertionError("id " + id + ": missing key " + missingKey + " yielded a value");
                                    }
                                    Misc.free(cursor);
                                }
                            } catch (Throwable t) {
                                error.compareAndSet(null, t);
                            }
                        });
                        readers[r].setDaemon(true);
                        readers[r].start();
                    }

                    for (int batch = 1; batch < COMMITS; batch++) {
                        long base = (long) batch * BP_BATCH;
                        for (int v = 0; v < BP_BATCH; v++) writer.add(0, base + v);
                        writer.setMaxValue(base + BP_BATCH - 1);
                        writer.commit();
                        committed.incrementAndGet();
                    }
                    writerDone.countDown();

                    joinReaders(readers);

                    if (error.get() != null) {
                        throw new AssertionError("Empty cursor reader failed", error.get());
                    }
                }
            }
        });
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
    public void testConcurrentMultiKeyBwdReaders() throws Exception {
        runMultiKeyConcurrentTest("conc_mk_bwd", 32, 4, false);
    }

    @Test
    public void testConcurrentMultiKeyFwdReaders() throws Exception {
        runMultiKeyConcurrentTest("conc_mk_fwd", 32, 4, true);
    }

    @Test
    public void testConcurrentReadersThroughSealCycle() throws Exception {
        assertMemoryLeak(64, () -> {
            final String dbRoot = configuration.getDbRoot();
            final AtomicReference<Throwable> error = new AtomicReference<>();
            final AtomicInteger committed = new AtomicInteger(0);
            final CountDownLatch writerDone = new CountDownLatch(1);

            try (Path path = new Path().of(dbRoot)) {
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, "conc_seal", COLUMN_NAME_TXN_NONE)) {
                    for (int v = 0; v < BP_BATCH; v++) writer.add(0, v);
                    writer.setMaxValue(BP_BATCH - 1);
                    writer.commit();
                    writer.seal();
                    committed.set(1);

                    Thread[] readers = new Thread[3];
                    for (int r = 0; r < readers.length; r++) {
                        final int id = r;
                        readers[r] = new Thread(() -> {
                            try {
                                readForward(dbRoot, "conc_seal", id, writerDone, committed);
                            } catch (Throwable t) {
                                error.compareAndSet(null, t);
                            }
                        });
                        readers[r].setDaemon(true);
                        readers[r].start();
                    }

                    for (int batch = 1; batch < COMMITS; batch++) {
                        long base = (long) batch * BP_BATCH;
                        for (int v = 0; v < BP_BATCH; v++) writer.add(0, base + v);
                        writer.setMaxValue(base + BP_BATCH - 1);
                        writer.commit();
                        committed.incrementAndGet();
                        if ((batch & 1) == 0) {
                            writer.seal();
                            committed.incrementAndGet();
                        }
                    }
                    writerDone.countDown();

                    joinReaders(readers);

                    if (error.get() != null) {
                        throw new AssertionError("Seal-cycle reader failed", error.get());
                    }
                }
            }
        });
    }

    @Test
    public void testConcurrentReadersWhileWriterRollsBack() throws Exception {
        assertMemoryLeak(64, () -> {
            final String dbRoot = configuration.getDbRoot();
            final AtomicReference<Throwable> error = new AtomicReference<>();
            final AtomicInteger committed = new AtomicInteger(0);
            final CountDownLatch writerDone = new CountDownLatch(1);
            final int rounds = 5;

            try (Path path = new Path().of(dbRoot)) {
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, "conc_rb", COLUMN_NAME_TXN_NONE)) {
                    // Seed initial data
                    for (int v = 0; v < BP_BATCH * 3; v++) {
                        writer.add(0, v);
                    }
                    writer.setMaxValue(BP_BATCH * 3 - 1);
                    writer.commit();
                    writer.seal();
                    committed.set(1);

                    Thread[] readers = new Thread[4];
                    for (int r = 0; r < readers.length; r++) {
                        final int id = r;
                        readers[r] = new Thread(() -> {
                            try {
                                readForward(dbRoot, "conc_rb", id, writerDone, committed);
                            } catch (Throwable t) {
                                // During rollback, readers may see empty state momentarily
                                if (!t.getMessage().contains("zero values")) {
                                    error.compareAndSet(null, t);
                                }
                            }
                        });
                        readers[r].setDaemon(true);
                        readers[r].start();
                    }

                    // Writer does repeated write → rollback cycles
                    for (int round = 0; round < rounds; round++) {
                        long base = (long) (round + 1) * BP_BATCH * 3;
                        for (int v = 0; v < BP_BATCH * 3; v++) {
                            writer.add(0, base + v);
                        }
                        writer.setMaxValue(base + BP_BATCH * 3 - 1);
                        writer.commit();
                        committed.incrementAndGet();

                        // Rollback half the data
                        writer.rollbackValues(base + BP_BATCH - 1);
                        committed.incrementAndGet();
                    }
                    writerDone.countDown();

                    for (Thread t : readers) {
                        t.join(JOIN_MS);
                        if (t.isAlive()) t.interrupt();
                    }
                    for (Thread t : readers) {
                        t.join(JOIN_MS);
                    }

                    if (error.get() != null) {
                        throw new AssertionError("Concurrent reader failed during rollback", error.get());
                    }
                }
            }
        });
    }

    @Test
    public void testConcurrentReadersWhileWriterTruncates() throws Exception {
        assertMemoryLeak(64, () -> {
            final String dbRoot = configuration.getDbRoot();
            final AtomicReference<Throwable> error = new AtomicReference<>();
            final CountDownLatch writerDone = new CountDownLatch(1);
            final AtomicInteger committed = new AtomicInteger(0);
            final int rounds = 3;

            try (Path path = new Path().of(dbRoot)) {
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, "conc_trunc", COLUMN_NAME_TXN_NONE)) {
                    // Seed data
                    for (int v = 0; v < BP_BATCH * 3; v++) {
                        writer.add(0, v);
                    }
                    writer.setMaxValue(BP_BATCH * 3 - 1);
                    writer.commit();
                    writer.seal();
                    committed.set(1);

                    Thread[] readers = new Thread[4];
                    for (int r = 0; r < readers.length; r++) {
                        final int id = r;
                        readers[r] = new Thread(() -> {
                            try (Path rPath = new Path().of(dbRoot);
                                 PostingIndexFwdReader reader = new PostingIndexFwdReader(
                                         configuration, rPath, "conc_trunc", COLUMN_NAME_TXN_NONE, -1, 0)) {
                                while (!Thread.interrupted() && writerDone.getCount() > 0) {
                                    try {
                                        reader.reloadConditionally();
                                        RowCursor cursor = reader.getCursor(0, 0, Long.MAX_VALUE);
                                        long prev = -1;
                                        while (cursor.hasNext()) {
                                            long val = cursor.next();
                                            if (val <= prev) {
                                                throw new AssertionError(
                                                        "fwd " + id + ": non-ascending " + prev + " -> " + val);
                                            }
                                            prev = val;
                                        }
                                        Misc.free(cursor);
                                    } catch (io.questdb.cairo.CairoException e) {
                                        // Transient corrupt reads are expected during truncate
                                        // cycles — the reader's seqlock snapshot can lag the
                                        // writer's file replacement.
                                    }
                                }
                            } catch (Throwable t) {
                                error.compareAndSet(null, t);
                            }
                        });
                        readers[r].setDaemon(true);
                        readers[r].start();
                    }

                    // Writer does write → truncate → write cycles
                    for (int round = 0; round < rounds; round++) {
                        writer.truncate();
                        committed.incrementAndGet();

                        long base = (long) round * BP_BATCH * 3;
                        for (int v = 0; v < BP_BATCH * 3; v++) {
                            writer.add(0, base + v);
                        }
                        writer.setMaxValue(base + BP_BATCH * 3 - 1);
                        writer.commit();
                        writer.seal();
                        committed.incrementAndGet();
                    }
                    writerDone.countDown();

                    for (Thread t : readers) {
                        t.join(JOIN_MS);
                        if (t.isAlive()) t.interrupt();
                    }
                    for (Thread t : readers) {
                        t.join(JOIN_MS);
                    }

                    if (error.get() != null) {
                        throw new AssertionError("Concurrent reader failed during truncate", error.get());
                    }
                }
            }
        });
    }

    @Test
    public void testConcurrentReadersWithRowRange() throws Exception {
        assertMemoryLeak(64, () -> {
            final String dbRoot = configuration.getDbRoot();
            final AtomicReference<Throwable> error = new AtomicReference<>();
            final AtomicInteger committed = new AtomicInteger(0);
            final CountDownLatch writerDone = new CountDownLatch(1);

            try (Path path = new Path().of(dbRoot)) {
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, "conc_range", COLUMN_NAME_TXN_NONE)) {
                    // Seed 3 blocks so readers with higher rowLo actually skip leading blocks.
                    for (int v = 0; v < BP_BATCH * 3; v++) writer.add(0, v);
                    writer.setMaxValue(BP_BATCH * 3 - 1);
                    writer.commit();
                    committed.set(1);

                    Thread[] readers = new Thread[4];
                    for (int r = 0; r < readers.length; r++) {
                        final int id = r;
                        final long rowLo = (long) r * BP_BATCH;
                        readers[r] = new Thread(() -> {
                            try (Path rPath = new Path().of(dbRoot);
                                 PostingIndexFwdReader reader = new PostingIndexFwdReader(
                                         configuration, rPath, "conc_range", COLUMN_NAME_TXN_NONE, -1, 0)) {
                                while (!Thread.interrupted() && (writerDone.getCount() > 0 || committed.get() < COMMITS)) {
                                    reader.reloadConditionally();
                                    RowCursor cursor = reader.getCursor(0, rowLo, Long.MAX_VALUE);
                                    long prev = -1;
                                    while (cursor.hasNext()) {
                                        long delta = cursor.next();
                                        if (delta < 0) {
                                            throw new AssertionError("id " + id + ": negative delta " + delta);
                                        }
                                        long absolute = delta + rowLo;
                                        if (absolute <= prev) {
                                            throw new AssertionError("id " + id + ": non-ascending " + prev + " -> " + absolute);
                                        }
                                        prev = absolute;
                                    }
                                    Misc.free(cursor);
                                }
                            } catch (Throwable t) {
                                error.compareAndSet(null, t);
                            }
                        });
                        readers[r].setDaemon(true);
                        readers[r].start();
                    }

                    for (int batch = 1; batch < COMMITS; batch++) {
                        long base = (long) batch * BP_BATCH * 3;
                        for (int v = 0; v < BP_BATCH * 3; v++) writer.add(0, base + v);
                        writer.setMaxValue(base + BP_BATCH * 3 - 1);
                        writer.commit();
                        committed.incrementAndGet();
                    }
                    writerDone.countDown();

                    joinReaders(readers);

                    if (error.get() != null) {
                        throw new AssertionError("Range reader failed", error.get());
                    }
                }
            }
        });
    }

    @Test
    public void testHighContentionManyReaders() throws Exception {
        runConcurrentTest("conc_high", 16, true, false);
    }

    /**
     * Red test for the torn-read bug in {@link PostingIndexUtils#readSealTxnFromKeyFd}.
     * The function performs four independent {@code pread} calls to validate the
     * A-B seqlock, then issues a fifth, separate {@code pread} for SEAL_TXN with
     * no post-validation. A writer that bumps SEQUENCE_START on the chosen page
     * between the seqlock check and the SEAL_TXN read can hand the caller a
     * sealTxn from a different generation than the seqlock it validated.
     * <p>
     * Layout:
     * <pre>
     *   page A: seqStart=2, seqEnd=2, sealTxn=100   (newest, picked by reader)
     *   page B: seqStart=1, seqEnd=1, sealTxn=99    (older)
     * </pre>
     * The injected FilesFacade fires on the 5th {@code readNonNegativeLong}
     * (the SEAL_TXN read) and rewrites page A as if a writer had started a new
     * seal: seqStart=4, sealTxn=200. The caller then reads sealTxn=200.
     * <p>
     * After the fix, the function must re-read SEQUENCE_START on the chosen
     * page after the SEAL_TXN read; a mismatch must produce either the
     * fallback sealTxn from page B (99) or -1. Returning 200 means the caller
     * walked away with a sealTxn from a generation it never validated.
     */
    @Test
    public void testReadSealTxnFromKeyFdRejectsTornSealTxnRead() throws Exception {
        assertMemoryLeak(64, () -> {
            try (Path path = new Path().of(configuration.getDbRoot()).concat("torn_seal_test.pk")) {
                final LPSZ pathLpsz = path.$();
                seedConsistentKeyFile(TestFilesFacadeImpl.INSTANCE, pathLpsz);

                final AtomicInteger readCount = new AtomicInteger();
                FilesFacade injectedFf = new TestFilesFacadeImpl() {
                    @Override
                    public long readNonNegativeLong(long readFd, long offset) {
                        // The 5th readNonNegativeLong is the SEAL_TXN fetch (calls 1..4
                        // are the four seqlock fields). Fire the writer-mid-flight injection
                        // exactly once, just before that read goes to the kernel.
                        if (readCount.incrementAndGet() == 5) {
                            long w = openRW(pathLpsz, 0);
                            if (w > 0) {
                                try {
                                    writeRawLong(this, w,
                                            PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START,
                                            4L);
                                    writeRawLong(this, w,
                                            PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEAL_TXN,
                                            200L);
                                } finally {
                                    close(w);
                                }
                            }
                        }
                        return super.readNonNegativeLong(readFd, offset);
                    }
                };

                long readFd = injectedFf.openRO(pathLpsz);
                Assert.assertTrue("openRO failed for " + path, readFd > 0);
                try {
                    long sealTxn = PostingIndexUtils.readSealTxnFromKeyFd(injectedFf, readFd);
                    Assert.assertNotEquals(
                            "readSealTxnFromKeyFd returned 200, the post-mutation sealTxn from a "
                                    + "generation (seq=4) the reader never seqlock-validated. The reader "
                                    + "observed page A consistent at seq=2; once the writer bumped page A "
                                    + "to seq=4 between the seqlock check and the SEAL_TXN read, the "
                                    + "function must re-validate and return 99 (fallback to page B at "
                                    + "seq=1) or -1 (race detected).",
                            200L, sealTxn);
                    Assert.assertTrue(
                            "expected sealTxn in {99, -1} after a mid-read seqStart flip on page A; got " + sealTxn,
                            sealTxn == 99L || sealTxn == -1L);
                } finally {
                    injectedFf.close(readFd);
                }
            }
        });
    }

    private static void joinReaders(Thread[] readers) throws InterruptedException {
        for (Thread t : readers) {
            t.join(JOIN_MS);
            if (t.isAlive()) {
                t.interrupt();
            }
        }
        for (Thread t : readers) {
            t.join(JOIN_MS);
        }
    }

    private static void readBackward(String dbRoot, String name, int id,
                                     CountDownLatch writerDone, AtomicInteger committed) {
        try (Path rPath = new Path().of(dbRoot);
             PostingIndexBwdReader reader = new PostingIndexBwdReader(
                     configuration, rPath, name, COLUMN_NAME_TXN_NONE, -1, 0, null, null, 0)) {
            while (!Thread.interrupted() && (writerDone.getCount() > 0 || committed.get() < COMMITS)) {
                reader.reloadConditionally();
                RowCursor cursor = reader.getCursor(0, 0, Long.MAX_VALUE);
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
                Misc.free(cursor);
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
                RowCursor cursor = reader.getCursor(0, 0, Long.MAX_VALUE);
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
                Misc.free(cursor);
            }
        }
    }

    private static void readMultiKeyBackward(String dbRoot, String name, int id, int numKeys,
                                             CountDownLatch writerDone, AtomicInteger committed) {
        try (Path rPath = new Path().of(dbRoot);
             PostingIndexBwdReader reader = new PostingIndexBwdReader(
                     configuration, rPath, name, COLUMN_NAME_TXN_NONE, -1, 0, null, null, 0)) {
            while (!Thread.interrupted() && (writerDone.getCount() > 0 || committed.get() < COMMITS)) {
                reader.reloadConditionally();
                for (int k = 0; k < numKeys; k++) {
                    RowCursor cursor = reader.getCursor(k, 0, Long.MAX_VALUE);
                    long prev = Long.MAX_VALUE;
                    int count = 0;
                    while (cursor.hasNext()) {
                        long val = cursor.next();
                        if (val >= prev) {
                            throw new AssertionError(
                                    "bwd " + id + " key=" + k + ": non-descending " + prev + " -> " + val);
                        }
                        if (val % numKeys != k) {
                            throw new AssertionError(
                                    "bwd " + id + " key=" + k + ": value " + val + " belongs to key " + (val % numKeys));
                        }
                        prev = val;
                        count++;
                    }
                    if (count == 0) {
                        throw new AssertionError("bwd " + id + " key=" + k + ": zero values");
                    }
                    Misc.free(cursor);
                }
            }
        }
    }

    private static void readMultiKeyForward(String dbRoot, String name, int id, int numKeys,
                                            CountDownLatch writerDone, AtomicInteger committed) {
        try (Path rPath = new Path().of(dbRoot);
             PostingIndexFwdReader reader = new PostingIndexFwdReader(
                     configuration, rPath, name, COLUMN_NAME_TXN_NONE, -1, 0)) {
            while (!Thread.interrupted() && (writerDone.getCount() > 0 || committed.get() < COMMITS)) {
                reader.reloadConditionally();
                for (int k = 0; k < numKeys; k++) {
                    RowCursor cursor = reader.getCursor(k, 0, Long.MAX_VALUE);
                    long prev = -1;
                    int count = 0;
                    while (cursor.hasNext()) {
                        long val = cursor.next();
                        if (val <= prev) {
                            throw new AssertionError(
                                    "fwd " + id + " key=" + k + ": non-ascending " + prev + " -> " + val);
                        }
                        if (val % numKeys != k) {
                            throw new AssertionError(
                                    "fwd " + id + " key=" + k + ": value " + val + " belongs to key " + (val % numKeys));
                        }
                        prev = val;
                        count++;
                    }
                    if (count == 0) {
                        throw new AssertionError("fwd " + id + " key=" + k + ": zero values");
                    }
                    Misc.free(cursor);
                }
            }
        }
    }

    private static void seedConsistentKeyFile(FilesFacade ff, LPSZ path) {
        long fd = ff.openRW(path, configuration.getWriterFileOpenOpts());
        Assert.assertTrue("openRW failed for " + path, fd > 0);
        try {
            Assert.assertTrue("allocate failed", ff.allocate(fd, PostingIndexUtils.KEY_FILE_RESERVED));
            // Page A: newest consistent snapshot (seq=2, sealTxn=100)
            writeRawLong(ff, fd, PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START, 2L);
            writeRawLong(ff, fd, PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEAL_TXN, 100L);
            writeRawLong(ff, fd, PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_END, 2L);
            // Page B: older consistent snapshot (seq=1, sealTxn=99)
            writeRawLong(ff, fd, PostingIndexUtils.PAGE_B_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START, 1L);
            writeRawLong(ff, fd, PostingIndexUtils.PAGE_B_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEAL_TXN, 99L);
            writeRawLong(ff, fd, PostingIndexUtils.PAGE_B_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_END, 1L);
        } finally {
            ff.close(fd);
        }
    }

    private static void writeRawLong(FilesFacade ff, long fd, long offset, long value) {
        long bufAddr = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putLong(bufAddr, value);
            Assert.assertEquals(
                    "short write at offset " + offset,
                    Long.BYTES,
                    ff.write(fd, bufAddr, Long.BYTES, offset));
        } finally {
            Unsafe.free(bufAddr, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
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

                    joinReaders(readers);

                    if (error.get() != null) {
                        throw new AssertionError("Concurrent reader failed", error.get());
                    }
                }
            }
        });
    }

    private void runMultiKeyConcurrentTest(String name, int numKeys, int numReaders, boolean forward) throws Exception {
        assertMemoryLeak(64, () -> {
            final String dbRoot = configuration.getDbRoot();
            final AtomicReference<Throwable> error = new AtomicReference<>();
            final AtomicInteger committed = new AtomicInteger(0);
            final CountDownLatch writerDone = new CountDownLatch(1);

            try (Path path = new Path().of(dbRoot)) {
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                    for (int k = 0; k < numKeys; k++) {
                        writer.add(k, k);
                    }
                    writer.setMaxValue(numKeys - 1);
                    writer.commit();
                    committed.set(1);

                    Thread[] readers = new Thread[numReaders];
                    for (int r = 0; r < numReaders; r++) {
                        final int id = r;
                        readers[r] = new Thread(() -> {
                            try {
                                if (forward) {
                                    readMultiKeyForward(dbRoot, name, id, numKeys, writerDone, committed);
                                } else {
                                    readMultiKeyBackward(dbRoot, name, id, numKeys, writerDone, committed);
                                }
                            } catch (Throwable t) {
                                error.compareAndSet(null, t);
                            }
                        });
                        readers[r].setDaemon(true);
                        readers[r].start();
                    }

                    for (int batch = 1; batch < COMMITS; batch++) {
                        long base = (long) batch * numKeys;
                        for (int k = 0; k < numKeys; k++) {
                            writer.add(k, base + k);
                        }
                        writer.setMaxValue(base + numKeys - 1);
                        writer.commit();
                        committed.incrementAndGet();
                    }
                    writerDone.countDown();

                    joinReaders(readers);

                    if (error.get() != null) {
                        throw new AssertionError("Multi-key concurrent reader failed", error.get());
                    }
                }
            }
        });
    }
}
