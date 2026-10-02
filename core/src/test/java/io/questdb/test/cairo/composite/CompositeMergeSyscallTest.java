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

package io.questdb.test.cairo.composite;

import io.questdb.PropertyKey;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.PartitionGeometry;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.std.Files;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.LPSZ;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Counts the file-system calls - mmap, munmap, fallocate and friends - that WAL apply makes with a real worker pool
 * running the O3 and column jobs in parallel, the way a server does, and holds them to a budget that does not grow
 * with the number of pieces or blocks.
 * <p>
 * Two shapes, both seen in a TSBS profile as mmap/munmap pairs on the shared-write workers:
 * <ul>
 *     <li>A MERGE into every piece of a 200-piece composite partition. The plan opens every column file once, but
 *     each MERGE still maps and unmaps the target's tail per column - pieces x columns map/unmap pairs per plan.</li>
 *     <li>A run of WAL blocks over the same set of segments. Each block maps every segment's column files afresh in
 *     the parallel shuffle tasks and unmaps them all when the block ends, so a segment that feeds ten blocks is
 *     mapped ten times - blocks x segments x columns map/unmap pairs.</li>
 *     <li>The same, with more segments than the server's default WAL fd cache (30) holds. Once the cache fills,
 *     closing a block closes every cached fd, so each block reopens every segment's column files as well.</li>
 * </ul>
 * The budgets are what the work needs, not what it takes today: these tests fail until the mappings and fds are
 * kept across MERGEs and across blocks.
 */
public class CompositeMergeSyscallTest extends AbstractCairoTest {
    private static final int DOUBLE_COLUMNS = 10;
    // s SYMBOL, d0..d9 DOUBLE, ts TIMESTAMP: the shape of TSBS cpu-only, give or take its tag columns.
    private static final int COLUMN_COUNT = DOUBLE_COLUMNS + 2;
    private static final int MERGE_ROWS_PER_PIECE = 3;
    private static final long MINUTE = 60_000_000L;
    private static final int TARGET_PIECES = 200;
    private static final int WAL_ROUNDS = 10;
    private static final int WAL_ROWS_PER_COMMIT = 500;
    private static final int WORKER_COUNT = 4;

    @Test
    public void testMergeIntoEveryPieceOfManyPieceCompositePartition() throws Exception {
        final SyscallCountingFilesFacade ff = new SyscallCountingFilesFacade();
        assertMemoryLeak(ff, () -> {
            // Pooled frame columns capture the FilesFacade they were built with; start from a fresh pool.
            engine.resetFrameFactory();
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "8K");
            // Small piece floor, so each backdated batch below founds pieces of its own instead of merging into one
            // big piece, and a piece-count cap well above the target, so nothing compacts the pieces back together.
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 64);
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_PIECE_THRESHOLD, 100_000);

            execute("CREATE TABLE x (" + columnsDdl() + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            // 2024-01-01 a row every 10 seconds, plus a later day so 2024-01-01 is never the active partition and
            // every batch below goes through the O3 path.
            execute("INSERT INTO x SELECT " + valuesSelect("x") + ", timestamp_sequence('2024-01-01', 10_000_000L) ts" +
                    " FROM long_sequence(8640)");
            execute("INSERT INTO x SELECT " + valuesSelect("x") + ", '2024-01-02T00:00:00'::TIMESTAMP ts FROM long_sequence(1)");
            drainWalQueue();

            final TableToken tableToken = engine.verifyTableName("x");
            final long day = MicrosTimestampDriver.floor("2024-01-01T00:00:00.000000Z");
            final WorkerPool pool = new TestWorkerPool(WORKER_COUNT, node1.getMetrics());
            WorkerPoolUtils.setupWriterJobs(pool, engine);
            pool.start(LOG);
            final LongList before = new LongList();
            final LongList after = new LongList();
            final StringBuilder mergeSql = new StringBuilder();
            int mergeRows = 0;
            int batch = 0;
            int targetedPieces = 0;
            try {
                // Backdated batches, each in a slot of the day no earlier batch touched, one commit apiece: every
                // batch cuts the piece it lands in and founds a piece of its own.
                while (snapshotPieces(tableToken, day, before) < TARGET_PIECES) {
                    Assert.assertTrue("could not cut the day into " + TARGET_PIECES + " pieces, got "
                            + before.size() / 4 + " after " + batch + " batches", batch < 1440 / 7);
                    final long start = day + batch * 7 * MINUTE + 3 * MINUTE + 3_000_000L;
                    execute("INSERT INTO x SELECT " + valuesSelect("x + " + (100_000 * (batch + 1))) + ", "
                            + "timestamp_sequence(" + start + "::TIMESTAMP, 2_000_000L) ts FROM long_sequence(20)");
                    drainWalQueue();
                    batch++;
                }
                Assert.assertFalse("table suspended while cutting pieces", engine.getTableSequencerAPI().isSuspended(tableToken));
                final long eBefore = readE(tableToken, day);

                // One commit that lands rows strictly inside every piece: a plan of one MERGE per piece.
                mergeSql.append("INSERT INTO x VALUES ");
                for (int p = 0, n = before.size() / 4; p < n; p++) {
                    final long tsLo = before.getQuick(p * 4);
                    final long tsHi = before.getQuick(p * 4 + 1);
                    if (tsHi - tsLo <= MERGE_ROWS_PER_PIECE + 1) {
                        continue;
                    }
                    targetedPieces++;
                    for (int r = 1; r <= MERGE_ROWS_PER_PIECE; r++) {
                        // Odd microseconds, so no row ties with one already in the piece.
                        final long ts = tsLo + (tsHi - tsLo) * r / (MERGE_ROWS_PER_PIECE + 1) | 1;
                        if (mergeRows++ > 0) {
                            mergeSql.append(", ");
                        }
                        appendValuesRow(mergeSql, 9_000_000 + mergeRows, ts);
                    }
                }

                ff.arm();
                execute(mergeSql);
                drainWalQueue();
                ff.disarm();

                Assert.assertFalse("table suspended by the merge", engine.getTableSequencerAPI().isSuspended(tableToken));
                snapshotPieces(tableToken, day, after);
                final int piecesBefore = before.size() / 4;
                final int piecesAfter = after.size() / 4;
                // The merged images land at the tail one after another, so they fold into a few pieces. A piece
                // the commit rewrote is one whose whole range now lies in a piece at or above the old extent.
                int mergedPieces = 0;
                for (int p = 0; p < piecesBefore; p++) {
                    final long tsLo = before.getQuick(p * 4);
                    final long tsHi = before.getQuick(p * 4 + 1);
                    for (int q = 0; q < piecesAfter; q++) {
                        if (after.getQuick(q * 4) <= tsLo && tsHi <= after.getQuick(q * 4 + 1)) {
                            if (after.getQuick(q * 4 + 2) >= eBefore) {
                                mergedPieces++;
                            }
                            break;
                        }
                    }
                }
                final String report = "merge into every piece [piecesBefore=" + piecesBefore
                        + ", piecesAfter=" + piecesAfter
                        + ", targetedPieces=" + targetedPieces
                        + ", mergedPieces=" + mergedPieces
                        + ", mergeRows=" + mergeRows
                        + ", columns=" + COLUMN_COUNT
                        + ", workers=" + WORKER_COUNT
                        + "] " + ff.report();
                LOG.info().$(report).$();

                Assert.assertTrue("the commit did not MERGE into most pieces: " + report, mergedPieces >= targetedPieces * 3 / 4);
                // The work this commit does is all of one plan, over files it can open, size and map once. A
                // budget of a few per column holds whatever the piece count; one map per MERGE per column does not.
                final long budget = 4L * COLUMN_COUNT;
                Assert.assertTrue("fallocate per piece: " + report, ff.total(SyscallCountingFilesFacade.ALLOCATE) <= budget);
                Assert.assertTrue("mmap per piece: " + report, ff.total(SyscallCountingFilesFacade.MMAP) <= budget);
                Assert.assertTrue("munmap per piece: " + report, ff.total(SyscallCountingFilesFacade.MUNMAP) <= budget);
            } finally {
                pool.halt();
            }

            assertQuery("SELECT count() c FROM x").noRandomAccess().expectSize().returns(
                    "c\n" + (8641 + 20 * batch + mergeRows) + "\n"
            );
        });
    }

    @Test
    public void testWalBlocksOverMoreSegmentsThanFdCacheOpenEachSegmentOnce() throws Exception {
        // The server's default WAL fd cache (tests run with 1000), against one segment more than it holds, as
        // with TSBS's 32 loader connections: when the cache fills, closeWalFiles() closes every cached fd.
        final int walWriterCount = 32;
        node1.setProperty(PropertyKey.CAIRO_WAL_MAX_SEGMENT_FILE_DESCRIPTORS_CACHE, 30);
        final SyscallCountingFilesFacade ff = new SyscallCountingFilesFacade();
        final String report = applyWalBlocksOverSameSegments(ff, walWriterCount);
        // Opening each segment's column files once for all the blocks it feeds costs segments x columns.
        final long budget = 2L * walWriterCount * COLUMN_COUNT + 4L * COLUMN_COUNT;
        Assert.assertTrue("WAL segment files reopened per block: " + report, ff.total(SyscallCountingFilesFacade.OPEN) <= budget);
        Assert.assertTrue("WAL segment files closed per block: " + report, ff.total(SyscallCountingFilesFacade.CLOSE) <= budget);
    }

    @Test
    public void testWalBlocksOverSameSegmentsMapEachSegmentOnce() throws Exception {
        final int walWriterCount = 16;
        final SyscallCountingFilesFacade ff = new SyscallCountingFilesFacade();
        final String report = applyWalBlocksOverSameSegments(ff, walWriterCount);
        // Mapping each segment's columns once for all the blocks it feeds costs segments x columns; mapping them
        // once per block costs that many again for every block. The budget sits between the two: two passes'
        // worth, plus the partition's own columns.
        final long budget = 2L * walWriterCount * COLUMN_COUNT + 4L * COLUMN_COUNT;
        Assert.assertTrue("WAL segment columns mapped per block: " + report, ff.total(SyscallCountingFilesFacade.MMAP) <= budget);
        Assert.assertTrue("WAL segment columns unmapped per block: " + report, ff.total(SyscallCountingFilesFacade.MUNMAP) <= budget);
    }

    private static void appendValuesRow(StringBuilder sink, long id, long ts) {
        sink.append("('h").append(id % 100).append('\'');
        for (int d = 0; d < DOUBLE_COLUMNS; d++) {
            sink.append(", ").append(id + d).append(".0");
        }
        sink.append(", ").append(ts).append("::TIMESTAMP)");
    }

    private static String columnsDdl() {
        final StringBuilder sink = new StringBuilder("s SYMBOL");
        for (int d = 0; d < DOUBLE_COLUMNS; d++) {
            sink.append(", d").append(d).append(" DOUBLE");
        }
        return sink.append(", ts TIMESTAMP").toString();
    }

    private static long readE(TableToken tableToken, long partitionTimestamp) {
        try (TableReader reader = engine.getReader(tableToken)) {
            return reader.getGeometry().getE(reader.getTxFile().getPartitionIndex(partitionTimestamp));
        }
    }

    /**
     * Fills {@code sink} with (tsLo, tsHi, rowOffset, rowCount) per piece of the partition, and returns the piece count.
     */
    private static int snapshotPieces(TableToken tableToken, long partitionTimestamp, LongList sink) {
        sink.clear();
        try (TableReader reader = engine.getReader(tableToken)) {
            final PartitionGeometry geometry = reader.getGeometry();
            final int partitionIndex = reader.getTxFile().getPartitionIndex(partitionTimestamp);
            final int pieceCount = geometry.getPieceCount(partitionIndex);
            for (int p = 0; p < pieceCount; p++) {
                sink.add(
                        geometry.getPieceTimestampLo(partitionIndex, p),
                        geometry.getPieceTimestampHi(partitionIndex, p),
                        geometry.getPieceRowOffset(partitionIndex, p),
                        geometry.getPieceRowCount(partitionIndex, p)
                );
            }
            return pieceCount;
        }
    }

    private static String valuesSelect(String idExpr) {
        final StringBuilder sink = new StringBuilder("'h' || ((").append(idExpr).append(") % 100) s");
        for (int d = 0; d < DOUBLE_COLUMNS; d++) {
            sink.append(", (").append(idExpr).append(") + ").append(d).append(".0 d").append(d);
        }
        return sink.toString();
    }

    /**
     * Commits WAL_ROUNDS rounds of one transaction per WAL writer, with every writer held open so each owns a
     * segment for the whole run, then applies them on a worker pool in blocks of exactly one round: every block
     * reads every segment, and every segment feeds every block. Counts only the apply.
     */
    private String applyWalBlocksOverSameSegments(SyscallCountingFilesFacade ff, int walWriterCount) throws Exception {
        final String[] report = new String[1];
        assertMemoryLeak(ff, () -> {
            node1.setProperty(PropertyKey.DEBUG_WAL_APPLY_MAX_TXN_BLOCK_SIZE, walWriterCount);
            execute("CREATE TABLE x (" + columnsDdl() + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            final TableToken tableToken = engine.verifyTableName("x");

            final ObjList<WalWriter> walWriters = new ObjList<>();
            try {
                for (int w = 0; w < walWriterCount; w++) {
                    walWriters.add(engine.getWalWriter(tableToken));
                }
                // Rows of all writers interleave in time, so every block has to sort across all its segments; each
                // round starts above the previous one, so the sorted block appends.
                final long base = MicrosTimestampDriver.floor("2024-01-01T00:00:00.000000Z");
                for (int r = 0; r < WAL_ROUNDS; r++) {
                    for (int w = 0; w < walWriterCount; w++) {
                        final WalWriter walWriter = walWriters.getQuick(w);
                        for (int i = 0; i < WAL_ROWS_PER_COMMIT; i++) {
                            final long rowId = ((long) r * WAL_ROWS_PER_COMMIT + i) * walWriterCount + w;
                            final TableWriter.Row row = walWriter.newRow(base + rowId * 1_000_000L);
                            row.putSym(0, "h" + (rowId % 100));
                            for (int d = 0; d < DOUBLE_COLUMNS; d++) {
                                row.putDouble(1 + d, rowId + d);
                            }
                            row.append();
                        }
                        walWriter.commit();
                    }
                }
            } finally {
                Misc.freeObjListAndClear(walWriters);
            }

            final WorkerPool pool = new TestWorkerPool(WORKER_COUNT, node1.getMetrics());
            WorkerPoolUtils.setupWriterJobs(pool, engine);
            pool.start(LOG);
            try {
                ff.arm();
                drainWalQueue();
                ff.disarm();
            } finally {
                pool.halt();
            }

            Assert.assertFalse("table suspended", engine.getTableSequencerAPI().isSuspended(tableToken));
            report[0] = "WAL blocks over the same segments [blocks=" + WAL_ROUNDS
                    + ", segments=" + walWriterCount
                    + ", columns=" + COLUMN_COUNT
                    + ", workers=" + WORKER_COUNT
                    + "] " + ff.report();
            LOG.info().$(report[0]).$();
            assertQuery("SELECT count() c FROM x").noRandomAccess().expectSize().returns(
                    "c\n" + ((long) WAL_ROUNDS * walWriterCount * WAL_ROWS_PER_COMMIT) + "\n"
            );
        });
        return report[0];
    }

    /**
     * Counts calls into the facade while armed, split by whether the calling thread is the one that armed it (the
     * test thread, which runs the WAL apply job and steals tasks) or another one (the pool's workers).
     */
    private static class SyscallCountingFilesFacade extends TestFilesFacadeImpl {
        static final int ALLOCATE = 0;
        static final int CLOSE = 1;
        static final int COPY_DATA = 2;
        static final int MADVISE = 3;
        static final int MMAP = 4;
        static final int MREMAP = 5;
        static final int MUNMAP = 6;
        static final int OPEN = 7;
        static final int TRUNCATE = 8;
        private static final String[] NAMES = {"fallocate", "close", "copyData", "madvise", "mmap", "mremap", "munmap", "open", "truncate"};
        private final AtomicLongArray counts = new AtomicLongArray(NAMES.length * 2);
        private volatile boolean armed;
        private volatile Thread armingThread;
        private long mmapReuseCountAtArm;
        private long mmapReuseCountAtDisarm;

        @Override
        public boolean allocate(long fd, long size) {
            count(ALLOCATE);
            return super.allocate(fd, size);
        }

        @Override
        public boolean close(long fd) {
            // Files.close() makes no call for an fd that was never opened, and callers close -1 freely.
            if (fd > 0) {
                count(CLOSE);
            }
            return super.close(fd);
        }

        @Override
        public long copyData(long srcFd, long destFd, long offsetSrc, long length) {
            count(COPY_DATA);
            return super.copyData(srcFd, destFd, offsetSrc, length);
        }

        @Override
        public long copyData(long srcFd, long destFd, long offsetSrc, long destOffset, long length) {
            count(COPY_DATA);
            return super.copyData(srcFd, destFd, offsetSrc, destOffset, length);
        }

        @Override
        public void madvise(long address, long len, int advise) {
            count(MADVISE);
            super.madvise(address, len, advise);
        }

        @Override
        public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
            count(MMAP);
            return super.mmap(fd, len, offset, flags, memoryTag);
        }

        @Override
        public long mmapNoCache(long fd, long len, long offset, int flags, int memoryTag) {
            count(MMAP);
            return super.mmapNoCache(fd, len, offset, flags, memoryTag);
        }

        @Override
        public long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
            count(MREMAP);
            return super.mremap(fd, addr, previousSize, newSize, offset, mode, memoryTag);
        }

        @Override
        public long mremapNoCache(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
            count(MREMAP);
            return super.mremapNoCache(fd, addr, previousSize, newSize, offset, mode, memoryTag);
        }

        @Override
        public void munmap(long address, long size, int memoryTag) {
            count(MUNMAP);
            super.munmap(address, size, memoryTag);
        }

        @Override
        public long openRO(LPSZ name) {
            count(OPEN);
            return super.openRO(name);
        }

        @Override
        public long openRONoCache(LPSZ path) {
            count(OPEN);
            return super.openRONoCache(path);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            count(OPEN);
            return super.openRW(name, opts);
        }

        @Override
        public long openRWNoCache(LPSZ name, int opts) {
            count(OPEN);
            return super.openRWNoCache(name, opts);
        }

        @Override
        public boolean truncate(long fd, long size) {
            count(TRUNCATE);
            return super.truncate(fd, size);
        }

        void arm() {
            for (int i = 0, n = counts.length(); i < n; i++) {
                counts.set(i, 0);
            }
            armingThread = Thread.currentThread();
            mmapReuseCountAtArm = Files.getMmapReuseCount();
            armed = true;
        }

        void disarm() {
            armed = false;
            mmapReuseCountAtDisarm = Files.getMmapReuseCount();
        }

        String report() {
            final StringBuilder sink = new StringBuilder("[");
            for (int i = 0; i < NAMES.length; i++) {
                if (i > 0) {
                    sink.append(", ");
                }
                sink.append(NAMES[i]).append('=').append(total(i))
                        .append(" (workers=").append(counts.get(i * 2 + 1)).append(')');
            }
            return sink.append(", mmapCacheReuse=").append(mmapReuseCountAtDisarm - mmapReuseCountAtArm).append(']').toString();
        }

        long total(int op) {
            return counts.get(op * 2) + counts.get(op * 2 + 1);
        }

        private void count(int op) {
            if (armed) {
                counts.incrementAndGet(op * 2 + (Thread.currentThread() == armingThread ? 0 : 1));
            }
        }
    }
}
