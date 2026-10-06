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
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.regex.Pattern;

/**
 * Counts the file-system calls - mmap, munmap, fallocate and friends - that WAL apply makes with a real worker pool
 * running the O3 and column jobs in parallel, the way a server does, and holds them to a budget that does not grow
 * with the number of pieces.
 * <p>
 * The MERGE shape was seen in a TSBS profile as mmap/munmap pairs on the shared-write workers: a MERGE into every
 * piece of a 200-piece composite partition, where each MERGE mapped and unmapped the target's tail per column -
 * pieces x columns map/unmap pairs per plan. The plan now opens and maps every column file once.
 */
public class CompositeMergeSyscallTest extends AbstractCairoTest {
    private static final int APPEND_ROWS = 9;
    private static final int APPEND_TEST_PIECES = 20;
    private static final int BATCH_ROWS = 20;
    private static final int DAY_ROWS = 8640;
    private static final int DOUBLE_COLUMNS = 10;
    // s SYMBOL, v STRING, d0..d9 DOUBLE, ts TIMESTAMP, topped LONG.
    private static final int COLUMN_COUNT = DOUBLE_COLUMNS + 4;
    private static final int COLUMN_FILE_COUNT = COLUMN_COUNT + 1;
    private static final int MERGE_ROWS_PER_PIECE = 3;
    private static final long MINUTE = 60_000_000L;
    private static final int REWRITE_TEST_PIECES = 30;
    private static final int TARGET_PIECES = 200;
    private static final int WORKER_COUNT = 4;

    @Test
    public void testAppendToManyPieceCompositePartitionWithMixedIoDoesNotMap() throws Exception {
        final SyscallCountingFilesFacade ff = new SyscallCountingFilesFacade();
        ff.setPartitionDir("2024-01-01");
        assertMemoryLeak(ff, () -> {
            node1.setProperty(PropertyKey.DEBUG_CAIRO_ALLOW_MIXED_IO, true);
            createDayTable();
            final TableToken tableToken = engine.verifyTableName("x");
            final long day = MicrosTimestampDriver.floor("2024-01-01T00:00:00.000000Z");
            final WorkerPool pool = new TestWorkerPool(WORKER_COUNT, node1.getMetrics());
            WorkerPoolUtils.setupWriterJobs(pool, engine);
            pool.start(LOG);
            final LongList pieces = new LongList();
            int batches = 0;
            try {
                batches = cutDayIntoPieces(tableToken, day, APPEND_TEST_PIECES, pieces);
                final long eBefore = readE(tableToken, day);

                // Rows above the day's last one and below the next day: appends only, no MERGE.
                ff.arm();
                execute("INSERT INTO x (" + columnNames() + ") SELECT " + valuesSelect("x + 8_000") + ", "
                        + "timestamp_sequence('2024-01-01T23:59:51', 1_000_000L) ts FROM long_sequence(" + APPEND_ROWS + ")");
                drainWalQueue();
                ff.disarm();

                Assert.assertFalse("table suspended by the append", engine.getTableSequencerAPI().isSuspended(tableToken));
                final long eAfter = readE(tableToken, day);
                final String report = "append to a composite partition with mixed I/O [pieces=" + pieces.size() / 4
                        + ", rows=" + APPEND_ROWS
                        + ", eBefore=" + eBefore
                        + ", eAfter=" + eAfter
                        + ", columns=" + COLUMN_COUNT
                        + ", workers=" + WORKER_COUNT
                        + "] " + ff.report();
                LOG.info().$(report).$();
                ff.assertReservedWrites(report);

                Assert.assertEquals("the commit did not append to the composite partition: " + report, eBefore + APPEND_ROWS, eAfter);
                // The plan reserves the full extent before its positioned writes but does not map the targets. A
                // previous page-rounded reservation can already cover this small append, hence the upper bound.
                // The one mapping left is the planner's designated-timestamp read.
                Assert.assertTrue("partition column files allocated more than once: " + report,
                        ff.partition(SyscallCountingFilesFacade.ALLOCATE) <= COLUMN_FILE_COUNT);
                Assert.assertTrue("partition column files mapped: " + report, ff.partition(SyscallCountingFilesFacade.MMAP) <= 1);
                Assert.assertEquals("partition column files remapped: " + report, 0, ff.partition(SyscallCountingFilesFacade.MREMAP));
                Assert.assertTrue("partition column files unmapped: " + report, ff.partition(SyscallCountingFilesFacade.MUNMAP) <= 1);
                Assert.assertTrue("partition column files not written: " + report, ff.partition(SyscallCountingFilesFacade.WRITE) >= COLUMN_COUNT);
            } finally {
                pool.halt();
            }

            assertQuery("SELECT count() c FROM x").noRandomAccess().expectSize().returns(
                    "c\n" + (DAY_ROWS + 1 + BATCH_ROWS * batches + APPEND_ROWS) + "\n"
            );
            assertQuery("SELECT count() c, sum(d0) s FROM x WHERE ts >= '2024-01-01T23:59:51' AND ts < '2024-01-02'")
                    .noRandomAccess().expectSize()
                    .returns("c\ts\n" + APPEND_ROWS + "\t" + (APPEND_ROWS * 8_000L + (long) APPEND_ROWS * (APPEND_ROWS + 1) / 2) + ".0\n");
        });
    }

    @Test
    public void testMergeIntoEveryPieceOfManyPieceCompositePartition() throws Exception {
        assertMergeAllocations(false);
    }

    @Test
    public void testMergeIntoEveryPieceOfManyPieceCompositePartitionWithMixedIo() throws Exception {
        assertMergeAllocations(true);
    }

    @Test
    public void testRewriteAllocatesOncePerColumnFile() throws Exception {
        assertRewriteAllocations(false);
    }

    @Test
    public void testRewriteWithMixedIoAllocatesOncePerColumnFile() throws Exception {
        assertRewriteAllocations(true);
    }

    private void assertMergeAllocations(boolean mixedIo) throws Exception {
        final SyscallCountingFilesFacade ff = new SyscallCountingFilesFacade();
        ff.setPartitionDir("2024-01-01");
        assertMemoryLeak(ff, () -> {
            node1.setProperty(PropertyKey.DEBUG_CAIRO_ALLOW_MIXED_IO, mixedIo);
            createDayTable();
            final TableToken tableToken = engine.verifyTableName("x");
            final long day = MicrosTimestampDriver.floor("2024-01-01T00:00:00.000000Z");
            final WorkerPool pool = new TestWorkerPool(WORKER_COUNT, node1.getMetrics());
            WorkerPoolUtils.setupWriterJobs(pool, engine);
            pool.start(LOG);
            final LongList before = new LongList();
            final LongList after = new LongList();
            final StringBuilder mergeSql = new StringBuilder();
            int mergeRows = 0;
            int batches = 0;
            int targetedPieces = 0;
            try {
                batches = cutDayIntoPieces(tableToken, day, TARGET_PIECES, before);
                final long eBefore = readE(tableToken, day);

                // One commit that lands rows strictly inside every piece: a plan of one MERGE per piece.
                mergeSql.append("INSERT INTO x (").append(columnNames()).append(") VALUES ");
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
                // The same plan also has a tail APPEND, which executeCompositePlan performs before the MERGEs.
                mergeSql.append(", ");
                appendValuesRow(mergeSql, 9_000_000 + ++mergeRows, day + 24 * 60 * MINUTE - 1);

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
                ff.assertReservedWrites(report);

                Assert.assertTrue("the commit did not MERGE into most pieces: " + report, mergedPieces >= targetedPieces * 3 / 4);
                // The work this commit does is all of one plan, over files it can open, size and map once. A
                // budget of a few per column holds whatever the piece count; one map per MERGE per column does not.
                final long budget = 4L * COLUMN_COUNT;
                Assert.assertTrue("plan did not reserve target columns: " + report,
                        ff.partition(SyscallCountingFilesFacade.ALLOCATE) > 0);
                Assert.assertTrue("fallocate per piece: " + report, ff.total(SyscallCountingFilesFacade.ALLOCATE) <= budget);
                Assert.assertTrue("mmap per piece: " + report, ff.total(SyscallCountingFilesFacade.MMAP) <= budget);
                Assert.assertTrue("munmap per piece: " + report, ff.total(SyscallCountingFilesFacade.MUNMAP) <= budget);
            } finally {
                pool.halt();
            }

            assertQuery("SELECT count() c FROM x").noRandomAccess().expectSize().returns(
                    "c\n" + (DAY_ROWS + 1 + BATCH_ROWS * batches + mergeRows) + "\n"
            );
        });
    }

    private static void appendValuesRow(StringBuilder sink, long id, long ts) {
        sink.append("('h").append(id % 100).append("', 'value").append(id).append('\'');
        for (int d = 0; d < DOUBLE_COLUMNS; d++) {
            sink.append(", ").append(id + d).append(".0");
        }
        sink.append(", ").append(ts).append("::TIMESTAMP)");
    }

    private static String columnNames() {
        final StringBuilder sink = new StringBuilder("s, v");
        for (int d = 0; d < DOUBLE_COLUMNS; d++) {
            sink.append(", d").append(d);
        }
        return sink.append(", ts").toString();
    }

    private static String columnsDdl() {
        final StringBuilder sink = new StringBuilder("s SYMBOL INDEX, v STRING");
        for (int d = 0; d < DOUBLE_COLUMNS; d++) {
            sink.append(", d").append(d).append(" DOUBLE");
        }
        return sink.append(", ts TIMESTAMP").toString();
    }

    /**
     * Cuts 2024-01-01 into at least {@code targetPieces} pieces with backdated batches, each in a slot of the day no
     * earlier batch touched, one commit apiece: every batch cuts the piece it lands in and founds a piece of its own.
     * Leaves the pieces in {@code pieces} and returns how many batches it took.
     */
    private static int cutDayIntoPieces(TableToken tableToken, long day, int targetPieces, LongList pieces) throws Exception {
        int batch = 0;
        while (snapshotPieces(tableToken, day, pieces) < targetPieces) {
            Assert.assertTrue("could not cut the day into " + targetPieces + " pieces, got "
                    + pieces.size() / 4 + " after " + batch + " batches", batch < 1440 / 7);
            final long start = day + batch * 7 * MINUTE + 3 * MINUTE + 3_000_000L;
            execute("INSERT INTO x (" + columnNames() + ") SELECT " + valuesSelect("x + " + (100_000 * (batch + 1))) + ", "
                    + "timestamp_sequence(" + start + "::TIMESTAMP, 2_000_000L) ts FROM long_sequence(" + BATCH_ROWS + ")");
            drainWalQueue();
            batch++;
        }
        Assert.assertFalse("table suspended while cutting pieces", engine.getTableSequencerAPI().isSuspended(tableToken));
        return batch;
    }

    /**
     * Table x, merge-append on, with 2024-01-01 a row every 10 seconds, plus a later day so 2024-01-01 is never the
     * active partition and every backdated batch goes through the O3 path.
     */
    private static void createDayTable() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "8K");
        // Small piece floor, so each backdated batch founds pieces of its own instead of merging into one big piece,
        // and a piece-count cap well above the target, so nothing compacts the pieces back together.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 64);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_PIECE_THRESHOLD, 100_000);
        // Pooled frame columns capture the FilesFacade and the mixed I/O flag they were built with; start from a
        // fresh pool.
        engine.resetFrameFactory();

        execute("CREATE TABLE x (" + columnsDdl() + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO x (" + columnNames() + ") SELECT " + valuesSelect("x")
                + ", timestamp_sequence('2024-01-01', 10_000_000L) ts FROM long_sequence(" + DAY_ROWS + ")");
        drainWalQueue();
        execute("ALTER TABLE x ADD COLUMN topped LONG");
        execute("INSERT INTO x (" + columnNames() + ") SELECT " + valuesSelect("x")
                + ", '2024-01-02T00:00:00'::TIMESTAMP ts FROM long_sequence(1)");
        drainWalQueue();
    }

    private static boolean isComposite(TableToken tableToken, long partitionTimestamp) {
        try (TableReader reader = engine.getReader(tableToken)) {
            final int partitionIndex = reader.getTxFile().getPartitionIndex(partitionTimestamp);
            return reader.getGeometry().isComposite(partitionIndex);
        }
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
        final StringBuilder sink = new StringBuilder("'h' || ((").append(idExpr).append(") % 100) s, 'value' || (")
                .append(idExpr).append(") v");
        for (int d = 0; d < DOUBLE_COLUMNS; d++) {
            sink.append(", (").append(idExpr).append(") + ").append(d).append(".0 d").append(d);
        }
        return sink.toString();
    }

    /**
     * Cuts 2024-01-01 into pieces, moves every piece to the tail with a MERGE so the day is mostly dead space, then
     * makes the day a waste-ratio candidate whose only way out is a REWRITE, and counts what the REWRITE does to the
     * partition's column files: it copies piece after piece into a fresh directory, and the final size is known
     * before the first copy.
     */
    private void assertRewriteAllocations(boolean mixedIo) throws Exception {
        final SyscallCountingFilesFacade ff = new SyscallCountingFilesFacade();
        ff.setPartitionDir("2024-01-01");
        assertMemoryLeak(ff, () -> {
            node1.setProperty(PropertyKey.DEBUG_CAIRO_ALLOW_MIXED_IO, mixedIo);
            createDayTable();
            final TableToken tableToken = engine.verifyTableName("x");
            final long day = MicrosTimestampDriver.floor("2024-01-01T00:00:00.000000Z");
            final WorkerPool pool = new TestWorkerPool(WORKER_COUNT, node1.getMetrics());
            WorkerPoolUtils.setupWriterJobs(pool, engine);
            pool.start(LOG);
            final LongList pieces = new LongList();
            int batches = 0;
            int mergeRows = 0;
            int passes = 0;
            try {
                batches = cutDayIntoPieces(tableToken, day, REWRITE_TEST_PIECES, pieces);
                final StringBuilder mergeSql = new StringBuilder("INSERT INTO x (").append(columnNames()).append(") VALUES ");
                for (int p = 0, n = pieces.size() / 4; p < n; p++) {
                    final long tsLo = pieces.getQuick(p * 4);
                    final long tsHi = pieces.getQuick(p * 4 + 1);
                    if (tsHi - tsLo > 2) {
                        if (mergeRows++ > 0) {
                            mergeSql.append(", ");
                        }
                        appendValuesRow(mergeSql, 9_000_000 + mergeRows, (tsLo + (tsHi - tsLo) / 2) | 1);
                    }
                }
                execute(mergeSql);
                drainWalQueue();
                Assert.assertTrue("the day is not composite before the REWRITE", isComposite(tableToken, day));

                // Any dead space makes the day a candidate and nothing counts as hot. The day is below the
                // split size, so the due-compaction MOVE-TAIL cannot replace this REWRITE.
                node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "1");
                node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_TABLE_PRESSURE_DEAD_RATIO, "0.005");
                node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_ROWS_RATIO, "0.01");
                node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_HOT_COMMITS, 0);
                node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_HOT_TIME, 0);
                node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_MOVE_TAIL_MIN_GAIN, Integer.MAX_VALUE);
                node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1G");

                // Compaction runs after a commit; each of these lands on a day of its own, never on 2024-01-01.
                ff.arm();
                while (isComposite(tableToken, day) && passes < 10) {
                    passes++;
                    execute("INSERT INTO x (" + columnNames() + ") SELECT " + valuesSelect("x + 7_000_000") + ", "
                            + (day + (2 + passes) * 24 * 60 * MINUTE) + "::TIMESTAMP ts FROM long_sequence(1)");
                    drainWalQueue();
                }
                ff.disarm();

                Assert.assertFalse("table suspended by compaction", engine.getTableSequencerAPI().isSuspended(tableToken));
                final String report = "REWRITE of a composite partition [mixedIo=" + mixedIo
                        + ", pieces=" + pieces.size() / 4
                        + ", passes=" + passes
                        + ", columns=" + COLUMN_COUNT
                        + ", workers=" + WORKER_COUNT
                        + "] " + ff.report();
                LOG.info().$(report).$();
                ff.assertReservedWrites(report);
                Assert.assertFalse("compaction did not rewrite the day: " + report, isComposite(tableToken, day));
                // The REWRITE knows its final size before the first copy: one allocation per column file, up front,
                // for both mmap and mixed I/O.
                final long allocations = ff.partition(SyscallCountingFilesFacade.ALLOCATE);
                Assert.assertTrue("REWRITE did not allocate target columns: " + report, allocations > 0);
                Assert.assertTrue("REWRITE allocated per piece: " + report, allocations <= COLUMN_FILE_COUNT);
                // Each target column file opens once for the whole REWRITE, not once per piece.
                Assert.assertTrue("REWRITE reopened its files per piece: " + report,
                        ff.partition(SyscallCountingFilesFacade.OPEN) <= 3L * COLUMN_COUNT);
            } finally {
                pool.halt();
            }

            assertQuery("SELECT count() c FROM x").noRandomAccess().expectSize().returns(
                    "c\n" + (DAY_ROWS + 1 + BATCH_ROWS * batches + mergeRows + passes) + "\n"
            );
        });
    }

    /**
     * Counts calls into the facade while armed, split by whether the calling thread is the one that armed it (the
     * test thread, which runs the WAL apply job and steals tasks) or another one (the pool's workers). Calls on the
     * column files of one partition directory - fds opened on them, and the mappings made of those - are also counted
     * on their own, see {@link #partition}.
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
        static final int WRITE = 9;
        // A column's data or aux file, with or without a column name txn: s.d, d0.d.3, v.i.
        private static final Pattern COLUMN_FILE = Pattern.compile("[^/\\\\]+\\.[di](\\.\\d+)?$");
        private static final String[] NAMES = {"fallocate", "close", "copyData", "madvise", "mmap", "mremap", "munmap", "open", "truncate", "write"};
        private final ConcurrentHashMap<Long, Integer> allocationCounts = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<Long, Long> allocatedSizes = new ConcurrentHashMap<>();
        private final AtomicLongArray counts = new AtomicLongArray(NAMES.length * 2);
        private final AtomicLongArray partitionCounts = new AtomicLongArray(NAMES.length);
        // Tracked whether armed or not: a cached frame opens and maps its files on one commit and writes them on the next.
        private final ConcurrentHashMap<Long, Boolean> partitionFds = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<Long, Boolean> partitionMappings = new ConcurrentHashMap<>();
        private volatile String allocationFailure;
        private volatile boolean armed;
        private volatile Thread armingThread;
        private long mmapReuseCountAtArm;
        private long mmapReuseCountAtDisarm;
        private volatile String partitionDir;

        @Override
        public boolean allocate(long fd, long size) {
            count(ALLOCATE, fd);
            noteAllocation(fd, size);
            return super.allocate(fd, size);
        }

        @Override
        public boolean allocate(long fd, long allocatedSize, long size) {
            count(ALLOCATE, fd);
            noteAllocation(fd, size);
            return super.allocate(fd, allocatedSize, size);
        }

        @Override
        public long append(long fd, long buf, long len) {
            checkPositionedWrite(fd, super.length(fd) + len);
            return super.append(fd, buf, len);
        }

        @Override
        public boolean close(long fd) {
            // Files.close() makes no call for an fd that was never opened, and callers close -1 freely.
            if (fd > 0) {
                count(CLOSE, fd);
                allocatedSizes.remove(fd);
                allocationCounts.remove(fd);
                partitionFds.remove(fd);
            }
            return super.close(fd);
        }

        @Override
        public long copyData(long srcFd, long destFd, long offsetSrc, long length) {
            count(COPY_DATA, destFd);
            return super.copyData(srcFd, destFd, offsetSrc, length);
        }

        @Override
        public long copyData(long srcFd, long destFd, long offsetSrc, long destOffset, long length) {
            count(COPY_DATA, destFd);
            checkPositionedWrite(destFd, destOffset + length);
            return super.copyData(srcFd, destFd, offsetSrc, destOffset, length);
        }

        @Override
        public void madvise(long address, long len, int advise) {
            count(MADVISE, -1);
            super.madvise(address, len, advise);
        }

        @Override
        public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
            count(MMAP, fd);
            return trackMapping(fd, super.mmap(fd, len, offset, flags, memoryTag));
        }

        @Override
        public long mmapNoCache(long fd, long len, long offset, int flags, int memoryTag) {
            count(MMAP, fd);
            return trackMapping(fd, super.mmapNoCache(fd, len, offset, flags, memoryTag));
        }

        @Override
        public long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
            count(MREMAP, fd);
            partitionMappings.remove(addr);
            return trackMapping(fd, super.mremap(fd, addr, previousSize, newSize, offset, mode, memoryTag));
        }

        @Override
        public long mremapNoCache(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
            count(MREMAP, fd);
            partitionMappings.remove(addr);
            return trackMapping(fd, super.mremapNoCache(fd, addr, previousSize, newSize, offset, mode, memoryTag));
        }

        @Override
        public void munmap(long address, long size, int memoryTag) {
            final boolean isPartition = partitionMappings.remove(address) != null;
            count(MUNMAP, isPartition);
            super.munmap(address, size, memoryTag);
        }

        @Override
        public long openRO(LPSZ name) {
            return trackOpen(name, super.openRO(name));
        }

        @Override
        public long openRONoCache(LPSZ path) {
            return trackOpen(path, super.openRONoCache(path));
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            return trackOpen(name, super.openRW(name, opts));
        }

        @Override
        public long openRWNoCache(LPSZ name, int opts) {
            return trackOpen(name, super.openRWNoCache(name, opts));
        }

        @Override
        public boolean truncate(long fd, long size) {
            count(TRUNCATE, fd);
            return super.truncate(fd, size);
        }

        @Override
        public long write(long fd, long address, long len, long offset) {
            count(WRITE, fd);
            checkPositionedWrite(fd, offset + len);
            return super.write(fd, address, len, offset);
        }

        void arm() {
            for (int i = 0, n = counts.length(); i < n; i++) {
                counts.set(i, 0);
            }
            for (int i = 0, n = partitionCounts.length(); i < n; i++) {
                partitionCounts.set(i, 0);
            }
            allocatedSizes.clear();
            allocationCounts.clear();
            allocationFailure = null;
            armingThread = Thread.currentThread();
            mmapReuseCountAtArm = Files.getMmapReuseCount();
            armed = true;
        }

        void disarm() {
            armed = false;
            mmapReuseCountAtDisarm = Files.getMmapReuseCount();
        }

        void assertReservedWrites(String report) {
            Assert.assertNull(allocationFailure + ": " + report, allocationFailure);
        }

        long partition(int op) {
            return partitionCounts.get(op);
        }

        String report() {
            final StringBuilder sink = new StringBuilder("[");
            for (int i = 0; i < NAMES.length; i++) {
                if (i > 0) {
                    sink.append(", ");
                }
                sink.append(NAMES[i]).append('=').append(total(i))
                        .append(" (workers=").append(counts.get(i * 2 + 1));
                if (partitionDir != null) {
                    sink.append(", partition=").append(partitionCounts.get(i));
                }
                sink.append(')');
            }
            return sink.append(", mmapCacheReuse=").append(mmapReuseCountAtDisarm - mmapReuseCountAtArm).append(']').toString();
        }

        /**
         * Counts the calls on the column files under the partition directory named {@code partitionDir} on their own
         * as well. Set before the files open: an fd is attributed when it is opened.
         */
        void setPartitionDir(String partitionDir) {
            this.partitionDir = Files.SEPARATOR + partitionDir;
        }

        long total(int op) {
            return counts.get(op * 2) + counts.get(op * 2 + 1);
        }

        private void checkPositionedWrite(long fd, long endOffset) {
            if (armed && fd > 0 && partitionFds.containsKey(fd)) {
                final long allocatedSize = allocatedSizes.getOrDefault(fd, super.length(fd));
                if (endOffset > allocatedSize && allocationFailure == null) {
                    allocationFailure = "positioned write exceeded reservation [fd=" + fd
                            + ", endOffset=" + endOffset + ", allocatedSize=" + allocatedSize + ']';
                }
            }
        }

        private void count(int op, long fd) {
            count(op, fd > 0 && partitionFds.containsKey(fd));
        }

        private void noteAllocation(long fd, long size) {
            if (armed && fd > 0 && partitionFds.containsKey(fd)) {
                allocatedSizes.merge(fd, size, Math::max);
                final int allocationCount = allocationCounts.merge(fd, 1, Integer::sum);
                if (allocationCount > 1 && allocationFailure == null) {
                    allocationFailure = "column file allocated more than once [fd=" + fd + ", count=" + allocationCount + ']';
                }
            }
        }

        private void count(int op, boolean isPartition) {
            if (armed) {
                counts.incrementAndGet(op * 2 + (Thread.currentThread() == armingThread ? 0 : 1));
                if (isPartition) {
                    partitionCounts.incrementAndGet(op);
                }
            }
        }

        private long trackMapping(long fd, long address) {
            if (address != FilesFacade.MAP_FAILED && fd > 0 && partitionFds.containsKey(fd)) {
                partitionMappings.put(address, Boolean.TRUE);
            }
            return address;
        }

        private long trackOpen(LPSZ name, long fd) {
            final String dir = partitionDir;
            final boolean isPartition = fd > 0 && dir != null && Utf8s.containsAscii(name, dir)
                    && COLUMN_FILE.matcher(Utf8s.stringFromUtf8Bytes(name)).find();
            if (isPartition) {
                partitionFds.put(fd, Boolean.TRUE);
            }
            count(OPEN, isPartition);
            return fd;
        }
    }
}
