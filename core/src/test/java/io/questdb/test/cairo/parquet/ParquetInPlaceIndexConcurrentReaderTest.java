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

package io.questdb.test.cairo.parquet;

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnPurgeJob;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.O3ParquetCounters;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.PostingSealPurgeJob;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.cairo.wal.WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE;

/**
 * Index MVCC of in-place parquet O3 under concurrency. The test thread commits random
 * in-place O3 inserts, replace commits and native appends to parquet partitions, while
 * readers query a symbol index (bitmap, posting or covering) at any point of a commit, some
 * pausing mid-scan across later commits (one reader holds each paused scan until an in-place
 * commit lands), and a background thread runs {@link ColumnPurgeJob} and
 * {@link PostingSealPurgeJob}.
 * <p>
 * Every row an index cursor returns must match the queried key. When no commit overlapped
 * a reader's cursor opening (seqlock), the index and full-scan cursors must return the same
 * rows. At least one such scan, opened on a stable snapshot, must pause mid-scan across an
 * in-place commit. At the end, only the live index version may remain on disk, and the table
 * must match its native twin.
 */
public class ParquetInPlaceIndexConcurrentReaderTest extends AbstractCairoTest {
    private static final long DAY1 = 1_704_067_200_000_000L; // 2024-01-01T00:00:00Z
    private static final String IDX_BITMAP = "INDEX";
    private static final String IDX_COVERING = "INDEX TYPE POSTING INCLUDE (v)";
    private static final String IDX_POSTING = "INDEX TYPE POSTING";
    private static final int KEY_COUNT = 4;
    private static final int READER_COUNT = 3;
    private static final long ROW_STEP = 30 * Micros.MINUTE_MICROS;
    private static final int ROWS_PER_DAY = (int) (Micros.DAY_MICROS / ROW_STEP);

    @Override
    @Before
    public void setUp() {
        super.setUp();
        // 48 rows per day -> 6 row groups of 8; never rewrite on dead bytes, so commits update
        // the parquet partitions in place and rebuild their indexes under a new column name txn
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 8);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, Long.MAX_VALUE);
        // the purge thread runs on the real clock: retry deferred purges every few microseconds
        node1.setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY, 1);
        node1.setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY_LIMIT, 1_000);
    }

    @Test
    public void testBitmap() throws Exception {
        assertConcurrentReaders(IDX_BITMAP, AttachedStart.NONE);
    }

    @Test
    public void testBitmapAttachedForeign() throws Exception {
        // pq's partition 2024-01-01 is attached from a copy of table src's, whose sym carries the column version
        // of the first commit of the loop, which goes into that partition: the commit rewrites the partition under
        // the readers and the purge thread, and names the index after its txn. The version is not superseded below
        // its txn: the purge of such a version would not wait for the readers pinned since the attach, a separate
        // bug. BITMAP only, as for every attached partition: a POSTING index copied from another table hides the
        // rows its chain stamped with a txn above the reader's, a separate bug.
        assertConcurrentReaders(IDX_BITMAP, AttachedStart.FOREIGN_VERSION);
    }

    @Test
    public void testBitmapAttachedForeignDropAddIndex() throws Exception {
        // As testBitmapAttachedForeign, with DROP INDEX and ADD INDEX on sym before the readers start, and the
        // version of sym that of the first commit of the loop, after them. DROP INDEX moves the partition to a new
        // directory: the first commit updates it in place there, and no purge of the version the DROP superseded
        // deletes the index the commit builds under that name.
        assertConcurrentReaders(IDX_BITMAP, AttachedStart.DROP_ADD_INDEX);
    }

    @Test
    public void testCovering() throws Exception {
        assertConcurrentReaders(IDX_COVERING, AttachedStart.NONE);
    }

    @Test
    public void testPosting() throws Exception {
        assertConcurrentReaders(IDX_POSTING, AttachedStart.NONE);
    }

    private static void appendRow(WalWriter ww, long ts, long v) {
        final TableWriter.Row row = ww.newRow(ts);
        row.putInt(0, (int) v);
        row.putSym(2, sym(v));
        row.putSym(3, "t" + (v % 3));
        row.putLong(4, v);
        row.append();
    }

    // The column name txn a symbol index file carries. Every in-place commit gives the files a
    // name txn above -1, so -1 (no txn in the name) only marks a version older than that.
    private static long fileNameTxn(String[] parts) {
        final String ext = parts[1];
        if ("pv".equals(ext)) {
            // <column>.pv[.<name txn>].<seal txn>
            return parts.length > 3 ? Long.parseLong(parts[2]) : -1;
        }
        if (ext.startsWith("pc") && !"pci".equals(ext)) {
            // <column>.pcN.<name txn, 0 for none>[.<covered name txn>].<seal txn>
            final long txn = Long.parseLong(parts[2]);
            return txn == 0 ? -1 : txn;
        }
        // <column>.<k|v|pk|pci>[.<name txn>]
        return parts.length > 2 ? Long.parseLong(parts[2]) : -1;
    }

    private static String indexQuery(String key) {
        return "SELECT ts, sym, v FROM pq WHERE sym = '" + key + "'";
    }

    private static String plainQuery(String key) {
        // the same rows without the index: a filter on a function of sym
        return "SELECT ts, sym, v FROM pq WHERE concat(sym, '') = '" + key + "'";
    }

    private static void readRest(RecordCursor cursor, RecordMetadata metadata, StringSink sink) {
        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            CursorPrinter.println(record, metadata, sink);
        }
    }

    // sorts (ts, v) pairs by ts
    private static void sortRows(long[] rows) {
        for (int i = 2; i < rows.length; i += 2) {
            final long ts = rows[i];
            final long v = rows[i + 1];
            int j = i - 2;
            while (j >= 0 && rows[j] > ts) {
                rows[j + 2] = rows[j];
                rows[j + 3] = rows[j + 1];
                j -= 2;
            }
            rows[j + 2] = ts;
            rows[j + 3] = v;
        }
    }

    // The sym and sym2 index files of pq's parquet partitions that belong to a version other
    // than the live one in _cv, as <partition dir>/<file>.
    private static ObjList<String> supersededIndexFiles() {
        final ObjList<String> files = new ObjList<>();
        final TableToken token = engine.verifyTableName("pq");
        try (TableReader reader = engine.getReader(token); Path path = new Path()) {
            for (int p = 0, n = reader.getPartitionCount(); p < n; p++) {
                if (!reader.getTxFile().isPartitionParquet(p)) {
                    continue;
                }
                final long partitionTimestamp = reader.getTxFile().getPartitionTimestampByIndex(p);
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForNativePartition(
                        path,
                        ColumnType.TIMESTAMP,
                        PartitionBy.DAY,
                        partitionTimestamp,
                        reader.getTxFile().getPartitionNameTxn(p)
                );
                final File dir = new File(path.toString());
                final String[] names = dir.list();
                Assert.assertNotNull("partition dir must exist: " + dir, names);
                for (String name : names) {
                    if (!name.startsWith("sym.") && !name.startsWith("sym2.")) {
                        continue;
                    }
                    final String[] parts = name.split("\\.");
                    final long live = reader.getColumnVersionReader().getColumnNameTxn(
                            partitionTimestamp,
                            reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex(parts[0]))
                    );
                    // a partition no in-place commit reached has only its first version
                    if (live > -1 && fileNameTxn(parts) != live) {
                        files.add(dir.getName() + '/' + name + " (live name txn " + live + ')');
                    }
                }
            }
        }
        return files;
    }

    private static String sym(long v) {
        return "s" + (v % KEY_COUNT);
    }

    // Checks one row an index cursor returned: its symbol is the queried key, its v maps to
    // that key, and timestamps do not go backwards.
    private static long verifyIndexRow(Record record, String key, int keyIndex, long prevTs) {
        final long ts = record.getTimestamp(0);
        final CharSequence sym = record.getSymA(1);
        final long v = record.getLong(2);
        if (!Chars.equals(key, sym) || v % KEY_COUNT != keyIndex) {
            throw new AssertionError("index cursor for " + key + " returned a row of another key [ts=" + ts + ", sym=" + sym + ", v=" + v + ']');
        }
        if (ts < prevTs) {
            throw new AssertionError("index cursor for " + key + " went back in time [prev=" + prevTs + ", ts=" + ts + ']');
        }
        return ts;
    }

    private void applyRound(String table, int op, long[] rows, long lo, long hi) throws Exception {
        try (WalWriter ww = engine.getWalWriter(engine.verifyTableName(table))) {
            for (int i = 0; i < rows.length; i += 2) {
                appendRow(ww, rows[i], rows[i + 1]);
            }
            if (op == 0 || op == 3) {
                ww.commit();
            } else {
                ww.commitWithParams(lo, hi + 1, WAL_DEDUP_MODE_REPLACE_RANGE);
            }
        }
        drainWalQueue();
    }

    private void assertConcurrentReaders(String indexClause, AttachedStart attachedStart) throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            createTwins(indexClause, attachedStart);
            // the txn and the directory name txn of partition 2024-01-01 at the first commit of the loop
            final long firstCommitTxn = pqTxn();
            final long firstPartitionNameTxn = pqPartitionNameTxn();
            if (attachedStart != AttachedStart.NONE) {
                Assert.assertEquals(attachedStart == AttachedStart.FOREIGN_VERSION ? firstCommitTxn : firstPartitionNameTxn, pqSymNameTxn());
            }
            final long inPlaceBase = O3ParquetCounters.getInPlaceCount();
            final long dropInPlaceBase = O3ParquetCounters.getDropInPlaceCount();

            // odd while the test thread commits to pq, even otherwise
            final AtomicLong commitSeq = new AtomicLong();
            // the commitSeq that started the latest pq commit that updated a parquet partition
            // in place, set only after that commit completed
            final AtomicLong inPlaceCommitStartSeq = new AtomicLong();
            final AtomicBoolean isCommitLoopDone = new AtomicBoolean();
            final AtomicBoolean stop = new AtomicBoolean();
            final AtomicReference<Throwable> error = new AtomicReference<>();
            final AtomicLong stableComparisons = new AtomicLong();
            final AtomicLong midScanAcrossInPlaceCommit = new AtomicLong();
            final AtomicLong indexRowsChecked = new AtomicLong();
            final CountDownLatch started = new CountDownLatch(READER_COUNT + 1);
            final Thread[] threads = new Thread[READER_COUNT + 1];

            for (int r = 0; r < READER_COUNT; r++) {
                final long readerSeed = rnd.nextLong();
                final boolean isHoldingPausedScan = r == 0;
                threads[r] = new Thread(() -> {
                    final Rnd rr = new Rnd(readerSeed, readerSeed ^ 0x5DEECE66DL);
                    final RecordCursorFactory[] indexFactories = new RecordCursorFactory[KEY_COUNT];
                    final RecordCursorFactory[] plainFactories = new RecordCursorFactory[KEY_COUNT];
                    final StringSink indexSink = new StringSink();
                    final StringSink plainSink = new StringSink();
                    try (
                            SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1)
                                    .with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null, -1, null);
                            SqlCompiler compiler = engine.getSqlCompiler()
                    ) {
                        for (int k = 0; k < KEY_COUNT; k++) {
                            indexFactories[k] = compiler.compile(indexQuery("s" + k), ctx).getRecordCursorFactory();
                            plainFactories[k] = compiler.compile(plainQuery("s" + k), ctx).getRecordCursorFactory();
                        }
                        started.countDown();
                        while (!stop.get() && error.get() == null) {
                            final int keyIndex = rr.nextInt(KEY_COUNT);
                            final String key = "s" + keyIndex;
                            final RecordCursorFactory indexFactory = indexFactories[keyIndex];
                            final RecordCursorFactory plainFactory = plainFactories[keyIndex];
                            final long seqBefore = commitSeq.get();
                            try (
                                    RecordCursor indexCursor = indexFactory.getCursor(ctx);
                                    RecordCursor plainCursor = plainFactory.getCursor(ctx)
                            ) {
                                final long seqAfter = commitSeq.get();
                                final boolean isStable = seqBefore == seqAfter && (seqBefore & 1) == 0;
                                indexSink.clear();
                                plainSink.clear();
                                final RecordMetadata metadata = indexFactory.getMetadata();
                                final Record record = indexCursor.getRecord();
                                // read part of the index scan, then (sometimes) wait for a later
                                // commit and the purge to run before reading the rest
                                final int headRows = rr.nextInt(4);
                                long prevTs = Long.MIN_VALUE;
                                int read = 0;
                                boolean hasMore = true;
                                while (read < headRows && (hasMore = indexCursor.hasNext())) {
                                    prevTs = verifyIndexRow(record, key, keyIndex, prevTs);
                                    CursorPrinter.println(record, metadata, indexSink);
                                    read++;
                                }
                                boolean isPausedAcrossInPlaceCommit = false;
                                if (hasMore && rr.nextInt(3) == 0) {
                                    // reader 0 holds the scan until an in-place commit that started after its cursors
                                    // opened has completed, however long it takes; the others pause at most 50 ms
                                    final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(50);
                                    while (inPlaceCommitStartSeq.get() <= seqAfter
                                            && (isHoldingPausedScan || System.nanoTime() < deadline)
                                            && !isCommitLoopDone.get() && !stop.get()) {
                                        Os.pause();
                                    }
                                    isPausedAcrossInPlaceCommit = read > 0 && inPlaceCommitStartSeq.get() > seqAfter;
                                }
                                while (hasMore && indexCursor.hasNext()) {
                                    prevTs = verifyIndexRow(record, key, keyIndex, prevTs);
                                    CursorPrinter.println(record, metadata, indexSink);
                                    read++;
                                }
                                indexRowsChecked.addAndGet(read);
                                readRest(plainCursor, plainFactory.getMetadata(), plainSink);
                                if (isStable) {
                                    if (!Chars.equals(plainSink, indexSink)) {
                                        throw new AssertionError("index and full scan of one snapshot differ for " + key
                                                + "\nfull scan:\n" + plainSink + "index:\n" + indexSink);
                                    }
                                    stableComparisons.incrementAndGet();
                                    if (isPausedAcrossInPlaceCommit) {
                                        midScanAcrossInPlaceCommit.incrementAndGet();
                                    }
                                }
                            }
                        }
                    } catch (Throwable th) {
                        error.compareAndSet(null, th);
                    } finally {
                        for (int k = 0; k < KEY_COUNT; k++) {
                            Misc.free(indexFactories[k]);
                            Misc.free(plainFactories[k]);
                        }
                        Path.clearThreadLocals();
                        started.countDown();
                    }
                }, "pq-index-reader-" + r);
            }

            threads[READER_COUNT] = new Thread(() -> {
                try (
                        ColumnPurgeJob columnPurgeJob = new ColumnPurgeJob(engine);
                        PostingSealPurgeJob postingPurgeJob = new PostingSealPurgeJob(engine)
                ) {
                    started.countDown();
                    while (!stop.get() && error.get() == null) {
                        final boolean isBusy = columnPurgeJob.run() | postingPurgeJob.run();
                        if (!isBusy) {
                            Os.pause();
                        }
                    }
                } catch (Throwable th) {
                    error.compareAndSet(null, th);
                } finally {
                    Path.clearThreadLocals();
                    started.countDown();
                }
            }, "pq-index-purge");

            final long[] noRows = new long[0];
            try {
                for (Thread thread : threads) {
                    thread.start();
                }
                started.await();
                long nextV = 10_000;
                final int rounds = 100 + rnd.nextInt(50);
                for (int round = 0; round < rounds && error.get() == null; round++) {
                    // the first commit after an attached start goes into the attached partition
                    final boolean isAttachedFirstCommit = round == 0 && attachedStart != AttachedStart.NONE;
                    // 0: O3 insert into a parquet day, 1: replace within a parquet day,
                    // 2: replace across both parquet days, 3: O3 insert into the native day
                    final int op = isAttachedFirstCommit ? 0 : rnd.nextInt(10) < 4 ? 0 : (rnd.nextInt(5) < 3 ? 1 : (rnd.nextBoolean() ? 2 : 3));
                    final long lo;
                    final long hi;
                    final long dayStart = op == 3 ? DAY1 + 2 * Micros.DAY_MICROS : DAY1 + (isAttachedFirstCommit ? 0 : rnd.nextInt(2)) * Micros.DAY_MICROS;
                    if (op == 2) {
                        lo = DAY1 + Micros.DAY_MICROS - (1 + rnd.nextInt(6 * 60)) * Micros.MINUTE_MICROS;
                        hi = DAY1 + Micros.DAY_MICROS + rnd.nextInt(6 * 60) * Micros.MINUTE_MICROS;
                    } else {
                        lo = dayStart + rnd.nextInt(24 * 60) * Micros.MINUTE_MICROS;
                        hi = Math.min(lo + (1 + rnd.nextInt(8 * 60)) * Micros.MINUTE_MICROS, dayStart + Micros.DAY_MICROS - 1);
                    }
                    final long windowMinutes = (hi - lo) / Micros.MINUTE_MICROS;
                    final int rowCount = op == 1 || op == 2 ? (windowMinutes > 0 ? rnd.nextInt(4) : 0) : 1 + rnd.nextInt(6);
                    final long[] rows = rowCount == 0 ? noRows : new long[2 * rowCount];
                    for (int i = 0; i < rowCount; i++) {
                        final long v = nextV++;
                        // unique: initial rows sit on whole half hours, these 1 + v % 1_000_000 micros past a minute
                        final long minute = op == 1 || op == 2
                                ? lo / Micros.MINUTE_MICROS + rnd.nextLong(windowMinutes)
                                : dayStart / Micros.MINUTE_MICROS + rnd.nextInt(24 * 60 - 1);
                        rows[2 * i] = minute * Micros.MINUTE_MICROS + 1 + v % 1_000_000;
                        rows[2 * i + 1] = v;
                    }
                    sortRows(rows);
                    final long inPlaceBefore = O3ParquetCounters.getInPlaceCount();
                    final long startSeq = commitSeq.incrementAndGet();
                    try {
                        applyRound("pq", op, rows, lo, hi);
                    } finally {
                        commitSeq.incrementAndGet();
                    }
                    if (O3ParquetCounters.getInPlaceCount() > inPlaceBefore) {
                        // the pq commit updated a parquet partition in place, superseding its index version
                        inPlaceCommitStartSeq.set(startSeq);
                    }
                    Assert.assertFalse("pq suspended", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pq")));
                    if (isAttachedFirstCommit) {
                        // The version of sym was at or above the txn: the commit rewrote the partition and named the index
                        // after its txn. After DROP INDEX moved the partition, the commit updated it in place.
                        Assert.assertEquals(attachedStart == AttachedStart.FOREIGN_VERSION ? firstCommitTxn : firstPartitionNameTxn, pqPartitionNameTxn());
                        Assert.assertEquals(firstCommitTxn, pqSymNameTxn());
                    }
                    applyRound("nat", op, rows, lo, hi);
                }
                isCommitLoopDone.set(true);
                // let the readers and the purge overlap the last commits
                final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (stableComparisons.get() < 20 && error.get() == null && System.nanoTime() < deadline) {
                    Os.sleep(1);
                }
            } finally {
                stop.set(true);
                for (Thread thread : threads) {
                    thread.join(60_000);
                    Assert.assertFalse("thread did not stop: " + thread.getName(), thread.isAlive());
                }
            }
            if (error.get() != null) {
                throw new AssertionError("concurrent reader or purge failed", error.get());
            }
            LOG.info().$("concurrent pq index readers [index=").$(indexClause)
                    .$(", stableComparisons=").$(stableComparisons.get())
                    .$(", midScanAcrossInPlaceCommit=").$(midScanAcrossInPlaceCommit.get())
                    .$(", indexRowsChecked=").$(indexRowsChecked.get())
                    .$(", inPlace=").$(O3ParquetCounters.getInPlaceCount() - inPlaceBase)
                    .$(", dropInPlace=").$(O3ParquetCounters.getDropInPlaceCount() - dropInPlaceBase)
                    .I$();
            Assert.assertTrue("no comparison on a stable snapshot", stableComparisons.get() > 0);
            Assert.assertTrue("no compared index scan paused mid-scan across an in-place commit", midScanAcrossInPlaceCommit.get() > 0);
            Assert.assertTrue("no in-place parquet commit", O3ParquetCounters.getInPlaceCount() > inPlaceBase);

            assertSqlCursors("nat", "pq");
            for (int k = 0; k < KEY_COUNT; k++) {
                assertSqlCursors(plainQuery("s" + k), indexQuery("s" + k));
                assertSqlCursors(plainQuery("s" + k).replace("pq", "nat"), indexQuery("s" + k));
            }

            // no reader is left: the purge must remove every superseded index version
            engine.releaseAllReaders();
            // under ADAPTIVE the save points must move past the superseded versions first
            releaseSavePointHolds("pq");
            try (
                    ColumnPurgeJob columnPurgeJob = new ColumnPurgeJob(engine);
                    PostingSealPurgeJob postingPurgeJob = new PostingSealPurgeJob(engine)
            ) {
                ObjList<String> leftover = supersededIndexFiles();
                LOG.info().$("superseded index files before the final purge [count=").$(leftover.size()).I$();
                final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (leftover.size() > 0 && System.nanoTime() < deadline) {
                    columnPurgeJob.run();
                    postingPurgeJob.run();
                    Os.sleep(1);
                    leftover = supersededIndexFiles();
                }
                Assert.assertEquals("superseded index files left after the purge: " + leftover, 0, leftover.size());
            }
            assertSqlCursors("nat", "pq");
        });
    }

    private static long pqPartitionNameTxn() {
        try (TableReader reader = engine.getReader(engine.verifyTableName("pq"))) {
            return reader.getTxFile().getPartitionNameTxn(0);
        }
    }

    // the column name txn of sym in partition 2024-01-01 of pq
    private static long pqSymNameTxn() {
        try (TableReader reader = engine.getReader(engine.verifyTableName("pq"))) {
            return reader.getColumnVersionReader().getColumnNameTxn(
                    reader.getPartitionTimestampByIndex(0),
                    reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex("sym"))
            );
        }
    }

    private static long pqTxn() {
        try (TableReader reader = engine.getReader(engine.verifyTableName("pq"))) {
            return reader.getTxn();
        }
    }

    /**
     * nat (native) and pq with 3 daily partitions of 48 rows at 30 minute spacing and two
     * indexed symbols, sym before sym2. pq's first two days are parquet. The symbol of a
     * row is a function of its unique v. An attached start attaches pq's first day from a copy
     * of table src's, which holds the same rows, with sym at the column version of the txn of
     * the first commit of the test's loop. DROP_ADD_INDEX runs DROP INDEX and ADD INDEX on sym
     * of pq before that commit.
     */
    private void createTwins(String indexClause, AttachedStart attachedStart) throws Exception {
        final String ddl = "CREATE TABLE %s (id INT, ts TIMESTAMP, sym SYMBOL %s, sym2 SYMBOL %s, v LONG) TIMESTAMP(ts) PARTITION BY DAY %s";
        final String idx2 = indexClause.replace("INCLUDE (v)", "INCLUDE (id)");
        execute(String.format(ddl, "nat", indexClause, idx2, "WAL"));
        execute(String.format(ddl, "pq", indexClause, idx2, "WAL"));
        // rows lo..hi, x - 1 half hours after the start of 2024-01-01; every day uses the symbols in the same order
        final String insert = """
                INSERT INTO %s
                SELECT
                    x::INT,
                    '2024-01-01'::TIMESTAMP + (x - 1) * %d,
                    's' || (x %% %d),
                    't' || (x %% 3),
                    x
                FROM long_sequence(%d)
                WHERE x >= %d
                """;
        execute(String.format(insert, "nat", ROW_STEP, KEY_COUNT, 3 * ROWS_PER_DAY, 1));
        if (attachedStart == AttachedStart.NONE) {
            execute(String.format(insert, "pq", ROW_STEP, KEY_COUNT, 3 * ROWS_PER_DAY, 1));
            drainWalQueue();
        } else {
            execute(String.format(insert, "pq", ROW_STEP, KEY_COUNT, 3 * ROWS_PER_DAY, ROWS_PER_DAY + 1));
            drainWalQueue();
            // the first commit of the loop follows ATTACH, a commit per partition CONVERT takes, and the DDLs
            final long foreignNameTxn = pqTxn() + 3 + (attachedStart == AttachedStart.DROP_ADD_INDEX ? 2 : 0);
            execute(String.format(ddl, "src", indexClause, idx2, "BYPASS WAL"));
            execute(String.format(insert, "src", ROW_STEP, KEY_COUNT, ROWS_PER_DAY, 1));
            final TableToken srcToken = engine.verifyTableName("src");
            while (srcTxn(srcToken) < foreignNameTxn) {
                execute("INSERT INTO src (ts) VALUES ('2024-01-09T00:00:00.000000Z')");
            }
            Assert.assertEquals(foreignNameTxn, srcTxn(srcToken));
            try (TableReader ignore = engine.getReader(srcToken)) {
                // sym takes the column version of the txn, the previous version stays on disk beside it
                execute("UPDATE src SET sym = sym WHERE ts IN '2024-01-01'");
            }
            try (Path from = new Path(); Path to = new Path(); TableReader reader = engine.getReader(srcToken)) {
                from.of(configuration.getDbRoot()).concat(srcToken);
                TableUtils.setPathForNativePartition(from, ColumnType.TIMESTAMP, PartitionBy.DAY, DAY1, reader.getTxFile().getPartitionNameTxn(0));
                from.slash$();
                to.of(configuration.getDbRoot()).concat(engine.verifyTableName("pq")).concat("2024-01-01").put(configuration.getAttachPartitionSuffix()).slash$();
                TestUtils.copyDirectory(from, to, configuration.getMkDirMode());
            }
            engine.releaseAllWriters();
            execute("ALTER TABLE pq ATTACH PARTITION LIST '2024-01-01'");
            drainWalQueue();
            Assert.assertEquals(foreignNameTxn, pqSymNameTxn());
        }
        execute("ALTER TABLE pq CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-03'");
        drainWalQueue();
        if (attachedStart == AttachedStart.DROP_ADD_INDEX) {
            final long dropIndexTxn = pqTxn();
            execute("ALTER TABLE pq ALTER COLUMN sym DROP INDEX");
            drainWalQueue();
            // the version of sym is at or above the txn: DROP INDEX moved the partition to the directory of its txn
            Assert.assertEquals(dropIndexTxn, pqPartitionNameTxn());
            execute("ALTER TABLE pq ALTER COLUMN sym ADD " + indexClause);
            drainWalQueue();
        }
        assertQuery("SELECT count() FROM table_partitions('pq') WHERE isParquet")
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("count\n2\n");
        // the index, not a full scan, must serve the reader queries
        final String planFragment = indexClause.contains("INCLUDE") ? "CoveringIndex on: sym" : "Index forward scan on: sym";
        assertQuery(indexQuery("s1")).noLeakCheck().assertsPlanContaining(planFragment);
        assertQuery(plainQuery("s1")).noLeakCheck().assertsPlanNotContaining("Index");
    }

    private static long srcTxn(TableToken srcToken) {
        try (TableReader reader = engine.getReader(srcToken)) {
            return reader.getTxn();
        }
    }

    private enum AttachedStart {
        // pq's first day holds its own rows
        NONE,
        // attached with sym at the version of the first commit of the loop
        FOREIGN_VERSION,
        // attached with sym at the version of the first commit of the loop, which DROP INDEX and ADD INDEX precede
        DROP_ADD_INDEX
    }
}
