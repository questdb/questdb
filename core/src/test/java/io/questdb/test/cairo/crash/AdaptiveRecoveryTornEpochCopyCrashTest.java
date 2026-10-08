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

package io.questdb.test.cairo.crash;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.DurableEpochManifest;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Recovery must never restore a damaged save point over the live files, and a crash anywhere inside the
 * writing of a save point must leave a state recovery accepts.
 *
 * <p>A save point holds byte copies of the live {@code _txn}, {@code _cv} and {@code _meta}. The damage rows
 * take a save point at a known cut, apply more rows lazily so the live files sit past it, damage one part of
 * the newest generation (a cut or garbage-filled copy, a flipped byte in each of the three copies, a torn
 * header, a header naming another cut), and require recovery to reject that generation, restore the
 * creation baseline and replay every row from the WAL.
 *
 * <p>The power-loss row crashes at every durability call of two consecutive save points (the first creates
 * a generation, the second overwrites the creation baseline in place) and requires recovery to accept what
 * is left, to keep every row, and to recover nothing on a second start. The restore row grows the live
 * {@code _meta} and {@code _cv} past their saved copies and requires the restore to leave each live file
 * byte-equal to its copy, so a second start counts no second recovery.
 */
public class AdaptiveRecoveryTornEpochCopyCrashTest extends AbstractAdaptiveCrashTest {

    private static final int K = 4; // rows before the epoch
    private static final int M = 5; // rows applied lazily AFTER the epoch (live _txn/_cv advance past it)
    private static final String RECOVERY_EVENTS = "questdb_wal_adaptive_recovery_events_total";

    /**
     * NEGATIVE CONTROL: restoring a torn {@code _txn} copy without validating it destroys the live
     * {@code _txn}. This is the failure the fall-back rows prevent.
     */
    @Test
    public void testNegativeControlOldBehaviourBricksOnTornCopy() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        try {
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());
            final FilesFacade ff = engine.getConfiguration().getFilesFacade();
            final TableToken tt = buildTableWithEpochAndLazyTail();
            Assert.assertTrue("live _txn must be healthy + ahead of the epoch before the bad restore",
                    readTxnSeqTxn(tt) >= K + M);

            engine.releaseAllWriters();
            engine.releaseAllReaders();

            tornByMode(tt, TornMode.TRUNCATE_TXN_EPOCH_TO_ZERO);
            // Emulate a restore without validation: the live _txn takes the bytes the torn copy still holds.
            try (Path src = new Path(); Path dst = new Path()) {
                final long[] copy = savedCopy(src, tt, 1, TableUtils.TXN_FILE_NAME);
                final long available = Math.max(0, Math.min(copy[1], ff.length(src.$()) - copy[0]));
                dst.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(TableUtils.TXN_FILE_NAME);
                final long srcFd = ff.openRO(src.$());
                final long dstFd = ff.openRW(dst.$(), engine.getConfiguration().getWriterFileOpenOpts());
                try {
                    Assert.assertTrue(ff.truncate(dstFd, 0));
                    if (available > 0) {
                        Assert.assertEquals(available, ff.copyData(srcFd, dstFd, copy[0], available));
                    }
                } finally {
                    ff.close(dstFd);
                    ff.close(srcFd);
                }
            }

            boolean bricked = false;
            try {
                readTxnSeqTxn(tt);
            } catch (Throwable expected) {
                bricked = true;
            }
            Assert.assertTrue("restoring a torn _txn copy without validation must brick the live _txn", bricked);
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
        }
    }

    /**
     * A crash at every durability call of two save points in a row: the first writes generation one, the
     * second overwrites generation zero, which still holds the creation baseline its marker slot names.
     * Whatever survives, recovery must start, keep every row, and find nothing to recover on a second start.
     */
    @Test
    public void testPowerLossAtEverySyncOfSavePoint() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        // A writer closed after the injected power loss must not publish the save point the crash interrupted.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
        try {
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());
            Assert.assertFalse(engine.getConfiguration().isAdaptiveEpochFlushOnClose());
            runWithCrashFacade(() -> {
                final Set<Long> fdBaseline = new HashSet<>(crashFf.noCacheOpenFdsSnapshot());
                final TableToken countToken = buildSweepTable(0);
                final int opsBefore = crashFf.durabilityOpCount();
                takeTwoSavePoints(countToken);
                final int n = crashFf.durabilityOpCount() - opsBefore;
                dropSweepTable(countToken);
                releaseEngineHandles();
                reclaimLingeringNonCacheFds(fdBaseline);
                Assert.assertTrue("the save points must make durability calls", n > 0);

                for (int k = 1; k <= n; k++) {
                    final TableToken tt = buildSweepTable(k);
                    markDurableBaseline();
                    crashFf.armCrashAt(crashFf.durabilityOpCount() + k);
                    boolean fired = false;
                    try {
                        takeTwoSavePoints(tt);
                    } catch (CrashSimulationError expected) {
                        fired = true;
                    }
                    Assert.assertTrue("crash point " + k + " of " + n + " never fired", fired);

                    final TableToken[] tokens = {tt};
                    simulateRestart(tokens);
                    try {
                        new RecoveryCoordinator(engine).recover();
                    } catch (CairoException e) {
                        Assert.fail("recovery refused after a power loss at durability call " + k + " of " + n
                                + ": " + e.getFlyweightMessage());
                    }
                    final long events = TestUtils.getMetricValue(engine, RECOVERY_EVENTS);
                    new RecoveryCoordinator(engine).recover();
                    Assert.assertEquals("a second start after a power loss at durability call " + k
                            + " must recover nothing", events, TestUtils.getMetricValue(engine, RECOVERY_EVENTS));
                    engine.notifyWalTxnRepublisher(tt);
                    drainWalQueue();
                    Assert.assertFalse("crash point " + k + ": table suspended",
                            engine.getTableSequencerAPI().isSuspended(tt));
                    assertRows(tt.getTableName(), K + M, "crash point " + k);
                    dropSweepTable(tt);
                    releaseEngineHandles();
                    reclaimLingeringNonCacheFds(fdBaseline);
                }
                Files.getMmapCache().asyncMunmap();
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "true");
        }
    }

    @Test
    public void testRecoveryFallsBackFromCorruptBodyTxnEpochAndReplaysWal() throws Exception {
        assertTornCopyHandledSafely(TornMode.CORRUPT_TXN_EPOCH_BODY);
    }

    @Test
    public void testRecoveryFallsBackFromFlippedCvByte() throws Exception {
        assertTornCopyHandledSafely(TornMode.FLIP_CV_BYTE);
    }

    @Test
    public void testRecoveryFallsBackFromFlippedMetaByte() throws Exception {
        assertTornCopyHandledSafely(TornMode.FLIP_META_BYTE);
    }

    @Test
    public void testRecoveryFallsBackFromFlippedTxnByte() throws Exception {
        assertTornCopyHandledSafely(TornMode.FLIP_TXN_BYTE);
    }

    @Test
    public void testRecoveryFallsBackFromMismatchedManifestAndReplaysWal() throws Exception {
        assertTornCopyHandledSafely(TornMode.MANIFEST_SEQTXN_MISMATCH);
    }

    /**
     * A tampered table id in the saved {@code _meta} is rejected. The checksum over the copy rejects it before
     * the table-id guard runs, so this proves the rejection, not that guard.
     */
    @Test
    public void testRecoveryFallsBackFromTamperedMetaEpoch() throws Exception {
        assertTornCopyHandledSafely(TornMode.TAMPERED_META_EPOCH);
    }

    @Test
    public void testRecoveryFallsBackFromTornHeader() throws Exception {
        assertTornCopyHandledSafely(TornMode.TORN_HEADER);
    }

    @Test
    public void testRecoveryFallsBackFromZeroByteTxnEpochAndReplaysWal() throws Exception {
        assertTornCopyHandledSafely(TornMode.TRUNCATE_TXN_EPOCH_TO_ZERO);
    }

    /**
     * Columns added and rows updated after the save point grow the live {@code _meta} and {@code _cv} past
     * their saved copies. The restore must leave each live file byte-equal to its copy, tail included, so a
     * second start finds the table at the save point and counts no second recovery.
     */
    @Test
    public void testRestoreTruncatesLiveFilesLongerThanTheirCopies() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        try {
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());
            final TableToken tt = buildTableWithEpochAndLazyTail();
            execute("alter table t add column c1 int");
            execute("alter table t add column c2 symbol");
            // Enough columns to grow _meta past the page it was allocated with.
            for (int i = 0; i < 64; i++) {
                execute("alter table t add column a_rather_long_column_name_" + i + " long");
            }
            execute("insert into t (ts, v, c1, c2) values ('2024-10-02T00:00:00.000000Z', " + (K + M) + ", 1, 'a')");
            execute("update t set c1 = 7 where v < 2");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tt));

            engine.releaseAllWriters();
            engine.releaseAllReaders();
            final long liveMeta = liveLength(tt, TableUtils.META_FILE_NAME);
            final long savedMeta = savedLength(tt, 1, TableUtils.META_FILE_NAME);
            Assert.assertTrue("live _meta must have grown past its copy [live=" + liveMeta + ", saved=" + savedMeta + ']', liveMeta > savedMeta);
            final long liveCv = liveLength(tt, TableUtils.COLUMN_VERSION_FILE_NAME);
            final long savedCv = savedLength(tt, 1, TableUtils.COLUMN_VERSION_FILE_NAME);
            Assert.assertTrue("live _cv must have grown past its copy [live=" + liveCv + ", saved=" + savedCv + ']', liveCv > savedCv);

            final long before = TestUtils.getMetricValue(engine, RECOVERY_EVENTS);
            new RecoveryCoordinator(engine).recover();
            final long afterFirst = TestUtils.getMetricValue(engine, RECOVERY_EVENTS);
            Assert.assertEquals("the first start restores the save point", before + 1, afterFirst);
            for (String fileName : new String[]{TableUtils.TXN_FILE_NAME, TableUtils.COLUMN_VERSION_FILE_NAME, TableUtils.META_FILE_NAME}) {
                Assert.assertTrue("restored " + fileName + " must equal its saved copy", liveEqualsSavedCopy(tt, 1, fileName));
            }

            new RecoveryCoordinator(engine).recover();
            Assert.assertEquals("a second start must recover nothing", afterFirst, TestUtils.getMetricValue(engine, RECOVERY_EVENTS));

            // A fresh process starts with an empty scoreboard. The pooled one still holds the pre-restore txn
            // high-water mark, and the UPDATE's replay would wait for a reader below it.
            engine.getTxnScoreboardPool().remove(tt);
            engine.notifyWalTxnRepublisher(tt);
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tt));
            assertQuery("select count(), sum(v), sum(c1) from t").noRandomAccess().expectSize().returns("count\tsum\tsum1\n" + (K + M + 1) + "\t" + ((K + M) * (K + M + 1) / 2) + "\t15\n");
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
        }
    }

    /**
     * One file per generation: a save point leaves no separate copy files, and its manifest is the header
     * followed by byte copies of the live {@code _txn}, {@code _cv} and {@code _meta}, and nothing else.
     */
    @Test
    public void testSavePointIsOneFilePerGeneration() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        try {
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());
            final TableToken tt = buildTableWithEpochAndLazyTail();
            try (TableWriter w = getWriter(tt)) {
                w.advanceDurableEpoch(2L);
            }
            engine.releaseAllWriters();
            final FilesFacade ff = engine.getConfiguration().getFilesFacade();
            final String[] names = {TableUtils.TXN_FILE_NAME, TableUtils.COLUMN_VERSION_FILE_NAME, TableUtils.META_FILE_NAME};
            long sections = 0;
            try (Path p = new Path()) {
                for (String name : names) {
                    for (String suffix : new String[]{"", ".0", ".1"}) {
                        p.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(name).put(TableUtils.EPOCH_COPY_SUFFIX).put(suffix);
                        Assert.assertFalse("no separate copy file: " + p, ff.exists(p.$()));
                    }
                    sections += liveLength(tt, name);
                }
                p.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(DurableEpochManifest.FILE_NAME).put(".0");
                Assert.assertEquals("the manifest is the header and the three sections",
                        DurableEpochManifest.FILE_SIZE + sections, ff.length(p.$()));
            }
            for (String name : names) {
                Assert.assertTrue("the section of " + name + " must equal the live file", liveEqualsSavedCopy(tt, 0, name));
            }
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
        }
    }

    private static long readLong(FilesFacade ff, Path p, long offset) {
        final long fd = ff.openRO(p.$());
        Assert.assertTrue("must open " + p, fd > -1);
        final long buf = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            Assert.assertEquals(Long.BYTES, ff.read(fd, buf, Long.BYTES, offset));
            return Unsafe.getUnsafe().getLong(buf);
        } finally {
            Unsafe.free(buf, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            ff.close(fd);
        }
    }

    private void assertRows(String tableName, int expected, String context) {
        final List<Long> rows = readVs(tableName);
        Assert.assertEquals(context + ": rows", expected, rows.size());
        for (int i = 0; i < expected; i++) {
            Assert.assertEquals(context + ": row " + i, Long.valueOf(i), rows.get(i));
        }
    }

    private void assertTornCopyHandledSafely(TornMode mode) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        // Drive the epoch explicitly at a KNOWN cut; the auto-epoch must not fire.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        try {
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());

            final TableToken tt = buildTableWithEpochAndLazyTail();
            Assert.assertTrue("live _txn must hold the post-epoch frontier", readTxnSeqTxn(tt) >= K + M);

            engine.releaseAllWriters();
            engine.releaseAllReaders();

            tornByMode(tt, mode);

            // Recovery rejects the newest generation and restores the proven creation baseline.
            new RecoveryCoordinator(engine).recover();
            Assert.assertEquals("a damaged newest generation must fall back to the generation-0 replay floor",
                    0, readTxnSeqTxn(tt));

            engine.notifyWalTxnRepublisher(tt);
            drainWalQueue();

            Assert.assertFalse("table must NOT be suspended after a safe fallback",
                    engine.getTableSequencerAPI().isSuspended(tt));
            assertRows("t", K + M, mode.name());
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
        }
    }

    /**
     * Create an ADAPTIVE table, apply K rows, take a durable epoch at seqTxn=K (into generation one), then apply
     * M MORE rows LAZILY so the live {@code _txn}/{@code _cv} advance past the epoch to the frontier.
     */
    private TableToken buildTableWithEpochAndLazyTail() throws Exception {
        execute("create table t (ts timestamp, v long) timestamp(ts) partition by day wal");
        for (int i = 0; i < K; i++) {
            execute("insert into t values ('2024-10-01T0" + i + ":00:00.000000Z', " + i + ")");
        }
        drainWalQueue();

        final TableToken tt = engine.verifyTableName("t");

        final long epochSeqTxn;
        try (TableWriter w = getWriter(tt)) {
            w.advanceDurableEpoch(1L);
            epochSeqTxn = w.getSeqTxn();
        }
        Assert.assertEquals("epoch must be taken at seqTxn=K", K, epochSeqTxn);

        for (int i = K; i < K + M; i++) {
            execute("insert into t values ('2024-10-01T0" + i + ":00:00.000000Z', " + i + ")");
        }
        drainWalQueue();
        return tt;
    }

    private TableToken buildSweepTable(int iteration) throws Exception {
        final String name = "s" + iteration;
        execute("create table " + name + " (ts timestamp, sym symbol, v long) timestamp(ts) partition by day wal");
        for (int i = 0; i < K; i++) {
            execute("insert into " + name + " values ('2024-10-01T0" + i + ":00:00.000000Z', 'a" + i + "', " + i + ")");
        }
        execute("alter table " + name + " add column sym2 symbol");
        for (int i = K; i < K + M; i++) {
            execute("insert into " + name + " values ('2024-10-01T0" + i + ":00:00.000000Z', 'a" + i + "', " + i + ", 'b')");
        }
        drainWalQueue();
        final TableToken tt = engine.verifyTableName(name);
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tt));
        return tt;
    }

    private void corruptRange(TableToken tt, String fileName, boolean flipOneByte) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path p = new Path()) {
            final long[] copy = savedCopy(p, tt, 1, fileName);
            Assert.assertTrue("the saved " + fileName + " must be non-empty", copy[1] > 0);
            final long offset = flipOneByte ? copy[0] + copy[1] / 2 : copy[0];
            final int len = flipOneByte ? 1 : (int) copy[1];
            final long fd = ff.openRW(p.$(), engine.getConfiguration().getWriterFileOpenOpts());
            Assert.assertTrue("must open " + p, fd > 0);
            final long buf = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
            try {
                Assert.assertEquals(len, ff.read(fd, buf, len, offset));
                for (int i = 0; i < len; i++) {
                    final byte b = Unsafe.getUnsafe().getByte(buf + i);
                    Unsafe.getUnsafe().putByte(buf + i, flipOneByte ? (byte) ~b : (byte) 0xA5);
                }
                Assert.assertEquals(len, ff.write(fd, buf, len, offset));
            } finally {
                Unsafe.free(buf, len, MemoryTag.NATIVE_DEFAULT);
                ff.close(fd);
            }
        }
    }

    // Every iteration marks the whole database durable, so a table left behind slows every later iteration.
    private void dropSweepTable(TableToken tt) throws Exception {
        execute("drop table " + tt.getTableName());
        drainWalQueue();
    }

    private long liveLength(TableToken tt, String fileName) {
        try (Path p = new Path()) {
            p.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(fileName);
            return engine.getConfiguration().getFilesFacade().length(p.$());
        }
    }

    private boolean liveEqualsSavedCopy(TableToken tt, int generation, String fileName) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path saved = new Path(); Path live = new Path()) {
            final long[] copy = savedCopy(saved, tt, generation, fileName);
            live.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(fileName);
            final long len = copy[1];
            if (ff.length(live.$()) != len) {
                return false;
            }
            final long savedBuf = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
            final long liveBuf = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
            final long savedFd = ff.openRO(saved.$());
            final long liveFd = ff.openRO(live.$());
            try {
                return ff.read(savedFd, savedBuf, len, copy[0]) == len
                        && ff.read(liveFd, liveBuf, len, 0) == len
                        && io.questdb.std.Vect.memeq(savedBuf, liveBuf, len);
            } finally {
                ff.close(liveFd);
                ff.close(savedFd);
                Unsafe.free(liveBuf, len, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(savedBuf, len, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    private long readTxnSeqTxn(TableToken tt) {
        try (TxReader tx = new TxReader(engine.getConfiguration().getFilesFacade());
             Path p = new Path()) {
            p.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(TableUtils.TXN_FILE_NAME);
            tx.ofRO(p.$(), ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY);
            if (!tx.unsafeLoadAll()) {
                throw new IllegalStateException("live _txn did not load (bricked)");
            }
            return tx.getSeqTxn();
        }
    }

    private List<Long> readVs(String tableName) {
        final List<Long> out = new ArrayList<>();
        try (
                SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(engine);
                RecordCursorFactory f = engine.select("select v from " + tableName + " order by ts", ctx);
                RecordCursor c = f.getCursor(ctx)
        ) {
            final io.questdb.cairo.sql.Record r = c.getRecord();
            while (c.hasNext()) {
                out.add(r.getLong(0));
            }
        } catch (SqlException e) {
            throw new RuntimeException(e);
        }
        return out;
    }

    /**
     * Points {@code p} at the file that holds the save point's copy of {@code fileName} in {@code generation},
     * and returns the copy's offset and length in it: the separate copy file of the older layout, else the
     * file's section in the generation's manifest (sizes in the header at 48, 64 and 80; sections in
     * {@code _txn}, {@code _cv}, {@code _meta} order after the header).
     */
    private long[] savedCopy(Path p, TableToken tt, int generation, String fileName) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        p.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(fileName)
                .put(TableUtils.EPOCH_COPY_SUFFIX).put('.').put(generation);
        if (ff.exists(p.$())) {
            return new long[]{0, ff.length(p.$())};
        }
        p.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(DurableEpochManifest.FILE_NAME).put('.').put(generation);
        final long txnSize = readLong(ff, p, 48);
        final long cvSize = readLong(ff, p, 64);
        final long metaSize = readLong(ff, p, 80);
        final long header = DurableEpochManifest.FILE_SIZE;
        if (TableUtils.TXN_FILE_NAME.equals(fileName)) {
            return new long[]{header, txnSize};
        }
        if (TableUtils.COLUMN_VERSION_FILE_NAME.equals(fileName)) {
            return new long[]{header + txnSize, cvSize};
        }
        return new long[]{header + txnSize + cvSize, metaSize};
    }

    private long savedLength(TableToken tt, int generation, String fileName) {
        try (Path p = new Path()) {
            return savedCopy(p, tt, generation, fileName)[1];
        }
    }

    private void takeTwoSavePoints(TableToken tt) {
        try (TableWriter w = getWriter(tt)) {
            w.advanceDurableEpoch(1L);
            w.advanceDurableEpoch(2L);
        }
    }

    private void tornByMode(TableToken tt, TornMode mode) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path p = new Path()) {
            switch (mode) {
                case TRUNCATE_TXN_EPOCH_TO_ZERO -> {
                    // Cut the file at the start of the _txn copy: nothing of it survives.
                    final long[] copy = savedCopy(p, tt, 1, TableUtils.TXN_FILE_NAME);
                    final long fd = ff.openRW(p.$(), engine.getConfiguration().getWriterFileOpenOpts());
                    Assert.assertTrue("must open " + p, fd > 0);
                    try {
                        Assert.assertTrue("cut the _txn copy", ff.truncate(fd, copy[0]));
                    } finally {
                        ff.close(fd);
                    }
                }
                case CORRUPT_TXN_EPOCH_BODY -> corruptRange(tt, TableUtils.TXN_FILE_NAME, false);
                case FLIP_TXN_BYTE -> corruptRange(tt, TableUtils.TXN_FILE_NAME, true);
                case FLIP_CV_BYTE -> corruptRange(tt, TableUtils.COLUMN_VERSION_FILE_NAME, true);
                case FLIP_META_BYTE -> corruptRange(tt, TableUtils.META_FILE_NAME, true);
                case TAMPERED_META_EPOCH -> {
                    final long[] copy = savedCopy(p, tt, 1, TableUtils.META_FILE_NAME);
                    final long fd = ff.openRW(p.$(), engine.getConfiguration().getWriterFileOpenOpts());
                    Assert.assertTrue("must open " + p, fd > 0);
                    final long buf = Unsafe.malloc(Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
                    try {
                        final long offset = copy[0] + TableUtils.META_OFFSET_TABLE_ID;
                        Assert.assertEquals(Integer.BYTES, ff.read(fd, buf, Integer.BYTES, offset));
                        Unsafe.getUnsafe().putInt(buf, Unsafe.getUnsafe().getInt(buf) + 1000);
                        Assert.assertEquals(Integer.BYTES, ff.write(fd, buf, Integer.BYTES, offset));
                    } finally {
                        Unsafe.free(buf, Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
                        ff.close(fd);
                    }
                }
                case MANIFEST_SEQTXN_MISMATCH, TORN_HEADER -> {
                    p.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(DurableEpochManifest.FILE_NAME).put('.').put(1);
                    final long fd = ff.openRW(p.$(), engine.getConfiguration().getWriterFileOpenOpts());
                    Assert.assertTrue("must open generation manifest", fd > 0);
                    final int len = mode == TornMode.TORN_HEADER ? 16 : Long.BYTES;
                    final long buf = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
                    try {
                        if (mode == TornMode.TORN_HEADER) {
                            // The header's last 16 bytes (closing magic and checksum) did not land.
                            Unsafe.getUnsafe().setMemory(buf, len, (byte) 0);
                            Assert.assertEquals(len, ff.write(fd, buf, len, DurableEpochManifest.FILE_SIZE - len));
                        } else {
                            Unsafe.getUnsafe().putLong(buf, K + 1000L);
                            Assert.assertEquals(len, ff.write(fd, buf, len, 24));
                        }
                    } finally {
                        Unsafe.free(buf, len, MemoryTag.NATIVE_DEFAULT);
                        ff.close(fd);
                    }
                }
                default -> throw new AssertionError(mode);
            }
        }
    }

    private enum TornMode {
        CORRUPT_TXN_EPOCH_BODY,
        FLIP_CV_BYTE,
        FLIP_META_BYTE,
        FLIP_TXN_BYTE,
        MANIFEST_SEQTXN_MISMATCH,
        TAMPERED_META_EPOCH,
        TORN_HEADER,
        TRUNCATE_TXN_EPOCH_TO_ZERO
    }
}
