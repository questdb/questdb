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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.SnapshotMarker;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * C1 crash oracle: recovery must NOT restore a TORN {@code _txn.epoch}/{@code _cv.epoch} copy.
 *
 * <p>The durable-epoch anchor is three files written NON-ATOMICALLY ({@code _cv.epoch}, then
 * {@code _txn.epoch}, then the {@code _snapshot} marker), and each {@code .epoch} copy is
 * single-buffered ({@code writeEpochCopy} = {@code creat(O_TRUNC)} + copy + fsync). A power cut INSIDE
 * that window can leave a LOADABLE {@code _snapshot} marker pointing at a {@code _txn.epoch} that is
 * 0-byte / half-written / stale. The old {@link RecoveryCoordinator} only checked
 * {@code ff.exists()} and then blindly {@code ff.copy()}'d the {@code .epoch} over the HEALTHY live
 * {@code _txn}/{@code _cv} — truncating/corrupting them and BRICKING the table on the next open
 * (a 0-byte {@code _txn.epoch} truncates live {@code _txn} to 0, so {@code TxReader} cannot open it).
 *
 * <p>This test reproduces the window directly: it produces a valid epoch + {@code _snapshot} marker,
 * applies more rows lazily (so the live {@code _txn}/{@code _cv} sit at the post-epoch FRONTIER and are
 * the ONLY healthy copy of that tail), then corrupts the {@code _txn.epoch} (truncate-to-0 or
 * corrupt-body) and runs {@code recover()}.
 *
 * <p><b>Expected (after the fix):</b> recovery VALIDATES the {@code .epoch} copies (loads {@code _txn.epoch}
 * with {@link TxReader} + cross-checks its {@code seqTxn} against the marker, loads {@code _cv.epoch} with
 * {@code ColumnVersionReader}); the torn copy fails validation, the restore is SKIPPED, and the table
 * falls through to normal open — the live {@code _txn}/{@code _cv} survive intact and a full WAL replay
 * re-derives every row. All N+M rows read back, table not suspended.
 *
 * <p><b>Before the fix this BRICKS:</b> {@code recover()} copies the 0-byte {@code _txn.epoch} over live
 * {@code _txn}, truncating it to 0; reopening throws (or returns no rows) — proven by
 * {@link #testNegativeControlOldBehaviourBricksOnTornCopy()}, which re-creates the exact unvalidated
 * copy-over-live behaviour and asserts the live {@code _txn} is destroyed.
 */
public class AdaptiveRecoveryTornEpochCopyCrashTest extends AbstractCairoTest {

    private static final int K = 4; // rows before the epoch
    private static final int M = 5; // rows applied lazily AFTER the epoch (live _txn/_cv advance past it)

    /**
     * GREEN (the fix): a 0-byte {@code _txn.epoch} is detected as torn, the restore is skipped, the live
     * files survive and the table rebuilds all rows from the durable WAL.
     */
    @Test
    public void testRecoverySkipsZeroByteTxnEpochAndKeepsTableIntact() throws Exception {
        assertTornCopyHandledSafely(TornMode.TRUNCATE_TXN_EPOCH_TO_ZERO);
    }

    /**
     * GREEN (the fix): a full-size but corrupt-BODY {@code _txn.epoch} (its A/B record overwritten with
     * garbage) fails the checksum/seqTxn validation and is skipped — same safe fallback.
     */
    @Test
    public void testRecoverySkipsCorruptBodyTxnEpochAndKeepsTableIntact() throws Exception {
        assertTornCopyHandledSafely(TornMode.CORRUPT_TXN_EPOCH_BODY);
    }

    /**
     * GREEN (the fix): a STALE {@code _txn.epoch} whose {@code seqTxn} does not match the {@code _snapshot}
     * marker (the anchor trio disagree) is rejected by the seqTxn cross-check and skipped.
     */
    @Test
    public void testRecoverySkipsSeqTxnMismatchedTxnEpochAndKeepsTableIntact() throws Exception {
        assertTornCopyHandledSafely(TornMode.MARKER_SEQTXN_AHEAD_OF_COPY);
    }

    /**
     * NEGATIVE CONTROL — proves the brick is real. Re-creates the OLD unvalidated behaviour (blindly copy
     * the torn {@code _txn.epoch} over live {@code _txn}, exactly what {@code RecoveryCoordinator} did
     * before C1) and asserts the live {@code _txn} is DESTROYED (truncated to 0, no longer openable).
     * This is the failure the GREEN tests above prevent.
     */
    @Test
    public void testNegativeControlOldBehaviourBricksOnTornCopy() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, -1);
        try {
            final FilesFacade ff = engine.getConfiguration().getFilesFacade();
            final TableToken tt = buildTableWithEpochAndLazyTail();

            // The live _txn is healthy here (post-epoch frontier).
            final long liveSeqTxnBefore = readTxnSeqTxn(tt);
            Assert.assertTrue("live _txn must be healthy + ahead of the epoch before the bad restore",
                    liveSeqTxnBefore >= K + M);

            engine.releaseAllWriters();
            engine.releaseAllReaders();

            // Make _txn.epoch torn (0 bytes), then EMULATE the old code: copy it straight over live _txn.
            tornByMode(tt, TornMode.TRUNCATE_TXN_EPOCH_TO_ZERO);
            try (Path src = new Path(); Path dst = new Path()) {
                src.of(engine.getConfiguration().getDbRoot()).concat(tt)
                        .concat(TableUtils.TXN_FILE_NAME).put(TableUtils.EPOCH_COPY_SUFFIX);
                dst.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(TableUtils.TXN_FILE_NAME);
                Assert.assertTrue("emulated unvalidated restore (the pre-C1 bug) must copy",
                        ff.copy(src.$(), dst.$()) >= 0);
            }

            // The live _txn is now bricked: a 0-byte _txn can no longer be opened by TxReader.
            boolean bricked = false;
            try {
                readTxnSeqTxn(tt);
            } catch (Throwable expected) {
                bricked = true;
            }
            Assert.assertTrue(
                    "pre-C1 behaviour MUST brick: copying a 0-byte _txn.epoch over live _txn truncates it to 0",
                    bricked
            );
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, 1000);
        }
    }

    // ---- core scenario ----

    private void assertTornCopyHandledSafely(TornMode mode) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        // Drive the epoch explicitly at a KNOWN cut; the auto-epoch must not fire.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, -1);
        try {
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());

            final TableToken tt = buildTableWithEpochAndLazyTail();

            // Pre-condition: live _txn/_cv are at the post-epoch frontier and OPENABLE (the healthy tail).
            final long liveSeqTxnBefore = readTxnSeqTxn(tt);
            Assert.assertTrue("live _txn must hold the post-epoch frontier", liveSeqTxnBefore >= K + M);

            engine.releaseAllWriters();
            engine.releaseAllReaders();

            // Corrupt the durable epoch copy to model a crash inside the epoch-copy window.
            tornByMode(tt, mode);

            // ACT: recovery. With the C1 guard it must DETECT the torn/mismatched copy and SKIP the restore
            // (NOT copy it over the live files). Recovery itself must not throw.
            new RecoveryCoordinator(engine).recover();

            // The live _txn must be UNTOUCHED (still the post-epoch frontier) — the torn copy was NOT
            // restored over it. (Before C1 this is 0 / unreadable.)
            final long liveSeqTxnAfter = readTxnSeqTxn(tt);
            Assert.assertEquals(
                    "live _txn must be intact after a skipped restore (torn copy must NOT overwrite it)",
                    liveSeqTxnBefore, liveSeqTxnAfter
            );

            // Re-init the WAL tracker + drain: the table opens normally and a full replay keeps all rows.
            engine.notifyWalTxnRepublisher(tt);
            drainWalQueue();

            Assert.assertFalse("table must NOT be suspended after a safe fallback",
                    engine.getTableSequencerAPI().isSuspended(tt));

            final List<Long> rows = readVs(engine, tt);
            Assert.assertEquals("all N+M rows must survive via the live files + WAL replay", K + M, rows.size());
            for (int i = 0; i < K + M; i++) {
                Assert.assertEquals("row " + i + " value", Long.valueOf(i), rows.get(i));
            }
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, 1000);
        }
    }

    /**
     * Create an ADAPTIVE table, apply K rows, take a durable epoch at seqTxn=K (writing the immutable
     * {@code _txn.epoch}/{@code _cv.epoch} + the {@code _snapshot} marker exactly as the apply-worker
     * {@code advance()} hook does), then apply M MORE rows LAZILY so the live {@code _txn}/{@code _cv}
     * advance past the epoch to the frontier. Returns the table token.
     */
    private TableToken buildTableWithEpochAndLazyTail() throws Exception {
        execute("create table t (ts timestamp, v long) timestamp(ts) partition by day wal");
        for (int i = 0; i < K; i++) {
            execute("insert into t values ('2024-10-01T0" + i + ":00:00.000000Z', " + i + ")");
        }
        drainWalQueue();

        final TableToken tt = engine.verifyTableName("t");

        final long epochSeqTxn;
        final long epochTxn;
        try (TableWriter w = getWriter(tt)) {
            w.fsyncMaterializedState();
            epochSeqTxn = w.getSeqTxn();
            epochTxn = w.getTxn();
        }
        Assert.assertEquals("epoch must be taken at seqTxn=K", K, epochSeqTxn);
        try (SnapshotMarker marker = new SnapshotMarker(engine.getConfiguration());
             Path p = new Path()) {
            p.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(TableUtils.SNAPSHOT_FILE_NAME);
            marker.of(p.$());
            marker.write(epochSeqTxn, epochTxn, 1L);
        }

        // M more rows applied lazily AFTER the epoch (no new epoch fires).
        for (int i = K; i < K + M; i++) {
            execute("insert into t values ('2024-10-01T0" + i + ":00:00.000000Z', " + i + ")");
        }
        drainWalQueue();
        return tt;
    }

    // ---- torn-copy injectors ----

    private enum TornMode {
        TRUNCATE_TXN_EPOCH_TO_ZERO,
        CORRUPT_TXN_EPOCH_BODY,
        MARKER_SEQTXN_AHEAD_OF_COPY
    }

    private void tornByMode(TableToken tt, TornMode mode) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path p = new Path()) {
            switch (mode) {
                case TRUNCATE_TXN_EPOCH_TO_ZERO: {
                    p.of(engine.getConfiguration().getDbRoot()).concat(tt)
                            .concat(TableUtils.TXN_FILE_NAME).put(TableUtils.EPOCH_COPY_SUFFIX);
                    final long fd = ff.openRW(p.$(), engine.getConfiguration().getWriterFileOpenOpts());
                    Assert.assertTrue("must open _txn.epoch", fd > 0);
                    try {
                        Assert.assertTrue("truncate _txn.epoch to 0", ff.truncate(fd, 0));
                    } finally {
                        ff.close(fd);
                    }
                    break;
                }
                case CORRUPT_TXN_EPOCH_BODY: {
                    // Overwrite the whole file with garbage bytes: both A/B records fail their checksum.
                    p.of(engine.getConfiguration().getDbRoot()).concat(tt)
                            .concat(TableUtils.TXN_FILE_NAME).put(TableUtils.EPOCH_COPY_SUFFIX);
                    final long len = ff.length(p.$());
                    Assert.assertTrue("the _txn.epoch must exist + be non-empty", len > 0);
                    final long fd = ff.openRW(p.$(), engine.getConfiguration().getWriterFileOpenOpts());
                    Assert.assertTrue("must open _txn.epoch", fd > 0);
                    try {
                        final int n = (int) len;
                        final long buf = io.questdb.std.Unsafe.malloc(n, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                        try {
                            for (int i = 0; i < n; i++) {
                                io.questdb.std.Unsafe.getUnsafe().putByte(buf + i, (byte) 0xA5);
                            }
                            Assert.assertEquals("overwrite _txn.epoch body with garbage", n, ff.write(fd, buf, n, 0));
                        } finally {
                            io.questdb.std.Unsafe.free(buf, n, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                        }
                    } finally {
                        ff.close(fd);
                    }
                    break;
                }
                case MARKER_SEQTXN_AHEAD_OF_COPY: {
                    // The _txn.epoch is a perfectly valid record at seqTxn=K, but the _snapshot marker claims
                    // a DIFFERENT (later) epochSeqTxn — the anchor trio disagree (a stale copy under a newer
                    // marker). The seqTxn cross-check must reject this.
                    try (SnapshotMarker marker = new SnapshotMarker(engine.getConfiguration())) {
                        p.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(TableUtils.SNAPSHOT_FILE_NAME);
                        marker.of(p.$());
                        Assert.assertTrue(marker.tryLoad());
                        // Rewrite the marker with a seqTxn the copy cannot match (K + 1000).
                        marker.write(marker.getEpochSeqTxn() + 1000, marker.getEpochTxn(), 2L);
                    }
                    break;
                }
                default:
                    throw new AssertionError(mode);
            }
        }
    }

    // ---- readers ----

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

    private List<Long> readVs(CairoEngine eng, TableToken ignored) {
        final List<Long> out = new ArrayList<>();
        try (
                SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(eng);
                RecordCursorFactory f = eng.select("select v from t order by ts", ctx)
        ) {
            try (RecordCursor c = f.getCursor(ctx)) {
                io.questdb.cairo.sql.Record r = c.getRecord();
                while (c.hasNext()) {
                    out.add(r.getLong(0));
                }
            }
        } catch (SqlException e) {
            throw new RuntimeException(e);
        }
        return out;
    }
}
