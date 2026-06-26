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
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.griffin.SqlException;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * The crash-durability ORACLE for adaptive GROUP-COMMIT (Deferred 2, {@code W > 0}).
 *
 * <h3>How the crash model sees the msync-vs-fdatasync gap</h3>
 * {@link CrashFaultFilesFacade} models {@code msync(MS_SYNC)} (async=false) as DURABLE (it journal-commits
 * and device-flushes), but {@code msync(MS_ASYNC)} (async=true) as NON-durable (page-cache writeback only —
 * no journal commit, no device flush, so a crash rolls it back). The W&gt;0 deferred path issues the
 * per-commit segment/events/sequencer push as {@code msync(MS_ASYNC)} and batches the {@code fdatasync}
 * (the device flush) into {@code WalWriter.flushPendingDurable}. So the facade DOES capture the gap: a
 * commit sequenced+msync'd but whose batch fdatasync had not completed is NON-durable and is lost on crash,
 * while a commit whose batch fdatasync completed (exactly when {@code localDurableSeqTxn} advances) is
 * durable. The two tests below exploit this directly.
 *
 * <ul>
 *   <li><b>Durable-ack safety</b> (must hold): a txn whose {@code localDurableSeqTxn} has advanced
 *       (durable-ack emitted) ALWAYS survives a crash.</li>
 *   <li><b>RPO bound + no corruption</b>: a commit sequenced &lt; W ago but NOT yet flushed MAY be lost on a
 *       power loss; recovery rolls forward whatever IS durable with NO corruption / suspend, and the
 *       durable-ack never claimed the lost txn.</li>
 * </ul>
 */
public class AdaptiveGroupCommitCrashTest extends AbstractCrashConsistencyTest {

    private static final long WINDOW_US = 1_000_000L; // 1s window driven by the test microsecond clock

    /**
     * DURABLE-ACK SAFETY: a txn whose {@code localDurableSeqTxn} has advanced (the durable-ack fired) MUST
     * survive a crash. Commit under W&gt;0, force the batch flush (durable advances past the txn), crash,
     * reopen — the rows are present and correct.
     */
    @Test
    public void testDurableAckedTxnSurvivesCrash() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW_US, String.valueOf(WINDOW_US));
        // Epoch every apply batch so the MATERIALIZED table is recoverable end-to-end (the durable epoch +
        // roll-forward rebuilds the applied columns from the durable WAL). The group-commit fdatasync is what
        // makes that WAL durable; this test asserts a durable-ACK'd WAL txn survives the crash end to end.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, 0);
        try {
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());
            runWithCrashFacade(() -> {
                setCurrentMicros(1_000_000L);
                execute("create table t (ts timestamp, v long) timestamp(ts) partition by day wal");
                final TableToken tt = engine.verifyTableName("t");
                final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);
                markDurableBaseline();

                final long durableSeqTxn;
                try (WalWriter w = engine.getWalWriter(tt)) {
                    // Two commits within the window: pending (msync(MS_ASYNC)'d, not yet device-durable).
                    setCurrentMicros(1_000_000L);
                    commitRow(w, ts("2024-01-01T00:00:00.000000Z"), 10);
                    setCurrentMicros(1_001_000L);
                    commitRow(w, ts("2024-01-01T01:00:00.000000Z"), 11);
                    Assert.assertTrue("must be pending before flush",
                            tracker.getLocalDurableSeqTxn() < tracker.getSeqTxn());

                    // Force the batched device flush (window elapsed -> the background sweep flushes it).
                    setCurrentMicros(1_000_000L + WINDOW_US + 1000L);
                    try (ExposedFlusher flusher = new ExposedFlusher(engine)) {
                        flusher.flushNow();
                    }
                    durableSeqTxn = tracker.getLocalDurableSeqTxn();
                    Assert.assertEquals("durable frontier must catch up to the committed seqTxn after the flush",
                            tracker.getSeqTxn(), durableSeqTxn);
                }

                // Materialize the durable WAL into the table so a reopened reader sees the rows. Apply does
                // not change WAL durability (which is what the durable-ack reflects).
                drainWalQueue();

                // POWER LOSS, then reopen + recover from the durable WAL frontier.
                crashAndReopen();
                new io.questdb.cairo.RecoveryCoordinator(engine).recover();
                engine.notifyWalTxnRepublisher(tt);
                drainWalQueue();

                Assert.assertFalse("table must not be suspended after recovery",
                        engine.getTableSequencerAPI().isSuspended(tt));
                final List<Long> rows = readVs();
                Assert.assertEquals("both durable-ack'd rows must survive the crash", 2, rows.size());
                Assert.assertEquals(Long.valueOf(10), rows.get(0));
                Assert.assertEquals(Long.valueOf(11), rows.get(1));
                Assert.assertTrue("durable frontier covered both txns", durableSeqTxn >= 2);
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW_US, "0");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, 1000);
        }
    }

    /**
     * RPO BOUND + no corruption: a commit sequenced but whose batch fdatasync had NOT completed MAY be lost
     * on a power loss, while the durable state is intact (NO corruption / suspend) and the durable-ack never
     * claimed the lost txn.
     *
     * <p>The first commit is made durable via a CLEAN release (its WAL fdatasync ran) + applied + epoch'd —
     * the durable baseline. The second commit is left PENDING on a held writer (its segment/events/sequencer
     * are only {@code msync(MS_ASYNC)}'d, NOT device-flushed), then a POWER LOSS rolls every file back to its
     * last durable content. The model drops the non-durable {@code MS_ASYNC} bytes of commit #2 — including
     * its sequencer record — so the DURABLE WAL frontier reverts to commit #1: the un-flushed commit is lost
     * (RPO &le; W), the durable baseline reads back clean as {@code [1]}, and {@code localDurableSeqTxn}
     * never advanced over the lost txn.
     */
    @Test
    public void testUnflushedTxnMayBeLostButNoCorruption() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW_US, String.valueOf(WINDOW_US));
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, 0);
        try {
            runWithCrashFacade(() -> {
                setCurrentMicros(1_000_000L);
                execute("create table t (ts timestamp, v long) timestamp(ts) partition by day wal");
                final TableToken tt = engine.verifyTableName("t");
                final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);

                // First commit via the SQL path -> the WAL writer is released, flushing it device-durable.
                // Apply + epoch it, then mark it the durable baseline ("old, already on disk").
                setCurrentMicros(1_000_000L);
                execute("insert into t values ('2024-01-01T00:00:00.000000Z', 1)");
                drainWalQueue();
                final long durableAfterFirst = tracker.getLocalDurableSeqTxn();
                final long frontierAfterFirst = engine.getTableSequencerAPI().lastTxn(tt);
                Assert.assertEquals("first commit must be durable (synchronous release flush)", 1, durableAfterFirst);
                markDurableBaseline();

                // Second commit on a HELD writer, left PENDING (un-flushed). It advances the sequencer's
                // maxTxn (in-memory + an MS_ASYNC, non-durable txnlog write) but NOT localDurableSeqTxn.
                final long unflushedSeqTxn;
                WalWriter w = engine.getWalWriter(tt);
                try {
                    setCurrentMicros(1_001_000L);
                    commitRow(w, ts("2024-01-01T01:00:00.000000Z"), 2);
                    unflushedSeqTxn = tracker.getSeqTxn();
                    Assert.assertEquals("the second commit sequenced past the first", frontierAfterFirst + 1, unflushedSeqTxn);
                    Assert.assertTrue("the durable-ack must NOT have advanced over the un-flushed txn",
                            tracker.getLocalDurableSeqTxn() < unflushedSeqTxn);
                    // POWER LOSS: distress the writer + drop its pending WITHOUT a flush (close() then takes
                    // the distressed path, never advancing localDurableSeqTxn over the un-flushed txn), and
                    // roll every file back to its last durable content (the non-durable MS_ASYNC tail — incl.
                    // commit #2's sequencer record — is dropped). A clean release would have flushed it.
                    w.simulatePowerLossDropPending();
                    crashFf.crash(engine.getConfiguration().getDbRoot());
                } finally {
                    w.close();
                }
                // Re-read the DURABLE on-disk state as a fresh boot would: drop all live handles + the
                // in-memory sequencer (whose maxTxn had advanced to the now-lost commit #2).
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                engine.releaseInactiveTableSequencers();

                // The DURABLE WAL frontier reverted to commit #1 — the un-flushed commit #2 is LOST (RPO<=W).
                final long durableFrontierAfterCrash = engine.getTableSequencerAPI().lastTxn(tt);
                Assert.assertEquals(
                        "the un-flushed commit must be lost: the durable WAL frontier reverts to commit #1",
                        frontierAfterFirst, durableFrontierAfterCrash
                );
                Assert.assertTrue("RPO bound: the lost txn was strictly beyond the durable frontier",
                        durableFrontierAfterCrash < unflushedSeqTxn);

                // NO corruption: the durable baseline (commit #1, epoch'd) reads back clean — not torn, not
                // suspended — and never shows a silently-wrong value.
                Assert.assertFalse("table must not be suspended after a power loss of an un-flushed tail",
                        engine.getTableSequencerAPI().isSuspended(tt));
                final List<Long> rows = readVs();
                Assert.assertEquals("only the durable first row survives", 1, rows.size());
                Assert.assertEquals(Long.valueOf(1), rows.get(0));
                // Durable-ack safety holds on the loss side too: the durable-ack never claimed commit #2.
                Assert.assertTrue("the durable-ack frontier never claimed the un-flushed txn",
                        durableAfterFirst < unflushedSeqTxn);
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW_US, "0");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, 1000);
        }
    }

    /** Append one (ts, v) row to a HELD WalWriter and commit it (one WAL txn). */
    private static void commitRow(WalWriter w, long tsMicros, long v) {
        TableWriter.Row row = w.newRow(tsMicros);
        row.putLong(1, v);
        row.append();
        w.commit();
    }

    private static long ts(String s) {
        try {
            return MicrosFormatUtils.parseTimestamp(s);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private List<Long> readVs() {
        final List<Long> out = new ArrayList<>();
        try (RecordCursorFactory f = select("select v from t order by ts")) {
            try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
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

    /** Exposes WalPurgeJob.runSerially() to drive the background group-commit flush deterministically. */
    static class ExposedFlusher extends WalPurgeJob {
        ExposedFlusher(io.questdb.cairo.CairoEngine engine) {
            super(engine);
        }

        boolean flushNow() {
            return runSerially();
        }
    }
}
