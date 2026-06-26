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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * The HEADLINE proof for Deferred 1 — PER-TABLE commit-mode ISOLATION, with the global default set to
 * {@code nosync} (the OPPOSITE of the per-table mode under test).
 *
 * <p>Two complementary tests:
 * <ol>
 *   <li>{@link #testAdaptiveTableFiresEpochWhileNosyncSiblingDoesNot()} — side by side on the SAME
 *       nosync instance, a {@code WITH commit_mode='adaptive'} table enters the FULL adaptive lifecycle
 *       (durable WAL + lazy apply + durable EPOCH) while a default (nosync) sibling does NOT fire any
 *       epoch. This is the per-table isolation proof (no crash needed; deterministic).</li>
 *   <li>{@link #testAdaptiveTableRecoversAfterCrashUnderGlobalNosync()} — the durability half: under
 *       global nosync, the adaptive table's WAL/sequencer ARE made durable (despite the nosync default),
 *       so after a crash that drops the lazily-applied column data, RECOVERY rolls every row forward from
 *       the durable WAL. A nosync table would lose those rows — recovery only fires because the table's
 *       PER-TABLE mode is adaptive.</li>
 * </ol>
 *
 * <p>Both run under per-inode journaling ({@code modelSharedJournal=false}) so a journal commit on
 * {@code _txn} does not incidentally journal the column files — the post-epoch columns are durable only
 * if something explicitly flushed them, which under truly-lazy adaptive nothing does.
 *
 * <p>The crash test uses a SINGLE adaptive table on purpose: a nosync table's {@code txn_seq}/columns are
 * genuinely torn by a power cut (nothing fsync'd them), and the test harness cannot cleanly close a torn
 * nosync sequencer on teardown — so the nosync-sibling isolation is proven by the (crash-free) first test.
 */
public class PerTableAdaptiveIsolationCrashTest extends AbstractCrashConsistencyTest {

    private static final int K = 4; // rows before the epoch
    private static final int M = 5; // rows after the epoch (lazily applied)

    /**
     * Per-table isolation (no crash): global nosync; an adaptive table fires a durable epoch while a
     * nosync sibling on the same instance does not. Proves the epoch lifecycle is driven by the PER-TABLE
     * effective mode, not the global one.
     */
    @Test
    public void testAdaptiveTableFiresEpochWhileNosyncSiblingDoesNot() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, 0); // epoch on the first applied batch
        try {
            assertMemoryLeak(() -> {
                execute("create table a (ts timestamp, v long) timestamp(ts) partition by day wal " +
                        "with commit_mode='adaptive'");
                execute("create table b (ts timestamp, v long) timestamp(ts) partition by day wal");
                for (int i = 0; i < K; i++) {
                    String ts = "'2024-10-01T0" + i + ":00:00.000000Z'";
                    execute("insert into a values (" + ts + ", " + i + ")");
                    execute("insert into b values (" + ts + ", " + i + ")");
                }
                drainWalQueue();

                final TableToken ttA = engine.verifyTableName("a");
                final TableToken ttB = engine.verifyTableName("b");
                final SeqTxnTracker trackerA = engine.getTableSequencerAPI().getTxnTracker(ttA);
                final SeqTxnTracker trackerB = engine.getTableSequencerAPI().getTxnTracker(ttB);

                // a (adaptive) fired a durable epoch despite the global nosync default.
                Assert.assertTrue(
                        "adaptive 'a' must fire a durable epoch under global nosync (durableEpochSeqTxn>0)",
                        trackerA.getDurableEpochSeqTxn() > 0
                );
                Assert.assertTrue("'a' must have a _snapshot epoch marker", snapshotExists(ttA));

                // b (nosync) did NOT.
                Assert.assertEquals(
                        "nosync sibling 'b' must NOT fire an epoch (durableEpochSeqTxn==0)",
                        0L, trackerB.getDurableEpochSeqTxn()
                );
                Assert.assertFalse("'b' must NOT have a _snapshot marker", snapshotExists(ttB));

                // Both tables read back correctly (sanity).
                Assert.assertEquals(K, readVs(engine, "a").size());
                Assert.assertEquals(K, readVs(engine, "b").size());
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, 1000);
        }
    }

    /**
     * Durability under global nosync: an adaptive table's WAL is made durable despite the nosync default,
     * so a crash that drops the lazily-applied post-epoch columns is fully recovered from the WAL.
     */
    @Test
    public void testAdaptiveTableRecoversAfterCrashUnderGlobalNosync() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync"); // global default is the OPPOSITE of the table mode
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, 0);
        try {
            runWithCrashFacade(() -> {
                crashFf.modelSharedJournal = false;

                execute("create table a (ts timestamp, v long) timestamp(ts) partition by day wal " +
                        "with commit_mode='adaptive'");

                // K rows -> apply -> the apply worker fires a durable epoch (interval 0) at seqTxn=K.
                for (int i = 0; i < K; i++) {
                    execute("insert into a values ('2024-10-01T0" + i + ":00:00.000000Z', " + i + ")");
                }
                drainWalQueue();

                final TableToken ttA = engine.verifyTableName("a");
                Assert.assertTrue(
                        "adaptive 'a' fired an epoch under global nosync",
                        engine.getTableSequencerAPI().getTxnTracker(ttA).getDurableEpochSeqTxn() > 0
                );

                // Disable the epoch -> the next M rows are applied LAZILY (no new durable cut).
                setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, -1);
                for (int i = K; i < K + M; i++) {
                    execute("insert into a values ('2024-10-01T0" + i + ":00:00.000000Z', " + i + ")");
                }
                drainWalQueue();

                Assert.assertEquals("pre-crash 'a' sees all K+M rows", K + M, readVs(engine, "a").size());

                // CRASH: drop non-durable column data; keep fsync/msync'd state + the durable WAL.
                crashAndReopen();

                // RESTART: recovery rewinds a's epoch cut, then the boot WAL apply re-derives (K, K+M].
                new io.questdb.cairo.RecoveryCoordinator(engine).recover();
                engine.notifyWalTxnRepublisher(ttA);
                drainWalQueue();

                Assert.assertFalse("'a' must NOT be suspended after recovery",
                        engine.getTableSequencerAPI().isSuspended(ttA));
                final List<Long> post = readVs(engine, "a");
                Assert.assertEquals("recovery must rebuild ALL K+M rows from a's durable WAL", K + M, post.size());
                for (int i = 0; i < K + M; i++) {
                    Assert.assertEquals("'a' row " + i, Long.valueOf(i), post.get(i));
                }
                Assert.assertTrue(
                        "adaptive 'a' recovery roll-forward must have fired (recoveryIncarnation>0)",
                        engine.getTableSequencerAPI().getTxnTracker(ttA).getRecoveryIncarnation() > 0
                );
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, 1000);
        }
    }

    private boolean snapshotExists(TableToken tt) {
        try (Path p = new Path()) {
            p.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(TableUtils.SNAPSHOT_FILE_NAME);
            return engine.getConfiguration().getFilesFacade().exists(p.$());
        }
    }

    /** Strict read: all rows of {@code table} in ts order, throwing on any error. */
    private List<Long> readVs(CairoEngine eng, String table) {
        final List<Long> out = new ArrayList<>();
        try (
                SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(eng);
                RecordCursorFactory f = eng.select("select v from " + table + " order by ts", ctx)
        ) {
            try (RecordCursor c = f.getCursor(ctx)) {
                Record r = c.getRecord();
                while (c.hasNext()) {
                    out.add(r.getLong(0));
                }
            }
        } catch (io.questdb.griffin.SqlException e) {
            throw new RuntimeException(e);
        }
        return out;
    }
}
