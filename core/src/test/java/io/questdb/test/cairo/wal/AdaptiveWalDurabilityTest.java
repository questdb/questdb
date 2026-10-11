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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.DurableEpochManifest;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxnScoreboard;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.std.Files;
import io.questdb.std.LongList;
import io.questdb.std.Os;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.wal.GroupCommitTestUtils.WalFdatasyncFacade;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.questdb.test.cairo.wal.GroupCommitTestUtils.commitRow;

/**
 * WAL commit and table apply durability per commit mode.
 *
 * <p>Under ADAPTIVE, every WAL commit issues {@code fdatasync} on the segment column data, then on the
 * events file ({@code _event}), then on the sequencer part ({@code _txn_parts/...}) and header
 * ({@code _txnlog}), before the commit returns. Apply ({@code ApplyWal2TableJob}) issues no {@code msync}
 * or {@code fdatasync} on table partition column files: the table is a rebuildable cache of the durable WAL,
 * and the durable epoch flushes it. Under SYNC, apply flushes the column files. Under NOSYNC, nothing
 * issues {@code fdatasync}.
 */
public class AdaptiveWalDurabilityTest extends AbstractCairoTest {
    private static final long WINDOW_US = 1_000_000L;

    /**
     * ADAPTIVE: a WAL commit fdatasyncs segment column data before the events file, and the events file
     * before the sequencer. A warmup insert pre-allocates the file pages, so the checked commit logs only
     * the per-commit durability fdatasyncs. The default window keeps the writeback drain
     * ({@code cairo.wal.commit.writeback.drain}) active, so the test also shows the drain keeps this order.
     */
    @Test
    public void testAdaptiveFdatasyncOrderDataBeforeEventsBeforeSequencer() throws Exception {
        // Use a small sequencer part size so the _txn_parts file is exercised.
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 16);
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");

        final FdatasyncOrderFacade trackFf = new FdatasyncOrderFacade();
        assertMemoryLeak(trackFf, () -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");

            // Warmup insert: allocates all file pages (segment columns, event file, sequencer).
            // The extension fdatasyncs from this insert are intentionally discarded.
            execute("insert into x values ('2024-01-01T00:00:00.000000Z', 1)");
            trackFf.resetFdatasyncOrder(); // discard all fdatasyncs from warmup + page allocation

            // Checked insert: the pages exist, so only the per-commit durability fdatasyncs appear in the log.
            execute("insert into x values ('2024-01-01T00:01:00.000000Z', 42)");

            // The commit should have issued fdatasync on column data, events, and sequencer.
            List<String> order = trackFf.getFdatasyncOrder();

            // Assert all three categories are present.
            boolean hasColumnData = order.stream().anyMatch(p ->
                    isWalColumnDataFile(p));
            boolean hasEvents = order.stream().anyMatch(p ->
                    p.endsWith(WalUtils.EVENT_FILE_NAME) || p.endsWith(WalUtils.EVENT_FILE_NAME + "."));
            boolean hasSeqPart = order.stream().anyMatch(p ->
                    p.contains(WalUtils.TXNLOG_PARTS_DIR));
            boolean hasSeqHeader = order.stream().anyMatch(p ->
                    p.endsWith(WalUtils.TXNLOG_FILE_NAME) || p.endsWith(WalUtils.TXNLOG_FILE_NAME + "."));

            if (!hasColumnData || !hasEvents || !hasSeqPart || !hasSeqHeader) {
                StringBuilder sb = buildMissingReport(order, hasColumnData, hasEvents, hasSeqPart, hasSeqHeader);
                Assert.fail(sb.toString());
            }

            // Assert ordering: first column-data fdatasync before first events fdatasync
            // before first sequencer (part or header) fdatasync.
            int firstColumnIdx = -1;
            int firstEventsIdx = -1;
            int firstSeqIdx = -1;
            for (int i = 0; i < order.size(); i++) {
                String p = order.get(i);
                if (firstColumnIdx < 0 && isWalColumnDataFile(p)) {
                    firstColumnIdx = i;
                }
                if (firstEventsIdx < 0 && (p.endsWith(WalUtils.EVENT_FILE_NAME)
                        || p.endsWith(WalUtils.EVENT_FILE_NAME + "."))) {
                    firstEventsIdx = i;
                }
                if (firstSeqIdx < 0 && (p.contains(WalUtils.TXNLOG_PARTS_DIR)
                        || p.endsWith(WalUtils.TXNLOG_FILE_NAME)
                        || p.endsWith(WalUtils.TXNLOG_FILE_NAME + "."))) {
                    firstSeqIdx = i;
                }
            }

            Assert.assertTrue(
                    "ADAPTIVE: column data fdatasync (" + firstColumnIdx + ") must come before events ("
                            + firstEventsIdx + "). Order: " + order,
                    firstColumnIdx < firstEventsIdx
            );
            Assert.assertTrue(
                    "ADAPTIVE: events fdatasync (" + firstEventsIdx + ") must come before sequencer ("
                            + firstSeqIdx + "). Order: " + order,
                    firstEventsIdx < firstSeqIdx
            );
        });
    }

    /**
     * NOSYNC: a WAL commit issues no fdatasync on the segment, events or sequencer files, including on the
     * V2 sequencer path ({@code TableTransactionLogV2.sync0}).
     */
    @Test
    public void testNosyncIssuesZeroFdatasync() throws Exception {
        // The test suite defaults to ADAPTIVE, so pin NOSYNC.
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        // Use a small part size so the V2 sequencer code path (TableTransactionLogV2.sync0) is
        // exercised during this insert. Without this the sequencer defaults to V1.
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 16);

        final FdatasyncOrderFacade trackFf = new FdatasyncOrderFacade();
        assertMemoryLeak(trackFf, () -> {
            execute("create table y (ts timestamp, v long) timestamp(ts) partition by day wal");

            trackFf.resetFdatasyncOrder();
            execute("insert into y values ('2024-01-02T00:00:00.000000Z', 99)");

            List<String> order = trackFf.getFdatasyncOrder();
            // Filter to only WAL-related paths (segment column data, _event, _txnlog, _txn_parts).
            long walFdatasyncs = order.stream().filter(p ->
                    isWalColumnDataFile(p)
                            || p.endsWith(WalUtils.EVENT_FILE_NAME)
                            || p.endsWith(WalUtils.EVENT_FILE_NAME + ".")
                            || p.contains(WalUtils.TXNLOG_PARTS_DIR)
                            || p.endsWith(WalUtils.TXNLOG_FILE_NAME)
                            || p.endsWith(WalUtils.TXNLOG_FILE_NAME + ".")
            ).count();
            Assert.assertEquals(
                    "NOSYNC must issue zero fdatasync on WAL commit paths (V1 + V2 sequencer), but got: " + order,
                    0, walFdatasyncs
            );
        });
    }

    /**
     * ADAPTIVE round trip: rows inserted and applied by {@code drainWalQueue} read back intact. The test runs
     * on the default FilesFacade, since only data correctness matters here.
     */
    @Test
    public void testAdaptiveRoundTrip() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 16);
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");

        assertMemoryLeak(() -> {
            execute("create table z (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into z values ('2024-03-01T00:00:00.000000Z', 7)");
            execute("insert into z values ('2024-03-01T01:00:00.000000Z', 13)");
            drainWalQueue();
            assertQuery("select * from z order by ts")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tv\n" +
                            "2024-03-01T00:00:00.000000Z\t7\n" +
                            "2024-03-01T01:00:00.000000Z\t13\n");
        });
    }

    /**
     * ADAPTIVE: a commit on a WAL table with a dropped column succeeds and the surviving data round-trips.
     * The WAL writer stores a dropped column's slot as {@code NullMemory.INSTANCE}, whose {@code getFd()}
     * throws {@link UnsupportedOperationException}, so {@code WalWriter.syncIfRequired()} must skip it.
     */
    @Test
    public void testAdaptiveDropColumnNoNullMemoryCrash() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 16);
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");

        assertMemoryLeak(() -> {
            // Create a WAL table with 3 columns (+ designated timestamp).
            execute("create table dc_test (ts timestamp, a long, b long, c long)" +
                    " timestamp(ts) partition by day wal");
            execute("insert into dc_test values ('2024-05-01T00:00:00.000000Z', 1, 2, 3)");
            drainWalQueue();

            // Drop column 'b': its slot becomes NullMemory.INSTANCE inside WalWriter.
            execute("alter table dc_test drop column b");
            drainWalQueue();

            // Force the WalWriter to be closed and reopened so it is initialised fresh
            // with NullMemory in the dropped slot (mirrors a server restart scenario).
            engine.releaseInactive();
            engine.releaseInactiveTableSequencers();

            // Insert after drop: WalWriter.syncIfRequired() must not call NullMemory.getFd().
            execute("insert into dc_test (ts, a, c) values ('2024-05-01T01:00:00.000000Z', 10, 30)");
            drainWalQueue();

            // Data round-trip: only surviving columns (ts, a, c) should be present.
            assertQuery("select * from dc_test order by ts")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\ta\tc\n" +
                            "2024-05-01T00:00:00.000000Z\t1\t3\n" +
                            "2024-05-01T01:00:00.000000Z\t10\t30\n");
        });
    }

    /**
     * ADAPTIVE: an in-order apply issues no msync or fdatasync on table partition column files.
     *
     * <p>The tracking facade maps mmap addresses to file paths at open time and records every
     * {@code msync} and {@code fdatasync}. The test resets the counters after the WAL insert, so the
     * WAL-commit syncs are excluded, and counts only the syncs issued by {@code drainWalQueue}.
     */
    @Test
    public void testAdaptiveApplyIssuezZeroColumnSyncsOnApply() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 16);
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        // Disable the durable epoch: it flushes the columns from inside the apply worker, and this test
        // covers the apply's own sync behaviour only.
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);

        final TableSyncTrackingFacade trackFf = new TableSyncTrackingFacade();
        assertMemoryLeak(trackFf, () -> {
            execute("create table tab_e (ts timestamp, v long) timestamp(ts) partition by day wal");

            // Insert: the WAL commit fdatasyncs the segment, events and sequencer files.
            execute("insert into tab_e values ('2024-06-01T00:00:00.000000Z', 42)");

            // Reset counters after the insert so WAL-commit syncs are excluded.
            // Only apply-side syncs (from drainWalQueue) are counted below.
            trackFf.resetTableColumnSyncs();

            // Apply: ApplyWal2TableJob materialises the WAL txn into the table partition.
            // Under ADAPTIVE, syncColumns() behaves like NOSYNC here: no msync or fdatasync.
            drainWalQueue();

            long tableColumnSyncs = trackFf.getTableColumnSyncCount();
            Assert.assertEquals(
                    "ADAPTIVE apply must issue ZERO msync/fdatasync on table partition column files, but got: "
                            + tableColumnSyncs + "; synced paths: " + trackFf.getTableColumnSyncPaths(),
                    0, tableColumnSyncs
            );

            // Correctness: data must be visible after the lazy apply.
            assertQuery("select * from tab_e order by ts")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tv\n" +
                            "2024-06-01T00:00:00.000000Z\t42\n");
        });
    }

    /**
     * ADAPTIVE: an out-of-order apply issues no msync or fdatasync on table partition column files.
     *
     * <p>{@link #testAdaptiveApplyIssuezZeroColumnSyncsOnApply} covers only the in-order fast path
     * ({@code applyFromWalLagToLastPartition}). An out-of-order apply merges through
     * {@code processO3Block} -> {@code O3CopyJob}, which syncs the destination columns on its own, gated on
     * {@code appliesColumnSync}. The test applies one row, then inserts an earlier timestamp into the same
     * materialized partition, so the second apply must take the O3 merge path.
     */
    @Test
    public void testAdaptiveO3ApplyIssuesZeroColumnSyncsOnApply() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 16);
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1); // isolate apply

        final TableSyncTrackingFacade trackFf = new TableSyncTrackingFacade();
        assertMemoryLeak(trackFf, () -> {
            execute("create table tab_e2 (ts timestamp, v long) timestamp(ts) partition by day wal");

            // First, an in-order row, fully applied so the partition is materialized on disk.
            execute("insert into tab_e2 values ('2024-06-01T05:00:00.000000Z', 1)");
            drainWalQueue();

            // Reset after the first apply: only the O3 merge apply below is counted.
            trackFf.resetTableColumnSyncs();

            // An earlier timestamp into the same partition: the next apply merges out-of-order
            // through processO3Block -> O3CopyJob, not the in-order fast path.
            execute("insert into tab_e2 values ('2024-06-01T01:00:00.000000Z', 2)");
            drainWalQueue();

            long tableColumnSyncs = trackFf.getTableColumnSyncCount();
            Assert.assertEquals(
                    "ADAPTIVE O3 apply must issue ZERO msync/fdatasync on table partition column files, but got: "
                            + tableColumnSyncs + "; synced paths: " + trackFf.getTableColumnSyncPaths(),
                    0, tableColumnSyncs
            );

            // Correctness: both rows visible, in timestamp order, after the lazy O3 merge.
            assertQuery("select * from tab_e2 order by ts")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tv\n" +
                            "2024-06-01T01:00:00.000000Z\t2\n" +
                            "2024-06-01T05:00:00.000000Z\t1\n");
        });
    }

    /**
     * SYNC: apply flushes table partition column files. Paired with
     * {@link #testAdaptiveApplyIssuezZeroColumnSyncsOnApply}, it shows that the tracking facade tells
     * table column syncs from WAL syncs, so the zero count under ADAPTIVE is not vacuous.
     */
    @Test
    public void testSyncApplyFlushesTableColumnFiles() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 16);
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");

        final TableSyncTrackingFacade trackFf = new TableSyncTrackingFacade();
        assertMemoryLeak(trackFf, () -> {
            execute("create table tab_f (ts timestamp, v long) timestamp(ts) partition by day wal");

            execute("insert into tab_f values ('2024-06-02T00:00:00.000000Z', 99)");

            // Reset after the insert and before apply, as in testAdaptiveApplyIssuezZeroColumnSyncsOnApply.
            trackFf.resetTableColumnSyncs();

            drainWalQueue();

            long tableColumnSyncs = trackFf.getTableColumnSyncCount();
            Assert.assertTrue(
                    "SYNC apply must issue at least one msync/fdatasync on table partition column files, but got 0",
                    tableColumnSyncs > 0
            );

            assertQuery("select * from tab_f order by ts")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tv\n" +
                            "2024-06-02T00:00:00.000000Z\t99\n");
        });
    }

    /**
     * ADAPTIVE: apply without a column flush leaves correct materialized state; reads after
     * {@code drainWalQueue} return every inserted row in order.
     */
    @Test
    public void testAdaptiveLazyApplyCorrectness() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 16);
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");

        assertMemoryLeak(() -> {
            execute("create table tab_g (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into tab_g values ('2024-07-01T00:00:00.000000Z', 1)");
            execute("insert into tab_g values ('2024-07-01T01:00:00.000000Z', 2)");
            execute("insert into tab_g values ('2024-07-01T02:00:00.000000Z', 3)");
            drainWalQueue();

            assertQuery("select * from tab_g order by ts")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tv\n" +
                            "2024-07-01T00:00:00.000000Z\t1\n" +
                            "2024-07-01T01:00:00.000000Z\t2\n" +
                            "2024-07-01T02:00:00.000000Z\t3\n");
        });
    }

    /**
     * ADAPTIVE: {@code fsyncMaterializedState()} flushes the table partition column files whatever the
     * commit mode, although apply skips the column sync, and writes a save point
     * ({@code _epoch.manifest.0/1}) holding copies of {@code _txn} and {@code _cv}. The test resets the
     * sync counters after apply, so only the syncs of {@code fsyncMaterializedState()} are counted.
     */
    @Test
    public void testFsyncMaterializedStateForcesFlushAndWritesEpochCopies() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 16);
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        // Disable the automatic epoch, so the explicit fsyncMaterializedState() call below is the only
        // durable cut. testApplyWorkerFiresDurableEpoch covers the epoch fired by the apply worker.
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);

        final TableSyncTrackingFacade trackFf = new TableSyncTrackingFacade();
        assertMemoryLeak(trackFf, () -> {
            execute("create table tab_h (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into tab_h values ('2024-08-01T00:00:00.000000Z', 1)");
            execute("insert into tab_h values ('2024-08-01T01:00:00.000000Z', 2)");
            drainWalQueue(); // lazy apply: no column syncs

            io.questdb.cairo.TableToken tt = engine.verifyTableName("tab_h");

            // Reset after the lazy apply: only the fsyncMaterializedState() syncs below are counted.
            trackFf.resetTableColumnSyncs();

            try (io.questdb.cairo.TableWriter writer = getWriter(tt)) {
                writer.fsyncMaterializedState();
            }

            long tableColumnSyncs = trackFf.getTableColumnSyncCount();
            Assert.assertTrue(
                    "fsyncMaterializedState() must flush table partition column files even under ADAPTIVE, but got 0",
                    tableColumnSyncs > 0
            );

            // The save point must hold copies of _txn and _cv.
            assertEpochCopyExists(tt, io.questdb.cairo.TableUtils.TXN_FILE_NAME);
            assertEpochCopyExists(tt, io.questdb.cairo.TableUtils.COLUMN_VERSION_FILE_NAME);

            // Data still correct after the durable cut.
            assertQuery("select * from tab_h order by ts")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tv\n" +
                            "2024-08-01T00:00:00.000000Z\t1\n" +
                            "2024-08-01T01:00:00.000000Z\t2\n");
        });
    }

    /**
     * ADAPTIVE: the apply worker fires a durable epoch on its own. With an epoch interval of 0 (every
     * batch), after {@code drainWalQueue} the save point holds copies of {@code _txn} and {@code _cv} at
     * the applied seqTxn, the tracker publishes that seqTxn as {@code durableEpochSeqTxn}, and the
     * scoreboard pins the epoch txn, so partition purge cannot reclaim it.
     */
    @Test
    public void testApplyWorkerFiresDurableEpoch() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 16);
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        // interval 0 => epoch fires on every apply batch (no cadence wait), deterministic for the test.
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);

        assertMemoryLeak(() -> {
            execute("create table tab_i (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into tab_i values ('2024-09-01T00:00:00.000000Z', 1)");
            execute("insert into tab_i values ('2024-09-01T01:00:00.000000Z', 2)");
            drainWalQueue();

            io.questdb.cairo.TableToken tt = engine.verifyTableName("tab_i");

            // The save point and its copies must all exist after the worker epoch fired.
            assertEpochCopyExists(tt, io.questdb.cairo.TableUtils.TXN_FILE_NAME);
            assertEpochCopyExists(tt, io.questdb.cairo.TableUtils.COLUMN_VERSION_FILE_NAME);

            // The current save point's epochSeqTxn must equal the applied seqTxn (2 inserts => seqTxn 2).
            Assert.assertEquals("epochSeqTxn == applied seqTxn", 2L, currentSavePoint(tt).epochSeqTxn);

            // The tracker published durableEpochSeqTxn = the applied seqTxn.
            io.questdb.cairo.wal.seq.SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);
            Assert.assertEquals("durableEpochSeqTxn published", 2L, tracker.getDurableEpochSeqTxn());
            Assert.assertTrue("an epoch txn is pinned", tracker.getPinnedEpochTxn() >= 0);

            // The pinned epoch txn must be held in the scoreboard: its version range is unavailable.
            long pinnedTxn = tracker.getPinnedEpochTxn();
            try (io.questdb.cairo.TxnScoreboard sb = engine.getTxnScoreboard(tt)) {
                Assert.assertFalse("pinned epoch txn must be held in the scoreboard",
                        sb.isRangeAvailable(pinnedTxn, pinnedTxn + 1));
            }

            // Data correct after the worker epoch.
            assertQuery("select * from tab_i order by ts")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tv\n" +
                            "2024-09-01T00:00:00.000000Z\t1\n" +
                            "2024-09-01T01:00:00.000000Z\t2\n");
        });
    }

    @Test
    public void testEpochCadenceTreatsBackwardClockStepAsElapsed() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 16);
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "0");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 60_000);
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, 0);

        assertMemoryLeak(() -> {
            setCurrentMicros(120_000_000L);
            execute("create table epoch_clock (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into epoch_clock values ('2024-09-01T00:00:00.000000Z', 1)");
            drainWalQueue();

            final io.questdb.cairo.TableToken tt = engine.verifyTableName("epoch_clock");
            final io.questdb.cairo.wal.seq.SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);
            final long firstEpoch = tracker.getDurableEpochSeqTxn();
            Assert.assertEquals(1L, firstEpoch);

            // Simulate an NTP/admin correction backwards. The next apply must epoch immediately rather than
            // waiting for wall time to climb back past the previous timestamp.
            setCurrentMicros(60_000_000L);
            execute("insert into epoch_clock values ('2024-09-01T01:00:00.000000Z', 2)");
            drainWalQueue();
            Assert.assertEquals(2L, tracker.getDurableEpochSeqTxn());
        });
    }

    @Test
    public void testAdaptiveEpochColumnSyncBatchedConfigRename() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_COLUMN_SYNC_BATCHED, false);
        assertMemoryLeak(() -> Assert.assertFalse(engine.getConfiguration().isAdaptiveEpochColumnSyncBatched()));
    }

    /**
     * SYNC: a WAL commit and its apply issue no fdatasync when a mapped column file extends; for
     * non-ADAPTIVE modes the memory primitive's {@code sync()} issues a full-range {@code msync} only. The
     * WAL segment and table column page sizes shrink to one OS page, and the checked insert writes 1000 rows
     * in one commit, so the commit extends the mapped files. {@link #testSyncModeNoSyncfs} covers
     * {@code syncfs} on apply.
     */
    @Test
    public void testSyncModeNoFdatasyncOnExtend() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        // Shrink column page sizes to one OS page, so the 1000-row insert below extends the mapped files
        // instead of fitting inside the default 1MB/16MB initial allocation.
        node1.setProperty(PropertyKey.CAIRO_WAL_WRITER_DATA_APPEND_PAGE_SIZE, String.valueOf(Files.PAGE_SIZE));
        node1.setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, String.valueOf(Files.PAGE_SIZE));
        final SyscallCountingFacade ff = new SyscallCountingFacade();
        assertMemoryLeak(ff, () -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into x values ('2024-01-01T00:00:00.000000Z', 1)"); // warmup: allocate first page
            drainWalQueue();
            ff.reset();
            // 1000 rows (16 bytes/row) cross the 4096-byte page in one commit, which extends the mapping
            // of both the WAL segment and the applied table partition column files.
            execute("insert into x select timestamp_sequence('2024-01-01T00:01:00.000000Z', 1_000_000L), x from long_sequence(1000)");
            drainWalQueue();
            Assert.assertEquals("SYNC must issue 0 fdatasync (S2 reverted)", 0, ff.fdatasyncCount);
            Assert.assertTrue("SYNC must still msync(MS_SYNC) on the WAL commit", ff.msyncSync > 0);
        });
    }

    /**
     * ADAPTIVE: a WAL commit fdatasyncs the column files. The fdatasync comes from the explicit
     * {@code ff.fdatasync(column.getFd())} loop in {@code WalWriter.syncIfRequired()}, which does not
     * depend on the memory primitive's {@code sync()}.
     */
    @Test
    public void testAdaptiveStillFsyncsAfterLegacyRevert() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0); // epoch every batch
        final SyscallCountingFacade ff = new SyscallCountingFacade();
        assertMemoryLeak(ff, () -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into x values ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            ff.reset();
            execute("insert into x values ('2024-01-01T00:01:00.000000Z', 2)");
            drainWalQueue(); // WAL commit + apply + epoch
            Assert.assertTrue("adaptive WAL commit must still fdatasync columns", ff.fdatasyncCount > 0);
        });
    }

    /**
     * ASYNC: a WAL commit and its apply issue {@code msync(MS_ASYNC)}, and no {@code fdatasync} or
     * {@code syncfs}.
     */
    @Test
    public void testAsyncModeMsyncAsyncOnly() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "async");
        final SyscallCountingFacade ff = new SyscallCountingFacade();
        assertMemoryLeak(ff, () -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into x values ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            ff.reset();
            execute("insert into x values ('2024-01-01T00:01:00.000000Z', 2)");
            drainWalQueue();
            Assert.assertTrue("ASYNC must issue msync(MS_ASYNC)", ff.msyncAsync > 0);
            Assert.assertEquals("ASYNC must not issue fdatasync", 0, ff.fdatasyncCount);
            Assert.assertEquals("ASYNC must not issue syncfs", 0, ff.syncfsCount);
        });
    }

    /**
     * NOSYNC: a WAL commit and its apply issue no device flush: no {@code fdatasync} and no {@code syncfs}.
     */
    @Test
    public void testNosyncModeNoSyncs() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        final SyscallCountingFacade ff = new SyscallCountingFacade();
        assertMemoryLeak(ff, () -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into x values ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            ff.reset();
            execute("insert into x values ('2024-01-01T00:01:00.000000Z', 2)");
            drainWalQueue();
            Assert.assertEquals("NOSYNC must not issue fdatasync", 0, ff.fdatasyncCount);
            Assert.assertEquals("NOSYNC must not issue syncfs", 0, ff.syncfsCount);
        });
    }

    /**
     * ADAPTIVE: when the scoreboard slot for the next epoch pin is busy, {@code advanceDurableEpoch()}
     * leaves the previous save point authoritative.
     */
    @Test
    public void testEpochMarkerDoesNotAdvanceWhenNextPinSlotIsBusy() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        assertMemoryLeak(() -> {
            execute("create table pin_busy (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into pin_busy values ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            final TableToken token = engine.verifyTableName("pin_busy");
            final long occupiedTxn = 1_000_000L;
            try (TxnScoreboard scoreboard = engine.getTxnScoreboard(token);
                 TableWriter writer = getWriter(token)) {
                writer.advanceDurableEpoch(100L);
                final int generationBefore = readMarkerGeneration(token);
                Assert.assertTrue(scoreboard.incrementTxn(TxnScoreboard.EPOCH_ID_B, occupiedTxn));
                try {
                    writer.advanceDurableEpoch(200L);
                    Assert.assertEquals("a busy pin slot must leave the prior marker authoritative",
                            generationBefore, readMarkerGeneration(token));
                } finally {
                    scoreboard.releaseTxn(TxnScoreboard.EPOCH_ID_B, occupiedTxn);
                }
            }
        });
    }

    /**
     * SYNC: apply issues no {@code syncfs}. {@code syncColumns()} syncs each column file through
     * {@code syncColumns0()}, not through the batched {@code syncColumnsBatchedSync()}.
     */
    @Test
    public void testSyncModeNoSyncfs() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        final SyscallCountingFacade ff = new SyscallCountingFacade();
        assertMemoryLeak(ff, () -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into x values ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            ff.reset();
            execute("insert into x values ('2024-01-01T00:01:00.000000Z', 2)");
            drainWalQueue();
            Assert.assertEquals("SYNC apply must not syncfs (S1 reverted)", 0, ff.syncfsCount);
        });
    }

    /**
     * ADAPTIVE: the durable epoch ({@code fsyncMaterializedState()}) issues {@code syncfs}. The batched
     * path ({@code syncColumnsBatchedSync()}, on Linux with {@code isAdaptiveEpochColumnSyncBatched()})
     * and the non-batched path ({@code fsyncMaterializedStateSyncFs()}) both flush the whole filesystem,
     * so the assertion holds on either; at the default configuration on Linux the batched path runs.
     */
    @Test
    public void testAdaptiveEpochStillSyncfs() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
        final SyscallCountingFacade ff = new SyscallCountingFacade();
        assertMemoryLeak(ff, () -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into x values ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            ff.reset();
            execute("insert into x values ('2024-01-01T00:01:00.000000Z', 2)");
            drainWalQueue();
            Assert.assertTrue("adaptive epoch must still syncfs the materialized state", ff.syncfsCount > 0);
        });
    }

    /**
     * The sync calls a 1-row WAL commit makes on each WAL segment file (the column files and the two
     * {@code _event} files), per commit mode and group window. ADAPTIVE drains writeback at every window,
     * W=0 included: each file gets {@code msync(MS_ASYNC)}, two {@code sync_file_range} calls (start the
     * writeback, then wait for it) and its own barrier, and never a blocking {@code msync(MS_SYNC)}. NOSYNC,
     * ASYNC and SYNC run neither the drain nor a barrier. The filesystem probe is pinned on, so the drain runs
     * on every platform.
     */
    @Test
    public void testWalCommitSegmentFileSyncsPerMode() throws Exception {
        final String perFile = "_event: %1$s; _event.i: %1$s; ts.d: %1$s; v.d: %1$s";
        final String drained = String.format(perFile, "barrierFsync=1 msyncAsync=1 syncFileRange=2");
        // commit mode, group window in microseconds, the sync calls of one commit on the segment files.
        final String[][] cases = {
                {"adaptive", "50000", drained},
                {"nosync", "0", ""},
                {"async", "0", String.format(perFile, "msyncAsync=1")},
                {"sync", "0", String.format(perFile, "msyncSync=1")},
                {"adaptive", "0", drained}
        };
        final WritebackDrainFacade ff = new WritebackDrainFacade();
        assertMemoryLeak(ff, () -> {
            for (int c = 0; c < cases.length; c++) {
                final String[] mode = cases[c];
                node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, mode[1]);
                node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, mode[0]);
                Assert.assertEquals(CommitMode.fromString(mode[0]), engine.getConfiguration().getCommitMode());
                Assert.assertEquals(Long.parseLong(mode[1]), engine.getConfiguration().getAdaptiveCommitGroupWindowUs());
                execute("CREATE TABLE x" + c + " (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (WalWriter writer = getWalWriter("x" + c)) {
                    for (int i = 0; i < 4; i++) {
                        final TableWriter.Row row = writer.newRow(i * 60_000_000L);
                        row.putLong(1, i);
                        row.append();
                        ff.segmentSyncs.clear();
                        writer.commit();
                        Assert.assertEquals(
                                "mode=" + mode[0] + ", window=" + mode[1] + "us, commit " + i,
                                mode[2],
                                ff.segmentSyncSummary()
                        );
                    }
                }
            }
        });
    }

    /**
     * The writeback drain's filesystem probe allocates a native path and costs a statfs, so
     * {@code WalWriter} resolves it once, on its own WAL directory, when it opens. A commit must not
     * repeat it, yet the drain itself must still run on every ADAPTIVE commit under {@code W > 0}.
     */
    @Test
    public void testWritebackDrainProbesFilesystemOncePerWriter() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, 50_000L);
        final WritebackDrainFacade ff = new WritebackDrainFacade();
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            ff.probeCount = 0;
            try (WalWriter writer = getWalWriter("x")) {
                Assert.assertEquals("opening the writer must probe the filesystem once", 1, ff.probeCount);
                Assert.assertTrue(ff.probedPath, ff.probedPath.endsWith(Files.SEPARATOR + writer.getWalName()));
                for (int i = 0; i < 3; i++) {
                    final TableWriter.Row row = writer.newRow(i * 60_000_000L);
                    row.putLong(1, i);
                    row.append();
                    writer.commit();
                }
                Assert.assertEquals("a commit must not probe the filesystem", 1, ff.probeCount);
                Assert.assertTrue("the commits must still drain writeback", ff.syncFileRangeCount > 0);
            }
        });
    }

    @Test
    public void testEpochCannotPublishAheadOfDeferredSequencerOnNonWideSyncfsPlatform() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 16);
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, String.valueOf(WINDOW_US));
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, "-1");

        final NonWideSyncfsWalFdatasyncFacade ff = new NonWideSyncfsWalFdatasyncFacade();
        assertMemoryLeak(ff, () -> {
            setCurrentMicros(1_000_000L);
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            final TableToken tt = engine.verifyTableName("x");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);

            try (WalWriter walWriter = engine.getWalWriter(tt)) {
                commitRow(walWriter, 0L, 1L);
                drainWalQueue();
                final long appliedSeqTxn = tracker.getWriterTxn();
                Assert.assertTrue("the W>0 sequencer barrier must still be pending",
                        tracker.getLocalDurableSeqTxn() < appliedSeqTxn);

                ff.reset();
                try (TableWriter tableWriter = getWriter(tt)) {
                    tableWriter.advanceDurableEpoch(1L);
                }

                Assert.assertTrue("a non-wide epoch must fdatasync the sequencer before publishing",
                        ff.sequencerFdatasyncs() > 0);
                Assert.assertTrue("the epoch cut must be inside the local durable WAL prefix",
                        tracker.getLocalDurableSeqTxn() >= appliedSeqTxn);
                Assert.assertEquals(appliedSeqTxn, tracker.getDurableEpochSeqTxn());
            }
        });
    }

    /**
     * On a platform whose syncfs is not filesystem-wide, the epoch flush scales with the write set since the
     * previous epoch, not with table history: the previous epoch already made everything older durable.
     * <p>
     * The first epoch of a writer's life walks every partition, since it must cover partitions a previous
     * writer instance left un-epoched. A later epoch that follows a write to one partition opens files under
     * that partition only.
     */
    @Test
    public void testEpochFlushesOnlyTheWriteSetOnNonWideSyncfsPlatform() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, "-1");

        final EpochOpenTrackingFacade ff = new EpochOpenTrackingFacade();
        assertMemoryLeak(ff, () -> {
            setCurrentMicros(1_000_000L);
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            final TableToken tt = engine.verifyTableName("x");

            // Three days of history.
            try (WalWriter walWriter = engine.getWalWriter(tt)) {
                commitRow(walWriter, 0L, 1L);
                commitRow(walWriter, Micros.DAY_MICROS, 2L);
                commitRow(walWriter, 2 * Micros.DAY_MICROS, 3L);
            }
            drainWalQueue();

            try (TableWriter tableWriter = getWriter(tt)) {
                Assert.assertEquals(3, tableWriter.getTxWriter().getPartitionCount());

                // First epoch of this writer's life: the write set cannot be vouched for, so everything is
                // walked. This is the fail-safe that covers a previous writer instance's un-epoched work.
                ff.reset();
                tableWriter.advanceDurableEpoch(1L);
                Assert.assertTrue(
                        "the first epoch after open must walk every partition, saw: " + ff.openedPartitionDirs(),
                        ff.openedPartitionDirs().contains("1970-01-01")
                                && ff.openedPartitionDirs().contains("1970-01-02")
                                && ff.openedPartitionDirs().contains("1970-01-03")
                );

                // Second epoch, with nothing written since the first: no partition owes a flush.
                ff.reset();
                tableWriter.advanceDurableEpoch(2L);
                Assert.assertTrue(
                        "an epoch with an empty write set must open no partition file, saw: " + ff.openedPartitionDirs(),
                        ff.openedPartitionDirs().isEmpty()
                );
            }

            // Write to the last partition only, then epoch again: history must not be re-walked.
            try (WalWriter walWriter = engine.getWalWriter(tt)) {
                commitRow(walWriter, 2 * Micros.DAY_MICROS + 1, 4L);
            }
            drainWalQueue();

            try (TableWriter tableWriter = getWriter(tt)) {
                ff.reset();
                tableWriter.advanceDurableEpoch(3L);
                Assert.assertTrue(
                        "the epoch must touch the written partition, saw: " + ff.openedPartitionDirs(),
                        ff.openedPartitionDirs().contains("1970-01-03")
                );
                Assert.assertFalse(
                        "the epoch must NOT re-walk untouched history, saw: " + ff.openedPartitionDirs(),
                        ff.openedPartitionDirs().contains("1970-01-01")
                );
                Assert.assertFalse(
                        "the epoch must NOT re-walk untouched history, saw: " + ff.openedPartitionDirs(),
                        ff.openedPartitionDirs().contains("1970-01-02")
                );
            }
        });
    }

    /**
     * A read-only (soft-linked) attached partition must not stall the epoch on a platform whose syncfs is
     * not filesystem-wide, where {@code fsyncAttachedPartitionFiles} is the epoch's column-flush path.
     * <p>
     * Such a partition is backed by storage this writer never writes, so it holds nothing an epoch needs to
     * flush, and {@code openRW} on its files fails. A failed open would reach
     * {@code handleBestEffortDurableEpochFailure}, which only logs: {@code durableEpochSeqTxn} would stay 0,
     * {@code WalPurgeJob} would hold the purge floor at 0, and the WAL would grow without bound. The test
     * therefore asserts the published epoch, not just the absence of an exception.
     */
    @Test
    public void testEpochSkipsReadOnlyPartitionsOnNonWideSyncfsPlatform() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, "-1");

        final ReadOnlyPartitionSyncfsFacade ff = new ReadOnlyPartitionSyncfsFacade();
        assertMemoryLeak(ff, () -> {
            setCurrentMicros(1_000_000L);
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            final TableToken tt = engine.verifyTableName("x");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);

            // Two partitions, so one can be flagged read-only while the other still carries the epoch.
            try (WalWriter walWriter = engine.getWalWriter(tt)) {
                commitRow(walWriter, 0L, 1L);
                commitRow(walWriter, Micros.DAY_MICROS, 2L);
            }
            drainWalQueue();

            final long appliedSeqTxn = tracker.getWriterTxn();
            try (TableWriter tableWriter = getWriter(tt)) {
                Assert.assertEquals(2, tableWriter.getTxWriter().getPartitionCount());
                // Flag the first partition read-only exactly as attachPartition does for a soft link, and
                // make its files unopenable for write exactly as the read-only mount behind one would.
                tableWriter.getTxWriter().setPartitionReadOnlyByTimestamp(0L, true);
                ff.denyWritesUnder("1970-01-01");

                tableWriter.advanceDurableEpoch(1L);
            }

            Assert.assertEquals(
                    "the epoch must publish despite the read-only partition",
                    appliedSeqTxn,
                    tracker.getDurableEpochSeqTxn()
            );
            Assert.assertEquals(
                    "no file under a read-only partition may be opened for write by the epoch",
                    0,
                    ff.deniedOpenAttempts()
            );
        });
    }

    /**
     * Both writeback drains, the ADAPTIVE WAL commit's (at window 0 and above) and the save point's batched
     * column flush, start the writeback of every file ({@code sync_file_range(WRITE)}) before they wait on any
     * ({@code WRITE | WAIT_AFTER}), so the writebacks overlap, and wait on every file they started.
     */
    @Test
    public void testWritebackDrainStartsEveryFileBeforeWaiting() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        final WritebackDrainFacade ff = new WritebackDrainFacade() {
            @Override
            public boolean isSyncfsFileSystemWide() {
                // Pinned, so the save point takes its batched flush on every platform.
                return true;
            }
        };
        assertMemoryLeak(ff, () -> {
            final List<String> failures = new ArrayList<>();
            int t = 0;
            for (long window : new long[]{0, 50_000}) {
                node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, window);
                Assert.assertEquals(window, engine.getConfiguration().getAdaptiveCommitGroupWindowUs());
                final String name = "x" + t++;
                execute("CREATE TABLE " + name + " (ts TIMESTAMP, v LONG, s SYMBOL, txt VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (WalWriter writer = getWalWriter(name)) {
                    appendRow(writer, 0);
                    ff.syncFileRangeCalls.clear();
                    writer.commit();
                    checkWritebackDrain("WAL commit, window=" + window + "us", ff.syncFileRangeCalls, failures);
                }
                drainWalQueue();
                try (TableWriter writer = getWriter(name)) {
                    ff.syncFileRangeCalls.clear();
                    writer.fsyncMaterializedState();
                    checkWritebackDrain("save point, window=" + window + "us", ff.syncFileRangeCalls, failures);
                }
            }
            Assert.assertTrue("drains that do not start every writeback before waiting:\n" + String.join("\n", failures), failures.isEmpty());
        });
    }

    /**
     * A writeback error on any WAL segment file fails the ADAPTIVE commit and keeps the durable frontier
     * where it was, at window 0 and above, with the writeback drain on and off. Linux reports a writeback
     * error once per open file, to the first call that waits on that file's writeback, and the facade does
     * the same: with the drain on, {@code sync_file_range} gets the error and the {@code fdatasync} after it
     * succeeds.
     */
    @Test
    public void testWritebackErrorFailsTheCommit() throws Exception {
        final String[] files = {"_event", "_event.i", "s.d", "ts.d", "txt.d", "txt.i", "v.d"};
        final WritebackDrainFacade ff = new WritebackDrainFacade();
        assertMemoryLeak(ff, () -> {
            final List<String> acknowledged = new ArrayList<>();
            int t = 0;
            for (long window : new long[]{0, 50_000}) {
                // The window differs on every pass and is set last, so the configuration is rebuilt.
                node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
                node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, window);
                Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());
                Assert.assertEquals(window, engine.getConfiguration().getAdaptiveCommitGroupWindowUs());
                for (boolean drain : new boolean[]{true, false}) {
                    ff.drain = drain;
                    for (String file : files) {
                        final String name = "x" + t++;
                        execute("CREATE TABLE " + name + " (ts TIMESTAMP, v LONG, s SYMBOL, txt VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL");
                        try (WalWriter writer = getWalWriter(name)) {
                            appendRow(writer, 0);
                            ff.segmentSyncs.clear();
                            writer.commit();
                            Assert.assertEquals("the files a commit barriers", Arrays.toString(files), ff.barrieredFiles());
                        }
                        // Closing the writer flushes the pending sequencer record at W > 0.
                        final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(engine.verifyTableName(name));
                        Assert.assertEquals(1, tracker.getLocalDurableSeqTxn());
                        final String row = "window=" + window + "us, drain " + (drain ? "on" : "off") + ", error on " + file;
                        boolean failed = false;
                        try (WalWriter writer = getWalWriter(name)) {
                            appendRow(writer, 1);
                            ff.failNextWait(file);
                            try {
                                writer.commit();
                            } catch (CairoError e) {
                                failed = CairoException.isDataSyncFailure(e);
                            }
                        } finally {
                            engine.resetDurabilityFailure();
                        }
                        Assert.assertNotNull(row + ": no call waited on the file", ff.writebackErrorCall);
                        final long durable = tracker.getLocalDurableSeqTxn();
                        if (!failed || durable != 1) {
                            acknowledged.add(row + ": the error went to " + ff.writebackErrorCall + ", the commit "
                                    + (failed ? "failed" : "returned") + ", durable frontier 1 -> " + durable);
                        }
                    }
                }
            }
            Assert.assertTrue("commits not failed by a writeback error:\n" + String.join("\n", acknowledged), acknowledged.isEmpty());
        });
    }

    private int readMarkerGeneration(TableToken token) {
        return currentSavePoint(token).generation;
    }

    private DurableEpochManifest.Candidate currentSavePoint(TableToken token) {
        try (io.questdb.std.str.Path p = new io.questdb.std.str.Path()) {
            p.of(engine.getConfiguration().getDbRoot()).concat(token);
            final DurableEpochManifest.Candidate[] saved = DurableEpochManifest.loadCandidates(engine.getConfiguration().getFilesFacade(), p, p.size(), token.getTableId());
            Assert.assertTrue("a save point must exist", saved.length > 0);
            return saved[0];
        }
    }

    private void assertEpochCopyExists(io.questdb.cairo.TableToken tt, String baseFileName) {
        try (io.questdb.std.str.Path p = new io.questdb.std.str.Path()) {
            final int generation = currentSavePoint(tt).generation;
            final io.questdb.std.FilesFacade ff = engine.getConfiguration().getFilesFacade();
            final long[] section = io.questdb.test.tools.TestUtils.savePointSection(ff, p, engine.getConfiguration().getDbRoot(), tt, generation, baseFileName);
            Assert.assertTrue("the save point must hold a copy of " + baseFileName + ": " + p, section[1] > 0);
        }
    }

    private static void appendRow(WalWriter writer, long v) {
        final TableWriter.Row row = writer.newRow(v * 60_000_000L);
        row.putLong(1, v);
        row.putSym(2, "s" + v);
        row.putVarchar(3, new Utf8String("a varchar longer than its inline prefix " + v));
        row.append();
    }

    // One drain's sync_file_range calls, as (fd, flags) pairs: every start before the first wait, every
    // waited file started once, every started file waited on.
    private static void checkWritebackDrain(String site, LongList calls, List<String> failures) {
        if (calls.size() == 0) {
            failures.add(site + ": no sync_file_range call");
            return;
        }
        final List<Long> started = new ArrayList<>();
        boolean waiting = false;
        for (int i = 0, n = calls.size(); i < n; i += 2) {
            final long fd = calls.getQuick(i);
            final long flags = calls.getQuick(i + 1);
            if (flags == Files.SYNC_FILE_RANGE_WRITE) {
                if (waiting) {
                    failures.add(site + ": fd " + fd + " started after the first wait");
                }
                started.add(fd);
            } else if (flags == (Files.SYNC_FILE_RANGE_WRITE | Files.SYNC_FILE_RANGE_WAIT_AFTER)) {
                waiting = true;
                if (!started.remove(fd)) {
                    failures.add(site + ": fd " + fd + " waited on without a start");
                }
            } else {
                failures.add(site + ": fd " + fd + " unexpected flags " + flags);
            }
        }
        if (!started.isEmpty()) {
            failures.add(site + ": started, never waited on " + started);
        }
    }

    /**
     * Returns true if the given path is a WAL segment column data file (.d file or fixed-size
     * column file in a wal/ segment directory), not an event or sequencer file.
     */
    private static boolean isWalColumnDataFile(String p) {
        if (p == null) {
            return false;
        }
        // WAL segment column files live inside wal<N>/<segmentId>/ and end with .d
        // (variable-length data: varchar, binary) or have no extension (fixed-size columns).
        // The path contains "/wal" and a segment integer directory, and is not an event or sequencer file.
        boolean inWalDir = p.contains("/wal") || p.contains("\\wal");
        boolean isEventFile = p.endsWith(WalUtils.EVENT_FILE_NAME)
                || p.endsWith(WalUtils.EVENT_FILE_NAME + ".")
                || p.endsWith(WalUtils.EVENT_INDEX_FILE_NAME)
                || p.endsWith(WalUtils.EVENT_INDEX_FILE_NAME + ".");
        boolean isSeqFile = p.contains(WalUtils.TXNLOG_PARTS_DIR)
                || p.endsWith(WalUtils.TXNLOG_FILE_NAME)
                || p.endsWith(WalUtils.TXNLOG_FILE_NAME + ".");
        boolean isMetaFile = p.endsWith("_meta") || p.endsWith("_meta.")
                || p.endsWith("_walmeta") || p.endsWith("_walmeta.");
        return inWalDir && !isEventFile && !isSeqFile && !isMetaFile;
    }

    private static StringBuilder buildMissingReport(
            List<String> order, boolean hasColumnData, boolean hasEvents,
            boolean hasSeqPart, boolean hasSeqHeader
    ) {
        StringBuilder sb = new StringBuilder("ADAPTIVE commit missing fdatasync on:\n");
        if (!hasColumnData) {
            sb.append("  - segment column data\n");
        }
        if (!hasEvents) {
            sb.append("  - WAL-e events file (_event)\n");
        }
        if (!hasSeqPart) {
            sb.append("  - sequencer part file (_txn_parts/…)\n");
        }
        if (!hasSeqHeader) {
            sb.append("  - sequencer header (_txnlog)\n");
        }
        sb.append("Recorded fdatasync paths:\n");
        for (int i = 0; i < order.size(); i++) {
            sb.append("  [").append(i).append("] ").append(order.get(i)).append('\n');
        }
        return sb;
    }

    /**
     * A FilesFacade that counts every {@code msync}, {@code fdatasync} and {@code sync_file_range} call on
     * table partition column files: not in {@code wal<N>/} segment dirs, not in {@code txn_seq/}, and not
     * control or metadata files.
     *
     * <p>It tracks fd -> path on open and addr -> fd on mmap, then resolves msync(addr) and
     * fdatasync(fd) to a path and classifies it. WAL segment and sequencer syncs are ignored, so a test
     * can reset after the WAL insert and count only the apply-side syncs.
     */
    static class TableSyncTrackingFacade extends TestFilesFacadeImpl {
        // fd -> file path (populated on every open call)
        private final Map<Long, String> fdToPath = new HashMap<>();
        // mmap address -> fd (populated on mmap, removed on munmap)
        private final Map<Long, Long> addrToFd = new HashMap<>();
        // paths of table-partition column files that were synced (msync, fdatasync or sync_file_range)
        private final List<String> tableColumnSyncPaths = new ArrayList<>();
        // total sync count on table partition column files
        private long tableColumnSyncCount = 0;

        public long getTableColumnSyncCount() {
            return tableColumnSyncCount;
        }

        public List<String> getTableColumnSyncPaths() {
            return new ArrayList<>(tableColumnSyncPaths);
        }

        public void resetTableColumnSyncs() {
            tableColumnSyncCount = 0;
            tableColumnSyncPaths.clear();
        }

        @Override
        public boolean close(long fd) {
            fdToPath.remove(fd);
            return super.close(fd);
        }

        @Override
        public void fdatasync(long fd) {
            super.fdatasync(fd);
            String p = fdToPath.get(fd);
            if (p != null && isTablePartitionColumnFile(p)) {
                tableColumnSyncCount++;
                tableColumnSyncPaths.add("fdatasync:" + p);
            }
        }

        @Override
        public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
            long addr = super.mmap(fd, len, offset, flags, memoryTag);
            if (addr != -1L && addr != 0L) {
                addrToFd.put(addr, fd);
            }
            return addr;
        }

        @Override
        public long mmapNoCache(long fd, long len, long offset, int flags, int memoryTag) {
            long addr = super.mmapNoCache(fd, len, offset, flags, memoryTag);
            if (addr != -1L && addr != 0L) {
                addrToFd.put(addr, fd);
            }
            return addr;
        }

        @Override
        public void msync(long addr, long len, boolean async) {
            super.msync(addr, len, async);
            Long fd = addrToFd.get(addr);
            if (fd != null) {
                String p = fdToPath.get(fd);
                if (p != null && isTablePartitionColumnFile(p)) {
                    tableColumnSyncCount++;
                    tableColumnSyncPaths.add("msync(" + (async ? "async" : "sync") + "):" + p);
                }
            }
        }

        @Override
        public void munmap(long address, long size, int memoryTag) {
            addrToFd.remove(address);
            super.munmap(address, size, memoryTag);
        }

        @Override
        public long openAppend(LPSZ name) {
            long fd = super.openAppend(name);
            trackFd(fd, name);
            return fd;
        }

        @Override
        public long openCleanRW(LPSZ name, long size) {
            long fd = super.openCleanRW(name, size);
            trackFd(fd, name);
            return fd;
        }

        @Override
        public long openRO(LPSZ name) {
            long fd = super.openRO(name);
            trackFd(fd, name);
            return fd;
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            long fd = super.openRW(name, opts);
            trackFd(fd, name);
            return fd;
        }

        @Override
        public int syncFileRange(long fd, long offset, long nbytes, int flags) {
            String p = fdToPath.get(fd);
            if (p != null && isTablePartitionColumnFile(p)) {
                tableColumnSyncCount++;
                tableColumnSyncPaths.add("syncFileRange(" + flags + "):" + p);
            }
            return super.syncFileRange(fd, offset, nbytes, flags);
        }

        private void trackFd(long fd, LPSZ name) {
            if (fd > -1) {
                fdToPath.put(fd, Utf8String.newInstance(name).toString());
            }
        }
    }

    /**
     * Returns true if the given path is a table partition column file, one that {@code syncColumns()}
     * flushes on each commit: not in a WAL segment directory ({@code /wal<N>/}), not in the sequencer
     * directory ({@code /txn_seq/}), and not a table-level control file ({@code _txn}, {@code _cv},
     * {@code _meta}, {@code _todo}).
     *
     * <p>Table partition column files live at paths like:
     * {@code <root>/<tableName>/2024-01-01/ts.d} or {@code <root>/<tableName>/2024-01-01/v.d}.
     */
    private static boolean isTablePartitionColumnFile(String p) {
        if (p == null) {
            return false;
        }
        // WAL segment files contain "/wal" in the path (e.g. .../wal1/0/ts.d)
        if (p.contains("/wal") || p.contains("\\wal")) {
            return false;
        }
        // Sequencer files contain "/txn_seq/" (or SEQ_DIR)
        if (p.contains(WalUtils.SEQ_DIR) || p.contains(WalUtils.SEQ_DIR_DEPRECATED)) {
            return false;
        }
        // Table-root control files (_txn, _cv, _meta, _todo, _lock, bitmap indexes _k and _v) live at
        // the table root, not in a partition subdirectory. Accept only partition column data files: the
        // last path component ends with .d (variable-length data or aux) or has no extension (a fixed-size
        // column such as "ts" or "v"), and is not a control file.
        String name = p;
        int sep = Math.max(p.lastIndexOf('/'), p.lastIndexOf('\\'));
        if (sep >= 0) {
            name = p.substring(sep + 1);
        }
        // Exclude known control/index file suffixes at all levels
        if (name.startsWith("_") || name.endsWith(".k") || name.endsWith(".v")
                || name.endsWith(".lock") || name.endsWith(".swp")) {
            return false;
        }
        return true;
    }

    /**
     * A FilesFacade that records every {@code fdatasync(fd)} call, resolving the fd to its
     * file path via open-time tracking. Inherits from {@link TestFilesFacadeImpl} so that the
     * standard open-file leak detection still applies.
     */
    static class FdatasyncOrderFacade extends TestFilesFacadeImpl {
        private final Map<Long, String> fdToPath = new HashMap<>();
        private final List<String> fdatasyncOrder = new ArrayList<>();

        public List<String> getFdatasyncOrder() {
            return fdatasyncOrder;
        }

        public void resetFdatasyncOrder() {
            fdatasyncOrder.clear();
        }

        @Override
        public boolean close(long fd) {
            fdToPath.remove(fd);
            return super.close(fd);
        }

        /**
         * WAL ordering barriers must be visible here, or every assertion counting them passes vacuously.
         * <p>
         * Off Darwin {@code FilesFacadeImpl.barrierFsync} deliberately routes through the overridable
         * {@code fdatasync}, so the override below has already recorded the call and recording again would
         * double-count. On Darwin it calls the static {@code Files.barrierFsync} directly -- which is the
         * whole point of the split, since only Darwin has a separate barrier syscall -- and a facade that
         * does not override this method observes no WAL barrier at all.
         */
        @Override
        public void barrierFsync(long fd) {
            super.barrierFsync(fd);
            if (Os.isOSX()) {
                String p = fdToPath.get(fd);
                if (p != null) {
                    fdatasyncOrder.add(p);
                }
            }
        }

        @Override
        public void fdatasync(long fd) {
            super.fdatasync(fd);
            String p = fdToPath.get(fd);
            if (p != null) {
                fdatasyncOrder.add(p);
            }
        }

        @Override
        public long openAppend(LPSZ name) {
            long fd = super.openAppend(name);
            trackFd(fd, name);
            return fd;
        }

        @Override
        public long openCleanRW(LPSZ name, long size) {
            long fd = super.openCleanRW(name, size);
            trackFd(fd, name);
            return fd;
        }

        @Override
        public long openRO(LPSZ name) {
            long fd = super.openRO(name);
            trackFd(fd, name);
            return fd;
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            long fd = super.openRW(name, opts);
            trackFd(fd, name);
            return fd;
        }

        private void trackFd(long fd, LPSZ name) {
            if (fd > -1) {
                fdToPath.put(fd, Utf8String.newInstance(name).toString());
            }
        }
    }

    /**
     * A FilesFacade that counts every {@code msync} (split sync/async), {@code fdatasync},
     * {@code fsync}, and {@code syncfs} call, with no path filtering, for the tests that need raw
     * syscall counts per commit mode rather than per-file attribution.
     *
     * <p>It also records in {@link #tableColumnMsyncLengths} the {@code len} argument of every non-async
     * {@code msync} on a table partition column file (per {@link #isTablePartitionColumnFile}, with the
     * fd/path/addr tracking of {@link TableSyncTrackingFacade}), so a test can tell a msync of the appended
     * bytes from a msync of the whole mapping without counting WAL segment or control-file msyncs.
     */
    static class SyscallCountingFacade extends TestFilesFacadeImpl {
        final LongList tableColumnMsyncLengths = new LongList();
        int fdatasyncCount, fsyncCount, msyncAsync, msyncSync, syncfsCount;
        private final Map<Long, Long> addrToFd = new HashMap<>();
        private final Map<Long, String> fdToPath = new HashMap<>();

        void reset() {
            msyncSync = msyncAsync = fdatasyncCount = fsyncCount = syncfsCount = 0;
            tableColumnMsyncLengths.clear();
        }

        @Override
        public boolean close(long fd) {
            fdToPath.remove(fd);
            return super.close(fd);
        }

        @Override
        public void fdatasync(long fd) {
            fdatasyncCount++;
            super.fdatasync(fd);
        }

        @Override
        public void fsync(long fd) {
            fsyncCount++;
            super.fsync(fd);
        }

        @Override
        public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
            long addr = super.mmap(fd, len, offset, flags, memoryTag);
            if (addr != -1L && addr != 0L) {
                addrToFd.put(addr, fd);
            }
            return addr;
        }

        @Override
        public long mmapNoCache(long fd, long len, long offset, int flags, int memoryTag) {
            long addr = super.mmapNoCache(fd, len, offset, flags, memoryTag);
            if (addr != -1L && addr != 0L) {
                addrToFd.put(addr, fd);
            }
            return addr;
        }

        @Override
        public void msync(long addr, long len, boolean async) {
            if (async) {
                msyncAsync++;
            } else {
                msyncSync++;
                Long fd = addrToFd.get(addr);
                if (fd != null) {
                    String p = fdToPath.get(fd);
                    if (p != null && isTablePartitionColumnFile(p)) {
                        tableColumnMsyncLengths.add(len);
                    }
                }
            }
            super.msync(addr, len, async);
        }

        @Override
        public void munmap(long address, long size, int memoryTag) {
            addrToFd.remove(address);
            super.munmap(address, size, memoryTag);
        }

        @Override
        public long openAppend(LPSZ name) {
            long fd = super.openAppend(name);
            trackFd(fd, name);
            return fd;
        }

        @Override
        public long openCleanRW(LPSZ name, long size) {
            long fd = super.openCleanRW(name, size);
            trackFd(fd, name);
            return fd;
        }

        @Override
        public long openRO(LPSZ name) {
            long fd = super.openRO(name);
            trackFd(fd, name);
            return fd;
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            long fd = super.openRW(name, opts);
            trackFd(fd, name);
            return fd;
        }

        @Override
        public void syncfs(long fd) {
            syncfsCount++;
            super.syncfs(fd);
        }

        private void trackFd(long fd, LPSZ name) {
            if (fd > -1) {
                fdToPath.put(fd, Utf8String.newInstance(name).toString());
            }
        }
    }

    /**
     * Pins the writeback drain on and counts its calls. Also counts the sync calls on each WAL segment file,
     * resolved from the fd, or for an msync from the mapping's address.
     */
    static class WritebackDrainFacade extends TestFilesFacadeImpl {
        private static final Pattern SEGMENT_FILE = Pattern.compile(".*[/\\\\]wal[0-9]+[/\\\\][0-9]+[/\\\\]([^/\\\\]+)");
        final Map<String, Map<String, int[]>> segmentSyncs = new TreeMap<>();
        // fd and flags of every sync_file_range call, in call order
        final LongList syncFileRangeCalls = new LongList();
        private final Map<Long, String> addrToFile = new HashMap<>();
        private final Map<Long, String> fdToFile = new HashMap<>();
        boolean drain = true;
        int probeCount, syncFileRangeCount;
        String probedPath;
        String writebackErrorCall;
        private String writebackErrorFile;

        @Override
        public void barrierFsync(long fd) {
            countSegmentSync(fdToFile.get(fd), "barrierFsync");
            super.barrierFsync(fd);
        }

        @Override
        public boolean close(long fd) {
            fdToFile.remove(fd);
            return super.close(fd);
        }

        @Override
        public void fdatasync(long fd) {
            if (takeWritebackError(fdToFile.get(fd), "fdatasync")) {
                throw CairoException.dataSyncFailure(this, 5, "fdatasync", "injected writeback error");
            }
            super.fdatasync(fd);
        }

        @Override
        public boolean isSyncFileRangeEffective(Utf8Sequence root) {
            probeCount++;
            probedPath = root.toString();
            // Pinned, so the test runs where the real probe says no: off Linux, or on ZFS.
            return drain;
        }

        @Override
        public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
            final long addr = super.mmap(fd, len, offset, flags, memoryTag);
            trackMapping(addr, fd);
            return addr;
        }

        @Override
        public long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
            final long newAddr = super.mremap(fd, addr, previousSize, newSize, offset, mode, memoryTag);
            addrToFile.remove(addr);
            trackMapping(newAddr, fd);
            return newAddr;
        }

        @Override
        public void msync(long addr, long len, boolean async) {
            countSegmentSync(addrToFile.get(addr), async ? "msyncAsync" : "msyncSync");
            if (!async && takeWritebackError(addrToFile.get(addr), "msync")) {
                throw CairoException.dataSyncFailure(this, 5, "msync", "injected writeback error");
            }
            super.msync(addr, len, async);
        }

        @Override
        public void munmap(long address, long size, int memoryTag) {
            addrToFile.remove(address);
            super.munmap(address, size, memoryTag);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            final long fd = super.openRW(name, opts);
            final Matcher segmentFile = SEGMENT_FILE.matcher(Utf8String.newInstance(name).toString());
            if (fd > -1 && segmentFile.matches()) {
                fdToFile.put(fd, segmentFile.group(1));
            }
            return fd;
        }

        @Override
        public int syncFileRange(long fd, long offset, long nbytes, int flags) {
            syncFileRangeCount++;
            syncFileRangeCalls.add(fd, flags);
            countSegmentSync(fdToFile.get(fd), "syncFileRange");
            if ((flags & Files.SYNC_FILE_RANGE_WAIT_AFTER) != 0 && takeWritebackError(fdToFile.get(fd), "sync_file_range")) {
                return -1;
            }
            return super.syncFileRange(fd, offset, nbytes, flags);
        }

        private String barrieredFiles() {
            final List<String> files = new ArrayList<>();
            for (Map.Entry<String, Map<String, int[]>> file : segmentSyncs.entrySet()) {
                if (file.getValue().containsKey("barrierFsync")) {
                    files.add(file.getKey());
                }
            }
            return files.toString();
        }

        private void countSegmentSync(String file, String call) {
            if (file != null) {
                segmentSyncs.computeIfAbsent(file, k -> new TreeMap<>()).computeIfAbsent(call, k -> new int[1])[0]++;
            }
        }

        /**
         * The next call that waits on the writeback of this segment file gets a writeback error, as Linux
         * reports one to the first waiter on each open file.
         */
        private void failNextWait(String file) {
            writebackErrorFile = file;
            writebackErrorCall = null;
        }

        private String segmentSyncSummary() {
            final StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, Map<String, int[]>> file : segmentSyncs.entrySet()) {
                if (!sb.isEmpty()) {
                    sb.append("; ");
                }
                sb.append(file.getKey()).append(':');
                for (Map.Entry<String, int[]> call : file.getValue().entrySet()) {
                    sb.append(' ').append(call.getKey()).append('=').append(call.getValue()[0]);
                }
            }
            return sb.toString();
        }

        private boolean takeWritebackError(String file, String call) {
            if (file != null && file.equals(writebackErrorFile)) {
                writebackErrorFile = null;
                writebackErrorCall = call;
                return true;
            }
            return false;
        }

        private void trackMapping(long addr, long fd) {
            final String file = fdToFile.get(fd);
            if (file != null && addr != -1 && addr != 0) {
                addrToFile.put(addr, file);
            }
        }
    }

    static class NonWideSyncfsWalFdatasyncFacade extends WalFdatasyncFacade {
        @Override
        public boolean isSyncfsFileSystemWide() {
            return false;
        }
    }

    /**
     * Non-wide syncfs plus a record of which partition directories the epoch opened files under.
     */
    static class EpochOpenTrackingFacade extends WalFdatasyncFacade {
        private final java.util.Set<String> openedPartitionDirs = java.util.concurrent.ConcurrentHashMap.newKeySet();

        @Override
        public boolean isSyncfsFileSystemWide() {
            return false;
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            for (int day = 1; day <= 3; day++) {
                final String dir = "1970-01-0" + day;
                if (Utf8s.containsAscii(name, dir)) {
                    openedPartitionDirs.add(dir);
                }
            }
            return super.openRW(name, opts);
        }

        public java.util.Set<String> openedPartitionDirs() {
            return openedPartitionDirs;
        }

        public void reset() {
            openedPartitionDirs.clear();
        }
    }

    /**
     * Non-wide syncfs plus a partition directory whose files refuse to open for write, standing in for the
     * read-only mount behind a soft-linked attached partition.
     */
    static class ReadOnlyPartitionSyncfsFacade extends WalFdatasyncFacade {
        private final AtomicInteger deniedOpenAttempts = new AtomicInteger();
        private volatile String deniedDir;

        public int deniedOpenAttempts() {
            return deniedOpenAttempts.get();
        }

        public void denyWritesUnder(String partitionDirName) {
            deniedDir = partitionDirName;
        }

        @Override
        public boolean isSyncfsFileSystemWide() {
            return false;
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            final String denied = deniedDir;
            if (denied != null && Utf8s.containsAscii(name, denied)) {
                deniedOpenAttempts.incrementAndGet();
                return -1;
            }
            return super.openRW(name, opts);
        }
    }
}
