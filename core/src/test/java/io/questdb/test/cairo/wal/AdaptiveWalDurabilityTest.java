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
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TDD tests for Plan 2 — CommitMode.ADAPTIVE: durable WAL commit (Task A) + lazy table apply (Task B).
 *
 * <p>Task A: under ADAPTIVE, every WAL commit issues {@code fdatasync} on the segment column
 * data file(s), the WAL-e events file ({@code _event}), AND the sequencer part file
 * ({@code _txn_parts/…}) and header ({@code _txnlog}), IN THAT ORDER
 * (data → events → sequencer), before the commit returns.
 *
 * <p>Task B: under ADAPTIVE, {@code ApplyWal2TableJob} (drainWalQueue) must NOT issue any
 * msync or fdatasync on the TABLE PARTITION column files during apply. The table is a
 * rebuildable cache of the durable WAL, so flushing it on apply wastes I/O.
 * Under SYNC mode, the column files ARE flushed — the test distinguishes both.
 *
 * <p>Under NOSYNC mode, zero fdatasync calls must be issued to any files.
 *
 * <p>(a) ordering test — RED before Task A, GREEN after.
 * <p>(b) NOSYNC zero-fdatasync test — regression guard.
 * <p>(c) round-trip data-integrity test — ingest → drainWalQueue → select returns data.
 * <p>(d) ADAPTIVE no-crash on dropped column (NullMemory guard, Task A review).
 * <p>(e) Task B — ADAPTIVE apply: ZERO msync/fdatasync on table partition column files.
 * <p>(f) Task B — SYNC apply: NON-ZERO msync/fdatasync on table partition column files.
 * <p>(g) Task B — correctness: adaptive lazy apply produces correct query results.
 */
public class AdaptiveWalDurabilityTest extends AbstractCairoTest {

    /**
     * (a) ADAPTIVE: a WAL commit must fdatasync segment column data BEFORE events BEFORE sequencer.
     * Fails until WalWriter.syncIfRequired(), WalEventWriter.sync(), and
     * TableTransactionLogV2.sync0() all call fdatasync after their msync under ADAPTIVE mode.
     *
     * <p>We do a "warmup" insert first (to pre-allocate all file pages so that no page-extension
     * fdatasyncs occur during the measurement commit). Then we reset the fdatasync log and do
     * the measurement insert, capturing only the per-commit durability fdatasyncs.
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

            // Measurement insert: pages already allocated, so only the per-commit durability
            // fdatasyncs (from our new ADAPTIVE code) appear in the log.
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
     * (b) NOSYNC: zero fdatasync calls must be issued on WAL commit.
     * Sets CAIRO_DEFAULT_SEQ_PART_TXN_COUNT > 0 so the V2 sequencer {@code sync0} NOSYNC branch
     * is also exercised (regression guard for the V2 path, not just V1).
     */
    @Test
    public void testNosyncIssuesZeroFdatasync() throws Exception {
        // NOSYNC is the default — no need to set the property, but we set it explicitly for clarity.
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
     * (c) Round-trip: adaptive ingest → drainWalQueue → select returns the inserted data.
     * Uses the default FilesFacade (not the tracking one) because we only need data correctness here.
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
    // Note: testAdaptiveRoundTrip uses the plain assertMemoryLeak() (no custom ff) so that
    // the BlockFileWriter and other engine components operate normally.

    /**
     * (d) Regression guard: ADAPTIVE commit must NOT crash on a WAL table that has had a column
     * dropped. Dropped-column slots are stored as {@code NullMemory.INSTANCE} (not {@code null}),
     * and {@code NullMemory.getFd()} throws {@link UnsupportedOperationException}.
     *
     * <p>The fix in {@code WalWriter.syncIfRequired()} guards with
     * {@code !(column instanceof NullMemory)} instead of {@code column.isOpen()}.
     *
     * <p>Before Fix 1 this test throws {@code UnsupportedOperationException} from
     * {@code NullMemory.getFd()} during the post-drop insert commit.
     * After Fix 1 it succeeds and the surviving data round-trips correctly.
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

            // Drop column 'b' — its slot becomes NullMemory.INSTANCE inside WalWriter.
            execute("alter table dc_test drop column b");
            drainWalQueue();

            // Force the WalWriter to be closed and reopened so it is initialised fresh
            // with NullMemory in the dropped slot (mirrors a server restart scenario).
            engine.releaseInactive();
            engine.releaseInactiveTableSequencers();

            // Insert after drop: WalWriter.syncIfRequired() must not call NullMemory.getFd().
            // Before Fix 1 this throws UnsupportedOperationException.
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

    // ---------- Task B: lazy apply tests ----------

    /**
     * (e) Task B — ADAPTIVE apply: ZERO msync/fdatasync on table partition column files.
     *
     * <p>The tracking facade records every {@code msync} and {@code fdatasync} call, mapping
     * mmap addresses to file paths via open-time tracking. We reset the counters AFTER the
     * WAL insert (so WAL-commit syncs from Task A are excluded) and BEFORE {@code drainWalQueue}.
     * After apply, table partition column-file sync count must be zero.
     *
     * <p>RED before Task B (ADAPTIVE currently falls through to syncColumns0 = per-file msync).
     * GREEN after making syncColumns skip SYNC/ASYNC paths for ADAPTIVE.
     */
    @Test
    public void testAdaptiveApplyIssuezZeroColumnSyncsOnApply() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 16);
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        // Disable the durable epoch so this test isolates the LAZY APPLY's own sync behavior; the
        // epoch (Plan 3B) deliberately forces a column flush from inside the apply worker, which is
        // covered separately by testFsyncMaterializedStateForcesFlushAndWritesEpochCopies + the
        // adaptive epoch crash test.
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, -1);

        final TableSyncTrackingFacade trackFf = new TableSyncTrackingFacade();
        assertMemoryLeak(trackFf, () -> {
            execute("create table tab_e (ts timestamp, v long) timestamp(ts) partition by day wal");

            // Insert: triggers WAL commit with Task A fdatasyncs (segment/events/seq).
            execute("insert into tab_e values ('2024-06-01T00:00:00.000000Z', 42)");

            // Reset counters AFTER insert so WAL-commit syncs are excluded.
            // Only apply-side syncs (from drainWalQueue) are counted below.
            trackFf.resetTableColumnSyncs();

            // Apply: ApplyWal2TableJob materialises the WAL txn into the table partition.
            // Under ADAPTIVE (Task B), syncColumns() must behave like NOSYNC here — no msync/fdatasync.
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
     * (f) Task B — SYNC apply: NON-ZERO msync/fdatasync on table partition column files.
     *
     * <p>Contrast test: proves the tracking facade correctly distinguishes table-partition
     * column syncs from WAL syncs, and that SYNC mode DOES flush the columns on apply.
     * If this test passes but (e) also passes, that means adaptive genuinely skips what
     * SYNC does — i.e. the two tests form a paired RED/GREEN guard.
     */
    @Test
    public void testSyncApplyFlushesTableColumnFiles() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 16);
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");

        final TableSyncTrackingFacade trackFf = new TableSyncTrackingFacade();
        assertMemoryLeak(trackFf, () -> {
            execute("create table tab_f (ts timestamp, v long) timestamp(ts) partition by day wal");

            execute("insert into tab_f values ('2024-06-02T00:00:00.000000Z', 99)");

            // Reset AFTER insert, BEFORE apply — same discipline as test (e).
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
     * (g) Task B — correctness: adaptive lazy apply produces correct data for multi-row txn.
     *
     * <p>Verifies that skipping the column flush on apply does not corrupt the materialized
     * state — reads after drainWalQueue return all inserted rows in order.
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
     * (h) B1 — {@code fsyncMaterializedState()} forces the column flush + commit-pointer fsync
     * INDEPENDENT of commit mode, even under ADAPTIVE (whose apply path skips column sync), AND
     * persists the durable epoch copies {@code _txn.epoch} / {@code _cv.epoch}.
     *
     * <p>Under ADAPTIVE, test (e) proved the lazy apply issues ZERO column syncs. This test resets
     * the sync counters AFTER drainWalQueue (so the lazy apply's zero is the baseline), then calls
     * {@code writer.fsyncMaterializedState()} and asserts:
     * <ol>
     *   <li>table partition column files WERE synced (the durable cut forces the flush the SYNC path
     *       proves, regardless of ADAPTIVE);</li>
     *   <li>the two durable epoch copies exist on disk in the table dir.</li>
     * </ol>
     */
    @Test
    public void testFsyncMaterializedStateForcesFlushAndWritesEpochCopies() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 16);
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        // Disable the automatic epoch so the EXPLICIT fsyncMaterializedState() call below is the only
        // durable cut under test here (B1 in isolation; the apply-worker hook is B2's concern).
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, -1);

        final TableSyncTrackingFacade trackFf = new TableSyncTrackingFacade();
        assertMemoryLeak(trackFf, () -> {
            execute("create table tab_h (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into tab_h values ('2024-08-01T00:00:00.000000Z', 1)");
            execute("insert into tab_h values ('2024-08-01T01:00:00.000000Z', 2)");
            drainWalQueue(); // lazy apply: zero column syncs (proven by test (e))

            io.questdb.cairo.TableToken tt = engine.verifyTableName("tab_h");

            // Reset AFTER the lazy apply so the zero-sync apply is the baseline; only the
            // fsyncMaterializedState() syncs below are counted.
            trackFf.resetTableColumnSyncs();

            try (io.questdb.cairo.TableWriter writer = getWriter(tt)) {
                writer.fsyncMaterializedState();
            }

            long tableColumnSyncs = trackFf.getTableColumnSyncCount();
            Assert.assertTrue(
                    "fsyncMaterializedState() must flush table partition column files even under ADAPTIVE, but got 0",
                    tableColumnSyncs > 0
            );

            // The durable epoch copies must exist in the table dir.
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
     * (i) B2 — the apply worker fires a durable epoch automatically: after drainWalQueue under
     * ADAPTIVE with epochs enabled (interval 0 => every batch), the {@code _snapshot} marker is
     * written, {@code _txn.epoch}/{@code _cv.epoch} exist, the tracker publishes
     * {@code durableEpochSeqTxn} = the applied seqTxn, and the epoch txn is PINNED in the scoreboard
     * (so partition purge can't reclaim it).
     */
    @Test
    public void testApplyWorkerFiresDurableEpoch() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 16);
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        // interval 0 => epoch fires on every apply batch (no cadence wait), deterministic for the test.
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, 0);

        assertMemoryLeak(() -> {
            execute("create table tab_i (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into tab_i values ('2024-09-01T00:00:00.000000Z', 1)");
            execute("insert into tab_i values ('2024-09-01T01:00:00.000000Z', 2)");
            drainWalQueue();

            io.questdb.cairo.TableToken tt = engine.verifyTableName("tab_i");

            // The marker and the durable epoch copies must all exist after the worker epoch fired.
            try (io.questdb.std.str.Path p = new io.questdb.std.str.Path()) {
                p.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(io.questdb.cairo.TableUtils.SNAPSHOT_FILE_NAME);
                Assert.assertTrue("_snapshot marker must exist", engine.getConfiguration().getFilesFacade().exists(p.$()));
            }
            assertEpochCopyExists(tt, io.questdb.cairo.TableUtils.TXN_FILE_NAME);
            assertEpochCopyExists(tt, io.questdb.cairo.TableUtils.COLUMN_VERSION_FILE_NAME);

            // The marker's recorded epochSeqTxn must equal the applied seqTxn (2 inserts => seqTxn 2).
            try (io.questdb.cairo.SnapshotMarker marker = new io.questdb.cairo.SnapshotMarker(engine.getConfiguration());
                 io.questdb.std.str.Path p = new io.questdb.std.str.Path()) {
                p.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(io.questdb.cairo.TableUtils.SNAPSHOT_FILE_NAME);
                marker.of(p.$());
                Assert.assertTrue("marker must load", marker.tryLoad());
                Assert.assertEquals("epochSeqTxn == applied seqTxn", 2L, marker.getEpochSeqTxn());
            }

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

    private void assertEpochCopyExists(io.questdb.cairo.TableToken tt, String baseFileName) {
        try (io.questdb.std.str.Path p = new io.questdb.std.str.Path()) {
            p.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(baseFileName).put(".epoch");
            Assert.assertTrue(
                    "durable epoch copy must exist: " + p,
                    engine.getConfiguration().getFilesFacade().exists(p.$())
            );
        }
    }

    // ---------- helpers ----------

    /**
     * Returns true if the given path is a WAL segment column data file (.d file or fixed-size
     * column file in a wal/ segment directory — i.e. NOT the event or sequencer files).
     */
    private static boolean isWalColumnDataFile(String p) {
        if (p == null) {
            return false;
        }
        // WAL segment column files live inside wal<N>/<segmentId>/ and end with .d
        // (variable-length data: varchar, binary) or have no extension (fixed-size columns).
        // The key distinguishing feature is the path containing "/wal" and a segment integer
        // directory, AND not being an event or sequencer file.
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
     * A FilesFacade that counts every {@code msync} and {@code fdatasync} call on TABLE
     * PARTITION COLUMN FILES (i.e. not in {@code wal<N>/} segment dirs, not in
     * {@code txn_seq/}, and not control/metadata files).
     *
     * <p>It tracks: fd→path (on open), addr→fd (on mmap), then resolves msync(addr) and
     * fdatasync(fd) to a path and classifies it. Only table-partition column file syncs
     * are counted; WAL-segment and sequencer syncs are ignored so the test can reset
     * AFTER the WAL insert (Task A syncs) and measure ONLY the apply-side syncs.
     */
    static class TableSyncTrackingFacade extends TestFilesFacadeImpl {
        // fd -> file path (populated on every open call)
        private final Map<Long, String> fdToPath = new HashMap<>();
        // mmap address -> fd (populated on mmap, removed on munmap)
        private final Map<Long, Long> addrToFd = new HashMap<>();
        // paths of table-partition column files that were synced (msync or fdatasync)
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

        private void trackFd(long fd, LPSZ name) {
            if (fd > -1) {
                fdToPath.put(fd, Utf8String.newInstance(name).toString());
            }
        }
    }

    /**
     * Returns true if the given path is a TABLE PARTITION column file — i.e. a file that
     * {@code syncColumns()} flushes on each commit. Specifically, it must NOT be in the WAL
     * segment directory ({@code /wal<N>/}), NOT in the sequencer directory ({@code /txn_seq/}),
     * and NOT a table-level control file ({@code _txn}, {@code _cv}, {@code _meta}, {@code _todo}).
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
        // Table-root control files: _txn, _cv, _meta, _todo, _lock, bitmap indexes (_k, _v)
        // that live AT the table root level (not in a date-partition subdirectory).
        // We only want partition column data files (.d, or fixed-size columns).
        // Filter: must end with .d (variable-len data/aux) or look like a fixed-size column
        // (no extension, e.g. "ts", "v"). Accept only if the last path component is a
        // plausible column file (not a QuestDB control file).
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
}
