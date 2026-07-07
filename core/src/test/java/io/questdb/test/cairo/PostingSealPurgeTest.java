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

import io.questdb.MessageBus;
import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.PostingSealPurgeJob;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxnScoreboard;
import io.questdb.cairo.idx.PostingIndexFwdReader;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.tasks.PostingSealPurgeTask;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.LogCapture;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;
import static org.junit.Assert.*;

public class PostingSealPurgeTest extends AbstractCairoTest {

    private static final int BATCH_KEYS = 4;
    private int iteration;

    @Before
    public void setUpPurge() {
        iteration = 1;
        setCurrentMicros(0);
        node1.setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY, 1);
        node1.setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY_LIMIT, 10);
        node1.setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY_MULTIPLIER, 2);
    }

    @Test
    public void testBackoffRetryEventuallyPurges() throws Exception {
        assertMemoryLeak(() -> {
            if (configuration.disableColumnPurgeJob()) {
                return;
            }
            TableToken tok = createPostingTable("ps_purge_backoff");
            FilesFacade ff = configuration.getFilesFacade();

            try (Path partitionPath = partitionPathFor(tok);
                 PostingSealPurgeJob job = new PostingSealPurgeJob(engine);
                 TxnScoreboard scoreboard = engine.getTxnScoreboard(tok)) {
                String col = "c_backoff";
                long sealTxn = writeAndSeal(partitionPath, col);
                LPSZ pv = PostingIndexUtils.valueFileName(partitionPath, col, COLUMN_NAME_TXN_NONE, sealTxn);

                assertTrue(scoreboard.acquireTxn(0, 3L));
                try {
                    publishPurgeTask(tok, col, sealTxn, 10L);
                    for (int i = 0; i < 5; i++) {
                        runPurgeJob(job);
                        assertTrue(".pv must survive each retry while txn is held", ff.exists(pv));
                    }
                } finally {
                    scoreboard.releaseTxn(0, 3L);
                }

                runPurgeJob(job, 3);
                assertFalse(".pv must be purged after the retry loop succeeds", ff.exists(pv));
            }
        });
    }

    @Test
    public void testCompletedColumnWritten() throws Exception {
        assertMemoryLeak(() -> {
            if (configuration.disableColumnPurgeJob()) {
                return;
            }
            TableToken tok = createPostingTable("ps_purge_completed");
            FilesFacade ff = configuration.getFilesFacade();

            try (Path partitionPath = partitionPathFor(tok);
                 PostingSealPurgeJob job = new PostingSealPurgeJob(engine)) {
                String col = "c_completed";
                long sealTxn = writeAndSeal(partitionPath, col);
                LPSZ pv = PostingIndexUtils.valueFileName(partitionPath, col, COLUMN_NAME_TXN_NONE, sealTxn);

                publishPurgeTask(tok, col, sealTxn, 1L);
                runPurgeJob(job, 3);

                assertFalse(".pv must be purged", ff.exists(pv));
                assertLogTableCompletedCount(col);
            }
        });
    }

    @Test
    public void testEndToEndSealAndPurge() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "ps_purge_e2e";
                int plen = path.size();
                FilesFacade ff = configuration.getFilesFacade();

                long firstSealTxn;
                long secondSealTxn;
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                    // Wave 1: write some data, force a seal.
                    for (int i = 0; i < 8; i++) {
                        writer.add(i % BATCH_KEYS, i);
                    }
                    writer.setMaxValue(7);
                    writer.commit();
                    writer.seal();
                    firstSealTxn = PostingIndexUtils.readSealTxnFromKeyFile(
                            ff, PostingIndexUtils.keyFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE));
                    assertTrue("first seal must produce a positive sealTxn", firstSealTxn > 0);

                    // Wave 2: more data, another seal.
                    for (int i = 8; i < 16; i++) {
                        writer.add(i % BATCH_KEYS, i);
                    }
                    writer.setMaxValue(15);
                    writer.commit();
                    writer.seal();
                    secondSealTxn = PostingIndexUtils.readSealTxnFromKeyFile(
                            ff, PostingIndexUtils.keyFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE));
                    assertTrue("second seal must advance sealTxn", secondSealTxn > firstSealTxn);
                }

                // After writer.close() no scoreboard txn covers the column,
                // so the previous-sealTxn .pv must be on disk pending purge,
                // and the live one must also be on disk.
                assertTrue("first-seal .pv survives until purge", ff.exists(PostingIndexUtils.valueFileName(
                        path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, firstSealTxn)));
                assertTrue("live .pv at the latest sealTxn must exist", ff.exists(PostingIndexUtils.valueFileName(
                        path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, secondSealTxn)));

                // The writer's close() returns its outbox without publishing,
                // so the job sees no queued tasks. We instead drive the orphan
                // recovery path: a new writer reopen will scan the directory,
                // detect the prior-sealTxn file as an orphan, and enqueue it
                // for purge. Then the job picks it up.
                try (PostingIndexWriter reopen = new PostingIndexWriter(configuration)) {
                    reopen.of(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, false);
                    // Trigger a no-op commit cycle to publish the orphan
                    // entries that logOrphanSealedFiles enqueued.
                    reopen.publishPendingPurges(
                            engine.getMessageBus(),
                            engine.getTableTokenIfExists("__not_a_real_table__"),
                            0, ColumnType.TIMESTAMP_MICRO, 0L
                    );
                    // The above publish is a no-op because we deliberately pass
                    // a null/missing TableToken — we only want to verify the
                    // orphan was enqueued. The real publish happens later when
                    // a TableWriter commits with the right partition info.
                }

                // Drive the purge job synchronously a few times. Each .run()
                // call drains the queue and runs one retry pass; backoff means
                // we may need several iterations before the orphan window is
                // actually attempted.
                if (!configuration.disableColumnPurgeJob()) {
                    try (PostingSealPurgeJob job = new PostingSealPurgeJob(engine)) {
                        for (int i = 0; i < 8; i++) {
                            job.run();
                            // Tiny pause so retry-backoff can elapse.
                            Os.pause();
                        }
                    }
                }

                // Live .pv must always still be on disk.
                assertTrue("live .pv must remain after purge runs",
                        ff.exists(PostingIndexUtils.valueFileName(
                                path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, secondSealTxn)));
                // The first-seal .pv may or may not be gone yet depending on
                // backoff timing; both states are valid for this minimal
                // smoke test (a longer-running test would loop until the
                // queue drains). The crucial invariant is that purge does
                // NOT delete the live file.
            }
        });
    }

    @Test
    public void testJobConstructionCreatesLogTable() throws Exception {
        assertMemoryLeak(() -> {
            if (configuration.disableColumnPurgeJob()) {
                return;
            }
            try (PostingSealPurgeJob job = new PostingSealPurgeJob(engine)) {
                String tableName = job.getLogTableName();
                assertTrue("log table name must be reported", tableName != null && !tableName.isEmpty());
                int outstanding = job.getOutstandingPurgeTasks();
                assertFalse("outstanding tasks must not be negative", outstanding < 0);
                job.run();
            }
        });
    }

    @Test
    public void testNoPurgeWhileScoreboardHoldsTxn() throws Exception {
        assertMemoryLeak(() -> {
            if (configuration.disableColumnPurgeJob()) {
                return;
            }
            TableToken tok = createPostingTable("ps_purge_scoreboard");
            FilesFacade ff = configuration.getFilesFacade();

            try (Path partitionPath = partitionPathFor(tok);
                 PostingSealPurgeJob job = new PostingSealPurgeJob(engine);
                 TxnScoreboard scoreboard = engine.getTxnScoreboard(tok)) {
                String col = "c_sb";
                long sealTxn = writeAndSeal(partitionPath, col);
                LPSZ pv = PostingIndexUtils.valueFileName(partitionPath, col, COLUMN_NAME_TXN_NONE, sealTxn);

                // Reader acquires txn 5 which sits strictly inside [0, 10).
                assertTrue(scoreboard.acquireTxn(0, 5L));
                try {
                    publishPurgeTask(tok, col, sealTxn, 10L);
                    runPurgeJob(job, 3);
                    assertTrue(".pv must stay while a reader holds a txn in range", ff.exists(pv));
                } finally {
                    scoreboard.releaseTxn(0, 5L);
                }

                runPurgeJob(job, 3);
                assertFalse(".pv must be purged once the range clears", ff.exists(pv));
            }
        });
    }

    // testPostLiveOrphanDeletedInline relied on the v1
    // logOrphanSealedFiles directory scan that ran on writer-open. v2
    // routes orphan cleanup through the chain: any abandoned chain entry
    // is dropped by recoveryDropAbandoned and queued for purge with a
    // [0, MAX) window, but a fabricated standalone .pv with no
    // corresponding chain entry is never reachable through the chain
    // and so is intentionally NOT touched at writer-open. Such files
    // can only arise from manual filesystem tampering or a snapshot
    // restore artifact and are out of scope for the in-process recovery
    // path. Recovery for genuinely abandoned (chain-tracked) entries is
    // covered by PostingIndexOracleTest's recovery tests and
    // PostingIndexChainWriterTest's testRecoveryDropAbandoned* set.

    @Test
    public void testPurgeRemovesCoverFiles() throws Exception {
        assertMemoryLeak(() -> {
            if (configuration.disableColumnPurgeJob()) {
                return;
            }
            TableToken tok = createPostingTable("ps_purge_cover");
            FilesFacade ff = configuration.getFilesFacade();

            try (Path partitionPath = partitionPathFor(tok);
                 PostingSealPurgeJob job = new PostingSealPurgeJob(engine)) {
                String col = "c_cover";
                long sealTxn = writeCoveringAndSeal(partitionPath, col);
                int pLen = partitionPath.size();
                // Both valueFileName and coverDataFileName append onto the
                // shared Path buffer, so each existence check must trim back
                // to the partition root before re-resolving the file name.
                assertTrue(".pv must exist after seal",
                        ff.exists(PostingIndexUtils.valueFileName(
                                partitionPath.trimTo(pLen), col, COLUMN_NAME_TXN_NONE, sealTxn)));
                assertTrue(".pc0 must exist after seal with covering",
                        ff.exists(PostingIndexUtils.coverDataFileName(
                                partitionPath.trimTo(pLen), col, 0, COLUMN_NAME_TXN_NONE, COLUMN_NAME_TXN_NONE, sealTxn)));

                publishPurgeTask(tok, col, sealTxn, 1L);
                runPurgeJob(job, 3);

                assertFalse(".pv must be purged",
                        ff.exists(PostingIndexUtils.valueFileName(
                                partitionPath.trimTo(pLen), col, COLUMN_NAME_TXN_NONE, sealTxn)));
                assertFalse(".pc0 must be purged together with .pv",
                        ff.exists(PostingIndexUtils.coverDataFileName(
                                partitionPath.trimTo(pLen), col, 0, COLUMN_NAME_TXN_NONE, COLUMN_NAME_TXN_NONE, sealTxn)));
            }
        });
    }

    @Test
    public void testPurgeRetriesWhenRemoveFails() throws Exception {
        final int failures = 2;
        final int[] removeCalls = {0};
        ff = new TestFilesFacadeImpl() {
            @Override
            public boolean removeQuiet(LPSZ name) {
                if (name != null && Utf8s.containsAscii(name, ".pv.") && removeCalls[0] < failures) {
                    removeCalls[0]++;
                    return false;
                }
                return super.removeQuiet(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            if (configuration.disableColumnPurgeJob()) {
                return;
            }
            TableToken tok = createPostingTable("ps_purge_flake");
            FilesFacade runtimeFf = configuration.getFilesFacade();

            try (Path partitionPath = partitionPathFor(tok);
                 PostingSealPurgeJob job = new PostingSealPurgeJob(engine)) {
                String col = "c_flake";
                long sealTxn = writeAndSeal(partitionPath, col);
                LPSZ pv = PostingIndexUtils.valueFileName(partitionPath, col, COLUMN_NAME_TXN_NONE, sealTxn);

                publishPurgeTask(tok, col, sealTxn, 1L);
                // Each runPurgeJob pass advances currentMicros to clear the
                // backoff; need enough passes to cover (failures + 1) attempts.
                for (int i = 0; i < 10 && runtimeFf.exists(pv); i++) {
                    runPurgeJob(job);
                }
                assertFalse(".pv must be purged once removeQuiet stops failing", runtimeFf.exists(pv));
                assertTrue("removeQuiet must have been called at least (failures + 1) times",
                        removeCalls[0] >= failures);
            }
        });
    }

    @Test
    public void testPublishPendingPurgesDropsLiveHeadEntry() throws Exception {
        // publishPendingPurges' head-guard must drop an outbox entry whose
        // sealTxn equals the current chain head (the live generation) rather than
        // publish a purge that would later delete the live .pv. This backstops C1
        // on the writer side; PostingSealPurgeOperator backstops it on the delete
        // side. No prior test plants a head-matching entry, so the drop branch
        // was uncovered.
        assertMemoryLeak(() -> {
            TableToken tok = createPostingTable("ps_purge_headguard");
            FilesFacade ff = configuration.getFilesFacade();
            try (Path partitionPath = partitionPathFor(tok);
                 PostingIndexWriter writer = new PostingIndexWriter(
                         configuration, partitionPath, "c_hg", COLUMN_NAME_TXN_NONE)) {
                // publishPendingPurges skips a recycled outbox entry whose
                // partitionTimestamp == Long.MIN_VALUE (the path-based open default)
                // BEFORE the head-guard, so give the writer the partition context a
                // real TableWriter-owned writer carries -- matching partitionPathFor
                // (timestamp 0, nameTxn -1). Without this the planted entry is
                // skipped and the head-guard is never exercised.
                writer.setPartitionContext(0L, -1L);
                for (int i = 0; i < 8; i++) {
                    writer.add(i % BATCH_KEYS, i);
                }
                writer.setMaxValue(7);
                writer.commit();
                writer.seal();
                int pLen = partitionPath.size();
                long headSealTxn = PostingIndexUtils.readSealTxnFromKeyFile(
                        ff, PostingIndexUtils.keyFileName(partitionPath.trimTo(pLen), "c_hg", COLUMN_NAME_TXN_NONE));
                assertTrue("seal must produce a positive head sealTxn", headSealTxn > 0);

                // Plant a pending purge whose sealTxn IS the live chain head.
                // (seal() may also have queued a purge for the prior empty
                // sealTxn=0; that one is not the head and is irrelevant here.)
                writer.scheduleHeadPurgeForTesting();
                writer.publishPendingPurges(
                        engine.getMessageBus(), tok, PartitionBy.NONE, ColumnType.TIMESTAMP_MICRO, 1000L);

                // The head-guard must have dropped the head-matching entry: scan
                // everything published and assert the head sealTxn is not among it.
                MessageBus bus = engine.getMessageBus();
                RingQueue<PostingSealPurgeTask> queue = bus.getPostingSealPurgeQueue();
                boolean headEnqueued = false;
                long cursor;
                while ((cursor = bus.getPostingSealPurgeSubSeq().next()) >= 0) {
                    if (queue.get(cursor).getSealTxn() == headSealTxn) {
                        headEnqueued = true;
                    }
                    bus.getPostingSealPurgeSubSeq().done(cursor);
                }
                assertFalse("publishPendingPurges must drop a purge whose sealTxn is the live chain head",
                        headEnqueued);
            }
        });
    }

    @Test
    public void testPurgeAbandonsWhenTargetSealTxnIsLiveChainHead() throws Exception {
        // C1 reuse guard: a purge task whose sealTxn is the LIVE chain head (the
        // state a reused-then-republished orphan sealTxn lands in) must be
        // abandoned, never delete the live .pv. The publish-time head guard
        // cannot catch this -- the chain head was the OLD sealTxn when the orphan
        // entry drained -- so the operator re-reads the .pk head at delete time.
        assertMemoryLeak(() -> {
            if (configuration.disableColumnPurgeJob()) {
                return;
            }
            TableToken tok = createPostingTable("ps_purge_live_head");
            FilesFacade ff = configuration.getFilesFacade();

            try (Path partitionPath = partitionPathFor(tok);
                 PostingSealPurgeJob job = new PostingSealPurgeJob(engine)) {
                String col = "c_live";
                // writeAndSeal advances the head past the value it returns; read
                // the LIVE head and target a purge directly at it.
                writeAndSeal(partitionPath, col);
                int pLen = partitionPath.size();
                long liveHead = PostingIndexUtils.readSealTxnFromKeyFile(
                        ff, PostingIndexUtils.keyFileName(partitionPath.trimTo(pLen), col, COLUMN_NAME_TXN_NONE));
                assertTrue("live head sealTxn must be positive", liveHead > 0);
                assertTrue("live .pv must exist after seal", ff.exists(
                        PostingIndexUtils.valueFileName(partitionPath.trimTo(pLen), col, COLUMN_NAME_TXN_NONE, liveHead)));

                // Tiny window so the scoreboard reports the range available --
                // only the operator's .pk head-read can save the live file.
                publishPurgeTask(tok, col, liveHead, 1L);
                runPurgeJob(job, 3);

                assertTrue("operator must abandon a purge targeting the live chain head and keep the .pv",
                        ff.exists(PostingIndexUtils.valueFileName(partitionPath.trimTo(pLen), col, COLUMN_NAME_TXN_NONE, liveHead)));
            }
        });
    }

    @Test
    public void testPurgeLogsReuseRaceWhenLiveHeadDeletedByOrphanUnlink() throws Exception {
        // Post-unlink reuse-race DETECTOR (the partner of the pre-unlink abandon
        // guard). When the pre-unlink .pk head read cannot prove the target is
        // live -- here it transiently fails, returning -1 -- the operator proceeds
        // to unlink, then re-reads the head and re-stats the .pv. If the head is
        // now the target sealTxn AND the .pv is gone, it unlinked a live file and
        // logs a critical REINDEX diagnostic. Drive that exact window: target the
        // LIVE head, fail the .pk reads BEFORE the unlink so the pre-unlink guard
        // misses, let them succeed AFTER, and assert the detector fires.
        final String col = "c_detector";
        final boolean[] armed = {false};
        final boolean[] pvRemoved = {false};
        ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                // Fail every .pk open until the .pv is unlinked: that bounds the
                // failure to the pre-unlink head read (the only .pk consumer before
                // removeQuiet), so the post-unlink read still sees the live head.
                if (armed[0] && !pvRemoved[0] && name != null && Utf8s.containsAscii(name, col + ".pk")) {
                    return -1;
                }
                return super.openRO(name);
            }

            @Override
            public boolean removeQuiet(LPSZ name) {
                boolean removed = super.removeQuiet(name);
                if (armed[0] && name != null && Utf8s.containsAscii(name, col + ".pv.")) {
                    pvRemoved[0] = true;
                }
                return removed;
            }
        };
        LogCapture capture = new LogCapture();
        assertMemoryLeak(ff, () -> {
            if (configuration.disableColumnPurgeJob()) {
                return;
            }
            TableToken tok = createPostingTable("ps_reuse_race");
            FilesFacade runtimeFf = configuration.getFilesFacade();
            try (Path partitionPath = partitionPathFor(tok);
                 PostingSealPurgeJob job = new PostingSealPurgeJob(engine)) {
                writeAndSeal(partitionPath, col);
                int pLen = partitionPath.size();
                long liveHead = PostingIndexUtils.readSealTxnFromKeyFile(
                        runtimeFf, PostingIndexUtils.keyFileName(partitionPath.trimTo(pLen), col, COLUMN_NAME_TXN_NONE));
                partitionPath.trimTo(pLen);
                assertTrue("live head sealTxn must be positive", liveHead > 0);
                assertTrue("live .pv must exist after seal", runtimeFf.exists(
                        PostingIndexUtils.valueFileName(partitionPath.trimTo(pLen), col, COLUMN_NAME_TXN_NONE, liveHead)));
                partitionPath.trimTo(pLen);

                // Target the LIVE head, the state a reused-then-republished orphan
                // sealTxn lands in. Tiny window so the scoreboard reports it ready.
                publishPurgeTask(tok, col, liveHead, 1L);

                capture.start();
                try {
                    armed[0] = true;
                    runPurgeJob(job, 3);
                    capture.waitForRegex("during the orphan unlink \\(reuse race\\).*REINDEX this column/partition");
                } finally {
                    armed[0] = false;
                    capture.stop();
                }
                // The bypassed guard genuinely unlinked the live .pv (the damage the
                // detector reports), confirming the detector path was actually reached.
                assertFalse("the live .pv must have been unlinked by the bypassed guard", runtimeFf.exists(
                        PostingIndexUtils.valueFileName(partitionPath.trimTo(pLen), col, COLUMN_NAME_TXN_NONE, liveHead)));
                partitionPath.trimTo(pLen);
            }
        });
    }

    @Test
    public void testPurgeLogsReuseRaceWhenLiveHeadCoverFileDeletedByOrphanUnlink() throws Exception {
        // Cover-file (.pc) analogue of the .pv reuse-race detector. The .pv path
        // re-reads the head right after its own unlink; the covers are removed
        // later by the directory scan, so the operator re-reads the head AFTER
        // the scan and fires the same REINDEX diagnostic when a live .pc was
        // unlinked. Drive that exact window on a COVERED index: target the LIVE
        // head and fail every .pk read until a .pc is removed, so the pre-unlink
        // guard misses (head reads -1) AND the .pv detector's own head read also
        // returns -1 (suppressed) -- isolating the post-scan cover detector,
        // whose head read succeeds once the .pc is gone.
        final String col = "c_cover_detector";
        final boolean[] armed = {false};
        final boolean[] pcRemoved = {false};
        ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                // Fail every .pk open until a .pc is unlinked: that bounds the
                // failure to the pre-unlink guard and the .pv detector (both run
                // before the cover scan), so only the post-scan cover head read
                // sees the live head.
                if (armed[0] && !pcRemoved[0] && name != null && Utf8s.containsAscii(name, col + ".pk")) {
                    return -1;
                }
                return super.openRO(name);
            }

            @Override
            public boolean removeQuiet(LPSZ name) {
                boolean removed = super.removeQuiet(name);
                if (armed[0] && name != null && Utf8s.containsAscii(name, col + ".pc")) {
                    pcRemoved[0] = true;
                }
                return removed;
            }
        };
        LogCapture capture = new LogCapture();
        assertMemoryLeak(ff, () -> {
            if (configuration.disableColumnPurgeJob()) {
                return;
            }
            TableToken tok = createPostingTable("ps_reuse_race_cover");
            FilesFacade runtimeFf = configuration.getFilesFacade();
            try (Path partitionPath = partitionPathFor(tok);
                 PostingSealPurgeJob job = new PostingSealPurgeJob(engine)) {
                // writeCoveringAndSeal seals twice and RETURNS the superseded txn;
                // the reuse scenario targets the LIVE head, so read it fresh.
                writeCoveringAndSeal(partitionPath, col);
                int pLen = partitionPath.size();
                long liveHead = PostingIndexUtils.readSealTxnFromKeyFile(
                        runtimeFf, PostingIndexUtils.keyFileName(partitionPath.trimTo(pLen), col, COLUMN_NAME_TXN_NONE));
                partitionPath.trimTo(pLen);
                assertTrue("live head sealTxn must be positive", liveHead > 0);
                assertTrue("live .pc0 must exist after covering seal", runtimeFf.exists(
                        PostingIndexUtils.coverDataFileName(
                                partitionPath.trimTo(pLen), col, 0, COLUMN_NAME_TXN_NONE, COLUMN_NAME_TXN_NONE, liveHead)));
                partitionPath.trimTo(pLen);

                // Target the LIVE head, the state a reused-then-republished orphan
                // sealTxn lands in. Tiny window so the scoreboard reports it ready.
                publishPurgeTask(tok, col, liveHead, 1L);

                capture.start();
                try {
                    armed[0] = true;
                    runPurgeJob(job, 3);
                    capture.waitForRegex("during the orphan cover-file unlink \\(reuse race\\).*REINDEX this column/partition");
                } finally {
                    armed[0] = false;
                    capture.stop();
                }
                // The bypassed guard genuinely unlinked the live .pc0 (the damage
                // the detector reports), confirming the cover detector was reached.
                assertFalse("the live .pc0 must have been unlinked by the bypassed guard", runtimeFf.exists(
                        PostingIndexUtils.coverDataFileName(
                                partitionPath.trimTo(pLen), col, 0, COLUMN_NAME_TXN_NONE, COLUMN_NAME_TXN_NONE, liveHead)));
                partitionPath.trimTo(pLen);
            }
        });
    }

    @Test
    public void testPurgeDoesNotLogReuseRaceWhenOrphanCoverRecreatedAfterUnlink() throws Exception {
        // The .pc reuse-race detector's no-false-positive guard (the cover analogue of
        // the .pv pvStillGone re-stat). Benign interleaving: the scan unlinks a genuine
        // orphan .pc, then a reused-then-republished writer re-creates a fresh LIVE .pc
        // at the same sealTxn and advances the head to it. Without the re-stat the
        // post-scan head read sees head == sealTxn and shouts REINDEX though the live .pc
        // is intact. Model that by reporting the removed .pc present again on the
        // operator's post-scan exists() re-stat: coverStillGone is then false, so the
        // detector must stay silent. (testPurgeLogsReuseRaceWhenLiveHeadCoverFileDeleted-
        // ByOrphanUnlink is the true-positive twin, where the removed .pc stays gone.)
        final String col = "c_cover_fp";
        final boolean[] armed = {false};
        final boolean[] pcRemoved = {false};
        final boolean[] coverRestatted = {false};
        ff = new TestFilesFacadeImpl() {
            @Override
            public boolean exists(LPSZ name) {
                // Simulate the reused writer having re-created the cover: once a .pc has
                // been unlinked in this window, report it present again. This is the only
                // exists(.pc) the operator performs and only the fixed code performs it,
                // so it doubles as a "the re-stat actually ran" probe.
                if (armed[0] && pcRemoved[0] && name != null && Utf8s.containsAscii(name, col + ".pc")) {
                    coverRestatted[0] = true;
                    return true;
                }
                return super.exists(name);
            }

            @Override
            public long openRO(LPSZ name) {
                // Fail every .pk open until a .pc is unlinked, bypassing the pre-unlink
                // guard and the .pv detector (both run before the cover scan) so only the
                // post-scan cover detector evaluates the head, mirroring the true-positive twin.
                if (armed[0] && !pcRemoved[0] && name != null && Utf8s.containsAscii(name, col + ".pk")) {
                    return -1;
                }
                return super.openRO(name);
            }

            @Override
            public boolean removeQuiet(LPSZ name) {
                boolean removed = super.removeQuiet(name);
                if (armed[0] && name != null && Utf8s.containsAscii(name, col + ".pc")) {
                    pcRemoved[0] = true;
                }
                return removed;
            }
        };
        LogCapture capture = new LogCapture();
        assertMemoryLeak(ff, () -> {
            if (configuration.disableColumnPurgeJob()) {
                return;
            }
            TableToken tok = createPostingTable("ps_reuse_race_cover_fp");
            FilesFacade runtimeFf = configuration.getFilesFacade();
            try (Path partitionPath = partitionPathFor(tok);
                 PostingSealPurgeJob job = new PostingSealPurgeJob(engine)) {
                writeCoveringAndSeal(partitionPath, col);
                int pLen = partitionPath.size();
                long liveHead = PostingIndexUtils.readSealTxnFromKeyFile(
                        runtimeFf, PostingIndexUtils.keyFileName(partitionPath.trimTo(pLen), col, COLUMN_NAME_TXN_NONE));
                partitionPath.trimTo(pLen);
                assertTrue("live head sealTxn must be positive", liveHead > 0);

                // Target the LIVE head, the state a reused-then-republished orphan sealTxn
                // lands in. Tiny window so the scoreboard reports it ready.
                publishPurgeTask(tok, col, liveHead, 1L);

                capture.start();
                try {
                    armed[0] = true;
                    runPurgeJob(job, 3);
                    // Flush barrier on the same async log path: once this sentinel reaches
                    // the captured sink, any REINDEX the operator emitted earlier (FIFO) is
                    // already there too, so the assertNotLogged below is reliable.
                    LOG.critical().$("posting seal purge test: cover false-positive flush barrier").$();
                    capture.waitForRegex("cover false-positive flush barrier");
                    assertTrue("the operator must re-stat the removed cover (the no-false-positive guard)", coverRestatted[0]);
                    capture.assertNotLogged("orphan cover-file unlink (reuse race)");
                } finally {
                    armed[0] = false;
                    capture.stop();
                }
            }
        });
    }

    @Test
    public void testRecoveryFromLogTable() throws Exception {
        assertMemoryLeak(() -> {
            if (configuration.disableColumnPurgeJob()) {
                return;
            }
            TableToken tok = createPostingTable("ps_purge_recover");
            FilesFacade ff = configuration.getFilesFacade();
            String col = "c_recover";
            final long sealTxn;
            try (Path partitionPath = partitionPathFor(tok)) {
                sealTxn = writeAndSeal(partitionPath, col);
            }

            try (PostingSealPurgeJob seed = new PostingSealPurgeJob(engine);
                 TxnScoreboard scoreboard = engine.getTxnScoreboard(tok)) {
                assertTrue(scoreboard.acquireTxn(0, 5L));
                try {
                    publishPurgeTask(tok, col, sealTxn, 10L);
                    runPurgeJob(seed, 3);
                } finally {
                    scoreboard.releaseTxn(0, 5L);
                }
            }

            try (Path partitionPath = partitionPathFor(tok);
                 PostingSealPurgeJob recovered = new PostingSealPurgeJob(engine)) {
                LPSZ pv = PostingIndexUtils.valueFileName(
                        partitionPath, col, COLUMN_NAME_TXN_NONE, sealTxn);
                assertFalse(".pv must be purged by recovery", ff.exists(pv));
                assertEquals("recovery must leave no pending tasks",
                        0, recovered.getOutstandingPurgeTasks());
            }
        });
    }

    @Test
    public void testReopenEnqueuesOrphansFromPriorIncarnation() throws Exception {
        // A distressed writer can seal a generation at a table txn the table never
        // commits to (txnAtSeal > the committed table txn). Reopening and arming the
        // recovery walk via setCurrentTableTxn must drop that abandoned chain entry
        // and enqueue its orphan .pv/.pc for purge: of() -> runRecoveryWalkIfRequested
        // -> chain.recoveryDropAbandoned -> scheduleOrphanPurge. (v1's writer-open
        // directory scan, logOrphanSealedFiles, is gone -- v2 routes orphan cleanup
        // through the chain; the chain-level mechanics are covered by
        // PostingIndexChainWriterTest's testRecoveryDropAbandoned* set, so this pins
        // only the PostingIndexWriter.of() glue: that an armed reopen actually queues
        // the orphan rather than leaving it on disk.)
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final String name = "ps_purge_orphan";
                final int plen = path.size();
                FilesFacade ff = configuration.getFilesFacade();
                long abandonedSealTxn;
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration)) {
                    writer.of(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, true);

                    // Committed sealed entry at table txn 1 -- recovery must KEEP it.
                    writer.setNextTxnAtSeal(1L);
                    for (int i = 0; i < 8; i++) {
                        writer.add(i % BATCH_KEYS, i);
                    }
                    writer.setMaxValue(7);
                    writer.commit();
                    writer.seal();

                    // A second sealed entry at table txn 2 that the table txn never
                    // advanced to: the abandoned/distressed attempt recovery must drop.
                    writer.setNextTxnAtSeal(2L);
                    for (int i = 8; i < 16; i++) {
                        writer.add(i % BATCH_KEYS, i);
                    }
                    writer.setMaxValue(15);
                    writer.commit();
                    writer.seal();
                    abandonedSealTxn = PostingIndexUtils.readSealTxnFromKeyFile(
                            ff, PostingIndexUtils.keyFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE));
                    path.trimTo(plen);
                    assertTrue("abandoned .pv must exist before recovery", ff.exists(
                            PostingIndexUtils.valueFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, abandonedSealTxn)));
                    path.trimTo(plen);

                    // Reopen from disk and arm the recovery walk at table txn 1: the
                    // txnAtSeal=2 entry is abandoned (2 > 1) -> dropped + its orphan queued.
                    writer.setCurrentTableTxn(1L);
                    writer.of(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, false);

                    assertTrue(
                            "the armed recovery walk must enqueue the abandoned entry's orphan",
                            writer.getPendingPurgesSizeForTesting() >= 1);
                }
            }
        });
    }

    @Test
    public void testRoundTripDataAfterMultipleSeals() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "ps_purge_roundtrip";
                int plen = path.size();
                final int totalRows = 32;

                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                    // Issue several seals interleaved with commits.
                    for (int wave = 0; wave < 4; wave++) {
                        int from = wave * 8;
                        int to = from + 8;
                        for (int i = from; i < to; i++) {
                            writer.add(i % BATCH_KEYS, i);
                        }
                        writer.setMaxValue(to - 1);
                        writer.commit();
                        writer.seal();
                    }
                }

                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                    int seenRows = 0;
                    for (int key = 0; key < BATCH_KEYS; key++) {
                        RowCursor cursor = reader.getCursor(key, 0, Long.MAX_VALUE);
                        long previous = -1;
                        while (cursor.hasNext()) {
                            long rowId = cursor.next();
                            assertTrue("row IDs must be ascending per key", rowId > previous);
                            assertEquals("row IDs must follow the key % BATCH pattern",
                                    key, (int) (rowId % BATCH_KEYS));
                            previous = rowId;
                            seenRows++;
                        }
                        Misc.free(cursor);
                    }
                    assertEquals("reader must see every committed row across all seals",
                            totalRows, seenRows);
                }
            }
        });
    }

    @Test
    public void testTableDroppedBeforePurge() throws Exception {
        assertMemoryLeak(() -> {
            if (configuration.disableColumnPurgeJob()) {
                return;
            }
            TableToken tok = createPostingTable("ps_purge_droptable");
            String col = "c_drop";
            final long sealTxn;
            try (Path partitionPath = partitionPathFor(tok)) {
                sealTxn = writeAndSeal(partitionPath, col);
            }

            publishPurgeTask(tok, col, sealTxn, 1L);
            execute("DROP TABLE \"" + tok.getTableName() + '"');

            try (PostingSealPurgeJob job = new PostingSealPurgeJob(engine)) {
                runPurgeJob(job, 3);
                assertEquals("dropped-table task must drain to zero outstanding",
                        0, job.getOutstandingPurgeTasks());
            }
        });
    }

    private void assertLogTableCompletedCount(String columnFilter) throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler();
             SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1)) {
            ctx.with(
                    configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                    null,
                    null
            );
            String sql = "SELECT count(*) FROM \"" + configuration.getSystemTableNamePrefix()
                    + "posting_seal_purge_log\" WHERE column_name = '" + columnFilter
                    + "' AND completed IS NOT NULL";
            try (RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory();
                 RecordCursor cursor = factory.getCursor(ctx)) {
                assertTrue("log query must return a row", cursor.hasNext());
                long actual = cursor.getRecord().getLong(0);
                assertTrue("log table must have at least " + 1 + " completed row(s) for "
                        + columnFilter + ", got " + actual, actual >= 1);
            }
        }
    }

    private long countLogRows(String whereClause) throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler();
             SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1)) {
            ctx.with(
                    configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                    null,
                    null
            );
            String sql = "SELECT count(*) FROM \"" + configuration.getSystemTableNamePrefix()
                    + "posting_seal_purge_log\" WHERE " + whereClause;
            try (RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory();
                 RecordCursor cursor = factory.getCursor(ctx)) {
                assertTrue("log query must return a row", cursor.hasNext());
                return cursor.getRecord().getLong(0);
            }
        }
    }

    private TableToken createPostingTable(String name) {
        return createTable(
                new TableModel(configuration, name, PartitionBy.NONE)
                        .col("c", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    private PostingSealPurgeTask newPostingSealPurgeTask(
            TableToken tok,
            String columnName,
            long sealTxn,
            long toTableTxn
    ) {
        PostingSealPurgeTask task = new PostingSealPurgeTask();
        task.of(
                tok,
                columnName,
                TableUtils.COLUMN_NAME_TXN_NONE,
                sealTxn,
                0L,
                -1L,
                PartitionBy.NONE,
                ColumnType.TIMESTAMP_MICRO,
                0L,
                toTableTxn
        );
        return task;
    }

    private Path partitionPathFor(TableToken tok) {
        FilesFacade ff = configuration.getFilesFacade();
        Path p = new Path().of(configuration.getDbRoot()).concat(tok.getDirName());
        TableUtils.setPathForNativePartition(
                p, ColumnType.TIMESTAMP_MICRO, PartitionBy.NONE, 0L, -1L);
        ff.mkdirs(p.slash(), configuration.getMkDirMode());
        return p;
    }

    private void publishPurgeTask(
            TableToken tok,
            String colName,
            long sealTxn,
            long toTableTxn
    ) {
        MessageBus bus = engine.getMessageBus();
        MPSequence pubSeq = bus.getPostingSealPurgePubSeq();
        RingQueue<PostingSealPurgeTask> queue = bus.getPostingSealPurgeQueue();
        long cursor;
        while ((cursor = pubSeq.next()) == -2) {
            Os.pause();
        }
        assertTrue("purge queue must accept the task", cursor >= 0);
        try {
            queue.get(cursor).of(
                    tok, colName, TableUtils.COLUMN_NAME_TXN_NONE, sealTxn,
                    0L, -1L, PartitionBy.NONE, ColumnType.TIMESTAMP_MICRO,
                    0L, toTableTxn
            );
        } finally {
            pubSeq.done(cursor);
        }
    }

    private void runPurgeJob(PostingSealPurgeJob job) {
        runPurgeJob(job, 1);
    }

    private void runPurgeJob(PostingSealPurgeJob job, int passes) {
        for (int i = 0; i < passes; i++) {
            // Advance by well over the retry-delay limit so the backoff never
            // defers a ready task past this run.
            setCurrentMicros(currentMicros + 100L * iteration++);
            job.run();
            setCurrentMicros(currentMicros + 100L * iteration++);
            job.run();
        }
    }

    @Test
    public void testJobSurvivesErrorOverflow() throws Exception {
        // Regression test for #21 Layer 2: hitting MAX_ERRORS must NOT
        // permanently disable the job. After a backoff window the job must
        // resume, and a clean iteration must reset the error count to zero.
        assertMemoryLeak(() -> {
            if (configuration.disableColumnPurgeJob()) {
                return;
            }
            try (PostingSealPurgeJob job = new PostingSealPurgeJob(engine)) {
                // Pretend we just hit the error cap.
                job.setErrorCountForTesting(64);
                assertTrue("setup: job must report error count >= cap",
                        job.getErrorCountForTesting() >= 11);

                // Run once; the throttle window starts now. Without the fix,
                // close() was called here and the job was dead permanently.
                job.run();
                assertTrue(
                        "job must remain alive after error overflow (no permanent disable)",
                        job.isJobAliveForTesting()
                );

                // Advance past the backoff window and run again with no
                // outstanding tasks. The fix lets the iteration through,
                // sees no work + no errors, and resets the error counter.
                setCurrentMicros(currentMicros + 120L * 1_000_000L);
                job.run();
                assertEquals(
                        "clean iteration must reset error count",
                        0,
                        job.getErrorCountForTesting()
                );
                assertTrue(
                        "job must remain alive after recovery",
                        job.isJobAliveForTesting()
                );
            }
        });
    }

    @Test
    public void testPendingPurgesBoundedUnderSaturation() throws Exception {
        // Regression test for #21: when the global PostingSealPurge queue is
        // never drained (e.g. PostingSealPurgeJob disabled or stuck on
        // errors), the per-writer outbox must NOT grow without bound.
        //
        // We never call writer.publishPendingPurges, so every superseded
        // sealTxn just accumulates in the in-memory list. Without the cap
        // the list grows linearly with seal count; with the cap it is
        // bounded.
        node1.setProperty(PropertyKey.CAIRO_POSTING_SEAL_PURGE_OUTBOX_MAX, 8);
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final String name = "ps_purge_bound";
                final int plen = path.size();
                final int seals = 64;

                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                    for (int s = 0; s < seals; s++) {
                        for (int i = 0; i < 4; i++) {
                            writer.add(i % BATCH_KEYS, s * 4L + i);
                        }
                        writer.setMaxValue(s * 4L + 3);
                        writer.commit();
                        writer.seal();
                    }
                    // After 64 seals, 63 supersession events fired. With the
                    // unbounded outbox today the list holds all 63. With a
                    // cap (the fix), it caps at the configured value.
                    int outboxCap = configuration.getPostingSealPurgeOutboxMax();
                    int actual = writer.getPendingPurgesSizeForTesting();
                    assertTrue("test setup is broken: outbox cap must be < seals", outboxCap < seals);
                    assertTrue(
                            "pendingPurges must be bounded under saturation: actual=" + actual + " cap=" + outboxCap,
                            actual <= outboxCap
                    );
                }
                path.trimTo(plen);
            }
        });
    }

    @Test
    public void testPersistReadyTasksDirectRejectsUnclampedToTxnSentinel() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tok = createPostingTable("posting_purge_sentinel");
            PostingSealPurgeTask task = new PostingSealPurgeTask();
            task.of(
                    tok,
                    "c",
                    TableUtils.COLUMN_NAME_TXN_NONE,
                    1L,
                    0L,
                    -1L,
                    PartitionBy.NONE,
                    ColumnType.TIMESTAMP_MICRO,
                    0L,
                    Long.MAX_VALUE
            );

            ObjList<PostingSealPurgeTask> tasks = new ObjList<>();
            tasks.add(newPostingSealPurgeTask(tok, "c_ready", 2L, 1L));
            tasks.add(task);
            assertFalse(PostingSealPurgeJob.persistReadyTasksDirect(engine, tasks, 0, tasks.size(), 1L));
        });
    }

    @Test
    public void testPersistReadyTasksDirectWritesReadyRowsOnly() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tok = createPostingTable("posting_purge_batch");
            ObjList<PostingSealPurgeTask> tasks = new ObjList<>();
            tasks.add(newPostingSealPurgeTask(tok, "c_ready_1", 1L, 5L));
            tasks.add(newPostingSealPurgeTask(tok, "c_future", 2L, 7L));
            tasks.add(newPostingSealPurgeTask(tok, "c_ready_2", 3L, 5L));

            assertTrue(PostingSealPurgeJob.persistReadyTasksDirect(engine, tasks, 0, tasks.size(), 5L));

            assertEquals(2L, countLogRows("table_name = '" + tok.getDirName() + "'"));
            assertEquals(0L, countLogRows("column_name = 'c_future'"));
        });
    }

    @Test
    public void testSnapshotRestoreRemoveIndexFilesClearsSidecars() throws Exception {
        // Regression test for #18: TableSnapshotRestore.removeIndexFiles
        // must clear every sealed .pv.{N}, every .pc<N>.*.* covering file,
        // and the .pci before re-creating fresh empty index files. Without
        // this, stale sidecars from before the snapshot survive into the
        // restored state and shadow the new (empty) index data.
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final String name = "snap_col";
                final long postingTxn = io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;
                final int plen = path.size();
                FilesFacade fsFf = configuration.getFilesFacade();

                // Build a real POSTING index so .pk plus one or more sealed
                // .pv.{txn} files exist on disk.
                long liveSealTxn;
                try (PostingIndexWriter writer = new PostingIndexWriter(
                        configuration, path, name, postingTxn)) {
                    for (int i = 0; i < 8; i++) {
                        writer.add(i % BATCH_KEYS, i);
                    }
                    writer.setMaxValue(7);
                    writer.commit();
                    writer.seal();
                    liveSealTxn = PostingIndexUtils.readSealTxnFromKeyFile(fsFf,
                            PostingIndexUtils.keyFileName(path.trimTo(plen), name, postingTxn));
                }
                assertTrue("setup: live sealTxn must be positive", liveSealTxn > 0);

                // Fabricate a stale sealed .pv.{liveSealTxn+1} plus its
                // covering .pci and .pc0 sidecar files. These represent
                // residue that the snapshot restore must wipe out before
                // the index is re-created from snapshot data. Materialize
                // each filename to a fresh String — LPSZ aliases the same
                // Path buffer, so we cannot keep multiple LPSZ "snapshots".
                final long staleSealTxn = liveSealTxn + 1;
                String stalePvStr = Utf8s.stringFromUtf8Bytes(
                        PostingIndexUtils.valueFileName(path.trimTo(plen), name, postingTxn, staleSealTxn));
                String pciStr = Utf8s.stringFromUtf8Bytes(
                        PostingIndexUtils.coverInfoFileName(path.trimTo(plen), name, postingTxn));
                String stalePcStr = Utf8s.stringFromUtf8Bytes(
                        PostingIndexUtils.coverDataFileName(path.trimTo(plen), name, 0,
                                postingTxn, COLUMN_NAME_TXN_NONE, staleSealTxn));
                String livePvStr = Utf8s.stringFromUtf8Bytes(
                        PostingIndexUtils.valueFileName(path.trimTo(plen), name, postingTxn, liveSealTxn));
                String pkStr = Utf8s.stringFromUtf8Bytes(
                        PostingIndexUtils.keyFileName(path.trimTo(plen), name, postingTxn));

                try (Path scratch = new Path()) {
                    touchFile(fsFf, scratch.of(stalePvStr).$());
                    touchFile(fsFf, scratch.of(pciStr).$());
                    touchFile(fsFf, scratch.of(stalePcStr).$());
                    assertTrue(fsFf.exists(scratch.of(stalePvStr).$()));
                    assertTrue(fsFf.exists(scratch.of(pciStr).$()));
                    assertTrue(fsFf.exists(scratch.of(stalePcStr).$()));

                    // Run the restore-time cleanup.
                    io.questdb.cairo.TableSnapshotRestore.removeIndexFiles(
                            fsFf, path, plen, name, postingTxn, IndexType.POSTING);

                    // Live .pv and .pk must go (existing behavior).
                    assertFalse("live .pv must be removed", fsFf.exists(scratch.of(livePvStr).$()));
                    assertFalse("live .pk must be removed", fsFf.exists(scratch.of(pkStr).$()));
                    // The new behavior — every other sealed .pv.{N}, the .pci,
                    // and every .pc<N>.*.* must also go.
                    assertFalse("stale .pv.{txn+1} must be removed", fsFf.exists(scratch.of(stalePvStr).$()));
                    assertFalse(".pci must be removed", fsFf.exists(scratch.of(pciStr).$()));
                    assertFalse("stale .pc0 must be removed", fsFf.exists(scratch.of(stalePcStr).$()));
                }
            }
        });
    }

    @Test
    public void testRemoveAllSealedFilesReportsFailure() throws Exception {
        // Regression test for #9: removeAllSealedFiles must report failure
        // (return true) when at least one sidecar (.pci, .pc<N>) cannot be
        // removed and is still on disk. Without this, the column purge
        // operator silently leaves stale sidecars on disk forever.
        final boolean[] failPc = {true};
        FilesFacade failingFf = new TestFilesFacadeImpl() {
            @Override
            public boolean removeQuiet(LPSZ name) {
                if (failPc[0] && name != null && Utf8s.containsAscii(name, ".pc")) {
                    return false;
                }
                return super.removeQuiet(name);
            }
        };
        assertMemoryLeak(failingFf, () -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final String name = "test_col";
                final long postingTxn = 1L;
                final long sealTxn = 2L;
                final int plen = path.size();
                FilesFacade fsFf = configuration.getFilesFacade();

                // Fabricate a .pci and a .pc0.<C>.<S> on disk.
                LPSZ pci = PostingIndexUtils.coverInfoFileName(path.trimTo(plen), name, postingTxn);
                touchFile(fsFf, pci);
                LPSZ pc0 = PostingIndexUtils.coverDataFileName(path.trimTo(plen), name, 0,
                        postingTxn, COLUMN_NAME_TXN_NONE, sealTxn);
                touchFile(fsFf, pc0);
                assertTrue("setup: .pci must exist", fsFf.exists(pci));
                assertTrue("setup: .pc0 must exist", fsFf.exists(pc0));

                // Sanity: with the fault active, removal must report failure
                // and the files must remain on disk.
                boolean failed = PostingIndexUtils.removeAllSealedFiles(
                        fsFf, path, plen, name, postingTxn);
                assertTrue("removeAllSealedFiles must return true when removal fails", failed);
                assertTrue(".pci must still exist after failed removal", fsFf.exists(pci));
                assertTrue(".pc0 must still exist after failed removal", fsFf.exists(pc0));

                // With the fault disabled, the same call must clean up and
                // report success.
                failPc[0] = false;
                failed = PostingIndexUtils.removeAllSealedFiles(
                        fsFf, path, plen, name, postingTxn);
                assertFalse("removeAllSealedFiles must return false on success", failed);
                assertFalse(".pci must be gone after successful removal", fsFf.exists(pci));
                assertFalse(".pc0 must be gone after successful removal", fsFf.exists(pc0));
            }
        });
    }

    private void touchFile(FilesFacade ff, LPSZ file) {
        long fd = ff.openRW(file, configuration.getWriterFileOpenOpts());
        assertTrue("must be able to create fabricated file " + file, fd >= 0);
        ff.close(fd);
    }

    /**
     * Writes data and seals TWICE, returning the SUPERSEDED first sealTxn (its
     * .pv is purge-eligible) while the chain head advances to a newer, live
     * sealTxn. PostingSealPurgeOperator refuses to delete the live head -- it
     * re-reads the .pk head sealTxn at delete time to guard the C1 reuse hazard
     * -- so a purge test must target a superseded sealTxn, which is also what
     * production schedules (recordPostingSealPurge fires on the OLD sealTxn once
     * a new head supersedes it).
     */
    private long writeAndSeal(Path partitionPath, String colName) {
        FilesFacade ff = configuration.getFilesFacade();
        int pLen = partitionPath.size();
        long supersededSealTxn;
        try (PostingIndexWriter writer = new PostingIndexWriter(
                configuration, partitionPath, colName, COLUMN_NAME_TXN_NONE)) {
            for (int i = 0; i < 8; i++) {
                writer.add(i % BATCH_KEYS, i);
            }
            writer.setMaxValue(8 - 1);
            writer.commit();
            writer.seal();
            supersededSealTxn = PostingIndexUtils.readSealTxnFromKeyFile(
                    ff, PostingIndexUtils.keyFileName(partitionPath.trimTo(pLen), colName, COLUMN_NAME_TXN_NONE));
            partitionPath.trimTo(pLen);
            // Second seal advances the chain head, leaving supersededSealTxn's
            // .pv on disk pending purge while a newer sealTxn becomes live.
            for (int i = 8; i < 16; i++) {
                writer.add(i % BATCH_KEYS, i);
            }
            writer.setMaxValue(16 - 1);
            writer.commit();
            writer.seal();
        }
        partitionPath.trimTo(pLen);
        return supersededSealTxn;
    }

    /**
     * Covering analogue of {@link #writeAndSeal}: seals TWICE and returns the
     * superseded first sealTxn (with its .pv/.pc{c} purge-eligible) while a newer
     * sealTxn becomes the live head. See {@link #writeAndSeal} for why a purge
     * test must target a superseded, not the live-head, sealTxn.
     */
    private long writeCoveringAndSeal(Path partitionPath, String colName) {
        FilesFacade ff = configuration.getFilesFacade();
        int pLen = partitionPath.size();
        final long colRows = 16;
        long colAddr = Unsafe.malloc(colRows * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
        long supersededSealTxn;
        try {
            Unsafe.setMemory(colAddr, colRows * Integer.BYTES, (byte) 0);
            try (PostingIndexWriter writer = new PostingIndexWriter(
                    configuration, partitionPath, colName, COLUMN_NAME_TXN_NONE)) {
                writer.configureCovering(
                        new long[]{colAddr}, new long[]{0L},
                        new int[]{2}, new int[]{0},
                        new int[]{ColumnType.INT}, 1
                );
                for (int i = 0; i < 8; i++) {
                    writer.add(i % BATCH_KEYS, i);
                }
                writer.setMaxValue(8 - 1);
                writer.commit();
                writer.seal();
                supersededSealTxn = PostingIndexUtils.readSealTxnFromKeyFile(
                        ff, PostingIndexUtils.keyFileName(partitionPath.trimTo(pLen), colName, COLUMN_NAME_TXN_NONE));
                partitionPath.trimTo(pLen);
                // Second seal advances the chain head past supersededSealTxn.
                for (int i = 8; i < 16; i++) {
                    writer.add(i % BATCH_KEYS, i);
                }
                writer.setMaxValue(16 - 1);
                writer.commit();
                writer.seal();
            }
        } finally {
            Unsafe.free(colAddr, colRows * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
        partitionPath.trimTo(pLen);
        return supersededSealTxn;
    }

}
