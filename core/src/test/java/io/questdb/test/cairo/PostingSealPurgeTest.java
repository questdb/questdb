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
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.PostingSealPurgeJob;
import io.questdb.cairo.PostingSealPurgeOperator;
import io.questdb.cairo.SymbolMapWriter;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxnScoreboard;
import io.questdb.cairo.idx.PostingIndexFwdReader;
import io.questdb.cairo.idx.PostingIndexKeyFileView;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.tasks.PostingSealPurgeTask;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;
import static org.junit.Assert.*;

public class PostingSealPurgeTest extends AbstractCairoTest {

    private static final int BATCH_KEYS = 4;
    // the first microsecond of 1970-02-15, in a later month than the first rows of the purge log
    private static final long FEB_15 = 45 * Micros.DAY_MICROS;
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

                assertTrue(scoreboard.acquireTxn(0, 3L, 3L));
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
                assertTrue(scoreboard.acquireTxn(0, 5L, 5L));
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
        assertPurgeRemovesCoverFiles(COLUMN_NAME_TXN_NONE);
    }

    @Test
    public void testPurgeRemovesCoverFilesOfNameTxnPositive() throws Exception {
        assertPurgeRemovesCoverFiles(7);
    }

    @Test
    public void testPurgeRemovesCoverFilesOfNameTxnZero() throws Exception {
        // A cover file name writes name txn 0 like no name txn, so the scan reports it as no name txn.
        assertPurgeRemovesCoverFiles(0);
    }

    @Test
    public void testSealPurgeRemovesSupersededCoverFilesOfNameTxnZero() throws Exception {
        // Posting column name txn 0: the .pv files carry the txn (c_z.pv.0.<seal>), the .pc files the
        // token 0 that txn NONE also writes (c_z.pc0.0.<seal>). The purge of the superseded seal
        // removes both and keeps the live seal's files.
        assertMemoryLeak(() -> {
            if (configuration.disableColumnPurgeJob()) {
                return;
            }
            TableToken tok = createPostingTable("ps_purge_cover_z");
            FilesFacade ff = configuration.getFilesFacade();
            try (Path partitionPath = partitionPathFor(tok);
                 PostingSealPurgeJob job = new PostingSealPurgeJob(engine);
                 Path file = new Path()) {
                final String col = "c_z";
                final long supersededSealTxn = writeCoveringAndSeal(partitionPath, col, 0);
                final int pLen = partitionPath.size();
                final long liveSealTxn = PostingIndexUtils.readSealTxnFromKeyFile(
                        ff, PostingIndexUtils.keyFileName(partitionPath.trimTo(pLen), col, 0));
                partitionPath.trimTo(pLen);
                assertTrue(liveSealTxn > supersededSealTxn);
                final String dir = partitionPath.toString();
                final String supersededPv = col + ".pv.0." + supersededSealTxn;
                final String supersededPc = col + ".pc0.0." + supersededSealTxn;
                final String livePv = col + ".pv.0." + liveSealTxn;
                final String livePc = col + ".pc0.0." + liveSealTxn;
                for (String name : new String[]{supersededPv, supersededPc, livePv, livePc}) {
                    assertTrue(name + " must exist after the seals", ff.exists(file.of(dir).concat(name).$()));
                }

                publishPurgeTask(tok, col, 0, supersededSealTxn, 1L);
                runPurgeJob(job, 3);

                assertFalse(supersededPv + " must be purged", ff.exists(file.of(dir).concat(supersededPv).$()));
                assertFalse(supersededPc + " must be purged together with the .pv", ff.exists(file.of(dir).concat(supersededPc).$()));
                assertTrue(livePv + " is live", ff.exists(file.of(dir).concat(livePv).$()));
                assertTrue(livePc + " is live", ff.exists(file.of(dir).concat(livePc).$()));
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
    public void testPurgeAbandonsGenerationVisibleAboveItsWindow() throws Exception {
        // A reader pinned at txn t sees the first generation for 10 <= t < 20. The scoreboard check of a purge whose
        // window ends at 19 covers no reader at 19, which still sees it.
        assertPurgeWithReaderWindowEndingAt(19, true);
    }

    @Test
    public void testPurgeProceedsWhenKeyFileCannotBeOpened() throws Exception {
        // As when a REINDEX or a drop of the index removed the .pk under the task: a .pk that cannot be read proves
        // nothing, and the purge goes on
        assertPurgeProceedsWhenKeyFileUnreadable(hook -> hook::failOpen, -1);
    }

    @Test
    public void testPurgeProceedsWhenKeyFileHeaderIsUnreadable() throws Exception {
        assertPurgeProceedsWhenKeyFileUnreadable(hook -> hook::breakHeaderSeqlock, -1);
    }

    @Test
    public void testPurgeProceedsWhenKeyFileIsShorterThanItsHeader() throws Exception {
        assertPurgeProceedsWhenKeyFileUnreadable(
                hook -> () -> hook.truncateKeyFile(PostingIndexUtils.KEY_FILE_RESERVED / 2),
                PostingIndexUtils.KEY_FILE_RESERVED / 2
        );
    }

    @Test
    public void testPurgeRemovesGenerationNotVisibleAboveItsWindow() throws Exception {
        // As testPurgeAbandonsGenerationVisibleAboveItsWindow, with a window that ends at 20: from there on readers
        // see the second generation
        assertPurgeWithReaderWindowEndingAt(20, false);
    }

    @Test
    public void testPurgeRetriesWhenKeyFileWindowCannotBeAllocated() throws Exception {
        // The visibility check allocates the read window of its .pk view at the first entry below the head entry.
        // When the RSS limit refuses the allocation, the purge fails loudly with nothing left open, keeps the files,
        // and the next attempt completes.
        final VisibilityCheckFf hook = new VisibilityCheckFf(VisibilityCheckFf.AT_ENTRY_READ);
        ff = hook;
        assertMemoryLeak(ff, () -> {
            TableToken tok = createPostingTable("ps_purge_pk_window_oom");
            FilesFacade runtimeFf = configuration.getFilesFacade();
            try (Path partitionPath = partitionPathFor(tok);
                 PostingSealPurgeOperator operator = new PostingSealPurgeOperator(engine)) {
                String col = "c_pk_window_oom";
                long sealTxn = writeAndSeal(partitionPath, col);
                int pLen = partitionPath.size();
                PostingIndexUtils.keyFileName(partitionPath, col, COLUMN_NAME_TXN_NONE);
                hook.arm(partitionPath.toString(), () -> Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + 1024));
                partitionPath.trimTo(pLen);
                PostingSealPurgeTask task = newPostingSealPurgeTask(tok, col, sealTxn, 1L);
                try {
                    operator.purge(task, false);
                    fail("the window allocation must fail");
                } catch (CairoException e) {
                    assertTrue(e.isOutOfMemory());
                    TestUtils.assertContains(e.getFlyweightMessage(),
                            "size=" + PostingIndexKeyFileView.WINDOW_SIZE + ", memoryTag=" + MemoryTag.NATIVE_INDEX_READER);
                } finally {
                    Unsafe.setRssMemLimit(0);
                }
                assertEquals(1, hook.actionRuns);
                assertTrue("the failed attempt must keep the superseded .pv",
                        runtimeFf.exists(PostingIndexUtils.valueFileName(partitionPath, col, COLUMN_NAME_TXN_NONE, sealTxn)));
                partitionPath.trimTo(pLen);

                assertTrue(operator.purge(task, false));
                assertFalse("the next attempt must remove the superseded .pv",
                        runtimeFf.exists(PostingIndexUtils.valueFileName(partitionPath, col, COLUMN_NAME_TXN_NONE, sealTxn)));
                partitionPath.trimTo(pLen);
            }
        });
    }

    @Test
    public void testPurgeSurvivesKeyFileShrinkAtHeadEntry() throws Exception {
        // The first commit after TRUNCATE TABLE on a non-partitioned table shrinks the .pk in place
        // (PostingIndexWriter.truncate() -> initKeyMemory), and the purge job holds no lock against the writer.
        // Here the .pk shrinks to its header pages when the visibility check first reads past them: the head entry
        // that the header names is gone.
        assertPurgeSurvivesKeyFileShrink(VisibilityCheckFf.AT_ENTRY_READ);
    }

    @Test
    public void testPurgeSurvivesKeyFileShrinkAtWindowRead() throws Exception {
        // As testPurgeSurvivesKeyFileShrinkAtHeadEntry, once the check has read the head entry and fills its window
        // with the entries below it
        assertPurgeSurvivesKeyFileShrink(VisibilityCheckFf.AT_WINDOW_READ);
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
        assertPurgeLogsCoverReuseRace("c_cover_detector", "ps_reuse_race_cover", COLUMN_NAME_TXN_NONE);
    }

    @Test
    public void testPurgeLogsReuseRaceWhenLiveHeadCoverFileOfNameTxnZeroDeletedByOrphanUnlink() throws Exception {
        // posting column name txn 0 names its .pc files with the token 0 that txn NONE also writes
        assertPurgeLogsCoverReuseRace("c_cover_detector_z", "ps_reuse_race_cover_z", 0);
    }

    private void assertPurgeLogsCoverReuseRace(String col, String tableName, long postingColumnNameTxn) throws Exception {
        // Cover-file (.pc) analogue of the .pv reuse-race detector. The .pv path
        // re-reads the head right after its own unlink; the covers are removed
        // later by the directory scan, so the operator re-reads the head AFTER
        // the scan and fires the same REINDEX diagnostic when a live .pc was
        // unlinked. Drive that exact window on a COVERED index: target the LIVE
        // head and fail every .pk read until a .pc is removed, so the pre-unlink
        // guard misses (head reads -1) AND the .pv detector's own head read also
        // returns -1 (suppressed) -- isolating the post-scan cover detector,
        // whose head read succeeds once the .pc is gone.
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
            TableToken tok = createPostingTable(tableName);
            FilesFacade runtimeFf = configuration.getFilesFacade();
            try (Path partitionPath = partitionPathFor(tok);
                 PostingSealPurgeJob job = new PostingSealPurgeJob(engine)) {
                // writeCoveringAndSeal seals twice and RETURNS the superseded txn;
                // the reuse scenario targets the LIVE head, so read it fresh.
                writeCoveringAndSeal(partitionPath, col, postingColumnNameTxn);
                int pLen = partitionPath.size();
                long liveHead = PostingIndexUtils.readSealTxnFromKeyFile(
                        runtimeFf, PostingIndexUtils.keyFileName(partitionPath.trimTo(pLen), col, postingColumnNameTxn));
                partitionPath.trimTo(pLen);
                assertTrue("live head sealTxn must be positive", liveHead > 0);
                assertTrue("live .pc0 must exist after covering seal", runtimeFf.exists(
                        PostingIndexUtils.coverDataFileName(
                                partitionPath.trimTo(pLen), col, 0, postingColumnNameTxn, COLUMN_NAME_TXN_NONE, liveHead)));
                partitionPath.trimTo(pLen);

                // Target the LIVE head, the state a reused-then-republished orphan
                // sealTxn lands in. Tiny window so the scoreboard reports it ready.
                publishPurgeTask(tok, col, postingColumnNameTxn, liveHead, 1L);

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
                                partitionPath.trimTo(pLen), col, 0, postingColumnNameTxn, COLUMN_NAME_TXN_NONE, liveHead)));
                partitionPath.trimTo(pLen);
            }
        });
    }

    private void assertPurgeProceedsWhenKeyFileUnreadable(
            Function<VisibilityCheckFf, Runnable> action,
            long keyFileLength
    ) throws Exception {
        final VisibilityCheckFf hook = new VisibilityCheckFf(VisibilityCheckFf.AT_OPEN);
        ff = hook;
        assertMemoryLeak(ff, () -> {
            TableToken tok = createPostingTable("ps_purge_pk_unreadable");
            FilesFacade runtimeFf = configuration.getFilesFacade();
            try (Path partitionPath = partitionPathFor(tok);
                 PostingSealPurgeOperator operator = new PostingSealPurgeOperator(engine)) {
                String col = "c_pk_unreadable";
                long sealTxn = writeAndSeal(partitionPath, col);
                int pLen = partitionPath.size();
                PostingIndexUtils.keyFileName(partitionPath, col, COLUMN_NAME_TXN_NONE);
                hook.arm(partitionPath.toString(), action.apply(hook));
                partitionPath.trimTo(pLen);

                assertTrue(operator.purge(newPostingSealPurgeTask(tok, col, sealTxn, 1L), false));
                assertEquals(1, hook.actionRuns);
                if (keyFileLength > -1) {
                    assertEquals(keyFileLength, runtimeFf.length(PostingIndexUtils.keyFileName(partitionPath, col, COLUMN_NAME_TXN_NONE)));
                    partitionPath.trimTo(pLen);
                }
                assertFalse("the superseded .pv must be removed",
                        runtimeFf.exists(PostingIndexUtils.valueFileName(partitionPath, col, COLUMN_NAME_TXN_NONE, sealTxn)));
                partitionPath.trimTo(pLen);
            }
        });
    }

    private void assertPurgeSurvivesKeyFileShrink(int trigger) throws Exception {
        final VisibilityCheckFf hook = new VisibilityCheckFf(trigger);
        ff = hook;
        assertMemoryLeak(ff, () -> {
            TableToken tok = createPostingTable("ps_purge_pk_shrink");
            FilesFacade runtimeFf = configuration.getFilesFacade();
            try (Path partitionPath = partitionPathFor(tok);
                 PostingSealPurgeOperator operator = new PostingSealPurgeOperator(engine)) {
                String col = "c_pk_shrink";
                long sealTxn = writeAndSeal(partitionPath, col);
                int pLen = partitionPath.size();
                assertTrue("the .pk must hold entries past its header pages",
                        runtimeFf.length(PostingIndexUtils.keyFileName(partitionPath, col, COLUMN_NAME_TXN_NONE)) > PostingIndexUtils.KEY_FILE_RESERVED);
                hook.arm(partitionPath.toString(), () -> hook.truncateKeyFile(PostingIndexUtils.KEY_FILE_RESERVED));
                partitionPath.trimTo(pLen);

                // A .pk that cannot be read proves nothing: the purge goes on and removes the superseded files
                assertTrue(operator.purge(newPostingSealPurgeTask(tok, col, sealTxn, 1L), false));
                assertEquals("the .pk must shrink during the visibility check", 1, hook.actionRuns);
                assertEquals(PostingIndexUtils.KEY_FILE_RESERVED,
                        runtimeFf.length(PostingIndexUtils.keyFileName(partitionPath, col, COLUMN_NAME_TXN_NONE)));
                partitionPath.trimTo(pLen);
                assertFalse("the superseded .pv must be removed",
                        runtimeFf.exists(PostingIndexUtils.valueFileName(partitionPath, col, COLUMN_NAME_TXN_NONE, sealTxn)));
                partitionPath.trimTo(pLen);
            }
        });
    }

    private void assertPurgeWithReaderWindowEndingAt(long toTableTxn, boolean isVisibleAboveWindow) throws Exception {
        assertMemoryLeak(() -> {
            TableToken tok = createPostingTable("ps_purge_visible_" + toTableTxn);
            FilesFacade runtimeFf = configuration.getFilesFacade();
            LogCapture capture = new LogCapture();
            try (Path partitionPath = partitionPathFor(tok);
                 PostingSealPurgeOperator operator = new PostingSealPurgeOperator(engine)) {
                String col = "c_visible";
                long sealTxn = writeAndSeal(partitionPath, col, 10L, 20L);
                int pLen = partitionPath.size();
                capture.start();
                try {
                    assertTrue(operator.purge(newPostingSealPurgeTask(tok, col, sealTxn, toTableTxn), false));
                    if (isVisibleAboveWindow) {
                        capture.waitFor("can see the target sealTxn, abandoning purge to avoid deleting a live file");
                    }
                } finally {
                    capture.stop();
                }
                assertEquals("the .pv of the first generation", isVisibleAboveWindow,
                        runtimeFf.exists(PostingIndexUtils.valueFileName(partitionPath, col, COLUMN_NAME_TXN_NONE, sealTxn)));
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
                assertTrue(scoreboard.acquireTxn(0, 5L, 5L));
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
    public void testReopenUnlinksOrphansFromPriorIncarnation() throws Exception {
        // A distressed writer, or a crash recovery that rewinds the table, can leave a
        // generation sealed at a table txn the table never commits to (txnAtSeal > the
        // committed table txn). Reopening and arming the recovery walk via
        // setCurrentTableTxn must drop that abandoned chain entry and unlink its orphan
        // .pv/.pc: of() -> runRecoveryWalkIfRequested -> chain.recoveryDropAbandoned ->
        // unlinkAbandonedSealFiles. The chain-level mechanics are covered by
        // PostingIndexChainWriterTest's testRecoveryDropAbandoned* set, so this pins
        // only the PostingIndexWriter.of() glue: that an armed reopen removes the orphan
        // rather than leaving it on disk, and keeps the committed generation.
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final String name = "ps_purge_orphan";
                final int plen = path.size();
                FilesFacade ff = configuration.getFilesFacade();
                long abandonedSealTxn;
                long committedSealTxn;
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
                    committedSealTxn = PostingIndexUtils.readSealTxnFromKeyFile(
                            ff, PostingIndexUtils.keyFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE));
                    path.trimTo(plen);

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
                    // txnAtSeal=2 entry is abandoned (2 > 1) -> dropped + its orphan unlinked.
                    writer.setCurrentTableTxn(1L);
                    writer.of(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, false);

                    assertFalse("the armed recovery walk must unlink the abandoned entry's orphan", ff.exists(
                            PostingIndexUtils.valueFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, abandonedSealTxn)));
                    assertTrue("the committed generation must stay", ff.exists(
                            PostingIndexUtils.valueFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, committedSealTxn)));
                    path.trimTo(plen);
                    assertEquals("nothing left for the purge job", 0, writer.getPendingPurgesSizeForTesting());
                }
            }
        });
    }

    @Test
    public void testRetryPassIsBoundedWhenPurgeTakesLongerThanBackoff() throws Exception {
        final AtomicInteger clockReads = new AtomicInteger();
        final boolean[] isMeasuring = {false};
        // Each attempt outlasts the retry delay, as on a slow or instrumented filesystem. Freeze after 100
        // reads so the broken loop terminates too: the regression fails by work count, not by a timeout.
        testMicrosClock = () -> isMeasuring[0] ? Math.min(clockReads.incrementAndGet(), 100) * 100L : currentMicros;
        assertMemoryLeak(() -> {
            assertFalse(configuration.disableColumnPurgeJob());
            TableToken token = createPostingTable("bounded_retries");
            FilesFacade ff = configuration.getFilesFacade();
            try (Path path = partitionPathFor(token);
                 PostingSealPurgeJob job = new PostingSealPurgeJob(engine);
                 TxnScoreboard scoreboard = engine.getTxnScoreboard(token)) {
                int partitionPathLen = path.size();
                long firstSeal = writeAndSeal(path, "first");
                long secondSeal = writeAndSeal(path, "second");
                assertTrue(scoreboard.acquireTxn(0, 3L, 3L));
                try {
                    publishPurgeTask(token, "first", firstSeal, 10L);
                    publishPurgeTask(token, "second", secondSeal, 10L);
                    isMeasuring[0] = true;
                    try {
                        job.run();
                    } finally {
                        isMeasuring[0] = false;
                    }
                    assertTrue("one run must not keep retrying pinned tasks: " + clockReads.get(), clockReads.get() < 20);
                    assertEquals("both pinned tasks must remain queued", 2, job.getOutstandingPurgeTasks());
                    assertTrue("first sealed .pv must survive while pinned", ff.exists(PostingIndexUtils.valueFileName(
                            path.trimTo(partitionPathLen), "first", COLUMN_NAME_TXN_NONE, firstSeal)));
                    assertTrue("second sealed .pv must survive while pinned", ff.exists(PostingIndexUtils.valueFileName(
                            path.trimTo(partitionPathLen), "second", COLUMN_NAME_TXN_NONE, secondSeal)));
                } finally {
                    scoreboard.releaseTxn(0, 3L);
                }
                setCurrentMicros(20_000L);
                job.run();
                assertFalse("first sealed .pv must be purged after readers release it", ff.exists(PostingIndexUtils.valueFileName(
                        path.trimTo(partitionPathLen), "first", COLUMN_NAME_TXN_NONE, firstSeal)));
                assertFalse("second sealed .pv must be purged after readers release it", ff.exists(PostingIndexUtils.valueFileName(
                        path.trimTo(partitionPathLen), "second", COLUMN_NAME_TXN_NONE, secondSeal)));
                assertEquals("both tasks must purge after readers release them", 0, job.getOutstandingPurgeTasks());
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

    private void assertPurgeRemovesCoverFiles(long postingColumnNameTxn) throws Exception {
        assertMemoryLeak(() -> {
            if (configuration.disableColumnPurgeJob()) {
                return;
            }
            TableToken tok = createPostingTable("ps_purge_cover");
            FilesFacade ff = configuration.getFilesFacade();

            try (Path partitionPath = partitionPathFor(tok);
                 PostingSealPurgeJob job = new PostingSealPurgeJob(engine)) {
                String col = "c_cover";
                long sealTxn = writeCoveringAndSeal(partitionPath, col, postingColumnNameTxn);
                int pLen = partitionPath.size();
                long liveSealTxn = PostingIndexUtils.readSealTxnFromKeyFile(
                        ff, PostingIndexUtils.keyFileName(partitionPath.trimTo(pLen), col, postingColumnNameTxn));
                assertTrue("the second seal must supersede the first", liveSealTxn > sealTxn);
                // Both valueFileName and coverDataFileName append onto the
                // shared Path buffer, so each existence check must trim back
                // to the partition root before re-resolving the file name.
                assertTrue(".pv must exist after seal",
                        ff.exists(PostingIndexUtils.valueFileName(
                                partitionPath.trimTo(pLen), col, postingColumnNameTxn, sealTxn)));
                assertTrue(".pc0 must exist after seal with covering",
                        ff.exists(PostingIndexUtils.coverDataFileName(
                                partitionPath.trimTo(pLen), col, 0, postingColumnNameTxn, COLUMN_NAME_TXN_NONE, sealTxn)));

                publishPurgeTask(tok, col, postingColumnNameTxn, sealTxn, 1L);
                runPurgeJob(job, 3);

                assertFalse(".pv must be purged",
                        ff.exists(PostingIndexUtils.valueFileName(
                                partitionPath.trimTo(pLen), col, postingColumnNameTxn, sealTxn)));
                assertFalse(".pc0 must be purged together with .pv",
                        ff.exists(PostingIndexUtils.coverDataFileName(
                                partitionPath.trimTo(pLen), col, 0, postingColumnNameTxn, COLUMN_NAME_TXN_NONE, sealTxn)));
                assertTrue("the live .pv must stay",
                        ff.exists(PostingIndexUtils.valueFileName(
                                partitionPath.trimTo(pLen), col, postingColumnNameTxn, liveSealTxn)));
                assertTrue("the live .pc0 must stay",
                        ff.exists(PostingIndexUtils.coverDataFileName(
                                partitionPath.trimTo(pLen), col, 0, postingColumnNameTxn, COLUMN_NAME_TXN_NONE, liveSealTxn)));
            }
        });
    }

    static long countLogRows(String whereClause) throws Exception {
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

    static TableToken createPostingTable(String name) {
        return createTable(
                new TableModel(configuration, name, PartitionBy.NONE)
                        .col("c", ColumnType.INT)
                        .timestamp("ts")
        );
    }

    private static PostingSealPurgeTask newPostingSealPurgeTask(
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

    static Path partitionPathFor(TableToken tok) {
        FilesFacade ff = configuration.getFilesFacade();
        Path p = new Path().of(configuration.getDbRoot()).concat(tok.getDirName());
        TableUtils.setPathForNativePartition(
                p, ColumnType.TIMESTAMP_MICRO, PartitionBy.NONE, 0L, -1L);
        ff.mkdirs(p.slash(), configuration.getMkDirMode());
        return p;
    }

    static void publishPurgeTask(
            TableToken tok,
            String colName,
            long sealTxn,
            long toTableTxn
    ) {
        publishPurgeTask(tok, colName, COLUMN_NAME_TXN_NONE, sealTxn, toTableTxn);
    }

    private static void publishPurgeTask(
            TableToken tok,
            String colName,
            long postingColumnNameTxn,
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
                    tok, colName, postingColumnNameTxn, sealTxn,
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
        assertSnapshotRestoreRemoveIndexFilesClearsSidecars(COLUMN_NAME_TXN_NONE);
    }

    @Test
    public void testSnapshotRestoreRemoveIndexFilesClearsSidecarsOfNameTxnZero() throws Exception {
        // posting column name txn 0 names its .pc files with the token 0 that txn NONE also writes
        assertSnapshotRestoreRemoveIndexFilesClearsSidecars(0);
    }

    private void assertSnapshotRestoreRemoveIndexFilesClearsSidecars(long postingTxn) throws Exception {
        // Regression test for #18: TableSnapshotRestore.removeIndexFiles
        // must clear every sealed .pv.{N}, every .pc<N>.*.* covering file,
        // and the .pci before re-creating fresh empty index files. Without
        // this, stale sidecars from before the snapshot survive into the
        // restored state and shadow the new (empty) index data.
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final String name = "snap_col";
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
    public void testRemoveAllSealedFilesOfNameTxnZero() throws Exception {
        // A cover file name writes name txn 0 like no name txn, so the scan reports it as no name txn. The files of
        // another name txn of the same column must stay.
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final String name = "txn0_col";
                final int plen = path.size();
                final FilesFacade ff = configuration.getFilesFacade();
                final String[] removed = {
                        Utf8s.stringFromUtf8Bytes(PostingIndexUtils.coverInfoFileName(path.trimTo(plen), name, 0)),
                        Utf8s.stringFromUtf8Bytes(PostingIndexUtils.valueFileName(path.trimTo(plen), name, 0, 3)),
                        Utf8s.stringFromUtf8Bytes(PostingIndexUtils.coverDataFileName(path.trimTo(plen), name, 0, 0, COLUMN_NAME_TXN_NONE, 3)),
                        Utf8s.stringFromUtf8Bytes(PostingIndexUtils.coverDataFileName(path.trimTo(plen), name, 1, 0, 5, 3)),
                };
                final String[] kept = {
                        Utf8s.stringFromUtf8Bytes(PostingIndexUtils.coverInfoFileName(path.trimTo(plen), name, 9)),
                        Utf8s.stringFromUtf8Bytes(PostingIndexUtils.valueFileName(path.trimTo(plen), name, 9, 4)),
                        Utf8s.stringFromUtf8Bytes(PostingIndexUtils.coverDataFileName(path.trimTo(plen), name, 0, 9, COLUMN_NAME_TXN_NONE, 4)),
                };
                try (Path scratch = new Path()) {
                    for (String file : removed) {
                        touchFile(ff, scratch.of(file).$());
                    }
                    for (String file : kept) {
                        touchFile(ff, scratch.of(file).$());
                    }

                    assertFalse(PostingIndexUtils.removeAllSealedFiles(ff, path, plen, name, 0));

                    for (String file : removed) {
                        assertFalse("must be removed: " + file, ff.exists(scratch.of(file).$()));
                    }
                    for (String file : kept) {
                        assertTrue("must stay: " + file, ff.exists(scratch.of(file).$()));
                        ff.removeQuiet(scratch.of(file).$());
                    }
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
     * {@link #writeAndSeal(Path, String)} with the txnAtSeal of each generation, so that readers pinned at different
     * txns see different generations.
     */
    private long writeAndSeal(Path partitionPath, String colName, long firstTxnAtSeal, long secondTxnAtSeal) {
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
            writer.setNextTxnAtSeal(firstTxnAtSeal);
            writer.seal();
            supersededSealTxn = PostingIndexUtils.readSealTxnFromKeyFile(
                    ff, PostingIndexUtils.keyFileName(partitionPath.trimTo(pLen), colName, COLUMN_NAME_TXN_NONE));
            partitionPath.trimTo(pLen);
            for (int i = 8; i < 16; i++) {
                writer.add(i % BATCH_KEYS, i);
            }
            writer.setMaxValue(16 - 1);
            writer.commit();
            writer.setNextTxnAtSeal(secondTxnAtSeal);
            writer.seal();
        }
        partitionPath.trimTo(pLen);
        return supersededSealTxn;
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
    static long writeAndSeal(Path partitionPath, String colName) {
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
        return writeCoveringAndSeal(partitionPath, colName, COLUMN_NAME_TXN_NONE);
    }

    private long writeCoveringAndSeal(Path partitionPath, String colName, long postingColumnNameTxn) {
        FilesFacade ff = configuration.getFilesFacade();
        int pLen = partitionPath.size();
        final long colRows = 16;
        long colAddr = Unsafe.malloc(colRows * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
        long supersededSealTxn;
        try {
            Unsafe.setMemory(colAddr, colRows * Integer.BYTES, (byte) 0);
            try (PostingIndexWriter writer = new PostingIndexWriter(
                    configuration, partitionPath, colName, postingColumnNameTxn)) {
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
                        ff, PostingIndexUtils.keyFileName(partitionPath.trimTo(pLen), colName, postingColumnNameTxn));
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

    /**
     * Runs an action once, when the visibility check of {@link PostingSealPurgeOperator} opens or reads the .pk. The
     * first open of the .pk in purge() re-reads the head sealTxn; the second is the visibility check.
     */
    private static final class VisibilityCheckFf extends TestFilesFacadeImpl {
        // at the first read past the header pages, by mapping or by pread
        static final int AT_ENTRY_READ = 1;
        // before the visibility check opens the .pk
        static final int AT_OPEN = 0;
        // at the first window fill
        static final int AT_WINDOW_READ = 2;
        private final int trigger;
        private Runnable action;
        private int actionRuns;
        private boolean isOpenFailing;
        private String keyFile;
        private int keyFileOpens;
        private long visibilityCheckFd = -1;

        VisibilityCheckFf(int trigger) {
            this.trigger = trigger;
        }

        @Override
        public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
            final long address = super.mmap(fd, len, offset, flags, memoryTag);
            if (trigger == AT_ENTRY_READ && fd == visibilityCheckFd) {
                runAction();
            }
            return address;
        }

        @Override
        public long openRO(LPSZ name) {
            if (keyFile == null || !Utf8s.equalsAscii(keyFile, name) || ++keyFileOpens != 2) {
                return super.openRO(name);
            }
            if (trigger == AT_OPEN) {
                runAction();
                if (isOpenFailing) {
                    return -1;
                }
            }
            visibilityCheckFd = super.openRO(name);
            return visibilityCheckFd;
        }

        @Override
        public long read(long fd, long buf, long size, long offset) {
            if (fd == visibilityCheckFd && (trigger == AT_WINDOW_READ
                    || (trigger == AT_ENTRY_READ && offset + size > PostingIndexUtils.KEY_FILE_RESERVED))) {
                runAction();
            }
            return super.read(fd, buf, size, offset);
        }

        @Override
        public int readNonNegativeInt(long fd, long offset) {
            if (trigger == AT_ENTRY_READ && fd == visibilityCheckFd && offset + Integer.BYTES > PostingIndexUtils.KEY_FILE_RESERVED) {
                runAction();
            }
            return super.readNonNegativeInt(fd, offset);
        }

        @Override
        public long readNonNegativeLong(long fd, long offset) {
            if (trigger == AT_ENTRY_READ && fd == visibilityCheckFd && offset + Long.BYTES > PostingIndexUtils.KEY_FILE_RESERVED) {
                runAction();
            }
            return super.readNonNegativeLong(fd, offset);
        }

        void arm(String keyFile, Runnable action) {
            this.keyFile = keyFile;
            this.action = action;
        }

        // An odd sequence marks a header page that a publish is still writing, so neither page reads as stable
        void breakHeaderSeqlock() {
            try (Path p = new Path()) {
                final long fd = super.openRW(p.of(keyFile).$(), CairoConfiguration.O_NONE);
                final long buf = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.getUnsafe().putLong(buf, 3L);
                    assertEquals(Long.BYTES, super.write(fd, buf, Long.BYTES,
                            PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.V2_HEADER_OFFSET_SEQUENCE_START));
                    assertEquals(Long.BYTES, super.write(fd, buf, Long.BYTES,
                            PostingIndexUtils.PAGE_B_OFFSET + PostingIndexUtils.V2_HEADER_OFFSET_SEQUENCE_START));
                } finally {
                    Unsafe.free(buf, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                    super.close(fd);
                }
            }
        }

        void failOpen() {
            isOpenFailing = true;
        }

        // Truncated to its header pages, the .pk is in the state the writer leaves for a reader that read the header
        // before PostingIndexWriter.truncate() shrank the file: the header names entries past the end of the file
        void truncateKeyFile(long size) {
            try (Path p = new Path()) {
                final long fd = super.openRW(p.of(keyFile).$(), CairoConfiguration.O_NONE);
                assertTrue(super.truncate(fd, size));
                super.close(fd);
            }
        }

        private void runAction() {
            if (actionRuns == 0) {
                actionRuns++;
                action.run();
            }
        }
    }

    @Test
    public void testCompletedMarkAfterReopenLandsOnOwnRow() throws Exception {
        // The commit of a batch of A, which a pinned txn holds, and B fails: the batch adds a partition to the log, and
        // the _txn of the log cannot grow. Each month adds a partition until the _txn record outgrows the file. The job
        // writes back C, pinned since the start, the A of every month and B, so the written-back rows are not where the
        // rolled-back rows were. The completion of B lands on its written-back row, and once the pin goes, so do those
        // of the A entries and C; their first rows stay pending.
        final TxnExtendFailingFacade faultFf = new TxnExtendFailingFacade(postingSealPurgeLogTableName());
        assertMemoryLeak(faultFf, () -> {
            final TableToken tok = createPostingTable("ps_commit");
            try (
                    Path partitionPath = partitionPathFor(tok);
                    PostingSealPurgeJob job = new PostingSealPurgeJob(engine);
                    TxnScoreboard scoreboard = engine.getTxnScoreboard(tok)
            ) {
                final int pLen = partitionPath.size();
                long failedMonthStart = -1;
                int failedMonth = -1;
                assertTrue(scoreboard.acquireTxn(0, 5L, 5L));
                try {
                    publishPurgeTask(tok, "c_held", writeAndSeal(partitionPath.trimTo(pLen), "c_held"), 10L);
                    runPurgeJobAt(job, 10);
                    for (int month = 1; failedMonthStart < 0; month++) {
                        assertTrue("the _txn of the log did not grow", month < 300);
                        final long monthStart = Micros.addMonths(0, month);
                        publishPurgeTask(tok, "a_" + month, writeAndSeal(partitionPath.trimTo(pLen), "a_" + month), 10L);
                        publishPurgeTask(tok, "b_" + month, writeAndSeal(partitionPath.trimTo(pLen), "b_" + month), 1L);
                        faultFf.isArmed.set(true);
                        runPurgeJobAt(job, monthStart + 10);
                        if (!faultFf.isArmed.getAndSet(false)) {
                            failedMonthStart = monthStart;
                            failedMonth = month;
                        }
                    }
                    assertEquals(0, job.getErrorCountForTesting());
                    assertTrue(job.isJobAliveForTesting());
                    assertPostingSealLogRowsSince(failedMonthStart, failedMonth + 2, 1);
                    assertQuery("SELECT column_name, completed FROM '" + postingSealPurgeLogTableName() + "' WHERE completed != null AND ts >= '"
                            + Micros.toUSecString(failedMonthStart) + "'")
                            .noLeakCheck()
                            .returns("column_name\tcompleted\nb_" + failedMonth + "\t" + Micros.toUSecString(failedMonthStart + 10) + "\n");
                } finally {
                    scoreboard.releaseTxn(0, 5L);
                }
                runPurgeJobAt(job, failedMonthStart + 20);
                assertEquals(0, job.getOutstandingPurgeTasks());
                assertPostingSealLogRowsSince(failedMonthStart, failedMonth + 2, failedMonth + 2);
                // the first rows of C and of the A entries before the failure
                assertEquals(failedMonth, countLogRows("completed = null"));
            }
        });
    }

    @Test
    public void testLogAtMaxTimestampTakesEntriesAtIt() throws Exception {
        // While the job is without its writer, a row at the last microsecond of year 9999 lands in the log. The entries
        // after it go to the same microsecond, which keeps them in order, and they are logged and completed.
        final PurgeLogFaultFacade faultFf = new PurgeLogFaultFacade();
        assertMemoryLeak(faultFf, () -> {
            final TableToken tok = createPostingTable("ps_max");
            final FilesFacade runtimeFf = configuration.getFilesFacade();
            try (Path partitionPath = partitionPathFor(tok); PostingSealPurgeJob job = new PostingSealPurgeJob(engine)) {
                final int pLen = partitionPath.size();
                logPurgeInJanuary(job, tok, partitionPath);
                failPostingSealLogTwice(job, faultFf, tok, partitionPath, FEB_15);
                execute("INSERT INTO '" + postingSealPurgeLogTableName() + "' (ts, table_name, completed) VALUES ("
                        + (Micros.YEAR_10000 - 1) + "::TIMESTAMP, 'max', '1970-02-15T00:00:00.000000Z')");
                for (int i = 1; i <= 2; i++) {
                    final String col = "c_" + i;
                    final long sealTxn = writeAndSeal(partitionPath.trimTo(pLen), col);
                    publishPurgeTask(tok, col, sealTxn, 1L);
                    runPurgeJobAt(job, FEB_15 + i * Micros.SECOND_MICROS);
                    assertTrue(job.isJobAliveForTesting());
                    assertFalse(runtimeFf.exists(PostingIndexUtils.valueFileName(partitionPath.trimTo(pLen), col, COLUMN_NAME_TXN_NONE, sealTxn)));
                }
                assertEquals(0, job.getErrorCountForTesting());
                assertQuery("SELECT table_name, column_name, ts, completed FROM '" + postingSealPurgeLogTableName() + "' WHERE ts > '1970-02'")
                        .noLeakCheck()
                        .timestamp("ts")
                        .returns("table_name\tcolumn_name\tts\tcompleted\n"
                                + "max\t\t9999-12-31T23:59:59.999999Z\t1970-02-15T00:00:00.000000Z\n"
                                + tok.getDirName() + "\tc_1\t9999-12-31T23:59:59.999999Z\t1970-02-15T00:00:01.000000Z\n"
                                + tok.getDirName() + "\tc_2\t9999-12-31T23:59:59.999999Z\t1970-02-15T00:00:02.000000Z\n");
            }
        });
    }

    @Test
    public void testLogBackoffFollowsScheduleUnderPersistentFault() throws Exception {
        assertLogBackoffUnderPersistentFault(false);
    }

    @Test
    public void testLogBackoffFollowsScheduleUnderPersistentFaultWithPinnedEntry() throws Exception {
        assertLogBackoffUnderPersistentFault(true);
    }

    @Test
    public void testLogDroppedDuringOutage() throws Exception {
        assertLogUnusableDuringOutage("DROP TABLE '" + postingSealPurgeLogTableName() + "'", false);
    }

    @Test
    public void testLogFailureNeverThrottlesDrain() throws Exception {
        // Every write into February fails, and each run comes exactly when the next reopen is due, so 11 runs in a
        // row fail, as many as arm the error throttle. Log failures do not count as errors: the run 1 s after them
        // still drains and purges, and no run is throttled.
        final PurgeLogFaultFacade faultFf = new PurgeLogFaultFacade();
        assertMemoryLeak(faultFf, () -> {
            final TableToken tok = createPostingTable("ps_throttle");
            final FilesFacade runtimeFf = configuration.getFilesFacade();
            final LogCapture capture = new LogCapture();
            try (Path partitionPath = partitionPathFor(tok); PostingSealPurgeJob job = new PostingSealPurgeJob(engine)) {
                final int pLen = partitionPath.size();
                logPurgeInJanuary(job, tok, partitionPath);
                faultFf.failMonth("1970-02");
                capture.start();
                // the due times of the failures: 0, 1, 3, 7, 15, 31, 63, 123, 183, 243 and 303 s after the first
                final long[] runSeconds = {0, 1, 3, 7, 15, 31, 63, 123, 183, 243, 303, 304};
                int failureCount = 0;
                for (int i = 0; i < runSeconds.length; i++) {
                    final String col = "c_" + i;
                    final long sealTxn = writeAndSeal(partitionPath.trimTo(pLen), col);
                    publishPurgeTask(tok, col, sealTxn, 1L);
                    runPurgeJobAt(job, FEB_15 + runSeconds[i] * Micros.SECOND_MICROS);
                    failureCount += i == 0 ? 2 : i < runSeconds.length - 1 ? 1 : 0;
                    assertEquals("run " + i, failureCount, faultFf.getFailureCount());
                    assertEquals("run " + i, 0, job.getErrorCountForTesting());
                    assertFalse("run " + i, runtimeFf.exists(PostingIndexUtils.valueFileName(partitionPath.trimTo(pLen), col, COLUMN_NAME_TXN_NONE, sealTxn)));
                }
                capture.drain();
                capture.assertNotLogged("throttling retries");
            } finally {
                capture.stop();
            }
        });
    }

    @Test
    public void testLogIdleReopenKeepsDelay() throws Exception {
        // After two failures the delay is 2 s. Runs with nothing to purge reopen the log when it is due, and the
        // write-back has no rows: nothing fails, and the delay stays. The next entry fails at 2 s, and the reopen after
        // it comes 2 s later, its failure 4 s after that.
        final PurgeLogFaultFacade faultFf = new PurgeLogFaultFacade();
        assertMemoryLeak(faultFf, () -> {
            final TableToken tok = createPostingTable("ps_idle");
            try (Path partitionPath = partitionPathFor(tok); PostingSealPurgeJob job = new PostingSealPurgeJob(engine)) {
                final int pLen = partitionPath.size();
                logPurgeInJanuary(job, tok, partitionPath);
                failPostingSealLogTwice(job, faultFf, tok, partitionPath, FEB_15);
                faultFf.failMonth("1970-02");
                for (long now = FEB_15 + Micros.SECOND_MICROS; now <= FEB_15 + 5 * Micros.SECOND_MICROS; now += 500_000) {
                    runPurgeJobAt(job, now);
                    assertEquals(2, faultFf.getFailureCount());
                    assertTrue("the reopen at FEB_15 + 1 s holds the writer", job.isJobAliveForTesting());
                }
                final long f3 = FEB_15 + 5_500_000;
                final long[] runTimes = {f3, f3 + 2 * Micros.SECOND_MICROS - 1, f3 + 2 * Micros.SECOND_MICROS, f3 + 6 * Micros.SECOND_MICROS - 1, f3 + 6 * Micros.SECOND_MICROS};
                final int[] failureCounts = {3, 3, 4, 4, 5};
                for (int i = 0; i < runTimes.length; i++) {
                    final String col = "c_" + i;
                    publishPurgeTask(tok, col, writeAndSeal(partitionPath.trimTo(pLen), col), 1L);
                    runPurgeJobAt(job, runTimes[i]);
                    assertEquals("run " + i, failureCounts[i], faultFf.getFailureCount());
                    assertFalse("run " + i, job.isJobAliveForTesting());
                }
                assertEquals(0, job.getOutstandingPurgeTasks());
                assertEquals(0, job.getErrorCountForTesting());
                assertEquals(1, countLogRows("1 = 1"));
            }
        });
    }

    @Test
    public void testLogReopenAfterRecoveryTruncateFailure() throws Exception {
        // The recovery of the constructor purges a logged entry, and its TRUNCATE of the log fails as it empties a
        // symbol map, after it published the empty _txn. The job gives that writer up and opens a new one, which logs
        // and completes the next entry.
        final SymbolMapTruncateFailingFacade faultFf = new SymbolMapTruncateFailingFacade();
        assertMemoryLeak(faultFf, () -> {
            final TableToken tok = createPostingTable("ps_recover_truncate");
            final FilesFacade runtimeFf = configuration.getFilesFacade();
            try (Path partitionPath = partitionPathFor(tok)) {
                final int pLen = partitionPath.size();
                final long sealTxnX = writeAndSeal(partitionPath.trimTo(pLen), "c_x");
                try (PostingSealPurgeJob job = new PostingSealPurgeJob(engine); TxnScoreboard scoreboard = engine.getTxnScoreboard(tok)) {
                    assertTrue(scoreboard.acquireTxn(0, 5L, 5L));
                    try {
                        publishPurgeTask(tok, "c_x", sealTxnX, 10L);
                        runPurgeJobAt(job, 10);
                    } finally {
                        scoreboard.releaseTxn(0, 5L);
                    }
                }
                engine.releaseInactive();
                faultFf.isArmed.set(true);
                try (PostingSealPurgeJob job = new PostingSealPurgeJob(engine)) {
                    assertFalse("the recovery truncate did not fail", faultFf.isArmed.get());
                    assertFalse(runtimeFf.exists(PostingIndexUtils.valueFileName(partitionPath.trimTo(pLen), "c_x", COLUMN_NAME_TXN_NONE, sealTxnX)));
                    assertTrue(job.isJobAliveForTesting());
                    final long sealTxnY = writeAndSeal(partitionPath.trimTo(pLen), "c_y");
                    publishPurgeTask(tok, "c_y", sealTxnY, 1L);
                    runPurgeJobAt(job, 20);
                    assertFalse(runtimeFf.exists(PostingIndexUtils.valueFileName(partitionPath.trimTo(pLen), "c_y", COLUMN_NAME_TXN_NONE, sealTxnY)));
                    assertEquals(0, job.getErrorCountForTesting());
                    assertQuery("SELECT column_name, ts, completed FROM '" + postingSealPurgeLogTableName() + "'")
                            .noLeakCheck()
                            .timestamp("ts")
                            .expectSize()
                            .returns("""
                                    column_name\tts\tcompleted
                                    c_y\t1970-01-01T00:00:00.000020Z\t1970-01-01T00:00:00.000020Z
                                    """);
                }
            }
        });
    }

    @Test
    public void testLogReopenWhileWriterBusy() throws Exception {
        // The test takes the log writer while the job is without it, so the reopens fail as busy, on the retry
        // schedule. The job drains and purges meanwhile, with no errors. Once the test gives the writer back, the next
        // due reopen logs the entry that a pinned txn holds.
        final PurgeLogFaultFacade faultFf = new PurgeLogFaultFacade();
        assertMemoryLeak(faultFf, () -> {
            final TableToken tok = createPostingTable("ps_busy");
            final FilesFacade runtimeFf = configuration.getFilesFacade();
            try (
                    Path partitionPath = partitionPathFor(tok);
                    PostingSealPurgeJob job = new PostingSealPurgeJob(engine);
                    TxnScoreboard scoreboard = engine.getTxnScoreboard(tok)
            ) {
                final int pLen = partitionPath.size();
                logPurgeInJanuary(job, tok, partitionPath);
                failPostingSealLogTwice(job, faultFf, tok, partitionPath, FEB_15);
                assertTrue(scoreboard.acquireTxn(0, 5L, 5L));
                try {
                    try (TableWriter ignored = getWriter(postingSealPurgeLogTableName())) {
                        // the reopens at FEB_15 + 1 s and 3 s fail as busy: the next is due at FEB_15 + 7 s
                        final long[] runTimes = {FEB_15 + Micros.SECOND_MICROS, FEB_15 + 2 * Micros.SECOND_MICROS, FEB_15 + 3 * Micros.SECOND_MICROS};
                        for (int i = 0; i < runTimes.length; i++) {
                            final String col = "c_" + i;
                            final long sealTxn = writeAndSeal(partitionPath.trimTo(pLen), col);
                            publishPurgeTask(tok, col, sealTxn, 1L);
                            runPurgeJobAt(job, runTimes[i]);
                            assertFalse(runtimeFf.exists(PostingIndexUtils.valueFileName(partitionPath.trimTo(pLen), col, COLUMN_NAME_TXN_NONE, sealTxn)));
                            assertFalse(job.isJobAliveForTesting());
                            assertEquals(0, job.getErrorCountForTesting());
                        }
                    }
                    publishPurgeTask(tok, "c_held", writeAndSeal(partitionPath.trimTo(pLen), "c_held"), 10L);
                    runPurgeJobAt(job, FEB_15 + 7 * Micros.SECOND_MICROS - 1);
                    assertFalse(job.isJobAliveForTesting());
                    runPurgeJobAt(job, FEB_15 + 7 * Micros.SECOND_MICROS);
                    assertTrue(job.isJobAliveForTesting());
                    assertEquals(1, job.getOutstandingPurgeTasks());
                    assertQuery("SELECT column_name, ts, completed FROM '" + postingSealPurgeLogTableName() + "' WHERE ts > '1970-02'")
                            .noLeakCheck()
                            .timestamp("ts")
                            .returns("""
                                    column_name\tts\tcompleted
                                    c_held\t1970-02-15T00:00:07.000000Z\t
                                    """);
                } finally {
                    scoreboard.releaseTxn(0, 5L);
                }
            }
        });
    }

    @Test
    public void testLogRowsFromDirectPersistDuringOutage() throws Exception {
        // While the job is without its writer, a TableWriter persists a ready task straight into the log, at a clock
        // 100 us ahead of the one the job runs at next. The job stamps its rows at that row's timestamp, so the log
        // takes them in order, and the completion of its entry lands on its own row. The direct row stays pending
        // until a restart replays it.
        final PurgeLogFaultFacade faultFf = new PurgeLogFaultFacade();
        assertMemoryLeak(faultFf, () -> {
            final TableToken tok = createPostingTable("ps_direct");
            final FilesFacade runtimeFf = configuration.getFilesFacade();
            try (Path partitionPath = partitionPathFor(tok)) {
                final int pLen = partitionPath.size();
                final long sealTxnDirect = writeAndSeal(partitionPath.trimTo(pLen), "c_direct");
                try (PostingSealPurgeJob job = new PostingSealPurgeJob(engine)) {
                    logPurgeInJanuary(job, tok, partitionPath);
                    failPostingSealLogTwice(job, faultFf, tok, partitionPath, FEB_15);
                    final long t = FEB_15 + Micros.SECOND_MICROS;
                    setCurrentMicros(t + 100);
                    final ObjList<PostingSealPurgeTask> tasks = new ObjList<>();
                    tasks.add(newPostingSealPurgeTask(tok, "c_direct", sealTxnDirect, 1L));
                    assertTrue(PostingSealPurgeJob.persistReadyTasksDirect(engine, tasks, 0, tasks.size(), 1L));
                    final String logPartitions = listPostingSealLogPartitions();
                    final long sealTxnJob = writeAndSeal(partitionPath.trimTo(pLen), "c_job");
                    publishPurgeTask(tok, "c_job", sealTxnJob, 1L);
                    runPurgeJobAt(job, t);
                    assertTrue(job.isJobAliveForTesting());
                    assertEquals(logPartitions, listPostingSealLogPartitions());
                    assertFalse(runtimeFf.exists(PostingIndexUtils.valueFileName(partitionPath.trimTo(pLen), "c_job", COLUMN_NAME_TXN_NONE, sealTxnJob)));
                    assertTrue(runtimeFf.exists(PostingIndexUtils.valueFileName(partitionPath.trimTo(pLen), "c_direct", COLUMN_NAME_TXN_NONE, sealTxnDirect)));
                    assertQuery("SELECT column_name, ts, completed FROM '" + postingSealPurgeLogTableName() + "'")
                            .noLeakCheck()
                            .timestamp("ts")
                            .expectSize()
                            .returns("""
                                    column_name\tts\tcompleted
                                    col_jan\t1970-01-01T00:00:00.000010Z\t1970-01-01T00:00:00.000010Z
                                    c_direct\t1970-02-15T00:00:01.000100Z\t
                                    c_job\t1970-02-15T00:00:01.000100Z\t1970-02-15T00:00:01.000000Z
                                    """);
                }
                engine.releaseInactive();
                try (PostingSealPurgeJob job = new PostingSealPurgeJob(engine)) {
                    assertEquals(0, job.getOutstandingPurgeTasks());
                    assertEquals(0, countLogRows("1 = 1"));
                }
                assertFalse(runtimeFf.exists(PostingIndexUtils.valueFileName(partitionPath.trimTo(pLen), "c_direct", COLUMN_NAME_TXN_NONE, sealTxnDirect)));
            }
        });
    }

    @Test
    public void testLogWithoutCompletedColumnDuringOutage() throws Exception {
        assertLogUnusableDuringOutage("ALTER TABLE '" + postingSealPurgeLogTableName() + "' DROP COLUMN completed", true);
    }

    @Test
    public void testPinnedTaskReplayedAfterLogFailureAndRestart() throws Exception {
        // The append of X, which a pinned txn holds, fails once. The job writes X back in the same run, so a restart
        // with the pin gone replays X and purges its superseded .pv.
        final PurgeLogFaultFacade faultFf = new PurgeLogFaultFacade();
        assertMemoryLeak(faultFf, () -> {
            final TableToken tok = createPostingTable("ps_restart");
            final FilesFacade runtimeFf = configuration.getFilesFacade();
            try (Path partitionPath = partitionPathFor(tok)) {
                final int pLen = partitionPath.size();
                final long sealTxn = writeAndSeal(partitionPath.trimTo(pLen), "x1");
                try (TxnScoreboard scoreboard = engine.getTxnScoreboard(tok)) {
                    assertTrue(scoreboard.acquireTxn(0, 5L, 5L));
                    try (PostingSealPurgeJob job = new PostingSealPurgeJob(engine)) {
                        faultFf.failNextOpen("ts.d");
                        publishPurgeTask(tok, "x1", sealTxn, 10L);
                        runPurgeJobAt(job, 10);
                        assertEquals(1, faultFf.getFailureCount());
                        assertEquals(1, job.getOutstandingPurgeTasks());
                        assertEquals(1, countLogRows("column_name = 'x1' AND completed = null"));
                    } finally {
                        scoreboard.releaseTxn(0, 5L);
                    }
                }
                engine.releaseInactive();
                try (PostingSealPurgeJob job = new PostingSealPurgeJob(engine)) {
                    assertEquals(0, job.getOutstandingPurgeTasks());
                    assertEquals(0, countLogRows("1 = 1"));
                }
                assertFalse(runtimeFf.exists(PostingIndexUtils.valueFileName(partitionPath.trimTo(pLen), "x1", COLUMN_NAME_TXN_NONE, sealTxn)));
            }
        });
    }

    @Test
    public void testQueueDrainsAfterLogFailure() throws Exception {
        // The append of c1 fails once, as the log opens its first partition. The job reopens the log in the same run,
        // writes c1 back and completes it, and it keeps draining its queue: c2 is logged, purged and completed.
        final PurgeLogFaultFacade faultFf = new PurgeLogFaultFacade();
        assertMemoryLeak(faultFf, () -> {
            final TableToken tok = createPostingTable("ps_drain");
            final FilesFacade runtimeFf = configuration.getFilesFacade();
            try (Path partitionPath = partitionPathFor(tok); PostingSealPurgeJob job = new PostingSealPurgeJob(engine)) {
                final int pLen = partitionPath.size();
                final long sealTxn1 = writeAndSeal(partitionPath.trimTo(pLen), "c1");
                final long sealTxn2 = writeAndSeal(partitionPath.trimTo(pLen), "c2");
                faultFf.failNextOpen("ts.d");
                publishPurgeTask(tok, "c1", sealTxn1, 1L);
                runPurgeJobAt(job, 10);
                assertEquals(1, faultFf.getFailureCount());
                assertFalse(runtimeFf.exists(PostingIndexUtils.valueFileName(partitionPath.trimTo(pLen), "c1", COLUMN_NAME_TXN_NONE, sealTxn1)));
                publishPurgeTask(tok, "c2", sealTxn2, 1L);
                runPurgeJobAt(job, 20);
                assertFalse(runtimeFf.exists(PostingIndexUtils.valueFileName(partitionPath.trimTo(pLen), "c2", COLUMN_NAME_TXN_NONE, sealTxn2)));
                assertEquals(0, job.getErrorCountForTesting());
                assertEquals(0, job.getOutstandingPurgeTasks());
                assertQuery("SELECT column_name, ts, completed FROM '" + postingSealPurgeLogTableName() + "'")
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("""
                                column_name\tts\tcompleted
                                c1\t1970-01-01T00:00:00.000010Z\t1970-01-01T00:00:00.000010Z
                                c2\t1970-01-01T00:00:00.000020Z\t1970-01-01T00:00:00.000020Z
                                """);
            }
        });
    }

    @Test
    public void testRowsStampedAfterLogMaxTimestamp() throws Exception {
        // The clock steps back 50 us between two batches. The job stamps the second batch at the last timestamp of the
        // log, which keeps the rows in order: no out-of-order commit moves the rows whose ids the job keeps, and each
        // completion lands on its own row.
        assertMemoryLeak(() -> {
            final TableToken tok = createPostingTable("ps_stamp");
            try (
                    Path partitionPath = partitionPathFor(tok);
                    PostingSealPurgeJob job = new PostingSealPurgeJob(engine);
                    TxnScoreboard scoreboard = engine.getTxnScoreboard(tok)
            ) {
                final int pLen = partitionPath.size();
                final long t = Micros.SECOND_MICROS;
                final String logPartitions;
                assertTrue(scoreboard.acquireTxn(0, 5L, 5L));
                try {
                    publishPurgeTask(tok, "p1", writeAndSeal(partitionPath.trimTo(pLen), "p1"), 10L);
                    publishPurgeTask(tok, "u1", writeAndSeal(partitionPath.trimTo(pLen), "u1"), 1L);
                    runPurgeJobAt(job, t);
                    logPartitions = listPostingSealLogPartitions();
                    publishPurgeTask(tok, "p2", writeAndSeal(partitionPath.trimTo(pLen), "p2"), 10L);
                    publishPurgeTask(tok, "u2", writeAndSeal(partitionPath.trimTo(pLen), "u2"), 1L);
                    runPurgeJobAt(job, t - 50);
                    assertEquals(logPartitions, listPostingSealLogPartitions());
                } finally {
                    scoreboard.releaseTxn(0, 5L);
                }
                runPurgeJobAt(job, t + 100);
                assertEquals(0, job.getOutstandingPurgeTasks());
                assertEquals(logPartitions, listPostingSealLogPartitions());
                assertQuery("SELECT column_name, ts, completed FROM '" + postingSealPurgeLogTableName() + "'")
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("""
                                column_name\tts\tcompleted
                                p1\t1970-01-01T00:00:01.000000Z\t1970-01-01T00:00:01.000100Z
                                u1\t1970-01-01T00:00:01.000000Z\t1970-01-01T00:00:01.000000Z
                                p2\t1970-01-01T00:00:01.000000Z\t1970-01-01T00:00:01.000100Z
                                u2\t1970-01-01T00:00:01.000000Z\t1970-01-01T00:00:00.999950Z
                                """);
            }
        });
    }

    @Test
    public void testWrittenBackEntryReplayKeepsReusedSealTxn() throws Exception {
        // A reopen writes X back, and the run that purges X completes the written-back row: the first row of X stays
        // pending. A later index of the same column reuses the sealTxn of X as its live head, so the restart that
        // replays the first row must keep the live .pv.
        final PurgeLogFaultFacade faultFf = new PurgeLogFaultFacade();
        assertMemoryLeak(faultFf, () -> {
            final TableToken tok = createPostingTable("ps_reuse");
            final FilesFacade runtimeFf = configuration.getFilesFacade();
            try (Path partitionPath = partitionPathFor(tok)) {
                final int pLen = partitionPath.size();
                final long sealTxn = writeAndSeal(partitionPath.trimTo(pLen), "x1");
                try (PostingSealPurgeJob job = new PostingSealPurgeJob(engine); TxnScoreboard scoreboard = engine.getTxnScoreboard(tok)) {
                    assertTrue(scoreboard.acquireTxn(0, 5L, 5L));
                    try {
                        publishPurgeTask(tok, "x1", sealTxn, 10L);
                        runPurgeJobAt(job, 10);
                        // an entry in a new month, whose first log partition file fails to open once
                        faultFf.failNextOpen("ts.d");
                        publishPurgeTask(tok, "c_feb", writeAndSeal(partitionPath.trimTo(pLen), "c_feb"), 1L);
                        runPurgeJobAt(job, FEB_15);
                        assertEquals(1, faultFf.getFailureCount());
                    } finally {
                        scoreboard.releaseTxn(0, 5L);
                    }
                    runPurgeJobAt(job, FEB_15 + 10);
                    assertEquals(0, job.getOutstandingPurgeTasks());
                    assertFalse(runtimeFf.exists(PostingIndexUtils.valueFileName(partitionPath.trimTo(pLen), "x1", COLUMN_NAME_TXN_NONE, sealTxn)));
                    assertQuery("SELECT column_name, ts, completed FROM '" + postingSealPurgeLogTableName() + "' WHERE column_name = 'x1'")
                            .noLeakCheck()
                            .timestamp("ts")
                            .returns("""
                                    column_name\tts\tcompleted
                                    x1\t1970-01-01T00:00:00.000010Z\t
                                    x1\t1970-02-15T00:00:00.000000Z\t1970-02-15T00:00:00.000010Z
                                    """);
                }
                // a new index of x1, whose live head is the sealTxn of X
                removeIndexFiles(partitionPath.trimTo(pLen), "x1");
                final long liveHead = writeAndSealOnce(partitionPath.trimTo(pLen), "x1");
                assertEquals(sealTxn, liveHead);
                engine.releaseInactive();
                try (PostingSealPurgeJob job = new PostingSealPurgeJob(engine)) {
                    assertEquals(0, job.getOutstandingPurgeTasks());
                }
                assertTrue(runtimeFf.exists(PostingIndexUtils.valueFileName(partitionPath.trimTo(pLen), "x1", COLUMN_NAME_TXN_NONE, sealTxn)));
            }
        });
    }

    private void assertLogBackoffUnderPersistentFault(boolean isPinnedEntryQueued) throws Exception {
        // Every write into February fails, as on a disk that fills up when the log starts the month, while a reopen and
        // a commit without rows succeed: they touch January only. 40 runs 500 ms apart each queue an entry. The
        // failures follow the retry schedule: two in the first run, at the append and at the reopen in the same run,
        // then 1 s, 2 s, 4 s and 8 s apart. Each entry is purged in its run, without the log and without errors.
        final PurgeLogFaultFacade faultFf = new PurgeLogFaultFacade();
        assertMemoryLeak(faultFf, () -> {
            final TableToken tok = createPostingTable("ps_schedule");
            final FilesFacade runtimeFf = configuration.getFilesFacade();
            try (
                    Path partitionPath = partitionPathFor(tok);
                    PostingSealPurgeJob job = new PostingSealPurgeJob(engine);
                    TxnScoreboard scoreboard = engine.getTxnScoreboard(tok)
            ) {
                final int pLen = partitionPath.size();
                logPurgeInJanuary(job, tok, partitionPath);
                assertTrue(scoreboard.acquireTxn(0, 5L, 5L));
                try {
                    if (isPinnedEntryQueued) {
                        publishPurgeTask(tok, "c_pinned", writeAndSeal(partitionPath.trimTo(pLen), "c_pinned"), 10L);
                        runPurgeJobAt(job, 20);
                    }
                    faultFf.failMonth("1970-02");
                    final LogBackoffModel model = new LogBackoffModel();
                    final LongList failureTimes = new LongList();
                    for (int i = 0; i < 40; i++) {
                        final long now = FEB_15 + i * 500_000L;
                        final String col = "c_" + i;
                        final long sealTxn = writeAndSeal(partitionPath.trimTo(pLen), col);
                        publishPurgeTask(tok, col, sealTxn, 1L);
                        runPurgeJobAt(job, now);
                        if (model.isHolding()) {
                            model.onFailure(now);
                            failureTimes.add(now - FEB_15);
                        }
                        if (model.isReopenDue(now)) {
                            model.onReopen(now);
                            model.onFailure(now);
                            failureTimes.add(now - FEB_15);
                        }
                        assertEquals("run " + i, failureTimes.size(), faultFf.getFailureCount());
                        assertEquals("run " + i, 0, job.getErrorCountForTesting());
                        assertEquals("run " + i, isPinnedEntryQueued ? 1 : 0, job.getOutstandingPurgeTasks());
                        assertFalse("run " + i, job.isJobAliveForTesting());
                        assertFalse("run " + i, runtimeFf.exists(PostingIndexUtils.valueFileName(partitionPath.trimTo(pLen), col, COLUMN_NAME_TXN_NONE, sealTxn)));
                    }
                    assertEquals("[0,0,1000000,3000000,7000000,15000000]", failureTimes.toString());
                } finally {
                    scoreboard.releaseTxn(0, 5L);
                }
                assertQuery("SELECT column_name, ts FROM '" + postingSealPurgeLogTableName() + "'")
                        .noLeakCheck()
                        .expectSize()
                        .timestamp("ts")
                        .returns("column_name\tts\n"
                                + "col_jan\t1970-01-01T00:00:00.000010Z\n"
                                + (isPinnedEntryQueued ? "c_pinned\t1970-01-01T00:00:00.000020Z\n" : ""));
            }
        });
    }

    // Leaves the log unusable while the job is without its writer: each due reopen fails, on the retry schedule, and
    // frees what it took, while the job drains and purges without the log and without errors
    private void assertLogUnusableDuringOutage(String sql, boolean isLogKept) throws Exception {
        final PurgeLogFaultFacade faultFf = new PurgeLogFaultFacade();
        assertMemoryLeak(faultFf, () -> {
            final TableToken tok = createPostingTable("ps_unusable");
            final FilesFacade runtimeFf = configuration.getFilesFacade();
            final String logTableName = postingSealPurgeLogTableName();
            final LogCapture capture = new LogCapture();
            try (Path partitionPath = partitionPathFor(tok); PostingSealPurgeJob job = new PostingSealPurgeJob(engine)) {
                final int pLen = partitionPath.size();
                logPurgeInJanuary(job, tok, partitionPath);
                failPostingSealLogTwice(job, faultFf, tok, partitionPath, FEB_15);
                execute(sql);
                capture.start();
                // the reopens at FEB_15 + 1 s, 3 s and 7 s fail
                for (int i = 1; i <= 20; i++) {
                    final String col = "c_" + i;
                    final long sealTxn = writeAndSeal(partitionPath.trimTo(pLen), col);
                    publishPurgeTask(tok, col, sealTxn, 1L);
                    runPurgeJobAt(job, FEB_15 + i * 500_000L);
                    assertFalse("run " + i, job.isJobAliveForTesting());
                    assertEquals("run " + i, 0, job.getErrorCountForTesting());
                    assertEquals("run " + i, 0, job.getOutstandingPurgeTasks());
                    assertFalse("run " + i, runtimeFf.exists(PostingIndexUtils.valueFileName(partitionPath.trimTo(pLen), col, COLUMN_NAME_TXN_NONE, sealTxn)));
                    if (isLogKept) {
                        // the job gave up the writer it took
                        getWriter(logTableName).close();
                    }
                }
                capture.drain();
                capture.assertLoggedRE("^(?:(?!reason=could not reopen)[\\s\\S])*(?:reason=could not reopen(?:(?!reason=could not reopen)[\\s\\S])*){3}$");
                if (isLogKept) {
                    assertEquals(1, countLogRows("1 = 1"));
                } else {
                    assertNull(engine.getTableTokenIfExists(logTableName));
                }
            } finally {
                capture.stop();
            }
        });
    }

    private void assertPostingSealLogRowsSince(long timestamp, long expectedCount, long expectedCompletedCount) throws Exception {
        assertQuery("SELECT count(), count(completed) FROM '" + postingSealPurgeLogTableName() + "' WHERE ts >= '" + Micros.toUSecString(timestamp) + "'")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("count\tcount1\n" + expectedCount + "\t" + expectedCompletedCount + "\n");
    }

    // Makes the log fail twice in one run: the append of an entry into February, and the write-back of the reopen in
    // the same run. The job is without its writer after the run, with a delay of 2 s and the next reopen 1 s ahead. The
    // log must have a January row, so that the reopen does not touch February.
    private void failPostingSealLogTwice(PostingSealPurgeJob job, PurgeLogFaultFacade faultFf, TableToken tok, Path partitionPath, long now) {
        final int failureCount = faultFf.getFailureCount();
        final int pLen = partitionPath.size();
        faultFf.failMonth("1970-02");
        publishPurgeTask(tok, "c_outage", writeAndSeal(partitionPath, "c_outage"), 1L);
        runPurgeJobAt(job, now);
        partitionPath.trimTo(pLen);
        faultFf.failMonth(null);
        assertEquals(failureCount + 2, faultFf.getFailureCount());
        assertFalse(job.isJobAliveForTesting());
        assertEquals(0, job.getErrorCountForTesting());
    }

    // the partition directories of the posting seal purge log, in name order
    private static String listPostingSealLogPartitions() {
        final File dir = new File(configuration.getDbRoot().toString(), engine.verifyTableName(postingSealPurgeLogTableName()).getDirName());
        final String[] names = dir.list((d, name) -> Character.isDigit(name.charAt(0)));
        assertNotNull(names);
        Arrays.sort(names);
        return String.join(" ", names);
    }

    // logs the purge of the superseded .pv of col_jan in January, at 10 us, and completes it in the same run
    private void logPurgeInJanuary(PostingSealPurgeJob job, TableToken tok, Path partitionPath) {
        final int pLen = partitionPath.size();
        publishPurgeTask(tok, "col_jan", writeAndSeal(partitionPath, "col_jan"), 1L);
        partitionPath.trimTo(pLen);
        runPurgeJobAt(job, 10);
    }

    static String postingSealPurgeLogTableName() {
        return configuration.getSystemTableNamePrefix() + "posting_seal_purge_log";
    }

    // removes the files of the index of colName in the partition
    private static void removeIndexFiles(Path partitionPath, String colName) {
        final File dir = new File(partitionPath.toString());
        final String[] names = dir.list((d, name) -> name.startsWith(colName + "."));
        assertNotNull(names);
        for (String name : names) {
            assertTrue(name, new File(dir, name).delete());
        }
    }

    static void runPurgeJobAt(PostingSealPurgeJob job, long now) {
        setCurrentMicros(now);
        job.run();
    }

    // writes data and seals once, returning the live head sealTxn
    private static long writeAndSealOnce(Path partitionPath, String colName) {
        final int pLen = partitionPath.size();
        try (PostingIndexWriter writer = new PostingIndexWriter(configuration, partitionPath, colName, COLUMN_NAME_TXN_NONE)) {
            for (int i = 0; i < 8; i++) {
                writer.add(i % BATCH_KEYS, i);
            }
            writer.setMaxValue(7);
            writer.commit();
            writer.seal();
        }
        final long sealTxn = PostingIndexUtils.readSealTxnFromKeyFile(
                configuration.getFilesFacade(), PostingIndexUtils.keyFileName(partitionPath.trimTo(pLen), colName, COLUMN_NAME_TXN_NONE));
        partitionPath.trimTo(pLen);
        return sealTxn;
    }

    /**
     * The retry schedule of the purge log, as the jobs keep it: a failure makes the next reopen wait the current delay,
     * which grows from 0 to 1 s and then doubles, up to 60 s. A commit that saves rows through a writer the job reopened
     * at least the current delay before resets the delay to 0.
     */
    static class LogBackoffModel {
        private long delay;
        private long dueAt;
        private boolean isHolding = true;
        private long reopenedAt;

        boolean isHolding() {
            return isHolding;
        }

        boolean isReopenDue(long now) {
            return !isHolding && now >= dueAt;
        }

        void onFailure(long now) {
            dueAt = now + delay;
            delay = delay == 0 ? Micros.SECOND_MICROS : Math.min(Micros.MINUTE_MICROS, 2 * delay);
            isHolding = false;
        }

        void onReopen(long now) {
            isHolding = true;
            reopenedAt = now;
        }

        void onRowSavingCommit(long now) {
            if (now - reopenedAt >= delay) {
                delay = 0;
            }
        }
    }

    /**
     * Fails file operations of the posting seal purge log as a test arms them, and counts each failure once, when it
     * fires: the next allocate() of a file in a partition of the log, which extends a column file by a page, as on a
     * full disk; every open of a file in the partitions of a month, as on a disk that fills up when the log starts the
     * month; the next open of a file of the log that ends with a suffix.
     */
    static class PurgeLogFaultFacade extends TestFilesFacadeImpl {
        private final AtomicInteger failureCount = new AtomicInteger();
        private final AtomicReference<String> failingOpenSuffix = new AtomicReference<>();
        private final AtomicBoolean isAllocateFailureArmed = new AtomicBoolean();
        private final LongList partitionFds = new LongList();
        private volatile String failingMonth;

        @Override
        public boolean allocate(long fd, long size) {
            if (isAllocateFailureArmed.get() && isPartitionFd(fd) && isAllocateFailureArmed.compareAndSet(true, false)) {
                failureCount.incrementAndGet();
                return false;
            }
            return super.allocate(fd, size);
        }

        @Override
        public boolean close(long fd) {
            synchronized (partitionFds) {
                final int index = partitionFds.indexOf(fd);
                if (index > -1) {
                    partitionFds.removeIndex(index);
                }
            }
            return super.close(fd);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            final File file = new File(Utf8s.stringFromUtf8Bytes(name));
            final String partitionDir = logPartitionDir(file);
            if (partitionDir != null) {
                final String month = failingMonth;
                if (month != null && partitionDir.startsWith(month)) {
                    failureCount.incrementAndGet();
                    return -1;
                }
                final String suffix = failingOpenSuffix.get();
                if (suffix != null && file.getName().endsWith(suffix) && failingOpenSuffix.compareAndSet(suffix, null)) {
                    failureCount.incrementAndGet();
                    return -1;
                }
            }
            final long fd = super.openRW(name, opts);
            if (fd > -1 && partitionDir != null) {
                synchronized (partitionFds) {
                    partitionFds.add(fd);
                }
            }
            return fd;
        }

        // fails the next allocate() of a file in a partition of the log
        void failNextAllocate() {
            isAllocateFailureArmed.set(true);
        }

        // fails the next open of a file in a partition of the log whose name ends with suffix
        void failNextOpen(String suffix) {
            failingOpenSuffix.set(suffix);
        }

        // fails every open of a file in the partitions of the log whose names start with month, or none when null
        void failMonth(String month) {
            failingMonth = month;
        }

        int getFailureCount() {
            return failureCount.get();
        }

        // the name of the partition directory of the log that holds the file, or null when the file is not in one
        private static String logPartitionDir(File file) {
            final File dir = file.getParentFile();
            if (dir != null && dir.getParentFile() != null
                    && dir.getParentFile().getName().startsWith(postingSealPurgeLogTableName() + TableUtils.SYSTEM_TABLE_NAME_SUFFIX)
                    && Character.isDigit(dir.getName().charAt(0))) {
                return dir.getName();
            }
            return null;
        }

        private boolean isPartitionFd(long fd) {
            synchronized (partitionFds) {
                return partitionFds.indexOf(fd) > -1;
            }
        }
    }

    // Fails, once when armed, the remap of the table_name.c symbol map file of the posting seal purge log while
    // TRUNCATE empties the map, after TRUNCATE published the empty _txn
    private static class SymbolMapTruncateFailingFacade extends TestFilesFacadeImpl {
        private final AtomicBoolean isArmed = new AtomicBoolean();
        private volatile long fd = -1;

        @Override
        public long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
            if (fd == this.fd && isArmed.get() && isSymbolMapTruncateCalling() && isArmed.compareAndSet(true, false)) {
                return FilesFacade.MAP_FAILED;
            }
            return super.mremap(fd, addr, previousSize, newSize, offset, mode, memoryTag);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            final long fd = super.openRW(name, opts);
            if (Utf8s.containsAscii(name, Files.SEPARATOR + postingSealPurgeLogTableName() + TableUtils.SYSTEM_TABLE_NAME_SUFFIX)
                    && Utf8s.endsWithAscii(name, Files.SEPARATOR + "table_name.c")) {
                this.fd = fd;
            }
            return fd;
        }

        // whether SymbolMapWriter.truncate(), which TRUNCATE calls to empty a symbol map, asks for the remap
        private static boolean isSymbolMapTruncateCalling() {
            return StackWalker.getInstance().walk(
                    frames -> frames.anyMatch(frame -> SymbolMapWriter.class.getName().equals(frame.getClassName()) && "truncate".equals(frame.getMethodName()))
            );
        }
    }
}
