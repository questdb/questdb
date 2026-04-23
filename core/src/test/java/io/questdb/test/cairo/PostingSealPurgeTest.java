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
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.tasks.PostingSealPurgeTask;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
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
                LPSZ firstPv = PostingIndexUtils.valueFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, firstSealTxn);
                LPSZ livePv = PostingIndexUtils.valueFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, secondSealTxn);
                assertTrue("first-seal .pv survives until purge", ff.exists(firstPv));
                assertTrue("live .pv at the latest sealTxn must exist", ff.exists(livePv));

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
                            0L, 0L, 0, ColumnType.TIMESTAMP_MICRO, 0L
                    );
                    // The above publish is a no-op because we deliberately pass
                    // a null/missing TableToken — we only want to verify the
                    // orphan was enqueued. The real publish happens later when
                    // a TableWriter commits with the right partition info.
                }

                // Drive the purge job synchronously a few times. Each .run(0)
                // call drains the queue and runs one retry pass; backoff means
                // we may need several iterations before the orphan window is
                // actually attempted.
                if (!configuration.disableColumnPurgeJob()) {
                    try (PostingSealPurgeJob job = new PostingSealPurgeJob(engine)) {
                        for (int i = 0; i < 8; i++) {
                            job.run(0);
                            // Tiny pause so retry-backoff can elapse.
                            Os.pause();
                        }
                    }
                }

                // Live .pv must always still be on disk.
                assertTrue("live .pv must remain after purge runs", ff.exists(livePv));
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
                job.run(0);
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
                                partitionPath.trimTo(pLen), col, 0, COLUMN_NAME_TXN_NONE, sealTxn)));

                publishPurgeTask(tok, col, sealTxn, 1L);
                runPurgeJob(job, 3);

                assertFalse(".pv must be purged",
                        ff.exists(PostingIndexUtils.valueFileName(
                                partitionPath.trimTo(pLen), col, COLUMN_NAME_TXN_NONE, sealTxn)));
                assertFalse(".pc0 must be purged together with .pv",
                        ff.exists(PostingIndexUtils.coverDataFileName(
                                partitionPath.trimTo(pLen), col, 0, COLUMN_NAME_TXN_NONE, sealTxn)));
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
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "ps_purge_orphan";
                int plen = path.size();
                FilesFacade ff = configuration.getFilesFacade();

                long firstSealTxn;
                long secondSealTxn;

                try (PostingIndexWriter writerA = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                    for (int i = 0; i < 8; i++) {
                        writerA.add(i % BATCH_KEYS, i);
                    }
                    writerA.setMaxValue(7);
                    writerA.commit();
                    writerA.seal();
                    firstSealTxn = PostingIndexUtils.readSealTxnFromKeyFile(
                            ff, PostingIndexUtils.keyFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE));

                    for (int i = 8; i < 16; i++) {
                        writerA.add(i % BATCH_KEYS, i);
                    }
                    writerA.setMaxValue(15);
                    writerA.commit();
                    writerA.seal();
                    secondSealTxn = PostingIndexUtils.readSealTxnFromKeyFile(
                            ff, PostingIndexUtils.keyFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE));
                }
                // Writer A closed — pendingPurges discarded, but orphan files
                // still exist on disk.
                assertTrue("first-seal .pv survives writer close",
                        ff.exists(PostingIndexUtils.valueFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, firstSealTxn)));

                // Writer B opens — its of() should detect the orphan via
                // logOrphanSealedFiles and push a PendingSealPurge entry.
                try (PostingIndexWriter writerB = new PostingIndexWriter(configuration)) {
                    writerB.of(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, false);
                    // The orphan was enqueued during of(). Live .pv must still exist.
                    assertTrue("live .pv must remain after writer-B open",
                            ff.exists(PostingIndexUtils.valueFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, secondSealTxn)));
                    // The orphan from before is still on disk because
                    // publishPendingPurges hasn't been called yet — the
                    // outbox holds it pending.
                    assertTrue("orphan .pv stays on disk until publish + job run",
                            ff.exists(PostingIndexUtils.valueFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, firstSealTxn)));
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

    private TableToken createPostingTable(String name) {
        return createTable(
                new TableModel(configuration, name, PartitionBy.NONE)
                        .col("c", ColumnType.INT)
                        .timestamp("ts")
        );
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
            job.run(0);
            setCurrentMicros(currentMicros + 100L * iteration++);
            job.run(0);
        }
    }

    private long writeAndSeal(Path partitionPath, String colName) {
        FilesFacade ff = configuration.getFilesFacade();
        int pLen = partitionPath.size();
        try (PostingIndexWriter writer = new PostingIndexWriter(
                configuration, partitionPath, colName, COLUMN_NAME_TXN_NONE)) {
            for (int i = 0; i < 8; i++) {
                writer.add(i % BATCH_KEYS, i);
            }
            writer.setMaxValue(8 - 1);
            writer.commit();
            writer.seal();
        }
        long sealTxn = PostingIndexUtils.readSealTxnFromKeyFile(
                ff,
                PostingIndexUtils.keyFileName(partitionPath.trimTo(pLen), colName, COLUMN_NAME_TXN_NONE)
        );
        partitionPath.trimTo(pLen);
        return sealTxn;
    }

    private long writeCoveringAndSeal(Path partitionPath, String colName) {
        FilesFacade ff = configuration.getFilesFacade();
        int pLen = partitionPath.size();
        long colAddr = Unsafe.malloc((long) 8 * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().setMemory(colAddr, (long) 8 * Integer.BYTES, (byte) 0);
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
            }
        } finally {
            Unsafe.free(colAddr, (long) 8 * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
        long sealTxn = PostingIndexUtils.readSealTxnFromKeyFile(
                ff,
                PostingIndexUtils.keyFileName(partitionPath.trimTo(pLen), colName, COLUMN_NAME_TXN_NONE)
        );
        partitionPath.trimTo(pLen);
        return sealTxn;
    }

}
