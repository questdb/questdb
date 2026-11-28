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

import io.questdb.PropertyKey;
import io.questdb.cairo.PostingSealPurgeJob;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TxnScoreboard;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.LogCapture;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;
import static io.questdb.test.cairo.PostingSealPurgeTest.countLogRows;
import static io.questdb.test.cairo.PostingSealPurgeTest.createPostingTable;
import static io.questdb.test.cairo.PostingSealPurgeTest.partitionPathFor;
import static io.questdb.test.cairo.PostingSealPurgeTest.postingSealPurgeLogTableName;
import static io.questdb.test.cairo.PostingSealPurgeTest.publishPurgeTask;
import static io.questdb.test.cairo.PostingSealPurgeTest.runPurgeJobAt;
import static io.questdb.test.cairo.PostingSealPurgeTest.writeAndSeal;
import static org.junit.Assert.*;

/**
 * Tests of the posting seal purge log whose runs queue a page of log rows at once, so that an append fails to allocate
 * a page: the retry schedule, the writer of the failed append, and the completions after an outage. Each entry is a
 * task of the purge queue, which the engine sizes at start.
 */
public class PostingSealPurgeLogBackoffTest extends AbstractCairoTest {

    @BeforeClass
    public static void setUpStatic() throws Exception {
        // a batch of one page of log rows and one more entry fits in the purge queue
        staticOverrides.setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_QUEUE_CAPACITY, 4096);
        AbstractCairoTest.setUpStatic();
    }

    @Before
    public void setUpBackoff() {
        setCurrentMicros(0);
        node1.setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY, 1);
        node1.setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY_LIMIT, 10);
        node1.setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY_MULTIPLIER, 2);
        // a page of log rows of a batch extends each 8-byte column file of the log by a page
        node1.setProperty(PropertyKey.CAIRO_SYSTEM_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
    }

    @Test
    public void testLogBackoffNotResetByCommitWithoutRows() throws Exception {
        // A run the full delay after the reopen of the writer drains no entry, and its retry of the pinned entry commits
        // no rows: only a commit that saves rows resets the delay. The failure after it waits the kept delay, so the job
        // reopens 1 s after that failure, not in its run.
        final PostingSealPurgeTest.PurgeLogFaultFacade faultFf = new PostingSealPurgeTest.PurgeLogFaultFacade();
        assertMemoryLeak(faultFf, () -> {
            final TableToken tok = createPostingTable("ps_no_rows");
            final TableToken droppedTok = createDroppedTable();
            try (
                    Path partitionPath = partitionPathFor(tok);
                    PostingSealPurgeJob job = new PostingSealPurgeJob(engine);
                    TxnScoreboard scoreboard = engine.getTxnScoreboard(tok)
            ) {
                final int pLen = partitionPath.size();
                assertTrue(scoreboard.acquireTxn(0, 5L, 5L));
                try {
                    final long t0 = Micros.DAY_MICROS;
                    publishPurgeTask(tok, "c_held", writeAndSeal(partitionPath.trimTo(pLen), "c_held"), 10L);
                    runPurgeJobAt(job, t0 - Micros.SECOND_MICROS);
                    final ResetProbe probe = new ResetProbe(job, faultFf, droppedTok);
                    // F1 at t0, delay 0: the same run reopens, r1 = t0, and the delay is 1 s
                    probe.failingRun(t0, true);
                    // exactly 1 s after r1, the retry of the pinned entry commits no rows: no reset
                    probe.retryRun(t0 + Micros.SECOND_MICROS);
                    // F2 waits the kept delay of 1 s, and the delay is 2 s
                    probe.failingRun(t0 + Micros.SECOND_MICROS, false);
                    probe.idleRun(t0 + 2 * Micros.SECOND_MICROS - 1, false);
                    probe.idleRun(t0 + 2 * Micros.SECOND_MICROS, true);
                    assertEquals(2, faultFf.getFailureCount());
                    assertEquals(0, job.getErrorCountForTesting());
                } finally {
                    scoreboard.releaseTxn(0, 5L);
                }
            }
        });
    }

    @Test
    public void testLogBackoffNotResetByWriteBackCommit() throws Exception {
        // Each run that starts with the job holding its writer fails its append: the test arms a page allocation
        // failure and queues a page of entries, besides the one entry of each run. The write-back of each reopen
        // commits rows, but the next run fails less than the delay after the reopen, so the delay never resets: the
        // failures follow the schedule, also past its 60 s cap. P, which a pinned txn holds, gains a row at each
        // reopen. Log failures add no errors, and each run purges its entry.
        final PostingSealPurgeTest.PurgeLogFaultFacade faultFf = new PostingSealPurgeTest.PurgeLogFaultFacade();
        assertMemoryLeak(faultFf, () -> {
            final TableToken tok = createPostingTable("ps_backoff");
            final TableToken droppedTok = createDroppedTable();
            final FilesFacade runtimeFf = configuration.getFilesFacade();
            try (
                    Path partitionPath = partitionPathFor(tok);
                    PostingSealPurgeJob job = new PostingSealPurgeJob(engine);
                    TxnScoreboard scoreboard = engine.getTxnScoreboard(tok)
            ) {
                final int pLen = partitionPath.size();
                assertTrue(scoreboard.acquireTxn(0, 5L, 5L));
                try {
                    final long t0 = Micros.DAY_MICROS;
                    publishPurgeTask(tok, "c_held", writeAndSeal(partitionPath.trimTo(pLen), "c_held"), 10L);
                    runPurgeJobAt(job, t0 - Micros.SECOND_MICROS);
                    final PostingSealPurgeTest.LogBackoffModel model = new PostingSealPurgeTest.LogBackoffModel();
                    final LongList failureSeconds = new LongList();
                    int heldTaskRows = 1;
                    for (int i = 0; i <= 200; i++) {
                        final long now = t0 + i * 2 * Micros.SECOND_MICROS;
                        final boolean isHolding = model.isHolding();
                        final String col = "c_" + i;
                        final long sealTxn = writeAndSeal(partitionPath.trimTo(pLen), col);
                        publishPurgeTask(tok, col, sealTxn, 1L);
                        if (isHolding) {
                            publishPageOfEntries(droppedTok);
                            faultFf.failNextAllocate();
                        }
                        runPurgeJobAt(job, now);
                        if (isHolding) {
                            model.onFailure(now);
                            failureSeconds.add((now - t0) / Micros.SECOND_MICROS);
                        }
                        if (model.isReopenDue(now)) {
                            model.onReopen(now);
                            model.onRowSavingCommit(now);
                            heldTaskRows++;
                        }
                        final String run = "run at " + (now - t0) / Micros.SECOND_MICROS + " s";
                        assertEquals(run, failureSeconds.size(), faultFf.getFailureCount());
                        assertEquals(run, heldTaskRows, countLogRows("column_name = 'c_held'"));
                        assertEquals(run, model.isHolding(), job.isJobAliveForTesting());
                        assertEquals(run, 0, job.getErrorCountForTesting());
                        assertFalse(run, runtimeFf.exists(PostingIndexUtils.valueFileName(partitionPath.trimTo(pLen), col, COLUMN_NAME_TXN_NONE, sealTxn)));
                    }
                    assertEquals("[0,2,6,10,16,26,44,78,140,202,264,326,388]", failureSeconds.toString());
                } finally {
                    scoreboard.releaseTxn(0, 5L);
                }
            }
        });
    }

    @Test
    public void testLogBackoffResetsOnlyAfterFullDelaySinceReopen() throws Exception {
        // The delay resets only at a commit that saves rows at least the current delay after the reopen of the writer,
        // with no failure since the reopen. A run with an armed allocation failure shows whether a reset happened: its
        // failure is retried in the same run only when the delay was 0.
        final PostingSealPurgeTest.PurgeLogFaultFacade faultFf = new PostingSealPurgeTest.PurgeLogFaultFacade();
        assertMemoryLeak(faultFf, () -> {
            final TableToken tok = createPostingTable("ps_reset");
            final TableToken droppedTok = createDroppedTable();
            try (
                    Path partitionPath = partitionPathFor(tok);
                    PostingSealPurgeJob job = new PostingSealPurgeJob(engine);
                    TxnScoreboard scoreboard = engine.getTxnScoreboard(tok)
            ) {
                final int pLen = partitionPath.size();
                assertTrue(scoreboard.acquireTxn(0, 5L, 5L));
                try {
                    final long t0 = Micros.DAY_MICROS;
                    publishPurgeTask(tok, "c_held", writeAndSeal(partitionPath.trimTo(pLen), "c_held"), 10L);
                    runPurgeJobAt(job, t0 - Micros.SECOND_MICROS);
                    final ResetProbe probe = new ResetProbe(job, faultFf, droppedTok);
                    // F1 at t0, delay 0: the same run reopens, r1 = t0, and the delay is 1 s
                    probe.failingRun(t0, true);
                    // a commit of rows 1 us short of the delay after r1 does not reset: F2 waits 1 s, delay 2 s
                    probe.savingRun(t0 + Micros.SECOND_MICROS - 1);
                    probe.failingRun(t0 + Micros.SECOND_MICROS - 1, false);
                    final long r2 = t0 + 2 * Micros.SECOND_MICROS - 1;
                    probe.idleRun(r2 - 1, false);
                    probe.idleRun(r2, true);
                    // 1 s after r2 and 2 s after F2: no reset, which a delay counted from the failure would give
                    probe.savingRun(r2 + Micros.SECOND_MICROS);
                    probe.failingRun(r2 + Micros.SECOND_MICROS, false);
                    // F3 waits 2 s: r3 = r2 + 3 s, delay 4 s
                    final long r3 = r2 + 3 * Micros.SECOND_MICROS;
                    probe.idleRun(r3 - 1, false);
                    probe.idleRun(r3, true);
                    probe.savingRun(r3 + 4 * Micros.SECOND_MICROS - 1);
                    probe.failingRun(r3 + 4 * Micros.SECOND_MICROS - 1, false);
                    // F4 waits 4 s: r4, delay 8 s, and a commit of rows exactly 8 s after r4 resets
                    final long r4 = r3 + 8 * Micros.SECOND_MICROS - 1;
                    probe.idleRun(r4 - 1, false);
                    probe.idleRun(r4, true);
                    probe.savingRun(r4 + 8 * Micros.SECOND_MICROS);
                    final long r5 = r4 + 8 * Micros.SECOND_MICROS;
                    probe.failingRun(r5, true);
                    // F6, 500 ms after r5, waits 1 s again
                    probe.failingRun(r5 + 500_000, false);
                    probe.idleRun(r5 + 1_500_000 - 1, false);
                    probe.idleRun(r5 + 1_500_000, true);
                    assertEquals(6, faultFf.getFailureCount());
                    assertEquals(0, job.getErrorCountForTesting());
                } finally {
                    scoreboard.releaseTxn(0, 5L);
                }
            }
        });
    }

    @Test
    public void testLogReopenAfterMarkedMonthTruncatedDuringOutage() throws Exception {
        // The job completes an entry in the January partition of the log, which keeps the completed.d of that
        // partition open for the next completion. Then the log fails twice in one run: a page of the log cannot be
        // allocated, and the reopen cannot open the files of January. TRUNCATE TABLE empties the log meanwhile, and the
        // reopen writes the pinned entry back as the first row of a new January partition. The completion of the entry
        // lands on that row, not in the completed.d of the partition that TRUNCATE removed.
        final PostingSealPurgeTest.PurgeLogFaultFacade faultFf = new PostingSealPurgeTest.PurgeLogFaultFacade();
        assertMemoryLeak(faultFf, () -> {
            final TableToken tok = createPostingTable("ps_truncated");
            final TableToken droppedTok = createDroppedTable();
            final FilesFacade runtimeFf = configuration.getFilesFacade();
            try (
                    Path partitionPath = partitionPathFor(tok);
                    PostingSealPurgeJob job = new PostingSealPurgeJob(engine);
                    TxnScoreboard scoreboard = engine.getTxnScoreboard(tok)
            ) {
                final int pLen = partitionPath.size();
                // logged, purged and completed in one run
                publishPurgeTask(tok, "c_jan", writeAndSeal(partitionPath.trimTo(pLen), "c_jan"), 1L);
                runPurgeJobAt(job, 10);
                final long sealTxn = writeAndSeal(partitionPath.trimTo(pLen), "c_held");
                assertTrue(scoreboard.acquireTxn(0, 5L, 5L));
                try {
                    publishPurgeTask(tok, "c_held", sealTxn, 10L);
                    runPurgeJobAt(job, 20);
                    publishPageOfEntries(droppedTok);
                    faultFf.failNextAllocate();
                    faultFf.failMonth("1970-01");
                    runPurgeJobAt(job, 30);
                    faultFf.failMonth(null);
                    assertEquals(2, faultFf.getFailureCount());
                    assertFalse(job.isJobAliveForTesting());
                    execute("TRUNCATE TABLE '" + postingSealPurgeLogTableName() + "'");
                    // the reopen is due 1 s after the failure of the reopen at 30 us
                    runPurgeJobAt(job, Micros.SECOND_MICROS + 30);
                    assertTrue(job.isJobAliveForTesting());
                    assertEquals(1, job.getOutstandingPurgeTasks());
                } finally {
                    scoreboard.releaseTxn(0, 5L);
                }
                runPurgeJobAt(job, Micros.SECOND_MICROS + 40);
                assertEquals(0, job.getOutstandingPurgeTasks());
                assertEquals(0, job.getErrorCountForTesting());
                assertFalse(runtimeFf.exists(PostingIndexUtils.valueFileName(partitionPath.trimTo(pLen), "c_held", COLUMN_NAME_TXN_NONE, sealTxn)));
                assertQuery("SELECT column_name, ts, completed FROM '" + postingSealPurgeLogTableName() + "'")
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("""
                                column_name\tts\tcompleted
                                c_held\t1970-01-01T00:00:01.000030Z\t1970-01-01T00:00:01.000040Z
                                """);
            }
        });
    }

    @Test
    public void testLogWriterDiscardedAfterFailedFirstRow() throws Exception {
        // The first row after a commit fails to start a page of the log. The pool would keep that writer, which still
        // points into the page it lost: the job marks it distressed, so the pool closes it, and the reopen in the same
        // run takes a new writer. After the job, the log takes an INSERT.
        final PostingSealPurgeTest.PurgeLogFaultFacade faultFf = new PostingSealPurgeTest.PurgeLogFaultFacade();
        assertMemoryLeak(faultFf, () -> {
            final TableToken droppedTok = createDroppedTable();
            final int rowsPerPage = logRowsPerPage();
            final LogCapture capture = new LogCapture();
            final TableToken logToken;
            try (PostingSealPurgeJob job = new PostingSealPurgeJob(engine)) {
                logToken = engine.verifyTableName(postingSealPurgeLogTableName());
                publishEntries(droppedTok, 0, rowsPerPage);
                runPurgeJobAt(job, 10);
                final long writerId = engine.getWriterId(logToken);
                capture.start();
                try {
                    faultFf.failNextAllocate();
                    publishEntries(droppedTok, rowsPerPage, rowsPerPage);
                    runPurgeJobAt(job, 30);
                    assertEquals(1, faultFf.getFailureCount());
                    assertTrue(job.isJobAliveForTesting());
                    assertNotEquals(writerId, engine.getWriterId(logToken));
                    capture.waitFor("closed [table=" + logToken.getDirName() + ", reason=DISTRESSED");
                } finally {
                    capture.stop();
                }
                assertEquals(0, job.getErrorCountForTesting());
                assertQuery("SELECT ts, count(), count(completed) FROM '" + logToken.getTableName() + "' ORDER BY ts")
                        .noLeakCheck()
                        .noRandomAccess()
                        .timestamp("ts")
                        .returns("ts\tcount\tcount1\n"
                                + "1970-01-01T00:00:00.000010Z\t" + rowsPerPage + "\t" + rowsPerPage + "\n"
                                + "1970-01-01T00:00:00.000030Z\t" + rowsPerPage + "\t" + rowsPerPage + "\n");
            }
            execute("INSERT INTO '" + logToken.getTableName() + "' (ts, table_name) VALUES ('1970-01-01T00:00:01.000000Z', 'next')");
            assertQuery("SELECT table_name, count() FROM '" + logToken.getTableName() + "' ORDER BY table_name")
                    .noLeakCheck()
                    .expectSize()
                    .returns("table_name\tcount\n"
                            + "next\t1\n"
                            + droppedTok.getDirName() + "\t" + 2 * rowsPerPage + "\n");
        });
    }

    // a table that the test drops: the job completes the purge of an entry of it at once
    private static TableToken createDroppedTable() throws Exception {
        final TableToken droppedTok = createPostingTable("ps_dropped");
        execute("DROP TABLE ps_dropped");
        return droppedTok;
    }

    // the rows of a batch that fill a page of each 8-byte column of the log
    private static int logRowsPerPage() {
        return (int) (configuration.getSystemDataAppendPageSize() / Long.BYTES);
    }

    // queues entries of a dropped table, with the sealTxns from fromSealTxn
    private static void publishEntries(TableToken droppedTok, int fromSealTxn, int count) {
        for (int i = 0; i < count; i++) {
            publishPurgeTask(droppedTok, "d", fromSealTxn + i, 1L);
        }
    }

    // queues one page of log rows and one more row: from any offset they reach into a page no writer mapped yet
    private static void publishPageOfEntries(TableToken droppedTok) {
        publishEntries(droppedTok, 0, logRowsPerPage() + 1);
    }

    /**
     * Runs the posting seal purge job for testLogBackoffResetsOnlyAfterFullDelaySinceReopen and
     * testLogBackoffNotResetByCommitWithoutRows, and checks after each run the failures, the rows of the pinned entry,
     * which each reopen writes back, and whether the job holds the writer.
     */
    private static class ResetProbe {
        private final TableToken droppedTok;
        private final PostingSealPurgeTest.PurgeLogFaultFacade faultFf;
        private final PostingSealPurgeJob job;
        private int failureCount;
        private int heldTaskRows = 1;

        ResetProbe(PostingSealPurgeJob job, PostingSealPurgeTest.PurgeLogFaultFacade faultFf, TableToken droppedTok) {
            this.job = job;
            this.faultFf = faultFf;
            this.droppedTok = droppedTok;
        }

        // A run that starts with the job holding the writer, and whose append of a page of entries fails. The job
        // reopens in the same run, and writes the pinned entry back, only when the delay was 0.
        void failingRun(long now, boolean isReopenedInRun) throws Exception {
            assertTrue(job.isJobAliveForTesting());
            publishPageOfEntries(droppedTok);
            faultFf.failNextAllocate();
            runPurgeJobAt(job, now);
            failureCount++;
            if (isReopenedInRun) {
                heldTaskRows++;
            }
            check(now, isReopenedInRun);
        }

        // a run without an entry while the job is without its writer: it reopens, and writes the pinned entry back, or
        // not
        void idleRun(long now, boolean isReopening) throws Exception {
            assertFalse(job.isJobAliveForTesting());
            runPurgeJobAt(job, now);
            if (isReopening) {
                heldTaskRows++;
            }
            check(now, isReopening);
        }

        // a run without an entry while the job holds the writer: its retry of the pinned entry commits no rows
        void retryRun(long now) throws Exception {
            assertTrue(job.isJobAliveForTesting());
            setCurrentMicros(now);
            assertTrue("the run retries the pinned entry", job.run());
            check(now, true);
        }

        // a run that starts with the job holding the writer, and commits the row of an entry
        void savingRun(long now) throws Exception {
            assertTrue(job.isJobAliveForTesting());
            publishEntries(droppedTok, 0, 1);
            runPurgeJobAt(job, now);
            check(now, true);
        }

        private void check(long now, boolean isHolding) throws Exception {
            final String run = "run at " + now;
            assertEquals(run, failureCount, faultFf.getFailureCount());
            assertEquals(run, heldTaskRows, countLogRows("column_name = 'c_held'"));
            assertEquals(run, isHolding, job.isJobAliveForTesting());
            assertEquals(run, 1, job.getOutstandingPurgeTasks());
        }
    }
}
