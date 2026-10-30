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
import io.questdb.cairo.RowExpiryCleanupJob;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.wal.WalReader;
import io.questdb.griffin.SqlException;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Power-loss recovery of the conditional REPLACE_RANGE commits that {@link RowExpiryCleanupJob} issues, and of the
 * EXPIRE ROWS policy that {@code _meta} carries, under ADAPTIVE with a group window of 0.
 * <p>
 * A cleanup commit goes through {@code WalWriter.commitWithParamsIfSeqTxn}, which sequences the commit only while
 * the view's latest txn is still the one the sweep scanned. Like every other WAL commit, it makes the files its
 * sequencer record names durable first: the column and event files, and the segment {@code _meta}. WAL apply maps
 * the column files through the table's own metadata, so a segment {@code _meta} lost in a power loss shows up only
 * through {@link WalReader}, which the live-view drain uses to read a base table's WAL. After every recovery the
 * tests therefore open each segment the txn log names, besides checking the view's rows and partitions.
 * <p>
 * Each cleanup phase releases the pooled WAL writers before its durable baseline, so the pool creates the sweep's
 * writer after it and the cleanup commits run first in a segment whose {@code _meta} no earlier commit flushed. The
 * mat-view refresh job shares the view's lock and pooled writer, and its commits flush {@code _meta}, so in
 * production the gap opens when a sweep is the first to commit on a fresh writer or segment.
 */
public class AdaptiveRowExpiryCleanupCrashTest extends AbstractAdaptiveCrashTest {

    @Test
    public void testCleanupInFreshWalWriterSurvivesPowerLoss() throws Exception {
        runAdaptive(() -> {
            createBaseAndView();
            final TableToken baseTt = engine.verifyTableName("base");
            final TableToken mvTt = engine.verifyTableName("mv");

            // The pool creates a new WAL writer for the sweep, so the cleanup commits are the first ones sequenced
            // in its segment. The epoch stays off, so no save point flushes the WAL files wholesale.
            engine.releaseAllWalWriters();
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
            final long baselineSeqTxn = engine.getTableSequencerAPI().getTxnTracker(mvTt).getSeqTxn();
            markDurableBaseline();

            Assert.assertTrue("the sweep must reclaim both fully expired partitions", cleanup(mvTt));

            // W=0: the cleanup commits returned, so their sequencer records are on disk.
            recoverAfterCrash(new TableToken[]{baseTt, mvTt});
            engine.hydrateMatViewStateStore();
            Assert.assertTrue("the cleanup must have sequenced data txns", assertWalSegmentsReadable(mvTt, baselineSeqTxn, "") > 0);
            drainWalAndMatViewQueues();

            Assert.assertFalse(anyTableSuspended(baseTt, mvTt));
            assertQuery("SELECT count() p FROM table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n1\n");
            assertQuery("SELECT v FROM mv ORDER BY v").noLeakCheck().returns("v\n3.0\n");
            dropBaseAndView();
        });
    }

    @Test
    public void testCleanupRejectedByFenceSurvivesPowerLoss() throws Exception {
        runAdaptive(() -> {
            createBaseAndView();
            final TableToken baseTt = engine.verifyTableName("base");
            final TableToken mvTt = engine.verifyTableName("mv");

            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
            final long baselineSeqTxn = engine.getTableSequencerAPI().getTxnTracker(mvTt).getSeqTxn();
            markDurableBaseline();

            // The barrier fires after the sweep flushed its first wipe and before the sequencer decides. The ALTER
            // takes the next txn, so the fence rejects the wipe; it keeps the same rows expired.
            engine.getTableSequencerAPI().setTestNextTxnIfLastTxnBarrier(() -> {
                try {
                    execute("ALTER MATERIALIZED VIEW mv SET EXPIRE ROWS WHEN v < -0.5");
                } catch (SqlException e) {
                    throw new RuntimeException(e);
                }
            });
            try {
                Assert.assertFalse("the fence must reject the sweep's first wipe", cleanup(mvTt));
            } finally {
                engine.getTableSequencerAPI().setTestNextTxnIfLastTxnBarrier(null);
            }

            // The rejected wipe's event is on disk in an abandoned segment; recovery must ignore it.
            recoverAfterCrash(new TableToken[]{baseTt, mvTt});
            engine.hydrateMatViewStateStore();
            Assert.assertTrue("the ALTER must have sequenced a data txn", assertWalSegmentsReadable(mvTt, baselineSeqTxn, "") > 0);
            drainWalAndMatViewQueues();

            Assert.assertFalse(anyTableSuspended(baseTt, mvTt));
            Assert.assertEquals("v < -0.5", expiryPredicate(mvTt));
            assertQuery("SELECT count() p FROM table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n3\n");
            assertQuery("SELECT v FROM mv ORDER BY v").noLeakCheck().returns("v\n3.0\n");

            // The post-recovery refresh opened a WAL writer before this baseline would mark its segment durable.
            // Releasing it makes the pool create the sweep's writer after the baseline, so the sweep's commits are
            // again first in their segment.
            engine.releaseAllWalWriters();
            final long restartSeqTxn = engine.getTableSequencerAPI().getTxnTracker(mvTt).getSeqTxn();
            markDurableBaseline();
            Assert.assertTrue("the sweep after the restart must reclaim", cleanup(mvTt));

            recoverAfterCrash(new TableToken[]{baseTt, mvTt});
            engine.hydrateMatViewStateStore();
            Assert.assertTrue("the cleanup must have sequenced data txns", assertWalSegmentsReadable(mvTt, restartSeqTxn, "") > 0);
            drainWalAndMatViewQueues();

            Assert.assertFalse(anyTableSuspended(baseTt, mvTt));
            assertQuery("SELECT count() p FROM table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n1\n");
            assertQuery("SELECT v FROM mv ORDER BY v").noLeakCheck().returns("v\n3.0\n");
            dropBaseAndView();
        });
    }

    @Test
    public void testExpiryPolicyChangesSurvivePowerLoss() throws Exception {
        runAdaptive(() -> {
            createBaseAndView();
            final TableToken baseTt = engine.verifyTableName("base");
            final TableToken mvTt = engine.verifyTableName("mv");

            // The save point taken at setup holds the original policy; with the epoch off, recovery restores that
            // _meta and must replay each policy change from the WAL.
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
            markDurableBaseline();

            execute("ALTER MATERIALIZED VIEW mv SET EXPIRE ROWS WHEN v < 5");
            drainWalAndMatViewQueues();
            final String setPredicate = expiryPredicate(mvTt);
            Assert.assertNotNull(setPredicate);

            recoverAfterCrash(new TableToken[]{baseTt, mvTt});
            engine.hydrateMatViewStateStore();
            drainWalAndMatViewQueues();
            Assert.assertFalse(anyTableSuspended(baseTt, mvTt));
            Assert.assertEquals(setPredicate, expiryPredicate(mvTt));
            assertQuery("SELECT v FROM mv ORDER BY v").noLeakCheck().returns("v\n");

            markDurableBaseline();
            execute("ALTER MATERIALIZED VIEW mv DROP EXPIRE");
            drainWalAndMatViewQueues();
            final String droppedPredicate = expiryPredicate(mvTt);

            recoverAfterCrash(new TableToken[]{baseTt, mvTt});
            engine.hydrateMatViewStateStore();
            drainWalAndMatViewQueues();
            Assert.assertFalse(anyTableSuspended(baseTt, mvTt));
            Assert.assertEquals(droppedPredicate, expiryPredicate(mvTt));
            assertQuery("SELECT v FROM mv ORDER BY v").expectSize().noLeakCheck().returns("v\n-2.0\n-1.0\n3.0\n");
            dropBaseAndView();
        });
    }

    static boolean cleanup(TableToken mvTt) {
        final String predicate = expiryPredicate(mvTt);
        try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
            return job.cleanupTable(mvTt, predicate);
        }
    }

    static String expiryPredicate(TableToken token) {
        try (TableMetadata m = engine.getTableMetadata(token)) {
            return m.getExpiryPredicate();
        }
    }

    private static void createBaseAndView() throws SqlException {
        execute("CREATE TABLE base (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("""
                INSERT INTO base VALUES
                (-1.0, '2024-01-01T00:00:00.000000Z'),
                (-2.0, '2024-01-02T00:00:00.000000Z'),
                (3.0, '2024-01-03T00:00:00.000000Z')""");
        drainWalAndMatViewQueues();
        execute("CREATE MATERIALIZED VIEW mv AS (SELECT * FROM base) EXPIRE ROWS WHEN v < 0");
        drainWalAndMatViewQueues();
    }

    private static void dropBaseAndView() throws SqlException {
        execute("DROP MATERIALIZED VIEW mv");
        execute("DROP TABLE base");
        drainWalAndMatViewQueues();
    }

    private void runAdaptive(TestUtils.LeakProneCode body) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "0");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
        Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());
        runWithCrashFacade(() -> {
            // Per-inode strictness: one file's fsync does not make another file's size durable.
            crashFf.modelSharedJournal = false;
            body.run();
        });
    }
}
