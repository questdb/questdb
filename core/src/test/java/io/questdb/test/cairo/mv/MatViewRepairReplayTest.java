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
package io.questdb.test.cairo.mv;

import io.questdb.PropertyKey;
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.mv.MatViewTimerJob;
import io.questdb.cairo.wal.seq.TableTransactionLogFile;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collection;

import static io.questdb.cairo.wal.WalUtils.SEQ_DIR;
import static io.questdb.cairo.wal.WalUtils.TXNLOG_FILE_NAME;

@RunWith(Parameterized.class)
public class MatViewRepairReplayTest extends AbstractCairoTest {
    private final String commitMode;
    private final int sequencerPartSize;
    private final TestTimestampType timestampType;

    public MatViewRepairReplayTest(String commitMode, int sequencerPartSize, TestTimestampType timestampType) {
        this.commitMode = commitMode;
        this.sequencerPartSize = sequencerPartSize;
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "{0}, seqPart={1}, {2}")
    public static Collection<Object[]> testParams() {
        final Collection<Object[]> params = new ArrayList<>();
        for (String mode : new String[]{"adaptive", "nosync"}) {
            for (int partSize : new int[]{0, 2}) {
                for (TestTimestampType type : TestTimestampType.values()) {
                    params.add(new Object[]{mode, partSize, type});
                }
            }
        }
        return params;
    }

    @Test
    public void testFullRefreshCancelsDeferredRepair() throws Exception {
        assertMemoryLeak(() -> {
            prepareRepair(false, false);
            assertDeferred();
            execute("REFRESH MATERIALIZED VIEW mv FULL");
            drainMatViewQueue(engine);
            Assert.assertFalse(viewState().isRepairPending());
            replayAndRetry();
            assertRepaired(false);
        });
    }

    @Test
    public void testInvalidationCancelsDeferredRepair() throws Exception {
        assertMemoryLeak(() -> {
            prepareRepair(false, false);
            assertDeferred();
            engine.getMatViewStateStore().enqueueInvalidate(engine.verifyTableName("mv"), "test invalidation");
            drainMatViewQueue(engine);
            Assert.assertFalse(viewState().isRepairPending());
            replayAndRetry();
            Assert.assertTrue(viewState().isInvalid());
            assertQuery("SELECT view_status, invalidation_reason FROM materialized_views()")
                    .noRandomAccess().noLeakCheck().returns("view_status\tinvalidation_reason\ninvalid\ttest invalidation\n");
            restart();
            Assert.assertTrue(viewState().isInvalid());
            Assert.assertFalse(viewState().isRepairPending());
        });
    }

    @Test
    public void testInvalidationRacingRepairIsNotLost() throws Exception {
        assertMemoryLeak(() -> {
            prepareRepair(false, false);
            assertDeferred();
            drainWalAndMatViewQueues();
            currentMicros += 10_000_000;
            drainMatViewTimerQueue(new MatViewTimerJob(engine));
            try (MatViewRefreshJob refresh = createMatViewRefreshJob(engine)) {
                refresh.setOnHoldingLockForTesting(() -> {
                    refresh.setOnHoldingLockForTesting(null);
                    engine.getMatViewStateStore().enqueueInvalidate(engine.verifyTableName("mv"), "racing invalidation");
                    try (MatViewRefreshJob invalidator = createMatViewRefreshJob(engine)) {
                        invalidator.run();
                    }
                });
                drainMatViewQueue(refresh);
            }
            Assert.assertTrue(viewState().isInvalid());
            Assert.assertFalse(viewState().isRepairPending());
            restart();
            Assert.assertTrue(viewState().isInvalid());
            Assert.assertFalse(viewState().isRepairPending());
        });
    }

    @Test
    public void testRepairAfterReplay() throws Exception {
        assertMemoryLeak(() -> {
            prepareRepair(false, false);
            drainWalQueue();
            drainWalAndMatViewQueues();
            assertRepaired(false);
        });
    }

    @Test
    public void testRepairBeforeReplay() throws Exception {
        assertMemoryLeak(() -> {
            prepareRepair(false, false);
            assertDeferred();
            replayAndRetry();
            assertRepaired(false);
        });
    }

    @Test
    public void testRepairCatchesUpCommitsOutsideWindow() throws Exception {
        assertMemoryLeak(() -> {
            prepareRepair(false, false);
            assertDeferred();
            execute("INSERT INTO base VALUES ('2024-01-01T04:10', 40)");
            drainWalAndMatViewQueues(); // consume the base notification while the view is invalid
            retryRepair();
            assertView("""
                    ts\tn\tv
                    2024-01-01T00:00:00.000000Z\t1\t10
                    2024-01-01T01:00:00.000000Z\t1\t20
                    2024-01-01T02:00:00.000000Z\t2\t200
                    2024-01-01T04:00:00.000000Z\t1\t40
                    """);
            Assert.assertEquals(4, viewState().getLastRefreshBaseTxn());
        });
    }

    @Test
    public void testRepairCatchesUpO3CommitsOutsideWindow() throws Exception {
        assertMemoryLeak(() -> {
            prepareRepair(false, false);
            assertDeferred();
            execute("INSERT INTO base VALUES ('2024-01-01T00:20', 40)");
            drainWalAndMatViewQueues();
            retryRepair();
            assertView("""
                    ts\tn\tv
                    2024-01-01T00:00:00.000000Z\t2\t50
                    2024-01-01T01:00:00.000000Z\t1\t20
                    2024-01-01T02:00:00.000000Z\t2\t200
                    """);
            Assert.assertEquals(4, viewState().getLastRefreshBaseTxn());
        });
    }

    @Test
    public void testRepairEmptyRecoveredWindow() throws Exception {
        assertMemoryLeak(() -> {
            prepareRepair(false, true);
            assertDeferred();
            replayAndRetry();
            assertRepaired(true);
        });
    }

    @Test
    public void testRepairWaitsThroughPartialReplay() throws Exception {
        assertMemoryLeak(() -> {
            prepareRepair(true, false);
            assertDeferred();
            replayAndRetry();
            assertRepaired(false);
        });
    }

    @Test
    public void testRestartWhileRepairWaitsForReplay() throws Exception {
        assertMemoryLeak(() -> {
            prepareRepair(false, false);
            assertDeferred();
            restart();
            Assert.assertTrue("restart must re-arm the pending repair", viewState().isRepairPending());
            Assert.assertEquals(3, viewState().getLastRefreshBaseTxn());
            assertDeferred();
            replayAndRetry();
            assertRepaired(false);
        });
    }

    @Test
    public void testRestartWithBaseAdvancedKeepsRepairCut() throws Exception {
        assertMemoryLeak(() -> {
            prepareRepair(false, false);
            assertDeferred();
            execute("INSERT INTO base VALUES ('2024-01-01T04:10', 40)");
            restart();
            Assert.assertTrue(viewState().isRepairPending());
            Assert.assertEquals(3, viewState().getLastRefreshBaseTxn());
            assertDeferred();
            replayAndRetry();
            assertView("""
                    ts\tn\tv
                    2024-01-01T00:00:00.000000Z\t1\t10
                    2024-01-01T01:00:00.000000Z\t1\t20
                    2024-01-01T02:00:00.000000Z\t2\t200
                    2024-01-01T04:00:00.000000Z\t1\t40
                    """);
        });
    }

    private void assertDeferred() throws Exception {
        final TableToken viewToken = engine.verifyTableName("mv");
        try (MatViewRefreshJob refresh = createMatViewRefreshJob(engine)) {
            refresh.run();
            Assert.assertTrue("repair must remain invalid while its base snapshot is behind", viewState().isInvalid());
            Assert.assertTrue(viewState().isRepairPending());
            final long viewTxn = engine.getTableSequencerAPI().lastTxn(viewToken);
            Assert.assertFalse("waiting must not self-feed the refresh queue", refresh.run());
            // Repeated expired retries must neither clear data nor consume the transient-error budget.
            for (int i = 0; i < 3; i++) {
                currentMicros += 10_000_000;
                drainMatViewTimerQueue(new MatViewTimerJob(engine));
                refresh.run();
                Assert.assertEquals(viewTxn, engine.getTableSequencerAPI().lastTxn(viewToken));
                Assert.assertEquals(0, viewState().getRefreshRetryCount());
                Assert.assertTrue(viewState().isInvalid());
                Assert.assertFalse(refresh.run());
            }
        }
    }

    private void assertRepaired(boolean isEmptyWindow) throws Exception {
        assertView(isEmptyWindow ? """
                ts\tn\tv
                2024-01-01T00:00:00.000000Z\t1\t10
                2024-01-01T01:00:00.000000Z\t3\t220
                """ : """
                ts\tn\tv
                2024-01-01T00:00:00.000000Z\t1\t10
                2024-01-01T01:00:00.000000Z\t1\t20
                2024-01-01T02:00:00.000000Z\t2\t200
                """);
        Assert.assertFalse(viewState().isInvalid());
        Assert.assertFalse(viewState().isRepairPending());
        Assert.assertEquals(3, viewState().getLastRefreshBaseTxn());
    }

    private void assertView(String expected) throws Exception {
        assertQuery("SELECT ts, n, v FROM mv ORDER BY ts").timestamp("ts").expectSize().noLeakCheck()
                .returns(timestampType == TestTimestampType.NANO ? expected.replace("000000Z", "000000000Z") : expected);
    }

    private void prepareRepair(boolean isPartialReplay, boolean isEmptyWindow) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "50ms");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
        setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, sequencerPartSize);
        currentMicros = parseFloorPartialTimestamp("2024-01-02T00:00:00");
        execute("CREATE TABLE base (ts " + timestampType.getTypeName() + ", v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE MATERIALIZED VIEW mv AS (SELECT ts, count() n, sum(v) v FROM base SAMPLE BY 1h) PARTITION BY DAY");
        execute("INSERT INTO base VALUES ('2024-01-01T00:10', 10)");
        drainWalAndMatViewQueues();
        if (!isPartialReplay) {
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        }
        execute("INSERT INTO base VALUES ('2024-01-01T01:10', 20)");
        drainWalAndMatViewQueues();
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        execute(isEmptyWindow
                ? "INSERT INTO base VALUES ('2024-01-01T01:20', 100), ('2024-01-01T01:30', 100)"
                : "INSERT INTO base VALUES ('2024-01-01T02:10', 100), ('2024-01-01T02:20', 100)");
        drainWalAndMatViewQueues();
        execute("INSERT INTO base VALUES ('2024-01-01T02:30', 999)");
        drainWalAndMatViewQueues();
        final TableToken baseToken = engine.verifyTableName("base");
        Assert.assertEquals(4, viewState().getLastRefreshBaseTxn());

        // Construct the crash image: the base loses an unflushed sequencer tail, the view survives.
        engine.clear();
        try (RandomAccessFile file = new RandomAccessFile(configuration.getDbRoot() + "/" + baseToken.getDirName()
                + "/" + SEQ_DIR + "/" + TXNLOG_FILE_NAME, "rw")) {
            file.seek(TableTransactionLogFile.MAX_TXN_OFFSET_64);
            file.writeLong(Long.reverseBytes(3));
        }
        new RecoveryCoordinator(engine).recover();
        // Exercise the same post-recovery repair in both modes; this is not a NOSYNC crash simulator.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        engine.getMetadataCache().hydrateAllTables();
        engine.buildViewGraphs();
        try (TableReader reader = engine.getReader(baseToken)) {
            Assert.assertEquals(isPartialReplay ? 2 : 1, reader.getSeqTxn());
        }
        Assert.assertTrue(viewState().isRepairPending());
        Assert.assertEquals(3, viewState().getLastRefreshBaseTxn());
    }

    private void replayAndRetry() throws Exception {
        drainWalQueue();
        retryRepair();
    }

    private void restart() {
        engine.clear();
        new RecoveryCoordinator(engine).recover();
        engine.getMetadataCache().hydrateAllTables();
        engine.buildViewGraphs();
    }

    private void retryRepair() throws Exception {
        currentMicros += 10_000_000;
        drainMatViewTimerQueue(new MatViewTimerJob(engine));
        drainWalAndMatViewQueues();
    }

    private MatViewState viewState() {
        return engine.getMatViewStateStore().getViewState(engine.verifyTableName("mv"));
    }
}
