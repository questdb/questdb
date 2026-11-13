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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.pool.RecentWriteTracker;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.wal.WalDirectoryPolicy;
import io.questdb.cairo.wal.WalListener;
import io.questdb.cairo.wal.WalTxnType;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.std.FilesFacade;
import io.questdb.std.ObjList;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * {@link WalWriter#prepareCommit()}: the part of a plain commit that precedes sequencing, then the
 * {@link WalWriter#commit()} that sequences it, or the rollback or close that abandons it. Runs under every commit
 * mode, and ADAPTIVE with and without a group window.
 */
@RunWith(Parameterized.class)
public class WalWriterPreparedCommitTest extends AbstractCairoTest {
    private static final long DAY_MICROS = 86_400_000_000L;
    private static final long WINDOW_US = 50_000L;
    private final int commitMode;
    private final String commitModeName;
    private final long groupWindowUs;

    public WalWriterPreparedCommitTest(String commitModeName, long groupWindowUs) {
        this.commitModeName = commitModeName;
        this.commitMode = CommitMode.fromString(commitModeName);
        this.groupWindowUs = groupWindowUs;
    }

    @Parameterized.Parameters(name = "{0}, W={1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"sync", 0L},
                {"adaptive", WINDOW_US},
                {"adaptive", 0L},
                {"nosync", 0L},
                {"async", 0L},
        });
    }

    @Before
    @Override
    public void setUp() {
        super.setUp();
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitModeName);
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, groupWindowUs);
        setCurrentMicros(1_000_000_000L);
    }

    @Test
    public void testAbandonedOnlyRecordOfLaterSegmentRollsBackItsDirectory() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken tt = createTable();
            final ClosureRecorder recorder = new ClosureRecorder();
            recorder.install();
            try {
                final int walId;
                try (WalWriter w = engine.getWalWriter(tt)) {
                    walId = w.getWalId();
                    appendRows(w, 1, 2);
                    w.commit();
                    w.rollSegment();
                    Assert.assertEquals(1, w.getSegmentId());
                    appendRows(w, 3, 1);
                    w.prepareCommit();
                    w.rollback();
                }
                Assert.assertEquals("segment 0 holds a sequenced record", "segmentClosed wal" + walId + "/0 txn=1", recorder.closures(walId, 0));
                Assert.assertEquals("segment 1 holds only the abandoned record", "rollbackDirectory wal" + walId + "/1", recorder.closures(walId, 1));
            } finally {
                recorder.uninstall();
            }
            drainWalQueue();
            assertRows("v\n1\n2\n");
        });
    }

    @Test
    public void testAbandonedOnlyRecordRollsBackSegmentDirectory() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken tt = createTable();
            final ClosureRecorder recorder = new ClosureRecorder();
            recorder.install();
            try {
                final int walId;
                try (WalWriter w = engine.getWalWriter(tt)) {
                    walId = w.getWalId();
                    appendRows(w, 1, 2);
                    w.prepareCommit();
                    w.rollback();
                }
                Assert.assertEquals("rollbackDirectory wal" + walId + "/0", recorder.closures(walId, 0));
                Assert.assertEquals("a never-sequenced writer reports no WAL closure", "", recorder.walClosures(walId));
            } finally {
                recorder.uninstall();
            }
            Assert.assertEquals(0, engine.getTableSequencerAPI().lastTxn(tt));
        });
    }

    @Test
    public void testAbandonedRecordAfterSequencedRecordIsNotCountedAtRollbackThenClose() throws Exception {
        assertAbandonedRecordAfterSequencedRecord(true);
    }

    @Test
    public void testAbandonedRecordAfterSequencedRecordIsNotCountedAtTenantClose() throws Exception {
        assertAbandonedRecordAfterSequencedRecord(false);
    }

    @Test
    public void testAppendFailureDuringPrepareDistressesWriter() throws Exception {
        // A record larger than the event file's first page makes the append map the next page, which fails.
        node1.setProperty(PropertyKey.CAIRO_WAL_WRITER_EVENT_APPEND_PAGE_SIZE, 4096);
        final EventFileFaultFacade ff = new EventFileFaultFacade();
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE t (v LONG, s SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final TableToken tt = engine.verifyTableName("t");
            final ClosureRecorder recorder = new ClosureRecorder();
            recorder.install();
            try {
                final int walId;
                try (WalWriter w = engine.getWalWriter(tt)) {
                    walId = w.getWalId();
                    final TableWriter.Row row = w.newRow(DAY_MICROS);
                    row.putLong(0, 1);
                    // a new symbol goes into the event record, which then outgrows the first page
                    row.putSym(1, "x".repeat(10_000));
                    row.append();
                    ff.isMapFailureArmed = true;
                    try {
                        w.prepareCommit();
                        Assert.fail("the append must fail");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "test: event file map failed");
                    } finally {
                        ff.isMapFailureArmed = false;
                    }
                    Assert.assertTrue("the event record append failed", ff.mapFailures > 0);
                    Assert.assertTrue(w.isDistressed());
                    Assert.assertFalse(w.isCommitPrepared());
                }
                Assert.assertEquals("rollbackDirectory wal" + walId + "/0", recorder.closures(walId, 0));
            } finally {
                recorder.uninstall();
            }
            Assert.assertEquals(0, engine.getTableSequencerAPI().lastTxn(tt));
            insertRows(tt, 5, 1);
            drainWalQueue();
            assertRows("v\n5\n");
        });
    }

    @Test
    public void testBackgroundFlushBetweenSequencingAndDueFlush() throws Exception {
        Assume.assumeTrue(isGroupWindow());
        final WalFlushCountingFacade ff = new WalFlushCountingFacade();
        assertMemoryLeak(ff, () -> {
            assertModeAndWindow();
            final TableToken tt = createTable();
            try (WalWriter w = engine.getWalWriter(tt)) {
                appendRows(w, 1, 1);
                w.commit();
                setCurrentMicros(1_000_000_000L + WINDOW_US);
                appendRows(w, 2, 1);
                w.prepareCommit();
                ff.arm();
                w.commit();
                Assert.assertEquals("the due flush waits for flushDueGroupCommit", 0, ff.getCount(WalFlushCountingFacade.SEQUENCER));
                // The background flusher takes the writer's batch between the sequencing and the due flush.
                final CountDownLatch swept = new CountDownLatch(1);
                final AtomicReference<Throwable> sweepFailure = new AtomicReference<>();
                final Thread flusher = new Thread(() -> {
                    try {
                        engine.getWalGroupCommitFlushQueue().sweep(WINDOW_US);
                    } catch (Throwable th) {
                        sweepFailure.set(th);
                    } finally {
                        swept.countDown();
                    }
                });
                flusher.start();
                swept.await();
                flusher.join();
                Assert.assertNull(sweepFailure.get());
                Assert.assertEquals(ff.getLog(), 1, ff.getCount(WalFlushCountingFacade.SEQUENCER));
                Assert.assertEquals(2, engine.getTableSequencerAPI().getTxnTracker(tt).getLocalDurableSeqTxn());
                w.flushDueGroupCommit();
                Assert.assertEquals("the background flush left nothing to flush", 1, ff.getCount(WalFlushCountingFacade.SEQUENCER));
                ff.disarm();
            }
            Assert.assertEquals(2, engine.getTableSequencerAPI().getTxnTracker(tt).getLocalDurableSeqTxn());
        });
    }

    @Test
    public void testCleanupBeforeCloseClearsDueGroupFlush() throws Exception {
        Assume.assumeTrue(isGroupWindow());
        final WalFlushCountingFacade ff = new WalFlushCountingFacade();
        assertMemoryLeak(ff, () -> {
            assertModeAndWindow();
            final TableToken tt = createTable();
            final int walId;
            try (WalWriter w = engine.getWalWriter(tt)) {
                walId = w.getWalId();
                appendRows(w, 1, 1);
                w.commit();
                setCurrentMicros(1_000_000_000L + WINDOW_US);
                appendRows(w, 2, 1);
                w.prepareCommit();
                w.commit();
                ff.arm();
                // no flushDueGroupCommit(): the pool return flushes
            }
            Assert.assertEquals(ff.getLog(), 1, ff.getCount(WalFlushCountingFacade.SEQUENCER));
            Assert.assertEquals(2, engine.getTableSequencerAPI().getTxnTracker(tt).getLocalDurableSeqTxn());
            try (WalWriter w = engine.getWalWriter(tt)) {
                Assert.assertEquals("the pool hands back the same writer", walId, w.getWalId());
                // A new batch, within its window: a due flag left over from before the pool return would flush it.
                appendRows(w, 3, 1);
                w.commit();
                w.flushDueGroupCommit();
                Assert.assertEquals("nothing is left due: " + ff.getLog(), 1, ff.getCount(WalFlushCountingFacade.SEQUENCER));
                ff.disarm();
            }
        });
    }

    @Test
    public void testCloseAbandonsPreparedCommit() throws Exception {
        assertMemoryLeak(() -> {
            assertModeAndWindow();
            final TableToken tt = createTable();
            final int walId;
            try (WalWriter w = engine.getWalWriter(tt)) {
                walId = w.getWalId();
                appendRows(w, 1, 3);
                w.prepareCommit();
                Assert.assertTrue(w.isCommitPrepared());
            }
            Assert.assertEquals("nothing is sequenced", 0, engine.getTableSequencerAPI().lastTxn(tt));
            try (WalWriter w = engine.getWalWriter(tt)) {
                Assert.assertNotEquals("the pool discarded the abandoned writer", walId, w.getWalId());
                appendRows(w, 10, 2);
                w.commit();
            }
            drainWalQueue();
            assertRows("v\n10\n11\n");
        });
    }

    @Test
    public void testConcurrentAddColumnAtFirstTxnOfSegment() throws Exception {
        assertConcurrentDdl(false, "ALTER TABLE t ADD COLUMN c INT", "v\tc\n1\tnull\n2\tnull\n");
    }

    @Test
    public void testConcurrentAddColumnAtLaterTxnOfSegment() throws Exception {
        assertConcurrentDdl(true, "ALTER TABLE t ADD COLUMN c INT", "v\tc\n0\tnull\n1\tnull\n2\tnull\n");
    }

    @Test
    public void testConcurrentAddIndex() throws Exception {
        assertConcurrentDdl(true, "ALTER TABLE t ALTER COLUMN s ADD INDEX", "v\ts\n0\ta\n1\ta\n2\ta\n");
    }

    @Test
    public void testConcurrentAddSymbolColumnAtFirstTxnOfSegment() throws Exception {
        assertConcurrentDdl(false, "ALTER TABLE t ADD COLUMN c SYMBOL", "v\tc\n1\t\n2\t\n");
    }

    @Test
    public void testConcurrentAddSymbolColumnAtLaterTxnOfSegment() throws Exception {
        assertConcurrentDdl(true, "ALTER TABLE t ADD COLUMN c SYMBOL", "v\tc\n0\t\n1\t\n2\t\n");
    }

    @Test
    public void testConcurrentAlterColumnType() throws Exception {
        assertConcurrentDdl(true, "ALTER TABLE t ALTER COLUMN v TYPE INT", "v\n0\n1\n2\n");
    }

    @Test
    public void testConcurrentDedupEnable() throws Exception {
        assertConcurrentDdl(true, "ALTER TABLE t DEDUP ENABLE UPSERT KEYS(ts)", "v\n0\n1\n2\n");
    }

    @Test
    public void testConcurrentDropColumn() throws Exception {
        assertConcurrentDdl(true, "ALTER TABLE t DROP COLUMN s", "v\n0\n1\n2\n");
    }

    @Test
    public void testConcurrentDropTableFailsCommit() throws Exception {
        assertConcurrentTableDdlFailsCommit("DROP TABLE t");
    }

    @Test
    public void testConcurrentRenameColumn() throws Exception {
        assertConcurrentDdl(true, "ALTER TABLE t RENAME COLUMN s TO s2", "v\ts2\n0\ta\n1\ta\n2\ta\n");
    }

    @Test
    public void testConcurrentRenameTableFailsCommit() throws Exception {
        assertConcurrentTableDdlFailsCommit("RENAME TABLE t TO t2");
    }

    @Test
    public void testDueFlushSkipsWriterDistressedAfterSequencing() throws Exception {
        Assume.assumeTrue(isGroupWindow());
        final WalFlushCountingFacade ff = new WalFlushCountingFacade();
        final ThrowingRecentWriteTracker tracker = new ThrowingRecentWriteTracker(configuration.getRecentWriteTrackerCapacity());
        assertMemoryLeak(ff, () -> {
            assertModeAndWindow();
            final TableToken tt = createTable();
            try (CairoEngine trackerEngine = newTrackerEngine(tracker)) {
                try (WalWriter w = trackerEngine.getWalWriter(tt)) {
                    appendRows(w, 1, 1);
                    w.commit();
                    setCurrentMicros(1_000_000_000L + WINDOW_US);
                    appendRows(w, 2, 1);
                    w.prepareCommit();
                    tracker.isArmed = true;
                    try {
                        w.commit();
                        Assert.fail("the tracker must throw");
                    } catch (IllegalStateException e) {
                        Assert.assertEquals("test: tracker failed", e.getMessage());
                    } finally {
                        tracker.isArmed = false;
                    }
                    Assert.assertTrue(w.isDistressed());
                    ff.arm();
                    w.flushDueGroupCommit();
                    Assert.assertEquals("a distressed writer leaves the flush to its close", 0, ff.getCount(WalFlushCountingFacade.SEQUENCER));
                }
                Assert.assertEquals(ff.getLog(), 1, ff.getCount(WalFlushCountingFacade.SEQUENCER));
                ff.disarm();
                Assert.assertEquals(2, trackerEngine.getTableSequencerAPI().getTxnTracker(tt).getLocalDurableSeqTxn());
                assertTrackerEngineRows(trackerEngine, "v\n1\n2\n");
            }
        });
    }

    @Test
    public void testFaultAfterAssignmentDistressesWriterAndKeepsRecord() throws Exception {
        final ThrowingRecentWriteTracker tracker = new ThrowingRecentWriteTracker(configuration.getRecentWriteTrackerCapacity());
        assertMemoryLeak(() -> {
            assertModeAndWindow();
            final TableToken tt = createTable();
            try (CairoEngine trackerEngine = newTrackerEngine(tracker)) {
                try (WalWriter w = trackerEngine.getWalWriter(tt)) {
                    appendRows(w, 1, 2);
                    w.prepareCommit();
                    tracker.isArmed = true;
                    try {
                        w.commit();
                        Assert.fail("the tracker must throw");
                    } catch (IllegalStateException e) {
                        Assert.assertEquals("test: tracker failed", e.getMessage());
                    } finally {
                        tracker.isArmed = false;
                    }
                    Assert.assertTrue("the txn may be sequenced: the writer must not be reused", w.isDistressed());
                    Assert.assertFalse(w.isCommitPrepared());
                }
                Assert.assertEquals(1, trackerEngine.getTableSequencerAPI().lastTxn(tt));
                assertTrackerEngineRows(trackerEngine, "v\n1\n2\n");
            }
        });
    }

    @Test
    public void testFlushDueGroupCommitWithoutDueFlushDoesNotFlush() throws Exception {
        Assume.assumeTrue(isGroupWindow());
        final WalFlushCountingFacade ff = new WalFlushCountingFacade();
        assertMemoryLeak(ff, () -> {
            assertModeAndWindow();
            final TableToken tt = createTable();
            try (WalWriter w = engine.getWalWriter(tt)) {
                final long frontierBefore = engine.getTableSequencerAPI().getTxnTracker(tt).getLocalDurableSeqTxn();
                appendRows(w, 1, 1);
                w.prepareCommit();
                w.commit();
                Assert.assertEquals("the batch is pending", frontierBefore, engine.getTableSequencerAPI().getTxnTracker(tt).getLocalDurableSeqTxn());
                ff.arm();
                w.flushDueGroupCommit();
                Assert.assertEquals(ff.getLog(), 0, ff.getCount(WalFlushCountingFacade.SEQUENCER));
                Assert.assertEquals(frontierBefore, engine.getTableSequencerAPI().getTxnTracker(tt).getLocalDurableSeqTxn());
                ff.disarm();
            }
        });
    }

    @Test
    public void testGuardedOperationsRefusePreparedWriter() throws Exception {
        assertMemoryLeak(() -> {
            assertModeAndWindow();
            final TableToken tt = createTable();
            final List<String> refused = new ArrayList<>();
            final GuardedOperation[] operations = {
                    w -> w.prepareCommit(),
                    w -> w.markSavepoint(),
                    w -> w.rollbackToSavepoint(),
                    w -> w.getColumnarRowAppender().beginColumnarWrite(1),
                    w -> w.apply(new AlterOperation(), true),
                    w -> {
                        try (UpdateOperation op = new UpdateOperation(tt, 1, 0, 0, new ObjList<>())) {
                            w.apply(op);
                        }
                    },
                    w -> w.addColumn("x", ColumnType.INT, AllowAllSecurityContext.INSTANCE),
                    w -> w.addColumn("x", ColumnType.INT, 128, true, (byte) 0, 256, false, AllowAllSecurityContext.INSTANCE),
                    w -> w.truncateSoft(),
                    w -> w.rollSegment(),
                    w -> w.appendCustomEvent((byte) 64, mem -> mem.putInt(1)),
                    w -> w.commitMatView(0, 0, 0, 0, Long.MAX_VALUE),
                    w -> w.commitLiveView(0),
                    w -> w.commitLiveViewWithReplaceRange(0, 0, Long.MAX_VALUE),
                    w -> w.commitWithParams(0, 0, WalUtils.WAL_DEDUP_MODE_DEFAULT),
                    w -> w.commitWithParamsIfSeqTxn(0, 0, 0, WalUtils.WAL_DEDUP_MODE_DEFAULT),
                    w -> w.resetMatViewState(0, 0, false, null, 0, null, 0),
                    w -> w.commitRebaseSeed(),
                    w -> w.goActive(),
                    w -> w.goActive(Long.MAX_VALUE),
            };
            final String[] expectedOperations = {
                    "prepare a commit",
                    "mark a savepoint",
                    "roll back to a savepoint",
                    "begin a columnar write",
                    "apply ALTER",
                    "apply UPDATE",
                    "add a column",
                    "add a column",
                    "truncate",
                    "roll the segment",
                    "append a custom event",
                    "commit a materialized view",
                    "commit a live view",
                    "commit a live view",
                    "commit with parameters",
                    "commit with parameters",
                    "reset the materialized view state",
                    "commit the rebase seed",
                    "apply structure changes",
                    "apply structure changes",
            };
            final StringBuilder expectedRows = new StringBuilder("v\n");
            try (WalWriter w = engine.getWalWriter(tt)) {
                for (int i = 0; i < operations.length; i++) {
                    appendRows(w, i, 1);
                    expectedRows.append(i).append('\n');
                    w.prepareCommit();
                    final long segmentTxnBefore = engine.getTableSequencerAPI().lastTxn(tt);
                    try {
                        operations[i].run(w);
                        Assert.fail("operation " + i + " must be refused");
                    } catch (CairoException e) {
                        Assert.assertTrue(e.isCritical());
                        Assert.assertEquals(
                                "cannot " + expectedOperations[i] + " while a WAL commit is prepared [table=t, wal=" + w.getWalId() + "]",
                                e.getFlyweightMessage().toString()
                        );
                        refused.add(expectedOperations[i]);
                    }
                    Assert.assertTrue("still prepared after " + expectedOperations[i], w.isCommitPrepared());
                    Assert.assertFalse(w.isDistressed());
                    w.commit();
                    Assert.assertEquals(segmentTxnBefore + 1, engine.getTableSequencerAPI().lastTxn(tt));
                    Assert.assertEquals(segmentTxnBefore + 1, w.getLastSeqTxn());
                }
            }
            Assert.assertEquals(operations.length, refused.size());
            drainWalQueue();
            assertRows(expectedRows.toString());
        });
    }

    @Test
    public void testPoisonAfterAssignmentAtNoWindow() throws Exception {
        Assume.assumeTrue(commitMode == CommitMode.ADAPTIVE && groupWindowUs == 0);
        assertMemoryLeak(() -> {
            assertModeAndWindow();
            final TableToken tt = createTable();
            final WalWriter.DeferredCommitInterceptor previous = WalWriter.deferredCommitInterceptor;
            WalWriter.deferredCommitInterceptor = new WalWriter.DeferredCommitInterceptor() {
                @Override
                public void onSequencedBeforeDurablePublish(int walId, long seqTxn) {
                    try {
                        engine.handleDataSyncFailureInEveryMode(
                                CairoException.dataSyncFailure(engine.getConfiguration().getFilesFacade(), 5, "fdatasync", "test: poisoned after assignment")
                        );
                    } catch (CairoError expected) {
                        // the returning durability handler of a test engine
                    }
                }

                @Override
                public void onSequencedBeforePin(int walId, long seqTxn) {
                }
            };
            try {
                try (WalWriter w = engine.getWalWriter(tt)) {
                    appendRows(w, 1, 2);
                    w.prepareCommit();
                    try {
                        w.commit();
                        Assert.fail("the publish re-check must refuse a poisoned engine");
                    } catch (CairoError e) {
                        TestUtils.assertContains(e.getMessage(), "engine is poisoned");
                    }
                    Assert.assertTrue(w.isDistressed());
                    Assert.assertFalse(w.isCommitPrepared());
                }
                Assert.assertEquals("the txn is sequenced", 1, engine.getTableSequencerAPI().lastTxn(tt));
                // With no group window the sequencer publishes the txn locally durable as it assigns it.
                Assert.assertEquals(1, engine.getTableSequencerAPI().getTxnTracker(tt).getLocalDurableSeqTxn());
            } finally {
                WalWriter.deferredCommitInterceptor = previous;
                engine.releaseAllWriters();
                engine.resetDurabilityFailure();
            }
            // The record the poisoned writer left behind is intact: the sequenced txn applies.
            drainWalQueue();
            assertRows("v\n1\n2\n");
        });
    }

    @Test
    public void testPoisonedEngineAfterPrepare() throws Exception {
        final WalFlushCountingFacade ff = new WalFlushCountingFacade();
        assertMemoryLeak(ff, () -> {
            assertModeAndWindow();
            final TableToken tt = createTable();
            final ClosureRecorder recorder = new ClosureRecorder();
            recorder.install();
            try {
                final int walId;
                try (WalWriter w = engine.getWalWriter(tt)) {
                    walId = w.getWalId();
                    appendRows(w, 1, 2);
                    w.prepareCommit();
                    try {
                        engine.handleDataSyncFailureInEveryMode(
                                CairoException.dataSyncFailure(engine.getConfiguration().getFilesFacade(), 5, "fdatasync", "test: poisoned after prepare")
                        );
                        Assert.fail();
                    } catch (CairoError expected) {
                        // the returning durability handler of a test engine
                    }
                    try {
                        w.commit();
                        Assert.fail("a poisoned engine refuses the commit");
                    } catch (CairoError e) {
                        TestUtils.assertContains(e.getMessage(), "engine is poisoned");
                    }
                    Assert.assertTrue(w.isDistressed());
                    Assert.assertTrue("the refused commit leaves the record prepared", w.isCommitPrepared());
                    ff.arm();
                    w.rollback();
                    Assert.assertEquals("abandoning does no file work: " + ff.getLog(), 0, ff.getCount(WalFlushCountingFacade.PRIVATE) + ff.getCount(WalFlushCountingFacade.SEQUENCER));
                    ff.disarm();
                    Assert.assertFalse(w.isCommitPrepared());
                    Assert.assertTrue(w.isDistressed());
                }
                Assert.assertEquals("rollbackDirectory wal" + walId + "/0", recorder.closures(walId, 0));
            } finally {
                recorder.uninstall();
                engine.releaseAllWriters();
                engine.resetDurabilityFailure();
            }
            Assert.assertEquals(0, engine.getTableSequencerAPI().lastTxn(tt));
        });
    }

    @Test
    public void testPrepareFailureAfterAppendIsNotCounted() throws Exception {
        Assume.assumeTrue("only SYNC and ADAPTIVE flush the event file before sequencing", CommitMode.isPowerLossDurable(commitMode));
        final EventFileFaultFacade ff = new EventFileFaultFacade();
        assertMemoryLeak(ff, () -> {
            assertModeAndWindow();
            final TableToken tt = createTable();
            final ClosureRecorder recorder = new ClosureRecorder();
            recorder.install();
            try {
                final int walId;
                try (WalWriter w = engine.getWalWriter(tt)) {
                    walId = w.getWalId();
                    appendRows(w, 1, 2);
                    ff.isFlushFailureArmed = true;
                    try {
                        w.prepareCommit();
                        Assert.fail("the event flush must fail");
                    } catch (CairoException e) {
                        // not a data-sync failure, so not fatal: the writer survives to its close
                        Assert.assertFalse(CairoException.isDataSyncFailure(e));
                        TestUtils.assertContains(e.getFlyweightMessage(), "test: event file flush failed");
                    } finally {
                        ff.isFlushFailureArmed = false;
                    }
                    Assert.assertEquals(1, ff.flushFailures);
                    Assert.assertFalse(engine.isDurabilityFailed());
                    Assert.assertTrue(w.isDistressed());
                    Assert.assertFalse(w.isCommitPrepared());
                }
                Assert.assertEquals("the record the failed prepare appended is not counted", "rollbackDirectory wal" + walId + "/0", recorder.closures(walId, 0));
                Assert.assertEquals("", recorder.walClosures(walId));
            } finally {
                recorder.uninstall();
            }
            Assert.assertEquals(0, engine.getTableSequencerAPI().lastTxn(tt));
        });
    }

    @Test
    public void testPrepareRefusesDistressedWriter() throws Exception {
        assertMemoryLeak(() -> {
            assertModeAndWindow();
            final TableToken tt = createTable();
            try (WalWriter w = engine.getWalWriter(tt)) {
                appendRows(w, 1, 1);
                w.prepareCommit();
                appendRows(w, 2, 1);
                try {
                    w.commit();
                    Assert.fail();
                } catch (CairoException expected) {
                    // the row guard distresses the writer
                }
                try {
                    // still prepared: the prepare guard answers first
                    w.prepareCommit();
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "cannot prepare a commit while a WAL commit is prepared");
                }
                w.rollback();
                Assert.assertFalse(w.isCommitPrepared());
                try {
                    w.prepareCommit();
                    Assert.fail("a distressed writer must be refused");
                } catch (CairoException e) {
                    Assert.assertTrue(e.isCritical());
                    Assert.assertEquals(
                            "WAL writer is distressed and cannot be used any more [table=t, wal=" + w.getWalId() + "]",
                            e.getFlyweightMessage().toString()
                    );
                }
                Assert.assertFalse(w.isCommitPrepared());
            }
        });
    }

    @Test
    public void testPrepareRefusesEmptyTransaction() throws Exception {
        assertMemoryLeak(() -> {
            assertModeAndWindow();
            final TableToken tt = createTable();
            try (WalWriter w = engine.getWalWriter(tt)) {
                try {
                    w.prepareCommit();
                    Assert.fail();
                } catch (CairoException e) {
                    Assert.assertTrue(e.isCritical());
                    Assert.assertEquals(
                            "cannot prepare a WAL commit without rows [table=t, wal=" + w.getWalId() + "]",
                            e.getFlyweightMessage().toString()
                    );
                }
                Assert.assertFalse(w.isCommitPrepared());
                Assert.assertFalse("a refused prepare leaves the writer healthy", w.isDistressed());
                appendRows(w, 1, 1);
                w.prepareCommit();
                w.commit();
            }
            drainWalQueue();
            assertRows("v\n1\n");
        });
    }

    @Test
    public void testPrepareRefusesWriterMidColumnarWrite() throws Exception {
        assertMemoryLeak(() -> {
            assertModeAndWindow();
            final TableToken tt = createTable();
            try (WalWriter w = engine.getWalWriter(tt)) {
                appendRows(w, 1, 1);
                w.getColumnarRowAppender().beginColumnarWrite(1);
                try {
                    w.prepareCommit();
                    Assert.fail();
                } catch (CairoException e) {
                    Assert.assertEquals("cannot prepare a commit during columnar write", e.getFlyweightMessage().toString());
                }
                Assert.assertFalse(w.isCommitPrepared());
                Assert.assertFalse(w.isDistressed());
                w.getColumnarRowAppender().cancelColumnarWrite();
                w.prepareCommit();
                w.commit();
            }
            drainWalQueue();
            assertRows("v\n1\n");
        });
    }

    @Test
    public void testPreparedCommitEqualsPlainCommit() throws Exception {
        assertMemoryLeak(() -> {
            assertModeAndWindow();
            execute("CREATE TABLE plain (v LONG, s SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE prepared (v LONG, s SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final TableToken plainToken = engine.verifyTableName("plain");
            final TableToken preparedToken = engine.verifyTableName("prepared");
            try (
                    WalWriter plain = engine.getWalWriter(plainToken);
                    WalWriter prepared = engine.getWalWriter(preparedToken)
            ) {
                for (int txn = 0; txn < 3; txn++) {
                    // out of order within the transaction, so the record's out-of-order flag matters to apply
                    appendOutOfOrderRows(plain, txn);
                    appendOutOfOrderRows(prepared, txn);
                    plain.commit();
                    prepared.prepareCommit();
                    Assert.assertEquals("a prepare does not touch the uncommitted rows", 3, prepared.getUncommittedRowCount());
                    prepared.commit();
                    prepared.flushDueGroupCommit();
                    Assert.assertEquals(plain.getLastSeqTxn(), prepared.getLastSeqTxn());
                    Assert.assertEquals(plain.getSegmentId(), prepared.getSegmentId());
                    Assert.assertEquals(0, prepared.getUncommittedRowCount());
                    Assert.assertEquals(
                            engine.getTableSequencerAPI().getTxnTracker(plainToken).getLocalDurableSeqTxn(),
                            engine.getTableSequencerAPI().getTxnTracker(preparedToken).getLocalDurableSeqTxn()
                    );
                }
            }
            Assert.assertEquals(
                    engine.getTableSequencerAPI().getTxnTracker(plainToken).getLocalDurableSeqTxn(),
                    engine.getTableSequencerAPI().getTxnTracker(preparedToken).getLocalDurableSeqTxn()
            );
            if (commitMode == CommitMode.ADAPTIVE) {
                Assert.assertEquals(3, engine.getTableSequencerAPI().getTxnTracker(preparedToken).getLocalDurableSeqTxn());
            }
            drainWalQueue();
            final String expectedRows = """
                    v\ts\tts
                    1\ts0\t1970-01-02T00:00:00.000001Z
                    0\ts0\t1970-01-02T00:00:00.000002Z
                    2\ts0\t1970-01-02T00:00:00.000003Z
                    11\ts1\t1970-01-02T00:00:00.000011Z
                    10\ts1\t1970-01-02T00:00:00.000012Z
                    12\ts1\t1970-01-02T00:00:00.000013Z
                    21\ts2\t1970-01-02T00:00:00.000021Z
                    20\ts2\t1970-01-02T00:00:00.000022Z
                    22\ts2\t1970-01-02T00:00:00.000023Z
                    """;
            assertQuery("SELECT v, s, ts FROM plain").timestamp("ts").expectSize().returns(expectedRows);
            assertQuery("SELECT v, s, ts FROM prepared").timestamp("ts").expectSize().returns(expectedRows);
            final String walTransactions = "SELECT sequencerTxn, segmentId, segmentTxn FROM wal_transactions('%s')";
            final String expectedTransactions = """
                    sequencerTxn\tsegmentId\tsegmentTxn
                    1\t0\t0
                    2\t0\t1
                    3\t0\t2
                    """;
            assertQuery(String.format(walTransactions, "plain")).noRandomAccess().returns(expectedTransactions);
            assertQuery(String.format(walTransactions, "prepared")).noRandomAccess().returns(expectedTransactions);
        });
    }

    @Test
    public void testPreparedCommitAtRowCountRollover() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 4);
        assertPreparedCommitAtRollover(4);
    }

    @Test
    public void testPreparedCommitAtSizeRollover() throws Exception {
        // the smallest threshold; 40 rows of 28 bytes pass it
        node1.setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_SIZE, 1024);
        assertPreparedCommitAtRollover(40);
    }

    @Test
    public void testPurgeRemovesAbandonedSegment() throws Exception {
        assertMemoryLeak(() -> {
            assertModeAndWindow();
            final TableToken tt = createTable();
            final int abandonedWalId;
            try (WalWriter w = engine.getWalWriter(tt)) {
                abandonedWalId = w.getWalId();
                appendRows(w, 1, 1);
                w.commit();
                // the abandoned record follows a sequenced one in segment 0
                appendRows(w, 2, 1);
                w.prepareCommit();
            }
            final int onlyRecordWalId;
            try (WalWriter w = engine.getWalWriter(tt)) {
                onlyRecordWalId = w.getWalId();
                // the abandoned record is the only record of its segment
                appendRows(w, 3, 1);
                w.prepareCommit();
            }
            try (WalWriter w = engine.getWalWriter(tt)) {
                appendRows(w, 4, 1);
                w.commit();
            }
            drainWalQueue();
            assertRows("v\n1\n4\n");
            engine.releaseAllWalWriters();
            advanceSavePoints(engine, tt);
            drainPurgeJob();
            assertWalExistence(false, tt, abandonedWalId);
            assertWalExistence(false, tt, onlyRecordWalId);
        });
    }

    @Test
    public void testPurgeRemovesAbandonedSegmentAfterRestart() throws Exception {
        assertMemoryLeak(() -> {
            assertModeAndWindow();
            final TableToken tt = createTable();
            final int abandonedWalId;
            try (WalWriter w = engine.getWalWriter(tt)) {
                abandonedWalId = w.getWalId();
                appendRows(w, 1, 1);
                w.commit();
                appendRows(w, 2, 1);
                w.prepareCommit();
            }
            engine.releaseAllWalWriters();
            engine.releaseInactiveTableSequencers();
            // A new engine on the same root, as after a restart, applies and purges.
            try (CairoEngine restarted = new CairoEngine(configuration)) {
                drainWalQueue(restarted);
                assertQuery("SELECT v FROM t")
                        .withEngine(restarted)
                        .withContext(TestUtils.createSqlExecutionCtx(restarted))
                        .noLeakCheck()
                        .expectSize()
                        .returns("v\n1\n");
                restarted.releaseAllWalWriters();
                advanceSavePoints(restarted, tt);
                TestUtils.drainPurgeJob(restarted);
            }
            assertWalExistence(false, tt, abandonedWalId);
        });
    }

    @Test
    public void testRollbackAbandonsPreparedCommit() throws Exception {
        assertMemoryLeak(() -> {
            assertModeAndWindow();
            final TableToken tt = createTable();
            final int walId;
            try (WalWriter w = engine.getWalWriter(tt)) {
                walId = w.getWalId();
                appendRows(w, 1, 3);
                w.prepareCommit();
                w.rollback();
                Assert.assertTrue(w.isDistressed());
                Assert.assertFalse(w.isCommitPrepared());
                // A second rollback, as a REVOKE's emergency rollback and then the close make, changes nothing.
                w.rollback();
                Assert.assertTrue(w.isDistressed());
            }
            Assert.assertEquals("nothing is sequenced", 0, engine.getTableSequencerAPI().lastTxn(tt));
            try (WalWriter w = engine.getWalWriter(tt)) {
                Assert.assertNotEquals("the pool discarded the abandoned writer", walId, w.getWalId());
                appendRows(w, 10, 2);
                w.commit();
            }
            drainWalQueue();
            assertRows("v\n10\n11\n");
        });
    }

    @Test
    public void testRowGuardRefusesRowsAppendedAfterPrepare() throws Exception {
        assertMemoryLeak(() -> {
            assertModeAndWindow();
            final TableToken tt = createTable();
            final ClosureRecorder recorder = new ClosureRecorder();
            recorder.install();
            try {
                final int walId;
                try (WalWriter w = engine.getWalWriter(tt)) {
                    walId = w.getWalId();
                    appendRows(w, 1, 2);
                    w.prepareCommit();
                    appendRows(w, 3, 1);
                    try {
                        w.commit();
                        Assert.fail("the row guard must refuse the commit");
                    } catch (CairoException e) {
                        Assert.assertTrue(e.isCritical());
                        Assert.assertEquals(
                                "rows changed under a prepared WAL commit [table=t, wal=" + walId + ", preparedRows=2, rows=3]",
                                e.getFlyweightMessage().toString()
                        );
                    }
                    Assert.assertTrue(w.isDistressed());
                    Assert.assertTrue("the close stops counting the record", w.isCommitPrepared());
                }
                Assert.assertEquals("rollbackDirectory wal" + walId + "/0", recorder.closures(walId, 0));
            } finally {
                recorder.uninstall();
            }
            Assert.assertEquals("no row of the transaction is stored", 0, engine.getTableSequencerAPI().lastTxn(tt));
            insertRows(tt, 7, 1);
            drainWalQueue();
            assertRows("v\n7\n");
        });
    }

    private static void appendOutOfOrderRows(WalWriter w, int txn) {
        final long base = DAY_MICROS + txn * 10L;
        final long[] tsOffsets = {2, 1, 3};
        for (int i = 0; i < 3; i++) {
            final TableWriter.Row row = w.newRow(base + tsOffsets[i]);
            row.putLong(0, txn * 10L + i);
            row.putSym(1, "s" + txn);
            row.append();
        }
    }

    private static void appendRows(WalWriter w, long firstValue, int count) {
        // By name: an ALTER COLUMN TYPE moves a column to a new index.
        final int vIndex = w.getMetadata().getColumnIndex("v");
        final int sIndex = w.getMetadata().getColumnIndexQuiet("s");
        final boolean isVLong = w.getMetadata().getColumnType(vIndex) == ColumnType.LONG;
        for (int i = 0; i < count; i++) {
            final TableWriter.Row row = w.newRow(DAY_MICROS + firstValue + i);
            if (isVLong) {
                row.putLong(vIndex, firstValue + i);
            } else {
                row.putInt(vIndex, (int) (firstValue + i));
            }
            if (sIndex > -1) {
                row.putSym(sIndex, "a");
            }
            row.append();
        }
    }

    // ADAPTIVE keeps an applied WAL until a second save point covers it.
    private static void advanceSavePoints(CairoEngine engine, TableToken tt) {
        if (engine.getConfiguration().getCommitMode() == CommitMode.ADAPTIVE) {
            try (TableWriter writer = engine.getWriter(tt, "test")) {
                writer.advanceDurableEpoch(10_000L);
                writer.advanceDurableEpoch(20_000L);
            }
        }
    }

    private static void insertRows(TableToken tt, long firstValue, int count) {
        try (WalWriter w = engine.getWalWriter(tt)) {
            appendRows(w, firstValue, count);
            w.commit();
        }
    }

    private void assertAbandonedRecordAfterSequencedRecord(boolean isRollbackFirst) throws Exception {
        assertMemoryLeak(() -> {
            assertModeAndWindow();
            final TableToken tt = createTable();
            final ClosureRecorder recorder = new ClosureRecorder();
            recorder.install();
            try {
                final int walId;
                try (WalWriter w = engine.getWalWriter(tt)) {
                    walId = w.getWalId();
                    appendRows(w, 1, 2);
                    w.commit();
                    appendRows(w, 3, 1);
                    w.prepareCommit();
                    if (isRollbackFirst) {
                        w.rollback();
                    }
                }
                // A second decrement would report the segment's one sequenced record as none, and roll back its
                // directory.
                Assert.assertEquals("segmentClosed wal" + walId + "/0 txn=1", recorder.closures(walId, 0));
                Assert.assertEquals("walClosed wal" + walId + " txn=1", recorder.walClosures(walId));
            } finally {
                recorder.uninstall();
            }
            drainWalQueue();
            assertRows("v\n1\n2\n");
        });
    }

    private void assertConcurrentDdl(boolean isLaterTxnOfSegment, String ddl, String expected) throws Exception {
        Assume.assumeTrue("the batch prepares only under SYNC and ADAPTIVE", CommitMode.isPowerLossDurable(commitMode));
        final WalFlushCountingFacade ff = new WalFlushCountingFacade();
        assertMemoryLeak(ff, () -> {
            assertModeAndWindow();
            final TableToken tt = createTable();
            try (WalWriter w = engine.getWalWriter(tt)) {
                if (isLaterTxnOfSegment) {
                    appendRows(w, 0, 1);
                    w.commit();
                }
                appendRows(w, 1, 2);
                w.prepareCommit();
                final int segmentBefore = w.getSegmentId();
                // Another writer's DDL lands between the prepare and the sequencing: the sequencing replays it.
                execute(ddl);
                final long ddlTxn = engine.getTableSequencerAPI().lastTxn(tt);
                ff.arm();
                w.commit();
                ff.disarm();
                w.flushDueGroupCommit();
                Assert.assertFalse(w.isDistressed());
                Assert.assertEquals(ddlTxn + 1, w.getLastSeqTxn());
                final boolean isSymbolColumnAdded = ddl.endsWith("SYMBOL");
                final boolean isRolled = isLaterTxnOfSegment && (ddl.contains("ADD COLUMN") || ddl.contains("DROP COLUMN")
                        || ddl.contains("RENAME COLUMN")) || ddl.contains(" TYPE ");
                Assert.assertEquals("the replay rolls the prepared record to a new segment: " + ddl,
                        segmentBefore + (isRolled ? 1 : 0), w.getSegmentId());
                if (isSymbolColumnAdded) {
                    // The replay rewrites the prepared record with the new column's null flag, and flushes the
                    // rewrite before the record is sequenced.
                    Assert.assertTrue("the rewritten record is flushed: " + ff.getLog(), ff.getLog().contains("_event"));
                }
                appendRows(w, 3, 1);
                w.commit();
            }
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tt));
            final String columns = expected.substring(0, expected.indexOf('\n')).replace('\t', ',');
            assertQuery("SELECT " + columns + " FROM t WHERE v < 3").returns(expected);
            assertQuery("SELECT count() FROM t").noRandomAccess().expectSize().returns("count\n" + (isLaterTxnOfSegment ? 4 : 3) + "\n");
        });
    }

    private void assertConcurrentTableDdlFailsCommit(String ddl) throws Exception {
        Assume.assumeTrue("the batch prepares only under SYNC and ADAPTIVE", CommitMode.isPowerLossDurable(commitMode));
        assertMemoryLeak(() -> {
            assertModeAndWindow();
            final TableToken tt = createTable();
            try (WalWriter w = engine.getWalWriter(tt)) {
                appendRows(w, 1, 2);
                w.prepareCommit();
                execute(ddl);
                try {
                    w.commit();
                    Assert.fail("the commit must fail as at head: " + ddl);
                } catch (CairoException e) {
                    // DROP fails the sequencing with a dropped table
                    Assert.assertTrue(e.getFlyweightMessage().toString(), ddl.startsWith("DROP") && e.isTableDropped());
                } catch (TableReferenceOutOfDateException e) {
                    // RENAME with an out-of-date reference
                    Assert.assertTrue(e.getFlyweightMessage().toString(), ddl.startsWith("RENAME"));
                }
                Assert.assertTrue(w.isDistressed());
                Assert.assertFalse(w.isCommitPrepared());
            }
        });
    }

    private void assertModeAndWindow() {
        Assert.assertEquals(commitMode, engine.getConfiguration().getCommitMode());
        Assert.assertEquals(groupWindowUs, engine.getConfiguration().getAdaptiveCommitGroupWindowUs());
    }

    private void assertPreparedCommitAtRollover(int rowCount) throws Exception {
        assertMemoryLeak(() -> {
            assertModeAndWindow();
            final TableToken tt = createTable();
            try (WalWriter w = engine.getWalWriter(tt)) {
                appendRows(w, 1, rowCount);
                w.prepareCommit();
                w.commit();
                Assert.assertEquals(0, w.getSegmentId());
                // the next row rolls the segment
                appendRows(w, rowCount + 1, 1);
                Assert.assertEquals(1, w.getSegmentId());
                w.prepareCommit();
                w.commit();
                w.flushDueGroupCommit();
            }
            drainWalQueue();
            assertQuery("SELECT count(), min(v), max(v) FROM t").noRandomAccess().expectSize().returns("count\tmin\tmax\n" + (rowCount + 1) + "\t1\t" + (rowCount + 1) + "\n");
            assertQuery("SELECT sequencerTxn, segmentId, segmentTxn FROM wal_transactions('t')").noRandomAccess().returns("""
                    sequencerTxn\tsegmentId\tsegmentTxn
                    1\t0\t0
                    2\t1\t0
                    """);
        });
    }

    private void assertTrackerEngineRows(CairoEngine trackerEngine, String expected) throws Exception {
        // The engine that sequenced the txn applies it.
        drainWalQueue(trackerEngine);
        assertQuery("SELECT v FROM t")
                .withEngine(trackerEngine)
                .withContext(TestUtils.createSqlExecutionCtx(trackerEngine))
                .noLeakCheck()
                .expectSize()
                .returns(expected);
    }

    private void assertRows(String expected) throws Exception {
        assertQuery("SELECT v FROM t").expectSize().returns(expected);
    }

    private TableToken createTable() throws Exception {
        execute("CREATE TABLE t (v LONG, s SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        return engine.verifyTableName("t");
    }

    private boolean isGroupWindow() {
        return commitMode == CommitMode.ADAPTIVE && groupWindowUs > 0;
    }

    private CairoEngine newTrackerEngine(RecentWriteTracker tracker) {
        return new CairoEngine(configuration) {
            @Override
            public RecentWriteTracker getRecentWriteTracker() {
                return tracker;
            }
        };
    }

    @FunctionalInterface
    private interface GuardedOperation {
        void run(WalWriter writer) throws Exception;
    }

    /**
     * Records each segment's closure: a {@code segmentClosed} notification with the reported txn, or a
     * {@code rollbackDirectory} of its directory.
     */
    private static final class ClosureRecorder {
        private final List<String> events = new ArrayList<>();
        private WalDirectoryPolicy previousPolicy;
        private WalListener previousListener;

        synchronized String closures(int walId, int segmentId) {
            final String segment = "wal" + walId + "/" + segmentId;
            final StringBuilder sb = new StringBuilder();
            for (String event : events) {
                if (event.contains(segment + " ") || event.endsWith(segment)) {
                    if (!sb.isEmpty()) {
                        sb.append('\n');
                    }
                    sb.append(event);
                }
            }
            return sb.toString();
        }

        void install() {
            previousPolicy = engine.getWalDirectoryPolicy();
            previousListener = engine.getWalListener();
            final WalDirectoryPolicy delegatePolicy = previousPolicy;
            final WalListener delegateListener = previousListener;
            engine.setWalDirectoryPolicy(new WalDirectoryPolicy() {
                @Override
                public void initDirectory(Path dirPath) {
                    delegatePolicy.initDirectory(dirPath);
                }

                @Override
                public boolean isInUse(Path path) {
                    return delegatePolicy.isInUse(path);
                }

                @Override
                public void rollbackDirectory(Path path) {
                    final String p = path.toString();
                    final int walAt = p.lastIndexOf(WalUtils.WAL_NAME_BASE);
                    record("rollbackDirectory " + p.substring(walAt).replace('\\', '/'));
                    delegatePolicy.rollbackDirectory(path);
                }

                @Override
                public boolean truncateFilesOnClose() {
                    return delegatePolicy.truncateFilesOnClose();
                }
            });
            engine.setWalListener(new WalListener() {
                @Override
                public void dataTxnCommitted(TableToken tableToken, long txn, long timestamp, int walId, int segmentId, int segmentTxn) {
                    delegateListener.dataTxnCommitted(tableToken, txn, timestamp, walId, segmentId, segmentTxn);
                }

                @Override
                public void nonDataTxnCommitted(TableToken tableToken, long txn, long timestamp) {
                    delegateListener.nonDataTxnCommitted(tableToken, txn, timestamp);
                }

                @Override
                public void segmentClosed(TableToken tableToken, long txn, int walId, int segmentId) {
                    record("segmentClosed wal" + walId + "/" + segmentId + " txn=" + txn);
                    delegateListener.segmentClosed(tableToken, txn, walId, segmentId);
                }

                @Override
                public void tableCreated(TableToken tableToken, long timestamp) {
                    delegateListener.tableCreated(tableToken, timestamp);
                }

                @Override
                public void tableDropped(TableToken tableToken, long txn, long timestamp) {
                    delegateListener.tableDropped(tableToken, txn, timestamp);
                }

                @Override
                public void tableRenamed(TableToken tableToken, long txn, long timestamp, TableToken oldTableToken) {
                    delegateListener.tableRenamed(tableToken, txn, timestamp, oldTableToken);
                }

                @Override
                public void walClosed(TableToken tableToken, long txn, int walId) {
                    record("walClosed wal" + walId + " txn=" + txn);
                    delegateListener.walClosed(tableToken, txn, walId);
                }
            });
        }

        void uninstall() {
            engine.setWalDirectoryPolicy(previousPolicy);
            engine.setWalListener(previousListener);
        }

        synchronized String walClosures(int walId) {
            final StringBuilder sb = new StringBuilder();
            for (String event : events) {
                if (event.startsWith("walClosed wal" + walId + " ")) {
                    if (!sb.isEmpty()) {
                        sb.append('\n');
                    }
                    sb.append(event);
                }
            }
            return sb.toString();
        }

        private synchronized void record(String event) {
            events.add(event);
        }
    }

    /**
     * Fails, while armed, the mapping of a new page of a WAL event file, or a flush of one with a plain
     * (not data-sync) error.
     */
    private static final class EventFileFaultFacade extends WalFlushCountingFacade {
        private final java.util.Set<Long> eventFds = java.util.concurrent.ConcurrentHashMap.newKeySet();
        private final java.util.concurrent.ConcurrentHashMap<Long, Long> eventMappings = new java.util.concurrent.ConcurrentHashMap<>();
        volatile int flushFailures;
        volatile boolean isFlushFailureArmed;
        volatile boolean isMapFailureArmed;
        volatile int mapFailures;

        @Override
        public void barrierFsync(long fd) {
            failFlush(fd);
            super.barrierFsync(fd);
        }

        @Override
        public boolean close(long fd) {
            eventFds.remove(fd);
            return super.close(fd);
        }

        @Override
        public void fdatasync(long fd) {
            failFlush(fd);
            super.fdatasync(fd);
        }

        @Override
        public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
            if (isMapFailureArmed && eventFds.contains(fd)) {
                mapFailures++;
                throw CairoException.critical(12).put("test: event file map failed");
            }
            final long addr = super.mmap(fd, len, offset, flags, memoryTag);
            if (eventFds.contains(fd)) {
                eventMappings.put(addr, fd);
            }
            return addr;
        }

        @Override
        public long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
            if (isMapFailureArmed && eventFds.contains(fd)) {
                mapFailures++;
                throw CairoException.critical(12).put("test: event file map failed");
            }
            return super.mremap(fd, addr, previousSize, newSize, offset, mode, memoryTag);
        }

        @Override
        public void msync(long addr, long len, boolean async) {
            if (!async) {
                for (java.util.Map.Entry<Long, Long> e : eventMappings.entrySet()) {
                    if (addr >= e.getKey() && addr < e.getKey() + 64L * 1024 * 1024) {
                        failFlush(e.getValue());
                        break;
                    }
                }
            }
            super.msync(addr, len, async);
        }

        @Override
        public void munmap(long address, long size, int memoryTag) {
            eventMappings.remove(address);
            super.munmap(address, size, memoryTag);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            final long fd = super.openRW(name, opts);
            if (fd > -1 && Utf8s.endsWithAscii(name, WalUtils.EVENT_FILE_NAME)) {
                eventFds.add(fd);
            }
            return fd;
        }

        private void failFlush(long fd) {
            if (isFlushFailureArmed && eventFds.contains(fd)) {
                isFlushFailureArmed = false;
                flushFailures++;
                throw CairoException.critical(5).put("test: event file flush failed");
            }
        }
    }

    /**
     * Throws a non-Cairo error, while armed, when a commit records its write: after its txn is sequenced.
     */
    private static final class ThrowingRecentWriteTracker extends RecentWriteTracker {
        volatile boolean isArmed;

        ThrowingRecentWriteTracker(int maxCapacity) {
            super(maxCapacity);
        }

        @Override
        public void recordWalWrite(@NotNull TableToken tableToken, long sequencerTxn, long walTimestamp, long txnRowCount) {
            if (isArmed) {
                throw new IllegalStateException("test: tracker failed");
            }
            super.recordWalWrite(tableToken, sequencerTxn, walTimestamp, txnRowCount);
        }
    }
}
