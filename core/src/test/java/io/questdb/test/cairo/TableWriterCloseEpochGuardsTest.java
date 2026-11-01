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

package io.questdb.test.cairo;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.pool.AbstractPool;
import io.questdb.cairo.pool.WriterPool;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The close-time durable-epoch flush of a {@link TableWriter} must neither hide a failed constructor's real
 * exception nor swallow a data-sync failure.
 *
 * <p>{@code doClose()} runs from the constructor's own catch block, when {@code columns},
 * {@code denseSymbolMapWriters} and {@code denseIndexers} may be only partly built while
 * {@code effectiveCommitMode} is already ADAPTIVE. An error from an epoch flush that walks those lists would
 * replace the real one, and {@code ApplyWal2TableJob.purgeTableFiles}, which catches only
 * {@code CairoException}, would then leave a dropped table's files on disk.
 */
public class TableWriterCloseEpochGuardsTest extends AbstractCairoTest {

    /**
     * A data-sync (fsync/msync) failure during the close epoch is indeterminate, so the broad catch around the
     * close epoch must not soften it: the failure still poisons the engine and surfaces as a
     * {@code CairoError}. Swallowing it would let a writer close cleanly after an epoch whose data never
     * reached the device. The failed epoch itself is skipped, not corrupted: {@code advanceDurableEpoch}
     * writes the inactive {@code _epoch.manifest} generation and its header last.
     */
    @Test
    public void testCloseEpochDataSyncFailureRemainsFatal() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, "1h");
        final OneShotFsyncFailureFacade ff = new OneShotFsyncFailureFacade();
        try {
            assertMemoryLeak(ff, () -> {
                execute("create table t2 (ts timestamp, sym symbol, v long) timestamp(ts) partition by day wal");
                execute("insert into t2 values ('2024-01-01T00:00:00.000000Z', 'a', 1)");
                drainWalQueue();
                execute("insert into t2 values ('2024-01-01T00:00:01.000000Z', 'b', 2)");
                drainWalQueue();

                final TableToken tt = engine.verifyTableName("t2");
                engine.releaseInactive();
                // Same restart model as the test below: leaves an un-epoched tail, so the close epoch is
                // eligible and reaches an fsync.
                engine.getTableSequencerAPI().purgeTxnTracker(tt.getDirName());

                final java.util.concurrent.atomic.AtomicInteger fatalCalls =
                        new java.util.concurrent.atomic.AtomicInteger();
                engine.setDurabilityFailureHandler(failure -> fatalCalls.incrementAndGet());

                // Off-pool writer: a pooled writer's close() only returns the tenant to the pool and never
                // reaches doClose(), so the epoch would not run.
                final TableWriter w = newOffPoolWriter("t2");
                ff.failNext = true;
                Throwable seen = null;
                try {
                    w.close();
                } catch (Throwable t) {
                    seen = t;
                }

                // Fail-stop: the failure surfaces as a data-sync CairoError.
                Assert.assertNotNull("a data-sync failure inside the close epoch must remain FATAL, not be "
                        + "swallowed as best-effort", seen);
                Assert.assertTrue("the fatal signal must be a CairoError", seen instanceof CairoError);
                Assert.assertTrue("the fatal error must be recognised as a data-sync failure",
                        CairoException.isDataSyncFailure(seen));
                Assert.assertEquals("the injected fsync failure must actually have been reached",
                        1, ff.injected);
                Assert.assertEquals("the durability failure handler must fire exactly once",
                        1, fatalCalls.get());
                Assert.assertTrue("the engine must be poisoned after an indeterminate writeback failure",
                        engine.isDurabilityFailed());
                // The writer must still release everything: doClose() raises the fatal error only after its
                // frees, so no fd, native memory or table lock leaks. The surrounding assertMemoryLeak()
                // checks fds and allocations, so no explicit assert is needed here.
            });
        } finally {
            engine.resetDurabilityFailure();
        }
    }

    /**
     * A test-crash teardown reclaims a writer the test thread left checked out. When that writer's close-time
     * epoch fails, the reclaim must still evict the entry and leave the table unlocked.
     */
    @Test
    public void testCrashOrphanedWriterCloseEpochFailureReleasesLock() throws Exception {
        final OneShotFsyncFailureFacade ff = new OneShotFsyncFailureFacade();
        assertMemoryLeak(() -> {
            final String root = temp.newFolder().getAbsolutePath();
            try (CairoEngine e2 = newPrivateEngine(root, ff, CommitMode.ADAPTIVE)) {
                final AtomicInteger fatalCalls = new AtomicInteger();
                e2.setDurabilityFailureHandler(failure -> fatalCalls.incrementAndGet());
                createIdlePooledTables(e2, "o1");
                final TableToken tt = e2.verifyTableName("o1");
                // Left checked out on purpose, as a crashed test leaves it. The reclaim below closes it.
                e2.getWriter(tt, "test");

                ff.failNext = true;
                final Throwable seen = catchThrowable(e2::releaseCrashOrphanedWriters);

                assertFatalDataSyncFailure(seen, ff, fatalCalls);
                assertEntryEvicted("the orphan reclaim must evict an entry whose writer close threw", e2, tt);
                Assert.assertFalse("the orphan reclaim must leave the table unlocked", isTableLockHeld(root, ff, tt));
            }
        });
    }

    /**
     * Open a writer on a table whose symbol offset file has been removed underneath it, the half-deleted
     * state a failed WAL drop leaves. The constructor must fail with the real
     * {@code CairoException}, not with an {@code AssertionError} thrown by the close-time epoch flush.
     */
    @Test
    public void testFailedOpenPropagatesRealErrorUnderAdaptive() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        // A long interval, so the cadence gate cannot fire an epoch of its own mid-test; the test creates
        // the eligibility gap below by discarding the in-memory tracker.
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, "1h");
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, sym symbol, v long) timestamp(ts) partition by day wal");
            execute("insert into t values ('2024-01-01T00:00:00.000000Z', 'a', 1)");
            drainWalQueue();
            execute("insert into t values ('2024-01-01T00:00:01.000000Z', 'b', 2)");
            drainWalQueue();

            final TableToken tt = engine.verifyTableName("t");

            // Drop every cached writer so the next open re-reads from disk. This close is a healthy one, so
            // it fires the graceful-close epoch and moves durableEpochSeqTxn up to seqTxn.
            engine.releaseInactive();

            // Model a restart: discard the in-memory tracker, so a fresh one starts at durableEpochSeqTxn=0
            // while _txn on disk is still at the applied seqTxn. That gap makes the close-epoch block
            // eligible on the next open, as after any process restart and in the failed-drop retry. Without
            // it the block short-circuits on seqTxn == durableEpochSeqTxn.
            engine.getTableSequencerAPI().purgeTxnTracker(tt.getDirName());

            // Remove the symbol offset file: configureSymbolTable() fails mid-construction, leaving the
            // column and symbol lists partly built, as the failed-drop path does.
            try (Path p = new Path()) {
                p.of(configuration.getDbRoot()).concat(tt).concat("sym.o");
                Assert.assertTrue("test setup: sym.o must exist before removal",
                        configuration.getFilesFacade().exists(p.$()));
                Assert.assertTrue("test setup: sym.o must be removable",
                        configuration.getFilesFacade().removeQuiet(p.$()));
            }

            Assert.assertEquals(
                    "test setup: a fresh tracker must report no durable epoch, leaving the close-epoch "
                            + "block eligible on the failing open",
                    0, engine.getTableSequencerAPI().getTxnTracker(tt).getDurableEpochSeqTxn());

            try (TableWriter ignored = getWriter(tt)) {
                Assert.fail("opening a writer with sym.o missing must fail");
            } catch (CairoException e) {
                // The real failure, surfaced intact.
                io.questdb.test.tools.TestUtils.assertContains(e.getFlyweightMessage(), "SymbolMap does not exist");
            } catch (AssertionError e) {
                // The best-effort close epoch walked half-built state and its AssertionError replaced the
                // constructor's CairoException.
                throw new AssertionError(
                        "the close-time durable-epoch flush ran on a PARTIALLY-CONSTRUCTED writer and its "
                                + "error replaced the real open failure: " + e, e);
            }
        });
    }

    /**
     * In NOSYNC a writer close raises no data-sync failure, so the same facade leaves engine teardown as it was:
     * no throw, and every table lock released.
     */
    @Test
    public void testNosyncEngineCloseWithFailingFsyncIsUnchanged() throws Exception {
        final OneShotFsyncFailureFacade ff = new OneShotFsyncFailureFacade();
        assertMemoryLeak(() -> {
            final String root = temp.newFolder().getAbsolutePath();
            final String[] tables = {"n1", "n2", "n3"};
            final TableToken[] tokens = new TableToken[tables.length];
            final CairoEngine e2 = newPrivateEngine(root, ff, CommitMode.NOSYNC);
            try {
                createIdlePooledTables(e2, tables);
                for (int i = 0; i < tables.length; i++) {
                    tokens[i] = e2.verifyTableName(tables[i]);
                }
            } catch (Throwable th) {
                e2.close();
                throw th;
            }
            ff.failNext = true;
            e2.close();
            ff.failNext = false;
            Assert.assertFalse("a NOSYNC engine must not be poisoned by its close", e2.isDurabilityFailed());
            for (TableToken tt : tokens) {
                Assert.assertFalse("engine.close() must release every pooled table's lock [table=" + tt.getTableName() + ']',
                        isTableLockHeld(root, ff, tt));
            }
        });
    }

    /**
     * A failed fsync in the close-time epoch of one pooled writer must not stop {@code engine.close()} midway: every
     * other pooled writer is closed and its table lock released, every other engine component is freed, and the
     * caller still gets the fatal error. A new engine on the same root then opens every table.
     */
    @Test
    public void testPooledCloseEpochFailureEngineCloseReleasesEveryTable() throws Exception {
        final OneShotFsyncFailureFacade ff = new OneShotFsyncFailureFacade();
        assertMemoryLeak(() -> {
            final String root = temp.newFolder().getAbsolutePath();
            final String[] tables = {"a1", "a2", "a3"};
            final TableToken[] tokens = new TableToken[tables.length];
            final AtomicInteger fatalCalls = new AtomicInteger();
            final CairoEngine e2 = newPrivateEngine(root, ff, CommitMode.ADAPTIVE);
            try {
                e2.setDurabilityFailureHandler(failure -> fatalCalls.incrementAndGet());
                createIdlePooledTables(e2, tables);
                for (int i = 0; i < tables.length; i++) {
                    tokens[i] = e2.verifyTableName(tables[i]);
                }
            } catch (Throwable th) {
                e2.close();
                throw th;
            }

            ff.failNext = true;
            final Throwable seen = catchThrowable(e2::close);

            assertFatalDataSyncFailure(seen, ff, fatalCalls);
            for (TableToken tt : tokens) {
                Assert.assertFalse("engine.close() must release every pooled table's lock [table=" + tt.getTableName() + ']',
                        isTableLockHeld(root, ff, tt));
            }
            try (CairoEngine e3 = newPrivateEngine(root, ff, CommitMode.ADAPTIVE)) {
                // The failed epoch left the tail to the WAL replay.
                TestUtils.drainWalQueue(e3);
                for (String table : tables) {
                    try (TableWriter w = e3.getWriter(e3.verifyTableName(table), "test")) {
                        Assert.assertEquals("a new engine must open the table with its rows [table=" + table + ']',
                                2, w.size());
                    }
                }
            }
        });
    }

    /**
     * A failed fsync in the close-time epoch of an idle writer must not leave its pool entry owned by the reaper:
     * every eligible entry is evicted, every table lock released, and a later lock of the table succeeds instead
     * of spinning.
     */
    @Test
    public void testPooledCloseEpochFailureIdleReapEvictsEveryEntry() throws Exception {
        final OneShotFsyncFailureFacade ff = new OneShotFsyncFailureFacade();
        assertMemoryLeak(() -> {
            final String root = temp.newFolder().getAbsolutePath();
            try (CairoEngine e2 = newPrivateEngine(root, ff, CommitMode.ADAPTIVE)) {
                final AtomicInteger fatalCalls = new AtomicInteger();
                e2.setDurabilityFailureHandler(failure -> fatalCalls.incrementAndGet());
                createIdlePooledTables(e2, "b1", "b2");
                final TableToken[] tokens = {e2.verifyTableName("b1"), e2.verifyTableName("b2")};

                ff.failNext = true;
                final Throwable seen = catchThrowable(e2::releaseInactive);

                assertFatalDataSyncFailure(seen, ff, fatalCalls);
                for (TableToken tt : tokens) {
                    // Checked before any other thread touches the table: a stuck entry would make it spin.
                    assertEntryEvicted("the idle reap must evict every entry, also one whose writer close threw", e2, tt);
                    Assert.assertFalse("the idle reap must leave the table unlocked [table=" + tt.getTableName() + ']',
                            isTableLockHeld(root, ff, tt));
                }
                for (TableToken tt : tokens) {
                    assertLockAllFromOtherThread(e2, tt);
                }
            }
        });
    }

    /**
     * A failed fsync in the close-time epoch of the writer that {@code lockAll} closes must leave nothing locked:
     * not the writer pool entry, not the metadata pools, not the table lock. The caller gets the fatal error.
     */
    @Test
    public void testPooledCloseEpochFailureLockAllLeavesNothingLocked() throws Exception {
        final OneShotFsyncFailureFacade ff = new OneShotFsyncFailureFacade();
        assertMemoryLeak(() -> {
            final String root = temp.newFolder().getAbsolutePath();
            try (CairoEngine e2 = newPrivateEngine(root, ff, CommitMode.ADAPTIVE)) {
                final AtomicInteger fatalCalls = new AtomicInteger();
                e2.setDurabilityFailureHandler(failure -> fatalCalls.incrementAndGet());
                createIdlePooledTables(e2, "d1");
                final TableToken tt = e2.verifyTableName("d1");

                ff.failNext = true;
                final Throwable seen = catchThrowable(() -> e2.lockAll(tt, "test", false));

                assertFatalDataSyncFailure(seen, ff, fatalCalls);
                final WriterPool.Entry entry = e2.getWriterPoolEntries().get(tt.getDirName());
                if (entry != null) {
                    Assert.assertEquals("the writer pool entry must be free after a failed writer close in lock()",
                            AbstractPool.UNALLOCATED, entry.getOwnerThread());
                }
                Assert.assertFalse("a failed lock must leave the table unlocked", isTableLockHeld(root, ff, tt));
                assertLockAllFromOtherThread(e2, tt);
            }
        });
    }

    private static void assertEntryEvicted(String message, CairoEngine engine, TableToken tt) {
        final WriterPool.Entry entry = engine.getWriterPoolEntries().get(tt.getDirName());
        if (entry != null) {
            Assert.fail(message + " [table=" + tt.getTableName() + ", owner=" + entry.getOwnerThread() + ']');
        }
    }

    private static void assertFatalDataSyncFailure(Throwable seen, OneShotFsyncFailureFacade ff, AtomicInteger fatalCalls) {
        Assert.assertNotNull("the failed close-time epoch must reach the caller", seen);
        Assert.assertTrue("the failure must be the fatal CairoError, got: " + seen, seen instanceof CairoError);
        Assert.assertTrue("the failure must be a data-sync failure", CairoException.isDataSyncFailure(seen));
        Assert.assertEquals("the injected fsync failure must have been reached", 1, ff.injected);
        Assert.assertEquals("the durability failure handler must fire exactly once", 1, fatalCalls.get());
    }

    private static void assertLockAllFromOtherThread(CairoEngine engine, TableToken tt) throws InterruptedException {
        final AtomicReference<String> result = new AtomicReference<>("<not returned>");
        final Thread locker = new Thread(() -> {
            try {
                final String reason = engine.lockAll(tt, "test", false);
                if (reason == null) {
                    engine.unlock(null, tt, null, false);
                }
                result.set(String.valueOf(reason));
            } catch (Throwable th) {
                result.set("threw " + th);
            } finally {
                Path.clearThreadLocals();
            }
        });
        locker.setDaemon(true);
        locker.start();
        locker.join(5_000);
        Assert.assertFalse("lockAll after a failed writer close must not spin [table=" + tt.getTableName() + ']',
                locker.isAlive());
        Assert.assertEquals("lockAll after a failed writer close must succeed [table=" + tt.getTableName() + ']',
                "null", result.get());
    }

    private static Throwable catchThrowable(Runnable code) {
        try {
            code.run();
            return null;
        } catch (Throwable th) {
            return th;
        }
    }

    // Two applied commits per table, so the tail is past the last epoch and every writer close runs an epoch
    // that reaches an fsync.
    private static void createIdlePooledTables(CairoEngine engine, String... tables) throws Exception {
        for (String table : tables) {
            engine.execute("create table " + table + " (ts timestamp, v long) timestamp(ts) partition by day wal");
        }
        for (String table : tables) {
            engine.execute("insert into " + table + " values ('2024-01-01T00:00:00.000000Z', 1)");
        }
        TestUtils.drainWalQueue(engine);
        for (String table : tables) {
            engine.execute("insert into " + table + " values ('2024-01-01T00:00:01.000000Z', 2)");
        }
        TestUtils.drainWalQueue(engine);
    }

    private static boolean isTableLockHeld(String root, FilesFacade ff, TableToken tt) {
        try (Path path = new Path()) {
            path.of(root).concat(tt.getDirName());
            final long fd = TableUtils.lock(ff, TableUtils.lockName(path), false);
            if (fd == -1) {
                return true;
            }
            ff.close(fd);
            return false;
        }
    }

    private static CairoEngine newPrivateEngine(String root, FilesFacade ff, int commitMode) {
        return new CairoEngine(new DefaultCairoConfiguration(root) {
            @Override
            public int getCommitMode() {
                return commitMode;
            }

            @Override
            public FilesFacade getFilesFacade() {
                return ff;
            }
        });
    }

    /**
     * Fails the next {@code fsync} with a genuine data-sync failure, then behaves normally. One-shot, so the
     * failure lands inside the close epoch and nothing else in the test is disturbed.
     */
    private static final class OneShotFsyncFailureFacade extends io.questdb.test.std.TestFilesFacadeImpl {
        boolean failNext;
        int injected;

        @Override
        public void fsync(long fd) {
            if (failNext) {
                failNext = false;
                injected++;
                throw CairoException.dataSyncFailure(this, 5, "fsync", "simulated fsync failure during close epoch [fd=" + fd + ']');
            }
            super.fsync(fd);
        }
    }
}
