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
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

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
                throw CairoException.dataSyncFailure(5, "fsync")
                        .put("simulated fsync failure during close epoch [fd=").put(fd).put(']');
            }
            super.fsync(fd);
        }
    }
}
