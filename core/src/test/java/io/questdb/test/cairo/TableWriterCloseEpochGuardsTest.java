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
 * A {@link TableWriter} whose CONSTRUCTOR fails must propagate the REAL open failure, and its
 * partially-built state must not be walked by any close-time action.
 *
 * <p>{@code doClose()} is invoked from the constructor's own catch block, so at that point
 * {@code columns} / {@code denseSymbolMapWriters} / {@code denseIndexers} may be only partly populated —
 * while {@code effectiveCommitMode} was resolved much earlier and is already ADAPTIVE. The
 * graceful-close durable-epoch flush therefore passed all of its guards and walked those half-built
 * lists, dying with {@code AssertionError: index out of bounds}. That is neither a
 * {@code CairoException} nor a {@code CairoError}, so the block's "best effort" catch missed it: the
 * AssertionError escaped {@code doClose()} and REPLACED the constructor's real exception.
 *
 * <p>The consequence was not merely a confusing error. {@code ApplyWal2TableJob.purgeTableFiles} catches
 * only {@code CairoException} when it reopens a half-deleted table to release its files, so the
 * substituted AssertionError aborted the WAL drop-table retry chain and left the table's files —
 * {@code _txn} included — on disk permanently. That end-to-end symptom is
 * {@code WalTableSqlTest.testDropFailedWhileSymbolFileLocked}; this test pins the mechanism directly, so
 * a regression is diagnosed here rather than as a mystifying failure three layers away.
 */
public class TableWriterCloseEpochGuardsTest extends AbstractCairoTest {

    /**
     * THE DURABILITY GUARANTEE. Widening the close-epoch's catch to {@code Throwable} must NOT soften the
     * fail-stop contract: a genuine data-sync (fsync/msync) failure during that epoch is INDETERMINATE —
     * the device may or may not hold the bytes — so it must still poison the engine and propagate as a
     * {@code CairoError}, never be logged-and-swallowed as "best effort".
     *
     * <p>That still holds because the catch routes to {@code handleBestEffortDurableEpochFailure}, which
     * re-raises via {@code engine.handleDataSyncFailure} for anything
     * {@code CairoException.isDataSyncFailure} recognises (it walks the whole cause chain). This test pins
     * that rather than leaving it as an argument: swallowing here would let a writer close "cleanly" after
     * an epoch whose payloads never reached the device, and a later boot would then trust a
     * {@code _snapshot} marker whose data is not actually durable.
     *
     * <p>Note the ordering that keeps this safe even so: {@code advanceDurableEpoch} writes into the
     * INACTIVE ping-pong generation and publishes the {@code _snapshot} marker LAST, after the payload
     * fsyncs. A failure part-way therefore leaves the previous generation authoritative — the epoch is
     * skipped, never corrupted.
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
                // Same restart model as the test below: leaves an un-epoched tail so the close-epoch is
                // eligible and actually reaches an fsync.
                engine.getTableSequencerAPI().purgeTxnTracker(tt.getDirName());

                final java.util.concurrent.atomic.AtomicInteger fatalCalls =
                        new java.util.concurrent.atomic.AtomicInteger();
                engine.setDurabilityFailureHandler(failure -> fatalCalls.incrementAndGet());

                // Off-pool writer: a POOLED writer's close() only returns the tenant to the pool and never
                // reaches doClose(), so the epoch would not run at all. Same pattern TableWriterTest uses
                // for its fatal directory-fsync test.
                final TableWriter w = newOffPoolWriter("t2");
                ff.failNext = true;
                Throwable seen = null;
                try {
                    w.close();
                } catch (Throwable t) {
                    seen = t;
                }

                // FAIL-STOP: the failure must surface, as a data-sync CairoError.
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
                // ...AND the writer must still have released everything. Raising the fatal error from the
                // point of failure would skip doClose()'s frees, leaking fds and native memory and
                // stranding the table lock -- on the very path whose job is to release them. That half is
                // enforced by the surrounding assertMemoryLeak()'s fd/allocation check, which failed
                // loudly ("cached file descriptors: 7 ... actual: 19") until the rethrow was deferred to
                // the end of doClose(). No explicit assert here: the harness IS the assert.
            });
        } finally {
            resetDurabilityPoisonForTest();
        }
    }

    /**
     * Open a writer on a table whose symbol offset file has been removed underneath it (exactly the
     * half-deleted state a failed WAL drop leaves). The constructor must fail with the real
     * {@code CairoException}, not with an {@code AssertionError} thrown by the close-time epoch flush.
     */
    @Test
    public void testFailedOpenPropagatesRealErrorUnderAdaptive() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        // Long interval so the cadence gate cannot fire an epoch of its own mid-test; the eligibility gap
        // the bug needs is created deliberately below by discarding the in-memory tracker.
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, "1h");
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, sym symbol, v long) timestamp(ts) partition by day wal");
            execute("insert into t values ('2024-01-01T00:00:00.000000Z', 'a', 1)");
            drainWalQueue();
            execute("insert into t values ('2024-01-01T00:00:01.000000Z', 'b', 2)");
            drainWalQueue();

            final TableToken tt = engine.verifyTableName("t");

            // Drop every cached writer so the next open really re-reads from disk. NOTE this close is a
            // HEALTHY one, so it fires the graceful-close epoch and drags durableEpochSeqTxn up to seqTxn.
            engine.releaseInactive();

            // Model a RESTART: discard the in-memory tracker so a fresh one starts at durableEpochSeqTxn=0
            // while _txn on disk is still at the applied seqTxn. That gap is what makes the close-epoch
            // block eligible on the very next open -- the ordinary state after any process restart, and the
            // state the failed-drop retry reopens into. Without it the block short-circuits on
            // seqTxn == durableEpochSeqTxn and the bug is unreachable.
            engine.getTableSequencerAPI().purgeTxnTracker(tt.getDirName());

            // Remove the symbol offset file: configureSymbolTable() will now fail mid-construction,
            // leaving the column/symbol lists partly built exactly as the failed-drop path does.
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
                // The REAL failure, surfaced intact.
                io.questdb.test.tools.TestUtils.assertContains(e.getFlyweightMessage(), "SymbolMap does not exist");
            } catch (AssertionError e) {
                // Exactly the regression: the best-effort close-epoch walked half-built state and its
                // AssertionError replaced the constructor's CairoException.
                throw new AssertionError(
                        "the close-time durable-epoch flush ran on a PARTIALLY-CONSTRUCTED writer and its "
                                + "error replaced the real open failure: " + e, e);
            }
        });
    }

    /**
     * Clears the engine-wide durability poison so it cannot leak into later tests. Mirrors the helper in
     * {@code AdaptiveGroupCommitTest} / {@code TableWriterTest}.
     */
    private void resetDurabilityPoisonForTest() throws Exception {
        final java.lang.reflect.Field field =
                io.questdb.cairo.CairoEngine.class.getDeclaredField("durabilityFailure");
        field.setAccessible(true);
        ((java.util.concurrent.atomic.AtomicReference<?>) field.get(engine)).set(null);
        engine.setDurabilityFailureHandler(failure -> {
        });
    }

    /**
     * Fails exactly the NEXT {@code fsync} with a genuine data-sync failure, then behaves normally. One-shot
     * so the failure lands inside the close epoch and nothing else in the test is disturbed.
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
