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

package io.questdb.test.cutlass.qwp;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.processors.LineHttpProcessorConfiguration;
import io.questdb.cutlass.line.tcp.DefaultColumnTypes;
import io.questdb.cutlass.line.tcp.WalTableUpdateDetails;
import io.questdb.cutlass.qwp.protocol.QwpColumnDef;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpTableBlockCursor;
import io.questdb.cutlass.qwp.server.QwpIngressProcessorState;
import io.questdb.cutlass.qwp.server.QwpTudCache;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/**
 * Fuzz test for INVARIANT B containment of the in-place PRIMARY-to-REPLICA
 * demote race on the QWP ingress path (see {@code rejectCairoError} in
 * {@link QwpIngressProcessorState}).
 * <p>
 * The read-only flag can flip at ANY point within a batch: before the
 * {@code engine.isReadOnlyMode()} gate at the top of {@code processMessage()},
 * between the gate and the WAL writer acquisition (the enterprise
 * {@code EntCairoEngine.getWalWriter} refusal), or between the gate and the
 * commit (the {@code TableUpdateDetails.commit} in-lock re-check refusal). The
 * deeper engine gates refuse with {@code CairoException.authorization()}
 * ("replica access is read-only") thrown RAW — deliberately outside
 * {@code CommitFailedException} wrapping — which the naive
 * {@code cairoExceptionStatus} mapping would turn into
 * {@code Status.SECURITY_ERROR}: a status a store-and-forward client latches
 * as a terminal HALT and surfaces to its producer. A transient demote must
 * instead map to the reconnect-eligible {@code Status.NOT_ACCEPTING_WRITES}
 * close ({@code isRoleChangeClosePending()}), while a genuine ACL denial on a
 * writable node must STILL map to {@code SECURITY_ERROR}.
 * <p>
 * Two fuzz phases:
 * <ol>
 *   <li>Deterministic: the flip is injected at a randomly chosen point in the
 *       batch lifecycle (before the gate / inside TUD acquisition / inside the
 *       commit / inside the deferred commit), interleaved with genuine ACL
 *       denials and clean batches, so every containment arm and the
 *       genuine-denial arm are exercised under one random schedule.</li>
 *   <li>Concurrent: a flipper thread demotes the engine at a random time while
 *       the driver pumps batches, reproducing the true race timing. Within a
 *       round the flip is one-directional (writable to read-only, matching a
 *       real demote), so any authorization refusal raised by the read-only
 *       gates happens-after the flip and the containment re-check must observe
 *       it: SECURITY_ERROR is impossible unless containment is broken.</li>
 * </ol>
 * The batch driver mirrors {@code QwpIngressUpgradeProcessor.handleBinaryMessage}
 * exactly, including reading {@code isRoleChangeClosePending()} AFTER the
 * commit calls — the ordering the containment fix depends on.
 */
public class QwpIngressDemoteRaceFuzzTest extends AbstractCairoTest {

    private static final String ACL_DENIAL_MESSAGE = "write permission denied [table=fuzz]";
    private static final int CONCURRENT_MAX_BATCHES = 10_000;
    private static final int CONCURRENT_ROUNDS = 30;
    private static final int DETERMINISTIC_ITERATIONS = 300;

    @Test
    public void testConcurrentDemoteFlipNeverEmitsSecurityError() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final AtomicBoolean readOnly = new AtomicBoolean(false);
            final LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);

            try (CairoEngine demotableEngine = newDemotableEngine(readOnly)) {
                for (int round = 0; round < CONCURRENT_ROUNDS; round++) {
                    readOnly.set(false);
                    final QwpIngressProcessorState state =
                            new QwpIngressProcessorState(1024, 4096, demotableEngine, lineConfig);
                    try {
                        state.of(1, AllowAllSecurityContext.INSTANCE);
                        final RaceTudCache tudCache = installRaceTudCache(state, demotableEngine, lineConfig);

                        // Mirror the TableUpdateDetails.commit in-lock re-check contract:
                        // once the demote flips the flag, the commit refuses with the MARKED
                        // read-only refusal (outside CommitFailedException wrapping).
                        final Runnable commitRefusal = () -> {
                            if (readOnly.get()) {
                                throw CairoException.readOnlyAccess();
                            }
                        };
                        tudCache.commitHook = commitRefusal;
                        tudCache.maxRowsCommitHook = commitRefusal;

                        final CyclicBarrier start = new CyclicBarrier(2);
                        final long flipDelayNanos = rnd.nextLong(1_000_000L); // 0..1ms into the round
                        final Thread flipper = new Thread(() -> {
                            try {
                                start.await();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                            LockSupport.parkNanos(flipDelayNanos);
                            readOnly.set(true);
                        }, "demote-flipper");
                        flipper.start();
                        start.await();

                        boolean refused = false;
                        for (int batch = 0; batch < CONCURRENT_MAX_BATCHES; batch++) {
                            if (driveBatchAndAssertContained(state, rnd, readOnly)) {
                                refused = true;
                                break;
                            }
                        }
                        flipper.join();

                        if (!refused) {
                            // The flip landed after the last batch. The flag is now
                            // read-only for certain, so one more batch MUST refuse via
                            // the top gate — still with the contained status.
                            Assert.assertTrue(
                                    "post-flip batch must be refused",
                                    driveBatchAndAssertContained(state, rnd, readOnly)
                            );
                        }
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    } finally {
                        state.onDisconnected();
                        state.close();
                    }
                }
            }
        });
    }

    @Test
    public void testDemoteFlipAtRandomInjectionPointsFuzz() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final AtomicBoolean readOnly = new AtomicBoolean(false);
            final LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);

            try (CairoEngine demotableEngine = newDemotableEngine(readOnly)) {
                for (int i = 0; i < DETERMINISTIC_ITERATIONS; i++) {
                    readOnly.set(false);
                    final int scenario = rnd.nextInt(7);
                    final QwpIngressProcessorState state =
                            new QwpIngressProcessorState(1024, 4096, demotableEngine, lineConfig);
                    try {
                        state.of(1, AllowAllSecurityContext.INSTANCE);
                        final RaceTudCache tudCache = installRaceTudCache(state, demotableEngine, lineConfig);
                        runScenario(scenario, state, tudCache, readOnly, rnd);

                        // Meta-invariant across ALL scenarios: SECURITY_ERROR may only
                        // ever be emitted while the node is writable. On a read-only
                        // node it means a transient demote leaked to the client as a
                        // terminal HALT — the exact Invariant B violation.
                        if (state.getStatus() == QwpIngressProcessorState.Status.SECURITY_ERROR) {
                            Assert.assertFalse(
                                    "SECURITY_ERROR emitted while the node is read-only [scenario=" + scenario + ']',
                                    readOnly.get()
                            );
                        }
                    } finally {
                        state.onDisconnected();
                        state.close();
                    }
                }
            }
        });
    }

    /**
     * Deterministic reproduction of the demote-REVERT race: the deep-gate refusal
     * is thrown while the node is read-only (demote Step 1 landed mid-batch), but
     * the demote FAILS before the exception reaches the state's catch block. Real
     * path: {@code EntCairoEngine.drainWriterPool(restorePrimaryOnTimeout=true)}
     * restores PRIMARY when the drain budget expires on busy non-WAL writers --
     * and nothing fences the throw-to-catch propagation (the commit path releases
     * the role-switch READ lock in its finally as the exception unwinds), so the
     * write-locked {@code setCurrentRole(PRIMARY)} can land in between.
     * A catch-time re-read of the LIVE {@code engine.isReadOnlyMode()} (the
     * pre-fix classification) then sees writable and falls through to
     * {@code cairoExceptionStatus} -> {@code SECURITY_ERROR}: the client latches
     * a permanent terminal HALT for a milliseconds-long condition, on a node
     * that ended up writable PRIMARY. The fix classifies by the
     * {@code isReadOnlyAccessRefusal()} marker stamped at the throw site, which
     * no later state flip can rewrite.
     * <p>
     * The hook compresses that interleaving deterministically: flip to read-only,
     * shape the refusal exactly as the deep gates produce it, revert to writable,
     * THEN throw -- the exception reaches the catch after the revert, exactly as
     * when the drain-timeout restore wins the race. Production
     * {@code isReadOnlyMode()} is a volatile flag read (enterprise widens it with
     * the cached volatile {@code isReadOnlyReplica}), so the AtomicBoolean is
     * memory-semantics-equivalent; the ordering is a legal production schedule,
     * not a test artifact.
     * <p>
     * NOTE: the meta-invariant in {@link #testDemoteFlipAtRandomInjectionPointsFuzz}
     * cannot catch this case -- it re-reads the same live flag the bug re-reads
     * (after the revert {@code readOnly.get()} IS false, so a leaked
     * SECURITY_ERROR passes it). The oracle here is cause-based instead: the
     * refusal was CAUSED by the transient demote, so the client must get the
     * reconnect-eligible shape regardless of what the flag reads at catch time.
     */
    @Test
    public void testDemoteRevertBetweenThrowAndCatchStaysContained() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicBoolean readOnly = new AtomicBoolean(false);
            final LineHttpProcessorConfiguration lineConfig =
                    new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration);

            try (CairoEngine demotableEngine = newDemotableEngine(readOnly)) {
                // The revert can race any of the three post-gate refusal windows.
                for (int window = 0; window < 3; window++) {
                    readOnly.set(false);
                    final QwpIngressProcessorState state =
                            new QwpIngressProcessorState(1024, 4096, demotableEngine, lineConfig);
                    try {
                        state.of(1, AllowAllSecurityContext.INSTANCE);
                        final RaceTudCache tudCache = installRaceTudCache(state, demotableEngine, lineConfig);
                        final Runnable demoteRefuseThenRevert = () -> {
                            readOnly.set(true);   // demote Step 1: dynamic flag flips mid-batch
                            // refusal shaped (and MARKED) while REPLICA, exactly as every
                            // production read-only gate now throws it
                            final CairoException e = CairoException.readOnlyAccess();
                            readOnly.set(false);  // drain budget expires -> demote fails -> PRIMARY restored
                            throw e;              // propagates to the catch AFTER the revert
                        };
                        final byte[] message;
                        final String where;
                        switch (window) {
                            case 0:
                                tudCache.getTudHook = demoteRefuseThenRevert;
                                message = oneTableMessage((byte) 0);
                                where = "revert vs writer acquisition";
                                break;
                            case 1:
                                tudCache.commitHook = demoteRefuseThenRevert;
                                message = zeroTableMessage((byte) 0);
                                where = "revert vs commit";
                                break;
                            default:
                                tudCache.maxRowsCommitHook = demoteRefuseThenRevert;
                                message = zeroTableMessage(QwpConstants.FLAG_DEFER_COMMIT);
                                where = "revert vs deferred commit";
                                break;
                        }
                        final boolean roleChangeClose = driveBatch(state, message);
                        assertContainedRefusal(state, roleChangeClose, where);
                    } finally {
                        state.onDisconnected();
                        state.close();
                    }
                }
            }
        });
    }

    private static void assertContainedRefusal(QwpIngressProcessorState state, boolean roleChangeClose, String where) {
        Assert.assertFalse(where + ": batch must be refused", state.isOk());
        Assert.assertNotEquals(
                where + ": transient demote must never map to SECURITY_ERROR (client latches terminal HALT)",
                QwpIngressProcessorState.Status.SECURITY_ERROR,
                state.getStatus()
        );
        Assert.assertEquals(
                where + ": demote refusal must map to the reconnect-eligible status",
                QwpIngressProcessorState.Status.NOT_ACCEPTING_WRITES,
                state.getStatus()
        );
        Assert.assertTrue(
                where + ": connection must be flagged for the role-change close",
                roleChangeClose
        );
        TestUtils.assertContains(state.getErrorText(), CairoException.READ_ONLY_ACCESS_MESSAGE);
    }

    private static void addNativeData(QwpIngressProcessorState state, byte[] data) {
        long ptr = Unsafe.malloc(data.length, MemoryTag.NATIVE_HTTP_CONN);
        try {
            for (int i = 0; i < data.length; i++) {
                Unsafe.putByte(ptr + i, data[i]);
            }
            state.addData(ptr, ptr + data.length);
        } finally {
            Unsafe.free(ptr, data.length, MemoryTag.NATIVE_HTTP_CONN);
        }
    }

    /**
     * Drives one message through the state, mirroring the exact call ordering of
     * {@code QwpIngressUpgradeProcessor.handleBinaryMessage}: addData →
     * isDeferCommit → processMessage → commit/commitIfMaxUncommittedRowsReached →
     * read isRoleChangeClosePending AFTER the commit calls.
     *
     * @return the roleChangeClose flag as the upgrade processor would observe it
     */
    private static boolean driveBatch(QwpIngressProcessorState state, byte[] message) {
        addNativeData(state, message);
        boolean deferCommit = state.isDeferCommit();
        state.processMessage();
        if (state.isOk() && !deferCommit) {
            state.commit();
        }
        if (state.isOk() && deferCommit) {
            state.commitIfMaxUncommittedRowsReached();
            if (state.isOk()) {
                // Mirrors the processor's deferred-ack containment: rows are
                // buffered but uncommitted, so the cumulative-ack watermark
                // must not advance past this frame until the group commits.
                state.markUncommittedDeferredRows();
            }
        }
        // Read AFTER the commit calls — the ordering the containment fix
        // (86f8556c8e) depends on: reading before commit() misses the
        // commit-path authorization refusal.
        return state.isRoleChangeClosePending();
    }

    /**
     * Drives one randomly-shaped batch and asserts the containment invariant if
     * it was refused.
     *
     * @return true if the batch was refused (the connection would close)
     */
    private static boolean driveBatchAndAssertContained(
            QwpIngressProcessorState state, Rnd rnd, AtomicBoolean readOnly
    ) {
        final boolean defer = rnd.nextBoolean();
        final byte[] message = zeroTableMessage(defer ? QwpConstants.FLAG_DEFER_COMMIT : 0);
        final boolean roleChangeClose = driveBatch(state, message);
        if (state.isOk()) {
            if (defer) {
                state.clearMessageState();
            } else {
                state.clear();
            }
            return false;
        }
        // Within a round the flip is one-directional, so the refusal can only be
        // the demote: it must be contained no matter which window it landed in
        // (top gate or commit-path re-check).
        Assert.assertTrue("refusal implies the demote flip landed", readOnly.get());
        assertContainedRefusal(state, roleChangeClose, "concurrent");
        return true;
    }

    private static RaceTudCache installRaceTudCache(
            QwpIngressProcessorState state, CairoEngine engine, LineHttpProcessorConfiguration lineConfig
    ) {
        try {
            Field f = QwpIngressProcessorState.class.getDeclaredField("tudCache");
            f.setAccessible(true);
            Misc.free((QwpTudCache) f.get(state));
            RaceTudCache cache = new RaceTudCache(engine, lineConfig);
            f.set(state, cache);
            return cache;
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static CairoEngine newDemotableEngine(AtomicBoolean readOnly) {
        // Enterprise widens isReadOnlyMode() at the ENGINE level (the dynamic replica
        // leg: super.isReadOnlyMode() || isReadOnlyReplica()) while the static
        // configuration.isReadOnlyInstance() flag stays false -- and the refusal-shape
        // decision keys on exactly that difference: only ROLE-DERIVED read-only takes
        // the reconnect-eligible role-change close, while static readonly=true keeps
        // the terminal SECURITY_ERROR NACK (see isStaticReadOnlyInstance() in
        // QwpIngressProcessorState). Model the demote the same way enterprise
        // implements it: override the engine method, keep the config flag false.
        // Backing it with an AtomicBoolean reproduces the in-place demote: the flag
        // flips dynamically while connections are mid-batch. The engine is
        // constructed writable.
        return new CairoEngine(new DefaultTestCairoConfiguration(root)) {
            @Override
            public boolean isReadOnlyMode() {
                return readOnly.get();
            }
        };
    }

    private static void runScenario(
            int scenario,
            QwpIngressProcessorState state,
            RaceTudCache tudCache,
            AtomicBoolean readOnly,
            Rnd rnd
    ) {
        switch (scenario) {
            case 0: {
                // Flip lands BEFORE the batch: the top-of-processMessage gate refuses.
                readOnly.set(true);
                boolean roleChangeClose = driveBatch(state, randomMessage(rnd));
                assertContainedRefusal(state, roleChangeClose, "top gate");
                break;
            }
            case 1: {
                // Flip lands between the gate and the WAL writer acquisition: the
                // engine-level acquire refuses with the marked read-only refusal
                // (EntCairoEngine.getWalWriter contract). Handled by the
                // processMessage CairoException arm → rejectCairoError.
                tudCache.getTudHook = () -> {
                    readOnly.set(true);
                    throw CairoException.readOnlyAccess();
                };
                boolean roleChangeClose = driveBatch(state, oneTableMessage((byte) 0));
                assertContainedRefusal(state, roleChangeClose, "writer acquisition");
                break;
            }
            case 2: {
                // Flip lands between the gate and the commit: the in-lock re-check in
                // TableUpdateDetails.commit refuses with the marked read-only refusal
                // (deliberately outside CommitFailedException wrapping; QwpTudCache
                // propagates it raw). Handled by rejectCommitError → rejectCairoError.
                tudCache.commitHook = () -> {
                    readOnly.set(true);
                    throw CairoException.readOnlyAccess();
                };
                boolean roleChangeClose = driveBatch(state, zeroTableMessage((byte) 0));
                assertContainedRefusal(state, roleChangeClose, "commit");
                break;
            }
            case 3: {
                // Same as 2, on the deferred-commit path (FLAG_DEFER_COMMIT →
                // commitIfMaxUncommittedRowsReached).
                tudCache.maxRowsCommitHook = () -> {
                    readOnly.set(true);
                    throw CairoException.readOnlyAccess();
                };
                boolean roleChangeClose = driveBatch(state, zeroTableMessage(QwpConstants.FLAG_DEFER_COMMIT));
                assertContainedRefusal(state, roleChangeClose, "deferred commit");
                break;
            }
            case 4: {
                // Clean batch on a writable node: no refusal, no role-change close.
                boolean roleChangeClose = driveBatch(state, zeroTableMessage(
                        rnd.nextBoolean() ? QwpConstants.FLAG_DEFER_COMMIT : 0));
                Assert.assertTrue("clean batch must succeed", state.isOk());
                Assert.assertFalse("clean batch must not flag role-change close", roleChangeClose);
                break;
            }
            case 5: {
                // GENUINE ACL denial from the commit on a WRITABLE node: containment
                // must NOT swallow it — the client must still see SECURITY_ERROR.
                tudCache.commitHook = () -> {
                    throw CairoException.authorization().put(ACL_DENIAL_MESSAGE);
                };
                boolean roleChangeClose = driveBatch(state, zeroTableMessage((byte) 0));
                Assert.assertFalse(state.isOk());
                Assert.assertEquals(
                        "genuine ACL denial on a writable node must stay SECURITY_ERROR",
                        QwpIngressProcessorState.Status.SECURITY_ERROR,
                        state.getStatus()
                );
                Assert.assertFalse(
                        "genuine ACL denial must not trigger the role-change close",
                        roleChangeClose
                );
                TestUtils.assertContains(state.getErrorText(), ACL_DENIAL_MESSAGE);
                break;
            }
            default: {
                // GENUINE ACL denial from the writer acquisition on a WRITABLE node.
                tudCache.getTudHook = () -> {
                    throw CairoException.authorization().put(ACL_DENIAL_MESSAGE);
                };
                boolean roleChangeClose = driveBatch(state, oneTableMessage((byte) 0));
                Assert.assertFalse(state.isOk());
                Assert.assertEquals(
                        "genuine ACL denial on a writable node must stay SECURITY_ERROR",
                        QwpIngressProcessorState.Status.SECURITY_ERROR,
                        state.getStatus()
                );
                Assert.assertFalse(
                        "genuine ACL denial must not trigger the role-change close",
                        roleChangeClose
                );
                TestUtils.assertContains(state.getErrorText(), ACL_DENIAL_MESSAGE);
                break;
            }
        }
    }

    /**
     * Message with one table block ("fuzz", 0 rows, 0 columns): decodes far enough
     * to reach {@code QwpTudCache.getTableUpdateDetails}.
     */
    private static byte[] oneTableMessage(byte flags) {
        byte[] payload = {
                4, 'f', 'u', 'z', 'z',
                0,    // rowCount=0
                0     // columnCount=0
        };
        return wrapQwpPayload(payload, (short) 1, flags);
    }

    private static byte[] randomMessage(Rnd rnd) {
        return rnd.nextBoolean()
                ? zeroTableMessage(rnd.nextBoolean() ? QwpConstants.FLAG_DEFER_COMMIT : 0)
                : oneTableMessage(rnd.nextBoolean() ? QwpConstants.FLAG_DEFER_COMMIT : 0);
    }

    private static byte[] wrapQwpPayload(byte[] payload, short tableCount, byte flags) {
        byte[] message = new byte[QwpConstants.HEADER_SIZE + payload.length];
        message[0] = 'Q';
        message[1] = 'W';
        message[2] = 'P';
        message[3] = '1';
        message[4] = QwpConstants.VERSION;
        message[5] = flags;
        message[6] = (byte) tableCount;
        message[7] = (byte) (tableCount >>> 8);
        message[8] = (byte) payload.length;
        message[9] = (byte) (payload.length >>> 8);
        message[10] = (byte) (payload.length >>> 16);
        message[11] = (byte) (payload.length >>> 24);
        System.arraycopy(payload, 0, message, QwpConstants.HEADER_SIZE, payload.length);
        return message;
    }

    /**
     * Well-formed message with zero table blocks: passes the read-only gate and the
     * decoder, then goes straight to the commit calls — the exact shape needed to
     * land the flip in the gate-to-commit window.
     */
    private static byte[] zeroTableMessage(byte flags) {
        return wrapQwpPayload(new byte[0], (short) 0, flags);
    }

    /**
     * QwpTudCache with injection hooks at the two engine-refusal sites the demote
     * race can hit after the top gate has passed: the WAL writer acquisition
     * ({@code getTableUpdateDetails}) and the commits. Hooks run INSIDE the cache,
     * i.e. between the {@code processMessage} gate and the state's catch blocks —
     * precisely the window the containment re-check must cover.
     */
    private static final class RaceTudCache extends QwpTudCache {
        volatile Runnable commitHook;
        volatile Runnable getTudHook;
        volatile Runnable maxRowsCommitHook;

        RaceTudCache(CairoEngine engine, LineHttpProcessorConfiguration lineConfig) {
            super(engine, true, true, new DefaultColumnTypes(lineConfig), PartitionBy.DAY);
        }

        @Override
        public void commitAll(CommittedTxnConsumer consumer) throws Throwable {
            Runnable hook = commitHook;
            if (hook != null) {
                hook.run();
            }
        }

        @Override
        public void commitIfMaxUncommittedRowsReached(CommittedTxnConsumer consumer) throws Throwable {
            Runnable hook = maxRowsCommitHook;
            if (hook != null) {
                hook.run();
            }
        }

        @Override
        public WalTableUpdateDetails getTableUpdateDetails(
                SecurityContext securityContext,
                Utf8Sequence tableName,
                ObjList<QwpColumnDef> schema,
                QwpTableBlockCursor cursor,
                int maxTables
        ) {
            Runnable hook = getTudHook;
            if (hook != null) {
                hook.run();
            }
            // Only reached by scenarios whose hook throws; table-bearing payloads
            // are never driven down the success path in this fuzz.
            throw new UnsupportedOperationException("fuzz getTudHook must throw");
        }
    }
}
