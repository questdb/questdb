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

package io.questdb.griffin.engine.ops;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.SCSequence;
import io.questdb.std.WeakSelfReturningObjectPool;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.locks.Lock;

public abstract class OperationDispatcher<T extends AbstractOperation> {

    public static final String FORCE_OPERATION_APPLY_REASON = "Force Alter Operation";
    // Test seam: when non-null, execute() runs this hook in the EntryUnavailableException async-enqueue
    // catch branch (the writer-pool-exhausted fallback), just before it acquires the role-switch read
    // lock for the authoritative re-check and enqueue. A test installs a hook that pauses there so a
    // concurrent PRIMARY-to-REPLICA demote can flip the role flag (the demote takes the write side of
    // the same lock) before the paused operation acquires the read side and re-checks -- coordinating
    // the async fallback against a demote deterministically without host load. Null in production (the
    // default): the fire-site is a single static volatile read with no side effect.
    @TestOnly
    private static volatile Runnable asyncEnqueueObserver;
    // Test seam: when non-null, execute() runs this hook just before it externalizes the operation
    // via apply(), i.e. inside the role-switch read-lock hold once the fence below is in place. A test
    // can install a hook that pauses there so a concurrent PRIMARY-to-REPLICA demote either blocks
    // behind the read hold or expires its tryLock budget, exercising the demote race deterministically
    // without host load. Null in production (the default): the fire-site is one volatile read with a
    // null check, negligible (the volatile read is an acquire load, not eliminable, but trivially cheap).
    @TestOnly
    private static volatile Runnable preApplyObserver;
    private final DoneOperationFuture doneFuture = new DoneOperationFuture();
    private final CairoEngine engine;
    private final WeakSelfReturningObjectPool<OperationFutureImpl> futurePool;
    private final String lockReason;

    public OperationDispatcher(CairoEngine engine, String lockReason) {
        this.engine = engine;
        futurePool = new WeakSelfReturningObjectPool<>(pool -> new OperationFutureImpl(engine, pool), 2);
        this.lockReason = lockReason;
    }

    /**
     * Test seam: installs a hook execute() fires at the top of the EntryUnavailableException
     * async-enqueue catch branch. Pass null to uninstall. The hook is shared across dispatcher
     * instances, so an installer must scope its own pause to the statement under test. Never set
     * outside tests -- the field defaults to null and the fire-site is a no-op then.
     */
    @TestOnly
    public static void setAsyncEnqueueObserver(Runnable observer) {
        asyncEnqueueObserver = observer;
    }

    /**
     * Test seam: installs a hook execute() fires just before apply(). Pass null to uninstall. The
     * hook is shared across dispatcher instances (the per-CompiledQuery dispatchers are anonymous and
     * not individually reachable), so an installer must scope its own pause to the statement under
     * test. Never set outside tests -- the field defaults to null and the fire-site is a no-op then.
     */
    @TestOnly
    public static void setPreApplyObserver(Runnable observer) {
        preApplyObserver = observer;
    }

    public OperationFuture execute(
            T operation,
            SqlExecutionContext sqlExecutionContext,
            @Nullable SCSequence eventSubSeq,
            boolean closeOnDone
    ) {
        // storing execution context for UPDATE, DROP INDEX execution
        // writer thread will call `apply()` when thread is ready to do so
        // `apply()` will use context stored in the operation
        operation.withContext(sqlExecutionContext);
        boolean isDone = false;
        final TableToken tableToken = operation.getTableToken();
        // When a table is hard-suspended with write-denial on, it rejects WAL writes. Route eligible
        // non-structural changes through the force/WAL-bypass path (direct TableWriter) so maintenance
        // still works, mirroring FORCE DROP PARTITION. Structural changes stay denied (versioned only).
        final boolean forceWalBypass = operation.isForceWalBypass()
                || (operation.isForceableWhenSuspended()
                && engine.getConfiguration().isWalApplySuspendedWriteDenied()
                && engine.isWalApplySuspended(tableToken));
        try (TableWriterAPI writer = !forceWalBypass ? engine.getTableWriterAPI(tableToken, lockReason) : engine.getWriter(tableToken, FORCE_OPERATION_APPLY_REASON)) {
            // Both-trees pre-externalization fire-point: fire the role-switch mint observer here, before
            // applyFenced acquires the role-switch read lock. The post-fence preApplyObserver below
            // fires only inside the fence, so a reverted/absent dispatcher fence would fire nothing and
            // a demote-race witness would degrade to a timing-only sleep window. Firing the shared mint
            // observer before the fence lets a witness arm one seam that trips whether or not the fence
            // wraps the externalization. A strict no-op in production (the observer field is null) and on
            // the existing fenced path (the post-fence preApplyObserver still fires).
            engine.fireRoleSwitchMintObserver();
            final long result = applyFenced(operation, writer, forceWalBypass);
            isDone = true;
            return doneFuture.of(result);
        } catch (EntryUnavailableException busyException) {
            // For non-WAL tables, when another thread holds the writer, this code enqueues the operation
            // on the TableWriter's async queue. The writer processes enqueued operations when it becomes free.
            // The operation does not require re-authorization when executed from the async queue, because the
            // caller already authorized it before enqueuing.
            // The caller blocks on the returned future until the writer executes the operation or the timeout
            // expires.
            if (eventSubSeq == null) {
                throw busyException;
            }
            fireAsyncEnqueueObserver();
            // Demote write-fence for the async-enqueue fallback. The success path is fenced in
            // applyFenced, but a WAL writer-pool exhaustion routes here: the enqueue applies the
            // operation through the legacy non-WAL writer pool (getWriterOrPublishCommand), which mints
            // no replicated sequencer txn for a WAL table, so on a demoting node the change is
            // acknowledged but never replicated -- a silent acked-loss. Re-check read-only before
            // enqueuing so a demote that flipped the flag refuses cleanly instead. (Separately, a WAL
            // UPDATE should never reach this non-WAL fallback at all; that pre-existing,
            // role-switch-independent robustness gap is left for a dedicated fix.)
            //
            // The eager check below is a fast-refuse optimization; the authoritative re-check runs
            // under the role-switch READ lock, mirroring applyFenced. Holding the read lock around the
            // re-check and the enqueue serializes the externalization against the role flip, which
            // takes the WRITE side around its REPLICA flag publish: either the flip ran first (the
            // in-lock re-check sees read-only and refuses without enqueuing) or the enqueue runs fully
            // as PRIMARY and the flip's write acquire waits for this read hold to release. The
            // authorized force/WAL-bypass break-glass path is exempt for the same reason as in
            // applyFenced (see isWriteRefused).
            if (isWriteRefused(forceWalBypass)) {
                throw CairoException.readOnlyAccess();
            }
            // Both-trees pre-externalization fire-point for the async-enqueue fallback, mirroring the
            // inline-apply path above: fire the shared role-switch mint observer before acquiring the
            // role-switch read lock so a reverted/absent fence here still trips a witness. A strict
            // no-op in production (the observer field is null).
            engine.fireRoleSwitchMintObserver();
            final Lock lock = engine.getRoleSwitchReadLock();
            lock.lock();
            try {
                if (isWriteRefused(forceWalBypass)) {
                    throw CairoException.readOnlyAccess();
                }
                OperationFutureImpl future = futurePool.pop();
                future.of(
                        operation,
                        sqlExecutionContext,
                        eventSubSeq,
                        operation.getTableNamePosition(),
                        closeOnDone
                );
                return future;
            } finally {
                lock.unlock();
            }
        } finally {
            if (closeOnDone && isDone) {
                operation.close();
            }
        }
    }

    private static void fireAsyncEnqueueObserver() {
        final Runnable observer = asyncEnqueueObserver;
        if (observer != null) {
            observer.run();
        }
    }

    private static void firePreApplyObserver() {
        final Runnable observer = preApplyObserver;
        if (observer != null) {
            observer.run();
        }
    }

    /**
     * Demote write-fence for the inline-apply success path, mirroring InsertOperationImpl.commit and
     * JsonQueryProcessor.executeDdlFenced. Both pg-wire (msgExecuteUpdate / msgExecuteDDL) and HTTP
     * /exec (executeUpdate / executeAlterTable) funnel WAL UPDATE and ALTER through here. The writer is
     * acquired while the node is still PRIMARY (the eager getTableWriterAPI gate passed), but only
     * apply() externalizes the operation: it mints a sequencer txn and hands the change to the WAL
     * uploader. A PRIMARY-to-REPLICA demote landing between that eager gate and apply() drains only the
     * non-WAL writer pool, so it does not wait for this in-flight WAL operation; the operation would
     * otherwise mint a txn on an already-demoting node and acknowledge client success for a change the
     * uploader never sends, which the downloader's same-lineage reconcile then silently supersedes.
     * <p>
     * Holding the role-switch READ lock across apply() serializes the externalization against the role
     * flip, which takes the WRITE side around its REPLICA flag publish: either the flip ran first (the
     * in-lock re-check sees read-only and refuses without externalizing) or this operation runs fully as
     * PRIMARY and the flip's write acquire waits for this read hold to release. The READ side lets
     * concurrent commits on other tables/protocols run in parallel; only the role flip is excluded.
     * <p>
     * The refusal here is a plain engine.isReadOnlyMode() re-check, NOT the ReadOnlyStatementGate
     * predicate the DDL fence uses. That is deliberate and safe: this dispatcher only ever applies WAL
     * UPDATE and ALTER, and the one refused-type exemption the gate carries -- the HTTP parquet
     * exporter's temp-table DROP a read-only replica keeps -- never reaches here (DROP routes through
     * executeDdl -> executeDdlFenced, not the UPDATE/ALTER dispatchers). A blanket gate-predicate call
     * is unnecessary and a future reader must not "fix" this into a blanket refusal that would refuse an
     * exempted DROP it never sees.
     * <p>
     * The fence is a strict no-op for pure-OSS deployments: the read lock is uncontended and
     * isReadOnlyMode() returns the static read-only-instance flag, so the only cost is one uncontended
     * lock/unlock and one volatile read. The EntryUnavailableException async-queue branch in execute()
     * is the writer-busy enqueue fallback. It is NOT only a non-WAL path: a WAL writer-pool exhaustion
     * routes a WAL UPDATE here too, and the legacy enqueue applies it without minting a replicated
     * sequencer txn, so on a demoting node it would be a silent acked-loss. That branch therefore
     * carries its own read-only re-check under the same role-switch READ lock (see execute()),
     * symmetric with this fence: the re-check and the enqueue are atomic against the flip's flag
     * publish, refusing cleanly on a demote. The inline-apply success path is the one fenced here.
     */
    private long applyFenced(T operation, TableWriterAPI writer, boolean forceWalBypass) {
        // An authorized force/WAL-bypass break-glass alter (a system-admin FORCE alter on a
        // hard-suspended table) is exempt from the replica read-only fence via isWriteRefused: it
        // applies directly through the exclusive writer acquired above and mints no replicated
        // sequencer txn, so it carries none of the demote acked-loss risk this fence guards against,
        // and a steady-state replica is always read-only. An instance-level read-only lockdown
        // (cairo.read.only) still refuses it. Structural changes are denied earlier, so only
        // non-structural force-alters reach here.
        if (isWriteRefused(forceWalBypass)) {
            throw CairoException.readOnlyAccess();
        }
        final Lock lock = engine.getRoleSwitchReadLock();
        lock.lock();
        try {
            // Authoritative in-lock re-check against the role flip, which holds the WRITE side of this
            // lock around the REPLICA flag publish. apply() runs inside the read hold so the flip cannot
            // interleave (its write acquire waits), while other commits share the read side.
            if (isWriteRefused(forceWalBypass)) {
                throw CairoException.readOnlyAccess();
            }
            firePreApplyObserver();
            return apply(operation, writer);
        } finally {
            lock.unlock();
        }
    }

    private boolean isWriteRefused(boolean forceWalBypass) {
        // The fence refuses while the node refuses writes, EXCEPT an authorized force/WAL-bypass
        // break-glass alter the engine still permits (isForceAlterAllowed): that alter applies directly
        // via the exclusive writer and mints no replicated sequencer txn, so it carries none of the
        // demote/replica acked-loss risk this fence guards against. isForceAlterAllowed() is true on a
        // read-only replica (the authorized maintenance path for a frozen replica table) but false under
        // an instance-level read-only lockdown (cairo.read.only), which refuses every write.
        return engine.isReadOnlyMode() && !(forceWalBypass && engine.isForceAlterAllowed());
    }

    protected abstract long apply(T operation, TableWriterAPI writerFronted);
}
