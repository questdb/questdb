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

package io.questdb.mp.continuation;

import io.questdb.mp.Job;
import io.questdb.std.CarrierLocal;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import jdk.internal.vm.Continuation;
import jdk.internal.vm.ContinuationScope;

/**
 * Thin wrapper over {@link jdk.internal.vm.Continuation} that hides the JDK-internal API
 * from the rest of the codebase. A WorkerContinuation represents a reified SQL call stack
 * that can be unmounted from one thread (via {@link #suspend()}) and remounted on a
 * different thread (via a subsequent {@link #run()} call from that thread).
 *
 * <p>Requires {@code --add-exports java.base/jdk.internal.vm=io.questdb} on the JVM
 * command line.
 */
public final class WorkerContinuation {
    public static final ContinuationScope SCOPE = new ContinuationScope("questdb-worker");
    // Thread-local pointer to the WorkerContinuation currently mounted on this thread.
    // Set on entry to {@link #run()} and cleared on exit. Suspending functions
    // discover the active cont via {@link #current()} without needing to plumb
    // the reference through the SQL execution context.
    private static final CarrierLocal<WorkerContinuation> CURRENT = new CarrierLocal<>();
    private static final long PARK_REFUSED_OFFSET = Unsafe.getFieldOffset(WorkerContinuation.class, "parkRefused");
    private final Continuation cont;
    private final ContinuationSink resumeSink;
    // Per-cont handoff slot: the body writes a dequeued cont here just before
    // yielding; the outer driver reads it after run() returns and remounts the
    // dequeued cont. Lives on the cont (not on the Worker) because the cont can
    // migrate to a different carrier between mount and unmount, while the
    // body's binding to a specific Worker instance does not. Single-writer
    // (the carrier currently mounting the cont) and read by whichever thread
    // called run(); the yield/run boundary supplies the memory fence, so no
    // volatile is needed.
    private WorkerContinuation handoff;
    // Snapshot of the worker's job generation that the body inside this cont is
    // iterating. Attached by the outer driver before run() and read after the
    // cont becomes done so the framework can call recycleInstance() on each
    // stateful Job. Single-writer-during-mount / single-reader-after-unmount,
    // same fence reasoning as {@link #handoff}.
    private ObjList<Job> jobs;
    // Set (1) when a body called suspend() but the JDK refused the yield AND a
    // scheduleResume has already pushed this cont onto its pool's resume queue
    // (i.e. a phantom entry exists). The cont is still mounted on the carrier
    // that tried to suspend; remounting from a peer worker would IllegalStateException
    // and busy-spin until that carrier unmounts. The dequeuing peer worker
    // consumes this flag (CAS 1 -> 0) to drop the phantom dequeue so it doesn't
    // burn CPU.
    // Encoded as int (not boolean) because Unsafe.cas has no boolean overload.
    @SuppressWarnings({"unused", "FieldCanBeLocal"})
    private volatile int parkRefused;
    private volatile boolean shutdown;

    public WorkerContinuation(Runnable body, ContinuationSink resumeSink) {
        this.cont = new Continuation(SCOPE, body);
        this.resumeSink = resumeSink;
    }

    /**
     * Returns the WorkerContinuation currently mounted on the calling thread, or
     * {@code null} if no continuation is mounted in {@link #SCOPE}. Suspending
     * functions (e.g., {@code wait_wal_table}) call this to obtain the cont they
     * pass to a {@code TxnWaiter} or other wait-mechanism object.
     */
    public static WorkerContinuation current() {
        return CURRENT.get();
    }

    /**
     * True if there is a mounted WorkerContinuation on the calling thread's stack.
     * A call to {@link #suspend()} is only legal when this returns true.
     */
    public static boolean isMounted() {
        return Continuation.getCurrentContinuation(SCOPE) != null;
    }

    /**
     * Unmounts the current call stack off the carrier thread, returning control to
     * whoever called {@link #run()}. Must only be called from a frame that was reached
     * through {@code run()} of a WorkerContinuation.
     *
     * <p>Returns {@code true} if the carrier was unmounted (the body will only continue
     * on a future {@link #run()} call). Returns {@code false} if the JDK refused to
     * yield because the carrier is pinned, e.g., a {@code synchronized} block or a
     * native frame sits above this call on the stack. When {@code false} is returned,
     * the body keeps running on the same carrier; callers must not assume they have
     * been parked.
     */
    public static boolean suspend() {
        return Continuation.yield(SCOPE);
    }

    /**
     * Attach the worker's job-generation snapshot that the body inside this
     * cont is iterating. The outer driver calls this before {@link #run()}.
     * On cont completion the framework reads the snapshot via
     * {@link #takeJobs()} and calls {@link Job#recycleInstance()} on each
     * entry so stateful instances return to their class pools.
     */
    public void attachJobs(ObjList<Job> jobs) {
        this.jobs = jobs;
    }

    /**
     * If the park-refused flag is set, clears it and returns {@code true}. Called by
     * the dequeuing peer worker on each remount attempt (and inside its
     * IllegalStateException spin loop) so that a phantom queue entry left behind by
     * a refused {@link #suspend()} is dropped instead of busy-spinning the peer.
     *
     * <p>Idempotent: a single set is consumed by exactly one observer. The CAS
     * enforces this contract -- if two threads race to consume, only the winner
     * sees {@code true} and clears the flag; the loser sees {@code false}.
     */
    public boolean consumeParkRefused() {
        return Unsafe.cas(this, PARK_REFUSED_OFFSET, 1, 0);
    }

    public boolean isDone() {
        return cont.isDone();
    }

    /**
     * Set when the owning context is closing. Suspending functions that loop on
     * suspend/wake (e.g. {@code wait_wal_table}) must check this and exit instead of
     * re-parking, so that {@link #run()} can drive the body all the way to completion.
     */
    public boolean isShutdown() {
        return shutdown;
    }

    /**
     * Marks this cont as having a phantom entry on its resume queue: a
     * {@link #suspend()} that returned {@code false} after a {@code scheduleResume}
     * had already enqueued it. The next remount attempt by a dequeuing peer worker
     * consumes the flag and drops the dequeue. Callers must only set this when they
     * know an enqueue has actually happened (e.g. tryCancel on the gating waiter
     * lost to a tryFire).
     */
    public void markParkRefused() {
        parkRefused = 1;
    }

    /**
     * Starts the continuation body or resumes it on the calling thread. Returns when
     * the body either completes (then {@link #isDone()} is true) or calls
     * {@link #suspend()} (then {@link #isDone()} is false and the frames are parked
     * inside this object, ready to be remounted by a future {@code run()} call).
     */
    public void run() {
        WorkerContinuation prev = CURRENT.get();
        CURRENT.set(this);
        try {
            cont.run();
        } finally {
            CURRENT.set(prev);
        }
    }

    /**
     * Pushes this continuation onto its origin pool's resume sink so a worker on
     * that pool will pick it up and call {@link #run()}. Safe to call from any
     * thread; safe against concurrent puts of the same continuation reference if
     * the caller has gated via the appropriate CAS (typically
     * {@code TxnWaiter.tryFire} / {@code tryCancel}).
     */
    public void scheduleResume() {
        resumeSink.put(this);
    }

    /**
     * Stash a dequeued continuation that should be remounted by the outer driver of
     * whichever carrier is currently running this cont. The body writes here just
     * before {@link #suspend()}; the outer driver reads via {@link #takeHandoff()}
     * after {@link #run()} returns. Must only be called from a frame currently
     * mounted in this cont.
     */
    public void setHandoff(WorkerContinuation handoff) {
        this.handoff = handoff;
    }

    /**
     * Marks this continuation as shutting down. Suspending functions that consult
     * {@link #isShutdown()} between suspends must terminate their wake loop. Combined
     * with a bounded {@link #run()} drive in the owning context's close path, this
     * guarantees the body unwinds and the parked native stack is released.
     */
    public void shutdown() {
        this.shutdown = true;
    }

    /**
     * Read and clear the handoff slot. Called by whichever thread drove {@link #run()}
     * once it returns: the yield/run boundary publishes the body's last write.
     */
    public WorkerContinuation takeHandoff() {
        WorkerContinuation h = handoff;
        handoff = null;
        return h;
    }

    /**
     * Read and clear the attached job-generation snapshot. Called by the
     * framework after {@link #run()} returns with {@link #isDone()} true so
     * stateful Jobs in the snapshot can be recycled to their class pools.
     */
    public ObjList<Job> takeJobs() {
        ObjList<Job> j = jobs;
        jobs = null;
        return j;
    }
}
