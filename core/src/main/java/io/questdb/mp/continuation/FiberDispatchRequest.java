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

import io.questdb.std.Unsafe;
import org.jetbrains.annotations.Nullable;

/**
 * Reusable, incarnation-checked dispatch request embedded in a controlled Fiber. A controller
 * that retains the request must retain {@link #getDispatchEpoch()} alongside it and supply that
 * epoch to {@link #grant(long, FiberDispatchTicket)}. This prevents a late grant from authorizing
 * a later use of the same Fiber.
 */
public final class FiberDispatchRequest {
    private static final int LIFECYCLE_STATE_BITS = 3;
    private static final long LIFECYCLE_STATE_MASK = (1L << LIFECYCLE_STATE_BITS) - 1;
    private static final long LIFECYCLE_STATE_OFFSET = Unsafe.getFieldOffset(
            FiberDispatchRequest.class,
            "lifecycleState"
    );
    private static final long MAX_DISPATCH_EPOCH = -1L >>> LIFECYCLE_STATE_BITS;
    private static final int STATE_CANCELLED = 6;
    private static final int STATE_CONSUMED = 5;
    private static final int STATE_GRANTED = 4;
    private static final int STATE_GRANTING = 3;
    private static final int STATE_IDLE = 0;
    private static final int STATE_PREPARING = 1;
    private static final int STATE_REQUESTED = 2;
    private final @Nullable FiberDispatchRequestState controllerState;
    private final Fiber fiber;
    private final FiberRuntime runtime;
    private @Nullable FiberDispatchContext dispatchContext;
    private @Nullable Throwable dispatchFailure;
    @SuppressWarnings("FieldMayBeFinal")
    private volatile long lifecycleState;
    private @Nullable FiberRuntime.OwnerContext ownerContext;
    private volatile @Nullable FiberDispatchRoute route;
    private @Nullable FiberTask task;
    private long taskIncarnation = -1;
    private @Nullable FiberDispatchTicket ticket;

    FiberDispatchRequest(
            Fiber fiber,
            FiberRuntime runtime,
            @Nullable FiberDispatchRequestState controllerState
    ) {
        this.controllerState = controllerState;
        this.fiber = fiber;
        this.runtime = runtime;
    }

    /**
     * Grants the captured request once. A successful return means the Fiber is mount-ready and
     * has been published to its runtime. A false return means the epoch is stale or another actor
     * already resolved the request.
     */
    public boolean grant(long dispatchEpoch, FiberDispatchTicket ticket) {
        if (!grantWithoutPublication(dispatchEpoch, ticket)) {
            return false;
        }
        runtime.publishGrantedDispatch(this);
        return true;
    }

    public long getDispatchEpoch() {
        return dispatchEpoch(lifecycleState);
    }

    public @Nullable FiberDispatchContext getDispatchContext() {
        return dispatchContext;
    }

    public @Nullable FiberDispatchRequestState getControllerState() {
        return controllerState;
    }

    public int getLastMountWorkerId() {
        return fiber.getLastMountWorkerId();
    }

    public int getOwnerWorkerId() {
        final FiberRuntime.OwnerContext ownerContext = this.ownerContext;
        return ownerContext != null ? ownerContext.getWorkerId() : FiberRuntime.NO_WORKER;
    }

    public @Nullable FiberDispatchRoute getRoute() {
        return route;
    }

    public FiberRuntime getRuntime() {
        return runtime;
    }

    public @Nullable FiberTask getTask() {
        return task;
    }

    public long getTaskIncarnation() {
        return taskIncarnation;
    }

    /**
     * Returns whether the supplied incarnation is still waiting for a grant. Controllers use this
     * to discard stale intrusive scheduler nodes without dereferencing task state.
     */
    public boolean isPending(long dispatchEpoch) {
        return lifecycleState == packLifecycleState(dispatchEpoch, STATE_REQUESTED);
    }

    boolean abort(long dispatchEpoch) {
        final long requested = packLifecycleState(dispatchEpoch, STATE_REQUESTED);
        if (!Unsafe.cas(this, LIFECYCLE_STATE_OFFSET, requested, packLifecycleState(dispatchEpoch, STATE_CANCELLED))) {
            return false;
        }
        clear();
        lifecycleState = packLifecycleState(dispatchEpoch, STATE_IDLE);
        return true;
    }

    long begin(FiberDispatchRoute route, @Nullable FiberRuntime.OwnerContext ownerContext) {
        final long current = lifecycleState;
        if (lifecycleState(current) != STATE_IDLE) {
            throw invalidLifecycleState("begin", current);
        }
        final long currentEpoch = dispatchEpoch(current);
        if (currentEpoch == MAX_DISPATCH_EPOCH) {
            throw new IllegalStateException("Fiber dispatch epoch exhausted");
        }
        final long dispatchEpoch = currentEpoch + 1;
        if (!Unsafe.cas(
                this,
                LIFECYCLE_STATE_OFFSET,
                current,
                packLifecycleState(dispatchEpoch, STATE_PREPARING)
        )) {
            throw invalidLifecycleState("begin", lifecycleState);
        }
        final FiberTask task = fiber.getAssignedTask();
        this.dispatchContext = fiber.getDispatchContextForDispatch();
        this.dispatchFailure = null;
        this.ownerContext = ownerContext;
        this.route = route;
        this.task = task;
        this.taskIncarnation = task != null ? task.getIncarnation() : -1;
        this.ticket = null;
        lifecycleState = packLifecycleState(dispatchEpoch, STATE_REQUESTED);
        return dispatchEpoch;
    }

    void complete(long dispatchEpoch, FiberDispatchTicket ticket) {
        final long consumed = packLifecycleState(dispatchEpoch, STATE_CONSUMED);
        if (lifecycleState != consumed || this.ticket != ticket) {
            throw invalidLifecycleState("complete", lifecycleState);
        }
        clear();
        lifecycleState = packLifecycleState(dispatchEpoch, STATE_IDLE);
    }

    FiberDispatchTicket consume() {
        final long current = lifecycleState;
        if (lifecycleState(current) != STATE_GRANTED
                || !Unsafe.cas(
                this,
                LIFECYCLE_STATE_OFFSET,
                current,
                withLifecycleState(current, STATE_CONSUMED)
        )) {
            throw invalidLifecycleState("consume", lifecycleState);
        }
        final FiberDispatchTicket ticket = this.ticket;
        if (ticket == null) {
            throw new IllegalStateException("granted Fiber dispatch request has no ticket");
        }
        return ticket;
    }

    @Nullable
    Throwable getDispatchFailure() {
        return dispatchFailure;
    }

    Fiber getFiber() {
        return fiber;
    }

    @Nullable
    FiberRuntime.OwnerContext getOwnerContext() {
        return ownerContext;
    }

    boolean grantDirect(long dispatchEpoch, FiberDispatchTicket ticket) {
        return grantWithoutPublication(dispatchEpoch, ticket);
    }

    boolean grantFailure(long dispatchEpoch, Throwable failure, FiberDispatchTicket failureTicket) {
        if (failure == null) {
            throw new IllegalArgumentException("Fiber dispatch failure must not be null");
        }
        if (failureTicket == null) {
            throw new IllegalArgumentException("Fiber dispatch failure ticket must not be null");
        }
        final long requested = packLifecycleState(dispatchEpoch, STATE_REQUESTED);
        if (!Unsafe.cas(this, LIFECYCLE_STATE_OFFSET, requested, packLifecycleState(dispatchEpoch, STATE_GRANTING))) {
            return false;
        }
        dispatchFailure = failure;
        ticket = failureTicket;
        lifecycleState = packLifecycleState(dispatchEpoch, STATE_GRANTED);
        return true;
    }

    void validateForMount() {
        final FiberTask task = this.task;
        if (fiber.getAssignedTask() != task
                || (task != null && task.getIncarnation() != taskIncarnation)) {
            throw new IllegalStateException(
                    "Fiber dispatch request task changed before mount [expectedIncarnation="
                            + taskIncarnation
                            + ", actualIncarnation="
                            + (task != null ? task.getIncarnation() : -1)
                            + ']'
            );
        }
    }

    void markDirectPending(long dispatchEpoch) {
        if (lifecycleState != packLifecycleState(dispatchEpoch, STATE_REQUESTED)
                || route != FiberDispatchRoute.DIRECT) {
            throw invalidLifecycleState("mark direct pending", lifecycleState);
        }
        route = FiberDispatchRoute.DIRECT_PENDING;
    }

    private static long dispatchEpoch(long lifecycleState) {
        return lifecycleState >>> LIFECYCLE_STATE_BITS;
    }

    private static int lifecycleState(long lifecycleState) {
        return (int) (lifecycleState & LIFECYCLE_STATE_MASK);
    }

    private static long packLifecycleState(long dispatchEpoch, int state) {
        return (dispatchEpoch << LIFECYCLE_STATE_BITS) | state;
    }

    private static long withLifecycleState(long lifecycleState, int state) {
        return (lifecycleState & ~LIFECYCLE_STATE_MASK) | state;
    }

    private void clear() {
        dispatchContext = null;
        dispatchFailure = null;
        ownerContext = null;
        route = null;
        task = null;
        taskIncarnation = -1;
        ticket = null;
    }

    private boolean grantWithoutPublication(long dispatchEpoch, FiberDispatchTicket ticket) {
        if (ticket == null) {
            throw new IllegalArgumentException("Fiber dispatch ticket must not be null");
        }
        final long requested = packLifecycleState(dispatchEpoch, STATE_REQUESTED);
        if (!Unsafe.cas(this, LIFECYCLE_STATE_OFFSET, requested, packLifecycleState(dispatchEpoch, STATE_GRANTING))) {
            return false;
        }
        this.ticket = ticket;
        lifecycleState = packLifecycleState(dispatchEpoch, STATE_GRANTED);
        return true;
    }

    private IllegalStateException invalidLifecycleState(CharSequence operation, long lifecycleState) {
        return new IllegalStateException(
                "invalid Fiber dispatch request state [operation=" + operation
                        + ", epoch=" + dispatchEpoch(lifecycleState)
                        + ", state=" + lifecycleState(lifecycleState)
                        + ']'
        );
    }
}
