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

package io.questdb.cutlass.http;

import io.questdb.cutlass.http.ex.RetryFailedOperationException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.network.HeartBeatException;
import io.questdb.network.IODispatcher;
import io.questdb.network.IOOperation;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.PeerIsSlowToWriteException;
import io.questdb.network.ServerDisconnectException;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * One HTTP connection's resumable work, reified for a pooled fiber. Each fd event
 * the dispatch job receives becomes one step: {@code runStep()} calls the existing
 * {@link HttpConnectionContext#handleClientOperation} unchanged and translates its
 * exception protocol into the task contract (the normal return needs no fd action,
 * exactly like the direct path). This covers every HTTP-hosted endpoint -- JSON
 * {@code /exec}, {@code /exp} export, ILP over HTTP, and the QWP WebSocket egress,
 * whose post-upgrade frames flow through the same {@code handleClientOperation}.
 *
 * <p>Selector confinement: the handler instances hang off a per-execution selector
 * (single-thread-confined scratch), and a fiber may mount on any worker, so a step
 * must not borrow the launching worker's selector. {@code runStep()} brackets each
 * step with {@code selectorFactory} acquire/release, so a suspended step retains
 * exclusive ownership until it resumes and releases the selector. Per-connection
 * request state lives on the context (LocalValue), never on the selector.
 *
 * <p>The busy-writer retry path is a third trigger, next to fd events and heartbeats:
 * a due retry launches this task with {@link #launchRerun(FiberRuntime, long)} and
 * the step calls the existing {@link HttpConnectionContext#tryRerun} unchanged.
 * The task itself is the {@link RescheduleContext} the step passes down.
 */
public final class HttpConnectionFiberTask extends FiberTask implements RescheduleContext {
    private static final int ACTION_HEARTBEAT = 3;
    private static final int ACTION_NONE = 0;
    private static final int ACTION_READ = 1;
    private static final int ACTION_WRITE = 2;
    private static final long EVENT_ACTION_MASK = 3;
    private static final int EVENT_READ = 1;
    private static final long EVENT_READY = 4;
    private static final int EVENT_RERUN = 2;
    private static final int EVENT_SHIFT = 3;
    private static final int EVENT_WRITE = 3;
    private static final Log LOG = LogFactory.getLog(HttpConnectionFiberTask.class);
    private static final long MAX_EVENT_INCARNATION = Long.MAX_VALUE >>> EVENT_SHIFT;
    private static final long STAGED_EVENT_OFFSET = Unsafe.getFieldOffset(HttpConnectionFiberTask.class, "stagedEvent");
    private final @Nullable Runnable beforeLaunchFailureSignalForTesting;
    private final HttpConnectionContext context;
    private final IODispatcher<HttpConnectionContext> dispatcher;
    private final WaitProcessor rescheduleContext;
    private final HttpServer.HttpRequestProcessorSelectorFactory selectorFactory;
    private int disconnectReason = IODispatcher.DISCONNECT_REASON_UNKNOWN_OPERATION;
    private boolean isDisconnectPending;
    private boolean isRearmed;
    private boolean isRescheduleNextAttempt;
    private boolean isReschedulePending;
    private int nextAction = ACTION_NONE;
    private long preparedRescheduleCursor = -1;
    @SuppressWarnings("FieldMayBeFinal")
    private volatile long stagedEvent;

    HttpConnectionFiberTask(
            HttpConnectionContext context,
            IODispatcher<HttpConnectionContext> dispatcher,
            HttpServer.HttpRequestProcessorSelectorFactory selectorFactory,
            WaitProcessor rescheduleContext
    ) {
        this(context, dispatcher, selectorFactory, rescheduleContext, null);
    }

    private HttpConnectionFiberTask(
            HttpConnectionContext context,
            IODispatcher<HttpConnectionContext> dispatcher,
            HttpServer.HttpRequestProcessorSelectorFactory selectorFactory,
            WaitProcessor rescheduleContext,
            @Nullable Runnable beforeLaunchFailureSignalForTesting
    ) {
        this.beforeLaunchFailureSignalForTesting = beforeLaunchFailureSignalForTesting;
        this.context = context;
        this.dispatcher = dispatcher;
        this.selectorFactory = selectorFactory;
        this.rescheduleContext = rescheduleContext;
    }

    @TestOnly
    public static HttpConnectionFiberTask createForTesting(
            HttpConnectionContext context,
            IODispatcher<HttpConnectionContext> dispatcher
    ) {
        return createForTesting(context, dispatcher, null);
    }

    @TestOnly
    public static HttpConnectionFiberTask createForTesting(
            HttpConnectionContext context,
            IODispatcher<HttpConnectionContext> dispatcher,
            @Nullable Runnable beforeLaunchFailureSignalForTesting
    ) {
        return createForTesting(context, dispatcher, null, beforeLaunchFailureSignalForTesting);
    }

    @TestOnly
    public static HttpConnectionFiberTask createForTesting(
            HttpConnectionContext context,
            IODispatcher<HttpConnectionContext> dispatcher,
            @Nullable WaitProcessor rescheduleContext,
            @Nullable Runnable beforeLaunchFailureSignalForTesting
    ) {
        final HttpConnectionFiberTask task = new HttpConnectionFiberTask(
                context,
                dispatcher,
                new HttpServer.HttpRequestProcessorSelectorFactory(1, 1),
                rescheduleContext,
                beforeLaunchFailureSignalForTesting
        );
        if (context != null) {
            context.setFiberTaskForTesting(task);
        }
        return task;
    }

    @TestOnly
    public void closeForTesting() {
        selectorFactory.close();
    }

    @TestOnly
    public LaunchResult launchForTesting(FiberRuntime runtime, int operation) {
        return launch(runtime, operation);
    }

    @TestOnly
    public LaunchResult launchRerunForTesting(FiberRuntime runtime) {
        return launchRerun(runtime, getIncarnation());
    }

    @TestOnly
    public LaunchResult launchReservedForTesting(
            FiberRuntime runtime,
            Fiber fiber,
            long reservationEpoch,
            int operation
    ) {
        return launchReserved(runtime, fiber, reservationEpoch, operation);
    }

    @Override
    public void reschedule(Retry retry) {
        assert retry == context : "foreign retry staged on connection task";
        isReschedulePending = true;
    }

    private static IllegalStateException incarnationOutOfRange(long taskIncarnation) {
        return new IllegalStateException("HTTP task incarnation is out of range [incarnation=" + taskIncarnation + ']');
    }

    private static IllegalArgumentException unsupportedOperation(int operation) {
        return new IllegalArgumentException("unsupported HTTP fiber operation [operation=" + operation + ']');
    }

    private void abortPreparedReschedule() {
        if (preparedRescheduleCursor > -1) {
            rescheduleContext.abortPreparedReschedule(preparedRescheduleCursor);
            preparedRescheduleCursor = -1;
        }
    }

    private void failRetry(RetryFailedOperationException e) {
        disconnectReason = IODispatcher.DISCONNECT_REASON_RETRY_FAILED;
        isDisconnectPending = true;
        nextAction = ACTION_NONE;
        final HttpServer.HttpRequestProcessorSelectorImpl selector = selectorFactory.acquire();
        try {
            context.fail(selector, e);
        } catch (PeerIsSlowToReadException slowToRead) {
            isDisconnectPending = false;
            nextAction = ACTION_WRITE;
        } catch (ServerDisconnectException disconnect) {
            disconnectReason = context.getDisconnectReason();
            isDisconnectPending = true;
        } finally {
            selectorFactory.release(selector);
        }
    }

    private LaunchResult launchEvent(
            FiberRuntime runtime,
            @Nullable Fiber fiber,
            long reservationEpoch,
            long taskIncarnation,
            int eventAction
    ) {
        if (taskIncarnation < 1 || taskIncarnation > MAX_EVENT_INCARNATION) {
            throw incarnationOutOfRange(taskIncarnation);
        }
        final long pendingEvent = (taskIncarnation << EVENT_SHIFT) | eventAction;
        while (true) {
            if (getIncarnation() != taskIncarnation) {
                return LaunchResult.STALE_INCARNATION;
            }
            final int state = getScheduleState();
            if (state == STATE_OWNED) {
                return LaunchResult.ALREADY_OWNED;
            }
            if (state != STATE_IDLE && state != STATE_ARMING && state != STATE_ARMING_SIGNALLED) {
                return LaunchResult.TERMINAL;
            }
            final long currentEvent = stagedEvent;
            if (currentEvent != 0) {
                if ((currentEvent >>> EVENT_SHIFT) != taskIncarnation) {
                    if (getIncarnation() != taskIncarnation) {
                        return LaunchResult.STALE_INCARNATION;
                    }
                    Unsafe.cas(this, STAGED_EVENT_OFFSET, currentEvent, 0L);
                    continue;
                }
                return LaunchResult.ALREADY_OWNED;
            }
            if (Unsafe.cas(this, STAGED_EVENT_OFFSET, 0L, pendingEvent)) {
                isRearmed = false;
                break;
            }
        }

        if (getIncarnation() != taskIncarnation) {
            return Unsafe.cas(this, STAGED_EVENT_OFFSET, pendingEvent, 0L)
                    ? LaunchResult.STALE_INCARNATION
                    : LaunchResult.ALREADY_OWNED;
        }
        final int state = getScheduleState();
        if (state == STATE_OWNED) {
            Unsafe.cas(this, STAGED_EVENT_OFFSET, pendingEvent, 0L);
            return LaunchResult.ALREADY_OWNED;
        }
        if (state != STATE_IDLE && state != STATE_ARMING && state != STATE_ARMING_SIGNALLED) {
            Unsafe.cas(this, STAGED_EVENT_OFFSET, pendingEvent, 0L);
            return LaunchResult.TERMINAL;
        }

        final long readyEvent = pendingEvent | EVENT_READY;
        if (!Unsafe.cas(this, STAGED_EVENT_OFFSET, pendingEvent, readyEvent)) {
            if (getIncarnation() != taskIncarnation) {
                return LaunchResult.STALE_INCARNATION;
            }
            return isDone() ? LaunchResult.TERMINAL : LaunchResult.ALREADY_OWNED;
        }
        final LaunchResult result;
        if (fiber != null) {
            result = runtime.launchReserved(fiber, reservationEpoch, this, taskIncarnation);
        } else {
            result = runtime.launch(this, taskIncarnation);
        }
        if (result != LaunchResult.LAUNCHED && result != LaunchResult.ALREADY_OWNED) {
            return resolveLaunchFailure(result, taskIncarnation, readyEvent);
        }
        return result;
    }

    private LaunchResult resolveLaunchFailure(LaunchResult result, long taskIncarnation, long readyEvent) {
        while (true) {
            if (getIncarnation() != taskIncarnation) {
                Unsafe.cas(this, STAGED_EVENT_OFFSET, readyEvent, 0L);
                return LaunchResult.STALE_INCARNATION;
            }
            final int state = getScheduleState();
            if (state == STATE_OWNED) {
                return LaunchResult.ALREADY_OWNED;
            }
            if (state == STATE_ARMING || state == STATE_ARMING_SIGNALLED) {
                final Runnable hook = beforeLaunchFailureSignalForTesting;
                if (hook != null) {
                    hook.run();
                }
                if ((result == LaunchResult.RESOURCE_FAILURE || result == LaunchResult.SATURATED)
                        && signalAxisA(taskIncarnation, SIGNAL_READY)) {
                    return LaunchResult.ALREADY_OWNED;
                }
                if (result == LaunchResult.QUIESCING
                        && signalAxisA(taskIncarnation, SIGNAL_DISCONNECT)) {
                    Unsafe.cas(this, STAGED_EVENT_OFFSET, readyEvent, 0L);
                    return LaunchResult.ALREADY_OWNED;
                }
                continue;
            }
            if (state != STATE_IDLE) {
                Unsafe.cas(this, STAGED_EVENT_OFFSET, readyEvent, 0L);
                return isDone() ? LaunchResult.TERMINAL : LaunchResult.ALREADY_OWNED;
            }
            if (!Unsafe.cas(this, STAGED_EVENT_OFFSET, readyEvent, 0L)) {
                return getIncarnation() == taskIncarnation
                        ? LaunchResult.ALREADY_OWNED
                        : LaunchResult.STALE_INCARNATION;
            }
            if ((result == LaunchResult.QUIESCING || result == LaunchResult.RESOURCE_FAILURE)
                    && !tryCancel(taskIncarnation)) {
                return isDone() ? LaunchResult.TERMINAL : LaunchResult.ALREADY_OWNED;
            }
            return result;
        }
    }

    private int takeEvent() {
        while (true) {
            final long event = stagedEvent;
            if (event == 0) {
                throw new IllegalStateException("HTTP fiber task has no staged event");
            }
            if ((event >>> EVENT_SHIFT) != getIncarnation()) {
                throw new IllegalStateException("HTTP fiber task has a stale staged event");
            }
            if ((event & EVENT_READY) == 0) {
                throw new IllegalStateException("HTTP fiber task has an unpublished staged event");
            }
            if (Unsafe.cas(this, STAGED_EVENT_OFFSET, event, 0L)) {
                return (int) (event & EVENT_ACTION_MASK);
            }
        }
    }

    LaunchResult launch(FiberRuntime runtime, int operation) {
        tryReopen();
        final int eventAction = switch (operation) {
            case IOOperation.READ -> EVENT_READ;
            case IOOperation.WRITE -> EVENT_WRITE;
            default -> throw unsupportedOperation(operation);
        };
        return launchEvent(runtime, null, 0, getIncarnation(), eventAction);
    }

    LaunchResult launchRerun(FiberRuntime runtime, long taskIncarnation) {
        return launchEvent(runtime, null, 0, taskIncarnation, EVENT_RERUN);
    }

    LaunchResult launchRerunReserved(
            FiberRuntime runtime,
            Fiber fiber,
            long reservationEpoch,
            long taskIncarnation
    ) {
        return launchEvent(runtime, fiber, reservationEpoch, taskIncarnation, EVENT_RERUN);
    }

    LaunchResult launchReserved(FiberRuntime runtime, Fiber fiber, long reservationEpoch, int operation) {
        try {
            tryReopen();
            final int eventAction = switch (operation) {
                case IOOperation.READ -> EVENT_READ;
                case IOOperation.WRITE -> EVENT_WRITE;
                default -> throw unsupportedOperation(operation);
            };
            return launchEvent(runtime, fiber, reservationEpoch, getIncarnation(), eventAction);
        } finally {
            runtime.releaseReservedFiber(fiber, reservationEpoch);
        }
    }

    @Override
    protected void onAbandoned() {
        abortPreparedReschedule();
        stagedEvent = 0;
        if (!isDisconnectPending) {
            disconnectReason = IODispatcher.DISCONNECT_REASON_SERVER_SHUTDOWN;
        }
        isDisconnectPending = true;
    }

    @Override
    protected void onDone() {
        stagedEvent = 0;
        if (isDisconnectPending) {
            isDisconnectPending = false;
            context.abandonRetry();
            if (!isRearmed) {
                dispatcher.disconnect(context, disconnectReason);
            }
        }
    }

    @Override
    protected void onError(Throwable th) {
        abortPreparedReschedule();
        LOG.critical().$("internal error [ex=").$(th).$(']').$();
        context.getMetrics().healthMetrics().incrementUnhandledErrors();
        disconnectReason = IODispatcher.DISCONNECT_REASON_SERVER_ERROR;
        isDisconnectPending = true;
    }

    @Override
    protected void onParkPrepare() {
        if (isReschedulePending && !context.hasPendingRetry()) {
            isRescheduleNextAttempt = false;
            isReschedulePending = false;
        }
        if (isReschedulePending) {
            try {
                preparedRescheduleCursor = isRescheduleNextAttempt
                        ? rescheduleContext.prepareRescheduleNextAttempt(context, getIncarnation())
                        : rescheduleContext.prepareReschedule(context, getIncarnation());
            } catch (RetryFailedOperationException e) {
                isRescheduleNextAttempt = false;
                isReschedulePending = false;
                failRetry(e);
            }
        }
    }

    @Override
    protected void onParked() {
        if (preparedRescheduleCursor > -1) {
            final long cursor = preparedRescheduleCursor;
            preparedRescheduleCursor = -1;
            isRescheduleNextAttempt = false;
            isReschedulePending = false;
            rescheduleContext.publishReschedule(cursor);
            return;
        }
        if (isDisconnectPending) {
            if (!signalAxisA(SIGNAL_DISCONNECT)) {
                throw new IllegalStateException("HTTP task is not arming");
            }
            return;
        }
        switch (nextAction) {
            case ACTION_READ -> {
                isRearmed = true;
                dispatcher.registerChannel(context, IOOperation.READ);
            }
            case ACTION_WRITE -> {
                isRearmed = true;
                dispatcher.registerChannel(context, IOOperation.WRITE);
            }
            case ACTION_HEARTBEAT -> {
                isRearmed = true;
                dispatcher.registerChannel(context, IOOperation.HEARTBEAT);
            }
            default -> {
            }
        }
    }

    @Override
    protected boolean runStep() {
        final int eventAction = takeEvent();
        assert preparedRescheduleCursor == -1;
        disconnectReason = IODispatcher.DISCONNECT_REASON_UNKNOWN_OPERATION;
        isDisconnectPending = false;
        isRearmed = false;
        isRescheduleNextAttempt = false;
        isReschedulePending = false;
        nextAction = ACTION_NONE;
        final HttpServer.HttpRequestProcessorSelectorImpl selector = selectorFactory.acquire();
        try {
            if (eventAction == EVENT_RERUN) {
                if (!context.tryRerun(selector, this)) {
                    isReschedulePending = true;
                    isRescheduleNextAttempt = true;
                } else {
                    nextAction = ACTION_READ;
                }
                return false;
            }
            final int operation = eventAction == EVENT_READ ? IOOperation.READ : IOOperation.WRITE;
            context.handleClientOperation(operation, selector, this);
            nextAction = ACTION_NONE;
            return false;
        } catch (HeartBeatException e) {
            nextAction = ACTION_HEARTBEAT;
            return false;
        } catch (PeerIsSlowToReadException e) {
            nextAction = ACTION_WRITE;
            return false;
        } catch (PeerIsSlowToWriteException e) {
            nextAction = ACTION_READ;
            return false;
        } catch (ServerDisconnectException e) {
            disconnectReason = context.getDisconnectReason();
            isDisconnectPending = true;
            return true;
        } finally {
            selectorFactory.release(selector);
        }
    }
}
