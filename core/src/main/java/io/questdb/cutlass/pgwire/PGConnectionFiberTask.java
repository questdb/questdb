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

package io.questdb.cutlass.pgwire;

import io.questdb.Metrics;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.mp.continuation.SuspensionScope;
import io.questdb.mp.continuation.TimerShards;
import io.questdb.network.IODispatcher;
import io.questdb.network.IOOperation;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.PeerIsSlowToWriteException;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.network.IODispatcher.*;

/**
 * One PG connection's resumable work, reified for a pooled fiber. Each fd event the
 * dispatch job receives for the connection becomes one step: {@code runStep()} calls
 * the existing {@link PGConnectionContext#handleClientOperation(int)} unchanged and
 * translates its outcome into the task contract. A wait function deep inside query
 * execution (wait_wal_table / sleep) freezes the fiber mid-step and resumes it
 * transparently; the socket-level exceptions keep their current meaning:
 * <ul>
 *   <li>normal return / {@code PeerIsSlowToWriteException} -&gt; park, re-arm READ;</li>
 *   <li>{@code PeerIsSlowToReadException} -&gt; park, re-arm WRITE;</li>
 *   <li>disconnect-class outcomes -&gt; step returns done.</li>
 * </ul>
 * The fd re-arm lives in {@link #onParked()} while the task is ARMING. While a
 * step runs (or is frozen in a wait), the fd is registered for nothing,
 * preserving the dispatcher's one-owner-at-a-time contract.
 *
 * <p>The task lives on the connection context and follows its recycling: a new
 * connection incarnation finds the gate terminal (after a disconnect) and reopens it
 * at launch.
 */
public final class PGConnectionFiberTask extends FiberTask {
    private static final long EVENT_ACTION_MASK = 3;
    private static final int EVENT_READ = 1;
    private static final long EVENT_READY = 4;
    private static final int EVENT_SHIFT = 3;
    // 2 is EVENT_RERUN in the HTTP twin of this encoding
    private static final int EVENT_WRITE = 3;
    private static final Log LOG = LogFactory.getLog(PGConnectionFiberTask.class);
    private static final long MAX_EVENT_INCARNATION = Long.MAX_VALUE >>> EVENT_SHIFT;
    private static final int NO_DISCONNECT = -1;
    private static final long STAGED_EVENT_OFFSET = Unsafe.getFieldOffset(PGConnectionFiberTask.class, "stagedEvent");
    private final PGConnectionContext context;
    private final IODispatcher<PGConnectionContext> dispatcher;
    private final Metrics metrics;
    private final TimerShards timerShards;
    private int disconnectReason = NO_DISCONNECT;
    private boolean isRearmed;
    private int nextOperation = IOOperation.READ;
    @SuppressWarnings("FieldMayBeFinal")
    private volatile long stagedEvent;

    PGConnectionFiberTask(
            PGConnectionContext context,
            IODispatcher<PGConnectionContext> dispatcher,
            Metrics metrics,
            TimerShards timerShards
    ) {
        this.context = context;
        this.dispatcher = dispatcher;
        this.metrics = metrics;
        this.timerShards = timerShards;
    }

    public LaunchResult launch(FiberRuntime runtime, int operation) {
        tryReopen();
        final int eventAction = switch (operation) {
            case IOOperation.READ -> EVENT_READ;
            case IOOperation.WRITE -> EVENT_WRITE;
            default -> throw unsupportedOperation(operation);
        };
        return launchEvent(runtime, null, 0, getIncarnation(), eventAction);
    }

    public LaunchResult launchReserved(FiberRuntime runtime, Fiber fiber, long reservationEpoch, int operation) {
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

    @TestOnly
    public void setReadyEventForTesting(long incarnation) {
        if (incarnation < 1 || incarnation > MAX_EVENT_INCARNATION) {
            throw incarnationOutOfRangeArgument(incarnation);
        }
        stagedEvent = (incarnation << EVENT_SHIFT) | EVENT_READ | EVENT_READY;
    }

    private LaunchResult launchEvent(
            FiberRuntime runtime,
            @Nullable Fiber fiber,
            long reservationEpoch,
            long taskIncarnation,
            int eventAction
    ) {
        if (taskIncarnation < 1 || taskIncarnation > MAX_EVENT_INCARNATION) {
            throw incarnationOutOfRangeState(taskIncarnation);
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

    private static IllegalArgumentException incarnationOutOfRangeArgument(long incarnation) {
        return new IllegalArgumentException("PG task incarnation is out of range [incarnation=" + incarnation + ']');
    }

    private static IllegalStateException incarnationOutOfRangeState(long taskIncarnation) {
        return new IllegalStateException("PG task incarnation is out of range [incarnation=" + taskIncarnation + ']');
    }

    private static IllegalArgumentException unsupportedOperation(int operation) {
        return new IllegalArgumentException("unsupported PG fiber operation [operation=" + operation + ']');
    }

    private int takeEvent() {
        while (true) {
            final long event = stagedEvent;
            if (event == 0) {
                throw new IllegalStateException("PG fiber task has no staged event");
            }
            if ((event >>> EVENT_SHIFT) != getIncarnation()) {
                throw new IllegalStateException("PG fiber task has a stale staged event");
            }
            if ((event & EVENT_READY) == 0) {
                throw new IllegalStateException("PG fiber task has an unpublished staged event");
            }
            if (Unsafe.cas(this, STAGED_EVENT_OFFSET, event, 0L)) {
                return (int) (event & EVENT_ACTION_MASK);
            }
        }
    }

    @Override
    protected void onAbandoned() {
        stagedEvent = 0;
        disconnectReason = DISCONNECT_REASON_SERVER_SHUTDOWN;
    }

    @Override
    protected void onDone() {
        stagedEvent = 0;
        if (disconnectReason != NO_DISCONNECT && !isRearmed) {
            dispatcher.disconnect(context, disconnectReason);
        }
    }

    @Override
    protected void onError(Throwable th) {
        LOG.critical().$("internal error [ex=").$(th).$(']').$();
        metrics.healthMetrics().incrementUnhandledErrors();
        disconnectReason = DISCONNECT_REASON_SERVER_ERROR;
    }

    @Override
    protected void onParked() {
        isRearmed = true;
        dispatcher.registerChannel(context, nextOperation);
    }

    @Override
    protected boolean runStep() {
        SuspensionScope.enterTimerShards(timerShards);
        SuspensionScope.enterCancellationSource(context.getCircuitBreaker());
        final int eventAction = takeEvent();
        final int operation = eventAction == EVENT_READ ? IOOperation.READ : IOOperation.WRITE;
        disconnectReason = NO_DISCONNECT;
        isRearmed = false;
        try {
            context.handleClientOperation(operation);
            nextOperation = IOOperation.READ;
            return false;
        } catch (PeerIsSlowToWriteException e) {
            nextOperation = IOOperation.READ;
            return false;
        } catch (PeerIsSlowToReadException e) {
            nextOperation = IOOperation.WRITE;
            return false;
        } catch (PeerDisconnectedException e) {
            disconnectReason = operation == IOOperation.READ
                    ? DISCONNECT_REASON_PEER_DISCONNECT_AT_RECV
                    : DISCONNECT_REASON_PEER_DISCONNECT_AT_SEND;
            return true;
        } catch (PGMessageProcessingException e) {
            LOG.error().$("protocol issue [err: `").$safe(e.getFlyweightMessage()).$("`]").$();
            disconnectReason = DISCONNECT_REASON_PROTOCOL_VIOLATION;
            return true;
        } catch (Exception e) {
            // mirrors the direct dispatch path's terminal catch
            LOG.critical().$("internal error [ex=").$(e).$(']').$();
            metrics.healthMetrics().incrementUnhandledErrors();
            disconnectReason = DISCONNECT_REASON_SERVER_ERROR;
            return true;
        }
    }
}
