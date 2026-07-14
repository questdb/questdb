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
import io.questdb.mp.continuation.QueryTask;
import io.questdb.network.HeartBeatException;
import io.questdb.network.IODispatcher;
import io.questdb.network.IOOperation;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.PeerIsSlowToWriteException;
import io.questdb.network.ServerDisconnectException;

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
 * step with {@code selectorFactory} acquire/release: a wait-freeze keeps the
 * selector referenced from the frozen stack until the thaw completes the step --
 * the same exclusivity the job-rotation machinery gives a parked generation, with
 * the same pool-converges-to-concurrency economics. Per-connection request state is
 * unaffected: it lives on the context (LocalValue), never on the selector, which is
 * what already lets different workers' selectors serve one connection today.
 *
 * <p>The fd re-arm runs in {@link #onParked()} after the gate returned to IDLE (no
 * lost wakeup); a disconnect runs in {@link #onDone()} after the gate is terminal,
 * so the recycled context cannot be relaunched while the gate is still RUNNING. An
 * escaped throwable is logged and the connection left unregistered, faithfully
 * mirroring how the direct path unwinds the job tick.
 *
 * <p>The busy-writer retry path is a third trigger, next to fd events and heartbeats:
 * a due retry launches this task with {@link #prepareRerun()} and the step calls the
 * existing {@link HttpConnectionContext#tryRerun} unchanged. The task itself is the
 * {@link RescheduleContext} the step passes down, and it only STAGES a reschedule;
 * the actual {@link WaitProcessor} enqueue runs in {@code onParked()}, after the gate
 * reopened. Enqueuing mid-step -- what the inline path does -- would start the
 * backoff clock while the gate is still RUNNING, and a due rerun popping before the
 * step parks would be refused by the gate and lost. A staged reschedule also
 * suppresses any same-step fd arm, keeping the retry the connection's single wakeup
 * trigger; the rerun re-derives the fd need when it resumes processing.
 */
public final class HttpConnectionFiberTask extends QueryTask implements RescheduleContext {
    private static final int ACTION_HEARTBEAT = 3;
    private static final int ACTION_NONE = 0;
    private static final int ACTION_READ = 1;
    private static final int ACTION_WRITE = 2;
    private static final Log LOG = LogFactory.getLog(HttpConnectionFiberTask.class);
    private final HttpConnectionContext context;
    private final IODispatcher<HttpConnectionContext> dispatcher;
    private final WaitProcessor rescheduleContext;
    private final HttpServer.HttpRequestProcessorSelectorFactory selectorFactory;
    private boolean isAbandoned;
    private boolean isDisconnectPending;
    private boolean isRerun;
    private boolean isRescheduleNextAttempt;
    private boolean isReschedulePending;
    private int nextAction = ACTION_NONE;
    private int operation = IOOperation.READ;

    HttpConnectionFiberTask(
            HttpConnectionContext context,
            IODispatcher<HttpConnectionContext> dispatcher,
            HttpServer.HttpRequestProcessorSelectorFactory selectorFactory,
            WaitProcessor rescheduleContext
    ) {
        this.context = context;
        this.dispatcher = dispatcher;
        this.selectorFactory = selectorFactory;
        this.rescheduleContext = rescheduleContext;
    }

    /**
     * The {@link RescheduleContext} the step hands to the connection: called when
     * request processing hits a busy writer ({@code scheduleRetry} or a new request
     * inside a rerun's receive loop). Only stages the retry; {@link #onParked()}
     * performs the {@link WaitProcessor} enqueue once the gate has reopened.
     */
    @Override
    public void reschedule(Retry retry) {
        assert retry == context : "foreign retry staged on connection task";
        isReschedulePending = true;
    }

    /**
     * Mirrors the inline path's queue-full handling ({@code retry.fail} in
     * {@link WaitProcessor}): sends the error response and arms the follow-up.
     * Runs from {@link #onParked()} with the gate IDLE; ownership is exclusive
     * because the failed enqueue armed no trigger and the fd is unregistered.
     */
    private void failRetry(RetryFailedOperationException e) {
        final HttpServer.HttpRequestProcessorSelectorImpl selector = selectorFactory.acquire();
        try {
            context.fail(selector, e);
        } catch (PeerIsSlowToReadException slowToRead) {
            dispatcher.registerChannel(context, IOOperation.WRITE);
        } catch (ServerDisconnectException disconnect) {
            dispatcher.disconnect(context, context.getDisconnectReason());
        } finally {
            selectorFactory.release(selector);
        }
    }

    @Override
    protected void onAbandoned() {
        // shutdown raced the launch: the step never ran, so nothing else will
        // return this checked-out context; disconnect it here
        isAbandoned = true;
    }

    @Override
    protected void onDone() {
        if (isAbandoned) {
            dispatcher.disconnect(context, IODispatcher.DISCONNECT_REASON_SERVER_SHUTDOWN);
        } else if (isDisconnectPending) {
            dispatcher.disconnect(context, context.getDisconnectReason());
        }
    }

    @Override
    protected void onError(Throwable th) {
        // matches the direct path: no disconnect, the connection stays unregistered
        LOG.critical().$("internal error [ex=").$(th).$(']').$();
    }

    @Override
    protected void onParked() {
        if (isReschedulePending) {
            // The staged retry owns the wakeup: no fd action is armed alongside it,
            // so the due rerun's launch is the only trigger and cannot be refused.
            isReschedulePending = false;
            try {
                if (isRescheduleNextAttempt) {
                    rescheduleContext.rescheduleNextAttempt(context);
                } else {
                    rescheduleContext.reschedule(context);
                }
            } catch (RetryFailedOperationException e) {
                failRetry(e);
            }
            return;
        }
        switch (nextAction) {
            case ACTION_READ -> dispatcher.registerChannel(context, IOOperation.READ);
            case ACTION_WRITE -> dispatcher.registerChannel(context, IOOperation.WRITE);
            case ACTION_HEARTBEAT -> dispatcher.registerChannel(context, IOOperation.HEARTBEAT);
            default -> {
            }
        }
    }

    /**
     * Stages the fd operation for the next step. The dispatch job calls this before
     * launching; the launch's gate CAS publishes the write to the mounting fiber.
     */
    void prepare(int operation) {
        this.operation = operation;
        this.isAbandoned = false;
        this.isDisconnectPending = false;
        this.isRerun = false;
        this.isRescheduleNextAttempt = false;
        this.isReschedulePending = false;
    }

    /**
     * Stages a due busy-writer retry for the next step. The dispatch job calls this
     * from its {@link WaitProcessor.RetryLauncher} before launching.
     */
    void prepareRerun() {
        this.isAbandoned = false;
        this.isDisconnectPending = false;
        this.isRerun = true;
        this.isRescheduleNextAttempt = false;
        this.isReschedulePending = false;
    }

    @Override
    protected boolean runStep() {
        final HttpServer.HttpRequestProcessorSelectorImpl selector = selectorFactory.acquire();
        try {
            if (isRerun) {
                if (!context.tryRerun(selector, this)) {
                    // still busy: stage the next backoff attempt, enqueued in onParked()
                    isReschedulePending = true;
                    isRescheduleNextAttempt = true;
                }
                nextAction = ACTION_NONE;
                return false;
            }
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
            isDisconnectPending = true;
            return true;
        } finally {
            selectorFactory.release(selector);
        }
    }
}
