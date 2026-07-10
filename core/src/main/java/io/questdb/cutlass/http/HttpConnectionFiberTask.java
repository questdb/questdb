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

package io.questdb.cutlass.http;

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
 */
public final class HttpConnectionFiberTask extends QueryTask {
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
        switch (nextAction) {
            case ACTION_READ -> dispatcher.registerChannel(context, IOOperation.READ);
            case ACTION_WRITE -> dispatcher.registerChannel(context, IOOperation.WRITE);
            case ACTION_HEARTBEAT -> dispatcher.registerChannel(context, IOOperation.HEARTBEAT);
            default -> {
            }
        }
    }

    @Override
    protected boolean runStep() {
        final HttpServer.HttpRequestProcessorSelectorImpl selector = selectorFactory.acquire();
        try {
            context.handleClientOperation(operation, selector, rescheduleContext);
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

    /**
     * Stages the fd operation for the next step. The dispatch job calls this before
     * launching; the launch's gate CAS publishes the write to the mounting fiber.
     */
    void prepare(int operation) {
        this.operation = operation;
        this.isAbandoned = false;
        this.isDisconnectPending = false;
    }
}
