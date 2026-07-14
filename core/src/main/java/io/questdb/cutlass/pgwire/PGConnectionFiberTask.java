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
import io.questdb.mp.continuation.QueryTask;
import io.questdb.network.IODispatcher;
import io.questdb.network.IOOperation;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.PeerIsSlowToWriteException;

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
 *   <li>disconnect-class outcomes -&gt; step returns done, and the disconnect runs in
 *       {@link #onDone()} -- strictly after the gate is terminal, so the recycled
 *       context (and this task with it) cannot be handed to a new connection while
 *       the gate is still RUNNING.</li>
 * </ul>
 * The fd re-arm lives in {@link #onParked()}, after the gate returned to IDLE, so
 * the event the registration produces can never find the gate closed (no lost
 * wakeup). While a step runs (or is frozen in a wait), the fd is registered for
 * nothing, preserving the dispatcher's one-owner-at-a-time contract; a client
 * disconnect during a long wait is still detected by the circuit breaker's socket
 * probe, exactly as on the legacy path.
 *
 * <p>The task lives on the connection context and follows its recycling: a new
 * connection incarnation finds the gate terminal (after a disconnect) and reopens it
 * at launch.
 */
public final class PGConnectionFiberTask extends QueryTask {
    private static final Log LOG = LogFactory.getLog(PGConnectionFiberTask.class);
    private static final int NO_DISCONNECT = -1;
    private final PGConnectionContext context;
    private final IODispatcher<PGConnectionContext> dispatcher;
    private final Metrics metrics;
    private int disconnectReason = NO_DISCONNECT;
    private int nextOperation = IOOperation.READ;
    private int operation = IOOperation.READ;

    PGConnectionFiberTask(PGConnectionContext context, IODispatcher<PGConnectionContext> dispatcher, Metrics metrics) {
        this.context = context;
        this.dispatcher = dispatcher;
        this.metrics = metrics;
    }

    @Override
    protected void onAbandoned() {
        // shutdown raced the launch: the step never ran, so nothing else will
        // return this checked-out context; disconnect it here
        disconnectReason = DISCONNECT_REASON_SERVER_SHUTDOWN;
    }

    @Override
    protected void onDone() {
        if (disconnectReason != NO_DISCONNECT) {
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
        dispatcher.registerChannel(context, nextOperation);
    }

    /**
     * Stages the fd operation for the next step. The dispatch job calls this before
     * launching; the launch's gate CAS publishes the write to the mounting fiber.
     */
    void prepare(int operation) {
        this.operation = operation;
        this.disconnectReason = NO_DISCONNECT;
    }

    @Override
    protected boolean runStep() {
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
