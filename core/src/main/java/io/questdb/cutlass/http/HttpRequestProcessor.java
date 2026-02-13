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

import io.questdb.cairo.SecurityContext;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.PeerIsSlowToWriteException;
import io.questdb.network.ServerDisconnectException;

import static io.questdb.cutlass.http.HttpRequestValidator.METHOD_GET;

/**
 * Processes HTTP requests for a specific URL endpoint.
 *
 * <h2>Threading model</h2>
 * <ol>
 *   <li>Processors are not expected to be thread-safe.</li>
 *   <li>Each worker thread has its own private processor instance.</li>
 *   <li>Only one worker processes a given connection at any point in time,
 *       but a single request can be served by multiple workers - and thus processor instances - over time:
 *       each park/resume cycle may hand the connection to a different
 *       worker.</li>
 * </ol>
 *
 *
 * <h2>Per-request state</h2>
 * <p>
 * A single processor instance may service multiple interleaved requests
 * from different connections: the worker picks up whichever connection is
 * ready next, so processor methods are called for different contexts in
 * arbitrary order. Processors must NOT store per-request state in
 * instance fields.
 * Use {@link LocalValue} with a {@code static final} field to attach
 * per-request state to the {@link HttpConnectionContext}.
 *
 * <h2>Park / resume lifecycle</h2>
 * <p>
 * When the send buffer is full and the peer is slow to read, the processor
 * throws {@link io.questdb.network.PeerIsSlowToReadException}. Before
 * throwing, the context calls {@link #parkRequest} and records the
 * processor's handler ID. The connection is re-queued for WRITE. A
 * different worker may pick up the WRITE event, resolve the handler ID
 * back to its own processor instance via
 * {@link HttpRequestProcessorSelector#resolveProcessorById}, and call
 * {@link #resumeSend}. This is safe because per-connection state lives in
 * the context (via {@code LocalValue}), not in the processor instance.
 *
 * <h2>Processor instance fields</h2>
 * <p>
 * Instance fields may be mutated during request processing — typical uses
 * are scratchpad buffers, sinks, and other temporary working memory
 * needed while handling a single request. However, mutations do not
 * survive park/resume boundaries. After a park, a different worker's
 * processor instance resumes the request, so any state written to instance
 * fields by the parking worker is invisible to the resuming worker.
 * Durable per-connection state must go through {@link LocalValue}.
 *
 */
public interface HttpRequestProcessor {
    // after this callback is invoked, the server will disconnect the client.
    // if a processor desires to write a goodbye letter to the client,
    // it must also send TCP FIN by invoking socket.shutdownWrite()
    default void failRequest(
            HttpConnectionContext context,
            HttpException exception
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
    }

    default String getName() {
        return ActiveConnectionTracker.PROCESSOR_OTHER;
    }

    default byte getRequiredAuthType() {
        return SecurityContext.AUTH_TYPE_CREDENTIALS;
    }

    default short getSupportedRequestTypes() {
        return METHOD_GET;
    }

    default boolean ignoreConnectionLimitCheck() {
        return false;
    }

    default void onConnectionClosed(HttpConnectionContext context) {
    }

    default void onHeadersReady(HttpConnectionContext context) {
    }

    default void onRequestComplete(
            HttpConnectionContext context
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
    }

    default void onRequestRetry(
            HttpConnectionContext context
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
    }

    default void parkRequest(HttpConnectionContext context, boolean pausedQuery) {
    }

    default boolean processServiceAccountCookie(
            HttpConnectionContext context,
            SecurityContext securityContext
    ) throws PeerIsSlowToReadException, PeerDisconnectedException {
        return true;
    }

    default boolean requiresAuthentication() {
        return getRequiredAuthType() != SecurityContext.AUTH_TYPE_NONE;
    }

    default boolean reservedOneAdminConnection() {
        return false;
    }

    default void resumeRecv(HttpConnectionContext context) throws PeerIsSlowToWriteException, ServerDisconnectException, PeerIsSlowToReadException {
    }

    default void resumeSend(
            HttpConnectionContext context
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
    }
}
