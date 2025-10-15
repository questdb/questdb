/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.Metrics;
import io.questdb.cairo.SecurityContext;
import io.questdb.metrics.AtomicLongGauge;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.QueryPausedException;
import io.questdb.network.ServerDisconnectException;

import static io.questdb.cutlass.http.HttpRequestValidator.METHOD_GET;

public interface HttpRequestProcessor {
    default AtomicLongGauge connectionCountGauge(Metrics metrics) {
        return metrics.jsonQueryMetrics().connectionCountGauge();
    }

    // after this callback is invoked, the server will disconnect the client.
    // if a processor desires to write a goodbye letter to the client,
    // it must also send TCP FIN by invoking socket.shutdownWrite()
    default void failRequest(
            HttpConnectionContext context,
            HttpException exception
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
    }

    default int getConnectionLimit(HttpContextConfiguration configuration) {
        return configuration.getJsonQueryConnectionLimit();
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
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException, QueryPausedException {
    }

    default void onRequestRetry(
            HttpConnectionContext context
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException, QueryPausedException {
    }

    default void parkRequest(HttpConnectionContext context, boolean pausedQuery) {
    }

    default boolean requiresAuthentication() {
        return getRequiredAuthType() != SecurityContext.AUTH_TYPE_NONE;
    }

    default void resumeRecv(HttpConnectionContext context) {
    }

    default void resumeSend(
            HttpConnectionContext context
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException, QueryPausedException {
    }
}
