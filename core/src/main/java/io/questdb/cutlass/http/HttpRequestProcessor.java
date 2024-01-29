/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.network.QueryPausedException;
import io.questdb.network.ServerDisconnectException;

public interface HttpRequestProcessor {
    // after this callback is invoked the server will disconnect the client
    // if processor desires to write a goodbye letter to the client
    // it must also send TCP FIN by invoking socket.shutdownWrite()
    default void failRequest(
            HttpConnectionContext context,
            HttpException exception
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
    }

    default byte getRequiredAuthType() {
        return SecurityContext.AUTH_TYPE_CREDENTIALS;
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

    default boolean processCookies(HttpConnectionContext context, SecurityContext securityContext) throws PeerIsSlowToReadException, PeerDisconnectedException {
        return true;
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
