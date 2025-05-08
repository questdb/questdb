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
import io.questdb.cutlass.http.processors.RejectProcessor;
import io.questdb.metrics.AtomicLongGauge;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.QueryPausedException;
import io.questdb.network.ServerDisconnectException;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;

import static io.questdb.cutlass.http.HttpConstants.*;
import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;

public interface HttpRequestProcessor {
    default HttpRequestProcessor checkRequestSupported(HttpHeaderParser headerParser, RejectProcessor rejectProcessor) {
        final Utf8Sequence method = headerParser.getMethod();
        final long contentLength = headerParser.getContentLength();
        final boolean chunked = Utf8s.equalsNcAscii("chunked", headerParser.getHeader(HEADER_TRANSFER_ENCODING));
        final boolean multipartRequest = Utf8s.equalsNcAscii("multipart/form-data", headerParser.getContentType())
                || Utf8s.equalsNcAscii("multipart/mixed", headerParser.getContentType());
        final boolean multipartProcessor = this instanceof HttpMultipartContentListener;

        if (Utf8s.equalsNcAscii(METHOD_POST, method) || Utf8s.equalsNcAscii(METHOD_PUT, method)) {
            if (!multipartProcessor) {
                if (multipartRequest) {
                    return rejectProcessor.reject(HTTP_NOT_FOUND, "Method (multipart POST) not supported");
                } else if (!(this instanceof HttpContentListener)) {
                    return rejectProcessor.reject(HTTP_NOT_FOUND, "Method (POST/PUT) not supported");
                }
            }
            if (chunked && contentLength > 0) {
                return rejectProcessor.reject(HTTP_BAD_REQUEST, "Invalid chunked request; content-length specified");
            }
            if (!chunked && !multipartRequest && contentLength < 0) {
                return rejectProcessor.reject(HTTP_BAD_REQUEST, "Content-length not specified for POST/PUT request");
            }
        } else if (Utf8s.equalsNcAscii(METHOD_GET, method)) {
            if (chunked || multipartRequest || contentLength > 0) {
                return rejectProcessor.reject(HTTP_BAD_REQUEST, "GET request method cannot have content");
            }
            if (multipartProcessor) {
                return rejectProcessor.reject(HTTP_NOT_FOUND, "Method GET not supported");
            }
        } else {
            return rejectProcessor.reject(HTTP_BAD_REQUEST, "Method not supported");
        }
        return this;
    }

    default AtomicLongGauge connectionCountGauge(Metrics metrics) {
        return metrics.jsonQueryMetrics().connectionCountGauge();
    }

    // after this callback is invoked the server will disconnect the client
    // if processor desires to write a goodbye letter to the client
    // it must also send TCP FIN by invoking socket.shutdownWrite()
    default void failRequest(
            HttpConnectionContext context,
            HttpException exception
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
    }

    default int getConnectionLimit(HttpContextConfiguration configuration) {
        return configuration.getJsonQueryConnectionLimit();
    }

    default byte getRequiredAuthType(Utf8Sequence method) {
        return SecurityContext.AUTH_TYPE_CREDENTIALS;
    }

    default boolean isErrorProcessor() {
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

    default boolean processCookies(HttpConnectionContext context, SecurityContext securityContext) throws PeerIsSlowToReadException, PeerDisconnectedException {
        return true;
    }

    default boolean requiresAuthentication(Utf8Sequence method) {
        return getRequiredAuthType(method) != SecurityContext.AUTH_TYPE_NONE;
    }

    default void resumeRecv(HttpConnectionContext context) {
    }

    default void resumeSend(
            HttpConnectionContext context
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException, QueryPausedException {
    }
}
