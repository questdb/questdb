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

package io.questdb.cutlass.http.processors;

import io.questdb.cairo.CairoEngine;
import io.questdb.cutlass.http.*;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.std.str.*;

import static io.questdb.cutlass.line.tcp.LineTcpParser.*;

public class LineHttpProcessor implements HttpRequestProcessor, HttpMultipartContentListener {
    public static final Utf8String URL_PARAM_PRECISION = new Utf8String("precision");
    private static final Log LOG = LogFactory.getLog(StaticContentProcessor.class);
    private static final LocalValue<LineHttpProcessorState> LV = new LocalValue<>();
    private static final Utf8String QUESTDB_ERROR_CONTENT_TYPE = new Utf8String("questdb-error-content-type");
    private final LineHttpProcessorConfiguration configuration;
    private final CairoEngine engine;
    private final int recvBufferSize;
    LineHttpProcessorState state;

    public LineHttpProcessor(CairoEngine engine, int recvBufferSize, LineHttpProcessorConfiguration configuration) {
        this.engine = engine;
        this.recvBufferSize = recvBufferSize;
        this.configuration = configuration;
    }

    @Override
    public void failRequest(HttpConnectionContext context, HttpException exception) {
        LOG.info().$("rolling back, client disconnected [fd=").$(context.getFd()).I$();
        state = LV.get(context);
        if (state != null) {
            state.onDisconnected();
        }
    }

    @Override
    public void onChunk(long lo, long hi) {
        this.state.parse(lo, hi);
    }

    @Override
    public void onConnectionClosed(HttpConnectionContext context) {
        state = LV.get(context);
        if (state != null) {
            state.onDisconnected();
        }
    }

    public void onHeadersReady(HttpConnectionContext context) {
        state = LV.get(context);
        if (state == null) {
            state = new LineHttpProcessorState(recvBufferSize, engine, configuration);
            LV.set(context, state);
        } else {
            state.clear();
        }
        byte timestampPrecision = ENTITY_UNIT_NANO;
        DirectUtf8Sequence precision = context.getRequestHeader().getUrlParam(URL_PARAM_PRECISION);
        if (precision != null) {
            int len = precision.size();
            if (len == 1 && precision.byteAt(0) == 'n') {
                timestampPrecision = ENTITY_UNIT_NANO;
            } else if (len == 1 && precision.byteAt(0) == 'u') {
                timestampPrecision = ENTITY_UNIT_MICRO;
            } else if (len == 2 && precision.byteAt(0) == 'm' && precision.byteAt(1) == 's') {
                timestampPrecision = ENTITY_UNIT_MILLI;
            } else if (len == 1 && precision.byteAt(0) == 's') {
                timestampPrecision = ENTITY_UNIT_SECOND;
            } else if (len == 1 && precision.byteAt(0) == 'm') {
                timestampPrecision = ENTITY_UNIT_MINUTE;
            } else if (len == 1 && precision.byteAt(0) == 'h') {
                timestampPrecision = ENTITY_UNIT_HOUR;
            } else {
                throw HttpException.instance("unsupported precision in URL query string [precision=").put(precision).put(']');
            }
        }
        final boolean plainTextErrors = Utf8s.equalsAscii("text/plain", context.getRequestHeader().getHeader(QUESTDB_ERROR_CONTENT_TYPE));
        state.of(context.getFd(), timestampPrecision, plainTextErrors);
    }

    @Override
    public void onPartBegin(HttpRequestHeader partHeader) {
    }

    @Override
    public void onPartEnd() {
    }

    @Override
    public void onRequestComplete(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException {
        state = LV.get(context);

        try {
            HttpChunkedResponseSocket r = context.getChunkedResponseSocket();
            state.onMessageComplete();
            if (state.isOk()) {
                state.commit();
            }

            // Check state again, commit may have failed
            if (state.isOk()) {
                r.status(204, "text/plain"); // OK, no content
                r.sendHeader();
                r.send();
            } else {
                sendError(context);
            }
        } finally {
            state.clear();
        }
    }

    @Override
    public boolean requiresAuthentication() {
        return false;
    }

    @Override
    public void resumeRecv(HttpConnectionContext context) {
        state = LV.get(context);
    }

    private void sendError(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException {
        HttpChunkedResponseSocket r = context.getChunkedResponseSocket();
        if (state.respondingWithPlainTextErrors()) {
            r.status(state.getHttpResponseCode(), "text/plain");
            final HttpResponseHeader header = context.getResponseHeader();
            state.formatPlainTextErrorHeader(header);
            r.sendHeader();
            state.formatPlainTextErrorMessage(r);
        } else {
            r.status(state.getHttpResponseCode(), "application/json");
            r.sendHeader();
            state.formatJsonError(r);
        }
        r.sendChunk(true);
    }
}
