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
import io.questdb.cairo.CairoError;
import io.questdb.cutlass.http.*;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;

import static io.questdb.cutlass.line.tcp.LineTcpParser.*;

public class LineHttpProcessor implements HttpRequestProcessor, HttpMultipartContentListener {
    private static final Utf8String CONTENT_ENCODING = new Utf8String("Content-Encoding");
    private static final Log LOG = LogFactory.getLog(StaticContentProcessor.class);
    private static final LocalValue<LineHttpProcessorState> LV = new LocalValue<>();
    private static final Utf8String URL_PARAM_PRECISION = new Utf8String("precision");
    private final LineHttpProcessorConfiguration configuration;
    private final CairoEngine engine;
    private final int maxResponseContentLength;
    private final int recvBufferSize;
    LineHttpProcessorState state;

    public LineHttpProcessor(CairoEngine engine, int recvBufferSize, int maxResponseContentLength, LineHttpProcessorConfiguration configuration) {
        this.engine = engine;
        this.recvBufferSize = recvBufferSize;
        this.maxResponseContentLength = maxResponseContentLength;
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
            state = new LineHttpProcessorState(recvBufferSize, maxResponseContentLength, engine, configuration);
            LV.set(context, state);
        } else {
            state.clear();
        }
        // Encoding
        Utf8Sequence encoding = context.getRequestHeader().getHeader(CONTENT_ENCODING);
        if (encoding != null && Utf8s.endsWithAscii(encoding, "gzip")) {
            LOG.error().$("gzip encoding not supported [fd=").put(context.getFd()).I$();
            try {
                HttpChunkedResponseSocket r = context.getChunkedResponseSocket();
                r.status(415, "text/plain");
                r.sendHeader();
                r.putAscii("gzip encoding not supported");
                r.sendChunk(true);
                throw HttpException.instance("gzip encoding not supported [fd=").put(context.getFd()).put(']');
            } catch (PeerDisconnectedException | PeerIsSlowToReadException e) {
                throw HttpException.instance("could not send simple response 415 to sender [fd=").put(context.getFd()).put(']');
            }
        }

        byte timestampPrecision = ENTITY_UNIT_NANO;
        DirectUtf8Sequence precision = context.getRequestHeader().getUrlParam(URL_PARAM_PRECISION);
        if (precision != null) {
            int len = precision.size();
            if ((len == 1 && precision.byteAt(0) == 'n') || (len == 2 && precision.byteAt(0) == 'n' && precision.byteAt(1) == 's')) {
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
        state.of(context.getFd(), timestampPrecision, context.getSecurityContext());
    }

    @Override
    public void onPartBegin(HttpRequestHeader partHeader) {
    }

    @Override
    public void onPartEnd() {
    }

    @Override
    public void onRequestComplete(HttpConnectionContext context) throws PeerDisconnectedException {
        try {
            state.onMessageComplete();
            if (state.isOk()) {
                state.commit();
            }

            // Check state again, commit may have failed
            if (state.isOk()) {
                context.simpleResponse().sendStatus(204);
            } else {
                sendError(context);
            }
        } catch (PeerIsSlowToReadException e) {
            LOG.critical().put("ILP HTTP response did not fit socket buffer, response cannot " +
                    " be delivered [fd=").put(context.getFd()).put(", responseCode=").put(state.getHttpResponseCode()).$();
            throw new CairoError(e);
        } finally {
            state.clear();
        }
    }

    @Override
    public void resumeRecv(HttpConnectionContext context) {
        state = LV.get(context);
    }

    private void sendError(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException {
        HttpChunkedResponseSocket r = context.getChunkedResponseSocket();
        r.status(state.getHttpResponseCode(), "text/plain");
        r.sendHeader();
        state.formatError(r);
        r.sendChunk(true);
    }
}
