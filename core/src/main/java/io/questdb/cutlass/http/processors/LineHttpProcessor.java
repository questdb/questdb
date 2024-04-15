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

package io.questdb.cutlass.http.processors;

import io.questdb.cairo.CairoEngine;
import io.questdb.cutlass.http.*;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.QueryPausedException;
import io.questdb.network.ServerDisconnectException;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;

import static io.questdb.cutlass.http.HttpConstants.CONTENT_TYPE_JSON;
import static io.questdb.cutlass.http.processors.LineHttpProcessorState.Status.*;
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

        // Method
        HttpRequestHeader requestHeader = context.getRequestHeader();
        if (!Utf8s.equalsNcAscii("POST", requestHeader.getMethod())) {
            state.reject(METHOD_NOT_SUPPORTED, "Not Found", context.getFd());
            return;
        }

        // Encoding
        Utf8Sequence encoding = requestHeader.getHeader(CONTENT_ENCODING);
        if (encoding != null && Utf8s.endsWithAscii(encoding, "gzip")) {
            state.reject(ENCODING_NOT_SUPPORTED, "gzip encoding is not supported", context.getFd());
            return;
        }

        byte timestampPrecision;
        DirectUtf8Sequence precision = requestHeader.getUrlParam(URL_PARAM_PRECISION);
        if (precision != null) {
            int len = precision.size();
            if ((len == 1 && precision.byteAt(0) == 'n') || (len == 2 && precision.byteAt(0) == 'n' && precision.byteAt(1) == 's')) {
                // V2 influx client sends "n" and V3 sends "ns"
                timestampPrecision = ENTITY_UNIT_NANO;
            } else if ((len == 1 && precision.byteAt(0) == 'u') || (len == 2 && precision.byteAt(0) == 'u' && precision.byteAt(1) == 's')) {
                // V2 influx client sends "u" and V3 sends "us"
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
                LOG.info().$("unsupported precision [url=")
                        .$(requestHeader.getUrl())
                        .$(", precision=").$(precision)
                        .I$();
                state.reject(PRECISION_NOT_SUPPORTED, "unsupported precision", context.getFd());
                return;
            }
        } else {
            timestampPrecision = ENTITY_UNIT_NANO;
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
    public void onRequestComplete(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException {
        state.onMessageComplete();
        if (state.isOk()) {
            state.commit();
        }
        // Check state again, commit may have failed
        if (state.isOk()) {
            state.setSendStatus(SendStatus.HEADER);
            context.simpleResponse().sendStatusNoContent(204);
        } else {
            state.setSendStatus(SendStatus.HEADER);
            sendErrorHeader(context);
            state.setSendStatus(SendStatus.CONTENT);
            sendErrorContent(context);
        }
    }

    @Override
    public void resumeRecv(HttpConnectionContext context) {
        state = LV.get(context);
    }

    @Override
    public void resumeSend(
            HttpConnectionContext context
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException, QueryPausedException {
        state = LV.get(context);
        assert state != null;

        switch (state.getSendStatus()) {
            case HEADER:
                context.resumeResponseSend();
                if (!state.isOk()) {
                    state.setSendStatus(SendStatus.CONTENT);
                    sendErrorContent(context);
                }
                break;

            case CONTENT:
                context.resumeResponseSend();
                break;

            default:
                throw HttpException.instance("unexpected send status: " + state.getSendStatus());
        }
    }

    private void sendErrorContent(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException {
        HttpChunkedResponse response = context.getChunkedResponse();
        state.formatError(response);
        response.sendChunk(true);
    }

    private void sendErrorHeader(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException {
        HttpChunkedResponse response = context.getChunkedResponse();
        response.status(state.getHttpResponseCode(), CONTENT_TYPE_JSON);
        response.sendHeader();
    }
}
