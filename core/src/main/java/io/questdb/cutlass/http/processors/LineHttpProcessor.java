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
import io.questdb.std.str.DirectUtf8Sequence;

import static io.questdb.cutlass.http.HttpConstants.TRACE_ID;

public class LineHttpProcessor implements HttpRequestProcessor, HttpMultipartContentListener {
    private static final Log LOG = LogFactory.getLog(StaticContentProcessor.class);
    private static final LocalValue<LineHttpProcessorState> LV = new LocalValue<>();
    private final LineHttpProcessorConfiguration configuration;
    private final CairoEngine engine;
    private final int recvBufferSize;
    LineHttpProcessorState state;
    private DirectUtf8Sequence requestId;

    public LineHttpProcessor(CairoEngine engine, int recvBufferSize, LineHttpProcessorConfiguration configuration) {
        this.engine = engine;
        this.recvBufferSize = recvBufferSize;
        this.configuration = configuration;
    }

    @Override
    public void failRequest(HttpConnectionContext context, HttpException exception) {
        LOG.info().$("rolling back, client disconnected [fd=").$(context.getFd()).I$();
        this.state.clear();
    }

    @Override
    public void onChunk(long lo, long hi) {
        this.state.parse(lo, hi);
    }

    public void onHeadersReady(HttpConnectionContext context) {
        state = LV.get(context);
        if (state == null) {
            state = new LineHttpProcessorState(recvBufferSize, engine, configuration);
            LV.set(context, state);
        } else {
            state.clear();
        }
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
                try {
                    state.commit();
                    r.status(204, "text/plain"); // OK, no content
                    r.sendHeader();
                    r.send();
                } catch (Throwable th) {
                    LOG.error().$("unexpected error committing the data [requestId=").$(requestId).$(", exception=").$(th).$();
                    sendError(context, "commit failed", th);
                }
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
        requestId = context.getRequestHeader().getHeader(TRACE_ID);
        state.of(context.getFd(), requestId);
    }

    private void sendError(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException {
        HttpChunkedResponseSocket r = context.getChunkedResponseSocket();
        r.status(state.getHttpResponseCode(), "text/plain");
        r.sendHeader();
        state.formatError(r);
        r.sendChunk(true);
    }

    private void sendError(HttpConnectionContext context, String message, Throwable th) throws PeerDisconnectedException, PeerIsSlowToReadException {
        HttpChunkedResponseSocket r = context.getChunkedResponseSocket();
        r.status(500, "text/plain");
        r.sendHeader();
        state.formatError(r, message, th);
        r.sendChunk(true);
    }
}
