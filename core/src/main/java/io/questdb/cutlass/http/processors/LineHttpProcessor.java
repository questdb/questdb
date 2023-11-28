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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cutlass.http.*;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.ServerDisconnectException;

public class LineHttpProcessor implements HttpRequestProcessor, HttpMultipartContentListener {
    private static final Log LOG = LogFactory.getLog(StaticContentProcessor.class);
    private static final LocalValue<LineHttpProcessorState> LV = new LocalValue<>();
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
            if (state.getStatus() == LineHttpProcessorState.Status.ERROR) {
                sendError(context, state.getError());
            } else if (state.getStatus() == LineHttpProcessorState.Status.OK) {
                try {
                    state.commit();
                    if (state.getStatus() == LineHttpProcessorState.Status.OK) {
                        HttpChunkedResponseSocket r = context.getChunkedResponseSocket();
                        r.status(200, "text/plain");
                        r.sendHeader();
                        r.putAscii("OK");
                        r.sendChunk(true);
                    } else {
                        // TODO: better error message
                        sendError(context, "commit failed");
                    }
                } catch (Throwable th) {
                    LOG.error().$(th).$();
                    sendError(context, th.getMessage());
                }
            } else if (state.getStatus() == LineHttpProcessorState.Status.NEEDS_REED) {
                LOG.error().$("Incomplete request").$();
                sendError(context, "Incomplete request");
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
        state.of(context.getFd());
    }

    private void sendError(HttpConnectionContext context, CharSequence errorText) throws PeerDisconnectedException, PeerIsSlowToReadException {
        HttpChunkedResponseSocket r = context.getChunkedResponseSocket();
        r.status(400, "text/plain");
        r.sendHeader();
        r.putAscii(errorText);
        r.sendChunk(true);
    }
}
