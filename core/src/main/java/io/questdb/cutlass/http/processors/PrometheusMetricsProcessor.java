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

import io.questdb.cutlass.http.*;
import io.questdb.metrics.Scrapable;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.std.QuietCloseable;
import io.questdb.std.str.DirectByteCharSink;

public class PrometheusMetricsProcessor implements HttpRequestProcessor {
    private static class State implements QuietCloseable {
        public final DirectByteCharSink sink = new DirectByteCharSink(4096);  // 1 page on most systems
        public int lastChunkLen = 0;
        public int sent = 0;

        @Override
        public void close() {
            sink.close();
        }
    }

    private static final CharSequence CONTENT_TYPE_TEXT = "text/plain; version=0.0.4; charset=utf-8";
    private static final LocalValue<State> LV = new LocalValue<>();
    private final Scrapable metrics;
    private final boolean requiresAuthentication;

    public PrometheusMetricsProcessor(Scrapable metrics, HttpMinServerConfiguration configuration) {
        this.metrics = metrics;
        this.requiresAuthentication = configuration.isHealthCheckAuthenticationRequired();
    }

    private static State setupState(HttpConnectionContext context) {
        State state = LV.get(context);
        if (state != null) {
            state.sink.clear();
            state.sent = 0;
        } else {
            LV.set(context, state = new State());
        }
        return state;
    }

    private boolean writeNextChunk(HttpChunkedResponseSocket r, State state) throws PeerIsSlowToReadException, PeerDisconnectedException {
        final int remain = state.sink.length() - state.sent;
        final int avail = r.availForWrite();
        System.err.println("writeNextChunk :: (A) remain: " + remain + ", avail: " + avail);
        final int chunkLen = Math.min(avail, remain);
        r.writeBytes(state.sink.getPtr() + state.sent, chunkLen);
        final boolean done = chunkLen == remain;
        state.lastChunkLen = chunkLen;
        System.err.println("writeNextChunk :: (B) done: " + done);
        return done;
    }

    @Override
    public void onRequestComplete(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final State state = setupState(context);

        // We double-buffer the metrics response.
        // This is because we send back chunked responses and each chunk may not be large enough.
        metrics.scrapeIntoPrometheus(state.sink);

        final HttpChunkedResponseSocket r = context.getChunkedResponseSocket();
        r.status(200, CONTENT_TYPE_TEXT);
        r.sendHeader();
        sendResponse(r, state);
    }

    private void sendResponse(HttpChunkedResponseSocket r, State state) throws PeerIsSlowToReadException, PeerDisconnectedException {
        boolean done = false;
        while (!done) {
            System.err.println("onRequestComplete :: (A)");
            done = writeNextChunk(r, state);
            r.sendChunk(done);

            // `writeNextChunk` and `sendChunk` may raise `PeerIsSlowToReadException`.
            // As such we track the number of sent bytes only after a successful call.
            // If `PeerIsSlowToReadException` is raised, the rest will be sent later via `resumeSend`.
            state.sent += state.lastChunkLen;
        }
    }

    @Override
    public void resumeSend(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException {
        // Continues after `PeerIsSlowToReadException` is thrown.
        final State state = LV.get(context);
        assert state != null;

        final HttpChunkedResponseSocket r = context.getChunkedResponseSocket();
        sendResponse(r, state);
    }

    @Override
    public boolean requiresAuthentication() {
        return requiresAuthentication;
    }
}
