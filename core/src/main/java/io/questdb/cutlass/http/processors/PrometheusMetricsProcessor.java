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
    private static final CharSequence CONTENT_TYPE_TEXT = "text/plain; version=0.0.4; charset=utf-8";
    private static final LocalValue<State> LV = new LocalValue<>();
    private final Scrapable metrics;
    private final boolean requiresAuthentication;
    public PrometheusMetricsProcessor(Scrapable metrics, HttpMinServerConfiguration configuration) {
        this.metrics = metrics;
        this.requiresAuthentication = configuration.isHealthCheckAuthenticationRequired();
    }

    @Override
    public void onRequestComplete(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException {
        System.err.println("onRequestComplete :: (A)");
        final State state = setupState(context);

        // We double-buffer the metrics response.
        // This is because we send back chunked responses and each chunk may not be large enough.
        metrics.scrapeIntoPrometheus(state.sink);

        final HttpChunkedResponseSocket r = context.getChunkedResponseSocket();
        r.status(200, CONTENT_TYPE_TEXT);
        r.sendHeader();
        sendResponse("onRequestComplete", r, state);
    }

    @Override
    public boolean requiresAuthentication() {
        return requiresAuthentication;
    }

    @Override
    public void resumeSend(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException {
        System.err.println("resumeSend :: (A)");
        final HttpChunkedResponseSocket r = context.getChunkedResponseSocket();
        System.err.println("resumeSend :: (B) r.availForWrite(): " + r.availForWrite());
        try {
            context.resumeResponseSend();
        } catch (PeerDisconnectedException | PeerIsSlowToReadException ex) {
            System.err.println("resumeSend :: (C) ex: " + ex);
            throw ex;
        }

        System.err.println("resumeSend :: (C)");

        // Continues after `PeerIsSlowToReadException` is thrown.
        final State state = LV.get(context);
        assert state != null;

        System.err.println("resumeSend :: (D)");

        System.err.println("resumeSend :: (E)");
        sendResponse("resumeSend", r, state);

        System.err.println("resumeSend :: (F)");
    }

    private static State setupState(HttpConnectionContext context) {
        State state = LV.get(context);
        if (state != null) {
            state.reset();
        } else {
            LV.set(context, state = new State());
        }
        return state;
    }

    private void sendResponse(String caller, HttpChunkedResponseSocket r, State state) throws PeerIsSlowToReadException, PeerDisconnectedException {
        System.err.println("sendResponse :: (A)");

        try {
            while (!sendNextChunk(caller, r, state)) {
                System.err.println("sendResponse :: (B)");
            }
        } catch (PeerIsSlowToReadException | PeerDisconnectedException ex) {
            System.err.println("sendResponse :: (C) ex: " + ex);
            throw ex;
        }

        System.err.println("sendResponse :: (D)");
    }

    private boolean sendNextChunk(String caller, HttpChunkedResponseSocket r, State state) throws PeerIsSlowToReadException, PeerDisconnectedException {
        final int remain = state.sink.length() - state.written;
        final int avail = r.availForWrite();
        System.err.println(caller + " -> sendNextChunk :: (A) remain: " + remain + ", avail: " + avail);
        if (avail == 0) {
            System.err.println(caller + " -> sendNextChunk :: (B) avail == 0, throwing PeerIsSlowToReadException");
            throw PeerIsSlowToReadException.INSTANCE;
        }
        final int chunkLen = Math.min(avail, remain);
        r.writeBytes(state.sink.getPtr() + state.written, chunkLen);
        state.written += chunkLen;
        final boolean done = chunkLen == remain;
        System.err.println(caller + " -> sendNextChunk :: (C) done: " + done);
        r.sendChunk(done);
        System.err.println(caller + " -> sendNextChunk :: (D) (sent!) done: " + done);
        return done;
    }

    private static class State implements QuietCloseable {
        /**
         * Metrics serialization destination, sent into one or more chunks later.
         */
        public final DirectByteCharSink sink = new DirectByteCharSink(4096);  // 1 page on most systems

        /**
         * Total number of bytes written to one or more chunks (`HttpChunkedResponseSocket` objects).
         */
        public int written = 0;

        @Override
        public void close() {
            sink.close();
        }

        public void reset() {
            sink.clear();
            written = 0;
        }
    }
}
