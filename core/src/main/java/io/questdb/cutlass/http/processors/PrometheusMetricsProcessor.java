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
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.str.DirectByteCharSink;

public class PrometheusMetricsProcessor implements HttpRequestProcessor, QuietCloseable {
    private static final CharSequence CONTENT_TYPE_TEXT = "text/plain; version=0.0.4; charset=utf-8";
    private static final LocalValue<RequestState> LV = new LocalValue<>();
    private final Scrapable metrics;
    private final RequestStatePool pool;
    private final boolean requiresAuthentication;

    public PrometheusMetricsProcessor(Scrapable metrics, HttpMinServerConfiguration configuration) {
        this.metrics = metrics;
        this.requiresAuthentication = configuration.isHealthCheckAuthenticationRequired();
        this.pool = new RequestStatePool(configuration.getWorkerCount());
    }

    @Override
    public void close() {
        pool.close();
    }

    @Override
    public void onRequestComplete(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException {
        System.err.println("onRequestComplete :: (A)");
        final RequestState state = setupState(context);

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
        final RequestState state = LV.get(context);
        assert state != null;

        System.err.println("resumeSend :: (D)");

        System.err.println("resumeSend :: (E)");
        sendResponse("resumeSend", r, state);

        System.err.println("resumeSend :: (F)");
    }

    private boolean sendNextChunk(String caller, HttpChunkedResponseSocket r, RequestState state) throws PeerIsSlowToReadException, PeerDisconnectedException {
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

    private void sendResponse(String caller, HttpChunkedResponseSocket r, RequestState state) throws PeerIsSlowToReadException, PeerDisconnectedException {
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

    private RequestState setupState(HttpConnectionContext context) {
        RequestState state = pool.take();
        LV.set(context, state);
        return state;
    }

    /**
     * State for processing a single request across multiple response chunks.
     * Each object is used for the lifetime of one request, then returned the pool.
     */
    private static class RequestState implements QuietCloseable {
        /**
         * Metrics serialization destination, sent into one or more chunks later.
         */
        public final DirectByteCharSink sink = new DirectByteCharSink(4096);  // One page on most systems.
        private final RequestStatePool pool;
        /**
         * Total number of bytes written to one or more chunks (`HttpChunkedResponseSocket` objects).
         */
        public int written = 0;

        private RequestState(RequestStatePool pool) {
            this.pool = pool;
        }

        /**
         * Return to pool at the end of a request.
         */
        @Override
        public void close() {
            sink.clear();
            written = 0;
            pool.give(this);
        }

        /**
         * Release off-heap buffer.
         */
        public void free() {
            sink.close();
        }
    }

    private static class RequestStatePool implements QuietCloseable {
        private final ObjList<RequestState> objects;

        public RequestStatePool(int initialCapacity) {
            this.objects = new ObjList<>(initialCapacity);
        }

        @Override
        public void close() {
            for (int i = 0, n = objects.size(); i < n; i++) {
                objects.getQuick(i).free();
            }
        }

        public synchronized void give(RequestState requestState) {
            objects.add(requestState);
        }

        public synchronized RequestState take() {
            final RequestState state;
            if (objects.size() > 0) {
                final int index = objects.size() - 1;
                state = objects.getQuick(index);
            } else {
                state = new RequestState(this);
            }
            return state;
        }
    }
}
