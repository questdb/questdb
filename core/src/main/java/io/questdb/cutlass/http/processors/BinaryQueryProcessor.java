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
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cutlass.http.HttpChunkedResponse;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpException;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.cutlass.http.LocalValue;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.NoSpaceLeftInResponseBufferException;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.ServerDisconnectException;
import io.questdb.std.Misc;
import io.questdb.std.str.DirectUtf8Sequence;

import java.io.Closeable;

import static io.questdb.cutlass.http.HttpConstants.CONTENT_TYPE_TEXT;
import static io.questdb.cutlass.http.HttpConstants.URL_PARAM_QUERY;

/**
 * HTTP processor for binary query results using Streaming Columnar Binary Format (SCBF).
 * <p>
 * Handles GET requests with ?query= parameter and streams results in columnar binary format.
 * Supports non-blocking writes with proper state management across multiple resumeSend() calls.
 */
public class BinaryQueryProcessor implements HttpRequestProcessor, Closeable {
    private static final Log LOG = LogFactory.getLog(BinaryQueryProcessor.class);
    private static final LocalValue<BinaryQueryProcessorState> LV = new LocalValue<>();
    private final NetworkSqlExecutionCircuitBreaker circuitBreaker;
    private final CairoEngine engine;
    private final int maxSqlRecompileAttempts = 10;
    private final SqlExecutionContextImpl sqlExecutionContext;

    public BinaryQueryProcessor(
            CairoEngine engine,
            SqlExecutionContextImpl sqlExecutionContext,
            NetworkSqlExecutionCircuitBreaker circuitBreaker
    ) {
        this.engine = engine;
        this.sqlExecutionContext = sqlExecutionContext;
        this.circuitBreaker = circuitBreaker;
    }

    @Override
    public void close() {
        Misc.free(circuitBreaker);
    }

    @Override
    public void failRequest(HttpConnectionContext context, HttpException e) throws PeerDisconnectedException, PeerIsSlowToReadException {
        BinaryQueryProcessorState state = LV.get(context);
        if (state != null) {
            state.clear();
        }
        // Send error response if not already sent
        HttpChunkedResponse response = context.getChunkedResponse();
        response.status(500, CONTENT_TYPE_TEXT);
        response.sendHeader();
        response.put("Error: ").put(e.getMessage());
        response.sendChunk(true);
    }

    @Override
    public void onConnectionClosed(HttpConnectionContext context) {
        BinaryQueryProcessorState state = LV.get(context);
        if (state != null) {
            state.close();
        }
    }

    @Override
    public void onRequestComplete(HttpConnectionContext context)
            throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        BinaryQueryProcessorState state = LV.get(context);
        if (state == null) {
            LV.set(context, state = new BinaryQueryProcessorState(context));
        }

        try {
            // Parse query parameter
            if (!parseUrl(context, state)) {
                readyForNextRequest(context);
                return;
            }

            // Compile and execute query
            compileAndExecuteQuery(state);

        } catch (SqlException | ImplicitCastException e) {
            syntaxError(context, state, e);
            readyForNextRequest(context);
        } catch (CairoException | CairoError e) {
            internalError(context, state, e);
        }
    }

    @Override
    public void parkRequest(HttpConnectionContext context, boolean pausedQuery) {
        // No special parking needed - state is preserved in connection context
    }

    @Override
    public void resumeSend(HttpConnectionContext context)
            throws PeerDisconnectedException, PeerIsSlowToReadException {
        BinaryQueryProcessorState state = LV.get(context);
        if (state != null) {
            try {
                doResumeSend(state);
            } catch (CairoException | CairoError e) {
                try {
                    internalError(context, state, e);
                } catch (ServerDisconnectException ex) {
                    // Expected - just disconnect
                }
            }
        }
    }

    private void compileAndExecuteQuery(BinaryQueryProcessorState state)
            throws PeerDisconnectedException, PeerIsSlowToReadException, SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            for (int retries = 0; retries < maxSqlRecompileAttempts; retries++) {
                try {
                    // Compile SQL
                    CompiledQuery cc = compiler.compile(state.sqlText, sqlExecutionContext);

                    // Only support SELECT queries
                    if (cc.getType() != CompiledQuery.SELECT) {
                        throw SqlException.$(0, "Binary format only supports SELECT queries");
                    }

                    // Check if cacheable
                    RecordCursorFactory factory = cc.getRecordCursorFactory();
                    state.queryCacheable = factory.recordCursorSupportsRandomAccess();

                    // Try to open cursor
                    state.cursor = factory.getCursor(sqlExecutionContext);
                    state.recordCursorFactory = factory;

                    // Initialize serializer
                    state.serializer.of(factory.getMetadata(), state.cursor);

                    // Start sending response
                    doResumeSend(state);
                    return;

                } catch (TableReferenceOutOfDateException e) {
                    // Schema changed, retry compilation
                    if (retries == maxSqlRecompileAttempts - 1) {
                        throw SqlException.$(0, e.getFlyweightMessage());
                    }
                    // Clear and retry
                    state.clear();
                }
            }
        }
    }

    private void doResumeSend(BinaryQueryProcessorState state)
            throws PeerDisconnectedException, PeerIsSlowToReadException {
        HttpConnectionContext context = state.getHttpConnectionContext();
        HttpChunkedResponse response = context.getChunkedResponse();

        // State machine for binary response
        while (true) {
            try {
                switch (state.queryState) {
                    case BinaryQueryProcessorState.QUERY_PREFIX:
                        // Configure adapter with current response
                        state.adapter.of(response);

                        // Send HTTP headers (only if not already sent)
                        state.queryState = BinaryQueryProcessorState.QUERY_DATA;
                        response.status(200, "application/octet-stream");
                        response.sendHeader();

                        // fall through

                    case BinaryQueryProcessorState.QUERY_DATA:
                        // Serialize data using our streaming serializer
                        // This will write to the adapter's buffer
                        state.serializer.serialize(state.adapter);

                        // If we reach here, serialization is complete
                        // Flush any remaining data in adapter's buffer
                        state.adapter.flush();

                        state.queryState = BinaryQueryProcessorState.QUERY_SUFFIX;
                        // fall through

                    case BinaryQueryProcessorState.QUERY_SUFFIX:
                        // Send final chunk and cleanup
                        response.sendChunk(true);
                        readyForNextRequest(context);
                        return;
                }

            } catch (NoSpaceLeftInResponseBufferException e) {
                // Adapter's buffer is full - flush to response
                // This transfers data from adapter's buffer to HTTP response
                state.adapter.flush();
                // If flush succeeds, loop continues to write more data
            }
        }
    }

    private void internalError(HttpConnectionContext context, BinaryQueryProcessorState state, Throwable e)
            throws ServerDisconnectException, PeerDisconnectedException, PeerIsSlowToReadException {
        LOG.error().$("Internal error [ex=").$(e).I$();

        state.clear();

        // If we've already started sending response, must disconnect
        if (context.getLastRequestBytesSent() > 0) {
            throw ServerDisconnectException.INSTANCE;
        }

        // Send error response
        HttpChunkedResponse response = context.getChunkedResponse();
        response.status(500, CONTENT_TYPE_TEXT);
        response.sendHeader();
        response.put("Internal server error");
        response.sendChunk(true);
    }

    private boolean parseUrl(HttpConnectionContext context, BinaryQueryProcessorState state)
            throws PeerDisconnectedException, PeerIsSlowToReadException {
        HttpRequestHeader header = context.getRequestHeader();
        DirectUtf8Sequence query = header.getUrlParam(URL_PARAM_QUERY);

        if (query == null || query.size() == 0) {
            HttpChunkedResponse response = context.getChunkedResponse();
            response.status(400, CONTENT_TYPE_TEXT);
            response.sendHeader();
            response.put("Missing query parameter");
            response.sendChunk(true);
            return false;
        }

        // Store query text
        state.sqlText.clear();
        state.sqlText.put(query);
        return true;
    }

    private void readyForNextRequest(HttpConnectionContext context) {
        BinaryQueryProcessorState state = LV.get(context);
        if (state != null) {
            state.clear();
        }
    }

    private void syntaxError(HttpConnectionContext context, BinaryQueryProcessorState state, Exception e)
            throws PeerDisconnectedException, PeerIsSlowToReadException {
        LOG.info().$("Syntax error [q=").$(state.sqlText).$(" ,ex=").$(e).I$();

        state.clear();

        HttpChunkedResponse response = context.getChunkedResponse();
        response.status(400, CONTENT_TYPE_TEXT);
        response.sendHeader();
        response.put("Syntax error: ").put(e.getMessage());
        response.sendChunk(true);
    }

    /**
     * Adapter to make HttpChunkedResponse work with StreamingColumnarSerializer's BinaryDataSink interface.
     * Uses an intermediate buffer and flushes to response via writeBytes().
     */
    static class BinaryDataSinkAdapter implements io.questdb.cutlass.binary.BinaryDataSink, Closeable {
        private static final int BUFFER_SIZE = 64 * 1024; // 64KB buffer
        private final io.questdb.cutlass.binary.BinaryDataSinkImpl buffer;
        private int bookmarkPos;
        private long bufferAddress;
        private HttpChunkedResponse response;

        BinaryDataSinkAdapter(HttpChunkedResponse response) {
            this.response = response;
            this.buffer = new io.questdb.cutlass.binary.BinaryDataSinkImpl();
            this.bufferAddress = io.questdb.std.Unsafe.malloc(BUFFER_SIZE, io.questdb.std.MemoryTag.NATIVE_HTTP_CONN);
            this.buffer.of(bufferAddress, BUFFER_SIZE);
            this.bookmarkPos = 0;
        }

        @Override
        public int available() {
            return buffer.available();
        }

        @Override
        public int bookmark() {
            bookmarkPos = buffer.bookmark();
            return bookmarkPos;
        }

        @Override
        public void close() {
            if (bufferAddress != 0) {
                io.questdb.std.Unsafe.free(bufferAddress, BUFFER_SIZE, io.questdb.std.MemoryTag.NATIVE_HTTP_CONN);
                bufferAddress = 0;
            }
        }

        public void flush() throws PeerIsSlowToReadException, PeerDisconnectedException {
            int pos = buffer.position();
            if (pos > 0) {
                response.writeBytes(bufferAddress, pos);
                response.bookmark();
                buffer.of(bufferAddress, BUFFER_SIZE); // Reset buffer
                bookmarkPos = 0;
            }
        }

        public void of(HttpChunkedResponse response) {
            this.response = response;
        }

        @Override
        public void of(long address, int size) {
            // Not used - we manage our own buffer
        }

        @Override
        public int position() {
            return buffer.position();
        }

        @Override
        public void putByte(byte value) {
            buffer.putByte(value);
        }

        @Override
        public void putChar(char value) {
            buffer.putChar(value);
        }

        @Override
        public void putDouble(double value) {
            buffer.putDouble(value);
        }

        @Override
        public void putFloat(float value) {
            buffer.putFloat(value);
        }

        @Override
        public void putInt(int value) {
            buffer.putInt(value);
        }

        @Override
        public void putLong(long value) {
            buffer.putLong(value);
        }

        @Override
        public void putLong128(long lo, long hi) {
            buffer.putLong128(lo, hi);
        }

        @Override
        public void putLong256(io.questdb.std.Long256 value) {
            buffer.putLong256(value);
        }

        @Override
        public void putShort(short value) {
            buffer.putShort(value);
        }

        @Override
        public int putUtf8(io.questdb.std.str.Utf8Sequence value, int offset, int length) {
            return buffer.putUtf8(value, offset, length);
        }

        @Override
        public void rollback(int position) {
            buffer.rollback(bookmarkPos);
        }
    }
}
