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

package io.questdb.cutlass.http.processors.v1;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cutlass.http.HttpChunkedResponse;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpKeywords;
import io.questdb.cutlass.http.HttpMultipartContentProcessor;
import io.questdb.cutlass.http.HttpRequestHandler;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.cutlass.http.LocalValue;
import io.questdb.cutlass.http.processors.JsonQueryProcessorConfiguration;
import io.questdb.griffin.engine.table.parquet.PartitionDecoder;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.ServerDisconnectException;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.Vect;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

import static io.questdb.cutlass.http.HttpConstants.CONTENT_TYPE_JSON_API;
import static io.questdb.cutlass.http.HttpConstants.URL_PARAM_OVERWRITE;

public class ImportsRouter implements HttpRequestHandler {
    private final HttpMultipartContentProcessor postProcessor;

    public ImportsRouter(CairoEngine cairoEngine, JsonQueryProcessorConfiguration configuration) {
        postProcessor = new ImportPostProcessor(cairoEngine, configuration);
    }

    public static ObjList<String> getRoutes(ObjList<String> parentRoutes) {
        ObjList<String> out = new ObjList<>(parentRoutes.size());
        for (int i = 0; i < parentRoutes.size(); i++) {
            out.extendAndSet(i, parentRoutes.get(i) + "/imports");
        }
        return out;
    }

    @Override
    public HttpRequestProcessor getProcessor(HttpRequestHeader requestHeader) {
        DirectUtf8Sequence method = requestHeader.getMethod();
        if (HttpKeywords.isPOST(method)) {
            return postProcessor;
        } else if (HttpKeywords.isGET(method)) {
            return null;
        }
        return null;
    }

    /**
     * Expects requests of the form POST /api/v1/import
     * multiple form/data requests can come in to upload multiple files at once
     * For now, locked to parquet files
     *
     */
    public static class ImportPostProcessor implements HttpMultipartContentProcessor {
        private static final Log LOG = LogFactory.getLog(ImportPostProcessor.class);
        private static final LocalValue<State> LV = new LocalValue<>();
        HttpConnectionContext context;
        PartitionDecoder decoder;
        CairoEngine engine;
        @Nullable CharSequence importRoot;
        byte requiredAuthType;
        State state;


        /**
         * Handles basic file upload (mainly for parquet, csv imports).
         * Returns:
         * 201 if successful with no location header and stored filenames in body
         * 409 if there's a write conflict
         * 400 if input root or filename are improperly set
         * 422 if the file ends with .parquet and it cannot be decoded
         * 500 for other errors
         */
        public ImportPostProcessor(CairoEngine cairoEngine, JsonQueryProcessorConfiguration configuration) {
            engine = cairoEngine;
            requiredAuthType = configuration.getRequiredAuthType();
            importRoot = engine.getConfiguration().getSqlCopyInputRoot();
            decoder = new PartitionDecoder();
        }

        @Override
        public byte getRequiredAuthType() {
            return requiredAuthType;
        }

        @Override
        public void onChunk(long lo, long hi) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
            if (hi > lo) {
                state.of(lo, hi);

                // on first entry, initialise files
                if (state.mem == null) {
                    assert state.path.size() != 0;
                    TableUtils.createDirsOrFail(state.ff, state.path, engine.getConfiguration().getMkDirMode());

                    state.mem = Vm.getCMARWInstance(
                            state.ff,
                            state.path.$(),
                            engine.getConfiguration().getDataAppendPageSize(),
                            state.size,
                            MemoryTag.MMAP_IMPORT, CairoConfiguration.O_NONE
                    );
                }

                final long chunkSize = hi - lo;
                long addr = state.mem.appendAddressFor(state.written, chunkSize);
                Vect.memcpy(addr, lo, chunkSize);
                state.written += chunkSize;
            }
        }

        @Override
        public void onPartBegin(HttpRequestHeader partHeader) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {

            final DirectUtf8Sequence name = partHeader.getContentDispositionName();
            if (Chars.isBlank(importRoot)) {
                // throw error 400
                state.errorMsg.put("invalid destination directory (sql.copy.input.root) [path=").put(importRoot).put(']');
                state.errorCode = 400;
                sendError(context);
                assert false; // exception should be thrown
            }

            final DirectUtf8Sequence filename = partHeader.getContentDispositionFilename();
            if (filename == null || filename.size() == 0) {
                state.errorMsg.put("received Content-Disposition without a filename");
                state.errorCode = 400;
                sendError(context);
                assert false;
            }

            state.clear();
            state.of(name, importRoot);

            // don't overwrite files by default
            final DirectUtf8Sequence overwrite = context.getRequestHeader().getUrlParam(URL_PARAM_OVERWRITE);
            boolean _overwrite = HttpKeywords.isTrue(overwrite);

            if (!_overwrite && state.ff.exists(state.path.$())) {
                // error 409 conflict
                state.errorMsg.put("file already exists and overwriting is disabled [file=").put(state.path).put(", overwrite=").put(false).put(']');
                state.errorCode = 409;
                sendError(context);
                assert false;
            }

            LOG.info().$("starting import [src=").$(filename)
                    .$(", dest=").$(state.path).I$();
        }

        @Override
        public void onPartEnd() throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
            assert state.mem != null;
            state.mem.setTruncateSize(state.written);

            if (Utf8s.endsWithAscii(state.path, ".parquet")) {
                try {
                    decoder.of(state.mem.addressOf(0), state.written, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);

                    if (decoder.metadata().columnCount() <= 0) {
                        // throw error 422
                        state.errorMsg.put("parquet file could not be decoded");
                        state.errorCode = 422;
                        sendError(context);
                        assert false;
                    }
                } catch (CairoException ce) {
                    LOG.error().$(ce.getMessage()).$();
                    state.errorMsg.put("internal server error");
                    state.errorCode = 500;
                    sendError(context);
                    assert false;
                }
            }

            state.mem.close(true, Vm.TRUNCATE_TO_POINTER);

            LOG.info().$("finished import [dest=").$(state.path).$(", size=").$(state.written).I$();
        }

        @Override
        public void onRequestComplete(HttpConnectionContext context) throws PeerIsSlowToReadException, ServerDisconnectException, PeerDisconnectedException {
            sendSuccess(context);
            if (state != null) {
                state.clear();
                state.filenames.clear();
            }
        }

        @Override
        public void onRequestRetry(
                HttpConnectionContext context
        ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
            this.context = context;
            this.state = LV.get(context);
            onChunk(state.lo, state.hi);
        }

        @Override
        public void resumeRecv(HttpConnectionContext context) {
            this.context = context;
            this.state = LV.get(context);
            if (state == null) {
                state = new State(engine.getConfiguration().getFilesFacade());
                LV.set(context, state);
            }
        }

        @Override
        public void resumeSend(
                HttpConnectionContext context
        ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
            context.resumeResponseSend();
            State state = LV.get(context);
            HttpChunkedResponse response = context.getChunkedResponse();
            assert state.errorMsg == null || state.errorMsg.size() < 1;
            encodeSuccessJson(response, state);
            response.sendChunk(true);
            response.done();
        }

        private void encodeErrorJson(HttpChunkedResponse response, State state) {
            response.bookmark();
            response.put("{\"errors\":[{")
                    .put("\"status\":\"").put(state.errorCode)
                    .put("\",\"detail\":\"").put(state.errorMsg)
                    .put("\"}]}");
        }

        private void encodeSuccessJson(HttpChunkedResponse response, State state) {
            response.put("{\"data\":[");
            for (int i = 0, n = state.filenames.size(); i < n; i++) {
                response.put("{\"type\":\"imports\",\"id\":\"");
                response.put(state.filenames.getQuick(i));
                response.put("\"}");
                if (i + 1 < n) {
                    response.put(",");
                }
            }
            response.put("]}");
        }

        private void sendError(HttpConnectionContext context) throws PeerIsSlowToReadException, PeerDisconnectedException, ServerDisconnectException {
            try {
                final State state = LV.get(context);
                final HttpChunkedResponse response = context.getChunkedResponse();
                if (state.errorCode < 500) {
                    LOG.error().$("POST /imports failed [status=").$(state.errorCode).$(", fd=").$(context.getFd()).$(", msg=").$safe(state.errorMsg).I$();
                }
                response.status(state.errorCode, state.errorMsg.asAsciiCharSequence());
                response.sendHeader();
                response.sendChunk(false);
                encodeErrorJson(response, state);
                response.sendChunk(true);
                response.shutdownWrite();
                throw ServerDisconnectException.INSTANCE;
            } finally {
                state.clear();
            }
        }

        private void sendSuccess(HttpConnectionContext context) throws PeerIsSlowToReadException, PeerDisconnectedException, ServerDisconnectException {
            try {
                HttpChunkedResponse response = context.getChunkedResponse();
                response.bookmark();
                // We do not send a location header since there may be multiple locations.
                response.status(201, CONTENT_TYPE_JSON_API);
                response.sendHeader();
                resumeSend(context);
            } finally {
                state.clear();
            }
        }

        public static class State implements Mutable, Closeable {
            int errorCode;
            Utf8StringSink errorMsg;
            FilesFacade ff;
            @Nullable DirectUtf8Sequence filename;
            ObjList<String> filenames;
            long hi;
            long lo;
            @Nullable MemoryCMARW mem;
            Path path;
            long size;
            long written;

            public State(FilesFacade ff) {
                this.ff = ff;
                path = new Path();
                filenames = new ObjList<>();
                errorMsg = new Utf8StringSink();
            }

            public void clear() {
                close();
            }

            @Override
            public void close() {
                Misc.free(mem);
                mem = null;
                Misc.free(path);
                filename = null;
                size = -1;
                written = -1;
                errorMsg.clear();
                errorCode = -1;
            }

            public void of(DirectUtf8Sequence filename, CharSequence importRoot) {
                this.filename = filename;
                filenames.add(filename.toString());
                path.of(importRoot).concat(filename);
            }

            public void of(long lo, long hi) {
                this.lo = lo;
                this.hi = hi;
            }
        }
    }
}
