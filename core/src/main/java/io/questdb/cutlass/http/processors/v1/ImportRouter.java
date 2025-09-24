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
import io.questdb.cutlass.http.processors.ImportProcessorState;
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
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

public class ImportRouter implements HttpRequestHandler {
    private final HttpMultipartContentProcessor postProcessor;
    private HttpConnectionContext transientContext;
    private ImportProcessorState transientState;


    public ImportRouter(CairoEngine cairoEngine, JsonQueryProcessorConfiguration configuration) {
        postProcessor = new ImportPostProcessor(cairoEngine, configuration);
    }

    public static ObjList<String> getRoutes(ObjList<String> parentRoutes) {
        ObjList<String> out = new ObjList<>(parentRoutes.size());
        for (int i = 0; i < parentRoutes.size(); i++) {
            out.extendAndSet(i, parentRoutes.get(i) + "/import");
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
     * Expects requests of form POST /api/v1/import
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
                // throw error
                return;
            }

            final DirectUtf8Sequence filename = partHeader.getContentDispositionFilename();

            if (filename == null || filename.size() == 0) {
                // throw error
                return;
            }

            state.clear();
            state.of(name, importRoot);

            LOG.info().$("starting parquet import [filename=").$(filename)
                    .$(", target=").$(state.path).$(']').$();
        }

        @Override
        public void onPartEnd() throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
            if (state.size != -1 && state.written != state.size) {
                // throw error
            }

            state.mem.setTruncateSize(state.written);
            decoder.of(state.mem.addressOf(0), state.written, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);

            if (decoder.metadata().columnCount() <= 0) {
                // throw error
            }

            state.mem.close(true, Vm.TRUNCATE_TO_POINTER);
            sendResponse(context);
            state.close();
        }

        @Override
        public void onRequestComplete(HttpConnectionContext context) {
            if (state != null) {
                state.clear();
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
            response.sendChunk(true);
            response.done();
        }

        private void sendResponse(HttpConnectionContext context) throws PeerIsSlowToReadException, PeerDisconnectedException, ServerDisconnectException {
            // check for error
            HttpChunkedResponse response = context.getChunkedResponse();
            response.bookmark();
            response.status(201, "text/plain");
            response.sendHeader();
            resumeSend(context);
        }

        public static class State implements Mutable, Closeable {
            FilesFacade ff;
            @Nullable DirectUtf8Sequence filename;
            long hi;
            long lo;
            @Nullable MemoryCMARW mem;
            Path path;
            long size;
            long written;

            public State(FilesFacade ff) {
                this.ff = ff;
                path = new Path();
            }

            public void clear() {

            }

            @Override
            public void close() {
                Misc.free(mem);
                Misc.free(path);
                filename = null;
                size = -1;
                written = -1;
            }

            public void of(DirectUtf8Sequence filename, CharSequence importRoot) {
                this.filename = filename;
                path.of(importRoot).concat(filename);
            }

            public void of(long lo, long hi) {
                this.lo = lo;
                this.hi = hi;
            }
        }
    }
}
