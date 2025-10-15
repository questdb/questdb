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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableUtils;
import io.questdb.cutlass.http.HttpChunkedResponse;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpKeywords;
import io.questdb.cutlass.http.HttpMultipartContentProcessor;
import io.questdb.cutlass.http.HttpRequestHandler;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.cutlass.http.LocalValue;
import io.questdb.cutlass.http.processors.JsonQueryProcessorConfiguration;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.ServerDisconnectException;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;

import java.io.Closeable;
import java.util.function.LongSupplier;

import static io.questdb.cutlass.http.HttpConstants.CONTENT_TYPE_JSON_API;
import static io.questdb.cutlass.http.HttpConstants.URL_PARAM_OVERWRITE;
import static io.questdb.cutlass.http.HttpResponseSink.HTTP_MULTI_STATUS;

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
     */
    public static class ImportPostProcessor implements HttpMultipartContentProcessor {
        private static final Log LOG = LogFactory.getLog(ImportPostProcessor.class);
        private static final LocalValue<State> LV = new LocalValue<>();

        HttpConnectionContext context;
        CairoEngine engine;
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
        }

        @Override
        public byte getRequiredAuthType() {
            return requiredAuthType;
        }

        @Override
        public void onChunk(long lo, long hi) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
            if (hi > lo) {
                state.of(lo, hi);

                // on first entry, open file for writing
                if (state.fd == -1) {
                    TableUtils.createDirsOrFail(state.ff, state.tempPath, engine.getConfiguration().getMkDirMode());
                    state.fd = Files.openRW(state.tempPath.$());
                    if (state.fd == -1) {
                        sendErrorWithSuccessInfo(context, 500, "cannot open file for writing [path=" + state.tempPath + ']');
                        return;
                    }
                    state.written = 0;
                }

                final long chunkSize = hi - lo;
                long bytesWritten = Files.write(state.fd, lo, chunkSize, state.written);
                if (bytesWritten != chunkSize) {
                    sendErrorWithSuccessInfo(context, 500, "failed to write chunk [expected=" + chunkSize + ", actual=" + bytesWritten + ']');
                    return;
                }
                state.written += chunkSize;
            }
        }

        @Override
        public void onPartBegin(HttpRequestHeader partHeader) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
            CharSequence importRoot = engine.getConfiguration().getSqlCopyInputRoot();
            if (Chars.isBlank(importRoot)) {
                sendErrorWithSuccessInfo(context, 400, "invalid destination directory (sql.copy.input.root) [path=" + importRoot + ']');
            }

            final DirectUtf8Sequence filename = partHeader.getContentDispositionName();
            if (filename == null || filename.size() == 0) {
                sendErrorWithSuccessInfo(context, 400, "received Content-Disposition without a filename");
                return;
            }

            state.currentFilename = filename.toString();
            state.of(filename, importRoot);
            state.overwrite = HttpKeywords.isTrue(context.getRequestHeader().getUrlParam(URL_PARAM_OVERWRITE));
            if (!state.overwrite && state.ff.exists(state.realPath.$())) {
                // error 409 conflict
                sendErrorWithSuccessInfo(context, 409, "file already exists and overwriting is disabled");
                return;
            }
            LOG.info().$("starting import [src=").$(filename)
                    .$(", dest=").$(state.realPath).I$();
        }

        @Override
        public void onPartEnd() throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
            if (state.fd != -1) {
                Files.close(state.fd);
                state.fd = -1;
            }

            if (state.ff.exists(state.realPath.$())) {
                if (!state.overwrite) {
                    state.ff.removeQuiet(state.tempPath.$());
                    sendErrorWithSuccessInfo(context, 409, "file already exists and overwriting is disabled [path=" + state.realPath + ", overwrite=false]");
                    return;
                } else {
                    state.ff.removeQuiet(state.realPath.$());
                }
            }

            TableUtils.createDirsOrFail(state.ff, state.realPath, engine.getConfiguration().getMkDirMode());
            int renameResult = state.ff.rename(state.tempPath.$(), state.realPath.$());
            if (renameResult != Files.FILES_RENAME_OK) {
                int errno = state.ff.errno();
                LOG.error().$("failed to rename temporary file [tempPath=").$(state.tempPath)
                        .$(", realPath=").$(state.realPath)
                        .$(", errno=").$(errno)
                        .$(", renameResult=").$(renameResult).I$();
                state.ff.remove(state.tempPath.$());
                sendErrorWithSuccessInfo(context, 500, "cannot rename temporary file [tempPath=" + state.tempPath +
                        ", realPath=" + state.realPath + ", errno=" + errno + ", renameResult=" + renameResult + ']');
                return;
            }

            state.successfulFiles.add(state.currentFilename);
            LOG.info().$("finished import [dest=").$(state.realPath).$(", size=").$(state.written).I$();
        }

        @Override
        public void onRequestComplete(HttpConnectionContext context) throws PeerIsSlowToReadException, ServerDisconnectException, PeerDisconnectedException {
            sendSuccess(context);
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
            if (state.fd != -1 && state.written != 0) {
                state.ff.truncate(state.fd, state.written);
            }
            onChunk(state.lo, state.hi);
        }

        @Override
        public void resumeRecv(HttpConnectionContext context) {
            this.context = context;
            this.state = LV.get(context);
            if (state == null) {
                state = new State(engine.getConfiguration().getFilesFacade(), engine.getConfiguration().getCopyIDSupplier());
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
                    .put("\"");

            if (state.errorCode == 409) {
                response.put(",\"meta\":{\"name\":\"")
                        .put(Utf8s.stringFromUtf8Bytes(state.realPath.lo() + 1, state.realPath.hi()))
                        .put("\"}");
            }
            response.put("}]}");
        }

        private void encodeSuccessJson(HttpChunkedResponse response, State state) {
            response.put("{\"data\":[");
            for (int i = 0, n = state.successfulFiles.size(); i < n; i++) {
                response.put("{\"type\":\"imports\",\"id\":\"");
                response.put(state.successfulFiles.getQuick(i));
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

        private void sendErrorWithSuccessInfo(HttpConnectionContext context, int errorCode, String errorMsg) throws PeerIsSlowToReadException, PeerDisconnectedException, ServerDisconnectException {
            try {
                final HttpChunkedResponse response = context.getChunkedResponse();
                if (state.successfulFiles.size() > 0) {
                    response.status(HTTP_MULTI_STATUS, CONTENT_TYPE_JSON_API);
                    response.sendHeader();
                    response.sendChunk(false);
                    response.put("{\"successful\":[");
                    for (int i = 0, n = state.successfulFiles.size(); i < n; i++) {
                        response.put("\"").put(state.successfulFiles.getQuick(i)).put("\"");
                        if (i + 1 < n) response.put(",");
                    }
                    response.put("],\"failed\":[{\"path\":\"").put(state.currentFilename)
                            .put("\",\"error\":\"").put(errorMsg)
                            .put("\",\"code\":").put(errorCode)
                            .put("}]}");
                    response.sendChunk(true);
                    response.done();
                } else {
                    LOG.error().$("POST /imports failed [status=").$(errorCode).$(", fd=").$(context.getFd()).$(", msg=").$(errorMsg).I$();
                    response.status(errorCode, errorMsg);
                    response.sendHeader();
                    response.sendChunk(false);
                    response.put("{\"errors\":[{\"status\":\"").put(errorCode)
                            .put("\",\"detail\":\"").put(errorMsg)
                            .put("\",\"name\":\"").put(state.currentFilename).put("\"}]}");
                    response.sendChunk(true);
                    response.shutdownWrite();
                    throw ServerDisconnectException.INSTANCE;
                }
            } finally {
                state.clear();
            }
        }

        private void sendSuccess(HttpConnectionContext context) throws PeerIsSlowToReadException, PeerDisconnectedException, ServerDisconnectException {
            try {
                HttpChunkedResponse response = context.getChunkedResponse();
                response.bookmark();
                response.status(200, CONTENT_TYPE_JSON_API);
                response.sendHeader();
                response.sendChunk(false);
                response.put("{\"successful\":[");
                for (int i = 0, n = state.successfulFiles.size(); i < n; i++) {
                    response.put("\"").put(state.successfulFiles.getQuick(i)).put("\"");
                    if (i + 1 < n) response.put(",");
                }
                response.put("]}");
                response.sendChunk(true);
                response.done();
            } finally {
                state.clear();
            }
        }

        public static class State implements Mutable, Closeable {
            final LongSupplier importIDSupplier;
            final Path realPath;
            final Path tempPath;
            String currentFilename;
            int errorCode;
            Utf8StringSink errorMsg;
            long fd = -1;
            FilesFacade ff;
            long hi;
            StringSink importId = new StringSink();
            long lo;
            boolean overwrite;
            ObjList<CharSequence> successfulFiles;
            int tempPathBaseLen;
            long written;

            public State(FilesFacade ff, LongSupplier importIDSupplier) {
                this.ff = ff;
                this.importIDSupplier = importIDSupplier;
                tempPath = new Path();
                realPath = new Path();
                successfulFiles = new ObjList<>();
                errorMsg = new Utf8StringSink();
            }

            public void clear() {
                written = 0;
                errorCode = -1;
                errorMsg.clear();
                successfulFiles.clear();
                currentFilename = null;
                if (fd != -1) {
                    Files.close(fd);
                    fd = -1;
                }
                importId.clear();
                if (tempPathBaseLen > 0 && tempPath.size() > tempPathBaseLen) {
                    tempPath.trimTo(tempPathBaseLen);
                    ff.rmdir(tempPath, false);
                    tempPathBaseLen = 0;
                }
            }

            @Override
            public void close() {
                clear();
                Misc.free(tempPath);
                Misc.free(realPath);
            }

            public void of(DirectUtf8Sequence filename, CharSequence importRoot) {
                // clear prev
                if (fd != -1) {
                    Files.close(fd);
                    fd = -1;
                }
                if (importId.isEmpty()) {
                    long id = importIDSupplier.getAsLong();
                    Numbers.appendHex(importId, id, true);
                }
                written = 0;
                tempPath.of(importRoot).concat(importId);
                tempPathBaseLen = tempPath.size();
                tempPath.concat(filename);
                realPath.of(importRoot).concat(filename);
            }

            public void of(long lo, long hi) {
                this.lo = lo;
                this.hi = hi;
            }
        }
    }
}
