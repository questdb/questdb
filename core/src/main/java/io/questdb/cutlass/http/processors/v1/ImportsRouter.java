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
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.MicrosTimestampDriver;
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
import io.questdb.griffin.engine.functions.str.SizePrettyFunctionFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.NoSpaceLeftInResponseBufferException;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.ServerDisconnectException;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntStack;
import io.questdb.std.LongStack;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sink;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.LongSupplier;

import static io.questdb.cutlass.http.HttpConstants.*;
import static io.questdb.cutlass.http.HttpResponseSink.HTTP_MULTI_STATUS;
import static java.net.HttpURLConnection.HTTP_OK;

public class ImportsRouter implements HttpRequestHandler {
    private final HttpRequestProcessor getProcessor;
    private final HttpMultipartContentProcessor postProcessor;

    public ImportsRouter(CairoEngine cairoEngine, JsonQueryProcessorConfiguration configuration) {
        postProcessor = new ImportPostProcessor(cairoEngine, configuration);
        getProcessor = new ImportGetProcessor(cairoEngine, configuration);
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
            return getProcessor;
        }
        return null;
    }

    public static class ImportGetProcessor implements HttpRequestProcessor {
        static final int FILE_SEND_INIT = 0;
        static final int FILE_SEND_CHUNK = FILE_SEND_INIT + 1;
        static final int FILE_SEND_COMPLETED = FILE_SEND_CHUNK + 1;
        private static final Log LOG = LogFactory.getLog(ImportGetProcessor.class);
        private static final LocalValue<State> LV = new LocalValue<>();
        CairoEngine engine;
        FilesFacade ff;
        byte requiredAuthType;

        public ImportGetProcessor(CairoEngine cairoEngine, JsonQueryProcessorConfiguration configuration) {
            engine = cairoEngine;
            requiredAuthType = configuration.getRequiredAuthType();
            ff = cairoEngine.getConfiguration().getFilesFacade();
        }

        @Override
        public byte getRequiredAuthType() {
            return requiredAuthType;
        }

        @Override
        public void onRequestComplete(
                HttpConnectionContext context
        ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
            State state = LV.get(context);
            if (state == null) {
                LV.set(context, state = new State(engine.getConfiguration().getFilesFacade()));
            }
            HttpChunkedResponse response = context.getChunkedResponse();
            CharSequence importRoot = engine.getConfiguration().getSqlCopyInputRoot();
            if (Chars.isBlank(importRoot)) {
                sendException(503, response, "service unavailable: sql.copy.input.root is not configured", state);
                return;
            }

            HttpRequestHeader request = context.getRequestHeader();
            final DirectUtf8Sequence file = request.getUrlParam(URL_PARAM_FILENAME);

            if (file == null || file.size() == 0) {
                // No filename parameter - list all files in import directory
                sendFileList(context, importRoot, state);
                return;
            }

            // Filename provided - download specific file
            if (containsAbsOrRelativePath(file)) {
                sendException(403, response, "forbidden: path traversal not allowed in filename", state);
                return;
            }
            state.file = file;
            state.contentType = getContentType(file);
            doResumeSend(context, state);
        }

        @Override
        public void resumeSend(
                HttpConnectionContext context
        ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
            try {
                State state = LV.get(context);
                if (state != null) {
                    if (state.file == null || state.file.size() == 0) {
                        context.simpleResponse().sendStatusJsonContent(HTTP_OK, state.sink, false);
                    } else {
                        doResumeSend(context, state);
                    }
                }
            } catch (CairoError | CairoException e) {
                throw ServerDisconnectException.INSTANCE;
            }
        }

        private boolean containsAbsOrRelativePath(DirectUtf8Sequence filename) {
            if (Utf8s.startsWithAscii(filename, "/") || Utf8s.startsWithAscii(filename, "\\")) {
                return true;
            }
            return Utf8s.containsAscii(filename, "../") || Utf8s.containsAscii(filename, "..\\");
        }

        private void doResumeSend(
                HttpConnectionContext context,
                State state
        ) throws PeerDisconnectedException, PeerIsSlowToReadException {
            final HttpChunkedResponse response = context.getChunkedResponse();
            OUT:
            while (true) {
                try {
                    switch (state.state) {
                        case FILE_SEND_INIT:
                            initFileSending(response, state);
                            state.state = FILE_SEND_CHUNK;
                            // fall through
                        case FILE_SEND_CHUNK:
                            sendFileChunk(response, state);
                            if (state.fileOffset >= state.fileSize) {
                                state.state = FILE_SEND_COMPLETED;
                            }
                            break;
                        case FILE_SEND_COMPLETED:
                            response.done();
                            break OUT;
                        default:
                            break OUT;
                    }
                } catch (NoSpaceLeftInResponseBufferException ignored) {
                    if (response.resetToBookmark()) {
                        response.sendChunk(false);
                    } else {
                        LOG.info().$("Response buffer is too small").$();
                        throw PeerDisconnectedException.INSTANCE;
                    }
                }
            }
            response.done();
        }

        private String getContentType(DirectUtf8Sequence filename) {
            if (Utf8s.endsWithAscii(filename, ".parquet")) {
                return CONTENT_TYPE_PARQUET;
            } else if (Utf8s.endsWithAscii(filename, ".csv")) {
                return CONTENT_TYPE_CSV;
            } else if (Utf8s.endsWithAscii(filename, ".json")) {
                return CONTENT_TYPE_JSON;
            } else if (Utf8s.endsWithAscii(filename, ".txt")) {
                return CONTENT_TYPE_TEXT;
            }
            return CONTENT_TYPE_OCTET_STREAM;
        }

        private void header(
                HttpChunkedResponse response,
                State state
        ) throws PeerDisconnectedException, PeerIsSlowToReadException {
            response.status(200, state.contentType);
            response.headers().putAscii("Content-Disposition: attachment; filename=\"").put(state.file).putAscii("\"").putEOL();
            response.sendHeader();
        }

        private void headerJsonError(int errorCode, HttpChunkedResponse response) throws PeerDisconnectedException, PeerIsSlowToReadException {
            response.status(errorCode, CONTENT_TYPE_JSON);
            response.sendHeader();
        }

        private void initFileSending(HttpChunkedResponse response, State state) throws PeerDisconnectedException, PeerIsSlowToReadException {
            Path path = Path.getThreadLocal(engine.getConfiguration().getSqlCopyInputRoot());
            path.concat(state.file);
            state.fd = state.ff.openRO(path.$());
            if (state.fd < 0) {
                sendException(404, response, "file not found in import files", state);
                return;
            }

            state.fileSize = state.ff.length(state.fd);
            if (state.fileSize > 0) {
                state.fileAddress = TableUtils.mapRO(state.ff, state.fd, state.fileSize, MemoryTag.MMAP_IMPORT);
                if (state.fileAddress == 0) {
                    sendException(500, response, "failed to memory-map file", state);
                }
            } else {
                sendException(400, response, "unprocessable entity: file is empty", state);
                return;
            }

            header(response, state);
        }

        private void scanDirectory(
                CharSequence rootPath,
                Utf8Sink sink,
                State state
        ) {
            int rootLen = rootPath.length();
            Path path = Path.getThreadLocal(rootPath);
            Utf8StringSink tempSink = Misc.getThreadLocalUtf8Sink();
            state.findStack.clear();
            state.pathLenStack.clear();
            long pFind = state.ff.findFirst(path.$());
            if (pFind == -1) {
                return;
            }

            try {
                while (true) {
                    // Handle empty find pointer - pop from stack
                    while (pFind <= 0 && state.findStack.notEmpty()) {
                        pFind = state.findStack.pop();
                        int pathLen = state.pathLenStack.pop();
                        path.trimTo(pathLen);
                    }
                    if (pFind <= 0) {
                        break;
                    }

                    long pUtf8NameZ = state.ff.findName(pFind);
                    if (pUtf8NameZ == 0) {
                        if (state.ff.findNext(pFind) <= 0) {
                            state.ff.findClose(pFind);
                            pFind = 0;
                        }
                        continue;
                    }

                    int type = state.ff.findType(pFind);
                    tempSink.clear();
                    Utf8s.utf8ZCopy(pUtf8NameZ, tempSink);

                    // Skip "." and ".." directories
                    if (!Files.notDots(tempSink)) {
                        if (state.ff.findNext(pFind) <= 0) {
                            state.ff.findClose(pFind);
                            pFind = 0;
                        }
                        continue;
                    }

                    if (type == Files.DT_DIR) {
                        // Save current findPtr to stack if there are more entries
                        if (state.ff.findNext(pFind) > 0) {
                            state.findStack.push(pFind);
                            state.pathLenStack.push(path.size());
                        } else {
                            state.ff.findClose(pFind);
                        }
                        // Enter subdirectory
                        path.concat(tempSink).slash();
                        pFind = state.ff.findFirst(path.$());
                        continue;
                    } else if (type == Files.DT_FILE) {
                        // Output file information
                        if (!state.firstFile) {
                            sink.put(',');
                        }
                        state.firstFile = false;
                        sink.put('{');
                        sink.putAsciiQuoted("path").put(':').putQuote();

                        // Build relative path
                        if (path.size() > rootLen) {
                            Utf8s.utf8ZCopyEscaped(path.ptr() + rootLen + 1, path.end(), sink);
                        }
                        Utf8s.utf8ZCopyEscaped(pUtf8NameZ, sink);
                        sink.putQuote().put(',');
                        sink.putAsciiQuoted("name").put(':').putQuote();
                        Utf8s.utf8ZCopyEscaped(pUtf8NameZ, sink);
                        sink.putQuote().put(',');
                        // Get file information
                        int oldLen = path.size();
                        path.concat(tempSink);
                        long fileSize = state.ff.length(path.$());
                        long lastModified = state.ff.getLastModified(path.$());
                        path.trimTo(oldLen);
                        sink.putAsciiQuoted("size").put(':').putQuote();
                        SizePrettyFunctionFactory.toSizePretty(sink, fileSize);
                        sink.putQuote().put(',');
                        sink.putAsciiQuoted("lastModified").put(':').putQuote();
                        MicrosTimestampDriver.INSTANCE.append(sink, lastModified);
                        sink.putQuote();
                        sink.put('}');
                    }

                    // Move to next entry
                    if (state.ff.findNext(pFind) <= 0) {
                        state.ff.findClose(pFind);
                        pFind = 0;
                    }
                }
            } finally {
                // Clean up any remaining find handles
                if (pFind > 0) {
                    state.ff.findClose(pFind);
                }
                while (state.findStack.notEmpty()) {
                    state.ff.findClose(state.findStack.pop());
                    state.pathLenStack.pop();
                }
            }
        }

        private void sendException(
                int errorCode,
                HttpChunkedResponse response,
                CharSequence message,
                State state
        ) throws PeerDisconnectedException, PeerIsSlowToReadException {
            if (state.fileOffset > 0) {
                LOG.error().$("partial file response sent, closing connection on error [fd=").$(state.getFd())
                        .$(", fileOffset=").$(state.fileOffset)
                        .$(", errorMessage=").$safe(message)
                        .I$();
                throw PeerDisconnectedException.INSTANCE;
            }
            headerJsonError(errorCode, response);
            response.putAscii('{')
                    .putAsciiQuoted("error").putAscii(':').putQuote().escapeJsonStr(message).putQuote()
                    .putAscii('}');
            response.sendChunk(true);
        }

        private void sendFileChunk(HttpChunkedResponse response, State state) throws PeerIsSlowToReadException, PeerDisconnectedException {
            if (state.fileOffset >= state.fileSize) {
                return;
            }

            long remainingSize = state.fileSize - state.fileOffset;
            int sendLSize = (int) Math.min(Integer.MAX_VALUE, remainingSize);
            long bytesWritten = response.writeBytes(state.fileAddress + state.fileOffset, sendLSize);
            state.fileOffset += bytesWritten;
            response.bookmark();
            if (state.fileOffset < state.fileSize) {
                response.sendChunk(false);
            }
        }

        private void sendFileList(
                HttpConnectionContext context,
                CharSequence importRoot,
                State state
        ) throws PeerDisconnectedException, PeerIsSlowToReadException {
            Utf8StringSink listSink = state.sink;
            listSink.put('[');
            try {
                scanDirectory(importRoot, listSink, state);
                listSink.put(']');
                context.simpleResponse().sendStatusJsonContent(HTTP_OK, listSink, false);
            } catch (CairoException e) {
                LOG.error().$("failed to list import files: ").$(e.getFlyweightMessage()).I$();
                HttpChunkedResponse response = context.getChunkedResponse();
                StringSink sink = Misc.getThreadLocalSink();
                sink.put("failed to list import contents, error: ").put(e.getFlyweightMessage());
                sendException(500, response, sink, state);
            } catch (Throwable e) {
                LOG.error().$("failed to list import files: ").$(e).I$();
                HttpChunkedResponse response = context.getChunkedResponse();
                StringSink sink = Misc.getThreadLocalSink();
                sink.put("failed to list import contents, error: ").put(e.getMessage());
                sendException(500, response, sink, state);
            }
        }

        public static class State implements Mutable, Closeable {
            final LongStack findStack = new LongStack();
            final IntStack pathLenStack = new IntStack();
            CharSequence contentType;
            HttpConnectionContext context;
            long fd = -1;
            FilesFacade ff;
            DirectUtf8Sequence file;
            long fileAddress = 0;
            long fileOffset;
            long fileSize;
            boolean firstFile = true;
            Utf8StringSink sink = new Utf8StringSink();
            int state;

            State(FilesFacade ff) {
                this.ff = ff;
            }

            @Override
            public void clear() {
                try {
                    if (fileAddress != 0) {
                        ff.munmap(fileAddress, fileSize, MemoryTag.MMAP_IMPORT);
                    }
                } catch (Exception e) {
                    LOG.error().$("failed to unmap memory [fileAddress=").$(fileAddress).$("]").$();
                } finally {
                    fileAddress = 0;
                }

                try {
                    if (fd != -1) {
                        ff.close(fd);
                    }
                } catch (Exception e) {
                    LOG.error().$("failed to close file descriptor [fd=").$(fd).$("]").$();
                } finally {
                    fd = -1;
                }

                fileSize = 0;
                fileOffset = 0;
                state = FILE_SEND_INIT;
                file = null;
                contentType = null;
                findStack.clear();
                pathLenStack.clear();
                firstFile = true;
                sink.clear();
            }

            @Override
            public void close() throws IOException {
            }

            public long getFd() {
                return context.getFd();
            }
        }
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
