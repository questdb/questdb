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
import io.questdb.cutlass.http.HttpChunkedResponse;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.cutlass.http.LocalValue;
import io.questdb.cutlass.http.processors.JsonQueryProcessorConfiguration;
import io.questdb.griffin.JsonSink;
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
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;

import java.io.Closeable;

import static io.questdb.cutlass.http.HttpConstants.*;
import static io.questdb.cutlass.http.HttpRequestValidator.METHOD_HEAD;
import static java.net.HttpURLConnection.HTTP_OK;

public class FileMetadataProcessor implements HttpRequestProcessor {
    private static final Log LOG = LogFactory.getLog(FileMetadataProcessor.class);
    private static final LocalValue<FileMetadataState> LV = new LocalValue<>();
    private final CairoEngine engine;
    private final FilesRootDir filesRoot;
    private final byte requiredAuthType;

    public FileMetadataProcessor(CairoEngine cairoEngine, JsonQueryProcessorConfiguration configuration, FilesRootDir root) {
        engine = cairoEngine;
        requiredAuthType = configuration.getRequiredAuthType();
        this.filesRoot = root;
    }

    @Override
    public byte getRequiredAuthType() {
        return requiredAuthType;
    }

    @Override
    public short getSupportedRequestTypes() {
        return METHOD_HEAD;
    }

    @Override
    public void onRequestComplete(
            HttpConnectionContext context
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        FileMetadataState state = LV.get(context);
        if (state == null) {
            LV.set(context, state = new FileMetadataState(engine.getConfiguration().getFilesFacade()));
        }

        HttpChunkedResponse response = context.getChunkedResponse();
        CharSequence root = FilesRootDir.getRootPath(filesRoot, engine.getConfiguration());
        if (Chars.isBlank(root)) {
            StringSink sink = Misc.getThreadLocalSink();
            sink.put(filesRoot.getConfigName()).put(" is not configured");
            sendException(400, response, sink, state);
            return;
        }

        HttpRequestHeader request = context.getRequestHeader();
        final DirectUtf8Sequence file = FileGetProcessorHelper.extractFilePathFromUrl(request, filesRoot);
        if (file == null || file.size() == 0) {
            sendException(400, response, "file path is required", state);
            return;
        }

        if (FileGetProcessorHelper.containsAbsOrRelativePath(file)) {
            sendException(403, response, "path traversal not allowed in file", state);
            return;
        }
        state.file = file;
        state.contentType = FileGetProcessorHelper.getContentType(file);
        sendMetadata(context, root, state);
    }

    @Override
    public void parkRequest(HttpConnectionContext context, boolean paused) {
        FileMetadataState state = LV.get(context);
        if (state != null) {
            state.paused = paused;
        }
    }

    @Override
    public void resumeSend(
            HttpConnectionContext context
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        try {
            FileMetadataState state = LV.get(context);
            if (state != null) {
                if (!state.paused) {
                    context.resumeResponseSend();
                } else {
                    state.paused = false;
                }
                sendMetadata(context, FilesRootDir.getRootPath(filesRoot, engine.getConfiguration()), state);
            }
        } catch (CairoError | CairoException e) {
            throw ServerDisconnectException.INSTANCE;
        }
    }

    private void sendMetadata(
            HttpConnectionContext context,
            CharSequence root,
            FileMetadataState state
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        if (!state.paused) {
            context.resumeResponseSend();
        } else {
            state.paused = false;
        }

        final HttpChunkedResponse response = context.getChunkedResponse();
        Path path = Path.getThreadLocal(root);
        path.concat(state.file);

        if (!state.ff.exists(path.$())) {
            sendException(404, response, "file not found", state);
            return;
        }

        if (state.ff.isDirOrSoftLinkDir(path.$())) {
            sendException(400, response, "cannot access directory", state);
            return;
        }

        long fd = state.ff.openRO(path.$());
        if (fd < 0) {
            sendException(404, response, "file not found", state);
            return;
        }

        try {
            long fileSize = state.ff.length(fd);
            long lastModified = state.ff.getLastModified(path.$());

            // Send headers with metadata
            response.status(HTTP_OK, state.contentType);
            response.headers()
                    .putAscii("Content-Disposition: attachment; filename=\"").put(state.file).putAscii("\"").putEOL()
                    .putAscii("Content-Length: ").put(fileSize).putEOL()
                    .putAscii("Last-Modified: ");

            DateFormatUtils.formatHTTP(response.headers(), lastModified);
            response.headers().putEOL();
            response.sendHeader();
            response.done();
        } finally {
            state.ff.close(fd);
        }
    }

    private void sendException(
            int errorCode,
            HttpChunkedResponse response,
            CharSequence message,
            FileMetadataState state
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        response.status(errorCode, CONTENT_TYPE_JSON_API);
        response.sendHeader();
        JsonSink sink = Misc.getThreadLocalJsonSink();
        sink.of(response).startObject()
                .key("errors").startArray().startObject()
                .key("status").valQuoted(errorCode)
                .key("detail").val(message)
                .endObject().endArray().endObject();
        response.sendChunk(true);
    }

    public static class FileMetadataState implements Mutable, Closeable {
        CharSequence contentType;
        DirectUtf8Sequence file;
        FilesFacade ff;
        boolean paused;

        public FileMetadataState(FilesFacade ff) {
            this.ff = ff;
        }

        @Override
        public void clear() {
            paused = false;
            file = null;
            contentType = null;
        }

        @Override
        public void close() {
            clear();
        }
    }
}
