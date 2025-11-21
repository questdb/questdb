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
import io.questdb.cairo.TableUtils;
import io.questdb.cutlass.http.HttpChunkedResponse;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.cutlass.http.LocalValue;
import io.questdb.cutlass.http.processors.JsonQueryProcessorConfiguration;
import io.questdb.griffin.JsonSink;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.NoSpaceLeftInResponseBufferException;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.ServerDisconnectException;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;

import java.io.Closeable;

import static io.questdb.cutlass.http.HttpConstants.*;
import static io.questdb.cutlass.http.HttpRequestValidator.METHOD_GET;
import static java.net.HttpURLConnection.HTTP_OK;

public class FileDownloadProcessor implements HttpRequestProcessor {
    static final int FILE_SEND_INIT = 0;
    static final int FILE_SEND_CHUNK = FILE_SEND_INIT + 1;
    static final int FILE_SEND_COMPLETED = FILE_SEND_CHUNK + 1;
    private static final Log LOG = LogFactory.getLog(FileDownloadProcessor.class);
    private static final LocalValue<FileDownloadState> LV = new LocalValue<>();
    private final CairoEngine engine;
    private final FilesRootDir filesRoot;
    private final byte requiredAuthType;

    public FileDownloadProcessor(CairoEngine cairoEngine, JsonQueryProcessorConfiguration configuration, FilesRootDir root) {
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
        return METHOD_GET;
    }

    @Override
    public void onRequestComplete(
            HttpConnectionContext context
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        FileDownloadState state = LV.get(context);
        if (state == null) {
            LV.set(context, state = new FileDownloadState(engine.getConfiguration().getFilesFacade()));
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
        doResumeSend(context, state);
    }

    @Override
    public void parkRequest(HttpConnectionContext context, boolean paused) {
        FileDownloadState state = LV.get(context);
        if (state != null) {
            state.paused = paused;
        }
    }

    @Override
    public void resumeSend(
            HttpConnectionContext context
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        try {
            FileDownloadState state = LV.get(context);
            if (state != null) {
                if (!state.paused) {
                    context.resumeResponseSend();
                } else {
                    state.paused = false;
                }
                doResumeSend(context, state);
            }
        } catch (CairoError | CairoException e) {
            throw ServerDisconnectException.INSTANCE;
        }
    }

    private void doResumeSend(
            HttpConnectionContext context,
            FileDownloadState state
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        if (!state.paused) {
            context.resumeResponseSend();
        } else {
            state.paused = false;
        }
        final HttpChunkedResponse response = context.getChunkedResponse();
        OUT:
        while (true) {
            try {
                switch (state.state) {
                    case FILE_SEND_INIT:
                        initFileSending(response, state);
                        break;
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
            } catch (CairoException e) {
                sendException(500, response, e.getFlyweightMessage(), state);
            }
        }
    }

    private void header(
            HttpChunkedResponse response,
            FileDownloadState state
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        response.status(200, state.contentType);
        response.headers()
                .putAscii("Content-Disposition: attachment; filename=\"").put(state.file).putAscii("\"").putEOL()
                .putAscii("Content-Length: ").put(state.fileSize).putEOL()
                .putAscii("Last-Modified: ");

        DateFormatUtils.formatHTTP(response.headers(), state.lastModified);
        response.headers().putEOL();
        response.sendHeader();
    }

    private void headerJsonError(int errorCode, HttpChunkedResponse response) throws PeerDisconnectedException, PeerIsSlowToReadException {
        response.status(errorCode, CONTENT_TYPE_JSON_API);
        response.sendHeader();
    }

    private void initFileSending(HttpChunkedResponse response, FileDownloadState state) throws PeerDisconnectedException, PeerIsSlowToReadException {
        Path path = Path.getThreadLocal(FilesRootDir.getRootPath(filesRoot, engine.getConfiguration()));
        path.concat(state.file);
        if (!state.ff.exists(path.$())) {
            sendException(404, response, "file not found", state);
            state.state = FILE_SEND_COMPLETED;
            return;
        }
        if (state.ff.isDirOrSoftLinkDir(path.$())) {
            sendException(400, response, "cannot download directory", state);
            state.state = FILE_SEND_COMPLETED;
            return;
        }
        state.fd = state.ff.openRO(path.$());
        if (state.fd < 0) {
            sendException(404, response, "file not found", state);
            state.state = FILE_SEND_COMPLETED;
            return;
        }

        state.fileSize = state.ff.length(state.fd);
        state.lastModified = state.ff.getLastModified(path.$());
        if (state.fileSize > 0) {
            state.fileAddress = TableUtils.mapRO(state.ff, state.fd, state.fileSize, MemoryTag.MMAP_DEFAULT);
            if (state.fileAddress == 0) {
                sendException(500, response, "failed to memory-map file", state);
                state.state = FILE_SEND_COMPLETED;
                return;
            }
        }

        header(response, state);
        state.state = FILE_SEND_CHUNK;
    }

    private void sendException(
            int errorCode,
            HttpChunkedResponse response,
            CharSequence message,
            FileDownloadState state
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        if (state.fileOffset > 0) {
            LOG.error().$("partial file response sent, closing connection on error [fd=").$(state.getFd())
                    .$(", fileOffset=").$(state.fileOffset)
                    .$(", errorMessage=").$safe(message)
                    .I$();
            throw PeerDisconnectedException.INSTANCE;
        }
        headerJsonError(errorCode, response);
        JsonSink sink = Misc.getThreadLocalJsonSink();
        sink.of(response).startObject()
                .key("errors").startArray().startObject()
                .key("status").valQuoted(errorCode)
                .key("detail").val(message)
                .endObject().endArray().endObject();
        response.sendChunk(true);
    }

    private void sendFileChunk(HttpChunkedResponse response, FileDownloadState state) throws PeerIsSlowToReadException, PeerDisconnectedException {
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

    public static class FileDownloadState implements Mutable, Closeable {
        CharSequence contentType;
        long fd = -1;
        FilesFacade ff;
        DirectUtf8Sequence file;
        long fileAddress = 0;
        long fileOffset;
        long fileSize;
        long lastModified;
        boolean paused;
        int state;

        public FileDownloadState(FilesFacade ff) {
            this.ff = ff;
        }

        @Override
        public void clear() {
            try {
                if (fileAddress != 0) {
                    ff.munmap(fileAddress, fileSize, MemoryTag.MMAP_DEFAULT);
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

            paused = false;
            lastModified = 0;
            fileSize = 0;
            fileOffset = 0;
            state = FILE_SEND_INIT;
            file = null;
            contentType = null;
        }

        @Override
        public void close() {
            clear();
        }

        public long getFd() {
            return fd;
        }
    }
}
