package io.questdb.cutlass.http.processors.v1;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.MillsTimestampDriver;
import io.questdb.cairo.TableUtils;
import io.questdb.cutlass.http.HttpChunkedResponse;
import io.questdb.cutlass.http.HttpConnectionContext;
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
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sink;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;

import java.io.Closeable;

import static io.questdb.cutlass.http.HttpConstants.*;
import static java.net.HttpURLConnection.HTTP_OK;

public class FileGetProcessor implements HttpRequestProcessor {
    static final int FILE_SEND_INIT = 0;
    static final int FILE_SEND_CHUNK = FILE_SEND_INIT + 1;
    static final int FILE_SEND_COMPLETED = FILE_SEND_CHUNK + 1;
    private static final Log LOG = LogFactory.getLog(FileGetProcessor.class);
    private static final LocalValue<State> LV = new LocalValue<>();
    private final CairoEngine engine;
    private final FilesRootDir filesRoot;
    private final byte requiredAuthType;

    public FileGetProcessor(CairoEngine cairoEngine, JsonQueryProcessorConfiguration configuration, FilesRootDir root) {
        engine = cairoEngine;
        requiredAuthType = configuration.getRequiredAuthType();
        this.filesRoot = root;
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
        CharSequence root = FilesRootDir.getRootPath(filesRoot, engine.getConfiguration());
        if (Chars.isBlank(root)) {
            StringSink sink = Misc.getThreadLocalSink();
            sink.put(filesRoot.getConfigName()).put(" is not configured");
            sendException(400, response, sink, state);
            return;
        }

        HttpRequestHeader request = context.getRequestHeader();
        final DirectUtf8Sequence file = request.getUrlParam(URL_PARAM_FILE);
        if (file == null || file.size() == 0) {
            // No file parameter - list all files in root directory
            sendFileList(context, root, state);
            return;
        }

        // File provided - download specific file
        if (containsAbsOrRelativePath(file)) {
            sendException(403, response, "path traversal not allowed in file", state);
            return;
        }
        state.file = file;
        state.contentType = getContentType(file);
        doResumeSend(context, state);
    }

    @Override
    public void parkRequest(HttpConnectionContext context, boolean paused) {
        State state = LV.get(context);
        if (state != null) {
            state.paused = paused;
        }
    }

    @Override
    public void resumeSend(
            HttpConnectionContext context
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        try {
            State state = LV.get(context);
            if (state != null) {
                if (!state.paused) {
                    context.resumeResponseSend();
                } else {
                    state.paused = false;
                }
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

    private void doResumeSend(
            HttpConnectionContext context,
            State state
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

                if (!Files.notDots(tempSink)) {
                    if (state.ff.findNext(pFind) <= 0) {
                        state.ff.findClose(pFind);
                        pFind = 0;
                    }
                    continue;
                }

                if (type == Files.DT_DIR) {
                    if (state.ff.findNext(pFind) > 0) {
                        state.findStack.push(pFind);
                        state.pathLenStack.push(path.size());
                    } else {
                        state.ff.findClose(pFind);
                    }
                    path.concat(tempSink).slash();
                    pFind = state.ff.findFirst(path.$());
                    continue;
                } else if (type == Files.DT_FILE) {
                    if (!state.firstFile) {
                        sink.put(',');
                    }
                    state.firstFile = false;
                    sink.put('{');
                    sink.putAsciiQuoted("path").put(':').putQuote();

                    if (path.size() > rootLen) {
                        Utf8s.utf8ZCopyEscaped(path.ptr() + rootLen + 1, path.end(), sink);
                    }
                    Utf8s.utf8ZCopyEscaped(pUtf8NameZ, sink);
                    sink.putQuote().put(',');
                    sink.putAsciiQuoted("name").put(':').putQuote();
                    Utf8s.utf8ZCopyEscaped(pUtf8NameZ, sink);
                    sink.putQuote().put(',');
                    int oldLen = path.size();
                    path.concat(tempSink);
                    long fileSize = state.ff.length(path.$());
                    long lastModified = state.ff.getLastModified(path.$());
                    path.trimTo(oldLen);
                    sink.putAsciiQuoted("size").put(':').putQuote();
                    SizePrettyFunctionFactory.toSizePretty(sink, fileSize);
                    sink.putQuote().put(',');
                    sink.putAsciiQuoted("lastModified").put(':').putQuote();
                    MillsTimestampDriver.INSTANCE.append(sink, lastModified);
                    sink.putQuote();
                    sink.put('}');
                }

                if (state.ff.findNext(pFind) <= 0) {
                    state.ff.findClose(pFind);
                    pFind = 0;
                }
            }
        } finally {
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
        response.putAscii("{\"error\":\"").putAscii(message).putAscii("\"}");
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
            CharSequence root,
            State state
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        Utf8StringSink listSink = state.sink;
        listSink.put('[');
        try {
            scanDirectory(root, listSink, state);
            listSink.put(']');
            context.simpleResponse().sendStatusJsonContent(HTTP_OK, listSink, false);
        } catch (CairoException e) {
            LOG.error().$("failed to list files: ").$(e.getFlyweightMessage()).I$();
            HttpChunkedResponse response = context.getChunkedResponse();
            StringSink sink = Misc.getThreadLocalSink();
            sink.put("failed to list files, error: ").put(e.getFlyweightMessage());
            sendException(500, response, sink, state);
        } catch (Throwable e) {
            LOG.error().$("failed to list files: ").$(e).I$();
            HttpChunkedResponse response = context.getChunkedResponse();
            StringSink sink = Misc.getThreadLocalSink();
            sink.put("failed to list files, error: ").put(e.getMessage());
            sendException(500, response, sink, state);
        }
    }

    static boolean containsAbsOrRelativePath(DirectUtf8Sequence filename) {
        if (filename.byteAt(0) == Files.SEPARATOR) {
            return true;
        }
        return Utf8s.containsAscii(filename, "../") || Utf8s.containsAscii(filename, "..\\");
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
        boolean paused;
        Utf8StringSink sink = new Utf8StringSink();
        int state;

        State(FilesFacade ff) {
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
        public void close() {
            clear();
        }

        public long getFd() {
            return context.getFd();
        }
    }
}