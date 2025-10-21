package io.questdb.cutlass.http.processors.v1;

import io.questdb.cairo.CairoEngine;
import io.questdb.cutlass.http.HttpChunkedResponse;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpKeywords;
import io.questdb.cutlass.http.HttpMultipartContentProcessor;
import io.questdb.cutlass.http.HttpRequestHeader;
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
import io.questdb.std.Os;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8StringSink;

import java.io.Closeable;
import java.util.function.LongSupplier;

import static io.questdb.cutlass.http.HttpConstants.CONTENT_TYPE_JSON;
import static io.questdb.cutlass.http.HttpConstants.URL_PARAM_OVERWRITE;
import static io.questdb.cutlass.http.HttpResponseSink.HTTP_MULTI_STATUS;
import static io.questdb.cutlass.http.processors.v1.FileGetProcessor.containsAbsOrRelativePath;

/**
 * multiple form/data requests can come in to upload multiple files at once
 */
public class FileUploadProcessor implements HttpMultipartContentProcessor {
    private static final Log LOG = LogFactory.getLog(FileUploadProcessor.class);
    private static final LocalValue<State> LV = new LocalValue<>();
    private final CairoEngine engine;
    private final FilesRootDir filesRoot;
    private final byte requiredAuthType;
    private HttpConnectionContext context;
    private State state;


    /**
     * Handles basic file upload (mainly for parquet, csv).
     * Returns:
     * 201 if successful with no location header and stored filenames in body
     * 409 if there's a write conflict
     * 400 if input root or filename are improperly set
     * 422 if the file ends with .parquet and it cannot be decoded
     * 500 for other errors
     */
    public FileUploadProcessor(CairoEngine cairoEngine, JsonQueryProcessorConfiguration configuration, FilesRootDir filesRoot) {
        this.engine = cairoEngine;
        this.requiredAuthType = configuration.getRequiredAuthType();
        this.filesRoot = filesRoot;
    }

    @Override
    public byte getRequiredAuthType() {
        return requiredAuthType;
    }

    @Override
    public void onChunk(long lo, long hi) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        if (hi > lo) {
            state.of(lo, hi);
            FilesFacade ff = state.ff;

            // on first entry, open file for writing
            if (state.fd == -1) {
                if (ff.mkdirs(state.tempPath, engine.getConfiguration().getMkDirMode()) != 0) {
                    StringSink sink = Misc.getThreadLocalSink();
                    sink.put("could not create directories [path=").put(state.tempPath).put(", errno=").put(Os.errno()).put(']');
                    sendErrorWithSuccessInfo(context, 500, sink);
                    return;
                }
                state.fd = ff.openRW(state.tempPath.$(), engine.getConfiguration().getWriterFileOpenOpts());
                if (state.fd == -1) {
                    StringSink sink = Misc.getThreadLocalSink();
                    sink.put("cannot open file for writing [path=").put(state.tempPath).put(']');
                    sendErrorWithSuccessInfo(context, 500, sink);
                    return;
                }
                state.written = 0;
            }

            final long chunkSize = hi - lo;
            long bytesWritten = ff.write(state.fd, lo, chunkSize, state.written);
            if (bytesWritten != chunkSize) {
                StringSink sink = Misc.getThreadLocalSink();
                sink.put("failed to write chunk [expected=").put(chunkSize).put(']').put(", actual=").put(bytesWritten).put(']');
                sendErrorWithSuccessInfo(context, 500, sink);
                return;
            }
            state.written += chunkSize;
        }
    }

    @Override
    public void onPartBegin(HttpRequestHeader partHeader) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        CharSequence root = FilesRootDir.getRootPath(filesRoot, engine.getConfiguration());
        if (Chars.isBlank(root)) {
            StringSink sink = Misc.getThreadLocalSink();
            sink.put(filesRoot.getConfigName()).put(" is not configured");
            sendErrorWithSuccessInfo(context, 400, sink);
            return;
        }

        final DirectUtf8Sequence file = partHeader.getContentDispositionName();
        if (file == null || file.size() == 0) {
            sendErrorWithSuccessInfo(context, 400, "received Content-Disposition without a filename");
            return;
        }

        if (containsAbsOrRelativePath(file)) {
            sendErrorWithSuccessInfo(context, 403, "path traversal not allowed in filename");
            return;
        }

        state.currentFilename = file.toString();
        state.of(file, root);
        state.overwrite = HttpKeywords.isTrue(context.getRequestHeader().getUrlParam(URL_PARAM_OVERWRITE));
        if (!state.overwrite && state.ff.exists(state.realPath.$())) {
            // error 409 conflict
            sendErrorWithSuccessInfo(context, 409, "file already exists and overwriting is disabled");
            return;
        }
        LOG.info().$("starting import [src=").$(file)
                .$(", dest=").$(state.realPath).I$();
    }

    @Override
    public void onPartEnd() throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        if (state.fd != -1) {
            state.ff.close(state.fd);
            state.fd = -1;
        }

        if (state.ff.exists(state.realPath.$())) {
            if (!state.overwrite) {
                state.ff.removeQuiet(state.tempPath.$());
                StringSink sink = Misc.getThreadLocalSink();
                sink.put("file already exists and overwriting is disabled [path=").put(state.tempPath).put(", overwrite=false]");
                sendErrorWithSuccessInfo(context, 409, sink);
                return;
            } else {
                state.ff.removeQuiet(state.realPath.$());
            }
        }

        if (state.ff.mkdirs(state.realPath, engine.getConfiguration().getMkDirMode()) != 0) {
            StringSink sink = Misc.getThreadLocalSink();
            sink.put("could not create directories [path=").put(state.realPath).put(", errno=").put(Os.errno()).put(']');
            sendErrorWithSuccessInfo(context, 500, sink);
            return;
        }

        int renameResult = state.ff.rename(state.tempPath.$(), state.realPath.$());
        if (renameResult != Files.FILES_RENAME_OK) {
            int errno = state.ff.errno();
            LOG.error().$("failed to rename temporary file [tempPath=").$(state.tempPath)
                    .$(", realPath=").$(state.realPath)
                    .$(", errno=").$(errno)
                    .$(", renameResult=").$(renameResult).I$();
            state.ff.removeQuiet(state.tempPath.$());
            StringSink sink = Misc.getThreadLocalSink();
            sink.put("cannot rename temporary file [tempPath=").put(state.tempPath).put(", realPath=").put(state.realPath).put(", errno=").put(errno).put(", renameResult=").put(renameResult).put(']');
            sendErrorWithSuccessInfo(context, 500, sink);
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
        encodeSuccessJson(response, state);
        response.sendChunk(true);
        response.done();
    }

    private void encodeSuccessJson(HttpChunkedResponse response, State state) {
        response.put("{\"successful\":[");
        encodeSuccessfulFilesList(response, state);
        response.put("]}");
    }

    private void encodeSuccessfulFilesList(HttpChunkedResponse response, State state) {
        for (int i = 0, n = state.successfulFiles.size(); i < n; i++) {
            response.putQuote().escapeJsonStr(state.successfulFiles.getQuick(i)).putQuote();
            if (i + 1 < n) {
                response.put(",");
            }
        }
    }

    private void sendErrorWithSuccessInfo(HttpConnectionContext context, int errorCode, CharSequence errorMsg) throws PeerIsSlowToReadException, PeerDisconnectedException, ServerDisconnectException {
        try {
            final HttpChunkedResponse response = context.getChunkedResponse();
            if (state.successfulFiles.size() > 0) {
                response.status(HTTP_MULTI_STATUS, CONTENT_TYPE_JSON);
                response.sendHeader();
                response.sendChunk(false);
                response.put("{\"successful\":[");
                encodeSuccessfulFilesList(response, state);
                response.put("],\"errors\":[{\"status\":\"").put(errorCode)
                        .put("\",\"detail\":\"").put(errorMsg);
                if (state.currentFilename != null && !state.currentFilename.isEmpty()) {
                    response.put("\",\"meta\":{\"filename\":")
                            .putQuote().escapeJsonStr(state.currentFilename).putQuote().put("}}]}");
                } else {
                    response.put("\"}]}");
                }
                response.sendChunk(true);
                response.done();
            } else {
                LOG.error().$("import file(s) failed [status=").$(errorCode).$(", fd=").$(context.getFd()).$(", msg=").$(errorMsg).I$();
                response.status(errorCode, CONTENT_TYPE_JSON);
                response.sendHeader();
                response.sendChunk(false);
                response.put("{\"errors\":[{\"status\":\"").put(errorCode)
                        .put("\",\"detail\":\"").put(errorMsg);
                if (state.currentFilename != null && !state.currentFilename.isEmpty()) {
                    response.put("\",\"meta\":{\"filename\":")
                            .putQuote().escapeJsonStr(state.currentFilename).putQuote().put("}}]}");
                } else {
                    response.put("\"}]}");
                }
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
            response.status(200, CONTENT_TYPE_JSON);
            response.sendHeader();
            response.sendChunk(false);
            encodeSuccessJson(response, state);
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
                ff.close(fd);
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

        public void of(DirectUtf8Sequence filename, CharSequence root) {
            // clear prev
            if (fd != -1) {
                ff.close(fd);
                fd = -1;
            }
            if (importId.isEmpty()) {
                long id = importIDSupplier.getAsLong();
                Numbers.appendHex(importId, id, true);
            }
            written = 0;
            tempPath.of(root).concat(importId);
            tempPathBaseLen = tempPath.size();
            tempPath.concat(filename);
            realPath.of(root).concat(filename);
        }

        public void of(long lo, long hi) {
            this.lo = lo;
            this.hi = hi;
        }
    }
}