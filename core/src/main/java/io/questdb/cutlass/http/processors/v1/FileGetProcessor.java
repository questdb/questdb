package io.questdb.cutlass.http.processors.v1;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.http.HttpChunkedResponse;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpKeywords;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.cutlass.http.LocalValue;
import io.questdb.cutlass.http.processors.JsonQueryProcessorConfiguration;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.JsonSink;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.griffin.engine.functions.catalogue.FilesRecordCursor;
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
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;

import java.io.Closeable;

import static io.questdb.cutlass.http.HttpConstants.*;
import static io.questdb.cutlass.http.HttpRequestValidator.METHOD_GET;
import static io.questdb.cutlass.http.HttpRequestValidator.METHOD_HEAD;
import static java.net.HttpURLConnection.HTTP_OK;


public class FileGetProcessor implements HttpRequestProcessor {
    static final int FILE_SEND_INIT = 0;
    static final int FILE_SEND_CHUNK = FILE_SEND_INIT + 1;
    static final int FILE_SEND_COMPLETED = FILE_SEND_CHUNK + 1;
    private static final Log LOG = LogFactory.getLog(FileGetProcessor.class);
    private static final LocalValue<State> LV = new LocalValue<>();
    private static final Utf8String URL_PARAM_LIMIT = new Utf8String("page[limit]");
    private static final Utf8String URL_PARAM_OFFSET = new Utf8String("page[offset]");
    private final CairoEngine engine;
    private final FilesRootDir filesRoot;
    private final CharSequence queryTemplate;
    private final byte requiredAuthType;

    public FileGetProcessor(CairoEngine cairoEngine, JsonQueryProcessorConfiguration configuration, FilesRootDir root) {
        engine = cairoEngine;
        requiredAuthType = configuration.getRequiredAuthType();
        this.filesRoot = root;
        CharSequence rootPath = FilesRootDir.getRootPath(root, engine.getConfiguration());
        this.queryTemplate = "SELECT * FROM files('" + rootPath + "') LIMIT :lo, :hi;";
    }

    @Override
    public byte getRequiredAuthType() {
        return requiredAuthType;
    }

    @Override
    public short getSupportedRequestTypes() {
        return METHOD_GET | METHOD_HEAD;
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
        state.isHeadRequest = HttpKeywords.isHEAD(request.getMethod());
        final DirectUtf8Sequence file = extractFilePathFromUrl(request);
        if (file == null || file.size() == 0) {
            // No file in path - list all files in root directory
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
                    context.simpleResponse().sendStatusJsonApiContent(HTTP_OK, state.sink, false);
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
    }

    private DirectUtf8Sequence extractFilePathFromUrl(HttpRequestHeader request) {
        DirectUtf8String url = request.getUrl();
        if (url == null || url.size() == 0) {
            return null;
        }

        // URL format: /api/v1/imports or /api/v1/imports/file1.parquet
        // We need to extract everything after /imports or /exports segment

        final long urlPtr = url.ptr();
        final int urlSize = url.size();

        // Look for "/imports" or "/exports" (without trailing slash first)
        String searchString = filesRoot == FilesRootDir.IMPORTS ? "/imports" : "/exports";
        int searchLen = searchString.length();

        // Find the route segment
        int routeEnd = -1;
        for (int i = 0; i <= urlSize - searchLen; i++) {
            boolean match = true;
            for (int j = 0; j < searchLen; j++) {
                if (url.byteAt(i + j) != searchString.charAt(j)) {
                    match = false;
                    break;
                }
            }
            if (match) {
                routeEnd = i + searchLen;
                break;
            }
        }

        if (routeEnd == -1) {
            return null;
        }

        // Check what comes after the route
        // If we're at the end of URL or next char is '?', there's no file parameter
        if (routeEnd >= urlSize) {
            return null;
        }

        // Next char must be '/' for a file path, or '?' for query params, or end of string
        byte nextChar = url.byteAt(routeEnd);
        if (nextChar == '?') {
            // Query string, no file in path
            return null;
        }

        // Must be a slash to indicate a file path follows
        if (nextChar != '/') {
            // Unexpected character, no file path
            return null;
        }

        // Skip the '/' and extract the file path
        int fileStart = routeEnd + 1;

        // Extract the file path from fileStart to the end or query string
        int endPos = urlSize;
        for (int i = fileStart; i < urlSize; i++) {
            if (url.byteAt(i) == '?') {
                endPos = i;
                break;
            }
        }

        // If nothing after the slash, return null (empty file path)
        if (endPos <= fileStart) {
            return null;
        }

        // Return the extracted file path as a DirectUtf8String with adjusted boundaries
        DirectUtf8String result = new DirectUtf8String();
        result.of(urlPtr + fileStart, urlPtr + endPos);
        return result;
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
        response.headers()
                .putAscii("Content-Disposition: attachment; filename=\"").put(state.file).putAscii("\"").putEOL()
                .putAscii("Content-Length: ").put(state.fileSize).putEOL()
                .putAscii("Last-Modified: ");

        // Format Last-Modified as RFC 2822 date
        DateFormatUtils.formatHTTP(response.headers(), state.lastModified);
        response.headers().putEOL();
        response.sendHeader();
    }

    private void headerJsonError(int errorCode, HttpChunkedResponse response) throws PeerDisconnectedException, PeerIsSlowToReadException {
        response.status(errorCode, CONTENT_TYPE_JSON_API);
        response.sendHeader();
    }

    private void initFileSending(HttpChunkedResponse response, State state) throws PeerDisconnectedException, PeerIsSlowToReadException {
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
        if (state.isHeadRequest) {
            state.state = FILE_SEND_COMPLETED;
        } else {
            state.state = FILE_SEND_CHUNK;
        }
    }

    // todo: probably replace this with the griffin factories that handle files/globs, so we can support glob filters
    // they are TBA in read_parquet upgrades
    private void scanDirectory(
            JsonSink jsonSink,
            State state,
            SecurityContext securityContext
    ) {
        // build query
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            try (SqlExecutionContextImpl executionContext = new SqlExecutionContextImpl(engine, 1)) {
                BindVariableService bindings = new BindVariableServiceImpl(engine.getConfiguration());
                bindings.setInt("lo", state.offset);
                bindings.setInt("hi", state.offset + state.limit);
                executionContext.with(securityContext, bindings);
                CompiledQuery cq = compiler.compile(queryTemplate, executionContext);
                RecordCursorFactory rcf = cq.getRecordCursorFactory();
                RecordCursor rc = rcf.getCursor(executionContext);

                int counter = 0;
                while (rc.hasNext()) {
                    io.questdb.cairo.sql.Record record = rc.getRecord();
                    Utf8Sequence path = record.getVarcharA(FilesRecordCursor.PATH_COLUMN);
                    assert path != null;
                    jsonSink
                            .startObject()
                            .key("type").val("file")
                            .key("id").val(path)
                            .key("attributes")
                            .startObject();

                    int lastSlash = Utf8s.lastIndexOfAscii(path, '/');
                    if (lastSlash >= 0) {
                        jsonSink.key("filename").val(path, lastSlash + 1, path.size());
                    } else {
                        jsonSink.key("filename").val(path);
                    }

                    jsonSink.key("path").val(path);

                    long fileSize = record.getLong(FilesRecordCursor.SIZE_COLUMN);
                    long lastModified = record.getLong(FilesRecordCursor.MODIFIED_TIME_COLUMN);

                    jsonSink.key("size").val(fileSize);
                    jsonSink.key("sizePretty");
                    SizePrettyFunctionFactory.toSizePretty(jsonSink, fileSize);
                    jsonSink.key("lastModified").valMillis(lastModified)
                            .endObject()
                            .endObject();

                    // Store the last returned id as the cursor for the next page
                    state.lastId = path.toString();
                    counter++;
                }
                state.fileCount = state.offset + counter;
            } catch (SqlException e) {
                throw new RuntimeException(e); //todo: error handling
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
        JsonSink sink = Misc.getThreadLocalJsonSink();
        sink.of(response).startObject()
                .key("errors").startArray().startObject()
                .key("status").valQuoted(errorCode)
                .key("detail").val(message)
                .endObject().endArray().endObject();
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
        // Extract pagination parameters
        HttpRequestHeader request = context.getRequestHeader();
        DirectUtf8Sequence offsetParam = request.getUrlParam(URL_PARAM_OFFSET);
        DirectUtf8Sequence limitParam = request.getUrlParam(URL_PARAM_LIMIT);

        if (offsetParam != null && offsetParam.size() > 0) {
            state.offset = Numbers.parseInt(offsetParam.toString());
        }
        if (limitParam != null && limitParam.size() > 0) {
            try {
                state.limit = Integer.parseInt(limitParam.toString());
                // Enforce reasonable limits to prevent abuse
                if (state.limit <= 0) {
                    state.limit = 10;
                } else if (state.limit > 1000) {
                    state.limit = 1000;
                }
            } catch (NumberFormatException e) {
                state.limit = 10;
            }
        }

        Utf8StringSink listSink = state.sink;
        JsonSink jsonSink = Misc.getThreadLocalJsonSink();
        jsonSink.of(listSink).startObject().key("data").startArray();
        try {
            scanDirectory(jsonSink, state, context.getSecurityContext()); // acquires the sink
            jsonSink.endArray()
                    .key("meta").startObject()
                    .key("totalFiles").val(state.fileCount)
                    .key("page").startObject()
                    .key("limit").val(state.limit);
            if (state.lastId != null) {
                jsonSink.key("cursor").val(state.lastId);
            }
            jsonSink.endObject()
                    .endObject().endObject();
            context.simpleResponse().sendStatusJsonApiContent(HTTP_OK, listSink, false);
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
        // check for windows
        if (filename.size() >= 2 && filename.byteAt(1) == ':') {
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
        int fileCount = 0;
        long fileOffset;
        long fileSize;
        boolean firstFile = true;
        boolean isHeadRequest;
        CharSequence lastId; // last returned id for cursor-based pagination
        long lastModified;
        int limit = 10; // default limit
        // Pagination fields
        int offset = 0;
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
            isHeadRequest = false;
            lastModified = 0;
            fileSize = 0;
            fileOffset = 0;
            state = FILE_SEND_INIT;
            file = null;
            contentType = null;
            findStack.clear();
            pathLenStack.clear();
            firstFile = true;
            fileCount = 0;
            sink.clear();
            offset = 0;
            limit = 10;
            lastId = null;
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