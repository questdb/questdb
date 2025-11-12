package io.questdb.cutlass.http.processors.v1;

import io.questdb.cairo.CairoEngine;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.cutlass.http.HttpRequestValidator;
import io.questdb.cutlass.http.processors.JsonQueryProcessorConfiguration;
import io.questdb.griffin.JsonSink;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.ServerDisconnectException;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8StringSink;

import static java.net.HttpURLConnection.HTTP_OK;

public class FileDeleteProcessor implements HttpRequestProcessor {
    private static final Log LOG = LogFactory.getLog(FileDeleteProcessor.class);
    private final CairoEngine engine;
    private final FilesFacade ff;
    private final FilesRootDir filesRoot;
    private final byte requiredAuthType;

    public FileDeleteProcessor(CairoEngine cairoEngine, JsonQueryProcessorConfiguration configuration, FilesRootDir filesRoot) {
        engine = cairoEngine;
        ff = cairoEngine.getConfiguration().getFilesFacade();
        requiredAuthType = configuration.getRequiredAuthType();
        this.filesRoot = filesRoot;
    }

    @Override
    public byte getRequiredAuthType() {
        return requiredAuthType;
    }

    @Override
    public short getSupportedRequestTypes() {
        return HttpRequestValidator.METHOD_DELETE;
    }

    @Override
    public void onRequestComplete(
            HttpConnectionContext context
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        HttpRequestHeader request = context.getRequestHeader();
        CharSequence root = FilesRootDir.getRootPath(filesRoot, engine.getConfiguration());
        if (Chars.isBlank(root)) {
            StringSink sink = Misc.getThreadLocalSink();
            sink.put(filesRoot.getConfigName()).put(" is not configured");
            sendException(context, 400, sink);
            return;
        }

        final DirectUtf8Sequence path = extractFilePathFromUrl(request);
        if (path == null || path.size() == 0) {
            sendException(context, 400, "missing required file path");
            return;
        }

        if (FileGetProcessor.containsAbsOrRelativePath(path)) {
            sendException(context, 403, "traversal not allowed in file");
            return;
        }
        Path filePath = Path.getThreadLocal(root);
        filePath.concat(path);
        if (!ff.exists(filePath.$())) {
            sendException(context, 404, "file(s) not found");
            return;
        }
        boolean isDirectory = Files.isDirOrSoftLinkDir(filePath.$());
        if (isDirectory) {
            if (!Files.rmdir(filePath, false)) {
                LOG.error().$("failed to delete directory [path=").$(filePath).$(", errno=").$(ff.errno()).I$();
                sendException(context, 500, "failed to delete directory - may not be empty or have permission issues");
                return;
            }
            LOG.info().$("deleted directory [path=").$(filePath).I$();
        } else {
            if (!ff.removeQuiet(filePath.$())) {
                LOG.error().$("failed to delete file [path=").$(filePath).$(", errno=").$(ff.errno()).I$();
                sendException(context, 500, "failed to delete file");
                return;
            }
            LOG.info().$("deleted file [path=").$(filePath).I$();
        }
        sendSuccess(context, path);
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

    private void sendException(
            HttpConnectionContext context,
            int errorCode,
            CharSequence message
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        Utf8StringSink sink = Misc.getThreadLocalUtf8Sink();
        JsonSink json = Misc.getThreadLocalJsonSink();
        json.of(sink, false)
                .startObject()
                .key("errors").startArray()
                .startObject()
                .key("status").val(errorCode)
                .key("detail").val(message)
                .endObject()
                .endArray()
                .endObject();
        context.simpleResponse().sendStatusJsonContent(errorCode, sink, false);
    }

    private void sendSuccess(
            HttpConnectionContext context,
            DirectUtf8Sequence path
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        Utf8StringSink sink = Misc.getThreadLocalUtf8Sink();
        JsonSink json = Misc.getThreadLocalJsonSink();
        json.of(sink, false)
                .startObject()
                .key("message").val("file(s) deleted successfully")
                .key("path").val(path)
                .endObject();
        context.simpleResponse().sendStatusJsonContent(HTTP_OK, sink, false);
    }
}