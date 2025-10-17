package io.questdb.cutlass.http.processors.v1;

import io.questdb.cairo.CairoEngine;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.cutlass.http.HttpRequestValidator;
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
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8StringSink;

import static io.questdb.cutlass.http.HttpConstants.URL_PARAM_PATH;
import static java.net.HttpURLConnection.HTTP_OK;

public class FileDeleteProcessor implements HttpRequestProcessor {
    private static final Log LOG = LogFactory.getLog(FileDeleteProcessor.class);
    private final CairoEngine engine;
    private final FilesFacade ff;
    private final byte requiredAuthType;

    public FileDeleteProcessor(CairoEngine cairoEngine, JsonQueryProcessorConfiguration configuration) {
        engine = cairoEngine;
        ff = cairoEngine.getConfiguration().getFilesFacade();
        requiredAuthType = configuration.getRequiredAuthType();
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
        CharSequence importRoot = engine.getConfiguration().getSqlCopyInputRoot();
        if (Chars.isBlank(importRoot)) {
            sendError(context, 400, "sql.copy.input.root is not configured");
            return;
        }

        final DirectUtf8Sequence path = request.getUrlParam(URL_PARAM_PATH);
        if (path == null || path.size() == 0) {
            sendError(context, 400, "missing required parameter: path");
            return;
        }

        if (FileGetProcessor.containsAbsOrRelativePath(path)) {
            sendError(context, 403, "traversal not allowed in path");
            return;
        }
        Path filePath = Path.getThreadLocal(importRoot);
        filePath.concat(path);
        if (!ff.exists(filePath.$())) {
            sendError(context, 404, "path not found");
            return;
        }
        boolean isDirectory = Files.isDirOrSoftLinkDir(filePath.$());
        if (isDirectory) {
            if (!Files.rmdir(filePath, false)) {
                LOG.error().$("failed to delete import directory [path=").$(filePath).$(", errno=").$(ff.errno()).I$();
                sendError(context, 500, "failed to delete directory - may not be empty or have permission issues");
                return;
            }
            LOG.info().$("deleted import directory [path=").$(filePath).I$();
        } else {
            if (!ff.removeQuiet(filePath.$())) {
                LOG.error().$("failed to delete import file [path=").$(filePath).$(", errno=").$(ff.errno()).I$();
                sendError(context, 500, "failed to delete file");
                return;
            }
            LOG.info().$("deleted import file [path=").$(filePath).I$();
        }
        sendSuccess(context, path);
    }

    private void sendError(
            HttpConnectionContext context,
            int errorCode,
            String message
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        Utf8StringSink sink = Misc.getThreadLocalUtf8Sink();
        sink.putAscii("{\"error\":\"").putAscii(message).putAscii("\"}");
        context.simpleResponse().sendStatusJsonContent(errorCode, sink, false);
    }

    private void sendSuccess(
            HttpConnectionContext context,
            DirectUtf8Sequence path
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        Utf8StringSink sink = Misc.getThreadLocalUtf8Sink();
        sink.putAscii("{\"message\":\"path deleted successfully\",\"path\":\"").put(path).put("\"}");
        context.simpleResponse().sendStatusJsonContent(HTTP_OK, sink, false);
    }
}