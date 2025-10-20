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
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8StringSink;

import static io.questdb.cutlass.http.HttpConstants.URL_PARAM_FILE;
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

        final DirectUtf8Sequence path = request.getUrlParam(URL_PARAM_FILE);
        if (path == null || path.size() == 0) {
            sendException(context, 400, "missing required parameter: file");
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

    private void sendException(
            HttpConnectionContext context,
            int errorCode,
            CharSequence message
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
        sink.putAscii("{\"message\":\"file(s) deleted successfully\",\"path\":\"").put(path).put("\"}");
        context.simpleResponse().sendStatusJsonContent(HTTP_OK, sink, false);
    }
}