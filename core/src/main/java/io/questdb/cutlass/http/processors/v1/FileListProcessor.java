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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.http.HttpChunkedResponse;
import io.questdb.cutlass.http.HttpConnectionContext;
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
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.ServerDisconnectException;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;

import java.io.Closeable;

import static io.questdb.cutlass.http.HttpConstants.CONTENT_TYPE_JSON_API;
import static io.questdb.cutlass.http.HttpRequestValidator.METHOD_GET;
import static java.net.HttpURLConnection.HTTP_OK;

public class FileListProcessor implements HttpRequestProcessor {
    private static final Log LOG = LogFactory.getLog(FileListProcessor.class);
    private static final LocalValue<FileListState> LV = new LocalValue<>();
    private static final Utf8Sequence URL_PARAM_LIMIT = new Utf8String("page[limit]");
    private static final Utf8Sequence URL_PARAM_OFFSET = new Utf8String("page[offset]");
    private final CairoEngine engine;
    private final FilesRootDir filesRoot;
    private final CharSequence queryTemplate;
    private final byte requiredAuthType;

    public FileListProcessor(CairoEngine cairoEngine, JsonQueryProcessorConfiguration configuration, FilesRootDir root) {
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
        return METHOD_GET;
    }

    @Override
    public void onRequestComplete(
            HttpConnectionContext context
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        FileListState state = LV.get(context);
        if (state == null) {
            LV.set(context, state = new FileListState());
        }

        CharSequence root = FilesRootDir.getRootPath(filesRoot, engine.getConfiguration());
        if (Chars.isBlank(root)) {
            StringSink sink = Misc.getThreadLocalSink();
            sink.put(filesRoot.getConfigName()).put(" is not configured");
            sendException(400, context.getChunkedResponse(), sink, state);
            return;
        }

        sendFileList(context, root, state);
    }

    @Override
    public void parkRequest(HttpConnectionContext context, boolean paused) {
        // File listing doesn't need to park
    }

    @Override
    public void resumeSend(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        // File listing is handled in onRequestComplete
    }

    private void scanDirectory(
            JsonSink jsonSink,
            FileListState state,
            SecurityContext securityContext
    ) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler();
             SqlExecutionContextImpl executionContext = new SqlExecutionContextImpl(engine, 1)) {
            BindVariableService bindings = new BindVariableServiceImpl(engine.getConfiguration());
            bindings.setInt("lo", state.offset);
            bindings.setInt("hi", state.offset + state.limit);
            executionContext.with(securityContext, bindings);
            CompiledQuery cq = compiler.compile(queryTemplate, executionContext);
            try (RecordCursorFactory rcf = cq.getRecordCursorFactory();
                 RecordCursor rc = rcf.getCursor(executionContext)) {
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
                    long lastModified = record.getDate(FilesRecordCursor.MODIFIED_TIME_COLUMN);

                    jsonSink.key("size").val(fileSize);
                    jsonSink.key("sizePretty");
                    SizePrettyFunctionFactory.toSizePretty(jsonSink, fileSize);
                    jsonSink.key("lastModified").valMillis(lastModified)
                            .endObject()
                            .endObject();

                    state.lastId = path.toString();
                    counter++;
                }
                state.fileCount = state.offset + counter;
            }
        }
    }

    private void sendException(
            int errorCode,
            HttpChunkedResponse response,
            CharSequence message,
            FileListState state
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

    private void sendFileList(
            HttpConnectionContext context,
            CharSequence root,
            FileListState state
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        HttpRequestHeader request = context.getRequestHeader();
        Utf8Sequence offsetParam = request.getUrlParam(URL_PARAM_OFFSET);
        Utf8Sequence limitParam = request.getUrlParam(URL_PARAM_LIMIT);

        if (offsetParam != null && offsetParam.size() > 0) {
            try {
                int parsedOffset = Numbers.parseInt(offsetParam.toString());
                if (parsedOffset >= 0) {
                    state.offset = parsedOffset;
                }
            } catch (NumberFormatException e) {
                // Keep default offset of 0
            }
        }
        if (limitParam != null && limitParam.size() > 0) {
            try {
                state.limit = Integer.parseInt(limitParam.toString());
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
            scanDirectory(jsonSink, state, context.getSecurityContext());
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
        } catch (CairoException | SqlException e) {
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

    public static class FileListState implements Closeable {
        int fileCount = 0;
        String lastId;
        int limit = 10;
        int offset = 0;
        Utf8StringSink sink = new Utf8StringSink();

        @Override
        public void close() {
            sink.clear();
            offset = 0;
            limit = 10;
            lastId = null;
            fileCount = 0;
        }
    }
}
