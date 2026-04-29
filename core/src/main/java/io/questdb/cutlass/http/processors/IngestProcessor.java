/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.cutlass.http.processors;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.pt.PayloadTransformDefinition;
import io.questdb.cairo.pt.PayloadTransformStore;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpPostPutProcessor;
import io.questdb.cutlass.http.HttpRequestHandler;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.cutlass.http.LocalValue;
import io.questdb.griffin.RecordToRowCopier;
import io.questdb.griffin.RecordToRowCopierUtils;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.ServerDisconnectException;
import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.Utf8SequenceObjHashMap;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_ENTITY_TOO_LARGE;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_OK;

public class IngestProcessor implements HttpRequestHandler {
    private static final int DLQ_COL_ERROR = 5;
    private static final int DLQ_COL_PAYLOAD = 2;
    private static final int DLQ_COL_QUERY = 3;
    private static final int DLQ_COL_STAGE = 4;
    private static final int DLQ_COL_TRANSFORM_NAME = 1;
    private static final Log LOG = LogFactory.getLog(IngestProcessor.class);
    private static final LocalValue<IngestProcessorState> LV = new LocalValue<>();
    private static final Utf8String URL_PARAM_TRANSFORM = new Utf8String("transform");
    private final CairoEngine engine;
    private final long maxRequestSize;
    private final String maxRequestSizeError;
    private final PostProcessor postProcessor = new PostProcessor();
    private final int recvBufferSize;
    private final int sharedWorkerCount;

    public IngestProcessor(CairoEngine engine, int recvBufferSize, long maxRequestSize, int sharedWorkerCount) {
        this.engine = engine;
        this.recvBufferSize = recvBufferSize;
        this.maxRequestSize = maxRequestSize;
        this.maxRequestSizeError = "request body exceeds maximum size of " + maxRequestSize + " bytes";
        this.sharedWorkerCount = sharedWorkerCount;
    }

    @Override
    public HttpRequestProcessor getProcessor(HttpRequestHeader requestHeader) {
        return postProcessor;
    }

    class PostProcessor implements HttpPostPutProcessor {
        private IngestProcessorState transientState;

        @Override
        public void onChunk(long lo, long hi) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
            if (hi > lo && transientState.getChunkError() == null) {
                final DirectUtf8Sink bodySink = transientState.getBodySink();
                if (bodySink.size() + (hi - lo) > maxRequestSize) {
                    transientState.setChunkError(maxRequestSizeError);
                    return;
                }
                bodySink.putNonAscii(lo, hi);
            }
        }

        @Override
        public void onHeadersReady(HttpConnectionContext context) {
            transientState = LV.get(context);
            if (transientState == null) {
                LOG.debug().$("new ingest state").$();
                LV.set(context, transientState = new IngestProcessorState(recvBufferSize));
            }
            transientState.clear();
        }

        @Override
        public void onRequestComplete(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
            final String chunkError = transientState.getChunkError();
            if (chunkError != null) {
                sendError(context, HTTP_ENTITY_TOO_LARGE, chunkError);
                return;
            }
            try {
                final HttpRequestHeader header = context.getRequestHeader();
                final DirectUtf8Sequence transformNameUtf8 = header.getUrlParam(URL_PARAM_TRANSFORM);
                if (transformNameUtf8 == null) {
                    sendError(context, HTTP_BAD_REQUEST, "missing 'transform' query parameter");
                    return;
                }
                final StringSink transformNameSink = transientState.getTransformNameSink();
                transformNameSink.clear();
                if (!Utf8s.utf8ToUtf16(transformNameUtf8, transformNameSink)) {
                    sendError(context, HTTP_BAD_REQUEST, "invalid UTF-8 in 'transform' parameter");
                    return;
                }
                final CharSequence transformName = transformNameSink;

                final SqlExecutionContextImpl sqlExecutionContext = context.getOrCreateSqlExecutionContext(engine, sharedWorkerCount);
                sqlExecutionContext.with(context.getSecurityContext(), null, null, context.getFd(), context.getOrCreateCircuitBreaker(engine).of(context.getFd()));
                sqlExecutionContext.initNow();

                final PayloadTransformStore store = engine.getPayloadTransformStore();
                final PayloadTransformDefinition def = store.lookupTransform(sqlExecutionContext, transformName, transientState.getTransformDef());
                if (def == null) {
                    sendError(context, HTTP_BAD_REQUEST, "transform not found: " + transformName.toString());
                    return;
                }

                final LowerCaseCharSequenceObjHashMap<CharSequence> overrides = buildOverrideValues(header);

                // Decode UTF-8 body into a pooled CharSequence to avoid heap String copy
                final DirectUtf8Sink bodySink = transientState.getBodySink();
                if (bodySink.size() == 0) {
                    sendError(context, HTTP_BAD_REQUEST, "request body is empty");
                    return;
                }
                final StringSink payloadSink = transientState.getPayloadSink();
                payloadSink.clear();
                if (!Utf8s.utf8ToUtf16(bodySink, payloadSink)) {
                    sendError(context, HTTP_BAD_REQUEST, "invalid UTF-8 in request body");
                    return;
                }
                sqlExecutionContext.setPayload(payloadSink);

                // Compile outside DLQ scope - compilation errors are config
                // problems (schema drift), not payload problems
                final RecordCursorFactory factory = compileTransform(sqlExecutionContext, def, overrides);
                try {
                    final long rowCount = executeTransform(sqlExecutionContext, def, factory);
                    sendSuccess(context, rowCount);
                } catch (SqlException | CairoException e) {
                    writeToDlq(sqlExecutionContext, def, payloadSink, "transform",
                            ((FlyweightMessageContainer) e).getFlyweightMessage().toString());
                    throw e;
                } finally {
                    factory.close();
                    sqlExecutionContext.setPayload(null);
                }
            } catch (SqlException e) {
                LOG.error().$("ingest SQL error [msg=").$safe(e.getFlyweightMessage()).$(']').$();
                sendError(context, HTTP_BAD_REQUEST, e.getFlyweightMessage());
            } catch (CairoException e) {
                LOG.error().$("ingest error [msg=").$safe(e.getFlyweightMessage()).$(']').$();
                sendError(context, HTTP_INTERNAL_ERROR, e.getFlyweightMessage());
            } catch (PeerDisconnectedException | PeerIsSlowToReadException e) {
                throw e;
            } catch (Throwable e) {
                LOG.critical().$("ingest error: ").$(e).$();
                String msg = e.getMessage();
                sendError(context, HTTP_INTERNAL_ERROR, msg != null ? msg : e.getClass().getName());
            }
        }

        @Override
        public void resumeRecv(HttpConnectionContext context) {
            transientState = LV.get(context);
        }

        @Override
        public void resumeSend(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
            transientState = LV.get(context);
            transientState.send(context);
        }

        private LowerCaseCharSequenceObjHashMap<CharSequence> buildOverrideValues(HttpRequestHeader header) {
            final Utf8SequenceObjHashMap<DirectUtf8String> params = header.getUrlParams();
            final ObjList<Utf8String> keys = params.keys();
            final LowerCaseCharSequenceObjHashMap<CharSequence> overrides = transientState.getOverrides();
            boolean hasOverrides = false;

            for (int i = 0, n = keys.size(); i < n; i++) {
                Utf8String key = keys.getQuick(i);
                if (Utf8s.equalsAscii("transform", key)) {
                    continue;
                }
                DirectUtf8String value = params.get(key);
                if (value == null) {
                    continue;
                }
                hasOverrides = true;
                // Variable names use @prefix; values are single-quoted string constants
                String varName = "@" + Utf8s.toString(key);
                String quotedValue = "'" + Utf8s.toString(value).replace("'", "''") + "'";
                overrides.put(varName, quotedValue);
            }
            return hasOverrides ? overrides : null;
        }

        private RecordCursorFactory compileTransform(
                SqlExecutionContextImpl sqlExecutionContext,
                PayloadTransformDefinition def,
                LowerCaseCharSequenceObjHashMap<CharSequence> overrides
        ) throws SqlException {
            final String selectSql = def.getSelectSql();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                RecordCursorFactory factory = (overrides != null
                        ? compiler.compileWithOverrides(selectSql, sqlExecutionContext, overrides)
                        : compiler.compile(selectSql, sqlExecutionContext)
                ).getRecordCursorFactory();
                if (factory == null) {
                    throw SqlException.$(0, "payload transform query must be a SELECT");
                }
                return factory;
            }
        }

        private long executeTransform(
                SqlExecutionContextImpl sqlExecutionContext,
                PayloadTransformDefinition def,
                RecordCursorFactory factory
        ) throws SqlException {
            final String targetTableName = def.getTargetTable();
            final TableToken targetToken = engine.verifyTableName(targetTableName);
            if (!targetToken.isWal()) {
                throw SqlException.$(0, "target table must be WAL-enabled [table=").put(targetTableName).put(']');
            }
            sqlExecutionContext.getSecurityContext().authorizeInsert(targetToken);

            final RecordMetadata cursorMetadata = factory.getMetadata();

            try (TableRecordMetadata writerMetadata = sqlExecutionContext.getMetadataForWrite(targetToken)) {
                final int cursorColCount = cursorMetadata.getColumnCount();
                final ListColumnFilter columnFilter = transientState.getColumnFilter();
                columnFilter.clear();
                for (int i = 0; i < cursorColCount; i++) {
                    CharSequence colName = cursorMetadata.getColumnName(i);
                    int writerIndex = writerMetadata.getColumnIndexQuiet(colName);
                    if (writerIndex == -1) {
                        throw SqlException.$(0, "column not found in target table [column=").put(colName).put(']');
                    }
                    // ListColumnFilter stores writerIndex + 1; getColumnIndexFactored subtracts 1
                    columnFilter.add(writerIndex + 1);
                }
                final RecordToRowCopier copier = RecordToRowCopierUtils.generateCopier(
                        transientState.getBytecodeAssembler(),
                        cursorMetadata,
                        writerMetadata,
                        columnFilter,
                        engine.getConfiguration()
                );

                final int writerTimestampIndex = writerMetadata.getTimestampIndex();

                // Find the cursor column index for the designated timestamp by name
                int cursorTimestampIndex = -1;
                if (writerTimestampIndex >= 0) {
                    CharSequence tsColName = writerMetadata.getColumnName(writerTimestampIndex);
                    cursorTimestampIndex = cursorMetadata.getColumnIndexQuiet(tsColName);
                }

                try (
                        TableWriterAPI writer = engine.getTableWriterAPI(targetToken, "ingest");
                        RecordCursor cursor = factory.getCursor(sqlExecutionContext)
                ) {
                    long rowCount;
                    try {
                        if (writerTimestampIndex == -1 || cursorTimestampIndex < 0) {
                            rowCount = SqlCompilerImpl.copyUnordered(sqlExecutionContext, cursor, writer, copier);
                        } else {
                            rowCount = SqlCompilerImpl.copyOrderedBatched(
                                    sqlExecutionContext,
                                    writer,
                                    cursorMetadata,
                                    cursor,
                                    copier,
                                    cursorTimestampIndex,
                                    Long.MAX_VALUE,
                                    0
                            );
                        }
                    } catch (Throwable e) {
                        try {
                            writer.rollback();
                        } catch (Throwable e2) {
                            LOG.error().$("could not rollback, writer must be distressed [table=").$(targetToken).$(']').$();
                        }
                        throw e;
                    }
                    writer.commit();
                    return rowCount;
                }
            }
        }

        private void sendError(HttpConnectionContext context, int statusCode, CharSequence message) throws PeerDisconnectedException, PeerIsSlowToReadException {
            transientState.clear();
            transientState.setStatusCode(statusCode);
            final DirectUtf8Sink responseSink = transientState.getResponseSink();
            responseSink.put("{\"status\":\"error\",\"message\":\"").escapeJsonStr(message).put("\"}");
            transientState.send(context);
        }

        private void sendSuccess(HttpConnectionContext context, long rowCount) throws PeerDisconnectedException, PeerIsSlowToReadException {
            transientState.clear();
            transientState.setStatusCode(HTTP_OK);
            final DirectUtf8Sink responseSink = transientState.getResponseSink();
            responseSink.put("{\"status\":\"ok\",\"rows_inserted\":").put(rowCount).put('}');
            transientState.send(context);
        }

        private void writeToDlq(
                SqlExecutionContextImpl sqlCtx,
                PayloadTransformDefinition def,
                CharSequence payload,
                CharSequence stage,
                CharSequence error
        ) {
            final String dlqTable = def.getDlqTable();
            if (dlqTable == null) {
                return;
            }
            try {
                final TableToken dlqToken = engine.verifyTableName(dlqTable);
                final Utf8StringSink utf8Sink = transientState.getDlqSink();
                utf8Sink.clear();
                try (TableWriterAPI writer = engine.getTableWriterAPI(dlqToken, "ingest-dlq")) {
                    TableWriter.Row row = writer.newRow(sqlCtx.getMicrosecondTimestamp());
                    row.putSym(DLQ_COL_TRANSFORM_NAME, def.getName());
                    utf8Sink.put(payload);
                    row.putVarchar(DLQ_COL_PAYLOAD, utf8Sink);
                    utf8Sink.clear();
                    utf8Sink.put(def.getSelectSql());
                    row.putVarchar(DLQ_COL_QUERY, utf8Sink);
                    row.putSym(DLQ_COL_STAGE, stage);
                    utf8Sink.clear();
                    utf8Sink.put(error);
                    row.putVarchar(DLQ_COL_ERROR, utf8Sink);
                    row.append();
                    writer.commit();
                }
            } catch (Throwable dlqError) {
                // Intentionally catching all exceptions: DLQ write is best-effort and
                // must not mask the original transform error propagated to the caller.
                LOG.critical().$("failed to write to DLQ [table=").$(dlqTable)
                        .$(",error=").$(dlqError).$(']').$();
            }
        }
    }
}
