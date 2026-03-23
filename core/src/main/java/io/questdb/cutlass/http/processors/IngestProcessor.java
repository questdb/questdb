/*******************************************************************************
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
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.Utf8SequenceObjHashMap;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_OK;

public class IngestProcessor implements HttpRequestHandler {
    private static final Log LOG = LogFactory.getLog(IngestProcessor.class);
    private static final LocalValue<IngestProcessorState> LV = new LocalValue<>();
    private static final Utf8String URL_PARAM_TRANSFORM = new Utf8String("transform");
    private final CairoEngine engine;
    private final PostProcessor postProcessor = new PostProcessor();
    private final int recvBufferSize;
    private final int sharedWorkerCount;

    public IngestProcessor(CairoEngine engine, int recvBufferSize, int sharedWorkerCount) {
        this.engine = engine;
        this.recvBufferSize = recvBufferSize;
        this.sharedWorkerCount = sharedWorkerCount;
    }

    @Override
    public HttpRequestProcessor getProcessor(HttpRequestHeader requestHeader) {
        return postProcessor;
    }

    class PostProcessor implements HttpPostPutProcessor {
        private final PayloadTransformDefinition transformDef = new PayloadTransformDefinition();
        private IngestProcessorState transientState;

        @Override
        public void onChunk(long lo, long hi) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
            if (hi > lo) {
                transientState.getBodySink().putNonAscii(lo, hi);
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
            try {
                final HttpRequestHeader header = context.getRequestHeader();
                final DirectUtf8Sequence transformNameUtf8 = header.getUrlParam(URL_PARAM_TRANSFORM);
                if (transformNameUtf8 == null) {
                    sendError(context, HTTP_BAD_REQUEST, "missing 'transform' query parameter");
                    return;
                }
                final String transformName = transformNameUtf8.toString();

                final SqlExecutionContextImpl sqlExecutionContext = context.getOrCreateSqlExecutionContext(engine, sharedWorkerCount);
                sqlExecutionContext.with(context.getSecurityContext(), null, null, context.getFd(), context.getOrCreateCircuitBreaker(engine).of(context.getFd()));
                sqlExecutionContext.initNow();

                final PayloadTransformStore store = engine.getPayloadTransformStore();
                transformDef.clear();
                final PayloadTransformDefinition def = store.lookupTransform(sqlExecutionContext, transformName, transformDef);
                if (def == null) {
                    sendError(context, HTTP_BAD_REQUEST, "transform not found: " + transformName);
                    return;
                }

                final LowerCaseCharSequenceObjHashMap<CharSequence> overrides = buildOverrideValues(header);

                final DirectUtf8Sink bodySink = transientState.getBodySink();
                final String payload = bodySink.toString();
                sqlExecutionContext.setPayload(payload);

                try {
                    final long rowCount = executeTransform(sqlExecutionContext, def, overrides);
                    sendSuccess(context, rowCount);
                } catch (SqlException | CairoException e) {
                    CharSequence errorMsg = e instanceof SqlException
                            ? ((SqlException) e).getFlyweightMessage()
                            : ((CairoException) e).getFlyweightMessage();
                    writeToDlq(sqlExecutionContext, def, payload, "transform", errorMsg);
                    throw e;
                } finally {
                    sqlExecutionContext.setPayload(null);
                }
            } catch (SqlException e) {
                LOG.error().$("ingest SQL error [msg=").$safe(e.getFlyweightMessage()).$(']').$();
                sendError(context, HTTP_BAD_REQUEST, e.getFlyweightMessage().toString());
            } catch (CairoException e) {
                LOG.error().$("ingest error [msg=").$safe(e.getFlyweightMessage()).$(']').$();
                sendError(context, HTTP_INTERNAL_ERROR, e.getFlyweightMessage().toString());
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
            transientState.send(context);
        }

        private LowerCaseCharSequenceObjHashMap<CharSequence> buildOverrideValues(HttpRequestHeader header) {
            final Utf8SequenceObjHashMap<DirectUtf8String> params = header.getUrlParams();
            final ObjList<Utf8String> keys = params.keys();
            LowerCaseCharSequenceObjHashMap<CharSequence> overrides = null;

            for (int i = 0, n = keys.size(); i < n; i++) {
                Utf8String key = keys.getQuick(i);
                if (Utf8s.equalsAscii("transform", key)) {
                    continue;
                }
                DirectUtf8String value = params.get(key);
                if (value == null) {
                    continue;
                }
                if (overrides == null) {
                    overrides = new LowerCaseCharSequenceObjHashMap<>();
                }
                // Variable names use @prefix; values are single-quoted string constants
                String varName = "@" + Utf8s.toString(key);
                String quotedValue = "'" + Utf8s.toString(value).replace("'", "''") + "'";
                overrides.put(varName, quotedValue);
            }
            return overrides;
        }

        private long executeTransform(
                SqlExecutionContextImpl sqlExecutionContext,
                PayloadTransformDefinition def,
                LowerCaseCharSequenceObjHashMap<CharSequence> overrides
        ) throws SqlException {
            final String targetTableName = def.getTargetTable();
            final String selectSql = def.getSelectSql();
            final TableToken targetToken = engine.verifyTableName(targetTableName);
            sqlExecutionContext.getSecurityContext().authorizeInsert(targetToken);

            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    RecordCursorFactory factory = (overrides != null
                            ? compiler.compileWithOverrides(selectSql, sqlExecutionContext, overrides)
                            : compiler.compile(selectSql, sqlExecutionContext)
                    ).getRecordCursorFactory()
            ) {
                final RecordMetadata cursorMetadata = factory.getMetadata();

                try (TableRecordMetadata writerMetadata = sqlExecutionContext.getMetadataForWrite(targetToken)) {
                    final int cursorColCount = cursorMetadata.getColumnCount();
                    final ListColumnFilter columnFilter = new ListColumnFilter(cursorColCount);
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
                            new BytecodeAssembler(),
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
                            if (writerTimestampIndex == -1) {
                                rowCount = SqlCompilerImpl.copyUnordered(sqlExecutionContext, cursor, writer, copier);
                            } else if (cursorTimestampIndex < 0) {
                                // Target has a timestamp but the transform doesn't produce it
                                rowCount = SqlCompilerImpl.copyUnordered(sqlExecutionContext, cursor, writer, copier);
                            } else {
                                int tsIndex = cursorTimestampIndex;
                                rowCount = SqlCompilerImpl.copyOrderedBatched(
                                        sqlExecutionContext,
                                        writer,
                                        cursorMetadata,
                                        cursor,
                                        copier,
                                        tsIndex,
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
                final Utf8StringSink utf8Sink = new Utf8StringSink();
                try (TableWriterAPI writer = engine.getTableWriterAPI(dlqToken, "ingest-dlq")) {
                    TableWriter.Row row = writer.newRow(sqlCtx.getMicrosecondTimestamp());
                    row.putSym(1, def.getName());
                    utf8Sink.put(payload);
                    row.putVarchar(2, utf8Sink);
                    utf8Sink.clear();
                    utf8Sink.put(def.getSelectSql());
                    row.putVarchar(3, utf8Sink);
                    row.putSym(4, stage);
                    utf8Sink.clear();
                    utf8Sink.put(error);
                    row.putVarchar(5, utf8Sink);
                    row.append();
                    writer.commit();
                }
            } catch (Throwable dlqError) {
                LOG.critical().$("failed to write to DLQ [table=").$(dlqTable)
                        .$(",error=").$(dlqError).$(']').$();
            }
        }
    }
}
