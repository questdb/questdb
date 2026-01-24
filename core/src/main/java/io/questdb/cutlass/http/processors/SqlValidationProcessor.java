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

import io.questdb.Metrics;
import io.questdb.TelemetryOrigin;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cutlass.http.HttpChunkedResponse;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpException;
import io.questdb.cutlass.http.HttpRequestHandler;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.cutlass.http.LocalValue;
import io.questdb.cutlass.http.ex.RetryOperationException;
import io.questdb.cutlass.text.Utf8Exception;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.NoSpaceLeftInResponseBufferException;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.datetime.NanosecondClock;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Path;

import java.io.Closeable;

import static io.questdb.cutlass.http.HttpConstants.URL_PARAM_QUERY;
import static java.net.HttpURLConnection.*;

public class SqlValidationProcessor implements HttpRequestProcessor, HttpRequestHandler, Closeable {

    private static final Log LOG = LogFactory.getLog(SqlValidationProcessor.class);
    private static final LocalValue<SqlValidationProcessorState> LV = new LocalValue<>();
    private final NetworkSqlExecutionCircuitBreaker circuitBreaker;
    private final JsonQueryProcessorConfiguration configuration;
    private final CairoEngine engine;
    private final NanosecondClock nanosecondClock;
    private final Path path;
    private final byte requiredAuthType;
    private final SqlExecutionContextImpl sqlExecutionContext;

    public SqlValidationProcessor(
            JsonQueryProcessorConfiguration configuration,
            CairoEngine engine,
            int sharedWorkerCount
    ) {
        this(
                configuration,
                engine,
                new SqlExecutionContextImpl(engine, sharedWorkerCount)
        );
    }

    public SqlValidationProcessor(
            JsonQueryProcessorConfiguration configuration,
            CairoEngine engine,
            SqlExecutionContextImpl sqlExecutionContext
    ) {
        try {
            this.configuration = configuration;
            this.path = new Path();
            this.engine = engine;
            requiredAuthType = configuration.getRequiredAuthType();
            this.sqlExecutionContext = sqlExecutionContext;
            this.nanosecondClock = configuration.getNanosecondClock();
            this.circuitBreaker = new NetworkSqlExecutionCircuitBreaker(engine, engine.getConfiguration().getCircuitBreakerConfiguration(), MemoryTag.NATIVE_CB3);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void close() {
        Misc.free(path);
        Misc.free(circuitBreaker);
    }

    @Override
    public void failRequest(HttpConnectionContext context, HttpException e) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final SqlValidationProcessorState state = LV.get(context);
        final HttpChunkedResponse response = context.getChunkedResponse();
        sendException(
                state,
                e.getPosition(),
                e.getFlyweightMessage(),
                HTTP_BAD_REQUEST
        );
        response.shutdownWrite();
    }

    @Override
    public HttpRequestProcessor getProcessor(HttpRequestHeader requestHeader) {
        return this;
    }

    @Override
    public byte getRequiredAuthType() {
        return requiredAuthType;
    }

    @Override
    public void onRequestComplete(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException {
        SqlValidationProcessorState state = LV.get(context);
        if (state == null) {
            LV.set(context, state = new SqlValidationProcessorState(
                    context,
                    configuration.getKeepAliveHeader()
            ));
        } else {
            state.clear();
        }

        // clear random for new request to avoid reusing random between requests
        state.setRnd(null);

        if (parseUrl(state, configuration.getKeepAliveHeader())) {
            validate0(state);
        } else {
            readyForNextRequest(context);
        }
    }

    @Override
    public void onRequestRetry(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException {
        validate0(LV.get(context));
    }

    @Override
    public void parkRequest(HttpConnectionContext context, boolean pausedQuery) {
        final SqlValidationProcessorState state = LV.get(context);
        if (state != null) {
            // preserve random when we park the context
            state.setRnd(sqlExecutionContext.getRandom());
        }
    }

    @Override
    public void resumeSend(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final SqlValidationProcessorState state = LV.get(context);
        if (state != null) {
            context.resumeResponseSend();
            try {
                doResumeSend(state, context);
            } catch (CairoError e) {
                internalError(state, HTTP_INTERNAL_ERROR, e);
            } catch (CairoException e) {
                internalError(state, HTTP_BAD_REQUEST, e);
            }
        }
    }

    public void validate0(SqlValidationProcessorState state) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final HttpConnectionContext context = state.getHttpConnectionContext();
        circuitBreaker.resetTimer();
        try {
            // new query
            compileAndValidate(state);
        } catch (SqlException | ImplicitCastException e) {
            sendException(
                    state,
                    e.getPosition(),
                    e.getFlyweightMessage(),
                    HTTP_BAD_REQUEST
            );
            // close the factory on reset instead of caching it
            readyForNextRequest(context);
        } catch (EntryUnavailableException e) {
            LOG.info().$("[fd=").$(context.getFd()).$("] resource busy, will retry").$();
            throw RetryOperationException.INSTANCE;
        } catch (CairoException e) {
            internalError(state, getStatusCode(e), e);
            readyForNextRequest(context);
        } catch (PeerIsSlowToReadException | PeerDisconnectedException e) {
            // re-throw the exception
            throw e;
        } catch (Throwable e) {
            internalError(state, HTTP_INTERNAL_ERROR, e);
            readyForNextRequest(context);
        }
    }

    private static int getStatusCode(CairoException e) {
        if (e.isAuthorizationError()) {
            return HTTP_FORBIDDEN;
        }
        return HTTP_BAD_REQUEST;
    }

    private static void logInternalError(
            Throwable e,
            SqlValidationProcessorState state,
            Metrics metrics
    ) {
        if (e instanceof CairoException ce) {
            if (ce.isInterruption()) {
                state.info().$("query cancelled [reason=`").$safe(ce.getFlyweightMessage())
                        .$("`, q=`").$safe(state.getQueryOrHidden())
                        .$("`]").$();
            } else if (ce.isCritical()) {
                state.critical().$("error [msg=`").$safe(ce.getFlyweightMessage())
                        .$("`, errno=").$(ce.getErrno())
                        .$(", q=`").$safe(state.getQueryOrHidden())
                        .$("`]").$();
            } else {
                state.error().$("error [msg=`").$safe(ce.getFlyweightMessage())
                        .$("`, errno=").$(ce.getErrno())
                        .$(", q=`").$safe(state.getQueryOrHidden())
                        .$("`]").$();
            }
        } else if (e instanceof HttpException) {
            state.error().$("internal HTTP server error [reason=`").$safe(((HttpException) e).getFlyweightMessage())
                    .$("`, q=`").$safe(state.getQueryOrHidden())
                    .$("`]").$();
        } else {
            state.critical().$("internal error [ex=").$(e)
                    .$(", q=`").$safe(state.getQueryOrHidden())
                    .$("`]").$();
            // This is a critical error, so we treat it as an unhandled one.
            metrics.healthMetrics().incrementUnhandledErrors();
        }
    }

    private static void sendConfirmation(
            SqlValidationProcessorState state,
            CharSequence keepAliveHeader,
            String queryTypeStringConfirmation
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final HttpConnectionContext context = state.getHttpConnectionContext();
        final HttpChunkedResponse response = context.getChunkedResponse();
        state.storeQueryTypeStringConfirmation(queryTypeStringConfirmation);
        JsonQueryProcessor.header(response, context, keepAliveHeader, 200);
        state.onResumeSendConfirmation(response);
    }

    private void compileAndValidate(
            SqlValidationProcessorState state
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            final long compilationStart = nanosecondClock.getTicks();
            HttpConnectionContext context = state.getHttpConnectionContext();
            sqlExecutionContext.with(
                    context.getSecurityContext(),
                    null,
                    state.getRnd(),
                    context.getFd(),
                    circuitBreaker.of(context.getFd())
            );
            sqlExecutionContext.setValidationOnly(true);
            final CompiledQuery cc = compiler.compile(state.getQuery(), sqlExecutionContext);
            sqlExecutionContext.storeTelemetry(cc.getType(), TelemetryOrigin.HTTP_QUERY_VALIDATE);
            state.setCompilerNanos(nanosecondClock.getTicks() - compilationStart);
            state.setQueryType(cc.getType());
            try {
                switch (state.getQueryType()) {
                    case CompiledQuery.SELECT -> executeNewSelect(state, cc);
                    case CompiledQuery.INSERT -> sendConfirmation(state, configuration.getKeepAliveHeader(), "INSERT");
                    case CompiledQuery.TRUNCATE ->
                            sendConfirmation(state, configuration.getKeepAliveHeader(), "TRUNCATE");
                    case CompiledQuery.ALTER ->
                            sendConfirmation(state, configuration.getKeepAliveHeader(), "ALTER TABLE");
                    case CompiledQuery.SET -> sendConfirmation(state, configuration.getKeepAliveHeader(), "SET");
                    case CompiledQuery.DROP -> sendConfirmation(state, configuration.getKeepAliveHeader(), "DROP");
                    case CompiledQuery.PSEUDO_SELECT ->
                            sendConfirmation(state, configuration.getKeepAliveHeader(), "COPY");
                    case CompiledQuery.CREATE_TABLE ->
                            sendConfirmation(state, configuration.getKeepAliveHeader(), "CREATE TABLE");
                    case CompiledQuery.INSERT_AS_SELECT ->
                            sendConfirmation(state, configuration.getKeepAliveHeader(), "INSERT AS SELECT");
                    case CompiledQuery.COPY_REMOTE ->
                            sendConfirmation(state, configuration.getKeepAliveHeader(), "COPY REMOTE");
                    case CompiledQuery.RENAME_TABLE ->
                            sendConfirmation(state, configuration.getKeepAliveHeader(), "RENAME TABLE");
                    case CompiledQuery.REPAIR -> sendConfirmation(state, configuration.getKeepAliveHeader(), "REPAIR");
                    case CompiledQuery.BACKUP_DATABASE ->
                            sendConfirmation(state, configuration.getKeepAliveHeader(), "BACKUP DATABASE");
                    case CompiledQuery.UPDATE -> sendConfirmation(state, configuration.getKeepAliveHeader(), "UPDATE");
                    case CompiledQuery.VACUUM -> sendConfirmation(state, configuration.getKeepAliveHeader(), "VACUUM");
                    case CompiledQuery.BEGIN -> sendConfirmation(state, configuration.getKeepAliveHeader(), "BEGIN");
                    case CompiledQuery.COMMIT -> sendConfirmation(state, configuration.getKeepAliveHeader(), "COMMIT");
                    case CompiledQuery.ROLLBACK ->
                            sendConfirmation(state, configuration.getKeepAliveHeader(), "ROLLBACK");
                    case CompiledQuery.CREATE_TABLE_AS_SELECT ->
                            sendConfirmation(state, configuration.getKeepAliveHeader(), "CREATE AS SELECT");
                    case CompiledQuery.CHECKPOINT_CREATE ->
                            sendConfirmation(state, configuration.getKeepAliveHeader(), "CHECKPOINT CREATE");
                    case CompiledQuery.CHECKPOINT_RELEASE ->
                            sendConfirmation(state, configuration.getKeepAliveHeader(), "CHECKPOINT RELEASE");
                    case CompiledQuery.DEALLOCATE ->
                            sendConfirmation(state, configuration.getKeepAliveHeader(), "DEALLOCATE");
                    case CompiledQuery.EXPLAIN ->
                            sendConfirmation(state, configuration.getKeepAliveHeader(), "EXPLAIN");
                    case CompiledQuery.TABLE_RESUME ->
                            sendConfirmation(state, configuration.getKeepAliveHeader(), "TABLE RESUME");
                    case CompiledQuery.TABLE_SUSPEND ->
                            sendConfirmation(state, configuration.getKeepAliveHeader(), "TABLE SUSPEND");
                    case CompiledQuery.TABLE_SET_TYPE ->
                            sendConfirmation(state, configuration.getKeepAliveHeader(), "TABLE SET TYPE");
                    case CompiledQuery.CREATE_USER ->
                            sendConfirmation(state, configuration.getKeepAliveHeader(), "CREATE USER");
                    case CompiledQuery.ALTER_USER ->
                            sendConfirmation(state, configuration.getKeepAliveHeader(), "ALTER USER");
                    case CompiledQuery.CANCEL_QUERY ->
                            sendConfirmation(state, configuration.getKeepAliveHeader(), "CANCEL QUERY");
                    case CompiledQuery.EMPTY -> sendConfirmation(state, configuration.getKeepAliveHeader(), "EMPTY");
                    case CompiledQuery.CREATE_MAT_VIEW ->
                            sendConfirmation(state, configuration.getKeepAliveHeader(), "CREATE MAT VIEW");
                    case CompiledQuery.REFRESH_MAT_VIEW ->
                            sendConfirmation(state, configuration.getKeepAliveHeader(), "REFRESH MAT VIEW");
                    case CompiledQuery.CREATE_VIEW ->
                            sendConfirmation(state, configuration.getKeepAliveHeader(), "CREATE VIEW");
                    case CompiledQuery.ALTER_VIEW ->
                            sendConfirmation(state, configuration.getKeepAliveHeader(), "ALTER VIEW");
                    default -> sendConfirmation(state, configuration.getKeepAliveHeader(), "UNKNOWN");
                }
            } catch (TableReferenceOutOfDateException e) {
                throw SqlException.$(0, e.getFlyweightMessage());
            }
        } finally {
            state.setContainsSecret(sqlExecutionContext.containsSecret());
        }
    }

    private void doResumeSend(
            SqlValidationProcessorState state,
            HttpConnectionContext context
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        LOG.debug().$("resume [fd=").$(context.getFd()).I$();

        final HttpChunkedResponse response = context.getChunkedResponse();
        while (true) {
            try {
                state.resume(response);
                break;
            } catch (SqlException | ImplicitCastException e) {
                sendException(
                        state,
                        e.getPosition(),
                        e.getFlyweightMessage(),
                        HTTP_BAD_REQUEST
                );
                // close the factory on reset instead of caching it
                break;
            } catch (NoSpaceLeftInResponseBufferException ignored) {
                if (response.resetToBookmark()) {
                    response.sendChunk(false);
                } else {
                    // out unit of data, column value or query is larger than response content buffer
                    state.logBufferTooSmall();
                    throw CairoException.nonCritical()
                            .put("response buffer is too small for the column value [columnName=").put(state.getCurrentColumnName())
                            .put(", columnIndex=").put(state.getCurrentColumnIndex())
                            .put(']');
                }
            }
        }
        // reached the end naturally?
        readyForNextRequest(context);
    }

    private void executeNewSelect(
            SqlValidationProcessorState state,
            CompiledQuery cq
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, SqlException {
        RecordCursorFactory factory = cq.getRecordCursorFactory();
        final HttpConnectionContext context = state.getHttpConnectionContext();
        if (!state.of(factory)) {
            readyForNextRequest(context);
            return;
        }

        doResumeSend(state, context);
    }

    private void internalError(
            SqlValidationProcessorState state,
            int httpStatusCode,
            Throwable e
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        HttpConnectionContext context = state.getHttpConnectionContext();
        logInternalError(e, state, context.getMetrics());
        final int errorMessagePosition;
        final CharSequence errorMessage;
        if (e instanceof FlyweightMessageContainer ex) {
            errorMessagePosition = ex.getPosition();
            errorMessage = ex.getFlyweightMessage();
        } else {
            errorMessagePosition = 0;
            errorMessage = e.getMessage();
        }
        sendException(
                state,
                errorMessagePosition,
                errorMessage,
                httpStatusCode
        );
    }

    private boolean parseUrl(
            SqlValidationProcessorState state,
            CharSequence keepAliveHeader
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        // Query text.
        final HttpConnectionContext context = state.getHttpConnectionContext();
        final HttpRequestHeader header = context.getRequestHeader();
        final DirectUtf8Sequence query = header.getUrlParam(URL_PARAM_QUERY);
        if (query == null || query.size() == 0) {
            try {
                state.configure(header, null);
            } catch (Utf8Exception e) {
                // This should never happen.
                // Since we are not parsing query text, we should not have any encoding issues.
            }
            state.info().$("Empty query header received. Sending empty reply.").$();
            final HttpChunkedResponse response = context.getChunkedResponse();
            state.storeEmptyQuery();
            JsonQueryProcessor.header(response, context, keepAliveHeader, 200);
            state.onResumeEmptyQuery(response);
            return false;
        }

        try {
            state.configure(header, query);
        } catch (Utf8Exception e) {
            state.info().$("Bad UTF8 encoding").$();
            HttpChunkedResponse response = context.getChunkedResponse();
            state.storeBadUtf8();
            JsonQueryProcessor.header(response, context, keepAliveHeader, HTTP_BAD_REQUEST);
            state.onResumeBadUtf8(response);
            return false;
        }
        return true;
    }

    private void sendException(
            SqlValidationProcessorState state,
            int errorPosition,
            CharSequence errorMessage,
            int httpStatusCode
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final HttpConnectionContext context = state.getHttpConnectionContext();
        final HttpChunkedResponse response = context.getChunkedResponse();
        state.storeError(errorPosition, errorMessage);
        JsonQueryProcessor.header(response, context, configuration.getKeepAliveHeader(), httpStatusCode);
        state.onResumeError(response);
    }

    static void readyForNextRequest(HttpConnectionContext context) {
        LOG.debug().$("all sent [fd=").$(context.getFd())
                .$(", lastRequestBytesSent=").$(context.getLastRequestBytesSent())
                .$(", nCompletedRequests=").$(context.getNCompletedRequests() + 1)
                .$(", totalBytesSent=").$(context.getTotalBytesSent()).I$();
    }
}
