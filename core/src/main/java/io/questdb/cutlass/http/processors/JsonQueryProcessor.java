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
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.sql.InsertOperation;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cutlass.http.ActiveConnectionTracker;
import io.questdb.cutlass.http.HttpChunkedResponse;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpConstants;
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
import io.questdb.griffin.SqlTimeoutException;
import io.questdb.griffin.engine.ops.Operation;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.NoSpaceLeftInResponseBufferException;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.std.Chars;
import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.Clock;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Path;

import java.io.Closeable;

import static io.questdb.cutlass.http.HttpConstants.URL_PARAM_LIMIT;
import static io.questdb.cutlass.http.HttpConstants.URL_PARAM_QUERY;
import static java.net.HttpURLConnection.*;

public class JsonQueryProcessor implements HttpRequestProcessor, HttpRequestHandler, Closeable {

    private static final Log LOG = LogFactory.getLog(JsonQueryProcessor.class);
    private static final LocalValue<JsonQueryProcessorState> LV = new LocalValue<>();
    protected final ObjList<QueryExecutor> queryExecutors = new ObjList<>();
    private final long asyncCommandTimeout;
    private final long asyncWriterStartTimeout;
    private final JsonQueryProcessorConfiguration configuration;
    private final CairoEngine engine;
    private final int maxSqlRecompileAttempts;
    private final Metrics metrics;
    private final Clock nanosecondClock;
    private final Path path;
    private final byte requiredAuthType;
    private final int sharedWorkerCount;

    public JsonQueryProcessor(
            JsonQueryProcessorConfiguration configuration,
            CairoEngine engine,
            int sharedQueryWorkerCount
    ) {
        try {
            this.configuration = configuration;
            this.path = new Path();
            this.engine = engine;
            this.sharedWorkerCount = sharedQueryWorkerCount;
            requiredAuthType = configuration.getRequiredAuthType();

            final QueryExecutor sendConfirmation = this::updateMetricsAndSendConfirmation;
            this.queryExecutors.extendAndSet(CompiledQuery.SELECT, this::executeNewSelect);
            this.queryExecutors.extendAndSet(CompiledQuery.INSERT, this::executeInsert);
            this.queryExecutors.extendAndSet(CompiledQuery.TRUNCATE, sendConfirmation);
            this.queryExecutors.extendAndSet(CompiledQuery.ALTER, this::executeAlterTable);
            this.queryExecutors.extendAndSet(CompiledQuery.SET, sendConfirmation);
            this.queryExecutors.extendAndSet(CompiledQuery.DROP, this::executeDdl);
            this.queryExecutors.extendAndSet(CompiledQuery.PSEUDO_SELECT, this::executePseudoSelect);
            this.queryExecutors.extendAndSet(CompiledQuery.CREATE_TABLE, this::executeDdl);
            this.queryExecutors.extendAndSet(CompiledQuery.INSERT_AS_SELECT, this::executeInsert);
            this.queryExecutors.extendAndSet(CompiledQuery.COPY_REMOTE, JsonQueryProcessor::cannotCopyRemote);
            this.queryExecutors.extendAndSet(CompiledQuery.RENAME_TABLE, sendConfirmation);
            this.queryExecutors.extendAndSet(CompiledQuery.REPAIR, sendConfirmation);
            this.queryExecutors.extendAndSet(CompiledQuery.UPDATE, this::executeUpdate);
            this.queryExecutors.extendAndSet(CompiledQuery.VACUUM, sendConfirmation);
            this.queryExecutors.extendAndSet(CompiledQuery.BEGIN, sendConfirmation);
            this.queryExecutors.extendAndSet(CompiledQuery.COMMIT, sendConfirmation);
            this.queryExecutors.extendAndSet(CompiledQuery.ROLLBACK, sendConfirmation);
            this.queryExecutors.extendAndSet(CompiledQuery.CREATE_TABLE_AS_SELECT, this::executeDdl);
            this.queryExecutors.extendAndSet(CompiledQuery.CHECKPOINT_CREATE, sendConfirmation);
            this.queryExecutors.extendAndSet(CompiledQuery.CHECKPOINT_RELEASE, sendConfirmation);
            this.queryExecutors.extendAndSet(CompiledQuery.DEALLOCATE, sendConfirmation);
            this.queryExecutors.extendAndSet(CompiledQuery.EXPLAIN, this::executeExplain);
            this.queryExecutors.extendAndSet(CompiledQuery.TABLE_RESUME, sendConfirmation);
            this.queryExecutors.extendAndSet(CompiledQuery.TABLE_SUSPEND, sendConfirmation);
            this.queryExecutors.extendAndSet(CompiledQuery.TABLE_SET_TYPE, sendConfirmation);
            this.queryExecutors.extendAndSet(CompiledQuery.CREATE_USER, sendConfirmation);
            this.queryExecutors.extendAndSet(CompiledQuery.ALTER_USER, sendConfirmation);
            this.queryExecutors.extendAndSet(CompiledQuery.CANCEL_QUERY, sendConfirmation);
            this.queryExecutors.extendAndSet(CompiledQuery.EMPTY, (state, cq, keepAliveHeader) -> sendEmptyQueryNotice(state, keepAliveHeader));
            this.queryExecutors.extendAndSet(CompiledQuery.CREATE_MAT_VIEW, this::executeDdl);
            this.queryExecutors.extendAndSet(CompiledQuery.CREATE_VIEW, this::executeDdl);
            this.queryExecutors.extendAndSet(CompiledQuery.COMPILE_VIEW, sendConfirmation);
            this.queryExecutors.extendAndSet(CompiledQuery.ALTER_VIEW, sendConfirmation);
            this.queryExecutors.extendAndSet(CompiledQuery.REFRESH_MAT_VIEW, sendConfirmation);
            this.queryExecutors.extendAndSet(CompiledQuery.BACKUP_DATABASE, sendConfirmation);

            // Query types start with 1 instead of 0, so we have to add 1 to the expected size.
            assert this.queryExecutors.size() == (CompiledQuery.TYPES_COUNT + 1);
            this.nanosecondClock = configuration.getNanosecondClock();
            this.maxSqlRecompileAttempts = engine.getConfiguration().getMaxSqlRecompileAttempts();
            this.metrics = engine.getMetrics();
            this.asyncWriterStartTimeout = engine.getConfiguration().getWriterAsyncCommandBusyWaitTimeout();
            this.asyncCommandTimeout = engine.getConfiguration().getWriterAsyncCommandMaxTimeout();
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void close() {
        Misc.free(path);
    }

    public void execute0(JsonQueryProcessorState state) throws PeerDisconnectedException, PeerIsSlowToReadException {
        OperationFuture fut = state.getOperationFuture();
        final HttpConnectionContext context = state.getHttpConnectionContext();
        NetworkSqlExecutionCircuitBreaker circuitBreaker = context.getOrCreateCircuitBreaker(engine);
        SqlExecutionContextImpl sqlExecutionContext = context.getOrCreateSqlExecutionContext(engine, sharedWorkerCount);
        sqlExecutionContext.setQueryFutureUpdateListener(configuration.getQueryFutureUpdateListener());
        circuitBreaker.resetTimer();

        if (fut == null) {
            metrics.jsonQueryMetrics().markStart();
            state.startExecutionTimer();
            // do not set random for a new request to avoid copying random from previous request into next one
            // the only time we need to copy random from the state is when we resume request execution
            sqlExecutionContext.with(context.getSecurityContext(), null, null, context.getFd(), circuitBreaker.of(context.getFd()));
            sqlExecutionContext.initNow();
            if (state.getStatementTimeout() > 0L) {
                circuitBreaker.setTimeout(state.getStatementTimeout());
            } else {
                circuitBreaker.resetMaxTimeToDefault();
            }
        }

        try {
            if (fut != null) {
                retryQueryExecution(state, fut);
                return;
            }

            final RecordCursorFactory factory = context.getSelectCache().poll(state.getQuery());
            if (factory != null) {
                // queries with sensitive info are not cached, doLog = true
                try {
                    sqlExecutionContext.storeTelemetry(CompiledQuery.SELECT, TelemetryOrigin.HTTP);
                    executeCachedSelect(state, factory);
                } catch (TableReferenceOutOfDateException e) {
                    LOG.info().$safe(e.getFlyweightMessage()).$();
                    compileAndExecuteQuery(state);
                }
            } else {
                // new query
                compileAndExecuteQuery(state);
            }
        } catch (SqlException | ImplicitCastException e) {
            state.setQueryCacheable(false);
            sendException(
                    state,
                    e.getPosition(),
                    e.getFlyweightMessage(),
                    HTTP_BAD_REQUEST
            );
            readyForNextRequest(context);
        } catch (EntryUnavailableException e) {
            LOG.info().$("[fd=").$(context.getFd()).$("] resource busy, will retry").$();
            throw RetryOperationException.INSTANCE;
        } catch (CairoException e) {
            internalError(
                    context.getChunkedResponse(),
                    context.getLastRequestBytesSent(),
                    e.getFlyweightMessage(),
                    getStatusCode(e),
                    e,
                    state,
                    context.getMetrics()
            );
            readyForNextRequest(context);
        } catch (PeerIsSlowToReadException | PeerDisconnectedException e) {
            // re-throw the exception
            throw e;
        } catch (Throwable e) {
            internalError(
                    context.getChunkedResponse(),
                    context.getLastRequestBytesSent(),
                    e.getMessage(),
                    HTTP_INTERNAL_ERROR,
                    e,
                    state,
                    context.getMetrics()
            );
            readyForNextRequest(context);
        }
    }

    @Override
    public void failRequest(HttpConnectionContext context, HttpException e) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final JsonQueryProcessorState state = LV.get(context);
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
    public String getName() {
        return ActiveConnectionTracker.PROCESSOR_JSON;
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
        JsonQueryProcessorState state = LV.get(context);
        if (state == null) {
            LV.set(context, state = new JsonQueryProcessorState(
                    context,
                    nanosecondClock,
                    configuration.getKeepAliveHeader()
            ));
        }

        // clear random for a new request to avoid reusing random between requests
        state.setRnd(null);

        if (parseUrl(state, configuration.getKeepAliveHeader())) {
            execute0(state);
        } else {
            readyForNextRequest(context);
        }
    }

    @Override
    public void onRequestRetry(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException {
        JsonQueryProcessorState state = LV.get(context);
        execute0(state);
    }

    @Override
    public void parkRequest(HttpConnectionContext context, boolean pausedQuery) {
        final JsonQueryProcessorState state = LV.get(context);
        if (state != null) {
            state.setPausedQuery(pausedQuery);
            // preserve random when we park the context
            SqlExecutionContextImpl sqlExecutionContext = context.getOrCreateSqlExecutionContext(engine, sharedWorkerCount);
            state.setRnd(sqlExecutionContext.getRandom());
        }
    }

    @Override
    public boolean processServiceAccountCookie(HttpConnectionContext context, SecurityContext securityContext) {
        return context.getCookieHandler().processServiceAccountCookie(context, securityContext);
    }

    @Override
    public boolean reservedOneAdminConnection() {
        return true;
    }

    @Override
    public void resumeSend(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final JsonQueryProcessorState state = LV.get(context);
        if (state != null) {
            // we are resuming request execution, we need to copy random to execution context
            NetworkSqlExecutionCircuitBreaker circuitBreaker = context.getOrCreateCircuitBreaker(engine);
            SqlExecutionContextImpl sqlExecutionContext = context.getOrCreateSqlExecutionContext(engine, sharedWorkerCount);
            sqlExecutionContext.with(context.getSecurityContext(), null, state.getRnd(), context.getFd(), circuitBreaker.of(context.getFd()));
            if (!state.isPausedQuery()) {
                context.resumeResponseSend();
            } else {
                state.setPausedQuery(false);
            }
            try {
                doResumeSend(state, context);
            } catch (CairoError e) {
                internalError(
                        context.getChunkedResponse(),
                        context.getLastRequestBytesSent(),
                        e.getFlyweightMessage(),
                        HTTP_INTERNAL_ERROR,
                        e,
                        state,
                        context.getMetrics()
                );
            } catch (CairoException e) {
                internalError(
                        context.getChunkedResponse(),
                        context.getLastRequestBytesSent(),
                        e.getFlyweightMessage(),
                        HTTP_BAD_REQUEST,
                        e,
                        state,
                        context.getMetrics()
                );
            }
        }
    }

    private static void cannotCopyRemote(
            JsonQueryProcessorState state,
            CompiledQuery cc,
            CharSequence keepAliveHeader
    ) throws SqlException {
        throw SqlException.$(0, "copy from STDIN is not supported over REST");
    }

    private static int getStatusCode(CairoException e) {
        if (e.isAuthorizationError()) {
            return HTTP_FORBIDDEN;
        }
        return HTTP_BAD_REQUEST;
    }

    private static void logInternalError(
            Throwable e,
            JsonQueryProcessorState state,
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

    private static void readyForNextRequest(HttpConnectionContext context) {
        LOG.debug().$("all sent [fd=").$(context.getFd())
                .$(", lastRequestBytesSent=").$(context.getLastRequestBytesSent())
                .$(", nCompletedRequests=").$(context.getNCompletedRequests() + 1)
                .$(", totalBytesSent=").$(context.getTotalBytesSent()).I$();
    }

    private static void sendConfirmation(
            JsonQueryProcessorState state,
            CharSequence keepAliveHeader
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final HttpConnectionContext context = state.getHttpConnectionContext();
        final HttpChunkedResponse response = context.getChunkedResponse();

        state.storeConfirmation();
        header(response, context, keepAliveHeader, 200);
        state.onResumeConfirmation(response);
    }

    private static void sendEmptyQueryNotice(
            JsonQueryProcessorState state,
            CharSequence keepAliveHeader
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final HttpConnectionContext context = state.getHttpConnectionContext();
        final HttpChunkedResponse response = context.getChunkedResponse();

        state.storeEmptyQuery();
        header(response, context, keepAliveHeader, 200);
        state.onResumeEmptyQuery(response);
    }

    private static void sendInsertConfirmation(
            JsonQueryProcessorState state,
            CharSequence keepAliveHeader
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final HttpConnectionContext context = state.getHttpConnectionContext();
        final HttpChunkedResponse response = context.getChunkedResponse();

        state.storeInsertConfirmation();
        header(response, context, keepAliveHeader, 200);
        state.onResumeInsertConfirmation(response);
    }

    private static void sendUpdateConfirmation(
            JsonQueryProcessorState state,
            CharSequence keepAliveHeader,
            long updateRecords
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final HttpConnectionContext context = state.getHttpConnectionContext();
        final HttpChunkedResponse response = context.getChunkedResponse();

        state.storeUpdateConfirmation(updateRecords);
        header(response, context, keepAliveHeader, 200);
        state.onResumeUpdateConfirmation(response);
    }

    private void compileAndExecuteQuery(
            JsonQueryProcessorState state
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, SqlException {
        SqlExecutionContextImpl sqlExecutionContext = state.getHttpConnectionContext().getOrCreateSqlExecutionContext(engine, sharedWorkerCount);
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            for (int retries = 0; ; retries++) {
                final long compilationStart = nanosecondClock.getTicks();
                final CompiledQuery cc = compiler.compile(state.getQuery(), sqlExecutionContext);
                sqlExecutionContext.storeTelemetry(cc.getType(), TelemetryOrigin.HTTP);
                state.setCompilerNanos(nanosecondClock.getTicks() - compilationStart);
                state.setQueryType(cc.getType());
                // todo: reconsider whether we need to keep the SqlCompiler instance open while executing the query
                // the problem is the each instance of the compiler has just a single instance of the CompilerQuery object.
                // the CompilerQuery is used as a flyweight(?) and we cannot return the SqlCompiler instance to the pool
                // until we extract the result from the CompilerQuery.
                try {
                    queryExecutors.getQuick(cc.getType()).execute(
                            state,
                            cc,
                            configuration.getKeepAliveHeader()
                    );
                    break;
                } catch (TableReferenceOutOfDateException e) {
                    if (retries == maxSqlRecompileAttempts) {
                        throw SqlException.$(0, e.getFlyweightMessage());
                    }
                    LOG.info().$safe(e.getFlyweightMessage()).$();
                    // will recompile
                }
            }
        } finally {
            state.setContainsSecret(sqlExecutionContext.containsSecret());
        }
    }

    private void doResumeSend(
            JsonQueryProcessorState state,
            HttpConnectionContext context
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        LOG.debug().$("resume [fd=").$(context.getFd()).I$();

        final HttpChunkedResponse response = context.getChunkedResponse();
        while (true) {
            try {
                state.resume(response);
                break;
            } catch (SqlException | ImplicitCastException e) {
                // close the factory on reset instead of caching it
                state.setQueryCacheable(false);
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

    private void executeAlterTable(
            JsonQueryProcessorState state,
            CompiledQuery cq,
            CharSequence keepAliveHeader
    ) throws PeerIsSlowToReadException, PeerDisconnectedException, SqlException {
        OperationFuture fut = null;
        try {
            fut = cq.execute(state.getEventSubSequence());
            int waitResult = fut.await(getAsyncWriterStartTimeout(state));
            if (waitResult != OperationFuture.QUERY_COMPLETE) {
                state.setOperationFuture(fut);
                fut = null;
                throw EntryUnavailableException.instance("retry alter table wait");
            }
        } finally {
            if (fut != null) {
                fut.close();
            }
        }
        metrics.jsonQueryMetrics().markComplete();
        sendConfirmation(state, keepAliveHeader);
    }

    private void executeCachedSelect(
            JsonQueryProcessorState state,
            RecordCursorFactory factory
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, SqlException {
        state.setCompilerNanos(0);
        SqlExecutionContextImpl sqlExecutionContext = state.getHttpConnectionContext().getOrCreateSqlExecutionContext(engine, sharedWorkerCount);
        sqlExecutionContext.setCacheHit(true);
        executeSelect0(state, factory, true);
    }

    private void executeDdl(
            JsonQueryProcessorState state,
            CompiledQuery cq,
            CharSequence keepAliveHeader
    ) throws PeerIsSlowToReadException, PeerDisconnectedException, SqlException {
        SqlExecutionContextImpl sqlExecutionContext = state.getHttpConnectionContext().getOrCreateSqlExecutionContext(engine, sharedWorkerCount);
        Operation op = cq.getOperation();
        try (OperationFuture fut = op.execute(sqlExecutionContext, state.getEventSubSequence())) {
            int waitResult = fut.await(getAsyncWriterStartTimeout(state));
            if (waitResult != OperationFuture.QUERY_COMPLETE) {
                state.setOperation(op);
                // clear operation to report to avoid closing it
                op = null;
                throw EntryUnavailableException.instance("retry alter table wait");
            }
        } finally {
            Misc.free(op);
        }
        metrics.jsonQueryMetrics().markComplete();
        sendConfirmation(state, keepAliveHeader);
    }

    // same as select new but disallows caching of EXPLAIN plans
    private void executeExplain(
            JsonQueryProcessorState state,
            CompiledQuery cq,
            CharSequence keepAliveHeader
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, SqlException {
        executeSelect0(state, cq.getRecordCursorFactory(), false);
    }

    private void executeInsert(
            JsonQueryProcessorState state,
            CompiledQuery cq,
            CharSequence keepAliveHeader
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, SqlException {
        SqlExecutionContextImpl sqlExecutionContext = state.getHttpConnectionContext().getOrCreateSqlExecutionContext(engine, sharedWorkerCount);
        try (InsertOperation insert = cq.popInsertOperation()) {
            insert.execute(sqlExecutionContext).await();
            metrics.jsonQueryMetrics().markComplete();
            sendInsertConfirmation(state, keepAliveHeader);
        }
    }

    private void executeNewSelect(
            JsonQueryProcessorState state,
            CompiledQuery cq,
            CharSequence keepAliveHeader
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, SqlException {
        executeSelect0(state, cq.getRecordCursorFactory(), cq.isCacheable());
    }

    private void executePseudoSelect(
            JsonQueryProcessorState state,
            CompiledQuery cq,
            CharSequence keepAliveHeader
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, SqlException {
        final RecordCursorFactory factory = cq.getRecordCursorFactory();
        if (factory == null) {
            updateMetricsAndSendConfirmation(state, cq, keepAliveHeader);
            return;
        }

        // new import case
        executeSelect0(state, factory, false);
    }

    private void executeSelect0(
            JsonQueryProcessorState state,
            RecordCursorFactory factory,
            boolean queryCacheable
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, SqlException {
        final HttpConnectionContext context = state.getHttpConnectionContext();
        SqlExecutionContextImpl sqlExecutionContext = context.getOrCreateSqlExecutionContext(engine, sharedWorkerCount);
        if (!state.of(factory, queryCacheable)) {
            readyForNextRequest(context);
            return;
        }

        final RecordCursor cursor;
        try {
            cursor = factory.getCursor(sqlExecutionContext);
        } catch (Throwable th) {
            // clear factory in the state because we already set it
            state.clearFactory();
            throw th;
        }

        try {
            state.setCursor(cursor);
            doResumeSend(state, context);
            metrics.jsonQueryMetrics().markComplete();
        } catch (CairoException ex) {
            state.setQueryCacheable(queryCacheable && ex.isCacheable());
            throw ex;
        }
    }

    private void executeUpdate(
            JsonQueryProcessorState state,
            CompiledQuery cq,
            CharSequence keepAliveHeader
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, SqlException {
        HttpConnectionContext context = state.getHttpConnectionContext();
        NetworkSqlExecutionCircuitBreaker circuitBreaker = context.getOrCreateCircuitBreaker(engine);
        SqlExecutionContextImpl sqlExecutionContext = context.getOrCreateSqlExecutionContext(engine, sharedWorkerCount);
        circuitBreaker.resetTimer();
        sqlExecutionContext.initNow();
        OperationFuture fut = null;
        boolean isAsyncWait = false;
        try {
            fut = cq.execute(sqlExecutionContext, state.getEventSubSequence(), true);
            int waitResult = fut.await(getAsyncWriterStartTimeout(state));
            if (waitResult != OperationFuture.QUERY_COMPLETE) {
                isAsyncWait = true;
                state.setOperationFuture(fut);
                throw EntryUnavailableException.instance("retry update table wait");
            }
            // All good, finished updates
            final long updatedCount = fut.getAffectedRowsCount();
            metrics.jsonQueryMetrics().markComplete();
            sendUpdateConfirmation(state, keepAliveHeader, updatedCount);
        } catch (CairoException e) {
            // close e.g., when the query has been canceled, or we got an OOM
            if (e.isInterruption() || e.isOutOfMemory()) {
                Misc.free(cq.getUpdateOperation());
            }
            throw e;
        } finally {
            if (!isAsyncWait && fut != null) {
                fut.close();
            }
        }
    }

    private long getAsyncWriterStartTimeout(JsonQueryProcessorState state) {
        return Math.min(asyncWriterStartTimeout, state.getStatementTimeout());
    }

    private void internalError(
            HttpChunkedResponse response,
            long bytesSent,
            CharSequence message,
            int code,
            Throwable e,
            JsonQueryProcessorState state,
            Metrics metrics
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        logInternalError(e, state, metrics);
        final int errorMessagePosition;
        final CharSequence errorMessage;
        if (e instanceof FlyweightMessageContainer ex) {
            errorMessagePosition = ex.getPosition();
            errorMessage = ex.getFlyweightMessage();
        } else {
            errorMessagePosition = 0;
            errorMessage = e.getMessage();
        }
        if (bytesSent > 0) {
            state.querySuffixWithError(response, code, message, errorMessagePosition);
        } else {
            sendException(
                    state,
                    errorMessagePosition,
                    errorMessage,
                    code
            );
        }
    }

    private boolean parseUrl(
            JsonQueryProcessorState state,
            CharSequence keepAliveHeader
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        // Query text.
        final HttpConnectionContext context = state.getHttpConnectionContext();
        final HttpRequestHeader header = context.getRequestHeader();
        final DirectUtf8Sequence query = header.getUrlParam(URL_PARAM_QUERY);
        if (query == null || query.size() == 0) {
            try {
                state.configure(header, null, 0, Long.MAX_VALUE);
            } catch (Utf8Exception e) {
                // This should never happen.
                // Since we are not parsing query text, we should not have any encoding issues.
            }
            state.info().$("Empty query header received. Sending empty reply.").$();
            sendEmptyQueryNotice(state, keepAliveHeader);
            return false;
        }

        // Url Params.
        long skip = 0;
        long stop = Long.MAX_VALUE;

        DirectUtf8Sequence limit = header.getUrlParam(URL_PARAM_LIMIT);
        if (limit != null) {
            int sepPos = Chars.indexOf(limit.asAsciiCharSequence(), ',');
            try {
                if (sepPos > 0) {
                    skip = Numbers.parseLong(limit, 0, sepPos) - 1;
                    if (sepPos + 1 < limit.size()) {
                        stop = Numbers.parseLong(limit, sepPos + 1, limit.size());
                    }
                } else {
                    stop = Numbers.parseLong(limit);
                }
            } catch (NumericException ex) {
                // Skip or stop will have the default value.
            }
        }
        if (stop < 0) {
            stop = 0;
        }

        if (skip < 0) {
            skip = 0;
        }

        if ((stop - skip) > configuration.getMaxQueryResponseRowLimit()) {
            stop = skip + configuration.getMaxQueryResponseRowLimit();
        }

        try {
            state.configure(header, query, skip, stop);
        } catch (Utf8Exception e) {
            state.info().$("Bad UTF8 encoding").$();
            HttpChunkedResponse response = context.getChunkedResponse();
            state.storeBadUtf8();
            header(response, context, keepAliveHeader, HTTP_BAD_REQUEST);
            state.onResumeBadUtf8(response);
            return false;
        }
        return true;
    }

    private void retryQueryExecution(
            JsonQueryProcessorState state,
            OperationFuture fut
    ) throws PeerIsSlowToReadException, PeerDisconnectedException, SqlException {
        final int waitResult;
        try {
            waitResult = fut.await(0);
        } catch (TableReferenceOutOfDateException e) {
            state.freeAsyncOperation();
            compileAndExecuteQuery(state);
            return;
        }

        if (waitResult != OperationFuture.QUERY_COMPLETE) {
            long timeout = state.getStatementTimeout() > 0 ? state.getStatementTimeout() : asyncCommandTimeout;
            if (state.getExecutionTimeNanos() / 1_000_000L < timeout) {
                // Schedule a retry
                throw EntryUnavailableException.instance("wait for update query");
            } else {
                state.freeAsyncOperation();
                throw SqlTimeoutException.timeout("Query timeout. Please add HTTP header 'Statement-Timeout' with timeout in ms [timeout=").put(timeout).put("ms]");
            }
        } else {
            // Done
            state.freeAsyncOperation();
            if (state.getQueryType() == CompiledQuery.UPDATE) {
                sendUpdateConfirmation(state, configuration.getKeepAliveHeader(), fut.getAffectedRowsCount());
            } else {
                // Alter, sends ddl:OK
                sendConfirmation(state, configuration.getKeepAliveHeader());
            }
        }
    }

    private void updateMetricsAndSendConfirmation(
            JsonQueryProcessorState state,
            CompiledQuery cq,
            CharSequence keepAliveHeader
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        metrics.jsonQueryMetrics().markComplete();
        sendConfirmation(state, keepAliveHeader);
    }

    protected static void header(
            HttpChunkedResponse response,
            HttpConnectionContext context,
            CharSequence keepAliveHeader,
            int statusCode
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        // State management note: We assume our response sink buffer is large enough to accommodate for all
        // HTTP response headers. In other words - this method never throws the NoSpaceLeftInResponseBufferException
        // exception.
        // It can still throw PeerIsSlowToReadException when there is no space in TCP send buffer! In practice, it means
        // the processor should advance its state BEFORE calling this method.

        response.status(statusCode, HttpConstants.CONTENT_TYPE_JSON);
        response.headers().setKeepAlive(keepAliveHeader);
        context.getCookieHandler().setServiceAccountCookie(response.headers(), context.getSecurityContext());
        final CharSequence sessionId = context.getSessionIdSink();
        if (!sessionId.isEmpty()) {
            context.getCookieHandler().setSessionCookie(response.headers(), sessionId);
        }
        response.sendHeader();
    }

    void sendException(
            JsonQueryProcessorState state,
            int position,
            CharSequence message,
            int code
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final HttpConnectionContext context = state.getHttpConnectionContext();
        final HttpChunkedResponse response = context.getChunkedResponse();

        state.storeError(position, message);
        header(response, context, configuration.getKeepAliveHeader(), code);
        state.onResumeError(response);
    }

    @FunctionalInterface
    public interface QueryExecutor {
        void execute(
                JsonQueryProcessorState state,
                CompiledQuery cc,
                CharSequence keepAliveHeader
        ) throws PeerDisconnectedException, PeerIsSlowToReadException, SqlException;
    }
}
