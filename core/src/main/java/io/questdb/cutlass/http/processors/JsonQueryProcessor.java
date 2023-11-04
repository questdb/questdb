/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.QueryLogger;
import io.questdb.TelemetryOrigin;
import io.questdb.cairo.*;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cutlass.http.*;
import io.questdb.cutlass.http.ex.RetryOperationException;
import io.questdb.cutlass.text.Utf8Exception;
import io.questdb.griffin.*;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.*;
import io.questdb.std.*;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

import static io.questdb.cutlass.http.HttpConstants.URL_PARAM_LIMIT;
import static io.questdb.cutlass.http.HttpConstants.URL_PARAM_QUERY;

public class JsonQueryProcessor implements HttpRequestProcessor, Closeable {

    private static final Log LOG = LogFactory.getLog(JsonQueryProcessor.class);
    private static final LocalValue<JsonQueryProcessorState> LV = new LocalValue<>();
    protected final ObjList<QueryExecutor> queryExecutors = new ObjList<>();
    private final long asyncCommandTimeout;
    private final long asyncWriterStartTimeout;
    private final NetworkSqlExecutionCircuitBreaker circuitBreaker;
    private final JsonQueryProcessorConfiguration configuration;
    private final CairoEngine engine;
    private final Metrics metrics;
    private final NanosecondClock nanosecondClock;
    private final Path path = new Path();
    private final QueryLogger queryLogger;
    private final SqlExecutionContextImpl sqlExecutionContext;

    @TestOnly
    public JsonQueryProcessor(
            JsonQueryProcessorConfiguration configuration,
            CairoEngine engine,
            int workerCount
    ) {
        this(configuration, engine, workerCount, workerCount);
    }

    public JsonQueryProcessor(
            JsonQueryProcessorConfiguration configuration,
            CairoEngine engine,
            int workerCount,
            int sharedWorkerCount
    ) {
        this(
                configuration,
                engine,
                new SqlExecutionContextImpl(engine, workerCount, sharedWorkerCount)
        );
    }

    public JsonQueryProcessor(
            JsonQueryProcessorConfiguration configuration,
            CairoEngine engine,
            SqlExecutionContextImpl sqlExecutionContext
    ) {
        this.configuration = configuration;
        this.engine = engine;
        queryLogger = engine.getConfiguration().getQueryLogger();
        final QueryExecutor sendConfirmation = this::updateMetricsAndSendConfirmation;
        this.queryExecutors.extendAndSet(CompiledQuery.SELECT, this::executeNewSelect);
        this.queryExecutors.extendAndSet(CompiledQuery.INSERT, this::executeInsert);
        this.queryExecutors.extendAndSet(CompiledQuery.TRUNCATE, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.ALTER, this::executeAlterTable);
        this.queryExecutors.extendAndSet(CompiledQuery.SET, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.DROP, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.PSEUDO_SELECT, this::executePseudoSelect);
        this.queryExecutors.extendAndSet(CompiledQuery.CREATE_TABLE, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.INSERT_AS_SELECT, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.COPY_REMOTE, JsonQueryProcessor::cannotCopyRemote);
        this.queryExecutors.extendAndSet(CompiledQuery.RENAME_TABLE, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.REPAIR, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.BACKUP_TABLE, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.UPDATE, this::executeUpdate);
        this.queryExecutors.extendAndSet(CompiledQuery.VACUUM, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.BEGIN, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.COMMIT, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.ROLLBACK, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.CREATE_TABLE_AS_SELECT, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.SNAPSHOT_DB_PREPARE, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.SNAPSHOT_DB_COMPLETE, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.DEALLOCATE, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.EXPLAIN, this::executeExplain);
        this.queryExecutors.extendAndSet(CompiledQuery.TABLE_RESUME, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.TABLE_SET_TYPE, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.CREATE_USER, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.ALTER_USER, sendConfirmation);
        // Query types start with 1 instead of 0, so we have to add 1 to the expected size.
        assert this.queryExecutors.size() == (CompiledQuery.TYPES_COUNT + 1);
        this.sqlExecutionContext = sqlExecutionContext;
        this.nanosecondClock = engine.getConfiguration().getNanosecondClock();
        this.circuitBreaker = new NetworkSqlExecutionCircuitBreaker(engine.getConfiguration().getCircuitBreakerConfiguration(), MemoryTag.NATIVE_CB3);
        this.metrics = engine.getMetrics();
        this.asyncWriterStartTimeout = engine.getConfiguration().getWriterAsyncCommandBusyWaitTimeout();
        this.asyncCommandTimeout = engine.getConfiguration().getWriterAsyncCommandMaxTimeout();
    }

    @Override
    public void close() {
//        Misc.free(compiler);
        Misc.free(path);
        Misc.free(circuitBreaker);
    }

    public void execute0(
            JsonQueryProcessorState state
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException, QueryPausedException {

        OperationFuture fut = state.getOperationFuture();
        final HttpConnectionContext context = state.getHttpConnectionContext();
        circuitBreaker.resetTimer();

        if (fut == null) {
            metrics.jsonQuery().markStart();
            state.startExecutionTimer();
            // do not set random for new request to avoid copying random from previous request into next one
            // the only time we need to copy random from state is when we resume request execution
            sqlExecutionContext.with(context.getSecurityContext(), null, null, context.getFd(), circuitBreaker.of(context.getFd()));
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
                queryLogger.logExecQuery(LOG, true, context.getFd(), state.getQuery(), context.getSecurityContext());
                try {
                    sqlExecutionContext.storeTelemetry(CompiledQuery.SELECT, TelemetryOrigin.HTTP_JSON);
                    executeCachedSelect(
                            state,
                            factory,
                            configuration.getKeepAliveHeader()
                    );
                } catch (TableReferenceOutOfDateException e) {
                    LOG.info().$(e.getFlyweightMessage()).$();
                    Misc.free(factory);
                    compileQuery(state);
                }
            } else {
                // new query
                compileQuery(state);
            }
        } catch (SqlException | ImplicitCastException e) {
            sqlError(context.getChunkedResponseSocket(), state, e, configuration.getKeepAliveHeader());
            readyForNextRequest(context);
        } catch (EntryUnavailableException e) {
            LOG.info().$("[fd=").$(context.getFd()).$("] resource busy, will retry").$();
            throw RetryOperationException.INSTANCE;
        } catch (DataUnavailableException e) {
            LOG.info().$("[fd=").$(context.getFd()).$("] data is in cold storage, will retry").$();
            throw QueryPausedException.instance(e.getEvent(), sqlExecutionContext.getCircuitBreaker());
        } catch (CairoError e) {
            internalError(context.getChunkedResponseSocket(), context.getLastRequestBytesSent(), e.getFlyweightMessage(),
                    400, e, state, context.getMetrics());
            readyForNextRequest(context);
        } catch (CairoException e) {
            internalError(context.getChunkedResponseSocket(), context.getLastRequestBytesSent(), e.getFlyweightMessage(),
                    e.isAuthorizationError() ? 403 : 400, e, state, context.getMetrics());
            readyForNextRequest(context);
            if (e.isEntityDisabled()) {
                throw ServerDisconnectException.INSTANCE;
            }
        } catch (PeerIsSlowToReadException | PeerDisconnectedException | QueryPausedException e) {
            // re-throw the exception
            throw e;
        } catch (Throwable e) {
            state.critical().$("Uh-oh. Error!").$(e).$();
            throw ServerDisconnectException.INSTANCE;
        }
    }

    @Override
    public void failRequest(HttpConnectionContext context, HttpException e) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final JsonQueryProcessorState state = LV.get(context);
        final HttpChunkedResponseSocket socket = context.getChunkedResponseSocket();
        logInternalError(e, state, metrics);
        sendException(socket, context, 0, e.getFlyweightMessage(), state.getQuery(), configuration.getKeepAliveHeader(), 400);
        socket.shutdownWrite();
    }

    @Override
    public void onRequestComplete(
            HttpConnectionContext context
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException, QueryPausedException {
        JsonQueryProcessorState state = LV.get(context);
        if (state == null) {
            LV.set(context, state = new JsonQueryProcessorState(
                    context,
                    nanosecondClock,
                    configuration.getFloatScale(),
                    configuration.getDoubleScale()
            ));
        }

        // clear random for new request to avoid reusing random between requests
        state.setRnd(null);

        if (parseUrl(state, configuration.getKeepAliveHeader())) {
            execute0(state);
        } else {
            readyForNextRequest(context);
        }
    }

    @Override
    public void onRequestRetry(
            HttpConnectionContext context
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException, QueryPausedException {
        JsonQueryProcessorState state = LV.get(context);
        execute0(state);
    }

    @Override
    public void parkRequest(HttpConnectionContext context, boolean pausedQuery) {
        final JsonQueryProcessorState state = LV.get(context);
        if (state != null) {
            state.setPausedQuery(pausedQuery);
            // preserve random when we park the context
            state.setRnd(sqlExecutionContext.getRandom());
        }
    }

    @Override
    public void resumeSend(
            HttpConnectionContext context
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException, QueryPausedException {
        final JsonQueryProcessorState state = LV.get(context);
        if (state != null) {
            // we are resuming request execution, we need to copy random to execution context
            sqlExecutionContext.with(context.getSecurityContext(), null, state.getRnd(), context.getFd(), circuitBreaker.of(context.getFd()));
            if (!state.isPausedQuery()) {
                context.resumeResponseSend();
            } else {
                state.setPausedQuery(false);
            }
            try {
                doResumeSend(state, context, sqlExecutionContext);
            } catch (CairoError | CairoException e) {
                // this is something we didn't expect
                // log the exception and disconnect
                logInternalError(e, state, context.getMetrics());
                throw ServerDisconnectException.INSTANCE;
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

    private static void doResumeSend(
            JsonQueryProcessorState state,
            HttpConnectionContext context,
            SqlExecutionContext sqlExecutionContext
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, QueryPausedException {
        LOG.debug().$("resume [fd=").$(context.getFd()).I$();

        final HttpChunkedResponseSocket socket = context.getChunkedResponseSocket();
        while (true) {
            try {
                state.resume(socket);
                break;
            } catch (DataUnavailableException e) {
                socket.resetToBookmark();
                throw QueryPausedException.instance(e.getEvent(), sqlExecutionContext.getCircuitBreaker());
            } catch (NoSpaceLeftInResponseBufferException ignored) {
                if (socket.resetToBookmark()) {
                    socket.sendChunk(false);
                } else {
                    // what we have here is out unit of data, column value or query
                    // is larger that response content buffer
                    // all we can do in this scenario is to log appropriately
                    // and disconnect socket
                    state.logBufferTooSmall();
                    throw PeerDisconnectedException.INSTANCE;
                }
            }
        }
        // reached the end naturally?
        readyForNextRequest(context);
    }

    private static void logInternalError(
            Throwable e,
            JsonQueryProcessorState state,
            Metrics metrics
    ) {
        if (e instanceof CairoException) {
            CairoException ce = (CairoException) e;
            if (ce.isInterruption()) {
                state.info().$("query cancelled [reason=`").$(((CairoException) e).getFlyweightMessage())
                        .$("`, q=`").utf8(state.getQueryOrHidden())
                        .$("`]").$();
            } else if (ce.isCritical()) {
                state.critical().$("error [msg=`").$(ce.getFlyweightMessage())
                        .$("`, errno=").$(ce.getErrno())
                        .$(", q=`").utf8(state.getQueryOrHidden())
                        .$("`]").$();
            } else {
                state.error().$("error [msg=`").$(ce.getFlyweightMessage())
                        .$("`, errno=").$(ce.getErrno())
                        .$(", q=`").utf8(state.getQueryOrHidden())
                        .$("`]").$();
            }
        } else if (e instanceof HttpException) {
            state.error().$("internal HTTP server error [reason=`").$(((HttpException) e).getFlyweightMessage())
                    .$("`, q=`").utf8(state.getQueryOrHidden())
                    .$("`]").$();
        } else {
            state.critical().$("internal error [ex=").$(e)
                    .$(", q=`").utf8(state.getQueryOrHidden())
                    .$("`]").$();
            // This is a critical error, so we treat it as an unhandled one.
            metrics.health().incrementUnhandledErrors();
        }
    }

    private static void readyForNextRequest(HttpConnectionContext context) {
        LOG.info().$("all sent [fd=").$(context.getFd())
                .$(", lastRequestBytesSent=").$(context.getLastRequestBytesSent())
                .$(", nCompletedRequests=").$(context.getNCompletedRequests() + 1)
                .$(", totalBytesSent=").$(context.getTotalBytesSent()).I$();
    }

    private static void sendConfirmation(
            JsonQueryProcessorState state,
            CharSequence keepAliveHeader
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final HttpConnectionContext context = state.getHttpConnectionContext();
        final HttpChunkedResponseSocket socket = context.getChunkedResponseSocket();
        header(socket, context, keepAliveHeader, 200);
        socket.put('{')
                .putQuoted("ddl").put(':').putQuoted("OK")
                .put('}');
        socket.sendChunk(true);
        readyForNextRequest(context);
    }

    private static void sendUpdateConfirmation(
            JsonQueryProcessorState state,
            CharSequence keepAliveHeader,
            long updateRecords
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final HttpConnectionContext context = state.getHttpConnectionContext();
        final HttpChunkedResponseSocket socket = context.getChunkedResponseSocket();
        header(socket, context, keepAliveHeader, 200);
        socket.put('{')
                .putQuoted("ddl").put(':').putQuoted("OK").put(',')
                .putQuoted("updated").put(':').put(updateRecords)
                .put('}');
        socket.sendChunk(true);
        readyForNextRequest(context);
    }

    private static void sqlError(
            HttpChunkedResponseSocket socket,
            JsonQueryProcessorState state,
            FlyweightMessageContainer container,
            CharSequence keepAliveHeader
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        state.logSqlError(container);
        sendException(
                socket,
                state.getHttpConnectionContext(),
                container.getPosition(),
                container.getFlyweightMessage(),
                state.getQuery(),
                keepAliveHeader,
                400
        );
    }

    private void compileQuery(
            JsonQueryProcessorState state
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, QueryPausedException, SqlException {
        boolean recompileStale = true;
        for (int retries = 0; recompileStale; retries++) {
            try {
                final long nanos = nanosecondClock.getTicks();
                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    final CompiledQuery cc = compiler.compile(state.getQuery(), sqlExecutionContext);
                    sqlExecutionContext.storeTelemetry(cc.getType(), TelemetryOrigin.HTTP_JSON);
                    state.setCompilerNanos(nanosecondClock.getTicks() - nanos);
                    state.setQueryType(cc.getType());
                    // todo: reconsider whether we need to keep the SqlCompiler instance open while executing the query
                    // the problem is the each instance of the compiler has just a single instance of the CompilerQuery object.
                    // the CompilerQuery is used as a flyweight(?) and we cannot return the SqlCompiler instance to the pool
                    // until we extract the result from the CompilerQuery.
                    queryExecutors.getQuick(cc.getType()).execute(
                            state,
                            cc,
                            configuration.getKeepAliveHeader()
                    );
                }
                recompileStale = false;
            } catch (TableReferenceOutOfDateException e) {
                if (retries == TableReferenceOutOfDateException.MAX_RETRY_ATTEMPS) {
                    throw e;
                }
                LOG.info().$(e.getFlyweightMessage()).$();
                // will recompile
            } finally {
                state.setContainsSecret(sqlExecutionContext.containsSecret());
            }
        }
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
        metrics.jsonQuery().markComplete();
        sendConfirmation(state, keepAliveHeader);
    }

    private void executeCachedSelect(
            JsonQueryProcessorState state,
            RecordCursorFactory factory,
            CharSequence keepAliveHeader
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, QueryPausedException, SqlException {
        state.setCompilerNanos(0);
        state.logExecuteCached();
        executeSelect(state, factory, keepAliveHeader);
    }

    //same as for select new but disallows caching of explain plans
    private void executeExplain(
            JsonQueryProcessorState state,
            CompiledQuery cq,
            CharSequence keepAliveHeader
    )
            throws PeerDisconnectedException, PeerIsSlowToReadException, QueryPausedException, SqlException {
        state.logExecuteNew();
        final RecordCursorFactory factory = cq.getRecordCursorFactory();
        final HttpConnectionContext context = state.getHttpConnectionContext();
        try {
            if (state.of(factory, false, sqlExecutionContext)) {
                header(context.getChunkedResponseSocket(), context, keepAliveHeader, 200);
                doResumeSend(state, context, sqlExecutionContext);
                metrics.jsonQuery().markComplete();
            } else {
                readyForNextRequest(context);
            }
        } catch (CairoException ex) {
            state.setQueryCacheable(ex.isCacheable());
            throw ex;
        }
    }

    private void executeInsert(
            JsonQueryProcessorState state,
            CompiledQuery cq,
            CharSequence keepAliveHeader
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, SqlException {
        cq.getInsertOperation().execute(sqlExecutionContext).await();
        metrics.jsonQuery().markComplete();
        sendConfirmation(state, keepAliveHeader);
    }

    private void executeNewSelect(
            JsonQueryProcessorState state,
            CompiledQuery cq,
            CharSequence keepAliveHeader
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, QueryPausedException, SqlException {
        state.logExecuteNew();
        final RecordCursorFactory factory = cq.getRecordCursorFactory();
        executeSelect(
                state,
                factory,
                keepAliveHeader
        );
    }

    private void executePseudoSelect(
            JsonQueryProcessorState state,
            CompiledQuery cq,
            CharSequence keepAliveHeader
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, QueryPausedException, SqlException {
        final RecordCursorFactory factory = cq.getRecordCursorFactory();
        if (factory == null) {
            updateMetricsAndSendConfirmation(state, cq, keepAliveHeader);
            return;
        }
        // new import case
        final HttpConnectionContext context = state.getHttpConnectionContext();
        // Make sure to mark the query as non-cacheable.
        if (state.of(factory, false, sqlExecutionContext)) {
            header(context.getChunkedResponseSocket(), context, keepAliveHeader, 200);
            doResumeSend(state, context, sqlExecutionContext);
            metrics.jsonQuery().markComplete();
        } else {
            readyForNextRequest(context);
        }
    }

    private void executeSelect(
            JsonQueryProcessorState state,
            RecordCursorFactory factory,
            CharSequence keepAliveHeader
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, QueryPausedException, SqlException {
        final HttpConnectionContext context = state.getHttpConnectionContext();
        try {
            if (state.of(factory, sqlExecutionContext)) {
                header(context.getChunkedResponseSocket(), context, keepAliveHeader, 200);
                doResumeSend(state, context, sqlExecutionContext);
                metrics.jsonQuery().markComplete();
            } else {
                readyForNextRequest(context);
            }
        } catch (CairoException ex) {
            state.setQueryCacheable(ex.isCacheable());
            throw ex;
        }
    }

    private void executeUpdate(
            JsonQueryProcessorState state,
            CompiledQuery cq,
            CharSequence keepAliveHeader
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, SqlException {
        circuitBreaker.resetTimer();
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
            // All good, finished update
            final long updatedCount = fut.getAffectedRowsCount();
            metrics.jsonQuery().markComplete();
            sendUpdateConfirmation(state, keepAliveHeader, updatedCount);
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
            HttpChunkedResponseSocket socket,
            long bytesSent,
            CharSequence message,
            int code,
            Throwable e,
            JsonQueryProcessorState state,
            Metrics metrics
    ) throws ServerDisconnectException, PeerDisconnectedException, PeerIsSlowToReadException {
        logInternalError(e, state, metrics);
        if (bytesSent > 0) {
            // We already sent a partial response to the client.
            // Give up and close the connection.
            throw ServerDisconnectException.INSTANCE;
        }
        int position = 0;
        if (e instanceof CairoException) {
            position = ((CairoException) e).getPosition();
        }

        sendException(socket, state.getHttpConnectionContext(), position, message, state.getQuery(), configuration.getKeepAliveHeader(), code);
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
            state.info().$("Empty query header received. Sending empty reply.").$();
            sendException(context.getChunkedResponseSocket(), context, 0, "No query text", query, keepAliveHeader, 400);
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
                // Skip or stop will have default value.
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
            sendException(context.getChunkedResponseSocket(), context, 0, "Bad UTF8 encoding in query text", query, keepAliveHeader, 400);
            return false;
        }
        return true;
    }

    private void retryQueryExecution(
            JsonQueryProcessorState state,
            OperationFuture fut
    ) throws PeerIsSlowToReadException, PeerDisconnectedException, QueryPausedException, SqlException {
        final int waitResult;
        try {
            waitResult = fut.await(0);
        } catch (TableReferenceOutOfDateException e) {
            state.freeAsyncOperation();
            compileQuery(state);
            return;
        }

        if (waitResult != OperationFuture.QUERY_COMPLETE) {
            long timeout = state.getStatementTimeout() > 0 ? state.getStatementTimeout() : asyncCommandTimeout;
            if (state.getExecutionTimeNanos() / 1_000_000L < timeout) {
                // Schedule a retry
                state.info().$("waiting for update query [instance=").$(fut.getInstanceId()).I$();
                throw EntryUnavailableException.instance("wait for update query");
            } else {
                state.freeAsyncOperation();
                throw SqlTimeoutException.timeout("Query timeout. Please add HTTP header 'Statement-Timeout' with timeout in ms");
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
        metrics.jsonQuery().markComplete();
        sendConfirmation(state, keepAliveHeader);
    }

    protected static void header(
            HttpChunkedResponseSocket socket,
            HttpConnectionContext context,
            CharSequence keepAliveHeader,
            int statusCode
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        socket.status(statusCode, HttpConstants.CONTENT_TYPE_JSON);
        socket.headers().setKeepAlive(keepAliveHeader);
        context.getCookieHandler().setCookie(socket.headers(), context.getSecurityContext());
        socket.sendHeader();
    }

    static void sendException(
            HttpChunkedResponseSocket socket,
            HttpConnectionContext context,
            int position,
            CharSequence message,
            CharSequence query,
            CharSequence keepAliveHeader,
            int code
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        header(socket, context, keepAliveHeader, code);
        JsonQueryProcessorState.prepareExceptionJson(socket, position, message, query);
    }

    static void sendException(
            HttpChunkedResponseSocket socket,
            HttpConnectionContext context,
            int position,
            CharSequence message,
            DirectUtf8Sequence query,
            CharSequence keepAliveHeader,
            int code
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        header(socket, context, keepAliveHeader, code);
        JsonQueryProcessorState.prepareExceptionJson(socket, position, message, query);
    }

    @FunctionalInterface
    public interface QueryExecutor {
        void execute(
                JsonQueryProcessorState state,
                CompiledQuery cc,
                CharSequence keepAliveHeader
        ) throws PeerDisconnectedException, PeerIsSlowToReadException, QueryPausedException, SqlException;
    }
}
