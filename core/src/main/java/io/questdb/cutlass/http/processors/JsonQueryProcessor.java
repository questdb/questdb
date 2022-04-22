/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.Telemetry;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.sql.ReaderOutOfDateException;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.http.*;
import io.questdb.cutlass.http.ex.RetryOperationException;
import io.questdb.cutlass.text.Utf8Exception;
import io.questdb.griffin.*;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.NoSpaceLeftInResponseBufferException;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.ServerDisconnectException;
import io.questdb.std.*;
import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

public class JsonQueryProcessor implements HttpRequestProcessor, Closeable {

    private static final LocalValue<JsonQueryProcessorState> LV = new LocalValue<>();
    private static final Log LOG = LogFactory.getLog(JsonQueryProcessor.class);

    protected final ObjList<QueryExecutor> queryExecutors = new ObjList<>();
    private final SqlCompiler compiler;
    private final JsonQueryProcessorConfiguration configuration;
    private final SqlExecutionContextImpl sqlExecutionContext;
    private final Path path = new Path();
    private final NanosecondClock nanosecondClock;
    private final NetworkSqlExecutionCircuitBreaker circuitBreaker;
    private final Metrics metrics;
    private final long alterStartTimeout;
    private final long alterStartFullTimeoutNs;

    @TestOnly
    public JsonQueryProcessor(
            JsonQueryProcessorConfiguration configuration,
            CairoEngine engine,
            int workerCount
    ) {
        this(configuration, engine, workerCount, null, null);
    }

    public JsonQueryProcessor(
            JsonQueryProcessorConfiguration configuration,
            CairoEngine engine,
            int workerCount,
            @Nullable FunctionFactoryCache functionFactoryCache,
            @Nullable DatabaseSnapshotAgent snapshotAgent
    ) {
        this(configuration, engine, new SqlCompiler(engine, functionFactoryCache, snapshotAgent), new SqlExecutionContextImpl(engine, workerCount));
    }

    public JsonQueryProcessor(
            JsonQueryProcessorConfiguration configuration,
            CairoEngine engine,
            SqlCompiler sqlCompiler,
            SqlExecutionContextImpl sqlExecutionContext
    ) {
        this.configuration = configuration;
        this.compiler = sqlCompiler;
        final QueryExecutor sendConfirmation = this::updateMetricsAndSendConfirmation;
        this.queryExecutors.extendAndSet(CompiledQuery.SELECT, this::executeNewSelect);
        this.queryExecutors.extendAndSet(CompiledQuery.INSERT, this::executeInsert);
        this.queryExecutors.extendAndSet(CompiledQuery.UPDATE, this::executeInsertOrUpdate);
        this.queryExecutors.extendAndSet(CompiledQuery.TRUNCATE, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.ALTER, this::executeAlterTable);
        this.queryExecutors.extendAndSet(CompiledQuery.REPAIR, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.SET, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.BEGIN, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.COMMIT, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.ROLLBACK, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.DROP, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.RENAME_TABLE, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.COPY_LOCAL, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.CREATE_TABLE, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.INSERT_AS_SELECT, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.COPY_REMOTE, JsonQueryProcessor::cannotCopyRemote);
        this.queryExecutors.extendAndSet(CompiledQuery.BACKUP_TABLE, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.CREATE_TABLE_AS_SELECT, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.SNAPSHOT_DB_PREPARE, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.SNAPSHOT_DB_COMPLETE, sendConfirmation);
        this.sqlExecutionContext = sqlExecutionContext;
        this.nanosecondClock = engine.getConfiguration().getNanosecondClock();
        this.circuitBreaker = new NetworkSqlExecutionCircuitBreaker(configuration.getCircuitBreakerConfiguration());
        this.metrics = engine.getMetrics();
        this.alterStartTimeout = engine.getConfiguration().getWriterAsyncCommandBusyWaitTimeout();
        this.alterStartFullTimeoutNs = engine.getConfiguration().getWriterAsyncCommandMaxTimeout() * 1000;
    }

    @Override
    public void close() {
        Misc.free(compiler);
        Misc.free(path);
        Misc.free(circuitBreaker);
    }

    public void execute0(JsonQueryProcessorState state) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {

        QueryFuture continueExecution = state.getContinueExecution();
        final HttpConnectionContext context = state.getHttpConnectionContext();

        if (continueExecution == null) {
            metrics.jsonQuery().markStart();
            state.startExecutionTimer();
            // do not set random for new request to avoid copying random from previous request into next one
            // the only time we need to copy random from state is when we resume request execution
            sqlExecutionContext.with(context.getCairoSecurityContext(), null, null, context.getFd(), circuitBreaker.of(context.getFd()));
            state.info().$("exec [q='").utf8(state.getQuery()).$("']").$();
        }

        final RecordCursorFactory factory = QueryCache.getInstance().poll(state.getQuery());
        try {
            if (continueExecution != null) {
                retryQueryExecution(state, continueExecution);
                return;
            }

            if (factory != null) {
                try {
                    sqlExecutionContext.storeTelemetry(CompiledQuery.SELECT, Telemetry.ORIGIN_HTTP_JSON);
                    executeCachedSelect(
                            state,
                            factory,
                            configuration.getKeepAliveHeader());
                } catch (ReaderOutOfDateException e) {
                    LOG.info().$(e.getFlyweightMessage()).$();
                    Misc.free(factory);
                    compileQuery(state);
                }
            } else {
                // new query
                compileQuery(state);
            }
        } catch (SqlException e) {
            syntaxError(context.getChunkedResponseSocket(), e, state, configuration.getKeepAliveHeader());
            readyForNextRequest(context);
        } catch (EntryUnavailableException e) {
            LOG.info().$("[fd=").$(context.getFd()).$("] Resource busy, will retry").$();
            throw RetryOperationException.INSTANCE;
        } catch (CairoError | CairoException e) {
            internalError(context.getChunkedResponseSocket(), e.getFlyweightMessage(), e, state);
            readyForNextRequest(context);
        } catch (PeerIsSlowToReadException | PeerDisconnectedException e) {
            // re-throw the exception
            throw e;
        } catch (Throwable e) {
            state.error().$("Uh-oh. Error!").$(e).$();
            throw ServerDisconnectException.INSTANCE;
        }
    }

    private void retryQueryExecution(JsonQueryProcessorState state, QueryFuture continueExecution) throws SqlException, PeerIsSlowToReadException, PeerDisconnectedException {
        if (continueExecution.await(0) != QueryFuture.QUERY_COMPLETE) {
            if (state.getExecutionTime() < alterStartFullTimeoutNs) {
                throw EntryUnavailableException.instance("retry alter table wait");
            } else {
                state.setContinueExecution(Misc.free(continueExecution));
                throw SqlException.$(0, "Timeout on waiting ALTER table to finish execution");
            }
        } else {
            state.setContinueExecution(Misc.free(continueExecution));
            sendConfirmation(state, configuration.getKeepAliveHeader());
        }
    }

    @Override
    public void onRequestComplete(
            HttpConnectionContext context
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
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
    public void resumeSend(
            HttpConnectionContext context
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final JsonQueryProcessorState state = LV.get(context);
        if (state != null) {
            // we are resuming request execution, we need to copy random to execution context
            sqlExecutionContext.with(context.getCairoSecurityContext(), null, state.getRnd(), context.getFd(), circuitBreaker.of(context.getFd()));
            doResumeSend(state, context);
        }
    }

    @Override
    public void parkRequest(HttpConnectionContext context) {
        final JsonQueryProcessorState state = LV.get(context);
        if (state != null) {
            // preserve random when we park the context
            state.setRnd(sqlExecutionContext.getRandom());
        }
    }

    private static void doResumeSend(
            JsonQueryProcessorState state,
            HttpConnectionContext context
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        if (state.noCursor()) {
            return;
        }

        LOG.debug().$("resume [fd=").$(context.getFd()).$(']').$();

        final HttpChunkedResponseSocket socket = context.getChunkedResponseSocket();
        while (true) {
            try {
                state.resume(socket);
                break;
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

    private static void cannotCopyRemote(
            JsonQueryProcessorState state,
            CompiledQuery cc,
            CharSequence keepAliveHeader
    ) throws SqlException {
        throw SqlException.$(0, "copy from STDIN is not supported over REST");
    }

    protected static void header(
            HttpChunkedResponseSocket socket,
            CharSequence keepAliveHeader,
            int status_code
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        socket.status(status_code, "application/json; charset=utf-8");
        socket.headers().setKeepAlive(keepAliveHeader);
        socket.sendHeader();
    }

    private static void readyForNextRequest(HttpConnectionContext context) {
        LOG.info().$("all sent [fd=").$(context.getFd()).$(", lastRequestBytesSent=").$(context.getLastRequestBytesSent()).$(", nCompletedRequests=").$(context.getNCompletedRequests() + 1)
                .$(", totalBytesSent=").$(context.getTotalBytesSent()).$(']').$();
    }

    private void updateMetricsAndSendConfirmation(
            JsonQueryProcessorState state,
            CompiledQuery cq,
            CharSequence keepAliveHeader
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        metrics.jsonQuery().markComplete();
        sendConfirmation(state, keepAliveHeader);
    }

    protected static void sendConfirmation(
            JsonQueryProcessorState state,
            CompiledQuery cq,
            CharSequence keepAliveHeader
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        sendConfirmation(state, keepAliveHeader);
    }

    private static void sendConfirmation(JsonQueryProcessorState state, CharSequence keepAliveHeader) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final HttpConnectionContext context = state.getHttpConnectionContext();
        final HttpChunkedResponseSocket socket = context.getChunkedResponseSocket();
        header(socket, keepAliveHeader, 200);
        socket.put('{').putQuoted("ddl").put(':').putQuoted("OK").put('}').put('\n');
        socket.sendChunk(true);
        readyForNextRequest(context);
    }

    static void sendException(
            HttpChunkedResponseSocket socket,
            int position,
            CharSequence message,
            CharSequence query,
            CharSequence keepAliveHeader
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        header(socket, keepAliveHeader, 400);
        JsonQueryProcessorState.prepareExceptionJson(socket, position, message, query);
    }

    private static void syntaxError(
            HttpChunkedResponseSocket socket,
            SqlException sqlException,
            JsonQueryProcessorState state,
            CharSequence keepAliveHeader
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        state.logSyntaxError(sqlException);
        sendException(
                socket,
                sqlException.getPosition(),
                sqlException.getFlyweightMessage(),
                state.getQuery(),
                keepAliveHeader
        );
    }

    private void compileQuery(JsonQueryProcessorState state) throws SqlException, PeerDisconnectedException, PeerIsSlowToReadException {
        final long nanos = nanosecondClock.getTicks();
        final CompiledQuery cc = compiler.compile(state.getQuery(), sqlExecutionContext);
        sqlExecutionContext.storeTelemetry(cc.getType(), Telemetry.ORIGIN_HTTP_JSON);
        state.setCompilerNanos(nanosecondClock.getTicks() - nanos);
        queryExecutors.getQuick(cc.getType()).execute(
                state,
                cc,
                configuration.getKeepAliveHeader()
        );
    }

    private void executeCachedSelect(
            JsonQueryProcessorState state,
            RecordCursorFactory factory,
            CharSequence keepAliveHeader
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, SqlException {
        state.setCompilerNanos(0);
        state.logExecuteCached();
        executeSelect(state, factory, keepAliveHeader);
    }

    @Override
    public void onRequestRetry(
            HttpConnectionContext context
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        JsonQueryProcessorState state = LV.get(context);
        execute0(state);
    }

    @Override
    public void failRequest(HttpConnectionContext context, HttpException e)
            throws PeerDisconnectedException, PeerIsSlowToReadException {
        final JsonQueryProcessorState state = LV.get(context);
        final HttpChunkedResponseSocket socket = context.getChunkedResponseSocket();
        internalError(socket, e.getFlyweightMessage(), e, state);
        socket.shutdownWrite();
    }

    private void executeAlterTable(
            JsonQueryProcessorState state,
            CompiledQuery cc,
            CharSequence keepAliveHeader
    ) throws PeerIsSlowToReadException, PeerDisconnectedException, SqlException {
        QueryFuture queryFuture = null;
        try {
            queryFuture = cc.execute(state.getEventSubSequence());
            int waitResult = queryFuture.await(alterStartTimeout);
            if (waitResult == QueryFuture.QUERY_NO_RESPONSE) {
                throw SqlException.$(0, "Timeout expired on waiting for the ALTER TABLE execution start");
            } else if (waitResult == QueryFuture.QUERY_STARTED) {
                state.setContinueExecution(queryFuture);
                queryFuture = null;
                throw EntryUnavailableException.instance("retry alter table wait");
            }
        } finally {
            if (queryFuture != null) {
                queryFuture.close();
            }
        }
        metrics.jsonQuery().markComplete();
        sendConfirmation(state, cc, keepAliveHeader);
    }

    private void executeInsertOrUpdate(
            JsonQueryProcessorState state,
            CompiledQuery cc,
            CharSequence keepAliveHeader
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, SqlException {
        try (
                UpdateOperation op = cc.getUpdateOperation();
                QueryFuture fut = op.execute(sqlExecutionContext, state.getEventSubSequence())
        ) {
            fut.await();
        }
        metrics.jsonQuery().markComplete();
        sendConfirmation(state, cc, keepAliveHeader);
    }

    private void executeInsert(
            JsonQueryProcessorState state,
            CompiledQuery cc,
            CharSequence keepAliveHeader
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, SqlException {
        cc.getInsertOperation().execute(sqlExecutionContext).await();
        metrics.jsonQuery().markComplete();
        sendConfirmation(state, cc, keepAliveHeader);
    }

    private void executeNewSelect(
            JsonQueryProcessorState state,
            CompiledQuery cc,
            CharSequence keepAliveHeader
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, SqlException {
        state.logExecuteNew();
        final RecordCursorFactory factory = cc.getRecordCursorFactory();
        executeSelect(
                state,
                factory,
                keepAliveHeader);
    }

    private void executeSelect(
            JsonQueryProcessorState state,
            RecordCursorFactory factory,
            CharSequence keepAliveHeader
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, SqlException {
        final HttpConnectionContext context = state.getHttpConnectionContext();
        try {
            if (state.of(factory, sqlExecutionContext)) {
                header(context.getChunkedResponseSocket(), keepAliveHeader, 200);
                doResumeSend(state, context);
                metrics.jsonQuery().markComplete();
            } else {
                readyForNextRequest(context);
            }
        } catch (CairoException ex) {
            state.setQueryCacheable(ex.isCacheable());
            throw ex;
        }
    }

    private void internalError(
            HttpChunkedResponseSocket socket,
            CharSequence message,
            Throwable e,
            JsonQueryProcessorState state
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        if (e instanceof CairoException && ((CairoException) e).isInterruption()) {
            state.info().$("query cancelled [q=`").utf8(state.getQuery()).$("`, reason=`").$(((CairoException) e).getFlyweightMessage()).$("`]").$();
        } else if (e instanceof HttpException) {
            state.error().$("internal HTTP server error [q=`").utf8(state.getQuery()).$("`, reason=`").$(((HttpException) e).getFlyweightMessage()).$("`]").$();
        } else {
            state.error().$("internal error [q=`").utf8(state.getQuery()).$("`, ex=").$(e).$(']').$();
            // This is a critical error, so we treat it as an unhandled one.
            metrics.healthCheck().incrementUnhandledErrors();
        }
        sendException(socket, 0, message, state.getQuery(), configuration.getKeepAliveHeader());
    }

    private boolean parseUrl(
            JsonQueryProcessorState state,
            CharSequence keepAliveHeader
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        // Query text.
        final HttpRequestHeader header = state.getHttpConnectionContext().getRequestHeader();
        final DirectByteCharSequence query = header.getUrlParam("query");
        if (query == null || query.length() == 0) {
            state.info().$("Empty query header received. Sending empty reply.").$();
            sendException(state.getHttpConnectionContext().getChunkedResponseSocket(), 0, "No query text", query, keepAliveHeader);
            return false;
        }

        // Url Params.
        long skip = 0;
        long stop = Long.MAX_VALUE;

        CharSequence limit = header.getUrlParam("limit");
        if (limit != null) {
            int sepPos = Chars.indexOf(limit, ',');
            try {
                if (sepPos > 0) {
                    skip = Numbers.parseLong(limit, 0, sepPos) - 1;
                    if (sepPos + 1 < limit.length()) {
                        stop = Numbers.parseLong(limit, sepPos + 1, limit.length());
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
            sendException(state.getHttpConnectionContext().getChunkedResponseSocket(), 0, "Bad UTF8 encoding in query text", query, keepAliveHeader);
            return false;
        }
        return true;
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
