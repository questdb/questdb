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
import io.questdb.cairo.*;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.ReaderOutOfDateException;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.http.*;
import io.questdb.cutlass.http.ex.RetryOperationException;
import io.questdb.cutlass.text.Utf8Exception;
import io.questdb.griffin.*;
import io.questdb.griffin.engine.ops.UpdateOperation;
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
    private final long asyncWriterStartTimeout;
    private final long asyncCommandTimeout;

    @TestOnly
    public JsonQueryProcessor(
            JsonQueryProcessorConfiguration configuration,
            CairoEngine engine,
            int workerCount
    ) {
        this(configuration, engine, workerCount, workerCount, null, null);
    }

    public JsonQueryProcessor(
            JsonQueryProcessorConfiguration configuration,
            CairoEngine engine,
            int workerCount,
            int sharedWorkerCount,
            @Nullable FunctionFactoryCache functionFactoryCache,
            @Nullable DatabaseSnapshotAgent snapshotAgent
    ) {
        this(configuration,
                engine,
                new SqlCompiler(engine, functionFactoryCache, snapshotAgent),
                new SqlExecutionContextImpl(engine, workerCount, sharedWorkerCount));
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
        this.queryExecutors.extendAndSet(CompiledQuery.TRUNCATE, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.ALTER, this::executeAlterTable);
        this.queryExecutors.extendAndSet(CompiledQuery.REPAIR, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.SET, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.DROP, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.COPY_LOCAL, this::executeCopy);
        this.queryExecutors.extendAndSet(CompiledQuery.CREATE_TABLE, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.INSERT_AS_SELECT, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.COPY_REMOTE, JsonQueryProcessor::cannotCopyRemote);
        this.queryExecutors.extendAndSet(CompiledQuery.RENAME_TABLE, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.BACKUP_TABLE, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.UPDATE, this::executeUpdate);
        this.queryExecutors.extendAndSet(CompiledQuery.LOCK, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.UNLOCK, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.VACUUM, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.BEGIN, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.COMMIT, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.ROLLBACK, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.CREATE_TABLE_AS_SELECT, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.SNAPSHOT_DB_PREPARE, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.SNAPSHOT_DB_COMPLETE, sendConfirmation);
        this.queryExecutors.extendAndSet(CompiledQuery.DEALLOCATE, sendConfirmation);
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
        Misc.free(compiler);
        Misc.free(path);
        Misc.free(circuitBreaker);
    }

    public void execute0(JsonQueryProcessorState state) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        OperationFuture fut = state.getOperationFuture();
        final HttpConnectionContext context = state.getHttpConnectionContext();
        circuitBreaker.resetTimer();

        if (fut == null) {
            metrics.jsonQuery().markStart();
            state.startExecutionTimer();
            // do not set random for new request to avoid copying random from previous request into next one
            // the only time we need to copy random from state is when we resume request execution
            sqlExecutionContext.with(context.getCairoSecurityContext(), null, null, context.getFd(), circuitBreaker.of(context.getFd()));
            if (state.getStatementTimeout() > 0L) {
                circuitBreaker.setTimeout(state.getStatementTimeout());
            } else {
                circuitBreaker.resetMaxTimeToDefault();
            }
            state.info().$("exec [q='").utf8(state.getQuery()).$("']").$();
        }

        try {
            if (fut != null) {
                retryQueryExecution(state, fut);
                return;
            }

            final RecordCursorFactory factory = QueryCache.getThreadLocalInstance().poll(state.getQuery());
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
        } catch (SqlException | ImplicitCastException e) {
            sqlError(context.getChunkedResponseSocket(), state, e, configuration.getKeepAliveHeader());
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
            state.critical().$("Uh-oh. Error!").$(e).$();
            throw ServerDisconnectException.INSTANCE;
        }
    }

    private static void sendUpdateConfirmation(JsonQueryProcessorState state, CharSequence keepAliveHeader, long updateRecords) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final HttpConnectionContext context = state.getHttpConnectionContext();
        final HttpChunkedResponseSocket socket = context.getChunkedResponseSocket();
        header(socket, keepAliveHeader, 200);
        socket.put('{').putQuoted("ddl").put(':').putQuoted("OK").put(',').putQuoted("updated").put(':').put(updateRecords).put('}').put('\n');
        socket.sendChunk(true);
        readyForNextRequest(context);
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

    private static void sendConfirmation(JsonQueryProcessorState state, CharSequence keepAliveHeader) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final HttpConnectionContext context = state.getHttpConnectionContext();
        final HttpChunkedResponseSocket socket = context.getChunkedResponseSocket();
        header(socket, keepAliveHeader, 200);
        socket.put('{').putQuoted("ddl").put(':').putQuoted("OK").put('}').put('\n');
        socket.sendChunk(true);
        readyForNextRequest(context);
    }

    private void compileQuery(JsonQueryProcessorState state) throws SqlException, PeerDisconnectedException, PeerIsSlowToReadException {
        boolean recompileStale = true;
        for (int retries = 0; recompileStale; retries++) {
            try {
                final long nanos = nanosecondClock.getTicks();
                final CompiledQuery cc = compiler.compile(state.getQuery(), sqlExecutionContext);
                sqlExecutionContext.storeTelemetry(cc.getType(), Telemetry.ORIGIN_HTTP_JSON);
                state.setCompilerNanos(nanosecondClock.getTicks() - nanos);
                state.setQueryType(cc.getType());
                queryExecutors.getQuick(cc.getType()).execute(
                        state,
                        cc,
                        configuration.getKeepAliveHeader()
                );
                recompileStale = false;
            } catch (ReaderOutOfDateException e) {
                if (retries == ReaderOutOfDateException.MAX_RETRY_ATTEMPS) {
                    throw e;
                }
                LOG.info().$(e.getFlyweightMessage()).$();
                // will recompile
            }
        }
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

    private static void sqlError(
            HttpChunkedResponseSocket socket,
            JsonQueryProcessorState state,
            FlyweightMessageContainer container,
            CharSequence keepAliveHeader
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        state.logSqlError(container);
        sendException(
                socket,
                container.getPosition(),
                container.getFlyweightMessage(),
                state.getQuery(),
                keepAliveHeader
        );
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
                state.setOperationFuture(null, fut); // Alter operation does not need to be disposable
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

    private void executeUpdate(
            JsonQueryProcessorState state,
            CompiledQuery cq,
            CharSequence keepAliveHeader
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, SqlException {
        UpdateOperation op = cq.getOperation();
        op.start();
        op.withContext(sqlExecutionContext);
        circuitBreaker.resetTimer();
        OperationFuture fut = null;
        boolean isAsyncWait = false;
        try {
            fut = cq.getDispatcher().execute(op, sqlExecutionContext, state.getEventSubSequence());
            int waitResult = fut.await(getAsyncWriterStartTimeout(state));
            if (waitResult != OperationFuture.QUERY_COMPLETE) {
                isAsyncWait = true;
                state.setOperationFuture(op, fut);
                throw EntryUnavailableException.instance("retry update table wait");
            }
            // All good, finished update
            final long updatedCount = fut.getAffectedRowsCount();
            metrics.jsonQuery().markComplete();
            sendUpdateConfirmation(state, keepAliveHeader, updatedCount);
        } finally {
            if (!isAsyncWait && fut != null) {
                fut.close();
                op.close();
            }
        }
    }

    private long getAsyncWriterStartTimeout(JsonQueryProcessorState state) {
        return Math.min(asyncWriterStartTimeout, state.getStatementTimeout());
    }

    private void retryQueryExecution(JsonQueryProcessorState state, OperationFuture fut) throws SqlException, PeerIsSlowToReadException, PeerDisconnectedException {
        final int waitResult;
        try {
            waitResult = fut.await(0);
        } catch (ReaderOutOfDateException e) {
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
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, SqlException {
        state.logExecuteNew();
        final RecordCursorFactory factory = cq.getRecordCursorFactory();
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

    private void executeCopy(
            JsonQueryProcessorState state,
            CompiledQuery cq,
            CharSequence keepAliveHeader
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, SqlException {
        final RecordCursorFactory factory = cq.getRecordCursorFactory();
        if (factory == null) {
            // COPY 'id' CANCEL; case
            updateMetricsAndSendConfirmation(state, cq, keepAliveHeader);
            return;
        }
        // new import case
        final HttpConnectionContext context = state.getHttpConnectionContext();
        // Make sure to mark the query as non-cacheable.
        if (state.of(factory, false, sqlExecutionContext)) {
            header(context.getChunkedResponseSocket(), keepAliveHeader, 200);
            doResumeSend(state, context);
            metrics.jsonQuery().markComplete();
        } else {
            readyForNextRequest(context);
        }
    }

    private void internalError(
            HttpChunkedResponseSocket socket,
            CharSequence message,
            Throwable e,
            JsonQueryProcessorState state
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        if (e instanceof CairoException) {
            CairoException ce = (CairoException) e;
            if (ce.isInterruption()) {
                state.info().$("query cancelled [q=`").utf8(state.getQuery()).$("`, reason=`").$(((CairoException) e).getFlyweightMessage()).$("`]").$();
            } else {
                if (ce.isCritical()) {
                    state.critical().$("error [q=`").utf8(state.getQuery()).$(", msg=`").$(ce.getFlyweightMessage()).$('`').$(", errno=`").$(ce.getErrno()).I$();
                } else {
                    state.error().$("error [q=`").utf8(state.getQuery()).$(", msg=`").$(ce.getFlyweightMessage()).$('`').$(", errno=`").$(ce.getErrno()).I$();
                }
            }
        } else if (e instanceof HttpException) {
            state.error().$("internal HTTP server error [q=`").utf8(state.getQuery()).$("`, reason=`").$(((HttpException) e).getFlyweightMessage()).$("`]").$();
        } else {
            state.critical().$("internal error [q=`").utf8(state.getQuery()).$("`, ex=").$(e).$(']').$();
            // This is a critical error, so we treat it as an unhandled one.
            metrics.health().incrementUnhandledErrors();
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
