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

package io.questdb.cutlass.http;

import io.questdb.Metrics;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.security.DenyAllSecurityContext;
import io.questdb.cairo.security.PrincipalContext;
import io.questdb.cairo.security.SecurityContextFactory;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.http.ex.BufferOverflowException;
import io.questdb.cutlass.http.ex.NotEnoughLinesException;
import io.questdb.cutlass.http.ex.RetryFailedOperationException;
import io.questdb.cutlass.http.ex.RetryOperationException;
import io.questdb.cutlass.http.ex.TooFewBytesReceivedException;
import io.questdb.cutlass.http.processors.RejectProcessor;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.HeartBeatException;
import io.questdb.network.IOContext;
import io.questdb.network.IOOperation;
import io.questdb.network.Net;
import io.questdb.network.NetworkFacade;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.PeerIsSlowToWriteException;
import io.questdb.network.ServerDisconnectException;
import io.questdb.network.Socket;
import io.questdb.network.SocketFactory;
import io.questdb.network.TlsSessionInitFailedException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.AssociativeCache;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Chars;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjectPool;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.datetime.Clock;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.StdoutSink;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.cutlass.http.HttpConstants.*;
import static io.questdb.cutlass.http.HttpResponseSink.HTTP_TOO_MANY_REQUESTS;
import static io.questdb.network.IODispatcher.*;
import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;

public class HttpConnectionContext extends IOContext<HttpConnectionContext> implements Locality, Retry {
    private static final String FALSE = "false";
    private static final Log LOG = LogFactory.getLog(HttpConnectionContext.class);
    private static final int NO_RESUME_PROCESSOR = Integer.MIN_VALUE;
    private static final String TRUE = "true";
    private final ActiveConnectionTracker activeConnectionTracker;
    private final HttpAuthenticator authenticator;
    private final ChunkedContentParser chunkedContentParser = new ChunkedContentParser();
    private final HttpServerConfiguration configuration;
    private final HttpCookieHandler cookieHandler;
    private final ObjectPool<DirectUtf8String> csPool;
    private final boolean dumpNetworkTraffic;
    private final int forceFragmentationReceiveChunkSize;
    private final HttpHeaderParser headerParser;
    private final LocalValueMap localValueMap = new LocalValueMap();
    private final Metrics metrics;
    private final HttpHeaderParser multipartContentHeaderParser;
    private final HttpMultipartContentParser multipartContentParser;
    private final long multipartIdleSpinCount;
    private final MultipartParserState multipartParserState = new MultipartParserState();
    private final NetworkFacade nf;
    private final CharSequenceObjHashMap<CharSequence> parsedCookies = new CharSequenceObjHashMap<>();
    private final boolean preAllocateBuffers;
    private final RejectProcessor rejectProcessor;
    private final HttpRequestValidator requestValidator = new HttpRequestValidator();
    private final HttpResponseSink responseSink;
    private final RetryAttemptAttributes retryAttemptAttributes = new RetryAttemptAttributes();
    private final RescheduleContext retryRescheduleContext = retry -> {
        LOG.info().$("Retry is requested after successful writer allocation. Retry will be re-scheduled [thread=").$(Thread.currentThread().getId()).I$();
        throw RetryOperationException.INSTANCE;
    };
    private final AssociativeCache<RecordCursorFactory> selectCache;
    private final StringSink sessionIdSink = new StringSink();
    private final HttpSessionStore sessionStore;
    private long authenticationNanos = 0L;
    private boolean connectionCounted;
    private boolean forceDisconnectOnComplete;
    private NetworkSqlExecutionCircuitBreaker httpCircuitBreaker;
    private SqlExecutionContextImpl httpSqlExecutionContext;
    private int nCompletedRequests;
    private boolean pendingRetry = false;
    private String processorName;
    private boolean protocolSwitched = false;  // WebSocket protocol switch flag
    private int receivedBytes;
    private long recvBuffer;
    private int recvBufferReadSize;
    private int recvBufferSize;
    private long recvPos;
    private int currentHandlerId = HttpRequestProcessorSelector.REJECT_PROCESSOR_ID;
    private int resumeHandlerId = NO_RESUME_PROCESSOR;
    private SecurityContext securityContext;
    private long totalBytesSent;
    private long totalReceived;

    @TestOnly
    public HttpConnectionContext(
            HttpServerConfiguration configuration,
            SocketFactory socketFactory
    ) {
        this(
                configuration,
                socketFactory,
                HttpServer.NO_OP_CACHE,
                ActiveConnectionTracker.NO_TRACKING
        );
    }

    public HttpConnectionContext(
            HttpServerConfiguration configuration,
            SocketFactory socketFactory,
            AssociativeCache<RecordCursorFactory> selectCache,
            ActiveConnectionTracker activeConnectionTracker
    ) {
        super(
                socketFactory,
                configuration.getHttpContextConfiguration().getNetworkFacade(),
                LOG
        );
        this.configuration = configuration;
        this.cookieHandler = configuration.getFactoryProvider().getHttpCookieHandler();
        this.sessionStore = configuration.getFactoryProvider().getHttpSessionStore();
        this.activeConnectionTracker = activeConnectionTracker;
        final HttpContextConfiguration contextConfiguration = configuration.getHttpContextConfiguration();
        this.nf = contextConfiguration.getNetworkFacade();
        this.csPool = new ObjectPool<>(DirectUtf8String.FACTORY, contextConfiguration.getConnectionStringPoolCapacity());
        this.headerParser = configuration.getFactoryProvider().getHttpHeaderParserFactory().newParser(contextConfiguration.getRequestHeaderBufferSize(), csPool);
        this.multipartContentHeaderParser = new HttpHeaderParser(contextConfiguration.getMultipartHeaderBufferSize(), csPool);
        this.multipartContentParser = new HttpMultipartContentParser(multipartContentHeaderParser);
        this.responseSink = new HttpResponseSink(configuration);
        this.recvBufferSize = configuration.getRecvBufferSize();
        this.preAllocateBuffers = configuration.preAllocateBuffers();
        if (preAllocateBuffers) {
            recvBuffer = Unsafe.malloc(recvBufferSize, MemoryTag.NATIVE_HTTP_CONN);
            this.responseSink.open(configuration.getSendBufferSize());
        }
        this.multipartIdleSpinCount = contextConfiguration.getMultipartIdleSpinCount();
        this.dumpNetworkTraffic = contextConfiguration.getDumpNetworkTraffic();
        // This is default behaviour until the security context is overridden with correct principal.
        this.securityContext = DenyAllSecurityContext.INSTANCE;
        this.metrics = contextConfiguration.getMetrics();
        this.authenticator = contextConfiguration.getFactoryProvider().getHttpAuthenticatorFactory().getHttpAuthenticator();
        this.rejectProcessor = contextConfiguration.getFactoryProvider().getRejectProcessorFactory().getRejectProcessor(this);
        this.forceFragmentationReceiveChunkSize = contextConfiguration.getForceRecvFragmentationChunkSize();
        this.recvBufferReadSize = Math.min(forceFragmentationReceiveChunkSize, recvBufferSize);
        this.selectCache = selectCache;
    }

    // called when returning the context back to a pool (=connection closed)
    @Override
    public void clear() {
        LOG.debug().$("clear [fd=").$(getFd()).I$();
        decrementActiveConnections(getFd());
        super.clear();
        reset();
        if (this.pendingRetry) {
            LOG.error().$("reused context with retry pending").$();
        }
        this.pendingRetry = false;
        if (!preAllocateBuffers) {
            this.recvBuffer = Unsafe.free(recvBuffer, recvBufferSize, MemoryTag.NATIVE_HTTP_CONN);
            this.responseSink.close();
            this.headerParser.close();
            this.multipartContentHeaderParser.close();
        }
        this.forceDisconnectOnComplete = false;
        this.localValueMap.disconnect();
        this.protocolSwitched = false;
        // Security: Always reset security context when context is returned to pool.
        // This ensures no security context leaks between connections.
        this.securityContext = DenyAllSecurityContext.INSTANCE;
    }

    @Override
    public void close() {
        final long fd = getFd();
        LOG.debug().$("close [fd=").$(fd).I$();
        decrementActiveConnections(fd);
        super.close();
        if (this.pendingRetry) {
            this.pendingRetry = false;
            LOG.info().$("closed context with retry pending [fd=").$(getFd()).I$();
        }
        this.nCompletedRequests = 0;
        this.totalBytesSent = 0;
        this.csPool.clear();
        this.multipartContentParser.close();
        this.multipartContentHeaderParser.close();
        this.headerParser.close();
        this.localValueMap.close();
        this.httpCircuitBreaker = Misc.free(httpCircuitBreaker);
        this.httpSqlExecutionContext = Misc.free(httpSqlExecutionContext);
        this.recvBuffer = Unsafe.free(recvBuffer, recvBufferSize, MemoryTag.NATIVE_HTTP_CONN);
        this.responseSink.close();
        this.receivedBytes = 0;
        this.securityContext = DenyAllSecurityContext.INSTANCE;
        this.sessionIdSink.clear();
        this.authenticator.close();
        LOG.debug().$("closed [fd=").$(fd).I$();
    }

    @Override
    public void fail(HttpRequestProcessorSelector selector, HttpException e) throws PeerIsSlowToReadException, ServerDisconnectException {
        LOG.info().$("failed to retry query [fd=").$(getFd()).I$();
        HttpRequestProcessor processor = getHttpRequestProcessor(selector);
        failProcessor(processor, e, DISCONNECT_REASON_RETRY_FAILED);
    }

    @Override
    public RetryAttemptAttributes getAttemptDetails() {
        return retryAttemptAttributes;
    }

    public long getAuthenticationNanos() {
        return authenticationNanos;
    }

    public HttpChunkedResponse getChunkedResponse() {
        return responseSink.getChunkedResponse();
    }

    public HttpCookieHandler getCookieHandler() {
        return cookieHandler;
    }

    public long getLastRequestBytesSent() {
        return responseSink.getTotalBytesSent();
    }

    @Override
    public LocalValueMap getMap() {
        return localValueMap;
    }

    public Metrics getMetrics() {
        return metrics;
    }

    public int getNCompletedRequests() {
        return nCompletedRequests;
    }

    public CharSequenceObjHashMap<CharSequence> getParsedCookiesMap() {
        return parsedCookies;
    }

    public HttpRawSocket getRawResponseSocket() {
        return responseSink.getRawSocket();
    }

    @SuppressWarnings("unused")
    public RejectProcessor getRejectProcessor() {
        return rejectProcessor;
    }

    public HttpRequestHeader getRequestHeader() {
        return headerParser;
    }

    public HttpResponseHeader getResponseHeader() {
        return responseSink.getHeader();
    }

    public SecurityContext getSecurityContext() {
        return securityContext;
    }

    /**
     * Returns the underlying socket for direct I/O after protocol switch (e.g., WebSocket).
     */
    public Socket getSocket() {
        return socket;
    }

    /**
     * Returns the receive buffer address for protocol-switched connections.
     */
    public long getRecvBuffer() {
        return recvBuffer;
    }

    /**
     * Returns the receive buffer size for protocol-switched connections.
     */
    public int getRecvBufferSize() {
        return recvBufferSize;
    }

    /**
     * Switches the connection to a different protocol (e.g., WebSocket).
     * After calling this, normal HTTP parsing is bypassed and the processor
     * handles raw socket I/O directly. The processor is resolved via
     * {@code currentHandlerId} which was set during request routing.
     */
    public void switchProtocol() {
        this.protocolSwitched = true;
        this.resumeHandlerId = currentHandlerId;
    }

    /**
     * Returns true if the connection has been switched to a different protocol.
     */
    public boolean isProtocolSwitched() {
        return protocolSwitched;
    }

    public AssociativeCache<RecordCursorFactory> getSelectCache() {
        return selectCache;
    }

    public NetworkSqlExecutionCircuitBreaker getCircuitBreaker() {
        return httpCircuitBreaker;
    }

    public SqlExecutionContextImpl getSqlExecutionContext() {
        return httpSqlExecutionContext;
    }

    public NetworkSqlExecutionCircuitBreaker getOrCreateCircuitBreaker(CairoEngine engine) {
        if (httpCircuitBreaker == null) {
            httpCircuitBreaker = new NetworkSqlExecutionCircuitBreaker(
                    engine,
                    engine.getConfiguration().getCircuitBreakerConfiguration(),
                    MemoryTag.NATIVE_CB3
            );
        }
        return httpCircuitBreaker;
    }

    public SqlExecutionContextImpl getOrCreateSqlExecutionContext(CairoEngine engine, int workerCount) {
        if (httpSqlExecutionContext == null) {
            httpSqlExecutionContext = new SqlExecutionContextImpl(engine, workerCount);
        }
        return httpSqlExecutionContext;
    }

    public @NotNull StringSink getSessionIdSink() {
        return sessionIdSink;
    }

    public long getTotalBytesSent() {
        return totalBytesSent;
    }

    public long getTotalReceived() {
        return totalReceived;
    }

    public boolean handleClientOperation(int operation, HttpRequestProcessorSelector selector, RescheduleContext rescheduleContext)
            throws HeartBeatException, PeerIsSlowToReadException, ServerDisconnectException, PeerIsSlowToWriteException {
        boolean keepGoing = switch (operation) {
            case IOOperation.READ -> handleClientRecv(selector, rescheduleContext);
            case IOOperation.WRITE -> handleClientSend(selector);
            case IOOperation.HEARTBEAT -> throw registerDispatcherHeartBeat();
            default -> throw registerDispatcherDisconnect(DISCONNECT_REASON_UNKNOWN_OPERATION);
        };

        boolean useful = keepGoing;
        if (keepGoing) {
            if (keepConnectionAlive()) {
                do {
                    keepGoing = handleClientRecv(selector, rescheduleContext);
                } while (keepGoing);
            } else {
                throw registerDispatcherDisconnect(DISCONNECT_REASON_KEEPALIVE_OFF);
            }
        }
        return useful;
    }

    @Override
    public boolean invalid() {
        return pendingRetry || receivedBytes > 0 || this.socket == null;
    }

    // called between requests on the same connections
    public void reset() {
        LOG.debug().$("reset [fd=").$(getFd()).$(']').$();
        this.totalBytesSent += responseSink.getTotalBytesSent();
        this.responseSink.clear();
        this.nCompletedRequests++;
        // Preserve resumeHandlerId for protocol-switched connections (e.g., WebSocket)
        if (!protocolSwitched) {
            this.resumeHandlerId = NO_RESUME_PROCESSOR;
        }
        this.headerParser.clear();
        this.multipartContentParser.clear();
        this.multipartContentHeaderParser.clear();
        this.csPool.clear();
        this.localValueMap.clear();
        if (httpCircuitBreaker != null) {
            httpCircuitBreaker.clear();
        }
        this.multipartParserState.multipartRetry = false;
        this.retryAttemptAttributes.waitStartTimestamp = 0;
        this.retryAttemptAttributes.lastRunTimestamp = 0;
        this.retryAttemptAttributes.attempt = 0;
        this.receivedBytes = 0;
        this.authenticationNanos = 0L;
        // Preserve securityContext for protocol-switched connections (e.g., WebSocket)
        // The security context was configured during the initial HTTP request and should
        // persist for the lifetime of the WebSocket connection.
        if (!protocolSwitched) {
            this.securityContext = DenyAllSecurityContext.INSTANCE;
        }
        this.sessionIdSink.clear();
        this.authenticator.clear();
        this.totalReceived = 0;
        this.chunkedContentParser.clear();
        this.recvPos = recvBuffer;
        this.rejectProcessor.clear();
    }

    public void resumeResponseSend() throws PeerIsSlowToReadException, PeerDisconnectedException {
        responseSink.resumeSend();
    }

    public void scheduleRetry(HttpRequestProcessor processor, RescheduleContext rescheduleContext) throws PeerIsSlowToReadException, ServerDisconnectException {
        try {
            pendingRetry = true;
            rescheduleContext.reschedule(this);
        } catch (RetryFailedOperationException e) {
            failProcessor(processor, e, DISCONNECT_REASON_RETRY_FAILED);
        }
    }

    public HttpResponseSink.SimpleResponseImpl simpleResponse() {
        return responseSink.simpleResponse();
    }

    @Override
    public boolean tryRerun(HttpRequestProcessorSelector selector, RescheduleContext rescheduleContext) throws PeerIsSlowToReadException, PeerIsSlowToWriteException, ServerDisconnectException {
        if (pendingRetry) {
            pendingRetry = false;
            HttpRequestProcessor processor = getHttpRequestProcessor(selector);
            try {
                LOG.info().$("retrying query [fd=").$(getFd()).I$();
                processor.onRequestRetry(this);
                if (multipartParserState.multipartRetry) {
                    if (continueConsumeMultipart(
                            socket,
                            multipartParserState.start,
                            multipartParserState.buf,
                            multipartParserState.bufRemaining,
                            (HttpMultipartContentProcessor) processor,
                            retryRescheduleContext
                    )) {
                        LOG.info().$("success retried multipart import [fd=").$(getFd()).I$();
                        busyRcvLoop(selector, rescheduleContext);
                    } else {
                        LOG.info().$("retry success but import not finished [fd=").$(getFd()).I$();
                    }
                } else {
                    busyRcvLoop(selector, rescheduleContext);
                }
            } catch (RetryOperationException e2) {
                pendingRetry = true;
                return false;
            } catch (PeerDisconnectedException ignore) {
                throw registerDispatcherDisconnect(DISCONNECT_REASON_PEER_DISCONNECT_AT_RERUN);
            } catch (PeerIsSlowToReadException e2) {
                LOG.info().$("peer is slow on running the rerun [fd=").$(getFd())
                        .$(", thread=").$(Thread.currentThread().getId()).I$();
                processor.parkRequest(this, false);
                resumeHandlerId = (processor instanceof RejectProcessor)
                        ? HttpRequestProcessorSelector.REJECT_PROCESSOR_ID : currentHandlerId;
                throw registerDispatcherWrite();
            } catch (ServerDisconnectException e) {
                LOG.info().$("kicked out [fd=").$(getFd()).I$();
                throw registerDispatcherDisconnect(DISCONNECT_REASON_KICKED_OUT_AT_RERUN);
            }
        }
        return true;
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private void busyRcvLoop(HttpRequestProcessorSelector selector, RescheduleContext rescheduleContext)
            throws PeerIsSlowToReadException, ServerDisconnectException, PeerIsSlowToWriteException {
        reset();
        if (keepConnectionAlive()) {
            while (handleClientRecv(selector, rescheduleContext)) ;
        } else {
            throw registerDispatcherDisconnect(DISCONNECT_REASON_KEEPALIVE_OFF);
        }
    }

    private HttpRequestProcessor checkConnectionLimit(HttpRequestProcessor processor) {
        processorName = processor.getName();
        final int connectionLimit = activeConnectionTracker.getLimit(processorName);
        final long numOfConnections = activeConnectionTracker.inc(processorName);
        connectionCounted = true;

        if (connectionLimit != ActiveConnectionTracker.UNLIMITED) {
            assert processorName != null;
            if (numOfConnections > connectionLimit) {
                rejectProcessor.getMessageSink()
                        .put("exceeded connection limit [name=").put(processorName)
                        .put(", numOfConnections=").put(numOfConnections)
                        .put(", connectionLimit=").put(connectionLimit)
                        .put(", fd=").put(getFd())
                        .put(']');
                decrementActiveConnections(getFd());
                forceDisconnectOnComplete = true;
                return rejectProcessor.withShutdownWrite().reject(HTTP_TOO_MANY_REQUESTS);
            }
            if (processor.reservedOneAdminConnection() && numOfConnections == connectionLimit && !securityContext.isSystemAdmin()) {
                rejectProcessor.getMessageSink()
                        .put("non-admin user reached connection limit [name=").put(processorName)
                        .put(", numOfConnections=").put(numOfConnections)
                        .put(", connectionLimit=").put(connectionLimit)
                        .put(", fd=").put(getFd())
                        .put(']');
                decrementActiveConnections(getFd());
                forceDisconnectOnComplete = true;
                return rejectProcessor.withShutdownWrite().reject(HTTP_TOO_MANY_REQUESTS);
            }
            LOG.debug().$("counted connection [name=").$(processorName)
                    .$(", numOfConnections=").$(numOfConnections)
                    .$(", connectionLimit=").$(connectionLimit)
                    .$(", fd=").$(getFd())
                    .I$();
        }
        return processor;
    }

    private void completeRequest(
            HttpRequestProcessor processor,
            RescheduleContext rescheduleContext
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        LOG.debug().$("complete [fd=").$(getFd()).I$();
        try {
            processor.onRequestComplete(this);
            reset();
        } catch (RetryOperationException e) {
            pendingRetry = true;
            scheduleRetry(processor, rescheduleContext);
        }
    }

    private boolean configureSecurityContext() {
        if (securityContext == DenyAllSecurityContext.INSTANCE) {
            final Clock clock = configuration.getHttpContextConfiguration().getNanosecondClock();
            final long authenticationStart = clock.getTicks();

            final CharSequence sessionId = cookieHandler.processSessionCookie(this);
            HttpSessionStore.SessionInfo sessionInfo = null;
            if (sessionId != null) {
                sessionInfo = sessionStore.verifySessionId(sessionId, this);
            }

            final PrincipalContext principalContext;
            if (authenticator.authenticate(headerParser)) {
                principalContext = authenticator;
            } else if (sessionInfo != null) {
                principalContext = sessionInfo;
            } else {
                // authenticationNanos stays 0, when it fails this value is irrelevant
                return false;
            }

            // auth successful, create security context from auth info
            final SecurityContextFactory scf = configuration.getFactoryProvider().getSecurityContextFactory();
            securityContext = scf.getInstance(principalContext, SecurityContextFactory.HTTP);

            if (configuration.getHttpContextConfiguration().areCookiesEnabled()) {
                // the client can request a session by sending 'session=true',
                // and close the session by sending 'session=false'
                // we do not create a session for clients by default to avoid excessive session creating
                // for clients which do not support/care about cookies, such as apps using the REST API
                final DirectUtf8Sequence sessionParam = getRequestHeader().getUrlParam(URL_PARAM_SESSION);

                // create session if
                // - we do not have one yet and the client requested one with 'session=true' or
                // - the client sent an expired/evicted session id or
                // - changed credentials without sending logout
                if (
                        (Utf8s.equalsNcAscii(TRUE, sessionParam) && sessionId == null)
                                || (sessionId != null && sessionInfo == null)
                                || (sessionInfo != null && !Chars.equals(sessionInfo.getPrincipal(), securityContext.getSessionPrincipal()))
                ) {
                    sessionStore.createSession(authenticator, this);
                } else if (Utf8s.equalsNcAscii(FALSE, sessionParam) && sessionInfo != null) {
                    // close session if client requested it
                    // note that this request is still going to be processed
                    sessionStore.destroySession(sessionInfo.getSessionId(), this);
                }
            }
            authenticationNanos = clock.getTicks() - authenticationStart;
        }
        return true;
    }

    private boolean consumeChunked(
            HttpPostPutProcessor processor,
            long headerEnd,
            long read,
            boolean newRequest
    ) throws PeerIsSlowToReadException, ServerDisconnectException, PeerDisconnectedException, PeerIsSlowToWriteException {
        if (!newRequest) {
            processor.resumeRecv(this);
        }

        while (true) {
            long lo, hi;
            int bufferLenLeft = (int) (recvBuffer + recvBufferSize - recvPos);
            if (newRequest) {
                processor.onHeadersReady(this);
                totalReceived -= headerEnd - recvBuffer;
                lo = headerEnd;
                hi = recvBuffer + read;
                newRequest = false;
            } else {
                read = socket.recv(recvPos, Math.min(forceFragmentationReceiveChunkSize, bufferLenLeft));
                lo = recvBuffer;
                hi = recvPos + read;
            }

            if (read > 0) {
                lo = chunkedContentParser.handleRecv(lo, hi, processor);
                if (lo == Long.MAX_VALUE) {
                    // done
                    processor.onRequestComplete(this);
                    reset();
                    if (keepConnectionAlive()) {
                        return true;
                    } else {
                        return disconnectHttp(processor, DISCONNECT_REASON_KEEPALIVE_OFF_RECV);
                    }
                } else if (lo == Long.MIN_VALUE) {
                    // protocol violation
                    LOG.error().$("cannot parse chunk length, chunked protocol violation, disconnecting [fd=").$(getFd()).I$();
                    return disconnectHttp(processor, DISCONNECT_REASON_KICKED_OUT_AT_EXTRA_BYTES);
                } else if (lo != hi) {
                    lo = -lo;
                    assert lo >= recvBuffer && lo <= hi && lo < recvBuffer + recvBufferSize;
                    if (lo != recvBuffer) {
                        // Compact recv buffer
                        Vect.memmove(recvBuffer, lo, hi - lo);
                    }
                    recvPos = recvBuffer + (hi - lo);
                } else {
                    recvPos = recvBuffer;
                }
            }

            if (read == 0 || read == forceFragmentationReceiveChunkSize) {
                // Schedule for read
                throw registerDispatcherRead();
            } else if (read < 0) {
                // client disconnected
                return disconnectHttp(processor, DISCONNECT_REASON_KICKED_OUT_AT_RECV);
            }
        }
    }

    private boolean consumeContent(
            long contentLength,
            Socket socket,
            HttpPostPutProcessor processor,
            long headerEnd,
            int read,
            boolean newRequest
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException, PeerIsSlowToWriteException {
        if (!newRequest) {
            processor.resumeRecv(this);
        }

        while (true) {
            long lo;
            if (newRequest) {
                processor.onHeadersReady(this);
                totalReceived -= headerEnd - recvBuffer;
                lo = headerEnd;
                newRequest = false;
            } else {
                read = socket.recv(recvBuffer, recvBufferReadSize);
                lo = recvBuffer;
            }

            if (read > 0) {
                if (totalReceived + read > contentLength) {
                    // HTTP protocol violation
                    // client sent more data than it promised in Content-Length header
                    // we will disconnect client and roll back
                    return disconnectHttp(processor, DISCONNECT_REASON_KICKED_OUT_AT_EXTRA_BYTES);
                }

                processor.onChunk(lo, recvBuffer + read);
                totalReceived += read;

                if (totalReceived == contentLength) {
                    // we have received all content, commit
                    // check that client has not disconnected
                    read = socket.recv(recvBuffer, recvBufferSize);
                    if (read < 0) {
                        // client disconnected, don't commit, rollback instead
                        return disconnectHttp(processor, DISCONNECT_REASON_KICKED_OUT_AT_RECV);
                    } else if (read > 0) {
                        // HTTP protocol violation
                        // client sent more data than it promised in Content-Length header
                        // we will disconnect client and roll back
                        return disconnectHttp(processor, DISCONNECT_REASON_KICKED_OUT_AT_EXTRA_BYTES);
                    }

                    processor.onRequestComplete(this);
                    reset();
                    if (keepConnectionAlive()) {
                        return true;
                    } else {
                        return disconnectHttp(processor, DISCONNECT_REASON_KEEPALIVE_OFF_RECV);
                    }
                }
            }

            if (read == 0 || read == forceFragmentationReceiveChunkSize) {
                // Schedule for read
                throw registerDispatcherRead();
            } else if (read < 0) {
                // client disconnected
                return disconnectHttp(processor, DISCONNECT_REASON_KICKED_OUT_AT_RECV);
            }
        }
    }

    private boolean consumeMultipart(
            Socket socket,
            HttpMultipartContentProcessor processor,
            long headerEnd,
            int read,
            boolean newRequest,
            RescheduleContext rescheduleContext
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException, PeerIsSlowToWriteException {
        if (newRequest) {
            if (!headerParser.hasBoundary()) {
                LOG.error().$("Bad request. Form data in multipart POST expected.").$(". Disconnecting [fd=").$(getFd()).I$();
                throw registerDispatcherDisconnect(DISCONNECT_REASON_PROTOCOL_VIOLATION);
            }
            processor.onHeadersReady(this);
            multipartContentParser.of(headerParser.getBoundary());
        }

        processor.resumeRecv(this);

        final long bufferEnd = recvBuffer + read;

        LOG.debug().$("multipart").$();

        // read socket into buffer until there is nothing to read
        long start;
        long buf;
        int bufRemaining;

        if (headerEnd < bufferEnd) {
            start = headerEnd;
            buf = bufferEnd;
            bufRemaining = (int) (recvBufferSize - (bufferEnd - recvBuffer));
        } else {
            start = recvBuffer;
            buf = start + receivedBytes;
            bufRemaining = recvBufferSize - receivedBytes;
            receivedBytes = 0;
        }

        return continueConsumeMultipart(socket, start, buf, bufRemaining, processor, rescheduleContext);
    }

    private boolean continueConsumeMultipart(
            Socket socket,
            long start,
            long buf,
            int bufRemaining,
            HttpMultipartContentProcessor processor,
            RescheduleContext rescheduleContext
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException, PeerIsSlowToWriteException {
        boolean keepGoing = false;

        if (buf > start) {
            try {
                if (parseMultipartResult(start, buf, bufRemaining, processor, rescheduleContext)) {
                    return true;
                }

                buf = start = recvBuffer;
                bufRemaining = recvBufferSize;
            } catch (TooFewBytesReceivedException e) {
                start = multipartContentParser.getResumePtr();
            }
        }

        long spinsRemaining = multipartIdleSpinCount;

        while (true) {
            final int n = socket.recv(buf, bufRemaining);
            if (n < 0) {
                throw registerDispatcherDisconnect(DISCONNECT_REASON_PEER_DISCONNECT_AT_MULTIPART_RECV);
            }

            if (n == 0) {
                // Text loader needs as big of a data chunk as possible
                // to analyse columns and delimiters correctly. To make sure we
                // can deliver large data chunk we have to implement mini-Nagle
                // algorithm by accumulating small data chunks client could be
                // sending into our receive buffer. To make sure we don't
                // sit around accumulating for too long we have spin limit
                if (spinsRemaining-- > 0) {
                    continue;
                }

                // do we have anything in the buffer?
                if (buf > start) {
                    try {
                        if (parseMultipartResult(start, buf, bufRemaining, processor, rescheduleContext)) {
                            keepGoing = true;
                            break;
                        }

                        buf = start = recvBuffer;
                        bufRemaining = recvBufferSize;
                        continue;
                    } catch (TooFewBytesReceivedException e) {
                        start = multipartContentParser.getResumePtr();
                        shiftReceiveBufferUnprocessedBytes(start, (int) (buf - start));
                        throw registerDispatcherRead();
                    }
                }

                LOG.debug().$("peer is slow [multipart]").$();
                throw registerDispatcherRead();
            }

            LOG.debug().$("multipart recv [len=").$(n).I$();

            dumpBuffer(buf, n);

            bufRemaining -= n;
            buf += n;

            if (bufRemaining == 0) {
                try {
                    if (buf - start > 1) {
                        if (parseMultipartResult(start, buf, bufRemaining, processor, rescheduleContext)) {
                            keepGoing = true;
                            break;
                        }
                    }

                    buf = start = recvBuffer;
                    bufRemaining = recvBufferSize;
                } catch (TooFewBytesReceivedException e) {
                    start = multipartContentParser.getResumePtr();
                    int unprocessedSize = (int) (buf - start);
                    // Shift to start
                    if (unprocessedSize < recvBufferSize) {
                        start = multipartContentParser.getResumePtr();
                        shiftReceiveBufferUnprocessedBytes(start, unprocessedSize);
                        throw registerDispatcherRead();
                    } else {
                        // Header does not fit receive buffer
                        failProcessor(processor, BufferOverflowException.INSTANCE, DISCONNECT_REASON_MULTIPART_HEADER_TOO_BIG);
                    }
                    break;
                }
            }
        }
        return keepGoing;
    }

    private void decrementActiveConnections(long fd) {
        if (processorName != null && connectionCounted) {
            long activeConnections = activeConnectionTracker.dec(processorName);
            LOG.debug().$("decrementing active connections [name=").$(processorName)
                    .$(", activeConnections=").$(activeConnections)
                    .$(", fd=").$(fd)
                    .I$();
            processorName = null;
        }
        connectionCounted = false;
    }

    private boolean disconnectHttp(HttpRequestProcessor processor, int reason) throws ServerDisconnectException {
        processor.onConnectionClosed(this);
        reset();
        throw registerDispatcherDisconnect(reason);
    }

    private void dumpBuffer(long buffer, int size) {
        if (dumpNetworkTraffic && size > 0) {
            StdoutSink.INSTANCE.put('>');
            Net.dump(buffer, size);
        }
    }

    private void failProcessor(HttpRequestProcessor processor, HttpException e, int reason) throws PeerIsSlowToReadException, ServerDisconnectException {
        pendingRetry = false;
        boolean canReset = true;
        try {
            LOG.info()
                    .$("failed query result cannot be delivered. Kicked out [fd=").$(getFd())
                    .$(", error=").$safe(e.getFlyweightMessage())
                    .I$();
            processor.failRequest(this, e);
            throw registerDispatcherDisconnect(reason);
        } catch (PeerDisconnectedException peerDisconnectedException) {
            throw registerDispatcherDisconnect(DISCONNECT_REASON_PEER_DISCONNECT_AT_SEND);
        } catch (PeerIsSlowToReadException peerIsSlowToReadException) {
            LOG.info().$("peer is slow to receive failed to retry response [fd=").$(getFd()).I$();
            processor.parkRequest(this, false);
            resumeHandlerId = (processor instanceof RejectProcessor)
                    ? HttpRequestProcessorSelector.REJECT_PROCESSOR_ID : currentHandlerId;
            canReset = false;
            throw registerDispatcherWrite();
        } finally {
            if (canReset) {
                reset();
            }
        }
    }

    private HttpRequestProcessor getHttpRequestProcessor(HttpRequestProcessorSelector selector) {
        final HttpRequestProcessor processor = selector.select(headerParser);
        this.currentHandlerId = selector.getLastSelectedHandlerId();
        return requestValidator.validateRequestType(processor, rejectProcessor);
    }

    /**
     * Handles receive for protocol-switched connections (e.g., WebSocket).
     * Instead of parsing HTTP, delegates to the processor's resumeRecv.
     */
    private boolean handleProtocolSwitchedRecv(HttpRequestProcessorSelector selector) throws PeerIsSlowToWriteException, ServerDisconnectException, PeerIsSlowToReadException {
        final HttpRequestProcessor processor = resolveResumeProcessor(selector);
        try {
            processor.resumeRecv(this);
            // If resumeRecv returns normally, keep processing
            return true;
        } catch (PeerIsSlowToReadException | PeerIsSlowToWriteException e) {
            // Need more data from/to peer
            throw e;
        } catch (ServerDisconnectException e) {
            // Connection should be closed
            LOG.info().$("protocol-switched connection closing [fd=").$(getFd()).I$();
            processor.onConnectionClosed(this);
            throw e;
        } catch (Throwable e) {
            // Any other error, close connection
            LOG.error().$("error in protocol-switched recv [fd=").$(getFd()).$(", e=").$(e).I$();
            processor.onConnectionClosed(this);
            throw registerDispatcherDisconnect(DISCONNECT_REASON_SERVER_ERROR);
        }
    }

    private boolean handleClientRecv(HttpRequestProcessorSelector selector, RescheduleContext rescheduleContext) throws PeerIsSlowToReadException, PeerIsSlowToWriteException, ServerDisconnectException {
        // Handle protocol-switched connections (e.g., WebSocket)
        if (protocolSwitched && resumeHandlerId != NO_RESUME_PROCESSOR) {
            return handleProtocolSwitchedRecv(selector);
        }

        boolean busyRecv = true;
        try {
            // this is address of where header ended in our receiving buffer
            // we need to process request content starting from this address
            long headerEnd = recvBuffer;
            int read = 0;
            final boolean newRequest = headerParser.isIncomplete();
            if (newRequest) {
                while (headerParser.isIncomplete()) {
                    // read headers
                    read = socket.recv(recvBuffer, recvBufferReadSize);
                    LOG.debug().$("recv [fd=").$(getFd()).$(", count=").$(read).I$();
                    if (read < 0 && !headerParser.onRecvError(read)) {
                        LOG.debug()
                                .$("done [fd=").$(getFd())
                                .$(", errno=").$(nf.errno())
                                .I$();
                        // peer disconnect
                        throw registerDispatcherDisconnect(DISCONNECT_REASON_PEER_DISCONNECT_AT_HEADER_RECV);
                    }

                    if (read == 0) {
                        // client is not sending anything
                        throw registerDispatcherRead();
                    }

                    dumpBuffer(recvBuffer, read);
                    headerEnd = headerParser.parse(recvBuffer, recvBuffer + read, true, false);
                }
                requestValidator.of(headerParser);
            }

            requestValidator.validateRequestHeader(rejectProcessor);
            HttpRequestProcessor processor = rejectProcessor.isRequestBeingRejected() ? rejectProcessor : getHttpRequestProcessor(selector);

            DirectUtf8Sequence acceptEncoding = headerParser.getHeader(HEADER_CONTENT_ACCEPT_ENCODING);
            if (configuration.getHttpContextConfiguration().allowDeflateBeforeSend()
                    && acceptEncoding != null
                    && Utf8s.containsAscii(acceptEncoding, "gzip")) {
                // re-read send buffer size in case the config was reloaded
                responseSink.setDeflateBeforeSend(true, configuration.getSendBufferSize());
            }

            try {
                if (newRequest) {
                    final boolean cookiesEnabled = configuration.getHttpContextConfiguration().areCookiesEnabled();
                    if (cookiesEnabled) {
                        if (!cookieHandler.parseCookies(this)) {
                            processor = rejectProcessor;
                        }
                    }

                    if (processor.requiresAuthentication() && !configureSecurityContext()) {
                        final byte requiredAuthType = processor.getRequiredAuthType();
                        processor = rejectProcessor.withAuthenticationType(requiredAuthType).reject(HTTP_UNAUTHORIZED);
                    }

                    if (cookiesEnabled) {
                        if (!processor.processServiceAccountCookie(this, securityContext)) {
                            processor = rejectProcessor;
                        }
                    }

                    try {
                        securityContext.checkEntityEnabled();
                    } catch (CairoException e) {
                        processor = rejectProcessor.reject(HTTP_FORBIDDEN, e.getFlyweightMessage());
                    }
                }

                if (!connectionCounted && !processor.ignoreConnectionLimitCheck()) {
                    processor = checkConnectionLimit(processor);
                }

                final long contentLength = headerParser.getContentLength();
                final boolean chunked = HttpKeywords.isChunked(headerParser.getHeader(HEADER_TRANSFER_ENCODING));
                final boolean multipartRequest = HttpKeywords.isContentTypeMultipartFormData(headerParser.getContentType())
                        || HttpKeywords.isContentTypeMultipartMixed(headerParser.getContentType());

                if (multipartRequest) {
                    busyRecv = consumeMultipart(socket, (HttpMultipartContentProcessor) processor, headerEnd, read, newRequest, rescheduleContext);
                } else if (chunked) {
                    busyRecv = consumeChunked((HttpPostPutProcessor) processor, headerEnd, read, newRequest);
                } else if (contentLength > 0) {
                    busyRecv = consumeContent(contentLength, socket, (HttpPostPutProcessor) processor, headerEnd, read, newRequest);
                } else {
                    // Do not expect any more bytes to be sent to us before
                    // we respond back to client. We will disconnect the client when
                    // they abuse protocol. In addition, we will not call processor
                    // if client has disconnected before we had a chance to reply.
                    read = socket.recv(recvBuffer, 1);

                    if (read != 0) {
                        dumpBuffer(recvBuffer, read);
                        LOG.info().$("disconnect after request [fd=").$(getFd()).$(", read=").$(read).I$();
                        int reason = read > 0 ? DISCONNECT_REASON_KICKED_OUT_AT_EXTRA_BYTES : DISCONNECT_REASON_PEER_DISCONNECT_AT_RECV;
                        throw registerDispatcherDisconnect(reason);
                    } else {
                        processor.onHeadersReady(this);
                        LOG.debug().$("good [fd=").$(getFd()).I$();
                        processor.onRequestComplete(this);
                        // Don't clear resumeHandlerId for protocol-switched connections (e.g., WebSocket)
                        if (!protocolSwitched) {
                            resumeHandlerId = NO_RESUME_PROCESSOR;
                        }
                        reset();
                    }
                }
            } catch (RetryOperationException e) {
                pendingRetry = true;
                scheduleRetry(processor, rescheduleContext);
                busyRecv = false;
            } catch (PeerDisconnectedException e) {
                return disconnectHttp(processor, DISCONNECT_REASON_PEER_DISCONNECT_AT_RECV);
            } catch (PeerIsSlowToReadException e) {
                LOG.debug().$("peer is slow reader [two]").$();
                // it is important to assign resume handler ID before we fire
                // event off to dispatcher
                processor.parkRequest(this, false);
                resumeHandlerId = (processor instanceof RejectProcessor)
                        ? HttpRequestProcessorSelector.REJECT_PROCESSOR_ID : currentHandlerId;
                throw registerDispatcherWrite();
            }
        } catch (ServerDisconnectException | PeerIsSlowToReadException | PeerIsSlowToWriteException e) {
            throw e;
        } catch (HttpException e) {
            LOG.error().$("http error [fd=").$(getFd()).$(", e=`").$safe(e.getFlyweightMessage()).$("`]").$();
            throw registerDispatcherDisconnect(DISCONNECT_REASON_PROTOCOL_VIOLATION);
        } catch (Throwable e) {
            LOG.error().$("internal error [fd=").$(getFd()).$(", e=`").$(e).$("`]").$();
            throw registerDispatcherDisconnect(DISCONNECT_REASON_SERVER_ERROR);
        }
        return busyRecv;
    }

    private HttpRequestProcessor resolveResumeProcessor(HttpRequestProcessorSelector selector) {
        if (resumeHandlerId == HttpRequestProcessorSelector.REJECT_PROCESSOR_ID) {
            return rejectProcessor;
        }
        HttpRequestProcessor processor = selector.resolveProcessorById(resumeHandlerId, headerParser);
        return processor != null ? processor : rejectProcessor;
    }

    private boolean handleClientSend(HttpRequestProcessorSelector selector) throws PeerIsSlowToReadException, ServerDisconnectException {
        if (resumeHandlerId != NO_RESUME_PROCESSOR) {
            final HttpRequestProcessor proc = resolveResumeProcessor(selector);
            try {
                proc.resumeSend(this);
                reset();
                return true;
            } catch (PeerIsSlowToReadException ignore) {
                proc.parkRequest(this, false);
                LOG.debug().$("peer is slow reader").$();
                throw registerDispatcherWrite();
                // resumeHandlerId stays set (re-park with same ID)
            } catch (PeerDisconnectedException ignore) {
                throw registerDispatcherDisconnect(DISCONNECT_REASON_PEER_DISCONNECT_AT_SEND);
            } catch (ServerDisconnectException ignore) {
                LOG.info().$("kicked out [fd=").$(getFd()).I$();
                throw registerDispatcherDisconnect(DISCONNECT_REASON_KICKED_OUT_AT_SEND);
            }
        } else {
            LOG.error().$("spurious write request [fd=").$(getFd()).I$();
        }
        return false;
    }

    private boolean keepConnectionAlive() {
        return !forceDisconnectOnComplete && configuration.getHttpContextConfiguration().getServerKeepAlive();
    }

    private boolean parseMultipartResult(
            long start,
            long buf,
            int bufRemaining,
            HttpMultipartContentProcessor processor,
            RescheduleContext rescheduleContext
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException, TooFewBytesReceivedException {
        boolean parseResult;
        try {
            parseResult = multipartContentParser.parse(start, buf, processor);
        } catch (RetryOperationException e) {
            this.multipartParserState.saveFdBufferPosition(multipartContentParser.getResumePtr(), buf, bufRemaining);
            throw e;
        } catch (NotEnoughLinesException e) {
            failProcessor(processor, e, DISCONNECT_REASON_KICKED_TXT_NOT_ENOUGH_LINES);
            parseResult = false;
        }

        if (parseResult) {
            // request is complete
            completeRequest(processor, rescheduleContext);
            return true;
        }
        return false;
    }

    private void shiftReceiveBufferUnprocessedBytes(long start, int receivedBytes) {
        // Shift to start
        this.receivedBytes = receivedBytes;
        Vect.memmove(recvBuffer, start, receivedBytes);
        LOG.debug().$("peer is slow, waiting for bigger part to parse [multipart]").$();
    }

    @Override
    protected void doInit() throws TlsSessionInitFailedException {
        // the context is obtained from the pool, so we should initialize the memory
        if (recvBuffer == 0) {
            // re-read recv buffer size in case the config was reloaded
            recvBufferSize = configuration.getRecvBufferSize();
            recvBufferReadSize = Math.min(forceFragmentationReceiveChunkSize, recvBufferSize);
            recvBuffer = Unsafe.malloc(recvBufferSize, MemoryTag.NATIVE_HTTP_CONN);
        }
        // re-read buffer sizes in case the config was reloaded
        responseSink.of(socket, configuration.getSendBufferSize());
        headerParser.reopen(configuration.getHttpContextConfiguration().getRequestHeaderBufferSize());
        multipartContentHeaderParser.reopen(configuration.getHttpContextConfiguration().getMultipartHeaderBufferSize());

        if (socket.supportsTls()) {
            socket.startTlsSession(null);
        }
        connectionCounted = false;
    }
}
