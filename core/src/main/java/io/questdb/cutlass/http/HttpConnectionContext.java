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

package io.questdb.cutlass.http;

import io.questdb.Metrics;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.security.DenyAllSecurityContext;
import io.questdb.cairo.security.SecurityContextFactory;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.http.ex.BufferOverflowException;
import io.questdb.cutlass.http.ex.NotEnoughLinesException;
import io.questdb.cutlass.http.ex.RetryFailedOperationException;
import io.questdb.cutlass.http.ex.RetryOperationException;
import io.questdb.cutlass.http.ex.TooFewBytesReceivedException;
import io.questdb.cutlass.http.processors.RejectProcessor;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.metrics.AtomicLongGauge;
import io.questdb.network.HeartBeatException;
import io.questdb.network.IOContext;
import io.questdb.network.IOOperation;
import io.questdb.network.Net;
import io.questdb.network.NetworkFacade;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.PeerIsSlowToWriteException;
import io.questdb.network.QueryPausedException;
import io.questdb.network.ServerDisconnectException;
import io.questdb.network.Socket;
import io.questdb.network.SocketFactory;
import io.questdb.network.SuspendEvent;
import io.questdb.network.TlsSessionInitFailedException;
import io.questdb.std.AssociativeCache;
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
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.cutlass.http.HttpConstants.HEADER_CONTENT_ACCEPT_ENCODING;
import static io.questdb.cutlass.http.HttpConstants.HEADER_TRANSFER_ENCODING;
import static io.questdb.network.IODispatcher.*;
import static java.net.HttpURLConnection.*;

public class HttpConnectionContext extends IOContext<HttpConnectionContext> implements Locality, Retry {
    private static final Log LOG = LogFactory.getLog(HttpConnectionContext.class);
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
    private final HttpSessionStore sessionStore;
    private long authenticationNanos = 0L;
    private AtomicLongGauge connectionCountGauge;
    private boolean connectionCounted;
    private int nCompletedRequests;
    private boolean pendingRetry = false;
    private int receivedBytes;
    private long recvBuffer;
    private int recvBufferReadSize;
    private int recvBufferSize;
    private long recvPos;
    private HttpRequestProcessor resumeProcessor = null;
    private SecurityContext securityContext;
    private String sessionId;
    private SuspendEvent suspendEvent;
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
                DefaultHttpCookieHandler.INSTANCE,
                DefaultHttpSessionStore.INSTANCE,
                DefaultHttpHeaderParserFactory.INSTANCE,
                HttpServer.NO_OP_CACHE
        );
    }

    public HttpConnectionContext(
            HttpServerConfiguration configuration,
            SocketFactory socketFactory,
            HttpCookieHandler cookieHandler,
            HttpSessionStore sessionStore,
            HttpHeaderParserFactory headerParserFactory,
            AssociativeCache<RecordCursorFactory> selectCache
    ) {
        super(
                socketFactory,
                configuration.getHttpContextConfiguration().getNetworkFacade(),
                LOG
        );
        this.configuration = configuration;
        this.cookieHandler = cookieHandler;
        this.sessionStore = sessionStore;
        final HttpContextConfiguration contextConfiguration = configuration.getHttpContextConfiguration();
        this.nf = contextConfiguration.getNetworkFacade();
        this.csPool = new ObjectPool<>(DirectUtf8String.FACTORY, contextConfiguration.getConnectionStringPoolCapacity());
        this.headerParser = headerParserFactory.newParser(contextConfiguration.getRequestHeaderBufferSize(), csPool);
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

    @Override
    public void clear() {
        LOG.debug().$("clear [fd=").$(getFd()).I$();
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
        this.localValueMap.disconnect();

        if (connectionCountGauge != null) {
            connectionCountGauge.dec();
            connectionCounted = false;
            connectionCountGauge = null;
        }
    }

    @Override
    public void clearSuspendEvent() {
        suspendEvent = Misc.free(suspendEvent);
    }

    @Override
    public void close() {
        final long fd = getFd();
        LOG.debug().$("close [fd=").$(fd).I$();
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
        this.recvBuffer = Unsafe.free(recvBuffer, recvBufferSize, MemoryTag.NATIVE_HTTP_CONN);
        this.responseSink.close();
        this.receivedBytes = 0;
        this.securityContext = DenyAllSecurityContext.INSTANCE;
        this.sessionId = null;
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

    public AssociativeCache<RecordCursorFactory> getSelectCache() {
        return selectCache;
    }

    public String getSessionId() {
        return sessionId;
    }

    @Override
    public SuspendEvent getSuspendEvent() {
        return suspendEvent;
    }

    public long getTotalBytesSent() {
        return totalBytesSent;
    }

    public long getTotalReceived() {
        return totalReceived;
    }

    public boolean handleClientOperation(int operation, HttpRequestProcessorSelector selector, RescheduleContext rescheduleContext)
            throws HeartBeatException, PeerIsSlowToReadException, ServerDisconnectException, PeerIsSlowToWriteException {
        boolean keepGoing;
        switch (operation) {
            case IOOperation.READ:
                keepGoing = handleClientRecv(selector, rescheduleContext);
                break;
            case IOOperation.WRITE:
                keepGoing = handleClientSend();
                break;
            case IOOperation.HEARTBEAT:
                throw registerDispatcherHeartBeat();
            default:
                throw registerDispatcherDisconnect(DISCONNECT_REASON_UNKNOWN_OPERATION);
        }

        boolean useful = keepGoing;
        if (keepGoing) {
            if (configuration.getHttpContextConfiguration().getServerKeepAlive()) {
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

    public void reset() {
        LOG.debug().$("reset [fd=").$(getFd()).$(']').$();
        this.totalBytesSent += responseSink.getTotalBytesSent();
        this.responseSink.clear();
        this.nCompletedRequests++;
        this.resumeProcessor = null;
        this.headerParser.clear();
        this.multipartContentParser.clear();
        this.multipartContentHeaderParser.clear();
        this.csPool.clear();
        this.localValueMap.clear();
        this.multipartParserState.multipartRetry = false;
        this.retryAttemptAttributes.waitStartTimestamp = 0;
        this.retryAttemptAttributes.lastRunTimestamp = 0;
        this.retryAttemptAttributes.attempt = 0;
        this.receivedBytes = 0;
        this.authenticationNanos = 0L;
        this.securityContext = DenyAllSecurityContext.INSTANCE;
        this.sessionId = null;
        this.authenticator.clear();
        this.totalReceived = 0;
        this.chunkedContentParser.clear();
        this.recvPos = recvBuffer;
        this.rejectProcessor.clear();
        clearSuspendEvent();
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
                resumeProcessor = processor;
                throw registerDispatcherWrite();
            } catch (QueryPausedException e) {
                LOG.info().$("partition is in cold storage, suspending query [fd=").$(getFd())
                        .$(", thread=").$(Thread.currentThread().getId()).I$();
                processor.parkRequest(this, true);
                resumeProcessor = processor;
                suspendEvent = e.getEvent();
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
        if (configuration.getHttpContextConfiguration().getServerKeepAlive()) {
            while (handleClientRecv(selector, rescheduleContext)) ;
        } else {
            throw registerDispatcherDisconnect(DISCONNECT_REASON_KEEPALIVE_OFF);
        }
    }

    private HttpRequestProcessor checkConnectionLimit(HttpRequestProcessor processor) {
        final int connectionLimit = processor.getConnectionLimit(configuration.getHttpContextConfiguration());
        if (connectionLimit > -1) {
            connectionCountGauge = processor.connectionCountGauge(metrics);
            final long numOfConnections = connectionCountGauge.incrementAndGet();
            if (numOfConnections > connectionLimit) {
                rejectProcessor.getMessageSink()
                        .put("exceeded connection limit [name=").put(connectionCountGauge.getName())
                        .put(", numOfConnections=").put(numOfConnections)
                        .put(", connectionLimit=").put(connectionLimit)
                        .put(']');
                return rejectProcessor.withShutdownWrite().reject(HTTP_BAD_REQUEST);
            }
            if (numOfConnections == connectionLimit && !securityContext.isSystemAdmin()) {
                rejectProcessor.getMessageSink()
                        .put("non-admin user reached connection limit [name=").put(connectionCountGauge.getName())
                        .put(", numOfConnections=").put(numOfConnections)
                        .put(", connectionLimit=").put(connectionLimit)
                        .put(']');
                return rejectProcessor.withShutdownWrite().reject(HTTP_BAD_REQUEST);
            }
        }
        return processor;
    }

    private void completeRequest(
            HttpRequestProcessor processor,
            RescheduleContext rescheduleContext
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException, QueryPausedException {
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
            final HttpSessionStore.SessionInfo sessionInfo = cookieHandler.processSessionCookie(this, sessionStore);
            if (!authenticator.authenticate(headerParser)) {
                // auth failed, fallback to session cookie if there is one
                if (sessionInfo != null) {
                    // create security context from session info
                    securityContext = configuration.getFactoryProvider().getSecurityContextFactory().getInstance(
                            sessionInfo.getPrincipal(),
                            sessionInfo.getGroups(),
                            sessionInfo.getAuthType(),
                            SecurityContextFactory.HTTP
                    );
                    authenticationNanos = clock.getTicks() - authenticationStart;
                    return true;
                }

                // authenticationNanos stays 0, when it fails this value is irrelevant
                return false;
            }

            // auth successful, create security context from auth info
            securityContext = configuration.getFactoryProvider().getSecurityContextFactory().getInstance(
                    authenticator.getPrincipal(),
                    authenticator.getGroups(),
                    authenticator.getAuthType(),
                    SecurityContextFactory.HTTP
            );
            // create session, if we do not have one yet
            if (sessionInfo == null || !Chars.equals(sessionInfo.getPrincipal(), securityContext.getPrincipal())) {
                sessionId = sessionStore.createSession(authenticator);
            }
            authenticationNanos = clock.getTicks() - authenticationStart;
        }
        return true;
    }

    private boolean consumeChunked(HttpPostPutProcessor processor, long headerEnd, long read, boolean newRequest) throws PeerIsSlowToReadException, ServerDisconnectException, PeerDisconnectedException, QueryPausedException, PeerIsSlowToWriteException {
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
                    if (configuration.getHttpContextConfiguration().getServerKeepAlive()) {
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
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException, QueryPausedException, PeerIsSlowToWriteException {
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
                    if (configuration.getHttpContextConfiguration().getServerKeepAlive()) {
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
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException, QueryPausedException, PeerIsSlowToWriteException {
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
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException, QueryPausedException, PeerIsSlowToWriteException {
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
            resumeProcessor = processor;
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
        return requestValidator.validateRequestType(processor, rejectProcessor);
    }

    private boolean handleClientRecv(HttpRequestProcessorSelector selector, RescheduleContext rescheduleContext) throws PeerIsSlowToReadException, PeerIsSlowToWriteException, ServerDisconnectException {
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
                        if (!cookieHandler.processServiceAccountCookie(this, securityContext)) {
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
                    connectionCounted = true;
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
                        resumeProcessor = null;
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
                // it is important to assign resume processor before we fire
                // event off to dispatcher
                processor.parkRequest(this, false);
                resumeProcessor = processor;
                throw registerDispatcherWrite();
            } catch (QueryPausedException e) {
                LOG.debug().$("partition is in cold storage").$();
                // it is important to assign resume processor before we fire
                // event off to dispatcher
                processor.parkRequest(this, true);
                resumeProcessor = processor;
                suspendEvent = e.getEvent();
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

    private boolean handleClientSend() throws PeerIsSlowToReadException, ServerDisconnectException {
        if (resumeProcessor != null) {
            try {
                resumeProcessor.resumeSend(this);
                reset();
                return true;
            } catch (PeerIsSlowToReadException ignore) {
                resumeProcessor.parkRequest(this, false);
                LOG.debug().$("peer is slow reader").$();
                throw registerDispatcherWrite();
            } catch (QueryPausedException e) {
                resumeProcessor.parkRequest(this, true);
                suspendEvent = e.getEvent();
                LOG.debug().$("partition is in cold storage").$();
                throw registerDispatcherWrite();
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

    private boolean parseMultipartResult(
            long start,
            long buf,
            int bufRemaining,
            HttpMultipartContentProcessor processor,
            RescheduleContext rescheduleContext
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException, QueryPausedException, TooFewBytesReceivedException {
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
