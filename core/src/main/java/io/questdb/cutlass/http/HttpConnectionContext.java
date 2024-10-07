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
import io.questdb.cutlass.http.ex.*;
import io.questdb.cutlass.http.processors.RejectProcessor;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.*;
import io.questdb.std.*;
import io.questdb.std.str.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.cairo.SecurityContext.AUTH_TYPE_NONE;
import static io.questdb.cutlass.http.HttpConstants.HEADER_CONTENT_ACCEPT_ENCODING;
import static io.questdb.cutlass.http.HttpConstants.HEADER_TRANSFER_ENCODING;
import static io.questdb.network.IODispatcher.*;
import static java.net.HttpURLConnection.*;

public class HttpConnectionContext extends IOContext<HttpConnectionContext> implements Locality, Retry {
    private static final Log LOG = LogFactory.getLog(HttpConnectionContext.class);
    private final HttpAuthenticator authenticator;
    private final ChunkedContentParser chunkedContentParser = new ChunkedContentParser();
    private final HttpContextConfiguration configuration;
    private final HttpCookieHandler cookieHandler;
    private final ObjectPool<DirectUtf8String> csPool;
    private final boolean dumpNetworkTraffic;
    private final int forceFragmentationReceiveChunkSize;
    private final HttpHeaderParser headerParser;
    private final boolean preAllocateBuffers;
    private final LocalValueMap localValueMap = new LocalValueMap();
    private final Metrics metrics;
    private final HttpHeaderParser multipartContentHeaderParser;
    private final HttpMultipartContentParser multipartContentParser;
    private final long multipartIdleSpinCount;
    private final MultipartParserState multipartParserState = new MultipartParserState();
    private final NetworkFacade nf;
    private final int recvBufferSize;
    private final RejectProcessor rejectProcessor;
    private final HttpResponseSink responseSink;
    private final RetryAttemptAttributes retryAttemptAttributes = new RetryAttemptAttributes();
    private final RescheduleContext retryRescheduleContext = retry -> {
        LOG.info().$("Retry is requested after successful writer allocation. Retry will be re-scheduled [thread=").$(Thread.currentThread().getId()).I$();
        throw RetryOperationException.INSTANCE;
    };
    private final AssociativeCache<RecordCursorFactory> selectCache;
    private long authenticationNanos = 0L;
    private int nCompletedRequests;
    private boolean pendingRetry = false;
    private int receivedBytes;
    private long recvBuffer;
    private long recvPos;
    private HttpRequestProcessor resumeProcessor = null;
    private SecurityContext securityContext;
    private SuspendEvent suspendEvent;
    private long totalBytesSent;
    private long totalReceived;

    @TestOnly
    public HttpConnectionContext(
            HttpMinServerConfiguration configuration,
            Metrics metrics,
            SocketFactory socketFactory
    ) {
        this(
                configuration,
                metrics,
                socketFactory,
                DefaultHttpCookieHandler.INSTANCE,
                DefaultHttpHeaderParserFactory.INSTANCE,
                HttpServer.NO_OP_CACHE
        );
    }

    public HttpConnectionContext(
            HttpMinServerConfiguration configuration,
            Metrics metrics,
            SocketFactory socketFactory,
            HttpCookieHandler cookieHandler,
            HttpHeaderParserFactory headerParserFactory,
            AssociativeCache<RecordCursorFactory> selectCache
    ) {
        super(
                socketFactory,
                configuration.getHttpContextConfiguration().getNetworkFacade(),
                LOG,
                metrics.jsonQuery().connectionCountGauge()
        );
        final HttpContextConfiguration contextConfiguration = configuration.getHttpContextConfiguration();
        this.configuration = contextConfiguration;
        this.cookieHandler = cookieHandler;
        this.nf = contextConfiguration.getNetworkFacade();
        this.csPool = new ObjectPool<>(DirectUtf8String.FACTORY, contextConfiguration.getConnectionStringPoolCapacity());
        this.headerParser = headerParserFactory.newParser(contextConfiguration.getRequestHeaderBufferSize(), csPool);
        this.multipartContentHeaderParser = new HttpHeaderParser(contextConfiguration.getMultipartHeaderBufferSize(), csPool);
        this.multipartContentParser = new HttpMultipartContentParser(multipartContentHeaderParser);
        this.responseSink = new HttpResponseSink(contextConfiguration);
        this.recvBufferSize = contextConfiguration.getRecvBufferSize();
        this.preAllocateBuffers = configuration.preAllocateBuffers();
        if (preAllocateBuffers) {
            recvBuffer = Unsafe.malloc(recvBufferSize, MemoryTag.NATIVE_HTTP_CONN);
            this.responseSink.open();
        }
        this.multipartIdleSpinCount = contextConfiguration.getMultipartIdleSpinCount();
        this.dumpNetworkTraffic = contextConfiguration.getDumpNetworkTraffic();
        // This is default behaviour until the security context is overridden with correct principal.
        this.securityContext = DenyAllSecurityContext.INSTANCE;
        this.metrics = metrics;
        this.authenticator = contextConfiguration.getFactoryProvider().getHttpAuthenticatorFactory().getHttpAuthenticator();
        this.rejectProcessor = contextConfiguration.getFactoryProvider().getRejectProcessorFactory().getRejectProcessor(this);
        this.forceFragmentationReceiveChunkSize = contextConfiguration.getForceRecvFragmentationChunkSize();
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
        }
        this.localValueMap.disconnect();
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
        this.authenticator.close();
        Misc.free(selectCache);
        LOG.debug().$("closed [fd=").$(fd).I$();
    }

    @Override
    public void fail(HttpRequestProcessorSelector selector, HttpException e) throws PeerIsSlowToReadException, ServerDisconnectException, PeerDisconnectedException {
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
            if (configuration.getServerKeepAlive()) {
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
    public void init() {
        if (socket.supportsTls()) {
            if (socket.startTlsSession(null) != 0) {
                throw CairoException.nonCritical().put("failed to start TLS session");
            }
        }
    }

    @Override
    public boolean invalid() {
        return pendingRetry || receivedBytes > 0 || this.socket == null;
    }

    @Override
    public HttpConnectionContext of(long fd, @NotNull IODispatcher<HttpConnectionContext> dispatcher) {
        super.of(fd, dispatcher);
        // The context is obtained from the pool, so we should initialize the memory.
        if (recvBuffer == 0) {
            recvBuffer = Unsafe.malloc(recvBufferSize, MemoryTag.NATIVE_HTTP_CONN);
        }
        responseSink.of(socket);
        return this;
    }

    public HttpRequestProcessor rejectRequest(int code, CharSequence userMessage) {
        return rejectRequest(code, userMessage, null, null);
    }

    public HttpRequestProcessor rejectRequest(int code, byte authenticationType) {
        LOG.error().$("rejecting request [code=").$(code).I$();
        return rejectProcessor.rejectRequest(code, null, null, null, authenticationType);
    }

    public HttpRequestProcessor rejectRequest(int code, CharSequence userMessage, CharSequence cookieName, CharSequence cookieValue) {
        LOG.error().$(userMessage).$(" [code=").$(code).I$();
        return rejectProcessor.rejectRequest(code, userMessage, cookieName, cookieValue, AUTH_TYPE_NONE);
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

    public void scheduleRetry(HttpRequestProcessor processor, RescheduleContext rescheduleContext) throws PeerIsSlowToReadException, ServerDisconnectException, PeerDisconnectedException {
        try {
            pendingRetry = true;
            rescheduleContext.reschedule(this);
        } catch (RetryFailedOperationException e) {
            failProcessor(processor, e, DISCONNECT_REASON_RETRY_FAILED);
        }
    }

    public HttpResponseSink.SimpleResponseImpl simpleResponse() {
        return responseSink.getSimple();
    }

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
                            (HttpMultipartContentListener) processor,
                            processor,
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
            throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException, PeerIsSlowToWriteException {
        reset();
        if (configuration.getServerKeepAlive()) {
            while (handleClientRecv(selector, rescheduleContext)) ;
        } else {
            throw registerDispatcherDisconnect(DISCONNECT_REASON_KEEPALIVE_OFF);
        }
    }

    private HttpRequestProcessor checkProcessorValidForRequest(
            Utf8Sequence method,
            HttpRequestProcessor processor,
            boolean chunked,
            boolean multipartRequest,
            long contentLength,
            boolean multipartProcessor
    ) {

        if (processor.isErrorProcessor()) {
            return processor;
        }

        if (Utf8s.equalsNcAscii("POST", method) || Utf8s.equalsNcAscii("PUT", method)) {
            if (!multipartProcessor) {
                if (multipartRequest) {
                    return rejectRequest(HTTP_NOT_FOUND, "Method (multipart POST) not supported");
                } else {
                    return rejectRequest(HTTP_NOT_FOUND, "Method not supported");
                }
            }
            if (chunked && contentLength > 0) {
                return rejectRequest(HTTP_BAD_REQUEST, "Invalid chunked request; content-length specified");
            }
            if (!chunked && !multipartRequest && contentLength < 0) {
                return rejectRequest(HTTP_BAD_REQUEST, "Content-length not specified for POST/PUT request");
            }
        } else if (Utf8s.equalsNcAscii("GET", method)) {
            if (chunked || multipartRequest || contentLength > 0) {
                return rejectRequest(HTTP_BAD_REQUEST, "GET request method cannot have content");
            }
            if (multipartProcessor) {
                return rejectRequest(HTTP_NOT_FOUND, "Method GET not supported");
            }
        } else {
            return rejectRequest(HTTP_BAD_REQUEST, "Method not supported");
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
            final long authenticationStart = configuration.getNanosecondClock().getTicks();
            if (!authenticator.authenticate(headerParser)) {
                // authenticationNanos stays 0, when it fails this value is irrelevant
                return false;
            }
            securityContext = configuration.getFactoryProvider().getSecurityContextFactory().getInstance(
                    authenticator.getPrincipal(),
                    authenticator.getGroups(),
                    authenticator.getAuthType(),
                    SecurityContextFactory.HTTP
            );
            authenticationNanos = configuration.getNanosecondClock().getTicks() - authenticationStart;
        }
        return true;
    }

    private boolean consumeChunked(HttpRequestProcessor processor, long headerEnd, long read, boolean newRequest) throws PeerIsSlowToReadException, ServerDisconnectException, PeerDisconnectedException, QueryPausedException, PeerIsSlowToWriteException {
        HttpMultipartContentListener contentProcessor = (HttpMultipartContentListener) processor;
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
                lo = chunkedContentParser.handleRecv(lo, hi, contentProcessor);
                if (lo == Long.MAX_VALUE) {
                    // done
                    processor.onRequestComplete(this);
                    reset();
                    if (configuration.getServerKeepAlive()) {
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
            HttpRequestProcessor processor,
            long headerEnd,
            int read,
            boolean newRequest
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException, QueryPausedException, PeerIsSlowToWriteException {
        HttpMultipartContentListener contentProcessor = (HttpMultipartContentListener) processor;
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
                read = socket.recv(recvBuffer, recvBufferSize);
                lo = recvBuffer;
            }

            if (read > 0) {
                if (totalReceived + read > contentLength) {
                    // HTTP protocol violation
                    // client sent more data than it promised in Content-Length header
                    // we will disconnect client and roll back
                    return disconnectHttp(processor, DISCONNECT_REASON_KICKED_OUT_AT_EXTRA_BYTES);
                }

                contentProcessor.onChunk(lo, recvBuffer + read);
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
                    if (configuration.getServerKeepAlive()) {
                        return true;
                    } else {
                        return disconnectHttp(processor, DISCONNECT_REASON_KEEPALIVE_OFF_RECV);
                    }
                }
            } else if (read == 0) {
                // Schedule for read
                throw registerDispatcherRead();
            } else {
                // client disconnected
                return disconnectHttp(processor, DISCONNECT_REASON_KICKED_OUT_AT_RECV);
            }
        }
    }

    private boolean consumeMultipart(
            Socket socket,
            HttpRequestProcessor processor,
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

        final HttpMultipartContentListener multipartListener = (HttpMultipartContentListener) processor;
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

        return continueConsumeMultipart(socket, start, buf, bufRemaining, multipartListener, processor, rescheduleContext);
    }

    private boolean continueConsumeMultipart(
            Socket socket,
            long start,
            long buf,
            int bufRemaining,
            HttpMultipartContentListener multipartListener,
            HttpRequestProcessor processor,
            RescheduleContext rescheduleContext
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException, QueryPausedException, PeerIsSlowToWriteException {
        boolean keepGoing = false;

        if (buf > start) {
            try {
                if (parseMultipartResult(start, buf, bufRemaining, multipartListener, processor, rescheduleContext)) {
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
                        if (parseMultipartResult(start, buf, bufRemaining, multipartListener, processor, rescheduleContext)) {
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
                        if (parseMultipartResult(start, buf, bufRemaining, multipartListener, processor, rescheduleContext)) {
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
                    .$(", error=").$(e.getFlyweightMessage())
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
        HttpRequestProcessor processor;
        final Utf8Sequence url = headerParser.getUrl();
        processor = selector.select(url);
        if (processor == null) {
            return selector.getDefaultProcessor();
        }
        return processor;
    }

    private boolean handleClientRecv(HttpRequestProcessorSelector selector, RescheduleContext rescheduleContext) throws PeerIsSlowToReadException, PeerIsSlowToWriteException, ServerDisconnectException {
        boolean busyRecv = true;
        try {
            // this is address of where header ended in our receive buffer
            // we need to being processing request content starting from this address
            long headerEnd = recvBuffer;
            int read = 0;
            final boolean newRequest = headerParser.isIncomplete();
            if (newRequest) {
                while (headerParser.isIncomplete()) {
                    // read headers
                    read = socket.recv(recvBuffer, recvBufferSize);
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
            }

            final Utf8Sequence url = headerParser.getUrl();
            if (url == null) {
                throw HttpException.instance("missing URL");
            }
            HttpRequestProcessor processor = rejectProcessor.isRequestBeingRejected() ? rejectProcessor : getHttpRequestProcessor(selector);
            long contentLength = headerParser.getContentLength();
            final boolean chunked = Utf8s.equalsNcAscii("chunked", headerParser.getHeader(HEADER_TRANSFER_ENCODING));
            final boolean multipartRequest = Utf8s.equalsNcAscii("multipart/form-data", headerParser.getContentType())
                    || Utf8s.equalsNcAscii("multipart/mixed", headerParser.getContentType());
            final boolean multipartProcessor = processor instanceof HttpMultipartContentListener;

            DirectUtf8Sequence acceptEncoding = headerParser.getHeader(HEADER_CONTENT_ACCEPT_ENCODING);
            if (configuration.allowDeflateBeforeSend() && acceptEncoding != null && Utf8s.containsAscii(acceptEncoding, "gzip")) {
                responseSink.setDeflateBeforeSend(true);
            }

            try {
                final byte requiredAuthType = processor.getRequiredAuthType();
                if (newRequest) {
                    if (processor.requiresAuthentication() && !configureSecurityContext()) {
                        processor = rejectRequest(HTTP_UNAUTHORIZED, requiredAuthType);
                    }

                    if (configuration.areCookiesEnabled()) {
                        if (!processor.processCookies(this, securityContext)) {
                            processor = rejectProcessor;
                        }
                    }

                    try {
                        securityContext.checkEntityEnabled();
                    } catch (CairoException e) {
                        processor = rejectRequest(HTTP_FORBIDDEN, e.getFlyweightMessage());
                    }
                }

                processor = checkProcessorValidForRequest(
                        headerParser.getMethod(),
                        processor,
                        chunked,
                        multipartRequest,
                        contentLength,
                        multipartProcessor
                );

                if (chunked) {
                    busyRecv = consumeChunked(processor, headerEnd, read, newRequest);
                } else if (multipartRequest) {
                    busyRecv = consumeMultipart(socket, processor, headerEnd, read, newRequest, rescheduleContext);
                } else if (contentLength > 0) {
                    busyRecv = consumeContent(contentLength, socket, processor, headerEnd, read, newRequest);
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
            LOG.error().$("http error [fd=").$(getFd()).$(", e=`").$(e.getFlyweightMessage()).$("`]").$();
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
            HttpMultipartContentListener multipartListener,
            HttpRequestProcessor processor,
            RescheduleContext rescheduleContext
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException, QueryPausedException, TooFewBytesReceivedException {
        boolean parseResult;
        try {
            parseResult = multipartContentParser.parse(start, buf, multipartListener);
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

}
