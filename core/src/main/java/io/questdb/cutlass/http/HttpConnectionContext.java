/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.cairo.CairoSecurityContext;
import io.questdb.cairo.security.CairoSecurityContextImpl;
import io.questdb.cutlass.http.ex.*;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.*;
import io.questdb.std.*;
import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.std.str.StdoutSink;

import static io.questdb.network.IODispatcher.*;

public class HttpConnectionContext implements IOContext, Locality, Mutable, Retry {
    private static final Log LOG = LogFactory.getLog(HttpConnectionContext.class);
    private final HttpHeaderParser headerParser;
    private final long recvBuffer;
    private final int recvBufferSize;
    private final HttpMultipartContentParser multipartContentParser;
    private final HttpHeaderParser multipartContentHeaderParser;
    private final HttpResponseSink responseSink;
    private final ObjectPool<DirectByteCharSequence> csPool;
    private final LocalValueMap localValueMap = new LocalValueMap();
    private final NetworkFacade nf;
    private final long multipartIdleSpinCount;
    private final CairoSecurityContext cairoSecurityContext;
    private final boolean dumpNetworkTraffic;
    private final boolean allowDeflateBeforeSend;
    private final MultipartParserState multipartParserState = new MultipartParserState();
    private final RetryAttemptAttributes retryAttemptAttributes = new RetryAttemptAttributes();
    private final RescheduleContext retryRescheduleContext = retry -> {
        LOG.info().$("Retry is requested after successful writer allocation. Retry will be re-scheduled [thread=").$(Thread.currentThread().getId()).$(']');
        throw RetryOperationException.INSTANCE;
    };
    private final boolean serverKeepAlive;
    private final Runnable onPeerDisconnect;
    private long fd;
    private HttpRequestProcessor resumeProcessor = null;
    private boolean pendingRetry = false;
    private IODispatcher<HttpConnectionContext> dispatcher;
    private int nCompletedRequests;
    private long totalBytesSent;
    private int receivedBytes;

    public HttpConnectionContext(HttpContextConfiguration configuration) {
        this.nf = configuration.getNetworkFacade();
        this.csPool = new ObjectPool<>(DirectByteCharSequence.FACTORY, configuration.getConnectionStringPoolCapacity());
        this.headerParser = new HttpHeaderParser(configuration.getRequestHeaderBufferSize(), csPool);
        this.multipartContentHeaderParser = new HttpHeaderParser(configuration.getMultipartHeaderBufferSize(), csPool);
        this.multipartContentParser = new HttpMultipartContentParser(multipartContentHeaderParser);
        this.recvBufferSize = configuration.getRecvBufferSize();
        this.recvBuffer = Unsafe.malloc(recvBufferSize, MemoryTag.NATIVE_HTTP_CONN);
        this.responseSink = new HttpResponseSink(configuration);
        this.multipartIdleSpinCount = configuration.getMultipartIdleSpinCount();
        this.dumpNetworkTraffic = configuration.getDumpNetworkTraffic();
        this.allowDeflateBeforeSend = configuration.allowDeflateBeforeSend();
        this.cairoSecurityContext = new CairoSecurityContextImpl(!configuration.readOnlySecurityContext());
        this.serverKeepAlive = configuration.getServerKeepAlive();
        this.onPeerDisconnect = configuration.onPeerDisconnect();
    }

    @Override
    public void clear() {
        LOG.debug().$("clear [fd=").$(fd).$(']').$();
        totalBytesSent += responseSink.getTotalBytesSent();
        nCompletedRequests++;
        this.resumeProcessor = null;
        this.headerParser.clear();
        this.multipartContentParser.clear();
        this.multipartContentHeaderParser.clear();
        this.csPool.clear();
        this.localValueMap.clear();
        this.responseSink.clear();
        if (this.pendingRetry) {
            LOG.error().$("Reused context with retry pending.").$();
        }
        this.pendingRetry = false;
        this.multipartParserState.multipartRetry = false;
        this.retryAttemptAttributes.waitStartTimestamp = 0;
        this.retryAttemptAttributes.lastRunTimestamp = 0;
        this.retryAttemptAttributes.attempt = 0;
        this.receivedBytes = 0;
    }

    @Override
    public void close() {
        this.fd = -1;
        nCompletedRequests = 0;
        totalBytesSent = 0;
        csPool.clear();
        multipartContentParser.close();
        multipartContentHeaderParser.close();
        responseSink.close();
        headerParser.close();
        localValueMap.close();
        Unsafe.free(recvBuffer, recvBufferSize, MemoryTag.NATIVE_HTTP_CONN);
        if (this.pendingRetry) {
            LOG.error().$("Closed context with retry pending.").$();
        }
        this.pendingRetry = false;
        this.receivedBytes = 0;
        LOG.debug().$("closed").$();
    }

    @Override
    public long getFd() {
        return fd;
    }

    @Override
    public boolean invalid() {
        return pendingRetry || receivedBytes > 0 || this.fd == -1;
    }

    @Override
    public IODispatcher<HttpConnectionContext> getDispatcher() {
        return dispatcher;
    }

    @Override
    public void fail(HttpRequestProcessorSelector selector, HttpException e) {
        LOG.info().$("failed to retry query [fd=").$(fd).$(']').$();
        HttpRequestProcessor processor = getHttpRequestProcessor(selector);
        failProcessor(processor, e, DISCONNECT_REASON_RETRY_FAILED);
    }

    @Override
    public RetryAttemptAttributes getAttemptDetails() {
        return retryAttemptAttributes;
    }

    public boolean tryRerun(HttpRequestProcessorSelector selector, RescheduleContext rescheduleContext) {
        if (pendingRetry) {
            pendingRetry = false;
            HttpRequestProcessor processor = getHttpRequestProcessor(selector);
            try {
                LOG.info().$("retrying query [fd=").$(fd).$(']').$();
                processor.onRequestRetry(this);
                if (multipartParserState.multipartRetry) {
                    if (continueConsumeMultipart(
                            fd,
                            multipartParserState.start,
                            multipartParserState.buf,
                            multipartParserState.bufRemaining,
                            (HttpMultipartContentListener) processor,
                            processor,
                            retryRescheduleContext
                    )) {
                        LOG.info().$("success retried multipart import [fd=").$(fd).$(']').$();
                        busyRcvLoop(selector, rescheduleContext);
                    } else {
                        LOG.info().$("retry success but import not finished [fd=").$(fd).$(']').$();
                    }
                } else {
                    busyRcvLoop(selector, rescheduleContext);
                }
            } catch (RetryOperationException e2) {
                pendingRetry = true;
                return false;
            } catch (PeerDisconnectedException ignore) {
                handlePeerDisconnect(DISCONNECT_REASON_PEER_DISCONNECT_AT_RERUN);
            } catch (PeerIsSlowToReadException e2) {
                LOG.info().$("peer is slow on running the rerun [fd=").$(fd).$(", thread=")
                        .$(Thread.currentThread().getId()).$(']').$();
                processor.parkRequest(this);
                resumeProcessor = processor;
                dispatcher.registerChannel(this, IOOperation.WRITE);
            } catch (ServerDisconnectException e) {
                LOG.info().$("kicked out [fd=").$(fd).$(']').$();
                dispatcher.disconnect(this, DISCONNECT_REASON_KICKED_OUT_AT_RERUN);
            }
        }
        return true;
    }

    public CairoSecurityContext getCairoSecurityContext() {
        return cairoSecurityContext;
    }

    public HttpChunkedResponseSocket getChunkedResponseSocket() {
        return responseSink.getChunkedSocket();
    }

    public long getLastRequestBytesSent() {
        return responseSink.getTotalBytesSent();
    }

    @Override
    public LocalValueMap getMap() {
        return localValueMap;
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

    public long getTotalBytesSent() {
        return totalBytesSent;
    }

    public void handleClientOperation(int operation, HttpRequestProcessorSelector selector, RescheduleContext rescheduleContext) {
        boolean keepGoing;
        switch (operation) {
            case IOOperation.READ:
                keepGoing = handleClientRecv(selector, rescheduleContext);
                break;
            case IOOperation.WRITE:
                keepGoing = handleClientSend();
                break;
            default:
                dispatcher.disconnect(this, DISCONNECT_REASON_UNKNOWN_OPERATION);
                keepGoing = false;
                break;
        }

        if (keepGoing) {
            if (serverKeepAlive) {
                do {
                    keepGoing = handleClientRecv(selector, rescheduleContext);
                } while (keepGoing);
            } else {
                dispatcher.disconnect(this, DISCONNECT_REASON_KEEPALIVE_OFF);
            }
        }
    }

    public HttpConnectionContext of(long fd, IODispatcher<HttpConnectionContext> dispatcher) {
        this.fd = fd;
        this.dispatcher = dispatcher;
        this.responseSink.of(fd);
        return this;
    }

    public void scheduleRetry(HttpRequestProcessor processor, RescheduleContext rescheduleContext) {
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

    @SuppressWarnings("StatementWithEmptyBody")
    private void busyRcvLoop(HttpRequestProcessorSelector selector, RescheduleContext rescheduleContext) {
        clear();
        if (serverKeepAlive) {
            while (handleClientRecv(selector, rescheduleContext)) ;
        } else {
            dispatcher.disconnect(this, DISCONNECT_REASON_KEEPALIVE_OFF_RECV);
        }
    }

    private void completeRequest(HttpRequestProcessor processor, RescheduleContext rescheduleContext) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        LOG.debug().$("complete [fd=").$(fd).$(']').$();
        try {
            processor.onRequestComplete(this);
            clear();
        } catch (RetryOperationException e) {
            pendingRetry = true;
            scheduleRetry(processor, rescheduleContext);
        }
    }

    private boolean consumeMultipart(
            long fd,
            HttpRequestProcessor processor,
            long headerEnd,
            int read,
            boolean newRequest,
            RescheduleContext rescheduleContext
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        if (newRequest) {
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

        return continueConsumeMultipart(fd, start, buf, bufRemaining, multipartListener, processor, rescheduleContext);
    }

    private boolean continueConsumeMultipart(
            long fd,
            long start,
            long buf,
            int bufRemaining,
            HttpMultipartContentListener multipartListener,
            HttpRequestProcessor processor,
            RescheduleContext rescheduleContext
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
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
            final int n = nf.recv(fd, buf, bufRemaining);
            if (n < 0) {
                handlePeerDisconnect(DISCONNECT_REASON_PEER_DISCONNECT_AT_MULTIPART_RECV);
                break;
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
                        dispatcher.registerChannel(this, IOOperation.READ);
                        break;
                    }
                }

                LOG.debug().$("peer is slow [multipart]").$();
                dispatcher.registerChannel(this, IOOperation.READ);
                break;
            }

            LOG.debug().$("multipart recv [len=").$(n).$(']').$();

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
                        dispatcher.registerChannel(this, IOOperation.READ);
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

    private void dumpBuffer(long buffer, int size) {
        if (dumpNetworkTraffic && size > 0) {
            StdoutSink.INSTANCE.put('>');
            Net.dump(buffer, size);
        }
    }

    private void failProcessor(HttpRequestProcessor processor, HttpException e, int reason) {
        pendingRetry = false;
        boolean canClear = true;
        try {
            LOG.info()
                    .$("failed query result cannot be delivered. Kicked out [fd=").$(fd)
                    .$(", error=").$(e.getFlyweightMessage())
                    .I$();
            processor.failRequest(this, e);
            dispatcher.disconnect(this, reason);
        } catch (PeerDisconnectedException peerDisconnectedException) {
            handlePeerDisconnect(DISCONNECT_REASON_PEER_DISCONNECT_AT_SEND);
        } catch (PeerIsSlowToReadException peerIsSlowToReadException) {
            LOG.info().$("peer is slow to receive failed to retry response [fd=").$(fd).$(']').$();
            processor.parkRequest(this);
            resumeProcessor = processor;
            dispatcher.registerChannel(this, IOOperation.WRITE);
            canClear = false;
        } catch (ServerDisconnectException serverDisconnectException) {
            dispatcher.disconnect(this, reason);
        } finally {
            if (canClear) {
                clear();
            }
        }
    }

    private HttpRequestProcessor getHttpRequestProcessor(HttpRequestProcessorSelector selector) {
        HttpRequestProcessor processor = selector.select(headerParser.getUrl());

        if (processor == null) {
            processor = selector.getDefaultProcessor();
        }
        return processor;
    }

    private boolean handleClientRecv(HttpRequestProcessorSelector selector, RescheduleContext rescheduleContext) {
        boolean busyRecv = true;
        try {
            final long fd = this.fd;
            // this is address of where header ended in our receive buffer
            // we need to being processing request content starting from this address
            long headerEnd = recvBuffer;
            int read = 0;
            final boolean newRequest = headerParser.isIncomplete();
            if (newRequest) {
                while (headerParser.isIncomplete()) {
                    // read headers
                    read = nf.recv(fd, recvBuffer, recvBufferSize);
                    LOG.debug().$("recv [fd=").$(fd).$(", count=").$(read).$(']').$();
                    if (read < 0) {
                        LOG.debug()
                                .$("done [fd=").$(fd)
                                .$(", errno=").$(nf.errno())
                                .$(']').$();
                        // peer disconnect
                        handlePeerDisconnect(DISCONNECT_REASON_PEER_DISCONNECT_AT_HEADER_RECV);
                        return false;
                    }

                    if (read == 0) {
                        // client is not sending anything
                        dispatcher.registerChannel(this, IOOperation.READ);
                        return false;
                    }

                    dumpBuffer(recvBuffer, read);
                    headerEnd = headerParser.parse(recvBuffer, recvBuffer + read, true);
                }
            }

            final CharSequence url = headerParser.getUrl();
            if (url == null) {
                throw HttpException.instance("missing URL");
            }
            HttpRequestProcessor processor = getHttpRequestProcessor(selector);

            final boolean multipartRequest = Chars.equalsNc("multipart/form-data", headerParser.getContentType());
            final boolean multipartProcessor = processor instanceof HttpMultipartContentListener;

            if (allowDeflateBeforeSend && Chars.contains(headerParser.getHeader("Accept-Encoding"), "gzip")) {
                responseSink.setDeflateBeforeSend(true);
            }

            try {
                if (multipartRequest && !multipartProcessor) {
                    // bad request - multipart request for processor that doesn't expect multipart
                    busyRecv = rejectRequest("Bad request. non-multipart GET expected.");
                } else if (!multipartRequest && multipartProcessor) {
                    // bad request - regular request for processor that expects multipart
                    busyRecv = rejectRequest("Bad request. Multipart POST expected.");
                } else if (multipartProcessor) {
                    busyRecv = consumeMultipart(fd, processor, headerEnd, read, newRequest, rescheduleContext);
                } else {

                    // Do not expect any more bytes to be sent to us before
                    // we respond back to client. We will disconnect the client when
                    // they abuse protocol. In addition, we will not call processor
                    // if client has disconnected before we had a chance to reply.
                    read = nf.recv(fd, recvBuffer, 1);
                    if (read != 0) {
                        dumpBuffer(recvBuffer, read);
                        LOG.info().$("disconnect after request [fd=").$(fd).$(']').$();
                        handlePeerDisconnect(DISCONNECT_REASON_KICKED_OUT_AT_EXTRA_BYTES);
                        busyRecv = false;
                    } else {
                        processor.onHeadersReady(this);
                        LOG.debug().$("good [fd=").$(fd).$(']').$();
                        processor.onRequestComplete(this);
                        resumeProcessor = null;
                        clear();
                    }
                }
            } catch (RetryOperationException e) {
                pendingRetry = true;
                scheduleRetry(processor, rescheduleContext);
                busyRecv = false;
            } catch (PeerDisconnectedException e) {
                handlePeerDisconnect(DISCONNECT_REASON_PEER_DISCONNECT_AT_RECV);
                busyRecv = false;
            } catch (ServerDisconnectException e) {
                LOG.info().$("kicked out [fd=").$(fd).$(']').$();
                dispatcher.disconnect(this, DISCONNECT_REASON_KICKED_OUT_AT_RECV);
                busyRecv = false;
            } catch (PeerIsSlowToReadException e) {
                LOG.debug().$("peer is slow reader [two]").$();
                // it is important to assign resume processor before we fire
                // event off to dispatcher
                processor.parkRequest(this);
                resumeProcessor = processor;
                dispatcher.registerChannel(this, IOOperation.WRITE);
                busyRecv = false;
            }
        } catch (HttpException e) {
            LOG.error().$("http error [fd=").$(fd).$(", e=`").$(e.getFlyweightMessage()).$("`]").$();
            dispatcher.disconnect(this, DISCONNECT_REASON_PROTOCOL_VIOLATION);
            busyRecv = false;
        }
        return busyRecv;
    }

    private boolean handleClientSend() {
        if (resumeProcessor != null) {
            try {
                responseSink.resumeSend();
                resumeProcessor.resumeSend(this);
                clear();
                return true;
            } catch (PeerIsSlowToReadException ignore) {
                resumeProcessor.parkRequest(this);
                LOG.debug().$("peer is slow reader").$();
                dispatcher.registerChannel(this, IOOperation.WRITE);
            } catch (PeerDisconnectedException ignore) {
                handlePeerDisconnect(DISCONNECT_REASON_PEER_DISCONNECT_AT_SEND);
            } catch (ServerDisconnectException ignore) {
                LOG.info().$("kicked out [fd=").$(fd).$(']').$();
                dispatcher.disconnect(this, DISCONNECT_REASON_KICKED_OUT_AT_SEND);
            }
        } else {
            LOG.error().$("spurious write request [fd=").$(fd).I$();
        }
        return false;
    }

    private void handlePeerDisconnect(int reason) {
        dispatcher.disconnect(this, reason);
        onPeerDisconnect.run();
    }

    private boolean parseMultipartResult(long start, long buf, int bufRemaining, HttpMultipartContentListener
            multipartListener, HttpRequestProcessor processor, RescheduleContext rescheduleContext) throws
            PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException, TooFewBytesReceivedException {
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

    private boolean rejectRequest(CharSequence userMessage) throws PeerDisconnectedException, PeerIsSlowToReadException {
        clear();
        LOG.error().$(userMessage).$();
        simpleResponse().sendStatus(404, userMessage);
        dispatcher.registerChannel(this, IOOperation.READ);
        return false;
    }

    private void shiftReceiveBufferUnprocessedBytes(long start, int receivedBytes) {
        // Shift to start
        this.receivedBytes = receivedBytes;
        Vect.memcpy(start, recvBuffer, receivedBytes);
        LOG.debug().$("peer is slow, waiting for bigger part to parse [multipart]").$();
    }
}
