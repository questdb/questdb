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
import io.questdb.cairo.pool.ex.RetryOperationException;
import io.questdb.cairo.security.CairoSecurityContextImpl;
import io.questdb.griffin.HttpSqlExecutionInterruptor;
import io.questdb.griffin.SqlExecutionInterruptor;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.*;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjectPool;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.std.str.StdoutSink;

public class HttpConnectionContext implements IOContext, Locality, Mutable, Retry {
    private static final Log LOG = LogFactory.getLog(HttpConnectionContext.class);
    private final HttpHeaderParser headerParser;
    private final long recvBuffer;
    private final int recvBufferSize;
    private final HttpMultipartContentParser multipartContentParser;
    private final HttpHeaderParser multipartContentHeaderParser;
    private final HttpResponseSink responseSink;
    private final ObjectPool<DirectByteCharSequence> csPool;
    private final HttpServerConfiguration configuration;
    private final LocalValueMap localValueMap = new LocalValueMap();
    private final NetworkFacade nf;
    private final long multipartIdleSpinCount;
    private final CairoSecurityContext cairoSecurityContext;
    private final boolean dumpNetworkTraffic;
    private final boolean allowDeflateBeforeSend;
    private final HttpSqlExecutionInterruptor execInterruptor;
    private final MultipartParserState multipartParserState = new MultipartParserState();
    private long fd;
    private HttpRequestProcessor resumeProcessor = null;
    private boolean pendingRetry = false;
    private IODispatcher<HttpConnectionContext> dispatcher;
    private int nCompletedRequests;
    private long totalBytesSent;

    public HttpConnectionContext(HttpServerConfiguration configuration) {
        this.configuration = configuration;
        this.nf = configuration.getDispatcherConfiguration().getNetworkFacade();
        this.csPool = new ObjectPool<>(DirectByteCharSequence.FACTORY, configuration.getConnectionStringPoolCapacity());
        this.headerParser = new HttpHeaderParser(configuration.getRequestHeaderBufferSize(), csPool);
        this.multipartContentHeaderParser = new HttpHeaderParser(configuration.getMultipartHeaderBufferSize(), csPool);
        this.multipartContentParser = new HttpMultipartContentParser(multipartContentHeaderParser);
        this.recvBufferSize = configuration.getRecvBufferSize();
        this.recvBuffer = Unsafe.malloc(recvBufferSize);
        this.responseSink = new HttpResponseSink(configuration);
        this.multipartIdleSpinCount = configuration.getMultipartIdleSpinCount();
        this.dumpNetworkTraffic = configuration.getDumpNetworkTraffic();
        this.allowDeflateBeforeSend = configuration.allowDeflateBeforeSend();
        cairoSecurityContext = new CairoSecurityContextImpl(!configuration.readOnlySecurityContext());
        execInterruptor = configuration.isInterruptOnClosedConnection()
                ? new HttpSqlExecutionInterruptor(this.nf, configuration.getInterruptorNIterationsPerCheck(), configuration.getInterruptorBufferSize())
                : null;
    }

    @Override
    public void clear() {
        LOG.debug().$("clear").$();
        totalBytesSent += responseSink.getTotalBytesSent();
        nCompletedRequests++;
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
    }

    @Override
    public void close() {
        this.fd = -1;
        Misc.free(execInterruptor);
        nCompletedRequests = 0;
        totalBytesSent = 0;
        csPool.clear();
        multipartContentParser.close();
        multipartContentHeaderParser.close();
        responseSink.close();
        headerParser.close();
        localValueMap.close();
        Unsafe.free(recvBuffer, recvBufferSize);
        if (this.pendingRetry) {
            LOG.error().$("Closed context with retry pending.").$();
        }
        this.pendingRetry = false;
        LOG.debug().$("closed").$();
    }

    @Override
    public long getFd() {
        return fd;
    }

    @Override
    public boolean invalid() {
        return pendingRetry || this.fd == -1;
    }

    @Override
    public IODispatcher<HttpConnectionContext> getDispatcher() {
        return dispatcher;
    }

    public CairoSecurityContext getCairoSecurityContext() {
        return cairoSecurityContext;
    }

    public HttpChunkedResponseSocket getChunkedResponseSocket() {
        return responseSink.getChunkedSocket();
    }

    @Override
    public LocalValueMap getMap() {
        return localValueMap;
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

    public void handleClientOperation(int operation, HttpRequestProcessorSelector selector, RescheduleContext rescheduleContext) {
        switch (operation) {
            case IOOperation.READ:
                handleClientRecv(selector, rescheduleContext);
                break;
            case IOOperation.WRITE:
                handleClientSend();
                break;
            default:
                dispatcher.disconnect(this);
                break;
        }
    }

    public HttpConnectionContext of(long fd, IODispatcher<HttpConnectionContext> dispatcher) {
        this.fd = fd;
        this.dispatcher = dispatcher;
        this.responseSink.of(fd);
        if (null != execInterruptor) {
            this.execInterruptor.of(fd);
        }
        return this;
    }

    public HttpResponseSink.SimpleResponseImpl simpleResponse() {
        return responseSink.getSimple();
    }

    private void completeRequest(HttpRequestProcessor processor, RescheduleContext rescheduleContext) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        LOG.debug().$("complete [fd=").$(fd).$(']').$();
        try {
            processor.onRequestComplete(this);
        } catch (RetryOperationException e) {
            pendingRetry = true;
            rescheduleContext.reschedule(this);
        }
    }

    private void consumeMultipart(
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
            buf = start = recvBuffer;
            bufRemaining = recvBufferSize;
        }

        continueConsumeMultipart(fd, start, buf, bufRemaining, multipartListener, processor, rescheduleContext);
    }

    private void continueConsumeMultipart(long fd, long start, long buf, int bufRemaining, HttpMultipartContentListener multipartListener, HttpRequestProcessor processor, RescheduleContext rescheduleContext) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        // do we have anything in the buffer?
        if (buf > start) {
            if (parseMultipartResult(start, buf, bufRemaining, multipartListener, processor, rescheduleContext)) {
                return;
            }

            buf = start = recvBuffer;
            bufRemaining = recvBufferSize;
        }

        long spinsRemaining = multipartIdleSpinCount;
        while (true) {
            // if this is not a retry.
            final int n = nf.recv(fd, buf, bufRemaining);
            if (n < 0) {
                dispatcher.disconnect(this);
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
                    if (parseMultipartResult(start, buf, bufRemaining, multipartListener, processor, rescheduleContext)) {
                        break;
                    }

                    buf = start = recvBuffer;
                    bufRemaining = recvBufferSize;
                    continue;

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
                if (buf - start > 1) {
                    if (parseMultipartResult(start, buf, bufRemaining, multipartListener, processor, rescheduleContext)) {
                        break;
                    }
                }

                buf = start = recvBuffer;
                bufRemaining = recvBufferSize;
            }
        }
    }

    private boolean parseMultipartResult(long start, long buf, int bufRemaining, HttpMultipartContentListener multipartListener, HttpRequestProcessor processor, RescheduleContext rescheduleContext) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        boolean parseResult;
        try {
            parseResult = multipartContentParser.parse(start, buf, multipartListener);
        } catch (RetryOperationException e) {
            this.multipartParserState.saveFdBufferPosition(multipartContentParser.getNextTokenPtr(), buf, bufRemaining,  multipartContentParser.getPreviousState());
            throw e;
        }

        if (parseResult) {
            // request is complete
            completeRequest(processor, rescheduleContext);
            return true;
        }
        return false;
    }

    private void dumpBuffer(long buffer, int size) {
        if (dumpNetworkTraffic && size > 0) {
            StdoutSink.INSTANCE.put('>');
            Net.dump(buffer, size);
        }
    }

    private void handleClientRecv(HttpRequestProcessorSelector selector, RescheduleContext rescheduleContext) {
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
                        LOG.debug().$("done [fd=").$(fd).$(']').$();
                        // peer disconnect
                        dispatcher.disconnect(this);
                        return;
                    }

                    if (read == 0) {
                        // client is not sending anything
                        dispatcher.registerChannel(this, IOOperation.READ);
                        return;
                    }

                    dumpBuffer(recvBuffer, read);
                    headerEnd = headerParser.parse(recvBuffer, recvBuffer + read, true);
                }
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
                    headerParser.clear();
                    LOG.error().$("bad request [multipart/non-multipart]").$();
                    dispatcher.registerChannel(this, IOOperation.READ);
                } else if (!multipartRequest && multipartProcessor) {
                    // bad request - regular request for processor that expects multipart
                    LOG.error().$("bad request [non-multipart/multipart]").$();
                    dispatcher.registerChannel(this, IOOperation.READ);
                } else if (multipartProcessor) {
                    consumeMultipart(fd, processor, headerEnd, read, newRequest, rescheduleContext);
                } else {

                    // Do not expect any more bytes to be sent to us before
                    // we respond back to client. We will disconnect the client when
                    // they abuse protocol. In addition, we will not call processor
                    // if client has disconnected before we had a chance to reply.
                    read = nf.recv(fd, recvBuffer, 1);
                    if (read != 0) {
                        dumpBuffer(recvBuffer, read);
                        LOG.info().$("disconnect after request [fd=").$(fd).$(']').$();
                        dispatcher.disconnect(this);
                    } else {
                        processor.onHeadersReady(this);
                        LOG.debug().$("good [fd=").$(fd).$(']').$();
                        processor.onRequestComplete(this);
                        resumeProcessor = null;
                    }
                }
            } catch (RetryOperationException e) {
                pendingRetry = true;
                rescheduleContext.reschedule(this);
            } catch (PeerDisconnectedException e) {
                dispatcher.disconnect(this);
            } catch (ServerDisconnectException e) {
                LOG.info().$("kicked out [fd=").$(fd).$(']').$();
                dispatcher.disconnect(this);
            } catch (PeerIsSlowToReadException e) {
                LOG.debug().$("peer is slow reader [two]").$();
                // it is important to assign resume processor before we fire
                // event off to dispatcher
                processor.parkRequest(this);
                resumeProcessor = processor;
                dispatcher.registerChannel(this, IOOperation.WRITE);
            }
        } catch (HttpException e) {
            LOG.error().$("http error [e=").$(e.getFlyweightMessage()).$(']').$();
            dispatcher.disconnect(this);
        }
    }

    private HttpRequestProcessor getHttpRequestProcessor(HttpRequestProcessorSelector selector) {
        HttpRequestProcessor processor = selector.select(headerParser.getUrl());

        if (processor == null) {
            processor = selector.getDefaultProcessor();
        }
        return processor;
    }

    private void handleClientSend() {
        assert resumeProcessor != null;
        try {
            responseSink.resumeSend();
            resumeProcessor.resumeSend(this);
            resumeProcessor = null;
        } catch (PeerIsSlowToReadException ignore) {
            resumeProcessor.parkRequest(this);
            LOG.debug().$("peer is slow reader").$();
            dispatcher.registerChannel(this, IOOperation.WRITE);
        } catch (PeerDisconnectedException ignore) {
            dispatcher.disconnect(this);
        } catch (ServerDisconnectException ignore) {
            LOG.info().$("kicked out [fd=").$(fd).$(']').$();
            dispatcher.disconnect(this);
        }
    }

    public int getNCompletedRequests() {
        return nCompletedRequests;
    }

    public long getTotalBytesSent() {
        return totalBytesSent;
    }

    public long getLastRequestBytesSent() {
        return responseSink.getTotalBytesSent();
    }

    public SqlExecutionInterruptor getSqlExecutionInterruptor() {
        return execInterruptor;
    }

    public boolean tryRerun(HttpRequestProcessorSelector selector) {
        if (pendingRetry) {
            pendingRetry = false;
            HttpRequestProcessor processor = getHttpRequestProcessor(selector);
            try {
                if (multipartParserState.multipartRetry) {
                    processor.onRequestRetry(this);
                    multipartContentParser.setState(multipartParserState.state);
                    continueConsumeMultipart(fd, multipartParserState.start, multipartParserState.buf, multipartParserState.bufRemaining, (HttpMultipartContentListener) processor, processor, retry -> {
                        LOG.info().$("Retry is requested after successful writer allocation. Retry will be re-scheduled [thread=").$(Thread.currentThread().getId()).$(']');
                        throw RetryOperationException.INSTANCE;
                    });
                } else {
                    processor.onRequestRetry(this);
                }
            } catch (RetryOperationException e2) {
                pendingRetry = true;
                return false;
            } catch (PeerDisconnectedException ignore) {
                dispatcher.disconnect(this);
            } catch (PeerIsSlowToReadException e2) {
                dispatcher.registerChannel(this, IOOperation.WRITE);
            } catch (ServerDisconnectException e) {
                LOG.info().$("kicked out [fd=").$(fd).$(']').$();
                dispatcher.disconnect(this);
            }
        }
        return true;
    }
}
