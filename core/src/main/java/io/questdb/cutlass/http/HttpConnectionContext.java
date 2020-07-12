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
import io.questdb.griffin.HttpSqlExecutionInterruptor;
import io.questdb.griffin.SqlExecutionInterruptor;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.*;
import io.questdb.std.*;
import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.std.str.StdoutSink;

public class HttpConnectionContext implements IOContext, Locality, Mutable {
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
    private final HttpSqlExecutionInterruptor execInterruptor;
    private long fd;
    private HttpRequestProcessor resumeProcessor = null;
    private IODispatcher<HttpConnectionContext> dispatcher;
    private int nCompletedRequests;
    private long totalBytesSent;
    private final boolean serverKeepAlive;

    public HttpConnectionContext(HttpServerConfiguration configuration) {
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
        this.serverKeepAlive = configuration.getServerKeepAlive();
    }

    @Override
    public void clear() {
        LOG.debug().$("clear").$();
        totalBytesSent += responseSink.getTotalBytesSent();
        nCompletedRequests++;
        this.resumeProcessor = null;
        this.headerParser.clear();
        this.multipartContentParser.clear();
        this.multipartContentHeaderParser.clear();
        this.csPool.clear();
        this.localValueMap.clear();
        this.responseSink.clear();
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
        LOG.debug().$("closed").$();
    }

    @Override
    public long getFd() {
        return fd;
    }

    @Override
    public boolean invalid() {
        return this.fd == -1;
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

    public void handleClientOperation(int operation, HttpRequestProcessorSelector selector) {
        boolean keepGoing;
        switch (operation) {
            case IOOperation.READ:
                keepGoing = handleClientRecv(selector);
                break;
            case IOOperation.WRITE:
                keepGoing = handleClientSend();
                break;
            default:
                dispatcher.disconnect(this);
                keepGoing = false;
                break;
        }

        if (keepGoing) {
            if (serverKeepAlive) {
                do {
                    keepGoing = handleClientRecv(selector);
                } while (keepGoing);
            } else {
                dispatcher.disconnect(this);
            }
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

    private void completeRequest(HttpRequestProcessor processor) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        LOG.debug().$("complete [fd=").$(fd).$(']').$();
        processor.onRequestComplete(this);
        clear();
    }

    private boolean consumeMultipart(
            long fd,
            HttpRequestProcessor processor,
            long headerEnd,
            int read,
            boolean newRequest
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        boolean keepGoing = false;
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

        long spinsRemaining = multipartIdleSpinCount;

        while (true) {
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
                    if (buf - start > 0 && multipartContentParser.parse(start, buf, multipartListener)) {
                        // request is complete
                        completeRequest(processor);
                        keepGoing = true;
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
                if (buf - start > 1 && multipartContentParser.parse(start, buf, multipartListener)) {
                    // request is complete
                    completeRequest(processor);
                    keepGoing = true;
                    break;
                }

                buf = start = recvBuffer;
                bufRemaining = recvBufferSize;
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

    private boolean handleClientRecv(HttpRequestProcessorSelector selector) {
        boolean keepGoing = true;
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
            HttpRequestProcessor processor = selector.select(headerParser.getUrl());

            if (processor == null) {
                processor = selector.getDefaultProcessor();
            }

            final boolean multipartRequest = Chars.equalsNc("multipart/form-data", headerParser.getContentType());
            final boolean multipartProcessor = processor instanceof HttpMultipartContentListener;

            if (allowDeflateBeforeSend && Chars.contains(headerParser.getHeader("Accept-Encoding"), "gzip")) {
                responseSink.setDeflateBeforeSend(true);
            }

            try {
                if (multipartRequest && !multipartProcessor) {
                    // bad request - multipart request for processor that doesn't expect multipart
                    keepGoing = rejectRequest("Bad request. non-multipart GET expected.");
                } else if (!multipartRequest && multipartProcessor) {
                    // bad request - regular request for processor that expects multipart
                    keepGoing = rejectRequest("Bad request. Multipart POST expected.");
                } else if (multipartProcessor) {
                    keepGoing = consumeMultipart(fd, processor, headerEnd, read, newRequest);
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
                        keepGoing = false;
                    } else {
                        processor.onHeadersReady(this);
                        LOG.debug().$("good [fd=").$(fd).$(']').$();
                        processor.onRequestComplete(this);
                        resumeProcessor = null;
                        clear();
                    }
                }
            } catch (PeerDisconnectedException e) {
                dispatcher.disconnect(this);
                keepGoing = false;
            } catch (ServerDisconnectException e) {
                LOG.info().$("kicked out [fd=").$(fd).$(']').$();
                dispatcher.disconnect(this);
                keepGoing = false;
            } catch (PeerIsSlowToReadException e) {
                LOG.debug().$("peer is slow reader [two]").$();
                // it is important to assign resume processor before we fire
                // event off to dispatcher
                processor.parkRequest(this);
                resumeProcessor = processor;
                dispatcher.registerChannel(this, IOOperation.WRITE);
                keepGoing = false;
            }
        } catch (HttpException e) {
            LOG.error().$("http error [fd=").$(fd).$(", e=`").$(e.getFlyweightMessage()).$("`]").$();
            dispatcher.disconnect(this);
            keepGoing = false;
        }
        return keepGoing;
    }

    private boolean rejectRequest(CharSequence userMessage) throws PeerDisconnectedException, PeerIsSlowToReadException {
        clear();
        LOG.error().$(userMessage).$();
        simpleResponse().sendStatus(400, userMessage);
        dispatcher.registerChannel(this, IOOperation.READ);
        return false;
    }

    private boolean handleClientSend() {
        assert resumeProcessor != null;
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
            dispatcher.disconnect(this);
        } catch (ServerDisconnectException ignore) {
            LOG.info().$("kicked out [fd=").$(fd).$(']').$();
            dispatcher.disconnect(this);
        }
        return false;
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
}
