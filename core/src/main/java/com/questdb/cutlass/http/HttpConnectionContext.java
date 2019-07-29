/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cutlass.http;

import com.questdb.cairo.CairoSecurityContext;
import com.questdb.cairo.security.AllowAllCairoSecurityContext;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.network.*;
import com.questdb.std.*;
import com.questdb.std.str.DirectByteCharSequence;

public class HttpConnectionContext implements IOContext, Locality, Mutable {
    private static final Log LOG = LogFactory.getLog(HttpConnectionContext.class);
    private final HttpHeaderParser headerParser;
    private final long recvBuffer;
    private final int recvBufferSize;
    private final HttpMultipartContentParser multipartContentParser;
    private final HttpHeaderParser multipartContentHeaderParser;
    private final HttpResponseSink responseSink;
    private final ObjectPool<DirectByteCharSequence> csPool;
    private final long sendBuffer;
    private final HttpServerConfiguration configuration;
    private final LocalValueMap localValueMap = new LocalValueMap();
    private final NetworkFacade nf;
    private final long multipartIdleSpinCount;
    private final CairoSecurityContext cairoSecurityContext = AllowAllCairoSecurityContext.INSTANCE;
    private long fd;
    private HttpRequestProcessor resumeProcessor = null;

    public HttpConnectionContext(HttpServerConfiguration configuration) {
        this.configuration = configuration;
        this.nf = configuration.getDispatcherConfiguration().getNetworkFacade();
        this.csPool = new ObjectPool<>(DirectByteCharSequence.FACTORY, configuration.getConnectionStringPoolCapacity());
        this.headerParser = new HttpHeaderParser(configuration.getRequestHeaderBufferSize(), csPool);
        this.multipartContentHeaderParser = new HttpHeaderParser(configuration.getMultipartHeaderBufferSize(), csPool);
        this.multipartContentParser = new HttpMultipartContentParser(multipartContentHeaderParser);
        this.recvBufferSize = configuration.getRecvBufferSize();
        this.recvBuffer = Unsafe.malloc(recvBufferSize);
        this.sendBuffer = Unsafe.malloc(configuration.getSendBufferSize());
        this.responseSink = new HttpResponseSink(configuration);
        this.multipartIdleSpinCount = configuration.getMultipartIdleSpinCount();
        LOG.debug().$("new").$();
    }

    @Override
    public void clear() {
        LOG.debug().$("clear").$();
        this.headerParser.clear();
        this.multipartContentParser.clear();
        this.multipartContentParser.clear();
        this.csPool.clear();
        this.localValueMap.clear();
        this.responseSink.clear();
    }

    @Override
    public void close() {
        this.fd = -1;
        csPool.clear();
        multipartContentParser.close();
        multipartContentHeaderParser.close();
        responseSink.close();
        headerParser.close();
        localValueMap.close();
        Unsafe.free(recvBuffer, recvBufferSize);
        Unsafe.free(sendBuffer, configuration.getSendBufferSize());
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

    public void handleClientOperation(int operation, IODispatcher<HttpConnectionContext> dispatcher, HttpRequestProcessorSelector selector) {
        switch (operation) {
            case IOOperation.READ:
                try {
                    handleClientRecv(dispatcher, selector);
                } catch (PeerDisconnectedException ignore) {
                    LOG.debug().$("peer disconnected").$();
                    dispatcher.disconnect(this);
                } catch (PeerIsSlowToReadException ignore) {
                    LOG.debug().$("peer is slow writer").$();
                    dispatcher.registerChannel(this, IOOperation.READ);
                }
                break;
            case IOOperation.WRITE:
                if (resumeProcessor != null) {
                    try {
                        responseSink.resumeSend();
                        resumeProcessor.resumeSend(this, dispatcher);
                        resumeProcessor = null;
                    } catch (PeerIsSlowToReadException ignore) {
                        LOG.debug().$("peer is slow reader").$();
                        dispatcher.registerChannel(this, IOOperation.WRITE);
                    } catch (PeerDisconnectedException ignore) {
                        dispatcher.disconnect(this);
                    }
                } else {
                    assert false;
                }
                break;
            default:
                dispatcher.disconnect(this);
                break;
        }
    }

    public HttpConnectionContext of(long fd) {
        this.fd = fd;
        this.responseSink.of(fd);
        return this;
    }

    public HttpResponseSink.SimpleResponseImpl simpleResponse() {
        return responseSink.getSimple();
    }

    private void completeRequest(IODispatcher<HttpConnectionContext> dispatcher, long fd, HttpRequestProcessor processor) {
        LOG.debug().$("complete [fd=").$(fd).$(']').$();
        try {
            processor.onRequestComplete(this, dispatcher);
        } catch (PeerDisconnectedException ignore) {
            dispatcher.disconnect(this);
        } catch (PeerIsSlowToReadException e) {
            dispatcher.registerChannel(this, IOOperation.WRITE);
        }
    }

    private void handleClientRecv(
            IODispatcher<HttpConnectionContext> dispatcher,
            HttpRequestProcessorSelector selector
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        try {
            long fd = this.fd;
            // this is address of where header ended in our receive buffer
            // we need to being processing request content starting from this address
            long headerEnd = recvBuffer;
            int read = 0;
            final boolean readResume;
            if (headerParser.isIncomplete()) {
                readResume = false;
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

//                    dump(recvBuffer, read);

                    headerEnd = headerParser.parse(recvBuffer, recvBuffer + read, true);
                }
            } else {
                readResume = true;
            }

            HttpRequestProcessor processor = selector.select(headerParser.getUrl());

            if (processor == null) {
                processor = selector.getDefaultProcessor();
            }

            final boolean multipartRequest = Chars.equalsNc("multipart/form-data", headerParser.getContentType());
            final boolean multipartProcessor = processor instanceof HttpMultipartContentListener;

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

                if (!readResume) {
                    processor.onHeadersReady(this);
                    multipartContentParser.of(headerParser.getBoundary());
                }

                processor.resumeRecv(this, dispatcher);

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

//                    dump(buf, n);

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
                            if (multipartContentParser.parse(start, buf, multipartListener)) {
                                // request is complete
                                completeRequest(dispatcher, fd, processor);
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

                    bufRemaining -= n;
                    buf += n;

                    if (bufRemaining == 0) {
                        if (multipartContentParser.parse(start, buf, multipartListener)) {
                            // request is complete
                            completeRequest(dispatcher, fd, processor);
                            break;
                        }

                        buf = start = recvBuffer;
                        bufRemaining = recvBufferSize;
                    }
                }
            } else {

                // Do not expect any more bytes to be sent to us before
                // we respond back to client. We will disconnect the client when
                // they abuse protocol. In addition, we will not call processor
                // if client has disconnected before we had a chance to reply.
                read = nf.recv(fd, recvBuffer, 1);
                if (read != 0) {
                    LOG.debug().$("disconnect after request [fd=").$(fd).$(']').$();
                    dispatcher.disconnect(this);
                } else {
                    processor.onHeadersReady(this);
                    LOG.debug().$("good [fd=").$(fd).$(']').$();
                    try {
                        processor.onRequestComplete(this, dispatcher);
                        resumeProcessor = null;
                    } catch (PeerDisconnectedException ignore) {
                        dispatcher.disconnect(this);
                    } catch (PeerIsSlowToReadException ignore) {
                        LOG.debug().$("peer is slow reader [two]").$();
                        // it is important to assign resume processor before we fire
                        // event off to dispatcher
                        resumeProcessor = processor;
                        dispatcher.registerChannel(this, IOOperation.WRITE);
                    }
                }
            }
        } catch (HttpException e) {
            e.printStackTrace();
        }
    }
}
