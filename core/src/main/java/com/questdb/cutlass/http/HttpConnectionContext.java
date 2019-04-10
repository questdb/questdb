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

import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.network.*;
import com.questdb.std.*;
import com.questdb.std.str.DirectByteCharSequence;

public class HttpConnectionContext implements IOContext, Locality {
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
    private final long fd;
    private final LocalValueMap localValueMap = new LocalValueMap();
    private HttpRequestProcessor resumeProcessor = null;

    public HttpConnectionContext(HttpServerConfiguration configuration, long fd) {
        this.configuration = configuration;
        this.csPool = new ObjectPool<>(DirectByteCharSequence.FACTORY, configuration.getConnectionWrapperObjPoolSize());
        this.headerParser = new HttpHeaderParser(configuration.getConnectionHeaderBufferSize(), csPool);
        this.multipartContentHeaderParser = new HttpHeaderParser(configuration.getConnectionMultipartHeaderBufferSize(), csPool);
        this.multipartContentParser = new HttpMultipartContentParser(multipartContentHeaderParser);
        this.recvBufferSize = configuration.getConnectionRecvBufferSize();
        this.recvBuffer = Unsafe.malloc(recvBufferSize);
        this.sendBuffer = Unsafe.malloc(configuration.getConnectionSendBufferSize());
        this.fd = fd;
        this.responseSink = new HttpResponseSink(configuration, fd);
    }

    public void clear() {
        this.headerParser.clear();
        this.multipartContentParser.clear();
        this.multipartContentParser.clear();
        this.csPool.clear();
    }

    @Override
    public void close() {
        csPool.clear();
        multipartContentParser.close();
        multipartContentHeaderParser.close();
        responseSink.close();
        headerParser.close();
        Unsafe.free(recvBuffer, recvBufferSize);
        Unsafe.free(sendBuffer, configuration.getConnectionSendBufferSize());
    }

    @Override
    public long getFd() {
        return fd;
    }

    public HttpResponseSink.DirectBufferResponse getDirectBufferResponse() {
        return responseSink.getDirectBufferResponse();
    }

    public HttpResponseSink.HeaderOnlyResponse getHeaderOnlyResponse() {
        return responseSink.getHeaderOnlyResponse();
    }

    public HttpHeaders getHeaders() {
        return headerParser;
    }

    @Override
    public LocalValueMap getMap() {
        return localValueMap;
    }

    public void handleClientOperation(int operation, NetworkFacade nf, IODispatcher<HttpConnectionContext> dispatcher, HttpRequestProcessorSelector selector) {
        switch (operation) {
            case IOOperation.READ:
                handleClientRecv(nf, dispatcher, selector);
                break;
            case IOOperation.WRITE:
                if (resumeProcessor != null) {
                    try {
                        responseSink.resume();
                        resumeProcessor.resume(this);
                    } catch (PeerIsSlowException ignore) {
                        dispatcher.registerChannel(this, IOOperation.WRITE);
                    }
                } else {
                    assert false;
                }
                break;
            default:
                dispatcher.disconnect(this, DisconnectReason.SILLY);
                break;
        }
    }

    public HttpResponseSink.SimpleResponseImpl simpleResponse() {
        return responseSink.getSimple();
    }

    private void checkRemainingInputAndCompleteRequest(NetworkFacade nf, IODispatcher<HttpConnectionContext> dispatcher, long fd, HttpRequestProcessor processor) {
        int read;// consume and throw away the remainder of TCP input
        read = nf.recv(fd, recvBuffer, 1);
        if (read != 0) {
            LOG.debug().$("disconnect after request [fd=").$(fd).$(']').$();
            dispatcher.disconnect(this, DisconnectReason.PEER);
        } else {
            LOG.debug().$("good [fd=").$(fd).$(']').$();
            try {
                processor.onRequestComplete(this, dispatcher);
            } catch (PeerDisconnectedException ignore) {
                dispatcher.disconnect(this, DisconnectReason.PEER);
            }
        }
    }

    private void handleClientRecv(
            NetworkFacade nf,
            IODispatcher<HttpConnectionContext> dispatcher,
            HttpRequestProcessorSelector selector
    ) {
        try {
            long fd = this.fd;
            // this is address of where header ended in our receive buffer
            // we need to being processing request content starting from this address
            long headerEnd = recvBuffer;
            int read = 0;
            while (headerParser.isIncomplete()) {
                // read headers
                read = nf.recv(fd, recvBuffer, recvBufferSize);
                LOG.debug().$("recv [fd=").$(fd).$(", count=").$(read).$(']').$();
                if (read < 0) {
                    LOG.debug().$("done [fd=").$(fd).$(']').$();
                    // peer disconnect
                    dispatcher.disconnect(this, DisconnectReason.PEER);
                    return;
                }

                if (read == 0) {
                    // client is not sending anything
                    dispatcher.registerChannel(this, IOOperation.READ);
                    return;
                }

                headerEnd = headerParser.parse(recvBuffer, recvBuffer + read, true);
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
                dispatcher.registerChannel(this, IOOperation.READ);
            } else if (!multipartRequest && multipartProcessor) {
                // bad request - regular request for processor that expects multipart
                dispatcher.registerChannel(this, IOOperation.READ);
            } else if (multipartProcessor) {

                processor.onHeadersReady(this);

                HttpMultipartContentListener multipartListener = (HttpMultipartContentListener) processor;

                long bufferEnd = recvBuffer + read;

                if (headerEnd >= bufferEnd || !multipartContentParser.parse(headerEnd, bufferEnd, multipartListener)) {
                    do {
                        read = nf.recv(fd, recvBuffer, recvBufferSize);

                        if (read < 0) {
                            dispatcher.disconnect(this, DisconnectReason.PEER);
                            break;
                        }

                        if (read == 0) {
                            // client is not sending anything
                            dispatcher.registerChannel(this, IOOperation.READ);
                            break;
                        }
                    } while (!multipartContentParser.parse(recvBuffer, recvBuffer + read, multipartListener));
                }
                checkRemainingInputAndCompleteRequest(nf, dispatcher, fd, processor);
            } else {

                // Do not expect any more bytes to be sent to us before
                // we respond back to client. We will disconnect the client when
                // they abuse protocol. In addition, we will not call processor
                // if client has disconnected before we had a chance to reply.
                read = nf.recv(fd, recvBuffer, 1);
                if (read != 0) {
                    LOG.debug().$("disconnect after request [fd=").$(fd).$(']').$();
                    dispatcher.disconnect(this, DisconnectReason.PEER);
                } else {
                    processor.onHeadersReady(this);
                    LOG.debug().$("good [fd=").$(fd).$(']').$();
                    try {
                        processor.onRequestComplete(this, dispatcher);
                        resumeProcessor = null;
                    } catch (PeerDisconnectedException ignore) {
                        dispatcher.disconnect(this, DisconnectReason.PEER);
                    } catch (PeerIsSlowException ignore) {
                        dispatcher.registerChannel(this, IOOperation.WRITE);
                        resumeProcessor = processor;
                    }
                }
            }
        } catch (HttpException e) {
            e.printStackTrace();
        }
    }
}
