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

import com.questdb.cutlass.http.io.IOContext;
import com.questdb.cutlass.http.io.IODispatcher;
import com.questdb.cutlass.http.io.IOOperation;
import com.questdb.std.Chars;
import com.questdb.std.NetworkFacade;
import com.questdb.std.ObjectPool;
import com.questdb.std.Unsafe;
import com.questdb.std.str.DirectByteCharSequence;

public class HttpConnectionContext implements IOContext {
    private final HttpHeaderParser headerParser;
    private final long recvBuffer;
    private final int recvBufferSize;
    private final HttpMultipartContentParser multipartContentParser;
    private final HttpHeaderParser multipartContentHeaderParser;
    private final ObjectPool<DirectByteCharSequence> csPool;
    private final long sendBuffer;
    private final HttpServerConfiguration configuration;
    private final long fd;

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
    }

    @Override
    public void close() {
        csPool.clear();
        multipartContentParser.close();
        multipartContentHeaderParser.close();
        headerParser.close();
        Unsafe.free(recvBuffer, recvBufferSize);
        Unsafe.free(sendBuffer, configuration.getConnectionSendBufferSize());
    }

    @Override
    public long getFd() {
        return fd;
    }

    public void handleClientOperation(int operation, NetworkFacade nf, IODispatcher<HttpConnectionContext> dispatcher, HttpRequestProcessorSelector selector) {
        switch (operation) {
            case IOOperation.CONNECT:
            case IOOperation.READ:
                handleClientRecv(nf, dispatcher, selector);
                break;
            default:
                dispatcher.registerChannel(this, IOOperation.DISCONNECT);
                break;
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
                if (read < 0) {
                    // peer disconnect
                    dispatcher.registerChannel(this, IOOperation.CLEANUP);
                    return;
                }

                if (read == 0) {
                    // client is not sending anything
                    dispatcher.registerChannel(this, IOOperation.READ);
                    return;
                }

                headerEnd = headerParser.parse(recvBuffer, recvBuffer + read, true);
            }

            final HttpRequestProcessor processor = selector.select(headerParser.getUrl());
            final boolean multipartRequest = Chars.equalsNc("multipart/form-data", headerParser.getContentType());
            final boolean multipartProcessor = processor instanceof HttpMultipartContentListener;

            if (multipartRequest && !multipartProcessor) {
                // bad request - multipart request for processor that doesn't expect multipart
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
                            dispatcher.registerChannel(this, IOOperation.CLEANUP);
                            break;
                        }

                        if (read == 0) {
                            // client is not sending anything
                            dispatcher.registerChannel(this, IOOperation.READ);
                            break;
                        }
                    } while (!multipartContentParser.parse(recvBuffer, recvBuffer + read, multipartListener));
                }
            } else {
                processor.onHeadersReady(this);
            }
        } catch (HttpException e) {
            e.printStackTrace();
        }
    }
}
