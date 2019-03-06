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

import com.questdb.cutlass.http.io.*;
import com.questdb.mp.RingQueue;
import com.questdb.mp.SCSequence;
import com.questdb.mp.SPSequence;
import com.questdb.std.NetworkFacade;
import com.questdb.std.NetworkFacadeImpl;
import com.questdb.std.ObjectFactory;
import com.questdb.std.time.MillisecondClock;
import com.questdb.std.time.MillisecondClockImpl;
import org.junit.Ignore;
import org.junit.Test;

public class EPollIODispatcherTest {
    @Test
    @Ignore
    public void testSimple() {

        HttpServerConfiguration httpServerConfiguration = new HttpServerConfiguration() {
            @Override
            public int getConnectionHeaderBufferSize() {
                return 1024;
            }

            @Override
            public int getConnectionMultipartHeaderBufferSize() {
                return 512;
            }

            @Override
            public int getConnectionRecvBufferSize() {
                return 1024 * 1024;
            }

            @Override
            public int getConnectionSendBufferSize() {
                return 1024 * 1024;
            }

            @Override
            public int getConnectionWrapperObjPoolSize() {
                return 16;
            }
        };

        RingQueue<IOEvent<HttpConnectionContext>> ioEventQueue = new RingQueue<>(IOEvent::new, 1024);
        SPSequence ioEventPubSeq = new SPSequence(ioEventQueue.getCapacity());
        SCSequence ioEventSubSeq = new SCSequence();
        ioEventPubSeq.then(ioEventSubSeq).then(ioEventPubSeq);

        NetworkFacade nf = NetworkFacadeImpl.INSTANCE;

        EPollIODispatcher<HttpConnectionContext> dispatcher = new EPollIODispatcher<>(
                new IODispatcherConfiguration<HttpConnectionContext>() {
                    @Override
                    public int getActiveConnectionLimit() {
                        return 10;
                    }

                    @Override
                    public CharSequence getBindIPv4Address() {
                        return "0.0.0.0";
                    }

                    @Override
                    public int getBindPort() {
                        return 9001;
                    }

                    @Override
                    public MillisecondClock getClock() {
                        return MillisecondClockImpl.INSTANCE;
                    }

                    @Override
                    public int getEventCapacity() {
                        return 1024;
                    }

                    @Override
                    public IOContextFactory<HttpConnectionContext> getIOContextFactory() {
                        return fd -> new HttpConnectionContext(httpServerConfiguration, fd, getNetworkFacade().getPeerIP(fd));
                    }

                    @Override
                    public ObjectFactory<IOEvent<HttpConnectionContext>> getIOEventFactory() {
                        return IOEvent::new;
                    }

                    @Override
                    public long getIdleConnectionTimeout() {
                        return 10000000000000000L;
                    }

                    @Override
                    public int getListenBacklog() {
                        return 128;
                    }

                    @Override
                    public NetworkFacade getNetworkFacade() {
                        return NetworkFacadeImpl.INSTANCE;
                    }
                },
                ioEventQueue,
                ioEventPubSeq
        );

        HttpRequestProcessorSelector selector = new HttpRequestProcessorSelector() {
            @Override
            public HttpRequestProcessor select(CharSequence url) {
                return new HttpRequestProcessor() {
                    @Override
                    public void onHeadersReady(HttpConnectionContext connectionContext) {

                    }
                };
            }
        };

        while (true) {
            dispatcher.run();

            long cursor = ioEventSubSeq.next();
            if (cursor > -1) {
                IOEvent<HttpConnectionContext> event = ioEventQueue.get(cursor);
                HttpConnectionContext connectionContext = event.context;
                int op = event.operation;
                ioEventSubSeq.done(cursor);

                if (op != IOOperation.READ) {
                    dispatcher.registerChannel(connectionContext, IOOperation.DISCONNECT);
                } else {

                    long fd = connectionContext.getFd();
                    // this is address of where header ended in our receive buffer
                    // we need to being processing request content starting from this address
                    final long hi = connectionContext.recvBuffer + connectionContext.recvBufferSize;
                    long headerEnd = hi;
                    while (fd > -1 && connectionContext.headerParser.isIncomplete()) {
                        // read headers
                        int read = nf.recv(fd, connectionContext.recvBuffer, connectionContext.recvBufferSize);
                        if (read < 0) {
                            // peer disconnect
                            dispatcher.registerChannel(connectionContext, IOOperation.CLEANUP);
                            fd = -1;
                            break;
                        }

                        if (read == 0) {
                            // client is not sending anything
                            dispatcher.registerChannel(connectionContext, IOOperation.READ);
                            fd = -1;
                            break;
                        }

                        headerEnd = connectionContext.headerParser.parse(connectionContext.recvBuffer, connectionContext.recvBuffer + read, true);
                    }

                    if (fd > -1) {

                        assert !connectionContext.headerParser.isIncomplete();

                        HttpRequestProcessor processor = selector.select(connectionContext.headerParser.getUrl());
                        processor.onHeadersReady(connectionContext);

                        if (processor instanceof HttpMultipartContentListener) {

                            if (headerEnd < hi
                                    && connectionContext.multipartContentParser.parse(headerEnd, hi, (HttpMultipartContentListener) processor)) {
                                fd = -1;
                            }

                            while (fd > -1) {
                                // todo: receive remainder of request
                                int read = nf.recv(fd, connectionContext.recvBuffer, connectionContext.recvBufferSize);

                                if (read < 0) {
                                    dispatcher.registerChannel(connectionContext, IOOperation.CLEANUP);
                                    break;
                                }

                                if (read == 0) {
                                    // client is not sending anything
                                    dispatcher.registerChannel(connectionContext, IOOperation.READ);
                                    break;
                                }

                                if (connectionContext.multipartContentParser.parse(
                                        connectionContext.recvBuffer,
                                        connectionContext.recvBuffer + read,
                                        (HttpMultipartContentListener) processor)) {
                                    break;
                                }
                            }
                        }

                    }
                }
            }
        }
    }
}