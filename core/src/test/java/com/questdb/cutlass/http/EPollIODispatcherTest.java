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
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.mp.RingQueue;
import com.questdb.mp.SCSequence;
import com.questdb.mp.SOCountDownLatch;
import com.questdb.mp.SPSequence;
import com.questdb.std.Net;
import com.questdb.std.NetworkFacade;
import com.questdb.std.NetworkFacadeImpl;
import com.questdb.std.ObjectFactory;
import com.questdb.std.time.MillisecondClock;
import com.questdb.std.time.MillisecondClockImpl;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

public class EPollIODispatcherTest {

    private static Log LOG = LogFactory.getLog(EPollIODispatcherTest.class);

    @Test
    @Ignore
    public void testSimple() throws Exception {

        LOG.info().$("started").$();

        TestUtils.assertMemoryLeak(() -> {
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

            try (EPollIODispatcher<HttpConnectionContext> dispatcher = new EPollIODispatcher<>(
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
            )) {

                HttpRequestProcessorSelector selector = url -> connectionContext -> {
                };

                AtomicBoolean serverRunning = new AtomicBoolean(true);
                SOCountDownLatch serverHaltLatch = new SOCountDownLatch(1);
                SOCountDownLatch connectLatch = new SOCountDownLatch(1);

                new Thread(() -> {

                    while (serverRunning.get()) {
                        dispatcher.run();
                        long cursor = ioEventSubSeq.next();
                        if (cursor > -1) {
                            IOEvent<HttpConnectionContext> event = ioEventQueue.get(cursor);
                            HttpConnectionContext connectionContext = event.context;
                            final int operation = event.operation;
                            ioEventSubSeq.done(cursor);

                            if (operation == IOOperation.CONNECT) {
                                connectLatch.countDown();
                            }

                            connectionContext.handleClientOperation(operation, nf, dispatcher, selector);
                        }
                    }

                    serverHaltLatch.countDown();
                }).start();


                long fd = Net.socketTcp(true);
                try {
                    long sockAddr = Net.sockaddr("127.0.0.1", 9001);
                    try {
                        Assert.assertTrue(fd > -1);
                        Assert.assertEquals(0, Net.connect(fd, sockAddr));

                        connectLatch.await();

                        serverRunning.set(false);
                        serverHaltLatch.await();

                        System.out.println(dispatcher.getConnectionCount());
                    } finally {
                        Net.freeSockAddr(sockAddr);
                    }
                } finally {
                    Net.close(fd);
                }
            }
        });
    }
}