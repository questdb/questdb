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

import com.questdb.cutlass.http.io.DefaultIODispatcherConfiguration;
import com.questdb.cutlass.http.io.IODispatcher;
import com.questdb.cutlass.http.io.IODispatchers;
import com.questdb.cutlass.http.io.IOOperation;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.mp.SOCountDownLatch;
import com.questdb.std.Net;
import com.questdb.std.NetworkFacade;
import com.questdb.std.NetworkFacadeImpl;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

public class IODispatcherTest {

    private static Log LOG = LogFactory.getLog(IODispatcherTest.class);

    @Test
    @Ignore
    public void testSimple() throws Exception {

        LOG.info().$("started").$();

        TestUtils.assertMemoryLeak(() -> {
            HttpServerConfiguration httpServerConfiguration = new DefaultHttpServerConfiguration();

            NetworkFacade nf = NetworkFacadeImpl.INSTANCE;

            SOCountDownLatch contextClosedLatch = new SOCountDownLatch(1);
            try (IODispatcher<HttpConnectionContext> dispatcher = IODispatchers.create(
                    new DefaultIODispatcherConfiguration(),
                    fd -> new HttpConnectionContext(httpServerConfiguration, fd) {
                        @Override
                        public void close() {
                            super.close();
                            contextClosedLatch.countDown();
                        }
                    }
            )) {
                HttpRequestProcessorSelector selector = url -> connectionContext -> {
                };

                AtomicBoolean serverRunning = new AtomicBoolean(true);
                SOCountDownLatch serverHaltLatch = new SOCountDownLatch(1);
                SOCountDownLatch connectLatch = new SOCountDownLatch(1);

                new Thread(() -> {

                    while (serverRunning.get()) {
                        dispatcher.run();
                        dispatcher.processIOQueue(
                                (operation, context, disp) -> {
                                    if (operation == IOOperation.CONNECT) {
                                        connectLatch.countDown();
                                    }
                                    context.handleClientOperation(operation, nf, disp, selector);
                                }
                        );
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

                        Net.close(fd);

                        contextClosedLatch.await();

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