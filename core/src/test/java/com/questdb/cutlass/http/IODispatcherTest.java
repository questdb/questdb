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
import com.questdb.mp.SOCountDownLatch;
import com.questdb.network.*;
import com.questdb.std.ObjList;
import com.questdb.std.Unsafe;
import com.questdb.std.str.StringSink;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class IODispatcherTest {
    private static Log LOG = LogFactory.getLog(IODispatcherTest.class);

    @Test
    public void testBiasWrite() throws Exception {

        LOG.info().$("started testBiasWrite").$();


        TestUtils.assertMemoryLeak(() -> {

            SOCountDownLatch connectLatch = new SOCountDownLatch(1);
            SOCountDownLatch contextClosedLatch = new SOCountDownLatch(1);

            try (IODispatcher<HelloContext> dispatcher = IODispatchers.create(
                    new DefaultIODispatcherConfiguration() {
                        @Override
                        public int getInitialBias() {
                            return IODispatcherConfiguration.BIAS_WRITE;
                        }
                    },
                    fd -> {
                        connectLatch.countDown();
                        return new HelloContext(fd, contextClosedLatch);
                    }
            )) {
                AtomicBoolean serverRunning = new AtomicBoolean(true);
                SOCountDownLatch serverHaltLatch = new SOCountDownLatch(1);

                new Thread(() -> {
                    while (serverRunning.get()) {
                        dispatcher.run();
                        dispatcher.processIOQueue(
                                (operation, context, disp) -> {
                                    if (operation == IOOperation.WRITE) {
                                        Assert.assertEquals(1024, Net.send(context.getFd(), context.buffer, 1024));
                                        disp.registerChannel(context, IOOperation.DISCONNECT);
                                    }
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

                        long buffer = Unsafe.malloc(2048);
                        try {
                            Assert.assertEquals(1024, Net.recv(fd, buffer, 1024));
                        } finally {
                            Unsafe.free(buffer, 2048);
                        }


                        Assert.assertEquals(0, Net.close(fd));
                        LOG.info().$("closed [fd=").$(fd).$(']').$();

                        contextClosedLatch.await();

                        serverRunning.set(false);
                        serverHaltLatch.await();

                        Assert.assertEquals(0, dispatcher.getConnectionCount());
                    } finally {
                        Net.freeSockAddr(sockAddr);
                    }
                } finally {
                    Net.close(fd);
                }
            }
        });
    }

    @Test
    public void testConnectDisconnect() throws Exception {

        LOG.info().$("started testConnectDisconnect").$();

        TestUtils.assertMemoryLeak(() -> {
            HttpServerConfiguration httpServerConfiguration = new DefaultHttpServerConfiguration();

            NetworkFacade nf = NetworkFacadeImpl.INSTANCE;
            SOCountDownLatch connectLatch = new SOCountDownLatch(1);
            SOCountDownLatch contextClosedLatch = new SOCountDownLatch(1);
            AtomicInteger closeCount = new AtomicInteger(0);

            try (IODispatcher<HttpConnectionContext> dispatcher = IODispatchers.create(
                    new DefaultIODispatcherConfiguration(),
                    new IOContextFactory<HttpConnectionContext>() {
                        @Override
                        public HttpConnectionContext newInstance(long fd) {
                            connectLatch.countDown();
                            return new HttpConnectionContext(httpServerConfiguration, fd) {
                                @Override
                                public void close() {
                                    // it is possible that context is closed twice in error
                                    // when crashes occur put debug line here to see how many times
                                    // context is closed
                                    if (closeCount.incrementAndGet() == 1) {
                                        super.close();
                                        contextClosedLatch.countDown();
                                    }
                                }
                            };
                        }
                    }
            )) {
                HttpRequestProcessorSelector selector = url -> (connectionContext, dispatcher1) -> {
                };

                AtomicBoolean serverRunning = new AtomicBoolean(true);
                SOCountDownLatch serverHaltLatch = new SOCountDownLatch(1);

                new Thread(() -> {

                    while (serverRunning.get()) {
                        dispatcher.run();
                        dispatcher.processIOQueue(
                                (operation, context, disp) -> context.handleClientOperation(operation, nf, disp, selector)
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

                        Assert.assertEquals(0, Net.close(fd));
                        LOG.info().$("closed [fd=").$(fd).$(']').$();

                        contextClosedLatch.await();

                        serverRunning.set(false);
                        serverHaltLatch.await();

                        Assert.assertEquals(0, dispatcher.getConnectionCount());
                    } finally {
                        Net.freeSockAddr(sockAddr);
                    }
                } finally {
                    Net.close(fd);
                }

                Assert.assertEquals(1, closeCount.get());
            }
        });
    }

    @Test
    public void testMaxConnections() throws Exception {

        LOG.info().$("started maxConnections").$();

        TestUtils.assertMemoryLeak(() -> {
            HttpServerConfiguration httpServerConfiguration = new DefaultHttpServerConfiguration();

            int N = 200;

            NetworkFacade nf = NetworkFacadeImpl.INSTANCE;
            AtomicInteger openCount = new AtomicInteger(0);
            AtomicInteger closeCount = new AtomicInteger(0);

            final IODispatcherConfiguration configuration = new DefaultIODispatcherConfiguration() {
                @Override
                public int getActiveConnectionLimit() {
                    return 15;
                }
            };

            try (IODispatcher<HttpConnectionContext> dispatcher = IODispatchers.create(
                    configuration,
                    new IOContextFactory<HttpConnectionContext>() {
                        @Override
                        public HttpConnectionContext newInstance(long fd) {
                            openCount.incrementAndGet();
                            return new HttpConnectionContext(httpServerConfiguration, fd) {
                                @Override
                                public void close() {
                                    closeCount.incrementAndGet();
                                    super.close();
                                }
                            };
                        }
                    }
            )) {
                HttpRequestProcessorSelector selector = url -> (connectionContext, dispatcher1) -> {
                };

                AtomicBoolean serverRunning = new AtomicBoolean(true);
                SOCountDownLatch serverHaltLatch = new SOCountDownLatch(1);

                new Thread(() -> {
                    do {
                        dispatcher.run();
                        dispatcher.processIOQueue(
                                (operation, context, disp) -> context.handleClientOperation(operation, nf, disp, selector)
                        );
                    } while (serverRunning.get());
                    serverHaltLatch.countDown();
                }).start();


                for (int i = 0; i < N; i++) {
                    long fd = Net.socketTcp(true);
                    long sockAddr = Net.sockaddr("127.0.0.1", 9001);
                    try {
                        Assert.assertTrue(fd > -1);
                        Assert.assertEquals(0, Net.connect(fd, sockAddr));
                        Assert.assertEquals(0, Net.close(fd));
                        LOG.info().$("closed [fd=").$(fd).$(']').$();
                    } finally {
                        Net.freeSockAddr(sockAddr);
                    }
                }

//                Thread.sleep(3000);
                Assert.assertFalse(configuration.getActiveConnectionLimit() < dispatcher.getConnectionCount());
                serverRunning.set(false);
                serverHaltLatch.await();
            }
            System.out.println("open: " + openCount.get());
            System.out.println("close: " + closeCount.get());
        });
    }

    @Test
    public void testSendHttpGet() throws Exception {

        LOG.info().$("started testSendHttpGet").$();

        final String request = "GET /status?x=1&a=%26b&c&d=x HTTP/1.1\r\n" +
                "Host: localhost:9000\r\n" +
                "Connection: keep-alive\r\n" +
                "Cache-Control: max-age=0\r\n" +
                "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8\r\n" +
                "User-Agent: Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.48 Safari/537.36\r\n" +
                "Accept-Encoding: gzip,deflate,sdch\r\n" +
                "Accept-Language: en-US,en;q=0.8\r\n" +
                "Cookie: textwrapon=false; textautoformat=false; wysiwyg=textarea\r\n" +
                "\r\n";

        // the difference between request and expected is url encoding (and ':' padding, which can easily be fixed)
        final String expected = "GET /status?x=1&a=&b&c&d=x HTTP/1.1\r\n" +
                "Host:localhost:9000\r\n" +
                "Connection:keep-alive\r\n" +
                "Cache-Control:max-age=0\r\n" +
                "Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8\r\n" +
                "User-Agent:Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.48 Safari/537.36\r\n" +
                "Accept-Encoding:gzip,deflate,sdch\r\n" +
                "Accept-Language:en-US,en;q=0.8\r\n" +
                "Cookie:textwrapon=false; textautoformat=false; wysiwyg=textarea\r\n" +
                "\r\n";

        TestUtils.assertMemoryLeak(() -> {
            HttpServerConfiguration httpServerConfiguration = new DefaultHttpServerConfiguration();

            NetworkFacade nf = NetworkFacadeImpl.INSTANCE;
            SOCountDownLatch connectLatch = new SOCountDownLatch(1);
            SOCountDownLatch contextClosedLatch = new SOCountDownLatch(1);
            AtomicInteger closeCount = new AtomicInteger(0);

            try (IODispatcher<HttpConnectionContext> dispatcher = IODispatchers.create(
                    new DefaultIODispatcherConfiguration(),
                    new IOContextFactory<HttpConnectionContext>() {
                        @Override
                        public HttpConnectionContext newInstance(long fd) {
                            connectLatch.countDown();
                            return new HttpConnectionContext(httpServerConfiguration, fd) {
                                @Override
                                public void close() {
                                    // it is possible that context is closed twice in error
                                    // when crashes occur put debug line here to see how many times
                                    // context is closed
                                    if (closeCount.incrementAndGet() == 1) {
                                        super.close();
                                        contextClosedLatch.countDown();
                                    }
                                }
                            };
                        }
                    }
            )) {
                StringSink sink = new StringSink();

                HttpRequestProcessorSelector selector = url -> (context, dispatcher1) -> {
                    HttpHeaders headers = context.getHeaders();
                    sink.put(headers.getMethodLine());
                    sink.put("\r\n");
                    ObjList<CharSequence> headerNames = headers.getHeaderNames();
                    for (int i = 0, n = headerNames.size(); i < n; i++) {
                        sink.put(headerNames.getQuick(i)).put(':');
                        sink.put(headers.getHeader(headerNames.getQuick(i)));
                        sink.put("\r\n");
                    }
                    sink.put("\r\n");


                    dispatcher1.registerChannel(context, IOOperation.READ);
                };

                AtomicBoolean serverRunning = new AtomicBoolean(true);
                SOCountDownLatch serverHaltLatch = new SOCountDownLatch(1);

                new Thread(() -> {
                    while (serverRunning.get()) {
                        dispatcher.run();
                        dispatcher.processIOQueue(
                                (operation, context, disp) -> context.handleClientOperation(operation, nf, disp, selector)
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

                        int len = request.length();
                        long buffer = TestUtils.toMemory(request);
                        try {
                            Assert.assertEquals(len, Net.send(fd, buffer, len));
                        } finally {
                            Unsafe.free(buffer, len);
                        }

                        Assert.assertEquals(0, Net.close(fd));
                        LOG.info().$("closed [fd=").$(fd).$(']').$();

                        contextClosedLatch.await();

                        serverRunning.set(false);
                        serverHaltLatch.await();

                        Assert.assertEquals(0, dispatcher.getConnectionCount());

                        TestUtils.assertEquals(expected, sink);
                    } finally {
                        Net.freeSockAddr(sockAddr);
                    }
                } finally {
                    Net.close(fd);
                }

                Assert.assertEquals(1, closeCount.get());
            }
        });
    }

    @Test
    public void testSendTimeout() throws Exception {

        LOG.info().$("started testSendHttpGet").$();

        final String request = "GET /status?x=1&a=%26b&c&d=x HTTP/1.1\r\n" +
                "Host: localhost:9000\r\n" +
                "Connection: keep-alive\r\n" +
                "Cache-Control: max-age=0\r\n" +
                "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8\r\n" +
                "User-Agent: Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.48 Safari/537.36\r\n" +
                "Accept-Encoding: gzip,deflate,sdch\r\n" +
                "Accept-Language: en-US,en;q=0.8\r\n" +
                "Cookie: textwrapon=false; textautoformat=false; wysiwyg=textarea\r\n" +
                "\r\n";

        TestUtils.assertMemoryLeak(() -> {
            HttpServerConfiguration httpServerConfiguration = new DefaultHttpServerConfiguration();

            NetworkFacade nf = NetworkFacadeImpl.INSTANCE;
            SOCountDownLatch connectLatch = new SOCountDownLatch(1);
            SOCountDownLatch contextClosedLatch = new SOCountDownLatch(1);
            AtomicInteger closeCount = new AtomicInteger(0);

            try (IODispatcher<HttpConnectionContext> dispatcher = IODispatchers.create(
                    new DefaultIODispatcherConfiguration() {
                        @Override
                        public long getIdleConnectionTimeout() {
                            // 0.5s idle timeout
                            return 500;
                        }
                    },
                    new IOContextFactory<HttpConnectionContext>() {
                        @Override
                        public HttpConnectionContext newInstance(long fd) {
                            connectLatch.countDown();
                            return new HttpConnectionContext(httpServerConfiguration, fd) {
                                @Override
                                public void close() {
                                    // it is possible that context is closed twice in error
                                    // when crashes occur put debug line here to see how many times
                                    // context is closed
                                    if (closeCount.incrementAndGet() == 1) {
                                        super.close();
                                        contextClosedLatch.countDown();
                                    }
                                }
                            };
                        }
                    }
            )) {
                StringSink sink = new StringSink();

                HttpRequestProcessorSelector selector = url -> (context, dispatcher1) -> {
                    HttpHeaders headers = context.getHeaders();
                    sink.put(headers.getMethodLine());
                    sink.put("\r\n");
                    ObjList<CharSequence> headerNames = headers.getHeaderNames();
                    for (int i = 0, n = headerNames.size(); i < n; i++) {
                        sink.put(headerNames.getQuick(i)).put(':');
                        sink.put(headers.getHeader(headerNames.getQuick(i)));
                        sink.put("\r\n");
                    }
                    sink.put("\r\n");


                    dispatcher1.registerChannel(context, IOOperation.READ);
                };

                AtomicBoolean serverRunning = new AtomicBoolean(true);
                SOCountDownLatch serverHaltLatch = new SOCountDownLatch(1);

                new Thread(() -> {
                    while (serverRunning.get()) {
                        dispatcher.run();
                        dispatcher.processIOQueue(
                                (operation, context, disp) -> context.handleClientOperation(operation, nf, disp, selector)
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
                        Net.setTcpNoDelay(fd, true);

                        connectLatch.await();

                        int len = request.length();
                        long buffer = TestUtils.toMemory(request);
                        try {
                            int part1 = len / 2;
                            Assert.assertEquals(part1, Net.send(fd, buffer, part1));
                            Thread.sleep(1000);
                            Assert.assertEquals(len - part1, Net.send(fd, buffer + part1, len - part1));
                        } finally {
                            Unsafe.free(buffer, len);
                        }

                        contextClosedLatch.await();

                        Assert.assertEquals(0, dispatcher.getConnectionCount());

                        serverRunning.set(false);
                        serverHaltLatch.await();

                        Assert.assertEquals(0, dispatcher.getConnectionCount());

                        // do not close client side before server does theirs
                        Assert.assertTrue(Net.isDead(fd));
                        Assert.assertEquals(0, Net.close(fd));
                        LOG.info().$("closed [fd=").$(fd).$(']').$();

                        TestUtils.assertEquals("", sink);
                    } finally {
                        Net.freeSockAddr(sockAddr);
                    }
                } finally {
                    Net.close(fd);
                }

                Assert.assertEquals(1, closeCount.get());
            }
        });
    }

    private static class HelloContext implements IOContext {
        private final long fd;
        private final long buffer = Unsafe.malloc(1024);
        private final SOCountDownLatch closeLatch;

        public HelloContext(long fd, SOCountDownLatch closeLatch) {
            this.fd = fd;
            this.closeLatch = closeLatch;
        }

        @Override
        public void close() {
            Unsafe.free(buffer, 1024);
            closeLatch.countDown();
        }

        @Override
        public long getFd() {
            return fd;
        }
    }
}