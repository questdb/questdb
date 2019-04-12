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

import com.questdb.cutlass.http.processors.StaticContentProcessor;
import com.questdb.cutlass.http.processors.StaticContentProcessorConfiguration;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.mp.MPSequence;
import com.questdb.mp.RingQueue;
import com.questdb.mp.SCSequence;
import com.questdb.mp.SOCountDownLatch;
import com.questdb.network.*;
import com.questdb.std.*;
import com.questdb.std.str.Path;
import com.questdb.std.str.StringSink;
import com.questdb.std.time.MillisecondClock;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
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
                                        disp.disconnect(context, DisconnectReason.SILLY);
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
                HttpRequestProcessorSelector selector = new HttpRequestProcessorSelector() {

                    @Override
                    public HttpRequestProcessor select(CharSequence url) {
                        return null;
                    }

                    @Override
                    public HttpRequestProcessor getDefaultProcessor() {
                        return new HttpRequestProcessor() {
                            @Override
                            public void onHeadersReady(HttpConnectionContext connectionContext) {
                            }

                            @Override
                            public void onRequestComplete(HttpConnectionContext connectionContext, IODispatcher<HttpConnectionContext> dispatcher1) {
                            }
                        };
                    }
                };

                AtomicBoolean serverRunning = new AtomicBoolean(true);
                SOCountDownLatch serverHaltLatch = new SOCountDownLatch(1);

                new Thread(() -> {

                    while (serverRunning.get()) {
                        dispatcher.run();
                        dispatcher.processIOQueue(
                                (operation, context, disp) -> context.handleClientOperation(operation, disp, selector)
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
                HttpRequestProcessorSelector selector =
                        new HttpRequestProcessorSelector() {
                            @Override
                            public HttpRequestProcessor select(CharSequence url) {
                                return null;
                            }

                            @Override
                            public HttpRequestProcessor getDefaultProcessor() {
                                return new HttpRequestProcessor() {
                                    @Override
                                    public void onHeadersReady(HttpConnectionContext connectionContext) {
                                    }

                                    @Override
                                    public void onRequestComplete(HttpConnectionContext connectionContext, IODispatcher<HttpConnectionContext> dispatcher) {
                                    }
                                };
                            }
                        };

                AtomicBoolean serverRunning = new AtomicBoolean(true);
                SOCountDownLatch serverHaltLatch = new SOCountDownLatch(1);

                new Thread(() -> {
                    do {
                        dispatcher.run();
                        dispatcher.processIOQueue(
                                (operation, context, disp) -> context.handleClientOperation(operation, disp, selector)
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

            SOCountDownLatch connectLatch = new SOCountDownLatch(1);
            SOCountDownLatch contextClosedLatch = new SOCountDownLatch(1);
            SOCountDownLatch requestReceivedLatch = new SOCountDownLatch(1);
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

                final HttpRequestProcessorSelector selector =

                        new HttpRequestProcessorSelector() {
                            @Override
                            public HttpRequestProcessor select(CharSequence url) {
                                return new HttpRequestProcessor() {
                                    @Override
                                    public void onHeadersReady(HttpConnectionContext context) {
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
                                        requestReceivedLatch.countDown();
                                    }

                                    @Override
                                    public void onRequestComplete(HttpConnectionContext context, IODispatcher<HttpConnectionContext> dispatcher1) {
                                        dispatcher1.registerChannel(context, IOOperation.READ);
                                    }
                                };
                            }

                            @Override
                            public HttpRequestProcessor getDefaultProcessor() {
                                return null;
                            }
                        };

                AtomicBoolean serverRunning = new AtomicBoolean(true);
                SOCountDownLatch serverHaltLatch = new SOCountDownLatch(1);

                new Thread(() -> {
                    while (serverRunning.get()) {
                        dispatcher.run();
                        dispatcher.processIOQueue(
                                (operation, context, disp) -> context.handleClientOperation(operation, disp, selector)
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

                        // do not disconnect right away, wait for server to receive the request
                        requestReceivedLatch.await();
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
    public void testSendHttpGetAndSimpleResponse() throws Exception {

        LOG.info().$("started testSendHttpGetAndSimpleResponse").$();

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

        final String expectedResponse = "HTTP/1.1 200 OK\r\n" +
                "Server: questDB/1.0\r\n" +
                "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                "Transfer-Encoding: chunked\r\n" +
                "Content-Type: text/html; charset=utf-8\r\n" +
                "\r\n" +
                "4\r\n" +
                "OK\r\n" +
                "\r\n" +
                "0\r\n" +
                "\r\n";

        TestUtils.assertMemoryLeak(() -> {
            HttpServerConfiguration httpServerConfiguration = new DefaultHttpServerConfiguration() {
                @Override
                public MillisecondClock getClock() {
                    return () -> 0;
                }
            };

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

                final HttpRequestProcessorSelector selector =

                        new HttpRequestProcessorSelector() {
                            @Override
                            public HttpRequestProcessor select(CharSequence url) {
                                return null;
                            }

                            @Override
                            public HttpRequestProcessor getDefaultProcessor() {
                                return new HttpRequestProcessor() {
                                    @Override
                                    public void onHeadersReady(HttpConnectionContext context) {
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
                                    }

                                    @Override
                                    public void onRequestComplete(HttpConnectionContext context, IODispatcher<HttpConnectionContext> dispatcher1) {
                                        HttpResponseSink.SimpleResponseImpl response = context.simpleResponse();
                                        response.send(200);
                                        dispatcher1.registerChannel(context, IOOperation.READ);
                                    }
                                };
                            }
                        };

                AtomicBoolean serverRunning = new AtomicBoolean(true);
                SOCountDownLatch serverHaltLatch = new SOCountDownLatch(1);

                new Thread(() -> {
                    while (serverRunning.get()) {
                        dispatcher.run();
                        dispatcher.processIOQueue(
                                (operation, context, disp) -> context.handleClientOperation(operation, disp, selector)
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
                            // read response we expect
                            StringSink sink2 = new StringSink();
                            final int expectedLen = 158;
                            int read = 0;
                            while (read < expectedLen) {
                                int n = Net.recv(fd, buffer, len);
                                Assert.assertTrue(n > 0);

                                for (int i = 0; i < n; i++) {
                                    sink2.put((char) Unsafe.getUnsafe().getByte(buffer + i));
                                }
                                // copy response bytes to sink
                                read += n;
                            }

                            TestUtils.assertEquals(expectedResponse, sink2);
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

                HttpRequestProcessorSelector selector = new HttpRequestProcessorSelector() {
                    @Override
                    public HttpRequestProcessor select(CharSequence url) {
                        return null;
                    }

                    @Override
                    public HttpRequestProcessor getDefaultProcessor() {
                        return new HttpRequestProcessor() {
                            @Override
                            public void onHeadersReady(HttpConnectionContext connectionContext) {
                                HttpHeaders headers = connectionContext.getHeaders();
                                sink.put(headers.getMethodLine());
                                sink.put("\r\n");
                                ObjList<CharSequence> headerNames = headers.getHeaderNames();
                                for (int i = 0, n = headerNames.size(); i < n; i++) {
                                    sink.put(headerNames.getQuick(i)).put(':');
                                    sink.put(headers.getHeader(headerNames.getQuick(i)));
                                    sink.put("\r\n");
                                }
                                sink.put("\r\n");
                            }

                            @Override
                            public void onRequestComplete(HttpConnectionContext connectionContext, IODispatcher<HttpConnectionContext> dispatcher1) {
                                dispatcher1.registerChannel(connectionContext, IOOperation.READ);
                            }
                        };
                    }
                };

                AtomicBoolean serverRunning = new AtomicBoolean(true);
                SOCountDownLatch serverHaltLatch = new SOCountDownLatch(1);

                new Thread(() -> {
                    while (serverRunning.get()) {
                        dispatcher.run();
                        dispatcher.processIOQueue(
                                (operation, context, disp) -> context.handleClientOperation(operation, disp, selector)
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

                        TestUtils.assertEquals("", sink);
                    } finally {
                        Net.freeSockAddr(sockAddr);
                    }
                } finally {
                    Net.close(fd);
                    LOG.info().$("closed [fd=").$(fd).$(']').$();
                }

                Assert.assertEquals(1, closeCount.get());
            }
        });
    }

    @Test
    public void testStaticContentHandlerSimple() {
        String baseDir = System.getProperty("java.io.tmpdir");
        final DefaultHttpServerConfiguration httpConfiguration = new DefaultHttpServerConfiguration() {

            private final StaticContentProcessorConfiguration staticContentProcessorConfiguration = new StaticContentProcessorConfiguration() {
                @Override
                public FilesFacade getFilesFacade() {
                    return FilesFacadeImpl.INSTANCE;
                }

                @Override
                public CharSequence getIndexFileName() {
                    return null;
                }

                @Override
                public MimeTypesCache getMimeTypesCache() {
                    return mimeTypesCache;
                }

                @Override
                public CharSequence getPublicDirectory() {
                    return baseDir;
                }
            };

            @Override
            public StaticContentProcessorConfiguration getStaticContentProcessorConfiguration() {
                return staticContentProcessorConfiguration;
            }

            @Override
            public MillisecondClock getClock() {
                return () -> 0;
            }
        };

        try (HttpServer httpServer = new HttpServer(httpConfiguration)) {
            httpServer.bind(new HttpRequestProcessorFactory() {
                @Override
                public String getUrl() {
                    return HttpServerConfiguration.DEFAULT_PROCESSOR_URL;
                }

                @Override
                public HttpRequestProcessor newInstance() {
                    return new StaticContentProcessor(httpConfiguration.getStaticContentProcessorConfiguration());
                }
            });

            httpServer.start();

            // create 100Mb file in /tmp directory
            try (Path path = new Path().of(baseDir).concat("questdb-temp.txt").$()) {
                try {
                    if (Files.exists(path)) {
                        Assert.assertTrue(Files.remove(path));
                    }
                    long fd = Files.openAppend(path);

                    final int bufLen = 1024 * 1024;
                    long buf = Unsafe.malloc(bufLen); // 1Mb buffer
                    Rnd rnd = new Rnd();
                    for (int i = 0; i < bufLen; i++) {
                        Unsafe.getUnsafe().putByte(buf + i, rnd.nextByte());
                    }

                    for (int i = 0; i < 20; i++) {
                        Assert.assertEquals(bufLen, Files.append(fd, buf, bufLen));
                    }

                    Files.close(fd);
                    Files.setLastModified(path, 122222212222L);
                    Unsafe.free(buf, bufLen);

                    httpServer.getStartedLatch().await();

                    // send request to server to download file we just created
                    final String request = "GET /questdb-temp.txt HTTP/1.1\r\n" +
                            "Host: localhost:9000\r\n" +
                            "Connection: keep-alive\r\n" +
                            "Cache-Control: max-age=0\r\n" +
                            "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8\r\n" +
                            "User-Agent: Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.48 Safari/537.36\r\n" +
                            "Accept-Encoding: gzip,deflate,sdch\r\n" +
                            "Accept-Language: en-US,en;q=0.8\r\n" +
                            "Cookie: textwrapon=false; textautoformat=false; wysiwyg=textarea\r\n" +
                            "\r\n";

                    String expectedResponseHeader = "HTTP/1.1 200 OK\r\n" +
                            "Server: questDB/1.0\r\n" +
                            "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                            "Content-Length: 20971520\r\n" +
                            "Content-Type: text/plain\r\n" +
                            "ETag: \"122222212000\"\r\n" + // this is last modified timestamp on the file, we set this value when we created file
                            "\r\n";

                    int headerLen = expectedResponseHeader.length();
                    int headerCheckRemaining = expectedResponseHeader.length();

                    // prepare random generator to validate the downloaded content
                    rnd.reset();

                    fd = Net.socketTcp(true);
                    try {
                        long sockAddr = Net.sockaddr("127.0.0.1", 9001);
                        try {
                            Assert.assertTrue(fd > -1);
                            Assert.assertEquals(0, Net.connect(fd, sockAddr));

                            int len = request.length();
                            long buffer = TestUtils.toMemory(request);
                            try {
                                int part1 = len / 2;
                                Assert.assertEquals(part1, Net.send(fd, buffer, part1));
                                Assert.assertEquals(len - part1, Net.send(fd, buffer + part1, len - part1));

                                // download
                                long downloadedSoFar = 0;
                                int contentRemaining = 0;
                                while (downloadedSoFar < 20971670) {
                                    int contentOffset = 0;
                                    int n = Net.recv(fd, buffer, len);
                                    if (n > 0) {
                                        if (headerCheckRemaining > 0) {
                                            for (int i = 0; i < n && headerCheckRemaining > 0; i++) {
                                                if (expectedResponseHeader.charAt(headerLen - headerCheckRemaining) != (char) Unsafe.getUnsafe().getByte(buffer + i)) {
                                                    Assert.fail("at " + (headerLen - headerCheckRemaining));
                                                }
                                                headerCheckRemaining--;
                                                contentOffset++;
                                            }
                                        }

                                        if (headerCheckRemaining == 0) {
                                            for (int i = contentOffset; i < n; i++) {
                                                if (contentRemaining == 0) {
                                                    contentRemaining = bufLen;
                                                    rnd.reset();
                                                }
                                                Assert.assertEquals(rnd.nextByte(), Unsafe.getUnsafe().getByte(buffer + i));
                                                contentRemaining--;
                                            }

                                        }

                                        downloadedSoFar += n;
                                    }
                                }

                            } finally {
                                Unsafe.free(buffer, len);
                            }
                        } finally {
                            Net.freeSockAddr(sockAddr);
                        }
                    } finally {
                        Net.close(fd);
                        LOG.info().$("closed [fd=").$(fd).$(']').$();
                    }

                    httpServer.halt();
                } finally {
                    Files.remove(path);
                }
            }
        }
    }

    @Test
    // this test is ignore for the time being because it is unstable on OSX and I
    // have not figured out the reason yet. I would like to see if this test
    // runs any different on Linux, just to narrow the problem down to either
    // dispatcher or Http parser.
    public void testTwoThreadsSendTwoThreadsRead() throws Exception {

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

        final int N = 1000;
        final int serverThreadCount = 2;
        final int senderCount = 2;


        TestUtils.assertMemoryLeak(() -> {
            HttpServerConfiguration httpServerConfiguration = new DefaultHttpServerConfiguration();

            final NetworkFacade nf = NetworkFacadeImpl.INSTANCE;
            final AtomicInteger requestsReceived = new AtomicInteger();

            try (IODispatcher<HttpConnectionContext> dispatcher = IODispatchers.create(
                    new DefaultIODispatcherConfiguration(),
                    fd -> new HttpConnectionContext(httpServerConfiguration, fd)
            )) {

                // server will publish status of each request to this queue
                final RingQueue<Status> queue = new RingQueue<>(Status::new, 1024);
                final MPSequence pubSeq = new MPSequence(queue.getCapacity());
                SCSequence subSeq = new SCSequence();
                pubSeq.then(subSeq).then(pubSeq);

                AtomicBoolean serverRunning = new AtomicBoolean(true);

                CountDownLatch serverHaltLatch = new CountDownLatch(serverThreadCount);
                for (int j = 0; j < serverThreadCount; j++) {
                    new Thread(() -> {
                        final StringSink sink = new StringSink();
                        final long responseBuf = Unsafe.malloc(32);
                        Unsafe.getUnsafe().putByte(responseBuf, (byte) 'A');

                        final HttpRequestProcessor processor = new HttpRequestProcessor() {
                            @Override
                            public void onHeadersReady(HttpConnectionContext context) {
                                HttpHeaders headers = context.getHeaders();
                                sink.clear();
                                sink.put(headers.getMethodLine());
                                sink.put("\r\n");
                                ObjList<CharSequence> headerNames = headers.getHeaderNames();
                                for (int i = 0, n = headerNames.size(); i < n; i++) {
                                    sink.put(headerNames.getQuick(i)).put(':');
                                    sink.put(headers.getHeader(headerNames.getQuick(i)));
                                    sink.put("\r\n");
                                }
                                sink.put("\r\n");

                                boolean result;
                                try {
                                    TestUtils.assertEquals(expected, sink);
                                    result = true;
                                } catch (Exception e) {
                                    result = false;
                                }

                                while (true) {
                                    long cursor = pubSeq.next();
                                    if (cursor < 0) {
                                        continue;
                                    }
                                    queue.get(cursor).valid = result;
                                    pubSeq.done(cursor);
                                    break;
                                }

                                requestsReceived.incrementAndGet();

                                nf.send(context.getFd(), responseBuf, 1);
                            }

                            @Override
                            public void onRequestComplete(HttpConnectionContext connectionContext, IODispatcher<HttpConnectionContext> dispatcher) {
                                connectionContext.clear();

                                // there is interesting situation here, its possible that header is fully
                                // read and there are either more bytes or disconnect lingering
                                dispatcher.disconnect(connectionContext, DisconnectReason.SILLY);
                            }
                        };

                        HttpRequestProcessorSelector selector = new HttpRequestProcessorSelector() {
                            @Override
                            public HttpRequestProcessor select(CharSequence url) {
                                return null;
                            }

                            @Override
                            public HttpRequestProcessor getDefaultProcessor() {
                                return processor;
                            }
                        };

                        while (serverRunning.get()) {
                            dispatcher.run();
                            dispatcher.processIOQueue(
                                    (operation, context, disp) -> context.handleClientOperation(operation, disp, selector)
                            );
                        }

                        Unsafe.free(responseBuf, 32);
                        serverHaltLatch.countDown();
                    }).start();
                }

                for (int j = 0; j < senderCount; j++) {
                    int k = j;
                    new Thread(() -> {
                        long sockAddr = Net.sockaddr("127.0.0.1", 9001);
                        try {
                            for (int i = 0; i < N; i++) {
                                long fd = Net.socketTcp(true);
                                try {
                                    Assert.assertTrue(fd > -1);
                                    Assert.assertEquals(0, Net.connect(fd, sockAddr));

                                    int len = request.length();
                                    long buffer = TestUtils.toMemory(request);
                                    try {
                                        Assert.assertEquals(len, Net.send(fd, buffer, len));
                                        Assert.assertEquals("fd=" + fd + ", i=" + i, 1, Net.recv(fd, buffer, 1));
                                        LOG.info().$("i=").$(i).$(", j=").$(k).$();
                                        Assert.assertEquals('A', Unsafe.getUnsafe().getByte(buffer));
                                    } finally {
                                        Unsafe.free(buffer, len);
                                    }
                                } finally {
                                    Net.close(fd);
                                }
                            }
                        } finally {
                            Net.freeSockAddr(sockAddr);
                        }
                    }).start();
                }

                int receiveCount = 0;
                while (receiveCount < N * senderCount) {
                    long cursor = subSeq.next();
                    if (cursor < 0) {
                        continue;
                    }
                    boolean valid = queue.get(cursor).valid;
                    subSeq.done(cursor);
                    Assert.assertTrue(valid);
                    receiveCount++;
                }

                serverRunning.set(false);
                serverHaltLatch.await();
            }
            Assert.assertEquals(N * senderCount, requestsReceived.get());
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

    class Status {
        boolean valid;
    }
}