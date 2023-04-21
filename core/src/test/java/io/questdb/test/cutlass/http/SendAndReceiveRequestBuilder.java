/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test.cutlass.http;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.*;
import io.questdb.std.str.ByteSequence;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;

import java.nio.file.Paths;
import java.util.concurrent.BrokenBarrierException;

public class SendAndReceiveRequestBuilder {
    public final static String RequestHeaders = "Host: localhost:9000\r\n" +
            "Connection: keep-alive\r\n" +
            "Accept: */*\r\n" +
            "X-Requested-With: XMLHttpRequest\r\n" +
            "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
            "Sec-Fetch-Site: same-origin\r\n" +
            "Sec-Fetch-Mode: cors\r\n" +
            "Referer: http://localhost:9000/index.html\r\n" +
            "Accept-Encoding: gzip, deflate, br\r\n" +
            "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
            "\r\n";
    public final static String ResponseHeaders = "HTTP/1.1 200 OK\r\n" +
            "Server: questDB/1.0\r\n" +
            "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
            "Transfer-Encoding: chunked\r\n" +
            "Content-Type: application/json; charset=utf-8\r\n" +
            "Keep-Alive: timeout=5, max=10000\r\n" +
            "\r\n";

    private static final Log LOG = LogFactory.getLog(SendAndReceiveRequestBuilder.class);
    private final int maxWaitTimeoutMs = 30_000;
    private int clientLingerSeconds = -1;
    private int compareLength = -1;
    private boolean expectReceiveDisconnect;
    private boolean expectSendDisconnect;
    private NetworkFacade nf = NetworkFacadeImpl.INSTANCE;
    private long pauseBetweenSendAndReceive;
    private boolean printOnly;
    private int requestCount = 1;
    private long statementTimeout = -1L;

    public int connectAndSendRequest(String request) {
        final int fd = nf.socketTcp(true);
        long sockAddrInfo = nf.getAddrInfo("127.0.0.1", 9001);
        try {
            TestUtils.assertConnectAddrInfo(fd, sockAddrInfo);
            if (clientLingerSeconds > -1) {
                Assert.assertEquals(0, nf.configureLinger(fd, clientLingerSeconds));
            }
            Assert.assertEquals(0, nf.setTcpNoDelay(fd, true));
            if (!expectReceiveDisconnect) {
                nf.configureNonBlocking(fd);
            }

            executeWithSocket(request, "", fd);
        } finally {
            nf.freeAddrInfo(sockAddrInfo);
        }
        return fd;
    }

    public int connectAndSendRequestWithHeaders(String request) {
        return connectAndSendRequest(request + requestHeaders());
    }

    public void execute(String request, CharSequence expectedResponse) {
        final int fd = nf.socketTcp(true);
        try {
            long sockAddrInfo = nf.sockaddr("127.0.0.1", 9001);
            try {
                Assert.assertTrue(fd > -1);
                TestUtils.assertConnect(nf, fd, sockAddrInfo);
                Assert.assertEquals(0, nf.setTcpNoDelay(fd, true));
                if (!expectReceiveDisconnect) {
                    nf.configureNonBlocking(fd);
                }

                executeWithSocket(request, expectedResponse, fd);
            } finally {
                nf.freeSockAddr(sockAddrInfo);
            }
        } finally {
            nf.close(fd);
        }
    }

    public void executeExplicit(
            String request,
            int fd,
            CharSequence expectedResponse,
            final int len,
            long ptr,
            HttpClientStateListener listener
    ) {
        long timestamp = System.currentTimeMillis();
        int sent = 0;
        int reqLen = request.length();
        Chars.asciiStrCpy(request, reqLen, ptr);
        while (sent < reqLen) {
            int n = nf.send(fd, ptr + sent, reqLen - sent);
            if (n < 0 && expectSendDisconnect) {
                return;
            }
            Assert.assertTrue(n > -1);
            sent += n;
        }

        if (pauseBetweenSendAndReceive > 0) {
            Os.sleep(pauseBetweenSendAndReceive);
        }
        // receive response
        final int expectedToReceive = (expectedResponse instanceof String) ? ((String) expectedResponse).getBytes().length : expectedResponse.length();
        int received = 0;
        if (printOnly) {
            System.out.println("expected");
            System.out.println(expectedResponse);
        }

        boolean disconnected = false;
        boolean timeoutExpired = false;
        IntList receivedByteList = new IntList(expectedToReceive);
        while (received < expectedToReceive || expectReceiveDisconnect) {
            int n = nf.recv(fd, ptr + received, len - received);
            if (n > 0) {
                for (int i = 0; i < n; i++) {
                    receivedByteList.add(Unsafe.getUnsafe().getByte(ptr + received + i) & 0xff);
                }
                received += n;
                if (null != listener) {
                    listener.onReceived(received);
                }
            } else if (n < 0) {
                LOG.error().$("server disconnected").$();
                disconnected = true;
                break;
            } else {
                if (System.currentTimeMillis() - timestamp > maxWaitTimeoutMs) {
                    timeoutExpired = true;
                    break;
                } else {
                    Os.pause();
                }
            }
        }

        int lim = Math.min(expectedToReceive, receivedByteList.size());
        byte[] receivedBytes = new byte[lim];
        for (int i = 0; i < lim; i++) {
            receivedBytes[i] = (byte) receivedByteList.getQuick(i);
        }

        if (!printOnly) {
            if (expectedResponse instanceof ByteSequence) {
                Assert.assertEquals(expectedResponse.length(), receivedBytes.length);
                for (int n = 0; n < receivedBytes.length; n++) {
                    Assert.assertEquals(receivedBytes[n], ((ByteSequence) expectedResponse).byteAt(n));
                }
            } else {
                String actual = new String(receivedBytes, Files.UTF_8);
                String expected = expectedResponse.toString();
                if (compareLength > 0) {
                    expected = expected.substring(0, Math.min(compareLength, expected.length()) - 1);
                    actual = actual.length() > 0 ? actual.substring(0, Math.min(compareLength, actual.length()) - 1) : actual;
                }
                if (!expectSendDisconnect) {
                    // expectSendDisconnect means that test expect disconnect during send or straight after
                    TestUtils.assertEquals(disconnected ? "Server disconnected" : null, expected, actual);
                }
            }
        } else {
            TestUtils.unchecked(() -> java.nio.file.Files.write(Paths.get("actual.txt"), receivedBytes));
        }

        if (disconnected && !expectReceiveDisconnect && !expectSendDisconnect) {
            LOG.error().$("disconnected?").$();
            Assert.fail();
        }

        if (expectReceiveDisconnect) {
            Assert.assertTrue("server disconnect was expected", disconnected);
        }

        if (timeoutExpired) {
            LOG.error().$("timeout expired").$();
            Assert.fail();
        }
    }

    public void executeMany(RequestAction action) throws InterruptedException, BrokenBarrierException {
        final int fd = nf.socketTcp(true);
        try {
            long sockAddr = nf.sockaddr("127.0.0.1", 9001);
            Assert.assertTrue(fd > -1);
            TestUtils.assertConnect(nf, fd, sockAddr);
            Assert.assertEquals(0, nf.setTcpNoDelay(fd, true));
            if (!expectReceiveDisconnect) {
                nf.configureNonBlocking(fd);
            }

            try {
                RequestExecutor executor = new RequestExecutor() {
                    @Override
                    public void execute(String request, String response) {
                        executeWithSocket(request, response, fd);
                    }

                    @Override
                    public void executeWithStandardHeaders(String request, String response) {
                        executeWithSocket(request + RequestHeaders, ResponseHeaders + response, fd);
                    }
                };

                action.run(executor);
            } finally {
                nf.freeSockAddr(sockAddr);
            }
        } finally {
            nf.close(fd);
        }
    }

    public void executeUntilDisconnect(String request, int fd, final int len, long ptr, HttpClientStateListener listener) {
        withExpectReceiveDisconnect(true);
        long timestamp = System.currentTimeMillis();
        int sent = 0;
        int reqLen = request.length();
        Chars.asciiStrCpy(request, reqLen, ptr);
        while (sent < reqLen) {
            int n = nf.send(fd, ptr + sent, reqLen - sent);
            Assert.assertTrue(n > -1);
            sent += n;
        }

        if (pauseBetweenSendAndReceive > 0) {
            Os.sleep(pauseBetweenSendAndReceive);
        }

        boolean timeoutExpired = false;
        int received = 0;
        IntList receivedByteList = new IntList();
        while (true) {
            int n = nf.recv(fd, ptr + received, len - received);
            if (n > 0) {
                for (int i = 0; i < n; i++) {
                    receivedByteList.add(Unsafe.getUnsafe().getByte(ptr + received + i));
                }
                received += n;
                if (null != listener) {
                    listener.onReceived(received);
                }
            } else if (n < 0) {
                LOG.error().$("server disconnected").$();
                if (listener != null) {
                    listener.onClosed();
                }
                break;
            } else {
                if (System.currentTimeMillis() - timestamp > maxWaitTimeoutMs) {
                    timeoutExpired = true;
                    break;
                } else {
                    Os.pause();
                }
            }
        }
        byte[] receivedBytes = new byte[receivedByteList.size()];
        for (int i = 0; i < receivedByteList.size(); i++) {
            receivedBytes[i] = (byte) receivedByteList.getQuick(i);
        }

        String actual = new String(receivedBytes, Files.UTF_8);
        if (printOnly) {
            System.out.println("actual");
            System.out.println(actual);
        }

        if (timeoutExpired) {
            LOG.error().$("timeout expired").$();
            Assert.fail();
        }
    }

    public void executeWithStandardHeaders(
            String request,
            String response
    ) throws InterruptedException {
        execute(request + requestHeaders(), ResponseHeaders + response);
    }

    public void executeWithStandardRequestHeaders(
            String request,
            CharSequence response
    ) throws InterruptedException {
        execute(request + requestHeaders(), response);
    }

    public SendAndReceiveRequestBuilder withClientLinger(int seconds) {
        this.clientLingerSeconds = seconds;
        return this;
    }

    public SendAndReceiveRequestBuilder withCompareLength(int compareLength) {
        this.compareLength = compareLength;
        return this;
    }

    public SendAndReceiveRequestBuilder withExpectReceiveDisconnect(boolean expectDisconnect) {
        this.expectReceiveDisconnect = expectDisconnect;
        return this;
    }

    public SendAndReceiveRequestBuilder withExpectSendDisconnect(boolean expectDisconnect) {
        this.expectSendDisconnect = expectDisconnect;
        return this;
    }

    public SendAndReceiveRequestBuilder withNetworkFacade(NetworkFacade nf) {
        this.nf = nf;
        return this;
    }

    public SendAndReceiveRequestBuilder withPauseBetweenSendAndReceive(long pauseBetweenSendAndReceive) {
        this.pauseBetweenSendAndReceive = pauseBetweenSendAndReceive;
        return this;
    }

    public SendAndReceiveRequestBuilder withPrintOnly(boolean printOnly) {
        this.printOnly = printOnly;
        return this;
    }

    public SendAndReceiveRequestBuilder withRequestCount(int requestCount) {
        this.requestCount = requestCount;
        return this;
    }

    public SendAndReceiveRequestBuilder withStatementTimeout(long statementTimeout) {
        this.statementTimeout = statementTimeout;
        return this;
    }

    private void executeWithSocket(String request, CharSequence expectedResponse, int fd) {
        final int len = Math.max(expectedResponse.length(), request.length()) * 2;
        long ptr = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int j = 0; j < requestCount; j++) {
                executeExplicit(request, fd, expectedResponse, len, ptr, null);
            }
        } finally {
            Unsafe.free(ptr, len, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private String requestHeaders() {
        if (statementTimeout < 0) {
            return RequestHeaders;
        } else {
            return "Host: localhost:9000\r\n" +
                    "Connection: keep-alive\r\n" +
                    "Accept: */*\r\n" +
                    "X-Requested-With: XMLHttpRequest\r\n" +
                    "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36\r\n" +
                    "Sec-Fetch-Site: same-origin\r\n" +
                    "Sec-Fetch-Mode: cors\r\n" +
                    "Referer: http://localhost:9000/index.html\r\n" +
                    "Accept-Encoding: gzip, deflate, br\r\n" +
                    "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8\r\n" +
                    "Statement-Timeout: " + statementTimeout + "\r\n" +
                    "\r\n";

        }
    }

    @FunctionalInterface
    public interface RequestAction {
        void run(RequestExecutor executor) throws InterruptedException, BrokenBarrierException;
    }

    public interface RequestExecutor {
        void execute(
                String request,
                String response
        ) throws InterruptedException;

        void executeWithStandardHeaders(
                String request,
                String response
        ) throws InterruptedException;
    }
}
