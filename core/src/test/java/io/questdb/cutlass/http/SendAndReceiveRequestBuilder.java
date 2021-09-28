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

import java.nio.charset.StandardCharsets;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.locks.LockSupport;

import io.questdb.std.*;
import org.junit.Assert;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.str.ByteSequence;
import io.questdb.test.tools.TestUtils;

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
    public final static String ResponseHeaders =
            "HTTP/1.1 200 OK\r\n" +
                    "Server: questDB/1.0\r\n" +
                    "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                    "Transfer-Encoding: chunked\r\n" +
                    "Content-Type: application/json; charset=utf-8\r\n" +
                    "Keep-Alive: timeout=5, max=10000\r\n" +
                    "\r\n";

    private static final Log LOG = LogFactory.getLog(SendAndReceiveRequestBuilder.class);
    private final int maxWaitTimeoutMs = 30000;
    private NetworkFacade nf = NetworkFacadeImpl.INSTANCE;
    private long pauseBetweenSendAndReceive;
    private boolean printOnly;
    private boolean expectDisconnect;
    private int requestCount = 1;
    private int compareLength = -1;
    private boolean expectSendDisconnect;

    public long connectAndSendRequest(String request) {
        final long fd = nf.socketTcp(true);
        nf.configureNoLinger(fd);
        long sockAddr = nf.sockaddr("127.0.0.1", 9001);
        try {
            TestUtils.assertConnect(fd, sockAddr);
            Assert.assertEquals(0, nf.setTcpNoDelay(fd, true));
            if (!expectDisconnect) {
                NetworkFacadeImpl.INSTANCE.configureNonBlocking(fd);
            }

            executeWithSocket(request, "", fd);
        } finally {
            nf.freeSockAddr(sockAddr);
        }
        return fd;
    }

    public void execute(
            String request,
            CharSequence response
    ) throws InterruptedException {
        final long fd = nf.socketTcp(true);
        nf.configureNoLinger(fd);
        try {
            long sockAddr = nf.sockaddr("127.0.0.1", 9001);
            try {
                Assert.assertTrue(fd > -1);
                TestUtils.assertConnect(nf, fd, sockAddr);
                Assert.assertEquals(0, nf.setTcpNoDelay(fd, true));
                if (!expectDisconnect) {
                    nf.configureNonBlocking(fd);
                }

                executeWithSocket(request, response, fd);
            } finally {
                nf.freeSockAddr(sockAddr);
            }
        } finally {
            nf.close(fd);
        }
    }

    public void executeExplicit(
            String request,
            long fd,
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
        final int expectedToReceive = expectedResponse.length();
        int received = 0;
        if (printOnly) {
            System.out.println("expected");
            System.out.println(expectedResponse);
        }

        boolean disconnected = false;
        boolean timeoutExpired = false;
        IntList receivedByteList = new IntList(expectedToReceive);
        while (received < expectedToReceive) {
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
                    Os.sleep(10);
                }
            }
        }

        byte[] receivedBytes = new byte[receivedByteList.size()];
        for (int i = 0; i < receivedByteList.size(); i++) {
            receivedBytes[i] = (byte) receivedByteList.getQuick(i);
        }

        String actual = new String(receivedBytes, StandardCharsets.UTF_8);
        if (!printOnly) {
            if (expectedResponse instanceof ByteSequence) {
                Assert.assertEquals(expectedResponse.length(), receivedBytes.length);
                for (int n = 0; n < receivedBytes.length; n++) {
                    Assert.assertEquals(receivedBytes[n], ((ByteSequence) expectedResponse).byteAt(n));
                }
            } else {
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
        }

        if (disconnected && !expectDisconnect && !expectSendDisconnect) {
            LOG.error().$("disconnected?").$();
            Assert.fail();
        }

        if (timeoutExpired) {
            LOG.error().$("timeout expired").$();
            Assert.fail();
        }
    }

    public void executeMany(RequestAction action) throws InterruptedException, BrokenBarrierException {
        final long fd = nf.socketTcp(true);
        nf.configureNoLinger(fd);
        try {
            long sockAddr = nf.sockaddr("127.0.0.1", 9001);
            Assert.assertTrue(fd > -1);
            TestUtils.assertConnect(nf, fd, sockAddr);
            Assert.assertEquals(0, nf.setTcpNoDelay(fd, true));
            if (!expectDisconnect) {
                nf.configureNonBlocking(fd);
            }

            try {
                RequestExecutor executor = new RequestExecutor() {
                    @Override
                    public void executeWithStandardHeaders(String request, String response) {
                        executeWithSocket(request + RequestHeaders, ResponseHeaders + response, fd);
                    }

                    @Override
                    public void execute(String request, String response) {
                        executeWithSocket(request, response, fd);
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

    public void executeUntilDisconnect(String request, long fd, final int len, long ptr, HttpClientStateListener listener) {
        withExpectDisconnect(true);
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
                assert listener != null;
                listener.onClosed();
                break;
            } else {
                if (System.currentTimeMillis() - timestamp > maxWaitTimeoutMs) {
                    timeoutExpired = true;
                    break;
                } else {
                    LockSupport.parkNanos(1);
                }
            }
        }
        byte[] receivedBytes = new byte[receivedByteList.size()];
        for (int i = 0; i < receivedByteList.size(); i++) {
            receivedBytes[i] = (byte) receivedByteList.getQuick(i);
        }

        String actual = new String(receivedBytes, StandardCharsets.UTF_8);
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
        execute(request + RequestHeaders, ResponseHeaders + response);
    }

    public SendAndReceiveRequestBuilder withCompareLength(int compareLength) {
        this.compareLength = compareLength;
        return this;
    }

    public SendAndReceiveRequestBuilder withExpectDisconnect(boolean expectDisconnect) {
        this.expectDisconnect = expectDisconnect;
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

    private void executeWithSocket(String request, CharSequence response, long fd) {
        final int len = Math.max(response.length(), request.length()) * 2;
        long ptr = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int j = 0; j < requestCount; j++) {
                executeExplicit(request, fd, response, len, ptr, null);
            }
        } finally {
            Unsafe.free(ptr, len, MemoryTag.NATIVE_DEFAULT);
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
