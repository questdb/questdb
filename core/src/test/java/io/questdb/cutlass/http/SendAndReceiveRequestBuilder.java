package io.questdb.cutlass.http;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.NetworkFacade;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Unsafe;
import org.junit.Assert;

import java.nio.charset.StandardCharsets;

public class SendAndReceiveRequestBuilder {
    private static final Log LOG = LogFactory.getLog(SendAndReceiveRequestBuilder.class);
    private NetworkFacade nf;
    private long pauseBetweenSendAndReceive;
    private boolean printOnly;
    private boolean expectDisconnect;
    private int requestCount;
    private int maxWaitTimeoutMs = 5000;

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

    public SendAndReceiveRequestBuilder withExpectDisconnect(boolean expectDisconnect) {
        this.expectDisconnect = expectDisconnect;
        return this;
    }
    
    public SendAndReceiveRequestBuilder withRequestCount(int requestCount) {
        this.requestCount = requestCount;
        return this;
    }

    public SendAndReceiveRequestBuilder withMaxTimeoutMs(int maxWaitTimeoutMs) {
        this.maxWaitTimeoutMs = maxWaitTimeoutMs;
        return this;
    }

    public void execute(
            String request,
            String response         
    ) throws InterruptedException {
        long fd = nf.socketTcp(true);
        try {
            long sockAddr = nf.sockaddr("127.0.0.1", 9001);
            try {
                Assert.assertTrue(fd > -1);
                Assert.assertEquals(0, nf.connect(fd, sockAddr));
                Assert.assertEquals(0, nf.setTcpNoDelay(fd, true));

                byte[] expectedResponse = response.getBytes();
                final int len = Math.max(expectedResponse.length, request.length()) * 2;
                long ptr = Unsafe.malloc(len);
                try {
                    for (int j = 0; j < requestCount; j++) {
                        executeExplicit(request, fd, expectedResponse, len, ptr, null);
                    }
                } finally {
                    Unsafe.free(ptr, len);
                }
            } finally {
                nf.freeSockAddr(sockAddr);
            }
        } finally {
            nf.close(fd);
        }
    }

    public void executeExplicit(String request, long fd, byte[] expectedResponse, final int len, long ptr,  HttpClientStateListener listener) throws InterruptedException {
        long timestamp = System.currentTimeMillis();

        if (null != listener) {
            listener.onStartingRequest();
        }
        int sent = 0;
        int reqLen = request.length();
        Chars.asciiStrCpy(request, reqLen, ptr);
        while (sent < reqLen) {
            int n = nf.send(fd, ptr + sent, reqLen - sent);
            Assert.assertTrue(n > -1);
            sent += n;
        }

        if (pauseBetweenSendAndReceive > 0) {
            Thread.sleep(pauseBetweenSendAndReceive);
        }
        // receive response
        final int expectedToReceive = expectedResponse.length;
        int received = 0;
        if (printOnly) {
            System.out.println("expected");
            System.out.println(new String(expectedResponse, StandardCharsets.UTF_8));
        }
        boolean disconnected = false;
        boolean timeoutExpired = false;
        IntList receivedByteList = new IntList(expectedToReceive);
        while (received < expectedToReceive) {
            int n = nf.recv(fd, ptr + received, len - received);
            if (n > 0) {
                for (int i = 0; i < n; i++) {
                    receivedByteList.add(Unsafe.getUnsafe().getByte(ptr + received + i));
                }
                received += n;
            } else if (n < 0) {
                disconnected = true;
                break;
            } else if (n == 0) {
                if (System.currentTimeMillis() - timestamp > maxWaitTimeoutMs) {
                    timeoutExpired = true;
                    break;
                }
            }
        }
        byte[] receivedBytes = new byte[receivedByteList.size()];
        for (int i = 0; i < receivedByteList.size(); i++) {
            receivedBytes[i] = (byte) receivedByteList.getQuick(i);
        }

        String actual = new String(receivedBytes, StandardCharsets.UTF_8);
        if (!printOnly) {
            Assert.assertEquals(new String(expectedResponse, StandardCharsets.UTF_8), actual);
        } else {
            System.out.println("actual");
            System.out.println(actual);
        }
        if (disconnected && !expectDisconnect) {
            LOG.error().$("disconnected?").$();
            Assert.fail();
        }
        if (timeoutExpired) {
            LOG.error().$("timeout expired").$();
            Assert.fail();
        }
    }
}
