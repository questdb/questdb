/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.network;

import io.questdb.std.Chars;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.CharSequenceZ;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class NetTest {
    private int port = 9992;

    @Test
    public void testBindAndListenTcpToLocalhost() {
        long fd = Net.socketTcp(false);
        try {
            if (!Net.bindTcp(fd, "127.0.0.1", 9005)) {
                Assert.fail("Failed to bind tcp socket to localhost. Errno=" + Os.errno());
            } else {
                Net.listen(fd, 100);
            }
        } finally {
            Net.close(fd);
        }
    }

    @Test
    public void testBindAndListenUdpToLocalhost() {
        long fd = Net.socketUdp();
        try {
            if (!Net.bindUdp(fd, Net.parseIPv4("127.0.0.1"), 9005)) {
                Assert.fail("Failed to bind udp socket to localhost. Errno=" + Os.errno());
            } else {
                Net.listen(fd, 100);
            }
        } finally {
            Net.close(fd);
        }
    }

    @Test
    public void testGetAddrInfoConnect() {
        NetworkFacade nf = NetworkFacadeImpl.INSTANCE;
        final long pAddrInfo = nf.getAddrInfo("questdb.io", 443);
        Assert.assertNotEquals(-1, pAddrInfo);
        long fd = nf.socketTcp(true);
        try {
            Assert.assertEquals(0, nf.connectAddrInfo(fd, pAddrInfo));
        } finally {
            nf.close(fd);
            nf.freeAddrInfo(pAddrInfo);
        }
    }

    @Test
    public void testGetAddrInfoConnectLocalhost() {
        long acceptFd = Net.socketTcp(true);
        Assert.assertTrue(acceptFd > 0);
        int port = assertCanBind(acceptFd);
        Net.listen(acceptFd, 1);

        long clientFd = Net.socketTcp(true);
        Assert.assertTrue(clientFd > 0);
        long addrInfo = Net.getAddrInfo("localhost", port);
        Assert.assertTrue(addrInfo > 0);
        TestUtils.assertConnectAddrInfo(clientFd, addrInfo);
        Net.freeAddrInfo(addrInfo);
        Net.close(clientFd);
        Net.close(acceptFd);
    }

    @Test
    @Ignore
    public void testMulticast() {
        long socket = Net.socketUdp();
        System.out.println(socket);
        bindSocket(socket);
        System.out.println(Net.setMulticastInterface(socket, Net.parseIPv4("192.168.1.156")));
        System.out.println(Net.setMulticastLoop(socket, true));
        System.out.println(Net.setMulticastTtl(socket, 1));
        System.out.println(Os.errno());
    }

    @Test
    public void testNoLinger() throws InterruptedException, BrokenBarrierException {
        bindAcceptConnectClose();
        bindAcceptConnectClose();
    }

    @Test
    public void testReusePort() {
        long fd1 = Net.socketUdp();
        try {
            bindSocket(fd1);
            Os.sleep(1000L);
            long fd2 = Net.socketUdp();
            try {
                bindSocket(fd2);
            } finally {
                Net.close(fd2);
            }
        } finally {
            Net.close(fd1);
        }
    }

    @Test
    public void testSeek() {
        String msg = "Test ABC";
        StringSink sink = new StringSink();
        CharSequenceZ charSink = new CharSequenceZ(msg);
        int msgLen = charSink.length() + 1;

        long acceptFd = Net.socketTcp(true);
        Assert.assertTrue(acceptFd > 0);
        int port = assertCanBind(acceptFd);
        Net.listen(acceptFd, 1024);

        long clientFd = Net.socketTcp(true);
        long sockAddr = Net.sockaddr("127.0.0.1", port);
        TestUtils.assertConnect(clientFd, sockAddr);
        Assert.assertEquals(msgLen, Net.send(clientFd, charSink.address(), msgLen));
        Net.close(clientFd);
        Net.freeSockAddr(sockAddr);

        long serverFd = Net.accept(acceptFd);
        long serverBuf = Unsafe.malloc(msgLen, MemoryTag.NATIVE_DEFAULT);
        Assert.assertEquals(msgLen, Net.peek(serverFd, serverBuf, msgLen));
        Chars.utf8DecodeZ(serverBuf, sink);
        TestUtils.assertEquals(msg, sink);
        Assert.assertEquals(msgLen, Net.recv(serverFd, serverBuf, msgLen));
        sink.clear();
        Chars.utf8DecodeZ(serverBuf, sink);
        TestUtils.assertEquals(msg, sink);
        Unsafe.free(serverBuf, msgLen, MemoryTag.NATIVE_DEFAULT);
        Net.close(serverFd);

        Net.close(acceptFd);
        charSink.close();
    }

    @Test
    public void testSendAndRecvBuffer() throws InterruptedException, BrokenBarrierException {
        long fd = Net.socketTcp(true);
        Assert.assertTrue(fd > 0);
        int port = assertCanBind(fd);
        Net.listen(fd, 1024);

        // make sure peerIp in correct byte order
        StringSink sink = new StringSink();

        CountDownLatch haltLatch = new CountDownLatch(1);
        CyclicBarrier barrier = new CyclicBarrier(2);
        AtomicBoolean threadFailed = new AtomicBoolean(false);
        CountDownLatch ipCollectedLatch = new CountDownLatch(1);

        new Thread(() -> {
            try {
                barrier.await();
                long clientfd = Net.accept(fd);
                Net.appendIP4(sink, Net.getPeerIP(clientfd));
                ipCollectedLatch.countDown();
                Net.configureNoLinger(clientfd);
                while (!Net.isDead(clientfd)) {
                    Os.pause();
                }
                Net.close(clientfd);
                haltLatch.countDown();
            } catch (Exception e) {
                threadFailed.set(true);
                e.printStackTrace();
            } finally {
                haltLatch.countDown();
            }
        }).start();

        barrier.await();
        long clientFd = Net.socketTcp(true);
        long sockAddr = Net.sockaddr("127.0.0.1", port);
        TestUtils.assertConnect(clientFd, sockAddr);
        Assert.assertEquals(0, Net.setSndBuf(clientFd, 256));
        // Linux kernel doubles the value we set, so we handle this case separately
        // http://man7.org/linux/man-pages/man7/socket.7.html
        if (Os.type == Os.LINUX_AMD64 || Os.type == Os.LINUX_ARM64) {
            Assert.assertEquals(4608, Net.getSndBuf(clientFd));
        } else {
            Assert.assertEquals(256, Net.getSndBuf(clientFd));
        }

        Assert.assertEquals(0, Net.setRcvBuf(clientFd, 512));
        if (Os.type == Os.LINUX_AMD64 || Os.type == Os.LINUX_ARM64) {
            Assert.assertEquals(2304, Net.getRcvBuf(clientFd));
        } else {
            int rcvBuf = Net.getRcvBuf(clientFd);
            if (Os.type == Os.OSX_AMD64 || Os.type == Os.OSX_ARM64) {
                // OSX can ignore setsockopt SO_RCVBUF sometimes
                Assert.assertTrue(rcvBuf == 512 || rcvBuf == 261824);
            } else {
                Assert.assertEquals(512, rcvBuf);
            }
        }
        ipCollectedLatch.await();
        Net.close(clientFd);
        Net.close(fd);
        Assert.assertTrue(haltLatch.await(10, TimeUnit.SECONDS));

        TestUtils.assertEquals("127.0.0.1", sink);
        Assert.assertFalse(threadFailed.get());
    }

    @Test
    public void testSocketShutdown() throws BrokenBarrierException, InterruptedException {
        final CyclicBarrier barrier = new CyclicBarrier(2);
        final CountDownLatch haltLatch = new CountDownLatch(1);
        final AtomicLong fileDescriptor = new AtomicLong();

        new Thread(() -> {
            long fd = Net.socketTcp(true);
            try {
                Net.configureNoLinger(fd);
                Assert.assertTrue(Net.bindTcp(fd, 0, 19004));
                Net.listen(fd, 64);
                barrier.await();
                fileDescriptor.set(fd);
                Net.accept(fd);
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            } finally {
                Net.close(fd);
                haltLatch.countDown();
            }
        }).start();

        barrier.await();
        Os.sleep(500);
        Net.abortAccept(fileDescriptor.get());
        Assert.assertTrue(haltLatch.await(2, TimeUnit.SECONDS));
    }

    @Test
    public void testTcpNoDelay() {
        long fd = Net.socketTcp(true);
        try {
            Assert.assertEquals(0, Net.setTcpNoDelay(fd, false));
            Assert.assertEquals(0, Net.getTcpNoDelay(fd));
            Assert.assertEquals(0, Net.setTcpNoDelay(fd, true));
            Assert.assertTrue(Net.getTcpNoDelay(fd) > 0);
        } finally {
            Net.close(fd);
        }
    }

    private int assertCanBind(long fd) {
        boolean bound = false;
        for (int i = 0; i < 1000 && !bound; i++) {
            bound = Net.bindTcp(fd, 0, ++port);
        }
        Assert.assertTrue(bound);
        return port;
    }

    private void bindAcceptConnectClose() throws InterruptedException, BrokenBarrierException {
        long fd = Net.socketTcp(true);
        Assert.assertTrue(fd > 0);
        int port = assertCanBind(fd);
        Net.listen(fd, 1024);

        // make sure peerIp in correct byte order
        StringSink sink = new StringSink();

        CountDownLatch haltLatch = new CountDownLatch(1);
        CyclicBarrier barrier = new CyclicBarrier(2);
        AtomicBoolean threadFailed = new AtomicBoolean(false);

        new Thread(() -> {
            try {
                barrier.await();
                long clientFd = Net.accept(fd);
                Net.appendIP4(sink, Net.getPeerIP(clientFd));
                Net.configureNoLinger(clientFd);
                Net.close(clientFd);
            } catch (Exception e) {
                threadFailed.set(true);
                e.printStackTrace();
            } finally {
                haltLatch.countDown();
            }
        }).start();

        barrier.await();
        long clientFd = Net.socketTcp(true);
        long sockAddr = Net.sockaddr("127.0.0.1", port);
        long sockFd = -1;
        for (int i = 0; i < 2000; i++) {
            Net.configureNoLinger(clientFd);
            sockFd = Net.connect(clientFd, sockAddr);
            if (sockFd >= 0) {
                break;
            }
            Os.sleep(5);
        }
        Assert.assertEquals(0, sockFd);
        Assert.assertTrue(haltLatch.await(10, TimeUnit.SECONDS));
        Net.close(clientFd);
        Net.close(fd);

        TestUtils.assertEquals("127.0.0.1", sink);
        Assert.assertFalse(threadFailed.get());
    }

    private void bindSocket(long fd) {
        Assert.assertTrue(fd > 0);
        Assert.assertEquals(0, Net.setReuseAddress(fd));
        Assert.assertEquals(0, Net.setReusePort(fd));
        Assert.assertTrue(Net.bindUdp(fd, 0, 18215));
        Assert.assertTrue(Net.join(fd, "0.0.0.0", "224.0.0.125"));
    }
}
