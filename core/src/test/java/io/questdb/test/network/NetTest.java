/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.test.network;

import io.questdb.network.Net;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
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

import static org.junit.Assert.fail;

public class NetTest {
    private int port = 9992;

    @Test
    public void testBindAndListenTcpToLocalhost() {
        long fd = Net.socketTcp(false);
        try {
            if (!Net.bindTcp(fd, "127.0.0.1", 9005)) {
                fail("Failed to bind tcp socket to localhost. Errno=" + Os.errno());
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
                fail("Failed to bind udp socket to localhost. Errno=" + Os.errno());
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
        if (pAddrInfo == -1) {
            try {
                //noinspection ResultOfMethodCallIgnored
                java.net.InetAddress.getByName("questdb.io");
                Assert.fail("getAddrInfo failed but Java DNS resolved questdb.io");
            } catch (java.net.UnknownHostException e) {
                // no internet connection, skip the test
                return;
            }
        }
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
    public void testIsPeerDisconnected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long acceptFd = Net.socketTcp(true);
            Assert.assertTrue(acceptFd > 0);
            // Hoisted so a failed assertion in any block still reclaims the open fds and buffer in
            // the finally rather than leaking them.
            long sockAddr = 0;
            long clientFd = -1;
            long serverFd = -1;
            long buf = 0;
            try {
                int port = assertCanBind(acceptFd);
                Net.listen(acceptFd, 1024);
                sockAddr = Net.sockaddr("127.0.0.1", port);
                // Idle, both ends open: no hangup on any platform.
                clientFd = Net.socketTcp(true);
                TestUtils.assertConnect(clientFd, sockAddr);
                serverFd = Net.accept(acceptFd);
                Net.configureNonBlocking(serverFd);
                Assert.assertFalse(Net.isPeerDisconnected(serverFd));
                // Probe-resource churn: a probe that leaks a descriptor per call (instead of
                // reusing the per-thread kqueue) would exhaust the fd table during this loop
                // (macOS default ulimit 10240). The probe fails open on kqueue() failure, so the
                // in-loop assertFalse cannot catch it -- the headroom check below does.
                for (int i = 0; i < 12_000; i++) {
                    Assert.assertFalse(Net.isPeerDisconnected(serverFd));
                }
                final long[] headroomFds = new long[64];
                try {
                    for (int i = 0; i < headroomFds.length; i++) {
                        headroomFds[i] = Net.socketTcp(true);
                        Assert.assertTrue("fd table exhausted after probe churn -- the probe leaks descriptors",
                                headroomFds[i] > 0);
                    }
                } finally {
                    for (int i = 0; i < headroomFds.length; i++) {
                        if (headroomFds[i] > 0) {
                            Net.close(headroomFds[i]);
                        }
                    }
                }
                clientFd = closeFd(clientFd);
                serverFd = closeFd(serverFd);

                // Live socket, readable inbound data, NO FIN: must stay false on every platform. This
                // is the dangerous false-positive to guard -- misreading merely-readable bytes (e.g. a
                // pipelined follow-up request during a running query) as a hangup would abort live
                // queries. Distinct from the FIN-behind-a-byte block below, which couples data WITH a
                // FIN; here the peer never shuts down.
                clientFd = Net.socketTcp(true);
                TestUtils.assertConnect(clientFd, sockAddr);
                serverFd = Net.accept(acceptFd);
                Net.configureNonBlocking(serverFd);
                buf = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
                Unsafe.getUnsafe().putByte(buf, (byte) 'x');
                Assert.assertEquals(1, Net.send(clientFd, buf, 1));
                // Wait until the byte is actually buffered on the server so the probe faces
                // readable-but-no-FIN data rather than an empty socket.
                boolean isBuffered = false;
                for (int i = 0; i < 1000 && !isBuffered; i++) {
                    if (Net.peek(serverFd, buf, 1) == 1) {
                        isBuffered = true;
                    } else {
                        Os.sleep(1);
                    }
                }
                Assert.assertTrue("test byte did not arrive on the server side", isBuffered);
                Assert.assertFalse("readable data without a FIN must not read as a disconnect",
                        Net.isPeerDisconnected(serverFd));
                buf = Unsafe.free(buf, 1, MemoryTag.NATIVE_DEFAULT);
                clientFd = closeFd(clientFd);
                serverFd = closeFd(serverFd);

                // Error branch (not a bare FIN): SO_LINGER 0 makes the client's close send an RST
                // instead of a FIN. The probe must still report a disconnect, via the error/hangup
                // side of the mask (Linux/Windows POLLERR|POLLHUP, macOS kqueue EV_EOF) -- the arm
                // the POLLRDHUP / EV_EOF FIN cases below never exercise. Detected on every
                // platform, so it also pins the running OS's error path (a bad/closed fd would trip
                // the fd-cache paranoia guard, so a live reset is used instead).
                clientFd = Net.socketTcp(true);
                TestUtils.assertConnect(clientFd, sockAddr);
                serverFd = Net.accept(acceptFd);
                Net.configureNonBlocking(serverFd);
                Net.configureNoLinger(clientFd);
                clientFd = closeFd(clientFd);
                awaitPeerDisconnected(serverFd);
                serverFd = closeFd(serverFd);

                // FIN behind a buffered byte: the whole point of the probe. Linux poll,
                // macOS kqueue, and Windows WSAPoll report the peer's FIN even with the
                // byte still buffered.
                clientFd = Net.socketTcp(true);
                TestUtils.assertConnect(clientFd, sockAddr);
                serverFd = Net.accept(acceptFd);
                Net.configureNonBlocking(serverFd);
                buf = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
                Unsafe.getUnsafe().putByte(buf, (byte) 'x');
                Assert.assertEquals(1, Net.send(clientFd, buf, 1));
                Net.shutdown(clientFd, Net.SHUT_WR);
                awaitPeerDisconnected(serverFd);
                buf = Unsafe.free(buf, 1, MemoryTag.NATIVE_DEFAULT);
                clientFd = closeFd(clientFd);
                serverFd = closeFd(serverFd);

                // Bare FIN, empty buffer: detected on every platform.
                clientFd = Net.socketTcp(true);
                TestUtils.assertConnect(clientFd, sockAddr);
                serverFd = Net.accept(acceptFd);
                Net.configureNonBlocking(serverFd);
                Net.shutdown(clientFd, Net.SHUT_WR);
                awaitPeerDisconnected(serverFd);
                clientFd = closeFd(clientFd);
                serverFd = closeFd(serverFd);
            } finally {
                if (buf != 0) {
                    Unsafe.free(buf, 1, MemoryTag.NATIVE_DEFAULT);
                }
                closeFd(clientFd);
                closeFd(serverFd);
                if (sockAddr != 0) {
                    Net.freeSockAddr(sockAddr);
                }
                Net.close(acceptFd);
            }
        });
    }

    @Test
    public void testLeakyAddrInfo() throws Exception {
        NetworkFacade nf = NetworkFacadeImpl.INSTANCE;
        boolean leakDetected = false;
        long[] addrInfo = new long[1];
        try {
            TestUtils.assertMemoryLeak(() -> addrInfo[0] = nf.getAddrInfo("localhost", 443));
        } catch (AssertionError e) {
            if (e.getMessage().contains("AddrInfo allocation count")) {
                leakDetected = true;
            }
        } finally {
            long ptr = addrInfo[0];
            if (ptr == -1) {
                fail("localhost could not be resolved. Something is wrong.");
            }
            nf.freeAddrInfo(ptr);
            if (!leakDetected) {
                fail("AddrInfo leak should have been detected");
            }
        }
    }

    @Test
    public void testLeakySockAddr() throws Exception {
        NetworkFacade nf = NetworkFacadeImpl.INSTANCE;
        boolean leakDetected = false;
        long[] sockAddr = new long[1];
        try {
            TestUtils.assertMemoryLeak(() -> sockAddr[0] = nf.sockaddr("127.0.0.1", 443));
        } catch (AssertionError e) {
            if (e.getMessage().contains("SockAddr allocation count")) {
                leakDetected = true;
            }
        } finally {
            long ptr = sockAddr[0];
            if (ptr == 0) {
                fail("SockAddr could no be allocated. Something is wrong.");
            }
            nf.freeSockAddr(ptr);
            if (!leakDetected) {
                fail("SockAddr leak should have been detected");
            }
        }
    }

    @Test
    @Ignore
    public void testMulticast() {
        long fd = Net.socketUdp();
        System.out.println(fd);
        bindSocket(fd);
        System.out.println(Net.setMulticastInterface(fd, Net.parseIPv4("192.168.1.156")));
        System.out.println(Net.setMulticastLoop(fd, true));
        System.out.println(Net.setMulticastTtl(fd, 1));
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
        try (Path msgSink = new Path().of(msg)) {
            int msgLen = msgSink.size() + 1;

            long acceptFd = Net.socketTcp(true);
            Assert.assertTrue(acceptFd > 0);
            int port = assertCanBind(acceptFd);
            Net.listen(acceptFd, 1024);

            long clientFd = Net.socketTcp(true);
            long sockAddr = Net.sockaddr("127.0.0.1", port);
            TestUtils.assertConnect(clientFd, sockAddr);
            Assert.assertEquals(msgLen, Net.send(clientFd, msgSink.$().ptr(), msgLen));
            Net.close(clientFd);
            Net.freeSockAddr(sockAddr);

            long serverFd = Net.accept(acceptFd);
            long serverBuf = Unsafe.malloc(msgLen, MemoryTag.NATIVE_IO_DISPATCHER_RSS);
            Assert.assertEquals(msgLen, Net.peek(serverFd, serverBuf, msgLen));
            Utf8s.utf8ToUtf16Z(serverBuf, sink);
            TestUtils.assertEquals(msg, sink);
            Assert.assertEquals(msgLen, Net.recv(serverFd, serverBuf, msgLen));
            sink.clear();
            Utf8s.utf8ToUtf16Z(serverBuf, sink);
            TestUtils.assertEquals(msg, sink);
            Unsafe.free(serverBuf, msgLen, MemoryTag.NATIVE_IO_DISPATCHER_RSS);
            Net.close(serverFd);

            Net.close(acceptFd);
        }
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
                long clientFd = Net.accept(fd);
                Net.appendIP4(sink, Net.getPeerIP(clientFd));
                ipCollectedLatch.countDown();
                Net.configureNoLinger(clientFd);
                while (!Net.isDead(clientFd)) {
                    Os.pause();
                }
                Net.close(clientFd);
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
        if (Os.isLinux()) {
            Assert.assertEquals(4608, Net.getSndBuf(clientFd));
        } else {
            Assert.assertEquals(256, Net.getSndBuf(clientFd));
        }

        Assert.assertEquals(0, Net.setRcvBuf(clientFd, 512));
        if (Os.isLinux()) {
            Assert.assertEquals(2304, Net.getRcvBuf(clientFd));
        } else {
            int rcvBuf = Net.getRcvBuf(clientFd);
            if (Os.type == Os.DARWIN) {
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

    private void awaitPeerDisconnected(long fd) {
        for (int i = 0; i < 1000; i++) {
            if (Net.isPeerDisconnected(fd)) {
                return;
            }
            Os.sleep(5);
        }
        Assert.fail("peer disconnect was not detected");
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
        long sockAddr = Net.sockaddr("127.0.0.1", port);
        // A TCP socket can be connect()-ed only once. When a transient error
        // such as ephemeral port exhaustion trips an attempt, the same fd can't
        // be reused for a second connect(), so open a fresh socket on each retry
        // to let the loop actually recover.
        long clientFd = -1;
        long sockFd = -1;
        for (int i = 0; i < 2000; i++) {
            clientFd = Net.socketTcp(true);
            Net.configureNoLinger(clientFd);
            sockFd = Net.connect(clientFd, sockAddr);
            if (sockFd == 0) {
                break;
            }
            Net.close(clientFd);
            clientFd = -1;
            Os.sleep(5);
        }
        Assert.assertEquals(0, sockFd);
        Assert.assertTrue(haltLatch.await(10, TimeUnit.SECONDS));
        Net.close(clientFd);
        Net.close(fd);
        Net.freeSockAddr(sockAddr);

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

    private long closeFd(long fd) {
        if (fd > 0) {
            Net.close(fd);
        }
        return -1;
    }
}
