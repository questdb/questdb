/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.std;

import com.questdb.std.str.StringSink;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class NetTest {
    @Test
    public void testNoLinger() throws InterruptedException {
        bindAcceptConnectClose();
        bindAcceptConnectClose();
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
        Thread.sleep(500);
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
            Assert.assertEquals(1, Net.getTcpNoDelay(fd));
        } finally {
            Net.close(fd);
        }
    }

    private void bindAcceptConnectClose() throws InterruptedException {
        int port = 9992;
        long fd = Net.socketTcp(true);
        Assert.assertTrue(fd > 0);
        Assert.assertTrue(Net.bindTcp(fd, 0, port));
        Net.listen(fd, 1024);

        // make sure peerIp in correct byte order
        StringSink sink = new StringSink();

        CountDownLatch haltLatch = new CountDownLatch(1);

        new Thread(() -> {
            long clientfd = Net.accept(fd);
            Net.appendIP4(sink, Net.getPeerIP(clientfd));
            Net.configureNoLinger(clientfd);
            Net.close(clientfd);
            haltLatch.countDown();
        }).start();


        long clientFd = Net.socketTcp(true);
        long sockAddr = Net.sockaddr("127.0.0.1", port);
        Assert.assertEquals(0, Net.connect(clientFd, sockAddr));
        haltLatch.await(10, TimeUnit.SECONDS);
        Net.close(clientFd);
        Net.close(fd);

        TestUtils.assertEquals("127.0.0.1", sink);
    }
}