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

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class NetTest {
    @Test
    public void testNoLinger() throws InterruptedException {
        bindAcceptConnectClose();
        bindAcceptConnectClose();
    }

    private void bindAcceptConnectClose() throws InterruptedException {
        int port = 9992;
        long fd = Net.socketTcp(true);
        Assert.assertTrue(fd > 0);
        Assert.assertTrue(Net.bindTcp(fd, 0, port));
        Net.listen(fd, 1024);

        CountDownLatch haltLatch = new CountDownLatch(1);

        new Thread(() -> {
            long clientfd = Net.accept(fd);
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
    }
}