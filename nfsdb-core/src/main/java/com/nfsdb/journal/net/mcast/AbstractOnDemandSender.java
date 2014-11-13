/*
 * Copyright (c) 2014. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.journal.net.mcast;

import com.nfsdb.journal.exceptions.JournalNetworkException;
import com.nfsdb.journal.exceptions.JournalRuntimeException;
import com.nfsdb.journal.logging.Logger;
import com.nfsdb.journal.net.config.NetworkConfig;

import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;

public abstract class AbstractOnDemandSender {

    private static final Logger LOGGER = Logger.getLogger(AbstractOnDemandSender.class);

    private final NetworkConfig networkConfig;
    private final InetSocketAddress socketAddress;
    private final int inMessageCode;
    private final int outMessageCode;
    private Selector selector;
    private volatile boolean running = false;
    private CountDownLatch latch;

    public AbstractOnDemandSender(NetworkConfig networkConfig, int inMessageCode, int outMessageCode) throws JournalNetworkException {
        this.networkConfig = networkConfig;
        this.socketAddress = new InetSocketAddress(networkConfig.getMulticastAddress(), networkConfig.getMulticastPort());
        this.inMessageCode = inMessageCode;
        this.outMessageCode = outMessageCode;
    }

    public void start() {
        if (!running) {
            running = true;
            latch = new CountDownLatch(1);
            new Thread(new Runnable() {
                @Override
                public void run() {
                    start0();
                }
            }).start();
        }
    }

    private void start0() {
        try {
            InetAddress multicastAddress = socketAddress.getAddress();
            ProtocolFamily family = NetworkConfig.isInet6(multicastAddress) ? StandardProtocolFamily.INET6 : StandardProtocolFamily.INET;

            LOGGER.info("Sending on: " + networkConfig.getNetworkInterface());

            try (DatagramChannel dc = DatagramChannel.open(family)
                    .setOption(StandardSocketOptions.SO_REUSEADDR, true)
                    .setOption(StandardSocketOptions.IP_MULTICAST_IF, networkConfig.getNetworkInterface())
                    .bind(new InetSocketAddress(socketAddress.getPort()))) {

                dc.join(multicastAddress, networkConfig.getNetworkInterface());

                selector = Selector.open();
                dc.configureBlocking(false);
                dc.register(selector, SelectionKey.OP_READ);
                ByteBuffer buf = ByteBuffer.allocateDirect(4096);
                while (true) {
                    int updated = selector.select();
                    if (!running) {
                        break;
                    }
                    if (updated > 0) {
                        Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
                        while (iter.hasNext()) {
                            SelectionKey sk = iter.next();
                            iter.remove();
                            DatagramChannel ch = (DatagramChannel) sk.channel();
                            buf.clear();
                            SocketAddress sa = ch.receive(buf);
                            if (sa != null) {
                                buf.flip();
                                if (buf.remaining() >= 4 && inMessageCode == buf.getInt(0)) {
                                    buf.clear();
                                    buf.putInt(outMessageCode);
                                    prepareBuffer(buf);
                                    dc.send(buf, socketAddress);
                                }
                            }
                        }
                    }
                }
            }
        } catch (Throwable e) {
            LOGGER.error("Multicast sender crashed", e);
        } finally {
            latch.countDown();
        }
    }

    protected abstract void prepareBuffer(ByteBuffer buf) throws JournalNetworkException;

    public void halt() {
        if (running) {
            running = false;
            if (selector != null) {
                selector.wakeup();
            }
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new JournalRuntimeException(e);
            }
        }
    }
}
