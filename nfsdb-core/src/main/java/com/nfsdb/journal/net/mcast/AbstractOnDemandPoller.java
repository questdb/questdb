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
import com.nfsdb.journal.net.config.NetworkConfig;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public abstract class AbstractOnDemandPoller<T> {
    private final NetworkConfig networkConfig;
    private final InetSocketAddress socketAddress;
    private final int inMessageCode;
    private final int outMessageCode;

    public AbstractOnDemandPoller(NetworkConfig networkConfig, int inMessageCode, int outMessageCode) throws JournalNetworkException {
        this.networkConfig = networkConfig;
        this.inMessageCode = inMessageCode;
        this.outMessageCode = outMessageCode;
        this.socketAddress = new InetSocketAddress(networkConfig.getMulticastAddress(), networkConfig.getMulticastPort());
    }

    public T poll(int retryCount, long timeout, TimeUnit timeUnit) throws JournalNetworkException {
        return transform(poll1(retryCount, timeout, timeUnit));
    }

    protected abstract T transform(ByteBuffer buf) throws JournalNetworkException;

    private ByteBuffer poll1(int retryCount, long timeout, TimeUnit timeUnit) throws JournalNetworkException {
        ProtocolFamily family = NetworkConfig.isInet6(socketAddress.getAddress()) ? StandardProtocolFamily.INET6 : StandardProtocolFamily.INET;

        System.out.println("poller: " + socketAddress);
        System.out.println("poller: " + networkConfig.getNetworkInterface());
        try (DatagramChannel dc = DatagramChannel.open(family)
                .setOption(StandardSocketOptions.SO_REUSEADDR, true)
                .setOption(StandardSocketOptions.IP_MULTICAST_IF, networkConfig.getNetworkInterface())
                .bind(new InetSocketAddress(networkConfig.getMulticastPort()))) {

            dc.join(socketAddress.getAddress(), networkConfig.getNetworkInterface());
            Selector selector = Selector.open();
            dc.configureBlocking(false);
            dc.register(selector, SelectionKey.OP_READ);
            // print out each datagram that we receive
            ByteBuffer buf = ByteBuffer.allocateDirect(4096);

            int count = retryCount;
            while (count > 0 && !poll0(dc, selector, buf, timeUnit.toMillis(timeout))) {
                buf.clear();
                count--;
            }
            return count > 0 ? buf : null;
        } catch (IOException e) {
            throw new JournalNetworkException(e);
        }
    }

    private boolean poll0(DatagramChannel dc, Selector selector, ByteBuffer buf, long timeoutMillis) throws IOException {
        while (true) {
            buf.putInt(outMessageCode);
            buf.flip();
            dc.send(buf, socketAddress);

            int count = 2;
            while (count-- > 0) {
                int updated = selector.select(timeoutMillis);
                if (updated == 0) {
                    return false;
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
                            if (buf.remaining() >= 4 && inMessageCode == buf.getInt()) {
                                return true;
                            }
                        }
                    }
                }
            }
        }
    }
}
