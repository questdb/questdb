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

package com.nfsdb.ha.mcast;

import com.nfsdb.exceptions.JournalNetworkException;
import com.nfsdb.ha.config.ClientConfig;
import com.nfsdb.ha.config.DatagramChannelWrapper;
import com.nfsdb.logging.Logger;
import com.nfsdb.utils.ByteBuffers;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public abstract class AbstractOnDemandPoller<T> {
    private static final Logger LOGGER = Logger.getLogger(AbstractOnDemandPoller.class);
    private final ClientConfig networkConfig;
    private final int inMessageCode;
    private final int outMessageCode;

    AbstractOnDemandPoller(ClientConfig networkConfig, int inMessageCode, int outMessageCode) {
        this.networkConfig = networkConfig;
        this.inMessageCode = inMessageCode;
        this.outMessageCode = outMessageCode;
    }

    public T poll(int retryCount, long timeout, TimeUnit timeUnit) throws JournalNetworkException {
        try (DatagramChannelWrapper dcw = networkConfig.openDatagramChannel()) {
            DatagramChannel dc = dcw.getChannel();
            LOGGER.info("Polling on %s [%s]", dcw.getGroup(), dc.getOption(StandardSocketOptions.IP_MULTICAST_IF).getName());

            Selector selector = Selector.open();
            dc.configureBlocking(false);
            dc.register(selector, SelectionKey.OP_READ);
            // print out each datagram that we receive
            ByteBuffer buf = ByteBuffer.allocateDirect(4096);
            try {
                int count = retryCount;
                InetSocketAddress sa = null;
                while (count > 0 && (sa = poll0(dc, dcw.getGroup(), selector, buf, timeUnit.toMillis(timeout))) == null) {
                    buf.clear();
                    count--;
                }

                if (count == 0) {
                    throw new JournalNetworkException("Cannot find NFSdb servers on network");
                }

                return transform(buf, sa);
            } finally {
                ByteBuffers.release(buf);
            }
        } catch (IOException e) {
            throw new JournalNetworkException(e);
        }
    }

    private InetSocketAddress poll0(DatagramChannel dc, SocketAddress group, Selector selector, ByteBuffer buf, long timeoutMillis) throws IOException {
        while (true) {
            buf.putInt(outMessageCode);
            buf.flip();
            dc.send(buf, group);

            int count = 2;
            while (count-- > 0) {
                int updated = selector.select(timeoutMillis);
                if (updated == 0) {
                    return null;
                }
                if (updated > 0) {
                    Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
                    while (iter.hasNext()) {
                        SelectionKey sk = iter.next();
                        iter.remove();
                        DatagramChannel ch = (DatagramChannel) sk.channel();
                        buf.clear();
                        InetSocketAddress sa = (InetSocketAddress) ch.receive(buf);
                        if (sa != null) {
                            buf.flip();
                            if (buf.remaining() >= 4 && inMessageCode == buf.getInt()) {
                                LOGGER.info("Receiving server information from: " + sa);
                                return sa;
                            }
                        }
                    }
                }
            }
        }
    }

    protected abstract T transform(ByteBuffer buf, InetSocketAddress sa);
}
