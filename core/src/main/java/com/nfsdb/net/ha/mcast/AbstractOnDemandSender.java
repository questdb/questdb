/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.net.ha.mcast;

import com.nfsdb.ex.JournalNetworkException;
import com.nfsdb.ex.JournalRuntimeException;
import com.nfsdb.log.Log;
import com.nfsdb.log.LogFactory;
import com.nfsdb.misc.ByteBuffers;
import com.nfsdb.net.ha.config.DatagramChannelWrapper;
import com.nfsdb.net.ha.config.ServerConfig;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;

@SuppressFBWarnings({"EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS"})
public abstract class AbstractOnDemandSender {

    private static final Log LOG = LogFactory.getLog(AbstractOnDemandSender.class);
    final int instance;
    private final ServerConfig serverConfig;
    private final int inMessageCode;
    private final int outMessageCode;
    private final String threadName;
    private Selector selector;
    private volatile boolean running = false;
    private volatile boolean selecting = false;
    private CountDownLatch latch;

    AbstractOnDemandSender(ServerConfig serverConfig, int inMessageCode, int outMessageCode, int instance) {
        this.serverConfig = serverConfig;
        this.inMessageCode = inMessageCode;
        this.outMessageCode = outMessageCode;
        this.threadName = "nfsdb-mcast-sender-" + instance;
        this.instance = instance;
    }

    @SuppressFBWarnings({"MDM_THREAD_YIELD"})
    public void halt() {
        if (running) {
            while (!selecting) {
                Thread.yield();
            }
            selector.wakeup();
            running = false;
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new JournalRuntimeException(e);
            }
        }
    }

    public void start() {
        if (!running) {
            running = true;
            latch = new CountDownLatch(1);
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    start0();
                }
            });
            thread.setName(threadName);
            thread.start();
        }
    }

    protected abstract void prepareBuffer(ByteBuffer buf) throws JournalNetworkException;

    private void start0() {
        try {
            try (DatagramChannelWrapper dcw = serverConfig.openDatagramChannel(instance)) {
                DatagramChannel dc = dcw.getChannel();
                LOG.info().$("Sending to ").$(dcw.getGroup()).$(" on [").$(dc.getOption(StandardSocketOptions.IP_MULTICAST_IF).getName()).$(']').$();

                selector = Selector.open();
                selecting = true;
                dc.configureBlocking(false);
                dc.register(selector, SelectionKey.OP_READ);
                ByteBuffer buf = ByteBuffer.allocateDirect(4096);
                try {
                    while (running) {
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
                                        LOG.debug().$("Sending server information [").$(inMessageCode).$("] to [").$(sa).$(']').$();
                                        buf.clear();
                                        buf.putInt(outMessageCode);
                                        prepareBuffer(buf);
                                        dc.send(buf, dcw.getGroup());
                                    }
                                }
                            }
                        }
                    }
                } finally {
                    ByteBuffers.release(buf);
                }
            }
        } catch (Throwable e) {
            LOG.error().$("Multicast sender crashed").$(e).$();
        } finally {
            latch.countDown();
        }
    }
}
