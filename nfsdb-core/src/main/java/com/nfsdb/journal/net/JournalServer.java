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

package com.nfsdb.journal.net;

import com.nfsdb.journal.Journal;
import com.nfsdb.journal.JournalKey;
import com.nfsdb.journal.JournalWriter;
import com.nfsdb.journal.concurrent.NamedDaemonThreadFactory;
import com.nfsdb.journal.exceptions.JournalDisconnectedChannelException;
import com.nfsdb.journal.exceptions.JournalNetworkException;
import com.nfsdb.journal.factory.JournalReaderFactory;
import com.nfsdb.journal.logging.Logger;
import com.nfsdb.journal.net.bridge.JournalEventBridge;
import com.nfsdb.journal.net.config.ServerConfig;
import com.nfsdb.journal.net.config.SslConfig;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class JournalServer {

    public static final int JOURNAL_KEY_NOT_FOUND = -1;
    private static final Logger LOGGER = Logger.getLogger(JournalServer.class);
    private static final ThreadFactory AGENT_THREAD_FACTORY = new NamedDaemonThreadFactory("journal-agent", true);
    private final List<JournalWriter> writers = new ArrayList<>();
    private final JournalReaderFactory factory;
    private final JournalEventBridge bridge;
    private final ServerConfig config;
    private final ExecutorService service;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ArrayList<SocketChannelHolder> channels = new ArrayList<>();
    private final JournalServerAddressMulticast multicast;
    private ServerSocketChannel serverSocketChannel;

    public JournalServer(JournalReaderFactory factory) {
        this(new ServerConfig(), factory);
    }

    public JournalServer(ServerConfig config, JournalReaderFactory factory) {
        this.config = config;
        this.factory = factory;
        this.service = Executors.newCachedThreadPool(AGENT_THREAD_FACTORY);
        this.bridge = new JournalEventBridge(config.getHeartbeatFrequency(), TimeUnit.MILLISECONDS, config.getEventBufferSize());
        this.multicast = new JournalServerAddressMulticast(config);
    }

    private static void closeChannel(SocketChannelHolder holder, boolean force) {
        if (holder != null) {
            try {
                if (holder.socketAddress != null) {
                    if (force) {
                        LOGGER.info("Client forced out: %s", holder.socketAddress);
                    } else {
                        LOGGER.info("Client disconnected: %s", holder.socketAddress);
                    }
                }
                holder.byteChannel.close();
            } catch (IOException e) {
                LOGGER.error("Cannot close channel: %s", holder.byteChannel);
            }
        }
    }

    public void publish(JournalWriter journal) {
        writers.add(journal);
    }

    public JournalReaderFactory getFactory() {
        return factory;
    }

    public JournalEventBridge getBridge() {
        return bridge;
    }

    public void start() throws JournalNetworkException {
        for (int i = 0; i < writers.size(); i++) {
            JournalEventPublisher publisher = new JournalEventPublisher(i, bridge);
            JournalWriter w = writers.get(i);
            w.setTxListener(publisher);
            w.setTxAsyncListener(publisher);

        }
        serverSocketChannel = config.openServerSocketChannel();
        InetSocketAddress address = config.getSocketAddress();
        if (address.getAddress().isAnyLocalAddress()) {
            LOGGER.warn("Server is bound to *any local address*. Multicast is DISABLED");
        } else {
            multicast.start();
        }
        bridge.start();
        running.set(true);
        service.execute(new Acceptor());
    }

    public void halt() {
        service.shutdown();
        running.set(false);
        for (JournalWriter journal : writers) {
            journal.setTxListener(null);
        }
        bridge.halt();
        multicast.halt();

        try {
            closeChannels();
            serverSocketChannel.close();
        } catch (IOException e) {
            LOGGER.debug(e);
        }

        try {
            service.awaitTermination(30, TimeUnit.SECONDS);
            LOGGER.info("Server is shutdown");
        } catch (InterruptedException e) {
            LOGGER.info("Server is shutdown, but some connections are still lingering.");
        }
    }

    public boolean isRunning() {
        return running.get();
    }

    public synchronized int getConnectedClients() {
        return channels.size();
    }

    int getWriterIndex(JournalKey key) {
        for (int i = 0; i < writers.size(); i++) {
            Journal journal = writers.get(i);
            JournalKey jk = journal.getKey();
            if (jk.getModelClassName().equals(key.getModelClassName()) && (
                    (jk.getLocation() == null && key.getLocation() == null)
                            || (jk.getLocation() != null && jk.getLocation().equals(key.getLocation())))) {
                return i;
            }
        }
        return JOURNAL_KEY_NOT_FOUND;
    }

    int getMaxWriterIndex() {
        return writers.size() - 1;
    }

    private synchronized void addChannel(SocketChannelHolder holder) {
        channels.add(holder);
    }

    private synchronized void removeChannel(SocketChannelHolder holder) {
        channels.remove(holder);
        closeChannel(holder, false);
    }

    private synchronized void closeChannels() {
        for (SocketChannelHolder h : channels) {
            closeChannel(h, true);
        }
    }

    private class Acceptor implements Runnable {
        @Override
        public void run() {
            try {
                while (true) {
                    if (!running.get()) {
                        break;
                    }
                    SocketChannel channel = serverSocketChannel.accept();
                    if (channel != null) {
                        SocketChannelHolder holder;

                        SslConfig sslConfig = config.getSslConfig();
                        channel.socket().setSoTimeout(config.getSoTimeout());

                        if (sslConfig.isSecure()) {
                            holder = new SocketChannelHolder(new SSLByteChannel(sslConfig.getSslContext(), channel, false), channel.getRemoteAddress());
                        } else {
                            holder = new SocketChannelHolder(channel, channel.getRemoteAddress());
                        }
                        addChannel(holder);
                        service.submit(new Handler(holder));
                        LOGGER.info("Connected: %s", holder.socketAddress);
                    }
                }
            } catch (IOException | JournalNetworkException e) {
                if (running.get()) {
                    LOGGER.error("Acceptor dying", e);
                }
            }
            LOGGER.debug("Acceptor shutdown");
        }
    }

    class Handler implements Runnable {

        private final JournalServerAgent agent;
        private final SocketChannelHolder holder;

        Handler(SocketChannelHolder holder) {
            this.holder = holder;
            this.agent = new JournalServerAgent(JournalServer.this, holder.socketAddress);
        }

        @Override
        public void run() {
            try {
                while (true) {
                    if (!running.get()) {
                        break;
                    }
                    try {
                        agent.process(holder.byteChannel);
                    } catch (JournalDisconnectedChannelException e) {
                        break;
                    } catch (JournalNetworkException e) {
                        if (running.get()) {
                            LOGGER.info("Client died: " + holder.socketAddress);
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug(e);
                            } else {
                                LOGGER.info(e.getMessage());
                            }
                        }
                        break;
                    } catch (Throwable e) {
                        LOGGER.error("Unhandled exception in server process", e);
                        if (e instanceof Error) {
                            throw e;
                        }
                        break;
                    }
                }
            } finally {
                agent.close();
                removeChannel(holder);
            }
        }
    }
}
