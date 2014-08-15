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
    private final ArrayList<SocketChannel> channels = new ArrayList<>();
    private final JournalServerAddressMulticast multicast;
    private ServerSocketChannel serverSocketChannel;

    public JournalServer(ServerConfig config, JournalReaderFactory factory) {
        this.config = config;
        this.factory = factory;
        this.service = Executors.newCachedThreadPool(AGENT_THREAD_FACTORY);
        this.bridge = new JournalEventBridge(config.getHeartbeatFrequency(), TimeUnit.MILLISECONDS, config.getEventBufferSize());
        this.multicast = new JournalServerAddressMulticast(config);
    }

    public void export(JournalWriter journal) {
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

    private static void closeChannel(SocketChannel channel, boolean force) {
        if (channel != null) {
            try {
                if (channel.socket().getRemoteSocketAddress() != null) {
                    if (force) {
                        LOGGER.info("Client forced out: %s", channel.socket().getRemoteSocketAddress());
                    } else {
                        LOGGER.info("Client disconnected: %s", channel.socket().getRemoteSocketAddress());
                    }
                }
                channel.close();
            } catch (IOException e) {
                LOGGER.error("Cannot close channel: %s", channel);
            }
        }
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

    private synchronized void addChannel(SocketChannel channel) {
        channels.add(channel);
    }

    private synchronized void removeChannel(SocketChannel channel) {
        channels.remove(channel);
        closeChannel(channel, false);
    }

    private synchronized void closeChannels() {
        for (SocketChannel channel : channels) {
            closeChannel(channel, true);
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
                        addChannel(channel);
                        channel.socket().setSoTimeout(config.getSoTimeout());
                        LOGGER.info("Connected: %s", channel.getRemoteAddress());
                        service.submit(new Handler(channel));
                    }
                }
            } catch (IOException e) {
                if (running.get()) {
                    LOGGER.error("Acceptor dying", e);
                }
            }
            LOGGER.debug("Acceptor shutdown");
        }
    }

    class Handler implements Runnable {

        private final JournalServerAgent agent;
        private final SocketChannel channel;

        @Override
        public void run() {
            while (true) {
                if (!running.get()) {
                    break;
                }
                try {
                    agent.process(channel);
                } catch (JournalDisconnectedChannelException e) {
                    break;
                } catch (JournalNetworkException e) {
                    if (running.get()) {
                        LOGGER.info("Client died: " + channel.socket().getRemoteSocketAddress());
                        LOGGER.debug(e);
                    }
                    break;
                }
            }
            agent.close();
            removeChannel(channel);
        }

        Handler(SocketChannel channel) {
            this.channel = channel;
            this.agent = new JournalServerAgent(JournalServer.this, channel.socket().getRemoteSocketAddress());
        }
    }
}
