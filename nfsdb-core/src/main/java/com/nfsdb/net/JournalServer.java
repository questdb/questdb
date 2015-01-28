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

package com.nfsdb.net;

import com.nfsdb.JournalKey;
import com.nfsdb.JournalWriter;
import com.nfsdb.collections.ObjIntHashMap;
import com.nfsdb.concurrent.NamedDaemonThreadFactory;
import com.nfsdb.exceptions.ClusterLossException;
import com.nfsdb.exceptions.JournalDisconnectedChannelException;
import com.nfsdb.exceptions.JournalNetworkException;
import com.nfsdb.factory.JournalReaderFactory;
import com.nfsdb.logging.Logger;
import com.nfsdb.net.auth.AuthorizationHandler;
import com.nfsdb.net.bridge.JournalEventBridge;
import com.nfsdb.net.config.ServerConfig;
import com.nfsdb.net.mcast.OnDemandAddressSender;

import java.io.IOException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class JournalServer {

    public static final int JOURNAL_KEY_NOT_FOUND = -1;
    private static final Logger LOGGER = Logger.getLogger(JournalServer.class);
    private final AtomicInteger writerIdGenerator = new AtomicInteger(0);
    private final ObjIntHashMap<JournalWriter> writers = new ObjIntHashMap<>();
    private final JournalReaderFactory factory;
    private final JournalEventBridge bridge;
    private final ServerConfig config;
    private final ThreadPoolExecutor service;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final List<SocketChannelHolder> channels = new CopyOnWriteArrayList<>();
    private final OnDemandAddressSender addressSender;
    private final AuthorizationHandler authorizationHandler;
    private final JournalServerLogger serverLogger = new JournalServerLogger();
    private final int serverInstance;
    private final AtomicBoolean ignoreVoting = new AtomicBoolean(false);
    private ServerSocketChannel serverSocketChannel;

    public JournalServer(JournalReaderFactory factory) {
        this(new ServerConfig(), factory);
    }

    public JournalServer(JournalReaderFactory factory, AuthorizationHandler authorizationHandler) {
        this(new ServerConfig(), factory, authorizationHandler);
    }

    public JournalServer(ServerConfig config, JournalReaderFactory factory) {
        this(config, factory, null);
    }

    public JournalServer(ServerConfig config, JournalReaderFactory factory, AuthorizationHandler authorizationHandler) {
        this(config, factory, authorizationHandler, 0);
    }

    public JournalServer(ServerConfig config, JournalReaderFactory factory, AuthorizationHandler authorizationHandler, int instance) {
        this.config = config;
        this.factory = factory;
        this.service = new ThreadPoolExecutor(
                0
                , Integer.MAX_VALUE
                , 60L
                , TimeUnit.SECONDS
                , new SynchronousQueue<Runnable>()
                , new NamedDaemonThreadFactory("nfsdb-server-" + instance + "-agent", true)
        );
        this.bridge = new JournalEventBridge(config.getHeartbeatFrequency(), TimeUnit.MILLISECONDS);
        if (config.isMultiCastEnabled()) {
            this.addressSender = new OnDemandAddressSender(config, 230, 235, instance);
        } else {
            this.addressSender = null;
        }
        this.authorizationHandler = authorizationHandler;
        this.serverInstance = instance;
    }

    public JournalEventBridge getBridge() {
        return bridge;
    }

    public int getConnectedClients() {
        return channels.size();
    }

    public JournalReaderFactory getFactory() {
        return factory;
    }

    public JournalServerLogger getLogger() {
        return serverLogger;
    }

    public int getServerInstance() {
        return serverInstance;
    }

    public void halt(long timeout, TimeUnit unit) {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        LOGGER.trace("Stopping agent services");
        service.shutdown();
        for (ObjIntHashMap.Entry<JournalWriter> e : writers) {
            e.key.setTxAsyncListener(null);
        }

        LOGGER.trace("Stopping acceptor");
        try {
            serverSocketChannel.close();
        } catch (IOException e) {
            LOGGER.debug(e);
        }


        if (timeout > 0) {
            try {
                service.awaitTermination(timeout, unit);
            } catch (InterruptedException e) {
                LOGGER.debug(e);
            }
        }

        LOGGER.trace("Stopping bridge");
        bridge.halt();

        if (addressSender != null) {
            LOGGER.trace("Stopping mcast sender");
            addressSender.halt();
        }

        LOGGER.trace("Closing channels");
        closeChannels();

        LOGGER.trace("Stopping logger");
        serverLogger.halt();

        try {
            if (timeout > 0) {
                LOGGER.info("Waiting for %s  agent services to stop on %s", service.getActiveCount(), serverInstance);
                service.awaitTermination(timeout, unit);
            }
            LOGGER.info("Server %d is shutdown", serverInstance);
        } catch (InterruptedException e) {
            LOGGER.info("Server %d is shutdown, but some connections are still lingering.", serverInstance);
        }

    }

    public void halt() {
        halt(30, TimeUnit.SECONDS);
    }

    public boolean isIgnoreVoting() {
        return ignoreVoting.get();
    }

    public void setIgnoreVoting(boolean ignore) {
        ignoreVoting.set(ignore);
    }

    public boolean isRunning() {
        return running.get();
    }

    public void publish(JournalWriter journal) {
        writers.put(journal, writerIdGenerator.getAndIncrement());
    }

    public void start() throws JournalNetworkException {
        serverLogger.start();
        for (ObjIntHashMap.Entry<JournalWriter> e : writers) {
            JournalEventPublisher publisher = new JournalEventPublisher(e.value, bridge);
            e.key.setTxListener(publisher);
            e.key.setTxAsyncListener(publisher);
        }

        serverSocketChannel = config.openServerSocketChannel(serverInstance);
        if (config.isMultiCastEnabled()) {
            addressSender.start();
        }
        bridge.start();
        running.set(true);
        service.execute(new Acceptor());
    }

    private void addChannel(SocketChannelHolder holder) {
        channels.add(holder);
    }

    private void closeChannel(SocketChannelHolder holder, boolean force) {
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
                LOGGER.error("Cannot close channel [%s]: %s", holder.byteChannel, e.getMessage());
            }
        }
    }

    private void closeChannels() {
        while (channels.size() > 0) {
            closeChannel(channels.remove(0), true);
        }
    }

    int getWriterIndex(JournalKey key) {
        for (ObjIntHashMap.Entry<JournalWriter> e : writers.immutableIterator()) {
            JournalKey jk = e.key.getKey();
            if (jk.getId().equals(key.getId()) && (
                    (jk.getLocation() == null && key.getLocation() == null)
                            || (jk.getLocation() != null && jk.getLocation().equals(key.getLocation())))) {
                return e.value;
            }
        }
        return JOURNAL_KEY_NOT_FOUND;
    }

    private void removeChannel(SocketChannelHolder holder) {
        if (channels.remove(holder)) {
            closeChannel(holder, false);
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
                        SocketChannelHolder holder = new SocketChannelHolder(
                                config.getSslConfig().isSecure() ? new SecureByteChannel(channel, config.getSslConfig()) : channel
                                , channel.getRemoteAddress()
                        );
                        addChannel(holder);
                        try {
                            service.submit(new Handler(holder));
                            LOGGER.info("Connected: %s", holder.socketAddress);
                        } catch (RejectedExecutionException e) {
                            LOGGER.info("Ignoring connection from %s. Server is shutting down.", holder.socketAddress);
                        }
                    }
                }
            } catch (IOException | JournalNetworkException e) {
                if (running.get()) {
                    LOGGER.error("Acceptor dying", e);
                }
            }
            LOGGER.info("Acceptor shutdown on %s", serverInstance);
        }
    }

    class Handler implements Runnable {

        private final JournalServerAgent agent;
        private final SocketChannelHolder holder;

        Handler(SocketChannelHolder holder) {
            this.holder = holder;
            this.agent = new JournalServerAgent(JournalServer.this, holder.socketAddress, authorizationHandler);
        }

        @Override
        public void run() {
            boolean haltServer = false;
            try {
                while (true) {
                    if (!running.get()) {
                        break;
                    }
                    try {
                        agent.process(holder.byteChannel);
                    } catch (JournalDisconnectedChannelException e) {
                        break;
                    } catch (ClusterLossException e) {
                        haltServer = true;
                        LOGGER.info("Server node %s lost cluster vote to %s", serverInstance, e.getInstance());
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

            if (haltServer) {
                halt(0, TimeUnit.SECONDS);
            }
        }
    }
}
