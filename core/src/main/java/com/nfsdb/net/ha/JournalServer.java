/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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

package com.nfsdb.net.ha;

import com.nfsdb.JournalKey;
import com.nfsdb.JournalWriter;
import com.nfsdb.collections.ObjIntHashMap;
import com.nfsdb.exceptions.JournalDisconnectedChannelException;
import com.nfsdb.exceptions.JournalNetworkException;
import com.nfsdb.factory.JournalReaderFactory;
import com.nfsdb.logging.Logger;
import com.nfsdb.misc.NamedDaemonThreadFactory;
import com.nfsdb.net.SecureSocketChannel;
import com.nfsdb.net.ha.auth.AuthorizationHandler;
import com.nfsdb.net.ha.bridge.JournalEventBridge;
import com.nfsdb.net.ha.config.ServerConfig;
import com.nfsdb.net.ha.config.ServerNode;
import com.nfsdb.net.ha.mcast.OnDemandAddressSender;
import com.nfsdb.net.ha.model.Command;
import com.nfsdb.net.ha.model.IndexedJournalKey;
import com.nfsdb.net.ha.protocol.CommandProducer;
import com.nfsdb.net.ha.protocol.commands.IntResponseConsumer;
import com.nfsdb.net.ha.protocol.commands.IntResponseProducer;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.ByteChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class JournalServer {

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
    private final int uid;
    private final IntResponseConsumer intResponseConsumer = new IntResponseConsumer();
    private final IntResponseProducer intResponseProducer = new IntResponseProducer();
    private ServerSocketChannel serverSocketChannel;
    private boolean leader = false;
    private boolean participant = false;
    private boolean passiveNotified = false;
    private boolean activeNotified = false;
    private ClusterStatusListener clusterStatusListener;

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
        this.uid = instance;
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

    public void halt(long timeout, TimeUnit unit) {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        LOGGER.info("Stopping agent services %d", uid);
        service.shutdown();

        LOGGER.info("Stopping acceptor");
        try {
            serverSocketChannel.close();
        } catch (IOException e) {
            LOGGER.debug("Error closing socket", e);
        }


        if (timeout > 0) {
            try {
                LOGGER.info("Waiting for %s agent services to complete data exchange on %s", service.getActiveCount(), uid);
                service.awaitTermination(timeout, unit);
            } catch (InterruptedException e) {
                LOGGER.debug("Interrupted wait", e);
            }
        }

        if (addressSender != null) {
            LOGGER.info("Stopping mcast sender on %d", uid);
            addressSender.halt();
        }

        LOGGER.info("Closing channels on %d", uid);
        closeChannels();

        try {
            if (timeout > 0) {
                LOGGER.info("Waiting for %s  agent services to stop on %s", service.getActiveCount(), uid);
                service.awaitTermination(timeout, unit);
            }
            LOGGER.info("Server %d is shutdown", uid);
        } catch (InterruptedException e) {
            LOGGER.info("Server %d is shutdown, but some connections are still lingering.", uid);
        }

    }

    public void halt() {
        halt(30, TimeUnit.SECONDS);
    }

    public synchronized boolean isLeader() {
        return leader;
    }

    public boolean isRunning() {
        return running.get();
    }

    public synchronized void joinCluster(ClusterStatusListener clusterStatusListener) {
        if (isRunning()) {
            this.passiveNotified = false;
            this.clusterStatusListener = clusterStatusListener;
            fwdElectionMessage(ElectionMessageReason.R1, uid, Command.ELECTION, 0);
        }
    }

    public void publish(JournalWriter journal) {
        writers.put(journal, writerIdGenerator.getAndIncrement());
    }

    public void start() throws JournalNetworkException {
        for (ObjIntHashMap.Entry<JournalWriter> e : writers) {
            JournalEventPublisher publisher = new JournalEventPublisher(e.value, bridge);
            e.key.setTxListener(publisher);
        }

        serverSocketChannel = config.openServerSocketChannel(uid);
        if (config.isMultiCastEnabled()) {
            addressSender.start();
        }
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
                        LOGGER.info("Server node %d: Client forced out: %s", uid, holder.socketAddress);
                    } else {
                        LOGGER.info("Server node %d: Client disconnected: %s", uid, holder.socketAddress);
                    }
                }
                holder.byteChannel.close();

            } catch (IOException e) {
                LOGGER.error("Server node %d: Cannot close channel [%s]: %s", uid, holder.byteChannel, e.getMessage());
            }
        }
    }

    private void closeChannels() {
        for (SocketChannelHolder h : channels) {
            closeChannel(h, true);
        }
        channels.clear();
    }

    private synchronized void fwdElectionMessage(ElectionMessageReason reason, int uid, Command command, int count) {
        this.participant = true;
        service.submit(new ElectionForwarder(reason, uid, command, count));
    }

    @SuppressWarnings("unchecked")
    IndexedJournalKey getWriterIndex0(JournalKey key) {
        for (ObjIntHashMap.Entry<JournalWriter> e : writers.immutableIterator()) {
            JournalKey jk = e.key.getKey();
            if (jk.derivedLocation().equals(key.derivedLocation())) {
                return new IndexedJournalKey(e.value, new JournalKey(jk.getId(), jk.getModelClass(), jk.getLocation(), jk.getRecordHint()));
            }
        }
        return null;
    }

    synchronized void handleElectedMessage(ByteChannel channel) throws JournalNetworkException {
        int theirUuid = intResponseConsumer.getValue(channel);
        int hops = intResponseConsumer.getValue(channel);
        int ourUuid = uid;

        if (isRunning()) {
            if (theirUuid != ourUuid) {
                participant = false;
                if (hops < config.getNodeCount() + 2) {

                    if (leader && theirUuid > ourUuid) {
                        leader = false;
                    }

                    fwdElectionMessage(ElectionMessageReason.R2, theirUuid, Command.ELECTED, hops + 1);
                    if (!passiveNotified && clusterStatusListener != null) {
                        clusterStatusListener.goPassive(config.getNodeByUID(theirUuid));
                        passiveNotified = true;
                    }
                } else {
                    fwdElectionMessage(ElectionMessageReason.R3, ourUuid, Command.ELECTION, 0);
                }
            } else if (leader) {
                if (!activeNotified && clusterStatusListener != null) {
                    LOGGER.info("%d is THE LEADER", ourUuid);
                    clusterStatusListener.goActive();
                    activeNotified = true;
                }
            }
            intResponseProducer.write(channel, 0xfc);
        } else {
            intResponseProducer.write(channel, 0xfd);
        }
    }

    synchronized void handleElectionMessage(ByteChannel channel) throws JournalNetworkException {
        int theirUid = intResponseConsumer.getValue(channel);
        int hops = intResponseConsumer.getValue(channel);
        int ourUid = uid;

        if (isRunning()) {
            if (leader && theirUid != ourUid) {
                // if it is ELECTION message and we are the leader
                // cry foul and attempt to curb the thread by sending ELECTED message wit our uid
                LOGGER.info("%d is insisting on leadership", ourUid);
                fwdElectionMessage(ElectionMessageReason.R4, ourUid, Command.ELECTED, 0);
            } else if (theirUid > ourUid) {
                // if theirUid is greater than ours - forward message on
                // with exception where hop count is greater than node count
                // this can happen when max uid node send election message and disappears from network
                // before this message is stopped.
                if (hops < config.getNodeCount() + 2) {
                    fwdElectionMessage(ElectionMessageReason.R5, theirUid, Command.ELECTION, hops + 1);
                } else {
                    // when infinite loop is detected, start voting exisitng node - "us"
                    fwdElectionMessage(ElectionMessageReason.R6, ourUid, Command.ELECTION, 0);
                }
            } else if (theirUid < ourUid && !participant) {
                // if thier Uid is smaller than ours - send ours and become participant
                fwdElectionMessage(ElectionMessageReason.R7, ourUid, Command.ELECTION, 0);
            } else if (!leader && theirUid == ourUid) {
                // our message came back to us, announce our uid as the LEADER
                leader = true;
                participant = false;
                fwdElectionMessage(ElectionMessageReason.R8, ourUid, Command.ELECTED, 0);
            }
            intResponseProducer.write(channel, 0xfc);
        } else {
            intResponseProducer.write(channel, 0xfd);
        }
    }

    @SuppressFBWarnings({"PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS"})
    private SocketChannel openSocketChannel0(ServerNode node, long timeout) throws IOException {
        InetSocketAddress address = new InetSocketAddress(node.getHostname(), node.getPort());
        SocketChannel channel = SocketChannel.open()
                .setOption(StandardSocketOptions.TCP_NODELAY, Boolean.FALSE)
                .setOption(StandardSocketOptions.SO_SNDBUF, 32 * 1024)
                .setOption(StandardSocketOptions.SO_RCVBUF, 32 * 1024);

        channel.configureBlocking(false);
        try {
            channel.connect(address);
            long t = System.currentTimeMillis();

            while (!channel.finishConnect()) {
                LockSupport.parkNanos(500000L);
                if (System.currentTimeMillis() - t > timeout) {
                    throw new IOException("Connection timeout");
                }
            }

            channel.configureBlocking(true);

            LOGGER.info("Connected to %s [%s]", node, channel.getLocalAddress());
            return channel;
        } catch (IOException e) {
            channel.close();
            throw e;
        }
    }

    private void removeChannel(SocketChannelHolder holder) {
        if (channels.remove(holder)) {
            closeChannel(holder, false);
        }
    }

    private enum ElectionMessageReason {
        R1, R2, R3, R4, R5, R6, R7, R8
    }

    private class ElectionForwarder implements Runnable {
        private final CommandProducer commandProducer = new CommandProducer();
        private final IntResponseProducer intResponseProducer = new IntResponseProducer();
        private final IntResponseConsumer intResponseConsumer = new IntResponseConsumer();
        private final Command command;
        private final int uid;
        private final int count;
        private final ElectionMessageReason reason;

        public ElectionForwarder(ElectionMessageReason reason, int uid, Command command, int count) {
            this.reason = reason;
            this.command = command;
            this.uid = uid;
            this.count = count;
        }

        @Override
        public void run() {
            int peer = config.getNodePosition(JournalServer.this.uid);
            while (true) {
                if (++peer == config.getNodeCount()) {
                    peer = 0;
                }

                ServerNode node = config.getNodeByPosition(peer);
                try (SocketChannel channel = openSocketChannel0(node, 2000)) {
                    commandProducer.write(channel, command);
                    intResponseProducer.write(channel, uid);
                    intResponseProducer.write(channel, count);
                    LOGGER.info("%s> %s [%d]{%d} %d -> %d", reason, command, uid, count, JournalServer.this.uid, node.getId());
                    if (intResponseConsumer.getValue(channel) == 0xfc) {
                        break;
                    } else {
                        LOGGER.info("Node %d is shutting down", peer);
                    }
                } catch (Exception e) {
                    LOGGER.info("Dead node %d: %s", peer, e.getMessage());
                }
            }
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
                                config.getSslConfig().isSecure() ? new SecureSocketChannel(channel, config.getSslConfig()) : channel
                                , channel.getRemoteAddress()
                        );
                        addChannel(holder);
                        try {
                            service.submit(new Handler(holder));
                            LOGGER.info("Server node %d: Connected %s", uid, holder.socketAddress);
                        } catch (RejectedExecutionException e) {
                            LOGGER.info("Node %d ignoring connection from %s. Server is shutting down.", uid, holder.socketAddress);
                        }
                    }
                }
            } catch (Exception e) {
                if (running.get()) {
                    LOGGER.error("Acceptor dying", e);
                }
            }
            LOGGER.info("Acceptor shutdown on %s", uid);
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
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug("Client died", e);
                            } else {
                                LOGGER.info("Server node %d: Client died %s: %s", uid, holder.socketAddress, e.getMessage());
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
