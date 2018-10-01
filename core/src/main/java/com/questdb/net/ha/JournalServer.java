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

package com.questdb.net.ha;

import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.net.SecureSocketChannel;
import com.questdb.net.ha.auth.AuthorizationHandler;
import com.questdb.net.ha.bridge.JournalEventBridge;
import com.questdb.net.ha.config.ServerConfig;
import com.questdb.net.ha.config.ServerNode;
import com.questdb.net.ha.mcast.OnDemandAddressSender;
import com.questdb.net.ha.model.Command;
import com.questdb.net.ha.model.IndexedJournalKey;
import com.questdb.net.ha.protocol.CommandProducer;
import com.questdb.net.ha.protocol.commands.IntResponseConsumer;
import com.questdb.net.ha.protocol.commands.IntResponseProducer;
import com.questdb.std.NamedDaemonThreadFactory;
import com.questdb.std.ObjIntHashMap;
import com.questdb.std.ex.JournalDisconnectedChannelException;
import com.questdb.std.ex.JournalNetworkException;
import com.questdb.store.JournalKey;
import com.questdb.store.JournalWriter;
import com.questdb.store.factory.ReaderFactory;

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

    private static final Log LOG = LogFactory.getLog(JournalServer.class);
    private static final int ER_NEW_SERVER_JOINED = 1;
    private static final int ER_FORWARD_ELECTED_THEIRS = 2;
    private static final int ER_FORWARD_ELECTED_OURS = 3;
    private static final int ER_INSISTING = 4;
    private static final int ER_FORWARD_ELECTION_THEIRS = 5;
    private static final int ER_FORWARD_ELECTION_OURS = 6;
    private static final int ER_CHANGING_ELECTION_TO_OURS = 7;
    private static final int ER_ANNOUNCE_LEADER = 8;
    private final AtomicInteger writerIdGenerator = new AtomicInteger(0);
    private final ObjIntHashMap<JournalWriter> writers = new ObjIntHashMap<>();
    private final ReaderFactory factory;
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

    public JournalServer(ReaderFactory factory) {
        this(new ServerConfig(), factory);
    }

    public JournalServer(ReaderFactory factory, AuthorizationHandler authorizationHandler) {
        this(new ServerConfig(), factory, authorizationHandler);
    }

    public JournalServer(ServerConfig config, ReaderFactory factory) {
        this(config, factory, null);
    }

    public JournalServer(ServerConfig config, ReaderFactory factory, AuthorizationHandler authorizationHandler) {
        this(config, factory, authorizationHandler, 0);
    }

    public JournalServer(ServerConfig config, ReaderFactory factory, AuthorizationHandler authorizationHandler, int instance) {
        this.config = config;
        this.factory = factory;
        this.service = new ThreadPoolExecutor(
                0
                , Integer.MAX_VALUE
                , 60L
                , TimeUnit.SECONDS
                , new SynchronousQueue<>()
                , new NamedDaemonThreadFactory("questdb-server-" + instance + "-agent", true)
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

    public ReaderFactory getFactory() {
        return factory;
    }

    public void halt(long timeout, TimeUnit unit) {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        LOG.info().$("Stopping agent services ").$(uid).$();
        service.shutdown();

        LOG.info().$("Stopping acceptor").$();
        try {
            serverSocketChannel.close();
        } catch (IOException e) {
            LOG.debug().$("Error closing socket").$(e).$();
        }


        if (timeout > 0) {
            try {
                LOG.info().$("Waiting for ").$(service.getActiveCount()).$(" agent services to complete data exchange on ").$(uid).$();
                service.awaitTermination(timeout, unit);
            } catch (InterruptedException e) {
                LOG.debug().$("Interrupted wait").$(e).$();
            }
        }

        if (addressSender != null) {
            LOG.info().$("Stopping mcast sender on ").$(uid).$();
            addressSender.halt();
        }

        LOG.info().$("Closing channels on ").$(uid).$();
        closeChannels();

        try {
            if (timeout > 0) {
                LOG.info().$("Waiting for ").$(service.getActiveCount()).$(" agent services to stop on ").$(uid).$();
                service.awaitTermination(timeout, unit);
            }
            LOG.info().$("Server ").$(uid).$(" is shutdown").$();
        } catch (InterruptedException e) {
            LOG.info().$("Server ").$(uid).$(" is shutdown, but some connections are still lingering.").$();
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
            fwdElectionMessage(ER_NEW_SERVER_JOINED, uid, Command.ELECTION, 0);
        }
    }

    public void publish(JournalWriter journal) {
        writers.put(journal, writerIdGenerator.getAndIncrement());
    }

    public void start() throws JournalNetworkException {
        for (ObjIntHashMap.Entry<JournalWriter> e : writers) {
            JournalEventPublisher publisher = new JournalEventPublisher(e.value, bridge);
            e.key.setJournalListener(publisher);
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
                        LOG.info().$("Server node ").$(uid).$(": Client forced out: ").$(holder.socketAddress.toString()).$();
                    } else {
                        LOG.info().$("Server node ").$(uid).$(": Client disconnected: ").$(holder.socketAddress.toString()).$();
                    }
                }
                holder.byteChannel.close();

            } catch (IOException e) {
                LOG.error().$("Server node ").$(uid).$(": Cannot close channel [").$(holder.byteChannel).$("]: ").$(e.getMessage()).$();
            }
        }
    }

    private void closeChannels() {
        for (SocketChannelHolder h : channels) {
            closeChannel(h, true);
        }
        channels.clear();
    }

    private synchronized void fwdElectionMessage(int reason, int uid, byte command, int count) {
        this.participant = true;
        service.submit(new ElectionForwarder(reason, uid, command, count));
    }

    @SuppressWarnings("unchecked")
    IndexedJournalKey getWriterIndex0(JournalKey key) {
        for (ObjIntHashMap.Entry<JournalWriter> e : writers.immutableIterator()) {
            JournalKey jk = e.key.getMetadata().getKey();
            if (e.key.getName().equals(key.getName())) {
                return new IndexedJournalKey(e.value, new JournalKey(jk.getModelClass(), jk.getName(), jk.getPartitionBy(), jk.getRecordHint()));
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

                    fwdElectionMessage(ER_FORWARD_ELECTED_THEIRS, theirUuid, Command.ELECTED, hops + 1);
                    if (!passiveNotified && clusterStatusListener != null) {
                        clusterStatusListener.goPassive(config.getNodeByUID(theirUuid));
                        passiveNotified = true;
                    }
                } else {
                    fwdElectionMessage(ER_FORWARD_ELECTED_OURS, ourUuid, Command.ELECTION, 0);
                }
            } else if (leader && !activeNotified && clusterStatusListener != null) {
                LOG.info().$(ourUuid).$(" is THE LEADER").$();
                clusterStatusListener.goActive();
                activeNotified = true;
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
                LOG.info().$(ourUid).$(" is insisting on leadership").$();
                fwdElectionMessage(ER_INSISTING, ourUid, Command.ELECTED, 0);
            } else if (theirUid > ourUid) {
                // if theirUid is greater than ours - forward message on
                // with exception where hop count is greater than node count
                // this can happen when max uid node send election message and disappears from network
                // before this message is stopped.
                if (hops < config.getNodeCount() + 2) {
                    fwdElectionMessage(ER_FORWARD_ELECTION_THEIRS, theirUid, Command.ELECTION, hops + 1);
                } else {
                    // when infinite loop is detected, start voting exisitng node - "us"
                    fwdElectionMessage(ER_FORWARD_ELECTION_OURS, ourUid, Command.ELECTION, 0);
                }
            } else if (theirUid < ourUid && !participant) {
                // if thier Uid is smaller than ours - send ours and become participant
                fwdElectionMessage(ER_CHANGING_ELECTION_TO_OURS, ourUid, Command.ELECTION, 0);
            } else if (!leader && theirUid == ourUid) {
                // our message came back to us, announce our uid as the LEADER
                leader = true;
                participant = false;
                fwdElectionMessage(ER_ANNOUNCE_LEADER, ourUid, Command.ELECTED, 0);
            }
            intResponseProducer.write(channel, 0xfc);
        } else {
            intResponseProducer.write(channel, 0xfd);
        }
    }

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

            LOG.info().$("Connected to ").$(node).$(" [").$(channel.getLocalAddress()).$(']').$();
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

    private class ElectionForwarder implements Runnable {
        private final CommandProducer commandProducer = new CommandProducer();
        private final IntResponseProducer intResponseProducer = new IntResponseProducer();
        private final IntResponseConsumer intResponseConsumer = new IntResponseConsumer();
        private final byte command;
        private final int uid;
        private final int count;
        private final int electionReason;

        public ElectionForwarder(int electionReason, int uid, byte command, int count) {
            this.electionReason = electionReason;
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
                    LOG.info().$(electionReason).$("> ").$(command).$(" [").$(uid).$("]{").$(count).$("} ").$(JournalServer.this.uid).$(" -> ").$(node.getId()).$();
                    if (intResponseConsumer.getValue(channel) == 0xfc) {
                        break;
                    } else {
                        LOG.info().$("Node ").$(peer).$(" is shutting down").$();
                    }
                } catch (Exception e) {
                    LOG.info().$("Dead node ").$(peer).$(": ").$(e.getMessage()).$();
                }
            }
        }
    }

    private class Acceptor implements Runnable {
        @Override
        public void run() {
            try {
                while (running.get()) {
                    SocketChannel channel = serverSocketChannel.accept();
                    if (channel != null) {
                        SocketChannelHolder holder = new SocketChannelHolder(
                                config.getSslConfig().isSecure() ? new SecureSocketChannel(channel, config.getSslConfig()) : channel
                                , channel.getRemoteAddress()
                        );
                        addChannel(holder);
                        try {
                            service.submit(new Handler(holder));
                            LOG.info().$("Server node ").$(uid).$(": Connected ").$(holder.socketAddress).$();
                        } catch (RejectedExecutionException e) {
                            LOG.info().$("Node ").$(uid).$(" ignoring connection from ").$(holder.socketAddress).$(". Server is shutting down.").$();
                        }
                    }
                }
            } catch (Exception e) {
                if (running.get()) {
                    LOG.error().$("Acceptor dying").$(e).$();
                }
            }
            LOG.info().$("Acceptor shutdown on ").$(uid).$();
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
                            LOG.info().$("Server node ").$(uid).$(": Client died ").$(holder.socketAddress).$(": ").$(e.getMessage()).$();
                        }
                        break;
                    } catch (Error e) {
                        LOG.error().$("Unhandled exception in server process").$(e).$();
                        throw e;
                    } catch (Throwable e) {
                        LOG.error().$("Unhandled exception in server process").$(e).$();
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
