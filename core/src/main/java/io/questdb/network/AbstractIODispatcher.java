/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.network;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.*;
import io.questdb.std.LongMatrix;
import io.questdb.std.datetime.millitime.MillisecondClock;

import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractIODispatcher<C extends IOContext> extends SynchronizedJob implements IODispatcher<C>, EagerThreadSetup {
    protected static final int M_TIMESTAMP = 0;
    protected static final int M_FD = 1;
    protected static final int DISCONNECT_SRC_QUEUE = 0;
    protected static final int DISCONNECT_SRC_IDLE = 1;
    protected static final int DISCONNECT_SRC_SHUTDOWN = 2;
    private final static String[] DISCONNECT_SOURCES;
    protected final Log LOG;
    private final IODispatcherConfiguration configuration;
    protected final RingQueue<IOEvent<C>> interestQueue;
    protected final MPSequence interestPubSeq;
    protected final SCSequence interestSubSeq;
    protected long serverFd;
    protected final RingQueue<IOEvent<C>> ioEventQueue;
    protected final SPSequence ioEventPubSeq;
    protected final MCSequence ioEventSubSeq;
    protected final MillisecondClock clock;
    protected final int activeConnectionLimit;
    protected final IOContextFactory<C> ioContextFactory;
    protected final NetworkFacade nf;
    protected final int initialBias;
    private final AtomicInteger connectionCount = new AtomicInteger();
    protected final RingQueue<IOEvent<C>> disconnectQueue;
    protected final MPSequence disconnectPubSeq;
    protected final SCSequence disconnectSubSeq;
    protected final QueueConsumer<IOEvent<C>> disconnectContextRef = this::disconnectContext;
    protected final long idleConnectionTimeout;
    protected final LongMatrix<C> pending = new LongMatrix<>(4);
    private final int sndBufSize;
    private final int rcvBufSize;
    private volatile boolean listening;
    private final long queuedConnectionTimeoutMs;
    private long closeListenFdEpochMs;
    private final boolean peerNoLinger;

    public AbstractIODispatcher(
            IODispatcherConfiguration configuration,
            IOContextFactory<C> ioContextFactory
    ) {
        this.LOG = LogFactory.getLog(configuration.getDispatcherLogName());
        this.configuration = configuration;
        this.nf = configuration.getNetworkFacade();

        this.interestQueue = new RingQueue<>(IOEvent::new, configuration.getInterestQueueCapacity());
        this.interestPubSeq = new MPSequence(interestQueue.getCapacity());
        this.interestSubSeq = new SCSequence();
        this.interestPubSeq.then(this.interestSubSeq).then(this.interestPubSeq);

        this.ioEventQueue = new RingQueue<>(IOEvent::new, configuration.getIOQueueCapacity());
        this.ioEventPubSeq = new SPSequence(configuration.getIOQueueCapacity());
        this.ioEventSubSeq = new MCSequence(configuration.getIOQueueCapacity());
        this.ioEventPubSeq.then(this.ioEventSubSeq).then(this.ioEventPubSeq);

        this.disconnectQueue = new RingQueue<>(IOEvent::new, configuration.getIOQueueCapacity());
        this.disconnectPubSeq = new MPSequence(disconnectQueue.getCapacity());
        this.disconnectSubSeq = new SCSequence();
        this.disconnectPubSeq.then(this.disconnectSubSeq).then(this.disconnectPubSeq);

        this.clock = configuration.getClock();
        this.activeConnectionLimit = configuration.getActiveConnectionLimit();
        this.ioContextFactory = ioContextFactory;
        this.initialBias = configuration.getInitialBias();
        this.idleConnectionTimeout = configuration.getIdleConnectionTimeout() > 0 ? configuration.getIdleConnectionTimeout() : Long.MIN_VALUE;
        this.queuedConnectionTimeoutMs = configuration.getQueuedConnectionTimeout() > 0 ? configuration.getQueuedConnectionTimeout() : 0;
        this.sndBufSize = configuration.getSndBufSize();
        this.rcvBufSize = configuration.getRcvBufSize();
        this.peerNoLinger = configuration.getPeerNoLinger();

        createListenFd();
        listening = true;
    }

    private void createListenFd() throws NetworkError {
        this.serverFd = nf.socketTcp(false);
        if (nf.bindTcp(this.serverFd, configuration.getBindIPv4Address(), configuration.getBindPort())) {
            nf.listen(this.serverFd, configuration.getListenBacklog());
        } else {
            throw NetworkError.instance(nf.errno()).couldNotBindSocket(
                    configuration.getDispatcherLogName(),
                    configuration.getBindIPv4Address(),
                    configuration.getBindPort());
        }
        LOG.advisory().$("listening on ").$ip(configuration.getBindIPv4Address()).$(':').$(configuration.getBindPort()).$(" [fd=").$(serverFd).$(']').$();
    }

    @Override
    public void close() {
        processDisconnects(Long.MAX_VALUE);
        for (int i = 0, n = pending.size(); i < n; i++) {
            doDisconnect(pending.get(i), DISCONNECT_SRC_SHUTDOWN);
        }

        interestSubSeq.consumeAll(interestQueue, this.disconnectContextRef);
        ioEventSubSeq.consumeAll(ioEventQueue, this.disconnectContextRef);
        if (serverFd > 0) {
            nf.close(serverFd, LOG);
            serverFd = -1;
        }
    }

    @Override
    public int getConnectionCount() {
        return connectionCount.get();
    }

    @Override
    public void registerChannel(C context, int operation) {
        long cursor = interestPubSeq.nextBully();
        IOEvent<C> evt = interestQueue.get(cursor);
        evt.context = context;
        evt.operation = operation;
        LOG.debug().$("queuing [fd=").$(context.getFd()).$(", op=").$(operation).$(']').$();
        interestPubSeq.done(cursor);
    }

    @Override
    public boolean processIOQueue(IORequestProcessor<C> processor) {
        long cursor = ioEventSubSeq.next();
        while (cursor == -2) {
            cursor = ioEventSubSeq.next();
        }

        if (cursor > -1) {
            IOEvent<C> event = ioEventQueue.get(cursor);
            C connectionContext = event.context;
            final int operation = event.operation;
            ioEventSubSeq.done(cursor);
            processor.onRequest(operation, connectionContext);
            return true;
        }

        return false;
    }

    @Override
    public void disconnect(C context, int reason) {
        LOG.info()
                .$("scheduling disconnect [fd=").$(context.getFd())
                .$(", reason=").$(reason)
                .I$();
        final long cursor = disconnectPubSeq.nextBully();
        assert cursor > -1;
        disconnectQueue.get(cursor).context = context;
        disconnectPubSeq.done(cursor);
    }

    @Override
    public void setup() {
        if (ioContextFactory instanceof EagerThreadSetup) {
            ((EagerThreadSetup) ioContextFactory).setup();
        }
    }

    protected void accept(long timestamp) {
        int tlConCount = this.connectionCount.get();
        while (tlConCount < activeConnectionLimit) {
            // this accept is greedy, rather than to rely on epoll(or similar) to
            // fire accept requests at us one at a time we will be actively accepting
            // until nothing left.

            long fd = nf.accept(serverFd);

            if (fd < 0) {
                if (nf.errno() != Net.EWOULDBLOCK) {
                    LOG.error().$("could not accept [ret=").$(fd).$(", errno=").$(nf.errno()).$(']').$();
                }
                break;
            }

            if (nf.configureNonBlocking(fd) < 0) {
                LOG.error().$("could not configure non-blocking [fd=").$(fd).$(", errno=").$(nf.errno()).$(']').$();
                nf.close(fd, LOG);
                break;
            }

            if (nf.setTcpNoDelay(fd, true) < 0) {
                // Randomly on OS X, if a client connects and the peer TCP socket has SO_LINGER set to false, then setting the TCP_NODELAY
                // option fails!
                LOG.info().$("could not turn off Nagle's algorithm [fd=").$(fd).$(", errno=").$(nf.errno()).$(']').$();
            }

            if (peerNoLinger) {
                nf.configureNoLinger(fd);
            }

            if (sndBufSize > 0) {
                nf.setSndBuf(fd, sndBufSize);
            }

            if (rcvBufSize > 0) {
                nf.setRcvBuf(fd, rcvBufSize);
            }

            LOG.info().$("connected [ip=").$ip(nf.getPeerIP(fd)).$(", fd=").$(fd).$(']').$();
            tlConCount = connectionCount.incrementAndGet();
            addPending(fd, timestamp);
        }

        if (tlConCount >= activeConnectionLimit) {
            if (connectionCount.get() >= activeConnectionLimit) {
                unregisterListenerFd();
                listening = false;
                closeListenFdEpochMs = timestamp + queuedConnectionTimeoutMs;
                LOG.info().$("max connection limit reached, unregistered listener [serverFd=").$(serverFd).I$();
            }
        }
    }

    private void addPending(long fd, long timestamp) {
        // append to pending
        // all rows below watermark will be registered with kqueue
        int r = pending.addRow();
        LOG.debug().$("pending [row=").$(r).$(", fd=").$(fd).$(']').$();
        pending.set(r, M_TIMESTAMP, timestamp);
        pending.set(r, M_FD, fd);
        pending.set(r, ioContextFactory.newInstance(fd, this));
        pendingAdded(r);
    }

    private void disconnectContext(IOEvent<C> event) {
        doDisconnect(event.context, DISCONNECT_SRC_QUEUE);
    }

    protected void doDisconnect(C context, int src) {
        if (context == null || context.invalid()) {
            return;
        }
        final long fd = context.getFd();
        LOG.info()
                .$("disconnected [ip=").$ip(nf.getPeerIP(fd))
                .$(", fd=").$(fd)
                .$(", src=").$(DISCONNECT_SOURCES[src])
                .$(']').$();
        nf.close(fd, LOG);
        ioContextFactory.done(context);
        if (connectionCount.getAndDecrement() >= activeConnectionLimit) {
            if (connectionCount.get() < activeConnectionLimit) {
                if (serverFd < 0) {
                    createListenFd();
                }
                registerListenerFd();
                listening = true;
                LOG.info().$("below maximum connection limit, registered listener [serverFd=").$(serverFd).I$();
            }
        }
    }

    protected abstract void pendingAdded(int index);

    protected abstract void registerListenerFd();

    protected abstract void unregisterListenerFd();

    protected void processDisconnects(long epochMs) {
        disconnectSubSeq.consumeAll(disconnectQueue, this.disconnectContextRef);
        if (!listening && serverFd >= 0 && epochMs >= closeListenFdEpochMs) {
            LOG.info().$("been unable to accept connections for ").$(queuedConnectionTimeoutMs).$("ms, closing listener [serverFd=").$(serverFd).I$();
            nf.close(serverFd);
            serverFd = -1;
        }
    }

    protected void publishOperation(int operation, C context) {
        long cursor = ioEventPubSeq.nextBully();
        IOEvent<C> evt = ioEventQueue.get(cursor);
        evt.context = context;
        evt.operation = operation;
        ioEventPubSeq.done(cursor);
        LOG.debug().$("fired [fd=").$(context.getFd()).$(", op=").$(evt.operation).$(", pos=").$(cursor).$(']').$();
    }

    @Override
    public boolean isListening() {
        return listening;
    }

    static {
        DISCONNECT_SOURCES = new String[] { "queue", "idle", "shutdown" };
    }
}
