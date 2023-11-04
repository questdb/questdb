/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.CairoException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.*;
import io.questdb.std.*;
import io.questdb.std.datetime.millitime.MillisecondClock;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Base class for all I/O dispatchers.
 * <p>
 * Important invariant:
 * dispatcher should never process a fd concurrently with I/O context. Instead, each of them does whatever
 * it has to do with a fd and sends a message over an in-memory queue to tell the other party that it's
 * free to proceed.
 */
public abstract class AbstractIODispatcher<C extends IOContext<C>> extends SynchronizedJob implements IODispatcher<C>, EagerThreadSetup {
    protected static final int DISCONNECT_SRC_IDLE = 1;
    protected static final int DISCONNECT_SRC_PEER_DISCONNECT = 3;
    protected static final int DISCONNECT_SRC_QUEUE = 0;
    protected static final int DISCONNECT_SRC_SHUTDOWN = 2;
    protected static final int DISCONNECT_SRC_TLS_ERROR = 4;
    protected static final int OPM_CREATE_TIMESTAMP = 0;
    protected static final int OPM_FD = 1;
    protected static final int OPM_HEARTBEAT_TIMESTAMP = 3;
    protected static final int OPM_ID = 4;
    protected static final int OPM_COLUMN_COUNT = OPM_ID + 1;
    protected static final int OPM_OPERATION = 2;
    private final static String[] DISCONNECT_SOURCES;
    protected final Log LOG;
    protected final int activeConnectionLimit;
    protected final MillisecondClock clock;
    protected final MPSequence disconnectPubSeq;
    protected final RingQueue<IOEvent<C>> disconnectQueue;
    protected final SCSequence disconnectSubSeq;
    protected final long idleConnectionTimeout;
    protected final int initialBias;
    protected final MPSequence interestPubSeq;
    protected final RingQueue<IOEvent<C>> interestQueue;
    protected final SCSequence interestSubSeq;
    protected final IOContextFactory<C> ioContextFactory;
    protected final SPSequence ioEventPubSeq;
    protected final RingQueue<IOEvent<C>> ioEventQueue;
    protected final MCSequence ioEventSubSeq;
    protected final NetworkFacade nf;
    protected final ObjLongMatrix<C> pending = new ObjLongMatrix<>(OPM_COLUMN_COUNT);
    protected final ObjLongMatrix<C> pendingHeartbeats = new ObjLongMatrix<>(OPM_COLUMN_COUNT);
    private final IODispatcherConfiguration configuration;
    private final AtomicInteger connectionCount = new AtomicInteger();
    private final boolean peerNoLinger;
    private final long queuedConnectionTimeoutMs;
    private final int rcvBufSize;
    private final int sndBufSize;
    private final int testConnectionBufSize;
    protected boolean closed = false;
    protected long heartbeatIntervalMs;
    protected int serverFd;
    private long closeListenFdEpochMs;
    private volatile boolean listening;
    private int port;
    protected final QueueConsumer<IOEvent<C>> disconnectContextRef = this::disconnectContext;
    private long testConnectionBuf;

    public AbstractIODispatcher(
            IODispatcherConfiguration configuration,
            IOContextFactory<C> ioContextFactory
    ) {
        this.LOG = LogFactory.getLog(configuration.getDispatcherLogName());
        this.configuration = configuration;
        this.nf = configuration.getNetworkFacade();

        this.testConnectionBufSize = configuration.getTestConnectionBufferSize();
        this.testConnectionBuf = Unsafe.malloc(testConnectionBufSize, MemoryTag.NATIVE_DEFAULT);

        this.interestQueue = new RingQueue<>(IOEvent::new, configuration.getInterestQueueCapacity());
        this.interestPubSeq = new MPSequence(interestQueue.getCycle());
        this.interestSubSeq = new SCSequence();
        this.interestPubSeq.then(interestSubSeq).then(interestPubSeq);

        this.ioEventQueue = new RingQueue<>(IOEvent::new, configuration.getIOQueueCapacity());
        this.ioEventPubSeq = new SPSequence(configuration.getIOQueueCapacity());
        this.ioEventSubSeq = new MCSequence(configuration.getIOQueueCapacity());
        this.ioEventPubSeq.then(ioEventSubSeq).then(ioEventPubSeq);

        this.disconnectQueue = new RingQueue<>(IOEvent::new, configuration.getIOQueueCapacity());
        this.disconnectPubSeq = new MPSequence(disconnectQueue.getCycle());
        this.disconnectSubSeq = new SCSequence();
        this.disconnectPubSeq.then(disconnectSubSeq).then(disconnectPubSeq);

        this.clock = configuration.getClock();
        this.activeConnectionLimit = configuration.getLimit();
        this.ioContextFactory = ioContextFactory;
        this.initialBias = configuration.getInitialBias();
        this.idleConnectionTimeout = configuration.getTimeout() > 0 ? configuration.getTimeout() : Long.MIN_VALUE;
        this.queuedConnectionTimeoutMs = configuration.getQueueTimeout() > 0 ? configuration.getQueueTimeout() : 0;
        this.sndBufSize = configuration.getSndBufSize();
        this.rcvBufSize = configuration.getRcvBufSize();
        this.peerNoLinger = configuration.getPeerNoLinger();
        this.port = 0;
        this.heartbeatIntervalMs = configuration.getHeartbeatInterval() > 0 ? configuration.getHeartbeatInterval() : Long.MIN_VALUE;
        createListenFd();
        listening = true;
    }

    @Override
    public void close() {
        closed = true;

        processDisconnects(Long.MAX_VALUE);
        for (int i = 0, n = pending.size(); i < n; i++) {
            doDisconnect(pending.get(i), DISCONNECT_SRC_SHUTDOWN);
        }

        interestSubSeq.consumeAll(interestQueue, disconnectContextRef);
        ioEventSubSeq.consumeAll(ioEventQueue, disconnectContextRef);

        // Important: we need to process all queues before we iterate through pending heartbeats.
        // Otherwise, we may end up closing the same context twice.
        for (int i = 0, n = pendingHeartbeats.size(); i < n; i++) {
            doDisconnect(pendingHeartbeats.get(i), DISCONNECT_SRC_SHUTDOWN);
        }

        if (serverFd > 0) {
            nf.close(serverFd, LOG);
            serverFd = -1;
        }

        testConnectionBuf = Unsafe.free(testConnectionBuf, testConnectionBufSize, MemoryTag.NATIVE_DEFAULT);
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
    public int getConnectionCount() {
        return connectionCount.get();
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public boolean isListening() {
        return listening;
    }

    @Override
    public boolean processIOQueue(IORequestProcessor<C> processor) {
        long cursor = ioEventSubSeq.next();
        while (cursor == -2) {
            Os.pause();
            cursor = ioEventSubSeq.next();
        }

        boolean useful = false;
        if (cursor > -1) {
            IOEvent<C> event = ioEventQueue.get(cursor);
            C connectionContext = event.context;
            final int operation = event.operation;
            ioEventSubSeq.done(cursor);
            useful = processor.onRequest(operation, connectionContext);
        }

        return useful;
    }

    @Override
    public void registerChannel(C context, int operation) {
        long cursor = interestPubSeq.nextBully();
        IOEvent<C> evt = interestQueue.get(cursor);
        evt.context = context;
        evt.operation = operation;
        LOG.debug().$("queuing [fd=").$(context.getFd()).$(", op=").$(operation).I$();
        interestPubSeq.done(cursor);
    }

    @Override
    public void setup() {
        if (ioContextFactory instanceof EagerThreadSetup) {
            ((EagerThreadSetup) ioContextFactory).setup();
        }
    }

    private void addPending(int fd, long timestamp) {
        // append pending connection
        // all rows below watermark will be registered with epoll (or similar)
        final C context = ioContextFactory.newInstance(fd, this);
        try {
            context.init();
        } catch (CairoException e) {
            LOG.error().$("could not initialize connection context [fd=").$(fd).$(", e=").$(e.getFlyweightMessage()).I$();
            ioContextFactory.done(context);
            return;
        }
        int r = pending.addRow();
        LOG.debug().$("pending [row=").$(r).$(", fd=").$(fd).I$();
        pending.set(r, OPM_CREATE_TIMESTAMP, timestamp);
        pending.set(r, OPM_HEARTBEAT_TIMESTAMP, timestamp);
        pending.set(r, OPM_FD, fd);
        pending.set(r, OPM_OPERATION, -1);
        pending.set(r, context);
        pendingAdded(r);
    }

    private void createListenFd() throws NetworkError {
        this.serverFd = nf.socketTcp(false);
        final int backlog = configuration.getListenBacklog();
        if (this.port == 0) {
            // Note that `configuration.getBindPort()` might also be 0.
            // In such case, we will bind to an ephemeral port.
            this.port = configuration.getBindPort();
            if (Os.isWindows()) {
                // Make windows release listening port faster, same as Linux
                nf.setReusePort(serverFd);
            }
        }
        if (nf.bindTcp(this.serverFd, configuration.getBindIPv4Address(), this.port)) {
            if (this.port == 0) {
                // We resolve port 0 only once. In case we close and re-open the
                // listening socket, we will reuse the previously resolved
                // ephemeral port.
                this.port = nf.resolvePort(this.serverFd);
            }
            nf.listen(this.serverFd, backlog);
        } else {
            throw NetworkError.instance(nf.errno()).couldNotBindSocket(
                    configuration.getDispatcherLogName(),
                    configuration.getBindIPv4Address(),
                    this.port
            );
        }
        LOG.advisory().$("listening on ").$ip(configuration.getBindIPv4Address()).$(':').$(configuration.getBindPort())
                .$(" [fd=").$(serverFd)
                .$(" backlog=").$(backlog)
                .I$();
    }

    private void disconnectContext(IOEvent<C> event) {
        C context = event.context;
        if (context != null && !context.invalid()) {
            final long heartbeatOpId = context.getAndResetHeartbeatId();
            if (heartbeatOpId != -1) {
                int r = pendingHeartbeats.binarySearch(heartbeatOpId, OPM_ID);
                if (r < 0) {
                    LOG.critical().$("internal error: heartbeat not found [opId=").$(heartbeatOpId).I$();
                } else {
                    pendingHeartbeats.deleteRow(r);
                }
            }
        }
        doDisconnect(context, DISCONNECT_SRC_QUEUE);
    }

    protected static int tlsIOFlags(int requestedOp, boolean readyForRead, boolean readyForWrite) {
        return (requestedOp == IOOperation.READ && readyForWrite ? Socket.WRITE_FLAG : 0)
                | (requestedOp == IOOperation.WRITE && readyForRead ? Socket.READ_FLAG : 0);
    }

    protected static int tlsIOFlags(boolean readyForRead, boolean readyForWrite) {
        return (readyForWrite ? Socket.WRITE_FLAG : 0) | (readyForRead ? Socket.READ_FLAG : 0);
    }

    protected void accept(long timestamp) {
        int tlConCount = this.connectionCount.get();
        while (tlConCount < activeConnectionLimit) {
            // this 'accept' is greedy, rather than to rely on epoll (or similar) to
            // fire accept requests at us one at a time we will be actively accepting
            // until nothing left.

            int fd = nf.accept(serverFd);

            if (fd < 0) {
                if (nf.errno() != Net.EWOULDBLOCK) {
                    LOG.error().$("could not accept [ret=").$(fd).$(", errno=").$(nf.errno()).I$();
                }
                break;
            }

            if (nf.configureNonBlocking(fd) < 0) {
                LOG.error().$("could not configure non-blocking [fd=").$(fd).$(", errno=").$(nf.errno()).I$();
                nf.close(fd, LOG);
                break;
            }

            if (nf.setTcpNoDelay(fd, true) < 0) {
                // Randomly on OS X, if a client connects and the peer TCP socket has SO_LINGER set to false, then setting the TCP_NODELAY
                // option fails!
                LOG.info().$("could not turn off Nagle's algorithm [fd=").$(fd).$(", errno=").$(nf.errno()).I$();
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
            nf.configureKeepAlive(fd);

            LOG.info().$("connected [ip=").$ip(nf.getPeerIP(fd)).$(", fd=").$(fd).I$();
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

    protected void doDisconnect(C context, int src) {
        if (context == null || context.invalid()) {
            return;
        }

        final int fd = context.getFd();
        LOG.info()
                .$("disconnected [ip=").$ip(nf.getPeerIP(fd))
                .$(", fd=").$(fd)
                .$(", src=").$(DISCONNECT_SOURCES[src])
                .I$();
        if (closed) {
            Misc.free(context);
        } else {
            ioContextFactory.done(context);
        }
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

    protected void processDisconnects(long epochMs) {
        disconnectSubSeq.consumeAll(disconnectQueue, disconnectContextRef);
        if (!listening && serverFd >= 0 && epochMs >= closeListenFdEpochMs) {
            LOG.error().$("been unable to accept connections for ").$(queuedConnectionTimeoutMs)
                    .$("ms, closing listener [serverFd=").$(serverFd).I$();
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
        LOG.debug().$("fired [fd=").$(context.getFd())
                .$(", op=").$(operation)
                .$(", pos=").$(cursor).I$();
    }

    protected abstract void registerListenerFd();

    protected boolean testConnection(int fd) {
        return nf.testConnection(fd, testConnectionBuf, testConnectionBufSize);
    }

    protected abstract void unregisterListenerFd();

    static {
        DISCONNECT_SOURCES = new String[]{"queue", "idle", "shutdown", "peer", "tls_error"};
    }
}
