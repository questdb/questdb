/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.metrics.Counter;
import io.questdb.metrics.LongGauge;
import io.questdb.mp.EagerThreadSetup;
import io.questdb.mp.MCSequence;
import io.questdb.mp.MPSequence;
import io.questdb.mp.QueueConsumer;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.SPSequence;
import io.questdb.mp.Sequence;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjLongMatrix;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
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
    private static final String[] DISCONNECT_SOURCES;
    protected final Log LOG;
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
    private final LongGauge connectionCountGauge;
    private final Counter listenerStateChangeCounter;
    private final boolean peerNoLinger;
    private final long queuedConnectionTimeoutMs;
    private final int testConnectionBufSize;
    protected volatile boolean closed = false;
    protected long heartbeatIntervalMs;
    protected long serverFd;
    private long closeListenFdEpochMs;
    // the final ids are shifted by 1 bit which is reserved to distinguish socket operations (0) and suspend events (1);
    // id 0 is reserved for operations on the server fd
    private long idSeq = 1;
    private volatile boolean listening;
    protected final QueueConsumer<IOEvent<C>> disconnectContextRef = this::disconnectContext;
    private int port;
    private long testConnectionBuf;

    public AbstractIODispatcher(
            IODispatcherConfiguration configuration,
            IOContextFactory<C> ioContextFactory
    ) {
        this.LOG = LogFactory.getLog(configuration.getDispatcherLogName());
        this.configuration = configuration;
        this.connectionCountGauge = configuration.getConnectionCountGauge();
        this.listenerStateChangeCounter = configuration.listenerStateChangeCounter();
        this.nf = configuration.getNetworkFacade();

        this.testConnectionBufSize = configuration.getTestConnectionBufferSize();
        this.testConnectionBuf = Unsafe.malloc(testConnectionBufSize, MemoryTag.NATIVE_DEFAULT);

        this.interestQueue = new RingQueue<>(IOEvent::new, configuration.getInterestQueueCapacity());
        this.interestPubSeq = new MPSequence(interestQueue.getCycle());
        this.interestSubSeq = new SCSequence();
        this.interestPubSeq.then(interestSubSeq).then(interestPubSeq);

        this.ioEventQueue = new RingQueue<>(IOEvent::new, configuration.getIOQueueCapacity());
        this.ioEventPubSeq = new SPSequence(ioEventQueue.getCycle());
        this.ioEventSubSeq = new MCSequence(ioEventQueue.getCycle());
        this.ioEventPubSeq.then(ioEventSubSeq).then(ioEventPubSeq);

        this.disconnectQueue = new RingQueue<>(IOEvent::new, configuration.getIOQueueCapacity());
        this.disconnectPubSeq = new MPSequence(disconnectQueue.getCycle());
        this.disconnectSubSeq = new SCSequence();
        this.disconnectPubSeq.then(disconnectSubSeq).then(disconnectPubSeq);

        this.clock = configuration.getClock();
        this.ioContextFactory = ioContextFactory;
        this.initialBias = configuration.getInitialBias();
        this.idleConnectionTimeout = configuration.getTimeout() > 0 ? configuration.getTimeout() : Long.MIN_VALUE;
        this.queuedConnectionTimeoutMs = configuration.getQueueTimeout() > 0 ? configuration.getQueueTimeout() : 0;
        this.peerNoLinger = configuration.getPeerNoLinger();
        this.port = 0;
        this.heartbeatIntervalMs = configuration.getHeartbeatInterval() > 0 ? configuration.getHeartbeatInterval() : Long.MIN_VALUE;

        try {
            createListenerFd();
        } catch (Throwable th) {
            close();
            throw th;
        }
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
        LOG.debug()
                .$("scheduling disconnect [fd=").$(context.getFd())
                .$(", reason=").$(reason)
                .I$();
        final long cursor = bullyUntilClosed(disconnectPubSeq);
        if (cursor < 0) {
            assert closed;
            return;
        }
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
            try {
                connectionContext.init();
                useful = processor.onRequest(operation, connectionContext, this);
            } catch (TlsSessionInitFailedException e) {
                LOG.error().$("could not initialize connection context [fd=").$(connectionContext.getFd())
                        .$(", e=").$safe(e.getFlyweightMessage())
                        .I$();
                ioContextFactory.done(connectionContext);
                disconnect(connectionContext, DISCONNECT_REASON_TLS_SESSION_INIT_FAILED);
            }
        }

        return useful;
    }

    @Override
    public void registerChannel(C context, int operation) {
        final long cursor = bullyUntilClosed(interestPubSeq);
        if (cursor < 0) {
            assert closed;
            return;
        }
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

    private void addPending(long fd, long timestamp) {
        // append pending connection
        // all rows below watermark will be registered with epoll (or similar)
        final C context = ioContextFactory.newInstance(fd);
        int r = pending.addRow();
        LOG.debug().$("pending [row=").$(r).$(", fd=").$(fd).I$();
        pending.set(r, OPM_CREATE_TIMESTAMP, timestamp);
        pending.set(r, OPM_HEARTBEAT_TIMESTAMP, timestamp);
        pending.set(r, OPM_FD, fd);
        pending.set(r, OPM_ID, nextOpId());
        pending.set(r, OPM_OPERATION, -1);
        pending.set(r, context);
        pendingAdded(r);
    }

    private long bullyUntilClosed(Sequence sequence) {
        // inlined version of sequence.nextBully() - we need to check for the 'closed' flag while looping
        // if the queue is full and all other workers are closed, then a naive bully would block forever
        long cursor;
        while ((cursor = sequence.next()) < 0 && !closed) {
            sequence.getBarrier().getWaitStrategy().signal();
        }
        return cursor;
    }

    private void checkConnectionLimitAndRestartListener() {
        final int activeConnectionLimit = configuration.getLimit();
        final int connCount = connectionCount.get();
        if (connCount < activeConnectionLimit) {
            if (serverFd < 0) {
                createListenerFd();
                // Make sure to always register for listening if server fd was recreated.
                listening = false;
            }

            if (!listening) {
                registerListenerFd();
                listening = true;
                listenerStateChangeCounter.inc();
                LOG.advisory().$("below maximum connection limit, registered listener [serverFd=").$(serverFd).$(", connCount=").$(connCount).I$();
            }
        }
    }

    private void createListenerFd() throws NetworkError {
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
        LOG.advisory().$("listening on ").$ip(configuration.getBindIPv4Address()).$(':').$(this.port)
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

    /**
     * Accepts pending connections in a greedy loop until the queue is drained (EAGAIN), timeout expires,
     * or connection limit is reached.
     *
     * @param timestamp current time in milliseconds
     * @return true if fully drained to EAGAIN, false if exited early (timeout/limit). When false,
     * caller must retry on next iteration to avoid stranding connections with edge-triggered epoll.
     */
    protected boolean accept(long timestamp) {
        // note: acceptEndTime intentionally uses current wall-clock time and not the passed timestamp
        // why? we want to run the accept loop for up to the configured timeout, regardless of any time
        // spent on activities before entering this loop
        final long acceptEndTime = clock.getTicks() + configuration.getAcceptLoopTimeout();
        int tlConCount = connectionCount.get();
        boolean drainedFully = false;
        while (tlConCount < configuration.getLimit() && acceptEndTime > clock.getTicks()) {
            // This 'accept' is greedy.
            // Rather than to rely on epoll (or similar) to fire accept requests at us one at
            // a time, we will be actively accepting until nothing left, or until we reach the
            // accept loop timeout.

            long fd = nf.accept(serverFd);

            if (fd < 0) {
                if (nf.errno() == Net.EWOULDBLOCK) {
                    drainedFully = true;
                } else {
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

            final int sndBufSize = configuration.getNetSendBufferSize();
            if (sndBufSize > 0) {
                nf.setSndBuf(fd, sndBufSize);
            }

            final int rcvBufSize = configuration.getNetRecvBufferSize();
            if (rcvBufSize > 0) {
                nf.setRcvBuf(fd, rcvBufSize);
            }
            nf.configureKeepAlive(fd);

            tlConCount = connectionCount.incrementAndGet();
            LOG.info().$("connected [ip=").$ip(nf.getPeerIP(fd)).$(", fd=").$(fd).$(", connCount=").$(tlConCount).I$();
            try {
                addPending(fd, timestamp);
            } catch (Throwable th) {
                LOG.error().$("could not accept connection [fd=").$(fd).$(", e=").$(th).I$();
                nf.close(fd, LOG);
                connectionCount.decrementAndGet();
                continue;
            }
            connectionCountGauge.inc();
        }

        // the condition below is checked against connection limit twice
        // since the limit might asynchronously change, it is imperative to
        // perform both checks against the same value
        final int lim = configuration.getLimit();
        if (tlConCount >= lim && connectionCount.get() >= lim) {
            unregisterListenerFd();
            listening = false;
            closeListenFdEpochMs = timestamp + queuedConnectionTimeoutMs;
            LOG.advisory()
                    .$("max connection limit reached, unregistered listener [serverFd=").$(serverFd)
                    .$(", tlConCount=").$(tlConCount)
                    .$(", connectionCount=").$(connectionCount.get())
                    .$(", limit=").$(configuration.getLimit())
                    .$(", lim=").$(lim)
                    .$(", connectionCountGauge=").$(connectionCountGauge.getValue())
                    .I$();
            listenerStateChangeCounter.inc();

            if (lim != configuration.getLimit()) {
                checkConnectionLimitAndRestartListener();
            }
        }
        return drainedFully;
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
                .I$();
        if (closed) {
            Misc.free(context);
        } else {
            ioContextFactory.done(context);
        }

        connectionCount.decrementAndGet();
        checkConnectionLimitAndRestartListener();
        connectionCountGauge.dec();
    }

    protected boolean isEventId(long id) {
        return (id & 1) == 1;
    }

    // returns monotonically growing event identifier (odd number);
    // may be used for suspend event identifiers
    protected long nextEventId() {
        return (idSeq++ << 1) + 1;
    }

    // returns monotonically growing operation identifier (even number)
    protected long nextOpId() {
        return idSeq++ << 1;
    }

    protected void pendingAdded(int index) {
        // no-op
    }

    protected boolean processDisconnects(long epochMs) {
        boolean useful = disconnectSubSeq.consumeAll(disconnectQueue, disconnectContextRef);
        if (!listening && serverFd >= 0 && epochMs >= closeListenFdEpochMs) {
            LOG.error().$("been unable to accept connections for ").$(queuedConnectionTimeoutMs)
                    .$("ms, closing listener [serverFd=").$(serverFd)
                    .I$();
            nf.close(serverFd);
            serverFd = -1;
            useful = true;
        }
        return useful;
    }

    protected void publishOperation(int operation, C context) {
        final long cursor = bullyUntilClosed(ioEventPubSeq);
        if (cursor < 0) {
            assert closed;
            return;
        }
        IOEvent<C> evt = ioEventQueue.get(cursor);
        evt.context = context;
        evt.operation = operation;
        ioEventPubSeq.done(cursor);
        LOG.debug().$("fired [fd=").$(context.getFd())
                .$(", op=").$(operation)
                .$(", pos=").$(cursor)
                .I$();
    }

    protected abstract void registerListenerFd();

    protected boolean testConnection(long fd) {
        return nf.testConnection(fd, testConnectionBuf, testConnectionBufSize);
    }

    protected abstract void unregisterListenerFd();

    static {
        DISCONNECT_SOURCES = new String[]{"queue", "idle", "shutdown", "peer", "tls_error"};
    }
}
