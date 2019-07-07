/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package com.questdb.network;

import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.mp.*;
import com.questdb.std.LongMatrix;
import com.questdb.std.time.MillisecondClock;

import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractIODispatcher<C extends IOContext> extends SynchronizedJob implements IODispatcher<C> {
    protected static final Log LOG = LogFactory.getLog("IODispatcher");
    protected static final int M_TIMESTAMP = 0;
    protected static final int M_FD = 1;
    protected final RingQueue<IOEvent<C>> interestQueue;
    protected final MPSequence interestPubSeq;
    protected final SCSequence interestSubSeq;
    protected final long serverFd;
    protected final RingQueue<IOEvent<C>> ioEventQueue;
    protected final SPSequence ioEventPubSeq;
    protected final MCSequence ioEventSubSeq;
    protected final MillisecondClock clock;
    protected final int activeConnectionLimit;
    protected final IOContextFactory<C> ioContextFactory;
    protected final NetworkFacade nf;
    protected final int initialBias;
    protected final AtomicInteger connectionCount = new AtomicInteger();
    protected final RingQueue<IOEvent<C>> disconnectQueue;
    protected final MPSequence disconnectPubSeq;
    protected final SCSequence disconnectSubSeq;
    protected final QueueConsumer<IOEvent<C>> disconnectContextRef = this::disconnectContext;
    protected final long idleConnectionTimeout;
    protected final LongMatrix<C> pending = new LongMatrix<>(4);
    private final int sndBufSize;
    private final int rcvBufSize;

    public AbstractIODispatcher(
            IODispatcherConfiguration configuration,
            IOContextFactory<C> ioContextFactory
    ) {
        this.nf = configuration.getNetworkFacade();
        this.serverFd = nf.socketTcp(false);

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
        this.idleConnectionTimeout = configuration.getIdleConnectionTimeout();

        this.sndBufSize = configuration.getSndBufSize();
        this.rcvBufSize = configuration.getRcvBufSize();

        if (nf.bindTcp(this.serverFd, configuration.getBindIPv4Address(), configuration.getBindPort())) {
            nf.listen(this.serverFd, configuration.getListenBacklog());
        } else {
            throw NetworkError.instance(nf.errno()).couldNotBindSocket();
        }
    }

    @Override
    public void close() {
        processDisconnects();
        nf.close(serverFd, LOG);

        for (int i = 0, n = pending.size(); i < n; i++) {
            doDisconnect(pending.get(i));
        }

        interestSubSeq.consumeAll(interestQueue, this.disconnectContextRef);
        ioEventSubSeq.consumeAll(ioEventQueue, this.disconnectContextRef);
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
            processor.onRequest(operation, connectionContext, this);
            return true;
        }

        return false;
    }

    @Override
    public void disconnect(C context) {
        final long cursor = disconnectPubSeq.nextBully();
        assert cursor > -1;
        disconnectQueue.get(cursor).context = context;
        disconnectPubSeq.done(cursor);
    }

    protected void accept(long timestamp) {
        while (true) {
            // this accept is greedy, rather than to rely on epoll(or similar) to
            // fire accept requests at us one at a time we will be actively accepting
            // until nothing left.

            long fd = nf.accept(serverFd);

            if (fd < 0) {
                if (nf.errno() != Net.EWOULDBLOCK) {
                    LOG.error().$("could not accept [ret=").$(fd).$(", errno=").$(nf.errno()).$(']').$();
                }
                return;
            }

            final int connectionCount = this.connectionCount.get();
            if (connectionCount == activeConnectionLimit) {
                LOG.info().$("connection limit exceeded [fd=").$(fd)
                        .$(", connectionCount=").$(connectionCount)
                        .$(", activeConnectionLimit=").$(activeConnectionLimit)
                        .$(']').$();
                nf.close(fd, LOG);
                return;
            }

            if (nf.configureNonBlocking(fd) < 0) {
                LOG.error().$("could not configure non-blocking [fd=").$(fd).$(", errno=").$(nf.errno()).$(']').$();
                nf.close(fd, LOG);
                return;
            }

            if (sndBufSize > 0) {
                nf.setSndBuf(fd, sndBufSize);
            }

            if (rcvBufSize > 0) {
                nf.setRcvBuf(fd, rcvBufSize);
            }

            LOG.info().$("connected [ip=").$ip(nf.getPeerIP(fd)).$(", fd=").$(fd).$(']').$();
            this.connectionCount.incrementAndGet();
            addPending(fd, timestamp);
        }
    }

    private void addPending(long fd, long timestamp) {
        // append to pending
        // all rows below watermark will be registered with kqueue
        int r = pending.addRow();
        LOG.debug().$("pending [row=").$(r).$(", fd=").$(fd).$(']').$();
        pending.set(r, M_TIMESTAMP, timestamp);
        pending.set(r, M_FD, fd);
        pending.set(r, ioContextFactory.newInstance(fd));
        pendingAdded(r);

    }

    private void disconnectContext(IOEvent<C> event) {
        doDisconnect(event.context);
    }

    protected void doDisconnect(C context) {
        if (context == null || context.invalid()) {
            return;
        }
        final long fd = context.getFd();
        LOG.info()
                .$("disconnected [ip=").$ip(nf.getPeerIP(fd))
                .$(", fd=").$(fd)
                .$(']').$();
        nf.close(fd, LOG);
        ioContextFactory.done(context);
        connectionCount.decrementAndGet();
    }

    protected void logSuccess(IODispatcherConfiguration configuration) {
        LOG.info()
                .$("listening on ")
                .$(configuration.getBindIPv4Address()).$(':').$(configuration.getBindPort())
                .$(" [fd=").$(serverFd).$(']').$();
    }

    protected abstract void pendingAdded(int index);

    protected void processDisconnects() {
        disconnectSubSeq.consumeAll(disconnectQueue, this.disconnectContextRef);
    }

    protected void publishOperation(int operation, C context) {
        long cursor = ioEventPubSeq.nextBully();
        IOEvent<C> evt = ioEventQueue.get(cursor);
        evt.context = context;
        evt.operation = operation;
        ioEventPubSeq.done(cursor);
        LOG.debug().$("fired [fd=").$(context.getFd()).$(", op=").$(evt.operation).$(", pos=").$(cursor).$(']').$();
    }
}
