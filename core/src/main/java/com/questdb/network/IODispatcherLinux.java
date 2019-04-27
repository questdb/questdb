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

public class IODispatcherLinux<C extends IOContext> extends SynchronizedJob implements IODispatcher<C> {
    private static final int M_TIMESTAMP = 1;
    private static final int M_FD = 2;
    private static final int M_ID = 0;
    private static final Log LOG = LogFactory.getLog(IODispatcherLinux.class);
    private final long serverFd;
    private final RingQueue<IOEvent<C>> ioEventQueue;
    private final Sequence ioEventPubSeq;
    private final Sequence ioEventSubSeq;
    private final RingQueue<IOEvent<C>> interestQueue;
    private final MPSequence interestPubSeq;
    private final SCSequence interestSubSeq;
    private final MillisecondClock clock;
    private final Epoll epoll;
    private final long idleConnectionTimeout;
    private final LongMatrix<C> pending = new LongMatrix<>(4);
    private final int activeConnectionLimit;
    private final IOContextFactory<C> ioContextFactory;
    private final NetworkFacade nf;
    private final int initialBias;
    private final AtomicInteger connectionCount = new AtomicInteger();
    private long fdid = 1;

    public IODispatcherLinux(
            IODispatcherConfiguration configuration,
            IOContextFactory<C> ioContextFactory
    ) {
        this.nf = configuration.getNetworkFacade();
        this.ioEventQueue = new RingQueue<>(IOEvent::new, configuration.getIOQueueCapacity());
        this.ioEventPubSeq = new SPSequence(configuration.getIOQueueCapacity());
        this.ioEventSubSeq = new MCSequence(configuration.getIOQueueCapacity());
        this.ioEventPubSeq.then(this.ioEventSubSeq).then(this.ioEventPubSeq);
        this.interestQueue = new RingQueue<>(IOEvent::new, configuration.getInterestQueueCapacity());
        this.interestPubSeq = new MPSequence(interestQueue.getCapacity());
        this.interestSubSeq = new SCSequence();
        this.interestPubSeq.then(this.interestSubSeq).then(this.interestPubSeq);
        this.clock = configuration.getClock();
        this.activeConnectionLimit = configuration.getActiveConnectionLimit();
        this.idleConnectionTimeout = configuration.getIdleConnectionTimeout();
        this.ioContextFactory = ioContextFactory;
        this.initialBias = configuration.getInitialBias();
        this.epoll = new Epoll(configuration.getEpollFacade(), configuration.getEventCapacity());
        this.serverFd = nf.socketTcp(false);
        if (nf.bindTcp(this.serverFd, configuration.getBindIPv4Address(), configuration.getBindPort())) {
            nf.listen(this.serverFd, configuration.getListenBacklog());
            this.epoll.listen(serverFd);
            LOG.info()
                    .$("listening on ")
                    .$(configuration.getBindIPv4Address()).$(':').$(configuration.getBindPort())
                    .$(" [fd=").$(serverFd).$(']').$();
        } else {
            throw NetworkError.instance(nf.errno()).couldNotBindSocket();
        }
    }

    @Override
    public void close() {
        this.epoll.close();
        nf.close(serverFd, LOG);

        int n = pending.size();
        for (int i = 0; i < n; i++) {
            disconnect(pending.get(i), DisconnectReason.SILLY);
        }

        drainQueueAndDisconnect();

        long cursor;
        do {
            cursor = ioEventSubSeq.next();
            if (cursor > -1) {
                disconnect(ioEventQueue.get(cursor).context, DisconnectReason.SILLY);
                ioEventSubSeq.done(cursor);
            }
        } while (cursor != -1);
        LOG.info().$("closed").$();
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
    public void disconnect(C context, int disconnectReason) {
        final long fd = context.getFd();
        LOG.info()
                .$("disconnected [ip=").$ip(nf.getPeerIP(fd))
                .$(", fd=").$(fd)
                .$(", reason=").$(DisconnectReason.nameOf(disconnectReason))
                .$(']').$();
        nf.close(fd, LOG);
        ioContextFactory.done(context);
        connectionCount.decrementAndGet();
    }

    private void accept() {
        while (true) {
            long fd = nf.accept(serverFd);

            if (fd < 0) {
                if (nf.errno() != Net.EWOULDBLOCK) {
                    LOG.error().$("could not accept [errno=").$(nf.errno()).$(']').$();
                }
                return;
            }

            if (nf.configureNonBlocking(fd) < 0) {
                LOG.error().$("could not configure non-blocking [fd=").$(fd).$(", errno=").$(nf.errno()).$(']').$();
                nf.close(fd, LOG);
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

            LOG.info().$("connected [ip=").$ip(nf.getPeerIP(fd)).$(", fd=").$(fd).$(']').$();
            this.connectionCount.incrementAndGet();
            addPending(fd, clock.getTicks());
        }
    }

    private void addPending(long fd, long timestamp) {
        // append to pending
        int r = pending.addRow();
        LOG.debug().$("pending [row=").$(r).$(", fd=").$(fd).$(']').$();
        pending.set(r, M_TIMESTAMP, timestamp);
        pending.set(r, M_FD, fd);
        pending.set(r, M_ID, fdid++);
        pending.set(r, ioContextFactory.newInstance(fd));
    }

    private void drainQueueAndDisconnect() {
        long cursor;
        do {
            cursor = interestSubSeq.next();
            if (cursor > -1) {
                final long available = interestSubSeq.available();
                while (cursor < available) {
                    disconnect(interestQueue.get(cursor++).context, DisconnectReason.SILLY);
                }
                interestSubSeq.done(available - 1);
            }
        } while (cursor != -1);
    }

    private void enqueuePending(int watermark) {
        for (int i = watermark, sz = pending.size(), offset = 0; i < sz; i++, offset += EpollAccessor.SIZEOF_EVENT) {
            epoll.setOffset(offset);
            if (
                    epoll.control(
                            (int) pending.get(i, M_FD),
                            pending.get(i, M_ID),
                            EpollAccessor.EPOLL_CTL_ADD,
                            initialBias == IODispatcherConfiguration.BIAS_READ ? EpollAccessor.EPOLLIN : EpollAccessor.EPOLLOUT
                    ) < 0) {
                LOG.debug().$("epoll_ctl failure ").$(nf.errno()).$();
            }
        }
    }

    private void processIdleConnections(long deadline) {
        int count = 0;
        for (int i = 0, n = pending.size(); i < n && pending.get(i, M_TIMESTAMP) < deadline; i++, count++) {
            disconnect(pending.get(i), DisconnectReason.IDLE);
        }
        pending.zapTop(count);
    }

    private boolean processRegistrations(long timestamp) {
        long cursor;
        int offset = 0;
        while ((cursor = interestSubSeq.next()) > -1) {
            IOEvent<C> evt = interestQueue.get(cursor);
            C context = evt.context;
            int operation = evt.operation;
            interestSubSeq.done(cursor);

            int fd = (int) context.getFd();
            final long id = fdid++;
            // we re-arm epoll globally, in that even when we disconnect
            // because we have to remove FD from epoll
            LOG.debug().$("registered [fd=").$(fd).$(", op=").$(operation).$(", id=").$(id).$(']').$();
            epoll.setOffset(offset);
            if (epoll.control(fd, id, EpollAccessor.EPOLL_CTL_MOD, operation == IOOperation.READ ? EpollAccessor.EPOLLIN : EpollAccessor.EPOLLOUT) < 0) {
                System.out.println("oops2: " + nf.errno());
            }
            offset += EpollAccessor.SIZEOF_EVENT;

            int r = pending.addRow();
            pending.set(r, M_TIMESTAMP, timestamp);
            pending.set(r, M_FD, fd);
            pending.set(r, M_ID, id);
            pending.set(r, context);
        }

        if (offset > 0) {
            LOG.debug().$("reg").$();
        }
        return offset > 0;
    }

    private void publishOperation(int operation, C context) {
        long cursor = ioEventPubSeq.nextBully();
        IOEvent<C> evt = ioEventQueue.get(cursor);
        evt.context = context;
        evt.operation = operation;
        ioEventPubSeq.done(cursor);
        LOG.debug().$("fired [fd=").$(context.getFd()).$(", op=").$(evt.operation).$(", pos=").$(cursor).$(']').$();
    }

    @Override
    protected boolean runSerially() {
        boolean useful = false;
        final int n = epoll.poll();
        int watermark = pending.size();
        int offset = 0;
        if (n > 0) {
            // check all activated FDs
            LOG.debug().$("epoll [n=").$(n).$(']').$();
            for (int i = 0; i < n; i++) {
                epoll.setOffset(offset);
                offset += EpollAccessor.SIZEOF_EVENT;
                final long id = epoll.getData();
                // this is server socket, accept if there aren't too many already
                if (id == 0) {
                    accept();
                } else {
                    // find row in pending for two reasons:
                    // 1. find payload
                    // 2. remove row from pending, remaining rows will be timed out
                    int row = pending.binarySearch(id);
                    if (row < 0) {
                        LOG.error().$("internal error: epoll returned unexpected id [id=").$(id).$(']').$();
                        continue;
                    }

                    publishOperation(
                            (epoll.getEvent() & EpollAccessor.EPOLLIN) > 0 ? IOOperation.READ : IOOperation.WRITE,
                            pending.get(row)
                    );
                    pending.deleteRow(row);
                    watermark--;
                }
            }

            // process rows over watermark
            if (watermark < pending.size()) {
                enqueuePending(watermark);
            }
            useful = true;
        }

        // process timed out connections
        final long timestamp = clock.getTicks();
        final long deadline = timestamp - idleConnectionTimeout;
        if (pending.size() > 0 && pending.get(0, M_TIMESTAMP) < deadline) {
            processIdleConnections(deadline);
            useful = true;
        }

        return processRegistrations(timestamp) || useful;
    }
}
