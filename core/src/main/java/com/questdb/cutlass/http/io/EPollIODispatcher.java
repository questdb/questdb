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

package com.questdb.cutlass.http.io;

import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.mp.*;
import com.questdb.net.Epoll;
import com.questdb.std.*;
import com.questdb.std.ex.NetworkError;
import com.questdb.std.time.MillisecondClock;

public class EPollIODispatcher<C extends IOContext> extends SynchronizedJob implements IODispatcher<C> {
    private static final int M_TIMESTAMP = 1;
    private static final int M_FD = 2;
    private static final int M_ID = 0;
    private static final Log LOG = LogFactory.getLog(EPollIODispatcher.class);
    private final long serverFd;
    private final RingQueue<IOEvent<C>> ioEventQueue;
    private final Sequence ioEventPubSeq;
    private final RingQueue<IOEvent<C>> interestQueue;
    private final MPSequence interestPubSequence;
    private final SCSequence interestSubSequence = new SCSequence();
    private final MillisecondClock clock;
    private final Epoll epoll;
    private final long timeout;
    private final LongMatrix<C> pending = new LongMatrix<>(4);
    private final int connectionLimit;
    private final IOContextFactory<C> contextFactory;
    private final NetworkFacade nf;
    private int connectionCount = 0;
    private long fdid = 1;

    public EPollIODispatcher(IODispatcherConfiguration<C> configuration, RingQueue<IOEvent<C>> ioEventQueue, Sequence ioEventPubSeq) {
        this.ioEventQueue = ioEventQueue;
        this.ioEventPubSeq = ioEventPubSeq;
        this.nf = configuration.getNetworkFacade();
        this.interestQueue = new RingQueue<>(configuration.getIOEventFactory(), ioEventQueue.getCapacity());
        this.interestPubSequence = new MPSequence(interestQueue.getCapacity());
        this.interestPubSequence.then(this.interestSubSequence).then(this.interestPubSequence);
        this.clock = configuration.getClock();
        this.connectionLimit = configuration.getActiveConnectionLimit();
        this.timeout = configuration.getIdleConnectionTimeout();
        this.contextFactory = configuration.getIOContextFactory();
        this.epoll = new Epoll(configuration.getEventCapacity());
        this.serverFd = nf.socketTcp(false);
        if (nf.bindTcp(this.serverFd, configuration.getBindIPv4Address(), configuration.getBindPort())) {
            nf.listen(this.serverFd, configuration.getListenBacklog());
            this.epoll.listen(serverFd);
            LOG.info()
                    .$("listening on ")
                    .$(configuration.getBindIPv4Address()).$(':').$(configuration.getBindPort())
                    .$(" [fd=").$(serverFd).$(']').$();
        } else {
            throw new NetworkError("Failed to bind socket");
        }
    }

    @Override
    public void close() {
        this.epoll.close();
        if (nf.close(serverFd) != 0) {
            LOG.error().$("failed to close socket [fd=").$(serverFd).$(", errno=").$(Os.errno()).$(']').$();
        }
        int n = pending.size();
        for (int i = 0; i < n; i++) {
            Misc.free(pending.get(i));
        }

        long cursor = interestSubSequence.next();
        if (cursor > -1) {
            long available = interestSubSequence.available();
            while (cursor < available) {
                final IOEvent<C> evt = interestQueue.get(cursor);
                disconnect((int) evt.context.getFd(), evt.context, DisconnectReason.SILLY);
                cursor++;
            }
        }
    }

    @Override
    public int getConnectionCount() {
        return connectionCount;
    }

    @Override
    public void registerChannel(C context, int operation) {
        long cursor = interestPubSequence.nextBully();
        IOEvent<C> evt = interestQueue.get(cursor);
        evt.context = context;
        evt.operation = operation;
        LOG.debug().$("Re-queuing ").$(context.getFd()).$();
        interestPubSequence.done(cursor);
    }

    private void accept() {
        while (true) {
            long fd = nf.accept(serverFd);

            if (fd < 0) {
                if (nf.errno() != Net.EWOULDBLOCK) {
                    LOG.error().$("could not accept [errno=").$(nf.errno()).$(']').$();
                }
                break;
            }

            if (nf.configureNonBlocking(fd) < 0) {
                LOG.error().$("could not configure non-blocking [fd=").$(fd).$(", errno=").$(Os.errno()).$(']').$();
                closeFd(fd);
                return;
            }


            if (connectionCount > connectionLimit) {
                LOG.info().$("connection limit exceeded [fd=").$(fd).$(']').$();
                closeFd(fd);
                return;
            }

            LOG.info().$("connected [ip=").$ip(nf.getPeerIP(fd)).$(", fd=").$(fd).$(']').$();
            connectionCount++;
            publishOperation(IOOperation.CONNECT, contextFactory.newInstance(fd));
        }
    }

    private void closeFd(long fd) {
        if (nf.close(fd) != 0) {
            LOG.error().$("could not close [fd=").$(fd).$(", errno=").$(nf.errno()).$(']').$();
        }
    }

    private void disconnect(long fd, C context, int disconnectReason) {
        LOG.info()
                .$("disconnected [ip=").$ip(nf.getPeerIP(fd))
                .$(", fd=").$(fd)
                .$(", reason=").$(DisconnectReason.nameOf(disconnectReason))
                .$(']').$();
        closeFd(fd);
        context.close();
        connectionCount--;
    }

    private void enqueuePending(int watermark) {
        for (int i = watermark, sz = pending.size(), offset = 0; i < sz; i++, offset += Epoll.SIZEOF_EVENT) {
            epoll.setOffset(offset);
            if (epoll.control((int) pending.get(i, M_FD), pending.get(i, M_ID), Epoll.EPOLL_CTL_ADD, Epoll.EPOLLIN) < 0) {
                LOG.debug().$("epoll_ctl failure ").$(Os.errno()).$();
            } else {
                LOG.debug().$("epoll_ctl ").$(pending.get(i, M_FD)).$(" as ").$(pending.get(i, M_ID)).$();
            }
        }
    }

    private void processIdleConnections(long deadline) {
        int count = 0;
        for (int i = 0, n = pending.size(); i < n && pending.get(i, M_TIMESTAMP) < deadline; i++, count++) {
            disconnect(
                    pending.get(i, M_FD),
                    pending.get(i),
                    DisconnectReason.IDLE
            );
        }
        pending.zapTop(count);
    }

    private boolean processRegistrations(long timestamp) {
        long cursor;
        boolean useful = false;
        int offset = 0;
        while ((cursor = interestSubSequence.next()) > -1) {
            useful = true;
            IOEvent<C> evt = interestQueue.get(cursor);
            C context = evt.context;
            int channelStatus = evt.operation;
            interestSubSequence.done(cursor);

            int fd = (int) context.getFd();
            final long id = fdid++;
            LOG.debug().$("Registering ").$(fd).$(" status ").$(channelStatus).$(" as ").$(id).$();
            epoll.setOffset(offset);
            offset += Epoll.SIZEOF_EVENT;
            switch (channelStatus) {
                case IOOperation.READ:
                    epoll.control(fd, id, Epoll.EPOLL_CTL_MOD, Epoll.EPOLLIN);
                    break;
                case IOOperation.WRITE:
                    epoll.control(fd, id, Epoll.EPOLL_CTL_MOD, Epoll.EPOLLOUT);
                    break;
                case IOOperation.DISCONNECT:
                    disconnect(fd, context, DisconnectReason.SILLY);
                    continue;
                case IOOperation.CLEANUP:
                    disconnect(fd, context, DisconnectReason.PEER);
                    continue;
                default:
                    break;
            }

            int r = pending.addRow();
            pending.set(r, M_TIMESTAMP, timestamp);
            pending.set(r, M_FD, fd);
            pending.set(r, M_ID, id);
            pending.set(r, context);
        }

        return useful;
    }

    private void publishOperation(int operation, C context) {
        long cursor = ioEventPubSeq.nextBully();
        IOEvent<C> evt = ioEventQueue.get(cursor);
        evt.context = context;
        evt.operation = operation;
        ioEventPubSeq.done(cursor);
        LOG.debug().$("fired [fd=").$(context.getFd()).$(", op=").$(evt.operation).$(']').$();
    }

    @Override
    protected boolean runSerially() {
        boolean useful = false;
        final int n = epoll.poll();
        int watermark = pending.size();
        int offset = 0;
        if (n > 0) {
            // check all activated FDs
            for (int i = 0; i < n; i++) {
                epoll.setOffset(offset);
                offset += Epoll.SIZEOF_EVENT;
                long id = epoll.getData();
                // this is server socket, accept if there aren't too many already
                if (id == 0) {
                    accept();
                } else {
                    // find row in pending for two reasons:
                    // 1. find payload
                    // 2. remove row from pending, remaining rows will be timed out
                    int row = pending.binarySearch(id);
                    if (row < 0) {
                        LOG.error().$("Internal error: unknown ID: ").$(id).$();
                        continue;
                    }

                    publishOperation(
                            (epoll.getEvent() & Epoll.EPOLLIN) > 0 ? IOOperation.READ : IOOperation.WRITE,
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
        final long deadline = timestamp - timeout;
        if (pending.size() > 0 && pending.get(0, M_TIMESTAMP) < deadline) {
            processIdleConnections(deadline);
            useful = true;
        }

        return processRegistrations(timestamp) || useful;
    }
}
