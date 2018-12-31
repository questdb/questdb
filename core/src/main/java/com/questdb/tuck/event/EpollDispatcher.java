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

package com.questdb.tuck.event;

import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.mp.*;
import com.questdb.std.*;
import com.questdb.std.ex.NetworkError;
import com.questdb.std.time.MillisecondClock;

public class EpollDispatcher<C extends Context> extends SynchronizedJob implements Dispatcher<C> {
    private static final int M_TIMESTAMP = 1;
    private static final int M_FD = 2;
    private static final int M_ID = 0;
    private static final Log LOG = LogFactory.getLog(EpollDispatcher.class);
    private final long socketFd;
    private final RingQueue<Event<C>> ioQueue;
    private final Sequence ioSequence;
    private final RingQueue<Event<C>> interestQueue;
    private final MPSequence interestPubSequence;
    private final SCSequence interestSubSequence = new SCSequence();
    private final MillisecondClock clock;
    private final Epoll epoll;
    private final int timeout;
    private final LongMatrix<C> pending = new LongMatrix<>(4);
    private final int maxConnections;
    private final ContextFactory<C> contextFactory;
    private int connectionCount = 0;
    private long fdid = 1;

    public EpollDispatcher(
            CharSequence ip,
            int port,
            int maxConnections,
            int timeout,
            RingQueue<Event<C>> ioQueue,
            Sequence ioSequence,
            MillisecondClock clock,
            int capacity,
            ObjectFactory<Event<C>> eventFactory,
            ContextFactory<C> contextFactory
    ) {
        this.ioQueue = ioQueue;
        this.ioSequence = ioSequence;
        this.interestQueue = new RingQueue<>(eventFactory, ioQueue.getCapacity());
        this.interestPubSequence = new MPSequence(interestQueue.getCapacity());
        this.interestPubSequence.then(this.interestSubSequence).then(this.interestPubSequence);
        this.clock = clock;
        this.maxConnections = maxConnections;
        this.timeout = timeout;
        this.contextFactory = contextFactory;

        // bind socket
        this.epoll = new Epoll(capacity);
        this.socketFd = Net.socketTcp(false);
        if (Net.bindTcp(this.socketFd, ip, port)) {
            Net.listen(this.socketFd, 128);
            this.epoll.listen(socketFd);
            LOG.debug().$("Listening socket: ").$(socketFd).$();
        } else {
            throw new NetworkError("Failed to find socket");
        }
    }

    @Override
    public void close() {
        this.epoll.close();
        if (Net.close(socketFd) != 0) {
            LOG.error().$("failed to close socket [fd=").$(socketFd).$(", errno=").$(Os.errno()).$(']').$();
        }
        int n = pending.size();
        for (int i = 0; i < n; i++) {
            Misc.free(pending.get(i));
        }
    }

    @Override
    public int getConnectionCount() {
        return connectionCount;
    }

    @Override
    public void registerChannel(C context, int channelStatus) {
        long cursor = interestPubSequence.nextBully();
        Event<C> evt = interestQueue.get(cursor);
        evt.context = context;
        evt.channelStatus = channelStatus;
        LOG.debug().$("Re-queuing ").$(context.getFd()).$();
        interestPubSequence.done(cursor);
    }

    private void accept(long timestamp) {
        while (true) {
            long _fd = Net.accept(socketFd);

            if (_fd < 0) {
                if (Os.errno() != Net.EWOULDBLOCK) {
                    LOG.error().$("Error in accept(): ").$(Os.errno()).$();
                }
                break;
            }

            LOG.info().$(" Connected ").$(_fd).$();

            if (Net.configureNonBlocking(_fd) < 0) {
                LOG.error().$("Cannot make FD non-blocking [fd=").$(_fd).$(", errno=").$(Os.errno()).$(']').$();
                if (Net.close(_fd) != 0) {
                    LOG.error().$("failed to close [fd=").$(_fd).$(", errno=").$(Os.errno()).$(']').$();
                }
            }

            connectionCount++;

            if (connectionCount > maxConnections) {
                LOG.info().$("Too many connections, kicking out [fd=").$(_fd).$(']').$();
                if (Net.close(_fd) != 0) {
                    LOG.error().$("failed to close [fd=").$(_fd).$(", errno=").$(Os.errno()).$(']').$();
                }
                connectionCount--;
                return;
            }

            addPending(_fd, timestamp);
        }
    }

    private void addPending(long _fd, long timestamp) {
        // append to pending
        // all rows below watermark will be registered with kqueue
        int r = pending.addRow();
        LOG.debug().$(" Matrix row ").$(r).$(" for ").$(_fd).$();
        pending.set(r, M_TIMESTAMP, timestamp);
        pending.set(r, M_FD, _fd);
        pending.set(r, M_ID, fdid++);

        pending.set(r, contextFactory.newInstance(_fd, clock));
    }

    private void disconnect(C context, int disconnectReason) {
        LOG.info().$("Disconnected ").$ip(context.getIp()).$(" [").$(DisconnectReason.nameOf(disconnectReason)).$(']').$();
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
            disconnect(pending.get(i), DisconnectReason.IDLE);
        }
        pending.zapTop(count);
    }

    private boolean processRegistrations(long timestamp) {
        long cursor;
        boolean useful = false;
        int offset = 0;
        while ((cursor = interestSubSequence.next()) > -1) {
            useful = true;
            Event<C> evt = interestQueue.get(cursor);
            C context = evt.context;
            int channelStatus = evt.channelStatus;
            interestSubSequence.done(cursor);

            int fd = (int) context.getFd();
            final long id = fdid++;
            LOG.debug().$("Registering ").$(fd).$(" status ").$(channelStatus).$(" as ").$(id).$();
            epoll.setOffset(offset);
            offset += Epoll.SIZEOF_EVENT;
            switch (channelStatus) {
                case ChannelStatus.READ:
                    epoll.control(fd, id, Epoll.EPOLL_CTL_MOD, Epoll.EPOLLIN);
                    break;
                case ChannelStatus.WRITE:
                    epoll.control(fd, id, Epoll.EPOLL_CTL_MOD, Epoll.EPOLLOUT);
                    break;
                case ChannelStatus.DISCONNECTED:
                    disconnect(context, DisconnectReason.SILLY);
                    continue;
                case ChannelStatus.EOF:
                    disconnect(context, DisconnectReason.PEER);
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

    @Override
    protected boolean runSerially() {
        boolean useful = false;
        final int n = epoll.poll();
        int watermark = pending.size();
        final long timestamp = clock.getTicks();
        int offset = 0;
        if (n > 0) {
            // check all activated FDs
            for (int i = 0; i < n; i++) {
                epoll.setOffset(offset);
                offset += Epoll.SIZEOF_EVENT;
                long id = epoll.getData();
                // this is server socket, accept if there aren't too many already
                if (id == 0) {
                    accept(timestamp);
                } else {
                    // find row in pending for two reasons:
                    // 1. find payload
                    // 2. remove row from pending, remaining rows will be timed out
                    int row = pending.binarySearch(id);
                    if (row < 0) {
                        LOG.error().$("Internal error: unknown ID: ").$(id).$();
                        continue;
                    }

                    final C context = pending.get(row);
                    long cursor = ioSequence.nextBully();
                    Event<C> evt = ioQueue.get(cursor);
                    evt.context = context;
                    evt.channelStatus = (epoll.getEvent() & Epoll.EPOLLIN) > 0 ? ChannelStatus.READ : ChannelStatus.WRITE;
                    ioSequence.done(cursor);
                    LOG.debug().$("Queuing ").$(id).$(" on ").$(context.getFd()).$(", status: ").$(evt.channelStatus).$();
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
        long deadline = timestamp - timeout;
        if (pending.size() > 0 && pending.get(0, M_TIMESTAMP) < deadline) {
            processIdleConnections(deadline);
            useful = true;
        }

        return processRegistrations(timestamp) || useful;
    }

}
