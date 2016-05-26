/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.questdb.net.http;

import com.questdb.ex.NetworkError;
import com.questdb.iter.clock.Clock;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.misc.Files;
import com.questdb.misc.Misc;
import com.questdb.misc.Net;
import com.questdb.misc.Os;
import com.questdb.mp.*;
import com.questdb.net.Epoll;
import com.questdb.net.NetworkChannelImpl;
import com.questdb.std.LongMatrix;

import java.io.IOException;

public class EpollDispatcher extends SynchronizedJob implements IODispatcher {
    private static final int M_TIMESTAMP = 1;
    private static final int M_FD = 2;
    private static final int M_ID = 0;
    private static final Log LOG = LogFactory.getLog(EpollDispatcher.class);
    private final long socketFd;
    private final RingQueue<IOEvent> ioQueue;
    private final Sequence ioSequence;
    private final RingQueue<IOEvent> interestQueue;
    private final MPSequence interestPubSequence;
    private final SCSequence interestSubSequence = new SCSequence();
    private final Clock clock;
    private final ServerConfiguration configuration;
    private final Epoll epoll;
    private final int timeout;
    private final LongMatrix<IOContext> pending = new LongMatrix<>(4);
    private final int maxConnections;
    private int connectionCount = 0;
    private long fdid = 1;

    public EpollDispatcher(
            CharSequence ip,
            int port,
            RingQueue<IOEvent> ioQueue,
            Sequence ioSequence,
            Clock clock,
            ServerConfiguration configuration,
            int capacity
    ) {
        this.ioQueue = ioQueue;
        this.ioSequence = ioSequence;
        this.interestQueue = new RingQueue<>(IOEvent.FACTORY, ioQueue.getCapacity());
        this.interestPubSequence = new MPSequence(interestQueue.getCapacity());
        this.interestPubSequence.followedBy(this.interestSubSequence);
        this.interestSubSequence.followedBy(this.interestPubSequence);
        this.clock = clock;
        this.configuration = configuration;
        this.maxConnections = configuration.getHttpMaxConnections();
        this.timeout = configuration.getHttpTimeout();

        // bind socket
        this.epoll = new Epoll(capacity);
        this.socketFd = Net.socketTcp(false);
        if (Net.bind(this.socketFd, ip, port)) {
            Net.listen(this.socketFd, 128);
            this.epoll.listen(socketFd);
            LOG.debug().$("Listening socket: ").$(socketFd).$();
        } else {
            throw new NetworkError("Failed to find socket");
        }
    }

    @Override
    public void close() throws IOException {
        this.epoll.close();
        Files.close(socketFd);
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
    public void registerChannel(IOContext context, ChannelStatus status) {
        long cursor = interestPubSequence.nextBully();
        IOEvent evt = interestQueue.get(cursor);
        evt.context = context;
        evt.status = status;
        LOG.debug().$("Re-queuing ").$(context.channel.getFd()).$();
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
                LOG.error().$("Cannot make FD non-blocking").$();
                Files.close(_fd);
            }

            connectionCount++;

            if (connectionCount > maxConnections) {
                LOG.info().$("Too many connections, kicking out ").$(_fd).$();
                Files.close(_fd);
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

        NetworkChannelImpl channel = new NetworkChannelImpl(_fd);
        pending.set(r, new IOContext(channel, configuration, clock));
    }

    private void disconnect(IOContext context, DisconnectReason reason) {
        LOG.info().$("Disconnected ").$(context.channel.getFd()).$(": ").$(reason).$();
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
            IOEvent evt = interestQueue.get(cursor);
            IOContext context = evt.context;
            ChannelStatus op = evt.status;
            interestSubSequence.done(cursor);

            int fd = (int) context.channel.getFd();
            LOG.debug().$("Registering ").$(fd).$(" status ").$(op).$();
            epoll.setOffset(offset);
            offset += Epoll.SIZEOF_EVENT;
            final long id = fdid++;
            switch (op) {
                case READ:
                    epoll.control(fd, id, Epoll.EPOLL_CTL_MOD, Epoll.EPOLLIN);
                    break;
                case WRITE:
                    epoll.control(fd, id, Epoll.EPOLL_CTL_MOD, Epoll.EPOLLOUT);
                    break;
                case DISCONNECTED:
                    disconnect(context, DisconnectReason.SILLY);
                    continue;
                case EOF:
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

                    final IOContext context = pending.get(row);
                    long cursor = ioSequence.nextBully();
                    IOEvent evt = ioQueue.get(cursor);
                    evt.context = context;
                    evt.status = (epoll.getEvent() & Epoll.EPOLLIN) > 0 ? ChannelStatus.READ : ChannelStatus.WRITE;
                    ioSequence.done(cursor);
                    LOG.debug().$("Queuing ").$(id).$(" on ").$(context.channel.getFd()).$();
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
