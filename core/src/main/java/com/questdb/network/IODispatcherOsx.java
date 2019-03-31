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
import com.questdb.net.ChannelStatus;
import com.questdb.net.DisconnectReason;
import com.questdb.net.Kqueue;
import com.questdb.std.LongMatrix;
import com.questdb.std.Misc;
import com.questdb.std.Os;
import com.questdb.std.time.MillisecondClock;

public class IODispatcherOsx<C extends IOContext> extends SynchronizedJob implements IODispatcher<C> {
    private static final Log LOG = LogFactory.getLog(IODispatcherOsx.class);

    private static final int M_TIMESTAMP = 0;
    private static final int M_FD = 1;

    private final long serverFd;
    private final RingQueue<IOEvent<C>> ioEventQueue;
    private final Sequence ioEventPubSeq;
    private final Sequence ioEventSubSeq;
    private final RingQueue<IOEvent<C>> interestQueue;
    private final MPSequence interestPubSeq;
    private final SCSequence interestSubSeq = new SCSequence();
    private final MillisecondClock clock;
    private final Kqueue kqueue;
    private final long timeout;
    private final LongMatrix<C> pending = new LongMatrix<>(2);
    private final int activeConnectionLimit;
    private final int capacity;
    private final IOContextFactory<C> ioContextFactory;
    private final NetworkFacade nf;
    private int connectionCount = 0;

    public IODispatcherOsx(
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
        this.interestPubSeq.then(this.interestSubSeq).then(this.interestPubSeq);
        this.clock = configuration.getClock();
        this.activeConnectionLimit = configuration.getActiveConnectionLimit();
        this.timeout = configuration.getIdleConnectionTimeout();
        this.capacity = configuration.getEventCapacity();
        this.ioContextFactory = ioContextFactory;

        // bind socket
        this.kqueue = new Kqueue(capacity);
        this.serverFd = nf.socketTcp(false);
        if (nf.bindTcp(this.serverFd, configuration.getBindIPv4Address(), configuration.getBindPort())) {
            nf.listen(this.serverFd, configuration.getListenBacklog());
            this.kqueue.listen(serverFd);
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
        this.kqueue.close();
        if (nf.close(serverFd) != 0) {
            LOG.error().$("failed to close socket [fd=").$(serverFd).$(", errno=").$(Os.errno()).$(']').$();
        }
        int n = pending.size();
        for (int i = 0; i < n; i++) {
            Misc.free(pending.get(i));
        }

        long cursor = interestSubSeq.next();
        if (cursor > -1) {
            long available = interestSubSeq.available();
            while (cursor < available) {
                final IOEvent<C> evt = interestQueue.get(cursor);
                disconnect(evt.context, DisconnectReason.SILLY);
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
        long cursor = interestPubSeq.nextBully();
        IOEvent<C> evt = interestQueue.get(cursor);
        evt.context = context;
        evt.operation = operation;
        LOG.debug().$("queuing [fd=").$(context.getFd()).$(", op=").$(operation).$(']').$();
        interestPubSeq.done(cursor);
    }

    @Override
    public void processIOQueue(IORequestProcessor<C> processor) {
        long cursor = ioEventSubSeq.next();
        if (cursor > -1) {
            IOEvent<C> event = ioEventQueue.get(cursor);
            C connectionContext = event.context;
            final int operation = event.operation;
            ioEventSubSeq.done(cursor);
            processor.onRequest(operation, connectionContext, this);
        }
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
                LOG.error().$("could not configure non-blocking [fd=").$(fd).$(", errno=").$(Os.errno()).$(']').$();
                closeFd(fd);
                return;
            }

            if (connectionCount > activeConnectionLimit) {
                LOG.info().$("connection limit exceeded [fd=").$(fd).$(']').$();
                closeFd(fd);
                return;
            }

            LOG.info().$("connected [ip=").$ip(nf.getPeerIP(fd)).$(", fd=").$(fd).$(']').$();
            connectionCount++;
            addPending(fd, clock.getTicks());
        }
    }

    private void addPending(long _fd, long timestamp) {
        // append to pending
        // all rows below watermark will be registered with kqueue
        int r = pending.addRow();
        LOG.debug().$(" Matrix row ").$(r).$(" for ").$(_fd).$();
        pending.set(r, 0, timestamp);
        pending.set(r, 1, _fd);
        pending.set(r, ioContextFactory.newInstance(_fd));
    }


    private void closeFd(long fd) {
        if (nf.close(fd) != 0) {
            LOG.error().$("could not close [fd=").$(fd).$(", errno=").$(nf.errno()).$(']').$();
        }
    }

    private void disconnect(C context, int disconnectReason) {
        final long fd = context.getFd();
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
        int index = 0;
        for (int i = watermark, sz = pending.size(), offset = 0; i < sz; i++, offset += Kqueue.SIZEOF_KEVENT) {
            kqueue.setOffset(offset);
            kqueue.readFD((int) pending.get(i, M_FD), pending.get(i, M_TIMESTAMP));
            LOG.debug().$("kqueued ").$(pending.get(i, M_FD)).$(" as ").$(index - 1).$();
            if (++index > capacity - 1) {
                kqueue.register(index);
                index = 0;
            }
        }
        if (index > 0) {
            kqueue.register(index);
            LOG.debug().$("Registered ").$(index).$();
        }
    }

    private int findPending(int fd, long ts) {
        int r = pending.binarySearch(ts);
        if (r < 0) {
            return r;
        }

        if (pending.get(r, M_FD) == fd) {
            return r;
        } else {
            return scanRow(r + 1, fd, ts);
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
        int count = 0;
        int offset = 0;
        while ((cursor = interestSubSeq.next()) > -1) {
            useful = true;
            IOEvent<C> evt = interestQueue.get(cursor);
            C context = evt.context;
            int operation = evt.operation;
            interestSubSeq.done(cursor);

            int fd = (int) context.getFd();
            LOG.debug().$("Registering ").$(fd).$(" status ").$(operation).$();
            kqueue.setOffset(offset);
            offset += Kqueue.SIZEOF_KEVENT;
            count++;
            switch (operation) {
                case IOOperation.READ:
                    kqueue.readFD(fd, timestamp);
                    break;
                case IOOperation.WRITE:
                    kqueue.writeFD(fd, timestamp);
                    break;
                case IOOperation.DISCONNECT:
                    disconnect(context, DisconnectReason.SILLY);
                    continue;
                case IOOperation.CLEANUP:
                    disconnect(context, DisconnectReason.PEER);
                    continue;
                default:
                    break;
            }

            int r = pending.addRow();
            pending.set(r, M_TIMESTAMP, timestamp);
            pending.set(r, M_FD, fd);
            pending.set(r, context);


            if (count > capacity - 1) {
                kqueue.register(count);
                count = 0;
            }
        }

        if (count > 0) {
            kqueue.register(count);
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
        final int n = kqueue.poll();
        int watermark = pending.size();
        int offset = 0;
        if (n > 0) {
            // check all activated FDs
            for (int i = 0; i < n; i++) {
                kqueue.setOffset(offset);
                offset += Kqueue.SIZEOF_KEVENT;
                int fd = kqueue.getFd();
                // this is server socket, accept if there aren't too many already
                if (fd == serverFd) {
                    accept();
                } else {
                    // find row in pending for two reasons:
                    // 1. find payload
                    // 2. remove row from pending, remaining rows will be timed out
                    int row = findPending(fd, kqueue.getData());
                    if (row < 0) {
                        LOG.error().$("Internal error: unknown FD: ").$(fd).$();
                        continue;
                    }

                    if (kqueue.getFlags() == Kqueue.EV_EOF) {
                        disconnect(pending.get(row), DisconnectReason.PEER);
                    } else {
                        publishOperation(kqueue.getFilter() == Kqueue.EVFILT_READ ? ChannelStatus.READ : ChannelStatus.WRITE, pending.get(row));
                        LOG.debug().$("Queuing ").$(kqueue.getFilter()).$(" on ").$(fd).$();
                    }
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

    private int scanRow(int r, int fd, long ts) {
        for (int i = r, n = pending.size(); i < n; i++) {
            // timestamps not match?
            if (pending.get(i, M_TIMESTAMP) != ts) {
                return -(i + 1);
            }

            if (pending.get(i, M_FD) == fd) {
                return i;
            }
        }
        return -1;
    }
}
