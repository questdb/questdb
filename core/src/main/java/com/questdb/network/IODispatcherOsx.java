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

import com.questdb.net.ChannelStatus;
import com.questdb.std.LongMatrix;
import com.questdb.std.Os;

public class IODispatcherOsx<C extends IOContext> extends AbstractIODispatcher<C> {

    private static final int M_TIMESTAMP = 0;
    private static final int M_FD = 1;

    private final Kqueue kqueue;
    private final LongMatrix<C> pending = new LongMatrix<>(2);
    private final int capacity;

    public IODispatcherOsx(
            IODispatcherConfiguration configuration,
            IOContextFactory<C> ioContextFactory
    ) {
        super(configuration, ioContextFactory);
        this.capacity = configuration.getEventCapacity();

        // bind socket
        this.kqueue = new Kqueue(capacity);
        if (nf.bindTcp(this.serverFd, configuration.getBindIPv4Address(), configuration.getBindPort())) {
            nf.listen(this.serverFd, configuration.getListenBacklog());

            if (this.kqueue.listen(serverFd) != 0) {
                throw NetworkError.instance(nf.errno(), "could not kqueue.listen()");
            }

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
        processDisconnects();
        this.kqueue.close();
        nf.close(serverFd, LOG);

        for (int i = 0, n = pending.size(); i < n; i++) {
            doDisconnect(pending.get(i));
        }

        interestSubSeq.consumeAll(interestQueue, this.disconnectContextRef);
        ioEventSubSeq.consumeAll(ioEventQueue, this.disconnectContextRef);
        LOG.info().$("closed").$();
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
        // all rows below watermark will be registered with kqueue
        int r = pending.addRow();
        LOG.debug().$("pending [row=").$(r).$(", fd=").$(fd).$(']').$();
        pending.set(r, M_TIMESTAMP, timestamp);
        pending.set(r, M_FD, fd);
        pending.set(r, ioContextFactory.newInstance(fd));
    }

    private void enqueuePending(int watermark) {
        int index = 0;
        for (int i = watermark, sz = pending.size(), offset = 0; i < sz; i++, offset += KqueueAccessor.SIZEOF_KEVENT) {
            kqueue.setWriteOffset(offset);
            final int fd = (int) pending.get(i, M_FD);
            if (initialBias == IODispatcherConfiguration.BIAS_READ) {
                kqueue.readFD(fd, pending.get(i, M_TIMESTAMP));
                LOG.debug().$("kq [op=1, fd=").$(fd).$(", index=").$(index).$(", offset=").$(offset).$(']').$();
            } else {
                kqueue.writeFD(fd, pending.get(i, M_TIMESTAMP));
                LOG.debug().$("kq [op=2, fd=").$(fd).$(", index=").$(index).$(", offset=").$(offset).$(']').$();
            }
            if (++index > capacity - 1) {
                registerWithKQueue(index);
                index = 0;
                offset = 0;
            }
        }
        if (index > 0) {
            registerWithKQueue(index);
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
            doDisconnect(pending.get(i));
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
            final IOEvent<C> evt = interestQueue.get(cursor);
            C context = evt.context;
            int operation = evt.operation;
            interestSubSeq.done(cursor);

            final int fd = (int) context.getFd();
            LOG.debug().$("registered [fd=").$(fd).$(", op=").$(operation).$(']').$();
            kqueue.setWriteOffset(offset);
            if (operation == IOOperation.READ) {
                kqueue.readFD(fd, timestamp);
            } else {
                kqueue.writeFD(fd, timestamp);
            }

            offset += KqueueAccessor.SIZEOF_KEVENT;
            count++;

            int r = pending.addRow();
            pending.set(r, M_TIMESTAMP, timestamp);
            pending.set(r, M_FD, fd);
            pending.set(r, context);


            if (count > capacity - 1) {
                registerWithKQueue(count);
                count = 0;
                offset = 0;
            }
        }

        if (count > 0) {
            registerWithKQueue(count);
        }

        return useful;
    }

    private void registerWithKQueue(int changeCount) {
        if (kqueue.register(changeCount) != 0) {
            throw NetworkError.instance(Os.errno()).put("could not register [changeCount=").put(changeCount).put(']');
        }
        LOG.debug().$("kqueued [count=").$(changeCount).$(']').$();
    }

    @Override
    protected boolean runSerially() {
        processDisconnects();
        boolean useful = false;
        final int n = kqueue.poll();
        int watermark = pending.size();
        int offset = 0;
        if (n > 0) {
            LOG.debug().$("poll [n=").$(n).$(']').$();
            // check all activated FDs
            for (int i = 0; i < n; i++) {
                kqueue.setReadOffset(offset);
                offset += KqueueAccessor.SIZEOF_KEVENT;
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

                    publishOperation(kqueue.getFilter() == KqueueAccessor.EVFILT_READ ? ChannelStatus.READ : ChannelStatus.WRITE, pending.get(row));
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
