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

import com.questdb.std.Os;

public class IODispatcherOsx<C extends IOContext> extends AbstractIODispatcher<C> {

    private final Kqueue kqueue;
    private final int capacity;

    public IODispatcherOsx(
            IODispatcherConfiguration configuration,
            IOContextFactory<C> ioContextFactory
    ) {
        super(configuration, ioContextFactory);
        this.capacity = configuration.getEventCapacity();

        // bind socket
        this.kqueue = new Kqueue(capacity);
        if (this.kqueue.listen(serverFd) != 0) {
            throw NetworkError.instance(nf.errno(), "could not kqueue.listen()");
        }
        logSuccess(configuration);
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

    @Override
    public void close() {
        super.close();
        this.kqueue.close();
        LOG.info().$("closed").$();
    }

    @Override
    protected void pendingAdded(int index) {
        // nothing to do
    }

    private int findPending(int fd, long ts) {
        int r = pending.binarySearch(ts, M_TIMESTAMP);
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
        final long timestamp = clock.getTicks();
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
                    accept(timestamp);
                } else {
                    // find row in pending for two reasons:
                    // 1. find payload
                    // 2. remove row from pending, remaining rows will be timed out
                    int row = findPending(fd, kqueue.getData());
                    if (row < 0) {
                        LOG.error().$("Internal error: unknown FD: ").$(fd).$();
                        continue;
                    }

                    publishOperation(kqueue.getFilter() == KqueueAccessor.EVFILT_READ ? IOOperation.READ : IOOperation.WRITE, pending.get(row));
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
