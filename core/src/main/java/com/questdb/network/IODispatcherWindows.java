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
import com.questdb.std.LongIntHashMap;
import com.questdb.std.LongMatrix;
import com.questdb.std.Misc;
import com.questdb.std.Unsafe;
import com.questdb.std.time.MillisecondClock;

import java.util.concurrent.atomic.AtomicInteger;

public class IODispatcherWindows<C extends IOContext> extends SynchronizedJob implements IODispatcher<C> {

    private static final int M_TIMESTAMP = 0;
    private static final int M_FD = 1;
    private static final int M_OPERATION = 2;
    private static final Log LOG = LogFactory.getLog(IODispatcherWindows.class);

    private final FDSet readFdSet;
    private final FDSet writeFdSet;
    private final long serverFd;
    private final RingQueue<IOEvent<C>> interestQueue;
    private final MPSequence interestPubSeq;
    private final SCSequence interestSubSeq;
    private final MillisecondClock clock;
    private final long idleConnectionTimeout;
    private final LongMatrix<C> pending = new LongMatrix<>(4);
    private final int activeConnectionLimit;
    private final LongIntHashMap fds = new LongIntHashMap();
    private final IOContextFactory<C> ioContextFactory;
    private final RingQueue<IOEvent<C>> ioEventQueue;
    private final SPSequence ioEventPubSeq;
    private final MCSequence ioEventSubSeq;
    private final AtomicInteger connectionCount = new AtomicInteger();
    private final NetworkFacade nf;
    private final int initialBias;
    private final SelectFacade sf;

    public IODispatcherWindows(
            IODispatcherConfiguration configuration,
            IOContextFactory<C> ioContextFactory
    ) {
        this.nf = configuration.getNetworkFacade();
        this.readFdSet = new FDSet(configuration.getEventCapacity());
        this.writeFdSet = new FDSet(configuration.getEventCapacity());
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
        this.serverFd = nf.socketTcp(false);
        this.sf = configuration.getSelectFacade();
        if (nf.bindTcp(this.serverFd, configuration.getBindIPv4Address(), configuration.getBindPort())) {
            nf.listen(this.serverFd, configuration.getListenBacklog());
            final int r = pending.addRow();
            pending.set(r, M_TIMESTAMP, clock.getTicks());
            pending.set(r, M_FD, serverFd);
            pending.set(r, M_OPERATION, IOOperation.READ);
            readFdSet.add(serverFd);
            readFdSet.setCount(1);
            writeFdSet.setCount(0);
        } else {
            throw NetworkError.instance(nf.errno()).couldNotBindSocket();
        }
    }

    @Override
    public void close() {
        readFdSet.close();
        writeFdSet.close();

        for (int i = 0, n = pending.size(); i < n; i++) {
            nf.close(pending.get(i, M_FD), LOG);
            Misc.free(pending.get(i));
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

    private void accept(long timestamp) {
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
            addPending(fd, timestamp);
        }
    }

    private void addPending(long fd, long timestamp) {
        int r = pending.addRow();
        LOG.debug().$("pending [row=").$(r).$(", fd=").$(fd).$(']').$();
        pending.set(r, M_TIMESTAMP, timestamp);
        pending.set(r, M_FD, fd);
        pending.set(r, M_OPERATION, initialBias == IODispatcherConfiguration.BIAS_READ ? IOOperation.READ : IOOperation.WRITE);
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

    private boolean processRegistrations(long timestamp) {
        long cursor;
        boolean useful = false;
        while ((cursor = interestSubSeq.next()) > -1) {
            useful = true;
            IOEvent<C> evt = interestQueue.get(cursor);
            C context = evt.context;
            int operation = evt.operation;
            interestSubSeq.done(cursor);

            int r = pending.addRow();
            pending.set(r, M_TIMESTAMP, timestamp);
            pending.set(r, M_FD, context.getFd());
            pending.set(r, M_OPERATION, operation);
            pending.set(r, context);
        }

        return useful;
    }

    private void queryFdSets(long timestamp) {
        for (int i = 0, n = readFdSet.getCount(); i < n; i++) {
            long fd = readFdSet.get(i);

            if (fd == serverFd) {
                accept(timestamp);
            } else {
                fds.put(fd, SelectAccessor.FD_READ);
            }
        }

        // collect writes into hash map
        for (int i = 0, n = writeFdSet.getCount(); i < n; i++) {
            long fd = writeFdSet.get(i);
            int op = fds.get(fd);
            if (op == -1) {
                fds.put(fd, SelectAccessor.FD_WRITE);
            } else {
                fds.put(fd, SelectAccessor.FD_READ | SelectAccessor.FD_WRITE);
            }
        }
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
        int count = sf.select(readFdSet.address, writeFdSet.address, 0);
        if (count < 0) {
            LOG.error().$("Error in select(): ").$(nf.errno()).$();
            return false;
        }

        final long timestamp = clock.getTicks();
        boolean useful = false;
        fds.clear();

        // collect reads into hash map
        if (count > 0) {
            queryFdSets(timestamp);
            useful = true;
        }

        // process returned fds
        useful = processRegistrations(timestamp) | useful;

        // re-arm select() fds
        int readFdCount = 0;
        int writeFdCount = 0;
        readFdSet.reset();
        writeFdSet.reset();
        long deadline = timestamp - idleConnectionTimeout;
        for (int i = 0, n = pending.size(); i < n; ) {
            long ts = pending.get(i, M_TIMESTAMP);
            long fd = pending.get(i, M_FD);
            int _new_op = fds.get(fd);

            if (_new_op == -1) {

                // check if expired
                if (ts < deadline && fd != serverFd) {
                    disconnect(pending.get(i), DisconnectReason.IDLE);
                    pending.deleteRow(i);
                    n--;
                    useful = true;
                    continue;
                }

                if (pending.get(i++, M_OPERATION) == IOOperation.READ) {
                    readFdSet.add(fd);
                    readFdCount++;
                } else {
                    writeFdSet.add(fd);
                    writeFdCount++;
                }
            } else {
                // this fd just has fired
                // publish event
                // and remove from pending
                final C context = pending.get(i);

                if ((_new_op & SelectAccessor.FD_READ) > 0) {
                    publishOperation(IOOperation.READ, context);
                }

                if ((_new_op & SelectAccessor.FD_WRITE) > 0) {
                    publishOperation(IOOperation.WRITE, context);
                }
                pending.deleteRow(i);
                n--;
            }
        }

        readFdSet.setCount(readFdCount);
        writeFdSet.setCount(writeFdCount);
        return useful;
    }

    private static class FDSet {
        private long address;
        private int size;
        private long _wptr;
        private long lim;

        private FDSet(int size) {
            int l = SelectAccessor.ARRAY_OFFSET + 8 * size;
            this.address = Unsafe.malloc(l);
            this.size = size;
            this._wptr = address + SelectAccessor.ARRAY_OFFSET;
            this.lim = address + l;
        }

        private void add(long fd) {
            if (_wptr == lim) {
                resize();
            }
            long p = _wptr;
            Unsafe.getUnsafe().putLong(p, fd);
            _wptr = p + 8;
        }

        private void close() {
            if (address != 0) {
                Unsafe.free(address, lim - address);
                address = 0;
            }
        }

        private long get(int index) {
            return Unsafe.getUnsafe().getLong(address + SelectAccessor.ARRAY_OFFSET + index * 8L);
        }

        private int getCount() {
            return Unsafe.getUnsafe().getInt(address + SelectAccessor.COUNT_OFFSET);
        }

        private void setCount(int count) {
            Unsafe.getUnsafe().putInt(address + SelectAccessor.COUNT_OFFSET, count);
        }

        private void reset() {
            _wptr = address + SelectAccessor.ARRAY_OFFSET;
        }

        private void resize() {
            int sz = size * 2;
            int l = SelectAccessor.ARRAY_OFFSET + 8 * sz;
            long _addr = Unsafe.malloc(l);
            Unsafe.getUnsafe().copyMemory(address, _addr, lim - address);
            Unsafe.free(address, lim - address);
            lim = _addr + l;
            size = sz;
            _wptr = _addr + (_wptr - address);
            address = _addr;
        }
    }
}

