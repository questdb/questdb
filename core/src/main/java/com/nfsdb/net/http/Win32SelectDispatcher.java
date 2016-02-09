/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.net.http;

import com.nfsdb.ex.NetworkError;
import com.nfsdb.iter.clock.Clock;
import com.nfsdb.log.Log;
import com.nfsdb.log.LogFactory;
import com.nfsdb.misc.Files;
import com.nfsdb.misc.Net;
import com.nfsdb.misc.Os;
import com.nfsdb.misc.Unsafe;
import com.nfsdb.mp.*;
import com.nfsdb.net.NetworkChannelImpl;
import com.nfsdb.net.NonBlockingSecureSocketChannel;
import com.nfsdb.std.IntIntHashMap;
import com.nfsdb.std.LongMatrix;

import java.io.IOException;

public class Win32SelectDispatcher extends SynchronizedJob implements IODispatcher {

    public static final int M_TIMESTAMP = 0;
    public static final int M_FD = 1;
    public static final int M_OPERATION = 2;
    private static final Log LOG = LogFactory.getLog(Win32SelectDispatcher.class);
    private static final int COUNT_OFFSET;
    private static final int ARRAY_OFFSET;
    private static final int FD_READ = 1;
    private static final int FD_WRITE = 2;
    private final FDSet readFdSet = new FDSet(1024);
    private final FDSet writeFdSet = new FDSet(1024);
    private final long socketFd;
    private final RingQueue<IOEvent> ioQueue;
    private final Sequence ioSequence;
    private final RingQueue<IOEvent> interestQueue;
    private final MPSequence interestPubSequence;
    private final SCSequence interestSubSequence = new SCSequence();
    private final Clock clock;
    private final HttpServerConfiguration configuration;
    private final int timeout;
    private final LongMatrix<IOContext> pending = new LongMatrix<>(4);
    private final int maxConnections;
    private final IntIntHashMap fds = new IntIntHashMap();
    private int connectionCount = 0;

    public Win32SelectDispatcher(
            CharSequence ip,
            int port,
            RingQueue<IOEvent> ioQueue,
            Sequence ioSequence,
            Clock clock,
            HttpServerConfiguration configuration
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
        this.socketFd = Net.socketTcp(false);
        if (!Net.bind(this.socketFd, ip, port)) {
            throw new NetworkError("Failed to find socket. System error " + Os.errno());
        }
        Net.listen(this.socketFd, 128);
        int r = pending.addRow();
        pending.set(r, M_TIMESTAMP, System.currentTimeMillis());
        pending.set(r, M_FD, socketFd);
        readFdSet.add(socketFd);
        readFdSet.setCount(1);
        writeFdSet.setCount(0);
    }

    @Override
    public void close() throws IOException {
        readFdSet.close();
        writeFdSet.close();
        Net.close(socketFd);

        for (int i = 0, n = pending.size(); i < n; i++) {
            Net.close(pending.get(i, M_FD));
        }

        pending.zapTop(pending.size());
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

    private static native int select(long readfds, long writefds, long exceptfds);

    private static native int countOffset();

    private static native int arrayOffset();

    @Override
    protected boolean _run() {
        final long timestamp = System.currentTimeMillis();
        int count = select(readFdSet.address, writeFdSet.address, 0);
        if (count < 0) {
            LOG.error().$("Error in select(): ").$(Os.errno()).$();
            return false;
        }

        // collect reads into hash map
        fds.clear();

        if (count > 0) {

            for (int i = 0, n = readFdSet.getCount(); i < n; i++) {
                long fd = readFdSet.get(i);

                if (fd == socketFd) {
                    long _fd = accept();
                    if (_fd < 0) {
                        continue;
                    }
                    addPending(_fd, timestamp);
                } else {
                    fds.put((int) fd, FD_READ);
                }
            }

            // collect writes into hash map
            for (int i = 0, n = writeFdSet.getCount(); i < n; i++) {
                long fd = writeFdSet.get(i);
                int op = fds.get((int) fd);
                if (op == -1) {
                    fds.put((int) fd, FD_WRITE);
                } else {
                    fds.put((int) fd, FD_READ | FD_WRITE);
                }
            }
        }

        // process returned fds
        processRegistrations(timestamp);

        // re-arm select() fds
        int readFdCount = 0;
        int writeFdCount = 0;
        readFdSet.reset();
        writeFdSet.reset();
        long deadline = timestamp - timeout;
        for (int i = 0, n = pending.size(); i < n; ) {
            long fd = pending.get(i, M_FD);
            long ts = pending.get(i, M_TIMESTAMP);
            int _new_op = fds.get((int) fd);

            // expired
            if (ts < deadline && fd != socketFd) {
                pending.deleteRow(i);
                n--;
                continue;
            }

            if (_new_op == -1) {
                // not fired, simply re-arm
                ChannelStatus op = ChannelStatus.values()[(int) pending.get(i, M_OPERATION)];
                switch (op) {
                    case READ:
                        readFdSet.add(fd);
                        readFdCount++;
                        break;
                    case WRITE:
                        writeFdSet.add(fd);
                        writeFdCount++;
                        break;
                }
                i++;
            } else {
                // this fd just has fired
                // publish event
                // and remove from pending
                long cursor = ioSequence.nextBully();
                IOEvent evt = ioQueue.get(cursor);
                evt.context = pending.get(i);
                evt.status = _new_op == FD_READ ? ChannelStatus.READ : ChannelStatus.WRITE;
                ioSequence.done(cursor);
                LOG.debug().$("Queuing ").$(_new_op).$(" on ").$(fd).$();
                pending.deleteRow(i);
                n--;
            }
        }

        readFdSet.setCount(readFdCount);
        writeFdSet.setCount(writeFdCount);

        return true;
    }

    private long accept() {
        long _fd = Net.accept(socketFd);
        LOG.debug().$(" Connected ").$(_fd).$();

        // something not right
        if (_fd < 0) {
            LOG.error().$("Error in accept: ").$(_fd).$();
            return -1;
        }

        if (Net.configureNonBlocking(_fd) < 0) {
            LOG.error().$("Cannot make FD non-blocking").$();
            Files.close(_fd);
            return -1;
        }

        connectionCount++;

        if (connectionCount > maxConnections) {
            LOG.info().$("Too many connections, kicking out ").$(_fd).$();
            Files.close(_fd);
            connectionCount--;
            return -1;
        }

        return _fd;
    }

    private void addPending(long _fd, long timestamp) {
        int r = pending.addRow();
        LOG.debug().$(" Matrix row ").$(r).$(" for ").$(_fd).$();
        pending.set(r, M_TIMESTAMP, timestamp);
        pending.set(r, M_FD, _fd);
        pending.set(r, M_OPERATION, ChannelStatus.READ.ordinal());
        NetworkChannelImpl channel = new NetworkChannelImpl(_fd);
        pending.set(r, new IOContext(
                        configuration.getSslConfig().isSecure() ?
                                new NonBlockingSecureSocketChannel(channel, configuration.getSslConfig()) :
                                channel,
                        clock,
                        configuration.getHttpBufReqHeader(),
                        configuration.getHttpBufReqContent(),
                        configuration.getHttpBufReqMultipart(),
                        configuration.getHttpBufRespHeader(),
                        configuration.getHttpBufRespContent()
                )
        );
    }

    private boolean processRegistrations(long timestamp) {
        long cursor;
        boolean useful = false;
        while ((cursor = interestSubSequence.next()) > -1) {
            useful = true;
            IOEvent evt = interestQueue.get(cursor);
            IOContext context = evt.context;
            ChannelStatus op = evt.status;
            interestSubSequence.done(cursor);

            int r = pending.addRow();
            pending.set(r, M_TIMESTAMP, timestamp);
            pending.set(r, M_FD, context.channel.getFd());
            pending.set(r, M_OPERATION, op.ordinal());
            pending.set(r, context);
        }

        return useful;
    }

    private static class FDSet {
        private long address;
        private int size;
        private long _wptr;
        private long lim;

        private FDSet(int size) {
            int l = ARRAY_OFFSET + 8 * size;
            this.address = Unsafe.getUnsafe().allocateMemory(l);
            this.size = size;
            this._wptr = address + ARRAY_OFFSET;
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
                Unsafe.getUnsafe().freeMemory(address);
                address = 0;
            }
        }

        private long get(int index) {
            return Unsafe.getUnsafe().getLong(address + ARRAY_OFFSET + index * 8);
        }

        private int getCount() {
            return Unsafe.getUnsafe().getInt(address + COUNT_OFFSET);
        }

        private void setCount(int count) {
            Unsafe.getUnsafe().putInt(address + COUNT_OFFSET, count);
        }

        private void reset() {
            _wptr = address + ARRAY_OFFSET;
        }

        private void resize() {
            int sz = size * 2;
            int l = ARRAY_OFFSET + 8 * sz;
            long _addr = Unsafe.getUnsafe().allocateMemory(l);
            Unsafe.getUnsafe().copyMemory(address, _addr, lim - address);
            lim = _addr + l;
            size = sz;
            _wptr = _addr + (_wptr - address);
            Unsafe.getUnsafe().freeMemory(address);
            address = _addr;
        }
    }

    static {
        Os.init();

        ARRAY_OFFSET = arrayOffset();
        COUNT_OFFSET = countOffset();
    }
}

