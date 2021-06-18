/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.network;

import io.questdb.std.LongIntHashMap;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

public class IODispatcherWindows<C extends IOContext> extends AbstractIODispatcher<C> {
    private static final int M_OPERATION = 2;
    private final FDSet readFdSet;
    private final FDSet writeFdSet;
    private final LongIntHashMap fds = new LongIntHashMap();
    private final SelectFacade sf;
    private boolean listenerRegistered;

    public IODispatcherWindows(
            IODispatcherConfiguration configuration,
            IOContextFactory<C> ioContextFactory
    ) {
        super(configuration, ioContextFactory);
        this.readFdSet = new FDSet(configuration.getEventCapacity());
        this.writeFdSet = new FDSet(configuration.getEventCapacity());
        this.sf = configuration.getSelectFacade();
        readFdSet.add(serverFd);
        readFdSet.setCount(1);
        writeFdSet.setCount(0);
        listenerRegistered = true;
    }

    @Override
    public void close() {
        super.close();
        readFdSet.close();
        writeFdSet.close();
        LOG.info().$("closed").$();
    }

    @Override
    protected void pendingAdded(int index) {
        pending.set(index, M_OPERATION, initialBias == IODispatcherConfiguration.BIAS_READ ? IOOperation.READ : IOOperation.WRITE);
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
            final long fd = writeFdSet.get(i);
            final int index = fds.keyIndex(fd);
            if (fds.valueAt(index) == -1) {
                fds.putAt(index, fd, SelectAccessor.FD_WRITE);
            } else {
                fds.putAt(index, fd, SelectAccessor.FD_READ | SelectAccessor.FD_WRITE);
            }
        }
    }

    @Override
    protected boolean runSerially() {

        final long timestamp = clock.getTicks();
        processDisconnects(timestamp);

        int count;
        if (readFdSet.getCount() > 0 || writeFdSet.getCount() > 0) {
            count = sf.select(readFdSet.address, writeFdSet.address, 0);
            if (count < 0) {
                LOG.error().$("Error in select(): ").$(nf.errno()).$();
                return false;
            }
        } else {
            count = 0;
        }

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
            final long ts = pending.get(i, M_TIMESTAMP);
            final long fd = pending.get(i, M_FD);
            final int _new_op = fds.get(fd);
            assert fd != serverFd;
            
            if (_new_op == -1) {

                // check if expired
                if (ts < deadline) {
                    doDisconnect(pending.get(i), DISCONNECT_SRC_IDLE);
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

        if (listenerRegistered) {
             assert serverFd >= 0;
             readFdSet.add(serverFd);
             readFdCount++;
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

        // todo: this method is untested
        private void resize() {
            int sz = size * 2;
            int l = SelectAccessor.ARRAY_OFFSET + 8 * sz;
            long _addr = Unsafe.malloc(l);
            Vect.memcpy(address, _addr, lim - address);
            Unsafe.free(address, lim - address);
            lim = _addr + l;
            size = sz;
            _wptr = _addr + (_wptr - address);
            address = _addr;
        }
    }

    @Override
    protected void registerListenerFd() {
        listenerRegistered = true;
    }

    @Override
    protected void unregisterListenerFd() {
        listenerRegistered = false;
    }
}

