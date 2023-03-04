/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

public class IODispatcherWindows<C extends IOContext<C>> extends AbstractIODispatcher<C> {
    private final LongIntHashMap fds = new LongIntHashMap();
    private final FDSet readFdSet;
    private final SelectFacade sf;
    private final FDSet writeFdSet;
    // used for heartbeats
    private long idSeq = 1;
    private boolean listenerRegistered;

    public IODispatcherWindows(
            IODispatcherConfiguration configuration,
            IOContextFactory<C> ioContextFactory
    ) {
        super(configuration, ioContextFactory);
        this.sf = configuration.getSelectFacade();
        this.readFdSet = new FDSet(configuration.getEventCapacity());
        this.writeFdSet = new FDSet(configuration.getEventCapacity());
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

    private long nextOpId() {
        return idSeq++;
    }

    private boolean processRegistrations(long timestamp) {
        long cursor;
        boolean useful = false;
        while ((cursor = interestSubSeq.next()) > -1) {
            final IOEvent<C> evt = interestQueue.get(cursor);
            final C context = evt.context;
            int operation = evt.operation;
            final long srcOpId = context.getAndResetHeartbeatId();

            final long opId = nextOpId();
            final int fd = context.getFd();

            interestSubSeq.done(cursor);
            if (operation == IOOperation.HEARTBEAT) {
                assert srcOpId != -1;

                int heartbeatRow = pendingHeartbeats.binarySearch(srcOpId, OPM_ID);
                if (heartbeatRow < 0) {
                    continue; // The connection is already closed.
                } else {
                    operation = (int) pendingHeartbeats.get(heartbeatRow, OPM_OPERATION);

                    LOG.debug().$("processing heartbeat registration [fd=").$(fd)
                            .$(", op=").$(operation)
                            .$(", srcId=").$(srcOpId)
                            .$(", id=").$(opId).I$();

                    int r = pending.addRow();
                    pending.set(r, OPM_CREATE_TIMESTAMP, pendingHeartbeats.get(heartbeatRow, OPM_CREATE_TIMESTAMP));
                    pending.set(r, OPM_HEARTBEAT_TIMESTAMP, timestamp);
                    pending.set(r, OPM_FD, fd);
                    pending.set(r, OPM_ID, opId);
                    pending.set(r, OPM_OPERATION, operation);
                    pending.set(r, context);

                    pendingHeartbeats.deleteRow(heartbeatRow);
                }
            } else {
                LOG.debug().$("processing registration [fd=").$(fd)
                        .$(", op=").$(operation)
                        .$(", id=").$(opId).I$();

                int r = pending.addRow();
                pending.set(r, OPM_CREATE_TIMESTAMP, timestamp);
                pending.set(r, OPM_HEARTBEAT_TIMESTAMP, timestamp);
                pending.set(r, OPM_FD, context.getFd());
                pending.set(r, OPM_ID, opId);
                pending.set(r, OPM_OPERATION, operation);
                pending.set(r, context);
            }
            useful = true;
        }
        return useful;
    }

    private void queryFdSets(long timestamp) {
        // collect reads into hash map
        for (int i = 0, n = readFdSet.getCount(); i < n; i++) {
            final long fd = readFdSet.get(i);
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
    protected void pendingAdded(int index) {
        pending.set(index, OPM_OPERATION, initialBias == IODispatcherConfiguration.BIAS_READ ? IOOperation.READ : IOOperation.WRITE);
    }

    @Override
    protected void registerListenerFd() {
        listenerRegistered = true;
    }

    @Override
    protected boolean runSerially() {
        final long timestamp = clock.getTicks();
        processDisconnects(timestamp);

        int count;
        if (readFdSet.getCount() > 0 || writeFdSet.getCount() > 0) {
            count = sf.select(readFdSet.address, writeFdSet.address, 0);
            if (count < 0) {
                LOG.error().$("select failure [err=").$(nf.errno()).I$();
                return false;
            }
        } else {
            count = 0;
        }

        boolean useful = false;
        fds.clear();
        int watermark = pending.size();
        // collect reads into hash map
        if (count > 0) {
            queryFdSets(timestamp);
            useful = true;
        }

        // process returned fds
        useful |= processRegistrations(timestamp);

        // re-arm select() fds
        int readFdCount = 0;
        int writeFdCount = 0;
        readFdSet.reset();
        writeFdSet.reset();
        long deadline = timestamp - idleConnectionTimeout;
        final long heartbeatTimestamp = timestamp - heartbeatIntervalMs;
        for (int i = 0, n = pending.size(); i < n; ) {
            final C context = pending.get(i);

            // check if the context is waiting for a suspend event
            final SuspendEvent suspendEvent = context.getSuspendEvent();
            if (suspendEvent != null) {
                if (suspendEvent.checkTriggered() || suspendEvent.isDeadlineMet(timestamp)) {
                    // the event has been triggered or expired already, clear it and proceed
                    context.clearSuspendEvent();
                }
            }

            final int fd = (int) pending.get(i, OPM_FD);
            final int newOp = fds.get(fd);
            assert fd != serverFd;

            if (newOp == -1) {
                // new operation case

                // check if the connection was idle for too long
                if (pending.get(i, OPM_CREATE_TIMESTAMP) < deadline) {
                    doDisconnect(context, DISCONNECT_SRC_IDLE);
                    pending.deleteRow(i);
                    n--;
                    if (i < watermark) {
                        watermark--;
                    }
                    useful = true;
                    continue;
                }

                // check if we have heartbeats to be sent
                if (i < watermark && pending.get(i, OPM_HEARTBEAT_TIMESTAMP) < heartbeatTimestamp) {
                    final long opId = pending.get(i, OPM_ID);
                    context.setHeartbeatId(opId);
                    publishOperation(IOOperation.HEARTBEAT, context);

                    int r = pendingHeartbeats.addRow();
                    pendingHeartbeats.set(r, OPM_CREATE_TIMESTAMP, pending.get(i, OPM_CREATE_TIMESTAMP));
                    pendingHeartbeats.set(r, OPM_FD, fd);
                    pendingHeartbeats.set(r, OPM_ID, opId);
                    pendingHeartbeats.set(r, OPM_OPERATION, pending.get(i, OPM_OPERATION));
                    pendingHeartbeats.set(r, context);

                    pending.deleteRow(i);
                    n--;
                    watermark--;
                    useful = true;
                } else {
                    int operation = (int) pending.get(i, OPM_OPERATION);
                    i++;
                    if (suspendEvent != null) {
                        // if the operation was suspended, we request a read to be able to detect a client disconnect
                        operation = IOOperation.READ;
                    }

                    if (operation == IOOperation.READ) {
                        readFdSet.add(fd);
                        readFdCount++;
                    } else {
                        writeFdSet.add(fd);
                        writeFdCount++;
                    }
                }
            } else {
                // select()'ed operation case
                if (suspendEvent != null) {
                    // the event is still pending, check if we have a client disconnect
                    if (testConnection(context.getFd())) {
                        doDisconnect(context, DISCONNECT_SRC_PEER_DISCONNECT);
                        pending.deleteRow(i);
                        n--;
                        watermark--;
                    } else {
                        i++; // just skip to the next operation
                    }
                    continue;
                }

                // publish event and remove from pending
                useful = true;

                if ((newOp & SelectAccessor.FD_READ) > 0) {
                    publishOperation(IOOperation.READ, context);
                }
                if ((newOp & SelectAccessor.FD_WRITE) > 0) {
                    publishOperation(IOOperation.WRITE, context);
                }

                pending.deleteRow(i);
                n--;
                watermark--;
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

    @Override
    protected void unregisterListenerFd() {
        listenerRegistered = false;
    }

    private static class FDSet {
        private long _wptr;
        private long address;
        private long lim;
        private int size;

        private FDSet(int size) {
            int l = SelectAccessor.ARRAY_OFFSET + 8 * size;
            this.address = Unsafe.malloc(l, MemoryTag.NATIVE_IO_DISPATCHER_RSS);
            this.size = size;
            this._wptr = address + SelectAccessor.ARRAY_OFFSET;
            this.lim = address + l;
        }

        private void add(int fd) {
            if (_wptr == lim) {
                resize();
            }
            long p = _wptr;
            Unsafe.getUnsafe().putLong(p, fd);
            _wptr = p + 8;
        }

        private void close() {
            if (address != 0) {
                address = Unsafe.free(address, lim - address, MemoryTag.NATIVE_IO_DISPATCHER_RSS);
            }
        }

        private long get(int index) {
            return Unsafe.getUnsafe().getLong(address + SelectAccessor.ARRAY_OFFSET + index * 8L);
        }

        private int getCount() {
            return Unsafe.getUnsafe().getInt(address + SelectAccessor.COUNT_OFFSET);
        }

        private void reset() {
            _wptr = address + SelectAccessor.ARRAY_OFFSET;
        }

        private void resize() {
            int sz = size * 2;
            int l = SelectAccessor.ARRAY_OFFSET + 8 * sz;
            long _addr = Unsafe.malloc(l, MemoryTag.NATIVE_IO_DISPATCHER_RSS);
            Vect.memcpy(_addr, address, lim - address);
            Unsafe.free(address, lim - address, MemoryTag.NATIVE_IO_DISPATCHER_RSS);
            lim = _addr + l;
            size = sz;
            _wptr = _addr + (_wptr - address);
            address = _addr;
        }

        private void setCount(int count) {
            Unsafe.getUnsafe().putInt(address + SelectAccessor.COUNT_OFFSET, count);
        }
    }
}

