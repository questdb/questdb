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

import io.questdb.std.LongMatrix;

public class IODispatcherOsx<C extends IOContext<C>> extends AbstractIODispatcher<C> {
    private static final int EVM_DEADLINE = 1;
    private static final int EVM_ID = 0;
    private static final int EVM_OPERATION_ID = 2;
    protected final LongMatrix pendingEvents = new LongMatrix(3);
    private final int capacity;
    private final Kqueue kqueue;
    // the final ids are shifted by 1 bit which is reserved to distinguish socket operations (0) and suspend events (1)
    private long idSeq = 1;

    public IODispatcherOsx(
            IODispatcherConfiguration configuration,
            IOContextFactory<C> ioContextFactory
    ) {
        super(configuration, ioContextFactory);
        this.capacity = configuration.getEventCapacity();
        // bind socket
        this.kqueue = new Kqueue(configuration.getKqueueFacade(), capacity);
        registerListenerFd();
    }

    @Override
    public void close() {
        super.close();
        this.kqueue.close();
        LOG.info().$("closed").$();
    }

    private static boolean isEventId(long id) {
        return (id & 1) == 1;
    }

    private void doDisconnect(C context, long id, int reason) {
        final SuspendEvent suspendEvent = context.getSuspendEvent();
        if (suspendEvent != null) {
            // yes, we can do a binary search over EVM_OPERATION_ID since
            // these ref ids are monotonically growing
            int eventRow = pendingEvents.binarySearch(id, EVM_OPERATION_ID);
            if (eventRow < 0) {
                LOG.critical().$("internal error: suspend event not found [id=").$(id).I$();
            } else {
                kqueue.setWriteOffset(0);
                kqueue.removeReadFD(suspendEvent.getFd());
                registerWithKQueue(1);
                pendingEvents.deleteRow(eventRow);
            }
        }
        doDisconnect(context, reason);
    }

    private void enqueuePending(int watermark) {
        int index = 0;
        for (int i = watermark, sz = pending.size(), offset = 0; i < sz; i++, offset += KqueueAccessor.SIZEOF_KEVENT) {
            kqueue.setWriteOffset(offset);

            final int fd = (int) pending.get(i, OPM_FD);
            long id = pending.get(i, OPM_ID);
            final int operation = initialBias == IODispatcherConfiguration.BIAS_READ ? IOOperation.READ : IOOperation.WRITE;
            if (operation == IOOperation.READ) {
                kqueue.readFD(fd, id);
            } else {
                kqueue.writeFD(fd, id);
            }
            pending.set(i, OPM_OPERATION, operation);
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

    private boolean handleSocketOperation(long id) {
        // find row in pending for two reasons:
        // 1. find payload
        // 2. remove row from pending, remaining rows will be timed out
        final int row = pending.binarySearch(id, OPM_ID);
        if (row < 0) {
            LOG.critical().$("internal error: kqueue returned unexpected id [id=").$(id).I$();
            return false;
        }

        final C context = pending.get(row);
        final SuspendEvent suspendEvent = context.getSuspendEvent();
        if (suspendEvent != null) {
            // the operation is suspended, check if we have a client disconnect
            if (testConnection(context.getFd())) {
                doDisconnect(context, id, DISCONNECT_SRC_PEER_DISCONNECT);
                pending.deleteRow(row);
                return true;
            }
        } else {
            publishOperation(
                    kqueue.getFilter() == KqueueAccessor.EVFILT_READ ? IOOperation.READ : IOOperation.WRITE,
                    context
            );
            pending.deleteRow(row);
            return true;
        }
        return false;
    }

    private void handleSuspendEvent(long id) {
        final int eventsRow = pendingEvents.binarySearch(id, EVM_ID);
        if (eventsRow < 0) {
            LOG.critical().$("internal error: kqueue returned unexpected event id [eventId=").$(id).I$();
            return;
        }

        final long opId = pendingEvents.get(eventsRow, EVM_OPERATION_ID);
        final int row = pending.binarySearch(opId, OPM_ID);
        if (row < 0) {
            LOG.critical().$("internal error: suspended operation not found [id=").$(opId).$(", eventId=").$(id).I$();
            return;
        }

        final long eventId = pendingEvents.get(eventsRow, EVM_ID);
        final int operation = (int) pending.get(row, OPM_OPERATION);
        final C context = pending.get(row);
        final SuspendEvent suspendEvent = context.getSuspendEvent();
        assert suspendEvent != null;

        LOG.debug().$("handling triggered suspend event and resuming original operation [fd=").$(context.getFd())
                .$(", opId=").$(opId)
                .$(", eventId=").$(eventId).I$();

        context.clearSuspendEvent();
        kqueue.setWriteOffset(0);
        if (operation == IOOperation.READ) {
            kqueue.readFD(context.getFd(), opId);
        } else {
            kqueue.writeFD(context.getFd(), opId);
        }
        registerWithKQueue(1);

        pendingEvents.deleteRow(eventsRow);
    }

    private long nextEventId() {
        return (idSeq++ << 1) + 1;
    }

    private long nextOpId() {
        return idSeq++ << 1;
    }

    private void processHeartbeats(int watermark, long timestamp) {
        int count = 0;
        for (int i = 0; i < watermark && pending.get(i, OPM_HEARTBEAT_TIMESTAMP) < timestamp; i++, count++) {
            final C context = pending.get(i);

            // De-register pending operation from epoll. We'll register it later when we get a heartbeat pong.
            int fd = context.getFd();
            final long opId = pending.get(i, OPM_ID);
            kqueue.setWriteOffset(0);
            long op = context.getSuspendEvent() != null ? IOOperation.READ : pending.get(i, OPM_OPERATION);
            if (op == IOOperation.READ) {
                kqueue.removeReadFD(fd);
            } else {
                kqueue.removeWriteFD(fd);
            }
            if (kqueue.register(1) != 0) {
                LOG.critical().$("internal error: kqueue remove fd failure [fd=").$(fd)
                        .$(", err=").$(nf.errno()).I$();
            } else {
                context.setHeartbeatId(opId);
                publishOperation(IOOperation.HEARTBEAT, context);

                final int operation = (int) pending.get(i, OPM_OPERATION);
                int r = pendingHeartbeats.addRow();
                pendingHeartbeats.set(r, OPM_CREATE_TIMESTAMP, pending.get(i, OPM_CREATE_TIMESTAMP));
                pendingHeartbeats.set(r, OPM_FD, fd);
                pendingHeartbeats.set(r, OPM_ID, opId);
                pendingHeartbeats.set(r, OPM_OPERATION, operation);
                pendingHeartbeats.set(r, context);

                LOG.debug().$("published heartbeat [fd=").$(fd)
                        .$(", op=").$(operation)
                        .$(", id=").$(opId).I$();
            }

            final SuspendEvent suspendEvent = context.getSuspendEvent();
            if (suspendEvent != null) {
                // Also, de-register suspend event from epoll.
                int eventRow = pendingEvents.binarySearch(opId, EVM_OPERATION_ID);
                if (eventRow < 0) {
                    LOG.critical().$("internal error: suspend event not found on heartbeat [id=").$(opId).I$();
                } else {
                    final long eventId = pendingEvents.get(eventRow, EVM_ID);
                    kqueue.setWriteOffset(0);
                    kqueue.readFD(context.getFd(), eventId);
                    registerWithKQueue(1);
                    pendingEvents.deleteRow(eventRow);
                }
            }
        }
        pending.zapTop(count);
    }

    private int processIdleConnections(long deadline) {
        int count = 0;
        for (int i = 0, n = pending.size(); i < n && pending.get(i, OPM_CREATE_TIMESTAMP) < deadline; i++, count++) {
            doDisconnect(pending.get(i), pending.get(i, OPM_ID), DISCONNECT_SRC_IDLE);
        }
        pending.zapTop(count);
        return count;
    }

    private boolean processRegistrations(long timestamp) {
        long cursor;
        boolean useful = false;
        int count = 0;
        int offset = 0;
        while ((cursor = interestSubSeq.next()) > -1) {
            final IOEvent<C> event = interestQueue.get(cursor);
            final C context = event.context;
            final int requestedOperation = event.operation;
            final long srcOpId = context.getAndResetHeartbeatId();
            interestSubSeq.done(cursor);

            useful = true;
            long opId = nextOpId();
            final int fd = context.getFd();

            int operation = requestedOperation;
            final SuspendEvent suspendEvent = context.getSuspendEvent();
            if (requestedOperation == IOOperation.HEARTBEAT) {
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

                int opRow = pending.addRow();
                pending.set(opRow, OPM_CREATE_TIMESTAMP, timestamp);
                pending.set(opRow, OPM_HEARTBEAT_TIMESTAMP, timestamp);
                pending.set(opRow, OPM_FD, fd);
                pending.set(opRow, OPM_ID, opId);
                pending.set(opRow, OPM_OPERATION, requestedOperation);
                pending.set(opRow, context);
            }

            if (suspendEvent != null) {
                // if the operation was suspended, we request a read to be able to detect a client disconnect
                operation = IOOperation.READ;
                // ok, the operation was suspended, so we need to track the suspend event
                final long eventId = nextEventId();
                LOG.debug().$("registering suspend event [fd=").$(fd)
                        .$(", op=").$(operation)
                        .$(", eventId=").$(eventId)
                        .$(", suspendedOpId=").$(opId)
                        .$(", deadline=").$(suspendEvent.getDeadline()).I$();

                int eventRow = pendingEvents.addRow();
                pendingEvents.set(eventRow, EVM_ID, eventId);
                pendingEvents.set(eventRow, EVM_OPERATION_ID, opId);
                pendingEvents.set(eventRow, EVM_DEADLINE, suspendEvent.getDeadline());

                kqueue.setWriteOffset(offset);
                kqueue.readFD(suspendEvent.getFd(), eventId);
                offset += KqueueAccessor.SIZEOF_KEVENT;
                if (++count > capacity - 1) {
                    registerWithKQueue(count);
                    count = offset = 0;
                }
            }

            kqueue.setWriteOffset(offset);
            if (operation == IOOperation.READ) {
                kqueue.readFD(fd, opId);
            } else {
                kqueue.writeFD(fd, opId);
            }
            offset += KqueueAccessor.SIZEOF_KEVENT;
            if (++count > capacity - 1) {
                registerWithKQueue(count);
                count = offset = 0;
            }
        }

        if (count > 0) {
            registerWithKQueue(count);
        }

        return useful;
    }

    private void processSuspendEventDeadlines(long timestamp) {
        int index = 0;
        int offset = 0;
        int count = 0;
        for (int i = 0, n = pendingEvents.size(); i < n && pendingEvents.get(i, EVM_DEADLINE) < timestamp; i++, count++) {
            final long opId = pendingEvents.get(i, EVM_OPERATION_ID);
            final int pendingRow = pending.binarySearch(opId, OPM_ID);
            if (pendingRow < 0) {
                LOG.critical().$("internal error: failed to find operation for expired suspend event [id=").$(opId).I$();
                continue;
            }

            // First, remove the suspend event from kqueue tracking.
            final C context = pending.get(pendingRow);
            final int operation = (int) pending.get(pendingRow, OPM_OPERATION);
            final SuspendEvent suspendEvent = context.getSuspendEvent();
            assert suspendEvent != null;
            kqueue.setWriteOffset(offset);
            kqueue.removeReadFD(suspendEvent.getFd());
            offset += KqueueAccessor.SIZEOF_KEVENT;
            if (++index > capacity - 1) {
                registerWithKQueue(index);
                index = offset = 0;
            }

            // Next, close the event and resume the original operation.
            context.clearSuspendEvent();
            kqueue.setWriteOffset(offset);
            if (operation == IOOperation.READ) {
                kqueue.readFD(context.getFd(), opId);
            } else {
                kqueue.writeFD(context.getFd(), opId);
            }
            offset += KqueueAccessor.SIZEOF_KEVENT;
            if (++index > capacity - 1) {
                registerWithKQueue(index);
                index = offset = 0;
            }
        }
        if (index > 0) {
            registerWithKQueue(index);
        }
        pendingEvents.zapTop(count);
    }

    private void registerWithKQueue(int changeCount) {
        if (kqueue.register(changeCount) != 0) {
            throw NetworkError.instance(nf.errno()).put("could not register [changeCount=").put(changeCount).put(']');
        }
        LOG.debug().$("kqueued [count=").$(changeCount).$(']').$();
    }

    @Override
    protected void pendingAdded(int index) {
        pending.set(index, OPM_ID, nextOpId());
    }

    @Override
    protected void registerListenerFd() {
        if (this.kqueue.listen(serverFd) != 0) {
            throw NetworkError.instance(nf.errno(), "could not kqueue.listen()");
        }
    }

    @Override
    protected boolean runSerially() {
        boolean useful = false;

        final long timestamp = clock.getTicks();
        processDisconnects(timestamp);
        final int n = kqueue.poll();
        int watermark = pending.size();
        int offset = 0;
        if (n > 0) {
            // check all activated FDs
            LOG.debug().$("poll [n=").$(n).$(']').$();
            for (int i = 0; i < n; i++) {
                kqueue.setReadOffset(offset);
                offset += KqueueAccessor.SIZEOF_KEVENT;
                final int fd = kqueue.getFd();
                final long id = kqueue.getData();
                // this is server socket, accept if there aren't too many already
                if (fd == serverFd) {
                    accept(timestamp);
                    useful = true;
                    continue;
                }
                if (isEventId(id)) {
                    handleSuspendEvent(id);
                    continue;
                }
                if (handleSocketOperation(id)) {
                    useful = true;
                    watermark--;
                }
            }
        }

        // process rows over watermark (new connections)
        if (watermark < pending.size()) {
            enqueuePending(watermark);
        }

        // process timed out suspend events and resume the original operations
        if (pendingEvents.size() > 0 && pendingEvents.get(0, EVM_DEADLINE) < timestamp) {
            processSuspendEventDeadlines(timestamp);
        }

        // process timed out connections
        final long idleTimestamp = timestamp - idleConnectionTimeout;
        if (pending.size() > 0 && pending.get(0, OPM_CREATE_TIMESTAMP) < idleTimestamp) {
            watermark -= processIdleConnections(idleTimestamp);
            useful = true;
        }

        // process heartbeat timers
        final long heartbeatTimestamp = timestamp - heartbeatIntervalMs;
        if (watermark > 0 && pending.get(0, OPM_HEARTBEAT_TIMESTAMP) < heartbeatTimestamp) {
            processHeartbeats(watermark, heartbeatTimestamp);
            useful = true;
        }

        return processRegistrations(timestamp) || useful;
    }

    @Override
    protected void unregisterListenerFd() {
        if (this.kqueue.removeListen(serverFd) != 0) {
            throw NetworkError.instance(nf.errno(), "could not kqueue.removeListen()");
        }
    }
}
