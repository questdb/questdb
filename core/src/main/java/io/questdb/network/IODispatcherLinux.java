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

public class IODispatcherLinux<C extends IOContext<C>> extends AbstractIODispatcher<C> {
    private static final int EVM_DEADLINE = 1;
    private static final int EVM_ID = 0;
    private static final int EVM_OPERATION_ID = 2;
    protected final LongMatrix pendingEvents = new LongMatrix(3);
    private final Epoll epoll;
    // the final ids are shifted by 1 bit which is reserved to distinguish socket operations (0) and suspend events (1);
    // id 0 is reserved for operations on the server fd
    private long idSeq = 1;

    public IODispatcherLinux(
            IODispatcherConfiguration configuration,
            IOContextFactory<C> ioContextFactory
    ) {
        super(configuration, ioContextFactory);
        this.epoll = new Epoll(configuration.getEpollFacade(), configuration.getEventCapacity());
        registerListenerFd();
    }

    @Override
    public void close() {
        super.close();
        epoll.close();
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
                final long eventId = pendingEvents.get(eventRow, EVM_ID);
                if (epoll.control(suspendEvent.getFd(), eventId, EpollAccessor.EPOLL_CTL_DEL, 0) < 0) {
                    LOG.critical().$("internal error: epoll_ctl remove suspend event failure [eventId=").$(eventId)
                            .$(", err=").$(nf.errno()).I$();
                }
                pendingEvents.deleteRow(eventRow);
            }
        }
        doDisconnect(context, reason);
    }

    private void enqueuePending(int watermark) {
        for (int i = watermark, sz = pending.size(), offset = 0; i < sz; i++, offset += EpollAccessor.SIZEOF_EVENT) {
            final long id = pending.get(i, OPM_ID);
            final int fd = (int) pending.get(i, OPM_FD);
            int operation = initialBias == IODispatcherConfiguration.BIAS_READ ? IOOperation.READ : IOOperation.WRITE;
            pending.set(i, OPM_OPERATION, operation);
            int event = operation == IOOperation.READ ? EpollAccessor.EPOLLIN : EpollAccessor.EPOLLOUT;
            if (epoll.control(fd, id, EpollAccessor.EPOLL_CTL_ADD, event) < 0) {
                LOG.critical().$("internal error: epoll_ctl failure [id=").$(id)
                        .$(", err=").$(nf.errno()).I$();
            }
        }
    }

    private boolean handleSocketOperation(long id) {
        // find row in pending for two reasons:
        // 1. find payload
        // 2. remove row from pending, remaining rows will be timed out
        final int row = pending.binarySearch(id, OPM_ID);
        if (row < 0) {
            LOG.critical().$("internal error: epoll returned unexpected id [id=").$(id).I$();
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
                    // Check EPOLLOUT flag and treat all other events, including EPOLLIN and EPOLLHUP, as a read.
                    (epoll.getEvent() & EpollAccessor.EPOLLOUT) > 0 ? IOOperation.WRITE : IOOperation.READ,
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
            LOG.critical().$("internal error: epoll returned unexpected event id [eventId=").$(id).I$();
            return;
        }

        final long opId = pendingEvents.get(eventsRow, EVM_OPERATION_ID);
        final int row = pending.binarySearch(opId, OPM_ID);
        if (row < 0) {
            LOG.critical().$("internal error: suspended operation not found [id=").$(opId).$(", eventId=").$(id).I$();
            return;
        }

        final int operation = (int) pending.get(row, OPM_OPERATION);
        final C context = pending.get(row);
        final SuspendEvent suspendEvent = context.getSuspendEvent();
        assert suspendEvent != null;

        resumeOperation(context, opId, operation);
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
            final int fd = context.getFd();
            final long opId = pending.get(i, OPM_ID);
            if (epoll.control(fd, opId, EpollAccessor.EPOLL_CTL_DEL, 0) < 0) {
                LOG.critical().$("internal error: epoll_ctl remove operation failure [id=").$(opId)
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
                    if (epoll.control(suspendEvent.getFd(), eventId, EpollAccessor.EPOLL_CTL_DEL, 0) < 0) {
                        LOG.critical().$("internal error: epoll_ctl remove suspend event failure [eventId=").$(eventId)
                                .$(", err=").$(nf.errno()).I$();
                    }
                    pendingEvents.deleteRow(eventRow);
                }
            }
        }
        pending.zapTop(count);
    }

    private int processIdleConnections(long idleTimestamp) {
        int count = 0;
        for (int i = 0, n = pending.size(); i < n && pending.get(i, OPM_CREATE_TIMESTAMP) < idleTimestamp; i++, count++) {
            doDisconnect(pending.get(i), pending.get(i, OPM_ID), DISCONNECT_SRC_IDLE);
        }
        pending.zapTop(count);
        return count;
    }

    private boolean processRegistrations(long timestamp) {
        boolean useful = false;
        long cursor;
        while ((cursor = interestSubSeq.next()) > -1) {
            final IOEvent<C> event = interestQueue.get(cursor);
            final C context = event.context;
            final int requestedOperation = event.operation;
            final long srcOpId = context.getAndResetHeartbeatId();
            interestSubSeq.done(cursor);

            useful = true;
            final long opId = nextOpId();
            final int fd = context.getFd();

            int operation = requestedOperation;
            final SuspendEvent suspendEvent = context.getSuspendEvent();
            int epollCmd = EpollAccessor.EPOLL_CTL_MOD;
            if (requestedOperation == IOOperation.HEARTBEAT) {
                assert srcOpId != -1;

                int heartbeatRow = pendingHeartbeats.binarySearch(srcOpId, OPM_ID);
                if (heartbeatRow < 0) {
                    continue; // The connection is already closed.
                } else {
                    epollCmd = EpollAccessor.EPOLL_CTL_ADD;
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

                if (epoll.control(suspendEvent.getFd(), eventId, EpollAccessor.EPOLL_CTL_ADD, EpollAccessor.EPOLLIN) < 0) {
                    LOG.critical().$("internal error: epoll_ctl add suspend event failure [id=").$(eventId)
                            .$(", err=").$(nf.errno()).I$();
                }
            }

            // we re-arm epoll globally, in that even when we disconnect
            // because we have to remove FD from epoll
            final int epollOp = operation == IOOperation.READ ? EpollAccessor.EPOLLIN : EpollAccessor.EPOLLOUT;
            if (epoll.control(fd, opId, epollCmd, epollOp) < 0) {
                LOG.critical().$("internal error: epoll_ctl modify operation failure [id=").$(opId)
                        .$(", err=").$(nf.errno()).I$();
            }
        }
        return useful;
    }

    private void processSuspendEventDeadlines(long timestamp) {
        int count = 0;
        for (int i = 0, n = pendingEvents.size(); i < n && pendingEvents.get(i, EVM_DEADLINE) < timestamp; i++, count++) {
            final long eventId = pendingEvents.get(i, EVM_ID);
            final long opId = pendingEvents.get(i, EVM_OPERATION_ID);
            final int pendingRow = pending.binarySearch(opId, OPM_ID);
            if (pendingRow < 0) {
                LOG.critical().$("internal error: failed to find operation for expired suspend event [id=").$(opId).I$();
                continue;
            }
            // First, remove the suspend event from the epoll interest list.
            final C context = pending.get(pendingRow);
            final int operation = (int) pending.get(pendingRow, OPM_OPERATION);
            final SuspendEvent suspendEvent = context.getSuspendEvent();
            assert suspendEvent != null;
            if (epoll.control(suspendEvent.getFd(), eventId, EpollAccessor.EPOLL_CTL_DEL, 0) < 0) {
                LOG.critical().$("internal error: epoll_ctl remove suspend event failure [eventId=").$(eventId)
                        .$(", err=").$(nf.errno()).I$();
            }
            // Next, resume the original operation and close the event.
            resumeOperation(context, opId, operation);
        }
        pendingEvents.zapTop(count);
    }

    private void resumeOperation(C context, long id, int operation) {
        // to resume a socket operation, we simply re-arm epoll
        if (
                epoll.control(
                        context.getFd(),
                        id,
                        EpollAccessor.EPOLL_CTL_MOD,
                        operation == IOOperation.READ ? EpollAccessor.EPOLLIN : EpollAccessor.EPOLLOUT
                ) < 0
        ) {
            LOG.critical().$("internal error: epoll_ctl operation mod failure [id=").$(id)
                    .$(", err=").$(nf.errno()).I$();
        }
        context.clearSuspendEvent();
    }

    @Override
    protected void pendingAdded(int index) {
        pending.set(index, OPM_ID, nextOpId());
    }

    @Override
    protected void registerListenerFd() {
        this.epoll.listen(serverFd);
    }

    @Override
    protected boolean runSerially() {
        // todo: introduce fairness factor
        //  current worker impl will still proceed to execute another job even if this one was useful
        //  we should see if we can stay inside of this method until we have a completely idle iteration
        //  at the same time we should hog this thread in case we are always 'useful', we can probably
        //  introduce a loop count after which we always exit
        boolean useful = false;

        final long timestamp = clock.getTicks();
        processDisconnects(timestamp);
        final int n = epoll.poll();
        int watermark = pending.size();
        int offset = 0;
        if (n > 0) {
            // check all activated FDs
            LOG.debug().$("epoll [n=").$(n).$(']').$();
            for (int i = 0; i < n; i++) {
                epoll.setOffset(offset);
                offset += EpollAccessor.SIZEOF_EVENT;
                final long id = epoll.getData();
                // this is server socket, accept if there aren't too many already
                if (id == 0) {
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
        epoll.removeListen(serverFd);
    }
}
