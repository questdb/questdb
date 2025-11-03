/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.KqueueAccessor;
import io.questdb.std.Files;
import io.questdb.std.IntHashSet;
import io.questdb.std.LongMatrix;
import io.questdb.std.Misc;

public class IODispatcherOsx<C extends IOContext<C>> extends AbstractIODispatcher<C> {
    private static final int EVM_DEADLINE = 1;
    private static final int EVM_ID = 0;
    private static final int EVM_OPERATION_ID = 2;
    protected final LongMatrix pendingEvents = new LongMatrix(3);
    private final IntHashSet alreadyHandledFds = new IntHashSet();
    private final int capacity;
    private final KeventWriter keventWriter = new KeventWriter();
    private final Kqueue kqueue;

    public IODispatcherOsx(
            IODispatcherConfiguration configuration,
            IOContextFactory<C> ioContextFactory
    ) {
        super(configuration, ioContextFactory);
        this.capacity = configuration.getEventCapacity();
        // bind socket
        try {
            this.kqueue = new Kqueue(configuration.getKqueueFacade(), capacity);
            registerListenerFd();
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void close() {
        super.close();
        Misc.free(kqueue);
        LOG.info().$("closed").$();
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
                keventWriter.prepare().removeReadFD(suspendEvent.getFd()).done();
                pendingEvents.deleteRow(eventRow);
            }
        }
        doDisconnect(context, reason);
    }

    private void enqueuePending(int watermark) {
        keventWriter.prepare();
        for (int i = watermark, sz = pending.size(); i < sz; i++) {
            final C context = pending.get(i);
            final long id = pending.get(i, OPM_ID);
            final int operation = initialBias == IODispatcherConfiguration.BIAS_READ ? IOOperation.READ : IOOperation.WRITE;
            pending.set(i, OPM_OPERATION, operation);

            // Important: We must register for writing *before* registering for reading
            // see #processRegistrations() for details.
            if (operation == IOOperation.WRITE || context.getSocket().wantsTlsWrite()) {
                keventWriter.writeFD(context.getFd(), id);
            }
            if (operation == IOOperation.READ || context.getSocket().wantsTlsRead()) {
                keventWriter.readFD(context.getFd(), id);
            }
        }
        keventWriter.done();
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
            } else {
                // the connection is alive, so we need to re-arm kqueue to be able to detect broken connection
                rearmKqueue(context, id, IOOperation.READ);
            }
        } else {
            final int requestedOp = (int) pending.get(row, OPM_OPERATION);
            final boolean readyForWrite = kqueue.getFilter() == KqueueAccessor.EVFILT_WRITE;
            final boolean readyForRead = kqueue.getFilter() == KqueueAccessor.EVFILT_READ;

            if ((requestedOp == IOOperation.WRITE && readyForWrite) || (requestedOp == IOOperation.READ && readyForRead)) {
                // disarm extra filter in case it was previously set and haven't fired yet
                keventWriter.prepare().tolerateErrors();
                if (requestedOp == IOOperation.READ && context.getSocket().wantsTlsWrite()) {
                    keventWriter.removeWriteFD(context.getFd());
                }
                if (requestedOp == IOOperation.WRITE && context.getSocket().wantsTlsRead()) {
                    keventWriter.removeReadFD(context.getFd());
                }
                keventWriter.done();
                // publish the operation and we're done
                publishOperation(requestedOp, context);
                pending.deleteRow(row);
                return true;
            } else {
                // that's not the requested operation, but something wanted by the socket
                if (context.getSocket().tlsIO(tlsIOFlags(readyForRead, readyForWrite)) < 0) {
                    doDisconnect(context, id, DISCONNECT_SRC_TLS_ERROR);
                    pending.deleteRow(row);
                    return true;
                }
                rearmKqueue(context, id, requestedOp);
            }
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
                .$(", eventId=").$(eventId)
                .I$();

        rearmKqueue(context, opId, operation);
        context.clearSuspendEvent();

        pendingEvents.deleteRow(eventsRow);
    }

    private void processHeartbeats(int watermark, long timestamp) {
        int count = 0;
        for (int i = 0; i < watermark && pending.get(i, OPM_HEARTBEAT_TIMESTAMP) < timestamp; i++, count++) {
            final C context = pending.get(i);

            // De-register pending operation from kqueue. We'll register it later when we get a heartbeat pong.
            final long fd = context.getFd();
            final long opId = pending.get(i, OPM_ID);
            long op = context.getSuspendEvent() != null ? IOOperation.READ : pending.get(i, OPM_OPERATION);
            keventWriter.prepare().tolerateErrors();
            if (op == IOOperation.READ || context.getSocket().wantsTlsRead()) {
                keventWriter.removeReadFD(fd);
            }
            if (op == IOOperation.WRITE || context.getSocket().wantsTlsWrite()) {
                keventWriter.removeWriteFD(fd);
            }
            if (keventWriter.done() != 0) {
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
                        .$(", id=").$(opId)
                        .I$();
            }

            final SuspendEvent suspendEvent = context.getSuspendEvent();
            if (suspendEvent != null) {
                // Also, de-register suspend event from kqueue.
                int eventRow = pendingEvents.binarySearch(opId, EVM_OPERATION_ID);
                if (eventRow < 0) {
                    LOG.critical().$("internal error: suspend event not found on heartbeat [id=").$(opId).I$();
                } else {
                    keventWriter.prepare().removeReadFD(suspendEvent.getFd()).done();
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
        keventWriter.prepare();
        while ((cursor = interestSubSeq.next()) > -1) {
            final IOEvent<C> event = interestQueue.get(cursor);
            final C context = event.context;
            final int requestedOperation = event.operation;
            final long srcOpId = context.getAndResetHeartbeatId();
            interestSubSeq.done(cursor);

            useful = true;
            final long fd = context.getFd();
            final long opId = nextOpId();

            int operation = requestedOperation;
            final SuspendEvent suspendEvent = context.getSuspendEvent();
            if (requestedOperation == IOOperation.HEARTBEAT) {
                assert srcOpId != -1;

                int heartbeatRow = pendingHeartbeats.binarySearch(srcOpId, OPM_ID);
                if (heartbeatRow < 0) {
                    LOG.info().$("could not find heartbeat, connection must be already closed [fd=").$(fd)
                            .$(", srcId=").$(srcOpId)
                            .I$();
                    continue;
                } else {
                    operation = (int) pendingHeartbeats.get(heartbeatRow, OPM_OPERATION);

                    LOG.debug().$("processing heartbeat registration [fd=").$(fd)
                            .$(", op=").$(operation)
                            .$(", srcId=").$(srcOpId)
                            .$(", id=").$(opId)
                            .I$();

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
                if (requestedOperation == IOOperation.READ && suspendEvent == null && context.getSocket().isMorePlaintextBuffered()) {
                    publishOperation(IOOperation.READ, context);
                    continue;
                }

                LOG.debug().$("processing registration [fd=").$(fd)
                        .$(", op=").$(operation)
                        .$(", id=").$(opId)
                        .I$();

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
                        .$(", deadline=").$(suspendEvent.getDeadline())
                        .I$();

                int eventRow = pendingEvents.addRow();
                pendingEvents.set(eventRow, EVM_ID, eventId);
                pendingEvents.set(eventRow, EVM_OPERATION_ID, opId);
                pendingEvents.set(eventRow, EVM_DEADLINE, suspendEvent.getDeadline());

                keventWriter.readFD(suspendEvent.getFd(), eventId);
            }

            // Important: We have to prioritize write registration over read registration!
            // In other words: Register for writing before registering for reading.
            // Why? If a TLS socket wants to write then it means it has encrypted data available
            // in its internal buffer, and we must write it out as soon as possible.
            // If we register for reading first, then kqueue may trigger a read event before write event,
            // and while processing the read event we have to deregister from write events.
            // This can create a loop where we keep deregistering and registering for write events without ever
            // writing out the encrypted data.
            if (operation == IOOperation.WRITE || context.getSocket().wantsTlsWrite()) {
                keventWriter.writeFD(fd, opId);
            }
            if (operation == IOOperation.READ || context.getSocket().wantsTlsRead()) {
                keventWriter.readFD(fd, opId);
            }
        }

        keventWriter.done();

        return useful;
    }

    private void processSuspendEventDeadlines(long timestamp) {
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
            keventWriter.prepare().removeReadFD(suspendEvent.getFd()).done();

            // Next, close the event and resume the original operation.
            // to resume a socket operation, we simply re-arm kqueue
            rearmKqueue(context, opId, operation);
            context.clearSuspendEvent();
        }
        pendingEvents.zapTop(count);
    }

    private void rearmKqueue(C context, long id, int operation) {
        keventWriter.prepare();

        // Important: We must register for writing *before* registering for reading
        // see #processRegistrations() for details.
        if (operation == IOOperation.WRITE || context.getSocket().wantsTlsWrite()) {
            keventWriter.writeFD(context.getFd(), id);
        }
        if (operation == IOOperation.READ || context.getSocket().wantsTlsRead()) {
            keventWriter.readFD(context.getFd(), id);
        }
        keventWriter.done();
    }

    @Override
    protected void registerListenerFd() {
        if (kqueue.listen(serverFd) != 0) {
            throw NetworkError.instance(nf.errno(), "could not kqueue.listen()");
        }
    }

    @Override
    protected boolean runSerially() {
        final long timestamp = clock.getTicks();
        boolean useful = processDisconnects(timestamp);
        alreadyHandledFds.clear();
        final int n = kqueue.poll(0);
        int watermark = pending.size();
        int offset = 0;
        if (n > 0) {
            // check all activated FDs
            LOG.debug().$("poll [n=").$(n).I$();
            for (int i = 0; i < n; i++) {
                kqueue.setReadOffset(offset);
                offset += KqueueAccessor.SIZEOF_KEVENT;
                final int osFd = kqueue.getOsFd();
                final long id = kqueue.getData();
                // this is server socket, accept if there aren't too many already
                if (osFd == Files.toOsFd(serverFd)) {
                    accept(timestamp);
                    useful = true;
                    continue;
                }
                if (!alreadyHandledFds.add(osFd)) {
                    // we already handled this fd (socket), but for another event;
                    // ignore this one as we already removed/re-armed kqueue filter
                    continue;
                }
                if (isEventId(id)) {
                    handleSuspendEvent(id);
                    continue;
                }
                // since we may register multiple times
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
        if (kqueue.removeListen(serverFd) != 0) {
            throw NetworkError.instance(nf.errno(), "could not kqueue.removeListen()");
        }
    }

    private class KeventWriter {
        private int index;
        private int lastError; // 0 means no error
        private int offset;
        private boolean tolerateErrors;

        public int done() {
            if (index > 0) {
                register(index);
            }
            index = 0;
            offset = 0;
            tolerateErrors = false;
            int lastError = this.lastError;
            this.lastError = 0;
            return lastError;
        }

        public void readFD(long fd, long id) {
            kqueue.setWriteOffset(offset);
            kqueue.readFD(fd, id);
            offset += KqueueAccessor.SIZEOF_KEVENT;
            if (++index > capacity - 1) {
                register(index);
                index = offset = 0;
            }
        }

        public KeventWriter removeReadFD(long fd) {
            kqueue.setWriteOffset(offset);
            kqueue.removeReadFD(fd);
            offset += KqueueAccessor.SIZEOF_KEVENT;
            if (++index > capacity - 1) {
                register(index);
                index = offset = 0;
            }
            return this;
        }

        public void removeWriteFD(long fd) {
            kqueue.setWriteOffset(offset);
            kqueue.removeWriteFD(fd);
            offset += KqueueAccessor.SIZEOF_KEVENT;
            if (++index > capacity - 1) {
                register(index);
                index = offset = 0;
            }
        }

        public void tolerateErrors() {
            this.tolerateErrors = true;
        }

        public void writeFD(long fd, long id) {
            kqueue.setWriteOffset(offset);
            kqueue.writeFD(fd, id);
            offset += KqueueAccessor.SIZEOF_KEVENT;
            if (++index > capacity - 1) {
                register(index);
                index = offset = 0;
            }
        }

        private KeventWriter prepare() {
            if (index > 0 || offset > 0) {
                throw new IllegalStateException("missing done() call");
            }
            return this;
        }

        private void register(int changeCount) {
            int res = kqueue.register(changeCount);
            if (!tolerateErrors && res != 0) {
                throw NetworkError.instance(nf.errno()).put("could not register [changeCount=").put(changeCount).put(']');
            }
            lastError = res != 0 ? res : lastError;
            LOG.debug().$("kqueued [count=").$(changeCount).I$();
        }
    }
}
