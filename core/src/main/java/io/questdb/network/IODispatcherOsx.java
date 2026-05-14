/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;

public class IODispatcherOsx<C extends IOContext<C>> extends AbstractIODispatcher<C> {
    /**
     * Per-poll scratch state for de-duplicating kevents on the same fd
     * within a single {@code kqueue.poll()} return. When the kernel marks
     * both READ and WRITE ready on the same fd, two kevents come back in
     * one poll; without this combining, the second filter was previously
     * swallowed by an {@code alreadyHandledFds} guard and only re-armed
     * for the next poll, costing a dispatcher cycle of latency on
     * back-to-back READ+WRITE. Parallel triples
     * {@code (kevtFds[i], kevtIds[i], kevtMasks[i])} hold the unique fds,
     * their pending-row opIds, and the OR-combined "ready" filter bits.
     * Cleared at the start of every {@code runSerially}.
     */
    private final IntList kevtFds = new IntList(16);
    private final LongList kevtIds = new LongList(16);
    private final IntList kevtMasks = new IntList(16);
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

    private boolean handleSocketOperation(long id, int readyMask) {
        // find row in pending for two reasons:
        // 1. find payload
        // 2. remove row from pending, remaining rows will be timed out
        final int row = pending.binarySearch(id, OPM_ID);
        if (row < 0) {
            LOG.critical().$("internal error: kqueue returned unexpected id [id=").$(id).I$();
            return false;
        }

        final C context = pending.get(row);
        // OPM_OPERATION is a bitmask of armed ops (READ / WRITE) accumulated
        // by processRegistrations's dedupe-by-fd path. Mirrors the Linux
        // dispatcher fix that closed the "two concurrent registerChannel
        // calls for the same fd overwrite each other's opId" race.
        // {@code readyMask} is the OR-combined "ready" filter mask
        // accumulated across all kevents for this fd in the current
        // {@code kqueue.poll()} return.
        final int requestedMask = (int) pending.get(row, OPM_OPERATION);
        final boolean readyForRead = (readyMask & IOOperation.READ) != 0;
        final boolean readyForWrite = (readyMask & IOOperation.WRITE) != 0;

        boolean writeReady = (requestedMask & IOOperation.WRITE) != 0 && readyForWrite;
        boolean readReady = (requestedMask & IOOperation.READ) != 0 && readyForRead;

        if (writeReady || readReady) {
            int satisfiedMask = 0;
            keventWriter.prepare().tolerateErrors();
            // When only one ready bit lines up with the requested mask
            // (the typical case under EV_ONESHOT - the other filter
            // didn't fire in this poll), pre-disarm the opposite filter
            // if the TLS layer would otherwise leave it armed.
            if (writeReady && !readReady && context.getSocket().wantsTlsRead()) {
                keventWriter.removeReadFD(context.getFd());
            }
            if (readReady && !writeReady && context.getSocket().wantsTlsWrite()) {
                keventWriter.removeWriteFD(context.getFd());
            }
            keventWriter.done();
            if (writeReady) {
                publishOperation(IOOperation.WRITE, context);
                satisfiedMask |= IOOperation.WRITE;
            }
            if (readReady) {
                publishOperation(IOOperation.READ, context);
                satisfiedMask |= IOOperation.READ;
            }
            int remainingMask = requestedMask & ~satisfiedMask;
            if (remainingMask == 0) {
                pending.deleteRow(row);
            } else {
                pending.set(row, OPM_OPERATION, remainingMask);
                rearmKqueue(context, id, remainingMask);
            }
            return true;
        } else {
            // that's not the requested operation, but something wanted by the socket
            if (context.getSocket().tlsIO(tlsIOFlags(readyForRead, readyForWrite)) < 0) {
                doDisconnect(context, DISCONNECT_SRC_TLS_ERROR);
                pending.deleteRow(row);
                return true;
            }
            rearmKqueue(context, id, requestedMask);
        }
        return false;
    }

    /**
     * Linear scan for a pending row whose {@code OPM_FD} matches. Returns
     * the row index, or -1 when no row exists for this fd. See
     * IODispatcherLinux for the full rationale; in short, the kqueue
     * back-end has the same "two concurrent registerChannels for the same
     * fd overwrite each other's opId" race the Linux dispatcher had, and
     * the fix is the same: dedupe pending rows by fd, OR-combine ops.
     */
    private int findPendingRowByFd(long fd) {
        for (int i = 0, n = pending.size(); i < n; i++) {
            if (pending.get(i, OPM_FD) == fd) {
                return i;
            }
        }
        return -1;
    }

    private void processHeartbeats(int watermark, long timestamp) {
        int count = 0;
        for (int i = 0; i < watermark && pending.get(i, OPM_HEARTBEAT_TIMESTAMP) < timestamp; i++, count++) {
            final C context = pending.get(i);

            // De-register pending operation from kqueue. We'll register it later when we get a heartbeat pong.
            // OPM_OPERATION is a bitmask after the dedupe-by-fd port; tests
            // must mask each bit individually rather than equality-match
            // a single op (a combined READ|WRITE row would match neither
            // == READ nor == WRITE, leaving stale kevent state behind).
            final long fd = context.getFd();
            final long opId = pending.get(i, OPM_ID);
            long op = pending.get(i, OPM_OPERATION);
            keventWriter.prepare().tolerateErrors();
            if ((op & IOOperation.READ) != 0 || context.getSocket().wantsTlsRead()) {
                keventWriter.removeReadFD(fd);
            }
            if ((op & IOOperation.WRITE) != 0 || context.getSocket().wantsTlsWrite()) {
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
        }
        pending.zapTop(count);
    }

    private int processIdleConnections(long deadline) {
        int count = 0;
        for (int i = 0, n = pending.size(); i < n && pending.get(i, OPM_CREATE_TIMESTAMP) < deadline; i++, count++) {
            doDisconnect(pending.get(i), DISCONNECT_SRC_IDLE);
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
            if (event.deferredRepublish) {
                event.deferredRepublish = false;
                interestSubSeq.done(cursor);
                publishOperation(requestedOperation, context);
                useful = true;
                continue;
            }
            final long srcOpId = context.getAndResetHeartbeatId();
            interestSubSeq.done(cursor);

            useful = true;
            final long fd = context.getFd();
            final long opId = nextOpId();

            int operation = requestedOperation;
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
                if (requestedOperation == IOOperation.READ && context.getSocket().isMorePlaintextBuffered()) {
                    publishOperation(IOOperation.READ, context);
                    continue;
                }

                // Dedupe by fd: see IODispatcherLinux.processRegistrations for
                // the full motivation. If a pending row already exists for
                // this fd, OR the new op into its bitmask and reuse the
                // existing opId rather than letting kqueue's udata get
                // overwritten by a second registration.
                int existingRow = findPendingRowByFd(fd);
                if (existingRow >= 0) {
                    int existingMask = (int) pending.get(existingRow, OPM_OPERATION);
                    int combinedMask = existingMask | requestedOperation;
                    pending.set(existingRow, OPM_OPERATION, combinedMask);
                    pending.set(existingRow, OPM_HEARTBEAT_TIMESTAMP, timestamp);
                    long existingOpId = pending.get(existingRow, OPM_ID);
                    LOG.debug().$("merging registration [fd=").$(fd)
                            .$(", existingMask=").$(existingMask)
                            .$(", addedOp=").$(requestedOperation)
                            .$(", combinedMask=").$(combinedMask)
                            .$(", id=").$(existingOpId).I$();
                    // Register the kevent(s) for the combined mask using the
                    // existing opId. Add only the newly-armed filters; the
                    // ones already armed under existing opId stay valid.
                    int newlyArmed = requestedOperation & ~existingMask;
                    if ((newlyArmed & IOOperation.WRITE) != 0 || context.getSocket().wantsTlsWrite()) {
                        keventWriter.writeFD(fd, existingOpId);
                    }
                    if ((newlyArmed & IOOperation.READ) != 0 || context.getSocket().wantsTlsRead()) {
                        keventWriter.readFD(fd, existingOpId);
                    }
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

            // Important: We have to prioritize write registration over read registration!
            // In other words: Register for writing before registering for reading.
            // Why? If a TLS socket wants to write then it means it has encrypted data available
            // in its internal buffer, and we must write it out as soon as possible.
            // If we register for reading first, then kqueue may trigger a read event before write event,
            // and while processing the read event we have to deregister from write events.
            // This can create a loop where we keep deregistering and registering for write events without ever
            // writing out the encrypted data.
            //
            // OPM_OPERATION is a bitmask after the dedupe-by-fd port; tests
            // must mask each bit individually rather than equality-match.
            if ((operation & IOOperation.WRITE) != 0 || context.getSocket().wantsTlsWrite()) {
                keventWriter.writeFD(fd, opId);
            }
            if ((operation & IOOperation.READ) != 0 || context.getSocket().wantsTlsRead()) {
                keventWriter.readFD(fd, opId);
            }
        }

        keventWriter.done();

        return useful;
    }

    private void rearmKqueue(C context, long id, int operation) {
        keventWriter.prepare();

        // {@code operation} is a bitmask of IOOperation bits accumulated by
        // {@link #processRegistrations}'s dedupe-by-fd path. Translating
        // bit-by-bit keeps both halves of a combined READ+WRITE arm
        // alive on the kqueue instead of losing one to the udata
        // overwrite that bit our Linux dispatcher (the kqueue analogue
        // of EPOLL_CTL_MOD's last-arm-wins).
        //
        // Important: We must register for writing *before* registering
        // for reading - see #processRegistrations() for details.
        if ((operation & IOOperation.WRITE) != 0 || context.getSocket().wantsTlsWrite()) {
            keventWriter.writeFD(context.getFd(), id);
        }
        if ((operation & IOOperation.READ) != 0 || context.getSocket().wantsTlsRead()) {
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
        kevtFds.clear();
        kevtIds.clear();
        kevtMasks.clear();
        final int n = kqueue.poll(0);
        int watermark = pending.size();
        int offset = 0;
        if (n > 0) {
            // Pass 1: scan all kevents and combine ready masks per fd.
            // The kernel can return TWO kevents in one poll for the same
            // fd (one EVFILT_READ + one EVFILT_WRITE) when both filters
            // are ready simultaneously. The previous implementation
            // dropped the second kevent via {@code alreadyHandledFds},
            // satisfied only the first filter's op, and re-armed the
            // other; the kernel would then re-deliver the second filter
            // in the NEXT poll, costing a dispatcher cycle of latency on
            // back-to-back READ+WRITE. OR'ing the ready bits per fd lets
            // {@link #handleSocketOperation} satisfy both ops in one go.
            LOG.debug().$("poll [n=").$(n).I$();
            for (int i = 0; i < n; i++) {
                kqueue.setReadOffset(offset);
                offset += KqueueAccessor.SIZEOF_KEVENT;
                final int osFd = kqueue.getOsFd();
                // this is server socket, accept if there aren't too many already
                if (osFd == Files.toOsFd(serverFd)) {
                    accept(timestamp);
                    useful = true;
                    continue;
                }
                final long id = kqueue.getData();
                final int filter = kqueue.getFilter();
                final int opBit;
                if (filter == KqueueAccessor.EVFILT_WRITE) {
                    opBit = IOOperation.WRITE;
                } else if (filter == KqueueAccessor.EVFILT_READ) {
                    opBit = IOOperation.READ;
                } else {
                    opBit = 0;
                }
                int existingSlot = kevtFds.indexOf(osFd, 0, kevtFds.size());
                if (existingSlot < 0) {
                    kevtFds.add(osFd);
                    kevtIds.add(id);
                    kevtMasks.add(opBit);
                } else {
                    kevtMasks.setQuick(existingSlot, kevtMasks.getQuick(existingSlot) | opBit);
                }
            }
            // Pass 2: dispatch each unique fd once with its combined mask.
            for (int i = 0, m = kevtFds.size(); i < m; i++) {
                if (handleSocketOperation(kevtIds.getQuick(i), kevtMasks.getQuick(i))) {
                    useful = true;
                    watermark--;
                }
            }
        }

        // process rows over watermark (new connections)
        if (watermark < pending.size()) {
            enqueuePending(watermark);
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

        public void removeReadFD(long fd) {
            kqueue.setWriteOffset(offset);
            kqueue.removeReadFD(fd);
            offset += KqueueAccessor.SIZEOF_KEVENT;
            if (++index > capacity - 1) {
                register(index);
                index = offset = 0;
            }
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
