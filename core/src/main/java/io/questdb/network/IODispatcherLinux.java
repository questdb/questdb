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

import io.questdb.std.Misc;

public class IODispatcherLinux<C extends IOContext<C>> extends AbstractIODispatcher<C> {
    private final Epoll epoll;
    private boolean pendingAccept;

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
        Misc.free(epoll);
        LOG.info().$("closed").$();
    }

    private void enqueuePending(int watermark) {
        for (int i = watermark, sz = pending.size(), offset = 0; i < sz; i++, offset += EpollAccessor.SIZEOF_EVENT) {
            final C context = pending.get(i);
            final long id = pending.get(i, OPM_ID);
            final long fd = pending.get(i, OPM_FD);
            final int operation = initialBias == IODispatcherConfiguration.BIAS_READ ? IOOperation.READ : IOOperation.WRITE;
            pending.set(i, OPM_OPERATION, operation);
            if (epoll.control(fd, id, EpollAccessor.EPOLL_CTL_ADD, epollOp(operation, context)) < 0) {
                LOG.critical().$("internal error: epoll_ctl failure [id=").$(id)
                        .$(", err=").$(nf.errno())
                        .I$();
            }
        }
    }

    private int epollOp(int operation, C context) {
        // {@code operation} is a bitmask of {@link IOOperation} bits
        // (READ / WRITE / HEARTBEAT) accumulated by
        // {@link #processRegistrations} when concurrent registrations
        // arrive for the same fd. Translating bit-by-bit (rather than the
        // legacy single-op switch) is what keeps both halves of an
        // {@code (READ, WRITE)} registration pair armed in the kernel
        // instead of losing the earlier one to {@code EPOLL_CTL_MOD}'s
        // last-arm-wins overwrite.
        int op = 0;
        if ((operation & IOOperation.READ) != 0) op |= EpollAccessor.EPOLLIN;
        if ((operation & IOOperation.WRITE) != 0) op |= EpollAccessor.EPOLLOUT;
        if (context.getSocket().wantsTlsRead()) {
            op |= EpollAccessor.EPOLLIN;
        }
        if (context.getSocket().wantsTlsWrite()) {
            op |= EpollAccessor.EPOLLOUT;
        }
        // EPOLLONESHOT: the kernel disarms the fd after firing this event
        // exactly once, until something calls epoll_ctl(MOD) again. This
        // eliminates the concurrent-handler race that fires when a
        // publisher's registerChannel(WRITE) re-arms an fd while a worker
        // is still mid-handleClientOperation for that fd: the kernel would
        // otherwise fire a second event (level-triggered, fd still ready)
        // and another worker would pick it up via processIOQueue,
        // producing two threads writing to the same response sink. With
        // ONESHOT, the next event only fires after the worker (or a
        // publisher) explicitly re-arms via registerChannel, which the
        // existing code already does on every exit path.
        //
        // The other dispatcher backends use the equivalent semantic:
        // {@link IODispatcherOsx} sets {@code EV_ADD | EV_ONESHOT} in
        // {@link Kqueue#readFD} and {@link Kqueue#writeFD};
        // {@link IODispatcherWindows} drops the fd from its select FDSet
        // after fire and only re-arms via {@code registerChannel}. So the
        // "no second-fire while a worker is mid-handler" invariant holds
        // identically on all three platforms.
        op |= EpollAccessor.EPOLLONESHOT;
        return op;
    }

    /**
     * Linear scan for a pending row whose {@code OPM_FD} matches. Returns
     * the row index, or -1 when no row exists for this fd. Called from the
     * dispatcher thread only, so the read-then-act sequence is race-free
     * against other {@code pending} mutations.
     * <p>
     * Used by {@link #processRegistrations} to dedupe concurrent
     * registrations for the same fd into a single pending row with an
     * OR-combined op mask. Without dedupe, two {@code registerChannel}
     * calls for the same fd would each allocate a fresh {@code opId} and
     * issue {@code EPOLL_CTL_MOD}; the kernel keeps only the last
     * {@code epoll_data}, orphaning the earlier pending row whose
     * {@code opId} is no longer recognised. This surfaces as a
     * server-initiated WRITE arm being silently dropped when it races
     * with the framework's PISR-driven re-register on the same fd.
     * <p>
     * Linear scan is O(n) over {@code pending.size()}. At ~20-1000
     * connections that is fine on the dispatcher hot path; for larger
     * deployments a {@code (fd -> rowId)} index would amortise the cost,
     * but ObjLongMatrix's row indices shift on {@code deleteRow}/
     * {@code zapTop}, so the index would need {@code opId} as the stable
     * key plus a binary-search step. Not done here.
     */
    private int findPendingRowByFd(long fd) {
        for (int i = 0, n = pending.size(); i < n; i++) {
            if (pending.get(i, OPM_FD) == fd) {
                return i;
            }
        }
        return -1;
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
        context.incrementEpollFireCount();
        // {@code requestedMask} is a bitmask of {@link IOOperation} bits
        // accumulated by {@link #processRegistrations}'s dedupe path. A
        // single registration is just one bit; concurrent (READ, WRITE)
        // arms for the same fd land here as both bits set.
        final int requestedMask = (int) pending.get(row, OPM_OPERATION);
        // We check EPOLLOUT flag and treat all other events, including EPOLLIN and EPOLLHUP, as a read.
        final boolean readyForWrite = (epoll.getEvent() & EpollAccessor.EPOLLOUT) > 0;
        final boolean readyForRead = !readyForWrite || (epoll.getEvent() & EpollAccessor.EPOLLIN) > 0;

        // Determine which armed ops are now satisfied by the kernel's
        // event report. Publish each one independently so concurrent
        // arms each get their own ioEvent (one drain pass per published
        // op). Build {@code satisfiedMask} so we can clear those bits and
        // either delete the row (all arms satisfied) or re-arm with the
        // remaining mask.
        int satisfiedMask = 0;
        boolean writeReady = (requestedMask & IOOperation.WRITE) != 0 && readyForWrite;
        boolean readReady = (requestedMask & IOOperation.READ) != 0 && readyForRead;

        if (writeReady || readReady) {
            // TLS plumbing: tlsIOFlags wants the operation we will
            // publish. When both are ready, prefer WRITE so a partial
            // write that still has bytes to send is drained ASAP; READ
            // satisfaction will land on the next epoll fire.
            int primaryOp = writeReady ? IOOperation.WRITE : IOOperation.READ;
            if (context.getSocket().tlsIO(tlsIOFlags(primaryOp, readyForRead, readyForWrite)) < 0) {
                doDisconnect(context, DISCONNECT_SRC_TLS_ERROR);
                pending.deleteRow(row);
                return true;
            }
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
                // Some armed ops still pending. Keep the row, clear the
                // satisfied bits, re-arm with what's left so the next
                // epoll fire only delivers the remaining ops.
                pending.set(row, OPM_OPERATION, remainingMask);
                rearmEpoll(context, id, remainingMask);
            }
            return true;
        }

        // It's something different from the requested operations.
        if (context.getSocket().tlsIO(tlsIOFlags(readyForRead, readyForWrite)) < 0) {
            doDisconnect(context, DISCONNECT_SRC_TLS_ERROR);
            pending.deleteRow(row);
            return true;
        }
        rearmEpoll(context, id, requestedMask);
        return false;
    }

    private void processHeartbeats(int watermark, long timestamp) {
        int count = 0;
        for (int i = 0; i < watermark && pending.get(i, OPM_HEARTBEAT_TIMESTAMP) < timestamp; i++, count++) {
            final C context = pending.get(i);

            // De-register pending operation from epoll. We'll register it later when we get a heartbeat pong.
            final long fd = context.getFd();
            final long opId = pending.get(i, OPM_ID);
            if (epoll.control(fd, opId, EpollAccessor.EPOLL_CTL_DEL, 0) < 0) {
                LOG.critical().$("internal error: epoll_ctl remove operation failure [id=").$(opId)
                        .$(", err=").$(nf.errno())
                        .I$();
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

    private int processIdleConnections(long idleTimestamp) {
        int count = 0;
        for (int i = 0, n = pending.size(); i < n && pending.get(i, OPM_CREATE_TIMESTAMP) < idleTimestamp; i++, count++) {
            doDisconnect(pending.get(i), DISCONNECT_SRC_IDLE);
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
            context.incrementInterestQueueConsumeCount();
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
            final long opId = nextOpId();
            final long fd = context.getFd();

            int operation = requestedOperation;
            int epollCmd = EpollAccessor.EPOLL_CTL_MOD;
            if (requestedOperation == IOOperation.HEARTBEAT) {
                assert srcOpId != -1;

                int heartbeatRow = pendingHeartbeats.binarySearch(srcOpId, OPM_ID);
                if (heartbeatRow < 0) {
                    LOG.info().$("could not find heartbeat, connection must be already closed [fd=").$(fd)
                            .$(", srcId=").$(srcOpId)
                            .I$();
                    continue;
                } else {
                    epollCmd = EpollAccessor.EPOLL_CTL_ADD;
                    operation = (int) pendingHeartbeats.get(heartbeatRow, OPM_OPERATION);

                    // Dedupe by fd: a regular {@code registerChannel(WRITE)}
                    // from any server-initiated publisher may have raced
                    // ahead of this heartbeat re-register in the interest
                    // queue and already added a {@code pending} row for
                    // this fd. Without merging, the heartbeat branch's
                    // unconditional {@code addRow} would create a second
                    // row for the same fd, and the kernel's
                    // {@code EPOLL_CTL_ADD} here would overwrite the
                    // regular row's {@code opId} in {@code epoll_data},
                    // orphaning that row's IOEvent. OR the heartbeat's
                    // mask into the existing row instead.
                    int existingRow = findPendingRowByFd(fd);
                    if (existingRow >= 0) {
                        int existingMask = (int) pending.get(existingRow, OPM_OPERATION);
                        int combinedMask = existingMask | operation;
                        pending.set(existingRow, OPM_OPERATION, combinedMask);
                        pending.set(existingRow, OPM_HEARTBEAT_TIMESTAMP, timestamp);
                        long existingOpId = pending.get(existingRow, OPM_ID);
                        LOG.debug().$("merging heartbeat registration [fd=").$(fd)
                                .$(", existingMask=").$(existingMask)
                                .$(", addedOp=").$(operation)
                                .$(", combinedMask=").$(combinedMask)
                                .$(", id=").$(existingOpId).I$();
                        // The regular branch that added the existing row will
                        // have issued EPOLL_CTL_MOD; if the heartbeat-driven
                        // DEL happened first, that MOD failed with ENOENT and
                        // an ADD here is required. If the MOD succeeded
                        // somehow, the ADD will fail with EEXIST. Either way
                        // we record the combined mask in {@code pending}; a
                        // subsequent registration on this fd will use the
                        // merged state.
                        if (epoll.control(fd, existingOpId, EpollAccessor.EPOLL_CTL_ADD, epollOp(combinedMask, context)) < 0) {
                            if (epoll.control(fd, existingOpId, EpollAccessor.EPOLL_CTL_MOD, epollOp(combinedMask, context)) < 0) {
                                LOG.critical().$("internal error: epoll_ctl heartbeat merge failed [id=").$(existingOpId)
                                        .$(", err=").$(nf.errno()).I$();
                            }
                        }
                        pendingHeartbeats.deleteRow(heartbeatRow);
                        continue;
                    }

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

                // Dedupe by fd: if a pending row already exists for this
                // connection (e.g. the publisher's wakeLocked just armed
                // WRITE while the worker's PISR catch is also arming
                // WRITE, or the framework's READ rearm is racing the
                // publisher's WRITE), OR the new op into the existing
                // row's mask and re-issue {@code EPOLL_CTL_MOD} with the
                // combined mask instead of allocating a fresh {@code opId}.
                // The kernel's {@code epoll_data} stays bound to the
                // existing row's {@code opId}, so the next epoll fire
                // routes to the row that's still in {@code pending}.
                // Without this dedupe, the second EPOLL_CTL_MOD overwrites
                // the kernel's data field; the first row is orphaned and
                // its IOEvent is permanently lost. See
                // {@link #findPendingRowByFd} for context.
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
                    if (epoll.control(fd, existingOpId, EpollAccessor.EPOLL_CTL_MOD, epollOp(combinedMask, context)) < 0) {
                        LOG.critical().$("internal error: epoll_ctl modify operation failure [id=").$(existingOpId)
                                .$(", err=").$(nf.errno()).I$();
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

            // we re-arm epoll globally, in that even when we disconnect
            // because we have to remove FD from epoll
            if (epoll.control(fd, opId, epollCmd, epollOp(operation, context)) < 0) {
                LOG.critical().$("internal error: epoll_ctl modify operation failure [id=").$(opId)
                        .$(", err=").$(nf.errno()).I$();
            }
        }
        return useful;
    }

    private void rearmEpoll(C context, long id, int operation) {
        if (epoll.control(context.getFd(), id, EpollAccessor.EPOLL_CTL_MOD, epollOp(operation, context)) < 0) {
            LOG.critical().$("internal error: epoll_ctl modify operation failure [id=").$(id)
                    .$(", err=").$(nf.errno())
                    .I$();
        }
    }

    @Override
    protected void registerListenerFd() {
        epoll.listen(serverFd);
    }

    @Override
    protected boolean runSerially() {
        // todo: introduce fairness factor
        //  current worker impl will still proceed to execute another job even if this one was useful
        //  we should see if we can stay inside of this method until we have a completely idle iteration
        //  at the same time we should hog this thread in case we are always 'useful', we can probably
        //  introduce a loop count after which we always exit

        final long timestamp = clock.getTicks();
        boolean useful = processDisconnects(timestamp);
        final int n = epoll.poll();
        int watermark = pending.size();
        int offset = 0;
        if (n > 0 || pendingAccept) {
            boolean acceptProcessed = false;

            // check all activated FDs
            LOG.debug().$("epoll [n=").$(n).I$();
            for (int i = 0; i < n; i++) {
                epoll.setOffset(offset);
                offset += EpollAccessor.SIZEOF_EVENT;
                final long id = epoll.getData();
                // this is server socket, accept if there aren't too many already
                if (id == 0) {
                    pendingAccept = !accept(timestamp);
                    acceptProcessed = true;
                    useful = true;
                } else if (handleSocketOperation(id)) {
                    useful = true;
                    watermark--;
                }
            }

            // pendingAccept is almost always false -> testing it as first to make the branch easy to predict
            if (pendingAccept && !acceptProcessed && isListening()) {
                // we have left-overs from a previous round, process them now
                pendingAccept = !accept(timestamp);
                useful = true;
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
        epoll.removeListen(serverFd);
    }
}
