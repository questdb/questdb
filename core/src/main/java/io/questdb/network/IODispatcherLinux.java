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

import io.questdb.mp.IdleParkJob;
import io.questdb.std.Misc;

/**
 * A worker that has found nothing to do may park in this dispatcher's poll instead of napping or
 * sleeping, see {@link IdleParkJob} and {@link IODispatcherConfiguration#getPollBlockTimeout()}.
 */
public class IODispatcherLinux<C extends IOContext<C>> extends AbstractIODispatcher<C> implements IdleParkJob {
    // How long the dispatcher must have found nothing to do before a worker may wait in its poll.
    // Under back-to-back requests a worker that just served one spins and picks the next up at
    // once; a worker waiting in the poll would hold the run lock and keep it from doing so, and
    // the request would wait for that worker's wake-up instead.
    private static final long IDLE_PARK_MIN_IDLE_NANOS = 1_000_000L;
    private final Epoll epoll;
    private final int pollBlockTimeoutMillis;
    // when a pass last found something to do, read and written under the run lock
    private long lastUsefulNanos;
    private boolean pendingAccept;

    public IODispatcherLinux(
            IODispatcherConfiguration configuration,
            IOContextFactory<C> ioContextFactory
    ) {
        super(configuration, ioContextFactory);
        this.epoll = new Epoll(configuration.getEpollFacade(), configuration.getEventCapacity());
        this.pollBlockTimeoutMillis = Math.max(0, configuration.getPollBlockTimeout());
        registerListenerFd();
    }

    @Override
    public void cancelIdlePark() {
        releaseRunLock();
    }

    // Call it once the worker pool that runs this dispatcher has halted, as ServerMain does: a
    // worker waiting in the poll (see idlePark()) uses the epoll event buffer until its wait ends,
    // at most http.net.poll.block.timeout later.
    @Override
    public void close() {
        super.close();
        Misc.free(epoll);
        LOG.info().$("closed").$();
    }

    @Override
    public boolean idlePark(long maxParkNanos) {
        try {
            return runPass((int) Math.max(1, Math.min(pollBlockTimeoutMillis, maxParkNanos / 1_000_000L)));
        } finally {
            releaseRunLock();
        }
    }

    /**
     * A worker may park in the poll when blocking is configured, no accept is pending, the
     * dispatcher is not closing, it has found nothing to do for a millisecond, every connection is
     * armed in the poll (connection count = pending rows), and one of them asks for it, see
     * {@link IOContext#isIdlePollTarget()} (on HTTP: a QWP connection). A connection that is not
     * armed is out with a worker or waits on a heartbeat, and comes back through the interest
     * queue, which cannot wake the poll: its next request would wait out the park. With every
     * connection armed, what can need the dispatcher is a socket event, which wakes the poll, an
     * out-of-band disconnect, or a timer (idle connections, heartbeats, the listener's), and those
     * wait at most for the park, as they would for the worker's sleep. Without a connection that
     * asks for it, the worker sleeps as it always did.
     */
    @Override
    public boolean tryBeginIdlePark() {
        if (pollBlockTimeoutMillis == 0 || !tryAcquireRunLock()) {
            return false;
        }
        final int pendingCount = pending.size();
        if (closed
                || pendingAccept
                || getConnectionCount() != pendingCount
                || System.nanoTime() - lastUsefulNanos < IDLE_PARK_MIN_IDLE_NANOS
                || !hasIdlePollTarget(pendingCount)) {
            releaseRunLock();
            return false;
        }
        return true;
    }

    private boolean hasIdlePollTarget(int pendingCount) {
        for (int i = 0; i < pendingCount; i++) {
            if (pending.get(i).isIdlePollTarget()) {
                return true;
            }
        }
        return false;
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
        int op = operation == IOOperation.READ ? EpollAccessor.EPOLLIN : EpollAccessor.EPOLLOUT;
        if (context.getSocket().wantsTlsRead()) {
            op |= EpollAccessor.EPOLLIN;
        }
        if (context.getSocket().wantsTlsWrite()) {
            op |= EpollAccessor.EPOLLOUT;
        }
        return op;
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
        final int requestedOp = (int) pending.get(row, OPM_OPERATION);
        // We check EPOLLOUT flag and treat all other events, including EPOLLIN and EPOLLHUP, as a read.
        final boolean readyForWrite = (epoll.getEvent() & EpollAccessor.EPOLLOUT) > 0;
        final boolean readyForRead = !readyForWrite || (epoll.getEvent() & EpollAccessor.EPOLLIN) > 0;

        if ((requestedOp == IOOperation.WRITE && readyForWrite) || (requestedOp == IOOperation.READ && readyForRead)) {
            // If the socket is also ready for another operation type, do it.
            if (context.getSocket().tlsIO(tlsIOFlags(requestedOp, readyForRead, readyForWrite)) < 0) {
                doDisconnect(context, DISCONNECT_SRC_TLS_ERROR);
                pending.deleteRow(row);
                return true;
            }
            publishOperation(requestedOp, context);
            pending.deleteRow(row);
            return true;
        }

        // It's something different from the requested operation.
        if (context.getSocket().tlsIO(tlsIOFlags(readyForRead, readyForWrite)) < 0) {
            doDisconnect(context, DISCONNECT_SRC_TLS_ERROR);
            pending.deleteRow(row);
            return true;
        }
        rearmEpoll(context, id, requestedOp);
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
                    // Should the connection still be counted, connection count and pending rows
                    // would differ from now on, and no worker would wait in this poll again (see
                    // tryBeginIdlePark()): the optimisation would be lost, nothing else.
                    continue;
                } else {
                    epollCmd = EpollAccessor.EPOLL_CTL_ADD;
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

    // One pass of the dispatcher. The poll waits for up to blockMillis for a socket event, unless
    // the pass already found something to do.
    private boolean runPass(int blockMillis) {
        long timestamp = clock.getTicks();
        boolean useful = processDisconnects(timestamp);
        final int n;
        if (useful || blockMillis == 0) {
            n = epoll.poll();
        } else {
            n = epoll.poll(blockMillis);
            timestamp = clock.getTicks();
        }
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

        if (processRegistrations(timestamp) || useful) {
            if (pollBlockTimeoutMillis > 0) {
                lastUsefulNanos = System.nanoTime();
            }
            return true;
        }
        return false;
    }

    @Override
    protected boolean runSerially() {
        // todo: introduce fairness factor
        //  current worker impl will still proceed to execute another job even if this one was useful
        //  we should see if we can stay inside of this method until we have a completely idle iteration
        //  at the same time we should hog this thread in case we are always 'useful', we can probably
        //  introduce a loop count after which we always exit
        return runPass(0);
    }

    @Override
    protected void unregisterListenerFd() {
        epoll.removeListen(serverFd);
    }
}
