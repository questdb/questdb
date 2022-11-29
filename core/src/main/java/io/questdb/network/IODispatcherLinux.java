/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

public class IODispatcherLinux<C extends IOContext> extends AbstractIODispatcher<C> {
    private static final int M_ID = 2;
    private final Epoll epoll;
    private long fdid = 1;

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
        this.epoll.close();
        LOG.info().$("closed").$();
    }

    private void enqueuePending(int watermark) {
        for (int i = watermark, sz = pending.size(), offset = 0; i < sz; i++, offset += EpollAccessor.SIZEOF_EVENT) {
            epoll.setOffset(offset);

            final int fd = (int) pending.get(i, M_FD);
            final long id = pending.get(i, M_ID);
            int operation = (int) pending.get(i, M_OP);
            int epollOp = EpollAccessor.EPOLL_CTL_MOD;
            if (operation < 0) {
                // This is a new connection.
                operation = initialBias == IODispatcherConfiguration.BIAS_READ ? IOOperation.READ : IOOperation.WRITE;
                epollOp = EpollAccessor.EPOLL_CTL_ADD;
            }

            if (
                epoll.control(
                    fd,
                    id,
                    epollOp,
                    operation == IOOperation.READ ? EpollAccessor.EPOLLIN : EpollAccessor.EPOLLOUT
                ) < 0
            ) {
                LOG.debug().$("epoll_ctl failure ").$(nf.errno()).$();
            }
        }
    }

    private void processIdleConnections(long deadline) {
        int count = 0;
        for (int i = 0, n = pending.size(); i < n && pending.get(i, M_TIMESTAMP) < deadline; i++, count++) {
            doDisconnect(pending.get(i), DISCONNECT_SRC_IDLE);
        }
        pending.zapTop(count);
    }

    private boolean processRegistrations(long timestamp) {
        boolean useful = false;
        long cursor;
        int offset = 0;
        while ((cursor = interestSubSeq.next()) > -1) {
            IOEvent<C> evt = interestQueue.get(cursor);
            C context = evt.context;
            int requestedOperation = evt.operation;
            interestSubSeq.done(cursor);

            useful = true;
            final long id = fdid++;
            int fd = (int) context.getFd();
            int operation = requestedOperation;
            int epollOp = EpollAccessor.EPOLL_CTL_MOD;
            LOG.debug().$("processing registration [fd=").$(fd).$(", op=").$(operation).$(", id=").$(id).$(']').$();

            final SuspendEvent suspendEvent = context.getSuspendEvent();
            if (suspendEvent != null) {
                // Looks like we need to suspend the original operation.
                fd = suspendEvent.getFd();
                operation = IOOperation.READ;
                epollOp = EpollAccessor.EPOLL_CTL_ADD;
                LOG.debug().$("registering suspend event [fd=").$(fd).$(", op=").$(operation).$(", id=").$(id).$(']').$();
            }

            // we re-arm epoll globally, in that even when we disconnect
            // because we have to remove FD from epoll
            epoll.setOffset(offset);
            if (
                epoll.control(
                    fd,
                    id,
                    epollOp,
                    operation == IOOperation.READ ? EpollAccessor.EPOLLIN : EpollAccessor.EPOLLOUT
                ) < 0
            ) {
                System.out.println("oops2: " + nf.errno());
            }
            offset += EpollAccessor.SIZEOF_EVENT;

            int r = pending.addRow();
            pending.set(r, M_TIMESTAMP, timestamp);
            pending.set(r, M_FD, fd);
            pending.set(r, M_ID, id);
            pending.set(r, M_OP, requestedOperation);
            pending.set(r, context);
        }

        if (offset > 0) {
            LOG.debug().$("reg").$();
        }

        return useful;
    }

    @Override
    protected void pendingAdded(int index) {
        pending.set(index, M_ID, fdid++);
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
                } else {
                    // find row in pending for two reasons:
                    // 1. find payload
                    // 2. remove row from pending, remaining rows will be timed out
                    int row = pending.binarySearch(id, M_ID);
                    if (row < 0) {
                        LOG.error().$("internal error: epoll returned unexpected id [id=").$(id).$(']').$();
                        continue;
                    }

                    final C context = pending.get(row);
                    useful |= !context.isLowPriority();
                    final int operation = (int) pending.get(row, M_OP);
                    final SuspendEvent suspendEvent = context.getSuspendEvent();
                    if (suspendEvent != null) {
                        // The original operation was suspended, so let's resume it.
                        // To do that, we add a new pending operation above the watermark.
                        int newRow = pending.addRow();
                        pending.set(newRow, M_TIMESTAMP, timestamp);
                        pending.set(newRow, M_FD, context.getFd());
                        pending.set(newRow, M_OP, operation);
                        pending.set(newRow, context);
                        pendingAdded(newRow);
                        context.clearSuspendEvent();
                    } else {
                        publishOperation(
                            (epoll.getEvent() & EpollAccessor.EPOLLIN) > 0 ? IOOperation.READ : IOOperation.WRITE,
                            context
                        );
                    }
                    pending.deleteRow(row);
                    watermark--;
                }
            }

            // process rows over watermark
            if (watermark < pending.size()) {
                enqueuePending(watermark);
            }
        }

        // process timed out connections
        final long deadline = timestamp - idleConnectionTimeout;
        if (pending.size() > 0 && pending.get(0, M_TIMESTAMP) < deadline) {
            processIdleConnections(deadline);
            useful = true;
        }

        return processRegistrations(timestamp) || useful;
    }

    @Override
    protected void unregisterListenerFd() {
        this.epoll.removeListen(serverFd);
    }
}
