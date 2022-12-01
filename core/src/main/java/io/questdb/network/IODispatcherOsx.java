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

public class IODispatcherOsx<C extends IOContext> extends AbstractIODispatcher<C> {

    private static final int M_ID = 3;
    private final int capacity;
    private final Kqueue kqueue;
    private long fdid = 1;

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

    private void enqueuePending(int watermark) {
        int index = 0;
        for (int i = watermark, sz = pending.size(), offset = 0; i < sz; i++, offset += KqueueAccessor.SIZEOF_KEVENT) {
            kqueue.setWriteOffset(offset);

            final int fd = (int) pending.get(i, M_FD);
            int operation = (int) pending.get(i, M_OPERATION);
            if (operation < 0) {
                // This is a new connection.
                operation = initialBias == IODispatcherConfiguration.BIAS_READ ? IOOperation.READ : IOOperation.WRITE;
            }

            if (operation == IOOperation.READ) {
                kqueue.readFD(fd, pending.get(i, M_ID));
                LOG.debug().$("kq [op=1, fd=").$(fd).$(", index=").$(index).$(", offset=").$(offset).$(']').$();
            } else {
                kqueue.writeFD(fd, pending.get(i, M_ID));
                LOG.debug().$("kq [op=2, fd=").$(fd).$(", index=").$(index).$(", offset=").$(offset).$(']').$();
            }
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

    private int findPending(long ts) {
        return pending.binarySearch(ts, M_ID);
    }

    private void processIdleConnections(long deadline) {
        int count = 0;
        for (int i = 0, n = pending.size(); i < n && pending.get(i, M_TIMESTAMP) < deadline; i++, count++) {
            doDisconnect(pending.get(i), DISCONNECT_SRC_IDLE);
        }
        pending.zapTop(count);
    }

    private boolean processRegistrations(long timestamp) {
        long cursor;
        boolean useful = false;
        int count = 0;
        int offset = 0;
        while ((cursor = interestSubSeq.next()) > -1) {
            final IOEvent<C> evt = interestQueue.get(cursor);
            C context = evt.context;
            int requestedOperation = evt.operation;
            interestSubSeq.done(cursor);

            useful = true;
            long id = fdid++;
            int fd = context.getFd();
            int operation = requestedOperation;
            LOG.debug().$("processing registration [fd=").$(fd).$(", op=").$(operation).$(", id=").$(id).$(']').$();

            final SuspendEvent suspendEvent = context.getSuspendEvent();
            if (suspendEvent != null) {
                // Looks like we need to suspend the original operation.
                fd = suspendEvent.getFd();
                operation = IOOperation.READ;
                LOG.debug().$("registering suspend event [fd=").$(fd).$(", op=").$(operation).$(", id=").$(id).$(']').$();
            }

            kqueue.setWriteOffset(offset);
            if (operation == IOOperation.READ) {
                kqueue.readFD(fd, id);
            } else {
                kqueue.writeFD(fd, id);
            }

            offset += KqueueAccessor.SIZEOF_KEVENT;
            count++;

            int r = pending.addRow();
            pending.set(r, M_TIMESTAMP, timestamp);
            pending.set(r, M_FD, fd);
            pending.set(r, M_ID, id);
            pending.set(r, M_OPERATION, requestedOperation);
            pending.set(r, context);

            if (count > capacity - 1) {
                registerWithKQueue(count);
                count = 0;
                offset = 0;
            }
        }

        if (count > 0) {
            registerWithKQueue(count);
        }

        return useful;
    }

    private void registerWithKQueue(int changeCount) {
        if (kqueue.register(changeCount) != 0) {
            throw NetworkError.instance(nf.errno()).put("could not register [changeCount=").put(changeCount).put(']');
        }
        LOG.debug().$("kqueued [count=").$(changeCount).$(']').$();
    }

    @Override
    protected void pendingAdded(int index) {
        pending.set(index, M_ID, fdid++);
    }

    @Override
    protected void registerListenerFd() {
        if (this.kqueue.listen(serverFd) != 0) {
            throw NetworkError.instance(nf.errno(), "could not kqueue.listen()");
        }
    }

    @Override
    protected boolean runSerially() {
        final long timestamp = clock.getTicks();
        processDisconnects(timestamp);
        boolean useful = false;
        final int n = kqueue.poll();
        int watermark = pending.size();
        int offset = 0;
        if (n > 0) {
            LOG.debug().$("poll [n=").$(n).$(']').$();
            // check all activated FDs
            for (int i = 0; i < n; i++) {
                kqueue.setReadOffset(offset);
                offset += KqueueAccessor.SIZEOF_KEVENT;
                int fd = kqueue.getFd();
                // this is server socket, accept if there aren't too many already
                if (fd == serverFd) {
                    accept(timestamp);
                    useful = true;
                } else {
                    // find row in pending for two reasons:
                    // 1. find payload
                    // 2. remove row from pending, remaining rows will be timed out
                    int row = findPending(kqueue.getData());
                    if (row < 0) {
                        findPending(kqueue.getData());
                        LOG.error().$("Internal error: unknown FD: ").$(fd).$();
                        continue;
                    }

                    C context = pending.get(row);
                    useful |= !context.isLowPriority();
                    final int operation = (int) pending.get(row, M_OPERATION);
                    final SuspendEvent suspendEvent = context.getSuspendEvent();
                    if (suspendEvent != null) {
                        // The original operation was suspended, so let's resume it.
                        // To do that, we add a new pending operation above the watermark.
                        int newRow = pending.addRow();
                        pending.set(newRow, M_TIMESTAMP, timestamp);
                        pending.set(newRow, M_FD, context.getFd());
                        pending.set(newRow, M_OPERATION, operation);
                        pending.set(newRow, context);
                        pendingAdded(newRow);
                        context.clearSuspendEvent();
                    } else {
                        publishOperation(kqueue.getFilter() == KqueueAccessor.EVFILT_READ ? IOOperation.READ : IOOperation.WRITE, context);
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
        if (this.kqueue.removeListen(serverFd) != 0) {
            throw NetworkError.instance(nf.errno(), "could not kqueue.removeListen()");
        }
    }
}
