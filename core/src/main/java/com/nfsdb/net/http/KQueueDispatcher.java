/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.net.http;

import com.nfsdb.ex.NetworkError;
import com.nfsdb.iter.clock.Clock;
import com.nfsdb.log.Log;
import com.nfsdb.log.LogFactory;
import com.nfsdb.misc.Files;
import com.nfsdb.misc.Net;
import com.nfsdb.mp.*;
import com.nfsdb.net.Kqueue;
import com.nfsdb.net.NetworkChannelImpl;
import com.nfsdb.net.NonBlockingSecureSocketChannel;
import com.nfsdb.std.LongMatrix;

import java.io.IOException;

public class KQueueDispatcher extends SynchronizedJob implements IODispatcher {
    private static final Log LOG = LogFactory.getLog(KQueueDispatcher.class);

    private final long socketFd;
    private final RingQueue<IOEvent> ioQueue;
    private final Sequence ioSequence;
    private final RingQueue<IOEvent> interestQueue;
    private final MPSequence interestPubSequence;
    private final SCSequence interestSubSequence = new SCSequence();
    private final Clock clock;
    private final HttpServerConfiguration configuration;
    private final Kqueue kqueue;
    private final int timeout;
    private final LongMatrix<IOContext> pending = new LongMatrix<>(2);
    private final int maxConnections;
    private volatile int connectionCount = 0;

    public KQueueDispatcher(
            CharSequence ip,
            int port,
            RingQueue<IOEvent> ioQueue,
            Sequence ioSequence,
            Clock clock,
            HttpServerConfiguration configuration
    ) {
        this.ioQueue = ioQueue;
        this.ioSequence = ioSequence;
        this.interestQueue = new RingQueue<>(IOEvent.FACTORY, ioQueue.getCapacity());
        this.interestPubSequence = new MPSequence(interestQueue.getCapacity());
        this.interestPubSequence.followedBy(this.interestSubSequence);
        this.interestSubSequence.followedBy(this.interestPubSequence);
        this.clock = clock;
        this.configuration = configuration;
        this.maxConnections = configuration.getHttpMaxConnections();
        this.timeout = configuration.getHttpTimeout();

        // bind socket
        this.kqueue = new Kqueue();
        this.socketFd = Net.socketTcp(false);
        if (!Net.bind(this.socketFd, ip, port)) {
            throw new NetworkError("Failed to find socket");
        }
        Net.listen(this.socketFd, 128);
        this.kqueue.listen(socketFd);
        LOG.debug().$("Listening socket: ").$(socketFd).$();
    }

    @Override
    public void close() throws IOException {
        this.kqueue.close();
        Files.close(socketFd);
    }

    @Override
    public int getConnectionCount() {
        return connectionCount;
    }

    @Override
    public void registerChannel(IOContext context, ChannelStatus status) {
        long cursor = interestPubSequence.nextBully();
        IOEvent evt = interestQueue.get(cursor);
        evt.context = context;
        evt.status = status;
        LOG.debug().$("Re-queuing ").$(context.channel.getFd()).$();
        interestPubSequence.done(cursor);
    }

    @Override
    protected boolean _run() {
        boolean useful = false;
        final int n = kqueue.poll();
        int watermark = pending.size();
        final long timestamp = clock.getTicks();
        int offset = 0;
        if (n > 0) {
            // check all activated FDs
            for (int i = 0; i < n; i++) {
                kqueue.setOffset(offset);
                offset += Kqueue.SIZEOF_KEVENT;
                int fd = kqueue.getFd();
                // this is server socket, accept if there aren't too many already
                if (fd == socketFd) {
                    long _fd = accept();
                    if (_fd < 0) {
                        continue;
                    }
                    addPending(_fd, timestamp);
                } else {
                    // find row in pending for two reasons:
                    // 1. find payload
                    // 2. remove row from pending, remaining rows will be timed out
                    int row = findPending(fd, kqueue.getData());
                    if (row < 0) {
                        LOG.error().$("Internal error: unknown FD: ").$(fd).$();
                        continue;
                    }

                    long cursor = ioSequence.nextBully();
                    IOEvent evt = ioQueue.get(cursor);
                    evt.context = pending.get(row);
                    evt.status = kqueue.getFilter() == Kqueue.EVFILT_READ ? ChannelStatus.READ : ChannelStatus.WRITE;
                    ioSequence.done(cursor);
                    LOG.debug().$("Queuing ").$(kqueue.getFilter()).$(" on ").$(fd).$();
                    pending.deleteRow(row);
                    watermark--;
                }
            }

            // process rows over watermark
            if (watermark < pending.size()) {
                enqueuePending(watermark);
            }
            useful = true;
        }

        // process timed out connections
        long deadline = timestamp - timeout;
        if (pending.size() > 0 && pending.get(0, 0) < deadline) {
            processIdleConnections(deadline);
            useful = true;
        }

        return processRegistrations(timestamp) || useful;
    }

    private long accept() {
        long _fd = Net.accept(socketFd);
        LOG.debug().$(" Connected ").$(_fd).$();

        // something not right
        if (_fd < 0) {
            LOG.error().$("Error in accept: ").$(_fd).$();
            return -1;
        }

        if (Net.configureNonBlocking(_fd) < 0) {
            LOG.error().$("Cannot make FD non-blocking").$();
            Files.close(_fd);
            return -1;
        }

        connectionCount++;

        if (connectionCount > maxConnections) {
            LOG.info().$("Too many connections, kicking out ").$(_fd).$();
            Files.close(_fd);
            connectionCount--;
            return -1;
        }

        return _fd;
    }

    private void addPending(long _fd, long timestamp) {
        // append to pending
        // all rows below watermark will be registered with kqueue
        int r = pending.addRow();
        LOG.debug().$(" Matrix row ").$(r).$(" for ").$(_fd).$();
        pending.set(r, 0, timestamp);
        pending.set(r, 1, _fd);
        NetworkChannelImpl channel = new NetworkChannelImpl(_fd);
        pending.set(r, new IOContext(
                        configuration.getSslConfig().isSecure() ?
                                new NonBlockingSecureSocketChannel(channel, configuration.getSslConfig()) :
                                channel,
                        clock,
                        configuration.getHttpBufReqHeader(),
                        configuration.getHttpBufReqContent(),
                        configuration.getHttpBufReqMultipart(),
                        configuration.getHttpBufRespHeader(),
                        configuration.getHttpBufRespContent()
                )
        );
    }

    private void disconnect(IOContext context, DisconnectReason reason) {
        LOG.debug().$("Disconnected ").$(context.channel.getFd()).$(": ").$(reason).$();
        context.close();
        connectionCount--;
    }

    private void enqueuePending(int watermark) {
        int index = 0;
        for (int i = watermark, sz = pending.size(), offset = 0; i < sz; i++, offset += Kqueue.SIZEOF_KEVENT) {
            kqueue.setOffset(offset);
            kqueue.readFD((int) pending.get(i, 1), pending.get(i, 0));
            LOG.debug().$("kqueued ").$(pending.get(i, 1)).$(" as ").$(index - 1).$();
            if (++index > Kqueue.NUM_KEVENTS - 1) {
                kqueue.register(index);
                index = 0;
            }
        }
        if (index > 0) {
            kqueue.register(index);
            LOG.debug().$("Registered ").$(index).$();
        }
    }

    private int findPending(int fd, long ts) {
        int r = pending.binarySearch(ts);
        if (r < 0) {
            return r;
        }

        if (pending.get(r, 1) == fd) {
            return r;
        } else {
            return scanRow(r + 1, fd, ts);
        }
    }

    private void processIdleConnections(long deadline) {
        int count = 0;
        for (int i = 0, n = pending.size(); i < n && pending.get(i, 0) < deadline; i++, count++) {
            disconnect(pending.get(i), DisconnectReason.IDLE);
        }
        pending.zapTop(count);
    }

    private boolean processRegistrations(long timestamp) {
        long cursor;
        boolean useful = false;
        int count = 0;
        int offset = 0;
        while ((cursor = interestSubSequence.next()) > -1) {
            useful = true;
            IOEvent evt = interestQueue.get(cursor);
            IOContext context = evt.context;
            ChannelStatus op = evt.status;
            interestSubSequence.done(cursor);

            int fd = (int) context.channel.getFd();
            LOG.debug().$("Registering ").$(fd).$(" status ").$(op).$();
            kqueue.setOffset(offset);
            offset += Kqueue.SIZEOF_KEVENT;
            count++;
            switch (op) {
                case READ:
                    kqueue.readFD(fd, timestamp);
                    break;
                case WRITE:
                    kqueue.writeFD(fd, timestamp);
                    break;
                case DISCONNECTED:
                    disconnect(context, DisconnectReason.SILLY);
                    continue;
                case EOF:
                    disconnect(context, DisconnectReason.PEER);
                    continue;
            }

            int r = pending.addRow();
            pending.set(r, 0, timestamp);
            pending.set(r, 1, fd);
            pending.set(r, context);


            if (count > Kqueue.NUM_KEVENTS - 1) {
                kqueue.register(count);
                count = 0;
            }
        }

        if (count > 0) {
            kqueue.register(count);
        }

        return useful;
    }

    private int scanRow(int r, int fd, long ts) {
        for (int i = r, n = pending.size(); i < n; i++) {
            // timestamps not match?
            if (pending.get(i, 0) != ts) {
                return -(i + 1);
            }

            if (pending.get(i, 1) == fd) {
                return i;
            }
        }
        return -1;
    }
}
