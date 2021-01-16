/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.cutlass.line.tcp;

import io.questdb.cutlass.line.tcp.LineTcpMeasurementScheduler.LineTcpMeasurementEvent;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.IOContext;
import io.questdb.network.IODispatcher;
import io.questdb.network.NetworkFacade;
import io.questdb.std.Mutable;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.DirectByteCharSequence;

class LineTcpConnectionContext implements IOContext, Mutable {
    private static final Log LOG = LogFactory.getLog(LineTcpConnectionContext.class);
    private static final long QUEUE_FULL_LOG_HYSTERESIS_IN_MS = 10_000;
    protected final NetworkFacade nf;
    private final LineTcpMeasurementScheduler scheduler;
    private final MillisecondClock milliClock;
    private final DirectByteCharSequence byteCharSequence = new DirectByteCharSequence();
    protected long fd;
    protected IODispatcher<LineTcpConnectionContext> dispatcher;
    protected long recvBufStart;
    protected long recvBufEnd;
    protected long recvBufPos;
    protected boolean peerDisconnected;
    private boolean queueFull;
    private long lastQueueFullLogMillis = 0;

    LineTcpConnectionContext(LineTcpReceiverConfiguration configuration, LineTcpMeasurementScheduler scheduler) {
        nf = configuration.getNetworkFacade();
        this.scheduler = scheduler;
        this.milliClock = configuration.getMillisecondClock();
        recvBufStart = Unsafe.malloc(configuration.getNetMsgBufferSize());
        recvBufEnd = recvBufStart + configuration.getNetMsgBufferSize();
    }

    @Override
    public void clear() {
        recvBufPos = recvBufStart;
        peerDisconnected = false;
        queueFull = false;
    }

    @Override
    public void close() {
        this.fd = -1;
        Unsafe.free(recvBufStart, recvBufEnd - recvBufStart);
        recvBufStart = recvBufEnd = recvBufPos = 0;
    }

    @Override
    public long getFd() {
        return fd;
    }

    @Override
    public boolean invalid() {
        return fd == -1;
    }

    @Override
    public IODispatcher<LineTcpConnectionContext> getDispatcher() {
        return dispatcher;
    }

    private boolean checkQueueFullLogHysteresis() {
        long millis = milliClock.getTicks();
        if ((millis - lastQueueFullLogMillis) >= QUEUE_FULL_LOG_HYSTERESIS_IN_MS) {
            lastQueueFullLogMillis = millis;
            return true;
        }
        return false;
    }

    protected final void compactBuffer(long recvBufNewStart) {
        assert recvBufNewStart <= recvBufPos;
        final int len = (int) (recvBufPos - recvBufNewStart);
        if (len > 0) {
            Unsafe.getUnsafe().copyMemory(recvBufNewStart, recvBufStart, len);
        }
        recvBufPos = recvBufStart + len;
    }

    private boolean doHandleDisconnectEvent() {
        if (recvBufPos == recvBufEnd) {
            LOG.error().$('[').$(fd).$("] buffer overflow [msgBufferSize=").$(recvBufEnd - recvBufStart).$(']').$();
            return true;
        }

        if (peerDisconnected) {
            // Peer disconnected, we have now finished disconnect our end
            if (recvBufPos != recvBufStart) {
                LOG.info().$('[').$(fd).$("] peer disconnected with partial measurement, ").$(recvBufPos - recvBufStart).$(" unprocessed bytes").$();
            } else {
                LOG.info().$('[').$(fd).$("] peer disconnected").$();
            }
        }
        return peerDisconnected;
    }

    private boolean handleDisconnectEvent() {
        if (recvBufPos < recvBufEnd && !peerDisconnected) {
            return false;
        }
        return doHandleDisconnectEvent();
    }

    IOContextResult handleIO() {
        while (read()) {
            try {
                // Process as much data as possible
                long recvBufLineStart = recvBufStart;
                final long lim = recvBufPos;
                queueFull = false;
                do {
                    final LineTcpMeasurementEvent event = scheduler.getNewEvent();
                    if (event != null) {
                        boolean success = true;
                        try {
                            final long recvBufLineNext = event.parseLine(recvBufLineStart, recvBufPos);
                            if (recvBufLineNext > -1 && event.isSuccess()) {
                                recvBufLineStart = recvBufLineNext;
                            } else if (recvBufLineNext == -1) {
                                // incomplete line
                                success = false;
                                break;
                            } else {
                                success = false;
                                LOG.error().$('[').$(fd).$("] could not parse measurement, code ").$(event.getErrorCode()).$(" at ").$(event.getErrorPosition()).$(" in ")
                                        .$(byteCharSequence.of(recvBufLineStart, Math.min(recvBufLineNext, recvBufPos))).$();
                                recvBufLineStart = recvBufLineNext;
                            }
                        } finally {
                            scheduler.commitNewEvent(event, success);
                        }
                    } else {
                        // Waiting for writer threads to drain queue, request callback as soon as possible
                        if (checkQueueFullLogHysteresis()) {
                            LOG.info().$('[').$(fd).$("] queue full, consider increasing queue size or number of writer jobs").$();
                        }
                        queueFull = true;
                        break;
                    }
                } while (recvBufLineStart != lim);

                // Compact input buffer
                assert recvBufLineStart <= recvBufPos;
                final int len = (int) (recvBufPos - recvBufLineStart);
                Unsafe.getUnsafe().copyMemory(recvBufLineStart, recvBufStart, len);
                recvBufPos = recvBufStart + len;

                if (queueFull) {
                    return IOContextResult.NEEDS_CPU;
                }

                if (handleDisconnectEvent()) {
                    return IOContextResult.NEEDS_DISCONNECT;
                }

            } catch (RuntimeException ex) {
                LOG.error().$('[').$(fd).$("] could not process line data").$(ex).$();
                return IOContextResult.NEEDS_DISCONNECT;
            }
        }

        if (handleDisconnectEvent()) {
            return IOContextResult.NEEDS_DISCONNECT;
        }
        return IOContextResult.NEEDS_READ;
    }

    LineTcpConnectionContext of(long clientFd, IODispatcher<LineTcpConnectionContext> dispatcher) {
        this.fd = clientFd;
        this.dispatcher = dispatcher;
        clear();
        return this;
    }

    protected final boolean read() {
        int bufferRemaining = (int) (recvBufEnd - recvBufPos);
        final int orig = bufferRemaining;
        while (bufferRemaining > 0 && !peerDisconnected) {
            int nRead = nf.recv(fd, recvBufPos, bufferRemaining);
            if (nRead > 0) {
                recvBufPos += nRead;
                bufferRemaining -= nRead;
            } else {
                peerDisconnected = nRead < 0;
                break;
            }
        }
        return bufferRemaining < orig || queueFull;
    }

    enum IOContextResult {
        NEEDS_READ, NEEDS_WRITE, NEEDS_CPU, NEEDS_DISCONNECT
    }
}