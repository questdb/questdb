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

package io.questdb.cutlass.line.tcp;

import io.questdb.cairo.CairoException;
import io.questdb.cutlass.line.tcp.LineTcpParser.ParseResult;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.IOContext;
import io.questdb.network.IODispatcher;
import io.questdb.network.NetworkFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Mutable;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.std.str.FloatingDirectCharSink;

class LineTcpConnectionContext implements IOContext, Mutable {
    private static final Log LOG = LogFactory.getLog(LineTcpConnectionContext.class);
    private static final long QUEUE_FULL_LOG_HYSTERESIS_IN_MS = 10_000;
    protected final NetworkFacade nf;
    private final LineTcpMeasurementScheduler scheduler;
    private final MillisecondClock milliClock;
    private final DirectByteCharSequence byteCharSequence = new DirectByteCharSequence();
    private final LineTcpParser parser = new LineTcpParser();
    private final FloatingDirectCharSink floatingDirectCharSink = new FloatingDirectCharSink();
    private final boolean disconnectOnError;
    protected long fd;
    protected IODispatcher<LineTcpConnectionContext> dispatcher;
    protected long recvBufStart;
    protected long recvBufEnd;
    protected long recvBufPos;
    protected boolean peerDisconnected;
    protected long recvBufStartOfMeasurement;
    private long lastQueueFullLogMillis = 0;
    private boolean goodMeasurement;

    LineTcpConnectionContext(LineTcpReceiverConfiguration configuration, LineTcpMeasurementScheduler scheduler) {
        nf = configuration.getNetworkFacade();
        disconnectOnError = configuration.getDisconnectOnError();
        this.scheduler = scheduler;
        this.milliClock = configuration.getMillisecondClock();
        recvBufStart = Unsafe.malloc(configuration.getNetMsgBufferSize(), MemoryTag.NATIVE_DEFAULT);
        recvBufEnd = recvBufStart + configuration.getNetMsgBufferSize();
        clear();
    }

    @Override
    public void clear() {
        recvBufPos = recvBufStart;
        peerDisconnected = false;
        resetParser();
    }

    @Override
    public void close() {
        this.fd = -1;
        Unsafe.free(recvBufStart, recvBufEnd - recvBufStart, MemoryTag.NATIVE_DEFAULT);
        recvBufStart = recvBufEnd = recvBufPos = 0;
        parser.close();
        floatingDirectCharSink.close();
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

    /**
     * Moves incompletely received measurement to start of the receive buffer. Also updates the state of the
     * context and protocol parser such that all pointers that point to the incomplete measurement will remain
     * valid. This allows protocol parser to resume execution from the point of where measurement ended abruptly
     *
     * @param recvBufStartOfMeasurement the address in receive buffer where incomplete measurement starts. Everything from
     *                                  this address to end of the receive buffer will be copied to the start of the
     *                                  receive buffer
     * @return true if there was an incomplete measurement in the first place
     */
    protected final boolean compactBuffer(long recvBufStartOfMeasurement) {
        assert recvBufStartOfMeasurement <= recvBufPos;
        if (recvBufStartOfMeasurement > recvBufStart) {
            final long len = recvBufPos - recvBufStartOfMeasurement;
            if (len > 0) {
                Vect.memmove(recvBufStart, recvBufStartOfMeasurement, len); // Use memmove, there may be an overlap
                final long shl = recvBufStartOfMeasurement - recvBufStart;
                parser.shl(shl);
                this.recvBufStartOfMeasurement -= shl;
            } else {
                assert len == 0;
                resetParser();
            }
            recvBufPos = recvBufStart + len;
            return true;
        }
        return false;
    }

    private void doHandleDisconnectEvent() {
        if (parser.getBufferAddress() == recvBufEnd) {
            LOG.error().$('[').$(fd).$("] buffer overflow [line.tcp.msg.buffer.size=").$(recvBufEnd - recvBufStart).$(']').$();
            return;
        }

        if (peerDisconnected) {
            // Peer disconnected, we have now finished disconnect our end
            if (recvBufPos != recvBufStart) {
                LOG.info().$('[').$(fd).$("] peer disconnected with partial measurement, ").$(recvBufPos - recvBufStart).$(" unprocessed bytes").$();
            } else {
                LOG.info().$('[').$(fd).$("] peer disconnected").$();
            }
        }
    }

    IOContextResult handleIO(NetworkIOJob netIoJob) {
        read();
        return parseMeasurements(netIoJob);
    }

    LineTcpConnectionContext of(long clientFd, IODispatcher<LineTcpConnectionContext> dispatcher) {
        this.fd = clientFd;
        this.dispatcher = dispatcher;
        clear();
        return this;
    }

    protected final IOContextResult parseMeasurements(NetworkIOJob netIoJob) {
        while (true) {
            try {
                ParseResult rc = goodMeasurement ? parser.parseMeasurement(recvBufPos) : parser.skipMeasurement(recvBufPos);
                switch (rc) {
                    case MEASUREMENT_COMPLETE: {
                        if (goodMeasurement) {
                            if (scheduler.scheduleEvent(netIoJob, parser, floatingDirectCharSink)) {
                                // Waiting for writer threads to drain queue, request callback as soon as possible
                                if (checkQueueFullLogHysteresis()) {
                                    LOG.debug().$('[').$(fd).$("] queue full").$();
                                }
                                return IOContextResult.QUEUE_FULL;
                            }
                        } else {
                            int position = (int) (parser.getBufferAddress() - recvBufStartOfMeasurement);
                            assert position >= 0;
                            LOG.error().$('[').$(fd).$("] could not parse measurement, code ").$(parser.getErrorCode()).$(" at ").$(position)
                                    .$(" line (may be mangled due to partial parsing) is ")
                                    .$(byteCharSequence.of(recvBufStartOfMeasurement, parser.getBufferAddress())).$();
                            goodMeasurement = true;
                        }

                        startNewMeasurement();

                        continue;
                    }

                    case ERROR: {
                        if (disconnectOnError) {
                            return IOContextResult.NEEDS_DISCONNECT;
                        }
                        goodMeasurement = false;
                        continue;
                    }

                    case BUFFER_UNDERFLOW: {
                        if (recvBufPos == recvBufEnd && !compactBuffer(recvBufStartOfMeasurement)) {
                            doHandleDisconnectEvent();
                            return IOContextResult.NEEDS_DISCONNECT;
                        }

                        if (!read()) {
                            if (peerDisconnected) {
                                return IOContextResult.NEEDS_DISCONNECT;
                            }
                            return IOContextResult.NEEDS_READ;
                        }
                        break;
                    }
                }
            } catch (CairoException ex) {
                LOG.error().
                        $('[').$(fd).$("] could not process line data [table=").$(parser.getMeasurementName())
                        .$(", msg=").$(ex.getFlyweightMessage())
                        .$(", errno=").$(ex.getErrno())
                        .I$();
                if (disconnectOnError) {
                    return IOContextResult.NEEDS_DISCONNECT;
                }
                goodMeasurement = false;
            } catch (Throwable ex) {
                LOG.error().
                        $('[').$(fd).$("] could not process line data [table=").$(parser.getMeasurementName()).
                        $(", ex=").$(ex)
                        .I$();
                return IOContextResult.NEEDS_DISCONNECT;
            }
        }
    }

    private void startNewMeasurement() {
        parser.startNextMeasurement();
        recvBufStartOfMeasurement = parser.getBufferAddress();
        // we ran out of buffer, move to start and start parsing new data from socket
        if (recvBufStartOfMeasurement == recvBufPos) {
            recvBufPos = recvBufStart;
            parser.of(recvBufStart);
            recvBufStartOfMeasurement = recvBufStart;
        }
    }

    protected boolean read() {
        int bufferRemaining = (int) (recvBufEnd - recvBufPos);
        final int orig = bufferRemaining;
        if (bufferRemaining > 0 && !peerDisconnected) {
            int bytesRead = nf.recv(fd, recvBufPos, bufferRemaining);
            if (bytesRead > 0) {
                recvBufPos += bytesRead;
                bufferRemaining -= bytesRead;
            } else {
                peerDisconnected = bytesRead < 0;
            }
            return bufferRemaining < orig;
        }
        return !peerDisconnected;
    }

    protected void resetParser() {
        parser.of(recvBufStart);
        goodMeasurement = true;
        recvBufStartOfMeasurement = recvBufStart;
    }

    enum IOContextResult {
        NEEDS_READ, NEEDS_WRITE, QUEUE_FULL, NEEDS_DISCONNECT
    }
}