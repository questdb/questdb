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

package io.questdb.cutlass.line.tcp;

import io.questdb.Metrics;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CommitFailedException;
import io.questdb.cutlass.line.tcp.LineTcpParser.ParseResult;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.IOContext;
import io.questdb.network.NetworkFacade;
import io.questdb.std.*;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.ByteCharSequence;
import io.questdb.std.str.DirectByteCharSequence;

class LineTcpConnectionContext extends IOContext<LineTcpConnectionContext> {
    private static final Log LOG = LogFactory.getLog(LineTcpConnectionContext.class);
    private static final long QUEUE_FULL_LOG_HYSTERESIS_IN_MS = 10_000;
    protected final NetworkFacade nf;
    private final DirectByteCharSequence byteCharSequence = new DirectByteCharSequence();
    private final long checkIdleInterval;
    private final long commitInterval;
    private final boolean disconnectOnError;
    private final long idleTimeout;
    private final Metrics metrics;
    private final MillisecondClock milliClock;
    private final LineTcpParser parser;
    private final LineTcpMeasurementScheduler scheduler;
    private final ByteCharSequenceObjHashMap<TableUpdateDetails> tableUpdateDetailsUtf8 = new ByteCharSequenceObjHashMap<>();
    protected boolean peerDisconnected;
    protected long recvBufEnd;
    protected long recvBufPos;
    protected long recvBufStart;
    protected long recvBufStartOfMeasurement;
    private boolean goodMeasurement;
    private long lastQueueFullLogMillis = 0;
    private long nextCheckIdleTime;
    private long nextCommitTime;

    LineTcpConnectionContext(LineTcpReceiverConfiguration configuration, LineTcpMeasurementScheduler scheduler, Metrics metrics) {
        nf = configuration.getNetworkFacade();
        disconnectOnError = configuration.getDisconnectOnError();
        this.scheduler = scheduler;
        this.metrics = metrics;
        this.milliClock = configuration.getMillisecondClock();
        parser = new LineTcpParser(configuration.isStringAsTagSupported(), configuration.isSymbolAsFieldSupported());
        recvBufStart = Unsafe.malloc(configuration.getNetMsgBufferSize(), MemoryTag.NATIVE_ILP_RSS);
        recvBufEnd = recvBufStart + configuration.getNetMsgBufferSize();
        clear();
        this.checkIdleInterval = configuration.getMaintenanceInterval();
        this.commitInterval = configuration.getCommitInterval();
        long now = milliClock.getTicks();
        this.nextCheckIdleTime = now + checkIdleInterval;
        this.nextCommitTime = now + commitInterval;
        this.idleTimeout = configuration.getWriterIdleTimeout();
    }

    public void checkIdle(long millis) {
        for (int n = tableUpdateDetailsUtf8.size() - 1; n >= 0; n--) {
            final ByteCharSequence tableNameUtf8 = tableUpdateDetailsUtf8.keys().get(n);
            final TableUpdateDetails tud = tableUpdateDetailsUtf8.get(tableNameUtf8);
            if (millis - tud.getLastMeasurementMillis() >= idleTimeout) {
                tableUpdateDetailsUtf8.remove(tableNameUtf8);
                tud.close();
            }
        }
    }

    @Override
    public void clear() {
        recvBufPos = recvBufStart;
        peerDisconnected = false;
        resetParser();
        ObjList<ByteCharSequence> keys = tableUpdateDetailsUtf8.keys();
        for (int n = keys.size() - 1; n >= 0; --n) {
            final ByteCharSequence tableNameUtf8 = keys.get(n);
            final TableUpdateDetails tud = tableUpdateDetailsUtf8.get(tableNameUtf8);
            tud.close();
            tableUpdateDetailsUtf8.remove(tableNameUtf8);
        }
    }

    @Override
    public void close() {
        this.fd = -1;
        recvBufStart = recvBufEnd = recvBufPos = Unsafe.free(recvBufStart, recvBufEnd - recvBufStart, MemoryTag.NATIVE_ILP_RSS);
        clear();
    }

    public long commitWalTables(long wallClockMillis) {
        long minTableNextCommitTime = Long.MAX_VALUE;
        for (int n = 0, sz = tableUpdateDetailsUtf8.size(); n < sz; n++) {
            final ByteCharSequence tableNameUtf8 = tableUpdateDetailsUtf8.keys().get(n);
            final TableUpdateDetails tud = tableUpdateDetailsUtf8.get(tableNameUtf8);

            if (tud.isWal()) {
                final MillisecondClock millisecondClock = tud.getMillisecondClock();
                try {
                    long tableNextCommitTime = tud.commitIfIntervalElapsed(wallClockMillis);
                    // get current time again, commit is not instant and take quite some time.
                    wallClockMillis = millisecondClock.getTicks();
                    if (tableNextCommitTime < minTableNextCommitTime) {
                        // taking the earliest commit time
                        minTableNextCommitTime = tableNextCommitTime;
                    }
                } catch (CommitFailedException ex) {
                    if (ex.isTableDropped()) {
                        // table dropped, nothing to worry about
                        LOG.info().$("closing writer because table has been dropped (2) [table=").$(tud.getTableNameUtf16()).I$();
                        tud.setWriterInError();
                        tud.releaseWriter(false);
                    } else {
                        LOG.critical().$("commit failed [table=").$(tud.getTableNameUtf16()).$(",ex=").$(ex).I$();
                    }
                } catch (Throwable ex) {
                    LOG.critical().$("commit failed [table=").$(tud.getTableNameUtf16()).$(",ex=").$(ex).I$();
                }
            }
        }
        // if no tables, just use the default commit interval
        return minTableNextCommitTime != Long.MAX_VALUE ? minTableNextCommitTime : wallClockMillis + commitInterval;
    }

    public void doMaintenance(long now) {
        if (now > nextCommitTime) {
            nextCommitTime = commitWalTables(now);
        }

        if (now > nextCheckIdleTime) {
            checkIdle(now);
            nextCheckIdleTime = now + checkIdleInterval;
        }
    }

    public TableUpdateDetails getTableUpdateDetails(DirectByteCharSequence tableName) {
        return tableUpdateDetailsUtf8.get(tableName);
    }

    private boolean checkQueueFullLogHysteresis() {
        long millis = milliClock.getTicks();
        if ((millis - lastQueueFullLogMillis) >= QUEUE_FULL_LOG_HYSTERESIS_IN_MS) {
            lastQueueFullLogMillis = millis;
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

    private void logParseError() {
        int position = (int) (parser.getBufferAddress() - recvBufStartOfMeasurement);
        assert position >= 0;
        LOG.error()
                .$('[').$(fd)
                .$("] could not parse measurement, ").$(parser.getErrorCode())
                .$(" at ").$(position)
                .$(", line (may be mangled due to partial parsing): '")
                .$(byteCharSequence.of(recvBufStartOfMeasurement, parser.getBufferAddress())).$("'")
                .$();
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

    void addTableUpdateDetails(ByteCharSequence tableNameUtf8, TableUpdateDetails tableUpdateDetails) {
        tableUpdateDetailsUtf8.put(tableNameUtf8, tableUpdateDetails);
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

    IOContextResult handleIO(NetworkIOJob netIoJob) {
        read();
        try {
            IOContextResult parasResult = parseMeasurements(netIoJob);
            doMaintenance(milliClock.getTicks());
            return parasResult;
        } finally {
            netIoJob.releaseWalTableDetails();
        }
    }

    protected final IOContextResult parseMeasurements(NetworkIOJob netIoJob) {
        while (true) {
            try {
                ParseResult rc = goodMeasurement ? parser.parseMeasurement(recvBufPos) : parser.skipMeasurement(recvBufPos);
                switch (rc) {
                    case MEASUREMENT_COMPLETE: {
                        if (goodMeasurement) {
                            if (scheduler.scheduleEvent(netIoJob, this, parser)) {
                                // Waiting for writer threads to drain queue, request callback as soon as possible
                                if (checkQueueFullLogHysteresis()) {
                                    LOG.debug().$('[').$(fd).$("] queue full").$();
                                }
                                return IOContextResult.QUEUE_FULL;
                            }
                        } else {
                            logParseError();
                            goodMeasurement = true;
                        }

                        startNewMeasurement();

                        continue;
                    }

                    case ERROR: {
                        if (disconnectOnError) {
                            logParseError();
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
                LOG.error()
                        .$('[').$(fd).$("] could not process line data [table=").$(parser.getMeasurementName())
                        .$(", msg=").$(ex.getFlyweightMessage())
                        .$(", errno=").$(ex.getErrno())
                        .I$();
                if (disconnectOnError) {
                    logParseError();
                    return IOContextResult.NEEDS_DISCONNECT;
                }
                goodMeasurement = false;
            } catch (Throwable ex) {
                LOG.critical()
                        .$('[').$(fd).$("] could not process line data [table=").$(parser.getMeasurementName())
                        .$(", ex=").$(ex)
                        .I$();
                // This is a critical error, so we treat it as an unhandled one.
                metrics.health().incrementUnhandledErrors();
                return IOContextResult.NEEDS_DISCONNECT;
            }
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

    TableUpdateDetails removeTableUpdateDetails(DirectByteCharSequence tableNameUtf8) {
        final int keyIndex = tableUpdateDetailsUtf8.keyIndex(tableNameUtf8);
        if (keyIndex < 0) {
            TableUpdateDetails tud = tableUpdateDetailsUtf8.valueAtQuick(keyIndex);
            tableUpdateDetailsUtf8.removeAt(keyIndex);
            return tud;
        }
        return null;
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