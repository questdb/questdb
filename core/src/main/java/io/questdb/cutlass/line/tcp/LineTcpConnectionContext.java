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

package io.questdb.cutlass.line.tcp;

import io.questdb.Metrics;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CommitFailedException;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.security.DenyAllSecurityContext;
import io.questdb.cairo.security.SecurityContextFactory;
import io.questdb.cutlass.auth.AuthenticatorException;
import io.questdb.cutlass.auth.SocketAuthenticator;
import io.questdb.cutlass.line.tcp.LineTcpParser.ParseResult;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.network.IOContext;
import io.questdb.network.NetworkFacade;
import io.questdb.network.TlsSessionInitFailedException;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Utf8StringObjHashMap;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Utf8String;

public class LineTcpConnectionContext extends IOContext<LineTcpConnectionContext> {
    private static final Log LOG = LogFactory.getLog(LineTcpConnectionContext.class);
    private static final long QUEUE_FULL_LOG_HYSTERESIS_IN_MS = 10_000;
    protected final NetworkFacade nf;
    private final SocketAuthenticator authenticator;
    private final DirectUtf8String byteCharSequence = new DirectUtf8String();
    private final long checkIdleInterval;
    private final long commitInterval;
    private final LineTcpReceiverConfiguration configuration;
    private final boolean disconnectOnError;
    private final long idleTimeout;
    private final boolean logMessageOnError;
    private final Metrics metrics;
    private final MillisecondClock milliClock;
    private final LineTcpParser parser;
    private final AdaptiveRecvBuffer recvBuffer;
    private final LineTcpMeasurementScheduler scheduler;
    private final Utf8StringObjHashMap<TableUpdateDetails> tableUpdateDetailsUtf8 = new Utf8StringObjHashMap<>();
    protected boolean peerDisconnected;
    protected SecurityContext securityContext = DenyAllSecurityContext.INSTANCE;
    private boolean goodMeasurement;
    private long lastQueueFullLogMillis = 0;
    private long nextCheckIdleTime;
    private long nextCommitTime;

    public LineTcpConnectionContext(LineTcpReceiverConfiguration configuration, LineTcpMeasurementScheduler scheduler) {
        super(
                configuration.getFactoryProvider().getLineSocketFactory(),
                configuration.getNetworkFacade(),
                LOG
        );

        try {
            this.configuration = configuration;
            this.nf = configuration.getNetworkFacade();
            this.disconnectOnError = configuration.getDisconnectOnError();
            this.logMessageOnError = configuration.logMessageOnError();
            this.scheduler = scheduler;
            this.metrics = configuration.getMetrics();
            this.milliClock = configuration.getMillisecondClock();
            parser = new LineTcpParser();
            recvBuffer = new AdaptiveRecvBuffer(parser, MemoryTag.NATIVE_ILP_RSS);
            this.authenticator = configuration.getFactoryProvider().getLineAuthenticatorFactory().getLineTCPAuthenticator();
            clear();
            this.checkIdleInterval = configuration.getMaintenanceInterval();
            this.commitInterval = configuration.getCommitInterval();
            long now = milliClock.getTicks();
            this.nextCheckIdleTime = now + checkIdleInterval;
            this.nextCommitTime = now + commitInterval;
            this.idleTimeout = configuration.getWriterIdleTimeout();
        } catch (Throwable t) {
            close();
            throw t;
        }
    }

    public void checkIdle(long millis) {
        for (int n = tableUpdateDetailsUtf8.size() - 1; n >= 0; n--) {
            final Utf8String tableNameUtf8 = tableUpdateDetailsUtf8.keys().get(n);
            final TableUpdateDetails tud = tableUpdateDetailsUtf8.get(tableNameUtf8);
            if (millis - tud.getLastMeasurementMillis() >= idleTimeout) {
                tableUpdateDetailsUtf8.remove(tableNameUtf8);
                tud.close();
            }
        }
    }

    @Override
    public void clear() {
        super.clear();
        securityContext = DenyAllSecurityContext.INSTANCE;
        authenticator.clear();
        Misc.free(recvBuffer);
        peerDisconnected = false;
        goodMeasurement = true;
        ObjList<Utf8String> keys = tableUpdateDetailsUtf8.keys();
        for (int n = keys.size() - 1; n >= 0; --n) {
            final Utf8String tableNameUtf8 = keys.get(n);
            final TableUpdateDetails tud = tableUpdateDetailsUtf8.get(tableNameUtf8);
            tud.close();
            tableUpdateDetailsUtf8.remove(tableNameUtf8);
        }
    }

    @Override
    public void close() {
        clear();
        Misc.free(authenticator);
        Misc.free(parser);
    }

    public long commitWalTables(long wallClockMillis) {
        long minTableNextCommitTime = Long.MAX_VALUE;
        for (int n = 0, sz = tableUpdateDetailsUtf8.size(); n < sz; n++) {
            final Utf8String tableNameUtf8 = tableUpdateDetailsUtf8.keys().get(n);
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
                        LOG.info().$("closing writer because table has been dropped (2) [table=").$(tud.getTableToken()).I$();
                        tud.setWriterInError();
                        tud.releaseWriter(false);
                    } else {
                        LOG.critical().$("commit failed [table=").$(tud.getTableToken()).$(",ex=").$(ex).I$();
                    }
                } catch (Throwable ex) {
                    LOG.critical().$("commit failed [table=").$(tud.getTableToken()).$(",ex=").$(ex).I$();
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

    public TableUpdateDetails getTableUpdateDetails(DirectUtf8Sequence tableName) {
        return tableUpdateDetailsUtf8.get(tableName);
    }

    public IOContextResult handleIO(NetworkIOJob netIoJob) {
        if (authenticator.isAuthenticated()) {
            read();
            try {
                IOContextResult parseResult = parseMeasurements(netIoJob);
                doMaintenance(milliClock.getTicks());
                return parseResult;
            } finally {
                netIoJob.releaseWalTableDetails();
            }
        } else {
            // uncommon branch in a separate method to avoid polluting common path
            return handleAuthentication(netIoJob);
        }
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
        if (parser.getBufferAddress() == recvBuffer.getBufEnd()) {
            LOG.error().$('[').$(getFd()).$("] buffer overflow [line.tcp.max.recv.buffer.size=")
                    .$(recvBuffer.getBufEnd() - recvBuffer.getBufStart()).$(']').$();
            return;
        }

        if (peerDisconnected) {
            // Peer disconnected, we have now finished disconnect our end
            if (recvBuffer.getBufPos() != recvBuffer.getBufStart()) {
                LOG.info().$('[').$(getFd()).$("] peer disconnected with partial measurement, ")
                        .$(recvBuffer.getBufPos() - recvBuffer.getBufStart())
                        .$(" unprocessed bytes").$();
            } else {
                LOG.info().$('[').$(getFd()).$("] peer disconnected").$();
            }
        }
    }

    private IOContextResult handleAuthentication(NetworkIOJob netIoJob) {
        try {
            int result = authenticator.handleIO();
            switch (result) {
                case SocketAuthenticator.NEEDS_WRITE:
                    return IOContextResult.NEEDS_WRITE;
                case SocketAuthenticator.OK:
                    assert authenticator.isAuthenticated();
                    assert securityContext == DenyAllSecurityContext.INSTANCE;
                    securityContext = configuration.getFactoryProvider().getSecurityContextFactory().getInstance(
                            authenticator.getPrincipal(),
                            authenticator.getAuthType(),
                            SecurityContextFactory.ILP
                    );
                    try {
                        securityContext.checkEntityEnabled();
                    } catch (CairoException e) {
                        LOG.error().$('[').$(getFd()).$("] ").$safe(e.getFlyweightMessage()).$();
                        return IOContextResult.NEEDS_DISCONNECT;
                    }

                    recvBuffer.setBufPos(authenticator.getRecvBufPos());
                    resetParser(authenticator.getRecvBufPseudoStart());
                    return parseMeasurements(netIoJob);
                case SocketAuthenticator.NEEDS_READ:
                    return IOContextResult.NEEDS_READ;
                case SocketAuthenticator.NEEDS_DISCONNECT:
                    return IOContextResult.NEEDS_DISCONNECT;
                case SocketAuthenticator.QUEUE_FULL:
                    return IOContextResult.QUEUE_FULL;
                default:
                    LOG.error().$("unexpected authenticator result [result=").$(result).I$();
                    return IOContextResult.NEEDS_DISCONNECT;
            }
        } catch (AuthenticatorException e) {
            return IOContextResult.NEEDS_DISCONNECT;
        }
    }

    private void logParseError() {
        int position = (int) (parser.getBufferAddress() - recvBuffer.getBufStartOfMeasurement());
        assert position >= 0;
        final LogRecord errorRec = LOG.error()
                .$('[').$(getFd())
                .$("] could not parse measurement, ").$(parser.getErrorCode())
                .$(" at ").$(position);
        if (logMessageOnError) {
            errorRec.$(", line (may be mangled due to partial parsing): '")
                    .$safe(byteCharSequence.of(recvBuffer.getBufStartOfMeasurement(), parser.getBufferAddress(), false))
                    .$("'");
        }
        errorRec.$();
    }

    private void resetParser(long pos) {
        parser.of(pos);
        goodMeasurement = true;
        recvBuffer.setBufStartOfMeasurement(pos);
    }

    void addTableUpdateDetails(Utf8String tableNameUtf8, TableUpdateDetails tableUpdateDetails) {
        tableUpdateDetailsUtf8.put(tableNameUtf8, tableUpdateDetails);
    }

    @Override
    protected void doInit() throws TlsSessionInitFailedException {
        if (recvBuffer.getBufStart() == 0) {
            recvBuffer.of(configuration.getRecvBufferSize(), configuration.getMaxRecvBufferSize());
            goodMeasurement = true;
        }

        authenticator.init(socket, recvBuffer.getBufStart(), recvBuffer.getBufEnd(), 0, 0);
        if (authenticator.isAuthenticated() && securityContext == DenyAllSecurityContext.INSTANCE) {
            // when security context has not been set by anything else (subclass) we assume
            // this is an authenticated, anonymous user
            securityContext = configuration.getFactoryProvider().getSecurityContextFactory().getInstance(
                    null,
                    SecurityContext.AUTH_TYPE_NONE,
                    SecurityContextFactory.ILP
            );
            securityContext.authorizeLineTcp();
        }

        if (socket.supportsTls()) {
            socket.startTlsSession(null);
        }
    }

    protected SecurityContext getSecurityContext() {
        return securityContext;
    }

    protected final IOContextResult parseMeasurements(NetworkIOJob netIoJob) {
        while (true) {
            try {
                ParseResult rc = goodMeasurement ? parser.parseMeasurement(recvBuffer.getBufPos()) : parser.skipMeasurement(recvBuffer.getBufPos());
                switch (rc) {
                    case MEASUREMENT_COMPLETE: {
                        if (goodMeasurement) {
                            if (scheduler.scheduleEvent(getSecurityContext(), netIoJob, this, parser)) {
                                // Waiting for writer threads to drain queue, request callback as soon as possible
                                if (checkQueueFullLogHysteresis()) {
                                    LOG.debug().$('[').$(getFd()).$("] queue full").$();
                                }
                                return IOContextResult.QUEUE_FULL;
                            }
                        } else {
                            logParseError();
                            goodMeasurement = true;
                        }

                        recvBuffer.startNewMeasurement();
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
                        if (!recvBuffer.tryCompactOrGrowBuffer()) {
                            doHandleDisconnectEvent();
                            return IOContextResult.NEEDS_DISCONNECT;
                        }

                        if (peerDisconnected) {
                            return IOContextResult.NEEDS_DISCONNECT;
                        }
                        return IOContextResult.NEEDS_READ;
                    }
                }
            } catch (CairoException ex) {
                LogRecord error = ex.isCritical() ? LOG.critical() : LOG.error();
                error
                        .$('[').$(getFd()).$("] could not process line data 1 [table=").$safe(parser.getMeasurementName())
                        .$(", msg=").$safe(ex.getFlyweightMessage())
                        .$(", errno=").$(ex.getErrno())
                        .I$();
                if (disconnectOnError) {
                    if (!ex.isAuthorizationError()) {
                        logParseError();
                    }
                    return IOContextResult.NEEDS_DISCONNECT;
                }
                goodMeasurement = false;
            } catch (Throwable ex) {
                LOG.critical()
                        .$('[').$(getFd()).$("] could not process line data 2 [table=").$safe(parser.getMeasurementName())
                        .$(", ex=").$(ex)
                        .I$();
                // This is a critical error, so we treat it as an unhandled one.
                metrics.healthMetrics().incrementUnhandledErrors();
                return IOContextResult.NEEDS_DISCONNECT;
            }
        }
    }

    protected boolean read() {
        long recvBufPos = recvBuffer.getBufPos();
        int bufferRemaining = (int) (recvBuffer.getBufEnd() - recvBufPos);
        final int orig = bufferRemaining;
        if (bufferRemaining > 0 && !peerDisconnected) {
            int bytesRead = socket.recv(recvBufPos, bufferRemaining);
            metrics.lineMetrics().totalIlpTcpBytesGauge().add(bytesRead);
            if (bytesRead > 0) {
                recvBuffer.setBufPos(recvBufPos + bytesRead);
                bufferRemaining -= bytesRead;
            } else {
                peerDisconnected = bytesRead < 0;
            }
            return bufferRemaining < orig;
        }
        return !peerDisconnected;
    }

    TableUpdateDetails removeTableUpdateDetails(DirectUtf8Sequence tableNameUtf8) {
        final int keyIndex = tableUpdateDetailsUtf8.keyIndex(tableNameUtf8);
        if (keyIndex < 0) {
            TableUpdateDetails tud = tableUpdateDetailsUtf8.valueAtQuick(keyIndex);
            tableUpdateDetailsUtf8.removeAt(keyIndex);
            return tud;
        }
        return null;
    }

    public enum IOContextResult {
        NEEDS_READ, NEEDS_WRITE, QUEUE_FULL, NEEDS_DISCONNECT
    }
}
