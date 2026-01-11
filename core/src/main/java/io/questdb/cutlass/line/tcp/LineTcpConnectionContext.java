/*******************************************************************************
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

package io.questdb.cutlass.line.tcp;

import io.questdb.Metrics;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CommitFailedException;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.security.DenyAllSecurityContext;
import io.questdb.cairo.security.PrincipalContext;
import io.questdb.cairo.security.SecurityContextFactory;
import io.questdb.cutlass.auth.AuthenticatorException;
import io.questdb.cutlass.auth.SocketAuthenticator;
import io.questdb.cutlass.line.tcp.LineTcpParser.ParseResult;
import io.questdb.cutlass.line.tcp.v4.*;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.network.IOContext;
import io.questdb.network.NetworkFacade;
import io.questdb.network.TlsSessionInitFailedException;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.ReadOnlyObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.Utf8StringObjHashMap;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Utf8String;

public class LineTcpConnectionContext extends IOContext<LineTcpConnectionContext> {
    private static final DummyPrincipalContext DUMMY_CONTEXT = new DummyPrincipalContext();
    private static final Log LOG = LogFactory.getLog(LineTcpConnectionContext.class);
    private static final long QUEUE_FULL_LOG_HYSTERESIS_IN_MS = 10_000;

    // Protocol detection states
    private static final byte PROTOCOL_UNKNOWN = 0;
    private static final byte PROTOCOL_TEXT = 1;
    private static final byte PROTOCOL_V4 = 2;

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
    private byte protocolType = PROTOCOL_UNKNOWN;
    private IlpV4ProtocolHandler v4Handler;

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
        protocolType = PROTOCOL_UNKNOWN;
        if (v4Handler != null) {
            v4Handler.reset();
        }
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
        Misc.free(v4Handler);
        v4Handler = null;
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
                // Protocol detection on first data after authentication
                if (protocolType == PROTOCOL_UNKNOWN) {
                    IOContextResult detectResult = detectProtocol();
                    if (detectResult != null) {
                        return detectResult;
                    }
                }

                if (protocolType == PROTOCOL_V4) {
                    return handleV4Protocol(netIoJob);
                }

                // Text protocol - continue with normal parsing
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

    /**
     * Handles ILP v4 protocol processing.
     */
    private IOContextResult handleV4Protocol(NetworkIOJob netIoJob) {
        // Initialize handler on first v4 call
        if (v4Handler == null) {
            v4Handler = new IlpV4ProtocolHandler(configuration, (int) getFd(), milliClock);
            v4Handler.setRecvBuffer(recvBuffer.getBufStart());
        } else if (v4Handler.getState() == IlpV4ProtocolHandler.STATE_HANDSHAKE_WAIT_REQUEST
                && v4Handler.getRecvBufProcessed() == 0) {
            // Handler was reset (e.g., by clear() during connection reuse) - update buffer
            v4Handler.setRecvBuffer(recvBuffer.getBufStart());
        }

        // Process based on handler state
        int result = v4Handler.process(recvBuffer.getBufPos());

        switch (result) {
            case IlpV4ProtocolHandler.RESULT_NEEDS_READ:
                if (peerDisconnected) {
                    return IOContextResult.NEEDS_DISCONNECT;
                }
                // Check if buffer is full and needs to grow
                if (recvBuffer.getBufPos() == recvBuffer.getBufEnd()) {
                    // First compact V4 buffer to move processed data out
                    compactV4RecvBuffer();
                    // Then try to grow if still needed
                    if (recvBuffer.getBufPos() == recvBuffer.getBufEnd()) {
                        if (!recvBuffer.tryCompactOrGrowBuffer()) {
                            LOG.error().$('[').$(getFd()).$("] v4 message too large for buffer").$();
                            return IOContextResult.NEEDS_DISCONNECT;
                        }
                        // Update handler's buffer pointer after growth
                        v4Handler.setRecvBuffer(recvBuffer.getBufStart());
                    }
                }
                return IOContextResult.NEEDS_READ;

            case IlpV4ProtocolHandler.RESULT_NEEDS_WRITE:
                int sendResult = v4Handler.send(socket);
                if (sendResult == IlpV4ProtocolHandler.RESULT_DISCONNECT) {
                    return IOContextResult.NEEDS_DISCONNECT;
                }
                if (sendResult == IlpV4ProtocolHandler.RESULT_NEEDS_WRITE) {
                    return IOContextResult.NEEDS_WRITE;
                }
                // After send, continue processing
                return handleV4Protocol(netIoJob);

            case IlpV4ProtocolHandler.RESULT_CONTINUE:
                if (v4Handler.getState() == IlpV4ProtocolHandler.STATE_PROCESSING) {
                    // Process the message
                    int processResult = v4Handler.processMessage(
                            securityContext,
                            scheduler.getCairoEngine(),
                            createTableUpdateDetailsProvider(netIoJob),
                            recvBuffer.getBufPos()
                    );

                    // Compact the recv buffer after processing
                    compactV4RecvBuffer();

                    if (processResult == IlpV4ProtocolHandler.RESULT_NEEDS_WRITE) {
                        int sendResult2 = v4Handler.send(socket);
                        if (sendResult2 == IlpV4ProtocolHandler.RESULT_DISCONNECT) {
                            return IOContextResult.NEEDS_DISCONNECT;
                        }
                        if (sendResult2 == IlpV4ProtocolHandler.RESULT_NEEDS_WRITE) {
                            return IOContextResult.NEEDS_WRITE;
                        }
                    } else if (processResult == IlpV4ProtocolHandler.RESULT_DISCONNECT) {
                        return IOContextResult.NEEDS_DISCONNECT;
                    }

                    // Continue to check if more messages are pending
                    return handleV4Protocol(netIoJob);
                }
                // State changed, continue
                return handleV4Protocol(netIoJob);

            case IlpV4ProtocolHandler.RESULT_DISCONNECT:
            default:
                return IOContextResult.NEEDS_DISCONNECT;
        }
    }

    /**
     * Compacts the recv buffer by removing processed data.
     */
    private void compactV4RecvBuffer() {
        long processed = v4Handler.getRecvBufProcessed();
        long bufStart = recvBuffer.getBufStart();

        if (processed > bufStart) {
            long remaining = recvBuffer.getBufPos() - processed;
            if (remaining > 0) {
                // Move unprocessed data to start of buffer
                Unsafe.getUnsafe().copyMemory(processed, bufStart, remaining);
                recvBuffer.setBufPos(bufStart + remaining);
            } else {
                recvBuffer.setBufPos(bufStart);
            }
            // Reset handler's processed pointer
            v4Handler.setRecvBuffer(bufStart);
        }
    }

    /**
     * Creates a TableUpdateDetailsProvider for v4 processing.
     */
    private IlpV4ProtocolHandler.TableUpdateDetailsProvider createTableUpdateDetailsProvider(NetworkIOJob netIoJob) {
        return new IlpV4ProtocolHandler.TableUpdateDetailsProvider() {
            @Override
            public TableUpdateDetails getTableUpdateDetails(String tableName) {
                // Look up in our cache
                for (int i = 0, n = tableUpdateDetailsUtf8.size(); i < n; i++) {
                    Utf8String key = tableUpdateDetailsUtf8.keys().get(i);
                    if (key.toString().equals(tableName)) {
                        return tableUpdateDetailsUtf8.get(key);
                    }
                }

                // Try to get from scheduler (without columns - table must exist)
                return scheduler.getTableUpdateDetailsForV4(
                        securityContext,
                        netIoJob,
                        LineTcpConnectionContext.this,
                        tableName,
                        null
                );
            }

            @Override
            public TableUpdateDetails createTableUpdateDetails(
                    String tableName,
                    IlpV4TableBlockCursor tableBlock,
                    CairoEngine engine,
                    SecurityContext securityContext
            ) {
                // Extract column definitions from the cursor (this creates a small temp array)
                int columnCount = tableBlock.getColumnCount();
                IlpV4ColumnDef[] schema = new IlpV4ColumnDef[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    schema[i] = tableBlock.getColumnDef(i);
                }
                // Create table with column definitions from the cursor
                return scheduler.getTableUpdateDetailsForV4(
                        securityContext,
                        netIoJob,
                        LineTcpConnectionContext.this,
                        tableName,
                        schema
                );
            }
        };
    }

    /**
     * Detects the protocol based on the first bytes received.
     *
     * @return IOContextResult if more data is needed or disconnect required, null if detection complete
     */
    private IOContextResult detectProtocol() {
        long bufStart = recvBuffer.getBufStart();
        long bufPos = recvBuffer.getBufPos();
        int available = (int) (bufPos - bufStart);

        if (available == 0) {
            return IOContextResult.NEEDS_READ;
        }

        IlpV4ProtocolDetector.DetectionResult result = IlpV4ProtocolDetector.detect(bufStart, available);

        switch (result) {
            case V4_HANDSHAKE:
            case V4_DIRECT:
                protocolType = PROTOCOL_V4;
                LOG.info().$('[').$(getFd()).$("] detected ILP v4 protocol").$();
                return null;

            case TEXT_PROTOCOL:
                protocolType = PROTOCOL_TEXT;
                LOG.debug().$('[').$(getFd()).$("] detected text protocol").$();
                return null;

            case NEED_MORE_DATA:
                return IOContextResult.NEEDS_READ;

            case UNKNOWN:
            default:
                LOG.error().$('[').$(getFd()).$("] unknown protocol detected").$();
                return IOContextResult.NEEDS_DISCONNECT;
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
                            authenticator, SecurityContextFactory.ILP
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
                    DUMMY_CONTEXT, SecurityContextFactory.ILP
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

    private static class DummyPrincipalContext implements PrincipalContext {
        @Override
        public byte getAuthType() {
            return SecurityContext.AUTH_TYPE_NONE;
        }

        @Override
        public ReadOnlyObjList<CharSequence> getGroups() {
            return null;
        }

        @Override
        public CharSequence getPrincipal() {
            return null;
        }
    }
}
