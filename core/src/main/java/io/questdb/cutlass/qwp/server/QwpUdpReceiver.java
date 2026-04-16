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

package io.questdb.cutlass.qwp.server;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cutlass.http.processors.LineHttpProcessorConfiguration;
import io.questdb.cutlass.line.tcp.DefaultColumnTypes;
import io.questdb.cutlass.line.tcp.QwpWalAppender;
import io.questdb.cutlass.line.tcp.WalTableUpdateDetails;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpMessageCursor;
import io.questdb.cutlass.qwp.protocol.QwpMessageHeader;
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.cutlass.qwp.protocol.QwpTableBlockCursor;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.SynchronizedJob;
import io.questdb.mp.WorkerPool;
import io.questdb.network.NetworkError;
import io.questdb.network.NetworkFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.HEADER_SIZE;

public class QwpUdpReceiver extends SynchronizedJob implements Closeable {
    private static final Log LOG = LogFactory.getLog(QwpUdpReceiver.class);
    protected static final int DATAGRAM_LEFT_UNCOMMITTED_ROWS = 1;
    protected static final int DATAGRAM_TRIGGERED_COMMIT = 2;

    protected final int bufLen;
    protected final long commitInterval;
    protected final int maxUncommittedDatagrams;
    protected final NetworkFacade nf;
    protected final QwpTudCache tudCache;
    private final long buf;
    private final QwpUdpReceiverConfiguration configuration;
    private final SOCountDownLatch halted = new SOCountDownLatch(1);
    private final QwpMessageCursor messageCursor;
    private final QwpMessageHeader messageHeader;
    protected final MillisecondClock millisecondClock;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final SOCountDownLatch started = new SOCountDownLatch(1);
    private final QwpWalAppender walAppender;

    protected long fd;
    protected long nextCommitTime = Long.MAX_VALUE;
    protected long processedCount;
    protected long totalCount;
    private volatile boolean closed;
    private volatile boolean closedAcknowledged;
    private long droppedBadMagicCount;
    private long droppedBadVersionCount;
    private long droppedParseErrorCount;
    private long droppedTooShortCount;
    private long droppedTruncatedCount;

    public QwpUdpReceiver(QwpUdpReceiverConfiguration configuration, CairoEngine engine) {
        this(configuration, engine, null);
    }

    public QwpUdpReceiver(QwpUdpReceiverConfiguration configuration, CairoEngine engine, @Nullable WorkerPool workerPool) {
        this.configuration = configuration;
        this.nf = configuration.getNetworkFacade();
        this.bufLen = configuration.getMsgBufferSize();
        this.commitInterval = configuration.getCommitInterval();
        this.maxUncommittedDatagrams = configuration.getMaxUncommittedDatagrams();
        this.millisecondClock = engine.getConfiguration().getMillisecondClock();

        fd = nf.socketUdp();
        if (fd < 0) {
            int errno = nf.errno();
            LOG.error().$("cannot open UDP socket [errno=").$(errno).$(']').$();
            throw NetworkError.instance(errno, "cannot open UDP socket");
        }

        long buf = 0;
        QwpWalAppender walAppender = null;
        QwpTudCache tudCache = null;
        try {
            if (nf.bindUdp(fd, configuration.isUnicast() ? configuration.getBindIPv4Address() : 0, configuration.getPort())) {
                if (!configuration.isUnicast() && !nf.join(fd, configuration.getBindIPv4Address(), configuration.getGroupIPv4Address())) {
                    throw NetworkError.instance(nf.errno())
                            .put("cannot join group ")
                            .put("[fd=").put(fd)
                            .put(", bind=").ip(configuration.getBindIPv4Address())
                            .put(", group=").ip(configuration.getGroupIPv4Address())
                            .put(']');
                }
            } else {
                throw NetworkError.instance(nf.errno()).couldNotBindSocket(
                        "qwp-udp-receiver", configuration.getBindIPv4Address(), configuration.getPort()
                );
            }

            if (configuration.getReceiveBufferSize() != -1 && nf.setRcvBuf(fd, configuration.getReceiveBufferSize()) != 0) {
                LOG.error()
                        .$("could not set receive buffer size [fd=").$(fd)
                        .$(", size=").$(configuration.getReceiveBufferSize())
                        .$(", errno=").$(nf.errno())
                        .I$();
            }

            buf = Unsafe.malloc(bufLen, MemoryTag.NATIVE_ILP_RSS);

            walAppender = new QwpWalAppender(
                    configuration.isAutoCreateNewColumns(),
                    engine.getConfiguration().getMaxFileNameLength(),
                    engine.getConfiguration().getMaxSqlRecompileAttempts()
            );

            DefaultColumnTypes defaultColumnTypes = new DefaultColumnTypes(
                    new CustomHttpProcessorConfiguration(configuration, engine, bufLen)
            );

            tudCache = new QwpTudCache(
                    engine,
                    configuration.isAutoCreateNewColumns(),
                    configuration.isAutoCreateNewTables(),
                    defaultColumnTypes,
                    configuration.getDefaultPartitionBy(),
                    commitInterval,
                    engine.getConfiguration().getMaxUncommittedRows()
            );

            this.messageHeader = new QwpMessageHeader();
            this.messageCursor = new QwpMessageCursor(configuration.getMaxRowsPerTable());
            this.buf = buf;
            this.walAppender = walAppender;
            this.tudCache = tudCache;

            if (!configuration.isOwnThread() && workerPool != null) {
                workerPool.assign(this);
                logStarted();
            }
        } catch (Throwable e) {
            Misc.free(tudCache);
            Misc.free(walAppender);
            if (buf != 0) {
                Unsafe.free(buf, bufLen, MemoryTag.NATIVE_ILP_RSS);
            }
            if (fd > -1) {
                nf.close(fd);
                fd = -1;
            }
            throw e;
        }
    }

    @Override
    public void close() {
        if (fd > -1) {
            boolean wasRunning = running.compareAndSet(true, false);
            closed = true;

            // Close socket to unblock any blocking recvRaw() call
            if (nf.close(fd) != 0) {
                LOG.error().$("could not close [fd=").$(fd).$(", errno=").$(nf.errno()).$(']').$();
            } else {
                LOG.info().$("closed [fd=").$(fd).$(']').$();
            }

            if (wasRunning) {
                started.await();
                halted.await();
            }

            // Ensure no in-flight runSerially() before freeing resources.
            // After setting closed=true and closing the fd, recvRaw() returns
            // promptly and runSerially() exits. We spin-call run() until
            // runSerially() executes under the SynchronizedJob lock with
            // closed=true, confirming no concurrent access to shared resources.
            while (!closedAcknowledged) {
                this.run(0);
                Os.pause();
            }

            fd = -1;

            tudCache.commitAllBestEffort();
            Misc.free(tudCache);
            Misc.free(walAppender);
            Unsafe.free(buf, bufLen, MemoryTag.NATIVE_ILP_RSS);
        }
    }

    public long getDroppedBadMagicCount() {
        return droppedBadMagicCount;
    }

    public long getDroppedBadVersionCount() {
        return droppedBadVersionCount;
    }

    public long getDroppedParseErrorCount() {
        return droppedParseErrorCount;
    }

    public long getDroppedTooShortCount() {
        return droppedTooShortCount;
    }

    public long getDroppedTruncatedCount() {
        return droppedTruncatedCount;
    }

    public long getProcessedCount() {
        return processedCount;
    }

    public long getTotalDroppedCount() {
        return droppedBadMagicCount + droppedBadVersionCount + droppedParseErrorCount
                + droppedTooShortCount + droppedTruncatedCount;
    }

    @Override
    public boolean runSerially() {
        if (checkClosed()) {
            return false;
        }
        boolean ran = false;
        int count;
        while ((count = nf.recvRaw(fd, buf, bufLen)) > 0) {
            ran = true;
            int datagramState = processDatagram(buf, count);
            processedCount++;
            if ((datagramState & DATAGRAM_TRIGGERED_COMMIT) != 0) {
                totalCount = 0;
            }
            if ((datagramState & DATAGRAM_LEFT_UNCOMMITTED_ROWS) != 0) {
                totalCount++;
            }
            if (totalCount >= maxUncommittedDatagrams) {
                totalCount = 0;
                forceCommitAll();
                return true;
            }
        }
        if (nextCommitTime != Long.MAX_VALUE) {
            long wallClockMillis = millisecondClock.getTicks();
            if (wallClockMillis >= nextCommitTime) {
                nextCommitTime = tudCache.commitWalTables(wallClockMillis);
                return true;
            }
        }
        return ran;
    }

    public void start() {
        if (configuration.isOwnThread() && running.compareAndSet(false, true)) {
            Thread thread = new Thread(() -> {
                started.countDown();
                try {
                    if (configuration.ownThreadAffinity() != -1) {
                        Os.setCurrentThreadAffinity(configuration.ownThreadAffinity());
                    }
                    logStarted();
                    while (running.get()) {
                        runSerially();
                    }
                    LOG.info().$("shutdown").$();
                } finally {
                    Path.clearThreadLocals();
                    halted.countDown();
                }
            });
            thread.setName("qwp-udp-receiver");
            thread.start();
        }
    }

    private void logStarted() {
        LOG.info()
                .$("receiving on ")
                .$ip(configuration.getBindIPv4Address())
                .$(':')
                .$(configuration.getPort())
                .$(" [fd=").$(fd)
                .$(", commitInterval=").$(commitInterval)
                .$(", maxUncommittedDatagrams=").$(maxUncommittedDatagrams)
                .I$();
    }

    protected boolean checkClosed() {
        if (closed) {
            closedAcknowledged = true;
            return true;
        }
        return false;
    }

    protected int processDatagram(long address, int length) {
        if (length < HEADER_SIZE) {
            droppedTooShortCount++;
            return 0;
        }
        try {
            messageHeader.parse(address, length);
        } catch (QwpParseException e) {
            switch (e.getErrorCode()) {
                case INVALID_MAGIC -> droppedBadMagicCount++;
                case UNSUPPORTED_VERSION -> droppedBadVersionCount++;
                default -> droppedParseErrorCount++;
            }
            LOG.error().$("header parse error: ").$(e.getFlyweightMessage()).$();
            return 0;
        }
        long totalLength = HEADER_SIZE + messageHeader.getPayloadLength();
        if (totalLength > length) {
            droppedTruncatedCount++;
            LOG.error().$("payload extends beyond datagram [payloadLen=").$(messageHeader.getPayloadLength())
                    .$(", received=").$(length).$(']').$();
            return 0;
        }
        int datagramState = 0;
        try {
            messageCursor.of(address, (int) totalLength, null, null);
            while (messageCursor.hasNextTable()) {
                QwpTableBlockCursor tableBlock = messageCursor.nextTable();
                WalTableUpdateDetails tud = tudCache.getTableUpdateDetails(
                        AllowAllSecurityContext.INSTANCE,
                        tableBlock.getTableNameUtf8(),
                        tableBlock.getSchema(),
                        tableBlock,
                        configuration.getMaxTablesPerConnection()
                );
                if (tud == null) {
                    LOG.error().$("failed to get table update details for: ").$(tableBlock.getTableName()).$();
                    continue;
                }
                final boolean hadUncommittedRows = !tud.isFirstRow();
                tud.markMeasurement();
                walAppender.appendToWalStreaming(AllowAllSecurityContext.INSTANCE, tableBlock, tud);
                noteCommitDeadline(tud);
                final boolean hasUncommittedRows = !tud.isFirstRow();
                if (hasUncommittedRows) {
                    datagramState |= DATAGRAM_LEFT_UNCOMMITTED_ROWS;
                } else if (hadUncommittedRows || tableBlock.getRowCount() > 0) {
                    datagramState |= DATAGRAM_TRIGGERED_COMMIT;
                }
            }
        } catch (Throwable t) {
            droppedParseErrorCount++;
            LOG.error().$("datagram processing error: ").$(t.getMessage()).$();
            return 0;
        }
        return datagramState;
    }

    protected void forceCommitAll() {
        tudCache.commitAllBestEffort();
        nextCommitTime = millisecondClock.getTicks() + commitInterval;
    }

    protected void noteCommitDeadline(WalTableUpdateDetails tud) {
        long tableNextCommitTime = tud.getNextCommitTime();
        if (tableNextCommitTime < nextCommitTime) {
            nextCommitTime = tableNextCommitTime;
        }
    }

    private record CustomHttpProcessorConfiguration(
            QwpUdpReceiverConfiguration configuration,
            CairoEngine engine,
            int bufLen
    ) implements LineHttpProcessorConfiguration {

        @Override
        public boolean autoCreateNewColumns() {
            return configuration.isAutoCreateNewColumns();
        }

        @Override
        public boolean autoCreateNewTables() {
            return configuration.isAutoCreateNewTables();
        }

        @Override
        public CairoConfiguration getCairoConfiguration() {
            return engine.getConfiguration();
        }

        @Override
        public short getDefaultColumnTypeForFloat() {
            return ColumnType.DOUBLE;
        }

        @Override
        public short getDefaultColumnTypeForInteger() {
            return ColumnType.LONG;
        }

        @Override
        public int getDefaultPartitionBy() {
            return configuration.getDefaultPartitionBy();
        }

        @Override
        public int getDefaultTimestampColumnType() {
            return ColumnType.UNDEFINED;
        }

        @Override
        public CharSequence getInfluxPingVersion() {
            return "";
        }

        @Override
        public long getMaxRecvBufferSize() {
            return bufLen;
        }

        @Override
        public MicrosecondClock getMicrosecondClock() {
            return engine.getConfiguration().getMicrosecondClock();
        }

        @Override
        public int getQwpMaxRowsPerTable() {
            return configuration.getMaxRowsPerTable();
        }

        @Override
        public int getQwpMaxSchemasPerConnection() {
            return QwpConstants.DEFAULT_MAX_SCHEMAS_PER_CONNECTION;
        }

        @Override
        public int getQwpMaxTablesPerConnection() {
            return configuration.getMaxTablesPerConnection();
        }

        @Override
        public long getSymbolCacheWaitUsBeforeReload() {
            return 0;
        }

        @Override
        public byte getTimestampUnit() {
            return 0;
        }

        @Override
        public boolean isEnabled() {
            return true;
        }

        @Override
        public boolean isStringToCharCastAllowed() {
            return false;
        }

        @Override
        public boolean isUseLegacyStringDefault() {
            return false;
        }

        @Override
        public boolean logMessageOnError() {
            return false;
        }
    }
}
