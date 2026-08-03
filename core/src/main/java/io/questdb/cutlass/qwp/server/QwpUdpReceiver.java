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
import io.questdb.cairo.CairoException;
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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.HEADER_SIZE;

public class QwpUdpReceiver extends SynchronizedJob implements Closeable {
    private static final Log LOG = LogFactory.getLog(QwpUdpReceiver.class);
    protected static final int DATAGRAM_DROPPED = 4;
    protected static final int DATAGRAM_LEFT_UNCOMMITTED_ROWS = 1;
    protected static final int DATAGRAM_TRIGGERED_COMMIT = 2;

    protected final AtomicBoolean acceptOpen;
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
    private boolean isBufferFreed;
    private boolean isCommitAttempted;
    private boolean isSocketClosed;
    private boolean isStartAttempted;
    private boolean isTudCacheFreed;
    private boolean isWalAppenderFreed;
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
        this(configuration, engine, workerPool, new AtomicBoolean(true));
    }

    public QwpUdpReceiver(QwpUdpReceiverConfiguration configuration, CairoEngine engine, @Nullable WorkerPool workerPool, AtomicBoolean acceptOpen) {
        this.acceptOpen = acceptOpen;
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
    public synchronized void close() {
        if (!closeBy(System.nanoTime() + WorkerPool.DEFAULT_HALT_TIMEOUT_NANOS)) {
            throw new IllegalStateException("QWP UDP receiver did not halt");
        }
    }

    public synchronized boolean closeBy(long deadlineNanos) {
        if (fd > -1) {
            running.set(false);
            closed = true;
            if (isStartAttempted
                    && (!started.await(Math.max(0, deadlineNanos - System.nanoTime()))
                    || !halted.await(Math.max(0, deadlineNanos - System.nanoTime())))) {
                return false;
            }

            while (!closedAcknowledged) {
                this.run();
                if (closedAcknowledged) {
                    break;
                }
                if (System.nanoTime() >= deadlineNanos) {
                    return false;
                }
                Os.pause();
            }

            if (!isSocketClosed) {
                if (nf.close(fd) != 0) {
                    try {
                        LOG.error().$("could not close [fd=").$(fd).$(", errno=").$(nf.errno()).$(']').$();
                    } catch (Throwable ignore) {
                    }
                } else {
                    isSocketClosed = true;
                }
            }

            if (System.nanoTime() >= deadlineNanos) {
                return false;
            }

            Throwable cleanupFailure = null;
            if (!isCommitAttempted) {
                try {
                    if (!tudCache.isCommitAllBestEffortComplete(deadlineNanos)) {
                        return false;
                    }
                    isCommitAttempted = true;
                } catch (Throwable th) {
                    isCommitAttempted = true;
                    cleanupFailure = th;
                }
            }
            if (!isTudCacheFreed) {
                try {
                    Misc.free(tudCache);
                    isTudCacheFreed = true;
                } catch (Throwable th) {
                    if (cleanupFailure == null) {
                        cleanupFailure = th;
                    } else if (cleanupFailure != th) {
                        cleanupFailure.addSuppressed(th);
                    }
                }
            }
            if (!isWalAppenderFreed) {
                try {
                    Misc.free(walAppender);
                    isWalAppenderFreed = true;
                } catch (Throwable th) {
                    if (cleanupFailure == null) {
                        cleanupFailure = th;
                    } else if (cleanupFailure != th) {
                        cleanupFailure.addSuppressed(th);
                    }
                }
            }
            if (!isBufferFreed) {
                try {
                    Unsafe.free(buf, bufLen, MemoryTag.NATIVE_ILP_RSS);
                    isBufferFreed = true;
                } catch (Throwable th) {
                    if (cleanupFailure == null) {
                        cleanupFailure = th;
                    } else if (cleanupFailure != th) {
                        cleanupFailure.addSuppressed(th);
                    }
                }
            }
            if (isSocketClosed && isBufferFreed && isTudCacheFreed && isWalAppenderFreed) {
                final long closedFd = fd;
                fd = -1;
                try {
                    LOG.info().$("closed [fd=").$(closedFd).$(']').$();
                } catch (Throwable ignore) {
                }
            }
            CairoException.rethrowCleanupFailure(cleanupFailure);
            if (!isSocketClosed) {
                return false;
            }
        }
        return true;
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
    public boolean run(@NotNull WorkerContext workerContext) {
        // Close-acknowledgment path: once closed=true, close() spins this.run()
        // until runSerially() executes under the SyncJob lock and checkClosed()
        // sets closedAcknowledged. Bypass the acceptOpen short-circuit in that
        // case so the spin can make progress. The receiver is already closed,
        // so no ingestion can happen even if super.run() is invoked.
        if (closed) {
            return super.run(workerContext);
        }
        if (!acceptOpen.get()) {
            return false;
        }
        return super.run(workerContext);
    }

    @Override
    public boolean runSerially() {
        if (checkClosed()) {
            return false;
        }
        if (!acceptOpen.get()) {
            // Mirror the worker-path acceptOpen gate so the own-thread driver
            // also quiesces after switchRole publishes acceptOpen=false. Placed
            // AFTER checkClosed() so close()'s acknowledgment spin (which sets
            // closedAcknowledged inside checkClosed()) can still progress.
            return false;
        }
        boolean ran = false;
        int count;
        while ((count = nf.recvRaw(fd, buf, bufLen)) > 0) {
            if (checkClosed()) {
                return ran;
            }
            ran = true;
            if (!acceptOpen.get()) {
                return true;
            }
            int datagramState = processDatagram(buf, count);
            if ((datagramState & DATAGRAM_DROPPED) == 0) {
                processedCount++;
            }
            if ((datagramState & DATAGRAM_TRIGGERED_COMMIT) != 0) {
                totalCount = 0;
            }
            if ((datagramState & DATAGRAM_LEFT_UNCOMMITTED_ROWS) != 0) {
                totalCount++;
            }
            if (totalCount >= maxUncommittedDatagrams) {
                if (checkClosed()) {
                    return true;
                }
                totalCount = 0;
                forceCommitAll();
                return true;
            }
        }
        if (checkClosed()) {
            return ran;
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

    public synchronized void start() {
        if (configuration.isOwnThread() && fd > -1 && !isSocketClosed && !isStartAttempted) {
            isStartAttempted = true;
            running.set(true);
            final Thread thread;
            try {
                thread = createThread(() -> {
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
            } catch (Throwable th) {
                resetFailedStart();
                throw th;
            }
            try {
                thread.start();
            } catch (Throwable th) {
                if (thread.getState() == Thread.State.NEW) {
                    resetFailedStart();
                }
                throw th;
            }
        }
    }

    protected boolean checkClosed() {
        if (closed) {
            closedAcknowledged = true;
            return true;
        }
        return false;
    }

    protected Thread createThread(Runnable runnable) {
        return new Thread(runnable);
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

    protected int processDatagram(long address, int length) {
        if (length < HEADER_SIZE) {
            droppedTooShortCount++;
            return DATAGRAM_DROPPED;
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
            return DATAGRAM_DROPPED;
        }
        long totalLength = HEADER_SIZE + messageHeader.getPayloadLength();
        if (totalLength > length) {
            droppedTruncatedCount++;
            LOG.error().$("payload extends beyond datagram [payloadLen=").$(messageHeader.getPayloadLength())
                    .$(", received=").$(length).$(']').$();
            return DATAGRAM_DROPPED;
        }
        int datagramState = 0;
        try {
            messageCursor.of(address, (int) totalLength, null);
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
            return DATAGRAM_DROPPED;
        }
        return datagramState;
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

    private void resetFailedStart() {
        running.set(false);
        started.countDown();
        halted.countDown();
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
        public int getQwpMaxTablesPerConnection() {
            return configuration.getMaxTablesPerConnection();
        }

        @Override
        public long getQwpMaxUncommittedRows() {
            return QwpConstants.DEFAULT_MAX_UNCOMMITTED_ROWS;
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
