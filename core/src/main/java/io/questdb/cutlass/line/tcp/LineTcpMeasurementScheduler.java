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

import io.questdb.Telemetry;
import io.questdb.TelemetryOrigin;
import io.questdb.TelemetrySystemEvent;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CommitFailedException;
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.WorkerPool;
import io.questdb.network.IODispatcher;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.SimpleReadWriteLock;
import io.questdb.std.Utf8StringObjHashMap;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;
import io.questdb.tasks.TelemetryTask;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.util.Arrays;
import java.util.concurrent.locks.ReadWriteLock;

public class LineTcpMeasurementScheduler implements Closeable {
    private static final Log LOG = LogFactory.getLog(LineTcpMeasurementScheduler.class);
    private final ObjList<TableUpdateDetails>[] assignedTables;
    private final boolean autoCreateNewColumns;
    private final boolean autoCreateNewTables;
    private final MillisecondClock clock;
    private final LineTcpReceiverConfiguration configuration;
    private final MemoryMARW ddlMem = Vm.getCMARWInstance();
    private final DefaultColumnTypes defaultColumnTypes;
    private final CairoEngine engine;
    private final LowerCaseCharSequenceObjHashMap<TableUpdateDetails> idleTableUpdateDetailsUtf16;
    private final LineWalAppender lineWalAppender;
    private final long[] loadByWriterThread;
    private final NetworkIOJob[] netIoJobs;
    private final Path path = new Path();
    private final MPSequence[] pubSeq;
    private final RingQueue<LineTcpMeasurementEvent>[] queue;
    private final long spinLockTimeoutMs;
    private final StringSink[] tableNameSinks;
    private final TableStructureAdapter tableStructureAdapter;
    private final ReadWriteLock tableUpdateDetailsLock = new SimpleReadWriteLock();
    private final LowerCaseCharSequenceObjHashMap<TableUpdateDetails> tableUpdateDetailsUtf16;
    private final Telemetry<TelemetryTask> telemetry;
    private final long writerIdleTimeout;

    public LineTcpMeasurementScheduler(
            LineTcpReceiverConfiguration lineConfiguration,
            CairoEngine engine,
            WorkerPool sharedPoolNetwork,
            IODispatcher<LineTcpConnectionContext> dispatcher,
            WorkerPool sharedPoolWrite
    ) {
        try {
            this.engine = engine;
            this.telemetry = engine.getTelemetry();
            CairoConfiguration cairoConfiguration = engine.getConfiguration();
            this.configuration = lineConfiguration;
            this.clock = cairoConfiguration.getMillisecondClock();
            this.spinLockTimeoutMs = cairoConfiguration.getSpinLockTimeout();
            this.defaultColumnTypes = new DefaultColumnTypes(lineConfiguration);
            final int networkSharedPoolSize = sharedPoolNetwork.getWorkerCount();
            this.netIoJobs = new NetworkIOJob[networkSharedPoolSize];
            this.tableNameSinks = new StringSink[networkSharedPoolSize];
            for (int i = 0; i < networkSharedPoolSize; i++) {
                tableNameSinks[i] = new StringSink();
                NetworkIOJob netIoJob = createNetworkIOJob(dispatcher, i);
                netIoJobs[i] = netIoJob;
                sharedPoolNetwork.assign(i, netIoJob);
                sharedPoolNetwork.freeOnExit(netIoJob);
            }

            // Worker count is set to 1 because we do not use this execution context
            // in worker threads.
            tableUpdateDetailsUtf16 = new LowerCaseCharSequenceObjHashMap<>();
            idleTableUpdateDetailsUtf16 = new LowerCaseCharSequenceObjHashMap<>();
            loadByWriterThread = new long[sharedPoolWrite.getWorkerCount()];
            autoCreateNewTables = lineConfiguration.getAutoCreateNewTables();
            autoCreateNewColumns = lineConfiguration.getAutoCreateNewColumns();
            int maxMeasurementSize = lineConfiguration.getMaxMeasurementSize();
            int queueSize = lineConfiguration.getWriterQueueCapacity();
            long commitInterval = configuration.getCommitInterval();
            int nWriterThreads = sharedPoolWrite.getWorkerCount();
            pubSeq = new MPSequence[nWriterThreads];
            //noinspection unchecked
            queue = new RingQueue[nWriterThreads];
            //noinspection unchecked
            assignedTables = new ObjList[nWriterThreads];
            for (int i = 0; i < nWriterThreads; i++) {
                MPSequence ps = new MPSequence(queueSize);
                pubSeq[i] = ps;

                RingQueue<LineTcpMeasurementEvent> q = new RingQueue<>(
                        (address, addressSize) -> new LineTcpMeasurementEvent(
                                address,
                                addressSize,
                                lineConfiguration.getTimestampUnit(),
                                defaultColumnTypes,
                                lineConfiguration.isStringToCharCastAllowed(),
                                lineConfiguration.getMaxFileNameLength(),
                                lineConfiguration.getAutoCreateNewColumns()
                        ),
                        getEventSlotSize(maxMeasurementSize),
                        queueSize,
                        MemoryTag.NATIVE_ILP_RSS
                );

                queue[i] = q;
                SCSequence subSeq = new SCSequence();
                ps.then(subSeq).then(ps);

                assignedTables[i] = new ObjList<>();

                final LineTcpLegacyWriterJob lineTcpLegacyWriterJob = new LineTcpLegacyWriterJob(
                        i,
                        q,
                        subSeq,
                        clock,
                        commitInterval, this, engine.getMetrics(), assignedTables[i]
                );
                sharedPoolWrite.assign(i, lineTcpLegacyWriterJob);
                sharedPoolWrite.freeOnExit(lineTcpLegacyWriterJob);
            }
            this.tableStructureAdapter = new TableStructureAdapter(
                    cairoConfiguration,
                    defaultColumnTypes,
                    configuration.getDefaultPartitionBy(),
                    cairoConfiguration.getWalEnabledDefault()
            );
            writerIdleTimeout = lineConfiguration.getWriterIdleTimeout();
            lineWalAppender = new LineWalAppender(
                    autoCreateNewColumns,
                    configuration.isStringToCharCastAllowed(),
                    configuration.getTimestampUnit(),
                    cairoConfiguration.getMaxFileNameLength()
            );
        } catch (Throwable t) {
            close();
            throw t;
        }
    }

    @Override
    public void close() {
        tableUpdateDetailsLock.writeLock().lock();
        try {
            closeLocals(tableUpdateDetailsUtf16);
            closeLocals(idleTableUpdateDetailsUtf16);
        } finally {
            tableUpdateDetailsLock.writeLock().unlock();
        }

        Misc.free(path);
        Misc.free(ddlMem);
        for (int i = 0, n = assignedTables.length; i < n; i++) {
            Misc.freeObjList(assignedTables[i]);
            assignedTables[i].clear();
        }
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0, n = queue.length; i < n; i++) {
            Misc.free(queue[i]);
        }
        for (int i = 0, n = netIoJobs.length; i < n; i++) {
            netIoJobs[i].close();
        }
    }

    public boolean doMaintenance(
            Utf8StringObjHashMap<TableUpdateDetails> tableUpdateDetailsUtf8,
            int readerWorkerId,
            long millis
    ) {
        for (int n = 0, sz = tableUpdateDetailsUtf8.size(); n < sz; n++) {
            final Utf8String tableNameUtf8 = tableUpdateDetailsUtf8.keys().get(n);
            final TableUpdateDetails tud = tableUpdateDetailsUtf8.get(tableNameUtf8);

            if (millis - tud.getLastMeasurementMillis() >= writerIdleTimeout) {
                tableUpdateDetailsLock.writeLock().lock();
                try {
                    if (tud.getNetworkIOOwnerCount() == 1) {
                        final int writerWorkerId = tud.getWriterThreadId();
                        final long seq = getNextPublisherEventSequence(writerWorkerId);
                        if (seq > -1) {
                            LineTcpMeasurementEvent event = queue[writerWorkerId].get(seq);
                            event.createWriterReleaseEvent(tud, true);
                            tableUpdateDetailsUtf8.remove(tableNameUtf8);
                            final CharSequence tableNameUtf16 = tud.getTableNameUtf16();
                            tableUpdateDetailsUtf16.remove(tableNameUtf16);
                            idleTableUpdateDetailsUtf16.put(tableNameUtf16, tud);
                            tud.removeReference(readerWorkerId);
                            pubSeq[writerWorkerId].done(seq);
                            LOG.info().$("active table going idle [tableName=").$safe(tableNameUtf16).I$();
                        }
                        return true;
                    } else {
                        tableUpdateDetailsUtf8.remove(tableNameUtf8);
                        tud.removeReference(readerWorkerId);
                    }
                    return sz > 1;
                } finally {
                    tableUpdateDetailsLock.writeLock().unlock();
                }
            }
        }
        return false;
    }

    public void processWriterReleaseEvent(LineTcpMeasurementEvent event, int workerId) {
        tableUpdateDetailsLock.readLock().lock();
        try {
            final TableUpdateDetails tub = event.getTableUpdateDetails();
            if (tub.getWriterThreadId() != workerId) {
                return;
            }
            if (!event.getTableUpdateDetails().isWriterInError() && tableUpdateDetailsUtf16.keyIndex(tub.getTableNameUtf16()) < 0) {
                // Table must have been re-assigned to an IO thread
                return;
            }
            LOG.info()
                    .$("releasing writer, its been idle since ").$ts(tub.getLastMeasurementMillis() * 1_000)
                    .$("[tableName=").$safe(tub.getTableNameUtf16())
                    .I$();

            event.releaseWriter();
        } finally {
            tableUpdateDetailsLock.readLock().unlock();
        }
    }

    public void releaseWalTableDetails(Utf8StringObjHashMap<TableUpdateDetails> tableUpdateDetailsUtf8) {
        ObjList<Utf8String> keys = tableUpdateDetailsUtf8.keys();
        for (int n = keys.size() - 1; n > -1; --n) {
            final Utf8String tableNameUtf8 = keys.getQuick(n);
            final TableUpdateDetails tud = tableUpdateDetailsUtf8.get(tableNameUtf8);
            if (tud.isWal()) {
                tableUpdateDetailsUtf8.remove(tableNameUtf8);
            }
        }
    }

    public boolean scheduleEvent(
            SecurityContext securityContext,
            NetworkIOJob netIoJob,
            LineTcpConnectionContext ctx,
            LineTcpParser parser
    ) throws Exception {
        DirectUtf8Sequence measurementName = parser.getMeasurementName();
        TableUpdateDetails tud;
        try {
            tud = ctx.getTableUpdateDetails(measurementName);
            if (tud == null) {
                tud = netIoJob.getLocalTableDetails(measurementName);
                if (tud == null) {
                    tud = getTableUpdateDetailsFromSharedArea(securityContext, netIoJob, ctx, parser);
                }
            } else if (tud.isWriterInError()) {
                TableUpdateDetails removed = ctx.removeTableUpdateDetails(measurementName);
                assert tud == removed;
                removed.close();
                tud = getTableUpdateDetailsFromSharedArea(securityContext, netIoJob, ctx, parser);
            }
        } catch (EntryUnavailableException ex) {
            // Table writer is locked
            LOG.info().$("could not get table writer [tableName=").$(measurementName)
                    .$(", ex=`")
                    .$(ex.getFlyweightMessage())
                    .$("`]").$();
            return true;
        } catch (CairoException ex) {
            // Table could not be created
            LOG.error().$("could not create table [tableName=").$(measurementName)
                    .$(", msg=").$safe(ex.getFlyweightMessage())
                    .$(", errno=").$(ex.getErrno())
                    .I$();
            // More details will be logged by catching thread
            throw ex;
        }

        if (tud.isWal()) {
            try {
                lineWalAppender.appendToWal(securityContext, parser, tud);
            } catch (CommitFailedException ex) {
                if (ex.isTableDropped()) {
                    // table dropped, nothing to worry about
                    LOG.info().$("closing writer because table has been dropped (1) [table=").$(measurementName).I$();
                    tud.setWriterInError();
                    tud.releaseWriter(false);
                    // continue to next line
                    return false;
                }
                handleAppendException(measurementName, tud, ex);
            } catch (Throwable ex) {
                handleAppendException(measurementName, tud, ex);
            }
            return false;
        }
        return dispatchEvent(securityContext, netIoJob, parser, tud);
    }

    private static long getEventSlotSize(int maxMeasurementSize) {
        return Numbers.ceilPow2((long) (maxMeasurementSize / 4) * (Integer.BYTES + Double.BYTES + 1));
    }

    private static void handleAppendException(DirectUtf8Sequence measurementName, TableUpdateDetails tud, Throwable ex) {
        tud.setWriterInError();
        LogRecord logRecord;
        if (ex instanceof CairoException && !((CairoException) ex).isCritical()) {
            logRecord = LOG.error();
        } else {
            logRecord = LOG.critical();
        }
        logRecord.$("closing writer because of error [table=").$(tud.getTableNameUtf16())
                .$(", ex=").$(ex)
                .I$();
        throw CairoException.critical(0).put("could not write ILP message to WAL [tableName=").put(measurementName).put(", error=").put(ex.getMessage()).put(']');
    }

    private void closeLocals(LowerCaseCharSequenceObjHashMap<TableUpdateDetails> tudUtf16) {
        ObjList<CharSequence> tableNames = tudUtf16.keys();
        for (int n = 0, sz = tableNames.size(); n < sz; n++) {
            tudUtf16.get(tableNames.get(n)).closeLocals();
        }
        tudUtf16.clear();
    }

    private boolean dispatchEvent(
            SecurityContext securityContext,
            NetworkIOJob netIoJob,
            LineTcpParser parser,
            TableUpdateDetails tud
    ) {
        final int writerThreadId = tud.getWriterThreadId();
        long seq = getNextPublisherEventSequence(writerThreadId);
        if (seq > -1) {
            try {
                if (tud.isWriterInError()) {
                    throw CairoException.critical(0).put("writer is in error, aborting ILP pipeline");
                }
                queue[writerThreadId].get(seq).createMeasurementEvent(securityContext, tud, parser, netIoJob.getWorkerId());
            } finally {
                pubSeq[writerThreadId].done(seq);
            }
            tud.incrementEventsProcessedSinceReshuffle();
            return false;
        }
        return true;
    }

    private TableUpdateDetails getTableUpdateDetailsFromSharedArea(
            SecurityContext securityContext,
            @NotNull NetworkIOJob netIoJob,
            @NotNull LineTcpConnectionContext ctx,
            @NotNull LineTcpParser parser
    ) {
        final DirectUtf8Sequence tableNameUtf8 = parser.getMeasurementName();
        final StringSink tableNameUtf16 = tableNameSinks[netIoJob.getWorkerId()];
        tableNameUtf16.clear();
        Utf8s.utf8ToUtf16(tableNameUtf8.lo(), tableNameUtf8.hi(), tableNameUtf16);

        tableUpdateDetailsLock.writeLock().lock();
        try {
            final long deadline = clock.getTicks() + spinLockTimeoutMs;
            while (true) {
                TableUpdateDetails tud;
                // check if the global cache has the table
                final int tudKeyIndex = tableUpdateDetailsUtf16.keyIndex(tableNameUtf16);
                if (tudKeyIndex < 0) {
                    // it does, which means that table is non-WAL
                    // we should not have "shared" WAL tables
                    tud = tableUpdateDetailsUtf16.valueAt(tudKeyIndex);
                } else {
                    final int status = engine.getTableStatus(path, tableNameUtf16);
                    if (status != TableUtils.TABLE_EXISTS) {
                        if (!autoCreateNewTables) {
                            throw CairoException.nonCritical()
                                    .put("table does not exist, creating new tables is disabled [table=").put(tableNameUtf16)
                                    .put(']');
                        }
                        if (!autoCreateNewColumns) {
                            throw CairoException.nonCritical()
                                    .put("table does not exist, cannot create table, creating new columns is disabled [table=").put(tableNameUtf16)
                                    .put(']');
                        }
                        // validate that parser entities do not contain NULLs
                        TableStructureAdapter tsa = tableStructureAdapter.of(tableNameUtf16, parser);
                        for (int i = 0, n = tsa.getColumnCount(); i < n; i++) {
                            if (tsa.getColumnType(i) == LineTcpParser.ENTITY_TYPE_NULL) {
                                throw CairoException.nonCritical().put("unknown column type [columnName=").put(tsa.getColumnName(i)).put(']');
                            }
                        }
                        engine.createTable(securityContext, ddlMem, path, true, tsa, false);
                    }
                    // by the time we get here, the table should exist on disk
                    // check the global idle cache - TUD can be there
                    final int idleTudKeyIndex = idleTableUpdateDetailsUtf16.keyIndex(tableNameUtf16);
                    if (idleTudKeyIndex < 0) {
                        // TUD is found in global idle cache - this meant it is non-WAL
                        tud = idleTableUpdateDetailsUtf16.valueAt(idleTudKeyIndex);
                        LOG.info().$("idle table going active [tableName=").$safe(tud.getTableNameUtf16()).I$();
                        if (tud.getWriter() == null) {
                            tud.closeNoLock();
                            // Use actual table name from the "details" to avoid case mismatches in the
                            // WriterPool. There was an error in the LineTcpReceiverFuzzTest, which helped
                            // to identify the cause
                            tud = unsafeAssignTableToWriterThread(tudKeyIndex, tud.getTableNameUtf16(), tud.getTableNameUtf8());
                        } else {
                            idleTableUpdateDetailsUtf16.removeAt(idleTudKeyIndex);
                            tableUpdateDetailsUtf16.putAt(tudKeyIndex, tud.getTableNameUtf16(), tud);
                        }
                    } else {
                        // check if table on disk is WAL
                        TableToken tableToken = engine.getTableTokenIfExists(tableNameUtf16);
                        if (tableToken == null) {
                            // someone already dropped the table
                            if (clock.getTicks() > deadline) {
                                throw CairoException.nonCritical()
                                        .put("could not create table within timeout [table=").put(tableNameUtf16)
                                        .put(", timeout=").put(spinLockTimeoutMs)
                                        .put(']');
                            }
                            continue; // go for another spin
                        }
                        if (tableToken.isMatView()) {
                            throw CairoException.nonCritical()
                                    .put("cannot modify materialized view [view=")
                                    .put(tableToken.getTableName())
                                    .put(']');
                        }
                        TelemetryTask.store(telemetry, TelemetryOrigin.ILP_TCP, TelemetrySystemEvent.ILP_RESERVE_WRITER);
                        if (engine.isWalTable(tableToken)) {
                            // create WAL-oriented TUD and DON'T add it to the global cache
                            tud = new WalTableUpdateDetails(
                                    engine,
                                    securityContext,
                                    engine.getWalWriter(tableToken),
                                    defaultColumnTypes,
                                    Utf8String.newInstance(tableNameUtf8),
                                    netIoJob.getSymbolCachePool(),
                                    configuration.getCommitInterval(),
                                    true,
                                    engine.getConfiguration().getMaxUncommittedRows()
                            );
                            ctx.addTableUpdateDetails(Utf8String.newInstance(tableNameUtf8), tud);
                            return tud;
                        } else {
                            tud = unsafeAssignTableToWriterThread(tudKeyIndex, tableNameUtf16, Utf8String.newInstance(tableNameUtf8));
                        }
                    }
                }

                // tud.getTableNameUtf8() can be different case from incoming tableNameUtf8
                Utf8String key = Utf8s.equals(tud.getTableNameUtf8(), tableNameUtf8) ? tud.getTableNameUtf8() : Utf8String.newInstance(tableNameUtf8);
                netIoJob.addTableUpdateDetails(key, tud);
                return tud;
            }
        } finally {
            tableUpdateDetailsLock.writeLock().unlock();
        }
    }

    private boolean isOpen() {
        return pubSeq != null;
    }

    @NotNull
    private TableUpdateDetails unsafeAssignTableToWriterThread(
            int tudKeyIndex,
            CharSequence tableNameUtf16,
            Utf8String tableNameUtf8
    ) {
        unsafeCalcThreadLoad();
        long leastLoad = Long.MAX_VALUE;
        int threadId = 0;

        for (int i = 0, n = loadByWriterThread.length; i < n; i++) {
            if (loadByWriterThread[i] < leastLoad) {
                leastLoad = loadByWriterThread[i];
                threadId = i;
            }
        }
        final TableUpdateDetails tud = new TableUpdateDetails(
                configuration,
                engine,
                null,
                // get writer here to avoid constructing
                // object instance and potentially leaking memory if
                // writer allocation fails
                engine.getTableWriterAPI(tableNameUtf16, "tcpIlp"),
                threadId,
                netIoJobs,
                defaultColumnTypes,
                tableNameUtf8
        );
        tableUpdateDetailsUtf16.putAt(tudKeyIndex, tud.getTableNameUtf16(), tud);
        LOG.info().$("assigned ").$safe(tableNameUtf16).$(" to thread ").$(threadId).$();
        return tud;
    }

    private void unsafeCalcThreadLoad() {
        Arrays.fill(loadByWriterThread, 0);
        ObjList<CharSequence> tableNames = tableUpdateDetailsUtf16.keys();
        for (int n = 0, sz = tableNames.size(); n < sz; n++) {
            final CharSequence tableName = tableNames.getQuick(n);
            final TableUpdateDetails stats = tableUpdateDetailsUtf16.get(tableName);
            if (stats != null) {
                loadByWriterThread[stats.getWriterThreadId()] += stats.getEventsProcessedSinceReshuffle();
            } else {
                LOG.error().$("could not find statistic for table [name=").$safe(tableName).I$();
            }
        }
    }

    protected NetworkIOJob createNetworkIOJob(IODispatcher<LineTcpConnectionContext> dispatcher, int workerId) {
        return new LineTcpNetworkIOJob(configuration, this, dispatcher, workerId);
    }

    long getNextPublisherEventSequence(int writerWorkerId) {
        assert isOpen();
        long seq;
        while ((seq = pubSeq[writerWorkerId].next()) == -2) {
            Os.pause();
        }
        return seq;
    }
}
