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

import io.questdb.Telemetry;
import io.questdb.cairo.*;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.WorkerPool;
import io.questdb.network.IODispatcher;
import io.questdb.std.*;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.tasks.TelemetryTask;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.util.Arrays;
import java.util.concurrent.locks.ReadWriteLock;

class LineTcpMeasurementScheduler implements Closeable {
    private static final Log LOG = LogFactory.getLog(LineTcpMeasurementScheduler.class);
    private final ObjList<TableUpdateDetails>[] assignedTables;
    private final boolean autoCreateNewColumns;
    private final boolean autoCreateNewTables;
    private final LineTcpReceiverConfiguration configuration;
    private final MemoryMARW ddlMem = Vm.getMARWInstance();
    private final DefaultColumnTypes defaultColumnTypes;
    private final CairoEngine engine;
    private final LowerCaseCharSequenceObjHashMap<TableUpdateDetails> idleTableUpdateDetailsUtf16;
    private final long[] loadByWriterThread;
    private final NetworkIOJob[] netIoJobs;
    private final Path path = new Path();
    private final MPSequence[] pubSeq;
    private final RingQueue<LineTcpMeasurementEvent>[] queue;
    private final CairoSecurityContext securityContext;
    private final StringSink[] tableNameSinks;
    private final TableStructureAdapter tableStructureAdapter;
    private final ReadWriteLock tableUpdateDetailsLock = new SimpleReadWriteLock();
    private final LowerCaseCharSequenceObjHashMap<TableUpdateDetails> tableUpdateDetailsUtf16;
    private final long writerIdleTimeout;
    private LineTcpReceiver.SchedulerListener listener;

    LineTcpMeasurementScheduler(
            LineTcpReceiverConfiguration lineConfiguration,
            CairoEngine engine,
            WorkerPool ioWorkerPool,
            IODispatcher<LineTcpConnectionContext> dispatcher,
            WorkerPool writerWorkerPool
    ) {
        this.engine = engine;
        this.securityContext = lineConfiguration.getCairoSecurityContext();
        CairoConfiguration cairoConfiguration = engine.getConfiguration();
        this.configuration = lineConfiguration;
        MillisecondClock milliClock = cairoConfiguration.getMillisecondClock();
        this.defaultColumnTypes = new DefaultColumnTypes(lineConfiguration);
        int n = ioWorkerPool.getWorkerCount();
        this.netIoJobs = new NetworkIOJob[n];
        this.tableNameSinks = new StringSink[n];
        for (int i = 0; i < n; i++) {
            tableNameSinks[i] = new StringSink();
            NetworkIOJob netIoJob = createNetworkIOJob(dispatcher, i);
            netIoJobs[i] = netIoJob;
            ioWorkerPool.assign(i, netIoJob);
            ioWorkerPool.freeOnExit(netIoJob);
        }

        // Worker count is set to 1 because we do not use this execution context
        // in worker threads.
        tableUpdateDetailsUtf16 = new LowerCaseCharSequenceObjHashMap<>();
        idleTableUpdateDetailsUtf16 = new LowerCaseCharSequenceObjHashMap<>();
        loadByWriterThread = new long[writerWorkerPool.getWorkerCount()];
        autoCreateNewTables = lineConfiguration.getAutoCreateNewTables();
        autoCreateNewColumns = lineConfiguration.getAutoCreateNewColumns();
        int maxMeasurementSize = lineConfiguration.getMaxMeasurementSize();
        int queueSize = lineConfiguration.getWriterQueueCapacity();
        long commitIntervalDefault = configuration.getCommitIntervalDefault();
        int nWriterThreads = writerWorkerPool.getWorkerCount();
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
                            lineConfiguration.getMicrosecondClock(),
                            lineConfiguration.getTimestampAdapter(),
                            defaultColumnTypes,
                            lineConfiguration.isStringToCharCastAllowed(),
                            lineConfiguration.isSymbolAsFieldSupported(),
                            lineConfiguration.getMaxFileNameLength(),
                            lineConfiguration.getAutoCreateNewColumns(),
                            engine.getConfiguration().getDefaultSymbolCapacity(),
                            engine.getConfiguration().getDefaultSymbolCacheFlag()
                    ),
                    getEventSlotSize(maxMeasurementSize),
                    queueSize,
                    MemoryTag.NATIVE_ILP_RSS
            );

            queue[i] = q;
            SCSequence subSeq = new SCSequence();
            ps.then(subSeq).then(ps);

            assignedTables[i] = new ObjList<>();

            final LineTcpWriterJob lineTcpWriterJob = new LineTcpWriterJob(
                    i,
                    q,
                    subSeq,
                    milliClock,
                    commitIntervalDefault,
                    this,
                    engine.getMetrics(),
                    assignedTables[i]
            );
            writerWorkerPool.assign(i, lineTcpWriterJob);
            writerWorkerPool.freeOnExit(lineTcpWriterJob);
        }
        this.tableStructureAdapter = new TableStructureAdapter(cairoConfiguration, defaultColumnTypes, configuration.getDefaultPartitionBy());
        writerIdleTimeout = lineConfiguration.getWriterIdleTimeout();
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
        for (int i = 0, n = queue.length; i < n; i++) {
            Misc.free(queue[i]);
        }
    }

    public boolean doMaintenance(
            DirectByteCharSequenceObjHashMap<TableUpdateDetails> tableUpdateDetailsUtf8,
            int readerWorkerId,
            long millis
    ) {
        for (int n = 0, sz = tableUpdateDetailsUtf8.size(); n < sz; n++) {
            final String tableNameUtf8 = tableUpdateDetailsUtf8.keys().get(n);
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
                            final CharSequence tableNameUtf16 = tud.getUtf16TableName();
                            tableUpdateDetailsUtf16.remove(tableNameUtf16);
                            idleTableUpdateDetailsUtf16.put(tableNameUtf16, tud);
                            tud.removeReference(readerWorkerId);
                            pubSeq[writerWorkerId].done(seq);
                            if (listener != null) {
                                // table going idle
                                listener.onEvent(tableNameUtf16, 1);
                            }
                            LOG.info().$("active table going idle [tableName=").$(tableNameUtf16).I$();
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
            final TableUpdateDetails tab = event.getTableUpdateDetails();
            if (tab.getWriterThreadId() != workerId) {
                return;
            }
            if (!event.getTableUpdateDetails().isWriterInError() && tableUpdateDetailsUtf16.keyIndex(tab.getUtf16TableName()) < 0) {
                // Table must have been re-assigned to an IO thread
                return;
            }
            LOG.info()
                    .$("releasing writer, its been idle since ").$ts(tab.getLastMeasurementMillis() * 1_000)
                    .$("[tableName=").$(tab.getUtf16TableName())
                    .I$();

            event.releaseWriter();
        } finally {
            tableUpdateDetailsLock.readLock().unlock();
        }
    }

    private static long getEventSlotSize(int maxMeasurementSize) {
        return Numbers.ceilPow2((long) (maxMeasurementSize / 4) * (Integer.BYTES + Double.BYTES + 1));
    }

    private void closeLocals(LowerCaseCharSequenceObjHashMap<TableUpdateDetails> tudUtf16) {
        ObjList<CharSequence> tableNames = tudUtf16.keys();
        for (int n = 0, sz = tableNames.size(); n < sz; n++) {
            tudUtf16.get(tableNames.get(n)).closeLocals();
        }
        tudUtf16.clear();
    }

    private TableUpdateDetails createTableAndNewUpdateDetails(@NotNull NetworkIOJob netIoJob, @NotNull LineTcpParser parser) {
        final StringSink sink = tableNameSinks[netIoJob.getWorkerId()];
        sink.clear();

        final DirectByteCharSequence utf8TableName = parser.getUtf8TableName();
        CharSequence utf16TableName = LineTcpUtils.utf8Decode(utf8TableName, sink, parser.hasNonAsciiChars());

        tableUpdateDetailsLock.writeLock().lock();
        try {
            TableUpdateDetails tud;
            final int tudKeyIndex = tableUpdateDetailsUtf16.keyIndex(utf16TableName);
            if (tudKeyIndex < 0) {
                tud = tableUpdateDetailsUtf16.valueAt(tudKeyIndex);
            } else {
                int status = engine.getStatus(securityContext, path, utf16TableName, 0, utf16TableName.length());
                if (status != TableUtils.TABLE_EXISTS) {
                    if (!autoCreateNewTables) {
                        throw CairoException.nonCritical()
                                .put("table does not exist, creating new tables is disabled [table=").put(utf16TableName)
                                .put(']');
                    }
                    if (!autoCreateNewColumns) {
                        throw CairoException.nonCritical()
                                .put("table does not exist, cannot create table, creating new columns is disabled [table=").put(utf16TableName)
                                .put(']');
                    }
                    // validate that parser entities do not contain NULLs
                    TableStructureAdapter tsa = tableStructureAdapter.of(utf16TableName, parser);
                    for (int i = 0, n = tsa.getColumnCount(); i < n; i++) {
                        if (tsa.getColumnType(i) == LineTcpParser.ENTITY_TYPE_NULL) {
                            throw CairoException.nonCritical().put("unknown column type [columnName=").put(tsa.getColumnName(i)).put(']');
                        }
                    }
                    LOG.info().$("creating table [tableName=").$(utf16TableName).$(']').$();
                    engine.createTable(securityContext, ddlMem, path, tsa);
                }

                boolean isWalTable = TableSequencerAPI.isWalTable(utf16TableName, path, FilesFacadeImpl.INSTANCE);

                if (isWalTable) {
                    // WAL tables are not shared between threads
                    tud = new TableUpdateDetails(
                        configuration,
                        engine,
                        engine.getWalWriter(securityContext, utf16TableName),   
                        -1, // indicate that table is based on WAL
                        netIoJobs,
                        defaultColumnTypes
                    );
                    
                } else {
                    final int idleTudKeyIndex = idleTableUpdateDetailsUtf16.keyIndex(utf16TableName);
                    if (idleTudKeyIndex < 0) {
                        tud = idleTableUpdateDetailsUtf16.valueAt(idleTudKeyIndex);
                        LOG.info().$("idle table going active [tableName=").$(tud.getUtf16TableName()).I$();
                        if (tud.getWriter() == null) {
                            tud.closeNoLock();
                            // Use actual table name from the "details" to avoid case mismatches in the
                            // WriterPool. There was an error in the LineTcpReceiverFuzzTest, which helped
                            // to identify the cause
                            tud = unsafeAssignTableToWriterThread(tudKeyIndex, tud.getUtf16TableName());
                        } else {
                            idleTableUpdateDetailsUtf16.removeAt(idleTudKeyIndex);
                            tableUpdateDetailsUtf16.putAt(tudKeyIndex, tud.getUtf16TableName(), tud);
                        }
                    } else {
                        TelemetryTask.doStoreTelemetry(engine, Telemetry.SYSTEM_ILP_RESERVE_WRITER, Telemetry.ORIGIN_ILP_TCP);
                        tud = unsafeAssignTableToWriterThread(tudKeyIndex, utf16TableName);
                    }
                }
            }

            // here we need to create a string image (mangled) of utf8 char sequence
            // deliberately not decoding UTF8, store bytes as chars each
            sink.clear();
            sink.put(utf8TableName);

            // at this point this is not UTF16 string
            netIoJob.addTableUpdateDetails(sink.toString(), tud);
            return tud;
        } finally {
            tableUpdateDetailsLock.writeLock().unlock();
        }
    }

    private boolean isOpen() {
        return null != pubSeq;
    }

    @NotNull
    private TableUpdateDetails unsafeAssignTableToWriterThread(int tudKeyIndex, CharSequence utf16TableName) {
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
                // get writer here to avoid constructing
                // object instance and potentially leaking memory if
                // writer allocation fails
                engine.getTableWriterAPI(securityContext, utf16TableName, "tcpIlp"),
                threadId,
                netIoJobs,
                defaultColumnTypes
        );
        tableUpdateDetailsUtf16.putAt(tudKeyIndex, tud.getUtf16TableName(), tud);
        LOG.info().$("assigned ").$(utf16TableName).$(" to thread ").$(threadId).$();
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
                LOG.error().$("could not find statistic for table [name=").$(tableName).I$();
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

    boolean scheduleEvent(NetworkIOJob netIoJob, LineTcpParser parser) {
        TableUpdateDetails tud;
        try {
            tud = netIoJob.getLocalTableDetails(parser.getUtf8TableName());
            if (tud == null) {
                tud = createTableAndNewUpdateDetails(netIoJob, parser);
            }
        } catch (EntryUnavailableException ex) {
            // Table writer is locked
            LOG.info()
                .$("could not get table writer [tableName=").utf8(parser.getUtf8TableName())
                .$(", ex=`").$(ex.getFlyweightMessage())
                .I$();
            return true;
        } catch (CairoException ex) {
            // Table could not be created
            LOG.error().$("could not create table [tableName=").utf8(parser.getUtf8TableName())
                    .$(", errno=").$(ex.getErrno())
                    .I$();
            // More details will be logged by catching thread
            throw ex;
        }

        final int writerThreadId = tud.getWriterThreadId();
        if (writerThreadId > -1) {
            // non WAL table inserts are serialized to writer queue
            // and executed by "writer" thread
            long seq = getNextPublisherEventSequence(writerThreadId);
            if (seq > -1) {
                try {
                    if (tud.isWriterInError()) {
                        throw CairoException.critical(0).put("writer is in error, aborting ILP pipeline");
                    }
                    queue[writerThreadId].get(seq).createMeasurementEvent(
                        tud,
                        parser,
                        netIoJob.getWorkerId()
                    );
                } finally {
                    pubSeq[writerThreadId].done(seq);
                }
                tud.incrementEventsProcessedSinceReshuffle();
                return false;
            }
        } else {
            
        }
        return true;
    }

    @TestOnly
    void setListener(LineTcpReceiver.SchedulerListener listener) {
        this.listener = listener;
    }
}
