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
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.*;
import io.questdb.network.IODispatcher;
import io.questdb.std.*;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.std.str.FloatingDirectCharSink;
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
    private final DefaultColumnTypes defaultColumnTypes;
    private final CairoEngine engine;
    private final CairoSecurityContext securityContext;
    private final RingQueue<LineTcpMeasurementEvent>[] queue;
    private final ReadWriteLock tableUpdateDetailsLock = new SimpleReadWriteLock();
    private final LowerCaseCharSequenceObjHashMap<TableUpdateDetails> tableUpdateDetailsUtf16;
    private final LowerCaseCharSequenceObjHashMap<TableUpdateDetails> idleTableUpdateDetailsUtf16;
    private final long[] loadByWriterThread;
    private final long writerIdleTimeout;
    private final NetworkIOJob[] netIoJobs;
    private final StringSink[] tableNameSinks;
    private final TableStructureAdapter tableStructureAdapter;
    private final Path path = new Path();
    private final MemoryMARW ddlMem = Vm.getMARWInstance();
    private final LineTcpReceiverConfiguration configuration;
    private final MPSequence[] pubSeq;
    private final boolean autoCreateNewTables;
    private final boolean autoCreateNewColumns;
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
            ioWorkerPool.assign(i, netIoJob::close);
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
                            lineConfiguration.getAutoCreateNewColumns()
                    ),
                    getEventSlotSize(maxMeasurementSize),
                    queueSize,
                    MemoryTag.NATIVE_DEFAULT
            );

            queue[i] = q;
            SCSequence subSeq = new SCSequence();
            ps.then(subSeq).then(ps);

            final LineTcpWriterJob lineTcpWriterJob = new LineTcpWriterJob(
                    i,
                    q,
                    subSeq,
                    milliClock,
                    commitIntervalDefault,
                    this,
                    engine.getMetrics()
            );
            writerWorkerPool.assign(i, (Job) lineTcpWriterJob);
            writerWorkerPool.assign(i, (Closeable) lineTcpWriterJob);
        }
        this.tableStructureAdapter = new TableStructureAdapter(cairoConfiguration, defaultColumnTypes, configuration.getDefaultPartitionBy());
        writerIdleTimeout = lineConfiguration.getWriterIdleTimeout();
    }

    @Override
    public void close() {
        tableUpdateDetailsLock.writeLock().lock();
        try {
            closeLocals(
            tableUpdateDetailsUtf16);
                closeLocals(
            idleTableUpdateDetailsUtf16);
        } finally {
            tableUpdateDetailsLock.writeLock().unlock();
        }
        Misc.free(path);
        Misc.free(ddlMem);
        for (int i = 0, n = queue.length; i < n; i++) {
            Misc.free(queue[i]);
        }
    }

    public boolean doMaintenance(
            CharSequenceObjHashMap<TableUpdateDetails> tableUpdateDetailsUtf8,
            int readerWorkerId,
            long millis
    ) {
        for (int n = 0, sz = tableUpdateDetailsUtf8.size(); n < sz; n++) {
            final CharSequence tableNameUtf8 = tableUpdateDetailsUtf8.keys().get(n);
            final TableUpdateDetails tab = tableUpdateDetailsUtf8.get(tableNameUtf8);
            if (millis - tab.getLastMeasurementMillis() >= writerIdleTimeout) {
                tableUpdateDetailsLock.writeLock().lock();
                try {
                    if (tab.getNetworkIOOwnerCount() == 1) {
                        final int writerWorkerId = tab.getWriterThreadId();
                        final long seq = getNextPublisherEventSequence(writerWorkerId);
                        if (seq > -1) {
                            LineTcpMeasurementEvent event = queue[writerWorkerId].get(seq);
                            event.createWriterReleaseEvent(tab, true);
                            tableUpdateDetailsUtf8.remove(tableNameUtf8);
                            final CharSequence tableNameUtf16 = tab.getTableNameUtf16();
                            tableUpdateDetailsUtf16.remove(tableNameUtf16);
                            idleTableUpdateDetailsUtf16.put(tableNameUtf16, tab);
                            tab.removeReference(readerWorkerId);
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
                        tab.removeReference(readerWorkerId);
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
            if (!event.getTableUpdateDetails().isWriterInError() && tableUpdateDetailsUtf16.keyIndex(tab.getTableNameUtf16()) < 0) {
                // Table must have been re-assigned to an IO thread
                return;
            }
            LOG.info()
                    .$("releasing writer, its been idle since ").$ts(tab.getLastMeasurementMillis() * 1_000)
                    .$("[tableName=").$(tab.getTableNameUtf16())
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

    protected NetworkIOJob createNetworkIOJob(IODispatcher<LineTcpConnectionContext> dispatcher, int workerId) {
        return new LineTcpNetworkIOJob(configuration, this, dispatcher, workerId);
    }

    long getNextPublisherEventSequence(int writerWorkerId) {
        assert isOpen();
        long seq;
        //noinspection StatementWithEmptyBody
        while ((seq = pubSeq[writerWorkerId].next()) == -2) {
        }
        return seq;
    }

    private TableUpdateDetails getTableUpdateDetailsFromSharedArea(@NotNull NetworkIOJob netIoJob, @NotNull LineTcpParser parser) {
        final DirectByteCharSequence tableNameUtf8 = parser.getMeasurementName();
        final StringSink tableNameUtf16 = tableNameSinks[netIoJob.getWorkerId()];
        tableNameUtf16.clear();
        Chars.utf8Decode(tableNameUtf8.getLo(), tableNameUtf8.getHi(), tableNameUtf16);

        tableUpdateDetailsLock.writeLock().lock();
        try {
            TableUpdateDetails tab;
            final int tudKeyIndex = tableUpdateDetailsUtf16.keyIndex(tableNameUtf16);
            if (tudKeyIndex < 0) {
                tab = tableUpdateDetailsUtf16.valueAt(tudKeyIndex);
            } else {
                int status = engine.getStatus(securityContext, path, tableNameUtf16, 0, tableNameUtf16.length());
                if (status != TableUtils.TABLE_EXISTS) {
                    if (!autoCreateNewTables) {
                        throw CairoException.instance(0)
                                .put("table does not exist, creating new tables is disabled [table=").put(tableNameUtf16)
                                .put(']');
                    }
                    if (!autoCreateNewColumns) {
                        throw CairoException.instance(0)
                                .put("table does not exist, cannot create table, creating new columns is disabled [table=").put(tableNameUtf16)
                                .put(']');
                    }
                    // validate that parser entities do not contain NULLs
                    TableStructureAdapter tsa = tableStructureAdapter.of(tableNameUtf16, parser);
                    for (int i = 0, n = tsa.getColumnCount(); i < n; i++) {
                        if (tsa.getColumnType(i) == LineTcpParser.ENTITY_TYPE_NULL) {
                            throw CairoException.instance(0).put("unknown column type [columnName=").put(tsa.getColumnName(i)).put(']');
                        }
                    }
                    LOG.info().$("creating table [tableName=").$(tableNameUtf16).$(']').$();
                    engine.createTable(securityContext, ddlMem, path, tsa);
                }

                final int idleTudKeyIndex = idleTableUpdateDetailsUtf16.keyIndex(tableNameUtf16);
                if (idleTudKeyIndex < 0) {
                    tab = idleTableUpdateDetailsUtf16.valueAt(idleTudKeyIndex);
                    LOG.info().$("idle table going active [tableName=").$(tab.getTableNameUtf16()).I$();
                    if (tab.getWriter() == null) {
                        tab.closeNoLock();
                        // Use actual table name from the "details" to avoid case mismatches in the
                        // WriterPool. There was an error in the LineTcpReceiverFuzzTest, which helped
                        // to identify the cause
                        tab = unsafeAssignTableToWriterThread(tudKeyIndex, tab.getTableNameUtf16());
                    } else {
                        idleTableUpdateDetailsUtf16.removeAt(idleTudKeyIndex);
                        tableUpdateDetailsUtf16.putAt(tudKeyIndex, tab.getTableNameUtf16(), tab);
                    }
                } else {
                    TelemetryTask.doStoreTelemetry(engine, Telemetry.SYSTEM_ILP_RESERVE_WRITER, Telemetry.ORIGIN_ILP_TCP);
                    tab = unsafeAssignTableToWriterThread(tudKeyIndex, tableNameUtf16);
                }
            }

            // here we need to create a string image (mangled) of utf8 char sequence
            // deliberately not decoding UTF8, store bytes as chars each
            tableNameUtf16.clear();
            tableNameUtf16.put(tableNameUtf8);

            // at this point this is not UTF16 string
            netIoJob.addTableUpdateDetails(tableNameUtf16.toString(), tab);
            return tab;
        } finally {
            tableUpdateDetailsLock.writeLock().unlock();
        }
    }

    private boolean isOpen() {
        return null != pubSeq;
    }

    boolean scheduleEvent(NetworkIOJob netIoJob, LineTcpParser parser, FloatingDirectCharSink floatingDirectCharSink) {
        TableUpdateDetails tab;
        try {
            tab = netIoJob.getLocalTableDetails(parser.getMeasurementName());
            if (tab == null) {
                tab = getTableUpdateDetailsFromSharedArea(netIoJob, parser);
            }
        } catch (EntryUnavailableException ex) {
            // Table writer is locked
            LOG.info().$("could not get table writer [tableName=").$(parser.getMeasurementName()).$(", ex=`").$(ex.getFlyweightMessage()).$("`]").$();
            return true;
        } catch (CairoException ex) {
            // Table could not be created
            LOG.error().$("could not create table [tableName=").$(parser.getMeasurementName())
                    .$(", errno=").$(ex.getErrno())
                    .I$();
            // More details will be logged by catching thread
            throw ex;
        }

        final int writerThreadId = tab.getWriterThreadId();
        long seq = getNextPublisherEventSequence(writerThreadId);
        if (seq > -1) {
            try {
                if (tab.isWriterInError()) {
                    throw CairoException.instance(0).put("writer is in error, aborting ILP pipeline");
                }
                queue[writerThreadId].get(seq).createMeasurementEvent(
                        tab,
                        parser,
                        netIoJob.getWorkerId()
                );
            } finally {
                pubSeq[writerThreadId].done(seq);
            }
            tab.incrementEventsProcessedSinceReshuffle();
            return false;
        }
        return true;
    }

    @TestOnly
    void setListener(LineTcpReceiver.SchedulerListener listener) {
        this.listener = listener;
    }

    @NotNull
    private TableUpdateDetails unsafeAssignTableToWriterThread(int tudKeyIndex, CharSequence tableNameUtf16) {
        unsafeCalcThreadLoad();
        long leastLoad = Long.MAX_VALUE;
        int threadId = 0;

        for (int i = 0, n = loadByWriterThread.length; i < n; i++) {
            if (loadByWriterThread[i] < leastLoad) {
                leastLoad = loadByWriterThread[i];
                threadId = i;
            }
        }

        final TableUpdateDetails tableUpdateDetails = new TableUpdateDetails(
                configuration,
                engine,
                // get writer here to avoid constructing
                // object instance and potentially leaking memory if
                // writer allocation fails
                engine.getWriter(securityContext, tableNameUtf16, "tcpIlp"),
                threadId,
                netIoJobs,
                defaultColumnTypes
        );
        tableUpdateDetailsUtf16.putAt(tudKeyIndex, tableUpdateDetails.getTableNameUtf16(), tableUpdateDetails);
        LOG.info().$("assigned ").$(tableNameUtf16).$(" to thread ").$(threadId).$();
        return tableUpdateDetails;
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
}
