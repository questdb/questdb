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
    private final CairoEngine engine;
    private final CairoSecurityContext securityContext;
    private final RingQueue<LineTcpMeasurementEvent> queue;
    private final ReadWriteLock tableUpdateDetailsLock = new SimpleReadWriteLock();
    private final CharSequenceObjHashMap<TableUpdateDetails> tableUpdateDetailsUtf16;
    private final CharSequenceObjHashMap<TableUpdateDetails> idleTableUpdateDetailsUtf16;
    private final int[] loadByWriterThread;
    private final int processedEventCountBeforeReshuffle;
    private final double maxLoadRatio;
    private final long writerIdleTimeout;
    private final NetworkIOJob[] netIoJobs;
    private final StringSink[] tableNameSinks;
    private final TableStructureAdapter tableStructureAdapter;
    private final Path path = new Path();
    private final MemoryMARW ddlMem = Vm.getMARWInstance();
    private final LineTcpReceiverConfiguration configuration;
    private Sequence pubSeq;
    private int loadCheckCycles = 0;
    private int reshuffleCount = 0;
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
        tableUpdateDetailsUtf16 = new CharSequenceObjHashMap<>();
        idleTableUpdateDetailsUtf16 = new CharSequenceObjHashMap<>();
        loadByWriterThread = new int[writerWorkerPool.getWorkerCount()];
        int maxMeasurementSize = lineConfiguration.getMaxMeasurementSize();
        int queueSize = lineConfiguration.getWriterQueueCapacity();
        queue = new RingQueue<>(
                (address, addressSize) -> new LineTcpMeasurementEvent(
                        address,
                        addressSize,
                        lineConfiguration.getMicrosecondClock(),
                        lineConfiguration.getTimestampAdapter()
                ),
                getEventSlotSize(maxMeasurementSize),
                queueSize,
                MemoryTag.NATIVE_DEFAULT
        );

        pubSeq = new MPSequence(queueSize);

        int nWriterThreads = writerWorkerPool.getWorkerCount();
        if (nWriterThreads > 1) {
            FanOut fanOut = new FanOut();
            for (int i = 0; i < nWriterThreads; i++) {
                SCSequence subSeq = new SCSequence();
                fanOut.and(subSeq);
                final LineTcpWriterJob lineTcpWriterJob = new LineTcpWriterJob(
                        i,
                        queue,
                        subSeq,
                        milliClock,
                        configuration.getMaintenanceInterval(),
                        this
                );
                writerWorkerPool.assign(i, (Job) lineTcpWriterJob);
                writerWorkerPool.assign(i, (Closeable) lineTcpWriterJob);
            }
            pubSeq.then(fanOut).then(pubSeq);
        } else {
            SCSequence subSeq = new SCSequence();
            pubSeq.then(subSeq).then(pubSeq);
            final LineTcpWriterJob lineTcpWriterJob = new LineTcpWriterJob(
                    0,
                    queue,
                    subSeq,
                    milliClock,
                    configuration.getMaintenanceInterval(),
                    this
            );
            writerWorkerPool.assign(0, (Job) lineTcpWriterJob);
            writerWorkerPool.assign(0, (Closeable) lineTcpWriterJob);
        }

        this.tableStructureAdapter = new TableStructureAdapter(cairoConfiguration, configuration.getDefaultPartitionBy());
        processedEventCountBeforeReshuffle = lineConfiguration.getNUpdatesPerLoadRebalance();
        maxLoadRatio = lineConfiguration.getMaxLoadRatio();
        writerIdleTimeout = lineConfiguration.getWriterIdleTimeout();
    }

    @Override
    public void close() {
        // Both the writer and the network reader worker pools must have been closed so that their respective cleaners have run
        if (null != pubSeq) {
            pubSeq = null;
            tableUpdateDetailsLock.writeLock().lock();
            try {
                ObjList<CharSequence> tableNames = tableUpdateDetailsUtf16.keys();
                for (int n = 0, sz = tableNames.size(); n < sz; n++) {
                    tableUpdateDetailsUtf16.get(tableNames.get(n)).closeLocals();
                }
                tableUpdateDetailsUtf16.clear();

                tableNames = idleTableUpdateDetailsUtf16.keys();
                for (int n = 0, sz = tableNames.size(); n < sz; n++) {
                    TableUpdateDetails updateDetails = idleTableUpdateDetailsUtf16.get(tableNames.get(n));
                    updateDetails.closeLocals();
                }
                idleTableUpdateDetailsUtf16.clear();
            } finally {
                tableUpdateDetailsLock.writeLock().unlock();
            }
            Misc.free(path);
            Misc.free(ddlMem);
            Misc.free(queue);
        }
    }

    public boolean doMaintenance(
            CharSequenceObjHashMap<TableUpdateDetails> tableUpdateDetailsUtf8,
            int workerId,
            long millis
    ) {
        for (int n = 0, sz = tableUpdateDetailsUtf8.size(); n < sz; n++) {
            final CharSequence tableNameUtf8 = tableUpdateDetailsUtf8.keys().get(n);
            final TableUpdateDetails tableUpdateDetails = tableUpdateDetailsUtf8.get(tableNameUtf8);
            if (millis - tableUpdateDetails.getLastMeasurementMillis() >= writerIdleTimeout) {
                tableUpdateDetailsLock.writeLock().lock();
                try {
                    if (tableUpdateDetails.getNetworkIOOwnerCount() == 1) {
                        final long seq = getNextPublisherEventSequence();
                        if (seq > -1) {
                            LineTcpMeasurementEvent event = queue.get(seq);
                            event.createWriterReleaseEvent(tableUpdateDetails, true);
                            tableUpdateDetailsUtf8.remove(tableNameUtf8);
                            final CharSequence tableNameUtf16 = tableUpdateDetails.getTableNameUtf16();
                            tableUpdateDetailsUtf16.remove(tableNameUtf16);
                            idleTableUpdateDetailsUtf16.put(tableNameUtf16, tableUpdateDetails);
                            tableUpdateDetails.removeReference(workerId);
                            pubSeq.done(seq);
                            if (listener != null) {
                                // table going idle
                                listener.onEvent(tableNameUtf16, 1);
                            }
                            LOG.info().$("active table going idle [tableName=").$(tableNameUtf16).I$();
                        }
                        return true;
                    } else {
                        tableUpdateDetailsUtf8.remove(tableNameUtf8);
                        tableUpdateDetails.removeReference(workerId);
                    }
                    return sz > 1;
                } finally {
                    tableUpdateDetailsLock.writeLock().unlock();
                }
            }
        }
        return false;
    }

    public boolean processWriterReleaseEvent(LineTcpMeasurementEvent event, int workerId) {
        tableUpdateDetailsLock.readLock().lock();
        try {
            final TableUpdateDetails tab = event.getTableUpdateDetails();
            if (tab.getWriterThreadId() != workerId) {
                return true;
            }
            if (tableUpdateDetailsUtf16.keyIndex(tab.getTableNameUtf16()) < 0) {
                // Table must have been re-assigned to an IO thread
                return true;
            }
            LOG.info()
                    .$("releasing writer, its been idle since ").$ts(tab.getLastMeasurementMillis() * 1_000)
                    .$("[tableName=").$(tab.getTableNameUtf16())
                    .I$();

            event.releaseWriter();
        } finally {
            tableUpdateDetailsLock.readLock().unlock();
        }
        return true;
    }

    private static long getEventSlotSize(int maxMeasurementSize) {
        return Numbers.ceilPow2((long) (maxMeasurementSize / 4) * (Integer.BYTES + Double.BYTES + 1));
    }

    protected NetworkIOJob createNetworkIOJob(IODispatcher<LineTcpConnectionContext> dispatcher, int workerId) {
        return new LineTcpNetworkIOJob(configuration, this, dispatcher, workerId);
    }

    @TestOnly
    int[] getLoadByWriterThread() {
        return loadByWriterThread;
    }

    @TestOnly
    int getLoadCheckCycles() {
        return loadCheckCycles;
    }

    long getNextPublisherEventSequence() {
        assert isOpen();
        long seq;
        //noinspection StatementWithEmptyBody
        while ((seq = pubSeq.next()) == -2) {
        }
        return seq;
    }

    @TestOnly
    int getReshuffleCount() {
        return reshuffleCount;
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
                    LOG.info().$("creating table [tableName=").$(tableNameUtf16).$(']').$();
                    engine.createTable(securityContext, ddlMem, path, tableStructureAdapter.of(tableNameUtf16, parser));
                }

                final int idleTudKeyIndex = idleTableUpdateDetailsUtf16.keyIndex(tableNameUtf16);
                if (idleTudKeyIndex < 0) {
                    LOG.info().$("idle table going active [tableName=").$(tableNameUtf16).I$();
                    tab = idleTableUpdateDetailsUtf16.valueAt(idleTudKeyIndex);
                    if (tab.getWriter() == null) {
                        tab.closeNoLock();
                        tab = unsafeAssignTableToWriterThread(tudKeyIndex, tableNameUtf16);
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

    private void reshuffleTablesAcrossWriterThreads() {
        LOG.debug().$("load check [cycle=").$(++loadCheckCycles).$(']').$();
        unsafeCalcThreadLoad();
        final int tableCount = tableUpdateDetailsUtf16.size();
        int fromThreadId = -1;
        int toThreadId = -1;
        TableUpdateDetails tableToMove = null;
        int maxLoad = Integer.MAX_VALUE;
        while (true) {
            int highestLoad = Integer.MIN_VALUE;
            int highestLoadedThreadId = -1;
            int lowestLoad = Integer.MAX_VALUE;
            int lowestLoadedThreadId = -1;
            for (int i = 0, n = loadByWriterThread.length; i < n; i++) {
                if (loadByWriterThread[i] >= maxLoad) {
                    continue;
                }

                if (highestLoad < loadByWriterThread[i]) {
                    highestLoad = loadByWriterThread[i];
                    highestLoadedThreadId = i;
                }

                if (lowestLoad > loadByWriterThread[i]) {
                    lowestLoad = loadByWriterThread[i];
                    lowestLoadedThreadId = i;
                }
            }

            if (highestLoadedThreadId == -1 || lowestLoadedThreadId == -1 || highestLoadedThreadId == lowestLoadedThreadId) {
                break;
            }

            double loadRatio = (double) highestLoad / (double) lowestLoad;
            if (loadRatio < maxLoadRatio) {
                // Load is not sufficiently unbalanced
                break;
            }

            int nTables = 0;
            lowestLoad = Integer.MAX_VALUE;
            String leastLoadedTableName = null;
            for (int i = 0; i < tableCount; i++) {
                TableUpdateDetails tab = tableUpdateDetailsUtf16.valueQuick(i);
                if (tab.getWriterThreadId() == highestLoadedThreadId && tab.getEventsProcessedSinceReshuffle() > 0) {
                    nTables++;
                    if (tab.getEventsProcessedSinceReshuffle() < lowestLoad) {
                        lowestLoad = tab.getEventsProcessedSinceReshuffle();
                        leastLoadedTableName = tab.getTableNameUtf16();
                    }
                }
            }

            if (nTables < 2) {
                // The most loaded thread only has 1 table with load assigned to it
                maxLoad = highestLoad;
                continue;
            }

            fromThreadId = highestLoadedThreadId;
            toThreadId = lowestLoadedThreadId;
            tableToMove = tableUpdateDetailsUtf16.get(leastLoadedTableName);
            break;
        }

        for (int i = 0; i < tableCount; i++) {
            tableUpdateDetailsUtf16.valueQuick(i).setEventsProcessedSinceReshuffle(0);
        }

        if (null != tableToMove) {
            long seq = getNextPublisherEventSequence();
            if (seq >= 0) {
                try {
                    LineTcpMeasurementEvent event = queue.get(seq);
                    event.createReshuffleEvent(fromThreadId, toThreadId, tableToMove);
                    tableToMove.setWriterThreadId(toThreadId);
                    LOG.info()
                            .$("reshuffle cycle, requesting table move [cycle=").$(loadCheckCycles)
                            .$(", reshuffleCount=").$(++reshuffleCount)
                            .$(", table=").$(tableToMove.getTableNameUtf16())
                            .$(", fromThreadId=").$(fromThreadId)
                            .$(", toThreadId=").$(toThreadId)
                            .I$();
                } finally {
                    pubSeq.done(seq);
                }
            }
        }
    }

    @TestOnly
    void setListener(LineTcpReceiver.SchedulerListener listener) {
        this.listener = listener;
    }

    boolean scheduleEvent(NetworkIOJob netIoJob, LineTcpParser parser, FloatingDirectCharSink floatingDirectCharSink) {
        TableUpdateDetails tableUpdateDetails;
        try {
            tableUpdateDetails = netIoJob.getLocalTableDetails(parser.getMeasurementName());
            if (tableUpdateDetails == null) {
                tableUpdateDetails = getTableUpdateDetailsFromSharedArea(netIoJob, parser);
            }
        } catch (EntryUnavailableException ex) {
            // Table writer is locked
            LOG.info().$("could not get table writer [tableName=").$(parser.getMeasurementName()).$(", ex=").$(ex.getFlyweightMessage()).I$();
            return true;
        } catch (CairoException ex) {
            // Table could not be created
            LOG.info()
                    .$("could not create table [tableName=").$(parser.getMeasurementName())
                    .$(", ex=").$(ex.getFlyweightMessage())
                    .$(", errno=").$(ex.getErrno())
                    .I$();
            return false;
        }

        long seq = getNextPublisherEventSequence();
        if (seq > -1) {
            try {
                queue.get(seq).createMeasurementEvent(
                        tableUpdateDetails,
                        parser,
                        floatingDirectCharSink,
                        netIoJob.getWorkerId()
                );
            } finally {
                pubSeq.done(seq);
            }
            if (tableUpdateDetails.incrementEventsProcessedSinceReshuffle() > processedEventCountBeforeReshuffle) {
                if (tableUpdateDetailsLock.writeLock().tryLock()) {
                    try {
                        reshuffleTablesAcrossWriterThreads();
                    } finally {
                        tableUpdateDetailsLock.writeLock().unlock();
                    }
                }
            }
            return false;
        }
        return true;
    }

    @NotNull
    private TableUpdateDetails unsafeAssignTableToWriterThread(int tudKeyIndex, CharSequence tableNameUtf16) {
        unsafeCalcThreadLoad();
        int leastLoad = Integer.MAX_VALUE;
        int threadId = 0;

        for (int n = 0; n < loadByWriterThread.length; n++) {
            if (loadByWriterThread[n] < leastLoad) {
                leastLoad = loadByWriterThread[n];
                threadId = n;
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
                netIoJobs
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
                LOG.error().$("could not find static for table [name=").$(tableName).I$();
            }
        }
    }
}
