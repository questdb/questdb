/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.cairo.TableWriter.Row;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.AppendOnlyVirtualMemory;
import io.questdb.cutlass.line.LineProtoTimestampAdapter;
import io.questdb.cutlass.line.tcp.NewLineProtoParser.ProtoEntity;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.*;
import io.questdb.network.IODispatcher;
import io.questdb.network.IOOperation;
import io.questdb.network.IORequestProcessor;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.DirectCharSink;
import io.questdb.std.str.FloatingDirectCharSink;
import io.questdb.std.str.Path;
import io.questdb.tasks.TelemetryTask;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.util.Arrays;
import java.util.concurrent.locks.ReadWriteLock;

import static io.questdb.network.IODispatcher.DISCONNECT_REASON_UNKNOWN_OPERATION;

class LineTcpMeasurementScheduler implements Closeable {
    private static final Log LOG = LogFactory.getLog(LineTcpMeasurementScheduler.class);
    private static final int REBALANCE_EVENT_ID = -1; // A rebalance event is used to rebalance load across different threads
    private static final int INCOMPLETE_EVENT_ID = -2; // An incomplete event is used when the queue producer has grabbed an event but is
    // not able to populate it for some reason, the event needs to be committed to the
    // queue incomplete
    private static final int RELEASE_WRITER_EVENT_ID = -3;
    private static final int[] DEFAULT_COLUMN_TYPES = new int[NewLineProtoParser.N_ENTITY_TYPES];
    private final CairoEngine engine;
    private final CairoSecurityContext securityContext;
    private final CairoConfiguration cairoConfiguration;
    private final MillisecondClock milliClock;
    private final RingQueue<LineTcpMeasurementEvent> queue;
    private final ReadWriteLock tableUpdateDetailsLock = new SimpleReadWriteLock();
    private final CharSequenceObjHashMap<TableUpdateDetails> tableUpdateDetailsByTableName;
    private final CharSequenceObjHashMap<TableUpdateDetails> idleTableUpdateDetailsByTableName;
    private final int[] loadByThread;
    private final int nUpdatesPerLoadRebalance;
    private final double maxLoadRatio;
    private final long maintenanceInterval;
    private final long writerIdleTimeout;
    private final int defaultPartitionBy;
    private final NetworkIOJob[] netIoJobs;
    private final TableStructureAdapter tableStructureAdapter = new TableStructureAdapter();
    private final Path path = new Path();
    private final AppendOnlyVirtualMemory mem = new AppendOnlyVirtualMemory();
    private Sequence pubSeq;
    private int nLoadCheckCycles = 0;
    private int nRebalances = 0;

    LineTcpMeasurementScheduler(
            LineTcpReceiverConfiguration lineConfiguration,
            CairoEngine engine,
            WorkerPool ioWorkerPool,
            IODispatcher<LineTcpConnectionContext> dispatcher,
            WorkerPool writerWorkerPool
    ) {
        this.engine = engine;
        this.securityContext = lineConfiguration.getCairoSecurityContext();
        this.cairoConfiguration = engine.getConfiguration();
        this.milliClock = cairoConfiguration.getMillisecondClock();

        this.netIoJobs = new NetworkIOJob[ioWorkerPool.getWorkerCount()];
        for (int i = 0; i < ioWorkerPool.getWorkerCount(); i++) {
            NetworkIOJob netIoJob = createNetworkIOJob(dispatcher, i);
            netIoJobs[i] = netIoJob;
            ioWorkerPool.assign(i, netIoJob);
            ioWorkerPool.assign(i, netIoJob::close);
        }

        // Worker count is set to 1 because we do not use this execution context
        // in worker threads.
        tableUpdateDetailsByTableName = new CharSequenceObjHashMap<>();
        idleTableUpdateDetailsByTableName = new CharSequenceObjHashMap<>();
        loadByThread = new int[writerWorkerPool.getWorkerCount()];
        int maxMeasurementSize = lineConfiguration.getMaxMeasurementSize();
        int queueSize = lineConfiguration.getWriterQueueCapacity();
        queue = new RingQueue<>(
                () -> new LineTcpMeasurementEvent(
                        maxMeasurementSize,
                        lineConfiguration.getMicrosecondClock(),
                        lineConfiguration.getTimestampAdapter()),
                queueSize);
        pubSeq = new MPSequence(queueSize);

        int nWriterThreads = writerWorkerPool.getWorkerCount();
        if (nWriterThreads > 1) {
            FanOut fanOut = new FanOut();
            for (int n = 0; n < nWriterThreads; n++) {
                SCSequence subSeq = new SCSequence();
                fanOut.and(subSeq);
                WriterJob writerJob = new WriterJob(n, subSeq);
                writerWorkerPool.assign(n, writerJob);
                writerWorkerPool.assign(n, writerJob::close);
            }
            pubSeq.then(fanOut).then(pubSeq);
        } else {
            SCSequence subSeq = new SCSequence();
            pubSeq.then(subSeq).then(pubSeq);
            WriterJob writerJob = new WriterJob(0, subSeq);
            writerWorkerPool.assign(0, writerJob);
            writerWorkerPool.assign(0, writerJob::close);
        }

        nUpdatesPerLoadRebalance = lineConfiguration.getNUpdatesPerLoadRebalance();
        maxLoadRatio = lineConfiguration.getMaxLoadRatio();
        maintenanceInterval = lineConfiguration.getMaintenanceInterval();
        defaultPartitionBy = lineConfiguration.getDefaultPartitionBy();
        writerIdleTimeout = lineConfiguration.getWriterIdleTimeout();
    }

    @Override
    public void close() {
        // Both the writer and the net worker pools must have been closed so that their respective cleaners have run
        if (null != pubSeq) {
            pubSeq = null;
            tableUpdateDetailsLock.writeLock().lock();
            try {
                ObjList<CharSequence> tableNames = tableUpdateDetailsByTableName.keys();
                for (int n = 0, sz = tableNames.size(); n < sz; n++) {
                    TableUpdateDetails updateDetails = tableUpdateDetailsByTableName.get(tableNames.get(n));
                    updateDetails.closeLocals();
                }
                tableUpdateDetailsByTableName.clear();

                tableNames = idleTableUpdateDetailsByTableName.keys();
                for (int n = 0, sz = tableNames.size(); n < sz; n++) {
                    TableUpdateDetails updateDetails = idleTableUpdateDetailsByTableName.get(tableNames.get(n));
                    updateDetails.closeLocals();
                }
                idleTableUpdateDetailsByTableName.clear();
            } finally {
                tableUpdateDetailsLock.writeLock().unlock();
            }
            for (int n = 0; n < queue.getCapacity(); n++) {
                queue.get(n).close();
            }
            path.close();
            mem.close();
        }
    }

    @NotNull
    private TableUpdateDetails assignTableToThread(String tableName) {
        TableUpdateDetails tableUpdateDetails;
        calcThreadLoad();
        int leastLoad = Integer.MAX_VALUE;
        int threadId = 0;
        for (int n = 0; n < loadByThread.length; n++) {
            if (loadByThread[n] < leastLoad) {
                leastLoad = loadByThread[n];
                threadId = n;
            }
        }
        tableUpdateDetails = new TableUpdateDetails(tableName, threadId, netIoJobs);

        int keyIndex = tableUpdateDetailsByTableName.keyIndex(tableName);
        tableUpdateDetailsByTableName.putAt(keyIndex, tableName, tableUpdateDetails);
        LOG.info().$("assigned ").$(tableName).$(" to thread ").$(threadId).$();
        return tableUpdateDetails;
    }

    private void calcThreadLoad() {
        Arrays.fill(loadByThread, 0);
        ObjList<CharSequence> tableNames = tableUpdateDetailsByTableName.keys();
        for (int n = 0, sz = tableNames.size(); n < sz; n++) {
            final CharSequence tableName = tableNames.getQuick(n);
            final TableUpdateDetails stats = tableUpdateDetailsByTableName.get(tableName);
            if (stats != null) {
                loadByThread[stats.writerThreadId] += stats.nUpdates;
            } else {
                LOG.error().$("could not find static for table [name=").$(tableName).I$();
            }
        }
    }

    protected NetworkIOJob createNetworkIOJob(IODispatcher<LineTcpConnectionContext> dispatcher, int workerId) {
        return new NetworkIOJobImpl(dispatcher, workerId);
    }

    int[] getLoadByThread() {
        return loadByThread;
    }

    int getNLoadCheckCycles() {
        return nLoadCheckCycles;
    }

    int getNRebalances() {
        return nRebalances;
    }

    long getNextPublisherEventSequence() {
        assert isOpen();
        long seq;
        //noinspection StatementWithEmptyBody
        while ((seq = pubSeq.next()) == -2) {
        }
        return seq;
    }

    private boolean isOpen() {
        return null != pubSeq;
    }

    private void loadRebalance() {
        LOG.debug().$("load check [cycle=").$(++nLoadCheckCycles).$(']').$();
        calcThreadLoad();
        ObjList<CharSequence> tableNames = tableUpdateDetailsByTableName.keys();
        int fromThreadId = -1;
        int toThreadId = -1;
        TableUpdateDetails tableToMove = null;
        int maxLoad = Integer.MAX_VALUE;
        while (true) {
            int highestLoad = Integer.MIN_VALUE;
            int highestLoadedThreadId = -1;
            int lowestLoad = Integer.MAX_VALUE;
            int lowestLoadedThreadId = -1;
            for (int n = 0; n < loadByThread.length; n++) {
                if (loadByThread[n] >= maxLoad) {
                    continue;
                }

                if (highestLoad < loadByThread[n]) {
                    highestLoad = loadByThread[n];
                    highestLoadedThreadId = n;
                }

                if (lowestLoad > loadByThread[n]) {
                    lowestLoad = loadByThread[n];
                    lowestLoadedThreadId = n;
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
            for (int n = 0, sz = tableNames.size(); n < sz; n++) {
                TableUpdateDetails stats = tableUpdateDetailsByTableName.get(tableNames.get(n));
                if (stats.writerThreadId == highestLoadedThreadId && stats.nUpdates > 0) {
                    nTables++;
                    if (stats.nUpdates < lowestLoad) {
                        lowestLoad = stats.nUpdates;
                        leastLoadedTableName = stats.tableName;
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
            tableToMove = tableUpdateDetailsByTableName.get(leastLoadedTableName);
            break;
        }

        for (int n = 0, sz = tableNames.size(); n < sz; n++) {
            TableUpdateDetails stats = tableUpdateDetailsByTableName.get(tableNames.get(n));
            stats.nUpdates = 0;
        }

        if (null != tableToMove) {
            long seq = getNextPublisherEventSequence();
            if (seq >= 0) {
                try {
                    LineTcpMeasurementEvent event = queue.get(seq);
                    event.threadId = INCOMPLETE_EVENT_ID;
                    event.createRebalanceEvent(fromThreadId, toThreadId, tableToMove);
                    tableToMove.writerThreadId = toThreadId;
                    LOG.info()
                            .$("rebalance cycle, requesting table move [cycle=").$(nLoadCheckCycles)
                            .$(", nRebalances=").$(++nRebalances)
                            .$(", table=").$(tableToMove.tableName)
                            .$(", fromThreadId=").$(fromThreadId)
                            .$(", toThreadId=").$(toThreadId)
                            .$(']').$();
                } finally {
                    pubSeq.done(seq);
                }
            }
        }
    }

    private TableUpdateDetails startNewMeasurementEvent(NetworkIOJob netIoJob, NewLineProtoParser protoParser) {
        final TableUpdateDetails tableUpdateDetails = netIoJob.getTableUpdateDetails(protoParser.getMeasurementName());
        if (null != tableUpdateDetails) {
            return tableUpdateDetails;
        }
        return startNewMeasurementEvent0(netIoJob, protoParser);
    }

    private TableUpdateDetails startNewMeasurementEvent0(NetworkIOJob netIoJob, NewLineProtoParser protoParser) {
        TableUpdateDetails tableUpdateDetails;
        tableUpdateDetailsLock.writeLock().lock();
        try {
            int keyIndex = tableUpdateDetailsByTableName.keyIndex(protoParser.getMeasurementName());
            if (keyIndex < 0) {
                tableUpdateDetails = tableUpdateDetailsByTableName.valueAt(keyIndex);
            } else {
                String tableName = protoParser.getMeasurementName().toString();
                int status = engine.getStatus(securityContext, path, tableName, 0, tableName.length());
                if (status != TableUtils.TABLE_EXISTS) {
                    LOG.info().$("creating table [tableName=").$(tableName).$(']').$();
                    engine.createTable(securityContext, mem, path, tableStructureAdapter.of(tableName, protoParser));
                }

                keyIndex = idleTableUpdateDetailsByTableName.keyIndex(tableName);
                if (keyIndex < 0) {
                    LOG.info().$("idle table going active [tableName=").$(tableName).I$();
                    tableUpdateDetails = idleTableUpdateDetailsByTableName.valueAt(keyIndex);
                    idleTableUpdateDetailsByTableName.removeAt(keyIndex);
                    tableUpdateDetailsByTableName.put(tableUpdateDetails.tableName, tableUpdateDetails);
                } else {
                    TelemetryTask.doStoreTelemetry(engine, Telemetry.SYSTEM_ILP_RESERVE_WRITER, Telemetry.ORIGIN_ILP_TCP);
                    tableUpdateDetails = assignTableToThread(tableName);
                }
            }

            netIoJob.addTableUpdateDetails(tableUpdateDetails);
            return tableUpdateDetails;
        } finally {
            tableUpdateDetailsLock.writeLock().unlock();
        }
    }

    boolean tryButCouldNotCommit(NetworkIOJob netIoJob, NewLineProtoParser protoParser, FloatingDirectCharSink charSink) {
        TableUpdateDetails tableUpdateDetails;
        try {
            tableUpdateDetails = startNewMeasurementEvent(netIoJob, protoParser);
        } catch (EntryUnavailableException ex) {
            // Table writer is locked
            LOG.info().$("could not get table writer [tableName=").$(protoParser.getMeasurementName()).$(", ex=").$(ex.getFlyweightMessage()).$(']').$();
            return true;
        } catch (CairoException ex) {
            // Table could not be created
            LOG.info().$("could not create table [tableName=").$(protoParser.getMeasurementName()).$(", ex=").$(ex.getFlyweightMessage()).$(']').$();
            return false;
        }
        if (null != tableUpdateDetails) {
            long seq = getNextPublisherEventSequence();
            if (seq >= 0) {
                try {
                    LineTcpMeasurementEvent event = queue.get(seq);
                    event.threadId = INCOMPLETE_EVENT_ID;
                    TableUpdateDetails.ThreadLocalDetails localDetails = tableUpdateDetails.startNewMeasurementEvent(netIoJob.getWorkerId());
                    event.createMeasurementEvent(tableUpdateDetails, localDetails, protoParser, charSink);
                    return false;
                } finally {
                    pubSeq.done(seq);
                    if (++tableUpdateDetails.nUpdates > nUpdatesPerLoadRebalance) {
                        if (tableUpdateDetailsLock.writeLock().tryLock()) {
                            try {
                                loadRebalance();
                            } finally {
                                tableUpdateDetailsLock.writeLock().unlock();
                            }
                        }
                    }
                }
            }
        }
        return true;
    }

    interface NetworkIOJob extends Job {
        void addTableUpdateDetails(TableUpdateDetails tableUpdateDetails);

        void close();

        TableUpdateDetails getTableUpdateDetails(CharSequence tableName);

        ObjList<SymbolCache> getUnusedSymbolCaches();

        int getWorkerId();
    }

    private class LineTcpMeasurementEvent implements Closeable {
        private final MicrosecondClock clock;
        private final LineProtoTimestampAdapter timestampAdapter;
        private final long bufSize;
        private int threadId;
        private TableUpdateDetails tableUpdateDetails;
        private long bufLo;
        private int rebalanceFromThreadId;
        private int rebalanceToThreadId;
        private volatile boolean rebalanceReleasedByFromThread;

        private LineTcpMeasurementEvent(int maxMeasurementSize, MicrosecondClock clock, LineProtoTimestampAdapter timestampAdapter) {
            bufSize = (long) (maxMeasurementSize / 4) * (Integer.BYTES + Double.BYTES + 1);
            bufLo = Unsafe.malloc(bufSize);
            this.clock = clock;
            this.timestampAdapter = timestampAdapter;
        }

        @Override
        public void close() {
            Unsafe.free(bufLo, bufSize);
            tableUpdateDetails = Misc.free(tableUpdateDetails);
            bufLo = 0;
        }

        void createMeasurementEvent(
                TableUpdateDetails tableUpdateDetails,
                TableUpdateDetails.ThreadLocalDetails localDetails,
                NewLineProtoParser protoParser,
                FloatingDirectCharSink floatingCharSink
        ) {
            threadId = INCOMPLETE_EVENT_ID;
            this.tableUpdateDetails = tableUpdateDetails;
            long timestamp = protoParser.getTimestamp();
            if (timestamp != NewLineProtoParser.NULL_TIMESTAMP) {
                timestamp = timestampAdapter.getMicros(timestamp);
            }
            long bufPos = bufLo;
            Unsafe.getUnsafe().putLong(bufPos, timestamp);
            bufPos += Long.BYTES;
            int nEntities = protoParser.getnEntities();
            Unsafe.getUnsafe().putInt(bufPos, nEntities);
            bufPos += Integer.BYTES;
            for (int nEntity = 0; nEntity < nEntities; nEntity++) {
                assert bufPos < (bufLo + bufSize + 6);
                ProtoEntity entity = protoParser.getEntity(nEntity);
                int colIndex = localDetails.getColumnIndex(entity.getName());
                if (colIndex < 0) {
                    int colNameLen = entity.getName().length();
                    Unsafe.getUnsafe().putInt(bufPos, -1 * colNameLen);
                    bufPos += Integer.BYTES;
                    Vect.memcpy(entity.getName().getLo(), bufPos, colNameLen);
                    bufPos += colNameLen;
                } else {
                    Unsafe.getUnsafe().putInt(bufPos, colIndex);
                    bufPos += Integer.BYTES;
                }
                byte entityType = entity.getType();
                switch (entityType) {
                    case NewLineProtoParser.ENTITY_TYPE_TAG: {
                        long tmpBufPos = bufPos;
                        int l = entity.getValue().length();
                        bufPos += Integer.BYTES + Byte.BYTES;
                        long hi = bufPos + 2L * l;
                        floatingCharSink.of(bufPos, hi);
                        if (!Chars.utf8Decode(entity.getValue().getLo(), entity.getValue().getHi(), floatingCharSink)) {
                            throw CairoException.instance(0).put("invalid UTF8 in value for ").put(entity.getName());
                        }

                        int symIndex = tableUpdateDetails.getSymbolIndex(localDetails, colIndex, floatingCharSink);
                        if (symIndex != SymbolTable.VALUE_NOT_FOUND) {
                            bufPos = tmpBufPos;
                            Unsafe.getUnsafe().putByte(bufPos, NewLineProtoParser.ENTITY_TYPE_CACHED_TAG);
                            bufPos += Byte.BYTES;
                            Unsafe.getUnsafe().putInt(bufPos, symIndex);
                            bufPos += Integer.BYTES;
                        } else {
                            Unsafe.getUnsafe().putByte(tmpBufPos, entity.getType());
                            tmpBufPos += Byte.BYTES;
                            Unsafe.getUnsafe().putInt(tmpBufPos, l);
                            bufPos = hi;
                        }
                        break;
                    }
                    case NewLineProtoParser.ENTITY_TYPE_INTEGER: {
                        Unsafe.getUnsafe().putByte(bufPos, entity.getType());
                        bufPos += Byte.BYTES;
                        Unsafe.getUnsafe().putLong(bufPos, entity.getIntegerValue());
                        bufPos += Long.BYTES;
                        break;
                    }
                    case NewLineProtoParser.ENTITY_TYPE_FLOAT: {
                        Unsafe.getUnsafe().putByte(bufPos, entity.getType());
                        bufPos += Byte.BYTES;
                        Unsafe.getUnsafe().putDouble(bufPos, entity.getFloatValue());
                        bufPos += Double.BYTES;
                        break;
                    }
                    case NewLineProtoParser.ENTITY_TYPE_STRING:
                    case NewLineProtoParser.ENTITY_TYPE_LONG256: {
                        Unsafe.getUnsafe().putByte(bufPos, entity.getType());
                        bufPos += Byte.BYTES;
                        int l = entity.getValue().length();
                        Unsafe.getUnsafe().putInt(bufPos, l);
                        bufPos += Integer.BYTES;
                        long hi = bufPos + 2L * l;
                        floatingCharSink.of(bufPos, hi);
                        if (!Chars.utf8Decode(entity.getValue().getLo(), entity.getValue().getHi(), floatingCharSink)) {
                            throw CairoException.instance(0).put("invalid UTF8 in value for ").put(entity.getName());
                        }
                        bufPos = hi;
                        break;
                    }
                    case NewLineProtoParser.ENTITY_TYPE_BOOLEAN: {
                        Unsafe.getUnsafe().putByte(bufPos, entity.getType());
                        bufPos += Byte.BYTES;
                        Unsafe.getUnsafe().putByte(bufPos, (byte) (entity.getBooleanValue() ? 1 : 0));
                        bufPos += Byte.BYTES;
                        break;
                    }
                }
            }
            threadId = tableUpdateDetails.writerThreadId;
        }

        void createRebalanceEvent(int fromThreadId, int toThreadId, TableUpdateDetails tableUpdateDetails) {
            threadId = REBALANCE_EVENT_ID;
            rebalanceFromThreadId = fromThreadId;
            rebalanceToThreadId = toThreadId;
            this.tableUpdateDetails = tableUpdateDetails;
            rebalanceReleasedByFromThread = false;
        }

        void createReleaseWriterEvent(TableUpdateDetails tableUpdateDetails) {
            threadId = RELEASE_WRITER_EVENT_ID;
            this.tableUpdateDetails = tableUpdateDetails;
        }

        void processMeasurementEvent(WriterJob job) {
            Row row = null;
            try {
                TableWriter writer = tableUpdateDetails.getWriter();
                long bufPos = bufLo;
                long timestamp = Unsafe.getUnsafe().getLong(bufPos);
                bufPos += Long.BYTES;
                if (timestamp == NewLineProtoParser.NULL_TIMESTAMP) {
                    timestamp = clock.getTicks();
                }
                row = writer.newRow(timestamp);
                int nEntities = Unsafe.getUnsafe().getInt(bufPos);
                bufPos += Integer.BYTES;
                long firstEntityBufPos = bufPos;
                for (int nEntity = 0; nEntity < nEntities; nEntity++) {
                    int colIndex = Unsafe.getUnsafe().getInt(bufPos);
                    bufPos += Integer.BYTES;
                    byte entityType;
                    if (colIndex >= 0) {
                        entityType = Unsafe.getUnsafe().getByte(bufPos);
                        bufPos += Byte.BYTES;
                    } else {
                        int colNameLen = -1 * colIndex;
                        long nameLo = bufPos; // UTF8 encoded
                        long nameHi = bufPos + colNameLen;
                        job.charSink.clear();
                        if (!Chars.utf8Decode(nameLo, nameHi, job.charSink)) {
                            throw CairoException.instance(0)
                                    .put("invalid UTF8 in column name ")
                                    .put(job.floatingCharSink.asCharSequence(nameLo, nameHi));
                        }
                        bufPos = nameHi;
                        entityType = Unsafe.getUnsafe().getByte(bufPos);
                        bufPos += Byte.BYTES;
                        colIndex = writer.getMetadata().getColumnIndexQuiet(job.charSink);
                        if (colIndex < 0) {
                            // Cannot create a column with an open row, writer will commit when a column is created
                            row.cancel();
                            row = null;
                            int colType = DEFAULT_COLUMN_TYPES[entityType];
                            if (TableUtils.isValidInfluxColumnName(job.charSink)) {
                                writer.addColumn(job.charSink, colType);
                            } else {
                                throw CairoException.instance(0)
                                        .put("invalid column name [table=").put(writer.getTableName())
                                        .put(", columnName=").put(job.charSink)
                                        .put(']');
                            }
                            // Reset to beginning of entities
                            bufPos = firstEntityBufPos;
                            nEntity = -1;
                            row = writer.newRow(timestamp);
                            continue;
                        }
                    }

                    switch (entityType) {
                        case NewLineProtoParser.ENTITY_TYPE_TAG: {
                            int len = Unsafe.getUnsafe().getInt(bufPos);
                            bufPos += Integer.BYTES;
                            long hi = bufPos + 2L * len;
                            job.floatingCharSink.asCharSequence(bufPos, hi);
                            int symIndex = writer.getSymbolIndex(colIndex, job.floatingCharSink);
                            row.putSymIndex(colIndex, symIndex);
                            bufPos = hi;
                            break;
                        }

                        case NewLineProtoParser.ENTITY_TYPE_CACHED_TAG: {
                            int symIndex = Unsafe.getUnsafe().getInt(bufPos);
                            bufPos += Integer.BYTES;
                            row.putSymIndex(colIndex, symIndex);
                            break;
                        }

                        case NewLineProtoParser.ENTITY_TYPE_INTEGER: {
                            final int colType = writer.getMetadata().getColumnType(colIndex);
                            long v = Unsafe.getUnsafe().getLong(bufPos);
                            bufPos += Long.BYTES;
                            switch (colType) {
                                case ColumnType.LONG:
                                    row.putLong(colIndex, v);
                                    break;

                                case ColumnType.INT:
                                    if (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) {
                                        throw CairoException.instance(0)
                                                .put("line protocol integer is out of int bounds [columnIndex=").put(colIndex)
                                                .put(", v=").put(v)
                                                .put(']');
                                    }
                                    row.putInt(colIndex, (int) v);
                                    break;

                                case ColumnType.SHORT:
                                    if (v < Short.MIN_VALUE || v > Short.MAX_VALUE) {
                                        throw CairoException.instance(0)
                                                .put("line protocol integer is out of short bounds [columnIndex=").put(colIndex)
                                                .put(", v=").put(v)
                                                .put(']');
                                    }
                                    row.putShort(colIndex, (short) v);
                                    break;

                                case ColumnType.BYTE:
                                    if (v < Byte.MIN_VALUE || v > Byte.MAX_VALUE) {
                                        throw CairoException.instance(0)
                                                .put("line protocol integer is out of byte bounds [columnIndex=").put(colIndex)
                                                .put(", v=").put(v)
                                                .put(']');
                                    }
                                    row.putByte(colIndex, (byte) v);
                                    break;

                                case ColumnType.TIMESTAMP:
                                    row.putTimestamp(colIndex, v);
                                    break;

                                case ColumnType.DATE:
                                    row.putDate(colIndex, v);
                                    break;

                                default:
                                    throw CairoException.instance(0)
                                            .put("cast error for line protocol integer [columnIndex=").put(colIndex)
                                            .put(", columnType=").put(ColumnType.nameOf(colType))
                                            .put(']');
                            }
                            break;
                        }

                        case NewLineProtoParser.ENTITY_TYPE_FLOAT: {
                            double v = Unsafe.getUnsafe().getDouble(bufPos);
                            bufPos += Double.BYTES;
                            final int colType = writer.getMetadata().getColumnType(colIndex);
                            switch (colType) {
                                case ColumnType.DOUBLE:
                                    row.putDouble(colIndex, v);
                                    break;

                                case ColumnType.FLOAT:
                                    row.putFloat(colIndex, (float) v);
                                    break;

                                default:
                                    throw CairoException.instance(0)
                                            .put("cast error for line protocol float [columnIndex=").put(colIndex)
                                            .put(", columnType=").put(ColumnType.nameOf(colType))
                                            .put(']');
                            }
                            break;
                        }

                        case NewLineProtoParser.ENTITY_TYPE_BOOLEAN: {
                            byte b = Unsafe.getUnsafe().getByte(bufPos);
                            bufPos += Byte.BYTES;
                            row.putBool(colIndex, b == 1);
                            break;
                        }

                        case NewLineProtoParser.ENTITY_TYPE_STRING: {
                            int len = Unsafe.getUnsafe().getInt(bufPos);
                            bufPos += Integer.BYTES;
                            long hi = bufPos + 2L * len;
                            job.floatingCharSink.asCharSequence(bufPos, hi);
                            bufPos = hi;
                            final int colType = writer.getMetadata().getColumnType(colIndex);
                            if (colType == ColumnType.STRING) {
                                row.putStr(colIndex, job.floatingCharSink);
                            } else {
                                throw CairoException.instance(0)
                                        .put("cast error for line protocol string [columnIndex=").put(colIndex)
                                        .put(", columnType=").put(ColumnType.nameOf(colType))
                                        .put(']');
                            }
                            break;
                        }

                        case NewLineProtoParser.ENTITY_TYPE_LONG256: {
                            int len = Unsafe.getUnsafe().getInt(bufPos);
                            bufPos += Integer.BYTES;
                            long hi = bufPos + 2L * len;
                            job.floatingCharSink.asCharSequence(bufPos, hi);
                            row.putLong256(colIndex, job.floatingCharSink);
                            bufPos = hi;
                            break;
                        }

                        default:
                            throw new UnsupportedOperationException("entityType " + entityType + " is not implemented!");
                    }
                }
                row.append();
                tableUpdateDetails.handleRowAppended();
            } catch (CairoException ex) {
                LOG.error()
                        .$("could not write line protocol measurement [tableName=").$(tableUpdateDetails.tableName)
                        .$(", ex=").$(ex.getFlyweightMessage())
                        .$(", errno=").$(ex.getErrno())
                        .I$();
                if (row != null) {
                    row.cancel();
                }
            }
        }
    }

    class TableUpdateDetails implements Closeable {
        final String tableName;
        private final ThreadLocalDetails[] localDetailsArray;
        private int writerThreadId;
        // Number of updates since the last load rebalance, this is an estimate because its incremented by
        // multiple threads without synchronisation
        private int nUpdates = 0;
        private TableWriter writer;
        private boolean assignedToJob = false;
        private long lastMeasurementMillis = Long.MAX_VALUE;
        private long lastCommitMillis;
        private int nNetworkIoWorkers = 0;

        private TableUpdateDetails(String tableName, int writerThreadId, NetworkIOJob[] netIoJobs) {
            this.tableName = tableName;
            this.writerThreadId = writerThreadId;
            final int n = netIoJobs.length;
            localDetailsArray = new ThreadLocalDetails[n];
            for (int i = 0; i < n; i++) {
                localDetailsArray[i] = new ThreadLocalDetails(netIoJobs[i].getUnusedSymbolCaches());
            }
            lastCommitMillis = milliClock.getTicks();
        }

        @Override
        public void close() {
            tableUpdateDetailsLock.writeLock().lock();
            try {
                closeNoLock();
            } finally {
                tableUpdateDetailsLock.writeLock().unlock();
            }
        }

        private void closeNoLock() {
            if (writerThreadId != Integer.MIN_VALUE) {
                LOG.info().$("closing table writer [tableName=").$(tableName).$(']').$();
                if (null != writer) {
                    writer.commit();
                    writer = Misc.free(writer);
                }
                writerThreadId = Integer.MIN_VALUE;
            }
        }

        private void closeLocals() {
            for (int n = 0; n < localDetailsArray.length; n++) {
                LOG.info().$("closing table parsers [tableName=").$(tableName).$(']').$();
                localDetailsArray[n] = Misc.free(localDetailsArray[n]);
            }
        }

        int getSymbolIndex(ThreadLocalDetails localDetails, int colIndex, CharSequence symValue) {
            if (colIndex >= 0) {
                return localDetails.getSymbolIndex(colIndex, symValue);
            }
            return SymbolTable.VALUE_NOT_FOUND;
        }

        TableWriter getWriter() {
            if (null != writer) {
                return writer;
            }
            return writer = engine.getWriter(securityContext, tableName, "ilpTcp");
        }

        void handleRowAppended() {
            if (writer.checkMaxAndCommitLag()) {
                lastCommitMillis = milliClock.getTicks();
            }
        }

        void handleWriterRelease() {
            if (null != writer) {
                LOG.debug().$("release commit [table=").$(writer.getTableName()).I$();
                writer.commit();
                writer = Misc.free(writer);
                lastCommitMillis = milliClock.getTicks();
            }
        }

        void handleWriterThreadMaintenance(long ticks) {
            if (ticks - lastCommitMillis < maintenanceInterval) {
                return;
            }
            if (null != writer) {
                LOG.debug().$("maintenance commit [table=").$(writer.getTableName()).I$();
                writer.commit();
                lastCommitMillis = milliClock.getTicks();
            }
        }

        ThreadLocalDetails startNewMeasurementEvent(int workerId) {
            ThreadLocalDetails localDetails = localDetailsArray[workerId];
            lastMeasurementMillis = milliClock.getTicks();
            return localDetails;
        }

        void switchThreads() {
            assignedToJob = false;
            handleWriterRelease();
        }

        private class ThreadLocalDetails implements Closeable {
            private final Path path = new Path();
            private final ObjIntHashMap<CharSequence> columnIndexByName = new ObjIntHashMap<>();
            private final ObjList<SymbolCache> symbolCacheByColumnIndex = new ObjList<>();
            private final ObjList<SymbolCache> unusedSymbolCaches;

            ThreadLocalDetails(ObjList<SymbolCache> unusedSymbolCaches) {
                this.unusedSymbolCaches = unusedSymbolCaches;
            }

            @Override
            public void close() {
                Misc.freeObjList(symbolCacheByColumnIndex);
                Misc.free(path);
            }

            private SymbolCache addSymbolCache(int colIndex) {
                try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
                    path.of(cairoConfiguration.getRoot()).concat(tableName);
                    SymbolCache symCache;
                    final int lastUnusedSymbolCacheIndex = unusedSymbolCaches.size() - 1;
                    if (lastUnusedSymbolCacheIndex > -1) {
                        symCache = unusedSymbolCaches.get(lastUnusedSymbolCacheIndex);
                        unusedSymbolCaches.remove(lastUnusedSymbolCacheIndex);
                    } else {
                        symCache = new SymbolCache();
                    }
                    int symIndex = resolveSymbolIndex(reader.getMetadata(), colIndex);
                    symCache.of(cairoConfiguration, path, reader.getMetadata().getColumnName(colIndex), symIndex);
                    symbolCacheByColumnIndex.extendAndSet(colIndex, symCache);
                    return symCache;
                }
            }

            void clear() {
                columnIndexByName.clear();
                for (int n = 0, sz = symbolCacheByColumnIndex.size(); n < sz; n++) {
                    SymbolCache symCache = symbolCacheByColumnIndex.getQuick(n);
                    if (null != symCache) {
                        symCache.close();
                        unusedSymbolCaches.add(symCache);
                    }
                }
                symbolCacheByColumnIndex.clear();
            }

            int getColumnIndex(CharSequence colName) {
                final int colIndex = columnIndexByName.get(colName);
                if (colIndex != CharSequenceIntHashMap.NO_ENTRY_VALUE) {
                    return colIndex;
                }
                return getColumnIndex0(colName);
            }

            private int getColumnIndex0(CharSequence colName) {
                try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
                    TableReaderMetadata metadata = reader.getMetadata();
                    int colIndex = metadata.getColumnIndexQuiet(colName);
                    if (colIndex < 0) {
                        return -1;
                    }
                    // re-cache all column names once
                    columnIndexByName.clear();
                    for (int n = 0, sz = metadata.getColumnCount(); n < sz; n++) {
                        columnIndexByName.put(metadata.getColumnName(n), n);
                    }
                    return colIndex;
                }
            }

            int getSymbolIndex(int colIndex, CharSequence symValue) {
                SymbolCache symCache = symbolCacheByColumnIndex.getQuiet(colIndex);
                if (null == symCache) {
                    symCache = addSymbolCache(colIndex);
                }
                return symCache.getSymIndex(symValue);
            }

            private int resolveSymbolIndex(TableReaderMetadata metadata, int colIndex) {
                int symIndex = 0;
                for (int n = 0; n < colIndex; n++) {
                    if (metadata.getColumnType(n) == ColumnType.SYMBOL) {
                        symIndex++;
                    }
                }
                return symIndex;
            }
        }
    }

    private class WriterJob implements Job {
        private final int workerId;
        private final Sequence sequence;
        private final AppendOnlyVirtualMemory appendMemory = new AppendOnlyVirtualMemory();
        private final Path path = new Path();
        private final DirectCharSink charSink = new DirectCharSink(64);
        private final FloatingDirectCharSink floatingCharSink = new FloatingDirectCharSink();
        private final ObjList<TableUpdateDetails> assignedTables = new ObjList<>();
        private long lastMaintenanceMillis = 0;

        private WriterJob(int id, Sequence sequence) {
            super();
            this.workerId = id;
            this.sequence = sequence;
        }

        @Override
        public boolean run(int workerId) {
            assert this.workerId == workerId;
            boolean busy = drainQueue();
            doMaintenance();
            return busy;
        }

        private void close() {
            LOG.info().$("line protocol writer closing [threadId=").$(workerId).$(']').$();
            // Finish all jobs in the queue before stopping
            for (int n = 0; n < queue.getCapacity(); n++) {
                if (!run(workerId)) {
                    break;
                }
            }

            Misc.free(appendMemory);
            Misc.free(path);
            Misc.free(charSink);
            Misc.free(floatingCharSink);
            Misc.freeObjList(assignedTables);
            assignedTables.clear();
        }

        private void doMaintenance() {
            final long millis = milliClock.getTicks();
            if (millis - lastMaintenanceMillis < maintenanceInterval) {
                return;
            }

            lastMaintenanceMillis = millis;
            for (int n = 0, sz = assignedTables.size(); n < sz; n++) {
                assignedTables.getQuick(n).handleWriterThreadMaintenance(millis);
            }
        }

        private boolean drainQueue() {
            boolean busy = false;
            while (true) {
                long cursor;
                while ((cursor = sequence.next()) < 0) {
                    if (cursor == -1) {
                        return busy;
                    }
                }
                busy = true;
                final LineTcpMeasurementEvent event = queue.get(cursor);
                boolean eventProcessed;
                if (event.threadId == workerId) {
                    if (!event.tableUpdateDetails.assignedToJob) {
                        assignedTables.add(event.tableUpdateDetails);
                        event.tableUpdateDetails.assignedToJob = true;
                        LOG.info().$("assigned table to writer thread [tableName=").$(event.tableUpdateDetails.tableName).$(", threadId=").$(workerId).I$();
                    }
                    event.processMeasurementEvent(this);
                    eventProcessed = true;
                } else {
                    switch (event.threadId) {
                        case REBALANCE_EVENT_ID:
                            eventProcessed = processRebalance(event);
                            break;

                        case RELEASE_WRITER_EVENT_ID:
                            eventProcessed = processReleaseWriter(event);
                            break;

                        default:
                            eventProcessed = true;
                            break;
                    }
                }

                // by not releasing cursor we force the sequence to return us the same value over and over
                // until cursor value is released
                if (eventProcessed) {
                    sequence.done(cursor);
                } else {
                    return false;
                }
            }
        }

        private boolean processRebalance(LineTcpMeasurementEvent event) {
            if (event.rebalanceToThreadId == workerId) {
                // This thread is now a declared owner of the table, but it can only become actual
                // owner when "old" owner is fully done. This is a volatile variable on the event, used by both threads
                // to handover the table. The starting point is "false" and the "old" owner thread will eventually set this
                // to "true". In the mean time current thread will not be processing the queue until the handover is
                // complete
                if (event.rebalanceReleasedByFromThread) {
                    LOG.info().$("rebalance cycle, new thread ready [threadId=").$(workerId).$(", table=").$(event.tableUpdateDetails.tableName).$(']').$();
                    return true;
                }

                return false;
            }

            if (event.rebalanceFromThreadId == workerId) {
                for (int n = 0, sz = assignedTables.size(); n < sz; n++) {
                    if (assignedTables.get(n) == event.tableUpdateDetails) {
                        assignedTables.remove(n);
                        break;
                    }
                }
                LOG.info()
                        .$("rebalance cycle, old thread finished [threadId=").$(workerId)
                        .$(", table=").$(event.tableUpdateDetails.tableName)
                        .I$();
                event.tableUpdateDetails.switchThreads();
                event.rebalanceReleasedByFromThread = true;
            }

            return true;
        }

        private boolean processReleaseWriter(LineTcpMeasurementEvent event) {
            tableUpdateDetailsLock.readLock().lock();
            try {
                if (event.tableUpdateDetails.writerThreadId != workerId) {
                    return true;
                }
                TableUpdateDetails tableUpdateDetails = event.tableUpdateDetails;
                if (tableUpdateDetailsByTableName.keyIndex(tableUpdateDetails.tableName) < 0) {
                    // Table must have been re-assigned to an IO thread
                    return true;
                }
                LOG.info()
                        .$("releasing writer, its been idle since ").$ts(tableUpdateDetails.lastMeasurementMillis * 1_000)
                        .$("[tableName=").$(tableUpdateDetails.tableName)
                        .I$();

                tableUpdateDetails.handleWriterRelease();
            } finally {
                tableUpdateDetailsLock.readLock().unlock();
            }
            return true;
        }
    }

    class NetworkIOJobImpl implements NetworkIOJob, Job {
        private final IODispatcher<LineTcpConnectionContext> dispatcher;
        private final int workerId;
        private final CharSequenceObjHashMap<TableUpdateDetails> localTableUpdateDetailsByTableName = new CharSequenceObjHashMap<>();
        private final ObjList<SymbolCache> unusedSymbolCaches = new ObjList<>();
        // Context blocked on LineTcpMeasurementScheduler queue
        private LineTcpConnectionContext busyContext = null;
        private final IORequestProcessor<LineTcpConnectionContext> onRequest = this::onRequest;
        private long lastMaintenanceJobMillis = 0;

        NetworkIOJobImpl(IODispatcher<LineTcpConnectionContext> dispatcher, int workerId) {
            super();
            this.dispatcher = dispatcher;
            this.workerId = workerId;
        }

        @Override
        public void addTableUpdateDetails(TableUpdateDetails tableUpdateDetails) {
            localTableUpdateDetailsByTableName.put(tableUpdateDetails.tableName, tableUpdateDetails);
            tableUpdateDetails.nNetworkIoWorkers++;
            LOG.info()
                    .$("network IO thread using table [workerId=").$(workerId)
                    .$(", tableName=").$(tableUpdateDetails.tableName)
                    .$(", nNetworkIoWorkers=").$(tableUpdateDetails.nNetworkIoWorkers)
                    .$(']').$();
        }

        @Override
        public void close() {
            Misc.freeObjList(unusedSymbolCaches);
        }

        @Override
        public TableUpdateDetails getTableUpdateDetails(CharSequence tableName) {
            return localTableUpdateDetailsByTableName.get(tableName);
        }

        @Override
        public ObjList<SymbolCache> getUnusedSymbolCaches() {
            return unusedSymbolCaches;
        }

        @Override
        public int getWorkerId() {
            return workerId;
        }

        @Override
        public boolean run(int workerId) {
            assert this.workerId == workerId;
            boolean busy = false;
            if (busyContext != null) {
                if (handleIO(busyContext)) {
                    return true;
                }
                LOG.debug().$("context is no longer waiting on a full queue [fd=").$(busyContext.getFd()).$(']').$();
                busyContext = null;
                busy = true;
            }

            if (dispatcher.processIOQueue(onRequest)) {
                busy = true;
            }

            long millis = milliClock.getTicks();
            if ((millis - lastMaintenanceJobMillis) > maintenanceInterval) {
                busy = doMaintenance(millis);
                if (!busy) {
                    lastMaintenanceJobMillis = millis;
                }
            }

            return busy;
        }

        private boolean doMaintenance(long millis) {
            for (int n = 0, sz = localTableUpdateDetailsByTableName.size(); n < sz; n++) {
                TableUpdateDetails tableUpdateDetails = localTableUpdateDetailsByTableName.get(localTableUpdateDetailsByTableName.keys().get(n));
                if (millis - tableUpdateDetails.lastMeasurementMillis >= writerIdleTimeout) {
                    tableUpdateDetailsLock.writeLock().lock();
                    try {
                        if (tableUpdateDetails.nNetworkIoWorkers == 1) {
                            final long seq = getNextPublisherEventSequence();
                            if (seq > -1) {
                                LineTcpMeasurementEvent event = queue.get(seq);
                                event.createReleaseWriterEvent(tableUpdateDetails);
                                removeTableUpdateDetails(tableUpdateDetails);
                                final CharSequence tableName = tableUpdateDetails.tableName;
                                tableUpdateDetailsByTableName.remove(tableName);
                                idleTableUpdateDetailsByTableName.put(tableName, tableUpdateDetails);
                                pubSeq.done(seq);
                                LOG.info().$("active table going idle [tableName=").$(tableName).I$();
                            }
                            return true;
                        } else {
                            removeTableUpdateDetails(tableUpdateDetails);
                        }
                        return sz > 1;
                    } finally {
                        tableUpdateDetailsLock.writeLock().unlock();
                    }
                }
            }
            return false;
        }

        private boolean handleIO(LineTcpConnectionContext context) {
            if (!context.invalid()) {
                switch (context.handleIO(this)) {
                    case NEEDS_READ:
                        context.getDispatcher().registerChannel(context, IOOperation.READ);
                        return false;
                    case NEEDS_WRITE:
                        context.getDispatcher().registerChannel(context, IOOperation.WRITE);
                        return false;
                    case QUEUE_FULL:
                        return true;
                    case NEEDS_DISCONNECT:
                        context.getDispatcher().disconnect(context, DISCONNECT_REASON_UNKNOWN_OPERATION);
                        return false;
                }
            }
            return false;
        }

        private void onRequest(int operation, LineTcpConnectionContext context) {
            if (handleIO(context)) {
                busyContext = context;
                LOG.debug().$("context is waiting on a full queue [fd=").$(context.getFd()).$(']').$();
            }
        }

        private void removeTableUpdateDetails(TableUpdateDetails tableUpdateDetails) {
            localTableUpdateDetailsByTableName.remove(tableUpdateDetails.tableName);
            tableUpdateDetails.nNetworkIoWorkers--;
            tableUpdateDetails.localDetailsArray[workerId].clear();
            LOG.info()
                    .$("network IO thread released table [workerId=").$(workerId)
                    .$(", tableName=").$(tableUpdateDetails.tableName)
                    .$(", nNetworkIoWorkers=").$(tableUpdateDetails.nNetworkIoWorkers)
                    .I$();
        }
    }

    private class TableStructureAdapter implements TableStructure {
        private CharSequence tableName;
        private NewLineProtoParser protoParser;

        @Override
        public int getColumnCount() {
            return protoParser.getnEntities() + 1;
        }

        @Override
        public CharSequence getColumnName(int columnIndex) {
            assert columnIndex <= getColumnCount();
            if (columnIndex == getTimestampIndex()) {
                return "timestamp";
            }
            CharSequence colName = protoParser.getEntity(columnIndex).getName().toString();
            if (TableUtils.isValidColumnName(colName)) {
                return colName;
            }
            throw CairoException.instance(0).put("column name contains invalid characters [colName=").put(colName).put(']');
        }

        @Override
        public int getColumnType(int columnIndex) {
            if (columnIndex == getTimestampIndex()) {
                return ColumnType.TIMESTAMP;
            }
            return DEFAULT_COLUMN_TYPES[protoParser.getEntity(columnIndex).getType()];
        }

        @Override
        public int getIndexBlockCapacity(int columnIndex) {
            return 0;
        }

        @Override
        public boolean isIndexed(int columnIndex) {
            return false;
        }

        @Override
        public boolean isSequential(int columnIndex) {
            return false;
        }

        @Override
        public int getPartitionBy() {
            return defaultPartitionBy;
        }

        @Override
        public boolean getSymbolCacheFlag(int columnIndex) {
            return cairoConfiguration.getDefaultSymbolCacheFlag();
        }

        @Override
        public int getSymbolCapacity(int columnIndex) {
            return cairoConfiguration.getDefaultSymbolCapacity();
        }

        @Override
        public CharSequence getTableName() {
            return tableName;
        }

        @Override
        public int getTimestampIndex() {
            return protoParser.getnEntities();
        }

        @Override
        public int getMaxUncommittedRows() {
            return cairoConfiguration.getMaxUncommittedRows();
        }

        @Override
        public long getCommitLag() {
            return cairoConfiguration.getCommitLag();
        }

        TableStructureAdapter of(CharSequence tableName, NewLineProtoParser protoParser) {
            this.tableName = tableName;
            this.protoParser = protoParser;
            return this;
        }
    }

    static {
        DEFAULT_COLUMN_TYPES[NewLineProtoParser.ENTITY_TYPE_TAG] = ColumnType.SYMBOL;
        DEFAULT_COLUMN_TYPES[NewLineProtoParser.ENTITY_TYPE_FLOAT] = ColumnType.DOUBLE;
        DEFAULT_COLUMN_TYPES[NewLineProtoParser.ENTITY_TYPE_INTEGER] = ColumnType.LONG;
        DEFAULT_COLUMN_TYPES[NewLineProtoParser.ENTITY_TYPE_STRING] = ColumnType.STRING;
        DEFAULT_COLUMN_TYPES[NewLineProtoParser.ENTITY_TYPE_BOOLEAN] = ColumnType.BOOLEAN;
        DEFAULT_COLUMN_TYPES[NewLineProtoParser.ENTITY_TYPE_LONG256] = ColumnType.LONG256;
    }
}
