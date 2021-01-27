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

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.jetbrains.annotations.NotNull;

import io.questdb.Telemetry;
import io.questdb.cairo.AppendMemory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CairoSecurityContext;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableStructure;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriter.Row;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cutlass.line.LineProtoTimestampAdapter;
import io.questdb.cutlass.line.tcp.NewLineProtoParser.ProtoEntity;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.FanOut;
import io.questdb.mp.Job;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.Sequence;
import io.questdb.mp.WorkerPool;
import io.questdb.std.CharSequenceIntHashMap;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.std.str.DirectCharSink;
import io.questdb.std.str.FloatingDirectCharSink;
import io.questdb.std.str.Path;
import io.questdb.tasks.TelemetryTask;

class LineTcpMeasurementScheduler implements Closeable {
    private static final Log LOG = LogFactory.getLog(LineTcpMeasurementScheduler.class);
    private static final int REBALANCE_EVENT_ID = -1; // A rebalance event is used to rebalance load across different threads
    private static final int INCOMPLETE_EVENT_ID = -2; // An incomplete event is used when the queue producer has grabbed an event but is
    // not able to populate it for some reason, the event needs to be committed to the
    // queue incomplete
    private final CairoEngine engine;
    private final CairoSecurityContext securityContext;
    private final CairoConfiguration cairoConfiguration;
    private final MillisecondClock milliClock;
    private final RingQueue<LineTcpMeasurementEvent> queue;
    private final ReentrantLock tableUpdateDetailsLock = new ReentrantLock(true);
    private final CharSequenceObjHashMap<TableUpdateDetails> tableUpdateDetailsByTableName;
    private final int[] loadByThread;
    private final int nUpdatesPerLoadRebalance;
    private final double maxLoadRatio;
    private final int maxUncommittedRows;
    private final long maintenanceJobHysteresisInMs;
    private final long minIdleMsBeforeWriterRelease;
    private final int defaultPartitionBy;
    private Sequence pubSeq;
    private int nLoadCheckCycles = 0;
    private int nRebalances = 0;

    private final TableStructureAdapter tableStructureAdapter = new TableStructureAdapter();
    private final Path path = new Path();
    private final AppendMemory mem = new AppendMemory();

    LineTcpMeasurementScheduler(
            LineTcpReceiverConfiguration lineConfiguration,
            CairoEngine engine,
            WorkerPool writerWorkerPool
    ) {
        this.engine = engine;
        this.securityContext = lineConfiguration.getCairoSecurityContext();
        this.cairoConfiguration = engine.getConfiguration();
        this.milliClock = cairoConfiguration.getMillisecondClock();
        // Worker count is set to 1 because we do not use this execution context
        // in worker threads.
        tableUpdateDetailsByTableName = new CharSequenceObjHashMap<>();
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
        maxUncommittedRows = lineConfiguration.getMaxUncommittedRows();
        maintenanceJobHysteresisInMs = lineConfiguration.getMaintenanceJobHysteresisInMs();
        defaultPartitionBy = lineConfiguration.getDefaultPartitionBy();
        minIdleMsBeforeWriterRelease = lineConfiguration.getMinIdleMsBeforeWriterRelease();
    }

    @Override
    public void close() {
        // Both the writer and the net worker pools must have been closed so that their respective cleaners have run
        if (null != pubSeq) {
            pubSeq = null;
            tableUpdateDetailsByTableName.clear();
            for (int n = 0; n < queue.getCapacity(); n++) {
                queue.get(n).close();
            }
            path.close();
            mem.close();
        }
    }

    @NotNull
    private TableUpdateDetails assignTableToThread(String tableName, int keyIndex) {
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
        tableUpdateDetails = new TableUpdateDetails(tableName, threadId);
        tableUpdateDetailsByTableName.putAt(keyIndex, tableName, tableUpdateDetails);
        LOG.info().$("assigned ").$(tableName).$(" to thread ").$(threadId).$();
        return tableUpdateDetails;
    }

    private void calcThreadLoad() {
        Arrays.fill(loadByThread, 0);
        ObjList<CharSequence> tableNames = tableUpdateDetailsByTableName.keys();
        for (int n = 0, sz = tableNames.size(); n < sz; n++) {
            TableUpdateDetails stats = tableUpdateDetailsByTableName.get(tableNames.get(n));
            loadByThread[stats.threadId] += stats.nUpdates.get();
        }
    }

    boolean tryCommitNewEvent(NewLineProtoParser protoParser, FloatingDirectCharSink charSink) {
        TableUpdateDetails tableUpdateDetails;
        try {
            tableUpdateDetails = getTableUpdateDetails(protoParser);
        } catch (EntryUnavailableException ex) {
            // Table writer is locked
            LOG.info().$("could not get table writer [tableName=").$(protoParser.getMeasurementName()).$(", ex=").$(ex.getFlyweightMessage()).$(']').$();
            return false;
        } catch (CairoException ex) {
            // Table could not be created
            LOG.info().$("could not create table [tableName=").$(protoParser.getMeasurementName()).$(", ex=").$(ex.getFlyweightMessage()).$(']').$();
            return true;
        }
        if (null != tableUpdateDetails) {
            long seq = getNextPublisherEventSequence();
            if (seq >= 0) {
                try {
                    LineTcpMeasurementEvent event = queue.get(seq);
                    event.threadId = INCOMPLETE_EVENT_ID;
                    event.createMeasurementEvent(tableUpdateDetails, protoParser, charSink);
                    return true;
                } finally {
                    pubSeq.done(seq);
                    if (tableUpdateDetails.nUpdates.incrementAndGet() > nUpdatesPerLoadRebalance) {
                        if (tableUpdateDetailsLock.tryLock()) {
                            try {
                                loadRebalance();
                            } finally {
                                tableUpdateDetailsLock.unlock();
                            }
                        }
                    }
                }
            }
        }
        return false;
    }

    private TableUpdateDetails getTableUpdateDetails(NewLineProtoParser protoParser) {
        tableUpdateDetailsLock.lock();
        final int keyIndex;
        try {
            keyIndex = tableUpdateDetailsByTableName.keyIndex(protoParser.getMeasurementName());
            if (keyIndex < 0) {
                return tableUpdateDetailsByTableName.valueAt(keyIndex);
            }

            // Original table name is transient
            String tableName = protoParser.getMeasurementName().toString();
            int status = engine.getStatus(securityContext, path, tableName, 0, tableName.length());
            if (status != TableUtils.TABLE_EXISTS) {
                engine.createTable(securityContext, mem, path, tableStructureAdapter.of(tableName, protoParser));
            }
            TelemetryTask.doStoreTelemetry(engine, Telemetry.SYSTEM_ILP_RESERVE_WRITER, Telemetry.ORIGIN_ILP_TCP);

            return assignTableToThread(tableName, keyIndex);
        } finally {
            tableUpdateDetailsLock.unlock();
        }
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
        while ((seq = pubSeq.next()) == -2) {
            ;
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
                if (stats.threadId == highestLoadedThreadId && stats.nUpdates.get() > 0) {
                    nTables++;
                    if (stats.nUpdates.get() < lowestLoad) {
                        lowestLoad = stats.nUpdates.get();
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
            stats.nUpdates.set(0);
        }

        if (null != tableToMove) {
            long seq = getNextPublisherEventSequence();
            if (seq >= 0) {
                try {
                    LineTcpMeasurementEvent event = queue.get(seq);
                    event.threadId = INCOMPLETE_EVENT_ID;
                    event.createRebalanceEvent(fromThreadId, toThreadId, tableToMove);
                    tableToMove.threadId = toThreadId;
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

    private interface EntityValueEventComitter {
        long put(LineTcpMeasurementEvent event, ProtoEntity entity, long bufPos, FloatingDirectCharSink charSink);
    }

    private static EntityValueEventComitter[] ENTITY_VALUE_COMMITERS = new EntityValueEventComitter[NewLineProtoParser.N_ENTITY_TYPES];
    private static EntityValueEventComitter STRING_ENTITY_VALUE_COMMITER = (event, entity, bufPos, charSink) -> {
        int l = entity.getValue().length();
        Unsafe.getUnsafe().putInt(bufPos, l);
        bufPos += Integer.BYTES;
        long hi = bufPos + 2 * l;
        charSink.of(bufPos, hi);
        if (!Chars.utf8Decode(entity.getValue().getLo(), entity.getValue().getHi(), charSink)) {
            throw CairoException.instance(0).put("invalid UTF8 in value for ").put(entity.getName());
        }
        return hi;
    };
    static {
        ENTITY_VALUE_COMMITERS[NewLineProtoParser.ENTITY_TYPE_TAG] = STRING_ENTITY_VALUE_COMMITER;
        ENTITY_VALUE_COMMITERS[NewLineProtoParser.ENTITY_TYPE_FLOAT] = (event, entity, bufPos, charSink) -> {
            Unsafe.getUnsafe().putDouble(bufPos, entity.getFloatValue());
            bufPos += Double.BYTES;
            return bufPos;
        };
        ENTITY_VALUE_COMMITERS[NewLineProtoParser.ENTITY_TYPE_INTEGER] = (event, entity, bufPos, charSink) -> {
            Unsafe.getUnsafe().putLong(bufPos, entity.getIntegerValue());
            bufPos += Long.BYTES;
            return bufPos;
        };
        ENTITY_VALUE_COMMITERS[NewLineProtoParser.ENTITY_TYPE_STRING] = STRING_ENTITY_VALUE_COMMITER;
        ENTITY_VALUE_COMMITERS[NewLineProtoParser.ENTITY_TYPE_BOOLEAN] = (event, entity, bufPos, charSink) -> {
            Unsafe.getUnsafe().putByte(bufPos, (byte) (entity.getBooleanValue() ? 1 : 0));
            bufPos += Byte.BYTES;
            return bufPos;
        };
        ENTITY_VALUE_COMMITERS[NewLineProtoParser.ENTITY_TYPE_LONG256] = STRING_ENTITY_VALUE_COMMITER;
    }
    private static int[] DEFAULT_COLUMN_TYPES = new int[NewLineProtoParser.N_ENTITY_TYPES];
    static {
        DEFAULT_COLUMN_TYPES[NewLineProtoParser.ENTITY_TYPE_TAG] = ColumnType.SYMBOL;
        DEFAULT_COLUMN_TYPES[NewLineProtoParser.ENTITY_TYPE_FLOAT] = ColumnType.DOUBLE;
        DEFAULT_COLUMN_TYPES[NewLineProtoParser.ENTITY_TYPE_INTEGER] = ColumnType.LONG;
        DEFAULT_COLUMN_TYPES[NewLineProtoParser.ENTITY_TYPE_STRING] = ColumnType.STRING;
        DEFAULT_COLUMN_TYPES[NewLineProtoParser.ENTITY_TYPE_BOOLEAN] = ColumnType.BOOLEAN;
        DEFAULT_COLUMN_TYPES[NewLineProtoParser.ENTITY_TYPE_LONG256] = ColumnType.LONG256;
    }

    private interface EntityValueRowWriter {
        long write(long bufPos, byte entityType, Row row, int columnIndex, FloatingDirectCharSink charSink);
    }

    private static final EntityValueRowWriter SYMBOL_ENTITY_VALUE_WRITER = (bufPos, entityType, row, columnIndex, charSink) -> {
        if (entityType != NewLineProtoParser.ENTITY_TYPE_TAG) {
            throw CairoException.instance(0).put("expected a line protocol tag [entityType=").put(entityType).put(']');
        }
        int len = Unsafe.getUnsafe().getInt(bufPos);
        bufPos += Integer.BYTES;
        long hi = bufPos + 2 * len;
        charSink.asCharSequence(bufPos, hi);
        row.putSym(columnIndex, charSink);
        return hi;
    };

    private static final EntityValueRowWriter INTEGER_TO_LONG_ENTITY_VALUE_WRITER = (bufPos, entityType, row, columnIndex, charSink) -> {
        if (entityType != NewLineProtoParser.ENTITY_TYPE_INTEGER) {
            throw CairoException.instance(0).put("expected a line protocol integer [entityType=").put(entityType).put(']');
        }
        long v = Unsafe.getUnsafe().getLong(bufPos);
        bufPos += Long.BYTES;
        row.putLong(columnIndex, v);
        return bufPos;
    };

    private static final EntityValueRowWriter INTEGER_TO_SHORT_ENTITY_VALUE_WRITER = (bufPos, entityType, row, columnIndex, charSink) -> {
        if (entityType != NewLineProtoParser.ENTITY_TYPE_INTEGER) {
            throw CairoException.instance(0).put("expected a line protocol integer [entityType=").put(entityType).put(']');
        }
        long v = Unsafe.getUnsafe().getLong(bufPos);
        bufPos += Long.BYTES;
        if (v < Short.MIN_VALUE || v > Short.MAX_VALUE) {
            throw CairoException.instance(0).put("line protocol integer is out of short bounds [columnIndex=").put(columnIndex).put(", v=").put(v).put(']');
        }
        row.putShort(columnIndex, (short) v);
        return bufPos;
    };

    private static final EntityValueRowWriter INTEGER_TO_TIMESTAMP_ENTITY_VALUE_WRITER = (bufPos, entityType, row, columnIndex, charSink) -> {
        if (entityType != NewLineProtoParser.ENTITY_TYPE_INTEGER) {
            throw CairoException.instance(0).put("expected a line protocol integer [entityType=").put(entityType).put(']');
        }
        long v = Unsafe.getUnsafe().getLong(bufPos);
        bufPos += Long.BYTES;
        row.putTimestamp(columnIndex, v);
        return bufPos;
    };

    private static final EntityValueRowWriter FLOAT_TO_DOUBLE_ENTITY_VALUE_WRITER = (bufPos, entityType, row, columnIndex, charSink) -> {
        if (entityType != NewLineProtoParser.ENTITY_TYPE_FLOAT) {
            throw CairoException.instance(0).put("expected a line protocol float [entityType=").put(entityType).put(']');
        }
        double v = Unsafe.getUnsafe().getDouble(bufPos);
        bufPos += Double.BYTES;
        row.putDouble(columnIndex, v);
        return bufPos;
    };

    private static final EntityValueRowWriter LONG256_ENTITY_VALUE_WRITER = (bufPos, entityType, row, columnIndex, charSink) -> {
        if (entityType != NewLineProtoParser.ENTITY_TYPE_LONG256) {
            throw CairoException.instance(0).put("expected a line protocol long256 [entityType=").put(entityType).put(']');
        }
        int len = Unsafe.getUnsafe().getInt(bufPos);
        bufPos += Integer.BYTES;
        long hi = bufPos + 2 * len;
        charSink.asCharSequence(bufPos, hi);
        row.putLong256(columnIndex, charSink);
        return hi;
    };

    private static final EntityValueRowWriter resolveRowWriter(int columnType, byte entityType) {
        switch (entityType) {
            case NewLineProtoParser.ENTITY_TYPE_TAG:
                if (columnType == ColumnType.SYMBOL) {
                    return SYMBOL_ENTITY_VALUE_WRITER;
                }
                break;
            case NewLineProtoParser.ENTITY_TYPE_INTEGER:
                if (columnType == ColumnType.LONG) {
                    return INTEGER_TO_LONG_ENTITY_VALUE_WRITER;
                }
                if (columnType == ColumnType.TIMESTAMP) {
                    return INTEGER_TO_TIMESTAMP_ENTITY_VALUE_WRITER;
                }
                if (columnType == ColumnType.SHORT) {
                    return INTEGER_TO_SHORT_ENTITY_VALUE_WRITER;
                }
                break;
            case NewLineProtoParser.ENTITY_TYPE_FLOAT:
                if (columnType == ColumnType.DOUBLE) {
                    return FLOAT_TO_DOUBLE_ENTITY_VALUE_WRITER;
                }
                break;
            case NewLineProtoParser.ENTITY_TYPE_LONG256:
                if (columnType == ColumnType.LONG256) {
                    return LONG256_ENTITY_VALUE_WRITER;
                }
                break;
        }
        throw CairoException.instance(0).put("line prototol entity type cannot be mapped to column type [entityType=").put(entityType).put(", columnType=").put(columnType).put(']');
    }

    private class LineTcpMeasurementEvent implements Closeable {
        private final MicrosecondClock clock;
        private final LineProtoTimestampAdapter timestampAdapter;
        private int threadId;

        private TableUpdateDetails tableUpdateDetails;
        private long bufLo;
        private long bufSize;

        private int rebalanceFromThreadId;
        private int rebalanceToThreadId;
        private volatile boolean rebalanceReleasedByFromThread;

        private LineTcpMeasurementEvent(int maxMeasurementSize, MicrosecondClock clock, LineProtoTimestampAdapter timestampAdapter) {
            bufSize = (maxMeasurementSize / 4) * (Integer.BYTES + Double.BYTES + 1);
            bufLo = Unsafe.malloc(bufSize);
            this.clock = clock;
            this.timestampAdapter = timestampAdapter;
        }

        @Override
        public void close() {
            Unsafe.free(bufLo, bufSize);
            bufLo = 0;
        }

        private void clear() {
        }

        void createRebalanceEvent(int fromThreadId, int toThreadId, TableUpdateDetails tableUpdateDetails) {
            clear();
            threadId = REBALANCE_EVENT_ID;
            rebalanceFromThreadId = fromThreadId;
            rebalanceToThreadId = toThreadId;
            this.tableUpdateDetails = tableUpdateDetails;
            rebalanceReleasedByFromThread = false;
        }

        void createMeasurementEvent(TableUpdateDetails tableUpdateDetails, NewLineProtoParser protoParser, FloatingDirectCharSink charSink) {
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
                int colIndex = tableUpdateDetails.getColumnIndex(entity.getName());
                if (colIndex < 0) {
                    int colNameLen = entity.getName().length();
                    Unsafe.getUnsafe().putInt(bufPos, -1 * colNameLen);
                    bufPos += Integer.BYTES;
                    Unsafe.getUnsafe().copyMemory(entity.getName().getLo(), bufPos, colNameLen);
                    bufPos += colNameLen;
                } else {
                    Unsafe.getUnsafe().putInt(bufPos, colIndex);
                    bufPos += Integer.BYTES;
                }
                byte entityType = entity.getType();
                Unsafe.getUnsafe().putByte(bufPos, entity.getType());
                bufPos += Byte.BYTES;
                bufPos = ENTITY_VALUE_COMMITERS[entityType].put(this, entity, bufPos, charSink);
            }
            threadId = tableUpdateDetails.threadId;
        }

        @SuppressWarnings("resource")
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
                            throw CairoException.instance(0).put("invalid UTF8 in column name ").put(job.floatingCharSink.asCharSequence(nameLo, nameHi));
                        }
                        bufPos = nameHi;
                        entityType = Unsafe.getUnsafe().getByte(bufPos);
                        bufPos += Byte.BYTES;
                        colIndex = writer.getMetadata().getColumnIndexQuiet(job.charSink);
                        if (colIndex < 0) {
                            // Cannot create a column with an open row, writer will commit when a column is created
                            row.cancel();
                            row = null;
                            int columnType = DEFAULT_COLUMN_TYPES[entityType];
                            if (TableUtils.isValidColumnName(job.charSink)) {
                                colIndex = writer.getMetadata().getColumnCount();
                                writer.addColumn(job.charSink, columnType);
                            } else {
                                throw CairoException.instance(0).put("invalid column name [table=").put(writer.getName())
                                        .put(", columnName=").put(job.charSink).put(']');
                            }
                            // Reset to begining of entities
                            bufPos = firstEntityBufPos;
                            nEntity = -1;
                            row = writer.newRow(timestamp);
                            continue;
                        }
                    }
                    EntityValueRowWriter valueWriter = getValueWriter(writer, colIndex, entityType);
                    bufPos = valueWriter.write(bufPos, entityType, row, colIndex, job.floatingCharSink);
                }
                row.append();
                tableUpdateDetails.handleRowAppended();
            } catch (CairoException ex) {
                LOG.error().$("could not write line protocol measurement [tableName=").$(tableUpdateDetails.tableName).$(", ex=").$(ex.getFlyweightMessage()).$(']').$();
                if (row != null) {
                    row.cancel();
                }
            }
        }

        private EntityValueRowWriter getValueWriter(TableWriter writer, int colIndex, byte entityType) {
            EntityValueRowWriter valueWriter = tableUpdateDetails.valueWriters.size() > colIndex ? tableUpdateDetails.valueWriters.get(colIndex) : null;
            if (null == valueWriter) {
                int columnType = writer.getMetadata().getColumnType(colIndex);
                valueWriter = resolveRowWriter(columnType, entityType);
                tableUpdateDetails.valueWriters.extendAndSet(colIndex, valueWriter);
            }
            return valueWriter;
        }

        boolean isRebalanceEvent() {
            return threadId == REBALANCE_EVENT_ID;
        }
    }

    private class TableUpdateDetails implements Closeable {
        private final String tableName;
        private int threadId = Integer.MIN_VALUE;
        private final AtomicInteger nUpdates = new AtomicInteger(); // Number of updates since the last load rebalance
        private TableWriter writer;
        private int nUncommitted = 0;
        private ThreadLocal<CharSequenceIntHashMap> refColumnIndexByName = ThreadLocal.withInitial(() -> {
            return new CharSequenceIntHashMap();
        });
        private final ObjList<EntityValueRowWriter> valueWriters = new ObjList<>();
        private boolean assignedToJob = false;
        private long lastWriterCommitEpochMs = Long.MAX_VALUE;

        private TableUpdateDetails(String tableName, int threadId) {
            super();
            this.tableName = tableName;
            this.threadId = threadId;
        }

        private void handleRowAppended() {
            nUncommitted++;
            if (nUncommitted >= maxUncommittedRows) {
                writer.commit();
                lastWriterCommitEpochMs = milliClock.getTicks();
                nUncommitted = 0;
            }
        }

        private void handleMaintenance() {
            if (nUncommitted == 0) {
                if ((milliClock.getTicks() - lastWriterCommitEpochMs) >= minIdleMsBeforeWriterRelease) {
                    LOG.info().$("releasing writer, its been idle since ").$ts(lastWriterCommitEpochMs * 1_000).$(" [tableName=").$(tableName).$(']').$();
                    writer.close();
                    writer = null;
                    lastWriterCommitEpochMs = Long.MAX_VALUE;
                }
                return;
            }
            writer.commit();
            lastWriterCommitEpochMs = milliClock.getTicks();
            nUncommitted = 0;
        }

        TableWriter getWriter() {
            if (null == writer) {
                writer = engine.getWriter(securityContext, tableName);
            }
            return writer;
        }

        int getColumnIndex(CharSequence colName) {
            CharSequenceIntHashMap columnIndexByName = refColumnIndexByName.get();
            int colIndex = columnIndexByName.get(colName);
            if (colIndex == CharSequenceIntHashMap.NO_ENTRY_VALUE) {
                try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
                    colIndex = reader.getMetadata().getColumnIndexQuiet(colName);
                    if (colIndex < 0) {
                        return -1;
                    }
                    columnIndexByName.put(colName.toString(), colIndex);
                }
            }
            return colIndex;
        }

        void switchThreads() {
            assignedToJob = false;
            if (null != writer) {
                if (nUncommitted > 0) {
                    writer.commit();
                }
                writer.close();
                writer = null;
            }
        }

        @Override
        public void close() throws IOException {
            if (threadId != Integer.MIN_VALUE) {
                refColumnIndexByName = null;
                Misc.freeObjList(valueWriters);
                valueWriters.clear();
                if (null != writer) {
                    if (nUncommitted > 0) {
                        writer.commit();
                    }
                    writer.close();
                    writer = null;
                }
                threadId = Integer.MIN_VALUE;
            }
        }
    }

    private class WriterJob implements Job {
        private final int id;
        private final Sequence sequence;
        private final AppendMemory appendMemory = new AppendMemory();
        private final Path path = new Path();
        private long lastMaintenanceJobMillis = 0;

        private final DirectCharSink charSink = new DirectCharSink(64);
        private final FloatingDirectCharSink floatingCharSink = new FloatingDirectCharSink();
        private final ObjList<TableUpdateDetails> assignedTables = new ObjList<>();

        private WriterJob(
                int id,
                Sequence sequence
        ) {
            super();
            this.id = id;
            this.sequence = sequence;
        }

        @Override
        public boolean run(int workerId) {
            assert workerId == id;
            boolean busy = drainQueue();
            doMaintenance();
            return busy;
        }

        private void close() {
            // Finish all jobs in the queue before stopping
            for (int n = 0; n < queue.getCapacity(); n++) {
                if (!run(id)) {
                    break;
                }
            }

            appendMemory.close();
            path.close();
            charSink.close();
            floatingCharSink.close();
            Misc.freeObjList(assignedTables);
            assignedTables.clear();
        }

        private void doMaintenance() {
            long millis = milliClock.getTicks();
            if ((millis - lastMaintenanceJobMillis) < maintenanceJobHysteresisInMs) {
                return;
            }

            lastMaintenanceJobMillis = millis;
            for (int n = 0, sz = assignedTables.size(); n < sz; n++) {
                assignedTables.getQuick(n).handleMaintenance();
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
                if (event.threadId == id) {
                    if (!event.tableUpdateDetails.assignedToJob) {
                        assignedTables.add(event.tableUpdateDetails);
                        event.tableUpdateDetails.assignedToJob = true;
                        LOG.info().$("assigned table to writer thread [tableName=").$(event.tableUpdateDetails.tableName).$(", threadId=").$(id).$();
                    }
                    event.processMeasurementEvent(this);
                    eventProcessed = true;
                } else {
                    if (event.isRebalanceEvent()) {
                        eventProcessed = processRebalance(event);
                    } else {
                        eventProcessed = true;
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
            if (event.rebalanceToThreadId == id) {
                // This thread is now a declared owner of the table, but it can only become actual
                // owner when "old" owner is fully done. This is a volatile variable on the event, used by both threads
                // to handover the table. The starting point is "false" and the "old" owner thread will eventually set this
                // to "true". In the mean time current thread will not be processing the queue until the handover is
                // complete
                if (event.rebalanceReleasedByFromThread) {
                    LOG.info().$("rebalance cycle, new thread ready [threadId=").$(id).$(", table=").$(event.tableUpdateDetails.tableName).$(']').$();
                    return true;
                }

                return false;
            }

            if (event.rebalanceFromThreadId == id) {
                for (int n = 0, sz = assignedTables.size(); n < sz; n++) {
                    if (assignedTables.get(n) == event.tableUpdateDetails) {
                        assignedTables.remove(n);
                        break;
                    }
                }
                LOG.info().$("rebalance cycle, old thread finished [threadId=").$(id).$(", table=").$(event.tableUpdateDetails.tableName).$(']').$(", nUncommitted=")
                        .$(event.tableUpdateDetails.nUncommitted).$(']').$();
                event.tableUpdateDetails.switchThreads();
                event.rebalanceReleasedByFromThread = true;
            }

            return true;
        }
    }

    private class TableStructureAdapter implements TableStructure {
        private String tableName;
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

        TableStructureAdapter of(String tableName, NewLineProtoParser protoParser) {
            this.tableName = tableName;
            this.protoParser = protoParser;
            return this;
        }
    }
}
