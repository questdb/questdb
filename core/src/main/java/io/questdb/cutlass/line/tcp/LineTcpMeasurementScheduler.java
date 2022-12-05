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

import static io.questdb.cutlass.line.tcp.LineTcpParser.ENTITY_TYPE_NULL;
import static io.questdb.cutlass.line.tcp.TableUpdateDetails.ThreadLocalDetails.COLUMN_NOT_FOUND;

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

    private TableUpdateDetails getTableUpdateDetailsFromSharedArea(@NotNull NetworkIOJob netIoJob, @NotNull LineTcpParser parser) {
        final DirectByteCharSequence tableNameUtf8 = parser.getMeasurementName();
        final StringSink tableNameUtf16 = tableNameSinks[netIoJob.getWorkerId()];
        tableNameUtf16.clear();
        Chars.utf8Decode(tableNameUtf8.getLo(), tableNameUtf8.getHi(), tableNameUtf16);

        tableUpdateDetailsLock.writeLock().lock();
        try {
            TableUpdateDetails tud;
            // check if the global cache has the table
            final int tudKeyIndex = tableUpdateDetailsUtf16.keyIndex(tableNameUtf16);
            if (tudKeyIndex < 0) {
                // it does, which means that table is non-WAL
                // we should not have "shared" WAL tables
                tud = tableUpdateDetailsUtf16.valueAt(tudKeyIndex);
            } else {
                // if table doesn't exist, we might need to create it
                // this could end up being WAL table, but we do not know yet
                int status = engine.getStatus(securityContext, path, tableNameUtf16, 0, tableNameUtf16.length());
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
                    LOG.info().$("creating table [tableName=").$(tableNameUtf16).$(']').$();
                    engine.createTable(securityContext, ddlMem, path, tsa);
                }

                // by the time we get here, definitely exists on disk
                // check the global idle cache - TUD can be there
                final int idleTudKeyIndex = idleTableUpdateDetailsUtf16.keyIndex(tableNameUtf16);
                if (idleTudKeyIndex < 0) {
                    // TUD is found in global idle cache - this meant it is non-WAL
                    tud = idleTableUpdateDetailsUtf16.valueAt(idleTudKeyIndex);
                    LOG.info().$("idle table going active [tableName=").$(tud.getTableNameUtf16()).I$();
                    if (tud.getWriter() == null) {
                        tud.closeNoLock();
                        // Use actual table name from the "details" to avoid case mismatches in the
                        // WriterPool. There was an error in the LineTcpReceiverFuzzTest, which helped
                        // to identify the cause
                        tud = unsafeAssignTableToWriterThread(tudKeyIndex, tud.getTableNameUtf16());
                    } else {
                        idleTableUpdateDetailsUtf16.removeAt(idleTudKeyIndex);
                        tableUpdateDetailsUtf16.putAt(tudKeyIndex, tud.getTableNameUtf16(), tud);
                    }
                } else {
                    TelemetryTask.doStoreTelemetry(engine, Telemetry.SYSTEM_ILP_RESERVE_WRITER, Telemetry.ORIGIN_ILP_TCP);
                    // check if table on disk is WAL
                    path.of(engine.getConfiguration().getRoot());
                    if (TableSequencerAPI.isWalTable(tableNameUtf16, path, ddlMem.getFilesFacade())) {
                        // create WAL-oriented TUD and NOT add it to the global cache
                        tud = new TableUpdateDetails(
                                configuration,
                                engine,
                                engine.getWalWriter(
                                        securityContext,
                                        tableNameUtf16
                                ),
                                -1,
                                netIoJobs,
                                defaultColumnTypes
                        );
                    } else {
                        tud = unsafeAssignTableToWriterThread(tudKeyIndex, tableNameUtf16);
                    }
                }
            }

            // here we need to create a string image (mangled) of utf8 char sequence
            // deliberately not decoding UTF8, store bytes as chars each
            tableNameUtf16.clear();
            tableNameUtf16.put(tableNameUtf8);

            // at this point this is not UTF16 string
            netIoJob.addTableUpdateDetails(tableNameUtf16.toString(), tud);
            return tud;
        } finally {
            tableUpdateDetailsLock.writeLock().unlock();
        }
    }

    private boolean isOpen() {
        return null != pubSeq;
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

        final TableUpdateDetails tud = new TableUpdateDetails(
                configuration,
                engine,
                // get writer here to avoid constructing
                // object instance and potentially leaking memory if
                // writer allocation fails
                engine.getTableWriterAPI(securityContext, tableNameUtf16, "tcpIlp"),
                threadId,
                netIoJobs,
                defaultColumnTypes
        );
        tableUpdateDetailsUtf16.putAt(tudKeyIndex, tud.getTableNameUtf16(), tud);
        LOG.info().$("assigned ").$(tableNameUtf16).$(" to thread ").$(threadId).$();
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
            tud = netIoJob.getLocalTableDetails(parser.getMeasurementName());
            if (tud == null) {
                tud = getTableUpdateDetailsFromSharedArea(netIoJob, parser);
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

        if (tud.getWriterThreadId() == -1) {
            // this is a WAL TUD
            return appendToWal(netIoJob, parser, tud);
        }
        return dispatchEvent(netIoJob, parser, tud);
    }

    private boolean appendToWal(NetworkIOJob netIoJob, LineTcpParser parser, TableUpdateDetails tud) {
        // pass 1: create all columns that do not exist
        for (int i = 0, n = parser.getEntityCount(); i < n; i++) {

        }

            final TableUpdateDetails.ThreadLocalDetails localDetails = tud.getThreadLocalDetails(netIoJob.getWorkerId());
        localDetails.resetStateIfNecessary();
        this.tableUpdateDetails = tud;
        long timestamp = parser.getTimestamp();
        if (timestamp != LineTcpParser.NULL_TIMESTAMP) {
            timestamp = timestampAdapter.getMicros(timestamp);
        }
        buffer.addStructureVersion(buffer.getAddress(), localDetails.getStructureVersion());
        // timestamp, entitiesWritten are written to the buffer after saving all fields
        // because their values are worked out while the columns are processed
        long offset = buffer.getAddressAfterHeader();
        int entitiesWritten = 0;
        for (int nEntity = 0, n = parser.getEntityCount(); nEntity < n; nEntity++) {
            LineTcpParser.ProtoEntity entity = parser.getEntity(nEntity);
            byte entityType = entity.getType();
            int colType;
            int columnWriterIndex = localDetails.getColumnIndex(entity.getName(), parser.hasNonAsciiChars());
            if (columnWriterIndex > -1) {
                // column index found, processing column by index
                if (columnWriterIndex == tud.getTimestampIndex()) {
                    timestamp = timestampAdapter.getMicros(entity.getLongValue());
                    continue;
                }

                offset = buffer.addColumnIndex(offset, columnWriterIndex);
                colType = localDetails.getColumnType(columnWriterIndex);
            } else if (columnWriterIndex == COLUMN_NOT_FOUND) {
                // send column by name
                final String columnName = localDetails.getColName();
                if (autoCreateNewColumns && TableUtils.isValidColumnName(columnName, maxColumnNameLength)) {
                    offset = buffer.addColumnName(offset, columnName);
                    colType = localDetails.getColumnType(columnName, entityType);
                } else if (!autoCreateNewColumns) {
                    throw newColumnsNotAllowed(columnName);
                } else {
                    throw invalidColNameError(columnName);
                }
            } else {
                // duplicate column, skip
                // we could set a boolean in the config if we want to throw exception instead
                continue;
            }

            entitiesWritten++;
            switch (entityType) {
                case LineTcpParser.ENTITY_TYPE_TAG: {
                    if (ColumnType.tagOf(colType) == ColumnType.SYMBOL) {
                        offset = buffer.addSymbol(offset, entity.getValue(), parser.hasNonAsciiChars(), localDetails.getSymbolLookup(columnWriterIndex));
                    } else {
                        throw castError("tag", columnWriterIndex, colType, entity.getName());
                    }
                    break;
                }
                case LineTcpParser.ENTITY_TYPE_INTEGER: {
                    switch (ColumnType.tagOf(colType)) {
                        case ColumnType.LONG:
                            offset = buffer.addLong(offset, entity.getLongValue());
                            break;

                        case ColumnType.INT: {
                            final long entityValue = entity.getLongValue();
                            if (entityValue >= Integer.MIN_VALUE && entityValue <= Integer.MAX_VALUE) {
                                offset = buffer.addInt(offset, (int) entityValue);
                            } else if (entityValue == Numbers.LONG_NaN) {
                                offset = buffer.addInt(offset, Numbers.INT_NaN);
                            } else {
                                throw boundsError(entityValue, columnWriterIndex, ColumnType.INT);
                            }
                            break;
                        }
                        case ColumnType.SHORT: {
                            final long entityValue = entity.getLongValue();
                            if (entityValue >= Short.MIN_VALUE && entityValue <= Short.MAX_VALUE) {
                                offset = buffer.addShort(offset, (short) entityValue);
                            } else if (entityValue == Numbers.LONG_NaN) {
                                offset = buffer.addShort(offset, (short) 0);
                            } else {
                                throw boundsError(entityValue, columnWriterIndex, ColumnType.SHORT);
                            }
                            break;
                        }
                        case ColumnType.BYTE: {
                            final long entityValue = entity.getLongValue();
                            if (entityValue >= Byte.MIN_VALUE && entityValue <= Byte.MAX_VALUE) {
                                offset = buffer.addByte(offset, (byte) entityValue);
                            } else if (entityValue == Numbers.LONG_NaN) {
                                offset = buffer.addByte(offset, (byte) 0);
                            } else {
                                throw boundsError(entityValue, columnWriterIndex, ColumnType.BYTE);
                            }
                            break;
                        }
                        case ColumnType.TIMESTAMP:
                            offset = buffer.addTimestamp(offset, entity.getLongValue());
                            break;

                        case ColumnType.DATE:
                            offset = buffer.addDate(offset, entity.getLongValue());
                            break;

                        case ColumnType.DOUBLE:
                            offset = buffer.addDouble(offset, entity.getLongValue());
                            break;

                        case ColumnType.FLOAT:
                            offset = buffer.addFloat(offset, entity.getLongValue());
                            break;

                        case ColumnType.SYMBOL:
                            offset = buffer.addSymbol(offset, entity.getValue(), parser.hasNonAsciiChars(), localDetails.getSymbolLookup(columnWriterIndex));
                            break;

                        default:
                            throw castError("integer", columnWriterIndex, colType, entity.getName());
                    }
                    break;
                }
                case LineTcpParser.ENTITY_TYPE_FLOAT: {
                    switch (ColumnType.tagOf(colType)) {
                        case ColumnType.DOUBLE:
                            offset = buffer.addDouble(offset, entity.getFloatValue());
                            break;

                        case ColumnType.FLOAT:
                            offset = buffer.addFloat(offset, (float) entity.getFloatValue());
                            break;

                        case ColumnType.SYMBOL:
                            offset = buffer.addSymbol(offset, entity.getValue(), parser.hasNonAsciiChars(), localDetails.getSymbolLookup(columnWriterIndex));
                            break;

                        default:
                            throw castError("float", columnWriterIndex, colType, entity.getName());
                    }
                    break;
                }
                case LineTcpParser.ENTITY_TYPE_STRING: {
                    final int colTypeMeta = localDetails.getColumnTypeMeta(columnWriterIndex);
                    final DirectByteCharSequence entityValue = entity.getValue();
                    if (colTypeMeta == 0) { // not geohash
                        switch (ColumnType.tagOf(colType)) {
                            case ColumnType.STRING:
                                offset = buffer.addString(offset, entityValue, parser.hasNonAsciiChars());
                                break;

                            case ColumnType.CHAR:
                                if (stringToCharCastAllowed || entityValue.length() == 1) {
                                    offset = buffer.addChar(offset, entityValue.charAt(0));
                                } else {
                                    throw castError("string", columnWriterIndex, colType, entity.getName());
                                }
                                break;

                            case ColumnType.SYMBOL:
                                offset = buffer.addSymbol(offset, entityValue, parser.hasNonAsciiChars(), localDetails.getSymbolLookup(columnWriterIndex));
                                break;

                            default:
                                throw castError("string", columnWriterIndex, colType, entity.getName());
                        }
                    } else {
                        offset = buffer.addGeoHash(offset, entityValue, colTypeMeta);
                    }
                    break;
                }
                case LineTcpParser.ENTITY_TYPE_LONG256: {
                    switch (ColumnType.tagOf(colType)) {
                        case ColumnType.LONG256:
                            offset = buffer.addLong256(offset, entity.getValue(), parser.hasNonAsciiChars());
                            break;

                        case ColumnType.SYMBOL:
                            offset = buffer.addSymbol(offset, entity.getValue(), parser.hasNonAsciiChars(), localDetails.getSymbolLookup(columnWriterIndex));
                            break;

                        default:
                            throw castError("long256", columnWriterIndex, colType, entity.getName());
                    }
                    break;
                }
                case LineTcpParser.ENTITY_TYPE_BOOLEAN: {
                    byte entityValue = (byte) (entity.getBooleanValue() ? 1 : 0);
                    switch (ColumnType.tagOf(colType)) {
                        case ColumnType.BOOLEAN:
                            offset = buffer.addBoolean(offset, entityValue);
                            break;

                        case ColumnType.BYTE:
                            offset = buffer.addByte(offset, entityValue);
                            break;

                        case ColumnType.SHORT:
                            offset = buffer.addShort(offset, entityValue);
                            break;

                        case ColumnType.INT:
                            offset = buffer.addInt(offset, entityValue);
                            break;

                        case ColumnType.LONG:
                            offset = buffer.addLong(offset, entityValue);
                            break;

                        case ColumnType.FLOAT:
                            offset = buffer.addFloat(offset, entityValue);
                            break;

                        case ColumnType.DOUBLE:
                            offset = buffer.addDouble(offset, entityValue);
                            break;

                        case ColumnType.SYMBOL:
                            offset = buffer.addSymbol(offset, entity.getValue(), parser.hasNonAsciiChars(), localDetails.getSymbolLookup(columnWriterIndex));
                            break;

                        default:
                            throw castError("boolean", columnWriterIndex, colType, entity.getName());
                    }
                    break;
                }
                case LineTcpParser.ENTITY_TYPE_TIMESTAMP: {
                    switch (ColumnType.tagOf(colType)) {
                        case ColumnType.TIMESTAMP:
                            offset = buffer.addTimestamp(offset, entity.getLongValue());
                            break;

                        case ColumnType.SYMBOL:
                            offset = buffer.addSymbol(offset, entity.getValue(), parser.hasNonAsciiChars(), localDetails.getSymbolLookup(columnWriterIndex));
                            break;

                        default:
                            throw castError("timestamp", columnWriterIndex, colType, entity.getName());
                    }
                    break;
                }
                case LineTcpParser.ENTITY_TYPE_SYMBOL: {
                    if (ColumnType.tagOf(colType) == ColumnType.SYMBOL) {
                        offset = buffer.addSymbol(offset, entity.getValue(), parser.hasNonAsciiChars(), localDetails.getSymbolLookup(columnWriterIndex));
                    } else {
                        throw castError("symbol", columnWriterIndex, colType, entity.getName());
                    }
                    break;
                }
                case ENTITY_TYPE_NULL:
                    offset = buffer.addNull(offset);
                    break;
                default:
                    // unsupported types are ignored
                    break;
            }
        }
        buffer.addDesignatedTimestamp(buffer.getAddress() + Long.BYTES, timestamp);
        buffer.addNumOfColumns(buffer.getAddress() + 2 * Long.BYTES, entitiesWritten);
        writerWorkerId = tud.getWriterThreadId();
    }

    private boolean dispatchEvent(NetworkIOJob netIoJob, LineTcpParser parser, TableUpdateDetails tud) {
        final int writerThreadId = tud.getWriterThreadId();
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
        return true;
    }

    @TestOnly
    void setListener(LineTcpReceiver.SchedulerListener listener) {
        this.listener = listener;
    }
}
