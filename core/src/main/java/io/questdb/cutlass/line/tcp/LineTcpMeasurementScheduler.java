/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.*;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cutlass.line.LineTcpTimestampAdapter;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.WorkerPool;
import io.questdb.network.IODispatcher;
import io.questdb.std.*;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.*;
import io.questdb.tasks.TelemetryTask;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.util.Arrays;
import java.util.concurrent.locks.ReadWriteLock;

import static io.questdb.cutlass.line.tcp.LineTcpMeasurementEvent.*;
import static io.questdb.cutlass.line.tcp.TableUpdateDetails.ThreadLocalDetails.COLUMN_NOT_FOUND;
import static io.questdb.cutlass.line.tcp.TableUpdateDetails.ThreadLocalDetails.DUPLICATED_COLUMN;

public class LineTcpMeasurementScheduler implements Closeable {
    private static final Log LOG = LogFactory.getLog(LineTcpMeasurementScheduler.class);
    private final ObjList<TableUpdateDetails>[] assignedTables;
    private final boolean autoCreateNewColumns;
    private final boolean autoCreateNewTables;
    private final CairoConfiguration cairoConfiguration;
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
    private final StringSink[] tableNameSinks;
    private final TableStructureAdapter tableStructureAdapter;
    private final ReadWriteLock tableUpdateDetailsLock = new SimpleReadWriteLock();
    private final LowerCaseCharSequenceObjHashMap<TableUpdateDetails> tableUpdateDetailsUtf16;
    private final Telemetry<TelemetryTask> telemetry;
    private final long writerIdleTimeout;

    public LineTcpMeasurementScheduler(
            LineTcpReceiverConfiguration lineConfiguration,
            CairoEngine engine,
            WorkerPool ioWorkerPool,
            IODispatcher<LineTcpConnectionContext> dispatcher,
            WorkerPool writerWorkerPool
    ) {
        this.engine = engine;
        this.telemetry = engine.getTelemetry();
        this.cairoConfiguration = engine.getConfiguration();
        this.configuration = lineConfiguration;
        MillisecondClock milliClock = cairoConfiguration.getMillisecondClock();
        this.defaultColumnTypes = new DefaultColumnTypes(lineConfiguration);
        final int ioWorkerPoolSize = ioWorkerPool.getWorkerCount();
        this.netIoJobs = new NetworkIOJob[ioWorkerPoolSize];
        this.tableNameSinks = new StringSink[ioWorkerPoolSize];
        for (int i = 0; i < ioWorkerPoolSize; i++) {
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
        long commitInterval = configuration.getCommitInterval();
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
                    commitInterval, this, engine.getMetrics(), assignedTables[i]
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
                    .$("[tableName=").$(tub.getTableNameUtf16())
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
                    .$("`]")
                    .$();
            return true;
        } catch (CairoException ex) {
            // Table could not be created
            LOG.error().$("could not create table [tableName=").$(measurementName)
                    .$(", errno=").$(ex.getErrno())
                    .$(", ex=`")
                    .$(ex.getFlyweightMessage())
                    .$("`]")
                    .I$();
            // More details will be logged by catching thread
            throw ex;
        }

        if (tud.isWal()) {
            try {
                while (true) {
                    try {
                        appendToWal(securityContext, netIoJob, parser, tud);
                        break;
                    } catch (MetadataChangedException e) {
                        // do another retry, metadata has changed while processing the line
                        // and all the resolved column indexes have been invalidated
                    }
                }
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
        LOG.critical().$("closing writer because of error [table=").$(tud.getTableNameUtf16())
                .$(",ex=")
                .$(ex)
                .I$();
        throw CairoException.critical(0).put("could not append to WAL [tableName=").put(measurementName).put(", error=").put(ex.getMessage()).put(']');
    }

    private void appendToWal(
            SecurityContext securityContext,
            NetworkIOJob netIoJob,
            LineTcpParser parser,
            TableUpdateDetails tud
    ) throws CommitFailedException, MetadataChangedException {
        final boolean stringToCharCastAllowed = configuration.isStringToCharCastAllowed();
        final LineTcpTimestampAdapter timestampAdapter = configuration.getTimestampAdapter();
        // pass 1: create all columns that do not exist
        final TableUpdateDetails.ThreadLocalDetails ld = tud.getThreadLocalDetails(netIoJob.getWorkerId());
        ld.resetStateIfNecessary();
        ld.clearColumnTypes();

        final TableWriterAPI writer = tud.getWriter();
        assert writer.supportsMultipleWriters();
        TableRecordMetadata metadata = writer.getMetadata();
        long initialMetadataVersion = ld.getMetadataVersion();

        long timestamp = parser.getTimestamp();
        if (timestamp != LineTcpParser.NULL_TIMESTAMP) {
            timestamp = timestampAdapter.getMicros(timestamp, parser.getTimestampUnit());
        } else {
            timestamp = configuration.getMicrosecondClock().getTicks();
        }

        final int entCount = parser.getEntityCount();
        for (int i = 0; i < entCount; i++) {
            final LineTcpParser.ProtoEntity ent = parser.getEntity(i);
            int columnWriterIndex = ld.getColumnWriterIndex(ent.getName(), parser.hasNonAsciiChars(), metadata);

            switch (columnWriterIndex) {
                default:
                    final int columnType = metadata.getColumnType(columnWriterIndex);
                    if (columnType > -1) {
                        if (columnWriterIndex == tud.getTimestampIndex()) {
                            timestamp = timestampAdapter.getMicros(ent.getLongValue(), ent.getUnit());
                            ld.addColumnType(DUPLICATED_COLUMN, ColumnType.UNDEFINED);
                        } else {
                            ld.addColumnType(columnWriterIndex, metadata.getColumnType(columnWriterIndex));
                        }
                        break;
                    } else {
                        // column has been deleted from the metadata, but it is in our utf8 cache
                        ld.removeFromCaches(ent.getName(), parser.hasNonAsciiChars());
                        // act as if we did not find this column and fall through
                    }
                case COLUMN_NOT_FOUND:
                    final String columnNameUtf16 = ld.getColNameUtf16();
                    if (autoCreateNewColumns && TableUtils.isValidColumnName(columnNameUtf16, cairoConfiguration.getMaxFileNameLength())) {
                        columnWriterIndex = metadata.getColumnIndexQuiet(columnNameUtf16);
                        if (columnWriterIndex < 0) {
                            securityContext.authorizeAlterTableAddColumn(writer.getTableToken());
                            tud.commit(false);
                            try {
                                writer.addColumn(columnNameUtf16, ld.getColumnType(ld.getColNameUtf8(), ent.getType()), securityContext);
                                columnWriterIndex = metadata.getColumnIndexQuiet(columnNameUtf16);
                            } catch (CairoException e) {
                                columnWriterIndex = metadata.getColumnIndexQuiet(columnNameUtf16);
                                if (columnWriterIndex < 0) {
                                    // the column is still not there, something must be wrong
                                    throw e;
                                }
                                // all good, someone added the column concurrently
                            }
                        }
                        if (ld.getMetadataVersion() != initialMetadataVersion) {
                            // Restart the whole line,
                            // some columns can be deleted or renamed in tud.commit and ww.addColumn calls
                            throw MetadataChangedException.INSTANCE;
                        }
                        ld.addColumnType(columnWriterIndex, metadata.getColumnType(columnWriterIndex));
                    } else if (!autoCreateNewColumns) {
                        throw newColumnsNotAllowed(tud, columnNameUtf16);
                    } else {
                        throw invalidColNameError(tud, columnNameUtf16);
                    }
                    break;
                case DUPLICATED_COLUMN:
                    // indicate to the second loop that writer index does not exist
                    ld.addColumnType(DUPLICATED_COLUMN, ColumnType.UNDEFINED);
                    break;
            }
        }

        TableWriter.Row r = writer.newRow(timestamp);
        try {
            for (int i = 0; i < entCount; i++) {
                int colTypeAndIndex = ld.getColumnType(i);
                int colType = Numbers.decodeLowShort(colTypeAndIndex);
                int columnIndex = Numbers.decodeHighShort(colTypeAndIndex);

                if (columnIndex < 0) {
                    continue;
                }

                final LineTcpParser.ProtoEntity ent = parser.getEntity(i);
                switch (ent.getType()) {
                    case LineTcpParser.ENTITY_TYPE_TAG: {
                        if (ColumnType.tagOf(colType) == ColumnType.SYMBOL) {
                            r.putSymUtf8(columnIndex, ent.getValue(), parser.hasNonAsciiChars());
                        } else {
                            throw castError("tag", i, colType, ent.getName());
                        }
                        break;
                    }
                    case LineTcpParser.ENTITY_TYPE_INTEGER: {
                        switch (ColumnType.tagOf(colType)) {
                            case ColumnType.LONG:
                                r.putLong(columnIndex, ent.getLongValue());
                                break;
                            case ColumnType.INT: {
                                final long entityValue = ent.getLongValue();
                                if (entityValue >= Integer.MIN_VALUE && entityValue <= Integer.MAX_VALUE) {
                                    r.putInt(columnIndex, (int) entityValue);
                                } else if (entityValue == Numbers.LONG_NaN) {
                                    r.putInt(columnIndex, Numbers.INT_NaN);
                                } else {
                                    throw boundsError(entityValue, i, ColumnType.INT);
                                }
                                break;
                            }
                            case ColumnType.SHORT: {
                                final long entityValue = ent.getLongValue();
                                if (entityValue >= Short.MIN_VALUE && entityValue <= Short.MAX_VALUE) {
                                    r.putShort(columnIndex, (short) entityValue);
                                } else if (entityValue == Numbers.LONG_NaN) {
                                    r.putShort(columnIndex, (short) 0);
                                } else {
                                    throw boundsError(entityValue, i, ColumnType.SHORT);
                                }
                                break;
                            }
                            case ColumnType.BYTE: {
                                final long entityValue = ent.getLongValue();
                                if (entityValue >= Byte.MIN_VALUE && entityValue <= Byte.MAX_VALUE) {
                                    r.putByte(columnIndex, (byte) entityValue);
                                } else if (entityValue == Numbers.LONG_NaN) {
                                    r.putByte(columnIndex, (byte) 0);
                                } else {
                                    throw boundsError(entityValue, i, ColumnType.BYTE);
                                }
                                break;
                            }
                            case ColumnType.TIMESTAMP:
                                r.putTimestamp(columnIndex, ent.getLongValue());
                                break;
                            case ColumnType.DATE:
                                r.putDate(columnIndex, ent.getLongValue());
                                break;
                            case ColumnType.DOUBLE:
                                r.putDouble(columnIndex, ent.getLongValue());
                                break;
                            case ColumnType.FLOAT:
                                r.putFloat(columnIndex, ent.getLongValue());
                                break;
                            case ColumnType.SYMBOL:
                                r.putSymUtf8(columnIndex, ent.getValue(), parser.hasNonAsciiChars());
                                break;
                            default:
                                throw castError("integer", i, colType, ent.getName());
                        }
                        break;
                    }
                    case LineTcpParser.ENTITY_TYPE_FLOAT: {
                        switch (ColumnType.tagOf(colType)) {
                            case ColumnType.DOUBLE:
                                r.putDouble(columnIndex, ent.getFloatValue());
                                break;
                            case ColumnType.FLOAT:
                                r.putFloat(columnIndex, (float) ent.getFloatValue());
                                break;
                            case ColumnType.SYMBOL:
                                r.putSymUtf8(columnIndex, ent.getValue(), parser.hasNonAsciiChars());
                                break;
                            default:
                                throw castError("float", i, colType, ent.getName());
                        }
                        break;
                    }
                    case LineTcpParser.ENTITY_TYPE_STRING: {
                        final int geoHashBits = ColumnType.getGeoHashBits(colType);
                        final DirectUtf8Sequence entityValue = ent.getValue();
                        if (geoHashBits == 0) { // not geohash
                            switch (ColumnType.tagOf(colType)) {
                                case ColumnType.IPv4:
                                    try {
                                        int value = Numbers.parseIPv4Nl(entityValue);
                                        r.putInt(columnIndex, value);
                                    } catch (NumericException e) {
                                        throw castError("string", i, colType, ent.getName());
                                    }
                                    break;
                                case ColumnType.STRING:
                                    r.putStrUtf8(columnIndex, entityValue, parser.hasNonAsciiChars());
                                    break;
                                case ColumnType.CHAR:
                                    if (entityValue.size() == 1 && entityValue.byteAt(0) > -1) {
                                        r.putChar(columnIndex, (char) entityValue.byteAt(0));
                                    } else if (stringToCharCastAllowed) {
                                        int encodedResult = Utf8s.utf8CharDecode(entityValue.lo(), entityValue.hi());
                                        if (Numbers.decodeLowShort(encodedResult) > 0) {
                                            r.putChar(columnIndex, (char) Numbers.decodeHighShort(encodedResult));
                                        } else {
                                            throw castError("string", i, colType, ent.getName());
                                        }
                                    } else {
                                        throw castError("string", i, colType, ent.getName());
                                    }
                                    break;
                                case ColumnType.SYMBOL:
                                    r.putSymUtf8(columnIndex, entityValue, parser.hasNonAsciiChars());
                                    break;
                                case ColumnType.UUID:
                                    r.putUuidUtf8(columnIndex, entityValue);
                                    break;
                                default:
                                    throw castError("string", i, colType, ent.getName());
                            }
                        } else {
                            long geoHash;
                            try {
                                DirectUtf8Sequence value = ent.getValue();
                                geoHash = GeoHashes.fromStringTruncatingNl(value.lo(), value.hi(), geoHashBits);
                            } catch (NumericException e) {
                                geoHash = GeoHashes.NULL;
                            }
                            r.putGeoHash(columnIndex, geoHash);
                        }
                        break;
                    }
                    case LineTcpParser.ENTITY_TYPE_LONG256: {
                        switch (ColumnType.tagOf(colType)) {
                            case ColumnType.LONG256:
                                r.putLong256Utf8(columnIndex, ent.getValue());
                                break;
                            case ColumnType.SYMBOL:
                                r.putSymUtf8(columnIndex, ent.getValue(), parser.hasNonAsciiChars());
                                break;
                            default:
                                throw castError("long256", i, colType, ent.getName());
                        }
                        break;
                    }
                    case LineTcpParser.ENTITY_TYPE_BOOLEAN: {
                        switch (ColumnType.tagOf(colType)) {
                            case ColumnType.BOOLEAN:
                                r.putBool(columnIndex, ent.getBooleanValue());
                                break;
                            case ColumnType.BYTE:
                                r.putByte(columnIndex, (byte) (ent.getBooleanValue() ? 1 : 0));
                                break;
                            case ColumnType.SHORT:
                                r.putShort(columnIndex, (short) (ent.getBooleanValue() ? 1 : 0));
                                break;
                            case ColumnType.INT:
                                r.putInt(columnIndex, ent.getBooleanValue() ? 1 : 0);
                                break;
                            case ColumnType.LONG:
                                r.putLong(columnIndex, ent.getBooleanValue() ? 1 : 0);
                                break;
                            case ColumnType.FLOAT:
                                r.putFloat(columnIndex, ent.getBooleanValue() ? 1 : 0);
                                break;
                            case ColumnType.DOUBLE:
                                r.putDouble(columnIndex, ent.getBooleanValue() ? 1 : 0);
                                break;
                            case ColumnType.SYMBOL:
                                r.putSymUtf8(columnIndex, ent.getValue(), parser.hasNonAsciiChars());
                                break;
                            default:
                                throw castError("boolean", i, colType, ent.getName());
                        }
                        break;
                    }
                    case LineTcpParser.ENTITY_TYPE_TIMESTAMP: {
                        switch (ColumnType.tagOf(colType)) {
                            case ColumnType.TIMESTAMP:
                                long timestampValue = LineTcpTimestampAdapter.TS_COLUMN_INSTANCE.getMicros(ent.getLongValue(), ent.getUnit());
                                r.putTimestamp(columnIndex, timestampValue);
                                break;
                            case ColumnType.DATE:
                                long dateValue = LineTcpTimestampAdapter.TS_COLUMN_INSTANCE.getMicros(ent.getLongValue(), ent.getUnit());
                                r.putTimestamp(columnIndex, dateValue / 1000);
                                break;
                            case ColumnType.SYMBOL:
                                r.putSymUtf8(columnIndex, ent.getValue(), parser.hasNonAsciiChars());
                                break;
                            default:
                                throw castError("timestamp", i, colType, ent.getName());
                        }
                        break;
                    }
                    // parser would reject this condition based on config
                    case LineTcpParser.ENTITY_TYPE_SYMBOL: {
                        if (ColumnType.tagOf(colType) == ColumnType.SYMBOL) {
                            r.putSymUtf8(columnIndex, ent.getValue(), parser.hasNonAsciiChars());
                        } else {
                            throw castError("symbol", i, colType, ent.getName());
                        }
                        break;
                    }
                    default:
                        break; // unsupported types are ignored
                }
            }
            r.append();
            tud.commitIfMaxUncommittedRowsCountReached();
        } catch (CommitFailedException commitFailedException) {
            throw commitFailedException;
        } catch (CairoException th) {
            LOG.error().$("could not write line protocol measurement [tableName=").$(tud.getTableNameUtf16()).$(", message=").$(th.getFlyweightMessage()).I$();
            if (r != null) {
                r.cancel();
            }
            throw th;
        } catch (Throwable th) {
            LOG.error().$("could not write line protocol measurement [tableName=").$(tud.getTableNameUtf16()).$(", message=").$(th.getMessage()).$(th).I$();
            if (r != null) {
                r.cancel();
            }
            throw th;
        }
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
            TableUpdateDetails tud;
            // check if the global cache has the table
            final int tudKeyIndex = tableUpdateDetailsUtf16.keyIndex(tableNameUtf16);
            if (tudKeyIndex < 0) {
                // it does, which means that table is non-WAL
                // we should not have "shared" WAL tables
                tud = tableUpdateDetailsUtf16.valueAt(tudKeyIndex);
            } else {
                TableToken tableToken = engine.getTableTokenIfExists(tableNameUtf16);
                int status = engine.getTableStatus(path, tableToken);
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
                    tableToken = engine.createTable(securityContext, ddlMem, path, true, tsa, false);
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
                        tud = unsafeAssignTableToWriterThread(tudKeyIndex, tud.getTableNameUtf16(), tud.getTableNameUtf8());
                    } else {
                        idleTableUpdateDetailsUtf16.removeAt(idleTudKeyIndex);
                        tableUpdateDetailsUtf16.putAt(tudKeyIndex, tud.getTableNameUtf16(), tud);
                    }
                } else {
                    TelemetryTask.store(telemetry, TelemetryOrigin.ILP_TCP, TelemetrySystemEvent.ILP_RESERVE_WRITER);
                    // check if table on disk is WAL
                    path.of(engine.getConfiguration().getRoot());
                    if (engine.isWalTable(tableToken)) {
                        // create WAL-oriented TUD and DON'T add it to the global cache
                        tud = new TableUpdateDetails(
                                configuration,
                                engine,
                                securityContext,
                                engine.getWalWriter(tableToken),
                                -1,
                                netIoJobs,
                                defaultColumnTypes,
                                Utf8String.newInstance(tableNameUtf8)
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
        } finally {
            tableUpdateDetailsLock.writeLock().unlock();
        }
    }

    private boolean isOpen() {
        return null != pubSeq;
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
}
