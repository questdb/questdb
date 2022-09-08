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

package io.questdb.cairo.wal;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.cairo.vm.api.MemoryMAR;
import io.questdb.cairo.vm.api.NullMemory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.ops.AbstractOperation;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.Path;
import io.questdb.std.str.SingleCharCharSequence;
import org.jetbrains.annotations.NotNull;

import static io.questdb.cairo.TableUtils.*;
import static io.questdb.cairo.wal.WalUtils.WAL_NAME_BASE;

public class WalWriter implements TableWriterFrontend {
    private static final Log LOG = LogFactory.getLog(WalWriter.class);
    private static final Runnable NOOP = () -> {
    };
    private static final int MEM_TAG = MemoryTag.MMAP_TABLE_WAL_WRITER;
    private static final long COLUMN_DELETED_NULL_FLAG = Long.MAX_VALUE;

    private final ObjList<MemoryMA> columns;
    private final ObjList<SymbolMapReader> symbolMapReaders;
    private final IntList initialSymbolCounts = new IntList();
    private final ObjList<CharSequenceIntHashMap> symbolMaps = new ObjList<>();
    private final MillisecondClock millisecondClock;
    private final Path path;
    private final LongList rowValueIsNotNull = new LongList();
    private final int rootLen;
    private final FilesFacade ff;
    private final MemoryMAR symbolMapMem = Vm.getMARInstance();
    private final int mkDirMode;
    private final String tableName;
    private final String walName;
    private final int walId;
    private final SequencerMetadata metadata;
    private final WalWriterEvents events;
    private final TableRegistry tableRegistry; //todo: rename to something more appropriate
    private final CairoConfiguration configuration;
    private final ObjList<Runnable> nullSetters;
    private final RowImpl row = new RowImpl();
    private final TableWriterBackend walMetadataUpdater = new WalMetadataUpdaterBackend();
    private final TableWriterBackend alterOperationValidationBackend = new AlterOperationValidationBackend();
    private long lockFd = -1;
    private int columnCount;
    private long currentTxnStartRowNum = -1;
    private long rowCount = -1;
    private long segmentId = -1;
    private long segmentStartMillis;
    private boolean txnOutOfOrder = false;
    private long txnMinTimestamp = Long.MAX_VALUE;
    private long txnMaxTimestamp = -1;
    private boolean rollSegmentOnNextRow = false;
    private WalWriterRollStrategy rollStrategy = new WalWriterRollStrategy() {
    };
    private long lastSegmentTxn = -1L;
    private SequencerStructureChangeCursor structureChangeCursor;
    private boolean open;
    private boolean distressed;

    public WalWriter(String tableName, TableRegistry tableRegistry, CairoConfiguration configuration) {
        LOG.info().$("open '").utf8(tableName).$('\'').$();
        this.tableRegistry = tableRegistry;
        this.configuration = configuration;
        this.millisecondClock = this.configuration.getMillisecondClock();
        this.mkDirMode = this.configuration.getMkDirMode();
        this.ff = this.configuration.getFilesFacade();
        this.tableName = tableName;
        final int walId = tableRegistry.getNextWalId(tableName);
        this.walName = WAL_NAME_BASE + walId;
        this.walId = walId;
        this.path = new Path().of(this.configuration.getRoot()).concat(tableName).concat(walName);
        this.rootLen = path.length();
        this.open = true;

        try {
            lock();

            metadata = new SequencerMetadata(ff, SequencerMetadata.READ_WRITE);
            tableRegistry.copyMetadataTo(tableName, metadata);

            columnCount = metadata.getColumnCount();
            columns = new ObjList<>(columnCount * 2);
            nullSetters = new ObjList<>(columnCount);

            symbolMapReaders = new ObjList<>();
            events = new WalWriterEvents(ff);
            events.of(symbolMaps, initialSymbolCounts);

            configureColumns();
            openNewSegment();
            configureSymbolTable();
        } catch (Throwable e) {
            doClose(false);
            throw e;
        }
    }

    @Override
    public long applyAlter(AlterOperation operation, boolean contextAllowsAnyStructureChanges) throws AlterTableContextException {
        if (inTransaction()) {
            throw CairoException.critical(0).put("cannot alter table with uncommitted inserts [table=").put(tableName).put(']');
        }
        if (operation.isMetadataChange()) {
            long txn;
            do {
                try {
                    operation.apply(alterOperationValidationBackend, true);
                } catch (SqlException e) {
                    // Table schema (metadata) changed and this alter is not valid anymore
                    throw CairoException.critical(0).put(e.getFlyweightMessage());
                }

                txn = tableRegistry.nextStructureTxn(tableName, getStructureVersion(), operation);
                if (txn == Sequencer.NO_TXN) {
                    applyStructureChanges();
                }
            } while (txn == Sequencer.NO_TXN);

            // Apply to itself.
            try {
                operation.apply(walMetadataUpdater, true);
            } catch (Throwable th) {
                // Transaction successful, but writing using this wal writer should not be possible
                LOG.error().$("Exception during alter").$(th).$();
                distressed = true;
            }
            return txn;
        } else {
            return applyNonStructuralOperation(operation);
        }
    }

    // Returns table transaction number
    @Override
    public long applyUpdate(UpdateOperation operation) {
        if (operation.getFactory().recordCursorSupportsRandomAccess() && operation.getFactory().supportsUpdateRowId(tableName)) {
            // no join in UPDATE statement
            return applyNonStructuralOperation(operation);
        }

        // here we have 2 options instead of throwing exception
        // 1. we could write the updated partitions into WAL.
        //   since we cannot really rely on row ids we should probably create
        //   a PARTITION_REWRITE event and use it here to replace the updated
        //   partitions entirely with new ones.
        // 2. we could still pass the SQL statement if we made sure that all
        //   tables involved in the join are guaranteed to be on the same
        //   version (exact same txn number) on each node when the update
        //   statement is run.
        //   so we would need to read current txn number for each table and
        //   put it into the SQL event as a requirement for running the SQL.
        //   when the WAL event is processed we would need to query the exact
        //   versions of each table involved in the join when running the SQL.
        operation.close();
        throw new UnsupportedOperationException("UPDATE statements with join are not supported yet for WAL tables");
    }

    @Override
    public void close() {
        if (isOpen()) {
            rollback();
            doClose(true);
        }
    }

    // Returns table transaction number.
    public long commit() {
        checkDistressed();
        try {
            if (inTransaction()) {
                LOG.debug().$("committing data block [wal=").$(path).$(Files.SEPARATOR).$(segmentId).$(", rowLo=").$(currentTxnStartRowNum).$(", roHi=").$(rowCount).I$();
                lastSegmentTxn = events.data(currentTxnStartRowNum, rowCount, txnMinTimestamp, txnMaxTimestamp, txnOutOfOrder);
                final long tableTxn = getTableTxn();
                resetDataTxnProperties();
                return tableTxn;
            }
        } catch (Throwable th) {
            rollback();
            throw th;
        }
        return Sequencer.NO_TXN;
    }

    @Override
    public long commitWithLag(long commitLag) {
        return commit();
    }

    @Override
    public BaseRecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public long getStructureVersion() {
        return metadata.getStructureVersion();
    }

    public CharSequence getTableName() {
        return tableName;
    }

    public TableWriter.Row newRow() {
        return newRow(0L);
    }

    public TableWriter.Row newRow(long timestamp) {
        checkDistressed();
        try {
            if (rollSegmentOnNextRow) {
                rollSegment();
                rollSegmentOnNextRow = false;
            }

            final int timestampIndex = metadata.getTimestampIndex();
            if (timestampIndex != -1) {
                //avoid lookups by having a designated field with primaryColumn
                final MemoryMA primaryColumn = getPrimaryColumn(timestampIndex);
                primaryColumn.putLongLong(timestamp, rowCount);
                setRowValueNotNull(timestampIndex);
                row.timestamp = timestamp;
            }
            return row;
        } catch (Throwable e) {
            throw new CairoError(e);
        }
    }

    @Override
    public void rollback() {
        if (inTransaction() || hasDirtyColumns(currentTxnStartRowNum)) {
            setAppendPosition(currentTxnStartRowNum);
        }
    }

    private boolean hasDirtyColumns(long currentTxnStartRowNum) {
        for(int i = 0; i < columnCount; i++) {
            long writtenCount = rowValueIsNotNull.getQuick(i);
            if (writtenCount >= currentTxnStartRowNum && writtenCount != COLUMN_DELETED_NULL_FLAG) {
                return true;
            }
        }
        return false;
    }

    public void doClose(boolean truncate) {
        open = false;
        Misc.free(metadata);
        Misc.free(events);
        freeSymbolMapWriters();
        Misc.free(symbolMapMem);
        freeColumns(truncate);

        try {
            releaseLock(!truncate);
        } finally {
            Misc.free(path);
            LOG.info().$("closed '").utf8(tableName).$('\'').$();
        }
    }

    public long getSegment() {
        return segmentId;
    }

    public long getTransientRowCount() {
        return rowCount - currentTxnStartRowNum;
    }

    public int getWalId() {
        return walId;
    }

    public String getWalName() {
        return walName;
    }

    public boolean goActive() {
        try {
            applyStructureChanges();
            return true;
        } catch (CairoException e) {
            LOG.critical().$("could not apply structure changes, wal will be closed [table=").$(tableName)
                    .$(", walId=").$(walId)
                    .$(", errno=").$(e.getErrno())
                    .$(", error=").$((Throwable) e).I$();
            distressed = true;
            return false;
        }
    }

    public boolean inTransaction() {
        return rowCount > currentTxnStartRowNum;
    }

    public boolean isDistressed() {
        return distressed;
    }

    public boolean isOpen() {
        return this.open;
    }

    public long rollSegmentIfLimitReached() {
        long segmentSize = 0;
        if (rollStrategy.isMaxSegmentSizeSet()) {
            for (int i = 0; i < columnCount; i++) {
                segmentSize = updateSegmentSize(segmentSize, i);
            }
        }

        long segmentAge = 0;
        if (rollStrategy.isRollIntervalSet()) {
            segmentAge = millisecondClock.getTicks() - segmentStartMillis;
        }

        if (rollStrategy.shouldRoll(segmentSize, rowCount, segmentAge)) {
            commit();
            return rollSegment();
        }
        return 0L;
    }

    public void rollUncommittedToNewSegment() {
        long uncommittedRows = rowCount - currentTxnStartRowNum;
        long newSegmentId = segmentId + 1;

        path.trimTo(rootLen);


        if (uncommittedRows > 0) {
            createSegmentDir(newSegmentId);
            path.trimTo(rootLen);
            LongList newColumnFiles = new LongList();
            newColumnFiles.setPos(columnCount * 4);
            newColumnFiles.fill(0, columnCount * 4, -1);
            rowValueIsNotNull.fill(0, columnCount, -1);

            try {
                int timestampIndex = metadata.getTimestampIndex();
                LOG.debug().$("rolling uncommitted rows to new segment [wal=").$(Files.SEPARATOR).$(segmentId + 1)
                        .$(", rowCount=").$(uncommittedRows).I$();

                for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                    int columnType = metadata.getColumnType(columnIndex);

                    if (columnType > 0) {
                        MemoryMA primaryColumn = getPrimaryColumn(columnIndex);
                        MemoryMA secondaryColumn = getSecondaryColumn(columnIndex);
                        String columnName = metadata.getColumnName(columnIndex);

                        CopySegmentFileJob.rollColumnToSegment(ff,
                                configuration.getWriterFileOpenOpts(),
                                primaryColumn,
                                secondaryColumn,
                                path,
                                newSegmentId,
                                columnName,
                                columnIndex == timestampIndex ? -columnType : columnType,
                                currentTxnStartRowNum,
                                uncommittedRows,
                                newColumnFiles,
                                columnIndex
                        );
                    } else {
                        rowValueIsNotNull.setQuick(columnIndex, COLUMN_DELETED_NULL_FLAG);
                    }
                }
            } catch (Throwable e) {
                closeSegmentSwitchFiles(newColumnFiles);
                throw e;
            }
            switchColumnsToNewSegment(newColumnFiles);
            rollLastWalEventRecord(newSegmentId, uncommittedRows);
            segmentId = newSegmentId;
            rowCount = uncommittedRows;
            currentTxnStartRowNum = 0;
        } else if (rowCount > 0 && uncommittedRows == 0) {
            rollSegmentOnNextRow = true;
        }
    }

    public void setRollStrategy(WalWriterRollStrategy rollStrategy) {
        this.rollStrategy = rollStrategy;
    }

    public long size() {
        return rowCount;
    }

    @Override
    public String toString() {
        return "WalWriter{" +
                "name=" + walName +
                ", table=" + tableName +
                '}';
    }

    private static int getPrimaryColumnIndex(int index) {
        return index * 2;
    }

    private static int getSecondaryColumnIndex(int index) {
        return getPrimaryColumnIndex(index) + 1;
    }

    private static void configureNullSetters(ObjList<Runnable> nullers, int type, MemoryA mem1, MemoryA mem2) {
        switch (ColumnType.tagOf(type)) {
            case ColumnType.BOOLEAN:
            case ColumnType.BYTE:
                nullers.add(() -> mem1.putByte((byte) 0));
                break;
            case ColumnType.DOUBLE:
                nullers.add(() -> mem1.putDouble(Double.NaN));
                break;
            case ColumnType.FLOAT:
                nullers.add(() -> mem1.putFloat(Float.NaN));
                break;
            case ColumnType.INT:
                nullers.add(() -> mem1.putInt(Numbers.INT_NaN));
                break;
            case ColumnType.LONG:
            case ColumnType.DATE:
            case ColumnType.TIMESTAMP:
                nullers.add(() -> mem1.putLong(Numbers.LONG_NaN));
                break;
            case ColumnType.LONG256:
                nullers.add(() -> mem1.putLong256(Numbers.LONG_NaN, Numbers.LONG_NaN, Numbers.LONG_NaN, Numbers.LONG_NaN));
                break;
            case ColumnType.SHORT:
                nullers.add(() -> mem1.putShort((short) 0));
                break;
            case ColumnType.CHAR:
                nullers.add(() -> mem1.putChar((char) 0));
                break;
            case ColumnType.STRING:
                nullers.add(() -> mem2.putLong(mem1.putNullStr()));
                break;
            case ColumnType.SYMBOL:
                nullers.add(() -> mem1.putInt(SymbolTable.VALUE_IS_NULL));
                break;
            case ColumnType.BINARY:
                nullers.add(() -> mem2.putLong(mem1.putNullBin()));
                break;
            case ColumnType.GEOBYTE:
                nullers.add(() -> mem1.putByte(GeoHashes.BYTE_NULL));
                break;
            case ColumnType.GEOSHORT:
                nullers.add(() -> mem1.putShort(GeoHashes.SHORT_NULL));
                break;
            case ColumnType.GEOINT:
                nullers.add(() -> mem1.putInt(GeoHashes.INT_NULL));
                break;
            case ColumnType.GEOLONG:
                nullers.add(() -> mem1.putLong(GeoHashes.NULL));
                break;
            default:
                throw new UnsupportedOperationException("unsupported column type: " + ColumnType.nameOf(type));
        }
    }

    private long applyNonStructuralOperation(AbstractOperation operation) {
        try {
            lastSegmentTxn = events.sql(operation.getCommandType(), operation.getSqlStatement());
            return getTableTxn();
        } catch (Throwable th) {
            rollback();
            throw th;
        }
    }

    private void applyStructureChanges() {
        try {
            structureChangeCursor = tableRegistry.getStructureChangeCursor(tableName, structureChangeCursor, getStructureVersion());
            if (structureChangeCursor == null) {
                // nothing to do
                return;
            }
            while (structureChangeCursor.hasNext()) {
                AlterOperation alterOperation = structureChangeCursor.next();
                long metadataVersion = getStructureVersion();
                try {
                    alterOperation.apply(walMetadataUpdater, true);
                } catch (SqlException e) {
                    distressed = true;
                    throw CairoException.critical(0).put("could not apply table definition changes to the current transaction. ").put(e.getFlyweightMessage());
                }
                if (metadataVersion >= getStructureVersion()) {
                    distressed = true;
                    throw CairoException.critical(0).put("could not apply table definition changes to the current transaction, version unchanged");
                }
            }
        } finally {
            if (structureChangeCursor != null) {
                structureChangeCursor.reset();
            }
        }
    }

    private void checkDistressed() {
        if (!distressed) {
            return;
        }
        throw CairoException.critical(0).
                put("wal writer is distressed and cannot be used any more [table=").put(tableName)
                .put(", wal=").put(walId).put(']');
    }

    private void closeCurrentSegment() {
        // TODO: mark segment as closed, commits should not be hidden to the client
        commit();
    }

    private void closeSegmentSwitchFiles(LongList newColumnFiles) {
        for (int fdIndex = 0; fdIndex < newColumnFiles.size(); fdIndex += 2) {
            long fd = newColumnFiles.get(fdIndex);
            if (fd > -1L) {
                ff.close(fd);
            }
        }
    }

    private void configureColumn(int index, int columnType) {
        final int baseIndex = getPrimaryColumnIndex(index);
        if (columnType > 0) {
            final MemoryMA primary = Vm.getMAInstance();
            final MemoryMA secondary = createSecondaryMem(columnType);
            columns.extendAndSet(baseIndex, primary);
            columns.extendAndSet(baseIndex + 1, secondary);
            configureNullSetters(nullSetters, columnType, primary, secondary);
            rowValueIsNotNull.add(-1);
        } else {
            columns.extendAndSet(baseIndex, NullMemory.INSTANCE);
            columns.extendAndSet(baseIndex + 1, NullMemory.INSTANCE);
            nullSetters.add(NOOP);
            rowValueIsNotNull.add(COLUMN_DELETED_NULL_FLAG);
        }
    }

    private MemoryMA createSecondaryMem(int columnType) {
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.BINARY:
            case ColumnType.STRING:
                return Vm.getMAInstance();
            default:
                return null;
        }
    }

    private void configureColumns() {
        for (int i = 0; i < columnCount; i++) {
            configureColumn(i, metadata.getColumnType(i));
        }
    }

    private void configureSymbolMapWriter(int columnWriterIndex, CharSequence columnName, int symbolCount, long columnNameTxn) {
        if (symbolCount == 0) {
            symbolMapReaders.extendAndSet(columnWriterIndex, EmptySymbolMapReader.INSTANCE);
            initialSymbolCounts.extendAndSet(columnWriterIndex, 0);
            symbolMaps.extendAndSet(columnWriterIndex, new CharSequenceIntHashMap(8, 0.5, SymbolTable.VALUE_NOT_FOUND));
            return;
        }

        // Copy or hard link symbol map files.
        FilesFacade ff = configuration.getFilesFacade();
        Path tempPath = Path.PATH.get();
        tempPath.of(configuration.getRoot()).concat(tableName);
        int tempPathTripLen = tempPath.length();

        path.trimTo(rootLen);
        TableUtils.offsetFileName(tempPath, columnName, columnNameTxn);
        TableUtils.offsetFileName(path, columnName, COLUMN_NAME_TXN_NONE);
        if (-1 == ff.hardLink(tempPath.$(), path.$())) {
            throw CairoException.critical(ff.errno()).put("failed to link offset file [from=")
                    .put(tempPath)
                    .put(", to=")
                    .put(path)
                    .put(']');
        }

        tempPath.trimTo(tempPathTripLen);
        path.trimTo(rootLen);
        TableUtils.charFileName(tempPath, columnName, columnNameTxn);
        TableUtils.charFileName(path, columnName, COLUMN_NAME_TXN_NONE);
        if (-1 == ff.hardLink(tempPath.$(), path.$())) {
            throw CairoException.critical(ff.errno()).put("failed to link char file [from=")
                    .put(tempPath)
                    .put(", to=")
                    .put(path)
                    .put(']');
        }

        tempPath.trimTo(tempPathTripLen);
        path.trimTo(rootLen);
        BitmapIndexUtils.keyFileName(tempPath, columnName, columnNameTxn);
        BitmapIndexUtils.keyFileName(path, columnName, COLUMN_NAME_TXN_NONE);
        if (-1 == ff.hardLink(tempPath.$(), path.$())) {
            throw CairoException.critical(ff.errno()).put("failed to link key file [from=")
                    .put(tempPath)
                    .put(", to=")
                    .put(path)
                    .put(']');
        }

        tempPath.trimTo(tempPathTripLen);
        path.trimTo(rootLen);
        BitmapIndexUtils.valueFileName(tempPath, columnName, columnNameTxn);
        BitmapIndexUtils.valueFileName(path, columnName, COLUMN_NAME_TXN_NONE);
        if (-1 == ff.hardLink(tempPath.$(), path.$())) {
            throw CairoException.critical(ff.errno()).put("failed to link value file [from=")
                    .put(tempPath)
                    .put(", to=")
                    .put(path)
                    .put(']');
        }

        path.trimTo(rootLen);
        SymbolMapReader symbolMapReader = new SymbolMapReaderImpl(
                configuration,
                path,
                columnName,
                COLUMN_NAME_TXN_NONE,
                symbolCount
        );

        symbolMapReaders.extendAndSet(columnWriterIndex, symbolMapReader);
        symbolMaps.extendAndSet(columnWriterIndex, new CharSequenceIntHashMap(8, 0.5, SymbolTable.VALUE_NOT_FOUND));
        initialSymbolCounts.add(symbolCount);
    }

    private void configureSymbolTable() {
        // we should not need the reader here, this will not work in a distributed environment
        // what if the wal is created on a node where the table itself is not present?
        // maybe copying symbols from existing table is not the best or sequencer needs an API for it
        // but then sequencer will have to keep track of all symbol tables
        try (
                TxReader txReader = new TxReader(ff);
                ColumnVersionReader columnVersionReader = new ColumnVersionReader()
        ) {
            MillisecondClock milliClock = configuration.getMillisecondClock();
            long spinLockTimeout = configuration.getSpinLockTimeout();

            Path path = Path.PATH2.get();
            path.of(configuration.getRoot()).concat(tableName).concat(TXN_FILE_NAME).$();

            // Does not matter which PartitionBy, as long as it is partitioned
            // WAL tables must be partitioned
            txReader.ofRO(path, PartitionBy.DAY);
            path.of(configuration.getRoot()).concat(tableName).concat(TableUtils.COLUMN_VERSION_FILE_NAME).$();
            columnVersionReader.ofRO(ff, path);

            do {
                TableUtils.safeReadTxn(txReader, milliClock, spinLockTimeout);
                columnVersionReader.readSafe(milliClock, spinLockTimeout);
            } while (txReader.getColumnVersion() != columnVersionReader.getVersion());
            int denseSymbolIndex = 0;

            for (int i = 0; i < columnCount; i++) {
                int columnType = metadata.getColumnType(i);
                if (!ColumnType.isSymbol(columnType)) {
                    // maintain sparse list of symbol writers
                    symbolMapReaders.extendAndSet(i, null);
                    initialSymbolCounts.extendAndSet(i, -1);
                    symbolMaps.extendAndSet(i, null);
                } else {
                    int symbolValueCount = txReader.getSymbolValueCount(denseSymbolIndex);
                    long columnNameTxn = columnVersionReader.getDefaultColumnNameTxn(i);
                    configureSymbolMapWriter(i, metadata.getColumnName(i), symbolValueCount, columnNameTxn);
                }

                if (columnType == ColumnType.SYMBOL || columnType == -ColumnType.SYMBOL) {
                    denseSymbolIndex++;
                }
            }
        }
    }

    private int createSegmentDir(long segmentId) {
        path.trimTo(rootLen);
        path.slash().put(segmentId);
        final int segmentPathLen = path.length();
        if (ff.mkdirs(path.slash$(), mkDirMode) != 0) {
            throw CairoException.critical(ff.errno()).put("Cannot create WAL segment directory: ").put(path);
        }
        path.trimTo(segmentPathLen);
        return segmentPathLen;
    }

    private void freeAndRemoveColumnPair(ObjList<MemoryMA> columns, int pi, int si) {
        Misc.free(columns.getAndSetQuick(pi, NullMemory.INSTANCE));
        Misc.free(columns.getAndSetQuick(si, NullMemory.INSTANCE));
    }

    private void freeColumns(boolean truncate) {
        // null check is because this method could be called from the constructor
        if (columns != null) {
            for (int i = 0, n = columns.size(); i < n; i++) {
                final MemoryMA m = columns.getQuick(i);
                if (m != null) {
                    m.close(truncate);
                }
            }
        }
    }

    private static void freeNullSetter(ObjList<Runnable> nullSetters, int columnIndex) {
        nullSetters.setQuick(columnIndex, NOOP);
    }

    private void freeSymbolMapWriters() {
        Misc.freeObjList(symbolMapReaders);
    }

    private MemoryMA getPrimaryColumn(int column) {
        assert column < columnCount : "Column index is out of bounds: " + column + " >= " + columnCount;
        return columns.getQuick(getPrimaryColumnIndex(column));
    }

    private MemoryMA getSecondaryColumn(int column) {
        assert column < columnCount : "Column index is out of bounds: " + column + " >= " + columnCount;
        return columns.getQuick(getSecondaryColumnIndex(column));
    }

    SymbolMapReader getSymbolMapReader(int columnIndex) {
        return symbolMapReaders.getQuick(columnIndex);
    }

    private long getTableTxn() {
        long txn;
        do {
            txn = tableRegistry.nextTxn(tableName, walId, metadata.getStructureVersion(), segmentId, lastSegmentTxn);
            if (txn == Sequencer.NO_TXN) {
                applyStructureChanges();
            }
        } while (txn == Sequencer.NO_TXN);
        return txn;
    }

    private void lock() {
        try {
            lockName(path);
            lockFd = TableUtils.lock(ff, path);
        } finally {
            path.trimTo(rootLen);
        }

        if (lockFd == -1L) {
            throw CairoException.critical(ff.errno()).put("Cannot lock table: ").put(path.$());
        }
    }

    private void markColumnRemoved(int columnIndex) {
        final int pi = getPrimaryColumnIndex(columnIndex);
        final int si = getSecondaryColumnIndex(columnIndex);
        freeNullSetter(nullSetters, columnIndex);
        freeAndRemoveColumnPair(columns, pi, si);
        rowValueIsNotNull.setQuick(columnIndex, COLUMN_DELETED_NULL_FLAG);
    }

    private void openColumnFiles(CharSequence name, int columnIndex, int pathTrimToLen) {
        try {
            final MemoryMA mem1 = getPrimaryColumn(columnIndex);
            mem1.of(ff,
                    dFile(path.trimTo(pathTrimToLen), name),
                    configuration.getDataAppendPageSize(),
                    -1,
                    MemoryTag.MMAP_TABLE_WRITER,
                    configuration.getWriterFileOpenOpts()
            );

            final MemoryMA mem2 = getSecondaryColumn(columnIndex);
            if (mem2 != null) {
                mem2.of(ff,
                        iFile(path.trimTo(pathTrimToLen), name),
                        configuration.getDataAppendPageSize(),
                        -1,
                        MemoryTag.MMAP_TABLE_WRITER,
                        configuration.getWriterFileOpenOpts()
                );
                mem2.putLong(0L);
            }
        } finally {
            path.trimTo(pathTrimToLen);
        }
    }

    private void openNewSegment() {
        try {
            segmentId++;
            currentTxnStartRowNum = 0;
            rowValueIsNotNull.fill(0, columnCount, -1);
            final int segmentPathLen = createSegmentDir(segmentId);

            for (int i = 0; i < columnCount; i++) {
                int type = metadata.getColumnType(i);
                if (type > 0) {
                    final CharSequence name = metadata.getColumnName(i);
                    openColumnFiles(name, i, segmentPathLen);

                    if (type == ColumnType.SYMBOL && symbolMapReaders.size() > 0) {
                        final SymbolMapReader reader = symbolMapReaders.getQuick(i);
                        initialSymbolCounts.setQuick(i, reader.getSymbolCount());
                    }
                } else {
                    rowValueIsNotNull.setQuick(i, COLUMN_DELETED_NULL_FLAG);
                }
            }

            rowCount = 0;
            metadata.dumpTo(path, segmentPathLen);
            events.openEventFile(path, segmentPathLen);
            segmentStartMillis = millisecondClock.getTicks();
            lastSegmentTxn = 0;
            LOG.info().$("opened WAL segment [path='").$(path).$('\'').I$();
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void releaseLock(boolean keepLockFile) {
        if (lockFd != -1L) {
            ff.close(lockFd);
            if (keepLockFile) {
                return;
            }

            try {
                lockName(path);
                removeOrException(ff, path);
            } finally {
                path.trimTo(rootLen);
            }
        }
    }

    private void removeSymbolMapWriter(int index) {
        Misc.free(symbolMapReaders.getAndSetQuick(index, null));
        initialSymbolCounts.setQuick(index, -1);
    }

    private void resetDataTxnProperties() {
        currentTxnStartRowNum = rowCount;
        txnMinTimestamp = Long.MAX_VALUE;
        txnMaxTimestamp = -1;
        txnOutOfOrder = false;
        events.startTxn();
    }

    private void rollLastWalEventRecord(long newSegmentId, long uncommittedRows) {
        path.trimTo(rootLen).slash().put(newSegmentId);
        events.openEventFile(path, path.length());
        lastSegmentTxn = events.data(0, uncommittedRows, txnMinTimestamp, txnMaxTimestamp, txnOutOfOrder);
    }

    long rollSegment() {
        try {
            final long rolledRowCount = rowCount;
            openNewSegment();
            return rolledRowCount;
        } catch (Throwable e) {
            throw new CairoError(e);
        }
    }

    private void rowAppend(ObjList<Runnable> activeNullSetters, long rowTimestamp) {
        for (int i = 0; i < columnCount; i++) {
            if (rowValueIsNotNull.getQuick(i) < rowCount) {
                activeNullSetters.getQuick(i).run();
            }
        }

        if (rowTimestamp > txnMaxTimestamp) {
            txnMaxTimestamp = rowTimestamp;
        } else {
            txnOutOfOrder = txnMaxTimestamp != rowTimestamp;
        }
        if (rowTimestamp < txnMinTimestamp) {
            txnMinTimestamp = rowTimestamp;
        }

        rowCount++;
    }

    private void setAppendPosition(final long segmentRowCount) {
        for (int i = 0; i < columnCount; i++) {
            setColumnSize(i, segmentRowCount);
            int type = metadata.getColumnType(i);
            if (type > 0) {
                rowValueIsNotNull.setQuick(i, segmentRowCount - 1);
            }
        }
    }

    private void setColumnNull(int columnType, int columnIndex, long rowCount) {
        if (ColumnType.isVariableLength(columnType)) {
            setVarColumnVarFileNull(columnType, columnIndex, rowCount);
            setVarColumnFixedFileNull(columnType, columnIndex, rowCount);
        } else {
            setFixColumnNulls(columnType, columnIndex, rowCount);
        }
    }

    private void setColumnSize(int columnIndex, long size) {
        MemoryMA mem1 = getPrimaryColumn(columnIndex);
        MemoryMA mem2 = getSecondaryColumn(columnIndex);
        int type = metadata.getColumnType(columnIndex);
        if (type > 0) { // Not deleted
            final long pos = size;
            if (pos > 0) {
                // subtract column top
                final long m1pos;
                switch (ColumnType.tagOf(type)) {
                    case ColumnType.BINARY:
                    case ColumnType.STRING:
                        assert mem2 != null;
                        // Jump to the number of records written to read length of var column correctly
                        mem2.jumpTo(pos * Long.BYTES);
                        m1pos = Unsafe.getUnsafe().getLong(mem2.getAppendAddress());
                        // Jump to the end of file to correctly trim the file
                        mem2.jumpTo((pos + 1) * Long.BYTES);
                        break;
                    default:
                        if (columnIndex == metadata.getTimestampIndex()) {
                            m1pos = pos << 4;
                        } else {
                            m1pos = pos << ColumnType.pow2SizeOf(type);
                        }
                        break;
                }
                mem1.jumpTo(m1pos);
            } else {
                mem1.jumpTo(0);
                if (mem2 != null) {
                    mem2.jumpTo(0);
                    mem2.putLong(0);
                }
            }
        }
    }

    private void setFixColumnNulls(int type, int columnIndex, long rowCount) {
        MemoryMA fixedSizeColumn = getPrimaryColumn(columnIndex);
        long columnFileSize = rowCount * ColumnType.sizeOf(type);
        fixedSizeColumn.jumpTo(columnFileSize);
        if (columnFileSize > 0) {
            long address = TableUtils.mapRW(ff, fixedSizeColumn.getFd(), columnFileSize, MEM_TAG);
            try {
                TableUtils.setNull(type, address, rowCount);
            } finally {
                ff.munmap(address, columnFileSize, MEM_TAG);
            }
        }
    }

    private void setRowValueNotNull(int columnIndex) {
        assert rowValueIsNotNull.getQuick(columnIndex) != rowCount;
        rowValueIsNotNull.setQuick(columnIndex, rowCount);
    }

    private void setVarColumnFixedFileNull(int columnType, int columnIndex, long rowCount) {
        MemoryMA fixedSizeColumn = getSecondaryColumn(columnIndex);
        long fixedSizeColSize = (rowCount + 1) * Long.BYTES;
        fixedSizeColumn.jumpTo(fixedSizeColSize);
        if (rowCount > 0) {
            long addressFixed = TableUtils.mapRW(ff, fixedSizeColumn.getFd(), fixedSizeColSize, MEM_TAG);
            try {
                if (columnType == ColumnType.STRING) {
                    Vect.setVarColumnRefs32Bit(addressFixed, 0, rowCount + 1);
                } else {
                    Vect.setVarColumnRefs64Bit(addressFixed, 0, rowCount + 1);
                }
            } finally {
                ff.munmap(addressFixed, fixedSizeColSize, MEM_TAG);
            }
        }
    }

    private void setVarColumnVarFileNull(int columnType, int columnIndex, long rowCount) {
        MemoryMA varColumn = getPrimaryColumn(columnIndex);
        long varColSize = rowCount * ColumnType.variableColumnLengthBytes(columnType);
        varColumn.jumpTo(varColSize);
        if (rowCount > 0) {
            long address = TableUtils.mapRW(ff, varColumn.getFd(), varColSize, MEM_TAG);
            try {
                Vect.memset(address, (byte) varColSize, -1);
            } finally {
                ff.munmap(address, varColSize, MEM_TAG);
            }
        }
    }

    private void switchColumnsToNewSegment(LongList newColumnFiles) {
        for (int i = 0; i < columnCount; i++) {
            long newPrimaryFd = newColumnFiles.get(i * 4);
            if (newPrimaryFd > -1L) {
                MemoryMA primaryColumnFile = getPrimaryColumn(i);
                long writeOffset = newColumnFiles.get(i * 4 + 1);
                primaryColumnFile.switchTo(newPrimaryFd, writeOffset);

                long newSecondaryFd = newColumnFiles.get(i * 4 + 2);
                if (newSecondaryFd > -1L) {
                    MemoryMA secondaryColumnFile = getSecondaryColumn(i);
                    writeOffset = newColumnFiles.get(i * 4 + 3);
                    secondaryColumnFile.switchTo(newSecondaryFd, writeOffset);
                }
            }
        }
    }

    private long updateSegmentSize(long segmentSize, int columnIndex) {
        final MemoryA primaryColumn = getPrimaryColumn(columnIndex);
        if (primaryColumn != null && primaryColumn != NullMemory.INSTANCE) {
            segmentSize += primaryColumn.getAppendOffset();

            final MemoryA secondaryColumn = getSecondaryColumn(columnIndex);
            if (secondaryColumn != null && secondaryColumn != NullMemory.INSTANCE) {
                segmentSize += secondaryColumn.getAppendOffset();
            }
        }
        return segmentSize;
    }

    private class RowImpl implements TableWriter.Row {
        private long timestamp;

        @Override
        public void append() {
            rowAppend(nullSetters, timestamp);
        }

        @Override
        public void cancel() {
            setAppendPosition(rowCount);
        }

        @Override
        public void putBin(int columnIndex, long address, long len) {
            getSecondaryColumn(columnIndex).putLong(getPrimaryColumn(columnIndex).putBin(address, len));
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putBin(int columnIndex, BinarySequence sequence) {
            getSecondaryColumn(columnIndex).putLong(getPrimaryColumn(columnIndex).putBin(sequence));
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putBool(int columnIndex, boolean value) {
            getPrimaryColumn(columnIndex).putBool(value);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putByte(int columnIndex, byte value) {
            getPrimaryColumn(columnIndex).putByte(value);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putChar(int columnIndex, char value) {
            getPrimaryColumn(columnIndex).putChar(value);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putDouble(int columnIndex, double value) {
            getPrimaryColumn(columnIndex).putDouble(value);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putFloat(int columnIndex, float value) {
            getPrimaryColumn(columnIndex).putFloat(value);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putGeoHash(int index, long value) {
            int type = metadata.getColumnType(index);
            WriterRowUtils.putGeoHash(index, value, type, this);
        }

        @Override
        public void putGeoHashDeg(int index, double lat, double lon) {
            final int type = metadata.getColumnType(index);
            WriterRowUtils.putGeoHash(index, GeoHashes.fromCoordinatesDegUnsafe(lat, lon, ColumnType.getGeoHashBits(type)), type, this);
        }

        @Override
        public void putGeoStr(int index, CharSequence hash) {
            final int type = metadata.getColumnType(index);
            WriterRowUtils.putGeoStr(index, hash, type, this);
        }

        @Override
        public void putInt(int columnIndex, int value) {
            getPrimaryColumn(columnIndex).putInt(value);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putLong(int columnIndex, long value) {
            getPrimaryColumn(columnIndex).putLong(value);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putLong128LittleEndian(int columnIndex, long hi, long lo) {
            MemoryA primaryColumn = getPrimaryColumn(columnIndex);
            primaryColumn.putLong(lo);
            primaryColumn.putLong(hi);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putLong256(int columnIndex, long l0, long l1, long l2, long l3) {
            getPrimaryColumn(columnIndex).putLong256(l0, l1, l2, l3);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putLong256(int columnIndex, Long256 value) {
            getPrimaryColumn(columnIndex).putLong256(value.getLong0(), value.getLong1(), value.getLong2(), value.getLong3());
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putLong256(int columnIndex, CharSequence hexString) {
            getPrimaryColumn(columnIndex).putLong256(hexString);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putLong256(int columnIndex, @NotNull CharSequence hexString, int start, int end) {
            getPrimaryColumn(columnIndex).putLong256(hexString, start, end);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putShort(int columnIndex, short value) {
            getPrimaryColumn(columnIndex).putShort(value);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putStr(int columnIndex, CharSequence value) {
            getSecondaryColumn(columnIndex).putLong(getPrimaryColumn(columnIndex).putStr(value));
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putStr(int columnIndex, char value) {
            getSecondaryColumn(columnIndex).putLong(getPrimaryColumn(columnIndex).putStr(value));
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putStr(int columnIndex, CharSequence value, int pos, int len) {
            getSecondaryColumn(columnIndex).putLong(getPrimaryColumn(columnIndex).putStr(value, pos, len));
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putSym(int columnIndex, CharSequence value) {
            SymbolMapReader symbolMapReader = symbolMapReaders.getQuick(columnIndex);
            if (symbolMapReader != null) {
                int key = symbolMapReader.keyOf(value);
                if (key == SymbolTable.VALUE_NOT_FOUND) {
                    if (value != null) {
                        // Add it to in-memory symbol map
                        int initialSymCount = initialSymbolCounts.get(columnIndex);
                        CharSequenceIntHashMap symbolMap = symbolMaps.getQuick(columnIndex);
                        key = symbolMap.get(value);
                        if (key == SymbolTable.VALUE_NOT_FOUND) {
                            key = initialSymCount + symbolMap.size();
                            symbolMap.put(value, key);
                        }
                    } else {
                        key = SymbolTable.VALUE_IS_NULL;
                    }
                }
                getPrimaryColumn(columnIndex).putInt(key);
                setRowValueNotNull(columnIndex);
            } else {
                throw new UnsupportedOperationException();
            }
        }

        @Override
        public void putSym(int columnIndex, char value) {
            CharSequence str = SingleCharCharSequence.get(value);
            putSym(columnIndex, str);
        }

        private MemoryA getPrimaryColumn(int columnIndex) {
            return columns.getQuick(getPrimaryColumnIndex(columnIndex));
        }

        private MemoryA getSecondaryColumn(int columnIndex) {
            return columns.getQuick(getSecondaryColumnIndex(columnIndex));
        }
    }

    private class WalMetadataUpdaterBackend implements SequencerMetadataWriterBackend {
        @Override
        public void addColumn(CharSequence columnName, int columnType, int symbolCapacity, boolean symbolCacheFlag, boolean isIndexed, int indexValueBlockCapacity, boolean isSequential) {
            int columnIndex = metadata.getColumnIndexQuiet(columnName);

            if (columnIndex < 0 || metadata.getColumnType(columnIndex) < 0) {
                long uncommittedRows = rowCount - currentTxnStartRowNum;
                if (currentTxnStartRowNum > 0) {
                    // Roll last transaction to new segment
                    rollUncommittedToNewSegment();
                }

                if (currentTxnStartRowNum == 0 || rowCount == currentTxnStartRowNum) {
                    long segmentRowCount = rowCount - currentTxnStartRowNum;
                    metadata.addColumn(columnName, columnType);
                    columnCount = metadata.getColumnCount();
                    columnIndex = metadata.getColumnCount() - 1;
                    // create column file
                    configureColumn(columnIndex, columnType);
                    if (ColumnType.isSymbol(columnType)) {
                        configureSymbolMapWriter(columnIndex, columnName, 0, -1);
                    }

                    if (!rollSegmentOnNextRow) {
                        // This is WAL writer receiving notification from another writer that the column is added
                        // it has to add it to the open segment.
                        metadata.syncToMetaFile();
                        path.trimTo(rootLen).slash().put(segmentId);
                        openColumnFiles(columnName, columnIndex, path.length());
                    }
                    // If this is the WAL writer performing the add column operation
                    // it will add the column file / flush metadata on next row write.

                    if (uncommittedRows > 0) {
                        setColumnNull(columnType, columnIndex, segmentRowCount);
                    }
                    LOG.info().$("added column to wal [path=").$(path).$(Files.SEPARATOR).$(segmentId).$(", columnName=").$(columnName).I$();
                } else {
                    throw CairoException.critical(0).put("column '").put(columnName).put("' added, cannot commit because of concurrent table definition change ");
                }
            } else {
                if (metadata.getColumnType(columnIndex) == columnType) {
                    // TODO: this should be some kind of warning probably that different wals adding the same column concurrently
                    LOG.info().$("column has already been added by another WAL [path=").$(path).$(", columnName=").$(columnName).I$();
                } else {
                    throw CairoException.nonCritical().put("column '").put(columnName).put("' already exists");
                }
            }
        }

        @Override
        public RecordMetadata getMetadata() {
            return metadata;
        }

        @Override
        public CharSequence getTableName() {
            return tableName;
        }

        @Override
        public void removeColumn(CharSequence columnName) {
            final int columnIndex = metadata.getColumnIndexQuiet(columnName);
            if (columnIndex > -1) {
                int type = metadata.getColumnType(columnIndex);
                if (type > 0) {
                    if (currentTxnStartRowNum > 0) {
                        // Roll last transaction to new segment
                        rollUncommittedToNewSegment();
                    }

                    if (currentTxnStartRowNum == 0 || rowCount == currentTxnStartRowNum) {
                        int index = metadata.getColumnIndex(columnName);
                        metadata.removeColumn(columnName);
                        columnCount = metadata.getColumnCount();

                        if (!rollSegmentOnNextRow) {
                            // This is WAL writer receiving notification from another writer that the column is added
                            // it has to add it to the open segment.
                            metadata.syncToMetaFile();
                        }
                        // If this is the WAL writer performing the add column operation
                        // it will add the column file / flush metadata on next row write.

                        if (ColumnType.isSymbol(type)) {
                            removeSymbolMapWriter(index);
                        }
                        markColumnRemoved(index);
                        LOG.info().$("removed column from wal [path=").$(path).$(", columnName=").$(columnName).I$();
                    } else {
                        throw CairoException.critical(0).put("column '").put(columnName)
                                .put("' removed, cannot commit because of concurrent table definition change ");
                    }
                }
            } else {
                throw CairoException.nonCritical().put("column '").put(columnName).put("' does not exists");
            }
        }

        @Override
        public void renameColumn(CharSequence columnName, CharSequence newName) {
        }
    }

    private class AlterOperationValidationBackend implements SequencerMetadataWriterBackend {
        @Override
        public void addColumn(CharSequence columnName, int columnType, int symbolCapacity, boolean symbolCacheFlag, boolean isIndexed, int indexValueBlockCapacity, boolean isSequential) {
            if (!TableUtils.isValidColumnName(columnName, columnName.length())) {
                throw CairoException.critical(0).put("invalid column name: ").put(columnName);
            }
            if (metadata.getColumnIndexQuiet(columnName) > -1) {
                throw CairoException.critical(0).put("duplicate column name: ").put(columnName);
            }
            if (columnType <= 0 || columnType >= ColumnType.MAX) {
                throw CairoException.critical(0).put("invalid column type: ").put(columnType);
            }
        }

        @Override
        public RecordMetadata getMetadata() {
            return metadata;
        }

        @Override
        public CharSequence getTableName() {
            return tableName;
        }

        @Override
        public void removeColumn(CharSequence columnName) {
            int columnIndex = metadata.getColumnIndexQuiet(columnName);
            if (columnIndex < 0 || metadata.getColumnType(columnIndex) < 0) {
                throw CairoException.critical(0).put("cannot remove column, column does not exists [table=").put(tableName)
                        .put(", column=").put(columnName).put(']');
            }

            if (columnIndex == metadata.getTimestampIndex()) {
                throw CairoException.critical(0).put("cannot remove designated timestamp column [table=").put(tableName)
                        .put(", column=").put(columnName);
            }
        }

        @Override
        public void renameColumn(CharSequence columnName, CharSequence newName) {
            int columnIndex = metadata.getColumnIndexQuiet(columnName);
            if (columnIndex < 0) {
                throw CairoException.critical(0).put("cannot rename column, column does not exists [table=").put(tableName)
                        .put(", column=").put(columnName).put(']');
            }
            if (columnIndex == metadata.getTimestampIndex()) {
                throw CairoException.critical(0).put("cannot rename designated timestamp column [table=").put(tableName)
                        .put(", column=").put(columnName).put(']');
            }

            int columnIndexNew = metadata.getColumnIndexQuiet(newName);
            if (columnIndexNew > -1) {
                throw CairoException.critical(0).put("cannot rename column, column with the name already exists [table=").put(tableName)
                        .put(", newName=").put(newName).put(']');
            }
            if (!TableUtils.isValidColumnName(newName, newName.length())) {
                throw CairoException.critical(0).put("invalid column name: ").put(columnName);
            }
        }
    }
}
