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

package io.questdb.cairo;

import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.cairo.vm.api.MemoryMAR;
import io.questdb.cairo.vm.api.NullMemory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.Path;
import io.questdb.std.str.SingleCharCharSequence;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.*;

public class WalWriter implements Closeable {
    static final String WAL_NAME_BASE = "wal";
    static final int WAL_FORMAT_VERSION = 0;
    private static final Log LOG = LogFactory.getLog(WalWriter.class);
    private static final Runnable NOOP = () -> {
    };
    private final TableDescriptor tableDescriptor = new TableDescriptorImpl();
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
    private final WalWriterMetadata metadata;
    private final WalWriterEvents events;
    private final Sequencer sequencer;
    private final CairoEngine engine;
    private final CairoConfiguration configuration;
    private final ObjList<Runnable> nullSetters;
    private final RowImpl row = new RowImpl();
    private long lockFd = -1;
    private int columnCount;
    private long startRowCount = -1;
    private long rowCount = -1;
    private long segmentId = -1;
    private long segmentStartMillis;
    private boolean txnOutOfOrder = false;
    private long txnMinTimestamp = Long.MAX_VALUE;
    private long txnMaxTimestamp = -1;
    private boolean rollSegmentOnNextRow = false;
    private WalWriterRollStrategy rollStrategy = new WalWriterRollStrategy() {
    };

    public WalWriter(CairoEngine engine, String tableName, int walId, Sequencer sequencer) {
        LOG.info().$("open '").utf8(tableName).$('\'').$();
        this.sequencer = sequencer;
        this.engine = engine;
        this.configuration = engine.getConfiguration();
        this.millisecondClock = configuration.getMillisecondClock();
        this.mkDirMode = configuration.getMkDirMode();
        this.ff = configuration.getFilesFacade();
        this.tableName = tableName;
        this.walName = WAL_NAME_BASE + walId;
        this.walId = walId;
        this.path = new Path().of(configuration.getRoot()).concat(tableName).concat(walName);
        this.rootLen = path.length();

        try {
            lock();

            sequencer.populateDescriptor(tableDescriptor);
            columnCount = tableDescriptor.getColumnCount();
            columns = new ObjList<>(columnCount * 2);
            nullSetters = new ObjList<>(columnCount);

            symbolMapReaders = new ObjList<>();
            metadata = new WalWriterMetadata(ff);
            metadata.of(tableDescriptor);
            events = new WalWriterEvents(ff);
            events.of(symbolMaps, initialSymbolCounts);

            configureColumns();
            openNewSegment();
            configureSymbolTable(tableDescriptor);
        } catch (Throwable e) {
            doClose(false);
            throw e;
        }
    }

    public long getSegment() {
        return segmentId;
    }

    public void setRollStrategy(WalWriterRollStrategy rollStrategy) {
        this.rollStrategy = rollStrategy;
    }

    private static int getPrimaryColumnIndex(int index) {
        return index * 2;
    }

    private static int getSecondaryColumnIndex(int index) {
        return getPrimaryColumnIndex(index) + 1;
    }

    public void addColumn(CharSequence name, int type) {
        addColumn(name, type, configuration.getDefaultSymbolCapacity());
    }

    public void addColumn(CharSequence name, int type, int symbolCapacity) {
        assert symbolCapacity == Numbers.ceilPow2(symbolCapacity) : "power of 2 expected";
        assert isValidColumnName(name, configuration.getMaxFileNameLength()) : "invalid column name";

        if (metadata.getColumnIndexQuiet(name) != -1) {
            throw CairoException.nonCritical().put("Duplicate column name: ").put(name);
        }

        try {
            LOG.info().$("adding column '").utf8(name).$('[').$(ColumnType.nameOf(type)).$("], to ").$(path).$();
            commit(true);

            final int index = columnCount;
            metadata.addColumn(index, name, type);
            configureColumn(index, type);
            columnCount++;
            configureSymbolMapWriter(index, name, 0, COLUMN_NAME_TXN_NONE);

            final long txn = sequencer.addColumn(index, name, type, walId, segmentId);
            events.addColumn(txn, index, name, type);
            LOG.info().$("ADDED column '").utf8(name).$('[').$(ColumnType.nameOf(type)).$("], to ").$(path).$();
        } catch (Throwable e) {
            throw new CairoError(e);
        }
    }

    @Override
    public void close() {
        try {
            closeCurrentSegment();
            doClose(true);
        } catch (Throwable e) {
            throw new CairoError(e);
        }
    }

    public String getTableName() {
        return tableName;
    }

    public String getWalName() {
        return walName;
    }

    public TableWriter.Row newRow() {
        return newRow(0L);
    }

    public TableWriter.Row newRow(long timestamp) {
        try {
            if (rollSegmentOnNextRow) {
                rollSegment();
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

    public void removeColumn(CharSequence name) {
        final int index = metadata.getColumnIndex(name);
        final int type = metadata.getColumnType(index);

        try {
            LOG.info().$("removing column '").utf8(name).$("' from ").$(path).$();
            commit(true);

            if (ColumnType.isSymbol(type)) {
                removeSymbolMapWriter(index);
            }
            metadata.removeColumn(index);
            removeColumn(index);

            final long txn = sequencer.removeColumn(index, walId, segmentId);
            events.removeColumn(txn, index);
            LOG.info().$("REMOVED column '").utf8(name).$("' from ").$(path).$();
        } catch (Throwable e) {
            throw new CairoError(e);
        }
    }

    public long size() {
        return rowCount;
    }

    @Override
    public String toString() {
        return "WalWriter{" +
                "name=" + tableName +
                '}';
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

    private void configureColumn(int index, int type) {
        final MemoryMA primary;
        final MemoryMA secondary;

        if (type > 0) {
            primary = Vm.getMAInstance();

            switch (ColumnType.tagOf(type)) {
                case ColumnType.BINARY:
                case ColumnType.STRING:
                    secondary = Vm.getMAInstance();
                    break;
                default:
                    secondary = null;
                    break;
            }
        } else {
            primary = secondary = NullMemory.INSTANCE;
        }

        int baseIndex = getPrimaryColumnIndex(index);
        columns.extendAndSet(baseIndex, primary);
        columns.extendAndSet(baseIndex + 1, secondary);
        configureNullSetters(nullSetters, type, primary, secondary);
        rowValueIsNotNull.extendAndSet(index, -1);
    }

    private void configureColumns() {
        for (int i = 0; i < columnCount; i++) {
            int columnType = metadata.getColumnType(i);
            if (columnType > 0) {
                configureColumn(i, columnType);
            }
        }
    }

    private void configureSymbolTable(TableDescriptor descriptor) {
        // we should not need the reader here, this will not work in a distributed environment
        // what if the wal is created on a node where the table itself is not present?
        // maybe copying symbols from existing table is not the best or sequencer needs an API for it
        // but then sequencer will have to keep track of all symbol tables
        int columnReaderIndex = 0;
        try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
            for (int i = 0; i < columnCount; i++) {
                int columnType = metadata.getColumnType(i);
                if (!ColumnType.isSymbol(columnType)) {
                    // maintain sparse list of symbol writers
                    symbolMapReaders.extendAndSet(i, null);
                    initialSymbolCounts.extendAndSet(i, -1);
                    symbolMaps.extendAndSet(i,null);
                } else {
                    SymbolMapReader symbolMapReader = reader.getSymbolMapReader(columnReaderIndex);
                    int symbolCount = symbolMapReader.getSymbolCount();
                    long columnNameTxn = reader.getColumnVersionReader().getDefaultColumnNameTxn(i);
                    configureSymbolMapWriter(i, descriptor.getColumnName(i), symbolCount, columnNameTxn);
                }

                if (columnType > 0) {
                    columnReaderIndex++;
                }
            }
        }
    }

    private void configureSymbolMapWriter(int columnWriterIndex, CharSequence columnName, int symbolCount, long columnNameTxm) {
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
        TableUtils.offsetFileName(tempPath, columnName, columnNameTxm);
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
        TableUtils.charFileName(tempPath, columnName, columnNameTxm);
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
        BitmapIndexUtils.keyFileName(tempPath, columnName, columnNameTxm);
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
        BitmapIndexUtils.valueFileName(tempPath, columnName, columnNameTxm);
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

    private void doClose(boolean truncate) {
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

    private void freeNullSetter(ObjList<Runnable> nullSetters, int columnIndex) {
        nullSetters.setQuick(columnIndex, NOOP);
    }

    private void freeSymbolMapWriters() {
        Misc.freeObjListIfCloseable(symbolMapReaders);
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

    private void openColumnFiles(CharSequence name, int columnIndex, int pathTrimToLen) {
        try {
            final MemoryMA mem1 = getPrimaryColumn(columnIndex);
            mem1.of(ff,
                    dFile(path.trimTo(pathTrimToLen), name),
                    configuration.getDataAppendPageSize(),
                    -1,
                    MemoryTag.MMAP_TABLE_WRITER,
                    configuration.getWriterFileOpenOpts(),
                    Files.POSIX_MADV_RANDOM
            );

            final MemoryMA mem2 = getSecondaryColumn(columnIndex);
            if (mem2 != null) {
                mem2.of(ff,
                        iFile(path.trimTo(pathTrimToLen), name),
                        configuration.getDataAppendPageSize(),
                        -1,
                        MemoryTag.MMAP_TABLE_WRITER,
                        configuration.getWriterFileOpenOpts(),
                        Files.POSIX_MADV_RANDOM
                );
                mem2.putLong(0L);
            }
        } finally {
            path.trimTo(pathTrimToLen);
        }
    }

    public long getTransientRowCount() {
        return rowCount - startRowCount;
    }

    public long commit() {
        return commit(false);
    }

    private long commit(boolean rollSegment) {
        rollSegmentOnNextRow = rollSegment;
        final long transientRowCount = getTransientRowCount();
        if (transientRowCount != 0) {
            events.data(nextTxn(), startRowCount, rowCount, txnMinTimestamp, txnMaxTimestamp, txnOutOfOrder);
            resetDataTxnProperties();
        }
        return transientRowCount;
    }

    private long nextTxn() {
        long txn;
        while ((txn = sequencer.nextTxn(tableDescriptor.getSchemaVersion(), walId, segmentId)) == Sequencer.NO_TXN) {
            // update table descriptor to get the latest version of the schema
            sequencer.populateDescriptor(tableDescriptor);

            // check schema diff !!!
            // might need to reject transaction here depending on schema diff!

            // then update metadata
            //metadata.of(tableDescriptor);
        }
        return txn;
    }

    private void resetDataTxnProperties() {
        startRowCount = rowCount;
        txnMinTimestamp = Long.MAX_VALUE;
        txnMaxTimestamp = -1;
        txnOutOfOrder = false;
    }

    private void closeCurrentSegment() {
        commit();
    }

    private void openNewSegment() {
        try {
            segmentId++;
            rowCount = 0;
            startRowCount = 0;
            rowValueIsNotNull.fill(0, columnCount, -1);
            final int segmentPathLen = createSegmentDir();

            for (int i = 0; i < columnCount; i++) {
                int type = metadata.getColumnType(i);

                if (type > 0) {
                    final CharSequence name = metadata.getColumnName(i);
                    openColumnFiles(name, i, segmentPathLen);

                    if (type == ColumnType.SYMBOL && symbolMapReaders.size() > 0) {
                        final SymbolMapReader reader = symbolMapReaders.getQuick(i);
                        initialSymbolCounts.setQuick(i, reader.getSymbolCount());
                    }
                }
            }

            metadata.openMetaFile(path, segmentPathLen, columnCount);
            events.openEventFile(path, segmentPathLen);
            segmentStartMillis = millisecondClock.getTicks();
            LOG.info().$("opened WAL segment [path='").$(path).$('\'').I$();
        } finally {
            path.trimTo(rootLen);
        }
    }

    private int createSegmentDir() {
        path.slash().put(segmentId);
        final int segmentPathLen = path.length();
        if (ff.mkdirs(path.slash$(), mkDirMode) != 0) {
            throw CairoException.critical(ff.errno()).put("Cannot create WAL segment directory: ").put(path);
        }
        path.trimTo(segmentPathLen);
        return segmentPathLen;
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

    private void removeColumn(int columnIndex) {
        final int pi = getPrimaryColumnIndex(columnIndex);
        final int si = getSecondaryColumnIndex(columnIndex);
        freeNullSetter(nullSetters, columnIndex);
        freeAndRemoveColumnPair(columns, pi, si);
    }

    private void removeSymbolMapWriter(int index) {
        Misc.freeIfCloseable(symbolMapReaders.getAndSetQuick(index, null));
        initialSymbolCounts.setQuick(index, -1);
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

    public long rollSegment() {
        try {
            closeCurrentSegment();
            final long rolledRowCount = rowCount;
            openNewSegment();
            return rolledRowCount;
        } catch (Throwable e) {
            throw new CairoError(e);
        }
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
            return rollSegment();
        }
        return 0L;
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

    private void setRowValueNotNull(int columnIndex) {
        assert rowValueIsNotNull.getQuick(columnIndex) != rowCount;
        rowValueIsNotNull.setQuick(columnIndex, rowCount);
    }

    private class RowImpl implements TableWriter.Row {
        private long timestamp;

        @Override
        public void append() {
            rowAppend(nullSetters, timestamp);
        }

        @Override
        public void cancel() {
            rollSegment();
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
        public void putLong128LittleEndian(int columnIndex, long hi, long lo) {
            MemoryA primaryColumn = getPrimaryColumn(columnIndex);
            primaryColumn.putLong(lo);
            primaryColumn.putLong(hi);
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
}
