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

import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.NullMapWriter;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.*;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

import static io.questdb.cairo.MapWriter.createSymbolMapFiles;
import static io.questdb.cairo.TableUtils.*;

public class WalWriter implements Closeable {
    static final String WAL_NAME_BASE = "wal";
    static final int WAL_FORMAT_VERSION = 0;
    private static final Log LOG = LogFactory.getLog(WalWriter.class);
    private static final Runnable NOOP = () -> {
    };
    private final ObjList<MemoryMA> columns;
    private final ObjList<MapWriter> symbolMapWriters;
    private final ObjList<MapWriter> denseSymbolMapWriters;
    private final IntList initSymbolCounts = new IntList();
    private final MillisecondClock millisecondClock;
    private final Path path;
    private final LongList rowValueIsNotNull = new LongList();
    private final int rootLen;
    private final FilesFacade ff;
    private final MemoryMAR symbolMapMem = Vm.getMARInstance();
    private final MemoryMAR metaMem = Vm.getMARInstance();
    private final int mkDirMode;
    private final String tableName;
    private final String walName;
    private final WalWriterMetadataCache metadataCache;
    private final CairoConfiguration configuration;
    private final ObjList<Runnable> nullSetters;
    private final Row row = new RowImpl();
    private long lockFd = -1;
    private int columnCount;
    private long rowCount = -1;
    private long segmentCount = -1;
    private long segmentStartMillis;
    private WalWriterRollStrategy rollStrategy = new WalWriterRollStrategy() {
    };

    public WalWriter(CairoConfiguration configuration, CharSequence tableName, CharSequence walName, TableReader reader) {
        this(configuration, tableName, walName, reader, configuration.getRoot());
    }

    public WalWriter(
            CairoConfiguration configuration,
            CharSequence tableName,
            CharSequence walName,
            TableReader tableReader,
            CharSequence root
    ) {
        LOG.info().$("open '").utf8(tableName).$('\'').$();
        this.configuration = configuration;
        this.millisecondClock = configuration.getMillisecondClock();
        this.mkDirMode = configuration.getMkDirMode();
        this.ff = configuration.getFilesFacade();
        this.tableName = Chars.toString(tableName);
        this.walName = Chars.toString(walName);
        this.path = new Path().of(root).concat(tableName).concat(walName);
        this.rootLen = path.length();

        try {
            lock();

            this.columnCount = tableReader.getColumnCount();
            this.columns = new ObjList<>(columnCount * 2);
            this.symbolMapWriters = new ObjList<>(columnCount);
            this.denseSymbolMapWriters = new ObjList<>(columnCount);
            this.nullSetters = new ObjList<>(columnCount);
            this.metadataCache = new WalWriterMetadataCache(configuration).of(tableReader);

            configureColumns(metadataCache);
            openNewSegment(metadataCache);
            configureSymbolTable(tableReader);
        } catch (Throwable e) {
            doClose(false);
            throw e;
        }
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

        if (metadataCache.getColumnIndexQuiet(name) != -1) {
            throw CairoException.instance(0).put("Duplicate column name: ").put(name);
        }

        try {
            closeCurrentSegment();
            LOG.info().$("adding column '").utf8(name).$('[').$(ColumnType.nameOf(type)).$("], to ").$(path).$();

            final int index = columnCount;
            metadataCache.addColumn(index, name, type);
            configureColumn(index, type);
            columnCount++;

            openNewSegment(metadataCache);
            configureSymbolMapWriter(index, name, type, null);
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

    public int getTimestampIndex() {
        return metadataCache.getTimestampIndex();
    }

    public Row newRow() {
        return newRow(0L);
    }

    public Row newRow(long timestamp) {
        try {
            final int timestampIndex = metadataCache.getTimestampIndex();
            if (timestampIndex != -1) {
                //avoid lookups by having a designated field with primaryColumn
                final MemoryMA primaryColumn = getPrimaryColumn(timestampIndex);
                primaryColumn.putLong128(timestamp, rowCount);
                setRowValueNotNull(timestampIndex);
            }
            return row;
        } catch (Throwable e) {
            throw new CairoError(e);
        }
    }

    public void removeColumn(CharSequence name) {
        try {
            closeCurrentSegment();
            LOG.info().$("removing column '").utf8(name).$("' from ").$(path).$();

            final int index = metadataCache.getColumnIndex(name);
            final int type = metadataCache.getColumnType(index);
            if (ColumnType.isSymbol(type)) {
                removeSymbolMapWriter(index);
            }

            metadataCache.removeColumn(index);
            removeColumn(index);

            openNewSegment(metadataCache);
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
                nullers.add(NOOP);
        }
    }

    private static void openMetaFile(FilesFacade ff, Path path, int rootLen, MemoryMR metaMem) {
        path.concat(META_FILE_NAME).$();
        try {
            metaMem.smallFile(ff, path, MemoryTag.MMAP_TABLE_WAL_WRITER);
        } finally {
            path.trimTo(rootLen);
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

    private void configureColumns(BaseRecordMetadata metadata) {
        for (int i = 0; i < columnCount; i++) {
            configureColumn(i, metadata.getColumnType(i));
        }
    }

    private void configureSymbolTable(TableReader tableReader) {
        final BaseRecordMetadata metadata = tableReader.getMetadata();
        for (int i = 0; i < columnCount; i++) {
            configureSymbolMapWriter(i, metadata.getColumnName(i), metadata.getColumnType(i), tableReader.getSymbolMapReader(i));
        }
    }

    private void configureSymbolMapWriter(int columnIndex, CharSequence columnName, int columnType, SymbolMapReader symbolMapReader) {
        if (!ColumnType.isSymbol(columnType)) {
            // maintain sparse list of symbol writers
            symbolMapWriters.extendAndSet(columnIndex, NullMapWriter.INSTANCE);
            initSymbolCounts.add(-1);
            return;
        }

        final SymbolMapWriter symbolMapWriter = new SymbolMapWriter(
                configuration,
                path.trimTo(rootLen),
                columnName,
                COLUMN_NAME_TXN_NONE,
                0,
                denseSymbolMapWriters.size(),
                SymbolValueCountCollector.NOOP
        );

        if (symbolMapReader != null) {
            // copy symbols from main table to wal
            for (int i = 0; i < symbolMapReader.getSymbolCount(); i++) {
                symbolMapWriter.put(symbolMapReader.valueOf(i));
            }
        }

        symbolMapWriters.extendAndSet(columnIndex, symbolMapWriter);
        denseSymbolMapWriters.add(symbolMapWriter);
        initSymbolCounts.add(symbolMapWriter.getSymbolCount());
    }

    private void doClose(boolean truncate) {
        Misc.free(metaMem);
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
        if (denseSymbolMapWriters != null) {
            for (int i = 0, n = denseSymbolMapWriters.size(); i < n; i++) {
                Misc.free(denseSymbolMapWriters.getQuick(i));
            }
            symbolMapWriters.clear();
        }

        if (symbolMapWriters != null) {
            symbolMapWriters.clear();
        }
    }

    private MemoryMA getPrimaryColumn(int column) {
        assert column < columnCount : "Column index is out of bounds: " + column + " >= " + columnCount;
        return columns.getQuick(getPrimaryColumnIndex(column));
    }

    private MemoryMA getSecondaryColumn(int column) {
        assert column < columnCount : "Column index is out of bounds: " + column + " >= " + columnCount;
        return columns.getQuick(getSecondaryColumnIndex(column));
    }

    MapWriter getSymbolMapWriter(int columnIndex) {
        return symbolMapWriters.getQuick(columnIndex);
    }

    private void lock() {
        try {
            lockName(path);
            lockFd = TableUtils.lock(ff, path);
        } finally {
            path.trimTo(rootLen);
        }

        if (lockFd == -1L) {
            throw CairoException.instance(ff.errno()).put("Cannot lock table: ").put(path.$());
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

    private void closeCurrentSegment() {
        writeSymbolMapDiffs();
    }

    private void openNewSegment(BaseRecordMetadata metadata) {
        try {
            segmentCount++;
            rowCount = 0;
            rowValueIsNotNull.fill(0, columnCount, -1);
            final int segmentPathLen = createSegmentDir();

            int liveColumnCount = 0;
            for (int i = 0; i < columnCount; i++) {
                int type = metadata.getColumnType(i);
                if (type > 0) {
                    liveColumnCount++;
                    final CharSequence name = metadata.getColumnName(i);
                    openColumnFiles(name, i, segmentPathLen);
                    if (ColumnType.isSymbol(type)) {
                        createSymbolMapFiles(
                                ff,
                                symbolMapMem,
                                path.trimTo(rootLen),
                                name,
                                COLUMN_NAME_TXN_NONE,
                                configuration.getDefaultSymbolCapacity(), // take this from TableStructure/TableWriter !!!
                                configuration.getDefaultSymbolCacheFlag() // take this from TableStructure/TableWriter !!!
                        );
                        path.slash().put(segmentCount);
                    }
                }
            }

            writeMetadata(metadata, segmentPathLen, liveColumnCount);
            segmentStartMillis = millisecondClock.getTicks();
            LOG.info().$("opened WAL segment [path='").$(path).$('\'').I$();
        } finally {
            path.trimTo(rootLen);
        }
    }

    private int createSegmentDir() {
        path.slash().put(segmentCount);
        final int segmentPathLen = path.length();
        if (ff.mkdirs(path.slash$(), mkDirMode) != 0) {
            throw CairoException.instance(ff.errno()).put("Cannot create WAL segment directory: ").put(path);
        }
        path.trimTo(segmentPathLen);
        return segmentPathLen;
    }

    private void writeMetadata(BaseRecordMetadata metadata, int pathLen, int liveColumnCount) {
        openMetaFile(ff, path, pathLen, metaMem);
        metaMem.putInt(WAL_FORMAT_VERSION);
        metaMem.putInt(liveColumnCount);
        for (int i = 0; i < columnCount; i++) {
            int type = metadata.getColumnType(i);
            if (type > 0) {
                metaMem.putInt(type);
                metaMem.putStr(metadata.getColumnName(i));
            }
        }
    }

    private void writeSymbolMapDiffs() {
        for (int i = 0; i < columnCount; i++) {
            final int initSymbolCount = initSymbolCounts.get(i);
            if (initSymbolCount > -1) {
                final MapWriter symbolMapWriter = symbolMapWriters.get(i);
                final int symbolCount = symbolMapWriter.getSymbolCount();
                if (symbolCount > initSymbolCount) {
                    metaMem.putInt(i);
                    metaMem.putInt(symbolCount - initSymbolCount);
                    for (int j = initSymbolCount; j < symbolCount; j++) {
                        metaMem.putInt(j);
                        metaMem.putStr(symbolMapWriter.valueOf(j));
                    }
                }
            }
        }
        metaMem.putInt(SymbolMapDiff.END_OF_SYMBOL_DIFFS);
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
        final MapWriter writer = symbolMapWriters.getAndSetQuick(index, NullMapWriter.INSTANCE);
        if (writer != null && writer != NullMapWriter.INSTANCE) {
            int symColIndex = denseSymbolMapWriters.remove(writer);
            // Shift all subsequent symbol indexes by 1 back
            while (symColIndex < denseSymbolMapWriters.size()) {
                MapWriter w = denseSymbolMapWriters.getQuick(symColIndex);
                w.setSymbolIndexInTxWriter(symColIndex);
                symColIndex++;
            }
            Misc.free(writer);
        }
        initSymbolCounts.setQuick(index, -1);
    }

    private void rowAppend(ObjList<Runnable> activeNullSetters) {
        for (int i = 0; i < columnCount; i++) {
            if (rowValueIsNotNull.getQuick(i) < rowCount) {
                activeNullSetters.getQuick(i).run();
            }
        }
        rowCount++;
    }

    public long rollSegment() {
        try {
            closeCurrentSegment();
            final long rolledRowCount = rowCount;
            openNewSegment(metadataCache);
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

    public interface Row {

        void append();

        void cancel();

        void putBin(int columnIndex, long address, long len);

        void putBin(int columnIndex, BinarySequence sequence);

        void putBool(int columnIndex, boolean value);

        void putByte(int columnIndex, byte value);

        void putChar(int columnIndex, char value);

        void putDate(int columnIndex, long value);

        void putDouble(int columnIndex, double value);

        void putFloat(int columnIndex, float value);

        void putGeoHash(int columnIndex, long value);

        void putGeoHashDeg(int index, double lat, double lon);

        void putGeoStr(int columnIndex, CharSequence value);

        void putInt(int columnIndex, int value);

        void putLong(int columnIndex, long value);

        void putLong256(int columnIndex, long l0, long l1, long l2, long l3);

        void putLong256(int columnIndex, Long256 value);

        void putLong256(int columnIndex, CharSequence hexString);

        void putLong256(int columnIndex, @NotNull CharSequence hexString, int start, int end);

        void putShort(int columnIndex, short value);

        void putStr(int columnIndex, CharSequence value);

        void putStr(int columnIndex, char value);

        void putStr(int columnIndex, CharSequence value, int pos, int len);

        void putSym(int columnIndex, CharSequence value);

        void putSym(int columnIndex, char value);

        void putSymIndex(int columnIndex, int symIndex);

        void putTimestamp(int columnIndex, long value);

        void putTimestamp(int columnIndex, CharSequence value);
    }

    private class RowImpl implements Row {
        @Override
        public void append() {
            rowAppend(nullSetters);
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
        public void putDate(int columnIndex, long value) {
            putLong(columnIndex, value);
            // putLong calls setRowValueNotNull
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
            int type = metadataCache.getColumnType(index);
            putGeoHash0(index, value, type);
        }

        @Override
        public void putGeoHashDeg(int index, double lat, double lon) {
            int type = metadataCache.getColumnType(index);
            putGeoHash0(index, GeoHashes.fromCoordinatesDegUnsafe(lat, lon, ColumnType.getGeoHashBits(type)), type);
        }

        @Override
        public void putGeoStr(int index, CharSequence hash) {
            long val;
            final int type = metadataCache.getColumnType(index);
            if (hash != null) {
                final int hashLen = hash.length();
                final int typeBits = ColumnType.getGeoHashBits(type);
                final int charsRequired = (typeBits - 1) / 5 + 1;
                if (hashLen < charsRequired) {
                    val = GeoHashes.NULL;
                } else {
                    try {
                        val = ColumnType.truncateGeoHashBits(
                                GeoHashes.fromString(hash, 0, charsRequired),
                                charsRequired * 5,
                                typeBits
                        );
                    } catch (NumericException e) {
                        val = GeoHashes.NULL;
                    }
                }
            } else {
                val = GeoHashes.NULL;
            }
            putGeoHash0(index, val, type);
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
            getPrimaryColumn(columnIndex).putInt(symbolMapWriters.getQuick(columnIndex).put(value));
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putSym(int columnIndex, char value) {
            getPrimaryColumn(columnIndex).putInt(symbolMapWriters.getQuick(columnIndex).put(value));
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putSymIndex(int columnIndex, int symIndex) {
            getPrimaryColumn(columnIndex).putInt(symIndex);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putTimestamp(int columnIndex, long value) {
            putLong(columnIndex, value);
        }

        @Override
        public void putTimestamp(int columnIndex, CharSequence value) {
            // try UTC timestamp first (micro)
            long l;
            try {
                l = value != null ? IntervalUtils.parseFloorPartialDate(value) : Numbers.LONG_NaN;
            } catch (NumericException e) {
                throw CairoException.instance(0).put("Invalid timestamp: ").put(value);
            }
            putTimestamp(columnIndex, l);
        }

        private MemoryA getPrimaryColumn(int columnIndex) {
            return columns.getQuick(getPrimaryColumnIndex(columnIndex));
        }

        private MemoryA getSecondaryColumn(int columnIndex) {
            return columns.getQuick(getSecondaryColumnIndex(columnIndex));
        }

        private void putGeoHash0(int index, long value, int type) {
            final MemoryA primaryColumn = getPrimaryColumn(index);
            switch (ColumnType.tagOf(type)) {
                case ColumnType.GEOBYTE:
                    primaryColumn.putByte((byte) value);
                    break;
                case ColumnType.GEOSHORT:
                    primaryColumn.putShort((short) value);
                    break;
                case ColumnType.GEOINT:
                    primaryColumn.putInt((int) value);
                    break;
                default:
                    primaryColumn.putLong(value);
                    break;
            }
            setRowValueNotNull(index);
        }
    }
}
