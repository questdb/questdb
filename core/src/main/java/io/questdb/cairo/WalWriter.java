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

import io.questdb.Metrics;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.NullMapWriter;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.*;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

import static io.questdb.cairo.MapWriter.createSymbolMapFiles;
import static io.questdb.cairo.BitmapIndexUtils.*;
import static io.questdb.cairo.TableUtils.*;

public class WalWriter implements Closeable {
    public static final String WAL_NAME_BASE = "wal";
    public static final int WAL_FORMAT_VERSION = 0;
    private static final Log LOG = LogFactory.getLog(WalWriter.class);
    private static final CharSequenceHashSet IGNORED_FILES = new CharSequenceHashSet();
    private static final Runnable NOOP = () -> {
    };
    private final ObjList<MemoryMA> columns;
    private final ObjList<MapWriter> symbolMapWriters;
    private final ObjList<MapWriter> denseSymbolMapWriters;
    private final Path path;
    private final Path other;
    private final LongList rowValueIsNotNull = new LongList();
    private final int rootLen;
    private final FilesFacade ff;
    private final MemoryMAR symbolMapMem = Vm.getMARInstance();
    private final int mkDirMode;
    private final String tableName;
    private final String walName;
    private final WalWriterMetadataCache metadata;
    private final CairoConfiguration configuration;
    private final int defaultCommitMode;
    private final ObjList<Runnable> nullSetters;
    private final Metrics metrics;
    private final Row row = new RowImpl();
    private long lockFd = -1;
    private int columnCount;
    private boolean distressed = false;
    private long walDRowCounter;
    private long waldSegmentCounter = -1;

    public WalWriter(CairoConfiguration configuration, CharSequence tableName, CharSequence walName, TableReader reader, Metrics metrics) {
        this(configuration, tableName, walName, reader, true, configuration.getRoot(), metrics);
    }

    public WalWriter(
            CairoConfiguration configuration,
            CharSequence tableName,
            CharSequence walName,
            TableReader tableReader,
            boolean lock,
            CharSequence root,
            Metrics metrics
    ) {
        LOG.info().$("open '").utf8(tableName).$('\'').$();
        this.configuration = configuration;
        this.metrics = metrics;
        this.defaultCommitMode = configuration.getCommitMode();
        this.ff = configuration.getFilesFacade();
        this.mkDirMode = configuration.getMkDirMode();
        this.tableName = Chars.toString(tableName);
        this.walName = Chars.toString(walName);
        this.path = new Path().of(root).concat(tableName).concat(walName);
        this.other = new Path().of(root).concat(tableName).concat(walName);
        this.rootLen = path.length();
        try {
            if (lock) {
                lock();
            } else {
                this.lockFd = -1L;
            }
            path.trimTo(rootLen);

            this.columnCount = tableReader.getColumnCount();
            this.columns = new ObjList<>(columnCount * 2);
            this.symbolMapWriters = new ObjList<>(columnCount);
            this.denseSymbolMapWriters = new ObjList<>(columnCount);
            this.nullSetters = new ObjList<>(columnCount);
            this.metadata = new WalWriterMetadataCache(configuration).of(tableReader);

            configureColumnMemory(metadata);
            openNewSegment(metadata);
            configureSymbolTable(tableReader);
            setAppendPosition(walDRowCounter, false);
        } catch (Throwable e) {
            doClose(false);
            throw e;
        }
    }

    public static int getPrimaryColumnIndex(int index) {
        return index * 2;
    }

    public static int getSecondaryColumnIndex(int index) {
        return getPrimaryColumnIndex(index) + 1;
    }

    public void addColumn(CharSequence name, int type) {
        addColumn(name, type, configuration.getDefaultSymbolCapacity(), configuration.getDefaultSymbolCacheFlag());
    }

    public void addColumn(
            CharSequence name,
            int type,
            int symbolCapacity,
            boolean symbolCacheFlag
    ) {

        assert symbolCapacity == Numbers.ceilPow2(symbolCapacity) : "power of 2 expected";
        assert TableUtils.isValidColumnName(name) : "invalid column name";

        checkDistressed();

        if (metadata.getColumnIndexQuiet(name) != -1) {
            throw CairoException.instance(0).put("Duplicate column name: ").put(name);
        }

        commit();

        LOG.info().$("adding column '").utf8(name).$('[').$(ColumnType.nameOf(type)).$("], to ").$(path).$();

        metadata.addColumn(name, type, columnCount);

        if (ColumnType.isSymbol(type)) {
            createSymbolMapWriter(name);
        } else {
            // maintain sparse list of symbol writers
            symbolMapWriters.extendAndSet(columnCount, NullMapWriter.INSTANCE);
        }

        // add column objects
        configureColumn(type, columnCount);
        columnCount++;
        // create column files
        openColumnFiles(name, columnCount - 1, path.length());

        openNewSegment(metadata);

        LOG.info().$("ADDED column '").utf8(name).$('[').$(ColumnType.nameOf(type)).$("], to ").$(path).$();
    }

    @Override
    public void close() {
        doClose(true);
    }

    public void commit() {
        commit(defaultCommitMode);
    }

    public String getTableName() {
        return tableName;
    }

    public String getWalName() {
        return walName;
    }

    public long getCurrentWalDSegmentRowCount() {
        return walDRowCounter;
    }

    public Row newRow() {
        return newRow(0L);
    }

    public Row newRow(long timestamp) {
        int timestampIndex = metadata.getTimestampIndex();
        if (timestampIndex != -1) {
            // todo: avoid lookups by having a designated field with primaryColumn
            MemoryMA primaryColumn = getPrimaryColumn(timestampIndex);
            primaryColumn.putLong128(timestamp, walDRowCounter);
            setRowValueNotNull(timestampIndex);
        }
        return row;
    }

    public void removeColumn(CharSequence name) {
        checkDistressed();

        final int index = metadata.getColumnIndex(name);
        final int type = metadata.getColumnType(index);

        commit();

        LOG.info().$("removing column '").utf8(name).$("' from ").$(path).$();

        metadata.removeColumn(index);

        // remove column objects
        removeColumn(index);

        if (ColumnType.isSymbol(type)) {
            // remove symbol map writer or entry for such
            removeSymbolMapWriter(index);
        }

        try {
            removeColumnFiles(name, type);
        } catch (CairoException err) {
            throwDistressException(err);
        }

        openNewSegment(metadata);

        LOG.info().$("REMOVED column '").utf8(name).$("' from ").$(path).$();
    }

    public long size() {
        return walDRowCounter;
    }

    @Override
    public String toString() {
        return "WalWriter{" +
                "name=" + tableName +
                '}';
    }

    private static void removeFileAndOrLog(FilesFacade ff, LPSZ name) {
        if (ff.exists(name)) {
            if (ff.remove(name)) {
                LOG.info().$("removed: ").$(name).$();
            } else {
                LOG.error().$("cannot remove: ").utf8(name).$(" [errno=").$(ff.errno()).$(']').$();
            }
        }
    }

    private static void removeOrException(FilesFacade ff, LPSZ path) {
        if (ff.exists(path) && !ff.remove(path)) {
            throw CairoException.instance(ff.errno()).put("Cannot remove ").put(path);
        }
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

    private void checkDistressed() {
        if (!distressed) {
            return;
        }
        throw new CairoError("Table '" + tableName + "' is distressed");
    }

    private void closeAppendMemoryTruncate(boolean truncate) {
        for (int i = 0, n = columns.size(); i < n; i++) {
            MemoryMA m = columns.getQuick(i);
            if (m != null) {
                m.close(truncate);
            }
        }
    }

    private void commit(int commitMode) {
        checkDistressed();

        if (commitMode != CommitMode.NOSYNC) {
            syncColumns(commitMode);
        }

        metrics.tableWriter().incrementCommits();
        metrics.tableWriter().addCommittedRows(walDRowCounter);
    }

    private void configureColumn(int type, int index) {
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

    private void configureColumnMemory(BaseRecordMetadata metadata) {
        for (int i = 0; i < columnCount; i++) {
            int type = metadata.getColumnType(i);
            configureColumn(type, i);
        }
    }

    private void configureSymbolTable(TableReader tableReader) {
        final BaseRecordMetadata metadata = tableReader.getMetadata();
        for (int i = 0; i < columnCount; i++) {
            int type = metadata.getColumnType(i);
            if (ColumnType.isSymbol(type)) {
                final SymbolMapWriter symbolMapWriter = new SymbolMapWriter(
                        configuration,
                        path.trimTo(rootLen),
                        metadata.getColumnName(i),
                        COLUMN_NAME_TXN_NONE,
                        0,
                        denseSymbolMapWriters.size(),
                        SymbolValueCountCollector.NOOP
                );

                // copy symbols from main table to wal table
                final SymbolMapReader symbolMapReader = tableReader.getSymbolMapReader(i);
                for (int j = 0; j < symbolMapReader.getSymbolCount(); j++) {
                    symbolMapWriter.put(symbolMapReader.valueOf(j));
                }

                symbolMapWriters.extendAndSet(i, symbolMapWriter);
                denseSymbolMapWriters.add(symbolMapWriter);
            }
        }
    }

    private void createSymbolMapWriter(CharSequence name) {
        final SymbolMapWriter w = new SymbolMapWriter(
                configuration,
                path,
                name,
                COLUMN_NAME_TXN_NONE,
                0,
                denseSymbolMapWriters.size(),
                SymbolValueCountCollector.NOOP
        );
        denseSymbolMapWriters.add(w);
        symbolMapWriters.extendAndSet(columnCount, w);
    }

    private void doClose(boolean truncate) {
        freeSymbolMapWriters();
        Misc.free(symbolMapMem);
        Misc.free(other);
        freeColumns(truncate & !distressed);
        try {
            releaseLock(!truncate | distressed);
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
            closeAppendMemoryTruncate(truncate);
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
            path.trimTo(rootLen);
            lockName(path);
            this.lockFd = TableUtils.lock(ff, path);
        } finally {
            path.trimTo(rootLen);
        }

        if (this.lockFd == -1L) {
            throw CairoException.instance(ff.errno()).put("Cannot lock table: ").put(path.$());
        }
    }

    private void openColumnFiles(CharSequence name, int columnIndex, int pathTrimToLen) {
        MemoryMA mem1 = getPrimaryColumn(columnIndex);
        MemoryMA mem2 = getSecondaryColumn(columnIndex);

        try {
            mem1.of(ff,
                    dFile(path.trimTo(pathTrimToLen), name),
                    configuration.getDataAppendPageSize(),
                    -1,
                    MemoryTag.MMAP_TABLE_WRITER,
                    configuration.getWriterFileOpenOpts()
            );
            if (mem2 != null) {
                mem2.of(ff,
                        iFile(path.trimTo(pathTrimToLen), name),
                        configuration.getDataAppendPageSize(),
                        -1,
                        MemoryTag.MMAP_TABLE_WRITER,
                        configuration.getWriterFileOpenOpts()
                );
            }
        } finally {
            path.trimTo(pathTrimToLen);
        }
    }

    private void openNewSegment(BaseRecordMetadata metadata) {
        waldSegmentCounter++;
        walDRowCounter = 0;
        rowValueIsNotNull.fill(0, columnCount, -1);
        try {
            path.slash().put(waldSegmentCounter);
            final int pathLen = path.length();
            if (ff.mkdirs(path.slash$(), mkDirMode) != 0) {
                throw CairoException.instance(ff.errno()).put("Cannot create WAL-D segment directory: ").put(path);
            }
            path.trimTo(pathLen);
            assert columnCount > 0;

            int liveColumnCounter = 0;
            for (int i = 0; i < columnCount; i++) {
                int type = metadata.getColumnType(i);
                if (type > 0) {
                    liveColumnCounter++;
                    final CharSequence name = metadata.getColumnName(i);
                    openColumnFiles(name, i, pathLen);
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
                        path.slash().put(waldSegmentCounter);
                    }
                }
            }
            writeMetadata(metadata, pathLen, liveColumnCounter);
            LOG.info().$("switched WAL-D segment [path='").$(path).$('\'').I$();
        } catch (Throwable e) {
            distressed = true;
            throw e;
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void writeMetadata(BaseRecordMetadata metadata, int pathLen, int columnCount) {
        try (MemoryMAR metaMem = Vm.getMARInstance()) {
            openMetaFile(ff, path, pathLen, metaMem);
            metaMem.putInt(WAL_FORMAT_VERSION);
            metaMem.putInt(columnCount);
            for (int i = 0; i < columnCount; i++) {
                int type = metadata.getColumnType(i);
                if (type > 0) {
                    metaMem.putInt(type);
                    metaMem.putStr(metadata.getColumnName(i));
                }
            }
        }
    }

    private void releaseLock(boolean distressed) {
        if (lockFd != -1L) {
            ff.close(lockFd);
            if (distressed) {
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

    private void removeColumnFiles(CharSequence columnName, int columnType) {
        try {
            removeColumnFilesInPartition(columnName, waldSegmentCounter);
            if (ColumnType.isSymbol(columnType)) {
                removeFileAndOrLog(ff, offsetFileName(path.trimTo(rootLen), columnName, COLUMN_NAME_TXN_NONE));
                removeFileAndOrLog(ff, charFileName(path.trimTo(rootLen), columnName, COLUMN_NAME_TXN_NONE));
                removeFileAndOrLog(ff, keyFileName(path.trimTo(rootLen), columnName, COLUMN_NAME_TXN_NONE));
                removeFileAndOrLog(ff, valueFileName(path.trimTo(rootLen), columnName, COLUMN_NAME_TXN_NONE));
            }
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void removeColumnFilesInPartition(CharSequence columnName, long segmentId) {
        path.slash().put(segmentId);
        final int pathLen = path.length();
        removeFileAndOrLog(ff, dFile(path, columnName, COLUMN_NAME_TXN_NONE));
        removeFileAndOrLog(ff, iFile(path.trimTo(pathLen), columnName, COLUMN_NAME_TXN_NONE));
        removeFileAndOrLog(ff, keyFileName(path.trimTo(pathLen), columnName, COLUMN_NAME_TXN_NONE));
        removeFileAndOrLog(ff, valueFileName(path.trimTo(pathLen), columnName, COLUMN_NAME_TXN_NONE));
        path.trimTo(rootLen);
    }

    private void removeSymbolMapWriter(int index) {
        MapWriter writer = symbolMapWriters.getAndSetQuick(index, NullMapWriter.INSTANCE);
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
    }

    private void rowAppend(ObjList<Runnable> activeNullSetters) {
        for (int i = 0; i < columnCount; i++) {
            if (rowValueIsNotNull.getQuick(i) < walDRowCounter) {
                activeNullSetters.getQuick(i).run();
            }
        }
        walDRowCounter++;
    }

    void rowCancel() {
        openNewSegment(metadata);
    }

    void setAppendPosition(final long position, boolean doubleAllocate) {
        for (int i = 0; i < columnCount; i++) {
            // stop calculating oversize as soon as we find first over-sized column
            setColumnSize(i, position, doubleAllocate);
        }
    }

    private void setColumnSize(int columnIndex, long pos, boolean doubleAllocate) {
        MemoryMA mem1 = getPrimaryColumn(columnIndex);
        MemoryMA mem2 = getSecondaryColumn(columnIndex);
        int type = metadata.getColumnType(columnIndex);
        if (type > 0) { // Not deleted
            if (pos > 0) {
                // subtract column top
                final long m1pos;
                switch (ColumnType.tagOf(type)) {
                    case ColumnType.BINARY:
                    case ColumnType.STRING:
                        assert mem2 != null;
                        if (doubleAllocate) {
                            mem2.allocate(pos * Long.BYTES + Long.BYTES);
                        }
                        // Jump to the number of records written to read length of var column correctly
                        mem2.jumpTo(pos * Long.BYTES);
                        m1pos = Unsafe.getUnsafe().getLong(mem2.getAppendAddress());
                        // Jump to the end of file to correctly trim the file
                        mem2.jumpTo((pos + 1) * Long.BYTES);
                        break;
                    default:
                        m1pos = pos << ColumnType.pow2SizeOf(type);
                        break;
                }
                if (doubleAllocate) {
                    mem1.allocate(m1pos);
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

    private void setRowValueNotNull(int columnIndex) {
        assert rowValueIsNotNull.getQuick(columnIndex) != walDRowCounter;
        rowValueIsNotNull.setQuick(columnIndex, walDRowCounter);
    }

    private void syncColumns(int commitMode) {
        final boolean async = commitMode == CommitMode.ASYNC;
        for (int i = 0; i < columnCount; i++) {
            columns.getQuick(i * 2).sync(async);
            final MemoryMA m2 = columns.getQuick(i * 2 + 1);
            if (m2 != null) {
                m2.sync(false);
            }
        }
    }

    private void throwDistressException(Throwable cause) {
        distressed = true;
        throw new CairoError(cause);
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
            rowCancel();
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
            int type = metadata.getColumnType(index);
            putGeoHash0(index, value, type);
        }

        @Override
        public void putGeoHashDeg(int index, double lat, double lon) {
            int type = metadata.getColumnType(index);
            putGeoHash0(index, GeoHashes.fromCoordinatesDegUnsafe(lat, lon, ColumnType.getGeoHashBits(type)), type);
        }

        @Override
        public void putGeoStr(int index, CharSequence hash) {
            long val;
            final int type = metadata.getColumnType(index);
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

    static {
        IGNORED_FILES.add("..");
        IGNORED_FILES.add(".");
        IGNORED_FILES.add(META_FILE_NAME);
        IGNORED_FILES.add(TXN_FILE_NAME);
        IGNORED_FILES.add(TODO_FILE_NAME);
    }
}
