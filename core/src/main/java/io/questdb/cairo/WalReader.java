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

import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.vm.NullMemoryMR;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import java.io.Closeable;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

public class WalReader implements Closeable, SymbolTableSource {
    private static final Log LOG = LogFactory.getLog(WalReader.class);

    private final FilesFacade ff;
    private final Path path;
    private final int rootLen;
    private final WalReaderMetadata metadata;
    private final WalReaderRecordCursor recordCursor = new WalReaderRecordCursor();
    private final String tableName;
    private final String walName;
    private final ObjList<SymbolMapReader> symbolMapReaders = new ObjList<>();
    private final CairoConfiguration configuration;
    private final long segmentId;
    private final long rowCount;
    private final ObjList<MemoryMR> columns;
    private final int columnCount;

    public WalReader(CairoConfiguration configuration, CharSequence tableName, CharSequence walName, long segmentId, IntList walSymbolCounts, long rowCount, int timestampIndex) {
        this.configuration = configuration;
        this.tableName = Chars.toString(tableName);
        this.walName = Chars.toString(walName);
        this.segmentId = segmentId;
        this.rowCount = rowCount;

        ff = configuration.getFilesFacade();
        path = new Path();
        path.of(configuration.getRoot()).concat(this.tableName).concat(this.walName);
        rootLen = path.length();
        try {
            metadata = openMetaFile(timestampIndex);
            columnCount = metadata.getColumnCount();
            LOG.debug().$("open [table=").$(this.tableName).I$();
            openSymbolMaps(walSymbolCounts);

            final int capacity = 2 * columnCount + 2;
            columns = new ObjList<>(capacity);
            columns.setPos(capacity + 2);
            columns.setQuick(0, NullMemoryMR.INSTANCE);
            columns.setQuick(1, NullMemoryMR.INSTANCE);
            recordCursor.of(this);
        } catch (Throwable e) {
            close();
            throw e;
        }
    }

    static int getPrimaryColumnIndex(int index) {
        return index * 2 + 2;
    }

    @Override
    public void close() {
        freeSymbolMapReaders();
        Misc.free(metadata);
        Misc.freeObjList(columns);
        Misc.free(path);
        LOG.debug().$("closed '").utf8(tableName).$('\'').$();
    }

    public MemoryR getColumn(int absoluteIndex) {
        return columns.getQuick(absoluteIndex);
    }

    public String getTableName() {
        return tableName;
    }

    public String getWalName() {
        return walName;
    }

    public int getTimestampIndex() {
        return metadata.getTimestampIndex();
    }

    public WalReaderRecordCursor getCursor() {
        recordCursor.toTop();
        return recordCursor;
    }

    public long openSegment() {
        path.slash().put(segmentId);
        try {
            if (ff.exists(path.$())) {
                path.chop$();
                openSegmentColumns();
                return rowCount;
            }
            LOG.error().$("open segment failed, segment does not exist on the disk. [path=").utf8(path.$()).I$();
            throw CairoException.instance(0)
                    .put("WAL data directory does not exist on disk at ")
                    .put(path);
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void openSegmentColumns() {
        for (int i = 0; i < columnCount; i++) {
            loadColumnAt(i);
        }
    }

    private void loadColumnAt(int columnIndex) {
        final int pathLen = path.length();
        try {
            final CharSequence name = metadata.getColumnName(columnIndex);
            final int primaryIndex = getPrimaryColumnIndex(columnIndex);
            final int secondaryIndex = primaryIndex + 1;
            final MemoryMR primaryMem = columns.getQuick(primaryIndex);

            final int columnType = metadata.getColumnType(columnIndex);
            if (ColumnType.isVariableLength(columnType)) {
                long columnSize = (rowCount + 1) << 3;
                TableUtils.iFile(path.trimTo(pathLen), name);
                MemoryMR secondaryMem = columns.getQuick(secondaryIndex);
                secondaryMem = openOrCreateMemory(path, columns, secondaryIndex, secondaryMem, columnSize);
                columnSize = secondaryMem.getLong(rowCount << 3);
                TableUtils.dFile(path.trimTo(pathLen), name);
                openOrCreateMemory(path, columns, primaryIndex, primaryMem, columnSize);
            } else {
                long columnSize = rowCount << ColumnType.pow2SizeOf(columnType);
                TableUtils.dFile(path.trimTo(pathLen), name);
                openOrCreateMemory(path, columns, primaryIndex, primaryMem, columnIndex == getTimestampIndex() ? columnSize << 1 : columnSize);
                Misc.free(columns.getAndSetQuick(secondaryIndex, null));
            }
        } finally {
            path.trimTo(pathLen);
        }
    }

    public long size() {
        return rowCount;
    }

    public String getColumnName(int columnIndex) {
        return metadata.getColumnName(columnIndex);
    }

    public int getColumnType(int columnIndex) {
        return metadata.getColumnType(columnIndex);
    }

    public SymbolMapReader getSymbolMapReader(int columnIndex) {
        return symbolMapReaders.getQuiet(columnIndex);
    }

    public SymbolMapDiff getSymbolMapDiff(int columnIndex) {
        return metadata.getSymbolMapDiff(columnIndex);
    }

    @Override
    public StaticSymbolTable getSymbolTable(int columnIndex) {
        return getSymbolMapReader(columnIndex);
    }

    @Override
    public StaticSymbolTable newSymbolTable(int columnIndex) {
        return getSymbolMapReader(columnIndex).newSymbolTableView();
    }

    private void freeSymbolMapReaders() {
        for (int i = 0, n = symbolMapReaders.size(); i < n; i++) {
            Misc.free(symbolMapReaders.getQuick(i));
        }
        symbolMapReaders.clear();
    }

    int getColumnCount() {
        return columnCount;
    }

    private WalReaderMetadata openMetaFile(int timestampIndex) {
        final long deadline = this.configuration.getMicrosecondClock().getTicks() + this.configuration.getSpinLockTimeoutUs();
        final WalReaderMetadata metadata = new WalReaderMetadata(ff, segmentId);
        try {
            while (true) {
                try {
                    return metadata.of(path, WalWriter.WAL_FORMAT_VERSION, timestampIndex);
                } catch (CairoException ex) {
                    TableUtils.handleMetadataLoadException(configuration, tableName, deadline, ex);
                }
            }
        } finally {
            path.trimTo(rootLen);
        }
    }

    @NotNull
    private MemoryMR openOrCreateMemory(
            Path path,
            ObjList<MemoryMR> columns,
            int primaryIndex,
            MemoryMR mem,
            long columnSize
    ) {
        if (mem != null && mem != NullMemoryMR.INSTANCE) {
            mem.of(ff, path, columnSize, columnSize, MemoryTag.MMAP_TABLE_WAL_READER);
        } else {
            mem = Vm.getMRInstance(ff, path, columnSize, MemoryTag.MMAP_TABLE_WAL_READER);
            columns.setQuick(primaryIndex, mem);
        }
        return mem;
    }

    private void openSymbolMaps(IntList walSymbolCounts) {
        int symbolMapIndex = 0;
        for (int i = 0; i < columnCount; i++) {
            if (ColumnType.isSymbol(metadata.getColumnType(i))) {
                // symbol map index array is sparse
                symbolMapReaders.extendAndSet(i, new SymbolMapReaderImpl(
                        configuration,
                        path,
                        metadata.getColumnName(i),
                        COLUMN_NAME_TXN_NONE,
                        walSymbolCounts.get(symbolMapIndex++)
                ));
            }
        }
    }
}
