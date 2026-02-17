/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypeDriver;
import io.questdb.cairo.SymbolMapReaderImpl;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.NullMemoryCMR;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.cairo.wal.seq.SequencerMetadata;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

public class WalReader implements Closeable {
    private static final Log LOG = LogFactory.getLog(WalReader.class);
    private final int columnCount;
    private final ObjList<MemoryCMR> columns;
    private final WalDataCursor dataCursor = new WalDataCursor();
    private final FilesFacade ff;
    private final SequencerMetadata metadata;
    private final Path path;
    private final int rootLen;
    private final long rowCount;
    private final ObjList<IntObjHashMap<CharSequence>> symbolMaps = new ObjList<>();
    private final String tableName;
    private final WalEventCursor walEventCursor;
    private final WalEventReader walEventReader;
    private final String walName;

    public WalReader(CairoConfiguration configuration, TableToken tableToken, CharSequence walName, int segmentId, long rowCount) {
        this.tableName = tableToken.getTableName();
        this.walName = Chars.toString(walName);
        this.rowCount = rowCount;

        ff = configuration.getFilesFacade();
        path = new Path();
        path.of(configuration.getDbRoot()).concat(tableToken.getDirName()).concat(walName);
        rootLen = path.size();

        try {
            metadata = new SequencerMetadata(configuration, true);
            metadata.open(path.slash().put(segmentId), rootLen, tableToken);
            columnCount = metadata.getColumnCount();
            walEventReader = new WalEventReader(configuration);
            LOG.debug().$("open [table=").$(tableToken).I$();
            int pathLen = path.size();
            walEventCursor = walEventReader.of(path.slash().put(segmentId), -1);
            path.trimTo(pathLen);
            openSymbolMaps(walEventCursor, configuration);
            path.slash().put(segmentId);
            walEventCursor.reset();

            final int capacity = 2 * columnCount + 2;
            columns = new ObjList<>(capacity);
            columns.setPos(capacity + 2);
            columns.setQuick(0, NullMemoryCMR.INSTANCE);
            columns.setQuick(1, NullMemoryCMR.INSTANCE);
            dataCursor.of(this);
        } catch (Throwable e) {
            close();
            throw e;
        }
    }

    @Override
    public void close() {
        Misc.free(walEventReader);
        Misc.free(metadata);
        Misc.freeObjList(columns);
        Misc.free(path);
        LOG.debug().$("closed '").$safe(tableName).$('\'').$();
    }

    public MemoryCR getColumn(int absoluteIndex) {
        return columns.getQuick(absoluteIndex);
    }

    public int getColumnCount() {
        return columnCount;
    }

    public String getColumnName(int columnIndex) {
        return metadata.getColumnName(columnIndex);
    }

    public int getColumnType(int columnIndex) {
        return metadata.getColumnType(columnIndex);
    }

    public WalDataCursor getDataCursor() {
        dataCursor.toTop();
        return dataCursor;
    }

    public int getRealColumnCount() {
        return metadata.getRealColumnCount();
    }

    public CharSequence getSymbolValue(int col, int key) {
        IntObjHashMap<CharSequence> symbolMap = symbolMaps.getQuick(col);
        return symbolMap.get(key);
    }

    public String getTableName() {
        return tableName;
    }

    public int getTimestampIndex() {
        return metadata.getTimestampIndex();
    }

    public WalEventCursor getWalEventCursor() {
        return walEventCursor;
    }

    public String getWalName() {
        return walName;
    }

    public long openSegment() {
        try {
            if (ff.exists(path.$())) {
                openSegmentColumns();
                return rowCount;
            }
            LOG.error().$("open segment failed, segment does not exist on the disk. [path=").$(path).I$();
            throw CairoException.critical(0)
                    .put("WAL data directory does not exist on disk at ")
                    .put(path);
        } finally {
            path.trimTo(rootLen);
        }
    }

    public long size() {
        return rowCount;
    }

    private void loadColumnAt(int columnIndex) {
        final int pathLen = path.size();
        try {
            final int columnType = metadata.getColumnType(columnIndex);
            if (columnType > 0) {
                final CharSequence columnName = metadata.getColumnName(columnIndex);
                final int dataMemIndex = getPrimaryColumnIndex(columnIndex);
                final int auxMemIndex = dataMemIndex + 1;
                final MemoryCMR dataMem = columns.getQuick(dataMemIndex);

                if (ColumnType.isVarSize(columnType)) {
                    ColumnTypeDriver columnTypeDriver = ColumnType.getDriver(columnType);
                    long auxMemSize = columnTypeDriver.getAuxVectorSize(rowCount);
                    TableUtils.iFile(path.trimTo(pathLen), columnName);

                    MemoryCMR auxMem = columns.getQuick(auxMemIndex);
                    auxMem = openOrCreateMemory(path, columns, auxMemIndex, auxMem, auxMemSize);
                    final long dataMemSize = columnTypeDriver.getDataVectorSizeAt(auxMem.addressOf(0), rowCount - 1);
                    TableUtils.dFile(path.trimTo(pathLen), columnName);
                    openOrCreateMemory(path, columns, dataMemIndex, dataMem, dataMemSize);
                } else {
                    final long dataMemSize = rowCount << ColumnType.pow2SizeOf(columnType);
                    TableUtils.dFile(path.trimTo(pathLen), columnName);
                    openOrCreateMemory(
                            path,
                            columns,
                            dataMemIndex,
                            dataMem,
                            columnIndex == getTimestampIndex() ? dataMemSize << 1 : dataMemSize
                    );
                    Misc.free(columns.getAndSetQuick(auxMemIndex, null));
                }
            }
        } finally {
            path.trimTo(pathLen);
        }
    }

    @NotNull
    private MemoryCMR openOrCreateMemory(
            Path path,
            ObjList<MemoryCMR> columns,
            int primaryIndex,
            MemoryCMR mem,
            long columnSize
    ) {
        if (mem != null && mem != NullMemoryCMR.INSTANCE) {
            mem.of(ff, path.$(), columnSize, columnSize, MemoryTag.MMAP_TABLE_WAL_READER);
        } else {
            mem = Vm.getCMRInstance(ff, path.$(), columnSize, MemoryTag.MMAP_TABLE_WAL_READER);
            columns.setQuick(primaryIndex, mem);
        }
        return mem;
    }

    private void openSegmentColumns() {
        for (int i = 0; i < columnCount; i++) {
            loadColumnAt(i);
        }
    }

    private void openSymbolMaps(WalEventCursor eventCursor, CairoConfiguration configuration) {
        while (eventCursor.hasNext()) {
            if (WalTxnType.isDataType(eventCursor.getType())) {
                WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                SymbolMapDiff symbolDiff = dataInfo.nextSymbolMapDiff();
                while (symbolDiff != null) {
                    int cleanSymbolCount = symbolDiff.getCleanSymbolCount();
                    int columnIndex = symbolDiff.getColumnIndex();
                    final IntObjHashMap<CharSequence> symbolMap;

                    if (symbolMaps.size() <= columnIndex || symbolMaps.getQuick(columnIndex) == null) {
                        symbolMap = new IntObjHashMap<>();
                        if (cleanSymbolCount > 0) {
                            try (
                                    SymbolMapReaderImpl symbolMapReader = new SymbolMapReaderImpl(
                                            configuration,
                                            path,
                                            this.metadata.getColumnName(columnIndex),
                                            COLUMN_NAME_TXN_NONE,
                                            cleanSymbolCount
                                    )
                            ) {
                                for (int key = 0; key < cleanSymbolCount; key++) {
                                    CharSequence symbol = symbolMapReader.valueOf(key);
                                    symbolMap.put(key, String.valueOf(symbol));
                                }
                            }
                        }
                        symbolMaps.extendAndSet(columnIndex, symbolMap);
                    } else {
                        symbolMap = symbolMaps.getQuick(columnIndex);
                    }

                    SymbolMapDiffEntry entry = symbolDiff.nextEntry();
                    while (entry != null) {
                        symbolMap.put(entry.getKey(), String.valueOf(entry.getSymbol()));
                        entry = symbolDiff.nextEntry();
                    }

                    symbolDiff = dataInfo.nextSymbolMapDiff();
                }
            }
        }
    }

    static int getPrimaryColumnIndex(int index) {
        return index * 2 + 2;
    }
}
