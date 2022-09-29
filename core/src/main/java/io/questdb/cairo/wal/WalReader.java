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
import static io.questdb.cairo.wal.WalTxnType.DATA;
import static io.questdb.cairo.wal.WalUtils.WAL_FORMAT_VERSION;

public class WalReader implements Closeable {
    private static final Log LOG = LogFactory.getLog(WalReader.class);

    private final FilesFacade ff;
    private final Path path;
    private final int rootLen;
    private final SequencerMetadata metadata;
    private final WalDataCursor dataCursor = new WalDataCursor();
    private final WalEventReader events;
    private final WalEventCursor eventCursor;
    private final String tableName;
    private final String walName;
    private final ObjList<IntObjHashMap<CharSequence>> symbolMaps = new ObjList<>();
    private final long rowCount;
    private final ObjList<MemoryMR> columns;
    private final int columnCount;

    public WalReader(CairoConfiguration configuration, CharSequence tableName, CharSequence fileSystemName, CharSequence walName, long segmentId, long rowCount) {
        this.tableName = Chars.toString(tableName);
        this.walName = Chars.toString(walName);
        this.rowCount = rowCount;

        ff = configuration.getFilesFacade();
        path = new Path().of(configuration.getRoot()).concat(fileSystemName).concat(walName);
        rootLen = path.length();

        try {
            this.metadata = new SequencerMetadata(ff, SequencerMetadata.READ_ONLY);
            this.metadata.open(Chars.toString(tableName), path.slash().put(segmentId), rootLen);
            columnCount = metadata.getColumnCount();
            events = new WalEventReader(ff);
            LOG.debug().$("open [table=").$(tableName).I$();
            int pathLen = path.length();
            eventCursor = events.of(path.slash().put(segmentId), WAL_FORMAT_VERSION, -1L);
            path.trimTo(pathLen);
            openSymbolMaps(eventCursor, configuration);
            path.slash().put(segmentId);
            eventCursor.reset();

            final int capacity = 2 * columnCount + 2;
            columns = new ObjList<>(capacity);
            columns.setPos(capacity + 2);
            columns.setQuick(0, NullMemoryMR.INSTANCE);
            columns.setQuick(1, NullMemoryMR.INSTANCE);
            dataCursor.of(this);
        } catch (Throwable e) {
            close();
            throw e;
        }
    }

    @Override
    public void close() {
        Misc.free(events);
        Misc.free(metadata);
        Misc.freeObjList(columns);
        Misc.free(path);
        LOG.debug().$("closed '").utf8(tableName).$('\'').$();
    }

    public MemoryR getColumn(int absoluteIndex) {
        return columns.getQuick(absoluteIndex);
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

    public WalEventCursor getEventCursor() {
        return eventCursor;
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

    public String getWalName() {
        return walName;
    }

    public long openSegment() {
        try {
            if (ff.exists(path.$())) {
                path.chop$();
                openSegmentColumns();
                return rowCount;
            }
            LOG.error().$("open segment failed, segment does not exist on the disk. [path=").utf8(path.$()).I$();
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

    static int getPrimaryColumnIndex(int index) {
        return index * 2 + 2;
    }

    int getRealColumnCount() {
        return metadata.getRealColumnCount();
    }

    int getColumnCount() {
        return columnCount;
    }

    private void loadColumnAt(int columnIndex) {
        final int pathLen = path.length();
        try {
            final int columnType = metadata.getColumnType(columnIndex);
            if (columnType > 0) {
                final CharSequence name = metadata.getColumnName(columnIndex);
                final int primaryIndex = getPrimaryColumnIndex(columnIndex);
                final int secondaryIndex = primaryIndex + 1;
                final MemoryMR primaryMem = columns.getQuick(primaryIndex);

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
            }
        } finally {
            path.trimTo(pathLen);
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

    private void openSegmentColumns() {
        for (int i = 0; i < columnCount; i++) {
            loadColumnAt(i);
        }
    }

    private void openSymbolMaps(WalEventCursor eventCursor, CairoConfiguration configuration) {
        while (eventCursor.hasNext()) {
            if (eventCursor.getType() == DATA) {
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
}
