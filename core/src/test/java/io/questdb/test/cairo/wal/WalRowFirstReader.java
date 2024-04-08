/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.cairo.wal;

import io.questdb.cairo.*;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.cairo.wal.SymbolMapDiff;
import io.questdb.cairo.wal.SymbolMapDiffEntry;
import io.questdb.cairo.wal.WalEventCursor;
import io.questdb.cairo.wal.WalEventReader;
import io.questdb.cairo.wal.seq.SequencerMetadata;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.Path;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;
import static io.questdb.cairo.wal.WalTxnType.ROW_FIRST_DATA;
import static io.questdb.cairo.wal.WalUtils.WAL_FORMAT_VERSION;

public class WalRowFirstReader implements Closeable {
    private static final Log LOG = LogFactory.getLog(WalRowFirstReader.class);
    private final int columnCount;
    private final WalRowFirstCursor dataCursor = new WalRowFirstCursor();
    private final WalEventCursor eventCursor;
    private final WalEventReader events;
    private final FilesFacade ff;
    private final SequencerMetadata metadata;
    private final Path path;
    private final int rootLen;
    private final long rowCount;
    private final MemoryMR rowMem;
    private final ObjList<IntObjHashMap<CharSequence>> symbolMaps = new ObjList<>();
    private final String tableName;
    private final String walName;

    public WalRowFirstReader(CairoConfiguration configuration, TableToken tableToken, CharSequence walName, int segmentId, long rowCount) {
        this.tableName = tableToken.getTableName();
        this.walName = Chars.toString(walName);
        this.rowCount = rowCount;

        ff = configuration.getFilesFacade();
        path = new Path();
        path.of(configuration.getRoot()).concat(tableToken.getDirName()).concat(walName);
        rootLen = path.size();

        try {
            metadata = new SequencerMetadata(ff, true);
            metadata.open(path.slash().put(segmentId), rootLen, tableToken);
            columnCount = metadata.getColumnCount();
            events = new WalEventReader(ff);
            LOG.debug().$("open [table=").$(tableName).I$();
            int pathLen = path.size();
            eventCursor = events.of(path.slash().put(segmentId), WAL_FORMAT_VERSION, -1L);
            path.trimTo(pathLen);
            openSymbolMaps(eventCursor, configuration);
            path.slash().put(segmentId);
            eventCursor.reset();

            rowMem = Vm.getMRInstance();
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
        Misc.free(rowMem);
        Misc.free(path);
        LOG.debug().$("closed '").utf8(tableName).$('\'').$();
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

    public WalRowFirstCursor getCursor() {
        dataCursor.toTop();
        return dataCursor;
    }

    public WalEventCursor getEventCursor() {
        return eventCursor;
    }

    public int getRealColumnCount() {
        return metadata.getRealColumnCount();
    }

    public MemoryMR getRowMem() {
        return rowMem;
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
                doOpenSegment();
                return rowCount;
            }
            LOG.error().$("open segment failed, segment does not exist on the disk [path=").$(path).I$();
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

    private void doOpenSegment() {
        final int pathLen = path.size();
        try {
            path.trimTo(pathLen).concat(TableUtils.WAL_SEGMENT_FILE_NAME).$();
            long size = ff.length(path);
            rowMem.of(ff, path, size, size, MemoryTag.MMAP_TABLE_WAL_READER);
        } finally {
            path.trimTo(pathLen);
        }
    }

    private void openSymbolMaps(WalEventCursor eventCursor, CairoConfiguration configuration) {
        while (eventCursor.hasNext()) {
            if (eventCursor.getType() == ROW_FIRST_DATA) {
                WalEventCursor.RowFirstDataInfo dataInfo = eventCursor.getRowFirstDataInfo();
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
