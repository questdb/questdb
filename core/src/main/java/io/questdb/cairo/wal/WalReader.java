/*+*****************************************************************************
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
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.NullMemoryCMR;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.cairo.wal.seq.SequencerMetadata;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.DirectSymbolMap;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.DirectString;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

public class WalReader implements Closeable {
    private static final Log LOG = LogFactory.getLog(WalReader.class);
    private final ObjList<MemoryCMR> columns = new ObjList<>();
    private final CairoConfiguration configuration;
    private final WalDataCursor dataCursor = new WalDataCursor();
    private final FilesFacade ff;
    private final SequencerMetadata metadata;
    private final Path path = new Path();
    private final ObjList<DirectSymbolMap> symbolMaps = new ObjList<>();
    private final WalEventReader walEventReader;
    private int columnCount;
    private int rootLen;
    private long rowCount;
    private String tableName;
    private WalEventCursor walEventCursor;
    private String walName;

    public WalReader(CairoConfiguration configuration) {
        this.configuration = configuration;
        this.ff = configuration.getFilesFacade();
        this.metadata = new SequencerMetadata(configuration, true);
        this.walEventReader = new WalEventReader(configuration);
    }

    public WalReader(CairoConfiguration configuration, TableToken tableToken, CharSequence walName, int segmentId, long rowCount) {
        this(configuration);
        try {
            of(tableToken, walName, segmentId, rowCount);
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
        Misc.freeObjList(symbolMaps);
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

    /**
     * Binds {@code view} to the bytes stored for {@code key} in column {@code col}.
     * The underlying bytes are stable for the current segment (the column's
     * {@link DirectSymbolMap} is populated once per {@link #of} call and is read-only
     * thereafter), so the returned view stays valid until the next {@link #of}
     * rebinds this reader, until {@link #close()}, or until the caller rebinds the
     * view itself via another call. Callers that need two simultaneous views on the
     * same column must supply two distinct {@link DirectString} instances.
     */
    /**
     * Returns the int key whose stored value equals {@code value} in column {@code col},
     * or {@link SymbolTable#VALUE_NOT_FOUND} if no such entry exists. The cumulative
     * symbol map is populated via {@link DirectSymbolMap#put(int, CharSequence)} which
     * does not maintain a reverse index, so this method walks the dense key range
     * 0..size-1 and compares each value byte-wise. The cost is proportional to the
     * column's accumulated dictionary size; the live view incremental refresh path calls
     * this once per filter init per segment, so the linear scan is acceptable.
     */
    public int getSymbolKey(int col, CharSequence value, DirectString view) {
        DirectSymbolMap symbolMap = col < symbolMaps.size() ? symbolMaps.getQuick(col) : null;
        if (symbolMap == null || value == null) {
            return SymbolTable.VALUE_NOT_FOUND;
        }
        for (int k = 0, n = symbolMap.size(); k < n; k++) {
            CharSequence v = symbolMap.valueOf(k, view);
            if (v != null && Chars.equals(value, v)) {
                return k;
            }
        }
        return SymbolTable.VALUE_NOT_FOUND;
    }

    public CharSequence getSymbolValue(int col, int key, DirectString view) {
        DirectSymbolMap symbolMap = symbolMaps.getQuick(col);
        return symbolMap != null ? symbolMap.valueOf(key, view) : null;
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

    /**
     * Rebinds this reader to a new (table, wal, segment). Reuses internal buffers and
     * off-heap symbol maps so a single instance can scan many segments without per-segment
     * object allocation. Not safe to call while a previously returned data cursor, symbol
     * view, or column pointer is still in use.
     */
    public WalReader of(TableToken tableToken, CharSequence walName, int segmentId, long rowCount) {
        this.tableName = tableToken.getTableName();
        this.walName = Chars.toString(walName);
        this.rowCount = rowCount;

        path.of(configuration.getDbRoot()).concat(tableToken.getDirName()).concat(walName);
        rootLen = path.size();

        metadata.open(path.slash().put(segmentId), rootLen, tableToken);
        columnCount = metadata.getColumnCount();
        LOG.debug().$("open [table=").$(tableToken).I$();
        int pathLen = path.size();
        walEventCursor = walEventReader.of(path.slash().put(segmentId), -1);
        path.trimTo(pathLen);
        openSymbolMaps(walEventCursor, configuration);
        path.slash().put(segmentId);
        walEventCursor.reset();

        final int capacity = 2 * columnCount + 2;
        // Drop column mmaps from a prior segment; loadColumnAt() remaps on demand for this one.
        Misc.freeObjList(columns);
        columns.clear();
        columns.setPos(capacity + 2);
        columns.setQuick(0, NullMemoryCMR.INSTANCE);
        columns.setQuick(1, NullMemoryCMR.INSTANCE);
        dataCursor.of(this);
        return this;
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
        // Preserve off-heap buffers but drop entries carried over from a prior segment.
        for (int i = 0, n = symbolMaps.size(); i < n; i++) {
            DirectSymbolMap m = symbolMaps.getQuick(i);
            if (m != null) {
                m.clear();
            }
        }
        while (eventCursor.hasNext()) {
            if (WalTxnType.isDataType(eventCursor.getType())) {
                WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                SymbolMapDiff symbolDiff = dataInfo.nextSymbolMapDiff();
                while (symbolDiff != null) {
                    int cleanSymbolCount = symbolDiff.getCleanSymbolCount();
                    int columnIndex = symbolDiff.getColumnIndex();
                    DirectSymbolMap symbolMap = columnIndex < symbolMaps.size() ? symbolMaps.getQuick(columnIndex) : null;

                    if (symbolMap == null) {
                        symbolMap = new DirectSymbolMap(256, 8, MemoryTag.NATIVE_DEFAULT);
                        symbolMaps.extendAndSet(columnIndex, symbolMap);
                    }
                    if (cleanSymbolCount > 0 && symbolMap.size() == 0) {
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
                                symbolMap.put(key, symbolMapReader.valueOf(key));
                            }
                        }
                    }

                    SymbolMapDiffEntry entry = symbolDiff.nextEntry();
                    while (entry != null) {
                        symbolMap.put(entry.getKey(), entry.getSymbol());
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
