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
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.std.*;
import io.questdb.std.str.Path;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.*;
import static io.questdb.cairo.wal.WalUtils.*;

public class SequencerMetadata extends BaseRecordMetadata implements TableRecordMetadata, Closeable, TableDescriptor {
    private final FilesFacade ff;
    private final boolean readonly;
    private final MemoryMARW metaMem;
    private final MemoryMR roMetaMem;

    private long structureVersion = -1;
    private int tableId;
    private String tableName;

    public SequencerMetadata(FilesFacade ff) {
        this(ff, false);
    }

    public SequencerMetadata(FilesFacade ff, boolean readonly) {
        this.ff = ff;
        this.readonly = readonly;
        if (!readonly) {
            roMetaMem = metaMem = Vm.getMARWInstance();
        } else {
            metaMem = null;
            roMetaMem = Vm.getMRInstance();
        }
        columnMetadata = new ObjList<>();
        columnNameIndexMap = new LowerCaseCharSequenceIntHashMap();
    }

    public void addColumn(CharSequence columnName, int columnType) {
        addColumn0(columnName, columnType);
        structureVersion++;
    }

    @Override
    public void close() {
        clear(Vm.TRUNCATE_TO_PAGE);
    }

    public void close(byte truncateMode) {
        clear(truncateMode);
    }

    public void copyFrom(TableDescriptor model, String tableName, int tableId, long structureVersion) {
        reset();
        this.tableName = tableName;
        timestampIndex = model.getTimestampIndex();
        this.tableId = tableId;

        for (int i = 0; i < model.getColumnCount(); i++) {
            final CharSequence name = model.getColumnName(i);
            final int type = model.getColumnType(i);
            addColumn0(name, type);
        }

        this.structureVersion = structureVersion;
        columnCount = columnMetadata.size();
    }

    public void copyFrom(SequencerMetadata metadata, String tableName) {
        copyFrom(metadata, tableName, metadata.getTableId(), metadata.getStructureVersion());
    }

    public void create(TableDescriptor model, String tableName, Path path, int pathLen, int tableId) {
        copyFrom(model, tableName, tableId, 0);
        switchTo(path, pathLen);
    }

    public void dropTable() {
        this.structureVersion = DROP_TABLE_STRUCTURE_VERSION;
        syncToMetaFile();
    }

    public boolean isDropped() {
        return structureVersion == DROP_TABLE_STRUCTURE_VERSION;
    }

    public void switchTo(Path path, int pathLen) {
        if (metaMem.getFd() > -1) {
            metaMem.close(true, Vm.TRUNCATE_TO_POINTER);
        }
        openSmallFile(ff, path, pathLen, metaMem, META_FILE_NAME, MemoryTag.MMAP_SEQUENCER);
        syncToMetaFile();
    }

    public int getRealColumnCount() {
        return columnNameIndexMap.size();
    }

    @Override
    public int getId() {
        return tableId;
    }

    public long getStructureVersion() {
        return structureVersion;
    }

    public int getTableId() {
        return tableId;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public boolean isWalEnabled() {
        return true;
    }

    @Override
    public void toReaderIndexes() {
        // Remove deleted columns from the metadata, e.g. make it reader metadata.
        // Deleted columns have negative type.
        int copyTo = 0;
        for (int i = 0; i < columnCount; i++) {
            int columnType = columnMetadata.getQuick(i).getType();
            if (columnType > 0) {
                if (copyTo != i) {
                    TableColumnMetadata columnMeta = columnMetadata.get(i);
                    columnMetadata.set(copyTo, columnMeta);
                    if (i == timestampIndex) {
                        timestampIndex = copyTo;
                    }
                    columnNameIndexMap.put(columnMeta.getName(), copyTo);
                }
                copyTo++;
            }
        }
        columnCount = copyTo;
        columnMetadata.setPos(columnCount);
    }

    public void open(String tableName, Path path, int pathLen) {
        reset();
        this.tableName = tableName;
        openSmallFile(ff, path, pathLen, roMetaMem, META_FILE_NAME, MemoryTag.MMAP_SEQUENCER);

        // get written data size
        if (!readonly) {
            metaMem.jumpTo(SEQ_META_OFFSET_WAL_VERSION);
            int size = metaMem.getInt(0);
            metaMem.jumpTo(size);
        }

        loadSequencerMetadata(roMetaMem);
        structureVersion = roMetaMem.getLong(SEQ_META_OFFSET_STRUCTURE_VERSION);
        columnCount = columnMetadata.size();
        timestampIndex = roMetaMem.getInt(SEQ_META_OFFSET_TIMESTAMP_INDEX);
        tableId = roMetaMem.getInt(SEQ_META_TABLE_ID);

        if (readonly) {
            // close early
            roMetaMem.close();
        }
    }

    public void removeColumn(CharSequence columnName) {
        final int columnIndex = columnNameIndexMap.get(columnName);
        if (columnIndex < 0) {
            throw CairoException.critical(0).put("Column not found: ").put(columnName);
        }

        columnNameIndexMap.remove(columnName);
        final TableColumnMetadata deletedMeta = columnMetadata.getQuick(columnIndex);
        deletedMeta.markDeleted();

        structureVersion++;
    }

    public void renameColumn(CharSequence columnName, CharSequence newName) {
        final int columnIndex = columnNameIndexMap.get(columnName);
        if (columnIndex < 0) {
            throw CairoException.critical(0).put("Column not found: ").put(columnName);
        }
        final String newNameStr = newName.toString();
        columnMetadata.getQuick(columnIndex).setName(newNameStr);

        columnNameIndexMap.removeEntry(columnName);
        columnNameIndexMap.put(newNameStr, columnIndex);

        structureVersion++;
    }

    public void syncToMetaFile() {
        metaMem.jumpTo(0);
        // Size of metadata
        metaMem.putInt(0);
        metaMem.putInt(WAL_FORMAT_VERSION);
        metaMem.putLong(structureVersion);
        metaMem.putInt(columnCount);
        metaMem.putInt(timestampIndex);
        metaMem.putInt(tableId);
        for (int i = 0; i < columnCount; i++) {
            final int columnType = getColumnType(i);
            metaMem.putInt(columnType);
            metaMem.putStr(getColumnName(i));
        }

        // update metadata size
        metaMem.putInt(0, (int) metaMem.getAppendOffset());
    }

    private void loadSequencerMetadata(MemoryMR metaMem) {
        columnMetadata.clear();
        columnNameIndexMap.clear();

        try {
            final long memSize = checkMemSize(metaMem, SEQ_META_OFFSET_COLUMNS);
            validateMetaVersion(metaMem, SEQ_META_OFFSET_WAL_VERSION, WAL_FORMAT_VERSION);
            final int columnCount = TableUtils.getColumnCount(metaMem, SEQ_META_OFFSET_COLUMN_COUNT);
            final int timestampIndex = TableUtils.getTimestampIndex(metaMem, SEQ_META_OFFSET_TIMESTAMP_INDEX, columnCount);

            // load column types and names
            long offset = SEQ_META_OFFSET_COLUMNS;
            for (int i = 0; i < columnCount; i++) {
                final int type = TableUtils.getColumnType(metaMem, memSize, offset, i);
                offset += Integer.BYTES;

                final String name = TableUtils.getColumnName(metaMem, memSize, offset, i).toString();
                offset += Vm.getStorageLength(name);

                if (type > 0) {
                    columnNameIndexMap.put(name, i);
                }

                if (ColumnType.isSymbol(Math.abs(type))) {
                    columnMetadata.add(new TableColumnMetadata(name, -1L, type, true, 1024, true, null));
                } else {
                    columnMetadata.add(new TableColumnMetadata(name, -1L, type));
                }
            }

            // validate designated timestamp column
            if (timestampIndex != -1) {
                final int timestampType = columnMetadata.getQuick(timestampIndex).getType();
                if (!ColumnType.isTimestamp(timestampType)) {
                    throw validationException(metaMem).put("Timestamp column must be TIMESTAMP, but found ").put(ColumnType.nameOf(timestampType));
                }
            }
        } catch (Throwable e) {
            columnNameIndexMap.clear();
            columnMetadata.clear();
            throw e;
        }
    }

    private void addColumn0(CharSequence columnName, int columnType) {
        final String name = columnName.toString();
        if (columnType > 0) {
            columnNameIndexMap.put(name, columnMetadata.size());
        }
        columnMetadata.add(new TableColumnMetadata(name, -1L, columnType, false, 0, false, null, columnMetadata.size()));
        columnCount++;
    }

    protected void clear(byte truncateMode) {
        reset();
        if (metaMem != null) {
            metaMem.close(true, truncateMode);
        }
        Misc.free(roMetaMem);
    }

    private void reset() {
        columnMetadata.clear();
        columnNameIndexMap.clear();
        columnCount = 0;
        timestampIndex = -1;
        tableName = null;
    }
}
