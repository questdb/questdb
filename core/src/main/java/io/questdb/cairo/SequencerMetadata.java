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

import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMAR;
import io.questdb.std.*;
import io.questdb.std.str.Path;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.*;

public class SequencerMetadata extends BaseRecordMetadata implements Closeable, TableDescriptor {
    private final FilesFacade ff;
    private final MemoryMAR metaMem = Vm.getMARInstance();

    private int structureVersion = -1;
    private int tableId;

    SequencerMetadata(FilesFacade ff) {
        this.ff = ff;
        columnMetadata = new ObjList<>();
        columnNameIndexMap = new LowerCaseCharSequenceIntHashMap();
    }

    public void abortClose() {
        metaMem.close(false);
    }

    public int getTableId() {
        return tableId;
    }

    public void renameColumn(CharSequence columnName, CharSequence newName) {
        int columnIndex = columnNameIndexMap.get(columnName);
        if (columnIndex < 0) {
            throw CairoException.instance(0).put("Column not found: ").put(columnName);
        }
        int columnType = columnMetadata.getQuick(columnIndex).getType();
        columnMetadata.setQuick(columnIndex, new TableColumnMetadata(newName.toString(), 0L, columnType));
        structureVersion++;
        syncToMetaFile();
    }

    void create(TableDescriptor model, Path path, int pathLen, int tableId) {
        reset();
        openSmallFile(ff, path, pathLen, metaMem, META_FILE_NAME, MemoryTag.MMAP_SEQUENCER);
        timestampIndex = model.getTimestampIndex();
        this.tableId = tableId;

        for (int i = 0; i < model.getColumnCount(); i++) {
            final CharSequence name = model.getColumnName(i);
            final int type = model.getColumnType(i);
            addColumn0(name, type);
        }

        structureVersion = 0;
        columnCount = columnMetadata.size();
        syncToMetaFile();
    }

    void open(Path path, int pathLen) {
        reset();
        openSmallFile(ff, path, pathLen, metaMem, META_FILE_NAME, MemoryTag.MMAP_SEQUENCER);

        // get written data size
        metaMem.jumpTo(SEQ_META_OFFSET_WAL_VERSION);
        int size = metaMem.getInt(0);
        metaMem.jumpTo(size);

        loadSequencerMetadata(metaMem, columnMetadata, columnNameIndexMap);
        structureVersion = metaMem.getInt(SEQ_META_OFFSET_SCHEMA_VERSION);
        columnCount = columnMetadata.size();
        timestampIndex = metaMem.getInt(SEQ_META_OFFSET_TIMESTAMP_INDEX);
        tableId = metaMem.getInt(SEQ_META_TABLE_ID);
    }

    private void reset() {
        columnMetadata.clear();
        columnNameIndexMap.clear();
        columnCount = 0;
        timestampIndex = -1;
    }

    public int getStructureVersion() {
        return structureVersion;
    }

    @Override
    public void close() {
        Misc.free(metaMem);
    }

    private void addColumn0(CharSequence columnName, int columnType) {
        final String name = columnName.toString();
        columnNameIndexMap.put(name, columnNameIndexMap.size());
        columnMetadata.add(new TableColumnMetadata(name, -1L, columnType, false, 0, false, null, columnMetadata.size()));
    }

    void addColumn(CharSequence columnName, int columnType) {
        addColumn0(columnName, columnType);
        structureVersion++;
        syncToMetaFile();
    }

    void removeColumn(CharSequence columnName) {
        int columnIndex = columnNameIndexMap.get(columnName);
        if (columnIndex < 0) {
            throw CairoException.instance(0).put("Column not found: ").put(columnName);
        }
        final TableColumnMetadata deletedMeta = columnMetadata.getQuick(columnIndex);
        deletedMeta.markDeleted();
        columnNameIndexMap.remove(deletedMeta.getName());
        structureVersion++;
        syncToMetaFile();
    }

    private void syncToMetaFile() {
        metaMem.jumpTo(0);
        // Size of metadata
        metaMem.putInt(0);
        metaMem.putInt(WalWriter.WAL_FORMAT_VERSION);
        metaMem.putInt(structureVersion);
        metaMem.putInt(columnMetadata.size());
        metaMem.putInt(timestampIndex);
        metaMem.putInt(tableId);
        for (int i = 0; i < columnMetadata.size(); i++) {
            final int type = getColumnType(i);
            metaMem.putInt(type);
            metaMem.putStr(getColumnName(i));
        }

        // Set metadata size
        int size = (int) metaMem.getAppendOffset();
        metaMem.jumpTo(0);
        metaMem.putInt(size);
        metaMem.jumpTo(size);
    }
}
