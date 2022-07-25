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
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.std.*;
import io.questdb.std.str.Path;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.*;

public class SequencerMetadata extends BaseRecordMetadata implements Closeable {
    private final FilesFacade ff;
    private final MemoryMAR metaMem = Vm.getMARInstance();

    private int schemaVersion = -1;
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

    void create(TableStructure model, Path path, int pathLen, int tableId) {
        reset();

        schemaVersion = 0;
        timestampIndex = model.getTimestampIndex();
        this.tableId = tableId;

        for (int i = 0; i < model.getColumnCount(); i++) {
            final CharSequence name = model.getColumnName(i);
            final int type = model.getColumnType(i);
            addColumn(i, name, type);
        }

        syncToMetaFile(path, pathLen);
    }

    void open(Path path, int pathLen) {
        reset();
        try (MemoryMR metaMem = Vm.getMRInstance()) {
            openSmallFile(ff, path, pathLen, metaMem, META_FILE_NAME, MemoryTag.MMAP_SEQUENCER);
            columnNameIndexMap.clear();
            loadSequencerMetadata(metaMem, columnMetadata, columnNameIndexMap);
            schemaVersion = metaMem.getInt(SEQ_META_OFFSET_SCHEMA_VERSION);
            columnCount = metaMem.getInt(SEQ_META_OFFSET_COLUMN_COUNT);
            timestampIndex = metaMem.getInt(SEQ_META_OFFSET_TIMESTAMP_INDEX);
            tableId = metaMem.getInt(SEQ_META_TABLE_ID);
        } catch (Throwable e) {
            close();
            throw e;
        }
    }

    private void reset() {
        columnMetadata.clear();
        columnNameIndexMap.clear();
        columnCount = 0;
        timestampIndex = -1;
    }

    public int getSchemaVersion() {
        return schemaVersion;
    }

    @Override
    public void close() {
        Misc.free(metaMem);
    }

    private void addColumn(int columnIndex, CharSequence columnName, int columnType) {
        final String name = columnName.toString();
        columnNameIndexMap.put(name, columnNameIndexMap.size());
        columnMetadata.add(new TableColumnMetadata(name, -1L, columnType, false, 0, false, null, columnIndex));
        columnCount++;
        schemaVersion++;
    }

    void addColumn(int columnIndex, CharSequence columnName, int columnType, Path path, int pathLen) {
        addColumn(columnIndex, columnName, columnType);
        syncToMetaFile(path, pathLen);
    }

    void removeColumn(int columnIndex, Path path, int pathLen) {
        final TableColumnMetadata deletedMeta = columnMetadata.getQuick(columnIndex);
        deletedMeta.markDeleted();
        columnNameIndexMap.remove(deletedMeta.getName());
        schemaVersion++;

        syncToMetaFile(path, pathLen);
    }

    private void syncToMetaFile(Path path, int pathLen) {
        int liveColumnCount = 0;
        for (int i = 0; i < columnCount; i++) {
            if (getColumnType(i) > 0) {
                liveColumnCount++;
            }
        }

        openSmallFile(ff, path, pathLen, metaMem, META_FILE_NAME, MemoryTag.MMAP_SEQUENCER);

        metaMem.putInt(WalWriter.WAL_FORMAT_VERSION);
        metaMem.putInt(schemaVersion);
        metaMem.putInt(liveColumnCount);
        metaMem.putInt(timestampIndex);
        metaMem.putInt(tableId);
        for (int i = 0; i < columnCount; i++) {
            final int type = getColumnType(i);
            if (type > 0) {
                metaMem.putInt(type);
                metaMem.putStr(getColumnName(i));
            }
        }
    }
}
