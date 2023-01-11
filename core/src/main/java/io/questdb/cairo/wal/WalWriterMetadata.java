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

import io.questdb.cairo.AbstractRecordMetadata;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.cairo.wal.seq.TableRecordMetadataSink;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.META_FILE_NAME;
import static io.questdb.cairo.TableUtils.openSmallFile;
import static io.questdb.cairo.wal.WalUtils.WAL_FORMAT_VERSION;

public class WalWriterMetadata extends AbstractRecordMetadata implements TableRecordMetadata, TableRecordMetadataSink {
    private final FilesFacade ff;
    private final MemoryMARW metaMem;
    private final MemoryMR roMetaMem;
    private long structureVersion = -1;
    private boolean suspended;
    private int tableId;
    private TableToken tableToken;

    public WalWriterMetadata(FilesFacade ff) {
        this(ff, false);
    }

    public WalWriterMetadata(FilesFacade ff, boolean readonly) {
        this.ff = ff;
        if (!readonly) {
            roMetaMem = metaMem = Vm.getMARWInstance();
        } else {
            metaMem = null;
            roMetaMem = Vm.getMRInstance();
        }
    }

    @Override
    public void addColumn(
            String columnName,
            int columnType,
            boolean columnIndexed,
            int indexValueBlockCapacity,
            boolean symbolTableStatic,
            int writerIndex
    ) {
        addColumn0(columnName, columnType);
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

    @Override
    public long getStructureVersion() {
        return structureVersion;
    }

    @Override
    public int getTableId() {
        return tableId;
    }

    @Override
    public TableToken getTableToken() {
        return tableToken;
    }

    @Override
    public boolean isWalEnabled() {
        return true;
    }

    @Override
    public void of(TableToken tableToken, int tableId, int timestampIndex, int compressedTimestampIndex, boolean suspended, long structureVersion, int columnCount) {
        this.tableToken = tableToken;
        this.tableId = tableId;
        this.timestampIndex = timestampIndex;
        this.suspended = suspended;
        this.structureVersion = structureVersion;
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

    public void switchTo(Path path, int pathLen) {
        if (metaMem.getFd() > -1) {
            metaMem.close(true, Vm.TRUNCATE_TO_POINTER);
        }
        openSmallFile(ff, path, pathLen, metaMem, META_FILE_NAME, MemoryTag.MMAP_SEQUENCER_METADATA);
        syncToMetaFile();
    }

    private void addColumn0(CharSequence columnName, int columnType) {
        final String name = columnName.toString();
        if (columnType > 0) {
            columnNameIndexMap.put(name, columnMetadata.size());
        }
        columnMetadata.add(
                new TableColumnMetadata(
                        name,
                        columnType,
                        false,
                        0,
                        false,
                        null,
                        columnMetadata.size()
                )
        );
        columnCount++;
    }

    private void reset() {
        columnMetadata.clear();
        columnNameIndexMap.clear();
        columnCount = 0;
        timestampIndex = -1;
        tableToken = null;
        tableId = -1;
        suspended = false;
    }

    protected void clear(byte truncateMode) {
        reset();
        if (metaMem != null) {
            metaMem.close(true, truncateMode);
        }
        Misc.free(roMetaMem);
    }

    void syncToMetaFile() {
        metaMem.jumpTo(0);
        // Size of metadata
        metaMem.putInt(0);
        metaMem.putInt(WAL_FORMAT_VERSION);
        metaMem.putLong(structureVersion);
        metaMem.putInt(columnCount);
        metaMem.putInt(timestampIndex);
        metaMem.putInt(tableId);
        metaMem.putBool(suspended);
        for (int i = 0; i < columnCount; i++) {
            final int columnType = getColumnType(i);
            metaMem.putInt(columnType);
            metaMem.putStr(getColumnName(i));
        }

        // update metadata size
        metaMem.putInt(0, (int) metaMem.getAppendOffset());
    }
}
