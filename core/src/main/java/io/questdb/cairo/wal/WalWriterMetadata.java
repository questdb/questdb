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

import io.questdb.cairo.AbstractRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.cairo.wal.seq.TableRecordMetadataSink;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.META_FILE_NAME;
import static io.questdb.cairo.TableUtils.openSmallFile;
import static io.questdb.cairo.wal.WalUtils.WAL_FORMAT_VERSION;

public class WalWriterMetadata extends AbstractRecordMetadata implements TableRecordMetadata, TableRecordMetadataSink {
    private final FilesFacade ff;
    private final MemoryMARW metaMem;
    private final MemoryMR roMetaMem;
    private long structureVersion = -1;
    private int tableId;
    private TableToken tableToken;

    public WalWriterMetadata(FilesFacade ff) {
        this(ff, false);
    }

    public WalWriterMetadata(FilesFacade ff, boolean readonly) {
        this.ff = ff;
        if (!readonly) {
            roMetaMem = metaMem = Vm.getCMARWInstance();
        } else {
            metaMem = null;
            roMetaMem = Vm.getCMRInstance();
        }
    }

    public static void syncToMetaFile(
            MemoryMARW metaMem,
            long structureVersion,
            int columnCount,
            int timestampIndex,
            int tableId,
            RecordMetadata metadata
    ) {
        final boolean firstWrite = metaMem.getAppendOffset() == 0;
        metaMem.jumpTo(0);
        // Size of metadata
        if (firstWrite) {
            metaMem.putInt(0);
        } else {
            // When overwriting the file, don't "blip" the size to zero
            // in case there are concurrent readers.
            metaMem.skip(Integer.BYTES);
        }
        metaMem.putInt(WAL_FORMAT_VERSION);
        metaMem.putLong(structureVersion);
        final long columnCountOffset = metaMem.getAppendOffset();
        if (firstWrite) {
            metaMem.putInt(0);
        } else {
            // Same as for the file-size, keep the old size until
            // a new col count is patched at the end.
            metaMem.skip(Integer.BYTES);
        }
        metaMem.putInt(timestampIndex);
        metaMem.putInt(tableId);
        // we do not persist suspended flag anymore, suspended flag is in SeqTxnTracker
        // field is kept for backwards compatibility only, the value is irrelevant
        metaMem.putBool(false);
        for (int i = 0; i < columnCount; i++) {
            final int columnType = metadata.getColumnType(i);
            metaMem.putInt(columnType);
            metaMem.putStr(metadata.getColumnName(i));
        }

        // To avoid consistency issues with concurrent readers,
        // update the column count and file size last.
        final long size = metaMem.getAppendOffset();
        metaMem.putInt(0, (int) size);
        metaMem.putInt(columnCountOffset, columnCount);
    }

    @Override
    public void addColumn(
            String columnName,
            int columnType,
            boolean columnIndexed,
            int indexValueBlockCapacity,
            boolean symbolTableStatic,
            int writerIndex,
            boolean isDedupKey,
            boolean symbolIsCached,
            int symbolCapacity
    ) {
        addColumn0(
                columnName,
                columnType,
                symbolCapacity,
                symbolIsCached,
                isDedupKey
        );
    }

    public void addColumn(
            CharSequence columnName,
            int columnType,
            boolean isDedupKey,
            boolean symbolIsCached,
            int symbolCapacity
    ) {
        addColumn0(
                columnName,
                columnType,
                symbolCapacity,
                symbolIsCached,
                isDedupKey
        );
        structureVersion++;
    }

    public void changeColumnType(
            CharSequence columnName,
            int columnType,
            int symbolCapacity,
            boolean symbolCacheFlag,
            boolean isIndexed,
            int indexValueBlockCapacity
    ) {
        TableUtils.changeColumnTypeInMetadata(
                columnName,
                columnType,
                symbolCapacity,
                symbolCacheFlag,
                isIndexed,
                indexValueBlockCapacity,
                columnNameIndexMap,
                columnMetadata
        );
        columnCount++;
        structureVersion++;
    }

    @Override
    public void close() {
        clear(true, Vm.TRUNCATE_TO_PAGE);
    }

    public void close(boolean truncate, byte truncateMode) {
        clear(truncate, truncateMode);
    }

    public void disableDeduplicate() {
        structureVersion++;
    }

    public boolean enableDeduplicationWithUpsertKeys() {
        boolean isSubsetOfOldKeys = true;
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            isSubsetOfOldKeys &= isDedupKey(columnIndex);
        }
        structureVersion++;
        return isSubsetOfOldKeys;
    }

    @Override
    public long getMetadataVersion() {
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
    public void of(
            TableToken tableToken,
            int tableId,
            int timestampIndex,
            int compressedTimestampIndex,
            long structureVersion,
            int columnCount,
            @Transient IntList readColumnOrder
    ) {
        this.tableToken = tableToken;
        this.tableId = tableId;
        this.timestampIndex = timestampIndex;
        this.structureVersion = structureVersion;
    }

    public void removeColumn(CharSequence columnName) {
        TableUtils.removeColumnFromMetadata(columnName, columnNameIndexMap, columnMetadata);
        structureVersion++;
    }

    public void renameColumn(CharSequence columnName, CharSequence newName) {
        TableUtils.renameColumnInMetadata(columnName, newName, columnNameIndexMap, columnMetadata);
        structureVersion++;
    }

    public void renameTable(TableToken toTableToken) {
        assert toTableToken != null;
        tableToken = toTableToken;
        structureVersion++;
    }

    public void switchTo(Path path, int pathLen, boolean truncate) {
        if (metaMem.getFd() > -1) {
            metaMem.close(truncate, Vm.TRUNCATE_TO_POINTER);
        }
        openSmallFile(ff, path, pathLen, metaMem, META_FILE_NAME, MemoryTag.MMAP_SEQUENCER_METADATA);
        syncToMetaFile(metaMem, structureVersion, columnCount, timestampIndex, tableId, this);
    }

    private void addColumn0(
            CharSequence columnName,
            int columnType,
            int symbolCapacity,
            boolean symbolCacheFlag,
            boolean isDedupKey
    ) {
        final String name = columnName.toString();
        if (columnType > 0) {
            columnNameIndexMap.put(name, columnMetadata.size());
        }
        // sequencer metadata is servicing WALs, and it does not have
        // information about symbol indexing and index storage parameters
        // therefore we ignore the incoming parameters and assume defaults
        columnMetadata.add(
                new TableColumnMetadata(
                        name,
                        columnType,
                        false,
                        0,
                        false,
                        null,
                        columnMetadata.size(),
                        isDedupKey,
                        0,
                        symbolCacheFlag,
                        symbolCapacity
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
    }

    protected void clear(boolean truncate, byte truncateMode) {
        reset();
        if (metaMem != null) {
            metaMem.close(truncate, truncateMode);
        }
        Misc.free(roMetaMem);
    }
}
