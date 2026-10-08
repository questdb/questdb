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

package io.questdb.cairo.mig;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.ParquetMetaFileReader;
import io.questdb.cairo.SymbolMapWriter;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.idx.BitmapIndexBwdReader;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.COLUMN_VERSION_FILE_NAME;
import static io.questdb.cairo.TableUtils.META_FILE_NAME;
import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;
import static io.questdb.cairo.view.ViewDefinition.VIEW_DEFINITION_FILE_NAME;

/**
 * Sets the symbol map null flag of every SYMBOL column that a partition proves to hold a NULL:
 * a column top, a parquet chunk null count, or a NULL entry in the column's bitmap index.
 * Earlier releases left the flag unset when {@code ALTER TABLE ... ALTER COLUMN ... TYPE SYMBOL}
 * converted a column whose partitions carried column tops, when {@code ATTACH PARTITION} brought
 * in NULL rows, and when a parquet conversion collapsed such column tops to zero.
 * <p>
 * The migration never reads column data. A native partition without a BITMAP index, or a parquet
 * partition written without statistics, contributes no evidence; see "Symbol null flag" in
 * cairo/CLAUDE.md.
 */
public final class Mig1002 {
    private static final Log LOG = LogFactory.getLog(EngineMigration.class);

    public static void migrate(MigrationContext migrationContext) {
        final FilesFacade ff = migrationContext.getFf();
        final Path path = migrationContext.getTablePath();
        final int plen = path.size();
        try {
            // a view owns no symbol maps
            if (ff.exists(path.concat(VIEW_DEFINITION_FILE_NAME).$())) {
                return;
            }
            if (!ff.exists(path.trimTo(plen).concat(META_FILE_NAME).$())) {
                LOG.error().$("meta file does not exist, nothing to migrate [path=").$(path).I$();
                return;
            }
            final long metaFileSize = ff.length(path.$());
            try (MemoryCMR metaMem = Vm.getCMRInstance(ff, path.$(), metaFileSize, MemoryTag.NATIVE_MIG_MMAP)) {
                final int columnCount = metaMem.getInt(TableUtils.META_OFFSET_COUNT);
                final int partitionBy = metaMem.getInt(TableUtils.META_OFFSET_PARTITION_BY);
                final int timestampIndex = metaMem.getInt(TableUtils.META_OFFSET_TIMESTAMP_INDEX);
                final int timestampType = timestampIndex > -1 && timestampIndex < columnCount
                        ? TableUtils.getColumnType(metaMem, timestampIndex)
                        : ColumnType.TIMESTAMP;

                if (!ff.exists(path.trimTo(plen).concat(TXN_FILE_NAME).$())) {
                    LOG.error().$("tx file does not exist, nothing to migrate [path=").$(path).I$();
                    return;
                }
                if (!ff.exists(path.trimTo(plen).concat(COLUMN_VERSION_FILE_NAME).$())) {
                    LOG.error().$("column version file does not exist, nothing to migrate [path=").$(path).I$();
                    return;
                }

                try (
                        TxReader txReader = new TxReader(ff).ofRO(path.trimTo(plen).concat(TXN_FILE_NAME).$(), timestampType, partitionBy);
                        ColumnVersionReader cvReader = new ColumnVersionReader().ofRO(ff, path.trimTo(plen).concat(COLUMN_VERSION_FILE_NAME).$())
                ) {
                    if (!txReader.unsafeLoadAll()) {
                        throw CairoException.critical(0).put("migration failed, could not read tx file [path=").put(path).put(']');
                    }
                    cvReader.readUnsafe();

                    final ObjList<String> columnNames = new ObjList<>(columnCount);
                    final IntList pendingColumns = new IntList();
                    long nameOffset = TableUtils.getColumnNameOffset(columnCount);
                    for (int i = 0; i < columnCount; i++) {
                        final CharSequence columnName = metaMem.getStrA(nameOffset);
                        nameOffset += Vm.getStorageLength(columnName);
                        columnNames.add(Chars.toString(columnName));
                        final int columnType = TableUtils.getColumnType(metaMem, i);
                        if (columnType > 0 && ColumnType.isSymbol(columnType)
                                && isNullFlagUnset(migrationContext, path.trimTo(plen), columnName, cvReader.getSymbolTableNameTxn(i))) {
                            pendingColumns.add(i);
                        }
                    }

                    final LongList columnTopPartitions = new LongList(columnCount);
                    columnTopPartitions.setAll(columnCount, Long.MIN_VALUE);
                    for (int i = 0, n = pendingColumns.size(); i < n; i++) {
                        final int columnIndex = pendingColumns.getQuick(i);
                        columnTopPartitions.setQuick(columnIndex, cvReader.getColumnTopPartitionTimestamp(columnIndex));
                    }

                    final IntList foundColumns = new IntList();
                    final ParquetMetaFileReader parquetMetadata = new ParquetMetaFileReader();
                    for (int i = 0, n = txReader.getPartitionCount(); i < n && pendingColumns.size() > 0; i++) {
                        if (txReader.getPartitionSize(i) < 1) {
                            continue;
                        }
                        foundColumns.clear();
                        path.trimTo(plen);
                        collectNullEvidence(
                                migrationContext, metaMem, txReader, cvReader, columnTopPartitions, parquetMetadata,
                                timestampType, partitionBy, i, columnNames, pendingColumns, foundColumns
                        );
                        for (int j = 0, m = foundColumns.size(); j < m; j++) {
                            final int columnIndex = foundColumns.getQuick(j);
                            setNullFlag(migrationContext, path.trimTo(plen), columnNames.getQuick(columnIndex), cvReader.getSymbolTableNameTxn(columnIndex));
                            pendingColumns.remove(columnIndex);
                        }
                    }
                }
            }
        } finally {
            path.trimTo(plen);
        }
    }

    private static void collectIndexNullEvidence(
            MigrationContext migrationContext,
            MemoryCMR metaMem,
            TxReader txReader,
            ColumnVersionReader cvReader,
            int timestampType,
            int partitionBy,
            int partitionIndex,
            ObjList<String> columnNames,
            IntList pendingColumns,
            IntList foundColumns
    ) {
        final Path path = migrationContext.getTablePath();
        final int plen = path.size();
        final long partitionTimestamp = txReader.getPartitionTimestampByIndex(partitionIndex);
        final long partitionNameTxn = txReader.getPartitionNameTxn(partitionIndex);
        final long partitionSize = txReader.getPartitionSize(partitionIndex);
        try {
            TableUtils.setPathForNativePartition(path, timestampType, partitionBy, partitionTimestamp, partitionNameTxn);
            for (int i = 0, n = pendingColumns.size(); i < n; i++) {
                final int columnIndex = pendingColumns.getQuick(i);
                if (foundColumns.contains(columnIndex) || TableUtils.getColumnIndexType(metaMem, columnIndex) != IndexType.BITMAP) {
                    continue;
                }
                final String columnName = columnNames.getQuick(columnIndex);
                final long columnNameTxn = cvReader.getColumnNameTxn(partitionTimestamp, columnIndex);
                try (BitmapIndexBwdReader indexReader = new BitmapIndexBwdReader(migrationContext.getConfiguration(), path, columnName, columnNameTxn, partitionNameTxn, 0)) {
                    try (RowCursor nullRows = indexReader.getCursor(0, 0, partitionSize - 1)) {
                        if (nullRows.hasNext()) {
                            foundColumns.add(columnIndex);
                        }
                    }
                } catch (CairoException e) {
                    LOG.info().$("could not read symbol index [path=").$(path).$(", column=").$safe(columnName)
                            .$(", error=").$safe(e.getFlyweightMessage()).I$();
                }
            }
        } finally {
            path.trimTo(plen);
        }
    }

    private static void collectNullEvidence(
            MigrationContext migrationContext,
            MemoryCMR metaMem,
            TxReader txReader,
            ColumnVersionReader cvReader,
            LongList columnTopPartitions,
            ParquetMetaFileReader parquetMetadata,
            int timestampType,
            int partitionBy,
            int partitionIndex,
            ObjList<String> columnNames,
            IntList pendingColumns,
            IntList foundColumns
    ) {
        final long partitionTimestamp = txReader.getPartitionTimestampByIndex(partitionIndex);
        final LongList versions = cvReader.getCachedColumnVersionList();
        final int versionsSize = versions.size();
        int recordIndex = versions.binarySearchBlock(ColumnVersionReader.BLOCK_SIZE_MSB, partitionTimestamp, Vect.BIN_SEARCH_SCAN_UP);
        if (recordIndex < 0) {
            recordIndex = versionsSize;
        }
        for (int i = 0, n = pendingColumns.size(); i < n; i++) {
            final int columnIndex = pendingColumns.getQuick(i);
            while (recordIndex < versionsSize
                    && versions.getQuick(recordIndex) == partitionTimestamp
                    && versions.getQuick(recordIndex + ColumnVersionReader.COLUMN_INDEX_OFFSET) < columnIndex) {
                recordIndex += ColumnVersionReader.BLOCK_SIZE;
            }
            final boolean hasRecord = recordIndex < versionsSize
                    && versions.getQuick(recordIndex) == partitionTimestamp
                    && versions.getQuick(recordIndex + ColumnVersionReader.COLUMN_INDEX_OFFSET) == columnIndex;
            final boolean hasColumnTop = hasRecord
                    ? versions.getQuick(recordIndex + ColumnVersionReader.COLUMN_TOP_OFFSET) != 0
                    : columnTopPartitions.getQuick(columnIndex) > partitionTimestamp;
            if (hasColumnTop) {
                foundColumns.add(columnIndex);
            }
        }
        if (foundColumns.size() == pendingColumns.size()) {
            return;
        }
        if (txReader.isPartitionParquet(partitionIndex)) {
            collectParquetNullEvidence(migrationContext, metaMem, txReader, parquetMetadata, timestampType, partitionBy, partitionIndex, pendingColumns, foundColumns);
        } else {
            collectIndexNullEvidence(migrationContext, metaMem, txReader, cvReader, timestampType, partitionBy, partitionIndex, columnNames, pendingColumns, foundColumns);
        }
    }

    private static void collectParquetNullEvidence(
            MigrationContext migrationContext,
            MemoryCMR metaMem,
            TxReader txReader,
            ParquetMetaFileReader parquetMetadata,
            int timestampType,
            int partitionBy,
            int partitionIndex,
            IntList pendingColumns,
            IntList foundColumns
    ) {
        final FilesFacade ff = migrationContext.getFf();
        final Path path = migrationContext.getTablePath();
        final int plen = path.size();
        try {
            TableUtils.setPathForParquetPartitionMetadata(
                    path,
                    timestampType,
                    partitionBy,
                    txReader.getPartitionTimestampByIndex(partitionIndex),
                    txReader.getPartitionNameTxn(partitionIndex)
            );
            final long metaAddr = ParquetMetaFileReader.openAndMapRO(ff, path.$(), parquetMetadata);
            if (metaAddr == 0) {
                return;
            }
            final long metaSize = parquetMetadata.getFileSize();
            try {
                if (!parquetMetadata.resolveFooter(txReader.getPartitionParquetFileSize(partitionIndex))) {
                    return;
                }
                for (int i = 0, n = pendingColumns.size(); i < n; i++) {
                    final int columnIndex = pendingColumns.getQuick(i);
                    if (foundColumns.contains(columnIndex)) {
                        continue;
                    }
                    int parquetColumnIndex = parquetMetadata.getColumnIndexById(columnIndex);
                    if (parquetColumnIndex == -1) {
                        parquetColumnIndex = parquetMetadata.getColumnIndexById(TableUtils.getReplacingChainHead(metaMem, columnIndex));
                    }
                    if (parquetColumnIndex == -1 || parquetMetadata.hasChunkNulls(parquetColumnIndex)) {
                        foundColumns.add(columnIndex);
                    }
                }
            } finally {
                parquetMetadata.clear();
                ff.munmap(metaAddr, metaSize, MemoryTag.MMAP_PARQUET_METADATA_READER);
            }
        } finally {
            path.trimTo(plen);
        }
    }

    private static boolean isNullFlagUnset(MigrationContext migrationContext, Path path, CharSequence columnName, long columnNameTxn) {
        final FilesFacade ff = migrationContext.getFf();
        TableUtils.offsetFileName(path, columnName, columnNameTxn);
        if (!ff.exists(path.$()) || ff.length(path.$()) < SymbolMapWriter.HEADER_SIZE) {
            LOG.error().$("symbol offset file is missing or too short, skipping [path=").$(path).I$();
            return false;
        }
        final long fd = TableUtils.openRO(ff, path.$(), LOG);
        try {
            final long flagMem = migrationContext.getTempMemory(Byte.BYTES);
            if (ff.read(fd, flagMem, Byte.BYTES, SymbolMapWriter.HEADER_NULL_FLAG) != Byte.BYTES) {
                throw CairoException.critical(ff.errno()).put("could not read symbol null flag [path=").put(path).put(']');
            }
            return Unsafe.getByte(flagMem) == 0;
        } finally {
            ff.close(fd);
        }
    }

    private static void setNullFlag(MigrationContext migrationContext, Path path, CharSequence columnName, long columnNameTxn) {
        final FilesFacade ff = migrationContext.getFf();
        TableUtils.offsetFileName(path, columnName, columnNameTxn);
        final long fd = TableUtils.openRW(ff, path.$(), LOG, migrationContext.getConfiguration().getWriterFileOpenOpts());
        try {
            final long flagMem = migrationContext.getTempMemory(Byte.BYTES);
            Unsafe.putByte(flagMem, (byte) 1);
            if (ff.write(fd, flagMem, Byte.BYTES, SymbolMapWriter.HEADER_NULL_FLAG) != Byte.BYTES) {
                throw CairoException.critical(ff.errno()).put("could not write symbol null flag [path=").put(path).put(']');
            }
            LOG.info().$("set symbol null flag [path=").$(path).I$();
        } finally {
            ff.close(fd);
        }
    }
}
