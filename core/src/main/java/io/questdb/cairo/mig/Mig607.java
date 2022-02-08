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

package io.questdb.cairo.mig;

import io.questdb.cairo.*;
import io.questdb.cairo.vm.MemoryCMARWImpl;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Path;

final class Mig607 {
    private static final String TXN_FILE_NAME = "_txn";
    private static final String META_FILE_NAME = "_meta";
    private static final String DEFAULT_PARTITION_NAME = "default";

    private static final long TX_OFFSET_TRANSIENT_ROW_COUNT = 8;
    private static final int LONGS_PER_TX_ATTACHED_PARTITION = 4;

    public static void migrate(
            FilesFacade ff,
            Path path,
            MigrationContext migrationContext,
            MemoryMARW metaMem,
            int columnCount,
            long rowCount,
            long columnNameOffset
    ) {
        final int plen2 = path.length();
        if (rowCount > 0) {
            long mem = migrationContext.getTempMemory();
            long currentColumnNameOffset = columnNameOffset;
            for (int i = 0; i < columnCount; i++) {
                final int columnType = ColumnType.tagOf(
                        metaMem.getInt(
                                MigrationActions.prefixedBlockOffset(MigrationActions.META_OFFSET_COLUMN_TYPES_606, i, MigrationActions.META_COLUMN_DATA_SIZE_606)
                        )
                );
                final CharSequence columnName = metaMem.getStr(currentColumnNameOffset);
                currentColumnNameOffset += Vm.getStorageLength(columnName);
                if (columnType == ColumnType.STRING || columnType == ColumnType.BINARY) {
                    final long columnTop = TableUtils.readColumnTop(
                            ff,
                            path.trimTo(plen2),
                            columnName,
                            plen2,
                            false
                    );
                    final long columnRowCount = rowCount - columnTop;
                    long offset = columnRowCount * 8L;
                    TableUtils.iFile(path.trimTo(plen2), columnName);
                    long fd = TableUtils.openRW(ff, path, MigrationActions.LOG);
                    try {
                        long fileLen = ff.length(fd);

                        if (fileLen < offset) {
                            throw CairoException.instance(0).put("file is too short [path=").put(path).put("]");
                        }

                        TableUtils.allocateDiskSpace(ff, fd, offset + 8);
                        long dataOffset = TableUtils.readLongOrFail(ff, fd, offset - 8L, mem, path);
                        TableUtils.dFile(path.trimTo(plen2), columnName);
                        final long fd2 = TableUtils.openRO(ff, path, MigrationActions.LOG);
                        try {
                            if (columnType == ColumnType.BINARY) {
                                long len = TableUtils.readLongOrFail(ff, fd2, dataOffset, mem, path);
                                if (len == -1) {
                                    dataOffset += 8;
                                } else {
                                    dataOffset += 8 + len;
                                }
                            } else {
                                long len = TableUtils.readIntOrFail(ff, fd2, dataOffset, mem, path);
                                if (len == -1) {
                                    dataOffset += 4;
                                } else {
                                    dataOffset += MigrationActions.prefixedBlockOffset(4, 2, len);
                                }

                            }
                        } finally {
                            ff.close(fd2);
                        }
                        TableUtils.writeLongOrFail(ff, fd, offset, dataOffset, mem, path);
                    } finally {
                        Vm.bestEffortClose(ff, MigrationActions.LOG, fd, true, offset + 8);
                    }
                }
            }
        }
    }

    public static void txnPartition(CharSink path, long txn) {
        path.put('.').put(txn);
    }

    private static void trimFile(FilesFacade ff, Path path, long size) {
        long fd = TableUtils.openFileRWOrFail(ff, path);
        if (!ff.truncate(fd, size)) {
            // This should never happens on migration but better to be on safe side anyway
            throw CairoException.instance(ff.errno()).put("Cannot trim to size [file=").put(path).put(']');
        }
        if (!ff.close(fd)) {
            // This should never happens on migration but better to be on safe side anyway
            throw CairoException.instance(ff.errno()).put("Cannot close [file=").put(path).put(']');
        }
    }

    static void migrate(MigrationContext migrationContext) {
        final FilesFacade ff = migrationContext.getFf();
        Path path = migrationContext.getTablePath();
        int plen = path.length();

        path.trimTo(plen).concat(META_FILE_NAME).$();
        long metaFileSize;
        long txFileSize;
        try (MemoryMARW metaMem = migrationContext.getRwMemory()) {
            metaMem.of(ff, path, ff.getPageSize(), ff.length(path), MemoryTag.NATIVE_DEFAULT);
            final int columnCount = metaMem.getInt(0);
            final int partitionBy = metaMem.getInt(4);
            final long columnNameOffset = MigrationActions.prefixedBlockOffset(
                    MigrationActions.META_OFFSET_COLUMN_TYPES_606,
                    columnCount,
                    MigrationActions.META_COLUMN_DATA_SIZE_606
            );

            try (MemoryMARW txMem = new MemoryCMARWImpl(
                    ff,
                    path.trimTo(plen).concat(TXN_FILE_NAME).$(),
                    ff.getPageSize(),
                    ff.length(path),
                    MemoryTag.NATIVE_DEFAULT)
            ) {
                // this is a variable length file; we need to count of symbol maps before we get to the partition
                // table data
                final int symbolMapCount = txMem.getInt(MigrationActions.TX_OFFSET_MAP_WRITER_COUNT_505);
                final long partitionCountOffset = MigrationActions.TX_OFFSET_MAP_WRITER_COUNT_505 + 4 + symbolMapCount * 8L;
                int partitionCount = txMem.getInt(partitionCountOffset) / Long.BYTES / LONGS_PER_TX_ATTACHED_PARTITION;
                final long transientRowCount = txMem.getLong(TX_OFFSET_TRANSIENT_ROW_COUNT);


                if (PartitionBy.isPartitioned(partitionBy)) {
                    for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
                        final long partitionDataOffset = partitionCountOffset + Integer.BYTES + partitionIndex * 8L * LONGS_PER_TX_ATTACHED_PARTITION;

                        TableUtils.setPathForPartition(
                                path.trimTo(plen),
                                partitionBy,
                                txMem.getLong(partitionDataOffset),
                                false
                        );
                        // the row count may not be stored in _txn file for the last partition
                        // we need to use transient row count instead
                        long rowCount = partitionIndex < partitionCount - 1 ? txMem.getLong(partitionDataOffset + Long.BYTES) : transientRowCount;
                        long txSuffix = txMem.getLong(MigrationActions.prefixedBlockOffset(partitionDataOffset, 2, Long.BYTES));
                        if (txSuffix > -1) {
                            txnPartition(path, txSuffix);
                        }
                        migrate(ff, path, migrationContext, metaMem, columnCount, rowCount, columnNameOffset);
                    }
                } else {
                    path.trimTo(plen).concat(DEFAULT_PARTITION_NAME);
                    migrate(ff, path, migrationContext, metaMem, columnCount, transientRowCount, columnNameOffset);
                }

                // update symbol maps
                long tmpMem = migrationContext.getTempMemory();
                int denseSymbolCount = 0;
                long currentColumnNameOffset = columnNameOffset;
                for (int i = 0; i < columnCount; i++) {
                    final CharSequence columnName = metaMem.getStr(currentColumnNameOffset);
                    currentColumnNameOffset += Vm.getStorageLength(columnName.length());

                    if (ColumnType.tagOf(
                            metaMem.getInt(
                                    MigrationActions.prefixedBlockOffset(
                                            MigrationActions.META_OFFSET_COLUMN_TYPES_606,
                                            i,
                                            MigrationActions.META_COLUMN_DATA_SIZE_606
                                    )
                            )) == ColumnType.SYMBOL
                    ) {
                        final int symbolCount = txMem.getInt(MigrationActions.TX_OFFSET_MAP_WRITER_COUNT_505 + 8 + denseSymbolCount * 8L);
                        final long offset = MigrationActions.prefixedBlockOffset(SymbolMapWriter.HEADER_SIZE, symbolCount, 8L);

                        SymbolMapWriter.offsetFileName(path.trimTo(plen), columnName);
                        long fd = TableUtils.openRW(ff, path, MigrationActions.LOG);
                        try {
                            long fileLen = ff.length(fd);
                            if (symbolCount > 0) {
                                if (fileLen < offset) {
                                    MigrationActions.LOG.error().$("file is too short [path=").$(path).I$();
                                } else {
                                    TableUtils.allocateDiskSpace(ff, fd, offset + 8);
                                    long dataOffset = TableUtils.readLongOrFail(ff, fd, offset - 8L, tmpMem, path);
                                    // string length
                                    SymbolMapWriter.charFileName(path.trimTo(plen), columnName);
                                    long fd2 = TableUtils.openRO(ff, path, MigrationActions.LOG);
                                    try {
                                        long len = TableUtils.readIntOrFail(ff, fd2, dataOffset, tmpMem, path);
                                        if (len == -1) {
                                            dataOffset += 4;
                                        } else {
                                            dataOffset += 4 + len * 2L;
                                        }
                                        TableUtils.writeLongOrFail(ff, fd, offset, dataOffset, tmpMem, path);
                                    } finally {
                                        ff.close(fd2);
                                    }
                                }
                            }
                        } finally {
                            Vm.bestEffortClose(ff, MigrationActions.LOG, fd, true, offset + 8);
                        }
                        denseSymbolCount++;
                    }
                }
                txFileSize = txMem.getAppendOffset();
            }
            metaFileSize = metaMem.getAppendOffset();
        }

        // This migration when written originally used implementation of MemoryMARW which truncated files to size on close
        // MemoryMARW now truncate to page size. To test old migrations here we simulate the migration as it is originally released
        // So trim TX and META files to their sizes
        path.trimTo(plen).concat(META_FILE_NAME).$();
        trimFile(ff, path, metaFileSize);

        path.trimTo(plen).concat(TXN_FILE_NAME).$();
        trimFile(ff, path, txFileSize);
    }
}
