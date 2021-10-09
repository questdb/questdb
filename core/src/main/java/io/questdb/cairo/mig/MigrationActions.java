/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.Vect;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.*;

class MigrationActions {
    public static final long TX_STRUCT_UPDATE_1_META_OFFSET_PARTITION_BY = 4;
    private static final long META_OFFSET_COLUMN_TYPES_606 = 128;
    private static final long META_COLUMN_DATA_SIZE_606 = 16;
    private static final long TX_OFFSET_MAP_WRITER_COUNT_505 = 72;
    private static final Log LOG = LogFactory.getLog(MigrationActions.class);

    public static void mig600(MigrationContext migrationContext) {
        LOG.info().$("configuring default commit lag [table=").$(migrationContext.getTablePath()).I$();
        final Path path = migrationContext.getTablePath();
        final FilesFacade ff = migrationContext.getFf();
        final long tempMem = migrationContext.getTempMemory(8);
        final long fd = migrationContext.getMetadataFd();

        TableUtils.writeIntOrFail(
                ff,
                fd,
                META_OFFSET_MAX_UNCOMMITTED_ROWS,
                migrationContext.getConfiguration().getMaxUncommittedRows(),
                tempMem,
                path
        );

        TableUtils.writeLongOrFail(
                ff,
                fd,
                META_OFFSET_COMMIT_LAG,
                migrationContext.getConfiguration().getCommitLag(),
                tempMem,
                path
        );
    }

    public static void mig605(MigrationContext migrationContext) {
        LOG.info().$("updating column type IDs [table=").$(migrationContext.getTablePath()).I$();
        final FilesFacade ff = migrationContext.getFf();
        Path path = migrationContext.getTablePath();
        path.concat(META_FILE_NAME).$();

        if (!ff.exists(path)) {
            LOG.error().$("meta file does not exist, nothing to migrate [path=").$(path).I$();
            return;
        }

        // Metadata file should already be backed up
        try (final MemoryMARW rwMem = migrationContext.getRwMemory()) {
            rwMem.of(ff, path, ff.getPageSize(), ff.length(path), MemoryTag.NATIVE_DEFAULT);

            // column count
            final int columnCount = rwMem.getInt(TableUtils.META_OFFSET_COUNT);

            long offset = TableUtils.META_OFFSET_COLUMN_TYPES;
            for (int i = 0; i < columnCount; i++) {
                final byte oldTypeId = rwMem.getByte(offset);
                final long oldFlags = rwMem.getLong(offset + 1);
                final int blockCapacity = rwMem.getInt(offset + 1 + 8);
                // column type id is int now
                // we grabbed 3 reserved bytes for extra type info
                // extra for old types is zeros
                rwMem.putInt(offset, oldTypeId == 13 ? 18 : oldTypeId + 1); // ColumnType.VERSION_420 - ColumnType.VERSION_419 = 1 except for BINARY, old 13 new 18
                rwMem.putLong(offset + 4, oldFlags);
                rwMem.putInt(offset + 4 + 8, blockCapacity);
                offset += 16; // old TableUtils.META_COLUMN_DATA_SIZE;
            }
        }
    }

    public static void mig607(MigrationContext migrationContext) {
        final FilesFacade ff = migrationContext.getFf();
        Path path = migrationContext.getTablePath();
        int plen = path.length();


        path.trimTo(plen).concat(META_FILE_NAME).$();
        try (MemoryMARW metaMem = migrationContext.getRwMemory()) {
            metaMem.of(ff, path, ff.getPageSize(), ff.length(path), MemoryTag.NATIVE_DEFAULT);
            final int columnCount = metaMem.getInt(0);
            final int partitionBy = metaMem.getInt(4);
            final long columnNameOffset = prefixedBlockOffset(
                    META_OFFSET_COLUMN_TYPES_606,
                    columnCount,
                    META_COLUMN_DATA_SIZE_606
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
                final int symbolMapCount = txMem.getInt(TX_OFFSET_MAP_WRITER_COUNT_505);
                final long partitionCountOffset = TX_OFFSET_MAP_WRITER_COUNT_505 + 4 + symbolMapCount * 8L;
                int partitionCount = txMem.getInt(partitionCountOffset) / Long.BYTES / LONGS_PER_TX_ATTACHED_PARTITION;
                final long transientRowCount = txMem.getLong(TX_OFFSET_TRANSIENT_ROW_COUNT);


                if (partitionBy != PartitionBy.NONE) {
                    for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
                        final long partitionDataOffset = partitionCountOffset + Integer.BYTES + partitionIndex * 8L * LONGS_PER_TX_ATTACHED_PARTITION;

                        setPathForPartition(
                                path.trimTo(plen),
                                partitionBy,
                                txMem.getLong(partitionDataOffset),
                                false
                        );
                        // the row count may not be stored in _txn file for the last partition
                        // we need to use transient row count instead
                        long rowCount = partitionIndex < partitionCount - 1 ? txMem.getLong(partitionDataOffset + Long.BYTES) : transientRowCount;
                        long txSuffix = txMem.getLong(prefixedBlockOffset(partitionDataOffset, 2, Long.BYTES));
                        if (txSuffix > -1) {
                            txnPartition(path, txSuffix);
                        }
                        mig607(ff, path, migrationContext, metaMem, columnCount, rowCount, columnNameOffset);
                    }
                } else {
                    path.trimTo(plen).concat(DEFAULT_PARTITION_NAME);
                    mig607(ff, path, migrationContext, metaMem, columnCount, transientRowCount, columnNameOffset);
                }

                // update symbol maps
                long tmpMem = migrationContext.getTempMemory();
                int denseSymbolCount = 0;
                long currentColumnNameOffset = columnNameOffset;
                for (int i = 0; i < columnCount; i++) {
                    final CharSequence columnName = metaMem.getStr(currentColumnNameOffset);
                    currentColumnNameOffset += Vm.getStorageLength(columnName.length());

                    if (ColumnType.tagOf(metaMem.getInt(prefixedBlockOffset(META_OFFSET_COLUMN_TYPES_606, i, META_COLUMN_DATA_SIZE_606))) == ColumnType.SYMBOL) {
                        final int symbolCount = txMem.getInt(TX_OFFSET_MAP_WRITER_COUNT_505 + 8 + denseSymbolCount * 8L);
                        final long offset = prefixedBlockOffset(SymbolMapWriter.HEADER_SIZE, symbolCount, 8L);

                        SymbolMapWriter.offsetFileName(path.trimTo(plen), columnName);
                        long fd = TableUtils.openRW(ff, path, LOG);
                        try {
                            long fileLen = ff.length(fd);
                            if (symbolCount > 0) {
                                if (fileLen < offset) {
                                    LOG.error().$("file is too short [path=").$(path).I$();
                                } else {
                                    TableUtils.allocateDiskSpace(ff, fd, offset + 8);
                                    long dataOffset = TableUtils.readLongOrFail(ff, fd, offset - 8L, tmpMem, path);
                                    // string length
                                    SymbolMapWriter.charFileName(path.trimTo(plen), columnName);
                                    long fd2 = TableUtils.openRO(ff, path, LOG);
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
                            Vm.bestEffortClose(ff, LOG, fd, true, offset + 8);
                        }
                        denseSymbolCount++;
                    }
                }
            }
        }
    }

    public static void mig608(MigrationContext migrationContext) {
        //  META_COLUMN_DATA_SIZE = 16 -> 32;
        //  TX_OFFSET_MAP_WRITER_COUNT = 72 -> 128

        final FilesFacade ff = migrationContext.getFf();
        final Path path = migrationContext.getTablePath();
        final int plen = path.length();

        path.concat(META_FILE_NAME).$();
        if (!ff.exists(path)) {
            LOG.error().$("meta file does not exist, nothing to migrate [path=").$(path).I$();
            return;
        }

        // modify metadata
        try (final MemoryMARW rwMem = migrationContext.getRwMemory()) {
            final long thatMetaColumnDataSize = 16;
            final long thisMetaColumnDataSize = 32;

            rwMem.of(ff, path, ff.getPageSize(), ff.length(path), MemoryTag.NATIVE_DEFAULT);

            // column count
            final int columnCount = rwMem.getInt(TableUtils.META_OFFSET_COUNT);
            long offset = TableUtils.META_OFFSET_COLUMN_TYPES;
            // 32L here is TableUtils.META_COLUMN_DATA_SIZE at the time of writing this migration
            long newNameOffset = offset + thisMetaColumnDataSize * columnCount;

            // the intent is to resize the _meta file and move the variable length (names) segment
            // to do that we need to work out size of the variable length segment first
            long oldNameOffset = offset + thatMetaColumnDataSize * columnCount;
            long o = oldNameOffset;
            for (int i = 0; i < columnCount; i++) {
                int len = rwMem.getStrLen(o);
                o += Vm.getStorageLength(len);
            }

            final long nameSegmentLen = o - oldNameOffset;

            // resize the file
            rwMem.extend(newNameOffset + nameSegmentLen);
            // move name segment
            Vect.memmove(rwMem.addressOf(newNameOffset), rwMem.addressOf(oldNameOffset), nameSegmentLen);

            // copy column information in reverse order
            o = offset + thatMetaColumnDataSize * (columnCount - 1);
            long o2 = offset + thisMetaColumnDataSize * (columnCount - 1);
            final Rnd rnd = SharedRandom.getRandom(migrationContext.getConfiguration());
            while (o > offset) {
                rwMem.putInt(o2, rwMem.getInt(o)); // type
                rwMem.putLong(o2 + 4, rwMem.getInt(o + 4)); // flags
                rwMem.putInt(o2 + 12, rwMem.getInt(o + 12)); // index block capacity
                rwMem.putLong(o2 + 20, rnd.nextLong()); // column hash
                o -= thatMetaColumnDataSize;
                o2 -= thisMetaColumnDataSize;
            }
        }

        // update _txn file
        path.trimTo(plen).concat(TXN_FILE_NAME).$();
        if (!ff.exists(path)) {
            LOG.error().$("tx file does not exist, nothing to migrate [path=").$(path).I$();
            return;
        }
        EngineMigration.backupFile(ff, path, migrationContext.getTablePath2(), TXN_FILE_NAME, 422);

        LOG.debug().$("opening for rw [path=").$(path).I$();
        try (MemoryMARW txMem = migrationContext.createRwMemoryOf(ff, path.$())) {

            // calculate size of the _txn file
            final long thatTxOffsetMapWriterCount = 72;
            final long thisTxOffsetMapWriterCount = 128;
            final int longsPerAttachedPartition = 4;

            int symbolCount = txMem.getInt(thatTxOffsetMapWriterCount);
            int partitionTableSize = txMem.getInt(thatTxOffsetMapWriterCount + 4 + symbolCount * 8L) * 8 * longsPerAttachedPartition;

            // resize existing file:
            // thisTxOffsetMapWriterCount + symbolCount + symbolData + partitionTableEntryCount + partitionTableSize
            long thatSize = thatTxOffsetMapWriterCount + 4 + symbolCount * 8L + 4L + partitionTableSize;
            long thisSize = thisTxOffsetMapWriterCount + 4 + symbolCount * 8L + 4L + partitionTableSize;
            txMem.extend(thisSize);
            Vect.memmove(txMem.addressOf(thisTxOffsetMapWriterCount), txMem.addressOf(thatTxOffsetMapWriterCount), thatSize - thatTxOffsetMapWriterCount);

            // zero out reserved area
            Vect.memset(txMem.addressOf(thatTxOffsetMapWriterCount), thisTxOffsetMapWriterCount - thatTxOffsetMapWriterCount, 0);
        }
    }

    private static void mig607(
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
                                prefixedBlockOffset(META_OFFSET_COLUMN_TYPES_606, i, META_COLUMN_DATA_SIZE_606)
                        )
                );
                final CharSequence columnName = metaMem.getStr(currentColumnNameOffset);
                currentColumnNameOffset += Vm.getStorageLength(columnName);
                if (columnType == ColumnType.STRING || columnType == ColumnType.BINARY) {
                    final long columnTop = readColumnTop(
                            ff,
                            path.trimTo(plen2),
                            columnName,
                            plen2,
                            mem,
                            false
                    );
                    final long columnRowCount = rowCount - columnTop;
                    long offset = columnRowCount * 8L;
                    iFile(path.trimTo(plen2), columnName);
                    long fd = TableUtils.openRW(ff, path, LOG);
                    try {
                        long fileLen = ff.length(fd);

                        if (fileLen < offset) {
                            throw CairoException.instance(0).put("file is too short [path=").put(path).put("]");
                        }

                        TableUtils.allocateDiskSpace(ff, fd, offset + 8);
                        long dataOffset = TableUtils.readLongOrFail(ff, fd, offset - 8L, mem, path);
                        dFile(path.trimTo(plen2), columnName);
                        final long fd2 = TableUtils.openRO(ff, path, LOG);
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
                                    dataOffset += prefixedBlockOffset(4, 2, len);
                                }

                            }
                        } finally {
                            ff.close(fd2);
                        }
                        TableUtils.writeLongOrFail(ff, fd, offset, dataOffset, mem, path);
                    } finally {
                        Vm.bestEffortClose(ff, LOG, fd, true, offset + 8);
                    }
                }
            }
        }
    }

    private static long prefixedBlockOffset(long prefix, long index, long blockSize) {
        return prefix + index * blockSize;
    }

    static void mig505(MigrationContext migrationContext) {
        LOG.info().$("assigning table ID [table=").$(migrationContext.getTablePath()).I$();
        final long mem = migrationContext.getTempMemory(8);
        final FilesFacade ff = migrationContext.getFf();
        final Path path = migrationContext.getTablePath();
        final long fd = migrationContext.getMetadataFd();

        LOG.info().$("setting table id in [path=").$(path).I$();
        TableUtils.writeIntOrFail(
                ff,
                fd,
                META_OFFSET_TABLE_ID,
                migrationContext.getNextTableId(),
                mem,
                path
        );
    }

    static void rebuildTransactionFile(MigrationContext migrationContext) {
        // Update transaction file
        // Before there was 1 int per symbol and list of removed partitions
        // Now there is 2 ints per symbol and 4 longs per each non-removed partition

        LOG.info().$("rebuilding tx file [table=").$(migrationContext.getTablePath()).I$();
        Path path = migrationContext.getTablePath();
        final FilesFacade ff = migrationContext.getFf();
        int pathDirLen = path.length();

        path.concat(TXN_FILE_NAME).$();
        if (!ff.exists(path)) {
            LOG.error().$("tx file does not exist, nothing to migrate [path=").$(path).I$();
            return;
        }

        EngineMigration.backupFile(
                ff,
                path,
                migrationContext.getTablePath2(),
                TXN_FILE_NAME,
                417
        );

        LOG.debug().$("opening for rw [path=").$(path).I$();
        try (MemoryMARW txMem = migrationContext.createRwMemoryOf(ff, path.$())) {
            long tempMem8b = migrationContext.getTempMemory(8);

            MemoryARW txFileUpdate = migrationContext.getTempVirtualMem();
            txFileUpdate.jumpTo(0);

            int symbolColumnCount = txMem.getInt(TX_OFFSET_MAP_WRITER_COUNT_505);
            for (int i = 0; i < symbolColumnCount; i++) {
                final int symbolCount = txMem.getInt(
                        prefixedBlockOffset(
                                TX_OFFSET_MAP_WRITER_COUNT_505,
                                i + 1L,
                                Integer.BYTES
                        )
                );
                txFileUpdate.putInt(symbolCount);
                txFileUpdate.putInt(symbolCount);
            }

            // Set partition segment size as 0 for now
            long partitionSegmentOffset = txFileUpdate.getAppendOffset();
            txFileUpdate.putInt(0);

            final int partitionBy = TableUtils.readIntOrFail(
                    ff,
                    migrationContext.getMetadataFd(),
                    TX_STRUCT_UPDATE_1_META_OFFSET_PARTITION_BY,
                    tempMem8b,
                    path
            );
            if (partitionBy != PartitionBy.NONE) {
                path.trimTo(pathDirLen);
                writeAttachedPartitions(ff, tempMem8b, path, txMem, partitionBy, symbolColumnCount, txFileUpdate);
            }
            long updateSize = txFileUpdate.getAppendOffset();
            long partitionSegmentSize = updateSize - partitionSegmentOffset - Integer.BYTES;
            txFileUpdate.putInt(partitionSegmentOffset, (int) partitionSegmentSize);

            // Save txFileUpdate to tx file starting at LOCAL_TX_OFFSET_MAP_WRITER_COUNT + 4
            long writeOffset = TX_OFFSET_MAP_WRITER_COUNT_505 + Integer.BYTES;
            txMem.jumpTo(writeOffset);

            for (int i = 0, size = 1; i < size && updateSize > 0; i++) {
                long writeSize = Math.min(updateSize, txFileUpdate.getPageSize());
                txMem.putBlockOfBytes(txFileUpdate.getPageAddress(i), writeSize);
                updateSize -= writeSize;
            }

            assert updateSize == 0;
        }
    }

    private static void writeAttachedPartitions(
            FilesFacade ff,
            long tempMem8b,
            Path path,
            MemoryMARW txMem,
            int partitionBy,
            int symbolsCount,
            MemoryARW writeTo
    ) {
        int rootLen = path.length();

        long minTimestamp = txMem.getLong(TX_OFFSET_MIN_TIMESTAMP);
        long maxTimestamp = txMem.getLong(TX_OFFSET_MAX_TIMESTAMP);
        long transientCount = txMem.getLong(TX_OFFSET_TRANSIENT_ROW_COUNT);

        Timestamps.TimestampFloorMethod timestampFloorMethod = getPartitionFloor(partitionBy);
        Timestamps.TimestampAddMethod timestampAddMethod = getPartitionAdd(partitionBy);

        final long tsLimit = timestampFloorMethod.floor(maxTimestamp);
        for (long ts = timestampFloorMethod.floor(minTimestamp); ts < tsLimit; ts = timestampAddMethod.calculate(ts, 1)) {
            path.trimTo(rootLen);
            setPathForPartition(path, partitionBy, ts, false);
            if (ff.exists(path.concat(EngineMigration.TX_STRUCT_UPDATE_1_ARCHIVE_FILE_NAME).$())) {
                if (!removedPartitionsIncludes(ts, txMem, symbolsCount)) {
                    long partitionSize = TableUtils.readLongAtOffset(ff, path, tempMem8b, 0);

                    // Update tx file with 4 longs per partition
                    writeTo.putLong(ts);
                    writeTo.putLong(partitionSize);
                    writeTo.putLong(-1L);
                    writeTo.putLong(0L);
                }
            }
        }
        // last partition
        writeTo.putLong(tsLimit);
        writeTo.putLong(transientCount);
        writeTo.putLong(-1);
        writeTo.putLong(0);
    }

    private static boolean removedPartitionsIncludes(long ts, MemoryR txMem, int symbolsCount) {
        long removedPartitionLo = prefixedBlockOffset(
                TX_OFFSET_MAP_WRITER_COUNT_505,
                symbolsCount + 1L,
                Integer.BYTES
        );
        long removedPartitionCount = txMem.getInt(removedPartitionLo);
        long removedPartitionsHi = prefixedBlockOffset(removedPartitionLo, Long.BYTES, removedPartitionCount);

        for (long offset = removedPartitionLo + Integer.BYTES; offset < removedPartitionsHi; offset += Long.BYTES) {
            long removedPartition = txMem.getLong(offset);
            if (removedPartition == ts) {
                return true;
            }
        }
        return false;
    }

}
