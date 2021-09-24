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
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.*;

class MigrationActions {
    private static final Log LOG = LogFactory.getLog(MigrationActions.class);

    public static void addTblMetaCommitLag(MigrationContext migrationContext) {
        LOG.info().$("configuring default commit lag [table=").$(migrationContext.getTablePath()).I$();
        Path path = migrationContext.getTablePath();
        final FilesFacade ff = migrationContext.getFf();
        path.concat(META_FILE_NAME).$();
        if (!ff.exists(path)) {
            LOG.error().$("meta file does not exist, nothing to migrate [path=").$(path).I$();
            return;
        }
        // Metadata file should already be backed up
        long tempMem = migrationContext.getTempMemory(8);
        Unsafe.getUnsafe().putInt(tempMem, migrationContext.getConfiguration().getMaxUncommittedRows());
        if (ff.write(migrationContext.getMetadataFd(), tempMem, Integer.BYTES, META_OFFSET_MAX_UNCOMMITTED_ROWS) != Integer.BYTES) {
            throw CairoException.instance(ff.errno()).put("Cannot update metadata [path=").put(path).put(']');
        }

        Unsafe.getUnsafe().putLong(tempMem, migrationContext.getConfiguration().getCommitLag());
        if (ff.write(migrationContext.getMetadataFd(), tempMem, Long.BYTES, META_OFFSET_COMMIT_LAG) != Long.BYTES) {
            throw CairoException.instance(ff.errno()).put("Cannot update metadata [path=").put(path).put(']');
        }
    }

    public static void bumpVarColumnIndex(MigrationContext migrationContext) {
        final FilesFacade ff = migrationContext.getFf();
        Path path = migrationContext.getTablePath();
        int plen = path.length();

        path.trimTo(plen).concat(META_FILE_NAME).$();
        try (TableReaderMetadata m = new TableReaderMetadata(ff)) {
            m.of(path, 420);
            final int columnCount = m.getColumnCount();
            try (TxReader txReader = new TxReader(ff, path.trimTo(plen), m.getPartitionBy())) {
                txReader.readUnchecked();
                int partitionCount = txReader.getPartitionCount();
                int partitionBy = m.getPartitionBy();
                if (partitionBy != PartitionBy.NONE) {
                    for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
                        setPathForPartition(path.trimTo(plen), m.getPartitionBy(), txReader.getPartitionTimestamp(partitionIndex), false);
                        int plen2 = path.length();
                        long rowCount = txReader.getPartitionSize(partitionIndex);
                        if (rowCount > 0) {
                            bumpVarColumnIndex0(migrationContext, ff, path, m, columnCount, plen2, rowCount);
                        }
                    }
                } else {
                    path.concat(DEFAULT_PARTITION_NAME);
                    int plen2 = path.length();
                    long rowCount = txReader.getPartitionSize(0);
                    if (rowCount > 0) {
                        bumpVarColumnIndex0(migrationContext, ff, path, m, columnCount, plen2, rowCount);
                    }
                }
            }
        }
    }

    public static void updateColumnTypeIds(MigrationContext migrationContext) {
        LOG.info().$("updating column type IDs [table=").$(migrationContext.getTablePath()).I$();
        final FilesFacade ff = migrationContext.getFf();
        Path path = migrationContext.getTablePath();
        path.concat(META_FILE_NAME).$();

        if (!ff.exists(path)) {
            LOG.error().$("meta file does not exist, nothing to migrate [path=").$(path).I$();
            return;
        }

        // Metadata file should already be backed up
        final MemoryMARW rwMem = migrationContext.getRwMemory();
        rwMem.of(ff, path, ff.getPageSize(), ff.length(path), MemoryTag.NATIVE_DEFAULT);

        // column count
        final int columnCount = rwMem.getInt(TableUtils.META_OFFSET_COUNT);
        rwMem.putInt(TableUtils.META_OFFSET_VERSION, EngineMigration.VERSION_COLUMN_TYPE_ENCODING_CHANGED);

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
            offset += TableUtils.META_COLUMN_DATA_SIZE;
        }
        rwMem.close();
    }

    private static void bumpVarColumnIndex0(MigrationContext migrationContext, FilesFacade ff, Path path, TableReaderMetadata m, int columnCount, int plen2, long rowCount) {
        long mem = migrationContext.getTempMemory();
        for (int i = 0; i < columnCount; i++) {
            int columnType = ColumnType.tagOf(m.getColumnType(i));
            switch (columnType) {
                case ColumnType.STRING: {
                    final long columnTop = readColumnTop(ff, path.trimTo(plen2), m.getColumnName(i), plen2, mem, false);
                    final long columnRowCount = rowCount - columnTop;
                    iFile(path.trimTo(plen2), m.getColumnName(i));
                    long fd = ff.openRW(path);
                    if (fd != -1) {
                        long fileLen = ff.length(fd);
                        long offset = columnRowCount * 8L;
                        if (fileLen < offset) {
                            LOG.error().$("file is too short [path=").$(path).I$();
                        } else {
                            if (ff.allocate(fd, offset + 8)) {
                                if (ff.read(fd, mem, 8, offset - 8L) == 8) {
                                    long dataOffset = Unsafe.getUnsafe().getLong(mem);
                                    // string length
                                    dFile(path.trimTo(plen2), m.getColumnName(i));
                                    long fd2 = ff.openRO(path);
                                    if (fd2 != -1) {
                                        if (ff.read(fd2, mem, 4, dataOffset) == 4) {
                                            long len = Unsafe.getUnsafe().getInt(mem);
                                            if (len == -1) {
                                                dataOffset += 4;
                                            } else {
                                                dataOffset += 4 + len * 2L;
                                            }

                                            // write this value back to index column
                                            Unsafe.getUnsafe().putLong(mem, dataOffset);
                                            // todo: this might fail - fail the migration ?
                                            ff.write(fd, mem, 8, offset);
                                        }
                                        ff.close(fd2);
                                    } else {
                                        LOG.error().$("could not read column file [path=").$(path).I$();
                                    }
                                }
                            } else {
                                LOG.error().$("could not allocate extra 8 bytes [file=").$(path).I$();
                            }
                        }
                        ff.close(fd);
                    } else {
                        LOG.error().$("column file does not exist [path=").$(path).I$();
                    }
                }
                break;
                case ColumnType.BINARY: {
                    final long columnTop = readColumnTop(ff, path.trimTo(plen2), m.getColumnName(i), plen2, mem, false);
                    final long columnRowCount = rowCount - columnTop;
                    iFile(path.trimTo(plen2), m.getColumnName(i));
                    long fd = ff.openRW(path);
                    if (fd != -1) {
                        long fileLen = ff.length(fd);
                        long offset = columnRowCount * 8L;
                        if (fileLen < offset) {
                            LOG.error().$("file is too short [path=").$(path).I$();
                        } else {
                            if (ff.allocate(fd, offset + 8)) {
                                if (ff.read(fd, mem, 8, offset - 8L) == 8) {
                                    long dataOffset = Unsafe.getUnsafe().getLong(mem);
                                    // string length
                                    dFile(path.trimTo(plen2), m.getColumnName(i));
                                    long fd2 = ff.openRO(path);
                                    if (fd2 != -1) {
                                        if (ff.read(fd2, mem, 8, dataOffset) == 8) {
                                            long len = Unsafe.getUnsafe().getLong(mem);
                                            if (len == -1) {
                                                dataOffset += 8;
                                            } else {
                                                dataOffset += 8 + len;
                                            }

                                            // write this value back to index column
                                            Unsafe.getUnsafe().putLong(mem, dataOffset);
                                            // todo: this might fail - fail the migration ?
                                            ff.write(fd, mem, 8, offset);
                                        }
                                        ff.close(fd2);
                                    } else {
                                        LOG.error().$("could not read column file [path=").$(path).I$();
                                    }
                                }
                            } else {
                                LOG.error().$("could not allocate extra 8 bytes [file=").$(path).I$();
                            }
                        }
                        ff.close(fd);
                    } else {
                        LOG.error().$("column file does not exist [path=").$(path).I$();
                    }
                }
                break;
                default:
                    break;
            }
        }
    }

    static void assignTableId(MigrationContext migrationContext) {
        LOG.info().$("assigning table ID [table=").$(migrationContext.getTablePath()).I$();
        long mem = migrationContext.getTempMemory(8);
        FilesFacade ff = migrationContext.getFf();
        Path path = migrationContext.getTablePath();

        LOG.info().$("setting table id in [path=").$(path).I$();
        Unsafe.getUnsafe().putInt(mem, migrationContext.getNextTableId());
        if (ff.write(migrationContext.getMetadataFd(), mem, Integer.BYTES, META_OFFSET_TABLE_ID) == Integer.BYTES) {
            return;
        }
        throw CairoException.instance(ff.errno()).put("Could not update table id [path=").put(path).put(']');
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
        EngineMigration.backupFile(ff, path, migrationContext.getTablePath2(), TXN_FILE_NAME, EngineMigration.VERSION_TX_STRUCT_UPDATE_1 - 1);

        LOG.debug().$("opening for rw [path=").$(path).I$();
        MemoryMARW txMem = migrationContext.createRwMemoryOf(ff, path.$());
        long tempMem8b = migrationContext.getTempMemory(8);

        MemoryARW txFileUpdate = migrationContext.getTempVirtualMem();
        txFileUpdate.jumpTo(0);

        try {
            int symbolsCount = txMem.getInt(EngineMigration.TX_STRUCT_UPDATE_1_OFFSET_MAP_WRITER_COUNT);
            for (int i = 0; i < symbolsCount; i++) {
                long symbolCountOffset = EngineMigration.TX_STRUCT_UPDATE_1_OFFSET_MAP_WRITER_COUNT + (i + 1L) * Integer.BYTES;
                int symDistinctCount = txMem.getInt(symbolCountOffset);
                txFileUpdate.putInt(symDistinctCount);
                txFileUpdate.putInt(symDistinctCount);
            }

            // Set partition segment size as 0 for now
            long partitionSegmentOffset = txFileUpdate.getAppendOffset();
            txFileUpdate.putInt(0);

            int partitionBy = EngineMigration.readIntAtOffset(ff, path, tempMem8b, migrationContext.getMetadataFd());
            if (partitionBy != PartitionBy.NONE) {
                path.trimTo(pathDirLen);
                writeAttachedPartitions(ff, tempMem8b, path, txMem, partitionBy, symbolsCount, txFileUpdate);
            }
            long updateSize = txFileUpdate.getAppendOffset();
            long partitionSegmentSize = updateSize - partitionSegmentOffset - Integer.BYTES;
            txFileUpdate.putInt(partitionSegmentOffset, (int) partitionSegmentSize);

            // Save txFileUpdate to tx file starting at LOCAL_TX_OFFSET_MAP_WRITER_COUNT + 4
            long writeOffset = EngineMigration.TX_STRUCT_UPDATE_1_OFFSET_MAP_WRITER_COUNT + Integer.BYTES;
            txMem.jumpTo(writeOffset);

            for (int i = 0, size = 1; i < size && updateSize > 0; i++) {
                long writeSize = Math.min(updateSize, txFileUpdate.getPageSize());
                txMem.putBlockOfBytes(txFileUpdate.getPageAddress(i), writeSize);
                updateSize -= writeSize;
            }

            assert updateSize == 0;
        } finally {
            txMem.close();
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
        long removedPartitionLo = EngineMigration.TX_STRUCT_UPDATE_1_OFFSET_MAP_WRITER_COUNT + (symbolsCount + 1L) * Integer.BYTES;
        long removedPartitionCount = txMem.getInt(removedPartitionLo);
        long removedPartitionsHi = removedPartitionLo + Long.BYTES * removedPartitionCount;

        for (long offset = removedPartitionLo + Integer.BYTES; offset < removedPartitionsHi; offset += Long.BYTES) {
            long removedPartition = txMem.getLong(offset);
            if (removedPartition == ts) {
                return true;
            }
        }
        return false;
    }
}
