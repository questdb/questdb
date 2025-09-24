/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;

final class Mig506 {
    private static final String TXN_FILE_NAME = "_txn";
    private static final long TX_OFFSET_TRANSIENT_ROW_COUNT = 8;
    private static final long TX_OFFSET_FIXED_ROW_COUNT_64 = TX_OFFSET_TRANSIENT_ROW_COUNT + 8;
    private static final long TX_OFFSET_MIN_TIMESTAMP = TX_OFFSET_FIXED_ROW_COUNT_64 + 8;
    private static final long TX_OFFSET_MAX_TIMESTAMP = TX_OFFSET_MIN_TIMESTAMP + 8;
    private static final String TX_STRUCT_UPDATE_1_ARCHIVE_FILE_NAME = "_archive";
    private static final long TX_STRUCT_UPDATE_1_META_OFFSET_PARTITION_BY = 4;

    private static boolean removedPartitionsIncludes(long ts, MemoryR txMem, int symbolsCount) {
        long removedPartitionLo = MigrationActions.prefixedBlockOffset(
                MigrationActions.TX_OFFSET_MAP_WRITER_COUNT_505,
                symbolsCount + 1L,
                Integer.BYTES
        );
        long removedPartitionCount = txMem.getInt(removedPartitionLo);
        long removedPartitionsHi = MigrationActions.prefixedBlockOffset(removedPartitionLo, Long.BYTES, removedPartitionCount);

        for (long offset = removedPartitionLo + Integer.BYTES; offset < removedPartitionsHi; offset += Long.BYTES) {
            long removedPartition = txMem.getLong(offset);
            if (removedPartition == ts) {
                return true;
            }
        }
        return false;
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
        int rootLen = path.size();

        long minTimestamp = txMem.getLong(TX_OFFSET_MIN_TIMESTAMP);
        long maxTimestamp = txMem.getLong(TX_OFFSET_MAX_TIMESTAMP);
        long transientCount = txMem.getLong(TX_OFFSET_TRANSIENT_ROW_COUNT);

        final TimestampDriver.TimestampFloorMethod partitionFloorMethod = PartitionBy.getPartitionFloorMethod(ColumnType.TIMESTAMP_MICRO, partitionBy);
        assert partitionFloorMethod != null;

        final TimestampDriver.PartitionAddMethod partitionAddMethod = PartitionBy.getPartitionAddMethod(ColumnType.TIMESTAMP_MICRO, partitionBy);
        assert partitionAddMethod != null;

        final long tsLimit = partitionFloorMethod.floor(maxTimestamp);

        for (long ts = partitionFloorMethod.floor(minTimestamp); ts < tsLimit; ts = partitionAddMethod.calculate(ts, 1)) {
            path.trimTo(rootLen);
            TableUtils.setPathForNativePartition(path, ColumnType.TIMESTAMP_MICRO, partitionBy, ts, -1);
            if (ff.exists(path.concat(TX_STRUCT_UPDATE_1_ARCHIVE_FILE_NAME).$())) {
                if (!removedPartitionsIncludes(ts, txMem, symbolsCount)) {
                    long partitionSize = TableUtils.readLongAtOffset(ff, path.$(), tempMem8b, 0);

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

    static void migrate(MigrationContext migrationContext) {
        // Update transaction file
        // Before there was 1 int per symbol and list of removed partitions
        // Now there is 2 ints per symbol and 4 longs per each non-removed partition

        MigrationActions.LOG.info().$("rebuilding tx file [table=").$(migrationContext.getTablePath()).I$();
        Path path = migrationContext.getTablePath();
        final FilesFacade ff = migrationContext.getFf();
        int pathDirLen = path.size();

        path.concat(TXN_FILE_NAME);
        if (!ff.exists(path.$())) {
            MigrationActions.LOG.error().$("tx file does not exist, nothing to migrate [path=").$(path).I$();
            return;
        }

        EngineMigration.backupFile(
                ff,
                path,
                migrationContext.getTablePath2(),
                TXN_FILE_NAME,
                417
        );

        MigrationActions.LOG.debug().$("opening for rw [path=").$(path).I$();
        try (MemoryMARW txMem = migrationContext.createRwMemoryOf(ff, path.$())) {
            long tempMem8b = migrationContext.getTempMemory(8);

            MemoryARW txFileUpdate = migrationContext.getTempVirtualMem();
            txFileUpdate.jumpTo(0);

            int symbolColumnCount = txMem.getInt(MigrationActions.TX_OFFSET_MAP_WRITER_COUNT_505);
            for (int i = 0; i < symbolColumnCount; i++) {
                final int symbolCount = txMem.getInt(
                        MigrationActions.prefixedBlockOffset(
                                MigrationActions.TX_OFFSET_MAP_WRITER_COUNT_505,
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
            if (PartitionBy.isPartitioned(partitionBy)) {
                path.trimTo(pathDirLen);
                writeAttachedPartitions(ff, tempMem8b, path, txMem, partitionBy, symbolColumnCount, txFileUpdate);
            }
            long updateSize = txFileUpdate.getAppendOffset();
            long partitionSegmentSize = updateSize - partitionSegmentOffset - Integer.BYTES;
            txFileUpdate.putInt(partitionSegmentOffset, (int) partitionSegmentSize);

            // Save txFileUpdate to tx file starting at LOCAL_TX_OFFSET_MAP_WRITER_COUNT + 4
            long writeOffset = MigrationActions.TX_OFFSET_MAP_WRITER_COUNT_505 + Integer.BYTES;
            txMem.jumpTo(writeOffset);

            for (int i = 0, size = 1; i < size && updateSize > 0; i++) {
                long writeSize = Math.min(updateSize, txFileUpdate.getPageSize());
                txMem.putBlockOfBytes(txFileUpdate.getPageAddress(i), writeSize);
                updateSize -= writeSize;
            }

            assert updateSize == 0;
        }
    }
}
