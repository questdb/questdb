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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Vect;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.setPathForNativePartition;
import static io.questdb.cairo.mig.MigrationUtils.openFileSafe;

public class Mig620 {
    private static final int COLUMN_VERSION_FILE_HEADER_SIZE_MIG = 40;
    private static final String COLUMN_VERSION_FILE_NAME_MIG = "_cv";
    private static final long CV_COL_TOP_DEFAULT_PARTITION_MIG = Long.MIN_VALUE;
    private static final int CV_OFFSET_VERSION_64 = 0;
    private static final int CV_OFFSET_OFFSET_A_64 = CV_OFFSET_VERSION_64 + 8;
    private static final int CV_OFFSET_SIZE_A_64 = CV_OFFSET_OFFSET_A_64 + 8;
    private static final int CV_OFFSET_OFFSET_B_64 = CV_OFFSET_SIZE_A_64 + 8;
    private static final int CV_OFFSET_SIZE_B_64 = CV_OFFSET_OFFSET_B_64 + 8;
    private static final int CV_HEADER_SIZE = CV_OFFSET_SIZE_B_64 + 8;
    private static final Log LOG = LogFactory.getLog(EngineMigration.class);
    private static final long META_COLUMN_DATA_SIZE_MIG = 32;
    private static final String META_FILE_NAME_MIG = "_meta";
    private static final long META_OFFSET_COLUMN_TYPES_MIG = 128;
    private static final long META_OFFSET_COUNT_MIG = 0;
    private static final long META_OFFSET_PARTITION_BY_MIG = 4;
    private static final long PARTITION_NAME_TX_OFFSET_MIG = 2;
    private static final String TXN_FILE_NAME_MIG = "_txn";
    private static final long TXN_OFFSET_MIG = 0;
    private static final int TX_BASE_HEADER_SECTION_PADDING_MIG = 12; // Add some free space into header for future use
    private static final long TX_BASE_OFFSET_VERSION_MIG = 0;
    private static final long TX_BASE_OFFSET_A_MIG = TX_BASE_OFFSET_VERSION_MIG + 8;
    private static final long TX_BASE_OFFSET_SYMBOLS_SIZE_A_MIG = TX_BASE_OFFSET_A_MIG + 4;
    private static final long TX_BASE_OFFSET_PARTITIONS_SIZE_A_MIG = TX_BASE_OFFSET_SYMBOLS_SIZE_A_MIG + 4;
    private static final long TX_BASE_OFFSET_B_MIG = TX_BASE_OFFSET_PARTITIONS_SIZE_A_MIG + 4 + TX_BASE_HEADER_SECTION_PADDING_MIG;
    private static final long TX_BASE_OFFSET_SYMBOLS_SIZE_B_MIG = TX_BASE_OFFSET_B_MIG + 4;
    private static final long TX_BASE_OFFSET_PARTITIONS_SIZE_B_MIG = TX_BASE_OFFSET_SYMBOLS_SIZE_B_MIG + 4;
    private static final int TX_BASE_HEADER_SIZE_MIG = (int) Math.max(TX_BASE_OFFSET_PARTITIONS_SIZE_B_MIG + 4 + TX_BASE_HEADER_SECTION_PADDING_MIG, 64);
    private static final long TX_DEFAULT_PARTITION_TIMESTAMP_MIG = 0L;
    private static final long TX_OFFSET_COLUMN_VERSION_MIG = 64;
    private static final long TX_OFFSET_MAP_WRITER_COUNT_MIG = 128;
    private static final long TX_OFFSET_TRUNCATE_VERSION_MIG = 72;

    private static void createColumnVersionFile(MemoryMARW txMemory, long partitionSizeOffset, int partitionTableSize, MigrationContext migrationContext, Path path, int pathLen) {
        final FilesFacade ff = migrationContext.getFf();

        try (MemoryMARW cvMemory = Vm.getCMARWInstance(
                ff,
                path.trimTo(pathLen).concat(COLUMN_VERSION_FILE_NAME_MIG).$(),
                Files.PAGE_SIZE,
                COLUMN_VERSION_FILE_HEADER_SIZE_MIG,
                MemoryTag.NATIVE_MIG_MMAP,
                CairoConfiguration.O_NONE
        )) {
            cvMemory.extend(COLUMN_VERSION_FILE_HEADER_SIZE_MIG);
            cvMemory.jumpTo(COLUMN_VERSION_FILE_HEADER_SIZE_MIG);
            cvMemory.zero();

            try (MemoryMARW metaMem = openFileSafe(
                    ff,
                    path.trimTo(pathLen).concat(META_FILE_NAME_MIG).$(),
                    META_OFFSET_COLUMN_TYPES_MIG
            )) {
                int partitionBy = metaMem.getInt(META_OFFSET_PARTITION_BY_MIG);
                ObjList<String> columnNames = readColumNames(metaMem);
                int columnCount = columnNames.size();
                LongList columnTops = readColumnTops(columnCount, partitionBy, partitionSizeOffset, partitionTableSize, txMemory, ff, path, pathLen, columnNames);
                path.trimTo(pathLen);
                long sizeBytes = writeColumnVersion(path, columnTops, columnCount, columnNames, cvMemory);
                cvMemory.putLong(CV_OFFSET_OFFSET_A_64, CV_HEADER_SIZE);
                cvMemory.putLong(CV_OFFSET_SIZE_A_64, sizeBytes);
                cvMemory.jumpTo(CV_HEADER_SIZE + sizeBytes);
            }
        }
    }

    private static LPSZ dFile(Path path, CharSequence columnName) {
        return path.concat(columnName).put('.').put('d').$();
    }

    private static long getColumnNameOffset(int columnCount) {
        return META_OFFSET_COLUMN_TYPES_MIG + columnCount * META_COLUMN_DATA_SIZE_MIG;
    }

    private static void migrateTxn(MemoryMARW txMemory, int symbolCount, int partitionTableSize, long existingTotalSize, long txn) {
        txMemory.putInt(TX_OFFSET_COLUMN_VERSION_MIG, 0);
        txMemory.putInt(TX_OFFSET_TRUNCATE_VERSION_MIG, 0);

        long pageAddress = txMemory.getPageAddress(0);
        Vect.memmove(pageAddress + TX_BASE_HEADER_SIZE_MIG, pageAddress, existingTotalSize);
        Vect.memset(pageAddress, TX_BASE_HEADER_SIZE_MIG, 0);

        txMemory.putLong(TX_BASE_OFFSET_VERSION_MIG, txn);

        boolean currentIsA = txn % 2 == 0;
        long offsetOffset = currentIsA ? TX_BASE_OFFSET_A_MIG : TX_BASE_OFFSET_B_MIG;
        long symbolSizeOffset = currentIsA ? TX_BASE_OFFSET_SYMBOLS_SIZE_A_MIG : TX_BASE_OFFSET_SYMBOLS_SIZE_B_MIG;
        long partitionsSizeOffset = currentIsA ? TX_BASE_OFFSET_PARTITIONS_SIZE_A_MIG : TX_BASE_OFFSET_PARTITIONS_SIZE_B_MIG;

        txMemory.putInt(offsetOffset, TX_BASE_HEADER_SIZE_MIG);
        txMemory.putInt(symbolSizeOffset, symbolCount * 8);
        txMemory.putInt(partitionsSizeOffset, partitionTableSize);

        txMemory.jumpTo(TX_BASE_HEADER_SIZE_MIG + existingTotalSize);
    }


    private static long openRO(FilesFacade ff, LPSZ path) {
        final long fd = ff.openRO(path);
        if (fd > -1) {
            Mig620.LOG.debug().$("open [file=").$(path).$(", fd=").$(fd).$(']').$();
            return fd;
        }
        throw CairoException.critical(ff.errno()).put("could not open read-only [file=").put(path).put(']');
    }

    private static ObjList<String> readColumNames(MemoryMARW metaMem) {
        ObjList<String> columnNames = new ObjList<>();
        final int columnCount = metaMem.getInt(META_OFFSET_COUNT_MIG);
        long offset = getColumnNameOffset(columnCount);
        for (int metaIndex = 0; metaIndex < columnCount; metaIndex++) {
            String name = Chars.toString(metaMem.getStrA(offset));
            columnNames.add(name);
            offset += Vm.getStorageLength(name);
        }
        return columnNames;
    }

    /**
     * Reads 8 bytes from "top" file.
     *
     * @param ff   files facade, - intermediary to intercept OS file system calls.
     * @param path path has to be set to location of "top" file, excluding file name. Zero terminated string.
     * @param name name of top file
     * @param plen path length to truncate "path" back to, path is reusable.
     * @return number of rows column doesn't have when column was added to table that already had data.
     */
    private static long readColumnTop(FilesFacade ff, Path path, CharSequence name, int plen) {
        try {
            if (ff.exists(topFile(path, name))) {
                final long fd = openRO(ff, path.$());
                try {
                    long n;
                    if ((n = ff.readNonNegativeLong(fd, 0)) < 0) {
                        return 0L;
                    }
                    return n;
                } finally {
                    ff.close(fd);
                }
            }
            return 0L;
        } finally {
            path.trimTo(plen);
        }
    }

    private static LongList readColumnTops(
            int columnCount,
            int partitionBy,
            long partitionSizeOffset,
            int partitionTableSize,
            MemoryMARW txMemory,
            FilesFacade ff,
            Path path,
            int pathLen,
            ObjList<String> columnNames
    ) {
        if (!PartitionBy.isPartitioned(partitionBy)) {
            LongList result = new LongList();
            readColumnTopsForPartition(result, columnNames, columnCount, partitionBy, TX_DEFAULT_PARTITION_TIMESTAMP_MIG, -1L, ff, path, pathLen);
            return result;
        }
        return readColumnTopsAllPartitions(columnCount, partitionBy, partitionSizeOffset, partitionTableSize, txMemory, ff, path, pathLen, columnNames);
    }

    private static LongList readColumnTopsAllPartitions(
            int columnCount,
            int partitionBy,
            long partitionSizeOffset,
            int partitionTableSize,
            MemoryMARW txMemory,
            FilesFacade ff,
            Path path,
            int pathLen,
            ObjList<String> columnNames
    ) {
        LongList result = new LongList();
        int partitionCount = partitionTableSize / 8 / 4;
        long offset = partitionSizeOffset + 4;
        long prevPartition = Long.MIN_VALUE;
        long txSize = txMemory.size() - 4 * 8;
        for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
            if (offset > txSize) {
                throw CairoException.critical(0).put("corrupt _txn file ").put(path.trimTo(pathLen).$()).put(", file is too small to read offset ").put(offset);
            }
            long partitionTs = txMemory.getLong(offset);
            if (partitionTs <= prevPartition) {
                throw CairoException.critical(0).put("corrupt _txn file, partitions are not ordered at ").put(path.trimTo(pathLen).$());
            }
            long partitionNameTxn = txMemory.getLong(offset + PARTITION_NAME_TX_OFFSET_MIG * 8);
            readColumnTopsForPartition(result, columnNames, columnCount, partitionBy, partitionTs, partitionNameTxn, ff, path, pathLen);
            offset += 4 * 8;
            prevPartition = partitionTs;
        }
        return result;
    }

    private static void readColumnTopsForPartition(
            LongList tops,
            ObjList<String> columnNames,
            int columnCount,
            int partitionBy,
            long partitionTimestamp,
            long partitionNameTxn,
            FilesFacade ff,
            Path path,
            int pathLen
    ) {
        tops.add(partitionTimestamp);

        path.trimTo(pathLen);
        setPathForNativePartition(path, ColumnType.TIMESTAMP_MICRO, partitionBy, partitionTimestamp, partitionNameTxn);
        int partitionPathLen = path.size();

        if (ff.exists(path.put(Files.SEPARATOR).$())) {
            for (int i = 0; i < columnCount; i++) {
                path.trimTo(partitionPathLen);
                String columnName = columnNames.get(i);
                long columnTop = -1;
                if (ff.exists(dFile(path, columnName))) {
                    columnTop = readColumnTop(ff, path.trimTo(partitionPathLen), columnName, partitionPathLen);
                }
                tops.add(columnTop);
            }
        } else {
            // Sometimes _txn file does not match the table directories, e.g. snapshot is inconsistent.
            // Consider that file presence is same as previous partition.
            // Except if previous partition column existed but column top was not 0, make it 0
            if (tops.size() > columnCount) {
                tops.add(tops, tops.size() - columnCount - 1, tops.size() - 1);
                for (int i = tops.size() - columnCount, n = tops.size(); i < n; i++) {
                    if (tops.getQuick(i) > 0) {
                        tops.setQuick(i, 0);
                    }
                }
            } else {
                for (int i = 0; i < columnCount; i++) {
                    tops.add(-1L);
                }
            }
        }
    }

    private static LPSZ topFile(Path path, CharSequence columnName) {
        return path.concat(columnName).put(".top").$();
    }

    private static long writeColumnVersion(Path tablePath, LongList columnTops, int columnCount, ObjList<String> columnNames, MemoryMARW cvMemory) {
        int topStep = columnCount + 1;
        LongList columnVersions = new LongList();
        LongList maxPartitionIndexWithNoColumnList = new LongList();

        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            // Column Tops list has long values as follows:
            // Partition Timestamp
            // Column top value per every column as follows:
            // -1: column does not exist for the partition
            //  0: column present with no column top
            // >0: column preset and there is a column top

            int maxPartitionIndexWithNoColumn = -1;
            for (int partitionIndex = 0; partitionIndex < columnTops.size(); partitionIndex += topStep) {
                long columnTop = columnTops.getQuick(partitionIndex + columnIndex + 1);
                if (columnTop < 0) {
                    maxPartitionIndexWithNoColumn = partitionIndex;
                }
            }

            if (maxPartitionIndexWithNoColumn != -1) {
                if (maxPartitionIndexWithNoColumn + topStep >= columnTops.size()) {
                    throw CairoException.critical(0).put("Table ").put(tablePath).put(" column '").put(columnNames.getQuick(columnIndex)).put("' is not present in the last partition.");
                }
                long columnAddedPartitionTs = columnTops.getQuick(maxPartitionIndexWithNoColumn + topStep);
                columnVersions.add(CV_COL_TOP_DEFAULT_PARTITION_MIG, columnIndex, -1L, columnAddedPartitionTs);
            }
            maxPartitionIndexWithNoColumnList.add(maxPartitionIndexWithNoColumn);
        }

        // Column Version file must be sorted by Partition Timestamp first and then by Column Index
        // Go partition by partition to keep adding records sorted
        for (int partitionIndex = 0; partitionIndex < columnTops.size(); partitionIndex += topStep) {
            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                long maxPartitionIndexWithNoColumn = maxPartitionIndexWithNoColumnList.getQuick(columnIndex);
                long partitionTs = columnTops.getQuick(partitionIndex);
                long columnTop = columnTops.getQuick(partitionIndex + columnIndex + 1);

                if (columnTop > 0 || (columnTop == 0 && partitionIndex < maxPartitionIndexWithNoColumn)) {
                    columnVersions.add(partitionTs, columnIndex, -1, columnTop);
                }
            }
        }
        // Flush column tops to the file.
        for (int i = 0; i < columnVersions.size(); i++) {
            cvMemory.putLong(Mig620.CV_HEADER_SIZE + i * 8L, columnVersions.getQuick(i));
        }
        int sizeByes = columnVersions.size() * 8;
        cvMemory.jumpTo(sizeByes + Mig620.CV_HEADER_SIZE);
        return sizeByes;
    }

    static void migrate(MigrationContext migrationContext) {
        final FilesFacade ff = migrationContext.getFf();
        final Path path = migrationContext.getTablePath();
        int pathLen = path.size();

        path.concat(TXN_FILE_NAME_MIG);
        EngineMigration.backupFile(
                ff,
                path,
                migrationContext.getTablePath2(),
                TXN_FILE_NAME_MIG,
                425
        );

        try (MemoryMARW txMemory = openFileSafe(ff, path.$(), TX_OFFSET_MAP_WRITER_COUNT_MIG + 8)) {
            int symbolCount = txMemory.getInt(TX_OFFSET_MAP_WRITER_COUNT_MIG);
            long partitionSizeOffset = TX_OFFSET_MAP_WRITER_COUNT_MIG + 4 + symbolCount * 8L;
            int partitionTableSize = txMemory.size() > partitionSizeOffset ? txMemory.getInt(partitionSizeOffset) : 0;
            long existingTotalSize = partitionSizeOffset + 4 + partitionTableSize;
            long txn = txMemory.getLong(TXN_OFFSET_MIG);

            createColumnVersionFile(txMemory, partitionSizeOffset, partitionTableSize, migrationContext, path, pathLen);
            migrateTxn(txMemory, symbolCount, partitionTableSize, existingTotalSize, txn);
        }
    }
}
