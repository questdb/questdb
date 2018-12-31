/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cairo;

import com.questdb.cairo.sql.SymbolTable;
import com.questdb.griffin.SqlException;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.std.*;
import com.questdb.std.microtime.DateFormat;
import com.questdb.std.microtime.DateFormatCompiler;
import com.questdb.std.microtime.Dates;
import com.questdb.std.str.LPSZ;
import com.questdb.std.str.Path;

public final class TableUtils {
    public static final int TABLE_EXISTS = 0;
    public static final int TABLE_DOES_NOT_EXIST = 1;
    public static final int TABLE_RESERVED = 2;
    public static final String META_FILE_NAME = "_meta";
    public static final String TXN_FILE_NAME = "_txn";
    public static final long META_OFFSET_COLUMN_TYPES = 128;
    public static final int INITIAL_TXN = 0;
    public static final int NULL_LEN = -1;
    public static final int ANY_TABLE_VERSION = -1;
    static final int MIN_INDEX_VALUE_BLOCK_SIZE = Numbers.ceilPow2(4);
    static final byte TODO_RESTORE_META = 2;
    static final byte TODO_TRUNCATE = 1;
    static final DateFormat fmtDay;
    static final DateFormat fmtMonth;
    static final DateFormat fmtYear;
    static final String ARCHIVE_FILE_NAME = "_archive";
    static final String DEFAULT_PARTITION_NAME = "default";
    // transaction file structure
    static final long TX_OFFSET_TXN = 0;
    static final long TX_OFFSET_TRANSIENT_ROW_COUNT = 8;
    static final long TX_OFFSET_FIXED_ROW_COUNT = 16;
    static final long TX_OFFSET_MAX_TIMESTAMP = 24;
    static final long TX_OFFSET_STRUCT_VERSION = 32;
    static final long TX_OFFSET_PARTITION_TABLE_VERSION = 40;
    static final long TX_OFFSET_TXN_CHECK = 48;
    static final long TX_OFFSET_MAP_WRITER_COUNT = 56;
    /**
     * struct {
     * long txn;
     * long transient_row_count; // rows count in last partition
     * long fixed_row_count; // row count in table excluding count in last partition
     * long max_timestamp; // last timestamp written to table
     * long struct_version; // data structure version; whenever columns added or removed this version changes.
     * long partition_version; // version that increments whenever non-current partitions are modified/added/removed
     * long txn_check; // same as txn - sanity check for concurrent reads and writes
     * int  map_writer_count; // symbol writer count
     * int  map_writer_position[map_writer_count]; // position of each of map writers
     * }
     * <p>
     * TableUtils.resetTxn() writes to this file, it could be using different offsets, beware
     */

    static final String META_SWAP_FILE_NAME = "_meta.swp";
    static final String META_PREV_FILE_NAME = "_meta.prev";
    static final String TODO_FILE_NAME = "_todo";
    static final long META_OFFSET_COUNT = 0;
    // INT - symbol map count, this is a variable part of transaction file
    // below this offset we will have INT values for symbol map size
    static final long META_OFFSET_PARTITION_BY = 4;
    static final long META_OFFSET_TIMESTAMP_INDEX = 8;
    private static final int MIN_SYMBOL_CAPACITY = 2;
    private static final int MAX_SYMBOL_CAPACITY = Numbers.ceilPow2(Integer.MAX_VALUE);
    private static final int MAX_SYMBOL_CAPACITY_CACHED = Numbers.ceilPow2(1_000_000);
    private static final int MAX_INDEX_VALUE_BLOCK_SIZE = Numbers.ceilPow2(8 * 1024 * 1024);
    private static final long META_COLUMN_DATA_SIZE = 16;
    private final static Log LOG = LogFactory.getLog(TableUtils.class);

    public static void createTable(
            FilesFacade ff,
            AppendMemory memory,
            Path path,
            CharSequence root,
            TableStructure structure,
            int mkDirMode
    ) {
        path.of(root).concat(structure.getTableName());

        if (ff.mkdirs(path.put(Files.SEPARATOR).$(), mkDirMode) != 0) {
            throw CairoException.instance(ff.errno()).put("could not create [dir=").put(path).put(']');
        }

        final int rootLen = path.length();

        try (AppendMemory mem = memory) {
            mem.of(ff, path.trimTo(rootLen).concat(META_FILE_NAME).$(), ff.getPageSize());
            final int count = structure.getColumnCount();
            mem.putInt(count);
            mem.putInt(structure.getPartitionBy());
            mem.putInt(structure.getTimestampIndex());
            mem.jumpTo(TableUtils.META_OFFSET_COLUMN_TYPES);

            for (int i = 0; i < count; i++) {
                mem.putByte((byte) structure.getColumnType(i));
                mem.putBool(structure.getIndexedFlag(i));
                mem.putInt(structure.getIndexBlockCapacity(i));
                mem.skip(10); // reserved
            }
            for (int i = 0; i < count; i++) {
                mem.putStr(structure.getColumnName(i));
            }

            // create symbol maps
            int symbolMapCount = 0;
            for (int i = 0; i < count; i++) {
                if (structure.getColumnType(i) == ColumnType.SYMBOL) {
                    SymbolMapWriter.createSymbolMapFiles(
                            ff,
                            mem,
                            path.trimTo(rootLen),
                            structure.getColumnName(i),
                            structure.getSymbolCapacity(i),
                            structure.getSymbolCacheFlag(i)
                    );
                    symbolMapCount++;
                }
            }
            mem.of(ff, path.trimTo(rootLen).concat(TXN_FILE_NAME).$(), ff.getPageSize());
            TableUtils.resetTxn(mem, symbolMapCount);
        }
    }

    public static int exists(FilesFacade ff, Path path, CharSequence root, CharSequence name) {
        return exists(ff, path, root, name, 0, name.length());
    }

    public static int exists(FilesFacade ff, Path path, CharSequence root, CharSequence name, int lo, int hi) {
        path.of(root).concat(name, lo, hi).$();
        if (ff.exists(path)) {
            // prepare to replace trailing \0
            if (ff.exists(path.chopZ().concat(TXN_FILE_NAME).$())) {
                return TABLE_EXISTS;
            } else {
                return TABLE_RESERVED;
            }
        } else {
            return TABLE_DOES_NOT_EXIST;
        }
    }

    public static long getColumnNameOffset(int columnCount) {
        return META_OFFSET_COLUMN_TYPES + columnCount * META_COLUMN_DATA_SIZE;
    }

    public static long getPartitionTableIndexOffset(int symbolWriterCount, int index) {
        return getPartitionTableSizeOffset(symbolWriterCount) + 4 + index * 8;
    }

    public static long getPartitionTableSizeOffset(int symbolWriterCount) {
        return getSymbolWriterIndexOffset(symbolWriterCount);
    }

    public static long getSymbolWriterIndexOffset(int index) {
        return TX_OFFSET_MAP_WRITER_COUNT + 4 + index * 4L;
    }

    public static long getTxMemSize(int symbolWriterCount, int removedPartitionsCount) {
        return getPartitionTableIndexOffset(symbolWriterCount, removedPartitionsCount);
    }

    public static long lock(FilesFacade ff, Path path) {
        long fd = ff.openRW(path);
        if (fd == -1) {
            LOG.error().$("cannot open '").$(path).$("' to lock [errno=").$(ff.errno()).$(']').$();
            return -1L;
        }

        if (ff.lock(fd) != 0) {
            LOG.error().$("cannot lock '").$(path).$("' [errno=").$(ff.errno()).$(", fd=").$(fd).$(']').$();
            ff.close(fd);
            return -1L;
        }

        return fd;
    }

    public static void lockName(Path path) {
        path.put(".lock").$();
    }

    public static void resetTxn(VirtualMemory txMem, int symbolMapCount) {
        // txn to let readers know table is being reset
        txMem.putLong(TX_OFFSET_TXN, INITIAL_TXN);
        Unsafe.getUnsafe().storeFence();

        // transient row count
        txMem.putLong(TX_OFFSET_TRANSIENT_ROW_COUNT, 0);
        // fixed row count
        txMem.putLong(TX_OFFSET_FIXED_ROW_COUNT, 0);
        // partition low
        txMem.putLong(TX_OFFSET_MAX_TIMESTAMP, Long.MIN_VALUE);
        // structure version
        txMem.putLong(TX_OFFSET_STRUCT_VERSION, 0);

        txMem.putInt(TX_OFFSET_MAP_WRITER_COUNT, symbolMapCount);
        for (int i = 0; i < symbolMapCount; i++) {
            txMem.putInt(getSymbolWriterIndexOffset(i), 0);
        }

        Unsafe.getUnsafe().storeFence();
        // txn check
        txMem.putLong(TX_OFFSET_TXN_CHECK, INITIAL_TXN);

        // partition update count
        txMem.putInt(getPartitionTableSizeOffset(symbolMapCount), 0);

        // make sure we put append pointer behind our data so that
        // files does not get truncated when closing
        txMem.jumpTo(getPartitionTableIndexOffset(symbolMapCount, 0));
    }

    public static int toIndexKey(int symbolKey) {
        return symbolKey == SymbolTable.VALUE_IS_NULL ? 0 : symbolKey + 1;
    }

    public static void validate(FilesFacade ff, ReadOnlyMemory metaMem, CharSequenceIntHashMap nameIndex) {
        try {
            final int columnCount = metaMem.getInt(META_OFFSET_COUNT);
            long offset = getColumnNameOffset(columnCount);

            if (offset < columnCount || (
                    columnCount > 0 && (offset < 0 || offset >= ff.length(metaMem.getFd())))) {
                throw validationException(metaMem).put("Incorrect columnCount: ").put(columnCount);
            }

            final int timestampIndex = metaMem.getInt(META_OFFSET_TIMESTAMP_INDEX);
            if (timestampIndex < -1 || timestampIndex >= columnCount) {
                throw validationException(metaMem).put("Timestamp index is outside of columnCount");
            }

            if (timestampIndex != -1) {
                int timestampType = getColumnType(metaMem, timestampIndex);
                if (timestampType != ColumnType.TIMESTAMP) {
                    throw validationException(metaMem).put("Timestamp column must be TIMESTAMP, but found ").put(ColumnType.nameOf(timestampType));
                }
            }

            // validate column types and index attributes
            for (int i = 0; i < columnCount; i++) {
                int type = getColumnType(metaMem, i);
                if (ColumnType.sizeOf(type) == -1) {
                    throw validationException(metaMem).put("Invalid column type ").put(type).put(" at [").put(i).put(']');
                }

                if (isColumnIndexed(metaMem, i)) {
                    if (type != ColumnType.SYMBOL) {
                        throw validationException(metaMem).put("Index flag is only supported for SYMBOL").put(" at [").put(i).put(']');
                    }

                    if (getIndexBlockCapacity(metaMem, i) < 2) {
                        throw validationException(metaMem).put("Invalid index value block capacity ").put(getIndexBlockCapacity(metaMem, i)).put(" at [").put(i).put(']');
                    }
                }
            }

            // validate column names
            for (int i = 0; i < columnCount; i++) {
                CharSequence name = metaMem.getStr(offset);
                if (name == null || name.length() < 1) {
                    throw validationException(metaMem).put("NULL column name at [").put(i).put(']');
                }

                String s = name.toString();
                if (!nameIndex.put(s, i)) {
                    throw validationException(metaMem).put("Duplicate column: ").put(s).put(" at [").put(i).put(']');
                }
                offset += ReadOnlyMemory.getStorageLength(name);
            }
        } catch (CairoException e) {
            nameIndex.clear();
            throw e;
        }
    }

    public static void validateIndexValueBlockSize(int position, int indexValueBlockSize) throws SqlException {
        if (indexValueBlockSize < MIN_INDEX_VALUE_BLOCK_SIZE) {
            throw SqlException.$(position, "min index block capacity is ").put(MIN_INDEX_VALUE_BLOCK_SIZE);
        }
        if (indexValueBlockSize > MAX_INDEX_VALUE_BLOCK_SIZE) {
            throw SqlException.$(position, "max index block capacity is ").put(MAX_INDEX_VALUE_BLOCK_SIZE);
        }
    }

    public static void validateSymbolCapacity(int position, int symbolCapacity) throws SqlException {
        if (symbolCapacity < MIN_SYMBOL_CAPACITY) {
            throw SqlException.$(position, "min symbol capacity is ").put(MIN_SYMBOL_CAPACITY);
        }
        if (symbolCapacity > MAX_SYMBOL_CAPACITY) {
            throw SqlException.$(position, "max symbol capacity is ").put(MAX_SYMBOL_CAPACITY);
        }
    }

    public static void validateSymbolCapacityCached(boolean cache, int symbolCapacity, int cacheKeywordPosition) throws SqlException {
        if (cache && symbolCapacity > MAX_SYMBOL_CAPACITY_CACHED) {
            throw SqlException.$(cacheKeywordPosition, "max cached symbol capacity is ").put(MAX_SYMBOL_CAPACITY_CACHED);
        }
    }

    /**
     * path member variable has to be set to location of "top" file.
     *
     * @return number of rows column doesn't have when column was added to table that already had data.
     */
    static long readColumnTop(FilesFacade ff, Path path, CharSequence name, int plen, long buf) {
        try {
            if (ff.exists(topFile(path.chopZ(), name))) {
                long fd = ff.openRO(path);
                try {
                    if (ff.read(fd, buf, 8, 0) != 8) {
                        throw CairoException.instance(Os.errno()).put("Cannot read top of column ").put(path);
                    }
                    return Unsafe.getUnsafe().getLong(buf);
                } finally {
                    ff.close(fd);
                }
            }
            return 0L;
        } finally {
            path.trimTo(plen);
        }
    }

    static LPSZ dFile(Path path, CharSequence columnName) {
        return path.concat(columnName).put(".d").$();
    }

    static LPSZ topFile(Path path, CharSequence columnName) {
        return path.concat(columnName).put(".top").$();
    }

    static LPSZ iFile(Path path, CharSequence columnName) {
        return path.concat(columnName).put(".i").$();
    }

    static int getColumnType(ReadOnlyMemory metaMem, int columnIndex) {
        return metaMem.getByte(META_OFFSET_COLUMN_TYPES + columnIndex * META_COLUMN_DATA_SIZE);
    }

    static boolean isColumnIndexed(ReadOnlyMemory metaMem, int columnIndex) {
        return metaMem.getBool(META_OFFSET_COLUMN_TYPES + columnIndex * META_COLUMN_DATA_SIZE + 1);
    }

    static int getIndexBlockCapacity(ReadOnlyMemory metaMem, int columnIndex) {
        return metaMem.getInt(META_OFFSET_COLUMN_TYPES + columnIndex * META_COLUMN_DATA_SIZE + 2);
    }

    static int openMetaSwapFile(FilesFacade ff, AppendMemory mem, Path path, int rootLen, int retryCount) {
        try {
            path.concat(META_SWAP_FILE_NAME).$();
            int l = path.length();
            int index = 0;
            do {
                if (index > 0) {
                    path.trimTo(l).put('.').put(index);
                    path.$();
                }

                if (!ff.exists(path) || ff.remove(path)) {
                    try {
                        mem.of(ff, path, ff.getPageSize());
                        return index;
                    } catch (CairoException e) {
                        // right, cannot open file for some reason?
                        LOG.error().$("Cannot open file: ").$(path).$('[').$(Os.errno()).$(']').$();
                    }
                } else {
                    LOG.error().$("Cannot remove file: ").$(path).$('[').$(Os.errno()).$(']').$();
                }
            } while (++index < retryCount);
            throw CairoException.instance(0).put("Cannot open indexed file. Max number of attempts reached [").put(index).put("]. Last file tried: ").put(path);
        } finally {
            path.trimTo(rootLen);
        }
    }

    static String getTodoText(long code) {
        switch ((int) (code & 0xff)) {
            case TODO_TRUNCATE:
                return "truncate";
            case TODO_RESTORE_META:
                return "restore meta";
            default:
                // really impossible to happen, but we keep this line to comply with Murphy's law.
                return "unknown";
        }
    }

    private static CairoException validationException(ReadOnlyMemory mem) {
        return CairoException.instance(0).put("Invalid metadata at fd=").put(mem.getFd()).put(". ");
    }

    static boolean isSamePartition(long timestampA, long timestampB, int partitionBy) {
        switch (partitionBy) {
            case PartitionBy.NONE:
                return true;
            case PartitionBy.DAY:
                return Dates.floorDD(timestampA) == Dates.floorDD(timestampB);
            case PartitionBy.MONTH:
                return Dates.floorMM(timestampA) == Dates.floorMM(timestampB);
            case PartitionBy.YEAR:
                return Dates.floorYYYY(timestampA) == Dates.floorYYYY(timestampB);
            default:
                throw CairoException.instance(0).put("Cannot compare timestamps for unsupported partition type: [").put(partitionBy).put(']');
        }
    }

    static long readPartitionSize(FilesFacade ff, Path path, long tempMem8b) {
        int plen = path.length();
        try {
            if (ff.exists(path.concat(ARCHIVE_FILE_NAME).$())) {
                long fd = ff.openRO(path);
                if (fd == -1) {
                    throw CairoException.instance(Os.errno()).put("Cannot open: ").put(path);
                }

                try {
                    if (ff.read(fd, tempMem8b, 8, 0) != 8) {
                        throw CairoException.instance(Os.errno()).put("Cannot read: ").put(path);
                    }
                    return Unsafe.getUnsafe().getLong(tempMem8b);
                } finally {
                    ff.close(fd);
                }
            } else {
                throw CairoException.instance(0).put("Doesn't exist: ").put(path);
            }
        } finally {
            path.trimTo(plen);
        }
    }

    static {
        DateFormatCompiler compiler = new DateFormatCompiler();
        fmtDay = compiler.compile("yyyy-MM-dd");
        fmtMonth = compiler.compile("yyyy-MM");
        fmtYear = compiler.compile("yyyy");
    }
}
