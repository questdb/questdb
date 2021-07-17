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

package io.questdb.cairo;

import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.*;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.microtime.TimestampFormatCompiler;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

public final class TableUtils {
    public static final int TABLE_EXISTS = 0;
    public static final int TABLE_DOES_NOT_EXIST = 1;
    public static final int TABLE_RESERVED = 2;
    public static final String META_FILE_NAME = "_meta";
    public static final String TXN_FILE_NAME = "_txn";
    public static final String TXN_SCOREBOARD_FILE_NAME = "_txn_scoreboard";
    public static final String UPGRADE_FILE_NAME = "_upgrade.d";
    public static final String DETACHED_DIR_MARKER = ".detached";
    public static final String TAB_INDEX_FILE_NAME = "_tab_index.d";
    public static final int INITIAL_TXN = 0;
    public static final int NULL_LEN = -1;
    public static final int ANY_TABLE_ID = -1;
    public static final int ANY_TABLE_VERSION = -1;
    public static final long TX_OFFSET_TRANSIENT_ROW_COUNT = 8;
    public static final long TX_OFFSET_FIXED_ROW_COUNT = 16;
    public static final long TX_OFFSET_STRUCT_VERSION = 40;
    public static final long TX_OFFSET_TXN_CHECK = 64;
    public static final long META_OFFSET_COUNT = 0;
    public static final long META_OFFSET_TIMESTAMP_INDEX = 8;
    public static final long META_OFFSET_VERSION = 12;
    public static final long META_OFFSET_TABLE_ID = 16;
    public static final long META_OFFSET_MAX_UNCOMMITTED_ROWS = 20;
    public static final long META_OFFSET_COMMIT_LAG = 24;
    public static final String FILE_SUFFIX_I = ".i";
    public static final String FILE_SUFFIX_D = ".d";
    public static final int LONGS_PER_TX_ATTACHED_PARTITION = 4;
    public static final int LONGS_PER_TX_ATTACHED_PARTITION_MSB = Numbers.msb(LONGS_PER_TX_ATTACHED_PARTITION);
    static final int MIN_INDEX_VALUE_BLOCK_SIZE = Numbers.ceilPow2(4);
    static final byte TODO_RESTORE_META = 2;
    static final byte TODO_TRUNCATE = 1;
    static final DateFormat fmtDay;
    static final DateFormat fmtMonth;
    static final DateFormat fmtYear;
    static final String DEFAULT_PARTITION_NAME = "default";
    // transaction file structure
    static final long TX_OFFSET_TXN = 0;
    static final long TX_OFFSET_MIN_TIMESTAMP = 24;
    static final long TX_OFFSET_MAX_TIMESTAMP = 32;
    static final long TX_OFFSET_DATA_VERSION = 48;
    static final long TX_OFFSET_PARTITION_TABLE_VERSION = 56;
    static final long TX_OFFSET_MAP_WRITER_COUNT = 72;
    /**
     * TXN file structure
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
    // INT - symbol map count, this is a variable part of transaction file
    // below this offset we will have INT values for symbol map size
    static final long META_OFFSET_PARTITION_BY = 4;
    static final long META_COLUMN_DATA_SIZE = 16;
    static final long META_COLUMN_DATA_RESERVED = 3;
    static final long META_OFFSET_COLUMN_TYPES = 128;
    static final int META_FLAG_BIT_INDEXED = 1;
    static final int META_FLAG_BIT_SEQUENTIAL = 1 << 1;
    static final String TODO_FILE_NAME = "_todo_";
    private static final int MIN_SYMBOL_CAPACITY = 2;
    private static final int MAX_SYMBOL_CAPACITY = Numbers.ceilPow2(Integer.MAX_VALUE);
    private static final int MAX_SYMBOL_CAPACITY_CACHED = Numbers.ceilPow2(30_000_000);
    private static final int MAX_INDEX_VALUE_BLOCK_SIZE = Numbers.ceilPow2(8 * 1024 * 1024);
    private final static Log LOG = LogFactory.getLog(TableUtils.class);
    private final static DateFormat fmtDefault;

    private TableUtils() {
    }

    public static void createTable(
            FilesFacade ff,
            AppendOnlyVirtualMemory memory,
            Path path,
            @Transient CharSequence root,
            TableStructure structure,
            int mkDirMode,
            int tableId
    ) {
        createTable(ff, memory, path, root, structure, mkDirMode, ColumnType.VERSION, tableId);
    }

    public static void createTable(
            FilesFacade ff,
            AppendOnlyVirtualMemory memory,
            Path path,
            @Transient CharSequence root,
            TableStructure structure,
            int mkDirMode,
            int tableVersion,
            int tableId
    ) {
        LOG.debug().$("create table [name=").$(structure.getTableName()).$(']').$();
        path.of(root).concat(structure.getTableName());

        if (ff.mkdirs(path.slash$(), mkDirMode) != 0) {
            throw CairoException.instance(ff.errno()).put("could not create [dir=").put(path).put(']');
        }

        final int rootLen = path.length();

        final long dirFd = !ff.isRestrictedFileSystem() ? TableUtils.openRO(ff, path.$(), LOG) : 0;
        try (AppendOnlyVirtualMemory mem = memory) {
            mem.of(ff, path.trimTo(rootLen).concat(META_FILE_NAME).$(), ff.getPageSize());
            final int count = structure.getColumnCount();
            path.trimTo(rootLen);
            mem.putInt(count);
            mem.putInt(structure.getPartitionBy());
            mem.putInt(structure.getTimestampIndex());
            mem.putInt(tableVersion);
            mem.putInt(tableId);
            mem.putInt(structure.getMaxUncommittedRows());
            mem.putLong(structure.getCommitLag());
            mem.jumpTo(TableUtils.META_OFFSET_COLUMN_TYPES);

            for (int i = 0; i < count; i++) {
                mem.putByte((byte) structure.getColumnType(i));
                long flags = 0;
                if (structure.isIndexed(i)) {
                    flags |= META_FLAG_BIT_INDEXED;
                }

                if (structure.isSequential(i)) {
                    flags |= META_FLAG_BIT_SEQUENTIAL;
                }

                mem.putLong(flags);
                mem.putInt(structure.getIndexBlockCapacity(i));
                mem.skip(META_COLUMN_DATA_RESERVED); // reserved
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
            TableUtils.resetTxn(mem, symbolMapCount, 0L, INITIAL_TXN, 0L);
            resetTodoLog(ff, path, rootLen, mem);
            // allocate txn scoreboard
            path.trimTo(rootLen).concat(TXN_SCOREBOARD_FILE_NAME).$();
        } finally {
            if (dirFd > 0) {
                if (ff.fsync(dirFd) != 0) {
                    LOG.error()
                            .$("could not fsync [fd=").$(dirFd)
                            .$(", errno=").$(ff.errno())
                            .$(']').$();
                }
                ff.close(dirFd);
            }
        }
    }

    public static int exists(FilesFacade ff, Path path, CharSequence root, CharSequence name) {
        return exists(ff, path, root, name, 0, name.length());
    }

    public static int exists(FilesFacade ff, Path path, CharSequence root, CharSequence name, int lo, int hi) {
        path.of(root).concat(name, lo, hi).$();
        if (ff.exists(path)) {
            // prepare to replace trailing \0
            if (ff.exists(path.chop$().concat(TXN_FILE_NAME).$())) {
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

    public static int getColumnType(ReadOnlyVirtualMemory metaMem, int columnIndex) {
        return metaMem.getByte(META_OFFSET_COLUMN_TYPES + columnIndex * META_COLUMN_DATA_SIZE);
    }

    public static Timestamps.TimestampAddMethod getPartitionAdd(int partitionBy) {
        switch (partitionBy) {
            case PartitionBy.DAY:
                return Timestamps.ADD_DD;
            case PartitionBy.MONTH:
                return Timestamps.ADD_MM;
            case PartitionBy.YEAR:
                return Timestamps.ADD_YYYY;
            default:
                throw new UnsupportedOperationException("partition by " + partitionBy + " does not have add method");
        }
    }

    public static long getPartitionTableIndexOffset(int symbolWriterCount, int index) {
        return getPartitionTableIndexOffset(getPartitionTableSizeOffset(symbolWriterCount), index);
    }

    public static long getPartitionTableIndexOffset(long partitionTableOffset, int index) {
        return partitionTableOffset + 4 + index * 8L;
    }

    public static long getPartitionTableSizeOffset(int symbolWriterCount) {
        return getSymbolWriterIndexOffset(symbolWriterCount);
    }

    public static long getSymbolWriterIndexOffset(int index) {
        return TX_OFFSET_MAP_WRITER_COUNT + 4 + index * 8L;
    }

    public static long getSymbolWriterTransientIndexOffset(int index) {
        return getSymbolWriterIndexOffset(index) + Integer.BYTES;
    }

    public static long getTxMemSize(int symbolWriterCount, int attachedPartitionsSize) {
        return getPartitionTableIndexOffset(symbolWriterCount, attachedPartitionsSize);
    }

    public static boolean isValidColumnName(CharSequence seq) {
        for (int i = 0, l = seq.length(); i < l; i++) {
            char c = seq.charAt(i);
            switch (c) {
                case ' ':
                case '?':
                case '.':
                case ',':
                case '\'':
                case '\"':
                case '\\':
                case '/':
                case '\0':
                case ':':
                case ')':
                case '(':
                case '+':
                case '-':
                case '*':
                case '%':
                case '~':
                case 0xfeff: // UTF-8 BOM (Byte Order Mark) can appear at the beginning of a character stream
                    return false;
                default:
                    break;
            }
        }
        return true;
    }

    public static boolean isValidInfluxColumnName(CharSequence seq) {
        for (int i = 0, l = seq.length(); i < l; i++) {
            switch (seq.charAt(i)) {
                default:
                    break;
                case ' ':
                case '?':
                case '.':
                case ',':
                case '\'':
                case '\"':
                case '\\':
                case '/':
                case '\0':
                case ':':
                case ')':
                case '(':
                case '+':
                case '*':
                case '%':
                case '~':
                case 0xfeff: // UTF-8 BOM (Byte Order Mark) can appear at the beginning of a character stream
                    return false;
                case '_':
                    if (i < 1) {
                        return false;
                    }
                    break;
                case '-':
                    if (i == 0 || i == l - 1) {
                        return false;
                    }
                    break;
            }
        }
        return true;
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

    public static long mapRWOrClose(FilesFacade ff, LPSZ path, long fd, long size) {
        final long mem = ff.mmap(fd, size, 0, Files.MAP_RW);
        if (mem == -1) {
            ff.close(fd);
            throw CairoException.instance(ff.errno()).put("Could not mmap [file=").put(path).put(']');
        }
        return mem;
    }

    public static void oldPartitionName(Path path, long txn) {
        path.put("-x-").put(txn);
    }

    public static long openFileRWOrFail(FilesFacade ff, LPSZ path) {
        return openRW(ff, path, LOG);
    }

    public static long openRO(FilesFacade ff, LPSZ path, Log log) {
        final long fd = ff.openRO(path);
        if (fd > -1) {
            log.debug().$("open [file=").$(path).$(", fd=").$(fd).$(']').$();
            return fd;
        }
        throw CairoException.instance(ff.errno()).put("could not open read-only [file=").put(path).put(']');
    }

    public static void renameOrFail(FilesFacade ff, Path src, Path dst) {
        if (!ff.rename(src, dst)) {
            throw CairoException.instance(ff.errno()).put("could not rename ").put(src).put(" -> ").put(dst);
        }
    }

    public static void resetTodoLog(FilesFacade ff, Path path, int rootLen, MappedReadWriteMemory mem) {
        mem.of(ff, path.trimTo(rootLen).concat(TODO_FILE_NAME).$(), ff.getPageSize());
        mem.putLong(24, 0); // txn check
        Unsafe.getUnsafe().storeFence();
        mem.putLong(8, 0); // hashLo
        mem.putLong(16, 0); // hashHi
        Unsafe.getUnsafe().storeFence();
        mem.putLong(0, 0); // txn
        mem.putLong(32, 0); // count
        mem.setSize(40);
    }

    public static void resetTxn(PagedVirtualMemory txMem, int symbolMapCount, long txn, long dataVersion, long partitionTableVersion) {
        // txn to let readers know table is being reset
        txMem.putLong(TX_OFFSET_TXN, txn);
        Unsafe.getUnsafe().storeFence();

        // transient row count
        txMem.putLong(TX_OFFSET_TRANSIENT_ROW_COUNT, 0);
        // fixed row count
        txMem.putLong(TX_OFFSET_FIXED_ROW_COUNT, 0);
        // min timestamp value in table
        txMem.putLong(TX_OFFSET_MIN_TIMESTAMP, Long.MAX_VALUE);
        // max timestamp value in table
        txMem.putLong(TX_OFFSET_MAX_TIMESTAMP, Long.MIN_VALUE);
        // structure version
        txMem.putLong(TX_OFFSET_STRUCT_VERSION, 0);
        // data version
        txMem.putLong(TX_OFFSET_DATA_VERSION, dataVersion);
        // partition table version
        txMem.putLong(TX_OFFSET_PARTITION_TABLE_VERSION, partitionTableVersion);

        txMem.putInt(TX_OFFSET_MAP_WRITER_COUNT, symbolMapCount);
        for (int i = 0; i < symbolMapCount; i++) {
            long offset = getSymbolWriterIndexOffset(i);
            txMem.putInt(offset, 0);
            offset += Integer.BYTES;
            txMem.putInt(offset, 0);
        }

        Unsafe.getUnsafe().storeFence();
        // txn check
        txMem.putLong(TX_OFFSET_TXN_CHECK, txn);

        // partition update count
        txMem.putInt(getPartitionTableSizeOffset(symbolMapCount), 0);

        // make sure we put append pointer behind our data so that
        // files does not get truncated when closing
        txMem.jumpTo(getPartitionTableIndexOffset(symbolMapCount, 0));
    }

    /**
     * Sets the path to the directory of a partition taking into account the timestamp and the partitioning scheme.
     *
     * @param path                  Set to the root directory for a table, this will be updated to the root directory of the partition
     * @param partitionBy           Partitioning scheme
     * @param timestamp             A timestamp in the partition
     * @param calculatePartitionMax flag when caller is going to use the return value of this method
     * @return The last timestamp in the partition
     */
    public static long setPathForPartition(Path path, int partitionBy, long timestamp, boolean calculatePartitionMax) {
        return setSinkForPartition(path.slash(), partitionBy, timestamp, calculatePartitionMax);
    }

    public static long setSinkForPartition(CharSink path, int partitionBy, long timestamp, boolean calculatePartitionMax) {
        int y, m, d;
        boolean leap;
        switch (partitionBy) {
            case PartitionBy.DAY:
                y = Timestamps.getYear(timestamp);
                leap = Timestamps.isLeapYear(y);
                m = Timestamps.getMonthOfYear(timestamp, y, leap);
                d = Timestamps.getDayOfMonth(timestamp, y, m, leap);
                TimestampFormatUtils.append000(path, y);
                path.put('-');
                TimestampFormatUtils.append0(path, m);
                path.put('-');
                TimestampFormatUtils.append0(path, d);

                if (calculatePartitionMax) {
                    return Timestamps.yearMicros(y, leap)
                            + Timestamps.monthOfYearMicros(m, leap)
                            + (d - 1) * Timestamps.DAY_MICROS + 24 * Timestamps.HOUR_MICROS - 1;
                }
                return 0;
            case PartitionBy.MONTH:
                y = Timestamps.getYear(timestamp);
                leap = Timestamps.isLeapYear(y);
                m = Timestamps.getMonthOfYear(timestamp, y, leap);
                TimestampFormatUtils.append000(path, y);
                path.put('-');
                TimestampFormatUtils.append0(path, m);

                if (calculatePartitionMax) {
                    return Timestamps.yearMicros(y, leap)
                            + Timestamps.monthOfYearMicros(m, leap)
                            + Timestamps.getDaysPerMonth(m, leap) * 24L * Timestamps.HOUR_MICROS - 1;
                }
                return 0;
            case PartitionBy.YEAR:
                y = Timestamps.getYear(timestamp);
                leap = Timestamps.isLeapYear(y);
                TimestampFormatUtils.append000(path, y);
                if (calculatePartitionMax) {
                    return Timestamps.addYear(Timestamps.yearMicros(y, leap), 1) - 1;
                }
                return 0;
            default:
                path.put(DEFAULT_PARTITION_NAME);
                return Long.MAX_VALUE;
        }
    }

    public static int toIndexKey(int symbolKey) {
        return symbolKey == SymbolTable.VALUE_IS_NULL ? 0 : symbolKey + 1;
    }

    public static void txnPartition(CharSink path, long txn) {
        path.put('.').put(txn);
    }

    public static void txnPartitionConditionally(CharSink path, long txn) {
        if (txn > -1) {
            txnPartition(path, txn);
        }
    }

    public static void validate(FilesFacade ff, MappedReadOnlyMemory metaMem, LowerCaseCharSequenceIntHashMap nameIndex) {
        try {
            final int metaVersion = metaMem.getInt(TableUtils.META_OFFSET_VERSION);
            if (ColumnType.VERSION != metaVersion && metaVersion != 404) {
                throw validationException(metaMem).put("Metadata version does not match runtime version");
            }

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

                if (nameIndex.put(name, i)) {
                    offset += VmUtils.getStorageLength(name);
                } else {
                    throw validationException(metaMem).put("Duplicate column: ").put(name).put(" at [").put(i).put(']');
                }
            }
        } catch (Throwable e) {
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

    public static void writeColumnTop(
            FilesFacade ff,
            Path path,
            CharSequence columnName,
            long columnTop,
            long tempBuf
    ) {
        topFile(path, columnName);
        final long fd = openRW(ff, path, LOG);
        try {
            //noinspection SuspiciousNameCombination
            Unsafe.getUnsafe().putLong(tempBuf, columnTop);
            O3Utils.allocateDiskSpace(ff, fd, Long.BYTES);
            if (ff.write(fd, tempBuf, Long.BYTES, 0) != Long.BYTES) {
                throw CairoException.instance(ff.errno()).put("could not write top file [path=").put(path).put(']');
            }
        } finally {
            ff.close(fd);
        }
    }

    static long readLongAtOffset(FilesFacade ff, Path path, long tempMem8b, long offset) {
        final long fd = TableUtils.openRO(ff, path, LOG);
        try {
            if (ff.read(fd, tempMem8b, Long.BYTES, offset) != Long.BYTES) {
                throw CairoException.instance(ff.errno()).put("Cannot read: ").put(path);
            }
            return Unsafe.getUnsafe().getLong(tempMem8b);
        } finally {
            ff.close(fd);
        }
    }

    /**
     * path member variable has to be set to location of "top" file.
     *
     * @return number of rows column doesn't have when column was added to table that already had data.
     */
    static long readColumnTop(FilesFacade ff, Path path, CharSequence name, int plen, long buf) {
        try {
            if (ff.exists(topFile(path.chop$(), name))) {
                final long fd = TableUtils.openRO(ff, path, LOG);
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
        return path.concat(columnName).put(FILE_SUFFIX_D).$();
    }

    static LPSZ topFile(Path path, CharSequence columnName) {
        return path.concat(columnName).put(".top").$();
    }

    static LPSZ iFile(Path path, CharSequence columnName) {
        return path.concat(columnName).put(FILE_SUFFIX_I).$();
    }

    static long getColumnFlags(ReadOnlyVirtualMemory metaMem, int columnIndex) {
        return metaMem.getLong(META_OFFSET_COLUMN_TYPES + columnIndex * META_COLUMN_DATA_SIZE + 1);
    }

    static boolean isColumnIndexed(ReadOnlyVirtualMemory metaMem, int columnIndex) {
        return (getColumnFlags(metaMem, columnIndex) & META_FLAG_BIT_INDEXED) != 0;
    }

    static boolean isSequential(ReadOnlyVirtualMemory metaMem, int columnIndex) {
        return (getColumnFlags(metaMem, columnIndex) & META_FLAG_BIT_SEQUENTIAL) != 0;
    }

    static int getIndexBlockCapacity(ReadOnlyVirtualMemory metaMem, int columnIndex) {
        return metaMem.getInt(META_OFFSET_COLUMN_TYPES + columnIndex * META_COLUMN_DATA_SIZE + 9);
    }

    static int openMetaSwapFile(FilesFacade ff, AppendOnlyVirtualMemory mem, Path path, int rootLen, int retryCount) {
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
                        LOG.error()
                                .$("could not open swap [file=").$(path)
                                .$(", errno=").$(e.getErrno())
                                .$(']').$();
                    }
                } else {
                    LOG.error()
                            .$("could not remove swap [file=").$(path)
                            .$(", errno=").$(ff.errno())
                            .$(']').$();
                }
            } while (++index < retryCount);
            throw CairoException.instance(0).put("Cannot open indexed file. Max number of attempts reached [").put(index).put("]. Last file tried: ").put(path);
        } finally {
            path.trimTo(rootLen);
        }
    }

    static void openMetaSwapFileByIndex(FilesFacade ff, AppendOnlyVirtualMemory mem, Path path, int rootLen, int swapIndex) {
        try {
            path.concat(META_SWAP_FILE_NAME);
            if (swapIndex > 0) {
                path.put('.').put(swapIndex);
            }
            path.$();
            mem.of(ff, path, ff.getPageSize());
        } finally {
            path.trimTo(rootLen);
        }
    }

    private static CairoException validationException(MappedReadOnlyMemory mem) {
        return CairoException.instance(0).put("Invalid metadata at fd=").put(mem.getFd()).put(". ");
    }

    static boolean isSamePartition(long timestampA, long timestampB, int partitionBy) {
        switch (partitionBy) {
            case PartitionBy.NONE:
                return true;
            case PartitionBy.DAY:
                return Timestamps.floorDD(timestampA) == Timestamps.floorDD(timestampB);
            case PartitionBy.MONTH:
                return Timestamps.floorMM(timestampA) == Timestamps.floorMM(timestampB);
            case PartitionBy.YEAR:
                return Timestamps.floorYYYY(timestampA) == Timestamps.floorYYYY(timestampB);
            default:
                throw CairoException.instance(0).put("Cannot compare timestamps for unsupported partition type: [").put(partitionBy).put(']');
        }
    }

    static DateFormat getPartitionDateFmt(int partitionBy) {
        switch (partitionBy) {
            case PartitionBy.DAY:
                return fmtDay;
            case PartitionBy.MONTH:
                return fmtMonth;
            case PartitionBy.YEAR:
                return fmtYear;
            case PartitionBy.NONE:
                return fmtDefault;
            default:
                throw new UnsupportedOperationException("partition by " + partitionBy + " does not have date format");
        }
    }

    static Timestamps.TimestampFloorMethod getPartitionFloor(int partitionBy) {
        switch (partitionBy) {
            case PartitionBy.DAY:
                return Timestamps.FLOOR_DD;
            case PartitionBy.MONTH:
                return Timestamps.FLOOR_MM;
            case PartitionBy.YEAR:
                return Timestamps.FLOOR_YYYY;
            default:
                throw new UnsupportedOperationException("partition by " + partitionBy + " does not have floor method");
        }
    }

    static Timestamps.TimestampCeilMethod getPartitionCeil(int partitionBy) {
        switch (partitionBy) {
            case PartitionBy.DAY:
                return Timestamps.CEIL_DD;
            case PartitionBy.MONTH:
                return Timestamps.CEIL_MM;
            case PartitionBy.YEAR:
                return Timestamps.CEIL_YYYY;
            default:
                throw new UnsupportedOperationException("partition by " + partitionBy + " does not have ceil method");
        }
    }

    // Scans timestamp file
    // returns size of partition detected, e.g. size of monotonic increase
    // of timestamp longs read from 0 offset to the end of the file
    // It also writes min and max values found in tempMem16b
    static long readPartitionSizeMinMax(FilesFacade ff, Path path, CharSequence columnName, long tempMem16b, long timestamp) {
        int plen = path.chop$().length();
        try {
            if (ff.exists(path.concat(columnName).put(FILE_SUFFIX_D).$())) {
                final long fd = TableUtils.openRO(ff, path, LOG);
                try {
                    long fileSize = ff.length(fd);
                    long mappedMem = ff.mmap(fd, fileSize, 0, Files.MAP_RO);
                    if (mappedMem < 0) {
                        throw CairoException.instance(ff.errno()).put("Cannot map: ").put(path);
                    }
                    try {
                        long minTimestamp;
                        long maxTimestamp = timestamp;
                        long size = 0L;

                        for (long ptr = mappedMem, hi = mappedMem + fileSize; ptr < hi; ptr += Long.BYTES) {
                            long ts = Unsafe.getUnsafe().getLong(ptr);
                            if (ts >= maxTimestamp) {
                                maxTimestamp = ts;
                                size++;
                            } else {
                                break;
                            }
                        }
                        if (size > 0) {
                            minTimestamp = Unsafe.getUnsafe().getLong(mappedMem);
                            Unsafe.getUnsafe().putLong(tempMem16b, minTimestamp);
                            Unsafe.getUnsafe().putLong(tempMem16b + Long.BYTES, maxTimestamp);
                        }
                        return size;
                    } finally {
                        ff.munmap(mappedMem, fileSize);
                    }
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

    static void createDirsOrFail(FilesFacade ff, Path path, int mkDirMode) {
        if (ff.mkdirs(path, mkDirMode) != 0) {
            throw CairoException.instance(ff.errno()).put("could not create directories [file=").put(path).put(']');
        }
    }

    static long openRW(FilesFacade ff, LPSZ path, Log log) {
        final long fd = ff.openRW(path);
        if (fd > -1) {
            log.debug().$("open [file=").$(path).$(", fd=").$(fd).$(']').$();
            return fd;
        }
        throw CairoException.instance(ff.errno()).put("could not open read-write [file=").put(path).put(']');
    }

    static long openCleanRW(FilesFacade ff, LPSZ path, long size, Log log) {
        final long fd = ff.openCleanRW(path, size);
        if (fd > -1) {
            log.debug().$("open clean [file=").$(path).$(", fd=").$(fd).$(']').$();
            return fd;
        }
        throw CairoException.instance(ff.errno()).put("could not open read-write with clean allocation [file=").put(path).put(']');
    }

    static {
        TimestampFormatCompiler compiler = new TimestampFormatCompiler();
        fmtDay = compiler.compile("yyyy-MM-dd");
        fmtMonth = compiler.compile("yyyy-MM");
        fmtYear = compiler.compile("yyyy");
        fmtDefault = new DateFormat() {
            @Override
            public void format(long datetime, DateLocale locale, CharSequence timeZoneName, CharSink sink) {
                sink.put(DEFAULT_PARTITION_NAME);
            }

            @Override
            public long parse(CharSequence in, DateLocale locale) {
                return 0;
            }

            @Override
            public long parse(CharSequence in, int lo, int hi, DateLocale locale) {
                return 0;
            }
        };
    }
}
