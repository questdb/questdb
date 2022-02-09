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

package io.questdb.cairo;

import io.questdb.MessageBus;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.*;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.MPSequence;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.tasks.O3PurgeDiscoveryTask;
import org.jetbrains.annotations.Nullable;

public final class TableUtils {
    public static final int TABLE_EXISTS = 0;
    public static final int TABLE_DOES_NOT_EXIST = 1;
    public static final int TABLE_RESERVED = 2;
    public static final String META_FILE_NAME = "_meta";
    public static final String TXN_FILE_NAME = "_txn";
    public static final String COLUMN_VERSION_FILE_NAME = "_cv";
    public static final String TXN_SCOREBOARD_FILE_NAME = "_txn_scoreboard";
    public static final String UPGRADE_FILE_NAME = "_upgrade.d";
    public static final String DETACHED_DIR_MARKER = ".detached";
    public static final String TAB_INDEX_FILE_NAME = "_tab_index.d";
    public static final int INITIAL_TXN = 0;
    public static final int NULL_LEN = -1;
    public static final int ANY_TABLE_ID = -1;
    public static final int ANY_TABLE_VERSION = -1;
    public static final long META_OFFSET_COUNT = 0;
    public static final long META_OFFSET_TIMESTAMP_INDEX = 8;
    public static final long META_OFFSET_VERSION = 12;
    public static final long META_OFFSET_TABLE_ID = 16;
    public static final long META_OFFSET_MAX_UNCOMMITTED_ROWS = 20; // LONG
    public static final long META_OFFSET_COMMIT_LAG = 24; // LONG
    public static final long META_OFFSET_STRUCTURE_VERSION = 32; // LONG
    public static final String FILE_SUFFIX_I = ".i";
    public static final String FILE_SUFFIX_D = ".d";
    public static final int LONGS_PER_TX_ATTACHED_PARTITION = 4;
    public static final int LONGS_PER_TX_ATTACHED_PARTITION_MSB = Numbers.msb(LONGS_PER_TX_ATTACHED_PARTITION);
    public static final String DEFAULT_PARTITION_NAME = "default";
    public static final long META_OFFSET_COLUMN_TYPES = 128;
    public static final long META_COLUMN_DATA_SIZE = 32;
    static final int MIN_INDEX_VALUE_BLOCK_SIZE = Numbers.ceilPow2(4);
    static final byte TODO_RESTORE_META = 2;
    static final byte TODO_TRUNCATE = 1;
    static final int COLUMN_VERSION_FILE_HEADER_SIZE = 40;

    // transaction file structure
    public static final int TX_BASE_HEADER_SECTION_PADDING = 12; // Add some free space into header for future use
    public static final long TX_BASE_OFFSET_VERSION_64 = 0;

    public static final long TX_BASE_OFFSET_A_32 = TX_BASE_OFFSET_VERSION_64 + 8;
    public static final long TX_BASE_OFFSET_SYMBOLS_SIZE_A_32 = TX_BASE_OFFSET_A_32 + 4;
    public static final long TX_BASE_OFFSET_PARTITIONS_SIZE_A_32 = TX_BASE_OFFSET_SYMBOLS_SIZE_A_32 + 4;

    public static final long TX_BASE_OFFSET_B_32 = TX_BASE_OFFSET_PARTITIONS_SIZE_A_32 + 4 + TX_BASE_HEADER_SECTION_PADDING;
    public static final long TX_BASE_OFFSET_SYMBOLS_SIZE_B_32 = TX_BASE_OFFSET_B_32 + 4;
    public static final long TX_BASE_OFFSET_PARTITIONS_SIZE_B_32 = TX_BASE_OFFSET_SYMBOLS_SIZE_B_32 + 4;

    public static final int TX_BASE_HEADER_SIZE = (int) Math.max(TX_BASE_OFFSET_PARTITIONS_SIZE_B_32 + 4 + TX_BASE_HEADER_SECTION_PADDING, 64);

    public static final long TX_OFFSET_TXN_64 = 0;
    public static final long TX_OFFSET_TRANSIENT_ROW_COUNT_64 = TX_OFFSET_TXN_64 + 8;
    public static final long TX_OFFSET_FIXED_ROW_COUNT_64 = TX_OFFSET_TRANSIENT_ROW_COUNT_64 + 8;
    public static final long TX_OFFSET_MIN_TIMESTAMP_64 = TX_OFFSET_FIXED_ROW_COUNT_64 + 8;
    public static final long TX_OFFSET_MAX_TIMESTAMP_64 = TX_OFFSET_MIN_TIMESTAMP_64 + 8;
    public static final long TX_OFFSET_STRUCT_VERSION_64 = TX_OFFSET_MAX_TIMESTAMP_64 + 8;
    public static final long TX_OFFSET_DATA_VERSION_64 = TX_OFFSET_STRUCT_VERSION_64 + 8;
    public static final long TX_OFFSET_PARTITION_TABLE_VERSION_64 = TX_OFFSET_DATA_VERSION_64 + 8;
    public static final long TX_OFFSET_COLUMN_VERSION_64 = TX_OFFSET_PARTITION_TABLE_VERSION_64 + 8;
    public static final long TX_OFFSET_MAP_WRITER_COUNT_32 = 128;
    public static final int TX_RECORD_HEADER_SIZE = (int) TX_OFFSET_MAP_WRITER_COUNT_32 + Integer.BYTES;

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
    static final int META_FLAG_BIT_INDEXED = 1;
    static final int META_FLAG_BIT_SEQUENTIAL = 1 << 1;
    static final String TODO_FILE_NAME = "_todo_";
    private static final int MIN_SYMBOL_CAPACITY = 2;
    private static final int MAX_SYMBOL_CAPACITY = Numbers.ceilPow2(Integer.MAX_VALUE);
    private static final int MAX_SYMBOL_CAPACITY_CACHED = Numbers.ceilPow2(30_000_000);
    private static final int MAX_INDEX_VALUE_BLOCK_SIZE = Numbers.ceilPow2(8 * 1024 * 1024);
    private final static Log LOG = LogFactory.getLog(TableUtils.class);

    private TableUtils() {
    }

    public static void allocateDiskSpace(FilesFacade ff, long fd, long size) {
        if (ff.length(fd) < size && !ff.allocate(fd, size)) {
            throw CairoException.instance(ff.errno()).put("No space left [size=").put(size).put(", fd=").put(fd).put(']');
        }
    }

    public static void createTable(
            CairoConfiguration configuration,
            MemoryMARW memory,
            Path path,
            TableStructure structure,
            int tableId
    ) {
        createTable(configuration, memory, path, structure, ColumnType.VERSION, tableId);
    }

    public static void createColumnVersionFile(MemoryMARW mem) {
        // Create page of 0s for Column Version file "_cv"
        mem.extend(COLUMN_VERSION_FILE_HEADER_SIZE);
        mem.jumpTo(COLUMN_VERSION_FILE_HEADER_SIZE);
        mem.zero();
    }

    public static void createTable(
            CairoConfiguration configuration,
            MemoryMARW memory,
            Path path,
            TableStructure structure,
            int tableVersion,
            int tableId
    ) {
        final FilesFacade ff = configuration.getFilesFacade();
        final CharSequence root = configuration.getRoot();
        final int mkDirMode = configuration.getMkDirMode();
        LOG.debug().$("create table [name=").$(structure.getTableName()).$(']').$();
        path.of(root).concat(structure.getTableName());

        if (ff.mkdirs(path.slash$(), mkDirMode) != 0) {
            throw CairoException.instance(ff.errno()).put("could not create [dir=").put(path).put(']');
        }

        final int rootLen = path.length();

        final long dirFd = !ff.isRestrictedFileSystem() ? TableUtils.openRO(ff, path.$(), LOG) : 0;
        try (MemoryMARW mem = memory) {
            mem.smallFile(ff, path.trimTo(rootLen).concat(META_FILE_NAME).$(), MemoryTag.MMAP_DEFAULT);
            mem.jumpTo(0);
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

            assert count > 0;

            for (int i = 0; i < count; i++) {
                mem.putInt(structure.getColumnType(i));
                long flags = 0;
                if (structure.isIndexed(i)) {
                    flags |= META_FLAG_BIT_INDEXED;
                }

                if (structure.isSequential(i)) {
                    flags |= META_FLAG_BIT_SEQUENTIAL;
                }

                mem.putLong(flags);
                mem.putInt(structure.getIndexBlockCapacity(i));
                mem.putLong(structure.getColumnHash(i));
                // reserved
                mem.skip(8);
            }

            for (int i = 0; i < count; i++) {
                mem.putStr(structure.getColumnName(i));
            }

            // create symbol maps
            int symbolMapCount = 0;
            for (int i = 0; i < count; i++) {
                if (ColumnType.isSymbol(structure.getColumnType(i))) {
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
            mem.smallFile(ff, path.trimTo(rootLen).concat(TXN_FILE_NAME).$(), MemoryTag.MMAP_DEFAULT);
            createTxn(mem, symbolMapCount, 0L, INITIAL_TXN, 0L, 0L);


            mem.smallFile(ff, path.trimTo(rootLen).concat(COLUMN_VERSION_FILE_NAME).$(), MemoryTag.MMAP_DEFAULT);
            createColumnVersionFile(mem);
            mem.close();

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

    public static void createTxn(MemoryMW txMem, int symbolMapCount, long txn, long dataVersion, long partitionTableVersion, long structureVersion) {
        txMem.putInt(TX_BASE_OFFSET_A_32, TX_BASE_HEADER_SIZE);
        txMem.putInt(TX_BASE_OFFSET_SYMBOLS_SIZE_A_32, symbolMapCount * 8);
        txMem.putInt(TX_BASE_OFFSET_PARTITIONS_SIZE_A_32, 0);
        resetTxn(txMem, TX_BASE_HEADER_SIZE, symbolMapCount, txn, dataVersion, partitionTableVersion, structureVersion);
        txMem.setTruncateSize(TX_BASE_HEADER_SIZE + TX_RECORD_HEADER_SIZE);
    }

    public static long createTransitionIndex(
            MemoryR masterMeta,
            MemoryR slaveMeta,
            int slaveColumnCount,
            LowerCaseCharSequenceIntHashMap slaveColumnNameIndexMap
    ) {
        int masterColumnCount = masterMeta.getInt(META_OFFSET_COUNT);
        int n = Math.max(slaveColumnCount, masterColumnCount);
        final long pTransitionIndex;
        final int size = n * 16;

        long index = pTransitionIndex = Unsafe.calloc(size, MemoryTag.NATIVE_DEFAULT);
        Unsafe.getUnsafe().putInt(index, size);
        Unsafe.getUnsafe().putInt(index + 4, masterColumnCount);
        index += 8;

        // index structure is
        // [copy from, copy to] int tuples, each of which is index into original column metadata
        // the number of these tuples is DOUBLE of maximum of old and new column count.
        // Tuples are separated into two areas, one is immutable, which drives how metadata should be moved,
        // the other is the state of moving algo. Moving algo will start with copy of immutable area and will
        // continue to zero out tuple values in mutable area when metadata is moved. Mutable area is

        // "copy from" == 0 indicates that column is newly added, similarly
        // "copy to" == 0 indicates that old column has been deleted
        //

        long offset = getColumnNameOffset(masterColumnCount);
        for (int i = 0; i < masterColumnCount; i++) {
            CharSequence name = masterMeta.getStr(offset);
            offset += Vm.getStorageLength(name);
            int oldPosition = slaveColumnNameIndexMap.get(name);
            // write primary (immutable) index
            boolean hashMatch = true;
            if (
                    oldPosition > -1
                            && getColumnType(masterMeta, i) == getColumnType(slaveMeta, oldPosition)
                            && isColumnIndexed(masterMeta, i) == isColumnIndexed(slaveMeta, oldPosition)
                            && (hashMatch = (getColumnHash(masterMeta, i) == getColumnHash(slaveMeta, oldPosition)))
            ) {
                Unsafe.getUnsafe().putInt(index + i * 8L, oldPosition + 1);
                Unsafe.getUnsafe().putInt(index + oldPosition * 8L + 4, i + 1);
            } else {
                Unsafe.getUnsafe().putLong(index + i * 8L, hashMatch ? 0 : -1);
            }
        }
        return pTransitionIndex;
    }

    public static LPSZ dFile(Path path, CharSequence columnName) {
        return path.concat(columnName).put(FILE_SUFFIX_D).$();
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

    public static void freeTransitionIndex(long address) {
        if (address == 0) {
            return;
        }
        Unsafe.free(address, Unsafe.getUnsafe().getInt(address), MemoryTag.NATIVE_DEFAULT);
    }

    public static long getColumnHash(MemoryR metaMem, int columnIndex) {
        return metaMem.getLong(META_OFFSET_COLUMN_TYPES + columnIndex * META_COLUMN_DATA_SIZE + 16);
    }

    public static long getColumnNameOffset(int columnCount) {
        return META_OFFSET_COLUMN_TYPES + columnCount * META_COLUMN_DATA_SIZE;
    }

    public static int getColumnType(MemoryR metaMem, int columnIndex) {
        return metaMem.getInt(META_OFFSET_COLUMN_TYPES + columnIndex * META_COLUMN_DATA_SIZE);
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
        return TX_OFFSET_MAP_WRITER_COUNT_32 + 4 + index * 8L;
    }

    public static long getSymbolWriterTransientIndexOffset(int index) {
        return getSymbolWriterIndexOffset(index) + Integer.BYTES;
    }

    public static LPSZ iFile(Path path, CharSequence columnName) {
        return path.concat(columnName).put(FILE_SUFFIX_I).$();
    }

    public static boolean isValidColumnName(CharSequence seq) {
        for (int i = 0, l = seq.length(); i < l; i++) {
            char c = seq.charAt(i);
            switch (c) {
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
            LOG.error().$("cannot open '").utf8(path).$("' to lock [errno=").$(ff.errno()).$(']').$();
            return -1L;
        }

        if (ff.lock(fd) != 0) {
            LOG.error().$("cannot lock '").utf8(path).$("' [errno=").$(ff.errno()).$(", fd=").$(fd).$(']').$();
            ff.close(fd);
            return -1L;
        }

        return fd;
    }

    public static void lockName(Path path) {
        path.put(".lock").$();
    }

    public static long mapRO(FilesFacade ff, long fd, long size, int memoryTag) {
        return mapRO(ff, fd, size, 0, memoryTag);
    }

    /**
     * Maps a file in read-only mode.
     * <p>
     * Important note. Linux requires the offset to be page aligned.
     * @param ff files facade, - intermediary to allow intercepting calls to the OS.
     * @param fd file descriptor, previously provided by one of openFile() functions
     * @param size size of the mapped file region
     * @param offset offset in file to begin mapping
     * @param memoryTag bucket to trace memory allocation calls
     * @return read-only memory address
     */
    public static long mapRO(FilesFacade ff, long fd, long size, long offset, int memoryTag) {
        assert offset % ff.getPageSize() == 0;
        final long address = ff.mmap(fd, size, offset, Files.MAP_RO, memoryTag);
        if (address == FilesFacade.MAP_FAILED) {
            throw CairoException.instance(ff.errno())
                    .put("could not mmap ")
                    .put(" [size=").put(size)
                    .put(", offset=").put(offset)
                    .put(", fd=").put(fd)
                    .put(", memUsed=").put(Unsafe.getMemUsed())
                    .put(", fileLen=").put(ff.length(fd))
                    .put(']');
        }
        return address;
    }

    public static long mapRW(FilesFacade ff, long fd, long size, int memoryTag) {
        return mapRW(ff, fd, size, 0, memoryTag);
    }

    /**
     * Maps a file in read-write mode.
     * <p>
     * Important note. Linux requires the offset to be page aligned.
     *
     * @param ff files facade, - intermediary to allow intercepting calls to the OS.
     * @param fd file descriptor, previously provided by one of openFile() functions. File has to be opened read-write
     * @param size size of the mapped file region
     * @param offset offset in file to begin mapping
     * @param memoryTag bucket to trace memory allocation calls
     * @return read-write memory address
     */
    public static long mapRW(FilesFacade ff, long fd, long size, long offset, int memoryTag) {
        assert offset % ff.getPageSize() == 0;
        allocateDiskSpace(ff, fd, size + offset);
        long addr = ff.mmap(fd, size, offset, Files.MAP_RW, memoryTag);
        if (addr > -1) {
            return addr;
        }
        throw CairoException.instance(ff.errno()).put("could not mmap column [fd=").put(fd).put(", size=").put(size).put(']');
    }

    public static long mapRWOrClose(FilesFacade ff, long fd, long size, int memoryTag) {
        try {
            return TableUtils.mapRW(ff, fd, size, memoryTag);
        } catch (CairoException e) {
            ff.close(fd);
            throw e;
        }
    }

    public static long mremap(
            FilesFacade ff,
            long fd,
            long prevAddress,
            long prevSize,
            long newSize,
            int mapMode,
            int memoryTag) {
        final long page = ff.mremap(fd, prevAddress, prevSize, newSize, 0, mapMode, memoryTag);
        if (page == FilesFacade.MAP_FAILED) {
            int errno = ff.errno();
            // Closing memory will truncate size to current append offset.
            // Since the failed resize can occur before append offset can be
            // explicitly set, we must assume that file size should be
            // equal to previous memory size
            throw CairoException.instance(errno).put("could not remap file [previousSize=").put(prevSize).put(", newSize=").put(newSize).put(", fd=").put(fd).put(']');
        }
        return page;
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

    public static long openRW(FilesFacade ff, LPSZ path, Log log) {
        final long fd = ff.openRW(path);
        if (fd > -1) {
            log.debug().$("open [file=").$(path).$(", fd=").$(fd).$(']').$();
            return fd;
        }
        throw CairoException.instance(ff.errno()).put("could not open read-write [file=").put(path).put(']');
    }

    public static int readIntOrFail(FilesFacade ff, long fd, long offset, long tempMem8b, Path path) {
        if (ff.read(fd, tempMem8b, Integer.BYTES, offset) != Integer.BYTES) {
            throw CairoException.instance(ff.errno()).put("Cannot read: ").put(path);
        }
        return Unsafe.getUnsafe().getInt(tempMem8b);
    }

    public static long readLongAtOffset(FilesFacade ff, Path path, long tempMem8b, long offset) {
        final long fd = TableUtils.openRO(ff, path, LOG);
        try {
            return readLongOrFail(ff, fd, offset, tempMem8b, path);
        } finally {
            ff.close(fd);
        }
    }

    public static long readLongOrFail(FilesFacade ff, long fd, long offset, long tempMem8b, @Nullable Path path) {
        if (ff.read(fd, tempMem8b, Long.BYTES, offset) != Long.BYTES) {
            if (path != null) {
                throw CairoException.instance(ff.errno()).put("could not read long [path=").put(path).put(", fd=").put(fd).put(", offset=").put(offset);
            }
            throw CairoException.instance(ff.errno()).put("could not read long [fd=").put(fd).put(", offset=").put(offset);
        }
        return Unsafe.getUnsafe().getLong(tempMem8b);
    }

    public static void renameOrFail(FilesFacade ff, Path src, Path dst) {
        if (!ff.rename(src, dst)) {
            throw CairoException.instance(ff.errno()).put("could not rename ").put(src).put(" -> ").put(dst);
        }
    }

    public static void resetTodoLog(FilesFacade ff, Path path, int rootLen, MemoryMARW mem) {
        mem.smallFile(ff, path.trimTo(rootLen).concat(TODO_FILE_NAME).$(), MemoryTag.MMAP_DEFAULT);
        mem.jumpTo(0);
        mem.putLong(24, 0); // txn check
        Unsafe.getUnsafe().storeFence();
        mem.putLong(8, 0); // hashLo
        mem.putLong(16, 0); // hashHi
        Unsafe.getUnsafe().storeFence();
        mem.putLong(0, 0); // txn
        mem.putLong(32, 0); // count
        mem.jumpTo(40);
    }

    public static void resetTxn(MemoryMW txMem, long baseOffset, int symbolMapCount, long txn, long dataVersion, long partitionTableVersion, long structureVersion) {
        // txn to let readers know table is being reset
        txMem.putLong(baseOffset + TX_OFFSET_TXN_64, txn);

        // transient row count
        txMem.putLong(baseOffset + TX_OFFSET_TRANSIENT_ROW_COUNT_64, 0);
        // fixed row count
        txMem.putLong(baseOffset + TX_OFFSET_FIXED_ROW_COUNT_64, 0);
        // min timestamp value in table
        txMem.putLong(baseOffset + TX_OFFSET_MIN_TIMESTAMP_64, Long.MAX_VALUE);
        // max timestamp value in table
        txMem.putLong(baseOffset + TX_OFFSET_MAX_TIMESTAMP_64, Long.MIN_VALUE);
        // structure version
        txMem.putLong(baseOffset + TX_OFFSET_STRUCT_VERSION_64, structureVersion);
        // data version
        txMem.putLong(baseOffset + TX_OFFSET_DATA_VERSION_64, dataVersion);
        // partition table version
        txMem.putLong(baseOffset + TX_OFFSET_PARTITION_TABLE_VERSION_64, partitionTableVersion);

        txMem.putInt(baseOffset + TX_OFFSET_MAP_WRITER_COUNT_32, symbolMapCount);
        for (int i = 0; i < symbolMapCount; i++) {
            long offset = getSymbolWriterIndexOffset(i);
            txMem.putInt(baseOffset + offset, 0);
            offset += Integer.BYTES;
            txMem.putInt(baseOffset + offset, 0);
        }

        // partition update count
        txMem.putInt(baseOffset + getPartitionTableSizeOffset(symbolMapCount), 0);
    }

    public static boolean schedulePurgeO3Partitions(MessageBus messageBus, String tableName, int partitionBy) {
        final MPSequence seq = messageBus.getO3PurgeDiscoveryPubSeq();
        while (true) {
            long cursor = seq.next();
            if (cursor > -1) {
                O3PurgeDiscoveryTask task = messageBus.getO3PurgeDiscoveryQueue().get(cursor);
                task.of(tableName, partitionBy);
                seq.done(cursor);
                return true;
            } else if (cursor == -1) {
                return false;
            }
        }
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
        return PartitionBy.setSinkForPartition(path.slash(), partitionBy, timestamp, calculatePartitionMax);
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

    public static void safeReadTxn(TxReader txReader, MicrosecondClock microsecondClock, long spinLockTimeoutUs) {
        long deadline = microsecondClock.getTicks() + spinLockTimeoutUs;
        if (txReader.unsafeReadVersion() == txReader.getVersion()) {
            LOG.debug().$("checked clean txn, version ").$(txReader.getVersion()).$(", txn=").$(txReader.getTxn()).$();
            return;
        }

        while (true) {
            if (txReader.unsafeLoadAll()) {
                LOG.debug().$("loaded clean txn, version ").$(txReader.getVersion())
                        .$(", offset=").$(txReader.getBaseOffset())
                        .$(", size=").$(txReader.getRecordSize())
                        .$(", txn=").$(txReader.getTxn()).$();
                // All good, snapshot read
                return;
            }
            // This is unlucky, sequences have changed while we were reading transaction data
            // We must discard and try again
            if (microsecondClock.getTicks() > deadline) {
                LOG.error().$("tx read timeout [timeout=").$(spinLockTimeoutUs).utf8("μs]").$();
                throw CairoException.instance(0).put("Transaction read timeout");
            }

            LOG.debug().$("loaded __dirty__ txn, version ").$(txReader.getVersion()).$();
            Os.pause();
        }
    }

    public static void validate(
            MemoryMR metaMem,
            LowerCaseCharSequenceIntHashMap nameIndex,
            int expectedVersion
    ) {
        try {
            long memSize = metaMem.size();
            if (memSize < META_OFFSET_COLUMN_TYPES) {
                throw CairoException.instance(0).put(". File is too small ").put(memSize);
            }
            final int metaVersion = metaMem.getInt(TableUtils.META_OFFSET_VERSION);
            if (expectedVersion != metaVersion) {
                throw validationException(metaMem)
                        .put("Metadata version does not match runtime version [expected=").put(expectedVersion)
                        .put(", actual=").put(metaVersion)
                        .put(']');
            }

            final int columnCount = metaMem.getInt(META_OFFSET_COUNT);
            long offset = getColumnNameOffset(columnCount);
            if (memSize < offset) {
                throw validationException(metaMem).put("File is too small, column types are missing ").put(memSize);
            }

            if (offset < columnCount || (
                    columnCount > 0 && (offset < 0 || offset >= memSize))) {
                throw validationException(metaMem).put("Incorrect columnCount: ").put(columnCount);
            }

            final int timestampIndex = metaMem.getInt(META_OFFSET_TIMESTAMP_INDEX);
            if (timestampIndex < -1 || timestampIndex >= columnCount) {
                throw validationException(metaMem).put("Timestamp index is outside of columnCount");
            }

            if (timestampIndex != -1) {
                int timestampType = getColumnType(metaMem, timestampIndex);
                if (!ColumnType.isTimestamp(timestampType)) {
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
                    if (!ColumnType.isSymbol(type)) {
                        throw validationException(metaMem).put("Index flag is only supported for SYMBOL").put(" at [").put(i).put(']');
                    }

                    if (getIndexBlockCapacity(metaMem, i) < 2) {
                        throw validationException(metaMem).put("Invalid index value block capacity ").put(getIndexBlockCapacity(metaMem, i)).put(" at [").put(i).put(']');
                    }
                }
            }

            // validate column names
            for (int i = 0; i < columnCount; i++) {
                if (offset + 4 >= memSize) {
                    throw validationException(metaMem).put("File is too small, column length for column ").put(i).put(" is missing");
                }
                int strLength = metaMem.getInt(offset);
                if (strLength == TableUtils.NULL_LEN) {
                    throw validationException(metaMem).put("NULL column name at [").put(i).put(']');
                }
                if (strLength < 1 || strLength > 255 || offset + Vm.getStorageLength(strLength) > memSize) {
                    // EXT4 and many others do not allow file name length > 255 bytes
                    throw validationException(metaMem)
                            .put("Column name length of ")
                            .put(strLength).put(" is invalid at offset ")
                            .put(offset);
                }

                CharSequence name = metaMem.getStr(offset);

                if (nameIndex.put(name, i)) {
                    offset += Vm.getStorageLength(name);
                } else {
                    throw validationException(metaMem).put("Duplicate column: ").put(name).put(" at [").put(i).put(']');
                }

                if (offset >= memSize) {
                    throw validationException(metaMem).put("File is too small, column names are missing ").put(memSize);
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

    public static void writeIntOrFail(FilesFacade ff, long fd, long offset, int value, long tempMem8b, Path path) {
        Unsafe.getUnsafe().putInt(tempMem8b, value);
        if (ff.write(fd, tempMem8b, Integer.BYTES, offset) != Integer.BYTES) {
            throw CairoException.instance(ff.errno())
                    .put("could not write 8 bytes [path=").put(path)
                    .put(", fd=").put(fd)
                    .put(", offset=").put(offset)
                    .put(", value=").put(value)
                    .put(']');
        }
    }

    public static void writeLongOrFail(FilesFacade ff, long fd, long offset, long value, long tempMem8b, Path path) {
        Unsafe.getUnsafe().putLong(tempMem8b, value);
        if (ff.write(fd, tempMem8b, Long.BYTES, offset) != Long.BYTES) {
            throw CairoException.instance(ff.errno())
                    .put("could not write 8 bytes [path=").put(path)
                    .put(", fd=").put(fd)
                    .put(", offset=").put(offset)
                    .put(", value=").put(value)
                    .put(']');
        }
    }

    static long getColumnFlags(MemoryR metaMem, int columnIndex) {
        return metaMem.getLong(META_OFFSET_COLUMN_TYPES + columnIndex * META_COLUMN_DATA_SIZE + 4);
    }

    static boolean isColumnIndexed(MemoryR metaMem, int columnIndex) {
        return (getColumnFlags(metaMem, columnIndex) & META_FLAG_BIT_INDEXED) != 0;
    }

    static boolean isSequential(MemoryR metaMem, int columnIndex) {
        return (getColumnFlags(metaMem, columnIndex) & META_FLAG_BIT_SEQUENTIAL) != 0;
    }

    static int getIndexBlockCapacity(MemoryR metaMem, int columnIndex) {
        return metaMem.getInt(META_OFFSET_COLUMN_TYPES + columnIndex * META_COLUMN_DATA_SIZE + 4 + 8);
    }

    static int openMetaSwapFile(FilesFacade ff, MemoryMA mem, Path path, int rootLen, int retryCount) {
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
                        mem.smallFile(ff, path, MemoryTag.MMAP_DEFAULT);
                        mem.jumpTo(0);
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

    static void openMetaSwapFileByIndex(FilesFacade ff, MemoryMA mem, Path path, int rootLen, int swapIndex) {
        try {
            path.concat(META_SWAP_FILE_NAME);
            if (swapIndex > 0) {
                path.put('.').put(swapIndex);
            }
            path.$();
            mem.smallFile(ff, path, MemoryTag.MMAP_DEFAULT);
        } finally {
            path.trimTo(rootLen);
        }
    }

    private static CairoException validationException(MemoryMR mem) {
        return CairoException.instance(CairoException.METADATA_VALIDATION).put("Invalid metadata at fd=").put(mem.getFd()).put(". ");
    }

    static void createDirsOrFail(FilesFacade ff, Path path, int mkDirMode) {
        if (ff.mkdirs(path, mkDirMode) != 0) {
            throw CairoException.instance(ff.errno()).put("could not create directories [file=").put(path).put(']');
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
                    long mappedMem = mapRO(ff, fd, fileSize, MemoryTag.MMAP_DEFAULT);
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
                        ff.munmap(mappedMem, fileSize, MemoryTag.MMAP_DEFAULT);
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

    static boolean isEntryToBeProcessed(long address, int index) {
        if (Unsafe.getUnsafe().getByte(address + index) == -1) {
            return false;
        }
        Unsafe.getUnsafe().putByte(address + index, (byte) -1);
        return true;
    }

    public interface FailureCloseable {
        void close(long prevSize);
    }

}
