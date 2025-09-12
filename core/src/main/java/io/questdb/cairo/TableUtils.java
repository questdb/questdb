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

package io.questdb.cairo;

import io.questdb.MessageBus;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.cairo.vm.api.MemoryMAR;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.cairo.vm.api.MemoryMW;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.griffin.AnyRecordMetadata;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.MPSequence;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;
import io.questdb.tasks.O3PartitionPurgeTask;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.ParanoiaState.VM_PARANOIA_MODE;
import static io.questdb.cairo.MapWriter.createSymbolMapFiles;
import static io.questdb.cairo.wal.WalUtils.CONVERT_FILE_NAME;

public final class TableUtils {
    public static final int ANY_TABLE_VERSION = -1;
    public static final String ATTACHABLE_DIR_MARKER = ".attachable";
    public static final String CHECKPOINT_DIRECTORY = ".checkpoint";
    public static final String CHECKPOINT_LEGACY_META_FILE_NAME = "_snapshot";
    public static final String CHECKPOINT_LEGACY_META_FILE_NAME_TXT = "_snapshot.txt";
    public static final String CHECKPOINT_META_FILE_NAME = "_checkpoint_meta.d";
    public static final long COLUMN_NAME_TXN_NONE = -1L;
    public static final String COLUMN_VERSION_FILE_NAME = "_cv";
    public static final String DEFAULT_PARTITION_NAME = "default";
    public static final String DETACHED_DIR_MARKER = ".detached";
    public static final long ESTIMATED_VAR_COL_SIZE = 28;
    public static final String FILE_SUFFIX_D = ".d";
    public static final String FILE_SUFFIX_I = ".i";
    public static final int INITIAL_TXN = 0;
    public static final String LEGACY_CHECKPOINT_DIRECTORY = "snapshot";
    public static final int LONGS_PER_TX_ATTACHED_PARTITION = 4;
    public static final int LONGS_PER_TX_ATTACHED_PARTITION_MSB = Numbers.msb(LONGS_PER_TX_ATTACHED_PARTITION);
    public static final long META_COLUMN_DATA_SIZE = 32;
    public static final String META_FILE_NAME = "_meta";
    public static final short META_FORMAT_MINOR_VERSION_LATEST = 1;
    public static final long META_OFFSET_COLUMN_TYPES = 128;
    public static final long META_OFFSET_COUNT = 0;
    public static final long META_OFFSET_MAX_UNCOMMITTED_ROWS = 20; // INT
    public static final long META_OFFSET_METADATA_VERSION = 32; // LONG
    public static final long META_OFFSET_O3_MAX_LAG = 24; // LONG
    // INT - symbol map count, this is a variable part of transaction file
    // below this offset we will have INT values for symbol map size
    public static final long META_OFFSET_PARTITION_BY = 4;
    public static final long META_OFFSET_TABLE_ID = 16;
    public static final long META_OFFSET_TIMESTAMP_INDEX = 8;
    public static final long META_OFFSET_VERSION = 12;
    public static final long META_OFFSET_WAL_ENABLED = 40; // BOOLEAN
    public static final long META_OFFSET_META_FORMAT_MINOR_VERSION = META_OFFSET_WAL_ENABLED + 1; // INT
    public static final long META_OFFSET_TTL_HOURS_OR_MONTHS = META_OFFSET_META_FORMAT_MINOR_VERSION + 4; // INT
    public static final String META_PREV_FILE_NAME = "_meta.prev";
    public static final String META_SWAP_FILE_NAME = "_meta.swp";
    public static final int MIN_INDEX_VALUE_BLOCK_SIZE = Numbers.ceilPow2(4);
    // 24-byte header left empty for possible future use
    // in case we decide to support ALTER MAT VIEW, and modify mat view metadata
    public static final int NULL_LEN = -1;
    public static final String PARQUET_PARTITION_NAME = "data.parquet";
    public static final String RESTORE_FROM_CHECKPOINT_TRIGGER_FILE_NAME = "_restore";
    public static final String SYMBOL_KEY_REMAP_FILE_SUFFIX = ".r";
    public static final char SYSTEM_TABLE_NAME_SUFFIX = '~';
    public static final int TABLE_DOES_NOT_EXIST = 1;
    public static final int TABLE_EXISTS = 0;
    public static final String TABLE_NAME_FILE = "_name";
    public static final int TABLE_RESERVED = 2;
    public static final int TABLE_TYPE_MAT = 2;
    public static final int TABLE_TYPE_NON_WAL = 0;
    public static final int TABLE_TYPE_WAL = 1;
    public static final String TAB_INDEX_FILE_NAME = "_tab_index.d";
    public static final String TODO_FILE_NAME = "_todo_";
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
    public static final String TXN_FILE_NAME = "_txn";
    public static final String TXN_SCOREBOARD_FILE_NAME = "_txn_scoreboard";
    // transaction file structure
    // @formatter:off
    public static final int TX_BASE_HEADER_SECTION_PADDING = 12; // Add some free space into header for future use
    public static final long TX_BASE_OFFSET_VERSION_64 = 0;
    public static final long TX_BASE_OFFSET_A_32 = TX_BASE_OFFSET_VERSION_64 + 8;
    public static final long TX_BASE_OFFSET_SYMBOLS_SIZE_A_32 = TX_BASE_OFFSET_A_32 + 4;
    public static final long TX_BASE_OFFSET_PARTITIONS_SIZE_A_32 = TX_BASE_OFFSET_SYMBOLS_SIZE_A_32 + 4;
    public static final long TX_BASE_OFFSET_B_32 = TX_BASE_OFFSET_PARTITIONS_SIZE_A_32 + 4 + TX_BASE_HEADER_SECTION_PADDING;
    public static final long TX_BASE_OFFSET_SYMBOLS_SIZE_B_32 = TX_BASE_OFFSET_B_32 + 4;
    public static final long TX_BASE_OFFSET_PARTITIONS_SIZE_B_32 = TX_BASE_OFFSET_SYMBOLS_SIZE_B_32 + 4;
    public static final int TX_BASE_HEADER_SIZE = (int) Math.max(TX_BASE_OFFSET_PARTITIONS_SIZE_B_32 + 4 + TX_BASE_HEADER_SECTION_PADDING, 64);
    public static final long TX_OFFSET_MAP_WRITER_COUNT_32 = 128;
    public static final long TX_OFFSET_TXN_64 = 0;
    public static final long TX_OFFSET_TRANSIENT_ROW_COUNT_64 = TX_OFFSET_TXN_64 + 8;
    public static final long TX_OFFSET_FIXED_ROW_COUNT_64 = TX_OFFSET_TRANSIENT_ROW_COUNT_64 + 8;
    public static final long TX_OFFSET_MIN_TIMESTAMP_64 = TX_OFFSET_FIXED_ROW_COUNT_64 + 8;
    public static final long TX_OFFSET_MAX_TIMESTAMP_64 = TX_OFFSET_MIN_TIMESTAMP_64 + 8;
    public static final long TX_OFFSET_STRUCT_VERSION_64 = TX_OFFSET_MAX_TIMESTAMP_64 + 8;
    public static final long TX_OFFSET_DATA_VERSION_64 = TX_OFFSET_STRUCT_VERSION_64 + 8;
    public static final long TX_OFFSET_PARTITION_TABLE_VERSION_64 = TX_OFFSET_DATA_VERSION_64 + 8;
    public static final long TX_OFFSET_COLUMN_VERSION_64 = TX_OFFSET_PARTITION_TABLE_VERSION_64 + 8;
    public static final long TX_OFFSET_TRUNCATE_VERSION_64 = TX_OFFSET_COLUMN_VERSION_64 + 8;
    public static final long TX_OFFSET_SEQ_TXN_64 = TX_OFFSET_TRUNCATE_VERSION_64 + 8;
    public static final long TX_OFFSET_CHECKSUM_32 = TX_OFFSET_SEQ_TXN_64 + 8;
    public static final long TX_OFFSET_LAG_TXN_COUNT_32 = TX_OFFSET_CHECKSUM_32 + 4;
    public static final long TX_OFFSET_LAG_ROW_COUNT_32 = TX_OFFSET_LAG_TXN_COUNT_32 + 4;
    public static final long TX_OFFSET_LAG_MIN_TIMESTAMP_64 = TX_OFFSET_LAG_ROW_COUNT_32 + 4;
    public static final long TX_OFFSET_LAG_MAX_TIMESTAMP_64 = TX_OFFSET_LAG_MIN_TIMESTAMP_64 + 8;
    // @formatter:on
    public static final int TX_RECORD_HEADER_SIZE = (int) TX_OFFSET_MAP_WRITER_COUNT_32 + Integer.BYTES;
    public static final String UPGRADE_FILE_NAME = "_upgrade.d";
    static final int COLUMN_VERSION_FILE_HEADER_SIZE = 40;
    static final int META_FLAG_BIT_INDEXED = 1;
    static final int META_FLAG_BIT_SYMBOL_CACHE = 1 << 2;
    static final int META_FLAG_BIT_DEDUP_KEY = META_FLAG_BIT_SYMBOL_CACHE << 1;
    static final byte TODO_RESTORE_META = 2;
    static final byte TODO_TRUNCATE = 1;
    private static final int EMPTY_TABLE_LAG_CHECKSUM = calculateTxnLagChecksum(0, 0, 0, Long.MAX_VALUE, Long.MIN_VALUE, 0);
    private static final Log LOG = LogFactory.getLog(TableUtils.class);
    private static final int MAX_INDEX_VALUE_BLOCK_SIZE = Numbers.ceilPow2(8 * 1024 * 1024);
    private static final int MAX_SYMBOL_CAPACITY = Numbers.ceilPow2(Integer.MAX_VALUE);
    private static final int MAX_SYMBOL_CAPACITY_CACHED = Numbers.ceilPow2(30_000_000);
    private static final int MIN_SYMBOL_CAPACITY = 2;

    private TableUtils() {
    }

    public static void allocateDiskSpace(FilesFacade ff, long fd, long size) {
        if (ff.length(fd) < size && !ff.allocate(fd, size)) {
            throw CairoException.critical(ff.errno()).put("No space left [size=").put(size).put(", fd=").put(fd).put(']');
        }
    }

    public static void allocateDiskSpaceToPage(FilesFacade ff, long fd, long size) {
        size = Files.ceilPageSize(size);
        allocateDiskSpace(ff, fd, size);
    }

    public static int calculateMetaFormatMinorVersionField(long metadataVersion, int columnCount) {
        // Metadata Format Minor Version field is 2 shorts:
        // - Low short is a checksum that changes with every update to _meta
        // - High short is TableUtils.META_FORMAT_MINOR_VERSION_LATEST
        // When reading the Metadata Format Minor Version field from the metadata record, use the checksum
        // to decide whether to trust the version stored in this field.
        return Numbers.encodeLowHighShorts(
                checksumForMetaFormatMinorVersionField(metadataVersion, columnCount),
                META_FORMAT_MINOR_VERSION_LATEST
        );
    }

    public static int calculateTxRecordSize(int bytesSymbols, int bytesPartitions) {
        // Note that 32 bit symbol length is included in TX_RECORD_HEADER_SIZE,
        // hence the record size is head + symbol sizes + 32bit partition count + bytes to store partitions
        return TX_RECORD_HEADER_SIZE + bytesSymbols + Integer.BYTES + bytesPartitions;
    }

    @SuppressWarnings("JavaExistingMethodCanBeUsed")
    // the mig methods are deliberately standalone so that between old versions
    // does not regress if the main code changes
    public static int calculateTxnLagChecksum(long txn, long seqTxn, int lagRowCount, long lagMinTimestamp, long lagMaxTimestamp, int lagTxnCount) {
        long checkSum = lagMinTimestamp;
        checkSum = checkSum * 31 + lagMaxTimestamp;
        checkSum = checkSum * 31 + txn;
        checkSum = checkSum * 31 + seqTxn;
        checkSum = checkSum * 31 + lagRowCount;
        checkSum = checkSum * 31 + lagTxnCount;
        //noinspection UseHashCodeMethodInspection
        return (int) (checkSum ^ (checkSum >>> 32));
    }

    public static int changeColumnTypeInMetadata(
            CharSequence columnName,
            int columnType,
            int symbolCapacity,
            boolean symbolCacheFlag,
            boolean isIndexed,
            int indexValueBlockCapacity,
            LowerCaseCharSequenceIntHashMap columnNameIndexMap,
            ObjList<TableColumnMetadata> columnMetadata
    ) {
        int existingIndex = columnNameIndexMap.get(columnName);
        if (existingIndex < 0) {
            throw CairoException.nonCritical().put("cannot change type, column '").put(columnName).put("' does not exist");
        }
        String columnNameStr = columnMetadata.getQuick(existingIndex).getColumnName();
        int columnIndex = columnMetadata.size();
        columnMetadata.add(
                new TableColumnMetadata(
                        columnNameStr,
                        columnType,
                        isIndexed,
                        indexValueBlockCapacity,
                        false,
                        null,
                        columnIndex,
                        false,
                        existingIndex + 1, // replacing column index by convention can be 0 if not in use
                        symbolCacheFlag,
                        symbolCapacity
                )
        );
        columnMetadata.getQuick(existingIndex).markDeleted();
        columnNameIndexMap.put(columnNameStr, columnIndex);
        return existingIndex;
    }

    public static LPSZ charFileName(Path path, CharSequence columnName, long columnNameTxn) {
        path.concat(columnName).put(".c");
        if (columnNameTxn > COLUMN_NAME_TXN_NONE) {
            path.put('.').put(columnNameTxn);
        }
        return path.$();
    }

    public static long checkMemSize(MemoryMR metaMem, long minSize) {
        final long memSize = metaMem.size();
        if (memSize < minSize) {
            throw CairoException.critical(0).put("File is too small, size=").put(memSize).put(", required=").put(minSize);
        }
        return memSize;
    }

    public static short checksumForMetaFormatMinorVersionField(long metadataVersion, int columnCount) {
        int metaVersionInt = Numbers.decodeLowInt(metadataVersion) ^ Numbers.decodeHighInt(metadataVersion);
        int checksumInt = 13 * metaVersionInt + 37 * columnCount;
        short checksum = (short) (Numbers.decodeLowShort(checksumInt) ^ Numbers.decodeHighShort(checksumInt));
        if (checksum == 0) {
            checksum = -1337;
        }
        return checksum;
    }

    public static int compressColumnCount(RecordMetadata metadata) {
        int count = 0;
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            if (metadata.getColumnType(i) > 0) {
                count++;
            }
        }
        return count;
    }

    public static void createColumnVersionFile(MemoryMARW mem) {
        // Create page of 0s for Column Version file "_cv"
        mem.extend(COLUMN_VERSION_FILE_HEADER_SIZE);
        mem.jumpTo(COLUMN_VERSION_FILE_HEADER_SIZE);
        mem.zero();
    }

    public static void createConvertFile(FilesFacade ff, Path path, byte walFlag) {
        long addr = 0;
        long fd = -1;
        try {
            fd = ff.openRW(path.concat(CONVERT_FILE_NAME).$(), CairoConfiguration.O_NONE);
            if (fd < 1) {
                throw CairoException.critical(ff.errno()).put("Could not open file [path=").put(path).put(']');
            }
            addr = Unsafe.malloc(Byte.BYTES, MemoryTag.NATIVE_TABLE_WAL_WRITER);
            if (addr < 1) {
                throw CairoException.critical(ff.errno()).put("Could not allocate 1 byte");
            }
            Unsafe.getUnsafe().putByte(addr, walFlag);
            ff.write(fd, addr, Byte.BYTES, 0);
        } finally {
            if (addr > 0) {
                Unsafe.free(addr, Byte.BYTES, MemoryTag.NATIVE_TABLE_WAL_WRITER);
            }
            ff.close(fd);
        }
    }

    @NotNull
    public static Function createCursorFunction(
            FunctionParser functionParser,
            @NotNull QueryModel model,
            @NotNull SqlExecutionContext executionContext
    ) throws SqlException {
        final ExpressionNode tableNameExpr = model.getTableNameExpr();
        final Function function = functionParser.parseFunction(
                tableNameExpr,
                AnyRecordMetadata.INSTANCE,
                executionContext
        );
        if (!ColumnType.isCursor(function.getType())) {
            Misc.free(function);
            throw SqlException.$(tableNameExpr.position, "function must return CURSOR");
        }
        return function;
    }

    public static void createTable(
            CairoConfiguration configuration,
            MemoryMARW memory,
            Path path,
            TableStructure structure,
            int tableVersion,
            int tableId,
            CharSequence dirName
    ) {
        final FilesFacade ff = configuration.getFilesFacade();
        final CharSequence root = configuration.getDbRoot();
        final int mkDirMode = configuration.getMkDirMode();
        createTable(ff, root, mkDirMode, memory, path, structure, tableVersion, tableId, dirName);
    }

    public static void createTable(
            FilesFacade ff,
            CharSequence root,
            int mkDirMode,
            MemoryMARW memory,
            Path path,
            TableStructure structure,
            int tableVersion,
            int tableId,
            CharSequence dirName
    ) {
        createTable(ff, root, mkDirMode, memory, path, dirName, structure, tableVersion, tableId);
    }

    public static void createTable(
            FilesFacade ff,
            CharSequence root,
            int mkDirMode,
            TableStructure structure,
            int tableVersion,
            int tableId,
            CharSequence dirName
    ) {
        try (
                Path path = new Path();
                MemoryMARW mem = Vm.getCMARWInstance()
        ) {
            createTable(ff, root, mkDirMode, mem, path, dirName, structure, tableVersion, tableId);
        }
    }

    public static void createTable(
            FilesFacade ff,
            CharSequence root,
            int mkDirMode,
            MemoryMARW memory,
            Path path,
            CharSequence tableDir,
            TableStructure structure,
            int tableVersion,
            int tableId
    ) {
        createTableOrMatView(ff, root, mkDirMode, memory, null, path, tableDir, structure, tableVersion, tableId);
    }

    public static void createTableNameFile(MemoryMAR mem, CharSequence charSequence) {
        mem.putStr(charSequence);
        mem.putByte((byte) 0);
        mem.sync(false);
        mem.close(true, Vm.TRUNCATE_TO_POINTER);
    }

    public static void createTableOrMatView(
            FilesFacade ff,
            CharSequence root,
            int mkDirMode,
            MemoryMARW memory,
            @Nullable BlockFileWriter blockFileWriter,
            Path path,
            CharSequence tableDir,
            TableStructure structure,
            int tableVersion,
            int tableId
    ) {
        LOG.debug().$("create table [name=").$safe(tableDir).I$();
        path.of(root).concat(tableDir).$();
        if (ff.isDirOrSoftLinkDir(path.$())) {
            throw CairoException.critical(ff.errno()).put("table directory already exists [path=").put(path).put(']');
        }
        int rootLen = path.size();
        boolean dirCreated = false;
        try {
            if (ff.mkdirs(path.slash(), mkDirMode) != 0) {
                throw CairoException.critical(ff.errno()).put("could not create [dir=").put(path.trimTo(rootLen).$()).put(']');
            }
            dirCreated = true;
            createTableOrMatViewFiles(ff, memory, blockFileWriter, path, rootLen, tableDir, structure, tableVersion, tableId);
        } catch (Throwable e) {
            if (dirCreated) {
                ff.rmdir(path.trimTo(rootLen).slash());
            }
            throw e;
        } finally {
            path.trimTo(rootLen);
        }
    }

    public static void createTableOrMatViewFiles(
            FilesFacade ff,
            MemoryMARW memory,
            @Nullable BlockFileWriter blockFileWriter,
            Path path,
            int rootLen,
            CharSequence tableDir,
            TableStructure structure,
            int tableVersion,
            int tableId
    ) {
        createTableOrMatViewFiles(ff, memory, blockFileWriter, path, rootLen, tableDir, structure, tableVersion, tableId, TXN_FILE_NAME);
    }

    public static void createTableOrMatViewFiles(
            FilesFacade ff,
            MemoryMARW memory,
            @Nullable BlockFileWriter blockFileWriter,
            Path path,
            int rootLen,
            CharSequence tableDir,
            TableStructure structure,
            int tableVersion,
            int tableId,
            CharSequence txnFileName
    ) {
        final long dirFd = !ff.isRestrictedFileSystem() ? TableUtils.openRONoCache(ff, path.trimTo(rootLen).$(), LOG) : 0;
        try (MemoryMARW mem = memory) {
            mem.smallFile(ff, path.trimTo(rootLen).concat(META_FILE_NAME).$(), MemoryTag.MMAP_DEFAULT);
            mem.jumpTo(0);
            final int count = structure.getColumnCount();
            path.trimTo(rootLen);
            writeMetadata(structure, tableVersion, tableId, mem);
            mem.sync(false);

            // create symbol maps
            int symbolMapCount = 0;
            for (int i = 0; i < count; i++) {
                if (ColumnType.isSymbol(structure.getColumnType(i))) {
                    createSymbolMapFiles(
                            ff,
                            mem,
                            path.trimTo(rootLen),
                            structure.getColumnName(i),
                            COLUMN_NAME_TXN_NONE,
                            structure.getSymbolCapacity(i),
                            structure.getSymbolCacheFlag(i)
                    );
                    symbolMapCount++;
                }
            }
            mem.smallFile(ff, path.trimTo(rootLen).concat(COLUMN_VERSION_FILE_NAME).$(), MemoryTag.MMAP_DEFAULT);
            createColumnVersionFile(mem);
            mem.sync(false);
            mem.close();

            resetTodoLog(ff, path, rootLen, mem);
            // allocate txn scoreboard
            path.trimTo(rootLen).concat(TXN_SCOREBOARD_FILE_NAME).$();

            mem.smallFile(ff, path.trimTo(rootLen).concat(TABLE_NAME_FILE).$(), MemoryTag.MMAP_DEFAULT);
            createTableNameFile(mem, getTableNameFromDirName(tableDir));

            if (structure.isMatView()) {
                assert blockFileWriter != null;
                try (BlockFileWriter writer = blockFileWriter) {
                    writer.of(path.trimTo(rootLen).concat(MatViewDefinition.MAT_VIEW_DEFINITION_FILE_NAME).$());
                    MatViewDefinition.append(structure.getMatViewDefinition(), writer);
                }
            }

            // Create TXN file last, it's used to determine if table exists
            mem.smallFile(ff, path.trimTo(rootLen).concat(txnFileName).$(), MemoryTag.MMAP_DEFAULT);
            createTxn(mem, symbolMapCount, 0L, 0L, INITIAL_TXN, 0L, 0L, 0L, 0L);
            mem.sync(false);
        } finally {
            if (dirFd > 0) {
                ff.fsyncAndClose(dirFd);
            }
        }
    }

    public static void createTableOrMatViewInVolume(
            FilesFacade ff,
            CharSequence root,
            int mkDirMode,
            MemoryMARW memory,
            Path path,
            CharSequence tableDir,
            TableStructure structure,
            int tableVersion,
            int tableId
    ) {
        createTableOrMatViewInVolume(ff, root, mkDirMode, memory, null, path, tableDir, structure, tableVersion, tableId);
    }

    public static void createTableOrMatViewInVolume(
            FilesFacade ff,
            CharSequence root,
            int mkDirMode,
            MemoryMARW memory,
            @Nullable BlockFileWriter blockFileWriter,
            Path path,
            CharSequence tableDir,
            TableStructure structure,
            int tableVersion,
            int tableId
    ) {
        LOG.info().$("create table in volume [path=").$(path).I$();
        Path normalPath = Path.getThreadLocal2(root).concat(tableDir);
        assert normalPath != path;
        if (ff.isDirOrSoftLinkDir(normalPath.$())) {
            throw CairoException.critical(ff.errno()).put("table directory already exists [path=").put(normalPath).put(']');
        }
        // path has been set by CREATE TABLE ... [IN VOLUME 'path'].
        // it is a valid directory, or link to a directory, checked at bootstrap
        if (ff.isDirOrSoftLinkDir(path.$())) {
            throw CairoException.critical(ff.errno()).put("table directory already exists in volume [path=").put(path).put(']');
        }
        int rootLen = path.size();
        try {
            if (ff.mkdirs(path.slash(), mkDirMode) != 0) {
                throw CairoException.critical(ff.errno()).put("could not create [dir=").put(path).put(']');
            }
            if (ff.softLink(path.trimTo(rootLen).$(), normalPath.$()) != 0) {
                if (!ff.rmdir(path.slash())) {
                    LOG.error().$("cannot remove table directory in volume [errno=").$(ff.errno()).$(", path=").$(path.trimTo(rootLen).$()).I$();
                }
                throw CairoException.critical(ff.errno()).put("could not create soft link [src=").put(path.trimTo(rootLen).$()).put(", tableDir=").put(tableDir).put(']');
            }
            createTableOrMatViewFiles(ff, memory, blockFileWriter, path, rootLen, tableDir, structure, tableVersion, tableId);
        } finally {
            path.trimTo(rootLen);
        }
    }

    public static void createTxn(
            MemoryMW txMem,
            int symbolMapCount,
            long txn,
            long seqTxn,
            long dataVersion,
            long partitionTableVersion,
            long structureVersion,
            long columnVersion,
            long truncateVersion
    ) {
        txMem.putInt(TX_BASE_OFFSET_A_32, TX_BASE_HEADER_SIZE);
        txMem.putInt(TX_BASE_OFFSET_SYMBOLS_SIZE_A_32, symbolMapCount * 8);
        txMem.putInt(TX_BASE_OFFSET_PARTITIONS_SIZE_A_32, 0);
        resetTxn(
                txMem,
                TX_BASE_HEADER_SIZE,
                symbolMapCount,
                txn,
                seqTxn,
                dataVersion,
                partitionTableVersion,
                structureVersion,
                columnVersion,
                truncateVersion
        );
        txMem.setTruncateSize(TX_BASE_HEADER_SIZE + TX_RECORD_HEADER_SIZE);
    }

    public static LPSZ dFile(Path path, @NotNull CharSequence columnName, long columnTxn) {
        path.concat(columnName).put(FILE_SUFFIX_D);
        if (columnTxn > COLUMN_NAME_TXN_NONE) {
            path.put('.').put(columnTxn);
        }
        return path.$();
    }

    public static LPSZ dFile(Path path, CharSequence columnName) {
        return dFile(path, columnName, COLUMN_NAME_TXN_NONE);
    }

    public static long estimateAvgRecordSize(RecordMetadata metadata) {
        long recSize = 0;
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            int columnType = metadata.getColumnType(i);
            if (ColumnType.isVarSize(columnType)) {
                // Estimate size of variable length column as 28 bytes
                recSize += ESTIMATED_VAR_COL_SIZE;
            } else if (columnType > 0) {
                recSize += ColumnType.sizeOf(columnType);
            }
        }
        return recSize;
    }

    public static int exists(FilesFacade ff, Path path, CharSequence root, CharSequence name) {
        return exists(ff, path.of(root).concat(name));
    }

    public static int exists(FilesFacade ff, Path path, CharSequence root, Utf8Sequence name) {
        return exists(ff, path.of(root).concat(name));
    }

    public static int existsInVolume(FilesFacade ff, Path volumePath, CharSequence name) {
        return exists(ff, volumePath.concat(name));
    }

    public static void freeTransitionIndex(long address) {
        if (address == 0) {
            return;
        }
        Unsafe.free(address, Unsafe.getUnsafe().getInt(address), MemoryTag.NATIVE_TABLE_READER);
    }

    public static int getColumnCount(MemoryMR metaMem, long offset) {
        final int columnCount = metaMem.getInt(offset);
        if (columnCount < 0) {
            throw validationException(metaMem).put("Incorrect columnCount: ").put(columnCount);
        }
        return columnCount;
    }

    public static CharSequence getColumnName(MemoryMR metaMem, long memSize, long offset, int columnIndex) {
        final int strLength = getInt(metaMem, memSize, offset);
        if (strLength == TableUtils.NULL_LEN) {
            throw validationException(metaMem).put("NULL column name at [").put(columnIndex).put(']');
        }
        return getCharSequence(metaMem, memSize, offset, strLength);
    }

    public static long getColumnNameOffset(int columnCount) {
        return META_OFFSET_COLUMN_TYPES + columnCount * META_COLUMN_DATA_SIZE;
    }

    public static int getColumnType(MemoryR metaMem, int columnIndex) {
        return metaMem.getInt(META_OFFSET_COLUMN_TYPES + columnIndex * META_COLUMN_DATA_SIZE);
    }

    public static int getColumnType(MemoryMR metaMem, long memSize, long offset, int columnIndex) {
        final int type = getInt(metaMem, memSize, offset);
        if (type >= 0 && ColumnType.sizeOf(type) == -1) {
            throw validationException(metaMem).put("Invalid column type ").put(type).put(" at [").put(columnIndex).put(']');
        }
        return type;
    }

    public static int getInt(MemoryR metaMem, long memSize, long offset) {
        if (memSize < offset + Integer.BYTES) {
            throw CairoException.critical(0).put("File is too small, size=").put(memSize).put(", required=").put(offset + Integer.BYTES);
        }
        return metaMem.getInt(offset);
    }

    public static int getMaxUncommittedRows(TableRecordMetadata metadata, CairoEngine engine) {
        if (!metadata.isWalEnabled() && metadata instanceof TableWriterMetadata) {
            return ((TableWriterMetadata) metadata).getMaxUncommittedRows();
        }
        try (TableMetadata tableMetadata = engine.getTableMetadata(metadata.getTableToken())) {
            return tableMetadata.getMaxUncommittedRows();
        }
    }

    public static long getNullLong(int columnType, @SuppressWarnings("unused") int longIndex) {
        // In theory, we can have a column type where `NULL` value will be different `LONG` values,
        // then this should return different values on longIndex. At the moment there are no such types.
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.BOOLEAN:
            case ColumnType.BYTE:
            case ColumnType.CHAR:
            case ColumnType.SHORT:
                return 0L;
            case ColumnType.SYMBOL:
                return Numbers.encodeLowHighInts(SymbolTable.VALUE_IS_NULL, SymbolTable.VALUE_IS_NULL);
            case ColumnType.FLOAT:
                return Numbers.encodeLowHighInts(Float.floatToIntBits(Float.NaN), Float.floatToIntBits(Float.NaN));
            case ColumnType.DOUBLE:
                return Double.doubleToLongBits(Double.NaN);
            case ColumnType.INT:
                return Numbers.encodeLowHighInts(Numbers.INT_NULL, Numbers.INT_NULL);
            case ColumnType.LONG256:
            case ColumnType.LONG:
            case ColumnType.DATE:
            case ColumnType.TIMESTAMP:
            case ColumnType.LONG128:
            case ColumnType.UUID:
            case ColumnType.INTERVAL:
                // Long128, UUID, and INTERVAL are null when all 2 longs are NaNs
                // Long256 is null when all 4 longs are NaNs
                return Numbers.LONG_NULL;
            case ColumnType.GEOBYTE:
            case ColumnType.GEOLONG:
            case ColumnType.GEOSHORT:
            case ColumnType.GEOINT:
                return GeoHashes.NULL;
            case ColumnType.IPv4:
                return Numbers.IPv4_NULL;
            case ColumnType.VARCHAR:
            case ColumnType.BINARY:
            case ColumnType.ARRAY:
                return NULL_LEN;
            case ColumnType.STRING:
                return Numbers.encodeLowHighInts(NULL_LEN, NULL_LEN);
            default:
                assert false : "Invalid column type: " + columnType;
                return 0;
        }
    }

    public static long getO3MaxLag(TableRecordMetadata metadata, CairoEngine engine) {
        if (!metadata.isWalEnabled()) {
            if (metadata instanceof TableWriterMetadata) {
                return ((TableWriterMetadata) metadata).getO3MaxLag();
            }

            try (TableMetadata tableMetadata = engine.getTableMetadata(metadata.getTableToken())) {
                return tableMetadata.getO3MaxLag();
            }
        }
        // Does not have effect for WAL enabled tables
        return 0;
    }

    public static int getPartitionBy(TableRecordMetadata metadata, CairoEngine engine) {
        if (!metadata.isWalEnabled() && metadata instanceof TableWriterMetadata) {
            return ((TableWriterMetadata) metadata).getPartitionBy();
        }
        try (TableMetadata tableMetadata = engine.getTableMetadata(metadata.getTableToken())) {
            return tableMetadata.getPartitionBy();
        }
    }

    public static long getPartitionTableIndexOffset(long partitionTableOffset, int index) {
        return partitionTableOffset + 4 + index * 8L;
    }

    public static long getPartitionTableSizeOffset(int symbolWriterCount) {
        return getSymbolWriterIndexOffset(symbolWriterCount);
    }

    public static int getReplacingColumnIndex(MemoryR metaMem, int columnIndex) {
        return metaMem.getInt(META_OFFSET_COLUMN_TYPES + columnIndex * META_COLUMN_DATA_SIZE + 4 + 8 + 4 + 8) - 1;
    }

    public static int getSymbolCapacity(MemoryR metaMem, int columnIndex) {
        return metaMem.getInt(META_OFFSET_COLUMN_TYPES + columnIndex * META_COLUMN_DATA_SIZE + 4 + 8 + 4);
    }

    public static long getSymbolWriterIndexOffset(int index) {
        return TX_OFFSET_MAP_WRITER_COUNT_32 + Integer.BYTES + (long) index * Long.BYTES;
    }

    public static long getSymbolWriterTransientIndexOffset(int index) {
        return getSymbolWriterIndexOffset(index) + Integer.BYTES;
    }

    @NotNull
    public static String getTableDir(boolean mangleDirNames, @NotNull String tableName, int tableId, boolean isWal) {
        String dirName = tableName;
        if (isWal) {
            dirName += TableUtils.SYSTEM_TABLE_NAME_SUFFIX;
            dirName += tableId;
        } else if (mangleDirNames) {
            dirName += TableUtils.SYSTEM_TABLE_NAME_SUFFIX;
        }
        return dirName;
    }

    public static CharSequence getTableNameFromDirName(CharSequence privateName) {
        int suffixIndex = Chars.indexOf(privateName, SYSTEM_TABLE_NAME_SUFFIX);
        if (suffixIndex == -1) {
            return privateName;
        }
        return Chars.toString(privateName).substring(0, suffixIndex);
    }

    public static int getTimestampIndex(MemoryMR metaMem, long offset, int columnCount) {
        final int timestampIndex = metaMem.getInt(offset);
        if (timestampIndex < -1 || timestampIndex >= columnCount) {
            throw validationException(metaMem).put("Timestamp index is outside of range, timestampIndex=").put(timestampIndex);
        }
        return timestampIndex;
    }

    public static int getTimestampType(TableStructure structure) {
        int timestampIndex = structure.getTimestampIndex();
        if (timestampIndex < 0) {
            return ColumnType.NULL;
        }
        return structure.getColumnType(timestampIndex);
    }

    public static void handleMetadataLoadException(
            TableToken tableToken,
            long deadline,
            CairoException ex,
            MillisecondClock millisecondClock,
            long spinLockTimeout
    ) {
        // This is temporary solution until we can get multiple version of metadata not overwriting each other
        if (ex.isFileCannotRead()) {
            if (millisecondClock.getTicks() < deadline) {
                LOG.info().$("error reloading metadata [table=").$(tableToken)
                        .$(", msg=").$safe(ex.getFlyweightMessage())
                        .$(", errno=").$(ex.getErrno())
                        .I$();
                Os.pause();
            } else {
                throw CairoException.critical(ex.getErrno()).put("Metadata read timeout [src=writer, timeout=").put(spinLockTimeout).put("ms, err=").put(ex.getFlyweightMessage()).put(']');
            }
        } else {
            throw ex;
        }
    }

    public static LPSZ iFile(Path path, CharSequence columnName, long columnTxn) {
        path.concat(columnName).put(FILE_SUFFIX_I);
        if (columnTxn > COLUMN_NAME_TXN_NONE) {
            path.put('.').put(columnTxn);
        }
        return path.$();
    }

    public static LPSZ iFile(Path path, CharSequence columnName) {
        return iFile(path, columnName, COLUMN_NAME_TXN_NONE);
    }

    /**
     * Check is table name does not start with temp table prefix. Usually in case
     * of table renames, table name can have temp prefix.
     *
     * @param tableName       name of the table
     * @param tempTablePrefix the temp prefix
     * @return true if table name is not pending table rename.
     */
    public static boolean isFinalTableName(String tableName, CharSequence tempTablePrefix) {
        return !Chars.startsWith(tableName, tempTablePrefix);
    }

    public static boolean isMatViewDefinitionFileExists(CairoConfiguration configuration, Path path, CharSequence dirName) {
        FilesFacade ff = configuration.getFilesFacade();
        path.of(configuration.getDbRoot()).concat(dirName).concat(MatViewDefinition.MAT_VIEW_DEFINITION_FILE_NAME);
        return ff.exists(path.$());
    }

    public static boolean isMatViewStateFileExists(CairoConfiguration configuration, Path path, CharSequence dirName) {
        FilesFacade ff = configuration.getFilesFacade();
        path.of(configuration.getDbRoot()).concat(dirName).concat(MatViewState.MAT_VIEW_STATE_FILE_NAME);
        return ff.exists(path.$());
    }

    /*
     * Checks that the minor version of the metadata format is up to date, i.e., at least the value
     * of the TableUtils.META_FORMAT_MINOR_VERSION_LATEST constant.
     *
     * Metadata Format Minor Version field encodes 2 shorts:
     * - Low short is a checksum that changes with every update to the metadata record
     * - High short is set to TableUtils.META_FORMAT_MINOR_VERSION_LATEST
     *
     * The Metadata Format Minor Version field was not present in the initial version of the metadata format. This is
     * why we need the checksum: when it doesn't match, we can't trust the version stored in it, and should assume
     * the QuestDB version that wrote the metadata predates its introduction.
     *
     * Table storage itself is forward- and backward-compatible, so it's safe to read regardless of this version.
     */
    public static boolean isMetaFormatUpToDate(MemoryR metaMem) {
        int metaFormatMinorVersionField = metaMem.getInt(META_OFFSET_META_FORMAT_MINOR_VERSION);
        short savedChecksum = Numbers.decodeLowShort(metaFormatMinorVersionField);
        short actualChecksum = checksumForMetaFormatMinorVersionField(
                metaMem.getLong(TableUtils.META_OFFSET_METADATA_VERSION),
                metaMem.getInt(TableUtils.META_OFFSET_COUNT)
        );
        short savedMetaFormatMinorVersion = Numbers.decodeHighShort(metaFormatMinorVersionField);
        return savedChecksum == actualChecksum && savedMetaFormatMinorVersion >= META_FORMAT_MINOR_VERSION_LATEST;
    }

    public static boolean isSymbolCached(MemoryR metaMem, int columnIndex) {
        return (getColumnFlags(metaMem, columnIndex) & META_FLAG_BIT_SYMBOL_CACHE) != 0;
    }

    public static boolean isValidColumnName(CharSequence columnName, int fsFileNameLimit) {
        final int length = columnName.length();
        if (length > fsFileNameLimit) {
            // Most file systems do not support file names longer than 255 bytes
            return false;
        }

        for (int i = 0; i < length; i++) {
            char c = columnName.charAt(i);
            switch (c) {
                case '?':
                case '.':
                case ',':
                case '\'':
                case '\"':
                case '\\':
                case '/':
                case ':':
                case ')':
                case '(':
                case '+':
                case '-':
                case '*':
                case '%':
                case '~':
                case '\u0000': // Control characters
                case '\u0001':
                case '\u0002':
                case '\u0003':
                case '\u0004':
                case '\u0005':
                case '\u0006':
                case '\u0007':
                case '\u0008':
                case '	':
                case '\u000B':
                case '\u000c':
                case '\n':
                case '\r':
                case '\u000e':
                case '\u000f':
                case '\u007f':
                case 0xfeff: // UTF-8 BOM (Byte Order Mark) can appear at the beginning of a character stream
                    return false;
                default:
                    break;
            }
        }
        return length > 0;
    }

    public static boolean isValidTableName(CharSequence tableName, int fsFileNameLimit) {
        final int length = tableName.length();
        if (length > fsFileNameLimit) {
            // Most file systems do not support file names longer than 255 bytes
            return false;
        }

        for (int i = 0; i < length; i++) {
            char c = tableName.charAt(i);
            switch (c) {
                case '.':
                    if (i == 0 || i == length - 1 || tableName.charAt(i - 1) == '.') {
                        // Single dot in the middle is allowed only
                        // Starting from . hides directory in Linux
                        // Ending . can be trimmed by some Windows versions / file systems
                        // Double, triple dot look suspicious
                        // Single dot allowed as compatibility,
                        // when someone uploads 'file_name.csv' the file name used as the table name
                        return false;
                    }
                    break;
                case '?':
                case ',':
                case '\'':
                case '\"':
                case '\\':
                case '/':
                case ':':
                case ')':
                case '(':
                case '+':
                case '*':
                case '%':
                case '~':
                case '\u0000':  // Control characters
                case '\u0001':
                case '\u0002':
                case '\u0003':
                case '\u0004':
                case '\u0005':
                case '\u0006':
                case '\u0007':
                case '\u0008':
                case '	':
                case '\u000B':
                case '\u000c':
                case '\r':
                case '\n':
                case '\u000e':
                case '\u000f':
                case '\u007f':
                case 0xfeff: // UTF-8 BOM (Byte Order Mark) can appear at the beginning of a character stream
                    return false;
            }
        }
        return length > 0 && tableName.charAt(0) != ' ' && tableName.charAt(length - 1) != ' ';
    }

    public static int lengthOf(@Nullable CharSequence columnValue) {
        return columnValue != null ? columnValue.length() : NULL_LEN;
    }

    public static long lock(FilesFacade ff, LPSZ path, boolean verbose) {
        // workaround for https://github.com/docker/for-mac/issues/7004
        if (Files.VIRTIO_FS_DETECTED) {
            if (!ff.touch(path)) {
                if (verbose) {
                    LOG.error().$("cannot touch '").$(path).$("' to lock [errno=").$(ff.errno()).I$();
                }
                return -1;
            }
        }

        long fd = ff.openRWNoCache(path, CairoConfiguration.O_NONE);
        if (fd == -1) {
            if (verbose) {
                LOG.error().$("cannot open '").$(path).$("' to lock [errno=").$(ff.errno()).I$();
            }
            return -1;
        }
        if (ff.lock(fd) != 0) {
            if (verbose) {
                LOG.error().$("cannot lock '").$(path).$("' [errno=").$(ff.errno()).$(", fd=").$(fd).I$();
            }
            ff.close(fd);
            return -1;
        }

        if (verbose) {
            LOG.debug().$("locked '").$(path).$("' [fd=").$(fd).I$();
        }
        return fd;
    }

    public static long lock(FilesFacade ff, LPSZ path) {
        return lock(ff, path, true);
    }

    public static LPSZ lockName(Path path) {
        return path.put(".lock").$();
    }

    public static long mapAppendColumnBuffer(FilesFacade ff, long fd, long offset, long size, boolean rw, int memoryTag) {
        assert !VM_PARANOIA_MODE || ff.length(fd) >= offset + size : "mmap ro buffer is beyond EOF";

        // Linux requires the mmap offset to be page aligned
        long alignedOffset = Files.floorPageSize(offset);
        long alignedExtraLen = offset - alignedOffset;
        long mapAddr = rw ?
                mapRWNoAlloc(ff, fd, size + alignedExtraLen, alignedOffset, memoryTag) :
                TableUtils.mapRO(ff, fd, size + alignedExtraLen, alignedOffset, memoryTag);
        ff.madvise(mapAddr, size + alignedExtraLen, rw ? Files.POSIX_MADV_RANDOM : Files.POSIX_MADV_SEQUENTIAL);
        return mapAddr + alignedExtraLen;
    }

    public static void mapAppendColumnBufferRelease(FilesFacade ff, long address, long offset, long size, int memoryTag) {
        long alignedOffset = Files.floorPageSize(offset);
        long alignedExtraLen = offset - alignedOffset;
        long adjustedAddress = address - alignedExtraLen;
        assert adjustedAddress > 0 : "address <= alignedExtraLen";
        ff.munmap(adjustedAddress, size + alignedExtraLen, memoryTag);
    }

    public static long mapRO(FilesFacade ff, long fd, long size, int memoryTag) {
        return TableUtils.mapRO(ff, fd, size, 0, memoryTag);
    }

    public static long mapRO(FilesFacade ff, LPSZ path, Log log, long size, int memoryTag) {
        final long fd = openRO(ff, path, log);
        try {
            return mapRO(ff, fd, size, memoryTag);
        } finally {
            ff.close(fd);
        }
    }

    /**
     * Maps a file in read-only mode.
     * <p>
     * Important note. Linux requires the offset to be page aligned.
     *
     * @param ff        files facade - intermediary to allow intercepting calls to the OS.
     * @param fd        file descriptor, previously provided by one of openFile() functions
     * @param size      size of the mapped file region
     * @param offset    offset in file to begin mapping
     * @param memoryTag bucket to trace memory allocation calls
     * @return read-only memory address
     */
    public static long mapRO(FilesFacade ff, long fd, long size, long offset, int memoryTag) {
        assert fd != -1;
        assert offset % Files.PAGE_SIZE == 0;
        final long address = ff.mmap(fd, size, offset, Files.MAP_RO, memoryTag);
        if (address == FilesFacade.MAP_FAILED) {
            throw CairoException.critical(ff.errno())
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
     * @param ff        files facade, - intermediary to allow intercepting calls to the OS.
     * @param fd        file descriptor, previously provided by one of openFile() functions. File has to be opened read-write
     * @param size      size of the mapped file region
     * @param offset    offset in file to begin mapping
     * @param memoryTag bucket to trace memory allocation calls
     * @return read-write memory address
     */
    public static long mapRW(FilesFacade ff, long fd, long size, long offset, int memoryTag) {
        assert fd != -1;
        assert offset % Files.PAGE_SIZE == 0;
        allocateDiskSpace(ff, fd, size + offset);
        return mapRWNoAlloc(ff, fd, size, offset, memoryTag);
    }

    /**
     * Maps a file in read-write mode without allocating the disk space.
     * <p>
     * Important note. Linux requires the offset to be page aligned.
     *
     * @param ff        files facade, - intermediary to allow intercepting calls to the OS.
     * @param fd        file descriptor, previously provided by one of openFile() functions. File has to be opened read-write
     * @param size      size of the mapped file region
     * @param offset    offset in file to begin mapping
     * @param memoryTag bucket to trace memory allocation calls
     * @return read-write memory address
     */
    public static long mapRWNoAlloc(FilesFacade ff, long fd, long size, long offset, int memoryTag) {
        long addr = ff.mmap(fd, size, offset, Files.MAP_RW, memoryTag);
        if (addr != FilesFacade.MAP_FAILED) {
            return addr;
        }
        int errno = ff.errno();
        if (Os.type != Os.WINDOWS || errno != 112) {
            throw CairoException.critical(ff.errno()).put("could not mmap column [fd=").put(fd).put(", size=").put(size).put(']');
        }
        throw CairoException.critical(ff.errno()).put("No space left [size=").put(size).put(", fd=").put(fd).put(']');
    }

    public static long mremap(
            FilesFacade ff,
            long fd,
            long prevAddress,
            long prevSize,
            long newSize,
            int mapMode,
            int memoryTag
    ) {
        return mremap(ff, fd, prevAddress, prevSize, newSize, 0L, mapMode, memoryTag);
    }

    public static long mremap(
            FilesFacade ff,
            long fd,
            long prevAddress,
            long prevSize,
            long newSize,
            long offset,
            int mapMode,
            int memoryTag
    ) {
        final long page = ff.mremap(fd, prevAddress, prevSize, newSize, offset, mapMode, memoryTag);
        if (page == FilesFacade.MAP_FAILED) {
            int errno = ff.errno();
            // Closing memory will truncate size to current append offset.
            // Since the failed resize can occur before append offset can be
            // explicitly set, we must assume that file size should be
            // equal to previous memory size
            throw CairoException.critical(errno).put("could not remap file [previousSize=").put(prevSize)
                    .put(", newSize=").put(newSize)
                    .put(", offset=").put(offset)
                    .put(", fd=").put(fd)
                    .put(']');
        }
        return page;
    }

    public static void msync(FilesFacade ff, long addr, long len, boolean async) {
        // Linux requires the msync address to be page aligned
        long alignedAddr = Files.floorPageSize(addr);
        long alignedExtraLen = addr - alignedAddr;
        ff.msync(alignedAddr, len + alignedExtraLen, async);
    }

    public static LPSZ offsetFileName(Path path, CharSequence columnName, long columnNameTxn) {
        path.concat(columnName).put(".o");
        if (columnNameTxn > COLUMN_NAME_TXN_NONE) {
            path.put('.').put(columnNameTxn);
        }
        return path.$();
    }

    public static void oldPartitionName(Path path, long txn) {
        path.put("-x-").put(txn);
    }

    public static long openAppend(FilesFacade ff, LPSZ path, Log log) {
        final long fd = ff.openAppend(path);
        if (fd > -1) {
            log.debug().$("open [file=").$(path).$(", fd=").$(fd).I$();
            return fd;
        }
        throw CairoException.critical(ff.errno()).put("could not open append [file=").put(path).put(']');
    }

    public static long openFileRWOrFail(FilesFacade ff, LPSZ path, int opts) {
        return openRW(ff, path, LOG, opts);
    }

    public static long openRO(FilesFacade ff, Path path, CharSequence fileName, Log log) {
        final int rootLen = path.size();
        path.concat(fileName);
        try {
            return TableUtils.openRO(ff, path.$(), log);
        } finally {
            path.trimTo(rootLen);
        }
    }

    public static long openRO(FilesFacade ff, LPSZ path, Log log) {
        final long fd = ff.openRO(path);
        if (fd > -1) {
            log.debug().$("open [file=").$(path).$(", fd=").$(fd).I$();
            return fd;
        }
        int errno = ff.errno();
        if (Files.isErrnoFileCannotRead(errno)) {
            throw CairoException.critical(errno).put("could not open, file does not exist: ").put(path).put(']');
        }
        throw CairoException.critical(errno).put("could not open read-only [file=").put(path).put(']');
    }

    public static long openRONoCache(FilesFacade ff, LPSZ path, Log log) {
        final long fd = ff.openRONoCache(path);
        if (fd > -1) {
            log.debug().$("open [file=").$(path).$(", fd=").$(fd).I$();
            return fd;
        }
        int errno = ff.errno();
        if (Files.isErrnoFileCannotRead(errno)) {
            throw CairoException.critical(errno).put("could not open, file does not exist: ").put(path).put(']');
        }
        throw CairoException.critical(errno).put("could not open read-only [file=").put(path).put(']');
    }

    public static long openRW(FilesFacade ff, LPSZ path, Log log, int opts) {
        final long fd = ff.openRW(path, opts);
        if (fd > -1) {
            log.debug().$("open [file=").$(path).$(", fd=").$(fd).I$();
            return fd;
        }
        throw CairoException.critical(ff.errno()).put("could not open read-write [file=").put(path).put(']');
    }

    public static void openSmallFile(FilesFacade ff, Path path, int rootLen, MemoryMR metaMem, CharSequence fileName, int memoryTag) {
        path.concat(fileName);
        try {
            metaMem.smallFile(ff, path.$(), memoryTag);
        } finally {
            path.trimTo(rootLen);
        }
    }

    public static void overwriteTableNameFile(Path tablePath, MemoryMAR memory, FilesFacade ff, @NotNull CharSequence tableName) {
        // Update name in _name file.
        // This is potentially racy but the file only read on startup when the tables.d file is missing
        // so very limited circumstances.
        Path nameFilePath = tablePath.concat(TABLE_NAME_FILE);
        memory.smallFile(ff, nameFilePath.$(), MemoryTag.MMAP_TABLE_WRITER);
        memory.jumpTo(0);
        createTableNameFile(memory, tableName);
        memory.close(true, Vm.TRUNCATE_TO_POINTER);
    }

    public static void populateRecordHashMap(
            SqlExecutionCircuitBreaker circuitBreaker,
            RecordCursor cursor,
            Map map,
            RecordSink recordSink,
            RecordChain chain
    ) {
        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            circuitBreaker.statefulThrowExceptionIfTripped();

            MapKey key = map.withKey();
            key.put(record, recordSink);
            MapValue value = key.createValue();
            if (value.isNew()) {
                long offset = chain.put(record, -1);
                value.putLong(0, offset);
                value.putLong(1, offset);
                value.putLong(2, 1);
            } else {
                value.putLong(1, chain.put(record, value.getLong(1)));
                value.addLong(2, 1);
            }
        }
    }

    public static int readIntOrFail(FilesFacade ff, long fd, long offset, long tempMem8b, Path path) {
        if (ff.read(fd, tempMem8b, Integer.BYTES, offset) != Integer.BYTES) {
            throw CairoException.critical(ff.errno()).put("Cannot read: ").put(path);
        }
        return Unsafe.getUnsafe().getInt(tempMem8b);
    }

    public static long readLongAtOffset(FilesFacade ff, LPSZ path, long tempMem8b, long offset) {
        final long fd = TableUtils.openRO(ff, path, LOG);
        try {
            return readLongOrFail(ff, fd, offset, tempMem8b, path);
        } finally {
            ff.close(fd);
        }
    }

    public static long readLongOrFail(FilesFacade ff, long fd, long offset, long tempMem8b, @Nullable LPSZ path) {
        if (ff.read(fd, tempMem8b, Long.BYTES, offset) != Long.BYTES) {
            if (path != null) {
                throw CairoException.critical(ff.errno()).put("could not read long [path=").put(path).put(", fd=").put(fd).put(", offset=").put(offset);
            }
            throw CairoException.critical(ff.errno()).put("could not read long [fd=").put(fd).put(", offset=").put(offset);
        }
        return Unsafe.getUnsafe().getLong(tempMem8b);
    }

    public static String readTableName(Path path, int rootLen, MemoryCMR mem, FilesFacade ff) {
        long fd = -1;
        try {
            path.concat(TableUtils.TABLE_NAME_FILE);
            LPSZ $path = path.$();
            fd = ff.openRO($path);
            if (fd < 1) {
                return null;
            }

            long fileLen = ff.length(fd);
            if (fileLen > Integer.BYTES) {
                int charLen = ff.readNonNegativeInt(fd, 0);
                if (charLen * 2L + Integer.BYTES != fileLen - 1) {
                    LOG.error().$("invalid table name file [path=").$(path).$(", headerLen=").$(charLen).$(", fileLen=").$(fileLen).I$();
                    return null;
                }

                mem.of(ff, $path, fileLen, fileLen, MemoryTag.MMAP_DEFAULT);
                return Chars.toString(mem.getStrA(0));
            } else {
                LOG.error().$("invalid table name file [path=").$(path).$(", fileLen=").$(fileLen).I$();
                return null;
            }
        } finally {
            path.trimTo(rootLen);
            ff.close(fd);
        }
    }

    public static String readText(FilesFacade ff, LPSZ path1) {
        long fd = ff.openRO(path1);
        long bytes = 0;
        long length = 0;
        if (fd > -1) {
            try {
                length = ff.length(fd);
                if (length > 0) {
                    bytes = Unsafe.malloc(length, MemoryTag.NATIVE_DEFAULT);
                    if (ff.read(fd, bytes, length, 0) == length) {
                        return Utf8s.stringFromUtf8Bytes(bytes, bytes + length);
                    }

                }
            } finally {
                if (bytes != 0) {
                    Unsafe.free(bytes, length, MemoryTag.NATIVE_DEFAULT);
                }
                ff.close(fd);
            }
        }
        return null;
    }

    public static void removeColumnFromMetadata(
            CharSequence columnName,
            LowerCaseCharSequenceIntHashMap columnNameIndexMap,
            ObjList<TableColumnMetadata> columnMetadata
    ) {
        final int columnIndex = columnNameIndexMap.get(columnName);
        if (columnIndex < 0) {
            throw CairoException.critical(0).put("Column not found: ").put(columnName);
        }

        columnNameIndexMap.remove(columnName);
        final TableColumnMetadata deletedMeta = columnMetadata.getQuick(columnIndex);
        deletedMeta.markDeleted();
    }

    public static void renameColumnInMetadata(
            CharSequence columnName,
            CharSequence newName,
            LowerCaseCharSequenceIntHashMap columnNameIndexMap,
            ObjList<TableColumnMetadata> columnMetadata
    ) {
        final int columnIndex = columnNameIndexMap.get(columnName);
        if (columnIndex < 0) {
            throw CairoException.critical(0).put("Column not found: ").put(columnName);
        }
        final String newNameStr = newName.toString();
        columnMetadata.getQuick(columnIndex).rename(newNameStr);

        columnNameIndexMap.removeEntry(columnName);
        columnNameIndexMap.put(newNameStr, columnIndex);
    }

    public static void renameOrFail(FilesFacade ff, LPSZ src, LPSZ dst) {
        if (ff.rename(src, dst) != Files.FILES_RENAME_OK) {
            throw CairoException.critical(ff.errno()).put("could not rename ").put(src).put(" -> ").put(dst);
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
        mem.sync(false);
    }

    public static void resetTxn(
            MemoryMW txMem,
            long baseOffset,
            int symbolMapCount,
            long txn,
            long seqTxn,
            long dataVersion,
            long partitionTableVersion,
            long structureVersion,
            long columnVersion,
            long truncateVersion
    ) {
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
        // column version
        txMem.putLong(baseOffset + TX_OFFSET_COLUMN_VERSION_64, columnVersion);
        // truncate version
        txMem.putLong(baseOffset + TX_OFFSET_TRUNCATE_VERSION_64, truncateVersion);
        // sequencer txn
        txMem.putLong(baseOffset + TX_OFFSET_SEQ_TXN_64, seqTxn);

        txMem.putInt(baseOffset + TX_OFFSET_MAP_WRITER_COUNT_32, symbolMapCount);

        txMem.putLong(baseOffset + TX_OFFSET_LAG_MIN_TIMESTAMP_64, Long.MAX_VALUE);
        txMem.putLong(baseOffset + TX_OFFSET_LAG_MAX_TIMESTAMP_64, Long.MIN_VALUE);
        txMem.putInt(baseOffset + TX_OFFSET_LAG_ROW_COUNT_32, 0);
        txMem.putInt(baseOffset + TX_OFFSET_LAG_TXN_COUNT_32, 0);
        txMem.putInt(baseOffset + TX_OFFSET_CHECKSUM_32, EMPTY_TABLE_LAG_CHECKSUM);

        for (int i = 0; i < symbolMapCount; i++) {
            long offset = getSymbolWriterIndexOffset(i);
            txMem.putInt(baseOffset + offset, 0);
            offset += Integer.BYTES;
            txMem.putInt(baseOffset + offset, 0);
        }

        // partition update count
        txMem.putInt(baseOffset + getPartitionTableSizeOffset(symbolMapCount), 0);
    }

    public static void safeReadTxn(TxReader txReader, MillisecondClock clock, long spinLockTimeout) {
        long deadline = clock.getTicks() + spinLockTimeout;
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
            if (clock.getTicks() > deadline) {
                throw CairoException.critical(0).put("Transaction read timeout [src=writer, timeout=").put(spinLockTimeout).put("ms]");
            }

            LOG.debug().$("loaded __dirty__ txn, version ").$(txReader.getVersion()).$();
            Os.pause();
        }
    }

    public static boolean schedulePurgeO3Partitions(MessageBus messageBus, TableToken tableName, int timestampType, int partitionBy) {
        final MPSequence seq = messageBus.getO3PurgeDiscoveryPubSeq();
        while (true) {
            long cursor = seq.next();
            if (cursor > -1) {
                O3PartitionPurgeTask task = messageBus.getO3PurgeDiscoveryQueue().get(cursor);
                task.of(tableName, timestampType, partitionBy);
                seq.done(cursor);
                return true;
            } else if (cursor == -1) {
                return false;
            }
            Os.pause();
        }
    }

    public static void setNull(int columnType, long addr, long count) {
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.BOOLEAN:
            case ColumnType.BYTE:
                Vect.memset(addr, count, 0);
                break;
            case ColumnType.GEOBYTE:
                Vect.memset(addr, count, GeoHashes.BYTE_NULL);
                break;
            case ColumnType.CHAR:
            case ColumnType.SHORT:
                Vect.setMemoryShort(addr, (short) 0, count);
                break;
            case ColumnType.GEOSHORT:
                Vect.setMemoryShort(addr, GeoHashes.SHORT_NULL, count);
                break;
            case ColumnType.INT:
                Vect.setMemoryInt(addr, Numbers.INT_NULL, count);
                break;
            case ColumnType.IPv4:
                Vect.setMemoryInt(addr, Numbers.IPv4_NULL, count);
                break;
            case ColumnType.GEOINT:
                Vect.setMemoryInt(addr, GeoHashes.INT_NULL, count);
                break;
            case ColumnType.FLOAT:
                Vect.setMemoryFloat(addr, Float.NaN, count);
                break;
            case ColumnType.SYMBOL:
                Vect.setMemoryInt(addr, SymbolTable.VALUE_IS_NULL, count);
                break;
            case ColumnType.LONG:
            case ColumnType.DATE:
            case ColumnType.TIMESTAMP:
                Vect.setMemoryLong(addr, Numbers.LONG_NULL, count);
                break;
            case ColumnType.GEOLONG:
                Vect.setMemoryLong(addr, GeoHashes.NULL, count);
                break;
            case ColumnType.DOUBLE:
                Vect.setMemoryDouble(addr, Double.NaN, count);
                break;
            case ColumnType.LONG256:
                // Long256 is null when all 4 longs are NaNs
                Vect.setMemoryLong(addr, Numbers.LONG_NULL, count * 4);
                break;
            case ColumnType.LONG128:
                // fall through
            case ColumnType.UUID:
                // Long128 and UUID are null when all 2 longs are NaNs
                Vect.setMemoryLong(addr, Numbers.LONG_NULL, count * 2);
                break;
            default:
                break;
        }
    }

    /**
     * Sets the path to the directory of a native partition taking into account the timestamp, the partitioning scheme
     * and the partition version.
     *
     * @param path          Set to the root directory for a table, this will be updated to the root directory of the partition
     * @param timestampType type (resolution) of the timestamp column
     * @param partitionBy   Partitioning scheme
     * @param timestamp     A timestamp in the partition
     * @param nameTxn       Partition txn suffix
     */
    public static void setPathForNativePartition(Path path, int timestampType, int partitionBy, long timestamp, long nameTxn) {
        setSinkForNativePartition(path.slash(), timestampType, partitionBy, timestamp, nameTxn);
    }

    /**
     * Sets the path to the file of a Parquet partition taking into account the timestamp, the partitioning scheme
     * and the partition version.
     *
     * @param path          Set to the root directory for a table, this will be updated to the file of the partition
     * @param timestampType type (resolution) of the timestamp column
     * @param partitionBy   Partitioning scheme
     * @param timestamp     A timestamp in the partition
     * @param nameTxn       Partition txn suffix
     */
    public static void setPathForParquetPartition(Path path, int timestampType, int partitionBy, long timestamp, long nameTxn) {
        setSinkForNativePartition(path.slash(), timestampType, partitionBy, timestamp, nameTxn);
        path.concat(PARQUET_PARTITION_NAME);
    }

    /**
     * Sets the sink to the directory of a native partition taking into account the timestamp, the partitioning scheme
     * and the partition version.
     *
     * @param sink          Set to the root directory for a table, this will be updated to the root directory of the partition
     * @param timestampType type (resolution) of the timestamp column
     * @param partitionBy   Partitioning scheme
     * @param timestamp     A timestamp in the partition
     * @param nameTxn       Partition txn suffix
     */
    public static void setSinkForNativePartition(CharSink<?> sink, int timestampType, int partitionBy, long timestamp, long nameTxn) {
        PartitionBy.setSinkForPartition(sink, timestampType, partitionBy, timestamp);
        if (nameTxn > -1L) {
            sink.put('.').put(nameTxn);
        }
    }

    public static void setTxReaderPath(@NotNull TxReader reader, @NotNull Path path, int timestampType, int partitionBy) {
        reader.ofRO(path.concat(TXN_FILE_NAME).$(), timestampType, partitionBy);
    }

    public static int toIndexKey(int symbolKey) {
        assert symbolKey != Integer.MAX_VALUE;
        return symbolKey == SymbolTable.VALUE_IS_NULL ? 0 : symbolKey + 1;
    }

    public static void validateIndexValueBlockSize(int position, int indexValueBlockSize) throws SqlException {
        if (indexValueBlockSize < MIN_INDEX_VALUE_BLOCK_SIZE) {
            throw SqlException.$(position, "min index block capacity is ").put(MIN_INDEX_VALUE_BLOCK_SIZE);
        }
        if (indexValueBlockSize > MAX_INDEX_VALUE_BLOCK_SIZE) {
            throw SqlException.$(position, "max index block capacity is ").put(MAX_INDEX_VALUE_BLOCK_SIZE);
        }
    }

    public static void validateMeta(
            MemoryMR metaMem,
            LowerCaseCharSequenceIntHashMap nameIndex,
            int expectedVersion
    ) {
        try {
            final long memSize = checkMemSize(metaMem, META_OFFSET_COLUMN_TYPES);
            validateMetaVersion(metaMem, META_OFFSET_VERSION, expectedVersion);
            final int columnCount = getColumnCount(metaMem, META_OFFSET_COUNT);

            long offset = getColumnNameOffset(columnCount);
            if (memSize < offset) {
                throw validationException(metaMem).put("File is too small, column types are missing ").put(memSize);
            }

            // validate designated timestamp column
            final int timestampIndex = getTimestampIndex(metaMem, META_OFFSET_TIMESTAMP_INDEX, columnCount);
            if (timestampIndex != -1) {
                final int timestampType = getColumnType(metaMem, timestampIndex);
                if (!ColumnType.isTimestamp(timestampType)) {
                    throw validationException(metaMem).put("Timestamp column must be TIMESTAMP, but found ").put(ColumnType.nameOf(timestampType));
                }
            }

            // validate column types and index attributes
            for (int i = 0; i < columnCount; i++) {
                final int type = Math.abs(getColumnType(metaMem, i));
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
            int denseCount = 0;
            if (nameIndex != null) {
                for (int i = 0; i < columnCount; i++) {
                    final CharSequence name = getColumnName(metaMem, memSize, offset, i);
                    if (getColumnType(metaMem, i) < 0 || nameIndex.put(name, denseCount++)) {
                        offset += Vm.getStorageLength(name);
                    } else {
                        throw validationException(metaMem).put("Duplicate column [name=").put(name).put("] at ").put(i);
                    }
                }
            }
        } catch (Throwable e) {
            if (nameIndex != null) {
                nameIndex.clear();
            }
            throw e;
        }
    }

    public static void validateMetaVersion(MemoryMR metaMem, long metaVersionOffset, int expectedVersion) {
        final int metaVersion = metaMem.getInt(metaVersionOffset);
        if (expectedVersion != metaVersion) {
            throw validationException(metaMem)
                    .put("Metadata version does not match runtime version [expected=").put(expectedVersion)
                    .put(", actual=").put(metaVersion)
                    .put(']');
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

    public static void validateSymbolCapacityCached(boolean isCached, int symbolCapacity, int cacheKeywordPosition) throws SqlException {
        if (isCached && symbolCapacity > MAX_SYMBOL_CAPACITY_CACHED) {
            throw SqlException.$(cacheKeywordPosition, "max cached symbol capacity is ").put(MAX_SYMBOL_CAPACITY_CACHED);
        }
    }

    public static CairoException validationException(MemoryMR mem) {
        return CairoException.critical(CairoException.METADATA_VALIDATION).put("Invalid metadata at fd=").put(mem.getFd()).put(". ");
    }

    public static CairoException validationException(MemoryR mem) {
        if (mem instanceof MemoryMR) {
            return CairoException.critical(CairoException.METADATA_VALIDATION).put("Invalid metadata at fd=").put(((MemoryMR) mem).getFd()).put(". ");
        }
        return CairoException.critical(CairoException.METADATA_VALIDATION).put("Invalid metadata. ");
    }

    public static void writeIntOrFail(FilesFacade ff, long fd, long offset, int value, long tempMem8b, Path path) {
        Unsafe.getUnsafe().putInt(tempMem8b, value);
        if (ff.write(fd, tempMem8b, Integer.BYTES, offset) != Integer.BYTES) {
            throw CairoException.critical(ff.errno())
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
            throw CairoException.critical(ff.errno())
                    .put("could not write 8 bytes [path=").put(path)
                    .put(", fd=").put(fd)
                    .put(", offset=").put(offset)
                    .put(", value=").put(value)
                    .put(']');
        }
    }

    public static void writeMetadata(TableStructure tableStruct, int tableVersion, int tableId, MemoryA mem) {
        int count = tableStruct.getColumnCount();
        mem.putInt(count);
        mem.putInt(tableStruct.getPartitionBy());
        int timestampIndex = tableStruct.getTimestampIndex();
        assert timestampIndex == -1 ||
                (timestampIndex >= 0 && timestampIndex < count && ColumnType.isTimestamp(tableStruct.getColumnType(timestampIndex)))
                : String.format("timestampIndex %d count %d columnType %d", timestampIndex, count, tableStruct.getColumnType(timestampIndex));
        mem.putInt(timestampIndex);
        mem.putInt(tableVersion);
        mem.putInt(tableId);
        mem.putInt(tableStruct.getMaxUncommittedRows());
        mem.putLong(tableStruct.getO3MaxLag());
        mem.putLong(0); // Metadata version.
        mem.putBool(tableStruct.isWalEnabled());
        mem.putInt(TableUtils.calculateMetaFormatMinorVersionField(0, count));
        mem.putInt(tableStruct.getTtlHoursOrMonths());

        mem.jumpTo(TableUtils.META_OFFSET_COLUMN_TYPES);
        assert count > 0;

        for (int i = 0; i < count; i++) {
            mem.putInt(tableStruct.getColumnType(i));
            long flags = 0;
            if (tableStruct.isIndexed(i)) {
                flags |= META_FLAG_BIT_INDEXED;
            }

            if (tableStruct.getSymbolCacheFlag(i)) {
                flags |= META_FLAG_BIT_SYMBOL_CACHE;
            }

            if (tableStruct.isDedupKey(i)) {
                flags |= META_FLAG_BIT_DEDUP_KEY;
            }

            mem.putLong(flags);
            mem.putInt(tableStruct.getIndexBlockCapacity(i));
            mem.putInt(tableStruct.getSymbolCapacity(i));
            // reserved
            mem.skip(12);
        }

        for (int i = 0; i < count; i++) {
            mem.putStr(tableStruct.getColumnName(i));
        }
    }

    private static int exists(FilesFacade ff, Path path) {
        if (ff.exists(path.$())) { // it can also be a file, for example created with touch
            if (ff.exists(path.concat(TXN_FILE_NAME).$())) {
                return TABLE_EXISTS;
            } else {
                return TABLE_RESERVED;
            }
        } else {
            return TABLE_DOES_NOT_EXIST;
        }
    }

    private static CharSequence getCharSequence(MemoryMR metaMem, long memSize, long offset, int strLength) {
        if (strLength < 1 || strLength > 255) {
            // EXT4 and many others do not allow file name length > 255 bytes
            throw validationException(metaMem).put("String length of ").put(strLength).put(" is invalid at offset ").put(offset);
        }
        final long storageLength = Vm.getStorageLength(strLength);
        if (offset + storageLength > memSize) {
            throw CairoException.critical(0).put("File is too small, size=").put(memSize).put(", required=").put(offset + storageLength);
        }
        return metaMem.getStrA(offset);
    }

    // Utility method for debugging. This method is not used in production.
    @SuppressWarnings("unused")
    static boolean assertTimestampInOrder(long srcTimestampAddr, long srcDataMax) {
        long prev = Long.MIN_VALUE;
        for (long i = 0; i < srcDataMax; i++) {
            long newTs = Unsafe.getUnsafe().getLong(srcTimestampAddr + i * Long.BYTES);
            if (newTs < prev) {
                return false;
            }
            prev = newTs;
        }
        return true;
    }

    /**
     * Creates a column list from the metadata file. Each entry of the list is a struct, but
     * Java doesn't support structs you may say! Yes, this is open-array struct, 3 elements per entry.
     * - writer index (will explain what this is later)
     * - column name offset - pointer at the beginning of the string list
     * - symbol map index
     * <p>
     * This list will be dense, e.g., it will not have deleted columns, not will it have extra columns that
     * have changed types. The type change is what makes this loading tricky. When a column type is changed, a new
     * entry is added to the metadata file on the disk. This new entry will reference the old column (type change) via
     * replace index, which is also written to the file.
     * <p>
     * When we read the file from disk, we don't need the "old" columns in the output. For this reason, as we read
     * new columns from the file, we might go back to the columns we already read, and replace them.
     * <p>
     * Writer index is the index of the column in the metadata file. The metadata file has "sparse" column list,
     * writer index refers to this sparse list.
     *
     * @param metaMem     the memory mapped metadata file
     * @param columnCount the column count in the file
     * @param targetList  the destination list (out parameter)
     */
    static void buildColumnListFromMetadataFile(MemoryR metaMem, int columnCount, IntList targetList) {
        int nameOffset = (int) TableUtils.getColumnNameOffset(columnCount);
        targetList.clear();

        int denseSymbolIndex = 0;
        for (int i = 0; i < columnCount; i++) {
            int strLen = TableUtils.getInt(metaMem, metaMem.size(), nameOffset);
            if (strLen == TableUtils.NULL_LEN) {
                throw validationException(metaMem).put("NULL column name at [").put(i).put(']');
            }
            if (strLen < 1 || strLen > 255) {
                // EXT4 and many others do not allow file name length > 255 bytes
                throw validationException(metaMem).put("String length of ").put(strLen).put(" is invalid at offset ").put(nameOffset);
            }
            int nameLen = (int) Vm.getStorageLength(strLen);
            int replacingColumnIndex = TableUtils.getReplacingColumnIndex(metaMem, i);
            boolean isSymbol = ColumnType.isSymbol(TableUtils.getColumnType(metaMem, i));

            if (replacingColumnIndex > -1 && replacingColumnIndex < columnCount - 1) {
                // Replace the column index
                targetList.set(3 * replacingColumnIndex, i);
                targetList.set(3 * replacingColumnIndex + 1, nameOffset);
                targetList.set(3 * replacingColumnIndex + 2, isSymbol ? denseSymbolIndex : -1);

                targetList.add(-replacingColumnIndex - 1);
                targetList.add(0);
                targetList.add(0);

            } else {
                targetList.add(i);
                targetList.add(nameOffset);
                targetList.add(isSymbol ? denseSymbolIndex : -1);
            }
            nameOffset += nameLen;
            if (isSymbol) {
                denseSymbolIndex++;
            }
        }
    }

    static void createDirsOrFail(FilesFacade ff, Path path, int mkDirMode) {
        if (ff.mkdirs(path, mkDirMode) != 0) {
            throw CairoException.critical(ff.errno()).put("could not create directories [file=").put(path).put(']');
        }
    }

    static long getColumnFlags(MemoryR metaMem, int columnIndex) {
        return metaMem.getLong(META_OFFSET_COLUMN_TYPES + columnIndex * META_COLUMN_DATA_SIZE + 4);
    }

    static int getIndexBlockCapacity(MemoryR metaMem, int columnIndex) {
        return metaMem.getInt(META_OFFSET_COLUMN_TYPES + columnIndex * META_COLUMN_DATA_SIZE + 4 + 8);
    }

    static int getTtlHoursOrMonths(MemoryR metaMem) {
        return isMetaFormatUpToDate(metaMem) ? metaMem.getInt(TableUtils.META_OFFSET_TTL_HOURS_OR_MONTHS) : 0;
    }

    static boolean isColumnDedupKey(MemoryR metaMem, int columnIndex) {
        return (getColumnFlags(metaMem, columnIndex) & META_FLAG_BIT_DEDUP_KEY) != 0;
    }

    static boolean isColumnIndexed(MemoryR metaMem, int columnIndex) {
        return (getColumnFlags(metaMem, columnIndex) & META_FLAG_BIT_INDEXED) != 0;
    }

    static int openMetaSwapFile(FilesFacade ff, MemoryMA mem, Path path, int rootLen, int retryCount) {
        try {
            path.concat(META_SWAP_FILE_NAME).$();
            int l = path.size();
            int index = 0;
            do {
                if (index > 0) {
                    path.trimTo(l).put('.').put(index);
                }

                LPSZ lpsz = path.$();
                if (ff.removeQuiet(lpsz)) {
                    try {
                        mem.smallFile(ff, lpsz, MemoryTag.MMAP_DEFAULT);
                        mem.jumpTo(0);
                        return index;
                    } catch (CairoException e) {
                        // right, cannot open file for some reason?
                        LOG.error()
                                .$("could not open swap [file=").$(path)
                                .$(", msg=").$safe(e.getFlyweightMessage())
                                .$(", errno=").$(e.getErrno())
                                .I$();
                    }
                } else {
                    LOG.error()
                            .$("could not remove swap [file=").$(path)
                            .$(", errno=").$(ff.errno())
                            .I$();
                }
            } while (++index < retryCount);
            throw CairoException.critical(0).put("Cannot open indexed file. Max number of attempts reached [").put(index).put("]. Last file tried: ").put(path);
        } finally {
            path.trimTo(rootLen);
        }
    }

    public interface FailureCloseable {
        void close(long prevSize);
    }

    static {
        //noinspection ConstantValue
        assert TX_OFFSET_LAG_MAX_TIMESTAMP_64 + 8 <= TX_OFFSET_MAP_WRITER_COUNT_32;
    }
}
