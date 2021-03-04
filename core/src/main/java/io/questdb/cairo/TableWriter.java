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

import io.questdb.MessageBus;
import io.questdb.MessageBusImpl;
import io.questdb.cairo.SymbolMapWriter.TransientSymbolCountChangeHandler;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.*;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.*;
import io.questdb.std.*;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.NativeLPSZ;
import io.questdb.std.str.Path;
import io.questdb.tasks.*;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;

import static io.questdb.cairo.StatusCode.*;
import static io.questdb.cairo.TableUtils.*;
import static io.questdb.std.Files.isDots;

public class TableWriter implements Closeable {
    public static final int TIMESTAMP_MERGE_ENTRY_BYTES = Long.BYTES * 2;
    public static final int OO_BLOCK_NONE = -1;
    public static final int OO_BLOCK_OO = 1;
    public static final int OO_BLOCK_DATA = 2;
    public static final int OO_BLOCK_MERGE = 3;
    private static final Log LOG = LogFactory.getLog(TableWriter.class);
    private static final CharSequenceHashSet IGNORED_FILES = new CharSequenceHashSet();
    private static final Runnable NOOP = () -> {
    };
    private final static RemoveFileLambda REMOVE_OR_LOG = TableWriter::removeFileAndOrLog;
    private final static RemoveFileLambda REMOVE_OR_EXCEPTION = TableWriter::removeOrException;
    private final static OutOfOrderNativeSortMethod SHUFFLE_8 = Vect::indexReshuffle8Bit;
    private final static OutOfOrderNativeSortMethod SHUFFLE_16 = Vect::indexReshuffle16Bit;
    private final static OutOfOrderNativeSortMethod SHUFFLE_32 = Vect::indexReshuffle32Bit;
    private final static OutOfOrderNativeSortMethod SHUFFLE_64 = Vect::indexReshuffle64Bit;
    final ObjList<AppendOnlyVirtualMemory> columns;
    private final ObjList<SymbolMapWriter> symbolMapWriters;
    private final ObjList<SymbolMapWriter> denseSymbolMapWriters;
    private final ObjList<WriterTransientSymbolCountChangeHandler> denseSymbolTransientCountHandlers;
    private final ObjList<ColumnIndexer> indexers;
    private final ObjList<ColumnIndexer> denseIndexers = new ObjList<>();
    private final Path path;
    private final Path other;
    private final LongList refs = new LongList();
    private final Row row = new Row();
    private final int rootLen;
    private final MappedReadOnlyMemory metaMem;
    private final int partitionBy;
    private final RowFunction switchPartitionFunction = new SwitchPartitionRowFunction();
    private final RowFunction openPartitionFunction = new OpenPartitionRowFunction();
    private final RowFunction noPartitionFunction = new NoPartitionFunction();
    private final RowFunction noTimestampFunction = new NoTimestampFunction();
    private final RowFunction oooRowFunction = new OutOfOrderPartitionFunction();
    private final NativeLPSZ nativeLPSZ = new NativeLPSZ();
    private final LongList columnTops;
    private final FilesFacade ff;
    private final DateFormat partitionDirFmt;
    private final AppendOnlyVirtualMemory ddlMem;
    private final int mkDirMode;
    private final int fileOperationRetryCount;
    private final CharSequence name;
    private final TableWriterMetadata metadata;
    private final CairoConfiguration configuration;
    private final CharSequenceIntHashMap validationMap = new CharSequenceIntHashMap();
    private final FragileCode RECOVER_FROM_META_RENAME_FAILURE = this::recoverFromMetaRenameFailure;
    private final SOCountDownLatch indexLatch = new SOCountDownLatch();
    private final LongList indexSequences = new LongList();
    private final MessageBus messageBus;
    private final boolean parallelIndexerEnabled;
    private final Timestamps.TimestampFloorMethod timestampFloorMethod;
    private final Timestamps.TimestampCeilMethod timestampCeilMethod;
    private final Timestamps.TimestampAddMethod timestampAddMethod;
    private final int defaultCommitMode;
    private final FindVisitor removePartitionDirectories = this::removePartitionDirectories0;
    private final ObjList<Runnable> nullSetters;
    private final ObjList<Runnable> oooNullSetters;
    private final ObjList<ContiguousVirtualMemory> oooColumns;
    private final ObjList<ContiguousVirtualMemory> oooColumns2;
    private final TableBlockWriter blockWriter;
    private final TimestampValueRecord dropPartitionFunctionRec = new TimestampValueRecord();
    private final boolean outOfOrderEnabled;
    private final ObjList<OutOfOrderSortTask> oooPendingSortTasks = new ObjList<>();
    private final OutOfOrderSortMethod oooSortVarColumnRef = this::oooSortVarColumn;
    private final OutOfOrderSortMethod oooSortFixColumnRef = this::oooSortFixColumn;
    private final SOUnboundedCountDownLatch oooLatch = new SOUnboundedCountDownLatch();
    private final LongList oooPartitions = new LongList();
    private final AtomicLong oooUpdRemaining = new AtomicLong();
    private final FindVisitor removePartitionDirsCreatedByOOOVisitor = this::removePartitionDirsCreatedByOOO0;
    private final AtomicInteger oooErrorCount = new AtomicInteger();
    private final MappedReadWriteMemory todoMem = new PagedMappedReadWriteMemory();
    private final TxWriter txFile;
    private long todoTxn;
    private ContiguousVirtualMemory timestampMergeMem;
    private long lockFd;
    private LongConsumer timestampSetter;
    private LongConsumer prevTimestampSetter;
    private int columnCount;
    private RowFunction rowFunction = openPartitionFunction;
    private boolean avoidIndexOnCommit = false;
    private long partitionHi;
    private long masterRef = 0;
    private boolean removeDirOnCancelRow = true;
    private long tempMem16b = Unsafe.malloc(16);
    private int metaSwapIndex;
    private int metaPrevIndex;
    private final FragileCode RECOVER_FROM_TODO_WRITE_FAILURE = this::recoverFrommTodoWriteFailure;
    private final FragileCode RECOVER_FROM_SYMBOL_MAP_WRITER_FAILURE = this::recoverFromSymbolMapWriterFailure;
    private final FragileCode RECOVER_FROM_SWAP_RENAME_FAILURE = this::recoverFromSwapRenameFailure;
    private final FragileCode RECOVER_FROM_COLUMN_OPEN_FAILURE = this::recoverOpenColumnFailure;
    private int indexCount;
    private boolean performRecovery;
    private boolean distressed = false;
    private LifecycleManager lifecycleManager;
    private String designatedTimestampColumnName;
    private long oooRowCount;
    private final LongConsumer mergeTimestampMethodRef = this::mergeTimestampSetter;
    private long transientRowCountBeforeOutOfOrder;
    private long removePartitionDirsNewerThanTimestamp;
    private final FindVisitor removePartitionDirsNewerThanVisitor = this::removePartitionDirsNewerThanTimestamp;

    public TableWriter(CairoConfiguration configuration, CharSequence name) {
        this(configuration, name, new MessageBusImpl(configuration));
    }

    public TableWriter(CairoConfiguration configuration, CharSequence name, @NotNull MessageBus messageBus) {
        this(configuration, name, messageBus, true, DefaultLifecycleManager.INSTANCE);
    }

    public TableWriter(
            CairoConfiguration configuration,
            CharSequence name,
            @NotNull MessageBus messageBus,
            boolean lock,
            LifecycleManager lifecycleManager
    ) {
        this(configuration, name, messageBus, lock, lifecycleManager, configuration.getRoot());
    }

    public TableWriter(
            CairoConfiguration configuration,
            CharSequence name,
            @NotNull MessageBus messageBus,
            boolean lock,
            LifecycleManager lifecycleManager,
            CharSequence root
    ) {
        LOG.info().$("open '").utf8(name).$('\'').$();
        this.configuration = configuration;
        this.outOfOrderEnabled = configuration.isOutOfOrderEnabled();
        this.messageBus = messageBus;
        this.defaultCommitMode = configuration.getCommitMode();
        this.lifecycleManager = lifecycleManager;
        this.parallelIndexerEnabled = configuration.isParallelIndexingEnabled();
        this.ff = configuration.getFilesFacade();
        this.mkDirMode = configuration.getMkDirMode();
        this.fileOperationRetryCount = configuration.getFileOperationRetryCount();
        this.path = new Path().of(root).concat(name);
        this.other = new Path().of(root).concat(name);
        this.name = Chars.toString(name);
        this.rootLen = path.length();
        this.blockWriter = new TableBlockWriter(configuration, messageBus);
        try {
            if (lock) {
                lock();
            } else {
                this.lockFd = -1L;
            }
            long todoCount = openTodoMem();
            int todo;
            if (todoCount > 0) {
                todo = (int) todoMem.getLong(40);
            } else {
                todo = -1;
            }
            if (todo == TODO_RESTORE_META) {
                repairMetaRename((int) todoMem.getLong(48));
            }
            this.ddlMem = new AppendOnlyVirtualMemory();
            this.metaMem = new SinglePageMappedReadOnlyPageMemory();
            openMetaFile(ff, path, rootLen, metaMem);
            this.metadata = new TableWriterMetadata(ff, metaMem);
            this.partitionBy = metaMem.getInt(META_OFFSET_PARTITION_BY);

            this.txFile = new TxWriter(this.ff, this.path);
            this.txFile.open();
            this.txFile.initPartitionBy(partitionBy);
            this.txFile.readUnchecked();

            // we have to do truncate repair at this stage of constructor
            // because this operation requires metadata
            switch (todo) {
                case TODO_TRUNCATE:
                    repairTruncate();
                    break;
                case TODO_RESTORE_META:
                case -1:
                    break;
                default:
                    LOG.error().$("ignoring unknown *todo* [code=").$(todo).$(']').$();
                    break;
            }
            this.columnCount = metadata.getColumnCount();
            if (metadata.getTimestampIndex() > -1) {
                this.designatedTimestampColumnName = metadata.getColumnName(metadata.getTimestampIndex());
            }
            this.refs.extendAndSet(columnCount, 0);
            this.columns = new ObjList<>(columnCount * 2);
            this.oooColumns = new ObjList<>(columnCount * 2);
            this.oooColumns2 = new ObjList<>(columnCount * 2);
            this.row.activeColumns = columns;
            this.symbolMapWriters = new ObjList<>(columnCount);
            this.indexers = new ObjList<>(columnCount);
            this.denseSymbolMapWriters = new ObjList<>(metadata.getSymbolMapCount());
            this.denseSymbolTransientCountHandlers = new ObjList<>(metadata.getSymbolMapCount());
            this.nullSetters = new ObjList<>(columnCount);
            this.oooNullSetters = new ObjList<>(columnCount);
            this.row.activeNullSetters = nullSetters;
            this.columnTops = new LongList(columnCount);
            if (partitionBy != PartitionBy.NONE) {
                timestampFloorMethod = getPartitionFloor(partitionBy);
                timestampCeilMethod = getPartitionCeil(partitionBy);
                timestampAddMethod = getPartitionAdd(partitionBy);
                partitionDirFmt = getPartitionDateFmt(partitionBy);
            } else {
                timestampFloorMethod = null;
                timestampCeilMethod = null;
                timestampAddMethod = null;
                partitionDirFmt = null;
            }

            this.txFile.initPartitionBy(partitionBy);
            configureColumnMemory();
            timestampSetter = configureTimestampSetter();
            configureAppendPosition();
            purgeUnusedPartitions();
            clearTodoLog();
        } catch (CairoException e) {
            LOG.error().$("could not open '").$(path).$("' and this is why: {").$((Sinkable) e).$('}').$();
            doClose(false);
            throw e;
        }
    }

    public static int getPrimaryColumnIndex(int index) {
        return index * 2;
    }

    public static int getSecondaryColumnIndex(int index) {
        return getPrimaryColumnIndex(index) + 1;
    }

    public static long getTimestampIndexRow(long timestampIndex, long indexRow) {
        return Unsafe.getUnsafe().getLong(timestampIndex + indexRow * 16 + Long.BYTES);
    }

    public static long getTimestampIndexValue(long timestampIndex, long indexRow) {
        return Unsafe.getUnsafe().getLong(timestampIndex + indexRow * 16);
    }

    public static DateFormat selectPartitionDirFmt(int partitionBy) {
        switch (partitionBy) {
            case PartitionBy.DAY:
                return fmtDay;
            case PartitionBy.MONTH:
                return fmtMonth;
            case PartitionBy.YEAR:
                return fmtYear;
            default:
                return null;
        }
    }

    public void addColumn(CharSequence name, int type) {
        addColumn(name, type, configuration.getDefaultSymbolCapacity(), configuration.getDefaultSymbolCacheFlag(), false, 0, false);
    }

    /**
     * Adds new column to table, which can be either empty or can have data already. When existing columns
     * already have data this function will create ".top" file in addition to column files. ".top" file contains
     * size of partition at the moment of column creation. It must be used to accurately position inside new
     * column when either appending or reading.
     *
     * <b>Failures</b>
     * Adding new column can fail in many different situations. None of the failures affect integrity of data that is already in
     * the table but can leave instance of TableWriter in inconsistent state. When this happens function will throw CairoError.
     * Calling code must close TableWriter instance and open another when problems are rectified. Those problems would be
     * either with disk or memory or both.
     * <p>
     * Whenever function throws CairoException application code can continue using TableWriter instance and may attempt to
     * add columns again.
     *
     * <b>Transactions</b>
     * <p>
     * Pending transaction will be committed before function attempts to add column. Even when function is unsuccessful it may
     * still have committed transaction.
     *
     * @param name                    of column either ASCII or UTF8 encoded.
     * @param symbolCapacity          when column type is SYMBOL this parameter specifies approximate capacity for symbol map.
     *                                It should be equal to number of unique symbol values stored in the table and getting this
     *                                value badly wrong will cause performance degradation. Must be power of 2
     * @param symbolCacheFlag         when set to true, symbol values will be cached on Java heap.
     * @param type                    {@link ColumnType}
     * @param isIndexed               configures column to be indexed or not
     * @param indexValueBlockCapacity approximation of number of rows for single index key, must be power of 2
     * @param isSequential            for columns that contain sequential values query optimiser can make assuptions on range searches (future feature)
     */
    public void addColumn(
            CharSequence name,
            int type,
            int symbolCapacity,
            boolean symbolCacheFlag,
            boolean isIndexed,
            int indexValueBlockCapacity,
            boolean isSequential
    ) {

        assert indexValueBlockCapacity == Numbers.ceilPow2(indexValueBlockCapacity) : "power of 2 expected";
        assert symbolCapacity == Numbers.ceilPow2(symbolCapacity) : "power of 2 expected";
        assert TableUtils.isValidColumnName(name) : "invalid column name";

        checkDistressed();

        if (getColumnIndexQuiet(metaMem, name, columnCount) != -1) {
            throw CairoException.instance(0).put("Duplicate column name: ").put(name);
        }

        LOG.info().$("adding column '").utf8(name).$('[').$(ColumnType.nameOf(type)).$("]' to ").$(path).$();

        commit();

        removeColumnFiles(name, type, REMOVE_OR_EXCEPTION);

        // create new _meta.swp
        this.metaSwapIndex = addColumnToMeta(name, type, isIndexed, indexValueBlockCapacity, isSequential);

        // close _meta so we can rename it
        metaMem.close();

        // validate new meta
        validateSwapMeta(name);

        // rename _meta to _meta.prev
        renameMetaToMetaPrev(name);

        // after we moved _meta to _meta.prev
        // we have to have _todo to restore _meta should anything go wrong
        writeRestoreMetaTodo(name);

        // rename _meta.swp to _meta
        renameSwapMetaToMeta(name);

        if (type == ColumnType.SYMBOL) {
            try {
                createSymbolMapWriter(name, symbolCapacity, symbolCacheFlag);
            } catch (CairoException e) {
                runFragile(RECOVER_FROM_SYMBOL_MAP_WRITER_FAILURE, name, e);
            }
        } else {
            // maintain sparse list of symbol writers
            symbolMapWriters.extendAndSet(columnCount, null);
        }

        // add column objects
        configureColumn(type, isIndexed);

        // increment column count
        columnCount++;

        // extend columnTop list to make sure row cancel can work
        // need for setting correct top is hard to test without being able to read from table
        columnTops.extendAndSet(columnCount - 1, txFile.getTransientRowCount());

        // create column files
        if (txFile.getTransientRowCount() > 0 || partitionBy == PartitionBy.NONE) {
            try {
                openNewColumnFiles(name, isIndexed, indexValueBlockCapacity);
            } catch (CairoException e) {
                runFragile(RECOVER_FROM_COLUMN_OPEN_FAILURE, name, e);
            }
        }

        try {
            // open _meta file
            openMetaFile(ff, path, rootLen, metaMem);

            // remove _todo
            clearTodoLog();

        } catch (CairoException err) {
            throwDistressException(err);
        }

        txFile.bumpStructureVersion(this.denseSymbolMapWriters);

        metadata.addColumn(name, type, isIndexed, indexValueBlockCapacity);

        LOG.info().$("ADDED column '").utf8(name).$('[').$(ColumnType.nameOf(type)).$("]' to ").$(path).$();
    }

    public void addIndex(CharSequence columnName, int indexValueBlockSize) {
        assert indexValueBlockSize == Numbers.ceilPow2(indexValueBlockSize) : "power of 2 expected";

        checkDistressed();

        final int columnIndex = getColumnIndexQuiet(metaMem, columnName, columnCount);

        if (columnIndex == -1) {
            throw CairoException.instance(0).put("Invalid column name: ").put(columnName);
        }

        commit();

        if (isColumnIndexed(metaMem, columnIndex)) {
            throw CairoException.instance(0).put("already indexed [column=").put(columnName).put(']');
        }

        final int existingType = getColumnType(metaMem, columnIndex);
        LOG.info().$("adding index to '").utf8(columnName).$('[').$(ColumnType.nameOf(existingType)).$(", path=").$(path).$(']').$();

        if (existingType != ColumnType.SYMBOL) {
            LOG.error().$("cannot create index for [column='").utf8(columnName).$(", type=").$(ColumnType.nameOf(existingType)).$(", path=").$(path).$(']').$();
            throw CairoException.instance(0).put("cannot create index for [column='").put(columnName).put(", type=").put(ColumnType.nameOf(existingType)).put(", path=").put(path).put(']');
        }

        // create indexer
        final SymbolColumnIndexer indexer = new SymbolColumnIndexer();

        try {
            try {

                // edge cases here are:
                // column spans only part of table - e.g. it was added after table was created and populated
                // column has top value, e.g. does not span entire partition
                // to this end, we have a super-edge case:
                //
                if (partitionBy != PartitionBy.NONE) {
                    // run indexer for the whole table
                    final long timestamp = indexHistoricPartitions(indexer, columnName, indexValueBlockSize);
                    path.trimTo(rootLen);
                    setStateForTimestamp(path, timestamp, true);
                } else {
                    setStateForTimestamp(path, 0, false);
                }

                // create index in last partition
                indexLastPartition(indexer, columnName, columnIndex, indexValueBlockSize);

            } finally {
                path.trimTo(rootLen);
            }
        } catch (CairoException | CairoError e) {
            LOG.error().$("rolling back index created so far [path=").$(path).$(']').$();
            removeIndexFiles(columnName);
            throw e;
        }

        // set index flag in metadata
        // create new _meta.swp

        metaSwapIndex = copyMetadataAndSetIndexed(columnIndex, indexValueBlockSize);

        // close _meta so we can rename it
        metaMem.close();

        // validate new meta
        validateSwapMeta(columnName);

        // rename _meta to _meta.prev
        renameMetaToMetaPrev(columnName);

        // after we moved _meta to _meta.prev
        // we have to have _todo to restore _meta should anything go wrong
        writeRestoreMetaTodo(columnName);

        // rename _meta.swp to -_meta
        renameSwapMetaToMeta(columnName);

        try {
            // open _meta file
            openMetaFile(ff, path, rootLen, metaMem);

            // remove _todo
            clearTodoLog();

        } catch (CairoException err) {
            throwDistressException(err);
        }

        txFile.bumpStructureVersion(this.denseSymbolMapWriters);

        indexers.extendAndSet((columnIndex) / 2, indexer);
        populateDenseIndexerList();

        TableColumnMetadata columnMetadata = metadata.getColumnQuick(columnIndex);
        columnMetadata.setIndexed(true);
        columnMetadata.setIndexValueBlockCapacity(indexValueBlockSize);

        LOG.info().$("ADDED index to '").utf8(columnName).$('[').$(ColumnType.nameOf(existingType)).$("]' to ").$(path).$();
    }

    public int attachPartition(long timestamp) {
        // Partitioned table must have a timestamp
        // SQL compiler will check that table is partitioned
        assert metadata.getTimestampIndex() > -1;

        CharSequence timestampCol = metadata.getColumnQuick(metadata.getTimestampIndex()).getName();
        if (txFile.attachedPartitionsContains(timestamp)) {
            LOG.info().$("partition is already attached [path=").$(path).$(']').$();
            return PARTITION_ALREADY_ATTACHED;
        }

        if (metadata.getSymbolMapCount() > 0) {
            LOG.error().$("attaching partitions on table with symbols not yet supported [table=").$(name)
                    .$(",partition=").$ts(timestamp).I$();
            return TABLE_HAS_SYMBOLS;
        }

        try {
            setPathForPartition(path, partitionBy, timestamp);
            if (!ff.exists(path.$())) {
                setPathForPartition(other, partitionBy, timestamp);
                other.put(DETACHED_DIR_MARKER);

                if (ff.exists(other.$())) {
                    if (ff.rename(other, path)) {
                        LOG.info().$("moved partition dir: ").$(other).$(" to ").$(path).$();
                    } else {
                        throw CairoException.instance(ff.errno()).put("File system error on trying to rename [from=")
                                .put(other).put(",to=").put(path).put(']');
                    }
                }
            }

            if (ff.exists(path.$())) {
                // find out lo, hi ranges of partition attached as well as size
                final long partitionSize = readPartitionSizeMinMax(ff, path, timestampCol, tempMem16b, timestamp);
                if (partitionSize > 0) {
                    if (inTransaction()) {
                        LOG.info().$("committing open transaction before applying attach partition command [table=").$(name)
                                .$(",partition=").$ts(timestamp).I$();
                        commit();
                    }

                    attachPartitionCheckFilesMatchMetadata(ff, path, getMetadata(), partitionSize);
                    long minPartitionTimestamp = Unsafe.getUnsafe().getLong(tempMem16b);
                    long maxPartitionTimestamp = Unsafe.getUnsafe().getLong(tempMem16b + 8);

                    assert timestamp <= minPartitionTimestamp && minPartitionTimestamp <= maxPartitionTimestamp;

                    long nextMinTimestamp = Math.min(minPartitionTimestamp, txFile.getMinTimestamp());
                    long nextMaxTimestamp = Math.max(maxPartitionTimestamp, txFile.getMaxTimestamp());
                    boolean appendPartitionAttached = size() == 0 || getPartitionLo(nextMaxTimestamp) > getPartitionLo(txFile.getMaxTimestamp());

                    txFile.beginPartitionSizeUpdate();
                    txFile.updatePartitionSizeByTimestamp(timestamp, partitionSize);
                    txFile.finishPartitionSizeUpdate(nextMinTimestamp, nextMaxTimestamp);
                    txFile.commit(defaultCommitMode, denseSymbolMapWriters);

                    if (appendPartitionAttached) {
                        freeColumns(true);
                        configureAppendPosition();
                    }

                    LOG.info().$("partition attached [path=").$(path).$(']').$();
                } else {
                    LOG.error().$("cannot detect partition size [path=").$(path).$(",timestampColumn=").$(timestampCol).$(']').$();
                    return PARTITION_EMPTY;
                }
            } else {
                LOG.error().$("cannot attach missing partition [path=").$(path).$(']').$();
                return CANNOT_ATTACH_MISSING_PARTITION;
            }

        } finally {
            path.trimTo(rootLen);
            other.trimTo(rootLen);
        }

        return StatusCode.OK;
    }

    public void changeCacheFlag(int columnIndex, boolean cache) {
        checkDistressed();

        commit();

        SymbolMapWriter symbolMapWriter = getSymbolMapWriter(columnIndex);
        if (symbolMapWriter.isCached() != cache) {
            symbolMapWriter.updateCacheFlag(cache);
        } else {
            return;
        }

        txFile.bumpStructureVersion(this.denseSymbolMapWriters);
    }

    @Override
    public void close() {
        if (null != blockWriter) {
            blockWriter.clear();
        }
        if (isOpen() && lifecycleManager.close()) {
            doClose(true);
        }
    }

    public void commit() {
        commit(defaultCommitMode);
    }

    /**
     * Commits newly added rows of data. This method updates transaction file with pointers to end of appended data.
     * <p>
     * <b>Pending rows</b>
     * <p>This method will cancel pending rows by calling {@link #cancelRow()}. Data in partially appended row will be lost.</p>
     *
     * @param commitMode commit durability mode.
     */
    public void commit(int commitMode) {

        checkDistressed();

        if ((masterRef & 1) != 0) {
            cancelRow();
        }

        if (inTransaction()) {

            if (oooRowCount > 0) {
                oooProcess();
            }

            if (commitMode != CommitMode.NOSYNC) {
                syncColumns(commitMode);
            }

            updateIndexes();

            txFile.commit(commitMode, this.denseSymbolMapWriters);
        }
    }

    public int getColumnIndex(CharSequence name) {
        int index = metadata.getColumnIndexQuiet(name);
        if (index > -1) {
            return index;
        }
        throw CairoException.instance(0).put("Invalid column name: ").put(name);
    }

    public String getDesignatedTimestampColumnName() {
        return designatedTimestampColumnName;
    }

    public long getMaxTimestamp() {
        return txFile.getMaxTimestamp();
    }

    public RecordMetadata getMetadata() {
        return metadata;
    }

    public CharSequence getName() {
        return name;
    }

    public int getPartitionBy() {
        return partitionBy;
    }

    public long getStructureVersion() {
        return txFile.getStructureVersion();
    }

    public int getSymbolIndex(int columnIndex, CharSequence symValue) {
        return symbolMapWriters.getQuick(columnIndex).put(symValue);
    }

    public long getTxn() {
        return txFile.getTxn();
    }

    public boolean inTransaction() {
        return txFile != null && txFile.inTransaction();
    }

    public boolean isOpen() {
        return tempMem16b != 0;
    }

    public TableBlockWriter newBlock() {
        bumpMasterRef();
        txFile.newBlock();
        blockWriter.open(this);
        return blockWriter;
    }

    public Row newRow(long timestamp) {
        return rowFunction.newRow(timestamp);
    }

    public Row newRow() {
        return newRow(0L);
    }

    public long partitionNameToTimestamp(CharSequence partitionName) {
        if (partitionDirFmt == null) {
            throw CairoException.instance(0).put("table is not partitioned");
        }
        try {
            return partitionDirFmt.parse(partitionName, null);
        } catch (NumericException e) {
            final CairoException ee = CairoException.instance(0);
            switch (partitionBy) {
                case PartitionBy.DAY:
                    ee.put("'YYYY-MM-DD'");
                    break;
                case PartitionBy.MONTH:
                    ee.put("'YYYY-MM'");
                    break;
                default:
                    ee.put("'YYYY'");
                    break;
            }
            ee.put(" expected");
            throw ee;
        }
    }

    public void removeColumn(CharSequence name) {

        checkDistressed();

        final int index = getColumnIndex(name);
        final int type = metadata.getColumnType(index);

        LOG.info().$("removing column '").utf8(name).$("' from ").$(path).$();

        // check if we are moving timestamp from a partitioned table
        final int timestampIndex = metaMem.getInt(META_OFFSET_TIMESTAMP_INDEX);
        boolean timestamp = index == timestampIndex;

        if (timestamp && partitionBy != PartitionBy.NONE) {
            throw CairoException.instance(0).put("Cannot remove timestamp from partitioned table");
        }

        commit();

        final CharSequence timestampColumnName = timestampIndex != -1 ? metadata.getColumnName(timestampIndex) : null;

        this.metaSwapIndex = removeColumnFromMeta(index);

        // close _meta so we can rename it
        metaMem.close();

        // rename _meta to _meta.prev
        renameMetaToMetaPrev(name);

        // after we moved _meta to _meta.prev
        // we have to have _todo to restore _meta should anything go wrong
        writeRestoreMetaTodo(name);

        // rename _meta.swp to _meta
        renameSwapMetaToMeta(name);

        // remove column objects
        removeColumn(index);

        // remove symbol map writer or entry for such
        removeSymbolMapWriter(index);

        // decrement column count
        columnCount--;

        // reset timestamp limits
        if (timestamp) {
            txFile.resetTimestamp();
            timestampSetter = value -> {
            };
        }

        try {
            // open _meta file
            openMetaFile(ff, path, rootLen, metaMem);

            // remove _todo
            clearTodoLog();

            // remove column files has to be done after _todo is removed
            removeColumnFiles(name, type, REMOVE_OR_LOG);
        } catch (CairoException err) {
            throwDistressException(err);
        }

        txFile.bumpStructureVersion(this.denseSymbolMapWriters);

        metadata.removeColumn(name);
        if (timestamp) {
            metadata.setTimestampIndex(-1);
        } else if (timestampColumnName != null) {
            metadata.setTimestampIndex(metadata.getColumnIndex(timestampColumnName));
        }

        LOG.info().$("REMOVED column '").utf8(name).$("' from ").$(path).$();
    }

    public boolean removePartition(long timestamp) {
        long minTimestamp = txFile.getMinTimestamp();
        long maxTimestamp = txFile.getMaxTimestamp();

        if (partitionBy == PartitionBy.NONE) {
            return false;
        }
        timestamp = getPartitionLo(timestamp);
        if (timestamp < getPartitionLo(minTimestamp) || timestamp > maxTimestamp) {
            return false;
        }

        if (timestamp == getPartitionLo(maxTimestamp)) {
            LOG.error()
                    .$("cannot remove active partition [path=").$(path)
                    .$(", maxTimestamp=").$ts(maxTimestamp)
                    .$(']').$();
            return false;
        }

        if (!txFile.attachedPartitionsContains(timestamp)) {
            LOG.error().$("partition is already detached [path=").$(path).$(']').$();
            return false;
        }

        try {
            // when we want to delete first partition we must find out
            // minTimestamp from next partition if it exists or next partition and so on
            //
            // when somebody removed data directories manually and then
            // attempts to tidy up metadata with logical partition delete
            // we have to uphold the effort and re-compute table size and its minTimestamp from
            // what remains on disk

            // find out if we are removing min partition
            setStateForTimestamp(path, timestamp, false);
            long nextMinTimestamp = minTimestamp;
            if (timestamp == txFile.getPartitionTimestamp(0)) {
                nextMinTimestamp = readMinTimestamp(txFile.getPartitionTimestamp(1));
            }
            txFile.beginPartitionSizeUpdate();
            txFile.removeAttachedPartitions(timestamp);
            txFile.setMinTimestamp(nextMinTimestamp);
            txFile.finishPartitionSizeUpdate(nextMinTimestamp, txFile.getMaxTimestamp());
            txFile.commit(defaultCommitMode, denseSymbolMapWriters);

            if (ff.exists(path.$())) {
                if (!ff.rmdir(path.chopZ().put(Files.SEPARATOR).$())) {
                    LOG.info().$("partition directory delete is postponed [ath=").$(path).$(']').$();
                } else {
                    LOG.info().$("partition marked for delete [path=").$(path).$(']').$();
                }
            } else {
                LOG.info().$("partition absent on disk now delached from table [path=").$(path).$(']').$();
            }
            return true;
        } finally {
            path.trimTo(rootLen);
        }
    }

    public void removePartition(Function function, int posForError) throws SqlException {
        if (partitionBy == PartitionBy.NONE) {
            throw SqlException.$(posForError, "table is not partitioned");
        }

        if (txFile.getPartitionsCount() == 0) {
            throw SqlException.$(posForError, "table is empty");
        } else {
            // Drop partitions in descending order so if folders are missing on disk
            // removePartition does not fail to determine next minTimestamp
            for (int i = txFile.getPartitionsCount() - 1; i > -1; i--) {
                long partitionTimestamp = txFile.getPartitionTimestamp(i);
                dropPartitionFunctionRec.setTimestamp(partitionTimestamp);
                if (function.getBool(dropPartitionFunctionRec)) {
                    removePartition(partitionTimestamp);
                }
            }
        }
    }

    public void renameColumn(CharSequence currentName, CharSequence newName) {

        checkDistressed();

        final int index = getColumnIndex(currentName);
        final int type = metadata.getColumnType(index);

        LOG.info().$("renaming column '").utf8(currentName).$("' to '").utf8(newName).$("' from ").$(path).$();

        commit();

        this.metaSwapIndex = renameColumnFromMeta(index, newName);

        // close _meta so we can rename it
        metaMem.close();

        // rename _meta to _meta.prev
        renameMetaToMetaPrev(currentName);

        // after we moved _meta to _meta.prev
        // we have to have _todo to restore _meta should anything go wrong
        writeRestoreMetaTodo(currentName);

        // rename _meta.swp to _meta
        renameSwapMetaToMeta(currentName);

        try {
            // open _meta file
            openMetaFile(ff, path, rootLen, metaMem);

            // remove _todo
            clearTodoLog();

            // rename column files has to be done after _todo is removed
            renameColumnFiles(currentName, newName, type);
        } catch (CairoException err) {
            throwDistressException(err);
        }

        txFile.bumpStructureVersion(this.denseSymbolMapWriters);

        metadata.renameColumn(currentName, newName);

        if (index == metadata.getTimestampIndex()) {
            designatedTimestampColumnName = Chars.toString(newName);
        }

        LOG.info().$("RENAMED column '").utf8(currentName).$("' to '").utf8(newName).$("' from ").$(path).$();
    }

    public void rollback() {
        checkDistressed();
        if (inTransaction()) {
            LOG.info().$("tx rollback [name=").$(name).$(']').$();
            freeColumns(false);
            configureAppendPosition();
            rollbackIndexes();
            purgeUnusedPartitions();
            LOG.info().$("tx rollback complete [name=").$(name).$(']').$();
        }
    }

    public void setLifecycleManager(LifecycleManager lifecycleManager) {
        this.lifecycleManager = lifecycleManager;
    }

    public long size() {
        return txFile.getRowCount();
    }

    @Override
    public String toString() {
        return "TableWriter{" +
                "name=" + name +
                '}';
    }

    public void transferLock(long lockFd) {
        assert lockFd != -1;
        this.lockFd = lockFd;
    }

    /**
     * Truncates table. When operation is unsuccessful it throws CairoException. With that truncate can be
     * retried or alternatively table can be closed. Outcome of any other operation with the table is undefined
     * and likely to cause segmentation fault. When table re-opens any partial truncate will be retried.
     */
    public final void truncate() {

        // we do this before size check so that "old" corrupt symbol tables are brought back in line
        for (int i = 0, n = denseSymbolMapWriters.size(); i < n; i++) {
            denseSymbolMapWriters.getQuick(i).truncate();
        }

        if (size() == 0) {
            return;
        }

        // this is a crude block to test things for now
        todoMem.putLong(0, ++todoTxn); // write txn, reader will first read txn at offset 24 and then at offset 0
        Unsafe.getUnsafe().storeFence(); // make sure we do not write hash before writing txn (view from another thread)
        todoMem.putLong(8, configuration.getInstanceHashLo()); // write out our instance hashes
        todoMem.putLong(16, configuration.getInstanceHashHi());
        Unsafe.getUnsafe().storeFence();
        todoMem.putLong(24, todoTxn);
        todoMem.putLong(32, 1);
        todoMem.putLong(40, TODO_TRUNCATE);
        todoMem.setSize(48);

        for (int i = 0; i < columnCount; i++) {
            getPrimaryColumn(i).truncate();
            AppendOnlyVirtualMemory mem = getSecondaryColumn(i);
            if (mem != null) {
                mem.truncate();
            }
        }

        if (partitionBy != PartitionBy.NONE) {
            freeColumns(false);
            if (indexers != null) {
                for (int i = 0, n = indexers.size(); i < n; i++) {
                    Misc.free(indexers.getQuick(i));
                }
            }
            removePartitionDirectories();
            rowFunction = openPartitionFunction;
        }

        txFile.resetTimestamp();
        txFile.truncate();

        try {
            clearTodoLog();
        } catch (CairoException err) {
            throwDistressException(err);
        }

        LOG.info().$("truncated [name=").$(name).$(']').$();
    }

    public void updateMetadataVersion() {

        checkDistressed();

        commit();
        // create new _meta.swp
        this.metaSwapIndex = copyMetadataAndUpdateVersion();

        // close _meta so we can rename it
        metaMem.close();

        // rename _meta to _meta.prev
        this.metaPrevIndex = rename(fileOperationRetryCount);

        // rename _meta.swp to -_meta
        restoreMetaFrom(META_SWAP_FILE_NAME, metaSwapIndex);

        try {
            // open _meta file
            openMetaFile(ff, path, rootLen, metaMem);
        } catch (CairoException err) {
            throwDistressException(err);
        }

        txFile.bumpStructureVersion(this.denseSymbolMapWriters);
        metadata.setTableVersion();
    }

    public void updateSymbols(int columnIndex, SymbolMapReader symReader) {
        int nSourceSymbols = symReader.size();
        SymbolMapWriter symWriter = getSymbolMapWriter(columnIndex);
        int nDestinationSymbols = symWriter.getSymbolCount();

        if (nSourceSymbols > nDestinationSymbols) {
            long address = symReader.symbolCharsAddressOf(nDestinationSymbols);
            long addressHi = symReader.symbolCharsAddressOf(nSourceSymbols);
            symWriter.appendSymbolCharsBlock(addressHi - address, address);
        }
    }

    /**
     * Eagerly sets up writer instance. Otherwise writer will initialize lazily. Invoking this method could improve
     * performance of some applications. UDP receivers use this in order to avoid initial receive buffer contention.
     */
    public void warmUp() {
        Row r = newRow(txFile.getMaxTimestamp());
        try {
            for (int i = 0; i < columnCount; i++) {
                r.putByte(i, (byte) 0);
            }
        } finally {
            r.cancel();
        }
    }

    private static void removeFileAndOrLog(FilesFacade ff, LPSZ name) {
        if (ff.exists(name)) {
            if (ff.remove(name)) {
                LOG.info().$("removed: ").$(name).$();
            } else {
                LOG.error().$("cannot remove: ").utf8(name).$(" [errno=").$(ff.errno()).$(']').$();
            }
        }
    }

    private static void renameFileOrLog(FilesFacade ff, LPSZ name, LPSZ to) {
        if (ff.exists(name)) {
            if (ff.rename(name, to)) {
                LOG.info().$("renamed: ").$(name).$();
            } else {
                LOG.error().$("cannot rename: ").utf8(name).$(" [errno=").$(ff.errno()).$(']').$();
            }
        }
    }

    static void indexAndCountDown(ColumnIndexer indexer, long lo, long hi, SOCountDownLatch latch) {
        try {
            indexer.refreshSourceAndIndex(lo, hi);
        } catch (CairoException e) {
            indexer.distress();
            LOG.error().$("index error [fd=").$(indexer.getFd()).$(']').$('{').$((Sinkable) e).$('}').$();
        } finally {
            latch.countDown();
        }
    }

    private static void removeOrException(FilesFacade ff, LPSZ path) {
        if (ff.exists(path) && !ff.remove(path)) {
            throw CairoException.instance(ff.errno()).put("Cannot remove ").put(path);
        }
    }

    private static void setColumnSize(
            FilesFacade ff,
            AppendOnlyVirtualMemory mem1,
            AppendOnlyVirtualMemory mem2,
            int type,
            long actualPosition,
            long buf,
            boolean ensureFileSize
    ) {
        long offset;
        long len;
        long mem1Size;
        if (actualPosition > 0) {
            // subtract column top
            switch (type) {
                case ColumnType.BINARY:
                    assert mem2 != null;
                    readOffsetBytes(ff, mem2, actualPosition, buf);
                    offset = Unsafe.getUnsafe().getLong(buf);
                    readBytes(ff, mem1, buf, Long.BYTES, offset, "Cannot read length, fd=");
                    len = Unsafe.getUnsafe().getLong(buf);
                    mem1Size = len == -1 ? offset + Long.BYTES : offset + len + Long.BYTES;
                    if (ensureFileSize) {
                        mem1.ensureFileSize(mem1.pageIndex(mem1Size));
                        mem2.ensureFileSize(mem2.pageIndex(actualPosition * Long.BYTES));
                    }
                    mem1.setSize(mem1Size);
                    mem2.setSize(actualPosition * Long.BYTES);
                    break;
                case ColumnType.STRING:
                    assert mem2 != null;
                    readOffsetBytes(ff, mem2, actualPosition, buf);
                    offset = Unsafe.getUnsafe().getLong(buf);
                    readBytes(ff, mem1, buf, Integer.BYTES, offset, "Cannot read length, fd=");
                    len = Unsafe.getUnsafe().getInt(buf);
                    mem1Size = len == -1 ? offset + Integer.BYTES : offset + len * Character.BYTES + Integer.BYTES;
                    if (ensureFileSize) {
                        mem1.ensureFileSize(mem1.pageIndex(mem1Size));
                        mem2.ensureFileSize(mem2.pageIndex(actualPosition * Long.BYTES));
                    }
                    mem1.setSize(mem1Size);
                    mem2.setSize(actualPosition * Long.BYTES);
                    break;
                default:
                    mem1Size = actualPosition << ColumnType.pow2SizeOf(type);
                    if (ensureFileSize) {
                        mem1.ensureFileSize(mem1.pageIndex(mem1Size));
                    }
                    mem1.setSize(mem1Size);
                    break;
            }
        } else {
            mem1.setSize(0);
            if (mem2 != null) {
                mem2.setSize(0);
            }
        }
    }

    /**
     * This an O(n) method to find if column by the same name already exists. The benefit of poor performance
     * is that we don't keep column name strings on heap. We only use this method when adding new column, where
     * high performance of name check does not matter much.
     *
     * @param name to check
     * @return 0 based column index.
     */
    private static int getColumnIndexQuiet(MappedReadOnlyMemory metaMem, CharSequence name, int columnCount) {
        long nameOffset = getColumnNameOffset(columnCount);
        for (int i = 0; i < columnCount; i++) {
            CharSequence col = metaMem.getStr(nameOffset);
            if (Chars.equalsIgnoreCase(col, name)) {
                return i;
            }
            nameOffset += VmUtils.getStorageLength(col);
        }
        return -1;
    }

    private static void readOffsetBytes(FilesFacade ff, AppendOnlyVirtualMemory mem, long position, long buf) {
        readBytes(ff, mem, buf, 8, (position - 1) * 8, "could not read offset, fd=");
    }

    private static void readBytes(FilesFacade ff, AppendOnlyVirtualMemory mem, long buf, int byteCount, long offset, CharSequence errorMsg) {
        if (ff.read(mem.getFd(), buf, byteCount, offset) != byteCount) {
            throw CairoException.instance(ff.errno()).put(errorMsg).put(mem.getFd()).put(", offset=").put(offset);
        }
    }

    private static void configureNullSetters(ObjList<Runnable> nullers, int type, WriteOnlyVirtualMemory mem1, WriteOnlyVirtualMemory mem2) {
        switch (type) {
            case ColumnType.BOOLEAN:
            case ColumnType.BYTE:
                nullers.add(() -> mem1.putByte((byte) 0));
                break;
            case ColumnType.DOUBLE:
                nullers.add(() -> mem1.putDouble(Double.NaN));
                break;
            case ColumnType.FLOAT:
                nullers.add(() -> mem1.putFloat(Float.NaN));
                break;
            case ColumnType.INT:
                nullers.add(() -> mem1.putInt(Numbers.INT_NaN));
                break;
            case ColumnType.LONG:
            case ColumnType.DATE:
            case ColumnType.TIMESTAMP:
                nullers.add(() -> mem1.putLong(Numbers.LONG_NaN));
                break;
            case ColumnType.LONG256:
                nullers.add(() -> mem1.putLong256(Numbers.LONG_NaN, Numbers.LONG_NaN, Numbers.LONG_NaN, Numbers.LONG_NaN));
                break;
            case ColumnType.SHORT:
                nullers.add(() -> mem1.putShort((short) 0));
                break;
            case ColumnType.CHAR:
                nullers.add(() -> mem1.putChar((char) 0));
                break;
            case ColumnType.STRING:
                nullers.add(() -> mem2.putLong(mem1.putNullStr()));
                break;
            case ColumnType.SYMBOL:
                nullers.add(() -> mem1.putInt(SymbolTable.VALUE_IS_NULL));
                break;
            case ColumnType.BINARY:
                nullers.add(() -> mem2.putLong(mem1.putNullBin()));
                break;
            default:
                break;
        }
    }

    private static void openMetaFile(FilesFacade ff, Path path, int rootLen, MappedReadOnlyMemory metaMem) {
        path.concat(META_FILE_NAME).$();
        try {
            metaMem.of(ff, path, ff.getPageSize(), ff.length(path));
        } finally {
            path.trimTo(rootLen);
        }
    }

    private static void attachPartitionCheckFilesMatchMetadata(FilesFacade ff, Path path, RecordMetadata metadata, long partitionSize) throws CairoException {
        // for each column, check that file exist in the partition folder
        int rootLen = path.length();
        for (int columnIndex = 0, size = metadata.getColumnCount(); columnIndex < size; columnIndex++) {
            try {
                int columnType = metadata.getColumnType(columnIndex);
                var columnName = metadata.getColumnName(columnIndex);
                path.concat(columnName);

                switch (columnType) {
                    case ColumnType.INT:
                    case ColumnType.LONG:
                    case ColumnType.BOOLEAN:
                    case ColumnType.BYTE:
                    case ColumnType.TIMESTAMP:
                    case ColumnType.DATE:
                    case ColumnType.DOUBLE:
                    case ColumnType.CHAR:
                    case ColumnType.SHORT:
                    case ColumnType.FLOAT:
                    case ColumnType.LONG256:
                        // Consider Symbols as fixed, check data file size
                    case ColumnType.SYMBOL:
                        attachPartitionCheckFilesMatchFixedColumn(ff, path, columnType, partitionSize);
                        break;
                    case ColumnType.STRING:
                    case ColumnType.BINARY:
                        attachPartitionCheckFilesMatchVarLenColumn(ff, path, partitionSize);
                        break;
                }
            } finally {
                path.trimTo(rootLen);
            }
        }
    }

    private static void attachPartitionCheckFilesMatchVarLenColumn(FilesFacade ff, Path path, long partitionSize) {
        int pathLen = path.length();
        path.put(FILE_SUFFIX_I).$();

        if (ff.exists(path)) {
            int typeSize = 4;
            long fileSize = ff.length(path);
            if (fileSize < partitionSize * typeSize) {
                throw CairoException.instance(0).put("Column file row count does not match timestamp file row count. " +
                        "Partition files inconsistent [file=")
                        .put(path)
                        .put(",expectedSize=")
                        .put(partitionSize * typeSize)
                        .put(",actual=")
                        .put(fileSize)
                        .put(']');
            }

            path.trimTo(pathLen);
            path.put(FILE_SUFFIX_D).$();
            if (ff.exists(path)) {
                // good
                return;
            }
        }
        throw CairoException.instance(0).put("Column file does not exist [path=").put(path).put(']');
    }

    private static void attachPartitionCheckFilesMatchFixedColumn(FilesFacade ff, Path path, int columnType, long partitionSize) {
        path.put(FILE_SUFFIX_D).$();
        if (ff.exists(path)) {
            long fileSize = ff.length(path);
            if (fileSize < partitionSize << ColumnType.pow2SizeOf(columnType)) {
                throw CairoException.instance(0).put("Column file row count does not match timestamp file row count. " +
                        "Partition files inconsistent [file=")
                        .put(path)
                        .put(",expectedSize=")
                        .put(partitionSize << ColumnType.pow2SizeOf(columnType))
                        .put(",actual=")
                        .put(fileSize)
                        .put(']');
            }
            return;
        }
        throw CairoException.instance(0).put("Column file does not exist [path=").put(path).put(']');
    }

    private int addColumnToMeta(
            CharSequence name,
            int type,
            boolean indexFlag,
            int indexValueBlockCapacity,
            boolean sequentialFlag
    ) {
        int index;
        try {
            index = openMetaSwapFile(ff, ddlMem, path, rootLen, configuration.getMaxSwapFileCount());
            int columnCount = metaMem.getInt(META_OFFSET_COUNT);

            ddlMem.putInt(columnCount + 1);
            ddlMem.putInt(metaMem.getInt(META_OFFSET_PARTITION_BY));
            ddlMem.putInt(metaMem.getInt(META_OFFSET_TIMESTAMP_INDEX));
            ddlMem.putInt(ColumnType.VERSION);
            ddlMem.putInt(metaMem.getInt(META_OFFSET_TABLE_ID));
            ddlMem.jumpTo(META_OFFSET_COLUMN_TYPES);
            for (int i = 0; i < columnCount; i++) {
                writeColumnEntry(i);
            }

            // add new column metadata to bottom of list
            ddlMem.putByte((byte) type);
            long flags = 0;
            if (indexFlag) {
                flags |= META_FLAG_BIT_INDEXED;
            }

            if (sequentialFlag) {
                flags |= META_FLAG_BIT_SEQUENTIAL;
            }

            ddlMem.putLong(flags);
            ddlMem.putInt(indexValueBlockCapacity);
            ddlMem.skip(META_COLUMN_DATA_RESERVED);

            long nameOffset = getColumnNameOffset(columnCount);
            for (int i = 0; i < columnCount; i++) {
                CharSequence columnName = metaMem.getStr(nameOffset);
                ddlMem.putStr(columnName);
                nameOffset += VmUtils.getStorageLength(columnName);
            }
            ddlMem.putStr(name);
        } finally {
            ddlMem.close();
        }
        return index;
    }

    private void bumpMasterRef() {
        if ((masterRef & 1) == 0) {
            masterRef++;
        } else {
            cancelRowAndBump();
        }
    }

    void bumpOooErrorCount() {
        oooErrorCount.incrementAndGet();
    }

    void bumpPartitionUpdateCount() {
        oooUpdRemaining.decrementAndGet();
    }

    void cancelRow() {

        if ((masterRef & 1) == 0) {
            return;
        }

        long dirtyMaxTimestamp = txFile.getMaxTimestamp();
        long dirtyTransientRowCount = txFile.getTransientRowCount();
        long rollbackToMaxTimestamp = txFile.cancelToMaxTimestamp();
        long rollbackToTransientRowCount = txFile.cancelToTransientRowCount();

        if (dirtyTransientRowCount == 0) {
            if (partitionBy != PartitionBy.NONE) {
                // we have to undo creation of partition
                freeColumns(false);
                if (removeDirOnCancelRow) {
                    try {
                        setStateForTimestamp(path, dirtyMaxTimestamp, false);
                        if (!ff.rmdir(path.$())) {
                            throw CairoException.instance(ff.errno()).put("Cannot remove directory: ").put(path);
                        }
                        removeDirOnCancelRow = false;
                    } finally {
                        path.trimTo(rootLen);
                    }
                }

                // open old partition
                if (rollbackToMaxTimestamp > Long.MIN_VALUE) {
                    try {
                        openPartition(rollbackToMaxTimestamp);
                        setAppendPosition(rollbackToTransientRowCount, false);
                    } catch (CairoException e) {
                        freeColumns(false);
                        throw e;
                    }
                } else {
                    rowFunction = openPartitionFunction;
                }

                // undo counts
                removeDirOnCancelRow = true;
                txFile.cancelRow();
            } else {
                txFile.cancelRow();
                // we only have one partition, jump to start on every column
                for (int i = 0; i < columnCount; i++) {
                    getPrimaryColumn(i).setSize(0);
                    AppendOnlyVirtualMemory mem = getSecondaryColumn(i);
                    if (mem != null) {
                        mem.setSize(0);
                    }
                }
            }
        } else {
            txFile.cancelRow();
            // we are staying within same partition, prepare append positions for row count
            boolean rowChanged = false;
            // verify if any of the columns have been changed
            // if not - we don't have to do
            for (int i = 0; i < columnCount; i++) {
                if (refs.getQuick(i) == masterRef) {
                    rowChanged = true;
                    break;
                }
            }

            // is no column has been changed we take easy option and do nothing
            if (rowChanged) {
                setAppendPosition(txFile.getTransientRowCount(), false);
            }
        }
        refs.fill(0, columnCount, --masterRef);
    }

    private void cancelRowAndBump() {
        cancelRow();
        masterRef++;
    }

    private long ceilMaxTimestamp() {
        long maxTimestamp = this.txFile.getMaxTimestamp();
        switch (partitionBy) {
            case PartitionBy.DAY:
                return Timestamps.ceilDD(maxTimestamp);
            case PartitionBy.MONTH:
                return Timestamps.ceilMM(maxTimestamp);
            case PartitionBy.YEAR:
                return Timestamps.ceilYYYY(maxTimestamp);
            default:
                assert false;
                return -1;
        }
    }

    private void checkDistressed() {
        if (!distressed) {
            return;
        }
        throw new CairoError("Table '" + name.toString() + "' is distressed");
    }

    private void clearTodoLog() {
        try {
            todoMem.putLong(0, ++todoTxn); // write txn, reader will first read txn at offset 24 and then at offset 0
            Unsafe.getUnsafe().storeFence(); // make sure we do not write hash before writing txn (view from another thread)
            todoMem.putLong(8, 0); // write out our instance hashes
            todoMem.putLong(16, 0);
            Unsafe.getUnsafe().storeFence();
            todoMem.putLong(32, 0);
            Unsafe.getUnsafe().storeFence();
            todoMem.putLong(24, todoTxn);
            todoMem.setSize(40);
        } finally {
            path.trimTo(rootLen);
        }
    }

    void closeActivePartition() {
        closeAppendMemoryNoTruncate(false);
        Misc.freeObjList(denseIndexers);
        denseIndexers.clear();
    }

    private void closeAppendMemoryNoTruncate(boolean truncate) {
        for (int i = 0, n = columns.size(); i < n; i++) {
            AppendOnlyVirtualMemory m = columns.getQuick(i);
            if (m != null) {
                m.close(truncate);
            }
        }
    }

    void commitBlock(long firstTimestamp) {
        if (txFile.getMinTimestamp() == Long.MAX_VALUE) {
            txFile.setMinTimestamp(firstTimestamp);
        }

        for (int i = 0; i < columnCount; i++) {
            refs.setQuick(i, masterRef);
        }

        masterRef++;
        commit();

        setAppendPosition(txFile.getTransientRowCount(), true);
    }

    private void configureAppendPosition() {
        this.txFile.readUnchecked();
        if (this.txFile.getMaxTimestamp() > Long.MIN_VALUE || partitionBy == PartitionBy.NONE) {
            openFirstPartition(this.txFile.getMaxTimestamp());
            if (partitionBy == PartitionBy.NONE) {
                if (metadata.getTimestampIndex() < 0) {
                    rowFunction = noTimestampFunction;
                } else {
                    rowFunction = noPartitionFunction;
                }
            } else {
                rowFunction = switchPartitionFunction;
            }
        } else {
            rowFunction = openPartitionFunction;
        }
    }

    private void configureColumn(int type, boolean indexFlag) {
        final AppendOnlyVirtualMemory primary = new AppendOnlyVirtualMemory();
        final AppendOnlyVirtualMemory secondary;
        final ContiguousVirtualMemory oooPrimary = new ContiguousVirtualMemory(16 * Numbers.SIZE_1MB, Integer.MAX_VALUE);
        final ContiguousVirtualMemory oooSecondary;
        final ContiguousVirtualMemory oooPrimary2 = new ContiguousVirtualMemory(16 * Numbers.SIZE_1MB, Integer.MAX_VALUE);
        final ContiguousVirtualMemory oooSecondary2;
        switch (type) {
            case ColumnType.BINARY:
            case ColumnType.STRING:
                secondary = new AppendOnlyVirtualMemory();
                oooSecondary = new ContiguousVirtualMemory(16 * Numbers.SIZE_1MB, Integer.MAX_VALUE);
                oooSecondary2 = new ContiguousVirtualMemory(16 * Numbers.SIZE_1MB, Integer.MAX_VALUE);
                break;
            default:
                secondary = null;
                oooSecondary = null;
                oooSecondary2 = null;
                break;
        }
        columns.add(primary);
        columns.add(secondary);
        oooColumns.add(oooPrimary);
        oooColumns.add(oooSecondary);
        oooColumns2.add(oooPrimary2);
        oooColumns2.add(oooSecondary2);
        configureNullSetters(nullSetters, type, primary, secondary);
        configureNullSetters(oooNullSetters, type, oooPrimary, oooSecondary);
        if (indexFlag) {
            indexers.extendAndSet((columns.size() - 1) / 2, new SymbolColumnIndexer());
            populateDenseIndexerList();
        }
        refs.add(0);
    }

    private void configureColumnMemory() {
        this.symbolMapWriters.setPos(columnCount);
        for (int i = 0; i < columnCount; i++) {
            int type = metadata.getColumnType(i);
            configureColumn(type, metadata.isColumnIndexed(i));

            if (type == ColumnType.SYMBOL) {
                final int symbolIndex = denseSymbolMapWriters.size();
                WriterTransientSymbolCountChangeHandler transientSymbolCountChangeHandler = new WriterTransientSymbolCountChangeHandler(symbolIndex);
                denseSymbolTransientCountHandlers.add(transientSymbolCountChangeHandler);
                SymbolMapWriter symbolMapWriter = new SymbolMapWriter(configuration, path.trimTo(rootLen), metadata.getColumnName(i), txFile.readSymbolCount(symbolIndex),
                        transientSymbolCountChangeHandler);

                symbolMapWriters.extendAndSet(i, symbolMapWriter);
                denseSymbolMapWriters.add(symbolMapWriter);
            }

            if (metadata.isColumnIndexed(i)) {
                indexers.extendAndSet(i, new SymbolColumnIndexer());
            }
        }
        final int timestampIndex = metadata.getTimestampIndex();
        if (timestampIndex != -1) {
            // todo: when column is removed we need to update this again to reflect shifted columns
            timestampMergeMem = oooColumns.getQuick(getPrimaryColumnIndex(timestampIndex));
        }
        populateDenseIndexerList();
    }

    private LongConsumer configureTimestampSetter() {
        int index = metadata.getTimestampIndex();
        if (index == -1) {
            return value -> {
            };
        } else {
            nullSetters.setQuick(index, NOOP);
            oooNullSetters.setQuick(index, NOOP);
            return getPrimaryColumn(index)::putLong;
        }
    }

    private int copyMetadataAndSetIndexed(int columnIndex, int indexValueBlockSize) {
        try {
            int index = openMetaSwapFile(ff, ddlMem, path, rootLen, configuration.getMaxSwapFileCount());
            int columnCount = metaMem.getInt(META_OFFSET_COUNT);
            ddlMem.putInt(columnCount);
            ddlMem.putInt(metaMem.getInt(META_OFFSET_PARTITION_BY));
            ddlMem.putInt(metaMem.getInt(META_OFFSET_TIMESTAMP_INDEX));
            ddlMem.putInt(ColumnType.VERSION);
            ddlMem.putInt(metaMem.getInt(META_OFFSET_TABLE_ID));
            ddlMem.jumpTo(META_OFFSET_COLUMN_TYPES);
            for (int i = 0; i < columnCount; i++) {
                if (i != columnIndex) {
                    writeColumnEntry(i);
                } else {
                    ddlMem.putByte((byte) getColumnType(metaMem, i));
                    long flags = META_FLAG_BIT_INDEXED;
                    if (isSequential(metaMem, i)) {
                        flags |= META_FLAG_BIT_SEQUENTIAL;
                    }
                    ddlMem.putLong(flags);
                    ddlMem.putInt(indexValueBlockSize);
                    ddlMem.skip(META_COLUMN_DATA_RESERVED);
                }
            }

            long nameOffset = getColumnNameOffset(columnCount);
            for (int i = 0; i < columnCount; i++) {
                CharSequence columnName = metaMem.getStr(nameOffset);
                ddlMem.putStr(columnName);
                nameOffset += VmUtils.getStorageLength(columnName);
            }
            return index;
        } finally {
            ddlMem.close();
        }
    }

    private int copyMetadataAndUpdateVersion() {
        int index;
        try {
            index = openMetaSwapFile(ff, ddlMem, path, rootLen, configuration.getMaxSwapFileCount());
            int columnCount = metaMem.getInt(META_OFFSET_COUNT);

            ddlMem.putInt(columnCount);
            ddlMem.putInt(metaMem.getInt(META_OFFSET_PARTITION_BY));
            ddlMem.putInt(metaMem.getInt(META_OFFSET_TIMESTAMP_INDEX));
            ddlMem.putInt(ColumnType.VERSION);
            ddlMem.putInt(metaMem.getInt(META_OFFSET_TABLE_ID));
            ddlMem.jumpTo(META_OFFSET_COLUMN_TYPES);
            for (int i = 0; i < columnCount; i++) {
                writeColumnEntry(i);
            }

            long nameOffset = getColumnNameOffset(columnCount);
            for (int i = 0; i < columnCount; i++) {
                CharSequence columnName = metaMem.getStr(nameOffset);
                ddlMem.putStr(columnName);
                nameOffset += VmUtils.getStorageLength(columnName);
            }
            return index;
        } finally {
            ddlMem.close();
        }
    }

    /**
     * Creates bitmap index files for a column. This method uses primary column instance as temporary tool to
     * append index data. Therefore it must be called before primary column is initialized.
     *
     * @param columnName              column name
     * @param indexValueBlockCapacity approximate number of values per index key
     * @param plen                    path length. This is used to trim shared path object to.
     */
    private void createIndexFiles(CharSequence columnName, int indexValueBlockCapacity, int plen, boolean force) {
        try {
            BitmapIndexUtils.keyFileName(path.trimTo(plen), columnName);

            if (!force && ff.exists(path)) {
                return;
            }

            // reuse memory column object to create index and close it at the end
            try {
                ddlMem.of(ff, path, ff.getPageSize());
                BitmapIndexWriter.initKeyMemory(ddlMem, indexValueBlockCapacity);
            } catch (CairoException e) {
                // looks like we could not create key file properly
                // lets not leave half baked file sitting around
                LOG.error()
                        .$("could not create index [name=").utf8(path)
                        .$(", errno=").$(e.getErrno())
                        .$(']').$();
                if (!ff.remove(path)) {
                    LOG.error()
                            .$("could not remove '").utf8(path).$("'. Please remove MANUALLY.")
                            .$("[errno=").$(ff.errno())
                            .$(']').$();
                }
                throw e;
            } finally {
                ddlMem.close();
            }
            if (!ff.touch(BitmapIndexUtils.valueFileName(path.trimTo(plen), columnName))) {
                LOG.error().$("could not create index [name=").$(path).$(']').$();
                throw CairoException.instance(ff.errno()).put("could not create index [name=").put(path).put(']');
            }
        } finally {
            path.trimTo(plen);
        }
    }

    private void createSymbolMapWriter(CharSequence name, int symbolCapacity, boolean symbolCacheFlag) {
        SymbolMapWriter.createSymbolMapFiles(ff, ddlMem, path, name, symbolCapacity, symbolCacheFlag);
        WriterTransientSymbolCountChangeHandler transientSymbolCountChangeHandler = new WriterTransientSymbolCountChangeHandler(denseSymbolMapWriters.size());
        denseSymbolTransientCountHandlers.add(transientSymbolCountChangeHandler);
        SymbolMapWriter w = new SymbolMapWriter(configuration, path, name, 0, transientSymbolCountChangeHandler);
        denseSymbolMapWriters.add(w);
        symbolMapWriters.extendAndSet(columnCount, w);
    }

    private void doClose(boolean truncate) {
        boolean tx = inTransaction();
        freeColumns(truncate);
        freeSymbolMapWriters();
        freeIndexers();
        Misc.free(txFile);
        Misc.free(blockWriter);
        Misc.free(metaMem);
        Misc.free(ddlMem);
        Misc.free(other);
        Misc.free(todoMem);
        try {
            releaseLock(!truncate | tx | performRecovery | distressed);
        } finally {
            Misc.free(path);
            freeTempMem();
            LOG.info().$("closed '").utf8(name).$('\'').$();
        }
    }

    private void freeColumns(boolean truncate) {
        // null check is because this method could be called from the constructor
        if (columns != null) {
            closeAppendMemoryNoTruncate(truncate);
        }
        Misc.freeObjListAndKeepObjects(oooColumns);
        Misc.freeObjListAndKeepObjects(oooColumns2);
    }

    private void freeIndexers() {
        if (indexers != null) {
            Misc.freeObjList(indexers);
            indexers.clear();
            denseIndexers.clear();
        }
    }

    private void freeSymbolMapWriters() {
        if (denseSymbolMapWriters != null) {
            for (int i = 0, n = denseSymbolMapWriters.size(); i < n; i++) {
                Misc.free(denseSymbolMapWriters.getQuick(i));
            }
            symbolMapWriters.clear();
        }

        if (symbolMapWriters != null) {
            symbolMapWriters.clear();
        }
    }

    private void freeTempMem() {
        if (tempMem16b != 0) {
            Unsafe.free(tempMem16b, 16);
            tempMem16b = 0;
        }
    }

    long getColumnTop(int columnIndex) {
        return columnTops.getQuick(columnIndex);
    }

    long getOooErrorCount() {
        return oooErrorCount.get();
    }

    private long getPartitionLo(long timestamp) {
        return timestampFloorMethod.floor(timestamp);
    }

    long getPrimaryAppendOffset(long timestamp, int columnIndex) {
        if (txFile.getAppendedPartitionCount() == 0) {
            openFirstPartition(timestamp);
        }

        if (timestamp > partitionHi) {
            return 0;
        }

        return columns.get(getPrimaryColumnIndex(columnIndex)).getAppendOffset();
    }

    private AppendOnlyVirtualMemory getPrimaryColumn(int column) {
        assert column < columnCount : "Column index is out of bounds: " + column + " >= " + columnCount;
        return columns.getQuick(getPrimaryColumnIndex(column));
    }

    long getSecondaryAppendOffset(long timestamp, int columnIndex) {
        if (txFile.getAppendedPartitionCount() == 0) {
            openFirstPartition(timestamp);
        }

        if (timestamp > partitionHi) {
            return 0;
        }

        return columns.get(getSecondaryColumnIndex(columnIndex)).getAppendOffset();
    }

    private AppendOnlyVirtualMemory getSecondaryColumn(int column) {
        assert column < columnCount : "Column index is out of bounds: " + column + " >= " + columnCount;
        return columns.getQuick(getSecondaryColumnIndex(column));
    }

    SymbolMapWriter getSymbolMapWriter(int columnIndex) {
        return symbolMapWriters.getQuick(columnIndex);
    }

    int getTxPartitionCount() {
        return txFile.getAppendedPartitionCount();
    }

    private long indexHistoricPartitions(SymbolColumnIndexer indexer, CharSequence columnName, int indexValueBlockSize) {
        final long maxTimestamp = timestampFloorMethod.floor(this.txFile.getMaxTimestamp());
        long timestamp = txFile.getMinTimestamp();

        try (indexer; final MappedReadOnlyMemory roMem = new SinglePageMappedReadOnlyPageMemory()) {

            while (timestamp < maxTimestamp) {

                path.trimTo(rootLen);

                setStateForTimestamp(path, timestamp, true);

                if (txFile.attachedPartitionsContains(timestamp) && ff.exists(path.$())) {

                    final int plen = path.length();

                    TableUtils.dFile(path.trimTo(plen), columnName);

                    if (ff.exists(path)) {

                        path.trimTo(plen);

                        LOG.info().$("indexing [path=").$(path).$(']').$();

                        createIndexFiles(columnName, indexValueBlockSize, plen, true);

                        final long partitionSize = txFile.getPartitionSizeByPartitionTimestamp(timestamp);
                        final long columnTop = TableUtils.readColumnTop(ff, path.trimTo(plen), columnName, plen, tempMem16b);

                        if (partitionSize > columnTop) {
                            TableUtils.dFile(path.trimTo(plen), columnName);

                            roMem.of(ff, path, ff.getPageSize(), 0);
                            roMem.grow((partitionSize - columnTop) << ColumnType.pow2SizeOf(ColumnType.INT));

                            indexer.configureWriter(configuration, path.trimTo(plen), columnName, columnTop);
                            indexer.index(roMem, columnTop, partitionSize);
                        }
                    }
                }
                timestamp = timestampAddMethod.calculate(timestamp, 1);
            }
        }
        return timestamp;
    }

    private void indexLastPartition(SymbolColumnIndexer indexer, CharSequence columnName, int columnIndex, int indexValueBlockSize) {
        final int plen = path.length();

        createIndexFiles(columnName, indexValueBlockSize, plen, true);

        final long columnTop = TableUtils.readColumnTop(ff, path.trimTo(plen), columnName, plen, tempMem16b);

        // set indexer up to continue functioning as normal
        indexer.configureFollowerAndWriter(configuration, path.trimTo(plen), columnName, getPrimaryColumn(columnIndex), columnTop);
        indexer.refreshSourceAndIndex(0, txFile.getTransientRowCount());
    }

    boolean isSymbolMapWriterCached(int columnIndex) {
        return symbolMapWriters.getQuick(columnIndex).isCached();
    }

    private void lock() {
        try {
            path.trimTo(rootLen);
            lockName(path);
            performRecovery = ff.exists(path);
            this.lockFd = TableUtils.lock(ff, path);
        } finally {
            path.trimTo(rootLen);
        }

        if (this.lockFd == -1L) {
            throw CairoException.instance(ff.errno()).put("Cannot lock table: ").put(path.$());
        }
    }

    private void mergeTimestampSetter(long timestamp) {
        timestampMergeMem.putLong(timestamp);
        timestampMergeMem.putLong(oooRowCount++);
    }

    private LongList oooComputePartitions(long mergedTimestamps, long indexMax, long ooTimestampMin, long ooTimestampHi) {
        // split out of order data into blocks for each partition
        final long hh = timestampCeilMethod.ceil(ooTimestampHi);

        // indexLo and indexHi formula is as follows:
        // indexLo = indexHigh[k-1] + 1;
        // indexHi = indexHigh[k]
        oooPartitions.clear();
        oooPartitions.add(-1);  // indexLo for first block
        oooPartitions.add(0);  // unused

        long lo = 0;
        long x1 = ooTimestampMin;
        while (lo < indexMax) {
            x1 = timestampCeilMethod.ceil(x1);
            final long hi = Vect.binarySearchIndexT(
                    mergedTimestamps,
                    timestampCeilMethod.ceil(x1),
                    lo,
                    indexMax - 1,
                    BinarySearch.SCAN_DOWN
            );
            oooPartitions.add(hi);
            oooPartitions.add(x1);
            lo = hi + 1;
            x1 = getTimestampIndexValue(mergedTimestamps, hi + 1);
        }

        oooPartitions.add(indexMax - 1);  // indexHi for last block
        oooPartitions.add(hh);  // partitionTimestampHi for last block

        return oooPartitions;
    }

    private void oooConsumeUpdPartitionSizeTasks(
            int workerId,
            long srcOooMax,
            long oooTimestampMin,
            long oooTimestampMax,
            long tableFloorOfMaxTimestamp
    ) {
        final MPSequence updSizePubSeq = messageBus.getOutOfOrderUpdPartitionSizePubSequence();
        final Sequence updSizeSubSeq = messageBus.getOutOfOrderUpdPartitionSizeSubSequence();
        final RingQueue<OutOfOrderUpdPartitionSizeTask> updSizeQueue = messageBus.getOutOfOrderUpdPartitionSizeQueue();
        final Sequence partitionSubSeq = messageBus.getOutOfOrderPartitionSubSeq();
        final RingQueue<OutOfOrderPartitionTask> partitionQueue = messageBus.getOutOfOrderPartitionQueue();
        final Sequence openColumnSubSeq = messageBus.getOutOfOrderOpenColumnSubSequence();
        final RingQueue<OutOfOrderOpenColumnTask> openColumnQueue = messageBus.getOutOfOrderOpenColumnQueue();
        final Sequence copyPubSeq = messageBus.getOutOfOrderCopyPubSequence();
        final Sequence copySubSeq = messageBus.getOutOfOrderCopySubSequence();
        final RingQueue<OutOfOrderCopyTask> copyQueue = messageBus.getOutOfOrderCopyQueue();

        do {
            long cursor = updSizeSubSeq.next();
            if (cursor > -1) {
                final OutOfOrderUpdPartitionSizeTask task = updSizeQueue.get(cursor);
                final long oooTimestampHi = task.getOooTimestampHi();
                final long srcOooPartitionLo = task.getSrcOooPartitionLo();
                final long srcOooPartitionHi = task.getSrcOooPartitionHi();
                final long srcDataMax = task.getSrcDataMax();
                final long dataTimestampHi = task.getDataTimestampHi();

                this.oooUpdRemaining.decrementAndGet();

                updSizeSubSeq.done(cursor);

                if (oooErrorCount.get() == 0) {
                    oooUpdatePartitionSize(
                            oooTimestampMin,
                            oooTimestampMax,
                            oooTimestampHi,
                            srcOooPartitionLo,
                            srcOooPartitionHi,
                            tableFloorOfMaxTimestamp,
                            dataTimestampHi,
                            srcOooMax,
                            srcDataMax
                    );
                }
            } else {
                cursor = partitionSubSeq.next();
                if (cursor > -1) {
                    final OutOfOrderPartitionTask partitionTask = partitionQueue.get(cursor);
                    if (oooErrorCount.get() > 0) {
                        // do we need to free anything on the task?
                        bumpPartitionUpdateCount();
                        partitionSubSeq.done(cursor);
                        partitionTask.getDoneLatch().countDown();
                    } else {
                        oooProcessPartitionSafe(
                                workerId,
                                updSizePubSeq,
                                updSizeQueue,
                                partitionSubSeq,
                                openColumnQueue,
                                copyPubSeq,
                                copyQueue,
                                cursor,
                                partitionTask
                        );
                    }
                } else {
                    cursor = openColumnSubSeq.next();
                    if (cursor > -1) {
                        OutOfOrderOpenColumnTask openColumnTask = openColumnQueue.get(cursor);
                        if (oooErrorCount.get() > 0) {
                            OutOfOrderCopyJob.closeColumnIdle(
                                    openColumnTask.getColumnCounter(),
                                    ff,
                                    openColumnTask.getTimestampMergeIndexAddr(),
                                    openColumnTask.getSrcTimestampFd(),
                                    openColumnTask.getSrcTimestampAddr(),
                                    openColumnTask.getSrcTimestampSize(),
                                    this,
                                    openColumnTask.getDoneLatch()
                            );
                            openColumnSubSeq.done(cursor);
                        } else {
                            oooOpenColumnSafe(
                                    workerId,
                                    updSizePubSeq,
                                    updSizeQueue,
                                    openColumnSubSeq,
                                    copyPubSeq,
                                    copyQueue,
                                    cursor,
                                    openColumnTask
                            );
                        }
                    } else {
                        cursor = copySubSeq.next();
                        if (cursor > -1) {
                            OutOfOrderCopyTask copyTask = copyQueue.get(cursor);
                            if (oooErrorCount.get() > 0) {
                                OutOfOrderCopyJob.copyIdle(
                                        copyTask.getColumnCounter(),
                                        copyTask.getPartCounter(),
                                        ff,
                                        copyTask.getTimestampMergeIndexAddr(),
                                        copyTask.getSrcDataFixFd(),
                                        copyTask.getSrcDataFixAddr(),
                                        copyTask.getSrcDataFixSize(),
                                        copyTask.getSrcDataVarFd(),
                                        copyTask.getSrcDataVarAddr(),
                                        copyTask.getSrcDataVarSize(),
                                        copyTask.getDstFixFd(),
                                        copyTask.getDstFixAddr(),
                                        copyTask.getDstFixSize(),
                                        copyTask.getDstVarFd(),
                                        copyTask.getDstVarAddr(),
                                        copyTask.getDstVarSize(),
                                        copyTask.getSrcTimestampFd(),
                                        copyTask.getSrcTimestampAddr(),
                                        copyTask.getSrcTimestampSize(),
                                        copyTask.getDstKFd(),
                                        copyTask.getDstVFd(),
                                        this,
                                        copyTask.getDoneLatch()
                                );
                                copySubSeq.done(cursor);
                            } else {
                                oooCopySafe(
                                        updSizePubSeq,
                                        updSizeQueue,
                                        copySubSeq,
                                        copyQueue,
                                        cursor
                                );
                            }
                        }
                    }
                }
            }
        } while (this.oooUpdRemaining.get() > 0);
    }

    private void oooCopySafe(
            MPSequence updSizePubSeq,
            RingQueue<OutOfOrderUpdPartitionSizeTask> updSizeQueue,
            Sequence copySubSeq,
            RingQueue<OutOfOrderCopyTask> copyQueue,
            long cursor
    ) {
        final OutOfOrderCopyTask task = copyQueue.get(cursor);
        try {
            OutOfOrderCopyJob.copy(
                    configuration,
                    updSizeQueue,
                    updSizePubSeq,
                    task,
                    cursor,
                    copySubSeq
            );
        } catch (CairoException | CairoError e) {
            LOG.error().$((Sinkable) e).$();
        } catch (Throwable e) {
            LOG.error().$(e).$();
        }
    }

    private void oooOpenColumnSafe(
            int workerId,
            MPSequence updSizePubSeq,
            RingQueue<OutOfOrderUpdPartitionSizeTask> updSizeQueue,
            Sequence openColumnSubSeq,
            Sequence copyPubSeq,
            RingQueue<OutOfOrderCopyTask> copyQueue,
            long cursor,
            OutOfOrderOpenColumnTask openColumnTask
    ) {
        try {
            OutOfOrderOpenColumnJob.openColumn(
                    workerId,
                    configuration,
                    copyQueue,
                    copyPubSeq,
                    updSizeQueue,
                    updSizePubSeq,
                    openColumnTask,
                    cursor,
                    openColumnSubSeq
            );
        } catch (CairoException | CairoError e) {
            LOG.error().$((Sinkable) e).$();
        } catch (Throwable e) {
            LOG.error().$(e).$();
        }
    }

    long getPartitionSizeByTimestamp(long ts) {
        return txFile.getPartitionSizeByPartitionTimestamp(ts);
    }

    private void oooProcess() {
        oooErrorCount.set(0);
        final int workerId;
        final Thread thread = Thread.currentThread();
        if (thread instanceof Worker) {
            workerId = ((Worker) thread).getWorkerId();
        } else {
            workerId = 0;
        }

        final int timestampIndex = metadata.getTimestampIndex();
        try {

            // we may need to re-use file descriptors when this partition is the "current" one
            // we cannot open file again due to sharing violation
            //
            // to determine that 'ooTimestampLo' goes into current partition
            // we need to compare 'partitionTimestampHi', which is appropriately truncated to DAY/MONTH/YEAR
            // to this.maxTimestamp, which isn't truncated yet. So we need to truncate it first
            LOG.info().$("sorting [name=").$(name).$(']').$();
            final long sortedTimestampsAddr = timestampMergeMem.addressOf(0);
            Vect.sortLongIndexAscInPlace(sortedTimestampsAddr, oooRowCount);
            // reshuffle all variable length columns
            // todo: writer must be aware of worker count
            if (this.messageBus == null) {
                oooSort(sortedTimestampsAddr, timestampIndex);
            } else {
                oooSortParallel(sortedTimestampsAddr, timestampIndex);
            }
            Vect.flattenIndex(sortedTimestampsAddr, oooRowCount);

            // we have three frames:
            // partition logical "lo" and "hi" - absolute bounds (partitionLo, partitionHi)
            // partition actual data "lo" and "hi" (dataLo, dataHi)
            // out of order "lo" and "hi" (indexLo, indexHi)

            final long srcOooMax = oooRowCount;
            final long oooTimestampMin = getTimestampIndexValue(sortedTimestampsAddr, 0);
            final long oooTimestampMax = getTimestampIndexValue(sortedTimestampsAddr, srcOooMax - 1);
            final long tableFloorOfMinTimestamp = timestampFloorMethod.floor(txFile.getMinTimestamp());
            final long tableFloorOfMaxTimestamp = timestampFloorMethod.floor(txFile.getMaxTimestamp());
            final long tableCeilOfMaxTimestamp = ceilMaxTimestamp();
            final long tableMaxTimestamp = txFile.getMaxTimestamp();
            final RingQueue<OutOfOrderPartitionTask> oooPartitionQueue = messageBus.getOutOfOrderPartitionQueue();
            final Sequence oooPartitionPubSeq = messageBus.getOutOfOrderPartitionPubSeq();

            // todo: we should not need to compile list and then process it, we can perhaps process data before
            //    it hits the list
            final LongList oooPartitions = oooComputePartitions(sortedTimestampsAddr, srcOooMax, oooTimestampMin, oooTimestampMax);
            final int affectedPartitionCount = oooPartitions.size() / 2 - 1;
            final int latchCount = affectedPartitionCount - 1;

            this.oooLatch.reset();
            this.oooUpdRemaining.set(affectedPartitionCount - 1);
            boolean success = true;
            int partitionsInFlight = affectedPartitionCount;
            try {
                for (int i = 1; i < affectedPartitionCount; i++) {
                    try {
                        final long srcOooLo = oooPartitions.getQuick(i * 2 - 2) + 1;
                        final long srcOooHi = oooPartitions.getQuick(i * 2);
                        final long oooTimestampHi = oooPartitions.getQuick(i * 2 + 1);
                        final long lastPartitionSize = transientRowCountBeforeOutOfOrder;
                        long cursor = oooPartitionPubSeq.next();
                        if (cursor > -1) {
                            OutOfOrderPartitionTask task = oooPartitionQueue.get(cursor);
                            task.of(
                                    ff,
                                    path,
                                    partitionBy,
                                    columns,
                                    oooColumns,
                                    srcOooLo,
                                    srcOooHi,
                                    srcOooMax,
                                    oooTimestampMin,
                                    oooTimestampMax,
                                    oooTimestampHi,
                                    getTxn(),
                                    sortedTimestampsAddr,
                                    lastPartitionSize,
                                    tableCeilOfMaxTimestamp,
                                    tableFloorOfMinTimestamp,
                                    tableFloorOfMaxTimestamp,
                                    tableMaxTimestamp,
                                    this,
                                    this.oooLatch
                            );
                            oooPartitionPubSeq.done(cursor);
                        } else {
                            OutOfOrderPartitionJob.processPartition(
                                    workerId,
                                    configuration,
                                    messageBus.getOutOfOrderOpenColumnQueue(),
                                    messageBus.getOutOfOrderOpenColumnPubSequence(),
                                    messageBus.getOutOfOrderCopyQueue(),
                                    messageBus.getOutOfOrderCopyPubSequence(),
                                    messageBus.getOutOfOrderUpdPartitionSizeQueue(),
                                    messageBus.getOutOfOrderUpdPartitionSizePubSequence(),
                                    ff,
                                    path,
                                    partitionBy,
                                    columns,
                                    oooColumns,
                                    srcOooLo,
                                    srcOooHi,
                                    srcOooMax,
                                    oooTimestampMin,
                                    oooTimestampMax,
                                    oooTimestampHi,
                                    getTxn(),
                                    sortedTimestampsAddr,
                                    lastPartitionSize,
                                    tableCeilOfMaxTimestamp,
                                    tableFloorOfMinTimestamp,
                                    tableFloorOfMaxTimestamp,
                                    tableMaxTimestamp,
                                    this,
                                    oooLatch
                            );
                        }
                    } catch (CairoException | CairoError e) {
                        LOG.error().$((Sinkable) e).$();
                        success = false;
                        partitionsInFlight = i + 1;
                        throw e;
                    } finally {
                        for (; partitionsInFlight < affectedPartitionCount; partitionsInFlight++) {
                            if (oooUpdRemaining.decrementAndGet() == 0) {
                                oooLatch.countDown();
                            }
                        }
                    }
                }
            } finally {
                // we are stealing work here it is possible we get exception from this method
                oooConsumeUpdPartitionSizeTasks(
                        workerId,
                        srcOooMax,
                        oooTimestampMin,
                        oooTimestampMax,
                        tableFloorOfMaxTimestamp
                );

                oooLatch.await(latchCount);

                if (success && oooErrorCount.get() > 0) {
                    //noinspection ThrowFromFinallyBlock
                    throw CairoException.instance(0).put("bulk update failed and will be rolled back");
                }
            }
            if (columns.getQuick(0).isClosed()) {
                openPartition(txFile.getMaxTimestamp());
            }
            setAppendPosition(txFile.getTransientRowCount(), true);
        } finally {
            if (denseIndexers.size() == 0) {
                populateDenseIndexerList();
            }
            path.trimTo(rootLen);
            this.oooRowCount = 0;
            // Alright, we finished updating partitions. Now we need to get this writer instance into
            // a consistent state.
            //
            // We start with ensuring append memory is in ready-to-use state. When max timestamp changes we need to
            // move append memory to new set of files. Otherwise we stay on the same set but advance the append position.
            avoidIndexOnCommit = true;
            rowFunction = switchPartitionFunction;
            row.activeColumns = columns;
            row.activeNullSetters = nullSetters;
            timestampSetter = prevTimestampSetter;
            transientRowCountBeforeOutOfOrder = 0;
        }
    }

    private void oooProcessPartitionSafe(
            int workerId,
            MPSequence updSizePubSeq,
            RingQueue<OutOfOrderUpdPartitionSizeTask> updSizeQueue,
            Sequence partitionSubSeq,
            RingQueue<OutOfOrderOpenColumnTask> openColumnQueue,
            Sequence copyPubSeq,
            RingQueue<OutOfOrderCopyTask> copyQueue,
            long cursor,
            OutOfOrderPartitionTask partitionTask
    ) {
        try {
            OutOfOrderPartitionJob.processPartition(
                    workerId,
                    configuration,
                    openColumnQueue,
                    messageBus.getOutOfOrderOpenColumnPubSequence(),
                    copyQueue,
                    copyPubSeq,
                    updSizeQueue,
                    updSizePubSeq,
                    partitionTask,
                    cursor,
                    partitionSubSeq
            );
        } catch (CairoException | CairoError e) {
            LOG.error().$((Sinkable) e).$();
        } catch (Throwable e) {
            LOG.error().$(e).$();
        }
    }

    private void oooSort(long mergedTimestamps, int timestampIndex) {
        for (int i = 0; i < columnCount; i++) {
            if (timestampIndex != i) {
                oooSortColumn(mergedTimestamps, i, metadata.getColumnType(i));
            }
        }
    }

    private void oooSortColumn(long mergedTimestamps, int i, int type) {
        switch (type) {
            case ColumnType.BINARY:
            case ColumnType.STRING:
                oooSortVarColumn(i, mergedTimestamps, oooRowCount, 0, null);
                break;
            case ColumnType.FLOAT:
            case ColumnType.INT:
            case ColumnType.SYMBOL:
                oooSortFixColumn(i, mergedTimestamps, oooRowCount, 2, SHUFFLE_32);
                break;
            case ColumnType.LONG:
            case ColumnType.DOUBLE:
            case ColumnType.DATE:
            case ColumnType.TIMESTAMP:
                oooSortFixColumn(i, mergedTimestamps, oooRowCount, 3, SHUFFLE_64);
                break;
            case ColumnType.SHORT:
            case ColumnType.CHAR:
                oooSortFixColumn(i, mergedTimestamps, oooRowCount, 1, SHUFFLE_16);
                break;
            default:
                oooSortFixColumn(i, mergedTimestamps, oooRowCount, 0, SHUFFLE_8);
                break;
        }
    }

    private void oooSortFixColumn(
            int columnIndex,
            long mergedTimestampsAddr,
            long valueCount,
            final int shl,
            final OutOfOrderNativeSortMethod shuffleFunc
    ) {
        final int columnOffset = getPrimaryColumnIndex(columnIndex);
        final ContiguousVirtualMemory mem = oooColumns.getQuick(columnOffset);
        final ContiguousVirtualMemory mem2 = oooColumns2.getQuick(columnOffset);
        final long src = mem.addressOf(0);
        final long srcSize = mem.size();
        final long tgtDataAddr = mem2.resize(valueCount << shl);
        final long tgtDataSize = mem2.size();
        shuffleFunc.shuffle(src, tgtDataAddr, mergedTimestampsAddr, valueCount);
        mem.replacePage(tgtDataAddr, tgtDataSize);
        mem2.replacePage(src, srcSize);
    }

    private void oooSortParallel(long mergedTimestamps, int timestampIndex) {
        oooPendingSortTasks.clear();

        final Sequence pubSeq = this.messageBus.getOutOfOrderSortPubSeq();
        final RingQueue<OutOfOrderSortTask> queue = this.messageBus.getOutOfOrderSortQueue();

        oooLatch.reset();
        int queuedCount = 0;
        for (int i = 0; i < columnCount; i++) {
            if (timestampIndex != i) {
                final int type = metadata.getColumnType(i);
                long cursor = pubSeq.next();
                if (cursor > -1) {
                    try {
                        final OutOfOrderSortTask task = queue.get(cursor);
                        switch (type) {
                            case ColumnType.BINARY:
                            case ColumnType.STRING:
                                task.of(
                                        oooLatch,
                                        i,
                                        0,
                                        mergedTimestamps,
                                        oooRowCount,
                                        null,
                                        oooSortVarColumnRef
                                );
                                break;
                            case ColumnType.FLOAT:
                            case ColumnType.INT:
                            case ColumnType.SYMBOL:
                                task.of(
                                        oooLatch,
                                        i,
                                        2,
                                        mergedTimestamps,
                                        oooRowCount,
                                        SHUFFLE_32,
                                        oooSortFixColumnRef
                                );
                                break;
                            case ColumnType.LONG:
                            case ColumnType.DOUBLE:
                            case ColumnType.DATE:
                            case ColumnType.TIMESTAMP:
                                task.of(
                                        oooLatch,
                                        i,
                                        3,
                                        mergedTimestamps,
                                        oooRowCount,
                                        SHUFFLE_64,
                                        oooSortFixColumnRef
                                );
                                break;
                            case ColumnType.SHORT:
                            case ColumnType.CHAR:
                                task.of(
                                        oooLatch,
                                        i,
                                        1,
                                        mergedTimestamps,
                                        oooRowCount,
                                        SHUFFLE_16,
                                        oooSortFixColumnRef
                                );
                                break;
                            default:
                                task.of(
                                        oooLatch,
                                        i,
                                        0,
                                        mergedTimestamps,
                                        oooRowCount,
                                        SHUFFLE_8,
                                        oooSortFixColumnRef
                                );
                                break;
                        }
                        oooPendingSortTasks.add(task);
                    } finally {
                        queuedCount++;
                        pubSeq.done(cursor);
                    }
                } else {
                    oooSortColumn(mergedTimestamps, i, type);
                }
            }
        }

        for (int n = oooPendingSortTasks.size() - 1; n > -1; n--) {
            final OutOfOrderSortTask task = oooPendingSortTasks.getQuick(n);
            if (task.tryLock()) {
                OutOfOrderSortJob.doSort(
                        task,
                        -1,
                        null
                );
            }
        }

        oooLatch.await(queuedCount);
    }

    private void oooSortVarColumn(
            int columnIndex,
            long mergedTimestampsAddr,
            long valueCount,
            int shl,
            OutOfOrderNativeSortMethod nativeSortMethod
    ) {
        final int primaryIndex = getPrimaryColumnIndex(columnIndex);
        final int secondaryIndex = primaryIndex + 1;
        final ContiguousVirtualMemory dataMem = oooColumns.getQuick(primaryIndex);
        final ContiguousVirtualMemory indexMem = oooColumns.getQuick(secondaryIndex);
        final ContiguousVirtualMemory dataMem2 = oooColumns2.getQuick(primaryIndex);
        final ContiguousVirtualMemory indexMem2 = oooColumns2.getQuick(secondaryIndex);
        final long dataSize = dataMem.getAppendOffset();
        // ensure we have enough memory allocated
        final long srcDataAddr = dataMem.addressOf(0);
        final long srcDataSize = dataMem.size();
        final long srcIndxAddr = indexMem.addressOf(0);
        final long srcIndxSize = indexMem.size();
        final long tgtDataAddr = dataMem2.resize(dataSize);
        final long tgtDataSize = dataMem2.size();
        final long tgtIndxAddr = indexMem2.resize(valueCount * Long.BYTES);
        final long tgtIndxSize = indexMem2.size();
        // add max offset so that we do not have conditionals inside loop
        indexMem.putLong(valueCount * Long.BYTES, dataSize);
        long offset = 0;
        for (long l = 0; l < valueCount; l++) {
            final long row = getTimestampIndexRow(mergedTimestampsAddr, l);
            final long o1 = Unsafe.getUnsafe().getLong(srcIndxAddr + row * Long.BYTES);
            final long o2 = Unsafe.getUnsafe().getLong(srcIndxAddr + row * Long.BYTES + Long.BYTES);
            final long len = o2 - o1;
            Unsafe.getUnsafe().copyMemory(srcDataAddr + o1, tgtDataAddr + offset, len);
            Unsafe.getUnsafe().putLong(tgtIndxAddr + l * Long.BYTES, offset);
            offset += len;
        }
        dataMem.replacePage(tgtDataAddr, tgtDataSize);
        indexMem.replacePage(tgtIndxAddr, tgtIndxSize);
        dataMem2.replacePage(srcDataAddr, srcDataSize);
        indexMem2.replacePage(srcIndxAddr, srcIndxSize);
        dataMem.jumpTo(offset);
        indexMem.jumpTo(valueCount * Long.BYTES);
    }

    private void oooUpdatePartitionSize(
            long oooTimestampMin,
            long oooTimestampMax,
            long oooTimestampHi,
            long srcOooPartitionLo,
            long srcOooPartitionHi,
            long tableFloorOfMaxTimestamp,
            long dataTimestampHi,
            long srcOooMax,
            long srcDataMax
    ) {
        final long partitionSize = srcDataMax + srcOooPartitionHi - srcOooPartitionLo + 1;
        if (srcOooPartitionHi + 1 < srcOooMax || oooTimestampHi < tableFloorOfMaxTimestamp) {

            updatePartitionSize(
                    oooTimestampHi,
                    partitionSize,
                    tableFloorOfMaxTimestamp,
                    srcDataMax
            );

            if (srcOooPartitionHi + 1 >= srcOooMax) {
                // no more out of order data and we just pre-pended data to existing
                // partitions
                this.txFile.minTimestamp = Math.min(oooTimestampMin, this.txFile.minTimestamp);
                // when we exit here we need to rollback transientRowCount we've been incrementing
                // while adding out-of-order data
                this.txFile.transientRowCount = this.transientRowCountBeforeOutOfOrder;
            }
        } else {
            updateActivePartitionDetails(
                    oooTimestampMin,
                    Math.max(dataTimestampHi, oooTimestampMax),
                    partitionSize
            );
        }
    }

    synchronized void oooUpdatePartitionSizeSynchronized(
            long oooTimestampMin,
            long oooTimestampMax,
            long oooTimestampHi,
            long srcOooPartitionLo,
            long srcOooPartitionHi,
            long tableFloorOfMaxTimestamp,
            long dataTimestampHi,
            long srcOooMax,
            long srcDataMax
    ) {
        bumpPartitionUpdateCount();

        oooUpdatePartitionSize(
                oooTimestampMin,
                oooTimestampMax,
                oooTimestampHi,
                srcOooPartitionLo,
                srcOooPartitionHi,
                tableFloorOfMaxTimestamp,
                dataTimestampHi,
                srcOooMax,
                srcDataMax
        );
    }

    private void openColumnFiles(CharSequence name, int i, int plen) {
        AppendOnlyVirtualMemory mem1 = getPrimaryColumn(i);
        AppendOnlyVirtualMemory mem2 = getSecondaryColumn(i);

        mem1.of(ff, dFile(path.trimTo(plen), name), configuration.getAppendPageSize());

        if (mem2 != null) {
            mem2.of(ff, iFile(path.trimTo(plen), name), configuration.getAppendPageSize());
        }

        path.trimTo(plen);
    }

    private void openFirstPartition(long timestamp) {
        openPartition(repairDataGaps(timestamp));
        setAppendPosition(txFile.getTransientRowCount(), true);
        if (performRecovery) {
            performRecovery();
        }
        txFile.openFirstPartition();
    }

    private void openMergePartition() {
        for (int i = 0; i < columnCount; i++) {
            ContiguousVirtualMemory mem1 = oooColumns.getQuick(getPrimaryColumnIndex(i));
            mem1.jumpTo(0);
            ContiguousVirtualMemory mem2 = oooColumns.getQuick(getSecondaryColumnIndex(i));
            if (mem2 != null) {
                mem2.jumpTo(0);
            }
        }
        row.activeColumns = oooColumns;
        row.activeNullSetters = oooNullSetters;
        LOG.info().$("switched partition to memory").$();
    }

    private void openNewColumnFiles(CharSequence name, boolean indexFlag, int indexValueBlockCapacity) {
        try {
            // open column files
            setStateForTimestamp(path, txFile.getMaxTimestamp(), false);
            final int plen = path.length();
            final int columnIndex = columnCount - 1;

            // index must be created before column is initialised because
            // it uses primary column object as temporary tool
            if (indexFlag) {
                createIndexFiles(name, indexValueBlockCapacity, plen, true);
            }

            openColumnFiles(name, columnIndex, plen);
            if (txFile.getTransientRowCount() > 0) {
                // write .top file
                writeColumnTop(name);
            }

            if (indexFlag) {
                ColumnIndexer indexer = indexers.getQuick(columnIndex);
                assert indexer != null;
                indexers.getQuick(columnIndex).configureFollowerAndWriter(configuration, path.trimTo(plen), name, getPrimaryColumn(columnIndex), txFile.getTransientRowCount());
            }

        } finally {
            path.trimTo(rootLen);
        }
    }

    private void openPartition(long timestamp) {
        try {
            setStateForTimestamp(path, timestamp, true);
            int plen = path.length();
            if (ff.mkdirs(path.put(Files.SEPARATOR).$(), mkDirMode) != 0) {
                throw CairoException.instance(ff.errno()).put("Cannot create directory: ").put(path);
            }

            assert columnCount > 0;

            for (int i = 0; i < columnCount; i++) {
                final CharSequence name = metadata.getColumnName(i);
                final boolean indexed = metadata.isColumnIndexed(i);
                final long columnTop;

                // prepare index writer if column requires indexing
                if (indexed) {
                    // we have to create files before columns are open
                    // because we are reusing AppendOnlyVirtualMemory object from columns list
                    createIndexFiles(name, metadata.getIndexValueBlockCapacity(i), plen, txFile.getTransientRowCount() < 1);
                }

                openColumnFiles(name, i, plen);
                columnTop = readColumnTop(ff, path, name, plen, tempMem16b);
                columnTops.extendAndSet(i, columnTop);

                if (indexed) {
                    ColumnIndexer indexer = indexers.getQuick(i);
                    assert indexer != null;
                    indexer.configureFollowerAndWriter(configuration, path, name, getPrimaryColumn(i), columnTop);
                }
            }

            LOG.info().$("switched partition to '").$(path).$('\'').$();
        } finally {
            path.trimTo(rootLen);
        }
    }

    private long openTodoMem() {
        path.concat(TODO_FILE_NAME).$();
        try {
            if (ff.exists(path)) {
                long fileLen = ff.length(path);
                if (fileLen < 32) {
                    throw CairoException.instance(0).put("corrupt ").put(path);
                }

                todoMem.of(ff, path, ff.getPageSize(), fileLen);
                this.todoTxn = todoMem.getLong(0);
                // check if _todo_ file is consistent, if not, we just ignore its contents and reset hash
                if (todoMem.getLong(24) != todoTxn) {
                    todoMem.putLong(8, configuration.getInstanceHashLo());
                    todoMem.putLong(16, configuration.getInstanceHashHi());
                    Unsafe.getUnsafe().storeFence();
                    todoMem.putLong(24, todoTxn);
                    return 0;
                }

                return todoMem.getLong(32);
            } else {
                TableUtils.resetTodoLog(ff, path, rootLen, todoMem);
                todoTxn = 0;
                return 0;
            }
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void performRecovery() {
        rollbackIndexes();
        rollbackSymbolTables();
        performRecovery = false;
    }

    private void populateDenseIndexerList() {
        denseIndexers.clear();
        for (int i = 0, n = indexers.size(); i < n; i++) {
            ColumnIndexer indexer = indexers.getQuick(i);
            if (indexer != null) {
                denseIndexers.add(indexer);
            }
        }
        indexCount = denseIndexers.size();
    }

    void purgeUnusedPartitions() {
        if (partitionBy != PartitionBy.NONE) {
            removePartitionDirsNewerThan(txFile.getMaxTimestamp());
            removePartitionDirsCreatedByOOO();
        }
    }

    private long readMinTimestamp(long partitionTimestamp) {
        setStateForTimestamp(other, partitionTimestamp, false);
        try {
            dFile(other, metadata.getColumnName(metadata.getTimestampIndex()));
            if (ff.exists(other)) {
                // read min timestamp value
                long fd = ff.openRO(other);
                if (fd == -1) {
                    // oops
                    throw CairoException.instance(Os.errno()).put("could not open [file=").put(other).put(']');
                }
                try {
                    long n = ff.read(fd, tempMem16b, Long.BYTES, 0);
                    if (n != Long.BYTES) {
                        throw CairoException.instance(Os.errno()).put("could not read timestamp value");
                    }
                    return Unsafe.getUnsafe().getLong(tempMem16b);
                } finally {
                    ff.close(fd);
                }
            } else {
                throw CairoException.instance(0).put("Partition does not exist [path=").put(other).put(']');
            }
        } finally {
            other.trimTo(rootLen);
        }
    }

    private void recoverFromMetaRenameFailure(CharSequence columnName) {
        openMetaFile(ff, path, rootLen, metaMem);
    }

    private void recoverFromSwapRenameFailure(CharSequence columnName) {
        recoverFrommTodoWriteFailure(columnName);
        clearTodoLog();
    }

    private void recoverFromSymbolMapWriterFailure(CharSequence columnName) {
        removeSymbolMapFilesQuiet(columnName);
        removeMetaFile();
        recoverFromSwapRenameFailure(columnName);
    }

    private void recoverFrommTodoWriteFailure(CharSequence columnName) {
        restoreMetaFrom(META_PREV_FILE_NAME, metaPrevIndex);
        openMetaFile(ff, path, rootLen, metaMem);
    }

    private void recoverOpenColumnFailure(CharSequence columnName) {
        final int index = columnCount - 1;
        removeMetaFile();
        removeLastColumn();
        recoverFromSwapRenameFailure(columnName);
        removeSymbolMapWriter(index);
    }

    private void releaseLock(boolean distressed) {
        if (lockFd != -1L) {
            ff.close(lockFd);
            if (distressed) {
                return;
            }

            try {
                lockName(path);
                removeOrException(ff, path);
            } finally {
                path.trimTo(rootLen);
            }
        }
    }

    private void removeColumn(int columnIndex) {
        final int pi = getPrimaryColumnIndex(columnIndex);
        final int si = getSecondaryColumnIndex(columnIndex);
        Misc.free(columns.getQuick(pi));
        Misc.free(columns.getQuick(si));
        columns.remove(pi);
        columns.remove(pi);
        Misc.free(oooColumns.getQuick(pi));
        Misc.free(oooColumns.getQuick(si));
        oooColumns.remove(pi);
        oooColumns.remove(pi);

        columnTops.removeIndex(columnIndex);
        nullSetters.remove(columnIndex);
        oooNullSetters.remove(columnIndex);
        if (columnIndex < indexers.size()) {
            Misc.free(indexers.getQuick(columnIndex));
            indexers.remove(columnIndex);
            populateDenseIndexerList();
        }
    }

    private void removeColumnFiles(CharSequence columnName, int columnType, RemoveFileLambda removeLambda) {
        try {
            ff.iterateDir(path.$(), (file, type) -> {
                nativeLPSZ.of(file);
                if (type == Files.DT_DIR && IGNORED_FILES.excludes(nativeLPSZ)) {
                    path.trimTo(rootLen);
                    path.concat(nativeLPSZ);
                    int plen = path.length();
                    removeLambda.remove(ff, dFile(path, columnName));
                    removeLambda.remove(ff, iFile(path.trimTo(plen), columnName));
                    removeLambda.remove(ff, topFile(path.trimTo(plen), columnName));
                    removeLambda.remove(ff, BitmapIndexUtils.keyFileName(path.trimTo(plen), columnName));
                    removeLambda.remove(ff, BitmapIndexUtils.valueFileName(path.trimTo(plen), columnName));
                }
            });

            if (columnType == ColumnType.SYMBOL) {
                removeLambda.remove(ff, SymbolMapWriter.offsetFileName(path.trimTo(rootLen), columnName));
                removeLambda.remove(ff, SymbolMapWriter.charFileName(path.trimTo(rootLen), columnName));
                removeLambda.remove(ff, BitmapIndexUtils.keyFileName(path.trimTo(rootLen), columnName));
                removeLambda.remove(ff, BitmapIndexUtils.valueFileName(path.trimTo(rootLen), columnName));
            }
        } finally {
            path.trimTo(rootLen);
        }
    }

    private int removeColumnFromMeta(int index) {
        try {
            int metaSwapIndex = openMetaSwapFile(ff, ddlMem, path, rootLen, fileOperationRetryCount);
            int timestampIndex = metaMem.getInt(META_OFFSET_TIMESTAMP_INDEX);
            ddlMem.putInt(columnCount - 1);
            ddlMem.putInt(partitionBy);

            if (timestampIndex == index) {
                ddlMem.putInt(-1);
            } else if (index < timestampIndex) {
                ddlMem.putInt(timestampIndex - 1);
            } else {
                ddlMem.putInt(timestampIndex);
            }
            ddlMem.putInt(ColumnType.VERSION);
            ddlMem.putInt(metaMem.getInt(META_OFFSET_TABLE_ID));
            ddlMem.jumpTo(META_OFFSET_COLUMN_TYPES);

            for (int i = 0; i < columnCount; i++) {
                if (i != index) {
                    writeColumnEntry(i);
                }
            }

            long nameOffset = getColumnNameOffset(columnCount);
            for (int i = 0; i < columnCount; i++) {
                CharSequence columnName = metaMem.getStr(nameOffset);
                if (i != index) {
                    ddlMem.putStr(columnName);
                }
                nameOffset += VmUtils.getStorageLength(columnName);
            }

            return metaSwapIndex;
        } finally {
            ddlMem.close();
        }
    }

    private void removeIndexFiles(CharSequence columnName) {
        try {
            ff.iterateDir(path.$(), (file, type) -> {
                nativeLPSZ.of(file);
                if (type == Files.DT_DIR && IGNORED_FILES.excludes(nativeLPSZ)) {
                    path.trimTo(rootLen);
                    path.concat(nativeLPSZ);
                    int plen = path.length();
                    removeFileAndOrLog(ff, BitmapIndexUtils.keyFileName(path.trimTo(plen), columnName));
                    removeFileAndOrLog(ff, BitmapIndexUtils.valueFileName(path.trimTo(plen), columnName));
                }
            });
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void removeLastColumn() {
        removeColumn(columnCount - 1);
        columnCount--;
    }

    private void removeMetaFile() {
        try {
            path.concat(META_FILE_NAME).$();
            if (ff.exists(path) && !ff.remove(path)) {
                throw CairoException.instance(ff.errno()).put("Recovery failed. Cannot remove: ").put(path);
            }
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void removePartitionDirectories() {
        try {
            ff.iterateDir(path.$(), removePartitionDirectories);
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void removePartitionDirectories0(long name, int type) {
        path.trimTo(rootLen);
        path.concat(name).$();
        nativeLPSZ.of(name);
        if (IGNORED_FILES.excludes(nativeLPSZ) && type == Files.DT_DIR && !ff.rmdir(path)) {
            LOG.info().$("could not remove [path=").$(path).$(", errno=").$(ff.errno()).$(']').$();
        }
    }

    private void removePartitionDirsCreatedByOOO() {
        LOG.debug().$("purging OOO artefacts [path=").$(path.$()).$(']').$();
        try {
            ff.iterateDir(path.$(), removePartitionDirsCreatedByOOOVisitor);
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void removePartitionDirsCreatedByOOO0(long pName, int type) {
        path.trimTo(rootLen);
        path.concat(pName).$();
        nativeLPSZ.of(pName);
        if (!isDots(nativeLPSZ) && type == Files.DT_DIR) {
            if (Chars.contains(nativeLPSZ, "-n-")) {
                if (ff.rmdir(path)) {
                    LOG.info().$("removing partition dir: ").$(path).$();
                } else {
                    LOG.error().$("cannot remove: ").$(path).$(" [errno=").$(ff.errno()).$(']').$();
                }
            }
        }
    }

    private void removePartitionDirsNewerThan(long timestamp) {
        if (timestamp > Long.MIN_VALUE) {
            LOG.info().$("purging [newerThen=").$ts(timestamp).$(", path=").$(path.$()).$(']').$();
        } else {
            LOG.debug().$("cleaning [path=").$(path.$()).$(']').$();
        }
        this.removePartitionDirsNewerThanTimestamp = timestamp;
        try {
            ff.iterateDir(path.$(), removePartitionDirsNewerThanVisitor);
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void removePartitionDirsNewerThanTimestamp(long pName, int type) {
        path.trimTo(rootLen);
        path.concat(pName).$();
        nativeLPSZ.of(pName);
        if (!isDots(nativeLPSZ) && type == Files.DT_DIR) {
            try {
                long dirTimestamp = partitionDirFmt.parse(nativeLPSZ, null);
                if (dirTimestamp <= removePartitionDirsNewerThanTimestamp) {
                    return;
                }
            } catch (NumericException ignore) {
                // not a date?
                // ignore exception and remove directory
                // we rely on this behaviour to remove leftover directories created by OOO processing
            }
            // Do not remove, try to rename instead
            try {
                other.concat(pName);
                other.put(DETACHED_DIR_MARKER);
                other.$();

                if (ff.rename(path, other)) {
                    LOG.info().$("moved partition dir: ").$(path).$(" to ").$(other).$();
                } else {
                    LOG.error().$("cannot rename: ").$(path).$(" [errno=").$(ff.errno()).$(']').$();
                }
            } finally {
                other.trimTo(rootLen);
            }
        }
    }

    private void removeSymbolMapFilesQuiet(CharSequence name) {
        try {
            removeFileAndOrLog(ff, SymbolMapWriter.offsetFileName(path.trimTo(rootLen), name));
            removeFileAndOrLog(ff, SymbolMapWriter.charFileName(path.trimTo(rootLen), name));
            removeFileAndOrLog(ff, BitmapIndexUtils.keyFileName(path.trimTo(rootLen), name));
            removeFileAndOrLog(ff, BitmapIndexUtils.valueFileName(path.trimTo(rootLen), name));
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void removeSymbolMapWriter(int index) {
        SymbolMapWriter writer = symbolMapWriters.getQuick(index);
        symbolMapWriters.remove(index);
        if (writer != null) {
            int symColIndex = denseSymbolMapWriters.remove(writer);
            denseSymbolTransientCountHandlers.remove(symColIndex);
            // Shift all subsequent symbol indexes by 1 back
            while (symColIndex < denseSymbolTransientCountHandlers.size()) {
                WriterTransientSymbolCountChangeHandler transientCountHandler = denseSymbolTransientCountHandlers.getQuick(symColIndex);
                assert transientCountHandler.symColIndex - 1 == symColIndex;
                transientCountHandler.symColIndex = symColIndex;
                symColIndex++;
            }
            Misc.free(writer);
        }
    }

    private int rename(int retries) {
        try {
            int index = 0;
            other.concat(META_PREV_FILE_NAME).$();
            path.concat(META_FILE_NAME).$();
            int l = other.length();

            do {
                if (index > 0) {
                    other.trimTo(l);
                    other.put('.').put(index);
                    other.$();
                }

                if (ff.exists(other) && !ff.remove(other)) {
                    LOG.info().$("cannot remove target of rename '").$(path).$("' to '").$(other).$(" [errno=").$(ff.errno()).$(']').$();
                    index++;
                    continue;
                }

                if (!ff.rename(path, other)) {
                    LOG.info().$("cannot rename '").$(path).$("' to '").$(other).$(" [errno=").$(ff.errno()).$(']').$();
                    index++;
                    continue;
                }

                return index;

            } while (index < retries);

            throw CairoException.instance(0).put("Cannot rename ").put(path).put(". Max number of attempts reached [").put(index).put("]. Last target was: ").put(other);
        } finally {
            path.trimTo(rootLen);
            other.trimTo(rootLen);
        }
    }

    private void renameColumnFiles(CharSequence columnName, CharSequence newName, int columnType) {
        try {
            ff.iterateDir(path.$(), (file, type) -> {
                nativeLPSZ.of(file);
                if (type == Files.DT_DIR && IGNORED_FILES.excludes(nativeLPSZ)) {
                    path.trimTo(rootLen);
                    path.concat(nativeLPSZ);
                    other.trimTo(rootLen);
                    other.concat(nativeLPSZ);
                    int plen = path.length();
                    renameFileOrLog(ff, dFile(path.trimTo(plen), columnName), dFile(other.trimTo(plen), newName));
                    renameFileOrLog(ff, iFile(path.trimTo(plen), columnName), iFile(other.trimTo(plen), newName));
                    renameFileOrLog(ff, topFile(path.trimTo(plen), columnName), topFile(other.trimTo(plen), newName));
                    renameFileOrLog(ff, BitmapIndexUtils.keyFileName(path.trimTo(plen), columnName), BitmapIndexUtils.keyFileName(other.trimTo(plen), newName));
                    renameFileOrLog(ff, BitmapIndexUtils.valueFileName(path.trimTo(plen), columnName), BitmapIndexUtils.valueFileName(other.trimTo(plen), newName));
                }
            });

            if (columnType == ColumnType.SYMBOL) {
                renameFileOrLog(ff, SymbolMapWriter.offsetFileName(path.trimTo(rootLen), columnName), SymbolMapWriter.offsetFileName(other.trimTo(rootLen), newName));
                renameFileOrLog(ff, SymbolMapWriter.charFileName(path.trimTo(rootLen), columnName), SymbolMapWriter.charFileName(other.trimTo(rootLen), newName));
                renameFileOrLog(ff, BitmapIndexUtils.keyFileName(path.trimTo(rootLen), columnName), BitmapIndexUtils.keyFileName(other.trimTo(rootLen), newName));
                renameFileOrLog(ff, BitmapIndexUtils.valueFileName(path.trimTo(rootLen), columnName), BitmapIndexUtils.valueFileName(other.trimTo(rootLen), newName));
            }
        } finally {
            path.trimTo(rootLen);
            other.trimTo(rootLen);
        }
    }

    private int renameColumnFromMeta(int index, CharSequence newName) {
        try {
            int metaSwapIndex = openMetaSwapFile(ff, ddlMem, path, rootLen, fileOperationRetryCount);
            int timestampIndex = metaMem.getInt(META_OFFSET_TIMESTAMP_INDEX);
            ddlMem.putInt(columnCount);
            ddlMem.putInt(partitionBy);
            ddlMem.putInt(timestampIndex);
            ddlMem.putInt(ColumnType.VERSION);
            ddlMem.putInt(metaMem.getInt(META_OFFSET_TABLE_ID));
            ddlMem.jumpTo(META_OFFSET_COLUMN_TYPES);

            for (int i = 0; i < columnCount; i++) {
                writeColumnEntry(i);
            }

            long nameOffset = getColumnNameOffset(columnCount);
            for (int i = 0; i < columnCount; i++) {
                CharSequence columnName = metaMem.getStr(nameOffset);
                nameOffset += VmUtils.getStorageLength(columnName);

                if (i == index) {
                    columnName = newName;
                }
                ddlMem.putStr(columnName);
            }

            return metaSwapIndex;
        } finally {
            ddlMem.close();
        }
    }

    private void renameMetaToMetaPrev(CharSequence columnName) {
        try {
            this.metaPrevIndex = rename(fileOperationRetryCount);
        } catch (CairoException e) {
            runFragile(RECOVER_FROM_META_RENAME_FAILURE, columnName, e);
        }
    }

    private void renameSwapMetaToMeta(CharSequence columnName) {
        // rename _meta.swp to _meta
        try {
            restoreMetaFrom(META_SWAP_FILE_NAME, metaSwapIndex);
        } catch (CairoException e) {
            runFragile(RECOVER_FROM_SWAP_RENAME_FAILURE, columnName, e);
        }
    }

    private long repairDataGaps(long timestamp) {
        if (txFile.getMaxTimestamp() != Numbers.LONG_NaN && partitionBy != PartitionBy.NONE) {
            long actualSize = 0;
            long lastTimestamp = -1;
            long transientRowCount = this.txFile.getTransientRowCount();
            long maxTimestamp = this.txFile.getMaxTimestamp();
            try {
                final long tsLimit = timestampFloorMethod.floor(this.txFile.getMaxTimestamp());
                for (long ts = getPartitionLo(txFile.getMinTimestamp()); ts < tsLimit; ts = timestampAddMethod.calculate(ts, 1)) {
                    path.trimTo(rootLen);
                    setStateForTimestamp(path, ts, false);
                    int p = path.length();

                    long partitionSize = txFile.getPartitionSizeByPartitionTimestamp(ts);
                    if (partitionSize >= 0 && ff.exists(path.$())) {
                        actualSize += partitionSize;
                        lastTimestamp = ts;
                    } else {
                        Path other = Path.getThreadLocal2(path.trimTo(p).$());
                        TableUtils.oldPartitionName(other, getTxn());
                        if (ff.exists(other.$())) {
                            if (!ff.rename(other, path)) {
                                LOG.error().$("could not rename [from=").$(other).$(", to=").$(path).$(']').$();
                                throw new CairoError("could not restore directory, see log for details");
                            } else {
                                LOG.info().$("restored [path=").$(path).$(']').$();
                            }
                        } else {
                            LOG.info().$("missing partition [name=").$(path.trimTo(p).$()).$(']').$();
                        }
                    }
                }

                if (lastTimestamp > -1) {
                    path.trimTo(rootLen);
                    setStateForTimestamp(path, tsLimit, false);
                    if (!ff.exists(path.$())) {
                        Path other = Path.getThreadLocal2(path);
                        TableUtils.oldPartitionName(other, getTxn());
                        if (ff.exists(other.$())) {
                            if (!ff.rename(other, path)) {
                                LOG.error().$("could not rename [from=").$(other).$(", to=").$(path).$(']').$();
                                throw new CairoError("could not restore directory, see log for details");
                            } else {
                                LOG.info().$("restored [path=").$(path).$(']').$();
                            }
                        } else {
                            LOG.error().$("last partition does not exist [name=").$(path).$(']').$();
                            // ok, create last partition we discovered the active
                            // 1. read its size
                            path.trimTo(rootLen);
                            setStateForTimestamp(path, lastTimestamp, false);
                            int p = path.length();
                            transientRowCount = txFile.getPartitionSizeByPartitionTimestamp(lastTimestamp);

                            // 2. read max timestamp
                            TableUtils.dFile(path.trimTo(p), metadata.getColumnName(metadata.getTimestampIndex()));
                            maxTimestamp = TableUtils.readLongAtOffset(ff, path, tempMem16b, (transientRowCount - 1) * Long.BYTES);
                            actualSize -= transientRowCount;
                            LOG.info()
                                    .$("updated active partition [name=").$(path.trimTo(p).$())
                                    .$(", maxTimestamp=").$ts(maxTimestamp)
                                    .$(", transientRowCount=").$(transientRowCount)
                                    .$(", fixedRowCount=").$(txFile.getFixedRowCount())
                                    .$(']').$();
                        }
                    }
                }
            } finally {
                path.trimTo(rootLen);
            }

            final long expectedSize = txFile.readFixedRowCount();
            if (expectedSize != actualSize || maxTimestamp != this.txFile.getMaxTimestamp()) {
                LOG.info()
                        .$("actual table size has been adjusted [name=`").utf8(name).$('`')
                        .$(", expectedFixedSize=").$(expectedSize)
                        .$(", actualFixedSize=").$(actualSize)
                        .$(']').$();

                txFile.reset(actualSize, transientRowCount, maxTimestamp);
                return maxTimestamp;
            }
        }

        return timestamp;
    }

    private void repairMetaRename(int index) {
        try {
            path.concat(META_PREV_FILE_NAME);
            if (index > 0) {
                path.put('.').put(index);
            }
            path.$();

            if (ff.exists(path)) {
                LOG.info().$("Repairing metadata from: ").$(path).$();
                if (ff.exists(other.concat(META_FILE_NAME).$()) && !ff.remove(other)) {
                    throw CairoException.instance(ff.errno()).put("Repair failed. Cannot replace ").put(other);
                }

                if (!ff.rename(path, other)) {
                    throw CairoException.instance(ff.errno()).put("Repair failed. Cannot rename ").put(path).put(" -> ").put(other);
                }
            }
        } finally {
            path.trimTo(rootLen);
            other.trimTo(rootLen);
        }

        clearTodoLog();
    }

    private void repairTruncate() {
        LOG.info().$("repairing abnormally terminated truncate on ").$(path).$();
        if (partitionBy != PartitionBy.NONE) {
            removePartitionDirectories();
        }
        txFile.reset();
        clearTodoLog();
    }

    private void restoreMetaFrom(CharSequence fromBase, int fromIndex) {
        try {
            path.concat(fromBase);
            if (fromIndex > 0) {
                path.put('.').put(fromIndex);
            }
            path.$();

            if (!ff.rename(path, other.concat(META_FILE_NAME).$())) {
                throw CairoException.instance(ff.errno()).put("Cannot rename ").put(path).put(" -> ").put(other);
            }
        } finally {
            path.trimTo(rootLen);
            other.trimTo(rootLen);
        }
    }

    private void rollbackIndexes() {
        final long maxRow = txFile.getTransientRowCount() - 1;
        for (int i = 0, n = denseIndexers.size(); i < n; i++) {
            ColumnIndexer indexer = denseIndexers.getQuick(i);
            LOG.info().$("recovering index [fd=").$(indexer.getFd()).$(']').$();
            indexer.rollback(maxRow);
        }
    }

    private void rollbackSymbolTables() {
        int expectedMapWriters = txFile.readWriterCount();
        for (int i = 0; i < expectedMapWriters; i++) {
            denseSymbolMapWriters.getQuick(i).rollback(txFile.readSymbolWriterIndexOffset(i));
        }
    }

    private void runFragile(FragileCode fragile, CharSequence columnName, CairoException e) {
        try {
            fragile.run(columnName);
        } catch (CairoException err) {
            LOG.error().$("DOUBLE ERROR: 1st: {").$((Sinkable) e).$('}').$();
            throwDistressException(err);
        }
        throw e;
    }

    private void setAppendPosition(final long position, boolean ensureFileSize) {
        for (int i = 0; i < columnCount; i++) {
            // stop calculating oversize as soon as we find first over-sized column
            setColumnSize(
                    ff,
                    getPrimaryColumn(i),
                    getSecondaryColumn(i),
                    getColumnType(metaMem, i),
                    position - columnTops.getQuick(i),
                    tempMem16b,
                    ensureFileSize
            );
        }
    }

    /**
     * Sets path member variable to partition directory for the given timestamp and
     * partitionLo and partitionHi to partition interval in millis. These values are
     * determined based on input timestamp and value of partitionBy. For any given
     * timestamp this method will determine either day, month or year interval timestamp falls to.
     * Partition directory name is ISO string of interval start.
     * <p>
     * Because this method modifies "path" member variable, be sure path is trimmed to original
     * state within try..finally block.
     *
     * @param path                    path instance to modify
     * @param timestamp               to determine interval for
     * @param updatePartitionInterval flag indicating that partition interval partitionLo and
     */
    private void setStateForTimestamp(Path path, long timestamp, boolean updatePartitionInterval) {
        long partitionHi = TableUtils.setPathForPartition(path, partitionBy, timestamp);
        if (updatePartitionInterval) {
            this.partitionHi = partitionHi;
        }
    }

    void startAppendedBlock(long timestampLo, long timestampHi, long nRowsAdded, LongList blockColumnTops) {
        if (timestampLo < txFile.getMaxTimestamp()) {
            throw CairoException.instance(ff.errno()).put("Cannot insert rows out of order. Table=").put(path);
        }

        if (txFile.getAppendedPartitionCount() == 0) {
            openFirstPartition(timestampLo);
        }

        if (partitionBy != PartitionBy.NONE && timestampLo > partitionHi) {
            // Need close memory without truncating
            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                AppendOnlyVirtualMemory mem1 = getPrimaryColumn(columnIndex);
                mem1.close(false);
                AppendOnlyVirtualMemory mem2 = getSecondaryColumn(columnIndex);
                if (null != mem2) {
                    mem2.close(false);
                }
            }
            switchPartition(timestampLo);
        }
        this.txFile.appendBlock(timestampLo, timestampHi, nRowsAdded);

        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            // Handle column tops
            long blockColumnTop = blockColumnTops.getQuick(columnIndex);
            if (blockColumnTop != -1) {
                long columnTop = columnTops.getQuick(columnIndex);
                if (blockColumnTop != columnTop) {
                    try {
                        assert columnTop == 0;
                        assert blockColumnTop > 0;
                        TableUtils.setPathForPartition(path, partitionBy, timestampLo);
                        columnTops.setQuick(columnIndex, blockColumnTop);
                        writeColumnTop(getMetadata().getColumnName(columnIndex), blockColumnTop);
                    } finally {
                        path.trimTo(rootLen);
                    }
                }
            }
        }
    }

    private void switchPartition(long timestamp) {
        // Before partition can be switched we need to index records
        // added so far. Index writers will start point to different
        // files after switch.
        updateIndexes();

        // We need to store reference on partition so that archive
        // file can be created in appropriate directory.
        // For simplicity use partitionLo, which can be
        // translated to directory name when needed
        txFile.switchPartitions();
        openPartition(timestamp);
        setAppendPosition(0, false);
    }

    private void syncColumns(int commitMode) {
        final boolean async = commitMode == CommitMode.ASYNC;
        for (int i = 0; i < columnCount; i++) {
            columns.getQuick(i * 2).sync(async);
            final AppendOnlyVirtualMemory m2 = columns.getQuick(i * 2 + 1);
            if (m2 != null) {
                m2.sync(false);
            }
        }
    }

    private void throwDistressException(Throwable cause) {
        this.distressed = true;
        throw new CairoError(cause);
    }

    void updateActivePartitionDetails(long minTimestamp, long maxTimestamp, long transientRowCount) {
        this.txFile.transientRowCount = transientRowCount;
        // Compute max timestamp as maximum of out of order data and
        // data in existing partition.
        // When partition is new, the data timestamp is MIN_LONG
        this.txFile.maxTimestamp = maxTimestamp;
        this.txFile.minTimestamp = Math.min(this.txFile.minTimestamp, minTimestamp);
    }

    private void updateIndexes() {
        if (indexCount == 0 || avoidIndexOnCommit) {
            avoidIndexOnCommit = false;
            return;
        }
        updateIndexesSlow();
    }

    private void updateIndexesParallel(long lo, long hi) {
        indexSequences.clear();
        indexLatch.setCount(indexCount);
        final int nParallelIndexes = indexCount - 1;
        final Sequence indexPubSequence = this.messageBus.getIndexerPubSequence();
        final RingQueue<ColumnIndexerTask> indexerQueue = this.messageBus.getIndexerQueue();

        LOG.info().$("parallel indexing [indexCount=").$(indexCount).$(']').$();
        int serialIndexCount = 0;

        // we are going to index last column in this thread while other columns are on the queue
        OUT:
        for (int i = 0; i < nParallelIndexes; i++) {

            long cursor = indexPubSequence.next();
            if (cursor == -1) {
                // queue is full, process index in the current thread
                indexAndCountDown(denseIndexers.getQuick(i), lo, hi, indexLatch);
                serialIndexCount++;
                continue;
            }

            if (cursor == -2) {
                // CAS issue, retry
                do {
                    cursor = indexPubSequence.next();
                    if (cursor == -1) {
                        indexAndCountDown(denseIndexers.getQuick(i), lo, hi, indexLatch);
                        serialIndexCount++;
                        continue OUT;
                    }

                } while (cursor < 0);
            }

            final ColumnIndexerTask queueItem = indexerQueue.get(cursor);
            final ColumnIndexer indexer = denseIndexers.getQuick(i);
            final long sequence = indexer.getSequence();
            queueItem.indexer = indexer;
            queueItem.lo = lo;
            queueItem.hi = hi;
            queueItem.countDownLatch = indexLatch;
            queueItem.sequence = sequence;
            indexSequences.add(sequence);
            indexPubSequence.done(cursor);
        }

        // index last column while other columns are brewing on the queue
        indexAndCountDown(denseIndexers.getQuick(indexCount - 1), lo, hi, indexLatch);
        serialIndexCount++;

        // At this point we have re-indexed our column and if things are flowing nicely
        // all other columns should have been done by other threads. Instead of actually
        // waiting we gracefully check latch count.
        if (!indexLatch.await(configuration.getWorkStealTimeoutNanos())) {
            // other columns are still in-flight, we must attempt to steal work from other threads
            for (int i = 0; i < nParallelIndexes; i++) {
                ColumnIndexer indexer = denseIndexers.getQuick(i);
                if (indexer.tryLock(indexSequences.getQuick(i))) {
                    indexAndCountDown(indexer, lo, hi, indexLatch);
                    serialIndexCount++;
                }
            }
            // wait for the ones we cannot steal
            indexLatch.await();
        }

        // reset lock on completed indexers
        boolean distressed = false;
        for (int i = 0; i < indexCount; i++) {
            ColumnIndexer indexer = denseIndexers.getQuick(i);
            distressed = distressed | indexer.isDistressed();
        }

        if (distressed) {
            throwDistressException(null);
        }

        LOG.info().$("parallel indexing done [serialCount=").$(serialIndexCount).$(']').$();
    }

    private void updateIndexesSerially(long lo, long hi) {
        LOG.info().$("serial indexing [indexCount=").$(indexCount).$(']').$();
        for (int i = 0, n = indexCount; i < n; i++) {
            try {
                denseIndexers.getQuick(i).refreshSourceAndIndex(lo, hi);
            } catch (CairoException e) {
                // this is pretty severe, we hit some sort of a limit
                LOG.error().$("index error {").$((Sinkable) e).$('}').$();
                throwDistressException(e);
            }
        }
        LOG.info().$("serial indexing done [indexCount=").$(indexCount).$(']').$();
    }

    private void updateIndexesSlow() {
        final long hi = txFile.getTransientRowCount();
        final long lo = txFile.getAppendedPartitionCount() == 1 ? hi - txFile.getLastTxSize() : 0;
        if (indexCount > 1 && parallelIndexerEnabled && hi - lo > configuration.getParallelIndexThreshold()) {
            updateIndexesParallel(lo, hi);
        } else {
            updateIndexesSerially(lo, hi);
        }
    }

    private void updateMaxTimestamp(long timestamp) {
        txFile.updateMaxTimestamp(timestamp);
        this.timestampSetter.accept(timestamp);
    }

    private void updatePartitionSize(long partitionTimestamp, long partitionSize, long tableFloorOfMaxTimestamp, long srcDataMax) {
        this.txFile.fixedRowCount += partitionSize;
        if (partitionTimestamp < tableFloorOfMaxTimestamp) {
            this.txFile.fixedRowCount -= srcDataMax;
        }

        // We just updated non-last partition. It is possible that this partition
        // has already been updated by transaction or out of order logic was the only
        // code that updated it. To resolve this fork we need to check if "txPendingPartitionSizes"
        // contains timestamp of this partition. If it does - we don't increment "txPartitionCount"
        // (it has been incremented before out-of-order logic kicked in) and
        // we use partition size from "txPendingPartitionSizes" to subtract from "txPartitionCount"

        txFile.updatePartitionSizeByTimestamp(partitionTimestamp, partitionSize);
    }

    private void validateSwapMeta(CharSequence columnName) {
        try {
            try {
                path.concat(META_SWAP_FILE_NAME);
                if (metaSwapIndex > 0) {
                    path.put('.').put(metaSwapIndex);
                }
                metaMem.of(ff, path.$(), ff.getPageSize(), ff.length(path));
                validationMap.clear();
                validate(ff, metaMem, validationMap);
            } finally {
                metaMem.close();
                path.trimTo(rootLen);
            }
        } catch (CairoException e) {
            runFragile(RECOVER_FROM_META_RENAME_FAILURE, columnName, e);
        }
    }

    private void writeColumnEntry(int i) {
        ddlMem.putByte((byte) getColumnType(metaMem, i));
        long flags = 0;
        if (isColumnIndexed(metaMem, i)) {
            flags |= META_FLAG_BIT_INDEXED;
        }

        if (isSequential(metaMem, i)) {
            flags |= META_FLAG_BIT_SEQUENTIAL;
        }
        ddlMem.putLong(flags);
        ddlMem.putInt(getIndexBlockCapacity(metaMem, i));
        ddlMem.skip(META_COLUMN_DATA_RESERVED);
    }

    private void writeColumnTop(CharSequence name) {
        writeColumnTop(name, txFile.getTransientRowCount());
    }

    private void writeColumnTop(CharSequence name, long columnTop) {
        TableUtils.writeColumnTop(
                ff,
                path,
                name,
                columnTop,
                tempMem16b
        );
    }

    private void writeRestoreMetaTodo(CharSequence columnName) {
        try {
            todoMem.putLong(0, ++todoTxn); // write txn, reader will first read txn at offset 24 and then at offset 0
            Unsafe.getUnsafe().storeFence(); // make sure we do not write hash before writing txn (view from another thread)
            todoMem.putLong(8, configuration.getInstanceHashLo()); // write out our instance hashes
            todoMem.putLong(16, configuration.getInstanceHashHi());
            Unsafe.getUnsafe().storeFence();
            todoMem.putLong(32, 1);
            todoMem.putLong(40, TODO_RESTORE_META);
            todoMem.putLong(48, metaPrevIndex);
            Unsafe.getUnsafe().storeFence();
            todoMem.putLong(24, todoTxn);
            todoMem.setSize(56);

        } catch (CairoException e) {
            runFragile(RECOVER_FROM_TODO_WRITE_FAILURE, columnName, e);
        }
    }


    @FunctionalInterface
    public interface OutOfOrderNativeSortMethod {
        void shuffle(long pSrc, long pDest, long pIndex, long valueCount);
    }

    @FunctionalInterface
    public interface MergeShuffleOutOfOrderDataInternal {
        void shuffle(long pSrc1, long pSrc2, long pDest, long pIndex, long count);
    }

    @FunctionalInterface
    private interface RemoveFileLambda {
        void remove(FilesFacade ff, LPSZ name);
    }

    @FunctionalInterface
    private interface FragileCode {
        void run(CharSequence columnName);
    }

    @FunctionalInterface
    public interface OutOfOrderSortMethod {
        void sort(
                int columnIndex,
                long mergedTimestampsAddr,
                long valueCount,
                final int shl,
                final OutOfOrderNativeSortMethod shuffleFunc
        );
    }

    static class TimestampValueRecord implements Record {
        private long value;

        @Override
        public long getTimestamp(int col) {
            return value;
        }

        public void setTimestamp(long value) {
            this.value = value;
        }
    }

    private class OpenPartitionRowFunction implements RowFunction {
        @Override
        public Row newRow(long timestamp) {
            if (txFile.getMaxTimestamp() != Long.MIN_VALUE) {
                return (rowFunction = switchPartitionFunction).newRow(timestamp);
            }
            return getRowSlow(timestamp);
        }

        private Row getRowSlow(long timestamp) {
            txFile.setMinTimestamp(timestamp);
            openFirstPartition(timestamp);
            return (rowFunction = switchPartitionFunction).newRow(timestamp);
        }
    }

    private class NoPartitionFunction implements RowFunction {
        @Override
        public Row newRow(long timestamp) {
            bumpMasterRef();
            if (timestamp >= txFile.getMaxTimestamp()) {
                updateMaxTimestamp(timestamp);
                return row;
            }
            throw CairoException.instance(ff.errno()).put("Cannot insert rows out of order. Table=").put(path);
        }
    }

    private class NoTimestampFunction implements RowFunction {
        @Override
        public Row newRow(long timestamp) {
            bumpMasterRef();
            return row;
        }
    }

    private class OutOfOrderPartitionFunction implements RowFunction {
        @Override
        public Row newRow(long timestamp) {
            bumpMasterRef();
            mergeTimestampSetter(timestamp);
            return row;
        }
    }

    private class SwitchPartitionRowFunction implements RowFunction {
        @Override
        public Row newRow(long timestamp) {
            bumpMasterRef();
            if (timestamp > partitionHi || timestamp < txFile.getMaxTimestamp()) {
                return newRow0(timestamp);
            }
            updateMaxTimestamp(timestamp);
            return row;
        }

        @NotNull
        private Row newRow0(long timestamp) {
            if (timestamp < txFile.getMaxTimestamp()) {
                if (outOfOrderEnabled) {
                    LOG.info().$("out-of-order").$();
                    transientRowCountBeforeOutOfOrder = txFile.getTransientRowCount();
                    txFile.beginPartitionSizeUpdate();
                    openMergePartition();
                    TableWriter.this.oooRowCount = 0;
                    assert timestampMergeMem != null;
                    prevTimestampSetter = timestampSetter;
                    timestampSetter = mergeTimestampMethodRef;
                    timestampSetter.accept(timestamp);
                    TableWriter.this.rowFunction = oooRowFunction;
                    return row;
                }
                throw CairoException.instance(ff.errno()).put("Cannot insert rows out of order. Table=").put(path);
            }

            if (timestamp > partitionHi && partitionBy != PartitionBy.NONE) {
                switchPartition(timestamp);
            }

            updateMaxTimestamp(timestamp);
            return row;
        }
    }

    public class Row {
        private ObjList<? extends WriteOnlyVirtualMemory> activeColumns;
        private ObjList<Runnable> activeNullSetters;

        public void append() {
            if ((masterRef & 1) != 0) {
                for (int i = 0; i < columnCount; i++) {
                    if (refs.getQuick(i) < masterRef) {
                        activeNullSetters.getQuick(i).run();
                    }
                }
                masterRef++;
                txFile.append();
            }
        }

        public void cancel() {
            cancelRow();
        }

        public void putBin(int index, long address, long len) {
            getSecondaryColumn(index).putLong(getPrimaryColumn(index).putBin(address, len));
            notNull(index);
        }

        public void putBin(int index, BinarySequence sequence) {
            getSecondaryColumn(index).putLong(getPrimaryColumn(index).putBin(sequence));
            notNull(index);
        }

        public void putBool(int index, boolean value) {
            getPrimaryColumn(index).putBool(value);
            notNull(index);
        }

        public void putByte(int index, byte value) {
            getPrimaryColumn(index).putByte(value);
            notNull(index);
        }

        public void putChar(int index, char value) {
            getPrimaryColumn(index).putChar(value);
            notNull(index);
        }

        public void putDate(int index, long value) {
            putLong(index, value);
        }

        public void putDouble(int index, double value) {
            getPrimaryColumn(index).putDouble(value);
            notNull(index);
        }

        public void putFloat(int index, float value) {
            getPrimaryColumn(index).putFloat(value);
            notNull(index);
        }

        public void putInt(int index, int value) {
            getPrimaryColumn(index).putInt(value);
            notNull(index);
        }

        public void putLong(int index, long value) {
            getPrimaryColumn(index).putLong(value);
            notNull(index);
        }

        public void putLong256(int index, long l0, long l1, long l2, long l3) {
            getPrimaryColumn(index).putLong256(l0, l1, l2, l3);
            notNull(index);
        }

        public void putLong256(int index, Long256 value) {
            getPrimaryColumn(index).putLong256(value.getLong0(), value.getLong1(), value.getLong2(), value.getLong3());
            notNull(index);
        }

        public void putLong256(int index, CharSequence hexString) {
            getPrimaryColumn(index).putLong256(hexString);
            notNull(index);
        }

        public void putLong256(int index, @NotNull CharSequence hexString, int start, int end) {
            getPrimaryColumn(index).putLong256(hexString, start, end);
            notNull(index);
        }

        public void putShort(int index, short value) {
            getPrimaryColumn(index).putShort(value);
            notNull(index);
        }

        public void putStr(int index, CharSequence value) {
            getSecondaryColumn(index).putLong(getPrimaryColumn(index).putStr(value));
            notNull(index);
        }

        public void putStr(int index, char value) {
            getSecondaryColumn(index).putLong(getPrimaryColumn(index).putStr(value));
            notNull(index);
        }

        public void putStr(int index, CharSequence value, int pos, int len) {
            getSecondaryColumn(index).putLong(getPrimaryColumn(index).putStr(value, pos, len));
            notNull(index);
        }

        public void putSym(int index, CharSequence value) {
            getPrimaryColumn(index).putInt(symbolMapWriters.getQuick(index).put(value));
            notNull(index);
        }

        public void putSym(int index, char value) {
            getPrimaryColumn(index).putInt(symbolMapWriters.getQuick(index).put(value));
            notNull(index);
        }

        public void putSymIndex(int index, int symIndex) {
            getPrimaryColumn(index).putInt(symIndex);
            notNull(index);
        }

        public void putTimestamp(int index, long value) {
            putLong(index, value);
        }

        public void putTimestamp(int index, CharSequence value) {
            // try UTC timestamp first (micro)
            long l;
            try {
                l = TimestampFormatUtils.parseUTCTimestamp(value);
            } catch (NumericException e) {
                try {
                    l = TimestampFormatUtils.parseTimestamp(value);
                } catch (NumericException numericException) {
                    throw CairoException.instance(0).put("could not convert to timestamp [value=").put(value).put(']');
                }
            }
            putTimestamp(index, l);
        }

        private WriteOnlyVirtualMemory getPrimaryColumn(int columnIndex) {
            return activeColumns.getQuick(getPrimaryColumnIndex(columnIndex));
        }

        private WriteOnlyVirtualMemory getSecondaryColumn(int columnIndex) {
            return activeColumns.getQuick(getSecondaryColumnIndex(columnIndex));
        }

        private void notNull(int index) {
            refs.setQuick(index, masterRef);
        }
    }

    private class WriterTransientSymbolCountChangeHandler implements TransientSymbolCountChangeHandler {
        private int symColIndex;

        private WriterTransientSymbolCountChangeHandler(int symColIndex) {
            this.symColIndex = symColIndex;
        }

        @Override
        public void handleTansientymbolCountChange(int symbolCount) {
            Unsafe.getUnsafe().storeFence();
            txFile.writeTransientSymbolCount(symColIndex, symbolCount);
        }

    }

    static {
        IGNORED_FILES.add("..");
        IGNORED_FILES.add(".");
        IGNORED_FILES.add(META_FILE_NAME);
        IGNORED_FILES.add(TXN_FILE_NAME);
        IGNORED_FILES.add(TODO_FILE_NAME);
    }

}
