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
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.*;
import io.questdb.std.*;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.NativeLPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
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
    public static final int O3_BLOCK_NONE = -1;
    public static final int O3_BLOCK_O3 = 1;
    public static final int O3_BLOCK_DATA = 2;
    public static final int O3_BLOCK_MERGE = 3;
    private static final int MEM_PAGE_SIZE = 16 * Numbers.SIZE_1MB;
    private static final Log LOG = LogFactory.getLog(TableWriter.class);
    private static final CharSequenceHashSet IGNORED_FILES = new CharSequenceHashSet();
    private static final Runnable NOOP = () -> {
    };
    private final static RemoveFileLambda REMOVE_OR_LOG = TableWriter::removeFileAndOrLog;
    private final static RemoveFileLambda REMOVE_OR_EXCEPTION = TableWriter::removeOrException;
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
    private final RowFunction o3RowFunction = new O3PartitionFunction();
    private final NativeLPSZ nativeLPSZ = new NativeLPSZ();
    private final LongList columnTops;
    private final FilesFacade ff;
    private final DateFormat partitionDirFmt;
    private final AppendOnlyVirtualMemory ddlMem;
    private final int mkDirMode;
    private final int fileOperationRetryCount;
    private final CharSequence tableName;
    private final TableWriterMetadata metadata;
    private final CairoConfiguration configuration;
    private final LowerCaseCharSequenceIntHashMap validationMap = new LowerCaseCharSequenceIntHashMap();
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
    private final ObjList<Runnable> o3NullSetters;
    private final ObjList<ContiguousVirtualMemory> o3Columns;
    private final ObjList<ContiguousVirtualMemory> o3Columns2;
    private final TableBlockWriter blockWriter;
    private final TimestampValueRecord dropPartitionFunctionRec = new TimestampValueRecord();
    private final ObjList<O3CallbackTask> o3PendingCallbackTasks = new ObjList<>();
    private final O3ColumnUpdateMethod oooSortVarColumnRef = this::o3SortVarColumn;
    private final O3ColumnUpdateMethod oooSortFixColumnRef = this::o3SortFixColumn;
    private final SOUnboundedCountDownLatch o3DoneLatch = new SOUnboundedCountDownLatch();
    private final AtomicLong o3PartitionUpdRemaining = new AtomicLong();
    private final AtomicInteger o3ErrorCount = new AtomicInteger();
    private final MappedReadWriteMemory todoMem = new PagedMappedReadWriteMemory();
    private final TxWriter txFile;
    private final FindVisitor removePartitionDirsNotAttached = this::removePartitionDirsNotAttached;
    private final LongList o3PartitionRemoveCandidates = new LongList();
    private final ObjectPool<O3MutableAtomicInteger> o3ColumnCounters = new ObjectPool<O3MutableAtomicInteger>(O3MutableAtomicInteger::new, 64);
    private final ObjectPool<O3Basket> o3BasketPool = new ObjectPool<O3Basket>(O3Basket::new, 64);
    private final TxnScoreboard txnScoreboard;
    private final StringSink o3Sink = new StringSink();
    private final NativeLPSZ o3NativeLPSZ = new NativeLPSZ();
    private final RingQueue<O3PartitionUpdateTask> o3PartitionUpdateQueue;
    private final MPSequence o3PartitionUpdatePubSeq;
    private final SCSequence o3PartitionUpdateSubSeq;
    private final boolean o3QuickSortEnabled;
    private final LongConsumer appendTimestampSetter;
    private long todoTxn;
    private ContiguousVirtualMemory o3TimestampMem;
    private final O3ColumnUpdateMethod o3MoveLagRef = this::o3MoveLag0;
    private ContiguousVirtualMemory o3TimestampMemCpy;
    private long lockFd = -1;
    private LongConsumer timestampSetter;
    private int columnCount;
    private RowFunction rowFunction = openPartitionFunction;
    private boolean avoidIndexOnCommit = false;
    private long partitionTimestampHi;
    private long masterRef = 0;
    private long o3MasterRef = -1;
    private boolean removeDirOnCancelRow = true;
    private long tempMem16b = Unsafe.malloc(16);
    private int metaSwapIndex;
    private int metaPrevIndex;
    private final FragileCode RECOVER_FROM_TODO_WRITE_FAILURE = this::recoverFromTodoWriteFailure;
    private final FragileCode RECOVER_FROM_SYMBOL_MAP_WRITER_FAILURE = this::recoverFromSymbolMapWriterFailure;
    private final FragileCode RECOVER_FROM_SWAP_RENAME_FAILURE = this::recoverFromSwapRenameFailure;
    private final FragileCode RECOVER_FROM_COLUMN_OPEN_FAILURE = this::recoverOpenColumnFailure;
    private int indexCount;
    private boolean performRecovery;
    private boolean distressed = false;
    private LifecycleManager lifecycleManager;
    private String designatedTimestampColumnName;
    private long o3RowCount;
    private final O3ColumnUpdateMethod o3MoveUncommittedRef = this::o3MoveUncommitted0;
    private long lastPartitionTimestamp;
    private boolean o3InError = false;

    public TableWriter(CairoConfiguration configuration, CharSequence tableName) {
        this(configuration, tableName, new MessageBusImpl(configuration));
    }

    public TableWriter(CairoConfiguration configuration, CharSequence tableName, @NotNull MessageBus messageBus) {
        this(configuration, tableName, messageBus, true, DefaultLifecycleManager.INSTANCE);
    }

    public TableWriter(
            CairoConfiguration configuration,
            CharSequence tableName,
            @NotNull MessageBus messageBus,
            boolean lock,
            LifecycleManager lifecycleManager
    ) {
        this(configuration, tableName, messageBus, lock, lifecycleManager, configuration.getRoot());
    }

    public TableWriter(
            CairoConfiguration configuration,
            CharSequence tableName,
            @NotNull MessageBus messageBus,
            boolean lock,
            LifecycleManager lifecycleManager,
            CharSequence root
    ) {
        LOG.info().$("open '").utf8(tableName).$('\'').$();
        this.configuration = configuration;
        this.messageBus = messageBus;
        this.defaultCommitMode = configuration.getCommitMode();
        this.lifecycleManager = lifecycleManager;
        this.parallelIndexerEnabled = configuration.isParallelIndexingEnabled();
        this.ff = configuration.getFilesFacade();
        this.mkDirMode = configuration.getMkDirMode();
        this.fileOperationRetryCount = configuration.getFileOperationRetryCount();
        this.tableName = Chars.toString(tableName);
        this.o3QuickSortEnabled = configuration.isO3QuickSortEnabled();
        this.o3PartitionUpdateQueue = new RingQueue<O3PartitionUpdateTask>(O3PartitionUpdateTask.CONSTRUCTOR, configuration.getO3PartitionUpdateQueueCapacity());
        this.o3PartitionUpdatePubSeq = new MPSequence(this.o3PartitionUpdateQueue.getCapacity());
        this.o3PartitionUpdateSubSeq = new SCSequence();
        o3PartitionUpdatePubSeq.then(o3PartitionUpdateSubSeq).then(o3PartitionUpdatePubSeq);

        this.path = new Path();
        this.path.of(root).concat(tableName);
        this.other = new Path().of(root).concat(tableName);
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
            this.txFile = new TxWriter(ff, path, partitionBy);
            this.txnScoreboard = new TxnScoreboard(ff, path.trimTo(rootLen), configuration.getTxnScoreboardEntryCount());
            path.trimTo(rootLen);
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
            this.o3Columns = new ObjList<>(columnCount * 2);
            this.o3Columns2 = new ObjList<>(columnCount * 2);
            this.row.activeColumns = columns;
            this.symbolMapWriters = new ObjList<>(columnCount);
            this.indexers = new ObjList<>(columnCount);
            this.denseSymbolMapWriters = new ObjList<>(metadata.getSymbolMapCount());
            this.denseSymbolTransientCountHandlers = new ObjList<>(metadata.getSymbolMapCount());
            this.nullSetters = new ObjList<>(columnCount);
            this.o3NullSetters = new ObjList<>(columnCount);
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

            configureColumnMemory();
            timestampSetter = configureTimestampSetter();
            this.appendTimestampSetter = timestampSetter;
            this.txFile.readRowCounts();
            configureAppendPosition();
            purgeUnusedPartitions();
            clearTodoLog();

        } catch (Throwable e) {
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
     * @param isSequential            for columns that contain sequential values query optimiser can make assumptions on range searches (future feature)
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
                    if (timestamp == Numbers.LONG_NaN) {
                        return;
                    }
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
        } catch (Throwable e) {
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
            LOG.error().$("attaching partitions on table with symbols not yet supported [table=").$(tableName)
                    .$(",partition=").$ts(timestamp).I$();
            return TABLE_HAS_SYMBOLS;
        }

        boolean rollbackRename = false;
        try {
            setPathForPartition(path, partitionBy, timestamp, false);
            if (!ff.exists(path.$())) {
                setPathForPartition(other, partitionBy, timestamp, false);
                other.put(DETACHED_DIR_MARKER);

                if (ff.exists(other.$())) {
                    if (ff.rename(other, path)) {
                        rollbackRename = true;
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
                        LOG.info().$("committing open transaction before applying attach partition command [table=").$(tableName)
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
                    rollbackRename = false;
                } else {
                    LOG.error().$("cannot detect partition size [path=").$(path).$(",timestampColumn=").$(timestampCol).$(']').$();
                    return PARTITION_EMPTY;
                }
            } else {
                LOG.error().$("cannot attach missing partition [path=").$(path).$(']').$();
                return CANNOT_ATTACH_MISSING_PARTITION;
            }
        } finally {
            if (rollbackRename) {
                // rename back to .detached
                // otherwise it can be deleted on writer re-open
                if (ff.rename(path.$(), other.$())) {
                    LOG.info().$("moved partition dir after failed attach attempt: ").$(path).$(" to ").$(other).$();
                } else {
                    LOG.info().$("file system error on trying to rename partition folder [errno=").$(ff.errno())
                            .$(",from=").$(path).$(",to=").$(other).I$();
                }
            }
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

    public boolean checkMaxAndCommitLag() {
        if (getO3RowCount() < metadata.getMaxUncommittedRows()) {
            return false;
        }
        commitWithLag();
        return true;
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

    public void commit(int commitMode) {
        commit(commitMode, 0);
    }

    public void commitWithLag() {
        commit(defaultCommitMode, metadata.getCommitLag());
    }

    public void commitWithLag(long lagMicros) {
        commit(defaultCommitMode, lagMicros);
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

    public FilesFacade getFilesFacade() {
        return ff;
    }

    public long getMaxTimestamp() {
        return txFile.getMaxTimestamp();
    }

    public TableWriterMetadata getMetadata() {
        return metadata;
    }

    public long getO3RowCount() {
        return (masterRef - o3MasterRef + 1) / 2;
    }

    public int getPartitionBy() {
        return partitionBy;
    }

    public int getPartitionCount() {
        return txFile.getPartitionCount();
    }

    public long getStructureVersion() {
        return txFile.getStructureVersion();
    }

    public int getSymbolIndex(int columnIndex, CharSequence symValue) {
        return symbolMapWriters.getQuick(columnIndex).put(symValue);
    }

    public CharSequence getTableName() {
        return tableName;
    }

    public long getTxn() {
        return txFile.getTxn();
    }

    public TxnScoreboard getTxnScoreboard() {
        return txnScoreboard;
    }

    public boolean inTransaction() {
        return txFile != null && (txFile.inTransaction() || hasO3());
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

    public void o3BumpErrorCount() {
        o3ErrorCount.incrementAndGet();
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
            int timestampIndex2 = metadata.getColumnIndex(timestampColumnName);
            metadata.setTimestampIndex(timestampIndex2);
            o3TimestampMem = o3Columns.getQuick(getPrimaryColumnIndex(timestampIndex2));
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
                int errno;
                if ((errno = ff.rmdir(path.chop$().slash$())) != 0) {
                    LOG.info().$("partition directory delete is postponed [path=").$(path)
                            .$(", errno=").$(errno)
                            .$(']').$();
                } else {
                    LOG.info().$("partition marked for delete [path=").$(path).$(']').$();
                }
            } else {
                LOG.info().$("partition absent on disk now detached from table [path=").$(path).$(']').$();
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

        if (txFile.getPartitionCount() == 0) {
            throw SqlException.$(posForError, "table is empty");
        } else {
            // Drop partitions in descending order so if folders are missing on disk
            // removePartition does not fail to determine next minTimestamp
            for (int i = txFile.getPartitionCount() - 1; i > -1; i--) {
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
        if (o3InError || inTransaction()) {
            try {
                LOG.info().$("tx rollback [name=").$(tableName).$(']').$();
                if ((masterRef & 1) != 0) {
                    masterRef++;
                }
                freeColumns(false);
                this.txFile.readUnchecked();
                rollbackIndexes();
                rollbackSymbolTables();
                purgeUnusedPartitions();
                configureAppendPosition();
                o3InError = false;
                LOG.info().$("tx rollback complete [name=").$(tableName).$(']').$();
            } catch (Throwable e) {
                LOG.error().$("could not perform rollback [name=").$(tableName).$(", msg=").$(e.getMessage()).$(']').$();
                distressed = true;
            }
        }
    }

    public void setLifecycleManager(LifecycleManager lifecycleManager) {
        this.lifecycleManager = lifecycleManager;
    }

    public void setMetaCommitLag(long commitLag) {
        try {
            commit();
            long metaSize = copyMetadataAndUpdateVersion();
            openMetaSwapFileByIndex(ff, ddlMem, path, rootLen, this.metaSwapIndex);
            try {
                ddlMem.jumpTo(META_OFFSET_COMMIT_LAG);
                ddlMem.putLong(commitLag);
                ddlMem.jumpTo(metaSize);
            } finally {
                ddlMem.close();
            }

            finishMetaSwapUpdate();
            metadata.setCommitLag(commitLag);
            clearTodoLog();
        } finally {
            ddlMem.close();
        }
    }

    public void setMetaMaxUncommittedRows(int maxUncommittedRows) {
        try {
            commit();
            long metaSize = copyMetadataAndUpdateVersion();
            openMetaSwapFileByIndex(ff, ddlMem, path, rootLen, this.metaSwapIndex);
            try {
                ddlMem.jumpTo(META_OFFSET_MAX_UNCOMMITTED_ROWS);
                ddlMem.putInt(maxUncommittedRows);
                ddlMem.jumpTo(metaSize);
            } finally {
                ddlMem.close();
            }

            finishMetaSwapUpdate();
            metadata.setMaxUncommittedRows(maxUncommittedRows);
            clearTodoLog();
        } finally {
            ddlMem.close();
        }
    }

    public long size() {
        return txFile.getRowCount();
    }

    @Override
    public String toString() {
        return "TableWriter{" +
                "name=" + tableName +
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
        todoMem.putLong(8, configuration.getDatabaseIdLo()); // write out our instance hashes
        todoMem.putLong(16, configuration.getDatabaseIdHi());
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

        LOG.info().$("truncated [name=").$(tableName).$(']').$();
    }

    public void updateMetadataVersion() {

        checkDistressed();

        commit();
        // create new _meta.swp
        copyMetadataAndUpdateVersion();

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
        Row r = newRow(Math.max(Timestamps.O3_MIN_TS, txFile.getMaxTimestamp()));
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
                final CharSequence columnName = metadata.getColumnName(columnIndex);
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
            copyVersionAndLagValues();
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

    void cancelRow() {
        if ((masterRef & 1) == 0) {
            return;
        }

        if (hasO3()) {
            masterRef--;
            setO3AppendPosition(getO3RowCount());
            return;
        }

        long dirtyMaxTimestamp = txFile.getMaxTimestamp();
        long dirtyTransientRowCount = txFile.getTransientRowCount();
        long rollbackToMaxTimestamp = txFile.cancelToMaxTimestamp();
        long rollbackToTransientRowCount = txFile.cancelToTransientRowCount();

        // dirty timestamp should be 1 because newRow() increments it
        if (dirtyTransientRowCount == 1) {
            if (partitionBy != PartitionBy.NONE) {
                // we have to undo creation of partition
                closeActivePartition(false);
                if (removeDirOnCancelRow) {
                    try {
                        setStateForTimestamp(path, dirtyMaxTimestamp, false);
                        int errno;
                        if ((errno = ff.rmdir(path.$())) != 0) {
                            throw CairoException.instance(errno).put("Cannot remove directory: ").put(path);
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
                    } catch (Throwable e) {
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
            boolean rowChanged = metadata.getTimestampIndex() >= 0; // adding new row already writes timestamp
            if (!rowChanged) {
                // verify if any of the columns have been changed
                // if not - we don't have to do
                for (int i = 0; i < columnCount; i++) {
                    if (refs.getQuick(i) == masterRef) {
                        rowChanged = true;
                        break;
                    }
                }
            }

            // is no column has been changed we take easy option and do nothing
            if (rowChanged) {
                setAppendPosition(dirtyTransientRowCount - 1, false);
            }
        }
        refs.fill(0, columnCount, --masterRef);
        txFile.transientRowCount--;
    }

    private void cancelRowAndBump() {
        cancelRow();
        masterRef++;
    }

    private void checkDistressed() {
        if (!distressed) {
            return;
        }
        throw new CairoError("Table '" + tableName.toString() + "' is distressed");
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

    void closeActivePartition(boolean truncate) {
        LOG.info().$("closing last partition [table=").$(tableName).I$();
        closeAppendMemoryTruncate(truncate);
        freeIndexers();
    }

    void closeActivePartition(long size) {
        for (int i = 0; i < columnCount; i++) {
            // stop calculating oversize as soon as we find first over-sized column
            final AppendOnlyVirtualMemory mem1 = getPrimaryColumn(i);
            final AppendOnlyVirtualMemory mem2 = getSecondaryColumn(i);
            setColumnSize(
                    ff,
                    mem1,
                    mem2,
                    getColumnType(metaMem, i),
                    size - columnTops.getQuick(i),
                    tempMem16b,
                    false
            );
            Misc.free(mem1);
            Misc.free(mem2);
        }
        Misc.freeObjList(denseIndexers);
        denseIndexers.clear();
    }

    private void closeAppendMemoryTruncate(boolean truncate) {
        for (int i = 0, n = columns.size(); i < n; i++) {
            AppendOnlyVirtualMemory m = columns.getQuick(i);
            if (m != null) {
                m.close(truncate);
            }
        }
    }

    /**
     * Commits newly added rows of data. This method updates transaction file with pointers to end of appended data.
     * <p>
     * <b>Pending rows</b>
     * <p>This method will cancel pending rows by calling {@link #cancelRow()}. Data in partially appended row will be lost.</p>
     *
     * @param commitMode commit durability mode.
     * @param commitLag  if > 0 then do a partial commit, leaving the rows within the lag in a new uncommitted transaction
     */
    private void commit(int commitMode, long commitLag) {

        checkDistressed();

        if (o3InError) {
            rollback();
            return;
        }

        if ((masterRef & 1) != 0) {
            cancelRow();
        }

        if (inTransaction()) {

            if (hasO3() && o3Commit(commitLag)) {
                return;
            }

            if (commitMode != CommitMode.NOSYNC) {
                syncColumns(commitMode);
            }

            updateIndexes();
            txFile.commit(commitMode, this.denseSymbolMapWriters);
            o3ProcessPartitionRemoveCandidates();
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
        if (this.txFile.getMaxTimestamp() > Long.MIN_VALUE || partitionBy == PartitionBy.NONE) {
            openFirstPartition(this.txFile.getMaxTimestamp());
            if (partitionBy == PartitionBy.NONE) {
                if (metadata.getTimestampIndex() < 0) {
                    rowFunction = noTimestampFunction;
                } else {
                    rowFunction = noPartitionFunction;
                    timestampSetter = appendTimestampSetter;
                }
            } else {
                rowFunction = switchPartitionFunction;
                timestampSetter = appendTimestampSetter;
            }
        } else {
            rowFunction = openPartitionFunction;
            timestampSetter = appendTimestampSetter;
        }
        row.activeColumns = columns;
    }

    private void configureColumn(int type, boolean indexFlag) {
        final AppendOnlyVirtualMemory primary = new AppendOnlyVirtualMemory();
        final AppendOnlyVirtualMemory secondary;
        final ContiguousVirtualMemory oooPrimary = new ContiguousVirtualMemory(MEM_PAGE_SIZE, Integer.MAX_VALUE);
        final ContiguousVirtualMemory oooSecondary;
        final ContiguousVirtualMemory oooPrimary2 = new ContiguousVirtualMemory(MEM_PAGE_SIZE, Integer.MAX_VALUE);
        final ContiguousVirtualMemory oooSecondary2;
        switch (type) {
            case ColumnType.BINARY:
            case ColumnType.STRING:
                secondary = new AppendOnlyVirtualMemory();
                oooSecondary = new ContiguousVirtualMemory(MEM_PAGE_SIZE, Integer.MAX_VALUE);
                oooSecondary2 = new ContiguousVirtualMemory(MEM_PAGE_SIZE, Integer.MAX_VALUE);
                break;
            default:
                secondary = null;
                oooSecondary = null;
                oooSecondary2 = null;
                break;
        }
        columns.add(primary);
        columns.add(secondary);
        o3Columns.add(oooPrimary);
        o3Columns.add(oooSecondary);
        o3Columns2.add(oooPrimary2);
        o3Columns2.add(oooSecondary2);
        configureNullSetters(nullSetters, type, primary, secondary);
        configureNullSetters(o3NullSetters, type, oooPrimary, oooSecondary);
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
            o3TimestampMem = o3Columns.getQuick(getPrimaryColumnIndex(timestampIndex));
            o3TimestampMemCpy = new ContiguousVirtualMemory(MEM_PAGE_SIZE, Integer.MAX_VALUE);
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
            o3NullSetters.setQuick(index, NOOP);
            return getPrimaryColumn(index)::putLong;
        }
    }

    private void consumeO3PartitionRemoveTasks() {
        // consume discovery jobs
        final RingQueue<O3PurgeDiscoveryTask> discoveryQueue = messageBus.getO3PurgeDiscoveryQueue();
        final Sequence discoverySubSeq = messageBus.getO3PurgeDiscoverySubSeq();
        final RingQueue<O3PurgeTask> purgeQueue = messageBus.getO3PurgeQueue();
        final Sequence purgePubSeq = messageBus.getO3PurgePubSeq();
        final Sequence purgeSubSeq = messageBus.getO3PurgeSubSeq();

        if (discoverySubSeq != null) {
            while (true) {
                long cursor = discoverySubSeq.next();
                if (cursor > -1) {
                    O3PurgeDiscoveryTask task = discoveryQueue.get(cursor);
                    O3PurgeDiscoveryJob.discoverPartitions(
                            ff,
                            o3Sink,
                            o3NativeLPSZ,
                            refs, // reuse, this is only called from writer close
                            purgeQueue,
                            purgePubSeq,
                            path,
                            tableName,
                            task.getPartitionBy(),
                            task.getTimestamp(),
                            txnScoreboard
                    );
                } else if (cursor == -1) {
                    break;
                }
            }
        }

        // consume purge jobs
        if (purgeSubSeq != null) {
            while (true) {
                long cursor = purgeSubSeq.next();
                if (cursor > -1) {
                    O3PurgeTask task = purgeQueue.get(cursor);
                    O3PurgeJob.purgePartitionDir(
                            ff,
                            other,
                            task.getPartitionBy(),
                            task.getTimestamp(),
                            txnScoreboard,
                            task.getNameTxnToRemove(),
                            task.getMinTxnToExpect()
                    );
                } else if (cursor == -1) {
                    break;
                }
            }
        }
    }

    private int copyMetadataAndSetIndexed(int columnIndex, int indexValueBlockSize) {
        try {
            int index = openMetaSwapFile(ff, ddlMem, path, rootLen, configuration.getMaxSwapFileCount());
            int columnCount = metaMem.getInt(META_OFFSET_COUNT);
            ddlMem.putInt(columnCount);
            ddlMem.putInt(metaMem.getInt(META_OFFSET_PARTITION_BY));
            ddlMem.putInt(metaMem.getInt(META_OFFSET_TIMESTAMP_INDEX));
            copyVersionAndLagValues();
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

    private long copyMetadataAndUpdateVersion() {
        try {
            int index = openMetaSwapFile(ff, ddlMem, path, rootLen, configuration.getMaxSwapFileCount());
            int columnCount = metaMem.getInt(META_OFFSET_COUNT);

            ddlMem.putInt(columnCount);
            ddlMem.putInt(metaMem.getInt(META_OFFSET_PARTITION_BY));
            ddlMem.putInt(metaMem.getInt(META_OFFSET_TIMESTAMP_INDEX));
            copyVersionAndLagValues();
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
            this.metaSwapIndex = index;
            return nameOffset;
        } finally {
            ddlMem.close();
        }
    }

    private void copyVersionAndLagValues() {
        ddlMem.putInt(ColumnType.VERSION);
        ddlMem.putInt(metaMem.getInt(META_OFFSET_TABLE_ID));
        ddlMem.putInt(metaMem.getInt(META_OFFSET_MAX_UNCOMMITTED_ROWS));
        ddlMem.putLong(metaMem.getLong(META_OFFSET_COMMIT_LAG));
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
        consumeO3PartitionRemoveTasks();
        boolean tx = inTransaction();
        freeColumns(truncate & !distressed);
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
            Misc.free(txnScoreboard);
            Misc.free(path);
            Misc.free(o3TimestampMemCpy);
            freeTempMem();
            LOG.info().$("closed '").utf8(tableName).$('\'').$();
        }
    }


    private void finishMetaSwapUpdate() {

        // rename _meta to _meta.prev
        this.metaPrevIndex = rename(fileOperationRetryCount);
        writeRestoreMetaTodo();

        try {
            // rename _meta.swp to -_meta
            restoreMetaFrom(META_SWAP_FILE_NAME, metaSwapIndex);
        } catch (CairoException ex) {
            try {
                recoverFromTodoWriteFailure(null);
            } catch (CairoException ex2) {
                throwDistressException(ex2);
            }
            throw ex;
        }

        try {
            // open _meta file
            openMetaFile(ff, path, rootLen, metaMem);
        } catch (CairoException err) {
            throwDistressException(err);
        }

        txFile.bumpStructureVersion(this.denseSymbolMapWriters);
        metadata.setTableVersion();
    }

    private void freeAndRemoveColumnPair(ObjList<?> columns, int pi, int si) {
        Misc.free(columns.getQuick(pi));
        Misc.free(columns.getQuick(si));
        columns.remove(pi);
        columns.remove(pi);
    }

    private void freeColumns(boolean truncate) {
        // null check is because this method could be called from the constructor
        if (columns != null) {
            closeAppendMemoryTruncate(truncate);
        }
        Misc.freeObjListAndKeepObjects(o3Columns);
        Misc.freeObjListAndKeepObjects(o3Columns2);
    }

    private void freeIndexers() {
        if (indexers != null) {
            // Don't change items of indexers, they are re-used
            if (indexers != null) {
                for (int i = 0, n = indexers.size(); i < n; i++) {
                    Misc.free(indexers.getQuick(i));
                }
            }
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

    BitmapIndexWriter getBitmapIndexWriter(int columnIndex) {
        return indexers.getQuick(columnIndex).getWriter();
    }

    long getColumnTop(int columnIndex) {
        return columnTops.getQuick(columnIndex);
    }

    CairoConfiguration getConfiguration() {
        return configuration;
    }

    Sequence getO3CopyPubSeq() {
        return messageBus.getO3CopyPubSeq();
    }

    RingQueue<O3CopyTask> getO3CopyQueue() {
        return messageBus.getO3CopyQueue();
    }

    Sequence getO3OpenColumnPubSeq() {
        return messageBus.getO3OpenColumnPubSeq();
    }

    RingQueue<O3OpenColumnTask> getO3OpenColumnQueue() {
        return messageBus.getO3OpenColumnQueue();
    }

    Sequence getO3PartitionUpdatePubSeq() {
        return o3PartitionUpdatePubSeq;
    }

    RingQueue<O3PartitionUpdateTask> getO3PartitionUpdateQueue() {
        return o3PartitionUpdateQueue;
    }

    private long getPartitionLo(long timestamp) {
        return timestampFloorMethod.floor(timestamp);
    }

    long getPartitionNameTxnByIndex(int index) {
        return txFile.getPartitionNameTxnByIndex(index);
    }

    long getPartitionSizeByIndex(int index) {
        return txFile.getPartitionSizeByIndex(index);
    }

    long getPrimaryAppendOffset(long timestamp, int columnIndex) {
        if (txFile.getAppendedPartitionCount() == 0) {
            openFirstPartition(timestamp);
        }

        if (timestamp > partitionTimestampHi) {
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

        if (timestamp > partitionTimestampHi) {
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

    private boolean hasO3() {
        return o3MasterRef > -1 && getO3RowCount() > 0;
    }

    private long indexHistoricPartitions(SymbolColumnIndexer indexer, CharSequence columnName, int indexValueBlockSize) {
        final long ts = this.txFile.getMaxTimestamp();
        if (ts > Numbers.LONG_NaN) {
            final long maxTimestamp = timestampFloorMethod.floor(ts);
            long timestamp = txFile.getMinTimestamp();

            //noinspection TryFinallyCanBeTryWithResources
            try (final MappedReadOnlyMemory roMem = new SinglePageMappedReadOnlyPageMemory()) {

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
            } finally {
                indexer.close();
            }
            return timestamp;
        }
        return ts;
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

    private long o3CalculatedMoveUncommittedSize(long transientRowsAdded, long committedTransientRowCount) {
        // We want to move as much as possible of uncommitted rows to O3 dedicated memory from column files
        // but not all the column file data is mapped and mapping is relatively expensive.
        // If the rows are not moved to O3 memory it will result in merge of bigger segment
        // from O3 to column files
        // If all the rows moved this will be sort shuffle in O3 memory and copying back to column files
        for (int colIndex = 0; colIndex < columnCount; colIndex++) {
            int columnType = metadata.getColumnType(colIndex);
            AppendOnlyVirtualMemory primaryColumn = getPrimaryColumn(colIndex);
            AppendOnlyVirtualMemory secondaryColumn = getSecondaryColumn(colIndex);
            // Fixed size column
            //
            //   Partition can be like this
            //
            //          Page 1 (16Mb) unmapped                            Page 2 (16Mb) mapped
            //   |  ================================ < === |  ================================ > ----- |
            //   |   committedTransientRowCount     I    transientRowsAdded                   |
            //
            // We want to move separator I between committedTransientRowCount and transientRowsAdded
            // to start from mapped page boundary but keeping sum of committedTransientRowCount + transientRowsAdded same
            // so that after the change in committedTransientRowCount, transientRowsAdded the picture looks like
            //
            //          Page 1 (16Mb) unmapped                            Page 2 (16Mb) mapped
            //   |  ================================ < === |  ================================ > ----- |
            //   |     committedTransientRowCount          I     transientRowsAdded            |

            int shl = ColumnType.pow2SizeOf(columnType);

            if (secondaryColumn == null) {
                long srcMappedRows = primaryColumn.offsetInPage((committedTransientRowCount + transientRowsAdded) << shl) >> shl;
                if (srcMappedRows < transientRowsAdded) {
                    long delta = transientRowsAdded - srcMappedRows;
                    committedTransientRowCount += delta;
                    transientRowsAdded -= delta;
                }

                // Assert that fixed column is mapped for row committedTransientRowCount
                assert primaryColumn.addressOf(committedTransientRowCount << shl) > 0;

            } else {
                // Variable length record column. It has 2 files:
                // Primary is the variable record length file
                // Secondary is fixed file with 64bit per record

                shl = 3;

                // Here both files have to be mapped into memory
                //
                // Step 1: process fixed record file in the same way as fixed size column above
                //
                long fixLenMappedRows = secondaryColumn.offsetInPage(secondaryColumn.getAppendOffset()) >> shl;
                if (fixLenMappedRows < transientRowsAdded) {
                    committedTransientRowCount += transientRowsAdded - fixLenMappedRows;
                    transientRowsAdded = fixLenMappedRows;
                }

                // Assert that secondary file is mapped for row committedTransientRowCount
                assert secondaryColumn.addressOf(committedTransientRowCount << shl) > 0;
                //
                // Step 2: check all rows in transientRowsAdded are mapped in variable record file
                //
                // Fixed record file (secondary) has offset records of the variable file
                //
                // Primary file:
                //
                // |   Page 1 (unmapped )     |     Page 2 (mapped)      |
                // | ========== I =========== | =================== > -- |
                // | record one | record 2 | r3 | value of record 3 |
                //
                //  Secondary file:
                //
                // | Page 1 (mapped )         |
                // | 0  | 14 | 25 | 30 | -----|
                //
                // here committedTransientRowCount == 0 and transientRowsAdded == 4

                // varFileCommittedOffset is record with 0 offset in the example and is 0
                long varFileCommittedOffset = secondaryColumn.getLong(committedTransientRowCount << shl);

                // varFileMappedOffset is 28 in the above example (offset in bytes where Page 2 of primary file starts)
                long varFileAppendOffset = primaryColumn.getAppendOffset();
                long varLenMappedBytes = primaryColumn.offsetInPage(primaryColumn.getAppendOffset());
                long varFileMappedOffset = varFileAppendOffset - varLenMappedBytes;

                // Check if all variable file records we want to move are mapped
                if (varFileCommittedOffset < varFileMappedOffset) {
                    // We need to binary search fix file
                    // to find the records where mapped page starts (Page 2 in the example, offset 28)
                    long firstMappedVarColRowOffset = Vect.binarySearch64Bit(
                            secondaryColumn.addressOf(committedTransientRowCount << shl), // this is address of memory of fix file where to start the search
                            varFileMappedOffset, // This is the value we search for (28 in case of example)
                            0, // start from index 0
                            fixLenMappedRows - 1, // and search in all mapped rows (inclusive of ixLenMappedRows - 1)
                            BinarySearch.SCAN_DOWN // doesn't matter
                    );

                    // In the example firstMappedVarColRowOffset expected to be -3 -1, e.g. no exact match found
                    // value 28 is between index 2 and 3 in the secondary file
                    if (firstMappedVarColRowOffset < 0) {
                        // convert it to index 3, the first index of the value >= 28
                        firstMappedVarColRowOffset = -firstMappedVarColRowOffset - 1;
                    }

                    // move transientRowsAdded to 1 and committedTransientRowCount to 3
                    transientRowsAdded -= firstMappedVarColRowOffset;
                    assert transientRowsAdded >= 0;
                    committedTransientRowCount += firstMappedVarColRowOffset;

                    // assert that secondary file is mapped for record committedTransientRowCount
                    assert primaryColumn.addressOf(secondaryColumn.getLong(committedTransientRowCount << shl)) > 0;
                }
            }
        }
        return transientRowsAdded;
    }

    void o3ClockDownPartitionUpdateCount() {
        o3PartitionUpdRemaining.decrementAndGet();
    }

    /**
     * Commits O3 data. Lag is optional. When 0 is specified the entire O3 segment is committed.
     *
     * @param lag interval in microseconds that determines the length of O3 segment that is not going to be
     *            committed to disk. The interval starts at max timestamp of O3 segment and ends <i>lag</i>
     *            microseconds before this timestamp.
     * @return <i>true</i> when commit has is a NOOP, e.g. no data has been committed to disk. <i>false</i> otherwise.
     */
    private boolean o3Commit(long lag) {
        o3RowCount = getO3RowCount();
        o3PartitionRemoveCandidates.clear();
        o3ErrorCount.set(0);
        o3ColumnCounters.clear();
        o3BasketPool.clear();

        long o3LagRowCount = 0;
        long maxUncommittedRows = metadata.getMaxUncommittedRows();

        final int timestampIndex = metadata.getTimestampIndex();
        this.lastPartitionTimestamp = timestampFloorMethod.floor(partitionTimestampHi);
        try {
            o3RowCount += o3MoveUncommitted(timestampIndex);

            // we may need to re-use file descriptors when this partition is the "current" one
            // we cannot open file again due to sharing violation
            //
            // to determine that 'ooTimestampLo' goes into current partition
            // we need to compare 'partitionTimestampHi', which is appropriately truncated to DAY/MONTH/YEAR
            // to this.maxTimestamp, which isn't truncated yet. So we need to truncate it first
            LOG.info().$("sorting o3 [table=").$(tableName).$(']').$();
            final long sortedTimestampsAddr = o3TimestampMem.addressOf(0);

            // ensure there is enough size
            if (o3RowCount > 600 || !o3QuickSortEnabled) {
                o3TimestampMemCpy.jumpTo(o3TimestampMem.getAppendOffset());
                Vect.radixSortLongIndexAscInPlace(sortedTimestampsAddr, o3RowCount, o3TimestampMemCpy.addressOf(0));
            } else {
                Vect.quickSortLongIndexAscInPlace(sortedTimestampsAddr, o3RowCount);
            }

            // we have three frames:
            // partition logical "lo" and "hi" - absolute bounds (partitionLo, partitionHi)
            // partition actual data "lo" and "hi" (dataLo, dataHi)
            // out of order "lo" and "hi" (indexLo, indexHi)

            long srcOooMax;
            final long o3TimestampMin = getTimestampIndexValue(sortedTimestampsAddr, 0);
            if (o3TimestampMin < Timestamps.O3_MIN_TS) {
                o3InError = true;
                throw CairoException.instance(0).put("timestamps before 1970-01-01 are not allowed for O3");
            }

            long o3TimestampMax = getTimestampIndexValue(sortedTimestampsAddr, o3RowCount - 1);
            if (o3TimestampMax < Timestamps.O3_MIN_TS) {
                o3InError = true;
                throw CairoException.instance(0).put("timestamps before 1970-01-01 are not allowed for O3");
            }

            // Safe check of the sort. No known way to reproduce
            assert o3TimestampMin <= o3TimestampMax;

            if (lag > 0) {
                long lagThresholdTimestamp = o3TimestampMax - lag;
                if (lagThresholdTimestamp >= o3TimestampMin) {
                    final long lagThresholdRow = Vect.boundedBinarySearchIndexT(
                            sortedTimestampsAddr,
                            lagThresholdTimestamp,
                            0,
                            o3RowCount - 1,
                            BinarySearch.SCAN_DOWN
                    );
                    o3LagRowCount = o3RowCount - lagThresholdRow - 1;
                    if (o3LagRowCount > maxUncommittedRows) {
                        o3LagRowCount = maxUncommittedRows;
                        srcOooMax = o3RowCount - maxUncommittedRows;
                    } else {
                        srcOooMax = lagThresholdRow + 1;
                    }
                } else {
                    o3LagRowCount = o3RowCount;
                    // This is a scenario where "lag" and "maxUncommitted" values do not work with the data
                    // in that the "lag" is larger than dictated "maxUncommitted". A simple plan here is to
                    // commit half of the lag.
                    if (o3LagRowCount > maxUncommittedRows) {
                        o3LagRowCount = maxUncommittedRows / 2;
                        srcOooMax = o3RowCount - o3LagRowCount;
                    } else {
                        srcOooMax = 0;
                    }
                }
                LOG.debug().$("o3 commit lag [table=").$(tableName)
                        .$(", lag=").$(lag)
                        .$(", maxUncommittedRows=").$(maxUncommittedRows)
                        .$(", o3max=").$ts(o3TimestampMax)
                        .$(", lagThresholdTimestamp=").$ts(lagThresholdTimestamp)
                        .$(", o3LagRowCount=").$(o3LagRowCount)
                        .$(", srcOooMax=").$(srcOooMax)
                        .$(", o3RowCount=").$(o3RowCount)
                        .I$();
            } else {
                LOG.debug()
                        .$("o3 commit no lag [table=").$(tableName)
                        .$(", o3RowCount=").$(o3RowCount)
                        .I$();
                srcOooMax = o3RowCount;
            }

            if (srcOooMax == 0) {
                return true;
            }

            // we could have moved the "srcOooMax" and hence we re-read the max timestamp
            o3TimestampMax = getTimestampIndexValue(sortedTimestampsAddr, srcOooMax - 1);
            // move uncommitted is liable to change max timestamp
            // however we need to identify last partition before max timestamp skips to NULL for example
            final long maxTimestamp = txFile.getMaxTimestamp();

            // we are going to use this soon to avoid double-copying lag data
//            final boolean yep = isAppendLastPartitionOnly(sortedTimestampsAddr, o3TimestampMax);

            // reshuffle all columns according to timestamp index
            o3Sort(sortedTimestampsAddr, timestampIndex, o3RowCount);
            LOG.info().$("sorted [table=").utf8(tableName).I$();

            this.o3DoneLatch.reset();
            this.o3PartitionUpdRemaining.set(0);
            boolean success = true;
            int latchCount = 0;
            long srcOoo = 0;
            boolean flattenTimestamp = true;
            int pCount = 0;
            try {
                while (srcOoo < srcOooMax) {
                    try {
                        final long srcOooLo = srcOoo;
                        final long o3Timestamp = getTimestampIndexValue(sortedTimestampsAddr, srcOoo);
                        final long srcOooHi;
                        final long srcOooTimestampCeil = timestampCeilMethod.ceil(o3Timestamp);
                        if (srcOooTimestampCeil < o3TimestampMax) {
                            srcOooHi = Vect.boundedBinarySearchIndexT(
                                    sortedTimestampsAddr,
                                    srcOooTimestampCeil,
                                    srcOoo,
                                    srcOooMax - 1,
                                    BinarySearch.SCAN_DOWN
                            );
                        } else {
                            srcOooHi = srcOooMax - 1;
                        }

                        final long partitionTimestamp = timestampFloorMethod.floor(o3Timestamp);
                        final boolean last = partitionTimestamp == lastPartitionTimestamp;
                        srcOoo = srcOooHi + 1;

                        final long srcDataSize;
                        final long srcNameTxn;
                        final int partitionIndex = txFile.findAttachedPartitionIndexByLoTimestamp(partitionTimestamp);
                        if (partitionIndex > -1) {
                            if (last) {
                                srcDataSize = txFile.transientRowCount;
                            } else {
                                srcDataSize = getPartitionSizeByIndex(partitionIndex);
                            }
                            srcNameTxn = getPartitionNameTxnByIndex(partitionIndex);
                        } else {
                            srcDataSize = -1;
                            srcNameTxn = -1;
                        }

                        final boolean append = last && (srcDataSize < 0 || o3Timestamp >= maxTimestamp);
                        LOG.debug().
                                $("o3 partition task [table=").$(tableName)
                                .$(", srcOooLo=").$(srcOooLo)
                                .$(", srcOooHi=").$(srcOooHi)
                                .$(", srcOooMax=").$(srcOooMax)
                                .$(", o3TimestampMin=").$ts(o3TimestampMin)
                                .$(", o3Timestamp=").$ts(o3Timestamp)
                                .$(", o3TimestampMax=").$ts(o3TimestampMax)
                                .$(", partitionTimestamp=").$ts(partitionTimestamp)
                                .$(", partitionIndex=").$(partitionIndex)
                                .$(", srcDataSize=").$(srcDataSize)
                                .$(", maxTimestamp=").$ts(maxTimestamp)
                                .$(", last=").$(last)
                                .$(", append=").$(append)
                                .$(", memUsed=").$(Unsafe.getMemUsed())
                                .I$();

                        pCount++;
                        o3PartitionUpdRemaining.incrementAndGet();
                        final O3Basket o3Basket = o3BasketPool.next();
                        o3Basket.ensureCapacity(columnCount, indexCount);

                        AtomicInteger columnCounter = o3ColumnCounters.next();
                        columnCounter.set(columnCount);
                        latchCount++;

                        if (append) {
                            Path pathToPartition = Path.getThreadLocal(this.path);
                            TableUtils.setPathForPartition(pathToPartition, partitionBy, o3TimestampMin, false);
                            TableUtils.txnPartitionConditionally(pathToPartition, srcNameTxn);
                            final int plen = pathToPartition.length();
                            for (int i = 0; i < columnCount; i++) {
                                final int colOffset = TableWriter.getPrimaryColumnIndex(i);
                                final boolean notTheTimestamp = i != timestampIndex;
                                final int columnType = metadata.getColumnType(i);
                                final CharSequence columnName = metadata.getColumnName(i);
                                final boolean isIndexed = metadata.isColumnIndexed(i);
                                final BitmapIndexWriter indexWriter = isIndexed ? getBitmapIndexWriter(i) : null;
                                final ContiguousVirtualMemory oooMem1 = o3Columns.getQuick(colOffset);
                                final ContiguousVirtualMemory oooMem2 = o3Columns.getQuick(colOffset + 1);
                                final AppendOnlyVirtualMemory mem1 = columns.getQuick(colOffset);
                                final AppendOnlyVirtualMemory mem2 = columns.getQuick(colOffset + 1);
                                final long srcDataTop = getColumnTop(i);
                                final long srcOooFixAddr;
                                final long srcOooFixSize;
                                final long srcOooVarAddr;
                                final long srcOooVarSize;
                                final AppendOnlyVirtualMemory dstFixMem;
                                final AppendOnlyVirtualMemory dstVarMem;
                                if (columnType != ColumnType.STRING && columnType != ColumnType.BINARY) {
                                    srcOooFixAddr = oooMem1.addressOf(0);
                                    srcOooFixSize = oooMem1.getAppendOffset();
                                    srcOooVarAddr = 0;
                                    srcOooVarSize = 0;
                                    dstFixMem = mem1;
                                    dstVarMem = null;
                                } else {
                                    srcOooFixAddr = oooMem2.addressOf(0);
                                    srcOooFixSize = oooMem2.getAppendOffset();
                                    srcOooVarAddr = oooMem1.addressOf(0);
                                    srcOooVarSize = oooMem1.getAppendOffset();
                                    dstFixMem = mem2;
                                    dstVarMem = mem1;
                                }

                                O3OpenColumnJob.appendLastPartition(
                                        pathToPartition,
                                        plen,
                                        columnName,
                                        columnCounter,
                                        notTheTimestamp ? columnType : -columnType,
                                        srcOooFixAddr,
                                        srcOooFixSize,
                                        srcOooVarAddr,
                                        srcOooVarSize,
                                        srcOooLo,
                                        srcOooHi,
                                        srcOooMax,
                                        o3TimestampMin,
                                        o3TimestampMax,
                                        partitionTimestamp,
                                        srcDataTop,
                                        Math.max(0, srcDataSize),
                                        isIndexed,
                                        dstFixMem,
                                        dstVarMem,
                                        this,
                                        indexWriter,
                                        tempMem16b
                                );
                            }
                        } else {
                            if (flattenTimestamp) {
                                Vect.flattenIndex(sortedTimestampsAddr, o3RowCount);
                                flattenTimestamp = false;
                            }
                            o3CommitPartitionAsync(
                                    columnCounter,
                                    maxTimestamp,
                                    sortedTimestampsAddr,
                                    srcOooMax,
                                    o3TimestampMin,
                                    o3TimestampMax,
                                    srcOooLo,
                                    srcOooHi,
                                    partitionTimestamp,
                                    last,
                                    srcDataSize,
                                    srcNameTxn,
                                    o3Basket
                            );
                        }
                    } catch (CairoException | CairoError e) {
                        LOG.error().$((Sinkable) e).$();
                        success = false;
                        throw e;
                    }
                }
            } finally {
                // we are stealing work here it is possible we get exception from this method
                LOG.debug()
                        .$("o3 expecting updates [table=").$(tableName)
                        .$(", partitionsPublished=").$(pCount)
                        .I$();

                o3ConsumePartitionUpdates(
                        srcOooMax,
                        o3TimestampMin,
                        o3TimestampMax
                );

                o3DoneLatch.await(latchCount);

                o3InError = !success || o3ErrorCount.get() > 0;
                if (success && o3ErrorCount.get() > 0) {
                    //noinspection ThrowFromFinallyBlock
                    throw CairoException.instance(0).put("bulk update failed and will be rolled back");
                }
            }

            if (o3LagRowCount > 0) {
                o3ShiftLagRowsUp(timestampIndex, o3LagRowCount, srcOooMax);
            }
        } finally {
            if (denseIndexers.size() == 0) {
                populateDenseIndexerList();
            }
            path.trimTo(rootLen);
            // Alright, we finished updating partitions. Now we need to get this writer instance into
            // a consistent state.
            //
            // We start with ensuring append memory is in ready-to-use state. When max timestamp changes we need to
            // move append memory to new set of files. Otherwise we stay on the same set but advance the append position.
            avoidIndexOnCommit = o3ErrorCount.get() == 0;
            if (o3LagRowCount == 0) {
                this.o3MasterRef = -1;
                rowFunction = switchPartitionFunction;
                row.activeColumns = columns;
                row.activeNullSetters = nullSetters;
                LOG.debug().$("lag segment is empty").$();
            } else {
                // adjust O3 master ref so that virtual row count becomes equal to value of "o3LagRowCount"
                this.o3MasterRef = this.masterRef - o3LagRowCount * 2 + 1;
                LOG.debug().$("adjusted [o3RowCount=").$(getO3RowCount()).I$();
            }
        }
        if (columns.getQuick(0).isClosed() || partitionTimestampHi < txFile.getMaxTimestamp()) {
            openPartition(txFile.getMaxTimestamp());
        }
        setAppendPosition(txFile.getTransientRowCount(), true);
        return false;
    }

    private void o3CommitPartitionAsync(
            AtomicInteger columnCounter,
            long maxTimestamp,
            long sortedTimestampsAddr,
            long srcOooMax,
            long oooTimestampMin,
            long oooTimestampMax,
            long srcOooLo,
            long srcOooHi,
            long partitionTimestamp,
            boolean last,
            long srcDataSize,
            long srcNameTxn,
            O3Basket o3Basket
    ) {
        long cursor = messageBus.getO3PartitionPubSeq().next();
        if (cursor > -1) {
            O3PartitionTask task = messageBus.getO3PartitionQueue().get(cursor);
            task.of(
                    path,
                    partitionBy,
                    columns,
                    o3Columns,
                    srcOooLo,
                    srcOooHi,
                    srcOooMax,
                    oooTimestampMin,
                    oooTimestampMax,
                    partitionTimestamp,
                    maxTimestamp,
                    srcDataSize,
                    srcNameTxn,
                    last,
                    getTxn(),
                    sortedTimestampsAddr,
                    this,
                    columnCounter,
                    o3Basket
            );
            messageBus.getO3PartitionPubSeq().done(cursor);
        } else {
            O3PartitionJob.processPartition(
                    path,
                    partitionBy,
                    columns,
                    o3Columns,
                    srcOooLo,
                    srcOooHi,
                    srcOooMax,
                    oooTimestampMin,
                    oooTimestampMax,
                    partitionTimestamp,
                    maxTimestamp,
                    srcDataSize,
                    srcNameTxn,
                    last,
                    getTxn(),
                    sortedTimestampsAddr,
                    this,
                    columnCounter,
                    o3Basket,
                    tempMem16b
            );
        }
    }

    private void o3ConsumePartitionUpdates(
            long srcOooMax,
            long timestampMin,
            long timestampMax
    ) {
        final Sequence partitionSubSeq = messageBus.getO3PartitionSubSeq();
        final RingQueue<O3PartitionTask> partitionQueue = messageBus.getO3PartitionQueue();
        final Sequence openColumnSubSeq = messageBus.getO3OpenColumnSubSeq();
        final RingQueue<O3OpenColumnTask> openColumnQueue = messageBus.getO3OpenColumnQueue();
        final Sequence copySubSeq = messageBus.getO3CopySubSeq();
        final RingQueue<O3CopyTask> copyQueue = messageBus.getO3CopyQueue();

        do {
            long cursor = o3PartitionUpdateSubSeq.next();
            if (cursor > -1) {
                final O3PartitionUpdateTask task = o3PartitionUpdateQueue.get(cursor);
                final long partitionTimestamp = task.getPartitionTimestamp();
                final long srcOooPartitionLo = task.getSrcOooPartitionLo();
                final long srcOooPartitionHi = task.getSrcOooPartitionHi();
                final long srcDataMax = task.getSrcDataMax();
                final boolean partitionMutates = task.isPartitionMutates();

                o3ClockDownPartitionUpdateCount();

                o3PartitionUpdateSubSeq.done(cursor);

                if (o3ErrorCount.get() == 0) {
                    o3PartitionUpdate(
                            timestampMin,
                            timestampMax,
                            partitionTimestamp,
                            srcOooPartitionLo,
                            srcOooPartitionHi,
                            srcOooMax,
                            srcDataMax,
                            partitionMutates
                    );
                }
                continue;
            }

            cursor = partitionSubSeq.next();
            if (cursor > -1) {
                final O3PartitionTask partitionTask = partitionQueue.get(cursor);
                if (partitionTask.getTableWriter() == this && o3ErrorCount.get() > 0) {
                    // do we need to free anything on the task?
                    partitionSubSeq.done(cursor);
                    o3ClockDownPartitionUpdateCount();
                    o3CountDownDoneLatch();
                } else {
                    o3ProcessPartitionSafe(partitionSubSeq, cursor, partitionTask);
                }
                continue;
            }

            cursor = openColumnSubSeq.next();
            if (cursor > -1) {
                O3OpenColumnTask openColumnTask = openColumnQueue.get(cursor);
                if (openColumnTask.getTableWriter() == this && o3ErrorCount.get() > 0) {
                    O3CopyJob.closeColumnIdle(
                            openColumnTask.getColumnCounter(),
                            openColumnTask.getTimestampMergeIndexAddr(),
                            openColumnTask.getSrcTimestampFd(),
                            openColumnTask.getSrcTimestampAddr(),
                            openColumnTask.getSrcTimestampSize(),
                            this
                    );
                    openColumnSubSeq.done(cursor);
                } else {
                    o3OpenColumnSafe(openColumnSubSeq, cursor, openColumnTask);
                }
                continue;
            }

            cursor = copySubSeq.next();
            if (cursor > -1) {
                O3CopyTask copyTask = copyQueue.get(cursor);
                if (copyTask.getTableWriter() == this && o3ErrorCount.get() > 0) {
                    O3CopyJob.copyIdle(
                            copyTask.getColumnCounter(),
                            copyTask.getPartCounter(),
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
                            this
                    );
                    copySubSeq.done(cursor);
                } else {
                    o3CopySafe(cursor);
                }
            }
        } while (this.o3PartitionUpdRemaining.get() > 0);
    }

    private void o3CopySafe(
            long cursor
    ) {
        final O3CopyTask task = messageBus.getO3CopyQueue().get(cursor);
        try {
            O3CopyJob.copy(
                    task,
                    cursor,
                    messageBus.getO3CopySubSeq()
            );
        } catch (CairoException | CairoError e) {
            LOG.error().$((Sinkable) e).$();
        } catch (Throwable e) {
            LOG.error().$(e).$();
        }
    }

    void o3CountDownDoneLatch() {
        o3DoneLatch.countDown();
    }

    private void o3MoveLag0(
            int columnIndex,
            final int columnType,
            long o3LagRowCount,
            long o3RowCount
    ) {
        if (columnIndex > -1) {
            ContiguousVirtualMemory o3DataMem = o3Columns.get(getPrimaryColumnIndex(columnIndex));
            ContiguousVirtualMemory o3IndexMem = o3Columns.get(getSecondaryColumnIndex(columnIndex));

            long size;
            long sourceOffset;
            final int shl = ColumnType.pow2SizeOf(columnType);
            if (null == o3IndexMem) {
                // Fixed size column
                sourceOffset = o3RowCount << shl;
                size = o3LagRowCount << shl;
            } else {
                // Var size column
                sourceOffset = o3IndexMem.getLong(o3RowCount * 8);
                size = o3DataMem.getAppendOffset() - sourceOffset;
                O3Utils.shiftCopyFixedSizeColumnData(
                        sourceOffset,
                        o3IndexMem.addressOf(o3RowCount * 8),
                        0,
                        o3LagRowCount,
                        o3IndexMem.addressOf(0)
                );
                o3IndexMem.jumpTo(o3LagRowCount * 8);
            }

            o3DataMem.jumpTo(size);
            Vect.memmove(o3DataMem.addressOf(0), o3DataMem.addressOf(sourceOffset), size);
        } else {
            // Special case, designated timestamp column
            // Move values and set index to  0..o3LagRowCount
            final long sourceOffset = o3RowCount * 16;
            final long mergeMemAddr = o3TimestampMem.addressOf(0);
            Vect.shiftTimestampIndex(mergeMemAddr + sourceOffset, o3LagRowCount, mergeMemAddr);
            o3TimestampMem.jumpTo(o3LagRowCount * 16);
        }
    }

    private void o3SetAppendOffset(
            int columnIndex,
            final int columnType,
            long o3RowCount
    ) {
        if (columnIndex != metadata.getTimestampIndex()) {
            ContiguousVirtualMemory o3DataMem = o3Columns.get(getPrimaryColumnIndex(columnIndex));
            ContiguousVirtualMemory o3IndexMem = o3Columns.get(getSecondaryColumnIndex(columnIndex));

            long size;
            if (null == o3IndexMem) {
                // Fixed size column
                size = o3RowCount << ColumnType.pow2SizeOf(columnType);
            } else {
                // Var size column
                size = o3IndexMem.getLong(o3RowCount * 8);
                o3IndexMem.jumpTo(o3RowCount * 8);
            }

            o3DataMem.jumpTo(size);
        } else {
            // Special case, designated timestamp column
            o3TimestampMem.jumpTo(o3RowCount * 16);
        }
    }

    private long o3MoveUncommitted(final int timestampIndex) {
        final long committedRowCount = txFile.getCommittedFixedRowCount() + txFile.getCommittedTransientRowCount();
        final long rowsAdded = txFile.getRowCount() - committedRowCount;
        final long committedTransientRowCount = txFile.getTransientRowCount() - Math.min(txFile.getTransientRowCount(), rowsAdded);
        if (Math.min(txFile.getTransientRowCount(), rowsAdded) > 0) {
            LOG.debug()
                    .$("o3 move uncommitted [table=").$(tableName)
                    .$(", transientRowsAdded=").$(Math.min(txFile.getTransientRowCount(), rowsAdded))
                    .I$();
            return o3ScheduleMoveUncommitted0(
                    timestampIndex,
                    Math.min(txFile.getTransientRowCount(), rowsAdded),
                    committedTransientRowCount
            );
        }
        return 0;
    }

    private void o3MoveUncommitted0(
            int colIndex,
            int columnType,
            long committedTransientRowCount,
            long transientRowsAdded
    ) {
        if (colIndex > -1) {
            AppendOnlyVirtualMemory srcDataMem = getPrimaryColumn(colIndex);
            int shl = ColumnType.pow2SizeOf(columnType);
            long srcFixOffset;
            final ContiguousVirtualMemory o3DataMem = o3Columns.get(getPrimaryColumnIndex(colIndex));
            final ContiguousVirtualMemory o3IndexMem = o3Columns.get(getSecondaryColumnIndex(colIndex));

            long extendedSize;
            long dstVarOffset = o3DataMem.getAppendOffset();

            if (null == o3IndexMem) {
                // Fixed size
                extendedSize = transientRowsAdded << shl;
                srcFixOffset = committedTransientRowCount << shl;
            } else {
                // Var size
                final AppendOnlyVirtualMemory srcFixMem = getSecondaryColumn(colIndex);
                long srcVarOffset = srcFixMem.getLong(committedTransientRowCount * Long.BYTES);
                // ensure memory is available
                long dstAppendOffset = o3IndexMem.getAppendOffset();
                o3IndexMem.jumpTo(o3IndexMem.getAppendOffset() + transientRowsAdded * Long.BYTES);

                O3Utils.shiftCopyFixedSizeColumnData(
                        srcVarOffset - dstVarOffset,
                        srcFixMem.addressOf(committedTransientRowCount * Long.BYTES),
                        0,
                        transientRowsAdded,
                        o3IndexMem.addressOf(dstAppendOffset)
                );
                long sourceEndOffset = srcDataMem.getAppendOffset();
                extendedSize = sourceEndOffset - srcVarOffset;
                srcFixOffset = srcVarOffset;
                srcFixMem.jumpTo(committedTransientRowCount * Long.BYTES);
            }

            o3DataMem.jumpTo(dstVarOffset + extendedSize);
            long appendAddress = o3DataMem.addressOf(dstVarOffset);
            long sourceAddress = srcDataMem.addressOf(srcFixOffset);
            Vect.memcpy(sourceAddress, appendAddress, extendedSize);
            srcDataMem.jumpTo(srcFixOffset);
        } else {
            // Timestamp column
            colIndex = -colIndex - 1;
            int shl = ColumnType.pow2SizeOf(ColumnType.TIMESTAMP);
            AppendOnlyVirtualMemory srcDataMem = getPrimaryColumn(colIndex);
            long srcFixOffset = committedTransientRowCount << shl;
            for (long n = 0; n < transientRowsAdded; n++) {
                long ts = srcDataMem.getLong(srcFixOffset + (n << shl));
                o3TimestampMem.putLong128(ts, o3RowCount + n);
            }
            srcDataMem.jumpTo(srcFixOffset);
        }
    }

    private void o3OpenColumnSafe(Sequence openColumnSubSeq, long cursor, O3OpenColumnTask openColumnTask) {
        try {
            O3OpenColumnJob.openColumn(openColumnTask, cursor, openColumnSubSeq, tempMem16b);
        } catch (CairoException | CairoError e) {
            LOG.error().$((Sinkable) e).$();
        } catch (Throwable e) {
            LOG.error().$(e).$();
        }
    }

    private void o3OpenColumns() {
        for (int i = 0; i < columnCount; i++) {
            ContiguousVirtualMemory mem1 = o3Columns.getQuick(getPrimaryColumnIndex(i));
            mem1.jumpTo(0);
            ContiguousVirtualMemory mem2 = o3Columns.getQuick(getSecondaryColumnIndex(i));
            if (mem2 != null) {
                mem2.jumpTo(0);
            }
        }
        row.activeColumns = o3Columns;
        row.activeNullSetters = o3NullSetters;
        LOG.debug().$("switched partition to memory").$();
    }

    private void o3PartitionUpdate(
            long timestampMin,
            long timestampMax,
            long partitionTimestamp,
            long srcOooPartitionLo,
            long srcOooPartitionHi,
            long srcOooMax,
            long srcDataMax,
            boolean partitionMutates
    ) {
        this.txFile.minTimestamp = Math.min(timestampMin, this.txFile.minTimestamp);
        final long partitionSize = srcDataMax + srcOooPartitionHi - srcOooPartitionLo + 1;
        final long rowDelta = srcOooPartitionHi - srcOooMax;
        if (partitionTimestamp < lastPartitionTimestamp) {
            this.txFile.fixedRowCount += partitionSize - srcDataMax;
            // when we exit here we need to rollback transientRowCount we've been incrementing
            // while adding out-of-order data
        } else if (rowDelta < -1) {
            this.txFile.fixedRowCount += partitionSize;
        } else {
            // this is last partition
            this.txFile.transientRowCount = partitionSize;
            this.txFile.maxTimestamp = Math.max(this.txFile.maxTimestamp, timestampMax);
        }

        final int partitionIndex = txFile.findAttachedPartitionIndexByLoTimestamp(partitionTimestamp);
        if (partitionTimestamp == lastPartitionTimestamp) {
            if (partitionMutates) {
                closeActivePartition(true);
            } else if (rowDelta < -1) {
                closeActivePartition(partitionSize);
            } else {
                setAppendPosition(partitionSize, false);
            }
        }

        if (partitionMutates) {
            final long srcDataTxn = txFile.getPartitionNameTxnByIndex(partitionIndex);
            LOG.info()
                    .$("merged partition [table=`").utf8(tableName)
                    .$("`, ts=").$ts(partitionTimestamp)
                    .$(", txn=").$(txFile.txn).$(']').$();
            txFile.updatePartitionSizeByIndexAndTxn(partitionIndex, partitionSize);
            o3PartitionRemoveCandidates.add(partitionTimestamp);
            o3PartitionRemoveCandidates.add(srcDataTxn);
            txFile.bumpPartitionTableVersion();
        } else {
            if (partitionTimestamp != lastPartitionTimestamp) {
                txFile.bumpPartitionTableVersion();
            }
            txFile.updatePartitionSizeByIndex(partitionIndex, partitionTimestamp, partitionSize);
        }
    }

    synchronized void o3PartitionUpdateSynchronized(
            long timestampMin,
            long timestampMax,
            long partitionTimestamp,
            long srcOooPartitionLo,
            long srcOooPartitionHi,
            boolean partitionMutates,
            long srcOooMax,
            long srcDataMax
    ) {
        o3ClockDownPartitionUpdateCount();
        o3PartitionUpdate(
                timestampMin,
                timestampMax,
                partitionTimestamp,
                srcOooPartitionLo,
                srcOooPartitionHi,
                srcOooMax,
                srcDataMax,
                partitionMutates
        );
    }

    private void o3ProcessPartitionRemoveCandidates() {
        try {
            final int n = o3PartitionRemoveCandidates.size();
            if (n > 0) {
                o3ProcessPartitionRemoveCandidates0(n);
            }
        } finally {
            o3PartitionRemoveCandidates.clear();
        }
    }

    private void o3ProcessPartitionRemoveCandidates0(int n) {
        final long readerTxn = txnScoreboard.getMin();
        final long readerTxnCount = txnScoreboard.getActiveReaderCount(readerTxn);
        if (txnScoreboard.isTxnAvailable(txFile.getTxn() - 1)) {
            for (int i = 0; i < n; i += 2) {
                final long timestamp = o3PartitionRemoveCandidates.getQuick(i);
                final long txn = o3PartitionRemoveCandidates.getQuick(i + 1);
                try {
                    setPathForPartition(
                            other,
                            partitionBy,
                            timestamp,
                            false
                    );
                    TableUtils.txnPartitionConditionally(other, txn);
                    other.slash$();
                    int errno;
                    if ((errno = ff.rmdir(other)) == 0) {
                        LOG.info().$(
                                "purged [path=").$(other)
                                .$(", readerTxn=").$(readerTxn)
                                .$(", readerTxnCount=").$(readerTxnCount)
                                .$(']').$();
                    } else {
                        LOG.info()
                                .$("queued to purge [errno=").$(errno)
                                .$(", table=").$(tableName)
                                .$(", ts=").$ts(timestamp)
                                .$(", txn=").$(txn)
                                .$(']').$();
                        o3QueuePartitionForPurge(timestamp, txn);
                    }
                } finally {
                    other.trimTo(rootLen);
                }
            }
        } else {
            // queue all updated partitions
            for (int i = 0; i < n; i += 2) {
                o3QueuePartitionForPurge(
                        o3PartitionRemoveCandidates.getQuick(i),
                        o3PartitionRemoveCandidates.getQuick(i + 1)
                );
            }
        }
    }

    private void o3ProcessPartitionSafe(Sequence partitionSubSeq, long cursor, O3PartitionTask partitionTask) {
        try {
            O3PartitionJob.processPartition(tempMem16b, partitionTask, cursor, partitionSubSeq);
        } catch (CairoException | CairoError e) {
            LOG.error().$((Sinkable) e).$();
        } catch (Throwable e) {
            LOG.error().$(e).$();
        }
    }

    private void o3QueuePartitionForPurge(long timestamp, long txn) {
        final MPSequence seq = messageBus.getO3PurgeDiscoveryPubSeq();
        long cursor = seq.next();
        if (cursor > -1) {
            O3PurgeDiscoveryTask task = messageBus.getO3PurgeDiscoveryQueue().get(cursor);
            task.of(
                    tableName,
                    partitionBy,
                    txnScoreboard,
                    timestamp,
                    txn
            );
            seq.done(cursor);
        } else {
            LOG.error()
                    .$("could not purge [errno=").$(ff.errno())
                    .$(", table=").$(tableName)
                    .$(", ts=").$ts(timestamp)
                    .$(", txn=").$(txn)
                    .$(']').$();
        }
    }

    private long o3ScheduleMoveUncommitted0(int timestampIndex, long transientRowsAdded, long committedTransientRowCount) {
        long transientRowsAddedNew = o3CalculatedMoveUncommittedSize(transientRowsAdded, committedTransientRowCount);
        long delta = transientRowsAdded - transientRowsAddedNew;
        assert delta >= 0;

        if (delta > 0) {
            // Not all uncommitted rows can be moved to O3 staging memory
            // because column files are not fully mapped to memory
            // reduce number of rows to move
            transientRowsAdded -= delta;
            committedTransientRowCount += delta;
        }

        if (transientRowsAdded > 0) {

            long maxCommittedTimestamp = 0;
            if (delta > 0) {
                // If there are rows to move
                // and we cannot move all uncommitted rows to o3 memory
                // we have to set maxCommittedTimestamp in tx file
                AppendOnlyVirtualMemory timestampColumn = getPrimaryColumn(timestampIndex);
                if (!timestampColumn.isMapped((committedTransientRowCount - 1) << 3, Long.BYTES)) {
                    // Need to leave one more record in column files
                    // to correctly get max timestamp
                    transientRowsAdded--;
                    committedTransientRowCount++;
                }
                maxCommittedTimestamp = timestampColumn.getLong((committedTransientRowCount - 1) << 3);
            }

            final Sequence pubSeq = this.messageBus.getO3CallbackPubSeq();
            final RingQueue<O3CallbackTask> queue = this.messageBus.getO3CallbackQueue();
            o3PendingCallbackTasks.clear();
            o3DoneLatch.reset();
            int queuedCount = 0;

            for (int colIndex = 0; colIndex < columnCount; colIndex++) {
                int columnType = metadata.getColumnType(colIndex);
                int columnIndex = colIndex != timestampIndex ? colIndex : -colIndex - 1;

                long cursor = pubSeq.next();

                // Pass column index as -1 when it's designated timestamp column to o3 move method
                if (cursor > -1) {
                    try {
                        final O3CallbackTask task = queue.get(cursor);
                        task.of(
                                o3DoneLatch,
                                columnIndex,
                                columnType,
                                committedTransientRowCount,
                                transientRowsAdded,
                                this.o3MoveUncommittedRef
                        );

                        o3PendingCallbackTasks.add(task);
                    } finally {
                        queuedCount++;
                        pubSeq.done(cursor);
                    }
                } else {
                    o3MoveUncommitted0(columnIndex, columnType, committedTransientRowCount, transientRowsAdded);
                }
            }

            for (int n = o3PendingCallbackTasks.size() - 1; n > -1; n--) {
                final O3CallbackTask task = o3PendingCallbackTasks.getQuick(n);
                if (task.tryLock()) {
                    O3CallbackJob.runCallbackWithCol(
                            task,
                            -1,
                            null
                    );
                }
            }

            o3DoneLatch.await(queuedCount);
            if (delta == 0) {
                txFile.resetToLastPartition(committedTransientRowCount);
            } else {
                // If transientRowsAdded is decreased because uncommitted area is not mapped
                // maxCommittedTimestamp the last value of the segment left in files
                txFile.resetToLastPartition(committedTransientRowCount, maxCommittedTimestamp);
            }
        }
        return transientRowsAdded;
    }

    private void o3ShiftLagRowsUp(int timestampIndex, long o3LagRowCount, long o3RowCount) {
        o3PendingCallbackTasks.clear();

        final Sequence pubSeq = this.messageBus.getO3CallbackPubSeq();
        final RingQueue<O3CallbackTask> queue = this.messageBus.getO3CallbackQueue();

        o3DoneLatch.reset();
        int queuedCount = 0;
        for (int colIndex = 0; colIndex < columnCount; colIndex++) {
            int columnType = metadata.getColumnType(colIndex);
            int columnIndex = colIndex != timestampIndex ? colIndex : -colIndex - 1;
            long cursor = pubSeq.next();

            // Pass column index as -1 when it's designated timestamp column to o3 move method
            if (cursor > -1) {
                try {
                    final O3CallbackTask task = queue.get(cursor);
                    task.of(
                            o3DoneLatch,
                            columnIndex,
                            columnType,
                            o3LagRowCount,
                            o3RowCount,
                            this.o3MoveLagRef
                    );

                    o3PendingCallbackTasks.add(task);
                } finally {
                    queuedCount++;
                    pubSeq.done(cursor);
                }
            } else {
                o3MoveLag0(columnIndex, columnType, o3LagRowCount, o3RowCount);
            }
        }

        for (int n = o3PendingCallbackTasks.size() - 1; n > -1; n--) {
            final O3CallbackTask task = o3PendingCallbackTasks.getQuick(n);
            if (task.tryLock()) {
                O3CallbackJob.runCallbackWithCol(
                        task,
                        -1,
                        null
                );
            }
        }

        o3DoneLatch.await(queuedCount);
    }

    private void o3Sort(long mergedTimestamps, int timestampIndex, long rowCount) {
        o3PendingCallbackTasks.clear();

        final Sequence pubSeq = this.messageBus.getO3CallbackPubSeq();
        final RingQueue<O3CallbackTask> queue = this.messageBus.getO3CallbackQueue();

        o3DoneLatch.reset();
        int queuedCount = 0;
        for (int i = 0; i < columnCount; i++) {
            if (timestampIndex != i) {
                final int type = metadata.getColumnType(i);
                long cursor = pubSeq.next();
                if (cursor > -1) {
                    try {
                        final O3CallbackTask task = queue.get(cursor);
                        task.of(
                                o3DoneLatch,
                                i,
                                type,
                                mergedTimestamps,
                                rowCount,
                                type == ColumnType.STRING || type == ColumnType.BINARY ? oooSortVarColumnRef : oooSortFixColumnRef
                        );
                        o3PendingCallbackTasks.add(task);
                    } finally {
                        queuedCount++;
                        pubSeq.done(cursor);
                    }
                } else {
                    o3SortColumn(mergedTimestamps, i, type, rowCount);
                }
            }
        }

        for (int n = o3PendingCallbackTasks.size() - 1; n > -1; n--) {
            final O3CallbackTask task = o3PendingCallbackTasks.getQuick(n);
            if (task.tryLock()) {
                O3CallbackJob.runCallbackWithCol(
                        task,
                        -1,
                        null
                );
            }
        }

        o3DoneLatch.await(queuedCount);
    }

    private void o3SortColumn(long mergedTimestamps, int i, int type, long rowCount) {
        switch (type) {
            case ColumnType.BINARY:
            case ColumnType.STRING:
                o3SortVarColumn(i, type, mergedTimestamps, rowCount);
                break;
            default:
                o3SortFixColumn(i, type, mergedTimestamps, rowCount);
                break;
        }
    }

    private void o3SortFixColumn(
            int columnIndex,
            final int columnType,
            long mergedTimestampsAddr,
            long valueCount
    ) {
        final int columnOffset = getPrimaryColumnIndex(columnIndex);
        final ContiguousVirtualMemory mem = o3Columns.getQuick(columnOffset);
        final ContiguousVirtualMemory mem2 = o3Columns2.getQuick(columnOffset);
        final long src = mem.addressOf(0);
        final long srcSize = mem.size();
        final int shl = ColumnType.pow2SizeOf(columnType);
        mem2.jumpTo(valueCount << shl);
        final long tgtDataAddr = mem2.addressOf(0);
        final long tgtDataSize = mem2.size();
        switch (shl) {
            case 0:
                Vect.indexReshuffle8Bit(src, tgtDataAddr, mergedTimestampsAddr, valueCount);
                break;
            case 1:
                Vect.indexReshuffle16Bit(src, tgtDataAddr, mergedTimestampsAddr, valueCount);
                break;
            case 2:
                Vect.indexReshuffle32Bit(src, tgtDataAddr, mergedTimestampsAddr, valueCount);
                break;
            case 3:
                Vect.indexReshuffle64Bit(src, tgtDataAddr, mergedTimestampsAddr, valueCount);
                break;
            case 5:
                Vect.indexReshuffle256Bit(src, tgtDataAddr, mergedTimestampsAddr, valueCount);
                break;
            default:
                assert false : "col type is unsupported";
                break;
        }
        mem.replacePage(tgtDataAddr, tgtDataSize);
        mem2.replacePage(src, srcSize);
    }

    private void o3SortVarColumn(
            int columnIndex,
            int columnType,
            long mergedTimestampsAddr,
            long valueCount
    ) {
        final int primaryIndex = getPrimaryColumnIndex(columnIndex);
        final int secondaryIndex = primaryIndex + 1;
        final ContiguousVirtualMemory dataMem = o3Columns.getQuick(primaryIndex);
        final ContiguousVirtualMemory indexMem = o3Columns.getQuick(secondaryIndex);
        final ContiguousVirtualMemory dataMem2 = o3Columns2.getQuick(primaryIndex);
        final ContiguousVirtualMemory indexMem2 = o3Columns2.getQuick(secondaryIndex);
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
        final long offset = Vect.sortVarColumn(
                mergedTimestampsAddr,
                valueCount,
                srcDataAddr,
                srcIndxAddr,
                tgtDataAddr,
                tgtIndxAddr
        );
        dataMem.replacePage(tgtDataAddr, tgtDataSize);
        indexMem.replacePage(tgtIndxAddr, tgtIndxSize);
        dataMem2.replacePage(srcDataAddr, srcDataSize);
        indexMem2.replacePage(srcIndxAddr, srcIndxSize);
        dataMem.jumpTo(offset);
        indexMem.jumpTo(valueCount * Long.BYTES);
    }

    private void o3TimestampSetter(long timestamp) {
        o3TimestampMem.putLong128(timestamp, getO3RowCount());
    }

    private void openColumnFiles(CharSequence name, int i, int plen) {
        AppendOnlyVirtualMemory mem1 = getPrimaryColumn(i);
        AppendOnlyVirtualMemory mem2 = getSecondaryColumn(i);

        try {
            mem1.of(ff, dFile(path.trimTo(plen), name), configuration.getAppendPageSize());
            if (mem2 != null) {
                mem2.of(ff, iFile(path.trimTo(plen), name), configuration.getAppendPageSize());
            }
        } finally {
            path.trimTo(plen);
        }
    }

    private void openFirstPartition(long timestamp) {
        openPartition(repairDataGaps(timestamp));
        setAppendPosition(txFile.getTransientRowCount(), true);
        if (performRecovery) {
            performRecovery();
        }
        txFile.openFirstPartition(timestamp);
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
            if (ff.mkdirs(path.slash$(), mkDirMode) != 0) {
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
            LOG.info().$("switched partition [path='").$(path).$("']").$();
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
                    todoMem.putLong(8, configuration.getDatabaseIdLo());
                    todoMem.putLong(16, configuration.getDatabaseIdHi());
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
            removeNonAttachedPartitions();
        }
    }

    private long readMinTimestamp(long partitionTimestamp) {
        setStateForTimestamp(other, partitionTimestamp, false);
        try {
            dFile(other, metadata.getColumnName(metadata.getTimestampIndex()));
            if (ff.exists(other)) {
                // read min timestamp value
                final long fd = TableUtils.openRO(ff, other, LOG);
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
        recoverFromTodoWriteFailure(columnName);
        clearTodoLog();
    }

    private void recoverFromSymbolMapWriterFailure(CharSequence columnName) {
        removeSymbolMapFilesQuiet(columnName);
        removeMetaFile();
        recoverFromSwapRenameFailure(columnName);
    }

    private void recoverFromTodoWriteFailure(CharSequence columnName) {
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
        freeAndRemoveColumnPair(columns, pi, si);
        freeAndRemoveColumnPair(o3Columns, pi, si);
        freeAndRemoveColumnPair(o3Columns2, pi, si);
        columnTops.removeIndex(columnIndex);
        nullSetters.remove(columnIndex);
        o3NullSetters.remove(columnIndex);
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
            copyVersionAndLagValues();
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

    private void removeNonAttachedPartitions() {
        LOG.info().$("purging non attached partitions [path=").$(path.$()).$(']').$();
        try {
            ff.iterateDir(path.$(), removePartitionDirsNotAttached);
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
        int errno;
        if (IGNORED_FILES.excludes(nativeLPSZ) && type == Files.DT_DIR && (errno = ff.rmdir(path)) != 0) {
            LOG.info().$("could not remove [path=").$(path).$(", errno=").$(errno).$(']').$();
        }
    }

    private void removePartitionDirsNotAttached(long pName, int type) {
        nativeLPSZ.of(pName);
        if (!isDots(nativeLPSZ) && type == Files.DT_DIR) {
            if (Chars.endsWith(nativeLPSZ, DETACHED_DIR_MARKER)) {
                // Do not remove detached partitions
                // They are probably about to be attached.
                return;
            }
            try {
                long txn = 0;
                int txnSep = Chars.indexOf(nativeLPSZ, '.');
                if (txnSep < 0) {
                    txnSep = nativeLPSZ.length();
                } else {
                    txn = Numbers.parseLong(nativeLPSZ, txnSep + 1, nativeLPSZ.length());
                }
                long dirTimestamp = partitionDirFmt.parse(nativeLPSZ, 0, txnSep, null);
                if (txn <= txFile.txn &&
                        (txFile.attachedPartitionsContains(dirTimestamp) || txFile.isActivePartition(dirTimestamp))) {
                    return;
                }
            } catch (NumericException ignore) {
                // not a date?
                // ignore exception and remove directory
                // we rely on this behaviour to remove leftover directories created by OOO processing
            }
            path.trimTo(rootLen);
            path.concat(pName).$();
            int errno;
            if ((errno = ff.rmdir(path)) == 0) {
                LOG.info().$("removed partition dir: ").$(path).$();
            } else {
                LOG.error().$("cannot remove: ").$(path).$(" [errno=").$(errno).$(']').$();
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
            copyVersionAndLagValues();
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

    private long repairDataGaps(final long timestamp) {
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
                        .$("actual table size has been adjusted [name=`").utf8(tableName).$('`')
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

            TableUtils.renameOrFail(ff, path, other.concat(META_FILE_NAME).$());
        } finally {
            path.trimTo(rootLen);
            other.trimTo(rootLen);
        }
    }

    private void rollbackIndexes() {
        final long maxRow = txFile.getTransientRowCount() - 1;
        for (int i = 0, n = denseIndexers.size(); i < n; i++) {
            ColumnIndexer indexer = denseIndexers.getQuick(i);
            long fd = indexer.getFd();
            LOG.info().$("recovering index [fd=").$(fd).$(']').$();
            if (fd > -1) {
                indexer.rollback(maxRow);
            }
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

    private void setO3AppendPosition(final long position) {
        for (int i = 0; i < columnCount; i++) {
            o3SetAppendOffset(i, metadata.getColumnType(i), position);
        }
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
        final long partitionTimestampHi = TableUtils.setPathForPartition(path, partitionBy, timestamp, true);
        TableUtils.txnPartitionConditionally(
                path,
                txFile.getPartitionNameTxnByPartitionTimestamp(partitionTimestampHi)
        );
        if (updatePartitionInterval) {
            this.partitionTimestampHi = partitionTimestampHi;
        }
    }

    void startAppendedBlock(long timestampLo, long timestampHi, long nRowsAdded, LongList blockColumnTops) {
        if (timestampLo < txFile.getMaxTimestamp()) {
            throw CairoException.instance(ff.errno()).put("Cannot insert rows out of order. Table=").put(path);
        }

        if (txFile.getAppendedPartitionCount() == 0) {
            openFirstPartition(timestampLo);
        }

        if (partitionBy != PartitionBy.NONE && timestampLo > partitionTimestampHi) {
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
                        TableUtils.setPathForPartition(path, partitionBy, timestampLo, false);
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
        txFile.switchPartitions(timestamp);
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
        for (int i = 0, n = denseIndexers.size(); i < n; i++) {
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
            writeRestoreMetaTodo();
        } catch (CairoException e) {
            runFragile(RECOVER_FROM_TODO_WRITE_FAILURE, columnName, e);
        }
    }

    private void writeRestoreMetaTodo() {
        todoMem.putLong(0, ++todoTxn); // write txn, reader will first read txn at offset 24 and then at offset 0
        Unsafe.getUnsafe().storeFence(); // make sure we do not write hash before writing txn (view from another thread)
        todoMem.putLong(8, configuration.getDatabaseIdLo()); // write out our instance hashes
        todoMem.putLong(16, configuration.getDatabaseIdHi());
        Unsafe.getUnsafe().storeFence();
        todoMem.putLong(32, 1);
        todoMem.putLong(40, TODO_RESTORE_META);
        todoMem.putLong(48, metaPrevIndex);
        Unsafe.getUnsafe().storeFence();
        todoMem.putLong(24, todoTxn);
        todoMem.setSize(56);
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
    public interface O3ColumnUpdateMethod {
        void run(
                int columnIndex,
                final int columnType,
                long mergedTimestampsAddr,
                long valueCount
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
            if (timestamp >= Timestamps.O3_MIN_TS) {
                txFile.setMinTimestamp(timestamp);
                openFirstPartition(timestamp);
                return (rowFunction = switchPartitionFunction).newRow(timestamp);
            }
            throw CairoException.instance(0).put("timestamp before 1970-01-01 is not allowed");
        }
    }

    private class NoPartitionFunction implements RowFunction {
        @Override
        public Row newRow(long timestamp) {
            bumpMasterRef();
            txFile.append();
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
            txFile.append();
            return row;
        }
    }

    private class O3PartitionFunction implements RowFunction {
        @Override
        public Row newRow(long timestamp) {
            bumpMasterRef();
            o3TimestampSetter(timestamp);
            return row;
        }
    }

    private class SwitchPartitionRowFunction implements RowFunction {
        @Override
        public Row newRow(long timestamp) {
            bumpMasterRef();
            if (timestamp > partitionTimestampHi || timestamp < txFile.getMaxTimestamp()) {
                return newRow0(timestamp);
            }
            updateMaxTimestamp(timestamp);
            txFile.append();
            return row;
        }

        @NotNull
        private Row newRow0(long timestamp) {
            if (timestamp < txFile.getMaxTimestamp()) {
                return newRowO3(timestamp);
            }

            if (timestamp > partitionTimestampHi && partitionBy != PartitionBy.NONE) {
                switchPartition(timestamp);
            }

            updateMaxTimestamp(timestamp);
            txFile.append();
            return row;
        }

        private Row newRowO3(long timestamp) {
            LOG.info().$("switched to o3 [table=").utf8(tableName).$(']').$();
            txFile.beginPartitionSizeUpdate();
            o3OpenColumns();
            o3InError = false;
            o3MasterRef = masterRef;
            o3TimestampSetter(timestamp);
            rowFunction = o3RowFunction;
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
                l = value != null ? IntervalUtils.parseFloorPartialDate(value) : Numbers.LONG_NaN;
            } catch (NumericException e) {
                throw CairoException.instance(0).put("Invalid timestamp: ").put(value);
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
        public void handleTransientSymbolCountChange(int symbolCount) {
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
