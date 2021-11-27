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
import io.questdb.MessageBusImpl;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.MemoryFCRImpl;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.*;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.*;
import io.questdb.std.*;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.tasks.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;

import static io.questdb.cairo.StatusCode.*;
import static io.questdb.cairo.TableUtils.*;

public class TableWriter implements Closeable {
    public static final int TIMESTAMP_MERGE_ENTRY_BYTES = Long.BYTES * 2;
    public static final int O3_BLOCK_NONE = -1;
    public static final int O3_BLOCK_O3 = 1;
    public static final int O3_BLOCK_DATA = 2;
    public static final int O3_BLOCK_MERGE = 3;
    private static final int ROW_ACTION_OPEN_PARTITION = 0;
    private static final int ROW_ACTION_NO_PARTITION = 1;
    private static final int ROW_ACTION_NO_TIMESTAMP = 2;
    private static final int ROW_ACTION_O3 = 3;
    private static final int ROW_ACTION_SWITCH_PARTITION = 4;
    private static final Log LOG = LogFactory.getLog(TableWriter.class);
    private static final CharSequenceHashSet IGNORED_FILES = new CharSequenceHashSet();
    private static final Runnable NOOP = () -> {
    };
    private final static RemoveFileLambda REMOVE_OR_LOG = TableWriter::removeFileAndOrLog;
    private final static RemoveFileLambda REMOVE_OR_EXCEPTION = TableWriter::removeOrException;
    final ObjList<MemoryMAR> columns;
    private final ObjList<MemoryMA> logColumns;
    //    private final ObjList<MemoryLogA<MemoryA>> replSpliceColumns;
    private final ObjList<SymbolMapWriter> symbolMapWriters;
    private final ObjList<SymbolMapWriter> denseSymbolMapWriters;
    private final ObjList<ColumnIndexer> indexers;
    private final ObjList<ColumnIndexer> denseIndexers = new ObjList<>();
    private final Path path;
    private final Path other;
    private final LongList rowValueIsNotNull = new LongList();
    private final Row regularRow = new RowImpl();
    private final int rootLen;
    private final MemoryMR metaMem;
    private final int partitionBy;
    private final LongList columnTops;
    private final FilesFacade ff;
    private final DateFormat partitionDirFmt;
    private final MemoryMAR ddlMem;
    private final int mkDirMode;
    private final int fileOperationRetryCount;
    private final String tableName;
    private final TableWriterMetadata metadata;
    private final CairoConfiguration configuration;
    private final LowerCaseCharSequenceIntHashMap validationMap = new LowerCaseCharSequenceIntHashMap();
    private final FragileCode RECOVER_FROM_META_RENAME_FAILURE = this::recoverFromMetaRenameFailure;
    private final SOCountDownLatch indexLatch = new SOCountDownLatch();
    private final LongList indexSequences = new LongList();
    // This is the same message bus. When TableWriter instance created via CairoEngine, message bus is shared
    // and is owned by the engine. Since TableWriter would not have ownership of the bus it must not free it up.
    // On other hand when TableWrite is created outside CairoEngine, primarily in tests, the ownership of the
    // message bus is with the TableWriter. Therefore, message bus must be freed when writer is freed.
    // To indicate ownership, the message bus owned by the writer will be assigned to `ownMessageBus`. This reference
    // will be released by the writer
    private final MessageBus messageBus;
    private final MessageBus ownMessageBus;
    private final boolean parallelIndexerEnabled;
    private final PartitionBy.PartitionFloorMethod partitionFloorMethod;
    private final PartitionBy.PartitionCeilMethod partitionCeilMethod;
    private final int defaultCommitMode;
    private final int o3ColumnMemorySize;
    private final ObjList<Runnable> nullSetters;
    private final ObjList<Runnable> o3NullSetters;
    private final ObjList<MemoryCARW> o3Columns;
    private final ObjList<MemoryCARW> o3Columns2;
    private final TimestampValueRecord dropPartitionFunctionRec = new TimestampValueRecord();
    private final ObjList<O3CallbackTask> o3PendingCallbackTasks = new ObjList<>();
    private final O3ColumnUpdateMethod oooSortVarColumnRef = this::o3SortVarColumn;
    private final O3ColumnUpdateMethod oooSortFixColumnRef = this::o3SortFixColumn;
    private final SOUnboundedCountDownLatch o3DoneLatch = new SOUnboundedCountDownLatch();
    private final AtomicLong o3PartitionUpdRemaining = new AtomicLong();
    private final AtomicInteger o3ErrorCount = new AtomicInteger();
    private final MemoryMARW todoMem = Vm.getMARWInstance();
    private final TxWriter txWriter;
    private final LongList o3PartitionRemoveCandidates = new LongList();
    private final ObjectPool<O3MutableAtomicInteger> o3ColumnCounters = new ObjectPool<>(O3MutableAtomicInteger::new, 64);
    private final ObjectPool<O3Basket> o3BasketPool = new ObjectPool<>(O3Basket::new, 64);
    private final TxnScoreboard txnScoreboard;
    private final StringSink o3Sink = new StringSink();
    private final StringSink fileNameSink = new StringSink();
    private final FindVisitor removePartitionDirectories = this::removePartitionDirectories0;
    private final FindVisitor removePartitionDirsNotAttached = this::removePartitionDirsNotAttached;
    private final StringSink o3FileNameSink = new StringSink();
    private final RingQueue<O3PartitionUpdateTask> o3PartitionUpdateQueue;
    private final MPSequence o3PartitionUpdatePubSeq;
    private final SCSequence o3PartitionUpdateSubSeq;
    private final boolean o3QuickSortEnabled;
    private final LongConsumer appendTimestampSetter;
    private final MemoryMR indexMem = Vm.getMRInstance();
    private final MemoryFR slaveMetaMem = new MemoryFCRImpl();
    private final SCSequence commandSubSeq;
    private final LongIntHashMap replPartitionHash = new LongIntHashMap();
    // Latest command sequence per command source.
    // Publisher source is identified by a long value
    private final LongLongHashMap cmdSequences = new LongLongHashMap();
    private Row row = regularRow;
    private long todoTxn;
    private MemoryMAT o3TimestampMem;
    private final O3ColumnUpdateMethod o3MoveLagRef = this::o3MoveLag0;
    private MemoryARW o3TimestampMemCpy;
    private long lockFd = -1;
    private LongConsumer timestampSetter;
    private int columnCount;
    private boolean avoidIndexOnCommit = false;
    private long partitionTimestampHi;
    private long masterRef = 0;
    private long o3MasterRef = -1;
    private boolean removeDirOnCancelRow = true;
    private long tempMem16b = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
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
    private ObjList<? extends MemoryA> activeColumns;
    private ObjList<Runnable> activeNullSetters;
    private int rowActon = ROW_ACTION_OPEN_PARTITION;

    public TableWriter(CairoConfiguration configuration, CharSequence tableName) {
        this(configuration, tableName, null, new MessageBusImpl(configuration), true, DefaultLifecycleManager.INSTANCE, configuration.getRoot());
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
        this(configuration, tableName, messageBus, null, lock, lifecycleManager, configuration.getRoot());
    }

    public TableWriter(
            CairoConfiguration configuration,
            CharSequence tableName,
            MessageBus messageBus,
            MessageBus ownMessageBus,
            boolean lock,
            LifecycleManager lifecycleManager,
            CharSequence root
    ) {
        LOG.info().$("open '").utf8(tableName).$('\'').$();
        this.configuration = configuration;
        this.ownMessageBus = ownMessageBus;
        if (ownMessageBus != null) {
            this.messageBus = ownMessageBus;
        } else {
            this.messageBus = messageBus;
        }
        this.defaultCommitMode = configuration.getCommitMode();
        this.lifecycleManager = lifecycleManager;
        this.parallelIndexerEnabled = configuration.isParallelIndexingEnabled();
        this.ff = configuration.getFilesFacade();
        this.mkDirMode = configuration.getMkDirMode();
        this.fileOperationRetryCount = configuration.getFileOperationRetryCount();
        this.tableName = Chars.toString(tableName);
        this.o3QuickSortEnabled = configuration.isO3QuickSortEnabled();
        this.o3PartitionUpdateQueue = new RingQueue<O3PartitionUpdateTask>(O3PartitionUpdateTask.CONSTRUCTOR, configuration.getO3PartitionUpdateQueueCapacity());
        this.o3PartitionUpdatePubSeq = new MPSequence(this.o3PartitionUpdateQueue.getCycle());
        this.o3PartitionUpdateSubSeq = new SCSequence();
        o3PartitionUpdatePubSeq.then(o3PartitionUpdateSubSeq).then(o3PartitionUpdatePubSeq);
        final FanOut commandFanOut = this.messageBus.getTableWriterCommandFanOut();
        if (commandFanOut != null) {
            // align our subscription to the current value of the fan out
            // to avoid picking up commands fired ahead of this instantiation
            commandSubSeq = new SCSequence(commandFanOut.current(), null);
            commandFanOut.and(commandSubSeq);
        } else {
            // dandling sequence
            commandSubSeq = new SCSequence();
        }
        this.o3ColumnMemorySize = configuration.getO3ColumnMemorySize();
        this.path = new Path();
        this.path.of(root).concat(tableName);
        this.other = new Path().of(root).concat(tableName);
        this.rootLen = path.length();
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
            this.ddlMem = Vm.getMARInstance();
            this.metaMem = Vm.getMRInstance();

            openMetaFile(ff, path, rootLen, metaMem);
            this.metadata = new TableWriterMetadata(ff, metaMem);
            this.partitionBy = metaMem.getInt(META_OFFSET_PARTITION_BY);
            this.txWriter = new TxWriter(ff, path, partitionBy);
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
            this.rowValueIsNotNull.extendAndSet(columnCount, 0);
            this.columns = new ObjList<>(columnCount * 2);
            this.logColumns = new ObjList<>(columnCount * 2);
            this.o3Columns = new ObjList<>(columnCount * 2);
            this.o3Columns2 = new ObjList<>(columnCount * 2);
            this.activeColumns = columns;
            this.symbolMapWriters = new ObjList<>(columnCount);
            this.indexers = new ObjList<>(columnCount);
            this.denseSymbolMapWriters = new ObjList<>(metadata.getSymbolMapCount());
            this.nullSetters = new ObjList<>(columnCount);
            this.o3NullSetters = new ObjList<>(columnCount);
            this.activeNullSetters = nullSetters;
            this.columnTops = new LongList(columnCount);
            this.partitionFloorMethod = PartitionBy.getPartitionFloorMethod(partitionBy);
            this.partitionCeilMethod = PartitionBy.getPartitionCeilMethod(partitionBy);
            if (PartitionBy.isPartitioned(partitionBy)) {
                partitionDirFmt = PartitionBy.getPartitionDirFormatMethod(partitionBy);
            } else {
                partitionDirFmt = null;
            }

            configureColumnMemory();
            configureTimestampSetter();
            this.appendTimestampSetter = timestampSetter;
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
     * Adding new column can fail in many situations. None of the failures affect integrity of data that is already in
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

        if (ColumnType.isSymbol(type)) {
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
        if (isIndexed) {
            populateDenseIndexerList();
        }

        // increment column count
        columnCount++;

        // extend columnTop list to make sure row cancel can work
        // need for setting correct top is hard to test without being able to read from table
        columnTops.extendAndSet(columnCount - 1, txWriter.getTransientRowCount());

        // create column files
        if (txWriter.getTransientRowCount() > 0 || !PartitionBy.isPartitioned(partitionBy)) {
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

        txWriter.bumpStructureVersion(this.denseSymbolMapWriters);

        metadata.addColumn(name, configuration.getRandom().nextLong(), type, isIndexed, indexValueBlockCapacity);

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

        if (!ColumnType.isSymbol(existingType)) {
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
                if (PartitionBy.isPartitioned(partitionBy)) {
                    // run indexer for the whole table
                    final long timestamp = indexHistoricPartitions(indexer, columnName, indexValueBlockSize);
                    if (timestamp != Numbers.LONG_NaN) {
                        path.trimTo(rootLen);
                        setStateForTimestamp(path, timestamp, true);
                        // create index in last partition
                        indexLastPartition(indexer, columnName, columnIndex, indexValueBlockSize);
                    }
                } else {
                    setStateForTimestamp(path, 0, false);
                    // create index in last partition
                    indexLastPartition(indexer, columnName, columnIndex, indexValueBlockSize);
                }
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

        txWriter.bumpStructureVersion(this.denseSymbolMapWriters);
        indexers.extendAndSet(columnIndex, indexer);
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
        if (txWriter.attachedPartitionsContains(timestamp)) {
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

                    long nextMinTimestamp = Math.min(minPartitionTimestamp, txWriter.getMinTimestamp());
                    long nextMaxTimestamp = Math.max(maxPartitionTimestamp, txWriter.getMaxTimestamp());
                    boolean appendPartitionAttached = size() == 0 || getPartitionLo(nextMaxTimestamp) > getPartitionLo(txWriter.getMaxTimestamp());

                    txWriter.beginPartitionSizeUpdate();
                    txWriter.updatePartitionSizeByTimestamp(timestamp, partitionSize);
                    txWriter.finishPartitionSizeUpdate(nextMinTimestamp, nextMaxTimestamp);
                    txWriter.commit(defaultCommitMode, denseSymbolMapWriters);
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

        txWriter.bumpStructureVersion(this.denseSymbolMapWriters);
    }

    public boolean checkMaxAndCommitLag(int commitMode) {
        if (!hasO3() || getO3RowCount0() < metadata.getMaxUncommittedRows()) {
            return false;
        }
        commit(commitMode, metadata.getCommitLag());
        return true;
    }

    @Override
    public void close() {
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

    public void commitWithLag(int commitMode) {
        commit(commitMode, metadata.getCommitLag());
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
        return txWriter.getMaxTimestamp();
    }

    public TableWriterMetadata getMetadata() {
        return metadata;
    }

    public long getO3RowCount() {
        return hasO3() ? getO3RowCount0() : 0;
    }

    public int getPartitionBy() {
        return partitionBy;
    }

    public int getPartitionCount() {
        return txWriter.getPartitionCount();
    }

    public long getRawMetaMemory() {
        return metaMem.getPageAddress(0);
    }

    // todo: this method is not tested in cases when metadata size changes due to column add/remove operations
    public long getRawMetaMemorySize() {
        return metadata.getFileDataSize();
    }

    // todo: hide raw memory access from public interface when slave is able to send data over the network
    public long getRawTxnMemory() {
        return txWriter.unsafeGetRawMemory();
    }

    public long getStructureVersion() {
        return txWriter.getStructureVersion();
    }

    public int getSymbolIndex(int columnIndex, CharSequence symValue) {
        return symbolMapWriters.getQuick(columnIndex).put(symValue);
    }

    public String getTableName() {
        return tableName;
    }

    public long getTxn() {
        return txWriter.getTxn();
    }

    public TxnScoreboard getTxnScoreboard() {
        return txnScoreboard;
    }

    public boolean inTransaction() {
        return txWriter != null && (txWriter.inTransaction() || hasO3());
    }

    public boolean isOpen() {
        return tempMem16b != 0;
    }

    public Row newRow(long timestamp) {

        switch (rowActon) {
            case ROW_ACTION_OPEN_PARTITION:

                if (timestamp < Timestamps.O3_MIN_TS) {
                    throw CairoException.instance(0).put("timestamp before 1970-01-01 is not allowed");
                }

                if (txWriter.getMaxTimestamp() == Long.MIN_VALUE) {
                    txWriter.setMinTimestamp(timestamp);
                    openFirstPartition(timestamp);
                }
                // fall thru

                rowActon = ROW_ACTION_SWITCH_PARTITION;

            default: // switch partition
                bumpMasterRef();
                if (timestamp > partitionTimestampHi || timestamp < txWriter.getMaxTimestamp()) {
                    if (timestamp < txWriter.getMaxTimestamp()) {
                        return newRowO3(timestamp);
                    }

                    if (timestamp > partitionTimestampHi && PartitionBy.isPartitioned(partitionBy)) {
                        switchPartition(timestamp);
                    }
                }
                updateMaxTimestamp(timestamp);
                break;
            case ROW_ACTION_NO_PARTITION:

                if (timestamp < Timestamps.O3_MIN_TS) {
                    throw CairoException.instance(0).put("timestamp before 1970-01-01 is not allowed");
                }

                if (timestamp < txWriter.getMaxTimestamp()) {
                    throw CairoException.instance(ff.errno()).put("Cannot insert rows out of order. Table=").put(path);
                }

                bumpMasterRef();
                updateMaxTimestamp(timestamp);
                break;
            case ROW_ACTION_NO_TIMESTAMP:
                bumpMasterRef();
                break;
            case ROW_ACTION_O3:
                bumpMasterRef();
                o3TimestampSetter(timestamp);
                return row;
        }
        txWriter.append();
        return row;
    }

    public Row newRow() {
        return newRow(0L);
    }

    public void o3BumpErrorCount() {
        o3ErrorCount.incrementAndGet();
    }

    public void removeColumn(CharSequence name) {

        checkDistressed();

        final int index = getColumnIndex(name);
        final int type = metadata.getColumnType(index);

        LOG.info().$("removing column '").utf8(name).$("' from ").$(path).$();

        // check if we are moving timestamp from a partitioned table
        final int timestampIndex = metaMem.getInt(META_OFFSET_TIMESTAMP_INDEX);
        boolean timestamp = index == timestampIndex;

        if (timestamp && PartitionBy.isPartitioned(partitionBy)) {
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
            txWriter.resetTimestamp();
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

        txWriter.bumpStructureVersion(this.denseSymbolMapWriters);

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
        long minTimestamp = txWriter.getMinTimestamp();
        long maxTimestamp = txWriter.getMaxTimestamp();

        if (!PartitionBy.isPartitioned(partitionBy)) {
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

        if (!txWriter.attachedPartitionsContains(timestamp)) {
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
            if (timestamp == txWriter.getPartitionTimestamp(0)) {
                nextMinTimestamp = readMinTimestamp(txWriter.getPartitionTimestamp(1));
            }
            txWriter.beginPartitionSizeUpdate();
            txWriter.removeAttachedPartitions(timestamp);
            txWriter.setMinTimestamp(nextMinTimestamp);
            txWriter.finishPartitionSizeUpdate(nextMinTimestamp, txWriter.getMaxTimestamp());
            txWriter.commit(defaultCommitMode, denseSymbolMapWriters);

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
        if (PartitionBy.isPartitioned(partitionBy)) {
            if (txWriter.getPartitionCount() == 0) {
                throw SqlException.$(posForError, "table is empty");
            } else {
                // Drop partitions in descending order so if folders are missing on disk
                // removePartition does not fail to determine next minTimestamp
                for (int i = txWriter.getPartitionCount() - 1; i > -1; i--) {
                    long partitionTimestamp = txWriter.getPartitionTimestamp(i);
                    dropPartitionFunctionRec.setTimestamp(partitionTimestamp);
                    if (function.getBool(dropPartitionFunctionRec)) {
                        removePartition(partitionTimestamp);
                    }
                }
            }
        } else {
            throw SqlException.$(posForError, "table is not partitioned");
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

        txWriter.bumpStructureVersion(this.denseSymbolMapWriters);

        metadata.renameColumn(currentName, newName);

        if (index == metadata.getTimestampIndex()) {
            designatedTimestampColumnName = Chars.toString(newName);
        }

        LOG.info().$("RENAMED column '").utf8(currentName).$("' to '").utf8(newName).$("' from ").$(path).$();
    }

    public TableSyncModel replCreateTableSyncModel(long slaveTxData, long slaveMetaData, long slaveMetaDataSize) {
        replPartitionHash.clear();

        final TableSyncModel model = new TableSyncModel();

        model.setMaxTimestamp(getMaxTimestamp());

        final int symbolsCount = Unsafe.getUnsafe().getInt(slaveTxData + TX_OFFSET_MAP_WRITER_COUNT);
        final int theirLast;
        if (Unsafe.getUnsafe().getLong(slaveTxData + TX_OFFSET_DATA_VERSION) != txWriter.getDataVersion()) {
            // truncate
            model.setTableAction(TableSyncModel.TABLE_ACTION_TRUNCATE);
            theirLast = -1;
        } else {
            // hash partitions on slave side
            int partitionCount = Unsafe.getUnsafe().getInt(slaveTxData + getPartitionTableSizeOffset(symbolsCount)) / 8;
            theirLast = partitionCount / 4 - 1;
            for (int i = 0; i < partitionCount; i += 4) {
                long p = slaveTxData + getPartitionTableIndexOffset(symbolsCount, i);
                long ts = Unsafe.getUnsafe().getLong(p);
                replPartitionHash.put(ts, i);
            }
        }
        model.setDataVersion(txWriter.getDataVersion());

        // collate local partitions that need to be propagated to this slave
        final int ourPartitionCount = txWriter.getPartitionCount();
        final int ourLast = ourPartitionCount - 1;

        for (int i = 0; i < ourPartitionCount; i++) {
            try {
                final long ts = txWriter.getPartitionTimestamp(i);
                final long ourSize = i < ourLast ? txWriter.getPartitionSize(i) : txWriter.transientRowCount;
                final long ourDataTxn = i < ourLast ? txWriter.getPartitionDataTxn(i) : txWriter.txn;
                final int keyIndex = replPartitionHash.keyIndex(ts);
                final long theirSize;
                if (keyIndex < 0) {
                    int slavePartitionIndex = replPartitionHash.valueAt(keyIndex);
                    long p = slaveTxData + getPartitionTableIndexOffset(symbolsCount, slavePartitionIndex);
                    // check if partition name ourDataTxn is the same
                    if (Unsafe.getUnsafe().getLong(p + 16) == txWriter.getPartitionNameTxn(i)) {
                        // this is the same partition roughly
                        theirSize = slavePartitionIndex / 4 < theirLast ?
                                Unsafe.getUnsafe().getLong(p + 8) :
                                Unsafe.getUnsafe().getLong(slaveTxData + TX_OFFSET_TRANSIENT_ROW_COUNT);

                        if (theirSize > ourSize) {
                            LOG.error()
                                    .$("slave partition is larger than that on master [table=").$(tableName)
                                    .$(", ts=").$ts(ts)
                                    .I$();
                        }
                    } else {
                        // partition name ourDataTxn is different, partition mutated
                        theirSize = 0;
                    }
                } else {
                    theirSize = 0;
                }

                if (theirSize < ourSize) {
                    final long partitionNameTxn = txWriter.getPartitionNameTxn(i);
                    model.addPartitionAction(
                            theirSize == 0 ?
                                    TableSyncModel.PARTITION_ACTION_WHOLE :
                                    TableSyncModel.PARTITION_ACTION_APPEND,
                            ts,
                            theirSize,
                            ourSize - theirSize,
                            partitionNameTxn,
                            ourDataTxn
                    );

                    setPathForPartition(path, partitionBy, ts, false);

                    if (partitionNameTxn > -1) {
                        path.put('.').put(partitionNameTxn);
                    }

                    int plen = path.length();

                    for (int j = 0; j < columnCount; j++) {
                        final CharSequence columnName = metadata.getColumnName(j);
                        final long top = TableUtils.readColumnTop(
                                ff,
                                path.trimTo(plen),
                                columnName,
                                plen,
                                tempMem16b,
                                true
                        );

                        if (top > 0) {
                            model.addColumnTop(ts, j, top);
                        }

                        if (ColumnType.isVariableLength(metadata.getColumnType(j))) {
                            iFile(path.trimTo(plen), columnName);
                            long sz = TableUtils.readLongAtOffset(
                                    ff,
                                    path,
                                    tempMem16b,
                                    ourSize * 8L
                            );
                            model.addVarColumnSize(ts, j, sz);
                        }
                    }
                }
            } finally {
                path.trimTo(rootLen);
            }
        }

        slaveMetaMem.of(slaveMetaData, slaveMetaDataSize);

        final LowerCaseCharSequenceIntHashMap slaveColumnNameIndexMap = new LowerCaseCharSequenceIntHashMap();
        // create column name - index map
        // We will rely on this writer's metadata to convert CharSequence instances
        // of column names to string in the map. The assumption here that most of the time
        // column names will be the same

        int slaveColumnCount = slaveMetaMem.getInt(TableUtils.META_OFFSET_COUNT);
        long offset = TableUtils.getColumnNameOffset(slaveColumnCount);

        // don't create strings in this loop, we already have them in columnNameIndexMap
        for (int i = 0; i < slaveColumnCount; i++) {
            final CharSequence name = slaveMetaMem.getStr(offset);
            int ourColumnIndex = this.metadata.getColumnIndexQuiet(name);
            if (ourColumnIndex > -1) {
                slaveColumnNameIndexMap.put(this.metadata.getColumnName(ourColumnIndex), i);
            } else {
                slaveColumnNameIndexMap.put(Chars.toString(name), i);
            }
            offset += Vm.getStorageLength(name);
        }

        final long pTransitionIndex = TableUtils.createTransitionIndex(
                metaMem,
                slaveMetaMem,
                slaveColumnCount,
                slaveColumnNameIndexMap
        );

        try {
            final long pIndexBase = pTransitionIndex + 8;

            int addedColumnMetadataIndex = -1;
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {

                final int copyFrom = Unsafe.getUnsafe().getInt(pIndexBase + i * 8L) - 1;

                if (copyFrom == i) {
                    // It appears that column hasn't changed its position. There are three possibilities here:
                    // 1. Column has been deleted and re-added by the same name. We must check if file
                    //    descriptor is still valid. If it isn't, reload the column from disk
                    // 2. Column has been forced out of the reader via closeColumnForRemove(). This is required
                    //    on Windows before column can be deleted. In this case we must check for marker
                    //    instance and the column from disk
                    // 3. Column hasn't been altered, and we can skip to next column.
                    continue;
                }

                if (copyFrom > -1) {
                    int copyTo = Unsafe.getUnsafe().getInt(pIndexBase + i * 8L + 4) - 1;
                    if (copyTo == -1) {
                        model.addColumnMetaAction(TableSyncModel.COLUMN_META_ACTION_REMOVE, i, -1);
                        model.addColumnMetaAction(TableSyncModel.COLUMN_META_ACTION_MOVE, copyFrom, i);
                    } else {
                        model.addColumnMetaAction(TableSyncModel.COLUMN_META_ACTION_MOVE, copyFrom, copyTo + 1);
                    }
                } else {
                    // new column
                    model.addColumnMetadata(metadata.getColumnQuick(i));
                    if (copyFrom == -2) {
                        model.addColumnMetaAction(TableSyncModel.COLUMN_META_ACTION_REMOVE, i, -1);
                    }
                    model.addColumnMetaAction(TableSyncModel.COLUMN_META_ACTION_ADD, ++addedColumnMetadataIndex, i);
                }
            }
        } finally {
            TableUtils.freeTransitionIndex(pTransitionIndex);
        }

        return model;
    }

    public void replPublishSyncEvent(TableWriterTask cmd, long cursor, Sequence sequence) {
        final long dst = cmd.getInstance();
        final long dstIP = cmd.getIp();
        final long tableId = cmd.getTableId();
        LOG.info()
                .$("received replication SYNC cmd [tableName=").$(tableName)
                .$(", tableId=").$(tableId)
                .$(", src=").$(dst)
                .$(", srcIP=").$ip(dstIP)
                .I$();

        final TableSyncModel syncModel = replHandleSyncCmd(cmd);
        // release command queue slot not to hold both queues
        sequence.done(cursor);
        if (syncModel != null) {
            replPublishSyncEvent0(syncModel, tableId, dst, dstIP);
        }
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
                this.txWriter.unsafeLoadAll();
                rollbackIndexes();
                rollbackSymbolTables();
                purgeUnusedPartitions();
                configureAppendPosition();
                o3InError = false;
                // when we rolled transaction back, hasO3() has to be false
                o3MasterRef = -1;
                LOG.info().$("tx rollback complete [name=").$(tableName).$(']').$();
                processCommandQueue();
            } catch (Throwable e) {
                LOG.error().$("could not perform rollback [name=").$(tableName).$(", msg=").$(e.getMessage()).$(']').$();
                distressed = true;
            }
        } else {
            tick();
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
        // This is uncommitted row count
        return txWriter.getRowCount() + getO3RowCount();
    }

    public void tick() {
        processCommandQueue();
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
        rollback();

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
        // ensure file is closed with correct length
        todoMem.jumpTo(48);

        if (partitionBy != PartitionBy.NONE) {
            freeColumns(false);
            if (indexers != null) {
                for (int i = 0, n = indexers.size(); i < n; i++) {
                    Misc.free(indexers.getQuick(i));
                }
            }
            removePartitionDirectories();
            rowActon = ROW_ACTION_OPEN_PARTITION;
        } else {
            // truncate columns, we cannot remove them
            for (int i = 0; i < columnCount; i++) {
                getPrimaryColumn(i).truncate();
                MemoryMA mem = getSecondaryColumn(i);
                if (mem != null && mem.isOpen()) {
                    mem.truncate();
                    mem.putLong(0);
                }
            }
        }

        txWriter.resetTimestamp();
        txWriter.truncate();
        row = regularRow;
        try {
            clearTodoLog();
        } catch (CairoException err) {
            throwDistressException(err);
        }

        LOG.info().$("truncated [name=").$(tableName).$(']').$();
    }

    /**
     * Eagerly sets up writer instance. Otherwise, writer will initialize lazily. Invoking this method could improve
     * performance of some applications. UDP receivers use this in order to avoid initial receive buffer contention.
     */
    public void warmUp() {
        Row r = newRow(Math.max(Timestamps.O3_MIN_TS, txWriter.getMaxTimestamp()));
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

    /**
     * This an O(n) method to find if column by the same name already exists. The benefit of poor performance
     * is that we don't keep column name strings on heap. We only use this method when adding new column, where
     * high performance of name check does not matter much.
     *
     * @param name to check
     * @return 0 based column index.
     */
    private static int getColumnIndexQuiet(MemoryMR metaMem, CharSequence name, int columnCount) {
        long nameOffset = getColumnNameOffset(columnCount);
        for (int i = 0; i < columnCount; i++) {
            CharSequence col = metaMem.getStr(nameOffset);
            if (Chars.equalsIgnoreCase(col, name)) {
                return i;
            }
            nameOffset += Vm.getStorageLength(col);
        }
        return -1;
    }

    private static void configureNullSetters(ObjList<Runnable> nullers, int type, MemoryA mem1, MemoryA mem2) {
        switch (ColumnType.tagOf(type)) {
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
            case ColumnType.GEOBYTE:
                nullers.add(() -> mem1.putByte(GeoHashes.BYTE_NULL));
                break;
            case ColumnType.GEOSHORT:
                nullers.add(() -> mem1.putShort(GeoHashes.SHORT_NULL));
                break;
            case ColumnType.GEOINT:
                nullers.add(() -> mem1.putInt(GeoHashes.INT_NULL));
                break;
            case ColumnType.GEOLONG:
                nullers.add(() -> mem1.putLong(GeoHashes.NULL));
            default:
                break;
        }
    }

    private static void openMetaFile(FilesFacade ff, Path path, int rootLen, MemoryMR metaMem) {
        path.concat(META_FILE_NAME).$();
        try {
            metaMem.smallFile(ff, path, MemoryTag.MMAP_TABLE_WRITER);
        } finally {
            path.trimTo(rootLen);
        }
    }

    private static void attachPartitionCheckFilesMatchMetadata(FilesFacade ff, Path path, RecordMetadata metadata, long partitionSize) throws CairoException {
        // for each column, check that file exists in the partition folder
        int rootLen = path.length();
        for (int columnIndex = 0, size = metadata.getColumnCount(); columnIndex < size; columnIndex++) {
            try {
                int columnType = metadata.getColumnType(columnIndex);
                final CharSequence columnName = metadata.getColumnName(columnIndex);
                path.concat(columnName);

                switch (ColumnType.tagOf(columnType)) {
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
                    case ColumnType.GEOBYTE:
                    case ColumnType.GEOSHORT:
                    case ColumnType.GEOINT:
                    case ColumnType.GEOLONG:
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
                throw CairoException.instance(0)
                        .put("Column file row count does not match timestamp file row count. ")
                        .put("Partition files inconsistent [file=")
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
                throw CairoException.instance(0)
                        .put("Column file row count does not match timestamp file row count. ")
                        .put("Partition files inconsistent [file=")
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
            ddlMem.putInt(type);
            long flags = 0;
            if (indexFlag) {
                flags |= META_FLAG_BIT_INDEXED;
            }

            if (sequentialFlag) {
                flags |= META_FLAG_BIT_SEQUENTIAL;
            }

            ddlMem.putLong(flags);
            ddlMem.putInt(indexValueBlockCapacity);
            ddlMem.putLong(configuration.getRandom().nextLong());
            ddlMem.skip(8);

            long nameOffset = getColumnNameOffset(columnCount);
            for (int i = 0; i < columnCount; i++) {
                CharSequence columnName = metaMem.getStr(nameOffset);
                ddlMem.putStr(columnName);
                nameOffset += Vm.getStorageLength(columnName);
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

    private void cancelRowAndBump() {
        rowCancel();
        masterRef++;
    }

    private void checkDistressed() {
        if (!distressed) {
            return;
        }
        throw new CairoError("Table '" + tableName + "' is distressed");
    }

    private void clearO3() {
        this.o3MasterRef = -1; // clears o3 flag, hasO3() will be returning false
        rowActon = ROW_ACTION_SWITCH_PARTITION;
        // transaction log is either not required or pending
        activeColumns = columns;
        activeNullSetters = nullSetters;
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
            // ensure file is closed with correct length
            todoMem.jumpTo(40);
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
            setColumnSize(i, size, false);
            Misc.free((MemoryMA) getPrimaryColumn(i));
            Misc.free((MemoryMA) getSecondaryColumn(i));
        }
        Misc.freeObjList(denseIndexers);
        denseIndexers.clear();
    }

    private void closeAppendMemoryTruncate(boolean truncate) {
        for (int i = 0, n = columns.size(); i < n; i++) {
            MemoryMA m = columns.getQuick(i);
            if (m != null) {
                m.close(truncate);
            }
        }
    }

    /**
     * Commits newly added rows of data. This method updates transaction file with pointers to end of appended data.
     * <p>
     * <b>Pending rows</b>
     * <p>This method will cancel pending rows by calling {@link #rowCancel()}. Data in partially appended row will be lost.</p>
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
            rowCancel();
        }

        if (inTransaction()) {

            if (hasO3() && o3Commit(commitLag)) {
                return;
            }

            if (commitMode != CommitMode.NOSYNC) {
                syncColumns(commitMode);
            }

            updateIndexes();
            txWriter.commit(commitMode, this.denseSymbolMapWriters);
            o3ProcessPartitionRemoveCandidates();
        }

        tick();
    }

    private void configureAppendPosition() {
        final boolean partitioned = PartitionBy.isPartitioned(partitionBy);
        if (this.txWriter.getMaxTimestamp() > Long.MIN_VALUE || !partitioned) {
            openFirstPartition(this.txWriter.getMaxTimestamp());
            if (partitioned) {
                rowActon = ROW_ACTION_OPEN_PARTITION;
                timestampSetter = appendTimestampSetter;
            } else {
                if (metadata.getTimestampIndex() < 0) {
                    rowActon = ROW_ACTION_NO_TIMESTAMP;
                } else {
                    rowActon = ROW_ACTION_NO_PARTITION;
                    timestampSetter = appendTimestampSetter;
                }
            }
        } else {
            rowActon = ROW_ACTION_OPEN_PARTITION;
            timestampSetter = appendTimestampSetter;
        }
        activeColumns = columns;
    }

    private void configureColumn(int type, boolean indexFlag) {
        final MemoryMAR primary = Vm.getMARInstance();
        final MemoryMAR secondary;
        final MemoryCARW oooPrimary = Vm.getCARWInstance(o3ColumnMemorySize, Integer.MAX_VALUE, MemoryTag.NATIVE_O3);
        final MemoryCARW oooSecondary;
        final MemoryCARW oooPrimary2 = Vm.getCARWInstance(o3ColumnMemorySize, Integer.MAX_VALUE, MemoryTag.NATIVE_O3);
        final MemoryCARW oooSecondary2;


        final MemoryMAR logPrimary = Vm.getMARInstance();
        final MemoryMAR logSecondary;

        switch (ColumnType.tagOf(type)) {
            case ColumnType.BINARY:
            case ColumnType.STRING:
                secondary = Vm.getMARInstance();
                oooSecondary = Vm.getCARWInstance(o3ColumnMemorySize, Integer.MAX_VALUE, MemoryTag.NATIVE_O3);
                oooSecondary2 = Vm.getCARWInstance(o3ColumnMemorySize, Integer.MAX_VALUE, MemoryTag.NATIVE_O3);
                logSecondary = Vm.getMARInstance();
                break;
            default:
                secondary = null;
                oooSecondary = null;
                oooSecondary2 = null;
                logSecondary = null;
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
        logColumns.add(logPrimary);
        logColumns.add(logSecondary);

        if (indexFlag) {
            indexers.extendAndSet((columns.size() - 1) / 2, new SymbolColumnIndexer());
        }
        rowValueIsNotNull.add(0);
    }

    private void configureColumnMemory() {
        this.symbolMapWriters.setPos(columnCount);
        for (int i = 0; i < columnCount; i++) {
            int type = metadata.getColumnType(i);
            configureColumn(type, metadata.isColumnIndexed(i));

            if (ColumnType.isSymbol(type)) {
                final int symbolIndex = denseSymbolMapWriters.size();
                SymbolMapWriter symbolMapWriter = new SymbolMapWriter(
                        configuration,
                        path.trimTo(rootLen),
                        metadata.getColumnName(i),
                        txWriter.unsafeReadSymbolCount(symbolIndex),
                        symbolIndex,
                        txWriter
                );

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
            o3TimestampMemCpy = Vm.getCARWInstance(o3ColumnMemorySize, Integer.MAX_VALUE, MemoryTag.NATIVE_O3);
        }
    }

    private void configureTimestampSetter() {
        int index = metadata.getTimestampIndex();
        if (index == -1) {
            timestampSetter = value -> {
            };
        } else {
            nullSetters.setQuick(index, NOOP);
            o3NullSetters.setQuick(index, NOOP);
            timestampSetter = getPrimaryColumn(index)::putLong;
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
                            o3FileNameSink,
                            rowValueIsNotNull, // reuse, this is only called from writer close
                            purgeQueue,
                            purgePubSeq,
                            path,
                            tableName,
                            task.getPartitionBy(),
                            task.getTimestamp(),
                            txnScoreboard,
                            task.getMostRecentTxn()
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
                    ddlMem.putInt(getColumnType(metaMem, i));
                    long flags = META_FLAG_BIT_INDEXED;
                    if (isSequential(metaMem, i)) {
                        flags |= META_FLAG_BIT_SEQUENTIAL;
                    }
                    ddlMem.putLong(flags);
                    ddlMem.putInt(indexValueBlockSize);
                    ddlMem.putLong(getColumnHash(metaMem, i));
                    ddlMem.skip(8);
                }
            }

            long nameOffset = getColumnNameOffset(columnCount);
            for (int i = 0; i < columnCount; i++) {
                CharSequence columnName = metaMem.getStr(nameOffset);
                ddlMem.putStr(columnName);
                nameOffset += Vm.getStorageLength(columnName);
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
                nameOffset += Vm.getStorageLength(columnName);
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
     * append index data. Therefore, it must be called before primary column is initialized.
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
                ddlMem.smallFile(ff, path, MemoryTag.MMAP_TABLE_WRITER);
                BitmapIndexWriter.initKeyMemory(ddlMem, indexValueBlockCapacity);
            } catch (CairoException e) {
                // looks like we could not create key file properly
                // lets not leave half-baked file sitting around
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
        SymbolMapWriter w = new SymbolMapWriter(
                configuration,
                path,
                name,
                0,
                denseSymbolMapWriters.size(),
                txWriter
        );
        denseSymbolMapWriters.add(w);
        symbolMapWriters.extendAndSet(columnCount, w);
    }

    private void doClose(boolean truncate) {
        consumeO3PartitionRemoveTasks();
        boolean tx = inTransaction();
        freeSymbolMapWriters();
        freeIndexers();
        Misc.free(txWriter);
        Misc.free(metaMem);
        Misc.free(ddlMem);
        Misc.free(indexMem);
        Misc.free(other);
        Misc.free(todoMem);
        freeColumns(truncate & !distressed);
        try {
            releaseLock(!truncate | tx | performRecovery | distressed);
        } finally {
            Misc.free(txnScoreboard);
            Misc.free(path);
            Misc.free(o3TimestampMemCpy);
            final FanOut commandFanOut = messageBus.getTableWriterCommandFanOut();
            if (commandFanOut != null) {
                commandFanOut.remove(commandSubSeq);
            }
            Misc.free(ownMessageBus);
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

        txWriter.bumpStructureVersion(this.denseSymbolMapWriters);
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
        Misc.freeObjListAndKeepObjects(logColumns);
    }

    private void freeIndexers() {
        if (indexers != null) {
            // Don't change items of indexers, they are re-used
            for (int i = 0, n = indexers.size(); i < n; i++) {
                Misc.free(indexers.getQuick(i));
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
            Unsafe.free(tempMem16b, 16, MemoryTag.NATIVE_DEFAULT);
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

    private long getO3RowCount0() {
        return (masterRef - o3MasterRef + 1) / 2;
    }

    private long getPartitionLo(long timestamp) {
        return partitionFloorMethod.floor(timestamp);
    }

    long getPartitionNameTxnByIndex(int index) {
        return txWriter.getPartitionNameTxnByIndex(index);
    }

    long getPartitionSizeByIndex(int index) {
        return txWriter.getPartitionSizeByIndex(index);
    }

    private MemoryMAR getPrimaryColumn(int column) {
        assert column < columnCount : "Column index is out of bounds: " + column + " >= " + columnCount;
        return columns.getQuick(getPrimaryColumnIndex(column));
    }

    private MemoryMAR getSecondaryColumn(int column) {
        assert column < columnCount : "Column index is out of bounds: " + column + " >= " + columnCount;
        return columns.getQuick(getSecondaryColumnIndex(column));
    }

    SymbolMapWriter getSymbolMapWriter(int columnIndex) {
        return symbolMapWriters.getQuick(columnIndex);
    }

    private boolean hasO3() {
        return o3MasterRef > -1;
    }

    private long indexHistoricPartitions(SymbolColumnIndexer indexer, CharSequence columnName, int indexValueBlockSize) {
        final long ts = this.txWriter.getMaxTimestamp();
        if (ts > Numbers.LONG_NaN) {
            final long maxTimestamp = partitionFloorMethod.floor(ts);
            long timestamp = txWriter.getMinTimestamp();
            try (final MemoryMR roMem = indexMem) {

                while (timestamp < maxTimestamp) {

                    path.trimTo(rootLen);

                    setStateForTimestamp(path, timestamp, true);

                    if (txWriter.attachedPartitionsContains(timestamp) && ff.exists(path.$())) {

                        final int plen = path.length();

                        TableUtils.dFile(path.trimTo(plen), columnName);

                        if (ff.exists(path)) {

                            path.trimTo(plen);

                            LOG.info().$("indexing [path=").$(path).$(']').$();

                            createIndexFiles(columnName, indexValueBlockSize, plen, true);

                            final long partitionSize = txWriter.getPartitionSizeByPartitionTimestamp(timestamp);
                            final long columnTop = TableUtils.readColumnTop(ff, path.trimTo(plen), columnName, plen, tempMem16b, true);

                            if (partitionSize > columnTop) {
                                TableUtils.dFile(path.trimTo(plen), columnName);
                                final long columnSize = (partitionSize - columnTop) << ColumnType.pow2SizeOf(ColumnType.INT);
                                roMem.of(ff, path, columnSize, columnSize, MemoryTag.MMAP_TABLE_WRITER);
                                indexer.configureWriter(configuration, path.trimTo(plen), columnName, columnTop);
                                indexer.index(roMem, columnTop, partitionSize);
                            }
                        }
                    }
                    timestamp = partitionCeilMethod.ceil(timestamp);
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

        final long columnTop = TableUtils.readColumnTop(ff, path.trimTo(plen), columnName, plen, tempMem16b, true);

        // set indexer up to continue functioning as normal
        indexer.configureFollowerAndWriter(configuration, path.trimTo(plen), columnName, getPrimaryColumn(columnIndex), columnTop);
        indexer.refreshSourceAndIndex(0, txWriter.getTransientRowCount());
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

    private Row newRowO3(long timestamp) {
        LOG.info().$("switched to o3 [table=").utf8(tableName).$(']').$();
        txWriter.beginPartitionSizeUpdate();
        o3OpenColumns();
        o3InError = false;
        o3MasterRef = masterRef;
        rowActon = ROW_ACTION_O3;
        o3TimestampSetter(timestamp);
        return row;
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
        o3RowCount = getO3RowCount0();
        o3PartitionRemoveCandidates.clear();
        o3ErrorCount.set(0);
        o3ColumnCounters.clear();
        o3BasketPool.clear();

        long o3LagRowCount = 0;
        long maxUncommittedRows = metadata.getMaxUncommittedRows();
        final int timestampIndex = metadata.getTimestampIndex();
        this.lastPartitionTimestamp = partitionFloorMethod.floor(partitionTimestampHi);
        // we will check new partitionTimestampHi value against the limit to see if the writer
        // will have to switch partition internally
        long partitionTimestampHiLimit = partitionCeilMethod.ceil(partitionTimestampHi) - 1;
        try {
            o3RowCount += o3MoveUncommitted(timestampIndex);
            final long transientRowCount = txWriter.transientRowCount;

            // we may need to re-use file descriptors when this partition is the "current" one
            // we cannot open file again due to sharing violation
            //
            // to determine that 'ooTimestampLo' goes into current partition
            // we need to compare 'partitionTimestampHi', which is appropriately truncated to DAY/MONTH/YEAR
            // to this.maxTimestamp, which isn't truncated yet. So we need to truncate it first
            LOG.info().$("sorting o3 [table=").$(tableName).$(']').$();
            final long sortedTimestampsAddr = o3TimestampMem.getAddress();

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
            final long maxTimestamp = txWriter.getMaxTimestamp();

            // we are going to use this soon to avoid double-copying lag data
            // final boolean yep = isAppendLastPartitionOnly(sortedTimestampsAddr, o3TimestampMax);

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
                // We do not know upfront which partition is going to be last because this is
                // a single pass over the data. Instead, we will update transient row count in a rolling
                // manner, assuming the partition marked "last" is the last and then for a new partition
                // we move prevTransientRowCount into the "fixedRowCount" sum and set new value on the
                // transientRowCount
                long prevTransientRowCount = transientRowCount;

                while (srcOoo < srcOooMax) {
                    try {
                        final long srcOooLo = srcOoo;
                        final long o3Timestamp = getTimestampIndexValue(sortedTimestampsAddr, srcOoo);
                        final long srcOooHi;
                        // keep ceil inclusive in the interval
                        final long srcOooTimestampCeil = partitionCeilMethod.ceil(o3Timestamp) - 1;
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

                        final long partitionTimestamp = partitionFloorMethod.floor(o3Timestamp);
                        final boolean last = partitionTimestamp == lastPartitionTimestamp;
                        srcOoo = srcOooHi + 1;

                        final long srcDataMax;
                        final long srcNameTxn;
                        final int partitionIndex = txWriter.findAttachedPartitionIndexByLoTimestamp(partitionTimestamp);
                        if (partitionIndex > -1) {
                            if (last) {
                                srcDataMax = transientRowCount;
                            } else {
                                srcDataMax = getPartitionSizeByIndex(partitionIndex);
                            }
                            srcNameTxn = getPartitionNameTxnByIndex(partitionIndex);
                        } else {
                            srcDataMax = 0;
                            srcNameTxn = -1;
                        }

                        final boolean append = last && (srcDataMax == 0 || o3Timestamp >= maxTimestamp);
                        final long partitionSize = srcDataMax + srcOooHi - srcOooLo + 1;

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
                                .$(", srcDataMax=").$(srcDataMax)
                                .$(", maxTimestamp=").$ts(maxTimestamp)
                                .$(", last=").$(last)
                                .$(", partitionSize=").$(partitionSize)
                                .$(", append=").$(append)
                                .$(", memUsed=").$(Unsafe.getMemUsed())
                                .I$();

                        if (partitionTimestamp < lastPartitionTimestamp) {
                            // increment fixedRowCount by number of rows old partition incremented
                            this.txWriter.fixedRowCount += partitionSize - srcDataMax;
                        } else if (partitionTimestamp == lastPartitionTimestamp) {
                            // this is existing "last" partition, we can set the size directly
                            prevTransientRowCount = partitionSize;
                        } else {
                            // this is potentially a new last partition
                            this.txWriter.fixedRowCount += prevTransientRowCount;
                            prevTransientRowCount = partitionSize;
                        }

                        pCount++;
                        o3PartitionUpdRemaining.incrementAndGet();
                        final O3Basket o3Basket = o3BasketPool.next();
                        o3Basket.ensureCapacity(columnCount, indexCount);

                        AtomicInteger columnCounter = o3ColumnCounters.next();

                        // async partition processing set this counter to the column count
                        // and then manages issues if publishing of column tasks fails
                        // mid-column-count.
                        latchCount++;

                        if (append) {
                            // we are appending last partition, make sure it has been mapped!
                            // this also might fail, make sure exception is trapped and partitions are
                            // counted down correctly
                            try {
                                setAppendPosition(srcDataMax, false);
                            } catch (Throwable e) {
                                o3BumpErrorCount();
                                o3ClockDownPartitionUpdateCount();
                                o3CountDownDoneLatch();
                                throw e;
                            }

                            columnCounter.set(columnCount);
                            Path pathToPartition = Path.getThreadLocal(this.path);
                            TableUtils.setPathForPartition(pathToPartition, partitionBy, o3TimestampMin, false);
                            TableUtils.txnPartitionConditionally(pathToPartition, srcNameTxn);
                            final int plen = pathToPartition.length();
                            int columnsPublished = 0;
                            for (int i = 0; i < columnCount; i++) {
                                final int colOffset = TableWriter.getPrimaryColumnIndex(i);
                                final boolean notTheTimestamp = i != timestampIndex;
                                final int columnType = metadata.getColumnType(i);
                                final CharSequence columnName = metadata.getColumnName(i);
                                final int indexBlockCapacity = metadata.isColumnIndexed(i) ? metadata.getIndexValueBlockCapacity(i) : -1;
                                final BitmapIndexWriter indexWriter = indexBlockCapacity > -1 ? getBitmapIndexWriter(i) : null;
                                final MemoryARW oooMem1 = o3Columns.getQuick(colOffset);
                                final MemoryARW oooMem2 = o3Columns.getQuick(colOffset + 1);
                                final MemoryMAR mem1 = columns.getQuick(colOffset);
                                final MemoryMAR mem2 = columns.getQuick(colOffset + 1);
                                final long srcDataTop = getColumnTop(i);
                                final long srcOooFixAddr;
                                final long srcOooVarAddr;
                                final MemoryMAR dstFixMem;
                                final MemoryMAR dstVarMem;
                                if (!ColumnType.isVariableLength(columnType)) {
                                    srcOooFixAddr = oooMem1.addressOf(0);
                                    srcOooVarAddr = 0;
                                    dstFixMem = mem1;
                                    dstVarMem = null;
                                } else {
                                    srcOooFixAddr = oooMem2.addressOf(0);
                                    srcOooVarAddr = oooMem1.addressOf(0);
                                    dstFixMem = mem2;
                                    dstVarMem = mem1;
                                }

                                columnsPublished++;
                                try {
                                    O3OpenColumnJob.appendLastPartition(
                                            pathToPartition,
                                            plen,
                                            columnName,
                                            columnCounter,
                                            notTheTimestamp ? columnType : ColumnType.setDesignatedTimestampBit(columnType, true),
                                            srcOooFixAddr,
                                            srcOooVarAddr,
                                            srcOooLo,
                                            srcOooHi,
                                            srcOooMax,
                                            o3TimestampMin,
                                            o3TimestampMax,
                                            partitionTimestamp,
                                            srcDataTop,
                                            srcDataMax,
                                            indexBlockCapacity,
                                            dstFixMem,
                                            dstVarMem,
                                            this,
                                            indexWriter
                                    );
                                } catch (Throwable e) {
                                    if (columnCounter.addAndGet(columnsPublished - columnCount) == 0) {
                                        o3ClockDownPartitionUpdateCount();
                                        o3CountDownDoneLatch();
                                    }
                                    throw e;
                                }
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
                                    srcDataMax,
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

                // at this point we should know the last partition row count
                this.txWriter.transientRowCount = prevTransientRowCount;
                this.partitionTimestampHi = Math.max(this.partitionTimestampHi, o3TimestampMax);
                this.txWriter.updateMaxTimestamp(Math.max(txWriter.getMaxTimestamp(), o3TimestampMax));
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
            // move append memory to new set of files. Otherwise, we stay on the same set but advance to append position.
            avoidIndexOnCommit = o3ErrorCount.get() == 0;
            if (o3LagRowCount == 0) {
                clearO3();
                LOG.debug().$("lag segment is empty").$();
            } else {
                // adjust O3 master ref so that virtual row count becomes equal to value of "o3LagRowCount"
                this.o3MasterRef = this.masterRef - o3LagRowCount * 2 + 1;
                LOG.debug().$("adjusted [o3RowCount=").$(getO3RowCount0()).I$();
            }
        }

        if (!columns.getQuick(0).isOpen() || partitionTimestampHi > partitionTimestampHiLimit) {
            openPartition(txWriter.getMaxTimestamp());
        }

        // Data is written out successfully, however, we can still fail to set append position, for
        // example when we ran out of address space and new page cannot be mapped. The "allocate" calls here
        // ensure we can trigger this situation in tests. We should perhaps align our data such that setAppendPosition()
        // will attempt to mmap new page and fail... Then we can remove the 'true' parameter
        try {
            setAppendPosition(txWriter.getTransientRowCount(), true);
        } catch (Throwable e) {
            LOG.error().$("data is committed but writer failed to update its state `").$(e).$('`').$();
            distressed = true;
            throw e;
        }
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
            long srcDataMax,
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
                    srcDataMax,
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
                    srcDataMax,
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
            MemoryARW o3DataMem = o3Columns.get(getPrimaryColumnIndex(columnIndex));
            MemoryARW o3IndexMem = o3Columns.get(getSecondaryColumnIndex(columnIndex));

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
                // move count + 1 rows, to make sure index column remains n+1
                O3Utils.shiftCopyFixedSizeColumnData(
                        sourceOffset,
                        o3IndexMem.addressOf(o3RowCount * 8),
                        0,
                        o3LagRowCount + 1,
                        o3IndexMem.addressOf(0)
                );
                // adjust append position of the index column to
                // maintain n+1 number of entries
                o3IndexMem.jumpTo(o3LagRowCount * 8 + 8);
            }

            o3DataMem.jumpTo(size);
            Vect.memmove(o3DataMem.addressOf(0), o3DataMem.addressOf(sourceOffset), size);
        } else {
            // Special case, designated timestamp column
            // Move values and set index to  0..o3LagRowCount
            final long sourceOffset = o3RowCount * 16;
            final long mergeMemAddr = o3TimestampMem.getAddress();
            Vect.shiftTimestampIndex(mergeMemAddr + sourceOffset, o3LagRowCount, mergeMemAddr);
            o3TimestampMem.jumpTo(o3LagRowCount * 16);
        }
    }

    private long o3MoveUncommitted(final int timestampIndex) {
        final long committedRowCount = txWriter.getCommittedFixedRowCount() + txWriter.getCommittedTransientRowCount();
        final long rowsAdded = txWriter.getRowCount() - committedRowCount;
        long transientRowsAdded = Math.min(txWriter.getTransientRowCount(), rowsAdded);
        final long committedTransientRowCount = txWriter.getTransientRowCount() - transientRowsAdded;
        if (transientRowsAdded > 0) {
            LOG.debug()
                    .$("o3 move uncommitted [table=").$(tableName)
                    .$(", transientRowsAdded=").$(transientRowsAdded)
                    .I$();
            return o3ScheduleMoveUncommitted0(
                    timestampIndex,
                    transientRowsAdded,
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
            MemoryMAR srcDataMem = getPrimaryColumn(colIndex);
            int shl = ColumnType.pow2SizeOf(columnType);
            long srcFixOffset;
            final MemoryARW o3DataMem = o3Columns.get(getPrimaryColumnIndex(colIndex));
            final MemoryARW o3IndexMem = o3Columns.get(getSecondaryColumnIndex(colIndex));

            long extendedSize;
            long dstVarOffset = o3DataMem.getAppendOffset();

            final long columnTop = columnTops.getQuick(colIndex);

            if (columnTop > 0) {
                LOG.debug()
                        .$("move uncommitted [columnTop=").$(columnTop)
                        .$(", columnIndex=").$(colIndex)
                        .$(", committedTransientRowCount=").$(committedTransientRowCount)
                        .$(", transientRowsAdded=").$(transientRowsAdded)
                        .I$();
            }

            if (null == o3IndexMem) {
                // Fixed size
                extendedSize = transientRowsAdded << shl;
                srcFixOffset = (committedTransientRowCount - columnTop) << shl;
            } else {
                // Var size
                int indexShl = ColumnType.pow2SizeOf(ColumnType.LONG);
                final MemoryMAR srcFixMem = getSecondaryColumn(colIndex);
                long sourceOffset = (committedTransientRowCount - columnTop) << indexShl;

                // the size includes trailing LONG
                long sourceLen = (transientRowsAdded + 1) << indexShl;
                long dstAppendOffset = o3IndexMem.getAppendOffset();

                // ensure memory is available
                o3IndexMem.jumpTo(dstAppendOffset + (transientRowsAdded << indexShl));
                long alignedExtraLen;
                long srcAddress;
                boolean isMapped = srcFixMem.isMapped(sourceOffset, sourceLen);

                if (isMapped) {
                    alignedExtraLen = 0;
                    srcAddress = srcFixMem.addressOf(sourceOffset);
                } else {
                    // Linux requires the mmap offset to be page aligned
                    long alignedOffset = Files.floorPageSize(sourceOffset);
                    alignedExtraLen = sourceOffset - alignedOffset;
                    srcAddress = mapRO(ff, srcFixMem.getFd(), sourceLen + alignedExtraLen, alignedOffset, MemoryTag.MMAP_TABLE_WRITER);
                }

                long srcVarOffset = Unsafe.getUnsafe().getLong(srcAddress);
                O3Utils.shiftCopyFixedSizeColumnData(
                        srcVarOffset - dstVarOffset,
                        srcAddress + alignedExtraLen + Long.BYTES,
                        0,
                        transientRowsAdded - 1,
                        // copy uncommitted index over the trailing LONG
                        o3IndexMem.addressOf(dstAppendOffset)
                );

                if (!isMapped) {
                    // If memory mapping was mapped specially for this move, close it
                    ff.munmap(srcAddress, sourceLen + alignedExtraLen, MemoryTag.MMAP_TABLE_WRITER);
                }

                long sourceEndOffset = srcDataMem.getAppendOffset();
                extendedSize = sourceEndOffset - srcVarOffset;
                srcFixOffset = srcVarOffset;
                srcFixMem.jumpTo(sourceOffset + Long.BYTES);
            }

            o3DataMem.jumpTo(dstVarOffset + extendedSize);
            long appendAddress = o3DataMem.addressOf(dstVarOffset);
            if (srcDataMem.isMapped(srcFixOffset, extendedSize)) {
                long sourceAddress = srcDataMem.addressOf(srcFixOffset);
                Vect.memcpy(appendAddress, sourceAddress, extendedSize);
            } else {
                // Linux requires the mmap offset to be page aligned
                long alignedOffset = Files.floorPageSize(srcFixOffset);
                long alignedExtraLen = srcFixOffset - alignedOffset;
                long sourceAddress = mapRO(ff, srcDataMem.getFd(), extendedSize + alignedExtraLen, alignedOffset, MemoryTag.MMAP_TABLE_WRITER);
                Vect.memcpy(appendAddress, sourceAddress + alignedExtraLen, extendedSize);
                ff.munmap(sourceAddress, extendedSize + alignedExtraLen, MemoryTag.MMAP_TABLE_WRITER);
            }
            srcDataMem.jumpTo(srcFixOffset);
        } else {
            // Timestamp column
            colIndex = -colIndex - 1;
            int shl = ColumnType.pow2SizeOf(ColumnType.TIMESTAMP);
            MemoryMAR srcDataMem = getPrimaryColumn(colIndex);
            // this cannot have "top"
            long srcFixOffset = committedTransientRowCount << shl;
            long srcFixLen = transientRowsAdded << shl;
            boolean isMapped = srcDataMem.isMapped(srcFixOffset, srcFixLen);
            long alignedExtraLen;
            long address;

            if (isMapped) {
                alignedExtraLen = 0;
                address = srcDataMem.addressOf(srcFixOffset);
            } else {
                // Linux requires the mmap offset to be page aligned
                long alignedOffset = Files.floorPageSize(srcFixOffset);
                alignedExtraLen = srcFixOffset - alignedOffset;
                address = mapRO(ff, srcDataMem.getFd(), srcFixLen + alignedExtraLen, alignedOffset, MemoryTag.MMAP_TABLE_WRITER);
            }

            for (long n = 0; n < transientRowsAdded; n++) {
                long ts = Unsafe.getUnsafe().getLong(address + alignedExtraLen + (n << shl));
                o3TimestampMem.putLong128(ts, o3RowCount + n);
            }

            if (!isMapped) {
                ff.munmap(address, srcFixLen + alignedExtraLen, MemoryTag.MMAP_TABLE_WRITER);
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
            MemoryARW mem1 = o3Columns.getQuick(getPrimaryColumnIndex(i));
            mem1.jumpTo(0);
            MemoryARW mem2 = o3Columns.getQuick(getSecondaryColumnIndex(i));
            if (mem2 != null) {
                mem2.jumpTo(0);
                mem2.putLong(0);
            }
        }
        activeColumns = o3Columns;
        activeNullSetters = o3NullSetters;
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
        this.txWriter.minTimestamp = Math.min(timestampMin, this.txWriter.minTimestamp);
        final long partitionSize = srcDataMax + srcOooPartitionHi - srcOooPartitionLo + 1;
        final long rowDelta = srcOooPartitionHi - srcOooMax;
        final int partitionIndex = txWriter.findAttachedPartitionIndexByLoTimestamp(partitionTimestamp);
        if (partitionTimestamp == lastPartitionTimestamp) {
            if (partitionMutates) {
                closeActivePartition(true);
            } else if (rowDelta < -1) {
                closeActivePartition(partitionSize);
            } else {
                setAppendPosition(partitionSize, false);
            }
        }

        LOG.debug().$("o3 partition update [timestampMin=").$ts(timestampMin)
                .$(", timestampMax=").$ts(timestampMax)
                .$(", last=").$(partitionTimestamp == lastPartitionTimestamp)
                .$(", partitionTimestamp=").$ts(partitionTimestamp)
                .$(", srcOooPartitionLo=").$(srcOooPartitionLo)
                .$(", srcOooPartitionHi=").$(srcOooPartitionHi)
                .$(", srcOooMax=").$(srcOooMax)
                .$(", srcDataMax=").$(srcDataMax)
                .$(", partitionMutates=").$(partitionMutates)
                .$(", lastPartitionTimestamp=").$(lastPartitionTimestamp)
                .$(", partitionSize=").$(partitionSize)
                .I$();

        if (partitionMutates) {
            final long srcDataTxn = txWriter.getPartitionNameTxnByIndex(partitionIndex);
            LOG.info()
                    .$("merged partition [table=`").utf8(tableName)
                    .$("`, ts=").$ts(partitionTimestamp)
                    .$(", txn=").$(txWriter.txn).$(']').$();
            txWriter.updatePartitionSizeByIndexAndTxn(partitionIndex, partitionSize);
            o3PartitionRemoveCandidates.add(partitionTimestamp, srcDataTxn);
            txWriter.bumpPartitionTableVersion();
        } else {
            if (partitionTimestamp != lastPartitionTimestamp) {
                txWriter.bumpPartitionTableVersion();
            }
            txWriter.updatePartitionSizeByIndex(partitionIndex, partitionTimestamp, partitionSize);
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
        if (txnScoreboard.isTxnAvailable(txWriter.getTxn() - 1)) {
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
                        LOG.info()
                                .$("purged [path=").$(other)
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
        if (transientRowsAdded > 0) {
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
        }
        txWriter.resetToLastPartition(committedTransientRowCount);
        return transientRowsAdded;
    }

    private void o3SetAppendOffset(
            int columnIndex,
            final int columnType,
            long o3RowCount
    ) {
        if (columnIndex != metadata.getTimestampIndex()) {
            MemoryARW o3DataMem = o3Columns.get(getPrimaryColumnIndex(columnIndex));
            MemoryARW o3IndexMem = o3Columns.get(getSecondaryColumnIndex(columnIndex));

            long size;
            if (null == o3IndexMem) {
                // Fixed size column
                size = o3RowCount << ColumnType.pow2SizeOf(columnType);
            } else {
                // Var size column
                if (o3RowCount > 0) {
                    // Usually we would find var col size of row count as (index[count] - index[count-1])
                    // but the record index[count] may not exist yet
                    // so the data size has to be calculated as (index[count-1] + len(data[count-1]) + 4)
                    // where len(data[count-1]) can be read as the int from var col data at offset index[count-1]
                    long prevOffset = o3IndexMem.getLong((o3RowCount - 1) * 8);
                    size = prevOffset + 2L * o3DataMem.getInt(prevOffset) + 4L;
                    o3IndexMem.jumpTo((o3RowCount + 1) * 8);
                } else {
                    size = 0;
                    o3IndexMem.jumpTo(0);
                }
            }

            o3DataMem.jumpTo(size);
        } else {
            // Special case, designated timestamp column
            o3TimestampMem.jumpTo(o3RowCount * 16);
        }
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
                                ColumnType.isVariableLength(type) ? oooSortVarColumnRef : oooSortFixColumnRef
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
        if (ColumnType.isVariableLength(type)) {
            o3SortVarColumn(i, type, mergedTimestamps, rowCount);
        } else {
            o3SortFixColumn(i, type, mergedTimestamps, rowCount);
        }
    }

    private void o3SortFixColumn(
            int columnIndex,
            final int columnType,
            long mergedTimestampsAddr,
            long valueCount
    ) {
        final int columnOffset = getPrimaryColumnIndex(columnIndex);
        final MemoryCARW mem = o3Columns.getQuick(columnOffset);
        final MemoryCARW mem2 = o3Columns2.getQuick(columnOffset);
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
        final MemoryCARW dataMem = o3Columns.getQuick(primaryIndex);
        final MemoryCARW indexMem = o3Columns.getQuick(secondaryIndex);
        final MemoryCARW dataMem2 = o3Columns2.getQuick(primaryIndex);
        final MemoryCARW indexMem2 = o3Columns2.getQuick(secondaryIndex);
        final long dataSize = dataMem.getAppendOffset();
        // ensure we have enough memory allocated
        final long srcDataAddr = dataMem.addressOf(0);
        final long srcDataSize = dataMem.size();
        final long srcIndxAddr = indexMem.addressOf(0);
        // exclude the trailing offset from shuffling
        final long srcIndxSize = indexMem.size();
        final long tgtDataAddr = dataMem2.resize(dataSize);
        final long tgtDataSize = dataMem2.size();
        final long tgtIndxAddr = indexMem2.resize(valueCount * Long.BYTES);
        final long tgtIndxSize = indexMem2.size();

        assert srcDataAddr != 0;
        assert srcIndxAddr != 0;
        assert tgtDataAddr != 0;
        assert tgtIndxAddr != 0;

        // add max offset so that we do not have conditionals inside loop
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
        indexMem.putLong(dataSize);
    }

    private void o3TimestampSetter(long timestamp) {
        o3TimestampMem.putLong128(timestamp, getO3RowCount0());
    }

    private void openColumnFiles(CharSequence name, int i, int plen) {
        MemoryMAR mem1 = getPrimaryColumn(i);
        MemoryMAR mem2 = getSecondaryColumn(i);

        try {
            mem1.of(ff, dFile(path.trimTo(plen), name), configuration.getDataAppendPageSize(), -1, MemoryTag.MMAP_TABLE_WRITER);
            if (mem2 != null) {
                mem2.of(ff, iFile(path.trimTo(plen), name), configuration.getDataAppendPageSize(), -1, MemoryTag.MMAP_TABLE_WRITER);
            }
        } finally {
            path.trimTo(plen);
        }
    }

    private void openFirstPartition(long timestamp) {
        final long ts = repairDataGaps(timestamp);
        openPartition(ts);
        populateDenseIndexerList();
        setAppendPosition(txWriter.getTransientRowCount(), false);
        if (performRecovery) {
            performRecovery();
        }
        txWriter.openFirstPartition(ts);
    }

    private void openNewColumnFiles(CharSequence name, boolean indexFlag, int indexValueBlockCapacity) {
        try {
            // open column files
            setStateForTimestamp(path, txWriter.getMaxTimestamp(), false);
            final int plen = path.length();
            final int columnIndex = columnCount - 1;

            // index must be created before column is initialised because
            // it uses primary column object as temporary tool
            if (indexFlag) {
                createIndexFiles(name, indexValueBlockCapacity, plen, true);
            }

            openColumnFiles(name, columnIndex, plen);
            if (txWriter.getTransientRowCount() > 0) {
                // write .top file
                writeColumnTop(name);
            }

            if (indexFlag) {
                ColumnIndexer indexer = indexers.getQuick(columnIndex);
                assert indexer != null;
                indexers.getQuick(columnIndex).configureFollowerAndWriter(configuration, path.trimTo(plen), name, getPrimaryColumn(columnIndex), txWriter.getTransientRowCount());
            }

            // configure append position for variable length columns
            MemoryMA mem2 = getSecondaryColumn(columnCount - 1);
            if (mem2 != null) {
                mem2.putLong(0);
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
                final ColumnIndexer indexer = metadata.isColumnIndexed(i) ? indexers.getQuick(i) : null;
                final long columnTop;

                // prepare index writer if column requires indexing
                if (indexer != null) {
                    // we have to create files before columns are open
                    // because we are reusing MAMemoryImpl object from columns list
                    createIndexFiles(name, metadata.getIndexValueBlockCapacity(i), plen, txWriter.getTransientRowCount() < 1);
                    indexer.closeSlider();
                }

                openColumnFiles(name, i, plen);
                columnTop = readColumnTop(ff, path, name, plen, tempMem16b, true);
                columnTops.extendAndSet(i, columnTop);

                if (indexer != null) {
                    indexer.configureFollowerAndWriter(configuration, path, name, getPrimaryColumn(i), columnTop);
                }
            }
            populateDenseIndexerList();
            LOG.info().$("switched partition [path='").$(path).I$();
        } catch (Throwable e) {
            distressed = true;
            throw e;
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

                todoMem.smallFile(ff, path, MemoryTag.MMAP_TABLE_WRITER);
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

    private void processCommandQueue() {
        final long cursor = commandSubSeq.next();
        if (cursor > -1) {
            final TableWriterTask cmd = messageBus.getTableWriterCommandQueue().get(cursor);
            if (cmd.getTableId() == getMetadata().getId()) {
                switch (cmd.getType()) {
                    case TableWriterTask.TSK_SLAVE_SYNC:
                        replPublishSyncEvent(cmd, cursor, commandSubSeq);
                        break;
                    default:
                        commandSubSeq.done(cursor);
                        break;
                }
            } else {
                commandSubSeq.done(cursor);
            }
        }
    }

    void purgeUnusedPartitions() {
        if (PartitionBy.isPartitioned(partitionBy)) {
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
                    return TableUtils.readLongOrFail(
                            ff,
                            fd,
                            0,
                            tempMem16b,
                            other
                    );
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
            ff.iterateDir(path.$(), (pUtf8NameZ, type) -> {
                if (Files.isDir(pUtf8NameZ, type)) {
                    path.trimTo(rootLen);
                    path.concat(pUtf8NameZ);
                    int plen = path.length();
                    removeLambda.remove(ff, dFile(path, columnName));
                    removeLambda.remove(ff, iFile(path.trimTo(plen), columnName));
                    removeLambda.remove(ff, topFile(path.trimTo(plen), columnName));
                    removeLambda.remove(ff, BitmapIndexUtils.keyFileName(path.trimTo(plen), columnName));
                    removeLambda.remove(ff, BitmapIndexUtils.valueFileName(path.trimTo(plen), columnName));
                }
            });

            if (ColumnType.isSymbol(columnType)) {
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
                nameOffset += Vm.getStorageLength(columnName);
            }

            return metaSwapIndex;
        } finally {
            ddlMem.close();
        }
    }

    private void removeIndexFiles(CharSequence columnName) {
        try {
            ff.iterateDir(path.$(), (pUtf8NameZ, type) -> {
                if (Files.isDir(pUtf8NameZ, type)) {
                    path.trimTo(rootLen);
                    path.concat(pUtf8NameZ);
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

    private void removePartitionDirectories0(long pUtf8NameZ, int type) {
        if (Files.isDir(pUtf8NameZ, type)) {
            path.trimTo(rootLen);
            path.concat(pUtf8NameZ).$();
            int errno;
            if ((errno = ff.rmdir(path)) != 0) {
                LOG.info().$("could not remove [path=").$(path).$(", errno=").$(errno).$(']').$();
            }
        }
    }

    private void removePartitionDirsNotAttached(long pUtf8NameZ, int type) {
        if (Files.isDir(pUtf8NameZ, type, fileNameSink)) {

            if (Chars.endsWith(fileNameSink, DETACHED_DIR_MARKER)) {
                // Do not remove detached partitions
                // They are probably about to be attached.
                return;
            }
            try {
                long txn = 0;
                int txnSep = Chars.indexOf(fileNameSink, '.');
                if (txnSep < 0) {
                    txnSep = fileNameSink.length();
                } else {
                    txn = Numbers.parseLong(fileNameSink, txnSep + 1, fileNameSink.length());
                }
                long dirTimestamp = partitionDirFmt.parse(fileNameSink, 0, txnSep, null);
                if (txn <= txWriter.txn &&
                        (txWriter.attachedPartitionsContains(dirTimestamp) || txWriter.isActivePartition(dirTimestamp))) {
                    return;
                }
            } catch (NumericException ignore) {
                // not a date?
                // ignore exception and remove directory
                // we rely on this behaviour to remove leftover directories created by OOO processing
            }
            path.trimTo(rootLen);
            path.concat(pUtf8NameZ).$();
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
            // Shift all subsequent symbol indexes by 1 back
            while (symColIndex < denseSymbolMapWriters.size()) {
                SymbolMapWriter w = denseSymbolMapWriters.getQuick(symColIndex);
                w.setSymbolIndexInTxWriter(symColIndex);
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
            ff.iterateDir(path.$(), (pUtf8NameZ, type) -> {
                if (Files.isDir(pUtf8NameZ, type)) {
                    path.trimTo(rootLen);
                    path.concat(pUtf8NameZ);
                    other.trimTo(rootLen);
                    other.concat(pUtf8NameZ);
                    int plen = path.length();
                    renameFileOrLog(ff, dFile(path.trimTo(plen), columnName), dFile(other.trimTo(plen), newName));
                    renameFileOrLog(ff, iFile(path.trimTo(plen), columnName), iFile(other.trimTo(plen), newName));
                    renameFileOrLog(ff, topFile(path.trimTo(plen), columnName), topFile(other.trimTo(plen), newName));
                    renameFileOrLog(ff, BitmapIndexUtils.keyFileName(path.trimTo(plen), columnName), BitmapIndexUtils.keyFileName(other.trimTo(plen), newName));
                    renameFileOrLog(ff, BitmapIndexUtils.valueFileName(path.trimTo(plen), columnName), BitmapIndexUtils.valueFileName(other.trimTo(plen), newName));
                }
            });

            if (ColumnType.isSymbol(columnType)) {
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
                nameOffset += Vm.getStorageLength(columnName);

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
        if (txWriter.getMaxTimestamp() != Numbers.LONG_NaN && PartitionBy.isPartitioned(partitionBy)) {
            long fixedRowCount = 0;
            long lastTimestamp = -1;
            long transientRowCount = this.txWriter.getTransientRowCount();
            long maxTimestamp = this.txWriter.getMaxTimestamp();
            try {
                final long tsLimit = partitionFloorMethod.floor(this.txWriter.getMaxTimestamp());
                for (long ts = getPartitionLo(txWriter.getMinTimestamp()); ts < tsLimit; ts = partitionCeilMethod.ceil(ts)) {
                    path.trimTo(rootLen);
                    setStateForTimestamp(path, ts, false);
                    int p = path.length();

                    long partitionSize = txWriter.getPartitionSizeByPartitionTimestamp(ts);
                    if (partitionSize >= 0 && ff.exists(path.$())) {
                        fixedRowCount += partitionSize;
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
                            LOG.debug().$("missing partition [name=").$(path.trimTo(p).$()).$(']').$();
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
                            transientRowCount = txWriter.getPartitionSizeByPartitionTimestamp(lastTimestamp);


                            // 2. read max timestamp
                            TableUtils.dFile(path.trimTo(p), metadata.getColumnName(metadata.getTimestampIndex()));
                            maxTimestamp = TableUtils.readLongAtOffset(ff, path, tempMem16b, (transientRowCount - 1) * Long.BYTES);
                            fixedRowCount -= transientRowCount;
                            txWriter.removeAttachedPartitions(txWriter.getMaxTimestamp());
                            LOG.info()
                                    .$("updated active partition [name=").$(path.trimTo(p).$())
                                    .$(", maxTimestamp=").$ts(maxTimestamp)
                                    .$(", transientRowCount=").$(transientRowCount)
                                    .$(", fixedRowCount=").$(txWriter.getFixedRowCount())
                                    .$(']').$();
                        }
                    }
                }
            } finally {
                path.trimTo(rootLen);
            }

            final long expectedSize = txWriter.unsafeReadFixedRowCount();
            if (expectedSize != fixedRowCount || maxTimestamp != this.txWriter.getMaxTimestamp()) {
                LOG.info()
                        .$("actual table size has been adjusted [name=`").utf8(tableName).$('`')
                        .$(", expectedFixedSize=").$(expectedSize)
                        .$(", actualFixedSize=").$(fixedRowCount)
                        .$(']').$();

                txWriter.reset(fixedRowCount, transientRowCount, maxTimestamp);
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
        if (PartitionBy.isPartitioned(partitionBy)) {
            removePartitionDirectories();
        }
        txWriter.truncate();
        clearTodoLog();
    }

    @Nullable TableSyncModel replHandleSyncCmd(TableWriterTask cmd) {
        final long instance = cmd.getInstance();
        final long sequence = cmd.getSequence();
        final int index = cmdSequences.keyIndex(instance);
        if (index < 0 && sequence <= cmdSequences.valueAt(index)) {
            return null;
        }
        cmdSequences.putAt(index, instance, sequence);
        final long txMemSize = Unsafe.getUnsafe().getLong(cmd.getData());
        return replCreateTableSyncModel(
                cmd.getData() + 8,
                cmd.getData() + txMemSize + 16,
                Unsafe.getUnsafe().getLong(cmd.getData() + txMemSize + 8)
        );
    }

    void replPublishSyncEvent0(TableSyncModel model, long tableId, long dst, long dstIP) {
        final long pubCursor = messageBus.getTableWriterEventPubSeq().next();
        if (pubCursor > -1) {
            final TableWriterTask event = messageBus.getTableWriterEventQueue().get(pubCursor);
            model.toBinary(event);
            event.setInstance(dst);
            event.setInstance(dstIP);
            event.setTableId(tableId);
            messageBus.getTableWriterEventPubSeq().done(pubCursor);
            LOG.info()
                    .$("published replication SYNC event [table=").$(tableName)
                    .$(", tableId=").$(tableId)
                    .$(", dst=").$(dst)
                    .$(", dstIP=").$ip(dstIP)
                    .I$();
        } else {
            LOG.error()
                    .$("could not publish slave sync event [table=").$(tableName)
                    .$(", tableId=").$(tableId)
                    .$(", dst=").$(dst)
                    .$(", dstIP=").$ip(dstIP)
                    .I$();
        }
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
        final long maxRow = txWriter.getTransientRowCount() - 1;
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
        int expectedMapWriters = txWriter.unsafeReadWriterCount();
        for (int i = 0; i < expectedMapWriters; i++) {
            denseSymbolMapWriters.getQuick(i).rollback(txWriter.unsafeReadSymbolWriterIndexOffset(i));
        }
    }

    private void rowAppend(ObjList<Runnable> activeNullSetters) {
        if ((masterRef & 1) != 0) {
            for (int i = 0; i < columnCount; i++) {
                if (rowValueIsNotNull.getQuick(i) < masterRef) {
                    activeNullSetters.getQuick(i).run();
                }
            }
            masterRef++;
        }
    }

    void rowCancel() {
        if ((masterRef & 1) == 0) {
            return;
        }

        if (hasO3()) {
            final long o3RowCount = getO3RowCount0();
            if (o3RowCount > 0) {
                // O3 mode and there are some rows.
                masterRef--;
                setO3AppendPosition(o3RowCount);
            } else {
                // Cancelling first row in o3, reverting to non-o3
                setO3AppendPosition(0);
                masterRef--;
                clearO3();
            }
            return;
        }

        long dirtyMaxTimestamp = txWriter.getMaxTimestamp();
        long dirtyTransientRowCount = txWriter.getTransientRowCount();
        long rollbackToMaxTimestamp = txWriter.cancelToMaxTimestamp();
        long rollbackToTransientRowCount = txWriter.cancelToTransientRowCount();

        // dirty timestamp should be 1 because newRow() increments it
        if (dirtyTransientRowCount == 1) {
            if (PartitionBy.isPartitioned(partitionBy)) {
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
                    rowActon = ROW_ACTION_OPEN_PARTITION;
                }

                // undo counts
                removeDirOnCancelRow = true;
                txWriter.cancelRow();
            } else {
                txWriter.cancelRow();
                // we only have one partition, jump to start on every column
                for (int i = 0; i < columnCount; i++) {
                    getPrimaryColumn(i).jumpTo(0L);
                    MemoryMA mem = getSecondaryColumn(i);
                    if (mem != null) {
                        mem.jumpTo(0L);
                        mem.putLong(0L);
                    }
                }
            }
        } else {
            txWriter.cancelRow();
            // we are staying within same partition, prepare append positions for row count
            boolean rowChanged = metadata.getTimestampIndex() >= 0; // adding new row already writes timestamp
            if (!rowChanged) {
                // verify if any of the columns have been changed
                // if not - we don't have to do
                for (int i = 0; i < columnCount; i++) {
                    if (rowValueIsNotNull.getQuick(i) == masterRef) {
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
        rowValueIsNotNull.fill(0, columnCount, --masterRef);
        txWriter.transientRowCount--;
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

    void setAppendPosition(final long position, boolean doubleAllocate) {
        for (int i = 0; i < columnCount; i++) {
            // stop calculating oversize as soon as we find first over-sized column
            setColumnSize(i, position, doubleAllocate);
        }
    }

    private void setColumnSize(int columnIndex, long size, boolean doubleAllocate) {
        MemoryMA mem1 = getPrimaryColumn(columnIndex);
        MemoryMA mem2 = getSecondaryColumn(columnIndex);
        int type = getColumnType(metaMem, columnIndex);
        final long pos = size - columnTops.getQuick(columnIndex);
        if (pos > 0) {
            // subtract column top
            final long m1pos;
            switch (ColumnType.tagOf(type)) {
                case ColumnType.BINARY:
                case ColumnType.STRING:
                    assert mem2 != null;
                    if (doubleAllocate) {
                        mem2.allocate(pos * Long.BYTES + Long.BYTES);
                    }
                    mem2.jumpTo(pos * Long.BYTES + Long.BYTES);
                    m1pos = Unsafe.getUnsafe().getLong(mem2.getAppendAddress() - 8);
                    break;
                default:
                    m1pos = pos << ColumnType.pow2SizeOf(type);
                    break;
            }
            if (doubleAllocate) {
                mem1.allocate(m1pos);
            }
            mem1.jumpTo(m1pos);
        } else {
            mem1.jumpTo(0);
            if (mem2 != null) {
                mem2.jumpTo(0);
                mem2.putLong(0);
            }
        }
    }

    private void setO3AppendPosition(final long position) {
        for (int i = 0; i < columnCount; i++) {
            o3SetAppendOffset(i, metadata.getColumnType(i), position);
        }
    }

    private void setRowValueNotNull(int columnIndex) {
        rowValueIsNotNull.setQuick(columnIndex, masterRef);
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
                txWriter.getPartitionNameTxnByPartitionTimestamp(partitionTimestampHi)
        );
        if (updatePartitionInterval) {
            this.partitionTimestampHi = partitionTimestampHi;
        }
    }

    private void switchPartition(long timestamp) {
        // Before partition can be switched we need to index records
        // added so far. Index writers will start point to different
        // files after switch.
        updateIndexes();
        txWriter.switchPartitions(timestamp);
        openPartition(timestamp);
        setAppendPosition(0, false);
    }

    private void syncColumns(int commitMode) {
        final boolean async = commitMode == CommitMode.ASYNC;
        for (int i = 0; i < columnCount; i++) {
            columns.getQuick(i * 2).sync(async);
            final MemoryMAR m2 = columns.getQuick(i * 2 + 1);
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

        LOG.info().$("parallel indexing [table=").$(tableName)
                .$(", indexCount=").$(indexCount)
                .$(", rowCount=").$(hi - lo)
                .I$();
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
        LOG.info().$("serial indexing [table=").$(tableName)
                .$(", indexCount=").$(indexCount)
                .$(", rowCount=").$(hi - lo)
                .I$();
        for (int i = 0, n = denseIndexers.size(); i < n; i++) {
            try {
                denseIndexers.getQuick(i).refreshSourceAndIndex(lo, hi);
            } catch (CairoException e) {
                // this is pretty severe, we hit some sort of limit
                LOG.error().$("index error {").$((Sinkable) e).$('}').$();
                throwDistressException(e);
            }
        }
        LOG.info().$("serial indexing done [table=").$(tableName).I$();
    }

    private void updateIndexesSlow() {
        final long hi = txWriter.getTransientRowCount();
        final long lo = txWriter.getAppendedPartitionCount() == 1 ? hi - txWriter.getLastTxSize() : 0;
        if (indexCount > 1 && parallelIndexerEnabled && hi - lo > configuration.getParallelIndexThreshold()) {
            updateIndexesParallel(lo, hi);
        } else {
            updateIndexesSerially(lo, hi);
        }
    }

    private void updateMaxTimestamp(long timestamp) {
        txWriter.updateMaxTimestamp(timestamp);
        this.timestampSetter.accept(timestamp);
    }

    private void validateSwapMeta(CharSequence columnName) {
        try {
            try {
                path.concat(META_SWAP_FILE_NAME);
                if (metaSwapIndex > 0) {
                    path.put('.').put(metaSwapIndex);
                }
                metaMem.smallFile(ff, path.$(), MemoryTag.MMAP_TABLE_WRITER);
                validationMap.clear();
                validate(ff, metaMem, validationMap, ColumnType.VERSION);
            } finally {
                metaMem.close();
                path.trimTo(rootLen);
            }
        } catch (CairoException e) {
            runFragile(RECOVER_FROM_META_RENAME_FAILURE, columnName, e);
        }
    }

    private void writeColumnEntry(int i) {
        ddlMem.putInt(getColumnType(metaMem, i));
        long flags = 0;
        if (isColumnIndexed(metaMem, i)) {
            flags |= META_FLAG_BIT_INDEXED;
        }

        if (isSequential(metaMem, i)) {
            flags |= META_FLAG_BIT_SEQUENTIAL;
        }
        ddlMem.putLong(flags);
        ddlMem.putInt(getIndexBlockCapacity(metaMem, i));
        ddlMem.putLong(getColumnHash(metaMem, i));
        ddlMem.skip(8);
    }

    private void writeColumnTop(CharSequence name) {
        writeColumnTop(name, txWriter.getTransientRowCount());
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
        todoMem.jumpTo(56);
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

    public interface Row {

        void append();

        void cancel();

        void putBin(int columnIndex, long address, long len);

        void putBin(int columnIndex, BinarySequence sequence);

        void putBool(int columnIndex, boolean value);

        void putByte(int columnIndex, byte value);

        void putChar(int columnIndex, char value);

        void putDate(int columnIndex, long value);

        void putDouble(int columnIndex, double value);

        void putFloat(int columnIndex, float value);

        void putGeoHash(int columnIndex, long value);

        void putGeoHashDeg(int index, double lat, double lon);

        void putGeoStr(int columnIndex, CharSequence value);

        void putInt(int columnIndex, int value);

        void putLong(int columnIndex, long value);

        void putLong256(int columnIndex, long l0, long l1, long l2, long l3);

        void putLong256(int columnIndex, Long256 value);

        void putLong256(int columnIndex, CharSequence hexString);

        void putLong256(int columnIndex, @NotNull CharSequence hexString, int start, int end);

        void putShort(int columnIndex, short value);

        void putStr(int columnIndex, CharSequence value);

        void putStr(int columnIndex, char value);

        void putStr(int columnIndex, CharSequence value, int pos, int len);

        void putSym(int columnIndex, CharSequence value);

        void putSym(int columnIndex, char value);

        void putSymIndex(int columnIndex, int symIndex);

        void putTimestamp(int columnIndex, long value);

        void putTimestamp(int columnIndex, CharSequence value);
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

    private class RowImpl implements Row {
        @Override
        public void append() {
            rowAppend(activeNullSetters);
        }

        @Override
        public void cancel() {
            rowCancel();
        }

        @Override
        public void putBin(int columnIndex, long address, long len) {
            getSecondaryColumn(columnIndex).putLong(getPrimaryColumn(columnIndex).putBin(address, len));
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putBin(int columnIndex, BinarySequence sequence) {
            getSecondaryColumn(columnIndex).putLong(getPrimaryColumn(columnIndex).putBin(sequence));
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putBool(int columnIndex, boolean value) {
            getPrimaryColumn(columnIndex).putBool(value);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putByte(int columnIndex, byte value) {
            getPrimaryColumn(columnIndex).putByte(value);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putChar(int columnIndex, char value) {
            getPrimaryColumn(columnIndex).putChar(value);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putDate(int columnIndex, long value) {
            putLong(columnIndex, value);
        }

        @Override
        public void putDouble(int columnIndex, double value) {
            getPrimaryColumn(columnIndex).putDouble(value);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putFloat(int columnIndex, float value) {
            getPrimaryColumn(columnIndex).putFloat(value);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putGeoHash(int index, long value) {
            int type = metadata.getColumnType(index);
            putGeoHash0(index, value, type);
        }

        @Override
        public void putGeoHashDeg(int index, double lat, double lon) {
            int type = metadata.getColumnType(index);
            putGeoHash0(index, GeoHashes.fromCoordinatesDegUnsafe(lat, lon, ColumnType.getGeoHashBits(type)), type);
        }

        @Override
        public void putGeoStr(int index, CharSequence hash) {
            long val;
            final int type = metadata.getColumnType(index);
            if (hash != null) {
                final int hashLen = hash.length();
                final int typeBits = ColumnType.getGeoHashBits(type);
                final int charsRequired = (typeBits - 1) / 5 + 1;
                if (hashLen < charsRequired) {
                    val = GeoHashes.NULL;
                } else {
                    try {
                        val = ColumnType.truncateGeoHashBits(
                                GeoHashes.fromString(hash, 0, charsRequired),
                                charsRequired * 5,
                                typeBits
                        );
                    } catch (NumericException e) {
                        val = GeoHashes.NULL;
                    }
                }
            } else {
                val = GeoHashes.NULL;
            }
            putGeoHash0(index, val, type);
        }

        @Override
        public void putInt(int columnIndex, int value) {
            getPrimaryColumn(columnIndex).putInt(value);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putLong(int columnIndex, long value) {
            getPrimaryColumn(columnIndex).putLong(value);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putLong256(int columnIndex, long l0, long l1, long l2, long l3) {
            getPrimaryColumn(columnIndex).putLong256(l0, l1, l2, l3);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putLong256(int columnIndex, Long256 value) {
            getPrimaryColumn(columnIndex).putLong256(value.getLong0(), value.getLong1(), value.getLong2(), value.getLong3());
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putLong256(int columnIndex, CharSequence hexString) {
            getPrimaryColumn(columnIndex).putLong256(hexString);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putLong256(int columnIndex, @NotNull CharSequence hexString, int start, int end) {
            getPrimaryColumn(columnIndex).putLong256(hexString, start, end);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putShort(int columnIndex, short value) {
            getPrimaryColumn(columnIndex).putShort(value);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putStr(int columnIndex, CharSequence value) {
            getSecondaryColumn(columnIndex).putLong(getPrimaryColumn(columnIndex).putStr(value));
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putStr(int columnIndex, char value) {
            getSecondaryColumn(columnIndex).putLong(getPrimaryColumn(columnIndex).putStr(value));
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putStr(int columnIndex, CharSequence value, int pos, int len) {
            getSecondaryColumn(columnIndex).putLong(getPrimaryColumn(columnIndex).putStr(value, pos, len));
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putSym(int columnIndex, CharSequence value) {
            getPrimaryColumn(columnIndex).putInt(symbolMapWriters.getQuick(columnIndex).put(value));
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putSym(int columnIndex, char value) {
            getPrimaryColumn(columnIndex).putInt(symbolMapWriters.getQuick(columnIndex).put(value));
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putSymIndex(int columnIndex, int symIndex) {
            getPrimaryColumn(columnIndex).putInt(symIndex);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putTimestamp(int columnIndex, long value) {
            putLong(columnIndex, value);
        }

        @Override
        public void putTimestamp(int columnIndex, CharSequence value) {
            // try UTC timestamp first (micro)
            long l;
            try {
                l = value != null ? IntervalUtils.parseFloorPartialDate(value) : Numbers.LONG_NaN;
            } catch (NumericException e) {
                throw CairoException.instance(0).put("Invalid timestamp: ").put(value);
            }
            putTimestamp(columnIndex, l);
        }

        private MemoryA getPrimaryColumn(int columnIndex) {
            return activeColumns.getQuick(getPrimaryColumnIndex(columnIndex));
        }

        private MemoryA getSecondaryColumn(int columnIndex) {
            return activeColumns.getQuick(getSecondaryColumnIndex(columnIndex));
        }

        private void putGeoHash0(int index, long value, int type) {
            final MemoryA primaryColumn = getPrimaryColumn(index);
            switch (ColumnType.tagOf(type)) {
                case ColumnType.GEOBYTE:
                    primaryColumn.putByte((byte) value);
                    break;
                case ColumnType.GEOSHORT:
                    primaryColumn.putShort((short) value);
                    break;
                case ColumnType.GEOINT:
                    primaryColumn.putInt((int) value);
                    break;
                default:
                    primaryColumn.putLong(value);
                    break;
            }
            setRowValueNotNull(index);
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
