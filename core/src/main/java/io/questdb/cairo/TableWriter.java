/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.Metrics;
import io.questdb.cairo.sql.AsyncWriterCommand;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.vm.NullMapWriter;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.*;
import io.questdb.cairo.wal.*;
import io.questdb.cairo.wal.seq.TableSequencer;
import io.questdb.griffin.DropIndexOperator;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.UpdateOperatorImpl;
import io.questdb.griffin.engine.ops.AbstractOperation;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.mp.*;
import io.questdb.std.*;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.tasks.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;

import static io.questdb.cairo.BitmapIndexUtils.keyFileName;
import static io.questdb.cairo.BitmapIndexUtils.valueFileName;
import static io.questdb.cairo.TableUtils.*;
import static io.questdb.cairo.sql.AsyncWriterCommand.Error.*;
import static io.questdb.cairo.wal.WalUtils.*;
import static io.questdb.std.Files.FILES_RENAME_OK;
import static io.questdb.tasks.TableWriterTask.*;

public class TableWriter implements TableWriterAPI, MetadataService, Closeable {
    public static final int O3_BLOCK_DATA = 2;
    public static final int O3_BLOCK_MERGE = 3;
    public static final int O3_BLOCK_NONE = -1;
    public static final int O3_BLOCK_O3 = 1;
    public static final int TIMESTAMP_MERGE_ENTRY_BYTES = Long.BYTES * 2;
    private static final ObjectFactory<MemoryCMOR> GET_MEMORY_CMOR = Vm::getMemoryCMOR;
    private static final long IGNORE = -1L;
    private static final Log LOG = LogFactory.getLog(TableWriter.class);
    private static final Runnable NOOP = () -> {
    };
    private static final Row NOOP_ROW = new NoOpRow();
    private static final int O3_ERRNO_FATAL = Integer.MAX_VALUE - 1;
    private static final int PARTITION_UPDATE_SINK_ENTRY_SIZE = 8;
    private static final int ROW_ACTION_NO_PARTITION = 1;
    private static final int ROW_ACTION_NO_TIMESTAMP = 2;
    private static final int ROW_ACTION_O3 = 3;
    private static final int ROW_ACTION_OPEN_PARTITION = 0;
    private static final int ROW_ACTION_SWITCH_PARTITION = 4;
    final ObjList<MemoryMA> columns;
    // Latest command sequence per command source.
    // Publisher source is identified by a long value
    private final AlterOperation alterOp = new AlterOperation();
    private final LongConsumer appendTimestampSetter;
    private final LongList columnTops;
    private final ColumnVersionWriter columnVersionWriter;
    private final MPSequence commandPubSeq;
    private final RingQueue<TableWriterTask> commandQueue;
    private final SCSequence commandSubSeq;
    private final CairoConfiguration configuration;
    private final MemoryMAR ddlMem;
    private final int defaultCommitMode;
    private final ObjList<ColumnIndexer> denseIndexers = new ObjList<>();
    private final ObjList<MapWriter> denseSymbolMapWriters;
    private final boolean directIOFlag;
    private final FilesFacade ff;
    private final StringSink fileNameSink = new StringSink();
    private final int fileOperationRetryCount;
    private final SOCountDownLatch indexLatch = new SOCountDownLatch();
    private final MemoryMR indexMem = Vm.getMRInstance();
    private final LongList indexSequences = new LongList();
    private final ObjList<ColumnIndexer> indexers;
    // This is the same message bus. When TableWriter instance created via CairoEngine, message bus is shared
    // and is owned by the engine. Since TableWriter would not have ownership of the bus it must not free it up.
    // On other hand when TableWrite is created outside CairoEngine, primarily in tests, the ownership of the
    // message bus is with the TableWriter. Therefore, message bus must be freed when writer is freed.
    // To indicate ownership, the message bus owned by the writer will be assigned to `ownMessageBus`. This reference
    // will be released by the writer
    private final MessageBus messageBus;
    private final MemoryMR metaMem;
    private final TableWriterMetadata metadata;
    private final Metrics metrics;
    private final int mkDirMode;
    private final ObjList<Runnable> nullSetters;
    private final ObjectPool<O3Basket> o3BasketPool = new ObjectPool<>(O3Basket::new, 64);
    private final ObjectPool<O3MutableAtomicInteger> o3ColumnCounters = new ObjectPool<>(O3MutableAtomicInteger::new, 64);
    private final int o3ColumnMemorySize;
    private final ObjList<MemoryCR> o3ColumnOverrides;
    private final SOUnboundedCountDownLatch o3DoneLatch = new SOUnboundedCountDownLatch();
    private final AtomicInteger o3ErrorCount = new AtomicInteger();
    private final long[] o3LastTimestampSpreads;
    private final AtomicLong o3PartitionUpdRemaining = new AtomicLong();
    private final boolean o3QuickSortEnabled;
    private final Path other;
    private final MessageBus ownMessageBus;
    private final boolean parallelIndexerEnabled;
    private final int partitionBy;
    private final PartitionBy.PartitionCeilMethod partitionCeilMethod;
    private final DateFormat partitionDirFmt;
    private final PartitionBy.PartitionFloorMethod partitionFloorMethod;
    private final LongList partitionRemoveCandidates = new LongList();
    private final Path path;
    private final AtomicLong physicallyWrittenRowsSinceLastCommit = new AtomicLong();
    private final int rootLen;
    private final FragileCode RECOVER_FROM_META_RENAME_FAILURE = this::recoverFromMetaRenameFailure;
    private final FindVisitor removePartitionDirectories = this::removePartitionDirectories0;
    private final Row row = new RowImpl();
    private final LongList rowValueIsNotNull = new LongList();
    private final TxReader slaveTxReader;
    private final ObjList<MapWriter> symbolMapWriters;
    private final IntList symbolRewriteMap = new IntList();
    private final MemoryMARW todoMem = Vm.getMARWInstance();
    private final TxWriter txWriter;
    private final FindVisitor removePartitionDirsNotAttached = this::removePartitionDirsNotAttached;
    private final TxnScoreboard txnScoreboard;
    private final Uuid uuid = new Uuid();
    private final LowerCaseCharSequenceIntHashMap validationMap = new LowerCaseCharSequenceIntHashMap();
    private final WeakClosableObjectPool<MemoryCMOR> walColumnMemoryPool;
    private final ObjList<MemoryCMOR> walMappedColumns = new ObjList<>();
    private ObjList<? extends MemoryA> activeColumns;
    private ObjList<Runnable> activeNullSetters;
    private ColumnVersionReader attachColumnVersionReader;
    private IndexBuilder attachIndexBuilder;
    private long attachMaxTimestamp;
    private MemoryCMR attachMetaMem;
    private TableWriterMetadata attachMetadata;
    private long attachMinTimestamp;
    private TxReader attachTxReader;
    private boolean avoidIndexOnCommit = false;
    private int columnCount;
    private long committedMasterRef;
    private String designatedTimestampColumnName;
    private boolean distressed = false;
    private DropIndexOperator dropIndexOperator;
    private int indexCount;
    private int lastErrno;
    private boolean lastOpenPartitionIsReadOnly;
    private long lastOpenPartitionTs = -1L;
    private long lastPartitionTimestamp;
    private LifecycleManager lifecycleManager;
    private int lockFd = -1;
    private long masterRef = 0L;
    private int metaPrevIndex;
    private final FragileCode RECOVER_FROM_TODO_WRITE_FAILURE = this::recoverFromTodoWriteFailure;
    private int metaSwapIndex;
    private long noOpRowCount;
    private DirectLongList o3ColumnTopSink;
    private ReadOnlyObjList<? extends MemoryCR> o3Columns;
    private long o3CommitBatchTimestampMin = Long.MAX_VALUE;
    private long o3EffectiveLag = 0L;
    private boolean o3InError = false;
    private long o3MasterRef = -1L;
    private ObjList<MemoryCARW> o3MemColumns;
    private ObjList<MemoryCARW> o3MemColumns2;
    private ObjList<Runnable> o3NullSetters;
    private ObjList<Runnable> o3NullSetters2;
    // o3PartitionUpdateSink (offset, description):
    // 0, partitionTimestamp
    // 1, timestampMin
    // 2, timestampMax
    // 3, srcOooPartitionLo
    // 4, srcOooPartitionHi
    // 5, partitionMutates ? 1 : 0
    // 6, srcOooMax
    // 7, srcDataMax
    private DirectLongList o3PartitionUpdateSink;
    private long o3RowCount;
    private MemoryMAT o3TimestampMem;
    private MemoryARW o3TimestampMemCpy;
    private long partitionTimestampHi;
    private boolean performRecovery;
    private boolean removeDirOnCancelRow = true;
    private int rowAction = ROW_ACTION_OPEN_PARTITION;
    private TableToken tableToken;
    private final O3ColumnUpdateMethod oooSortFixColumnRef = this::o3SortFixColumn;
    private final O3ColumnUpdateMethod oooSortVarColumnRef = this::o3SortVarColumn;
    private final O3ColumnUpdateMethod o3MergeLagVarColumnRef = this::o3MergeVarColumnLag;
    private final O3ColumnUpdateMethod o3MoveUncommittedRef = this::o3MoveUncommitted0;
    private final O3ColumnUpdateMethod o3MoveLagRef = this::o3MoveLag0;
    private final O3ColumnUpdateMethod o3MergeLagFixColumnRef = this::o3MergeFixColumnLag;
    private long tempMem16b = Unsafe.malloc(16, MemoryTag.NATIVE_TABLE_WRITER);
    private LongConsumer timestampSetter;
    private long todoTxn;
    private final FragileCode RECOVER_FROM_SYMBOL_MAP_WRITER_FAILURE = this::recoverFromSymbolMapWriterFailure;
    private final FragileCode RECOVER_FROM_SWAP_RENAME_FAILURE = this::recoverFromSwapRenameFailure;
    private final FragileCode RECOVER_FROM_COLUMN_OPEN_FAILURE = this::recoverOpenColumnFailure;
    private UpdateOperatorImpl updateOperatorImpl;

    public TableWriter(
            CairoConfiguration configuration,
            TableToken tableToken,
            MessageBus messageBus,
            MessageBus ownMessageBus,
            boolean lock,
            LifecycleManager lifecycleManager,
            CharSequence root,
            Metrics metrics
    ) {
        LOG.info().$("open '").utf8(tableToken.getTableName()).$('\'').$();
        this.configuration = configuration;
        this.directIOFlag = (Os.type != Os.WINDOWS || configuration.getWriterFileOpenOpts() != CairoConfiguration.O_NONE);
        this.metrics = metrics;
        this.ownMessageBus = ownMessageBus;
        this.messageBus = ownMessageBus != null ? ownMessageBus : messageBus;
        this.defaultCommitMode = configuration.getCommitMode();
        this.lifecycleManager = lifecycleManager;
        this.parallelIndexerEnabled = configuration.isParallelIndexingEnabled();
        this.ff = configuration.getFilesFacade();
        this.mkDirMode = configuration.getMkDirMode();
        this.fileOperationRetryCount = configuration.getFileOperationRetryCount();
        this.tableToken = tableToken;
        this.o3QuickSortEnabled = configuration.isO3QuickSortEnabled();
        this.o3ColumnMemorySize = configuration.getO3ColumnMemorySize();
        this.path = new Path().of(root).concat(tableToken);
        this.other = new Path().of(root).concat(tableToken);
        this.rootLen = path.length();
        try {
            if (lock) {
                lock();
            } else {
                this.lockFd = -1;
            }
            int todo = readTodo();
            if (todo == TODO_RESTORE_META) {
                repairMetaRename((int) todoMem.getLong(48));
            }
            this.ddlMem = Vm.getMARInstance();
            this.metaMem = Vm.getMRInstance();
            this.columnVersionWriter = openColumnVersionFile(ff, path, rootLen);

            openMetaFile(ff, path, rootLen, metaMem);
            this.metadata = new TableWriterMetadata(this.tableToken, metaMem);
            this.partitionBy = metaMem.getInt(META_OFFSET_PARTITION_BY);
            this.txWriter = new TxWriter(ff).ofRW(path.concat(TXN_FILE_NAME).$(), partitionBy);
            this.txnScoreboard = new TxnScoreboard(ff, configuration.getTxnScoreboardEntryCount()).ofRW(path.trimTo(rootLen));
            path.trimTo(rootLen);
            this.o3ColumnOverrides = metadata.isWalEnabled() ? new ObjList<>() : null;
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
                    LOG.error().$("ignoring unknown *todo* [code=").$(todo).I$();
                    break;
            }
            this.columnCount = metadata.getColumnCount();
            if (metadata.getTimestampIndex() > -1) {
                this.designatedTimestampColumnName = metadata.getColumnName(metadata.getTimestampIndex());
            }
            this.rowValueIsNotNull.extendAndSet(columnCount, 0);
            this.columns = new ObjList<>(columnCount * 2);
            this.o3MemColumns = new ObjList<>(columnCount * 2);
            this.o3MemColumns2 = new ObjList<>(columnCount * 2);
            this.o3Columns = this.o3MemColumns;
            this.activeColumns = columns;
            this.symbolMapWriters = new ObjList<>(columnCount);
            this.indexers = new ObjList<>(columnCount);
            this.denseSymbolMapWriters = new ObjList<>(metadata.getSymbolMapCount());
            this.nullSetters = new ObjList<>(columnCount);
            this.o3NullSetters = new ObjList<>(columnCount);
            this.o3NullSetters2 = new ObjList<>(columnCount);
            this.activeNullSetters = nullSetters;
            this.columnTops = new LongList(columnCount);
            this.partitionFloorMethod = PartitionBy.getPartitionFloorMethod(partitionBy);
            this.partitionCeilMethod = PartitionBy.getPartitionCeilMethod(partitionBy);
            if (PartitionBy.isPartitioned(partitionBy)) {
                this.partitionDirFmt = PartitionBy.getPartitionDirFormatMethod(partitionBy);
                this.partitionTimestampHi = txWriter.getLastPartitionTimestamp();
            } else {
                this.partitionDirFmt = null;
            }

            configureColumnMemory();
            configureTimestampSetter();
            this.appendTimestampSetter = timestampSetter;
            configureAppendPosition();
            purgeUnusedPartitions();
            clearTodoLog();
            this.slaveTxReader = new TxReader(ff);
            commandQueue = new RingQueue<>(
                    TableWriterTask::new,
                    configuration.getWriterCommandQueueSlotSize(),
                    configuration.getWriterCommandQueueCapacity(),
                    MemoryTag.NATIVE_REPL
            );
            commandSubSeq = new SCSequence();
            commandPubSeq = new MPSequence(commandQueue.getCycle());
            commandPubSeq.then(commandSubSeq).then(commandPubSeq);
            walColumnMemoryPool = new WeakClosableObjectPool<>(GET_MEMORY_CMOR, columnCount);
            o3LastTimestampSpreads = new long[configuration.getO3LagCalculationWindowsSize()];
            Arrays.fill(o3LastTimestampSpreads, 0);
        } catch (Throwable e) {
            doClose(false);
            throw e;
        }
    }

    @TestOnly
    public TableWriter(CairoConfiguration configuration, TableToken tableToken, Metrics metrics) {
        this(configuration, tableToken, null, new MessageBusImpl(configuration), true, DefaultLifecycleManager.INSTANCE, configuration.getRoot(), metrics);
    }

    @TestOnly
    public TableWriter(CairoConfiguration configuration, TableToken tableToken, MessageBus messageBus, Metrics metrics) {
        this(configuration, tableToken, null, messageBus, true, DefaultLifecycleManager.INSTANCE, configuration.getRoot(), metrics);
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

    @Override
    public void addColumn(CharSequence columnName, int columnType) {
        addColumn(
                columnName,
                columnType,
                configuration.getDefaultSymbolCapacity(),
                configuration.getDefaultSymbolCacheFlag(),
                false,
                0,
                false
        );
    }

    @Override
    public void addColumn(
            CharSequence columnName,
            int columnType,
            int symbolCapacity,
            boolean symbolCacheFlag,
            boolean isIndexed,
            int indexValueBlockCapacity
    ) {
        addColumn(
                columnName,
                columnType,
                symbolCapacity,
                symbolCacheFlag,
                isIndexed,
                indexValueBlockCapacity,
                false
        );
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
     * @param columnName              of column either ASCII or UTF8 encoded.
     * @param symbolCapacity          when column columnType is SYMBOL this parameter specifies approximate capacity for symbol map.
     *                                It should be equal to number of unique symbol values stored in the table and getting this
     *                                value badly wrong will cause performance degradation. Must be power of 2
     * @param symbolCacheFlag         when set to true, symbol values will be cached on Java heap.
     * @param columnType              {@link ColumnType}
     * @param isIndexed               configures column to be indexed or not
     * @param indexValueBlockCapacity approximation of number of rows for single index key, must be power of 2
     * @param isSequential            for columns that contain sequential values query optimiser can make assumptions on range searches (future feature)
     */
    public void addColumn(
            CharSequence columnName,
            int columnType,
            int symbolCapacity,
            boolean symbolCacheFlag,
            boolean isIndexed,
            int indexValueBlockCapacity,
            boolean isSequential
    ) {

        assert indexValueBlockCapacity == Numbers.ceilPow2(indexValueBlockCapacity) : "power of 2 expected";
        assert symbolCapacity == Numbers.ceilPow2(symbolCapacity) : "power of 2 expected";

        checkDistressed();
        checkColumnName(columnName);

        if (getColumnIndexQuiet(metaMem, columnName, columnCount) != -1) {
            throw CairoException.duplicateColumn(columnName);
        }

        commit();

        long columnNameTxn = getTxn();
        LOG.info().$("adding column '").utf8(columnName).$('[').$(ColumnType.nameOf(columnType)).$("], columnName txn ").$(columnNameTxn).$(" to ").$(path).$();

        // create new _meta.swp
        this.metaSwapIndex = addColumnToMeta(columnName, columnType, isIndexed, indexValueBlockCapacity, isSequential);

        // close _meta so we can rename it
        metaMem.close();

        // validate new meta
        validateSwapMeta(columnName);

        // rename _meta to _meta.prev
        renameMetaToMetaPrev(columnName);

        // after we moved _meta to _meta.prev
        // we have to have _todo to restore _meta should anything go wrong
        writeRestoreMetaTodo(columnName);

        // rename _meta.swp to _meta
        renameSwapMetaToMeta(columnName);

        if (ColumnType.isSymbol(columnType)) {
            try {
                createSymbolMapWriter(columnName, columnNameTxn, symbolCapacity, symbolCacheFlag);
            } catch (CairoException e) {
                runFragile(RECOVER_FROM_SYMBOL_MAP_WRITER_FAILURE, columnName, e);
            }
        } else {
            // maintain sparse list of symbol writers
            symbolMapWriters.extendAndSet(columnCount, NullMapWriter.INSTANCE);
        }

        // add column objects
        configureColumn(columnType, isIndexed, columnCount);
        if (isIndexed) {
            populateDenseIndexerList();
        }

        // increment column count
        columnCount++;

        // extend columnTop list to make sure row cancel can work
        // need for setting correct top is hard to test without being able to read from table
        int columnIndex = columnCount - 1;
        columnTops.extendAndSet(columnIndex, txWriter.getTransientRowCount());

        // Set txn number in the column version file to mark the transaction where the column is added
        columnVersionWriter.upsertDefaultTxnName(columnIndex, columnNameTxn, txWriter.getLastPartitionTimestamp());

        // create column files
        if (txWriter.getTransientRowCount() > 0 || !PartitionBy.isPartitioned(partitionBy)) {
            try {
                openNewColumnFiles(columnName, isIndexed, indexValueBlockCapacity);
            } catch (CairoException e) {
                runFragile(RECOVER_FROM_COLUMN_OPEN_FAILURE, columnName, e);
            }
        }

        try {
            // open _meta file
            openMetaFile(ff, path, rootLen, metaMem);

            // remove _todo
            clearTodoLog();
        } catch (CairoException e) {
            throwDistressException(e);
        }

        bumpStructureVersion();

        metadata.addColumn(columnName, columnType, isIndexed, indexValueBlockCapacity, columnIndex);

        LOG.info().$("ADDED column '").utf8(columnName).$('[').$(ColumnType.nameOf(columnType)).$("], columnName txn ").$(columnNameTxn).$(" to ").$(path).$();
    }

    @Override
    public void addIndex(CharSequence columnName, int indexValueBlockSize) {
        assert indexValueBlockSize == Numbers.ceilPow2(indexValueBlockSize) : "power of 2 expected";

        checkDistressed();

        final int columnIndex = getColumnIndexQuiet(metaMem, columnName, columnCount);

        if (columnIndex == -1) {
            throw CairoException.nonCritical().put("column '").put(columnName).put("' does not exist");
        }

        commit();

        if (isColumnIndexed(metaMem, columnIndex)) {
            throw CairoException.nonCritical().put("already indexed [column=").put(columnName).put(']');
        }

        final int existingType = getColumnType(metaMem, columnIndex);
        LOG.info().$("adding index to '").utf8(columnName).$('[').$(ColumnType.nameOf(existingType)).$(", path=").$(path).I$();

        if (!ColumnType.isSymbol(existingType)) {
            LOG.error().$("cannot create index for [column='").utf8(columnName).$(", type=").$(ColumnType.nameOf(existingType)).$(", path=").$(path).I$();
            throw CairoException.nonCritical().put("cannot create index for [column='").put(columnName).put(", type=").put(ColumnType.nameOf(existingType)).put(", path=").put(path).put(']');
        }

        // create indexer
        final SymbolColumnIndexer indexer = new SymbolColumnIndexer();

        final long columnNameTxn = columnVersionWriter.getColumnNameTxn(txWriter.getLastPartitionTimestamp(), columnIndex);
        try {
            try {
                // edge cases here are:
                // column spans only part of table - e.g. it was added after table was created and populated
                // column has top value, e.g. does not span entire partition
                // to this end, we have a super-edge case:

                // This piece of code is unbelievably fragile!
                if (PartitionBy.isPartitioned(partitionBy)) {
                    // run indexer for the whole table
                    indexHistoricPartitions(indexer, columnName, indexValueBlockSize);
                    long timestamp = txWriter.getMaxTimestamp();
                    if (timestamp != Numbers.LONG_NaN) {
                        path.trimTo(rootLen);
                        setStateForTimestamp(path, timestamp, false);
                        // create index in last partition
                        indexLastPartition(indexer, columnName, columnNameTxn, columnIndex, indexValueBlockSize);
                    }
                } else {
                    setStateForTimestamp(path, 0, false);
                    // create index in last partition
                    indexLastPartition(indexer, columnName, columnNameTxn, columnIndex, indexValueBlockSize);
                }
            } finally {
                path.trimTo(rootLen);
            }
        } catch (Throwable e) {
            LOG.error().$("rolling back index created so far [path=").$(path).I$();
            removeIndexFiles(columnName, columnIndex);
            throw e;
        }

        // set index flag in metadata and  create new _meta.swp
        metaSwapIndex = copyMetadataAndSetIndexAttrs(columnIndex, META_FLAG_BIT_INDEXED, indexValueBlockSize);

        swapMetaFile(columnName);

        indexers.extendAndSet(columnIndex, indexer);
        populateDenseIndexerList();

        TableColumnMetadata columnMetadata = metadata.getColumnMetadata(columnIndex);
        columnMetadata.setIndexed(true);
        columnMetadata.setIndexValueBlockCapacity(indexValueBlockSize);

        LOG.info().$("ADDED index to '").utf8(columnName).$('[').$(ColumnType.nameOf(existingType)).$("]' to ").$(path).$();
    }

    public void addPhysicallyWrittenRows(long rows) {
        // maybe not thread safe but hey it's just a metric
        physicallyWrittenRowsSinceLastCommit.addAndGet(rows);
        metrics.tableWriter().addPhysicallyWrittenRows(rows);
    }

    public void apply(AbstractOperation operation, long seqTxn) {
        try {
            setSeqTxn(seqTxn);
            long txnBefore = getTxn();
            operation.apply(this, true);
            if (txnBefore == getTxn()) {
                // Commit to update seqTxn
                txWriter.commit(defaultCommitMode, denseSymbolMapWriters);
            }
        } catch (CairoException ex) {
            // This is non-critical error, we can mark seqTxn as processed
            if (ex.isWALTolerable()) {
                try {
                    rollback(); // rollback in case on any dirty state
                    commitSeqTxn(seqTxn);
                } catch (Throwable th2) {
                    LOG.critical().$("could not rollback, table is distressed [table=").utf8(tableToken.getTableName()).$(", error=").$(th2).I$();
                }
            }
            throw ex;
        } catch (Throwable th) {
            try {
                rollback(); // rollback seqTxn
            } catch (Throwable th2) {
                LOG.critical().$("could not rollback, table is distressed [table=").utf8(tableToken.getTableName()).$(", error=").$(th2).I$();
            }
            throw th;
        }
    }

    @Override
    public long apply(AlterOperation alterOp, boolean contextAllowsAnyStructureChanges) throws AlterTableContextException {
        return alterOp.apply(this, contextAllowsAnyStructureChanges);
    }

    @Override
    public long apply(UpdateOperation operation) {
        return operation.apply(this, true);
    }

    @Override
    public AttachDetachStatus attachPartition(long timestamp) {
        // -1 means unknown size
        return attachPartition(timestamp, -1L);
    }

    /**
     * Attaches a partition to the table. If size is given, partition file data is not validated.
     *
     * @param timestamp     partition timestamp
     * @param partitionSize partition size in rows. Negative means unknown size.
     * @return attached status code
     */
    public AttachDetachStatus attachPartition(long timestamp, long partitionSize) {
        // Partitioned table must have a timestamp
        // SQL compiler will check that table has it
        assert metadata.getTimestampIndex() > -1;

        if (txWriter.attachedPartitionsContains(timestamp)) {
            LOG.info().$("partition is already attached [path=").$(path).I$();
            // TODO: potentially we can merge with existing data
            return AttachDetachStatus.ATTACH_ERR_PARTITION_EXISTS;
        }

        if (inTransaction()) {
            LOG.info().$("committing open transaction before applying attach partition command [table=").utf8(tableToken.getTableName())
                    .$(", partition=").$ts(timestamp).I$();
            commit();

            // Check that partition we're about to attach hasn't appeared after commit
            if (txWriter.attachedPartitionsContains(timestamp)) {
                LOG.info().$("partition is already attached [path=").$(path).I$();
                return AttachDetachStatus.ATTACH_ERR_PARTITION_EXISTS;
            }
        }

        // final name of partition folder after attach
        setPathForPartition(path.trimTo(rootLen), partitionBy, timestamp, false);
        TableUtils.txnPartitionConditionally(path, getTxn());
        path.$();

        if (ff.exists(path)) {
            // Very unlikely since txn is part of the folder name
            return AttachDetachStatus.ATTACH_ERR_DIR_EXISTS;
        }

        Path detachedPath = Path.PATH.get().of(configuration.getRoot()).concat(tableToken);
        setPathForPartition(detachedPath, partitionBy, timestamp, false);
        detachedPath.put(configuration.getAttachPartitionSuffix()).$();
        int detachedRootLen = detachedPath.length();
        boolean forceRenamePartitionDir = partitionSize < 0;

        boolean checkPassed = false;
        boolean isSoftLink;
        try {
            if (ff.exists(detachedPath)) {

                isSoftLink = ff.isSoftLink(detachedPath); // returns false regardless in Windows

                // detached metadata files validation
                CharSequence timestampColName = metadata.getColumnMetadata(metadata.getTimestampIndex()).getName();
                if (partitionSize > -1L) {
                    // read detachedMinTimestamp and detachedMaxTimestamp
                    readPartitionMinMax(ff, timestamp, detachedPath.trimTo(detachedRootLen), timestampColName, partitionSize);
                } else {
                    // read size, detachedMinTimestamp and detachedMaxTimestamp
                    partitionSize = readPartitionSizeMinMax(ff, timestamp, detachedPath.trimTo(detachedRootLen), timestampColName);
                }

                if (partitionSize < 1) {
                    return AttachDetachStatus.ATTACH_ERR_EMPTY_PARTITION;
                }

                if (forceRenamePartitionDir && !attachPrepare(timestamp, partitionSize, detachedPath, detachedRootLen)) {
                    attachValidateMetadata(partitionSize, detachedPath.trimTo(detachedRootLen), timestamp);
                }

                // main columnVersionWriter is now aligned with the detached partition values read from partition _cv file
                // in case of an error it has to be clean up

                if (forceRenamePartitionDir && configuration.attachPartitionCopy() && !isSoftLink) { // soft links are read-only, no copy involved
                    // Copy partition if configured to do so and it's not CSV import
                    if (ff.copyRecursive(detachedPath.trimTo(detachedRootLen), path, configuration.getMkDirMode()) == 0) {
                        LOG.info().$("copied partition dir [from=").$(detachedPath).$(", to=").$(path).I$();
                    } else {
                        LOG.error().$("could not copy [errno=").$(ff.errno()).$(", from=").$(detachedPath).$(", to=").$(path).I$();
                        return AttachDetachStatus.ATTACH_ERR_COPY;
                    }
                } else {
                    if (ff.rename(detachedPath.trimTo(detachedRootLen).$(), path) == FILES_RENAME_OK) {
                        LOG.info().$("renamed partition dir [from=").$(detachedPath).$(", to=").$(path).I$();
                    } else {
                        LOG.error().$("could not rename [errno=").$(ff.errno()).$(", from=").$(detachedPath).$(", to=").$(path).I$();
                        return AttachDetachStatus.ATTACH_ERR_RENAME;
                    }
                }

                checkPassed = true;
            } else {
                LOG.info().$("attach partition command failed, partition to attach does not exist [path=").$(detachedPath).I$();
                return AttachDetachStatus.ATTACH_ERR_MISSING_PARTITION;
            }
        } finally {
            path.trimTo(rootLen);
            if (!checkPassed) {
                columnVersionWriter.readUnsafe();
            }
        }

        try {
            // find out lo, hi ranges of partition attached as well as size
            assert timestamp <= attachMinTimestamp && attachMinTimestamp <= attachMaxTimestamp;
            long nextMinTimestamp = Math.min(attachMinTimestamp, txWriter.getMinTimestamp());
            long nextMaxTimestamp = Math.max(attachMaxTimestamp, txWriter.getMaxTimestamp());
            boolean appendPartitionAttached = size() == 0 || getPartitionLo(nextMaxTimestamp) > getPartitionLo(txWriter.getMaxTimestamp());

            txWriter.beginPartitionSizeUpdate();
            txWriter.updatePartitionSizeByTimestamp(timestamp, partitionSize, getTxn());
            txWriter.finishPartitionSizeUpdate(nextMinTimestamp, nextMaxTimestamp);
            if (isSoftLink) {
                txWriter.setPartitionReadOnlyByTimestamp(timestamp, true);
            }
            txWriter.bumpTruncateVersion();

            columnVersionWriter.commit();
            txWriter.setColumnVersion(columnVersionWriter.getVersion());
            txWriter.commit(defaultCommitMode, denseSymbolMapWriters);

            LOG.info().$("partition attached [table=").utf8(tableToken.getTableName())
                    .$(", partition=").$ts(timestamp).I$();

            if (appendPartitionAttached) {
                LOG.info().$("switch partition after partition attach [tableName=").utf8(tableToken.getTableName())
                        .$(", partition=").$ts(timestamp).I$();
                freeColumns(true);
                configureAppendPosition();
            }
            return AttachDetachStatus.OK;
        } catch (Throwable e) {
            // This is pretty serious, after partition copied there are no OS operations to fail
            // Do full rollback to clean up the state
            LOG.critical().$("failed on attaching partition to the table and rolling back [tableName=").utf8(tableToken.getTableName())
                    .$(", error=").$(e).I$();
            rollback();
            throw e;
        }
    }

    @Override
    public void changeCacheFlag(int columnIndex, boolean cache) {
        checkDistressed();

        commit();

        final MapWriter symbolMapWriter = symbolMapWriters.getQuick(columnIndex);
        if (symbolMapWriter.isCached() != cache) {
            symbolMapWriter.updateCacheFlag(cache);
        } else {
            return;
        }
        updateMetaStructureVersion();
    }

    public boolean checkScoreboardHasReadersBeforeLastCommittedTxn() {
        long lastCommittedTxn = txWriter.getTxn();
        try {
            if (txnScoreboard.acquireTxn(lastCommittedTxn)) {
                txnScoreboard.releaseTxn(lastCommittedTxn);
            }
        } catch (CairoException ex) {
            // Scoreboard can be over allocated, don't stall writing because of that.
            // Schedule async purge and continue
            LOG.error().$("cannot lock last txn in scoreboard, partition purge will be scheduled [table=")
                    .utf8(tableToken.getTableName())
                    .$(", txn=").$(lastCommittedTxn)
                    .$(", error=").$(ex.getFlyweightMessage())
                    .$(", errno=").$(ex.getErrno()).I$();
        }

        return txnScoreboard.getMin() != lastCommittedTxn;
    }

    @Override
    public void close() {
        if (lifecycleManager.close() && isOpen()) {
            doClose(true);
        }
    }

    @Override
    public long commit() {
        return commit(defaultCommitMode);
    }

    public long commit(int commitMode) {
        return commit(commitMode, 0);
    }

    public void commitSeqTxn(long seqTxn) {
        txWriter.setSeqTxn(seqTxn);
        txWriter.commit(defaultCommitMode, denseSymbolMapWriters);
    }

    public void destroy() {
        // Closes all the files and makes this instance unusable e.g. it cannot return to the pool on close.
        LOG.info().$("closing table files [table=").utf8(tableToken.getTableName())
                .$(", dirName=").utf8(tableToken.getDirName()).I$();
        distressed = true;
        doClose(false);
    }

    public AttachDetachStatus detachPartition(long timestamp) {
        // Should be checked by SQL compiler
        assert metadata.getTimestampIndex() > -1;
        assert PartitionBy.isPartitioned(partitionBy);

        if (inTransaction()) {
            LOG.info()
                    .$("committing open transaction before applying detach partition command [table=")
                    .utf8(tableToken.getTableName())
                    .$(", partition=").$ts(timestamp)
                    .I$();
            commit();
        }

        int partitionIndex = txWriter.getPartitionIndex(timestamp);
        if (partitionIndex == -1) {
            assert !txWriter.attachedPartitionsContains(timestamp);
            return AttachDetachStatus.DETACH_ERR_MISSING_PARTITION;
        }

        long maxTimestamp = txWriter.getMaxTimestamp();
        if (timestamp == getPartitionLo(maxTimestamp)) {
            return AttachDetachStatus.DETACH_ERR_ACTIVE;
        }
        long minTimestamp = txWriter.getMinTimestamp();

        long partitionNameTxn = txWriter.getPartitionNameTxn(partitionIndex);
        Path detachedPath = Path.PATH.get();

        try {
            // path: partition folder to be detached
            setPathForPartition(path, rootLen, partitionBy, timestamp, partitionNameTxn);
            if (!ff.exists(path.$())) {
                LOG.error().$("partition folder does not exist [path=").$(path).I$();
                return AttachDetachStatus.DETACH_ERR_MISSING_PARTITION_DIR;
            }

            final int detachedPathLen;
            AttachDetachStatus attachDetachStatus;
            if (ff.isSoftLink(path)) {
                detachedPathLen = 0;
                attachDetachStatus = AttachDetachStatus.OK;
                LOG.info().$("detaching partition via unlink [path=").$(path).I$();
            } else {

                detachedPath.of(configuration.getRoot()).concat(tableToken.getDirName());
                int detachedRootLen = detachedPath.length();
                // detachedPath: detached partition folder
                if (!ff.exists(detachedPath.slash$())) {
                    // the detached and standard folders can have different roots
                    // (server.conf: cairo.sql.detached.root)
                    if (0 != ff.mkdirs(detachedPath, mkDirMode)) {
                        LOG.error().$("could no create detached partition folder [errno=").$(ff.errno())
                                .$(", path=").$(detachedPath).I$();
                        return AttachDetachStatus.DETACH_ERR_MKDIR;
                    }
                }
                setPathForPartition(detachedPath.trimTo(detachedRootLen), partitionBy, timestamp, false);
                detachedPath.put(DETACHED_DIR_MARKER).$();
                detachedPathLen = detachedPath.length();
                if (ff.exists(detachedPath)) {
                    LOG.error().$("detached partition folder already exist [path=").$(detachedPath).I$();
                    return AttachDetachStatus.DETACH_ERR_ALREADY_DETACHED;
                }

                // Hard link partition folder recursive to partition.detached
                if (ff.hardLinkDirRecursive(path, detachedPath, mkDirMode) != 0) {
                    if (ff.isCrossDeviceCopyError(ff.errno())) {
                        // Cross drive operation. Make full copy to another device.
                        if (ff.copyRecursive(path, detachedPath, mkDirMode) != 0) {
                            LOG.critical().$("could not copy detached partition [errno=").$(ff.errno())
                                    .$(", from=").$(path)
                                    .$(", to=").$(detachedPath)
                                    .I$();
                            return AttachDetachStatus.DETACH_ERR_COPY;
                        }
                    } else {
                        LOG.critical().$("could not create hard link to detached partition [errno=").$(ff.errno())
                                .$(", from=").$(path)
                                .$(", to=").$(detachedPath)
                                .I$();
                        return AttachDetachStatus.DETACH_ERR_HARD_LINK;
                    }
                }

                // copy _meta, _cv and _txn to partition.detached _meta, _cv and _txn
                other.of(path).trimTo(rootLen).concat(META_FILE_NAME).$(); // exists already checked
                detachedPath.trimTo(detachedPathLen).concat(META_FILE_NAME).$();

                attachDetachStatus = AttachDetachStatus.OK;
                if (-1 == copyOverwrite(detachedPath)) {
                    attachDetachStatus = AttachDetachStatus.DETACH_ERR_COPY_META;
                    LOG.critical().$("could not copy [errno=").$(ff.errno())
                            .$(", from=").$(other)
                            .$(", to=").$(detachedPath)
                            .I$();
                } else {
                    other.parent().concat(COLUMN_VERSION_FILE_NAME).$();
                    detachedPath.parent().concat(COLUMN_VERSION_FILE_NAME).$();
                    if (-1 == copyOverwrite(detachedPath)) {
                        attachDetachStatus = AttachDetachStatus.DETACH_ERR_COPY_META;
                        LOG.critical().$("could not copy [errno=").$(ff.errno())
                                .$(", from=").$(other)
                                .$(", to=").$(detachedPath)
                                .I$();
                    } else {
                        other.parent().concat(TXN_FILE_NAME).$();
                        detachedPath.parent().concat(TXN_FILE_NAME).$();
                        if (-1 == copyOverwrite(detachedPath)) {
                            attachDetachStatus = AttachDetachStatus.DETACH_ERR_COPY_META;
                            LOG.critical().$("could not copy [errno=").$(ff.errno())
                                    .$(", from=").$(other)
                                    .$(", to=").$(detachedPath)
                                    .I$();
                        }
                    }
                }
            }

            if (attachDetachStatus == AttachDetachStatus.OK) {
                // find out if we are removing min partition
                long nextMinTimestamp = minTimestamp;
                if (timestamp == txWriter.getPartitionTimestamp(0)) {
                    other.of(path).trimTo(rootLen);
                    nextMinTimestamp = readMinTimestamp(txWriter.getPartitionTimestamp(1));
                }

                // all good, commit
                txWriter.beginPartitionSizeUpdate();
                txWriter.removeAttachedPartitions(timestamp);
                txWriter.setMinTimestamp(nextMinTimestamp);
                txWriter.finishPartitionSizeUpdate(nextMinTimestamp, txWriter.getMaxTimestamp());
                txWriter.bumpTruncateVersion();

                columnVersionWriter.removePartition(timestamp);
                columnVersionWriter.commit();

                txWriter.setColumnVersion(columnVersionWriter.getVersion());
                txWriter.commit(defaultCommitMode, denseSymbolMapWriters);
                // return at the end of the method after removing partition directory
            } else {
                // rollback detached copy
                detachedPath.trimTo(detachedPathLen).slash().$();
                if (ff.rmdir(detachedPath) != 0) {
                    LOG.error()
                            .$("could not rollback detached copy (rmdir) [errno=").$(ff.errno())
                            .$(", undo=").$(detachedPath)
                            .$(", original=").$(path)
                            .I$();
                }
                return attachDetachStatus;
            }
        } finally {
            path.trimTo(rootLen);
            other.trimTo(rootLen);
        }
        safeDeletePartitionDir(timestamp, partitionNameTxn);
        return AttachDetachStatus.OK;
    }

    @Override
    public void dropIndex(CharSequence columnName) {
        checkDistressed();

        final int columnIndex = getColumnIndexQuiet(metaMem, columnName, columnCount);
        if (columnIndex == -1) {
            throw CairoException.invalidMetadata("Column does not exist", columnName);
        }
        if (!isColumnIndexed(metaMem, columnIndex)) {
            // if a column is indexed, it is al so of type SYMBOL
            throw CairoException.invalidMetadata("Column is not indexed", columnName);
        }
        final int defaultIndexValueBlockSize = Numbers.ceilPow2(configuration.getIndexValueBlockSize());

        if (inTransaction()) {
            LOG.info()
                    .$("committing current transaction before DROP INDEX execution [txn=").$(txWriter.getTxn())
                    .$(", table=").utf8(tableToken.getTableName())
                    .$(", column=").utf8(columnName)
                    .I$();
            commit();
        }

        try {
            LOG.info().$("BEGIN DROP INDEX [txn=").$(txWriter.getTxn())
                    .$(", table=").utf8(tableToken.getTableName())
                    .$(", column=").utf8(columnName)
                    .I$();
            // drop index
            if (dropIndexOperator == null) {
                dropIndexOperator = new DropIndexOperator(configuration, messageBus, this, path, other, rootLen);
            }
            dropIndexOperator.executeDropIndex(columnName, columnIndex); // upserts column version in partitions
            // swap meta commit
            metaSwapIndex = copyMetadataAndSetIndexAttrs(columnIndex, META_FLAG_BIT_NOT_INDEXED, defaultIndexValueBlockSize);
            swapMetaFile(columnName); // bumps structure version, this is in effect a commit
            // refresh metadata
            TableColumnMetadata columnMetadata = metadata.getColumnMetadata(columnIndex);
            columnMetadata.setIndexed(false);
            columnMetadata.setIndexValueBlockCapacity(defaultIndexValueBlockSize);
            // remove indexer
            ColumnIndexer columnIndexer = indexers.getQuick(columnIndex);
            if (columnIndexer != null) {
                indexers.setQuick(columnIndex, null);
                Misc.free(columnIndexer);
                populateDenseIndexerList();
            }
            // purge old column versions
            dropIndexOperator.purgeOldColumnVersions();
            LOG.info().$("END DROP INDEX [txn=").$(txWriter.getTxn())
                    .$(", table=").utf8(tableToken.getTableName())
                    .$(", column=").utf8(columnName)
                    .I$();
        } catch (Throwable e) {
            throw CairoException.critical(0)
                    .put("Cannot DROP INDEX for [txn=").put(txWriter.getTxn())
                    .put(", table=").put(tableToken.getTableName())
                    .put(", column=").put(columnName)
                    .put("]: ").put(e.getMessage());
        }
    }

    public int getColumnIndex(CharSequence name) {
        int index = metadata.getColumnIndexQuiet(name);
        if (index > -1) {
            return index;
        }
        throw CairoException.critical(0).put("column '").put(name).put("' does not exist");
    }

    public long getColumnNameTxn(long partitionTimestamp, int columnIndex) {
        return columnVersionWriter.getColumnNameTxn(partitionTimestamp, columnIndex);
    }

    public long getColumnTop(long partitionTimestamp, int columnIndex, long defaultValue) {
        long colTop = columnVersionWriter.getColumnTop(partitionTimestamp, columnIndex);
        return colTop > -1L ? colTop : defaultValue;
    }

    @TestOnly
    public ObjList<MapWriter> getDenseSymbolMapWriters() {
        return denseSymbolMapWriters;
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

    @Override
    public long getMetaMaxUncommittedRows() {
        return metadata.getMaxUncommittedRows();
    }

    @Override
    public TableRecordMetadata getMetadata() {
        return metadata;
    }

    public long getO3RowCount() {
        return hasO3() ? getO3RowCount0() : 0L;
    }

    @Override
    public int getPartitionBy() {
        return partitionBy;
    }

    public int getPartitionCount() {
        return txWriter.getPartitionCount();
    }

    public long getPartitionNameTxn(int partitionIndex) {
        return txWriter.getPartitionNameTxn(partitionIndex);
    }

    public long getPartitionSize(int partitionIndex) {
        if (partitionIndex == txWriter.getPartitionCount() - 1 || !PartitionBy.isPartitioned(partitionBy)) {
            return txWriter.getTransientRowCount();
        }
        return txWriter.getPartitionSize(partitionIndex);
    }

    public long getPartitionTimestamp(int partitionIndex) {
        return txWriter.getPartitionTimestamp(partitionIndex);
    }

    public long getPhysicallyWrittenRowsSinceLastCommit() {
        return physicallyWrittenRowsSinceLastCommit.get();
    }

    public long getRowCount() {
        return txWriter.getRowCount();
    }

    public long getSeqTxn() {
        return txWriter.getSeqTxn();
    }

    @Override
    public long getStructureVersion() {
        return txWriter.getStructureVersion();
    }

    @Override
    public int getSymbolCountWatermark(int columnIndex) {
        // We don't need the watermark for non-WAL tables.
        return -1;
    }

    public int getSymbolIndexNoTransientCountUpdate(int columnIndex, CharSequence symValue) {
        return symbolMapWriters.getQuick(columnIndex).put(symValue, SymbolValueCountCollector.NOOP);
    }

    public MapWriter getSymbolMapWriter(int columnIndex) {
        return symbolMapWriters.getQuick(columnIndex);
    }

    @Override
    public TableToken getTableToken() {
        return tableToken;
    }

    public long getTransientRowCount() {
        return txWriter.getTransientRowCount();
    }

    public long getTruncateVersion() {
        return txWriter.getTruncateVersion();
    }

    @TestOnly
    public TxWriter getTxWriter() {
        return txWriter;
    }

    public long getTxn() {
        return txWriter.getTxn();
    }

    public TxnScoreboard getTxnScoreboard() {
        return txnScoreboard;
    }

    @Override
    public long getUncommittedRowCount() {
        return (masterRef - committedMasterRef) >> 1;
    }

    @Override
    public UpdateOperator getUpdateOperator() {
        if (updateOperatorImpl == null) {
            updateOperatorImpl = new UpdateOperatorImpl(configuration, messageBus, this, path, rootLen);
        }
        return updateOperatorImpl;
    }

    public boolean hasO3() {
        return o3MasterRef > -1;
    }

    @Override
    public void ic(long o3MaxLag) {
        commit(defaultCommitMode, o3MaxLag);
    }

    public void ic(int commitMode) {
        commit(commitMode, metadata.getO3MaxLag());
    }

    @Override
    public void ic() {
        commit(defaultCommitMode, metadata.getO3MaxLag());
    }

    public boolean inTransaction() {
        return txWriter != null && (txWriter.inTransaction() || hasO3() || columnVersionWriter.hasChanges());
    }

    public boolean isOpen() {
        return tempMem16b != 0;
    }

    public boolean isPartitionReadOnly(int partitionIndex) {
        return txWriter.isPartitionReadOnly(partitionIndex);
    }

    public void markSeqTxnCommitted(long seqTxn) {
        setSeqTxn(seqTxn);
        txWriter.commit(defaultCommitMode, denseSymbolMapWriters);
    }

    @Override
    public Row newRow() {
        return newRow(0L);
    }

    @Override
    public Row newRow(long timestamp) {
        switch (rowAction) {
            case ROW_ACTION_OPEN_PARTITION:

                if (timestamp < Timestamps.O3_MIN_TS) {
                    throw CairoException.nonCritical().put("timestamp before 1970-01-01 is not allowed");
                }

                if (txWriter.getMaxTimestamp() == Long.MIN_VALUE) {
                    txWriter.setMinTimestamp(timestamp);
                    openFirstPartition(timestamp);
                }
                // fall thru

                rowAction = ROW_ACTION_SWITCH_PARTITION;

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
                if (lastOpenPartitionIsReadOnly) {
                    masterRef--;
                    noOpRowCount++;
                    return NOOP_ROW;
                }
                updateMaxTimestamp(timestamp);
                break;
            case ROW_ACTION_NO_PARTITION:

                if (timestamp < Timestamps.O3_MIN_TS) {
                    throw CairoException.nonCritical().put("timestamp before 1970-01-01 is not allowed");
                }

                if (timestamp < txWriter.getMaxTimestamp()) {
                    throw CairoException.nonCritical().put("Cannot insert rows out of order to non-partitioned table. Table=").put(path);
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

    public void o3BumpErrorCount() {
        o3ErrorCount.incrementAndGet();
    }

    public void openLastPartition() {
        try {
            openPartition(txWriter.getLastPartitionTimestamp());
            setAppendPosition(txWriter.getTransientRowCount(), false);
        } catch (Throwable e) {
            freeColumns(false);
            throw e;
        }
    }

    public void processCommandQueue(TableWriterTask cmd, Sequence commandSubSeq, long cursor, boolean contextAllowsAnyStructureChanges) {
        if (cmd.getTableId() == getMetadata().getTableId()) {
            switch (cmd.getType()) {
                case CMD_ALTER_TABLE:
                    processAsyncWriterCommand(alterOp, cmd, cursor, commandSubSeq, contextAllowsAnyStructureChanges);
                    break;
                case CMD_UPDATE_TABLE:
                    processAsyncWriterCommand(cmd.getAsyncWriterCommand(), cmd, cursor, commandSubSeq, false);
                    break;
                default:
                    LOG.error().$("unknown TableWriterTask type, ignored: ").$(cmd.getType()).$();
                    // Don't block the queue even if command is unknown
                    commandSubSeq.done(cursor);
                    break;
            }
        } else {
            LOG.info()
                    .$("not my command [cmdTableId=").$(cmd.getTableId())
                    .$(", cmdTableName=").$(cmd.getTableToken())
                    .$(", myTableId=").$(getMetadata().getTableId())
                    .$(", myTableName=").utf8(tableToken.getTableName())
                    .I$();
            commandSubSeq.done(cursor);
        }
    }

    public boolean processWalBlock(
            @Transient Path walPath,
            int timestampIndex,
            boolean ordered,
            long rowLo,
            long rowHi,
            long o3TimestampMin,
            long o3TimestampMax,
            SymbolMapDiffCursor mapDiffCursor,
            long commitToTimestamp
    ) {
        this.lastPartitionTimestamp = partitionFloorMethod.floor(partitionTimestampHi);
        long partitionTimestampHiLimit = partitionCeilMethod.ceil(partitionTimestampHi) - 1;
        int walRootPathLen = walPath.length();

        try {
            mmapWalColumns(walPath, timestampIndex, rowLo, rowHi);
            long o3TimestampOffset = o3TimestampMem.getAppendOffset();
            long lagRowCount = o3TimestampOffset >> 4;
            if (lagRowCount > 0) {
                o3TimestampMin = Math.min(o3TimestampMin, getTimestampIndexValue(o3TimestampMem.getAddress(), 0));
            }

            try {
                long timestampAddr;
                long o3Lo = rowLo;
                long o3Hi = rowHi;
                final boolean copiedToMemory;
                final boolean needsOrdering = !ordered || lagRowCount > 0;
                final long symbolRowLo = needsOrdering || commitToTimestamp < 0 ? lagRowCount : rowLo;
                o3Columns = remapWalSymbols(mapDiffCursor, rowLo, rowHi, walPath, symbolRowLo);
                MemoryCR walTimestampColumn = walMappedColumns.getQuick(getPrimaryColumnIndex(timestampIndex));

                if (commitToTimestamp < o3TimestampMin) {
                    // Don't commit anything, move everything to memory instead.
                    // This usually happens when WAL transactions are very small, so it's faster
                    // to squash several of them together before writing anything to disk.
                    LOG.debug().$("all WAL rows copied to LAG [table=").$(tableToken).I$();
                    // This will copy data from mmap files to memory.
                    // Symbols are already mapped to the correct destination.
                    o3ShiftLagRowsUp(timestampIndex, o3Hi - o3Lo, o3Lo, lagRowCount, true);
                    return false;
                }

                if (needsOrdering) {
                    LOG.info().$("sorting WAL [table=").$(tableToken)
                            .$(", ordered=").$(ordered)
                            .$(", lagRowCount=").$(lagRowCount)
                            .$(", rowLo=").$(rowLo)
                            .$(", rowHi=").$(rowHi).I$();

                    final long timestampMemorySize = (rowHi - rowLo) << 4;
                    o3TimestampMem.jumpTo(o3TimestampOffset + timestampMemorySize);
                    o3TimestampMemCpy.jumpTo(o3TimestampOffset + timestampMemorySize);

                    timestampAddr = o3TimestampMem.getAddress();
                    final long mappedTimestampIndexAddr = walTimestampColumn.addressOf(rowLo << 4);
                    Vect.radixSortABLongIndexAscInA(
                            timestampAddr,
                            lagRowCount,
                            mappedTimestampIndexAddr,
                            rowHi - rowLo,
                            o3TimestampMemCpy.addressOf(0)
                    );
                    o3MergeIntoLag(timestampAddr, lagRowCount, rowLo, rowHi, timestampIndex);

                    // Sorted data is now sorted in memory copy of the data from mmap files
                    // Row indexes start from 0, not rowLo
                    o3Hi = rowHi - rowLo + lagRowCount;
                    o3Lo = 0L;
                    lagRowCount = 0L;
                    o3Columns = o3MemColumns;
                    copiedToMemory = true;
                    o3TimestampMin = getTimestampIndexValue(timestampAddr, o3Lo);
                    o3TimestampMax = getTimestampIndexValue(timestampAddr, o3Hi - 1);
                } else {
                    timestampAddr = walTimestampColumn.addressOf(0);
                    copiedToMemory = false;
                }

                if (commitToTimestamp < o3TimestampMin) {
                    // Don't commit anything, it is enough to move everything to memory instead.
                    // Copying is already done while sorting at the point so we can finish here.
                    return false;
                } else {
                    if (commitToTimestamp < o3TimestampMax) {
                        final long lagThresholdRow = 1 +
                                Vect.boundedBinarySearchIndexT(
                                        timestampAddr,
                                        commitToTimestamp,
                                        o3Lo,
                                        o3Hi - 1,
                                        BinarySearch.SCAN_DOWN
                                );
                        lagRowCount = o3Hi - o3Lo - lagThresholdRow;
                        assert lagRowCount > 0 && lagRowCount < o3Hi - o3Lo;
                        o3Hi -= lagRowCount;
                        o3TimestampMax = getTimestampIndexValue(timestampAddr, o3Hi - 1);
                        assert o3TimestampMax >= o3TimestampMin && o3TimestampMax <= commitToTimestamp;

                        LOG.debug().$("committing WAL with LAG [table=").$(tableToken)
                                .$(", lagRowCount=").$(lagRowCount)
                                .$(", rowLo=").$(o3Lo)
                                .$(", rowHi=").$(o3Hi).I$();
                    } else {
                        lagRowCount = 0;
                    }

                    o3RowCount = o3Hi - o3Lo + lagRowCount;
                    processO3Block(
                            lagRowCount,
                            timestampIndex,
                            timestampAddr,
                            o3Hi,
                            o3TimestampMin,
                            o3TimestampMax,
                            copiedToMemory,
                            o3Lo
                    );
                    if (lagRowCount == 0) {
                        o3TimestampMem.jumpTo(0);
                    }
                }
            } finally {
                finishO3Append(lagRowCount);
                o3Columns = o3MemColumns;
                closeWalColumns();
            }

            finishO3Commit(partitionTimestampHiLimit);
            return true;
        } finally {
            walPath.trimTo(walRootPathLen);
        }
    }

    public long processWalData(
            @Transient Path walPath,
            boolean inOrder,
            long rowLo,
            long rowHi,
            long o3TimestampMin,
            long o3TimestampMax,
            SymbolMapDiffCursor mapDiffCursor,
            long seqTxn,
            long commitToTimestamp
    ) {
        if (inTransaction()) {
            // When writer is returned to pool, it should be rolled back. Having an open transaction is very suspicious.
            // Set the writer to distressed state and throw exception so that writer is re-created.
            distressed = true;
            throw CairoException.critical(0).put("cannot process WAL while in transaction");
        }

        txWriter.beginPartitionSizeUpdate();
        LOG.info().$("processing WAL [path=").$(walPath).$(", roLo=").$(rowLo)
                .$(", seqTxn").$(seqTxn)
                .$(", roHi=").$(rowHi)
                .$(", tsMin=").$ts(o3TimestampMin).$(", tsMax=").$ts(o3TimestampMax)
                .$(", commitToTimestamp=").$ts(commitToTimestamp)
                .I$();
        if (rowAction == ROW_ACTION_OPEN_PARTITION && txWriter.getMaxTimestamp() == Long.MIN_VALUE) {
            // table truncated, open partition file.
            openFirstPartition(o3TimestampMin);
        }
        boolean dataSavedToDisk = processWalBlock(walPath, metadata.getTimestampIndex(), inOrder, rowLo, rowHi, o3TimestampMin, o3TimestampMax, mapDiffCursor, commitToTimestamp);
        if (dataSavedToDisk) {
            final long committedRowCount = txWriter.unsafeCommittedFixedRowCount() + txWriter.unsafeCommittedTransientRowCount();
            final long rowsAdded = txWriter.getRowCount() - committedRowCount;

            updateIndexes();
            columnVersionWriter.commit();
            txWriter.setSeqTxn(seqTxn);
            txWriter.setColumnVersion(columnVersionWriter.getVersion());
            txWriter.commit(defaultCommitMode, this.denseSymbolMapWriters);

            // Bookmark masterRef to track how many rows is in uncommitted state
            this.committedMasterRef = masterRef;
            processPartitionRemoveCandidates();

            metrics.tableWriter().incrementCommits();
            metrics.tableWriter().addCommittedRows(rowsAdded);
            return rowsAdded;
        } else {
            // Keep in memory last committed seq txn, but do not write it to _txn file.
            txWriter.setSeqTxn(seqTxn);
        }
        return 0L;
    }

    public void publishAsyncWriterCommand(AsyncWriterCommand asyncWriterCommand) {
        while (true) {
            long seq = commandPubSeq.next();
            if (seq > -1) {
                TableWriterTask task = commandQueue.get(seq);
                asyncWriterCommand.serialize(task);
                commandPubSeq.done(seq);
                return;
            } else if (seq == -1) {
                throw CairoException.nonCritical().put("could not publish, command queue is full [table=").put(tableToken.getTableName()).put(']');
            }
            Os.pause();
        }
    }

    /**
     * Truncates table partitions leaving symbol files.
     * Used for truncate without holding Read lock on the table like in case of WAL tables.
     * This method leaves symbol files intact.
     */
    public final void removeAllPartitions() {
        if (size() == 0) {
            return;
        }

        if (partitionBy == PartitionBy.NONE) {
            throw CairoException.critical(0).put("cannot remove partitions from non-partitioned table");
        }

        // Remove all partition from txn file, column version file.
        txWriter.beginPartitionSizeUpdate();

        closeActivePartition(false);
        for (int i = txWriter.getPartitionCount() - 1; i > -1L; i--) {
            long timestamp = txWriter.getPartitionTimestamp(i);
            long partitionTxn = txWriter.getPartitionNameTxn(i);
            partitionRemoveCandidates.add(timestamp, partitionTxn);
        }

        columnVersionWriter.truncate(true);
        txWriter.removeAllPartitions();
        columnVersionWriter.commit();
        txWriter.setColumnVersion(columnVersionWriter.getVersion());
        txWriter.commit(defaultCommitMode, denseSymbolMapWriters);
        rowAction = ROW_ACTION_OPEN_PARTITION;

        processPartitionRemoveCandidates();

        LOG.info().$("removed all partitions (soft truncated) [name=").utf8(tableToken.getTableName()).I$();
    }

    @Override
    public void removeColumn(CharSequence name) {
        checkDistressed();
        checkColumnName(name);

        final int index = getColumnIndex(name);
        final int type = metadata.getColumnType(index);

        LOG.info().$("removing [column=").utf8(name).$(", path=").utf8(path).I$();

        // check if we are moving timestamp from a partitioned table
        final int timestampIndex = metaMem.getInt(META_OFFSET_TIMESTAMP_INDEX);
        boolean timestamp = index == timestampIndex;

        if (timestamp && PartitionBy.isPartitioned(partitionBy)) {
            throw CairoException.nonCritical().put("Cannot remove timestamp from partitioned table");
        }

        commit();

        metaSwapIndex = removeColumnFromMeta(index);

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
            removeColumnFiles(name, index, type);
        } catch (CairoException e) {
            throwDistressException(e);
        }

        bumpStructureVersion();

        metadata.removeColumn(index);
        if (timestamp) {
            metadata.clearTimestampIndex();
        }

        LOG.info().$("REMOVED column '").utf8(name).$('[').$(ColumnType.nameOf(type)).$("]' from ").$(path).$();
    }

    @Override
    public boolean removePartition(long timestamp) {
        if (!PartitionBy.isPartitioned(partitionBy)) {
            return false;
        }

        // commit changes, there may be uncommitted rows of any partition
        commit();

        final long minTimestamp = txWriter.getMinTimestamp(); // partition min timestamp
        final long maxTimestamp = txWriter.getMaxTimestamp(); // partition max timestamp

        timestamp = getPartitionLo(timestamp);
        final int index = txWriter.getPartitionIndex(timestamp);
        if (index < 0) {
            LOG.error().$("partition is already removed [path=").utf8(path).$(", partitionTimestamp=").$ts(timestamp).I$();
            return false;
        }

        final long partitionNameTxn = txWriter.getPartitionNameTxnByPartitionTimestamp(timestamp);

        if (timestamp == getPartitionLo(maxTimestamp)) {

            // removing active partition

            // calculate new transient row count, min/max timestamps and find the partition to open next
            final long nextMaxTimestamp;
            final long newTransientRowCount;
            final long prevTimestamp;
            if (index == 0) {
                nextMaxTimestamp = Long.MIN_VALUE;
                newTransientRowCount = 0L;
                prevTimestamp = 0L; // meaningless
            } else {
                final int prevIndex = index - 1;
                prevTimestamp = txWriter.getPartitionTimestamp(prevIndex);
                newTransientRowCount = txWriter.getPartitionSize(prevIndex);
                try {
                    setPathForPartition(path.trimTo(rootLen), partitionBy, prevTimestamp, false);
                    TableUtils.txnPartitionConditionally(path, txWriter.getPartitionNameTxn(prevIndex));
                    readPartitionMinMax(ff, prevTimestamp, path, metadata.getColumnName(metadata.getTimestampIndex()), newTransientRowCount);
                    nextMaxTimestamp = attachMaxTimestamp;
                } finally {
                    path.trimTo(rootLen);
                }
            }

            columnVersionWriter.removePartition(timestamp);
            txWriter.beginPartitionSizeUpdate();
            txWriter.removeAttachedPartitions(timestamp);
            txWriter.finishPartitionSizeUpdate(index == 0 ? Long.MAX_VALUE : txWriter.getMinTimestamp(), nextMaxTimestamp);
            txWriter.bumpTruncateVersion();

            columnVersionWriter.commit();
            txWriter.setColumnVersion(columnVersionWriter.getVersion());
            txWriter.commit(defaultCommitMode, denseSymbolMapWriters);

            closeActivePartition(true);

            if (index != 0) {
                openPartition(prevTimestamp);
                setAppendPosition(newTransientRowCount, false);
            } else {
                rowAction = ROW_ACTION_OPEN_PARTITION;
            }
        } else {

            // when we want to delete first partition we must find out minTimestamp from
            // next partition if it exists, or next partition, and so on
            //
            // when somebody removed data directories manually and then attempts to tidy
            // up metadata with logical partition delete we have to uphold the effort and
            // re-compute table size and its minTimestamp from what remains on disk

            // find out if we are removing min partition
            long nextMinTimestamp = minTimestamp;
            if (timestamp == txWriter.getPartitionTimestamp(0)) {
                nextMinTimestamp = readMinTimestamp(txWriter.getPartitionTimestamp(1));
            }

            columnVersionWriter.removePartition(timestamp);

            txWriter.beginPartitionSizeUpdate();
            txWriter.removeAttachedPartitions(timestamp);
            txWriter.setMinTimestamp(nextMinTimestamp);
            txWriter.finishPartitionSizeUpdate(nextMinTimestamp, txWriter.getMaxTimestamp());
            txWriter.bumpTruncateVersion();

            columnVersionWriter.commit();
            txWriter.setColumnVersion(columnVersionWriter.getVersion());
            txWriter.commit(defaultCommitMode, denseSymbolMapWriters);
        }

        // Call O3 methods to remove check TxnScoreboard and remove partition directly
        safeDeletePartitionDir(timestamp, partitionNameTxn);
        return true;
    }

    @Override
    public void renameColumn(CharSequence currentName, CharSequence newName) {
        checkDistressed();
        checkColumnName(newName);

        final int index = getColumnIndex(currentName);
        final int type = metadata.getColumnType(index);

        LOG.info().$("renaming column '").utf8(currentName).$('[').$(ColumnType.nameOf(type)).$("]' to '").utf8(newName).$("' in ").$(path).$();

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
            renameColumnFiles(currentName, index, newName, type);
        } catch (CairoException e) {
            throwDistressException(e);
        }

        bumpStructureVersion();

        metadata.renameColumn(currentName, newName);

        if (index == metadata.getTimestampIndex()) {
            designatedTimestampColumnName = Chars.toString(newName);
        }

        LOG.info().$("RENAMED column '").utf8(currentName).$("' to '").utf8(newName).$("' from ").$(path).$();
    }

    @Override
    public void rollback() {
        checkDistressed();
        if (o3InError || inTransaction()) {
            try {
                LOG.info().$("tx rollback [name=").utf8(tableToken.getTableName()).I$();
                partitionRemoveCandidates.clear();
                o3CommitBatchTimestampMin = Long.MAX_VALUE;
                if ((masterRef & 1) != 0) {
                    masterRef++;
                }
                freeColumns(false);
                txWriter.unsafeLoadAll();
                rollbackIndexes();
                rollbackSymbolTables();
                columnVersionWriter.readUnsafe();
                purgeUnusedPartitions();
                configureAppendPosition();
                o3InError = false;
                // when we rolled transaction back, hasO3() has to be false
                o3MasterRef = -1;
                LOG.info().$("tx rollback complete [name=").utf8(tableToken.getTableName()).I$();
                processCommandQueue(false);
                metrics.tableWriter().incrementRollbacks();
            } catch (Throwable e) {
                LOG.critical().$("could not perform rollback [name=").utf8(tableToken.getTableName()).$(", msg=").$(e.getMessage()).I$();
                distressed = true;
            }
        }
    }

    public void setExtensionListener(ExtensionListener listener) {
        txWriter.setExtensionListener(listener);
    }

    public void setLifecycleManager(LifecycleManager lifecycleManager) {
        this.lifecycleManager = lifecycleManager;
    }

    @Override
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

    @Override
    public void setMetaO3MaxLag(long o3MaxLagUs) {
        try {
            commit();
            long metaSize = copyMetadataAndUpdateVersion();
            openMetaSwapFileByIndex(ff, ddlMem, path, rootLen, this.metaSwapIndex);
            try {
                ddlMem.jumpTo(META_OFFSET_O3_MAX_LAG);
                ddlMem.putLong(o3MaxLagUs);
                ddlMem.jumpTo(metaSize);
            } finally {
                ddlMem.close();
            }

            finishMetaSwapUpdate();
            metadata.setO3MaxLag(o3MaxLagUs);
            clearTodoLog();
        } finally {
            ddlMem.close();
        }
    }

    public void setSeqTxn(long seqTxn) {
        txWriter.setSeqTxn(seqTxn);
    }

    public long size() {
        // This is uncommitted row count
        return txWriter.getRowCount() + getO3RowCount();
    }

    @Override
    public boolean supportsMultipleWriters() {
        return false;
    }

    /**
     * Processes writer command queue to execute writer async commands such as replication and table alters.
     * Does not accept structure changes, e.g. equivalent to tick(false)
     * Some tick calls can result into transaction commit.
     */
    @Override
    public void tick() {
        tick(false);
    }

    /**
     * Processes writer command queue to execute writer async commands such as replication and table alters.
     * Some tick calls can result into transaction commit.
     *
     * @param contextAllowsAnyStructureChanges If true accepts any Alter table command, if false does not accept significant table
     *                                         structure changes like column drop, rename
     */
    public void tick(boolean contextAllowsAnyStructureChanges) {
        // Some alter table trigger commit() which trigger tick()
        // If already inside the tick(), do not re-enter it.
        processCommandQueue(contextAllowsAnyStructureChanges);
    }

    @Override
    public String toString() {
        return "TableWriter{name=" + tableToken.getTableName() + '}';
    }

    public void transferLock(int lockFd) {
        assert lockFd != -1;
        this.lockFd = lockFd;
    }

    /**
     * Truncates table. When operation is unsuccessful it throws CairoException. With that truncate can be
     * retried or alternatively table can be closed. Outcome of any other operation with the table is undefined
     * and likely to cause segmentation fault. When table re-opens any partial truncate will be retried.
     */
    @Override
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
            rowAction = ROW_ACTION_OPEN_PARTITION;
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
        columnVersionWriter.truncate(PartitionBy.isPartitioned(partitionBy));
        txWriter.truncate(columnVersionWriter.getVersion());
        try {
            clearTodoLog();
        } catch (CairoException e) {
            throwDistressException(e);
        }

        LOG.info().$("truncated [name=").utf8(tableToken.getTableName()).I$();
    }

    public void updateTableToken(TableToken tableToken) {
        this.tableToken = tableToken;
        this.metadata.updateTableToken(tableToken);
    }

    public void upsertColumnVersion(long partitionTimestamp, int columnIndex, long columnTop) {
        columnVersionWriter.upsert(partitionTimestamp, columnIndex, txWriter.txn, columnTop);
        txWriter.updatePartitionColumnVersion(partitionTimestamp);
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
            case ColumnType.LONG128:
                // fall through
            case ColumnType.UUID:
                nullers.add(() -> mem1.putLong128(Numbers.LONG_NaN, Numbers.LONG_NaN));
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
                break;
            default:
                nullers.add(NOOP);
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
            int columnType = getColumnType(metaMem, i); // Negative means deleted column
            if (columnType > 0 && Chars.equalsIgnoreCase(col, name)) {
                return i;
            }
            nameOffset += Vm.getStorageLength(col);
        }
        return -1;
    }

    private static ColumnVersionWriter openColumnVersionFile(FilesFacade ff, Path path, int rootLen) {
        path.concat(COLUMN_VERSION_FILE_NAME).$();
        try {
            return new ColumnVersionWriter(ff, path, 0);
        } finally {
            path.trimTo(rootLen);
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

    private static void removeFileAndOrLog(FilesFacade ff, LPSZ name) {
        if (ff.exists(name)) {
            if (ff.remove(name)) {
                LOG.debug().$("removed [file=").utf8(name).I$();
            } else {
                LOG.error()
                        .$("could not remove [errno=").$(ff.errno())
                        .$(", file=").utf8(name)
                        .I$();
            }
        }
    }

    private static void renameFileOrLog(FilesFacade ff, LPSZ from, LPSZ to) {
        if (ff.exists(from)) {
            if (ff.rename(from, to) == FILES_RENAME_OK) {
                LOG.debug().$("renamed [from=").utf8(from).$(", to=").utf8(to).I$();
            } else {
                LOG.critical()
                        .$("could not rename [errno=").$(ff.errno())
                        .$(", from=").utf8(from)
                        .$(", to=").utf8(to)
                        .I$();
            }
        }
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
                writeColumnEntry(i, false);
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

    private void attachPartitionCheckFilesMatchFixedColumn(
            int columnType,
            long partitionSize,
            long columnTop,
            String columnName,
            long columnNameTxn,
            Path partitionPath,
            long partitionTimestamp,
            int columnIndex
    ) {
        long columnSize = partitionSize - columnTop;
        if (columnSize == 0) {
            return;
        }

        TableUtils.dFile(partitionPath, columnName, columnNameTxn);
        if (!ff.exists(partitionPath.$())) {
            LOG.info().$("attaching partition with missing column [path=").$(partitionPath).I$();
            columnVersionWriter.upsertColumnTop(partitionTimestamp, columnIndex, partitionSize);
        } else {
            long fileSize = ff.length(partitionPath);
            if (fileSize < (columnSize << ColumnType.pow2SizeOf(columnType))) {
                throw CairoException.critical(0)
                        .put("Column file is too small. ")
                        .put("Partition files inconsistent [file=")
                        .put(partitionPath)
                        .put(", expectedSize=")
                        .put(columnSize << ColumnType.pow2SizeOf(columnType))
                        .put(", actual=")
                        .put(fileSize)
                        .put(']');
            }
        }
    }

    private void attachPartitionCheckFilesMatchVarLenColumn(
            long partitionSize,
            long columnTop,
            String columnName,
            long columnNameTxn,
            Path partitionPath,
            long partitionTimestamp,
            int columnIndex
    ) throws CairoException {
        long columnSize = partitionSize - columnTop;
        if (columnSize == 0) {
            return;
        }

        int pathLen = partitionPath.length();
        TableUtils.dFile(partitionPath, columnName, columnNameTxn);
        long dataLength = ff.length(partitionPath.$());

        if (dataLength > 0) {
            partitionPath.trimTo(pathLen);
            TableUtils.iFile(partitionPath, columnName, columnNameTxn);

            int typeSize = Long.BYTES;
            int indexFd = openRO(ff, partitionPath, LOG);
            try {
                long fileSize = ff.length(indexFd);
                long expectedFileSize = (columnSize + 1) * typeSize;
                if (fileSize < expectedFileSize) {
                    throw CairoException.critical(0)
                            .put("Column file is too small. ")
                            .put("Partition files inconsistent [file=")
                            .put(partitionPath)
                            .put(",expectedSize=")
                            .put(expectedFileSize)
                            .put(",actual=")
                            .put(fileSize)
                            .put(']');
                }

                long mappedAddr = mapRO(ff, indexFd, expectedFileSize, MemoryTag.MMAP_DEFAULT);
                try {
                    long prevDataAddress = dataLength;
                    for (long offset = columnSize * typeSize; offset >= 0; offset -= typeSize) {
                        long dataAddress = Unsafe.getUnsafe().getLong(mappedAddr + offset);
                        if (dataAddress < 0 || dataAddress > dataLength) {
                            throw CairoException.critical(0).put("Variable size column has invalid data address value [path=").put(path)
                                    .put(", indexOffset=").put(offset)
                                    .put(", dataAddress=").put(dataAddress)
                                    .put(", dataFileSize=").put(dataLength)
                                    .put(']');
                        }

                        // Check that addresses are monotonic
                        if (dataAddress > prevDataAddress) {
                            throw CairoException.critical(0).put("Variable size column has invalid data address value [path=").put(partitionPath)
                                    .put(", indexOffset=").put(offset)
                                    .put(", dataAddress=").put(dataAddress)
                                    .put(", prevDataAddress=").put(prevDataAddress)
                                    .put(", dataFileSize=").put(dataLength)
                                    .put(']');
                        }
                        prevDataAddress = dataAddress;
                    }
                } finally {
                    ff.munmap(mappedAddr, expectedFileSize, MemoryTag.MMAP_DEFAULT);
                }
            } finally {
                ff.close(indexFd);
            }
        } else {
            LOG.info().$("attaching partition with missing column [path=").$(partitionPath).I$();
            columnVersionWriter.upsertColumnTop(partitionTimestamp, columnIndex, partitionSize);
        }
    }

    private void attachPartitionCheckSymbolColumn(long partitionSize, long columnTop, String columnName, long columnNameTxn, Path partitionPath, long partitionTimestamp, int columnIndex) {
        long columnSize = partitionSize - columnTop;
        if (columnSize == 0) {
            return;
        }

        int pathLen = partitionPath.length();
        TableUtils.dFile(partitionPath, columnName, columnNameTxn);
        if (!ff.exists(partitionPath.$())) {
            columnVersionWriter.upsertColumnTop(partitionTimestamp, columnIndex, partitionSize);
            return;
        }

        int fd = openRO(ff, partitionPath.$(), LOG);
        try {
            long fileSize = ff.length(fd);
            int typeSize = Integer.BYTES;
            long expectedSize = columnSize * typeSize;
            if (fileSize < expectedSize) {
                throw CairoException.critical(0)
                        .put("Column file is too small. ")
                        .put("Partition files inconsistent [file=")
                        .put(partitionPath)
                        .put(", expectedSize=")
                        .put(expectedSize)
                        .put(", actual=")
                        .put(fileSize)
                        .put(']');
            }

            long address = mapRO(ff, fd, fileSize, MemoryTag.MMAP_DEFAULT);
            try {
                int maxKey = Vect.maxInt(address, columnSize);
                int symbolValues = symbolMapWriters.getQuick(columnIndex).getSymbolCount();
                if (maxKey >= symbolValues) {
                    throw CairoException.critical(0)
                            .put("Symbol file does not match symbol column [file=")
                            .put(path)
                            .put(", key=")
                            .put(maxKey)
                            .put(", columnKeys=")
                            .put(symbolValues)
                            .put(']');
                }
                int minKey = Vect.minInt(address, columnSize);
                if (minKey != SymbolTable.VALUE_IS_NULL && minKey < 0) {
                    throw CairoException.critical(0)
                            .put("Symbol file does not match symbol column, invalid key [file=")
                            .put(path)
                            .put(", key=")
                            .put(minKey)
                            .put(']');
                }
            } finally {
                ff.munmap(address, fileSize, MemoryTag.MMAP_DEFAULT);
            }

            if (metadata.isColumnIndexed(columnIndex)) {
                valueFileName(partitionPath.trimTo(pathLen), columnName, columnNameTxn);
                if (!ff.exists(partitionPath.$())) {
                    throw CairoException.critical(0)
                            .put("Symbol index value file does not exist [file=")
                            .put(partitionPath)
                            .put(']');
                }
                keyFileName(partitionPath.trimTo(pathLen), columnName, columnNameTxn);
                if (!ff.exists(partitionPath.$())) {
                    throw CairoException.critical(0)
                            .put("Symbol index key file does not exist [file=")
                            .put(partitionPath)
                            .put(']');
                }
            }
        } finally {
            ff.close(fd);
        }
    }

    private boolean attachPrepare(long partitionTimestamp, long partitionSize, Path detachedPath, int detachedPartitionRoot) {
        try {
            // load/check _meta
            detachedPath.trimTo(detachedPartitionRoot).concat(META_FILE_NAME);
            if (!ff.exists(detachedPath.$())) {
                // Backups and older versions of detached partitions will not have _dmeta
                LOG.info().$("detached ").$(META_FILE_NAME).$(" file not found, skipping check [path=").$(detachedPath).I$();
                return false;
            }

            if (attachMetadata == null) {
                attachMetaMem = Vm.getCMRInstance();
                attachMetaMem.smallFile(ff, detachedPath, MemoryTag.MMAP_TABLE_WRITER);
                attachMetadata = new TableWriterMetadata(tableToken, attachMetaMem);
            } else {
                attachMetaMem.smallFile(ff, detachedPath, MemoryTag.MMAP_TABLE_WRITER);
                attachMetadata.reload(attachMetaMem);
            }

            if (metadata.getTableId() != attachMetadata.getTableId()) {
                // very same table, attaching foreign partitions is not allowed
                throw CairoException.detachedMetadataMismatch("table_id");
            }
            if (metadata.getTimestampIndex() != attachMetadata.getTimestampIndex()) {
                // designated timestamps in both tables, same index
                throw CairoException.detachedMetadataMismatch("timestamp_index");
            }

            // load/check _dcv, updating local column tops
            // set current _dcv to where the partition was
            detachedPath.trimTo(detachedPartitionRoot).concat(COLUMN_VERSION_FILE_NAME).$();
            if (!ff.exists(detachedPath)) {
                // Backups and older versions of detached partitions will not have _cv
                LOG.error().$("detached _dcv file not found, skipping check [path=").$(detachedPath).I$();
                return false;
            } else {
                if (attachColumnVersionReader == null) {
                    attachColumnVersionReader = new ColumnVersionReader();
                }
                attachColumnVersionReader.ofRO(ff, detachedPath);
                attachColumnVersionReader.readUnsafe();
            }

            // override column tops for the partition we are attaching
            columnVersionWriter.copyPartition(partitionTimestamp, attachColumnVersionReader);

            for (int colIdx = 0; colIdx < columnCount; colIdx++) {
                String columnName = metadata.getColumnName(colIdx);

                // check name
                int detColIdx = attachMetadata.getColumnIndexQuiet(columnName);
                if (detColIdx == -1) {
                    columnVersionWriter.upsertColumnTop(partitionTimestamp, colIdx, partitionSize);
                    continue;
                }

                if (detColIdx != colIdx) {
                    throw CairoException.detachedColumnMetadataMismatch(colIdx, columnName, "name");
                }

                // check type
                int tableColType = metadata.getColumnType(colIdx);
                int attachColType = attachMetadata.getColumnType(detColIdx);
                if (tableColType != attachColType && tableColType != -attachColType) {
                    throw CairoException.detachedColumnMetadataMismatch(colIdx, columnName, "type");
                }

                if (tableColType != attachColType) {
                    // This is very suspicious. The column was deleted in the detached partition,
                    // but it exists in the target table.
                    LOG.info().$("detached partition has column deleted while the table has the same column alive [tableName=").utf8(tableToken.getTableName())
                            .$(", columnName=").utf8(columnName)
                            .$(", columnType=").$(ColumnType.nameOf(tableColType))
                            .I$();
                    columnVersionWriter.upsertColumnTop(partitionTimestamp, colIdx, partitionSize);
                }

                // check column is / was indexed
                if (ColumnType.isSymbol(tableColType)) {
                    boolean isIndexedNow = metadata.isColumnIndexed(colIdx);
                    boolean wasIndexedAtDetached = attachMetadata.isColumnIndexed(detColIdx);
                    int indexValueBlockCapacityNow = metadata.getIndexValueBlockCapacity(colIdx);
                    int indexValueBlockCapacityDetached = attachMetadata.getIndexValueBlockCapacity(detColIdx);

                    if (!isIndexedNow && wasIndexedAtDetached) {
                        long columnNameTxn = attachColumnVersionReader.getColumnNameTxn(partitionTimestamp, colIdx);
                        keyFileName(detachedPath.trimTo(detachedPartitionRoot), columnName, columnNameTxn);
                        removeFileAndOrLog(ff, detachedPath);
                        valueFileName(detachedPath.trimTo(detachedPartitionRoot), columnName, columnNameTxn);
                        removeFileAndOrLog(ff, detachedPath);
                    } else if (isIndexedNow
                            && (!wasIndexedAtDetached || indexValueBlockCapacityNow != indexValueBlockCapacityDetached)) {
                        // Was not indexed before or value block capacity has changed
                        detachedPath.trimTo(detachedPartitionRoot);
                        rebuildAttachedPartitionColumnIndex(partitionTimestamp, partitionSize, detachedPath, columnName);
                    }
                }
            }
            return true;
            // Do not remove _dmeta and _dcv to keep partition attachable in case of fs copy / rename failure
        } finally {
            Misc.free(attachColumnVersionReader);
            Misc.free(attachMetaMem);
            Misc.free(attachIndexBuilder);
        }
    }

    private void attachValidateMetadata(long partitionSize, Path partitionPath, long partitionTimestamp) throws CairoException {
        // for each column, check that file exists in the partition folder
        int rootLen = partitionPath.length();
        for (int columnIndex = 0, size = metadata.getColumnCount(); columnIndex < size; columnIndex++) {
            try {
                final String columnName = metadata.getColumnName(columnIndex);
                int columnType = metadata.getColumnType(columnIndex);

                if (columnType > -1L) {
                    long columnTop = columnVersionWriter.getColumnTop(partitionTimestamp, columnIndex);
                    if (columnTop < 0 || columnTop == partitionSize) {
                        // Column does not exist in the partition
                        continue;
                    }
                    long columnNameTxn = columnVersionWriter.getDefaultColumnNameTxn(columnIndex);
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
                        case ColumnType.LONG128:
                        case ColumnType.LONG256:
                        case ColumnType.GEOBYTE:
                        case ColumnType.GEOSHORT:
                        case ColumnType.GEOINT:
                        case ColumnType.GEOLONG:
                        case ColumnType.UUID:
                            attachPartitionCheckFilesMatchFixedColumn(columnType, partitionSize, columnTop, columnName, columnNameTxn, partitionPath, partitionTimestamp, columnIndex);
                            break;
                        case ColumnType.STRING:
                        case ColumnType.BINARY:
                            attachPartitionCheckFilesMatchVarLenColumn(partitionSize, columnTop, columnName, columnNameTxn, partitionPath, partitionTimestamp, columnIndex);
                            break;
                        case ColumnType.SYMBOL:
                            attachPartitionCheckSymbolColumn(partitionSize, columnTop, columnName, columnNameTxn, partitionPath, partitionTimestamp, columnIndex);
                            break;
                    }
                }
            } finally {
                partitionPath.trimTo(rootLen);
            }
        }
    }

    private void bumpMasterRef() {
        if ((masterRef & 1) == 0) {
            masterRef++;
        } else {
            cancelRowAndBump();
        }
    }

    private void bumpStructureVersion() {
        columnVersionWriter.commit();
        txWriter.setColumnVersion(columnVersionWriter.getVersion());
        txWriter.bumpStructureVersion(this.denseSymbolMapWriters);
        assert txWriter.getStructureVersion() == metadata.getStructureVersion();
    }

    private void cancelRowAndBump() {
        rowCancel();
        masterRef++;
    }

    private void checkColumnName(CharSequence name) {
        if (!TableUtils.isValidColumnName(name, configuration.getMaxFileNameLength())) {
            throw CairoException.nonCritical().put("invalid column name [table=").put(tableToken.getTableName()).put(", column=").putAsPrintable(name).put(']');
        }
    }

    private void checkDistressed() {
        if (!distressed) {
            return;
        }
        throw new CairoError("Table '" + tableToken.getTableName() + "' is distressed");
    }

    private void checkO3Errors() {
        if (o3ErrorCount.get() > 0) {
            if (lastErrno == O3_ERRNO_FATAL) {
                distressed = true;
                throw new CairoError("commit failed with fatal error, see logs for details [table=" +
                        tableToken.getTableName() +
                        ", tableDir=" + tableToken.getDirName() + "]");
            } else {
                throw CairoException.critical(lastErrno)
                        .put("commit failed, see logs for details [table=")
                        .put(tableToken.getTableName())
                        .put(", tableDir=").put(tableToken.getDirName())
                        .put(']');
            }
        }
    }

    private void clearO3() {
        this.o3MasterRef = -1; // clears o3 flag, hasO3() will be returning false
        rowAction = ROW_ACTION_SWITCH_PARTITION;
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

    private void closeAppendMemoryTruncate(boolean truncate) {
        for (int i = 0, n = columns.size(); i < n; i++) {
            MemoryMA m = columns.getQuick(i);
            if (m != null) {
                m.close(truncate);
            }
        }
    }

    private void closeWalColumns() {
        for (int col = 0, n = walMappedColumns.size(); col < n; col++) {
            MemoryCMOR mappedColumnMem = walMappedColumns.getQuick(col);
            if (mappedColumnMem != null) {
                Misc.free(mappedColumnMem);
                walColumnMemoryPool.push(mappedColumnMem);
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
     * @param o3MaxLag   if > 0 then do a partial commit, leaving the rows within the lag in a new uncommitted transaction
     * @return commit transaction number or -1 if there was nothing to commit
     */
    private long commit(int commitMode, long o3MaxLag) {
        checkDistressed();
        physicallyWrittenRowsSinceLastCommit.set(0);

        if (o3InError) {
            rollback();
            return TableSequencer.NO_TXN;
        }

        if ((masterRef & 1) != 0) {
            rowCancel();
        }

        if (inTransaction()) {
            final boolean o3 = hasO3();
            if (o3) {
                final boolean noop = o3Commit(o3MaxLag);
                if (noop) {
                    // Bookmark masterRef to track how many rows is in uncommitted state
                    this.committedMasterRef = masterRef;
                    return getTxn();
                } else if (o3MaxLag > 0) {
                    // It is possible that O3 commit will create partition just before
                    // the last one, leaving last partition row count 0 when doing ic().
                    // That's when the data from the last partition is moved to in-memory lag.
                    // One way to detect this is to check if index of the "last" partition is not
                    // last partition in the attached partition list.
                    if (txWriter.reconcileOptimisticPartitions()) {
                        this.lastPartitionTimestamp = txWriter.getLastPartitionTimestamp();
                        this.partitionTimestampHi = partitionCeilMethod.ceil(txWriter.getMaxTimestamp()) - 1;
                        openLastPartition();
                    }
                }
            } else if (noOpRowCount > 0) {
                LOG.critical()
                        .$("o3 ignoring write on read-only partition [table=").utf8(tableToken.getTableName())
                        .$(", timestamp=").$ts(lastOpenPartitionTs)
                        .$(", numRows=").$(noOpRowCount)
                        .$();
            }

            if (commitMode != CommitMode.NOSYNC) {
                syncColumns(commitMode);
            }

            final long committedRowCount = txWriter.unsafeCommittedFixedRowCount() + txWriter.unsafeCommittedTransientRowCount();
            final long rowsAdded = txWriter.getRowCount() - committedRowCount;

            updateIndexes();
            columnVersionWriter.commit();
            txWriter.setColumnVersion(columnVersionWriter.getVersion());
            txWriter.commit(commitMode, denseSymbolMapWriters);

            // Bookmark masterRef to track how many rows is in uncommitted state
            this.committedMasterRef = masterRef;
            processPartitionRemoveCandidates();

            metrics.tableWriter().incrementCommits();
            metrics.tableWriter().addCommittedRows(rowsAdded);
            if (!o3) {
                // If `o3`, the metric is tracked inside `o3Commit`, possibly async.
                addPhysicallyWrittenRows(rowsAdded);
            }

            noOpRowCount = 0L;
            return getTxn();
        }
        return TableSequencer.NO_TXN;
    }

    private void configureAppendPosition() {
        final boolean partitioned = PartitionBy.isPartitioned(partitionBy);
        if (this.txWriter.getMaxTimestamp() > Long.MIN_VALUE || !partitioned) {
            openFirstPartition(this.txWriter.getMaxTimestamp());
            if (partitioned) {
                partitionTimestampHi = partitionCeilMethod.ceil(txWriter.getMaxTimestamp()) - 1;
                rowAction = ROW_ACTION_OPEN_PARTITION;
                timestampSetter = appendTimestampSetter;
            } else {
                if (metadata.getTimestampIndex() < 0) {
                    rowAction = ROW_ACTION_NO_TIMESTAMP;
                } else {
                    rowAction = ROW_ACTION_NO_PARTITION;
                    timestampSetter = appendTimestampSetter;
                }
            }
        } else {
            rowAction = ROW_ACTION_OPEN_PARTITION;
            timestampSetter = appendTimestampSetter;
        }
        activeColumns = columns;
        activeNullSetters = nullSetters;
    }

    private void configureColumn(int type, boolean indexFlag, int index) {
        final MemoryMA primary;
        final MemoryMA secondary;
        final MemoryCARW oooPrimary;
        final MemoryCARW oooSecondary;
        final MemoryCARW oooPrimary2;
        final MemoryCARW oooSecondary2;

        if (type > 0) {
            primary = Vm.getMAInstance();
            oooPrimary = Vm.getCARWInstance(o3ColumnMemorySize, configuration.getO3MemMaxPages(), MemoryTag.NATIVE_O3);
            oooPrimary2 = Vm.getCARWInstance(o3ColumnMemorySize, configuration.getO3MemMaxPages(), MemoryTag.NATIVE_O3);

            switch (ColumnType.tagOf(type)) {
                case ColumnType.BINARY:
                case ColumnType.STRING:
                    secondary = Vm.getMAInstance();
                    oooSecondary = Vm.getCARWInstance(o3ColumnMemorySize, configuration.getO3MemMaxPages(), MemoryTag.NATIVE_O3);
                    oooSecondary2 = Vm.getCARWInstance(o3ColumnMemorySize, configuration.getO3MemMaxPages(), MemoryTag.NATIVE_O3);
                    break;
                default:
                    secondary = null;
                    oooSecondary = null;
                    oooSecondary2 = null;
                    break;
            }
        } else {
            primary = secondary = NullMemory.INSTANCE;
            oooPrimary = oooSecondary = oooPrimary2 = oooSecondary2 = NullMemory.INSTANCE;
        }

        int baseIndex = getPrimaryColumnIndex(index);
        columns.extendAndSet(baseIndex, primary);
        columns.extendAndSet(baseIndex + 1, secondary);
        o3MemColumns.extendAndSet(baseIndex, oooPrimary);
        o3MemColumns.extendAndSet(baseIndex + 1, oooSecondary);
        o3MemColumns2.extendAndSet(baseIndex, oooPrimary2);
        o3MemColumns2.extendAndSet(baseIndex + 1, oooSecondary2);
        configureNullSetters(nullSetters, type, primary, secondary);
        configureNullSetters(o3NullSetters, type, oooPrimary, oooSecondary);
        configureNullSetters(o3NullSetters2, type, oooPrimary2, oooSecondary2);

        if (indexFlag) {
            indexers.extendAndSet(index, new SymbolColumnIndexer());
        }
        rowValueIsNotNull.add(0);
    }

    private void configureColumnMemory() {
        this.symbolMapWriters.setPos(columnCount);
        for (int i = 0; i < columnCount; i++) {
            int type = metadata.getColumnType(i);
            configureColumn(type, metadata.isColumnIndexed(i), i);

            if (ColumnType.isSymbol(type)) {
                final int symbolIndex = denseSymbolMapWriters.size();
                long columnNameTxn = columnVersionWriter.getDefaultColumnNameTxn(i);
                SymbolMapWriter symbolMapWriter = new SymbolMapWriter(
                        configuration,
                        path.trimTo(rootLen),
                        metadata.getColumnName(i),
                        columnNameTxn,
                        txWriter.unsafeReadSymbolTransientCount(symbolIndex),
                        symbolIndex,
                        txWriter
                );

                symbolMapWriters.extendAndSet(i, symbolMapWriter);
                denseSymbolMapWriters.add(symbolMapWriter);
            }
        }
        final int timestampIndex = metadata.getTimestampIndex();
        if (timestampIndex != -1) {
            o3TimestampMem = o3MemColumns.getQuick(getPrimaryColumnIndex(timestampIndex));
            o3TimestampMemCpy = o3MemColumns2.getQuick(getPrimaryColumnIndex(timestampIndex));
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
            o3NullSetters2.setQuick(index, NOOP);
            timestampSetter = getPrimaryColumn(index)::putLong;
        }
    }

    private int copyMetadataAndSetIndexAttrs(int columnIndex, int indexedFlag, int indexValueBlockSize) {
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
                    writeColumnEntry(i, false);
                } else {
                    ddlMem.putInt(getColumnType(metaMem, i));
                    long flags = indexedFlag;
                    if (isSequential(metaMem, i)) {
                        flags |= META_FLAG_BIT_SEQUENTIAL;
                    }
                    ddlMem.putLong(flags);
                    ddlMem.putInt(indexValueBlockSize);
                    ddlMem.skip(16);
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
                writeColumnEntry(i, false);
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

    private int copyOverwrite(Path to) {
        int res = ff.copy(other, to);
        if (Os.isWindows() && res == -1 && ff.errno() == Files.WINDOWS_ERROR_FILE_EXISTS) {
            // Windows throws an error the destination file already exists, other platforms do not
            if (!ff.remove(to)) {
                // If file is open, return here so that errno is 5 in the error message
                return -1;
            }
            return ff.copy(other, to);
        }
        return res;
    }

    private void copyVersionAndLagValues() {
        ddlMem.putInt(ColumnType.VERSION);
        ddlMem.putInt(metaMem.getInt(META_OFFSET_TABLE_ID));
        ddlMem.putInt(metaMem.getInt(META_OFFSET_MAX_UNCOMMITTED_ROWS));
        ddlMem.putLong(metaMem.getLong(META_OFFSET_O3_MAX_LAG));
        ddlMem.putLong(txWriter.getStructureVersion() + 1);
        ddlMem.putBool(metaMem.getBool(META_OFFSET_WAL_ENABLED));
        metadata.setStructureVersion(txWriter.getStructureVersion() + 1);
    }

    /**
     * Creates bitmap index files for a column. This method uses primary column instance as temporary tool to
     * append index data. Therefore, it must be called before primary column is initialized.
     *
     * @param columnName              column name
     * @param indexValueBlockCapacity approximate number of values per index key
     * @param plen                    path length. This is used to trim shared path object to.
     */
    private void createIndexFiles(CharSequence columnName, long columnNameTxn, int indexValueBlockCapacity, int plen, boolean force) {
        try {
            keyFileName(path.trimTo(plen), columnName, columnNameTxn);

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
                        .I$();
                if (!ff.remove(path)) {
                    LOG.critical()
                            .$("could not remove '").utf8(path).$("'. Please remove MANUALLY.")
                            .$("[errno=").$(ff.errno())
                            .I$();
                }
                throw e;
            } finally {
                ddlMem.close();
            }
            if (!ff.touch(valueFileName(path.trimTo(plen), columnName, columnNameTxn))) {
                LOG.error().$("could not create index [name=").$(path)
                        .$(", errno=").$(ff.errno())
                        .I$();
                throw CairoException.critical(ff.errno()).put("could not create index [name=").put(path).put(']');
            }
        } finally {
            path.trimTo(plen);
        }
    }

    private void createSymbolMapWriter(CharSequence name, long columnNameTxn, int symbolCapacity, boolean symbolCacheFlag) {
        MapWriter.createSymbolMapFiles(ff, ddlMem, path, name, columnNameTxn, symbolCapacity, symbolCacheFlag);
        SymbolMapWriter w = new SymbolMapWriter(
                configuration,
                path,
                name,
                columnNameTxn,
                0,
                denseSymbolMapWriters.size(),
                txWriter
        );
        denseSymbolMapWriters.add(w);
        symbolMapWriters.extendAndSet(columnCount, w);
    }

    private boolean createWalSymbolMapping(SymbolMapDiff symbolMapDiff, int columnIndex, IntList symbolMap) {
        final int cleanSymbolCount = symbolMapDiff.getCleanSymbolCount();
        symbolMap.setPos(symbolMapDiff.getSize());

        // This is defensive. It validates that all the symbols used in WAL are set in SymbolMapDiff
        symbolMap.setAll(symbolMapDiff.getSize(), -1);
        final MapWriter mapWriter = symbolMapWriters.get(columnIndex);
        boolean identical = true;

        if (symbolMapDiff.hasNullValue()) {
            mapWriter.updateNullFlag(true);
        }

        SymbolMapDiffEntry entry;
        while ((entry = symbolMapDiff.nextEntry()) != null) {
            final CharSequence symbolValue = entry.getSymbol();
            final int newKey = mapWriter.put(symbolValue);
            identical &= newKey == entry.getKey();
            symbolMap.setQuick(entry.getKey() - cleanSymbolCount, newKey);
        }
        return identical;
    }

    private void dispatchO3CallbackQueue(RingQueue<O3CallbackTask> queue, int queuedCount) {
        // This is work stealing, can run tasks from other table writers
        final Sequence subSeq = this.messageBus.getO3CallbackSubSeq();
        while (!o3DoneLatch.done(queuedCount)) {
            long cursor = subSeq.next();
            if (cursor > -1) {
                O3CallbackJob.runCallbackWithCol(queue.get(cursor), cursor, subSeq);
            } else if (cursor == -1) {
                o3DoneLatch.await(queuedCount);
            } else {
                Os.pause();
            }
        }
        checkO3Errors();
    }

    private void doClose(boolean truncate) {
        // destroy() may already closed everything
        boolean tx = inTransaction();
        freeSymbolMapWriters();
        freeIndexers();
        Misc.free(txWriter);
        Misc.free(metaMem);
        Misc.free(ddlMem);
        Misc.free(indexMem);
        Misc.free(other);
        Misc.free(todoMem);
        Misc.free(attachMetaMem);
        Misc.free(attachColumnVersionReader);
        Misc.free(attachIndexBuilder);
        Misc.free(columnVersionWriter);
        Misc.free(o3ColumnTopSink);
        Misc.free(o3PartitionUpdateSink);
        Misc.free(slaveTxReader);
        Misc.free(commandQueue);
        updateOperatorImpl = Misc.free(updateOperatorImpl);
        dropIndexOperator = null;
        noOpRowCount = 0L;
        lastOpenPartitionTs = -1L;
        lastOpenPartitionIsReadOnly = false;
        freeColumns(truncate & !distressed);
        try {
            releaseLock(!truncate | tx | performRecovery | distressed);
        } finally {
            Misc.free(txnScoreboard);
            Misc.free(path);
            Misc.free(o3TimestampMem);
            Misc.free(o3TimestampMemCpy);
            Misc.free(ownMessageBus);
            if (tempMem16b != 0) {
                Unsafe.free(tempMem16b, 16, MemoryTag.NATIVE_TABLE_WRITER);
                tempMem16b = 0;
            }
            LOG.info().$("closed '").utf8(tableToken.getTableName()).$('\'').$();
        }
    }

    private void finishMetaSwapUpdate() {

        // rename _meta to _meta.prev
        this.metaPrevIndex = rename(fileOperationRetryCount);
        writeRestoreMetaTodo();

        try {
            // rename _meta.swp to -_meta
            restoreMetaFrom(META_SWAP_FILE_NAME, metaSwapIndex);
        } catch (CairoException e) {
            try {
                recoverFromTodoWriteFailure(null);
            } catch (CairoException e2) {
                throwDistressException(e2);
            }
            throw e;
        }

        try {
            // open _meta file
            openMetaFile(ff, path, rootLen, metaMem);
        } catch (CairoException e) {
            throwDistressException(e);
        }

        bumpStructureVersion();
        metadata.setTableVersion();
    }

    private void finishO3Append(long o3LagRowCount) {
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

    private void finishO3Commit(long partitionTimestampHiLimit) {
        if (!o3InError) {
            updateO3ColumnTops();
        }
        if (!isLastPartitionColumnsOpen() || partitionTimestampHi > partitionTimestampHiLimit) {
            openPartition(txWriter.getMaxTimestamp());
        }

        // Data is written out successfully, however, we can still fail to set append position, for
        // example when we ran out of address space and new page cannot be mapped. The "allocate" calls here
        // ensure we can trigger this situation in tests. We should perhaps align our data such that setAppendPosition()
        // will attempt to mmap new page and fail... Then we can remove the 'true' parameter
        try {
            setAppendPosition(txWriter.getTransientRowCount(), true);
        } catch (Throwable e) {
            LOG.critical().$("data is committed but writer failed to update its state `").$(e).$('`').$();
            distressed = true;
            throw e;
        }

        metrics.tableWriter().incrementO3Commits();
    }

    private void freeAndRemoveColumnPair(ObjList<MemoryMA> columns, int pi, int si) {
        Misc.free(columns.getAndSetQuick(pi, NullMemory.INSTANCE));
        Misc.free(columns.getAndSetQuick(si, NullMemory.INSTANCE));
    }

    private void freeAndRemoveO3ColumnPair(ObjList<MemoryCARW> columns, int pi, int si) {
        Misc.free(columns.getAndSetQuick(pi, NullMemory.INSTANCE));
        Misc.free(columns.getAndSetQuick(si, NullMemory.INSTANCE));
    }

    private void freeColumns(boolean truncate) {
        // null check is because this method could be called from the constructor
        if (columns != null) {
            closeAppendMemoryTruncate(truncate);
        }
        Misc.freeObjListAndKeepObjects(o3MemColumns);
        Misc.freeObjListAndKeepObjects(o3MemColumns2);
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

    private void freeNullSetter(ObjList<Runnable> nullSetters, int columnIndex) {
        nullSetters.setQuick(columnIndex, NOOP);
    }

    private void freeSymbolMapWriters() {
        if (denseSymbolMapWriters != null) {
            for (int i = 0, n = denseSymbolMapWriters.size(); i < n; i++) {
                Misc.freeIfCloseable(denseSymbolMapWriters.getQuick(i));
            }
            denseSymbolMapWriters.clear();
        }

        if (symbolMapWriters != null) {
            symbolMapWriters.clear();
        }
    }

    private long getO3RowCount0() {
        return (masterRef - o3MasterRef + 1) / 2;
    }

    private long getPartitionLo(long timestamp) {
        return partitionFloorMethod.floor(timestamp);
    }

    private MemoryMA getPrimaryColumn(int column) {
        assert column < columnCount : "Column index is out of bounds: " + column + " >= " + columnCount;
        return columns.getQuick(getPrimaryColumnIndex(column));
    }

    private MemoryMA getSecondaryColumn(int column) {
        assert column < columnCount : "Column index is out of bounds: " + column + " >= " + columnCount;
        return columns.getQuick(getSecondaryColumnIndex(column));
    }

    private void handleWorkStealingException(
            String message,
            int columnIndex,
            int columnType,
            long indexAddr,
            long row1Count,
            long row2Lo,
            long row2Hi,
            Throwable e
    ) {
        o3ErrorCount.incrementAndGet();
        LogRecord logRecord = LOG.critical().$(message + " [table=").$(tableToken.getTableName())
                .$(", column=").$(columnIndex)
                .$(", type=").$(columnType)
                .$(", indexAddr=").$(indexAddr)
                .$(", row1Count=").$(row1Count)
                .$(", row2Lo=").$(row2Lo)
                .$(", row2Hi=").$(row2Hi);
        if (e instanceof CairoException) {
            lastErrno = lastErrno == 0 ? ((CairoException) e).errno : lastErrno;
            logRecord.$(", errno=").$(lastErrno)
                    .$(", ex=").$(((CairoException) e).getFlyweightMessage())
                    .I$();
        } else {
            lastErrno = O3_ERRNO_FATAL;
            logRecord.$(", ex=").$(e).I$();
        }
    }

    private void indexHistoricPartitions(SymbolColumnIndexer indexer, CharSequence columnName, int indexValueBlockSize) {
        long ts = this.txWriter.getMaxTimestamp();
        if (ts > Numbers.LONG_NaN) {
            final int columnIndex = metadata.getColumnIndex(columnName);
            try (final MemoryMR roMem = indexMem) {
                // Index last partition separately
                for (int i = 0, n = txWriter.getPartitionCount() - 1; i < n; i++) {

                    long timestamp = txWriter.getPartitionTimestamp(i);
                    path.trimTo(rootLen);
                    setStateForTimestamp(path, timestamp, false);

                    if (ff.exists(path.$())) {
                        final int plen = path.length();

                        long columnNameTxn = columnVersionWriter.getColumnNameTxn(timestamp, columnIndex);
                        TableUtils.dFile(path.trimTo(plen), columnName, columnNameTxn);

                        if (ff.exists(path)) {

                            path.trimTo(plen);
                            LOG.info().$("indexing [path=").$(path).I$();

                            createIndexFiles(columnName, columnNameTxn, indexValueBlockSize, plen, true);
                            final long partitionSize = txWriter.getPartitionSizeByPartitionTimestamp(timestamp);
                            final long columnTop = columnVersionWriter.getColumnTop(timestamp, columnIndex);

                            if (columnTop > -1L && partitionSize > columnTop) {
                                TableUtils.dFile(path.trimTo(plen), columnName, columnNameTxn);
                                final long columnSize = (partitionSize - columnTop) << ColumnType.pow2SizeOf(ColumnType.INT);
                                roMem.of(ff, path, columnSize, columnSize, MemoryTag.MMAP_TABLE_WRITER);
                                indexer.configureWriter(configuration, path.trimTo(plen), columnName, columnNameTxn, columnTop);
                                indexer.index(roMem, columnTop, partitionSize);
                            }
                        }
                    }
                }
            } finally {
                Misc.free(indexer);
            }
        }
    }

    private void indexLastPartition(SymbolColumnIndexer indexer, CharSequence columnName, long columnNameTxn, int columnIndex, int indexValueBlockSize) {
        final int plen = path.length();

        createIndexFiles(columnName, columnNameTxn, indexValueBlockSize, plen, true);

        final long lastPartitionTs = txWriter.getLastPartitionTimestamp();
        final long columnTop = columnVersionWriter.getColumnTopQuick(lastPartitionTs, columnIndex);

        // set indexer up to continue functioning as normal
        indexer.configureFollowerAndWriter(configuration, path.trimTo(plen), columnName, columnNameTxn, getPrimaryColumn(columnIndex), columnTop);
        indexer.refreshSourceAndIndex(0, txWriter.getTransientRowCount());
    }

    private boolean isLastPartitionColumnsOpen() {
        for (int i = 0; i < columnCount; i++) {
            if (metadata.getColumnType(i) > 0) {
                return columns.getQuick(getPrimaryColumnIndex(i)).isOpen();
            }
        }
        // No columns, doesn't matter
        return true;
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

        if (this.lockFd == -1) {
            throw CairoException.critical(ff.errno()).put("Cannot lock table: ").put(path.$());
        }
    }

    private void mmapWalColumns(@Transient Path walPath, int timestampIndex, long rowLo, long rowHi) {
        walMappedColumns.clear();
        int walPathLen = walPath.length();
        final int columnCount = metadata.getColumnCount();

        try {
            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                int type = metadata.getColumnType(columnIndex);
                o3RowCount = rowHi - rowLo;
                if (type > 0) {
                    int sizeBitsPow2 = ColumnType.pow2SizeOf(type);
                    if (columnIndex == timestampIndex) {
                        sizeBitsPow2 += 1;
                    }

                    if (!ColumnType.isVariableLength(type)) {
                        MemoryCMOR primary = walColumnMemoryPool.pop();

                        dFile(walPath, metadata.getColumnName(columnIndex), -1L);
                        primary.ofOffset(
                                configuration.getFilesFacade(),
                                walPath,
                                rowLo << sizeBitsPow2,
                                rowHi << sizeBitsPow2,
                                MemoryTag.MMAP_TABLE_WRITER,
                                CairoConfiguration.O_NONE
                        );
                        walPath.trimTo(walPathLen);

                        walMappedColumns.add(primary);
                        walMappedColumns.add(null);
                    } else {
                        sizeBitsPow2 = 3;
                        MemoryCMOR fixed = walColumnMemoryPool.pop();
                        MemoryCMOR var = walColumnMemoryPool.pop();

                        iFile(walPath, metadata.getColumnName(columnIndex), -1L);
                        fixed.ofOffset(
                                configuration.getFilesFacade(),
                                walPath,
                                rowLo << sizeBitsPow2,
                                (rowHi + 1) << sizeBitsPow2,
                                MemoryTag.MMAP_TABLE_WRITER,
                                CairoConfiguration.O_NONE
                        );
                        walPath.trimTo(walPathLen);

                        long varOffset = fixed.getLong(rowLo << sizeBitsPow2);
                        long varLen = fixed.getLong(rowHi << sizeBitsPow2) - varOffset;
                        dFile(walPath, metadata.getColumnName(columnIndex), -1L);
                        var.ofOffset(
                                configuration.getFilesFacade(),
                                walPath,
                                varOffset,
                                varOffset + varLen,
                                MemoryTag.MMAP_TABLE_WRITER,
                                CairoConfiguration.O_NONE
                        );
                        walPath.trimTo(walPathLen);

                        walMappedColumns.add(var);
                        walMappedColumns.add(fixed);
                    }
                } else {
                    walMappedColumns.add(null);
                    walMappedColumns.add(null);
                }
            }
            o3Columns = walMappedColumns;
        } catch (Throwable th) {
            closeWalColumns();
            throw th;
        }
    }

    private Row newRowO3(long timestamp) {
        LOG.info().$("switched to o3 [table=").utf8(tableToken.getTableName()).I$();
        txWriter.beginPartitionSizeUpdate();
        o3OpenColumns();
        o3InError = false;
        o3MasterRef = masterRef;
        rowAction = ROW_ACTION_O3;
        o3TimestampSetter(timestamp);
        return row;
    }

    /**
     * Commits O3 data. Lag is optional. When 0 is specified the entire O3 segment is committed.
     *
     * @param o3MaxLag interval in microseconds that determines the length of O3 segment that is not going to be
     *                 committed to disk. The interval starts at max timestamp of O3 segment and ends <i>o3MaxLag</i>
     *                 microseconds before this timestamp.
     * @return <i>true</i> when commit has is a NOOP, e.g. no data has been committed to disk. <i>false</i> otherwise.
     */
    private boolean o3Commit(long o3MaxLag) {
        o3RowCount = getO3RowCount0();

        long o3LagRowCount = 0;
        long maxUncommittedRows = metadata.getMaxUncommittedRows();
        final int timestampIndex = metadata.getTimestampIndex();
        lastPartitionTimestamp = partitionFloorMethod.floor(partitionTimestampHi);
        // we will check new partitionTimestampHi value against the limit to see if the writer
        // will have to switch partition internally
        long partitionTimestampHiLimit = partitionCeilMethod.ceil(partitionTimestampHi) - 1;
        try {
            o3RowCount += o3MoveUncommitted(timestampIndex);

            // we may need to re-use file descriptors when this partition is the "current" one
            // we cannot open file again due to sharing violation
            //
            // to determine that 'ooTimestampLo' goes into current partition
            // we need to compare 'partitionTimestampHi', which is appropriately truncated to DAY/MONTH/YEAR
            // to this.maxTimestamp, which isn't truncated yet. So we need to truncate it first
            LOG.debug().$("sorting o3 [table=").utf8(tableToken.getTableName()).I$();
            final long sortedTimestampsAddr = o3TimestampMem.getAddress();

            // ensure there is enough size
            assert o3TimestampMem.getAppendOffset() == o3RowCount * TIMESTAMP_MERGE_ENTRY_BYTES;
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
                throw CairoException.nonCritical().put("timestamps before 1970-01-01 are not allowed for O3");
            }

            long o3TimestampMax = getTimestampIndexValue(sortedTimestampsAddr, o3RowCount - 1);
            if (o3TimestampMax < Timestamps.O3_MIN_TS) {
                o3InError = true;
                throw CairoException.nonCritical().put("timestamps before 1970-01-01 are not allowed for O3");
            }

            // Safe check of the sort. No known way to reproduce
            assert o3TimestampMin <= o3TimestampMax;

            if (o3MaxLag > 0) {
                long lagError = 0;
                if (getMaxTimestamp() != Long.MIN_VALUE) {

                    // When table already has data we can calculate the overlap of the newly added
                    // batch of records with existing data in the table. Positive value of the overlap
                    // means that our o3EffectiveLag was undersized.

                    lagError = getMaxTimestamp() - o3CommitBatchTimestampMin;

                    int n = o3LastTimestampSpreads.length - 1;

                    if (lagError > 0) {
                        o3EffectiveLag += lagError * configuration.getO3LagIncreaseFactor();
                        o3EffectiveLag = Math.min(o3EffectiveLag, o3MaxLag);
                    } else {
                        // avoid using negative o3EffectiveLag
                        o3EffectiveLag += lagError * configuration.getO3LagDecreaseFactor();
                        o3EffectiveLag = Math.max(0, o3EffectiveLag);
                    }

                    long max = Long.MIN_VALUE;
                    for (int i = 0; i < n; i++) {
                        // shift array left and find out max at the same time
                        final long e = o3LastTimestampSpreads[i + 1];
                        o3LastTimestampSpreads[i] = e;
                        max = Math.max(e, max);
                    }

                    o3LastTimestampSpreads[n] = o3EffectiveLag;
                    o3EffectiveLag = Math.max(o3EffectiveLag, max);
                } else {
                    o3EffectiveLag = o3MaxLag;
                }

                long lagThresholdTimestamp = o3TimestampMax - o3EffectiveLag;
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
                    // This is a scenario where "o3MaxLag" and "maxUncommitted" values do not work with the data
                    // in that the "o3EffectiveLag" is larger than dictated "maxUncommitted". A simple plan here is to
                    // commit half of the o3MaxLag.
                    if (o3LagRowCount > maxUncommittedRows) {
                        o3LagRowCount = maxUncommittedRows / 2;
                        srcOooMax = o3RowCount - o3LagRowCount;
                    } else {
                        srcOooMax = 0;
                    }
                }

                LOG.info().$("o3 commit [table=").utf8(tableToken.getTableName())
                        .$(", maxUncommittedRows=").$(maxUncommittedRows)
                        .$(", o3TimestampMin=").$ts(o3TimestampMin)
                        .$(", o3TimestampMax=").$ts(o3TimestampMax)
                        .$(", o3MaxLagUs=").$(o3MaxLag)
                        .$(", o3EffectiveLagUs=").$(o3EffectiveLag)
                        .$(", lagError=").$(lagError)
                        .$(", o3SpreadUs=").$(o3TimestampMax - o3TimestampMin)
                        .$(", lagThresholdTimestamp=").$ts(lagThresholdTimestamp)
                        .$(", o3LagRowCount=").$(o3LagRowCount)
                        .$(", srcOooMax=").$(srcOooMax)
                        .$(", o3RowCount=").$(o3RowCount)
                        .I$();

            } else {
                LOG.info()
                        .$("o3 commit [table=").utf8(tableToken.getTableName())
                        .$(", o3RowCount=").$(o3RowCount)
                        .I$();
                srcOooMax = o3RowCount;
            }

            o3CommitBatchTimestampMin = Long.MAX_VALUE;

            if (srcOooMax == 0) {
                return true;
            }

            // we could have moved the "srcOooMax" and hence we re-read the max timestamp
            o3TimestampMax = getTimestampIndexValue(sortedTimestampsAddr, srcOooMax - 1);


            // we are going to use this soon to avoid double-copying lag data
            // final boolean yep = isAppendLastPartitionOnly(sortedTimestampsAddr, o3TimestampMax);

            // reshuffle all columns according to timestamp index
            o3Sort(sortedTimestampsAddr, timestampIndex, o3RowCount);
            LOG.info()
                    .$("sorted [table=").utf8(tableToken.getTableName())
                    .$(", o3RowCount=").$(o3RowCount)
                    .I$();

            processO3Block(
                    o3LagRowCount,
                    timestampIndex,
                    sortedTimestampsAddr,
                    srcOooMax,
                    o3TimestampMin,
                    o3TimestampMax,
                    true,
                    0L
            );
        } finally {
            finishO3Append(o3LagRowCount);
        }

        finishO3Commit(partitionTimestampHiLimit);
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
            O3Basket o3Basket,
            long colTopSinkAddr
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
                    o3Basket,
                    colTopSinkAddr
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
                    colTopSinkAddr
            );
        }
    }

    private void o3ConsumePartitionUpdateSink() {
        long size = o3PartitionUpdateSink.size();

        for (long offset = 0; offset < size; offset += PARTITION_UPDATE_SINK_ENTRY_SIZE) {
            long partitionTimestamp = o3PartitionUpdateSink.get(offset);
            long timestampMin = o3PartitionUpdateSink.get(offset + 1);

            if (partitionTimestamp != -1L && timestampMin != -1L) {
                long timestampMax = o3PartitionUpdateSink.get(offset + 2);
                long srcOooPartitionLo = o3PartitionUpdateSink.get(offset + 3);
                long srcOooPartitionHi = o3PartitionUpdateSink.get(offset + 4);
                boolean partitionMutates = o3PartitionUpdateSink.get(offset + 5) != 0;
                long srcOooMax = o3PartitionUpdateSink.get(offset + 6);
                long srcDataMax = o3PartitionUpdateSink.get(offset + 7);

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
        }
    }

    private void o3ConsumePartitionUpdates() {
        final Sequence partitionSubSeq = messageBus.getO3PartitionSubSeq();
        final RingQueue<O3PartitionTask> partitionQueue = messageBus.getO3PartitionQueue();
        final Sequence openColumnSubSeq = messageBus.getO3OpenColumnSubSeq();
        final RingQueue<O3OpenColumnTask> openColumnQueue = messageBus.getO3OpenColumnQueue();
        final Sequence copySubSeq = messageBus.getO3CopySubSeq();
        final RingQueue<O3CopyTask> copyQueue = messageBus.getO3CopyQueue();

        do {
            long cursor = partitionSubSeq.next();
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
                            openColumnTask.getTimestampMergeIndexSize(),
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
                            copyTask.getTimestampMergeIndexSize(),
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

        if (o3ErrorCount.get() == 0) {
            o3ConsumePartitionUpdateSink();
        }
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

    private void o3MergeFixColumnLag(int columnIndex, int columnType, long mergeIndex, long lagRows, long mappedRowLo, long mappedRowHi) {
        if (o3ErrorCount.get() > 0) {
            return;
        }
        try {
            final long rowCount = lagRows + mappedRowHi - mappedRowLo;
            final int primaryColumnIndex = getPrimaryColumnIndex(columnIndex);
            final MemoryCARW lagMem = o3MemColumns.getQuick(primaryColumnIndex);
            final MemoryCR mappedMem = o3Columns.getQuick(primaryColumnIndex);
            if (mappedMem == lagMem) {
                if (ColumnType.isSymbol(columnType)) {
                    mappedRowLo = lagRows;
                } else {
                    throw CairoException.critical(0)
                            .put("same source and destination to copy data from for non-symbol column [")
                            .put(ColumnType.nameOf(columnType)).put("] in o3MergeFixColumnLag routine is not supported ");
                }
            }
            final MemoryCARW destMem = o3MemColumns2.getQuick(primaryColumnIndex);

            final int shl = ColumnType.pow2SizeOf(columnType);
            destMem.jumpTo(rowCount << shl);
            long src1 = mappedMem.addressOf(mappedRowLo << shl);
            long src2 = lagMem.addressOf(0);
            final long dest = destMem.addressOf(0);
            if (src2 == 0 && lagRows != 0) {
                throw CairoException.critical(0)
                        .put("cannot sort WAL data, lag rows are missing [table").put(tableToken.getTableName())
                        .put(", columnName=").put(metadata.getColumnName(columnIndex))
                        .put(", type=").put(ColumnType.nameOf(columnType))
                        .put(", lagRows=").put(lagRows)
                        .put(']');
            }
            if (src1 == 0) {
                throw CairoException.critical(0)
                        .put("cannot sort WAL data, rows are missing [table").put(tableToken.getTableName())
                        .put(", columnName=").put(metadata.getColumnName(columnIndex))
                        .put(", type=").put(ColumnType.nameOf(columnType))
                        .put(']');
            }
            if (dest == 0) {
                throw CairoException.critical(0)
                        .put("cannot sort WAL data, destination buffer is empty [table").put(tableToken.getTableName())
                        .put(", columnName=").put(metadata.getColumnName(columnIndex))
                        .put(", type=").put(ColumnType.nameOf(columnType))
                        .put(']');
            }

            switch (shl) {
                case 0:
                    Vect.mergeShuffle8Bit(src1, src2, dest, mergeIndex, rowCount);
                    break;
                case 1:
                    Vect.mergeShuffle16Bit(src1, src2, dest, mergeIndex, rowCount);
                    break;
                case 2:
                    Vect.mergeShuffle32Bit(src1, src2, dest, mergeIndex, rowCount);
                    break;
                case 3:
                    Vect.mergeShuffle64Bit(src1, src2, dest, mergeIndex, rowCount);
                    break;
                case 4:
                    Vect.mergeShuffle128Bit(src1, src2, dest, mergeIndex, rowCount);
                    break;
                case 5:
                    Vect.mergeShuffle256Bit(src1, src2, dest, mergeIndex, rowCount);
                    break;
                default:
                    assert false : "col type is unsupported";
                    break;
            }
        } catch (Throwable e) {
            handleWorkStealingException(
                    "cannot merge fix column into lag",
                    columnIndex,
                    columnType,
                    mergeIndex,
                    lagRows,
                    mappedRowLo,
                    mappedRowHi,
                    e
            );
        }
    }

    private void o3MergeIntoLag(long mergedTimestamps, long countInLag, long mappedRowLo, long mappedRoHi, int timestampIndex) {
        final Sequence pubSeq = this.messageBus.getO3CallbackPubSeq();
        final RingQueue<O3CallbackTask> queue = this.messageBus.getO3CallbackQueue();

        o3DoneLatch.reset();
        int queuedCount = 0;
        for (int i = 0; i < columnCount; i++) {
            final int type = metadata.getColumnType(i);
            if (timestampIndex != i && type > 0) {
                long cursor = pubSeq.next();
                if (cursor > -1) {
                    final O3CallbackTask task = queue.get(cursor);
                    task.of(
                            o3DoneLatch,
                            i,
                            type,
                            mergedTimestamps,
                            countInLag,
                            mappedRowLo,
                            mappedRoHi,
                            ColumnType.isVariableLength(type) ? o3MergeLagVarColumnRef : o3MergeLagFixColumnRef
                    );
                    queuedCount++;
                    pubSeq.done(cursor);
                } else {
                    o3MergeIntoLagColumn(mergedTimestamps, i, type, countInLag, mappedRowLo, mappedRoHi);
                }
            }
        }

        dispatchO3CallbackQueue(queue, queuedCount);
        swapO3ColumnsExcept(timestampIndex);
    }

    private void o3MergeIntoLagColumn(long mergedTimestampAddress, int columnIndex, int type, long lagRows, long mappedRowLo, long mappedRowHi) {
        if (ColumnType.isVariableLength(type)) {
            o3MergeVarColumnLag(columnIndex, type, mergedTimestampAddress, lagRows, mappedRowLo, mappedRowHi);
        } else {
            o3MergeFixColumnLag(columnIndex, type, mergedTimestampAddress, lagRows, mappedRowLo, mappedRowHi);
        }
    }

    private void o3MergeVarColumnLag(int columnIndex, int columnType, long mergedTimestampAddress, long lagRows, long mappedRowLo, long mappedRowHi) {
        if (o3ErrorCount.get() > 0) {
            return;
        }
        try {
            final long rowCount = lagRows + mappedRowHi - mappedRowLo;
            final int primaryIndex = getPrimaryColumnIndex(columnIndex);
            final int secondaryIndex = primaryIndex + 1;

            final MemoryCR src1Data = o3Columns.getQuick(primaryIndex);
            final MemoryCR src1Index = o3Columns.getQuick(secondaryIndex);
            final MemoryCR src2Data = o3MemColumns.getQuick(primaryIndex);
            final MemoryCR src2Index = o3MemColumns.getQuick(secondaryIndex);

            final MemoryCARW destData = o3MemColumns2.getQuick(primaryIndex);
            final MemoryCARW destIndex = o3MemColumns2.getQuick(secondaryIndex);

            // ensure we have enough memory allocated
            final long src1DataHi = src1Index.getLong(mappedRowHi << 3);
            final long src1DataLo = src1Index.getLong(mappedRowLo << 3);
            final long src1DataSize = src1DataHi - src1DataLo;
            assert src1Data.size() >= src1DataSize;
            final long src2DataSize = lagRows > 0 ? src2Index.getLong(lagRows << 3) : 0;
            assert src2Data.size() >= src2DataSize;

            destData.jumpTo(src1DataSize + src2DataSize);
            destIndex.jumpTo((rowCount + 1) >> 3);
            destIndex.putLong(rowCount << 3, src1DataSize + src2DataSize);

            // exclude the trailing offset from shuffling
            final long destDataAddr = destData.addressOf(0);
            final long destIndxAddr = destIndex.addressOf(0);

            final long src1DataAddr = src1Data.addressOf(src1DataLo) - src1DataLo;
            final long src1IndxAddr = src1Index.addressOf(mappedRowLo << 3);
            final long src2DataAddr = src2Data.addressOf(0);
            final long src2IndxAddr = src2Index.addressOf(0);

            if (columnType == ColumnType.STRING) {
                // add max offset so that we do not have conditionals inside loop
                Vect.oooMergeCopyStrColumn(
                        mergedTimestampAddress,
                        rowCount,
                        src1IndxAddr,
                        src1DataAddr,
                        src2IndxAddr,
                        src2DataAddr,
                        destIndxAddr,
                        destDataAddr,
                        0L
                );
            } else if (columnType == ColumnType.BINARY) {
                Vect.oooMergeCopyBinColumn(
                        mergedTimestampAddress,
                        rowCount,
                        src1IndxAddr,
                        src1DataAddr,
                        src2IndxAddr,
                        src2DataAddr,
                        destIndxAddr,
                        destDataAddr,
                        0L
                );
            } else {
                throw new UnsupportedOperationException("unsupported column type:" + ColumnType.nameOf(columnType));
            }
        } catch (Throwable e) {
            handleWorkStealingException(
                    "cannot merge variable length column into lag",
                    columnIndex,
                    columnType,
                    mergedTimestampAddress,
                    lagRows,
                    mappedRowLo,
                    mappedRowHi,
                    e
            );
        }
    }

    private void o3MoveLag0(
            int columnIndex,
            final int columnType,
            long copyToLagRowCount,
            long columnDataRowOffset,
            long existingLagRows,
            long excludeSymbols
    ) {
        if (o3ErrorCount.get() > 0) {
            return;
        }
        try {
            if (columnIndex > -1) {
                MemoryCR o3SrcDataMem = o3Columns.get(getPrimaryColumnIndex(columnIndex));
                MemoryCR o3SrcIndexMem = o3Columns.get(getSecondaryColumnIndex(columnIndex));
                MemoryARW o3DstDataMem = o3MemColumns.get(getPrimaryColumnIndex(columnIndex));
                MemoryARW o3DstIndexMem = o3MemColumns.get(getSecondaryColumnIndex(columnIndex));

                if (o3SrcDataMem == o3DstDataMem && excludeSymbols > 0 && columnType == ColumnType.SYMBOL) {
                    // nothing to do. This is the case when WAL symbols are remapped to the correct place in LAG buffers.
                    return;
                }

                long size;
                long sourceOffset;
                long destOffset;
                final int shl = ColumnType.pow2SizeOf(columnType);
                if (null == o3SrcIndexMem) {
                    // Fixed size column
                    sourceOffset = columnDataRowOffset << shl;
                    size = copyToLagRowCount << shl;
                    destOffset = existingLagRows << shl;
                } else {
                    // Var size column
                    long committedIndexOffset = columnDataRowOffset << 3;
                    sourceOffset = o3SrcIndexMem.getLong(committedIndexOffset);
                    size = o3SrcIndexMem.getLong((columnDataRowOffset + copyToLagRowCount) << 3) - sourceOffset;
                    destOffset = existingLagRows == 0 ? 0L : o3DstIndexMem.getLong(existingLagRows << 3);

                    // adjust append position of the index column to
                    // maintain n+1 number of entries
                    o3DstIndexMem.jumpTo((existingLagRows + copyToLagRowCount + 1) << 3);

                    // move count + 1 rows, to make sure index column remains n+1
                    // the data is copied back to start of the buffer, no need to set size first
                    O3Utils.shiftCopyFixedSizeColumnData(
                            sourceOffset - destOffset,
                            o3SrcIndexMem.addressOf(committedIndexOffset),
                            0,
                            copyToLagRowCount, // No need to do +1 here, hi is inclusive
                            o3DstIndexMem.addressOf(existingLagRows << 3)
                    );
                }


                o3DstDataMem.jumpTo(destOffset + size);
                assert o3SrcDataMem.size() >= size;
                Vect.memmove(o3DstDataMem.addressOf(destOffset), o3SrcDataMem.addressOf(sourceOffset), size);
                // the data is copied back to start of the buffer, no need to set size first
            } else {
                MemoryCR o3SrcDataMem = o3Columns.get(getPrimaryColumnIndex(-columnIndex - 1));

                // Special case, designated timestamp column
                // Move values and set index to  0..copyToLagRowCount
                final long sourceOffset = columnDataRowOffset << 4;
                o3TimestampMem.jumpTo((copyToLagRowCount + existingLagRows) << 4);
                final long dstTimestampAddr = o3TimestampMem.getAddress() + (existingLagRows << 4);
                Vect.shiftTimestampIndex(o3SrcDataMem.addressOf(sourceOffset), copyToLagRowCount, dstTimestampAddr);
            }
        } catch (Throwable ex) {
            handleWorkStealingException(
                    "o3 move lag failed",
                    columnIndex,
                    columnType,
                    copyToLagRowCount,
                    columnDataRowOffset,
                    existingLagRows,
                    excludeSymbols,
                    ex
            );
        }
    }

    private long o3MoveUncommitted(final int timestampIndex) {
        final long committedRowCount = txWriter.unsafeCommittedFixedRowCount() + txWriter.unsafeCommittedTransientRowCount();
        final long rowsAdded = txWriter.getRowCount() - committedRowCount;
        final long transientRowCount = txWriter.getTransientRowCount();
        final long transientRowsAdded = Math.min(transientRowCount, rowsAdded);
        if (transientRowsAdded > 0) {
            LOG.debug()
                    .$("o3 move uncommitted [table=").utf8(tableToken.getTableName())
                    .$(", transientRowsAdded=").$(transientRowsAdded)
                    .I$();
            final long committedTransientRowCount = transientRowCount - transientRowsAdded;
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
            long transientRowsAdded,
            long ignore1,
            long ignore2
    ) {
        if (o3ErrorCount.get() > 0) {
            return;
        }
        try {
            if (colIndex > -1) {
                MemoryMA srcDataMem = getPrimaryColumn(colIndex);
                int shl = ColumnType.pow2SizeOf(columnType);
                long srcFixOffset;
                final MemoryARW o3DataMem = o3MemColumns.get(getPrimaryColumnIndex(colIndex));
                final MemoryARW o3IndexMem = o3MemColumns.get(getSecondaryColumnIndex(colIndex));

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
                    final int indexShl = 3; // ColumnType.pow2SizeOf(ColumnType.LONG);
                    final MemoryMA srcFixMem = getSecondaryColumn(colIndex);
                    long sourceOffset = (committedTransientRowCount - columnTop) << indexShl;

                    // the size includes trailing LONG
                    long sourceLen = (transientRowsAdded + 1) << indexShl;
                    long dstAppendOffset = o3IndexMem.getAppendOffset();

                    // ensure memory is available
                    o3IndexMem.jumpTo(dstAppendOffset + (transientRowsAdded << indexShl));
                    long alignedExtraLen;
                    long srcAddress = srcFixMem.map(sourceOffset, sourceLen);
                    boolean locallyMapped = srcAddress == 0;

                    if (!locallyMapped) {
                        alignedExtraLen = 0;
                    } else {
                        // Linux requires the mmap offset to be page aligned
                        final long alignedOffset = Files.floorPageSize(sourceOffset);
                        alignedExtraLen = sourceOffset - alignedOffset;
                        srcAddress = mapRO(ff, srcFixMem.getFd(), sourceLen + alignedExtraLen, alignedOffset, MemoryTag.MMAP_TABLE_WRITER);
                    }

                    final long srcVarOffset = Unsafe.getUnsafe().getLong(srcAddress + alignedExtraLen);
                    O3Utils.shiftCopyFixedSizeColumnData(
                            srcVarOffset - dstVarOffset,
                            srcAddress + alignedExtraLen + Long.BYTES,
                            0,
                            transientRowsAdded - 1,
                            // copy uncommitted index over the trailing LONG
                            o3IndexMem.addressOf(dstAppendOffset)
                    );

                    if (locallyMapped) {
                        // If memory mapping was mapped specially for this move, close it
                        ff.munmap(srcAddress, sourceLen + alignedExtraLen, MemoryTag.MMAP_TABLE_WRITER);
                    }

                    extendedSize = srcDataMem.getAppendOffset() - srcVarOffset;
                    srcFixOffset = srcVarOffset;
                    srcFixMem.jumpTo(sourceOffset + Long.BYTES);
                }

                o3DataMem.jumpTo(dstVarOffset + extendedSize);
                long appendAddress = o3DataMem.addressOf(dstVarOffset);
                long sourceAddress = srcDataMem.map(srcFixOffset, extendedSize);
                if (sourceAddress != 0) {
                    Vect.memcpy(appendAddress, sourceAddress, extendedSize);
                } else {
                    // Linux requires the mmap offset to be page aligned
                    long alignedOffset = Files.floorPageSize(srcFixOffset);
                    long alignedExtraLen = srcFixOffset - alignedOffset;
                    sourceAddress = mapRO(ff, srcDataMem.getFd(), extendedSize + alignedExtraLen, alignedOffset, MemoryTag.MMAP_TABLE_WRITER);
                    Vect.memcpy(appendAddress, sourceAddress + alignedExtraLen, extendedSize);
                    ff.munmap(sourceAddress, extendedSize + alignedExtraLen, MemoryTag.MMAP_TABLE_WRITER);
                }
                srcDataMem.jumpTo(srcFixOffset);
            } else {
                // Timestamp column
                colIndex = -colIndex - 1;
                int shl = ColumnType.pow2SizeOf(ColumnType.TIMESTAMP);
                MemoryMA srcDataMem = getPrimaryColumn(colIndex);
                // this cannot have "top"
                long srcFixOffset = committedTransientRowCount << shl;
                long srcFixLen = transientRowsAdded << shl;
                long alignedExtraLen;
                long address = srcDataMem.map(srcFixOffset, srcFixLen);
                boolean locallyMapped = address == 0;

                // column could not provide necessary length of buffer
                // because perhaps its internal buffer is not big enough
                if (!locallyMapped) {
                    alignedExtraLen = 0;
                } else {
                    // Linux requires the mmap offset to be page aligned
                    long alignedOffset = Files.floorPageSize(srcFixOffset);
                    alignedExtraLen = srcFixOffset - alignedOffset;
                    address = mapRO(ff, srcDataMem.getFd(), srcFixLen + alignedExtraLen, alignedOffset, MemoryTag.MMAP_TABLE_WRITER);
                }

                try {
                    for (long n = 0; n < transientRowsAdded; n++) {
                        long ts = Unsafe.getUnsafe().getLong(address + alignedExtraLen + (n << shl));
                        o3TimestampMem.putLong128(ts, o3RowCount + n);
                    }
                } finally {
                    if (locallyMapped) {
                        ff.munmap(address, srcFixLen + alignedExtraLen, MemoryTag.MMAP_TABLE_WRITER);
                    }
                }

                srcDataMem.jumpTo(srcFixOffset);
            }
        } catch (Throwable ex) {
            handleWorkStealingException(
                    "could not move uncommitted data",
                    colIndex,
                    columnType,
                    committedTransientRowCount,
                    transientRowsAdded,
                    ignore1,
                    ignore2,
                    ex
            );
        }
    }

    private void o3OpenColumnSafe(Sequence openColumnSubSeq, long cursor, O3OpenColumnTask openColumnTask) {
        try {
            O3OpenColumnJob.openColumn(openColumnTask, cursor, openColumnSubSeq);
        } catch (CairoException | CairoError e) {
            LOG.error().$((Sinkable) e).$();
        } catch (Throwable e) {
            LOG.error().$(e).$();
        }
    }

    private void o3OpenColumns() {
        for (int i = 0; i < columnCount; i++) {
            if (metadata.getColumnType(i) > 0) {
                MemoryARW mem1 = o3MemColumns.getQuick(getPrimaryColumnIndex(i));
                mem1.jumpTo(0);
                MemoryARW mem2 = o3MemColumns.getQuick(getSecondaryColumnIndex(i));
                if (mem2 != null) {
                    mem2.jumpTo(0);
                    mem2.putLong(0);
                }
            }
        }
        activeColumns = o3MemColumns;
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
        txWriter.minTimestamp = Math.min(timestampMin, txWriter.minTimestamp);
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
                    .$("merged partition [table=`").utf8(tableToken.getTableName())
                    .$("`, ts=").$ts(partitionTimestamp)
                    .$(", txn=").$(txWriter.txn).I$();
            txWriter.updatePartitionSizeAndTxnByIndex(partitionIndex, partitionSize);
            partitionRemoveCandidates.add(partitionTimestamp, srcDataTxn);
            txWriter.bumpPartitionTableVersion();
        } else {
            if (partitionTimestamp != lastPartitionTimestamp) {
                txWriter.bumpPartitionTableVersion();
            }
            txWriter.updatePartitionSizeByIndex(partitionIndex, partitionTimestamp, partitionSize);
        }
    }

    private void o3ProcessPartitionSafe(Sequence partitionSubSeq, long cursor, O3PartitionTask partitionTask) {
        try {
            O3PartitionJob.processPartition(partitionTask, cursor, partitionSubSeq);
        } catch (CairoException | CairoError e) {
            LOG.error().$((Sinkable) e).$();
        } catch (Throwable e) {
            LOG.error().$(e).$();
        }
    }

    private long o3ScheduleMoveUncommitted0(int timestampIndex, long transientRowsAdded, long committedTransientRowCount) {
        if (transientRowsAdded > 0) {
            final Sequence pubSeq = this.messageBus.getO3CallbackPubSeq();
            final RingQueue<O3CallbackTask> queue = this.messageBus.getO3CallbackQueue();
            o3DoneLatch.reset();
            int queuedCount = 0;

            for (int colIndex = 0; colIndex < columnCount; colIndex++) {
                int columnType = metadata.getColumnType(colIndex);
                if (columnType > 0) {
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
                                    IGNORE,
                                    IGNORE,
                                    this.o3MoveUncommittedRef
                            );
                        } finally {
                            queuedCount++;
                            pubSeq.done(cursor);
                        }
                    } else {
                        o3MoveUncommitted0(columnIndex, columnType, committedTransientRowCount, transientRowsAdded, IGNORE, IGNORE);
                    }
                }
            }

            dispatchO3CallbackQueue(queue, queuedCount);
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
            MemoryARW o3DataMem = o3MemColumns.get(getPrimaryColumnIndex(columnIndex));
            MemoryARW o3IndexMem = o3MemColumns.get(getSecondaryColumnIndex(columnIndex));

            long size;
            if (null == o3IndexMem) {
                // Fixed size column
                size = o3RowCount << ColumnType.pow2SizeOf(columnType);
            } else {
                // Var size column
                if (o3RowCount > 0) {
                    size = o3IndexMem.getLong(o3RowCount * 8);
                } else {
                    size = 0;
                }
                o3IndexMem.jumpTo((o3RowCount + 1) * 8);
            }

            o3DataMem.jumpTo(size);
        } else {
            // Special case, designated timestamp column
            o3TimestampMem.jumpTo(o3RowCount * 16);
        }
    }

    private void o3ShiftLagRowsUp(int timestampIndex, long o3LagRowCount, long o3RowCount, long existingLagRowCount, boolean excludeSymbols) {

        final Sequence pubSeq = this.messageBus.getO3CallbackPubSeq();
        final RingQueue<O3CallbackTask> queue = this.messageBus.getO3CallbackQueue();

        o3DoneLatch.reset();
        int queuedCount = 0;
        long excludeSymbolsL = excludeSymbols ? 1 : 0;
        for (int colIndex = 0; colIndex < columnCount; colIndex++) {
            int columnType = metadata.getColumnType(colIndex);
            int columnIndex = colIndex != timestampIndex ? colIndex : -colIndex - 1;

            if (columnType > 0) {
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
                                existingLagRowCount,
                                excludeSymbolsL,
                                this.o3MoveLagRef
                        );

                    } finally {
                        queuedCount++;
                        pubSeq.done(cursor);
                    }
                } else {
                    o3MoveLag0(columnIndex, columnType, o3LagRowCount, o3RowCount, existingLagRowCount, excludeSymbolsL);
                }
            }
        }

        dispatchO3CallbackQueue(queue, queuedCount);
    }

    private void o3Sort(long mergedTimestamps, int timestampIndex, long rowCount) {
        o3ErrorCount.set(0);
        lastErrno = 0;

        final Sequence pubSeq = this.messageBus.getO3CallbackPubSeq();
        final RingQueue<O3CallbackTask> queue = this.messageBus.getO3CallbackQueue();

        o3DoneLatch.reset();
        int queuedCount = 0;
        for (int i = 0; i < columnCount; i++) {
            final int type = metadata.getColumnType(i);
            if (timestampIndex != i && type > 0) {
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
                                IGNORE,
                                IGNORE,
                                ColumnType.isVariableLength(type) ? oooSortVarColumnRef : oooSortFixColumnRef
                        );
                    } finally {
                        queuedCount++;
                        pubSeq.done(cursor);
                    }
                } else {
                    o3SortColumn(mergedTimestamps, i, type, rowCount);
                }
            }
        }

        dispatchO3CallbackQueue(queue, queuedCount);
        swapO3ColumnsExcept(timestampIndex);
    }

    private void o3SortColumn(long mergedTimestamps, int i, int type, long rowCount) {
        if (ColumnType.isVariableLength(type)) {
            o3SortVarColumn(i, type, mergedTimestamps, rowCount, IGNORE, IGNORE);
        } else {
            o3SortFixColumn(i, type, mergedTimestamps, rowCount, IGNORE, IGNORE);
        }
    }

    private void o3SortFixColumn(
            int columnIndex,
            final int columnType,
            long mergedTimestampsAddr,
            long valueCount,
            long ignore1,
            long ignore2
    ) {
        if (o3ErrorCount.get() > 0) {
            return;
        }
        try {
            final int columnOffset = getPrimaryColumnIndex(columnIndex);
            final MemoryCR mem = o3Columns.getQuick(columnOffset);
            final MemoryCARW mem2 = o3MemColumns2.getQuick(columnOffset);
            final int shl = ColumnType.pow2SizeOf(columnType);
            final long src = mem.addressOf(0);
            mem2.jumpTo(valueCount << shl);
            final long tgtDataAddr = mem2.addressOf(0);
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
                case 4:
                    Vect.indexReshuffle128Bit(src, tgtDataAddr, mergedTimestampsAddr, valueCount);
                    break;
                case 5:
                    Vect.indexReshuffle256Bit(src, tgtDataAddr, mergedTimestampsAddr, valueCount);
                    break;
                default:
                    assert false : "col type is unsupported";
                    break;
            }
        } catch (Throwable th) {
            handleWorkStealingException("sort fixed size column failed", columnIndex, columnType, mergedTimestampsAddr, valueCount, ignore1, ignore2, th);
        }
    }

    private void o3SortVarColumn(
            int columnIndex,
            int columnType,
            long mergedTimestampsAddr,
            long valueCount,
            long ignore1,
            long ignore2
    ) {
        if (o3ErrorCount.get() > 0) {
            return;
        }
        try {
            final int primaryIndex = getPrimaryColumnIndex(columnIndex);
            final int secondaryIndex = primaryIndex + 1;
            final MemoryCR dataMem = o3Columns.getQuick(primaryIndex);
            final MemoryCR indexMem = o3Columns.getQuick(secondaryIndex);
            final MemoryCARW dataMem2 = o3MemColumns2.getQuick(primaryIndex);
            final MemoryCARW indexMem2 = o3MemColumns2.getQuick(secondaryIndex);
            // ensure we have enough memory allocated
            final long srcDataAddr = dataMem.addressOf(0);
            final long srcIndxAddr = indexMem.addressOf(0);
            // exclude the trailing offset from shuffling
            final long tgtDataAddr = dataMem2.resize(dataMem.size());
            final long tgtIndxAddr = indexMem2.resize(valueCount * Long.BYTES);

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
            dataMem2.jumpTo(offset);
            indexMem2.jumpTo(valueCount * Long.BYTES);
            indexMem2.putLong(offset);
        } catch (Throwable th) {
            handleWorkStealingException("sort variable size column failed", columnIndex, columnType, mergedTimestampsAddr, valueCount, ignore1, ignore2, th);
        }
    }

    private void o3TimestampSetter(long timestamp) {
        o3TimestampMem.putLong128(timestamp, getO3RowCount0());
        o3CommitBatchTimestampMin = Math.min(o3CommitBatchTimestampMin, timestamp);
    }

    private void openColumnFiles(CharSequence name, long columnNameTxn, int columnIndex, int pathTrimToLen) {
        MemoryMA mem1 = getPrimaryColumn(columnIndex);
        MemoryMA mem2 = getSecondaryColumn(columnIndex);

        try {
            mem1.of(ff,
                    dFile(path.trimTo(pathTrimToLen), name, columnNameTxn),
                    configuration.getDataAppendPageSize(),
                    -1,
                    MemoryTag.MMAP_TABLE_WRITER,
                    configuration.getWriterFileOpenOpts(),
                    Files.POSIX_MADV_RANDOM
            );
            if (mem2 != null) {
                mem2.of(
                        ff,
                        iFile(path.trimTo(pathTrimToLen), name, columnNameTxn),
                        configuration.getDataAppendPageSize(),
                        -1,
                        MemoryTag.MMAP_TABLE_WRITER,
                        configuration.getWriterFileOpenOpts(),
                        Files.POSIX_MADV_RANDOM
                );
            }
        } finally {
            path.trimTo(pathTrimToLen);
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
            long partitionTimestamp = txWriter.getLastPartitionTimestamp();
            setStateForTimestamp(path, partitionTimestamp, false);
            final int plen = path.length();
            final int columnIndex = columnCount - 1;

            // Adding column in the current transaction.
            long columnNameTxn = getTxn();

            // index must be created before column is initialised because
            // it uses primary column object as temporary tool
            if (indexFlag) {
                createIndexFiles(name, columnNameTxn, indexValueBlockCapacity, plen, true);
            }

            openColumnFiles(name, columnNameTxn, columnIndex, plen);
            if (txWriter.getTransientRowCount() > 0) {
                // write top offset to column version file
                columnVersionWriter.upsert(txWriter.getLastPartitionTimestamp(), columnIndex, columnNameTxn, txWriter.getTransientRowCount());
            }

            if (indexFlag) {
                ColumnIndexer indexer = indexers.getQuick(columnIndex);
                assert indexer != null;
                indexers.getQuick(columnIndex).configureFollowerAndWriter(configuration, path.trimTo(plen), name, columnNameTxn, getPrimaryColumn(columnIndex), txWriter.getTransientRowCount());
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
                throw CairoException.critical(ff.errno()).put("Cannot create directory: ").put(path);
            }

            assert columnCount > 0;

            lastOpenPartitionTs = txWriter.getPartitionTimestampLo(timestamp);
            lastOpenPartitionIsReadOnly = partitionBy != PartitionBy.NONE && txWriter.isPartitionReadOnlyByPartitionTimestamp(lastOpenPartitionTs);

            for (int i = 0; i < columnCount; i++) {
                if (metadata.getColumnType(i) > 0) {
                    final CharSequence name = metadata.getColumnName(i);
                    long columnNameTxn = columnVersionWriter.getColumnNameTxn(lastOpenPartitionTs, i);
                    final ColumnIndexer indexer = metadata.isColumnIndexed(i) ? indexers.getQuick(i) : null;
                    final long columnTop;

                    // prepare index writer if column requires indexing
                    if (indexer != null) {
                        // we have to create files before columns are open
                        // because we are reusing MAMemoryImpl object from columns list
                        createIndexFiles(name, columnNameTxn, metadata.getIndexValueBlockCapacity(i), plen, txWriter.getTransientRowCount() < 1);
                        indexer.closeSlider();
                    }

                    openColumnFiles(name, columnNameTxn, i, plen);
                    columnTop = columnVersionWriter.getColumnTopQuick(lastOpenPartitionTs, i);
                    columnTops.extendAndSet(i, columnTop);

                    if (indexer != null) {
                        indexer.configureFollowerAndWriter(configuration, path, name, columnNameTxn, getPrimaryColumn(i), columnTop);
                    }
                }
            }
            populateDenseIndexerList();
            LOG.info().$("switched partition [path='").utf8(path).$('\'').I$();
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
                    throw CairoException.critical(0).put("corrupt ").put(path);
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

    private void processAsyncWriterCommand(
            AsyncWriterCommand asyncWriterCommand,
            TableWriterTask cmd,
            long cursor,
            Sequence sequence,
            boolean contextAllowsAnyStructureChanges
    ) {
        final int cmdType = cmd.getType();
        final long correlationId = cmd.getInstance();
        final long tableId = cmd.getTableId();

        int errorCode = 0;
        CharSequence errorMsg = null;
        long affectedRowsCount = 0;
        try {
            publishTableWriterEvent(cmdType, tableId, correlationId, AsyncWriterCommand.Error.OK, null, 0L, TSK_BEGIN);
            LOG.info()
                    .$("received async cmd [type=").$(cmdType)
                    .$(", tableName=").utf8(tableToken.getTableName())
                    .$(", tableId=").$(tableId)
                    .$(", correlationId=").$(correlationId)
                    .$(", cursor=").$(cursor)
                    .I$();
            asyncWriterCommand = asyncWriterCommand.deserialize(cmd);
            affectedRowsCount = asyncWriterCommand.apply(this, contextAllowsAnyStructureChanges);
        } catch (TableReferenceOutOfDateException ex) {
            LOG.info()
                    .$("cannot complete async cmd, reader is out of date [type=").$(cmdType)
                    .$(", tableName=").utf8(tableToken.getTableName())
                    .$(", tableId=").$(tableId)
                    .$(", correlationId=").$(correlationId)
                    .I$();
            errorCode = READER_OUT_OF_DATE;
            errorMsg = ex.getMessage();
        } catch (AlterTableContextException ex) {
            LOG.info()
                    .$("cannot complete async cmd, table structure change is not allowed [type=").$(cmdType)
                    .$(", tableName=").utf8(tableToken.getTableName())
                    .$(", tableId=").$(tableId)
                    .$(", correlationId=").$(correlationId)
                    .I$();
            errorCode = STRUCTURE_CHANGE_NOT_ALLOWED;
            errorMsg = "async cmd cannot change table structure while writer is busy";
        } catch (CairoException ex) {
            errorCode = CAIRO_ERROR;
            errorMsg = ex.getFlyweightMessage();
        } catch (Throwable ex) {
            LOG.error().$("error on processing async cmd [type=").$(cmdType)
                    .$(", tableName=").utf8(tableToken.getTableName())
                    .$(", ex=").$(ex)
                    .I$();
            errorCode = UNEXPECTED_ERROR;
            errorMsg = ex.getMessage();
        } finally {
            sequence.done(cursor);
        }
        publishTableWriterEvent(cmdType, tableId, correlationId, errorCode, errorMsg, affectedRowsCount, TSK_COMPLETE);
    }

    private void processCommandQueue(boolean contextAllowsAnyStructureChanges) {
        long cursor;
        while ((cursor = commandSubSeq.next()) > -1) {
            TableWriterTask cmd = commandQueue.get(cursor);
            processCommandQueue(cmd, commandSubSeq, cursor, contextAllowsAnyStructureChanges);
        }
    }

    private void processO3Block(
            final long o3LagRowCount,
            int timestampIndex,
            long sortedTimestampsAddr,
            final long srcOooMax,
            long o3TimestampMin,
            long o3TimestampMax,
            boolean flattenTimestamp,
            long rowLo
    ) {
        o3ErrorCount.set(0);
        lastErrno = 0;
        partitionRemoveCandidates.clear();
        o3ColumnCounters.clear();
        o3BasketPool.clear();

        // move uncommitted is liable to change max timestamp
        // however we need to identify last partition before max timestamp skips to NULL for example
        final long maxTimestamp = txWriter.getMaxTimestamp();
        final long transientRowCount = txWriter.transientRowCount;

        o3DoneLatch.reset();
        o3PartitionUpdRemaining.set(0L);
        boolean success = true;
        int latchCount = 0;
        long srcOoo = rowLo;
        int pCount = 0;
        try {
            // We do not know upfront which partition is going to be last because this is
            // a single pass over the data. Instead, we will update transient row count in a rolling
            // manner, assuming the partition marked "last" is the last and then for a new partition
            // we move prevTransientRowCount into the "fixedRowCount" sum and set new value on the
            // transientRowCount
            long commitTransientRowCount = transientRowCount;

            resizeColumnTopSink(o3TimestampMin, o3TimestampMax);
            resizePartitionUpdateSink(o3TimestampMin, o3TimestampMax);

            // One loop iteration per partition.
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
                                srcOooLo,
                                srcOooMax - 1,
                                BinarySearch.SCAN_DOWN
                        );
                    } else {
                        srcOooHi = srcOooMax - 1;
                    }

                    final long partitionTimestamp = partitionFloorMethod.floor(o3Timestamp);

                    // This partition is the last partition.
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
                        // A version needed to housekeep dropped partitions
                        // When partition created without O3 merge, use `txn-1` as partition version.
                        // `txn` version is used when partition is merged. Both `txn-1` and `txn` can
                        // be written within the same commit when new partition initially written in order
                        // and then O3 triggers a merge of the partition.
                        srcNameTxn = txWriter.getTxn() - 1;
                    }

                    // We're appending onto the last (active) partition.
                    final boolean append = last && (srcDataMax == 0 || o3Timestamp >= maxTimestamp);

                    // Number of rows to insert from the O3 segment into this partition.
                    final long srcOooBatchRowSize = srcOooHi - srcOooLo + 1;

                    // Final partition size after current insertions.
                    final long partitionSize = srcDataMax + srcOooBatchRowSize;

                    // check partition read-only state
                    final boolean partitionIsReadOnly = txWriter.isPartitionReadOnlyByPartitionTimestamp(partitionTimestamp);

                    pCount++;

                    LOG.info().
                            $("o3 partition task [table=").utf8(tableToken.getTableName())
                            .$(", partitionIsReadOnly=").$(partitionIsReadOnly)
                            .$(", srcOooBatchRowSize=").$(srcOooBatchRowSize)
                            .$(", srcOooLo=").$(srcOooLo)
                            .$(", srcOooHi=").$(srcOooHi)
                            .$(", srcOooMax=").$(srcOooMax)
                            .$(", o3RowCount=").$(o3RowCount)
                            .$(", o3LagRowCount=").$(o3LagRowCount)
                            .$(", srcDataMax=").$(srcDataMax)
                            .$(", o3TimestampMin=").$ts(o3TimestampMin)
                            .$(", o3Timestamp=").$ts(o3Timestamp)
                            .$(", o3TimestampMax=").$ts(o3TimestampMax)
                            .$(", partitionTimestamp=").$ts(partitionTimestamp)
                            .$(", partitionIndex=").$(partitionIndex)
                            .$(", partitionSize=").$(partitionSize)
                            .$(", maxTimestamp=").$ts(maxTimestamp)
                            .$(", last=").$(last)
                            .$(", append=").$(append)
                            .$(", pCount=").$(pCount)
                            .$(", flattenTimestamp=").$(flattenTimestamp)
                            .$(", memUsed=").$(Unsafe.getMemUsed())
                            .I$();

                    if (partitionIsReadOnly) {
                        // move over read-only partitions
                        LOG.critical()
                                .$("o3 ignoring write on read-only partition [table=").utf8(tableToken.getTableName())
                                .$(", timestamp=").$ts(partitionTimestamp)
                                .$(", numRows=").$(srcOooBatchRowSize)
                                .$();
                        continue;
                    }

                    if (partitionTimestamp < lastPartitionTimestamp) {
                        // increment fixedRowCount by number of rows old partition incremented
                        this.txWriter.fixedRowCount += srcOooBatchRowSize;
                    } else if (partitionTimestamp == lastPartitionTimestamp) {
                        // this is existing "last" partition, we can set the size directly
                        commitTransientRowCount = partitionSize;
                    } else {
                        // this is potentially a new last partition
                        this.txWriter.fixedRowCount += commitTransientRowCount;
                        commitTransientRowCount = partitionSize;
                    }

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

                        columnCounter.set(TableUtils.compressColumnCount(metadata));
                        Path pathToPartition = Path.getThreadLocal(path);
                        TableUtils.setPathForPartition(pathToPartition, partitionBy, o3TimestampMin, false);
                        TableUtils.txnPartitionConditionally(pathToPartition, srcNameTxn);
                        final int plen = pathToPartition.length();
                        int columnsPublished = 0;
                        for (int i = 0; i < columnCount; i++) {
                            final int columnType = metadata.getColumnType(i);
                            if (columnType < 0) {
                                continue;
                            }
                            final int colOffset = TableWriter.getPrimaryColumnIndex(i);
                            final boolean notTheTimestamp = i != timestampIndex;
                            final CharSequence columnName = metadata.getColumnName(i);
                            final int indexBlockCapacity = metadata.isColumnIndexed(i) ? metadata.getIndexValueBlockCapacity(i) : -1;
                            final BitmapIndexWriter indexWriter = indexBlockCapacity > -1 ? getBitmapIndexWriter(i) : null;
                            final MemoryR oooMem1 = o3Columns.getQuick(colOffset);
                            final MemoryR oooMem2 = o3Columns.getQuick(colOffset + 1);
                            final MemoryMA mem1 = columns.getQuick(colOffset);
                            final MemoryMA mem2 = columns.getQuick(colOffset + 1);
                            final long srcDataTop = getColumnTop(i);
                            final long srcOooFixAddr;
                            final long srcOooVarAddr;
                            final MemoryMA dstFixMem;
                            final MemoryMA dstVarMem;
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
                                        indexWriter,
                                        getColumnNameTxn(partitionTimestamp, i)
                                );
                            } catch (Throwable e) {
                                if (columnCounter.addAndGet(columnsPublished - columnCount) == 0) {
                                    o3ClockDownPartitionUpdateCount();
                                    o3CountDownDoneLatch();
                                }
                                throw e;
                            }
                        }

                        addPhysicallyWrittenRows(srcOooBatchRowSize);
                    } else {
                        if (flattenTimestamp && o3RowCount > 0) {
                            Vect.flattenIndex(sortedTimestampsAddr, o3RowCount);
                            flattenTimestamp = false;
                        }

                        // To collect column top values from o3 partition tasks add them to pre-allocated array of longs
                        // use o3ColumnTopSink LongList and allocate columns + 1 longs per partition
                        // then set first value to partition timestamp
                        long colTopSinkIndex = (long) (pCount - 1) * (metadata.getColumnCount() + 1);
                        long columnTopSinkAddress = colTopSinkIndex * Long.BYTES;
                        long columnTopPartitionSinkAddr = o3ColumnTopSink.getAddress() + columnTopSinkAddress;
                        assert columnTopPartitionSinkAddr + (columnCount + 1L) * Long.BYTES <= o3ColumnTopSink.getAddress() + o3ColumnTopSink.size() * Long.BYTES;

                        o3ColumnTopSink.set(colTopSinkIndex, partitionTimestamp);
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
                                o3Basket,
                                columnTopPartitionSinkAddr + Long.BYTES
                        );
                    }
                } catch (CairoException | CairoError e) {
                    LOG.error().$((Sinkable) e).$();
                    success = false;
                    throw e;
                }
            } // end while(srcOoo < srcOooMax)

            // at this point we should know the last partition row count
            this.txWriter.transientRowCount = commitTransientRowCount;
            this.partitionTimestampHi = Math.max(this.partitionTimestampHi, o3TimestampMax);
            this.txWriter.updateMaxTimestamp(Math.max(txWriter.getMaxTimestamp(), o3TimestampMax));
        } catch (Throwable th) {
            LOG.error().$(th).$();
            throw th;
        } finally {
            // we are stealing work here it is possible we get exception from this method
            LOG.debug()
                    .$("o3 expecting updates [table=").utf8(tableToken.getTableName())
                    .$(", partitionsPublished=").$(pCount)
                    .I$();

            o3ConsumePartitionUpdates();
            o3DoneLatch.await(latchCount);

            o3InError = !success || o3ErrorCount.get() > 0;
            if (success && o3ErrorCount.get() > 0) {
                //noinspection ThrowFromFinallyBlock
                throw CairoException.critical(0).put("bulk update failed and will be rolled back");
            }
        }

        if (o3LagRowCount > 0) {
            LOG.info().$("shifting lag rows up [table=").$(tableToken.getTableName()).$(", lagCount=").$(o3LagRowCount).I$();
            o3ShiftLagRowsUp(timestampIndex, o3LagRowCount, srcOooMax, 0L, false);
        }
    }

    private void processPartitionRemoveCandidates() {
        try {
            final int n = partitionRemoveCandidates.size();
            if (n > 0) {
                processPartitionRemoveCandidates0(n);
            }
        } finally {
            partitionRemoveCandidates.clear();
        }
    }

    private void processPartitionRemoveCandidates0(int n) {
        boolean anyReadersBeforeCommittedTxn = checkScoreboardHasReadersBeforeLastCommittedTxn();
        // This flag will determine to schedule O3PartitionPurgeJob at the end or all done already.
        boolean scheduleAsyncPurge = anyReadersBeforeCommittedTxn;

        if (!anyReadersBeforeCommittedTxn) {
            for (int i = 0; i < n; i += 2) {
                try {
                    final long timestamp = partitionRemoveCandidates.getQuick(i);
                    final long txn = partitionRemoveCandidates.getQuick(i + 1);
                    setPathForPartition(
                            other,
                            partitionBy,
                            timestamp,
                            false
                    );
                    TableUtils.txnPartitionConditionally(other, txn);
                    other.$();
                    int errno = ff.unlinkOrRemove(other, LOG);
                    if (!(errno == 0 || errno == -1)) {
                        LOG.info()
                                .$("could not purge partition version, async purge will be scheduled [path=")
                                .utf8(other)
                                .$(", errno=").$(errno).I$();
                        scheduleAsyncPurge = true;
                    }
                } finally {
                    other.trimTo(rootLen);
                }
            }
        }

        if (scheduleAsyncPurge) {
            // Any more complicated case involve looking at what folders are present on disk before removing
            // do it async in O3PartitionPurgeJob
            if (schedulePurgeO3Partitions(messageBus, tableToken, partitionBy)) {
                LOG.info().$("scheduled to purge partitions [table=").utf8(tableToken.getTableName()).I$();
            } else {
                LOG.error().$("could not queue for purge, queue is full [table=").utf8(tableToken.getTableName()).I$();
            }
        }
    }

    private void publishTableWriterEvent(int cmdType, long tableId, long correlationId, int errorCode, CharSequence errorMsg, long affectedRowsCount, int eventType) {
        long pubCursor;
        do {
            pubCursor = messageBus.getTableWriterEventPubSeq().next();
            if (pubCursor == -2) {
                Os.pause();
            }
        } while (pubCursor < -1);

        if (pubCursor > -1) {
            try {
                final TableWriterTask event = messageBus.getTableWriterEventQueue().get(pubCursor);
                event.of(eventType, tableId, tableToken);
                event.putInt(errorCode);
                if (errorCode != AsyncWriterCommand.Error.OK) {
                    event.putStr(errorMsg);
                } else {
                    event.putLong(affectedRowsCount);
                }
                event.setInstance(correlationId);
            } finally {
                messageBus.getTableWriterEventPubSeq().done(pubCursor);
            }

            // Log result
            if (eventType == TSK_COMPLETE) {
                LogRecord lg = LOG.info()
                        .$("published async command complete event [type=").$(cmdType)
                        .$(",tableName=").utf8(tableToken.getTableName())
                        .$(",tableId=").$(tableId)
                        .$(",correlationId=").$(correlationId);
                if (errorCode != AsyncWriterCommand.Error.OK) {
                    lg.$(",errorCode=").$(errorCode).$(",errorMsg=").$(errorMsg);
                }
                lg.I$();
            }
        } else {
            // Queue is full
            LOG.error()
                    .$("could not publish sync command complete event [type=").$(cmdType)
                    .$(",tableName=").utf8(tableToken.getTableName())
                    .$(",tableId=").$(tableId)
                    .$(",correlationId=").$(correlationId)
                    .I$();
        }
    }

    private long readMinTimestamp(long partitionTimestamp) {
        setStateForTimestamp(other, partitionTimestamp, false);
        try {
            dFile(other, metadata.getColumnName(metadata.getTimestampIndex()), COLUMN_NAME_TXN_NONE);
            if (ff.exists(other)) {
                // read min timestamp value
                final int fd = TableUtils.openRO(ff, other, LOG);
                try {
                    return TableUtils.readLongOrFail(ff, fd, 0, tempMem16b, other);
                } finally {
                    ff.close(fd);
                }
            } else {
                throw CairoException.critical(0).put("Partition does not exist [path=").put(other).put(']');
            }
        } finally {
            other.trimTo(rootLen);
        }
    }

    private void readPartitionMinMax(FilesFacade ff, long partitionTimestamp, Path path, CharSequence columnName, long partitionSize) {
        dFile(path, columnName, COLUMN_NAME_TXN_NONE);
        final int fd = TableUtils.openRO(ff, path, LOG);
        try {
            attachMinTimestamp = ff.readNonNegativeLong(fd, 0);
            attachMaxTimestamp = ff.readNonNegativeLong(fd, (partitionSize - 1) * ColumnType.sizeOf(ColumnType.TIMESTAMP));
            if (attachMinTimestamp < 0 || attachMaxTimestamp < 0) {
                throw CairoException.critical(ff.errno())
                        .put("cannot read min, max timestamp from the column [path=").put(path)
                        .put(", partitionSizeRows=").put(partitionSize)
                        .put(", errno=").put(ff.errno()).put(']');
            }
            if (partitionFloorMethod.floor(attachMinTimestamp) != partitionTimestamp
                    || partitionFloorMethod.floor(attachMaxTimestamp) != partitionTimestamp) {
                throw CairoException.critical(0)
                        .put("invalid timestamp column data in detached partition, data does not match partition directory name [path=").put(path)
                        .put(", minTimestamp=").ts(attachMinTimestamp)
                        .put(", maxTimestamp=").ts(attachMaxTimestamp).put(']');
            }
        } finally {
            ff.close(fd);
        }
    }

    // Scans timestamp file
    // returns size of partition detected, e.g. size of monotonic increase
    // of timestamp longs read from 0 offset to the end of the file
    // It also writes min and max values found in detachedMinTimestamp and detachedMaxTimestamp
    private long readPartitionSizeMinMax(FilesFacade ff, long partitionTimestamp, Path path, CharSequence columnName) {
        int pathLen = path.length();
        try {
            path.concat(TXN_FILE_NAME).$();
            if (ff.exists(path)) {
                if (attachTxReader == null) {
                    attachTxReader = new TxReader(ff);
                }
                attachTxReader.ofRO(path, partitionBy);
                attachTxReader.unsafeLoadAll();

                try {
                    path.trimTo(pathLen);
                    long partitionSize = attachTxReader.getPartitionSizeByPartitionTimestamp(partitionTimestamp);
                    if (partitionSize <= 0) {
                        throw CairoException.nonCritical()
                                .put("partition is not preset in detached txn file [path=")
                                .put(path).put(", partitionSize=").put(partitionSize).put(']');
                    }

                    // Read min and max timestamp values from the file
                    readPartitionMinMax(ff, partitionTimestamp, path.trimTo(pathLen), columnName, partitionSize);
                    return partitionSize;
                } finally {
                    Misc.free(attachTxReader);
                }
            }

            // No txn file found, scan the file to get min, max timestamp
            // Scan forward while value increases

            dFile(path.trimTo(pathLen), columnName, COLUMN_NAME_TXN_NONE);
            final int fd = TableUtils.openRO(ff, path, LOG);
            try {
                long fileSize = ff.length(fd);
                if (fileSize <= 0) {
                    throw CairoException.critical(ff.errno())
                            .put("timestamp column is too small to attach the partition [path=")
                            .put(path).put(", fileSize=").put(fileSize).put(']');
                }
                long mappedMem = mapRO(ff, fd, fileSize, MemoryTag.MMAP_DEFAULT);
                try {
                    long maxTimestamp = partitionTimestamp;
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
                        attachMinTimestamp = Unsafe.getUnsafe().getLong(mappedMem);
                        attachMaxTimestamp = maxTimestamp;
                    }
                    return size;
                } finally {
                    ff.munmap(mappedMem, fileSize, MemoryTag.MMAP_DEFAULT);
                }
            } finally {
                ff.close(fd);
            }
        } finally {
            path.trimTo(pathLen);
        }
    }

    private int readTodo() {
        long todoCount;
        try {
            // This is first FS call to the table directory.
            // If table is removed / renamed this should fail with table does not exist.
            todoCount = openTodoMem();
        } catch (CairoException ex) {
            if (ex.errnoReadPathDoesNotExist()) {
                throw CairoException.tableDoesNotExist(tableToken.getTableName());
            }
            throw ex;
        }
        int todo;
        if (todoCount > 0) {
            todo = (int) todoMem.getLong(40);
        } else {
            todo = -1;
        }
        return todo;
    }

    private void rebuildAttachedPartitionColumnIndex(long partitionTimestamp, long partitionSize, Path path, CharSequence columnName) {
        if (attachIndexBuilder == null) {
            attachIndexBuilder = new IndexBuilder();

            // no need to pass table name, full partition name will be specified
            attachIndexBuilder.of("", configuration);
        }

        attachIndexBuilder.reindexColumn(
                attachColumnVersionReader,
                // use metadata instead of detachedMetadata to get correct value block capacity
                // detachedMetadata does not have the column
                metadata,
                metadata.getColumnIndex(columnName),
                path,
                -1L,
                partitionTimestamp,
                partitionSize
        );
    }

    private void recoverFromMetaRenameFailure(CharSequence columnName) {
        openMetaFile(ff, path, rootLen, metaMem);
    }

    private void recoverFromSwapRenameFailure(CharSequence columnName) {
        recoverFromTodoWriteFailure(columnName);
        clearTodoLog();
    }

    private void recoverFromSymbolMapWriterFailure(CharSequence columnName) {
        removeSymbolMapFilesQuiet(columnName, getTxn());
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
        columnCount--;
        recoverFromSwapRenameFailure(columnName);
        removeSymbolMapWriter(index);
    }

    private void releaseLock(boolean distressed) {
        if (lockFd != -1L) {
            if (distressed) {
                ff.close(lockFd);
                return;
            }

            try {
                lockName(path);
                removeOrException(ff, lockFd, path);
            } finally {
                path.trimTo(rootLen);
            }
        }
    }

    private ReadOnlyObjList<? extends MemoryCR> remapWalSymbols(
            SymbolMapDiffCursor symbolMapDiffCursor,
            long rowLo,
            long rowHi,
            Path walPath,
            long destRowLo
    ) {
        o3ColumnOverrides.clear();
        if (symbolMapDiffCursor != null) {
            SymbolMapDiff symbolMapDiff;
            while ((symbolMapDiff = symbolMapDiffCursor.nextSymbolMapDiff()) != null) {
                int columnIndex = symbolMapDiff.getColumnIndex();
                int columnType = metadata.getColumnType(columnIndex);
                if (columnType == -ColumnType.SYMBOL) {
                    // Scroll the cursor, don't apply, symbol is deleted
                    symbolMapDiff.drain();
                    continue;
                }

                if (!ColumnType.isSymbol(columnType)) {
                    throw CairoException.critical(0).put("WAL column and table writer column types don't match [columnIndex=").put(columnIndex)
                            .put(", walPath=").put(walPath)
                            .put(']');
                }
                boolean identical = createWalSymbolMapping(symbolMapDiff, columnIndex, symbolRewriteMap);

                if (!identical) {
                    int primaryColumnIndex = getPrimaryColumnIndex(columnIndex);
                    MemoryCR o3SymbolColumn = o3Columns.getQuick(primaryColumnIndex);
                    final MemoryCARW symbolColumnDest;

                    // Column is read-only mapped memory, so we need to take in RAM column and remap values into it
                    if (o3ColumnOverrides.size() == 0) {
                        o3ColumnOverrides.addAll(o3Columns);
                    }

                    symbolColumnDest = o3MemColumns.get(primaryColumnIndex);
                    long destOffset = (destRowLo - rowLo) << 2;
                    symbolColumnDest.jumpTo(destOffset + (rowHi << 2));
                    o3ColumnOverrides.setQuick(primaryColumnIndex, symbolColumnDest);
                    final int cleanSymbolCount = symbolMapDiff.getCleanSymbolCount();
                    for (long rowId = rowLo; rowId < rowHi; rowId++) {

                        final long valueOffset = rowId << 2;
                        int symKey = o3SymbolColumn.getInt(valueOffset);
                        assert (symKey >= 0 || symKey == SymbolTable.VALUE_IS_NULL);
                        if (symKey >= cleanSymbolCount) {
                            int newKey = symbolRewriteMap.getQuick(symKey - cleanSymbolCount);
                            if (newKey < 0) {
                                // This symbol was not mapped in WAL
                                // WAL is invalid
                                throw CairoException.critical(0).put("WAL symbol key not mapped [columnIndex=").put(columnIndex)
                                        .put(", columnKey=").put(symKey)
                                        .put(", walPath=").put(walPath)
                                        .put(", walRowId=").put(rowId)
                                        .put(']');
                            }
                            symKey = newKey;
                        }
                        symbolColumnDest.putInt(destOffset + valueOffset, symKey);
                    }
                }
            }
        }

        if (o3ColumnOverrides.size() == 0) {
            // No mappins were made.
            return o3Columns;
        }
        return o3ColumnOverrides;
    }

    private void removeColumn(int columnIndex) {
        final int pi = getPrimaryColumnIndex(columnIndex);
        final int si = getSecondaryColumnIndex(columnIndex);
        freeNullSetter(nullSetters, columnIndex);
        freeNullSetter(o3NullSetters, columnIndex);
        freeNullSetter(o3NullSetters2, columnIndex);
        freeAndRemoveColumnPair(columns, pi, si);
        freeAndRemoveO3ColumnPair(o3MemColumns, pi, si);
        freeAndRemoveO3ColumnPair(o3MemColumns2, pi, si);
        if (columnIndex < indexers.size()) {
            Misc.free(indexers.getAndSetQuick(columnIndex, null));
            populateDenseIndexerList();
        }
    }

    private void removeColumnFiles(CharSequence columnName, int columnIndex, int columnType) {
        try {
            for (int i = txWriter.getPartitionCount() - 1; i > -1L; i--) {
                long partitionTimestamp = txWriter.getPartitionTimestamp(i);
                long partitionNameTxn = txWriter.getPartitionNameTxn(i);
                removeColumnFilesInPartition(columnName, columnIndex, partitionTimestamp, partitionNameTxn);
            }
            if (!PartitionBy.isPartitioned(partitionBy)) {
                removeColumnFilesInPartition(columnName, columnIndex, txWriter.getLastPartitionTimestamp(), -1L);
            }

            long columnNameTxn = columnVersionWriter.getDefaultColumnNameTxn(columnIndex);
            if (ColumnType.isSymbol(columnType)) {
                removeFileAndOrLog(ff, offsetFileName(path.trimTo(rootLen), columnName, columnNameTxn));
                removeFileAndOrLog(ff, charFileName(path.trimTo(rootLen), columnName, columnNameTxn));
                removeFileAndOrLog(ff, keyFileName(path.trimTo(rootLen), columnName, columnNameTxn));
                removeFileAndOrLog(ff, valueFileName(path.trimTo(rootLen), columnName, columnNameTxn));
            }
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void removeColumnFilesInPartition(CharSequence columnName, int columnIndex, long partitionTimestamp, long partitionNameTxn) {
        if (!txWriter.isPartitionReadOnlyByPartitionTimestamp(partitionTimestamp)) {
            setPathForPartition(path, partitionBy, partitionTimestamp, false);
            txnPartitionConditionally(path, partitionNameTxn);
            int plen = path.length();
            long columnNameTxn = columnVersionWriter.getColumnNameTxn(partitionTimestamp, columnIndex);
            removeFileAndOrLog(ff, dFile(path, columnName, columnNameTxn));
            removeFileAndOrLog(ff, iFile(path.trimTo(plen), columnName, columnNameTxn));
            removeFileAndOrLog(ff, keyFileName(path.trimTo(plen), columnName, columnNameTxn));
            removeFileAndOrLog(ff, valueFileName(path.trimTo(plen), columnName, columnNameTxn));
            path.trimTo(rootLen);
        } else {
            LOG.critical()
                    .$("o3 ignoring removal of column in read-only partition [table=").utf8(tableToken.getTableName())
                    .$(", columnName=").utf8(columnName)
                    .$(", timestamp=").$ts(partitionTimestamp)
                    .$();
        }
    }

    private int removeColumnFromMeta(int index) {
        try {
            int metaSwapIndex = openMetaSwapFile(ff, ddlMem, path, rootLen, fileOperationRetryCount);
            int timestampIndex = metaMem.getInt(META_OFFSET_TIMESTAMP_INDEX);
            ddlMem.putInt(columnCount);
            ddlMem.putInt(partitionBy);

            if (timestampIndex == index) {
                ddlMem.putInt(-1);
            } else {
                ddlMem.putInt(timestampIndex);
            }
            copyVersionAndLagValues();
            ddlMem.jumpTo(META_OFFSET_COLUMN_TYPES);

            for (int i = 0; i < columnCount; i++) {
                writeColumnEntry(i, i == index);
            }

            long nameOffset = getColumnNameOffset(columnCount);
            for (int i = 0; i < columnCount; i++) {
                CharSequence columnName = metaMem.getStr(nameOffset);
                ddlMem.putStr(columnName);
                nameOffset += Vm.getStorageLength(columnName);
            }

            return metaSwapIndex;
        } finally {
            ddlMem.close();
        }
    }

    private void removeIndexFiles(CharSequence columnName, int columnIndex) {
        try {
            for (int i = txWriter.getPartitionCount() - 1; i > -1L; i--) {
                long partitionTimestamp = txWriter.getPartitionTimestamp(i);
                long partitionNameTxn = txWriter.getPartitionNameTxn(i);
                removeIndexFilesInPartition(columnName, columnIndex, partitionTimestamp, partitionNameTxn);
            }
            if (!PartitionBy.isPartitioned(partitionBy)) {
                removeColumnFilesInPartition(columnName, columnIndex, txWriter.getLastPartitionTimestamp(), -1L);
            }
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void removeIndexFilesInPartition(CharSequence columnName, int columnIndex, long partitionTimestamp, long partitionNameTxn) {
        setPathForPartition(path, partitionBy, partitionTimestamp, false);
        txnPartitionConditionally(path, partitionNameTxn);
        int plen = path.length();
        long columnNameTxn = columnVersionWriter.getColumnNameTxn(partitionTimestamp, columnIndex);
        removeFileAndOrLog(ff, keyFileName(path.trimTo(plen), columnName, columnNameTxn));
        removeFileAndOrLog(ff, valueFileName(path.trimTo(plen), columnName, columnNameTxn));
        path.trimTo(rootLen);
    }

    private void removeLastColumn() {
        removeColumn(columnCount - 1);
    }

    private void removeMetaFile() {
        try {
            path.concat(META_FILE_NAME).$();
            if (ff.exists(path) && !ff.remove(path)) {
                // On Windows opened file cannot be removed
                // but can be renamed
                other.concat(META_FILE_NAME).put('.').put(configuration.getMicrosecondClock().getTicks()).$();
                if (ff.rename(path, other) != FILES_RENAME_OK) {
                    LOG.error().$("could not remove [path=").$(path).$(']').$();
                    throw CairoException.critical(ff.errno()).put("Recovery failed. Cannot remove: ").put(path);
                }
            }
        } finally {
            path.trimTo(rootLen);
            other.trimTo(rootLen);
        }
    }

    private void removeNonAttachedPartitions() {
        LOG.info().$("purging non-attached partitions [path=").$(path.$()).I$();
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
        int checkedType = ff.typeDirOrSoftLinkDirNoDots(path, rootLen, pUtf8NameZ, type, fileNameSink);
        if (checkedType != Files.DT_UNKNOWN &&
                !Chars.endsWith(fileNameSink, DETACHED_DIR_MARKER) &&
                !Chars.startsWith(fileNameSink, WAL_NAME_BASE) &&
                !Chars.startsWith(fileNameSink, SEQ_DIR)) {
            ff.unlinkOrRemove(path, checkedType, LOG);
            path.trimTo(rootLen).$();
        }
    }

    private void removePartitionDirsNotAttached(long pUtf8NameZ, int type) {
        // Do not remove detached partitions, they are probably about to be attached
        // Do not remove wal and sequencer directories either
        int checkedType = ff.typeDirOrSoftLinkDirNoDots(path, rootLen, pUtf8NameZ, type, fileNameSink);
        if (checkedType != Files.DT_UNKNOWN &&
                !Chars.endsWith(fileNameSink, DETACHED_DIR_MARKER) &&
                !Chars.startsWith(fileNameSink, WAL_NAME_BASE) &&
                !Chars.startsWith(fileNameSink, SEQ_DIR) &&
                !Chars.startsWith(fileNameSink, SEQ_DIR_DEPRECATED) &&
                !Chars.endsWith(fileNameSink, configuration.getAttachPartitionSuffix())
        ) {
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
                ff.unlinkOrRemove(path, checkedType, LOG);
                path.trimTo(rootLen).$();
            } catch (NumericException ignore) {
                // not a date?
                // ignore exception and leave the directory
                path.trimTo(rootLen);
                path.concat(pUtf8NameZ).$();
                LOG.error().$("invalid partition directory inside table folder: ").utf8(path).$();
            }
        }
    }

    private void removeSymbolMapFilesQuiet(CharSequence name, long columnNamTxn) {
        try {
            removeFileAndOrLog(ff, offsetFileName(path.trimTo(rootLen), name, columnNamTxn));
            removeFileAndOrLog(ff, charFileName(path.trimTo(rootLen), name, columnNamTxn));
            removeFileAndOrLog(ff, keyFileName(path.trimTo(rootLen), name, columnNamTxn));
            removeFileAndOrLog(ff, valueFileName(path.trimTo(rootLen), name, columnNamTxn));
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void removeSymbolMapWriter(int index) {
        MapWriter writer = symbolMapWriters.getAndSetQuick(index, NullMapWriter.INSTANCE);
        if (writer != null && writer != NullMapWriter.INSTANCE) {
            int symColIndex = denseSymbolMapWriters.remove(writer);
            // Shift all subsequent symbol indexes by 1 back
            while (symColIndex < denseSymbolMapWriters.size()) {
                MapWriter w = denseSymbolMapWriters.getQuick(symColIndex);
                w.setSymbolIndexInTxWriter(symColIndex);
                symColIndex++;
            }
            Misc.freeIfCloseable(writer);
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
                    LOG.info().$("could not remove target of rename '").$(path).$("' to '").$(other).$(" [errno=").$(ff.errno()).I$();
                    index++;
                    continue;
                }

                if (ff.rename(path, other) != FILES_RENAME_OK) {
                    LOG.info().$("could not rename '").$(path).$("' to '").$(other).$(" [errno=").$(ff.errno()).I$();
                    index++;
                    continue;
                }

                return index;

            } while (index < retries);

            throw CairoException.critical(0).put("could not rename ").put(path).put(". Max number of attempts reached [").put(index).put("]. Last target was: ").put(other);
        } finally {
            path.trimTo(rootLen);
            other.trimTo(rootLen);
        }
    }

    private void renameColumnFiles(CharSequence columnName, int columnIndex, CharSequence newName, int columnType) {
        try {
            for (int i = txWriter.getPartitionCount() - 1; i > -1L; i--) {
                long partitionTimestamp = txWriter.getPartitionTimestamp(i);
                long partitionNameTxn = txWriter.getPartitionNameTxn(i);
                renameColumnFiles(columnName, columnIndex, newName, partitionTimestamp, partitionNameTxn);
            }
            if (!PartitionBy.isPartitioned(partitionBy)) {
                renameColumnFiles(columnName, columnIndex, newName, txWriter.getLastPartitionTimestamp(), -1L);
            }

            long columnNameTxn = columnVersionWriter.getDefaultColumnNameTxn(columnIndex);
            if (ColumnType.isSymbol(columnType)) {
                renameFileOrLog(ff, offsetFileName(path.trimTo(rootLen), columnName, columnNameTxn), offsetFileName(other.trimTo(rootLen), newName, columnNameTxn));
                renameFileOrLog(ff, charFileName(path.trimTo(rootLen), columnName, columnNameTxn), charFileName(other.trimTo(rootLen), newName, columnNameTxn));
                renameFileOrLog(ff, keyFileName(path.trimTo(rootLen), columnName, columnNameTxn), keyFileName(other.trimTo(rootLen), newName, columnNameTxn));
                renameFileOrLog(ff, valueFileName(path.trimTo(rootLen), columnName, columnNameTxn), valueFileName(other.trimTo(rootLen), newName, columnNameTxn));
            }
        } finally {
            path.trimTo(rootLen);
            other.trimTo(rootLen);
        }
    }

    private void renameColumnFiles(CharSequence columnName, int columnIndex, CharSequence newName, long partitionTimestamp, long partitionNameTxn) {
        setPathForPartition(path, partitionBy, partitionTimestamp, false);
        setPathForPartition(other, partitionBy, partitionTimestamp, false);
        txnPartitionConditionally(path, partitionNameTxn);
        txnPartitionConditionally(other, partitionNameTxn);
        int plen = path.length();
        long columnNameTxn = columnVersionWriter.getColumnNameTxn(partitionTimestamp, columnIndex);
        renameFileOrLog(ff, dFile(path.trimTo(plen), columnName, columnNameTxn), dFile(other.trimTo(plen), newName, columnNameTxn));
        renameFileOrLog(ff, iFile(path.trimTo(plen), columnName, columnNameTxn), iFile(other.trimTo(plen), newName, columnNameTxn));
        renameFileOrLog(ff, keyFileName(path.trimTo(plen), columnName, columnNameTxn), keyFileName(other.trimTo(plen), newName, columnNameTxn));
        renameFileOrLog(ff, valueFileName(path.trimTo(plen), columnName, columnNameTxn), valueFileName(other.trimTo(plen), newName, columnNameTxn));
        path.trimTo(rootLen);
        other.trimTo(rootLen);
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
                writeColumnEntry(i, false);
            }

            long nameOffset = getColumnNameOffset(columnCount);
            for (int i = 0; i < columnCount; i++) {
                CharSequence columnName = metaMem.getStr(nameOffset);
                nameOffset += Vm.getStorageLength(columnName);

                if (i == index && getColumnType(metaMem, i) > 0) {
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
                            if (ff.rename(other, path) != FILES_RENAME_OK) {
                                LOG.error().$("could not rename [from=").$(other).$(", to=").$(path).I$();
                                throw new CairoError("could not restore directory, see log for details");
                            } else {
                                LOG.info().$("restored [path=").$(path).I$();
                            }
                        } else {
                            LOG.debug().$("missing partition [name=").$(path.trimTo(p).$()).I$();
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
                            if (ff.rename(other, path) != FILES_RENAME_OK) {
                                LOG.error().$("could not rename [from=").$(other).$(", to=").$(path).I$();
                                throw new CairoError("could not restore directory, see log for details");
                            } else {
                                LOG.info().$("restored [path=").$(path).I$();
                            }
                        } else {
                            LOG.error().$("last partition does not exist [name=").$(path).I$();
                            // ok, create last partition we discovered the active
                            // 1. read its size
                            path.trimTo(rootLen);
                            setStateForTimestamp(path, lastTimestamp, false);
                            int p = path.length();
                            transientRowCount = txWriter.getPartitionSizeByPartitionTimestamp(lastTimestamp);


                            // 2. read max timestamp
                            TableUtils.dFile(path.trimTo(p), metadata.getColumnName(metadata.getTimestampIndex()), COLUMN_NAME_TXN_NONE);
                            maxTimestamp = TableUtils.readLongAtOffset(ff, path, tempMem16b, (transientRowCount - 1) * Long.BYTES);
                            fixedRowCount -= transientRowCount;
                            txWriter.removeAttachedPartitions(txWriter.getMaxTimestamp());
                            LOG.info()
                                    .$("updated active partition [name=").$(path.trimTo(p).$())
                                    .$(", maxTimestamp=").$ts(maxTimestamp)
                                    .$(", transientRowCount=").$(transientRowCount)
                                    .$(", fixedRowCount=").$(txWriter.getFixedRowCount())
                                    .I$();
                        }
                    }
                }
            } finally {
                path.trimTo(rootLen);
            }

            final long expectedSize = txWriter.unsafeReadFixedRowCount();
            if (expectedSize != fixedRowCount || maxTimestamp != this.txWriter.getMaxTimestamp()) {
                LOG.info()
                        .$("actual table size has been adjusted [name=`").utf8(tableToken.getTableName()).$('`')
                        .$(", expectedFixedSize=").$(expectedSize)
                        .$(", actualFixedSize=").$(fixedRowCount)
                        .I$();

                txWriter.reset(
                        fixedRowCount,
                        transientRowCount,
                        maxTimestamp,
                        defaultCommitMode,
                        denseSymbolMapWriters
                );
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
                    throw CairoException.critical(ff.errno()).put("Repair failed. Cannot replace ").put(other);
                }

                if (ff.rename(path, other) != FILES_RENAME_OK) {
                    throw CairoException.critical(ff.errno()).put("Repair failed. Cannot rename ").put(path).put(" -> ").put(other);
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
        txWriter.truncate(columnVersionWriter.getVersion());
        clearTodoLog();
    }

    private void resizeColumnTopSink(long o3TimestampMin, long o3TimestampMax) {
        long maxPartitionsAffected = (o3TimestampMax - o3TimestampMin) / PartitionBy.getPartitionTimeIntervalFloor(partitionBy) + 2;
        long size = maxPartitionsAffected * (metadata.getColumnCount() + 1);
        if (o3ColumnTopSink == null) {
            o3ColumnTopSink = new DirectLongList(size, MemoryTag.NATIVE_O3);
        }
        o3ColumnTopSink.setCapacity(size);
        o3ColumnTopSink.setPos(size);
        o3ColumnTopSink.zero(-1L);
    }

    private void resizePartitionUpdateSink(long o3TimestampMin, long o3TimestampMax) {
        int maxPartitionsAffected = (int) ((o3TimestampMax - o3TimestampMin) / PartitionBy.getPartitionTimeIntervalFloor(partitionBy) + 2);
        int size = maxPartitionsAffected * PARTITION_UPDATE_SINK_ENTRY_SIZE;
        if (o3PartitionUpdateSink == null) {
            o3PartitionUpdateSink = new DirectLongList(size, MemoryTag.NATIVE_O3);
        }
        o3PartitionUpdateSink.setCapacity(size);
        o3PartitionUpdateSink.setPos(size);
        o3PartitionUpdateSink.zero(-1);
        o3PartitionUpdateSink.set(0, partitionFloorMethod.floor(o3TimestampMin));
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
            int fd = indexer.getFd();
            if (fd > -1) {
                LOG.info().$("recovering index [fd=").$(fd).I$();
                indexer.rollback(maxRow);
            }
        }
    }

    private void rollbackSymbolTables() {
        int expectedMapWriters = txWriter.unsafeReadSymbolColumnCount();
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

    private void runFragile(FragileCode fragile, CharSequence columnName, CairoException e) {
        try {
            fragile.run(columnName);
        } catch (CairoException e2) {
            LOG.error().$("DOUBLE ERROR: 1st: {").$((Sinkable) e).$('}').$();
            throwDistressException(e2);
        }
        throw e;
    }

    private void safeDeletePartitionDir(long timestamp, long partitionNameTxn) {
        // Call O3 methods to remove check TxnScoreboard and remove partition directly
        partitionRemoveCandidates.clear();
        partitionRemoveCandidates.add(timestamp, partitionNameTxn);
        processPartitionRemoveCandidates();
    }

    private void setAppendPosition(final long position, boolean doubleAllocate) {
        for (int i = 0; i < columnCount; i++) {
            // stop calculating oversize as soon as we find first over-sized column
            setColumnSize(i, position, doubleAllocate);
        }
    }

    private void setColumnSize(int columnIndex, long size, boolean doubleAllocate) {
        try {
            MemoryMA mem1 = getPrimaryColumn(columnIndex);
            MemoryMA mem2 = getSecondaryColumn(columnIndex);
            int type = metadata.getColumnType(columnIndex);
            if (type > 0) { // Not deleted
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
                            // Jump to the number of records written to read length of var column correctly
                            mem2.jumpTo(pos * Long.BYTES);
                            m1pos = Unsafe.getUnsafe().getLong(mem2.getAppendAddress());
                            // Jump to the end of file to correctly trim the file
                            mem2.jumpTo((pos + 1) * Long.BYTES);
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
        } catch (CairoException e) {
            throwDistressException(e);
        }
    }

    private void setO3AppendPosition(final long position) {
        for (int i = 0; i < columnCount; i++) {
            int columnType = metadata.getColumnType(i);
            if (columnType > 0) {
                o3SetAppendOffset(i, columnType, position);
            }
        }
    }

    private void setRowValueNotNull(int columnIndex) {
        assert rowValueIsNotNull.getQuick(columnIndex) != masterRef;
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
        // When partition is create a txn name must always be set to purge dropped partitions.
        // When partition is created outside O3 merge use `txn-1` as the version
        long partitionTxnName = PartitionBy.isPartitioned(partitionBy) ? txWriter.getTxn() - 1 : -1;
        TableUtils.txnPartitionConditionally(
                path,
                txWriter.getPartitionNameTxnByPartitionTimestamp(partitionTimestampHi, partitionTxnName)
        );
        if (updatePartitionInterval) {
            this.partitionTimestampHi = partitionTimestampHi;
        }
    }

    private void swapMetaFile(CharSequence columnName) {
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
        } catch (CairoException e) {
            throwDistressException(e);
        }
        bumpStructureVersion();
    }

    private void swapO3ColumnsExcept(int timestampIndex) {
        ObjList<MemoryCARW> temp = o3MemColumns;
        o3MemColumns = o3MemColumns2;
        o3MemColumns2 = temp;

        // Swap timestamp column back, timestamp column is not sorted, it's the sort key.
        final int timestampMemoryIndex = getPrimaryColumnIndex(timestampIndex);
        o3MemColumns2.setQuick(
                timestampMemoryIndex,
                o3MemColumns.getAndSetQuick(timestampMemoryIndex, o3MemColumns2.getQuick(timestampMemoryIndex))
        );
        o3Columns = o3MemColumns;
        activeColumns = o3MemColumns;

        ObjList<Runnable> tempNullSetters = o3NullSetters;
        o3NullSetters = o3NullSetters2;
        o3NullSetters2 = tempNullSetters;
        activeNullSetters = o3NullSetters;
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
            final MemoryMA m2 = columns.getQuick(i * 2 + 1);
            if (m2 != null) {
                m2.sync(false);
            }
        }
    }

    private void throwDistressException(CairoException cause) {
        LOG.critical().$("writer error [table=").utf8(tableToken.getTableName()).$(", e=").$((Sinkable) cause).I$();
        distressed = true;
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

        LOG.info().$("parallel indexing [table=").utf8(tableToken.getTableName())
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
                    Os.pause();
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

        LOG.info().$("parallel indexing done [serialCount=").$(serialIndexCount).I$();
    }

    private void updateIndexesSerially(long lo, long hi) {
        LOG.info().$("serial indexing [table=").utf8(tableToken.getTableName())
                .$(", indexCount=").$(indexCount)
                .$(", rowCount=").$(hi - lo)
                .I$();
        for (int i = 0, n = denseIndexers.size(); i < n; i++) {
            try {
                denseIndexers.getQuick(i).refreshSourceAndIndex(lo, hi);
            } catch (CairoException e) {
                // this is pretty severe, we hit some sort of limit
                throwDistressException(e);
            }
        }
        LOG.info().$("serial indexing done [table=").utf8(tableToken.getTableName()).I$();
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

    private void updateMetaStructureVersion() {
        try {
            copyMetadataAndUpdateVersion();
            finishMetaSwapUpdate();
            clearTodoLog();
        } finally {
            ddlMem.close();
        }
    }

    private void updateO3ColumnTops() {
        int columnCount = metadata.getColumnCount();
        int increment = columnCount + 1;

        for (int partitionOffset = 0, n = (int) o3ColumnTopSink.size(); partitionOffset < n; partitionOffset += increment) {
            long partitionTimestamp = o3ColumnTopSink.get(partitionOffset);
            if (partitionTimestamp > -1) {
                for (int column = 0; column < columnCount; column++) {
                    long colTop = o3ColumnTopSink.get(partitionOffset + column + 1);
                    if (colTop > -1L) {
                        // Upsert even when colTop value is 0.
                        // TableReader uses the record to determine if the column is supposed to be present for the partition.
                        columnVersionWriter.upsertColumnTop(partitionTimestamp, column, colTop);
                    }
                }
            }
        }
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
                validateMeta(metaMem, validationMap, ColumnType.VERSION);
            } finally {
                metaMem.close();
                path.trimTo(rootLen);
            }
        } catch (CairoException e) {
            runFragile(RECOVER_FROM_META_RENAME_FAILURE, columnName, e);
        }
    }

    private void writeColumnEntry(int i, boolean markDeleted) {
        int columnType = getColumnType(metaMem, i);
        // When column is deleted it's written to metadata with negative type
        if (markDeleted) {
            columnType = -Math.abs(columnType);
        }
        ddlMem.putInt(columnType);

        long flags = 0;
        if (isColumnIndexed(metaMem, i)) {
            flags |= META_FLAG_BIT_INDEXED;
        }

        if (isSequential(metaMem, i)) {
            flags |= META_FLAG_BIT_SEQUENTIAL;
        }
        ddlMem.putLong(flags);
        ddlMem.putInt(getIndexBlockCapacity(metaMem, i));
        ddlMem.skip(16);
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

    static void indexAndCountDown(ColumnIndexer indexer, long lo, long hi, SOCountDownLatch latch) {
        try {
            indexer.refreshSourceAndIndex(lo, hi);
        } catch (CairoException e) {
            indexer.distress();
            LOG.critical().$("index error [fd=").$(indexer.getFd()).$(']').$('{').$((Sinkable) e).$('}').$();
        } finally {
            latch.countDown();
        }
    }

    void closeActivePartition(boolean truncate) {
        LOG.info().$("closing last partition [table=").utf8(tableToken.getTableName()).I$();
        closeAppendMemoryTruncate(truncate);
        freeIndexers();
    }

    void closeActivePartition(long size) {
        for (int i = 0; i < columnCount; i++) {
            // stop calculating oversize as soon as we find first over-sized column
            setColumnSize(i, size, false);
            Misc.free(getPrimaryColumn(i));
            Misc.free(getSecondaryColumn(i));
        }
        Misc.freeObjList(denseIndexers);
        denseIndexers.clear();
    }

    BitmapIndexWriter getBitmapIndexWriter(int columnIndex) {
        return indexers.getQuick(columnIndex).getWriter();
    }

    long getColumnTop(int columnIndex) {
        return columnTops.getQuick(columnIndex);
    }

    ColumnVersionReader getColumnVersionReader() {
        return columnVersionWriter;
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

    long getPartitionNameTxnByIndex(int index) {
        return txWriter.getPartitionNameTxnByIndex(index);
    }

    long getPartitionSizeByIndex(int index) {
        return txWriter.getPartitionSizeByIndex(index);
    }

    TxReader getTxReader() {
        return txWriter;
    }

    boolean isSymbolMapWriterCached(int columnIndex) {
        return symbolMapWriters.getQuick(columnIndex).isCached();
    }

    void o3ClockDownPartitionUpdateCount() {
        o3PartitionUpdRemaining.decrementAndGet();
    }

    void o3CountDownDoneLatch() {
        o3DoneLatch.countDown();
    }

    void o3NotifyPartitionUpdate(
            long timestampMin,
            long timestampMax,
            long partitionTimestamp,
            long srcOooPartitionLo,
            long srcOooPartitionHi,
            boolean partitionMutates,
            long srcOooMax,
            long srcDataMax
    ) {
        long basePartitionTs = o3PartitionUpdateSink.get(0);
        int partitionSinkIndex = (int) ((partitionTimestamp - basePartitionTs) / PartitionBy.getPartitionTimeIntervalFloor(partitionBy));
        int offset = partitionSinkIndex * PARTITION_UPDATE_SINK_ENTRY_SIZE;

        o3PartitionUpdateSink.set(offset, partitionTimestamp);
        o3PartitionUpdateSink.set(offset + 1, timestampMin);
        o3PartitionUpdateSink.set(offset + 2, timestampMax);
        o3PartitionUpdateSink.set(offset + 3, srcOooPartitionLo);
        o3PartitionUpdateSink.set(offset + 4, srcOooPartitionHi);
        o3PartitionUpdateSink.set(offset + 5, partitionMutates ? 1 : 0);
        o3PartitionUpdateSink.set(offset + 6, srcOooMax);
        o3PartitionUpdateSink.set(offset + 7, srcDataMax);

        o3ClockDownPartitionUpdateCount();
    }

    boolean preferDirectIO() {
        return directIOFlag;
    }

    void purgeUnusedPartitions() {
        if (PartitionBy.isPartitioned(partitionBy)) {
            removeNonAttachedPartitions();
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
            rowValueIsNotNull.fill(0, columnCount, masterRef);
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
                            throw CairoException.critical(errno).put("Cannot remove directory: ").put(path);
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
                    rowAction = ROW_ACTION_OPEN_PARTITION;
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

    @FunctionalInterface
    public interface ExtensionListener {
        void onTableExtended(long timestamp);
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
                long row1Count,
                long row2CountLo,
                long row2CountHi
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

        default void putDate(int columnIndex, long value) {
            putLong(columnIndex, value);
        }

        void putDouble(int columnIndex, double value);

        void putFloat(int columnIndex, float value);

        void putGeoHash(int columnIndex, long value);

        void putGeoHashDeg(int index, double lat, double lon);

        void putGeoStr(int columnIndex, CharSequence value);

        void putInt(int columnIndex, int value);

        void putLong(int columnIndex, long value);

        void putLong128(int columnIndex, long lo, long hi);

        void putLong256(int columnIndex, long l0, long l1, long l2, long l3);

        void putLong256(int columnIndex, Long256 value);

        void putLong256(int columnIndex, CharSequence hexString);

        void putLong256(int columnIndex, @NotNull CharSequence hexString, int start, int end);

        void putShort(int columnIndex, short value);

        void putStr(int columnIndex, CharSequence value);

        void putStr(int columnIndex, char value);

        void putStr(int columnIndex, CharSequence value, int pos, int len);

        /**
         * Writes UTF8-encoded string to WAL. As the name of the function suggest the storage format is
         * expected to be UTF16. The function must re-encode string from UTF8 to UTF16 before storing.
         *
         * @param columnIndex      index of the column we are writing to
         * @param value            UTF8 bytes represented as CharSequence interface.
         *                         On this interface getChar() returns a byte, not complete character.
         * @param hasNonAsciiChars helper flag to indicate implementation if all bytes can be assumed as ASCII.
         *                         "true" here indicates that UTF8 decoding is compulsory.
         */
        void putStrUtf8AsUtf16(int columnIndex, DirectByteCharSequence value, boolean hasNonAsciiChars);

        void putSym(int columnIndex, CharSequence value);

        void putSym(int columnIndex, char value);

        default void putSymIndex(int columnIndex, int key) {
            putInt(columnIndex, key);
        }

        default void putSymUtf8(int columnIndex, DirectByteCharSequence value, boolean hasNonAsciiChars) {
            throw new UnsupportedOperationException();
        }

        default void putTimestamp(int columnIndex, long value) {
            putLong(columnIndex, value);
        }

        void putUuid(int columnIndex, CharSequence uuid);
    }

    private static class NoOpRow implements Row {
        @Override
        public void append() {
            // no-op
        }

        @Override
        public void cancel() {
            // no-op
        }

        @Override
        public void putBin(int columnIndex, long address, long len) {
            // no-op
        }

        @Override
        public void putBin(int columnIndex, BinarySequence sequence) {
            // no-op
        }

        @Override
        public void putBool(int columnIndex, boolean value) {
            // no-op
        }

        @Override
        public void putByte(int columnIndex, byte value) {
            // no-op
        }

        @Override
        public void putChar(int columnIndex, char value) {
            // no-op
        }

        @Override
        public void putDate(int columnIndex, long value) {
            // no-op
        }

        @Override
        public void putDouble(int columnIndex, double value) {
            // no-op
        }

        @Override
        public void putFloat(int columnIndex, float value) {
            // no-op
        }

        @Override
        public void putGeoHash(int columnIndex, long value) {
            // no-op
        }

        @Override
        public void putGeoHashDeg(int index, double lat, double lon) {
            // no-op
        }

        @Override
        public void putGeoStr(int columnIndex, CharSequence value) {

        }

        @Override
        public void putInt(int columnIndex, int value) {
            // no-op
        }

        @Override
        public void putLong(int columnIndex, long value) {
            // no-op
        }

        @Override
        public void putLong128(int columnIndex, long lo, long hi) {
            // no-op
        }

        @Override
        public void putLong256(int columnIndex, long l0, long l1, long l2, long l3) {
            // no-op
        }

        @Override
        public void putLong256(int columnIndex, Long256 value) {
            // no-op
        }

        @Override
        public void putLong256(int columnIndex, CharSequence hexString) {
            // no-op
        }

        @Override
        public void putLong256(int columnIndex, @NotNull CharSequence hexString, int start, int end) {
            // no-op
        }

        @Override
        public void putShort(int columnIndex, short value) {
            // no-op
        }

        @Override
        public void putStr(int columnIndex, CharSequence value) {
            // no-op
        }

        @Override
        public void putStr(int columnIndex, char value) {
            // no-op
        }

        @Override
        public void putStr(int columnIndex, CharSequence value, int pos, int len) {
            // no-op
        }

        @Override
        public void putStrUtf8AsUtf16(int columnIndex, DirectByteCharSequence value, boolean hasNonAsciiChars) {
            // no-op
        }

        @Override
        public void putSym(int columnIndex, CharSequence value) {
            // no-op
        }

        @Override
        public void putSym(int columnIndex, char value) {
            // no-op
        }

        @Override
        public void putSymIndex(int columnIndex, int key) {
            // no-op
        }

        @Override
        public void putSymUtf8(int columnIndex, DirectByteCharSequence value, boolean hasNonAsciiChars) {
            // no-op
        }

        @Override
        public void putTimestamp(int columnIndex, long value) {
            // no-op
        }

        @Override
        public void putUuid(int columnIndex, CharSequence uuid) {

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
            WriterRowUtils.putGeoHash(index, value, type, this);
        }

        @Override
        public void putGeoHashDeg(int index, double lat, double lon) {
            int type = metadata.getColumnType(index);
            WriterRowUtils.putGeoHash(index, GeoHashes.fromCoordinatesDegUnsafe(lat, lon, ColumnType.getGeoHashBits(type)), type, this);
        }

        @Override
        public void putGeoStr(int index, CharSequence hash) {
            final int type = metadata.getColumnType(index);
            WriterRowUtils.putGeoStr(index, hash, type, this);
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
        public void putLong128(int columnIndex, long lo, long hi) {
            MemoryA primaryColumn = getPrimaryColumn(columnIndex);
            primaryColumn.putLong(lo);
            primaryColumn.putLong(hi);
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
        public void putStrUtf8AsUtf16(int columnIndex, DirectByteCharSequence value, boolean hasNonAsciiChars) {
            getSecondaryColumn(columnIndex).putLong(getPrimaryColumn(columnIndex).putStrUtf8AsUtf16(value, hasNonAsciiChars));
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
        public void putUuid(int columnIndex, CharSequence uuidStr) {
            SqlUtil.implicitCastStrAsUuid(uuidStr, uuid);
            putLong128(columnIndex, uuid.getLo(), uuid.getHi());
        }

        private MemoryA getPrimaryColumn(int columnIndex) {
            return activeColumns.getQuick(getPrimaryColumnIndex(columnIndex));
        }

        private MemoryA getSecondaryColumn(int columnIndex) {
            return activeColumns.getQuick(getSecondaryColumnIndex(columnIndex));
        }
    }
}
