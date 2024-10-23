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
import io.questdb.Metrics;
import io.questdb.cairo.frm.Frame;
import io.questdb.cairo.frm.FrameAlgebra;
import io.questdb.cairo.frm.file.FrameFactory;
import io.questdb.cairo.sql.AsyncWriterCommand;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.vm.NullMapWriter;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.cairo.vm.api.MemoryCMOR;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.cairo.vm.api.MemoryMAR;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cairo.vm.api.MemoryMAT;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.cairo.vm.api.NullMemory;
import io.questdb.cairo.wal.MetadataService;
import io.questdb.cairo.wal.O3JobParallelismRegulator;
import io.questdb.cairo.wal.SymbolMapDiff;
import io.questdb.cairo.wal.SymbolMapDiffCursor;
import io.questdb.cairo.wal.SymbolMapDiffEntry;
import io.questdb.cairo.wal.WalTxnDetails;
import io.questdb.cairo.wal.WriterRowUtils;
import io.questdb.cairo.wal.seq.TableSequencer;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.griffin.ConvertOperatorImpl;
import io.questdb.griffin.DropIndexOperator;
import io.questdb.griffin.PurgingOperator;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.UpdateOperatorImpl;
import io.questdb.griffin.engine.ops.AbstractOperation;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.griffin.engine.table.parquet.MappedMemoryPartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.ParquetCompression;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.mp.Sequence;
import io.questdb.std.BinarySequence;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FindVisitor;
import io.questdb.std.IntList;
import io.questdb.std.Long256;
import io.questdb.std.LongList;
import io.questdb.std.LongObjHashMap;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectFactory;
import io.questdb.std.ObjectPool;
import io.questdb.std.Os;
import io.questdb.std.PagedDirectLongList;
import io.questdb.std.ReadOnlyObjList;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.Uuid;
import io.questdb.std.Vect;
import io.questdb.std.WeakClosableObjectPool;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.tasks.ColumnIndexerTask;
import io.questdb.tasks.ColumnTask;
import io.questdb.tasks.O3CopyTask;
import io.questdb.tasks.O3OpenColumnTask;
import io.questdb.tasks.O3PartitionTask;
import io.questdb.tasks.TableWriterTask;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;

import static io.questdb.cairo.BitmapIndexUtils.keyFileName;
import static io.questdb.cairo.BitmapIndexUtils.valueFileName;
import static io.questdb.cairo.SymbolMapWriter.HEADER_SIZE;
import static io.questdb.cairo.TableUtils.*;
import static io.questdb.cairo.sql.AsyncWriterCommand.Error.*;
import static io.questdb.std.Files.FILES_RENAME_OK;
import static io.questdb.std.Files.PAGE_SIZE;
import static io.questdb.tasks.TableWriterTask.*;

public class TableWriter implements TableWriterAPI, MetadataService, Closeable {
    public static final int O3_BLOCK_DATA = 2;
    public static final int O3_BLOCK_MERGE = 3;
    public static final int O3_BLOCK_NONE = -1;
    public static final int O3_BLOCK_O3 = 1;
    // Oversized partitionUpdateSink (offset, description):
    // 0, partitionTimestamp
    // 1, timestampMin
    // 2, newPartitionSize
    // 3, oldPartitionSize
    // 4, flags (partitionMutates INT, isLastWrittenPartition INT)
    // 5. o3SplitPartitionSize size of "split" partition, new partition that branches out of the old one
    // 6. original partition timestamp (before the split)
    // 7. parquet partition file size
    // ... column top for every column
    public static final int PARTITION_SINK_SIZE_LONGS = 8;
    public static final int PARTITION_SINK_COL_TOP_OFFSET = PARTITION_SINK_SIZE_LONGS * Long.BYTES;
    public static final int TIMESTAMP_MERGE_ENTRY_BYTES = Long.BYTES * 2;
    private static final ObjectFactory<MemoryCMOR> GET_MEMORY_CMOR = Vm::getMemoryCMOR;
    private static final long IGNORE = -1L;
    private static final Log LOG = LogFactory.getLog(TableWriter.class);
    /*
        The most recent logical partition is allowed to have up to cairo.o3.last.partition.max.splits (20 by default) splits.
        Any other partition is allowed to have 0 splits (1 partition in total).
     */
    private static final int MAX_MID_SUB_PARTITION_COUNT = 1;
    private static final Runnable NOOP = () -> {
    };
    private static final Row NOOP_ROW = new NoOpRow();
    private static final int O3_ERRNO_FATAL = Integer.MAX_VALUE - 1;
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
    private final DatabaseCheckpointStatus checkpointStatus;
    private final ColumnVersionWriter columnVersionWriter;
    private final MPSequence commandPubSeq;
    private final RingQueue<TableWriterTask> commandQueue;
    private final SCSequence commandSubSeq;
    private final CairoConfiguration configuration;
    private final long dataAppendPageSize;
    private final DdlListener ddlListener;
    private final MemoryMAR ddlMem;
    private final ObjList<ColumnIndexer> denseIndexers = new ObjList<>();
    private final ObjList<MapWriter> denseSymbolMapWriters;
    private final int detachedMkDirMode;
    private final CairoEngine engine;
    private final FilesFacade ff;
    private final int fileOperationRetryCount;
    private final FrameFactory frameFactory;
    private final SOCountDownLatch indexLatch = new SOCountDownLatch();
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
    private final boolean mixedIOFlag;
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
    private final DateFormat partitionDirFmt;
    private final LongList partitionRemoveCandidates = new LongList();
    private final Path path;
    private final int pathRootSize;
    private final int pathSize;
    private final FragileCode RECOVER_FROM_META_RENAME_FAILURE = this::recoverFromMetaRenameFailure;
    private final AtomicLong physicallyWrittenRowsSinceLastCommit = new AtomicLong();
    private final Row row = new RowImpl();
    private final LongList rowValueIsNotNull = new LongList();
    private final TxReader slaveTxReader;
    private final ObjList<MapWriter> symbolMapWriters;
    private final IntList symbolRewriteMap = new IntList();
    private final MemoryMARW todoMem = Vm.getMARWInstance();
    private final TxWriter txWriter;
    private final TxnScoreboard txnScoreboard;
    private final Utf8StringSink utf8Sink = new Utf8StringSink();
    private final FindVisitor removePartitionDirsNotAttached = this::removePartitionDirsNotAttached;
    private final Uuid uuid = new Uuid();
    private final LowerCaseCharSequenceIntHashMap validationMap = new LowerCaseCharSequenceIntHashMap();
    private final WeakClosableObjectPool<MemoryCMOR> walColumnMemoryPool;
    private final LongObjHashMap<LongList> walFdCache = new LongObjHashMap<>();
    private final WeakClosableObjectPool<LongList> walFdCacheListPool = new WeakClosableObjectPool<>(LongList::new, 5, true);
    private final LongObjHashMap.LongObjConsumer<LongList> walFdCloseCachedFdAction;
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
    private long avgRecordSize;
    private boolean avoidIndexOnCommit = false;
    private int columnCount;
    private long committedMasterRef;
    private ConvertOperatorImpl convertOperatorImpl;
    private DedupColumnCommitAddresses dedupColumnCommitAddresses;
    private String designatedTimestampColumnName;
    private boolean distressed = false;
    private DropIndexOperator dropIndexOperator;
    private int indexCount;
    private int lastErrno;
    private boolean lastOpenPartitionIsReadOnly;
    private long lastOpenPartitionTs = Long.MIN_VALUE;
    private long lastPartitionTimestamp;
    private LifecycleManager lifecycleManager;
    private long lockFd = -2;
    private long masterRef = 0L;
    private int metaPrevIndex;
    private final FragileCode RECOVER_FROM_TODO_WRITE_FAILURE = this::recoverFromTodoWriteFailure;
    private int metaSwapIndex;
    private long minSplitPartitionTimestamp;
    private long noOpRowCount;
    private ReadOnlyObjList<? extends MemoryCR> o3Columns;
    private long o3CommitBatchTimestampMin = Long.MAX_VALUE;
    private long o3EffectiveLag = 0L;
    private boolean o3InError = false;
    private long o3MasterRef = -1L;
    private ObjList<MemoryCARW> o3MemColumns1;
    private ObjList<MemoryCARW> o3MemColumns2;
    private ObjList<Runnable> o3NullSetters1;
    private ObjList<Runnable> o3NullSetters2;
    private PagedDirectLongList o3PartitionUpdateSink;
    private long o3RowCount;
    private MemoryMAT o3TimestampMem;
    private MemoryARW o3TimestampMemCpy;
    private volatile boolean o3oomObserved;
    private long partitionTimestampHi;
    private boolean performRecovery;
    private boolean processingQueue;
    private PurgingOperator purgingOperator;
    private boolean removeDirOnCancelRow = true;
    private int rowAction = ROW_ACTION_OPEN_PARTITION;
    private TableToken tableToken;
    private final ColumnTaskHandler cthAppendWalColumnToLastPartition = this::cthAppendWalColumnToLastPartition;
    private final ColumnTaskHandler cthO3SortColumnRef = this::cthO3SortColumn;
    private final ColumnTaskHandler cthMergeWalColumnWithLag = this::cthMergeWalColumnWithLag;
    private final ColumnTaskHandler cthO3MoveUncommittedRef = this::cthO3MoveUncommitted;
    private final ColumnTaskHandler cthO3ShiftColumnInLagToTopRef = this::cthO3ShiftColumnInLagToTop;
    private long tempMem16b = Unsafe.malloc(16, MemoryTag.NATIVE_TABLE_WRITER);
    private LongConsumer timestampSetter;
    private long todoTxn;
    private final FragileCode RECOVER_FROM_SYMBOL_MAP_WRITER_FAILURE = this::recoverFromSymbolMapWriterFailure;
    private final FragileCode RECOVER_FROM_SWAP_RENAME_FAILURE = this::recoverFromSwapRenameFailure;
    private final FragileCode RECOVER_FROM_COLUMN_OPEN_FAILURE = this::recoverOpenColumnFailure;
    private UpdateOperatorImpl updateOperatorImpl;
    private int walFdCacheSize;
    private WalTxnDetails walTxnDetails;

    public TableWriter(
            CairoConfiguration configuration,
            TableToken tableToken,
            MessageBus messageBus,
            MessageBus ownMessageBus,
            boolean lock,
            LifecycleManager lifecycleManager,
            CharSequence root,
            DdlListener ddlListener,
            DatabaseCheckpointStatus checkpointStatus,
            Metrics metrics,
            CairoEngine cairoEngine
    ) {
        LOG.info().$("open '").utf8(tableToken.getTableName()).$('\'').$();
        this.configuration = configuration;
        this.ddlListener = ddlListener;
        this.checkpointStatus = checkpointStatus;
        this.frameFactory = new FrameFactory(configuration);
        this.mixedIOFlag = configuration.isWriterMixedIOEnabled();
        this.metrics = metrics;
        this.ownMessageBus = ownMessageBus;
        this.messageBus = ownMessageBus != null ? ownMessageBus : messageBus;
        this.lifecycleManager = lifecycleManager;
        this.parallelIndexerEnabled = configuration.isParallelIndexingEnabled();
        this.ff = configuration.getFilesFacade();
        this.mkDirMode = configuration.getMkDirMode();
        this.detachedMkDirMode = configuration.getDetachedMkDirMode();
        this.fileOperationRetryCount = configuration.getFileOperationRetryCount();
        this.tableToken = tableToken;
        this.o3QuickSortEnabled = configuration.isO3QuickSortEnabled();
        this.engine = cairoEngine;
        try {
            this.path = new Path().of(root);
            this.pathRootSize = path.size();
            path.concat(tableToken);
            this.other = new Path().of(root).concat(tableToken);
            this.pathSize = path.size();
            if (lock) {
                lock();
            } else {
                this.lockFd = -1;
            }
            int todo = readTodo();
            if (todo == TODO_RESTORE_META) {
                repairMetaRename((int) todoMem.getLong(48));
            }
            this.ddlMem = Vm.getMARInstance(configuration.getCommitMode());
            this.metaMem = Vm.getCMRInstance();
            openMetaFile(ff, path, pathSize, metaMem);
            this.metadata = new TableWriterMetadata(this.tableToken, metaMem);
            this.partitionBy = metadata.getPartitionBy();
            this.txWriter = new TxWriter(ff, configuration).ofRW(path.concat(TXN_FILE_NAME).$(), partitionBy);
            this.txnScoreboard = new TxnScoreboard(ff, configuration.getTxnScoreboardEntryCount()).ofRW(path.trimTo(pathSize));
            path.trimTo(pathSize);
            this.columnVersionWriter = openColumnVersionFile(configuration, path, pathSize, partitionBy != PartitionBy.NONE);
            this.o3ColumnOverrides = metadata.isWalEnabled() ? new ObjList<>() : null;

            if (metadata.isWalEnabled()) {
                // O3 columns will be allocated to the size of the transaction, not reason to over allocate.
                this.o3ColumnMemorySize = (int) PAGE_SIZE;
                if (tableToken.isSystem()) {
                    this.dataAppendPageSize = configuration.getSystemDataAppendPageSize();
                } else {
                    this.dataAppendPageSize = configuration.getDataAppendPageSize();
                }
            } else {
                if (tableToken.isSystem()) {
                    this.o3ColumnMemorySize = configuration.getSystemO3ColumnMemorySize();
                    this.dataAppendPageSize = configuration.getSystemDataAppendPageSize();
                } else {
                    this.o3ColumnMemorySize = configuration.getO3ColumnMemorySize();
                    this.dataAppendPageSize = configuration.getDataAppendPageSize();
                }
            }

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
            this.o3MemColumns1 = new ObjList<>(columnCount * 2);
            this.o3MemColumns2 = new ObjList<>(columnCount * 2);
            this.o3Columns = this.o3MemColumns1;
            this.activeColumns = columns;
            this.symbolMapWriters = new ObjList<>(columnCount);
            this.indexers = new ObjList<>(columnCount);
            this.denseSymbolMapWriters = new ObjList<>(metadata.getSymbolMapCount());
            this.nullSetters = new ObjList<>(columnCount);
            this.o3NullSetters1 = new ObjList<>(columnCount);
            this.o3NullSetters2 = new ObjList<>(columnCount);
            this.activeNullSetters = nullSetters;
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
            minSplitPartitionTimestamp = findMinSplitPartitionTimestamp();
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
            o3LastTimestampSpreads = new long[configuration.getO3LagCalculationWindowsSize()];
            Arrays.fill(o3LastTimestampSpreads, 0);

            // Some wal specific initialization
            if (metadata.isWalEnabled()) {
                walColumnMemoryPool = new WeakClosableObjectPool<>(GET_MEMORY_CMOR, configuration.getWalMaxSegmentFileDescriptorsCache(), true);
                walFdCloseCachedFdAction = (key, fdList) -> {
                    for (int i = 0, n = fdList.size(); i < n; i++) {
                        ff.close(fdList.getQuick(i));
                    }
                    fdList.clear();
                    walFdCacheListPool.push(fdList);
                };
            } else {
                walColumnMemoryPool = null;
                walFdCloseCachedFdAction = null;
            }
        } catch (Throwable e) {
            doClose(false);
            throw e;
        }
    }

    // this method is public to allow testing
    public static void consumeColumnTasks0(RingQueue<ColumnTask> queue, int queuedCount, Sequence subSeq, SOUnboundedCountDownLatch o3DoneLatch) {
        while (!o3DoneLatch.done(queuedCount)) {
            long cursor = subSeq.next();
            if (cursor > -1) {
                ColumnTaskJob.processColumnTask(queue.get(cursor), cursor, subSeq);
            } else {
                Os.pause();
            }
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

    @Override
    public void addColumn(@NotNull CharSequence columnName, int columnType, SecurityContext securityContext) {
        addColumn(
                columnName,
                columnType,
                configuration.getDefaultSymbolCapacity(),
                configuration.getDefaultSymbolCacheFlag(),
                false,
                0,
                false,
                false,
                securityContext
        );
    }

    @Override
    public void addColumn(
            CharSequence columnName,
            int columnType,
            int symbolCapacity,
            boolean symbolCacheFlag,
            boolean isIndexed,
            int indexValueBlockCapacity,
            boolean isDedupKey
    ) {
        addColumn(
                columnName,
                columnType,
                symbolCapacity,
                symbolCacheFlag,
                isIndexed,
                indexValueBlockCapacity,
                false,
                isDedupKey,
                null
        );
    }

    @Override
    public void addColumn(
            CharSequence columnName,
            int columnType,
            int symbolCapacity,
            boolean symbolCacheFlag,
            boolean isIndexed,
            int indexValueBlockCapacity,
            boolean isDedupKey,
            SecurityContext securityContext
    ) {
        addColumn(
                columnName,
                columnType,
                symbolCapacity,
                symbolCacheFlag,
                isIndexed,
                indexValueBlockCapacity,
                false,
                isDedupKey,
                securityContext
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
            boolean isSequential,
            boolean isDedupKey,
            SecurityContext securityContext
    ) {
        assert txWriter.getLagRowCount() == 0;
        assert indexValueBlockCapacity == Numbers.ceilPow2(indexValueBlockCapacity) : "power of 2 expected";
        assert symbolCapacity == Numbers.ceilPow2(symbolCapacity) : "power of 2 expected";

        checkDistressed();
        checkColumnName(columnName);

        if (getColumnIndexQuiet(metaMem, columnName, columnCount) != -1) {
            throw CairoException.duplicateColumn(columnName);
        }

        commit();

        long columnNameTxn = getTxn();
        LOG.info().$("adding column '").utf8(columnName).$('[').$(ColumnType.nameOf(columnType)).$("], columnName txn ").$(columnNameTxn).$(" to ").$substr(pathRootSize, path).$();

        addColumnToMeta(columnName, columnType, symbolCapacity, symbolCacheFlag, isIndexed, indexValueBlockCapacity, isSequential, isDedupKey, columnNameTxn, -1);

        // extend columnTop list to make sure row cancel can work
        // need for setting correct top is hard to test without being able to read from table
        int columnIndex = columnCount - 1;

        // Set txn number in the column version file to mark the transaction where the column is added
        columnVersionWriter.upsertDefaultTxnName(columnIndex, columnNameTxn, txWriter.getLastPartitionTimestamp());

        // create column files
        if (txWriter.getTransientRowCount() > 0 || !PartitionBy.isPartitioned(partitionBy)) {
            try {
                openNewColumnFiles(columnName, columnType, isIndexed, indexValueBlockCapacity);
            } catch (CairoException e) {
                runFragile(RECOVER_FROM_COLUMN_OPEN_FAILURE, columnName, e);
            }
        }

        try {
            // open _meta file
            openMetaFile(ff, path, pathSize, metaMem);
            // remove _todo
            clearTodoLog();
        } catch (CairoException e) {
            throwDistressException(e);
        }

        bumpMetadataAndColumnStructureVersion();

        metadata.addColumn(columnName, columnType, isIndexed, indexValueBlockCapacity, columnIndex, isSequential, symbolCapacity, isDedupKey, symbolCacheFlag);

        if (!Os.isWindows()) {
            ff.fsyncAndClose(TableUtils.openRO(ff, path.$(), LOG));
        }

        if (securityContext != null) {
            ddlListener.onColumnAdded(securityContext, tableToken, columnName);
        }

        try (MetadataCacheWriter metadataRW = engine.getMetadataCache().writeLock()) {
            metadataRW.hydrateTable(metadata);
        }
    }

    @Override
    public void addIndex(@NotNull CharSequence columnName, int indexValueBlockSize) {
        assert indexValueBlockSize == Numbers.ceilPow2(indexValueBlockSize) : "power of 2 expected";

        checkDistressed();

        final int columnIndex = getColumnIndexQuiet(metaMem, columnName, columnCount);

        if (columnIndex == -1) {
            throw CairoException.invalidMetadataRecoverable("column does not exist", columnName);
        }

        commit();

        if (isColumnIndexed(metaMem, columnIndex)) {
            throw CairoException.invalidMetadataRecoverable("column is already indexed", columnName);
        }

        final int existingType = getColumnType(metaMem, columnIndex);
        LOG.info().$("adding index to '").utf8(columnName).$("' [").$(ColumnType.nameOf(existingType)).$(", path=").$substr(pathRootSize, path).I$();

        if (!ColumnType.isSymbol(existingType)) {
            LOG.error().$("cannot create index for [column='").utf8(columnName).$(", type=").$(ColumnType.nameOf(existingType)).$(", path=").$substr(pathRootSize, path).I$();
            throw CairoException.invalidMetadataRecoverable("cannot create index, column type is not SYMBOL", columnName);
        }

        final SymbolColumnIndexer indexer = new SymbolColumnIndexer(configuration);
        writeIndex(columnName, indexValueBlockSize, columnIndex, indexer);
        // set index flag in metadata and  create new _meta.swp
        metaSwapIndex = copyMetadataAndSetIndexAttrs(columnIndex, true, indexValueBlockSize);

        swapMetaFile(columnName);

        indexers.extendAndSet(columnIndex, indexer);
        populateDenseIndexerList();

        TableColumnMetadata columnMetadata = metadata.getColumnMetadata(columnIndex);
        columnMetadata.setIndexed(true);
        columnMetadata.setIndexValueBlockCapacity(indexValueBlockSize);

        try (MetadataCacheWriter metadataRW = engine.getMetadataCache().writeLock()) {
            metadataRW.hydrateTable(metadata);
        }
        LOG.info().$("ADDED index to '").utf8(columnName).$('[').$(ColumnType.nameOf(existingType)).$("]' to ").$substr(pathRootSize, path).$();
    }

    public void addPhysicallyWrittenRows(long rows) {
        physicallyWrittenRowsSinceLastCommit.addAndGet(rows);
        metrics.tableWriter().addPhysicallyWrittenRows(rows);
    }

    public long apply(AbstractOperation operation, long seqTxn) {
        try {
            setSeqTxn(seqTxn);
            long txnBefore = getTxn();
            long rowsAffected = operation.apply(this, true);
            if (txnBefore == getTxn()) {
                // Commit to update seqTxn
                txWriter.commit(denseSymbolMapWriters);
            }
            return rowsAffected;
        } catch (CairoException ex) {
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
            LOG.info().$("partition is already attached [path=").$substr(pathRootSize, path).I$();
            // TODO: potentially we can merge with existing data
            return AttachDetachStatus.ATTACH_ERR_PARTITION_EXISTS;
        }

        if (inTransaction()) {
            LOG.info().$("committing open transaction before applying attach partition command [table=").utf8(tableToken.getTableName())
                    .$(", partition=").$ts(timestamp).I$();
            commit();

            // Check that partition we're about to attach hasn't appeared after commit
            if (txWriter.attachedPartitionsContains(timestamp)) {
                LOG.info().$("partition is already attached [path=").$substr(pathRootSize, path).I$();
                return AttachDetachStatus.ATTACH_ERR_PARTITION_EXISTS;
            }
        }

        // final name of partition folder after attach
        setPathForPartition(path.trimTo(pathSize), partitionBy, timestamp, getTxn());
        if (ff.exists(path.$())) {
            // Very unlikely since txn is part of the folder name
            return AttachDetachStatus.ATTACH_ERR_DIR_EXISTS;
        }

        Path detachedPath = Path.PATH.get().of(configuration.getRoot()).concat(tableToken);
        setPathForPartition(detachedPath, partitionBy, timestamp, -1L);
        detachedPath.put(configuration.getAttachPartitionSuffix()).$();
        int detachedRootLen = detachedPath.size();
        boolean forceRenamePartitionDir = partitionSize < 0;

        boolean checkPassed = false;
        boolean isSoftLink;
        try {
            if (ff.exists(detachedPath.$())) {

                isSoftLink = ff.isSoftLink(detachedPath.$()); // returns false regardless in Windows

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
                    if (ff.rename(detachedPath.trimTo(detachedRootLen).$(), path.$()) == FILES_RENAME_OK) {
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
            path.trimTo(pathSize);
            if (!checkPassed) {
                columnVersionWriter.readUnsafe();
            }
        }

        try {
            // find out lo, hi ranges of partition attached as well as size
            assert timestamp <= attachMinTimestamp && attachMinTimestamp <= attachMaxTimestamp;
            long nextMinTimestamp = Math.min(attachMinTimestamp, txWriter.getMinTimestamp());
            long nextMaxTimestamp = Math.max(attachMaxTimestamp, txWriter.getMaxTimestamp());
            boolean appendPartitionAttached = size() == 0 || txWriter.getNextPartitionTimestamp(nextMaxTimestamp) > txWriter.getNextPartitionTimestamp(txWriter.getMaxTimestamp());

            txWriter.beginPartitionSizeUpdate();
            txWriter.updatePartitionSizeByTimestamp(timestamp, partitionSize, getTxn());
            txWriter.finishPartitionSizeUpdate(nextMinTimestamp, nextMaxTimestamp);
            if (isSoftLink) {
                txWriter.setPartitionReadOnlyByTimestamp(timestamp, true);
            }
            txWriter.bumpTruncateVersion();

            columnVersionWriter.commit();
            txWriter.setColumnVersion(columnVersionWriter.getVersion());
            txWriter.commit(denseSymbolMapWriters);

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
            TableWriterMetadata.WriterTableColumnMetadata columnMetadata = (TableWriterMetadata.WriterTableColumnMetadata) metadata.getColumnMetadata(columnIndex);
            columnMetadata.setSymbolCached(cache);
            updateMetaStructureVersion();
            txWriter.bumpTruncateVersion();
            try (MetadataCacheWriter metadataRW = engine.getMetadataCache().writeLock()) {
                metadataRW.hydrateTable(metadata);
            }
        }
    }

    @Override
    public void changeColumnType(
            CharSequence columnName,
            int newType,
            int symbolCapacity,
            boolean symbolCacheFlag,
            boolean isIndexed,
            int indexValueBlockCapacity,
            boolean isSequential,
            SecurityContext securityContext
    ) {

        int existingColIndex = metadata.getColumnIndexQuiet(columnName);
        if (existingColIndex < 0) {
            throw CairoException.nonCritical().put("cannot change column type, column does not exists [table=")
                    .put(tableToken.getTableName()).put(", column=").put(columnName).put(']');
        }

        if (existingColIndex == metadata.getTimestampIndex()) {
            throw CairoException.nonCritical().put("cannot change column type, column is the designated timestamp [table=")
                    .put(tableToken.getTableName()).put(", column=").put(columnName).put(']');
        }

        int existingType = metadata.getColumnType(existingColIndex);
        assert existingType > 0;

        if (existingType == newType) {
            // It only makes sense to change symbol parameters
            // It has to be another type of ALTER command since it's non-structural change in WAL tables
            throw CairoException.nonCritical().put("cannot change column type, new type is the same as existing [table=")
                    .put(tableToken.getTableName()).put(", column=").put(columnName).put(']');
        }

        ConvertOperatorImpl convertOperator = getConvertOperator();
        try {
            commit();

            LOG.info().$("converting column [table=").$(tableToken).$(", column=").utf8(columnName)
                    .$(", from=").$(ColumnType.nameOf(existingType))
                    .$(", to=").$(ColumnType.nameOf(newType)).I$();

            boolean isDedupKey = metadata.isDedupKey(existingColIndex);
            int columnIndex = columnCount;
            long columnNameTxn = getTxn();

            // Set txn number in the column version file to mark the transaction where the column is added
            long firstPartitionTsm = columnVersionWriter.getColumnTopPartitionTimestamp(existingColIndex);
            if (firstPartitionTsm == Long.MIN_VALUE && txWriter.getPartitionCount() > 0) {
                firstPartitionTsm = txWriter.getPartitionTimestampByIndex(0);
            }
            columnVersionWriter.upsertDefaultTxnName(columnIndex, columnNameTxn, firstPartitionTsm);

            if (ColumnType.isSymbol(newType)) {
                createSymbolMapWriter(columnName, columnNameTxn, symbolCapacity, symbolCacheFlag);
            } else {
                // maintain sparse list of symbol writers
                symbolMapWriters.extendAndSet(columnCount, NullMapWriter.INSTANCE);
            }
            try {
                convertOperator.convertColumn(columnName, existingColIndex, existingType, columnIndex, newType);

                // Column converted, add new one to _meta file and remove the existing column
                addColumnToMeta(columnName, newType, symbolCapacity, symbolCacheFlag, isIndexed, indexValueBlockCapacity, isSequential, isDedupKey, columnNameTxn, existingColIndex);

                // close old column files
                freeColumnMemory(existingColIndex);

                // remove symbol map writer or entry for such
                removeSymbolMapWriter(existingColIndex);

                // remove old column to in-memory metadata object and add new one
                metadata.removeColumn(existingColIndex);
                metadata.addColumn(columnName, newType, isIndexed, indexValueBlockCapacity, existingColIndex, isSequential, symbolCapacity, isDedupKey, existingColIndex + 1, symbolCacheFlag); // by convention, replacingIndex is +1

                // open new column files
                if (txWriter.getTransientRowCount() > 0 || !PartitionBy.isPartitioned(partitionBy)) {
                    long partitionTimestamp = txWriter.getLastPartitionTimestamp();
                    setStateForTimestamp(path, partitionTimestamp);
                    openColumnFiles(columnName, columnNameTxn, columnIndex, path.size());
                    setColumnAppendPosition(columnIndex, txWriter.getTransientRowCount(), false);
                    path.trimTo(pathSize);
                }

                // write index if necessary or remove the old one
                // index must be created before column is initialised because
                // it uses primary column object as temporary tool
                if (isIndexed) {
                    SymbolColumnIndexer indexer = (SymbolColumnIndexer) indexers.get(columnIndex);
                    writeIndex(columnName, indexValueBlockCapacity, columnIndex, indexer);
                    // add / remove indexers
                    indexers.extendAndSet(columnIndex, indexer);
                    populateDenseIndexerList();
                }

                try {
                    // open _meta file
                    openMetaFile(ff, path, pathSize, metaMem);
                    // remove _todo
                    clearTodoLog();
                } catch (CairoException e) {
                    throwDistressException(e);
                }

                // commit transaction to _txn file
                bumpMetadataAndColumnStructureVersion();

                try (MetadataCacheWriter metadataRW = engine.getMetadataCache().writeLock()) {
                    metadataRW.hydrateTable(metadata);
                }
            } finally {
                // clear temp resources
                convertOperator.finishColumnConversion();
                path.trimTo(pathSize);
            }
        } catch (Throwable th) {
            LOG.error().$("could not change column type [table=").$(tableToken.getTableName()).$(", column=").utf8(columnName)
                    .$(", error=").$(th).I$();
            throw th;
        }
    }

    public boolean checkScoreboardHasReadersBeforeLastCommittedTxn() {
        if (checkpointStatus.isInProgress()) {
            // do not alter scoreboard while checkpoint is in progress
            return true;
        }
        long lastCommittedTxn = txWriter.getTxn();
        try {
            if (txnScoreboard.acquireTxn(lastCommittedTxn)) {
                txnScoreboard.releaseTxn(lastCommittedTxn);
            }
        } catch (CairoException ex) {
            // Scoreboard can be over allocated, don't stall writing because of that.
            // Schedule async purge and continue
            LOG.critical().$("cannot lock last txn in scoreboard, partition purge will be scheduled [table=")
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

    public void closeActivePartition(boolean truncate) {
        LOG.debug().$("closing last partition [table=").utf8(tableToken.getTableName()).I$();
        closeAppendMemoryTruncate(truncate);
        freeIndexers();
    }

    @Override
    public long commit() {
        return commit(0);
    }

    public void commitSeqTxn(long seqTxn) {
        txWriter.setSeqTxn(seqTxn);
        txWriter.commit(denseSymbolMapWriters);
    }

    public void commitSeqTxn() {
        if (txWriter.inTransaction()) {
            metrics.tableWriter().incrementCommits();
            syncColumns();
        }
        txWriter.commit(denseSymbolMapWriters);
    }

    public long commitWalTransaction(
            @Transient Path walPath,
            boolean inOrder,
            long rowLo,
            long rowHi,
            long o3TimestampMin,
            long o3TimestampMax,
            SymbolMapDiffCursor mapDiffCursor,
            long seqTxn,
            O3JobParallelismRegulator regulator
    ) {
        if (hasO3() || columnVersionWriter.hasChanges()) {
            // When writer is returned to pool, it should be rolled back. Having an open transaction is very suspicious.
            // Set the writer to distressed state and throw exception so that writer is re-created.
            distressed = true;
            throw CairoException.critical(0).put("cannot process WAL while in transaction");
        }

        physicallyWrittenRowsSinceLastCommit.set(0);
        txWriter.beginPartitionSizeUpdate();
        long commitToTimestamp = walTxnDetails.getCommitToTimestamp(seqTxn);

        LOG.info().$("processing WAL [path=").$substr(pathRootSize, walPath).$(", roLo=").$(rowLo)
                .$(", roHi=").$(rowHi)
                .$(", seqTxn=").$(seqTxn)
                .$(", tsMin=").$ts(o3TimestampMin).$(", tsMax=").$ts(o3TimestampMax)
                .$(", commitToTs=").$ts(commitToTimestamp)
                .I$();

        final long committedRowCount = txWriter.getRowCount();
        final long walSegmentId = walTxnDetails.getWalSegmentId(seqTxn);
        boolean isLastSegmentUsage = walTxnDetails.isLastSegmentUsage(seqTxn);
        boolean committed;
        try {
            committed = processWalBlock(
                    walPath,
                    metadata.getTimestampIndex(),
                    inOrder,
                    rowLo,
                    rowHi,
                    o3TimestampMin,
                    o3TimestampMax,
                    mapDiffCursor,
                    commitToTimestamp,
                    walSegmentId,
                    isLastSegmentUsage,
                    regulator
            );
        } catch (CairoException e) {
            if (e.isOutOfMemory()) {
                // oom -> we cannot rely on internal TableWriter consistency, all bets are off, better to discard it and re-recreate
                distressed = true;
            }
            throw e;
        }
        final long rowsAdded = txWriter.getRowCount() - committedRowCount;

        if (committed) {
            // Useful for debugging
            assert txWriter.getLagRowCount() == 0;

            updateIndexes();
            columnVersionWriter.commit();
            txWriter.setSeqTxn(seqTxn);
            txWriter.setLagTxnCount(0);
            txWriter.setLagOrdered(true);

            syncColumns();
            txWriter.setColumnVersion(columnVersionWriter.getVersion());
            txWriter.commit(denseSymbolMapWriters);

            squashSplitPartitions(minSplitPartitionTimestamp, txWriter.maxTimestamp, configuration.getO3LastPartitionMaxSplits());

            // Bookmark masterRef to track how many rows is in uncommitted state
            committedMasterRef = masterRef;
            processPartitionRemoveCandidates();

            metrics.tableWriter().incrementCommits();

            shrinkO3Mem();
        }

        // Nothing was committed to the table, only copied to LAG.
        // Sometimes data from LAG made visible to the table using fast commit that increment transient row count.
        // Keep in memory last committed seq txn, but do not write it to _txn file.
        assert txWriter.getLagTxnCount() == (seqTxn - txWriter.getSeqTxn());
        metrics.tableWriter().addCommittedRows(rowsAdded);
        return rowsAdded;
    }

    @Override
    public boolean convertPartition(long partitionTimestamp) {
        final int memoryTag = MemoryTag.MMAP_PARTITION_CONVERTER;

        assert metadata.getTimestampIndex() > -1;
        assert PartitionBy.isPartitioned(partitionBy);

        if (inTransaction()) {
            LOG.info()
                    .$("committing open transaction before applying convert partition to parquet command [table=")
                    .utf8(tableToken.getTableName())
                    .$(", partition=").$ts(partitionTimestamp)
                    .I$();
            commit();
        }

        partitionTimestamp = txWriter.getLogicalPartitionTimestamp(partitionTimestamp);
        int partitionIndex = txWriter.getPartitionIndex(partitionTimestamp);
        if (partitionIndex < 0) {
            formatPartitionForTimestamp(partitionTimestamp, -1);
            throw CairoException.nonCritical().put("cannot convert partition to parquet, partition does not exist [table=").put(tableToken.getTableName())
                    .put(", partition=").put(utf8Sink).put(']');
        }
        lastPartitionTimestamp = txWriter.getLastPartitionTimestamp();
        boolean lastPartitionConverted = lastPartitionTimestamp == partitionTimestamp;
        squashPartitionForce(partitionIndex);

        long partitionNameTxn = txWriter.getPartitionNameTxn(partitionIndex);

        setPathForPartition(path.trimTo(pathSize), partitionBy, partitionTimestamp, partitionNameTxn);
        final int partitionLen = path.size();
        if (!ff.exists(path.$())) {
            throw CairoException.nonCritical().put("partition folder does not exist [path=").put(path).put(']');
        }

        // upgrade partition version
        TableUtils.setPathForPartition(other.trimTo(pathSize), partitionBy, partitionTimestamp, getTxn());
        TableUtils.createDirsOrFail(ff, other.slash(), configuration.getMkDirMode());
        final int newPartitionFolderLen = other.size();

        // set parquet file full path
        TableUtils.setParquetPartitionPath(other.trimTo(pathSize), partitionBy, partitionTimestamp, getTxn());

        LOG.info().$("converting partition to parquet [path=").$substr(pathRootSize, path).I$();
        long parquetFileLength;
        try {
            try (PartitionDescriptor partitionDescriptor = new MappedMemoryPartitionDescriptor()) {

                final long partitionRowCount = getPartitionSize(partitionIndex);
                final int timestampIndex = metadata.getTimestampIndex();
                partitionDescriptor.of(getTableToken().getTableName(), partitionRowCount, timestampIndex);

                final int columnCount = metadata.getColumnCount();
                for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                    final String columnName = metadata.getColumnName(columnIndex);
                    final int columnType = metadata.getColumnType(columnIndex);
                    final int columnId = metadata.getColumnMetadata(columnIndex).getWriterIndex();
                    if (columnType > 0) {
                        final long columnNameTxn = getColumnNameTxn(partitionTimestamp, columnIndex);
                        final long columnTop = columnVersionWriter.getColumnTop(partitionTimestamp, columnIndex);
                        final long columnRowCount = (columnTop != -1) ? partitionRowCount - columnTop : 0;

                        if (columnRowCount != 0) {
                            // Do not add the column to the parquet file if there are no rows

                            if (ColumnType.isSymbol(columnType)) {
                                long columnSize = columnRowCount * ColumnType.sizeOf(columnType);
                                long columnAddr = TableUtils.mapRO(ff, dFile(path.trimTo(partitionLen), columnName, columnNameTxn), LOG, columnSize, memoryTag);

                                offsetFileName(path.trimTo(pathSize), columnName, columnNameTxn);
                                if (!ff.exists(path.$())) {
                                    LOG.error().$(path).$(" is not found").$();
                                    throw CairoException.critical(0).put("SymbolMap does not exist: ").put(path);
                                }

                                long fileLength = ff.length(path.$());
                                if (fileLength < SymbolMapWriter.HEADER_SIZE) {
                                    LOG.error().$(path).$("symbol file is too short [fileLength=").$(fileLength).$(']').$();
                                    throw CairoException.critical(0).put("SymbolMap is too short: ").put(path);
                                }

                                final int symbolCount = getSymbolMapWriter(columnIndex).getSymbolCount();
                                final long offsetsMemSize = SymbolMapWriter.keyToOffset(symbolCount + 1);

                                long symbolOffsetsAddr = TableUtils.mapRO(ff, path.$(), LOG, offsetsMemSize, memoryTag);

                                final LPSZ charFileName = charFileName(path.trimTo(pathSize), columnName, columnNameTxn);
                                long columnSecondarySize = ff.length(charFileName);
                                long columnSecondaryAddr = TableUtils.mapRO(ff, charFileName, LOG, columnSecondarySize, memoryTag);

                                partitionDescriptor.addColumn(
                                        columnName,
                                        columnType,
                                        columnId,
                                        columnTop,
                                        columnAddr,
                                        columnSize,
                                        columnSecondaryAddr,
                                        columnSecondarySize,
                                        symbolOffsetsAddr + HEADER_SIZE,
                                        symbolCount
                                );

                                // recover partition path
                                setPathForPartition(path.trimTo(pathSize), partitionBy, partitionTimestamp, partitionNameTxn);
                            } else if (ColumnType.isVarSize(columnType)) {
                                final ColumnTypeDriver columnTypeDriver = ColumnType.getDriver(columnType);
                                long auxVectorSize = columnTypeDriver.getAuxVectorSize(columnRowCount);
                                long auxVectorAddr = TableUtils.mapRO(ff, iFile(path.trimTo(partitionLen), columnName, columnNameTxn), LOG, auxVectorSize, memoryTag);

                                long dataSize = columnTypeDriver.getDataVectorSizeAt(auxVectorAddr, columnRowCount - 1);
                                if (dataSize < columnTypeDriver.getDataVectorMinEntrySize() || dataSize >= (1L << 40)) {
                                    LOG.critical().$("Invalid var len column size [column=").$(columnName).$(", size=").$(dataSize).$(", path=").$(path).I$();
                                    throw CairoException.critical(0).put("Invalid column size [column=").put(path).put(", size=").put(dataSize).put(']');
                                }

                                long dataAddr = dataSize == 0 ? 0 : TableUtils.mapRO(ff, dFile(path.trimTo(partitionLen), columnName, columnNameTxn), LOG, dataSize, memoryTag);
                                partitionDescriptor.addColumn(
                                        columnName,
                                        columnType,
                                        columnId,
                                        columnTop,
                                        dataAddr,
                                        dataSize,
                                        auxVectorAddr,
                                        auxVectorSize,
                                        0,
                                        0
                                );
                            } else {
                                long mapBytes = columnRowCount * ColumnType.sizeOf(columnType);
                                long fixedAddr = TableUtils.mapRO(ff, dFile(path.trimTo(partitionLen), columnName, columnNameTxn), LOG, mapBytes, memoryTag);
                                partitionDescriptor.addColumn(
                                        columnName,
                                        columnType,
                                        columnId,
                                        columnTop,
                                        fixedAddr,
                                        mapBytes,
                                        0,
                                        0,
                                        0,
                                        0
                                );
                            }
                        }
                    } else {
                        throw CairoException.critical(0).put("unsupported column type [column=").put(columnName).put(", type=").put(columnType).put(']');
                    }
                }

                final CairoConfiguration config = this.getConfiguration();
                final int compressionCodec = config.getPartitionEncoderParquetCompressionCodec();
                final int compressionLevel = config.getPartitionEncoderParquetCompressionLevel();
                final int rowGroupSize = config.getPartitionEncoderParquetRowGroupSize();
                final int dataPageSize = config.getPartitionEncoderParquetDataPageSize();
                final boolean statisticsEnabled = config.isPartitionEncoderParquetStatisticsEnabled();
                final int parquetVersion = config.getPartitionEncoderParquetVersion();

                PartitionEncoder.encodeWithOptions(
                        partitionDescriptor,
                        other,
                        ParquetCompression.packCompressionCodecLevel(compressionCodec, compressionLevel),
                        statisticsEnabled,
                        rowGroupSize,
                        dataPageSize,
                        parquetVersion

                );
                parquetFileLength = ff.length(other.$());
            }
        } catch (CairoException e) {
            LOG.error().$("could not convert partition to parquet [table=").utf8(tableToken.getTableName())
                    .$(", partition=").$ts(partitionTimestamp)
                    .$(", error=").$(e.getMessage()).I$();

            // rollback
            if (!ff.rmdir(other.trimTo(newPartitionFolderLen).slash())) {
                LOG.error().$("could not remove parquet file [path=").$(other).I$();
            }
            throw e;
        } finally {
            path.trimTo(pathSize);
            other.trimTo(pathSize);
        }
        final long originalSize = txWriter.getPartitionSize(partitionIndex);
        // used to update txn and bump recordStructureVersion
        txWriter.updatePartitionSizeAndTxnByRawIndex(partitionIndex * LONGS_PER_TX_ATTACHED_PARTITION, originalSize);
        txWriter.setPartitionParquetFormat(partitionTimestamp, parquetFileLength);
        txWriter.bumpPartitionTableVersion();
        txWriter.commit(denseSymbolMapWriters);

        if (lastPartitionConverted) {
            closeActivePartition(false);
        }

        // remove old partition folder
        safeDeletePartitionDir(partitionTimestamp, partitionNameTxn);

        if (lastPartitionConverted) {
            // Open last partition as read-only
            openPartition(partitionTimestamp);
        }
        return true;
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

        timestamp = txWriter.getLogicalPartitionTimestamp(timestamp);
        if (timestamp == txWriter.getLogicalPartitionTimestamp(txWriter.getMaxTimestamp())) {
            return AttachDetachStatus.DETACH_ERR_ACTIVE;
        }

        int partitionIndex = txWriter.getPartitionIndex(timestamp);
        if (partitionIndex < 0) {
            assert !txWriter.attachedPartitionsContains(timestamp);
            return AttachDetachStatus.DETACH_ERR_MISSING_PARTITION;
        }

        // To detach the partition, squash it into single folder if required
        squashPartitionForce(partitionIndex);

        // To check that partition is squashed get the next partition and
        // verify that it's not the same timestamp as the one we are trying to detach.
        // The next partition should exist, since last partition cannot be detached.
        assert txWriter.getLogicalPartitionTimestamp(txWriter.getPartitionTimestampByIndex(partitionIndex + 1)) != timestamp;

        long minTimestamp = txWriter.getMinTimestamp();
        long partitionNameTxn = txWriter.getPartitionNameTxn(partitionIndex);
        Path detachedPath = Path.PATH.get();

        try {
            // path: partition folder to be detached
            setPathForPartition(path.trimTo(pathSize), partitionBy, timestamp, partitionNameTxn);
            if (!ff.exists(path.$())) {
                LOG.error().$("partition folder does not exist [path=").$substr(pathRootSize, path).I$();
                return AttachDetachStatus.DETACH_ERR_MISSING_PARTITION_DIR;
            }

            final int detachedPathLen;
            AttachDetachStatus attachDetachStatus;
            if (ff.isSoftLink(path.$())) {
                detachedPathLen = 0;
                attachDetachStatus = AttachDetachStatus.OK;
                LOG.info().$("detaching partition via unlink [path=").$substr(pathRootSize, path).I$();
            } else {

                detachedPath.of(configuration.getRoot()).concat(tableToken.getDirName());
                int detachedRootLen = detachedPath.size();
                // detachedPath: detached partition folder
                if (!ff.exists(detachedPath.slash$())) {
                    // the detached and standard folders can have different roots
                    // (server.conf: cairo.sql.detached.root)
                    if (0 != ff.mkdirs(detachedPath, detachedMkDirMode)) {
                        LOG.error().$("could no create detached partition folder [errno=").$(ff.errno())
                                .$(", path=").$(detachedPath).I$();
                        return AttachDetachStatus.DETACH_ERR_MKDIR;
                    }
                }
                setPathForPartition(detachedPath.trimTo(detachedRootLen), partitionBy, timestamp, -1L);
                detachedPath.put(DETACHED_DIR_MARKER);
                detachedPathLen = detachedPath.size();
                if (ff.exists(detachedPath.$())) {
                    LOG.error().$("detached partition folder already exist [path=").$(detachedPath).I$();
                    return AttachDetachStatus.DETACH_ERR_ALREADY_DETACHED;
                }

                // Hard link partition folder recursive to partition.detached
                if (ff.hardLinkDirRecursive(path, detachedPath, detachedMkDirMode) != 0) {
                    if (ff.isCrossDeviceCopyError(ff.errno())) {
                        // Cross drive operation. Make full copy to another device.
                        if (ff.copyRecursive(path, detachedPath, detachedMkDirMode) != 0) {
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
                other.of(path).trimTo(pathSize).concat(META_FILE_NAME).$(); // exists already checked
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
                if (timestamp == txWriter.getPartitionTimestampByIndex(0)) {
                    other.of(path).trimTo(pathSize);
                    nextMinTimestamp = readMinTimestamp(txWriter.getPartitionTimestampByIndex(1));
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
                txWriter.commit(denseSymbolMapWriters);
                // return at the end of the method after removing partition directory
            } else {
                // rollback detached copy
                detachedPath.trimTo(detachedPathLen).slash().$();
                if (ff.rmdir(detachedPath)) {
                    LOG.error()
                            .$("could not rollback detached copy (rmdir) [errno=").$(ff.errno())
                            .$(", undo=").$(detachedPath)
                            .$(", original=").$(path)
                            .I$();
                }
                return attachDetachStatus;
            }
        } finally {
            path.trimTo(pathSize);
            other.trimTo(pathSize);
        }
        safeDeletePartitionDir(timestamp, partitionNameTxn);
        return AttachDetachStatus.OK;
    }

    @Override
    public void disableDeduplication() {
        assert txWriter.getLagRowCount() == 0;
        checkDistressed();
        LOG.info().$("disabling row deduplication [table=").utf8(tableToken.getTableName()).I$();
        updateMetadataWithDeduplicationUpsertKeys(false, null);
        try (MetadataCacheWriter metadataRW = engine.getMetadataCache().writeLock()) {
            metadataRW.hydrateTable(metadata);
        }
    }

    @Override
    public void dropIndex(@NotNull CharSequence columnName) {
        checkDistressed();

        final int columnIndex = getColumnIndexQuiet(metaMem, columnName, columnCount);
        if (columnIndex == -1) {
            throw CairoException.invalidMetadataRecoverable("column does not exist", columnName);
        }
        if (!isColumnIndexed(metaMem, columnIndex)) {
            // if a column is indexed, it is also of type SYMBOL
            throw CairoException.invalidMetadataRecoverable("column is not indexed", columnName);
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
            LOG.info().$("removing index [txn=").$(txWriter.getTxn())
                    .$(", table=").utf8(tableToken.getTableName())
                    .$(", column=").utf8(columnName)
                    .I$();
            // drop index
            if (dropIndexOperator == null) {
                dropIndexOperator = new DropIndexOperator(configuration, this, path, other, pathSize, getPurgingOperator());
            }
            dropIndexOperator.executeDropIndex(columnName, columnIndex); // upserts column version in partitions
            // swap meta commit
            metaSwapIndex = copyMetadataAndSetIndexAttrs(columnIndex, false, defaultIndexValueBlockSize);
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
            finishColumnPurge();
            LOG.info().$("REMOVED index [txn=").$(txWriter.getTxn()).$();

            try (MetadataCacheWriter metadataRW = engine.getMetadataCache().writeLock()) {
                metadataRW.hydrateTable(metadata);
            }

            LOG.info().$("END DROP INDEX [txn=").$(txWriter.getTxn())
                    .$(", table=").utf8(tableToken.getTableName())
                    .$(", column=").utf8(columnName)
                    .I$();
        } catch (Throwable e) {
            throw CairoException.critical(0)
                    .put("cannot remove index for [txn=").put(txWriter.getTxn())
                    .put(", table=").put(tableToken.getTableName())
                    .put(", column=").put(columnName)
                    .put("]: ").put(e.getMessage());
        }
    }

    @Override
    public void enableDeduplicationWithUpsertKeys(LongList columnsIndexes) {
        assert txWriter.getLagRowCount() == 0;
        checkDistressed();
        LogRecord logRec = LOG.info().$("enabling row deduplication [table=").utf8(tableToken.getTableName()).$(", columns=[");

        try {
            int upsertKeyColumn = columnsIndexes.size();
            for (int i = 0; i < upsertKeyColumn; i++) {
                int dedupColIndex = (int) columnsIndexes.getQuick(i);
                if (dedupColIndex < 0 || dedupColIndex >= metadata.getColumnCount()) {
                    throw CairoException.critical(0).put("Invalid column index to make a dedup key [table=")
                            .put(tableToken.getTableName()).put(", columnIndex=").put(dedupColIndex);
                }

                int columnType = metadata.getColumnType(dedupColIndex);
                if (columnType < 0) {
                    throw CairoException.critical(0).put("Invalid column used as deduplicate key, column is dropped [table=")
                            .put(tableToken.getTableName()).put(", columnIndex=").put(dedupColIndex);
                }
                if (i > 0) {
                    logRec.$(',');
                }
                logRec.$(getColumnNameSafe(dedupColIndex)).$(':').$(ColumnType.nameOf(columnType));
            }
        } finally {
            logRec.I$();
        }
        updateMetadataWithDeduplicationUpsertKeys(true, columnsIndexes);
        try (MetadataCacheWriter metadataRW = engine.getMetadataCache().writeLock()) {
            metadataRW.hydrateTable(metadata);
        }
    }

    public long getAppliedSeqTxn() {
        return txWriter.getSeqTxn() + txWriter.getLagTxnCount();
    }

    public int getColumnCount() {
        return columns.size();
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

    public long getColumnStructureVersion() {
        return txWriter.getColumnStructureVersion();
    }

    public long getColumnTop(long partitionTimestamp, int columnIndex, long defaultValue) {
        long colTop = columnVersionWriter.getColumnTop(partitionTimestamp, columnIndex);
        return colTop > -1L ? colTop : defaultValue;
    }

    public long getDataAppendPageSize() {
        return dataAppendPageSize;
    }

    public DedupColumnCommitAddresses getDedupCommitAddresses() {
        return dedupColumnCommitAddresses;
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
    public int getMetaMaxUncommittedRows() {
        return metadata.getMaxUncommittedRows();
    }

    @Override
    public long getMetaO3MaxLag() {
        return metadata.getO3MaxLag();
    }

    @Override
    public TableMetadata getMetadata() {
        return metadata;
    }

    @Override
    public long getMetadataVersion() {
        return txWriter.getMetadataVersion();
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

    public int getPartitionIndexByTimestamp(long timestamp) {
        return txWriter.getPartitionIndex(timestamp);
    }

    public long getPartitionNameTxn(int partitionIndex) {
        return txWriter.getPartitionNameTxn(partitionIndex);
    }

    public long getPartitionNameTxnByPartitionTimestamp(long partitionTimestamp) {
        return txWriter.getPartitionNameTxnByPartitionTimestamp(partitionTimestamp, -1L);
    }

    public long getPartitionO3SplitThreshold() {
        long splitMinSizeBytes = configuration.getPartitionO3SplitMinSize();
        return splitMinSizeBytes /
                (avgRecordSize != 0 ? avgRecordSize : (avgRecordSize = TableUtils.estimateAvgRecordSize(metadata)));
    }

    public long getPartitionParquetFileSize(int partitionIndex) {
        return txWriter.getPartitionParquetFileSize(partitionIndex);
    }

    public long getPartitionSize(int partitionIndex) {
        if (partitionIndex == txWriter.getPartitionCount() - 1 || !PartitionBy.isPartitioned(partitionBy)) {
            return txWriter.getTransientRowCount();
        }
        return txWriter.getPartitionSize(partitionIndex);
    }

    public long getPartitionTimestamp(int partitionIndex) {
        return txWriter.getPartitionTimestampByIndex(partitionIndex);
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

    public MemoryMA getStorageColumn(int index) {
        return columns.getQuick(index);
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
            updateOperatorImpl = new UpdateOperatorImpl(configuration, this, path, pathSize, getPurgingOperator());
        }
        return updateOperatorImpl;
    }

    public WalTxnDetails getWalTnxDetails() {
        return walTxnDetails;
    }

    public void goActive() {

    }

    public void goPassive() {
        Misc.freeObjListAndKeepObjects(o3MemColumns1);
        Misc.freeObjListAndKeepObjects(o3MemColumns2);
    }

    public boolean hasO3() {
        return o3MasterRef > -1;
    }

    @Override
    public void ic(long o3MaxLag) {
        commit(o3MaxLag);
    }

    @Override
    public void ic() {
        commit(metadata.getO3MaxLag());
    }

    public boolean inTransaction() {
        return txWriter != null && (txWriter.inTransaction() || hasO3() || (columnVersionWriter != null && columnVersionWriter.hasChanges()));
    }

    public boolean isDeduplicationEnabled() {
        int tsIndex = metadata.timestampIndex;
        return tsIndex > -1 && metadata.isDedupKey(tsIndex);
    }

    public boolean isOpen() {
        return tempMem16b != 0;
    }

    public boolean isPartitionReadOnly(int partitionIndex) {
        return txWriter.isPartitionReadOnly(partitionIndex);
    }

    public boolean isSymbolMapWriterCached(int columnIndex) {
        return symbolMapWriters.getQuick(columnIndex).isCached();
    }

    public void markSeqTxnCommitted(long seqTxn) {
        setSeqTxn(seqTxn);
        txWriter.commit(denseSymbolMapWriters);
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
                    initLastPartition(txWriter.getPartitionTimestampByTimestamp(timestamp));
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
                        switchPartition(txWriter.getPartitionTimestampByTimestamp(timestamp));
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
                    throw CairoException.nonCritical().put("cannot insert rows out of order to non-partitioned table. Table=").put(path);
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

    @Override
    public Row newRowDeferTimestamp() {
        throw new UnsupportedOperationException();
    }

    public void o3BumpErrorCount(boolean oom) {
        o3ErrorCount.incrementAndGet();
        if (oom) {
            o3oomObserved = true;
        }
    }

    public void openLastPartition() {
        try {
            openLastPartitionAndSetAppendPosition(txWriter.getLastPartitionTimestamp());
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

    // returns true if the tx was committed into the table and can be made visible to readers
    // returns false if the tx was only copied to LAG and not committed - in this case the tx is not visible to readers
    public boolean processWalBlock(
            @Transient Path walPath,
            int timestampIndex,
            boolean ordered,
            long rowLo,
            long rowHi,
            final long o3TimestampMin,
            final long o3TimestampMax,
            SymbolMapDiffCursor mapDiffCursor,
            long commitToTimestamp,
            long walSegmentId,
            boolean isLastSegmentUsage,
            O3JobParallelismRegulator regulator
    ) {
        int walRootPathLen = walPath.size();
        long maxTimestamp = txWriter.getMaxTimestamp();
        if (isLastPartitionClosed()) {
            if (isEmptyTable()) {
                // The table is empty, last partition does not exist
                // WAL processing needs last partition to store LAG data
                // Create artificial partition at the point of o3TimestampMin.
                openPartition(o3TimestampMin);
                txWriter.setMaxTimestamp(o3TimestampMin);
                // Add the partition to the list of partitions with 0 size.
                txWriter.updatePartitionSizeByTimestamp(o3TimestampMin, 0, txWriter.getTxn() - 1);
            } else {
                throw CairoException.critical(0).put("system error, cannot resolve WAL table last partition [path=")
                        .put(path).put(']');
            }
        }

        assert maxTimestamp == Long.MIN_VALUE ||
                txWriter.getPartitionTimestampByTimestamp(partitionTimestampHi) == txWriter.getPartitionTimestampByTimestamp(txWriter.maxTimestamp);

        lastPartitionTimestamp = txWriter.getPartitionTimestampByTimestamp(partitionTimestampHi);
        boolean success = true;
        try {
            boolean forceFullCommit = commitToTimestamp == WalTxnDetails.FORCE_FULL_COMMIT;
            final long maxLagRows = getWalMaxLagRows();
            final long walLagMaxTimestampBefore = txWriter.getLagMaxTimestamp();
            mmapWalColumns(walPath, walSegmentId, timestampIndex, rowLo, rowHi);
            final long newMinLagTimestamp = Math.min(o3TimestampMin, txWriter.getLagMinTimestamp());
            long initialPartitionTimestampHi = partitionTimestampHi;
            long commitMaxTimestamp, commitMinTimestamp;

            long walLagRowCount = txWriter.getLagRowCount();
            long o3Hi = rowHi;
            try {
                long o3Lo = rowLo;
                long commitRowCount = rowHi - rowLo;
                boolean copiedToMemory;

                long totalUncommitted = walLagRowCount + commitRowCount;
                long newMaxLagTimestamp = Math.max(o3TimestampMax, txWriter.getLagMaxTimestamp());
                boolean needFullCommit = forceFullCommit
                        // Too many rows in LAG
                        || totalUncommitted > maxLagRows
                        // Can commit without O3 and LAG has just enough rows
                        || (commitToTimestamp >= newMaxLagTimestamp && totalUncommitted > getMetaMaxUncommittedRows())
                        // Too many uncommitted transactions in LAG
                        || (configuration.getWalMaxLagTxnCount() > 0 && txWriter.getLagTxnCount() >= configuration.getWalMaxLagTxnCount());

                boolean canFastCommit = indexers.size() == 0 && applyFromWalLagToLastPartitionPossible(commitToTimestamp, txWriter.getLagRowCount(), txWriter.isLagOrdered(), txWriter.getMaxTimestamp(), txWriter.getLagMinTimestamp(), txWriter.getLagMaxTimestamp());
                boolean lagOrderedNew = !isDeduplicationEnabled() && txWriter.isLagOrdered() && ordered && walLagMaxTimestampBefore <= o3TimestampMin;
                boolean canFastCommitNew = applyFromWalLagToLastPartitionPossible(commitToTimestamp, totalUncommitted, lagOrderedNew, txWriter.getMaxTimestamp(), newMinLagTimestamp, newMaxLagTimestamp);

                // Fast commit of existing LAG data is possible but will not be possible after current transaction is added to the lag.
                // Also fast LAG commit will not cause O3 with the current transaction.
                if (!needFullCommit && canFastCommit && !canFastCommitNew && txWriter.getLagMaxTimestamp() <= o3TimestampMin) {
                    // Fast commit lag data and then proceed.
                    applyFromWalLagToLastPartition(commitToTimestamp, false);

                    // Re-evaluate commit decision
                    walLagRowCount = txWriter.getLagRowCount();
                    totalUncommitted = commitRowCount;
                    newMaxLagTimestamp = Math.max(o3TimestampMax, txWriter.getLagMaxTimestamp());
                    needFullCommit =
                            // Too many rows in LAG
                            totalUncommitted > maxLagRows
                                    // Can commit without O3 and LAG has just enough rows
                                    || (commitToTimestamp >= newMaxLagTimestamp && totalUncommitted > getMetaMaxUncommittedRows())
                                    // Too many uncommitted transactions in LAG
                                    || (configuration.getWalMaxLagTxnCount() > 0 && txWriter.getLagTxnCount() > configuration.getWalMaxLagTxnCount());
                }

                if (!needFullCommit || canFastCommitNew) {
                    // Don't commit anything, move everything to lag area of last partition instead.
                    // This usually happens when WAL transactions are very small, so it's faster
                    // to squash several of them together before writing anything to all the partitions.
                    LOG.debug().$("all WAL rows copied to LAG [table=").$(tableToken).I$();

                    o3Columns = remapWalSymbols(mapDiffCursor, rowLo, rowHi, walPath);
                    // This will copy data from mmap files to memory.
                    // Symbols are already mapped to the correct destination.
                    dispatchColumnTasks(
                            o3Hi - o3Lo,
                            IGNORE,
                            o3Lo,
                            walLagRowCount,
                            1,
                            this.cthAppendWalColumnToLastPartition
                    );
                    walLagRowCount += commitRowCount;
                    addPhysicallyWrittenRows(commitRowCount);
                    txWriter.setLagRowCount((int) walLagRowCount);
                    txWriter.setLagOrdered(lagOrderedNew);
                    txWriter.setLagMinTimestamp(newMinLagTimestamp);
                    txWriter.setLagMaxTimestamp(Math.max(o3TimestampMax, txWriter.getLagMaxTimestamp()));
                    txWriter.setLagTxnCount(txWriter.getLagTxnCount() + 1);

                    if (canFastCommitNew) {
                        applyFromWalLagToLastPartition(commitToTimestamp, false);
                    }

                    return forceFullCommit;
                }

                // Try to fast apply records from LAG to last partition which are before o3TimestampMin and commitToTimestamp.
                // This application will not include the current transaction data, only what's already in WAL lag.
                if (applyFromWalLagToLastPartition(Math.min(o3TimestampMin, commitToTimestamp), true) != Long.MIN_VALUE) {
                    walLagRowCount = txWriter.getLagRowCount();
                    totalUncommitted = walLagRowCount + commitRowCount;
                }

                // Re-valuate WAL lag min/max with impact of the current transaction.
                txWriter.setLagMinTimestamp(Math.min(o3TimestampMin, txWriter.getLagMinTimestamp()));
                txWriter.setLagMaxTimestamp(Math.max(o3TimestampMax, txWriter.getLagMaxTimestamp()));
                boolean needsOrdering = !ordered || walLagRowCount > 0;
                boolean needsDedup = isDeduplicationEnabled();

                long timestampAddr = 0;
                MemoryCR walTimestampColumn = walMappedColumns.getQuick(getPrimaryColumnIndex(timestampIndex));
                o3Columns = remapWalSymbols(mapDiffCursor, rowLo, rowHi, walPath);

                if (needsOrdering || needsDedup) {
                    if (needsOrdering) {
                        LOG.info().$("sorting WAL [table=").$(tableToken)
                                .$(", ordered=").$(ordered)
                                .$(", lagRowCount=").$(walLagRowCount)
                                .$(", walRowLo=").$(rowLo)
                                .$(", walRowHi=").$(rowHi).I$();

                        final long timestampMemorySize = totalUncommitted << 4;
                        o3TimestampMem.jumpTo(timestampMemorySize);
                        o3TimestampMemCpy.jumpTo(timestampMemorySize);

                        MemoryMA timestampColumn = columns.get(getPrimaryColumnIndex(timestampIndex));
                        final long tsLagOffset = txWriter.getTransientRowCount() << 3;
                        final long tsLagSize = walLagRowCount << 3;
                        final long mappedTimestampIndexAddr = walTimestampColumn.addressOf(rowLo << 4);
                        timestampAddr = o3TimestampMem.getAddress();

                        final long tsLagBufferAddr = mapAppendColumnBuffer(timestampColumn, tsLagOffset, tsLagSize, false);
                        try {
                            Vect.radixSortABLongIndexAsc(
                                    Math.abs(tsLagBufferAddr),
                                    walLagRowCount,
                                    mappedTimestampIndexAddr,
                                    commitRowCount,
                                    timestampAddr,
                                    o3TimestampMemCpy.addressOf(0)
                            );
                        } finally {
                            mapAppendColumnBufferRelease(tsLagBufferAddr, tsLagOffset, tsLagSize);
                        }
                    } else {
                        // Needs deduplication only
                        timestampAddr = walTimestampColumn.addressOf(rowLo * TIMESTAMP_MERGE_ENTRY_BYTES);
                    }

                    if (needsDedup) {
                        o3TimestampMemCpy.jumpTo(totalUncommitted * TIMESTAMP_MERGE_ENTRY_BYTES);
                        o3TimestampMem.jumpTo(totalUncommitted * TIMESTAMP_MERGE_ENTRY_BYTES);
                        long dedupTimestampAddr = o3TimestampMem.getAddress();
                        long deduplicatedRowCount = deduplicateSortedIndex(
                                totalUncommitted,
                                timestampAddr,
                                dedupTimestampAddr,
                                o3TimestampMemCpy.addressOf(0),
                                walLagRowCount
                        );
                        if (deduplicatedRowCount > 0) {
                            // There are timestamp duplicates, reshuffle the records
                            needsOrdering = true;
                            timestampAddr = dedupTimestampAddr;
                            totalUncommitted = deduplicatedRowCount;
                        }
                    }
                }

                if (needsOrdering) {
                    dispatchColumnTasks(timestampAddr, totalUncommitted, walLagRowCount, rowLo, rowHi, cthMergeWalColumnWithLag);
                    swapO3ColumnsExcept(timestampIndex);

                    // Sorted data is now sorted in memory copy of the data from mmap files
                    // Row indexes start from 0, not rowLo
                    o3Hi = totalUncommitted;
                    o3Lo = 0L;
                    walLagRowCount = 0L;
                    o3Columns = o3MemColumns1;
                    copiedToMemory = true;
                } else {
                    // Wal column can are lazily mapped to improve performance. It works ok, except in this case
                    // where access getAddress() calls be concurrent. Map them eagerly now.
                    mmapWalColsEager();

                    timestampAddr = walTimestampColumn.addressOf(0);
                    copiedToMemory = false;
                }

                // We could commit some portion of the lag into the partitions and keep some data in the lag
                // like 70/30 split, but it would break snapshot assumptions and this optimization is removed
                // in the next release after v7.3.9.
                // Commit everything.
                walLagRowCount = 0;
                commitMinTimestamp = txWriter.getLagMinTimestamp();
                commitMaxTimestamp = txWriter.getLagMaxTimestamp();
                txWriter.setLagMinTimestamp(Long.MAX_VALUE);
                txWriter.setLagMaxTimestamp(Long.MIN_VALUE);

                o3RowCount = o3Hi - o3Lo + walLagRowCount;

                // Real data writing into table happens here.
                // Everything above it is to figure out how much data to write now,
                // map symbols and sort data if necessary.
                if (o3Hi > o3Lo) {
                    // Now that everything from WAL lag is in memory or in WAL columns,
                    // we can remove artificial 0 length partition created to store lag when table did not have any partitions
                    if (txWriter.getRowCount() == 0 && txWriter.getPartitionCount() == 1 && txWriter.getPartitionSize(0) == 0) {
                        txWriter.setMaxTimestamp(Long.MIN_VALUE);
                        lastPartitionTimestamp = Long.MIN_VALUE;
                        closeActivePartition(false);
                        partitionTimestampHi = Long.MIN_VALUE;
                        long partitionTimestamp = txWriter.getPartitionTimestampByIndex(0);
                        long partitionNameTxn = txWriter.getPartitionNameTxnByRawIndex(0);
                        txWriter.removeAttachedPartitions(partitionTimestamp);
                        safeDeletePartitionDir(partitionTimestamp, partitionNameTxn);
                    }

                    processO3Block(
                            walLagRowCount,
                            timestampIndex,
                            timestampAddr,
                            o3Hi,
                            commitMinTimestamp,
                            commitMaxTimestamp,
                            copiedToMemory,
                            o3Lo,
                            regulator
                    );

                    finishO3Commit(initialPartitionTimestampHi);
                }
                txWriter.setLagOrdered(true);
                txWriter.setLagRowCount((int) walLagRowCount);
            } finally {
                finishO3Append(walLagRowCount);
                o3Columns = o3MemColumns1;
            }

            return true;
        } catch (Throwable th) {
            success = false;
            throw th;
        } finally {
            walPath.trimTo(walRootPathLen);
            closeWalColumns(isLastSegmentUsage || !success, walSegmentId);
        }
    }

    public void publishAsyncWriterCommand(AsyncWriterCommand asyncWriterCommand) {
        while (true) {
            long seq = commandPubSeq.next();
            if (seq > -1) {
                TableWriterTask task = commandQueue.get(seq);
                asyncWriterCommand.serialize(task);
                assert task.getInstance() == asyncWriterCommand.getCorrelationId();
                commandPubSeq.done(seq);
                return;
            } else if (seq == -1) {
                throw CairoException.nonCritical().put("could not publish, command queue is full [table=").put(tableToken.getTableName()).put(']');
            }
            Os.pause();
        }
    }

    public void readWalTxnDetails(TransactionLogCursor transactionLogCursor) {
        if (walTxnDetails == null) {
            // Lazy creation
            walTxnDetails = new WalTxnDetails(ff, configuration.getWalApplyLookAheadTransactionCount() * 10);
        }

        long appliedSeqTxn = getAppliedSeqTxn();
        transactionLogCursor.setPosition(Math.max(appliedSeqTxn, walTxnDetails.getLastSeqTxn()));
        walTxnDetails.readObservableTxnMeta(other, transactionLogCursor, pathSize, appliedSeqTxn, txWriter.getMaxTimestamp());
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

        // Remove all partitions from txn file, column version file.
        txWriter.beginPartitionSizeUpdate();

        closeActivePartition(false);
        scheduleRemoveAllPartitions();

        columnVersionWriter.truncate();
        txWriter.removeAllPartitions();
        columnVersionWriter.commit();
        txWriter.setColumnVersion(columnVersionWriter.getVersion());
        txWriter.commit(denseSymbolMapWriters);
        rowAction = ROW_ACTION_OPEN_PARTITION;

        closeActivePartition(false);
        processPartitionRemoveCandidates();

        LOG.info().$("removed all partitions (soft truncated) [name=").utf8(tableToken.getTableName()).I$();
    }

    @Override
    public void removeColumn(@NotNull CharSequence name) {
        assert txWriter.getLagRowCount() == 0;

        checkDistressed();
        checkColumnName(name);

        final int index = getColumnIndex(name);
        final int type = metadata.getColumnType(index);

        LOG.info().$("removing [column=").utf8(name).$(", path=").$substr(pathRootSize, path).I$();

        // check if we are moving timestamp from a partitioned table
        final int timestampIndex = metaMem.getInt(META_OFFSET_TIMESTAMP_INDEX);
        boolean timestamp = (index == timestampIndex);

        if (timestamp && PartitionBy.isPartitioned(partitionBy)) {
            throw CairoException.nonCritical().put("cannot remove timestamp from partitioned table");
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
        freeColumnMemory(index);

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
            openMetaFile(ff, path, pathSize, metaMem);

            // remove _todo
            clearTodoLog();

            // remove column files has to be done after _todo is removed
            removeColumnFiles(index, type);
        } catch (CairoException e) {
            throwDistressException(e);
        }

        bumpMetadataAndColumnStructureVersion();

        metadata.removeColumn(index);
        if (timestamp) {
            metadata.clearTimestampIndex();
        }

        finishColumnPurge();

        try (MetadataCacheWriter metadataRW = engine.getMetadataCache().writeLock()) {
            metadataRW.hydrateTable(metadata);
        }
        LOG.info().$("REMOVED column '").utf8(name).$('[').$(ColumnType.nameOf(type)).$("]' from ").$substr(pathRootSize, path).$();
    }

    @Override
    public boolean removePartition(long timestamp) {
        if (!PartitionBy.isPartitioned(partitionBy)) {
            return false;
        }

        // commit changes, there may be uncommitted rows of any partition
        commit();

        // Handle split partitions.
        // One logical partition may be split into multiple physical partitions.
        // For example partition daily '2024-02-24' can be stored as 2 pieces '2024-02-24' and '2024-02-24T12'
        long logicalPartitionTimestampToDelete = txWriter.getLogicalPartitionTimestamp(timestamp);
        int partitionIndex = txWriter.getPartitionIndex(logicalPartitionTimestampToDelete);
        boolean dropped = false;
        if (partitionIndex >= 0) {
            long partitionTimestamp;
            while (partitionIndex < txWriter.getPartitionCount() &&
                    txWriter.getLogicalPartitionTimestamp(
                            partitionTimestamp = txWriter.getPartitionTimestampByIndex(partitionIndex)
                    ) == logicalPartitionTimestampToDelete) {
                dropped |= dropPartitionByExactTimestamp(partitionTimestamp);
            }
        }
        return dropped;
    }

    @Override
    public void renameColumn(
            @NotNull CharSequence currentName,
            @NotNull CharSequence newName,
            SecurityContext securityContext
    ) {
        checkDistressed();
        checkColumnName(newName);

        final int index = getColumnIndex(currentName);
        final int type = metadata.getColumnType(index);

        LOG.info().$("renaming column '").utf8(currentName).$('[').$(ColumnType.nameOf(type)).$("]' to '").utf8(newName).$("' in ").$substr(pathRootSize, path).$();

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
            openMetaFile(ff, path, pathSize, metaMem);

            // remove _todo
            clearTodoLog();

            // rename column files has to be done after _todo is removed
            hardLinkAndPurgeColumnFiles(currentName, index, newName, type);
        } catch (CairoException e) {
            throwDistressException(e);
        }

        bumpMetadataAndColumnStructureVersion();

        // Call finish purge to remove old column files before renaming them in metadata
        finishColumnPurge();
        metadata.renameColumn(currentName, newName);

        if (index == metadata.getTimestampIndex()) {
            designatedTimestampColumnName = Chars.toString(newName);
        }

        if (securityContext != null) {
            ddlListener.onColumnRenamed(securityContext, tableToken, currentName, newName);
        }

        try (MetadataCacheWriter metadataRW = engine.getMetadataCache().writeLock()) {
            metadataRW.hydrateTable(metadata);
        }

        LOG.info().$("RENAMED column '").utf8(currentName).$("' to '").utf8(newName).$("' from ").$substr(pathRootSize, path).$();
    }

    @Override
    public void renameTable(@NotNull CharSequence fromTableName, @NotNull CharSequence toTableName) {
        // table writer is not involved in concurrent table rename, the `fromTableName` must
        // always match tableWriter's table name
        LOG.debug().$("renaming table [path=").$substr(pathRootSize, path).$(", seqTxn=").$(txWriter.getSeqTxn()).I$();
        try {
            TableUtils.overwriteTableNameFile(path, ddlMem, ff, toTableName);
        } finally {
            path.trimTo(pathSize);
        }
        // Record column structure version bump in txn file for WAL sequencer structure version to match writer structure version.
        bumpColumnStructureVersion();

//        try (MetadataCacheWriter metadataRW = engine.getCairoMetadata().write()) {
//            metadataRW.dropTable(fromTableName);
//            metadataRW.hydrateTable(metadata, true);
//        }
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
                closeActivePartition(false);
                purgeUnusedPartitions();
                configureAppendPosition();
                o3InError = false;
                // when we rolled transaction back, hasO3() has to be false
                o3MasterRef = -1;
                LOG.info().$("tx rollback complete [name=").utf8(tableToken.getTableName()).I$();
                processCommandQueue(false);
                metrics.tableWriter().incrementRollbacks();
            } catch (Throwable e) {
                LOG.critical().$("could not perform rollback [name=").utf8(tableToken.getTableName()).$(", msg=").$(e).I$();
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
            openMetaSwapFileByIndex(ff, ddlMem, path, pathSize, this.metaSwapIndex);
            try {
                ddlMem.jumpTo(META_OFFSET_MAX_UNCOMMITTED_ROWS);
                ddlMem.putInt(maxUncommittedRows);
                ddlMem.jumpTo(metaSize);
            } finally {
                ddlMem.close();
            }

            finishMetaSwapUpdate();
            metadata.setMaxUncommittedRows(maxUncommittedRows);

            try (MetadataCacheWriter metadataRW = engine.getMetadataCache().writeLock()) {
                metadataRW.hydrateTable(metadata);
            }

        } finally {
            ddlMem.close();
        }
    }

    @Override
    public void setMetaO3MaxLag(long o3MaxLagUs) {
        try {
            commit();
            long metaSize = copyMetadataAndUpdateVersion();
            openMetaSwapFileByIndex(ff, ddlMem, path, pathSize, this.metaSwapIndex);
            try {
                ddlMem.jumpTo(META_OFFSET_O3_MAX_LAG);
                ddlMem.putLong(o3MaxLagUs);
                ddlMem.jumpTo(metaSize);
            } finally {
                ddlMem.close();
            }

            finishMetaSwapUpdate();
            metadata.setO3MaxLag(o3MaxLagUs);

            try (MetadataCacheWriter metadataRW = engine.getMetadataCache().writeLock()) {
                metadataRW.hydrateTable(metadata);
            }
        } finally {
            ddlMem.close();
        }
    }

    public void setSeqTxn(long seqTxn) {
        assert txWriter.getLagRowCount() == 0 && txWriter.getLagTxnCount() == 0;
        txWriter.setSeqTxn(seqTxn);
    }

    public long size() {
        // This is uncommitted row count
        return txWriter.getRowCount() + getO3RowCount();
    }

    @TestOnly
    public void squashAllPartitionsIntoOne() {
        squashSplitPartitions(0, txWriter.getPartitionCount(), 1, false);
    }

    @Override
    public void squashPartitions() {
        // Do not cache txWriter.getPartitionCount() as it changes during the squashing
        for (int i = 0; i < txWriter.getPartitionCount(); i++) {
            squashPartitionForce(i);
        }
        // Reopen last partition if we've closed it.
        if (isLastPartitionClosed() && !isEmptyTable()) {
            openLastPartition();
        }
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

    public void transferLock(long lockFd) {
        assert lockFd > -1;
        this.lockFd = lockFd;
    }

    /**
     * Truncates table including symbol tables. When operation is unsuccessful it throws CairoException.
     * With that truncate can be retried or alternatively table can be closed. Outcome of any other operation
     * with the table is undefined and likely to cause segmentation fault. When table re-opens any partial
     * truncate will be retried.
     */
    @Override
    public final void truncate() {
        truncate(false);
    }

    /**
     * Truncates table, but keeps symbol tables. When operation is unsuccessful it throws CairoException.
     * With that truncate can be retried or alternatively table can be closed. Outcome of any other operation
     * with the table is undefined and likely to cause segmentation fault. When table re-opens any partial
     * truncate will be retried.
     */
    @Override
    public final void truncateSoft() {
        truncate(true);
    }

    public void updateTableToken(TableToken tableToken) {
        this.tableToken = tableToken;
        this.metadata.updateTableToken(tableToken);
    }

    public void upsertColumnVersion(long partitionTimestamp, int columnIndex, long columnTop) {
        columnVersionWriter.upsert(partitionTimestamp, columnIndex, txWriter.txn, columnTop);
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

    private static void configureNullSetters(ObjList<Runnable> nullers, int columnType, MemoryA dataMem, MemoryA auxMem) {
        short columnTag = ColumnType.tagOf(columnType);
        if (ColumnType.isVarSize(columnTag)) {
            final ColumnTypeDriver typeDriver = ColumnType.getDriver(columnTag);
            nullers.add(() -> typeDriver.appendNull(auxMem, dataMem));
        } else {
            switch (columnTag) {
                case ColumnType.BOOLEAN:
                case ColumnType.BYTE:
                    nullers.add(() -> dataMem.putByte((byte) 0));
                    break;
                case ColumnType.DOUBLE:
                    nullers.add(() -> dataMem.putDouble(Double.NaN));
                    break;
                case ColumnType.FLOAT:
                    nullers.add(() -> dataMem.putFloat(Float.NaN));
                    break;
                case ColumnType.INT:
                    nullers.add(() -> dataMem.putInt(Numbers.INT_NULL));
                    break;
                case ColumnType.IPv4:
                    nullers.add(() -> dataMem.putInt(Numbers.IPv4_NULL));
                    break;
                case ColumnType.LONG:
                case ColumnType.DATE:
                case ColumnType.TIMESTAMP:
                    nullers.add(() -> dataMem.putLong(Numbers.LONG_NULL));
                    break;
                case ColumnType.LONG128:
                    // fall through
                case ColumnType.UUID:
                    nullers.add(() -> dataMem.putLong128(Numbers.LONG_NULL, Numbers.LONG_NULL));
                    break;
                case ColumnType.LONG256:
                    nullers.add(() -> dataMem.putLong256(Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL));
                    break;
                case ColumnType.SHORT:
                    nullers.add(() -> dataMem.putShort((short) 0));
                    break;
                case ColumnType.CHAR:
                    nullers.add(() -> dataMem.putChar((char) 0));
                    break;
                case ColumnType.SYMBOL:
                    nullers.add(() -> dataMem.putInt(SymbolTable.VALUE_IS_NULL));
                    break;
                case ColumnType.GEOBYTE:
                    nullers.add(() -> dataMem.putByte(GeoHashes.BYTE_NULL));
                    break;
                case ColumnType.GEOSHORT:
                    nullers.add(() -> dataMem.putShort(GeoHashes.SHORT_NULL));
                    break;
                case ColumnType.GEOINT:
                    nullers.add(() -> dataMem.putInt(GeoHashes.INT_NULL));
                    break;
                case ColumnType.GEOLONG:
                    nullers.add(() -> dataMem.putLong(GeoHashes.NULL));
                    break;
                default:
                    nullers.add(NOOP);
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
    private static int getColumnIndexQuiet(MemoryMR metaMem, CharSequence name, int columnCount) {
        long nameOffset = getColumnNameOffset(columnCount);
        for (int i = 0; i < columnCount; i++) {
            CharSequence col = metaMem.getStrA(nameOffset);
            int columnType = getColumnType(metaMem, i); // Negative means deleted column
            if (columnType > 0 && Chars.equalsIgnoreCase(col, name)) {
                return i;
            }
            nameOffset += Vm.getStorageLength(col);
        }
        return -1;
    }

    private static void linkFile(FilesFacade ff, LPSZ from, LPSZ to) {
        if (ff.exists(from)) {
            if (ff.hardLink(from, to) == FILES_RENAME_OK) {
                LOG.debug().$("renamed [from=").$(from).$(", to=").$(to).I$();
                return;
            } else if (ff.exists(to)) {
                LOG.info().$("rename destination file exists, assuming previously failed rename attempt [path=").$(to).I$();
                ff.remove(to);
                if (ff.hardLink(from, to) == FILES_RENAME_OK) {
                    LOG.debug().$("renamed [from=").$(from).$(", to=").$(to).I$();
                    return;
                }
            }

            throw CairoException.critical(ff.errno())
                    .put("could not create hard link [errno=").put(ff.errno())
                    .put(", from=").put(from)
                    .put(", to=").put(to)
                    .put(']');
        }
    }

    @NotNull
    private static ColumnVersionWriter openColumnVersionFile(
            CairoConfiguration configuration,
            Path path,
            int rootLen,
            boolean partitioned
    ) {
        path.concat(COLUMN_VERSION_FILE_NAME);
        try {
            return new ColumnVersionWriter(configuration, path.$(), partitioned);
        } finally {
            path.trimTo(rootLen);
        }
    }

    private static void openMetaFile(FilesFacade ff, Path path, int rootLen, MemoryMR metaMem) {
        path.concat(META_FILE_NAME);
        try {
            metaMem.smallFile(ff, path.$(), MemoryTag.MMAP_TABLE_WRITER);
        } finally {
            path.trimTo(rootLen);
        }
    }

    private static void removeFileOrLog(FilesFacade ff, LPSZ name) {
        if (!ff.removeQuiet(name)) {
            LOG.error()
                    .$("could not remove [errno=").$(ff.errno())
                    .$(", file=").$(name)
                    .I$();
        }
    }

    private void addColumnToMeta(
            CharSequence columnName,
            int columnType,
            int symbolCapacity,
            boolean symbolCacheFlag,
            boolean isIndexed,
            int indexValueBlockCapacity,
            boolean isSequential,
            boolean isDedupKey,
            long columnNameTxn,
            int replaceColumnIndex
    ) {
        // create new _meta.swp
        this.metaSwapIndex = addColumnToMeta0(columnName, columnType, isIndexed, indexValueBlockCapacity, isSequential, isDedupKey, replaceColumnIndex);

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

        // don't create symbol writer when column conversion happens, it should be created before the conversion
        if (replaceColumnIndex < 0) {
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
        }

        // add column objects
        configureColumn(columnType, isIndexed, columnCount);
        if (isIndexed) {
            populateDenseIndexerList();
        }

        // increment column count
        columnCount++;
    }

    private int addColumnToMeta0(
            CharSequence name,
            int type,
            boolean indexFlag,
            int indexValueBlockCapacity,
            boolean sequentialFlag,
            boolean dedupKeyFlag,
            int replaceColumnIndex
    ) {
        int index;
        try {
            index = openMetaSwapFile(ff, ddlMem, path, pathSize, configuration.getMaxSwapFileCount());
            int columnCount = metaMem.getInt(META_OFFSET_COUNT);

            ddlMem.putInt(columnCount + 1);
            ddlMem.putInt(metaMem.getInt(META_OFFSET_PARTITION_BY));
            ddlMem.putInt(metaMem.getInt(META_OFFSET_TIMESTAMP_INDEX));
            copyVersionAndLagValues();
            ddlMem.jumpTo(META_OFFSET_COLUMN_TYPES);
            for (int i = 0; i < columnCount; i++) {
                writeColumnEntry(i, i == replaceColumnIndex);
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

            if (dedupKeyFlag) {
                flags |= META_FLAG_BIT_DEDUP_KEY;
            }

            ddlMem.putLong(flags);
            ddlMem.putInt(indexValueBlockCapacity);
            ddlMem.putLong(configuration.getRandom().nextLong());

            // Write place where to put this column when reading the metadata.
            // The column can be replaced multiple times, find the very original index of the column
            if (replaceColumnIndex > -1) {
                int originColumnIndex = TableUtils.getReplacingColumnIndex(metaMem, replaceColumnIndex);
                if (originColumnIndex > -1) {
                    replaceColumnIndex = originColumnIndex;
                }
            }

            ddlMem.putInt(replaceColumnIndex > -1 ? replaceColumnIndex + 1 : 0);
            ddlMem.skip(4);

            long nameOffset = getColumnNameOffset(columnCount);
            for (int i = 0; i < columnCount; i++) {
                CharSequence columnName = metaMem.getStrA(nameOffset);
                ddlMem.putStr(columnName);
                nameOffset += Vm.getStorageLength(columnName);
            }
            ddlMem.putStr(name);
            ddlMem.sync(false);
        } finally {
            // truncate _meta file exactly, the file size never changes.
            // Metadata updates are written to a new file and then swapped by renaming.
            ddlMem.close(true, Vm.TRUNCATE_TO_POINTER);
        }
        return index;
    }

    private long applyFromWalLagToLastPartition(long commitToTimestamp, boolean allowPartial) {
        long lagMinTimestamp = txWriter.getLagMinTimestamp();
        if (!isDeduplicationEnabled()
                && txWriter.getLagRowCount() > 0
                && txWriter.isLagOrdered()
                && txWriter.getMaxTimestamp() <= lagMinTimestamp
                && txWriter.getPartitionTimestampByTimestamp(lagMinTimestamp) == lastPartitionTimestamp) {
            // There is some data in LAG, it's ordered, and it's already written to the last partition.
            // We can simply increase the last partition transient row count to make it committed.

            long lagMaxTimestamp = txWriter.getLagMaxTimestamp();
            commitToTimestamp = Math.min(commitToTimestamp, partitionTimestampHi);
            if (lagMaxTimestamp <= commitToTimestamp) {
                // Easy case, all lag data can be marked as committed in the last partition
                LOG.debug().$("fast apply full lag to last partition [table=").$(tableToken).I$();
                applyLagToLastPartition(lagMaxTimestamp, txWriter.getLagRowCount(), Long.MAX_VALUE);
                txWriter.setSeqTxn(txWriter.getSeqTxn() + txWriter.getLagTxnCount());
                txWriter.setLagTxnCount(0);
                txWriter.setLagRowCount(0);
                return lagMaxTimestamp;
            } else if (allowPartial && lagMinTimestamp <= commitToTimestamp) {
                // Find the max row which can be marked as committed in the last timestamp
                long lagRows = txWriter.getLagRowCount();
                long timestampMapOffset = txWriter.getTransientRowCount() * Long.BYTES;
                long timestampMapSize = lagRows * Long.BYTES;
                long timestampMaAddr = mapAppendColumnBuffer(
                        getPrimaryColumn(metadata.getTimestampIndex()),
                        timestampMapOffset,
                        timestampMapSize,
                        false
                );
                try {
                    final long timestampAddr = Math.abs(timestampMaAddr);
                    final long binarySearchInsertionPoint = Vect.binarySearch64Bit(
                            timestampAddr,
                            commitToTimestamp,
                            0,
                            lagRows - 1,
                            BinarySearch.SCAN_DOWN
                    );
                    long applyCount = (binarySearchInsertionPoint < 0) ? -binarySearchInsertionPoint - 1 : binarySearchInsertionPoint + 1;

                    long newMinLagTimestamp = Unsafe.getUnsafe().getLong(timestampAddr + applyCount * Long.BYTES);
                    long newMaxTimestamp = Unsafe.getUnsafe().getLong(timestampAddr + (applyCount - 1) * Long.BYTES);
                    assert newMinLagTimestamp > commitToTimestamp && commitToTimestamp >= newMaxTimestamp;

                    applyLagToLastPartition(newMaxTimestamp, (int) applyCount, newMinLagTimestamp);

                    LOG.debug().$("partial apply lag to last partition [table=").$(tableToken)
                            .$(" ,lagSize=").$(lagRows)
                            .$(" ,rowApplied=").$(applyCount)
                            .$(", commitToTimestamp=").$(commitToTimestamp)
                            .$(", newMaxTimestamp=").$(newMaxTimestamp)
                            .$(", newMinLagTimestamp=").$(newMinLagTimestamp)
                            .I$();
                    return newMaxTimestamp;
                } finally {
                    mapAppendColumnBufferRelease(timestampMaAddr, timestampMapOffset, timestampMapSize);
                }
            }
        }
        return Long.MIN_VALUE;
    }

    private boolean applyFromWalLagToLastPartitionPossible(long commitToTimestamp, long lagRowCount, boolean lagOrdered, long committedMaxTimestamp, long lagMinTimestamp, long lagMaxTimestamp) {
        return !isDeduplicationEnabled()
                && lagRowCount > 0
                && lagOrdered
                && committedMaxTimestamp <= lagMinTimestamp
                && txWriter.getPartitionTimestampByTimestamp(lagMinTimestamp) == lastPartitionTimestamp
                && lagMaxTimestamp <= Math.min(commitToTimestamp, partitionTimestampHi);
    }

    private void applyLagToLastPartition(long maxTimestamp, int lagRowCount, long lagMinTimestamp) {
        long initialTransientRowCount = txWriter.transientRowCount;
        txWriter.transientRowCount += lagRowCount;
        txWriter.updatePartitionSizeByTimestamp(lastPartitionTimestamp, txWriter.transientRowCount);
        txWriter.setMinTimestamp(Math.min(txWriter.getMinTimestamp(), txWriter.getLagMinTimestamp()));
        txWriter.setLagMinTimestamp(lagMinTimestamp);
        if (txWriter.getLagRowCount() == lagRowCount) {
            txWriter.setLagMaxTimestamp(Long.MIN_VALUE);
        }
        txWriter.setLagRowCount(txWriter.getLagRowCount() - lagRowCount);
        txWriter.setMaxTimestamp(maxTimestamp);
        if (indexCount > 0) {
            // To index correctly, we need to set append offset of symbol columns first.
            // So that re-indexing can read symbol values to the correct limits.
            final long newTransientRowCount = txWriter.getTransientRowCount();
            final int shl = ColumnType.pow2SizeOf(ColumnType.SYMBOL);
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                if (metadata.getColumnType(i) == ColumnType.SYMBOL && metadata.isColumnIndexed(i)) {
                    getPrimaryColumn(i).jumpTo(newTransientRowCount << shl);
                }
            }
            updateIndexesParallel(initialTransientRowCount, newTransientRowCount);
        }
        // set append position on columns so that the files are truncated to the correct size
        // if the partition is closed after the commit.
        // If wal commit terminates here, column positions should include lag to not truncate the WAL lag data.
        // Otherwise, lag will be copied out and ok to truncate to the transient row count.
        long partitionTruncateRowCount = txWriter.getTransientRowCount();
        setAppendPosition(partitionTruncateRowCount, false);
    }

    private boolean assertColumnPositionIncludeWalLag() {
        return txWriter.getLagRowCount() == 0
                || columns.get(getPrimaryColumnIndex(metadata.getTimestampIndex())).getAppendOffset() == (txWriter.getTransientRowCount() + txWriter.getLagRowCount()) * Long.BYTES;
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
            LOG.info().$("attaching partition with missing column [path=").$substr(pathRootSize, partitionPath).I$();
            columnVersionWriter.upsertColumnTop(partitionTimestamp, columnIndex, partitionSize);
        } else {
            long fileSize = ff.length(partitionPath.$());
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

    private void attachPartitionCheckFilesMatchVarSizeColumn(
            long partitionSize,
            long columnTop,
            String columnName,
            long columnNameTxn,
            Path partitionPath,
            long partitionTimestamp,
            int columnIndex,
            int columnType
    ) throws CairoException {
        long columnSize = partitionSize - columnTop;
        if (columnSize == 0) {
            return;
        }

        int pathLen = partitionPath.size();
        TableUtils.iFile(partitionPath, columnName, columnNameTxn);
        long indexLength = ff.length(partitionPath.$());
        if (indexLength > 0) {
            long indexFd = openRO(ff, partitionPath.$(), LOG);
            try {
                long fileSize = ff.length(indexFd);
                ColumnTypeDriver driver = ColumnType.getDriver(columnType);
                long expectedFileSize = driver.getAuxVectorSize(columnSize);
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
                    partitionPath.trimTo(pathLen);
                    TableUtils.dFile(partitionPath, columnName, columnNameTxn);
                    long dataLength = ff.length(partitionPath.$());
                    long prevDataAddress = dataLength;
                    for (long row = columnSize - 1; row >= 0; row--) {
                        long dataAddress = driver.getDataVectorOffset(mappedAddr, row);
                        if (dataAddress < 0 || dataAddress > dataLength) {
                            throw CairoException.critical(0).put("Variable size column has invalid data address value [path=").put(path)
                                    .put(", row=").put(row)
                                    .put(", dataAddress=").put(dataAddress)
                                    .put(", dataFileSize=").put(dataLength)
                                    .put(']');
                        }

                        // Check that addresses are monotonic
                        if (dataAddress > prevDataAddress) {
                            throw CairoException.critical(0).put("Variable size column has invalid data address value [path=").put(partitionPath)
                                    .put(", row=").put(row)
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

        int pathLen = partitionPath.size();
        TableUtils.dFile(partitionPath, columnName, columnNameTxn);
        if (!ff.exists(partitionPath.$())) {
            columnVersionWriter.upsertColumnTop(partitionTimestamp, columnIndex, partitionSize);
            return;
        }

        long fd = openRO(ff, partitionPath.$(), LOG);
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
                attachMetaMem.smallFile(ff, detachedPath.$(), MemoryTag.MMAP_TABLE_WRITER);
                attachMetadata = new TableWriterMetadata(tableToken, attachMetaMem);
            } else {
                attachMetaMem.smallFile(ff, detachedPath.$(), MemoryTag.MMAP_TABLE_WRITER);
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
            if (!ff.exists(detachedPath.$())) {
                // Backups and older versions of detached partitions will not have _cv
                LOG.error().$("detached _dcv file not found, skipping check [path=").$(detachedPath).I$();
                return false;
            } else {
                if (attachColumnVersionReader == null) {
                    attachColumnVersionReader = new ColumnVersionReader();
                }
                // attach partition is only possible on partitioned table
                attachColumnVersionReader.ofRO(ff, detachedPath.$());
                attachColumnVersionReader.readUnsafe();
            }

            // override column tops for the partition we are attaching
            columnVersionWriter.overrideColumnVersions(partitionTimestamp, attachColumnVersionReader);

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
                            .$(", column=").utf8(columnName)
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
                        removeFileOrLog(ff, detachedPath.$());
                        valueFileName(detachedPath.trimTo(detachedPartitionRoot), columnName, columnNameTxn);
                        removeFileOrLog(ff, detachedPath.$());
                    } else if (isIndexedNow
                            && (!wasIndexedAtDetached || indexValueBlockCapacityNow != indexValueBlockCapacityDetached)) {
                        // Was not indexed before or value block capacity has changed
                        detachedPath.trimTo(detachedPartitionRoot);
                        rebuildAttachedPartitionColumnIndex(partitionTimestamp, partitionSize, columnName);
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
        int rootLen = partitionPath.size();
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
                    final int type = ColumnType.tagOf(columnType);
                    if (ColumnType.isSymbol(type)) {
                        attachPartitionCheckSymbolColumn(partitionSize, columnTop, columnName, columnNameTxn, partitionPath, partitionTimestamp, columnIndex);
                    } else if (ColumnType.isVarSize(type)) {
                        attachPartitionCheckFilesMatchVarSizeColumn(partitionSize, columnTop, columnName, columnNameTxn, partitionPath, partitionTimestamp, columnIndex, columnType);
                    } else if (ColumnType.isFixedSize(type)) {
                        attachPartitionCheckFilesMatchFixedColumn(columnType, partitionSize, columnTop, columnName, columnNameTxn, partitionPath, partitionTimestamp, columnIndex);
                    } else {
                        assert false;
                    }
                }
            } finally {
                partitionPath.trimTo(rootLen);
            }
        }
    }

    private void bumpColumnStructureVersion() {
        columnVersionWriter.commit();
        txWriter.setColumnVersion(columnVersionWriter.getVersion());
        txWriter.bumpColumnStructureVersion(this.denseSymbolMapWriters);
        assert txWriter.getMetadataVersion() == metadata.getMetadataVersion();
    }

    private void bumpMasterRef() {
        if ((masterRef & 1) == 0) {
            masterRef++;
        } else {
            cancelRowAndBump();
        }
    }

    private void bumpMetadataAndColumnStructureVersion() {
        columnVersionWriter.commit();
        txWriter.setColumnVersion(columnVersionWriter.getVersion());
        txWriter.bumpMetadataAndColumnStructureVersion(this.denseSymbolMapWriters);
        assert txWriter.getMetadataVersion() == metadata.getMetadataVersion();
    }

    private void bumpMetadataVersion() {
        columnVersionWriter.commit();
        txWriter.setColumnVersion(columnVersionWriter.getVersion());
        txWriter.bumpMetadataVersion(this.denseSymbolMapWriters);
        assert txWriter.getMetadataVersion() == metadata.getMetadataVersion();
    }

    private boolean canSquashOverwritePartitionTail(int partitionIndex) {
        long fromTxn = txWriter.getPartitionNameTxn(partitionIndex);
        if (fromTxn < 0) {
            fromTxn = 0;
        }
        long toTxn = txWriter.getTxn();
        if (partitionIndex + 1 < txWriter.getPartitionCount()) {
            // If next partition is a split partition part of same logical partition
            // for example if the partition is '2020-01-01' and the next partition is '2020-01-01T12.3'
            // then if there are no readers between transaction range [0, 3) the partition is unlocked to append.
            if (txWriter.getLogicalPartitionTimestamp(txWriter.getPartitionTimestampByIndex(partitionIndex)) ==
                    txWriter.getLogicalPartitionTimestamp(txWriter.getPartitionTimestampByIndex(partitionIndex + 1))) {
                toTxn = Math.max(fromTxn + 1, getPartitionNameTxn(partitionIndex + 1) + 1);
            }
        }

        return txnScoreboard.isRangeAvailable(fromTxn, toTxn);
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
                        .setOutOfMemory(o3oomObserved)
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
            todoMem.sync(false);
        } finally {
            path.trimTo(pathSize);
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

    private void closeWalColumns(boolean isLastSegmentUsage, long walSegmentId) {
        int key = walFdCache.keyIndex(walSegmentId);
        boolean cacheIsFull = !isLastSegmentUsage && key > -1 && walFdCacheSize == configuration.getWalMaxSegmentFileDescriptorsCache();
        if (isLastSegmentUsage || cacheIsFull) {
            if (key < 0) {
                LongList fds = walFdCache.valueAt(key);
                walFdCache.removeAt(key);
                walFdCacheSize--;
                fds.clear();
                walFdCacheListPool.push(fds);
            }

            for (int col = 0, n = walMappedColumns.size(); col < n; col++) {
                MemoryCMOR mappedColumnMem = walMappedColumns.getQuick(col);
                if (mappedColumnMem != null) {
                    Misc.free(mappedColumnMem);
                    walColumnMemoryPool.push(mappedColumnMem);
                }
            }
        } else {
            LongList fds = null;
            if (key > -1) {
                // Add FDs to a new FD cache list
                fds = walFdCacheListPool.pop();
                walFdCache.putAt(key, walSegmentId, fds);
                walFdCacheSize++;
            }

            for (int col = 0, n = walMappedColumns.size(); col < n; col++) {
                MemoryCMOR mappedColumnMem = walMappedColumns.getQuick(col);
                if (mappedColumnMem != null) {
                    long fd = mappedColumnMem.detachFdClose();
                    if (fds != null) {
                        fds.add(fd);
                    }
                    walColumnMemoryPool.push(mappedColumnMem);
                }
            }
        }

        if (cacheIsFull) {
            // Close all cached FDs.
            // It is possible to use more complicated algo and evict only those which
            // will not be used in the near future, but it's non-trivial
            // and can ruin the benefit of caching any FDs.
            // This supposed to happen rarely.
            closeWalFiles();
        }
    }

    private void closeWalFiles() {
        walFdCache.forEach(walFdCloseCachedFdAction);
        walFdCache.clear();
        walFdCacheSize = 0;
    }

    /**
     * Commits newly added rows of data. This method updates transaction file with pointers to end of appended data.
     * <p>
     * <b>Pending rows</b>
     * <p>This method will cancel pending rows by calling {@link #rowCancel()}. Data in partially appended row will be lost.</p>
     *
     * @param o3MaxLag if > 0 then do a partial commit, leaving the rows within the lag in a new uncommitted transaction
     * @return commit transaction number or -1 if there was nothing to commit
     */
    private long commit(long o3MaxLag) {
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
                    if (reconcileOptimisticPartitions()) {
                        this.lastPartitionTimestamp = txWriter.getLastPartitionTimestamp();
                        this.partitionTimestampHi = txWriter.getNextPartitionTimestamp(txWriter.getMaxTimestamp()) - 1;
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


            final long committedRowCount = txWriter.unsafeCommittedFixedRowCount() + txWriter.unsafeCommittedTransientRowCount();
            final long rowsAdded = txWriter.getRowCount() - committedRowCount;

            updateIndexes();
            syncColumns();
            columnVersionWriter.commit();
            txWriter.setColumnVersion(columnVersionWriter.getVersion());
            txWriter.commit(denseSymbolMapWriters);

            // Check if partitions are split into too many pieces and merge few of them back.
            squashSplitPartitions(minSplitPartitionTimestamp, txWriter.getMaxTimestamp(), configuration.getO3LastPartitionMaxSplits());

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
            initLastPartition(this.txWriter.getMaxTimestamp());
            if (partitioned) {
                partitionTimestampHi = txWriter.getNextPartitionTimestamp(txWriter.getMaxTimestamp()) - 1;
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
        final MemoryMA dataMem;
        final MemoryMA auxMem;
        final MemoryCARW o3DataMem1;
        final MemoryCARW o3AuxMem1;
        final MemoryCARW o3DataMem2;
        final MemoryCARW o3AuxMem2;

        if (type > 0) {
            dataMem = Vm.getMAInstance(configuration.getCommitMode());
            o3DataMem1 = Vm.getCARWInstance(o3ColumnMemorySize, configuration.getO3MemMaxPages(), MemoryTag.NATIVE_O3);
            o3DataMem2 = Vm.getCARWInstance(o3ColumnMemorySize, configuration.getO3MemMaxPages(), MemoryTag.NATIVE_O3);

            if (ColumnType.isVarSize(type)) {
                auxMem = Vm.getMAInstance(configuration.getCommitMode());
                o3AuxMem1 = Vm.getCARWInstance(o3ColumnMemorySize, configuration.getO3MemMaxPages(), MemoryTag.NATIVE_O3);
                o3AuxMem2 = Vm.getCARWInstance(o3ColumnMemorySize, configuration.getO3MemMaxPages(), MemoryTag.NATIVE_O3);
            } else {
                auxMem = null;
                o3AuxMem1 = null;
                o3AuxMem2 = null;
            }
        } else {
            dataMem = auxMem = NullMemory.INSTANCE;
            o3DataMem1 = o3AuxMem1 = o3DataMem2 = o3AuxMem2 = NullMemory.INSTANCE;
        }

        int baseIndex = getPrimaryColumnIndex(index);
        columns.extendAndSet(baseIndex, dataMem);
        columns.extendAndSet(baseIndex + 1, auxMem);
        o3MemColumns1.extendAndSet(baseIndex, o3DataMem1);
        o3MemColumns1.extendAndSet(baseIndex + 1, o3AuxMem1);
        o3MemColumns2.extendAndSet(baseIndex, o3DataMem2);
        o3MemColumns2.extendAndSet(baseIndex + 1, o3AuxMem2);
        configureNullSetters(nullSetters, type, dataMem, auxMem);
        configureNullSetters(o3NullSetters1, type, o3DataMem1, o3AuxMem1);
        configureNullSetters(o3NullSetters2, type, o3DataMem2, o3AuxMem2);

        if (indexFlag && type > 0) {
            indexers.extendAndSet(index, new SymbolColumnIndexer(configuration));
        }
        rowValueIsNotNull.add(0);
    }

    private void configureColumnMemory() {
        symbolMapWriters.setPos(columnCount);
        int dedupColCount = 0;
        for (int i = 0; i < columnCount; i++) {
            int type = metadata.getColumnType(i);
            configureColumn(type, metadata.isColumnIndexed(i), i);

            if (type > -1) {
                if (ColumnType.isSymbol(type)) {
                    final int symbolIndex = denseSymbolMapWriters.size();
                    long columnNameTxn = columnVersionWriter.getDefaultColumnNameTxn(i);
                    SymbolMapWriter symbolMapWriter = new SymbolMapWriter(
                            configuration,
                            path.trimTo(pathSize),
                            metadata.getColumnName(i),
                            columnNameTxn,
                            txWriter.getSymbolValueCount(symbolIndex),
                            symbolIndex,
                            txWriter
                    );

                    symbolMapWriters.extendAndSet(i, symbolMapWriter);
                    denseSymbolMapWriters.add(symbolMapWriter);
                }
                if (metadata.isDedupKey(i)) {
                    dedupColCount++;
                }
            }
        }
        if (isDeduplicationEnabled()) {
            dedupColumnCommitAddresses = new DedupColumnCommitAddresses();
            // Set dedup column count, excluding designated timestamp
            dedupColumnCommitAddresses.setDedupColumnCount(dedupColCount - 1);
        }
        final int timestampIndex = metadata.getTimestampIndex();
        if (timestampIndex != -1) {
            o3TimestampMem = o3MemColumns1.getQuick(getPrimaryColumnIndex(timestampIndex));
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
            o3NullSetters1.setQuick(index, NOOP);
            o3NullSetters2.setQuick(index, NOOP);
            timestampSetter = getPrimaryColumn(index)::putLong;
        }
    }

    private void consumeColumnTasks(RingQueue<ColumnTask> queue, int queuedCount) {
        // This is work stealing, can run tasks from other table writers
        final Sequence subSeq = this.messageBus.getColumnTaskSubSeq();
        consumeColumnTasks0(queue, queuedCount, subSeq, o3DoneLatch);
        checkO3Errors();
    }

    private int copyMetadataAndSetIndexAttrs(int columnIndex, boolean indexedFlag, int indexValueBlockSize) {
        try {
            int index = openMetaSwapFile(ff, ddlMem, path, pathSize, configuration.getMaxSwapFileCount());
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
                    long flags = getColumnFlags(metaMem, columnIndex);
                    if (indexedFlag) {
                        flags |= META_FLAG_BIT_INDEXED;
                    } else {
                        flags &= ~META_FLAG_BIT_INDEXED;
                    }
                    ddlMem.putLong(flags);
                    ddlMem.putInt(indexValueBlockSize);
                    ddlMem.skip(16);
                }
            }

            long nameOffset = getColumnNameOffset(columnCount);
            for (int i = 0; i < columnCount; i++) {
                CharSequence columnName = metaMem.getStrA(nameOffset);
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
            int index = openMetaSwapFile(ff, ddlMem, path, pathSize, configuration.getMaxSwapFileCount());
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
                CharSequence columnName = metaMem.getStrA(nameOffset);
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
        int res = ff.copy(other.$(), to.$());
        if (Os.isWindows() && res == -1 && ff.errno() == Files.WINDOWS_ERROR_FILE_EXISTS) {
            // Windows throws an error the destination file already exists, other platforms do not
            if (!ff.removeQuiet(to.$())) {
                // If file is open, return here so that errno is 5 in the error message
                return -1;
            }
            return ff.copy(other.$(), to.$());
        }
        return res;
    }

    private void copyVersionAndLagValues() {
        ddlMem.putInt(ColumnType.VERSION);
        ddlMem.putInt(metaMem.getInt(META_OFFSET_TABLE_ID));
        ddlMem.putInt(metaMem.getInt(META_OFFSET_MAX_UNCOMMITTED_ROWS));
        ddlMem.putLong(metaMem.getLong(META_OFFSET_O3_MAX_LAG));
        ddlMem.putLong(txWriter.getMetadataVersion() + 1);
        ddlMem.putBool(metaMem.getBool(META_OFFSET_WAL_ENABLED));
        metadata.setMetadataVersion(txWriter.getMetadataVersion() + 1);
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

            if (!force && ff.exists(path.$())) {
                return;
            } else {
                ff.removeQuiet(path.$());
            }

            // reuse memory column object to create index and close it at the end
            try {
                ddlMem.smallFile(ff, path.$(), MemoryTag.MMAP_TABLE_WRITER);
                ddlMem.truncate();
                BitmapIndexWriter.initKeyMemory(ddlMem, indexValueBlockCapacity);
            } catch (CairoException e) {
                // looks like we could not create key file properly
                // lets not leave half-baked file sitting around
                LOG.error()
                        .$("could not create index [name=").$(path)
                        .$(", errno=").$(e.getErrno())
                        .I$();
                if (!ff.removeQuiet(path.$())) {
                    LOG.critical()
                            .$("could not remove '").$(path).$("'. Please remove MANUALLY.")
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
        // In case there are some dirty files left from rolled back transaction
        // clean the newly created symbol files.
        w.truncate();
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

    private void cthAppendWalColumnToLastPartition(
            int columnIndex,
            int columnType,
            long timestampColumnIndex,
            long copyRowCount,
            long ignore,
            long columnRowCount,
            long existingLagRows,
            long symbolsFlags
    ) {
        if (o3ErrorCount.get() > 0) {
            return;
        }
        try {
            boolean designatedTimestamp = columnIndex == timestampColumnIndex;
            MemoryCR o3SrcDataMem = o3Columns.get(getPrimaryColumnIndex(columnIndex));
            MemoryCR o3srcAuxMem = o3Columns.get(getSecondaryColumnIndex(columnIndex));
            MemoryMA dstDataMem = columns.get(getPrimaryColumnIndex(columnIndex));
            MemoryMA dstAuxMem = columns.get(getSecondaryColumnIndex(columnIndex));
            long dstRowCount = txWriter.getTransientRowCount() - getColumnTop(columnIndex) + existingLagRows;

            long dataVectorCopySize;
            long o3srcDataOffset;
            long o3dstDataOffset;
            if (ColumnType.isVarSize(columnType)) {
                // Var dataVectorCopySize column
                final ColumnTypeDriver columnTypeDriver = ColumnType.getDriver(columnType);

                final long committedAuxOffset = columnTypeDriver.getAuxVectorOffset(columnRowCount);
                final long o3srcAuxMemAddr = o3srcAuxMem.addressOf(0);
                o3srcDataOffset = columnTypeDriver.getDataVectorOffset(o3srcAuxMemAddr, columnRowCount);
                dataVectorCopySize = columnTypeDriver.getDataVectorSize(o3srcAuxMemAddr, columnRowCount, columnRowCount + copyRowCount - 1);

                final long o3dstAuxOffset = columnTypeDriver.getAuxVectorOffset(dstRowCount);
                final long o3dstAuxSize = columnTypeDriver.getAuxVectorSize(copyRowCount);

                if (o3dstAuxOffset > 0) {
                    o3dstDataOffset = dstDataMem.getAppendOffset();
                } else {
                    o3dstDataOffset = 0;
                }

                // move count + 1 rows, to make sure index column remains n+1
                // the data is copied back to start of the buffer, no need to set dataVectorCopySize first
                long o3dstAuxAddr = mapAppendColumnBuffer(dstAuxMem, o3dstAuxOffset, o3dstAuxSize, true);
                assert o3dstAuxAddr != 0;
                try {
                    final long shift = o3srcDataOffset - o3dstDataOffset;
                    columnTypeDriver.shiftCopyAuxVector(
                            shift,
                            o3srcAuxMem.addressOf(committedAuxOffset),
                            0,
                            copyRowCount - 1, // inclusive
                            Math.abs(o3dstAuxAddr),
                            o3dstAuxSize
                    );
                } finally {
                    mapAppendColumnBufferRelease(o3dstAuxAddr, o3dstAuxOffset, o3dstAuxSize);
                }
            } else {
                // Fixed dataVectorCopySize column
                final int shl = ColumnType.pow2SizeOf(columnType);
                o3srcDataOffset = designatedTimestamp ? columnRowCount << 4 : columnRowCount << shl;
                dataVectorCopySize = copyRowCount << shl;
                o3dstDataOffset = dstRowCount << shl;
            }

            dstDataMem.jumpTo(o3dstDataOffset + dataVectorCopySize);

            // data vector size could be 0 for some inlined varsize column types
            if (!designatedTimestamp && dataVectorCopySize > 0) {
                if (mixedIOFlag) {
                    if (o3SrcDataMem.isFileBased()) {
                        long bytesWritten = ff.copyData(o3SrcDataMem.getFd(), dstDataMem.getFd(), o3srcDataOffset, o3dstDataOffset, dataVectorCopySize);
                        if (bytesWritten != dataVectorCopySize) {
                            throw CairoException.critical(ff.errno())
                                    .put("could not copy WAL column (fd-fd) [dstFd=").put(dstDataMem.getFd())
                                    .put(", column=").put(getColumnNameSafe(columnIndex))
                                    .put(", o3dstDataOffset=").put(o3dstDataOffset)
                                    .put(", srcFd=").put(o3SrcDataMem.getFd())
                                    .put(", dataVectorCopySize=").put(dataVectorCopySize)
                                    .put(", bytesWritten=").put(bytesWritten)
                                    .put(']');
                        }
                    } else {
                        long bytesWritten = ff.write(dstDataMem.getFd(), o3SrcDataMem.addressOf(o3srcDataOffset), dataVectorCopySize, o3dstDataOffset);
                        if (bytesWritten != dataVectorCopySize) {
                            throw CairoException.critical(ff.errno())
                                    .put("could not copy WAL column (mem-fd) [fd=").put(dstDataMem.getFd())
                                    .put(", column=").put(getColumnNameSafe(columnIndex))
                                    .put(", columnType").put(ColumnType.nameOf(columnType))
                                    .put(", o3dstDataOffset=").put(o3dstDataOffset)
                                    .put(", o3srcDataOffset=").put(o3srcDataOffset)
                                    .put(", dataVectorCopySize=").put(dataVectorCopySize)
                                    .put(", bytesWritten=").put(bytesWritten)
                                    .put(']');
                        }
                    }
                } else {
                    long destAddr = mapAppendColumnBuffer(dstDataMem, o3dstDataOffset, dataVectorCopySize, true);
                    try {
                        Vect.memcpy(Math.abs(destAddr), o3SrcDataMem.addressOf(o3srcDataOffset), dataVectorCopySize);
                    } finally {
                        mapAppendColumnBufferRelease(destAddr, o3dstDataOffset, dataVectorCopySize);
                    }
                }
            } else if (designatedTimestamp) {
                // WAL format has timestamp written as 2 LONGs per record, in so-called timestamp index data structure.
                // There is no point storing in 2 LONGs per record the LAG it is enough to have 1 LONG with timestamp.
                // The sort will convert the format back to timestamp index data structure.
                long srcLo = o3SrcDataMem.addressOf(o3srcDataOffset);
                // timestamp size must not be 0
                long destAddr = mapAppendColumnBuffer(dstDataMem, o3dstDataOffset, dataVectorCopySize, true);
                try {
                    Vect.copyFromTimestampIndex(srcLo, 0, copyRowCount - 1, Math.abs(destAddr));
                } finally {
                    mapAppendColumnBufferRelease(destAddr, o3dstDataOffset, dataVectorCopySize);
                }
            }
        } catch (Throwable th) {
            handleColumnTaskException(
                    "could not copy WAL column",
                    columnIndex,
                    columnType,
                    copyRowCount,
                    columnRowCount,
                    existingLagRows,
                    symbolsFlags,
                    th
            );
        }
    }

    private void cthMergeWalColumnWithLag(int columnIndex, int columnType, long timestampColumnIndex, long mergedTimestampAddress, long mergeCount, long lagRows, long mappedRowLo, long mappedRowHi) {
        if (ColumnType.isVarSize(columnType)) {
            cthMergeWalVarColumnWithLag(columnIndex, columnType, mergedTimestampAddress, mergeCount, lagRows, mappedRowLo, mappedRowHi);
        } else if (columnIndex != timestampColumnIndex) {
            // do not merge timestamp columns
            cthMergeWalFixColumnWithLag(columnIndex, columnType, mergedTimestampAddress, mergeCount, lagRows, mappedRowLo, mappedRowHi);
        }
    }

    private void cthMergeWalFixColumnWithLag(int columnIndex, int columnType, long mergeIndex, long mergeCount, long lagRows, long mappedRowLo, long mappedRowHi) {
        if (o3ErrorCount.get() > 0) {
            return;
        }
        try {
            final int primaryColumnIndex = getPrimaryColumnIndex(columnIndex);
            final MemoryMA lagMem = columns.getQuick(primaryColumnIndex);
            final MemoryCR mappedMem = o3Columns.getQuick(primaryColumnIndex);
            final MemoryCARW destMem = o3MemColumns2.getQuick(primaryColumnIndex);

            final int shl = ColumnType.pow2SizeOf(columnType);
            destMem.jumpTo(mergeCount << shl);
            final long srcMapped = mappedMem.addressOf(mappedRowLo << shl) - (mappedRowLo << shl);
            long lagMemOffset = (txWriter.getTransientRowCount() - getColumnTop(columnIndex)) << shl;
            long lagAddr = mapAppendColumnBuffer(lagMem, lagMemOffset, lagRows << shl, false);
            try {
                long srcLag = Math.abs(lagAddr);
                destMem.shiftAddressRight(0);
                final long dest = destMem.addressOf(0);
                if (srcLag == 0 && lagRows != 0) {
                    throw CairoException.critical(0)
                            .put("cannot sort WAL data, lag rows are missing [table").put(tableToken.getTableName())
                            .put(", column=").put(getColumnNameSafe(columnIndex))
                            .put(", type=").put(ColumnType.nameOf(columnType))
                            .put(", lagRows=").put(lagRows)
                            .put(']');
                }
                if (srcMapped == 0) {
                    throw CairoException.critical(0)
                            .put("cannot sort WAL data, rows are missing [table").put(tableToken.getTableName())
                            .put(", column=").put(getColumnNameSafe(columnIndex))
                            .put(", type=").put(ColumnType.nameOf(columnType))
                            .put(']');
                }
                if (dest == 0) {
                    throw CairoException.critical(0)
                            .put("cannot sort WAL data, destination buffer is empty [table").put(tableToken.getTableName())
                            .put(", column=").put(getColumnNameSafe(columnIndex))
                            .put(", type=").put(ColumnType.nameOf(columnType))
                            .put(']');
                }

                switch (shl) {
                    case 0:
                        Vect.mergeShuffle8Bit(srcLag, srcMapped, dest, mergeIndex, mergeCount);
                        break;
                    case 1:
                        Vect.mergeShuffle16Bit(srcLag, srcMapped, dest, mergeIndex, mergeCount);
                        break;
                    case 2:
                        Vect.mergeShuffle32Bit(srcLag, srcMapped, dest, mergeIndex, mergeCount);
                        break;
                    case 3:
                        Vect.mergeShuffle64Bit(srcLag, srcMapped, dest, mergeIndex, mergeCount);
                        break;
                    case 4:
                        Vect.mergeShuffle128Bit(srcLag, srcMapped, dest, mergeIndex, mergeCount);
                        break;
                    case 5:
                        Vect.mergeShuffle256Bit(srcLag, srcMapped, dest, mergeIndex, mergeCount);
                        break;
                    default:
                        assert false : "col type is unsupported";
                        break;
                }
            } finally {
                mapAppendColumnBufferRelease(lagAddr, lagMemOffset, lagRows << shl);
            }
        } catch (Throwable e) {
            handleColumnTaskException(
                    "could not merge fix WAL column",
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

    private void cthMergeWalVarColumnWithLag(
            int columnIndex,
            int columnType,
            long timestampMergeIndexAddr,
            long timestampMergeIndexCount,
            long lagRows,
            long mappedRowLo,
            long mappedRowHi
    ) {
        if (o3ErrorCount.get() > 0) {
            return;
        }
        try {
            final int primaryIndex = getPrimaryColumnIndex(columnIndex);
            final int secondaryIndex = primaryIndex + 1;

            final MemoryCR o3dataMem = o3Columns.getQuick(primaryIndex);
            final MemoryCR o3auxMem = o3Columns.getQuick(secondaryIndex);
            final MemoryMA lagDataMem = columns.getQuick(primaryIndex);
            final MemoryMA lagAuxMem = columns.getQuick(secondaryIndex);

            final MemoryCARW dstDataAddr = o3MemColumns2.getQuick(primaryIndex);
            final MemoryCARW dstAuxAddr = o3MemColumns2.getQuick(secondaryIndex);

            ColumnTypeDriver columnTypeDriver = ColumnType.getDriver(columnType);

            final long srcMappedDataAddr = o3dataMem.addressOf(0);
            final long srcMappedAuxAddr = o3auxMem.addressOf(0);

            final long src1DataSize = columnTypeDriver.getDataVectorSize(srcMappedAuxAddr, mappedRowLo, mappedRowHi - 1);
            assert o3dataMem.size() >= src1DataSize;
            final long lagAuxOffset = columnTypeDriver.getAuxVectorOffset(txWriter.getTransientRowCount() - getColumnTop(columnIndex));
            final long lagAuxSize = columnTypeDriver.getAuxVectorSize(lagRows);
            final long signedLagAuxAddr = lagRows > 0 ? mapAppendColumnBuffer(lagAuxMem, lagAuxOffset, lagAuxSize, false) : 0;

            try {
                final long lagAuxAddr = Math.abs(signedLagAuxAddr);
                final long lagDataBegin = lagRows > 0 ? columnTypeDriver.getDataVectorOffset(lagAuxAddr, 0) : 0;
                final long lagDataSize = lagRows > 0 ? columnTypeDriver.getDataVectorSizeAt(lagAuxAddr, lagRows - 1) : 0;
                final long lagDataMapAddr = lagRows > 0 ? mapAppendColumnBuffer(lagDataMem, lagDataBegin, lagDataSize, false) : 0;

                try {
                    final long lagDataAddr = Math.abs(lagDataMapAddr) - lagDataBegin;
                    dstDataAddr.jumpTo(src1DataSize + lagDataSize);
                    dstAuxAddr.jumpTo(columnTypeDriver.getAuxVectorSize(timestampMergeIndexCount));

                    // exclude the trailing offset from shuffling
                    ColumnType.getDriver(columnType).o3ColumnMerge(
                            timestampMergeIndexAddr,
                            timestampMergeIndexCount,
                            lagAuxAddr,
                            lagDataAddr,
                            srcMappedAuxAddr,
                            srcMappedDataAddr,
                            dstAuxAddr.addressOf(0),
                            dstDataAddr.addressOf(0),
                            0L
                    );
                } finally {
                    mapAppendColumnBufferRelease(lagDataMapAddr, lagDataBegin, lagDataSize);
                }
            } finally {
                mapAppendColumnBufferRelease(signedLagAuxAddr, lagAuxOffset, lagAuxSize);
            }
        } catch (Throwable e) {
            handleColumnTaskException(
                    "could not merge varsize WAL column",
                    columnIndex,
                    columnType,
                    timestampMergeIndexAddr,
                    lagRows,
                    mappedRowLo,
                    mappedRowHi,
                    e
            );
        }
    }

    private void cthO3MoveUncommitted(
            int columnIndex,
            int columnType,
            long timestampColumnIndex,
            long committedTransientRowCount,
            long ignore1,
            long transientRowsAdded,
            long ignore2,
            long ignore3
    ) {
        if (o3ErrorCount.get() > 0) {
            return;
        }
        try {
            if (columnIndex != timestampColumnIndex) {
                MemoryMA colDataMem = getPrimaryColumn(columnIndex);
                long colDataOffset;
                final MemoryARW o3DataMem = o3MemColumns1.get(getPrimaryColumnIndex(columnIndex));
                final MemoryARW o3auxMem = o3MemColumns1.get(getSecondaryColumnIndex(columnIndex));

                long colDataExtraSize;
                long o3dataOffset = o3DataMem.getAppendOffset();

                final long columnTop = getColumnTop(columnIndex);

                if (columnTop > 0) {
                    LOG.debug()
                            .$("move uncommitted [columnTop=").$(columnTop)
                            .$(", columnIndex=").$(columnIndex)
                            .$(", committedTransientRowCount=").$(committedTransientRowCount)
                            .$(", transientRowsAdded=").$(transientRowsAdded)
                            .I$();
                }

                final long committedRowCount = committedTransientRowCount - columnTop;
                if (ColumnType.isVarSize(columnType)) {
                    final ColumnTypeDriver columnTypeDriver = ColumnType.getDriver(columnType);
                    final MemoryMA colAuxMem = getSecondaryColumn(columnIndex);
                    final long colAuxMemOffset = columnTypeDriver.getAuxVectorOffset(committedRowCount);
                    long colAuxMemRequiredSize = columnTypeDriver.getAuxVectorSize(transientRowsAdded);
                    long o3auxMemAppendOffset = o3auxMem.getAppendOffset();

                    // ensure memory is available
                    long offsetLimit = o3auxMemAppendOffset + columnTypeDriver.getAuxVectorOffset(transientRowsAdded);
                    o3auxMem.jumpTo(offsetLimit);
                    long colAuxMemAddr = colAuxMem.map(colAuxMemOffset, colAuxMemRequiredSize);
                    boolean locallyMapped = colAuxMemAddr == 0;

                    long alignedExtraLen;
                    if (!locallyMapped) {
                        alignedExtraLen = 0;
                    } else {
                        // Linux requires the mmap offset to be page aligned
                        final long alignedOffset = Files.floorPageSize(colAuxMemOffset);
                        alignedExtraLen = colAuxMemOffset - alignedOffset;
                        colAuxMemAddr = mapRO(ff, colAuxMem.getFd(), colAuxMemRequiredSize + alignedExtraLen, alignedOffset, MemoryTag.MMAP_TABLE_WRITER);
                    }

                    colDataOffset = columnTypeDriver.getDataVectorOffset(colAuxMemAddr + alignedExtraLen, 0);
                    long dstAddr = o3auxMem.addressOf(o3auxMemAppendOffset) - columnTypeDriver.getMinAuxVectorSize();
                    long dstAddrLimit = o3auxMem.addressOf(offsetLimit);
                    long dstAddrSize = dstAddrLimit - dstAddr;

                    columnTypeDriver.shiftCopyAuxVector(
                            colDataOffset - o3dataOffset,
                            colAuxMemAddr + alignedExtraLen,
                            0,
                            transientRowsAdded - 1, // inclusive
                            dstAddr,
                            dstAddrSize
                    );

                    if (locallyMapped) {
                        // If memory mapping was mapped specially for this move, close it
                        ff.munmap(colAuxMemAddr, colAuxMemRequiredSize + alignedExtraLen, MemoryTag.MMAP_TABLE_WRITER);
                    }

                    colDataExtraSize = colDataMem.getAppendOffset() - colDataOffset;
                    // we have to restore aux column size to its required size to hold "committedRowCount" row count.
                    colAuxMem.jumpTo(columnTypeDriver.getAuxVectorSize(committedRowCount));
                } else {
                    // Fixed size
                    final int shl = ColumnType.pow2SizeOf(columnType);
                    colDataExtraSize = transientRowsAdded << shl;
                    colDataOffset = committedRowCount << shl;
                }

                o3DataMem.jumpTo(o3dataOffset + colDataExtraSize);
                long o3dataAddr = o3DataMem.addressOf(o3dataOffset);
                long sourceAddress = colDataMem.map(colDataOffset, colDataExtraSize);
                if (sourceAddress != 0) {
                    Vect.memcpy(o3dataAddr, sourceAddress, colDataExtraSize);
                } else {
                    // Linux requires the mmap offset to be page aligned
                    long alignedOffset = Files.floorPageSize(colDataOffset);
                    long alignedExtraLen = colDataOffset - alignedOffset;
                    long size = colDataExtraSize + alignedExtraLen;
                    if (size > 0) {
                        sourceAddress = mapRO(ff, colDataMem.getFd(), size, alignedOffset, MemoryTag.MMAP_TABLE_WRITER);
                        Vect.memcpy(o3dataAddr, sourceAddress + alignedExtraLen, colDataExtraSize);
                        ff.munmap(sourceAddress, size, MemoryTag.MMAP_TABLE_WRITER);
                    }
                }
                colDataMem.jumpTo(colDataOffset);
            } else {
                // Timestamp column
                int shl = ColumnType.pow2SizeOf(ColumnType.TIMESTAMP);
                MemoryMA srcDataMem = getPrimaryColumn(columnIndex);
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
            handleColumnTaskException(
                    "could not move uncommitted data",
                    columnIndex,
                    columnType,
                    committedTransientRowCount,
                    transientRowsAdded,
                    ignore1,
                    ignore2,
                    ex
            );
        }
    }

    private void cthO3ShiftColumnInLagToTop(
            int columnIndex,
            int columnType,
            long timestampColumnIndex,
            long copyToLagRowCount,
            long ignore,
            long columnDataRowOffset,
            long existingLagRows,
            long excludeSymbols
    ) {
        if (o3ErrorCount.get() > 0) {
            return;
        }
        try {
            if (columnIndex != timestampColumnIndex) {
                MemoryCR srcDataMem = o3Columns.get(getPrimaryColumnIndex(columnIndex));
                MemoryCR srcAuxMem = o3Columns.get(getSecondaryColumnIndex(columnIndex));
                MemoryARW dstDataMem = o3MemColumns1.get(getPrimaryColumnIndex(columnIndex));
                MemoryARW dstAuxMem = o3MemColumns1.get(getSecondaryColumnIndex(columnIndex));

                if (srcDataMem == dstDataMem && excludeSymbols > 0 && columnType == ColumnType.SYMBOL) {
                    // nothing to do. This is the case when WAL symbols are remapped to the correct place in LAG buffers.
                    return;
                }

                if (ColumnType.isVarSize(columnType)) {
                    final ColumnTypeDriver columnTypeDriver = ColumnType.getDriver(columnType);
                    final long dataOffset = columnTypeDriver.getDataVectorOffset(srcAuxMem.addressOf(0), columnDataRowOffset);
                    final long dataSize = columnTypeDriver.getDataVectorSize(srcAuxMem.addressOf(0), columnDataRowOffset, columnDataRowOffset + copyToLagRowCount - 1);
                    final long destOffset = existingLagRows == 0 ? 0L : columnTypeDriver.getDataVectorOffset(dstAuxMem.addressOf(0), existingLagRows);

                    // adjust append position of the index column to
                    // maintain n+1 number of entries
                    long rowLimit = columnTypeDriver.getAuxVectorSize(existingLagRows + copyToLagRowCount);
                    dstAuxMem.jumpTo(rowLimit);

                    // move count + 1 rows, to make sure index column remains n+1
                    // the data is copied back to start of the buffer, no need to set dataSize first
                    long dstAddr = dstAuxMem.addressOf(columnTypeDriver.getAuxVectorOffset(existingLagRows));
                    long dstAddrLimit = dstAuxMem.addressOf(rowLimit);
                    long dstAddrSize = dstAddrLimit - dstAddr;
                    columnTypeDriver.shiftCopyAuxVector(
                            dataOffset - destOffset,
                            srcAuxMem.addressOf(columnTypeDriver.getAuxVectorOffset(columnDataRowOffset)),
                            0,
                            copyToLagRowCount - 1, // inclusive
                            dstAddr,
                            dstAddrSize
                    );
                    dstDataMem.jumpTo(destOffset + dataSize);
                    assert srcDataMem.size() >= dataSize;
                    Vect.memmove(dstDataMem.addressOf(destOffset), srcDataMem.addressOf(dataOffset), dataSize);
                } else {
                    final int shl = ColumnType.pow2SizeOf(columnType);
                    // Fixed size column
                    long sourceOffset = columnDataRowOffset << shl;
                    long size = copyToLagRowCount << shl;
                    long destOffset = existingLagRows << shl;
                    dstDataMem.jumpTo(destOffset + size);
                    assert srcDataMem.size() >= size;
                    Vect.memmove(dstDataMem.addressOf(destOffset), srcDataMem.addressOf(sourceOffset), size);
                }

                // the data is copied back to start of the buffer, no need to set size first
            } else {
                MemoryCR o3SrcDataMem = o3Columns.get(getPrimaryColumnIndex(columnIndex));

                // Special case, designated timestamp column
                // Move values and set index to  0..copyToLagRowCount
                final long sourceOffset = columnDataRowOffset << 4;
                o3TimestampMem.jumpTo((copyToLagRowCount + existingLagRows) << 4);
                final long dstTimestampAddr = o3TimestampMem.getAddress() + (existingLagRows << 4);
                Vect.shiftTimestampIndex(o3SrcDataMem.addressOf(sourceOffset), copyToLagRowCount, dstTimestampAddr);
            }
        } catch (Throwable ex) {
            handleColumnTaskException(
                    "could not shift o3 lag",
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

    private void cthO3SortColumn(int columnIndex, int columnType, long timestampColumnIndex, long sortedTimestampsAddr, long sortedTimestampsRowCount, long long2, long long3, long long4) {
        if (ColumnType.isVarSize(columnType)) {
            cthO3SortVarColumn(columnIndex, columnType, sortedTimestampsAddr, sortedTimestampsRowCount);
        } else if (columnIndex != timestampColumnIndex) {
            cthO3SortFixColumn(columnIndex, columnType, sortedTimestampsAddr, sortedTimestampsRowCount);
        }
    }

    private void cthO3SortFixColumn(
            int columnIndex,
            int columnType,
            long sortedTimestampsAddr,
            long sortedTimestampsRowCount
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
            mem2.jumpTo(sortedTimestampsRowCount << shl);
            final long tgtDataAddr = mem2.addressOf(0);
            switch (shl) {
                case 0:
                    Vect.indexReshuffle8Bit(src, tgtDataAddr, sortedTimestampsAddr, sortedTimestampsRowCount);
                    break;
                case 1:
                    Vect.indexReshuffle16Bit(src, tgtDataAddr, sortedTimestampsAddr, sortedTimestampsRowCount);
                    break;
                case 2:
                    Vect.indexReshuffle32Bit(src, tgtDataAddr, sortedTimestampsAddr, sortedTimestampsRowCount);
                    break;
                case 3:
                    Vect.indexReshuffle64Bit(src, tgtDataAddr, sortedTimestampsAddr, sortedTimestampsRowCount);
                    break;
                case 4:
                    Vect.indexReshuffle128Bit(src, tgtDataAddr, sortedTimestampsAddr, sortedTimestampsRowCount);
                    break;
                case 5:
                    Vect.indexReshuffle256Bit(src, tgtDataAddr, sortedTimestampsAddr, sortedTimestampsRowCount);
                    break;
                default:
                    assert false : "col type is unsupported";
                    break;
            }
        } catch (Throwable th) {
            handleColumnTaskException(
                    "could not sort fix o3 column",
                    columnIndex,
                    columnType,
                    sortedTimestampsAddr,
                    sortedTimestampsRowCount,
                    IGNORE,
                    IGNORE,
                    th
            );
        }
    }

    private void cthO3SortVarColumn(
            int columnIndex,
            int columnType,
            long sortedTimestampsAddr,
            long sortedTimestampsRowCount
    ) {
        if (o3ErrorCount.get() > 0) {
            return;
        }
        try {
            final int primaryIndex = getPrimaryColumnIndex(columnIndex);
            final int secondaryIndex = primaryIndex + 1;

            ColumnType.getDriver(columnType).o3sort(
                    sortedTimestampsAddr,
                    sortedTimestampsRowCount,
                    o3Columns.getQuick(primaryIndex),
                    o3Columns.getQuick(secondaryIndex),
                    o3MemColumns2.getQuick(primaryIndex),
                    o3MemColumns2.getQuick(secondaryIndex)
            );
        } catch (Throwable th) {
            handleColumnTaskException(
                    "could not sort varsize o3 column",
                    columnIndex,
                    columnType,
                    sortedTimestampsAddr,
                    IGNORE,
                    IGNORE,
                    IGNORE,
                    th
            );
        }
    }

    private long deduplicateSortedIndex(long longIndexLength, long indexSrcAddr, long indexDstAddr, long tempIndexAddr, long lagRows) {
        LOG.info().$("WAL dedup sorted commit index [table=").$(tableToken).$(", totalRows=").$(longIndexLength).$(", lagRows=").$(lagRows).I$();
        int dedupKeyIndex = 0;
        long dedupCommitAddr = 0;
        try {
            if (dedupColumnCommitAddresses.getColumnCount() > 0) {
                dedupCommitAddr = dedupColumnCommitAddresses.allocateBlock();
                for (int i = 0; i < metadata.getColumnCount(); i++) {
                    int columnType = metadata.getColumnType(i);
                    if (i != metadata.getTimestampIndex() && columnType > 0 && metadata.isDedupKey(i)) {
                        long addr = DedupColumnCommitAddresses.setColValues(
                                dedupCommitAddr,
                                dedupKeyIndex++,
                                columnType,
                                ColumnType.isVarSize(columnType) ? -1 : ColumnType.sizeOf(columnType),
                                0L
                        );

                        if (!ColumnType.isVarSize(columnType)) {
                            int shl = ColumnType.pow2SizeOf(columnType);
                            long lagMemOffset = lagRows > 0 ? (txWriter.getTransientRowCount() - getColumnTop(i)) << shl : 0L;
                            long lagMapSize = lagRows << shl;

                            // Map column buffers for lag rows for deduplication
                            long lagKeyAddr = lagRows > 0 ? mapAppendColumnBuffer(columns.get(getPrimaryColumnIndex(i)), lagMemOffset, lagMapSize, false) : 0L;
                            long o3ColumnData = o3Columns.get(getPrimaryColumnIndex(i)).addressOf(0);
                            assert o3ColumnData != 0;

                            DedupColumnCommitAddresses.setColAddressValues(addr, o3ColumnData);
                            DedupColumnCommitAddresses.setO3DataAddressValues(addr, Math.abs(lagKeyAddr));
                            DedupColumnCommitAddresses.setReservedValuesSet1(
                                    addr,
                                    lagKeyAddr,
                                    lagMemOffset,
                                    lagMapSize
                            );
                        } else {
                            ColumnTypeDriver driver = ColumnType.getDriver(columnType);
                            MemoryCR o3VarColumn = o3Columns.get(getPrimaryColumnIndex(i));
                            long o3ColumnVarDataAddr = o3VarColumn.addressOf(0);
                            long o3ColumnVarAuxAddr = o3Columns.get(getSecondaryColumnIndex(i)).addressOf(0);
                            long o3ColumnVarDataSize = o3VarColumn.addressHi() - o3ColumnVarDataAddr;
                            DedupColumnCommitAddresses.setColAddressValues(addr, o3ColumnVarAuxAddr, o3ColumnVarDataAddr, o3ColumnVarDataSize);

                            if (lagRows > 0) {
                                long roLo = txWriter.getTransientRowCount() - getColumnTop(i);
                                long roHi = roLo + lagRows;

                                long lagAuxOffset = driver.getAuxVectorOffset(roLo);
                                long lagAuxSize = driver.getAuxVectorSize(lagRows);
                                long lagAuxKeyAddrRaw = mapAppendColumnBuffer(columns.get(getSecondaryColumnIndex(i)), lagAuxOffset, lagAuxSize, false);
                                long lagAuxKeyAddr = Math.abs(lagAuxKeyAddrRaw);

                                long lagVarDataOffset = driver.getDataVectorOffset(lagAuxKeyAddr, 0);
                                long lagVarDataSize = driver.getDataVectorSize(lagAuxKeyAddr, 0, lagRows - 1);
                                long lagVarDataAddrRaw = mapAppendColumnBuffer(columns.get(getPrimaryColumnIndex(i)), lagVarDataOffset, lagVarDataSize, false);
                                long lagVarDataAddr = Math.abs(lagVarDataAddrRaw);
                                // Aux points into the var buffer as if it's mapped from 0 row.
                                // Compensate the mapped with offset address of var buffer by subtracting lagVarDataOffset
                                DedupColumnCommitAddresses.setO3DataAddressValues(addr, lagAuxKeyAddr, lagVarDataAddr - lagVarDataOffset, lagVarDataSize + lagVarDataOffset);
                                DedupColumnCommitAddresses.setReservedValuesSet1(addr, lagAuxKeyAddrRaw, lagAuxOffset, lagAuxSize);
                                DedupColumnCommitAddresses.setReservedValuesSet2(addr, lagVarDataAddrRaw, lagVarDataOffset);
                            } else {
                                DedupColumnCommitAddresses.setO3DataAddressValues(addr, DedupColumnCommitAddresses.NULL, DedupColumnCommitAddresses.NULL, 0);
                            }
                        }
                    }
                }
                assert dedupKeyIndex <= dedupColumnCommitAddresses.getColumnCount();
            }
            return Vect.dedupSortedTimestampIndexIntKeysChecked(
                    indexSrcAddr,
                    longIndexLength,
                    indexDstAddr,
                    tempIndexAddr,
                    dedupKeyIndex,
                    DedupColumnCommitAddresses.getAddress(dedupCommitAddr)
            );
        } finally {
            if (dedupColumnCommitAddresses.getColumnCount() > 0 && lagRows > 0) {
                // Release mapped column buffers for lag rows
                for (int i = 0; i < dedupKeyIndex; i++) {
                    long lagAuxAddr = DedupColumnCommitAddresses.getColReserved1(dedupCommitAddr, i);
                    long lagAuxMemOffset = DedupColumnCommitAddresses.getColReserved2(dedupCommitAddr, i);
                    long mapAuxSize = DedupColumnCommitAddresses.getColReserved3(dedupCommitAddr, i);

                    mapAppendColumnBufferRelease(lagAuxAddr, lagAuxMemOffset, mapAuxSize);

                    long mapVarSize = DedupColumnCommitAddresses.getO3VarDataLen(dedupCommitAddr, i);
                    if (mapVarSize > 0) {
                        long lagVarAddr = DedupColumnCommitAddresses.getColReserved4(dedupCommitAddr, i);
                        long lagVarMemOffset = DedupColumnCommitAddresses.getColReserved5(dedupCommitAddr, i);
                        assert mapVarSize > lagVarMemOffset;
                        mapAppendColumnBufferRelease(lagVarAddr, lagVarMemOffset, mapVarSize - lagVarMemOffset);
                    }
                }
            }
            dedupColumnCommitAddresses.clear();
        }
    }

    private void dispatchColumnTasks(
            long long0,
            long long1,
            long long2,
            long long3,
            long long4,
            ColumnTaskHandler taskHandler
    ) {
        final long timestampColumnIndex = metadata.getTimestampIndex();
        final Sequence pubSeq = this.messageBus.getColumnTaskPubSeq();
        final RingQueue<ColumnTask> queue = this.messageBus.getColumnTaskQueue();
        o3DoneLatch.reset();
        o3ErrorCount.set(0);
        lastErrno = 0;
        int queuedCount = 0;

        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            int columnType = metadata.getColumnType(columnIndex);
            if (columnType > 0) {
                long cursor = pubSeq.next();

                // Pass column index as -1 when it's designated timestamp column to o3 move method
                if (cursor > -1) {
                    try {
                        final ColumnTask task = queue.get(cursor);
                        task.of(
                                o3DoneLatch,
                                columnIndex,
                                columnType,
                                timestampColumnIndex,
                                long0,
                                long1,
                                long2,
                                long3,
                                long4,
                                taskHandler
                        );
                    } finally {
                        queuedCount++;
                        pubSeq.done(cursor);
                    }
                } else {
                    taskHandler.run(columnIndex, columnType, timestampColumnIndex, long0, long1, long2, long3, long4);
                }
            }
        }
        consumeColumnTasks(queue, queuedCount);
    }

    private void doClose(boolean truncate) {
        // destroy() may have already closed everything
        boolean tx = inTransaction();
        freeSymbolMapWriters();
        Misc.freeObjList(indexers);
        denseIndexers.clear();
        Misc.free(txWriter);
        Misc.free(metaMem);
        Misc.free(ddlMem);
        Misc.free(other);
        Misc.free(todoMem);
        Misc.free(attachMetaMem);
        Misc.free(attachColumnVersionReader);
        Misc.free(attachIndexBuilder);
        Misc.free(columnVersionWriter);
        Misc.free(o3PartitionUpdateSink);
        Misc.free(slaveTxReader);
        Misc.free(commandQueue);
        Misc.free(dedupColumnCommitAddresses);
        closeWalFiles();
        updateOperatorImpl = Misc.free(updateOperatorImpl);
        convertOperatorImpl = Misc.free(convertOperatorImpl);
        dropIndexOperator = null;
        noOpRowCount = 0L;
        lastOpenPartitionTs = Long.MIN_VALUE;
        lastOpenPartitionIsReadOnly = false;
        Misc.free(frameFactory);
        assert !truncate || distressed || assertColumnPositionIncludeWalLag();
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
                tempMem16b = Unsafe.free(tempMem16b, 16, MemoryTag.NATIVE_TABLE_WRITER);
            }
            LOG.info().$("closed '").utf8(tableToken.getTableName()).$('\'').$();
        }
    }

    private boolean dropPartitionByExactTimestamp(long timestamp) {
        final long minTimestamp = txWriter.getMinTimestamp(); // partition min timestamp
        final long maxTimestamp = txWriter.getMaxTimestamp(); // partition max timestamp

        timestamp = txWriter.getPartitionTimestampByTimestamp(timestamp);
        final int index = txWriter.getPartitionIndex(timestamp);
        if (index < 0) {
            LOG.error().$("partition is already removed [path=").$substr(pathRootSize, path).$(", partitionTimestamp=").$ts(timestamp).I$();
            return false;
        }

        final long partitionNameTxn = txWriter.getPartitionNameTxnByPartitionTimestamp(timestamp);

        if (timestamp == txWriter.getPartitionTimestampByTimestamp(maxTimestamp)) {

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
                prevTimestamp = txWriter.getPartitionTimestampByIndex(prevIndex);
                newTransientRowCount = txWriter.getPartitionSize(prevIndex);
                try {
                    setPathForPartition(path.trimTo(pathSize), partitionBy, prevTimestamp, txWriter.getPartitionNameTxn(prevIndex));
                    readPartitionMinMax(ff, prevTimestamp, path, metadata.getColumnName(metadata.getTimestampIndex()), newTransientRowCount);
                    nextMaxTimestamp = attachMaxTimestamp;
                } finally {
                    path.trimTo(pathSize);
                }
            }

            columnVersionWriter.removePartition(timestamp);
            txWriter.beginPartitionSizeUpdate();
            txWriter.removeAttachedPartitions(timestamp);
            txWriter.finishPartitionSizeUpdate(index == 0 ? Long.MAX_VALUE : txWriter.getMinTimestamp(), nextMaxTimestamp);
            txWriter.bumpTruncateVersion();

            columnVersionWriter.commit();
            txWriter.setColumnVersion(columnVersionWriter.getVersion());
            txWriter.commit(denseSymbolMapWriters);

            // No need to truncate before, files to be deleted.
            closeActivePartition(false);

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
            if (timestamp == txWriter.getPartitionTimestampByIndex(0)) {
                nextMinTimestamp = readMinTimestamp(txWriter.getPartitionTimestampByIndex(1));
            }

            columnVersionWriter.removePartition(timestamp);

            txWriter.beginPartitionSizeUpdate();
            txWriter.removeAttachedPartitions(timestamp);
            txWriter.setMinTimestamp(nextMinTimestamp);
            txWriter.finishPartitionSizeUpdate(nextMinTimestamp, txWriter.getMaxTimestamp());
            txWriter.bumpTruncateVersion();

            columnVersionWriter.commit();
            txWriter.setColumnVersion(columnVersionWriter.getVersion());
            txWriter.commit(denseSymbolMapWriters);
        }

        // Call O3 methods to remove check TxnScoreboard and remove partition directly
        safeDeletePartitionDir(timestamp, partitionNameTxn);
        return true;
    }

    private long findMinSplitPartitionTimestamp() {
        for (int i = 0, n = txWriter.getPartitionCount(); i < n; i++) {
            long partitionTimestamp = txWriter.getPartitionTimestampByIndex(i);
            if (txWriter.getLogicalPartitionTimestamp(partitionTimestamp) != partitionTimestamp) {
                return partitionTimestamp;
            }
        }
        return Long.MAX_VALUE;
    }

    private void finishColumnPurge() {
        if (purgingOperator == null) {
            return;
        }
        boolean asyncOnly = checkScoreboardHasReadersBeforeLastCommittedTxn();
        purgingOperator.purge(
                path.trimTo(pathSize),
                tableToken,
                partitionBy,
                asyncOnly,
                metadata,
                getTruncateVersion(),
                getTxn()
        );
        purgingOperator.clear();
    }

    private void finishMetaSwapUpdate() {
        finishMetadataSwap();
        bumpMetadataVersion();
        clearTodoLog();
    }

    private void finishMetaSwapUpdateStructural() {
        // rename _meta to _meta.prev
        finishMetadataSwap();

        bumpMetadataAndColumnStructureVersion();
        clearTodoLog();
    }

    private void finishMetadataSwap() {
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
            openMetaFile(ff, path, pathSize, metaMem);
        } catch (CairoException e) {
            throwDistressException(e);
        }
    }

    private void finishO3Append(long o3LagRowCount) {
        if (denseIndexers.size() == 0) {
            populateDenseIndexerList();
        }
        path.trimTo(pathSize);
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
        if (isLastPartitionClosed() || partitionTimestampHi > partitionTimestampHiLimit) {
            openPartition(txWriter.getLastPartitionTimestamp());
        }

        // Data is written out successfully, however, we can still fail to set append position, for
        // example when we ran out of address space and new page cannot be mapped. The "allocate" calls here
        // ensure we can trigger this situation in tests. We should perhaps align our data such that setAppendPosition()
        // will attempt to mmap new page and fail... Then we can remove the 'true' parameter
        try {
            setAppendPosition(txWriter.getTransientRowCount(), !metadata.isWalEnabled());
        } catch (Throwable e) {
            LOG.critical().$("data is committed but writer failed to update its state `").$(e).$('`').$();
            distressed = true;
            throw e;
        }

        metrics.tableWriter().incrementO3Commits();
    }

    private Utf8Sequence formatPartitionForTimestamp(long partitionTimestamp, long nameTxn) {
        utf8Sink.clear();
        TableUtils.setSinkForPartition(utf8Sink, partitionBy, partitionTimestamp, nameTxn);
        return utf8Sink;
    }

    private void freeAndRemoveColumnPair(ObjList<MemoryMA> columns, int pi, int si) {
        Misc.free(columns.getAndSetQuick(pi, NullMemory.INSTANCE));
        Misc.free(columns.getAndSetQuick(si, NullMemory.INSTANCE));
    }

    private void freeAndRemoveO3ColumnPair(ObjList<MemoryCARW> columns, int pi, int si) {
        Misc.free(columns.getAndSetQuick(pi, NullMemory.INSTANCE));
        Misc.free(columns.getAndSetQuick(si, NullMemory.INSTANCE));
    }

    private void freeColumnMemory(int columnIndex) {
        final int pi = getPrimaryColumnIndex(columnIndex);
        final int si = getSecondaryColumnIndex(columnIndex);
        freeNullSetter(nullSetters, columnIndex);
        freeNullSetter(o3NullSetters1, columnIndex);
        freeNullSetter(o3NullSetters2, columnIndex);
        freeAndRemoveColumnPair(columns, pi, si);
        freeAndRemoveO3ColumnPair(o3MemColumns1, pi, si);
        freeAndRemoveO3ColumnPair(o3MemColumns2, pi, si);
        if (columnIndex < indexers.size()) {
            Misc.free(indexers.getAndSetQuick(columnIndex, null));
            populateDenseIndexerList();
        }
    }

    private void freeColumns(boolean truncate) {
        // null check is because this method could be called from the constructor
        if (columns != null) {
            closeAppendMemoryTruncate(truncate);
        }
        Misc.freeObjListAndKeepObjects(o3MemColumns1);
        Misc.freeObjListAndKeepObjects(o3MemColumns2);
    }

    private void freeIndexers() {
        if (indexers != null) {
            // Don't change items of indexers, they are re-used
            for (int i = 0, n = indexers.size(); i < n; i++) {
                ColumnIndexer indexer = indexers.getQuick(i);
                if (indexer != null) {
                    indexers.getQuick(i).releaseIndexWriter();
                }
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

    private CharSequence getColumnNameSafe(int columnIndex) {
        try {
            return metadata.getColumnName(columnIndex);
        } catch (Throwable th) {
            return "<unknown, index: " + columnIndex + ">";
        }
    }

    private ConvertOperatorImpl getConvertOperator() {
        if (convertOperatorImpl == null) {
            convertOperatorImpl = new ConvertOperatorImpl(configuration, this, columnVersionWriter, path, pathSize, getPurgingOperator(), messageBus);
        }
        return convertOperatorImpl;
    }

    private long getO3RowCount0() {
        return (masterRef - o3MasterRef + 1) / 2;
    }

    private long getPartitionTimestampOrMax(int partitionIndex) {
        if (partitionIndex < txWriter.getPartitionCount()) {
            return txWriter.getPartitionTimestampByIndex(partitionIndex);
        } else {
            return Long.MAX_VALUE;
        }
    }

    private MemoryMA getPrimaryColumn(int column) {
        assert column < columnCount : "Column index is out of bounds: " + column + " >= " + columnCount;
        return columns.getQuick(getPrimaryColumnIndex(column));
    }

    private PurgingOperator getPurgingOperator() {
        if (purgingOperator == null) {
            purgingOperator = new PurgingOperator(LOG, configuration, messageBus);
        } else {
            purgingOperator.clear();
        }
        return purgingOperator;
    }

    private MemoryMA getSecondaryColumn(int column) {
        assert column < columnCount : "Column index is out of bounds: " + column + " >= " + columnCount;
        return columns.getQuick(getSecondaryColumnIndex(column));
    }

    private long getWalMaxLagRows() {
        return Math.min(
                Math.max(0L, (long) configuration.getWalLagRowsMultiplier() * metadata.getMaxUncommittedRows()),
                getWalMaxLagSize()
        );
    }

    private long getWalMaxLagSize() {
        long maxLagSize = configuration.getWalMaxLagSize();
        return (maxLagSize /
                (avgRecordSize != 0 ? avgRecordSize : (avgRecordSize = TableUtils.estimateAvgRecordSize(metadata))));
    }

    private void handleColumnTaskException(
            String message,
            int columnIndex,
            int columnType,
            long long0,
            long long1,
            long long2,
            long long3,
            Throwable e
    ) {
        o3ErrorCount.incrementAndGet();
        LogRecord logRecord = LOG.critical().$(message + " [table=").$(tableToken.getTableName())
                .$(", column=").$(getColumnNameSafe(columnIndex))
                .$(", type=").$(ColumnType.nameOf(columnType))
                .$(", long0=").$(long0)
                .$(", long1=").$(long1)
                .$(", long2=").$(long2)
                .$(", long3=").$(long3);
        if (e instanceof CairoException) {
            o3oomObserved = ((CairoException) e).isOutOfMemory();
            lastErrno = lastErrno == 0 ? ((CairoException) e).errno : lastErrno;
            logRecord.$(", errno=").$(lastErrno)
                    .$(", ex=").$(((CairoException) e).getFlyweightMessage())
                    .I$();
        } else {
            lastErrno = O3_ERRNO_FATAL;
            logRecord.$(", ex=").$(e).I$();
        }
    }

    private void hardLinkAndPurgeColumnFiles(CharSequence columnName, int columnIndex, CharSequence newName, int columnType) {
        try {
            PurgingOperator purgingOperator = getPurgingOperator();
            long newColumnNameTxn = getTxn();
            long defaultColumnNameTxn = columnVersionWriter.getDefaultColumnNameTxn(columnIndex);
            if (PartitionBy.isPartitioned(partitionBy)) {
                for (int i = txWriter.getPartitionCount() - 1; i > -1L; i--) {
                    // Link files in each partition.
                    long partitionTimestamp = txWriter.getPartitionTimestampByIndex(i);
                    long partitionNameTxn = txWriter.getPartitionNameTxn(i);
                    long columnNameTxn = columnVersionWriter.getColumnNameTxn(partitionTimestamp, columnIndex);
                    hardLinkAndPurgeColumnFiles(columnName, columnIndex, columnType, newName, partitionTimestamp, partitionNameTxn, newColumnNameTxn, columnNameTxn);
                    if (columnVersionWriter.getRecordIndex(partitionTimestamp, columnIndex) > -1L) {
                        long columnTop = columnVersionWriter.getColumnTop(partitionTimestamp, columnIndex);
                        columnVersionWriter.upsert(partitionTimestamp, columnIndex, newColumnNameTxn, columnTop);
                    }
                }
            } else {
                long columnNameTxn = columnVersionWriter.getColumnNameTxn(txWriter.getLastPartitionTimestamp(), columnIndex);
                hardLinkAndPurgeColumnFiles(columnName, columnIndex, columnType, newName, txWriter.getLastPartitionTimestamp(), -1L, newColumnNameTxn, columnNameTxn);
                long columnTop = columnVersionWriter.getColumnTop(txWriter.getLastPartitionTimestamp(), columnIndex);
                columnVersionWriter.upsert(txWriter.getLastPartitionTimestamp(), columnIndex, newColumnNameTxn, columnTop);
            }

            if (ColumnType.isSymbol(columnType)) {
                // Link .o, .c, .k, .v symbol files in the table root folder
                linkFile(ff, offsetFileName(path.trimTo(pathSize), columnName, defaultColumnNameTxn), offsetFileName(other.trimTo(pathSize), newName, newColumnNameTxn));
                linkFile(ff, charFileName(path.trimTo(pathSize), columnName, defaultColumnNameTxn), charFileName(other.trimTo(pathSize), newName, newColumnNameTxn));
                linkFile(ff, keyFileName(path.trimTo(pathSize), columnName, defaultColumnNameTxn), keyFileName(other.trimTo(pathSize), newName, newColumnNameTxn));
                linkFile(ff, valueFileName(path.trimTo(pathSize), columnName, defaultColumnNameTxn), valueFileName(other.trimTo(pathSize), newName, newColumnNameTxn));
                purgingOperator.add(columnIndex, defaultColumnNameTxn, PurgingOperator.TABLE_ROOT_PARTITION, -1L);
            }
            long columnAddedPartition = columnVersionWriter.getColumnTopPartitionTimestamp(columnIndex);
            columnVersionWriter.upsertDefaultTxnName(columnIndex, newColumnNameTxn, columnAddedPartition);
        } finally {
            path.trimTo(pathSize);
            other.trimTo(pathSize);
        }
    }

    private void hardLinkAndPurgeColumnFiles(CharSequence columnName, int columnIndex, int columnType, CharSequence newName, long partitionTimestamp, long partitionNameTxn, long newColumnNameTxn, long columnNameTxn) {
        setPathForPartition(path, partitionBy, partitionTimestamp, partitionNameTxn);
        setPathForPartition(other, partitionBy, partitionTimestamp, partitionNameTxn);
        int plen = path.size();
        linkFile(ff, dFile(path.trimTo(plen), columnName, columnNameTxn), dFile(other.trimTo(plen), newName, newColumnNameTxn));
        if (ColumnType.isVarSize(columnType)) {
            linkFile(ff, iFile(path.trimTo(plen), columnName, columnNameTxn), iFile(other.trimTo(plen), newName, newColumnNameTxn));
        } else if (ColumnType.isSymbol(columnType) && metadata.isColumnIndexed(columnIndex)) {
            linkFile(ff, keyFileName(path.trimTo(plen), columnName, columnNameTxn), keyFileName(other.trimTo(plen), newName, newColumnNameTxn));
            linkFile(ff, valueFileName(path.trimTo(plen), columnName, columnNameTxn), valueFileName(other.trimTo(plen), newName, newColumnNameTxn));
        }
        path.trimTo(pathSize);
        other.trimTo(pathSize);
        purgingOperator.add(columnIndex, columnNameTxn, partitionTimestamp, partitionNameTxn);
    }

    private void indexHistoricPartitions(SymbolColumnIndexer indexer, CharSequence columnName, int indexValueBlockSize, int columnIndex) {
        long ts = this.txWriter.getMaxTimestamp();
        if (ts > Numbers.LONG_NULL) {
            try {
                // Index last partition separately
                for (int i = 0, n = txWriter.getPartitionCount() - 1; i < n; i++) {

                    long timestamp = txWriter.getPartitionTimestampByIndex(i);
                    path.trimTo(pathSize);
                    setStateForTimestamp(path, timestamp);

                    if (ff.exists(path.$())) {
                        final int plen = path.size();

                        long columnNameTxn = columnVersionWriter.getColumnNameTxn(timestamp, columnIndex);

                        if (ff.exists(TableUtils.dFile(path.trimTo(plen), columnName, columnNameTxn))) {
                            path.trimTo(plen);
                            LOG.info().$("indexing [path=").$substr(pathRootSize, path).I$();

                            createIndexFiles(columnName, columnNameTxn, indexValueBlockSize, plen, true);
                            final long partitionSize = txWriter.getPartitionRowCountByTimestamp(timestamp);
                            final long columnTop = columnVersionWriter.getColumnTop(timestamp, columnIndex);

                            if (columnTop > -1L && partitionSize > columnTop) {
                                long columnDataFd = TableUtils.openRO(ff, TableUtils.dFile(path.trimTo(plen), columnName, columnNameTxn), LOG);
                                try {
                                    indexer.configureWriter(path.trimTo(plen), columnName, columnNameTxn, columnTop);
                                    indexer.index(ff, columnDataFd, columnTop, partitionSize);
                                } finally {
                                    ff.close(columnDataFd);
                                }
                            }
                        }
                    }
                }
            } finally {
                indexer.releaseIndexWriter();
            }
        }
    }

    private void indexLastPartition(SymbolColumnIndexer indexer, CharSequence columnName, long columnNameTxn, int columnIndex, int indexValueBlockSize) {
        final int plen = path.size();

        createIndexFiles(columnName, columnNameTxn, indexValueBlockSize, plen, true);

        final long lastPartitionTs = txWriter.getLastPartitionTimestamp();
        final long columnTop = columnVersionWriter.getColumnTopQuick(lastPartitionTs, columnIndex);

        // set indexer up to continue functioning as normal
        indexer.configureFollowerAndWriter(path.trimTo(plen), columnName, columnNameTxn, getPrimaryColumn(columnIndex), columnTop);
        indexer.refreshSourceAndIndex(0, txWriter.getTransientRowCount());
    }

    private void initLastPartition(long timestamp) {
        final long ts = repairDataGaps(timestamp);
        openLastPartitionAndSetAppendPosition(ts);
        populateDenseIndexerList();
        if (performRecovery) {
            performRecovery();
        }
        txWriter.initLastPartition(ts);
    }

    private boolean isEmptyTable() {
        return txWriter.getPartitionCount() == 0 && txWriter.getLagRowCount() == 0;
    }

    private boolean isLastPartitionClosed() {
        for (int i = 0; i < columnCount; i++) {
            if (metadata.getColumnType(i) > 0) {
                return !columns.getQuick(getPrimaryColumnIndex(i)).isOpen();
            }
        }
        // No columns, doesn't matter
        return false;
    }

    private void lock() {
        try {
            path.trimTo(pathSize);
            performRecovery = ff.exists(lockName(path));
            this.lockFd = TableUtils.lock(ff, path.$());
        } finally {
            path.trimTo(pathSize);
        }

        if (this.lockFd == -1) {
            throw CairoException.critical(ff.errno()).put("cannot lock table: ").put(path.$());
        }
    }

    private long mapAppendColumnBuffer(MemoryMA column, long offset, long size, boolean rw) {
        if (size == 0) {
            return 0;
        }

        column.jumpTo(offset + size);
        long address = column.map(offset, size);

        // column could not provide necessary length of buffer
        // because perhaps its internal buffer is not big enough
        if (address != 0) {
            return address;
        } else {
            return -TableUtils.mapAppendColumnBuffer(ff, column.getFd(), offset, size, rw, MemoryTag.MMAP_TABLE_WRITER);
        }
    }

    private void mapAppendColumnBufferRelease(long address, long offset, long size) {
        if (address < 0) {
            TableUtils.mapAppendColumnBufferRelease(ff, -address, offset, size, MemoryTag.MMAP_TABLE_WRITER);
        }
    }

    private void mmapWalColsEager() {
        for (int i = 0, n = walMappedColumns.size(); i < n; i++) {
            MemoryCR columnMem = o3Columns.get(i);
            if (columnMem != null) {
                columnMem.map();
            }
        }
    }

    private void mmapWalColumns(@Transient Path walPath, long walSegmentId, int timestampIndex, long rowLo, long rowHi) {
        walMappedColumns.clear();
        int walPathLen = walPath.size();
        final int columnCount = metadata.getColumnCount();
        int key = walFdCache.keyIndex(walSegmentId);
        LongList fds = null;
        if (key < 0) {
            fds = walFdCache.valueAt(key);
        }

        try {
            int file = 0;
            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                final int columnType = metadata.getColumnType(columnIndex);
                o3RowCount = rowHi - rowLo;
                if (columnType > 0) {
                    int sizeBitsPow2 = ColumnType.getWalDataColumnShl(columnType, columnIndex == timestampIndex);

                    if (ColumnType.isVarSize(columnType)) {
                        MemoryCMOR auxMem = walColumnMemoryPool.pop();
                        MemoryCMOR dataMem = walColumnMemoryPool.pop();

                        walMappedColumns.add(dataMem);
                        walMappedColumns.add(auxMem);

                        final long dataFd = fds != null ? fds.get(file++) : -1;
                        final long auxFd = fds != null ? fds.get(file++) : -1;

                        final ColumnTypeDriver columnTypeDriver = ColumnType.getDriver(columnType);

                        LPSZ ifile = auxFd == -1 ? iFile(walPath, metadata.getColumnName(columnIndex), -1L) : null;
                        LOG.debug().$("reusing file descriptor for WAL files [fd=").$(auxFd).$(", path=").$(walPath).$(", walSegment=").$(walSegmentId).I$();
                        columnTypeDriver.configureAuxMemOM(
                                configuration.getFilesFacade(),
                                auxMem,
                                auxFd,
                                ifile,
                                rowLo,
                                rowHi,
                                MemoryTag.MMAP_TABLE_WRITER,
                                CairoConfiguration.O_NONE
                        );
                        walPath.trimTo(walPathLen);

                        LPSZ dfile = dataFd == -1 ? dFile(walPath, metadata.getColumnName(columnIndex), -1L) : null;
                        LOG.debug().$("reusing file descriptor for WAL files [fd=").$(dataFd).$(", wal=").$(walPath).$(", walSegment=").$(walSegmentId).I$();
                        columnTypeDriver.configureDataMemOM(
                                configuration.getFilesFacade(),
                                auxMem,
                                dataMem,
                                dataFd,
                                dfile,
                                rowLo,
                                rowHi,
                                MemoryTag.MMAP_TABLE_WRITER,
                                CairoConfiguration.O_NONE
                        );
                    } else {
                        MemoryCMOR primary = walColumnMemoryPool.pop();
                        walMappedColumns.add(primary);
                        walMappedColumns.add(null);

                        long fd = fds != null ? fds.get(file++) : -1;
                        LPSZ dfile = fd == -1 ? dFile(walPath, metadata.getColumnName(columnIndex), -1L) : null;
                        LOG.debug().$("reusing file descriptor for WAL files [fd=").$(fd).$(", path=").$(walPath).$(", walSegment=").$(walSegmentId).I$();
                        primary.ofOffset(
                                configuration.getFilesFacade(),
                                fd,
                                dfile,
                                rowLo << sizeBitsPow2,
                                rowHi << sizeBitsPow2,
                                MemoryTag.MMAP_TX_LOG,
                                CairoConfiguration.O_NONE
                        );
                    }
                    walPath.trimTo(walPathLen);
                } else {
                    walMappedColumns.add(null);
                    walMappedColumns.add(null);
                }
            }
            o3Columns = walMappedColumns;
        } catch (Throwable th) {
            closeWalColumns(true, walSegmentId);
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
        final int timestampColumnIndex = metadata.getTimestampIndex();
        lastPartitionTimestamp = txWriter.getPartitionTimestampByTimestamp(partitionTimestampHi);
        // we will check new partitionTimestampHi value against the limit to see if the writer
        // will have to switch partition internally
        long partitionTimestampHiLimit = txWriter.getNextPartitionTimestamp(partitionTimestampHi) - 1;
        try {
            o3RowCount += o3MoveUncommitted();

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
                        o3EffectiveLag += (long) (lagError * configuration.getO3LagIncreaseFactor());
                        o3EffectiveLag = Math.min(o3EffectiveLag, o3MaxLag);
                    } else {
                        // avoid using negative o3EffectiveLag
                        o3EffectiveLag += (long) (lagError * configuration.getO3LagDecreaseFactor());
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
            long sortedTimestampsRowCount = o3RowCount;
            dispatchColumnTasks(sortedTimestampsAddr, sortedTimestampsRowCount, IGNORE, IGNORE, IGNORE, cthO3SortColumnRef);
            swapO3ColumnsExcept(timestampColumnIndex);
            LOG.info()
                    .$("sorted [table=").utf8(tableToken.getTableName())
                    .$(", o3RowCount=").$(o3RowCount)
                    .I$();

            processO3Block(
                    o3LagRowCount,
                    timestampColumnIndex,
                    sortedTimestampsAddr,
                    srcOooMax,
                    o3TimestampMin,
                    o3TimestampMax,
                    true,
                    0L,
                    O3JobParallelismRegulator.EMPTY
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
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long oooTimestampMin,
            long partitionTimestamp,
            long srcDataMax,
            boolean last,
            long srcNameTxn,
            O3Basket o3Basket,
            long newPartitionSize,
            long oldPartitionSize,
            long partitionUpdateSinkAddr,
            long dedupColSinkAddr,
            boolean isParquet
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
                    newPartitionSize,
                    oldPartitionSize,
                    partitionUpdateSinkAddr,
                    dedupColSinkAddr,
                    isParquet
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
                    newPartitionSize,
                    oldPartitionSize,
                    partitionUpdateSinkAddr,
                    dedupColSinkAddr,
                    isParquet
            );
        }
    }

    private void o3ConsumePartitionUpdateSink() {
        long blockIndex = -1;

        long commitTransientRowCount = txWriter.transientRowCount;

        while ((blockIndex = o3PartitionUpdateSink.nextBlockIndex(blockIndex)) > -1L) {
            final long blockAddress = o3PartitionUpdateSink.getBlockAddress(blockIndex);
            long partitionTimestamp = Unsafe.getUnsafe().getLong(blockAddress);
            long timestampMin = Unsafe.getUnsafe().getLong(blockAddress + Long.BYTES);

            if (partitionTimestamp != -1L && timestampMin != -1L) {
                final long srcDataNewPartitionSize = Unsafe.getUnsafe().getLong(blockAddress + 2 * Long.BYTES);
                final long srcDataOldPartitionSize = Unsafe.getUnsafe().getLong(blockAddress + 3 * Long.BYTES);
                final long flags = Unsafe.getUnsafe().getLong(blockAddress + 4 * Long.BYTES);
                final boolean partitionMutates = Numbers.decodeLowInt(flags) != 0;
                final boolean isLastWrittenPartition = o3PartitionUpdateSink.nextBlockIndex(blockIndex) == -1;
                final long o3SplitPartitionSize = Unsafe.getUnsafe().getLong(blockAddress + 5 * Long.BYTES);

                txWriter.minTimestamp = Math.min(timestampMin, txWriter.minTimestamp);
                int partitionIndexRaw = txWriter.findAttachedPartitionRawIndexByLoTimestamp(partitionTimestamp);

                final long newPartitionTimestamp = partitionTimestamp;
                final int newPartitionIndex = partitionIndexRaw;
                if (partitionIndexRaw < 0) {
                    // This is partition split. Instead of rewriting partition because of O3 merge
                    // the partition is kept, and its tail rewritten.
                    // The new partition overlaps in time with the previous one.
                    partitionTimestamp = txWriter.getPartitionTimestampByTimestamp(partitionTimestamp);
                    partitionIndexRaw = txWriter.findAttachedPartitionRawIndexByLoTimestamp(partitionTimestamp);
                }

                if (partitionTimestamp == lastPartitionTimestamp && newPartitionTimestamp == partitionTimestamp) {
                    if (partitionMutates) {
                        // Last partition is rewritten.
                        closeActivePartition(true);
                    } else if (!isLastWrittenPartition) {
                        // Last partition is appended, and it is not the last partition anymore.
                        closeActivePartition(srcDataNewPartitionSize);
                    } else {
                        // Last partition is appended, and it is still the last partition.
                        setAppendPosition(srcDataNewPartitionSize, false);
                    }
                }

                if (partitionTimestamp < lastPartitionTimestamp) {
                    // increment fixedRowCount by number of rows old partition incremented
                    this.txWriter.fixedRowCount += srcDataNewPartitionSize - srcDataOldPartitionSize + o3SplitPartitionSize;
                } else {
                    if (partitionTimestamp != lastPartitionTimestamp) {
                        this.txWriter.fixedRowCount += commitTransientRowCount;
                    }
                    if (o3SplitPartitionSize > 0) {
                        // yep, it was
                        // the "current" active becomes fixed
                        this.txWriter.fixedRowCount += srcDataNewPartitionSize;
                        commitTransientRowCount = o3SplitPartitionSize;
                    } else {
                        commitTransientRowCount = srcDataNewPartitionSize;
                    }
                }

                LOG.debug().$("o3 partition update [timestampMin=").$ts(timestampMin)
                        .$(", last=").$(partitionTimestamp == lastPartitionTimestamp)
                        .$(", partitionTimestamp=").$ts(partitionTimestamp)
                        .$(", partitionMutates=").$(partitionMutates)
                        .$(", lastPartitionTimestamp=").$(lastPartitionTimestamp)
                        .$(", srcDataOldPartitionSize=").$(srcDataOldPartitionSize)
                        .$(", srcDataNewPartitionSize=").$(srcDataNewPartitionSize)
                        .$(", o3SplitPartitionSize=").$(o3SplitPartitionSize)
                        .$(", commitTransientRowCount=").$(commitTransientRowCount)
                        .$(", fixedRowCount=").$(this.txWriter.fixedRowCount)
                        .I$();

                if (newPartitionTimestamp != partitionTimestamp) {
                    LOG.info()
                            .$("o3 split partition [table=").utf8(tableToken.getTableName())
                            .$(", part1=").$(
                                    formatPartitionForTimestamp(
                                            partitionTimestamp,
                                            txWriter.getPartitionNameTxnByPartitionTimestamp(partitionTimestamp)
                                    )
                            )
                            .$(", part1OldSize=").$(srcDataOldPartitionSize)
                            .$(", part1NewSize=").$(srcDataNewPartitionSize)
                            .$(", part2=").$(formatPartitionForTimestamp(newPartitionTimestamp, txWriter.txn))
                            .$(", part2Size=").$(o3SplitPartitionSize)
                            .I$();
                    this.minSplitPartitionTimestamp = Math.min(this.minSplitPartitionTimestamp, newPartitionTimestamp);
                    txWriter.bumpPartitionTableVersion();
                    txWriter.updateAttachedPartitionSizeByRawIndex(newPartitionIndex, newPartitionTimestamp, o3SplitPartitionSize, txWriter.txn);
                    if (partitionTimestamp == lastPartitionTimestamp) {
                        // Close last partition without truncating it.
                        long committedLastPartitionSize = txWriter.getPartitionRowCountByTimestamp(partitionTimestamp);
                        closeActivePartition(committedLastPartitionSize);
                    }
                }

                if (partitionMutates && newPartitionTimestamp == partitionTimestamp) {
                    final long srcNameTxn = txWriter.getPartitionNameTxnByRawIndex(partitionIndexRaw);
                    LOG.info()
                            .$("merged partition [table=`").utf8(tableToken.getTableName())
                            .$("`, ts=").$ts(partitionTimestamp)
                            .$(", txn=").$(txWriter.txn).I$();

                    final long parquetFileSize = Unsafe.getUnsafe().getLong(blockAddress + 7 * Long.BYTES);
                    if (parquetFileSize > -1) {
                        // Since we're technically performing an "append" here,
                        // there's no need to increment the txn or partition table version.
                        txWriter.updatePartitionSizeByRawIndex(partitionIndexRaw, partitionTimestamp, srcDataNewPartitionSize);
                        txWriter.setPartitionParquetFormat(partitionTimestamp, parquetFileSize);
                    } else {
                        txWriter.updatePartitionSizeAndTxnByRawIndex(partitionIndexRaw, srcDataNewPartitionSize);
                        partitionRemoveCandidates.add(partitionTimestamp, srcNameTxn);
                        txWriter.bumpPartitionTableVersion();
                    }
                } else {
                    if (partitionTimestamp != lastPartitionTimestamp) {
                        txWriter.bumpPartitionTableVersion();
                    }
                    txWriter.updatePartitionSizeByRawIndex(partitionIndexRaw, partitionTimestamp, srcDataNewPartitionSize);
                }
            }
        }
        txWriter.transientRowCount = commitTransientRowCount;
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

    private long o3MoveUncommitted() {
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
            dispatchColumnTasks(
                    committedTransientRowCount,
                    IGNORE,
                    transientRowsAdded,
                    IGNORE,
                    IGNORE,
                    cthO3MoveUncommittedRef
            );
            txWriter.resetToLastPartition(committedTransientRowCount);
            return transientRowsAdded;
        }
        return 0;
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
            final int columnType = metadata.getColumnType(i);
            if (columnType > 0) {
                MemoryARW dataMem = o3MemColumns1.getQuick(getPrimaryColumnIndex(i));
                MemoryARW auxMem = o3MemColumns1.getQuick(getSecondaryColumnIndex(i));
                dataMem.jumpTo(0);
                if (ColumnType.isVarSize(columnType)) {
                    auxMem.jumpTo(0);
                    ColumnType.getDriver(columnType).configureAuxMemO3RSS(auxMem);
                }
            }
        }
        activeColumns = o3MemColumns1;
        activeNullSetters = o3NullSetters1;
        LOG.debug().$("switched partition to memory").$();
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

    private void o3SetAppendOffset(
            int columnIndex,
            final int columnType,
            long o3RowCount
    ) {
        if (columnIndex != metadata.getTimestampIndex()) {
            MemoryARW o3DataMem = o3MemColumns1.get(getPrimaryColumnIndex(columnIndex));
            MemoryARW o3IndexMem = o3MemColumns1.get(getSecondaryColumnIndex(columnIndex));

            long size;
            if (null == o3IndexMem) {
                // Fixed size column
                size = o3RowCount << ColumnType.pow2SizeOf(columnType);
            } else {
                // Var size column
                ColumnTypeDriver driver = ColumnType.getDriver(columnType);
                if (o3RowCount > 0) {
                    size = driver.getDataVectorSizeAt(o3IndexMem.addressOf(0), o3RowCount - 1);
                } else {
                    size = 0;
                }
                o3IndexMem.jumpTo(driver.getAuxVectorSize(o3RowCount));
            }

            o3DataMem.jumpTo(size);
        } else {
            // Special case, designated timestamp column
            o3TimestampMem.jumpTo(o3RowCount * 16);
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
            mem1.of(
                    ff,
                    dFile(path.trimTo(pathTrimToLen), name, columnNameTxn),
                    dataAppendPageSize,
                    -1,
                    MemoryTag.MMAP_TABLE_WRITER,
                    configuration.getWriterFileOpenOpts(),
                    Files.POSIX_MADV_RANDOM
            );
            if (mem2 != null) {
                mem2.of(
                        ff,
                        iFile(path.trimTo(pathTrimToLen), name, columnNameTxn),
                        dataAppendPageSize,
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

    private void openLastPartitionAndSetAppendPosition(long ts) {
        openPartition(ts);
        setAppendPosition(txWriter.getTransientRowCount() + txWriter.getLagRowCount(), false);
    }

    private void openNewColumnFiles(CharSequence name, int columnType, boolean indexFlag, int indexValueBlockCapacity) {
        try {
            // open column files
            long partitionTimestamp = txWriter.getLastPartitionTimestamp();
            setStateForTimestamp(path, partitionTimestamp);
            final int plen = path.size();
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
                indexers.getQuick(columnIndex).configureFollowerAndWriter(path.trimTo(plen), name, columnNameTxn, getPrimaryColumn(columnIndex), txWriter.getTransientRowCount());
            }

            // configure append position for variable length columns
            if (ColumnType.isVarSize(columnType)) {
                ColumnType.getDriver(columnType).configureAuxMemMA(getSecondaryColumn(columnCount - 1));
            }

            LOG.info().$("ADDED column '").utf8(name)
                    .$('[').$(ColumnType.nameOf(columnType)).$("], columnName txn ").$(columnNameTxn)
                    .$(" to ").$substr(pathRootSize, path)
                    .$(" with columnTop ").$(txWriter.getTransientRowCount())
                    .$();
        } finally {
            path.trimTo(pathSize);
        }
    }

    private void openPartition(long timestamp) {
        try {
            timestamp = txWriter.getPartitionTimestampByTimestamp(timestamp);
            setStateForTimestamp(path, timestamp);
            partitionTimestampHi = txWriter.getNextPartitionTimestamp(timestamp) - 1;
            int plen = path.size();
            if (ff.mkdirs(path.slash(), mkDirMode) != 0) {
                throw CairoException.critical(ff.errno()).put("cannot create directory: ").put(path);
            }

            assert columnCount > 0;

            lastOpenPartitionTs = timestamp;
            lastOpenPartitionIsReadOnly = partitionBy != PartitionBy.NONE && txWriter.isPartitionReadOnlyByPartitionTimestamp(lastOpenPartitionTs);

            for (int i = 0; i < columnCount; i++) {
                if (metadata.getColumnType(i) > 0) {
                    final CharSequence name = metadata.getColumnName(i);
                    long columnNameTxn = columnVersionWriter.getColumnNameTxn(lastOpenPartitionTs, i);
                    final ColumnIndexer indexer = metadata.isColumnIndexed(i) ? indexers.getQuick(i) : null;

                    // prepare index writer if column requires indexing
                    if (indexer != null) {
                        // we have to create files before columns are open
                        // because we are reusing MAMemoryImpl object from columns list
                        createIndexFiles(name, columnNameTxn, metadata.getIndexValueBlockCapacity(i), plen, txWriter.getTransientRowCount() < 1);
                    }

                    openColumnFiles(name, columnNameTxn, i, plen);

                    if (indexer != null) {
                        final long columnTop = columnVersionWriter.getColumnTopQuick(lastOpenPartitionTs, i);
                        indexer.configureFollowerAndWriter(path, name, columnNameTxn, getPrimaryColumn(i), columnTop);
                    }
                }
            }
            populateDenseIndexerList();
            LOG.info().$("switched partition [path=").$substr(pathRootSize, path).I$();
        } catch (Throwable e) {
            distressed = true;
            throw e;
        } finally {
            path.trimTo(pathSize);
        }
    }

    private long openTodoMem() {
        path.concat(TODO_FILE_NAME);
        try {
            if (ff.exists(path.$())) {
                long fileLen = ff.length(path.$());
                if (fileLen < 32) {
                    throw CairoException.critical(0).put("corrupt ").put(path);
                }

                todoMem.smallFile(ff, path.$(), MemoryTag.MMAP_TABLE_WRITER);
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
                TableUtils.resetTodoLog(ff, path, pathSize, todoMem);
                todoTxn = 0;
                return 0;
            }
        } finally {
            path.trimTo(pathSize);
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
        // In case processing of a queue calls rollback() on the writer
        // do not recursively start processing the queue again.
        if (!processingQueue) {
            try {
                processingQueue = true;
                long cursor;
                while ((cursor = commandSubSeq.next()) != -1) {
                    if (cursor > -1) {
                        TableWriterTask cmd = commandQueue.get(cursor);
                        processCommandQueue(cmd, commandSubSeq, cursor, contextAllowsAnyStructureChanges);
                    } else {
                        Os.pause();
                    }
                }
            } finally {
                processingQueue = false;
            }
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
            long rowLo,
            O3JobParallelismRegulator regulator
    ) {
        o3ErrorCount.set(0);
        o3oomObserved = false;
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
        int partitionParallelism = regulator.getMaxO3MergeParallelism();
        try {
            resizePartitionUpdateSink();

            // One loop iteration per partition.
            int inflightPartitions = 0;
            while (srcOoo < srcOooMax) {
                inflightPartitions++;
                regulator.updateInflightPartitions(inflightPartitions);
                try {
                    final long srcOooLo = srcOoo;
                    final long o3Timestamp = getTimestampIndexValue(sortedTimestampsAddr, srcOoo);

                    // Check that the value is not 0 (or another unreasonable value) because of reading beyond written range.
                    assert o3Timestamp >= Math.min(o3TimestampMin, Math.max(txWriter.getMinTimestamp(), 0));

                    final long srcOooHi;
                    // keep ceil inclusive in the interval
                    final long srcOooTimestampCeil = txWriter.getNextPartitionTimestamp(o3Timestamp) - 1;
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

                    final long partitionTimestamp = txWriter.getPartitionTimestampByTimestamp(o3Timestamp);

                    // This partition is the last partition.
                    final boolean last = partitionTimestamp == lastPartitionTimestamp;

                    srcOoo = srcOooHi + 1;

                    final long srcDataMax;
                    final long srcNameTxn;
                    final int partitionIndexRaw = txWriter.findAttachedPartitionRawIndexByLoTimestamp(partitionTimestamp);
                    if (partitionIndexRaw > -1) {
                        if (last) {
                            srcDataMax = transientRowCount;
                        } else {
                            srcDataMax = getPartitionSizeByRawIndex(partitionIndexRaw);
                        }
                        srcNameTxn = getPartitionNameTxnByRawIndex(partitionIndexRaw);
                    } else {
                        srcDataMax = 0;
                        // A version needed to housekeep dropped partitions.
                        // When partition created without O3 merge, use `txn-1` as partition version.
                        // `txn` version is used when partition is merged. Both `txn-1` and `txn` can
                        // be written within the same commit when new partition initially written in order
                        // and then O3 triggers a merge of the partition.
                        srcNameTxn = txWriter.getTxn() - 1;
                    }

                    // We're appending onto the last (active) partition.
                    final boolean append = last && (srcDataMax == 0 || (isDeduplicationEnabled() && o3Timestamp > maxTimestamp) || (!isDeduplicationEnabled() && o3Timestamp >= maxTimestamp));

                    // Number of rows to insert from the O3 segment into this partition.
                    final long srcOooBatchRowSize = srcOooHi - srcOooLo + 1;

                    // Final partition size after current insertions.
                    final long newPartitionSize = srcDataMax + srcOooBatchRowSize;

                    // check partition read-only state
                    final boolean partitionIsReadOnly = partitionIndexRaw > -1 && txWriter.isPartitionReadOnlyByRawIndex(partitionIndexRaw);
                    final boolean isParquet = partitionIndexRaw > -1 && txWriter.isPartitionParquetByRawIndex(partitionIndexRaw);

                    pCount++;

                    LOG.info().$("o3 partition task [table=").utf8(tableToken.getTableName())
                            .$(", partitionTs=").$ts(partitionTimestamp)
                            .$(", partitionIndex=").$(partitionIndexRaw)
                            .$(", last=").$(last)
                            .$(", append=").$(append)
                            .$(", ro=").$(partitionIsReadOnly)
                            .$(", srcOooLo=").$(srcOooLo)
                            .$(", srcOooHi=").$(srcOooHi)
                            .$(", srcOooMax=").$(srcOooMax)
                            .$(", o3RowCount=").$(o3RowCount)
                            .$(", o3LagRowCount=").$(o3LagRowCount)
                            .$(", srcDataMax=").$(srcDataMax)
                            .$(", o3Ts=").$ts(o3Timestamp)
                            .$(", newSize=").$(newPartitionSize)
                            .$(", maxTs=").$ts(maxTimestamp)
                            .$(", pCount=").$(pCount)
                            .$(", flattenTs=").$(flattenTimestamp)
                            .$(", memUsed=").$size(Unsafe.getMemUsed())
                            .$(", rssMemUsed=").$size(Unsafe.getRssMemUsed())
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
                    final O3Basket o3Basket = o3BasketPool.next();
                    o3Basket.checkCapacity(configuration, columnCount, indexCount);
                    AtomicInteger columnCounter = o3ColumnCounters.next();

                    // To collect column top values and partition updates
                    // from o3 partition tasks add them to pre-allocated continuous block of memory
                    long partitionUpdateSinkAddr = o3PartitionUpdateSink.allocateBlock();

                    o3PartitionUpdRemaining.incrementAndGet();
                    // async partition processing set this counter to the column count
                    // and then manages issues if publishing of column tasks fails
                    // mid-column-count.
                    latchCount++;
                    // Set column top memory to -1, no need to initialize partition update memory, it always set by O3 partition tasks
                    Vect.memset(partitionUpdateSinkAddr + (long) PARTITION_SINK_SIZE_LONGS * Long.BYTES, (long) metadata.getColumnCount() * Long.BYTES, -1);
                    Unsafe.getUnsafe().putLong(partitionUpdateSinkAddr, partitionTimestamp);
                    // original partition timestamp
                    Unsafe.getUnsafe().putLong(partitionUpdateSinkAddr + 6 * Long.BYTES, partitionTimestamp);


                    if (append) {
                        // we are appending last partition, make sure it has been mapped!
                        // this also might fail, make sure exception is trapped and partitions are
                        // counted down correctly
                        try {
                            setAppendPosition(srcDataMax, false);
                        } catch (Throwable e) {
                            o3BumpErrorCount(CairoException.isCairoOomError(e));
                            o3ClockDownPartitionUpdateCount();
                            o3CountDownDoneLatch();
                            throw e;
                        }

                        columnCounter.set(TableUtils.compressColumnCount(metadata));
                        Path pathToPartition = Path.getThreadLocal(path);
                        TableUtils.setPathForPartition(pathToPartition, partitionBy, txWriter.getPartitionTimestampByTimestamp(o3TimestampMin), srcNameTxn);
                        final int plen = pathToPartition.size();
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
                            if (!ColumnType.isVarSize(columnType)) {
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
                                        partitionTimestamp,
                                        srcDataTop,
                                        srcDataMax,
                                        indexBlockCapacity,
                                        dstFixMem,
                                        dstVarMem,
                                        newPartitionSize,
                                        srcDataMax,
                                        0,
                                        this,
                                        indexWriter,
                                        getColumnNameTxn(partitionTimestamp, i),
                                        partitionUpdateSinkAddr
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
                        final long dedupColSinkAddr = dedupColumnCommitAddresses != null ? dedupColumnCommitAddresses.allocateBlock() : 0;
                        o3CommitPartitionAsync(
                                columnCounter,
                                maxTimestamp,
                                sortedTimestampsAddr,
                                srcOooLo,
                                srcOooHi,
                                srcOooMax,
                                o3TimestampMin,
                                partitionTimestamp,
                                srcDataMax,
                                last,
                                srcNameTxn,
                                o3Basket,
                                newPartitionSize,
                                srcDataMax,
                                partitionUpdateSinkAddr,
                                dedupColSinkAddr,
                                isParquet
                        );
                    }
                } catch (CairoException | CairoError e) {
                    LOG.error().$((Sinkable) e).$();
                    success = false;
                    throw e;
                }
                if (inflightPartitions % partitionParallelism == 0) {
                    o3ConsumePartitionUpdates();
                    o3DoneLatch.await(latchCount);
                    inflightPartitions = 0;
                }
            } // end while(srcOoo < srcOooMax)

            // at this point we should know the last partition row count
            this.partitionTimestampHi = Math.max(this.partitionTimestampHi, txWriter.getNextPartitionTimestamp(o3TimestampMax) - 1);
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
            if (o3ErrorCount.get() == 0 && success) {
                o3ConsumePartitionUpdateSink();
            }
            o3DoneLatch.await(latchCount);

            o3InError = !success || o3ErrorCount.get() > 0;
            if (success && o3ErrorCount.get() > 0) {
                //noinspection ThrowFromFinallyBlock
                throw CairoException.critical(0).put("bulk update failed and will be rolled back").setOutOfMemory(o3oomObserved);
            }
        }

        if (o3LagRowCount > 0 && !metadata.isWalEnabled()) {
            LOG.info().$("shifting lag rows up [table=").$(tableToken.getTableName()).$(", lagCount=").$(o3LagRowCount).I$();
            dispatchColumnTasks(
                    o3LagRowCount,
                    IGNORE,
                    srcOooMax,
                    0L,
                    0,
                    this.cthO3ShiftColumnInLagToTopRef
            );
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
        boolean scheduleAsyncPurge = false;
        long lastCommittedTxn = this.getTxn();

        for (int i = 0; i < n; i += 2) {
            try {
                final long timestamp = partitionRemoveCandidates.getQuick(i);
                final long txn = partitionRemoveCandidates.getQuick(i + 1);
                // txn >= lastCommittedTxn means there are some versions found in the table directory
                // that are not attached to the table most likely as result of a rollback
                if (!anyReadersBeforeCommittedTxn || txn >= lastCommittedTxn) {
                    setPathForPartition(
                            other,
                            partitionBy,
                            timestamp,
                            txn
                    );
                    other.$();
                    engine.getPartitionOverwriteControl().notifyPartitionMutates(tableToken, timestamp, txn, 0);
                    if (!ff.unlinkOrRemove(other, LOG)) {
                        LOG.info()
                                .$("could not purge partition version, async purge will be scheduled [path=").$substr(pathRootSize, other)
                                .$(", errno=").$(ff.errno()).I$();
                        scheduleAsyncPurge = true;
                    }
                } else {
                    scheduleAsyncPurge = true;
                }
            } finally {
                other.trimTo(pathSize);
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
        setStateForTimestamp(other, partitionTimestamp);
        try {
            if (ff.exists(dFile(other, metadata.getColumnName(metadata.getTimestampIndex()), COLUMN_NAME_TXN_NONE))) {
                // read min timestamp value
                final long fd = TableUtils.openRO(ff, other.$(), LOG);
                try {
                    return TableUtils.readLongOrFail(ff, fd, 0, tempMem16b, other.$());
                } finally {
                    ff.close(fd);
                }
            } else {
                throw CairoException.critical(0).put("Partition does not exist [path=").put(other).put(']');
            }
        } finally {
            other.trimTo(pathSize);
        }
    }

    private void readPartitionMinMax(FilesFacade ff, long partitionTimestamp, Path path, CharSequence columnName, long partitionSize) {
        final long fd = TableUtils.openRO(ff, dFile(path, columnName, COLUMN_NAME_TXN_NONE), LOG);
        try {
            attachMinTimestamp = ff.readNonNegativeLong(fd, 0);
            attachMaxTimestamp = ff.readNonNegativeLong(fd, (partitionSize - 1) * ColumnType.sizeOf(ColumnType.TIMESTAMP));
            if (attachMinTimestamp < 0 || attachMaxTimestamp < 0) {
                throw CairoException.critical(ff.errno())
                        .put("cannot read min, max timestamp from the column [path=").put(path)
                        .put(", partitionSizeRows=").put(partitionSize)
                        .put(", errno=").put(ff.errno()).put(']');
            }
            if (txWriter.getPartitionTimestampByTimestamp(attachMinTimestamp) != partitionTimestamp
                    || txWriter.getPartitionTimestampByTimestamp(attachMaxTimestamp) != partitionTimestamp) {
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
        int pathLen = path.size();
        try {
            path.concat(TXN_FILE_NAME);
            if (ff.exists(path.$())) {
                if (attachTxReader == null) {
                    attachTxReader = new TxReader(ff);
                }
                attachTxReader.ofRO(path.$(), partitionBy);
                attachTxReader.unsafeLoadAll();

                try {
                    path.trimTo(pathLen);
                    long partitionSize = attachTxReader.getPartitionRowCountByTimestamp(partitionTimestamp);
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
            final long fd = TableUtils.openRO(ff, dFile(path.trimTo(pathLen), columnName, COLUMN_NAME_TXN_NONE), LOG);
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

    // This method is useful for debugging, it's not used in production code.
    @SuppressWarnings("unused")
    private long readTimestampRaw(long transientRowCount) {
        long offset = (transientRowCount - 1) * 8;
        long addr = mapAppendColumnBuffer(getPrimaryColumn(metadata.getTimestampIndex()), offset, 8, false);
        try {
            return Unsafe.getUnsafe().getLong(Math.abs(addr));
        } finally {
            mapAppendColumnBufferRelease(addr, offset, 8);
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

    private void rebuildAttachedPartitionColumnIndex(long partitionTimestamp, long partitionSize, CharSequence columnName) {
        if (attachIndexBuilder == null) {
            attachIndexBuilder = new IndexBuilder(configuration);

            // no need to pass table name, full partition name will be specified
            attachIndexBuilder.of(Utf8String.EMPTY);
        }

        attachIndexBuilder.reindexColumn(
                ff,
                attachColumnVersionReader,
                // use metadata instead of detachedMetadata to get correct value block capacity
                // detachedMetadata does not have the column
                metadata,
                metadata.getColumnIndex(columnName),
                -1L,
                partitionTimestamp,
                partitionBy,
                partitionSize
        );
    }

    private boolean reconcileOptimisticPartitions() {
        if (txWriter.getPartitionTimestampByIndex(txWriter.getPartitionCount() - 1) > txWriter.getMaxTimestamp()) {
            int maxTimestampPartitionIndex = txWriter.getPartitionIndex(txWriter.getMaxTimestamp());
            if (maxTimestampPartitionIndex < getPartitionCount() - 1) {
                for (int i = maxTimestampPartitionIndex + 1, n = getPartitionCount(); i < n; i++) {
                    // Schedule partitions directory deletions
                    long timestamp = txWriter.getPartitionTimestampByIndex(i);
                    long partitionTxn = txWriter.getPartitionNameTxn(i);
                    partitionRemoveCandidates.add(timestamp, partitionTxn);
                }
                txWriter.reconcileOptimisticPartitions();
                return true;
            }
        }
        return false;
    }

    private void recoverFromMetaRenameFailure(CharSequence columnName) {
        openMetaFile(ff, path, pathSize, metaMem);
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
        openMetaFile(ff, path, pathSize, metaMem);
    }

    private void recoverOpenColumnFailure(CharSequence columnName) {
        final int index = columnCount - 1;
        removeMetaFile();
        removeLastColumn();
        columnCount--;
        recoverFromSwapRenameFailure(columnName);
        removeSymbolMapWriter(index);
    }

    private void releaseIndexerWriters() {
        for (int i = 0, n = denseIndexers.size(); i < n; i++) {
            ColumnIndexer indexer = denseIndexers.getQuick(i);
            if (indexer != null) {
                indexer.releaseIndexWriter();
            }
        }
        denseIndexers.clear();
    }

    private void releaseLock(boolean distressed) {
        if (lockFd != -1L) {
            if (distressed) {
                ff.close(lockFd);
                return;
            }

            try {
                removeOrException(ff, lockFd, lockName(path));
            } finally {
                path.trimTo(pathSize);
            }
        }
    }

    private ReadOnlyObjList<? extends MemoryCR> remapWalSymbols(
            SymbolMapDiffCursor symbolMapDiffCursor,
            long rowLo,
            long rowHi,
            Path walPath
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

                    symbolColumnDest = o3MemColumns1.get(primaryColumnIndex);
                    // If rowLo != 0 then we
                    symbolColumnDest.shiftAddressRight(0);
                    symbolColumnDest.jumpTo((rowHi - rowLo) << 2);

                    o3ColumnOverrides.setQuick(primaryColumnIndex, symbolColumnDest);
                    final int cleanSymbolCount = symbolMapDiff.getCleanSymbolCount();
                    for (long rowId = rowLo; rowId < rowHi; rowId++) {

                        int symKey = o3SymbolColumn.getInt(rowId << 2);
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
                        symbolColumnDest.putInt((rowId - rowLo) << 2, symKey);
                    }
                    symbolColumnDest.shiftAddressRight(rowLo << 2);
                }
            }
        }

        if (o3ColumnOverrides.size() == 0) {
            // No mappings were made.
            return o3Columns;
        }
        return o3ColumnOverrides;
    }

    private void removeColumnFiles(int columnIndex, int columnType) {
        PurgingOperator purgingOperator = getPurgingOperator();
        long defaultNameTxn = columnVersionWriter.getDefaultColumnNameTxn(columnIndex);
        if (PartitionBy.isPartitioned(partitionBy)) {
            for (int i = txWriter.getPartitionCount() - 1; i > -1L; i--) {
                long partitionTimestamp = txWriter.getPartitionTimestampByIndex(i);
                if (!txWriter.isPartitionReadOnlyByPartitionTimestamp(partitionTimestamp)) {
                    long partitionNameTxn = txWriter.getPartitionNameTxn(i);
                    long columnNameTxn = columnVersionWriter.getColumnNameTxn(partitionTimestamp, columnIndex);
                    purgingOperator.add(columnIndex, columnNameTxn, partitionTimestamp, partitionNameTxn);
                }
            }
        } else {
            purgingOperator.add(columnIndex, defaultNameTxn, txWriter.getLastPartitionTimestamp(), -1L);
        }
        if (ColumnType.isSymbol(columnType)) {
            purgingOperator.add(columnIndex, defaultNameTxn, PurgingOperator.TABLE_ROOT_PARTITION, -1);
        }
    }

    private void removeColumnFilesInPartition(CharSequence columnName, int columnIndex, long partitionTimestamp) {
        if (!txWriter.isPartitionReadOnlyByPartitionTimestamp(partitionTimestamp)) {
            setPathForPartition(path, partitionBy, partitionTimestamp, -1);
            int plen = path.size();
            long columnNameTxn = columnVersionWriter.getColumnNameTxn(partitionTimestamp, columnIndex);
            removeFileOrLog(ff, dFile(path, columnName, columnNameTxn));
            removeFileOrLog(ff, iFile(path.trimTo(plen), columnName, columnNameTxn));
            removeFileOrLog(ff, keyFileName(path.trimTo(plen), columnName, columnNameTxn));
            removeFileOrLog(ff, valueFileName(path.trimTo(plen), columnName, columnNameTxn));
            path.trimTo(pathSize);
        } else {
            LOG.critical()
                    .$("o3 ignoring removal of column in read-only partition [table=").utf8(tableToken.getTableName())
                    .$(", column=").utf8(columnName)
                    .$(", timestamp=").$ts(partitionTimestamp)
                    .$();
        }
    }

    private int removeColumnFromMeta(int index) {
        try {
            int metaSwapIndex = openMetaSwapFile(ff, ddlMem, path, pathSize, fileOperationRetryCount);
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
                CharSequence columnName = metaMem.getStrA(nameOffset);
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
                long partitionTimestamp = txWriter.getPartitionTimestampByIndex(i);
                long partitionNameTxn = txWriter.getPartitionNameTxn(i);
                removeIndexFilesInPartition(columnName, columnIndex, partitionTimestamp, partitionNameTxn);
            }
            if (!PartitionBy.isPartitioned(partitionBy)) {
                removeColumnFilesInPartition(columnName, columnIndex, txWriter.getLastPartitionTimestamp());
            }
        } finally {
            path.trimTo(pathSize);
        }
    }

    private void removeIndexFilesInPartition(CharSequence columnName, int columnIndex, long partitionTimestamp, long partitionNameTxn) {
        setPathForPartition(path, partitionBy, partitionTimestamp, partitionNameTxn);
        int plen = path.size();
        long columnNameTxn = columnVersionWriter.getColumnNameTxn(partitionTimestamp, columnIndex);
        removeFileOrLog(ff, keyFileName(path.trimTo(plen), columnName, columnNameTxn));
        removeFileOrLog(ff, valueFileName(path.trimTo(plen), columnName, columnNameTxn));
        path.trimTo(pathSize);
    }

    private void removeLastColumn() {
        freeColumnMemory(columnCount - 1);
    }

    private void removeMetaFile() {
        try {
            path.concat(META_FILE_NAME);
            if (!ff.removeQuiet(path.$())) {
                // On Windows opened file cannot be removed
                // but can be renamed
                other.concat(META_FILE_NAME).put('.').put(configuration.getMicrosecondClock().getTicks());
                if (ff.rename(path.$(), other.$()) != FILES_RENAME_OK) {
                    LOG.error()
                            .$("could not rename [from=").$(path)
                            .$(", to=").$(other)
                            .I$();
                    throw CairoException.critical(ff.errno()).put("Recovery failed. Could not rename: ").put(path);
                }
            }
        } finally {
            path.trimTo(pathSize);
            other.trimTo(pathSize);
        }
    }

    private void removeNonAttachedPartitions() {
        LOG.debug().$("purging non attached partitions [path=").$substr(pathRootSize, path.$()).I$();
        try {
            ff.iterateDir(path.$(), removePartitionDirsNotAttached);
            processPartitionRemoveCandidates();
        } finally {
            path.trimTo(pathSize);
        }
    }

    private void removePartitionDirsNotAttached(long pUtf8NameZ, int type) {
        // Do not remove detached partitions, they are probably about to be attached
        // Do not remove wal and sequencer directories either
        int checkedType = ff.typeDirOrSoftLinkDirNoDots(path, pathSize, pUtf8NameZ, type, utf8Sink);
        if (checkedType != Files.DT_UNKNOWN &&
                !CairoKeywords.isDetachedDirMarker(pUtf8NameZ) &&
                !CairoKeywords.isWal(pUtf8NameZ) &&
                !CairoKeywords.isTxnSeq(pUtf8NameZ) &&
                !CairoKeywords.isSeq(pUtf8NameZ) &&
                !Utf8s.endsWithAscii(utf8Sink, configuration.getAttachPartitionSuffix())
        ) {
            try {
                long txn;
                int txnSep = Utf8s.indexOfAscii(utf8Sink, '.');
                if (txnSep < 0) {
                    txnSep = utf8Sink.size();
                    txn = -1;
                } else {
                    txn = Numbers.parseLong(utf8Sink, txnSep + 1, utf8Sink.size());
                }
                long dirTimestamp = partitionDirFmt.parse(utf8Sink.asAsciiCharSequence(), 0, txnSep, DateFormatUtils.EN_LOCALE);
                if (txn != txWriter.getPartitionNameTxnByPartitionTimestamp(dirTimestamp, -2)) {
                    partitionRemoveCandidates.add(dirTimestamp, txn);
                }
            } catch (NumericException ignore) {
                // not a date?
                // ignore exception and leave the directory
                path.trimTo(pathSize);
                path.concat(pUtf8NameZ).$();
                LOG.error().$("invalid partition directory inside table folder: ").$(path).$();
            }
        }
    }

    private void removeSymbolMapFilesQuiet(CharSequence name, long columnNamTxn) {
        try {
            removeFileOrLog(ff, offsetFileName(path.trimTo(pathSize), name, columnNamTxn));
            removeFileOrLog(ff, charFileName(path.trimTo(pathSize), name, columnNamTxn));
            removeFileOrLog(ff, keyFileName(path.trimTo(pathSize), name, columnNamTxn));
            removeFileOrLog(ff, valueFileName(path.trimTo(pathSize), name, columnNamTxn));
        } finally {
            path.trimTo(pathSize);
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
            int l = other.size();

            do {
                if (index > 0) {
                    other.trimTo(l);
                    other.put('.').put(index);
                }

                if (!ff.removeQuiet(other.$())) {
                    LOG.info().$("could not remove target of rename '").$(path).$("' to '").$(other).$(" [errno=").$(ff.errno()).I$();
                    index++;
                    continue;
                }

                if (ff.rename(path.$(), other.$()) != FILES_RENAME_OK) {
                    LOG.info().$("could not rename '").$(path).$("' to '").$(other).$(" [errno=").$(ff.errno()).I$();
                    index++;
                    continue;
                }

                return index;

            } while (index < retries);

            throw CairoException.critical(0)
                    .put("could not rename ").put(path)
                    .put(". Max number of attempts reached [").put(index)
                    .put("]. Last target was: ").put(other);
        } finally {
            path.trimTo(pathSize);
            other.trimTo(pathSize);
        }
    }

    private int renameColumnFromMeta(int index, CharSequence newName) {
        try {
            int metaSwapIndex = openMetaSwapFile(ff, ddlMem, path, pathSize, fileOperationRetryCount);
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
                CharSequence columnName = metaMem.getStrA(nameOffset);
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
        if (txWriter.getMaxTimestamp() != Numbers.LONG_NULL && PartitionBy.isPartitioned(partitionBy)) {
            long fixedRowCount = 0;
            long lastTimestamp = -1;
            long transientRowCount = txWriter.getTransientRowCount();
            long maxTimestamp = txWriter.getMaxTimestamp();
            try {
                final long tsLimit = txWriter.getPartitionTimestampByTimestamp(txWriter.getMaxTimestamp());
                for (long ts = txWriter.getPartitionTimestampByTimestamp(txWriter.getMinTimestamp()); ts < tsLimit; ts = txWriter.getNextPartitionTimestamp(ts)) {
                    path.trimTo(pathSize);
                    setStateForTimestamp(path, ts);
                    int p = path.size();

                    long partitionSize = txWriter.getPartitionRowCountByTimestamp(ts);
                    if (partitionSize >= 0 && ff.exists(path.$())) {
                        fixedRowCount += partitionSize;
                        lastTimestamp = ts;
                    } else {
                        Path other = Path.getThreadLocal2(path.trimTo(p));
                        TableUtils.oldPartitionName(other, getTxn());
                        if (ff.exists(other.$())) {
                            if (ff.rename(other.$(), path.$()) != FILES_RENAME_OK) {
                                LOG.error().$("could not rename [from=").$(other).$(", to=").$(path).I$();
                                throw new CairoError("could not restore directory, see log for details");
                            } else {
                                LOG.info().$("restored [path=").$substr(pathRootSize, path).I$();
                            }
                        } else {
                            LOG.debug().$("missing partition [name=").$(path.trimTo(p).$()).I$();
                        }
                    }
                }

                if (lastTimestamp > -1) {
                    path.trimTo(pathSize);
                    setStateForTimestamp(path, tsLimit);
                    if (!ff.exists(path.$())) {
                        Path other = Path.getThreadLocal2(path);
                        TableUtils.oldPartitionName(other, getTxn());
                        if (ff.exists(other.$())) {
                            if (ff.rename(other.$(), path.$()) != FILES_RENAME_OK) {
                                LOG.error().$("could not rename [from=").$(other).$(", to=").$(path).I$();
                                throw new CairoError("could not restore directory, see log for details");
                            } else {
                                LOG.info().$("restored [path=").$substr(pathRootSize, path).I$();
                            }
                        } else {
                            LOG.error().$("last partition does not exist [name=").$(path).I$();
                            // ok, create last partition we discovered the active
                            // 1. read its size
                            path.trimTo(pathSize);
                            setStateForTimestamp(path, lastTimestamp);
                            int p = path.size();
                            transientRowCount = txWriter.getPartitionRowCountByTimestamp(lastTimestamp);

                            // 2. read max timestamp
                            TableUtils.dFile(path.trimTo(p), metadata.getColumnName(metadata.getTimestampIndex()), COLUMN_NAME_TXN_NONE);
                            maxTimestamp = TableUtils.readLongAtOffset(ff, path.$(), tempMem16b, (transientRowCount - 1) * Long.BYTES);
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
                path.trimTo(pathSize);
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

            if (ff.exists(path.$())) {
                LOG.info().$("Repairing metadata from: ").$substr(pathRootSize, path).$();
                ff.remove(other.concat(META_FILE_NAME).$());

                if (ff.rename(path.$(), other.$()) != FILES_RENAME_OK) {
                    throw CairoException.critical(ff.errno()).put("Repair failed. Cannot rename ").put(path).put(" -> ").put(other);
                }
            }
        } finally {
            path.trimTo(pathSize);
            other.trimTo(pathSize);
        }

        clearTodoLog();
    }

    private void repairTruncate() {
        LOG.info().$("repairing abnormally terminated truncate on ").$substr(pathRootSize, path).$();
        scheduleRemoveAllPartitions();
        txWriter.truncate(columnVersionWriter.getVersion(), denseSymbolMapWriters);
        clearTodoLog();
        processPartitionRemoveCandidates();
    }

    private void resizePartitionUpdateSink() {
        if (o3PartitionUpdateSink == null) {
            o3PartitionUpdateSink = new PagedDirectLongList(MemoryTag.NATIVE_O3);
        }
        o3PartitionUpdateSink.clear();
        o3PartitionUpdateSink.setBlockSize(PARTITION_SINK_SIZE_LONGS + metadata.getColumnCount());
    }

    private void restoreMetaFrom(CharSequence fromBase, int fromIndex) {
        try {
            path.concat(fromBase);
            if (fromIndex > 0) {
                path.put('.').put(fromIndex);
            }
            path.$();

            TableUtils.renameOrFail(ff, path.$(), other.concat(META_FILE_NAME).$());
        } finally {
            path.trimTo(pathSize);
            other.trimTo(pathSize);
        }
    }

    private void rollbackIndexes() {
        final long maxRow = txWriter.getTransientRowCount() - 1;
        for (int i = 0, n = denseIndexers.size(); i < n; i++) {
            ColumnIndexer indexer = denseIndexers.getQuick(i);
            long fd = indexer.getFd();
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

    private void scheduleRemoveAllPartitions() {
        for (int i = txWriter.getPartitionCount() - 1; i > -1L; i--) {
            long timestamp = txWriter.getPartitionTimestampByIndex(i);
            long partitionTxn = txWriter.getPartitionNameTxn(i);
            partitionRemoveCandidates.add(timestamp, partitionTxn);
        }
    }

    private void setAppendPosition(final long rowCount, boolean doubleAllocate) {
        long recordLength = 0;
        for (int i = 0; i < columnCount; i++) {
            recordLength += setColumnAppendPosition(i, rowCount, doubleAllocate);
        }
        avgRecordSize = rowCount > 0 ? recordLength / rowCount : Math.max(avgRecordSize, recordLength);
    }

    private long setColumnAppendPosition(int columnIndex, long size, boolean doubleAllocate) {
        long dataSizeBytes = 0;
        try {
            MemoryMA dataMem = getPrimaryColumn(columnIndex);
            MemoryMA auxMem = getSecondaryColumn(columnIndex);
            int columnType = metadata.getColumnType(columnIndex);
            if (columnType > 0) { // Not deleted
                final long pos = size - getColumnTop(columnIndex);
                if (ColumnType.isVarSize(columnType)) {
                    ColumnTypeDriver driver = ColumnType.getDriver(columnType);
                    dataSizeBytes = driver.setAppendPosition(
                            pos,
                            auxMem,
                            dataMem
                    ) - driver.getMinAuxVectorSize();
                } else {
                    dataSizeBytes = pos << ColumnType.pow2SizeOf(columnType);
                    if (doubleAllocate) {
                        dataMem.allocate(dataSizeBytes);
                    }
                    dataMem.jumpTo(dataSizeBytes);

                }
            }
        } catch (CairoException e) {
            throwDistressException(e);
        }

        return dataSizeBytes;
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
     * @param path      path instance to modify
     * @param timestamp to determine interval for
     */
    private void setStateForTimestamp(Path path, long timestamp) {
        // When partition is create a txn name must always be set to purge dropped partitions.
        // When partition is created outside O3 merge use `txn-1` as the version
        long partitionTxnName = PartitionBy.isPartitioned(partitionBy) ? txWriter.getTxn() - 1 : -1;
        partitionTxnName = txWriter.getPartitionNameTxnByPartitionTimestamp(timestamp, partitionTxnName);
        TableUtils.setPathForPartition(path, partitionBy, timestamp, partitionTxnName);
    }

    private void shrinkO3Mem() {
        for (int i = 0, n = o3MemColumns1.size(); i < n; i++) {
            MemoryCARW o3mem = o3MemColumns1.getQuick(i);
            if (o3mem != null) {
                // truncate will shrink the memory to a single page
                o3mem.truncate();
            }
        }
        for (int i = 0, n = o3MemColumns2.size(); i < n; i++) {
            MemoryCARW o3mem2 = o3MemColumns2.getQuick(i);
            if (o3mem2 != null) {
                o3mem2.truncate();
            }
        }
    }

    private void squashPartitionForce(int partitionIndex) {
        int lastLogicalPartitionIndex = partitionIndex;
        long lastLogicalPartitionTimestamp = txWriter.getPartitionTimestampByIndex(partitionIndex);
        assert lastLogicalPartitionTimestamp == txWriter.getLogicalPartitionTimestamp(lastLogicalPartitionTimestamp);

        // Do not cache txWriter.getPartitionCount() as it changes during the squashing
        while (partitionIndex < txWriter.getPartitionCount()) {
            long partitionTimestamp = txWriter.getPartitionTimestampByIndex(partitionIndex);
            long logicalPartitionTimestamp = txWriter.getLogicalPartitionTimestamp(partitionTimestamp);
            if (logicalPartitionTimestamp != lastLogicalPartitionTimestamp) {
                if (partitionIndex > lastLogicalPartitionIndex + 1) {
                    squashSplitPartitions(lastLogicalPartitionIndex, partitionIndex, 1, true);
                }
                return;
            }
            partitionIndex++;
        }
        if (partitionIndex > lastLogicalPartitionIndex + 1) {
            squashSplitPartitions(lastLogicalPartitionIndex, partitionIndex, 1, true);
        }
    }

    private void squashPartitionRange(int maxLastSubPartitionCount, int partitionIndexLo, int partitionIndexHi) {
        if (partitionIndexHi > partitionIndexLo) {
            int subpartitions = partitionIndexHi - partitionIndexLo;
            int optimalPartitionCount = partitionIndexHi == txWriter.getPartitionCount() ? maxLastSubPartitionCount : MAX_MID_SUB_PARTITION_COUNT;
            if (subpartitions > Math.max(1, optimalPartitionCount)) {
                squashSplitPartitions(partitionIndexLo, partitionIndexHi, optimalPartitionCount, false);
            } else if (subpartitions == 1) {
                if (partitionIndexLo >= 0 &&
                        partitionIndexLo < txWriter.getPartitionCount() && minSplitPartitionTimestamp == txWriter.getPartitionTimestampByIndex(partitionIndexLo)) {
                    minSplitPartitionTimestamp = getPartitionTimestampOrMax(partitionIndexLo + 1);
                }
            }
        }
    }

    private long squashPartitionTimestamp(long ts) {
        int partitionIndex = txWriter.findAttachedPartitionIndexByLoTimestamp(ts);
        if (partitionIndex < 0) {
            partitionIndex = -partitionIndex - 1;
        }
        if (partitionIndex >= txWriter.getPartitionCount()) {
            return Long.MAX_VALUE;
        }
        return txWriter.getLogicalPartitionTimestamp(txWriter.getPartitionTimestampByIndex(partitionIndex));
    }

    private void squashSplitPartitions(long timestampMin, long timestampMax, int maxLastSubPartitionCount) {
        if (timestampMin > txWriter.getMaxTimestamp() || txWriter.getPartitionCount() < 2) {
            return;
        }

        // Take the control of split partition population here.
        // When the number of split partitions is too big, start merging them together.
        // This is to avoid having too many partitions / files in the system which penalizes the reading performance.
        long logicalPartition = squashPartitionTimestamp(timestampMin);
        int partitionIndex = txWriter.getPartitionIndex(logicalPartition);
        if (partitionIndex > -1) {
            int partitionIndexLo = partitionIndex;
            int partitionCount = txWriter.getPartitionCount();

            while (logicalPartition < timestampMax && ++partitionIndex < partitionCount) {
                long partitionTimestamp = txWriter.getPartitionTimestampByIndex(partitionIndex);
                long newLogicalPartition = txWriter.getLogicalPartitionTimestamp(partitionTimestamp);

                if (logicalPartition != newLogicalPartition) {
                    squashPartitionRange(maxLastSubPartitionCount, partitionIndexLo, partitionIndex);

                    // txn records can be changed by squashing. Reset the position and the partition count.
                    partitionCount = txWriter.getPartitionCount();
                    partitionIndex = txWriter.getPartitionIndex(newLogicalPartition);

                    // switch to the next logical partition
                    logicalPartition = newLogicalPartition;
                    partitionIndexLo = partitionIndex;
                }
            }

            // This can shift last partition timestamp, save what was the last partition timestamp before squashing
            long lastPartitionTimestamp = txWriter.getLastPartitionTimestamp();
            squashPartitionRange(maxLastSubPartitionCount, partitionIndexLo, partitionIndex);
            if (lastPartitionTimestamp != txWriter.getLastPartitionTimestamp()) {
                openLastPartition();
            }
        }
    }

    private void squashSplitPartitions(final int partitionIndexLo, final int partitionIndexHi, final int optimalPartitionCount, boolean force) {
        if (partitionIndexHi <= partitionIndexLo + Math.max(1, optimalPartitionCount)) {
            // Nothing to do
            return;
        }

        if (checkpointStatus.isInProgress()) {
            LOG.info().$("cannot squash partition [table=").$(tableToken.getTableName()).$("], checkpoint in progress").$();
            return;
        }

        assert partitionIndexHi >= 0 && partitionIndexHi <= txWriter.getPartitionCount() && partitionIndexLo >= 0;

        long targetPartition = Long.MIN_VALUE;
        boolean copyTargetFrame = false;

        // Move targetPartitionIndex to the first unlocked partition in the range
        int targetPartitionIndex = partitionIndexLo;
        for (int n = partitionIndexHi - 1; targetPartitionIndex < n; targetPartitionIndex++) {
            boolean canOverwrite = canSquashOverwritePartitionTail(targetPartitionIndex);
            if (canOverwrite || force) {
                targetPartition = txWriter.getPartitionTimestampByIndex(targetPartitionIndex);
                copyTargetFrame = !canOverwrite;
                break;
            }
        }
        if (targetPartition == Long.MIN_VALUE) {
            return;
        }

        boolean lastPartitionSquashed = false;
        int squashCount = Math.min(partitionIndexHi - targetPartitionIndex - 1, partitionIndexHi - partitionIndexLo - optimalPartitionCount);

        if (squashCount <= 0) {
            // There are fewer partitions we can squash than optimalPartitionCount
            return;
        }

        long targetPartitionNameTxn = txWriter.getPartitionNameTxnByPartitionTimestamp(targetPartition);
        TableUtils.setPathForPartition(path, partitionBy, targetPartition, targetPartitionNameTxn);
        final long originalSize = txWriter.getPartitionRowCountByTimestamp(targetPartition);

        boolean rw = !copyTargetFrame;
        Frame targetFrame = null;
        Frame firstPartitionFrame = frameFactory.open(rw, path, targetPartition, metadata, columnVersionWriter, originalSize);
        try {
            if (copyTargetFrame) {
                try {
                    TableUtils.setPathForPartition(other, partitionBy, targetPartition, txWriter.txn);
                    TableUtils.createDirsOrFail(ff, other.slash(), configuration.getMkDirMode());
                    LOG.info().$("copying partition to force squash [from=").$substr(pathRootSize, path).$(", to=").$(other).I$();

                    targetFrame = frameFactory.openRW(other, targetPartition, metadata, columnVersionWriter, 0);
                    FrameAlgebra.append(targetFrame, firstPartitionFrame, configuration.getCommitMode());
                    addPhysicallyWrittenRows(firstPartitionFrame.getRowCount());
                    txWriter.updatePartitionSizeAndTxnByRawIndex(targetPartitionIndex * LONGS_PER_TX_ATTACHED_PARTITION, originalSize);
                    partitionRemoveCandidates.add(targetPartition, targetPartitionNameTxn);
                    targetPartitionNameTxn = txWriter.txn;
                } finally {
                    Misc.free(firstPartitionFrame);
                }
            } else {
                targetFrame = firstPartitionFrame;
            }

            engine.getPartitionOverwriteControl().notifyPartitionMutates(
                    tableToken,
                    targetPartition,
                    targetPartitionNameTxn,
                    targetFrame.getRowCount()
            );
            for (int i = 0; i < squashCount; i++) {
                long sourcePartition = txWriter.getPartitionTimestampByIndex(targetPartitionIndex + 1);

                other.trimTo(pathSize);
                long sourceNameTxn = txWriter.getPartitionNameTxnByPartitionTimestamp(sourcePartition);
                TableUtils.setPathForPartition(other, partitionBy, sourcePartition, sourceNameTxn);
                long partitionRowCount = txWriter.getPartitionRowCountByTimestamp(sourcePartition);
                lastPartitionSquashed = targetPartitionIndex + 2 == txWriter.getPartitionCount();
                if (lastPartitionSquashed) {
                    closeActivePartition(false);
                    partitionRowCount = txWriter.getTransientRowCount() + txWriter.getLagRowCount();
                }

                assert partitionRowCount > 0;

                LOG.info().$("squashing partitions [table=").$(tableToken)
                        .$(", target=").$(formatPartitionForTimestamp(targetPartition, targetPartitionNameTxn))
                        .$(", targetSize=").$(targetFrame.getRowCount())
                        .$(", source=").$(formatPartitionForTimestamp(sourcePartition, sourceNameTxn))
                        .$(", sourceSize=").$(partitionRowCount)
                        .I$();

                try (Frame sourceFrame = frameFactory.openRO(other, sourcePartition, metadata, columnVersionWriter, partitionRowCount)) {
                    FrameAlgebra.append(targetFrame, sourceFrame, configuration.getCommitMode());
                    addPhysicallyWrittenRows(sourceFrame.getRowCount());
                } catch (Throwable th) {
                    LOG.critical().$("partition squashing failed [table=").$(tableToken).$(", error=").$(th).I$();
                    throw th;
                }

                txWriter.removeAttachedPartitions(sourcePartition);
                columnVersionWriter.removePartition(sourcePartition);
                partitionRemoveCandidates.add(sourcePartition, sourceNameTxn);
                if (sourcePartition == minSplitPartitionTimestamp) {
                    minSplitPartitionTimestamp = getPartitionTimestampOrMax(targetPartitionIndex + 1);
                }
            }

            txWriter.updatePartitionSizeByTimestamp(targetPartition, targetFrame.getRowCount());
            if (lastPartitionSquashed) {
                // last partition is squashed, adjust fixed/transient row sizes
                long newTransientRowCount = targetFrame.getRowCount() - txWriter.getLagRowCount();
                assert newTransientRowCount >= 0;
                txWriter.fixedRowCount += txWriter.getTransientRowCount() - newTransientRowCount;
                assert txWriter.fixedRowCount >= 0;
                txWriter.transientRowCount = newTransientRowCount;
            }
        } finally {
            Misc.free(targetFrame);
            path.trimTo(pathSize);
            other.trimTo(pathSize);
        }

        // Commit the new transaction with the partitions squashed
        columnVersionWriter.commit();
        txWriter.setColumnVersion(columnVersionWriter.getVersion());
        txWriter.commit(denseSymbolMapWriters);
        processPartitionRemoveCandidates();
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
            openMetaFile(ff, path, pathSize, metaMem);
            // remove _todo
            clearTodoLog();
        } catch (CairoException e) {
            throwDistressException(e);
        }
        bumpMetadataVersion();
    }

    private void swapO3ColumnsExcept(int timestampIndex) {
        ObjList<MemoryCARW> temp = o3MemColumns1;
        o3MemColumns1 = o3MemColumns2;
        o3MemColumns2 = temp;

        // Swap timestamp column back, timestamp column is not sorted, it's the sort key.
        final int timestampMemoryIndex = getPrimaryColumnIndex(timestampIndex);
        o3MemColumns2.setQuick(
                timestampMemoryIndex,
                o3MemColumns1.getAndSetQuick(timestampMemoryIndex, o3MemColumns2.getQuick(timestampMemoryIndex))
        );
        o3Columns = o3MemColumns1;
        activeColumns = o3MemColumns1;

        ObjList<Runnable> tempNullSetters = o3NullSetters1;
        o3NullSetters1 = o3NullSetters2;
        o3NullSetters2 = tempNullSetters;
        activeNullSetters = o3NullSetters1;
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

    private void syncColumns() {
        final int commitMode = configuration.getCommitMode();
        if (commitMode != CommitMode.NOSYNC) {
            final boolean async = commitMode == CommitMode.ASYNC;
            syncColumns0(async);
            for (int i = 0, n = denseIndexers.size(); i < n; i++) {
                denseIndexers.getQuick(i).sync(async);
            }
            for (int i = 0, n = denseSymbolMapWriters.size(); i < n; i++) {
                denseSymbolMapWriters.getQuick(i).sync(async);
            }
        }
    }

    private void syncColumns0(boolean async) {
        for (int i = 0; i < columnCount; i++) {
            columns.getQuick(i * 2).sync(async);
            final MemoryMA m2 = columns.getQuick(i * 2 + 1);
            if (m2 != null) {
                m2.sync(async);
            }
        }
    }

    private void throwDistressException(CairoException cause) {
        LOG.critical().$("writer error [table=").utf8(tableToken.getTableName()).$(", e=").$((Sinkable) cause).I$();
        distressed = true;
        throw new CairoError(cause);
    }

    private void truncate(boolean keepSymbolTables) {
        rollback();

        if (!keepSymbolTables) {
            // we do this before size check so that "old" corrupt symbol tables are brought back in line
            for (int i = 0, n = denseSymbolMapWriters.size(); i < n; i++) {
                denseSymbolMapWriters.getQuick(i).truncate();
            }
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
            releaseIndexerWriters();
            // Schedule removal of all partitions
            scheduleRemoveAllPartitions();
            rowAction = ROW_ACTION_OPEN_PARTITION;
        } else {
            // truncate columns, we cannot remove them
            truncateColumns();
        }

        txWriter.resetTimestamp();
        columnVersionWriter.truncate();
        txWriter.truncate(columnVersionWriter.getVersion(), denseSymbolMapWriters);
        try {
            clearTodoLog();
        } catch (CairoException e) {
            throwDistressException(e);
        }
        this.minSplitPartitionTimestamp = Long.MAX_VALUE;
        processPartitionRemoveCandidates();

        LOG.info().$("truncated [name=").utf8(tableToken.getTableName()).I$();

        try (MetadataCacheWriter metadataRW = engine.getMetadataCache().writeLock()) {
            metadataRW.hydrateTable(metadata);
        }
    }

    private void truncateColumns() {
        for (int i = 0; i < columnCount; i++) {
            final int columnType = metadata.getColumnType(i);
            if (columnType >= 0) {
                getPrimaryColumn(i).truncate();
                if (ColumnType.isVarSize(columnType)) {
                    MemoryMA auxMem = getSecondaryColumn(i);
                    auxMem.truncate();
                    ColumnType.getDriver(columnType).configureAuxMemMA(auxMem);
                }
            }
        }
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
        } finally {
            ddlMem.close();
        }
    }

    private void updateMetadataWithDeduplicationUpsertKeys(boolean enable, LongList columnsIndexes) {
        try {
            int index = openMetaSwapFile(ff, ddlMem, path, pathSize, configuration.getMaxSwapFileCount());
            int columnCount = metaMem.getInt(META_OFFSET_COUNT);

            ddlMem.putInt(columnCount);
            ddlMem.putInt(metaMem.getInt(META_OFFSET_PARTITION_BY));
            ddlMem.putInt(metaMem.getInt(META_OFFSET_TIMESTAMP_INDEX));
            copyVersionAndLagValues();
            ddlMem.jumpTo(META_OFFSET_COLUMN_TYPES);
            for (int i = 0; i < columnCount; i++) {
                writeColumnEntryWithDedupFlag(i, enable && columnsIndexes.indexOf(i) >= 0);
            }

            long nameOffset = getColumnNameOffset(columnCount);
            for (int i = 0; i < columnCount; i++) {
                CharSequence columnName = metaMem.getStrA(nameOffset);
                ddlMem.putStr(columnName);
                nameOffset += Vm.getStorageLength(columnName);
            }
            this.metaSwapIndex = index;
        } finally {
            ddlMem.close();
        }

        finishMetaSwapUpdateStructural();

        for (int i = 0; i < columnCount; i++) {
            metadata.getColumnMetadata(i).setDedupKeyFlag(enable && columnsIndexes.indexOf(i) >= 0);
        }

        if (enable) {
            if (dedupColumnCommitAddresses == null) {
                dedupColumnCommitAddresses = new DedupColumnCommitAddresses();
            } else {
                dedupColumnCommitAddresses.clear();
            }
            dedupColumnCommitAddresses.setDedupColumnCount(columnsIndexes.size() - 1);
        } else {
            if (dedupColumnCommitAddresses != null) {
                dedupColumnCommitAddresses.setDedupColumnCount(0);
            }
        }
    }

    private void updateO3ColumnTops() {
        int columnCount = metadata.getColumnCount();
        long blockIndex = -1;

        while ((blockIndex = o3PartitionUpdateSink.nextBlockIndex(blockIndex)) > -1L) {
            long blockAddress = o3PartitionUpdateSink.getBlockAddress(blockIndex);
            long partitionTimestamp = Unsafe.getUnsafe().getLong(blockAddress);
            final long o3SplitPartitionSize = Unsafe.getUnsafe().getLong(blockAddress + 5 * Long.BYTES);
            // When partition is split, data partition timestamp and partition timestamp diverge
            final long dataPartitionTimestamp = Unsafe.getUnsafe().getLong(blockAddress + 6 * Long.BYTES);

            if (o3SplitPartitionSize > 0) {
                // This is partition split. Copy all the column name txns from the donor partition.
                columnVersionWriter.copyColumnVersions(dataPartitionTimestamp, partitionTimestamp);
            }

            if (partitionTimestamp > -1) {
                blockAddress += PARTITION_SINK_SIZE_LONGS * Long.BYTES;
                for (int column = 0; column < columnCount; column++) {

                    long colTop = Unsafe.getUnsafe().getLong(blockAddress);
                    blockAddress += Long.BYTES;
                    if (colTop > -1L) {
                        // Upsert even when colTop value is 0.
                        // TableReader uses the record to determine if the column is supposed to be present for the partition.
                        columnVersionWriter.upsertColumnTop(partitionTimestamp, column, colTop);
                    } else if (o3SplitPartitionSize > 0) {
                        // Remove column tops for the new partition part.
                        columnVersionWriter.removeColumnTop(partitionTimestamp, column);
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
                path.trimTo(pathSize);
            }
        } catch (CairoException e) {
            runFragile(RECOVER_FROM_META_RENAME_FAILURE, columnName, e);
        }
    }

    private void writeColumnEntry(int i, boolean changeToDeleted) {
        int columnType = getColumnType(metaMem, i);
        // When column is deleted it's written to metadata with negative type
        if (changeToDeleted) {
            columnType = -Math.abs(columnType);
        }
        ddlMem.putInt(columnType);
        long flags = getColumnFlags(metaMem, i);
        ddlMem.putLong(flags);
        ddlMem.putInt(getIndexBlockCapacity(metaMem, i));
        ddlMem.skip(8);
        ddlMem.putInt(getReplacingColumnIndexRaw(metaMem, i));
        ddlMem.skip(4);
    }

    private void writeColumnEntryWithDedupFlag(int i, boolean changeToDedup) {
        int columnType = getColumnType(metaMem, i);
        // When column is deleted it's written to metadata with negative type
        ddlMem.putInt(columnType);
        long flags = getColumnFlags(metaMem, i);
        if (changeToDedup) {
            flags |= META_FLAG_BIT_DEDUP_KEY;
        } else {
            flags &= ~META_FLAG_BIT_DEDUP_KEY;
        }
        ddlMem.putLong(flags);
        ddlMem.putInt(getIndexBlockCapacity(metaMem, i));
        ddlMem.skip(16);
    }

    private void writeIndex(@NotNull CharSequence columnName, int indexValueBlockSize, int columnIndex, SymbolColumnIndexer indexer) {
        // create indexer
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
                    indexHistoricPartitions(indexer, columnName, indexValueBlockSize, columnIndex);
                    long timestamp = txWriter.getLastPartitionTimestamp();
                    if (timestamp != Numbers.LONG_NULL) {
                        path.trimTo(pathSize);
                        setStateForTimestamp(path, timestamp);
                        // create index in last partition
                        indexLastPartition(indexer, columnName, columnNameTxn, columnIndex, indexValueBlockSize);
                    }
                } else {
                    setStateForTimestamp(path, 0);
                    // create index in last partition
                    indexLastPartition(indexer, columnName, columnNameTxn, columnIndex, indexValueBlockSize);
                }
            } finally {
                path.trimTo(pathSize);
            }
        } catch (Throwable e) {
            Misc.free(indexer);
            LOG.error().$("rolling back index created so far [path=").$substr(pathRootSize, path).I$();
            removeIndexFiles(columnName, columnIndex);
            throw e;
        }
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
        todoMem.sync(false);
    }

    static void indexAndCountDown(ColumnIndexer indexer, long lo, long hi, SOCountDownLatch latch) {
        try {
            indexer.refreshSourceAndIndex(lo, hi);
        } catch (CairoException e) {
            indexer.distress();
            LOG.critical().$("index error [fd=").$(indexer.getFd()).$("]{").$((Sinkable) e).$('}').$();
        } finally {
            latch.countDown();
        }
    }

    boolean allowMixedIO() {
        return mixedIOFlag;
    }

    void closeActivePartition(long size) {
        for (int i = 0; i < columnCount; i++) {
            setColumnAppendPosition(i, size, false);
            Misc.free(getPrimaryColumn(i));
            Misc.free(getSecondaryColumn(i));
        }
        releaseIndexerWriters();
    }

    BitmapIndexWriter getBitmapIndexWriter(int columnIndex) {
        return indexers.getQuick(columnIndex).getWriter();
    }

    long getColumnTop(int columnIndex) {
        assert lastOpenPartitionTs != Long.MIN_VALUE;
        return columnVersionWriter.getColumnTopQuick(lastOpenPartitionTs, columnIndex);
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

    long getPartitionNameTxnByRawIndex(int index) {
        return txWriter.getPartitionNameTxnByRawIndex(index);
    }

    long getPartitionSizeByRawIndex(int index) {
        return txWriter.getPartitionSizeByRawIndex(index);
    }

    TxReader getTxReader() {
        return txWriter;
    }

    void o3ClockDownPartitionUpdateCount() {
        o3PartitionUpdRemaining.decrementAndGet();
    }

    void o3CountDownDoneLatch() {
        o3DoneLatch.countDown();
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
                        setStateForTimestamp(path, txWriter.getPartitionTimestampByTimestamp(dirtyMaxTimestamp));
                        if (!ff.rmdir(path)) {
                            throw CairoException.critical(ff.errno()).put("could not remove directory: ").put(path);
                        }
                        removeDirOnCancelRow = false;
                    } finally {
                        path.trimTo(pathSize);
                    }
                }

                // open old partition
                if (rollbackToMaxTimestamp > Long.MIN_VALUE) {
                    try {
                        txWriter.setMaxTimestamp(rollbackToMaxTimestamp);
                        openPartition(rollbackToMaxTimestamp);
                        setAppendPosition(rollbackToTransientRowCount, false);
                    } catch (Throwable e) {
                        freeColumns(false);
                        throw e;
                    }
                } else {
                    // we have no partitions, clear partitions in TableWriter
                    txWriter.removeAllPartitions();
                    rowAction = ROW_ACTION_OPEN_PARTITION;
                }

                // undo counts
                removeDirOnCancelRow = true;
                txWriter.cancelRow();
            } else {
                txWriter.cancelRow();
                // we only have one partition, jump to start on every column
                truncateColumns();
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

        // Some executions path in this method already call txWriter.removeAllPartitions()
        // which resets transientRowCount.
        if (txWriter.transientRowCount > 0) {
            txWriter.transientRowCount--;
        }
    }

    @FunctionalInterface
    public interface ColumnTaskHandler {
        void run(
                int columnIndex,
                final int columnType,
                final long timestampColumnIndex,
                long long0,
                long long1,
                long long2,
                long long3,
                long long4
        );
    }

    @FunctionalInterface
    public interface ExtensionListener {
        void onTableExtended(long timestamp);
    }

    @FunctionalInterface
    private interface FragileCode {
        void run(CharSequence columnName);
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

        void putGeoHashDeg(int columnIndex, double lat, double lon);

        void putGeoStr(int columnIndex, CharSequence value);

        void putGeoVarchar(int columnIndex, Utf8Sequence value);

        void putIPv4(int columnIndex, int value);

        void putInt(int columnIndex, int value);

        void putLong(int columnIndex, long value);

        void putLong128(int columnIndex, long lo, long hi);

        void putLong256(int columnIndex, long l0, long l1, long l2, long l3);

        void putLong256(int columnIndex, Long256 value);

        void putLong256(int columnIndex, CharSequence hexString);

        void putLong256(int columnIndex, @NotNull CharSequence hexString, int start, int end);

        void putLong256Utf8(int columnIndex, DirectUtf8Sequence hexString);

        void putShort(int columnIndex, short value);

        void putStr(int columnIndex, CharSequence value);

        void putStr(int columnIndex, char value);

        void putStr(int columnIndex, CharSequence value, int pos, int len);

        /**
         * Writes UTF8-encoded string to WAL. As the name of the function suggest the storage format is
         * expected to be UTF16. The function must re-encode string from UTF8 to UTF16 before storing.
         *
         * @param columnIndex index of the column we are writing to
         * @param value       UTF8 bytes represented as CharSequence interface.
         *                    On this interface getChar() returns a byte, not complete character.
         */
        void putStrUtf8(int columnIndex, DirectUtf8Sequence value);

        void putSym(int columnIndex, CharSequence value);

        void putSym(int columnIndex, char value);

        void putSymIndex(int columnIndex, int key);

        /**
         * Writes UTF8-encoded symbol to WAL. Supported for WAL tables only.
         */
        void putSymUtf8(int columnIndex, DirectUtf8Sequence value);

        void putTimestamp(int columnIndex, long value);

        void putUuid(int columnIndex, CharSequence uuid);

        void putUuidUtf8(int columnIndex, Utf8Sequence uuid);

        void putVarchar(int columnIndex, char value);

        void putVarchar(int columnIndex, Utf8Sequence value);
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
            // no-op
        }

        @Override
        public void putGeoVarchar(int columnIndex, Utf8Sequence value) {
            // no-op
        }

        @Override
        public void putIPv4(int columnIndex, int value) {
            // no-op
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
        public void putLong256Utf8(int columnIndex, DirectUtf8Sequence hexString) {
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
        public void putStrUtf8(int columnIndex, DirectUtf8Sequence value) {
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
        public void putSymUtf8(int columnIndex, DirectUtf8Sequence value) {
            // no-op
        }

        @Override
        public void putTimestamp(int columnIndex, long value) {
            // no-op
        }

        @Override
        public void putUuid(int columnIndex, CharSequence uuid) {
            // no-op
        }

        @Override
        public void putUuidUtf8(int columnIndex, Utf8Sequence uuid) {
            // no-op
        }

        @Override
        public void putVarchar(int columnIndex, char value) {
            // no-op
        }

        @Override
        public void putVarchar(int columnIndex, Utf8Sequence value) {
            // no-op
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
        public void putGeoHash(int columnIndex, long value) {
            int type = metadata.getColumnType(columnIndex);
            WriterRowUtils.putGeoHash(columnIndex, value, type, this);
        }

        @Override
        public void putGeoHashDeg(int columnIndex, double lat, double lon) {
            int type = metadata.getColumnType(columnIndex);
            WriterRowUtils.putGeoHash(columnIndex, GeoHashes.fromCoordinatesDegUnsafe(lat, lon, ColumnType.getGeoHashBits(type)), type, this);
        }

        @Override
        public void putGeoStr(int columnIndex, CharSequence hash) {
            final int type = metadata.getColumnType(columnIndex);
            WriterRowUtils.putGeoStr(columnIndex, hash, type, this);
        }

        @Override
        public void putGeoVarchar(int columnIndex, Utf8Sequence hash) {
            final int type = metadata.getColumnType(columnIndex);
            WriterRowUtils.putGeoVarchar(columnIndex, hash, type, this);
        }

        @Override
        public void putIPv4(int columnIndex, int value) {
            putInt(columnIndex, value);
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
        public void putLong256Utf8(int columnIndex, DirectUtf8Sequence hexString) {
            getPrimaryColumn(columnIndex).putLong256Utf8(hexString);
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
        public void putStrUtf8(int columnIndex, DirectUtf8Sequence value) {
            getSecondaryColumn(columnIndex).putLong(getPrimaryColumn(columnIndex).putStrUtf8(value));
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
        public void putSymIndex(int columnIndex, int key) {
            putInt(columnIndex, key);
        }

        @Override
        public void putSymUtf8(int columnIndex, DirectUtf8Sequence value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putTimestamp(int columnIndex, long value) {
            putLong(columnIndex, value);
        }

        @Override
        public void putUuid(int columnIndex, CharSequence uuidStr) {
            SqlUtil.implicitCastStrAsUuid(uuidStr, uuid);
            putLong128(columnIndex, uuid.getLo(), uuid.getHi());
        }

        @Override
        public void putUuidUtf8(int columnIndex, Utf8Sequence uuidStr) {
            SqlUtil.implicitCastStrAsUuid(uuidStr, uuid);
            putLong128(columnIndex, uuid.getLo(), uuid.getHi());
        }

        @Override
        public void putVarchar(int columnIndex, char value) {
            utf8Sink.clear();
            utf8Sink.put(value);
            VarcharTypeDriver.appendValue(
                    getSecondaryColumn(columnIndex), getPrimaryColumn(columnIndex),
                    utf8Sink
            );
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putVarchar(int columnIndex, Utf8Sequence value) {
            VarcharTypeDriver.appendValue(
                    getSecondaryColumn(columnIndex), getPrimaryColumn(columnIndex),
                    value
            );
            setRowValueNotNull(columnIndex);
        }

        private MemoryA getPrimaryColumn(int columnIndex) {
            return activeColumns.getQuick(getPrimaryColumnIndex(columnIndex));
        }

        private MemoryA getSecondaryColumn(int columnIndex) {
            return activeColumns.getQuick(getSecondaryColumnIndex(columnIndex));
        }
    }
}
